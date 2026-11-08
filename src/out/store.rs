//! Content-addressed object store for one site's publication.
//!
//! Every published artifact is immutable: the key is its SHA-256, the path
//! is sharded `aa/bb/<full-sha>`, and an existing target is never rewritten
//! (a size mismatch is an error, not a reason to overwrite). Publication
//! commits by moving `HEAD` — a plain text file holding the root hash — over
//! its predecessor only after every object it references is durable.
//!
//! Temp files are unique (`pid` + counter), fsynced before rename, and
//! removed by a guard on every error path.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::blobs::sync_dir;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Object classes that live under `out/<site>/objects/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Pages,
    Files,
    Shell,
    Archives,
}

impl ObjectKind {
    pub fn name(self) -> &'static str {
        match self {
            ObjectKind::Pages => "pages",
            ObjectKind::Files => "files",
            ObjectKind::Shell => "shell",
            ObjectKind::Archives => "archives",
        }
    }

    pub fn ext(self) -> &'static str {
        match self {
            ObjectKind::Pages | ObjectKind::Files => ".json",
            ObjectKind::Shell => "",
            ObjectKind::Archives => ".zst",
        }
    }
}

/// Result of putting one immutable object: its hash and whether this call
/// actually created the file (as opposed to finding it already present).
#[derive(Debug, Clone)]
pub struct Committed {
    pub sha: String,
    pub wrote: bool,
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Hash a file without loading it into memory.
pub fn stream_sha256(path: &Path) -> Result<String> {
    use std::io::Read;
    let mut file = File::open(path).with_context(|| format!("reading {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .with_context(|| format!("hashing {}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub fn is_sha256_lower_hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn shard(sha: &str) -> (&str, &str) {
    (&sha[..2], &sha[2..4])
}

/// Relative path of an object, as stored in manifests and root sections.
pub fn object_ref(kind: ObjectKind, sha: &str) -> String {
    let (aa, bb) = shard(sha);
    format!("objects/{}/{aa}/{bb}/{sha}{}", kind.name(), kind.ext())
}

/// Relative path of a root object.
pub fn root_ref(sha: &str) -> String {
    let (aa, bb) = shard(sha);
    format!("roots/{aa}/{bb}/{sha}.json")
}

pub fn object_path(out_dir: &Path, kind: ObjectKind, sha: &str) -> PathBuf {
    out_dir.join(object_ref(kind, sha))
}

pub fn root_path(out_dir: &Path, sha: &str) -> PathBuf {
    out_dir.join(root_ref(sha))
}

/// Put `bytes` as an object, computing its hash. Existing objects are
/// verified by size and left untouched.
pub fn put_bytes(out_dir: &Path, kind: ObjectKind, bytes: &[u8]) -> Result<Committed> {
    let sha = sha256_hex(bytes);
    let dest = object_path(out_dir, kind, &sha);
    if dest.exists() {
        verify_size(&dest, bytes.len() as u64)?;
        return Ok(Committed { sha, wrote: false });
    }
    write_atomic(&dest, out_dir, |file| file.write_all(bytes))?;
    Ok(Committed { sha, wrote: true })
}

/// Put a root object (same discipline, different namespace).
pub fn put_root_bytes(out_dir: &Path, bytes: &[u8]) -> Result<Committed> {
    let sha = sha256_hex(bytes);
    let dest = root_path(out_dir, &sha);
    if dest.exists() {
        verify_size(&dest, bytes.len() as u64)?;
        return Ok(Committed { sha, wrote: false });
    }
    write_atomic(&dest, out_dir, |file| file.write_all(bytes))?;
    Ok(Committed { sha, wrote: true })
}

/// Adopt an existing file (the legacy `pages_by_id` archive) into the
/// object store without recompressing it. Hardlink when possible, copy
/// otherwise.
pub fn adopt_file(out_dir: &Path, kind: ObjectKind, src: &Path, sha: &str) -> Result<Committed> {
    let dest = object_path(out_dir, kind, sha);
    let src_len = fs::metadata(src)
        .with_context(|| format!("stat {}", src.display()))?
        .len();
    if dest.exists() {
        verify_size(&dest, src_len)?;
        return Ok(Committed {
            sha: sha.to_string(),
            wrote: false,
        });
    }
    let parent = dest
        .parent()
        .with_context(|| format!("no parent for {}", dest.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;

    if fs::hard_link(src, &dest).is_err() {
        let (tmp, mut out) = TempFile::create(parent, "adopt")?;
        let mut input = File::open(src).with_context(|| format!("reading {}", src.display()))?;
        std::io::copy(&mut input, &mut out)
            .with_context(|| format!("copying {} into the object store", src.display()))?;
        out.sync_all().context("fsync adopted object")?;
        drop(out);
        fs::rename(tmp.path(), &dest)
            .with_context(|| format!("renaming {} -> {}", tmp.path().display(), dest.display()))?;
    }
    sync_chain(parent, out_dir)?;
    Ok(Committed {
        sha: sha.to_string(),
        wrote: true,
    })
}

/// Commit a freshly written temp file (a just-packed archive) as an object.
pub fn commit_temp_file(
    out_dir: &Path,
    kind: ObjectKind,
    src: &Path,
    sha: &str,
) -> Result<Committed> {
    let dest = object_path(out_dir, kind, sha);
    let src_len = fs::metadata(src)
        .with_context(|| format!("stat {}", src.display()))?
        .len();
    if dest.exists() {
        verify_size(&dest, src_len)?;
        let _ = fs::remove_file(src);
        return Ok(Committed {
            sha: sha.to_string(),
            wrote: false,
        });
    }
    let parent = dest
        .parent()
        .with_context(|| format!("no parent for {}", dest.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    fs::rename(src, &dest)
        .with_context(|| format!("renaming {} -> {}", src.display(), dest.display()))?;
    sync_chain(parent, out_dir)?;
    Ok(Committed {
        sha: sha.to_string(),
        wrote: true,
    })
}

fn verify_size(path: &Path, expected: u64) -> Result<()> {
    let actual = fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    if actual != expected {
        bail!(
            "immutable object {} has size {actual}, expected {expected}",
            path.display()
        );
    }
    Ok(())
}

fn write_atomic(
    dest: &Path,
    out_dir: &Path,
    write: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> Result<()> {
    let parent = dest
        .parent()
        .with_context(|| format!("no parent for {}", dest.display()))?;
    let (tmp, mut file) = TempFile::create(parent, "object")?;
    write(&mut file).with_context(|| format!("writing {}", tmp.path().display()))?;
    file.sync_all().context("fsync object")?;
    drop(file);
    fs::rename(tmp.path(), dest)
        .with_context(|| format!("renaming {} -> {}", tmp.path().display(), dest.display()))?;
    sync_chain(parent, out_dir)?;
    Ok(())
}

/// `HEAD` as a root hash, if present. A malformed file is an error: guessing
/// could serve a half-written generation. Used by readers/tests; publication
/// compares the bytes directly before switching it.
#[allow(dead_code)]
pub fn read_head(out_dir: &Path) -> Result<Option<String>> {
    let path = out_dir.join("HEAD");
    let raw = match fs::read(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let text =
        String::from_utf8(raw).with_context(|| format!("{} is not UTF-8", path.display()))?;
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }
    if !is_sha256_lower_hex(text) {
        bail!("{} does not contain a lowercase SHA-256", path.display());
    }
    Ok(Some(text.to_string()))
}

/// Atomically point `HEAD` at `sha`. Returns whether the file changed.
pub fn write_head_if_changed(out_dir: &Path, sha: &str) -> Result<bool> {
    if !is_sha256_lower_hex(sha) {
        bail!("refusing to write malformed root hash '{sha}'");
    }
    let desired = format!("{sha}\n");
    let path = out_dir.join("HEAD");
    if let Ok(existing) = fs::read(&path)
        && existing == desired.as_bytes()
    {
        return Ok(false);
    }
    fs::create_dir_all(out_dir).with_context(|| format!("creating {}", out_dir.display()))?;
    let (tmp, mut file) = TempFile::create(out_dir, "HEAD")?;
    file.write_all(desired.as_bytes())
        .with_context(|| format!("writing {}", tmp.path().display()))?;
    file.sync_all().context("fsync HEAD")?;
    drop(file);
    fs::rename(tmp.path(), &path)
        .with_context(|| format!("renaming {} -> {}", tmp.path().display(), path.display()))?;
    sync_dir(out_dir)?;
    Ok(true)
}

/// fsync `dir` and each parent up to (and including) `out_dir`. Object
/// shards are fresh directories until first use, so the whole chain must
/// become durable before HEAD names them.
pub fn sync_chain(mut dir: &Path, out_dir: &Path) -> Result<()> {
    for _ in 0..8 {
        sync_dir(dir)?;
        if dir == out_dir {
            return Ok(());
        }
        match dir.parent() {
            Some(parent) => dir = parent,
            None => break,
        }
    }
    Ok(())
}

/// Exclusive publication lock. Panics never happen on contention: the caller
/// gets an error and the job retries on its next tick.
pub struct PublishLock(File);

impl PublishLock {
    pub fn acquire(out_dir: &Path) -> Result<PublishLock> {
        fs::create_dir_all(out_dir).with_context(|| format!("creating {}", out_dir.display()))?;
        let path = out_dir.join(".publish.lock");
        let file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        let locked = fs4::fs_std::FileExt::try_lock_exclusive(&file)
            .map_err(|e| anyhow::anyhow!("flock {}: {e}", path.display()))?;
        if !locked {
            bail!(
                "another publication is already running for {}",
                out_dir.display()
            );
        }
        Ok(PublishLock(file))
    }
}

impl Drop for PublishLock {
    fn drop(&mut self) {
        let _ = fs4::fs_std::FileExt::unlock(&self.0);
    }
}

/// A unique temp file that deletes itself if it is not renamed away.
pub struct TempFile {
    path: PathBuf,
}

impl TempFile {
    pub fn create(dir: &Path, stem: &str) -> Result<(TempFile, File)> {
        fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        for _ in 0..100 {
            let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = dir.join(format!(".{stem}.{}.{}.tmp", std::process::id(), n));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => return Ok((TempFile { path }, file)),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    return Err(e).with_context(|| format!("creating {}", path.display()));
                }
            }
        }
        bail!("could not allocate a unique temp file in {}", dir.display())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
