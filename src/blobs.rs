//! Content-addressed blob storage under `{site}/out/files_ca/`.
//!
//! Layout matches v1's `_files/` exactly — `ab/cd/<sha256[4..]>` — so the
//! legacy importer can move blobs without renaming. Blobs are immutable:
//! write is tmp + fsync + rename + directory fsync (the bytes are the only
//! copy of an attachment, so the rename must survive power loss before the
//! DB row referencing it commits), and an existing target short-circuits.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

/// Write `bytes` content-addressed under `out_dir/files_ca/`, returning the
/// full sha256 hex. Idempotent: existing blobs are left untouched.
pub fn write_blob(out_dir: &Path, bytes: &[u8]) -> Result<String> {
    let hash = hex::encode(Sha256::digest(bytes));
    let dest = blob_path(out_dir, &hash);
    if dest.exists() {
        return Ok(hash);
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp = dest.with_extension("tmp");
    {
        use std::io::Write;
        let mut f =
            std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &dest)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), dest.display()))?;
    // Blob bytes are the only copy of an attachment — the DB stores hashes,
    // not content — and the `files` row referencing this blob commits right
    // after write_blob returns (durable: synchronous=FULL). The blob must be
    // on disk first: the file fsync above covers the bytes, this covers the
    // rename's directory entry plus any fresh shard dirs (bounded walk — the
    // shard is exactly files_ca/<2>/<2> under out_dir).
    let mut dir = dest.parent().map(Path::to_path_buf);
    let mut depth = 0;
    while let Some(d) = dir
        && depth < 8
    {
        sync_dir(&d)?;
        if d == out_dir {
            break;
        }
        dir = d.parent().map(Path::to_path_buf);
        depth += 1;
    }
    Ok(hash)
}

/// fsync a directory so a just-renamed entry survives power loss.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("fsyncing {}", dir.display()))
}

/// `out/files_ca/ab/cd/<sha[4..]>` (v1 sharding: 2/2/rest of the key).
pub fn blob_path(out_dir: &Path, sha256: &str) -> PathBuf {
    debug_assert!(sha256.len() >= 5, "sha256 hex");
    out_dir
        .join("files_ca")
        .join(&sha256[..2])
        .join(&sha256[2..4])
        .join(&sha256[4..])
}

/// Best-effort media type for stored bytes: magic numbers first, falling
/// back to the file-name extension (`name` is the files-row path, which
/// carries the original name — the blob path is a bare hash). Used wherever
/// no server Content-Type exists (legacy import) or as a fetch fallback.
pub fn sniff_content_type(bytes: &[u8], name: &str) -> Option<String> {
    infer::get(bytes)
        .map(|t| t.mime_type().to_string())
        // The extension lives in the path; query/fragment carry none
        // (`…/css?family=Exo+2` is still a `.css`-ish URL).
        .or_else(|| {
            let bare = name.split(['?', '#']).next().unwrap_or(name);
            mime_guess::from_path(bare)
                .first_raw()
                .map(str::to_string)
        })
}

/// `sniff_content_type` for an on-disk blob: reads a bounded prefix (the
/// magic-byte matchers never need more) instead of the whole file.
pub fn sniff_content_type_at(path: &Path, name: &str) -> Option<String> {
    use std::io::Read;
    let mut prefix = [0u8; 8192];
    let n = std::fs::File::open(path).ok()?.read(&mut prefix).ok()?;
    sniff_content_type(&prefix[..n], name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blobs_are_content_addressed_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path();
        let h1 = write_blob(out, b"hello").unwrap();
        assert_eq!(h1.len(), 64);
        let h2 = write_blob(out, b"hello").unwrap();
        assert_eq!(h1, h2);
        let p = blob_path(out, &h1);
        assert!(p.exists());
        assert_eq!(
            p,
            out.join("files_ca")
                .join(&h1[..2])
                .join(&h1[2..4])
                .join(&h1[4..])
        );
        assert_eq!(std::fs::read(p).unwrap(), b"hello");
        // same bytes under a different out dir → same hash
        let h3 = write_blob(tempfile::tempdir().unwrap().path(), b"hello").unwrap();
        assert_eq!(h1, h3);
    }

    #[test]
    fn sniff_prefers_magic_bytes_over_extension() {
        // PNG bytes with a .css name: magic wins.
        let png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR";
        assert_eq!(
            sniff_content_type(png, "local--files/x/pic.css").as_deref(),
            Some("image/png")
        );
        // Text bytes: no magic match, extension decides.
        assert_eq!(
            sniff_content_type(b"h1 { color: red }", "theme/style.css").as_deref(),
            Some("text/css")
        );
        // No magic, no extension → unknown.
        assert_eq!(sniff_content_type(b"???", "noext"), None);
    }

    #[test]
    fn sniff_at_reads_prefix_and_uses_row_name() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("abcd"); // hash-like name, no extension
        std::fs::write(&blob, b"\x25PDF-1.7 rest of pdf").unwrap();
        assert_eq!(
            sniff_content_type_at(&blob, "local--files/x/doc.bin").as_deref(),
            Some("application/pdf")
        );
        // CSS text under an extension-less blob name: the row path decides.
        let css = dir.path().join("efgh");
        std::fs::write(&css, b"@import url(x);\nbody {}").unwrap();
        assert_eq!(
            sniff_content_type_at(&css, "files/cdn/x/style.css").as_deref(),
            Some("text/css")
        );
        assert_eq!(sniff_content_type_at(&dir.path().join("nope"), "a.png"), None);
    }
}
