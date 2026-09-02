//! Content-addressed blob storage under `{site}/out/files_ca/`.
//!
//! Layout matches v1's `_files/` exactly — `ab/cd/<sha256[4..]>` — so the
//! legacy importer can move blobs without renaming. Blobs are immutable:
//! write is tmp + fsync + rename, and an existing target short-circuits.

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
    Ok(hash)
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
}
