//! Deterministic publication object schemas.
//!
//! All objects are compact JSON plus a trailing newline. Field order comes
//! from the struct declaration order, arrays are already sorted by SQL, and
//! nothing timestamp-like or counter-like is serialized. The hash of an
//! object is the hash of these exact bytes.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::store::is_sha256_lower_hex;

pub const FORMAT_VERSION: u32 = 2;

/// `roots/<aa>/<bb>/<sha>.json` — the generation root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Root {
    pub v: u32,
    pub site: String,
    pub pages: String,
    pub files: String,
    pub shell: String,
}

impl Root {
    pub fn new(site: &str, pages: String, files: String, shell: String) -> Root {
        Root {
            v: FORMAT_VERSION,
            site: site.to_string(),
            pages,
            files,
            shell,
        }
    }
}

/// `objects/pages/...json`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PagesSection {
    pub v: u32,
    pub site: String,
    pub pages: Vec<PageEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageEntry {
    pub id: String,
    pub slug: String,
    pub title: String,
    pub tags: Vec<String>,
    pub revisions_stored: i64,
    pub revisions_known: i64,
    pub max_rev: i64,
    pub archive: Option<String>,
    pub archive_sha256: Option<String>,
}

/// `objects/files/...json`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesSection {
    pub v: u32,
    pub site: String,
    pub files: Vec<FileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub sha256: Option<String>,
    pub size: Option<i64>,
    pub content_type: Option<String>,
    pub status: String,
    pub blob: Option<String>,
}

impl FileEntry {
    pub fn from_row(row: crate::db::FileManifestRow) -> FileEntry {
        let blob = row.sha256.as_deref().and_then(files_ca_ref);
        FileEntry {
            path: row.path,
            sha256: row.sha256,
            size: row.size,
            content_type: row.content_type,
            status: row.status,
            blob,
        }
    }
}

/// `files_ca/ab/cd/<sha[4..]>` — the attachment store layout, or None for a
/// non-canonical hash (never panic on legacy junk).
pub fn files_ca_ref(sha: &str) -> Option<String> {
    if !is_sha256_lower_hex(sha) {
        return None;
    }
    Some(format!(
        "files_ca/{}/{}/{}",
        &sha[..2],
        &sha[2..4],
        &sha[4..]
    ))
}

/// Compact JSON + `\n`. This is the only serialization used for hashed
/// objects.
pub fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value).context("serializing publication object")?;
    bytes.push(b'\n');
    Ok(bytes)
}
