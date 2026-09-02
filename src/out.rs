//! Derived, rebuildable publication of a site DB into `{site}/out/`.
//!
//! The database is the source of truth; everything here can be regenerated
//! from it at any time:
//!
//! - `pages_by_id/ab/cd/<page_id>.zst` — one deterministic tar+zst per page,
//!   holding `rNNN.txt` entries (v1 frontmatter format) for every revision
//!   whose content is stored. Repacked only when the stored-revision count
//!   drifts from `out_state.packed_revs`.
//! - `pages.json` / `files.json` — deterministic manifests (stable ordering,
//!   write-if-changed so untouched publications don't churn bytes).
//! - `shell` — site title/subtitle/theme-roots (v1 format), written by
//!   `shell.sync`.
//!
//! Determinism: tar headers carry mtime 0 / uid 0 / gid 0 / mode 0644, GNU
//! format, entries in rev_no order, and the zstd stream is single-threaded —
//! identical DB rows produce byte-identical archives.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::db::Db;

/// v1's rN.txt frontmatter, byte for byte:
/// `---\n<yaml>\n---\n<content>\n`. Strings are double-quoted with the same
/// four escapes (`\`, `"`, newline, tab) v1's minimal YAML emitter used.
/// Page identity for frontmatter rendering (everything but the revision).
pub struct PageIdentity<'a> {
    pub site: &'a str,
    pub page_id: &'a str,
    pub slug: &'a str,
    pub title: &'a str,
    pub tags: &'a [String],
}

pub fn revision_txt(page: &PageIdentity<'_>, rev: &crate::db::FullRevision) -> String {
    let tags = if page.tags.is_empty() {
        "[]".to_string()
    } else {
        format!(
            "[{}]",
            page.tags
                .iter()
                .map(|t| yaml_quote(t))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "---\n\
         title: {title}\n\
         tags: {tags}\n\
         page_id: {page_id}\n\
         site: {site}\n\
         slug: {slug}\n\
         revision: {rev_no}\n\
         revision_id: {revision_id}\n\
         author: {author}\n\
         timestamp: {ts}\n\
         ---\n\
         {content}\n",
        title = yaml_quote(page.title),
        page_id = yaml_quote(page.page_id),
        site = yaml_quote(page.site),
        slug = yaml_quote(page.slug),
        revision_id = yaml_quote(&rev.rev_id),
        rev_no = rev.rev_no,
        author = rev.author,
        ts = rev.ts,
        content = rev.content,
    )
}

fn yaml_quote(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\t', "\\t");
    format!("\"{escaped}\"")
}

/// `rNNN.txt`, zero-padded to 3 like v1 (wider numbers keep their width).
fn rev_entry_name(rev_no: i64) -> String {
    format!("r{rev_no:0>3}.txt")
}

/// `pages_by_id` sharding: 2/2/rest of a zero-padded page id. Real Wikidot
/// ids are 7+ digits — the padding only keeps pathological short ids from
/// panicking on slicing, and gives the archive writer and the manifest
/// builder one shared shape so they can never disagree.
fn page_id_shards(page_id: &str) -> (String, String, String) {
    let padded = format!("{page_id:0>5}");
    (
        padded[..2].to_string(),
        padded[2..4].to_string(),
        padded[4..].to_string(),
    )
}

pub fn page_archive_path(out_dir: &Path, page_id: &str) -> PathBuf {
    let (a, b, rest) = page_id_shards(page_id);
    out_dir.join("pages_by_id").join(a).join(b).join(format!("{rest}.zst"))
}

#[derive(Serialize)]
struct PageManifest {
    id: String,
    slug: String,
    title: String,
    tags: Vec<String>,
    #[serde(rename = "revisions_stored")]
    stored: i64,
    #[serde(rename = "revisions_known")]
    known: i64,
    max_rev: i64,
    archive: String,
}

#[derive(Serialize)]
struct FileManifest {
    path: String,
    sha256: Option<String>,
    size: Option<i64>,
    content_type: Option<String>,
    status: String,
    blob: Option<String>,
}

#[derive(Debug, Default, PartialEq)]
pub struct PubStats {
    pub pages_packed: usize,
    pub pages_skipped: usize,
    pub manifests_rewritten: usize,
}

/// Publish everything that drifted. See module docs for the layout.
pub fn publish(db: &Db, site: &str, out_dir: &Path, zstd_level: i32) -> Result<PubStats> {
    let mut stats = PubStats::default();

    for page in db.pages_to_pack()? {
        let revisions = db.page_revisions_full(page.page_id)?;
        debug_assert_eq!(revisions.len() as i64, page.stored_count);
        if revisions.is_empty() {
            // Nothing fetchable yet — leave any existing archive alone and
            // keep packed_revs at 0 (equal counts) so it isn't rewritten.
            db.set_packed(page.page_id, 0)?;
            stats.pages_skipped += 1;
            continue;
        }
        if page.packed_revs == page.stored_count
            && page_archive_path(out_dir, &page.id_str()).exists()
        {
            stats.pages_skipped += 1;
            continue;
        }
        let identity = PageIdentity {
            site,
            page_id: &page.id_str(),
            slug: &page.slug,
            title: &page.title,
            tags: &page.tags,
        };
        write_page_archive(out_dir, &page.id_str(), &revisions, zstd_level, |r| {
            revision_txt(&identity, r)
        })
        .with_context(|| format!("packing {}", page.slug))?;
        db.set_packed(page.page_id, page.stored_count)?;
        stats.pages_packed += 1;
    }

    stats.manifests_rewritten = write_manifests(db, site, out_dir)?;
    Ok(stats)
}

/// One page's tar+zst, atomically (tmp + fsync + rename).
fn write_page_archive(
    out_dir: &Path,
    page_id: &str,
    revisions: &[crate::db::FullRevision],
    zstd_level: i32,
    render: impl Fn(&crate::db::FullRevision) -> String,
) -> Result<()> {
    let dest = page_archive_path(out_dir, page_id);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp = dest.with_extension("zst.tmp");
    {
        let file =
            std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        let mut enc = zstd::stream::Encoder::new(file, zstd_level).context("zstd encoder")?;
        {
            let mut tar = tar::Builder::new(&mut enc);
            for rev in revisions {
                let body = render(rev);
                let mut header = tar::Header::new_gnu();
                header.set_size(body.len() as u64);
                header.set_mode(0o644);
                header.set_mtime(0);
                header.set_uid(0);
                header.set_gid(0);
                tar.append_data(&mut header, rev_entry_name(rev.rev_no), body.as_bytes())?;
            }
            tar.finish().context("finishing tar")?;
        }
        let file = enc.finish().context("finishing zstd")?;
        file.sync_all().context("fsync archive")?;
    }
    std::fs::rename(&tmp, &dest)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), dest.display()))?;
    if let Some(parent) = dest.parent() {
        crate::blobs::sync_dir(parent).context("fsync archive dir")?;
    }
    Ok(())
}

/// Deterministic `pages.json` + `files.json`; rewritten only when bytes drift.
fn write_manifests(db: &Db, site: &str, out_dir: &Path) -> Result<usize> {
    let mut rewritten = 0;

    let pages: Vec<PageManifest> = db
        .pages_manifest()?
        .into_iter()
        .map(|p| {
            let id = p.id_str();
            let (a, b, rest) = page_id_shards(&id);
            PageManifest {
                archive: format!("pages_by_id/{a}/{b}/{rest}.zst"),
                id,
                slug: p.slug,
                title: p.title,
                tags: p.tags,
                stored: p.stored_count,
                known: p.known_count,
                max_rev: p.max_rev,
            }
        })
        .collect();
    let files: Vec<FileManifest> = db
        .files_manifest()?
        .into_iter()
        .map(|f| FileManifest {
            path: f.path,
            blob: f
                .sha256
                .as_ref()
                .map(|sha| format!("files_ca/{}/{}/{}", &sha[..2], &sha[2..4], &sha[4..])),
            sha256: f.sha256,
            size: f.size,
            content_type: f.content_type,
            status: f.status,
        })
        .collect();

    let mut pages_doc = serde_json::to_vec_pretty(&serde_json::json!({
        "site": site,
        "pages": pages,
    }))
    .context("serializing pages.json")?;
    pages_doc.push(b'\n');
    let mut files_doc = serde_json::to_vec_pretty(&serde_json::json!({
        "site": site,
        "files": files,
    }))
    .context("serializing files.json")?;
    files_doc.push(b'\n');

    if write_if_changed(&out_dir.join("pages.json"), &pages_doc)? {
        rewritten += 1;
    }
    if write_if_changed(&out_dir.join("files.json"), &files_doc)? {
        rewritten += 1;
    }
    Ok(rewritten)
}

/// Write bytes to `dest` via tmp+rename, skipping the rename when the
/// existing file is byte-identical — untouched publications keep their
/// mtimes.
fn write_if_changed(dest: &Path, bytes: &[u8]) -> Result<bool> {
    if let Ok(existing) = std::fs::read(dest)
        && existing == bytes
    {
        return Ok(false);
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp = dest.with_extension("tmp");
    {
        let mut f =
            std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dest)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), dest.display()))?;
    if let Some(parent) = dest.parent() {
        crate::blobs::sync_dir(parent).context("fsync manifest dir")?;
    }
    Ok(true)
}

// ── Site shell ──

/// The site's display identity (v1 `meta.Shell`). `theme_roots` are the
/// custom-theme @import entry points (base theme filtered off) — recorded
/// here, evacuation of the theme graph is a separate concern.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
pub struct Shell {
    pub title: String,
    pub subtitle: String,
    pub theme_roots: Vec<String>,
}

impl Shell {
    /// v1 `format_shell`: YAML-ish lines, theme_root raw (unquoted URL).
    pub fn to_text(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!("title: {}\n", yaml_quote(&self.title)));
        s.push_str(&format!("subtitle: {}\n", yaml_quote(&self.subtitle)));
        for root in &self.theme_roots {
            s.push_str(&format!("theme_root: {root}\n"));
        }
        s
    }
}

/// Write `{out}/shell` (idempotent, byte-stable for equal shells).
pub fn write_shell(out_dir: &Path, shell: &Shell) -> Result<()> {
    write_if_changed(&out_dir.join("shell"), shell.to_text().as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_txt_matches_v1_layout() {
        let page = PageIdentity {
            site: "agiat",
            page_id: "1402564633",
            slug: "theme:magnorum-undecim",
            title: "Magnorum Undecim",
            tags: &["theme".into(), "sigma".into()],
        };
        let rev = crate::db::FullRevision {
            rev_no: 20,
            rev_id: "1533147940".into(),
            ts: 1761070219,
            author: 3090772,
            content: "[[div class=\"theme\"]]\nbody text".into(),
        };
        let txt = revision_txt(&page, &rev);
        let expected = concat!(
            "---\n",
            "title: \"Magnorum Undecim\"\n",
            "tags: [\"theme\", \"sigma\"]\n",
            "page_id: \"1402564633\"\n",
            "site: \"agiat\"\n",
            "slug: \"theme:magnorum-undecim\"\n",
            "revision: 20\n",
            "revision_id: \"1533147940\"\n",
            "author: 3090772\n",
            "timestamp: 1761070219\n",
            "---\n",
            "[[div class=\"theme\"]]\nbody text\n",
        );
        assert_eq!(txt, expected);
    }

    #[test]
    fn yaml_quote_escapes_like_v1() {
        assert_eq!(yaml_quote("plain"), "\"plain\"");
        assert_eq!(yaml_quote("a\"b\\c"), "\"a\\\"b\\\\c\"");
        assert_eq!(yaml_quote("multi\nline\ttab"), "\"multi\\nline\\ttab\"");
        assert_eq!(yaml_quote(""), "\"\"");
    }

    #[test]
    fn entry_names_zero_pad_to_three() {
        assert_eq!(rev_entry_name(1), "r001.txt");
        assert_eq!(rev_entry_name(20), "r020.txt");
        assert_eq!(rev_entry_name(445), "r445.txt");
        assert_eq!(rev_entry_name(1234), "r1234.txt");
    }

    #[test]
    fn shell_text_matches_v1_format() {
        let shell = Shell {
            title: "La AGIAT".into(),
            subtitle: "Archivo de terror".into(),
            theme_roots: vec!["http://agiat.wdfiles.com/local--code/theme/1.css".into()],
        };
        assert_eq!(
            shell.to_text(),
            concat!(
                "title: \"La AGIAT\"\n",
                "subtitle: \"Archivo de terror\"\n",
                "theme_root: http://agiat.wdfiles.com/local--code/theme/1.css\n",
            )
        );
    }

    #[test]
    fn page_archives_are_deterministic_and_incremental() {
        use crate::db::FullRevision;
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path();
        let rev = |no: i64| FullRevision {
            rev_no: no,
            rev_id: format!("rev{no}"),
            ts: 1_700_000_000 + no,
            author: 42,
            content: format!("body of {no}"),
        };
        let revs: Vec<_> = vec![rev(1), rev(2), rev(10)];
        let identity = PageIdentity {
            site: "s",
            page_id: "1234567890",
            slug: "some:page",
            title: "T",
            tags: &[],
        };
        let render = |r: &FullRevision| revision_txt(&identity, r);
        write_page_archive(out, "1234567890", &revs, 19, render).unwrap();
        let first = std::fs::read(page_archive_path(out, "1234567890")).unwrap();

        // Same inputs → identical bytes.
        write_page_archive(out, "1234567890", &revs, 19, render).unwrap();
        let second = std::fs::read(page_archive_path(out, "1234567890")).unwrap();
        assert_eq!(first, second);

        // And it round-trips through tar+zst with the right entries.
        let dec = zstd::decode_all(first.as_slice()).unwrap();
        let mut ar = tar::Archive::new(dec.as_slice());
        let mut names = Vec::new();
        for entry in ar.entries().unwrap() {
            names.push(
                entry
                    .unwrap()
                    .path()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        assert_eq!(names, ["r001.txt", "r002.txt", "r010.txt"]);
    }

    /// End-to-end publish against a real DB: pack, incremental skip,
    /// repack when a new revision lands, manifest contents.
    #[test]
    fn publish_is_incremental_and_repacks_on_change() {
        use rusqlite::params;
        let dir = tempfile::tempdir().unwrap();
        let db = crate::db::Db::open(&dir.path().join("site.db")).unwrap();
        let out = dir.path().join("out");

        let ins_page = |db: &crate::db::Db, id: i64, slug: &str, tags: &str| {
            db.with_conn(|conn| {
                conn.execute(
                    "INSERT INTO pages(page_id, slug, title, tags, discovered_at) VALUES(?1,?2,?3,?4,?5)",
                    params![id, slug, format!("Title {slug}"), tags, 1],
                )
                .unwrap();
            })
        };
        let ins_rev = |db: &crate::db::Db, page: i64, no: i64, content: Option<&str>| {
            db.with_conn(|conn| {
                conn.execute(
                    "INSERT INTO revisions(page_id, rev_no, rev_id, ts, author, content)
                     VALUES(?1,?2,?3,?4,?5,?6)",
                    params![
                        page,
                        no,
                        format!("rid-{no}"),
                        1_700_000_000 + no,
                        7,
                        content
                    ],
                )
                .unwrap();
            })
        };

        ins_page(&db, 1234567890, "some:page", r#"["a"]"#);
        ins_rev(&db, 1234567890, 1, Some("one"));
        ins_rev(&db, 1234567890, 2, Some("two"));
        // A meta-only revision: exists, content not fetched yet.
        ins_rev(&db, 1234567890, 3, None);
        // A page with nothing fetchable yet — no archive, but no crash.
        ins_page(&db, 9876543210, "bare", "[]");
        ins_rev(&db, 9876543210, 1, None);

        let s1 = publish(&db, "s", &out, 19).unwrap();
        assert_eq!((s1.pages_packed, s1.pages_skipped), (1, 1));
        let archive = page_archive_path(&out, "1234567890");
        assert!(archive.exists());
        assert!(!page_archive_path(&out, "9876543210").exists());
        let first_bytes = std::fs::read(&archive).unwrap();
        let first_mtime = std::fs::metadata(&archive).unwrap().modified().unwrap();

        // Nothing changed → everything skipped, archive byte/mtime untouched,
        // manifests not rewritten.
        let s2 = publish(&db, "s", &out, 19).unwrap();
        assert_eq!((s2.pages_packed, s2.pages_skipped), (0, 2));
        assert_eq!(s2.manifests_rewritten, 0);
        assert_eq!(std::fs::read(&archive).unwrap(), first_bytes);
        assert_eq!(
            std::fs::metadata(&archive).unwrap().modified().unwrap(),
            first_mtime
        );

        // A new revision's content lands (meta row existed, content was
        // NULL) → exactly that page repacks.
        db.with_conn(|conn| {
            conn.execute(
                "UPDATE revisions SET content='three' WHERE page_id=1234567890 AND rev_no=3",
                params![],
            )
            .unwrap();
        });
        let s3 = publish(&db, "s", &out, 19).unwrap();
        assert_eq!(s3.pages_packed, 1);
        assert_ne!(std::fs::read(&archive).unwrap(), first_bytes);

        // Manifests: stable content, sorted, archive + blob paths resolve.
        let pages: serde_json::Value =
            serde_json::from_slice(&std::fs::read(out.join("pages.json")).unwrap()).unwrap();
        let entries = pages["pages"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["id"], "1234567890");
        assert_eq!(entries[0]["revisions_known"], 3);
        assert_eq!(entries[0]["revisions_stored"], 3);
        let rel = entries[0]["archive"].as_str().unwrap();
        assert!(out.join(rel).exists(), "manifest points at real archive");

        // ...and they're not rewritten when nothing drifts: identical bytes
        // skip the tmp+rename entirely, so mtime survives.
        let (pm_bytes, pm_mtime) = {
            let p = out.join("pages.json");
            (
                std::fs::read(&p).unwrap(),
                std::fs::metadata(&p).unwrap().modified().unwrap(),
            )
        };
        let s4 = publish(&db, "s", &out, 19).unwrap();
        assert_eq!(s4.manifests_rewritten, 0);
        assert_eq!(std::fs::read(out.join("pages.json")).unwrap(), pm_bytes);
        assert_eq!(
            std::fs::metadata(out.join("pages.json"))
                .unwrap()
                .modified()
                .unwrap(),
            pm_mtime
        );
    }
}
