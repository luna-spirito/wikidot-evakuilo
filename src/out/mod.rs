//! Derived, rebuildable publication of a site DB into `out/<site>/`.
//!
//! The database is the source of truth; the publication is an immutable,
//! content-addressed snapshot:
//!
//! ```text
//! out/<site>/
//!   HEAD                                   # <root_sha>\n — the commit point
//!   roots/<aa>/<bb>/<root_sha>.json
//!   objects/pages/<aa>/<bb>/<sha>.json
//!   objects/files/<aa>/<bb>/<sha>.json
//!   objects/shell/<aa>/<bb>/<sha>
//!   objects/archives/<aa>/<bb>/<sha>.zst
//!   files_ca/...                           # attachments (unchanged)
//! ```
//!
//! A generation is committed by atomically replacing `HEAD` with the root
//! hash after every object reachable from that root is durable. Old roots
//! and objects are never deleted, so a reader that resolved the old `HEAD`
//! first keeps a valid snapshot even while the next one is being built.
//! Every site is independent: there is no shared `out/HEAD` and no
//! cross-site manifest.
//!
//! Publication reads the entire DB (pages, files, shell) through one
//! read-only SQLite snapshot, so a worker commit mid-publish cannot mix
//! generations. Page archives are tar+zstd, unchanged from v1; they are
//! addressed as a whole, so a page change creates exactly one new archive
//! object and untouched pages keep theirs.
//!
//! Migration from the old mutable tree is automatic: an `out_state` row
//! whose `packed_revs` match and whose `archive_sha256` is still NULL adopts
//! its old `pages_by_id/...zst` by streaming hash, without recompressing.

mod pack;
mod schema;
mod store;

pub use pack::{PageIdentity, revision_txt};
pub use schema::{FileEntry, FilesSection, PageEntry, PagesSection, Root};

use std::path::Path;

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use serde::Serialize;

use crate::db::{self, Db, OutStateUpdate, PageToPack};

use schema::{FORMAT_VERSION, json_bytes};
use store::ObjectKind;

#[derive(Debug, Default, PartialEq)]
pub struct PubStats {
    /// Pages whose archive was (re)compressed this run.
    pub pages_packed: usize,
    /// Pages not repacked for any reason (no content, reuse, adoption).
    pub pages_skipped: usize,
    /// Pages that reused their existing archive object.
    pub pages_reused: usize,
    /// Pages whose legacy `pages_by_id` archive was adopted without repack.
    pub pages_adopted: usize,
    /// New immutable objects written (archives + sections + root).
    pub objects_written: usize,
    /// Whether the atomic `HEAD` switch actually changed bytes.
    pub head_changed: bool,
}

/// Publish one site. See the module docs for the invariant ordering: all
/// objects are durable before `HEAD` is switched.
pub fn publish(db: &Db, site: &str, out_dir: &Path, zstd_level: i32) -> Result<PubStats> {
    // Serialise publications of this site. Contention is a retryable error,
    // never a concurrent writer.
    let _lock = store::PublishLock::acquire(out_dir)?;
    let mut stats = PubStats::default();

    // One SQLite snapshot for pages, files and shell.
    let snapshot = db.read_snapshot()?;
    let pages = db::pages_for_publish_on(snapshot.conn())?;
    let files = db::files_manifest_on(snapshot.conn())?;
    let shell_bytes = shell_bytes_on(snapshot.conn())?;

    let mut entries: Vec<PageEntry> = Vec::with_capacity(pages.len());
    let mut updates: Vec<OutStateUpdate> = Vec::new();
    for page in &pages {
        let (archive, archive_sha256) =
            resolve_archive(&snapshot, out_dir, site, page, zstd_level, &mut stats)?;

        if page.packed_revs != page.stored_count || page.archive_sha256 != archive_sha256 {
            updates.push(OutStateUpdate {
                page_id: page.page_id,
                packed_revs: page.stored_count,
                archive_sha256: archive_sha256.clone(),
            });
        }

        entries.push(PageEntry {
            id: page.id_str(),
            slug: page.slug.clone(),
            title: page.title.clone(),
            tags: page.tags.clone(),
            revisions_stored: page.stored_count,
            revisions_known: page.known_count,
            max_rev: page.max_rev,
            archive,
            archive_sha256,
        });
    }
    drop(snapshot);

    // Immutable sections and the root that names them. Archives are already
    // durable at this point.
    let pages_section = PagesSection {
        v: FORMAT_VERSION,
        site: site.to_string(),
        pages: entries.clone(),
    };
    let files_section = FilesSection {
        v: FORMAT_VERSION,
        site: site.to_string(),
        files: files.iter().cloned().map(FileEntry::from_row).collect(),
    };

    let pages_obj = store::put_bytes(out_dir, ObjectKind::Pages, &json_bytes(&pages_section)?)?;
    let files_obj = store::put_bytes(out_dir, ObjectKind::Files, &json_bytes(&files_section)?)?;
    let shell_obj = store::put_bytes(out_dir, ObjectKind::Shell, &shell_bytes)?;
    stats.objects_written +=
        pages_obj.wrote as usize + files_obj.wrote as usize + shell_obj.wrote as usize;

    let root_bytes = json_bytes(&Root::new(
        site,
        pages_obj.sha.clone(),
        files_obj.sha.clone(),
        shell_obj.sha.clone(),
    ))?;
    let root = store::put_root_bytes(out_dir, &root_bytes)?;
    stats.objects_written += root.wrote as usize;

    // The archive-identity cache is only a hint; update it atomically and
    // only where it actually drifted.
    db.update_out_state(&updates)?;

    // The commit point. Everything the root names is durable now.
    stats.head_changed = store::write_head_if_changed(out_dir, &root.sha)?;
    Ok(stats)
}

/// Decide an archive for one page: reuse, adopt the legacy file, or repack.
fn resolve_archive(
    snapshot: &db::Snapshot,
    out_dir: &Path,
    site: &str,
    page: &PageToPack,
    zstd_level: i32,
    stats: &mut PubStats,
) -> Result<(Option<String>, Option<String>)> {
    if page.stored_count == 0 {
        // Nothing fetchable yet: no archive in the section, no packed count.
        stats.pages_skipped += 1;
        return Ok((None, None));
    }

    if page.packed_revs == page.stored_count {
        if let Some(sha) = &page.archive_sha256 {
            if store::is_sha256_lower_hex(sha)
                && store::object_path(out_dir, ObjectKind::Archives, sha).exists()
            {
                stats.pages_skipped += 1;
                stats.pages_reused += 1;
                return Ok((
                    Some(store::object_ref(ObjectKind::Archives, sha)),
                    Some(sha.clone()),
                ));
            }
        } else {
            // Pre-v8 row: the last archive is still at the legacy path.
            // Hash and adopt it instead of recompressing every page once.
            let legacy = pack::page_archive_path(out_dir, &page.id_str());
            if legacy.exists() {
                let sha = store::stream_sha256(&legacy)?;
                let adopted = store::adopt_file(out_dir, ObjectKind::Archives, &legacy, &sha)?;
                stats.objects_written += adopted.wrote as usize;
                stats.pages_skipped += 1;
                stats.pages_adopted += 1;
                return Ok((
                    Some(store::object_ref(ObjectKind::Archives, &sha)),
                    Some(sha),
                ));
            }
        }
    }

    // Count drift, a missing object, or no legacy file: repack from the
    // same snapshot the counts came from.
    let revisions = db::page_revisions_full_on(snapshot.conn(), page.page_id)?;
    if revisions.len() as i64 != page.stored_count {
        bail!(
            "page {}/{}: snapshot has {} content revisions, metadata says {} — \
             refusing to publish a partial archive",
            page.page_id,
            page.slug,
            revisions.len(),
            page.stored_count
        );
    }
    let id = page.id_str();
    let identity = PageIdentity {
        site,
        page_id: &id,
        slug: &page.slug,
        title: &page.title,
        tags: &page.tags,
    };
    let (tmp, file) = store::TempFile::create(out_dir, "archive")?;
    pack::write_archive(file, &revisions, zstd_level, |rev| {
        revision_txt(&identity, rev)
    })
    .with_context(|| format!("packing {}", page.slug))?;
    let sha = store::stream_sha256(tmp.path())?;
    let committed = store::commit_temp_file(out_dir, ObjectKind::Archives, tmp.path(), &sha)?;
    stats.objects_written += committed.wrote as usize;
    stats.pages_packed += 1;
    Ok((
        Some(store::object_ref(ObjectKind::Archives, &sha)),
        Some(sha),
    ))
}

/// Shell bytes for the immutable shell object. `meta['shell']` owns the
/// content once `shell.sync` has run; before that the shell publishes as
/// empty. Broken JSON in `meta['shell']` is fatal: a stale HEAD beats
/// publishing an empty shell.
fn shell_bytes_on(conn: &Connection) -> Result<Vec<u8>> {
    if let Some(raw) = db::meta_get_on(conn, "shell")? {
        let shell: Shell = serde_json::from_str(&raw).context("parsing meta['shell']")?;
        return Ok(shell.to_text().into_bytes());
    }
    Ok(Shell::default().to_text().into_bytes())
}

// ── Site shell ──

/// The site's display identity (v1 `meta.Shell` + landing). `theme_roots`
/// are the custom-theme @import entry points (base theme filtered off) —
/// recorded here, evacuation of the theme graph is a separate concern.
#[derive(Debug, Clone, Default, PartialEq, Serialize, serde::Deserialize)]
pub struct Shell {
    pub title: String,
    pub subtitle: String,
    /// Landing page slug — the page the site root serves. Empty when
    /// unknown (v1 imports carry no landing; `shell.sync` fills it in).
    #[serde(default)]
    pub landing: String,
    pub theme_roots: Vec<String>,
}

impl Shell {
    /// v1 `format_shell` shape: YAML-ish lines, theme_root raw (unquoted
    /// URL). `landing` is a v2 addition, emitted only when known so v1
    /// files round-trip byte-identically.
    pub fn to_text(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!("title: {}\n", pack::yaml_quote(&self.title)));
        s.push_str(&format!("subtitle: {}\n", pack::yaml_quote(&self.subtitle)));
        if !self.landing.is_empty() {
            s.push_str(&format!("landing: {}\n", pack::yaml_quote(&self.landing)));
        }
        for root in &self.theme_roots {
            s.push_str(&format!("theme_root: {root}\n"));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{self, Db};
    use rusqlite::params;
    use serde::Deserialize;
    use std::fs::File;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    fn setup(name: &str) -> (tempfile::TempDir, Db, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(&dir.path().join("meta").join(name).join("site.db")).unwrap();
        let out = dir.path().join("out").join(name);
        (dir, db, out)
    }

    fn insert_page(db: &Db, id: i64, slug: &str, title: &str, tags: &str) {
        db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO pages(page_id, slug, title, tags, discovered_at)
                 VALUES(?1, ?2, ?3, ?4, ?5)",
                params![id, slug, title, tags, 1],
            )
            .unwrap();
        });
    }

    fn insert_rev(db: &Db, page: i64, no: i64, content: Option<&str>) {
        db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO revisions(page_id, rev_no, rev_id, ts, author, content)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
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
        });
    }

    fn set_out_state(db: &Db, page: i64, packed: i64, sha: Option<&str>) {
        db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO out_state(page_id, packed_revs, archive_sha256, packed_at)
                 VALUES(?1, ?2, ?3, ?4)
                 ON CONFLICT(page_id) DO UPDATE SET
                   packed_revs=excluded.packed_revs,
                   archive_sha256=excluded.archive_sha256,
                   packed_at=excluded.packed_at",
                params![page, packed, sha, 1],
            )
            .unwrap();
        });
    }

    fn head(out: &Path) -> String {
        std::fs::read_to_string(out.join("HEAD"))
            .unwrap()
            .trim()
            .to_string()
    }

    fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    fn root_of(out: &Path) -> (String, Root) {
        let sha = head(out);
        let root: Root = read_json(&store::root_path(out, &sha));
        (sha, root)
    }

    fn pages_of(out: &Path, root: &Root) -> PagesSection {
        read_json(&store::object_path(out, ObjectKind::Pages, &root.pages))
    }

    fn files_of(out: &Path, root: &Root) -> FilesSection {
        read_json(&store::object_path(out, ObjectKind::Files, &root.files))
    }

    fn object_files(out: &Path) -> Vec<PathBuf> {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        walk(&path, out);
                    } else {
                        out.push(path);
                    }
                }
            }
        }
        let mut files = Vec::new();
        walk(&out.join("objects"), &mut files);
        files.sort();
        files
    }

    fn archive_hashes(pages: &PagesSection) -> Vec<Option<String>> {
        pages
            .pages
            .iter()
            .map(|p| p.archive_sha256.clone())
            .collect()
    }

    fn seed_two_pages(db: &Db) {
        insert_page(db, 1234567890, "some:page", "Title", r#"["a","b"]"#);
        insert_rev(db, 1234567890, 1, Some("one"));
        insert_rev(db, 1234567890, 2, Some("two"));
        insert_rev(db, 1234567890, 3, None);
        insert_page(db, 9876543210, "bare", "", "[]");
        insert_rev(db, 9876543210, 1, None);
    }

    #[test]
    fn shell_text_matches_v1_format() {
        let shell = Shell {
            title: "La AGIAT".into(),
            subtitle: "Archivo de terror".into(),
            landing: "blog:_start".into(),
            theme_roots: vec!["http://agiat.wdfiles.com/local--code/theme/1.css".into()],
        };
        assert_eq!(
            shell.to_text(),
            concat!(
                "title: \"La AGIAT\"\n",
                "subtitle: \"Archivo de terror\"\n",
                "landing: \"blog:_start\"\n",
                "theme_root: http://agiat.wdfiles.com/local--code/theme/1.css\n",
            )
        );
    }

    #[test]
    fn shell_without_landing_omits_the_line() {
        let shell = Shell {
            title: "T".into(),
            subtitle: String::new(),
            landing: String::new(),
            theme_roots: vec![],
        };
        assert_eq!(shell.to_text(), "title: \"T\"\nsubtitle: \"\"\n");
    }

    /// Full layout: HEAD, root, three sections, sharded content-addressed
    /// archives; no global HEAD and no new `pages_by_id`.
    #[test]
    fn publish_creates_immutable_layout() {
        let (_dir, db, out) = setup("layout");
        seed_two_pages(&db);

        let stats = publish(&db, "layout", &out, 19).unwrap();
        assert_eq!(stats.pages_packed, 1);
        assert_eq!(stats.pages_skipped, 1);
        assert!(stats.head_changed);
        assert!(stats.objects_written >= 5);

        // One commit file per site, never a global one.
        assert!(out.join("HEAD").is_file());
        assert!(
            !std::fs::symlink_metadata(out.join("HEAD"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(!out.parent().unwrap().join("HEAD").exists());
        assert!(!out.join("pages_by_id").exists());

        let sha = head(&out);
        assert_eq!(sha.len(), 64);
        assert_eq!(
            store::read_head(&out).unwrap().as_deref(),
            Some(sha.as_str())
        );

        let (root_sha, root) = root_of(&out);
        assert_eq!(root_sha, sha);
        assert_eq!(root.v, 2);
        assert_eq!(root.site, "layout");
        for (kind, digest) in [
            (ObjectKind::Pages, &root.pages),
            (ObjectKind::Files, &root.files),
            (ObjectKind::Shell, &root.shell),
        ] {
            let path = store::object_path(&out, kind, digest);
            assert!(path.exists(), "missing {}", path.display());
            assert_eq!(&store::stream_sha256(&path).unwrap(), digest);
            assert!(path.to_string_lossy().contains(&format!(
                "{}/{}/{}",
                &digest[..2],
                &digest[2..4],
                digest
            )));
        }
        assert_eq!(
            store::stream_sha256(&store::root_path(&out, &sha)).unwrap(),
            sha
        );

        let pages = pages_of(&out, &root);
        assert_eq!(pages.v, 2);
        assert_eq!(pages.site, "layout");
        assert_eq!(pages.pages.len(), 2);
        assert_eq!(pages.pages[0].id, "1234567890");
        assert_eq!(pages.pages[0].revisions_stored, 2);
        assert_eq!(pages.pages[0].revisions_known, 3);
        assert_eq!(pages.pages[0].max_rev, 3);
        assert_eq!(pages.pages[0].tags, ["a", "b"]);
        let archive = pages.pages[0].archive.as_deref().unwrap();
        let archive_sha = pages.pages[0].archive_sha256.as_deref().unwrap();
        assert_eq!(
            archive,
            store::object_ref(ObjectKind::Archives, archive_sha)
        );
        assert!(out.join(archive).exists());
        assert_eq!(
            &store::stream_sha256(&out.join(archive)).unwrap(),
            archive_sha
        );

        assert_eq!(pages.pages[1].id, "9876543210");
        assert_eq!(pages.pages[1].revisions_stored, 0);
        assert!(pages.pages[1].archive.is_none());
        assert!(pages.pages[1].archive_sha256.is_none());

        let files = files_of(&out, &root);
        assert_eq!(files.v, 2);
        assert!(files.files.is_empty());
    }

    /// A second publish with no DB change is a true no-op: same HEAD bytes
    /// and mtime, no new objects, no `out_state` churn.
    #[test]
    fn unchanged_publish_is_a_no_op() {
        let (_dir, db, out) = setup("noop");
        seed_two_pages(&db);
        let first = publish(&db, "noop", &out, 19).unwrap();
        assert!(first.head_changed);

        let head_bytes = std::fs::read(out.join("HEAD")).unwrap();
        let head_mtime = std::fs::metadata(out.join("HEAD"))
            .unwrap()
            .modified()
            .unwrap();
        let objects_before = object_files(&out);
        let state_before: Vec<(i64, i64, Option<String>, i64)> = db
            .with_conn(|conn| {
                conn.prepare("SELECT page_id, packed_revs, archive_sha256, packed_at FROM out_state ORDER BY page_id")?
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap();

        std::thread::sleep(Duration::from_millis(20));
        let second = publish(&db, "noop", &out, 19).unwrap();
        assert_eq!(second.pages_packed, 0);
        assert_eq!(second.pages_reused, 1);
        assert_eq!(second.pages_skipped, 2);
        assert_eq!(second.objects_written, 0);
        assert!(!second.head_changed);

        assert_eq!(std::fs::read(out.join("HEAD")).unwrap(), head_bytes);
        assert_eq!(
            std::fs::metadata(out.join("HEAD"))
                .unwrap()
                .modified()
                .unwrap(),
            head_mtime
        );
        assert_eq!(object_files(&out), objects_before);
        let state_after: Vec<(i64, i64, Option<String>, i64)> = db
            .with_conn(|conn| {
                conn.prepare("SELECT page_id, packed_revs, archive_sha256, packed_at FROM out_state ORDER BY page_id")?
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap();
        assert_eq!(state_after, state_before);
    }

    /// One page gaining a revision creates exactly one new archive object;
    /// the other page and the old generation stay intact.
    #[test]
    fn page_change_adds_one_archive_object() {
        let (_dir, db, out) = setup("page-change");
        insert_page(&db, 111, "one", "One", "[]");
        insert_rev(&db, 111, 1, Some("v1"));
        insert_page(&db, 222, "two", "Two", "[]");
        insert_rev(&db, 222, 1, Some("other"));
        publish(&db, "page-change", &out, 19).unwrap();

        let (old_root_sha, old_root) = root_of(&out);
        let old_pages = pages_of(&out, &old_root);
        let old_archives = archive_hashes(&old_pages);
        let old_objects = object_files(&out);
        assert!(store::root_path(&out, &old_root_sha).exists());

        insert_rev(&db, 111, 2, Some("v2"));
        let stats = publish(&db, "page-change", &out, 19).unwrap();
        assert_eq!(stats.pages_packed, 1);
        assert!(stats.head_changed);

        let (new_root_sha, new_root) = root_of(&out);
        assert_ne!(new_root_sha, old_root_sha);
        let new_pages = pages_of(&out, &new_root);
        let new_archives = archive_hashes(&new_pages);
        assert_ne!(new_archives[0], old_archives[0]);
        assert_eq!(new_archives[1], old_archives[1]);

        // Old generation remains readable, and the archive set grew by one.
        assert!(store::root_path(&out, &old_root_sha).exists());
        assert!(store::object_path(&out, ObjectKind::Pages, &old_root.pages).exists());
        for old in &old_objects {
            assert!(old.exists(), "old object {} disappeared", old.display());
        }
        // Exactly one new archive object; the pages section also moved, the
        // root lives outside `objects/`.
        let count_archives = |paths: &[PathBuf]| {
            paths
                .iter()
                .filter(|p| p.to_string_lossy().contains("/archives/"))
                .count()
        };
        let new_objects = object_files(&out);
        assert_eq!(
            count_archives(&new_objects),
            count_archives(&old_objects) + 1
        );
        assert_eq!(new_objects.len(), old_objects.len() + 2);
    }

    /// Title/tags/slug-only changes move pages section/root/HEAD but not the
    /// archive object.
    #[test]
    fn metadata_only_change_keeps_archive() {
        let (_dir, db, out) = setup("metadata");
        insert_page(&db, 111, "one", "One", r#"["a"]"#);
        insert_rev(&db, 111, 1, Some("v1"));
        publish(&db, "metadata", &out, 19).unwrap();

        let (old_root_sha, old_root) = root_of(&out);
        let old_pages = pages_of(&out, &old_root);
        let old_archives = archive_hashes(&old_pages);
        let old_objects = object_files(&out);

        db.with_conn(|conn| {
            conn.execute(
                "UPDATE pages SET slug='renamed', title='New title', tags='[\"b\"]' WHERE page_id=111",
                [],
            )
            .unwrap();
        });
        let stats = publish(&db, "metadata", &out, 19).unwrap();
        assert_eq!(stats.pages_packed, 0);
        assert_eq!(stats.pages_reused, 1);
        assert!(stats.head_changed);

        let (new_root_sha, new_root) = root_of(&out);
        assert_ne!(new_root_sha, old_root_sha);
        assert_ne!(new_root.pages, old_root.pages);
        let new_pages = pages_of(&out, &new_root);
        assert_eq!(archive_hashes(&new_pages), old_archives);
        assert_eq!(new_pages.pages[0].title, "New title");
        assert_eq!(new_pages.pages[0].tags, ["b"]);
        assert_eq!(object_files(&out).len(), old_objects.len() + 1); // pages section only
    }

    /// Files and shell changes move their sections/root; page archives are
    /// untouched, and no mutable `shell` file is ever written.
    #[test]
    fn files_and_shell_change_sections_without_touching_archives() {
        let (_dir, db, out) = setup("files-shell");
        insert_page(&db, 111, "one", "One", "[]");
        insert_rev(&db, 111, 1, Some("v1"));
        publish(&db, "files-shell", &out, 19).unwrap();

        let (_old_root_sha, old_root) = root_of(&out);
        let old_archives = archive_hashes(&pages_of(&out, &old_root));

        let sha = blobs_sha(b"hello");
        db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO files(path, sha256, size, status, first_seen, content_type)
                 VALUES('http://site.wikidot.com/local--files/x/a.png', ?1, 5, 'saved', 1, 'image/png')",
                params![sha],
            )
            .unwrap();
        });
        publish(&db, "files-shell", &out, 19).unwrap();

        let (_file_root_sha, file_root) = root_of(&out);
        assert_ne!(file_root.files, old_root.files);
        assert_eq!(file_root.pages, old_root.pages);
        let files = files_of(&out, &file_root);
        assert_eq!(files.files.len(), 1);
        assert_eq!(
            files.files[0].blob.as_deref(),
            Some(format!("files_ca/{}/{}/{}", &sha[..2], &sha[2..4], &sha[4..]).as_str())
        );
        assert_eq!(archive_hashes(&pages_of(&out, &file_root)), old_archives);

        let shell = Shell {
            title: "Published".into(),
            subtitle: "sub".into(),
            landing: "start".into(),
            theme_roots: vec!["http://cdn/x.css".into()],
        };
        db.meta_set("shell", &serde_json::to_string(&shell).unwrap())
            .unwrap();
        publish(&db, "files-shell", &out, 19).unwrap();

        let (_shell_root_sha, shell_root) = root_of(&out);
        assert_ne!(shell_root.shell, file_root.shell);
        assert_eq!(
            std::fs::read(store::object_path(
                &out,
                ObjectKind::Shell,
                &shell_root.shell
            ))
            .unwrap(),
            shell.to_text().as_bytes()
        );
        assert!(!out.join("shell").exists());
        assert_eq!(archive_hashes(&pages_of(&out, &shell_root)), old_archives);
    }

    /// Pre-v8 state: matching counts + old archive + NULL hash → adopt the
    /// bytes without recompressing and fill the cached hash.
    #[test]
    fn legacy_archive_is_adopted_without_repack() {
        let (_dir, db, out) = setup("adopt");
        insert_page(&db, 1234567890, "some:page", "Title", "[]");
        insert_rev(&db, 1234567890, 1, Some("one"));
        insert_rev(&db, 1234567890, 2, Some("two"));
        set_out_state(&db, 1234567890, 2, None);

        let legacy = pack::page_archive_path(&out, "1234567890");
        std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        let identity = PageIdentity {
            site: "adopt",
            page_id: "1234567890",
            slug: "some:page",
            title: "Title",
            tags: &[],
        };
        let revisions = vec![
            db::FullRevision {
                rev_no: 1,
                rev_id: "rid-1".into(),
                ts: 1_700_000_001,
                author: 7,
                content: "one".into(),
            },
            db::FullRevision {
                rev_no: 2,
                rev_id: "rid-2".into(),
                ts: 1_700_000_002,
                author: 7,
                content: "two".into(),
            },
        ];
        pack::write_archive(File::create(&legacy).unwrap(), &revisions, 19, |r| {
            revision_txt(&identity, r)
        })
        .unwrap();
        let legacy_bytes = std::fs::read(&legacy).unwrap();
        let legacy_mtime = std::fs::metadata(&legacy).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let stats = publish(&db, "adopt", &out, 19).unwrap();
        assert_eq!(stats.pages_packed, 0);
        assert_eq!(stats.pages_adopted, 1);

        let (_root_sha, root) = root_of(&out);
        let pages = pages_of(&out, &root);
        let sha = pages.pages[0].archive_sha256.clone().unwrap();
        assert_eq!(
            pages.pages[0].archive.as_deref(),
            Some(store::object_ref(ObjectKind::Archives, &sha).as_str())
        );
        let object = store::object_path(&out, ObjectKind::Archives, &sha);
        assert_eq!(std::fs::read(&object).unwrap(), legacy_bytes);
        assert_eq!(
            std::fs::metadata(&object).unwrap().modified().unwrap(),
            legacy_mtime
        );
        let cached: Option<String> = db.with_conn(|conn| {
            conn.query_row(
                "SELECT archive_sha256 FROM out_state WHERE page_id=1234567890",
                [],
                |r| r.get(0),
            )
            .unwrap()
        });
        assert_eq!(cached.as_deref(), Some(sha.as_str()));
    }

    /// NULL hash with no legacy file → exactly one repack.
    #[test]
    fn missing_legacy_archive_repacks_once() {
        let (_dir, db, out) = setup("missing");
        insert_page(&db, 111, "one", "One", "[]");
        insert_rev(&db, 111, 1, Some("v1"));
        set_out_state(&db, 111, 1, None);

        let stats = publish(&db, "missing", &out, 19).unwrap();
        assert_eq!(stats.pages_packed, 1);
        assert_eq!(stats.pages_adopted, 0);

        let (_root_sha, root) = root_of(&out);
        let pages = pages_of(&out, &root);
        let sha = pages.pages[0].archive_sha256.clone().unwrap();
        let object = store::object_path(&out, ObjectKind::Archives, &sha);
        assert!(object.exists());
        // The repacked object is a valid tar+zstd stream.
        let dec = zstd::decode_all(std::fs::read(&object).unwrap().as_slice()).unwrap();
        let mut ar = tar::Archive::new(dec.as_slice());
        let names: Vec<String> = ar
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["r001.txt"]);
    }

    /// Shell ownership: no `meta['shell']` yet publishes the empty shell,
    /// a later `shell.sync` moves the object, and broken JSON aborts
    /// publication with the old HEAD intact.
    #[test]
    fn shell_ownership_and_fallbacks() {
        let (_dir, db, out) = setup("shell-owner");
        insert_page(&db, 111, "one", "One", "[]");
        insert_rev(&db, 111, 1, Some("v1"));

        // No meta['shell']: publish the empty shell.
        publish(&db, "shell-owner", &out, 19).unwrap();
        let (_root_sha, root) = root_of(&out);
        assert_eq!(
            std::fs::read(store::object_path(&out, ObjectKind::Shell, &root.shell)).unwrap(),
            Shell::default().to_text().as_bytes()
        );
        assert!(!out.join("shell").exists());

        // meta['shell'] (set by shell.sync) becomes the shell object.
        let shell = Shell {
            title: "Meta".into(),
            subtitle: "Sub".into(),
            landing: String::new(),
            theme_roots: vec![],
        };
        db.meta_set("shell", &serde_json::to_string(&shell).unwrap())
            .unwrap();
        publish(&db, "shell-owner", &out, 19).unwrap();
        let (head_before_bad, root) = root_of(&out);
        assert_eq!(
            std::fs::read(store::object_path(&out, ObjectKind::Shell, &root.shell)).unwrap(),
            shell.to_text().as_bytes()
        );

        // Broken JSON is fatal; HEAD does not move.
        db.meta_set("shell", "{not json").unwrap();
        assert!(publish(&db, "shell-owner", &out, 19).is_err());
        assert_eq!(head(&out), head_before_bad);
    }

    /// A second publication against the same site fails immediately while
    /// the lock is held, and succeeds after it is released.
    #[test]
    fn concurrent_publish_is_rejected() {
        let (_dir, db, out) = setup("lock");
        std::fs::create_dir_all(&out).unwrap();
        let held = store::PublishLock::acquire(&out).unwrap();
        assert!(publish(&db, "lock", &out, 19).is_err());
        drop(held);
        assert!(publish(&db, "lock", &out, 19).is_ok());
    }

    /// Two sites under one `out/` are fully independent.
    #[test]
    fn sites_are_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let out_root = dir.path().join("out");
        let mut caches = Vec::new();
        for site in ["alpha", "beta"] {
            let db = Db::open(&dir.path().join("meta").join(site).join("site.db")).unwrap();
            insert_page(&db, 1, "start", "Start", "[]");
            insert_rev(&db, 1, 1, Some(site));
            let out = out_root.join(site);
            publish(&db, site, &out, 19).unwrap();
            assert!(out.join("HEAD").is_file());
            assert!(out.join("roots").is_dir());
            assert!(out.join("objects").is_dir());
            caches.push((db, out));
        }
        assert!(!out_root.join("HEAD").exists());
        assert!(out_root.join("alpha").join("HEAD").exists());
        assert!(out_root.join("beta").join("HEAD").exists());
        let (_, alpha_root) = root_of(&out_root.join("alpha"));
        let (_, beta_root) = root_of(&out_root.join("beta"));
        assert_eq!(alpha_root.site, "alpha");
        assert_eq!(beta_root.site, "beta");
    }

    /// No temp files survive a successful publication (including adoption
    /// and fresh object writes).
    #[test]
    fn publish_leaves_no_temp_files() {
        let (_dir, db, out) = setup("no-litter");
        add_content_page(&db);
        publish(&db, "no-litter", &out, 19).unwrap();
        publish(&db, "no-litter", &out, 19).unwrap();

        fn walk(dir: &Path) -> Vec<PathBuf> {
            let mut files = Vec::new();
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    files.extend(walk(&path));
                } else {
                    files.push(path);
                }
            }
            files
        }
        for path in walk(&out) {
            let name = path.file_name().unwrap().to_string_lossy();
            assert!(!name.contains(".tmp"), "temp litter: {}", path.display());
        }
    }

    fn add_content_page(db: &Db) {
        insert_page(db, 111, "one", "One", "[]");
        insert_rev(db, 111, 1, Some("v1"));
    }

    fn blobs_sha(bytes: &[u8]) -> String {
        store::sha256_hex(bytes)
    }
}
