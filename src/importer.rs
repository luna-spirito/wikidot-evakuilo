//! One-shot importer: a legacy (v1 Gleam) site tree into the v2 DB + out/.
//!
//! Legacy layout (`legacy/data/<instance>/<site>/`):
//!
//! - `_meta/<a>/<b>/<rest>` — per-page journal: `slug:`/`title:`/`tags:`
//!   header, then `rev_no\trev_id\tts\tauthor` TSV lines for every
//!   *discovered* revision
//! - `_meta/saved_up_to` — discovery watermark singleton
//! - `_pages_by_id/<a>/<b>/<rest>/rNNN.txt` — v1 frontmatter + wikitext
//! - `files/<host>/<segments>` — symlinks into `_files/` (content store)
//! - `_files/<a>/<b>/<rest>` — sha256 blobs, same 2/2/rest layout as v2's
//!   `out/files_ca/`; blobs are HARDLINKED over (same filesystem: instant,
//!   and deleting the legacy tree later keeps the data)
//! - `shell` — title/subtitle/theme_root lines
//! - `_files_modern` — ignored (excluded by prior decision)
//!
//! Everything lands in ONE transaction per site; content-less discovered
//! revisions enqueue `revision.fetch` so the daemon backfills them.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::config::Config;
use crate::db::{self, Db, NewJob};
use crate::jobs;
use crate::model::{ChangeEntry, Slug};
use crate::out;

#[derive(Debug, Default)]
pub struct ImportStats {
    pub pages: usize,
    pub revisions_meta: usize,
    pub revisions_content: usize,
    pub files: usize,
    pub blobs_linked: usize,
    pub revision_jobs: usize,
    pub watermark: bool,
    pub shell: bool,
}

/// Import every configured site found under `from/<instance>/<site>/`.
pub fn run(cfg: &Config, from: &Path) -> Result<()> {
    let repo = from.join(&cfg.instance.name);
    if !repo.is_dir() {
        bail!(
            "no legacy instance directory at {} — check --from",
            repo.display()
        );
    }
    for site in &cfg.instance.sites {
        let site_dir = repo.join(site);
        if !site_dir.is_dir() {
            eprintln!("  {site}: no legacy tree (skipped)");
            continue;
        }
        print!("  {site}: importing …");
        use std::io::Write;
        std::io::stdout().flush().ok();
        let stats =
            import_site(cfg, &site_dir, site).with_context(|| format!("importing {site}"))?;
        println!(
            " {} pages, {} revisions ({} with content), {} files ({} blobs linked), \
             {} revision jobs, watermark={}, shell={}",
            stats.pages,
            stats.revisions_meta,
            stats.revisions_content,
            stats.files,
            stats.blobs_linked,
            stats.revision_jobs,
            stats.watermark,
            stats.shell
        );
    }
    Ok(())
}

fn import_site(cfg: &Config, from: &Path, site: &str) -> Result<ImportStats> {
    let (db, _lock) = Db::open_locked(&cfg.site_db(site))?;
    let out_dir = cfg.site_out(site);
    std::fs::create_dir_all(&out_dir)?;

    let meta_dir = from.join("_meta");
    let by_id_dir = from.join("_pages_by_id");
    let files_dir = from.join("files");

    // 1. Journals → pages + revision metas.
    let mut stats = ImportStats::default();
    let mut jobs_to_enqueue: Vec<NewJob> = Vec::new();
    db.with_conn(|conn| {
        let tx = conn.transaction().map_err(anyhow::Error::from)?;

        for journal in sharded_files(&meta_dir)? {
            let page_id: i64 = sharded_key(&journal, &meta_dir)
                .parse()
                .with_context(|| format!("page_id from {}", journal.display()))?;
            let (slug, title, tags, revs) = parse_journal(&std::fs::read_to_string(&journal)?)?;
            tx.execute(
                "INSERT INTO pages(page_id, slug, title, tags, updated_at, discovered_at)
                 VALUES(?1, ?2, ?3, ?4,
                        (SELECT max(ts) FROM revisions WHERE page_id=?1), ?5)
                 ON CONFLICT(page_id) DO UPDATE SET
                   slug=excluded.slug, title=excluded.title, tags=excluded.tags",
                rusqlite::params![
                    page_id,
                    slug,
                    title,
                    serde_json::to_string(&tags)?,
                    db::now()
                ],
            )?;
            stats.pages += 1;
            for rev in &revs {
                tx.execute(
                    "INSERT OR IGNORE INTO revisions(page_id, rev_no, rev_id, ts, author)
                     VALUES(?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![page_id, rev.rev_no, rev.rev_id, rev.ts, rev.author],
                )?;
                stats.revisions_meta += 1;
            }
        }

        // 2. rN.txt → content (meta rows came from the journals; the
        //    frontmatter is authoritative for the row that receives it).
        for txt in r_files(&by_id_dir)? {
            let page_id: i64 = txt
                .parent()
                .map(|dir| sharded_key(dir, &by_id_dir))
                .with_context(|| format!("page_id from {}", txt.display()))?
                .parse()
                .with_context(|| format!("page_id from {}", txt.display()))?;
            let raw = std::fs::read_to_string(&txt)?;
            let fm = parse_frontmatter(&raw)
                .with_context(|| format!("frontmatter in {}", txt.display()))?;
            if fm.page_id != page_id {
                bail!(
                    "{}: frontmatter page_id {} != dir {}",
                    txt.display(),
                    fm.page_id,
                    page_id
                );
            }
            if fm.site != site {
                bail!(
                    "{}: frontmatter site {} != target site {site} — wrong tree?",
                    txt.display(),
                    fm.site
                );
            }
            tx.execute(
                "INSERT INTO pages(page_id, slug, title, tags, updated_at, discovered_at)
                 VALUES(?1,?2,?3,?4,(SELECT max(ts) FROM revisions WHERE page_id=?1),?5)
                 ON CONFLICT(page_id) DO UPDATE SET
                   slug=excluded.slug, title=excluded.title, tags=excluded.tags",
                rusqlite::params![
                    page_id,
                    fm.slug,
                    fm.title,
                    serde_json::to_string(&fm.tags)?,
                    db::now()
                ],
            )?;
            let n = tx.execute(
                "INSERT INTO revisions(page_id, rev_no, rev_id, ts, author, content, fetched_at)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)
                 ON CONFLICT(page_id, rev_no) DO UPDATE SET
                   rev_id=excluded.rev_id, ts=excluded.ts, author=excluded.author,
                   content=excluded.content, fetched_at=excluded.fetched_at",
                rusqlite::params![
                    page_id,
                    fm.revision,
                    fm.revision_id,
                    fm.timestamp,
                    fm.author,
                    fm.body,
                    db::now()
                ],
            )?;
            stats.revisions_content += n.max(1);
        }

        // 3. Contentless revisions → revision.fetch backfill jobs.
        {
            let mut stmt =
                tx.prepare("SELECT page_id, rev_no, rev_id FROM revisions WHERE content IS NULL")?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            for row in rows {
                let (page_id, rev_no, rev_id) = row?;
                jobs_to_enqueue.push(jobs::revision_fetch(page_id, rev_no, &rev_id));
                stats.revision_jobs += 1;
            }
        }

        // 4. Attachments + theme assets: files/<host>/... symlinks into
        //    _files/ blobs → rows + hardlinked out/files_ca blobs.
        for link in symlink_tree(&files_dir)? {
            let rel = link.strip_prefix(&files_dir)?;
            let mut segments = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            let Some(host) = segments.first().cloned() else {
                continue;
            };
            segments.remove(0);
            let url_path = segments.join("/");
            // Reconstruct the absolute URL, then share the crawler's
            // own-host → site-relative path mapping.
            let full_url = format!("https://{host}/{url_path}");
            let (path, url) = crate::parsers::file_row_paths(site, &full_url);
            let target = std::fs::read_link(&link)?;
            let blob = if target.is_absolute() {
                target
            } else {
                link.parent().unwrap_or(Path::new(".")).join(target)
            };
            // The blob lives at `_files/<a>/<b>/<rest>` where the sha256 is
            // the concatenation a+b+rest (same 2/2/rest convention as v2's
            // files_ca — only the final path components carry the key).
            let mut comps = blob
                .components()
                .rev()
                .take(3)
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            comps.reverse(); // [a, b, rest]
            let sha = if comps.len() == 3 {
                comps.concat()
            } else {
                String::new()
            };
            if sha.len() != 64 {
                // Not the sharded convention (hand-placed file?) — record
                // the reference but don't guess a blob location.
                let size = std::fs::metadata(&blob)
                    .map(|m| m.len() as i64)
                    .unwrap_or(0);
                tx.execute(
                    "INSERT INTO files(path, url, sha256, size, status, first_seen)
                     VALUES(?1,?2,NULL,?3,'pending',?4)
                     ON CONFLICT(path) DO NOTHING",
                    rusqlite::params![path, url, size, db::now()],
                )?;
                stats.files += 1;
                continue;
            }
            // Layouts match (2/2/rest of sha) — hardlink when possible.
            let dest = out_dir
                .join("files_ca")
                .join(&sha[..2])
                .join(&sha[2..4])
                .join(&sha[4..]);
            let mut linked = false;
            if dest.exists() {
                linked = true;
            } else if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
                if std::fs::hard_link(&blob, &dest).is_ok() {
                    linked = true;
                    stats.blobs_linked += 1;
                }
            }
            if !linked {
                // Cross-device or exotic: fall back to a copy.
                std::fs::copy(&blob, &dest)?;
                stats.blobs_linked += 1;
            }
            let size = std::fs::metadata(&blob)
                .map(|m| m.len() as i64)
                .unwrap_or(0);
            tx.execute(
                "INSERT INTO files(path, url, sha256, size, status, first_seen, saved_at)
                 VALUES(?1,?2,?3,?4,'saved',?5,?6)
                 ON CONFLICT(path) DO UPDATE SET
                   sha256=excluded.sha256, size=excluded.size, status='saved'",
                rusqlite::params![path, url, sha, size, db::now(), db::now()],
            )?;
            stats.files += 1;
        }

        // 5. Watermark: v1 singleton → v2 meta JSON (same shape the daemon
        //    reads/writes) so incremental discovery continues where v1 left.
        let wm_file = meta_dir.join("saved_up_to");
        if wm_file.is_file()
            && let Some(entry) = parse_watermark(&std::fs::read_to_string(&wm_file)?)
        {
            tx.execute(
                "INSERT INTO meta(key, value) VALUES('saved_up_to', ?1)
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                rusqlite::params![serde_json::to_string(&entry)?],
            )?;
            stats.watermark = true;
        }

        // 6. Shell.
        let shell_file = from.join("shell");
        if shell_file.is_file() {
            let shell = parse_shell(&std::fs::read_to_string(&shell_file)?);
            tx.execute(
                "INSERT INTO meta(key, value) VALUES('shell', ?1)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                rusqlite::params![serde_json::to_string(&shell)?],
            )?;
            out::write_shell(&out_dir, &shell)?;
            stats.shell = true;
        }

        if !jobs_to_enqueue.is_empty() {
            crate::db::enqueue_on(&tx, &jobs_to_enqueue)?;
        }
        // updated_at was computed before the revision rows existed —
        // backfill it now that everything is inserted.
        tx.execute(
            "UPDATE pages SET updated_at=(SELECT max(ts) FROM revisions r
                                        WHERE r.page_id=pages.page_id)
             WHERE updated_at IS NULL",
            [],
        )?;
        tx.commit().map_err(anyhow::Error::from)?;
        Ok::<(), anyhow::Error>(())
    })?;

    // 7. Publish everything we just imported.
    let pub_stats =
        out::publish(&db, site, &out_dir, cfg.zstd_level).context("publishing imported data")?;
    println!(
        "      published: {} page archives ({} skipped), {} manifests",
        pub_stats.pages_packed, pub_stats.pages_skipped, pub_stats.manifests_rewritten
    );
    Ok(stats)
}

// ── Legacy format parsers ──

/// One `_meta` journal: header + TSV revision lines.
fn parse_journal(raw: &str) -> Result<(String, String, Vec<String>, Vec<MetaRev>)> {
    let mut slug = String::new();
    let mut title = String::new();
    let mut tags = Vec::new();
    let mut revs = Vec::new();
    for line in raw.lines() {
        if line.starts_with('\t') || line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("slug: ") {
            slug = rest.trim().to_string();
        } else if let Some(rest) = line.strip_prefix("title: ") {
            title = unquote(rest.trim());
        } else if let Some(rest) = line.strip_prefix("tags: ") {
            tags = parse_quoted_list(rest.trim());
        } else if let Some((rev_no, tail)) = line.split_once('\t') {
            // rev_no \t rev_id \t ts \t author
            let mut it = tail.split('\t');
            let (Some(rev_id), Some(ts), Some(author)) = (it.next(), it.next(), it.next()) else {
                continue;
            };
            if let (Ok(rev_no), Ok(ts), Ok(author)) = (
                rev_no.parse::<i64>(),
                ts.parse::<i64>(),
                author.parse::<i64>(),
            ) {
                revs.push(MetaRev {
                    rev_no,
                    rev_id: rev_id.to_string(),
                    ts,
                    author,
                });
            }
        }
    }
    Ok((slug, title, tags, revs))
}

struct MetaRev {
    rev_no: i64,
    rev_id: String,
    ts: i64,
    author: i64,
}

/// Parsed v1 `rN.txt` frontmatter + body.
#[derive(Debug, Deserialize)]
pub struct Frontmatter {
    pub title: String,
    pub tags: Vec<String>,
    pub page_id: i64,
    pub site: String,
    pub slug: String,
    pub revision: i64,
    pub revision_id: String,
    pub author: i64,
    pub timestamp: i64,
    pub body: String,
}

/// Parse `---\n<yaml>\n---\n<body>` with v1's known flat keys.
pub fn parse_frontmatter(raw: &str) -> Result<Frontmatter> {
    let rest = raw.strip_prefix("---\n").context("no leading ---")?;
    let (yaml, body) = rest.split_once("\n---\n").context("no closing ---")?;
    let mut kv: HashMap<String, String> = HashMap::new();
    for line in yaml.lines() {
        if let Some((k, v)) = line.split_once(": ") {
            kv.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    let need = |k: &str| {
        kv.get(k)
            .map(|v| unquote(v))
            .with_context(|| format!("key {k}"))
    };
    Ok(Frontmatter {
        title: need("title")?,
        tags: kv
            .get("tags")
            .map(|v| parse_quoted_list(v))
            .unwrap_or_default(),
        page_id: need("page_id")?.parse().context("page_id")?,
        site: need("site")?,
        slug: need("slug")?,
        revision: need("revision")?.parse().context("revision")?,
        revision_id: need("revision_id")?,
        author: need("author")?.parse().context("author")?,
        timestamp: need("timestamp")?.parse().context("timestamp")?,
        // v1 appended one trailing newline when writing; strip it back off.
        body: body.strip_suffix('\n').unwrap_or(body).to_string(),
    })
}

fn unquote(s: &str) -> String {
    let s = s.strip_prefix('"').unwrap_or(s);
    let s = s.strip_suffix('"').unwrap_or(s);
    s.replace("\\\"", "\"")
        .replace("\\\\", "\\")
        .replace("\\n", "\n")
        .replace("\\t", "\t")
}

/// `["a", "b"]` (v1 always double-quotes list items).
fn parse_quoted_list(s: &str) -> Vec<String> {
    let inner = s.trim().trim_start_matches('[').trim_end_matches(']');
    inner
        .split(", ")
        .map(|item| unquote(item.trim()))
        .filter(|item| !item.is_empty())
        .collect()
}

/// v1 `saved_up_to` singleton → ChangeEntry.
fn parse_watermark(raw: &str) -> Option<ChangeEntry> {
    let mut kv: HashMap<&str, &str> = HashMap::new();
    for line in raw.lines() {
        if let Some((k, v)) = line.split_once(": ") {
            kv.insert(k.trim(), v.trim());
        }
    }
    Some(ChangeEntry {
        slug: Slug {
            category: kv.get("category").map(|c| unquote(c)),
            name: unquote(kv.get("slug")?),
        },
        rev_no: kv.get("revision")?.parse().ok()?,
        ts: kv.get("timestamp")?.parse().ok()?,
        author: kv.get("author")?.parse().ok()?,
    })
}

/// v1 `shell` file: `title:` / `subtitle:` / `theme_root:` lines.
fn parse_shell(raw: &str) -> out::Shell {
    let mut title = String::new();
    let mut subtitle = String::new();
    let mut theme_roots = Vec::new();
    for line in raw.lines() {
        if let Some(v) = line.strip_prefix("title: ") {
            title = unquote(v.trim());
        } else if let Some(v) = line.strip_prefix("subtitle: ") {
            subtitle = unquote(v.trim());
        } else if let Some(v) = line.strip_prefix("theme_root: ") {
            theme_roots.push(v.trim().to_string());
        }
    }
    out::Shell {
        title,
        subtitle,
        theme_roots,
    }
}

// ── Tree walkers ──

/// A sharded key (`<a>/<b>/<rest>`) reconstructed by concatenation: the
/// directory/file names hold only their own slice of the key.
fn sharded_key(path: &Path, base: &Path) -> String {
    path.strip_prefix(base)
        .map(|rel| {
            rel.components()
                .filter_map(|c| c.as_os_str().to_str())
                .collect::<String>()
        })
        .unwrap_or_default()
}

/// `_meta/<a>/<b>/<rest>` journal files (singleton files at the root are
/// skipped — journals live exactly two levels down).
fn sharded_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).with_context(|| format!("reading {}", d.display()))? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.components().count() - dir.components().count() == 3 {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Every `r*.txt` under `_pages_by_id` (sorted for determinism).
fn r_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        if !d.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(&d).with_context(|| format!("reading {}", d.display()))? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .map(|n| n.to_string_lossy().starts_with('r'))
                .unwrap_or(false)
            {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Every symlink under `files/` (sorted).
fn symlink_tree(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).with_context(|| format!("reading {}", d.display()))? {
            let entry = entry?;
            let path = entry.path();
            let is_link = entry.file_type().map(|t| t.is_symlink()).unwrap_or(false);
            if is_link {
                out.push(path);
            } else if path.is_dir() {
                stack.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_parses_header_and_tsv() {
        let raw = "slug: component:world-contest-theme\ntitle: \"World Contest Theme\"\ntags: [\"theme\"]\n19\t1523155270\t1721597426\t5982545\n18\t1462171625\t1655746574\t3118961\n";
        let (slug, title, tags, revs) = parse_journal(raw).unwrap();
        assert_eq!(slug, "component:world-contest-theme");
        assert_eq!(title, "World Contest Theme");
        assert_eq!(tags, ["theme"]);
        assert_eq!(revs.len(), 2);
        assert_eq!(revs[0].rev_no, 19);
        assert_eq!(revs[0].rev_id, "1523155270");
        assert_eq!(revs[0].ts, 1721597426);
        assert_eq!(revs[0].author, 5982545);
    }

    #[test]
    fn frontmatter_roundtrips_v1_writer() {
        let txt = out::revision_txt(
            &out::PageIdentity {
                site: "rpcauthority",
                page_id: "106024589",
                slug: "a-013",
                title: "A-013",
                tags: &["document".into(), "gear".into()],
            },
            &crate::db::FullRevision {
                rev_no: 36,
                rev_id: "1530315817".into(),
                ts: 1748703934,
                author: 9487909,
                content: "[[div]]\nbody \"quoted\" and \\ slash\n[[/div]]".into(),
            },
        );
        let fm = parse_frontmatter(&txt).unwrap();
        assert_eq!(fm.title, "A-013");
        assert_eq!(fm.tags, ["document", "gear"]);
        assert_eq!(fm.page_id, 106024589);
        assert_eq!(fm.slug, "a-013");
        assert_eq!(fm.revision, 36);
        assert_eq!(fm.revision_id, "1530315817");
        assert_eq!(fm.author, 9487909);
        assert_eq!(fm.timestamp, 1748703934);
        assert_eq!(fm.body, "[[div]]\nbody \"quoted\" and \\ slash\n[[/div]]");
    }

    #[test]
    fn watermark_parses_v1_singleton() {
        let raw = "slug: site-168\nrevision: 7\ntimestamp: 1786638194\nauthor: 42\n";
        let wm = parse_watermark(raw).unwrap();
        assert_eq!(wm.slug.name, "site-168");
        assert!(wm.slug.category.is_none());
        assert_eq!(wm.rev_no, 7);
        assert_eq!(wm.ts, 1786638194);
        assert_eq!(wm.author, 42);

        let raw_cat = "slug: thing\ncategory: draft\nrevision: 1\ntimestamp: 2\nauthor: 3\n";
        let wm = parse_watermark(raw_cat).unwrap();
        assert_eq!(wm.slug.category.as_deref(), Some("draft"));
    }

    #[test]
    fn shell_parses_v1_file() {
        let raw = "title: \"RPC Authority\"\nsubtitle: \"Research, Protection, Containment\"\ntheme_root: files/cdn.jsdelivr.net/gh/x/style.css\n";
        let shell = parse_shell(raw);
        assert_eq!(shell.title, "RPC Authority");
        assert_eq!(shell.theme_roots, ["files/cdn.jsdelivr.net/gh/x/style.css"]);
        // And it re-serializes losslessly through the v2 writer.
        assert_eq!(shell.to_text(), raw);
    }
}
