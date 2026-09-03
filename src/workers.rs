//! Job execution — each worker performs one logical operation and returns
//! either completion (effects + optional reschedule, applied atomically) or
//! a failure for the queue's retry machinery.
//!
//! v1's three-stage actor pipeline (discovery → revision → files) collapses
//! into: `discover` enqueues `page.sync`, `page.sync` enqueues
//! `revision.fetch` + `file.fetch`, each revision's content enqueues more
//! `file.fetch`. The database IS the pipeline.

use std::{collections::HashSet, path::PathBuf};

use rusqlite::{Transaction, params};
use serde::Deserialize;

use crate::{
    blobs,
    config::Config,
    db::{self, Db, enqueue_on},
    http::Wikidot,
    jobs,
    model::{ChangeEntry, FetchError, Slug},
    out, parsers,
    wikidot::SiteApi,
};

/// Atomic effects of a completed job.
pub type Effects = Box<dyn FnOnce(&Transaction) -> anyhow::Result<()> + Send>;

pub enum Outcome {
    /// Complete a durable job: effects + job-state change in one
    /// transaction. `resched` re-arms periodic jobs `interval` seconds out
    /// instead of finishing.
    Complete {
        resched: Option<i64>,
        effects: Effects,
    },
    /// Complete an ephemeral (event) job: effects + row deletion in one
    /// transaction — signal jobs have no `done` state.
    CompleteEphemeral { effects: Effects },
    /// Hand to the queue's retry/dead-letter machinery.
    Fail { error: String, permanent: bool },
}

fn complete(resched: Option<i64>, effects: Effects) -> Outcome {
    Outcome::Complete { resched, effects }
}

fn complete_ephemeral(effects: Effects) -> Outcome {
    Outcome::CompleteEphemeral { effects }
}

fn noop_effects() -> Effects {
    Box::new(|_| Ok(()))
}

fn fail_err(e: &FetchError) -> Outcome {
    Outcome::Fail {
        error: e.to_string(),
        permanent: e.permanent(),
    }
}

pub async fn run_job(db: &Db, cfg: &Config, wik: &Wikidot, site: &str, job: &db::Job) -> Outcome {
    let api = SiteApi::new(wik, site);
    let out_dir = cfg.site_out(site);
    let result = match job.kind.as_str() {
        jobs::kind::DISCOVER => discover(db, cfg, &api).await,
        jobs::kind::PAGE_SYNC => page_sync(db, &api, job).await,
        jobs::kind::REVISION_FETCH => revision_fetch(db, &api, out_dir, job).await,
        jobs::kind::FILE_FETCH => file_fetch(db, &api, &out_dir, job).await,
        jobs::kind::SHELL_SYNC => shell_sync(db, cfg, &api).await,
        jobs::kind::THEME_CRAWL => theme_crawl(db, &api, &out_dir, job).await,
        jobs::kind::OUT_UPDATE => out_update(db, cfg, site).await,
        jobs::kind::BACKFILL => backfill(cfg),
        other => Ok(Outcome::Fail {
            error: format!("unknown job kind '{other}'"),
            permanent: true,
        }),
    };
    match result {
        Ok(outcome) => outcome,
        Err(e) => fail_err(&e),
    }
}

// ── discover: RecentChanges watermark scan ──
//
// Snapshot page 1 first: its head becomes the next watermark BEFORE any
// collection happens, so changes arriving mid-scan are strictly newer than
// it and belong to the next interval. Then walk deeper (page 2, 3, …) until
// the old watermark — or the end of the feed, on a cold start — shows up,
// emitting every entry not older than it. Newest-first walking makes the
// sliding feed OVERLAP between window fetches instead of gapping: oldest-
// first (v1's direction) permanently skips entries that slide across a page
// boundary mid-scan. The whole scan commits once at the end: a crash
// mid-scan re-runs it from the old watermark.

const WATERMARK_KEY: &str = "saved_up_to";

async fn discover(db: &Db, cfg: &Config, api: &SiteApi<'_>) -> Result<Outcome, FetchError> {
    let site = &api.site;
    let wm: Option<ChangeEntry> = db
        .meta_get(WATERMARK_KEY)
        .ok()
        .flatten()
        .and_then(|v| serde_json::from_str(&v).ok());

    let first = api.fetch_site_changes(1, 200).await?;
    if first.is_empty() {
        // Brand-new site, or a transient parse-to-nothing: keep the old
        // watermark and try again next interval.
        tracing::warn!(site, "feed page 1 empty; keeping watermark");
        return Ok(complete(Some(cfg.monitor_interval_s), noop_effects()));
    }
    let mut new_wm = first[0].clone();
    if let Some(w) = &wm
        && new_wm.ts <= w.ts
    {
        // Bogus/future watermark (clock skew): never regress it.
        new_wm = w.clone();
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut reached_wm = absorb_window(site, &first, &wm, &mut seen);
    let mut page = 2i64;
    while !reached_wm {
        let window = api.fetch_site_changes(page, 200).await?;
        if window.is_empty() {
            // Ran off the feed: cold start reached the end, or a watermark
            // older than the feed's entire history (catch-up collected
            // everything there is).
            break;
        }
        tracing::debug!(site, page, entries = window.len(), "feed window");
        reached_wm = absorb_window(site, &window, &wm, &mut seen);
        page += 1;
    }

    let n = seen.len();
    let slugs: Vec<String> = seen.into_iter().collect();
    let watermark = Some(new_wm);
    let effects: Effects = Box::new(move |tx| {
        if let Some(w) = &watermark {
            tx.execute(
                "INSERT INTO meta(key, value) VALUES(?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![WATERMARK_KEY, serde_json::to_string(w)?],
            )?;
        }
        let jobs: Vec<_> = slugs.iter().map(|s| jobs::page_sync(s)).collect();
        enqueue_on(tx, &jobs)?;
        Ok(())
    });
    tracing::info!(site, pages = n, "discovery scan complete");
    Ok(complete(Some(cfg.monitor_interval_s), effects))
}

/// v1 take_while semantics: collect a window's entries until the watermark
/// entry (or anything older) shows up; returns whether the walk reached it.
fn absorb_window(
    site: &str,
    window: &[ChangeEntry],
    wm: &Option<ChangeEntry>,
    seen: &mut HashSet<String>,
) -> bool {
    for entry in window {
        if let Some(w) = wm
            && (entry == w || entry.ts < w.ts)
        {
            return true;
        }
        if seen.insert(entry.slug.as_str()) {
            tracing::debug!(site, slug = %entry.slug.as_str(), ts = entry.ts, "change discovered");
        }
    }
    false
}

// ── page.sync: resolve a slug → page row + revision metas + file listing ──

async fn page_sync(db: &Db, api: &SiteApi<'_>, job: &db::Job) -> Result<Outcome, FetchError> {
    #[derive(Deserialize)]
    struct P {
        slug: String,
    }
    let p: P = serde_json::from_str(&job.payload)
        .map_err(|e| FetchError::Parse(format!("bad payload: {e}")))?;
    let slug = Slug::parse(&p.slug);

    let (title, tags, page_id_str, _html) = api.fetch_page(&slug).await?;
    let page_id: i64 = page_id_str
        .parse()
        .map_err(|_| FetchError::Parse(format!("non-numeric page_id '{page_id_str}'")))?;

    let max_rev = db.max_rev(page_id).unwrap_or(-1);
    let new_revs = api.fetch_revisions_above(&page_id_str, max_rev).await?;
    let listing = api.fetch_page_files(&page_id_str).await?;

    tracing::info!(
        site = %api.site, slug = %slug.as_str(), page_id,
        new_revs = new_revs.len(), files = listing.len(),
        "page synced"
    );

    let effects: Effects = Box::new(move |tx| {
        tx.execute(
            "INSERT INTO pages(page_id, slug, title, tags, updated_at, discovered_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(page_id) DO UPDATE SET
               slug=excluded.slug, title=excluded.title, tags=excluded.tags,
               updated_at=COALESCE(excluded.updated_at, pages.updated_at)",
            params![
                page_id,
                slug.as_str(),
                title,
                serde_json::to_string(&tags)?,
                new_revs.iter().map(|r| r.ts).max(),
                db::now()
            ],
        )?;
        let mut rev_jobs = Vec::new();
        // v1 high/low split: only the page's newest revision rides the head
        // priority; everything older is intermediate-history backlog.
        let head_rev = new_revs.iter().map(|r| r.rev_no).max();
        for r in &new_revs {
            tx.execute(
                "INSERT OR IGNORE INTO revisions(page_id, rev_no, rev_id, ts, author)
                 VALUES(?1, ?2, ?3, ?4, ?5)",
                params![page_id, r.rev_no, r.rev_id, r.ts, r.author],
            )?;
            // Always enqueue the fresh ones: rows that already have content
            // complete as a cheap no-op. resurrect re-arms a dead job if the
            // row was hand-repaired; routine revival of dead fetches is the
            // periodic backfill's job (revs behind max_rev never reappear
            // in new_revs).
            let mut j =
                jobs::revision_fetch(page_id, r.rev_no, &r.rev_id, Some(r.rev_no) == head_rev);
            j.resurrect = true;
            rev_jobs.push(j);
        }
        enqueue_on(tx, &rev_jobs)?;
        let mut file_jobs = Vec::new();
        for path in &listing {
            tx.execute(
                "INSERT OR IGNORE INTO files(path, first_seen) VALUES(?1, ?2)",
                params![path, db::now()],
            )?;
            file_jobs.push(jobs::file_fetch(path));
        }
        enqueue_on(tx, &file_jobs)?;
        Ok(())
    });
    Ok(complete_ephemeral(effects))
}

// ── revision.fetch: one revision's wikitext ──

async fn revision_fetch(
    db: &Db,
    api: &SiteApi<'_>,
    _out_dir: PathBuf,
    job: &db::Job,
) -> Result<Outcome, FetchError> {
    #[derive(Deserialize)]
    struct P {
        page_id: i64,
        rev_no: i64,
        rev_id: String,
    }
    let p: P = serde_json::from_str(&job.payload)
        .map_err(|e| FetchError::Parse(format!("bad payload: {e}")))?;

    // Fast path: already fetched, or recorded as permanently unobtainable —
    // denied rows are terminal and must not be re-attempted across rescans.
    if let Some((Some(_content), _, _)) = db.revision_state(p.page_id, p.rev_no).ok().flatten() {
        return Ok(complete(None, noop_effects()));
    }
    if db.is_denied(p.page_id, p.rev_no).unwrap_or(false) {
        return Ok(complete(None, noop_effects()));
    }

    let source = match api.fetch_revision_source(&p.rev_id).await {
        Ok(s) => s,
        Err(FetchError::NotFound) => {
            // A 404 from the AJAX connector is site/CDN weirdness, not
            // evidence the revision is gone (the listing just vouched for
            // it): surface as a transient error so the queue retries — and
            // the periodic backfill keeps dead-lettered attempts alive.
            return Err(FetchError::Http(format!(
                "HTTP 404 fetching source of rev {}",
                p.rev_id
            )));
        }
        Err(FetchError::Forbidden) => {
            // Permanently private revision: record it (v1 `revs_denied`).
            let (ts, author) = db
                .revision_state(p.page_id, p.rev_no)
                .ok()
                .flatten()
                .map(|(_, ts, author)| (ts, author))
                .unwrap_or((0, 0));
            let effects: Effects = Box::new(move |tx| {
                tx.execute(
                    "INSERT OR REPLACE INTO denied_revisions(page_id, rev_no, rev_id, ts, author, reason)
                     VALUES(?1, ?2, ?3, ?4, ?5, 'no_permission')",
                    params![p.page_id, p.rev_no, p.rev_id, ts, author],
                )?;
                Ok(())
            });
            tracing::warn!(site = %api.site, page_id = p.page_id, rev = p.rev_no, "revision denied");
            return Ok(complete(None, effects));
        }
        Err(e) => return Err(e),
    };

    let links = parsers::extract_file_links(&source);
    let n_links = links.len();
    let fetched_at = db::now();
    let effects: Effects = Box::new(move |tx| {
        let n = tx.execute(
            "UPDATE revisions SET content=?4, fetched_at=?5 WHERE page_id=?1 AND rev_no=?2 AND rev_id=?3",
            params![p.page_id, p.rev_no, p.rev_id, source, fetched_at],
        )?;
        if n == 0 {
            anyhow::bail!("revision row vanished: page {} rev {}", p.page_id, p.rev_no);
        }
        let mut file_jobs = Vec::new();
        for path in &links {
            tx.execute(
                "INSERT OR IGNORE INTO files(path, first_seen) VALUES(?1, ?2)",
                params![path, db::now()],
            )?;
            file_jobs.push(jobs::file_fetch(path));
        }
        enqueue_on(tx, &file_jobs)?;
        Ok(())
    });
    tracing::debug!(site = %api.site, page_id = p.page_id, rev = p.rev_no, links = n_links, "revision fetched");
    Ok(complete(None, effects))
}

// ── file.fetch: one attachment, content-addressed ──

async fn file_fetch(
    db: &Db,
    api: &SiteApi<'_>,
    out_dir: &std::path::Path,
    job: &db::Job,
) -> Result<Outcome, FetchError> {
    #[derive(Deserialize)]
    struct P {
        path: String,
    }
    let p: P = serde_json::from_str(&job.payload)
        .map_err(|e| FetchError::Parse(format!("bad payload: {e}")))?;

    match db.file_status(&p.path).ok().flatten().as_deref() {
        Some("saved") | Some("missing") => return Ok(complete(None, noop_effects())),
        _ => {}
    }

    let (bytes, header_type) = match api.fetch_attachment(&p.path).await {
        Ok(b) => b,
        Err(FetchError::NotFound) => {
            // Deleted upstream (v1 `failed_files`): permanent, recorded.
            let path = p.path.clone();
            let effects: Effects = Box::new(move |tx| {
                tx.execute(
                    "INSERT INTO files(path, status, first_seen) VALUES(?1, 'missing', ?2)
                     ON CONFLICT(path) DO UPDATE SET status='missing'",
                    params![path, db::now()],
                )?;
                Ok(())
            });
            tracing::info!(site = %api.site, path = %p.path, "attachment missing (404)");
            return Ok(complete(None, effects));
        }
        Err(e) => return Err(e),
    };

    let sha = blobs::write_blob(out_dir, &bytes)
        .map_err(|e| FetchError::Http(format!("blob write: {e}")))?;
    // Server's word first; sniff when it said nothing.
    let content_type = header_type.or_else(|| blobs::sniff_content_type(&bytes, &p.path));
    let url = format!("http://{}.wikidot.com/{}", api.site, p.path);
    let path = p.path.clone();
    let size = bytes.len() as i64;
    let effects: Effects = Box::new(move |tx| {
        tx.execute(
            "INSERT INTO files(path, url, sha256, size, status, first_seen, saved_at, content_type)
             VALUES(?1, ?2, ?3, ?4, 'saved', ?5, ?6, ?7)
             ON CONFLICT(path) DO UPDATE SET
               url=excluded.url, sha256=excluded.sha256, size=excluded.size,
               status='saved', saved_at=excluded.saved_at,
               content_type=excluded.content_type",
            params![path, url, sha, size, db::now(), db::now(), content_type],
        )?;
        Ok(())
    });
    tracing::debug!(site = %api.site, path = %p.path, size, "attachment saved");
    Ok(complete(None, effects))
}

// ── shell.sync: site display identity ──

const SHELL_KEY: &str = "shell";
const THEME_CRAWLED_KEY: &str = "theme.crawled";

/// Refresh the site shell (title / subtitle / theme roots) from the
/// homepage. The homepage GET doubles as a CSRF warmup for a cold session.
/// Roots are compared against the last completed theme crawl's marker:
/// changed roots (or a crawl that ended with failures) enqueue
/// `theme.crawl`; a themeless site never crawls.
async fn shell_sync(db: &Db, cfg: &Config, api: &SiteApi<'_>) -> Result<Outcome, FetchError> {
    let site = api.site.as_str();
    let out_dir = cfg.site_out(site);
    let (_page_title, _tags, _page_id, html) = api.fetch_page(&Slug::parse("")).await?;
    let title = parsers::extract_site_title(&html);
    let subtitle = parsers::extract_site_subtitle(&html);
    let theme_roots = parsers::plan_theme_roots(&html);
    let shell = out::Shell {
        title,
        subtitle,
        theme_roots: theme_roots.clone(),
    };

    // Marker from the last *completed* crawl: {roots, failed}. A crawl
    // that crashed or died left no (or a stale) marker and is re-armed.
    let needs_crawl = !theme_roots.is_empty()
        && match db.meta_get(THEME_CRAWLED_KEY).ok().flatten() {
            None => true,
            Some(m) => match serde_json::from_str::<ThemeMarker>(&m) {
                Ok(mk) => {
                    let mut planned = theme_roots.clone();
                    planned.sort();
                    mk.roots != planned || !mk.failed.is_empty()
                }
                Err(_) => true,
            },
        };

    // Derived file first (idempotent, byte-stable): a crash between the
    // write and the meta commit just rewrites the same bytes next run.
    out::write_shell(&out_dir, &shell)
        .map_err(|e| FetchError::Http(format!("shell write: {e}")))?;

    let payload = serde_json::to_string(&shell)
        .map_err(|e| FetchError::Http(format!("shell encode: {e}")))?;
    tracing::info!(site, roots = theme_roots.len(), needs_crawl, "shell synced");
    let effects: Effects = Box::new(move |tx| {
        tx.execute(
            "INSERT INTO meta(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![SHELL_KEY, payload],
        )?;
        if needs_crawl {
            enqueue_on(tx, &[jobs::theme_crawl(&theme_roots)])?;
        }
        Ok(())
    });
    Ok(complete(Some(cfg.shell_interval_s), effects))
}

#[derive(serde::Serialize, Deserialize)]
struct ThemeMarker {
    /// Crawled roots, sorted for byte-stability.
    roots: Vec<String>,
    /// Absolute URLs that yielded nothing (fetch error or 404). Non-empty
    /// keeps the crawl re-arming on every shell.sync — cheap, because
    /// everything already saved is read back from its blob, so a retry
    /// costs only the dead URL itself.
    failed: Vec<String>,
}

// ── theme.crawl: evacuate the theme @import / url() graph ──

/// One files-row upsert produced during the crawl:
/// (path, url, sha256, size, content_type).
type SavedRow = (String, String, String, i64, Option<String>);

async fn theme_crawl(
    db: &Db,
    api: &SiteApi<'_>,
    out_dir: &std::path::Path,
    job: &db::Job,
) -> Result<Outcome, FetchError> {
    #[derive(Deserialize)]
    struct P {
        roots: Vec<String>,
    }
    let p: P = serde_json::from_str(&job.payload)
        .map_err(|e| FetchError::Parse(format!("bad payload: {e}")))?;

    let saved: std::sync::Arc<parking_lot::Mutex<Vec<SavedRow>>> = Default::default();
    let missing: std::sync::Arc<parking_lot::Mutex<Vec<String>>> = Default::default();
    let from_blob: std::sync::Arc<parking_lot::Mutex<usize>> = Default::default();

    // DB-first CSS resolver: a saved row's body comes straight from its
    // content-addressed blob (zero HTTP); only genuinely-new URLs fetch.
    // Failures return None — the crawl records and moves on (v1 semantics:
    // a dead CDN URL must not dead-letter the whole theme).
    let outcome = crate::theme::crawl(&p.roots, |url| {
        let saved = saved.clone();
        let missing = missing.clone();
        let from_blob = from_blob.clone();
        async move {
            let (path, row_url) = parsers::file_row_paths(&api.site, &url);
            if let Ok(Some((status, sha))) = db.file_row(&path)
                && status == "saved"
                && let Some(sha) = sha
                && let Ok(bytes) = std::fs::read(blobs::blob_path(out_dir, &sha))
            {
                return match String::from_utf8(bytes) {
                    Ok(text) => {
                        *from_blob.lock() += 1;
                        Some(crate::theme::Fetched::Css(text))
                    }
                    // Binary saved earlier (an `@import url(font.woff2)`
                    // masquerader): already archived, don't descend.
                    Err(_) => Some(crate::theme::Fetched::Binary),
                };
            }
            match api.fetch_public(&url).await {
                Ok((bytes, content_type)) => {
                    let size = bytes.len() as i64;
                    match blobs::write_blob(out_dir, &bytes) {
                        Ok(sha) => {
                            saved.lock().push((path, row_url, sha, size, content_type));
                            match String::from_utf8(bytes) {
                                Ok(text) => Some(crate::theme::Fetched::Css(text)),
                                Err(_) => Some(crate::theme::Fetched::Binary),
                            }
                        }
                        // Blob-write failure (disk trouble): failed URL.
                        Err(_) => None,
                    }
                }
                Err(FetchError::NotFound) => {
                    missing.lock().push(path);
                    None
                }
                Err(_) => None,
            }
        }
    })
    .await;

    // url() assets: same store-or-skip, no descending. Fetch and blob-write
    // failures MUST land in marker_failed: a silent drop would write a clean
    // crawl marker and the asset would never be retried.
    let mut asset_ok = 0usize;
    let mut asset_failed: Vec<String> = Vec::new();
    let mut asset_urls = outcome.assets.clone();
    asset_urls.sort();
    asset_urls.dedup();
    for url in &asset_urls {
        let (path, row_url) = parsers::file_row_paths(&api.site, url);
        if let Ok(Some((status, _))) = db.file_row(&path)
            && status == "saved"
        {
            asset_ok += 1;
            continue;
        }
        match api.fetch_public(url).await {
            Ok((bytes, content_type)) => {
                let size = bytes.len() as i64;
                match blobs::write_blob(out_dir, &bytes) {
                    Ok(sha) => {
                        saved.lock().push((path, row_url, sha, size, content_type));
                        asset_ok += 1;
                    }
                    Err(_) => asset_failed.push(url.clone()),
                }
            }
            Err(FetchError::NotFound) => missing.lock().push(path),
            Err(_) => asset_failed.push(url.clone()),
        }
    }

    let saved_rows = std::sync::Arc::try_unwrap(saved)
        .unwrap_or_default()
        .into_inner();
    let missing_paths = std::sync::Arc::try_unwrap(missing)
        .unwrap_or_default()
        .into_inner();
    let read_from_blob = *std::sync::Arc::try_unwrap(from_blob)
        .unwrap_or_default()
        .lock();

    // Marker last, after everything the crawl could do: crash earlier = no
    // marker = re-armed by the next shell.sync.
    let mut marker_failed: Vec<String> = outcome.failed.clone();
    marker_failed.extend(asset_failed);
    for u in &asset_urls {
        let (path, _) = parsers::file_row_paths(&api.site, u);
        if missing_paths.contains(&path) {
            marker_failed.push(u.clone());
        }
    }
    marker_failed.sort();
    marker_failed.dedup();
    let mut marker_roots = p.roots.clone();
    marker_roots.sort();
    let marker = serde_json::to_string(&ThemeMarker {
        roots: marker_roots,
        failed: marker_failed.clone(),
    })
    .map_err(|e| FetchError::Http(format!("marker encode: {e}")))?;

    let n_saved = saved_rows.len();
    let n_missing = missing_paths.len();
    let effects: Effects = Box::new(move |tx| {
        for (path, url, sha, size, content_type) in &saved_rows {
            tx.execute(
                "INSERT INTO files(path, url, sha256, size, status, first_seen, saved_at, content_type)
                 VALUES(?1, ?2, ?3, ?4, 'saved', ?5, ?6, ?7)
                 ON CONFLICT(path) DO UPDATE SET
                   url=excluded.url, sha256=excluded.sha256, size=excluded.size,
                   status='saved', saved_at=excluded.saved_at,
                   content_type=excluded.content_type",
                params![path, url, sha, size, db::now(), db::now(), content_type],
            )?;
        }
        for path in &missing_paths {
            tx.execute(
                "INSERT INTO files(path, status, first_seen) VALUES(?1, 'missing', ?2)
                 ON CONFLICT(path) DO UPDATE SET status='missing'",
                params![path, db::now()],
            )?;
        }
        tx.execute(
            "INSERT INTO meta(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![THEME_CRAWLED_KEY, marker],
        )?;
        Ok(())
    });
    tracing::info!(
        site = %api.site,
        css = outcome.css.len(),
        read_from_blob,
        assets = asset_urls.len(),
        asset_ok,
        fetched = n_saved,
        missing = n_missing,
        failed = marker_failed.len(),
        "theme crawl complete"
    );
    Ok(complete(None, effects))
}

// ── out.update: publish derived artifacts ──

async fn out_update(db: &Db, cfg: &Config, site: &str) -> Result<Outcome, FetchError> {
    // zstd level 19 packing is seconds of CPU per page: keep it off the
    // async workers so it can't stall the runtime driving HTTP.
    let out_dir = cfg.site_out(site);
    let site_name = site.to_string();
    let level = cfg.zstd_level;
    let db = db.clone();
    let stats = tokio::task::spawn_blocking(move || out::publish(&db, &site_name, &out_dir, level))
        .await
        .map_err(|e| FetchError::Http(format!("publish join: {e}")))?
        .map_err(|e| FetchError::Http(format!("publish: {e}")))?;
    tracing::info!(
        site,
        packed = stats.pages_packed,
        skipped = stats.pages_skipped,
        manifests = stats.manifests_rewritten,
        "out/ published"
    );
    Ok(complete(Some(cfg.out_interval_s), noop_effects()))
}

// ── backfill: resurrect dead fetches for known-missing content ──
//
// A dead-lettered fetch job is invisible to re-discovery: page.sync only
// enqueues revisions ABOVE max_rev (dead rows sit at or below it) and its
// file enqueues dedupe against the dead row. Without this sweep, one
// dead-letter strands that content forever. The sweep enqueues with
// `resurrect`: pending/running jobs are untouched, dead ones re-arm with a
// fresh attempt budget, and rows with no job at all get one.

fn backfill(cfg: &Config) -> Result<Outcome, FetchError> {
    let effects: Effects = Box::new(|tx| {
        let mut jobs = Vec::new();
        let mut stmt = tx.prepare(
            "SELECT r.page_id, r.rev_no, r.rev_id,
                    r.rev_no = (SELECT max(rev_no) FROM revisions r2
                                WHERE r2.page_id = r.page_id)
             FROM revisions r
             WHERE r.content IS NULL AND NOT EXISTS (
               SELECT 1 FROM denied_revisions d
               WHERE d.page_id = r.page_id AND d.rev_no = r.rev_no)",
        )?;
        let revs = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, bool>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        for (page_id, rev_no, rev_id, is_head) in &revs {
            let mut j = jobs::revision_fetch(*page_id, *rev_no, rev_id, *is_head);
            j.resurrect = true;
            jobs.push(j);
        }
        let mut stmt = tx.prepare("SELECT path FROM files WHERE status='pending'")?;
        let files = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        for path in &files {
            let mut j = jobs::file_fetch(path);
            j.resurrect = true;
            jobs.push(j);
        }
        enqueue_on(tx, &jobs)?;
        if !jobs.is_empty() {
            tracing::info!(revs = revs.len(), files = files.len(), "backfill armed");
        }
        Ok(())
    });
    Ok(complete(Some(cfg.backfill_interval_s), effects))
}
