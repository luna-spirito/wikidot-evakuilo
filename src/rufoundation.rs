//! RuFoundation sites (the Django/FTL Wikidot-successor platform, e.g.
//! backroomswiki.ru) — a JSON API instead of AJAX scraping.
//!
//! Same job taxonomy, DB shapes and publication pipeline as the Wikidot
//! workers; only the transport and the discovery model differ:
//!
//! * `discover` — `GET /api/articles` lists every page with its
//!   `updatedAt` watermark; pages newer than their stored row enqueue
//!   `page.sync`. (Verified live: `updatedAt` equals the newest log
//!   entry's `createdAt`, so the diff is stable — `page.sync` stores the
//!   listing value it was triggered by, and a page never re-syncs for a
//!   watermark it already recorded.)
//! * `page.sync` — the article JSON (identity, tags, head `source`: the
//!   head revision arrives free, no separate fetch), the full event log
//!   (`/log?all=1`) and the file listing. A log entry names a source
//!   snapshot only when it carries `meta.version_id` — directly, or via
//!   `meta.source` on a revert: tag edits, renames and file ops bump
//!   `rev_number` but archive nothing.
//! * `revision.fetch` — `GET /api/articles/{slug}/version?revNum=N`. A 404
//!   is TERMINAL here (verified live: pruned versions answer 404 with
//!   `{"error":"Версии с данным идентификатором не существует"}`), unlike
//!   the Wikidot connector's 404 weirdness.
//! * `file.fetch` — `/local--files/{slug}/{name}` on the site host; the
//!   server 302s to its media domain. Row keys stay on the site host (see
//!   `parsers::file_row_paths_own`).
//! * `shell.sync` — the homepage `<title>`. RuFoundation has no theme CSS
//!   graph, so `theme_roots` is empty and `theme.crawl` never arms.
//!
//! Anonymous access archives public sites fully; `api_key` (a `type="bot"`
//! user's bearer token, see `BotAuthTokenMiddleware`) additionally
//! unlocks hidden categories.

use std::collections::HashMap;

use rusqlite::params;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::{
    blobs,
    config::Config,
    db::{self, Db, NewJob, enqueue_on},
    http::{HostHealth, Wikidot},
    jobs,
    model::{FetchError, FetchResult, Slug},
    out, parsers,
    workers::{self, Effects, Outcome},
};

// ── API wire types ──

/// `GET /api/articles` — one list entry. (The entry's `uid` is ignored:
/// identity comes from the article fetch in `page.sync`.)
#[derive(Debug, Deserialize)]
struct ArticleEntry {
    #[serde(rename = "pageId")]
    page_id: String,
    #[serde(rename = "updatedAt")]
    updated_at: String,
}

/// `GET /api/articles/{slug}` — page identity + head source.
#[derive(Debug, Deserialize)]
struct Article {
    uid: i64,
    #[serde(default)]
    title: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    source: String,
}

/// `GET /api/articles/{slug}/log?all=1`.
#[derive(Debug, Deserialize)]
struct Log {
    #[serde(default)]
    entries: Vec<LogEntry>,
}

#[derive(Debug, Deserialize)]
struct LogEntry {
    #[serde(rename = "revNumber")]
    rev_number: i64,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(default)]
    user: Option<LogUser>,
    #[serde(default)]
    meta: LogMeta,
}

#[derive(Debug, Deserialize)]
struct LogUser {
    id: i64,
}

#[derive(Debug, Default, Deserialize)]
struct LogMeta {
    #[serde(default)]
    version_id: Option<i64>,
    #[serde(default)]
    source: Option<MetaSource>,
}

#[derive(Debug, Deserialize)]
struct MetaSource {
    version_id: i64,
}

impl LogEntry {
    /// The version this entry archived, if it is a source snapshot:
    /// `meta.version_id` directly, or `meta.source.version_id` on a
    /// revert. Everything else (tags, renames, file ops, `wikidot`
    /// migration markers) bumps `rev_number` without archiving a source.
    fn version_id(&self) -> Option<i64> {
        self.meta
            .version_id
            .or_else(|| self.meta.source.as_ref().map(|s| s.version_id))
    }

    fn ts(&self) -> i64 {
        parse_iso8601(&self.created_at).unwrap_or(0)
    }

    fn author(&self) -> i64 {
        self.user.as_ref().map_or(0, |u| u.id)
    }
}

/// `GET /api/articles/{slug}/version?revNum=N` — `{rendered, source}`;
/// only `source` (the FTML wikitext) matters here.
#[derive(Debug, Deserialize)]
struct Version {
    #[serde(default)]
    source: String,
}

/// `GET /api/articles/{slug}/files`.
#[derive(Debug, Deserialize)]
struct Files {
    #[serde(default)]
    files: Vec<FileEntry>,
}

#[derive(Debug, Deserialize)]
struct FileEntry {
    name: String,
}

// ── ISO-8601 → unix (no chrono in the tree; Django emits
//    `2026-09-15T16:57:11.917538+00:00`) ──

/// Parse an ISO-8601 timestamp into unix seconds. Accepts the Django
/// shape (`T` or space separator, fractional seconds, `Z` or `±HH:MM` /
/// `±HHMM` offsets, or none — naive reads as UTC); fractions truncate.
fn parse_iso8601(s: &str) -> Option<i64> {
    let s = s.trim();
    let b = s.as_bytes();
    if b.len() < 19
        || b[4] != b'-'
        || b[7] != b'-'
        || (b[10] != b'T' && b[10] != b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let num = |range: std::ops::Range<usize>| s.get(range)?.parse::<i64>().ok();
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, ss) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || ss > 60 {
        return None;
    }
    let mut rest = &s[19..];
    if let Some(tail) = rest.strip_prefix('.') {
        let frac_end = tail
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(tail.len());
        rest = &tail[frac_end..];
    }
    let offset_secs = match rest {
        "" | "Z" | "z" => 0,
        _ if rest.len() == 6 && rest.as_bytes()[3] == b':' => {
            let sign = match rest.as_bytes()[0] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            let oh = rest.get(1..3)?.parse::<i64>().ok()?;
            let om = rest.get(4..6)?.parse::<i64>().ok()?;
            if oh > 23 || om > 59 {
                return None;
            }
            sign * (oh * 3600 + om * 60)
        }
        _ if rest.len() == 5 => {
            let sign = match rest.as_bytes()[0] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            let oh = rest.get(1..3)?.parse::<i64>().ok()?;
            let om = rest.get(3..5)?.parse::<i64>().ok()?;
            if oh > 23 || om > 59 {
                return None;
            }
            sign * (oh * 3600 + om * 60)
        }
        _ => return None,
    };
    Some(days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + ss - offset_secs)
}

/// Days since the unix epoch (Howard Hinnant's `days_from_civil`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12; // Mar = 0 … Feb = 11
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

// ── API client ──

const API_RETRIES: u32 = 5;

/// Per-site RuFoundation API handle. Every call rides the global limiter
/// at the executing job's priority and retries transient errors (v1
/// `retryable` semantics, like the Wikidot `SiteApi`).
pub struct RuApi<'a> {
    wik: &'a Wikidot,
    /// Configured host, `www.` included where the site uses it.
    host: String,
    bearer: Option<String>,
    prio: i64,
}

impl<'a> RuApi<'a> {
    pub fn new(wik: &'a Wikidot, host: &str, api_key: &Option<String>, prio: i64) -> RuApi<'a> {
        RuApi {
            wik,
            host: host.trim().trim_end_matches('/').to_string(),
            bearer: api_key.clone(),
            prio,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("https://{}{path}", self.host)
    }

    /// Whether a URL sits on the site's own host (www spelling included).
    fn is_own(&self, url: &str) -> bool {
        fn bare(host: &str) -> &str {
            host.strip_prefix("www.").unwrap_or(host)
        }
        match parsers::url_host(url) {
            Some(h) => bare(&h) == bare(&self.host),
            // Relative/junk paths ride the site's own origin.
            None => true,
        }
    }

    async fn retryable<T, F, Fut>(&self, label: &str, mut op: F) -> FetchResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = FetchResult<T>>,
    {
        let mut attempts_left = API_RETRIES;
        loop {
            let result = op().await;
            match result {
                Ok(v) => return Ok(v),
                Err(e) if e.retryable() && attempts_left > 0 => {
                    tracing::warn!(
                        site = %self.host, %label, error = %e,
                        remaining = attempts_left,
                        "retrying after backoff"
                    );
                    tokio::time::sleep(e.backoff()).await;
                    attempts_left -= 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> FetchResult<T> {
        let url = self.url(path);
        self.retryable(&format!("GET {path}"), || async {
            let resp = self.wik.get_api(&url, self.bearer.as_deref(), self.prio).await?;
            let body = resp
                .text()
                .await
                .map_err(|e| FetchError::Http(e.to_string()))?;
            serde_json::from_str(&body)
                .map_err(|e| FetchError::Parse(format!("{path}: {e}")))
        })
        .await
    }

    async fn list_articles(&self) -> FetchResult<Vec<ArticleEntry>> {
        self.get_json("/api/articles").await
    }

    async fn fetch_article(&self, slug: &str) -> FetchResult<Article> {
        self.get_json(&format!("/api/articles/{slug}")).await
    }

    async fn fetch_log(&self, slug: &str) -> FetchResult<Log> {
        self.get_json(&format!("/api/articles/{slug}/log?all=1")).await
    }

    async fn fetch_version(&self, slug: &str, rev_no: i64) -> FetchResult<String> {
        let v: Version = self
            .get_json(&format!("/api/articles/{slug}/version?revNum={rev_no}"))
            .await?;
        Ok(v.source)
    }

    async fn fetch_files(&self, slug: &str) -> FetchResult<Files> {
        self.get_json(&format!("/api/articles/{slug}/files")).await
    }

    /// One attachment: (bytes, media type). Own-host fetches carry the
    /// bearer token; off-host URLs (theme/CDN assets extracted from
    /// wikitext) never do — the API key must not leak to third parties.
    /// Redirects to the media domain are the server's business (public
    /// bytes; reqwest already strips the Authorization header on the hop).
    async fn fetch_file(&self, url: &str) -> FetchResult<(Vec<u8>, Option<String>)> {
        let bearer = if self.is_own(url) { self.bearer.as_deref() } else { None };
        self.retryable(&format!("attachment {url}"), || async {
            let resp = self.wik.get_api(url, bearer, self.prio).await?;
            let content_type = crate::wikidot::media_type(resp.headers());
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| FetchError::Http(e.to_string()))?;
            Ok((bytes.to_vec(), content_type))
        })
        .await
    }

    /// The rendered homepage (shell identity).
    async fn fetch_homepage(&self) -> FetchResult<String> {
        let url = self.url("/");
        self.retryable("GET /", || async {
            let resp = self.wik.get_api(&url, None, self.prio).await?;
            resp.text()
                .await
                .map_err(|e| FetchError::Http(e.to_string()))
        })
        .await
    }
}

/// Canonical row key of a listed file: the raw `{slug}/{name}` tail pushed
/// through the same URL normalization the wikitext-extraction side uses
/// (`parsers::file_row_paths_own`), so both spellings of one file
/// converge on one row.
fn file_row_url(host: &str, slug: &str, name: &str) -> String {
    parsers::file_row_paths_own(host, &format!("https://{host}/local--files/{slug}/{name}")).0
}

// ── Job dispatch ──

/// Execute one job for a RuFoundation site. Same contract as
/// `workers::run_job`; `site` is the directory name, `host`/`api_key` the
/// platform parameters from the config.
pub async fn run_kind(
    db: &Db,
    cfg: &Config,
    wik: &Wikidot,
    site: &str,
    host: &str,
    api_key: &Option<String>,
    job: &db::Job,
) -> Result<Outcome, FetchError> {
    let api = RuApi::new(wik, host, api_key, job.priority);
    let out_dir = cfg.site_out(site);
    match job.kind.as_str() {
        jobs::kind::DISCOVER => discover(db, cfg, &api).await,
        jobs::kind::PAGE_SYNC => page_sync(db, &api, job).await,
        jobs::kind::REVISION_FETCH => revision_fetch(db, &api, job).await,
        jobs::kind::FILE_FETCH => file_fetch(db, &api, &out_dir, job).await,
        jobs::kind::SHELL_SYNC => shell_sync(cfg, &api, site).await,
        // Never enqueued for this platform (empty theme_roots); a stray
        // row must not spin the queue.
        other => Ok(Outcome::Fail {
            error: format!("job kind '{other}' is not runnable on a rufoundation site"),
            permanent: true,
        }),
    }
}

// ── discover: listing-watermark diff ──

/// Slugs whose listing watermark is newer than the stored one (or
/// unknown) → `(slug, updated_at)` pairs to reconcile. Equal watermarks
/// skip — that is the whole point: a page never re-syncs for a change
/// state it already recorded.
fn stale_pages(list: &[ArticleEntry], known: &HashMap<String, i64>) -> Vec<(String, i64)> {
    list.iter()
        .filter_map(|e| {
            let slug = e.page_id.to_lowercase();
            let updated = parse_iso8601(&e.updated_at).unwrap_or(0);
            match known.get(&slug) {
                Some(&stored) if stored >= updated => None,
                _ => Some((slug, updated)),
            }
        })
        .collect()
}

async fn discover(db: &Db, cfg: &Config, api: &RuApi<'_>) -> Result<Outcome, FetchError> {
    let list = api.list_articles().await?;
    let known: HashMap<String, i64> = db
        .pages_watermarks()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let stale = stale_pages(&list, &known);
    let n = stale.len();
    let sync_jobs: Vec<NewJob> = stale
        .into_iter()
        .map(|(slug, updated_at)| {
            // The watermark rides in the payload so page.sync stores the
            // exact listing value it was triggered by — the diff baseline
            // stays stable whatever `updatedAt` semantically tracks
            // upstream.
            let mut j = jobs::page_sync(&slug);
            j.payload =
                serde_json::json!({ "slug": slug, "updated_at": updated_at }).to_string();
            j
        })
        .collect();
    let effects: Effects = Box::new(move |tx| {
        enqueue_on(tx, &sync_jobs)?;
        Ok(())
    });
    tracing::info!(site = %api.host, pages = list.len(), stale = n, "discovery scan complete");
    Ok(workers::complete(Some(cfg.monitor_interval_s), effects))
}

// ── page.sync: identity + revision metas + file listing ──

/// A source snapshot newer than anything stored.
struct NewRev {
    rev_no: i64,
    version_id: i64,
    ts: i64,
    author: i64,
}

/// Log entries that archive a source snapshot above `max_rev`, newest
/// first (the API is newest-first; the sort makes that explicit).
fn snapshots_above(entries: &[LogEntry], max_rev: i64) -> Vec<NewRev> {
    let mut out: Vec<NewRev> = entries
        .iter()
        .filter_map(|e| {
            let version_id = e.version_id()?;
            (e.rev_number > max_rev).then(|| NewRev {
                rev_no: e.rev_number,
                version_id,
                ts: e.ts(),
                author: e.author(),
            })
        })
        .collect();
    out.sort_unstable_by_key(|r| std::cmp::Reverse(r.rev_no));
    out.dedup_by_key(|r| r.rev_no);
    out
}

async fn page_sync(db: &Db, api: &RuApi<'_>, job: &db::Job) -> Result<Outcome, FetchError> {
    #[derive(Deserialize)]
    struct P {
        slug: String,
        updated_at: Option<i64>,
    }
    let p: P = serde_json::from_str(&job.payload)
        .map_err(|e| FetchError::Parse(format!("bad payload: {e}")))?;
    let slug = Slug::parse(&p.slug);
    let slug_str = slug.as_str();

    let article = api.fetch_article(&slug_str).await?;
    let log = api.fetch_log(&slug_str).await?;
    let files = api.fetch_files(&slug_str).await?;

    let page_id = article.uid;
    let max_rev = db.max_rev(page_id).unwrap_or(-1);
    let new_revs = snapshots_above(&log.entries, max_rev);

    // The article JSON's `source` IS the newest source-bearing version —
    // the head revision archives without a second request.
    let head = new_revs.first().map(|r| r.rev_no);
    let head_source = head.is_some().then(|| article.source.clone());

    tracing::info!(
        site = %api.host, slug = %slug_str, page_id,
        new_revs = new_revs.len(), files = files.files.len(),
        "page synced"
    );

    let host = api.host.clone();
    let watermark = p.updated_at.or_else(|| new_revs.iter().map(|r| r.ts).max());
    let effects: Effects = Box::new(move |tx| {
        tx.execute(
            "INSERT INTO pages(page_id, slug, title, tags, updated_at, discovered_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(page_id) DO UPDATE SET
               slug=excluded.slug, title=excluded.title, tags=excluded.tags,
               updated_at=COALESCE(excluded.updated_at, pages.updated_at)",
            params![
                page_id,
                slug_str,
                article.title,
                serde_json::to_string(&article.tags)?,
                watermark,
                db::now()
            ],
        )?;
        let mut rev_jobs = Vec::new();
        let mut file_jobs = Vec::new();
        for r in &new_revs {
            let rev_id = r.version_id.to_string();
            if Some(r.rev_no) == head && let Some(src) = &head_source {
                // Store the head content that arrived with the article,
                // mine it for file refs, skip its fetch job. The upsert is
                // idempotent against a racing sync (content kept if set).
                tx.execute(
                    "INSERT INTO revisions(page_id, rev_no, rev_id, ts, author, content, fetched_at)
                     VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(page_id, rev_no) DO UPDATE SET
                       content=COALESCE(revisions.content, excluded.content),
                       fetched_at=COALESCE(revisions.fetched_at, excluded.fetched_at)",
                    params![page_id, r.rev_no, rev_id, r.ts, r.author, src, db::now()],
                )?;
                for path in parsers::extract_file_links_origin(&host, src) {
                    tx.execute(
                        "INSERT OR IGNORE INTO files(path, first_seen) VALUES(?1, ?2)",
                        params![path, db::now()],
                    )?;
                    file_jobs.push(jobs::file_fetch(&path));
                }
            } else {
                tx.execute(
                    "INSERT OR IGNORE INTO revisions(page_id, rev_no, rev_id, ts, author)
                     VALUES(?1, ?2, ?3, ?4, ?5)",
                    params![page_id, r.rev_no, rev_id, r.ts, r.author],
                )?;
                let mut j =
                    jobs::revision_fetch(page_id, r.rev_no, &rev_id, Some(r.rev_no) == head);
                j.resurrect = true;
                rev_jobs.push(j);
            }
        }
        enqueue_on(tx, &rev_jobs)?;
        for f in &files.files {
            let path = file_row_url(&host, &slug_str, &f.name);
            tx.execute(
                "INSERT OR IGNORE INTO files(path, first_seen) VALUES(?1, ?2)",
                params![path, db::now()],
            )?;
            file_jobs.push(jobs::file_fetch(&path));
        }
        enqueue_on(tx, &file_jobs)?;
        Ok(())
    });
    Ok(workers::complete_ephemeral(effects))
}

// ── revision.fetch: one revision's wikitext ──

async fn revision_fetch(db: &Db, api: &RuApi<'_>, job: &db::Job) -> Result<Outcome, FetchError> {
    #[derive(Deserialize)]
    struct P {
        page_id: i64,
        rev_no: i64,
        rev_id: String,
    }
    let p: P = serde_json::from_str(&job.payload)
        .map_err(|e| FetchError::Parse(format!("bad payload: {e}")))?;

    // Fast paths like the Wikidot worker: content already stored, or the
    // revision recorded as permanently unobtainable.
    if let Some((Some(_content), _, _)) = db.revision_state(p.page_id, p.rev_no).ok().flatten() {
        return Ok(workers::complete(None, workers::noop_effects()));
    }
    if db.is_denied(p.page_id, p.rev_no).unwrap_or(false) {
        return Ok(workers::complete(None, workers::noop_effects()));
    }

    let slug = db
        .page_slug(p.page_id)
        .ok()
        .flatten()
        .ok_or_else(|| FetchError::Parse(format!("no slug for page {}", p.page_id)))?;

    let source = match api.fetch_version(&slug, p.rev_no).await {
        Ok(s) => s,
        Err(e @ (FetchError::NotFound | FetchError::Forbidden | FetchError::Gone(_))) => {
            // Terminal, recorded (v1 `revs_denied`): a 404 here means the
            // version was pruned upstream (verified live), a 403 a hidden
            // category without a key. Without the row the hourly backfill
            // would re-arm the fetch forever.
            let reason = match &e {
                FetchError::NotFound => "not_found".to_string(),
                FetchError::Forbidden => "no_permission".to_string(),
                FetchError::Gone(status) => format!("gone:{status}"),
                _ => unreachable!("matched arms are exhaustive"),
            };
            tracing::warn!(site = %api.host, page_id = p.page_id, rev = p.rev_no, %reason, "revision denied");
            let (ts, author) = db
                .revision_state(p.page_id, p.rev_no)
                .ok()
                .flatten()
                .map(|(_, ts, author)| (ts, author))
                .unwrap_or((0, 0));
            let effects: Effects = Box::new(move |tx| {
                tx.execute(
                    "INSERT OR REPLACE INTO denied_revisions(page_id, rev_no, rev_id, ts, author, reason)
                     VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                    params![p.page_id, p.rev_no, p.rev_id, ts, author, reason],
                )?;
                Ok(())
            });
            return Ok(workers::complete(None, effects));
        }
        Err(e) => return Err(e),
    };

    let links = parsers::extract_file_links_origin(&api.host, &source);
    let n_links = links.len();
    let fetched_at = db::now();
    let effects: Effects = Box::new(move |tx| {
        let n = tx.execute(
            "UPDATE revisions SET content=?4, fetched_at=?5
             WHERE page_id=?1 AND rev_no=?2 AND rev_id=?3",
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
    tracing::debug!(site = %api.host, page_id = p.page_id, rev = p.rev_no, links = n_links, "revision fetched");
    Ok(workers::complete(None, effects))
}

// ── file.fetch: one attachment, content-addressed ──

async fn file_fetch(
    db: &Db,
    api: &RuApi<'_>,
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
        Some("saved") | Some("missing") => return Ok(workers::complete(None, workers::noop_effects())),
        _ => {}
    }

    // RAM host-health short-circuit, same as the Wikidot worker: a host
    // suppressed after transport failures fails fast (the periodic
    // backfill re-arms it once the TTL lapses).
    match api.wik.host_health(&p.path) {
        HostHealth::Gone => {
            tracing::info!(site = %api.host, path = %p.path, "attachment missing (origin gone)");
            return Ok(workers::complete(None, workers::missing_effects(p.path)));
        }
        HostHealth::Unreachable => {
            return Ok(Outcome::Fail {
                error: "host unreachable (recent transport failures)".into(),
                permanent: true,
            });
        }
        HostHealth::Alive => {}
    }

    let (bytes, header_type) = match api.fetch_file(&p.path).await {
        Ok(b) => b,
        // Files are hard-deleted upstream; the listing drives truth.
        Err(FetchError::NotFound) => {
            tracing::info!(site = %api.host, path = %p.path, "attachment missing (404)");
            return Ok(workers::complete(None, workers::missing_effects(p.path)));
        }
        Err(e) => return Err(e),
    };

    // No attachment is zero bytes — an empty body must not freeze an empty
    // blob as the evacuated truth (same rationale as wdfiles).
    if bytes.is_empty() {
        return Err(FetchError::Http(format!("empty body for {}", p.path)));
    }

    let sha = blobs::write_blob(out_dir, &bytes)
        .map_err(|e| FetchError::Http(format!("blob write: {e}")))?;
    let content_type = header_type.or_else(|| blobs::sniff_content_type(&bytes, &p.path));
    let url = p.path.clone();
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
    tracing::debug!(site = %api.host, path = %p.path, size, "attachment saved");
    Ok(workers::complete(None, effects))
}

// ── shell.sync: site display identity ──

const SHELL_KEY: &str = "shell";

async fn shell_sync(cfg: &Config, api: &RuApi<'_>, site: &str) -> Result<Outcome, FetchError> {
    let html = api.fetch_homepage().await?;
    let shell = out::Shell {
        title: html_title(&html),
        // RuFoundation templates expose no subtitle; theme CSS lives in
        // per-site FTML/component setup, not a crawlable import graph.
        subtitle: String::new(),
        landing: String::new(),
        theme_roots: Vec::new(),
    };
    let out_dir = cfg.site_out(site);
    out::write_shell(&out_dir, &shell)
        .map_err(|e| FetchError::Http(format!("shell write: {e}")))?;
    let payload = serde_json::to_string(&shell)
        .map_err(|e| FetchError::Http(format!("shell encode: {e}")))?;
    tracing::info!(site, title = %shell.title, "shell synced");
    let effects: Effects = Box::new(move |tx| {
        tx.execute(
            "INSERT INTO meta(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![SHELL_KEY, payload],
        )?;
        Ok(())
    });
    Ok(workers::complete(Some(cfg.shell_interval_s), effects))
}

/// First `<title>` text — RuFoundation's `page.html` emits the bare site
/// title (`<title>{{ site_title }}</title>`), no page suffix to strip.
fn html_title(html: &str) -> String {
    let Some((_, rest)) = html.split_once("<title") else {
        return String::new();
    };
    let Some((_, body)) = rest.split_once('>') else {
        return String::new();
    };
    // Title text never contains '<'; taking up to the next tag sidesteps
    // case variants of the closing tag.
    body[..body.find('<').unwrap_or(body.len())]
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── ISO-8601 ──

    #[test]
    fn iso8601_known_values() {
        // The exact Django shape the live API emits (µs fraction, offset).
        assert_eq!(parse_iso8601("2026-09-15T16:57:11.917538+00:00"), Some(1789491431));
        assert_eq!(parse_iso8601("2004-03-01T00:00:00+03:30"), Some(1078086600));
        assert_eq!(parse_iso8601("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_iso8601("1970-01-01T00:00:00"), Some(0));
        // Negative offset adds; compact ±HHMM accepted.
        assert_eq!(parse_iso8601("1970-01-02T00:30:00-0100"), Some(91800));
        // Space separator, no fraction.
        assert_eq!(parse_iso8601("2020-02-29 12:00:00+00:00"), Some(1582977600));
    }

    #[test]
    fn iso8601_rejects_garbage() {
        for bad in ["", "yesterday", "2026-09-15", "2026-13-01T00:00:00Z",
                    "2026-09-15T25:00:00Z", "2026-09-15T16:57:11+25:00"] {
            assert_eq!(parse_iso8601(bad), None, "{bad:?}");
        }
    }

    // ── log filtering ──

    #[test]
    fn snapshots_above_keeps_only_version_bearing_entries() {
        let log: Log = serde_json::from_value(json_log()).unwrap();
        let revs = snapshots_above(&log.entries, -1);
        let got: Vec<(i64, i64)> = revs.iter().map(|r| (r.rev_no, r.version_id)).collect();
        // newest-first, `new` (rev 0) and `revert` (meta.source.version_id)
        // included; file/tag entries dropped.
        assert_eq!(got, vec![(482, 42912), (438, 30991), (0, 8747)]);
        // Watermark cuts everything at or below it.
        assert!(snapshots_above(&log.entries, 438).iter().all(|r| r.rev_no > 438));
    }

    fn json_log() -> serde_json::Value {
        serde_json::json!({ "entries": [
            { "type": "source", "revNumber": 482, "createdAt": "2026-09-15T16:57:11.917538+00:00",
              "user": { "id": 413 }, "meta": { "version_id": 42912 } },
            { "type": "file_added", "revNumber": 478, "createdAt": "2026-08-01T00:00:00+00:00",
              "user": { "id": 413 }, "meta": { "id": 103225, "name": "a.png" } },
            { "type": "revert", "revNumber": 438, "createdAt": "2024-05-05T00:00:00+00:00",
              "user": { "id": 41 }, "meta": { "source": { "version_id": 30991 } } },
            { "type": "wikidot", "revNumber": 353, "createdAt": "2023-01-01T00:00:00+00:00",
              "user": { "id": 14 }, "meta": {} },
            { "type": "new", "revNumber": 0, "createdAt": "2020-08-05T10:18:08+00:00",
              "user": { "id": 383 }, "meta": { "title": "", "version_id": 8747 } }
        ]})
    }

    // ── discovery diff ──

    #[test]
    fn stale_pages_diffs_against_stored_watermarks() {
        let list: Vec<ArticleEntry> = serde_json::from_value(serde_json::json!([
            { "uid": 1, "pageId": "main", "updatedAt": "2026-09-15T16:57:11+00:00" },
            { "uid": 2, "pageId": "old", "updatedAt": "2020-01-01T00:00:00+00:00" },
            { "uid": 3, "pageId": "Fresh", "updatedAt": "2026-10-01T00:00:00+00:00" }
        ]))
        .unwrap();
        let known: HashMap<String, i64> = [
            ("main".to_string(), 1789491431), // equal → skip
            ("old".to_string(), 1789491431),  // stored newer → skip
        ]
        .into_iter()
        .collect();
        // Slugs compare lowercased; unknown pages always sync.
        assert_eq!(
            stale_pages(&list, &known),
            vec![("fresh".to_string(), 1790812800)]
        );
    }

    // ── file URL convergence ──

    #[test]
    fn listed_file_urls_converge_with_wikitext_refs() {
        let host = "www.backroomswiki.ru";
        // The files-API name and the wikitext spelling of the same file
        // must produce one row key.
        assert_eq!(
            file_row_url(host, "main", "oversight-cut-logo.png"),
            "https://www.backroomswiki.ru/local--files/main/oversight-cut-logo.png"
        );
        let src = r#"[[image oversight-cut-logo.png]] and http://backroomswiki.ru/local--files/main/oversight-cut-logo.png"#;
        let extracted = parsers::extract_file_links_origin(host, src);
        // The bare `[[image name]]` form is not a local--files reference —
        // only the two explicit ones are, and they converge.
        assert!(extracted.iter().all(|p| p.ends_with("/local--files/main/oversight-cut-logo.png")));
        assert_eq!(extracted.len(), 1);
    }

    // ── shell ──

    #[test]
    fn homepage_title_is_extracted() {
        let html = "<html><head>\n<title>База данных Backrooms</title>\n</head></html>";
        assert_eq!(html_title(html), "База данных Backrooms");
        assert_eq!(html_title("<TITLE>x</TITLE>"), "");
        assert_eq!(html_title("no title"), "");
    }
}
