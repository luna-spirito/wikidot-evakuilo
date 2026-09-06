//! Per-site SQLite store — the single source of truth.
//!
//! One database file per site (`{site}/meta/site.db`), one connection parked
//! behind a mutex. Every operation is a sub-millisecond transaction, so
//! blocking is a non-issue at our write rate (the global rate limiter caps
//! Wikidot fetches at one per `rate_limit_ms`).
//!
//! ## The job queue
//!
//! "Queues like normal people": a `jobs` table. Workers `claim`, execute,
//! then either `complete` (effects + job state + child enqueues in ONE
//! transaction) or `fail` (retry with exponential backoff, or dead-letter).
//! Two lifecycles share the table (the `jobs` module documents the purity
//! taxonomy):
//!
//! * durable jobs have a stable identity: deduped on (kind, payload),
//!   `done` rows are truthful tombstones ("this immutable work is finished
//!   forever");
//! * ephemeral (event) jobs have none: every signal inserts a row and
//!   completion DELETES it (`complete_ephemeral`) — no dedup that could
//!   swallow a second signal, no tombstone that could lie.
//!
//! Periodic jobs (`discover`, `shell.sync`, `out.update`, `backfill`) are durable and
//! never finish — `complete` with a reschedule flips them back to `pending`
//! at `now + interval` (the interval comes from the live config, never
//! from the payload: the payload is the dedup key).
//!
//! ## Crash model
//!
//! * SQLite WAL + `synchronous=FULL`: every commit is durable, atomic,
//!   all-or-nothing. There is no torn state, ever.
//! * A crash between claim and complete leaves the job `running`; the daemon
//!   re-claims it at startup (`recover`). A crash mid-HTTP just loses the
//!   fetch; the job is retried. Idempotent effects make both safe.
//! * Single daemon per site DB is enforced with an flock on
//!   `meta/{site}/daemon.lock`, so a stale `running` row really means a dead
//!   process, never a concurrent one.

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};
use serde::{Deserialize, Serialize};

/// Unix time, seconds. Queue granularity never needs finer.
pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before 1970")
        .as_secs() as i64
}

// ── Schema ──

const SCHEMA_V1: &str = r#"
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- One row per known Wikidot page. page_id is the stable key; slug can
-- change over a page's lifetime (a rename upserts the row).
CREATE TABLE pages (
  page_id      INTEGER PRIMARY KEY,
  slug         TEXT NOT NULL,
  title        TEXT NOT NULL DEFAULT '',
  tags         TEXT NOT NULL DEFAULT '[]',   -- JSON array of strings
  updated_at   INTEGER,                      -- newest revision timestamp seen
  discovered_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX ux_pages_slug ON pages(slug);

-- Full revision history. Content arrives via revision.fetch; a row exists
-- (with NULL content) as soon as discovery has seen the revision metadata.
CREATE TABLE revisions (
  page_id    INTEGER NOT NULL REFERENCES pages(page_id),
  rev_no     INTEGER NOT NULL,
  rev_id     TEXT NOT NULL,
  ts         INTEGER NOT NULL,
  author     INTEGER NOT NULL,
  content    TEXT,
  fetched_at INTEGER,
  PRIMARY KEY (page_id, rev_no)
);
CREATE INDEX ix_revisions_rid ON revisions(rev_id);

-- Revisions that are permanently unobtainable (no_permission / deleted).
-- Recorded so they are never re-attempted across discovery rescans.
CREATE TABLE denied_revisions (
  page_id INTEGER NOT NULL,
  rev_no  INTEGER NOT NULL,
  rev_id  TEXT NOT NULL,
  ts      INTEGER NOT NULL,
  author  INTEGER NOT NULL,
  reason  TEXT,
  PRIMARY KEY (page_id, rev_no)
);

-- File attachments and theme assets, keyed by site-relative path
-- (e.g. "local--files/page/image.png") or absolute URL path for off-site
-- theme assets. status: pending → saved (bytes content-addressed) or
-- missing (permanent 404: the attachment was deleted upstream).
CREATE TABLE files (
  path       TEXT PRIMARY KEY,
  url        TEXT,                 -- absolute URL the bytes came from
  sha256     TEXT,
  size       INTEGER,
  status     TEXT NOT NULL DEFAULT 'pending',
  first_seen INTEGER NOT NULL,
  saved_at   INTEGER
);
CREATE INDEX ix_files_sha ON files(sha256);

-- The durable work queue.
--   status: pending | running | done | dead
--   run_at: earliest unix time the job may be claimed (backoff / schedule)
--   max_attempts <= 0 means "never dead-letter" (periodic jobs)
-- Dedup on (kind, payload) is the privilege of jobs with a stable identity
-- (immutable targets, periodic singletons). Event jobs (page.sync) have no
-- identity — every signal is its own row, completed by deletion — so the
-- unique index deliberately skips them (see jobs::is_ephemeral).
CREATE TABLE jobs (
  id           INTEGER PRIMARY KEY,
  kind         TEXT NOT NULL,
  payload      TEXT NOT NULL DEFAULT '{}',
  priority     INTEGER NOT NULL DEFAULT 0,   -- higher runs first
  status       TEXT NOT NULL DEFAULT 'pending',
  run_at       INTEGER NOT NULL DEFAULT 0,
  attempts     INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 8,
  claimed_at   INTEGER,
  last_error   TEXT,
  created_at   INTEGER NOT NULL
);
CREATE UNIQUE INDEX ux_jobs_kind_payload ON jobs(kind, payload)
  WHERE kind <> 'page.sync';
CREATE INDEX ix_jobs_ready ON jobs(status, priority DESC, run_at, id);

-- Publication progress for the out/ updater (see out module).
CREATE TABLE out_state (
  page_id     INTEGER PRIMARY KEY REFERENCES pages(page_id),
  packed_revs INTEGER NOT NULL DEFAULT 0,
  packed_at   INTEGER
);
"#;

const SCHEMA_V2: &str = r#"
-- Media type of the stored bytes: the server's Content-Type (sans
-- parameters) when a fetch supplied one, otherwise a best-effort guess from
-- magic bytes / file-name extension (legacy import). NULL = unknown.
ALTER TABLE files ADD COLUMN content_type TEXT;
"#;

/// v3: priority retune (see `jobs::prio`). Priorities are frozen on the
/// row at enqueue time, so rows seated under the old table must be
/// rewritten: shell.sync jumps to the top of the claim order (a cold-start
/// site gets its identity + theme roots before the history backlog
/// drains), and files/theme move above intermediate history. revision.fetch
/// rows are untouched — their head/old split (20/8) did not change.
/// Deliberately unscoped by status: a stale `running` row is recovered to
/// `pending` carrying whatever priority it has, so it must be retuned too.
/// The literals have no compiler tie to `jobs::prio` —
/// `v3_retune_matches_prio_table` pins them together.
const RETUNE_PRIORITIES_V3: &str = r#"
UPDATE jobs SET priority=35 WHERE kind='shell.sync';
UPDATE jobs SET priority=10 WHERE kind IN ('file.fetch', 'theme.crawl');
UPDATE jobs SET priority=15 WHERE kind='page.sync';
"#;

/// v4: publication priority retune (see `jobs::prio`). `out.update` moves
/// from the bottom of the claim order (-10) to just under the newest
/// revision (18): the incremental repack is nearly free, so publication
/// loses nothing by claiming promptly, and a fetch backlog no longer
/// stretches the out/ refresh cycle. Same rules as v3 — deliberately
/// unscoped by status, and pinned to the constant by
/// `v4_retune_matches_prio_table`.
const RETUNE_PRIORITIES_V4: &str = r#"
UPDATE jobs SET priority=18 WHERE kind='out.update';
"#;

/// v5: the shell gained `landing` (the slug the site root serves). The
/// homepage HTML is never persisted, so landing is only observable by a
/// live GET — existing sites must re-run `shell.sync` once instead of
/// waiting out their `shell_interval_s`. Pull the singleton's next
/// scheduled run to "now". `running` rows need no touch: a live one is
/// mid-rewrite already, a stale one is recovered to `pending` with a past
/// `run_at` (it was due when claimed), so it is claimable either way.
/// `priority` is untouched — v3 already seated shell.sync at the top.
const REARM_SHELL_SYNC_V5: &str = r#"
UPDATE jobs SET run_at=0 WHERE kind='shell.sync' AND status='pending';
"#;

// ── v6: file-reference repair (see `repair_files_v6`) ──

/// Context for Rust repair steps: the site's name (own-host classification
/// when re-running extraction) and its `out/` dir (blob GC). Derived from
/// the DB path per the layout contract `config.rs` documents:
/// `{repo}/meta/{site}/site.db` ↔ `{repo}/out/{site}`.
pub(crate) struct RepairCtx {
    pub site: String,
    pub out_dir: PathBuf,
}

/// One schema step. SQL covers what SQL can say; repairs are Rust data
/// surgery inside the same transaction (v6 re-parses wikitext, which SQL
/// cannot). Repairs may collect orphaned blob shas for post-commit unlink.
enum Migration {
    Sql(&'static str),
    Repair(fn(&Transaction, &RepairCtx, &mut Vec<String>) -> Result<()>),
}

const MIGRATIONS: &[Migration] = &[
    Migration::Sql(SCHEMA_V1),
    Migration::Sql(SCHEMA_V2),
    Migration::Sql(RETUNE_PRIORITIES_V3),
    Migration::Sql(RETUNE_PRIORITIES_V4),
    Migration::Sql(REARM_SHELL_SYNC_V5),
    Migration::Repair(repair_files_v6),
];

/// v6: file references mined from wikitext were defective twice over —
/// `extract_file_links` dropped the host of absolute references (an
/// off-site asset — a sandbox, a sister wiki — was silently re-pointed at
/// THIS site, where it soft-404'd into a saved HTML error page or fetched
/// an unrelated same-named file) and leaked `|` argument separators /
/// trailing garbage into paths. The repair, all in one transaction:
///
/// * garbage paths (`…|caption=`, site-relative refs with no page/name
///   split) are deleted, along with their pending fetch jobs;
/// * rows "saved" as `text/html` — Wikidot never serves attachments as
///   HTML, every one is an error page — reset to `pending` with the blob
///   fields cleared, so the fixed fetcher records a truthful `missing`
///   (or re-saves a genuinely re-fetchable file);
/// * link extraction re-runs over every stored revision with the fixed
///   parser, seeding the correct rows as pending — no `revision.fetch`
///   backfill is needed, which is lucky because those fast-path skip
///   revisions that already have content;
/// * every row key is canonicalized to an absolute URL: site-relative
///   rows lift onto `http://{site}.wikidot.com`, and the broken era's
///   off-site rows were stored under whichever host spelling the wikitext
///   used (`…wdfiles.com`, `www.…`, `https`) even though Wikidot serves
///   all of them from one namespace — duplicate keys for one file merge
///   into the row carrying the most state (saved over missing over
///   pending), and fetch jobs keyed on a dead spelling are deleted;
/// * fetch jobs are armed for every pending row (`resurrect`, so the
///   done/dead tombstones the broken era burned re-arm instead of
///   swallowing the enqueue);
/// * the junk blobs are unlinked after commit (a rollback must never cost
///   a live row its bytes), guarded by a final reference check.
///
/// Idempotent by construction (deletes match nothing twice, `INSERT OR
/// IGNORE` re-seeds nothing), though the version bump means it runs once.
fn repair_files_v6(tx: &Transaction, ctx: &RepairCtx, orphans: &mut Vec<String>) -> Result<()> {
    use std::collections::{BTreeMap, BTreeSet, HashSet};

    // Junk paths go with their queued jobs: a stale pending job would
    // re-create its row (as a junk `missing`) on the next run.
    let junk_paths: BTreeSet<String> = {
        let mut stmt = tx.prepare(
            "SELECT path FROM files
              WHERE path LIKE '%|%'
                 OR path = 'local--files/'
                 OR (path LIKE 'local--files/%' AND path NOT LIKE 'local--files/%/%')",
        )?;
        stmt.query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?
    };

    // GC candidates while the rows still carry their shas.
    {
        let mut stmt = tx.prepare(
            "SELECT DISTINCT sha256 FROM files
              WHERE status='saved' AND content_type='text/html'",
        )?;
        let shas: Vec<Option<String>> =
            stmt.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
        orphans.extend(shas.into_iter().flatten());
    }

    let mut junk_rows = 0usize;
    let mut junk_jobs = 0usize;
    if !junk_paths.is_empty() {
        let ph_paths = vec!["?"; junk_paths.len()].join(",");
        junk_rows = tx.execute(
            &format!("DELETE FROM files WHERE path IN ({ph_paths})"),
            params_from_iter(junk_paths.iter()),
        )?;
        // jobs dedupe on (kind, payload); the payload is exactly
        // `{"path":"…"}` — rebuild it rather than pattern-matching JSON.
        let payloads: Vec<String> = junk_paths
            .iter()
            .map(|p| serde_json::json!({ "path": p }).to_string())
            .collect();
        junk_jobs = tx.execute(
            &format!(
                "DELETE FROM jobs
                  WHERE kind='file.fetch' AND status='pending'
                    AND payload IN ({})",
                vec!["?"; payloads.len()].join(",")
            ),
            params_from_iter(payloads.iter()),
        )?;
    }

    let html_reset = tx.execute(
        "UPDATE files
            SET status='pending', sha256=NULL, size=NULL, content_type=NULL, saved_at=NULL
          WHERE status='saved' AND content_type='text/html'",
        [],
    )?;

    // The union of file references across ALL stored revisions — the
    // evacuation set — not just page heads. Extraction already yields
    // canonical absolute URLs.
    let mut refs: HashSet<String> = HashSet::new();
    {
        let mut stmt = tx.prepare("SELECT content FROM revisions WHERE content IS NOT NULL")?;
        let rows: Vec<String> =
            stmt.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
        for content in rows {
            for path in crate::parsers::extract_file_links(&ctx.site, &content) {
                refs.insert(path);
            }
        }
    }
    let mut seeded = 0usize;
    for path in refs.into_iter().collect::<BTreeSet<_>>() {
        seeded += tx.execute(
            "INSERT OR IGNORE INTO files(path, first_seen) VALUES(?1, ?2)",
            params![path, now()],
        )?;
    }

    // Canonicalize the remaining (pre-v6) keys and merge the duplicates
    // the host spellings created. Groups keyed by canonical path; the
    // survivor is the member carrying the most state (saved > missing >
    // pending, ties by path order for determinism); losers are deleted
    // and, when their blob differs from the survivor's, queued for GC.
    let mut lifted = 0usize;
    let mut merged = 0usize;
    {
        let mut stmt = tx.prepare("SELECT path, status, sha256 FROM files ORDER BY path")?;
        let rows: Vec<(String, String, Option<String>)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        drop(stmt);
        let rank = |s: &str| if s == "saved" { 2 } else if s == "missing" { 1 } else { 0 };
        let mut groups: BTreeMap<String, Vec<(String, String, Option<String>)>> = BTreeMap::new();
        for (path, status, sha) in rows {
            groups
                .entry(canonical_row_key(&ctx.site, &path))
                .or_default()
                .push((path, status, sha));
        }
        for (canonical, mut members) in groups {
            members.sort_by_key(|a| std::cmp::Reverse(rank(&a.1)));
            let survivor_sha = members[0].2.clone();
            for (path, status, sha) in &members[1..] {
                tx.execute("DELETE FROM files WHERE path=?1", params![path])?;
                merged += 1;
                if status == "saved" && *sha != survivor_sha {
                    orphans.extend(sha.iter().cloned());
                }
            }
            let old_path = &members[0].0;
            if old_path != &canonical {
                tx.execute(
                    "UPDATE files SET path=?1 WHERE path=?2",
                    params![canonical, old_path],
                )?;
                lifted += 1;
            }
            // Fetch jobs keyed on a non-canonical spelling die with it: a
            // stale pending job would re-create its old row on the next
            // run, and a stale tombstone only blocks nothing. The re-arm
            // step below re-creates the jobs that matter (pending rows,
            // fresh attempt budget); terminal rows need none.
            for (path, _, _) in &members {
                if path != &canonical {
                    let payload = serde_json::json!({ "path": path }).to_string();
                    tx.execute(
                        "DELETE FROM jobs WHERE kind='file.fetch' AND payload=?1",
                        params![payload],
                    )?;
                }
            }
        }
    }

    // Arm fetches for every pending row — the freshly seeded references,
    // the reset soft-404 rows and the merged survivors whose own fetch
    // never ran. `resurrect` re-arms the done/dead jobs the broken era
    // already burned (pending/running rows are left alone, as ever); the
    // periodic backfill would do the same within its interval, this just
    // doesn't make the repair wait for it.
    let mut stmt = tx.prepare("SELECT path FROM files WHERE status='pending'")?;
    let pending: Vec<String> =
        stmt.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
    drop(stmt);
    let jobs: Vec<NewJob> = pending
        .iter()
        .map(|path| {
            let mut j = crate::jobs::file_fetch(path);
            j.resurrect = true;
            j
        })
        .collect();
    enqueue_on(tx, &jobs)?;

    tracing::info!(
        site = %ctx.site,
        junk_paths = junk_paths.len(),
        junk_rows,
        junk_jobs,
        html_reset,
        file_refs = seeded,
        lifted,
        merged,
        "v6 file-reference repair"
    );
    Ok(())
}

/// Canonical files-row key for an existing row: a site-relative path lifts
/// onto the main domain — then through the same canonicalization as any
/// freshly extracted reference, so an old `%3A`-spelled slug converges with
/// its `:` twin instead of forking a duplicate row; an absolute URL
/// canonicalizes directly (Wikidot host spellings collapse, everything
/// else stays verbatim).
fn canonical_row_key(site: &str, path: &str) -> String {
    let lifted = if path.starts_with("http://") || path.starts_with("https://") {
        path.to_string()
    } else {
        format!("http://{site}.wikidot.com/{path}")
    };
    crate::parsers::file_row_paths(site, &lifted).0
}

// ── Job types ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: i64,
    pub kind: String,
    pub payload: String,
    pub priority: i64,
    pub attempts: i64,
    pub max_attempts: i64,
    pub run_at: i64,
}

/// A job to insert. Non-ephemeral kinds dedupe on (kind, payload);
/// ephemeral kinds (`jobs::is_ephemeral`) always insert a fresh row.
#[derive(Debug, Clone)]
pub struct NewJob {
    pub kind: String,
    pub payload: String,
    pub priority: i64,
    pub run_at: i64,
    /// <= 0 = unlimited (periodic jobs must never dead-letter).
    pub max_attempts: i64,
    /// Resurrect a `done`/`dead` job with this (kind, payload) back to
    /// `pending` — for state-diff jobs (theme.crawl) and fetches re-armed
    /// by the periodic backfill (dead-lettered attempts must not strand the
    /// content they were fetching). Jobs that are pending/running are left
    /// alone (dedupe as before). Meaningless for ephemeral kinds.
    pub resurrect: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailOutcome {
    /// Back to pending; claimable at this unix time.
    Retry { run_at: i64 },
    /// Dead-lettered with `last_error` set.
    Dead,
}

// ── Database handle ──

#[derive(Clone)]
pub struct Db(Arc<Mutex<Connection>>);

/// Held by the daemon for its lifetime; dropping it releases the flock.
pub struct DaemonLock(File);

impl Drop for DaemonLock {
    fn drop(&mut self) {
        // Best-effort: a dead process's fd releases the flock anyway.
        let _ = fs4::fs_std::FileExt::unlock(&self.0);
    }
}

impl Db {
    /// Open (creating if needed) a site database. Does NOT recover stale
    /// `running` jobs — plain opens are safe against a live daemon.
    pub fn open(path: &Path) -> Result<Db> {
        Self::open_inner(path, false)
    }

    /// Open a site database read-only; errors if it does not exist yet.
    pub fn open_read_only(path: &Path) -> Result<Db> {
        Self::open_inner(path, true)
    }

    fn open_inner(path: &Path, read_only: bool) -> Result<Db> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        let conn = if read_only {
            if !path.exists() {
                bail!("no database at {}", path.display());
            }
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .with_context(|| format!("opening {} read-only", path.display()))?
        } else {
            Connection::open(path).with_context(|| format!("opening {}", path.display()))?
        };
        if !read_only {
            let _wal: String = conn
                .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
                .context("setting WAL mode")?;
            conn.pragma_update(None, "synchronous", "FULL")
                .context("setting synchronous=FULL")?;
            conn.pragma_update(None, "foreign_keys", "ON")
                .context("enabling foreign_keys")?;
        }
        conn.busy_timeout(std::time::Duration::from_secs(10))
            .context("setting busy_timeout")?;
        let db = Db(Arc::new(Mutex::new(conn)));
        if !read_only {
            db.migrate(&repair_ctx_for(path)).context("migrating schema")?;
        }
        Ok(db)
    }

    /// Daemon entry point: open + take the site lock + recover stale jobs.
    pub fn open_locked(path: &Path) -> Result<(Db, DaemonLock)> {        let lock_path = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("daemon.lock");
        if let Some(dir) = lock_path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        let lock = File::create(&lock_path)
            .with_context(|| format!("creating {}", lock_path.display()))?;
        // NB: fs4 signals contention with Ok(false), not Err.
        let locked = fs4::fs_std::FileExt::try_lock_exclusive(&lock)
            .map_err(|e| anyhow::anyhow!("flock {}: {e}", lock_path.display()))?;
        if !locked {
            anyhow::bail!(
                "another evakuilo daemon holds {} — refusing to start",
                lock_path.display()
            );
        }
        let db = Db::open(path)?;
        db.recover()?;
        Ok((db, DaemonLock(lock)))
    }

    fn migrate(&self, ctx: &RepairCtx) -> Result<()> {
        let mut conn = self.0.lock();
        let tx = conn.transaction()?;
        let version: i64 = if !has_table(&tx, "meta") {
            0 // fresh database
        } else {
            tx.query_row(
                "SELECT value FROM meta WHERE key='schema_version'",
                [],
                |r| r.get(0),
            )
            .optional()?
            .map(|v: String| v.parse().unwrap_or(0))
            .unwrap_or(0)
        };
        // Blobs the applied repairs orphaned; unlinked after commit so a
        // rollback can never strand a live row without its bytes.
        let mut orphans: Vec<String> = Vec::new();
        for (i, migration) in MIGRATIONS.iter().enumerate().skip(version as usize) {
            match migration {
                Migration::Sql(sql) => tx
                    .execute_batch(sql)
                    .with_context(|| format!("applying migration v{}", i + 1))?,
                Migration::Repair(repair) => {
                    repair(&tx, ctx, &mut orphans)
                        .with_context(|| format!("applying migration v{}", i + 1))?;
                }
            }
            tx.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?1)",
                params![(i + 1).to_string()],
            )?;
        }
        tx.commit()?;
        // Best-effort junk-blob GC: an orphan left behind by a crash here is
        // inert slack in the content-addressed store, never corruption —
        // and the reference recheck keeps a racing re-save of the same sha
        // from losing its blob to us.
        for sha in orphans {
            if sha.len() != 64 {
                continue;
            }
            let live: i64 = conn.query_row(
                "SELECT count(*) FROM files WHERE sha256=?1",
                params![sha],
                |r| r.get(0),
            )?;
            if live == 0 {
                let _ = std::fs::remove_file(crate::blobs::blob_path(&ctx.out_dir, &sha));
            }
        }
        Ok(())
    }

    /// Re-queue everything a dead predecessor left `running`. Only the
    /// flock holder may call this.
    fn recover(&self) -> Result<()> {
        let conn = self.0.lock();
        conn.execute(
            "UPDATE jobs SET status='pending' WHERE status='running'",
            [],
        )?;
        Ok(())
    }

    // ── Queue operations ──

    /// Insert jobs, skipping any whose (kind, payload) already exists.
    /// Returns how many were actually inserted.
    pub fn enqueue(&self, jobs: &[NewJob]) -> Result<usize> {
        let mut conn = self.0.lock();
        let tx = conn.transaction()?;
        let n = enqueue_on(&tx, jobs)?;
        tx.commit()?;
        Ok(n)
    }

    /// Atomically claim up to `limit` runnable jobs (pending, due, highest
    /// priority first). Claimed jobs are `running` with `attempts` bumped.
    ///
    /// Priority is the row's own column, frozen at enqueue time — the v1
    /// freshness split (head vs intermediate revisions, `jobs::prio`) is
    /// inherently per-job and cannot be resolved from the kind alone.
    pub fn claim(&self, limit: i64) -> Result<Vec<Job>> {
        let conn = self.0.lock();
        let mut stmt = conn.prepare(
            "UPDATE jobs
             SET status='running', claimed_at=?1, attempts=attempts+1
             WHERE id IN (
               SELECT id FROM jobs
               WHERE status='pending' AND run_at<=?2
               ORDER BY priority DESC, run_at, id
               LIMIT ?3
             )
             RETURNING id, kind, payload, priority, attempts, max_attempts, run_at",
        )?;
        let rows = stmt
            .query_map(params![now(), now(), limit], |r| {
                Ok(Job {
                    id: r.get(0)?,
                    kind: r.get(1)?,
                    payload: r.get(2)?,
                    priority: r.get(3)?,
                    attempts: r.get(4)?,
                    max_attempts: r.get(5)?,
                    run_at: r.get(6)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Complete a job: run `effects` and mark the job done (or reschedule it
    /// `secs` into the future) in ONE transaction. Either everything lands
    /// or nothing does — a revision row never exists without its job
    /// completion, and vice versa.
    pub fn complete<F>(&self, id: i64, resched: Option<i64>, effects: F) -> Result<()>
    where
        F: FnOnce(&Transaction) -> Result<()>,
    {
        let mut conn = self.0.lock();
        let tx = conn.transaction()?;
        let updated = match resched {
            Some(secs) => tx.execute(
                "UPDATE jobs SET status='pending', run_at=?2, last_error=NULL WHERE id=?1",
                params![id, now() + secs],
            )?,
            None => tx.execute("UPDATE jobs SET status='done' WHERE id=?1", params![id])?,
        };
        if updated == 0 {
            bail!("job {id} vanished before completion");
        }
        effects(&tx)?;
        tx.commit()?;
        Ok(())
    }

    /// Complete an ephemeral (event) job: run `effects` and DELETE the row
    /// in ONE transaction. There is no `done` state — the row lives exactly
    /// from signal to successful execution, so a signal arriving mid-run
    /// simply inserts a fresh row that runs after this one. Nothing is ever
    /// swallowed, and no tombstone can go stale.
    pub fn complete_ephemeral<F>(&self, id: i64, effects: F) -> Result<()>
    where
        F: FnOnce(&Transaction) -> Result<()>,
    {
        let mut conn = self.0.lock();
        let tx = conn.transaction()?;
        let deleted = tx.execute("DELETE FROM jobs WHERE id=?1", params![id])?;
        if deleted == 0 {
            bail!("job {id} vanished before completion");
        }
        effects(&tx)?;
        tx.commit()?;
        Ok(())
    }

    /// Fail a job: retry with exponential backoff, or dead-letter once
    /// attempts are exhausted (`max_attempts <= 0` = never). `permanent`
    /// failures (403, fatal parse) dead-letter immediately.
    pub fn fail(&self, id: i64, error: &str, permanent: bool) -> Result<FailOutcome> {
        let conn = self.0.lock();
        let (attempts, max_attempts): (i64, i64) = conn
            .query_row(
                "SELECT attempts, max_attempts FROM jobs WHERE id=?1",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .context("job vanished before failure was recorded")?;
        let outcome = if permanent || (max_attempts > 0 && attempts >= max_attempts) {
            conn.execute(
                "UPDATE jobs SET status='dead', last_error=?2 WHERE id=?1",
                params![id, error],
            )?;
            FailOutcome::Dead
        } else {
            let run_at = now() + backoff_secs(attempts);
            conn.execute(
                "UPDATE jobs SET status='pending', run_at=?3, last_error=?2 WHERE id=?1",
                params![id, error, run_at],
            )?;
            FailOutcome::Retry { run_at }
        };
        Ok(outcome)
    }

    // ── Read helpers for workers ──

    /// Escape hatch for callers that need raw SQL (tests, `import-legacy`,
    /// forensics). Everything mutating queue state should stay in here.
    #[allow(dead_code)]
    pub fn with_conn<R>(&self, f: impl FnOnce(&mut Connection) -> R) -> R {
        let mut conn = self.0.lock();
        f(&mut conn)
    }

    /// `meta` table lookup.
    pub fn meta_get(&self, key: &str) -> Result<Option<String>> {
        let conn = self.0.lock();
        Ok(conn
            .query_row("SELECT value FROM meta WHERE key=?1", params![key], |r| {
                r.get(0)
            })
            .optional()?)
    }

    /// Highest revision number known for a page, -1 if none.
    pub fn max_rev(&self, page_id: i64) -> Result<i64> {
        let conn = self.0.lock();
        Ok(conn
            .query_row(
                "SELECT COALESCE(MAX(rev_no), -1) FROM revisions WHERE page_id=?1",
                params![page_id],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(-1))
    }

    /// (content, ts, author) of a revision row; None if the row is absent.
    pub fn revision_state(
        &self,
        page_id: i64,
        rev_no: i64,
    ) -> Result<Option<(Option<String>, i64, i64)>> {
        let conn = self.0.lock();
        Ok(conn
            .query_row(
                "SELECT content, ts, author FROM revisions WHERE page_id=?1 AND rev_no=?2",
                params![page_id, rev_no],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?)
    }

    /// Whether a revision is recorded in `denied_revisions` (permanently
    /// unobtainable). Terminal — callers skip re-attempts.
    pub fn is_denied(&self, page_id: i64, rev_no: i64) -> Result<bool> {
        let conn = self.0.lock();
        Ok(conn
            .query_row(
                "SELECT 1 FROM denied_revisions WHERE page_id=?1 AND rev_no=?2",
                params![page_id, rev_no],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    /// `files.status` for a path; None if the row is absent.
    pub fn file_status(&self, path: &str) -> Result<Option<String>> {
        let conn = self.0.lock();
        Ok(conn
            .query_row(
                "SELECT status FROM files WHERE path=?1",
                params![path],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// `(status, sha256)` for a files-row path; None if the row is absent.
    /// The theme crawl uses this to read already-saved CSS straight from its
    /// blob instead of re-fetching.
    pub fn file_row(&self, path: &str) -> Result<Option<(String, Option<String>)>> {
        let conn = self.0.lock();
        Ok(conn
            .query_row(
                "SELECT status, sha256 FROM files WHERE path=?1",
                params![path],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?)
    }

    /// Earliest `run_at` among pending jobs (None = queue drained of
    /// schedulable work). The daemon sleeps until then.
    pub fn next_wake(&self) -> Result<Option<i64>> {
        let conn = self.0.lock();
        Ok(conn
            .query_row(
                "SELECT MIN(run_at) FROM jobs WHERE status='pending'",
                [],
                |r| r.get::<_, Option<i64>>(0),
            )
            .optional()?
            .flatten())
    }

    // ── Publication (out/) ──

    /// Every page, with its stored-revision count and how many revisions the
    /// last publication packed. The publisher repacks when they differ.
    pub fn pages_to_pack(&self) -> Result<Vec<PageToPack>> {
        let conn = self.0.lock();
        let mut stmt = conn.prepare(
            "SELECT p.page_id, p.slug, p.title, p.tags,
                    (SELECT count(*) FROM revisions r
                      WHERE r.page_id=p.page_id AND r.content IS NOT NULL),
                    COALESCE(o.packed_revs, 0)
             FROM pages p LEFT JOIN out_state o ON o.page_id=p.page_id
             ORDER BY p.page_id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(PageToPack {
                page_id: r.get(0)?,
                slug: r.get(1)?,
                title: r.get(2)?,
                tags: parse_tags(&r.get::<_, String>(3)?),
                stored_count: r.get(4)?,
                packed_revs: r.get(5)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// All content-bearing revisions of a page, oldest first.
    pub fn page_revisions_full(&self, page_id: i64) -> Result<Vec<FullRevision>> {
        let conn = self.0.lock();
        let mut stmt = conn.prepare(
            "SELECT rev_no, rev_id, ts, author, content
             FROM revisions WHERE page_id=?1 AND content IS NOT NULL
             ORDER BY rev_no",
        )?;
        let rows = stmt.query_map(params![page_id], |r| {
            Ok(FullRevision {
                rev_no: r.get(0)?,
                rev_id: r.get(1)?,
                ts: r.get(2)?,
                author: r.get(3)?,
                content: r.get::<_, Option<String>>(4)?.unwrap_or_default(),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Record that `n` revisions are packed into the page's archive.
    pub fn set_packed(&self, page_id: i64, n: i64) -> Result<()> {
        let conn = self.0.lock();
        conn.execute(
            "INSERT INTO out_state(page_id, packed_revs, packed_at) VALUES(?1, ?2, ?3)
             ON CONFLICT(page_id) DO UPDATE
             SET packed_revs=excluded.packed_revs, packed_at=excluded.packed_at",
            params![page_id, n, now()],
        )?;
        Ok(())
    }

    /// Rows for `pages.json`: identity + coverage per page, by page_id.
    pub fn pages_manifest(&self) -> Result<Vec<PageManifestRow>> {
        let conn = self.0.lock();
        let mut stmt = conn.prepare(
            "SELECT p.page_id, p.slug, p.title, p.tags,
                    (SELECT count(*) FROM revisions r
                      WHERE r.page_id=p.page_id AND r.content IS NOT NULL),
                    (SELECT count(*) FROM revisions r WHERE r.page_id=p.page_id),
                    COALESCE((SELECT max(rev_no) FROM revisions r
                               WHERE r.page_id=p.page_id), 0)
             FROM pages p ORDER BY p.page_id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(PageManifestRow {
                page_id: r.get(0)?,
                slug: r.get(1)?,
                title: r.get(2)?,
                tags: parse_tags(&r.get::<_, String>(3)?),
                stored_count: r.get(4)?,
                known_count: r.get(5)?,
                max_rev: r.get(6)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Rows for `files.json`: path, hash, size, status, media type — by path.
    pub fn files_manifest(&self) -> Result<Vec<FileManifestRow>> {
        let conn = self.0.lock();
        let mut stmt = conn
            .prepare("SELECT path, sha256, size, status, content_type FROM files ORDER BY path")?;
        let rows = stmt.query_map([], |r| {
            Ok(FileManifestRow {
                path: r.get(0)?,
                sha256: r.get(1)?,
                size: r.get(2)?,
                status: r.get(3)?,
                content_type: r.get(4)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // ── Observability (status) ──

    pub fn stats(&self) -> Result<SiteStats> {
        let conn = self.0.lock();
        let mut jobs = Vec::new();
        {
            let mut stmt = conn.prepare("SELECT status, count(*) FROM jobs GROUP BY status")?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
            for row in rows {
                jobs.push(row?);
            }
        }
        let count = |sql: &str| -> Result<i64> { Ok(conn.query_row(sql, [], |r| r.get(0))?) };
        Ok(SiteStats {
            jobs,
            pages: count("SELECT count(*) FROM pages")?,
            revisions: count("SELECT count(*) FROM revisions WHERE content IS NOT NULL")?,
            revision_meta: count("SELECT count(*) FROM revisions")?,
            denied: count("SELECT count(*) FROM denied_revisions")?,
            files_pending: count("SELECT count(*) FROM files WHERE status='pending'")?,
            files_saved: count("SELECT count(*) FROM files WHERE status='saved'")?,
            files_missing: count("SELECT count(*) FROM files WHERE status='missing'")?,
            dead_errors: {
                let mut stmt = conn.prepare(
                    "SELECT kind, last_error FROM jobs WHERE status='dead' ORDER BY id LIMIT 5",
                )?;
                let rows =
                    stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            },
        })
    }
}

/// `{repo}/meta/{site}/site.db` → site name + `{repo}/out/{site}` for the
/// Rust repair steps (the layout contract `config.rs` documents). Odd
/// paths degrade: an empty site classifies every reference host as
/// off-site, a bogus out dir just leaves blob GC a no-op.
fn repair_ctx_for(db_path: &Path) -> RepairCtx {
    let site_dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let site = site_dir
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let out_dir = site_dir
        .parent()
        .and_then(|meta_dir| meta_dir.parent())
        .map(|repo| repo.join("out").join(&site))
        .unwrap_or_else(|| PathBuf::from("out").join(&site));
    RepairCtx { site, out_dir }
}

/// Insert jobs inside a transaction. Non-ephemeral kinds deduplicate on
/// (kind, payload); `resurrect` jobs re-arm finished `done`/`dead` rows
/// (see `NewJob.resurrect`). Ephemeral kinds always insert.
pub fn enqueue_on(tx: &Transaction, jobs: &[NewJob]) -> Result<usize> {
    let mut inserted = 0;
    for j in jobs {
        inserted += if crate::jobs::is_ephemeral(&j.kind) {
            // Event jobs have no identity to dedupe on and no tombstone:
            // every signal is its own row, completed by deletion.
            tx.execute(
                "INSERT INTO jobs(kind, payload, priority, run_at, max_attempts, created_at)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    j.kind,
                    j.payload,
                    j.priority,
                    j.run_at,
                    j.max_attempts,
                    now()
                ],
            )?
        } else if j.resurrect {
            // The conflict target carries the partial index's WHERE (SQLite
            // requires it verbatim to match); ephemeral kinds never reach
            // this branch.
            tx.execute(
                "INSERT INTO jobs(kind, payload, priority, run_at, max_attempts, created_at)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(kind, payload) WHERE kind <> 'page.sync' DO UPDATE SET
                   status='pending', run_at=excluded.run_at, priority=excluded.priority,
                   attempts=0, last_error=NULL
                 WHERE jobs.status IN ('done', 'dead')",
                params![
                    j.kind,
                    j.payload,
                    j.priority,
                    j.run_at,
                    j.max_attempts,
                    now()
                ],
            )?
        } else {
            tx.execute(
                "INSERT OR IGNORE INTO jobs(kind, payload, priority, run_at, max_attempts, created_at)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    j.kind,
                    j.payload,
                    j.priority,
                    j.run_at,
                    j.max_attempts,
                    now()
                ],
            )?
        };
    }
    Ok(inserted)
}

fn has_table(tx: &Transaction, name: &str) -> bool {
    tx.query_row(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
        params![name],
        |_| Ok(()),
    )
    .optional()
    .unwrap_or(None)
    .is_some()
}

/// Exponential backoff after the `attempts`-th failure: 10s, 20s, 40s, …
/// capped at one hour.
pub fn backoff_secs(attempts: i64) -> i64 {
    let exp = (attempts - 1).clamp(0, 10) as u32;
    (10i64 << exp).min(3600)
}

// ── Stats ──

#[derive(Debug, Default)]
pub struct SiteStats {
    pub jobs: Vec<(String, i64)>,
    pub pages: i64,
    pub revisions: i64,
    pub revision_meta: i64,
    pub denied: i64,
    pub files_pending: i64,
    pub files_saved: i64,
    pub files_missing: i64,
    pub dead_errors: Vec<(String, String)>,
}

// ── Publication rows ──

#[derive(Debug, Clone)]
pub struct PageToPack {
    pub page_id: i64,
    pub slug: String,
    pub title: String,
    pub tags: Vec<String>,
    /// Revisions with stored content.
    pub stored_count: i64,
    /// Revisions the last publication packed (out_state).
    pub packed_revs: i64,
}

impl PageToPack {
    pub fn id_str(&self) -> String {
        self.page_id.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct FullRevision {
    pub rev_no: i64,
    pub rev_id: String,
    pub ts: i64,
    pub author: i64,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct PageManifestRow {
    pub page_id: i64,
    pub slug: String,
    pub title: String,
    pub tags: Vec<String>,
    pub stored_count: i64,
    pub known_count: i64,
    pub max_rev: i64,
}

impl PageManifestRow {
    pub fn id_str(&self) -> String {
        self.page_id.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct FileManifestRow {
    pub path: String,
    pub sha256: Option<String>,
    pub size: Option<i64>,
    pub status: String,
    pub content_type: Option<String>,
}

fn parse_tags(json: &str) -> Vec<String> {
    serde_json::from_str(json).unwrap_or_default()
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;

    fn site(name: &str) -> (tempfile::TempDir, Db, DaemonLock) {
        let dir = tempfile::tempdir().unwrap();
        let (db, lock) = Db::open_locked(&dir.path().join(format!("{name}.db"))).unwrap();
        (dir, db, lock)
    }

    fn nj(kind: &str, payload: &str) -> NewJob {
        NewJob {
            kind: kind.into(),
            payload: payload.into(),
            priority: 0,
            run_at: 0,
            max_attempts: 3,
            resurrect: false,
        }
    }

    #[test]
    fn enqueue_dedupes_on_kind_payload() {
        let (_d, db, _l) = site("dedup");
        assert_eq!(
            db.enqueue(&[nj("a", "1"), nj("a", "1"), nj("a", "2")])
                .unwrap(),
            2
        );
        assert_eq!(db.enqueue(&[nj("a", "1")]).unwrap(), 0);
        assert_eq!(db.claim(10).unwrap().len(), 2); // the two distinct jobs, claimed
    }

    #[test]
    fn claim_respects_run_at_priority_and_marks_running() {
        let (_d, db, _l) = site("claim");
        let mut future = nj("f", "1");
        future.run_at = now() + 100;
        let mut urgent = nj("u", "1");
        urgent.priority = 10;
        db.enqueue(&[future, urgent, nj("z", "1")]).unwrap();

        let claimed = db.claim(10).unwrap();
        assert_eq!(claimed.len(), 2); // future job not claimable yet
        assert_eq!(claimed[0].kind, "u"); // priority first
        assert_eq!(claimed[1].kind, "z");
        // Second claim: only the future job is pending, and it is not due.
        assert!(db.claim(10).unwrap().is_empty());
    }

    /// v3 must retune seated rows to exactly the current `jobs::prio`
    /// values — the migration's SQL literals have no compiler tie to the
    /// constants, so this test is the tie.
    #[test]
    fn v3_retune_matches_prio_table() {
        let (_d, db, _l) = site("retune");
        db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO jobs(kind, payload, priority, created_at) VALUES
                 ('shell.sync',  '{}', -5, ?1),
                 ('file.fetch',  '{}',  0, ?1),
                 ('theme.crawl', '{}',  0, ?1),
                 ('page.sync',   '{}', 10, ?1),
                 ('revision.fetch', '{}', 8, ?1)",
                params![now()],
            )
            .unwrap();
            conn.execute_batch(RETUNE_PRIORITIES_V3).unwrap();
        });
        let prio_of = |kind: &str| {
            db.with_conn(|c| {
                c.query_row(
                    "SELECT priority FROM jobs WHERE kind=?1",
                    params![kind],
                    |r| r.get::<_, i64>(0),
                )
            })
            .unwrap()
        };
        assert_eq!(prio_of("shell.sync"), crate::jobs::prio::SHELL);
        assert_eq!(prio_of("file.fetch"), crate::jobs::prio::FILE);
        assert_eq!(prio_of("theme.crawl"), crate::jobs::prio::FILE);
        assert_eq!(prio_of("page.sync"), crate::jobs::prio::PAGE);
        // The head/old split is per-row and unchanged — v3 must not
        // flatten it.
        assert_eq!(prio_of("revision.fetch"), crate::jobs::prio::REVISION_OLD);
    }

    /// v4 must retune seated `out.update` rows to exactly
    /// `jobs::prio::OUT` — the SQL literal has no compiler tie to the
    /// constant, so this test is the tie.
    #[test]
    fn v4_retune_matches_prio_table() {
        let (_d, db, _l) = site("retune4");
        db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO jobs(kind, payload, priority, created_at) VALUES
                 ('out.update', '{}', -10, ?1)",
                params![now()],
            )
            .unwrap();
            conn.execute_batch(RETUNE_PRIORITIES_V4).unwrap();
        });
        let prio: i64 = db
            .with_conn(|c| {
                c.query_row(
                    "SELECT priority FROM jobs WHERE kind='out.update'",
                    [],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert_eq!(prio, crate::jobs::prio::OUT);
    }

    /// v5 must pull a seated shell.sync's next run to "now" (run_at=0) so
    /// every existing site re-fetches its homepage and gains a `landing`
    /// — and must leave every other schedule alone.
    #[test]
    fn v5_rearms_shell_sync_only() {
        let (_d, db, _l) = site("rearm5");
        let far = now() + 86_400;
        db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO jobs(kind, payload, run_at, created_at) VALUES
                 ('shell.sync', '{}', ?1, ?1),
                 ('out.update', '{}', ?1, ?1)",
                params![far],
            )
            .unwrap();
            conn.execute_batch(REARM_SHELL_SYNC_V5).unwrap();
        });
        let run_at = |kind: &str| {
            db.with_conn(|c| {
                c.query_row(
                    "SELECT run_at FROM jobs WHERE kind=?1",
                    params![kind],
                    |r| r.get::<_, i64>(0),
                )
            })
            .unwrap()
        };
        assert_eq!(run_at("shell.sync"), 0);
        assert_eq!(run_at("out.update"), far);
    }

    #[test]
    fn complete_is_atomic_effects_plus_job_state() {
        let (_d, db, _l) = site("complete");
        db.enqueue(&[nj("rev", "1")]).unwrap();
        let job = db.claim(1).unwrap().pop().unwrap();

        // Happy path: effect + done in one commit.
        db.complete(job.id, None, |tx| {
            tx.execute(
                "INSERT INTO pages(page_id, slug, discovered_at) VALUES(1, 'x', ?1)",
                params![now()],
            )?;
            Ok(())
        })
        .unwrap();
        let done: i64 =
            db.0.lock()
                .query_row("SELECT count(*) FROM jobs WHERE status='done'", [], |r| {
                    r.get(0)
                })
                .unwrap();
        assert_eq!(done, 1);

        // Failing effect: job stays running, effect rolled back.
        db.enqueue(&[nj("rev", "2")]).unwrap();
        let job = db.claim(1).unwrap().pop().unwrap();
        let err = db
            .complete(job.id, None, |tx| {
                tx.execute(
                    "INSERT INTO pages(page_id, slug, discovered_at) VALUES(2, 'y', ?1)",
                    params![now()],
                )?;
                anyhow::bail!("boom");
            })
            .unwrap_err();
        assert!(err.to_string().contains("boom"));
        let pages: i64 =
            db.0.lock()
                .query_row("SELECT count(*) FROM pages", [], |r| r.get(0))
                .unwrap();
        assert_eq!(pages, 1); // page 2 rolled back with the failed job
    }

    #[test]
    fn child_jobs_enqueue_transactionally() {
        let (_d, db, _l) = site("children");
        db.enqueue(&[nj("page", "1")]).unwrap();
        let job = db.claim(1).unwrap().pop().unwrap();
        db.complete(job.id, None, |tx| {
            enqueue_on(tx, &[nj("rev", "1"), nj("rev", "2"), nj("file", "a")])?;
            Ok(())
        })
        .unwrap();
        let pending: i64 =
            db.0.lock()
                .query_row(
                    "SELECT count(*) FROM jobs WHERE status='pending'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
        assert_eq!(pending, 3);
    }

    #[test]
    fn periodic_jobs_reschedule_instead_of_finishing() {
        let (_d, db, _l) = site("periodic");
        let mut disc = nj("discover", "{}");
        disc.max_attempts = 0; // unlimited
        db.enqueue(&[disc]).unwrap();
        let job = db.claim(1).unwrap().pop().unwrap();
        db.complete(job.id, Some(1800), |_| Ok(())).unwrap();

        // Not claimable now; claimable "later".
        assert!(db.claim(1).unwrap().is_empty());
        db.0.lock()
            .execute("UPDATE jobs SET run_at=?1", params![now() - 1])
            .unwrap();
        let again = db.claim(1).unwrap().pop().unwrap();
        assert_eq!(again.kind, "discover");
        assert_eq!(again.attempts, 2);
    }

    #[test]
    fn fail_retries_with_backoff_then_dies() {
        let (_d, db, _l) = site("fail");
        db.enqueue(&[nj("f", "1")]).unwrap(); // max_attempts = 3
        let j1 = db.claim(1).unwrap().pop().unwrap();
        assert!(matches!(
            db.fail(j1.id, "timeout", false).unwrap(),
            FailOutcome::Retry { .. }
        ));
        // Backoff gates the next claim; simulate it elapsing.
        db.0.lock().execute("UPDATE jobs SET run_at=0", []).unwrap();
        let j2 = db.claim(1).unwrap().pop().unwrap();
        assert_eq!(j2.attempts, 2);
        // run_at in the future → not claimable until backoff elapses
        assert!(db.claim(1).unwrap().is_empty());
        assert!(matches!(
            db.fail(j2.id, "timeout", false).unwrap(),
            FailOutcome::Retry { .. }
        ));
        db.0.lock().execute("UPDATE jobs SET run_at=0", []).unwrap();
        let j3 = db.claim(1).unwrap().pop().unwrap();
        assert_eq!(j3.attempts, 3);
        assert_eq!(db.fail(j3.id, "timeout", false).unwrap(), FailOutcome::Dead);
        let err: String =
            db.0.lock()
                .query_row(
                    "SELECT last_error FROM jobs WHERE id=?1",
                    params![j3.id],
                    |r| r.get(0),
                )
                .unwrap();
        assert_eq!(err, "timeout");
    }

    #[test]
    fn permanent_failure_dead_letters_immediately() {
        let (_d, db, _l) = site("perm");
        db.enqueue(&[nj("p", "1")]).unwrap();
        let job = db.claim(1).unwrap().pop().unwrap();
        assert_eq!(
            db.fail(job.id, "no_permission", true).unwrap(),
            FailOutcome::Dead
        );
    }

    #[test]
    fn unlimited_jobs_never_die() {
        let (_d, db, _l) = site("unlimited");
        let mut j = nj("u", "1");
        j.max_attempts = 0;
        db.enqueue(&[j]).unwrap();
        for _ in 0..20 {
            let job = db.claim(1).unwrap().pop().unwrap();
            assert!(matches!(
                db.fail(job.id, "timeout", false).unwrap(),
                FailOutcome::Retry { .. }
            ));
            db.0.lock().execute("UPDATE jobs SET run_at=0", []).unwrap();
        }
    }

    #[test]
    fn resurrect_rearms_only_done_jobs() {
        let (_d, db, _l) = site("resurrect");
        let mut r = nj("theme.crawl", "x"); // any non-ephemeral kind
        r.resurrect = true;
        db.enqueue(&[r.clone()]).unwrap();
        let j = db.claim(1).unwrap().pop().unwrap();
        db.complete(j.id, None, |_| Ok(())).unwrap();
        // done → resurrect re-arms it.
        assert_eq!(db.enqueue(&[r.clone()]).unwrap(), 1);
        let again = db.claim(1).unwrap().pop().unwrap();
        assert_eq!(again.id, j.id);
        assert_eq!(again.attempts, 1); // reset by resurrect
        // pending → second enqueue is a no-op while it runs.
        assert_eq!(db.enqueue(&[r.clone()]).unwrap(), 0);
    }

    #[test]
    fn ephemeral_signals_are_rows_not_dedupe_keys() {
        let (_d, db, _l) = site("ephemeral");
        let e = nj("page.sync", r#"{"slug":"x"}"#);
        db.enqueue(&[e.clone(), e.clone()]).unwrap(); // two signals, two rows
        let j1 = db.claim(1).unwrap().pop().unwrap();
        // A signal arriving while j1 runs is a THIRD row — not swallowed.
        assert_eq!(db.enqueue(&[e]).unwrap(), 1);
        let j2 = db.claim(1).unwrap().pop().unwrap();
        assert_ne!(j1.id, j2.id);
        db.complete_ephemeral(j1.id, |_| Ok(())).unwrap();
        let j1_gone: i64 =
            db.0.lock()
                .query_row(
                    "SELECT count(*) FROM jobs WHERE id=?1",
                    params![j1.id],
                    |r| r.get(0),
                )
                .unwrap();
        assert_eq!(j1_gone, 0); // deleted, never 'done'
        let done: i64 =
            db.0.lock()
                .query_row("SELECT count(*) FROM jobs WHERE status='done'", [], |r| {
                    r.get(0)
                })
                .unwrap();
        assert_eq!(done, 0);
    }

    #[test]
    fn dead_ephemeral_row_does_not_block_new_signals() {
        let (_d, db, _l) = site("ephemeral-dead");
        let e = nj("page.sync", r#"{"slug":"y"}"#);
        db.enqueue(std::slice::from_ref(&e)).unwrap();
        let j = db.claim(1).unwrap().pop().unwrap();
        assert_eq!(db.fail(j.id, "boom", true).unwrap(), FailOutcome::Dead);
        // New signal = new row; the dead one stays for audit, blocks nothing.
        assert_eq!(db.enqueue(&[e]).unwrap(), 1);
        let again = db.claim(1).unwrap().pop().unwrap();
        assert_ne!(again.id, j.id);
    }

    #[test]
    fn reopen_recovers_stale_running_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recover.db");
        {
            let (db, _l) = Db::open_locked(&path).unwrap();
            db.enqueue(&[nj("r", "1")]).unwrap();
            let _claimed = db.claim(1).unwrap().pop().unwrap();
            // "crash": claimed, never completed; drop everything
        }
        let (db2, _l2) = Db::open_locked(&path).unwrap();
        let running: i64 = db2
            .0
            .lock()
            .query_row(
                "SELECT count(*) FROM jobs WHERE status='running'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(running, 0);
        let job = db2.claim(1).unwrap().pop().unwrap();
        assert_eq!(job.attempts, 2); // recovered and re-claimed
    }

    #[test]
    fn open_locked_refuses_second_daemon() {
        let (_d, db, _l) = site("lock");
        let path = {
            let conn = db.0.lock();
            Path::new(conn.path().unwrap()).to_path_buf()
        };
        assert!(Db::open_locked(&path).is_err());
    }

    /// v6 must repair the broken-era files state: delete garbage paths
    /// (and their pending jobs), reset soft-404 HTML rows to pending with
    /// the blob fields cleared, re-seed extraction over stored revisions
    /// (canonical absolute URLs), lift/merge every remaining key onto its
    /// canonical URL, re-point the fetch jobs, arm them, and GC the junk
    /// blob once no row references it.
    #[test]
    fn v6_repair_reextracts_resets_and_gcs() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let db_path = repo.join("meta").join("demo").join("site.db");
        let (db, _l) = Db::open_locked(&db_path).unwrap(); // fresh → all steps apply
        let ctx = RepairCtx {
            site: "demo".into(),
            out_dir: repo.join("out").join("demo"),
        };

        const JUNK_SHA: &str = "eabe424dd70c56173c2cfcfe8ca6b328ef2077d6ce9b3243540148a2d76f20ab";
        const GOOD_SHA: &str = "00000000000000000000000000000000000000000000000000000000000000ff";
        let junk_payload = serde_json::json!({ "path": "local--files/q/d.jpg|width=5" }).to_string();
        let dupe_payload =
            serde_json::json!({ "path": "http://sandbox.wdfiles.com/local--files/m/x.png" })
                .to_string();
        db.with_conn(|conn| {
            // Roll the schema back to v5 so the next migrate() replays v6.
            conn.execute("UPDATE meta SET value='5' WHERE key='schema_version'", [])
                .unwrap();
            conn.execute(
                "INSERT INTO pages(page_id, slug, discovered_at) VALUES(1, 'p', ?1)",
                params![now()],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO revisions(page_id, rev_no, rev_id, ts, author, content, fetched_at)
                 VALUES(1, 0, 'r', 1, 1, ?1, 1)",
                params![concat!(
                    "rel local--files/p/a.png ",
                    "own http://demo.wdfiles.com/local--files/p/b.png ",
                    "foreign http://sandbox.wikidot.com/local--files/p/c.png|caption=x ",
                    "pipe local--files/q/d.jpg|width=5 ",
                    "pct local--files/nav:side/discord.png",
                )],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO files(path, url, sha256, size, status, first_seen, saved_at, content_type) VALUES
                 ('local--files/p/a.png', 'http://demo.wikidot.com/local--files/p/a.png',
                  NULL, NULL, 'pending', ?1, NULL, NULL),
                 ('local--files/sbx/c.png', 'http://demo.wikidot.com/local--files/sbx/c.png',
                  ?2, 404, 'saved', ?1, ?1, 'text/html'),
                 -- one file, two host spellings: saved bytes + a pending twin
                 ('http://sandbox.wikidot.com/local--files/m/x.png',
                  'https://sandbox.wikidot.com/local--files/m/x.png',
                  ?3, 10, 'saved', ?1, ?1, 'image/png'),
                 ('http://sandbox.wdfiles.com/local--files/m/x.png', NULL,
                  NULL, NULL, 'pending', ?1, NULL, NULL),
                 -- pre-v6 row spelled with %3A: must converge with the
                 -- ':'-spelled reference the re-extraction seeds
                 ('local--files/nav%3Aside/discord.png',
                  'http://demo.wikidot.com/local--files/nav%3Aside/discord.png',
                  ?3, 20, 'saved', ?1, ?1, 'image/png'),
                 ('local--files/q/d.jpg|width=5', NULL, NULL, NULL, 'pending', ?1, NULL, NULL)",
                params![now(), JUNK_SHA, GOOD_SHA],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO jobs(kind, payload, status, created_at) VALUES
                 ('file.fetch', ?1, 'pending', ?3),
                 ('file.fetch', ?2, 'pending', ?3)",
                params![junk_payload, dupe_payload, now()],
            )
            .unwrap();
        });
        // Blobs on disk: the junk one must be GC'd, the good one kept.
        let junk_blob = crate::blobs::blob_path(&ctx.out_dir, JUNK_SHA);
        let good_blob = crate::blobs::blob_path(&ctx.out_dir, GOOD_SHA);
        for (blob, bytes) in [(junk_blob.clone(), b"<html>error page</html>".as_slice()), (good_blob.clone(), b"real png bytes")]
        {
            std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
            std::fs::write(&blob, bytes).unwrap();
        }

        db.migrate(&ctx).unwrap();

        db.with_conn(|conn| {
            let status = |path: &str| {
                conn.query_row(
                    "SELECT status, sha256, content_type FROM files WHERE path=?1",
                    params![path],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, Option<String>>(2)?)),
                )
                .optional()
                .unwrap()
            };
            // Own-site references (bare and own-host absolute) lift onto
            // the main domain as absolute URLs.
            assert_eq!(
                status("http://demo.wikidot.com/local--files/p/a.png"),
                Some(("pending".into(), None, None))
            );
            assert_eq!(
                status("http://demo.wikidot.com/local--files/p/b.png"),
                Some(("pending".into(), None, None))
            );
            // Off-site reference keeps its site, canonical wikidot host.
            assert_eq!(
                status("http://sandbox.wikidot.com/local--files/p/c.png"),
                Some(("pending".into(), None, None))
            );
            // Pipe garbage stopped at '|' seeds the real path instead.
            assert_eq!(
                status("http://demo.wikidot.com/local--files/q/d.jpg"),
                Some(("pending".into(), None, None))
            );
            // Soft-404 row reset to pending, blob fields cleared, lifted.
            assert_eq!(
                status("http://demo.wikidot.com/local--files/sbx/c.png"),
                Some(("pending".into(), None, None))
            );
            // The two spellings of the sandbox file merged: the saved row
            // survives under the canonical key, the pending twin is gone.
            assert_eq!(
                status("http://sandbox.wikidot.com/local--files/m/x.png"),
                Some(("saved".into(), Some(GOOD_SHA.to_string()), Some("image/png".to_string())))
            );
            // The %3A-spelled legacy row converged with the ':'-spelled
            // reference: one saved row, decoded key.
            assert_eq!(
                status("http://demo.wikidot.com/local--files/nav:side/discord.png"),
                Some(("saved".into(), Some(GOOD_SHA.to_string()), Some("image/png".to_string())))
            );
            // The old keys are all gone…
            for gone in [
                "local--files/p/a.png",
                "local--files/sbx/c.png",
                "http://sandbox.wdfiles.com/local--files/m/x.png",
                "local--files/nav%3Aside/discord.png",
                "local--files/q/d.jpg|width=5",
            ] {
                assert_eq!(status(gone), None, "{gone} should be gone");
            }
            // …and no file.fetch payload references a non-canonical path:
            // the junk job died with its row, the dupe re-pointed.
            let stale: i64 = conn
                .query_row(
                    "SELECT count(*) FROM jobs WHERE kind='file.fetch' AND payload IN
                       (?1, ?2)",
                    params![junk_payload, dupe_payload],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(stale, 0);
            // Every pending row carries an armed fetch job with its
            // canonical payload.
            let armed: i64 = conn
                .query_row(
                    "SELECT count(*) FROM jobs WHERE kind='file.fetch' AND status='pending'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(armed, 5); // a, b, c.png, d.jpg, sbx/c.png (reset)
        });
        // Junk blob GC'd now that nothing references its sha; good one kept.
        assert!(!junk_blob.exists());
        assert!(good_blob.exists());
    }
}
