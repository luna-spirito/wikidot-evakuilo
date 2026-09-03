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
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
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

const MIGRATIONS: &[&str] = &[SCHEMA_V1, SCHEMA_V2, RETUNE_PRIORITIES_V3];

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
            db.migrate().context("migrating schema")?;
        }
        Ok(db)
    }

    /// Daemon entry point: open + take the site lock + recover stale jobs.
    pub fn open_locked(path: &Path) -> Result<(Db, DaemonLock)> {
        let lock_path = path
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

    fn migrate(&self) -> Result<()> {
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
        for (i, migration) in MIGRATIONS.iter().enumerate().skip(version as usize) {
            tx.execute_batch(migration)
                .with_context(|| format!("applying migration v{}", i + 1))?;
            tx.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?1)",
                params![(i + 1).to_string()],
            )?;
        }
        tx.commit()?;
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
        let mut stmt = conn.prepare(
            "SELECT path, sha256, size, status, content_type FROM files ORDER BY path",
        )?;
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
}
