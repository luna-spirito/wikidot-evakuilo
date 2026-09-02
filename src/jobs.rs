//! Job taxonomy: kinds, priorities, payload shapes, constructors.
//!
//! ## Purity classes
//!
//! Every kind belongs to one of three classes, and the class — not the
//! kind — decides its queue lifecycle (`db` module docs describe the two
//! lifecycles):
//!
//! * **immutable target** — the payload names something that cannot
//!   change, so a `done` tombstone is truthful and dedup on
//!   (kind, payload) is meaningful.
//!   - `revision.fetch`: a Wikidot revision's wikitext is fixed forever;
//!     the only transition is *disappearing* (moderator hide/delete →
//!     terminal `denied_revisions` row).
//!   - `file.fetch`: `path → bytes` is FROZEN AT FIRST OBSERVATION, BY
//!     POLICY. A re-upload under the same name upstream is deliberately
//!     not tracked (evacuation snapshot, v1 semantics); an observed 404
//!     (`missing`) is terminal the same way. This is an assumption we
//!     choose to make, not one Wikidot guarantees.
//! * **event** — the payload is a *request to reconcile now*, not a fixed
//!   target: `page.sync` (the page behind a slug can change at any
//!   instant; Wikidot offers no guarantees). No identity → no dedup (the
//!   partial unique index skips exactly this kind), no tombstone: every
//!   signal inserts a row, completion deletes it
//!   (`Db::complete_ephemeral`). Execution is catch-up, so one run
//!   absorbs any number of coalesced signals.
//! * **state-diff** — re-armed by comparing observed state against a
//!   marker, not by consuming events. `theme.crawl`: the CSS URL
//!   (including its `/N` revision component) is the identity; content
//!   behind a fixed URL is frozen like file paths, a theme edit publishes
//!   a new root URL, the `theme.crawled` marker stops matching, and the
//!   next `shell.sync` re-enqueues. A swallowed enqueue therefore
//!   self-heals — no race to fix.
//!
//! Periodic singletons (`discover`, `shell.sync`, `out.update`) never go
//! `done`: dedup on their constant `{}` payload is just the singleton
//! guard. Their cadence is read from the live config at completion time,
//! never from the payload — the payload is the dedup key, and embedding a
//! tunable there would fork a second eternal copy of the job on every
//! config change.
//!
//! ## Priorities
//!
//! Priorities decide claim order when the queue backs up (it won't, at one
//! request per `rate_limit_ms`): discovery and page resolution first, then
//! newest revisions, then files, then publication.

use serde_json::json;

use crate::db::NewJob;

pub mod kind {
    /// Periodic RecentChanges scan (self-rescheduling).
    pub const DISCOVER: &str = "discover";
    /// Resolve a slug: page identity, revision list, file listing.
    pub const PAGE_SYNC: &str = "page.sync";
    /// Fetch one revision's wikitext by rev_id.
    pub const REVISION_FETCH: &str = "revision.fetch";
    /// Fetch one attachment by site-relative path.
    pub const FILE_FETCH: &str = "file.fetch";
    /// Evacuate the theme CSS `@import`/`url()` graph from planned roots.
    pub const THEME_CRAWL: &str = "theme.crawl";
    /// Periodic site shell + theme refresh (self-rescheduling).
    pub const SHELL_SYNC: &str = "shell.sync";
    /// Periodic out/ publication refresh (self-rescheduling).
    pub const OUT_UPDATE: &str = "out.update";
}

pub mod prio {
    /// The actual payload drains first; a page.sync can always refill the
    /// revision queue, so bookkeeping never starves the archive either —
    /// whoever's queue is empty yields to the other.
    ///
    /// Resolved from the kind at claim time (SQL `kind_prio()`), so retuning
    /// applies to already-queued rows — unlike the priority column, which is
    /// frozen at enqueue time.
    pub const DISCOVER: i64 = 30;
    pub const REVISION: i64 = 20;
    pub const PAGE: i64 = 10;
    pub const FILE: i64 = 0;
    pub const SHELL: i64 = -5;
    pub const OUT: i64 = -10;

    pub fn of(kind: &str) -> i64 {
        match kind {
            super::kind::DISCOVER => DISCOVER,
            super::kind::REVISION_FETCH => REVISION,
            super::kind::PAGE_SYNC => PAGE,
            super::kind::FILE_FETCH | super::kind::THEME_CRAWL => FILE,
            super::kind::SHELL_SYNC => SHELL,
            super::kind::OUT_UPDATE => OUT,
            _ => FILE,
        }
    }
}

/// Event-class kinds: no stable identity, no dedup, deleted on completion.
/// The partial unique index `ux_jobs_kind_payload` skips exactly these.
pub fn is_ephemeral(kind: &str) -> bool {
    kind == kind::PAGE_SYNC
}

/// Periodic self-rescheduling job: never done, never dead. Payload is a
/// constant `{}` — the singleton dedup key (cadence lives in the config).
fn periodic(kind: &str, priority: i64) -> NewJob {
    NewJob {
        kind: kind.into(),
        payload: "{}".into(),
        priority,
        run_at: 0,
        max_attempts: 0,
        resurrect: false,
    }
}

impl NewJob {
    /// Payload-only helper for one-shot jobs.
    fn one_shot(kind: &str, payload: String, priority: i64) -> NewJob {
        NewJob {
            kind: kind.into(),
            payload,
            priority,
            run_at: 0,
            max_attempts: 8,
            resurrect: false,
        }
    }
}

/// The set of periodic jobs every site starts with. Cadences come from the
/// config and are applied by the workers at completion time.
pub fn seed_periodic() -> Vec<NewJob> {
    vec![
        periodic(kind::DISCOVER, prio::DISCOVER),
        periodic(kind::SHELL_SYNC, prio::SHELL),
        periodic(kind::OUT_UPDATE, prio::OUT),
    ]
}

/// Event job: reconcile one slug with upstream as of execution time.
/// Ephemeral — every signal is its own row; no dedup, no resurrect.
pub fn page_sync(slug: &str) -> NewJob {
    NewJob::one_shot(
        kind::PAGE_SYNC,
        json!({ "slug": slug }).to_string(),
        prio::PAGE,
    )
}

pub fn revision_fetch(page_id: i64, rev_no: i64, rev_id: &str) -> NewJob {
    NewJob::one_shot(
        kind::REVISION_FETCH,
        json!({ "page_id": page_id, "rev_no": rev_no, "rev_id": rev_id }).to_string(),
        prio::REVISION,
    )
}

pub fn file_fetch(path: &str) -> NewJob {
    NewJob::one_shot(
        kind::FILE_FETCH,
        json!({ "path": path }).to_string(),
        prio::FILE,
    )
}

/// Theme-graph evacuation from planned roots. `resurrect`: a finished crawl
/// re-arms when `shell.sync` re-enqueues it (roots changed, or the previous
/// crawl recorded failures worth retrying) instead of being swallowed by
/// `INSERT OR IGNORE` against the done row.
pub fn theme_crawl(roots: &[String]) -> NewJob {
    let mut j = NewJob::one_shot(
        kind::THEME_CRAWL,
        json!({ "roots": roots }).to_string(),
        prio::FILE,
    );
    j.resurrect = true;
    j
}
