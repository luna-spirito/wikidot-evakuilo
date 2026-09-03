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
//! Periodic singletons (`discover`, `shell.sync`, `out.update`, `backfill`)
//! never go `done`: dedup on their constant `{}` payload is just the singleton
//! guard. Their cadence is read from the live config at completion time,
//! never from the payload — the payload is the dedup key, and embedding a
//! tunable there would fork a second eternal copy of the job on every
//! config change.
//!
//! ## Priorities
//!
//! Priorities are per-row (set at enqueue time, see `prio`) and decide
//! claim order when the queue backs up. The order is evacuation triage:
//! the shell first — one cheap homepage GET that also seeds the theme
//! crawl, without which the exported site is barely usable — then the
//! freshness ladder (a fresh recent-change never waits behind the history
//! backlog: discovery feed, newest revision, slug reconciliation), then
//! files/theme, which the live edge references here and now, before
//! intermediate history; derived publication comes last.

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
    /// Periodic reconciliation: resurrect dead fetch jobs for content the
    /// DB knows is missing (self-rescheduling).
    pub const BACKFILL: &str = "backfill";
}

pub mod prio {
    /// Enqueue-time claim priorities (higher runs first) — an evacuation
    /// triage: what makes the export *usable* and what the live edge needs
    /// outranks bulk history, and derived publication comes last:
    ///
    /// * 35 shell — one homepage GET per shell_interval_s that seeds the
    ///   theme crawl; the top slot costs ~one rate-limit ticket a day and
    ///   ends the cold-start starvation of identity + theme
    /// * 30 discovery feed · 20 the newest revision of a page (v1 "high")
    /// * 15 slug reconciliation (v1: page fetches rode the high class)
    /// * 10 files/theme — attachments and CSS referenced by the live edge
    /// *  8 intermediate revision history (v1 "low" slot)
    /// *  5 backfill sweep · -10 publication
    ///
    /// Frozen on the row at enqueue time: the head/intermediate split is
    /// inherently per-job, so a claim-time kind lookup cannot express it.
    /// Retuning the table therefore applies to future enqueues only;
    /// periodic jobs re-read nothing — their singleton row keeps the
    /// priority it was seeded with (v3 retunes seated rows, see
    /// `db::MIGRATIONS`).
    pub const SHELL: i64 = 35;
    pub const DISCOVER: i64 = 30;
    pub const REVISION_HEAD: i64 = 20;
    pub const PAGE: i64 = 15;
    pub const FILE: i64 = 10;
    pub const REVISION_OLD: i64 = 8;
    pub const BACKFILL: i64 = 5;
    pub const OUT: i64 = -10;
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
        periodic(kind::BACKFILL, prio::BACKFILL),
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

/// One revision's wikitext. `head` marks the page's newest revision — v1's
/// high slot: heads jump ahead of the intermediate-history backlog (and of
/// page reconciliation), so a fresh edit lands without waiting for the
/// catch-up grind. Intermediate revisions stay below `page.sync`, exactly
/// like v1's low limiter class.
pub fn revision_fetch(page_id: i64, rev_no: i64, rev_id: &str, head: bool) -> NewJob {
    NewJob::one_shot(
        kind::REVISION_FETCH,
        json!({ "page_id": page_id, "rev_no": rev_no, "rev_id": rev_id }).to_string(),
        if head {
            prio::REVISION_HEAD
        } else {
            prio::REVISION_OLD
        },
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The full claim order as one inequality chain: shell (usability of
    /// the export) → discovery → freshness ladder (newest revision >
    /// slug reconciliation > intermediate history) → files/theme →
    /// backfill → publication.
    #[test]
    fn priority_order_is_evacuation_triage() {
        const { assert!(prio::SHELL > prio::DISCOVER) };
        const { assert!(prio::DISCOVER > prio::REVISION_HEAD) };

        let head = revision_fetch(1, 9, "9", true);
        let intermediate = revision_fetch(1, 8, "8", false);
        let sync = page_sync("some:slug");
        assert_eq!(head.priority, prio::REVISION_HEAD);
        assert_eq!(intermediate.priority, prio::REVISION_OLD);
        assert!(head.priority > sync.priority);
        assert!(sync.priority > prio::FILE);

        // Files/theme ride one tier and outrank intermediate history.
        assert_eq!(file_fetch("a.png").priority, prio::FILE);
        assert_eq!(
            theme_crawl(&["http://x.test/a.css".into()]).priority,
            prio::FILE
        );
        assert!(prio::FILE > intermediate.priority);

        assert!(intermediate.priority > prio::BACKFILL);
        const { assert!(prio::BACKFILL > prio::OUT) };
    }
}
