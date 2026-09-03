//! Daemon: per-site worker loops over the shared global rate limiter.
//!
//! Each site gets a dispatcher that claims one job at a time and runs up to
//! `SITE_CONCURRENCY` jobs concurrently (the limiter serializes HTTP
//! anyway; this only overlaps parsing and DB work). When the queue is
//! drained, the dispatcher sleeps until the next `run_at` (bounded).

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tokio::sync::Semaphore;

use crate::{config::Config, db::Db, http::Wikidot, jobs, workers};

const SITE_CONCURRENCY: usize = 4;

pub async fn run(cfg: Config) -> Result<()> {
    let wik = Arc::new(Wikidot::new(cfg.rate_limit_ms, cfg.timeout_s)?);

    let mut locks = Vec::new();
    let mut handles = Vec::new();
    for site in cfg.instance.sites.clone() {
        // One unreachable site must not take the others down with it.
        let (db, lock) = match Db::open_locked(&cfg.site_db(&site)) {
            Ok(opened) => opened,
            Err(e) => {
                tracing::error!(site, error = %e, "opening site DB failed; site skipped");
                continue;
            }
        };
        // Idempotent seeding (also re-seeds anything deleted by hand).
        let seeded = match db.enqueue(&jobs::seed_periodic()) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(site, error = %e, "seeding periodic jobs failed; site skipped");
                continue;
            }
        };
        if seeded > 0 {
            tracing::info!(site, seeded, "seeded periodic jobs");
        }
        locks.push(lock);
        let handle = tokio::spawn(site_worker(cfg.clone(), Arc::clone(&wik), db, site));
        handles.push(handle);
    }
    if handles.is_empty() {
        anyhow::bail!("no site could be started");
    }

    tracing::info!(
        sites = handles.len(),
        rate_limit_ms = cfg.rate_limit_ms,
        "daemon running (ctrl-c to stop)"
    );
    tokio::signal::ctrl_c().await?;
    tracing::info!("ctrl-c received; exiting (in-flight jobs recover on next start)");
    Ok(())
}

async fn site_worker(cfg: Config, wik: Arc<Wikidot>, db: Db, site: String) {
    let semaphore = Arc::new(Semaphore::new(SITE_CONCURRENCY));
    loop {
        let claimed = match db.claim(1) {
            Ok(jobs) if jobs.is_empty() => {
                let next = db.next_wake().ok().flatten();
                let wait = match next {
                    Some(at) => (at - crate::db::now()).clamp(1, 60) as u64,
                    None => 30,
                };
                tokio::time::sleep(Duration::from_secs(wait)).await;
                continue;
            }
            Ok(mut jobs) => jobs.pop(),
            Err(e) => {
                tracing::error!(site, error = %e, "claim failed");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        let Some(job) = claimed else { continue };
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .expect("semaphore never closed");
        let cfg = cfg.clone();
        let wik = Arc::clone(&wik);
        let db = db.clone();
        let site = site.clone();
        tokio::spawn(async move {
            let job_id = job.id;
            let kind = job.kind.clone();
            let outcome = workers::run_job(&db, &cfg, &wik, &site, &job).await;
            let result = match outcome {
                workers::Outcome::Complete { resched, effects } => {
                    db.complete(job_id, resched, |tx| effects(tx))
                }
                workers::Outcome::CompleteEphemeral { effects } => {
                    db.complete_ephemeral(job_id, |tx| effects(tx))
                }
                workers::Outcome::Fail { error, permanent } => {
                    let out = db.fail(job_id, &error, permanent);
                    match &out {
                        Ok(crate::db::FailOutcome::Dead) => {
                            tracing::error!(site, kind, job_id, error, "job dead-lettered")
                        }
                        Ok(crate::db::FailOutcome::Retry { run_at }) => {
                            tracing::warn!(
                                site,
                                kind,
                                job_id,
                                error,
                                run_at,
                                "job failed; will retry"
                            )
                        }
                        // SQLite itself is misbehaving; the row stays
                        // `running` and startup recovery re-queues it.
                        Err(e) => {
                            tracing::error!(site, job_id, error = %e, "recording failure failed")
                        }
                    }
                    return;
                }
            };
            if let Err(e) = result {
                // The effects transaction rolled back atomically; re-queue
                // with backoff instead of leaving the row `running` until
                // the next daemon restart (the fetched data is simply
                // refetched — every effect is idempotent).
                tracing::warn!(site, job_id, error = %e, "job completion failed; re-queueing");
                match db.fail(job_id, &format!("completion failed: {e}"), false) {
                    Ok(crate::db::FailOutcome::Dead) => {
                        tracing::error!(
                            site,
                            kind,
                            job_id,
                            "job dead-lettered after completion failures"
                        )
                    }
                    Ok(crate::db::FailOutcome::Retry { run_at }) => {
                        tracing::warn!(site, kind, job_id, run_at, "re-queued")
                    }
                    Err(e2) => {
                        tracing::error!(
                            site,
                            job_id,
                            error = %e2,
                            "re-queueing failed; startup recovery will retry"
                        )
                    }
                }
            }
            drop(permit);
        });
    }
}
