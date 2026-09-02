//! evakuilo v2 — Wikidot evacuation archiver.
//!
//! SQLite-first: the per-site database is the single source of truth; the
//! scraper is a job queue over it; `out/` is a derived, rebuildable
//! publication artifact.

// v2 scaffold: `out/` publication consumes `zstd_level`; remove this
// attribute when the out writer lands.

mod blobs;
mod config;
mod daemon;
mod db;
mod http;
mod importer;
mod jobs;
mod model;
mod out;
mod parsers;
mod theme;
mod wikidot;
mod workers;

use std::path::Path;

use anyhow::{Context, Result};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("status") => cmd_status(),
        Some("init") => cmd_init(),
        Some("run") => {
            let cfg = load_config()?;
            init_tracing();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            runtime.block_on(daemon::run(cfg))
        }
        Some("import-legacy") => cmd_import_legacy(&args[1..]),
        _ => usage(),
    }
}

fn usage() -> ! {
    eprintln!(
        "evakuilo — Wikidot evacuation archiver

USAGE:
    evakuilo init                  create site databases + seed periodic jobs
    evakuilo status                per-site database statistics
    evakuilo run                   daemon (ctrl-c to stop)
    evakuilo import-legacy [FROM]  one-shot v1 tree import (default: legacy/data)"
    );
    std::process::exit(2);
}

fn cmd_import_legacy(args: &[String]) -> Result<()> {
    let from = args
        .iter()
        .find(|a| !a.starts_with('-'))
        .cloned()
        .unwrap_or_else(|| "legacy/data".into());
    let cfg = load_config()?;
    println!("importing legacy tree from {from}");
    importer::run(&cfg, Path::new(&from))
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn load_config() -> Result<config::Config> {
    config::Config::load(Path::new("evakuilo.toml"))
        .context("evakuilo.toml not found in the working directory")
}

fn cmd_init() -> Result<()> {
    let cfg = load_config()?;
    for site in &cfg.instance.sites {
        let db = db::Db::open(&cfg.site_db(site))?;
        let seeded = db.enqueue(&jobs::seed_periodic())?;
        println!(
            "{}: {} ({} new periodic job{})",
            site,
            cfg.site_db(site).display(),
            seeded,
            if seeded == 1 { "" } else { "s" }
        );
    }
    Ok(())
}

fn cmd_status() -> Result<()> {
    let cfg = load_config()?;
    println!(
        "instance {} ({} sites)",
        cfg.instance.name,
        cfg.instance.sites.len()
    );
    println!(
        "rate limit: 1 req / {} ms (global), timeout {}s",
        cfg.rate_limit_ms, cfg.timeout_s
    );
    for site in &cfg.instance.sites {
        let db_path = cfg.site_db(site);
        match db::Db::open_read_only(&db_path) {
            Ok(db) => {
                let s = db.stats()?;
                let jobs: String = if s.jobs.is_empty() {
                    "none".into()
                } else {
                    s.jobs
                        .iter()
                        .map(|(k, n)| format!("{k}={n}"))
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                println!("\n{site}:");
                println!("  db: {}", db_path.display());
                println!("  jobs: {jobs}");
                println!(
                    "  pages={} revisions={} (+{} meta-only, {} denied)",
                    s.pages,
                    s.revisions,
                    s.revision_meta - s.revisions,
                    s.denied
                );
                println!(
                    "  files: {} saved, {} pending, {} missing",
                    s.files_saved, s.files_pending, s.files_missing
                );
                for (kind, err) in &s.dead_errors {
                    println!("  DEAD {kind}: {err}");
                }
            }
            Err(_) => println!("\n{site}: no database yet ({})", db_path.display()),
        }
    }
    Ok(())
}
