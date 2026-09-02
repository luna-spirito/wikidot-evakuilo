//! Configuration (`evakuilo.toml`): one instance per file, mirroring v1's
//! shape minus everything git-related.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Root data directory; the instance lives at `{data_dir}/{instance}`.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// HTTP timeout per request.
    #[serde(default = "default_timeout")]
    pub timeout_s: u64,

    /// GLOBAL rate limit across all sites (one request per N ms), matching
    /// v1's single shared limiter.
    #[serde(default = "default_rate_limit")]
    pub rate_limit_ms: u64,

    /// RecentChanges discovery scan interval.
    #[serde(default = "default_monitor")]
    pub monitor_interval_s: i64,

    /// Site shell + theme refresh interval.
    #[serde(default = "default_shell")]
    pub shell_interval_s: i64,

    /// out/ publication refresh interval.
    #[serde(default = "default_out")]
    pub out_interval_s: i64,

    /// Reconciliation sweep interval: re-arm dead fetch jobs for content
    /// the DB knows is missing (contentless revisions, pending files).
    #[serde(default = "default_backfill")]
    pub backfill_interval_s: i64,

    /// zstd compression level for out/ page archives.
    #[serde(default = "default_zstd")]
    pub zstd_level: i32,

    pub instance: Instance,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Instance {
    pub name: String,
    pub sites: Vec<String>,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("data")
}
fn default_timeout() -> u64 {
    30
}
fn default_rate_limit() -> u64 {
    2000
}
fn default_monitor() -> i64 {
    1800
}
fn default_shell() -> i64 {
    86400
}
fn default_out() -> i64 {
    300
}
fn default_backfill() -> i64 {
    3600
}
fn default_zstd() -> i32 {
    19
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
        if cfg.instance.sites.is_empty() {
            anyhow::bail!("no sites configured for instance {}", cfg.instance.name);
        }
        Ok(cfg)
    }

    /// `{data_dir}/{instance.name}` — the repo directory, tier-first:
    /// `{repo}/meta/{site}/` is private state (source-of-truth DBs, daemon
    /// locks), `{repo}/out/{site}/` is the public publication — the whole
    /// tree is serveable as-is.
    pub fn repo_dir(&self) -> PathBuf {
        self.data_dir.join(&self.instance.name)
    }

    /// `{repo}/meta/{site}/site.db` — the site's source of truth.
    pub fn site_db(&self, site: &str) -> PathBuf {
        self.repo_dir().join("meta").join(site).join("site.db")
    }

    /// `{repo}/out/{site}` — the site's published artifact.
    pub fn site_out(&self, site: &str) -> PathBuf {
        self.repo_dir().join("out").join(site)
    }
}
