//! Configuration (`evakuilo.toml`): one instance per file, mirroring v1's
//! shape minus everything git-related.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
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
    pub sites: Vec<SiteSpec>,
}

/// One entry of `instance.sites` — every site is spelled out the same
/// explicit way, no shorthand:
///
/// ```toml
/// sites = [
///     { type = "wikidot", host = "obscurative" },
///     { type = "rufoundation", host = "www.backroomswiki.ru", api_key = "…" },
/// ]
/// ```
///
/// `host` is the site's address on its platform: the Wikidot slug
/// (`obscurative` → obscurative.wikidot.com) or the RuFoundation host
/// exactly as the site canonicalizes (keep the `www.` when it uses one).
/// `api_key`/`name` are RuFoundation-only: the bot API key (public sites
/// archive fully without one; hidden categories need it; never persisted,
/// never sent to any other host) and a directory-name override for when
/// the host-derived default would collide with another site.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SiteSpec {
    #[serde(rename = "type")]
    kind: String,
    pub host: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

/// The platform a site runs on — decides which worker set executes its
/// jobs. Everything else (queue, DB schema, backfill, out/ publication) is
/// shared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Platform {
    Wikidot,
    RuFoundation {
        host: String,
        api_key: Option<String>,
    },
}

impl Platform {
    pub fn label(&self) -> &'static str {
        match self {
            Platform::Wikidot => "wikidot",
            Platform::RuFoundation { .. } => "rufoundation",
        }
    }
}

impl SiteSpec {
    /// The site's `(directory name, platform)` pair.
    pub fn resolve(&self) -> Result<(String, Platform)> {
        match self.kind.as_str() {
            "wikidot" => {
                if self.api_key.is_some() || self.name.is_some() {
                    bail!(
                        "wikidot site '{}': 'api_key' and 'name' are rufoundation-only",
                        self.host
                    );
                }
                Ok((self.host.clone(), Platform::Wikidot))
            }
            "rufoundation" => Ok((
                self.name.clone().unwrap_or_else(|| ru_dir_name(&self.host)),
                Platform::RuFoundation {
                    host: self.host.clone(),
                    api_key: self.api_key.clone(),
                },
            )),
            other => bail!(
                "unknown site type '{other}' (host '{}'): expected \"wikidot\" or \"rufoundation\"",
                self.host
            ),
        }
    }
}

/// Default RuFoundation directory name: the configured host verbatim, minus
/// scheme and trailing slash, lowercased. `www.` is part of the host's
/// identity — stripping it would publish under a name nobody addresses the
/// site by (the kolorinko registration uses the configured spelling).
fn ru_dir_name(host: &str) -> String {
    let h = host.trim();
    let h = h
        .strip_prefix("https://")
        .or_else(|| h.strip_prefix("http://"))
        .unwrap_or(h);
    h.trim_end_matches('/').to_ascii_lowercase()
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
            bail!("no sites configured for instance {}", cfg.instance.name);
        }
        let mut seen: HashSet<String> = HashSet::new();
        for (name, platform) in cfg.resolved_sites()? {
            if let Platform::RuFoundation { host, .. } = &platform
                && host.trim().is_empty()
            {
                bail!("rufoundation site '{name}' has an empty host");
            }
            if !seen.insert(name.clone()) {
                bail!(
                    "duplicate site directory '{name}': two sites would share one database"
                );
            }
        }
        Ok(cfg)
    }

    /// Every configured site as its `(directory name, platform)` pair.
    pub fn resolved_sites(&self) -> Result<Vec<(String, Platform)>> {
        self.instance.sites.iter().map(SiteSpec::resolve).collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn load_str(raw: &str) -> Result<Config> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("evakuilo.toml");
        std::fs::write(&path, raw).unwrap();
        Config::load(&path)
    }

    /// Wikidot sites use the same explicit shape as everything else — a
    /// bare string is a parse error, not a hidden shorthand.
    #[test]
    fn wikidot_sites_resolve_to_their_slug() {
        let cfg = load_str(
            r#"
[instance]
name = "demo"
sites = [
    { type = "wikidot", host = "backrooms-wiki" },
    { type = "wikidot", host = "rpcauthority" },
]
"#,
        )
        .unwrap();
        let resolved = cfg.resolved_sites().unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0], ("backrooms-wiki".into(), Platform::Wikidot));
        assert_eq!(resolved[1], ("rpcauthority".into(), Platform::Wikidot));
    }

    #[test]
    fn bare_strings_are_rejected() {
        let err = load_str(
            r#"
[instance]
name = "demo"
sites = ["backrooms-wiki"]
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("parsing"), "{err}");
    }

    /// A RuFoundation site resolves its platform, key and default directory
    /// name (the configured host verbatim, `www.` kept — the publication
    /// dir must match what kolorinko registers the site under).
    #[test]
    fn rufoundation_site_resolves_host_key_and_default_dir() {
        let cfg = load_str(
            r#"
[instance]
name = "demo"
sites = [
    { type = "wikidot", host = "backrooms-wiki" },
    { type = "rufoundation", host = "www.backroomswiki.ru", api_key = "sekret" },
]
"#,
        )
        .unwrap();
        let resolved = cfg.resolved_sites().unwrap();
        assert_eq!(resolved.len(), 2);
        let (name, platform) = &resolved[1];
        assert_eq!(name, "www.backroomswiki.ru");
        let Platform::RuFoundation { host, api_key } = platform else {
            panic!("expected rufoundation platform");
        };
        assert_eq!(host, "www.backroomswiki.ru");
        assert_eq!(api_key.as_deref(), Some("sekret"));
    }

    #[test]
    fn rufoundation_name_override() {
        let cfg = load_str(
            r#"
[instance]
name = "demo"
sites = [{ type = "rufoundation", host = "scpfoundation.net", name = "scp-ru" }]
"#,
        )
        .unwrap();
        assert_eq!(cfg.resolved_sites().unwrap()[0].0, "scp-ru");
    }

    /// An explicit `name` colliding with another site's directory is
    /// rejected, not silently shared.
    #[test]
    fn duplicate_directories_are_rejected() {
        let err = load_str(
            r#"
[instance]
name = "demo"
sites = [
    { type = "wikidot", host = "backrooms-wiki" },
    { type = "rufoundation", host = "www.backroomswiki.ru", name = "backrooms-wiki" },
]
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate site directory"), "{err}");
    }

    /// Typos fail loudly: an unknown `type` from `resolve`, an unknown
    /// field straight from the deserializer.
    #[test]
    fn unknown_type_and_unknown_fields_are_rejected() {
        let err = load_str(
            r#"
[instance]
name = "demo"
sites = [{ type = "wikdot", host = "x" }]
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("unknown site type 'wikdot'"),
            "{err}"
        );

        let err = load_str(
            r#"
[instance]
name = "demo"
sites = [{ type = "rufoundation", host = "x.test", typo = 1 }]
"#,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("unknown field"), "{err:#}");
    }

    #[test]
    fn wikidot_takes_no_rufoundation_options() {
        let err = load_str(
            r#"
[instance]
name = "demo"
sites = [{ type = "wikidot", host = "x", api_key = "k" }]
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("rufoundation-only"),
            "{err}"
        );
    }
}
