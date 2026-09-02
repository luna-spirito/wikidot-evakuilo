//! Domain types shared across the scrape pipeline — ported from v1's
//! `site/types.gleam`.

use serde::{Deserialize, Serialize};

/// Wikidot page slug with optional category prefix:
/// `draft:guardian-spirit` → category `draft`, name `guardian-spirit`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Slug {
    pub category: Option<String>,
    pub name: String,
}

impl Slug {
    pub fn parse(raw: &str) -> Slug {
        let raw = raw.trim().to_lowercase();
        match raw.split_once(':') {
            Some((cat, name)) if !cat.is_empty() && !name.is_empty() => Slug {
                category: Some(cat.to_string()),
                name: name.to_string(),
            },
            _ => Slug {
                category: None,
                name: raw,
            },
        }
    }

    pub fn as_str(&self) -> String {
        match &self.category {
            Some(cat) => format!("{cat}:{}", self.name),
            None => self.name.clone(),
        }
    }
}

/// One RecentChanges feed entry (from SiteChangesListModule).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeEntry {
    pub slug: Slug,
    pub rev_no: i64,
    pub ts: i64,
    pub author: i64,
}

/// One revision row (from PageRevisionListModule).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRevision {
    pub rev_id: String,
    pub rev_no: i64,
    pub ts: i64,
    pub author: i64,
}

// ── Fetch errors (v1 `types.FetchError` semantics) ──

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FetchError {
    #[error("timeout")]
    Timeout,
    #[error("rate limited (HTTP 429)")]
    RateLimited,
    #[error("not found (HTTP 404)")]
    NotFound,
    #[error("forbidden / no permission")]
    Forbidden,
    #[error("http error: {0}")]
    Http(String),
    #[error("parse error: {0}")]
    Parse(String),
}

impl FetchError {
    /// v1 `is_retryable`: everything but NotFound/Forbidden.
    pub fn retryable(&self) -> bool {
        !matches!(self, FetchError::NotFound | FetchError::Forbidden)
    }

    /// v1 `backoff_ms`: Timeout 10s, RateLimited 30s, Parse/Http 5s.
    pub fn backoff(&self) -> std::time::Duration {
        match self {
            FetchError::Timeout => std::time::Duration::from_secs(10),
            FetchError::RateLimited => std::time::Duration::from_secs(30),
            _ => std::time::Duration::from_secs(5),
        }
    }

    /// Whether a job failing with this error should dead-letter instead of
    /// retrying. Same as !retryable(), named for the job layer.
    pub fn permanent(&self) -> bool {
        !self.retryable()
    }
}

/// A fetch result.
pub type FetchResult<T> = Result<T, FetchError>;
