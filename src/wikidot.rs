//! Wikidot API — AJAX modules and page fetching, port of v1
//! `web/wikidot_api.gleam`.
//!
//! Every public call is wrapped in `retryable`: fresh rate-limit ticket per
//! attempt, v1 backoffs on transient errors, five attempts. Sustained
//! outages outliving that budget surface as `Err` to the caller — the job
//! queue then retries with its own (longer) backoff, which replaces v1's
//! outer infinite `fetch_retrying` loop.

use serde::Deserialize;

use crate::{
    http::Wikidot,
    model::{ChangeEntry, FetchError, FetchResult, ParsedRevision, Slug},
    parsers,
};

const AJAX_RETRIES: u32 = 5;

/// Per-site API handle.
pub struct SiteApi<'a> {
    pub wik: &'a Wikidot,
    pub site: String,
    /// The executing job's claim priority (`jobs::prio`). Rides on every
    /// rate-limiter ticket request, so the shared limiter serves this job's
    /// HTTP ahead of lower-priority backlog that queued earlier from other
    /// workers and sites.
    pub prio: i64,
}

impl<'a> SiteApi<'a> {
    pub fn new(wik: &'a Wikidot, site: &str, prio: i64) -> SiteApi<'a> {
        SiteApi {
            wik,
            site: site.to_string(),
            prio,
        }
    }

    fn base(&self) -> String {
        format!("http://{}.wikidot.com", self.site)
    }

    // ── Generic retry wrapper (v1 `retryable`) ──

    async fn retryable<T, F, Fut>(&self, label: &str, mut op: F) -> FetchResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = FetchResult<T>>,
    {
        let mut attempts_left = AJAX_RETRIES;
        loop {
            let result = op().await;
            match result {
                Ok(v) => return Ok(v),
                Err(e) if e.retryable() && attempts_left > 0 => {
                    tracing::warn!(
                        site = %self.site, %label, error = %e,
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

    // ── Session bootstrap ──

    /// Lazily fetch a CSRF token (v1 `ensure_token`): the first AJAX call
    /// of a cold session pulls one via a plain site GET. The per-site gate
    /// keeps concurrent cold workers from racing — one GET refreshes, the
    /// rest re-check under the lock and find the cookie already set.
    async fn ensure_token(&self) -> FetchResult<()> {
        if self.wik.token7(&self.site).is_some() {
            return Ok(());
        }
        let gate = self.wik.token_gate(&self.site);
        let _guard = gate.lock().await;
        if self.wik.token7(&self.site).is_some() {
            return Ok(());
        }
        self.retryable("token refresh", || async {
            let url = format!("{}/", self.base());
            let _resp = self.wik.get(&self.site, &url, self.prio).await?;
            // Absorbing cookies happened in the client; confirm the token.
            if self.wik.token7(&self.site).is_some() {
                Ok(())
            } else {
                Err(FetchError::Http(format!("no wikidot_token7 from {url}")))
            }
        })
        .await?;
        tracing::info!(site = %self.site, "CSRF token acquired");
        Ok(())
    }

    // ── AJAX plumbing ──

    async fn ajax(&self, params: &[(&str, String)]) -> FetchResult<String> {
        self.ensure_token().await?;
        let token = self
            .wik
            .token7(&self.site)
            .ok_or_else(|| FetchError::Http("token vanished mid-call".into()))?;
        let mut full: Vec<(&str, String)> = params.to_vec();
        full.push(("callbackIndex", "0".into()));
        full.push(("wikidot_token7", token));
        let url = format!("{}/ajax-module-connector.php", self.base());
        let resp = self.wik.post_form(&self.site, &url, &full, self.prio).await?;
        let body = resp
            .text()
            .await
            .map_err(|e| FetchError::Http(e.to_string()))?;
        match parse_ajax_body(&body) {
            Ok(body) => Ok(body),
            Err(AjaxRejection::NoPermission) => Err(FetchError::Forbidden),
            Err(AjaxRejection::WrongToken) => {
                // The CSRF token went stale server-side (possible now that
                // sessions persist across restarts). Drop it — any session
                // cookie alongside it stays — and surface a retryable
                // error: the next attempt re-bootstraps via `ensure_token`.
                self.wik.forget_token(&self.site);
                Err(FetchError::Http(
                    "wikidot_token7 rejected as stale (wrong_token7)".into(),
                ))
            }
            Err(AjaxRejection::Other(status)) => Err(FetchError::Parse(format!(
                "AJAX status '{status}' (expected 'ok')"
            ))),
        }
    }

    // ── Endpoints ──

    pub async fn fetch_site_changes(
        &self,
        page: i64,
        perpage: i64,
    ) -> FetchResult<Vec<ChangeEntry>> {
        let body = self
            .retryable(&format!("SiteChanges p={page}"), || async {
                self.ajax(&[
                    ("moduleName", "changes/SiteChangesListModule".into()),
                    ("options", r#"{"all":true}"#.into()),
                    ("page", page.to_string()),
                    ("perpage", perpage.to_string()),
                ])
                .await
            })
            .await?;
        Ok(parsers::extract_changes(&body))
    }

    /// GET the page HTML; returns (title, tags, page_id, html).
    pub async fn fetch_page(
        &self,
        slug: &Slug,
    ) -> FetchResult<(String, Vec<String>, String, String)> {
        let slug_str = slug.as_str();
        let url = if slug_str.is_empty() {
            format!("{}/", self.base())
        } else {
            format!("{}/{slug_str}/noredirect/true", self.base())
        };
        let html = self
            .retryable(&format!("GET {slug_str}"), || async {
                let resp = self.wik.get(&self.site, &url, self.prio).await?;
                resp.text()
                    .await
                    .map(|body| body.replace('\u{feff}', ""))
                    .map_err(|e| FetchError::Http(e.to_string()))
            })
            .await?;
        let page_id = parsers::extract_page_id(&html)
            .ok_or_else(|| FetchError::Parse(format!("no page_id in {slug_str}")))?;
        let title = parsers::extract_page_title(&html);
        let tags = parsers::extract_page_tags(&html);
        Ok((title, tags, page_id, html))
    }

    pub async fn fetch_revisions_above(
        &self,
        page_id: &str,
        min_rev: i64,
    ) -> FetchResult<Vec<ParsedRevision>> {
        let mut acc = Vec::new();
        let mut page = 1i64;
        loop {
            let body = self
                .retryable(&format!("PageRevisions p={page}"), || async {
                    self.ajax(&[
                        ("moduleName", "history/PageRevisionListModule".into()),
                        ("options", r#"{"all":true}"#.into()),
                        ("page_id", page_id.to_string()),
                        ("page", page.to_string()),
                        ("perpage", "200".into()),
                    ])
                    .await
                })
                .await?;
            let revs = parsers::extract_revisions(&body);
            let new_revs: Vec<ParsedRevision> = revs
                .into_iter()
                .take_while(|r| r.rev_no > min_rev)
                .collect();
            let full_page = new_revs.len() >= 200;
            acc.extend(new_revs);
            if !full_page {
                return Ok(acc);
            }
            page += 1;
        }
    }

    pub async fn fetch_revision_source(&self, rev_id: &str) -> FetchResult<String> {
        let body = self
            .retryable(&format!("PageSource rev={rev_id}"), || async {
                self.ajax(&[
                    ("moduleName", "history/PageSourceModule".into()),
                    ("revision_id", rev_id.to_string()),
                ])
                .await
            })
            .await?;
        parsers::extract_page_source(&body)
            .ok_or_else(|| FetchError::Parse(format!("no page source for revision {rev_id}")))
    }

    pub async fn fetch_page_files(&self, page_id: &str) -> FetchResult<Vec<String>> {
        let body = self
            .retryable(&format!("PageFiles id={page_id}"), || async {
                self.ajax(&[
                    ("moduleName", "files/PageFilesModule".into()),
                    ("page_id", page_id.to_string()),
                ])
                .await
            })
            .await?;
        Ok(parsers::extract_page_files(&self.site, &body))
    }

    /// Fetch an attachment: (bytes, media type). `path_or_url` is the
    /// canonical absolute row URL (`http://{site}.wikidot.com/…`, which
    /// 302s to wdfiles server-side); a site-relative path still works
    /// (pre-v6 stragglers) and lifts onto the main domain. Session cookies
    /// ride along — this is the site's own namespace. The media type is
    /// the server's Content-Type sans parameters, or None when it sent
    /// nothing usable — callers fall back to sniffing.
    pub async fn fetch_attachment(&self, path_or_url: &str) -> FetchResult<(Vec<u8>, Option<String>)> {
        let url = if path_or_url.starts_with("http://") || path_or_url.starts_with("https://") {
            path_or_url.to_string()
        } else {
            format!("{}/{path_or_url}", self.base())
        };
        self.retryable(&format!("attachment {path_or_url}"), || async {
            let resp = self.wik.get(&self.site, &url, self.prio).await?;
            let content_type = media_type(resp.headers());
            let bytes = resp
                .bytes()
                .await
                .map(|b| b.to_vec())
                .map_err(|e| FetchError::Http(e.to_string()))?;
            Ok((bytes, content_type))
        })
        .await
    }

    /// Fetch a public absolute URL (theme CSS / fonts on third-party CDNs):
    /// no site cookies either direction, shared global limiter, retried on
    /// transient failures like every other fetch (Wikidot's 302→500 lesson).
    /// Returns (bytes, media type) like `fetch_attachment`.
    pub async fn fetch_public(&self, url: &str) -> FetchResult<(Vec<u8>, Option<String>)> {
        self.retryable(&format!("public asset {url}"), || async {
            let resp = self.wik.get_public(url, self.prio).await?;
            let content_type = media_type(resp.headers());
            let bytes = resp
                .bytes()
                .await
                .map(|b| b.to_vec())
                .map_err(|e| FetchError::Http(e.to_string()))?;
            Ok((bytes, content_type))
        })
        .await
    }
}

/// `Content-Type` header value with its parameters stripped
/// (`text/css; charset=utf-8` → `text/css`), or None if absent/invalid.
fn media_type(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let raw = headers.get(reqwest::header::CONTENT_TYPE)?.to_str().ok()?;
    let mime = raw.split(';').next()?.trim();
    (!mime.is_empty()).then(|| mime.to_ascii_lowercase())
}

/// v1 `parse_ajax_body`: `{status, body?}`. The rejections callers act on
/// are distinct: `no_permission` is genuinely private content (never a
/// token problem), `wrong_token7` (observed verbatim from the live
/// connector on a bogus token) means the CSRF token went stale, and
/// anything else is a parse failure.
#[derive(Debug, PartialEq, Eq)]
enum AjaxRejection {
    NoPermission,
    WrongToken,
    Other(String),
}

fn parse_ajax_body(json: &str) -> Result<String, AjaxRejection> {
    #[derive(Deserialize)]
    struct Ajax {
        status: String,
        body: Option<String>,
    }
    let parsed: Ajax = serde_json::from_str(json)
        .map_err(|_| AjaxRejection::Other("failed to parse AJAX JSON".into()))?;
    match parsed.status.as_str() {
        "ok" => Ok(parsed.body.unwrap_or_default()),
        "no_permission" => Err(AjaxRejection::NoPermission),
        "wrong_token7" => Err(AjaxRejection::WrongToken),
        other => Err(AjaxRejection::Other(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ajax_body_distinguishes_rejections() {
        assert_eq!(parse_ajax_body(r#"{"status":"ok","body":"<b>x</b>"}"#).unwrap(), "<b>x</b>");
        assert_eq!(parse_ajax_body(r#"{"status":"ok"}"#).unwrap(), "");
        assert_eq!(
            parse_ajax_body(r#"{"status":"no_permission"}"#),
            Err(AjaxRejection::NoPermission)
        );
        assert_eq!(
            parse_ajax_body(r#"{"status":"wrong_token7","message":"no","CURRENT_TIMESTAMP":1}"#),
            Err(AjaxRejection::WrongToken)
        );
        assert!(matches!(
            parse_ajax_body(r#"{"status":"internal_error"}"#),
            Err(AjaxRejection::Other(_))
        ));
        assert!(matches!(
            parse_ajax_body("<html>gateway error</html>"),
            Err(AjaxRejection::Other(_))
        ));
    }

    fn ct(value: &str) -> Option<String> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_str(value).unwrap(),
        );
        media_type(&headers)
    }

    #[test]
    fn media_type_strips_parameters_and_lowercases() {
        assert_eq!(ct("image/png").as_deref(), Some("image/png"));
        assert_eq!(ct("text/css; charset=utf-8").as_deref(), Some("text/css"));
        assert_eq!(ct("Text/HTML;Charset=UTF-8").as_deref(), Some("text/html"));
        assert_eq!(ct(""), None);
        assert_eq!(ct("; charset=utf-8"), None);
        // Non-ASCII header values are dropped, not propagated.
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );
        assert_eq!(media_type(&headers), None);
        // Absent header → None.
        assert_eq!(media_type(&reqwest::header::HeaderMap::new()), None);
    }
}
