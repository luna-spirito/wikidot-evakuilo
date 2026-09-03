//! Theme `@import` graph walker (v1 `site/theme.gleam`).
//!
//! Wikidot injects a `<style id="internal-style">` block whose last
//! `@import` is the site theme root (see `parsers::plan_theme_roots`). That
//! CSS `@import`s further CSS and references binary assets via `url()`
//! (woff/woff2 fonts, svg/png images) — possibly across several CDNs
//! (wdfiles, jsdelivr, fonts.googleapis). This module BFS-walks the graph:
//!
//! - cycle-safe: a visited set keyed on `parsers::normalize_url` guarantees
//!   each CSS is fetched at most once even when the import graph loops
//!   (sigma → sub → sigma);
//! - `url()` assets are collected resolved absolute against their containing
//!   document; `@import url(x)` targets are filtered out of the asset set so
//!   imported CSS isn't double-counted as binary;
//! - a binary masquerading as `@import` (an author wrote
//!   `@import url(font.woff2)`) fails UTF-8 decoding: the caller's fetch
//!   closure stores it as an asset and reports no body, so the walk stores
//!   the file but doesn't descend into it.
//!
//! The fetch closure is injected, so the walk is unit-testable with no
//! network; the worker (`workers::theme_crawl`) supplies a DB-first
//! resolver: saved rows are read back from the content-addressed blobs on
//! disk and only genuinely-new URLs hit the network.

use std::collections::{HashSet, VecDeque};
use std::future::Future;

use crate::parsers;

/// Outcome of walking the `@import` graph.
#[derive(Debug, Default)]
pub struct CrawlOutcome {
    /// Every decoded CSS body, paired with its absolute URL.
    pub css: Vec<(String, String)>,
    /// Every `url()` asset discovered, resolved absolute, deduped.
    pub assets: Vec<String>,
    /// URLs whose fetch failed (already handled by the caller's closure).
    pub failed: Vec<String>,
}

/// What the injected fetch closure got for one URL.
#[derive(Debug)]
pub enum Fetched {
    /// A decoded CSS body to descend into.
    Css(String),
    /// Archived bytes that are not CSS (a font behind `@import url(…)`):
    /// saved by the closure, nothing to descend into — NOT a failure.
    Binary,
}

/// BFS the `@import` graph from `roots`. `fetch` returns `Some(Fetched)`
/// for anything saved (CSS to descend into, or a binary masquerader) and
/// `None` for a failure.
pub async fn crawl<F, Fut>(roots: &[String], mut fetch: F) -> CrawlOutcome
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Option<Fetched>>,
{
    let mut queue: VecDeque<String> = roots.iter().cloned().collect();
    let mut visited: HashSet<String> = HashSet::new();
    let mut out = CrawlOutcome::default();

    while let Some(url) = queue.pop_front() {
        let key = parsers::normalize_url(&url);
        if !visited.insert(key) {
            continue;
        }
        let body = match fetch(url.clone()).await {
            Some(Fetched::Css(body)) => body,
            Some(Fetched::Binary) => continue,
            None => {
                out.failed.push(url);
                continue;
            }
        };
        // Resolve every child reference against THIS document's URL.
        let imports: Vec<String> = parsers::extract_css_imports(&body)
            .into_iter()
            .map(|r| parsers::resolve_url(&url, &r))
            .collect();
        // url() refs that are NOT @import targets (an @import is itself
        // written `@import url(...)`, so the raw url() set would otherwise
        // double-count imported CSS files as binary assets).
        for asset in parsers::extract_css_urls(&body) {
            let a = parsers::resolve_url(&url, &asset);
            if !imports.contains(&a) && !out.assets.contains(&a) {
                out.assets.push(a);
            }
        }
        queue.extend(imports);
        out.css.push((url, body));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn map_fetch(
        pages: HashMap<&'static str, &'static str>,
    ) -> impl FnMut(String) -> std::future::Ready<Option<Fetched>> {
        move |url: String| {
            let body = pages.get(url.as_str()).copied();
            std::future::ready(body.map(|b| Fetched::Css(String::from(b))))
        }
    }

    #[tokio::test]
    async fn walks_imports_and_assets_with_resolution() {
        let pages = HashMap::from([
            (
                "https://cdn.example.org/sigma.min.css",
                "@import url(font-bauhaus.css); @import 'icons.css';\n\
                 body { background: url('img/bg.png'); font-family: x }\n\
                 li:before { content: url(data:image/png;base64,AAA) }",
            ),
            (
                "https://cdn.example.org/font-bauhaus.css",
                "@font-face { src: url('/fonts/bauhaus.woff2') format('woff2'); }",
            ),
            (
                "https://cdn.example.org/icons.css",
                ".ico{background:url(#none)}",
            ),
        ]);
        let out = crawl(
            &["https://cdn.example.org/sigma.min.css".into()],
            map_fetch(pages),
        )
        .await;
        assert_eq!(out.css.len(), 3);
        assert!(out.failed.is_empty());
        // Relative refs resolved against their containing document.
        assert!(
            out.assets
                .contains(&"https://cdn.example.org/img/bg.png".to_string())
        );
        assert!(
            out.assets
                .contains(&"https://cdn.example.org/fonts/bauhaus.woff2".to_string())
        );
        // data: and # refs filtered; @import targets not counted as assets.
        assert_eq!(out.assets.len(), 2);
    }

    #[tokio::test]
    async fn cycle_safe_and_failed_fetches_recorded() {
        // a imports b, b imports a (loop); c is dead.
        let pages = HashMap::from([
            (
                "https://x.test/a.css",
                "@import url(b.css); @import url(c.css);",
            ),
            ("https://x.test/b.css", "@import url(a.css);"),
        ]);
        let out = crawl(&["https://x.test/a.css".into()], map_fetch(pages)).await;
        assert_eq!(out.css.len(), 2, "a and b each fetched once");
        assert_eq!(out.failed, vec!["https://x.test/c.css".to_string()]);
    }

    #[tokio::test]
    async fn same_url_different_fragment_is_one_visit() {
        let out = crawl(
            &[
                "https://x.test/a.css".into(),
                "https://x.test/a.css#theme".into(),
            ],
            |url: String| async move {
                assert_eq!(url, "https://x.test/a.css");
                Some(Fetched::Css("/* css */".to_string()))
            },
        )
        .await;
        assert_eq!(out.css.len(), 1);
    }

    #[tokio::test]
    async fn binary_masquerader_is_saved_not_failed() {
        // A font behind `@import url(...)`: archived by the closure, not
        // descendable, and crucially NOT a failure — a failed marker would
        // re-arm the crawl forever for something already evacuated.
        let out = crawl(&["https://x.test/a.css".into()], |url: String| async move {
            match url.as_str() {
                "https://x.test/a.css" => Some(Fetched::Css("@import url(font.woff2);".into())),
                _ => Some(Fetched::Binary),
            }
        })
        .await;
        assert_eq!(out.css.len(), 1);
        assert!(out.failed.is_empty());
    }
}
