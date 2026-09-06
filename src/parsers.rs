//! HTML/wikitext parsers — port of v1's `parsers.gleam` onto html5ever.
//!
//! html5ever performs spec-compliant tree recovery, so v1's workaround for
//! Wikidot's unclosed `</td>` before the comment `<td>` (a depth-counting
//! artefact of presentable_soup) is unnecessary here: the tree comes out
//! correct and rows parse naturally.
//!
//! Text entities are decoded by the parser (v1 had to unescape manually);
//! the one textual transform kept is stripping literal `<br />` markers from
//! page sources, matching v1 byte-for-byte.

use std::collections::HashSet;

use scraper::{ElementRef, Html, Selector};

use crate::model::{ChangeEntry, ParsedRevision, Slug};

// ── Page identity (from page HTML) ──

/// `WIKIREQUEST.info.pageId = 12345` → `Some("12345")`.
pub fn extract_page_id(html: &str) -> Option<String> {
    let rest = html.split_once("WIKIREQUEST.info.pageId = ")?.1;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        Some(digits)
    }
}

pub fn extract_page_title(html: &str) -> String {
    select_first_text(html, "#page-title")
}

pub fn extract_page_tags(html: &str) -> Vec<String> {
    select_texts(html, ".page-tags a")
}

/// Site title/subtitle (`#header h1` / `#header h2`) — for shell.sync.
pub fn extract_site_title(html: &str) -> String {
    select_first_text(html, "#header h1")
}

pub fn extract_site_subtitle(html: &str) -> String {
    select_first_text(html, "#header h2")
}

/// The landing page slug from the homepage's HTML:
/// `WIKIREQUEST.info.pageUnixName = "blog:_start"` → `Some("blog:_start")`.
/// The homepage GET serves the landing page, so its unix name *is* the
/// landing slug. Wikidot's emitter quotes with `"` (observed) but `'` is
/// accepted too — the same script block uses both for other keys.
pub fn extract_landing_slug(html: &str) -> Option<String> {
    let rest = html.split_once("WIKIREQUEST.info.pageUnixName")?.1;
    let rest = rest.trim_start().strip_prefix('=')?.trim_start();
    let quote = rest.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let inner = &rest[quote.len_utf8()..];
    let end = inner.find(quote)?;
    let slug = &inner[..end];
    (!slug.is_empty()).then(|| slug.to_string())
}

// ── Theme roots (v1 parsers.extract_theme_imports + theme.plan_roots) ──

/// The `@import` URLs from the page's `<style id="internal-style">` block.
/// String-scanned like v1: the block body is CSS, not HTML, so tree parsing
/// buys nothing.
pub fn extract_theme_imports(html: &str) -> Vec<String> {
    let Some((_, rest)) = html.split_once("id=\"internal-style\"") else {
        return Vec::new();
    };
    let body = match rest.split_once("</style>") {
        Some((b, _)) => b,
        None => rest,
    };
    extract_css_imports(body)
}

/// All `@import` targets in a CSS string: `url(...)`, quoted `url('...')`,
/// or bare `"..."` / `'...'`; trailing media queries are ignored.
pub(crate) fn extract_css_imports(css: &str) -> Vec<String> {
    let mut out = Vec::new();
    for piece in css.split("@import").skip(1) {
        let stmt = piece.split(';').next().unwrap_or("");
        if let Some(target) = parse_import_target(stmt) {
            out.push(target);
        }
    }
    out
}

fn parse_import_target(stmt: &str) -> Option<String> {
    let stmt = stmt.trim();
    if let Some(inner) = stmt.strip_prefix("url(") {
        let inner = inner.split(')').next().unwrap_or("");
        let inner = unquote_ref(inner.trim());
        return (!inner.is_empty()).then_some(inner);
    }
    // Bare quoted-string form.
    let mut chars = stmt.chars();
    match (chars.next(), chars.last()) {
        (Some('"'), Some('"')) | (Some('\''), Some('\'')) if stmt.len() >= 2 => {
            Some(stmt[1..stmt.len() - 1].to_string())
        }
        _ => None,
    }
}

fn unquote_ref(s: &str) -> String {
    let s = s.trim();
    let s = s.strip_prefix('"').unwrap_or(s);
    let s = s.strip_suffix('"').unwrap_or(s);
    let s = s.strip_prefix('\'').unwrap_or(s);
    s.strip_suffix('\'').unwrap_or(s).to_string()
}

/// The site's custom theme root (v1 `theme.plan_roots`): the LAST import —
/// the first is always Wikidot's base theme — and `[]` when the only import
/// was the base theme (site uses no custom theme).
const BASE_THEME_PATH: &str = "common--theme/base/css/style.css";

pub fn plan_theme_roots(html: &str) -> Vec<String> {
    let imports = extract_theme_imports(html);
    match imports.last() {
        Some(last) if !last.contains(BASE_THEME_PATH) => vec![last.clone()],
        _ => Vec::new(),
    }
}

// ── Theme graph references (v1 parsers: extract_css_urls / resolve_url /
//    normalize_url) ──

/// All `url(...)` targets in a CSS string, order-preserving dedupe, with
/// `data:` payloads and `#fragment` refs filtered (nothing to fetch).
pub fn extract_css_urls(css: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for piece in css.split("url(").skip(1) {
        let inner = piece.split(')').next().unwrap_or("");
        let u = unquote_ref(inner);
        if !u.starts_with("data:") && !u.starts_with('#') && !out.contains(&u) {
            out.push(u);
        }
    }
    out
}

/// Resolve a possibly-relative CSS reference against the absolute URL of
/// the document containing it (RFC 3986 merge via the `url` crate).
/// Absolute references (`scheme://…`, `data:`) come back verbatim — v1
/// deliberately kept their original spelling instead of re-serializing.
pub fn resolve_url(base: &str, reference: &str) -> String {
    let r = reference.trim();
    if is_absolute_url(r) {
        return r.to_string();
    }
    match url::Url::parse(base) {
        Ok(b) => match b.join(r) {
            Ok(merged) => merged.to_string(),
            Err(_) => r.to_string(),
        },
        Err(_) => r.to_string(),
    }
}

/// Canonical form for visited-set membership: lowercase host, no fragment.
/// Path/query/port stay — a `?v=4.1` cache-buster is a distinct resource.
pub fn normalize_url(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut u) => {
            u.set_fragment(None);
            u.to_string()
        }
        Err(_) => url.to_string(),
    }
}

/// A reference is absolute iff it carries an explicit authority
/// (`scheme://…`) or is an inline `data:` payload. Everything else
/// (protocol-relative `//h/x`, `/abs/path`, `rel/path`) merges against the
/// base. The `://` rule avoids misclassifying a bare `category:name` Wikidot
/// slug as a URI scheme.
fn is_absolute_url(s: &str) -> bool {
    s.contains("://") || s.starts_with("data:")
}

// ── URL → files-row mapping (shared by the legacy importer, the theme
//    crawl and wikitext extraction) ──

/// Map a URL to its `(path, url)` files-row pair. `path` is the canonical
/// row key — always an absolute URL, and for anything Wikidot operates
/// always `http://{slug}.wikidot.com/…`: Wikidot serves one file namespace
/// per site under several host spellings (`{slug}.wikidot.com` 302s to
/// `{slug}.wdfiles.com`; `www.` prefixes and `https` work too — verified
/// live, identical final URLs), so they all collapse onto one spelling and
/// references written differently stop creating duplicate rows. `url` is
/// the origin the bytes came (or would come) from, kept verbatim.
pub fn file_row_paths(site: &str, url: &str) -> (String, String) {
    match url::Url::parse(url) {
        Ok(u) => match canonical_wikidot_url(site, &u) {
            Some(canonical) => (canonical, url.to_string()),
            None => {
                let mut u = u;
                u.set_fragment(None);
                let verbatim = u.to_string();
                (verbatim.clone(), verbatim)
            }
        },
        Err(_) => (url.to_string(), url.to_string()),
    }
}

/// Canonical `http://{slug}.wikidot.com{path}?{query}` when `u` sits on a
/// Wikidot-operated host: this site's own hosts, or any
/// `{slug}.wikidot.com` / `{slug}.wdfiles.com` (cross-site references —
/// sandboxes, sister wikis). `www.` prefixes are stripped (same namespace)
/// and `%3A` in the path is decoded to the literal `:` — Wikidot page
/// slugs are `category:name` and both spellings of the separator name the
/// same file. Returns None for hosts Wikidot does not operate: CDNs and
/// custom domains fronting a site keep their URL verbatim.
fn canonical_wikidot_url(site: &str, u: &url::Url) -> Option<String> {
    if !matches!(u.scheme(), "http" | "https") {
        return None;
    }
    let host = u.host_str()?.to_ascii_lowercase();
    let host = host.strip_prefix("www.").unwrap_or(&host);
    let own = host == format!("{site}.wikidot.com") || host == format!("{site}.wdfiles.com");
    let slug = if own {
        site.to_string()
    } else {
        host.strip_suffix(".wikidot.com")
            .filter(|s| !s.is_empty())
            .or_else(|| host.strip_suffix(".wdfiles.com").filter(|s| !s.is_empty()))?
            .to_string()
    };
    let mut out = format!("http://{slug}.wikidot.com{}", decode_colons(u.path()));
    if let Some(q) = u.query().filter(|q| !q.is_empty()) {
        out.push('?');
        out.push_str(q);
    }
    Some(out)
}

/// Lowercased host of an absolute URL (None for relative paths / junk).
pub(crate) fn url_host(s: &str) -> Option<String> {
    url::Url::parse(s)
        .ok()
        .and_then(|u| u.host_str().map(str::to_ascii_lowercase))
}

fn decode_colons(path: &str) -> String {
    path.replace("%3A", ":").replace("%3a", ":")
}

// ── Page source (from PageSourceModule AJAX body) ──

/// The page's wikitext from `<div class="page-source">…</div>`.
/// A missing element is a parse error for the caller.
pub fn extract_page_source(ajax_body_html: &str) -> Option<String> {
    let doc = Html::parse_document(ajax_body_html);
    let sel = Selector::parse(".page-source").ok()?;
    let el = doc.select(&sel).next()?;
    let mut text: String = el.text().collect();
    // v1 fidelity: drop literal `<br />` markers.
    if text.contains("<br />") {
        text = text.replace("<br />", "");
    }
    Some(text)
}

// ── RecentChanges (SiteChangesListModule) ──

/// Extract change rows. Each `.changes-list-item` contributes:
/// slug (td.title > a[href]), rev (td.revision-no), timestamp
/// (`time_…` class on `.odate` inside td.mod-date), author
/// (`data-id` on `.printuser` or `userInfo(…)` in a child `<a>` onclick).
pub fn extract_changes(html: &str) -> Vec<ChangeEntry> {
    let doc = Html::parse_document(html);
    let s_item = match Selector::parse(".changes-list-item") {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let s_title_a = ok_sel("td.title a");
    let s_revno = ok_sel("td.revision-no");
    let s_odate = ok_sel("td.mod-date .odate");
    let s_printuser = ok_sel(".printuser");

    let mut out = Vec::new();
    for item in doc.select(&s_item) {
        let Some(href) = item
            .select(&s_title_a)
            .find_map(|a| a.value().attr("href").map(String::from))
        else {
            continue;
        };
        let slug_str = href.strip_prefix('/').unwrap_or(&href).to_string();
        let rev_no = item.select(&s_revno).next().map(parse_rev).unwrap_or(0);
        let ts = item
            .select(&s_odate)
            .find_map(|el| time_from_class(el.value().attr("class")))
            .unwrap_or(0);
        let author = item
            .select(&s_printuser)
            .find_map(author_from_el)
            .unwrap_or(0);
        out.push(ChangeEntry {
            slug: Slug::parse(&slug_str),
            rev_no,
            ts,
            author,
        });
    }
    out
}

// ── Revision list (PageRevisionListModule) ──

/// Extract revision rows from `<tr>`s:
/// rev_id (`input[value]` or `input[id]`), rev_no (first `<td>` whose text
/// parses as an integer, "." separators stripped), ts (`.odate time_…`),
/// author (`.printuser`).
pub fn extract_revisions(html: &str) -> Vec<ParsedRevision> {
    let doc = Html::parse_document(html);
    let s_tr = match Selector::parse("tr") {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let s_input = ok_sel("input");
    let s_td = ok_sel("td");
    let s_odate = ok_sel(".odate");
    let s_printuser = ok_sel(".printuser");

    let mut out = Vec::new();
    for tr in doc.select(&s_tr) {
        let Some(rev_id) = tr.select(&s_input).find_map(|i| {
            let v = i.value();
            v.attr("value")
                .or_else(|| v.attr("id"))
                .filter(|s| !s.is_empty())
                .map(String::from)
        }) else {
            continue;
        };
        // Only rows that carry a revision number are real rows.
        let Some(rev_no) = tr.select(&s_td).find_map(|td| rev_from_td(td)) else {
            continue;
        };
        let ts = tr
            .select(&s_odate)
            .find_map(|el| time_from_class(el.value().attr("class")))
            .unwrap_or(0);
        let author = tr
            .select(&s_printuser)
            .find_map(author_from_el)
            .unwrap_or(0);
        out.push(ParsedRevision {
            rev_id,
            rev_no,
            ts,
            author,
        });
    }
    out
}

// ── Files ──

/// Attachment paths from a PageFilesModule listing: every `a[href]`
/// containing `local--files/`, as canonical absolute URLs (the listing may
/// spell the host either way; own-site references lift onto the main
/// domain).
pub fn extract_page_files(site: &str, html: &str) -> Vec<String> {
    let doc = Html::parse_document(html);
    let s_a = match Selector::parse("a") {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for a in doc.select(&s_a) {
        if let Some(href) = a.value().attr("href")
            && let Some(path) = file_path_from_ref(site, href)
            && seen.insert(path.clone())
        {
            out.push(path);
        }
    }
    out
}

/// `local--files/…` references inside wikitext source, deduped (v1
/// `extract_file_links`), as canonical absolute URLs.
///
/// A reference preceded immediately by a `scheme://host/` prefix is
/// absolute and canonicalizes through `file_row_paths`: hosts on this site
/// and any other `{slug}.wikidot.com`/`{slug}.wdfiles.com` spelling fold
/// onto `http://{slug}.wikidot.com`, every other host (CDNs, custom
/// domains fronting a site) keeps its full URL. Bare references are
/// site-relative and lift onto `http://{site}.wikidot.com` — so a row key
/// always names exactly one file no matter how the wikitext spelled it.
pub fn extract_file_links(site: &str, content: &str) -> Vec<String> {
    let parts: Vec<&str> = content.split("local--files/").collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for pair in parts.windows(2) {
        let taken = take_until_stop(pair[1]);
        if taken.is_empty() {
            continue;
        }
        let path = match absolute_base_before(pair[0]) {
            Some(base) => file_row_paths(site, &format!("{base}/local--files/{taken}")).0,
            None => own_file_url(site, &taken),
        };
        if seen.insert(path.clone()) {
            out.push(path);
        }
    }
    out
}

/// If the text ending right before a `local--files/` occurrence is an
/// `http(s)://host/` URL prefix, return `scheme://host` — the reference is
/// absolute. Only web schemes qualify: wikitext typos like
/// `[[=imagehttp://host/…` (missing space) would otherwise pass as the
/// "scheme" `imagehttp` and jam the two URLs together. Anything else
/// sitting between host and `local--files/` (a path segment, a space, …)
/// also disqualifies: the reference is site-relative.
fn absolute_base_before(prev: &str) -> Option<String> {
    let pos = prev.rfind("://")?;
    let scheme_start = prev[..pos]
        .char_indices()
        .rev()
        .find(|(_, c)| !c.is_ascii_alphanumeric() && !matches!(c, '+' | '-' | '.'))
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0);
    let scheme = &prev[scheme_start..pos];
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return None;
    }
    let after = &prev[pos + 3..];
    let (host, rest) = after.split_once('/').unwrap_or((after, ""));
    let host_ok = !host.is_empty()
        && host.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.'));
    if !host_ok || !rest.is_empty() {
        return None;
    }
    Some(format!("{scheme}://{host}"))
}

// ── Helpers ──

fn ok_sel(css: &str) -> Selector {
    Selector::parse(css).expect("static selector")
}

fn select_first_text(html: &str, css: &str) -> String {
    let doc = Html::parse_document(html);
    match Selector::parse(css) {
        Ok(sel) => doc
            .select(&sel)
            .next()
            .map(|el| el.text().collect::<String>().trim().to_string())
            .unwrap_or_default(),
        Err(_) => String::new(),
    }
}

fn select_texts(html: &str, css: &str) -> Vec<String> {
    let doc = Html::parse_document(html);
    match Selector::parse(css) {
        Ok(sel) => doc
            .select(&sel)
            .map(|el| el.text().collect::<String>().trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        Err(_) => vec![],
    }
}

/// `class="odate time_1781026204 format_…"` → `1781026204`.
fn time_from_class(class: Option<&str>) -> Option<i64> {
    let rest = class?.split_once("time_")?.1;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Author from a `.printuser` element: `data-id` attr (deleted accounts) or
/// `userInfo(4518363)` in a child `<a>`'s onclick.
fn author_from_el(el: ElementRef) -> Option<i64> {
    if let Some(id) = el.value().attr("data-id")
        && let Ok(n) = id.trim().parse()
    {
        return Some(n);
    }
    let s_a = ok_sel("a");
    el.select(&s_a).find_map(|a| {
        let onclick = a.value().attr("onclick")?;
        let rest = onclick.split_once("userInfo(")?.1;
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        digits.parse().ok()
    })
}

/// Revision number from "(рев. 445)" / "(rev. 445)" / "(новый)" / "(new)".
/// Language-agnostic like v1: strip parens, take the last whitespace token,
/// parse as int; anything else is 0 (new page).
fn parse_rev(el: ElementRef) -> i64 {
    let text: String = el.text().collect();
    text.replace(['(', ')'], "")
        .split_whitespace()
        .next_back()
        .and_then(|t| t.trim().parse().ok())
        .unwrap_or(0)
}

/// First `<td>` whose text is an integer ("." thousands separators stripped).
fn rev_from_td(td: ElementRef) -> Option<i64> {
    let text: String = td.text().collect();
    let cleaned: String = text.trim().replace('.', "");
    if cleaned.is_empty() {
        return None;
    }
    cleaned.parse().ok()
}

/// Any string containing `local--files/…` → the canonical URL, stopping at
/// URL/quote delimiters (v1 `take_until_stop_char`). An absolute
/// `scheme://host/` prefix right before the reference routes through
/// `file_row_paths`; anything else (a leading `/`, bare name) is this
/// site's namespace.
fn file_path_from_ref(site: &str, href: &str) -> Option<String> {
    let (prev, rest) = href.split_once("local--files/")?;
    let taken = take_until_stop(rest);
    if taken.is_empty() {
        return None;
    }
    Some(match absolute_base_before(prev) {
        Some(base) => file_row_paths(site, &format!("{base}/local--files/{taken}")).0,
        None => own_file_url(site, &taken),
    })
}

/// Canonical URL of a site-relative `local--files/…` tail: lifted onto the
/// main domain and pushed through the same canonicalization as absolute
/// references, so a `%3A`-spelled slug converges with its `:` twin.
fn own_file_url(site: &str, tail: &str) -> String {
    file_row_paths(site, &format!("http://{site}.wikidot.com/local--files/{tail}")).0
}

fn take_until_stop(s: &str) -> String {
    // `|` stops too: it separates wikitext arguments
    // (`…name=x.png|align=center`), never forms part of a URL path.
    s.chars()
        .take_while(|c| {
            !matches!(
                c,
                '"' | ' ' | '\n' | ')' | '>' | '\'' | ']' | '\\' | '|'
            )
        })
        .collect()
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_id() {
        let html = r#"<script>WIKIREQUEST.info.pageId = 9992; WIKIREQUEST.info.pageUnixName="start";</script>"#;
        assert_eq!(extract_page_id(html).as_deref(), Some("9992"));
        assert_eq!(extract_page_id("no marker"), None);
    }

    #[test]
    fn landing_slug() {
        // Real kumawa homepage shape.
        let html = r#"<script type="text/javascript">
WIKIREQUEST.info.domain = "kumawa.wikidot.com";
WIKIREQUEST.info.requestPageName = "blog:_start";
WIKIREQUEST.info.pageUnixName = "blog:_start";
WIKIREQUEST.info.pageId = 1457213925;
</script>"#;
        assert_eq!(extract_landing_slug(html).as_deref(), Some("blog:_start"));
        // Default landing, no category.
        assert_eq!(
            extract_landing_slug(r#"WIKIREQUEST.info.pageUnixName = "start";"#).as_deref(),
            Some("start")
        );
        // Single-quoted form.
        assert_eq!(
            extract_landing_slug(r#"WIKIREQUEST.info.pageUnixName = 'start';"#).as_deref(),
            Some("start")
        );
        // Absent, unquoted or empty → None (caller keeps the last known value).
        assert_eq!(extract_landing_slug("no marker"), None);
        assert_eq!(
            extract_landing_slug("WIKIREQUEST.info.pageUnixName = start;"),
            None
        );
        assert_eq!(
            extract_landing_slug(r#"WIKIREQUEST.info.pageUnixName = "";"#),
            None
        );
    }

    #[test]
    fn revision_number_is_language_agnostic() {
        let doc = Html::parse_document(
            "<table><tr><td>1.</td><td class='revision-no'>(рев. 445)</td></tr></table>",
        );
        let sel = Selector::parse(".revision-no").unwrap();
        let el = doc.select(&sel).next().unwrap();
        assert_eq!(parse_rev(el), 445);
        let doc = Html::parse_document("<table><tr><td>(rev. 12)</td></tr></table>");
        let sel = Selector::parse("td").unwrap();
        assert_eq!(parse_rev(doc.select(&sel).next().unwrap()), 12);
        let doc = Html::parse_document("<table><tr><td>(new)</td></tr></table>");
        let sel = Selector::parse("td").unwrap();
        assert_eq!(parse_rev(doc.select(&sel).next().unwrap()), 0);
    }

    #[test]
    fn changes_rows() {
        let html = r##"
        <table class="changes-list">
          <tr class="changes-list-item">
            <td class="title"><a href="/scp-001">SCP-001</a></td>
            <td class="revision-no" style="text-align: right">(рев. 4)</td>
            <td class="mod-date"><span class="odate time_1781026204 format_%e %b %Y, %H:%M (%O) ago">…</span></td>
            <td class="mod-by"><span class="printuser"><a onclick="OZONE.dialog.userInfo(4518363); return false;" href="#">Dr Ash</a></span></td>
          </tr>
          <tr class="changes-list-item">
            <td class="title"><a href="/draft:thing">thing</a></td>
            <td class="revision-no">(новый)</td>
            <td class="mod-date"><span class="odate time_1781000000">…</span></td>
            <td class="mod-by"><span class="printuser deleted" data-id="4473061"></span></td>
          </tr>
        </table>"##;
        let changes = extract_changes(html);
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].slug.as_str(), "scp-001");
        assert_eq!(changes[0].rev_no, 4);
        assert_eq!(changes[0].ts, 1781026204);
        assert_eq!(changes[0].author, 4518363);
        assert_eq!(changes[1].slug.as_str(), "draft:thing");
        assert_eq!(changes[1].rev_no, 0);
        assert_eq!(changes[1].author, 4473061);
    }

    #[test]
    fn revision_rows_survive_unclosed_td() {
        // Wikidot never closes the date <td> before the comment <td> opens.
        // html5ever recovers the tree; rows must still parse individually.
        let html = r#"
        <table>
          <tr>
            <td><input type="checkbox" name="revision-1297293773" value="1297293773"></td>
            <td>1.299</td>
            <td><span class="odate time_1595924913">…</span>
            <td class="diff-no">…</td>
            <td><span class="printuser"><a onclick="userInfo(4518363)">x</a></span></td>
          </tr>
          <tr>
            <td><input value="1297293774"></td>
            <td>1.300</td>
            <td><span class="odate time_1595925900">…</span></td>
            <td><span class="printuser" data-id="99"></span></td>
          </tr>
        </table>"#;
        let revs = extract_revisions(html);
        assert_eq!(revs.len(), 2);
        assert_eq!(revs[0].rev_id, "1297293773");
        assert_eq!(revs[0].rev_no, 1299); // "1.299" → ru locale thousands separator
        assert_eq!(revs[0].ts, 1595924913);
        assert_eq!(revs[0].author, 4518363);
        assert_eq!(revs[1].rev_id, "1297293774");
        assert_eq!(revs[1].rev_no, 1300);
        assert_eq!(revs[1].author, 99);
    }

    #[test]
    fn file_links() {
        let src = concat!(
            r#"[[image 9992.jpg]] rel local--files/page/Photo (1).jpg [[/image]] and "#,
            r#"own http://mywiki.wdfiles.com/local--files/x/a.png or "#,
            r#"foreign [http://sandbox.wikidot.com/local--files/p/c.png|caption] "#,
            r#"not-a-host-prefix http://other.wikidot.com/zap local--files/w/u.png" end"#
        );
        let links = extract_file_links("mywiki", src);
        // Every reference is a canonical absolute URL: bare and own-host
        // spellings lift/collapse onto the main domain, a foreign wikidot
        // host keeps its site, `|` ends a name, and a URL followed by more
        // text does NOT prefix the next reference.
        assert_eq!(
            links,
            vec![
                "http://mywiki.wikidot.com/local--files/page/Photo".to_string(),
                "http://mywiki.wikidot.com/local--files/x/a.png".to_string(),
                "http://sandbox.wikidot.com/local--files/p/c.png".to_string(),
                "http://mywiki.wikidot.com/local--files/w/u.png".to_string(),
            ]
        );
        let listing = concat!(
            r#"<a href="/local--files/rpc-001/cover.jpg">cover</a>"#,
            r#"<a href="http://rpcauthority.wikidot.com/local--files/rpc-001/cover.jpg">dup</a>"#,
        );
        assert_eq!(
            extract_page_files("rpcauthority", listing),
            vec!["http://rpcauthority.wikidot.com/local--files/rpc-001/cover.jpg".to_string()]
        );
    }

    /// One file, many spellings — all Wikidot-operated host variants
    /// collapse to `http://{slug}.wikidot.com`; hosts Wikidot does not
    /// operate keep their URL (fragment dropped).
    #[test]
    fn canonical_urls_collapse_wikidot_host_spellings() {
        let key = |u: &str| file_row_paths("rpcauthority", u).0;
        let own = "http://rpcauthority.wikidot.com/local--files/rpc-1/a.jpg";
        assert_eq!(key("http://rpcauthority.wikidot.com/local--files/rpc-1/a.jpg"), own);
        assert_eq!(key("https://rpcauthority.wdfiles.com/local--files/rpc-1/a.jpg"), own);
        assert_eq!(key("http://www.rpcauthority.wikidot.com/local--files/rpc-1/a.jpg#x"), own);
        assert_eq!(
            key("http://RPCSANDBOX.wdfiles.com/local--files/component%3Atheme/x.css"),
            "http://rpcsandbox.wikidot.com/local--files/component:theme/x.css"
        );
        assert_eq!(
            key("https://cdn.jsdelivr.net/gh/x/style.css"),
            "https://cdn.jsdelivr.net/gh/x/style.css"
        );
        // A custom domain fronting a site is NOT Wikidot-operated: verbatim.
        assert_eq!(
            key("http://www.rpc-wiki.net/local--files/rpc-1/a.jpg?v=2"),
            "http://www.rpc-wiki.net/local--files/rpc-1/a.jpg?v=2"
        );
        // The pair's second element keeps the original spelling (fetch
        // provenance) with the fragment stripped.
        assert_eq!(
            file_row_paths("s", "http://x.test/a.css#frag"),
            ("http://x.test/a.css".to_string(), "http://x.test/a.css".to_string())
        );
        assert_eq!(
            file_row_paths("s", "https://s.wdfiles.com/local--code/theme/1.css").1,
            "https://s.wdfiles.com/local--code/theme/1.css"
        );
        // Query survives canonicalization (cache-busted theme CSS).
        assert_eq!(
            key("http://s.wdfiles.com/local--code/theme/1.css?v=4"),
            "http://s.wikidot.com/local--code/theme/1.css?v=4"
        );
    }

    #[test]
    fn file_links_pipe_arguments_do_not_leak_into_names() {
        let src = r#"[[include component:image-block name=http://sandbox.wdfiles.com/local--files/hydrozen-sandbox/01.png|align=center|width=660px|caption=RPC-001.]]"#;
        assert_eq!(
            extract_file_links("rpcauthority", src),
            vec!["http://sandbox.wikidot.com/local--files/hydrozen-sandbox/01.png".to_string()]
        );
    }

    #[test]
    fn absolute_base_before_requires_bare_host() {
        assert_eq!(
            absolute_base_before("see http://rpcsandbox.wdfiles.com/"),
            Some("http://rpcsandbox.wdfiles.com".to_string())
        );
        assert_eq!(
            absolute_base_before("see https://rpcsandbox.wdfiles.com"),
            Some("https://rpcsandbox.wdfiles.com".to_string())
        );
        // path segment between host and the reference → not a prefix
        assert_eq!(absolute_base_before("see http://x.wikidot.com/page "), None);
        // no scheme at all
        assert_eq!(absolute_base_before("[[image "), None);
    }

    /// `[[=imagehttp://host/…` — the author forgot the space after the
    /// image element. `imagehttp` must not pass as a URL scheme (that
    /// jams two URLs into one path); the reference reads as site-relative,
    /// which for an own-host typo is exactly the intended file.
    #[test]
    fn typo_jammed_urls_do_not_become_schemes() {
        let src = "[[=imagehttp://rpcauthority.wdfiles.com/local--files/two-sides/due.jpg]";
        assert_eq!(
            extract_file_links("rpcauthority", src),
            vec![
                "http://rpcauthority.wikidot.com/local--files/two-sides/due.jpg".to_string()
            ]
        );
        assert_eq!(absolute_base_before("[[=imagehttp://x.wdfiles.com/"), None);
    }

    /// Relative references spelled with `%3A` converge with their `:`
    /// twins — one canonical key per file regardless of spelling.
    #[test]
    fn relative_refs_decode_percent_colons() {
        assert_eq!(
            extract_file_links("demo", "img local--files/nav%3Aside/discord.png"),
            vec!["http://demo.wikidot.com/local--files/nav:side/discord.png".to_string()]
        );
        assert_eq!(
            extract_page_files(
                "demo",
                r#"<a href="/local--files/component%3Atheme/x.css">t</a>"#
            ),
            vec!["http://demo.wikidot.com/local--files/component:theme/x.css".to_string()]
        );
    }

    #[test]
    fn page_source_strips_br_markers() {
        let body = r#"<div class="page-source">line one&lt;br /&gt;line two &amp; more</div>"#;
        assert_eq!(
            extract_page_source(body).as_deref(),
            Some("line oneline two & more")
        );
    }
}
