//! HTTP client + global rate limiter — port of v1's `web/http.gleam` +
//! `ratelimiter.gleam` + `cookie_jar.gleam`.
//!
//! The limiter is a single bottleneck for ALL Wikidot requests across all
//! sites (v1 semantics): grants are spaced at least `rate_limit_ms` apart.
//! Grants are NOT FIFO: every waiter carries its job's
//! claim priority (`jobs::prio`), and the highest-priority waiter wins the
//! next ticket — so a fresh site's urgent jobs take tickets ahead of bulk
//! history queued long before by backed-up sites (FIFO within one
//! priority). An idle limiter answers immediately instead of waiting out a
//! tick phase. Every attempt of every request takes a fresh ticket,
//! including retries.
//!
//! Cookies are managed manually (v1-style): all `Set-Cookie` pairs merge
//! into a per-site name→value map, replayed as a `Cookie` header. The CSRF
//! token `wikidot_token7` is tracked for the AJAX form parameter. The map
//! is persisted to the site's own database (`meta` key `cookies`) whenever
//! it changes and reloaded on daemon start — a restart keeps its session
//! instead of burning a bootstrap GET per site.

use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::{db::Db, model::FetchError};

// ── Rate limiter ──

/// A queued ticket request: `prio` is the executing job's claim priority,
/// `seq` preserves arrival order within one priority.
struct Waiter {
    prio: i64,
    seq: u64,
    tx: oneshot::Sender<()>,
}

/// Max-heap order: highest priority first, then oldest arrival.
impl Ord for Waiter {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.prio, Reverse(self.seq)).cmp(&(other.prio, Reverse(other.seq)))
    }
}
impl PartialOrd for Waiter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for Waiter {}
impl PartialEq for Waiter {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

static NEXT_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct Limiter {
    tx: mpsc::UnboundedSender<Waiter>,
}

impl Limiter {
    /// One grant per `interval_ms`, highest priority first (FIFO within a
    /// priority).
    pub fn new(interval_ms: u64) -> Limiter {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(limiter_task(interval_ms, rx));
        Limiter { tx }
    }

    /// Wait for a request ticket. `prio` is the executing job's enqueue
    /// priority (`jobs::prio`): when the queue backs up, urgent jobs claim
    /// tickets ahead of bulk backlog that has been waiting longer.
    pub async fn acquire(&self, prio: i64) {
        let seq = NEXT_SEQ.fetch_add(1, Relaxed);
        let (done_tx, done_rx) = oneshot::channel();
        if self.tx.send(Waiter { prio, seq, tx: done_tx }).is_ok() {
            let _ = done_rx.await;
        }
    }
}

async fn limiter_task(interval_ms: u64, mut rx: mpsc::UnboundedReceiver<Waiter>) {
    let interval = Duration::from_millis(interval_ms.max(1));
    let mut queue: BinaryHeap<Waiter> = BinaryHeap::new();
    let mut last_grant: Option<tokio::time::Instant> = None;
    loop {
        // Earliest legal grant: immediately after an idle stretch, else
        // `interval` after the previous one (absolute time, so arrival
        // latency never accumulates).
        let due = last_grant.map_or(tokio::time::Instant::now(), |t| t + interval);
        tokio::select! {
            waiter = rx.recv() => match waiter {
                Some(w) => queue.push(w),
                None => break,
            },
            _ = tokio::time::sleep_until(due), if !queue.is_empty() => {
                if let Some(w) = queue.pop() {
                    let _ = w.tx.send(());
                    last_grant = Some(tokio::time::Instant::now());
                }
            }
        }
    }
    // Channel closed (shutdown): release everyone still queued.
    while let Some(w) = queue.pop() {
        let _ = w.tx.send(());
    }
}

// ── Cookies ──

/// `meta` row holding the site's persisted session (`CookieJar::to_json`).
const COOKIE_KEY: &str = "cookies";

#[derive(Default)]
struct CookieJar {
    cookies: HashMap<String, String>,
}

impl CookieJar {
    fn token7(&self) -> Option<&str> {
        self.cookies.get("wikidot_token7").map(String::as_str)
    }

    fn header(&self) -> Option<String> {
        if self.cookies.is_empty() {
            return None;
        }
        Some(
            self.cookies
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("; "),
        )
    }

    /// Merge `Set-Cookie` values; true when anything changed (a fresh name
    /// or a new value — the persistence trigger).
    fn merge_set_cookies<'a, I>(&mut self, headers: I) -> bool
    where
        I: Iterator<Item = &'a str>,
    {
        let mut changed = false;
        for value in headers {
            let pair = value.split(';').next().unwrap_or("").trim();
            if let Some((name, val)) = pair.split_once('=') {
                let name = name.trim();
                let val = val.trim();
                if !name.is_empty() && self.cookies.get(name) != Some(&val.to_string()) {
                    self.cookies.insert(name.to_string(), val.to_string());
                    changed = true;
                }
            }
        }
        changed
    }

    /// Drop the CSRF token: the connector rejected it as stale
    /// (`wrong_token7`), and only the token is suspect — any session cookie
    /// alongside it stays. True when it was present.
    fn drop_token7(&mut self) -> bool {
        self.cookies.remove("wikidot_token7").is_some()
    }

    fn to_json(&self) -> String {
        serde_json::to_string(&self.cookies).unwrap_or_default()
    }

    fn from_json(json: &str) -> Option<CookieJar> {
        serde_json::from_str::<HashMap<String, String>>(json)
            .ok()
            .map(|cookies| CookieJar { cookies })
    }
}

// ── Client ──

pub struct Wikidot {
    http: reqwest::Client,
    limiter: Limiter,
    jars: Mutex<HashMap<String, CookieJar>>,
    /// Per-site session store: the site's own database. Present only for
    /// daemon-run sites; one-shot commands run sessionless.
    stores: Mutex<HashMap<String, Db>>,
    /// Serializes CSRF bootstrap per site: without it, N concurrent workers
    /// with a cold session each burn a GET on token refresh.
    token_gates: Mutex<HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>>,
    timeout: Duration,
}

impl Wikidot {
    pub fn new(rate_limit_ms: u64, timeout_s: u64) -> anyhow::Result<Wikidot> {
        let http = reqwest::Client::builder()
            .user_agent("WikidotEvakuilo/4.0")
            .timeout(Duration::from_secs(timeout_s))
            .build()?;
        Ok(Wikidot {
            http,
            limiter: Limiter::new(rate_limit_ms),
            jars: Mutex::new(HashMap::new()),
            stores: Mutex::new(HashMap::new()),
            token_gates: Mutex::new(HashMap::new()),
            timeout: Duration::from_secs(timeout_s),
        })
    }

    /// Attach a site's database as its session store: cookies persisted
    /// under `meta.cookies` are loaded (a restart keeps its CSRF token
    /// instead of re-bootstrapping), and later changes are written back.
    /// Call once per site at startup, before any worker runs.
    pub fn attach_store(&self, site: &str, db: Db) {
        if let Ok(Some(json)) = db.meta_get(COOKIE_KEY)
            && let Some(jar) = CookieJar::from_json(&json)
        {
            // Never clobber a live jar; attach runs before any worker anyway.
            self.jars.lock().entry(site.to_string()).or_insert(jar);
        }
        self.stores.lock().insert(site.to_string(), db);
    }

    /// Write the site's current jar to its database. Best-effort: a failed
    /// write only costs a re-bootstrap after the next restart.
    fn persist_cookies(&self, site: &str) {
        let json = self.jars.lock().get(site).map(CookieJar::to_json);
        if let (Some(json), Some(db)) = (json, self.stores.lock().get(site).cloned())
            && let Err(e) = db.meta_set(COOKIE_KEY, &json)
        {
            tracing::warn!(site, error = %e, "persisting session cookies failed");
        }
    }

    /// Drop the site's CSRF token, in memory and on disk: the AJAX
    /// connector rejected it as stale, so the next call must re-bootstrap.
    pub fn forget_token(&self, site: &str) {
        if self.jars.lock().get_mut(site).is_some_and(CookieJar::drop_token7) {
            tracing::info!(site, "wikidot_token7 rejected as stale; will re-bootstrap");
            self.persist_cookies(site);
        }
    }

    /// Rate-limited GET. `url` must be absolute. Cookies in, cookies out.
    pub async fn get(&self, site: &str, url: &str, prio: i64) -> Result<reqwest::Response, FetchError> {
        self.limiter.acquire(prio).await;
        let mut req = self.http.get(url).timeout(self.timeout);
        if let Some(cookie) = self.cookie_header(site) {
            req = req.header("cookie", cookie);
        }
        let resp = req.send().await.map_err(map_reqwest_err)?;
        self.absorb_cookies(site, &resp);
        check_status(resp).await
    }

    /// Rate-limited GET of a public third-party resource (theme CSS on a
    /// CDN, fonts, …): NO cookie header out, NO cookie absorb in — the
    /// site's Wikidot session must never leak to (or be polluted by) other
    /// hosts. Shares the global limiter: one politeness budget for all hosts.
    pub async fn get_public(&self, url: &str, prio: i64) -> Result<reqwest::Response, FetchError> {
        self.limiter.acquire(prio).await;
        let req = self.http.get(url).timeout(self.timeout);
        let resp = req.send().await.map_err(map_reqwest_err)?;
        check_status(resp).await
    }

    /// Rate-limited POST of a urlencoded form.
    pub async fn post_form(
        &self,
        site: &str,
        url: &str,
        params: &[(&str, String)],
        prio: i64,
    ) -> Result<reqwest::Response, FetchError> {
        self.limiter.acquire(prio).await;
        let mut req = self.http.post(url).timeout(self.timeout).form(params);
        if let Some(cookie) = self.cookie_header(site) {
            req = req.header("cookie", cookie);
        }
        let resp = req.send().await.map_err(map_reqwest_err)?;
        self.absorb_cookies(site, &resp);
        check_status(resp).await
    }

    pub fn cookie_header(&self, site: &str) -> Option<String> {
        self.jars.lock().get(site).and_then(|j| j.header())
    }

    /// Current CSRF token for the site, if any.
    pub fn token7(&self, site: &str) -> Option<String> {
        self.jars
            .lock()
            .get(site)
            .and_then(|j| j.token7().map(String::from))
    }

    /// Async per-site mutex guarding token bootstrap.
    pub fn token_gate(&self, site: &str) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        self.token_gates
            .lock()
            .entry(site.to_string())
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn absorb_cookies(&self, site: &str, resp: &reqwest::Response) {
        let values: Vec<&str> = resp
            .headers()
            .get_all(reqwest::header::SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect();
        if values.is_empty() {
            return;
        }
        let changed = {
            let mut jars = self.jars.lock();
            jars.entry(site.to_string())
                .or_default()
                .merge_set_cookies(values.iter().copied())
        };
        if changed {
            self.persist_cookies(site);
        }
    }
}

fn map_reqwest_err(e: reqwest::Error) -> FetchError {
    if e.is_timeout() {
        FetchError::Timeout
    } else if e.is_status() {
        // non-2xx that reqwest surfaced — mapped by check_status normally;
        // here it means redirect-policy or similar edge.
        FetchError::Http(format!(
            "HTTP {}",
            e.status().map(|s| s.as_u16()).unwrap_or(0)
        ))
    } else {
        FetchError::Http(e.to_string())
    }
}

async fn check_status(resp: reqwest::Response) -> Result<reqwest::Response, FetchError> {
    match resp.status().as_u16() {
        404 => Err(FetchError::NotFound),
        403 => Err(FetchError::Forbidden),
        429 => Err(FetchError::RateLimited),
        code if (400..500).contains(&code) => Err(FetchError::Http(format!("HTTP {code}"))),
        code if code >= 500 => Err(FetchError::Http(format!("server {code}"))),
        _ => Ok(resp),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // ── Cookie jar ──

    #[test]
    fn jar_merge_reports_changes() {
        let mut jar = CookieJar::default();
        assert!(jar.merge_set_cookies(["wikidot_token7=abc; Path=/"].into_iter()));
        assert_eq!(jar.token7(), Some("abc"));
        // Identical re-set: no change.
        assert!(!jar.merge_set_cookies(["wikidot_token7=abc; HttpOnly"].into_iter()));
        // Fresh value: change.
        assert!(jar.merge_set_cookies(["wikidot_token7=def"].into_iter()));
        assert_eq!(jar.token7(), Some("def"));
        // Garbage pairs are ignored without poisoning the changed flag.
        assert!(!jar.merge_set_cookies(["novalue", "=novalue2"].into_iter()));
    }

    #[test]
    fn jar_json_round_trip_and_token_drop() {
        let mut jar = CookieJar::default();
        jar.merge_set_cookies(["wikidot_token7=abc", "wikidot_session=s1"].into_iter());
        let json = jar.to_json();
        let back = CookieJar::from_json(&json).unwrap();
        assert_eq!(back.token7(), Some("abc"));
        assert!(back.header().unwrap().contains("wikidot_session=s1"));
        // Garbage JSON is a cold session, never a panic.
        assert!(CookieJar::from_json("not json").is_none());
        // Dropping the token keeps the session.
        let mut jar = back;
        assert!(jar.drop_token7());
        assert_eq!(jar.token7(), None);
        assert!(jar.header().unwrap().contains("wikidot_session=s1"));
        assert!(!jar.drop_token7());
    }

    // ── Session persistence through the site DB ──

    #[tokio::test]
    async fn attach_store_loads_and_persists_session() {
        let dir = tempfile::tempdir().unwrap();
        let (db, _lock) = Db::open_locked(&dir.path().join("site.db")).unwrap();
        db.meta_set(COOKIE_KEY, r#"{"wikidot_token7":"t1","wikidot_session":"s1"}"#)
            .unwrap();

        let wik = Wikidot::new(50, 5).unwrap();
        wik.attach_store("demo", db.clone());
        // The persisted session is live without any bootstrap GET.
        let header = wik.cookie_header("demo").unwrap();
        assert!(header.contains("wikidot_token7=t1"));
        assert!(header.contains("wikidot_session=s1"));

        // Forgetting the token drops it from memory AND from the store,
        // keeping the session cookie.
        wik.forget_token("demo");
        assert_eq!(wik.token7("demo"), None);
        let stored = db.meta_get(COOKIE_KEY).unwrap().unwrap();
        assert!(!stored.contains("wikidot_token7"));
        assert!(stored.contains("wikidot_session"));

        // A second client attaches from the healed store.
        let wik2 = Wikidot::new(50, 5).unwrap();
        wik2.attach_store("demo", db);
        let header = wik2.cookie_header("demo").unwrap();
        assert!(!header.contains("wikidot_token7"));
        assert!(header.contains("wikidot_session=s1"));
    }

    // ── Limiter ──

    #[tokio::test]
    async fn limiter_serves_higher_priority_first() {
        let lim = Arc::new(Limiter::new(120));
        let order: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let mut handles = Vec::new();
        for (name, prio, delay_ms) in [("low-a", 1i64, 0u64), ("low-b", 1, 15), ("high", 9, 30)] {
            let lim = lim.clone();
            let order = order.clone();
            handles.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                lim.acquire(prio).await;
                order.lock().push(name.to_string());
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // The limiter is idle when `low-a` arrives, so it is granted
        // immediately; the later `high` then jumps the earlier-queued
        // `low-b`, and equal priorities keep arrival order.
        assert_eq!(*order.lock(), ["low-a", "high", "low-b"]);
    }

    #[tokio::test]
    async fn limiter_grants_one_per_interval_when_idle() {
        // An idle limiter still paces grants: two sequential acquires take
        // at least one full interval apart.
        let lim = Limiter::new(80);
        let start = std::time::Instant::now();
        lim.acquire(0).await;
        assert!(start.elapsed() < Duration::from_millis(80));
        lim.acquire(0).await;
        assert!(start.elapsed() >= Duration::from_millis(80));
    }
}
