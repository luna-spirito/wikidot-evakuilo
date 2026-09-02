//! HTTP client + global rate limiter — port of v1's `web/http.gleam` +
//! `ratelimiter.gleam` + `cookie_jar.gleam`.
//!
//! The limiter is a single bottleneck for ALL Wikidot requests across all
//! sites (v1 semantics): a ticker grants one queued waiter per
//! `rate_limit_ms`. Every attempt of every request takes a fresh ticket,
//! including retries.
//!
//! Cookies are managed manually (v1-style): all `Set-Cookie` pairs merge
//! into a per-site name→value map, replayed as a `Cookie` header. The CSRF
//! token `wikidot_token7` is tracked for the AJAX form parameter.

use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::model::FetchError;

// ── Rate limiter ──

#[derive(Clone)]
pub struct Limiter {
    tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
}

impl Limiter {
    /// One grant per `interval_ms`, FIFO.
    pub fn new(interval_ms: u64) -> Limiter {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(limiter_task(interval_ms, rx));
        Limiter { tx }
    }

    /// Wait for a request ticket.
    pub async fn acquire(&self) {
        let (done_tx, done_rx) = oneshot::channel();
        if self.tx.send(done_tx).is_ok() {
            let _ = done_rx.await;
        }
    }
}

async fn limiter_task(interval_ms: u64, rx: mpsc::UnboundedReceiver<oneshot::Sender<()>>) {
    let mut rx = rx;
    let mut queue: VecDeque<oneshot::Sender<()>> = VecDeque::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // first tick fires immediately; skip it
    loop {
        tokio::select! {
            waiter = rx.recv() => match waiter {
                Some(tx) => queue.push_back(tx),
                None => break,
            },
            _ = ticker.tick() => {
                if let Some(tx) = queue.pop_front() {
                    let _ = tx.send(());
                }
            }
        }
    }
    // Channel closed (shutdown): release everyone still queued.
    while let Some(tx) = queue.pop_front() {
        let _ = tx.send(());
    }
}

// ── Cookies ──

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

    fn merge_set_cookies<'a, I>(&mut self, headers: I)
    where
        I: Iterator<Item = &'a str>,
    {
        for value in headers {
            let pair = value.split(';').next().unwrap_or("").trim();
            if let Some((name, val)) = pair.split_once('=') {
                let name = name.trim();
                if !name.is_empty() {
                    self.cookies
                        .insert(name.to_string(), val.trim().to_string());
                }
            }
        }
    }
}

// ── Client ──

pub struct Wikidot {
    http: reqwest::Client,
    limiter: Limiter,
    jars: Mutex<HashMap<String, CookieJar>>,
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
            token_gates: Mutex::new(HashMap::new()),
            timeout: Duration::from_secs(timeout_s),
        })
    }

    /// Rate-limited GET. `url` must be absolute. Cookies in, cookies out.
    pub async fn get(&self, site: &str, url: &str) -> Result<reqwest::Response, FetchError> {
        self.limiter.acquire().await;
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
    pub async fn get_public(&self, url: &str) -> Result<reqwest::Response, FetchError> {
        self.limiter.acquire().await;
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
    ) -> Result<reqwest::Response, FetchError> {
        self.limiter.acquire().await;
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
        let mut jars = self.jars.lock();
        jars.entry(site.to_string())
            .or_default()
            .merge_set_cookies(values.iter().copied());
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
