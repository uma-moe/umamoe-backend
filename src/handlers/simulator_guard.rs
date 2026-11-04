//! Keeps website users from flooding the simulator. Identical requests are answered from a short
//! cache, and each website user runs one request at a time and, per route, a few a minute
//! (`routeLimit`: a share of `SIMULATOR_RUNS_PER_MINUTE`, or a fixed few for the costliest).
//! Authenticated API keys are not limited.
#![allow(non_snake_case)]

use axum::{
    body::Bytes,
    http::{header, HeaderMap},
};
use dashmap::{mapref::entry::Entry, DashMap};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// How long an answer stays cached; a redeployed simulator may answer differently after it.
const CACHE_TTL: Duration = Duration::from_secs(600);

/// The largest answer kept, and all kept answers together.
pub(crate) const CACHE_ENTRY_BYTES: usize = 2 * 1024 * 1024;
const CACHE_BYTES: usize = 64 * 1024 * 1024;

const RUN_WINDOW: Duration = Duration::from_secs(60);
const DEFAULT_RUNS_PER_MINUTE: u32 = 8;

/// A Skill Explorer build races about 1,500 times, 10–30 seconds of the simulator's workers: a
/// website user values a few every ten minutes, however many runs a minute the other routes allow.
const EXPLORER_BUILD_RUNS: u32 = 3;
const EXPLORER_BUILD_WINDOW: Duration = Duration::from_secs(600);

pub(crate) type CacheKey = [u8; 32];

pub struct SimulatorGuard {
    runsPerMinute: u32,
    running: Arc<DashMap<String, ()>>,
    windows: DashMap<String, RunWindow>,
    cache: Mutex<Cache>,
}

struct RunWindow {
    runs: u32,
    resetAt: Instant,
}

#[derive(Default)]
struct Cache {
    entries: HashMap<CacheKey, Cached>,
    bytes: usize,
}

struct Cached {
    headers: HeaderMap,
    body: Bytes,
    storedAt: Instant,
}

/// How many runs a website user may start on a route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteLimit {
    /// `share` times `SIMULATOR_RUNS_PER_MINUTE` a minute.
    PerMinute(u32),
    /// `runs` in each `window`, whatever `SIMULATOR_RUNS_PER_MINUTE` allows.
    Fixed { runs: u32, window: Duration },
}

/// Why a user's run was not started.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Refused {
    /// The user's previous run is still going.
    Running,
    /// The user started every run the route's window allows; the next may start in `retryAfter`
    /// seconds.
    TooMany { retryAfter: u64 },
}

/// A user's run in progress; the user may start another once it is dropped.
pub(crate) struct Run {
    running: Arc<DashMap<String, ()>>,
    user: String,
}

impl Drop for Run {
    fn drop(&mut self) {
        self.running.remove(&self.user);
    }
}

impl SimulatorGuard {
    pub fn new(runsPerMinute: u32) -> Self {
        Self {
            runsPerMinute,
            running: Arc::default(),
            windows: DashMap::new(),
            cache: Mutex::default(),
        }
    }

    /// `SIMULATOR_RUNS_PER_MINUTE` (default 8, 0 for no limit).
    pub fn fromEnv() -> Self {
        let runsPerMinute = std::env::var("SIMULATOR_RUNS_PER_MINUTE")
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(DEFAULT_RUNS_PER_MINUTE);
        Self::new(runsPerMinute)
    }

    /// Starts `user`'s run on `path` unless one is still going or the runs `limit` allows in its
    /// window are used up. With `SIMULATOR_RUNS_PER_MINUTE` 0 no route has a window.
    pub(crate) fn startRun(
        &self,
        user: &str,
        path: &str,
        limit: RouteLimit,
    ) -> Result<Run, Refused> {
        match self.running.entry(user.to_owned()) {
            Entry::Occupied(_) => return Err(Refused::Running),
            Entry::Vacant(slot) => {
                slot.insert(());
            }
        }

        let run = Run {
            running: self.running.clone(),
            user: user.to_owned(),
        };
        if self.runsPerMinute == 0 {
            return Ok(run);
        }

        let (allowed, length) = match limit {
            RouteLimit::PerMinute(share) => (self.runsPerMinute.saturating_mul(share), RUN_WINDOW),
            RouteLimit::Fixed { runs, window } => (runs, window),
        };
        let now = Instant::now();
        if self.windows.len() > 10_000 {
            self.windows.retain(|_, window| window.resetAt > now);
        }

        let mut window = self
            .windows
            .entry(format!("{user} {path}"))
            .or_insert(RunWindow {
                runs: 0,
                resetAt: now + length,
            });
        if now >= window.resetAt {
            window.runs = 0;
            window.resetAt = now + length;
        }

        if window.runs >= allowed {
            let retryAfter = window
                .resetAt
                .saturating_duration_since(now)
                .as_secs()
                .max(1);
            return Err(Refused::TooMany { retryAfter });
        }

        window.runs += 1;
        Ok(run)
    }

    /// A cached answer to the request `key` names, while it is fresh.
    pub(crate) fn cached(&self, key: &CacheKey) -> Option<(HeaderMap, Bytes)> {
        let mut cache = self.cache.lock().unwrap();
        let fresh = cache.entries.get(key)?.storedAt.elapsed() < CACHE_TTL;
        if !fresh {
            let gone = cache.entries.remove(key).unwrap();
            cache.bytes -= gone.body.len();
            return None;
        }

        let entry = &cache.entries[key];
        Some((entry.headers.clone(), entry.body.clone()))
    }

    /// Keeps a successful answer, dropping stale answers and then the oldest to stay in bounds.
    pub(crate) fn store(&self, key: CacheKey, headers: &HeaderMap, body: Bytes) {
        if body.len() > CACHE_ENTRY_BYTES {
            return;
        }

        let mut headers = headers.clone();
        headers.remove("server-timing");
        let mut cache = self.cache.lock().unwrap();
        let cache = &mut *cache;
        cache.entries.remove(&key);
        cache
            .entries
            .retain(|_, entry| entry.storedAt.elapsed() < CACHE_TTL);
        cache.bytes = cache.entries.values().map(|entry| entry.body.len()).sum();
        while cache.bytes + body.len() > CACHE_BYTES {
            let Some(oldest) = cache
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.storedAt)
                .map(|(key, _)| *key)
            else {
                break;
            };

            let gone = cache.entries.remove(&oldest).unwrap();
            cache.bytes -= gone.body.len();
        }

        cache.bytes += body.len();
        cache.entries.insert(
            key,
            Cached {
                headers,
                body,
                storedAt: Instant::now(),
            },
        );
    }
}

/// How many runs a website user may start on `path`; None when the route is not limited at all.
pub(crate) fn routeLimit(path: &str) -> Option<RouteLimit> {
    match path {
        // A resimulation replays one race, and a session replays its races one after another.
        "/api/sim/race" => None,
        // Skill impact compares 4 skills a request and team search sends its races in batches, so
        // one analysis sends several.
        "/api/sim/comparison" | "/api/sim/batch" => Some(RouteLimit::PerMinute(4)),
        "/api/sim/skill-explorer-build" => Some(RouteLimit::Fixed {
            runs: EXPLORER_BUILD_RUNS,
            window: EXPLORER_BUILD_WINDOW,
        }),
        // Drawing a field fetches a published CM room and adapts its builds: a request a click.
        "/api/sim/field" => Some(RouteLimit::PerMinute(1)),
        // Monte Carlo, optimize and stamina: a request a click.
        _ => Some(RouteLimit::PerMinute(1)),
    }
}

/// Names a request by its route, the representation it negotiates and its body. A JSON body is
/// read as JSON, so key order and whitespace do not matter; anything else counts byte for byte.
pub(crate) fn cacheKey(path: &str, headers: &HeaderMap, body: &[u8]) -> CacheKey {
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    for name in [header::ACCEPT, header::ACCEPT_ENCODING] {
        hasher.update([0]);
        for value in headers.get_all(&name) {
            hasher.update(value.as_bytes());
            hasher.update([b',']);
        }
    }

    hasher.update([0]);
    match serde_json::from_slice::<Value>(body) {
        Ok(value) => {
            let mut canonical = Vec::with_capacity(body.len());
            writeCanonical(&value, &mut canonical);
            hasher.update(&canonical);
        }
        Err(_) => hasher.update(body),
    }

    hasher.finalize().into()
}

/// `value` as JSON with every object's keys sorted.
fn writeCanonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }

                serde_json::to_writer(&mut *out, key).expect("JSON string");
                out.push(b':');
                writeCanonical(&map[key], out);
            }

            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }

                writeCanonical(item, out);
            }

            out.push(b']');
        }
        other => serde_json::to_writer(&mut *out, other).expect("JSON value"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    const ONCE: RouteLimit = RouteLimit::PerMinute(1);
    const FOUR: RouteLimit = RouteLimit::PerMinute(4);

    #[test]
    fn eachUserRunsOneAtATimeAndAFewAMinute() {
        const OPTIMIZE: &str = "/api/sim/optimize";
        let guard = SimulatorGuard::new(2);
        let first = guard.startRun("ip:1", OPTIMIZE, ONCE).unwrap();
        assert_eq!(
            guard.startRun("ip:1", "/api/sim/stamina", ONCE).err(),
            Some(Refused::Running)
        );
        let other = guard.startRun("ip:2", OPTIMIZE, ONCE).unwrap();
        drop(first);

        let second = guard.startRun("ip:1", OPTIMIZE, ONCE).unwrap();
        drop(second);
        match guard.startRun("ip:1", OPTIMIZE, ONCE).err() {
            Some(Refused::TooMany { retryAfter }) => assert!((1..=60).contains(&retryAfter)),
            refused => panic!("expected the minute's limit, got {refused:?}"),
        }

        // A refused run holds no slot; each route and each user has its own minute.
        assert!(matches!(
            guard.startRun("ip:1", OPTIMIZE, ONCE),
            Err(Refused::TooMany { .. })
        ));
        for _ in 0..8 {
            assert!(guard.startRun("ip:1", "/api/sim/comparison", FOUR).is_ok());
        }

        assert!(guard.startRun("ip:1", "/api/sim/comparison", FOUR).is_err());
        drop(other);
        assert!(guard.startRun("ip:2", OPTIMIZE, ONCE).is_ok());
        assert!(SimulatorGuard::new(0)
            .startRun("ip:1", OPTIMIZE, ONCE)
            .is_ok());
        assert_eq!(routeLimit("/api/sim/race"), None);
        assert_eq!(routeLimit("/api/sim/batch"), Some(FOUR));
        assert_eq!(routeLimit(OPTIMIZE), Some(ONCE));
        assert_eq!(routeLimit("/api/sim/field"), Some(ONCE));
    }

    #[test]
    fn explorerBuildsAllowAFewEveryTenMinutes() {
        const BUILD: &str = "/api/sim/skill-explorer-build";
        let guard = SimulatorGuard::new(100);
        let limit = routeLimit(BUILD).unwrap();
        for _ in 0..EXPLORER_BUILD_RUNS {
            drop(guard.startRun("ip:1", BUILD, limit).unwrap());
        }

        match guard.startRun("ip:1", BUILD, limit).err() {
            Some(Refused::TooMany { retryAfter }) => assert!((61..=600).contains(&retryAfter)),
            refused => panic!("expected the build limit, got {refused:?}"),
        }

        // The other routes keep their own minute.
        assert!(guard.startRun("ip:1", "/api/sim/optimize", ONCE).is_ok());
    }

    #[test]
    fn sameJsonInAnyOrderSharesAnAnswer() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
        let key =
            |path: &str, headers: &HeaderMap, body: &str| cacheKey(path, headers, body.as_bytes());
        let base = key(
            "/api/sim/optimize",
            &headers,
            r#"{"seed":1,"mode":"cm","runs":[1,2]}"#,
        );

        assert_eq!(
            base,
            key(
                "/api/sim/optimize",
                &headers,
                "{ \"mode\": \"cm\", \"runs\": [1, 2], \"seed\": 1 }"
            )
        );
        assert_ne!(
            base,
            key(
                "/api/sim/optimize",
                &headers,
                r#"{"seed":2,"mode":"cm","runs":[1,2]}"#
            )
        );
        assert_ne!(
            base,
            key(
                "/api/sim/optimize",
                &headers,
                r#"{"seed":1,"mode":"cm","runs":[2,1]}"#
            )
        );
        assert_ne!(
            base,
            key(
                "/api/sim/race",
                &headers,
                r#"{"seed":1,"mode":"cm","runs":[1,2]}"#
            )
        );
        assert_ne!(
            base,
            key(
                "/api/sim/optimize",
                &HeaderMap::new(),
                r#"{"seed":1,"mode":"cm","runs":[1,2]}"#
            )
        );

        let guard = SimulatorGuard::new(1);
        assert!(guard.cached(&base).is_none());
        let mut stored = headers.clone();
        stored.insert("server-timing", HeaderValue::from_static("compute;dur=9"));
        guard.store(base, &stored, Bytes::from_static(b"{\"ok\":true}"));
        let (cachedHeaders, body) = guard.cached(&base).unwrap();
        assert_eq!(body, "{\"ok\":true}");
        assert!(!cachedHeaders.contains_key("server-timing"));
        guard.store(
            [7; 32],
            &headers,
            Bytes::from(vec![0; CACHE_ENTRY_BYTES + 1]),
        );
        assert!(guard.cached(&[7; 32]).is_none());
    }
}
