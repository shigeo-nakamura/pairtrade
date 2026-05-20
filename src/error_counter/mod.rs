//! Error / warn counter layered over an inner `log::Log`.
//!
//! Counters are kept in per-instance buckets, attributed via the
//! `CurrentInstance` thread-local that the pairtrade engine installs
//! around `step_for_instance`. Each variant's `status.json` reads its
//! own bucket merged with the shared (None) bucket — events emitted
//! outside any instance scope (connector WS resets, account refresh
//! failures, …) land in the shared bucket and surface on every
//! variant, while variant-specific events (e.g. `[SESSION_DD] c …`)
//! only inflate the offending variant's counters. See bot-strategy#367
//! for the leak this replaces.
//!
//! `pending_ws`, `pending_step_overrun`, and `ws_resets_24h` stay
//! process-global: WS recovery / order-completion markers are shared
//! signals that drain queues regardless of which variant logged the
//! deferred entry, and `ws_reset_24h_count` is a fleet-level health
//! readout that doesn't fragment per variant.

mod classification;
mod deferral;

use log::{Level, Log, Metadata, Record};
use serde::Serialize;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use classification::{
    is_step_overrun_event, is_step_overrun_recovery_event, is_ws_recovery_event,
    is_ws_transient_event, WS_RESET_PHRASE,
};
use deferral::{
    flush_all_expired_pending, PendingEntry, STEP_OVERRUN_DEFER_WINDOW_SECS, WS_DEFER_WINDOW_SECS,
};

/// Process-global counter handle, populated by the binary's logger
/// initialization. Library code (e.g. `StatusReporter`) reads it to
/// include an `error_summary` section in `status.json`.
static GLOBAL_HANDLE: OnceLock<ErrorCounterHandle> = OnceLock::new();

/// When true, `ErrorCountingLogger` stops incrementing warn/error counters
/// and stops updating `last_error`/`last_warn`. Set from the pairtrade loop
/// whenever an upcoming-maintenance window is detected on the DEX connector
/// (see bot-strategy#199): WS reconnect bursts, 503s and stale-price WARNs
/// during a pre-announced outage are expected side effects, not actionable
/// signal, and should not inflate `error_summary` in `status.json`. Log
/// emission to the inner logger (journalctl) is unaffected.
static SUPPRESS_COUNTING: AtomicBool = AtomicBool::new(false);

pub fn install_global(handle: ErrorCounterHandle) {
    let _ = GLOBAL_HANDLE.set(handle);
}

pub fn global() -> Option<&'static ErrorCounterHandle> {
    GLOBAL_HANDLE.get()
}

pub fn set_counting_suppressed(suppressed: bool) {
    SUPPRESS_COUNTING.store(suppressed, Ordering::Relaxed);
}

pub fn is_counting_suppressed() -> bool {
    SUPPRESS_COUNTING.load(Ordering::Relaxed)
}

thread_local! {
    /// Current variant id whose code path is executing on this thread.
    /// Set by `CurrentInstanceGuard::enter` around `step_for_instance`
    /// and read by `ErrorCountingLogger::log` to attribute the record.
    /// `None` means the log line originated outside any instance scope
    /// (connector callbacks, startup, status writes between instances).
    static CURRENT_INSTANCE: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// RAII guard that installs `id` as the current instance for this
/// thread, restoring the previous value on drop. Pairtrade wraps the
/// body of `step_for_instance` with one of these so every log line
/// emitted by per-instance code paths (session DD, daily DD, equity
/// fetch, position sync, …) lands in that variant's bucket.
pub struct CurrentInstanceGuard {
    prev: Option<String>,
}

impl CurrentInstanceGuard {
    pub fn enter(id: &str) -> Self {
        let prev = CURRENT_INSTANCE.with(|c| {
            let mut slot = c.borrow_mut();
            std::mem::replace(&mut *slot, Some(id.to_string()))
        });
        Self { prev }
    }
}

impl Drop for CurrentInstanceGuard {
    fn drop(&mut self) {
        let prev = self.prev.take();
        CURRENT_INSTANCE.with(|c| {
            *c.borrow_mut() = prev;
        });
    }
}

fn current_instance() -> Option<String> {
    CURRENT_INSTANCE.with(|c| c.borrow().clone())
}

/// Window (seconds) for the short-term rolling counts published in the
/// status snapshot. Was 300 until bot-strategy#168: GitHub Actions scheduled
/// runs drift 40–70 min under load so a 5-min window let warns age out
/// between polls. 1800 (30 min) absorbs typical drift.
const ROLLING_WINDOW_SECS: i64 = 1800;

/// 24h window for the ws-reset counter. Replaces the dashboard's
/// `journalctl ... | awk '/Connection reset.../ {c++}'` SSM probe with
/// a self-reported field in `status.json` (bot-strategy#343). Threshold
/// for alerting is 10/day per #47.
const WS_RESET_24H_WINDOW_SECS: i64 = 24 * 60 * 60;

/// Keep the last error message truncated to this many chars so the
/// dashboard can display it without blowing up the JSON payload.
const LAST_ERROR_MAX_CHARS: usize = 200;

#[derive(Debug, Clone, Serialize)]
pub struct ErrorSummary {
    pub error_count_30m: u64,
    pub warn_count_30m: u64,
    pub error_count_total: u64,
    pub warn_count_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_warn_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_warn_message: Option<String>,
}

/// Per-instance committed state. One bucket per variant id, plus a
/// shared `None`-keyed bucket for unattributed events.
struct Bucket {
    recent: Mutex<VecDeque<(i64, Level)>>,
    last_error: Mutex<Option<(i64, String)>>,
    last_warn: Mutex<Option<(i64, String)>>,
    error_total: AtomicU64,
    warn_total: AtomicU64,
}

impl Bucket {
    fn new() -> Self {
        Self {
            recent: Mutex::new(VecDeque::new()),
            last_error: Mutex::new(None),
            last_warn: Mutex::new(None),
            error_total: AtomicU64::new(0),
            warn_total: AtomicU64::new(0),
        }
    }
}

/// Top-level counter state. Holds the per-instance committed buckets
/// keyed on `Option<String>` (None = shared/unattributed) plus the
/// process-global defer queues and ws-reset ring.
struct Counters {
    /// Per-instance committed state. Lazily inserted on first matching
    /// log record so we don't need to know variant ids at install time.
    buckets: Mutex<HashMap<Option<String>, Arc<Bucket>>>,
    /// Transient WS-reset events queued for deferred commit. Each entry
    /// stays here until either (a) a recovery log line drains it before
    /// `WS_DEFER_WINDOW_SECS` elapses, or (b) `snapshot()` flushes it
    /// into the captured-instance's bucket once its deadline passes.
    /// See bot-strategy#261.
    pending_ws: Mutex<VecDeque<PendingEntry>>,
    /// `[STEP_OVERRUN]` warns queued for deferred commit. Drained by
    /// `[ORDER] ... entry/exit orders filled` recovery markers within
    /// `STEP_OVERRUN_DEFER_WINDOW_SECS`. See bot-strategy#267.
    pending_step_overrun: Mutex<VecDeque<PendingEntry>>,
    /// Timestamps (epoch seconds) of WS reset events in the last 24h —
    /// any log line that contains `Connection reset without closing
    /// handshake`. Surfaced as `ws_reset_24h_count` in `status.json` so
    /// the dashboard does not need a journalctl SSM probe. Stays
    /// process-global because the metric represents fleet-level WS
    /// health, not per-variant attribution. See bot-strategy#343.
    ws_resets_24h: Mutex<VecDeque<i64>>,
}

impl Counters {
    fn new() -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            pending_ws: Mutex::new(VecDeque::new()),
            pending_step_overrun: Mutex::new(VecDeque::new()),
            ws_resets_24h: Mutex::new(VecDeque::new()),
        }
    }

    fn bucket(&self, instance: Option<&str>) -> Arc<Bucket> {
        let key = instance.map(|s| s.to_string());
        let mut map = self.buckets.lock().unwrap();
        Arc::clone(map.entry(key).or_insert_with(|| Arc::new(Bucket::new())))
    }
}

#[derive(Clone)]
pub struct ErrorCounterHandle {
    counters: Arc<Counters>,
}

impl ErrorCounterHandle {
    /// Read the 24h ws-reset count without mutating any other counter.
    /// Used by `StatusReporter` to populate `ws_reset_24h_count`
    /// (bot-strategy#343). Prunes expired timestamps as a side effect
    /// so the snapshot stays bounded.
    pub fn ws_reset_24h_count(&self) -> u64 {
        let now = chrono::Utc::now().timestamp();
        let cutoff = now - WS_RESET_24H_WINDOW_SECS;
        let mut q = self.counters.ws_resets_24h.lock().unwrap();
        while let Some(&front) = q.front() {
            if front < cutoff {
                q.pop_front();
            } else {
                break;
            }
        }
        q.len() as u64
    }

    /// Snapshot view merged across every bucket. Retained for callers
    /// that have no variant context (or want a fleet-wide rollup).
    pub fn snapshot(&self) -> ErrorSummary {
        let now = chrono::Utc::now().timestamp();
        flush_all_expired_pending(&self.counters, now);
        let cutoff = now - ROLLING_WINDOW_SECS;
        let buckets: Vec<Arc<Bucket>> = self
            .counters
            .buckets
            .lock()
            .unwrap()
            .values()
            .map(Arc::clone)
            .collect();
        merge_buckets(&buckets, cutoff)
    }

    /// Snapshot view for a specific variant. Merges the shared (None)
    /// bucket with the `instance` bucket so connector-layer events
    /// (WS reset, startup) still show up on every variant's status,
    /// while variant-specific events (SESSION_DD, daily DD, …) only
    /// inflate the bucket that emitted them. See bot-strategy#367.
    pub fn snapshot_for(&self, instance: Option<&str>) -> ErrorSummary {
        let now = chrono::Utc::now().timestamp();
        flush_all_expired_pending(&self.counters, now);
        let cutoff = now - ROLLING_WINDOW_SECS;
        let key = instance.map(|s| s.to_string());
        let mut buckets: Vec<Arc<Bucket>> = Vec::with_capacity(2);
        let map = self.counters.buckets.lock().unwrap();
        if let Some(b) = map.get(&None) {
            buckets.push(Arc::clone(b));
        }
        if key.is_some() {
            if let Some(b) = map.get(&key) {
                buckets.push(Arc::clone(b));
            }
        }
        drop(map);
        merge_buckets(&buckets, cutoff)
    }
}

/// Build an `ErrorSummary` over the supplied buckets, honouring the
/// 30-minute rolling window for `*_count_30m`. Totals are summed.
/// `last_error` / `last_warn` pick the most recent across buckets.
fn merge_buckets(buckets: &[Arc<Bucket>], cutoff: i64) -> ErrorSummary {
    let mut err_window = 0u64;
    let mut warn_window = 0u64;
    let mut error_total = 0u64;
    let mut warn_total = 0u64;
    let mut last_error: Option<(i64, String)> = None;
    let mut last_warn: Option<(i64, String)> = None;
    for b in buckets {
        let mut recent = b.recent.lock().unwrap();
        while let Some(&(ts, _)) = recent.front() {
            if ts < cutoff {
                recent.pop_front();
            } else {
                break;
            }
        }
        for (_, lvl) in recent.iter() {
            match lvl {
                Level::Error => err_window += 1,
                Level::Warn => warn_window += 1,
                _ => {}
            }
        }
        drop(recent);
        error_total += b.error_total.load(Ordering::Relaxed);
        warn_total += b.warn_total.load(Ordering::Relaxed);
        if let Some(entry) = b.last_error.lock().unwrap().clone() {
            if last_error.as_ref().map_or(true, |cur| entry.0 > cur.0) {
                last_error = Some(entry);
            }
        }
        if let Some(entry) = b.last_warn.lock().unwrap().clone() {
            if last_warn.as_ref().map_or(true, |cur| entry.0 > cur.0) {
                last_warn = Some(entry);
            }
        }
    }
    let (last_error_ts, last_error_message) = match last_error {
        Some((ts, msg)) => (Some(ts), Some(msg)),
        None => (None, None),
    };
    let (last_warn_ts, last_warn_message) = match last_warn {
        Some((ts, msg)) => (Some(ts), Some(msg)),
        None => (None, None),
    };
    ErrorSummary {
        error_count_30m: err_window,
        warn_count_30m: warn_window,
        error_count_total: error_total,
        warn_count_total: warn_total,
        last_error_ts,
        last_error_message,
        last_warn_ts,
        last_warn_message,
    }
}

pub struct ErrorCountingLogger {
    counters: Arc<Counters>,
    inner: Box<dyn Log>,
}

impl ErrorCountingLogger {
    pub fn wrap(inner: Box<dyn Log>) -> (Self, ErrorCounterHandle) {
        let counters = Arc::new(Counters::new());
        let handle = ErrorCounterHandle {
            counters: Arc::clone(&counters),
        };
        (Self { counters, inner }, handle)
    }
}

/// Commit a single record into the bucket identified by `instance`.
/// Used both by the live log path and by the deferred-pending flush.
fn commit_to_bucket(
    counters: &Counters,
    instance: Option<&str>,
    ts: i64,
    level: Level,
    msg: String,
) {
    let bucket = counters.bucket(instance);
    bucket.recent.lock().unwrap().push_back((ts, level));
    match level {
        Level::Error => {
            bucket.error_total.fetch_add(1, Ordering::Relaxed);
            *bucket.last_error.lock().unwrap() = Some((ts, msg));
        }
        Level::Warn => {
            bucket.warn_total.fetch_add(1, Ordering::Relaxed);
            *bucket.last_warn.lock().unwrap() = Some((ts, msg));
        }
        _ => {}
    }
}

impl Log for ErrorCountingLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let ts = chrono::Utc::now().timestamp();
            let msg = record.args().to_string();
            // 24h ws-reset counter (#343). Tracked independently of the
            // pending-WS defer machinery so the dashboard sees the same
            // raw count the journalctl probe used to produce. Always
            // counts, regardless of `is_counting_suppressed`, because
            // maintenance suppression should not hide reset volume from
            // the dashboard.
            if msg.contains(WS_RESET_PHRASE) {
                let cutoff = ts - WS_RESET_24H_WINDOW_SECS;
                let mut q = self.counters.ws_resets_24h.lock().unwrap();
                while let Some(&front) = q.front() {
                    if front < cutoff {
                        q.pop_front();
                    } else {
                        break;
                    }
                }
                q.push_back(ts);
            }
            // Recovery markers fire at INFO; check before the level gate so
            // they can drain pending entries from any preceding transient.
            // Recovery drains across all instances — the connector layer is
            // shared, and a fresh WS connection invalidates anyone's pending
            // transient regardless of which variant noticed it.
            if is_ws_recovery_event(&msg) {
                let cutoff = ts - WS_DEFER_WINDOW_SECS;
                self.counters
                    .pending_ws
                    .lock()
                    .unwrap()
                    .retain(|e| e.ts < cutoff);
            }
            if is_step_overrun_recovery_event(&msg) {
                let cutoff = ts - STEP_OVERRUN_DEFER_WINDOW_SECS;
                self.counters
                    .pending_step_overrun
                    .lock()
                    .unwrap()
                    .retain(|e| e.ts < cutoff);
            }
            // Always flush any pending entries whose deadline passed before
            // we count anything new — keeps the counter monotone in real
            // time even when snapshot() isn't being called (e.g. dashboard
            // poll lag).
            flush_all_expired_pending(&self.counters, ts);
            let level = record.level();
            if (level == Level::Error || level == Level::Warn) && !is_counting_suppressed() {
                let truncated = if msg.chars().count() > LAST_ERROR_MAX_CHARS {
                    msg.chars().take(LAST_ERROR_MAX_CHARS).collect::<String>() + "…"
                } else {
                    msg
                };
                let instance = current_instance();
                if is_ws_transient_event(&truncated) {
                    self.counters
                        .pending_ws
                        .lock()
                        .unwrap()
                        .push_back(PendingEntry {
                            ts,
                            level,
                            message: truncated,
                            instance,
                        });
                } else if is_step_overrun_event(&truncated) {
                    self.counters
                        .pending_step_overrun
                        .lock()
                        .unwrap()
                        .push_back(PendingEntry {
                            ts,
                            level,
                            message: truncated,
                            instance,
                        });
                } else {
                    commit_to_bucket(&self.counters, instance.as_deref(), ts, level, truncated);
                }
            }
        }
        self.inner.log(record);
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn make_counters() -> Arc<Counters> {
        Arc::new(Counters::new())
    }

    /// Test-only: simulate `log()` without going through the real `log!()`
    /// macro / global logger, so each test controls timestamps, pattern
    /// matching, and instance attribution directly. Mirrors the real
    /// `log()` body. `instance = None` matches the shared / unattributed
    /// path the connector layer takes today.
    fn fake_log_for(counters: &Counters, instance: Option<&str>, ts: i64, level: Level, msg: &str) {
        if msg.contains(WS_RESET_PHRASE) {
            let cutoff = ts - WS_RESET_24H_WINDOW_SECS;
            let mut q = counters.ws_resets_24h.lock().unwrap();
            while let Some(&front) = q.front() {
                if front < cutoff {
                    q.pop_front();
                } else {
                    break;
                }
            }
            q.push_back(ts);
        }
        if is_ws_recovery_event(msg) {
            let cutoff = ts - WS_DEFER_WINDOW_SECS;
            counters
                .pending_ws
                .lock()
                .unwrap()
                .retain(|e| e.ts < cutoff);
        }
        if is_step_overrun_recovery_event(msg) {
            let cutoff = ts - STEP_OVERRUN_DEFER_WINDOW_SECS;
            counters
                .pending_step_overrun
                .lock()
                .unwrap()
                .retain(|e| e.ts < cutoff);
        }
        flush_all_expired_pending(counters, ts);
        if level != Level::Error && level != Level::Warn {
            return;
        }
        if is_counting_suppressed() {
            return;
        }
        let truncated = msg.to_string();
        let instance_owned = instance.map(|s| s.to_string());
        if is_ws_transient_event(&truncated) {
            counters.pending_ws.lock().unwrap().push_back(PendingEntry {
                ts,
                level,
                message: truncated,
                instance: instance_owned,
            });
        } else if is_step_overrun_event(&truncated) {
            counters
                .pending_step_overrun
                .lock()
                .unwrap()
                .push_back(PendingEntry {
                    ts,
                    level,
                    message: truncated,
                    instance: instance_owned,
                });
        } else {
            commit_to_bucket(counters, instance, ts, level, truncated);
        }
    }

    fn fake_log(counters: &Counters, ts: i64, level: Level, msg: &str) {
        fake_log_for(counters, None, ts, level, msg)
    }

    /// Count over all buckets — mirrors the legacy `snapshot()` view used
    /// by the existing tests, which never cared about per-instance
    /// attribution.
    fn snap_counts(counters: &Counters, now: i64) -> (u64, u64) {
        flush_all_expired_pending(counters, now);
        let cutoff = now - ROLLING_WINDOW_SECS;
        let buckets: Vec<Arc<Bucket>> = counters
            .buckets
            .lock()
            .unwrap()
            .values()
            .map(Arc::clone)
            .collect();
        let s = merge_buckets(&buckets, cutoff);
        (s.error_count_30m, s.warn_count_30m)
    }

    /// Count for a specific instance bucket — used by the per-instance
    /// attribution tests below.
    fn snap_counts_for(counters: &Counters, instance: Option<&str>, now: i64) -> (u64, u64) {
        flush_all_expired_pending(counters, now);
        let cutoff = now - ROLLING_WINDOW_SECS;
        let key = instance.map(|s| s.to_string());
        let mut buckets: Vec<Arc<Bucket>> = Vec::new();
        let map = counters.buckets.lock().unwrap();
        if let Some(b) = map.get(&None) {
            buckets.push(Arc::clone(b));
        }
        if key.is_some() {
            if let Some(b) = map.get(&key) {
                buckets.push(Arc::clone(b));
            }
        }
        drop(map);
        let s = merge_buckets(&buckets, cutoff);
        (s.error_count_30m, s.warn_count_30m)
    }

    // bot-strategy#261: WS reset that auto-recovers within
    // WS_DEFER_WINDOW_SECS must NOT inflate the rolling counter (#260
    // was the trigger case — single Lighter WS RST produced 2 ERROR + 2
    // WARN, tripping auto-error workflow despite 27s clean reconnect).

    /// Order matters here — these tests share the static
    /// `SUPPRESS_COUNTING` flag with the rest of the suite. Take an
    /// internal lock so they don't interleave. Recover from poisoning
    /// so a panic in one test doesn't cascade-fail the rest.
    fn _serialize() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        let m = LOCK.get_or_init(|| StdMutex::new(()));
        match m.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[test]
    fn ws_reset_with_recovery_within_60s_is_suppressed() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 1_000_000;
        // Simulate the #260 sequence verbatim (~27s window).
        fake_log(
            &c,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer (os error 104)",
        );
        fake_log(
            &c,
            t0,
            Level::Error,
            "WebSocket IO error detail: kind=ConnectionReset, error=Connection reset by peer",
        );
        fake_log(&c, t0 + 18, Level::Warn, "[XVENUE] tick error: read_mid Lighter\n\nCaused by:\n    get_order_book(ETH, 1): Other(\"order book snapshot unavailable (no recent update)\")");
        fake_log(
            &c,
            t0 + 23,
            Level::Warn,
            "[XVENUE] tick error: read_mid Lighter",
        );
        fake_log(
            &c,
            t0 + 27,
            Level::Info,
            "WebSocket connected successfully: ...",
        );
        fake_log(
            &c,
            t0 + 27,
            Level::Info,
            "WebSocket subscriptions sent successfully",
        );
        // After recovery, the pending queue should be empty.
        assert!(
            c.pending_ws.lock().unwrap().is_empty(),
            "recovery within window must drain pending WS entries"
        );
        // And the durable counter sees zero contribution.
        let (e, w) = snap_counts(&c, t0 + 30);
        assert_eq!(e, 0, "transient WS errors must not commit");
        assert_eq!(w, 0, "transient WS warns must not commit");
    }

    #[test]
    fn ws_reset_without_recovery_commits_after_deadline() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 2_000_000;
        fake_log(
            &c,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer (os error 104)",
        );
        fake_log(
            &c,
            t0 + 5,
            Level::Warn,
            "[XVENUE] tick error: read_mid Lighter",
        );
        // Sample BEFORE either deadline — neither WS-typed event has aged
        // out of its individual recovery window. Counter still zero.
        let (e0, w0) = snap_counts(&c, t0 + 30);
        assert_eq!((e0, w0), (0, 0), "pre-deadline must not commit");
        // Sample after BOTH deadlines (ERROR @ t0, WARN @ t0+5; window=60).
        // Use t0+70 to be safely past both.
        let (e1, w1) = snap_counts(&c, t0 + WS_DEFER_WINDOW_SECS + 10);
        assert_eq!(e1, 1, "post-deadline ERROR commits");
        assert_eq!(w1, 1, "post-deadline WARN commits");
    }

    #[test]
    fn ws_reset_with_late_recovery_does_not_uncommit() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 3_000_000;
        fake_log(
            &c,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer",
        );
        // No recovery within window — flush via a snapshot past the deadline
        // promotes the entry to `recent`.
        let (e0, _) = snap_counts(&c, t0 + WS_DEFER_WINDOW_SECS + 1);
        assert_eq!(e0, 1, "post-deadline ERROR commits");
        // A late recovery marker must NOT retroactively decrement the
        // already-committed counter — the `recent` queue is durable.
        fake_log(
            &c,
            t0 + 120,
            Level::Info,
            "WebSocket connected successfully: ...",
        );
        let (e1, _) = snap_counts(&c, t0 + 130);
        assert_eq!(e1, 1, "late recovery cannot uncommit");
    }

    #[test]
    fn non_ws_error_commits_immediately() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 4_000_000;
        fake_log(&c, t0, Level::Error, "Some other ERROR unrelated to WS");
        let (e, _) = snap_counts(&c, t0 + 1);
        assert_eq!(e, 1, "non-WS errors must not be deferred");
    }

    #[test]
    fn maintenance_suppression_overrides_defer_path() {
        let _g = _serialize();
        let c = make_counters();
        set_counting_suppressed(true);
        let t0 = 5_000_000;
        fake_log(
            &c,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer",
        );
        fake_log(
            &c,
            t0 + 5,
            Level::Warn,
            "[XVENUE] tick error: read_mid Lighter",
        );
        // Maintenance mode short-circuits before either path runs.
        assert!(
            c.pending_ws.lock().unwrap().is_empty(),
            "maintenance suppression must not enqueue pending entries"
        );
        let (e, w) = snap_counts(&c, t0 + 10);
        assert_eq!((e, w), (0, 0));
        set_counting_suppressed(false);
    }

    #[test]
    fn ws_warn_orderbook_unavailable_is_deferred() {
        // pairtrade Lighter sees this WARN form during reconnect (not
        // xvenue-arb's "tick error: read_mid"). Same defer + recovery
        // semantics required.
        let _g = _serialize();
        let c = make_counters();
        let t0 = 6_000_000;
        fake_log(
            &c,
            t0,
            Level::Error,
            "WebSocket error: IO error: Connection reset",
        );
        fake_log(
            &c,
            t0 + 5,
            Level::Warn,
            "orderbook BTC/ETH unavailable: waiting for websocket data",
        );
        fake_log(&c, t0 + 10, Level::Info, "WebSocket connected successfully");
        let (e, w) = snap_counts(&c, t0 + 15);
        assert_eq!((e, w), (0, 0), "pairtrade orderbook WARN suppressed too");
    }

    #[test]
    fn extended_ws_reset_three_streams_drained_by_first_recovery() {
        // bot-strategy#261 / #301 (Tokyo Extended 2026-05-03 12:36:58 UTC):
        // a single Extended WS reset surfaces as three concurrent WARN
        // lines (orderbook BTC, public trades BTC, orderbook ETH). The
        // dex-connector reconnect is silent until the new
        // `WebSocket connected successfully (stream=...)` info line is
        // emitted; once it arrives the entire pending pool drains.
        let _g = _serialize();
        let c = make_counters();
        let t0 = 8_000_000;
        fake_log(
            &c,
            t0,
            Level::Warn,
            "orderbook stream error: Other error: ws error: WebSocket protocol error: Connection reset without closing handshake (stream=orderbook symbol=BTC ...)",
        );
        fake_log(
            &c,
            t0,
            Level::Warn,
            "public trades stream error: Other error: ws error: WebSocket protocol error: Connection reset without closing handshake (stream=trades symbol=BTC ...)",
        );
        fake_log(
            &c,
            t0,
            Level::Warn,
            "orderbook stream error: Other error: ws error: WebSocket protocol error: Connection reset without closing handshake (stream=orderbook symbol=ETH ...)",
        );
        fake_log(
            &c,
            t0 + 2,
            Level::Info,
            "WebSocket connected successfully (stream=orderbook symbol=BTC ...)",
        );
        assert!(
            c.pending_ws.lock().unwrap().is_empty(),
            "first recovery must drain all three pending Extended stream errors"
        );
        let (e, w) = snap_counts(&c, t0 + 5);
        assert_eq!((e, w), (0, 0), "Extended WS reset triplet must commit zero");
    }

    #[test]
    fn extended_account_stream_error_is_deferred() {
        // The account stream error has a different prefix from the
        // public ones; verify it still rides the same defer machinery.
        let _g = _serialize();
        let c = make_counters();
        let t0 = 8_500_000;
        fake_log(
            &c,
            t0,
            Level::Warn,
            "account stream error: ws error: WebSocket protocol error: Connection reset without closing handshake",
        );
        fake_log(
            &c,
            t0 + 3,
            Level::Info,
            "WebSocket connected successfully (stream=account ...)",
        );
        let (e, w) = snap_counts(&c, t0 + 5);
        assert_eq!(
            (e, w),
            (0, 0),
            "account stream WARN must suppress on recovery"
        );
    }

    #[test]
    fn extended_ws_reset_without_recovery_commits_after_deadline() {
        // If reconnect never lands within WS_DEFER_WINDOW_SECS, the
        // Extended stream-error WARNs must surface so a real persistent
        // disconnect still reaches the auto-error workflow.
        let _g = _serialize();
        let c = make_counters();
        let t0 = 9_000_000;
        fake_log(
            &c,
            t0,
            Level::Warn,
            "orderbook stream error: Other error: ws error: WebSocket protocol error: Connection reset (stream=orderbook symbol=BTC ...)",
        );
        let (_, w) = snap_counts(&c, t0 + WS_DEFER_WINDOW_SECS + 5);
        assert_eq!(w, 1, "expired Extended transient must commit");
    }

    // bot-strategy#267: STEP_OVERRUN warn during normal partial-fill
    // chain must NOT inflate the rolling counter once the matching
    // `[ORDER] ... orders filled` recovery log lands within
    // STEP_OVERRUN_DEFER_WINDOW_SECS.

    #[test]
    fn step_overrun_with_entry_completion_is_suppressed() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 7_000_000;
        // Verbatim shape from #267 (Frankfurt 2026-05-01 14:01:17 UTC):
        // STEP_OVERRUN warn followed 48s later by entry completion.
        fake_log(
            &c,
            t0,
            Level::Warn,
            "[STEP_OVERRUN] step() took 12.18s >= 7.50s (1.5x interval_secs=5); wall-clock tick skipped",
        );
        fake_log(
            &c,
            t0 + 48,
            Level::Info,
            "[ORDER] BTC/ETH entry orders filled",
        );
        assert!(
            c.pending_step_overrun.lock().unwrap().is_empty(),
            "entry completion must drain pending STEP_OVERRUN"
        );
        let (e, w) = snap_counts(&c, t0 + 60);
        assert_eq!(
            (e, w),
            (0, 0),
            "STEP_OVERRUN with completion must not commit"
        );
    }

    #[test]
    fn step_overrun_with_exit_completion_is_suppressed() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 8_000_000;
        fake_log(
            &c,
            t0,
            Level::Warn,
            "[STEP_OVERRUN] step() took 8.02s >= 7.50s (1.5x interval_secs=5); wall-clock tick skipped",
        );
        fake_log(
            &c,
            t0 + 30,
            Level::Info,
            "[ORDER] BTC/ETH exit orders filled",
        );
        let (_, w) = snap_counts(&c, t0 + 60);
        assert_eq!(
            w, 0,
            "STEP_OVERRUN paired with exit completion must not commit"
        );
    }

    #[test]
    fn step_overrun_without_completion_commits_after_deadline() {
        // No recovery log → genuinely stalled step() should still surface
        // as a real warn after the defer deadline. Protects against
        // deadlock / runaway REST patterns hiding behind suppression.
        let _g = _serialize();
        let c = make_counters();
        let t0 = 9_000_000;
        fake_log(
            &c,
            t0,
            Level::Warn,
            "[STEP_OVERRUN] step() took 30.00s >= 7.50s (1.5x interval_secs=5); wall-clock tick skipped",
        );
        // Before deadline: still pending.
        let (_, w0) = snap_counts(&c, t0 + 60);
        assert_eq!(w0, 0, "pre-deadline must not commit");
        // After deadline: committed.
        let (_, w1) = snap_counts(&c, t0 + STEP_OVERRUN_DEFER_WINDOW_SECS + 10);
        assert_eq!(w1, 1, "stalled STEP_OVERRUN must commit after deadline");
    }

    #[test]
    fn step_overrun_late_completion_does_not_uncommit() {
        // Symmetry with `ws_reset_with_late_recovery_does_not_uncommit`:
        // a completion log arriving past the defer deadline must not
        // retroactively cancel an already-committed warn.
        let _g = _serialize();
        let c = make_counters();
        let t0 = 10_000_000;
        fake_log(
            &c,
            t0,
            Level::Warn,
            "[STEP_OVERRUN] step() took 30.00s >= 7.50s (1.5x interval_secs=5); wall-clock tick skipped",
        );
        // Promote past deadline.
        let (_, w0) = snap_counts(&c, t0 + STEP_OVERRUN_DEFER_WINDOW_SECS + 1);
        assert_eq!(w0, 1, "post-deadline WARN commits");
        // Late completion arrives; counter must stay at 1.
        fake_log(
            &c,
            t0 + STEP_OVERRUN_DEFER_WINDOW_SECS + 60,
            Level::Info,
            "[ORDER] BTC/ETH entry orders filled",
        );
        let (_, w1) = snap_counts(&c, t0 + STEP_OVERRUN_DEFER_WINDOW_SECS + 70);
        assert_eq!(w1, 1, "late completion cannot uncommit");
    }

    #[test]
    fn step_overrun_and_ws_reset_independent() {
        // Both kinds of pending entries can coexist; recovery for one
        // must not drain the other.
        let _g = _serialize();
        let c = make_counters();
        let t0 = 11_000_000;
        fake_log(
            &c,
            t0,
            Level::Warn,
            "[STEP_OVERRUN] step() took 12.18s >= 7.50s (1.5x interval_secs=5); wall-clock tick skipped",
        );
        fake_log(
            &c,
            t0 + 5,
            Level::Error,
            "WebSocket error: IO error: Connection reset by peer",
        );
        // WS recovery only — STEP_OVERRUN entry must remain pending.
        fake_log(&c, t0 + 15, Level::Info, "WebSocket connected successfully");
        assert!(
            c.pending_ws.lock().unwrap().is_empty(),
            "WS reset drained by WS recovery"
        );
        assert_eq!(
            c.pending_step_overrun.lock().unwrap().len(),
            1,
            "STEP_OVERRUN must NOT be drained by WS recovery"
        );
        // Now the entry completion drains STEP_OVERRUN.
        fake_log(
            &c,
            t0 + 50,
            Level::Info,
            "[ORDER] BTC/ETH entry orders filled",
        );
        assert!(c.pending_step_overrun.lock().unwrap().is_empty());
        let (e, w) = snap_counts(&c, t0 + 60);
        assert_eq!((e, w), (0, 0));
    }

    // bot-strategy#343: ws_reset_24h_count replaces the dashboard's old
    // journalctl `Connection reset without closing handshake` SSM probe
    // with a self-reported counter. Tests live in this module because
    // the substring match + 24h ring lives in `Counters`.

    fn ws_reset_count(counters: &Counters, now: i64) -> u64 {
        // Mirror of `ErrorCounterHandle::ws_reset_24h_count` against an
        // explicit `now` so tests are deterministic.
        let cutoff = now - WS_RESET_24H_WINDOW_SECS;
        let mut q = counters.ws_resets_24h.lock().unwrap();
        while let Some(&front) = q.front() {
            if front < cutoff {
                q.pop_front();
            } else {
                break;
            }
        }
        q.len() as u64
    }

    #[test]
    fn ws_reset_24h_counts_matching_substring() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 12_000_000;
        // Three matching messages from different stream prefixes — the
        // substring match should catch them all regardless of which
        // is_ws_transient_event branch they otherwise take.
        fake_log(
            &c,
            t0,
            Level::Warn,
            "orderbook stream error: ws error: WebSocket protocol error: Connection reset without closing handshake (stream=orderbook BTC)",
        );
        fake_log(
            &c,
            t0 + 1,
            Level::Warn,
            "public trades stream error: ws error: Connection reset without closing handshake (stream=trades ETH)",
        );
        fake_log(
            &c,
            t0 + 2,
            Level::Warn,
            "account stream error: Connection reset without closing handshake",
        );
        // A non-matching warn must not contribute.
        fake_log(
            &c,
            t0 + 3,
            Level::Warn,
            "[XVENUE] tick error: read_mid Lighter",
        );
        assert_eq!(ws_reset_count(&c, t0 + 5), 3);
    }

    #[test]
    fn ws_reset_24h_expires_old_entries() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 13_000_000;
        fake_log(
            &c,
            t0,
            Level::Warn,
            "Connection reset without closing handshake (1)",
        );
        fake_log(
            &c,
            t0 + 100,
            Level::Warn,
            "Connection reset without closing handshake (2)",
        );
        // Probe just before the 24h boundary ages out the first entry —
        // both still in window.
        let now = t0 + WS_RESET_24H_WINDOW_SECS - 10;
        assert_eq!(ws_reset_count(&c, now), 2);
        // After both are 24h old, count is zero.
        let now = t0 + 100 + WS_RESET_24H_WINDOW_SECS + 10;
        assert_eq!(ws_reset_count(&c, now), 0);
    }

    #[test]
    fn ws_reset_24h_counts_independently_of_suppression() {
        // ws_reset count must not be silenced by maintenance suppression
        // — operators want to see the raw reset volume even during a
        // pre-announced outage so the dashboard alert remains accurate.
        let _g = _serialize();
        let c = make_counters();
        set_counting_suppressed(true);
        let t0 = 14_000_000;
        fake_log(
            &c,
            t0,
            Level::Warn,
            "Connection reset without closing handshake",
        );
        // The committed `recent` queue stays empty (suppression worked
        // for the rolling window), but ws_resets_24h has the entry.
        let (e, w) = snap_counts(&c, t0 + 5);
        assert_eq!((e, w), (0, 0));
        assert_eq!(ws_reset_count(&c, t0 + 5), 1);
        set_counting_suppressed(false);
    }

    #[test]
    fn ws_reset_24h_ignores_non_matching_phrase() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 15_000_000;
        // Closely-related but not exact phrases must not contribute. The
        // dashboard's old journalctl probe matched the exact substring;
        // the bot self-reported counter must match the same set.
        fake_log(
            &c,
            t0,
            Level::Warn,
            "Connection reset by peer (os error 104)",
        );
        fake_log(&c, t0 + 1, Level::Warn, "WebSocket reset");
        fake_log(
            &c,
            t0 + 2,
            Level::Warn,
            "Connection reset without graceful close",
        );
        assert_eq!(ws_reset_count(&c, t0 + 5), 0);
    }

    // bot-strategy#367: per-instance attribution. A WARN emitted inside
    // variant B's scope must inflate only B's bucket; variants A and C
    // must stay clean. Connector-layer events (no instance scope) land
    // in the shared bucket and surface on every variant.

    #[test]
    fn per_instance_event_isolates_to_emitting_variant() {
        let _g = _serialize();
        let c = make_counters();
        let t0 = 16_000_000;
        // B logs a SESSION_DD breach — the variant-tagged messages today
        // all carry the variant id in the body, but attribution now comes
        // from the CurrentInstance scope rather than string parsing.
        fake_log_for(
            &c,
            Some("b"),
            t0,
            Level::Warn,
            "[SESSION_DD] b breach: equity=148.0 peak=150.0 dd_bps=133.3 ...",
        );
        // A and C are clean (no log activity in their scope).
        let (a_e, a_w) = snap_counts_for(&c, Some("a"), t0 + 5);
        let (b_e, b_w) = snap_counts_for(&c, Some("b"), t0 + 5);
        let (c_e, c_w) = snap_counts_for(&c, Some("c"), t0 + 5);
        assert_eq!((a_e, a_w), (0, 0), "A must stay clean when only B errors");
        assert_eq!((b_e, b_w), (0, 1), "B must see its own warn");
        assert_eq!((c_e, c_w), (0, 0), "C must stay clean when only B errors");
    }

    #[test]
    fn shared_event_surfaces_on_every_variant() {
        // Connector-layer event (no CurrentInstance) goes into the None
        // bucket. Each variant's snapshot merges its own bucket with
        // None, so the shared event shows up everywhere — matching today's
        // semantics for genuinely shared signals (account refresh failure,
        // non-transient connector error, …).
        let _g = _serialize();
        let c = make_counters();
        let t0 = 17_000_000;
        fake_log_for(
            &c,
            None,
            t0,
            Level::Error,
            "Account refresh failed: rpc error from upstream",
        );
        let (a_e, _) = snap_counts_for(&c, Some("a"), t0 + 5);
        let (b_e, _) = snap_counts_for(&c, Some("b"), t0 + 5);
        let (c_e, _) = snap_counts_for(&c, Some("c"), t0 + 5);
        assert_eq!(a_e, 1, "shared event surfaces on A");
        assert_eq!(b_e, 1, "shared event surfaces on B");
        assert_eq!(c_e, 1, "shared event surfaces on C");
    }

    #[test]
    fn snapshot_for_merges_shared_and_instance() {
        // The exact scenario the issue describes: B trips SESSION_DD, a
        // shared connector ERROR fires separately. B sees both, A and C
        // see only the shared one.
        let _g = _serialize();
        let c = make_counters();
        let t0 = 18_000_000;
        fake_log_for(&c, None, t0, Level::Error, "shared connector failure");
        fake_log_for(
            &c,
            Some("b"),
            t0 + 1,
            Level::Warn,
            "[SESSION_DD] b breach: …",
        );
        let (a_e, a_w) = snap_counts_for(&c, Some("a"), t0 + 5);
        let (b_e, b_w) = snap_counts_for(&c, Some("b"), t0 + 5);
        assert_eq!((a_e, a_w), (1, 0), "A sees shared error only");
        assert_eq!((b_e, b_w), (1, 1), "B sees shared error + own warn");
    }

    #[test]
    fn deferred_pending_commits_into_captured_instance_bucket() {
        // STEP_OVERRUN fired inside instance C's scope must, on expiry,
        // commit to C's bucket — not bleed into A or B even though the
        // pending queue is process-global.
        let _g = _serialize();
        let c = make_counters();
        let t0 = 19_000_000;
        fake_log_for(
            &c,
            Some("c"),
            t0,
            Level::Warn,
            "[STEP_OVERRUN] step() took 30.00s >= 7.50s (1.5x interval_secs=5); wall-clock tick skipped",
        );
        let now = t0 + STEP_OVERRUN_DEFER_WINDOW_SECS + 10;
        let (_, a_w) = snap_counts_for(&c, Some("a"), now);
        let (_, b_w) = snap_counts_for(&c, Some("b"), now);
        let (_, c_w) = snap_counts_for(&c, Some("c"), now);
        assert_eq!(a_w, 0, "STEP_OVERRUN from C must not commit into A");
        assert_eq!(b_w, 0, "STEP_OVERRUN from C must not commit into B");
        assert_eq!(c_w, 1, "STEP_OVERRUN must commit into the emitting variant");
    }

    #[test]
    fn snapshot_for_unknown_variant_returns_shared_only() {
        // Querying a variant id that never logged anything is fine —
        // we just don't create a bucket for it. Shared events still
        // show through.
        let _g = _serialize();
        let c = make_counters();
        let t0 = 20_000_000;
        fake_log_for(&c, None, t0, Level::Warn, "shared warn");
        let (_, w) = snap_counts_for(&c, Some("never-existed"), t0 + 5);
        assert_eq!(
            w, 1,
            "shared events visible regardless of which variant queries"
        );
    }

    #[test]
    fn current_instance_guard_restores_previous() {
        // Nested guards must restore the outer scope on drop so the
        // logger can never leak attribution across consecutive
        // per-instance steps.
        assert_eq!(current_instance(), None);
        {
            let _outer = CurrentInstanceGuard::enter("a");
            assert_eq!(current_instance().as_deref(), Some("a"));
            {
                let _inner = CurrentInstanceGuard::enter("b");
                assert_eq!(current_instance().as_deref(), Some("b"));
            }
            assert_eq!(
                current_instance().as_deref(),
                Some("a"),
                "drop must restore the outer scope"
            );
        }
        assert_eq!(current_instance(), None, "drop must restore None");
    }
}
