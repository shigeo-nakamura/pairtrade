//! Error / warn counter layered over an inner `log::Log`.
//!
//! Used by the Status Dashboard to surface log-level anomalies that don't
//! cause the service to fail (see bot-strategy#45). Counters are maintained
//! in-process; snapshot via `ErrorCounterHandle::snapshot()` and embed in
//! `status.json`.

use log::{Level, Log, Metadata, Record};
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

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

/// Window (seconds) for the short-term rolling counts published in the
/// status snapshot. Was 300 until bot-strategy#168: GitHub Actions scheduled
/// runs drift 40–70 min under load so a 5-min window let warns age out
/// between polls. 1800 (30 min) absorbs typical drift.
const ROLLING_WINDOW_SECS: i64 = 1800;

/// Defer-window for transient WebSocket reset events. A WS reset that
/// auto-recovers within this window does not contribute to the rolling
/// counts (see bot-strategy#261). Sized for typical Lighter / Extended
/// reconnect cycles (~5–30s observed); 60s gives headroom for slow
/// reconnects without ageing out a real persistent disconnect.
const WS_DEFER_WINDOW_SECS: i64 = 60;

/// Defer-window for `[STEP_OVERRUN]` warns. STEP_OVERRUN typically fires
/// when step() blocks on a partial-fill chain during entry / exit; the
/// `[ORDER] ... orders filled` recovery marker arrives within seconds-to-
/// minutes after the warn. Bot-strategy#267 observed a 48s gap between
/// STEP_OVERRUN and `entry orders filled` for a normal LongSpread fill, so
/// 180s gives ~3-4× headroom while still committing genuinely stuck steps
/// (e.g. deadlock, runaway REST loop) before the next status poll cycle.
const STEP_OVERRUN_DEFER_WINDOW_SECS: i64 = 180;

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

struct Counters {
    recent: Mutex<VecDeque<(i64, Level)>>,
    last_error: Mutex<Option<(i64, String)>>,
    last_warn: Mutex<Option<(i64, String)>>,
    error_total: AtomicU64,
    warn_total: AtomicU64,
    /// Transient WS-reset events queued for deferred commit. Each entry
    /// stays here until either (a) a recovery log line drains it before
    /// `WS_DEFER_WINDOW_SECS` elapses, or (b) `snapshot()` flushes it into
    /// `recent` once its deadline passes. See bot-strategy#261.
    pending_ws: Mutex<VecDeque<PendingEntry>>,
    /// `[STEP_OVERRUN]` warns queued for deferred commit. Drained by
    /// `[ORDER] ... entry/exit orders filled` recovery markers within
    /// `STEP_OVERRUN_DEFER_WINDOW_SECS`. See bot-strategy#267.
    pending_step_overrun: Mutex<VecDeque<PendingEntry>>,
}

#[derive(Debug, Clone)]
struct PendingEntry {
    ts: i64,
    level: Level,
    message: String,
}

/// Match log lines that signal a transient connectivity event whose effect
/// should be suppressed if the bot recovers within `WS_DEFER_WINDOW_SECS`.
/// Covers (1) the connector ERROR raised by the tungstenite WS layer when
/// the upstream RST-resets, and (2) the WARN downstream of that — the
/// xvenue-arb tick error and the pairtrade orderbook-stale signals — that
/// fire while the reconnect is in progress.
fn is_ws_transient_event(msg: &str) -> bool {
    msg.starts_with("WebSocket error:")
        || msg.starts_with("WebSocket IO error detail:")
        || msg.contains("tick error: read_mid")
        || msg.contains("order book snapshot unavailable")
        || msg.contains("waiting for websocket data")
        || msg.starts_with("orderbook stream error:")
        || msg.starts_with("public trades stream error:")
        || msg.starts_with("account stream error:")
}

/// Match log lines that signal a successful WS reconnect. Drains pending
/// transient entries logged within the past `WS_DEFER_WINDOW_SECS`.
fn is_ws_recovery_event(msg: &str) -> bool {
    msg.starts_with("WebSocket connected successfully")
        || msg.contains("WebSocket subscriptions sent successfully")
}

/// Match the critical `[STEP_OVERRUN]` warn (mild overruns log at INFO and
/// don't reach this path). Bot-strategy#267 traced one such warn to a
/// normal partial-fill chain: ENTRY started, ETH leg full-filled, BTC leg
/// chained 8 partial fills + reissues, step() returned 12s late, but the
/// trade itself completed cleanly. The warn is observational rather than a
/// failure signal — defer it until the matching completion log lands.
fn is_step_overrun_event(msg: &str) -> bool {
    msg.contains("[STEP_OVERRUN]")
}

/// Match the `[ORDER] X entry orders filled` / `[ORDER] X exit orders filled`
/// log lines that drain pending STEP_OVERRUN entries. A successful trade
/// completion within the defer window is taken as proof the slow step()
/// was waiting on order management (not a real stall).
fn is_step_overrun_recovery_event(msg: &str) -> bool {
    msg.contains("entry orders filled") || msg.contains("exit orders filled")
}

#[derive(Clone)]
pub struct ErrorCounterHandle {
    counters: Arc<Counters>,
}

impl ErrorCounterHandle {
    pub fn snapshot(&self) -> ErrorSummary {
        let now = chrono::Utc::now().timestamp();
        // Flush any pending entries whose recovery window has expired.
        // Lock order: pending_ws → pending_step_overrun → recent →
        // last_error/last_warn, matching the order in `log()` so no
        // deadlock is possible.
        flush_all_expired_pending(&self.counters, now);
        let cutoff = now - ROLLING_WINDOW_SECS;
        let (err_window, warn_window) = {
            let mut recent = self.counters.recent.lock().unwrap();
            while let Some(&(ts, _)) = recent.front() {
                if ts < cutoff {
                    recent.pop_front();
                } else {
                    break;
                }
            }
            let mut e = 0u64;
            let mut w = 0u64;
            for (_, lvl) in recent.iter() {
                match lvl {
                    Level::Error => e += 1,
                    Level::Warn => w += 1,
                    _ => {}
                }
            }
            (e, w)
        };
        let (last_err_ts, last_err_msg) = match self.counters.last_error.lock().unwrap().clone() {
            Some((ts, msg)) => (Some(ts), Some(msg)),
            None => (None, None),
        };
        let (last_warn_ts, last_warn_msg) = match self.counters.last_warn.lock().unwrap().clone() {
            Some((ts, msg)) => (Some(ts), Some(msg)),
            None => (None, None),
        };
        ErrorSummary {
            error_count_30m: err_window,
            warn_count_30m: warn_window,
            error_count_total: self.counters.error_total.load(Ordering::Relaxed),
            warn_count_total: self.counters.warn_total.load(Ordering::Relaxed),
            last_error_ts: last_err_ts,
            last_error_message: last_err_msg,
            last_warn_ts,
            last_warn_message: last_warn_msg,
        }
    }
}

pub struct ErrorCountingLogger {
    counters: Arc<Counters>,
    inner: Box<dyn Log>,
}

impl ErrorCountingLogger {
    pub fn wrap(inner: Box<dyn Log>) -> (Self, ErrorCounterHandle) {
        let counters = Arc::new(Counters {
            recent: Mutex::new(VecDeque::new()),
            last_error: Mutex::new(None),
            last_warn: Mutex::new(None),
            error_total: AtomicU64::new(0),
            warn_total: AtomicU64::new(0),
            pending_ws: Mutex::new(VecDeque::new()),
            pending_step_overrun: Mutex::new(VecDeque::new()),
        });
        let handle = ErrorCounterHandle {
            counters: Arc::clone(&counters),
        };
        (Self { counters, inner }, handle)
    }
}

/// Move pending entries from `queue` whose defer window has expired into
/// the durable `recent` queue (and update last_error/last_warn + totals).
/// Called from both `snapshot()` and `log()` so the counts stay current
/// regardless of whether the dashboard is polling.
fn flush_expired_pending(
    queue: &Mutex<VecDeque<PendingEntry>>,
    counters: &Counters,
    now: i64,
    window: i64,
) {
    let cutoff = now - window;
    let mut pending = queue.lock().unwrap();
    let mut to_commit: Vec<PendingEntry> = Vec::new();
    while let Some(front) = pending.front() {
        if front.ts <= cutoff {
            to_commit.push(pending.pop_front().unwrap());
        } else {
            break;
        }
    }
    drop(pending);
    if to_commit.is_empty() {
        return;
    }
    let mut recent = counters.recent.lock().unwrap();
    for entry in &to_commit {
        recent.push_back((entry.ts, entry.level));
    }
    drop(recent);
    for entry in to_commit {
        match entry.level {
            Level::Error => {
                counters.error_total.fetch_add(1, Ordering::Relaxed);
                *counters.last_error.lock().unwrap() = Some((entry.ts, entry.message));
            }
            Level::Warn => {
                counters.warn_total.fetch_add(1, Ordering::Relaxed);
                *counters.last_warn.lock().unwrap() = Some((entry.ts, entry.message));
            }
            _ => {}
        }
    }
}

fn flush_all_expired_pending(counters: &Counters, now: i64) {
    flush_expired_pending(&counters.pending_ws, counters, now, WS_DEFER_WINDOW_SECS);
    flush_expired_pending(
        &counters.pending_step_overrun,
        counters,
        now,
        STEP_OVERRUN_DEFER_WINDOW_SECS,
    );
}

impl Log for ErrorCountingLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let ts = chrono::Utc::now().timestamp();
            let msg = record.args().to_string();
            // Recovery markers fire at INFO; check before the level gate so
            // they can drain pending entries from any preceding transient.
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
                if is_ws_transient_event(&truncated) {
                    // Defer: held in pending_ws until either drained by a
                    // recovery marker or expired by flush_all_expired_pending.
                    self.counters
                        .pending_ws
                        .lock()
                        .unwrap()
                        .push_back(PendingEntry {
                            ts,
                            level,
                            message: truncated,
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
                        });
                } else {
                    self.counters.recent.lock().unwrap().push_back((ts, level));
                    if level == Level::Error {
                        self.counters.error_total.fetch_add(1, Ordering::Relaxed);
                        *self.counters.last_error.lock().unwrap() = Some((ts, truncated));
                    } else {
                        self.counters.warn_total.fetch_add(1, Ordering::Relaxed);
                        *self.counters.last_warn.lock().unwrap() = Some((ts, truncated));
                    }
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

    /// Test-only: build a Counters in isolation (no inner Log dependency).
    fn make_counters() -> Arc<Counters> {
        Arc::new(Counters {
            recent: Mutex::new(VecDeque::new()),
            last_error: Mutex::new(None),
            last_warn: Mutex::new(None),
            error_total: AtomicU64::new(0),
            warn_total: AtomicU64::new(0),
            pending_ws: Mutex::new(VecDeque::new()),
            pending_step_overrun: Mutex::new(VecDeque::new()),
        })
    }

    /// Test-only: simulate `log()` without going through the real `log!()`
    /// macro / global logger, so each test controls timestamps and
    /// pattern matching directly. Mirrors the real `log()` body.
    fn fake_log(counters: &Counters, ts: i64, level: Level, msg: &str) {
        if is_ws_recovery_event(msg) {
            let cutoff = ts - WS_DEFER_WINDOW_SECS;
            counters.pending_ws.lock().unwrap().retain(|e| e.ts < cutoff);
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
        if is_ws_transient_event(&truncated) {
            counters.pending_ws.lock().unwrap().push_back(PendingEntry {
                ts,
                level,
                message: truncated,
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
                });
        } else {
            counters.recent.lock().unwrap().push_back((ts, level));
            if level == Level::Error {
                counters.error_total.fetch_add(1, Ordering::Relaxed);
                *counters.last_error.lock().unwrap() = Some((ts, truncated));
            } else {
                counters.warn_total.fetch_add(1, Ordering::Relaxed);
                *counters.last_warn.lock().unwrap() = Some((ts, truncated));
            }
        }
    }

    fn snap_counts(counters: &Counters, now: i64) -> (u64, u64) {
        flush_all_expired_pending(counters, now);
        let recent = counters.recent.lock().unwrap();
        let cutoff = now - ROLLING_WINDOW_SECS;
        let mut e = 0u64;
        let mut w = 0u64;
        for &(ts, lvl) in recent.iter() {
            if ts < cutoff {
                continue;
            }
            match lvl {
                Level::Error => e += 1,
                Level::Warn => w += 1,
                _ => {}
            }
        }
        (e, w)
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
        fake_log(&c, t0, Level::Error, "WebSocket error: IO error: Connection reset by peer (os error 104)");
        fake_log(&c, t0, Level::Error, "WebSocket IO error detail: kind=ConnectionReset, error=Connection reset by peer");
        fake_log(&c, t0 + 18, Level::Warn, "[XVENUE] tick error: read_mid Lighter\n\nCaused by:\n    get_order_book(ETH, 1): Other(\"order book snapshot unavailable (no recent update)\")");
        fake_log(&c, t0 + 23, Level::Warn, "[XVENUE] tick error: read_mid Lighter");
        fake_log(&c, t0 + 27, Level::Info, "WebSocket connected successfully: ...");
        fake_log(&c, t0 + 27, Level::Info, "WebSocket subscriptions sent successfully");
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
        fake_log(&c, t0, Level::Error, "WebSocket error: IO error: Connection reset by peer (os error 104)");
        fake_log(&c, t0 + 5, Level::Warn, "[XVENUE] tick error: read_mid Lighter");
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
        fake_log(&c, t0, Level::Error, "WebSocket error: IO error: Connection reset by peer");
        // No recovery within window — flush via a snapshot past the deadline
        // promotes the entry to `recent`.
        let (e0, _) = snap_counts(&c, t0 + WS_DEFER_WINDOW_SECS + 1);
        assert_eq!(e0, 1, "post-deadline ERROR commits");
        // A late recovery marker must NOT retroactively decrement the
        // already-committed counter — the `recent` queue is durable.
        fake_log(&c, t0 + 120, Level::Info, "WebSocket connected successfully: ...");
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
        fake_log(&c, t0, Level::Error, "WebSocket error: IO error: Connection reset by peer");
        fake_log(&c, t0 + 5, Level::Warn, "[XVENUE] tick error: read_mid Lighter");
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
        fake_log(&c, t0, Level::Error, "WebSocket error: IO error: Connection reset");
        fake_log(&c, t0 + 5, Level::Warn, "orderbook BTC/ETH unavailable: waiting for websocket data");
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
        assert_eq!((e, w), (0, 0), "account stream WARN must suppress on recovery");
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
        fake_log(&c, t0 + 48, Level::Info, "[ORDER] BTC/ETH entry orders filled");
        assert!(
            c.pending_step_overrun.lock().unwrap().is_empty(),
            "entry completion must drain pending STEP_OVERRUN"
        );
        let (e, w) = snap_counts(&c, t0 + 60);
        assert_eq!((e, w), (0, 0), "STEP_OVERRUN with completion must not commit");
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
        fake_log(&c, t0 + 30, Level::Info, "[ORDER] BTC/ETH exit orders filled");
        let (_, w) = snap_counts(&c, t0 + 60);
        assert_eq!(w, 0, "STEP_OVERRUN paired with exit completion must not commit");
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
        fake_log(&c, t0 + 5, Level::Error, "WebSocket error: IO error: Connection reset by peer");
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
        fake_log(&c, t0 + 50, Level::Info, "[ORDER] BTC/ETH entry orders filled");
        assert!(c.pending_step_overrun.lock().unwrap().is_empty());
        let (e, w) = snap_counts(&c, t0 + 60);
        assert_eq!((e, w), (0, 0));
    }
}
