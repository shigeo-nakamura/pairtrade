//! Error / warn counter layered over an inner `log::Log`.
//!
//! Used by the Status Dashboard to surface log-level anomalies that don't
//! cause the service to fail (see bot-strategy#45). Counters are maintained
//! in-process; snapshot via `ErrorCounterHandle::snapshot()` and embed in
//! `status.json`.

use log::{Level, Log, Metadata, Record};
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Process-global counter handle, populated by the binary's logger
/// initialization. Library code (e.g. `StatusReporter`) reads it to
/// include an `error_summary` section in `status.json`.
static GLOBAL_HANDLE: OnceLock<ErrorCounterHandle> = OnceLock::new();

pub fn install_global(handle: ErrorCounterHandle) {
    let _ = GLOBAL_HANDLE.set(handle);
}

pub fn global() -> Option<&'static ErrorCounterHandle> {
    GLOBAL_HANDLE.get()
}

/// Window (seconds) for the short-term rolling counts published in the
/// status snapshot.
const ROLLING_WINDOW_SECS: i64 = 300;

/// Keep the last error message truncated to this many chars so the
/// dashboard can display it without blowing up the JSON payload.
const LAST_ERROR_MAX_CHARS: usize = 200;

#[derive(Debug, Clone, Serialize)]
pub struct ErrorSummary {
    pub error_count_5m: u64,
    pub warn_count_5m: u64,
    pub error_count_total: u64,
    pub warn_count_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
}

struct Counters {
    recent: Mutex<VecDeque<(i64, Level)>>,
    last_error: Mutex<Option<(i64, String)>>,
    error_total: AtomicU64,
    warn_total: AtomicU64,
}

#[derive(Clone)]
pub struct ErrorCounterHandle {
    counters: Arc<Counters>,
}

impl ErrorCounterHandle {
    pub fn snapshot(&self) -> ErrorSummary {
        let now = chrono::Utc::now().timestamp();
        let cutoff = now - ROLLING_WINDOW_SECS;
        let (err_5m, warn_5m) = {
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
        let (last_ts, last_msg) = match self.counters.last_error.lock().unwrap().clone() {
            Some((ts, msg)) => (Some(ts), Some(msg)),
            None => (None, None),
        };
        ErrorSummary {
            error_count_5m: err_5m,
            warn_count_5m: warn_5m,
            error_count_total: self.counters.error_total.load(Ordering::Relaxed),
            warn_count_total: self.counters.warn_total.load(Ordering::Relaxed),
            last_error_ts: last_ts,
            last_error_message: last_msg,
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
            error_total: AtomicU64::new(0),
            warn_total: AtomicU64::new(0),
        });
        let handle = ErrorCounterHandle {
            counters: Arc::clone(&counters),
        };
        (Self { counters, inner }, handle)
    }
}

impl Log for ErrorCountingLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let level = record.level();
            if level == Level::Error || level == Level::Warn {
                let ts = chrono::Utc::now().timestamp();
                self.counters.recent.lock().unwrap().push_back((ts, level));
                if level == Level::Error {
                    self.counters.error_total.fetch_add(1, Ordering::Relaxed);
                    let msg = record.args().to_string();
                    let truncated = if msg.chars().count() > LAST_ERROR_MAX_CHARS {
                        msg.chars().take(LAST_ERROR_MAX_CHARS).collect::<String>() + "…"
                    } else {
                        msg
                    };
                    *self.counters.last_error.lock().unwrap() = Some((ts, truncated));
                } else {
                    self.counters.warn_total.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        self.inner.log(record);
    }

    fn flush(&self) {
        self.inner.flush();
    }
}
