//! Pending-error state machine. Transient WS-reset and `[STEP_OVERRUN]`
//! warns sit in per-kind queues until either a recovery marker drains
//! them or their defer window expires, at which point they commit into
//! the durable per-instance buckets owned by `Counters`.

use log::Level;
use std::collections::VecDeque;
use std::sync::Mutex;

use super::{commit_to_bucket, Counters};

/// Defer-window for transient WebSocket reset events. A WS reset that
/// auto-recovers within this window does not contribute to the rolling
/// counts (see bot-strategy#261). Sized for typical Lighter / Extended
/// reconnect cycles (~5–30s observed); 60s gives headroom for slow
/// reconnects without ageing out a real persistent disconnect.
pub(super) const WS_DEFER_WINDOW_SECS: i64 = 60;

/// Defer-window for `[STEP_OVERRUN]` warns. STEP_OVERRUN typically fires
/// when step() blocks on a partial-fill chain during entry / exit; the
/// `[ORDER] ... orders filled` recovery marker arrives within seconds-to-
/// minutes after the warn. Bot-strategy#267 observed a 48s gap between
/// STEP_OVERRUN and `entry orders filled` for a normal LongSpread fill, so
/// 180s gives ~3-4× headroom while still committing genuinely stuck steps
/// (e.g. deadlock, runaway REST loop) before the next status poll cycle.
pub(super) const STEP_OVERRUN_DEFER_WINDOW_SECS: i64 = 180;

#[derive(Debug, Clone)]
pub(super) struct PendingEntry {
    pub(super) ts: i64,
    pub(super) level: Level,
    pub(super) message: String,
    /// Instance id active when the entry was deferred. On expiry the
    /// entry commits to this bucket; recovery markers drain the entire
    /// queue regardless of attribution (recovery is a connector-level
    /// signal shared across variants).
    pub(super) instance: Option<String>,
}

/// Move pending entries from `queue` whose defer window has expired into
/// the durable per-instance buckets (and update last_error/last_warn +
/// totals). Called from both `snapshot()` and `log()` so the counts stay
/// current regardless of whether the dashboard is polling.
fn flush_expired_pending(
    queue: &Mutex<VecDeque<PendingEntry>>,
    counters: &Counters,
    now: i64,
    window: i64,
) {
    let cutoff = now - window;
    let mut to_commit: Vec<PendingEntry> = Vec::new();
    {
        let mut pending = queue.lock().unwrap();
        while let Some(front) = pending.front() {
            if front.ts <= cutoff {
                to_commit.push(pending.pop_front().unwrap());
            } else {
                break;
            }
        }
    }
    for entry in to_commit {
        commit_to_bucket(
            counters,
            entry.instance.as_deref(),
            entry.ts,
            entry.level,
            entry.message,
        );
    }
}

pub(super) fn flush_all_expired_pending(counters: &Counters, now: i64) {
    flush_expired_pending(&counters.pending_ws, counters, now, WS_DEFER_WINDOW_SECS);
    flush_expired_pending(
        &counters.pending_step_overrun,
        counters,
        now,
        STEP_OVERRUN_DEFER_WINDOW_SECS,
    );
}
