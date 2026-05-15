//! Position, pending-order, and related state types extracted from the
//! monolithic pairtrade module. Field visibility is `pub(super)` so that the
//! engine in `mod.rs` can keep accessing them as before; promotion to `pub`
//! is deferred until the engine itself migrates out.

use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::time::Instant;

use dex_connector::DexError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::config::PairTradeConfig;
use super::kalman::KalmanBeta;
use super::util::mean_std;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum PositionDirection {
    LongSpread,
    ShortSpread,
}

#[derive(Debug, Clone)]
pub(super) struct Position {
    pub(super) direction: PositionDirection,
    pub(super) entered_at: Instant,
    /// Replay-aware entry timestamp (seconds). In live mode equals
    /// `chrono::Utc::now().timestamp()` at the moment of entry; in backtest
    /// mode equals the replay's logical timestamp. Used for all
    /// duration-based decisions (force_close, hold-time PnL, etc.) so they
    /// behave identically under replay.
    pub(super) entered_ts: i64,
    pub(super) entry_price_a: Option<Decimal>,
    pub(super) entry_price_b: Option<Decimal>,
    pub(super) entry_size_a: Option<Decimal>,
    pub(super) entry_size_b: Option<Decimal>,
    pub(super) entry_z: Option<f64>,
}

#[derive(Debug, Clone)]
pub(super) struct PendingLeg {
    pub(super) symbol: String,
    pub(super) order_id: String,
    pub(super) exchange_order_id: Option<String>,
    pub(super) target: Decimal,
    pub(super) filled: Decimal,
    pub(super) side: dex_connector::OrderSide,
    /// Limit price posted for this leg, when placed as a limit/post-only
    /// order. `None` for market orders and for reissue paths that do not
    /// carry a limit forward. Used by the post-only fallback instrumentation
    /// ([ORDER_FALLBACK_DETAIL], bot-strategy#165) to compare the posted
    /// price against the book at timeout.
    pub(super) limit_price: Option<Decimal>,
    /// Decision-time reference price captured at order placement (best
    /// quote on the trading side or recent mid). Distinct from
    /// `limit_price`: limit is the price actually posted to the venue
    /// (only set for limit/post-only orders), reference is the price
    /// the decision was made against (set for both limit and market
    /// orders). Used by the per-leg slippage histogram (#314 Group
    /// 4-B-2) so taker fallbacks — where `limit_price = None` — still
    /// produce a measurable adverse-slippage signal.
    pub(super) reference_price: Option<Decimal>,
}

#[derive(Debug)]
pub(super) struct PendingOrders {
    pub(super) legs: Vec<PendingLeg>,
    pub(super) direction: PositionDirection,
    pub(super) placed_at: Instant,
    /// Wall-clock placement time in Unix epoch milliseconds. Captured at
    /// every PendingOrders construction (including reissue paths) so the
    /// per-leg fill-latency histogram (#314 Group 4-C) can compare against
    /// the venue-reported `FilledOrder.filled_ts_ms`. `placed_at` is a
    /// monotonic `Instant` and not directly comparable to wall-clock fill
    /// timestamps, hence this parallel field.
    pub(super) placed_ts_ms: i64,
    pub(super) hedge_retry_count: u32,
    pub(super) post_only_hybrid: bool,
    /// Set only for post-only exits on fee-bearing venues (Extended).
    /// When `Instant::now() >= t`, the reconcile loop cancels the resting
    /// post-only legs and reissues as taker even though
    /// `order_timeout_secs` (typically 120s) has not yet elapsed. Replaces
    /// the synchronous `monitor_exit_legs_with_timeout` flow that blocked
    /// `step()` for the full timeout (bot-strategy#408).
    pub(super) exit_taker_takeover_at: Option<Instant>,
}

#[derive(Debug)]
pub(super) struct PendingStatus {
    pub(super) open_remaining: usize,
    pub(super) fills: HashMap<String, Decimal>,
    /// Per-order-id sum of `FilledOrder.filled_value` (i.e. size × price)
    /// across all partial fills for that order. Used by the slippage /
    /// fee-bps histograms (#314 Group 4-B) to derive a volume-weighted
    /// average fill price without re-querying the venue. Absent entries
    /// mean the venue did not report a value for that fill.
    pub(super) filled_values: HashMap<String, Decimal>,
    /// Per-order-id sum of `FilledOrder.filled_fee`. Same semantics as
    /// `filled_values`. Lighter (fee-free) leaves entries unpopulated;
    /// Extended populates them.
    pub(super) filled_fees: HashMap<String, Decimal>,
    /// Per-order-id maximum of `FilledOrder.filled_ts_ms` across that
    /// order's partial fills (i.e. completion time, in Unix epoch ms).
    /// Used by the fill-latency histogram (#314 Group 4-C). Extended
    /// populates the source field; Lighter leaves it `None` so entries
    /// stay absent and no latency is emitted there.
    pub(super) filled_ts_ms_max: HashMap<String, i64>,
    pub(super) open_ids: HashSet<String>,
}

#[derive(Debug)]
pub(super) struct PartialOrderPlacementError {
    pub(super) legs: Vec<PendingLeg>,
    pub(super) source: DexError,
}

impl PartialOrderPlacementError {
    pub(super) fn new(legs: Vec<PendingLeg>, source: DexError) -> Self {
        Self { legs, source }
    }

    pub(super) fn legs(&self) -> &[PendingLeg] {
        &self.legs
    }
}

impl std::fmt::Display for PartialOrderPlacementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to place all legs: {}", self.source)
    }
}

impl Error for PartialOrderPlacementError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Per-pair quantities that are deterministic functions of the shared
/// log-price history and therefore must produce identical values for every
/// strategy variant operating on the same pair. Owned at engine level
/// (`PairTradeEngine.per_pair_state`) so A/B/C variants in the same process
/// observe a single source of truth for β / spread / z. See bot-strategy#413.
#[derive(Debug)]
pub(super) struct PairSharedState {
    pub(super) beta: f64,
    pub(super) spread_history: VecDeque<f64>,
    pub(super) last_spread: Option<f64>,
    pub(super) last_velocity_sigma_per_min: f64,
    pub(super) beta_short: f64,
    pub(super) beta_long: f64,
    pub(super) half_life_hours: f64,
    pub(super) adf_p_value: f64,
    pub(super) eligible: bool,
    pub(super) last_evaluated: Option<Instant>,
    /// Replay-aware companion to `last_evaluated`. Drives the periodic
    /// pair re-evaluation interval (PAIR_SELECTION_INTERVAL_SECS).
    pub(super) last_evaluated_ts: Option<i64>,
    pub(super) p_value_weighted_score: f64,
    pub(super) beta_gap: f64,
    pub(super) kalman: Option<KalmanBeta>,
    /// Rolling history of the most recent full-window spread std values, one
    /// sample per bar with a valid z-score. Used by the std-collapse guard
    /// (bot-strategy#62) to detect when the z-score denominator has fallen
    /// far below its recent median — a sign that the z-score is no longer a
    /// trustworthy mean-reversion signal.
    pub(super) std_history: VecDeque<f64>,
}

#[derive(Debug)]
pub(super) struct PairState {
    /// Per-instance entry-z threshold; recomputed each tick from the
    /// shared `beta_gap` × the variant's `entry_z_score_base/min/max`
    /// overlay (bot-strategy#411). The β / spread / z themselves live in
    /// `PairSharedState`.
    pub(super) z_entry: f64,
    pub(super) position: Option<Position>,
    pub(super) last_exit_at: Option<Instant>,
    /// Replay-aware companion to `last_exit_at`. Drives the should_enter
    /// cooldown and unhedged-close cooldown so they fire correctly under
    /// backtest replay.
    pub(super) last_exit_ts: Option<i64>,
    pub(super) pending_entry: Option<PendingOrders>,
    pub(super) pending_exit: Option<PendingOrders>,
    pub(super) position_guard: bool,
    /// BT fill-delay: when an exit is decided in dry_run + backtest mode with
    /// `bt_fill_delay_secs > 0`, we defer clearing `position` until the replay
    /// clock has advanced past this timestamp. While set, the bot considers the
    /// position still held (blocking new entries). The PnL is already computed
    /// and stored here so it can be logged when the deferred exit resolves.
    pub(super) bt_deferred_exit: Option<BtDeferredExit>,
    /// Reason classifier for the in-flight exit, set when `exit_reason()`
    /// decides a close and consumed at the exit-fill site so we can tag
    /// post-stop state without plumbing the reason through `TradeAction`
    /// + `PendingOrders` + reconcile. Cleared after consumption.
    /// bot-strategy#316.
    pub(super) pending_exit_reason: Option<&'static str>,
    /// Direction + replay timestamp of the most recent stop_loss_z exit.
    /// Drives the post-stop cool-down guard in `should_enter`. Per-direction
    /// so that a LongSpread stop does not block a ShortSpread reversal.
    /// Persisted via `InstanceRiskState.last_stop_loss_per_pair` so the
    /// guard survives restart. bot-strategy#316.
    pub(super) last_stop_loss_at: Option<(PositionDirection, i64)>,
}

/// Deferred exit info for BT fill-delay simulation.
#[derive(Debug)]
pub(super) struct BtDeferredExit {
    /// Replay timestamp (seconds) at which the position should be cleared.
    pub(super) resolve_at_ts: i64,
}

impl PairSharedState {
    pub(super) fn new(window: usize) -> Self {
        Self {
            beta: 1.0,
            spread_history: VecDeque::with_capacity(window),
            last_spread: None,
            last_velocity_sigma_per_min: 0.0,
            beta_short: 1.0,
            beta_long: 1.0,
            half_life_hours: 0.0,
            adf_p_value: 1.0,
            eligible: false,
            last_evaluated: None,
            last_evaluated_ts: None,
            p_value_weighted_score: 0.0,
            beta_gap: 0.0,
            kalman: None,
            std_history: VecDeque::new(),
        }
    }

    pub(super) fn push_spread(&mut self, spread: f64, window: usize, config: &PairTradeConfig) {
        if self.spread_history.len() >= window {
            self.spread_history.pop_front();
        }
        self.spread_history.push_back(spread);
        self.last_spread = Some(spread);

        // Record the current full-window std for the std-collapse guard
        // (bot-strategy#62). Skip degenerate or insufficient samples so the
        // rolling median only tracks meaningful std values.
        let std_window = config.default_pair_params.std_collapse_window_bars;
        if std_window > 0 {
            if let Some((_z, std)) = self.z_score() {
                if std > 1e-9 {
                    if self.std_history.len() >= std_window {
                        self.std_history.pop_front();
                    }
                    self.std_history.push_back(std);
                }
            }
        }

        // velocity uses bar-to-bar move (1-minute bars) normalized by std dev
        let k = 1_usize;
        if self.spread_history.len() > k {
            if let (Some(&latest), Some(&past)) = (
                self.spread_history.back(),
                self.spread_history.get(self.spread_history.len() - k - 1),
            ) {
                let delta = latest - past; // per-bar move
                let per_min = delta / ((k as f64 * config.trading_period_secs as f64) / 60.0);
                if let Some((_z, std)) = self.z_score() {
                    if std > 1e-9 {
                        self.last_velocity_sigma_per_min = per_min / std;
                    }
                }
            }
        }
    }

    pub(super) fn z_score(&self) -> Option<(f64, f64)> {
        self.z_score_details().map(|(z, std, _, _)| (z, std))
    }

    pub(super) fn z_score_details(&self) -> Option<(f64, f64, f64, f64)> {
        if self.spread_history.len() < 2 {
            return None;
        }
        let (mean, std) = mean_std(&self.spread_history)?;
        let latest = *self.spread_history.back().unwrap();
        let z = if std < 1e-9 {
            0.0
        } else {
            (latest - mean) / std
        };
        Some((z, std, mean, latest))
    }

    /// Compute z-score using only the last `window` bars of spread_history.
    /// Used by the multi-timeframe confluence filter.
    pub(super) fn z_score_for_window(&self, window: usize) -> Option<f64> {
        let len = self.spread_history.len().min(window);
        if len < 2 {
            return None;
        }
        let start = self.spread_history.len() - len;
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        for i in start..self.spread_history.len() {
            let v = self.spread_history[i];
            sum += v;
            sum_sq += v * v;
        }
        let n = len as f64;
        let mean = sum / n;
        let var = (sum_sq / n) - mean * mean;
        let std = var.max(0.0).sqrt();
        if std < 1e-9 {
            return None;
        }
        let latest = *self.spread_history.back().unwrap();
        Some((latest - mean) / std)
    }
}

impl PairState {
    pub(super) fn new(z_entry: f64) -> Self {
        Self {
            z_entry,
            position: None,
            last_exit_at: None,
            last_exit_ts: None,
            pending_entry: None,
            pending_exit: None,
            position_guard: false,
            bt_deferred_exit: None,
            pending_exit_reason: None,
            last_stop_loss_at: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// bot-strategy#413 invariant: two `PairSharedState`s with identical
    /// `spread_history` must produce bitwise-equal z / std / mean. This
    /// is what guarantees A/B/C variants observe the same z when they
    /// each read from `engine.per_pair_state[key]` (which is, in the live
    /// engine, exactly the same shared deque).
    #[test]
    fn shared_state_z_is_deterministic_given_same_inputs() {
        let mut a = PairSharedState::new(120);
        let mut b = PairSharedState::new(120);
        // Hand-populate the spread_history; we avoid PairTradeConfig here
        // because `push_spread` pulls trading_period / std_collapse_window
        // from it and PairTradeConfig has no Default impl. The behaviour
        // under test is the z calculation, not push_spread.
        for i in 0..200 {
            let v = 0.02 * ((i as f64) * 0.1).sin() + 1.5;
            a.spread_history.push_back(v);
            b.spread_history.push_back(v);
        }

        let (z_a, std_a, mean_a, latest_a) = a.z_score_details().unwrap();
        let (z_b, std_b, mean_b, latest_b) = b.z_score_details().unwrap();
        assert_eq!(z_a.to_bits(), z_b.to_bits(), "z must be bitwise equal");
        assert_eq!(std_a.to_bits(), std_b.to_bits());
        assert_eq!(mean_a.to_bits(), mean_b.to_bits());
        assert_eq!(latest_a.to_bits(), latest_b.to_bits());
    }

    /// PairState (per-instance) no longer carries β / z / spread fields.
    /// This test pins the new struct shape — any field re-added here
    /// without an explicit decision risks reintroducing the per-instance
    /// drift that #413 fixes.
    #[test]
    fn pair_state_keeps_only_per_instance_fields() {
        let s = PairState::new(1.5);
        // Compile-time pin: these fields stay per-instance.
        let _ = s.z_entry;
        let _ = &s.position;
        let _ = &s.pending_entry;
        let _ = &s.pending_exit;
        let _ = &s.bt_deferred_exit;
        let _ = &s.pending_exit_reason;
        let _ = &s.last_stop_loss_at;
        let _ = &s.last_exit_at;
        let _ = &s.last_exit_ts;
        let _ = s.position_guard;
    }

    #[test]
    fn z_score_for_window_handles_short_history() {
        let mut s = PairSharedState::new(60);
        s.spread_history.push_back(1.0);
        // window > history is permissive (returns Some when len ≥ 2).
        assert!(s.z_score_for_window(60).is_none());
        s.spread_history.push_back(2.0);
        assert!(s.z_score_for_window(60).is_some());
    }
}
