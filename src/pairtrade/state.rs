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
use super::stats::PriceSample;
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
}

#[derive(Debug)]
pub(super) struct PendingOrders {
    pub(super) legs: Vec<PendingLeg>,
    pub(super) direction: PositionDirection,
    pub(super) placed_at: Instant,
    pub(super) hedge_retry_count: u32,
    pub(super) post_only_hybrid: bool,
}

#[derive(Debug)]
pub(super) struct PendingStatus {
    pub(super) open_remaining: usize,
    pub(super) fills: HashMap<String, Decimal>,
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

#[derive(Debug)]
pub(super) struct PairState {
    pub(super) beta: f64,
    pub(super) z_entry: f64,
    pub(super) spread_history: VecDeque<f64>,
    pub(super) last_spread: Option<f64>,
    pub(super) last_velocity_sigma_per_min: f64,
    pub(super) position: Option<Position>,
    pub(super) last_exit_at: Option<Instant>,
    /// Replay-aware companion to `last_exit_at`. Drives the should_enter
    /// cooldown and unhedged-close cooldown so they fire correctly under
    /// backtest replay.
    pub(super) last_exit_ts: Option<i64>,
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
    pub(super) pending_entry: Option<PendingOrders>,
    pub(super) pending_exit: Option<PendingOrders>,
    pub(super) position_guard: bool,
    pub(super) kalman: Option<KalmanBeta>,
    /// Rolling history of the most recent full-window spread std values, one
    /// sample per bar with a valid z-score. Used by the std-collapse guard
    /// (bot-strategy#62) to detect when the z-score denominator has fallen
    /// far below its recent median — a sign that the z-score is no longer a
    /// trustworthy mean-reversion signal.
    pub(super) std_history: VecDeque<f64>,
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

impl PairState {
    pub(super) fn new(window: usize, z_entry: f64) -> Self {
        Self {
            beta: 1.0,
            z_entry,
            spread_history: VecDeque::with_capacity(window),
            last_spread: None,
            last_velocity_sigma_per_min: 0.0,
            position: None,
            last_exit_at: None,
            last_exit_ts: None,
            beta_short: 1.0,
            beta_long: 1.0,
            half_life_hours: 0.0,
            adf_p_value: 1.0,
            eligible: false,
            last_evaluated: None,
            last_evaluated_ts: None,
            p_value_weighted_score: 0.0,
            beta_gap: 0.0,
            pending_entry: None,
            pending_exit: None,
            position_guard: false,
            kalman: None,
            std_history: VecDeque::new(),
            bt_deferred_exit: None,
            pending_exit_reason: None,
            last_stop_loss_at: None,
        }
    }

    /// Rebuild `spread_history` from the trailing `window` bars of the base
    /// and quote symbol histories, evaluated against the supplied `new_beta`.
    ///
    /// Motivation (bot-strategy#274): `push_spread` records each spread with
    /// whatever `state.beta` was active at push time. Across A/B/C instances
    /// (or across hosts) the eval-fire timestamps drift, so the 240-entry
    /// rolling window ends up as a mix of historical β regimes — and the
    /// resulting mean/std/z diverge even when both processes observe the same
    /// bar series. Calling this fn immediately after `state.beta` is updated
    /// by `evaluate_pair` collapses that mix back to "240 bars × current β",
    /// removing the trajectory term as a source of cross-host divergence.
    ///
    /// `std_history` is intentionally left untouched. It is the time series
    /// of past full-window std observations used by the std-collapse guard
    /// (bot-strategy#62); rewriting it with synthetic stds from the rebuilt
    /// spread series would erase the very signal the guard exists to detect.
    pub(super) fn rebuild_spread_history_with_beta(
        &mut self,
        hist_a: &VecDeque<PriceSample>,
        hist_b: &VecDeque<PriceSample>,
        window: usize,
        new_beta: f64,
    ) {
        let take = window.min(hist_a.len()).min(hist_b.len());
        if take == 0 {
            self.spread_history.clear();
            self.last_spread = None;
            return;
        }
        let start_a = hist_a.len() - take;
        let start_b = hist_b.len() - take;
        let mut rebuilt = VecDeque::with_capacity(take);
        for i in 0..take {
            let log_a = hist_a[start_a + i].log_price;
            let log_b = hist_b[start_b + i].log_price;
            rebuilt.push_back(log_a - new_beta * log_b);
        }
        self.last_spread = rebuilt.back().copied();
        self.spread_history = rebuilt;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(log_price: f64, ts: i64) -> PriceSample {
        PriceSample { log_price, ts }
    }

    fn assert_close(a: f64, b: f64, eps: f64) {
        assert!(
            (a - b).abs() < eps,
            "expected {} ≈ {} (within {})",
            a,
            b,
            eps
        );
    }

    #[test]
    fn rebuild_uses_supplied_beta_for_every_bar() {
        let mut state = PairState::new(240, 2.0);
        // Pre-seed with garbage values to prove rebuild overwrites them.
        state.spread_history.extend([99.0, -42.0, 0.0]);
        state.last_spread = Some(99.0);

        let hist_a: VecDeque<PriceSample> = (0..10)
            .map(|i| sample(10.0 + i as f64 * 0.01, 1_000 + i))
            .collect();
        let hist_b: VecDeque<PriceSample> = (0..10)
            .map(|i| sample(7.0 + i as f64 * 0.005, 1_000 + i))
            .collect();

        state.rebuild_spread_history_with_beta(&hist_a, &hist_b, 240, 0.5);

        assert_eq!(state.spread_history.len(), 10);
        for (i, spread) in state.spread_history.iter().enumerate() {
            let expected = hist_a[i].log_price - 0.5 * hist_b[i].log_price;
            assert_close(*spread, expected, 1e-12);
        }
        assert_eq!(state.last_spread, state.spread_history.back().copied());
    }

    #[test]
    fn rebuild_clips_to_window_keeping_most_recent_bars() {
        let mut state = PairState::new(240, 2.0);
        let hist_a: VecDeque<PriceSample> = (0..500)
            .map(|i| sample(10.0 + i as f64 * 0.001, 1_000 + i))
            .collect();
        let hist_b: VecDeque<PriceSample> = (0..500)
            .map(|i| sample(7.0 + i as f64 * 0.0005, 1_000 + i))
            .collect();

        state.rebuild_spread_history_with_beta(&hist_a, &hist_b, 240, 0.8);

        assert_eq!(state.spread_history.len(), 240);
        // First entry of the rebuilt window should align with the 240-th
        // bar from the end of the supplied history.
        let first_idx = hist_a.len() - 240;
        let expected_first =
            hist_a[first_idx].log_price - 0.8 * hist_b[first_idx].log_price;
        assert_close(state.spread_history[0], expected_first, 1e-12);
        // Last entry aligns with the newest bar.
        let last = hist_a.len() - 1;
        let expected_last = hist_a[last].log_price - 0.8 * hist_b[last].log_price;
        assert_close(*state.spread_history.back().unwrap(), expected_last, 1e-12);
    }

    #[test]
    fn rebuild_uses_min_of_two_histories_when_misaligned() {
        let mut state = PairState::new(240, 2.0);
        let hist_a: VecDeque<PriceSample> = (0..5)
            .map(|i| sample(10.0 + i as f64, 1_000 + i))
            .collect();
        let hist_b: VecDeque<PriceSample> = (0..3)
            .map(|i| sample(7.0 + i as f64, 1_000 + i))
            .collect();

        state.rebuild_spread_history_with_beta(&hist_a, &hist_b, 240, 1.0);

        // Both histories tail-aligned at len=3. So we take the LAST 3 of
        // each: hist_a[2..5] vs hist_b[0..3].
        assert_eq!(state.spread_history.len(), 3);
        let start_a = hist_a.len() - 3;
        let start_b = hist_b.len() - 3;
        for i in 0..3 {
            let expected = hist_a[start_a + i].log_price
                - 1.0 * hist_b[start_b + i].log_price;
            assert_close(state.spread_history[i], expected, 1e-12);
        }
    }

    #[test]
    fn rebuild_with_empty_history_clears_state() {
        let mut state = PairState::new(240, 2.0);
        state.spread_history.extend([1.0, 2.0, 3.0]);
        state.last_spread = Some(3.0);

        let empty: VecDeque<PriceSample> = VecDeque::new();
        let hist_b: VecDeque<PriceSample> = (0..3).map(|i| sample(7.0, i)).collect();

        state.rebuild_spread_history_with_beta(&empty, &hist_b, 240, 0.7);

        assert!(state.spread_history.is_empty());
        assert_eq!(state.last_spread, None);
    }

    #[test]
    fn rebuild_removes_beta_trajectory_mix() {
        // Reproduce the bot-strategy#274 scenario: push_spread captured
        // each bar with whatever β was active at push time (mixed-β),
        // then we re-evaluate β and rebuild. The rebuilt window should
        // be a pure function of (bars, new β), not of the historical
        // push-time betas.
        let mut state = PairState::new(240, 2.0);
        let hist_a: VecDeque<PriceSample> = (0..6)
            .map(|i| sample(10.0 + i as f64 * 0.1, 1_000 + i))
            .collect();
        let hist_b: VecDeque<PriceSample> = (0..6)
            .map(|i| sample(7.0 + i as f64 * 0.05, 1_000 + i))
            .collect();

        // Simulate mixed-β push history: half pushed with β=0.5, half with
        // β=1.0 (the trajectory bot-strategy#274 observed cross-host).
        for i in 0..3 {
            let s = hist_a[i].log_price - 0.5 * hist_b[i].log_price;
            state.spread_history.push_back(s);
        }
        for i in 3..6 {
            let s = hist_a[i].log_price - 1.0 * hist_b[i].log_price;
            state.spread_history.push_back(s);
        }

        // Now rebuild with the "current" β = 0.8. Every entry should
        // re-evaluate against 0.8, dropping the 0.5/1.0 mix.
        state.rebuild_spread_history_with_beta(&hist_a, &hist_b, 240, 0.8);
        for i in 0..6 {
            let expected = hist_a[i].log_price - 0.8 * hist_b[i].log_price;
            assert_close(state.spread_history[i], expected, 1e-12);
        }
    }

    #[test]
    fn rebuild_leaves_std_history_untouched() {
        // std_history is the std-collapse guard's time series of past
        // full-window stds (bot-strategy#62). Option A explicitly does
        // NOT touch it — the past observations stay even when we
        // reinterpret the spread series under a new β.
        let mut state = PairState::new(240, 2.0);
        state.std_history.extend([0.1, 0.2, 0.3, 0.4]);
        let hist_a: VecDeque<PriceSample> = (0..3)
            .map(|i| sample(10.0 + i as f64, 1_000 + i))
            .collect();
        let hist_b: VecDeque<PriceSample> = (0..3)
            .map(|i| sample(7.0 + i as f64, 1_000 + i))
            .collect();

        state.rebuild_spread_history_with_beta(&hist_a, &hist_b, 240, 0.9);

        assert_eq!(state.std_history.len(), 4);
        assert_eq!(state.std_history.iter().copied().collect::<Vec<_>>(), vec![0.1, 0.2, 0.3, 0.4]);
    }
}
