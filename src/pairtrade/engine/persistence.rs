//! Persistence helpers for `PairTradeEngine`.
//!
//! Bridges `PairTradeEngine` with the on-disk history and risk-state files
//! (`history_io.rs` / `risk_io.rs`) plus the in-memory warm-start that
//! seeds `state.beta` and `state.spread_history` from the loaded log-price
//! history. Pure relocation from the god-module split (#291); no semantic
//! change.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::super::history_io;
use super::super::risk_io;
use super::super::stats::{regression_beta, tail_samples};
use super::super::PairTradeEngine;
use super::super::RISK_ACK_PATH;

impl PairTradeEngine {
    pub(in crate::pairtrade) fn persist_history_to_disk(&self) {
        // Persist the engine's shared log-price history plus the first
        // instance's per-pair `spread_history`. We pick instance 0 as
        // the representative: A/B/C instances drift ≤0.3% per the
        // existing TODO near evaluate_pair, which on reload converges
        // back to whatever was persisted. Persisting per-instance would
        // require an instance ID in the schema — over-engineered for
        // the single-bot-per-process setup this field currently supports.
        let spread_histories: HashMap<String, VecDeque<f64>> = self
            .instances
            .first()
            .map(|inst| {
                inst.states
                    .iter()
                    .map(|(k, s)| (k.clone(), s.spread_history.clone()))
                    .collect()
            })
            .unwrap_or_default();
        history_io::persist_history_to_disk(
            &self.cfg,
            &self.history,
            &spread_histories,
            &self.history_path,
        );
    }

    pub(in crate::pairtrade) fn load_history_from_disk(&mut self) {
        let now = self.current_now_ts();
        let max_len = self.max_history_len();
        let mut loaded_spreads: HashMap<String, VecDeque<f64>> = HashMap::new();
        history_io::load_history_from_disk(
            &self.cfg,
            &mut self.history,
            &mut loaded_spreads,
            &self.history_path,
            now,
            max_len,
        );
        if loaded_spreads.is_empty() {
            return;
        }
        // Apply the persisted spread_history only when the instance's own
        // spread_history is still empty — i.e. on the initial post-restart
        // load, before any ticks have pushed a live spread. Subsequent
        // per-tick loads must NOT clobber the instance's accumulating
        // series, otherwise every step would silently revert the
        // previous step's push (in single-bot mode) or import another
        // bot's beta trajectory (in multi-bot mode, which is not the
        // intended sharing axis — peer bots coordinate on log_prices,
        // not on state.beta-dependent derived series).
        for inst in &mut self.instances {
            for (pair_key, spreads) in &loaded_spreads {
                if let Some(state) = inst.states.get_mut(pair_key) {
                    if state.spread_history.is_empty() {
                        state.last_spread = spreads.back().copied();
                        state.spread_history = spreads.clone();
                    }
                }
            }
        }
    }

    /// Restore circuit-breaker state from disk so a crash or `systemctl
    /// restart` during an active cool-down does not silently clear the
    /// consecutive-loss counter. See bot-strategy#185 Phase 1-3, extended
    /// in Phase 2 to restore `session_start_*` / `realized_pnl_today`.
    ///
    /// On round transition (configured `round_id` differs from the value
    /// persisted in the snapshot), round-bound per-instance fields are
    /// zeroed before assignment so a Round N → N+1 restart starts from a
    /// clean baseline without the operator running `reset-round-state.sh`.
    /// bot-strategy#354.
    pub(in crate::pairtrade) fn load_risk_state(&mut self) {
        if self.cfg.backtest_mode {
            return;
        }
        let mut snapshot = risk_io::load_risk_state(&self.risk_state_path);
        if snapshot.instances.is_empty() {
            return;
        }
        let configured = self.cfg.round_id.as_deref();
        let persisted = snapshot.round_id.as_deref();
        let transition = matches!((configured, persisted),
            (Some(new), Some(old)) if new != old);
        if transition {
            let inst_ids: Vec<String> = snapshot.instances.keys().cloned().collect();
            log::warn!(
                "[ROUND_ID] transition {:?} -> {:?}; resetting round-bound state for instances={:?}",
                persisted.unwrap_or(""),
                configured.unwrap_or(""),
                inst_ids,
            );
            for state in snapshot.instances.values_mut() {
                state.consecutive_losses = 0;
                state.circuit_breaker_until_ts = None;
                state.last_stop_loss_per_pair.clear();
                state.equity_samples.clear();
                state.session_halted = false;
                state.session_halt_reason = None;
                state.session_halt_ts = None;
                state.total_trades = 0;
                state.total_wins = 0;
                state.total_pnl = 0.0;
                state.peak_pnl = 0.0;
                state.max_dd = 0.0;
            }
        }
        let loaded = snapshot.instances;
        let now_ts = self.current_now_ts();
        for inst in &mut self.instances {
            let Some(state) = loaded.get(&inst.id) else { continue };
            inst.consecutive_losses = state.consecutive_losses;
            inst.session_start_equity = state.session_start_equity;
            inst.session_start_ts = state.session_start_ts;
            inst.realized_pnl_today = state.realized_pnl_today;
            inst.equity_samples = state.equity_samples.clone();
            inst.session_halted = state.session_halted;
            inst.session_halt_reason = state.session_halt_reason.clone();
            inst.session_halt_ts = state.session_halt_ts;
            inst.total_trades = state.total_trades;
            inst.total_wins = state.total_wins;
            inst.total_pnl = state.total_pnl;
            inst.peak_pnl = state.peak_pnl;
            inst.max_dd = state.max_dd;
            for (pair_key, mark) in &state.last_stop_loss_per_pair {
                if let Some(pair_state) = inst.states.get_mut(pair_key) {
                    pair_state.last_stop_loss_at = Some((mark.direction, mark.ts));
                    let elapsed = now_ts.saturating_sub(mark.ts).max(0);
                    log::info!(
                        "[STOP_COOLDOWN] {} restored: direction={:?} elapsed={}s",
                        pair_key, mark.direction, elapsed
                    );
                } else {
                    log::debug!(
                        "[STOP_COOLDOWN] {} restore skipped (no PairState yet); will not block re-entry on first eval",
                        pair_key
                    );
                }
            }
            if inst.session_halted {
                log::warn!(
                    "[SESSION_DD] {} restored halt: reason={} since_ts={} (waiting for {} ack)",
                    inst.id,
                    inst.session_halt_reason.as_deref().unwrap_or("unknown"),
                    inst.session_halt_ts.unwrap_or(0),
                    RISK_ACK_PATH
                );
            }
            match state.circuit_breaker_until_ts {
                Some(until_ts) if until_ts > now_ts => {
                    inst.circuit_breaker_until_ts = Some(until_ts);
                    let remaining_secs = (until_ts - now_ts).max(0) as u64;
                    inst.circuit_breaker_until =
                        Some(Instant::now() + Duration::from_secs(remaining_secs));
                    log::warn!(
                        "[RISK_STATE] {} restored: consecutive_losses={}, cool-down {}s remaining, realized_pnl_today={:.4}",
                        inst.id, inst.consecutive_losses, remaining_secs, inst.realized_pnl_today
                    );
                }
                Some(_) => {
                    if inst.consecutive_losses > 0 || inst.realized_pnl_today != 0.0 {
                        log::info!(
                            "[RISK_STATE] {} restored: consecutive_losses={}, prior cool-down expired, realized_pnl_today={:.4}",
                            inst.id, inst.consecutive_losses, inst.realized_pnl_today
                        );
                    }
                }
                None => {
                    if inst.consecutive_losses > 0 || inst.realized_pnl_today != 0.0 {
                        log::info!(
                            "[RISK_STATE] {} restored: consecutive_losses={}, realized_pnl_today={:.4}",
                            inst.id, inst.consecutive_losses, inst.realized_pnl_today
                        );
                    }
                }
            }
        }
    }

    pub(in crate::pairtrade) fn persist_risk_state(&self) {
        if self.cfg.backtest_mode {
            return;
        }
        let instances: HashMap<String, risk_io::InstanceRiskState> = self
            .instances
            .iter()
            .map(|inst| {
                let last_stop_loss_per_pair = inst
                    .states
                    .iter()
                    .filter_map(|(key, st)| {
                        st.last_stop_loss_at
                            .map(|(direction, ts)| {
                                (
                                    key.clone(),
                                    risk_io::StopLossMark { direction, ts },
                                )
                            })
                    })
                    .collect();
                (
                    inst.id.clone(),
                    risk_io::InstanceRiskState {
                        consecutive_losses: inst.consecutive_losses,
                        circuit_breaker_until_ts: inst.circuit_breaker_until_ts,
                        session_start_equity: inst.session_start_equity,
                        session_start_ts: inst.session_start_ts,
                        realized_pnl_today: inst.realized_pnl_today,
                        equity_samples: inst.equity_samples.clone(),
                        session_halted: inst.session_halted,
                        session_halt_reason: inst.session_halt_reason.clone(),
                        session_halt_ts: inst.session_halt_ts,
                        total_trades: inst.total_trades,
                        total_wins: inst.total_wins,
                        total_pnl: inst.total_pnl,
                        peak_pnl: inst.peak_pnl,
                        max_dd: inst.max_dd,
                        last_stop_loss_per_pair,
                    },
                )
            })
            .collect();
        risk_io::persist_risk_state(
            &self.risk_state_path,
            self.cfg.round_id.as_deref(),
            &instances,
        );
    }

    /// Rebuild each pair's beta and spread_history from the shared on-disk
    /// price history so A/B/C bots have identical regression windows the
    /// instant they start, instead of waiting metrics_window live bars to
    /// converge (pairtrade#4). Computes beta directly from whatever bars
    /// are available — does not go through evaluate_pair() because that
    /// path enforces full lookback_hours_long under Strict warm-start and
    /// would skip the seed when the loaded history is shorter than the
    /// configured long window.
    pub(in crate::pairtrade) fn warm_start_states_from_history(&mut self) {
        if self.cfg.disable_history_persist {
            return;
        }
        for inst_idx in 0..self.instances.len() {
            for pair in self.cfg.universe.clone() {
                let key = format!("{}/{}", pair.base, pair.quote);
                let (Some(hist_a), Some(hist_b)) =
                    (self.history.get(&pair.base), self.history.get(&pair.quote))
                else { continue };
                let take = self.cfg.metrics_window.min(hist_a.len()).min(hist_b.len());
                if take < 2 { continue }
                let tail_a = tail_samples(hist_a, take);
                let tail_b = tail_samples(hist_b, take);
                let beta = regression_beta(&tail_b, &tail_a);
                let Some(state) = self.instances[inst_idx].states.get_mut(&key) else { continue };
                state.beta = beta;
                state.beta_short = beta;
                state.beta_long = beta;
                // If `load_history_from_disk` / `load_history_snapshot_for_bt`
                // has already restored the real persisted `spread_history`
                // (v2 snapshot), keep it as-is. Synthesizing a
                // single-OLS-beta series here would overwrite a 240-bar
                // real series with one whose variance is artificially
                // compressed — the mechanism behind the 2026-04-15 06:02
                // UTC "std collapse" restart incident (bot-strategy#62).
                // Only synthesize when the instance has no live spreads
                // (fresh start with no persisted snapshot, or a v1
                // snapshot from a pre-fix bot).
                if state.spread_history.is_empty() {
                    let spreads: VecDeque<f64> = tail_a
                        .iter()
                        .zip(tail_b.iter())
                        .map(|(sa, sb)| sa.log_price - beta * sb.log_price)
                        .collect();
                    state.last_spread = spreads.back().copied();
                    state.spread_history = spreads;
                    log::info!(
                        "[WARM_START] {} synthesized spread_history len={} beta={:.4} (no persisted v2 series)",
                        key, state.spread_history.len(), state.beta
                    );
                } else {
                    log::info!(
                        "[WARM_START] {} kept persisted spread_history len={} beta={:.4} (no synthesis)",
                        key, state.spread_history.len(), state.beta
                    );
                }
            }
        }
    }
}
