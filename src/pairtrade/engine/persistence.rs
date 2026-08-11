//! Persistence helpers for `PairTradeEngine`.
//!
//! Bridges `PairTradeEngine` with the on-disk history and risk-state files
//! (`history_io.rs` / `risk_io.rs`) plus the in-memory warm-start that seeds
//! `per_pair_state[*].beta` and `per_pair_state[*].spread_history` from the
//! loaded log-price history. Post-bot-strategy#413 the snapshot is owned at
//! engine level (single source of truth per pair), and the loader also
//! restores the committed β and Kalman filter state so a restart preserves
//! the live β trajectory instead of reverting to a fresh OLS warm-start.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::super::history_io::{self, KalmanSnapshot};
use super::super::kalman::KalmanBeta;
use super::super::risk_ack_path;
use super::super::risk_io;
use super::super::stats::{regression_beta, tail_samples};
use super::super::PairTradeEngine;

impl PairTradeEngine {
    pub(in crate::pairtrade) fn persist_history_to_disk(&self) {
        // bot-strategy#413: spread_history, β and Kalman state are now
        // owned at engine level (single source of truth), so we read
        // straight from `self.per_pair_state` instead of cherry-picking
        // instance[0]. A/B/C now share these by construction.
        let spread_histories: HashMap<String, VecDeque<f64>> = self
            .per_pair_state
            .iter()
            .map(|(k, s)| (k.clone(), s.spread_history.clone()))
            .collect();
        let betas: HashMap<String, f64> = self
            .per_pair_state
            .iter()
            .map(|(k, s)| (k.clone(), s.beta))
            .collect();
        let kalman_states: HashMap<String, KalmanSnapshot> = self
            .per_pair_state
            .iter()
            .filter_map(|(k, s)| {
                s.kalman.as_ref().map(|kf| {
                    (
                        k.clone(),
                        KalmanSnapshot {
                            beta: kf.beta,
                            p: kf.p,
                            updates: kf.updates,
                        },
                    )
                })
            })
            .collect();
        history_io::persist_history_to_disk(
            &self.cfg,
            &self.history,
            &spread_histories,
            &betas,
            &kalman_states,
            &self.history_path,
        );
    }

    pub(in crate::pairtrade) fn load_history_from_disk(&mut self) {
        let now = self.current_now_ts();
        let max_len = self.max_history_len();
        let mut loaded_spreads: HashMap<String, VecDeque<f64>> = HashMap::new();
        let mut loaded_betas: HashMap<String, f64> = HashMap::new();
        let mut loaded_kalman: HashMap<String, KalmanSnapshot> = HashMap::new();
        history_io::load_history_from_disk(history_io::LoadHistoryRequest {
            cfg: &self.cfg,
            history: &mut self.history,
            spread_histories_out: &mut loaded_spreads,
            betas_out: &mut loaded_betas,
            kalman_states_out: &mut loaded_kalman,
            history_path: &self.history_path,
            now_ts: now,
            max_history_len: max_len,
            last_logged_key: &mut self.last_warm_start_key,
        });
        if loaded_spreads.is_empty() && loaded_betas.is_empty() && loaded_kalman.is_empty() {
            return;
        }
        // Apply the persisted state only when the engine's own series is
        // still empty — i.e. on the initial post-restart load, before any
        // ticks have pushed a live spread. Subsequent per-tick loads must
        // NOT clobber the engine's accumulating series, otherwise every
        // step would silently revert the previous step's push.
        let kalman_q = self.cfg.kalman_q;
        let kalman_r = self.cfg.kalman_r;
        for (pair_key, spreads) in &loaded_spreads {
            if let Some(shared) = self.per_pair_state.get_mut(pair_key) {
                if shared.spread_history.is_empty() {
                    shared.last_spread = spreads.back().copied();
                    shared.spread_history = spreads.clone();
                }
            }
        }
        for (pair_key, beta) in &loaded_betas {
            if let Some(shared) = self.per_pair_state.get_mut(pair_key) {
                // Only seed on a fresh boot: β=1.0 is the engine's
                // default-init value (`PairSharedState::new`), so a value
                // other than 1.0 means a later eval already overwrote it
                // and we must not roll it back.
                if (shared.beta - 1.0).abs() < f64::EPSILON {
                    shared.beta = *beta;
                }
            }
        }
        for (pair_key, kalman) in &loaded_kalman {
            if let Some(shared) = self.per_pair_state.get_mut(pair_key) {
                // Skip if a live Kalman filter is already collecting
                // updates (engine has been running for a while); only
                // restore when the field is at its post-construction
                // state.
                let already_warm = shared
                    .kalman
                    .as_ref()
                    .map(|kf| kf.updates > 0)
                    .unwrap_or(false);
                if !already_warm {
                    shared.kalman = Some(KalmanBeta::from_snapshot(
                        kalman.beta,
                        kalman.p,
                        kalman.updates,
                        kalman_q,
                        kalman_r,
                    ));
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
        let persisted_for_log = snapshot.round_id.clone();
        if snapshot.apply_round_transition(configured) {
            let inst_ids: Vec<String> = snapshot.instances.keys().cloned().collect();
            log::warn!(
                "[ROUND_ID] transition {:?} -> {:?}; resetting round-bound state for instances={:?}",
                persisted_for_log.as_deref().unwrap_or(""),
                configured.unwrap_or(""),
                inst_ids,
            );
        }
        let loaded = snapshot.instances;
        let now_ts = self.current_now_ts();
        for inst in &mut self.instances {
            let Some(state) = loaded.get(&inst.id) else {
                continue;
            };
            inst.consecutive_losses = state.consecutive_losses;
            inst.session_start_equity = state.session_start_equity;
            // Zero is the serde default for snapshots written before #752.
            // Preserve it as an explicit migration-pending sentinel: the old
            // reference is unknowable, so stamping the current config here
            // would falsely mark a stale denominator as reconciled forever.
            // `detect_capital_event_and_rebaseline` resolves it only after a
            // flat/settled live-equity observation.
            inst.session_equity_reference_usd = state.session_equity_reference_usd;
            inst.session_start_ts = state.session_start_ts;
            inst.realized_pnl_today = state.realized_pnl_today;
            inst.funding_carry_today = state.funding_carry_today;
            inst.total_funding_carry = state.total_funding_carry;
            inst.equity_samples = state.equity_samples.clone();
            inst.capital_baseline_equity = state.capital_baseline_equity;
            inst.capital_baseline_accounted_pnl = state.capital_baseline_accounted_pnl;
            inst.capital_position_seen_since_baseline = state.capital_position_seen_since_baseline;
            inst.capital_rebaseline_deferred = false;
            inst.capital_rebaseline_deferred_since = None;
            inst.capital_guard_equity_snapshot = None;
            inst.session_halted = state.session_halted;
            inst.session_halt_reason = state.session_halt_reason.clone();
            inst.session_halt_ts = state.session_halt_ts;
            inst.entry_blocked_pairs = state.entry_blocked_pairs.clone();
            inst.total_trades = state.total_trades;
            inst.total_wins = state.total_wins;
            inst.total_pnl = state.total_pnl;
            inst.peak_pnl = state.peak_pnl;
            inst.max_dd = state.max_dd;
            if inst.session_equity_reference_usd <= 0.0 {
                log::warn!(
                    "[DAILY_DD] {} legacy snapshot has no equity reference; pending flat/settled reconciliation to {:.2}, preserving session_start_equity={:.2}",
                    inst.id,
                    inst.equity_reference_usd,
                    inst.session_start_equity,
                );
            } else if (inst.session_equity_reference_usd - inst.equity_reference_usd).abs() > 1e-9 {
                log::warn!(
                    "[DAILY_DD] {} equity reference change {:.2} -> {:.2} pending flat/settled reconciliation; preserving session_start_equity={:.2}",
                    inst.id,
                    inst.session_equity_reference_usd,
                    inst.equity_reference_usd,
                    inst.session_start_equity,
                );
            }
            // bot-strategy#469: surface the persisted lifetime totals on the
            // dashboard immediately. Without this seed the status reporter's
            // `trade_stats` stays at its `Some(zeros)` initial value until
            // `write_pnl_record` fires for the first post-restart trade — so
            // a variant that doesn't trade for hours (e.g. under a raised
            // entry_z threshold) looks freshly-zeroed even though the on-disk
            // state has the real counts.
            if let Some(reporter) = inst.status_reporter.as_mut() {
                reporter.set_trade_stats_totals(
                    inst.total_trades,
                    inst.total_wins,
                    inst.total_pnl,
                    inst.max_dd,
                );
            }
            for (pair_key, mark) in &state.last_stop_loss_per_pair {
                if let Some(pair_state) = inst.states.get_mut(pair_key) {
                    pair_state.last_stop_loss_at = Some((mark.direction, mark.ts));
                    let elapsed = now_ts.saturating_sub(mark.ts).max(0);
                    log::info!(
                        "[STOP_COOLDOWN] {} restored: direction={:?} elapsed={}s",
                        pair_key,
                        mark.direction,
                        elapsed
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
                    risk_ack_path()
                );
            }
            for (pair_key, reason) in &inst.entry_blocked_pairs {
                log::warn!(
                    "[ENTRY_RECONCILE] {} {} restored entry block: reason={} (waiting for {} ack)",
                    inst.id,
                    pair_key,
                    reason,
                    risk_ack_path()
                );
                super::super::prom::ENTRY_EXPOSURE_BLOCKED
                    .with_label_values(&[inst.id.as_str(), pair_key.as_str()])
                    .set(1);
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
                        st.last_stop_loss_at.map(|(direction, ts)| {
                            (key.clone(), risk_io::StopLossMark { direction, ts })
                        })
                    })
                    .collect();
                (
                    inst.id.clone(),
                    risk_io::InstanceRiskState {
                        consecutive_losses: inst.consecutive_losses,
                        circuit_breaker_until_ts: inst.circuit_breaker_until_ts,
                        session_start_equity: inst.session_start_equity,
                        session_equity_reference_usd: inst.session_equity_reference_usd,
                        session_start_ts: inst.session_start_ts,
                        realized_pnl_today: inst.realized_pnl_today,
                        funding_carry_today: inst.funding_carry_today,
                        total_funding_carry: inst.total_funding_carry,
                        equity_samples: inst.equity_samples.clone(),
                        capital_baseline_equity: inst.capital_baseline_equity,
                        capital_baseline_accounted_pnl: inst.capital_baseline_accounted_pnl,
                        capital_position_seen_since_baseline: inst
                            .capital_position_seen_since_baseline,
                        session_halted: inst.session_halted,
                        session_halt_reason: inst.session_halt_reason.clone(),
                        session_halt_ts: inst.session_halt_ts,
                        total_trades: inst.total_trades,
                        total_wins: inst.total_wins,
                        total_pnl: inst.total_pnl,
                        peak_pnl: inst.peak_pnl,
                        max_dd: inst.max_dd,
                        last_stop_loss_per_pair,
                        entry_blocked_pairs: inst.entry_blocked_pairs.clone(),
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
    /// price history so the bot has a populated regression window the
    /// instant it starts, instead of waiting metrics_window live bars to
    /// converge (pairtrade#4). Computes beta directly from whatever bars
    /// are available — does not go through evaluate_pair() because that
    /// path enforces full lookback_hours_long under Strict warm-start and
    /// would skip the seed when the loaded history is shorter than the
    /// configured long window.
    ///
    /// Post-bot-strategy#413: operates on engine-level `per_pair_state`
    /// rather than per-instance, so there's a single β / spread_history
    /// per pair shared across A/B/C.
    pub(in crate::pairtrade) fn warm_start_states_from_history(&mut self) {
        if self.cfg.disable_history_persist {
            return;
        }
        for pair in self.cfg.universe.clone() {
            let key = format!("{}/{}", pair.base, pair.quote);
            let (Some(hist_a), Some(hist_b)) =
                (self.history.get(&pair.base), self.history.get(&pair.quote))
            else {
                continue;
            };
            let take = self.cfg.metrics_window.min(hist_a.len()).min(hist_b.len());
            if take < 2 {
                continue;
            }
            let tail_a = tail_samples(hist_a, take);
            let tail_b = tail_samples(hist_b, take);
            let beta = regression_beta(&tail_b, &tail_a);
            let Some(shared) = self.per_pair_state.get_mut(&key) else {
                continue;
            };
            shared.beta = beta;
            shared.beta_short = beta;
            shared.beta_long = beta;
            // If `load_history_from_disk` / `load_history_snapshot_for_bt`
            // has already restored the real persisted `spread_history`
            // (v2+ snapshot), keep it as-is. Synthesizing a
            // single-OLS-beta series here would overwrite a 240-bar
            // real series with one whose variance is artificially
            // compressed — the mechanism behind the 2026-04-15 06:02
            // UTC "std collapse" restart incident (bot-strategy#62).
            // Only synthesize when the engine has no live spreads
            // (fresh start with no persisted snapshot, or a v1
            // snapshot from a pre-fix bot).
            if shared.spread_history.is_empty() {
                let spreads: VecDeque<f64> = tail_a
                    .iter()
                    .zip(tail_b.iter())
                    .map(|(sa, sb)| sa.log_price - beta * sb.log_price)
                    .collect();
                shared.last_spread = spreads.back().copied();
                shared.spread_history = spreads;
                log::info!(
                    "[WARM_START] {} synthesized spread_history len={} beta={:.4} (no persisted v2 series)",
                    key, shared.spread_history.len(), shared.beta
                );
            } else {
                log::info!(
                    "[WARM_START] {} kept persisted spread_history len={} beta={:.4} (no synthesis)",
                    key, shared.spread_history.len(), shared.beta
                );
            }
        }
    }
}
