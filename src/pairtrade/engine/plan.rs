//! Per-instance trade action planning for the pairtrade engine.
//!
//! Extracted from `engine/step.rs` (bot-strategy#443): this module owns the
//! `PlannedAction` / `TradeAction` model and `step_plan_pair_actions`, which
//! turns the prepared per-tick state into the set of open/close actions the
//! executor later places. `dispatch_rehedge` lives here too since it is only
//! driven from the planning pass. Behaviour is unchanged from the prior
//! in-`step.rs` implementation.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use anyhow::{anyhow, Result};
use rust_decimal::Decimal;

use super::super::apply_post_exit_state;
use super::super::config::PairSpec;
use super::super::entry::{entry_z_for_pair, should_enter, EntryCheck};
use super::super::exit::{exit_reason, risk_exit_reason, ExitCheck};
use super::super::market::{
    ineligible_close_book_degraded, liquidity_score, net_funding_for_direction, SymbolSnapshot,
};
use super::super::state::PositionDirection;
use super::super::stats::spread_slope_sigma;
use super::super::PairTradeEngine;
use super::gating::StepSetup;

pub(super) struct PlannedAction {
    pub(super) pair: PairSpec,
    pub(super) key: String,
    pub(super) action: TradeAction,
    pub(super) net_funding_per_hour: f64,
    pub(super) abs_z: f64,
    pub(super) liquidity_score: f64,
    pub(super) p1: SymbolSnapshot,
    pub(super) p2: SymbolSnapshot,
}

pub(super) enum TradeAction {
    Open {
        direction: PositionDirection,
        z: f64,
        beta: f64,
    },
    Close {
        direction: PositionDirection,
        z: f64,
        beta: f64,
        force: bool,
    },
    None,
}

impl PairTradeEngine {
    /// Phase 2 of bot-strategy#463: dispatch a re-hedge order for an
    /// open position whose β has drifted past the configured threshold.
    ///
    /// Behaviour by mode (cascade):
    ///   1. Skip if `pending_entry` / `pending_exit` is in flight — we
    ///      do not want to fight the position state machine.
    ///   2. Plan the one-sided leg-B order via `plan_rehedge_order`.
    ///      Skip if planning returns `None` (missing entry sizes).
    ///   3. **Dry-run / backtest**: simulate the fill synchronously —
    ///      update `Position::entry_size_b`, `entry_beta`, and
    ///      `last_rehedge_ts` in place; increment counter with
    ///      `mode="dry_run"`. This is what BT measures.
    ///   4. **Live, `rehedge_live_enabled=true`**: place a taker order
    ///      via `connector::create_order`, await the response, then
    ///      apply the same Position-state update with the
    ///      venue-reported filled qty; counter `mode="live"`.
    ///   5. **Live, `rehedge_live_enabled=false`**: log
    ///      `[REHEDGE_LIVE_DISABLED]` and return — preserves the
    ///      Phase 1 detect-only behaviour.
    ///
    /// Returns `Ok(())` on every branch including the disabled-live
    /// path; only an unexpected internal error (state missing, bad
    /// plan) bubbles up.
    async fn dispatch_rehedge(
        &mut self,
        inst_idx: usize,
        key: &str,
        pair: &crate::pairtrade::config::PairSpec,
        current_beta: f64,
        current_price_b: Decimal,
        now_ts: i64,
    ) -> Result<()> {
        // Guard: no in-flight pair-level orders.
        let (has_pending_entry, has_pending_exit, has_pending_rehedge) = {
            let state = self.instances[inst_idx]
                .states
                .get(key)
                .ok_or_else(|| anyhow!("missing state for {}", key))?;
            (
                state.pending_entry.is_some(),
                state.pending_exit.is_some(),
                state.pending_rehedge.is_some(),
            )
        };
        if has_pending_entry || has_pending_exit || has_pending_rehedge {
            log::info!(
                "[REHEDGE_SKIP] variant={} pair={} reason=pending_order_in_flight \
                 entry={} exit={} rehedge={}",
                self.instances[inst_idx].id,
                key,
                has_pending_entry,
                has_pending_exit,
                has_pending_rehedge,
            );
            return Ok(());
        }

        // Plan the order shape.
        let plan = {
            let position = self.instances[inst_idx]
                .states
                .get(key)
                .and_then(|s| s.position.as_ref())
                .cloned();
            match position
                .as_ref()
                .and_then(|pos| crate::pairtrade::rehedge::plan_rehedge_order(pos, current_beta))
            {
                Some(p) => p,
                None => {
                    log::info!(
                        "[REHEDGE_SKIP] variant={} pair={} reason=plan_unavailable",
                        self.instances[inst_idx].id,
                        key,
                    );
                    return Ok(());
                }
            }
        };

        let pp = self.pair_params_for(inst_idx, key).clone();
        let mode_label = if self.cfg.backtest_mode || self.cfg.dry_run {
            "dry_run"
        } else if pp.rehedge_live_enabled {
            "live"
        } else {
            // Live + opt-in flag off → preserve Phase 1 behaviour.
            log::info!(
                "[REHEDGE_LIVE_DISABLED] variant={} pair={} side={:?} qty={} new_size_b={} \
                 (set rehedge_live_enabled=true to opt in)",
                self.instances[inst_idx].id,
                key,
                plan.side,
                plan.qty,
                plan.expected_new_entry_size_b,
            );
            return Ok(());
        };

        // Live: actually submit the taker order.
        if mode_label == "live" {
            match self
                .connector
                .create_order(&pair.quote, plan.qty, plan.side, None, None, false, None)
                .await
            {
                Ok(resp) => {
                    log::info!(
                        "[REHEDGE_LIVE] variant={} pair={} side={:?} qty={} order_id={} new_size_b={}",
                        self.instances[inst_idx].id,
                        key,
                        plan.side,
                        plan.qty,
                        resp.order_id,
                        plan.expected_new_entry_size_b,
                    );
                }
                Err(e) => {
                    log::warn!(
                        "[REHEDGE_LIVE_FAIL] variant={} pair={} side={:?} qty={} error={:?} — position state unchanged",
                        self.instances[inst_idx].id,
                        key,
                        plan.side,
                        plan.qty,
                        e,
                    );
                    return Ok(());
                }
            }
        }

        // Update Position state. The accounting depends on whether the
        // re-hedge GROWS or SHRINKS the position:
        //   * Grow: new quote units priced at `current_price_b`. Update
        //     `entry_price_b` to the volume-weighted average so the
        //     final-close PnL uses the right cost basis.
        //   * Shrink: close `|delta|` quote units NOW at `current_price_b`,
        //     realize the partial PnL into `rehedge_realized_pnl`, and
        //     keep `entry_price_b` unchanged for the remaining units.
        // `entry_beta` resets to the post-rehedge β so subsequent drift
        // detection anchors here.
        let (new_entry_price_b, realized_delta) = {
            let pos = self.instances[inst_idx]
                .states
                .get(key)
                .and_then(|s| s.position.as_ref())
                .cloned();
            match pos {
                Some(p) => {
                    let old_size = p.entry_size_b.unwrap_or(Decimal::ZERO);
                    let old_price = p.entry_price_b.unwrap_or(current_price_b);
                    let new_size = plan.expected_new_entry_size_b;
                    if new_size > old_size {
                        let grew = new_size - old_size;
                        // VWAP
                        let new_price = if new_size > Decimal::ZERO {
                            (old_size * old_price + grew * current_price_b) / new_size
                        } else {
                            current_price_b
                        };
                        (new_price, Decimal::ZERO)
                    } else {
                        // shrink — realize partial PnL on the closed portion
                        let closed = old_size - new_size;
                        let sign = match p.direction {
                            crate::pairtrade::state::PositionDirection::LongSpread => Decimal::ONE, // short leg
                            crate::pairtrade::state::PositionDirection::ShortSpread => {
                                -Decimal::ONE
                            } // long leg
                        };
                        let realized = sign * (old_price - current_price_b) * closed;
                        (old_price, realized)
                    }
                }
                None => (current_price_b, Decimal::ZERO),
            }
        };
        if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
            if let Some(pos) = state.position.as_mut() {
                pos.entry_size_b = Some(plan.expected_new_entry_size_b);
                pos.entry_price_b = Some(new_entry_price_b);
                pos.entry_beta = Some(current_beta);
                pos.last_rehedge_ts = Some(now_ts);
                let acc = pos.rehedge_realized_pnl.unwrap_or(Decimal::ZERO) + realized_delta;
                pos.rehedge_realized_pnl = Some(acc);
            }
        }
        log::info!(
            "[REHEDGE_EXECUTED] variant={} pair={} mode={} side={:?} qty={} new_size_b={} new_entry_price_b={} realized_delta={} new_entry_beta={:.4}",
            self.instances[inst_idx].id,
            key,
            mode_label,
            plan.side,
            plan.qty,
            plan.expected_new_entry_size_b,
            new_entry_price_b,
            realized_delta,
            current_beta,
        );
        crate::pairtrade::prom::REHEDGE_EXECUTED_TOTAL
            .with_label_values(&[self.instances[inst_idx].id.as_str(), key, mode_label])
            .inc();
        Ok(())
    }

    pub(super) async fn step_plan_pair_actions(
        &mut self,
        inst_idx: usize,
        price_map: &HashMap<String, SymbolSnapshot>,
        updated: &HashSet<String>,
        setup: StepSetup,
        now_ts: i64,
    ) -> Result<Vec<PlannedAction>> {
        let StepSetup {
            maintenance_block_entries,
            vol_median,
            regime_ok,
            positions_clear,
        } = setup;
        let mut planned: Vec<PlannedAction> = Vec::new();

        let universe = self.cfg.universe.clone();
        for pair in &universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            let (p1, p2) = match (price_map.get(&pair.base), price_map.get(&pair.quote)) {
                (Some(a), Some(b)) => (a, b),
                _ => continue,
            };
            // Resolve BT deferred exits whose fill delay has elapsed
            // (bot-strategy#69). Must run before reconcile so the position
            // is cleared before entry evaluation on the same tick.
            //
            // bot-strategy#306 B-3a: moved out of the bar-tick gate below so
            // BT fill-delay timing tracks `interval_secs` (~5s) rather than
            // `trading_period_secs` (~60s). No-op in live where
            // `bt_fill_delay_secs == 0`.
            if self.cfg.bt_fill_delay_secs > 0 {
                let inst_id = self.instances[inst_idx].id.clone();
                if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                    if let Some(ref deferred) = state.bt_deferred_exit {
                        if now_ts >= deferred.resolve_at_ts {
                            log::debug!(
                                "[BT_FILL_DELAY] {} resolved (delay={}s, now_ts={})",
                                key,
                                self.cfg.bt_fill_delay_secs,
                                now_ts
                            );
                            // The deferred exit is the resolution of an
                            // earlier exit decision; pending_exit_reason
                            // is still set on state from that decision.
                            // bot-strategy#316.
                            let dir = state
                                .position
                                .as_ref()
                                .map(|p| p.direction)
                                .unwrap_or(PositionDirection::LongSpread);
                            apply_post_exit_state(
                                state,
                                self.per_pair_state.get(&key),
                                dir,
                                now_ts,
                                &inst_id,
                                key.as_str(),
                            );
                            state.bt_deferred_exit = None;
                        }
                    }
                }
            }

            // Reconcile every tick (interval_secs ~5s) instead of only on
            // bar completion (trading_period_secs ~60s). bot-strategy#306
            // B-3a: WS pushes fills into the dex-connector cache within ~1s,
            // but pairtrade used to sleep up to 60s before acting on them,
            // dominating exit leg-sync. Both `get_open_orders` and
            // `get_filled_orders` already read from in-memory caches for
            // Lighter and Extended, so the higher cadence does not introduce
            // REST traffic.
            self.reconcile_pending_orders(inst_idx, &key, price_map)
                .await?;

            if !(updated.contains(&pair.base) && updated.contains(&pair.quote)) {
                continue;
            }

            let mut action = TradeAction::None;

            // Read pair-level shared state (bot-strategy#413). β / spread /
            // z were already computed once in step_pair_shared, so every
            // variant on this pair observes the same values.
            let Some(shared_snap) = self.per_pair_state.get(&key).map(|s| {
                (
                    s.z_score_details(),
                    s.last_evaluated_ts,
                    s.spread_history.len(),
                    s.last_velocity_sigma_per_min,
                    s.beta,
                    s.beta_short,
                    s.beta_long,
                    s.eligible,
                )
            }) else {
                continue;
            };
            let (
                z_snapshot,
                last_eval_ts,
                spread_len,
                _velocity,
                beta_eff,
                beta_short,
                beta_long,
                eligible_shared,
            ) = shared_snap;
            let position_state = self.instances[inst_idx]
                .states
                .get(&key)
                .and_then(|s| s.position.clone());

            // bot-strategy#531: any eligible tick resets the ineligible-close
            // deferral timer, so a later ineligibility spell starts a fresh
            // deferral window instead of inheriting a stale start timestamp.
            if eligible_shared {
                if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                    state.ineligible_defer_since_ts = None;
                }
            }

            let pp = self.pair_params_for(inst_idx, &key).clone();
            let pp = &pp;
            let force_close_due = position_state
                .as_ref()
                .map(|pos| now_ts.saturating_sub(pos.entered_ts) >= pp.force_close_secs as i64)
                .unwrap_or(false);
            if force_close_due {
                if let Some(pos) = &position_state {
                    log::info!("[EXIT_CHECK] {} reason=force_close", key);
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                        state.pending_exit_reason = Some("force_close");
                    }
                    action = TradeAction::Close {
                        direction: pos.direction,
                        z: 0.0,
                        beta: beta_eff,
                        force: true,
                    };
                }
            }
            // Maintenance pre-flatten: the entry-blocker only suppresses Open
            // actions, so a position whose remaining hold (force_close_secs −
            // age) extends past the next maintenance window would otherwise
            // ride through the venue outage. We reuse the existing bool window
            // API; granularity is 1h, so we may close up to ~1h earlier than
            // strictly required.
            //
            // The lookahead is capped at the entry-block horizon (1h, see
            // gating.rs `maintenance_status(1)`). Without the cap, a fresh
            // position with a large force_close_secs (variant B is now 21600s
            // = 6h) queries `is_upcoming_maintenance(6)` and preempt-closes up
            // to 6h early; since maintenance_preempt is not a stop, only the
            // generic ~60s cooldown gates re-entry, so the position re-enters
            // and re-preempts, paying market-taker slippage repeatedly for
            // hours before a possibly-short window. Capping at 1h aligns the
            // preempt with the entry block: once inside 1h new entries are
            // blocked too (no re-entry thrash), and 1h is ample to market-close
            // before the outage.
            if !force_close_due {
                if let Some(pos) = &position_state {
                    let position_age = now_ts.saturating_sub(pos.entered_ts).max(0);
                    let remaining_hold = (pp.force_close_secs as i64).saturating_sub(position_age);
                    if remaining_hold > 0 {
                        const MAINT_PREEMPT_MAX_LOOKAHEAD_HOURS: i64 = 1;
                        let hours_to_check = ((remaining_hold + 3599) / 3600)
                            .max(1)
                            .min(MAINT_PREEMPT_MAX_LOOKAHEAD_HOURS);
                        if self.connector.is_upcoming_maintenance(hours_to_check).await {
                            log::info!(
                                "[EXIT_CHECK] {} reason=maintenance_preempt remaining_hold_s={} window_h={}",
                                key, remaining_hold, hours_to_check
                            );
                            if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                                state.pending_exit_reason = Some("maintenance_preempt");
                            }
                            action = TradeAction::Close {
                                direction: pos.direction,
                                z: 0.0,
                                beta: beta_eff,
                                force: true,
                            };
                        }
                    }
                }
            }

            if self.instances[inst_idx].states[&key]
                .pending_entry
                .is_some()
                || self.instances[inst_idx].states[&key].pending_exit.is_some()
                || self.instances[inst_idx].states[&key]
                    .bt_deferred_exit
                    .is_some()
            {
                if !matches!(action, TradeAction::None) {
                    log::debug!("[ORDER] {} has pending orders; skipping new actions", key);
                }
                continue;
            }
            if self.instances[inst_idx].states[&key].position_guard
                && matches!(action, TradeAction::None)
            {
                if self.should_log_position_warn(&key) {
                    log::warn!(
                        "[POSITION] {} in unhedged/mismatch state; skipping new actions",
                        key
                    );
                    self.last_position_warn.insert(key.clone(), Instant::now());
                }
                continue;
            }

            let mut log_positions_not_ready = false;
            let circuit_breaker_until_ts_snapshot =
                self.instances[inst_idx].circuit_breaker_until_ts;
            let kill_switch_active_snapshot = self.kill_switch_active;
            let daily_loss_blocks_snapshot = self.daily_loss_blocks(&self.instances[inst_idx]);
            let session_halted_snapshot = self.instances[inst_idx].session_halted;
            let consecutive_losses_snapshot = self.instances[inst_idx].consecutive_losses;
            let equity_reference_snapshot = self.instances[inst_idx].equity_reference_usd;

            // Refresh the per-instance entry_z threshold from the shared
            // β_gap and the variant's entry_z_score_base/min/max overlay
            // (bot-strategy#411 fix). Variant differences land here —
            // β / spread / z themselves are shared.
            let z_entry = self
                .per_pair_state
                .get(&key)
                .map(|shared| entry_z_for_pair(&self.cfg, pp, shared, vol_median))
                .unwrap_or(pp.entry_z_base);
            if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                state.z_entry = z_entry;
            }

            let min_points = (self.cfg.metrics_window / 2).max(10);
            if matches!(action, TradeAction::None) {
                if eligible_shared && spread_len >= min_points {
                    if let Some((z, std, mean, latest_spread)) = z_snapshot {
                        let net_funding = net_funding_for_direction(z, p1, p2);
                        let position_open = self.instances[inst_idx]
                            .states
                            .get(&key)
                            .map(|s| s.position.is_some())
                            .unwrap_or(false);
                        if position_open {
                            // bot-strategy#463 Phase 1: detect β-drift
                            // requiring a re-hedge. No order placed yet —
                            // we log + count so operators can tune
                            // threshold / cooldown / min-notional from
                            // live data before Phase 2 enables the actual
                            // re-balance. Disabled by default
                            // (`rehedge_drift_threshold_pct=0`).
                            let current_beta =
                                self.per_pair_state.get(&key).map(|s| s.beta).unwrap_or(0.0);
                            let rehedge_decision = {
                                let state = self.instances[inst_idx]
                                    .states
                                    .get(&key)
                                    .ok_or_else(|| anyhow!("missing state for {}", key))?;
                                // #465: pass current z for the optional
                                // no-revert gate. z is already in scope
                                // from the `z_snapshot` destructure above.
                                state.position.as_ref().and_then(|pos| {
                                    super::super::rehedge::should_rehedge(
                                        pp,
                                        pos,
                                        current_beta,
                                        Some(z),
                                        pp.force_close_secs,
                                        now_ts,
                                    )
                                })
                            };
                            // bot-strategy#465 Option B: refresh
                            // `prev_beta_for_velocity` AFTER evaluating
                            // so the NEXT tick's velocity gate has a
                            // real per-tick interval. Independent of
                            // whether the gate fires this tick.
                            if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                                if let Some(pos) = state.position.as_mut() {
                                    pos.prev_beta_for_velocity = Some((current_beta, now_ts));
                                }
                            }
                            if let Some(dec) = rehedge_decision {
                                log::info!(
                                    "[REHEDGE_NEEDED] variant={} pair={} entry_beta={:.4} current_beta={:.4} drift_pct={:.4} swing_usd={:.2}",
                                    self.instances[inst_idx].id,
                                    key,
                                    dec.entry_beta,
                                    dec.current_beta,
                                    dec.drift_pct,
                                    dec.notional_swing_usd,
                                );
                                crate::pairtrade::prom::REHEDGE_NEEDED_TOTAL
                                    .with_label_values(&[
                                        self.instances[inst_idx].id.as_str(),
                                        key.as_str(),
                                    ])
                                    .inc();
                                // bot-strategy#463 Phase 2: dispatch the
                                // actual re-hedge if no other order is in
                                // flight on this position. Dry-run / BT
                                // always simulate; live opt-in via
                                // `rehedge_live_enabled` (default false,
                                // safety gate).
                                let current_price_b = price_map
                                    .get(&pair.quote)
                                    .map(|s| s.price)
                                    .unwrap_or(Decimal::ZERO);
                                self.dispatch_rehedge(
                                    inst_idx,
                                    &key,
                                    pair,
                                    dec.current_beta,
                                    current_price_b,
                                    now_ts,
                                )
                                .await
                                .unwrap_or_else(|e| {
                                    log::warn!(
                                        "[REHEDGE_DISPATCH] variant={} pair={} failed: {}",
                                        self.instances[inst_idx].id,
                                        key,
                                        e
                                    );
                                });
                            }
                            let equity_base = equity_reference_snapshot;
                            let reason_opt = {
                                let state = self.instances[inst_idx]
                                    .states
                                    .get(&key)
                                    .ok_or_else(|| anyhow!("missing state for {}", key))?;
                                let shared = self
                                    .per_pair_state
                                    .get(&key)
                                    .ok_or_else(|| anyhow!("missing shared state for {}", key))?;
                                exit_reason(ExitCheck {
                                    cfg: &self.cfg,
                                    pp,
                                    state,
                                    shared,
                                    z,
                                    std,
                                    p1,
                                    p2,
                                    equity_base,
                                    now_ts,
                                })
                            };
                            if let Some(reason) = reason_opt {
                                let velocity_for_log = self
                                    .per_pair_state
                                    .get(&key)
                                    .map(|s| s.last_velocity_sigma_per_min)
                                    .unwrap_or(0.0);
                                log::info!(
                                    "[EXIT_CHECK] {} reason={} z={:.2} exit_z={:.2} stop_z={:.2} vel={:.3} max_vel={:.3}",
                                    key,
                                    reason,
                                    z,
                                    pp.exit_z,
                                    pp.stop_loss_z,
                                    velocity_for_log,
                                    pp.spread_velocity_max_sigma_per_min
                                );
                                let direction = self.instances[inst_idx]
                                    .states
                                    .get(&key)
                                    .and_then(|s| s.position.as_ref().map(|p| p.direction))
                                    .unwrap_or(PositionDirection::LongSpread);
                                if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                                    // Stash the reason so the exit-fill site
                                    // can tag last_stop_loss_at without
                                    // plumbing the reason through TradeAction
                                    // + PendingOrders. bot-strategy#316.
                                    state.pending_exit_reason = Some(reason);
                                }
                                action = TradeAction::Close {
                                    direction,
                                    z,
                                    beta: beta_eff,
                                    force: false,
                                };
                            }
                        } else if !self.positions_ready {
                            log_positions_not_ready = true;
                        } else {
                            // Pre-`should_enter` block chain. The reject
                            // counter increments only here (each tick that
                            // reaches the eligible/z_snapshot branch counts
                            // as one entry attempt) so the dashboard's
                            // breakdown stays comparable to in-filter
                            // rejects. bot-strategy#355 follow-up.
                            let pre_reject_reason: Option<&'static str> =
                                if kill_switch_active_snapshot {
                                    Some("kill_switch")
                                } else if session_halted_snapshot {
                                    Some("session_halted")
                                } else if daily_loss_blocks_snapshot {
                                    Some("daily_loss")
                                } else if circuit_breaker_until_ts_snapshot
                                    .is_some_and(|until| now_ts < until)
                                {
                                    Some("circuit_breaker")
                                } else if last_eval_ts.is_none() {
                                    Some("waiting_first_eval")
                                } else if !regime_ok {
                                    Some("regime")
                                } else {
                                    None
                                };
                            if let Some(reason) = pre_reject_reason {
                                crate::pairtrade::prom::ENTRY_REJECT_TOTAL
                                    .with_label_values(&[
                                        self.instances[inst_idx].id.as_str(),
                                        key.as_str(),
                                        reason,
                                    ])
                                    .inc();
                            } else {
                                let direction = if z > 0.0 {
                                    PositionDirection::ShortSpread
                                } else {
                                    PositionDirection::LongSpread
                                };
                                let decision = {
                                    let state = self.instances[inst_idx]
                                        .states
                                        .get(&key)
                                        .ok_or_else(|| anyhow!("missing state for {}", key))?;
                                    let shared =
                                        self.per_pair_state.get(&key).ok_or_else(|| {
                                            anyhow!("missing shared state for {}", key)
                                        })?;
                                    should_enter(EntryCheck {
                                        cfg: &self.cfg,
                                        pp,
                                        state,
                                        shared,
                                        z,
                                        std,
                                        net_funding,
                                        now_ts,
                                        proposed_direction: direction,
                                    })
                                };
                                match decision {
                                    Ok(()) => {
                                        action = TradeAction::Open {
                                            direction,
                                            z,
                                            beta: beta_eff,
                                        };
                                    }
                                    Err(reason) => {
                                        crate::pairtrade::prom::ENTRY_REJECT_TOTAL
                                            .with_label_values(&[
                                                self.instances[inst_idx].id.as_str(),
                                                key.as_str(),
                                                reason,
                                            ])
                                            .inc();
                                    }
                                }
                            }
                        }
                        let slope_sig = self.per_pair_state.get(&key).and_then(|s| {
                            spread_slope_sigma(&s.spread_history, self.cfg.metrics_window)
                        });
                        let beta_gap_for_log = self
                            .per_pair_state
                            .get(&key)
                            .map(|s| s.beta_gap)
                            .unwrap_or(0.0);
                        log::debug!(
                            "[ZCHECK] {} z={:.2} entry={:.2} std={:.4} mean={:.4} spread={:.4} hist={} beta_s={:.3} beta_l={:.3} funding={:.5} eligible={} beta_gap={:.3} slope_sigma={:.3} consec_loss={}",
                            key,
                            z,
                            z_entry,
                            std,
                            mean,
                            latest_spread,
                            spread_len,
                            beta_short,
                            beta_long,
                            net_funding,
                            eligible_shared,
                            beta_gap_for_log,
                            slope_sig.unwrap_or(0.0),
                            consecutive_losses_snapshot
                        );
                    }
                } else if eligible_shared && spread_len < min_points {
                    log::debug!(
                        "[ZCHECK] {} skipped (spread history too short: {} < {})",
                        key,
                        spread_len,
                        min_points
                    );
                } else if position_state.is_some() && !eligible_shared {
                    // If pair falls out of eligibility, flatten. When the
                    // book-quality guard is enabled (bot-strategy#531), a
                    // degraded book defers the flatten — re-checked every
                    // tick — until the book recovers or the deferral cap
                    // runs out. The eligibility break is often caused by
                    // degraded venue data, so the immediate close would
                    // execute into exactly the book conditions that
                    // triggered it (the 2026-06-10 -$30.85 pooled event).
                    if let Some(pos) = &position_state {
                        let defer_cap = self.cfg.ineligible_close_defer_cap_secs;
                        // A risk-triggered exit (stop_loss_z / max_loss_r /
                        // risk_budget) is never deferred: those gates live in
                        // the eligible branch, so once the pair turns
                        // ineligible this flatten is the only path that
                        // realizes them — holding it for up to `defer_cap`
                        // would keep an already-stopped-out position open
                        // into exactly the conditions the stop exists for
                        // (PR #166 Codex review). The close still fires with
                        // reason=ineligible, matching pre-guard behavior.
                        let risk_exit = if defer_cap > 0 {
                            let (z_now, std_now) = z_snapshot
                                .map(|(z, std, _, _)| (z, std))
                                .unwrap_or((0.0, 0.0));
                            let state = self.instances[inst_idx]
                                .states
                                .get(&key)
                                .ok_or_else(|| anyhow!("missing state for {}", key))?;
                            let shared = self
                                .per_pair_state
                                .get(&key)
                                .ok_or_else(|| anyhow!("missing shared state for {}", key))?;
                            risk_exit_reason(ExitCheck {
                                cfg: &self.cfg,
                                pp,
                                state,
                                shared,
                                z: z_now,
                                std: std_now,
                                p1,
                                p2,
                                equity_base: equity_reference_snapshot,
                                now_ts,
                            })
                        } else {
                            None
                        };
                        let degraded = if defer_cap > 0 && risk_exit.is_none() {
                            // This branch only runs on ticks where both
                            // legs emitted a bar, i.e. both just had an
                            // *accepted* tick — so the raw exchange_ts is
                            // fresh here by construction. The feed-health
                            // view is what lets the guard see a rejection
                            // storm that ended on this very tick (recovery
                            // holddown) instead of firing into the first
                            // post-storm book (PR #166 Codex review).
                            ineligible_close_book_degraded(
                                p1,
                                p2,
                                [
                                    self.tick_feed_health.get(&pair.base),
                                    self.tick_feed_health.get(&pair.quote),
                                ],
                                now_ts,
                                self.cfg.ineligible_close_defer_spread_bps,
                                self.cfg.ineligible_close_defer_stale_secs,
                            )
                        } else {
                            None
                        };
                        if let Some(reason) = risk_exit {
                            log::warn!(
                                "[EXIT_DEFER] {} bypass: risk exit ({}) pending; closing immediately regardless of book quality",
                                key,
                                reason
                            );
                        }
                        let mut fire_close = true;
                        if let Some(reason) = degraded {
                            let since = self.instances[inst_idx]
                                .states
                                .get_mut(&key)
                                .map(|s| *s.ineligible_defer_since_ts.get_or_insert(now_ts))
                                .unwrap_or(now_ts);
                            let elapsed = now_ts.saturating_sub(since);
                            let variant = self.instances[inst_idx].id.clone();
                            if elapsed < defer_cap {
                                log::warn!(
                                    "[EXIT_DEFER] {} reason=ineligible book_degraded={} elapsed={}s cap={}s",
                                    key,
                                    reason,
                                    elapsed,
                                    defer_cap
                                );
                                crate::pairtrade::prom::INELIGIBLE_CLOSE_DEFER_TOTAL
                                    .with_label_values(&[variant.as_str(), key.as_str(), reason])
                                    .inc();
                                fire_close = false;
                            } else {
                                log::warn!(
                                    "[EXIT_DEFER] {} cap exceeded ({}s >= {}s); closing into degraded book (reason={})",
                                    key,
                                    elapsed,
                                    defer_cap,
                                    reason
                                );
                                crate::pairtrade::prom::INELIGIBLE_CLOSE_DEFER_TOTAL
                                    .with_label_values(&[
                                        variant.as_str(),
                                        key.as_str(),
                                        "cap_exceeded",
                                    ])
                                    .inc();
                            }
                        }
                        if fire_close {
                            log::info!("[EXIT_CHECK] {} reason=ineligible", key);
                            if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                                state.pending_exit_reason = Some("ineligible");
                                state.ineligible_defer_since_ts = None;
                            }
                            action = TradeAction::Close {
                                direction: pos.direction,
                                z: 0.0,
                                beta: beta_eff,
                                force: false,
                            };
                        }
                    }
                }
            }
            if !positions_clear && matches!(action, TradeAction::Open { .. }) {
                log::debug!("[ENTRY] blocked due to open positions; key={}", key);
                action = TradeAction::None;
            }
            if maintenance_block_entries && matches!(action, TradeAction::Open { .. }) {
                action = TradeAction::None;
            }
            if self.shutdown_pending && matches!(action, TradeAction::Open { .. }) {
                log::debug!("[ENTRY] blocked by graceful shutdown; key={}", key);
                action = TradeAction::None;
            }

            if log_positions_not_ready && self.should_log_position_warn(&self.cfg.dex_name) {
                log::warn!("[POSITION] positions not synced yet; skipping entry");
                self.last_position_warn
                    .insert(self.cfg.dex_name.clone(), Instant::now());
            }

            if !matches!(action, TradeAction::None) {
                let net_funding = net_funding_for_direction(
                    match &action {
                        TradeAction::Open { z, .. } => *z,
                        TradeAction::Close { z, .. } => *z,
                        TradeAction::None => 0.0,
                    },
                    p1,
                    p2,
                );
                let abs_z = match &action {
                    TradeAction::Open { z, .. } | TradeAction::Close { z, .. } => z.abs(),
                    TradeAction::None => 0.0,
                };
                planned.push(PlannedAction {
                    pair: pair.clone(),
                    key: key.clone(),
                    action,
                    net_funding_per_hour: net_funding,
                    abs_z,
                    liquidity_score: liquidity_score(p1, p2),
                    p1: p1.clone(),
                    p2: p2.clone(),
                });
            }
        }
        Ok(planned)
    }
}
