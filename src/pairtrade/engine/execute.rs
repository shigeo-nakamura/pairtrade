//! Execution of planned trade actions for the pairtrade engine.
//!
//! Extracted from `engine/step.rs` (bot-strategy#444): this module owns
//! `step_execute_exits` and `step_execute_entry`, which take the
//! `PlannedAction` set produced by the planning pass and place (or, in
//! dry-run / backtest mode, simulate) the corresponding close and open
//! orders. Behaviour is unchanged from the prior in-`step.rs`
//! implementation.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use super::super::apply_post_exit_state;
use super::super::exit::compute_pnl;
use super::super::funding_history;
use super::super::market::SymbolSnapshot;
use super::super::pnl_log::{PnlLogRecord, PnlTradeDetails};
use super::super::state::{BtDeferredExit, PendingOrders, Position};
use super::super::PairTradeEngine;

use super::plan::{PlannedAction, TradeAction};

impl PairTradeEngine {
    pub(super) async fn step_execute_exits(
        &mut self,
        inst_idx: usize,
        planned: &[PlannedAction],
        price_map: &HashMap<String, SymbolSnapshot>,
        now_ts: i64,
    ) -> Result<()> {
        // Process exits first
        for plan in planned.iter() {
            if let TradeAction::Close {
                direction,
                z,
                beta,
                force,
            } = plan.action
            {
                let qtys = self
                    .exit_sizes_for_pair(inst_idx, &plan.key, &plan.pair, beta, &plan.p1, &plan.p2)
                    .context("exit_sizes_for_pair")?;
                if qtys.0 <= Decimal::ZERO && qtys.1 <= Decimal::ZERO {
                    log::warn!(
                        "[EXIT] {} no open position sizes available; clearing state",
                        plan.key
                    );
                    // bot-strategy#514: record the close context before the
                    // position state is dropped — DEX-side accounting may
                    // still realise PnL for whatever was actually open.
                    let already_recorded = self.instances[inst_idx]
                        .states
                        .get(&plan.key)
                        .map(|s| s.position.is_none() || s.recovery_recorded)
                        .unwrap_or(true);
                    if !already_recorded {
                        self.write_recovery_no_pnl_record(
                            inst_idx,
                            &plan.key,
                            direction,
                            "no_exit_sizes",
                            now_ts,
                            price_map,
                        );
                    }
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.position = None;
                        state.recovery_recorded = false;
                        state.pending_exit = None;
                        state.position_guard = false;
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                    }
                    continue;
                }
                if qtys.0 <= Decimal::ZERO || qtys.1 <= Decimal::ZERO {
                    log::warn!(
                        "[EXIT] {} missing leg size (base={}, quote={}); closing available legs only",
                        plan.key,
                        qtys.0,
                        qtys.1
                    );
                }
                if self.cfg.dry_run {
                    let price_a = price_map
                        .get(&plan.pair.base)
                        .map(|s| s.price)
                        .unwrap_or_default();
                    let price_b = price_map
                        .get(&plan.pair.quote)
                        .map(|s| s.price)
                        .unwrap_or_default();
                    let pnl = self.instances[inst_idx]
                        .states
                        .get(&plan.key)
                        .and_then(|s| s.position.as_ref())
                        .and_then(|pos| compute_pnl(pos, price_a, price_b));
                    if let Some(pnl) = pnl {
                        if let Some(pnl_value) = pnl.to_f64() {
                            let pos_ref = self.instances[inst_idx]
                                .states
                                .get(&plan.key)
                                .and_then(|s| s.position.as_ref());
                            let hold_secs =
                                pos_ref.map(|p| now_ts.saturating_sub(p.entered_ts).max(0) as f64);
                            let entry_a = pos_ref
                                .and_then(|p| p.entry_price_a)
                                .and_then(|v| v.to_f64());
                            let entry_b = pos_ref
                                .and_then(|p| p.entry_price_b)
                                .and_then(|v| v.to_f64());
                            let (carry_usd, ticks_observed) = match pos_ref {
                                Some(p) => match (
                                    p.entry_size_a,
                                    p.entry_price_a,
                                    p.entry_size_b,
                                    p.entry_price_b,
                                ) {
                                    (Some(sa), Some(pa), Some(sb), Some(pb)) => {
                                        funding_history::compute_carry_usd(
                                            funding_history::FundingCarryInput {
                                                history: &self.funding_history,
                                                base_symbol: &plan.pair.base,
                                                quote_symbol: &plan.pair.quote,
                                                open_ts: p.entered_ts,
                                                close_ts: now_ts,
                                                direction,
                                                entry_size_a: sa,
                                                entry_price_a: pa,
                                                entry_size_b: sb,
                                                entry_price_b: pb,
                                            },
                                        )
                                    }
                                    _ => (0.0, 0),
                                },
                                None => (0.0, 0),
                            };
                            let mut record = PnlLogRecord::new(
                                &plan.pair.base,
                                &plan.pair.quote,
                                direction,
                                pnl_value,
                                now_ts,
                                "exit_dry_run",
                            )
                            .with_trade_details(PnlTradeDetails {
                                entry_a,
                                entry_b,
                                exit_a: price_a.to_f64(),
                                exit_b: price_b.to_f64(),
                                beta: Some(beta),
                                z_entry: Some(z),
                                z_exit: self
                                    .per_pair_state
                                    .get(&plan.key)
                                    .and_then(|s| s.last_spread.map(|_| z)),
                                hold_secs,
                            });
                            if ticks_observed > 0 {
                                record = record.with_funding(carry_usd, ticks_observed);
                            }
                            let close_reason = self.instances[inst_idx]
                                .states
                                .get(&plan.key)
                                .and_then(|s| s.pending_exit_reason)
                                .unwrap_or("unknown");
                            record = record.with_close_reason(close_reason);
                            self.write_pnl_record(inst_idx, record);
                            self.instances[inst_idx].realized_pnl_today += pnl_value;
                            self.instances[inst_idx].funding_carry_today += carry_usd;
                            self.instances[inst_idx].total_funding_carry += carry_usd;
                            // write_pnl_record always bumps total_trades / total_pnl
                            // (now persisted, bot-strategy#320), so the snapshot is
                            // dirty regardless of pnl sign.
                            let mut risk_state_dirty = true;
                            // Collect circuit-breaker history events here
                            // and emit them after the borrow on
                            // self.instances releases, since
                            // record_risk_event_for_instance needs &mut self.
                            let mut cb_event: Option<(&'static str, u32, Option<u64>)> = None;
                            if pnl_value < 0.0 {
                                self.instances[inst_idx].consecutive_losses += 1;
                                risk_state_dirty = true;
                                if let Some(cooldown) = self.cfg.circuit_breaker_cooldown_for(
                                    self.instances[inst_idx].consecutive_losses,
                                ) {
                                    self.instances[inst_idx].circuit_breaker_until =
                                        Some(Instant::now() + cooldown);
                                    self.instances[inst_idx].circuit_breaker_until_ts =
                                        Some(now_ts + cooldown.as_secs() as i64);
                                    log::warn!(
                                        "[CIRCUIT_BREAKER] activated after {} consecutive losses, cooldown {}s",
                                        self.instances[inst_idx].consecutive_losses, cooldown.as_secs()
                                    );
                                    cb_event = Some((
                                        "activated",
                                        self.instances[inst_idx].consecutive_losses,
                                        Some(cooldown.as_secs()),
                                    ));
                                }
                            } else if pnl_value > 0.0 {
                                if self.instances[inst_idx].consecutive_losses > 0 {
                                    log::info!("[CIRCUIT_BREAKER] reset after win (was {} consecutive losses)", self.instances[inst_idx].consecutive_losses);
                                    risk_state_dirty = true;
                                    let was_active =
                                        self.instances[inst_idx].circuit_breaker_until_ts.is_some();
                                    if was_active {
                                        cb_event = Some((
                                            "cleared",
                                            self.instances[inst_idx].consecutive_losses,
                                            None,
                                        ));
                                    }
                                }
                                self.instances[inst_idx].consecutive_losses = 0;
                                self.instances[inst_idx].circuit_breaker_until = None;
                                self.instances[inst_idx].circuit_breaker_until_ts = None;
                            }
                            if risk_state_dirty {
                                self.persist_risk_state();
                            }
                            if let Some((event_type, prior_losses, cooldown_secs)) = cb_event {
                                let detail = match cooldown_secs {
                                    Some(secs) => Some(serde_json::json!({
                                        "consecutive_losses": prior_losses,
                                        "cooldown_secs": secs,
                                        "pnl_value": pnl_value,
                                    })),
                                    None => Some(serde_json::json!({
                                        "prior_losses": prior_losses,
                                        "pnl_value": pnl_value,
                                        "trigger": "winning_trade",
                                    })),
                                };
                                self.record_risk_event_for_instance(
                                    inst_idx,
                                    "circuit_breaker",
                                    event_type,
                                    None,
                                    detail,
                                );
                            }
                        }
                        log::info!(
                            "[EXIT] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} force={} pnl={} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            force,
                            pnl,
                            now_ts
                        );
                    } else {
                        log::info!(
                            "[EXIT] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} force={} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            force,
                            now_ts
                        );
                    }
                    let inst_id = self.instances[inst_idx].id.clone();
                    let shared_for_exit = self.per_pair_state.get(&plan.key);
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        if self.cfg.backtest_mode && self.cfg.bt_fill_delay_secs > 0 {
                            // Defer position clearing to simulate exchange
                            // fill latency (bot-strategy#69). The deferred
                            // resolve site reads pending_exit_reason to
                            // tag last_stop_loss_at for #316.
                            state.bt_deferred_exit = Some(BtDeferredExit {
                                resolve_at_ts: now_ts + self.cfg.bt_fill_delay_secs,
                            });
                        } else {
                            apply_post_exit_state(
                                state,
                                shared_for_exit,
                                direction,
                                now_ts,
                                &inst_id,
                                plan.key.as_str(),
                            );
                        }
                    }
                } else if self.cfg.observe_only {
                    log::info!(
                        "[EXIT] observe-only mode; skipping close orders for {}/{}",
                        plan.pair.base,
                        plan.pair.quote
                    );
                } else {
                    let placed_ts_ms = Utc::now().timestamp_millis();
                    let (legs, exit_taker_takeover_at) = match self
                        .close_pair_orders(&plan.pair, direction, qtys, price_map, force)
                        .await
                    {
                        Ok(out) => out,
                        Err(err) => {
                            self.register_partial_leg_failure(
                                inst_idx,
                                &plan.key,
                                direction,
                                placed_ts_ms,
                                &err,
                                true,
                            );
                            return Err(err);
                        }
                    };
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.pending_exit = Some(
                            PendingOrders {
                                legs,
                                direction,
                                placed_at: Instant::now(),
                                placed_ts_ms,
                                hedge_retry_count: 0,
                                post_only_hybrid: false,
                                exit_taker_takeover_at,
                            }
                            .with_leg_decision_ts(),
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn step_execute_entry(
        &mut self,
        inst_idx: usize,
        planned: &[PlannedAction],
        price_map: &HashMap<String, SymbolSnapshot>,
        now_ts: i64,
    ) -> Result<()> {
        let mut active_symbols: HashSet<String> = self
            .cfg
            .universe
            .iter()
            .filter_map(|pair| {
                let key = format!("{}/{}", pair.base, pair.quote);
                let state = self.instances[inst_idx].states.get(&key)?;
                let is_active = state.position.is_some()
                    || state.pending_entry.is_some()
                    || state.pending_exit.is_some()
                    || state.bt_deferred_exit.is_some()
                    || state.position_guard;
                if is_active {
                    let mut symbols = HashSet::new();
                    symbols.insert(pair.base.clone());
                    symbols.insert(pair.quote.clone());
                    Some(symbols)
                } else {
                    None
                }
            })
            .flatten()
            .collect();
        for symbol in self.open_positions.keys() {
            if self.history.contains_key(symbol) {
                active_symbols.insert(symbol.clone());
            }
        }

        // Among entry candidates, shortlist by model score then pick best by funding->score->liquidity->|z|
        let mut entry_candidates: Vec<&PlannedAction> = planned
            .iter()
            .filter(|p| matches!(p.action, TradeAction::Open { .. }))
            .filter(|p| {
                if active_symbols.is_empty() {
                    return true;
                }
                let overlaps =
                    active_symbols.contains(&p.pair.base) || active_symbols.contains(&p.pair.quote);
                if overlaps {
                    log::debug!(
                        "[OVERLAP] skipping {}/{} due to active symbol overlap",
                        p.pair.base,
                        p.pair.quote
                    );
                }
                !overlaps
            })
            .collect();
        entry_candidates.sort_by(|a, b| {
            self.state_score(inst_idx, &b.key)
                .partial_cmp(&self.state_score(inst_idx, &a.key))
                .unwrap_or(Ordering::Equal)
        });
        let shortlisted: Vec<&PlannedAction> = entry_candidates
            .into_iter()
            .take(self.cfg.max_active_pairs.max(1))
            .collect();
        let best_entry = shortlisted.into_iter().max_by(|a, b| {
            a.net_funding_per_hour
                .partial_cmp(&b.net_funding_per_hour)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    self.state_score(inst_idx, &a.key)
                        .partial_cmp(&self.state_score(inst_idx, &b.key))
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| {
                    a.liquidity_score
                        .partial_cmp(&b.liquidity_score)
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| a.abs_z.partial_cmp(&b.abs_z).unwrap_or(Ordering::Equal))
        });
        if let Some(plan) = best_entry {
            if let TradeAction::Open { direction, z, beta } = plan.action {
                // Force-fresh equity immediately before sizing: entries happen
                // rarely enough that this REST call is cheap, and it keeps
                // notional sized against the current balance rather than the
                // 30-min cache used for dashboard / R-budget. See
                // bot-strategy#156.
                self.fetch_equity_rest(inst_idx).await;
                // bot-strategy#461: shrink notional under beta-uncertainty.
                // Lookup pair-specific params (with default fallback) so the
                // YAML `pairs.<pair>.beta_gap_notional_scale` override works
                // identically to the other Phase 2 filters.
                let pair_key = format!("{}/{}", plan.pair.base, plan.pair.quote);
                let beta_gap = self
                    .per_pair_state
                    .get(&pair_key)
                    .map(|s| s.beta_gap)
                    .unwrap_or(0.0);
                let (
                    notional_scale_param,
                    notional_floor_param,
                    depth_slope,
                    depth_s_min,
                    depth_s_max,
                    entry_z_base,
                ) = {
                    let pp = self.pair_params_for(inst_idx, &pair_key);
                    (
                        pp.beta_gap_notional_scale,
                        pp.beta_gap_notional_floor,
                        pp.depth_size_slope,
                        pp.depth_size_min,
                        pp.depth_size_max,
                        pp.entry_z_base,
                    )
                };
                // bot-strategy#515: concentrate capital on deeper entries.
                // Resolved once here and frozen for the hold (the position
                // qtys carry it implicitly).
                let depth_mult = crate::pairtrade::sizing::depth_size_mult(
                    z.abs(),
                    entry_z_base,
                    depth_slope,
                    depth_s_min,
                    depth_s_max,
                );
                if depth_mult != 1.0 {
                    log::info!(
                        "[DEPTH_SIZE] pair={} z={:.2} entry_z_base={:.2} mult={:.3}",
                        pair_key,
                        z,
                        entry_z_base,
                        depth_mult
                    );
                }
                let notional_scale = crate::pairtrade::sizing::beta_gap_notional_scale(
                    beta_gap,
                    notional_scale_param,
                    notional_floor_param,
                ) * depth_mult;
                crate::pairtrade::prom::ENTRY_NOTIONAL_SCALE
                    .with_label_values(&[self.instances[inst_idx].id.as_str(), pair_key.as_str()])
                    .set(notional_scale);
                let qtys = self
                    .hedged_sizes(
                        inst_idx,
                        &plan.pair,
                        beta,
                        &plan.p1,
                        &plan.p2,
                        notional_scale,
                    )
                    .context("hedged_sizes")?;
                let price_a = price_map
                    .get(&plan.pair.base)
                    .map(|s| s.price)
                    .unwrap_or_default();
                let price_b = price_map
                    .get(&plan.pair.quote)
                    .map(|s| s.price)
                    .unwrap_or_default();
                if self.cfg.dry_run {
                    log::info!(
                            "[ENTRY] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} carry={:.4} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            plan.net_funding_per_hour,
                            now_ts
                        );
                    let inst_id = self.instances[inst_idx].id.clone();
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.position = Some(Position {
                            direction,
                            entered_at: Instant::now(),
                            entered_ts: now_ts,
                            entry_price_a: Some(price_a),
                            entry_price_b: Some(price_b),
                            entry_size_a: Some(qtys.0),
                            entry_size_b: Some(qtys.1),
                            entry_z: Some(z),
                            // bot-strategy#463: snapshot the effective β
                            // used to size this hedge. Phase 1 only reads
                            // it for drift detection; Phase 2 will use it
                            // as the reference for re-balance qty.
                            entry_beta: Some(beta),
                            last_rehedge_ts: None,
                            rehedge_realized_pnl: None,
                            prev_beta_for_velocity: None,
                        });
                        state.recovery_recorded = false;
                        super::super::prom::LAST_ENTRY_Z
                            .with_label_values(&[&inst_id, plan.key.as_str()])
                            .set(z);
                    }
                } else if self.cfg.observe_only {
                    log::info!(
                        "[ENTRY] observe-only mode; skipping entry orders for {}/{}",
                        plan.pair.base,
                        plan.pair.quote
                    );
                } else {
                    log::info!(
                        "[ENTRY] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} carry={:.4} ts={}",
                        plan.pair.base,
                        plan.pair.quote,
                        direction,
                        qtys.0,
                        price_a,
                        qtys.1,
                        price_b,
                        z,
                        beta,
                        plan.net_funding_per_hour,
                        now_ts
                    );
                    let placed_ts_ms = Utc::now().timestamp_millis();
                    let legs = match self
                        .place_pair_orders(inst_idx, &plan.pair, direction, qtys, price_map)
                        .await
                    {
                        Ok(legs) => legs,
                        Err(err) => {
                            self.register_partial_leg_failure(
                                inst_idx,
                                &plan.key,
                                direction,
                                placed_ts_ms,
                                &err,
                                false,
                            );
                            return Err(err);
                        }
                    };
                    // place_pair_orders returns Ok(Vec::new()) when the entry is
                    // gated (hedge-ratio deviation, zero qty). Treat that as
                    // "skip"; setting pending_entry with empty legs would let
                    // all_filled() be vacuously true on the next step and
                    // synthesize a phantom position. See bot-strategy#211.
                    if !legs.is_empty() {
                        let entry_pp = self.pair_params_for(inst_idx, &plan.key).clone();
                        let entry_pp = &entry_pp;
                        let hybrid =
                            entry_pp.entry_post_only_timeout_secs > 0 && self.post_only_supported();
                        if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                            state.pending_entry = Some(
                                PendingOrders {
                                    legs,
                                    direction,
                                    placed_at: Instant::now(),
                                    placed_ts_ms,
                                    hedge_retry_count: 0,
                                    post_only_hybrid: hybrid,
                                    // Entry path — exit_taker_takeover_at only
                                    // applies to exit pending orders (#408).
                                    exit_taker_takeover_at: None,
                                }
                                .with_leg_decision_ts(),
                            );
                        }
                        // bot-strategy#783 Codex P2 follow-up: step_setup's
                        // detect_capital_event_and_rebaseline (which normally
                        // latches and persists capital_position_seen_since_baseline
                        // on the open->flat transition) runs before this
                        // entry exists — that transition is only observed on
                        // the *next* tick's step_setup call. If the process
                        // exits before then, the persisted guard is still
                        // false, and a startup force-close that flattens
                        // this position without recording its PnL would be
                        // misclassified as a verified capital event once
                        // the delayed settlement lands. Latch and persist
                        // synchronously here instead of waiting. dry_run has
                        // no real venue position for force_close_on_startup
                        // to ever find, so there is nothing to guard.
                        if !self.cfg.dry_run {
                            let inst = &mut self.instances[inst_idx];
                            let was_seen = std::mem::replace(
                                &mut inst.capital_position_seen_since_baseline,
                                true,
                            );
                            inst.flat_since = None;
                            if !was_seen {
                                self.persist_risk_state();
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
