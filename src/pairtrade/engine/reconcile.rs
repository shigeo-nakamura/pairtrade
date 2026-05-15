//! Pending-order reconciliation for `PairTradeEngine`.
//!
//! `reconcile_pending_orders` is the per-(instance, pair) state machine
//! that transitions a `PendingOrders` set toward filled / partially-filled
//! / stale handling. Helpers in this file (`pending_status`,
//! `cancel_pending_orders`, leg-fill aggregation) are private to the
//! cluster and only used by the reconciliation flow. Pure relocation
//! from the god-module split (#291); no semantic change.
//!
//! The reconcile loop touches several adjacent subsystems still living
//! in `mod.rs`:
//! - sizing / pricing helpers (`limit_price_for`, `order_spread_param`,
//!   `quantize_order_size_exit`)
//! - placement helpers (`reissue_partial_legs`, `reissue_entry_as_taker`,
//!   `force_close_all_positions`)
//! - PnL logging (`write_pnl_record`) + risk-state persistence
//!   (`persist_risk_state`)
//! - error classifiers (`engine::error_class::is_reduce_only_rejection`,
//!   `confirm_reduce_only_position_missing`, `fetch_residual_position_size`)
//!
//! These are reached via the existing `&mut self` dispatch on
//! `PairTradeEngine`, so the cluster is portable as-is.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use super::super::apply_post_exit_state;
use super::super::defaults::MAX_EXIT_RETRIES;
use super::super::engine;
use super::super::exit::compute_pnl;
use super::super::funding_history;
use super::super::market::SymbolSnapshot;
use super::super::pnl_log::PnlLogRecord;
use super::super::state::{PendingLeg, PendingOrders, PendingStatus, Position};
use super::super::PairTradeEngine;

impl PairTradeEngine {

    pub(in crate::pairtrade) async fn reconcile_pending_orders(
        &mut self,
        inst_idx: usize,
        key: &str,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Result<()> {
        let timeout = Duration::from_secs(self.cfg.order_timeout_secs.max(1));
        let now_ts = self.current_now_ts();
        let (pending_entry, pending_exit) = {
            let state = self
                .instances[inst_idx]
                .states
                .get_mut(key)
                .ok_or_else(|| anyhow!("missing state for {}", key))?;
            (state.pending_entry.take(), state.pending_exit.take())
        };

        if let Some(mut pending) = pending_entry {
            let status = self.pending_status(&pending).await?;
            Self::update_pending_fills(&mut pending, &status.fills);
            let filled_qtys = Self::filled_by_leg(&pending, &status.fills);
            if Self::all_filled(&pending, &status.fills) {
                let z_at_entry = self
                    .per_pair_state
                    .get(key)
                    .and_then(|s| s.z_score().map(|(z, _)| z));
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    let (mut ep_a, mut ep_b, mut es_a, mut es_b) = (None, None, None, None);
                    if let Some((base, quote)) = key.split_once('/') {
                        ep_a = price_map.get(base).map(|s| s.price);
                        ep_b = price_map.get(quote).map(|s| s.price);
                        let (sum_a, sum_b) =
                            Self::sum_entry_sizes_by_symbol(&pending.legs, base, quote);
                        es_a = sum_a;
                        es_b = sum_b;
                    }
                    state.position = Some(Position {
                        direction: pending.direction,
                        entered_at: Instant::now(),
                        entered_ts: now_ts,
                        entry_price_a: ep_a,
                        entry_price_b: ep_b,
                        entry_size_a: es_a,
                        entry_size_b: es_b,
                        entry_z: z_at_entry,
                    });
                    state.pending_entry = None;
                }
                Self::observe_leg_execution_quality(
                    &self.instances[inst_idx].id,
                    key,
                    "entry",
                    &pending.legs,
                    &status,
                );
                log::info!("[ORDER] {} entry orders filled", key);
            } else if filled_qtys.values().any(|qty| *qty > Decimal::ZERO) {
                let next_retry = pending.hedge_retry_count.saturating_add(1);
                let max_retries = self.cfg.entry_partial_fill_max_retries;
                let use_market = max_retries > 0 && next_retry > max_retries;
                if use_market {
                    log::info!(
                        "[ORDER] {} entry leg partially filled, retries exceeded ({} > {}); reissuing remaining legs as MARKET",
                        key,
                        next_retry,
                        max_retries
                    );
                } else if max_retries > 0 {
                    log::info!(
                        "[ORDER] {} entry leg partially filled, reissuing remaining legs (retry {}/{})",
                        key,
                        next_retry,
                        max_retries
                    );
                } else {
                    log::warn!(
                        "[ORDER] {} entry leg partially filled, reissuing remaining legs",
                        key
                    );
                }
                self.cancel_pending_orders(&pending).await?;
                if let Some(new_pending) = self
                    .reissue_partial_legs(
                        &pending,
                        &filled_qtys,
                        price_map,
                        false,
                        use_market,
                        next_retry,
                    )
                    .await?
                {
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_entry = Some(new_pending);
                    }
                } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    state.pending_entry = None;
                }
                return Ok(());
            } else if pending.post_only_hybrid {
                let recon_pp = self.pair_params_for(inst_idx, key).clone();
                let recon_pp = &recon_pp;
                let timeout_elapsed = recon_pp.entry_post_only_timeout_secs > 0
                    && pending.placed_at.elapsed()
                        >= Duration::from_secs(recon_pp.entry_post_only_timeout_secs);
                if !timeout_elapsed {
                    // Post-only entry still within timeout window — keep pending
                    // alive so the next ENTRY signal sees active_symbols and
                    // doesn't fire a duplicate. Fires when
                    // entry_post_only_timeout_secs > trading_period_secs (e.g.
                    // Tokyo Extended 120s vs 60s tick): the first reconcile
                    // tick after ENTRY hits this branch with elapsed < timeout
                    // and the catch-all at the end of the if-chain is
                    // unreachable. See bot-strategy#243.
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_entry = Some(pending);
                    }
                } else {
                    // Phase 0 instrumentation (bot-strategy#165): capture per-leg
                    // fill status, posted limit vs current book, and z-movement
                    // from entry to timeout so we can tell why the post-only
                    // legs didn't fill before falling back to taker.
                    let (z_entry, z_now) = {
                        let state_ref = self.instances[inst_idx].states.get(key);
                        let ze = state_ref.map(|s| s.z_entry).unwrap_or(0.0);
                        let zn = self
                            .per_pair_state
                            .get(key)
                            .and_then(|s| s.z_score().map(|(z, _)| z))
                            .unwrap_or(0.0);
                        (ze, zn)
                    };
                    let leg_details: Vec<String> = pending
                        .legs
                        .iter()
                        .map(|leg| {
                            let filled = status
                                .fills
                                .get(&leg.order_id)
                                .cloned()
                                .unwrap_or(Decimal::ZERO);
                            let open = status.open_ids.contains(&leg.order_id);
                            let snap = price_map.get(&leg.symbol);
                            let bid = snap.and_then(|s| s.bid_price);
                            let ask = snap.and_then(|s| s.ask_price);
                            let tick = snap.and_then(|s| s.min_tick);
                            format!(
                                "[{}|{:?}|tgt={}|filled={}|open={}|limit={}|bid={}|ask={}|tick={}]",
                                leg.symbol,
                                leg.side,
                                leg.target,
                                filled,
                                open,
                                leg.limit_price
                                    .map(|d| d.to_string())
                                    .unwrap_or_else(|| "none".into()),
                                bid.map(|d| d.to_string())
                                    .unwrap_or_else(|| "?".into()),
                                ask.map(|d| d.to_string())
                                    .unwrap_or_else(|| "?".into()),
                                tick.map(|d| d.to_string())
                                    .unwrap_or_else(|| "?".into()),
                            )
                        })
                        .collect();
                    log::info!(
                        "[ORDER_FALLBACK_DETAIL] {} elapsed={}s dir={:?} z_entry={:.2} z_now={:.2} legs={}",
                        key,
                        pending.placed_at.elapsed().as_secs(),
                        pending.direction,
                        z_entry,
                        z_now,
                        leg_details.join(" ")
                    );

                    // Post-only entry timed out; cancel and reissue as taker
                    log::info!(
                        "[ORDER] {} post-only entry timeout ({}s), falling back to taker",
                        key,
                        recon_pp.entry_post_only_timeout_secs
                    );
                    self.cancel_pending_orders(&pending).await?;
                    let new_pending = self
                        .reissue_entry_as_taker(key, &pending, price_map)
                        .await?;
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_entry = new_pending;
                    }
                }
            } else if pending.placed_at.elapsed() >= timeout {
                // Partial fill or stuck orders; cancel and flatten any filled leg
                if status.open_remaining > 0 {
                    log::warn!(
                        "[ORDER] {} entry orders stale ({}s), cancelling {} legs",
                        key,
                        pending.placed_at.elapsed().as_secs(),
                        status.open_remaining
                    );
                    for leg in &pending.legs {
                        let filled = filled_qtys
                            .get(&leg.order_id)
                            .cloned()
                            .unwrap_or(Decimal::ZERO);
                        let is_open = status.open_ids.contains(&leg.order_id);
                        log::debug!(
                            "[ORDER] {} entry leg status symbol={} order_id={} target={} filled={} open={}",
                            key,
                            leg.symbol,
                            leg.order_id,
                            leg.target,
                            filled,
                            is_open
                        );
                    }
                    self.cancel_pending_orders(&pending).await?;
                }
                let filled_qtys = Self::filled_by_leg(&pending, &status.fills);
                let mut flattened_any = false;
                let mut hedge_failed = false;
                let mut retry_count = pending.hedge_retry_count;
                let max_retries = 3u32;
                for leg in &pending.legs {
                    let filled = filled_qtys
                        .get(&leg.order_id)
                        .cloned()
                        .unwrap_or(Decimal::ZERO);
                    if filled > Decimal::ZERO {
                        if price_map.contains_key(&leg.symbol) {
                            let hedge_side = match leg.side {
                                dex_connector::OrderSide::Long => dex_connector::OrderSide::Short,
                                dex_connector::OrderSide::Short => dex_connector::OrderSide::Long,
                            };
                            let use_market = retry_count + 1 >= max_retries;
                            let limit = if use_market {
                                None
                            } else {
                                self.limit_price_for(&leg.symbol, hedge_side, price_map)
                            };
                            if !use_market && limit.is_none() {
                                log::warn!(
                                    "[ORDER] Missing reference price for hedge {} leg {}",
                                    leg.symbol,
                                    leg.order_id
                                );
                                hedge_failed = true;
                                continue;
                            }
                            let spread = self.order_spread_param(limit, false);
                            if let Err(e) = self
                                .connector
                                .create_order(
                                    &leg.symbol,
                                    filled,
                                    hedge_side,
                                    limit,
                                    spread,
                                    true,
                                    None,
                                )
                                .await
                            {
                                log::error!(
                                    "[ORDER] Failed to hedge partial entry {} ({}): {:?}",
                                    leg.symbol,
                                    leg.order_id,
                                    e
                                );
                                hedge_failed = true;
                            } else {
                                flattened_any = true;
                                let mode = if use_market { "MARKET" } else { "LIMIT" };
                                log::warn!(
                                    "[ORDER] Hedged partial entry on {} size={} mode={} retries={}",
                                    leg.symbol,
                                    filled,
                                    mode,
                                    retry_count
                                );
                            }
                        } else {
                            log::warn!(
                                "[ORDER] Missing price map entry for hedge {} leg {}",
                                leg.symbol,
                                leg.order_id
                            );
                            hedge_failed = true;
                        }
                    }
                }
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    if hedge_failed {
                        retry_count = retry_count.saturating_add(1);
                        pending.hedge_retry_count = retry_count;
                        log::warn!(
                            "[ORDER] Hedge retry scheduled for {} (retry {} of {})",
                            key,
                            retry_count,
                            max_retries
                        );
                        pending.placed_at = Instant::now();
                        state.pending_entry = Some(pending);
                    } else {
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                        state.pending_entry = None;
                        if flattened_any {
                            state.position = None;
                        }
                    }
                }
            } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                state.pending_entry = Some(pending);
            }
        }

        if let Some(pending) = pending_exit {
            let status = self.pending_status(&pending).await?;
            let mut pending = pending;
            Self::update_pending_fills(&mut pending, &status.fills);
            let filled_qtys = Self::filled_by_leg(&pending, &status.fills);
            // (record, realized_pnl, funding_carry_usd) — the third element
            // is folded into `inst.funding_carry_today` at the same site as
            // `realized_pnl_today` below. 0.0 when no ticks were observed.
            let mut pnl_record: Option<(PnlLogRecord, f64, f64)> = None;
            if status.open_remaining == 0 && Self::all_filled(&pending, &status.fills) {
                let inst_id = self.instances[inst_idx].id.clone();
                let z_exit = self
                    .per_pair_state
                    .get(key)
                    .and_then(|s| s.z_score().map(|(z, _)| z));
                let beta_val = self.per_pair_state.get(key).map(|s| s.beta);
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    if let Some(pos) = state.position.as_ref() {
                        if let Some((base, quote)) = key.split_once('/') {
                            if let (Some(p1), Some(p2)) =
                                (price_map.get(base), price_map.get(quote))
                            {
                                if let Some(pnl) =
                                    compute_pnl(pos, p1.price, p2.price).and_then(|p| p.to_f64())
                                {
                                    let hold_secs = Some(
                                        now_ts.saturating_sub(pos.entered_ts).max(0) as f64,
                                    );
                                    let entry_a = pos.entry_price_a.and_then(|v| v.to_f64());
                                    let entry_b = pos.entry_price_b.and_then(|v| v.to_f64());
                                    let (carry_usd, ticks_observed) = match (
                                        pos.entry_size_a,
                                        pos.entry_price_a,
                                        pos.entry_size_b,
                                        pos.entry_price_b,
                                    ) {
                                        (Some(sa), Some(pa), Some(sb), Some(pb)) => {
                                            funding_history::compute_carry_usd(
                                                &self.funding_history,
                                                base,
                                                quote,
                                                pos.entered_ts,
                                                now_ts,
                                                pos.direction,
                                                sa,
                                                pa,
                                                sb,
                                                pb,
                                            )
                                        }
                                        _ => (0.0, 0),
                                    };
                                    let mut record = PnlLogRecord::new(
                                        base,
                                        quote,
                                        pos.direction,
                                        pnl,
                                        now_ts,
                                        "exit_fill",
                                    ).with_trade_details(
                                        entry_a, entry_b,
                                        p1.price.to_f64(), p2.price.to_f64(),
                                        beta_val,
                                        pos.entry_z,
                                        z_exit,
                                        hold_secs,
                                    );
                                    if ticks_observed > 0 {
                                        record = record.with_funding(carry_usd, ticks_observed);
                                    }
                                    // #314 Group 4: emit gross/funding bps to
                                    // Prometheus. Same Some(...,...,...,...)
                                    // gate as the funding compute above so the
                                    // bps denominator stays consistent.
                                    if let (Some(sa), Some(pa), Some(sb), Some(pb)) = (
                                        pos.entry_size_a,
                                        pos.entry_price_a,
                                        pos.entry_size_b,
                                        pos.entry_price_b,
                                    ) {
                                        let leg_a = (sa * pa).abs().to_f64();
                                        let leg_b = (sb * pb).abs().to_f64();
                                        if let (Some(a), Some(b)) = (leg_a, leg_b) {
                                            let notional = a + b;
                                            if notional > 0.0 {
                                                super::super::prom::CLOSE_GROSS_PNL_BPS
                                                    .with_label_values(&[&inst_id, key])
                                                    .observe(pnl / notional * 10_000.0);
                                                if ticks_observed > 0 {
                                                    super::super::prom::CLOSE_FUNDING_BPS
                                                        .with_label_values(&[&inst_id, key])
                                                        .observe(carry_usd / notional * 10_000.0);
                                                }
                                            }
                                        }
                                    }
                                    pnl_record = Some((record, pnl, carry_usd));
                                }
                            }
                        }
                    }
                    apply_post_exit_state(
                        state,
                        self.per_pair_state.get(key),
                        pending.direction,
                        now_ts,
                        &inst_id,
                        key,
                    );
                    state.pending_exit = None;
                }
                Self::observe_leg_execution_quality(
                    &inst_id,
                    key,
                    "exit",
                    &pending.legs,
                    &status,
                );
                log::info!("[ORDER] {} exit orders filled", key);
                if let Some((record, pnl_value, funding_value)) = pnl_record {
                    self.write_pnl_record(inst_idx, record);
                    self.instances[inst_idx].realized_pnl_today += pnl_value;
                    self.instances[inst_idx].funding_carry_today += funding_value;
                    // write_pnl_record always bumps total_trades / total_pnl
                    // (now persisted, bot-strategy#320), so the snapshot is
                    // dirty regardless of pnl sign.
                    let mut risk_state_dirty = true;
                    if pnl_value < 0.0 {
                        self.instances[inst_idx].consecutive_losses += 1;
                        risk_state_dirty = true;
                        if let Some(cooldown) = self
                            .cfg
                            .circuit_breaker_cooldown_for(self.instances[inst_idx].consecutive_losses)
                        {
                            self.instances[inst_idx].circuit_breaker_until = Some(Instant::now() + cooldown);
                            self.instances[inst_idx].circuit_breaker_until_ts =
                                Some(now_ts + cooldown.as_secs() as i64);
                            log::warn!(
                                "[CIRCUIT_BREAKER] activated after {} consecutive losses, cooldown {}s",
                                self.instances[inst_idx].consecutive_losses, cooldown.as_secs()
                            );
                        }
                    } else if pnl_value > 0.0 {
                        if self.instances[inst_idx].consecutive_losses > 0 {
                            log::info!(
                                "[CIRCUIT_BREAKER] reset after win (was {} consecutive losses)",
                                self.instances[inst_idx].consecutive_losses
                            );
                            risk_state_dirty = true;
                        }
                        self.instances[inst_idx].consecutive_losses = 0;
                        self.instances[inst_idx].circuit_breaker_until = None;
                        self.instances[inst_idx].circuit_breaker_until_ts = None;
                    }
                    if risk_state_dirty {
                        self.persist_risk_state();
                    }
                }
            } else if filled_qtys.values().any(|qty| *qty > Decimal::ZERO) {
                let next_retry = pending.hedge_retry_count.saturating_add(1);
                if next_retry > MAX_EXIT_RETRIES {
                    self.force_close_all_positions(key, "partial_fill").await;
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_exit = None;
                    }
                    return Ok(());
                }
                log::info!(
                    "[ORDER] {} exit leg partially filled, reissuing remaining legs",
                    key
                );
                self.cancel_pending_orders(&pending).await?;
                if let Some(new_pending) = self
                    .reissue_partial_legs(&pending, &filled_qtys, price_map, true, true, next_retry)
                    .await?
                {
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_exit = Some(new_pending);
                    }
                } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    state.pending_exit = None;
                }
                return Ok(());
            } else if pending.placed_at.elapsed() >= timeout
                || pending
                    .exit_taker_takeover_at
                    .map_or(false, |t| Instant::now() >= t)
                || status.open_remaining == 0
            {
                let takeover_fired = pending
                    .exit_taker_takeover_at
                    .map_or(false, |t| Instant::now() >= t)
                    && pending.placed_at.elapsed() < timeout
                    && status.open_remaining > 0;
                if takeover_fired {
                    // bot-strategy#408: post-only exit hit its dedicated taker-
                    // takeover deadline (Extended `exit_post_only_timeout_secs`)
                    // before `order_timeout_secs` elapsed. Drop one
                    // grep-friendly WARN that mirrors the legacy in-step
                    // monitor's `[EXIT_FILL_TIMEOUT]` log so error-watch /
                    // dashboards keep working.
                    log::warn!(
                        "[EXIT_FILL_TIMEOUT] {} post-only takeover after {}s; cancelling {} legs and reissuing as taker",
                        key,
                        pending.placed_at.elapsed().as_secs(),
                        status.open_remaining,
                    );
                }
                let next_retry = pending.hedge_retry_count.saturating_add(1);
                if next_retry > MAX_EXIT_RETRIES {
                    self.force_close_all_positions(key, "timeout").await;
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.pending_exit = None;
                    }
                    return Ok(());
                }
                if status.open_remaining > 0 && !takeover_fired {
                    log::warn!(
                        "[ORDER] {} exit orders stale ({}s), cancelling {} legs",
                        key,
                        pending.placed_at.elapsed().as_secs(),
                        status.open_remaining
                    );
                    for leg in &pending.legs {
                        let filled = filled_qtys
                            .get(&leg.order_id)
                            .cloned()
                            .unwrap_or(Decimal::ZERO);
                        let is_open = status.open_ids.contains(&leg.order_id);
                        log::debug!(
                            "[ORDER] {} exit leg status symbol={} order_id={} target={} filled={} open={}",
                            key,
                            leg.symbol,
                            leg.order_id,
                            leg.target,
                            filled,
                            is_open
                        );
                    }
                    self.cancel_pending_orders(&pending).await?;
                }
                // Re-attempt closing missing legs based on filled qty
                // reusing filled_qtys defined earlier
                let mut new_legs = Vec::new();
                for leg in &pending.legs {
                    let filled = filled_qtys
                        .get(&leg.order_id)
                        .cloned()
                        .unwrap_or(Decimal::ZERO);
                    let remaining_qty = (leg.target - filled).max(Decimal::ZERO);
                    if remaining_qty > Decimal::ZERO {
                        let quantized =
                            self.quantize_order_size_exit(&leg.symbol, remaining_qty, price_map);
                        if quantized <= Decimal::ZERO {
                            continue;
                        }
                        let limit = None;
                        match self
                            .connector
                            .create_order(&leg.symbol, quantized, leg.side, limit, None, true, None)
                            .await
                        {
                            Ok(resp) => {
                                new_legs.push(PendingLeg {
                                    symbol: leg.symbol.clone(),
                                    order_id: resp.order_id,
                                    exchange_order_id: resp.exchange_order_id,
                                    target: quantized,
                                    filled: Decimal::ZERO,
                                    side: leg.side,
                                    limit_price: None,
                                });
                                log::warn!(
                                    "[ORDER] Retrying exit leg {} size={} mode=MARKET",
                                    leg.symbol,
                                    quantized
                                );
                            }
                            Err(e) if engine::error_class::is_reduce_only_rejection(&e) => {
                                // Extended code 1136/1137: bot-side qty exceeds the actual
                                // residual position. Query the exchange for residual size
                                // and resubmit with min(quantized, actual). 0 → already flat.
                                match self
                                    .fetch_residual_position_size(&leg.symbol)
                                    .await
                                {
                                    Some(actual) if actual == Decimal::ZERO => {
                                        log::info!(
                                            "[ORDER] {} retry skipped; positions already flat",
                                            leg.symbol
                                        );
                                    }
                                    Some(actual) => {
                                        let target = quantized.min(actual);
                                        let downsized = self.quantize_order_size_exit(
                                            &leg.symbol,
                                            target,
                                            price_map,
                                        );
                                        if downsized <= Decimal::ZERO {
                                            log::info!(
                                                "[ORDER] {} retry skipped; residual {} below min lot",
                                                leg.symbol,
                                                actual
                                            );
                                            continue;
                                        }
                                        match self
                                            .connector
                                            .create_order(
                                                &leg.symbol,
                                                downsized,
                                                leg.side,
                                                None,
                                                None,
                                                true,
                                                None,
                                            )
                                            .await
                                        {
                                            Ok(resp) => {
                                                new_legs.push(PendingLeg {
                                                    symbol: leg.symbol.clone(),
                                                    order_id: resp.order_id,
                                                    exchange_order_id: resp.exchange_order_id,
                                                    target: downsized,
                                                    filled: Decimal::ZERO,
                                                    side: leg.side,
                                                    limit_price: None,
                                                });
                                                log::warn!(
                                                    "[ORDER] Retrying exit leg {} size={} mode=MARKET (sized down from {})",
                                                    leg.symbol,
                                                    downsized,
                                                    quantized
                                                );
                                            }
                                            Err(e2) => log::error!(
                                                "[ORDER] Failed to retry sized-down exit leg {}: {:?}",
                                                leg.symbol,
                                                e2
                                            ),
                                        }
                                    }
                                    None => log::error!(
                                        "[ORDER] Failed to retry exit leg {}: {:?} (residual check failed)",
                                        leg.symbol,
                                        e
                                    ),
                                }
                            }
                            Err(e) => log::error!(
                                "[ORDER] Failed to retry exit leg {}: {:?}",
                                leg.symbol,
                                e
                            ),
                        }
                    }
                }
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    if new_legs.is_empty() {
                        state.pending_exit = None;
                        // Keep position state unchanged; will retry next loop
                    } else {
                        state.pending_exit = Some(PendingOrders {
                            legs: new_legs,
                            direction: pending.direction,
                            placed_at: Instant::now(),
                            hedge_retry_count: next_retry,
                            post_only_hybrid: false,
                            // This branch reissues remaining exit legs (post-
                            // only retry, not taker); the dedicated post-only
                            // takeover deadline does not apply here — the
                            // generic `order_timeout_secs` will govern.
                            exit_taker_takeover_at: None,
                        });
                    }
                }
            } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                state.pending_exit = Some(pending);
            }
        }

        Ok(())
    }

    async fn cancel_pending_orders(&self, pending: &PendingOrders) -> Result<()> {
        let mut by_symbol: HashMap<String, Vec<String>> = HashMap::new();
        for leg in &pending.legs {
            by_symbol
                .entry(leg.symbol.clone())
                .or_default()
                .push(leg.order_id.clone());
        }
        for (symbol, order_ids) in by_symbol {
            if let Err(e) = self
                .connector
                .cancel_orders(Some(symbol.clone()), order_ids.clone())
                .await
            {
                log::error!(
                    "[ORDER] cancel failed for {} ({} ids): {:?}",
                    symbol,
                    order_ids.len(),
                    e
                );
            }
        }
        Ok(())
    }

    async fn pending_status(&self, pending: &PendingOrders) -> Result<PendingStatus> {
        let mut open_remaining = 0;
        let mut fills: HashMap<String, Decimal> = HashMap::new();
        let mut filled_values: HashMap<String, Decimal> = HashMap::new();
        let mut filled_fees: HashMap<String, Decimal> = HashMap::new();
        let mut open_ids: HashSet<String> = HashSet::new();
        let mut per_symbol_open: HashMap<String, HashSet<String>> = HashMap::new();
        let mut per_symbol_fill: HashMap<String, HashSet<String>> = HashMap::new();
        for leg in &pending.legs {
            per_symbol_open
                .entry(leg.symbol.clone())
                .or_default()
                .insert(leg.order_id.clone());
            let fill_ids = per_symbol_fill.entry(leg.symbol.clone()).or_default();
            fill_ids.insert(leg.order_id.clone());
            if let Some(exchange_id) = &leg.exchange_order_id {
                fill_ids.insert(exchange_id.clone());
            }
        }
        for (symbol, open_ids_filter) in per_symbol_open.iter() {
            let fill_ids_filter = per_symbol_fill.get(symbol).cloned().unwrap_or_default();
            let open = self
                .connector
                .get_open_orders(symbol)
                .await
                .with_context(|| format!("open orders {}", symbol))?;
            let mut open_count = 0;
            for order in open
                .orders
                .iter()
                .filter(|o| open_ids_filter.contains(&o.order_id))
            {
                open_ids.insert(order.order_id.clone());
                open_count += 1;
            }
            open_remaining += open_count;

            let filled = self
                .connector
                .get_filled_orders(symbol)
                .await
                .with_context(|| format!("filled orders {}", symbol))?;
            for order in filled.orders {
                if fill_ids_filter.contains(&order.order_id) {
                    let sz = order.filled_size.unwrap_or(Decimal::ZERO);
                    *fills.entry(order.order_id.clone()).or_default() += sz;
                    if let Some(value) = order.filled_value {
                        *filled_values.entry(order.order_id.clone()).or_default() += value;
                    }
                    if let Some(fee) = order.filled_fee {
                        *filled_fees.entry(order.order_id.clone()).or_default() += fee;
                    }
                    log::debug!(
                        "[ORDER][FILLED] symbol={} order_id={} side={:?} size={} value={:?} fee={:?} trade_id={}",
                        symbol,
                        order.order_id,
                        order.filled_side,
                        sz,
                        order.filled_value,
                        order.filled_fee,
                        order.trade_id
                    );
                }
            }
            log::debug!(
                "[ORDER][PENDING_STATUS] symbol={} open_orders={} tracked_orders={} filled_entries={}",
                symbol,
                open_count,
                open_ids_filter.len(),
                fills.len()
            );
        }
        Ok(PendingStatus {
            open_remaining,
            fills,
            filled_values,
            filled_fees,
            open_ids,
        })
    }

    /// Look up the per-order-id fill, falling back to the exchange-side
    /// order id when present. `&self` is not needed — promoted to an
    /// associated fn so unit tests can exercise the lookup without
    /// constructing an engine. bot-strategy#396.
    fn leg_fill_from_map(leg: &PendingLeg, fills: &HashMap<String, Decimal>) -> Decimal {
        fills
            .get(&leg.order_id)
            .cloned()
            .or_else(|| {
                leg.exchange_order_id
                    .as_ref()
                    .and_then(|id| fills.get(id).cloned())
            })
            .unwrap_or(Decimal::ZERO)
    }

    /// Per-leg slippage / fee bps observation (#314 Group 4-B). Reads the
    /// volume-weighted fill price out of `status.filled_values` and
    /// compares it to `leg.limit_price`. Only fires for legs that have
    /// both a posted limit (post-only / limit) and a non-zero filled
    /// size + value reported by the venue. Sign convention: positive =
    /// cost (paid more / received less than posted limit).
    fn observe_leg_execution_quality(
        variant: &str,
        pair: &str,
        leg_type: &str,
        legs: &[PendingLeg],
        status: &PendingStatus,
    ) {
        for leg in legs {
            let fill_size = Self::leg_fill_from_map(leg, &status.fills);
            if fill_size <= Decimal::ZERO {
                continue;
            }
            let lookup_value = |map: &HashMap<String, Decimal>| {
                map.get(&leg.order_id).cloned().or_else(|| {
                    leg.exchange_order_id
                        .as_ref()
                        .and_then(|id| map.get(id).cloned())
                })
            };
            if let Some(fill_value) = lookup_value(&status.filled_values) {
                if fill_value > Decimal::ZERO {
                    if let Some(fill_value_f64) = fill_value.to_f64() {
                        if let Some(limit) = leg.limit_price {
                            if let (Some(limit_f64), Some(size_f64)) =
                                (limit.to_f64(), fill_size.to_f64())
                            {
                                if limit_f64 > 0.0 && size_f64 > 0.0 {
                                    let avg_price = fill_value_f64 / size_f64;
                                    let sign = match leg.side {
                                        dex_connector::OrderSide::Long => 1.0,
                                        dex_connector::OrderSide::Short => -1.0,
                                    };
                                    let slippage_bps =
                                        sign * (avg_price - limit_f64) / limit_f64 * 10_000.0;
                                    super::super::prom::LEG_SLIPPAGE_BPS
                                        .with_label_values(&[variant, pair, leg_type])
                                        .observe(slippage_bps);
                                }
                            }
                        }
                        if let Some(fee) = lookup_value(&status.filled_fees) {
                            if let Some(fee_f64) = fee.to_f64() {
                                let fee_bps = fee_f64 / fill_value_f64 * 10_000.0;
                                super::super::prom::LEG_FEE_BPS
                                    .with_label_values(&[variant, pair, leg_type])
                                    .observe(fee_bps);
                            }
                        }
                    }
                }
            }
        }
    }

    fn update_pending_fills(pending: &mut PendingOrders, fills: &HashMap<String, Decimal>) {
        for leg in &mut pending.legs {
            let filled = Self::leg_fill_from_map(leg, fills);
            if filled > leg.filled {
                leg.filled = filled.min(leg.target);
            }
        }
    }

    fn filled_for_leg(leg: &PendingLeg, fills: &HashMap<String, Decimal>) -> Decimal {
        let filled = Self::leg_fill_from_map(leg, fills);
        filled.max(leg.filled).min(leg.target)
    }

    fn filled_by_leg(
        pending: &PendingOrders,
        fills: &HashMap<String, Decimal>,
    ) -> HashMap<String, Decimal> {
        let mut map = HashMap::new();
        for leg in &pending.legs {
            let filled = Self::filled_for_leg(leg, fills);
            map.insert(leg.order_id.clone(), filled);
        }
        map
    }

    fn all_filled(pending: &PendingOrders, fills: &HashMap<String, Decimal>) -> bool {
        pending
            .legs
            .iter()
            .all(|leg| Self::filled_for_leg(leg, fills) >= leg.target)
    }
}

#[cfg(test)]
mod tests {
    //! Coverage for the fill-aggregation helpers used by
    //! `reconcile_pending_orders`. These do not read engine state, so we
    //! exercise them directly through `PairTradeEngine::<helper>(...)`
    //! without standing up an engine. Reaches the leg-fill aggregation
    //! that drives partial / full / fallback branching in the reconcile
    //! loop. bot-strategy#396.
    use std::collections::HashMap;
    use std::time::Instant;

    use dex_connector::OrderSide;
    use rust_decimal::Decimal;

    use super::super::super::state::{PendingLeg, PendingOrders, PositionDirection};
    use super::PairTradeEngine;

    fn dec(v: &str) -> Decimal {
        v.parse().unwrap()
    }

    fn leg(symbol: &str, order_id: &str, target: &str, filled: &str) -> PendingLeg {
        PendingLeg {
            symbol: symbol.to_string(),
            order_id: order_id.to_string(),
            exchange_order_id: None,
            target: dec(target),
            filled: dec(filled),
            side: OrderSide::Long,
            limit_price: None,
        }
    }

    fn leg_with_exchange(
        symbol: &str,
        order_id: &str,
        exchange_id: &str,
        target: &str,
    ) -> PendingLeg {
        PendingLeg {
            symbol: symbol.to_string(),
            order_id: order_id.to_string(),
            exchange_order_id: Some(exchange_id.to_string()),
            target: dec(target),
            filled: Decimal::ZERO,
            side: OrderSide::Long,
            limit_price: None,
        }
    }

    fn pending(legs: Vec<PendingLeg>) -> PendingOrders {
        PendingOrders {
            legs,
            direction: PositionDirection::LongSpread,
            placed_at: Instant::now(),
            hedge_retry_count: 0,
            post_only_hybrid: false,
            exit_taker_takeover_at: None,
        }
    }

    #[test]
    fn leg_fill_returns_zero_when_neither_id_present() {
        let l = leg("BTC", "ord-1", "1.0", "0.0");
        let fills: HashMap<String, Decimal> = HashMap::new();
        assert_eq!(
            PairTradeEngine::leg_fill_from_map(&l, &fills),
            Decimal::ZERO
        );
    }

    #[test]
    fn leg_fill_prefers_internal_order_id_over_exchange_id() {
        // When both ids are present in the fills map, the internal
        // bot-assigned id wins. Locks the "primary key" invariant the
        // reconcile loop relies on when reissuing partial legs.
        let l = leg_with_exchange("BTC", "ord-1", "exch-9", "1.0");
        let mut fills = HashMap::new();
        fills.insert("ord-1".to_string(), dec("0.3"));
        fills.insert("exch-9".to_string(), dec("0.7"));
        assert_eq!(
            PairTradeEngine::leg_fill_from_map(&l, &fills),
            dec("0.3")
        );
    }

    #[test]
    fn leg_fill_falls_back_to_exchange_order_id_when_internal_missing() {
        // Extended often surfaces fills under its own exchange-side id;
        // the reconcile loop must still aggregate them onto the right leg.
        let l = leg_with_exchange("BTC", "ord-1", "exch-9", "1.0");
        let mut fills = HashMap::new();
        fills.insert("exch-9".to_string(), dec("0.5"));
        assert_eq!(
            PairTradeEngine::leg_fill_from_map(&l, &fills),
            dec("0.5")
        );
    }

    #[test]
    fn filled_for_leg_caps_at_target_when_exchange_overreports() {
        // Exchange briefly reports a filled qty larger than the order
        // target (Extended idempotency-retry artifact). Cap at target so
        // downstream `target - filled` arithmetic in reissue_partial_legs
        // doesn't underflow to negative remaining and skip the reissue.
        let l = leg("BTC", "ord-1", "1.0", "0.0");
        let mut fills = HashMap::new();
        fills.insert("ord-1".to_string(), dec("1.5"));
        assert_eq!(PairTradeEngine::filled_for_leg(&l, &fills), dec("1.0"));
    }

    #[test]
    fn filled_for_leg_never_regresses_below_leg_filled() {
        // If the in-memory `leg.filled` is already higher than the
        // exchange map (e.g. between two polls only the older value is
        // still in flight), keep the higher value. Monotonic per-leg fill
        // accounting prevents the reconcile loop from re-reissuing a leg
        // it has already accepted as further-along.
        let l = leg("BTC", "ord-1", "1.0", "0.6");
        let mut fills = HashMap::new();
        fills.insert("ord-1".to_string(), dec("0.4"));
        assert_eq!(PairTradeEngine::filled_for_leg(&l, &fills), dec("0.6"));
    }

    #[test]
    fn update_pending_fills_advances_leg_filled_monotonically() {
        // The reconcile loop's first action: roll the exchange's fill
        // view into `leg.filled` if it has advanced. Must not regress.
        let mut p = pending(vec![
            leg("BTC", "ord-1", "1.0", "0.2"), // stays at 0.6 from map
            leg("ETH", "ord-2", "2.0", "1.5"), // map shows 0.0 → unchanged
        ]);
        let mut fills = HashMap::new();
        fills.insert("ord-1".to_string(), dec("0.6"));
        // ord-2 deliberately missing from map.
        PairTradeEngine::update_pending_fills(&mut p, &fills);
        assert_eq!(p.legs[0].filled, dec("0.6"));
        assert_eq!(p.legs[1].filled, dec("1.5"));
    }

    #[test]
    fn update_pending_fills_clamps_advance_at_target() {
        // Same overreport scenario as filled_for_leg, but through the
        // in-place update path that mutates the engine's pending state.
        let mut p = pending(vec![leg("BTC", "ord-1", "1.0", "0.0")]);
        let mut fills = HashMap::new();
        fills.insert("ord-1".to_string(), dec("1.7"));
        PairTradeEngine::update_pending_fills(&mut p, &fills);
        assert_eq!(p.legs[0].filled, dec("1.0"));
    }

    #[test]
    fn filled_by_leg_keys_by_internal_order_id() {
        // Downstream reissue path looks up by internal id only — the
        // aggregator must therefore also surface fills under the internal
        // id even when the source row carried only an exchange id.
        let p = pending(vec![
            leg_with_exchange("BTC", "ord-1", "exch-9", "1.0"),
            leg("ETH", "ord-2", "2.0", "0.0"),
        ]);
        let mut fills = HashMap::new();
        fills.insert("exch-9".to_string(), dec("0.5"));
        fills.insert("ord-2".to_string(), dec("2.0"));
        let by_leg = PairTradeEngine::filled_by_leg(&p, &fills);
        assert_eq!(by_leg.get("ord-1"), Some(&dec("0.5")));
        assert_eq!(by_leg.get("ord-2"), Some(&dec("2.0")));
    }

    #[test]
    fn all_filled_true_when_every_leg_meets_target() {
        let p = pending(vec![
            leg("BTC", "ord-1", "1.0", "0.0"),
            leg("ETH", "ord-2", "2.0", "0.0"),
        ]);
        let mut fills = HashMap::new();
        fills.insert("ord-1".to_string(), dec("1.0"));
        fills.insert("ord-2".to_string(), dec("2.0"));
        assert!(PairTradeEngine::all_filled(&p, &fills));
    }

    #[test]
    fn all_filled_false_when_any_leg_short() {
        // The reconcile partial-fill branch fires when this returns false
        // *and* at least one leg has non-zero fill. Exactly-zero fills
        // route to the stale / timeout branch instead.
        let p = pending(vec![
            leg("BTC", "ord-1", "1.0", "0.0"),
            leg("ETH", "ord-2", "2.0", "0.0"),
        ]);
        let mut fills = HashMap::new();
        fills.insert("ord-1".to_string(), dec("1.0"));
        fills.insert("ord-2".to_string(), dec("1.9")); // short
        assert!(!PairTradeEngine::all_filled(&p, &fills));
    }

    #[test]
    fn all_filled_uses_in_memory_filled_when_map_silent() {
        // After an `update_pending_fills` advance, the next reconcile poll
        // may see no map entry for an already-completed leg. The
        // aggregator must still treat it as filled to avoid reissuing.
        let p = pending(vec![leg("BTC", "ord-1", "1.0", "1.0")]);
        let fills: HashMap<String, Decimal> = HashMap::new();
        assert!(PairTradeEngine::all_filled(&p, &fills));
    }
}
