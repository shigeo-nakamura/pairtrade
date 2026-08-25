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
use tokio::time::sleep;

use super::super::apply_post_exit_state;
use super::super::defaults::MAX_EXIT_RETRIES;
use super::super::engine;
use super::super::execution_ledger::{self, ExecutionLegFillRecord, ExecutionPairSummaryRecord};
use super::super::exit::compute_pnl;
use super::super::funding_history;
use super::super::market::SymbolSnapshot;
use super::super::pnl_log::{PnlLogRecord, PnlTradeDetails};
use super::super::prom;
use super::super::state::{PairState, PendingLeg, PendingOrders, PendingStatus, Position};
use super::super::PairTradeEngine;
use super::placement::ReissuePartialLegsRequest;

/// Narrow inputs for `build_exit_fill_pnl`, grouped so the helper does not
/// need `&self` while the caller holds a `&mut` borrow on the instance's
/// `PairState` (bot-strategy#502).
struct ExitFillPnlContext<'a> {
    inst_id: &'a str,
    key: &'a str,
    state: &'a PairState,
    price_map: &'a HashMap<String, SymbolSnapshot>,
    /// Exit legs (settled slices + final remainder) and their fill status,
    /// used to derive actual exit fill VWAP instead of a mark snapshot
    /// (bot-strategy#750).
    legs: &'a [PendingLeg],
    status: &'a PendingStatus,
    funding_history: &'a funding_history::FundingHistory,
    z_exit: Option<f64>,
    beta_val: Option<f64>,
    now_ts: i64,
}

/// Pure outcome of the entry partial-fill reissue policy. Extracted from
/// `reconcile_pending_orders` so the retry/escalation thresholds are
/// auditable and unit-testable without driving the async state machine
/// (bot-strategy#502).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartialFillDecision {
    /// `hedge_retry_count + 1`: the retry index this reissue would become.
    next_retry: u32,
    /// Hard cap hit (bot-strategy#480): cancel pending, flatten filled
    /// legs, and clear `pending_entry`. Takes precedence over `use_market`.
    give_up: bool,
    /// Escalate the remaining legs to MARKET (taker) instead of another
    /// limit reissue.
    use_market: bool,
}

impl PairTradeEngine {
    /// Pure decision for the partial-fill entry-reissue branch: from the
    /// current retry count and the configured caps, derive the next retry
    /// index, whether to give up (hard cap), and whether to escalate to
    /// MARKET. `0` for either cap disables that cap (legacy behaviour).
    /// Mirrors the inline logic this replaced exactly. bot-strategy#502.
    fn decide_partial_fill_reissue(
        hedge_retry_count: u32,
        max_retries: u32,
        giveup_retries: u32,
    ) -> PartialFillDecision {
        let next_retry = hedge_retry_count.saturating_add(1);
        PartialFillDecision {
            next_retry,
            give_up: giveup_retries > 0 && next_retry > giveup_retries,
            use_market: max_retries > 0 && next_retry > max_retries,
        }
    }

    /// Pure derivation of the entry-fill record's per-leg prices and sizes:
    /// split the `base/quote` pair key, derive each side's actual fill VWAP
    /// from `status` (falling back to the `price_map` mark snapshot only on
    /// total value-coverage blackout, bot-strategy#750), and sum the
    /// filled-leg sizes by symbol. Returns `(price_a, price_b, size_a,
    /// size_b)`; all `None` when `key` is not a `base/quote` pair. Extracted
    /// from `reconcile_pending_orders` so the entry-price field derivation
    /// is auditable/testable (bot-strategy#502).
    #[allow(clippy::type_complexity)]
    fn entry_prices_and_sizes(
        key: &str,
        price_map: &HashMap<String, SymbolSnapshot>,
        legs: &[PendingLeg],
        status: &PendingStatus,
    ) -> (
        Option<Decimal>,
        Option<Decimal>,
        Option<Decimal>,
        Option<Decimal>,
    ) {
        match key.split_once('/') {
            Some((base, quote)) => {
                let (es_a, es_b) = Self::sum_entry_sizes_by_symbol(legs, base, quote);
                (
                    Self::fill_price_or_snapshot(legs, status, price_map, key, "entry", base),
                    Self::fill_price_or_snapshot(legs, status, price_map, key, "entry", quote),
                    es_a,
                    es_b,
                )
            }
            None => (None, None, None, None),
        }
    }

    /// Assemble the `exit_fill` PnL record for a fully-filled exit: compute
    /// realized PnL from current mark prices, fold in funding carry when the
    /// entry sizes/prices were captured, emit the close gross/funding bps
    /// histograms, and stamp the close reason. Returns
    /// `(record, realized_pnl, funding_carry_usd)`; `None` when the position
    /// or any required price is missing — matching the original nested
    /// if-lets it replaced. Pure relocation from `reconcile_exit`
    /// (bot-strategy#502).
    fn build_exit_fill_pnl(ctx: ExitFillPnlContext<'_>) -> Option<(PnlLogRecord, f64, f64)> {
        let pos = ctx.state.position.as_ref()?;
        let (base, quote) = ctx.key.split_once('/')?;
        // bot-strategy#750: PnL, trade stats, and the consecutive-loss
        // circuit breaker must be driven by actual exit fill VWAP, not the
        // reconciliation-time mark snapshot `build_exit_fill_pnl` used to
        // read here — the two can differ materially after a partial-fill
        // amend/reissue. Falls back to the snapshot only on total
        // value-coverage blackout (logged in `fill_price_or_snapshot`).
        let exit_price_a = Self::fill_price_or_snapshot(
            ctx.legs,
            ctx.status,
            ctx.price_map,
            ctx.key,
            "exit",
            base,
        )?;
        let exit_price_b = Self::fill_price_or_snapshot(
            ctx.legs,
            ctx.status,
            ctx.price_map,
            ctx.key,
            "exit",
            quote,
        )?;
        let pnl = compute_pnl(pos, exit_price_a, exit_price_b).and_then(|p| p.to_f64())?;
        let hold_secs = Some(ctx.now_ts.saturating_sub(pos.entered_ts).max(0) as f64);
        let entry_a = pos.entry_price_a.and_then(|v| v.to_f64());
        let entry_b = pos.entry_price_b.and_then(|v| v.to_f64());
        let (carry_usd, ticks_observed) = match (
            pos.entry_size_a,
            pos.entry_price_a,
            pos.entry_size_b,
            pos.entry_price_b,
        ) {
            (Some(sa), Some(pa), Some(sb), Some(pb)) => {
                funding_history::compute_carry_usd(funding_history::FundingCarryInput {
                    history: ctx.funding_history,
                    base_symbol: base,
                    quote_symbol: quote,
                    open_ts: pos.entered_ts,
                    close_ts: ctx.now_ts,
                    direction: pos.direction,
                    entry_size_a: sa,
                    entry_price_a: pa,
                    entry_size_b: sb,
                    entry_price_b: pb,
                })
            }
            _ => (0.0, 0),
        };
        let mut record =
            PnlLogRecord::new(base, quote, pos.direction, pnl, ctx.now_ts, "exit_fill")
                .with_trade_details(PnlTradeDetails {
                    entry_a,
                    entry_b,
                    exit_a: exit_price_a.to_f64(),
                    exit_b: exit_price_b.to_f64(),
                    beta: ctx.beta_val,
                    z_entry: pos.entry_z,
                    z_exit: ctx.z_exit,
                    hold_secs,
                });
        if ticks_observed > 0 {
            record = record.with_funding(carry_usd, ticks_observed);
        }
        // #314 Group 4: emit gross/funding bps to Prometheus. Same
        // Some(...,...,...,...) gate as the funding compute above so the
        // bps denominator stays consistent. `reason` is the same
        // Option<&'static str> that apply_post_exit_state consumes
        // immediately after this — read without .take() so the
        // close-reason counter still receives it (bot-strategy#421).
        let reason = ctx.state.pending_exit_reason.unwrap_or("unknown");
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
                        .with_label_values(&[ctx.inst_id, ctx.key, reason])
                        .observe(pnl / notional * 10_000.0);
                    if ticks_observed > 0 {
                        super::super::prom::CLOSE_FUNDING_BPS
                            .with_label_values(&[ctx.inst_id, ctx.key])
                            .observe(carry_usd / notional * 10_000.0);
                    }
                }
            }
        }
        record = record.with_close_reason(reason);
        Some((record, pnl, carry_usd))
    }

    pub(in crate::pairtrade) async fn reconcile_pending_orders(
        &mut self,
        inst_idx: usize,
        key: &str,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Result<()> {
        let timeout = Duration::from_secs(self.cfg.order_timeout_secs.max(1));
        let now_ts = self.current_now_ts();
        let (pending_entry, pending_exit) = {
            let state = self.instances[inst_idx]
                .states
                .get_mut(key)
                .ok_or_else(|| anyhow!("missing state for {}", key))?;
            (state.pending_entry.take(), state.pending_exit.take())
        };

        if let Some(pending) = pending_entry {
            if self
                .reconcile_entry(inst_idx, key, price_map, pending, now_ts, timeout)
                .await?
            {
                return Ok(());
            }
        }

        if let Some(pending) = pending_exit {
            self.reconcile_exit(inst_idx, key, price_map, pending, now_ts, timeout)
                .await?;
        }

        Ok(())
    }

    /// Post-exit risk bookkeeping for a realized exit fill: fold the cycle's
    /// PnL / funding carry into the session counters, advance or reset the
    /// consecutive-loss circuit breaker, and persist the risk-state snapshot.
    /// `write_pnl_record` always bumps total_trades / total_pnl (persisted,
    /// bot-strategy#320), so the snapshot is dirty regardless of pnl sign.
    /// Pure relocation from `reconcile_exit` (bot-strategy#502).
    fn record_exit_realized_pnl(
        &mut self,
        inst_idx: usize,
        now_ts: i64,
        pnl_value: f64,
        funding_value: f64,
    ) {
        self.instances[inst_idx].realized_pnl_today += pnl_value;
        self.instances[inst_idx].funding_carry_today += funding_value;
        self.instances[inst_idx].total_funding_carry += funding_value;
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
                    self.instances[inst_idx].consecutive_losses,
                    cooldown.as_secs()
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

    /// Re-attempt closing the remaining (unfilled) exit legs as MARKET
    /// orders, sizing each retry down to the exchange-reported residual on a
    /// reduce-only rejection (Extended 1136/1137). Returns the freshly
    /// placed legs; an empty vec means nothing was reissued (caller clears
    /// `pending_exit` and retries next loop). Pure relocation of the exit
    /// retry loop from `reconcile_exit` (bot-strategy#502).
    async fn retry_exit_remaining_legs(
        &mut self,
        pending: &PendingOrders,
        filled_qtys: &HashMap<String, Decimal>,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Vec<PendingLeg> {
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
                // Reference for the taker retry — feeds the
                // taker-side slippage histogram (#314 Group
                // 4-B-2). The downsized retry below uses the
                // same captured value.
                let ref_price_retry = self.order_reference_price(&leg.symbol, leg.side, price_map);
                let meta = self.order_submit_metadata(&leg.symbol, quantized, leg.side, price_map);
                match self
                    .connector
                    .create_order(&leg.symbol, quantized, leg.side, limit, None, true, None)
                    .await
                    .map(|resp| Self::order_result_from_response(resp, false, None, meta))
                {
                    Ok(resp) => {
                        new_legs.push(Self::pending_leg_from_order(
                            leg.symbol.clone(),
                            leg.side,
                            quantized,
                            Decimal::ZERO,
                            ref_price_retry,
                            true,
                            resp,
                        ));
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
                        match self.fetch_residual_position_size(&leg.symbol).await {
                            Some(actual) if actual == Decimal::ZERO => {
                                log::info!(
                                    "[ORDER] {} retry skipped; positions already flat",
                                    leg.symbol
                                );
                            }
                            Some(actual) => {
                                let target = quantized.min(actual);
                                let downsized =
                                    self.quantize_order_size_exit(&leg.symbol, target, price_map);
                                if downsized <= Decimal::ZERO {
                                    log::info!(
                                        "[ORDER] {} retry skipped; residual {} below min lot",
                                        leg.symbol,
                                        actual
                                    );
                                    continue;
                                }
                                let meta = self.order_submit_metadata(
                                    &leg.symbol,
                                    downsized,
                                    leg.side,
                                    price_map,
                                );
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
                                    .map(|resp| {
                                        Self::order_result_from_response(resp, false, None, meta)
                                    }) {
                                    Ok(resp) => {
                                        new_legs.push(Self::pending_leg_from_order(
                                            leg.symbol.clone(),
                                            leg.side,
                                            downsized,
                                            Decimal::ZERO,
                                            ref_price_retry,
                                            true,
                                            resp,
                                        ));
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
                    Err(e) => {
                        log::error!("[ORDER] Failed to retry exit leg {}: {:?}", leg.symbol, e)
                    }
                }
            }
        }
        new_legs
    }

    /// Exit-side branch of `reconcile_pending_orders`, extracted to a
    /// `&mut self` helper (bot-strategy#502 item2). The exit branch was the
    /// final block in the method, so no early-return signalling is needed:
    /// the in-block `return Ok(())` sites simply return from the helper and
    /// the caller falls through to its own `Ok(())`, matching the original.
    /// Pure structural move: the body is the former exit if-let block verbatim.
    async fn reconcile_exit(
        &mut self,
        inst_idx: usize,
        key: &str,
        price_map: &HashMap<String, SymbolSnapshot>,
        pending: PendingOrders,
        now_ts: i64,
        timeout: Duration,
    ) -> Result<()> {
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
            let close_reason = self.instances[inst_idx]
                .states
                .get(key)
                .and_then(|s| s.pending_exit_reason)
                .unwrap_or("unknown")
                .to_string();
            let z_exit = self
                .per_pair_state
                .get(key)
                .and_then(|s| s.z_score().map(|(z, _)| z));
            let beta_val = self.per_pair_state.get(key).map(|s| s.beta);
            if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                pnl_record = Self::build_exit_fill_pnl(ExitFillPnlContext {
                    inst_id: &inst_id,
                    key,
                    state,
                    price_map,
                    legs: &pending.legs,
                    status: &status,
                    funding_history: &self.funding_history,
                    z_exit,
                    beta_val,
                    now_ts,
                });
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
            self.record_leg_execution_quality(
                &inst_id,
                key,
                "exit",
                Some(&close_reason),
                &pending.legs,
                &status,
                pending.placed_ts_ms,
                pending.hedge_retry_count,
                price_map,
            );
            log::info!("[ORDER] {} exit orders filled", key);
            if let Some((record, pnl_value, funding_value)) = pnl_record {
                self.write_pnl_record(inst_idx, record);
                self.record_exit_realized_pnl(inst_idx, now_ts, pnl_value, funding_value);
            }
        } else if filled_qtys.values().any(|qty| *qty > Decimal::ZERO) {
            let next_retry = pending.hedge_retry_count.saturating_add(1);
            if next_retry > MAX_EXIT_RETRIES {
                self.write_recovery_no_pnl_record(
                    inst_idx,
                    key,
                    pending.direction,
                    "partial_fill",
                    now_ts,
                    price_map,
                );
                let close_confirmed = self.force_close_all_positions(key, "partial_fill").await;
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    state.pending_exit = None;
                    // bot-strategy#514: the context record above covers this
                    // close; suppress the duplicate exchange-snapshot record —
                    // but only when the close was actually confirmed. On a
                    // failed close the position is still live on the exchange
                    // and its eventual out-of-band close must still record.
                    state.recovery_recorded = close_confirmed;
                }
                return Ok(());
            }
            log::info!(
                "[ORDER] {} exit leg partially filled, reissuing remaining legs",
                key
            );
            self.cancel_pending_orders(&pending).await?;
            if let Some(new_pending) = self
                .reissue_partial_legs(ReissuePartialLegsRequest {
                    pending: &pending,
                    filled_qtys: &filled_qtys,
                    price_map,
                    reduce_only: true,
                    use_market: true,
                    retry_count: next_retry,
                    use_amend: false,
                })
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
                .is_some_and(|t| Instant::now() >= t)
            || status.open_remaining == 0
        {
            let takeover_fired = pending
                .exit_taker_takeover_at
                .is_some_and(|t| Instant::now() >= t)
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
                prom::EXIT_POST_ONLY_TAKEOVER_TOTAL
                    .with_label_values(&[self.instances[inst_idx].id.as_str(), key])
                    .inc();
            }
            let next_retry = pending.hedge_retry_count.saturating_add(1);
            if next_retry > MAX_EXIT_RETRIES {
                self.write_recovery_no_pnl_record(
                    inst_idx,
                    key,
                    pending.direction,
                    "timeout",
                    now_ts,
                    price_map,
                );
                let close_confirmed = self.force_close_all_positions(key, "timeout").await;
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    state.pending_exit = None;
                    // bot-strategy#514: the context record above covers this
                    // close; suppress the duplicate exchange-snapshot record —
                    // but only when the close was actually confirmed. On a
                    // failed close the position is still live on the exchange
                    // and its eventual out-of-band close must still record.
                    state.recovery_recorded = close_confirmed;
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
            // Re-attempt closing missing legs based on filled qty,
            // reusing filled_qtys defined earlier.
            let placed_ts_ms = chrono::Utc::now().timestamp_millis();
            let new_legs = self
                .retry_exit_remaining_legs(&pending, &filled_qtys, price_map)
                .await;
            if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                if new_legs.is_empty() {
                    state.pending_exit = None;
                    // Keep position state unchanged; will retry next loop
                } else {
                    state.pending_exit = Some(
                        PendingOrders {
                            legs: new_legs,
                            direction: pending.direction,
                            placed_at: Instant::now(),
                            placed_ts_ms,
                            hedge_retry_count: next_retry,
                            post_only_hybrid: false,
                            // This branch reissues remaining exit legs (post-
                            // only retry, not taker); the dedicated post-only
                            // takeover deadline does not apply here — the
                            // generic `order_timeout_secs` will govern.
                            exit_taker_takeover_at: None,
                        }
                        .with_leg_decision_ts(),
                    );
                }
            }
        } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
            state.pending_exit = Some(pending);
        }
        Ok(())
    }

    /// Emit the `[ORDER_FALLBACK_DETAIL]` instrumentation line when a
    /// post-only entry times out and falls back to taker (bot-strategy#165
    /// Phase 0): per-leg fill status, posted limit vs current book, and the
    /// z-movement from entry to timeout. Pure logging — relocated verbatim
    /// from `reconcile_entry` (bot-strategy#502).
    fn log_post_only_fallback_detail(
        key: &str,
        pending: &PendingOrders,
        status: &PendingStatus,
        price_map: &HashMap<String, SymbolSnapshot>,
        z_entry: f64,
        z_now: f64,
    ) {
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
                    bid.map(|d| d.to_string()).unwrap_or_else(|| "?".into()),
                    ask.map(|d| d.to_string()).unwrap_or_else(|| "?".into()),
                    tick.map(|d| d.to_string()).unwrap_or_else(|| "?".into()),
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
    }

    /// Entry-side branch of `reconcile_pending_orders`, extracted to a
    /// `&mut self` helper (bot-strategy#502 item2). Returns `true` when the
    /// caller should return early (skip exit reconciliation this tick) —
    /// matching the original in-block `return Ok(())` sites; `false` falls
    /// through so the caller proceeds to any pending exit. Pure structural
    /// move: the body is the former entry if-let block verbatim.
    async fn reconcile_entry(
        &mut self,
        inst_idx: usize,
        key: &str,
        price_map: &HashMap<String, SymbolSnapshot>,
        mut pending: PendingOrders,
        now_ts: i64,
        timeout: Duration,
    ) -> Result<bool> {
        let status = self.pending_status(&pending).await?;
        Self::update_pending_fills(&mut pending, &status.fills);
        let mut filled_qtys = Self::filled_by_leg(&pending, &status.fills);
        if Self::all_filled(&pending, &status.fills) {
            let z_at_entry = self
                .per_pair_state
                .get(key)
                .and_then(|s| s.z_score().map(|(z, _)| z));
            // bot-strategy#463: snapshot β at fill time so the
            // re-hedge guard measures drift against the actually-
            // hedged ratio, not against a later re-estimation.
            let beta_at_entry = self.per_pair_state.get(key).map(|s| s.beta);
            if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                let (ep_a, ep_b, es_a, es_b) =
                    Self::entry_prices_and_sizes(key, price_map, &pending.legs, &status);
                state.position = Some(Position {
                    direction: pending.direction,
                    entered_at: Instant::now(),
                    entered_ts: now_ts,
                    entry_price_a: ep_a,
                    entry_price_b: ep_b,
                    entry_size_a: es_a,
                    entry_size_b: es_b,
                    entry_z: z_at_entry,
                    entry_beta: beta_at_entry,
                    last_rehedge_ts: None,
                    rehedge_realized_pnl: None,
                    prev_beta_for_velocity: None,
                });
                state.pending_entry = None;
                state.recovery_recorded = false;
            }
            let inst_id = self.instances[inst_idx].id.clone();
            self.record_leg_execution_quality(
                &inst_id,
                key,
                "entry",
                None,
                &pending.legs,
                &status,
                pending.placed_ts_ms,
                pending.hedge_retry_count,
                price_map,
            );
            log::info!("[ORDER] {} entry orders filled", key);
            // bot-strategy#721: entry completion is not considered safe
            // until the actual venue position per leg reconciles against
            // the intended signed target. Catches the late-fill overfill
            // the pre-submit checks (#470 cap, cancel-ack refresh) cannot:
            // a fill that lands after their snapshot.
            self.reconcile_entry_exposure(inst_idx, key, &pending.legs, price_map)
                .await;
        } else if filled_qtys.values().any(|qty| *qty > Decimal::ZERO) {
            let max_retries = self.cfg.entry_partial_fill_max_retries;
            let giveup_retries = self.cfg.entry_partial_fill_giveup_retries;
            let decision = Self::decide_partial_fill_reissue(
                pending.hedge_retry_count,
                max_retries,
                giveup_retries,
            );
            let next_retry = decision.next_retry;
            // bot-strategy#480: hard cap on the reissue loop. Once
            // `hedge_retry_count` crosses this, give up entirely —
            // cancel pending, flatten any filled legs via
            // `force_close_all_positions`, and clear `pending_entry`
            // so the next ENTRY signal starts from a clean slate.
            // Pre-#480 this loop was unbounded: a stuck reissue
            // (MARKET reissue not progressing because the underlying
            // `create_order(price=None)` was emitting LIMIT GTT on
            // Extended, dex-connector#9) ran for ~75 h / 54 k
            // retries on `debot-pair-btceth-extended`, blocking all
            // entries / exits while burning no capital but leaving
            // operators to discover the loop by hand. The connector
            // fix removes the underlying cause; this cap is the
            // defense-in-depth so any future failure mode that
            // breaks the reissue convergence surfaces as a single
            // ERROR + flatten instead of an unbounded loop.
            //
            // 0 disables the cap (legacy unbounded behaviour) so
            // existing deployments can opt out via yaml or env
            // override during the rollout window if needed.
            if decision.give_up {
                let filled_summary: Vec<String> = pending
                    .legs
                    .iter()
                    .filter_map(|leg| {
                        let q = filled_qtys
                            .get(&leg.order_id)
                            .copied()
                            .unwrap_or(Decimal::ZERO);
                        if q > Decimal::ZERO {
                            Some(format!("{}={}", leg.symbol, q))
                        } else {
                            None
                        }
                    })
                    .collect();
                log::error!(
                        "[ORDER][GIVEUP] {} entry-reissue hit hard cap ({} > {}); cancelling pending and flattening filled legs [{}]",
                        key,
                        next_retry,
                        giveup_retries,
                        filled_summary.join(" ")
                    );
                let variant_id = self.instances[inst_idx].id.clone();
                self.cancel_pending_orders(&pending).await?;
                self.write_recovery_no_pnl_record(
                    inst_idx,
                    key,
                    pending.direction,
                    "entry_reissue_giveup",
                    now_ts,
                    price_map,
                );
                self.force_close_all_positions(key, "entry_reissue_giveup")
                    .await;
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    state.pending_entry = None;
                }
                super::super::prom::ENTRY_REISSUE_GIVEUP_TOTAL
                    .with_label_values(&[&variant_id, key])
                    .inc();
                return Ok(true);
            }
            let use_market = decision.use_market;
            // bot-strategy#471: when amend-on-partial-fill is enabled (and
            // we're not escalating to a market takeover, which a native
            // amend can't retarget the TIF for), skip the blanket cancel
            // so `reissue_partial_legs` can amend the resting order in
            // place. It falls back to cancel+reissue per-leg on any amend
            // error, and cancels any leg it does NOT amend, so the end
            // state matches the legacy path.
            let use_amend = !use_market
                && self
                    .pair_params_for(inst_idx, key)
                    .use_amend_on_partial_fill;
            // bot-strategy#471: log the verb matching the path actually taken
            // — "amending" the resting order in place vs "reissuing"
            // (cancel + replace). The market-takeover branch always reissues.
            let verb = if use_amend { "amending" } else { "reissuing" };
            if use_market {
                log::info!(
                        "[ORDER] {} entry leg partially filled, retries exceeded ({} > {}); reissuing remaining legs as MARKET",
                        key,
                        next_retry,
                        max_retries
                    );
            } else if max_retries > 0 {
                log::info!(
                    "[ORDER] {} entry leg partially filled, {} remaining legs (retry {}/{})",
                    key,
                    verb,
                    next_retry,
                    max_retries
                );
            } else {
                log::warn!(
                    "[ORDER] {} entry leg partially filled, {} remaining legs",
                    key,
                    verb
                );
            }
            if !use_amend {
                self.cancel_pending_orders(&pending).await?;
            }
            // bot-strategy#721: the MARKET takeover is the TOCTOU-prone
            // step — a late fill on the old maker order between the fill
            // snapshot above and the replacement placement overfills the
            // leg (Frankfurt 2026-07-08 09:42:30 UTC, variant A ETH
            // +7.10%). Wait for the cancels to be acknowledged (no order
            // left open → no further fills possible), then refresh the
            // fill state so the remaining MARKET quantity is recomputed
            // immediately before placement.
            if use_market {
                self.await_pending_cancellation(&pending).await;
                match self.pending_status(&pending).await {
                    Ok(refreshed) => {
                        Self::update_pending_fills(&mut pending, &refreshed.fills);
                        filled_qtys = Self::filled_by_leg(&pending, &refreshed.fills);
                    }
                    Err(err) => {
                        // Keep the pre-cancel snapshot: the #470 venue-
                        // position cap inside reissue_partial_legs and the
                        // post-fill reconciliation still cover the gap.
                        log::warn!(
                            "[ORDER] {} post-cancel fill refresh failed; using pre-cancel snapshot: {:?}",
                            key,
                            err
                        );
                    }
                }
            }
            if let Some(new_pending) = self
                .reissue_partial_legs(ReissuePartialLegsRequest {
                    pending: &pending,
                    filled_qtys: &filled_qtys,
                    price_map,
                    reduce_only: false,
                    use_market,
                    retry_count: next_retry,
                    use_amend,
                })
                .await?
            {
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    state.pending_entry = Some(new_pending);
                }
            } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                state.pending_entry = None;
            }
            return Ok(true);
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
                Self::log_post_only_fallback_detail(
                    key, &pending, &status, price_map, z_entry, z_now,
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
            let mut retry_count = pending.hedge_retry_count;
            let max_retries = 3u32;
            let (flattened_any, hedge_failed) = self
                .hedge_partial_entry_legs(
                    &pending,
                    &filled_qtys,
                    price_map,
                    retry_count,
                    max_retries,
                )
                .await;
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
                        state.recovery_recorded = false;
                    }
                }
            }
        } else if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
            state.pending_entry = Some(pending);
        }
        Ok(false)
    }

    /// Flatten the already-filled legs of a timed-out entry by sending the
    /// opposing (reduce-only) order per filled leg: LIMIT for the first
    /// retries, escalating to MARKET at `max_retries`. Returns
    /// `(flattened_any, hedge_failed)` so the caller decides whether to
    /// reschedule the hedge or clear the pending entry. Pure relocation of
    /// the hedge loop from `reconcile_entry` (bot-strategy#502).
    async fn hedge_partial_entry_legs(
        &mut self,
        pending: &PendingOrders,
        filled_qtys: &HashMap<String, Decimal>,
        price_map: &HashMap<String, SymbolSnapshot>,
        retry_count: u32,
        max_retries: u32,
    ) -> (bool, bool) {
        let mut flattened_any = false;
        let mut hedge_failed = false;
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
                        .create_order(&leg.symbol, filled, hedge_side, limit, spread, true, None)
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
        (flattened_any, hedge_failed)
    }

    /// Poll the venue until none of `pending`'s orders remain open — i.e.
    /// the blanket cancel has been acknowledged and no further fills can
    /// land on the old orders. Bounded (~1.5 s worst case) and best-effort:
    /// on exhaustion it logs and returns, leaving the #470 venue-position
    /// cap and the post-fill reconciliation (bot-strategy#721) as the
    /// remaining safety nets. Skipped in backtest replay, where the cancel
    /// is synchronous by construction.
    async fn await_pending_cancellation(&self, pending: &PendingOrders) {
        const CANCEL_ACK_ATTEMPTS: usize = 10;
        const CANCEL_ACK_DELAY_MS: u64 = 150;
        if self.cfg.backtest_mode {
            return;
        }
        let mut by_symbol: HashMap<String, HashSet<String>> = HashMap::new();
        for leg in &pending.legs {
            by_symbol
                .entry(leg.symbol.clone())
                .or_default()
                .insert(leg.order_id.clone());
        }
        for attempt in 0..CANCEL_ACK_ATTEMPTS {
            if attempt > 0 {
                sleep(Duration::from_millis(CANCEL_ACK_DELAY_MS)).await;
            }
            let mut any_open = false;
            for (symbol, order_ids) in &by_symbol {
                match self.connector.get_open_orders(symbol).await {
                    Ok(open) => {
                        if open
                            .orders
                            .iter()
                            .any(|order| order_ids.contains(&order.order_id))
                        {
                            any_open = true;
                        }
                    }
                    // Can't confirm — assume still open and keep polling.
                    Err(_) => any_open = true,
                }
            }
            if !any_open {
                return;
            }
        }
        log::warn!(
            "[ORDER] cancel not confirmed for {} legs after {}ms; proceeding with post-cancel fill refresh",
            pending.legs.len(),
            CANCEL_ACK_ATTEMPTS as u64 * CANCEL_ACK_DELAY_MS,
        );
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
        let mut filled_value_qty: HashMap<String, Decimal> = HashMap::new();
        let mut filled_fees: HashMap<String, Decimal> = HashMap::new();
        let mut filled_ts_ms_max: HashMap<String, i64> = HashMap::new();
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
                        *filled_value_qty.entry(order.order_id.clone()).or_default() += sz;
                    }
                    if let Some(fee) = order.filled_fee {
                        *filled_fees.entry(order.order_id.clone()).or_default() += fee;
                    }
                    if let Some(ts) = order.filled_ts_ms {
                        let entry = filled_ts_ms_max.entry(order.order_id.clone()).or_insert(ts);
                        if ts > *entry {
                            *entry = ts;
                        }
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
            filled_value_qty,
            filled_fees,
            filled_ts_ms_max,
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

    fn lookup_decimal_for_leg(leg: &PendingLeg, map: &HashMap<String, Decimal>) -> Option<Decimal> {
        map.get(&leg.order_id).cloned().or_else(|| {
            leg.exchange_order_id
                .as_ref()
                .and_then(|id| map.get(id).cloned())
        })
    }

    /// Volume-weighted average fill price across every leg for `symbol`,
    /// from the actual venue fills the reconcile loop already collected
    /// (`status.filled_values` / `filled_value_qty`) — not a reconciliation-
    /// time mark snapshot. A partial-fill reissue leaves one settled leg
    /// (the filled slice, original order_id) plus a fresh leg for the
    /// remainder (bot-strategy#502's `sum_entry_sizes_by_symbol`), so this
    /// sums value/qty across every leg matching `symbol`, not just the last
    /// one — the same coverage-matched invariant `ledger_fill_price` uses
    /// per-leg, extended to the per-symbol total a position needs.
    /// `None` when no leg for this symbol has any reported fill value
    /// (bot-strategy#750): callers must treat that as an explicit missing-
    /// coverage case, not silently substitute a snapshot in its place.
    fn fill_vwap_by_symbol(
        legs: &[PendingLeg],
        status: &PendingStatus,
        symbol: &str,
    ) -> Option<Decimal> {
        let mut total_value = Decimal::ZERO;
        let mut total_qty = Decimal::ZERO;
        let mut covered = false;
        for leg in legs.iter().filter(|leg| leg.symbol == symbol) {
            let value = Self::lookup_decimal_for_leg(leg, &status.filled_values);
            let qty = Self::lookup_decimal_for_leg(leg, &status.filled_value_qty);
            if let (Some(value), Some(qty)) = (value, qty) {
                if qty > Decimal::ZERO {
                    total_value += value;
                    total_qty += qty;
                    covered = true;
                }
            }
        }
        (covered && total_qty > Decimal::ZERO).then(|| total_value / total_qty)
    }

    /// `fill_vwap_by_symbol`, falling back to the reconciliation-time mark
    /// snapshot only when no leg reported any fill value at all — the rare
    /// zero-coverage case (bot-strategy#750 acceptance: never fabricate a
    /// VWAP by blending in a snapshot price for a partially-covered fill,
    /// but a total value blackout still needs *some* price rather than
    /// dropping the record entirely). The fallback is logged so it stays
    /// auditable instead of silently masquerading as fill-grade truth.
    fn fill_price_or_snapshot(
        legs: &[PendingLeg],
        status: &PendingStatus,
        price_map: &HashMap<String, SymbolSnapshot>,
        key: &str,
        phase: &str,
        symbol: &str,
    ) -> Option<Decimal> {
        Self::fill_vwap_by_symbol(legs, status, symbol).or_else(|| {
            log::warn!(
                "[PNL] {} {} {} fill-value coverage missing; falling back to mark snapshot",
                key,
                phase,
                symbol
            );
            price_map.get(symbol).map(|s| s.price)
        })
    }

    fn lookup_ts_for_leg(leg: &PendingLeg, map: &HashMap<String, i64>) -> Option<i64> {
        map.get(&leg.order_id).copied().or_else(|| {
            leg.exchange_order_id
                .as_ref()
                .and_then(|id| map.get(id).copied())
        })
    }

    fn execution_order_type(leg: &PendingLeg) -> &'static str {
        if leg.post_only {
            "post_only"
        } else {
            "taker"
        }
    }

    /// Per-leg slippage / fee / fill-latency observation (#314 Group 4-B,
    /// 4-C) plus durable execution-ledger JSONL output (bot-strategy#613).
    /// Uses the same fill/status snapshot for Prometheus and JSONL, with the
    /// in-memory filled fallback matching pending-order completion.
    fn record_leg_execution_quality(
        &mut self,
        variant: &str,
        pair: &str,
        phase: &str,
        close_reason: Option<&str>,
        legs: &[PendingLeg],
        status: &PendingStatus,
        placed_ts_ms: i64,
        attempt: u32,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) {
        let mut leg_records = Vec::new();
        let mut filled_leg_count = 0usize;
        let mut notional_usd = 0.0f64;
        let mut slippage_notional_usd = 0.0f64;
        let mut slippage_usd_total = 0.0f64;
        let mut min_fill_ts: Option<i64> = None;
        let mut max_fill_ts: Option<i64> = None;
        let mut overfill_detected = false;
        let mut underfill_detected = false;

        for leg in legs {
            let (fill_size, capped_fill_size) = Self::ledger_fill_for_leg(leg, &status.fills);
            if fill_size <= Decimal::ZERO {
                continue;
            }
            filled_leg_count += 1;
            let filled_value = Self::lookup_decimal_for_leg(leg, &status.filled_values);
            let filled_fee = Self::lookup_decimal_for_leg(leg, &status.filled_fees);
            let fill_ts_ms = Self::lookup_ts_for_leg(leg, &status.filled_ts_ms_max);
            if let Some(ts) = fill_ts_ms {
                min_fill_ts = Some(min_fill_ts.map_or(ts, |prev| prev.min(ts)));
                max_fill_ts = Some(max_fill_ts.map_or(ts, |prev| prev.max(ts)));
            }
            let positive_filled_value = filled_value.filter(|value| *value > Decimal::ZERO);
            let fill_value_f64 = positive_filled_value.and_then(|v| v.to_f64());
            let fill_price =
                Self::ledger_fill_price(leg, &status.filled_values, &status.filled_value_qty);
            if let Some(value) = fill_value_f64 {
                notional_usd += value.abs();
            }

            let ref_price_opt = leg.reference_price.or(leg.limit_price);
            let slippage_bps_vs_decision = match (ref_price_opt, fill_price) {
                (Some(ref_price), Some(avg_price)) => ref_price.to_f64().and_then(|ref_f64| {
                    if ref_f64 <= 0.0 {
                        return None;
                    }
                    let sign = match leg.side {
                        dex_connector::OrderSide::Long => 1.0,
                        dex_connector::OrderSide::Short => -1.0,
                    };
                    Some(sign * (avg_price - ref_f64) / ref_f64 * 10_000.0)
                }),
                _ => None,
            };
            let submit_ref_price_opt = leg.submit_reference_price.or(ref_price_opt);
            let slippage_bps_vs_submit = match (submit_ref_price_opt, fill_price) {
                (Some(ref_price), Some(avg_price)) => ref_price.to_f64().and_then(|ref_f64| {
                    if ref_f64 <= 0.0 {
                        return None;
                    }
                    let sign = match leg.side {
                        dex_connector::OrderSide::Long => 1.0,
                        dex_connector::OrderSide::Short => -1.0,
                    };
                    Some(sign * (avg_price - ref_f64) / ref_f64 * 10_000.0)
                }),
                _ => None,
            };
            let slippage_usd_vs_decision = match (slippage_bps_vs_decision, fill_value_f64) {
                (Some(bps), Some(value)) => {
                    let usd = value.abs() * bps / 10_000.0;
                    slippage_notional_usd += value.abs();
                    slippage_usd_total += usd;
                    Some(usd)
                }
                _ => None,
            };
            let slippage_usd_vs_submit = match (slippage_bps_vs_submit, fill_value_f64) {
                (Some(bps), Some(value)) => Some(value.abs() * bps / 10_000.0),
                _ => None,
            };

            let order_type = Self::execution_order_type(leg);
            if let Some(slippage_bps) = slippage_bps_vs_decision {
                super::super::prom::LEG_SLIPPAGE_BPS
                    .with_label_values(&[variant, pair, phase, order_type])
                    .observe(slippage_bps);
            }
            let fee_bps = match (filled_fee, fill_value_f64) {
                (Some(fee), Some(value)) if value > 0.0 => {
                    fee.to_f64().map(|fee| fee / value * 10_000.0)
                }
                _ => None,
            };
            if let Some(fee_bps) = fee_bps {
                super::super::prom::LEG_FEE_BPS
                    .with_label_values(&[variant, pair, phase])
                    .observe(fee_bps);
            }
            let submit_ts_for_latency = if leg.submit_ts_ms > 0 {
                leg.submit_ts_ms
            } else {
                placed_ts_ms
            };
            let latency_submit_fill_ms = match (submit_ts_for_latency > 0, fill_ts_ms) {
                (true, Some(fill_ts)) => {
                    let latency_ms = fill_ts - submit_ts_for_latency;
                    if latency_ms >= 0 {
                        super::super::prom::LEG_FILL_LATENCY_MS
                            .with_label_values(&[variant, pair, phase])
                            .observe(latency_ms as f64);
                        Some(latency_ms)
                    } else {
                        None
                    }
                }
                _ => None,
            };

            let leg_overfill = fill_size > leg.target;
            let leg_underfill = capped_fill_size < leg.target;
            overfill_detected |= leg_overfill;
            underfill_detected |= leg_underfill;
            let snap = price_map.get(&leg.symbol);
            leg_records.push(ExecutionLegFillRecord {
                event: "leg_fill",
                ts_ms: execution_ledger::now_ms(),
                variant: variant.to_string(),
                pair: pair.to_string(),
                phase: phase.to_string(),
                close_reason: close_reason.map(str::to_string),
                // Per-leg decision time: a leg carried forward by a reissue
                // (kept/settled) keeps its original decision time, so its row
                // doesn't report a decision after its own submit/fill (Codex
                // review PR #159). Unstamped legs fall back to the group time.
                ts_decision_ms: if leg.decision_ts_ms > 0 {
                    leg.decision_ts_ms
                } else {
                    placed_ts_ms
                },
                ts_submit_ms: leg.submit_ts_ms,
                ts_ack_ms: leg.ack_ts_ms,
                leg_symbol: leg.symbol.clone(),
                side: format!("{:?}", leg.side),
                target_qty: leg.target,
                submitted_qty: leg.submitted_qty,
                filled_qty: fill_size,
                remaining_qty: (leg.target - capped_fill_size).max(Decimal::ZERO),
                order_id: leg.order_id.clone(),
                exchange_order_id: leg.exchange_order_id.clone(),
                client_order_id: leg.client_order_id.clone(),
                post_only: leg.post_only,
                reduce_only: leg.reduce_only,
                order_type: order_type.to_string(),
                attempt,
                placed_ts_ms,
                fill_ts_ms,
                latency_submit_fill_ms,
                reference_price: ref_price_opt,
                submit_reference_price: submit_ref_price_opt,
                submit_mid: leg.submit_mid,
                limit_price: leg.limit_price,
                submit_bid: leg.submit_bid,
                submit_ask: leg.submit_ask,
                best_bid: snap.and_then(|s| s.bid_price),
                best_ask: snap.and_then(|s| s.ask_price),
                fill_value: filled_value,
                fill_price,
                filled_fee,
                fee_bps,
                slippage_bps_vs_decision,
                slippage_usd_vs_decision,
                slippage_bps_vs_submit,
                slippage_usd_vs_submit,
                overfill_detected: leg_overfill,
                underfill_detected: leg_underfill,
            });
        }

        let Some(ledger) = self.execution_ledger.as_mut() else {
            return;
        };
        for record in &leg_records {
            ledger.write_leg_fill(record);
        }
        if filled_leg_count == 0 {
            return;
        }
        let gross_execution_slippage_bps = if slippage_notional_usd > 0.0 {
            Some(slippage_usd_total / slippage_notional_usd * 10_000.0)
        } else {
            None
        };
        let leg_sync_gap_ms = match (min_fill_ts, max_fill_ts) {
            (Some(min_ts), Some(max_ts)) => Some(max_ts - min_ts),
            _ => None,
        };
        ledger.write_pair_summary(&ExecutionPairSummaryRecord {
            event: "pair_fill_summary",
            ts_ms: execution_ledger::now_ms(),
            trade_id: format!("{}:{}:{}:{}", variant, pair, phase, placed_ts_ms),
            variant: variant.to_string(),
            pair: pair.to_string(),
            phase: phase.to_string(),
            close_reason: close_reason.map(str::to_string),
            leg_count: legs.len(),
            filled_leg_count,
            notional_usd,
            gross_execution_slippage_bps,
            gross_execution_slippage_usd: if slippage_notional_usd > 0.0 {
                Some(slippage_usd_total)
            } else {
                None
            },
            leg_sync_gap_ms,
            overfill_detected,
            underfill_detected,
        });
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

    fn ledger_fill_for_leg(
        leg: &PendingLeg,
        fills: &HashMap<String, Decimal>,
    ) -> (Decimal, Decimal) {
        let filled = Self::leg_fill_from_map(leg, fills).max(leg.filled);
        (filled, filled.min(leg.target))
    }

    /// Volume-weighted average fill price over the (value, qty) slice the
    /// fill map actually covers for this leg — both sides of the division
    /// come from the same set of reported fills. The denominator must NOT
    /// be the leg's total fill (`ledger_fill_for_leg`): that total can
    /// include qty recovered by the bot-strategy#470 exchange-position
    /// cross-check whose USD value never landed in `filled_values`, and
    /// dividing a partial value sum by the full size books the missing
    /// fraction as ~1000 bps of phantom slippage (bot-strategy#705).
    fn ledger_fill_price(
        leg: &PendingLeg,
        filled_values: &HashMap<String, Decimal>,
        filled_value_qty: &HashMap<String, Decimal>,
    ) -> Option<f64> {
        let value = Self::lookup_decimal_for_leg(leg, filled_values)
            .filter(|value| *value > Decimal::ZERO)?;
        let qty = Self::lookup_decimal_for_leg(leg, filled_value_qty)
            .filter(|qty| *qty > Decimal::ZERO)?;
        match (value.to_f64(), qty.to_f64()) {
            (Some(value), Some(qty)) if qty > 0.0 => Some(value / qty),
            _ => None,
        }
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
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use dex_connector::{
        BalanceResponse, CanceledOrdersResponse, CombinedBalanceResponse, CreateOrderResponse,
        DexConnector, DexError, FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse,
        OrderBookSnapshot, OrderSide, PositionSnapshot, PriceUpdate, TickerResponse, TpSl,
        TriggerOrderStyle,
    };
    use rust_decimal::prelude::ToPrimitive;
    use rust_decimal::Decimal;

    use super::super::super::exit::compute_pnl;
    use super::super::super::funding_history::FundingHistory;
    use super::super::super::market::SymbolSnapshot;
    use super::super::super::state::{
        PairState, PendingLeg, PendingOrders, PendingStatus, Position, PositionDirection,
    };
    use super::{ExitFillPnlContext, PairTradeEngine};

    /// Minimal `DexConnector`: every method is `unimplemented!()` except
    /// `get_open_orders` (always empty — no open remainder) and
    /// `get_filled_orders`, which replays whatever `script_fill` recorded
    /// for that symbol. Unlike `pending_tests.rs`'s `DummyConnector`, this
    /// scripts `filled_value` (not just `filled_size`), since exercising
    /// the #750 fill-VWAP-vs-mark-snapshot path requires fill *value*
    /// coverage.
    #[derive(Default)]
    struct NullConnector {
        // symbol -> (order_id, filled_size, filled_value)
        scripted_fills: Mutex<HashMap<String, (String, Decimal, Decimal)>>,
    }

    impl NullConnector {
        fn script_fill(&self, symbol: &str, order_id: &str, size: Decimal, value: Decimal) {
            self.scripted_fills
                .lock()
                .unwrap()
                .insert(symbol.to_string(), (order_id.to_string(), size, value));
        }
    }

    #[async_trait]
    impl DexConnector for NullConnector {
        async fn start(&self) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn stop(&self) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn get_ticker(
            &self,
            _symbol: &str,
            _test_price: Option<Decimal>,
        ) -> Result<TickerResponse, DexError> {
            unimplemented!()
        }
        async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
            let scripted = self.scripted_fills.lock().unwrap();
            Ok(match scripted.get(symbol) {
                Some((order_id, size, value)) => FilledOrdersResponse {
                    orders: vec![dex_connector::FilledOrder {
                        order_id: order_id.clone(),
                        is_rejected: false,
                        trade_id: "trade".to_string(),
                        filled_side: None,
                        filled_size: Some(*size),
                        filled_value: Some(*value),
                        filled_fee: None,
                        filled_ts_ms: None,
                    }],
                },
                None => FilledOrdersResponse::default(),
            })
        }
        async fn get_canceled_orders(
            &self,
            _symbol: &str,
        ) -> Result<CanceledOrdersResponse, DexError> {
            unimplemented!()
        }
        async fn get_open_orders(&self, _symbol: &str) -> Result<OpenOrdersResponse, DexError> {
            Ok(OpenOrdersResponse::default())
        }
        async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
            unimplemented!()
        }
        async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
            unimplemented!()
        }
        async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
            unimplemented!()
        }
        async fn get_last_trades(&self, _symbol: &str) -> Result<LastTradesResponse, DexError> {
            unimplemented!()
        }
        async fn get_order_book(
            &self,
            _symbol: &str,
            _depth: usize,
        ) -> Result<OrderBookSnapshot, DexError> {
            unimplemented!()
        }
        async fn clear_filled_order(&self, _symbol: &str, _trade_id: &str) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn clear_canceled_order(
            &self,
            _symbol: &str,
            _order_id: &str,
        ) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn create_order(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _price: Option<Decimal>,
            _spread: Option<i64>,
            _reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            unimplemented!()
        }
        async fn create_advanced_trigger_order(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _trigger_px: Decimal,
            _limit_px: Option<Decimal>,
            _order_style: TriggerOrderStyle,
            _slippage_bps: Option<u32>,
            _tpsl: TpSl,
            _reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            unimplemented!()
        }
        async fn create_order_taker_ioc(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _slippage_bps: u32,
            _reduce_only: bool,
        ) -> Result<CreateOrderResponse, DexError> {
            unimplemented!()
        }
        async fn modify_order(
            &self,
            _symbol: &str,
            _order_id: &str,
            _side: OrderSide,
            _target_total_size: Decimal,
            _open_remaining_size: Decimal,
            _price: Option<Decimal>,
            _spread: Option<i64>,
            _reduce_only: bool,
        ) -> Result<CreateOrderResponse, DexError> {
            unimplemented!()
        }
        async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn cancel_orders(
            &self,
            _symbol: Option<String>,
            _order_ids: Vec<String>,
        ) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
            unimplemented!()
        }
        async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
            unimplemented!()
        }
        async fn sign_evm_65b(&self, _message: &str) -> Result<String, DexError> {
            unimplemented!()
        }
        async fn sign_evm_65b_with_eip191(&self, _message: &str) -> Result<String, DexError> {
            unimplemented!()
        }
        fn subscribe_price_updates(
            &self,
        ) -> Result<tokio::sync::broadcast::Receiver<PriceUpdate>, DexError> {
            unimplemented!()
        }
    }

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
            submitted_qty: Decimal::ZERO,
            limit_price: None,
            reference_price: None,
            submit_ts_ms: 0,
            ack_ts_ms: None,
            decision_ts_ms: 0,
            submit_reference_price: None,
            submit_mid: None,
            submit_bid: None,
            submit_ask: None,
            client_order_id: None,
            reduce_only: false,
            post_only: false,
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
            submitted_qty: Decimal::ZERO,
            limit_price: None,
            reference_price: None,
            submit_ts_ms: 0,
            ack_ts_ms: None,
            decision_ts_ms: 0,
            submit_reference_price: None,
            submit_mid: None,
            submit_bid: None,
            submit_ask: None,
            client_order_id: None,
            reduce_only: false,
            post_only: false,
        }
    }

    fn pending(legs: Vec<PendingLeg>) -> PendingOrders {
        PendingOrders {
            legs,
            direction: PositionDirection::LongSpread,
            placed_at: Instant::now(),
            placed_ts_ms: 0,
            hedge_retry_count: 0,
            post_only_hybrid: false,
            exit_taker_takeover_at: None,
        }
    }

    fn pending_at(legs: Vec<PendingLeg>, placed_ts_ms: i64) -> PendingOrders {
        let mut p = pending(legs);
        p.placed_ts_ms = placed_ts_ms;
        p
    }

    /// Codex review PR #159: a leg carried forward by a reissue (cloned via
    /// `kept_leg`/`settled_leg`, which preserves `decision_ts_ms`) must keep
    /// its original decision time, while a freshly placed leg inherits the new
    /// group's `placed_ts_ms`. Otherwise the carried filled leg's ledger
    /// `ts_decision_ms` would jump to the reissue time — after its own
    /// submit/fill.
    #[test]
    fn with_leg_decision_ts_stamps_fresh_legs_and_preserves_carried() {
        // First placement at T=1000: both legs are fresh (decision_ts_ms == 0)
        // and must inherit the group time.
        let original = pending_at(
            vec![leg("AAA", "a", "1", "1"), leg("BBB", "b", "1", "0")],
            1000,
        )
        .with_leg_decision_ts();
        assert_eq!(original.legs[0].decision_ts_ms, 1000);
        assert_eq!(original.legs[1].decision_ts_ms, 1000);

        // Reissue at T=2000: carry leg AAA forward (clone preserves its 1000),
        // add a freshly placed remainder leg (decision_ts_ms == 0).
        let carried = original.legs[0].clone();
        assert_eq!(carried.decision_ts_ms, 1000);
        let reissued =
            pending_at(vec![carried, leg("AAA", "a2", "1", "0")], 2000).with_leg_decision_ts();

        // Carried leg keeps its original decision time; the fresh leg takes
        // the reissue time.
        assert_eq!(reissued.legs[0].decision_ts_ms, 1000);
        assert_eq!(reissued.legs[1].decision_ts_ms, 2000);
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
        assert_eq!(PairTradeEngine::leg_fill_from_map(&l, &fills), dec("0.3"));
    }

    #[test]
    fn leg_fill_falls_back_to_exchange_order_id_when_internal_missing() {
        // Extended often surfaces fills under its own exchange-side id;
        // the reconcile loop must still aggregate them onto the right leg.
        let l = leg_with_exchange("BTC", "ord-1", "exch-9", "1.0");
        let mut fills = HashMap::new();
        fills.insert("exch-9".to_string(), dec("0.5"));
        assert_eq!(PairTradeEngine::leg_fill_from_map(&l, &fills), dec("0.5"));
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
    fn ledger_fill_for_leg_keeps_raw_overreport_and_capped_completion() {
        let l = leg("BTC", "ord-1", "1.0", "0.0");
        let mut fills = HashMap::new();
        fills.insert("ord-1".to_string(), dec("1.5"));
        assert_eq!(
            PairTradeEngine::ledger_fill_for_leg(&l, &fills),
            (dec("1.5"), dec("1.0"))
        );
    }

    #[test]
    fn ledger_fill_for_leg_falls_back_to_in_memory_when_map_silent() {
        let l = leg("BTC", "ord-1", "1.0", "1.0");
        let fills: HashMap<String, Decimal> = HashMap::new();
        assert_eq!(
            PairTradeEngine::ledger_fill_for_leg(&l, &fills),
            (dec("1.0"), dec("1.0"))
        );
    }

    #[test]
    fn ledger_fill_price_divides_by_value_covered_qty_not_total_fill() {
        // bot-strategy#705 regression: leg.filled carries 1.0 (0.1 of it
        // recovered via the #470 exchange-position cross-check, so its
        // value never reached the map), while the map covers only the 0.9
        // that reported a value. Dividing the 0.9-slice value by the full
        // 1.0 booked a ~1000 bps phantom price drop; the price must come
        // out of the covered slice alone.
        let l = leg("BTC", "ord-1", "1.0", "1.0");
        let mut values = HashMap::new();
        values.insert("ord-1".to_string(), dec("56700.0")); // 0.9 × 63000
        let mut value_qty = HashMap::new();
        value_qty.insert("ord-1".to_string(), dec("0.9"));
        assert_eq!(
            PairTradeEngine::ledger_fill_price(&l, &values, &value_qty),
            Some(63000.0)
        );
    }

    #[test]
    fn ledger_fill_price_full_coverage_is_value_over_size() {
        let l = leg("BTC", "ord-1", "1.0", "1.0");
        let mut values = HashMap::new();
        values.insert("ord-1".to_string(), dec("63000.0"));
        let mut value_qty = HashMap::new();
        value_qty.insert("ord-1".to_string(), dec("1.0"));
        assert_eq!(
            PairTradeEngine::ledger_fill_price(&l, &values, &value_qty),
            Some(63000.0)
        );
    }

    #[test]
    fn ledger_fill_price_none_when_no_value_reported() {
        // In-memory fill state alone carries no USD value; better no
        // fill_price than a fabricated one.
        let l = leg("BTC", "ord-1", "1.0", "1.0");
        let values: HashMap<String, Decimal> = HashMap::new();
        let value_qty: HashMap<String, Decimal> = HashMap::new();
        assert_eq!(
            PairTradeEngine::ledger_fill_price(&l, &values, &value_qty),
            None
        );
    }

    #[test]
    fn ledger_fill_price_resolves_via_exchange_order_id() {
        let l = leg_with_exchange("BTC", "ord-1", "ex-9", "1.0");
        let mut values = HashMap::new();
        values.insert("ex-9".to_string(), dec("31500.0"));
        let mut value_qty = HashMap::new();
        value_qty.insert("ex-9".to_string(), dec("0.5"));
        assert_eq!(
            PairTradeEngine::ledger_fill_price(&l, &values, &value_qty),
            Some(63000.0)
        );
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

    #[test]
    fn partial_fill_reissue_limit_then_market_then_giveup() {
        // max=2, giveup=4. retry index = count + 1.
        let d = |count| PairTradeEngine::decide_partial_fill_reissue(count, 2, 4);
        // counts 0,1 -> next 1,2 <= max: limit reissue, no give-up
        assert_eq!(
            d(0),
            super::PartialFillDecision {
                next_retry: 1,
                give_up: false,
                use_market: false
            }
        );
        assert_eq!(
            d(1),
            super::PartialFillDecision {
                next_retry: 2,
                give_up: false,
                use_market: false
            }
        );
        // count 2 -> next 3 > max(2): escalate to MARKET, still < giveup
        assert_eq!(
            d(2),
            super::PartialFillDecision {
                next_retry: 3,
                give_up: false,
                use_market: true
            }
        );
        // count 3 -> next 4 == giveup(4): not yet (strictly >)
        assert_eq!(
            d(3),
            super::PartialFillDecision {
                next_retry: 4,
                give_up: false,
                use_market: true
            }
        );
        // count 4 -> next 5 > giveup(4): give up (precedence over market)
        assert_eq!(
            d(4),
            super::PartialFillDecision {
                next_retry: 5,
                give_up: true,
                use_market: true
            }
        );
    }

    #[test]
    fn partial_fill_reissue_caps_zero_disables() {
        // Both caps 0 = legacy unbounded: never give up, never escalate.
        let d = PairTradeEngine::decide_partial_fill_reissue(10_000, 0, 0);
        assert_eq!(d.next_retry, 10_001);
        assert!(!d.give_up);
        assert!(!d.use_market);
    }

    #[test]
    fn partial_fill_reissue_next_retry_saturates() {
        let d = PairTradeEngine::decide_partial_fill_reissue(u32::MAX, 0, 0);
        assert_eq!(d.next_retry, u32::MAX);
    }

    fn empty_status() -> PendingStatus {
        PendingStatus {
            open_remaining: 0,
            fills: HashMap::new(),
            filled_values: HashMap::new(),
            filled_value_qty: HashMap::new(),
            filled_fees: HashMap::new(),
            filled_ts_ms_max: HashMap::new(),
            open_ids: HashSet::new(),
        }
    }

    /// Builds a `PendingStatus` reporting `value`/`qty` fill coverage for
    /// `order_id`, as `pending_status()` would after querying the venue.
    fn status_with_fill(order_id: &str, value: &str, qty: &str) -> PendingStatus {
        let mut status = empty_status();
        status
            .filled_values
            .insert(order_id.to_string(), dec(value));
        status
            .filled_value_qty
            .insert(order_id.to_string(), dec(qty));
        status
    }

    #[test]
    fn entry_prices_and_sizes_non_pair_key_is_all_none() {
        let legs = vec![leg("BTC", "ord-1", "0.5", "0.5")];
        let prices: HashMap<String, SymbolSnapshot> = HashMap::new();
        let status = empty_status();
        let got = PairTradeEngine::entry_prices_and_sizes("NOTAPAIR", &prices, &legs, &status);
        assert_eq!(got, (None, None, None, None));
    }

    #[test]
    fn entry_prices_and_sizes_sums_legs_when_prices_absent() {
        // Empty price_map and no fill-value coverage: prices stay None, but
        // the split + per-symbol size summing still runs (BTC long leg, ETH
        // short leg).
        let legs = vec![
            leg("BTC", "ord-1", "0.5", "0.5"),
            leg("ETH", "ord-2", "2.0", "2.0"),
        ];
        let prices: HashMap<String, SymbolSnapshot> = HashMap::new();
        let status = empty_status();
        let (pa, pb, sa, sb) =
            PairTradeEngine::entry_prices_and_sizes("BTC/ETH", &prices, &legs, &status);
        assert_eq!((pa, pb), (None, None));
        // sum_entry_sizes_by_symbol aggregates filled size per side.
        assert_eq!(sa, Some(dec("0.5")));
        assert_eq!(sb, Some(dec("2.0")));
    }

    // bot-strategy#750: entry/exit PnL must be driven by actual fill VWAP,
    // not a reconciliation-time mark snapshot.

    #[test]
    fn entry_prices_prefer_fill_vwap_over_snapshot() {
        // Fill VWAP (100.10) differs materially from the mark snapshot
        // (105.00) sampled at reconcile time — must use the fill, not the
        // snapshot, even though a snapshot is available.
        let legs = vec![leg("BTC", "ord-1", "1.0", "1.0")];
        let status = status_with_fill("ord-1", "100.10", "1.0");
        let mut prices: HashMap<String, SymbolSnapshot> = HashMap::new();
        prices.insert(
            "BTC".to_string(),
            SymbolSnapshot {
                price: dec("105.00"),
                funding_rate: dec("0"),
                bid_price: None,
                ask_price: None,
                bid_size: dec("0"),
                ask_size: dec("0"),
                min_order: None,
                min_tick: None,
                size_decimals: None,
                exchange_ts: None,
            },
        );
        let (pa, _, _, _) =
            PairTradeEngine::entry_prices_and_sizes("BTC/ETH", &prices, &legs, &status);
        assert_eq!(pa, Some(dec("100.10")));
    }

    #[test]
    fn entry_prices_blend_vwap_across_settled_and_reissued_legs() {
        // Partial-fill reissue: 0.4 filled at 100 on the original order,
        // remaining 0.6 filled at 102 on the reissued order (settled leg +
        // fresh leg for the same symbol, per sum_entry_sizes_by_symbol's
        // documented shape). Blended VWAP = (0.4*100 + 0.6*102) / 1.0 =
        // 101.2, not the mark snapshot and not either fill in isolation.
        let legs = vec![
            leg("BTC", "ord-1", "0.4", "0.4"),
            leg("BTC", "ord-2", "0.6", "0.6"),
        ];
        let mut status = empty_status();
        status
            .filled_values
            .insert("ord-1".to_string(), dec("40.00"));
        status
            .filled_value_qty
            .insert("ord-1".to_string(), dec("0.4"));
        status
            .filled_values
            .insert("ord-2".to_string(), dec("61.20"));
        status
            .filled_value_qty
            .insert("ord-2".to_string(), dec("0.6"));
        let prices: HashMap<String, SymbolSnapshot> = HashMap::new();
        let (pa, _, sa, _) =
            PairTradeEngine::entry_prices_and_sizes("BTC/ETH", &prices, &legs, &status);
        assert_eq!(pa, Some(dec("101.2")));
        assert_eq!(sa, Some(dec("1.0")));
    }

    #[test]
    fn entry_prices_fall_back_to_snapshot_on_zero_coverage() {
        // No leg reports any fill value at all (total coverage blackout) —
        // the only case allowed to fall back to the mark snapshot.
        let legs = vec![leg("BTC", "ord-1", "1.0", "1.0")];
        let status = empty_status();
        let mut prices: HashMap<String, SymbolSnapshot> = HashMap::new();
        prices.insert(
            "BTC".to_string(),
            SymbolSnapshot {
                price: dec("105.00"),
                funding_rate: dec("0"),
                bid_price: None,
                ask_price: None,
                bid_size: dec("0"),
                ask_size: dec("0"),
                min_order: None,
                min_tick: None,
                size_decimals: None,
                exchange_ts: None,
            },
        );
        let (pa, _, _, _) =
            PairTradeEngine::entry_prices_and_sizes("BTC/ETH", &prices, &legs, &status);
        assert_eq!(pa, Some(dec("105.00")));
    }

    #[test]
    fn fill_vwap_by_symbol_ignores_legs_with_qty_but_no_value() {
        // A leg whose fill map has qty but zero/no reported value must not
        // be divided in — that is exactly the #705 phantom-slippage shape
        // (qty recovered by the exchange-position cross-check with no
        // matching value). It must be excluded from the VWAP, not treated
        // as a zero-price fill.
        let legs = vec![
            leg("BTC", "ord-1", "0.5", "0.5"),
            leg("BTC", "ord-2", "0.5", "0.5"),
        ];
        let mut status = empty_status();
        status
            .filled_values
            .insert("ord-1".to_string(), dec("50.00"));
        status
            .filled_value_qty
            .insert("ord-1".to_string(), dec("0.5"));
        // ord-2 has a qty-only fill entry (no value/value_qty reported).
        status.fills.insert("ord-2".to_string(), dec("0.5"));
        let got = PairTradeEngine::fill_vwap_by_symbol(&legs, &status, "BTC");
        assert_eq!(got, Some(dec("100.00")));
    }

    #[test]
    fn build_exit_fill_pnl_follows_fill_vwap_even_when_snapshot_sign_disagrees() {
        // bot-strategy#750 acceptance ("sign-crossing regression"): the
        // consecutive-loss circuit breaker in `record_exit_realized_pnl`
        // is a direct, deterministic function of the pnl sign this helper
        // returns, so proving the sign here is fill-derived — even in a
        // scenario engineered so the stale mark snapshot would have called
        // the same close a WIN — is equivalent to proving the breaker
        // follows actual fill economics rather than a reconciliation-time
        // mark.
        //
        // Entry: long AAA @ 100 / short BBB @ 50, size 1.0 each side.
        // Actual exit fills: AAA @ 95 (down), BBB @ 50 (flat) -> fill PnL
        // = (95-100)*1 + (50-50)*1 = -5 (LOSS).
        // Mark snapshot sampled at reconcile time: AAA @ 110 -> would have
        // scored (110-100)*1 + (50-50)*1 = +10 (WIN) had it been used
        // instead of the fill VWAP. The two signs disagree.
        let position = Position {
            direction: PositionDirection::LongSpread,
            entered_at: Instant::now(),
            entered_ts: 1_700_000_000,
            entry_price_a: Some(dec("100")),
            entry_price_b: Some(dec("50")),
            entry_size_a: Some(dec("1.0")),
            entry_size_b: Some(dec("1.0")),
            entry_z: Some(2.4),
            entry_beta: Some(1.0),
            last_rehedge_ts: None,
            rehedge_realized_pnl: None,
            prev_beta_for_velocity: None,
        };
        let mut state = PairState::new(2.0);
        state.position = Some(position);

        let legs = vec![
            leg("AAA", "exit-a", "1.0", "1.0"),
            leg("BBB", "exit-b", "1.0", "1.0"),
        ];
        let mut status = empty_status();
        status
            .filled_values
            .insert("exit-a".to_string(), dec("95.0"));
        status
            .filled_value_qty
            .insert("exit-a".to_string(), dec("1.0"));
        status
            .filled_values
            .insert("exit-b".to_string(), dec("50.0"));
        status
            .filled_value_qty
            .insert("exit-b".to_string(), dec("1.0"));

        let mut price_map: HashMap<String, SymbolSnapshot> = HashMap::new();
        price_map.insert(
            "AAA".to_string(),
            SymbolSnapshot {
                price: dec("110.0"),
                funding_rate: dec("0"),
                bid_price: None,
                ask_price: None,
                bid_size: dec("0"),
                ask_size: dec("0"),
                min_order: None,
                min_tick: None,
                size_decimals: None,
                exchange_ts: None,
            },
        );
        price_map.insert(
            "BBB".to_string(),
            SymbolSnapshot {
                price: dec("50.0"),
                funding_rate: dec("0"),
                bid_price: None,
                ask_price: None,
                bid_size: dec("0"),
                ask_size: dec("0"),
                min_order: None,
                min_tick: None,
                size_decimals: None,
                exchange_ts: None,
            },
        );

        let funding = FundingHistory::new();
        let now_ts = 1_700_000_300;
        let ctx = ExitFillPnlContext {
            inst_id: "default",
            key: "AAA/BBB",
            state: &state,
            price_map: &price_map,
            legs: &legs,
            status: &status,
            funding_history: &funding,
            z_exit: Some(0.1),
            beta_val: Some(1.0),
            now_ts,
        };
        let (_record, pnl, _funding_usd) =
            PairTradeEngine::build_exit_fill_pnl(ctx).expect("pnl available");

        // Sanity: the mark snapshot alone (never actually used once fill
        // coverage exists) would have scored the opposite sign — this is
        // what makes it a sign-crossing case rather than a same-direction
        // magnitude difference.
        let snapshot_pnl = compute_pnl(state.position.as_ref().unwrap(), dec("110.0"), dec("50.0"))
            .and_then(|p| p.to_f64())
            .unwrap();
        assert!(
            snapshot_pnl > 0.0,
            "test setup sanity: snapshot price must imply a win, got {snapshot_pnl}"
        );

        // The value that actually reaches `record_exit_realized_pnl` (and
        // therefore drives `consecutive_losses`) must follow the fill, not
        // the snapshot: a loss, not a win.
        assert_eq!(
            pnl, -5.0,
            "exit pnl must come from fill VWAP, not the mark snapshot"
        );
    }

    /// Codex review on PR #180: a test that only calls `build_exit_fill_pnl`
    /// and `record_exit_realized_pnl` directly stays green even if the real
    /// call site in `reconcile_exit` (below) stops forwarding the fill
    /// derived value, swaps the `(record, pnl, funding)` tuple order, or
    /// substitutes something else. Drive a real exit through
    /// `reconcile_exit` — the actual production handoff — with a connector
    /// that reports fill *value* coverage (not just size) for both legs, so
    /// the whole #750 pipeline (fill VWAP -> pnl -> circuit breaker) runs
    /// end to end.
    #[tokio::test]
    async fn reconcile_exit_drives_circuit_breaker_from_fill_vwap_not_snapshot() {
        // Same sign-crossing setup as the pure-fn test above: fill VWAP
        // implies a loss (-5), the mark snapshot would have implied a win
        // (+10) had it been used instead.
        let connector = Arc::new(NullConnector::default());
        connector.script_fill("AAA", "exit-a", dec("1.0"), dec("95.0"));
        connector.script_fill("BBB", "exit-b", dec("1.0"), dec("50.0"));

        let mut engine = PairTradeEngine::test_instance(connector);
        let risk_state_dir = tempfile::TempDir::new().unwrap();
        engine.risk_state_path = risk_state_dir.path().join("risk_state.json");

        let position = Position {
            direction: PositionDirection::LongSpread,
            entered_at: Instant::now(),
            entered_ts: 1_700_000_000,
            entry_price_a: Some(dec("100")),
            entry_price_b: Some(dec("50")),
            entry_size_a: Some(dec("1.0")),
            entry_size_b: Some(dec("1.0")),
            entry_z: Some(2.4),
            entry_beta: Some(1.0),
            last_rehedge_ts: None,
            rehedge_realized_pnl: None,
            prev_beta_for_velocity: None,
        };
        let mut state = PairState::new(2.0);
        state.position = Some(position);
        engine.instances[0]
            .states
            .insert("AAA/BBB".to_string(), state);
        assert_eq!(engine.instances[0].consecutive_losses, 0);

        let pending = PendingOrders {
            legs: vec![
                leg("AAA", "exit-a", "1.0", "0.0"),
                leg("BBB", "exit-b", "1.0", "0.0"),
            ],
            direction: PositionDirection::LongSpread,
            placed_at: Instant::now(),
            placed_ts_ms: 0,
            hedge_retry_count: 0,
            post_only_hybrid: false,
            exit_taker_takeover_at: None,
        };
        // Mark snapshot (never consulted once fill-value coverage exists)
        // would have scored the opposite sign — see the pure-fn test above
        // for the arithmetic.
        let mut price_map: HashMap<String, SymbolSnapshot> = HashMap::new();
        price_map.insert(
            "AAA".to_string(),
            SymbolSnapshot {
                price: dec("110.0"),
                funding_rate: dec("0"),
                bid_price: None,
                ask_price: None,
                bid_size: dec("0"),
                ask_size: dec("0"),
                min_order: None,
                min_tick: None,
                size_decimals: None,
                exchange_ts: None,
            },
        );
        price_map.insert(
            "BBB".to_string(),
            SymbolSnapshot {
                price: dec("50.0"),
                funding_rate: dec("0"),
                bid_price: None,
                ask_price: None,
                bid_size: dec("0"),
                ask_size: dec("0"),
                min_order: None,
                min_tick: None,
                size_decimals: None,
                exchange_ts: None,
            },
        );

        engine
            .reconcile_exit(
                0,
                "AAA/BBB",
                &price_map,
                pending,
                1_700_000_300,
                Duration::from_secs(30),
            )
            .await
            .unwrap();

        assert_eq!(
            engine.instances[0].consecutive_losses, 1,
            "the real reconcile_exit handoff must count this as a loss, matching the \
             fill VWAP (-5), not reset it as the mark snapshot (+10) would have"
        );
    }
}
