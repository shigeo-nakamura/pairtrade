//! Order placement / closure for `PairTradeEngine`.
//!
//! Holds the pair-leg placement state machine plus the post-only retry
//! / fallback path:
//!
//! - `place_pair_orders` / `close_pair_orders` — entry and exit
//!   submission with hedge-ratio quantize search.
//! - `create_order_with_post_only_retry` — bounded post-only retry
//!   loop that escalates to taker on exhaustion.
//! - `recover_from_leg_b_failure` — leg-A unwind / hedge when leg-B
//!   placement fails after leg-A succeeded.
//! - `reissue_partial_legs` / `reissue_entry_as_taker` — partial-fill
//!   recovery used by the reconcile loop (`engine::reconcile`).
//! - `register_partial_leg_failure` — surface a leg-A success / leg-B
//!   failure as a `pending_*` so the next reconcile tick takes over.
//!
//! Pure relocation from the god-module split (#291); no semantic change.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use dex_connector::DexError;
use rust_decimal::Decimal;
use tokio::time::sleep;

use super::super::config::PairSpec;
use super::super::defaults::{
    POST_ONLY_ENTRY_ATTEMPTS, POST_ONLY_EXIT_ATTEMPTS, POST_ONLY_RETRY_DELAY_MS,
    POST_ONLY_RETRY_MAX_ELAPSED_MS,
};
use super::super::engine;
use super::super::market::SymbolSnapshot;
use super::super::order_pricing;
use super::super::state::{
    PartialOrderPlacementError, PendingLeg, PendingOrders, PositionDirection,
};
use super::super::PairTradeEngine;

impl PairTradeEngine {

    pub(in crate::pairtrade) async fn reissue_partial_legs(
        &mut self,
        pending: &PendingOrders,
        filled_qtys: &HashMap<String, Decimal>,
        price_map: &HashMap<String, SymbolSnapshot>,
        reduce_only: bool,
        use_market: bool,
        retry_count: u32,
    ) -> Result<Option<PendingOrders>> {
        let mut new_legs = Vec::new();
        let stage = if reduce_only { "exit" } else { "entry" };
        for leg in &pending.legs {
            let filled = filled_qtys
                .get(&leg.order_id)
                .cloned()
                .unwrap_or(Decimal::ZERO)
                .max(leg.filled)
                .min(leg.target);
            let remaining = (leg.target - filled).max(Decimal::ZERO);
            if remaining <= Decimal::ZERO {
                let mut kept = leg.clone();
                kept.filled = filled;
                new_legs.push(kept);
                continue;
            }
            if !use_market {
                let has_price = price_map
                    .get(&leg.symbol)
                    .map(|s| s.price > Decimal::ZERO)
                    .unwrap_or(false);
                if !has_price {
                    log::warn!(
                        "[ORDER] Cannot reissue {} leg {}: missing price",
                        stage,
                        leg.symbol
                    );
                    let mut kept = leg.clone();
                    kept.filled = filled;
                    new_legs.push(kept);
                    continue;
                }
            }
            let quantized_size = if reduce_only {
                self.quantize_order_size_close(&leg.symbol, remaining, price_map)
            } else {
                self.quantize_order_size(&leg.symbol, remaining, price_map)
            };
            if quantized_size <= Decimal::ZERO {
                log::warn!(
                    "[ORDER] {} leg {} remaining {} below tick; skipping",
                    stage,
                    leg.symbol,
                    remaining
                );
                let mut kept = leg.clone();
                kept.filled = filled;
                new_legs.push(kept);
                continue;
            }
            let limit = if use_market {
                None
            } else {
                self.limit_price_for(&leg.symbol, leg.side, price_map)
            };
            // Decision-time reference price for the reissue. Captured
            // for both market and limit reissues so the slippage
            // histogram (#314 Group 4-B-2) can tag the order_type
            // correctly and produce taker-side measurements.
            let ref_price_reissue = self.order_reference_price(&leg.symbol, leg.side, price_map);
            if !use_market && limit.is_none() {
                log::warn!(
                    "[ORDER] Cannot reissue {} leg {}: missing reference price",
                    stage,
                    leg.symbol
                );
                let mut kept = leg.clone();
                kept.filled = filled;
                new_legs.push(kept);
                continue;
            }
            let spread = self.order_spread_param(limit, false);
            // Pre-flight position check on the exit reissue path: if a prior
            // reissue tick already filled the leg flat, sending another
            // reduce-only HTTP just races and gets rejected with Extended
            // code 1137. The post-error handler below catches that, but
            // skipping the round-trip here is cleaner and avoids the auto-
            // error workflow trip on the upstream HTTP WARN.
            if reduce_only && self.confirm_reduce_only_position_missing(&leg.symbol).await {
                log::info!(
                    "[ORDER] {} leg {} already closed; skipping reissue",
                    stage,
                    leg.symbol
                );
                let mut kept = leg.clone();
                kept.filled = leg.target;
                new_legs.push(kept);
                continue;
            }
            match self
                .connector
                .create_order(
                    &leg.symbol,
                    quantized_size,
                    leg.side,
                    limit,
                    spread,
                    reduce_only,
                    None,
                )
                .await
            {
                Ok(resp) => {
                    log::info!(
                        "[ORDER] Reissued {} leg {} size={}",
                        stage,
                        leg.symbol,
                        quantized_size
                    );
                    if filled > Decimal::ZERO {
                        // Preserved record of the already-filled portion
                        // of the original leg. No new fill expected on
                        // this entry, so both decision-time fields stay
                        // None.
                        new_legs.push(PendingLeg {
                            symbol: leg.symbol.clone(),
                            order_id: leg.order_id.clone(),
                            exchange_order_id: leg.exchange_order_id.clone(),
                            target: filled,
                            filled,
                            side: leg.side,
                            limit_price: None,
                            reference_price: None,
                        });
                    }
                    new_legs.push(PendingLeg {
                        symbol: leg.symbol.clone(),
                        order_id: resp.order_id,
                        exchange_order_id: resp.exchange_order_id,
                        target: quantized_size,
                        filled: Decimal::ZERO,
                        side: leg.side,
                        limit_price: limit,
                        reference_price: ref_price_reissue,
                    });
                }
                Err(e) => {
                    let symbol = leg.symbol.clone();
                    if reduce_only && engine::error_class::is_reduce_only_rejection(&e) {
                        if self.confirm_reduce_only_position_missing(&symbol).await {
                            log::info!(
                                "[ORDER] {} leg {} already closed; skipping reissue",
                                stage,
                                symbol
                            );
                            let mut kept = leg.clone();
                            kept.filled = leg.target;
                            new_legs.push(kept);
                        } else {
                            log::error!(
                                "[ORDER] Failed to reissue {} leg {}: {:?}",
                                stage,
                                symbol,
                                e
                            );
                            let mut kept = leg.clone();
                            kept.filled = filled;
                            new_legs.push(kept);
                        }
                    } else {
                        log::error!(
                            "[ORDER] Failed to reissue {} leg {}: {:?}",
                            stage,
                            symbol,
                            e
                        );
                        let mut kept = leg.clone();
                        kept.filled = filled;
                        new_legs.push(kept);
                    }
                }
            }
        }
        if new_legs.is_empty() {
            return Ok(None);
        }
        Ok(Some(PendingOrders {
            legs: new_legs,
            direction: pending.direction,
            placed_at: Instant::now(),
            placed_ts_ms: chrono::Utc::now().timestamp_millis(),
            hedge_retry_count: retry_count,
            post_only_hybrid: false,
            // The reissue is itself the taker-takeover step (either market or
            // a fresh post-only attempt budgeted by `order_timeout_secs`);
            // no further dedicated post-only takeover deadline applies.
            exit_taker_takeover_at: None,
        }))
    }

    pub(in crate::pairtrade) async fn reissue_entry_as_taker(
        &mut self,
        key: &str,
        pending: &PendingOrders,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Result<Option<PendingOrders>> {
        let mut new_legs = Vec::new();
        for leg in &pending.legs {
            let size = self.quantize_order_size(&leg.symbol, leg.target, price_map);
            if size <= Decimal::ZERO {
                log::warn!(
                    "[ORDER] {} taker reissue leg {} below min size; skipping",
                    key,
                    leg.symbol
                );
                continue;
            }
            // Decision-time reference for the taker reissue. Powers the
            // taker-side slippage histogram (#314 Group 4-B-2) — this is
            // the principal post-only-fallback path the BT/live gap
            // analysis cares about.
            let ref_price_taker =
                self.order_reference_price(&leg.symbol, leg.side, price_map);
            match self
                .connector
                .create_order(
                    &leg.symbol,
                    size,
                    leg.side,
                    None, // no limit price = market/taker
                    None,
                    false,
                    None,
                )
                .await
            {
                Ok(resp) => {
                    log::info!(
                        "[ORDER] {} taker reissue leg {} size={}",
                        key,
                        leg.symbol,
                        size
                    );
                    new_legs.push(PendingLeg {
                        symbol: leg.symbol.clone(),
                        order_id: resp.order_id,
                        exchange_order_id: resp.exchange_order_id,
                        target: size,
                        filled: Decimal::ZERO,
                        side: leg.side,
                        limit_price: None,
                        reference_price: ref_price_taker,
                    });
                }
                Err(e) => {
                    log::error!(
                        "[ORDER] {} taker reissue failed for {}: {:?}",
                        key,
                        leg.symbol,
                        e
                    );
                }
            }
        }
        if new_legs.is_empty() {
            return Ok(None);
        }
        Ok(Some(PendingOrders {
            legs: new_legs,
            direction: pending.direction,
            placed_at: Instant::now(),
            placed_ts_ms: chrono::Utc::now().timestamp_millis(),
            hedge_retry_count: 0,
            post_only_hybrid: false,
            // Entry path — no exit takeover deadline.
            exit_taker_takeover_at: None,
        }))
    }

    async fn create_order_with_post_only_retry(
        &mut self,
        symbol: &str,
        size: Decimal,
        side: dex_connector::OrderSide,
        reduce_only: bool,
        prices: &HashMap<String, SymbolSnapshot>,
        allow_post_only: bool,
        max_post_only_attempts: usize,
        fallback_to_taker: bool,
    ) -> Result<dex_connector::CreateOrderResponse, DexError> {
        let use_post_only = allow_post_only && self.should_post_only();
        let max_attempts = max_post_only_attempts.max(1);
        let max_elapsed = Duration::from_millis(POST_ONLY_RETRY_MAX_ELAPSED_MS);
        let start = Instant::now();
        let mut attempt = 0usize;
        #[allow(unused_assignments)]
        let mut last_limit: Option<Decimal> = None;

        let last_err = loop {
            attempt += 1;
            let limit = if use_post_only {
                self.refreshed_limit_price(symbol, side, prices).await
            } else {
                self.limit_price_for(symbol, side, prices)
            };
            if use_post_only && limit.is_none() {
                return Err(DexError::Transient(format!(
                    "[ORDER] Missing reference price for post-only {}",
                    symbol
                )));
            }
            last_limit = limit;
            let spread = self.order_spread_param(limit, use_post_only);
            match self
                .connector
                .create_order(symbol, size, side, limit, spread, reduce_only, None)
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    if !use_post_only {
                        return Err(err);
                    }
                    if attempt >= max_attempts || start.elapsed() >= max_elapsed {
                        break err;
                    }
                    // bot-strategy#165 Phase 0: capture per-attempt context so we
                    // can tell whether the exchange rejected for would-cross
                    // (post-only crossing the touch) vs. some other reason. Tag
                    // is grep-friendly alongside [ORDER_FALLBACK_DETAIL].
                    let snap = prices.get(symbol);
                    log::info!(
                        "[ORDER_REJECT_DETAIL] {} attempt={} side={:?} size={} limit={} bid={} ask={} err={}",
                        symbol,
                        attempt,
                        side,
                        size,
                        limit.map(|d| d.to_string()).unwrap_or_else(|| "none".into()),
                        snap.and_then(|s| s.bid_price)
                            .map(|d| d.to_string())
                            .unwrap_or_else(|| "?".into()),
                        snap.and_then(|s| s.ask_price)
                            .map(|d| d.to_string())
                            .unwrap_or_else(|| "?".into()),
                        format!("{:?}", err).chars().take(160).collect::<String>(),
                    );
                }
            }
            sleep(Duration::from_millis(POST_ONLY_RETRY_DELAY_MS)).await;
        };

        if use_post_only && fallback_to_taker {
            let snap = prices.get(symbol);
            log::warn!(
                "[ORDER] {} post-only attempts exhausted ({} attempts, elapsed={}ms); falling back to taker side={:?} size={} last_limit={} bid={} ask={} last_err={}",
                symbol,
                attempt,
                start.elapsed().as_millis(),
                side,
                size,
                last_limit.map(|d| d.to_string()).unwrap_or_else(|| "none".into()),
                snap.and_then(|s| s.bid_price)
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "?".into()),
                snap.and_then(|s| s.ask_price)
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "?".into()),
                format!("{:?}", last_err).chars().take(160).collect::<String>(),
            );
            return self
                .connector
                .create_order(symbol, size, side, None, None, reduce_only, None)
                .await;
        }

        Err(last_err)
    }

    /// Map a spread `direction` to the (base, quote) leg sides used when
    /// **opening** a pair position. LongSpread = long base / short quote;
    /// ShortSpread = short base / long quote. Extracted from the inline
    /// match in `place_pair_orders` so the leg-shape invariant is
    /// independently testable. bot-strategy#396.
    pub(in crate::pairtrade) fn entry_sides_for(
        direction: PositionDirection,
    ) -> (dex_connector::OrderSide, dex_connector::OrderSide) {
        match direction {
            PositionDirection::LongSpread => (
                dex_connector::OrderSide::Long,
                dex_connector::OrderSide::Short,
            ),
            PositionDirection::ShortSpread => (
                dex_connector::OrderSide::Short,
                dex_connector::OrderSide::Long,
            ),
        }
    }

    /// Map a spread `direction` to the (base, quote) leg sides used when
    /// **closing** the pair. Inverse of `entry_sides_for`. Bugs here
    /// double-the-leverage instead of flattening — the kind of failure
    /// that should never escape unit tests.
    pub(in crate::pairtrade) fn exit_sides_for(
        direction: PositionDirection,
    ) -> (dex_connector::OrderSide, dex_connector::OrderSide) {
        match direction {
            PositionDirection::LongSpread => (
                dex_connector::OrderSide::Short,
                dex_connector::OrderSide::Long,
            ),
            PositionDirection::ShortSpread => (
                dex_connector::OrderSide::Long,
                dex_connector::OrderSide::Short,
            ),
        }
    }

    pub(in crate::pairtrade) async fn place_pair_orders(
        &mut self,
        inst_idx: usize,
        pair: &PairSpec,
        direction: PositionDirection,
        qtys: (Decimal, Decimal),
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Result<Vec<PendingLeg>> {
        let (side_a, side_b) = Self::entry_sides_for(direction);
        let ref_price_a = self.order_reference_price(&pair.base, side_a, prices);
        let ref_price_b = self.order_reference_price(&pair.quote, side_b, prices);
        // Pick the floor/ceiling combination per leg that best preserves the
        // requested hedge ratio. With Extended's coarse ETH lot (0.01 ≈ $23) and a
        // small Phase 3 budget (~$25/leg), a single rounding direction routinely
        // produces 23-82% hedge deviation — searching the 4 combinations finds
        // the round that keeps the ratio intact. See bot-strategy#211.
        let qa_floor = self.quantize_order_size(&pair.base, qtys.0, prices);
        let qa_ceil = self.quantize_order_size_exit(&pair.base, qtys.0, prices);
        let qb_floor = self.quantize_order_size(&pair.quote, qtys.1, prices);
        let qb_ceil = self.quantize_order_size_exit(&pair.quote, qtys.1, prices);
        let (qty_a, qty_b, ratio_dev) = match order_pricing::pick_entry_quantize(
            qtys, qa_floor, qa_ceil, qb_floor, qb_ceil,
        ) {
            Some(x) => x,
            None => {
                log::warn!(
                    "[ORDER_ADJUST][ENTRY] {}/{} BLOCKED: zero qty after quantize (qty_a={}, qty_b={})",
                    pair.base, pair.quote, qa_floor, qb_floor
                );
                return Ok(Vec::new());
            }
        };
        if qty_a != qtys.0 {
            log::debug!(
                "[ORDER_ADJUST][ENTRY] {} settled qty_a {} -> {}",
                pair.base,
                qtys.0,
                qty_a
            );
        }
        if qty_b != qtys.1 {
            log::debug!(
                "[ORDER_ADJUST][ENTRY] {} settled qty_b {} -> {}",
                pair.quote,
                qtys.1,
                qty_b
            );
        }
        let pair_key_for_dev = format!("{}/{}", pair.base, pair.quote);
        let pp_for_dev = self.pair_params_for(inst_idx, &pair_key_for_dev).clone();
        let pp_for_dev = &pp_for_dev;
        if pp_for_dev.hedge_ratio_max_deviation < 1.0
            && ratio_dev > pp_for_dev.hedge_ratio_max_deviation
        {
            log::warn!(
                "[ORDER_ADJUST][ENTRY] {}/{} BLOCKED: hedge ratio deviation {:.1}% exceeds limit {:.1}%",
                pair.base, pair.quote, ratio_dev * 100.0, pp_for_dev.hedge_ratio_max_deviation * 100.0
            );
            return Ok(Vec::new());
        }
        let limit_a = self.limit_price_for(&pair.base, side_a, prices);
        let limit_b = self.limit_price_for(&pair.quote, side_b, prices);
        let pair_key_for_hybrid = format!("{}/{}", pair.base, pair.quote);
        let pp_for_hybrid = self.pair_params_for(inst_idx, &pair_key_for_hybrid).clone();
        let pp_for_hybrid = &pp_for_hybrid;
        let hybrid_active =
            pp_for_hybrid.entry_post_only_timeout_secs > 0 && self.post_only_supported();
        let post_only = self.should_post_only();
        let entry_attempts = if hybrid_active {
            1
        } else {
            POST_ONLY_ENTRY_ATTEMPTS
        };
        log::debug!(
            "[ORDER_PARAMS][ENTRY] pair={}/{} side_a={:?} qty_a={} ref_price_a={} limit_a={:?} side_b={:?} qty_b={} ref_price_b={} limit_b={:?} post_only={} hybrid={}",
            pair.base,
            pair.quote,
            side_a,
            qty_a,
            ref_price_a.unwrap_or(Decimal::ZERO),
            limit_a,
            side_b,
            qty_b,
            ref_price_b.unwrap_or(Decimal::ZERO),
            limit_b,
            post_only,
            hybrid_active
        );
        let mut legs: Vec<PendingLeg> = Vec::new();
        let res_a = self
            .create_order_with_post_only_retry(
                &pair.base,
                qty_a,
                side_a,
                false,
                prices,
                true,
                entry_attempts,
                false,
            )
            .await
            .context("place leg A")?;
        let target_a = if res_a.ordered_size > Decimal::ZERO {
            if res_a.ordered_size != qtys.0 {
                log::debug!(
                    "[ORDER_PARAMS][ENTRY] size adjusted by exchange for {}: requested={} ordered={}",
                    pair.base,
                    qtys.0,
                    res_a.ordered_size
                );
            }
            res_a.ordered_size
        } else {
            qtys.0
        };
        legs.push(PendingLeg {
            symbol: pair.base.clone(),
            order_id: res_a.order_id.clone(),
            exchange_order_id: res_a.exchange_order_id.clone(),
            target: target_a,
            filled: Decimal::ZERO,
            side: side_a,
            limit_price: limit_a,
            reference_price: ref_price_a,
        });

        let res_b = match self
            .create_order_with_post_only_retry(
                &pair.quote,
                qty_b,
                side_b,
                false,
                prices,
                true,
                entry_attempts,
                false,
            )
            .await
        {
            Ok(res) => res,
            Err(e) => {
                self.recover_from_leg_b_failure(pair, &res_a, side_a, &e).await;
                return Err(PartialOrderPlacementError::new(legs.clone(), e).into());
            }
        };
        let target_b = if res_b.ordered_size > Decimal::ZERO {
            if res_b.ordered_size != qtys.1 {
                log::debug!(
                    "[ORDER_PARAMS][ENTRY] size adjusted by exchange for {}: requested={} ordered={}",
                    pair.quote,
                    qtys.1,
                    res_b.ordered_size
                );
            }
            res_b.ordered_size
        } else {
            qtys.1
        };
        legs.push(PendingLeg {
            symbol: pair.quote.clone(),
            order_id: res_b.order_id.clone(),
            exchange_order_id: res_b.exchange_order_id.clone(),
            target: target_b,
            filled: Decimal::ZERO,
            side: side_b,
            limit_price: limit_b,
            reference_price: ref_price_b,
        });
        Ok(legs)
    }

    /// Recovery path when leg B placement fails after leg A succeeded:
    /// cancel leg A, wait briefly, check whether the exchange filled it
    /// anyway, and if so submit a market reduce-only order in the opposite
    /// direction to neutralize the unhedged exposure. All errors here are
    /// logged but not propagated — the caller still surfaces the original
    /// leg-B failure.
    async fn recover_from_leg_b_failure(
        &self,
        pair: &PairSpec,
        res_a: &dex_connector::CreateOrderResponse,
        side_a: dex_connector::OrderSide,
        leg_b_err: &DexError,
    ) {
        log::error!(
            "[ORDER] Failed to place leg B for {}/{} (leg A={}): {:?}",
            pair.base,
            pair.quote,
            res_a.order_id,
            leg_b_err
        );

        // Attempt to cancel leg A, but proceed even if it fails.
        if let Err(cancel_err) = self
            .connector
            .cancel_order(&pair.base, &res_a.order_id)
            .await
        {
            log::warn!(
                "[SAFETY] Failed to cancel leg A {} after leg B failed: {:?}",
                res_a.order_id,
                cancel_err
            );
        } else {
            log::info!(
                "[SAFETY] Canceled leg A {} after leg B failed.",
                res_a.order_id
            );
        }

        // Give the exchange time to settle any concurrent fill.
        sleep(Duration::from_secs(5)).await;

        let filled_orders = match self.connector.get_filled_orders(&pair.base).await {
            Ok(orders) => orders,
            Err(e) => {
                log::error!(
                    "[SAFETY] Could not check for filled orders for {}: {:?}",
                    pair.base,
                    e
                );
                return;
            }
        };

        let matches_order = |order_id: &str| {
            order_id == res_a.order_id
                || res_a
                    .exchange_order_id
                    .as_ref()
                    .map_or(false, |id| order_id == id)
        };
        let Some(filled_order) = filled_orders
            .orders
            .iter()
            .find(|o| matches_order(&o.order_id))
        else {
            return;
        };
        let filled_size = filled_order.filled_size.unwrap_or(Decimal::ZERO);
        if filled_size <= Decimal::ZERO {
            return;
        }

        log::warn!(
            "[SAFETY] Leg A {} was filled for {}. Hedging immediately.",
            res_a.order_id,
            pair.base
        );
        let hedge_side = match side_a {
            dex_connector::OrderSide::Long => dex_connector::OrderSide::Short,
            dex_connector::OrderSide::Short => dex_connector::OrderSide::Long,
        };
        if let Err(hedge_err) = self
            .connector
            .create_order(&pair.base, filled_size, hedge_side, None, None, true, None)
            .await
        {
            log::error!(
                "[SAFETY] FAILED TO HEDGE partial fill for {}: {:?}",
                pair.base,
                hedge_err
            );
        } else {
            log::info!(
                "[SAFETY] Successfully submitted hedge order for partial fill on {}",
                pair.base
            );
        }
    }

    pub(in crate::pairtrade) async fn close_pair_orders(
        &mut self,
        pair: &PairSpec,
        direction: PositionDirection,
        qtys: (Decimal, Decimal),
        prices: &HashMap<String, SymbolSnapshot>,
        use_market: bool,
    ) -> Result<(Vec<PendingLeg>, Option<Instant>)> {
        let (side_a, side_b) = Self::exit_sides_for(direction);
        let ref_price_a = self.order_reference_price(&pair.base, side_a, prices);
        let ref_price_b = self.order_reference_price(&pair.quote, side_b, prices);
        let qty_a = self.quantize_order_size_close(&pair.base, qtys.0, prices);
        let qty_b = self.quantize_order_size_close(&pair.quote, qtys.1, prices);
        if qty_a != qtys.0 {
            log::debug!(
                "[ORDER_ADJUST][EXIT] {} settled qty_a {} -> {}",
                pair.base,
                qtys.0,
                qty_a
            );
        }
        if qty_b != qtys.1 {
            log::debug!(
                "[ORDER_ADJUST][EXIT] {} settled qty_b {} -> {}",
                pair.quote,
                qtys.1,
                qty_b
            );
        }
        let limit_a = if use_market {
            None
        } else {
            self.limit_price_for(&pair.base, side_a, prices)
        };
        let limit_b = if use_market {
            None
        } else {
            self.limit_price_for(&pair.quote, side_b, prices)
        };
        let post_only = !use_market && self.should_post_only();
        log::debug!(
            "[ORDER_PARAMS][EXIT] pair={}/{} side_a={:?} qty_a={} ref_price_a={} limit_a={:?} side_b={:?} qty_b={} ref_price_b={} limit_b={:?} post_only={}",
            pair.base,
            pair.quote,
            side_a,
            qty_a,
            ref_price_a.unwrap_or(Decimal::ZERO),
            limit_a,
            side_b,
            qty_b,
            ref_price_b.unwrap_or(Decimal::ZERO),
            limit_b,
            post_only
        );
        let mut legs: Vec<PendingLeg> = Vec::new();
        let mut res_a = None;
        let mut skipped_already_closed = false;
        // The pre-flight reduce-only check below is only useful on Extended,
        // which exhibits cache/exchange state drift that yields HTTP 400 / code
        // 1137 noise. Lighter does not — skipping the extra get_positions RPC
        // there avoids unnecessary load against Lighter's stricter rate limit.
        let preflight_reduce_only = self.cfg.dex_name.contains("extended");
        if qty_a > Decimal::ZERO && preflight_reduce_only {
            if self.confirm_reduce_only_position_missing(&pair.base).await {
                log::info!(
                    "[ORDER] {} reduce-only close skipped (preflight); position already closed",
                    pair.base
                );
                skipped_already_closed = true;
            }
        }
        if qty_a > Decimal::ZERO && !skipped_already_closed {
            let res = if use_market {
                self.connector
                    .create_order(&pair.base, qty_a, side_a, None, None, true, None)
                    .await
            } else {
                self.create_order_with_post_only_retry(
                    &pair.base,
                    qty_a,
                    side_a,
                    true,
                    prices,
                    true,
                    POST_ONLY_EXIT_ATTEMPTS,
                    true,
                )
                .await
            };
            match res {
                Ok(res) => {
                    if res.ordered_size > Decimal::ZERO && res.ordered_size != qty_a {
                        log::debug!(
                            "[ORDER_PARAMS][EXIT] size adjusted by exchange for {}: requested={} ordered={}",
                            pair.base,
                            qty_a,
                            res.ordered_size
                        );
                    }
                    legs.push(PendingLeg {
                        symbol: pair.base.clone(),
                        order_id: res.order_id.clone(),
                        exchange_order_id: res.exchange_order_id.clone(),
                        target: qty_a,
                        filled: Decimal::ZERO,
                        side: side_a,
                        // limit_price was previously hardcoded to None
                        // for exit legs even on the post-only path,
                        // which silently disabled exit-side slippage
                        // observation in Group 4-B. Carry through the
                        // actual posted limit so the order_type tag
                        // resolves correctly (#314 Group 4-B-2).
                        limit_price: limit_a,
                        reference_price: ref_price_a,
                    });
                    res_a = Some(res);
                }
                Err(err) => {
                    if engine::error_class::is_reduce_only_rejection(&err) {
                        let symbol = pair.base.clone();
                        if self.confirm_reduce_only_position_missing(&symbol).await {
                            log::info!(
                                "[ORDER] {} reduce-only close skipped; position already closed",
                                symbol
                            );
                            skipped_already_closed = true;
                        } else {
                            return Err(err).context("close leg A");
                        }
                    } else {
                        return Err(err).context("close leg A");
                    }
                }
            }
        }

        let mut quote_already_flat = false;
        if qty_b > Decimal::ZERO && preflight_reduce_only {
            if self.confirm_reduce_only_position_missing(&pair.quote).await {
                log::info!(
                    "[ORDER] {} reduce-only close skipped (preflight); position already closed",
                    pair.quote
                );
                quote_already_flat = true;
                skipped_already_closed = true;
            }
        }
        if qty_b > Decimal::ZERO && !quote_already_flat {
            let res_b = if use_market {
                self.connector
                    .create_order(&pair.quote, qty_b, side_b, None, None, true, None)
                    .await
            } else {
                self.create_order_with_post_only_retry(
                    &pair.quote,
                    qty_b,
                    side_b,
                    true,
                    prices,
                    true,
                    POST_ONLY_EXIT_ATTEMPTS,
                    true,
                )
                .await
            };
            let res_b = match res_b {
                Ok(res) => Some(res),
                Err(e) => {
                    let mut skip = false;
                    if engine::error_class::is_reduce_only_rejection(&e) {
                        let symbol = pair.quote.clone();
                        if self.confirm_reduce_only_position_missing(&symbol).await {
                            log::info!(
                                "[ORDER] {} reduce-only close skipped; position already closed",
                                symbol
                            );
                            skip = true;
                            skipped_already_closed = true;
                        }
                    }
                    if skip {
                        None
                    } else {
                        if let Some(ref res_a) = res_a {
                            self.recover_from_leg_b_failure(pair, res_a, side_a, &e).await;
                        } else {
                            log::error!(
                                "[ORDER] Failed to close leg B for {}/{}: {:?}",
                                pair.base,
                                pair.quote,
                                e
                            );
                        }

                        return Err(PartialOrderPlacementError::new(legs.clone(), e).into());
                    }
                }
            };
            if let Some(res_b) = res_b {
                if res_b.ordered_size > Decimal::ZERO && res_b.ordered_size != qty_b {
                    log::debug!(
                        "[ORDER_PARAMS][EXIT] size adjusted by exchange for {}: requested={} ordered={}",
                        pair.quote,
                        qty_b,
                        res_b.ordered_size
                    );
                }
                legs.push(PendingLeg {
                    symbol: pair.quote.clone(),
                    order_id: res_b.order_id.clone(),
                    exchange_order_id: res_b.exchange_order_id.clone(),
                    target: qty_b,
                    filled: Decimal::ZERO,
                    side: side_b,
                    // Same fix as the leg-A site above — exit legs
                    // need limit_price carried through for post-only
                    // slippage tagging (#314 Group 4-B-2).
                    limit_price: limit_b,
                    reference_price: ref_price_b,
                });
            }
        }

        if legs.is_empty() {
            if skipped_already_closed {
                log::info!(
                    "[ORDER] No exit legs placed for {}/{}; positions already flat",
                    pair.base,
                    pair.quote
                );
            } else {
                log::warn!(
                    "[ORDER] No exit legs placed for {}/{} (qty_a={}, qty_b={})",
                    pair.base,
                    pair.quote,
                    qty_a,
                    qty_b
                );
            }
        }

        // bot-strategy#306 / #408: on fee-bearing venues (Extended) where the
        // exit went out post-only, schedule a deadline at which the reconcile
        // loop will cancel the resting legs and reissue as taker. Frankfurt
        // (fee_bps=0) takes the use_market / non-post-only path above, so
        // post_only=false here and `takeover_at` stays None — Frankfurt
        // behavior is unchanged regardless of the configured timeout. Prior
        // to #408 this was a synchronous in-step monitor that blocked
        // `step()` for the full timeout and caused STEP_OVERRUN warns.
        let exit_timeout = self.cfg.default_pair_params.exit_post_only_timeout_secs;
        let takeover_at = if post_only && exit_timeout > 0 && !legs.is_empty() {
            Some(Instant::now() + Duration::from_secs(exit_timeout))
        } else {
            None
        };

        Ok((legs, takeover_at))
    }

    pub(in crate::pairtrade) fn register_partial_leg_failure(
        &mut self,
        inst_idx: usize,
        key: &str,
        direction: PositionDirection,
        err: &anyhow::Error,
        is_exit: bool,
    ) {
        if let Some(partial) = err.downcast_ref::<PartialOrderPlacementError>() {
            if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                let pending = PendingOrders {
                    legs: partial.legs().to_vec(),
                    direction,
                    placed_at: Instant::now(),
                    placed_ts_ms: chrono::Utc::now().timestamp_millis(),
                    hedge_retry_count: 0,
                    post_only_hybrid: false,
                    // Partial-failure recovery — let the next reconcile tick
                    // decide normal-timeout behavior; no fast takeover.
                    exit_taker_takeover_at: None,
                };
                if is_exit {
                    state.pending_exit = Some(pending);
                } else {
                    state.pending_entry = Some(pending);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Coverage for the static leg-side maps used by
    //! `place_pair_orders` / `close_pair_orders`. A wrong side here
    //! doubles leverage on close instead of flattening; the inverse-pair
    //! invariant is therefore worth pinning at the unit level
    //! independent of the engine. bot-strategy#396.
    use dex_connector::OrderSide;

    use super::super::super::state::PositionDirection;
    use super::PairTradeEngine;

    #[test]
    fn entry_sides_long_spread() {
        let (a, b) = PairTradeEngine::entry_sides_for(PositionDirection::LongSpread);
        assert_eq!(a, OrderSide::Long);
        assert_eq!(b, OrderSide::Short);
    }

    #[test]
    fn entry_sides_short_spread() {
        let (a, b) = PairTradeEngine::entry_sides_for(PositionDirection::ShortSpread);
        assert_eq!(a, OrderSide::Short);
        assert_eq!(b, OrderSide::Long);
    }

    #[test]
    fn exit_sides_invert_entry_sides_long_spread() {
        // Exit must reverse both legs; otherwise a "close" doubles
        // leverage instead of flattening.
        let (ea, eb) = PairTradeEngine::entry_sides_for(PositionDirection::LongSpread);
        let (xa, xb) = PairTradeEngine::exit_sides_for(PositionDirection::LongSpread);
        assert_ne!(ea, xa);
        assert_ne!(eb, xb);
        assert_eq!(xa, OrderSide::Short);
        assert_eq!(xb, OrderSide::Long);
    }

    #[test]
    fn exit_sides_invert_entry_sides_short_spread() {
        let (ea, eb) = PairTradeEngine::entry_sides_for(PositionDirection::ShortSpread);
        let (xa, xb) = PairTradeEngine::exit_sides_for(PositionDirection::ShortSpread);
        assert_ne!(ea, xa);
        assert_ne!(eb, xb);
        assert_eq!(xa, OrderSide::Long);
        assert_eq!(xb, OrderSide::Short);
    }
}
