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
use super::super::prom;
use super::super::state::{
    PartialOrderPlacementError, PendingLeg, PendingOrders, PositionDirection,
};
use super::super::PairTradeEngine;

pub(in crate::pairtrade) struct ReissuePartialLegsRequest<'a> {
    pub(in crate::pairtrade) pending: &'a PendingOrders,
    pub(in crate::pairtrade) filled_qtys: &'a HashMap<String, Decimal>,
    pub(in crate::pairtrade) price_map: &'a HashMap<String, SymbolSnapshot>,
    pub(in crate::pairtrade) reduce_only: bool,
    pub(in crate::pairtrade) use_market: bool,
    pub(in crate::pairtrade) retry_count: u32,
    pub(in crate::pairtrade) use_amend: bool,
}

struct PostOnlyOrderRequest<'a> {
    symbol: &'a str,
    size: Decimal,
    side: dex_connector::OrderSide,
    reduce_only: bool,
    prices: &'a HashMap<String, SymbolSnapshot>,
    allow_post_only: bool,
    max_post_only_attempts: usize,
    fallback_to_taker: bool,
    capital_guard_inst_idx: Option<usize>,
}

/// See `PairTradeEngine::capital_guard_prior_state` /
/// `unlatch_capital_guard_if_no_order_was_ever_created`.
#[derive(Clone, Copy)]
struct CapitalGuardPriorState {
    inst_idx: usize,
    was_seen: bool,
    flat_since: Option<Instant>,
}

pub(in crate::pairtrade) struct OrderSubmitMetadata {
    submitted_qty: Decimal,
    submit_ts_ms: i64,
    submit_reference_price: Option<Decimal>,
    submit_mid: Option<Decimal>,
    submit_bid: Option<Decimal>,
    submit_ask: Option<Decimal>,
}

pub(in crate::pairtrade) struct PostOnlyOrderResult {
    response: dex_connector::CreateOrderResponse,
    post_only: bool,
    limit_price: Option<Decimal>,
    submitted_qty: Decimal,
    submit_ts_ms: i64,
    ack_ts_ms: Option<i64>,
    submit_reference_price: Option<Decimal>,
    submit_mid: Option<Decimal>,
    submit_bid: Option<Decimal>,
    submit_ask: Option<Decimal>,
}

/// Per-leg inputs for `place_or_amend_reissue_leg`, grouped so the amend /
/// cancel+reissue decision can live in one named helper without widening
/// its argument list past the clippy boundary (bot-strategy#502).
struct ReissueLegPlacement<'a> {
    stage: &'a str,
    leg: &'a PendingLeg,
    quantized_size: Decimal,
    limit: Option<Decimal>,
    spread: Option<i64>,
    reduce_only: bool,
    use_amend: bool,
    price_map: &'a HashMap<String, SymbolSnapshot>,
}

impl PairTradeEngine {
    /// In amend mode the caller skips the upstream blanket cancel so the
    /// order we are about to amend stays alive. Any leg we end up NOT
    /// amending (fully filled, below tick, missing price) may still have an
    /// open remainder resting, so cancel it here to reproduce the legacy
    /// cancel-then-reissue end state (bot-strategy#471). No-op when not
    /// amending.
    async fn cancel_amend_skipped_leg(&self, use_amend: bool, leg: &PendingLeg) {
        if use_amend {
            let _ = self
                .connector
                .cancel_order(&leg.symbol, &leg.order_id)
                .await;
        }
    }

    /// Clone `leg` with its `filled` field overwritten — the "keep this leg
    /// as-is and carry the observed fill" pattern repeated across every
    /// skip/failure branch of the reissue loop (bot-strategy#502).
    fn kept_leg(leg: &PendingLeg, filled: Decimal) -> PendingLeg {
        let mut kept = leg.clone();
        kept.filled = filled;
        kept
    }

    /// Preserve the execution metadata for an already-filled slice while
    /// shrinking its target to the settled size. Used when cancel+reissue
    /// splits one original leg into a completed slice plus a fresh
    /// remainder; ledger attribution still needs the original order mode and
    /// reference price for the completed slice.
    fn settled_leg(leg: &PendingLeg, filled: Decimal) -> PendingLeg {
        let mut settled = leg.clone();
        settled.target = filled;
        settled.filled = filled;
        settled
    }

    pub(in crate::pairtrade) fn order_submit_metadata(
        &self,
        symbol: &str,
        size: Decimal,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> OrderSubmitMetadata {
        match prices.get(symbol) {
            Some(snapshot) => {
                self.order_submit_metadata_from_snapshot(symbol, size, side, snapshot)
            }
            None => OrderSubmitMetadata {
                submitted_qty: size,
                submit_ts_ms: chrono::Utc::now().timestamp_millis(),
                submit_reference_price: None,
                submit_mid: None,
                submit_bid: None,
                submit_ask: None,
            },
        }
    }

    pub(in crate::pairtrade) fn order_submit_metadata_from_snapshot(
        &self,
        symbol: &str,
        size: Decimal,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> OrderSubmitMetadata {
        OrderSubmitMetadata {
            submitted_qty: size,
            submit_ts_ms: chrono::Utc::now().timestamp_millis(),
            submit_reference_price: Some(
                self.order_reference_price_from_snapshot(symbol, side, snapshot),
            ),
            submit_mid: Some(snapshot.price),
            submit_bid: snapshot.bid_price,
            submit_ask: snapshot.ask_price,
        }
    }

    pub(in crate::pairtrade) fn order_result_from_response(
        response: dex_connector::CreateOrderResponse,
        post_only: bool,
        limit_price: Option<Decimal>,
        meta: OrderSubmitMetadata,
    ) -> PostOnlyOrderResult {
        PostOnlyOrderResult {
            response,
            post_only,
            limit_price,
            submitted_qty: meta.submitted_qty,
            submit_ts_ms: meta.submit_ts_ms,
            ack_ts_ms: Some(chrono::Utc::now().timestamp_millis()),
            submit_reference_price: meta.submit_reference_price,
            submit_mid: meta.submit_mid,
            submit_bid: meta.submit_bid,
            submit_ask: meta.submit_ask,
        }
    }

    pub(in crate::pairtrade) fn pending_leg_from_order(
        symbol: String,
        side: dex_connector::OrderSide,
        target: Decimal,
        filled: Decimal,
        reference_price: Option<Decimal>,
        reduce_only: bool,
        result: PostOnlyOrderResult,
    ) -> PendingLeg {
        let dex_connector::CreateOrderResponse {
            order_id,
            exchange_order_id,
            ordered_price: _,
            ordered_size: _,
            client_order_id,
        } = result.response;
        PendingLeg {
            symbol,
            order_id,
            exchange_order_id,
            target,
            filled,
            side,
            submitted_qty: result.submitted_qty,
            limit_price: result.limit_price,
            reference_price,
            submit_ts_ms: result.submit_ts_ms,
            ack_ts_ms: result.ack_ts_ms,
            // Stamped from the owning PendingOrders.placed_ts_ms via
            // `with_leg_decision_ts` after the group is built; a fresh leg
            // starts at 0 so it inherits this placement's decision time.
            decision_ts_ms: 0,
            submit_reference_price: result.submit_reference_price,
            submit_mid: result.submit_mid,
            submit_bid: result.submit_bid,
            submit_ask: result.submit_ask,
            client_order_id,
            reduce_only,
            post_only: result.post_only,
        }
    }

    /// Derive the effective `(filled, remaining)` for one reissue leg.
    /// On the entry-side path this cross-checks the exchange position
    /// against the local fill state (bot-strategy#470): the
    /// cancel-then-reissue boundary can drop fills from the local map (or
    /// carry over a stale `leg.filled = 0`), and the unconditional
    /// `target - local` arithmetic then sends a full duplicate order on top
    /// of qty already sitting on the exchange (Frankfurt 2026-05-22 variant
    /// C ETH leg ended at 2× target via this race). Pure relocation from
    /// `reissue_partial_legs` (bot-strategy#502).
    async fn reissue_leg_fill_state(
        &mut self,
        leg: &PendingLeg,
        local_filled: Decimal,
        reduce_only: bool,
    ) -> (Decimal, Decimal) {
        if !reduce_only {
            let exch = self.fetch_residual_position_size(&leg.symbol).await;
            let r = Self::cap_entry_reissue_remaining(leg.target, local_filled, exch);
            let effective = leg.target - r;
            if let Some(exch_qty) = exch {
                if exch_qty > local_filled {
                    log::warn!(
                        "[ENTRY_CAP] {} exchange position {} exceeds local filled {} (target {}); \
                         capping reissue remaining to {}",
                        leg.symbol,
                        exch_qty,
                        local_filled,
                        leg.target,
                        r,
                    );
                    let variant_id = self.instances.first().map(|i| i.id.as_str()).unwrap_or("?");
                    // The reconcile loop drives only one (variant,
                    // pair) at a time, so labelling with `*` for pair
                    // is fine — the symbol label is the discriminator.
                    prom::ENTRY_OVERSIZE_CAPPED_TOTAL
                        .with_label_values(&[variant_id, "*", &leg.symbol])
                        .inc();
                }
            }
            (effective, r)
        } else {
            let r = (leg.target - local_filled).max(Decimal::ZERO);
            (local_filled, r)
        }
    }

    /// Place one reissue leg: native amend when opted in (bot-strategy#471),
    /// falling back to cancel+reissue on any amend error or when no
    /// maker-safe price exists for a post-only leg. Resolves to
    /// `PostOnlyOrderResult` so the rebuilt `PendingLeg` records how the
    /// live order actually rests on the venue. Pure
    /// relocation from `reissue_partial_legs` (bot-strategy#502).
    async fn place_or_amend_reissue_leg(
        &mut self,
        req: ReissueLegPlacement<'_>,
    ) -> Result<PostOnlyOrderResult, DexError> {
        let ReissueLegPlacement {
            stage,
            leg,
            quantized_size,
            limit,
            spread,
            reduce_only,
            use_amend,
            price_map,
        } = req;
        // bot-strategy#471: amend in place of cancel+reissue when opted
        // in. `target_total_size = leg.target` re-asserts the original
        // total (native-amend venues keep the filled portion and re-open
        // only the remainder); `open_remaining_size = quantized_size` is
        // the #470-capped remainder that cancel-replace venues place
        // afresh. On any amend error fall back to cancel+reissue for this
        // leg (the upstream blanket cancel was skipped in amend mode).
        //
        // Extended's edit endpoint only permits price/size changes — the
        // replacement order must re-assert the original post-only flag
        // (with a maker-safe refreshed price) or the venue rejects the
        // whole edit with 1133 InvalidOrderParameters. Without this,
        // every first-retry amend of a post-only entry leg burned the
        // edit and fell back (2026-06-09..12 Tokyo soak). A post-only
        // leg with no maker-safe price available cannot be amended
        // without flipping the flag, so it takes the fallback path.
        let amend_pricing = if !use_amend {
            None
        } else if leg.post_only {
            self.refreshed_limit_price(&leg.symbol, leg.side, price_map)
                .await
                .filter(|pricing| pricing.limit > Decimal::ZERO)
                .map(|pricing| (Some(pricing.limit), Some(-2), pricing.submit_snapshot))
                .or_else(|| limit.map(|px| (Some(px), Some(-2), None)))
        } else {
            Some((limit, spread, None))
        };
        match amend_pricing {
            Some((amend_limit, amend_spread, submit_snapshot)) => {
                let meta = match submit_snapshot.as_ref() {
                    Some(snapshot) => self.order_submit_metadata_from_snapshot(
                        &leg.symbol,
                        quantized_size,
                        leg.side,
                        snapshot,
                    ),
                    None => {
                        self.order_submit_metadata(&leg.symbol, quantized_size, leg.side, price_map)
                    }
                };
                match self
                    .connector
                    .modify_order(
                        &leg.symbol,
                        &leg.order_id,
                        leg.side,
                        leg.target,
                        quantized_size,
                        amend_limit,
                        amend_spread,
                        reduce_only,
                    )
                    .await
                {
                    Ok(resp) => Ok(Self::order_result_from_response(
                        resp,
                        leg.post_only,
                        amend_limit,
                        meta,
                    )),
                    Err(e) => {
                        log::warn!(
                            "[ORDER] amend failed for {} leg {} ({:?}); falling back to cancel+reissue",
                            stage,
                            leg.symbol,
                            e
                        );
                        let _ = self
                            .connector
                            .cancel_order(&leg.symbol, &leg.order_id)
                            .await;
                        let fallback_meta = self.order_submit_metadata(
                            &leg.symbol,
                            quantized_size,
                            leg.side,
                            price_map,
                        );
                        self.connector
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
                            .map(|resp| {
                                Self::order_result_from_response(resp, false, limit, fallback_meta)
                            })
                    }
                }
            }
            None => {
                if use_amend {
                    log::warn!(
                        "[ORDER] cannot amend {} leg {}: no maker-safe price for post-only; falling back to cancel+reissue",
                        stage,
                        leg.symbol
                    );
                    let _ = self
                        .connector
                        .cancel_order(&leg.symbol, &leg.order_id)
                        .await;
                }
                let meta =
                    self.order_submit_metadata(&leg.symbol, quantized_size, leg.side, price_map);
                self.connector
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
                    .map(|resp| Self::order_result_from_response(resp, false, limit, meta))
            }
        }
    }

    pub(in crate::pairtrade) async fn reissue_partial_legs(
        &mut self,
        request: ReissuePartialLegsRequest<'_>,
    ) -> Result<Option<PendingOrders>> {
        let placed_ts_ms = chrono::Utc::now().timestamp_millis();
        let ReissuePartialLegsRequest {
            pending,
            filled_qtys,
            price_map,
            reduce_only,
            use_market,
            retry_count,
            use_amend,
        } = request;
        let mut new_legs = Vec::new();
        let stage = if reduce_only { "exit" } else { "entry" };
        // Clone the legs first so each iteration can call `&mut self`
        // helpers (exchange-position fetch / order placement) without
        // tripping the borrow against `&pending.legs`.
        let pending_legs: Vec<PendingLeg> = pending.legs.clone();
        for leg in &pending_legs {
            let local_filled = filled_qtys
                .get(&leg.order_id)
                .cloned()
                .unwrap_or(Decimal::ZERO)
                .max(leg.filled)
                .min(leg.target);
            // bot-strategy#470: on the entry-side reissue path, cross-
            // check the exchange position against the local fill state.
            // The cancel-then-reissue boundary can drop fills from the
            // local map (or carry over a stale `leg.filled = 0`), and the
            // unconditional `target - local` arithmetic then sends a full
            // duplicate order on top of qty already sitting on the
            // exchange. Frankfurt 2026-05-22 06:27 UTC variant C ETH leg
            // ended at 2× target (0.8905 → 1.7810) via this race.
            let (filled, remaining) = self
                .reissue_leg_fill_state(leg, local_filled, reduce_only)
                .await;
            if remaining <= Decimal::ZERO {
                self.cancel_amend_skipped_leg(use_amend, leg).await;
                new_legs.push(Self::kept_leg(leg, filled));
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
                    self.cancel_amend_skipped_leg(use_amend, leg).await;
                    new_legs.push(Self::kept_leg(leg, filled));
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
                self.cancel_amend_skipped_leg(use_amend, leg).await;
                new_legs.push(Self::kept_leg(leg, filled));
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
                self.cancel_amend_skipped_leg(use_amend, leg).await;
                new_legs.push(Self::kept_leg(leg, filled));
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
                new_legs.push(Self::kept_leg(leg, leg.target));
                continue;
            }
            // Each placement resolves to the live order mode and submit metadata
            // so the rebuilt PendingLeg records how the live order actually
            // rests on the venue (see place_or_amend_reissue_leg for the
            // amend-vs-cancel+reissue policy, bot-strategy#471).
            let placement = self
                .place_or_amend_reissue_leg(ReissueLegPlacement {
                    stage,
                    leg,
                    quantized_size,
                    limit,
                    spread,
                    reduce_only,
                    use_amend,
                    price_map,
                })
                .await;
            match placement {
                Ok(result) => {
                    let amended_in_place = use_amend && result.response.order_id == leg.order_id;
                    if amended_in_place {
                        log::info!(
                            "[ORDER] Amended {} leg {} remaining={} in place (order {})",
                            stage,
                            leg.symbol,
                            quantized_size,
                            leg.order_id
                        );
                        let mut amended = Self::pending_leg_from_order(
                            leg.symbol.clone(),
                            leg.side,
                            leg.target,
                            filled,
                            ref_price_reissue,
                            reduce_only,
                            result,
                        );
                        amended.exchange_order_id = leg.exchange_order_id.clone();
                        new_legs.push(amended);
                    } else {
                        log::info!(
                            "[ORDER] Reissued {} leg {} size={}",
                            stage,
                            leg.symbol,
                            quantized_size
                        );
                        if filled > Decimal::ZERO {
                            new_legs.push(Self::settled_leg(leg, filled));
                        }
                        new_legs.push(Self::pending_leg_from_order(
                            leg.symbol.clone(),
                            leg.side,
                            quantized_size,
                            Decimal::ZERO,
                            ref_price_reissue,
                            reduce_only,
                            result,
                        ));
                    }
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
                            new_legs.push(Self::kept_leg(leg, leg.target));
                        } else {
                            log::error!(
                                "[ORDER] Failed to reissue {} leg {}: {:?}",
                                stage,
                                symbol,
                                e
                            );
                            new_legs.push(Self::kept_leg(leg, filled));
                        }
                    } else {
                        log::error!(
                            "[ORDER] Failed to reissue {} leg {}: {:?}",
                            stage,
                            symbol,
                            e
                        );
                        new_legs.push(Self::kept_leg(leg, filled));
                    }
                }
            }
        }
        if new_legs.is_empty() {
            return Ok(None);
        }
        Ok(Some(
            PendingOrders {
                legs: new_legs,
                direction: pending.direction,
                placed_at: Instant::now(),
                placed_ts_ms,
                hedge_retry_count: retry_count,
                post_only_hybrid: false,
                // The reissue is itself the taker-takeover step (either market
                // or a fresh post-only attempt budgeted by `order_timeout_secs`);
                // no further dedicated post-only takeover deadline applies.
                exit_taker_takeover_at: None,
            }
            // Fresh reissue legs inherit this reissue's decision time; legs
            // kept/settled forward keep their original decision time.
            .with_leg_decision_ts(),
        ))
    }

    pub(in crate::pairtrade) async fn reissue_entry_as_taker(
        &mut self,
        key: &str,
        pending: &PendingOrders,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Result<Option<PendingOrders>> {
        let placed_ts_ms = chrono::Utc::now().timestamp_millis();
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
            let ref_price_taker = self.order_reference_price(&leg.symbol, leg.side, price_map);
            let meta = self.order_submit_metadata(&leg.symbol, size, leg.side, price_map);
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
                .map(|resp| Self::order_result_from_response(resp, false, None, meta))
            {
                Ok(resp) => {
                    log::info!(
                        "[ORDER] {} taker reissue leg {} size={}",
                        key,
                        leg.symbol,
                        size
                    );
                    new_legs.push(Self::pending_leg_from_order(
                        leg.symbol.clone(),
                        leg.side,
                        size,
                        Decimal::ZERO,
                        ref_price_taker,
                        false,
                        resp,
                    ));
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
        Ok(Some(
            PendingOrders {
                legs: new_legs,
                direction: pending.direction,
                placed_at: Instant::now(),
                placed_ts_ms,
                hedge_retry_count: 0,
                post_only_hybrid: false,
                // Entry path — no exit takeover deadline.
                exit_taker_takeover_at: None,
            }
            .with_leg_decision_ts(),
        ))
    }

    async fn create_order_with_post_only_retry(
        &mut self,
        request: PostOnlyOrderRequest<'_>,
    ) -> Result<PostOnlyOrderResult, DexError> {
        let PostOnlyOrderRequest {
            symbol,
            size,
            side,
            reduce_only,
            prices,
            allow_post_only,
            max_post_only_attempts,
            fallback_to_taker,
            capital_guard_inst_idx,
        } = request;
        let use_post_only = allow_post_only && self.should_post_only();
        let max_attempts = max_post_only_attempts.max(1);
        let max_elapsed = Duration::from_millis(POST_ONLY_RETRY_MAX_ELAPSED_MS);
        let start = Instant::now();
        let mut attempt = 0usize;
        #[allow(unused_assignments)]
        let mut last_limit: Option<Decimal> = None;
        // Carry the most recent refreshed book snapshot so a taker fallback
        // (below) can price submit metadata against the book the last post-only
        // attempt actually saw, instead of the stale decision-time `prices` map.
        #[allow(unused_assignments)]
        let mut last_submit_snapshot: Option<SymbolSnapshot> = None;
        // Captured once, before the first attempt: every attempt below
        // reuses the same inst_idx, and the guard can only ever transition
        // false-to-true on the very first one, so this is the sole
        // reference point unlatch needs regardless of how many attempts
        // this operation ends up making.
        let capital_guard_prior_state =
            capital_guard_inst_idx.map(|inst_idx| self.capital_guard_prior_state(inst_idx));
        // Whether *every* create_order attempt this operation has made so
        // far definitively proved no order was created. A single ambiguous
        // attempt anywhere latches this false permanently, since that one
        // attempt alone could have created real exposure.
        let mut capital_guard_every_attempt_definitively_rejected = true;

        let last_err = loop {
            attempt += 1;
            let (limit, submit_snapshot) = if use_post_only {
                match self.refreshed_limit_price(symbol, side, prices).await {
                    Some(pricing) => (Some(pricing.limit), pricing.submit_snapshot),
                    None => (None, None),
                }
            } else {
                (self.limit_price_for(symbol, side, prices), None)
            };
            if use_post_only && limit.is_none() {
                // A later retry's own pricing failure (not attempt 1's,
                // which the guard-latch below hasn't even run for yet on
                // this operation) never reaches create_order, so it can't
                // change what the prior attempts already proved. If every
                // one of them was a definitive DexError::ServerResponse,
                // this exit must still unlatch -- otherwise this early
                // return silently bypasses every unlatch call the loop
                // and taker-fallback paths make, leaving the guard latched
                // indefinitely (Codex P2 follow-up, bot-strategy#783).
                if attempt > 1 {
                    self.unlatch_capital_guard_if_no_order_was_ever_created(
                        capital_guard_prior_state,
                        capital_guard_every_attempt_definitively_rejected,
                    );
                }
                return Err(DexError::Transient(format!(
                    "[ORDER] Missing reference price for post-only {}",
                    symbol
                )));
            }
            last_limit = limit;
            // Track the submit source of *this* attempt unconditionally so a
            // later taker fallback prices its metadata against the book the
            // final failed post-only attempt actually saw. If this attempt got
            // no fresh snapshot (refresh returned None), clear the cache so the
            // fallback drops to the decision-time `prices` map rather than
            // reusing an earlier attempt's stale refresh across the retry gap.
            last_submit_snapshot = submit_snapshot.clone();
            let spread = self.order_spread_param(limit, use_post_only);
            let meta = match submit_snapshot.as_ref() {
                Some(snapshot) => {
                    self.order_submit_metadata_from_snapshot(symbol, size, side, snapshot)
                }
                None => self.order_submit_metadata(symbol, size, side, prices),
            };
            if let Some(inst_idx) = capital_guard_inst_idx {
                self.latch_capital_position_activity(inst_idx);
            }
            match self
                .connector
                .create_order(symbol, size, side, limit, spread, reduce_only, None)
                .await
            {
                Ok(response) => {
                    return Ok(Self::order_result_from_response(
                        response,
                        use_post_only,
                        limit,
                        meta,
                    ));
                }
                Err(err) => {
                    capital_guard_every_attempt_definitively_rejected &=
                        matches!(err, DexError::ServerResponse(_));
                    if !use_post_only {
                        self.unlatch_capital_guard_if_no_order_was_ever_created(
                            capital_guard_prior_state,
                            capital_guard_every_attempt_definitively_rejected,
                        );
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
            let meta = match last_submit_snapshot.as_ref() {
                Some(snapshot) => {
                    self.order_submit_metadata_from_snapshot(symbol, size, side, snapshot)
                }
                None => self.order_submit_metadata(symbol, size, side, prices),
            };
            return match self
                .connector
                .create_order(symbol, size, side, None, None, reduce_only, None)
                .await
            {
                Ok(response) => Ok(Self::order_result_from_response(response, false, None, meta)),
                Err(err) => {
                    capital_guard_every_attempt_definitively_rejected &=
                        matches!(err, DexError::ServerResponse(_));
                    self.unlatch_capital_guard_if_no_order_was_ever_created(
                        capital_guard_prior_state,
                        capital_guard_every_attempt_definitively_rejected,
                    );
                    Err(err)
                }
            };
        }

        self.unlatch_capital_guard_if_no_order_was_ever_created(
            capital_guard_prior_state,
            capital_guard_every_attempt_definitively_rejected,
        );
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
            .create_order_with_post_only_retry(PostOnlyOrderRequest {
                symbol: &pair.base,
                size: qty_a,
                side: side_a,
                reduce_only: false,
                prices,
                allow_post_only: true,
                max_post_only_attempts: entry_attempts,
                fallback_to_taker: false,
                capital_guard_inst_idx: Some(inst_idx),
            })
            .await
            .context("place leg A")?;
        let target_a = if res_a.response.ordered_size > Decimal::ZERO {
            if res_a.response.ordered_size != qtys.0 {
                log::debug!(
                    "[ORDER_PARAMS][ENTRY] size adjusted by exchange for {}: requested={} ordered={}",
                    pair.base,
                    qtys.0,
                    res_a.response.ordered_size
                );
            }
            res_a.response.ordered_size
        } else {
            qtys.0
        };
        let res_a_response = dex_connector::CreateOrderResponse {
            order_id: res_a.response.order_id.clone(),
            exchange_order_id: res_a.response.exchange_order_id.clone(),
            ordered_price: res_a.response.ordered_price,
            ordered_size: res_a.response.ordered_size,
            client_order_id: res_a.response.client_order_id.clone(),
        };
        legs.push(Self::pending_leg_from_order(
            pair.base.clone(),
            side_a,
            target_a,
            Decimal::ZERO,
            ref_price_a,
            false,
            res_a,
        ));

        let res_b = match self
            .create_order_with_post_only_retry(PostOnlyOrderRequest {
                symbol: &pair.quote,
                size: qty_b,
                side: side_b,
                reduce_only: false,
                prices,
                allow_post_only: true,
                max_post_only_attempts: entry_attempts,
                fallback_to_taker: false,
                capital_guard_inst_idx: Some(inst_idx),
            })
            .await
        {
            Ok(res) => res,
            Err(e) => {
                self.recover_from_leg_b_failure(pair, &res_a_response, side_a, &e)
                    .await;
                return Err(PartialOrderPlacementError::new(legs.clone(), e).into());
            }
        };
        let target_b = if res_b.response.ordered_size > Decimal::ZERO {
            if res_b.response.ordered_size != qtys.1 {
                log::debug!(
                    "[ORDER_PARAMS][ENTRY] size adjusted by exchange for {}: requested={} ordered={}",
                    pair.quote,
                    qtys.1,
                    res_b.response.ordered_size
                );
            }
            res_b.response.ordered_size
        } else {
            qtys.1
        };
        legs.push(Self::pending_leg_from_order(
            pair.quote.clone(),
            side_b,
            target_b,
            Decimal::ZERO,
            ref_price_b,
            false,
            res_b,
        ));
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
                    .is_some_and(|id| order_id == id)
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
        if qty_a > Decimal::ZERO
            && preflight_reduce_only
            && self.confirm_reduce_only_position_missing(&pair.base).await
        {
            log::info!(
                "[ORDER] {} reduce-only close skipped (preflight); position already closed",
                pair.base
            );
            skipped_already_closed = true;
        }
        if qty_a > Decimal::ZERO && !skipped_already_closed {
            let res = if use_market {
                let meta = self.order_submit_metadata(&pair.base, qty_a, side_a, prices);
                self.connector
                    .create_order(&pair.base, qty_a, side_a, None, None, true, None)
                    .await
                    .map(|response| Self::order_result_from_response(response, false, None, meta))
            } else {
                self.create_order_with_post_only_retry(PostOnlyOrderRequest {
                    symbol: &pair.base,
                    size: qty_a,
                    side: side_a,
                    reduce_only: true,
                    prices,
                    allow_post_only: true,
                    max_post_only_attempts: POST_ONLY_EXIT_ATTEMPTS,
                    fallback_to_taker: true,
                    capital_guard_inst_idx: None,
                })
                .await
            };
            match res {
                Ok(res) => {
                    if res.response.ordered_size > Decimal::ZERO
                        && res.response.ordered_size != qty_a
                    {
                        log::debug!(
                            "[ORDER_PARAMS][EXIT] size adjusted by exchange for {}: requested={} ordered={}",
                            pair.base,
                            qty_a,
                            res.response.ordered_size
                        );
                    }
                    let res_a_response = dex_connector::CreateOrderResponse {
                        order_id: res.response.order_id.clone(),
                        exchange_order_id: res.response.exchange_order_id.clone(),
                        ordered_price: res.response.ordered_price,
                        ordered_size: res.response.ordered_size,
                        client_order_id: res.response.client_order_id.clone(),
                    };
                    legs.push(Self::pending_leg_from_order(
                        pair.base.clone(),
                        side_a,
                        qty_a,
                        Decimal::ZERO,
                        ref_price_a,
                        true,
                        res,
                    ));
                    res_a = Some(res_a_response);
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
        if qty_b > Decimal::ZERO
            && preflight_reduce_only
            && self.confirm_reduce_only_position_missing(&pair.quote).await
        {
            log::info!(
                "[ORDER] {} reduce-only close skipped (preflight); position already closed",
                pair.quote
            );
            quote_already_flat = true;
            skipped_already_closed = true;
        }
        if qty_b > Decimal::ZERO && !quote_already_flat {
            let res_b = if use_market {
                let meta = self.order_submit_metadata(&pair.quote, qty_b, side_b, prices);
                self.connector
                    .create_order(&pair.quote, qty_b, side_b, None, None, true, None)
                    .await
                    .map(|response| Self::order_result_from_response(response, false, None, meta))
            } else {
                self.create_order_with_post_only_retry(PostOnlyOrderRequest {
                    symbol: &pair.quote,
                    size: qty_b,
                    side: side_b,
                    reduce_only: true,
                    prices,
                    allow_post_only: true,
                    max_post_only_attempts: POST_ONLY_EXIT_ATTEMPTS,
                    fallback_to_taker: true,
                    capital_guard_inst_idx: None,
                })
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
                            self.recover_from_leg_b_failure(pair, res_a, side_a, &e)
                                .await;
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
                if res_b.response.ordered_size > Decimal::ZERO
                    && res_b.response.ordered_size != qty_b
                {
                    log::debug!(
                        "[ORDER_PARAMS][EXIT] size adjusted by exchange for {}: requested={} ordered={}",
                        pair.quote,
                        qty_b,
                        res_b.response.ordered_size
                    );
                }
                legs.push(Self::pending_leg_from_order(
                    pair.quote.clone(),
                    side_b,
                    qty_b,
                    Decimal::ZERO,
                    ref_price_b,
                    true,
                    res_b,
                ));
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
        // (fee_bps=0) takes the use_market / non-post-only path above, so no
        // leg is post-only and `takeover_at` stays None — Frankfurt behavior
        // is unchanged regardless of the configured timeout. Prior to #408
        // this was a synchronous in-step monitor that blocked `step()` for
        // the full timeout and caused STEP_OVERRUN warns.
        let exit_timeout = self.cfg.default_pair_params.exit_post_only_timeout_secs;
        let exit_post_only = legs.iter().any(|leg| leg.post_only);
        let takeover_at = if exit_post_only && exit_timeout > 0 {
            Some(Instant::now() + Duration::from_secs(exit_timeout))
        } else {
            None
        };

        Ok((legs, takeover_at))
    }

    pub(in crate::pairtrade) fn latch_capital_position_activity(&mut self, inst_idx: usize) {
        // dry_run never creates venue exposure, so its simulated positions
        // cannot produce a delayed account-equity settlement on restart.
        if self.cfg.dry_run {
            return;
        }

        let inst = &mut self.instances[inst_idx];
        let was_seen = std::mem::replace(&mut inst.capital_position_seen_since_baseline, true);
        inst.flat_since = None;
        if !was_seen {
            self.persist_risk_state();
        }
    }

    /// Snapshot of the fields `latch_capital_position_activity` mutates,
    /// taken immediately before calling it so a definitively-failed
    /// single-shot attempt can be rolled back precisely rather than
    /// guessed at afterwards (Codex P2 follow-up, bot-strategy#783).
    fn capital_guard_prior_state(&self, inst_idx: usize) -> CapitalGuardPriorState {
        let inst = &self.instances[inst_idx];
        CapitalGuardPriorState {
            inst_idx,
            was_seen: inst.capital_position_seen_since_baseline,
            flat_since: inst.flat_since,
        }
    }

    /// Undo the optimistic latch `latch_capital_position_activity`
    /// performed before the first create_order attempt of one logical
    /// placement operation (a single shot, or a post-only retry loop plus
    /// optional taker fallback), but only when every one of these holds:
    /// - this operation is the one that flipped the guard false-to-true
    ///   (an already-latched guard from a genuine earlier fill this
    ///   session must survive untouched);
    /// - `definitively_no_order_created` is true, meaning *every* attempt
    ///   this operation made returned `DexError::ServerResponse` -- a
    ///   completed HTTP round trip carrying an explicit non-2xx rejection
    ///   (e.g. insufficient balance). Every other variant either means a
    ///   given attempt's outcome is unknown (`Transient`) or is not
    ///   returned by `create_order` at all. A single ambiguous attempt
    ///   anywhere in the sequence must keep the whole operation's guard
    ///   latched, since that one attempt alone could have created real
    ///   exposure even if every other attempt was definitively rejected.
    fn unlatch_capital_guard_if_no_order_was_ever_created(
        &mut self,
        prior: Option<CapitalGuardPriorState>,
        definitively_no_order_created: bool,
    ) {
        if self.cfg.dry_run {
            return;
        }
        let Some(prior) = prior else {
            return;
        };
        if prior.was_seen || !definitively_no_order_created {
            return;
        }
        let inst = &mut self.instances[prior.inst_idx];
        inst.capital_position_seen_since_baseline = false;
        inst.flat_since = prior.flat_since;
        self.persist_risk_state();
    }

    pub(in crate::pairtrade) fn register_partial_leg_failure(
        &mut self,
        inst_idx: usize,
        key: &str,
        direction: PositionDirection,
        placed_ts_ms: i64,
        err: &anyhow::Error,
        is_exit: bool,
    ) {
        let mut registered_live_entry = false;
        if let Some(partial) = err.downcast_ref::<PartialOrderPlacementError>() {
            if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                let has_placed_leg = !partial.legs().is_empty();
                let pending = PendingOrders {
                    legs: partial.legs().to_vec(),
                    direction,
                    placed_at: Instant::now(),
                    placed_ts_ms,
                    hedge_retry_count: 0,
                    post_only_hybrid: false,
                    // Partial-failure recovery — let the next reconcile tick
                    // decide normal-timeout behavior; no fast takeover.
                    exit_taker_takeover_at: None,
                }
                .with_leg_decision_ts();
                if is_exit {
                    state.pending_exit = Some(pending);
                } else {
                    state.pending_entry = Some(pending);
                    registered_live_entry = has_placed_leg;
                }
            }
        }

        // A partial entry has already created venue exposure even though the
        // fallible two-leg placement returns Err. Persist the capital guard
        // before that error reaches the caller so an immediate process exit
        // followed by startup force-close cannot turn the unaccounted close
        // settlement into a false capital event. bot-strategy#783.
        if registered_live_entry {
            self.latch_capital_position_activity(inst_idx);
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
    use rust_decimal::Decimal;

    use super::super::super::state::{PendingLeg, PositionDirection};
    use super::PairTradeEngine;

    fn dec(v: &str) -> Decimal {
        v.parse().unwrap()
    }

    #[test]
    fn settled_leg_preserves_original_execution_metadata() {
        let original = PendingLeg {
            symbol: "BTC".to_string(),
            order_id: "ord-1".to_string(),
            exchange_order_id: Some("ex-1".to_string()),
            target: dec("1.0"),
            filled: dec("0.25"),
            side: OrderSide::Short,
            submitted_qty: Decimal::ZERO,
            limit_price: Some(dec("101.25")),
            reference_price: Some(dec("100.80")),
            submit_ts_ms: 0,
            ack_ts_ms: None,
            decision_ts_ms: 0,
            submit_reference_price: None,
            submit_mid: None,
            submit_bid: None,
            submit_ask: None,
            client_order_id: None,
            reduce_only: false,
            post_only: true,
        };

        let settled = PairTradeEngine::settled_leg(&original, dec("0.25"));

        assert_eq!(settled.symbol, original.symbol);
        assert_eq!(settled.order_id, original.order_id);
        assert_eq!(settled.exchange_order_id, original.exchange_order_id);
        assert_eq!(settled.target, dec("0.25"));
        assert_eq!(settled.filled, dec("0.25"));
        assert_eq!(settled.side, original.side);
        assert_eq!(settled.limit_price, original.limit_price);
        assert_eq!(settled.reference_price, original.reference_price);
        assert_eq!(settled.post_only, original.post_only);
    }

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
