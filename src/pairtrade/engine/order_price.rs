//! Order-pricing helpers for `PairTradeEngine`: reference-price selection
//! (book side vs ticker), slippage application, tick/size quantization, and
//! the post-only passive-price enforcement. Moved out of `pairtrade/mod.rs`
//! per bot-strategy#502 (pure relocation, no behavior change).

use std::collections::HashMap;

use anyhow::{Context, Result};
use rust_decimal::Decimal;

use super::super::market::SymbolSnapshot;
use super::super::order_pricing;
use super::super::util::{enforce_post_only_passive, round_price_by_tick};
use super::super::PairTradeEngine;

pub(in crate::pairtrade) struct RefreshedLimitPrice {
    pub(in crate::pairtrade) limit: Decimal,
    pub(in crate::pairtrade) submit_snapshot: Option<SymbolSnapshot>,
}

impl PairTradeEngine {
    pub(in crate::pairtrade) fn post_only_supported(&self) -> bool {
        let dex = self.cfg.dex_name.to_ascii_lowercase();
        dex.contains("lighter") || dex.contains("extended") || dex.contains("hyperliquid")
    }

    pub(in crate::pairtrade) fn should_post_only(&self) -> bool {
        self.cfg.fee_bps > 0.0 && self.post_only_supported()
    }

    /// Exit-side post-only gate. Fee-bearing venues retain their existing
    /// behavior; zero-fee venues must opt in explicitly so #860 cannot change
    /// entry placement or any unrelated deployment by default.
    pub(in crate::pairtrade) fn should_post_only_exit(&self) -> bool {
        self.post_only_supported()
            && (self.cfg.fee_bps > 0.0 || self.cfg.default_pair_params.exit_post_only_enabled)
    }

    pub(in crate::pairtrade) fn order_reference_price_from_snapshot(
        &self,
        symbol: &str,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> Decimal {
        self.order_reference_price_from_snapshot_for_mode(
            symbol,
            side,
            snapshot,
            self.should_post_only(),
        )
    }

    pub(in crate::pairtrade) fn order_reference_price_from_snapshot_for_mode(
        &self,
        symbol: &str,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
        post_only: bool,
    ) -> Decimal {
        let use_book = self.cfg.slippage_bps < 0 || post_only;
        if use_book {
            let side_price = match side {
                dex_connector::OrderSide::Long => snapshot.ask_price,
                dex_connector::OrderSide::Short => snapshot.bid_price,
            };
            if side_price.is_none() {
                log::debug!(
                    "[ORDER] {} missing top-of-book price; using ticker price",
                    symbol
                );
            }
            return side_price.unwrap_or(snapshot.price);
        }
        snapshot.price
    }

    pub(in crate::pairtrade) fn order_reference_price(
        &self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        let snapshot = prices.get(symbol)?;
        Some(self.order_reference_price_from_snapshot(symbol, side, snapshot))
    }

    pub(in crate::pairtrade) fn order_reference_price_for_mode(
        &self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
        post_only: bool,
    ) -> Option<Decimal> {
        let snapshot = prices.get(symbol)?;
        Some(self.order_reference_price_from_snapshot_for_mode(symbol, side, snapshot, post_only))
    }

    pub(in crate::pairtrade) fn limit_price_for(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        self.limit_price_for_mode(symbol, side, prices, self.should_post_only())
    }

    pub(in crate::pairtrade) fn limit_price_for_mode(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
        post_only: bool,
    ) -> Option<Decimal> {
        let snapshot = prices.get(symbol)?;
        let reference =
            self.order_reference_price_from_snapshot_for_mode(symbol, side, snapshot, post_only);
        let adjusted = self.apply_slippage(Some(reference), side)?;
        Some(self.quantize_order_price_with_snapshot_for_mode(
            symbol, adjusted, side, snapshot, post_only,
        ))
    }

    pub(in crate::pairtrade) fn limit_price_for_snapshot_for_mode(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
        post_only: bool,
    ) -> Option<Decimal> {
        let reference =
            self.order_reference_price_from_snapshot_for_mode(symbol, side, snapshot, post_only);
        let adjusted = self.apply_slippage(Some(reference), side)?;
        Some(self.quantize_order_price_with_snapshot_for_mode(
            symbol, adjusted, side, snapshot, post_only,
        ))
    }

    pub(in crate::pairtrade) async fn refreshed_limit_price(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<RefreshedLimitPrice> {
        self.refreshed_limit_price_for_mode(symbol, side, prices, self.should_post_only())
            .await
    }

    pub(in crate::pairtrade) async fn refreshed_limit_price_for_mode(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
        post_only: bool,
    ) -> Option<RefreshedLimitPrice> {
        match self.refresh_symbol_snapshot(symbol).await {
            Ok(snapshot) => self
                .limit_price_for_snapshot_for_mode(symbol, side, &snapshot, post_only)
                .map(|limit| RefreshedLimitPrice {
                    limit,
                    submit_snapshot: Some(snapshot),
                }),
            Err(err) => {
                log::debug!(
                    "[ORDER] Failed to refresh price snapshot for {}: {:?}",
                    symbol,
                    err
                );
                self.limit_price_for_mode(symbol, side, prices, post_only)
                    .map(|limit| RefreshedLimitPrice {
                        limit,
                        submit_snapshot: None,
                    })
            }
        }
    }

    pub(in crate::pairtrade) async fn refresh_symbol_snapshot(
        &mut self,
        symbol: &str,
    ) -> Result<SymbolSnapshot> {
        let ticker = self
            .connector
            .get_ticker(symbol, None)
            .await
            .with_context(|| format!("ticker {}", symbol))?;
        let (bid_price, ask_price, bid_size, ask_size) =
            match self.connector.get_order_book(symbol, 1).await {
                Ok(ob) => (
                    ob.bids.first().map(|l| l.price),
                    ob.asks.first().map(|l| l.price),
                    ob.bids.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                    ob.asks.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                ),
                Err(err) => {
                    log::debug!(
                        "[ORDER] orderbook {} unavailable during retry: {:?}",
                        symbol,
                        err
                    );
                    (None, None, Decimal::ZERO, Decimal::ZERO)
                }
            };
        Ok(SymbolSnapshot {
            price: ticker.price,
            funding_rate: ticker.funding_rate.unwrap_or(Decimal::ZERO),
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            min_order: ticker.min_order,
            min_tick: ticker.min_tick,
            size_decimals: ticker.size_decimals,
            exchange_ts: ticker.exchange_ts.map(|v| v as i64),
        })
    }

    pub(in crate::pairtrade) fn order_spread_param(
        &self,
        limit: Option<Decimal>,
        allow_post_only: bool,
    ) -> Option<i64> {
        if allow_post_only && limit.is_some() {
            Some(-2)
        } else {
            None
        }
    }

    pub(in crate::pairtrade) fn apply_slippage(
        &self,
        price: Option<Decimal>,
        side: dex_connector::OrderSide,
    ) -> Option<Decimal> {
        order_pricing::apply_slippage(self.cfg.slippage_bps, price, side)
    }

    pub(in crate::pairtrade) fn quantize_order_size(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        order_pricing::quantize_order_size(symbol, size, prices)
    }

    pub(in crate::pairtrade) fn quantize_order_size_exit(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        order_pricing::quantize_order_size_exit(symbol, size, prices)
    }

    pub(in crate::pairtrade) fn quantize_order_size_close(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        order_pricing::quantize_order_size_close(symbol, size, prices)
    }

    pub(in crate::pairtrade) fn quantize_order_price_with_snapshot_for_mode(
        &mut self,
        symbol: &str,
        price: Decimal,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
        post_only: bool,
    ) -> Decimal {
        let mut effective_tick_size = snapshot.min_tick;

        // Extended occasionally returns markets without `min_tick` populated
        // in the snapshot (dex-connector fills this from the markets cache,
        // which may lag a reconnect). Fall back to tick=1 so we don't spam
        // the "No min tick" warning every cycle.
        if effective_tick_size.is_none() && self.cfg.dex_name.contains("extended") {
            effective_tick_size = Some(Decimal::ONE);
        }

        let Some(tick_size) = effective_tick_size else {
            if !self.min_tick_warned.contains(symbol) {
                log::warn!(
                    "[ORDER] No min tick for {}; price rounding disabled",
                    symbol
                );

                self.min_tick_warned.insert(symbol.to_string());
            }

            return price;
        };

        if tick_size <= Decimal::ZERO {
            return price;
        }

        let rounded = round_price_by_tick(price, tick_size, side);

        // bot-strategy#216: tick rounding is a no-op when the touch price is
        // already a tick multiple (Extended BTC tick=1 with integer prices),
        // so post-only limits land at touch and get rejected/crossed.
        if post_only {
            let touch = match side {
                dex_connector::OrderSide::Long => snapshot.ask_price,
                dex_connector::OrderSide::Short => snapshot.bid_price,
            };
            if let Some(touch) = touch {
                return enforce_post_only_passive(rounded, touch, tick_size, side);
            }
        }

        rounded
    }
}
