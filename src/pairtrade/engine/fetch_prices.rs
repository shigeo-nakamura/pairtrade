//! Market-snapshot fetch for `PairTradeEngine`: the per-tick parallel
//! ticker + order-book sweep (`fetch_latest_prices`) and its rate-limited
//! warning helpers. Moved out of `pairtrade/mod.rs` per bot-strategy#502
//! (pure relocation, no behavior change).

use std::collections::HashMap;
use std::time::Instant;

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use tokio::time::Duration;

use super::super::engine;
use super::super::market::SymbolSnapshot;
use super::super::PairTradeEngine;

impl PairTradeEngine {
    pub(in crate::pairtrade) fn should_log_ob_warn(&self, symbol: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_ob_warn
            .get(symbol)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    pub(in crate::pairtrade) fn should_log_ticker_warn(&self, symbol: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_ticker_warn
            .get(symbol)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    pub(in crate::pairtrade) async fn fetch_latest_prices(
        &mut self,
    ) -> Result<HashMap<String, SymbolSnapshot>> {
        let symbols: Vec<String> = self
            .cfg
            .universe
            .iter()
            .flat_map(|p| [p.base.clone(), p.quote.clone()])
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let connector = self.connector.clone();
        let mut join_set = tokio::task::JoinSet::new();
        for sym in symbols.iter().cloned() {
            let conn = connector.clone();
            join_set.spawn(async move {
                let (ticker_res, ob_res) =
                    tokio::join!(conn.get_ticker(&sym, None), conn.get_order_book(&sym, 1),);
                (sym, ticker_res, ob_res)
            });
        }
        let mut results = Vec::new();
        while let Some(res) = join_set.join_next().await {
            results.push(res.expect("fetch task panicked"));
        }
        // Sort by symbol so any [TICKER] / [ORDERBOOK] warning emit order
        // is deterministic across runs — JoinSet completion order is
        // tokio-scheduler dependent and previously caused intermittent
        // golden_baseline drift.
        results.sort_by(|a, b| a.0.cmp(&b.0));

        let mut map = HashMap::new();
        for (symbol, ticker_res, ob_res) in results {
            let ticker = match ticker_res {
                Ok(ticker) => ticker,
                Err(e) => {
                    let msg = e.to_string();
                    if engine::error_class::is_ticker_auth_error(&msg) {
                        if self.should_log_ticker_warn(&symbol) {
                            log::warn!("ticker {} unavailable: {}", symbol, msg);
                            self.last_ticker_warn.insert(symbol.clone(), Instant::now());
                        } else {
                            log::debug!("ticker {} unavailable: {}", symbol, msg);
                        }
                        continue;
                    }
                    if engine::error_class::is_ticker_rate_limited(&e, &msg) {
                        if self.should_log_ticker_warn(&symbol) {
                            log::warn!("ticker {} rate-limited (cooling down): {}", symbol, msg);
                            self.last_ticker_warn.insert(symbol.clone(), Instant::now());
                        } else {
                            log::debug!("ticker {} rate-limited (cooling down): {}", symbol, msg);
                        }
                        continue;
                    }
                    return Err(e).with_context(|| format!("ticker {}", symbol));
                }
            };
            let (top_bid_price, top_ask_price, top_bid_size, top_ask_size) = match ob_res {
                Ok(ob) => (
                    ob.bids.first().map(|l| l.price),
                    ob.asks.first().map(|l| l.price),
                    ob.bids.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                    ob.asks.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                ),
                Err(e) => {
                    let msg = format!("{:?}", e);
                    let is_stale = msg.contains("order book snapshot unavailable");
                    if is_stale {
                        log::debug!("orderbook {} unavailable: {}", symbol, msg);
                    } else if self.should_log_ob_warn(&symbol) {
                        log::warn!("orderbook {} unavailable: {}", symbol, msg);
                        self.last_ob_warn.insert(symbol.clone(), Instant::now());
                    } else {
                        log::debug!("orderbook {} unavailable: {}", symbol, msg);
                    }
                    (None, None, Decimal::ZERO, Decimal::ZERO)
                }
            };
            if ticker.min_order.is_none() && !self.min_order_warned.contains(&symbol) {
                let size_decimals_desc = ticker
                    .size_decimals
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "none".into());
                log::warn!(
                    "[TICKER] {} missing min_order (size_decimals={}); using fallback step",
                    symbol,
                    size_decimals_desc
                );
                self.min_order_warned.insert(symbol.clone());
            }
            if ticker.min_tick.is_none() && !self.min_tick_warned.contains(&symbol) {
                let min_tick_desc = ticker
                    .min_tick
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "none".into());
                log::warn!(
                    "[TICKER] {} missing min_tick (ticker reports {}); price will be rounded with fallback",
                    symbol,
                    min_tick_desc
                );
                self.min_tick_warned.insert(symbol.clone());
            }
            map.insert(
                symbol.clone(),
                SymbolSnapshot {
                    price: ticker.price,
                    funding_rate: ticker.funding_rate.unwrap_or(Decimal::ZERO),
                    bid_price: top_bid_price,
                    ask_price: top_ask_price,
                    bid_size: top_bid_size,
                    ask_size: top_ask_size,
                    min_order: ticker.min_order,
                    min_tick: ticker.min_tick,
                    size_decimals: ticker.size_decimals,
                    exchange_ts: ticker.exchange_ts.map(|v| v as i64),
                },
            );
            log::debug!(
                "[PRICE_SNAPSHOT] {} price={} bid={:?} ask={:?} bid_sz={} ask_sz={} min_order={:?} min_tick={:?}",
                symbol,
                ticker.price,
                top_bid_price,
                top_ask_price,
                top_bid_size,
                top_ask_size,
                ticker.min_order,
                ticker.min_tick
            );
        }
        Ok(map)
    }
}
