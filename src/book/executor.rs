//! Venue access behind one trait so DRY_RUN, live and replay share every
//! other code path (`docs/book-runtime.md` §6).
//!
//! - [`PaperExecutor`]: in-memory positions, fills at `mid * (1 ± slippage)`,
//!   prices pushed in by the caller (WS feed or replay bars).
//! - [`LiveExecutor`]: `DexConnector`-backed. Orders go out as
//!   `create_order(price=None)` (venue-native market/IOC with protection
//!   price); the fill is what the venue position says it is, never the
//!   HTTP acknowledgement (bot-strategy#875 G-2).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use dex_connector::{DexConnector, OrderSide, PositionSnapshot};
use rust_decimal::prelude::ToPrimitive;

use rust_decimal::Decimal;
use serde::Serialize;
use tokio::sync::{Mutex, RwLock};

use super::rebalance::{LotMeta, OrderIntent, Side};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Default)]
pub struct VenuePosition {
    /// Signed base quantity.
    pub qty: f64,
    pub entry_price: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FillReport {
    pub requested_qty: f64,
    /// Absolute filled quantity as confirmed by the venue position (paper:
    /// always the request).
    pub filled_qty: f64,
    pub fill_price: f64,
    /// `paper` | `venue_fills` | `mid_estimate`
    pub fill_price_source: &'static str,
    /// Venue fee for this fill. `None` when the fill was confirmed from
    /// the position delta but no matching fill record could be read (a
    /// lost acknowledgement, or an eventually-consistent fills endpoint):
    /// the cost is real but unknown, and booking zero would silently
    /// understate cumulative fees and PnL.
    pub fee_usd: Option<f64>,
    pub order_id: Option<String>,
    pub venue_error: Option<String>,
    pub latency_ms: i64,
    /// Venue position after the order (signed), when readable.
    pub position_after: Option<f64>,
}

#[async_trait]
pub trait Executor: Send + Sync {
    fn is_paper(&self) -> bool;
    /// Latest mid per symbol; symbols without a price are absent.
    async fn prices(&self, symbols: &[String]) -> HashMap<String, f64>;
    async fn lot_meta(&self, symbol: &str) -> Result<LotMeta>;
    async fn positions(&self) -> Result<BTreeMap<String, VenuePosition>>;
    /// Venue equity in USD; `None` for paper (the engine derives it).
    async fn equity(&self) -> Result<Option<f64>>;
    /// Current funding rate per hour (fraction of notional, positive =
    /// longs pay), when the venue reports one.
    async fn funding_rate_hourly(&self, symbol: &str) -> Option<f64>;
    async fn execute(&self, intent: &OrderIntent) -> Result<FillReport>;
}

// ---------------------------------------------------------------- paper

pub struct PaperExecutor {
    /// Mids with the instant each arrived; an entry older than
    /// `WS_PRICE_MAX_AGE_SECS` is treated as absent rather than served
    /// stale, matching `LiveExecutor::price_for`. Paper has no ticker
    /// fallback, so a quiet feed simply drops the price instead of
    /// serving one indefinitely -- a long-running DRY_RUN process would
    /// otherwise keep marking (and filling) a symbol at a price from
    /// hours or days ago during a partial feed outage.
    prices: RwLock<HashMap<String, (f64, Instant)>>,
    lots: RwLock<HashMap<String, LotMeta>>,
    funding: RwLock<HashMap<String, f64>>,
    positions: Mutex<BTreeMap<String, VenuePosition>>,
    slippage_bps: f64,
    fee_bps: f64,
    /// False for replay: a bar date is a simulated instant, not wall-clock
    /// time, so a slow runner (large universe, loaded machine) taking real
    /// seconds per bar must not expire prices that are perfectly fresh for
    /// the date being replayed -- that would violate the documented
    /// byte-identical replay guarantee. Only the long-running live DRY_RUN
    /// loop, where `Instant::now()` really does track wall-clock staleness
    /// of the WS feed, enables this.
    expire_stale_mids: bool,
}

impl PaperExecutor {
    pub fn new(slippage_bps: f64, fee_bps: f64) -> Self {
        Self::with_expiry(slippage_bps, fee_bps, true)
    }

    /// For replay: never treats a mid as stale by wall-clock age (see
    /// `expire_stale_mids`).
    pub fn new_for_replay(slippage_bps: f64, fee_bps: f64) -> Self {
        Self::with_expiry(slippage_bps, fee_bps, false)
    }

    fn with_expiry(slippage_bps: f64, fee_bps: f64, expire_stale_mids: bool) -> Self {
        Self {
            prices: RwLock::new(HashMap::new()),
            lots: RwLock::new(HashMap::new()),
            funding: RwLock::new(HashMap::new()),
            positions: Mutex::new(BTreeMap::new()),
            slippage_bps,
            fee_bps,
            expire_stale_mids,
        }
    }

    pub async fn set_price(&self, symbol: &str, mid: f64) {
        if mid.is_finite() && mid > 0.0 {
            self.prices
                .write()
                .await
                .insert(symbol.to_string(), (mid, Instant::now()));
        }
    }

    /// Current mid for `symbol` if a fresh-enough observation exists.
    async fn fresh_price(&self, symbol: &str) -> Option<f64> {
        let (px, at) = *self.prices.read().await.get(symbol)?;
        if !self.expire_stale_mids || at.elapsed().as_secs() < WS_PRICE_MAX_AGE_SECS {
            Some(px)
        } else {
            log::warn!(
                "[PRICE] paper mid for {symbol} is {}s old; treating as absent",
                at.elapsed().as_secs()
            );
            None
        }
    }

    pub async fn set_lot(&self, symbol: &str, lot: LotMeta) {
        self.lots.write().await.insert(symbol.to_string(), lot);
    }

    pub async fn set_funding_rate_hourly(&self, symbol: &str, rate: f64) {
        self.funding.write().await.insert(symbol.to_string(), rate);
    }

    /// Drop a prior funding-rate observation (a failed or empty refresh):
    /// `funding_rate_hourly` must go back to reporting unavailable rather
    /// than keep serving the last known rate indefinitely.
    pub async fn clear_funding_rate_hourly(&self, symbol: &str) {
        self.funding.write().await.remove(symbol);
    }

    /// Drop every price and funding observation (replay: before each bar
    /// date, so a symbol missing from a date is missing, not stale).
    pub async fn clear_observations(&self) {
        self.prices.write().await.clear();
        self.funding.write().await.clear();
    }

    /// Seed the paper book (restart: from `state.json`).
    pub async fn seed_positions(&self, positions: BTreeMap<String, VenuePosition>) {
        *self.positions.lock().await = positions;
    }
}

#[async_trait]
impl Executor for PaperExecutor {
    fn is_paper(&self) -> bool {
        true
    }

    async fn prices(&self, symbols: &[String]) -> HashMap<String, f64> {
        let mut out = HashMap::with_capacity(symbols.len());
        for s in symbols {
            if let Some(px) = self.fresh_price(s).await {
                out.insert(s.clone(), px);
            }
        }
        out
    }

    async fn lot_meta(&self, symbol: &str) -> Result<LotMeta> {
        self.lots
            .read()
            .await
            .get(symbol)
            .copied()
            .ok_or_else(|| anyhow!("no lot metadata for {symbol}"))
    }

    async fn positions(&self) -> Result<BTreeMap<String, VenuePosition>> {
        Ok(self.positions.lock().await.clone())
    }

    async fn equity(&self) -> Result<Option<f64>> {
        Ok(None)
    }

    async fn funding_rate_hourly(&self, symbol: &str) -> Option<f64> {
        self.funding.read().await.get(symbol).copied()
    }

    async fn execute(&self, intent: &OrderIntent) -> Result<FillReport> {
        let mid = self.fresh_price(&intent.symbol).await.ok_or_else(|| {
            anyhow!(PreSendAbort(format!(
                "paper: no price for {}",
                intent.symbol
            )))
        })?;
        let slip = self.slippage_bps / 10_000.0;
        let price = match intent.side {
            Side::Buy => mid * (1.0 + slip),
            Side::Sell => mid * (1.0 - slip),
        };
        let signed = match intent.side {
            Side::Buy => intent.qty,
            Side::Sell => -intent.qty,
        };
        let mut book = self.positions.lock().await;
        let entry = book.entry(intent.symbol.clone()).or_default();
        let mut filled = intent.qty;
        if intent.reduce_only {
            // A reduce-only IOC never crosses zero at the venue either.
            let reducible = entry.qty.abs();
            if reducible == 0.0 || (entry.qty > 0.0) != (signed < 0.0) {
                filled = 0.0;
            } else {
                filled = filled.min(reducible);
            }
        }
        let signed_filled = if signed > 0.0 { filled } else { -filled };
        entry.qty += signed_filled;
        if entry.qty.abs() < 1e-12 {
            entry.qty = 0.0;
        }
        if entry.qty == 0.0 {
            book.remove(&intent.symbol);
        } else if entry.entry_price.is_none() || (entry.qty - signed_filled).abs() < 1e-12 {
            entry.entry_price = Some(price);
        }
        let position_after = book.get(&intent.symbol).map(|p| p.qty).or(Some(0.0));
        Ok(FillReport {
            requested_qty: intent.qty,
            filled_qty: filled,
            fill_price: price,
            fill_price_source: "paper",
            fee_usd: Some(filled * price * self.fee_bps / 10_000.0),
            order_id: None,
            venue_error: None,
            // Paper fills report a fixed latency, never the wall clock:
            // a replay writes every FillReport into `ledger.jsonl`, so an
            // OS scheduling pause would otherwise turn a 0 ms fill into a
            // 1 ms one and break the byte-identical guarantee for inputs
            // that never changed.
            latency_ms: 0,
            position_after,
        })
    }
}

// ----------------------------------------------------------------- live

pub struct LiveExecutor {
    connector: Arc<dyn DexConnector + Send + Sync>,
    /// WS mids with the instant each arrived. A symbol whose feed goes
    /// quiet while the socket stays open would otherwise keep serving the
    /// same number to both sizing and the send-time drift guard, so an
    /// entry older than `WS_PRICE_MAX_AGE_SECS` is ignored and the
    /// timestamped ticker fallback is used instead.
    prices: RwLock<HashMap<String, (f64, Instant)>>,
    /// Ticker-sourced prices for symbols the WS feed does not carry (a leg
    /// adopted from the venue outside the configured universe), with the
    /// instant they were fetched; refreshed at most every
    /// `FALLBACK_PRICE_TTL_SECS` so a reduce-only close of such a leg can
    /// always be priced without hammering the REST fallback.
    fallback_prices: RwLock<HashMap<String, (f64, Instant)>>,
    fill_confirm_timeout_secs: i64,
    slippage_bps: u32,
    allow_venue_protection_fallback: bool,
}

const FALLBACK_PRICE_TTL_SECS: u64 = 60;
/// A WS mid older than this is treated as absent (the engine ticks every
/// 5 s, and a live perp feed updates far faster than this bound).
const WS_PRICE_MAX_AGE_SECS: u64 = 30;

/// An intent that never reached the venue: no usable price, or the book
/// moved past the slippage budget between planning and sending. The
/// engine does not count these against a decision's attempt budget, since
/// nothing was submitted and the condition is typically transient.
#[derive(Debug)]
pub struct PreSendAbort(pub String);

impl std::fmt::Display for PreSendAbort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for PreSendAbort {}

/// Whether `mid` is still within `slippage_bps` of the price the intent
/// was sized at, in the adverse direction for `side` (a favourable move
/// never blocks).
pub fn within_slippage(reference: f64, mid: f64, side: Side, slippage_bps: u32) -> bool {
    if !(reference > 0.0 && mid > 0.0) {
        return false;
    }
    let adverse_bps = match side {
        Side::Buy => (mid - reference) / reference * 10_000.0,
        Side::Sell => (reference - mid) / reference * 10_000.0,
    };
    adverse_bps <= slippage_bps as f64
}

impl LiveExecutor {
    pub fn new(
        connector: Arc<dyn DexConnector + Send + Sync>,
        fill_confirm_timeout_secs: i64,
        slippage_bps: u32,
        allow_venue_protection_fallback: bool,
    ) -> Self {
        Self {
            connector,
            prices: RwLock::new(HashMap::new()),
            fallback_prices: RwLock::new(HashMap::new()),
            fill_confirm_timeout_secs,
            slippage_bps,
            allow_venue_protection_fallback,
        }
    }

    /// Send the order with the configured slippage cap: the venue's
    /// price-capped IOC when it has one, otherwise (only if the operator
    /// opted in) the venue-native market/IOC with its own protection price.
    /// `Err((message, submitted))`: `submitted` is false only when the
    /// order provably never reached the venue, which lets the caller
    /// report a `PreSendAbort` instead of confirming a phantom fill.
    async fn send_capped(
        &self,
        intent: &OrderIntent,
        size: Decimal,
        side: OrderSide,
    ) -> Result<dex_connector::CreateOrderResponse, (String, bool)> {
        match self
            .connector
            .create_order_taker_ioc(
                &intent.symbol,
                size,
                side,
                self.slippage_bps,
                intent.reduce_only,
            )
            .await
        {
            Ok(r) => Ok(r),
            Err(dex_connector::DexError::Permanent(msg))
                if msg.to_ascii_lowercase().contains("not implemented")
                    || msg.to_ascii_lowercase().contains("not supported")
                    || msg.to_ascii_lowercase().contains("no native") =>
            {
                if !self.allow_venue_protection_fallback {
                    return Err((
                        format!(
                            "venue has no price-capped IOC ({msg}) and execution.allow_venue_protection_fallback is false; order not sent"
                        ),
                        false,
                    ));
                }
                log::warn!(
                    "[EXEC] {} {}: no price-capped IOC on this venue ({msg}); falling back to create_order(price=None) with the venue protection price (bot-strategy#918)",
                    intent.side,
                    intent.symbol
                );
                self.connector
                    .create_order(
                        &intent.symbol,
                        size,
                        side,
                        None,
                        None,
                        intent.reduce_only,
                        None,
                    )
                    .await
                    .map_err(|e| (format!("{e:?}"), true))
            }
            Err(e) => Err((format!("{e:?}"), true)),
        }
    }

    pub async fn set_price(&self, symbol: &str, mid: f64) {
        if mid.is_finite() && mid > 0.0 {
            self.prices
                .write()
                .await
                .insert(symbol.to_string(), (mid, Instant::now()));
        }
    }

    /// Current price for `symbol`: the WS mid when the feed carries it,
    /// otherwise the ticker fallback (cached `FALLBACK_PRICE_TTL_SECS`).
    /// Used both by the planner (`prices`) and at send time (`execute`) so
    /// an adopted out-of-universe leg can be planned *and* sent.
    async fn price_for(&self, symbol: &str) -> Option<f64> {
        if let Some((px, at)) = self.prices.read().await.get(symbol).copied() {
            if at.elapsed().as_secs() < WS_PRICE_MAX_AGE_SECS {
                return Some(px);
            }
            log::warn!(
                "[PRICE] WS mid for {symbol} is {}s old; falling back to the ticker",
                at.elapsed().as_secs()
            );
        }
        let cached = self.fallback_prices.read().await.get(symbol).copied();
        if let Some((px, at)) = cached {
            if at.elapsed().as_secs() < FALLBACK_PRICE_TTL_SECS {
                return Some(px);
            }
        }
        match self.connector.get_ticker(symbol, None).await {
            Ok(t) => {
                let px = t.price.to_f64().filter(|p| *p > 0.0)?;
                self.fallback_prices
                    .write()
                    .await
                    .insert(symbol.to_string(), (px, Instant::now()));
                Some(px)
            }
            Err(e) => {
                log::warn!("[PRICE] no WS mid for {symbol} and ticker fallback failed: {e:?}");
                None
            }
        }
    }

    async fn venue_position(&self, symbol: &str) -> Result<Option<VenuePosition>> {
        let snaps = self
            .connector
            .get_positions()
            .await
            .map_err(|e| anyhow!("get_positions: {e:?}"))?;
        Ok(position_from_snapshots(&snaps, symbol))
    }
}

pub fn position_from_snapshots(snaps: &[PositionSnapshot], symbol: &str) -> Option<VenuePosition> {
    let s = snaps.iter().find(|p| p.symbol == symbol)?;
    let size = s.size.abs().to_f64().unwrap_or(0.0);
    if size == 0.0 || s.sign == 0 {
        return None;
    }
    let qty = if s.sign < 0 { -size } else { size };
    Some(VenuePosition {
        qty,
        entry_price: s.entry_price.and_then(|p| p.to_f64()).filter(|p| *p > 0.0),
    })
}

fn decimal(v: f64) -> Result<Decimal> {
    Decimal::from_f64_retain(v).ok_or_else(|| anyhow!("{v} is not representable as Decimal"))
}

#[async_trait]
impl Executor for LiveExecutor {
    fn is_paper(&self) -> bool {
        false
    }

    async fn prices(&self, symbols: &[String]) -> HashMap<String, f64> {
        let mut out = HashMap::new();
        for s in symbols {
            if let Some(px) = self.price_for(s).await {
                out.insert(s.clone(), px);
            }
        }
        out
    }

    async fn lot_meta(&self, symbol: &str) -> Result<LotMeta> {
        let t = self
            .connector
            .get_ticker(symbol, None)
            .await
            .map_err(|e| anyhow!("get_ticker {symbol}: {e:?}"))?;
        let size_decimals = t
            .size_decimals
            .with_context(|| format!("venue reports no size_decimals for {symbol}"))?;
        Ok(LotMeta {
            size_decimals,
            min_order_qty: t.min_order.and_then(|m| m.to_f64()).filter(|m| *m > 0.0),
        })
    }

    async fn positions(&self) -> Result<BTreeMap<String, VenuePosition>> {
        let snaps = self
            .connector
            .get_positions()
            .await
            .map_err(|e| anyhow!("get_positions: {e:?}"))?;
        let mut out = BTreeMap::new();
        for s in &snaps {
            if let Some(p) = position_from_snapshots(&snaps, &s.symbol) {
                out.insert(s.symbol.clone(), p);
            }
        }
        Ok(out)
    }

    async fn equity(&self) -> Result<Option<f64>> {
        let b = self
            .connector
            .get_balance(None)
            .await
            .map_err(|e| anyhow!("get_balance: {e:?}"))?;
        Ok(b.equity.to_f64())
    }

    async fn funding_rate_hourly(&self, symbol: &str) -> Option<f64> {
        self.connector
            .get_ticker(symbol, None)
            .await
            .ok()
            .and_then(|t| t.funding_rate)
            .and_then(|r| r.to_f64())
    }

    async fn execute(&self, intent: &OrderIntent) -> Result<FillReport> {
        let started = Instant::now();
        // Drift guard: the plan was sized at `reference_price`; if the book
        // has already moved past the slippage budget the order is not sent
        // (the engine re-plans on the next tick with fresh prices).
        let mid = self.price_for(&intent.symbol).await.ok_or_else(|| {
            anyhow!(PreSendAbort(format!(
                "live: no price for {}",
                intent.symbol
            )))
        })?;
        if !within_slippage(intent.reference_price, mid, intent.side, self.slippage_bps) {
            return Err(anyhow!(PreSendAbort(format!(
                "price moved beyond slippage_bps={} before send (reference={} mid={})",
                self.slippage_bps, intent.reference_price, mid
            ))));
        }
        // The preflight read happens before any submission, so a
        // transient account-read outage here must not spend an attempt.
        let before = self
            .venue_position(&intent.symbol)
            .await
            .map_err(|e| anyhow!(PreSendAbort(format!("preflight position read: {e}"))))?
            .map(|p| p.qty)
            .unwrap_or(0.0);
        let side = match intent.side {
            Side::Buy => OrderSide::Long,
            Side::Sell => OrderSide::Short,
        };
        let size = decimal(intent.qty)?;
        let (order_id, venue_error) = match self.send_capped(intent, size, side).await {
            Ok(r) => (Some(r.order_id), None),
            // Provably never submitted: report it as a pre-send abort so
            // the engine does not spend an attempt confirming a fill that
            // cannot exist.
            Err((msg, false)) => return Err(anyhow!(PreSendAbort(msg))),
            Err((msg, true)) => (None, Some(msg)),
        };
        // Confirm against the venue position regardless of the ack: a send
        // error can still have executed (REST/WS limits are coupled).
        let expected_delta = match intent.side {
            Side::Buy => intent.qty,
            Side::Sell => -intent.qty,
        };
        let deadline = Instant::now()
            + std::time::Duration::from_secs(self.fill_confirm_timeout_secs.max(1) as u64);
        let mut after = before;
        let mut last_read_ok;
        loop {
            match self.venue_position(&intent.symbol).await {
                Ok(p) => {
                    last_read_ok = true;
                    after = p.map(|p| p.qty).unwrap_or(0.0);
                    let delta = after - before;
                    if (delta - expected_delta).abs() <= intent.qty * 1e-6
                        || (expected_delta > 0.0 && delta >= expected_delta)
                        || (expected_delta < 0.0 && delta <= expected_delta)
                    {
                        break;
                    }
                }
                Err(e) => {
                    last_read_ok = false;
                    log::warn!(
                        "[EXEC] position read failed while confirming {}: {e}",
                        intent.symbol
                    );
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }
        if !last_read_ok {
            return Err(anyhow!(
                "fill confirmation for {} unreadable after {}s (order_id={:?}, send_error={:?})",
                intent.symbol,
                self.fill_confirm_timeout_secs,
                order_id,
                venue_error
            ));
        }
        let delta = after - before;
        let same_dir = (delta > 0.0) == (expected_delta > 0.0);
        let filled = if same_dir {
            delta.abs().min(intent.qty)
        } else {
            0.0
        };
        if same_dir && delta.abs() > intent.qty * (1.0 + 1e-6) {
            log::warn!(
                "[EXEC] {} filled more than requested: delta={} requested={}",
                intent.symbol,
                delta,
                intent.qty
            );
        }
        // Fill price: venue fill records when they can be matched, else mid.
        // Fallback price: the mid read right before the send (bounded by
        // the drift guard in the adverse direction; favourable moves are
        // reflected too), not the older planning reference.
        let mut fill_price = mid;
        let mut source = "mid_estimate";
        // `None` until a fill record is actually read: a lost ack or an
        // eventually-consistent fills endpoint leaves the real cost
        // unknown, and booking zero would understate cumulative fees.
        let mut fee_usd: Option<f64> = None;
        if let Some(oid) = order_id.as_deref() {
            if let Ok(f) = self.connector.get_filled_orders(&intent.symbol).await {
                let mut value = 0.0;
                let mut size = 0.0;
                let mut fee_acc = 0.0;
                let mut saw_record = false;
                // One matched fill without a usable fee makes the total
                // unknown: adding it as zero would report `fee_known:
                // true` on a number that is short by that fill's cost.
                let mut all_fees_known = true;
                for o in f
                    .orders
                    .iter()
                    .filter(|o| o.order_id == oid && !o.is_rejected)
                {
                    saw_record = true;
                    let s = o.filled_size.and_then(|d| d.to_f64()).unwrap_or(0.0);
                    let v = o.filled_value.and_then(|d| d.to_f64()).unwrap_or(0.0);
                    match o.filled_fee.and_then(|d| d.to_f64()) {
                        Some(f) => fee_acc += f,
                        None => all_fees_known = false,
                    }
                    if s > 0.0 && v > 0.0 {
                        size += s;
                        value += v;
                    }
                }
                // The fills endpoint is eventually consistent: a matched
                // slice that is missing some already-landed fills still
                // reports its own fees as fully known. Only trust the total
                // once the matched size covers what the position delta
                // confirmed was actually filled; otherwise leave it unknown
                // rather than freeze it short.
                let covers_confirmed = size + intent.qty.abs() * 1e-6 >= filled;
                if saw_record && all_fees_known && covers_confirmed {
                    fee_usd = Some(fee_acc);
                }
                // Same coverage rule as the fee: a partial, eventually
                // consistent slice's VWAP is not the price of the whole
                // confirmed fill if the missing part traded elsewhere, so
                // only trust it once the matched size covers what the
                // position delta confirmed.
                if size > 0.0 && covers_confirmed {
                    fill_price = value / size;
                    source = "venue_fills";
                }
            }
        }
        if filled > 0.0 && fee_usd.is_none() {
            log::warn!(
                "[EXEC] {} filled {} but no fill record was readable (order_id={:?}); its fee is unknown, not zero",
                intent.symbol,
                filled,
                order_id
            );
        }
        Ok(FillReport {
            requested_qty: intent.qty,
            filled_qty: filled,
            fill_price,
            fill_price_source: source,
            fee_usd,
            order_id,
            venue_error,
            latency_ms: started.elapsed().as_millis() as i64,
            position_after: Some(after),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::rebalance::IntentKind;

    fn intent(symbol: &str, side: Side, qty: f64, reduce_only: bool) -> OrderIntent {
        OrderIntent {
            symbol: symbol.into(),
            side,
            qty,
            reference_price: 100.0,
            notional_usd: qty * 100.0,
            reduce_only,
            kind: IntentKind::Open,
        }
    }

    #[tokio::test]
    async fn paper_fills_with_slippage_and_reduce_only_never_crosses_zero() {
        let ex = PaperExecutor::new(10.0, 2.0);
        ex.set_price("SOL", 100.0).await;
        let f = ex
            .execute(&intent("SOL", Side::Buy, 2.0, false))
            .await
            .unwrap();
        assert_eq!(f.filled_qty, 2.0);
        assert!((f.fill_price - 100.1).abs() < 1e-9);
        assert!((f.fee_usd.unwrap() - 2.0 * 100.1 * 0.0002).abs() < 1e-9);
        assert_eq!(f.position_after, Some(2.0));
        // A replay writes the whole report to the ledger, so the paper
        // latency must not be the wall clock.
        assert_eq!(f.latency_ms, 0);
        // reduce-only sell of 5 on a 2 long fills 2
        let f = ex
            .execute(&intent("SOL", Side::Sell, 5.0, true))
            .await
            .unwrap();
        assert_eq!(f.filled_qty, 2.0);
        assert!((f.fill_price - 99.9).abs() < 1e-9);
        assert_eq!(f.position_after, Some(0.0));
        assert!(ex.positions().await.unwrap().is_empty());
        // reduce-only on flat fills nothing
        let f = ex
            .execute(&intent("SOL", Side::Sell, 1.0, true))
            .await
            .unwrap();
        assert_eq!(f.filled_qty, 0.0);
        // no price → error
        assert!(ex
            .execute(&intent("DOT", Side::Buy, 1.0, false))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_stale_paper_mid_is_treated_as_absent_not_served_forever() {
        let ex = PaperExecutor::new(10.0, 2.0);
        ex.set_price("SOL", 100.0).await;
        assert_eq!(
            ex.prices(&["SOL".to_string()]).await.get("SOL").copied(),
            Some(100.0)
        );
        assert!(ex
            .execute(&intent("SOL", Side::Buy, 1.0, false))
            .await
            .is_ok());

        // Back-date the observation past WS_PRICE_MAX_AGE_SECS without a
        // real sleep: a quiet feed during a long-running DRY_RUN process
        // must not keep marking/filling at an arbitrarily old price.
        let stale_at = Instant::now() - std::time::Duration::from_secs(WS_PRICE_MAX_AGE_SECS + 1);
        ex.prices
            .write()
            .await
            .insert("SOL".to_string(), (100.0, stale_at));

        assert!(ex.prices(&["SOL".to_string()]).await.get("SOL").is_none());
        assert!(ex
            .execute(&intent("SOL", Side::Buy, 1.0, false))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_replay_executor_never_expires_a_mid_by_wall_clock_age() {
        let ex = PaperExecutor::new_for_replay(10.0, 2.0);
        ex.set_price("SOL", 100.0).await;
        // Back-date the observation well past WS_PRICE_MAX_AGE_SECS: a
        // slow bar (large universe, loaded runner) taking real seconds
        // must not lose a price that is fresh for the simulated date, or
        // replay stops being byte-identical across machines/speeds.
        let stale_at = Instant::now() - std::time::Duration::from_secs(WS_PRICE_MAX_AGE_SECS + 1);
        ex.prices
            .write()
            .await
            .insert("SOL".to_string(), (100.0, stale_at));
        assert_eq!(
            ex.prices(&["SOL".to_string()]).await.get("SOL").copied(),
            Some(100.0)
        );
        assert!(ex
            .execute(&intent("SOL", Side::Buy, 1.0, false))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn a_failed_funding_refresh_clears_the_prior_rate() {
        let ex = PaperExecutor::new(10.0, 2.0);
        ex.set_funding_rate_hourly("SOL", 0.0001).await;
        assert_eq!(ex.funding_rate_hourly("SOL").await, Some(0.0001));
        ex.clear_funding_rate_hourly("SOL").await;
        assert_eq!(
            ex.funding_rate_hourly("SOL").await,
            None,
            "a cleared rate must not keep charging marks/closes at a stale number"
        );
    }

    #[test]
    fn ws_price_age_bound_is_shorter_than_the_fallback_ttl() {
        // A WS mid must expire before the ticker cache it falls back to,
        // otherwise a quiet feed would keep serving the same number.
        assert!(WS_PRICE_MAX_AGE_SECS < FALLBACK_PRICE_TTL_SECS);
    }

    #[test]
    fn slippage_guard_blocks_adverse_moves_only() {
        assert!(within_slippage(100.0, 100.4, Side::Buy, 50));
        assert!(!within_slippage(100.0, 100.6, Side::Buy, 50));
        assert!(within_slippage(100.0, 99.0, Side::Buy, 50)); // favourable
        assert!(within_slippage(100.0, 99.6, Side::Sell, 50));
        assert!(!within_slippage(100.0, 99.4, Side::Sell, 50));
        assert!(within_slippage(100.0, 101.0, Side::Sell, 50)); // favourable
        assert!(!within_slippage(0.0, 100.0, Side::Buy, 50));
    }

    #[test]
    fn position_from_snapshots_uses_sign_not_size_sign() {
        let snaps = vec![PositionSnapshot {
            symbol: "SNDK".into(),
            size: Decimal::new(15, 1),
            sign: -1,
            entry_price: Some(Decimal::new(1000, 1)),
        }];
        let p = position_from_snapshots(&snaps, "SNDK").unwrap();
        assert_eq!(p.qty, -1.5);
        assert_eq!(p.entry_price, Some(100.0));
        assert!(position_from_snapshots(&snaps, "MU").is_none());
        let flat = vec![PositionSnapshot {
            symbol: "SNDK".into(),
            size: Decimal::ZERO,
            sign: 0,
            entry_price: None,
        }];
        assert!(position_from_snapshots(&flat, "SNDK").is_none());
    }
}
