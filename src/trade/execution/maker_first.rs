//! `MakerFirst` execution of an [`ExecIntent`] (bot-strategy#1099 Step 2b).
//!
//! Rest a post-only order at the touch, follow the touch with re-quotes,
//! then take whatever is left with one bounded IOC when the maker window
//! ends. The lifecycle is driven against [`OrderVenue`], a deliberately
//! small view of a venue, so it can be tested without a real connector.
//! [`DexVenue`] adapts any `DexConnector` to it.
//!
//! **The venue position is the ground truth for how much has filled.**
//! Fill records are eventually consistent: they can trail an order leaving
//! the book, and an IOC that met several counterparties can surface one
//! trade at a time. Before any new send (re-quote, taker remainder) and
//! before an IOC counts as settled, the booked fills must agree with the
//! position change since the start. Otherwise the executor waits, and if
//! they still disagree it stops with the work **unresolved** (Codex on
//! pairtrade#374). This assumes the executor is the only thing moving this
//! account's position in the symbol while it runs: a dedicated MM
//! sub-account, or a book runtime that owns the symbol.
//!
//! **An order is over only on positive evidence**: it is absent from the
//! open orders AND either listed among the cancelled orders or filled to its
//! full size. One missing id in a cached open-orders view is not proof of
//! absence (a WS cache can be empty during a reconnect, cf. bull_holder).
//! Until there is evidence nothing new is sent, and without evidence
//! within the confirm polls the order is unresolved (Codex on #374).
//!
//! Safety rules (#1099 design invariants):
//! - Fills are harvested by order id after every cancel, so a fill that
//!   lands while a cancel is in flight is still booked.
//! - A send whose error does not prove it never reached the venue (anything
//!   but a local refusal, a 429, an explicit venue rejection, or a permanent
//!   error) is **unresolved**: the order may be live with an unknown id.
//! - Once anything is unresolved, nothing more is sent (no re-quote, no
//!   taker), and the caller reconciles before trading the symbol again.
//! - The taker remainder needs a guaranteed bound (`max_slip_bps = Some(b)`):
//!   it is sent at the absolute limit `reference · (1 ± b)`, and only while
//!   the current touch is within that bound.
//! - An IOC whose fill never shows in agreeing records and position is
//!   unresolved, because a zero-fill IOC cannot be told apart from a delayed
//!   record.
//! - Never arms a dead-man switch (account-wide on Lighter, stops included).
//! - Fees the venue does not report stay `None`.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{bail, Result};
use async_trait::async_trait;
use dex_connector::{DexConnector, DexError, OrderSide};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use super::intent::{
    slippage_bps, ExecIntent, ExecOutcome, ExecStyle, FillRow, PriceSource, Role, Side,
};

/// One fill as the venue reports it, keyed by `trade_id`.
#[derive(Debug, Clone, PartialEq)]
pub struct VenueFill {
    pub order_id: String,
    pub trade_id: String,
    pub qty: f64,
    pub price: f64,
    pub fee_usd: Option<f64>,
}

/// The few venue operations the executor needs.
#[async_trait]
pub trait OrderVenue: Send + Sync {
    /// Best bid and best ask.
    async fn touch(&self, symbol: &str) -> Result<(f64, f64), DexError>;
    /// Signed position (base units; long > 0).
    async fn position(&self, symbol: &str) -> Result<f64, DexError>;
    /// A post-only limit order; returns its order id.
    async fn place_post_only(
        &self,
        symbol: &str,
        side: Side,
        qty: f64,
        price: f64,
        reduce_only: bool,
    ) -> Result<String, DexError>;
    async fn cancel(&self, symbol: &str, order_id: &str) -> Result<(), DexError>;
    async fn open_order_ids(&self, symbol: &str) -> Result<HashSet<String>, DexError>;
    /// Ids the venue reports as cancelled (positive terminal evidence).
    async fn canceled_order_ids(&self, symbol: &str) -> Result<HashSet<String>, DexError>;
    async fn fills(&self, symbol: &str) -> Result<Vec<VenueFill>, DexError>;
    /// An IOC at an absolute limit; returns its order id.
    async fn place_ioc(
        &self,
        symbol: &str,
        side: Side,
        qty: f64,
        limit: f64,
        reduce_only: bool,
    ) -> Result<String, DexError>;
}

/// Whether a failed send provably never reached the venue (safe to retry or
/// to skip). Anything else may have been accepted with an id we never saw.
pub fn send_definitely_not_placed(e: &DexError) -> bool {
    matches!(
        e,
        DexError::InvalidInput { .. }
            | DexError::RateLimited { .. }
            | DexError::ServerResponse(_)
            | DexError::Permanent(_)
            | DexError::ApiKeyRegistrationRequired
            | DexError::UpcomingMaintenance
    )
}

/// Polling cadence and bounded waits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MakerFirstTiming {
    pub poll: Duration,
    /// Polls after a cancel before an order still listed open is unresolved.
    pub cancel_confirm_polls: u32,
    /// Polls for fill records and position to agree before a new send.
    pub settle_polls: u32,
    /// Polls for an IOC's fills to settle.
    pub ioc_fill_polls: u32,
}

impl Default for MakerFirstTiming {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(500),
            cancel_confirm_polls: 10,
            settle_polls: 10,
            ioc_fill_polls: 10,
        }
    }
}

pub struct MakerFirstExecutor<'a, V: OrderVenue + ?Sized> {
    pub venue: &'a V,
    pub timing: MakerFirstTiming,
}

struct Book {
    rows: Vec<FillRow>,
    seen: HashSet<String>,
    filled: f64,
}

impl Book {
    fn harvest(&mut self, intent: &ExecIntent, fills: &[VenueFill], ids: &HashMap<String, Role>) {
        for f in fills {
            let Some(role) = ids.get(&f.order_id) else {
                continue;
            };
            if !self.seen.insert(f.trade_id.clone()) {
                continue;
            }
            self.filled += f.qty;
            self.rows.push(FillRow {
                order_id: Some(f.order_id.clone()),
                role: *role,
                price: f.price,
                price_source: PriceSource::Venue,
                qty: f.qty,
                fee_usd: f.fee_usd,
                arrival_mid: intent.reference_price,
                slippage_bps: slippage_bps(intent.side, f.price, intent.reference_price),
            });
        }
    }
}

/// Per-run state.
struct Run<'i> {
    intent: &'i ExecIntent,
    pos0: f64,
    eps: f64,
    ids: HashMap<String, Role>,
    book: Book,
    unresolved: Vec<String>,
    last_pos: Option<f64>,
    /// Size each maker order was placed with.
    order_qty: HashMap<String, f64>,
}

impl Run<'_> {
    fn filled_for(&self, id: &str) -> f64 {
        self.book
            .rows
            .iter()
            .filter(|r| r.order_id.as_deref() == Some(id))
            .map(|r| r.qty)
            .sum()
    }
    fn dir(&self) -> f64 {
        match self.intent.side {
            Side::Buy => 1.0,
            Side::Sell => -1.0,
        }
    }
    /// Quantity filled according to the position.
    fn pos_filled(&self, pos: f64) -> f64 {
        self.dir() * (pos - self.pos0)
    }
    fn agrees(&self, pos: f64) -> bool {
        (self.pos_filled(pos) - self.book.filled).abs() <= self.eps
    }
}

impl<'a, V: OrderVenue + ?Sized> MakerFirstExecutor<'a, V> {
    async fn harvest(&self, run: &mut Run<'_>) {
        match self.venue.fills(&run.intent.symbol).await {
            Ok(fills) => {
                let (intent, ids) = (run.intent, &run.ids);
                run.book.harvest(intent, &fills, ids);
            }
            Err(e) => log::warn!(
                "[MAKER_FIRST] {}: fills unreadable: {e:?}",
                run.intent.symbol
            ),
        }
    }

    async fn read_position(&self, run: &mut Run<'_>) -> Option<f64> {
        match self.venue.position(&run.intent.symbol).await {
            Ok(p) => {
                run.last_pos = Some(p);
                Some(p)
            }
            Err(e) => {
                log::warn!(
                    "[MAKER_FIRST] {}: position unreadable: {e:?}",
                    run.intent.symbol
                );
                None
            }
        }
    }

    /// Wait until the booked fills agree with the position change on two
    /// consecutive polls with nothing changing in between. A single agreeing
    /// sample is not enough: right after an order leaves the book, records
    /// AND position can both still show the baseline (Codex on #374). `false`
    /// = no stable agreement within the wait. A lag on both views longer than
    /// one poll interval is not caught (known limitation, documented).
    async fn settle(&self, run: &mut Run<'_>) -> bool {
        let mut prev: Option<(f64, f64)> = None;
        for i in 0..=self.timing.settle_polls {
            self.harvest(run).await;
            if let Some(p) = self.read_position(run).await {
                let sample = (run.book.filled, p);
                if run.agrees(p) && prev == Some(sample) {
                    return true;
                }
                prev = Some(sample);
            }
            if i < self.timing.settle_polls {
                tokio::time::sleep(self.timing.poll).await;
            }
        }
        false
    }

    /// Positive evidence that `id` is over: absent from the open orders AND
    /// (listed cancelled OR filled to its full size). `None` = a view was
    /// unreadable.
    async fn terminal(&self, run: &mut Run<'_>, id: &str) -> Option<bool> {
        self.harvest(run).await;
        let open = self.venue.open_order_ids(&run.intent.symbol).await.ok()?;
        if open.contains(id) {
            return Some(false);
        }
        let qty = run.order_qty.get(id).copied().unwrap_or(f64::INFINITY);
        if run.filled_for(id) >= qty - run.eps {
            return Some(true);
        }
        let canceled = self
            .venue
            .canceled_order_ids(&run.intent.symbol)
            .await
            .ok()?;
        Some(canceled.contains(id))
    }

    /// Cancel `id` and wait for positive terminal evidence, harvesting
    /// meanwhile. `false` = no evidence within the confirm polls.
    async fn cancel_and_confirm(&self, run: &mut Run<'_>, id: &str) -> bool {
        if let Err(e) = self.venue.cancel(&run.intent.symbol, id).await {
            log::warn!(
                "[MAKER_FIRST] {}: cancel {id} failed: {e:?}",
                run.intent.symbol
            );
        }
        for _ in 0..=self.timing.cancel_confirm_polls {
            if self.terminal(run, id).await == Some(true) {
                return true;
            }
            tokio::time::sleep(self.timing.poll).await;
        }
        false
    }

    /// Order gone → settle, or mark unresolved. `true` = settled.
    async fn after_gone(&self, run: &mut Run<'_>, id: &str) -> bool {
        if self.settle(run).await {
            true
        } else {
            run.unresolved.push(id.to_string());
            false
        }
    }

    pub async fn execute(&self, intent: &ExecIntent) -> Result<ExecOutcome> {
        let ExecStyle::MakerFirst(params) = &intent.style else {
            bail!("MakerFirstExecutor only executes MakerFirst intents");
        };
        if !(intent.qty > 0.0 && intent.reference_price > 0.0) {
            bail!("intent needs a positive qty and reference price");
        }
        // The baseline position must be known before anything is sent.
        let pos0 = self.venue.position(&intent.symbol).await?;
        let mut run = Run {
            intent,
            pos0,
            eps: intent.qty * 1e-9 + 1e-12,
            ids: HashMap::new(),
            book: Book {
                rows: Vec::new(),
                seen: HashSet::new(),
                filled: 0.0,
            },
            unresolved: Vec::new(),
            last_pos: Some(pos0),
            order_qty: HashMap::new(),
        };
        let started = tokio::time::Instant::now();
        let maker_end = started + Duration::from_millis(params.maker_window_ms);
        let mut resting: Option<(String, f64)> = None;
        let mut drifted = false;
        // Consecutive polls the resting order was missing from the open
        // orders without terminal evidence.
        let mut missing_polls: u32 = 0;

        // ---- maker phase
        while tokio::time::Instant::now() < maker_end && run.unresolved.is_empty() {
            self.harvest(&mut run).await;
            // Size from whichever of records / position shows more filled.
            let pos_filled = match self.read_position(&mut run).await {
                Some(p) => run.pos_filled(p),
                None => run.book.filled,
            };
            let remaining = intent.qty - run.book.filled.max(pos_filled);
            if remaining <= run.eps {
                break;
            }
            // Did the resting order leave the book (filled, expired, rejected)?
            if let Some((id, _)) = &resting {
                let id = id.clone();
                let listed = self
                    .venue
                    .open_order_ids(&intent.symbol)
                    .await
                    .map(|open| open.contains(&id))
                    .unwrap_or(true);
                if listed {
                    missing_polls = 0;
                } else if self.terminal(&mut run, &id).await == Some(true) {
                    missing_polls = 0;
                    resting = None;
                    if !self.after_gone(&mut run, &id).await {
                        break;
                    }
                    continue;
                } else {
                    // Missing without evidence: a cache gap, not proof. Send
                    // nothing new; give up after the confirm polls.
                    missing_polls += 1;
                    if missing_polls > self.timing.cancel_confirm_polls {
                        run.unresolved.push(id);
                        resting = None;
                        break;
                    }
                    tokio::time::sleep(self.timing.poll).await;
                    continue;
                }
            }
            let (bid, ask) = match self.venue.touch(&intent.symbol).await {
                Ok(t) if t.0 > 0.0 && t.1 > 0.0 => t,
                _ => {
                    tokio::time::sleep(self.timing.poll).await;
                    continue;
                }
            };
            let target = match intent.side {
                Side::Buy => bid,
                Side::Sell => ask,
            };
            if let Some(bound) = intent.max_slip_bps {
                if slippage_bps(intent.side, target, intent.reference_price) > bound {
                    drifted = true;
                    break;
                }
            }
            match &resting {
                Some((id, px)) => {
                    let moved = (target - px).abs() / px * 1e4;
                    if moved >= params.requote_bps {
                        let id = id.clone();
                        resting = None;
                        if !self.cancel_and_confirm(&mut run, &id).await {
                            run.unresolved.push(id);
                            break;
                        }
                        if !self.after_gone(&mut run, &id).await {
                            break;
                        }
                        continue;
                    }
                }
                None => match self
                    .venue
                    .place_post_only(
                        &intent.symbol,
                        intent.side,
                        remaining,
                        target,
                        intent.reduce_only,
                    )
                    .await
                {
                    Ok(id) => {
                        run.ids.insert(id.clone(), Role::Maker);
                        run.order_qty.insert(id.clone(), remaining);
                        resting = Some((id, target));
                    }
                    // Provably not placed (post-only reject, 429, ...): retry.
                    Err(e) if send_definitely_not_placed(&e) => log::info!(
                        "[MAKER_FIRST] {}: post-only not placed: {e:?}",
                        intent.symbol
                    ),
                    // May be live with an id we never saw: stop.
                    Err(e) => {
                        log::warn!(
                            "[MAKER_FIRST] {}: post-only send ambiguous, stopping: {e:?}",
                            intent.symbol
                        );
                        run.unresolved
                            .push(format!("post_only:{}:ambiguous", intent.symbol));
                        break;
                    }
                },
            }
            tokio::time::sleep(self.timing.poll).await;
        }

        // ---- leave the book
        if let Some((id, _)) = resting.take() {
            if !self.cancel_and_confirm(&mut run, &id).await {
                run.unresolved.push(id);
            } else {
                self.after_gone(&mut run, &id).await;
            }
        }
        if run.unresolved.is_empty() && !self.settle(&mut run).await {
            run.unresolved
                .push(format!("fills_unsettled:{}", intent.symbol));
        }

        // ---- taker remainder
        let remaining = intent.qty - run.book.filled;
        if remaining > run.eps && run.unresolved.is_empty() && !drifted {
            if let Some(bound) = intent.max_slip_bps {
                self.take_remainder(&mut run, remaining, bound).await;
            }
        }

        Ok(ExecOutcome {
            filled_qty: run.book.filled,
            fills: run.book.rows,
            position_after: run.last_pos,
            unresolved: run.unresolved,
        })
    }

    async fn take_remainder(&self, run: &mut Run<'_>, remaining: f64, bound: f64) {
        let intent = run.intent;
        // The book may have moved during the cancel/settle waits: an IOC that
        // cannot be marketable inside the bound is not sent at all.
        let crossing = match self.venue.touch(&intent.symbol).await {
            Ok((bid, ask)) if bid > 0.0 && ask > 0.0 => match intent.side {
                Side::Buy => ask,
                Side::Sell => bid,
            },
            _ => {
                log::warn!(
                    "[MAKER_FIRST] {}: no touch for the taker remainder",
                    intent.symbol
                );
                return;
            }
        };
        if slippage_bps(intent.side, crossing, intent.reference_price) > bound {
            log::info!(
                "[MAKER_FIRST] {}: touch {crossing} is outside the {bound} bp bound; remainder left unfilled",
                intent.symbol
            );
            return;
        }
        let b = bound / 1e4;
        let limit = match intent.side {
            Side::Buy => intent.reference_price * (1.0 + b),
            Side::Sell => intent.reference_price * (1.0 - b),
        };
        let id = match self
            .venue
            .place_ioc(
                &intent.symbol,
                intent.side,
                remaining,
                limit,
                intent.reduce_only,
            )
            .await
        {
            Ok(id) => id,
            Err(e) if send_definitely_not_placed(&e) => {
                log::warn!(
                    "[MAKER_FIRST] {}: taker remainder not sent: {e:?}",
                    intent.symbol
                );
                return;
            }
            Err(e) => {
                log::warn!("[MAKER_FIRST] {}: IOC send ambiguous: {e:?}", intent.symbol);
                run.unresolved
                    .push(format!("ioc:{}:ambiguous", intent.symbol));
                return;
            }
        };
        run.ids.insert(id.clone(), Role::Taker);
        let before = run.book.filled;
        // Settled = some fill booked for it, records agreeing with the
        // position, and the position unchanged since the previous poll (later
        // slices of a multi-counterparty IOC still arriving otherwise).
        let mut prev: Option<f64> = None;
        for _ in 0..=self.timing.ioc_fill_polls {
            self.harvest(run).await;
            if let Some(p) = self.read_position(run).await {
                if run.book.filled > before + run.eps && run.agrees(p) && prev == Some(p) {
                    return;
                }
                prev = Some(p);
            }
            tokio::time::sleep(self.timing.poll).await;
        }
        run.unresolved.push(id);
    }
}

// ------------------------------------------------------------- DexVenue

/// [`OrderVenue`] over any `DexConnector`.
pub struct DexVenue<'a> {
    pub dex: &'a dyn DexConnector,
}

fn dec(v: f64, what: &str) -> Result<Decimal, DexError> {
    // `from_f64`, never `from_f64_retain`: the latter keeps the binary
    // expansion and a tick price can round a whole tick away (pairtrade#315).
    Decimal::from_f64(v).ok_or_else(|| DexError::InvalidInput {
        field: what.to_string(),
        value: v.to_string(),
    })
}

fn order_side(side: Side) -> OrderSide {
    match side {
        Side::Buy => OrderSide::Long,
        Side::Sell => OrderSide::Short,
    }
}

#[async_trait]
impl<'a> OrderVenue for DexVenue<'a> {
    async fn touch(&self, symbol: &str) -> Result<(f64, f64), DexError> {
        let ob = self.dex.get_order_book(symbol, 1).await?;
        let bid = ob
            .bids
            .first()
            .and_then(|l| l.price.to_f64())
            .unwrap_or(0.0);
        let ask = ob
            .asks
            .first()
            .and_then(|l| l.price.to_f64())
            .unwrap_or(0.0);
        Ok((bid, ask))
    }

    async fn position(&self, symbol: &str) -> Result<f64, DexError> {
        let positions = self.dex.get_positions().await?;
        Ok(positions
            .iter()
            .find(|p| p.symbol == symbol)
            .and_then(|p| {
                p.size
                    .abs()
                    .to_f64()
                    .map(|s| s * f64::from(p.sign.signum()))
            })
            .unwrap_or(0.0))
    }

    async fn place_post_only(
        &self,
        symbol: &str,
        side: Side,
        qty: f64,
        price: f64,
        reduce_only: bool,
    ) -> Result<String, DexError> {
        let r = self
            .dex
            .create_order(
                symbol,
                dec(qty, "qty")?,
                order_side(side),
                Some(dec(price, "price")?),
                Some(-2), // post-only
                reduce_only,
                None,
            )
            .await?;
        Ok(r.order_id)
    }

    async fn cancel(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        self.dex.cancel_order(symbol, order_id).await
    }

    async fn open_order_ids(&self, symbol: &str) -> Result<HashSet<String>, DexError> {
        Ok(self
            .dex
            .get_open_orders(symbol)
            .await?
            .orders
            .into_iter()
            .map(|o| o.order_id)
            .collect())
    }

    async fn canceled_order_ids(&self, symbol: &str) -> Result<HashSet<String>, DexError> {
        Ok(self
            .dex
            .get_canceled_orders(symbol)
            .await?
            .orders
            .into_iter()
            .map(|o| o.order_id)
            .collect())
    }

    async fn fills(&self, symbol: &str) -> Result<Vec<VenueFill>, DexError> {
        Ok(self
            .dex
            .get_filled_orders(symbol)
            .await?
            .orders
            .into_iter()
            .filter(|f| !f.is_rejected)
            .filter_map(|f| {
                let qty = f.filled_size?.to_f64()?;
                let value = f.filled_value?.to_f64()?;
                if qty <= 0.0 {
                    return None;
                }
                Some(VenueFill {
                    order_id: f.order_id,
                    trade_id: f.trade_id,
                    qty,
                    price: value / qty,
                    fee_usd: f.filled_fee.and_then(|d| d.to_f64()),
                })
            })
            .collect())
    }

    async fn place_ioc(
        &self,
        symbol: &str,
        side: Side,
        qty: f64,
        limit: f64,
        reduce_only: bool,
    ) -> Result<String, DexError> {
        let r = self
            .dex
            .create_order_taker_ioc_at(
                symbol,
                dec(qty, "qty")?,
                order_side(side),
                dec(limit, "limit")?,
                reduce_only,
            )
            .await?;
        Ok(r.order_id)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::trade::execution::intent::MakerFirstParams;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    struct MockOrder {
        qty: f64,
        filled: f64,
        price: f64,
    }

    #[derive(Default)]
    struct MockState {
        touches: VecDeque<(f64, f64)>,
        last_touch: (f64, f64),
        orders: HashMap<String, MockOrder>,
        open: HashSet<String>,
        fills: Vec<VenueFill>,
        next_id: u64,
        next_trade: u64,
        /// Maker qty filled per `fills()` call on each resting order.
        maker_fill_per_poll: f64,
        reject_post_only: u32,
        rate_limit_post_only: u32,
        /// A fill of this qty lands on the order as it is being cancelled.
        fill_on_cancel: Option<f64>,
        /// Cancels are acknowledged but the order stays listed open.
        cancel_noop: bool,
        ioc_fills: bool,
        ioc_hidden: bool,
        /// The IOC fills in two slices, the second visible later.
        ioc_two_slices: bool,
        /// Fill records become visible this many `fills()` calls after the
        /// fill (the position moves at once).
        record_delay: usize,
        pending: Vec<(usize, VenueFill)>,
        fills_calls: usize,
        position: f64,
        canceled: HashSet<String>,
        /// The open-orders view drops live orders (a cache gap): they are
        /// neither cancelled nor filled.
        open_omits_live: bool,
        /// The position view shows a fill this many `position()` calls late.
        position_delay: usize,
        position_pending: Vec<(usize, f64)>,
        position_calls: usize,
        /// Error to return from the next post-only / IOC send.
        post_only_err: Option<DexError>,
        ioc_err: Option<DexError>,
        placements: Vec<(String, f64, f64)>,
        cancels: Vec<String>,
        iocs: Vec<(f64, f64)>,
    }

    struct MockVenue(Mutex<MockState>);

    impl MockVenue {
        fn new(touch: (f64, f64)) -> Self {
            Self(Mutex::new(MockState {
                last_touch: touch,
                ..Default::default()
            }))
        }
        fn with(self, f: impl FnOnce(&mut MockState)) -> Self {
            f(&mut self.0.lock().unwrap());
            self
        }
        fn s(&self) -> std::sync::MutexGuard<'_, MockState> {
            self.0.lock().unwrap()
        }
    }

    /// A fill (all test intents buy): the position moves now, the record
    /// shows after `record_delay` calls of `fills()`.
    fn push_fill(st: &mut MockState, id: &str, qty: f64, price: f64) {
        push_fill_after(st, id, qty, price, st.record_delay);
    }

    fn push_fill_after(st: &mut MockState, id: &str, qty: f64, price: f64, delay: usize) {
        st.next_trade += 1;
        let trade_id = format!("t{}", st.next_trade);
        if st.position_delay == 0 {
            st.position += qty;
        } else {
            let due = st.position_calls + st.position_delay;
            st.position_pending.push((due, qty));
        }
        let due = st.fills_calls + delay;
        st.pending.push((
            due,
            VenueFill {
                order_id: id.to_string(),
                trade_id,
                qty,
                price,
                fee_usd: Some(0.0),
            },
        ));
    }

    #[async_trait]
    impl OrderVenue for MockVenue {
        async fn position(&self, _: &str) -> Result<f64, DexError> {
            let mut st = self.s();
            st.position_calls += 1;
            let now = st.position_calls;
            let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut st.position_pending)
                .into_iter()
                .partition(|(d, _)| *d <= now);
            st.position_pending = later;
            st.position += due.into_iter().map(|(_, q)| q).sum::<f64>();
            Ok(st.position)
        }
        async fn touch(&self, _: &str) -> Result<(f64, f64), DexError> {
            let mut st = self.s();
            if let Some(t) = st.touches.pop_front() {
                st.last_touch = t;
            }
            Ok(st.last_touch)
        }
        async fn place_post_only(
            &self,
            _: &str,
            _: Side,
            qty: f64,
            price: f64,
            _: bool,
        ) -> Result<String, DexError> {
            let mut st = self.s();
            if let Some(e) = st.post_only_err.take() {
                return Err(e);
            }
            if st.rate_limit_post_only > 0 {
                st.rate_limit_post_only -= 1;
                return Err(DexError::RateLimited { until_unix: 0 });
            }
            if st.reject_post_only > 0 {
                st.reject_post_only -= 1;
                return Err(DexError::ServerResponse("post-only would cross".into()));
            }
            st.next_id += 1;
            let id = format!("m{}", st.next_id);
            st.orders.insert(
                id.clone(),
                MockOrder {
                    qty,
                    filled: 0.0,
                    price,
                },
            );
            st.open.insert(id.clone());
            st.placements.push((id.clone(), qty, price));
            Ok(id)
        }
        async fn cancel(&self, _: &str, order_id: &str) -> Result<(), DexError> {
            let mut st = self.s();
            st.cancels.push(order_id.to_string());
            if let Some(q) = st.fill_on_cancel.take() {
                let o = st.orders.get_mut(order_id).unwrap();
                let q = q.min(o.qty - o.filled);
                o.filled += q;
                let px = o.price;
                push_fill(&mut st, order_id, q, px);
            }
            // Like a venue: cancelling an order that already left the book
            // (fully filled) leaves no cancel record.
            if !st.cancel_noop && st.open.remove(order_id) {
                st.canceled.insert(order_id.to_string());
            }
            Ok(())
        }
        async fn open_order_ids(&self, _: &str) -> Result<HashSet<String>, DexError> {
            let st = self.s();
            if st.open_omits_live {
                return Ok(HashSet::new());
            }
            Ok(st.open.clone())
        }
        async fn canceled_order_ids(&self, _: &str) -> Result<HashSet<String>, DexError> {
            Ok(self.s().canceled.clone())
        }
        async fn fills(&self, _: &str) -> Result<Vec<VenueFill>, DexError> {
            let mut st = self.s();
            let per = st.maker_fill_per_poll;
            if per > 0.0 {
                let open: Vec<String> = st.open.iter().cloned().collect();
                for id in open {
                    let o = st.orders.get_mut(&id).unwrap();
                    let q = per.min(o.qty - o.filled);
                    if q <= 0.0 {
                        continue;
                    }
                    o.filled += q;
                    let (px, done) = (o.price, o.filled >= o.qty - 1e-12);
                    push_fill(&mut st, &id, q, px);
                    if done {
                        st.open.remove(&id);
                    }
                }
            }
            st.fills_calls += 1;
            let now = st.fills_calls;
            let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut st.pending)
                .into_iter()
                .partition(|(d, _)| *d <= now);
            st.pending = later;
            st.fills.extend(due.into_iter().map(|(_, f)| f));
            Ok(st.fills.clone())
        }
        async fn place_ioc(
            &self,
            _: &str,
            _: Side,
            qty: f64,
            limit: f64,
            _: bool,
        ) -> Result<String, DexError> {
            let mut st = self.s();
            if let Some(e) = st.ioc_err.take() {
                return Err(e);
            }
            st.next_id += 1;
            let id = format!("i{}", st.next_id);
            st.iocs.push((qty, limit));
            if st.ioc_fills {
                let px = st.last_touch.1.min(limit); // buy at the ask, capped
                if st.ioc_hidden {
                    // The position moves; the record never shows.
                    push_fill_after(&mut st, &id, qty, px, usize::MAX / 2);
                } else if st.ioc_two_slices {
                    push_fill_after(&mut st, &id, qty / 2.0, px, 0);
                    push_fill_after(&mut st, &id, qty / 2.0, px, 3);
                } else {
                    push_fill(&mut st, &id, qty, px);
                }
            }
            Ok(id)
        }
    }

    fn timing() -> MakerFirstTiming {
        MakerFirstTiming {
            poll: Duration::from_millis(100),
            cancel_confirm_polls: 3,
            settle_polls: 5,
            ioc_fill_polls: 8,
        }
    }

    fn buy(qty: f64, bound: Option<f64>, window_ms: u64) -> ExecIntent {
        ExecIntent {
            symbol: "BTC".into(),
            side: Side::Buy,
            qty,
            reference_price: 100.0,
            reduce_only: false,
            deadline_ms: None,
            max_slip_bps: bound,
            style: ExecStyle::MakerFirst(MakerFirstParams {
                maker_window_ms: window_ms,
                requote_bps: 5.0,
            }),
        }
    }

    async fn run(v: &MockVenue, i: &ExecIntent) -> ExecOutcome {
        MakerFirstExecutor {
            venue: v,
            timing: timing(),
        }
        .execute(i)
        .await
        .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_maker_fill_needs_no_taker() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| s.maker_fill_per_poll = 1.0);
        let o = run(&v, &buy(1.0, Some(50.0), 1_000)).await;
        assert!((o.filled_qty - 1.0).abs() < 1e-12);
        assert!(o.fills.iter().all(|f| f.role == Role::Maker));
        assert_eq!(v.s().placements[0].2, 99.9, "joins the bid");
        assert!(v.s().iocs.is_empty());
        assert!(o.unresolved.is_empty());
        // Maker at the bid is a negative cost vs the mid reference.
        assert!(o.fills[0].slippage_bps < 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_partial_fill_then_the_deadline_takes_the_rest_within_the_bound() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.maker_fill_per_poll = 0.2;
            s.ioc_fills = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 250)).await;
        assert!((o.filled_qty - 1.0).abs() < 1e-9, "{}", o.filled_qty);
        let maker: f64 = o
            .fills
            .iter()
            .filter(|f| f.role == Role::Maker)
            .map(|f| f.qty)
            .sum();
        let taker: f64 = o
            .fills
            .iter()
            .filter(|f| f.role == Role::Taker)
            .map(|f| f.qty)
            .sum();
        assert!(maker > 0.0 && taker > 0.0);
        let (ioc_qty, ioc_limit) = v.s().iocs[0];
        assert!(
            (ioc_qty - (1.0 - maker)).abs() < 1e-9,
            "only the remainder goes taker"
        );
        assert!((ioc_limit - 100.5).abs() < 1e-9, "absolute limit ref·(1+b)");
        assert!(o.unresolved.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn post_only_rejects_and_rate_limits_are_retried() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.reject_post_only = 2;
            s.rate_limit_post_only = 1;
            s.maker_fill_per_poll = 1.0;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 2_000)).await;
        assert!((o.filled_qty - 1.0).abs() < 1e-12);
        assert_eq!(v.s().placements.len(), 1);
        assert!(v.s().iocs.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_fill_during_the_cancel_is_booked_and_shrinks_the_taker() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.fill_on_cancel = Some(0.4);
            s.ioc_fills = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 250)).await;
        let maker: f64 = o
            .fills
            .iter()
            .filter(|f| f.role == Role::Maker)
            .map(|f| f.qty)
            .sum();
        assert!(
            (maker - 0.4).abs() < 1e-12,
            "the in-flight fill is booked as maker"
        );
        assert!(
            (v.s().iocs[0].0 - 0.6).abs() < 1e-12,
            "taker only for what is left"
        );
        assert!((o.filled_qty - 1.0).abs() < 1e-12);
    }

    #[tokio::test(start_paused = true)]
    async fn an_unconfirmed_cancel_is_unresolved_and_blocks_the_taker() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.cancel_noop = true;
            s.ioc_fills = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 250)).await;
        assert_eq!(o.unresolved, vec!["m1".to_string()]);
        assert!(
            v.s().iocs.is_empty(),
            "nothing more is sent while an order is unknown"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_touch_move_requotes_by_cancel_then_place() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            // first quote at 99.9, then the bid moves +10 bp to 100.0
            s.touches = VecDeque::from(vec![(99.9, 100.1), (100.0, 100.2)]);
            s.ioc_fills = true;
        });
        let _ = run(&v, &buy(1.0, Some(50.0), 450)).await;
        let st = v.s();
        assert!(st.placements.len() >= 2, "{:?}", st.placements);
        assert_eq!(st.placements[0].2, 99.9);
        assert_eq!(st.placements[1].2, 100.0);
        assert_eq!(
            st.cancels[0], st.placements[0].0,
            "the old quote is cancelled first"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_adverse_move_beyond_the_bound_sends_no_taker() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.touches = VecDeque::from(vec![(99.9, 100.1), (101.0, 101.2)]);
            s.ioc_fills = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 1_000)).await;
        assert!(v.s().iocs.is_empty());
        assert!(o.filled_qty < 1.0);
        assert!(o.unresolved.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_guaranteed_bound_there_is_no_taker_remainder() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| s.ioc_fills = true);
        let o = run(&v, &buy(1.0, None, 250)).await;
        assert!(v.s().iocs.is_empty());
        assert_eq!(o.filled_qty, 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_ioc_whose_fill_never_shows_is_unresolved() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.ioc_fills = true;
            s.ioc_hidden = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 250)).await;
        assert_eq!(v.s().iocs.len(), 1);
        assert_eq!(o.unresolved.len(), 1);
        assert!(o.unresolved[0].starts_with('i'));
    }

    #[tokio::test(start_paused = true)]
    async fn a_taker_intent_is_refused() {
        let v = MockVenue::new((99.9, 100.1));
        let mut i = buy(1.0, Some(50.0), 250);
        i.style = ExecStyle::Taker;
        assert!(MakerFirstExecutor {
            venue: &v,
            timing: timing()
        }
        .execute(&i)
        .await
        .is_err());
    }

    /// Codex on #374: a fill whose record trails the order leaving the book
    /// must be waited for before the remainder is sized, or the IOC overfills.
    #[tokio::test(start_paused = true)]
    async fn a_delayed_fill_record_is_awaited_before_sizing_the_taker() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.fill_on_cancel = Some(0.4);
            s.record_delay = 3;
            s.ioc_fills = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 250)).await;
        assert!(
            (v.s().iocs[0].0 - 0.6).abs() < 1e-12,
            "IOC sized after the record landed"
        );
        assert!((o.filled_qty - 1.0).abs() < 1e-12, "{}", o.filled_qty);
        assert!(o.unresolved.is_empty());
        assert!((o.position_after.unwrap() - 1.0).abs() < 1e-12);
    }

    #[tokio::test(start_paused = true)]
    async fn records_that_never_catch_up_leave_it_unresolved_with_no_taker() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.fill_on_cancel = Some(0.4);
            s.record_delay = 1_000;
            s.ioc_fills = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 250)).await;
        assert!(
            v.s().iocs.is_empty(),
            "no send while the position disagrees with the records"
        );
        assert!(!o.unresolved.is_empty());
    }

    /// Codex on #374: an IOC that fills in slices is settled only when every
    /// slice is booked and agrees with the position.
    #[tokio::test(start_paused = true)]
    async fn a_multi_slice_ioc_is_booked_in_full() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.ioc_fills = true;
            s.ioc_two_slices = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 250)).await;
        let taker: f64 = o
            .fills
            .iter()
            .filter(|f| f.role == Role::Taker)
            .map(|f| f.qty)
            .sum();
        assert!((taker - 1.0).abs() < 1e-12, "both slices: {taker}");
        assert!(o.unresolved.is_empty());
    }

    /// Codex on #374: a send error that does not prove the order never
    /// reached the venue stops everything as unresolved.
    #[tokio::test(start_paused = true)]
    async fn an_ambiguous_post_only_error_stops_without_retry_or_taker() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.post_only_err = Some(DexError::Transient("timeout after send".into()));
            s.ioc_fills = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 1_000)).await;
        assert!(v.s().placements.is_empty(), "no retry");
        assert!(v.s().iocs.is_empty(), "no taker");
        assert_eq!(o.unresolved, vec!["post_only:BTC:ambiguous".to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_ambiguous_ioc_error_is_unresolved_not_a_clean_zero_fill() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.ioc_err = Some(DexError::Transient("connection reset".into()));
        });
        let o = run(&v, &buy(1.0, Some(50.0), 250)).await;
        assert_eq!(o.unresolved, vec!["ioc:BTC:ambiguous".to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_definite_ioc_rejection_is_not_unresolved() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.ioc_err = Some(DexError::ServerResponse("rejected".into()));
        });
        let o = run(&v, &buy(1.0, Some(50.0), 250)).await;
        assert!(o.unresolved.is_empty());
        assert_eq!(o.filled_qty, 0.0);
    }

    /// Codex on #374: with no maker window (or a move during the final
    /// cancel) the touch is re-read before the IOC; outside the bound it is
    /// not sent, and nothing is left unresolved.
    #[tokio::test(start_paused = true)]
    async fn the_touch_is_rechecked_before_the_taker_remainder() {
        let v = MockVenue::new((101.0, 101.2)).with(|s| s.ioc_fills = true);
        let o = run(&v, &buy(1.0, Some(50.0), 0)).await;
        assert!(v.s().iocs.is_empty());
        assert!(o.unresolved.is_empty());
        assert_eq!(o.filled_qty, 0.0);
    }

    #[test]
    fn only_provable_non_sends_are_retryable() {
        assert!(send_definitely_not_placed(&DexError::RateLimited {
            until_unix: 0
        }));
        assert!(send_definitely_not_placed(&DexError::ServerResponse(
            "x".into()
        )));
        assert!(send_definitely_not_placed(&DexError::InvalidInput {
            field: "f".into(),
            value: "v".into()
        }));
        assert!(!send_definitely_not_placed(&DexError::Transient(
            "x".into()
        )));
        assert!(!send_definitely_not_placed(&DexError::NoConnection));
    }

    /// Codex on #374 (round 2): both views can still show the baseline when
    /// the order leaves the book. One agreeing (0, 0) sample must not size
    /// the remainder.
    #[tokio::test(start_paused = true)]
    async fn a_baseline_sample_on_both_lagging_views_does_not_settle() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.fill_on_cancel = Some(0.4);
            s.position_delay = 2;
            s.record_delay = 4;
            s.ioc_fills = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 250)).await;
        assert!((v.s().iocs[0].0 - 0.6).abs() < 1e-12, "{:?}", v.s().iocs);
        assert!((o.filled_qty - 1.0).abs() < 1e-12, "{}", o.filled_qty);
    }

    /// `settle` alone: with both views still at the baseline on the first
    /// poll, it must not report agreement until the fill shows in both.
    #[tokio::test(start_paused = true)]
    async fn settle_waits_past_a_first_baseline_sample() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.position_delay = 2;
            s.record_delay = 3;
        });
        let i = buy(1.0, Some(50.0), 250);
        let ex = MakerFirstExecutor {
            venue: &v,
            timing: timing(),
        };
        let mut run = Run {
            intent: &i,
            pos0: 0.0,
            eps: 1e-9,
            ids: HashMap::from([("m1".to_string(), Role::Maker)]),
            book: Book {
                rows: Vec::new(),
                seen: HashSet::new(),
                filled: 0.0,
            },
            unresolved: Vec::new(),
            last_pos: Some(0.0),
            order_qty: HashMap::new(),
        };
        push_fill(&mut v.s(), "m1", 0.4, 99.9);
        assert!(ex.settle(&mut run).await);
        assert!(
            (run.book.filled - 0.4).abs() < 1e-12,
            "settled on the stale baseline"
        );
    }

    /// Codex on #374 (round 3): a live order missing from a cached
    /// open-orders view is not over. Without cancel/fill evidence nothing new
    /// is sent, and the order ends unresolved.
    #[tokio::test(start_paused = true)]
    async fn a_live_order_missing_from_the_cache_is_not_treated_as_gone() {
        let v = MockVenue::new((99.9, 100.1)).with(|s| {
            s.open_omits_live = true;
            s.ioc_fills = true;
        });
        let o = run(&v, &buy(1.0, Some(50.0), 2_000)).await;
        assert_eq!(
            v.s().placements.len(),
            1,
            "no second maker while m1 may be live"
        );
        assert!(v.s().iocs.is_empty(), "no taker while m1 may be live");
        assert_eq!(o.unresolved, vec!["m1".to_string()]);
    }
}
