//! `MakerFirst` execution of an [`ExecIntent`] (bot-strategy#1099 Step 2b).
//!
//! Rest a post-only order at the touch, follow the touch with re-quotes,
//! then take whatever is left with one bounded IOC when the maker window
//! ends. The lifecycle is driven against [`OrderVenue`], a deliberately
//! small view of a venue, so it can be tested without a real connector.
//! [`DexVenue`] adapts any `DexConnector` to it.
//!
//! Safety rules (#1099 design invariants):
//! - A fill that lands while a cancel is in flight is still booked: fills are
//!   harvested by order id after every cancel, not only while resting.
//! - An order whose cancel cannot be confirmed (still listed open after the
//!   confirm polls) is **unresolved**. Nothing more is sent: no re-quote and
//!   no taker remainder. The caller reconciles it before trading the symbol
//!   again.
//! - The taker remainder is sent only with a guaranteed bound
//!   (`max_slip_bps = Some(b)`), at an absolute limit `reference · (1 ± b)`,
//!   and only while the touch is within that bound. Otherwise the remainder
//!   is left unfilled.
//! - An IOC whose fills never become visible is unresolved too: a zero-fill
//!   IOC cannot be told apart from a delayed fill record.
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

/// Polling cadence and bounded waits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MakerFirstTiming {
    pub poll: Duration,
    /// Polls after a cancel before an order still listed open is unresolved.
    pub cancel_confirm_polls: u32,
    /// Polls for an IOC's fills to become visible.
    pub ioc_fill_polls: u32,
}

impl Default for MakerFirstTiming {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(500),
            cancel_confirm_polls: 10,
            ioc_fill_polls: 10,
        }
    }
}

/// Smallest quantity treated as "something left".
const QTY_EPS: f64 = 1e-12;

pub struct MakerFirstExecutor<'a, V: OrderVenue + ?Sized> {
    pub venue: &'a V,
    pub timing: MakerFirstTiming,
}

/// Adverse move of `price` vs `reference` for `side`, in bps.
fn adverse_bps(side: Side, price: f64, reference: f64) -> f64 {
    slippage_bps(side, price, reference)
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

impl<'a, V: OrderVenue + ?Sized> MakerFirstExecutor<'a, V> {
    async fn harvest(&self, intent: &ExecIntent, book: &mut Book, ids: &HashMap<String, Role>) {
        match self.venue.fills(&intent.symbol).await {
            Ok(fills) => book.harvest(intent, &fills, ids),
            Err(e) => log::warn!("[MAKER_FIRST] {}: fills unreadable: {e:?}", intent.symbol),
        }
    }

    /// Cancel `id` and wait until it is no longer listed open, booking any
    /// fill that lands meanwhile. `false` = could not confirm (unresolved).
    async fn cancel_and_confirm(
        &self,
        intent: &ExecIntent,
        id: &str,
        book: &mut Book,
        ids: &HashMap<String, Role>,
    ) -> bool {
        if let Err(e) = self.venue.cancel(&intent.symbol, id).await {
            log::warn!("[MAKER_FIRST] {}: cancel {id} failed: {e:?}", intent.symbol);
        }
        for _ in 0..=self.timing.cancel_confirm_polls {
            self.harvest(intent, book, ids).await;
            match self.venue.open_order_ids(&intent.symbol).await {
                Ok(open) if !open.contains(id) => {
                    // A fill can trail the removal: one more harvest.
                    self.harvest(intent, book, ids).await;
                    return true;
                }
                Ok(_) => {}
                Err(e) => log::warn!(
                    "[MAKER_FIRST] {}: open orders unreadable: {e:?}",
                    intent.symbol
                ),
            }
            tokio::time::sleep(self.timing.poll).await;
        }
        false
    }

    pub async fn execute(&self, intent: &ExecIntent) -> Result<ExecOutcome> {
        let ExecStyle::MakerFirst(params) = &intent.style else {
            bail!("MakerFirstExecutor only executes MakerFirst intents");
        };
        if !(intent.qty > 0.0 && intent.reference_price > 0.0) {
            bail!("intent needs a positive qty and reference price");
        }
        let started = tokio::time::Instant::now();
        let maker_end = started + Duration::from_millis(params.maker_window_ms);
        let mut ids: HashMap<String, Role> = HashMap::new();
        let mut book = Book {
            rows: Vec::new(),
            seen: HashSet::new(),
            filled: 0.0,
        };
        let mut unresolved: Vec<String> = Vec::new();
        let mut resting: Option<(String, f64)> = None;
        let mut drifted = false;

        // ---- maker phase
        while tokio::time::Instant::now() < maker_end {
            self.harvest(intent, &mut book, &ids).await;
            let remaining = intent.qty - book.filled;
            if remaining <= QTY_EPS {
                break;
            }
            // Did the resting order leave the book (filled, expired, rejected)?
            if let Some((id, _)) = &resting {
                if let Ok(open) = self.venue.open_order_ids(&intent.symbol).await {
                    if !open.contains(id) {
                        self.harvest(intent, &mut book, &ids).await;
                        resting = None;
                        continue;
                    }
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
                if adverse_bps(intent.side, target, intent.reference_price) > bound {
                    drifted = true;
                    break;
                }
            }
            match &resting {
                Some((id, px)) => {
                    let moved = (target - px).abs() / px * 1e4;
                    if moved >= params.requote_bps {
                        let id = id.clone();
                        if !self.cancel_and_confirm(intent, &id, &mut book, &ids).await {
                            unresolved.push(id);
                            resting = None;
                            break;
                        }
                        resting = None;
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
                        ids.insert(id.clone(), Role::Maker);
                        resting = Some((id, target));
                    }
                    // Post-only reject, 429, ...: try again next poll.
                    Err(e) => log::info!(
                        "[MAKER_FIRST] {}: post-only not placed: {e:?}",
                        intent.symbol
                    ),
                },
            }
            tokio::time::sleep(self.timing.poll).await;
        }

        // ---- leave the book
        if let Some((id, _)) = resting.take() {
            if !self.cancel_and_confirm(intent, &id, &mut book, &ids).await {
                unresolved.push(id);
            }
        }
        self.harvest(intent, &mut book, &ids).await;

        // ---- taker remainder
        let remaining = intent.qty - book.filled;
        if remaining > QTY_EPS && unresolved.is_empty() && !drifted {
            if let Some(bound) = intent.max_slip_bps {
                let b = bound / 1e4;
                let limit = match intent.side {
                    Side::Buy => intent.reference_price * (1.0 + b),
                    Side::Sell => intent.reference_price * (1.0 - b),
                };
                match self
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
                    Ok(id) => {
                        ids.insert(id.clone(), Role::Taker);
                        let mut seen_fill = false;
                        for _ in 0..=self.timing.ioc_fill_polls {
                            let before = book.rows.len();
                            self.harvest(intent, &mut book, &ids).await;
                            if book.rows[before..]
                                .iter()
                                .any(|r| r.order_id.as_deref() == Some(id.as_str()))
                            {
                                seen_fill = true;
                                break;
                            }
                            tokio::time::sleep(self.timing.poll).await;
                        }
                        if !seen_fill {
                            unresolved.push(id);
                        }
                    }
                    Err(e) => log::warn!(
                        "[MAKER_FIRST] {}: taker remainder not sent: {e:?}",
                        intent.symbol
                    ),
                }
            }
        }

        Ok(ExecOutcome {
            filled_qty: book.filled,
            fills: book.rows,
            position_after: None,
            unresolved,
        })
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

    fn push_fill(st: &mut MockState, id: &str, qty: f64, price: f64) {
        st.next_trade += 1;
        let trade_id = format!("t{}", st.next_trade);
        st.fills.push(VenueFill {
            order_id: id.to_string(),
            trade_id,
            qty,
            price,
            fee_usd: Some(0.0),
        });
    }

    #[async_trait]
    impl OrderVenue for MockVenue {
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
            if !st.cancel_noop {
                st.open.remove(order_id);
            }
            Ok(())
        }
        async fn open_order_ids(&self, _: &str) -> Result<HashSet<String>, DexError> {
            Ok(self.s().open.clone())
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
            st.next_id += 1;
            let id = format!("i{}", st.next_id);
            st.iocs.push((qty, limit));
            if st.ioc_fills && !st.ioc_hidden {
                let px = st.last_touch.1.min(limit); // buy at the ask, capped
                push_fill(&mut st, &id, qty, px);
            }
            Ok(id)
        }
    }

    fn timing() -> MakerFirstTiming {
        MakerFirstTiming {
            poll: Duration::from_millis(100),
            cancel_confirm_polls: 3,
            ioc_fill_polls: 3,
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
}
