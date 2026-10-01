//! Deterministic paper venue for passive (maker) execution
//! (bot-strategy#1099 Step 2c).
//!
//! [`PaperBook`] is a pure, event-driven simulator implementing the fill
//! rules frozen in the #1091 G1 spec (`G1_SPEC.md` §3.2–3.5).
//!
//! - **Latency (§3.2).** A decision at time `τ` takes effect at
//!   `τ + D + L`: a post-only order goes live at `τ + D + L_place`; a cancel
//!   removes the order at `τ + D + L_cancel`, and until then it stays live
//!   and fillable (cancel-in-flight fills count); an IOC executes at
//!   `τ + D + L_taker`.
//! - **Same millisecond (§3.2).** An effect scheduled at ms `T` is applied
//!   *after* every tape event stamped `T`. A new order cannot be filled by a
//!   print at `T`; an order being cancelled at `T` can.
//! - **Queue (§3.3).** On going live at `P`, `queue_ahead` is the displayed
//!   size at `P` on our side: the L1 size at the best price, a depth level's
//!   size, `0` when we improve the touch or join an empty level between
//!   displayed ones, and the largest displayed size when `P` lies beyond the
//!   displayed levels. Size decreases never advance us. Our order is not
//!   inserted into the book (no self-impact).
//! - **Fills (§3.4).** A print at exactly `P`, from the side that hits us,
//!   consumes `queue_ahead` first and fills us with the excess. A print
//!   through `P` (below our bid / above our ask) clears the level: the
//!   print's full size fills us. The fill price is always `P`. A partial fill
//!   keeps the rest at the front of the queue.
//! - **Taker (§3.5).** An IOC fills against the book at execution time: L1
//!   first, then depth levels, within its limit; the rest is cancelled.
//! - **Book validity (§3.1).** While the book is invalid no order goes live
//!   and nothing fills.
//!
//! [`PaperVenue`] exposes a `PaperBook` as an [`OrderVenue`], so the same
//! [`super::maker_first::MakerFirstExecutor`] runs on paper and live. The
//! caller supplies the decision clock and feeds the tape.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use dex_connector::DexError;

use super::intent::Side;
use super::maker_first::{OrderVenue, VenueFill};

/// Per-tier delays and fees.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PaperParams {
    /// Feed + processing + signing + transport (G1 `D`).
    pub d_ms: u64,
    pub place_ms: u64,
    pub cancel_ms: u64,
    pub taker_ms: u64,
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
}

impl PaperParams {
    /// G1 Standard tier: place 200 ms, cancel 300 ms, taker 300 ms, fee 0.
    pub fn lighter_standard() -> Self {
        Self {
            d_ms: 150,
            place_ms: 200,
            cancel_ms: 300,
            taker_ms: 300,
            maker_fee_bps: 0.0,
            taker_fee_bps: 0.0,
        }
    }
    /// G1 Premium tier: place / cancel 0 ms, taker 140 ms, maker 0.40 bp,
    /// taker 2.80 bp.
    pub fn lighter_premium() -> Self {
        Self {
            d_ms: 150,
            place_ms: 0,
            cancel_ms: 0,
            taker_ms: 140,
            maker_fee_bps: 0.40,
            taker_fee_bps: 2.80,
        }
    }
}

/// One price level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone)]
struct Resting {
    side: Side,
    price: f64,
    remaining: f64,
    queue_ahead: f64,
}

#[derive(Debug, Clone)]
enum Effect {
    Live {
        id: String,
        side: Side,
        qty: f64,
        price: f64,
    },
    Remove {
        id: String,
    },
    Ioc {
        id: String,
        side: Side,
        qty: f64,
        limit: f64,
    },
}

/// The pure simulator. Times are epoch-ms.
#[derive(Debug)]
pub struct PaperBook {
    params: PaperParams,
    /// Best-first bid levels / ask levels (index 0 = L1).
    bids: Vec<Level>,
    asks: Vec<Level>,
    valid: bool,
    /// Scheduled effects keyed by (time, sequence).
    effects: BTreeMap<(u64, u64), Effect>,
    seq: u64,
    live: HashMap<String, Resting>,
    /// Orders sent but not yet live (they count as open).
    pending: HashSet<String>,
    canceled: HashSet<String>,
    fills: Vec<VenueFill>,
    position: f64,
    next_id: u64,
    next_trade: u64,
}

impl PaperBook {
    pub fn new(params: PaperParams) -> Self {
        Self {
            params,
            bids: Vec::new(),
            asks: Vec::new(),
            valid: true,
            effects: BTreeMap::new(),
            seq: 0,
            live: HashMap::new(),
            pending: HashSet::new(),
            canceled: HashSet::new(),
            fills: Vec::new(),
            position: 0.0,
            next_id: 0,
            next_trade: 0,
        }
    }

    fn schedule(&mut self, at: u64, e: Effect) {
        self.seq += 1;
        self.effects.insert((at, self.seq), e);
    }

    /// Apply every scheduled effect strictly before `ts` (the same-ms rule:
    /// effects at `ts` wait until the tape events at `ts` are processed).
    fn run_effects_before(&mut self, ts: u64) {
        while let Some((&key, _)) = self.effects.iter().next() {
            if key.0 >= ts {
                break;
            }
            let e = self.effects.remove(&key).unwrap();
            self.apply_effect(e);
        }
    }

    /// Apply every scheduled effect at or before `ts` (end of the events at
    /// `ts`).
    pub fn flush(&mut self, ts: u64) {
        self.run_effects_before(ts + 1);
    }

    fn displayed_queue(&self, side: Side, price: f64) -> f64 {
        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        let Some(best) = levels.first() else {
            return 0.0;
        };
        // Better than the best (we improve the touch) → front of an empty level.
        let improves = match side {
            Side::Buy => price > best.price,
            Side::Sell => price < best.price,
        };
        if improves {
            return 0.0;
        }
        if let Some(l) = levels.iter().find(|l| l.price == price) {
            return l.size;
        }
        let worst = levels.last().unwrap();
        let beyond = match side {
            Side::Buy => price < worst.price,
            Side::Sell => price > worst.price,
        };
        if beyond {
            levels.iter().map(|l| l.size).fold(0.0, f64::max)
        } else {
            0.0 // an empty level between displayed ones
        }
    }

    fn apply_effect(&mut self, e: Effect) {
        match e {
            Effect::Live {
                id,
                side,
                qty,
                price,
            } => {
                self.pending.remove(&id);
                // Cancelled before it ever went live.
                if self.canceled.contains(&id) {
                    return;
                }
                if !self.valid {
                    // No order exists while the book is invalid.
                    self.canceled.insert(id);
                    return;
                }
                // Post-only: an order that would cross is rejected.
                let crosses = match side {
                    Side::Buy => self.asks.first().is_some_and(|a| price >= a.price),
                    Side::Sell => self.bids.first().is_some_and(|b| price <= b.price),
                };
                if crosses {
                    self.canceled.insert(id);
                    return;
                }
                let queue_ahead = self.displayed_queue(side, price);
                self.live.insert(
                    id,
                    Resting {
                        side,
                        price,
                        remaining: qty,
                        queue_ahead,
                    },
                );
            }
            Effect::Remove { id } => {
                if self.live.remove(&id).is_some() || self.pending.remove(&id) {
                    self.canceled.insert(id);
                }
            }
            Effect::Ioc {
                id,
                side,
                qty,
                limit,
            } => {
                self.pending.remove(&id);
                if !self.valid {
                    return;
                }
                let levels: Vec<Level> = match side {
                    Side::Buy => self.asks.clone(),
                    Side::Sell => self.bids.clone(),
                };
                let mut left = qty;
                for l in levels {
                    let ok = match side {
                        Side::Buy => l.price <= limit,
                        Side::Sell => l.price >= limit,
                    };
                    if !ok || left <= 0.0 {
                        break;
                    }
                    let q = l.size.min(left);
                    left -= q;
                    self.book_fill(&id, side, q, l.price, self.params.taker_fee_bps);
                }
            }
        }
    }

    fn book_fill(&mut self, id: &str, side: Side, qty: f64, price: f64, fee_bps: f64) {
        if qty <= 0.0 {
            return;
        }
        self.next_trade += 1;
        self.position += match side {
            Side::Buy => qty,
            Side::Sell => -qty,
        };
        self.fills.push(VenueFill {
            order_id: id.to_string(),
            trade_id: format!("p{}", self.next_trade),
            qty,
            price,
            fee_usd: Some(qty * price * fee_bps / 1e4),
        });
    }

    /// A book update at `ts` (best-first levels; L1 at index 0).
    pub fn on_book(&mut self, ts: u64, bids: Vec<Level>, asks: Vec<Level>) {
        self.run_effects_before(ts);
        self.bids = bids;
        self.asks = asks;
    }

    pub fn set_valid(&mut self, ts: u64, valid: bool) {
        self.run_effects_before(ts);
        self.valid = valid;
    }

    /// A trade print at `ts`. `taker` is the aggressor's side: a taker sell
    /// hits bids, a taker buy lifts asks.
    pub fn on_trade(&mut self, ts: u64, price: f64, size: f64, taker: Side) {
        self.run_effects_before(ts);
        if !self.valid {
            return;
        }
        let hit_side = match taker {
            Side::Sell => Side::Buy,
            Side::Buy => Side::Sell,
        };
        let mut ids: Vec<String> = self
            .live
            .iter()
            .filter(|(_, r)| r.side == hit_side)
            .map(|(id, _)| id.clone())
            .collect();
        ids.sort(); // deterministic order
        for id in ids {
            let r = self.live.get_mut(&id).unwrap();
            let at_price = price == r.price;
            let through = match hit_side {
                Side::Buy => price < r.price,
                Side::Sell => price > r.price,
            };
            let fill = if through {
                r.queue_ahead = 0.0;
                size.min(r.remaining)
            } else if at_price {
                let consumed = size.min(r.queue_ahead);
                r.queue_ahead -= consumed;
                (size - consumed).min(r.remaining)
            } else {
                0.0
            };
            if fill <= 0.0 {
                continue;
            }
            r.remaining -= fill;
            let (side, px, done) = (r.side, r.price, r.remaining <= 1e-12);
            if done {
                self.live.remove(&id);
            }
            self.book_fill(&id, side, fill, px, self.params.maker_fee_bps);
        }
    }

    // ---- the OrderVenue side (decision time `now`)

    pub fn touch(&self) -> (f64, f64) {
        (
            self.bids.first().map(|l| l.price).unwrap_or(0.0),
            self.asks.first().map(|l| l.price).unwrap_or(0.0),
        )
    }

    pub fn place_post_only(&mut self, now: u64, side: Side, qty: f64, price: f64) -> String {
        self.next_id += 1;
        let id = format!("pm{}", self.next_id);
        self.pending.insert(id.clone());
        let at = now + self.params.d_ms + self.params.place_ms;
        self.schedule(
            at,
            Effect::Live {
                id: id.clone(),
                side,
                qty,
                price,
            },
        );
        id
    }

    pub fn cancel(&mut self, now: u64, id: &str) {
        let at = now + self.params.d_ms + self.params.cancel_ms;
        self.schedule(at, Effect::Remove { id: id.to_string() });
    }

    pub fn place_ioc(&mut self, now: u64, side: Side, qty: f64, limit: f64) -> String {
        self.next_id += 1;
        let id = format!("pi{}", self.next_id);
        self.pending.insert(id.clone());
        let at = now + self.params.d_ms + self.params.taker_ms;
        self.schedule(
            at,
            Effect::Ioc {
                id: id.clone(),
                side,
                qty,
                limit,
            },
        );
        id
    }

    pub fn open_ids(&self) -> HashSet<String> {
        self.live
            .keys()
            .chain(self.pending.iter().filter(|id| id.starts_with("pm")))
            .cloned()
            .collect()
    }

    pub fn canceled_ids(&self) -> HashSet<String> {
        self.canceled.clone()
    }

    pub fn fills(&self) -> Vec<VenueFill> {
        self.fills.clone()
    }

    pub fn position(&self) -> f64 {
        self.position
    }
}

/// Decision clock for [`PaperVenue`] (epoch-ms).
pub type PaperClock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// A [`PaperBook`] as an [`OrderVenue`]. The tape feeder holds a clone of
/// `book` and drives `on_book` / `on_trade` / `flush`.
pub struct PaperVenue {
    pub book: Arc<Mutex<PaperBook>>,
    pub clock: PaperClock,
}

impl PaperVenue {
    fn b(&self) -> std::sync::MutexGuard<'_, PaperBook> {
        self.book.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[async_trait]
impl OrderVenue for PaperVenue {
    async fn touch(&self, _: &str) -> Result<(f64, f64), DexError> {
        Ok(self.b().touch())
    }
    async fn position(&self, _: &str) -> Result<f64, DexError> {
        Ok(self.b().position())
    }
    async fn place_post_only(
        &self,
        _: &str,
        side: Side,
        qty: f64,
        price: f64,
        _: bool,
    ) -> Result<String, DexError> {
        let now = (self.clock)();
        Ok(self.b().place_post_only(now, side, qty, price))
    }
    async fn cancel(&self, _: &str, order_id: &str) -> Result<(), DexError> {
        let now = (self.clock)();
        self.b().cancel(now, order_id);
        Ok(())
    }
    async fn open_order_ids(&self, _: &str) -> Result<HashSet<String>, DexError> {
        Ok(self.b().open_ids())
    }
    async fn canceled_order_ids(&self, _: &str) -> Result<HashSet<String>, DexError> {
        Ok(self.b().canceled_ids())
    }
    async fn fills(&self, _: &str) -> Result<Vec<VenueFill>, DexError> {
        Ok(self.b().fills())
    }
    async fn place_ioc(
        &self,
        _: &str,
        side: Side,
        qty: f64,
        limit: f64,
        _: bool,
    ) -> Result<String, DexError> {
        let now = (self.clock)();
        Ok(self.b().place_ioc(now, side, qty, limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trade::execution::intent::{ExecIntent, ExecStyle, MakerFirstParams, Role};
    use crate::trade::execution::maker_first::{MakerFirstExecutor, MakerFirstTiming};
    use std::time::Duration;

    fn lv(price: f64, size: f64) -> Level {
        Level { price, size }
    }

    /// A book at t=0: bid 99.9×5 (then 99.8×7), ask 100.1×4; zero latency
    /// unless a test sets its own.
    fn book(params: PaperParams) -> PaperBook {
        let mut b = PaperBook::new(params);
        b.on_book(0, vec![lv(99.9, 5.0), lv(99.8, 7.0)], vec![lv(100.1, 4.0)]);
        b
    }

    fn zero() -> PaperParams {
        PaperParams {
            d_ms: 0,
            place_ms: 0,
            cancel_ms: 0,
            taker_ms: 0,
            maker_fee_bps: 0.0,
            taker_fee_bps: 0.0,
        }
    }

    fn filled(b: &PaperBook, id: &str) -> f64 {
        b.fills()
            .iter()
            .filter(|f| f.order_id == id)
            .map(|f| f.qty)
            .sum()
    }

    #[test]
    fn a_print_at_our_price_eats_the_queue_first_then_fills_us_at_p() {
        let mut b = book(zero());
        let id = b.place_post_only(10, Side::Buy, 3.0, 99.9);
        b.flush(10);
        b.on_trade(20, 99.9, 3.0, Side::Sell); // queue 5 → 2
        assert_eq!(filled(&b, &id), 0.0);
        b.on_trade(30, 99.9, 4.0, Side::Sell); // 2 more queue, 2 to us
        assert!((filled(&b, &id) - 2.0).abs() < 1e-12);
        assert!(b.fills().iter().all(|f| f.price == 99.9), "fill at P");
        // The rest keeps the front of the queue.
        b.on_trade(40, 99.9, 1.0, Side::Sell);
        assert!((filled(&b, &id) - 3.0).abs() < 1e-12);
        assert!(!b.open_ids().contains(&id), "fully filled leaves the book");
        assert!((b.position() - 3.0).abs() < 1e-12);
    }

    #[test]
    fn a_trade_through_fills_the_prints_full_size_and_clears_the_queue() {
        let mut b = book(zero());
        let id = b.place_post_only(10, Side::Buy, 10.0, 99.9);
        b.flush(10);
        b.on_trade(20, 99.8, 6.0, Side::Sell); // through our bid
        assert!((filled(&b, &id) - 6.0).abs() < 1e-12);
        assert!(
            b.fills().iter().all(|f| f.price == 99.9),
            "fill at P, not the print"
        );
        b.on_trade(30, 99.9, 1.0, Side::Sell); // queue already cleared
        assert!((filled(&b, &id) - 7.0).abs() < 1e-12);
    }

    #[test]
    fn queue_position_follows_the_displayed_levels() {
        let mut b = book(zero());
        let improve = b.place_post_only(1, Side::Buy, 1.0, 99.95); // inside the best
        let depth = b.place_post_only(1, Side::Buy, 1.0, 99.8); // a depth level
        let gap = b.place_post_only(1, Side::Buy, 1.0, 99.85); // empty, between levels
        let beyond = b.place_post_only(1, Side::Buy, 1.0, 99.0); // past the levels
        b.flush(1);
        let q = |id: &str| b.live[id].queue_ahead;
        assert_eq!(q(&improve), 0.0);
        assert_eq!(q(&depth), 7.0);
        assert_eq!(q(&gap), 0.0);
        assert_eq!(q(&beyond), 7.0, "largest displayed size");
    }

    #[test]
    fn size_decreases_never_advance_the_queue() {
        let mut b = book(zero());
        let id = b.place_post_only(1, Side::Buy, 1.0, 99.9);
        b.flush(1);
        b.on_book(2, vec![lv(99.9, 1.0)], vec![lv(100.1, 4.0)]); // 5 → 1 displayed
        assert_eq!(b.live[&id].queue_ahead, 5.0);
    }

    #[test]
    fn latency_and_the_same_millisecond_rule() {
        let mut b = book(PaperParams::lighter_standard()); // live at +350 ms
        let id = b.place_post_only(1_000, Side::Buy, 1.0, 99.95);
        b.on_trade(1_349, 99.95, 5.0, Side::Sell);
        assert_eq!(filled(&b, &id), 0.0, "not live yet");
        b.on_trade(1_350, 99.95, 5.0, Side::Sell);
        assert_eq!(
            filled(&b, &id),
            0.0,
            "a print at the live ms cannot fill a new order"
        );
        b.on_trade(1_351, 99.95, 5.0, Side::Sell);
        assert!((filled(&b, &id) - 1.0).abs() < 1e-12);
    }

    #[test]
    fn a_cancelled_order_fills_until_its_removal_time_and_is_then_recorded() {
        let mut b = book(PaperParams::lighter_standard()); // cancel removes at +450 ms
        let id = b.place_post_only(0, Side::Buy, 2.0, 99.95);
        b.flush(400);
        b.cancel(1_000, &id);
        b.on_trade(1_450, 99.95, 1.0, Side::Sell); // same ms as the removal: still fills
        assert!((filled(&b, &id) - 1.0).abs() < 1e-12);
        b.on_trade(1_451, 99.95, 1.0, Side::Sell); // removed
        assert!((filled(&b, &id) - 1.0).abs() < 1e-12);
        assert!(b.canceled_ids().contains(&id));
        assert!(!b.open_ids().contains(&id));
    }

    #[test]
    fn a_cancel_before_going_live_never_rests() {
        let mut b = book(PaperParams::lighter_premium()); // live and remove both at +150
        let id = b.place_post_only(0, Side::Buy, 1.0, 99.95);
        b.cancel(0, &id);
        b.flush(200);
        assert!(!b.open_ids().contains(&id));
        assert!(b.canceled_ids().contains(&id));
        b.on_trade(300, 99.95, 5.0, Side::Sell);
        assert_eq!(filled(&b, &id), 0.0);
    }

    #[test]
    fn a_crossing_post_only_is_rejected() {
        let mut b = book(zero());
        let id = b.place_post_only(1, Side::Buy, 1.0, 100.1); // at the ask
        b.flush(1);
        assert!(!b.open_ids().contains(&id));
        assert!(b.canceled_ids().contains(&id));
    }

    #[test]
    fn an_ioc_walks_the_book_within_its_limit_and_pays_the_taker_fee() {
        let mut p = zero();
        p.taker_fee_bps = 2.8;
        let mut b = PaperBook::new(p);
        b.on_book(
            0,
            vec![lv(99.9, 5.0)],
            vec![lv(100.1, 1.0), lv(100.2, 1.0), lv(100.6, 9.0)],
        );
        let id = b.place_ioc(1, Side::Buy, 3.0, 100.5);
        b.flush(1);
        assert!(
            (filled(&b, &id) - 2.0).abs() < 1e-12,
            "100.6 is beyond the limit"
        );
        let fee: f64 = b.fills().iter().map(|f| f.fee_usd.unwrap()).sum();
        assert!((fee - (100.1 + 100.2) * 2.8e-4).abs() < 1e-9);
    }

    #[test]
    fn an_invalid_book_neither_activates_nor_fills() {
        let mut b = book(zero());
        b.set_valid(1, false);
        let id = b.place_post_only(2, Side::Buy, 1.0, 99.95);
        b.flush(2);
        assert!(!b.open_ids().contains(&id));
        b.set_valid(3, true);
        b.on_trade(4, 99.8, 5.0, Side::Sell);
        assert_eq!(filled(&b, &id), 0.0);
    }

    /// End to end: the 2b executor on the paper venue under tokio's paused
    /// clock. A feeder replays prints; the maker order fills at P with the
    /// maker fee and no taker is needed.
    #[tokio::test(start_paused = true)]
    async fn maker_first_executes_on_the_paper_venue() {
        let mut p = PaperParams::lighter_premium();
        p.maker_fee_bps = 0.4;
        let book = Arc::new(Mutex::new(PaperBook::new(p)));
        book.lock()
            .unwrap()
            .on_book(0, vec![lv(99.9, 1.0)], vec![lv(100.1, 1.0)]);
        let t0 = tokio::time::Instant::now();
        let clock: PaperClock = Arc::new(move || t0.elapsed().as_millis() as u64);
        let venue = PaperVenue {
            book: book.clone(),
            clock: clock.clone(),
        };
        // Feeder: at 1 s and 1.2 s a seller hits 99.9 (our bid joins behind 1.0).
        let feeder = {
            let book = book.clone();
            tokio::spawn(async move {
                for (at, size) in [(1_000u64, 1.5), (1_200, 2.0)] {
                    tokio::time::sleep_until(t0 + Duration::from_millis(at)).await;
                    let mut b = book.lock().unwrap();
                    b.on_trade(at, 99.9, size, Side::Sell);
                    b.flush(at);
                }
                // Keep scheduled effects (cancels) moving.
                for _ in 0..200 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let now = t0.elapsed().as_millis() as u64;
                    book.lock().unwrap().flush(now.saturating_sub(1));
                }
            })
        };
        let intent = ExecIntent {
            symbol: "BTC".into(),
            side: Side::Buy,
            qty: 2.0,
            reference_price: 100.0,
            reduce_only: false,
            deadline_ms: None,
            max_slip_bps: Some(50.0),
            style: ExecStyle::MakerFirst(MakerFirstParams {
                maker_window_ms: 1_500,
                requote_bps: 5.0,
            }),
        };
        let ex = MakerFirstExecutor {
            venue: &venue,
            timing: MakerFirstTiming {
                poll: Duration::from_millis(50),
                cancel_confirm_polls: 20,
                settle_polls: 20,
                ioc_fill_polls: 20,
            },
        };
        let o = ex.execute(&intent).await.unwrap();
        feeder.abort();
        assert!(o.unresolved.is_empty(), "{:?}", o.unresolved);
        assert!((o.filled_qty - 2.0).abs() < 1e-12, "{}", o.filled_qty);
        assert!(o
            .fills
            .iter()
            .all(|f| f.role == Role::Maker && f.price == 99.9));
        let fee: f64 = o.fills.iter().map(|f| f.fee_usd.unwrap()).sum();
        assert!((fee - 2.0 * 99.9 * 0.4e-4).abs() < 1e-9);
    }
}
