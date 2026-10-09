//! Pure decision logic: what to quote, when to flatten and in which order,
//! book shocks. The async loop in `main.rs` only wires these to IO.

use dex_connector::{DexError, OrderBookLevel, OrderSide};
use rust_decimal::{Decimal, RoundingStrategy};
use std::collections::{HashMap, HashSet, VecDeque};

/// One of our two resting quotes. A bid fill buys (+inventory).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum QSide {
    Bid,
    Ask,
}

impl QSide {
    pub fn order_side(self) -> OrderSide {
        match self {
            QSide::Bid => OrderSide::Long,
            QSide::Ask => OrderSide::Short,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            QSide::Bid => "bid",
            QSide::Ask => "ask",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuoteTarget {
    pub side: QSide,
    pub px: Decimal,
    pub qty: Decimal,
}

/// Presence quoting (bot-strategy#1093 option B): quotes rest `offset_bps`
/// behind the touch so they fill only on a sweep, and stay put while the
/// touch moves within `band_bps` of the offset, so they rest for a long time
/// ("quoting consistently") instead of being cancelled and re-placed every
/// tick. `offset_bps == 0` is plain at-touch quoting. Config guarantees
/// `0 < band_bps < offset_bps` whenever the offset is on, so a quote that is
/// kept can never be at or through the touch. The band must be wider than
/// one price tick (in bp): the venue rounds a resting price away from the
/// touch by up to a tick.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Presence {
    pub offset_bps: Decimal,
    pub band_bps: Decimal,
}

impl Presence {
    pub fn on(&self) -> bool {
        self.offset_bps > Decimal::ZERO
    }
}

pub struct QuoteParams {
    pub clip_usd: Decimal,
    pub skew_usd: Decimal,
    /// Post-fill |inventory| must stay ≤ this (effective cap × headroom).
    pub cap_usd: Decimal,
    pub min_quote_usd: Decimal,
    pub qty_decimals: u32,
    pub presence: Presence,
    /// Venue minimums (bot-strategy#1093 dust fix): the smallest base size the
    /// venue accepts at the current price (its `minOrderSize` raised to the
    /// `minOrderNotional` floor, as the connector's ticker reports it) and
    /// its size step in decimals. `None` until the first ticker read; then
    /// `dust_usd` is the only dust test and sizes round to `qty_decimals`.
    pub venue_min: VenueMin,
}

/// What the venue will still accept as an order: inventory below it is DUST
/// — it cannot be flattened (the venue rejects the order) and cannot be
/// quoted out, so it is carried as if flat until a later fill absorbs it.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct VenueMin {
    /// Smallest base quantity accepted (incl. the notional floor), if known.
    pub min_order_qty: Option<Decimal>,
    /// The venue's size step, in decimals, if known.
    pub size_decimals: Option<u32>,
    /// Notional below which inventory is dust when the venue minimum is not
    /// known (the venue's documented floor is $5).
    pub dust_usd: Decimal,
}

impl VenueMin {
    /// Dust: nothing (zero), or less than the venue would accept as an
    /// order. With the venue minimum known that minimum decides (it already
    /// includes the notional floor at the current price); otherwise the
    /// notional fallback does.
    pub fn is_dust(&self, qty: Decimal, px: Decimal) -> bool {
        let qty = qty.abs();
        if qty.is_zero() {
            return true;
        }
        match self.min_order_qty.filter(|m| *m > Decimal::ZERO) {
            Some(min) => qty < min,
            None => qty * px.max(Decimal::ZERO) < self.dust_usd,
        }
    }

    /// Whether a below-minimum rejection recorded for `forced` still holds
    /// for the current inventory `qty` at `px`: it lapses when the inventory
    /// is no longer that quantity, or the quantity is an acceptable order
    /// again (Codex round 2 on pairtrade#382: a forced override is never
    /// permanent).
    pub fn forced_dust_still_holds(&self, forced: Decimal, qty: Decimal, px: Decimal) -> bool {
        forced == qty && self.is_dust(forced, px)
    }

    /// Decimals a venue-bound size is rounded to: the venue's own step when
    /// known (finer than the runtime's quoting decimals on Arcus RWA
    /// markets: 7 vs 5), else the runtime's.
    pub fn size_decimals_or(&self, fallback: u32) -> u32 {
        self.size_decimals.unwrap_or(fallback)
    }
}

const BPS: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);

/// The price `offset_bps` behind `touch` on `side` (bid below, ask above),
/// NOT rounded to a tick: the connector rounds a resting price away from
/// the touch when it places the order and reports the price it used, and
/// keep / re-peg is judged on the distance band, so the runtime needs no
/// tick of its own. Offset 0 is the touch itself.
pub fn offset_px(side: QSide, touch: Decimal, offset_bps: Decimal) -> Decimal {
    let frac = offset_bps.max(Decimal::ZERO) / BPS;
    match side {
        QSide::Bid => touch * (Decimal::ONE - frac),
        QSide::Ask => touch * (Decimal::ONE + frac),
    }
}

/// `px` rounded AWAY from the touch to `tick` (bid down, ask up), so
/// rounding can only make a quote more passive. Fails closed: without a
/// positive tick there is no rounded price (`None`), and the caller must
/// decide explicitly what to do instead.
pub fn round_away(side: QSide, px: Decimal, tick: Option<Decimal>) -> Option<Decimal> {
    let tick = tick.filter(|t| *t > Decimal::ZERO)?;
    let px = match side {
        QSide::Bid => (px / tick).floor() * tick,
        QSide::Ask => (px / tick).ceil() * tick,
    };
    (px > Decimal::ZERO).then_some(px)
}

/// How far `px` rests behind `touch` on `side`, in bp of the touch
/// (negative = through the touch).
pub fn dist_bps(side: QSide, px: Decimal, touch: Decimal) -> Decimal {
    if touch <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    match side {
        QSide::Bid => (touch - px) / touch * BPS,
        QSide::Ask => (px - touch) / touch * BPS,
    }
}

/// A resting quote's two distances for `status.json`, in bp: from its side's
/// peg reference (the price the band decision uses, i.e. excluding our own
/// quote; `None` when only our quote is displayed), and from the raw touch.
pub fn quote_dists(side: QSide, px: Decimal, t: &Touches) -> (Option<Decimal>, Decimal) {
    let (raw, peg) = match side {
        QSide::Bid => (t.bid, t.peg_bid),
        QSide::Ask => (t.ask, t.peg_ask),
    };
    (
        peg.map(|touch| dist_bps(side, px, touch)),
        dist_bps(side, px, raw),
    )
}

/// Book levels to read per side. Paper presence quotes join the queue
/// displayed at a deep price, so the sim needs that level in the snapshot
/// (100 = the venue's maximum); everything else needs only the top.
pub fn book_depth(dry_run: bool, presence_on: bool, gate_on: bool) -> usize {
    let base = if dry_run && presence_on { 100 } else { 10 };
    // The quote gate's L2 imbalance features are defined on the top 20
    // levels (bot-strategy#1120); never fewer while it is on.
    if gate_on {
        base.max(crate::quote_gate::features::OWN_L2_LEVELS)
    } else {
        base
    }
}

/// Whether the periodic venue ticker read is due now (every mode: it carries
/// the venue's order minimums, the dust threshold; in DRY_RUN presence
/// quoting also the price tick for the paper sim). Never during a 429
/// backoff.
pub fn ticker_read_due(now_ms: u64, next_read_ms: u64, backoff_until_ms: u64) -> bool {
    now_ms >= next_read_ms && now_ms >= backoff_until_ms
}

/// The best price on one side of the book that is NOT our own resting quote
/// (live presence quoting): pegging off a touch our own order makes would
/// measure a distance of 0 and walk the quote outward every tick. `levels`
/// is that side of the book, best first (only the first two are read);
/// `own` is our resting quote's `(price, open size)` on that side. The best
/// level counts as ours when its price equals ours and it shows no more
/// than our open size (+1%); the next level is then the reference. `None` =
/// nothing but our own quote is displayed.
pub fn peg_touch(levels: &[OrderBookLevel], own: Option<(Decimal, Decimal)>) -> Option<Decimal> {
    let best = levels.first()?;
    let Some((own_px, own_open)) = own else {
        return Some(best.price);
    };
    let only_ours = best.price == own_px && best.size <= own_open + own_open / Decimal::ONE_HUNDRED;
    if only_ours {
        levels.get(1).map(|l| l.price)
    } else {
        Some(best.price)
    }
}

/// Our own quote on a side as the book displays it, `(price, open size)`,
/// for `peg_touch`. Paper quotes are virtual (never in the book): `None`.
pub fn own_displayed(dry_run: bool, resting: Option<&Resting>) -> Option<(Decimal, Decimal)> {
    if dry_run {
        return None;
    }
    resting.map(|r| (r.px, r.qty - r.filled))
}

/// What one tick's quotes are planned against: the raw top of book, plus
/// (presence quoting) each side's peg reference, i.e. the best price that
/// is not our own quote.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Touches {
    pub bid: Decimal,
    pub ask: Decimal,
    /// `peg_touch` per side; `None` = only our own quote is displayed.
    pub peg_bid: Option<Decimal>,
    pub peg_ask: Option<Decimal>,
}

/// `Touches` from the two sides of a book (best first) and our own resting
/// quotes' `(price, open size)`; `None` when a side is empty.
pub fn touches(
    bids: &[OrderBookLevel],
    asks: &[OrderBookLevel],
    own_bid: Option<(Decimal, Decimal)>,
    own_ask: Option<(Decimal, Decimal)>,
) -> Option<Touches> {
    Some(Touches {
        bid: bids.first()?.price,
        ask: asks.first()?.price,
        peg_bid: peg_touch(bids, own_bid),
        peg_ask: peg_touch(asks, own_ask),
    })
}

/// One side's plan for this tick: everything `quote_action` needs.
#[derive(Debug, Clone, PartialEq)]
pub struct SidePlan {
    pub side: QSide,
    /// Size to quote. `None` = this side must not quote (inventory at the
    /// skew, no cap room, unusable book): a resting quote is cancelled.
    pub qty: Option<Decimal>,
    pub price: PricePlan,
}

/// Where a side quotes this tick.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PricePlan {
    /// Quote at `px`; a resting price is compared with it per `tol`.
    At { px: Decimal, tol: PriceTol },
    /// No price can be formed this tick (presence quoting: only our own
    /// quote is displayed on the side): nothing is placed. A resting quote
    /// of the right size stays; one whose size is off is cancelled and
    /// comes back when the side is priced again.
    Unpriced,
    /// Presence quoting while the book is crossed: as `Unpriced`, and
    /// nothing says which side of a crossed book is the stale one, so a
    /// resting quote is kept only while it is still at least `min_bps`
    /// behind `near`, the book top nearer to it (the lower of the two for a
    /// bid, the higher for an ask). Closer than that it is cancelled.
    Crossed { near: Decimal, min_bps: Decimal },
}

#[cfg(test)]
impl SidePlan {
    /// The quote this plan would place; `None` when the side is suppressed
    /// or has no price.
    pub fn target(&self) -> Option<QuoteTarget> {
        let PricePlan::At { px, .. } = self.price else {
            return None;
        };
        Some(QuoteTarget {
            side: self.side,
            px,
            qty: self.qty?,
        })
    }
}

/// Both sides' plans. A side is suppressed when inventory already sits at
/// the skew on the side it would grow, and every quote is sized so its full
/// fill keeps |inventory| within `cap_usd`. `inv_qty` is signed base. The
/// mid of the raw top of book is the one price used for sizing, the skew
/// and the exit decision.
///
/// At-touch quoting (offset 0): both sides quote the raw touch and are kept
/// only at exactly that price; a crossed book suppresses both sides.
///
/// Presence quoting: the exit side is decided ONCE here (`exit_quote`). It
/// quotes at the raw touch, exact price, sized to the inventory only (never
/// more, so working a sweep fill out cannot flip the position and start
/// at-touch churn), and like any at-touch quote it is suppressed while the
/// book is crossed. Every other side rests `offset_bps` behind its peg
/// reference and is kept inside the band. With no peg reference it has no
/// price this tick and a resting offset quote stays where it is. While the
/// book is crossed (the feed does that for a few ticks at a time, and for
/// longer when one side of it freezes) it has no price either, and a
/// resting offset quote stays only while it is still `offset − band` behind
/// BOTH book tops (`PricePlan::Crossed`). Dust inventory (below what the
/// venue accepts as an order, `VenueMin::is_dust`) has no exit side and is
/// carried as if flat.
pub fn plan_quotes(t: &Touches, inv_qty: Decimal, p: &QuoteParams) -> (SidePlan, SidePlan) {
    let presence = p.presence;
    let crossed = t.ask <= t.bid;
    if t.bid <= Decimal::ZERO || t.ask <= Decimal::ZERO || (crossed && !presence.on()) {
        let none = |side| SidePlan {
            side,
            qty: None,
            price: PricePlan::Unpriced,
        };
        return (none(QSide::Bid), none(QSide::Ask));
    }
    let mid = (t.bid + t.ask) / Decimal::TWO;
    let inv_usd = inv_qty * mid;
    let exit = exit_quote(inv_qty, mid, p);
    let size = |room_usd: Decimal| -> Option<Decimal> {
        let usd = p.clip_usd.min(room_usd);
        if usd < p.min_quote_usd {
            return None;
        }
        let qty = (usd / mid).round_dp_with_strategy(p.qty_decimals, RoundingStrategy::ToZero);
        (qty > Decimal::ZERO).then_some(qty)
    };
    let side_plan = |side: QSide, qty: Option<Decimal>| -> SidePlan {
        let (raw, peg) = match side {
            QSide::Bid => (t.bid, t.peg_bid),
            QSide::Ask => (t.ask, t.peg_ask),
        };
        let exit_qty = exit.filter(|(s, _)| *s == side).map(|(_, q)| q);
        if !presence.on() || exit_qty.is_some() {
            // At the touch. Crossed here means presence mode's exit side,
            // whose size is the exit size itself (`exit_quote`), never the
            // clip at the current mid: that is how a 3.24483 short was
            // worked out with a 3.24437 bid and left 0.00046 of dust that
            // the venue would not let us flatten (bot-strategy#1093, live
            // 2026-10-03 22:55Z).
            let qty = exit_qty.or(qty);
            let (qty, price) = if crossed {
                (None, PricePlan::Unpriced)
            } else {
                let tol = PriceTol::Exact;
                (qty, PricePlan::At { px: raw, tol })
            };
            return SidePlan { side, qty, price };
        }
        let price = match peg {
            _ if crossed => PricePlan::Crossed {
                near: match side {
                    QSide::Bid => t.bid.min(t.ask),
                    QSide::Ask => t.bid.max(t.ask),
                },
                min_bps: presence.offset_bps - presence.band_bps,
            },
            None => PricePlan::Unpriced,
            Some(touch) => PricePlan::At {
                px: offset_px(side, touch, presence.offset_bps),
                tol: PriceTol::Band {
                    touch,
                    offset_bps: presence.offset_bps,
                    band_bps: presence.band_bps,
                },
            },
        };
        SidePlan { side, qty, price }
    };
    // A bid fill moves inventory up by q: |inv + q| ≤ cap ⇔ q ≤ cap − inv.
    let bid_qty = if inv_usd >= p.skew_usd {
        None
    } else {
        size(p.cap_usd - inv_usd)
    };
    let ask_qty = if -inv_usd >= p.skew_usd {
        None
    } else {
        size(p.cap_usd + inv_usd)
    };
    (
        side_plan(QSide::Bid, bid_qty),
        side_plan(QSide::Ask, ask_qty),
    )
}

/// Presence mode only: the side that works existing inventory out AT the
/// touch, and how much. The side that reduces a non-zero inventory exits at
/// the touch, so a sweep fill is worked out as a maker instead of waiting
/// for max-hold and paying taker. `None` when presence is off, or the
/// inventory is dust (`VenueMin::is_dust`, which covers flat): then both
/// sides rest at the offset.
///
/// Size (bot-strategy#1093 dust fix): the WHOLE inventory, rounded towards
/// zero to the venue's size step, whenever the inventory is at most a clip
/// or exceeds a clip only by dust; only an inventory more than a clip (plus
/// dust) above the clip is worked out a clip at a time. Sizing the exit from
/// the clip at the current mid (the previous rule) left the entry/exit
/// price difference as unflattenable dust. Only
/// `plan_quotes` decides this.
fn exit_quote(inv_qty: Decimal, mid: Decimal, p: &QuoteParams) -> Option<(QSide, Decimal)> {
    if !p.presence.on() || p.venue_min.is_dust(inv_qty, mid) {
        return None;
    }
    let step = p.venue_min.size_decimals_or(p.qty_decimals);
    let inv = inv_qty
        .abs()
        .round_dp_with_strategy(step, RoundingStrategy::ToZero);
    let clip_qty = (p.clip_usd / mid).round_dp_with_strategy(step, RoundingStrategy::ToZero);
    let excess = inv - clip_qty;
    let qty = if excess <= Decimal::ZERO || p.venue_min.is_dust(excess, mid) {
        inv
    } else {
        clip_qty
    };
    if p.venue_min.is_dust(qty, mid) {
        return None;
    }
    let side = if inv_qty > Decimal::ZERO {
        QSide::Ask
    } else {
        QSide::Bid
    };
    Some((side, qty))
}

/// What a connector error means for the runtime's state, independent of
/// whether it is logged (Codex P2 on pairtrade#382: a log throttle must never
/// swallow the classification).
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorEffect {
    /// 429: no new placement until this unix second (ms).
    Backoff { until_ms: u64 },
    /// The venue state is uncertain: reconcile before quoting again.
    Reconcile,
    /// A flatten IOC the venue rejected as below its minimum order: the
    /// inventory is dust (bot-strategy#1093), no retry.
    BelowMinimum,
    /// Expected noise (post-only would cross): debug only.
    Quiet,
    /// Anything else: warn.
    Other,
}

/// Classify a connector error. Pure: the caller applies the effect to its
/// state and decides what to log.
pub fn classify_error(err: &DexError) -> ErrorEffect {
    match err {
        DexError::RateLimited { until_unix } => ErrorEffect::Backoff {
            until_ms: (*until_unix).max(0) as u64 * 1_000,
        },
        DexError::ReconciliationRequired { .. } => ErrorEffect::Reconcile,
        other => {
            let text = other.to_string();
            let lower = text.to_ascii_lowercase();
            if matches!(other, DexError::InvalidInput { .. })
                && lower.contains("below arcus minimum")
            {
                ErrorEffect::BelowMinimum
            } else if text.contains("POST_ONLY") || lower.contains("would cross") {
                ErrorEffect::Quiet
            } else {
                ErrorEffect::Other
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FlattenReason {
    Cap,
    /// The open position moved against its average entry by at least the
    /// configured per-position stop (bot-strategy#1093). Not a halt: once
    /// flat, quoting resumes after a short cooldown.
    PositionStop,
    MaxHold,
    Halt(String),
    Startup,
}

/// Flatten when inventory reaches the effective cap, has been open longer
/// than `max_hold_secs`, or a halt is in force. Never for `dust`: the venue
/// would reject the order (bot-strategy#1093: 11,383 rejected IOCs for
/// 0.00046 SPY in ten hours, quotes pulled the whole time), so dust is
/// carried as if flat and absorbed by the next fill.
///
/// Startup, halt (incl. KILL_SWITCH) and max-hold need no book: side and
/// size come from the position, and the live IOC prices off the connector's
/// own view (Codex P1, pairtrade#361). Only the cap check needs a mid.
///
/// `position_stop` is the caller's `position_stop_bps(..).is_some()` (fresh
/// mark only). It ranks after startup / halt / cap, so it never masks a
/// halt, and before max-hold.
#[allow(clippy::too_many_arguments)]
pub fn flatten_reason(
    inv_qty: Decimal,
    mid: Option<Decimal>,
    effective_cap_usd: Decimal,
    opened_at_ms: Option<u64>,
    now_ms: u64,
    max_hold_secs: u64,
    halt: Option<&str>,
    startup: bool,
    position_stop: bool,
    dust: bool,
) -> Option<FlattenReason> {
    if inv_qty.is_zero() || dust {
        return None;
    }
    if startup {
        return Some(FlattenReason::Startup);
    }
    if let Some(reason) = halt {
        return Some(FlattenReason::Halt(reason.to_string()));
    }
    if mid.is_some_and(|m| (inv_qty * m).abs() >= effective_cap_usd) {
        return Some(FlattenReason::Cap);
    }
    if position_stop {
        return Some(FlattenReason::PositionStop);
    }
    if opened_at_ms.is_some_and(|t| now_ms.saturating_sub(t) > max_hold_secs * 1_000) {
        return Some(FlattenReason::MaxHold);
    }
    None
}

/// Per-position stop (bot-strategy#1093): the open position's adverse move
/// against its average entry, in bp of the entry, at a FRESH mark — a long
/// loses when the mark is below the entry, a short when it is above.
/// `Some(adverse_bps)` when the stop is configured, the position is not
/// dust, a fresh mark exists and the adverse move is at least `stop_bps`;
/// `None` otherwise (no mark = no trigger, like the deferred-unrealized
/// rule of the daily stop).
pub fn position_stop_bps(
    inv_qty: Decimal,
    avg_px: Decimal,
    fresh_mark: Option<Decimal>,
    stop_bps: Option<Decimal>,
    dust: bool,
) -> Option<Decimal> {
    let stop = stop_bps?;
    let mark = fresh_mark?;
    if dust || inv_qty.is_zero() || avg_px <= Decimal::ZERO {
        return None;
    }
    let adverse = if inv_qty > Decimal::ZERO {
        (avg_px - mark) / avg_px
    } else {
        (mark - avg_px) / avg_px
    } * Decimal::from(10_000);
    (adverse >= stop).then_some(adverse)
}

/// How many `position_stop` journal rows belong to `day` (UTC
/// `YYYY-MM-DD`): the status counter survives a restart.
pub fn count_position_stops(rows: &[serde_json::Value], day: &str) -> u32 {
    rows.iter()
        .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some("position_stop"))
        .filter(|r| r.get("day").and_then(|d| d.as_str()) == Some(day))
        .count() as u32
}

#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    Cancel(QSide),
    Ioc { side: OrderSide, qty: Decimal },
}

/// The flatten sequence. Arcus has no self-trade prevention, so the quote on
/// the side the IOC would hit (our bid for a sell, our ask for a buy) is
/// cancelled FIRST; the other quote is pulled too (a fill there after the
/// flatten would reopen inventory). The IOC is last and must only be sent
/// once every cancel before it succeeded.
pub fn flatten_steps(inv_qty: Decimal, resting_bid: bool, resting_ask: bool) -> Vec<Step> {
    if inv_qty.is_zero() {
        return Vec::new();
    }
    let (hit, other, ioc_side) = if inv_qty > Decimal::ZERO {
        (QSide::Bid, QSide::Ask, OrderSide::Short)
    } else {
        (QSide::Ask, QSide::Bid, OrderSide::Long)
    };
    let resting = |s: QSide| match s {
        QSide::Bid => resting_bid,
        QSide::Ask => resting_ask,
    };
    let mut steps = Vec::new();
    if resting(hit) {
        steps.push(Step::Cancel(hit));
    }
    if resting(other) {
        steps.push(Step::Cancel(other));
    }
    steps.push(Step::Ioc {
        side: ioc_side,
        qty: inv_qty.abs(),
    });
    steps
}

/// A resting quote as we believe it rests.
#[derive(Debug, Clone, PartialEq)]
pub struct Resting {
    pub order_id: String,
    pub px: Decimal,
    pub qty: Decimal,
    pub filled: Decimal,
}

/// An order we placed less than this long ago may be missing from an
/// open-orders read without being gone: the read lags the placement
/// (bot-strategy#1093, 2026-10-06 00:00:00Z: the periodic reconcile ran in
/// the tick after a placement, did not see the two new quotes, forgot them,
/// and the same tick placed a second pair; the first pair rested untracked
/// until the next sweep cancelled it as stray 15 s later).
pub const PLACE_VISIBILITY_GRACE_MS: u64 = 10_000;

/// What a periodic open-orders read changes in the local quote book.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrderSweep {
    /// Sides whose quote is gone: absent from the read and old enough that
    /// the absence is real.
    pub forget: Vec<QSide>,
    /// Open orders on our market that are not one of our resting quotes.
    pub strays: Vec<String>,
}

/// Reconcile our resting quotes with an open-orders read. A quote absent from
/// the read is forgotten only once it is at least `grace_ms` old (by its
/// placement time; unknown placement time = old): a young quote missing from
/// the read is still ours, so it is neither re-placed nor treated as a stray
/// when it shows up. Strays are open ids that are none of our resting quotes.
pub fn sweep_open_orders(
    resting: &HashMap<QSide, Resting>,
    placed_at_ms: &HashMap<String, u64>,
    open_ids: &HashSet<String>,
    now_ms: u64,
    grace_ms: u64,
) -> OrderSweep {
    let mut forget: Vec<QSide> = resting
        .iter()
        .filter(|(_, r)| {
            !open_ids.contains(&r.order_id)
                && placed_at_ms
                    .get(&r.order_id)
                    .is_none_or(|t| now_ms.saturating_sub(*t) >= grace_ms)
        })
        .map(|(side, _)| *side)
        .collect();
    forget.sort_by_key(|s| s.as_str());
    let ours: HashSet<&String> = resting.values().map(|r| &r.order_id).collect();
    let mut strays: Vec<String> = open_ids
        .iter()
        .filter(|id| !ours.contains(id))
        .cloned()
        .collect();
    strays.sort();
    OrderSweep { forget, strays }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QuoteAction {
    Keep,
    Place(QuoteTarget),
    /// Reprice/resize an untouched resting order in place.
    Modify(QuoteTarget),
    /// Cancel (and, with `Some`, place the target afterwards): a partly
    /// filled order is replaced rather than modified, so "new total size"
    /// never has to account for fills.
    Replace(Option<QuoteTarget>),
}

/// In-place modify is OFF: on mainnet (2026-10-01, first live run) every
/// `batchModifyOrders` row was rejected with "provide exactly one of orderId
/// or clientId (not both)" (dex-connector sends both), so quotes could not be
/// repriced. Requotes go through cancel + place instead, both verified live;
/// Arcus modify is a cancel-replace anyway, so no queue priority is lost.
/// Re-enable once the connector's batch modify is fixed and verified.
pub const MODIFY_ENABLED: bool = false;

/// When a resting quote's price still counts as "on target".
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PriceTol {
    /// At-touch quoting: only the exact target price.
    Exact,
    /// Presence quoting: the target price, or anywhere whose distance from
    /// `touch` (the peg reference on the quote's side) lies within
    /// `offset_bps ± band_bps`.
    Band {
        touch: Decimal,
        offset_bps: Decimal,
        band_bps: Decimal,
    },
}

impl PriceTol {
    fn holds(&self, side: QSide, resting_px: Decimal, target_px: Decimal) -> bool {
        if resting_px == target_px {
            return true;
        }
        match *self {
            PriceTol::Exact => false,
            PriceTol::Band {
                touch,
                offset_bps,
                band_bps,
            } => {
                let dist = dist_bps(side, resting_px, touch);
                dist >= offset_bps - band_bps && dist <= offset_bps + band_bps
            }
        }
    }
}

/// The one requote rule, per side. A suppressed side (`plan.qty` None) is
/// cancelled, whatever else is unknown. Otherwise a resting quote is kept
/// while its price is on target and its open size is within 1% of the
/// target, and requoted (cancel + place while `MODIFY_ENABLED` is off) when
/// either is off. With no price this tick (`Unpriced` / `Crossed`) NOTHING
/// is ever placed: the book cannot be trusted to price an order. The price
/// counts as on target, a quote whose size is off is cancelled (it comes
/// back once the side is priced again), and in a crossed book a quote that
/// is no longer far enough behind both book tops is cancelled too.
pub fn quote_action(resting: Option<&Resting>, plan: &SidePlan) -> QuoteAction {
    let side = plan.side;
    let Some(qty) = plan.qty else {
        return match resting {
            Some(_) => QuoteAction::Replace(None),
            None => QuoteAction::Keep,
        };
    };
    let Some(r) = resting else {
        return match plan.price {
            PricePlan::At { px, .. } => QuoteAction::Place(QuoteTarget { side, px, qty }),
            PricePlan::Unpriced | PricePlan::Crossed { .. } => QuoteAction::Keep,
        };
    };
    let open = r.qty - r.filled;
    let size_off = (open - qty).abs() > qty / Decimal::ONE_HUNDRED;
    let px = match plan.price {
        PricePlan::At { px, tol } => {
            if tol.holds(side, r.px, px) && !size_off {
                return QuoteAction::Keep;
            }
            px
        }
        PricePlan::Unpriced => return unpriced_action(size_off),
        PricePlan::Crossed { near, min_bps } => {
            return unpriced_action(size_off || dist_bps(side, r.px, near) < min_bps);
        }
    };
    let target = QuoteTarget { side, px, qty };
    if MODIFY_ENABLED && r.filled.is_zero() {
        QuoteAction::Modify(target)
    } else {
        QuoteAction::Replace(Some(target))
    }
}

/// A resting quote on a side with no price: keep it, or cancel it. Never a
/// new order.
fn unpriced_action(cancel: bool) -> QuoteAction {
    if cancel {
        QuoteAction::Replace(None)
    } else {
        QuoteAction::Keep
    }
}

/// What the quote section of a tick may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotePass {
    /// Place, requote and cancel.
    Full,
    /// A 429 backoff is running: cancels only (`cancel_only`).
    CancelOnly,
}

/// Whether the tick plans quotes at all, and how. `Quote` is the full pass.
/// While waiting out a 429 backoff, presence quoting still runs the plan
/// for its cancels (a suppressed side, a crossed book come too close): a
/// quote that must go is never left resting because of a rate limit.
/// At-touch quoting leaves its quotes as they are while waiting, as before.
pub fn quote_pass(plan: &TickPlan, presence_on: bool) -> Option<QuotePass> {
    match plan {
        TickPlan::Quote => Some(QuotePass::Full),
        TickPlan::Wait if presence_on => Some(QuotePass::CancelOnly),
        _ => None,
    }
}

impl QuotePass {
    /// The action this pass actually executes for a planned `action`.
    pub fn apply(self, action: QuoteAction) -> QuoteAction {
        match self {
            QuotePass::Full => action,
            QuotePass::CancelOnly => cancel_only(action),
        }
    }
}

/// The cancel-only form of an action: nothing is placed or modified. A
/// quote that would be requoted is off target, so it is cancelled; it is
/// placed again by the first full pass after the backoff.
fn cancel_only(action: QuoteAction) -> QuoteAction {
    match action {
        QuoteAction::Keep | QuoteAction::Place(_) => QuoteAction::Keep,
        QuoteAction::Modify(_) | QuoteAction::Replace(_) => QuoteAction::Replace(None),
    }
}

/// Mid range over the last `window_ms` exceeds `bps` of the low.
pub fn shock(
    history: &VecDeque<(u64, Decimal)>,
    now_ms: u64,
    window_ms: u64,
    bps: Decimal,
) -> bool {
    let recent = history
        .iter()
        .filter(|(t, _)| now_ms.saturating_sub(*t) <= window_ms)
        .map(|(_, m)| *m);
    let (mut lo, mut hi) = (None::<Decimal>, None::<Decimal>);
    for m in recent {
        lo = Some(lo.map_or(m, |v| v.min(m)));
        hi = Some(hi.map_or(m, |v| v.max(m)));
    }
    match (lo, hi) {
        (Some(lo), Some(hi)) if lo > Decimal::ZERO => (hi - lo) / lo * Decimal::from(10_000) > bps,
        _ => false,
    }
}

/// What one tick may do, decided before any IO (Codex P1, pairtrade#361).
#[derive(Debug, Clone, Default)]
pub struct TickInputs {
    pub has_book: bool,
    pub halted: bool,
    pub stale: bool,
    /// A book shock now or its cooldown still running.
    pub shock_or_cooldown: bool,
    /// A 429 cooldown is running.
    pub backoff: bool,
    /// Live: the dead man's switch is armed and its refresh is succeeding
    /// (always true in DRY_RUN). No new quote without it.
    pub dms_armed: bool,
    /// Live: the startup position read succeeded and is in the ledger
    /// (always true in DRY_RUN). No new quote before it.
    pub startup_reconciled: bool,
    /// Live: an ambiguous order outcome still awaits its cancel-all +
    /// position read (always false in DRY_RUN).
    pub reconcile_pending: bool,
    /// Live: the last fill harvest succeeded and every fill it returned is
    /// booked (always true in DRY_RUN).
    pub fills_synced: bool,
    /// DRY_RUN: the trades tape is subscribed (always true live).
    pub tape_ready: bool,
    /// A failed journal append could not be rolled back: no new quoting
    /// until a restart repairs fills.jsonl.
    pub journal_unsafe: bool,
    /// The UTC rollover is not done / not on disk yet: nothing may book, so
    /// nothing new is quoted either.
    pub rollover_pending: bool,
    pub flatten: Option<FlattenReason>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TickPlan {
    /// Cancel quotes (hit side first) then reduce-only IOC.
    Flatten(FlattenReason),
    /// Pull every resting quote; the label says why.
    PullQuotes(&'static str),
    /// Rate-limit backoff with nothing unsafe resting: leave quotes as they are.
    Wait,
    Quote,
}

/// Safety first, and never gated by a rate-limit backoff: a flatten, then a
/// quote pull for no book / halt / stale book / shock. Only new placement or
/// modification waits out the backoff.
pub fn tick_plan(i: &TickInputs) -> TickPlan {
    if let Some(reason) = &i.flatten {
        return TickPlan::Flatten(reason.clone());
    }
    if i.journal_unsafe {
        return TickPlan::PullQuotes("journal_unsafe");
    }
    if i.rollover_pending {
        return TickPlan::PullQuotes("rollover_pending");
    }
    if !i.has_book {
        return TickPlan::PullQuotes("no_book");
    }
    if i.halted {
        return TickPlan::PullQuotes("halt");
    }
    if i.stale {
        return TickPlan::PullQuotes("stale_book");
    }
    if i.shock_or_cooldown {
        return TickPlan::PullQuotes("shock");
    }
    if !i.tape_ready {
        // Paper quotes are only fair while the sim sees every print.
        return TickPlan::PullQuotes("tape_not_ready");
    }
    if i.reconcile_pending {
        return TickPlan::PullQuotes("reconcile_pending");
    }
    if !i.fills_synced {
        return TickPlan::PullQuotes("fills_unsynced");
    }
    if !i.startup_reconciled {
        // Quoting on an unknown position could stack onto inventory we do
        // not know about (Codex P1, pairtrade#361); main retries the read.
        return TickPlan::PullQuotes("unreconciled");
    }
    if !i.dms_armed {
        // No resting quote without a dead man's switch behind it (Codex P1,
        // pairtrade#361); main keeps trying to arm it every tick.
        return TickPlan::PullQuotes("dms_unarmed");
    }
    if i.backoff {
        return TickPlan::Wait;
    }
    TickPlan::Quote
}

/// Everything the tick plan depends on, gathered after the tick's IO.
#[derive(Debug, Clone, Default)]
pub struct PlanState {
    pub dry_run: bool,
    pub has_book: bool,
    pub halted: bool,
    pub book_ts_ms: Option<u64>,
    pub book_stale_secs: u64,
    pub cooldown_until_ms: u64,
    pub backoff_until_ms: u64,
    pub dms_last_ok_ms: Option<u64>,
    pub dms_last_failed: bool,
    pub dms_secs: u64,
    pub startup_reconciled: bool,
    pub reconcile_pending: bool,
    /// The periodic position check last returned Pending/Unread
    /// (pre-G2, Codex P1 4146269811): same quote gate as a reconcile.
    pub position_pending: bool,
    pub fills_synced: bool,
    pub tape_ready: bool,
    pub journal_unsafe: bool,
    pub rollover_pending: bool,
    pub flatten: Option<FlattenReason>,
}

/// The tick plan's inputs at `now_ms`, which the caller reads AFTER every
/// awaited call of the tick (Codex P1, pairtrade#361): a DMS arm or a book
/// that was fresh when the tick began can be stale by the time it decides.
pub fn plan_inputs(s: &PlanState, now_ms: u64) -> TickInputs {
    TickInputs {
        has_book: s.has_book,
        halted: s.halted,
        stale: book_stale(s.book_ts_ms, now_ms, s.book_stale_secs),
        shock_or_cooldown: now_ms < s.cooldown_until_ms,
        backoff: now_ms < s.backoff_until_ms,
        dms_armed: s.dry_run || dms_armed(s.dms_last_ok_ms, s.dms_last_failed, now_ms, s.dms_secs),
        startup_reconciled: s.dry_run || s.startup_reconciled,
        reconcile_pending: !s.dry_run && (s.reconcile_pending || s.position_pending),
        fills_synced: s.dry_run || s.fills_synced,
        tape_ready: !s.dry_run || s.tape_ready,
        journal_unsafe: s.journal_unsafe,
        rollover_pending: s.rollover_pending,
        flatten: s.flatten.clone(),
    }
}

/// This runtime's position out of an account-wide `get_positions` read:
/// only the configured market counts (a shared subaccount may hold other
/// markets, e.g. NVDA-USD, which must never enter the ledger comparison,
/// the cap, or a flatten). Signed qty and entry price; flat if absent.
pub fn market_position(
    positions: &[dex_connector::PositionSnapshot],
    market: &str,
) -> (Decimal, Option<Decimal>) {
    let base = market.trim_end_matches("-USD").to_ascii_uppercase();
    positions
        .iter()
        .find(|p| {
            p.symbol
                .trim_end_matches("-USD")
                .eq_ignore_ascii_case(&base)
        })
        .map_or((Decimal::ZERO, None), |p| {
            (p.size.abs() * Decimal::from(p.sign.signum()), p.entry_price)
        })
}

/// Await `fut` for at most `limit`; `None` on timeout (pre-G2, Codex P2
/// 4152541379: the final shutdown spill read must not hang the shutdown).
pub async fn read_within<T, E>(
    fut: impl std::future::Future<Output = Result<T, E>>,
    limit: std::time::Duration,
) -> Option<Result<T, E>> {
    tokio::time::timeout(limit, fut).await.ok()
}

/// What to spill at shutdown: the fresh read when it came back, else the
/// unbooked records the bounded harvest already saw (never nothing just
/// because the venue stalled).
pub fn spill_rows<E>(
    read: Option<Result<Vec<dex_connector::FilledOrder>, E>>,
    seen: Vec<dex_connector::FilledOrder>,
) -> Vec<dex_connector::FilledOrder> {
    match read {
        Some(Ok(rows)) => rows,
        Some(Err(_)) | None => seen,
    }
}

/// Outcome of comparing the ledger with the venue position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PosCheck {
    /// The venue read failed.
    Unread,
    InSync,
    /// Different, but fills may still be in flight: wait.
    Pending,
    /// Still different after `grace_ms` with every fill harvested.
    Mismatch,
}

/// Position check (Codex P1, pairtrade#361). The runtime NEVER adopts the
/// venue quantity into the ledger: the connector cannot say which trade ids
/// a venue position already includes, so any adoption could be counted a
/// second time when a delayed fill arrives. A difference only waits (fills
/// in flight) and, when it outlives `grace_ms` while fills are fully
/// harvested, becomes `Mismatch` → sticky halt "position_mismatch" for a
/// human. The grace clock only runs while fills are synced. Returns the
/// verdict and the new "mismatch since" time.
pub fn position_check(
    ledger_qty: Decimal,
    venue_qty: Option<Decimal>,
    fills_synced: bool,
    since_ms: Option<u64>,
    now_ms: u64,
    grace_ms: u64,
) -> (PosCheck, Option<u64>) {
    let Some(venue) = venue_qty else {
        return (PosCheck::Unread, since_ms);
    };
    if (venue - ledger_qty).abs() <= Decimal::new(1, 8) {
        return (PosCheck::InSync, None);
    }
    if !fills_synced {
        return (PosCheck::Pending, None);
    }
    match since_ms {
        Some(t) if now_ms.saturating_sub(t) >= grace_ms => (PosCheck::Mismatch, Some(t)),
        Some(t) => (PosCheck::Pending, Some(t)),
        None => (PosCheck::Pending, Some(now_ms)),
    }
}

/// Whether a pending reconcile may be cleared after a position check
/// (Codex P1, pairtrade#361): only when the ledger agrees with the venue, or
/// the mismatch has escalated to the sticky position_mismatch halt (which
/// then holds quoting). `Pending` (a fill in flight) and `Unread` keep it.
pub fn reconcile_cleared(check: PosCheck) -> bool {
    matches!(check, PosCheck::InSync | PosCheck::Mismatch)
}

/// The periodic position check's quote gate (pre-G2, Codex P1 4146269811):
/// the same rule as the reconcile path. While the venue shows exposure the
/// ledger hasn't booked yet (Pending) or the read failed (Unread), no new
/// quote goes out; it clears on InSync, or once the mismatch has escalated
/// to the sticky position_mismatch halt (which then holds quoting itself).
pub fn position_gate_after(check: PosCheck) -> bool {
    !reconcile_cleared(check)
}

/// Prefix of the sticky-halt reason a position mismatch writes.
pub const POSITION_MISMATCH: &str = "position_mismatch";

/// The halt label to flatten on, if any: a position-mismatch halt pulls
/// quotes but never auto-flattens (the position itself is in doubt).
pub fn flatten_halt_label(halt_label: Option<&str>) -> Option<&str> {
    halt_label.filter(|l| !l.contains(POSITION_MISMATCH))
}

/// Something that sends quote batches, each gated on the DMS at send time.
pub(crate) trait BatchSink<B> {
    /// DMS armed right now (fresh clock).
    fn armed(&self) -> bool;
    async fn send(&mut self, batch: B);
}

/// Send `batches` in order, re-checking `armed()` immediately before EACH
/// one (Codex P1, pairtrade#361): a modify batch that took long must not
/// let the place batch after it go out on a lapsed DMS. Returns how many
/// were sent.
pub(crate) async fn send_gated<B, S: BatchSink<B>>(sink: &mut S, batches: Vec<B>) -> usize {
    let mut sent = 0;
    for batch in batches {
        if !sink.armed() {
            log::warn!(
                "[ARCUS_VOL] DMS not armed at send time; skipping the remaining quote batches"
            );
            break;
        }
        sink.send(batch).await;
        sent += 1;
    }
    sent
}

/// The UTC-rollover mark: only the mid of a timestamp-fresh book, by the
/// same predicate `plan_inputs` uses (Codex P2, pairtrade#361); a stale
/// snapshot never becomes the new day's baseline.
pub fn fresh_mark(
    mid: Option<Decimal>,
    book_ts_ms: Option<u64>,
    now_ms: u64,
    stale_secs: u64,
) -> Option<Decimal> {
    mid.filter(|_| !book_stale(book_ts_ms, now_ms, stale_secs))
}

/// The DMS counts as armed when the last successful arm/refresh is younger
/// than its own deadline and the latest attempt did not fail.
pub fn dms_armed(
    last_ok_ms: Option<u64>,
    last_attempt_failed: bool,
    now_ms: u64,
    dms_secs: u64,
) -> bool {
    !last_attempt_failed && last_ok_ms.is_some_and(|t| now_ms.saturating_sub(t) < dms_secs * 1_000)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownStep {
    CancelAll,
    /// Read open orders back: the DMS is only disarmed when none of ours rest.
    VerifyNoneResting,
    /// Book fills that landed up to the cancel acks (bounded wait).
    HarvestFills,
    Persist,
    DisarmDms,
}

/// Shutdown order (Codex P1, pairtrade#361): cancel first so nothing new
/// fills, verify nothing of ours still rests, harvest the fills that did
/// land and persist them, and only then disarm the dead man's switch (see
/// `may_disarm_dms`). DRY_RUN only persists.
pub fn shutdown_steps(live: bool) -> Vec<ShutdownStep> {
    if live {
        vec![
            ShutdownStep::CancelAll,
            ShutdownStep::VerifyNoneResting,
            ShutdownStep::HarvestFills,
            ShutdownStep::Persist,
            ShutdownStep::DisarmDms,
        ]
    } else {
        vec![ShutdownStep::Persist]
    }
}

/// The shutdown line when the per-market disarm did not come back
/// confirmed (an error, or the venue did not echo `marketId`;
/// bot-strategy#1093). Not a failure of the shutdown: our quotes were
/// already cancelled and read back, and the market switch, if still armed,
/// can only cancel this market's orders when its deadline passes.
pub fn dms_disarm_unconfirmed_note(market: &str, dms_secs: u64, err: &str) -> String {
    format!(
        "[ARCUS_VOL] shutdown: per-market DMS disarm unconfirmed ({err}); the {market} switch may \
         still fire within {dms_secs}s -- it only cancels {market} orders, which are already \
         cancelled; exiting normally"
    )
}

/// Disarm the DMS at shutdown only when the cancel succeeded AND a
/// read-back found none of our orders resting (`None` = the read failed).
pub fn may_disarm_dms(cancel_ok: bool, open_after: Option<usize>) -> bool {
    cancel_ok && open_after == Some(0)
}

/// Book older than `stale_secs`, or served without a feed timestamp (REST
/// fallback), is not quoted against.
pub fn book_stale(book_ts_ms: Option<u64>, now_ms: u64, stale_secs: u64) -> bool {
    match book_ts_ms {
        None => true,
        Some(t) => now_ms.saturating_sub(t) > stale_secs * 1_000,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn an_unconfirmed_shutdown_disarm_is_a_warning_about_this_market_only() {
        let n = dms_disarm_unconfirmed_note(
            "SPY-USD",
            30,
            "Arcus per-market disarm unconfirmed: venue did not echo marketId 7",
        );
        assert!(
            n.contains("SPY-USD switch may still fire within 30s"),
            "{n}"
        );
        assert!(n.contains("only cancels SPY-USD orders"), "{n}");
        assert!(n.contains("exiting normally"), "{n}");
        assert!(n.contains("did not echo marketId 7"), "{n}");
    }

    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn params() -> QuoteParams {
        QuoteParams {
            clip_usd: d("10000"),
            skew_usd: d("10000"),
            cap_usd: d("9500"),
            min_quote_usd: d("50"),
            qty_decimals: 5,
            presence: Presence::default(),
            venue_min: VenueMin {
                min_order_qty: None,
                size_decimals: None,
                dust_usd: d("5"),
            },
        }
    }

    /// A book whose peg references are its raw top (no quote of ours in it).
    fn at(bid: &str, ask: &str) -> Touches {
        Touches {
            bid: d(bid),
            ask: d(ask),
            peg_bid: Some(d(bid)),
            peg_ask: Some(d(ask)),
        }
    }

    fn priced(px: Decimal, tol: PriceTol) -> PricePlan {
        PricePlan::At { px, tol }
    }

    fn targets(plans: (SidePlan, SidePlan)) -> (Option<QuoteTarget>, Option<QuoteTarget>) {
        (plans.0.target(), plans.1.target())
    }

    #[test]
    fn flat_quotes_both_sides_sized_to_the_cap_headroom() {
        let (bid, ask) = targets(plan_quotes(
            &at("83642.9", "83643.0"),
            Decimal::ZERO,
            &params(),
        ));
        let bid = bid.unwrap();
        let ask = ask.unwrap();
        assert_eq!(bid.px, d("83642.9"));
        assert_eq!(ask.px, d("83643.0"));
        // min(clip 10k, cap 9.5k) / mid, rounded down to 5 dp.
        assert_eq!(bid.qty, d("0.11357"));
        assert_eq!(ask.qty, d("0.11357"));
        assert!(bid.qty * d("83642.95") <= d("9500"));
    }

    #[test]
    fn long_inventory_at_skew_quotes_only_the_reducing_ask() {
        let mut p = params();
        p.skew_usd = d("5000");
        let mid = d("83642.95");
        let inv = d("0.06"); // ~$5,019 ≥ skew
        let (bid, ask) = targets(plan_quotes(&at("83642.9", "83643.0"), inv, &p));
        assert!(bid.is_none());
        let ask = ask.unwrap();
        // cap + inv = 9500 + 5018.58 → clip 10k binds.
        assert_eq!(
            ask.qty,
            (d("10000") / mid).round_dp_with_strategy(5, RoundingStrategy::ToZero)
        );
    }

    #[test]
    fn short_inventory_below_skew_shrinks_the_growing_ask_to_the_cap() {
        let inv = d("-0.05"); // ~ -$4,182
        let (bid, ask) = targets(plan_quotes(&at("83642.9", "83643.0"), inv, &params()));
        let mid = d("83642.95");
        // ask grows |short|: room = 9500 − 4182.1475 = 5317.85
        let ask = ask.unwrap();
        assert!(ask.qty * mid <= d("5317.8525"), "{}", ask.qty * mid);
        assert!(ask.qty * mid > d("5300"));
        // bid reduces: room = 9500 + 4182 → clip binds
        assert_eq!(bid.unwrap().qty, d("0.11955"));
    }

    #[test]
    fn a_side_with_less_room_than_the_minimum_is_not_quoted() {
        let inv = d("0.1133"); // ~$9,477: bid room $23 < $50
        let (bid, ask) = targets(plan_quotes(&at("83642.9", "83643.0"), inv, &params()));
        assert!(bid.is_none());
        assert!(ask.is_some());
    }

    #[test]
    fn crossed_or_empty_book_quotes_nothing() {
        assert_eq!(
            targets(plan_quotes(&at("100", "100"), Decimal::ZERO, &params())),
            (None, None)
        );
        assert_eq!(
            targets(plan_quotes(&at("0", "100"), Decimal::ZERO, &params())),
            (None, None)
        );
    }

    #[test]
    fn flatten_triggers_on_cap_max_hold_and_halt() {
        let cap = d("10000");
        let m = Some(d("83642.95"));
        let fr = |inv: &str, mid: Option<Decimal>, now: u64, halt: Option<&str>, startup: bool| {
            flatten_reason(
                d(inv),
                mid,
                cap,
                Some(0),
                now,
                300,
                halt,
                startup,
                false,
                false,
            )
        };
        assert_eq!(fr("0", m, 1_000_000, Some("kill"), true), None);
        assert_eq!(
            fr("0.1", m, 1_000, Some("kill"), false),
            Some(FlattenReason::Halt("kill".into()))
        );
        assert_eq!(fr("-0.12", m, 1_000, None, false), Some(FlattenReason::Cap));
        assert_eq!(fr("0.1", m, 300_000, None, false), None);
        assert_eq!(
            fr("0.1", m, 300_001, None, false),
            Some(FlattenReason::MaxHold)
        );
        assert_eq!(
            fr("0.1", m, 1_000, Some("kill"), true),
            Some(FlattenReason::Startup)
        );
    }

    #[test]
    fn position_stop_triggers_at_the_threshold_long_and_short() {
        let stop = Some(d("25"));
        let avg = d("1000");
        // Long: adverse = mark below entry. 25 bp below 1000 = 997.5.
        assert_eq!(
            position_stop_bps(d("3"), avg, Some(d("997.5")), stop, false),
            Some(d("25"))
        );
        assert_eq!(
            position_stop_bps(d("3"), avg, Some(d("997.51")), stop, false),
            None
        );
        // A long gains when the mark is ABOVE entry: never a stop.
        assert_eq!(
            position_stop_bps(d("3"), avg, Some(d("1002.5")), stop, false),
            None
        );
        // Short: adverse = mark above entry.
        assert_eq!(
            position_stop_bps(d("-3"), avg, Some(d("1002.5")), stop, false),
            Some(d("25"))
        );
        assert_eq!(
            position_stop_bps(d("-3"), avg, Some(d("1002.49")), stop, false),
            None
        );
        assert_eq!(
            position_stop_bps(d("-3"), avg, Some(d("997.5")), stop, false),
            None
        );
        // Beyond the threshold reports the actual adverse move.
        assert_eq!(
            position_stop_bps(d("-3"), avg, Some(d("1004")), stop, false),
            Some(d("40"))
        );
    }

    #[test]
    fn position_stop_needs_a_config_a_fresh_mark_and_real_inventory() {
        let avg = d("1000");
        let far = Some(d("900"));
        assert_eq!(position_stop_bps(d("3"), avg, far, None, false), None); // off
        assert_eq!(
            position_stop_bps(d("3"), avg, None, Some(d("25")), false),
            None
        ); // no mark
        assert_eq!(
            position_stop_bps(d("3"), avg, far, Some(d("25")), true),
            None
        ); // dust
        assert_eq!(
            position_stop_bps(d("0"), avg, far, Some(d("25")), false),
            None
        ); // flat
        assert_eq!(
            position_stop_bps(d("3"), d("0"), far, Some(d("25")), false),
            None
        ); // no entry
        assert!(position_stop_bps(d("3"), avg, far, Some(d("25")), false).is_some());
    }

    #[test]
    fn position_stop_ranks_after_halt_and_cap_and_before_max_hold() {
        let cap = d("10000");
        let m = Some(d("1000"));
        let fr =
            |inv: &str, now: u64, halt: Option<&str>, startup: bool, stop: bool, dust: bool| {
                flatten_reason(d(inv), m, cap, Some(0), now, 300, halt, startup, stop, dust)
            };
        assert_eq!(
            fr("1", 1_000, None, false, true, false),
            Some(FlattenReason::PositionStop)
        );
        // A halt is never masked by the stop.
        assert_eq!(
            fr("1", 1_000, Some("daily_stop"), false, true, false),
            Some(FlattenReason::Halt("daily_stop".into()))
        );
        assert_eq!(
            fr("1", 1_000, None, true, true, false),
            Some(FlattenReason::Startup)
        );
        // Cap outranks it; it outranks max-hold.
        assert_eq!(
            fr("12", 1_000, None, false, true, false),
            Some(FlattenReason::Cap)
        );
        assert_eq!(
            fr("1", 300_001, None, false, true, false),
            Some(FlattenReason::PositionStop)
        );
        // Dust never flattens.
        assert_eq!(fr("1", 1_000, None, false, true, true), None);
        // Once flat the stop is gone and the plan quotes again after the
        // cooldown (the cooldown itself is a plain quote pull).
        assert_eq!(fr("0", 1_000, None, false, false, false), None);
        let cooling = TickInputs {
            has_book: true,
            shock_or_cooldown: true,
            dms_armed: true,
            startup_reconciled: true,
            fills_synced: true,
            tape_ready: true,
            ..Default::default()
        };
        assert_eq!(tick_plan(&cooling), TickPlan::PullQuotes("shock"));
        let after = TickInputs {
            shock_or_cooldown: false,
            ..cooling.clone()
        };
        assert_eq!(tick_plan(&after), TickPlan::Quote);
        // While still holding, the flatten wins over the cooldown pull.
        let holding = TickInputs {
            flatten: Some(FlattenReason::PositionStop),
            ..cooling
        };
        assert_eq!(
            tick_plan(&holding),
            TickPlan::Flatten(FlattenReason::PositionStop)
        );
    }

    #[test]
    fn position_stops_are_counted_per_day_from_the_journal() {
        let rows = vec![
            serde_json::json!({"kind": "position_stop", "day": "2026-10-08"}),
            serde_json::json!({"kind": "position_stop", "day": "2026-10-09"}),
            serde_json::json!({"kind": "position_stop", "day": "2026-10-09"}),
            serde_json::json!({"kind": "fill", "day": "2026-10-09"}),
        ];
        assert_eq!(count_position_stops(&rows, "2026-10-09"), 2);
        assert_eq!(count_position_stops(&rows, "2026-10-10"), 0);
    }

    #[test]
    fn halt_kill_startup_and_max_hold_flatten_without_a_book() {
        let cap = d("10000");
        let fr = |now: u64, halt: Option<&str>, startup: bool| {
            flatten_reason(
                d("-0.12"),
                None,
                cap,
                Some(0),
                now,
                300,
                halt,
                startup,
                false,
                false,
            )
        };
        assert_eq!(
            fr(1_000, Some("kill_switch"), false),
            Some(FlattenReason::Halt("kill_switch".into()))
        );
        assert_eq!(fr(1_000, None, true), Some(FlattenReason::Startup));
        assert_eq!(fr(300_001, None, false), Some(FlattenReason::MaxHold));
        // Only the cap check needs a mid.
        assert_eq!(fr(1_000, None, false), None);
        // And the tick plan flattens with no book at all.
        let plan = tick_plan(&TickInputs {
            has_book: false,
            halted: true,
            backoff: true,
            flatten: fr(1_000, Some("kill_switch"), false),
            ..TickInputs::default()
        });
        assert_eq!(
            plan,
            TickPlan::Flatten(FlattenReason::Halt("kill_switch".into()))
        );
    }

    #[test]
    fn no_new_quotes_without_an_armed_dms_but_safety_still_runs() {
        let armed = TickInputs {
            has_book: true,
            dms_armed: true,
            startup_reconciled: true,
            fills_synced: true,
            tape_ready: true,
            ..TickInputs::default()
        };
        assert_eq!(tick_plan(&armed), TickPlan::Quote);
        let unarmed = TickInputs {
            dms_armed: false,
            ..armed.clone()
        };
        assert_eq!(tick_plan(&unarmed), TickPlan::PullQuotes("dms_unarmed"));
        let flat = TickInputs {
            flatten: Some(FlattenReason::MaxHold),
            ..unarmed
        };
        assert_eq!(tick_plan(&flat), TickPlan::Flatten(FlattenReason::MaxHold));
        // armed = fresh success and no failed attempt since
        assert!(dms_armed(Some(1_000), false, 30_999, 30));
        assert!(!dms_armed(Some(1_000), false, 31_000, 30));
        assert!(!dms_armed(Some(1_000), true, 2_000, 30));
        assert!(!dms_armed(None, false, 2_000, 30));
    }

    fn live_state() -> PlanState {
        PlanState {
            dry_run: false,
            has_book: true,
            book_ts_ms: Some(1_000),
            book_stale_secs: 5,
            dms_last_ok_ms: Some(1_000),
            dms_secs: 30,
            startup_reconciled: true,
            fills_synced: true,
            ..PlanState::default()
        }
    }

    #[test]
    fn a_pending_periodic_position_check_gates_quoting_until_in_sync() {
        // pre-G2, Codex P1 4146269811: an acknowledged quote fill shows up in
        // get_positions before get_filled_orders; the periodic check returns
        // Pending and must hold new quotes, like the reconcile path does.
        let now = 1_500;
        let quoting = |check: PosCheck| {
            let s = PlanState {
                position_pending: position_gate_after(check),
                ..live_state()
            };
            tick_plan(&plan_inputs(&s, now))
        };
        assert_eq!(
            quoting(PosCheck::Pending),
            TickPlan::PullQuotes("reconcile_pending")
        );
        assert_eq!(
            quoting(PosCheck::Unread),
            TickPlan::PullQuotes("reconcile_pending")
        );
        assert_eq!(quoting(PosCheck::InSync), TickPlan::Quote);
        // An escalated mismatch clears this gate; the sticky halt holds quotes.
        assert!(!position_gate_after(PosCheck::Mismatch));
        // Paper mode has no venue position: never gated by it.
        let dry = PlanState {
            dry_run: true,
            position_pending: true,
            tape_ready: true,
            ..live_state()
        };
        assert_eq!(tick_plan(&plan_inputs(&dry, now)), TickPlan::Quote);
    }

    #[test]
    fn other_markets_on_a_shared_account_never_enter_the_position_check() {
        use std::str::FromStr;
        let d = |v: &str| Decimal::from_str(v).unwrap();
        let snap = |sym: &str, size: &str, sign: i32| dex_connector::PositionSnapshot {
            symbol: sym.to_string(),
            size: d(size),
            sign,
            entry_price: Some(d("100")),
        };
        // An NVDA-USD position (manual trading) next to a flat BTC book.
        let positions = vec![snap("NVDA-USD", "4.43", -1)];
        let (qty, _) = market_position(&positions, "BTC-USD");
        assert_eq!(qty, Decimal::ZERO);
        // So a flat BTC ledger is InSync: no mismatch, no halt, no flatten.
        let (check, _) = position_check(Decimal::ZERO, Some(qty), true, None, 1_000, 5_000);
        assert_eq!(check, PosCheck::InSync);
        // With BTC present too, only BTC counts (sign applied).
        let positions = vec![snap("NVDA-USD", "4.43", -1), snap("BTC-USD", "0.12", -1)];
        assert_eq!(market_position(&positions, "BTC-USD").0, d("-0.12"));
        assert_eq!(market_position(&positions, "BTC").0, d("-0.12"));
        // A symbol that only shares a prefix is not ours.
        let positions = vec![snap("BTCDOM-USD", "1", 1)];
        assert_eq!(market_position(&positions, "BTC-USD").0, Decimal::ZERO);
    }

    #[tokio::test]
    async fn a_stalled_spill_read_falls_back_to_the_harvested_records() {
        // pre-G2, Codex P2 4152541379: a venue read that never returns is
        // cut off within the budget, and the records the harvest already
        // saw are spilled instead.
        let rec = |id: &str| dex_connector::FilledOrder {
            trade_id: id.into(),
            ..Default::default()
        };
        let t0 = std::time::Instant::now();
        let read = read_within(
            std::future::pending::<Result<Vec<dex_connector::FilledOrder>, String>>(),
            std::time::Duration::from_millis(50),
        )
        .await;
        assert!(read.is_none());
        assert!(t0.elapsed() < std::time::Duration::from_secs(2));
        let out = spill_rows(read, vec![rec("t1"), rec("t2")]);
        assert_eq!(
            out.iter().map(|r| r.trade_id.as_str()).collect::<Vec<_>>(),
            ["t1", "t2"]
        );
        // A read that returns in time is authoritative.
        let read = read_within(
            async { Ok::<_, String>(vec![rec("t3")]) },
            std::time::Duration::from_millis(50),
        )
        .await;
        assert_eq!(spill_rows(read, vec![rec("t1")])[0].trade_id, "t3");
        // A failed read also falls back.
        let read: Option<Result<Vec<_>, String>> = Some(Err("down".into()));
        assert_eq!(spill_rows(read, vec![rec("t1")])[0].trade_id, "t1");
    }

    #[test]
    fn a_dms_arm_fresh_at_tick_start_but_stale_at_plan_time_blocks_quoting() {
        let s = PlanState {
            book_ts_ms: Some(30_000),
            ..live_state()
        };
        // Tick started at 30.9 s: armed (29.9 s old).
        assert_eq!(tick_plan(&plan_inputs(&s, 30_900)), TickPlan::Quote);
        // IO took long; at plan time the arm is 30 s old → no quoting.
        assert_eq!(
            tick_plan(&plan_inputs(&s, 31_000)),
            TickPlan::PullQuotes("dms_unarmed")
        );
        // Same for the book: fresh at start, stale at plan time.
        let b = PlanState {
            dms_last_ok_ms: Some(10_000),
            ..live_state()
        };
        assert_eq!(tick_plan(&plan_inputs(&b, 6_000)), TickPlan::Quote);
        assert_eq!(
            tick_plan(&plan_inputs(&b, 6_001)),
            TickPlan::PullQuotes("stale_book")
        );
        // DRY_RUN needs neither a DMS nor a startup read.
        let dry = PlanState {
            dry_run: true,
            dms_last_ok_ms: None,
            startup_reconciled: false,
            tape_ready: true,
            ..live_state()
        };
        assert_eq!(tick_plan(&plan_inputs(&dry, 2_000)), TickPlan::Quote);
    }

    #[test]
    fn no_live_quoting_until_the_startup_position_is_reconciled() {
        let s = PlanState {
            startup_reconciled: false,
            ..live_state()
        };
        assert_eq!(
            tick_plan(&plan_inputs(&s, 2_000)),
            TickPlan::PullQuotes("unreconciled")
        );
        // Safety still runs.
        let f = PlanState {
            flatten: Some(FlattenReason::Halt("kill_switch".into())),
            ..s
        };
        assert_eq!(
            tick_plan(&plan_inputs(&f, 2_000)),
            TickPlan::Flatten(FlattenReason::Halt("kill_switch".into()))
        );
        let ok = PlanState {
            startup_reconciled: true,
            ..live_state()
        };
        assert_eq!(tick_plan(&plan_inputs(&ok, 2_000)), TickPlan::Quote);
    }

    #[test]
    fn pending_reconcile_or_unsynced_fills_block_quoting_but_not_safety() {
        let s = PlanState {
            reconcile_pending: true,
            ..live_state()
        };
        assert_eq!(
            tick_plan(&plan_inputs(&s, 2_000)),
            TickPlan::PullQuotes("reconcile_pending")
        );
        let f = PlanState {
            fills_synced: false,
            ..live_state()
        };
        assert_eq!(
            tick_plan(&plan_inputs(&f, 2_000)),
            TickPlan::PullQuotes("fills_unsynced")
        );
        let flat = PlanState {
            flatten: Some(FlattenReason::MaxHold),
            ..s
        };
        assert_eq!(
            tick_plan(&plan_inputs(&flat, 2_000)),
            TickPlan::Flatten(FlattenReason::MaxHold)
        );
        // DRY_RUN has neither.
        let dry = PlanState {
            dry_run: true,
            reconcile_pending: true,
            fills_synced: false,
            tape_ready: true,
            ..live_state()
        };
        assert_eq!(tick_plan(&plan_inputs(&dry, 2_000)), TickPlan::Quote);
    }

    #[test]
    fn position_check_waits_then_halts_and_never_adopts() {
        let g = 5_000;
        // Read failed.
        assert_eq!(
            position_check(d("0"), None, true, Some(1), 9, g),
            (PosCheck::Unread, Some(1))
        );
        // In sync clears the timer.
        assert_eq!(
            position_check(d("0.1"), Some(d("0.1")), true, Some(1), 9, g),
            (PosCheck::InSync, None)
        );
        // Fills unsynced: wait without starting the clock.
        assert_eq!(
            position_check(d("0"), Some(d("0.1")), false, None, 1_000, g),
            (PosCheck::Pending, None)
        );
        // Synced: start the clock, wait, then halt.
        assert_eq!(
            position_check(d("0"), Some(d("0.1")), true, None, 1_000, g),
            (PosCheck::Pending, Some(1_000))
        );
        assert_eq!(
            position_check(d("0"), Some(d("0.1")), true, Some(1_000), 5_999, g),
            (PosCheck::Pending, Some(1_000))
        );
        assert_eq!(
            position_check(d("0"), Some(d("0.1")), true, Some(1_000), 6_000, g),
            (PosCheck::Mismatch, Some(1_000))
        );
    }

    #[test]
    fn a_delayed_fill_after_a_mismatch_moves_inventory_exactly_once() {
        use crate::ledger::{book_fill, FillIn, Ledger};
        let g = 5_000;
        let mut l = Ledger::new("live", "2026-09-30");
        let venue = Some(d("0.1")); // the venue already holds the fill
                                    // The fill is not reported yet; past the grace the check halts...
        let (c, since) = position_check(l.position.qty, venue, true, None, 1_000, g);
        assert_eq!(c, PosCheck::Pending);
        let (c, _) = position_check(l.position.qty, venue, true, since, 7_000, g);
        assert_eq!(c, PosCheck::Mismatch);
        // ...and the ledger was never adopted to the venue.
        assert!(l.position.qty.is_zero());
        // The delayed fill arrives: booked once, now in sync.
        let fill = FillIn {
            trade_id: "late".into(),
            buy: true,
            qty: d("0.1"),
            px: d("83642.9"),
            fee: Decimal::ZERO,
            maker: true,
            order_id: "o".into(),
            fee_estimated: false,
        };
        book_fill(&mut l, &fill, 8_000, |_| Ok(())).unwrap();
        book_fill(&mut l, &fill, 9_000, |_| Ok(())).unwrap();
        assert_eq!(l.position.qty, d("0.1"));
        assert_eq!(
            position_check(l.position.qty, venue, true, since, 9_000, g).0,
            PosCheck::InSync
        );
    }

    #[test]
    fn an_ambiguous_fill_keeps_the_reconcile_gate_until_it_is_booked() {
        use crate::ledger::{book_fill, FillIn, Ledger};
        let g = 5_000;
        let mut l = Ledger::new("live", "2026-09-30");
        let venue = Some(d("0.1")); // the fill shows in get_positions first
                                    // get_filled_orders has not delivered it yet (fills unsynced).
        let (c, since) = position_check(l.position.qty, venue, false, None, 1_000, g);
        assert_eq!(c, PosCheck::Pending);
        assert!(!reconcile_cleared(c));
        // Still pending with fills synced but inside the grace.
        let (c, since) = position_check(l.position.qty, venue, true, since, 2_000, g);
        assert_eq!(c, PosCheck::Pending);
        assert!(!reconcile_cleared(c));
        let gated = PlanState {
            reconcile_pending: !reconcile_cleared(c),
            ..live_state()
        };
        assert_eq!(
            tick_plan(&plan_inputs(&gated, 2_000)),
            TickPlan::PullQuotes("reconcile_pending")
        );
        assert!(!reconcile_cleared(PosCheck::Unread));
        // The fill is booked: in sync, the gate clears and quoting resumes.
        let fill = FillIn {
            trade_id: "amb".into(),
            buy: true,
            qty: d("0.1"),
            px: d("83642.9"),
            fee: Decimal::ZERO,
            maker: true,
            order_id: "o".into(),
            fee_estimated: false,
        };
        book_fill(&mut l, &fill, 3_000, |_| Ok(())).unwrap();
        let (c, _) = position_check(l.position.qty, venue, true, since, 3_000, g);
        assert_eq!(c, PosCheck::InSync);
        assert!(reconcile_cleared(c));
        let clear = PlanState {
            reconcile_pending: !reconcile_cleared(c),
            ..live_state()
        };
        assert_eq!(tick_plan(&plan_inputs(&clear, 3_000)), TickPlan::Quote);
        // An escalated mismatch also clears it (the sticky halt takes over).
        assert!(reconcile_cleared(PosCheck::Mismatch));
    }

    #[test]
    fn a_position_mismatch_halt_never_auto_flattens() {
        assert_eq!(flatten_halt_label(Some("kill_switch")), Some("kill_switch"));
        assert_eq!(
            flatten_halt_label(Some("sticky: position_mismatch: ledger 0 venue 0.1")),
            None
        );
        assert_eq!(flatten_halt_label(None), None);
    }

    struct FakeSink {
        armed: Vec<bool>,
        sent: Vec<&'static str>,
    }

    impl BatchSink<&'static str> for FakeSink {
        fn armed(&self) -> bool {
            self.armed[self.sent.len()]
        }
        async fn send(&mut self, batch: &'static str) {
            self.sent.push(batch);
        }
    }

    #[tokio::test]
    async fn each_quote_batch_is_gated_on_the_dms_at_its_own_send_time() {
        // Armed before the modify batch, lapsed before the place batch.
        let mut sink = FakeSink {
            armed: vec![true, false],
            sent: Vec::new(),
        };
        assert_eq!(send_gated(&mut sink, vec!["modify", "place"]).await, 1);
        assert_eq!(sink.sent, vec!["modify"]);
        let mut sink = FakeSink {
            armed: vec![true, true],
            sent: Vec::new(),
        };
        assert_eq!(send_gated(&mut sink, vec!["modify", "place"]).await, 2);
        let mut sink = FakeSink {
            armed: vec![false, true],
            sent: Vec::new(),
        };
        assert_eq!(send_gated(&mut sink, vec!["modify", "place"]).await, 0);
    }

    #[test]
    fn paper_quotes_wait_for_the_tape_and_live_ignores_it() {
        let dry = PlanState {
            dry_run: true,
            tape_ready: false,
            ..live_state()
        };
        assert_eq!(
            tick_plan(&plan_inputs(&dry, 2_000)),
            TickPlan::PullQuotes("tape_not_ready")
        );
        let ready = PlanState {
            tape_ready: true,
            ..dry.clone()
        };
        assert_eq!(tick_plan(&plan_inputs(&ready, 2_000)), TickPlan::Quote);
        // Safety first even without a tape.
        let f = PlanState {
            flatten: Some(FlattenReason::MaxHold),
            ..dry
        };
        assert_eq!(
            tick_plan(&plan_inputs(&f, 2_000)),
            TickPlan::Flatten(FlattenReason::MaxHold)
        );
        // Live never reads the tape.
        let live = PlanState {
            tape_ready: false,
            ..live_state()
        };
        assert_eq!(tick_plan(&plan_inputs(&live, 2_000)), TickPlan::Quote);
    }

    #[test]
    fn an_unsafe_journal_blocks_quoting_but_not_the_flatten() {
        let s = PlanState {
            journal_unsafe: true,
            ..live_state()
        };
        assert_eq!(
            tick_plan(&plan_inputs(&s, 2_000)),
            TickPlan::PullQuotes("journal_unsafe")
        );
        let f = PlanState {
            flatten: Some(FlattenReason::MaxHold),
            ..s
        };
        assert_eq!(
            tick_plan(&plan_inputs(&f, 2_000)),
            TickPlan::Flatten(FlattenReason::MaxHold)
        );
    }

    #[test]
    fn a_stale_snapshot_after_midnight_postpones_the_rollover() {
        use crate::ledger::{Ledger, Rollover};
        let mut l = Ledger::new("live", "2026-09-30");
        l.record_fill(true, d("0.1"), d("100000"), Decimal::ZERO, true, 1);
        let mid = Some(d("99990"));
        // Book stamped 10 s before the plan clock (> 5 s): not a mark.
        let mark = fresh_mark(mid, Some(1_000), 11_001, 5);
        assert_eq!(mark, None);
        assert_eq!(l.rollover("2026-10-01", mark), Rollover::Postponed);
        // A fresh one: rolled at it.
        let mark = fresh_mark(mid, Some(11_000), 11_001, 5);
        assert_eq!(l.rollover("2026-10-01", mark), Rollover::Rolled);
        assert_eq!(l.day_start_unrealized, d("-1"));
        // A REST book without a feed time is not fresh either.
        assert_eq!(fresh_mark(mid, None, 11_001, 5), None);
    }

    #[test]
    fn a_pending_rollover_pulls_quotes_until_it_is_on_disk() {
        use crate::ledger::{ensure_rolled, Ledger};
        let mut l = Ledger::new("dry_run", "2026-09-30");
        let mut pending = false;
        // The rollover's persist fails: nothing may book, and quoting stops.
        let ok = ensure_rolled(&mut l, "2026-10-01", Some(d("1")), &mut pending, |_| {
            Err(std::io::Error::other("disk"))
        })
        .unwrap_or(false);
        let s = PlanState {
            dry_run: true,
            tape_ready: true,
            rollover_pending: !ok,
            ..live_state()
        };
        assert_eq!(
            tick_plan(&plan_inputs(&s, 2_000)),
            TickPlan::PullQuotes("rollover_pending")
        );
        // Persisted on the retry: quoting resumes.
        let ok = ensure_rolled(&mut l, "2026-10-01", Some(d("1")), &mut pending, |_| Ok(()))
            .unwrap_or(false);
        let s = PlanState {
            rollover_pending: !ok,
            ..s
        };
        assert_eq!(tick_plan(&plan_inputs(&s, 2_000)), TickPlan::Quote);
        // A flatten still goes first.
        let f = PlanState {
            rollover_pending: true,
            flatten: Some(FlattenReason::MaxHold),
            ..s
        };
        assert_eq!(
            tick_plan(&plan_inputs(&f, 2_000)),
            TickPlan::Flatten(FlattenReason::MaxHold)
        );
    }

    #[test]
    fn dms_is_disarmed_only_after_a_verified_clean_cancel() {
        assert!(may_disarm_dms(true, Some(0)));
        assert!(!may_disarm_dms(false, Some(0)));
        assert!(!may_disarm_dms(true, Some(1)));
        assert!(!may_disarm_dms(true, None));
    }

    #[test]
    fn flatten_cancels_the_hit_side_quote_before_the_ioc() {
        assert_eq!(
            flatten_steps(d("0.1"), true, true),
            vec![
                Step::Cancel(QSide::Bid),
                Step::Cancel(QSide::Ask),
                Step::Ioc {
                    side: OrderSide::Short,
                    qty: d("0.1")
                }
            ]
        );
        assert_eq!(
            flatten_steps(d("-0.2"), true, true),
            vec![
                Step::Cancel(QSide::Ask),
                Step::Cancel(QSide::Bid),
                Step::Ioc {
                    side: OrderSide::Long,
                    qty: d("0.2")
                }
            ]
        );
        assert_eq!(
            flatten_steps(d("0.1"), false, false),
            vec![Step::Ioc {
                side: OrderSide::Short,
                qty: d("0.1")
            }]
        );
        assert!(flatten_steps(Decimal::ZERO, true, true).is_empty());
    }

    fn presence(offset: &str, band: &str) -> Presence {
        Presence {
            offset_bps: d(offset),
            band_bps: d(band),
        }
    }

    fn lv(px: &str, size: &str) -> OrderBookLevel {
        OrderBookLevel {
            price: d(px),
            size: d(size),
        }
    }

    fn resting(px: &str, qty: &str) -> Resting {
        Resting {
            order_id: "o".into(),
            px: d(px),
            qty: d(qty),
            filled: Decimal::ZERO,
        }
    }

    #[test]
    fn a_session_switch_of_the_offset_moves_a_resting_quote_and_back() {
        // bot-strategy#1093 session offset: the runtime swaps the presence
        // pair at a session boundary; the ordinary band rule must move a
        // quote resting at the old distance, both ways, and keep it once
        // it rests at the new one.
        let t = at("770.43", "770.44");
        let off = QuoteParams {
            clip_usd: d("2500"),
            presence: presence("2", "1"),
            ..params()
        };
        let on = QuoteParams {
            clip_usd: d("2500"),
            presence: presence("5", "2"),
            ..params()
        };
        let (bid_off, _) = plan_quotes(&t, Decimal::ZERO, &off);
        let qty = bid_off.qty.unwrap();
        let px2 = offset_px(QSide::Bid, d("770.43"), d("2"));
        let px5 = offset_px(QSide::Bid, d("770.43"), d("5"));
        let r2 = Resting {
            qty,
            ..resting(&px2.to_string(), "0")
        };
        let r5 = Resting {
            qty,
            ..resting(&px5.to_string(), "0")
        };
        // Off-session: the 2 bp quote is kept.
        assert_eq!(quote_action(Some(&r2), &bid_off), QuoteAction::Keep);
        // Session opens: 2 bp is outside 5 ± 2 → moved to 5 bp.
        let (bid_on, _) = plan_quotes(&t, Decimal::ZERO, &on);
        assert_eq!(
            quote_action(Some(&r2), &bid_on),
            QuoteAction::Replace(Some(QuoteTarget {
                side: QSide::Bid,
                px: px5,
                qty
            }))
        );
        assert_eq!(quote_action(Some(&r5), &bid_on), QuoteAction::Keep);
        // Session closes: 5 bp is outside 2 ± 1 → back to 2 bp.
        assert_eq!(
            quote_action(Some(&r5), &bid_off),
            QuoteAction::Replace(Some(QuoteTarget {
                side: QSide::Bid,
                px: px2,
                qty
            }))
        );
    }

    /// Presence params used by the tests below: $500 clip, 5 bp ± 2 bp.
    fn presence_params() -> QuoteParams {
        QuoteParams {
            clip_usd: d("500"),
            skew_usd: d("2000"),
            cap_usd: d("2000"),
            presence: presence("5", "2"),
            ..params()
        }
    }

    #[test]
    fn offset_prices_are_unrounded_and_rounding_fails_closed_without_a_tick() {
        // Presence quoting (bot-strategy#1093 B). The runtime's offset price
        // is NOT rounded (review 2 A): 83438.4 × 0.9995 and 83438.5 × 1.0005.
        assert_eq!(offset_px(QSide::Bid, d("83438.4"), d("5")), d("83396.6808"));
        assert_eq!(
            offset_px(QSide::Ask, d("83438.5"), d("5")),
            d("83480.21925")
        );
        // Offset 0 is the touch itself.
        assert_eq!(
            offset_px(QSide::Bid, d("84600.13"), Decimal::ZERO),
            d("84600.13")
        );
        assert_eq!(
            offset_px(QSide::Ask, d("84600.13"), Decimal::ZERO),
            d("84600.13")
        );
        // Rounding (paper sim only) goes AWAY from the touch: bid down, ask
        // up; a price already on the tick is not pushed a further tick.
        let tick = Some(d("0.1"));
        assert_eq!(
            round_away(QSide::Bid, d("83396.6808"), tick),
            Some(d("83396.6"))
        );
        assert_eq!(
            round_away(QSide::Ask, d("83480.21925"), tick),
            Some(d("83480.3"))
        );
        assert_eq!(round_away(QSide::Bid, d("99950"), tick), Some(d("99950")));
        assert_eq!(round_away(QSide::Ask, d("100050"), tick), Some(d("100050")));
        // Fail closed: asking for rounding with no / zero / negative tick
        // gives NO price, never the unrounded one.
        for bad in [None, Some(Decimal::ZERO), Some(d("-0.1"))] {
            assert_eq!(round_away(QSide::Bid, d("83396.6808"), bad), None);
            assert_eq!(round_away(QSide::Ask, d("83480.21925"), bad), None);
        }
        // Distance is measured from the quote's own side of the touch.
        assert!(dist_bps(QSide::Bid, d("83396.6"), d("83438.4")) > d("5"));
        assert!(dist_bps(QSide::Bid, d("83396.6"), d("83438.4")) < d("5.02"));
        assert!(dist_bps(QSide::Ask, d("83480.3"), d("83438.5")) > d("5"));
        assert!(dist_bps(QSide::Ask, d("83438.4"), d("83438.5")) < Decimal::ZERO);

        // plan_quotes: offset 0 quotes the touch; an offset moves both sides
        // when flat, with no tick involved anywhere.
        let book = at("83438.4", "83438.5");
        let (bid, ask) = targets(plan_quotes(&book, Decimal::ZERO, &params()));
        assert_eq!(bid.unwrap().px, d("83438.4"));
        assert_eq!(ask.unwrap().px, d("83438.5"));
        let p = presence_params();
        let (bid, ask) = plan_quotes(&book, Decimal::ZERO, &p);
        assert_eq!(bid.target().unwrap().px, d("83396.6808"));
        assert_eq!(ask.target().unwrap().px, d("83480.21925"));
        // The connector rounds the resting price away from the touch and
        // reports it (83396.6 / 83480.3). Judged on distance, those quotes
        // are on target although they differ from the unrounded targets.
        assert_eq!(
            quote_action(Some(&resting("83396.6", "0.00599")), &bid),
            QuoteAction::Keep
        );
        assert_eq!(
            quote_action(Some(&resting("83480.3", "0.00599")), &ask),
            QuoteAction::Keep
        );
    }

    #[test]
    fn the_ticker_read_is_periodic_and_never_during_a_backoff() {
        // Review 2 A: no tick state can gate or delay quoting (see below);
        // the ticker read itself (dust threshold in every mode, paper tick in
        // DRY_RUN) is simply due when its clock says so, bot-strategy#1093.
        assert!(ticker_read_due(1_000, 1_000, 0));
        assert!(ticker_read_due(1_001, 1_000, 0));
        assert!(!ticker_read_due(999, 1_000, 0));
        // Never during a 429 backoff; again once it is over.
        assert!(!ticker_read_due(5_000, 1_000, 5_001));
        assert!(ticker_read_due(5_000, 1_000, 5_000));
        // And no tick state can pull quotes: a healthy live state quotes.
        assert_eq!(
            tick_plan(&plan_inputs(&live_state(), 2_000)),
            TickPlan::Quote
        );
    }

    #[test]
    fn the_peg_reference_excludes_our_own_resting_quote() {
        // Review #1 (live self-reference). Our bid 83396.6 × 0.006 became
        // the best bid; pegging off it would measure 0 bp and walk it out.
        let levels = [lv("83396.6", "0.006"), lv("83390.1", "1.2")];
        let own = Some((d("83396.6"), d("0.006")));
        assert_eq!(peg_touch(&levels, own), Some(d("83390.1")));
        // Within the 1% size tolerance it is still only us.
        let dusty = [lv("83396.6", "0.00605"), lv("83390.1", "1.2")];
        assert_eq!(peg_touch(&dusty, own), Some(d("83390.1")));
        // Someone else rests at our price too: that level is a real touch.
        let shared = [lv("83396.6", "0.5"), lv("83390.1", "1.2")];
        assert_eq!(peg_touch(&shared, own), Some(d("83396.6")));
        // Our quote is behind the best: the best is the reference.
        let behind = [lv("83438.4", "0.4"), lv("83396.6", "0.006")];
        assert_eq!(peg_touch(&behind, own), Some(d("83438.4")));
        // Partly filled: only the open size is ours.
        let partly = [lv("83396.6", "0.004")];
        assert_eq!(peg_touch(&partly, Some((d("83396.6"), d("0.004")))), None);
        // Nothing but our own quote is displayed → no reference.
        assert_eq!(peg_touch(&levels[..1], own), None);
        // No quote of ours (DRY_RUN, or nothing resting): the plain best.
        assert_eq!(peg_touch(&levels, None), Some(d("83396.6")));
        assert_eq!(peg_touch(&[], own), None);

        // `touches` wires each side's own quote to that side only: the raw
        // top stays the book's, the bid peg skips our quote, the ask peg
        // (no ask of ours) is the best ask.
        let asks = [lv("83438.5", "0.3"), lv("83440", "2")];
        let t = touches(&levels, &asks, own, None).unwrap();
        assert_eq!(
            t,
            Touches {
                bid: d("83396.6"),
                ask: d("83438.5"),
                peg_bid: Some(d("83390.1")),
                peg_ask: Some(d("83438.5")),
            }
        );
        // Our ask alone at the top of the asks, no bid of ours.
        let own_ask = Some((d("83438.5"), d("0.3")));
        let t = touches(&levels, &asks, None, own_ask).unwrap();
        assert_eq!(t.peg_bid, Some(d("83396.6")));
        assert_eq!(t.peg_ask, Some(d("83440")));
        assert_eq!(touches(&[], &asks, None, None), None);
        // What `touches` is fed: live, the resting quote's price and OPEN
        // size; in DRY_RUN nothing (virtual quotes are not in the book).
        let partly = Resting {
            filled: d("0.002"),
            ..resting("83396.6", "0.006")
        };
        assert_eq!(
            own_displayed(false, Some(&partly)),
            Some((d("83396.6"), d("0.004")))
        );
        assert_eq!(own_displayed(true, Some(&partly)), None);
        assert_eq!(own_displayed(false, None), None);
        assert_eq!(touches(&levels, &[], None, None), None);

        // With the next level as reference our 83396.6 bid is THROUGH it
        // (−0.8 bp): out of the 3..7 bp band → re-peg behind the real touch.
        let p = presence_params();
        let t = touches(&levels, &asks, own, None).unwrap();
        let (bid, _) = plan_quotes(&t, Decimal::ZERO, &p);
        let r = resting("83396.6", "0.006");
        assert_eq!(
            quote_action(Some(&r), &bid),
            QuoteAction::Replace(Some(QuoteTarget {
                side: QSide::Bid,
                px: offset_px(QSide::Bid, d("83390.1"), d("5")),
                qty: bid.qty.unwrap(),
            }))
        );
    }

    #[test]
    fn with_no_peg_reference_the_target_is_still_honoured() {
        // Review 2 B. Only our own bid is displayed → no price this tick.
        let own = Some((d("83396.6"), d("0.00599")));
        let asks = [lv("83438.5", "0.3")];
        let t = touches(&[lv("83396.6", "0.00599")], &asks, own, None).unwrap();
        assert_eq!(t.peg_bid, None);
        let p = presence_params();
        let r = resting("83396.6", "0.00599");
        // Price cannot be judged, size is on target → keep where it is.
        let (bid, _) = plan_quotes(&t, Decimal::ZERO, &p);
        assert_eq!(bid.price, PricePlan::Unpriced);
        assert!(bid.qty.is_some());
        assert_eq!(quote_action(Some(&r), &bid), QuoteAction::Keep);
        // Nothing resting and no price → nothing is placed.
        assert_eq!(quote_action(None, &bid), QuoteAction::Keep);
        // Size off by more than 1% → cancel only (review 3 #1): with no
        // price nothing is ever placed, not even at the resting price.
        let big = resting("83396.6", "0.012");
        assert_eq!(quote_action(Some(&big), &bid), QuoteAction::Replace(None));
        let partly = Resting {
            filled: d("0.003"),
            ..resting("83396.6", "0.00599")
        };
        assert_eq!(
            quote_action(Some(&partly), &bid),
            QuoteAction::Replace(None)
        );
        // Within 1% → still kept.
        let near = resting("83396.6", "0.00603");
        assert_eq!(quote_action(Some(&near), &bid), QuoteAction::Keep);
        // Long past the skew: the bid is suppressed and MUST be cancelled
        // even though there is no reference to price against.
        let (bid, _) = plan_quotes(&t, d("0.03"), &p); // ~$2,503 ≥ skew $2,000
        assert_eq!((bid.qty, bid.price), (None, PricePlan::Unpriced));
        assert_eq!(quote_action(Some(&r), &bid), QuoteAction::Replace(None));
        assert_eq!(quote_action(None, &bid), QuoteAction::Keep);
    }

    #[test]
    fn a_crossed_book_keeps_resting_offset_quotes_in_presence_mode() {
        // Review 2 C: the feed's book is briefly crossed (ask ≤ bid) on a
        // few ticks a minute. Presence quotes must not be cancelled for it.
        let crossed = at("83438.6", "83438.5");
        let p = presence_params();
        let (bid, ask) = plan_quotes(&crossed, Decimal::ZERO, &p);
        // No price; the guard is offset − band = 3 bp behind the NEARER book
        // top: the lower one for the bid, the higher one for the ask.
        let guard = |near: &str| PricePlan::Crossed {
            near: d(near),
            min_bps: d("3"),
        };
        assert_eq!((bid.price, ask.price), (guard("83438.5"), guard("83438.6")));
        let rb = resting("83396.6", "0.00599");
        let ra = resting("83480.3", "0.00599");
        assert_eq!(quote_action(Some(&rb), &bid), QuoteAction::Keep);
        assert_eq!(quote_action(Some(&ra), &ask), QuoteAction::Keep);
        // Nothing is placed into a crossed book.
        assert_eq!(quote_action(None, &bid), QuoteAction::Keep);
        assert_eq!(quote_action(None, &ask), QuoteAction::Keep);
        // A partial fill during the crossed book (review 3 #1): the open
        // size is off, the quote is CANCELLED and no new order goes into a
        // book that cannot be trusted; same for any other size mismatch.
        let partly = Resting {
            filled: d("0.003"),
            ..rb.clone()
        };
        assert_eq!(
            quote_action(Some(&partly), &bid),
            QuoteAction::Replace(None)
        );
        let fat = resting("83480.3", "0.012");
        assert_eq!(quote_action(Some(&fat), &ask), QuoteAction::Replace(None));
        // A suppressed side is cancelled.
        let (bid, ask) = plan_quotes(&crossed, d("0.03"), &p);
        assert_eq!(quote_action(Some(&rb), &bid), QuoteAction::Replace(None));
        // The exit side (an AT-touch quote) is cancelled in a crossed book,
        // exactly like at-touch quoting.
        assert_eq!(ask.qty, None);
        let exit = resting("83438.5", "0.00599");
        assert_eq!(quote_action(Some(&exit), &ask), QuoteAction::Replace(None));

        // A frozen side (seen in the 2026-10-01 smoke: the ask stuck at
        // 83868.8 for ~100 s while the bid climbed to 83892 and beyond). The
        // resting ask 83902.8 is 4.1 bp behind the stale ask but only 1.3 bp
        // behind the live bid: it must NOT be kept blind, it is cancelled.
        let frozen = at("83892", "83868.8");
        let (bid, ask) = plan_quotes(&frozen, Decimal::ZERO, &p);
        assert_eq!((bid.price, ask.price), (guard("83868.8"), guard("83892")));
        let ask_q = resting("83902.8", "0.00596");
        assert_eq!(quote_action(Some(&ask_q), &ask), QuoteAction::Replace(None));
        // The bid, 6.6 bp below the lower top, is far from both: kept, even
        // though it is 9.4 bp (outside the band) from the higher top.
        let bid_q = resting("83813.4", "0.00596");
        assert_eq!(quote_action(Some(&bid_q), &bid), QuoteAction::Keep);
        // Mirror: the bid side frozen high, the live ask falling toward our
        // bid → the bid is cancelled once it is under 3 bp from the ask.
        let (bid, ask) = plan_quotes(&at("83892", "83830"), Decimal::ZERO, &p);
        assert_eq!(quote_action(Some(&bid_q), &bid), QuoteAction::Replace(None));
        let far_ask = resting("83990", "0.00596"); // 11.7 bp above the bid
        assert_eq!(quote_action(Some(&far_ask), &ask), QuoteAction::Keep);
        // Exactly at the guard distance is still kept (3 bp of 100000).
        let (bid, _) = plan_quotes(&at("100001", "100000"), Decimal::ZERO, &p);
        assert_eq!(
            quote_action(Some(&resting("99970", "0.00499")), &bid),
            QuoteAction::Keep
        );
        assert_eq!(
            quote_action(Some(&resting("99970.1", "0.00499")), &bid),
            QuoteAction::Replace(None)
        );

        // An unusable book (no positive price) suppresses everything.
        let (bid, ask) = plan_quotes(&at("0", "83438.5"), Decimal::ZERO, &p);
        assert_eq!(quote_action(Some(&rb), &bid), QuoteAction::Replace(None));
        assert_eq!(quote_action(Some(&ra), &ask), QuoteAction::Replace(None));
        // At offset 0 a crossed book cancels both quotes, as before.
        let (bid, ask) = plan_quotes(&crossed, Decimal::ZERO, &params());
        let touch_bid = resting("83438.6", "0.11385");
        let touch_ask = resting("83438.5", "0.11385");
        assert_eq!(
            quote_action(Some(&touch_bid), &bid),
            QuoteAction::Replace(None)
        );
        assert_eq!(
            quote_action(Some(&touch_ask), &ask),
            QuoteAction::Replace(None)
        );
    }

    #[test]
    fn a_pegged_quote_is_kept_inside_the_band_and_repegged_outside() {
        let p = presence_params();
        // The plan for a flat book whose touch on `side` is `touch`.
        let plan = |side: QSide, touch: &str| {
            let book = match side {
                QSide::Bid => Touches {
                    ask: d(touch) + d("0.1"),
                    ..at(touch, touch)
                },
                QSide::Ask => Touches {
                    bid: d(touch) - d("0.1"),
                    ..at(touch, touch)
                },
            };
            let (bid, ask) = plan_quotes(&book, Decimal::ZERO, &p);
            let plan = if side == QSide::Bid { bid } else { ask };
            assert_eq!(
                plan.price,
                priced(
                    offset_px(side, d(touch), d("5")),
                    PriceTol::Band {
                        touch: d(touch),
                        offset_bps: d("5"),
                        band_bps: d("2"),
                    }
                )
            );
            plan
        };
        let act = |r: &Resting, side: QSide, touch: &str| quote_action(Some(r), &plan(side, touch));
        let repeg = |side: QSide, touch: &str| {
            QuoteAction::Replace(Some(plan(side, touch).target().unwrap()))
        };
        // Bid resting 5 bp below a 83438.4 touch (as the connector rounded it).
        let bid = resting("83396.6", "0.00599");
        assert_eq!(act(&bid, QSide::Bid, "83438.4"), QuoteAction::Keep);
        // Touch moves AWAY (up): 5.9 bp is inside 3..7 → keep; 7.6 bp → re-peg.
        assert_eq!(act(&bid, QSide::Bid, "83446"), QuoteAction::Keep);
        assert_eq!(act(&bid, QSide::Bid, "83460"), repeg(QSide::Bid, "83460"));
        // Touch moves TOWARD the quote (down): 3.4 bp → keep; 2.8 bp → re-peg
        // back out to 5 bp, so a kept quote never reaches the touch.
        assert_eq!(act(&bid, QSide::Bid, "83425"), QuoteAction::Keep);
        assert_eq!(act(&bid, QSide::Bid, "83420"), repeg(QSide::Bid, "83420"));
        // Ask resting 5 bp above a 83438.5 touch: mirror image.
        let ask = resting("83480.3", "0.00599");
        assert_eq!(act(&ask, QSide::Ask, "83438.5"), QuoteAction::Keep);
        assert_eq!(act(&ask, QSide::Ask, "83431"), QuoteAction::Keep); // away: 5.9 bp
        assert_eq!(act(&ask, QSide::Ask, "83417"), repeg(QSide::Ask, "83417")); // away: 7.6 bp
        assert_eq!(act(&ask, QSide::Ask, "83452"), QuoteAction::Keep); // toward: 3.4 bp
        assert_eq!(act(&ask, QSide::Ask, "83457"), repeg(QSide::Ask, "83457")); // toward: 2.8 bp
                                                                                // In the band but the size is off by more than 1% → requote.
        let fat = resting("83396.6", "0.007");
        assert_eq!(act(&fat, QSide::Bid, "83446"), repeg(QSide::Bid, "83446"));
        // The other arms: place when nothing rests, cancel when suppressed.
        let fresh = plan(QSide::Bid, "83438.4");
        assert_eq!(
            quote_action(None, &fresh),
            QuoteAction::Place(fresh.target().unwrap())
        );
        let suppressed = SidePlan { qty: None, ..fresh };
        assert_eq!(
            quote_action(Some(&bid), &suppressed),
            QuoteAction::Replace(None)
        );
    }

    #[test]
    fn at_touch_quoting_keeps_the_exact_price_rule() {
        // Offset 0: both sides are planned at the raw touch with the Exact
        // rule, whatever the band or the peg references say, so the one
        // requote rule behaves exactly as before presence mode existed.
        let mut p = params();
        p.presence = presence("0", "5");
        let book = Touches {
            peg_bid: Some(d("99")),
            peg_ask: None,
            ..at("100.2", "100.3")
        };
        let (bid, ask) = plan_quotes(&book, Decimal::ZERO, &p);
        assert_eq!(bid.price, priced(d("100.2"), PriceTol::Exact));
        assert_eq!(ask.price, priced(d("100.3"), PriceTol::Exact));
        // 1 bp from the touch: would be inside a band, but must requote.
        let qty = bid.qty.unwrap().to_string();
        assert_eq!(
            quote_action(Some(&resting("100.19", &qty)), &bid),
            QuoteAction::Replace(Some(bid.target().unwrap()))
        );
        assert_eq!(
            quote_action(Some(&resting("100.2", &qty)), &bid),
            QuoteAction::Keep
        );
        // And at-touch plan_quotes never moves a price off the touch or caps
        // the reducing side at the inventory (that is presence-only): long
        // 0.05, the ask is still a full $10k clip at the touch.
        let (bid, ask) = targets(plan_quotes(&at("83438.4", "83438.5"), d("0.05"), &params()));
        assert_eq!(bid.unwrap().px, d("83438.4"));
        let ask = ask.unwrap();
        assert_eq!(ask.px, d("83438.5"));
        assert_eq!(ask.qty, d("0.11984"));
    }

    #[test]
    fn in_presence_mode_the_reducing_side_quotes_at_the_touch_sized_to_inventory() {
        // Review #3 / review 2 D: a sweep filled our deep bid → long 0.004.
        // plan_quotes alone decides the exit side: the ask (reducing) goes to
        // the RAW touch with exactly the inventory and the exact-price rule;
        // the bid (growing) rests 5 bp behind its peg with the band rule.
        let p = presence_params();
        let (bb, ba) = (d("83438.4"), d("83438.5"));
        let book = at("83438.4", "83438.5");
        let band = |touch: Decimal| PriceTol::Band {
            touch,
            offset_bps: d("5"),
            band_bps: d("2"),
        };
        let (bid, ask) = plan_quotes(&book, d("0.004"), &p);
        assert_eq!(ask.price, priced(ba, PriceTol::Exact));
        assert_eq!(ask.qty, Some(d("0.004")));
        assert_eq!(bid.price, priced(d("83396.6808"), band(bb)));
        // Short: the bid is the reducing side, at the touch, sized to cover.
        let (bid, ask) = plan_quotes(&book, d("-0.004"), &p);
        assert_eq!(bid.price, priced(bb, PriceTol::Exact));
        assert_eq!(bid.qty, Some(d("0.004")));
        assert_eq!(ask.price, priced(d("83480.21925"), band(ba)));
        // Inventory more than a clip (plus dust) above a clip: the exit quote
        // is still at most a clip ($500 / mid, rounded down).
        let (_, ask) = plan_quotes(&book, d("0.02"), &p);
        assert_eq!(ask.price, priced(ba, PriceTol::Exact));
        assert_eq!(ask.qty, Some(d("0.00599")));
        // Inventory a hair above a clip: the WHOLE inventory exits (the excess
        // 0.00001 = $0.83 is dust the venue would not take on its own).
        let (_, ask) = plan_quotes(&book, d("0.006"), &p);
        assert_eq!(ask.qty, Some(d("0.006")));
        // Inventory below min_quote_usd ($50) but not dust ($16.7 ≥ the $5
        // venue floor) is still worked out as a maker at the touch rather
        // than left to max-hold and a taker IOC (bot-strategy#1093 dust fix).
        let (_, ask) = plan_quotes(&book, d("0.0002"), &p);
        assert_eq!(ask.price, priced(ba, PriceTol::Exact));
        assert_eq!(ask.qty, Some(d("0.0002")));
        // Dust (0.00005 × 83438 = $4.17 < $5): no exit side, the ask stays at
        // the offset; the dust is carried as if flat.
        let (_, ask) = plan_quotes(&book, d("0.00005"), &p);
        assert_eq!(ask.price, priced(d("83480.21925"), band(ba)));
        // Flat: both sides at the offset.
        let (bid, ask) = plan_quotes(&book, Decimal::ZERO, &p);
        assert_eq!(bid.price, priced(d("83396.6808"), band(bb)));
        assert_eq!(ask.price, priced(d("83480.21925"), band(ba)));

        // ONE reference set: the exit side uses the raw touch even when its
        // peg differs (our own exit quote IS the best ask, peg = next level),
        // so the exit quote is kept instead of chasing the level behind it;
        // the offset side uses its peg; sizes come from the raw mid for both
        // (0.00599 = $500 / 83438.45, not $500 / a peg-based mid).
        let own_top = Touches {
            peg_bid: Some(d("83000")),
            peg_ask: Some(d("83450")),
            ..book
        };
        let (bid, ask) = plan_quotes(&own_top, d("0.004"), &p);
        assert_eq!(ask.price, priced(ba, PriceTol::Exact));
        assert_eq!(
            quote_action(Some(&resting("83438.5", "0.004")), &ask),
            QuoteAction::Keep
        );
        assert_eq!(
            bid.price,
            priced(offset_px(QSide::Bid, d("83000"), d("5")), band(d("83000")))
        );
        assert_eq!(bid.qty, Some(d("0.00599")));
        // The exit side needs no peg at all; the offset side without one
        // has no price.
        let no_pegs = Touches {
            peg_bid: None,
            peg_ask: None,
            ..book
        };
        let (bid, ask) = plan_quotes(&no_pegs, d("0.004"), &p);
        assert_eq!(ask.price, priced(ba, PriceTol::Exact));
        assert_eq!(bid.price, PricePlan::Unpriced);
    }

    #[test]
    fn only_paper_presence_reads_a_deep_book() {
        // Review #7: a deep virtual quote must find its level in the
        // snapshot to join behind the displayed size.
        assert_eq!(book_depth(true, true, false), 100);
        assert_eq!(book_depth(true, false, false), 10);
        assert_eq!(book_depth(false, true, false), 10);
        assert_eq!(book_depth(false, false, false), 10);
        // The quote gate needs 20 levels for its L2 features; a deeper paper
        // read stays deeper.
        assert_eq!(book_depth(false, false, true), 20);
        assert_eq!(book_depth(true, true, true), 100);
        // With the level in the snapshot the quote joins behind it; without
        // it the queue is 0 (approximate, see `sim::join`).
        let levels = vec![lv("83438.4", "0.5"), lv("83396.6", "1.25")];
        let deep = crate::sim::join(QSide::Bid, d("83396.6"), d("0.006"), &levels, 1);
        assert_eq!(deep.queue_ahead, d("1.25"));
        let off_book = crate::sim::join(QSide::Bid, d("83396.5"), d("0.006"), &levels, 1);
        assert_eq!(off_book.queue_ahead, Decimal::ZERO);
        // The paper quote price: rounded away from the touch onto a real
        // level when the tick is known, else the unrounded price (queue 0).
        use crate::sim::paper_px;
        assert_eq!(
            paper_px(QSide::Bid, d("83396.6808"), Some(d("0.1"))),
            d("83396.6")
        );
        assert_eq!(
            paper_px(QSide::Ask, d("83480.21925"), Some(d("0.1"))),
            d("83480.3")
        );
        assert_eq!(paper_px(QSide::Bid, d("83396.6808"), None), d("83396.6808"));
        let px = paper_px(QSide::Bid, d("83396.6808"), Some(d("0.1")));
        assert_eq!(
            crate::sim::join(QSide::Bid, px, d("0.006"), &levels, 1).queue_ahead,
            d("1.25")
        );
    }

    #[test]
    fn a_backoff_still_cancels_in_presence_mode_but_never_places() {
        // Review 3 #2. Which pass the quote section runs.
        assert_eq!(quote_pass(&TickPlan::Quote, true), Some(QuotePass::Full));
        assert_eq!(quote_pass(&TickPlan::Quote, false), Some(QuotePass::Full));
        assert_eq!(
            quote_pass(&TickPlan::Wait, true),
            Some(QuotePass::CancelOnly)
        );
        // At-touch quoting leaves its quotes alone while waiting, as before.
        assert_eq!(quote_pass(&TickPlan::Wait, false), None);
        assert_eq!(quote_pass(&TickPlan::PullQuotes("halt"), true), None);
        assert_eq!(
            quote_pass(&TickPlan::Flatten(FlattenReason::MaxHold), true),
            None
        );
        // The cancel-only form: cancels stay, a requote becomes a cancel,
        // nothing is placed or modified.
        let t = QuoteTarget {
            side: QSide::Ask,
            px: d("83480.3"),
            qty: d("0.00599"),
        };
        assert_eq!(cancel_only(QuoteAction::Keep), QuoteAction::Keep);
        assert_eq!(
            cancel_only(QuoteAction::Place(t.clone())),
            QuoteAction::Keep
        );
        assert_eq!(
            cancel_only(QuoteAction::Replace(None)),
            QuoteAction::Replace(None)
        );
        assert_eq!(
            cancel_only(QuoteAction::Replace(Some(t.clone()))),
            QuoteAction::Replace(None)
        );
        assert_eq!(
            cancel_only(QuoteAction::Modify(t)),
            QuoteAction::Replace(None)
        );
        // End to end: the frozen-ask book during a backoff. The ask that the
        // live bid has come too close to is cancelled; the bid stays; the
        // empty side gets nothing.
        let p = presence_params();
        let (bid, ask) = plan_quotes(&at("83892", "83868.8"), Decimal::ZERO, &p);
        let ask_q = resting("83902.8", "0.00596");
        let bid_q = resting("83813.4", "0.00596");
        assert_eq!(
            cancel_only(quote_action(Some(&ask_q), &ask)),
            QuoteAction::Replace(None)
        );
        assert_eq!(
            cancel_only(quote_action(Some(&bid_q), &bid)),
            QuoteAction::Keep
        );
        let (bid, _) = plan_quotes(&at("83438.4", "83438.5"), Decimal::ZERO, &p);
        let place = quote_action(None, &bid);
        assert!(matches!(place, QuoteAction::Place(_)));
        assert_eq!(cancel_only(place.clone()), QuoteAction::Keep);
        // What main executes: the full pass runs the action unchanged, the
        // cancel-only pass its cancel-only form.
        assert_eq!(QuotePass::Full.apply(place.clone()), place);
        assert_eq!(QuotePass::CancelOnly.apply(place), QuoteAction::Keep);
        let requote = QuoteAction::Replace(Some(bid.target().unwrap()));
        assert_eq!(QuotePass::Full.apply(requote.clone()), requote);
        assert_eq!(
            QuotePass::CancelOnly.apply(requote),
            QuoteAction::Replace(None)
        );
    }

    #[test]
    fn status_distances_use_the_band_reference_and_the_raw_touch() {
        // Review 3 #5. Our bid 83396.6 is the best bid; the band is judged
        // against the next level 83390.1 (−0.78 bp: through it), while the
        // raw touch is our own price (0 bp).
        let t = Touches {
            bid: d("83396.6"),
            ask: d("83438.5"),
            peg_bid: Some(d("83390.1")),
            peg_ask: Some(d("83438.5")),
        };
        let (peg, raw) = quote_dists(QSide::Bid, d("83396.6"), &t);
        assert_eq!(peg, Some(dist_bps(QSide::Bid, d("83396.6"), d("83390.1"))));
        assert!(peg.unwrap() < Decimal::ZERO);
        assert_eq!(raw, Decimal::ZERO);
        // Ask: no quote of ours at the top, both distances agree (5 bp).
        let (peg, raw) = quote_dists(QSide::Ask, d("83480.3"), &t);
        assert_eq!(peg, Some(raw));
        assert!(raw > d("5") && raw < d("5.02"));
        // Only our own quote displayed: no band reference, raw still there.
        let alone = Touches { peg_bid: None, ..t };
        assert_eq!(
            quote_dists(QSide::Bid, d("83396.6"), &alone),
            (None, Decimal::ZERO)
        );
    }

    #[test]
    fn quote_action_keeps_modifies_or_replaces() {
        let t = QuoteTarget {
            side: QSide::Bid,
            px: d("100.1"),
            qty: d("1"),
        };
        let exact = |t: &QuoteTarget| SidePlan {
            side: t.side,
            qty: Some(t.qty),
            price: priced(t.px, PriceTol::Exact),
        };
        let r = Resting {
            order_id: "o".into(),
            px: d("100.1"),
            qty: d("1"),
            filled: Decimal::ZERO,
        };
        assert_eq!(quote_action(Some(&r), &exact(&t)), QuoteAction::Keep);
        let moved = QuoteTarget {
            px: d("100.2"),
            ..t.clone()
        };
        // In-place modify is disabled (MODIFY_ENABLED = false, mainnet
        // rejected batch modify): a moved untouched quote is cancel + place.
        assert_eq!(
            quote_action(Some(&r), &exact(&moved)),
            QuoteAction::Replace(Some(moved.clone()))
        );
        let partly = Resting {
            filled: d("0.4"),
            ..r.clone()
        };
        assert_eq!(
            quote_action(Some(&partly), &exact(&t)),
            QuoteAction::Replace(Some(t.clone()))
        );
        let suppressed = SidePlan {
            qty: None,
            ..exact(&t)
        };
        assert_eq!(
            quote_action(Some(&r), &suppressed),
            QuoteAction::Replace(None)
        );
        assert_eq!(quote_action(None, &suppressed), QuoteAction::Keep);
        assert_eq!(
            quote_action(None, &exact(&t)),
            QuoteAction::Place(t.clone())
        );
        let tiny = QuoteTarget {
            qty: d("1.005"),
            ..t.clone()
        };
        assert_eq!(quote_action(Some(&r), &exact(&tiny)), QuoteAction::Keep);
    }

    #[test]
    fn shock_fires_on_range_over_the_window_only() {
        let mut h = VecDeque::new();
        h.push_back((0, d("100000")));
        h.push_back((1_500, d("100030")));
        h.push_back((2_000, d("100040")));
        // window 2s at t=2000: range 40/100000 = 4 bp ≤ 5
        assert!(!shock(&h, 2_000, 2_000, d("5")));
        h.push_back((2_100, d("100060")));
        // t=2100 window [100,2100]: 100030..100060 = 3 bp
        assert!(!shock(&h, 2_100, 2_000, d("5")));
        h.push_back((2_200, d("100090")));
        assert!(shock(&h, 2_200, 2_000, d("5")));
    }

    #[test]
    fn safety_actions_run_during_a_rate_limit_backoff() {
        let base = TickInputs {
            has_book: true,
            backoff: true,
            dms_armed: true,
            startup_reconciled: true,
            fills_synced: true,
            tape_ready: true,
            ..TickInputs::default()
        };
        // Nothing unsafe: wait out the backoff.
        assert_eq!(tick_plan(&base), TickPlan::Wait);
        // Flatten beats everything, including the backoff and a halt.
        let f = TickInputs {
            flatten: Some(FlattenReason::Halt("kill_switch".into())),
            halted: true,
            ..base.clone()
        };
        assert_eq!(
            tick_plan(&f),
            TickPlan::Flatten(FlattenReason::Halt("kill_switch".into()))
        );
        for (inputs, why) in [
            (
                TickInputs {
                    has_book: false,
                    ..base.clone()
                },
                "no_book",
            ),
            (
                TickInputs {
                    halted: true,
                    ..base.clone()
                },
                "halt",
            ),
            (
                TickInputs {
                    stale: true,
                    ..base.clone()
                },
                "stale_book",
            ),
            (
                TickInputs {
                    shock_or_cooldown: true,
                    ..base.clone()
                },
                "shock",
            ),
        ] {
            assert_eq!(tick_plan(&inputs), TickPlan::PullQuotes(why));
        }
        let clear = TickInputs {
            backoff: false,
            ..base
        };
        assert_eq!(tick_plan(&clear), TickPlan::Quote);
    }

    #[test]
    fn shutdown_cancels_then_harvests_and_persists_before_disarming() {
        let live = shutdown_steps(true);
        let at = |s: ShutdownStep| live.iter().position(|x| *x == s).unwrap();
        assert!(at(ShutdownStep::CancelAll) < at(ShutdownStep::VerifyNoneResting));
        assert!(at(ShutdownStep::VerifyNoneResting) < at(ShutdownStep::DisarmDms));
        assert!(at(ShutdownStep::CancelAll) < at(ShutdownStep::HarvestFills));
        assert!(at(ShutdownStep::HarvestFills) < at(ShutdownStep::Persist));
        assert!(at(ShutdownStep::Persist) < at(ShutdownStep::DisarmDms));
        assert_eq!(shutdown_steps(false), vec![ShutdownStep::Persist]);
    }

    #[test]
    fn book_without_feed_time_or_too_old_is_stale() {
        assert!(book_stale(None, 10_000, 5));
        assert!(!book_stale(Some(5_000), 10_000, 5));
        assert!(book_stale(Some(4_999), 10_000, 5));
    }

    /// bot-strategy#1093 live 2026-10-03 22:55Z (SPY-USD, presence 2 bp, clip
    /// $2,500): a 3.24483 short was worked out with a bid sized from the clip
    /// at the exit mid (2500 / 770.56 = 3.24437), leaving 0.00046 of dust
    /// that the venue (min 0.001) refused to flatten — 11,383 rejected IOCs,
    /// quotes pulled for ten hours.
    fn spy_params() -> QuoteParams {
        QuoteParams {
            clip_usd: d("2500"),
            skew_usd: d("2500"),
            cap_usd: d("4750"),
            min_quote_usd: d("50"),
            qty_decimals: 5,
            presence: presence("2", "1"),
            venue_min: VenueMin {
                min_order_qty: Some(d("0.001")),
                size_decimals: Some(7),
                dust_usd: d("5"),
            },
        }
    }

    #[test]
    fn a_forced_dust_override_lapses_when_the_quantity_is_an_order_again() {
        // Venue minimum unknown → the $5 notional floor decides: 0.0065 SPY is
        // dust at $750 ($4.88) and an order again at $800 ($5.20).
        let v = VenueMin {
            min_order_qty: None,
            size_decimals: None,
            dust_usd: d("5"),
        };
        let q = d("-0.0065");
        assert!(v.forced_dust_still_holds(q, q, d("750")));
        assert!(!v.forced_dust_still_holds(q, q, d("800")));
        // A different inventory (a fill landed on it) lapses the override.
        assert!(!v.forced_dust_still_holds(q, d("-3.2465"), d("750")));
        // With the venue minimum known, that minimum decides (it already
        // includes the notional floor at the current price).
        let known = VenueMin {
            min_order_qty: Some(d("0.0066")),
            ..v
        };
        assert!(known.forced_dust_still_holds(q, q, d("800")));
        let known = VenueMin {
            min_order_qty: Some(d("0.0064")),
            ..v
        };
        assert!(!known.forced_dust_still_holds(q, q, d("750")));
    }

    #[test]
    fn connector_errors_classify_independently_of_logging() {
        assert_eq!(
            classify_error(&DexError::RateLimited {
                until_unix: 1_790_000_000
            }),
            ErrorEffect::Backoff {
                until_ms: 1_790_000_000_000
            }
        );
        assert_eq!(
            classify_error(&DexError::RateLimited { until_unix: -5 }),
            ErrorEffect::Backoff { until_ms: 0 }
        );
        assert!(matches!(
            classify_error(&DexError::ReconciliationRequired {
                action: "x".into(),
                nonce: 1,
                detail: "y".into()
            }),
            ErrorEffect::Reconcile
        ));
        assert_eq!(
            classify_error(&DexError::InvalidInput {
                field: "size".into(),
                value: "0.00046 (rounded 0.00046) below Arcus minimum for SPY-USD".into()
            }),
            ErrorEffect::BelowMinimum
        );
        assert_eq!(
            classify_error(&DexError::InvalidInput {
                field: "price".into(),
                value: "negative".into()
            }),
            ErrorEffect::Other
        );
        // Only the connector's own pre-flight rejection (InvalidInput) means
        // dust; the same words in a venue/transport error are not trusted
        // (the order may have been sent).
        assert_eq!(
            classify_error(&DexError::Transient(
                "below Arcus minimum (gateway echoed)".into()
            )),
            ErrorEffect::Other
        );
        assert_eq!(
            classify_error(&DexError::Permanent("POST_ONLY order would cross".into())),
            ErrorEffect::Quiet
        );
    }

    #[test]
    fn the_exit_covers_the_whole_inventory_even_when_the_mid_moved() {
        let p = spy_params();
        // Entry sized at mid 770.455 (clip = 3.24483, the live ask at 770.62
        // sat 2 bp above); the exit is planned at mid 770.56, where a clip is
        // only 3.24437. The bid must be 3.24483.
        let entry_mid = d("770.455");
        let entry_qty =
            (p.clip_usd / entry_mid).round_dp_with_strategy(5, RoundingStrategy::ToZero);
        assert_eq!(entry_qty, d("3.24483"));
        let (bid, _) = plan_quotes(&at("770.555", "770.565"), -entry_qty, &p);
        assert_eq!(bid.price, priced(d("770.555"), PriceTol::Exact));
        assert_eq!(bid.qty, Some(d("3.24483")));
        // Carried dust plus a fresh fill: the exit covers both (venue step).
        let (bid, _) = plan_quotes(&at("770.555", "770.565"), d("-3.2452900"), &p);
        assert_eq!(bid.qty, Some(d("3.24529")));
        // Two clips short: a clip at a time, on the venue's 7-decimal step.
        let (bid, _) = plan_quotes(&at("770.555", "770.565"), d("-6.5"), &p);
        assert_eq!(bid.qty, Some(d("3.2443936")));
    }

    #[test]
    fn dust_inventory_neither_flattens_nor_blocks_quoting() {
        let p = spy_params();
        let mid = d("770.5");
        // The live residual: below the venue's 0.001 minimum.
        assert!(p.venue_min.is_dust(d("-0.00046"), mid));
        assert!(!p.venue_min.is_dust(d("-0.001"), mid));
        assert!(p.venue_min.is_dust(Decimal::ZERO, mid));
        // Without the venue minimum the $5 notional fallback decides.
        let unknown = VenueMin {
            min_order_qty: None,
            size_decimals: None,
            dust_usd: d("5"),
        };
        assert!(unknown.is_dust(d("-0.00046"), mid)); // $0.35
        assert!(!unknown.is_dust(d("0.007"), mid)); // $5.39
                                                    // No flatten for dust, whatever else is true (max-hold long past,
                                                    // halt in force, startup).
        for (halt, startup) in [(None, false), (Some("daily_stop"), false), (None, true)] {
            assert_eq!(
                flatten_reason(
                    d("-0.00046"),
                    Some(mid),
                    d("5000"),
                    Some(0),
                    10_000_000,
                    300,
                    halt,
                    startup,
                    false,
                    true
                ),
                None
            );
        }
        // The same inventory flagged non-dust (venue minimum unknown and a
        // higher-priced market) does flatten on max-hold.
        assert_eq!(
            flatten_reason(
                d("-0.00046"),
                Some(mid),
                d("5000"),
                Some(0),
                10_000_000,
                300,
                None,
                false,
                false,
                false
            ),
            Some(FlattenReason::MaxHold)
        );
        // Both sides quote at the offset as if flat.
        let (bid, ask) = plan_quotes(&at("770.49", "770.51"), d("-0.00046"), &p);
        assert_eq!(
            bid.price,
            priced(
                offset_px(QSide::Bid, d("770.49"), d("2")),
                PriceTol::Band {
                    touch: d("770.49"),
                    offset_bps: d("2"),
                    band_bps: d("1"),
                }
            )
        );
        assert_eq!(
            ask.price,
            priced(
                offset_px(QSide::Ask, d("770.51"), d("2")),
                PriceTol::Band {
                    touch: d("770.51"),
                    offset_bps: d("2"),
                    band_bps: d("1"),
                }
            )
        );
        assert!(bid.qty.is_some() && ask.qty.is_some());
        // Dust is in sync with itself on the venue (the position check never
        // sees a mismatch for a carried residual).
        assert_eq!(
            position_check(d("-0.00046"), Some(d("-0.00046")), true, None, 1_000, 5_000).0,
            PosCheck::InSync
        );
    }

    #[test]
    fn a_young_quote_missing_from_the_open_orders_read_stays_ours() {
        // 2026-10-06 00:00:00Z: quotes placed, the next tick's open-orders
        // read does not list them yet.
        let mut resting = HashMap::new();
        resting.insert(
            QSide::Bid,
            Resting {
                order_id: "b1".into(),
                px: d("774.0"),
                qty: d("3"),
                filled: Decimal::ZERO,
            },
        );
        resting.insert(
            QSide::Ask,
            Resting {
                order_id: "a1".into(),
                px: d("775.0"),
                qty: d("3"),
                filled: Decimal::ZERO,
            },
        );
        let mut placed = HashMap::new();
        placed.insert("b1".to_string(), 1_000u64);
        placed.insert("a1".to_string(), 1_000u64);
        let empty: HashSet<String> = HashSet::new();
        // 500 ms later: not visible yet → nothing forgotten (so the tick does
        // not place a second pair), nothing stray.
        let s = sweep_open_orders(&resting, &placed, &empty, 1_500, PLACE_VISIBILITY_GRACE_MS);
        assert_eq!(s, OrderSweep::default());
        // Visible on the next read: still ours, not stray.
        let both: HashSet<String> = ["b1".to_string(), "a1".to_string()].into();
        let s = sweep_open_orders(&resting, &placed, &both, 16_000, PLACE_VISIBILITY_GRACE_MS);
        assert_eq!(s, OrderSweep::default());
        // Gone (filled / cancelled by the venue) past the grace: forgotten.
        let only_ask: HashSet<String> = ["a1".to_string()].into();
        let s = sweep_open_orders(
            &resting,
            &placed,
            &only_ask,
            1_000 + PLACE_VISIBILITY_GRACE_MS,
            PLACE_VISIBILITY_GRACE_MS,
        );
        assert_eq!(s.forget, vec![QSide::Bid]);
        assert!(s.strays.is_empty());
        // Unknown placement time counts as old (startup / adopted state).
        let s = sweep_open_orders(
            &resting,
            &HashMap::new(),
            &only_ask,
            1_500,
            PLACE_VISIBILITY_GRACE_MS,
        );
        assert_eq!(s.forget, vec![QSide::Bid]);
        // A foreign / lost order on our market is a stray.
        let extra: HashSet<String> = ["b1".to_string(), "a1".to_string(), "x9".to_string()].into();
        let s = sweep_open_orders(&resting, &placed, &extra, 1_500, PLACE_VISIBILITY_GRACE_MS);
        assert!(s.forget.is_empty());
        assert_eq!(s.strays, vec!["x9".to_string()]);
    }
}
