//! Pure decision logic: what to quote, when to flatten and in which order,
//! book shocks. The async loop in `main.rs` only wires these to IO.

use dex_connector::OrderSide;
use rust_decimal::{Decimal, RoundingStrategy};
use std::collections::VecDeque;

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
/// kept can never be at or through the touch.
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
    /// The venue's price tick for this market at the current price (from
    /// the connector, see `usable_tick`). Needed only for an offset quote;
    /// `None` there means no quote (fail closed), never the touch.
    pub tick: Option<Decimal>,
}

const BPS: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);

/// The quote price `offset_bps` behind `touch` on `side`, rounded AWAY from
/// the touch to `tick` (bid down, ask up), so rounding can only make the
/// quote more passive. Offset 0 is the touch itself, unrounded. With an
/// offset but no positive tick there is NO price (`None`): an offset quote
/// must never silently become an at-touch quote.
pub fn offset_px(
    side: QSide,
    touch: Decimal,
    offset_bps: Decimal,
    tick: Option<Decimal>,
) -> Option<Decimal> {
    if offset_bps <= Decimal::ZERO {
        return Some(touch);
    }
    let tick = tick.filter(|t| *t > Decimal::ZERO)?;
    let frac = offset_bps / BPS;
    let px = match side {
        QSide::Bid => ((touch * (Decimal::ONE - frac)) / tick).floor() * tick,
        QSide::Ask => ((touch * (Decimal::ONE + frac)) / tick).ceil() * tick,
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

/// The tick an offset quote may be rounded to. The venue's tick (read from
/// the connector at `read_at_ms`) is the source of truth and expires after
/// `max_age_ms` (tick tiers depend on price). `override_tick`
/// (`ARCUS_VOL_PRICE_TICK`) can only make it coarser: it is used only when
/// it is a positive multiple of the venue tick. No fresh venue tick, or an
/// override that is not such a multiple, gives `None` → no offset quote.
pub fn usable_tick(
    venue: Option<(Decimal, u64)>,
    override_tick: Option<Decimal>,
    now_ms: u64,
    max_age_ms: u64,
) -> Option<Decimal> {
    let (tick, read_at_ms) = venue?;
    if tick <= Decimal::ZERO || now_ms.saturating_sub(read_at_ms) > max_age_ms {
        return None;
    }
    match override_tick {
        None => Some(tick),
        Some(o) if o > Decimal::ZERO && (o % tick).is_zero() => Some(o),
        Some(_) => None,
    }
}

/// Book levels to read per side. Paper presence quotes join the queue
/// displayed at a deep price, so the sim needs that level in the snapshot
/// (100 = the venue's maximum); everything else needs only the top.
pub fn book_depth(dry_run: bool, presence_on: bool) -> usize {
    if dry_run && presence_on {
        100
    } else {
        10
    }
}

/// The best price on one side of the book that is NOT our own resting quote
/// (live presence quoting): pegging off a touch our own order makes would
/// measure a distance of 0 and walk the quote outward every tick. `levels`
/// is best-first `(price, displayed size)`; `own` is our resting quote's
/// `(price, open size)` on that side. The best level counts as ours when its
/// price equals ours and it shows no more than our open size (+1%); the
/// next level is then the reference. `None` = nothing but our own quote is
/// displayed: the caller keeps the quote where it is.
pub fn peg_touch(
    levels: &[(Decimal, Decimal)],
    own: Option<(Decimal, Decimal)>,
) -> Option<Decimal> {
    let (best_px, best_size) = *levels.first()?;
    let Some((own_px, own_open)) = own else {
        return Some(best_px);
    };
    let only_ours = best_px == own_px && best_size <= own_open + own_open / Decimal::ONE_HUNDRED;
    if only_ours {
        levels.get(1).map(|(px, _)| *px)
    } else {
        Some(best_px)
    }
}

/// Quotes at the touch, or (presence quoting) `offset_bps` behind it. A side
/// is dropped when inventory already sits at the skew on the side it would
/// grow, and every quote is sized so its full fill keeps |inventory| within
/// `cap_usd`. `inv_qty` is signed base. `best_bid` / `best_ask` are the
/// reference touches (for an offset side: `peg_touch`, i.e. excluding our
/// own quote).
///
/// In presence mode the inventory-reducing side quotes AT the touch, sized
/// to the inventory only (never more, so working a sweep fill out cannot
/// flip the position and start at-touch churn). Inventory too small to
/// quote (< `min_quote_usd`) stays at the offset and is left to max-hold.
/// An offset side with no usable tick gets no quote.
pub fn plan_quotes(
    best_bid: Decimal,
    best_ask: Decimal,
    inv_qty: Decimal,
    p: &QuoteParams,
) -> (Option<QuoteTarget>, Option<QuoteTarget>) {
    if best_bid <= Decimal::ZERO || best_ask <= best_bid {
        return (None, None);
    }
    let mid = (best_bid + best_ask) / Decimal::TWO;
    let inv_usd = inv_qty * mid;
    let size = |room_usd: Decimal| -> Option<Decimal> {
        let usd = p.clip_usd.min(room_usd);
        if usd < p.min_quote_usd {
            return None;
        }
        let qty = (usd / mid).round_dp_with_strategy(p.qty_decimals, RoundingStrategy::ToZero);
        (qty > Decimal::ZERO).then_some(qty)
    };
    let target = |side: QSide, touch: Decimal, qty: Decimal| -> Option<QuoteTarget> {
        let (offset, qty) = side_quote(side, inv_qty, qty, mid, p);
        offset_px(side, touch, offset, p.tick).map(|px| QuoteTarget { side, px, qty })
    };
    // A bid fill moves inventory up by q: |inv + q| ≤ cap ⇔ q ≤ cap − inv.
    let bid = if inv_usd >= p.skew_usd {
        None
    } else {
        size(p.cap_usd - inv_usd).and_then(|qty| target(QSide::Bid, best_bid, qty))
    };
    let ask = if -inv_usd >= p.skew_usd {
        None
    } else {
        size(p.cap_usd + inv_usd).and_then(|qty| target(QSide::Ask, best_ask, qty))
    };
    (bid, ask)
}

/// Presence mode only: the side that works existing inventory out AT the
/// touch, and how much (the inventory, rounded down to the size step). The
/// side that reduces a non-zero inventory exits at the touch, so a sweep
/// fill is worked out as a maker instead of waiting for max-hold and paying
/// taker. `None` when presence is off, flat, or the inventory is too small
/// to quote (< `min_quote_usd`): then both sides rest at the offset.
pub fn exit_quote(inv_qty: Decimal, mid: Decimal, p: &QuoteParams) -> Option<(QSide, Decimal)> {
    if !p.presence.on() || inv_qty.is_zero() {
        return None;
    }
    let qty = inv_qty
        .abs()
        .round_dp_with_strategy(p.qty_decimals, RoundingStrategy::ToZero);
    if qty * mid < p.min_quote_usd {
        return None;
    }
    let side = if inv_qty > Decimal::ZERO {
        QSide::Ask
    } else {
        QSide::Bid
    };
    Some((side, qty))
}

/// `(offset_bps, qty)` a side actually quotes with. Outside presence mode:
/// offset 0 and the clip-sized `qty`, unchanged. In presence mode the exit
/// side (`exit_quote`) goes to the touch with at most the inventory; every
/// other side rests at the offset.
fn side_quote(
    side: QSide,
    inv_qty: Decimal,
    qty: Decimal,
    mid: Decimal,
    p: &QuoteParams,
) -> (Decimal, Decimal) {
    match exit_quote(inv_qty, mid, p) {
        Some((exit_side, exit_qty)) if exit_side == side => (Decimal::ZERO, qty.min(exit_qty)),
        _ => (p.presence.offset_bps, qty),
    }
}

/// How a side's resting price is compared with its target this tick: the
/// same choice `plan_quotes` made for the price (`touch` is the same
/// reference touch that was passed to it).
pub fn side_tolerance(
    target: Option<&QuoteTarget>,
    touch: Decimal,
    presence: &Presence,
) -> PriceTol {
    // An offset price is strictly behind the touch (rounded away from it),
    // so a target AT the touch is the exit side's at-touch quote.
    let at_touch = target.is_some_and(|t| t.px == touch);
    if !presence.on() || at_touch {
        PriceTol::Exact
    } else {
        PriceTol::Band {
            touch,
            offset_bps: presence.offset_bps,
            band_bps: presence.band_bps,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FlattenReason {
    Cap,
    MaxHold,
    Halt(String),
    Startup,
}

/// Flatten when inventory reaches the effective cap, has been open longer
/// than `max_hold_secs`, or a halt is in force.
///
/// Startup, halt (incl. KILL_SWITCH) and max-hold need no book: side and
/// size come from the position, and the live IOC prices off the connector's
/// own view (Codex P1, pairtrade#361). Only the cap check needs a mid.
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
) -> Option<FlattenReason> {
    if inv_qty.is_zero() {
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
    if opened_at_ms.is_some_and(|t| now_ms.saturating_sub(t) > max_hold_secs * 1_000) {
        return Some(FlattenReason::MaxHold);
    }
    None
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

/// One side's action this tick. `reference` is the touch that side was
/// planned against (`peg_touch` for an offset side); `None` means nothing
/// but our own quote is displayed there, and the quote is kept where it is
/// (re-pegging off our own price would walk it outward).
pub fn side_action(
    resting: Option<&Resting>,
    target: Option<&QuoteTarget>,
    reference: Option<Decimal>,
    presence: &Presence,
) -> QuoteAction {
    match reference {
        None => QuoteAction::Keep,
        Some(touch) => quote_action(resting, target, &side_tolerance(target, touch, presence)),
    }
}

/// When a resting quote's price still counts as "on target".
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PriceTol {
    /// At-touch quoting: only the exact target price.
    Exact,
    /// Presence quoting: the target price, or anywhere whose distance from
    /// `touch` (the reference touch on the quote's side) lies within
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

/// The one requote rule: keep a resting quote while its price is on target
/// (`tol`) and its open size is within 1% of the target; otherwise requote
/// (cancel + place while `MODIFY_ENABLED` is off).
pub fn quote_action(
    resting: Option<&Resting>,
    target: Option<&QuoteTarget>,
    tol: &PriceTol,
) -> QuoteAction {
    match (resting, target) {
        (None, None) => QuoteAction::Keep,
        (None, Some(t)) => QuoteAction::Place(t.clone()),
        (Some(_), None) => QuoteAction::Replace(None),
        (Some(r), Some(t)) => {
            let open = r.qty - r.filled;
            let size_off = (open - t.qty).abs() > t.qty / Decimal::ONE_HUNDRED;
            if tol.holds(t.side, r.px, t.px) && !size_off {
                QuoteAction::Keep
            } else if MODIFY_ENABLED && r.filled.is_zero() {
                QuoteAction::Modify(t.clone())
            } else {
                QuoteAction::Replace(Some(t.clone()))
            }
        }
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
    /// Presence quoting is on but the venue's price tick is not known (or
    /// too old): an offset price cannot be formed, so nothing is quoted.
    pub no_tick: bool,
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
    if i.no_tick {
        return TickPlan::PullQuotes("no_tick");
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
    pub no_tick: bool,
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
        no_tick: s.no_tick,
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
            tick: None,
        }
    }

    #[test]
    fn flat_quotes_both_sides_sized_to_the_cap_headroom() {
        let (bid, ask) = plan_quotes(d("83642.9"), d("83643.0"), Decimal::ZERO, &params());
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
        let (bid, ask) = plan_quotes(d("83642.9"), d("83643.0"), inv, &p);
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
        let (bid, ask) = plan_quotes(d("83642.9"), d("83643.0"), inv, &params());
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
        let (bid, ask) = plan_quotes(d("83642.9"), d("83643.0"), inv, &params());
        assert!(bid.is_none());
        assert!(ask.is_some());
    }

    #[test]
    fn crossed_or_empty_book_quotes_nothing() {
        assert_eq!(
            plan_quotes(d("100"), d("100"), Decimal::ZERO, &params()),
            (None, None)
        );
        assert_eq!(
            plan_quotes(Decimal::ZERO, d("100"), Decimal::ZERO, &params()),
            (None, None)
        );
    }

    #[test]
    fn flatten_triggers_on_cap_max_hold_and_halt() {
        let cap = d("10000");
        let m = Some(d("83642.95"));
        let fr = |inv: &str, mid: Option<Decimal>, now: u64, halt: Option<&str>, startup: bool| {
            flatten_reason(d(inv), mid, cap, Some(0), now, 300, halt, startup)
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
    fn halt_kill_startup_and_max_hold_flatten_without_a_book() {
        let cap = d("10000");
        let fr = |now: u64, halt: Option<&str>, startup: bool| {
            flatten_reason(d("-0.12"), None, cap, Some(0), now, 300, halt, startup)
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

    #[test]
    fn offset_price_rounds_away_from_the_touch_and_fails_closed_without_a_tick() {
        // Presence quoting (bot-strategy#1093 B). 83438.4 × 0.9995 =
        // 83396.6808 → bid DOWN to 83396.6; 83438.5 × 1.0005 = 83480.21925
        // → ask UP to 83480.3. Rounding only ever makes the quote more passive.
        let tick = Some(d("0.1"));
        assert_eq!(
            offset_px(QSide::Bid, d("83438.4"), d("5"), tick),
            Some(d("83396.6"))
        );
        assert_eq!(
            offset_px(QSide::Ask, d("83438.5"), d("5"), tick),
            Some(d("83480.3"))
        );
        // Already on the tick: not pushed a further tick away.
        assert_eq!(
            offset_px(QSide::Bid, d("100000"), d("5"), tick),
            Some(d("99950"))
        );
        assert_eq!(
            offset_px(QSide::Ask, d("100000"), d("5"), tick),
            Some(d("100050"))
        );
        // Offset 0 is the touch itself, with or without a tick.
        assert_eq!(
            offset_px(QSide::Bid, d("84600.13"), Decimal::ZERO, None),
            Some(d("84600.13"))
        );
        assert_eq!(
            offset_px(QSide::Ask, d("84600.13"), Decimal::ZERO, tick),
            Some(d("84600.13"))
        );
        // Fail closed (review #5): an offset with no / zero / negative tick
        // has NO price. It must never fall back to the touch.
        for bad in [None, Some(Decimal::ZERO), Some(d("-0.1"))] {
            assert_eq!(offset_px(QSide::Bid, d("83438.4"), d("5"), bad), None);
            assert_eq!(offset_px(QSide::Ask, d("83438.5"), d("5"), bad), None);
        }
        // Distance is measured from the quote's own side of the touch.
        assert!(dist_bps(QSide::Bid, d("83396.6"), d("83438.4")) > d("5"));
        assert!(dist_bps(QSide::Bid, d("83396.6"), d("83438.4")) < d("5.02"));
        assert!(dist_bps(QSide::Ask, d("83480.3"), d("83438.5")) > d("5"));
        assert!(dist_bps(QSide::Ask, d("83438.4"), d("83438.5")) < Decimal::ZERO);

        // plan_quotes: offset 0 quotes the touch; an offset moves both sides
        // when flat; an offset without a tick quotes NOTHING.
        let mut p = params();
        let (bid, ask) = plan_quotes(d("83438.4"), d("83438.5"), Decimal::ZERO, &p);
        assert_eq!(bid.unwrap().px, d("83438.4"));
        assert_eq!(ask.unwrap().px, d("83438.5"));
        p.presence = presence("5", "2");
        p.tick = tick;
        let (bid, ask) = plan_quotes(d("83438.4"), d("83438.5"), Decimal::ZERO, &p);
        assert_eq!(bid.unwrap().px, d("83396.6"));
        assert_eq!(ask.unwrap().px, d("83480.3"));
        p.tick = None;
        assert_eq!(
            plan_quotes(d("83438.4"), d("83438.5"), Decimal::ZERO, &p),
            (None, None)
        );
    }

    #[test]
    fn the_venue_tick_is_the_source_and_an_override_must_be_its_multiple() {
        // Review #2. Venue tick 0.1 read at t=1000, valid for 300 s.
        let venue = Some((d("0.1"), 1_000u64));
        assert_eq!(usable_tick(venue, None, 2_000, 300_000), Some(d("0.1")));
        // Too old (tick tiers depend on price) → no tick.
        assert_eq!(usable_tick(venue, None, 301_001, 300_000), None);
        // Never read, or a nonsense venue tick → no tick, override or not.
        assert_eq!(usable_tick(None, Some(d("0.5")), 2_000, 300_000), None);
        assert_eq!(
            usable_tick(Some((Decimal::ZERO, 1_000)), None, 2_000, 300_000),
            None
        );
        // The override may only be a coarser multiple of the venue tick.
        assert_eq!(
            usable_tick(venue, Some(d("0.5")), 2_000, 300_000),
            Some(d("0.5"))
        );
        assert_eq!(
            usable_tick(venue, Some(d("0.1")), 2_000, 300_000),
            Some(d("0.1"))
        );
        assert_eq!(usable_tick(venue, Some(d("0.25")), 2_000, 300_000), None);
        assert_eq!(usable_tick(venue, Some(d("0.01")), 2_000, 300_000), None);
        assert_eq!(
            usable_tick(venue, Some(Decimal::ZERO), 2_000, 300_000),
            None
        );
    }

    #[test]
    fn the_peg_reference_excludes_our_own_resting_quote() {
        // Review #1 (live self-reference). Our bid 83396.6 × 0.006 became
        // the best bid; pegging off it would measure 0 bp and walk it out.
        let levels = [(d("83396.6"), d("0.006")), (d("83390.1"), d("1.2"))];
        let own = Some((d("83396.6"), d("0.006")));
        assert_eq!(peg_touch(&levels, own), Some(d("83390.1")));
        // Within the 1% size tolerance it is still only us.
        let dusty = [(d("83396.6"), d("0.00605")), (d("83390.1"), d("1.2"))];
        assert_eq!(peg_touch(&dusty, own), Some(d("83390.1")));
        // Someone else rests at our price too: that level is a real touch.
        let shared = [(d("83396.6"), d("0.5")), (d("83390.1"), d("1.2"))];
        assert_eq!(peg_touch(&shared, own), Some(d("83396.6")));
        // Our quote is behind the best: the best is the reference.
        let behind = [(d("83438.4"), d("0.4")), (d("83396.6"), d("0.006"))];
        assert_eq!(peg_touch(&behind, own), Some(d("83438.4")));
        // Partly filled: only the open size is ours.
        let partly = [(d("83396.6"), d("0.004"))];
        assert_eq!(peg_touch(&partly, Some((d("83396.6"), d("0.004")))), None);
        // Nothing but our own quote is displayed → no reference (keep it).
        assert_eq!(peg_touch(&levels[..1], own), None);
        // No quote of ours (DRY_RUN, or nothing resting): the plain best.
        assert_eq!(peg_touch(&levels, None), Some(d("83396.6")));
        assert_eq!(peg_touch(&[], own), None);

        // With no reference the quote is KEPT, not cancelled or re-pegged;
        // with the next level as reference it is judged against that level.
        let pres = presence("5", "2");
        let resting = Resting {
            order_id: "b".into(),
            px: d("83396.6"),
            qty: d("0.006"),
            filled: Decimal::ZERO,
        };
        let far = QuoteTarget {
            side: QSide::Bid,
            px: d("83000"),
            qty: d("0.006"),
        };
        assert_eq!(
            side_action(Some(&resting), Some(&far), None, &pres),
            QuoteAction::Keep
        );
        assert_eq!(
            side_action(Some(&resting), None, None, &pres),
            QuoteAction::Keep
        );
        // Reference 83390.1 puts our 83396.6 bid THROUGH it (−0.8 bp): out
        // of the 3..7 bp band → re-peg behind the real touch.
        let t = QuoteTarget {
            side: QSide::Bid,
            px: offset_px(QSide::Bid, d("83390.1"), d("5"), Some(d("0.1"))).unwrap(),
            qty: d("0.006"),
        };
        assert_eq!(
            side_action(Some(&resting), Some(&t), Some(d("83390.1")), &pres),
            QuoteAction::Replace(Some(t.clone()))
        );
    }

    #[test]
    fn a_pegged_quote_is_kept_inside_the_band_and_repegged_outside() {
        let pres = presence("5", "2");
        let tick = Some(d("0.1"));
        let target = |side: QSide, touch: &str| QuoteTarget {
            side,
            px: offset_px(side, d(touch), pres.offset_bps, tick).unwrap(),
            qty: d("0.024"),
        };
        let act = |r: &Resting, side: QSide, touch: &str| {
            let t = target(side, touch);
            let tol = side_tolerance(Some(&t), d(touch), &pres);
            assert!(matches!(tol, PriceTol::Band { .. }));
            quote_action(Some(r), Some(&t), &tol)
        };
        // Bid placed 5 bp below a 83438.4 touch.
        let bid = Resting {
            order_id: "b".into(),
            px: d("83396.6"),
            qty: d("0.024"),
            filled: Decimal::ZERO,
        };
        assert_eq!(act(&bid, QSide::Bid, "83438.4"), QuoteAction::Keep);
        // Touch moves AWAY (up): 5.9 bp is inside 3..7 → keep; 7.6 bp → re-peg.
        assert_eq!(act(&bid, QSide::Bid, "83446"), QuoteAction::Keep);
        assert_eq!(
            act(&bid, QSide::Bid, "83460"),
            QuoteAction::Replace(Some(target(QSide::Bid, "83460")))
        );
        // Touch moves TOWARD the quote (down): 3.4 bp → keep; 2.8 bp → re-peg
        // back out to 5 bp, so a kept quote never reaches the touch.
        assert_eq!(act(&bid, QSide::Bid, "83425"), QuoteAction::Keep);
        assert_eq!(
            act(&bid, QSide::Bid, "83420"),
            QuoteAction::Replace(Some(target(QSide::Bid, "83420")))
        );
        // Ask placed 5 bp above a 83438.5 touch: mirror image.
        let ask = Resting {
            order_id: "a".into(),
            px: d("83480.3"),
            qty: d("0.024"),
            filled: Decimal::ZERO,
        };
        assert_eq!(act(&ask, QSide::Ask, "83438.5"), QuoteAction::Keep);
        assert_eq!(act(&ask, QSide::Ask, "83431"), QuoteAction::Keep); // away: 5.9 bp
        assert_eq!(
            act(&ask, QSide::Ask, "83417"),
            QuoteAction::Replace(Some(target(QSide::Ask, "83417"))) // away: 7.6 bp
        );
        assert_eq!(act(&ask, QSide::Ask, "83452"), QuoteAction::Keep); // toward: 3.4 bp
        assert_eq!(
            act(&ask, QSide::Ask, "83457"),
            QuoteAction::Replace(Some(target(QSide::Ask, "83457"))) // toward: 2.8 bp
        );
        // In the band but the size is off by more than 1% → re-peg.
        let resized = QuoteTarget {
            qty: d("0.02"),
            ..target(QSide::Bid, "83446")
        };
        let tol = side_tolerance(Some(&resized), d("83446"), &pres);
        assert_eq!(
            quote_action(Some(&bid), Some(&resized), &tol),
            QuoteAction::Replace(Some(resized.clone()))
        );
        // The other arms are unchanged: place when nothing rests, cancel
        // when the side is suppressed.
        let t = target(QSide::Bid, "83438.4");
        assert_eq!(quote_action(None, Some(&t), &tol), QuoteAction::Place(t));
        assert_eq!(
            quote_action(Some(&bid), None, &tol),
            QuoteAction::Replace(None)
        );
    }

    #[test]
    fn at_touch_quoting_keeps_the_exact_price_rule() {
        // Offset 0: the tolerance is Exact whatever the band says, so the
        // one requote rule behaves exactly as before presence mode existed.
        let off = presence("0", "5");
        let r = Resting {
            order_id: "o".into(),
            px: d("100.19"),
            qty: d("1"),
            filled: Decimal::ZERO,
        };
        // 1 bp from the touch: would be inside a band, but must requote.
        let moved = QuoteTarget {
            side: QSide::Bid,
            px: d("100.2"),
            qty: d("1"),
        };
        let tol = side_tolerance(Some(&moved), d("100.2"), &off);
        assert_eq!(tol, PriceTol::Exact);
        // Exact because presence is off, not merely because the target sits
        // at the touch: also with a reference that differs from the target.
        assert_eq!(
            side_tolerance(Some(&moved), d("100.3"), &off),
            PriceTol::Exact
        );
        assert_eq!(side_tolerance(None, d("100.3"), &off), PriceTol::Exact);
        assert_eq!(
            quote_action(Some(&r), Some(&moved), &tol),
            QuoteAction::Replace(Some(moved.clone()))
        );
        // And at-touch plan_quotes never moves a price off the touch or caps
        // the reducing side at the inventory (that is presence-only): long
        // 0.05, the ask is still a full $10k clip at the touch.
        let p = params();
        let (bid, ask) = plan_quotes(d("83438.4"), d("83438.5"), d("0.05"), &p);
        assert_eq!(bid.unwrap().px, d("83438.4"));
        let ask = ask.unwrap();
        assert_eq!(ask.px, d("83438.5"));
        assert_eq!(ask.qty, d("0.11984"));
    }

    #[test]
    fn in_presence_mode_the_reducing_side_quotes_at_the_touch_sized_to_inventory() {
        // Review #3: a sweep filled our deep bid → long 0.006. The ask (the
        // reducing side) goes to the touch with exactly the inventory and the
        // exact-price rule; the bid (growing side) stays 5 bp out.
        let mut p = params();
        p.clip_usd = d("500");
        p.skew_usd = d("2000");
        p.cap_usd = d("2000");
        p.presence = presence("5", "2");
        p.tick = Some(d("0.1"));
        let (bb, ba) = (d("83438.4"), d("83438.5"));
        let (bid, ask) = plan_quotes(bb, ba, d("0.004"), &p);
        let (bid, ask) = (bid.unwrap(), ask.unwrap());
        assert_eq!(ask.px, ba);
        assert_eq!(ask.qty, d("0.004"));
        assert_eq!(bid.px, d("83396.6"));
        assert_eq!(side_tolerance(Some(&ask), ba, &p.presence), PriceTol::Exact);
        assert!(matches!(
            side_tolerance(Some(&bid), bb, &p.presence),
            PriceTol::Band { .. }
        ));
        // Short: the bid is the reducing side, at the touch, sized to cover.
        let (bid, ask) = plan_quotes(bb, ba, d("-0.004"), &p);
        let (bid, ask) = (bid.unwrap(), ask.unwrap());
        assert_eq!(bid.px, bb);
        assert_eq!(bid.qty, d("0.004"));
        assert_eq!(ask.px, d("83480.3"));
        // Inventory above one clip: the exit quote is still at most a clip.
        let (_, ask) = plan_quotes(bb, ba, d("0.02"), &p);
        let ask = ask.unwrap();
        assert_eq!(ask.px, ba);
        assert_eq!(ask.qty, d("0.00599")); // $500 / mid, rounded down
                                           // Dust below min_quote_usd ($50) cannot be worked as a maker: the
                                           // side stays at the offset (max-hold deals with the dust).
        let (_, ask) = plan_quotes(bb, ba, d("0.0002"), &p);
        assert_eq!(ask.unwrap().px, d("83480.3"));
        let mid = (bb + ba) / Decimal::TWO;
        assert_eq!(
            exit_quote(d("0.004"), mid, &p),
            Some((QSide::Ask, d("0.004")))
        );
        assert_eq!(
            exit_quote(d("-0.004"), mid, &p),
            Some((QSide::Bid, d("0.004")))
        );
        assert_eq!(exit_quote(d("0.0002"), mid, &p), None);
        assert_eq!(exit_quote(Decimal::ZERO, mid, &p), None);
        assert_eq!(exit_quote(d("0.004"), mid, &params()), None); // presence off
                                                                  // Flat: both sides at the offset.
        let (bid, ask) = plan_quotes(bb, ba, Decimal::ZERO, &p);
        assert_eq!(bid.unwrap().px, d("83396.6"));
        assert_eq!(ask.unwrap().px, d("83480.3"));
        // The exit side needs no tick (it is at the touch); the offset side
        // without one is not quoted at all.
        p.tick = None;
        let (bid, ask) = plan_quotes(bb, ba, d("0.004"), &p);
        assert!(bid.is_none());
        assert_eq!(ask.unwrap().px, ba);
    }

    #[test]
    fn only_paper_presence_reads_a_deep_book() {
        // Review #7: a deep virtual quote must find its level in the
        // snapshot to join behind the displayed size.
        assert_eq!(book_depth(true, true), 100);
        assert_eq!(book_depth(true, false), 10);
        assert_eq!(book_depth(false, true), 10);
        assert_eq!(book_depth(false, false), 10);
        // With the level in the snapshot the quote joins behind it; without
        // it the queue is 0 (approximate, see `sim::join`).
        use dex_connector::OrderBookLevel;
        let levels = vec![
            OrderBookLevel {
                price: d("83438.4"),
                size: d("0.5"),
            },
            OrderBookLevel {
                price: d("83396.6"),
                size: d("1.25"),
            },
        ];
        let deep = crate::sim::join(QSide::Bid, d("83396.6"), d("0.006"), &levels, 1);
        assert_eq!(deep.queue_ahead, d("1.25"));
        let off_book = crate::sim::join(QSide::Bid, d("83396.5"), d("0.006"), &levels, 1);
        assert_eq!(off_book.queue_ahead, Decimal::ZERO);
    }

    #[test]
    fn presence_without_a_tick_pulls_quotes() {
        let ok = live_state();
        assert_eq!(tick_plan(&plan_inputs(&ok, 2_000)), TickPlan::Quote);
        let s = PlanState {
            no_tick: true,
            ..live_state()
        };
        assert_eq!(
            tick_plan(&plan_inputs(&s, 2_000)),
            TickPlan::PullQuotes("no_tick")
        );
        // A flatten still comes first.
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
    fn quote_action_keeps_modifies_or_replaces() {
        let t = QuoteTarget {
            side: QSide::Bid,
            px: d("100.1"),
            qty: d("1"),
        };
        let r = Resting {
            order_id: "o".into(),
            px: d("100.1"),
            qty: d("1"),
            filled: Decimal::ZERO,
        };
        assert_eq!(
            quote_action(Some(&r), Some(&t), &PriceTol::Exact),
            QuoteAction::Keep
        );
        let moved = QuoteTarget {
            px: d("100.2"),
            ..t.clone()
        };
        // In-place modify is disabled (MODIFY_ENABLED = false, mainnet
        // rejected batch modify): a moved untouched quote is cancel + place.
        assert_eq!(
            quote_action(Some(&r), Some(&moved), &PriceTol::Exact),
            QuoteAction::Replace(Some(moved.clone()))
        );
        let partly = Resting {
            filled: d("0.4"),
            ..r.clone()
        };
        assert_eq!(
            quote_action(Some(&partly), Some(&t), &PriceTol::Exact),
            QuoteAction::Replace(Some(t.clone()))
        );
        assert_eq!(
            quote_action(Some(&r), None, &PriceTol::Exact),
            QuoteAction::Replace(None)
        );
        assert_eq!(
            quote_action(None, Some(&t), &PriceTol::Exact),
            QuoteAction::Place(t.clone())
        );
        let tiny = QuoteTarget {
            qty: d("1.005"),
            ..t.clone()
        };
        assert_eq!(
            quote_action(Some(&r), Some(&tiny), &PriceTol::Exact),
            QuoteAction::Keep
        );
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
}
