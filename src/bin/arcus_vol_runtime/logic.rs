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

pub struct QuoteParams {
    pub clip_usd: Decimal,
    pub skew_usd: Decimal,
    /// Post-fill |inventory| must stay ≤ this (effective cap × headroom).
    pub cap_usd: Decimal,
    pub min_quote_usd: Decimal,
    pub qty_decimals: u32,
}

/// Quotes at the touch. A side is dropped when inventory already sits at the
/// skew on the side it would grow, and every quote is sized so its full fill
/// keeps |inventory| within `cap_usd`. `inv_qty` is signed base.
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
    // A bid fill moves inventory up by q: |inv + q| ≤ cap ⇔ q ≤ cap − inv.
    let bid = if inv_usd >= p.skew_usd {
        None
    } else {
        size(p.cap_usd - inv_usd).map(|qty| QuoteTarget {
            side: QSide::Bid,
            px: best_bid,
            qty,
        })
    };
    let ask = if -inv_usd >= p.skew_usd {
        None
    } else {
        size(p.cap_usd + inv_usd).map(|qty| QuoteTarget {
            side: QSide::Ask,
            px: best_ask,
            qty,
        })
    };
    (bid, ask)
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

/// Requote only when the price moved, or the size is off by more than 1%.
pub fn quote_action(resting: Option<&Resting>, target: Option<&QuoteTarget>) -> QuoteAction {
    match (resting, target) {
        (None, None) => QuoteAction::Keep,
        (None, Some(t)) => QuoteAction::Place(t.clone()),
        (Some(_), None) => QuoteAction::Replace(None),
        (Some(r), Some(t)) => {
            let open = r.qty - r.filled;
            let size_off = (open - t.qty).abs() > t.qty / Decimal::ONE_HUNDRED;
            if r.px == t.px && !size_off {
                QuoteAction::Keep
            } else if r.filled.is_zero() {
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
        assert_eq!(quote_action(Some(&r), Some(&t)), QuoteAction::Keep);
        let moved = QuoteTarget {
            px: d("100.2"),
            ..t.clone()
        };
        assert_eq!(
            quote_action(Some(&r), Some(&moved)),
            QuoteAction::Modify(moved.clone())
        );
        let partly = Resting {
            filled: d("0.4"),
            ..r.clone()
        };
        assert_eq!(
            quote_action(Some(&partly), Some(&t)),
            QuoteAction::Replace(Some(t.clone()))
        );
        assert_eq!(quote_action(Some(&r), None), QuoteAction::Replace(None));
        assert_eq!(quote_action(None, Some(&t)), QuoteAction::Place(t.clone()));
        let tiny = QuoteTarget {
            qty: d("1.005"),
            ..t.clone()
        };
        assert_eq!(quote_action(Some(&r), Some(&tiny)), QuoteAction::Keep);
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
