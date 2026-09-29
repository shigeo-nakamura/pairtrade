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
