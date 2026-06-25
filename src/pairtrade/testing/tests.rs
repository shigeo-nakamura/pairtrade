use super::state::PendingLeg;
use super::util::round_price_by_tick;
use super::util::{
    enforce_post_only_passive, quantize_size_by_step, quantize_size_by_step_ceiling,
};
use super::*;
use rust_decimal::Decimal;
use std::str::FromStr;

fn dec(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

#[test]
fn round_price_by_tick_rounds_long_down() {
    let price = dec("100.123");
    let step = dec("0.01");
    let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Long);
    assert_eq!(quantized, dec("100.12"));
}

#[test]
fn round_price_by_tick_rounds_short_up() {
    let price = dec("100.123");
    let step = dec("0.01");
    let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Short);
    assert_eq!(quantized, dec("100.13"));
}

#[test]
fn round_price_by_tick_enforces_minimum_step() {
    let price = dec("0.0001");
    let step = dec("0.005");
    let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Long);
    assert_eq!(quantized, step);
}

// bot-strategy#216: post-only passive enforcement
#[test]
fn post_only_passive_long_extended_btc_at_touch() {
    // Extended BTC tick=1, ask=77641, ToNegativeInfinity rounding leaves
    // limit at touch (no-op). Must shift to ask - 1 tick.
    let rounded = dec("77641");
    let touch = dec("77641");
    let tick = dec("1");
    let limit = enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Long);
    assert_eq!(limit, dec("77640"));
}

#[test]
fn post_only_passive_short_extended_eth_at_touch() {
    // Extended ETH tick=0.1, bid=2315.5, ToPositiveInfinity rounding
    // leaves limit at touch. Must shift to bid + 1 tick.
    let rounded = dec("2315.5");
    let touch = dec("2315.5");
    let tick = dec("0.1");
    let limit = enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Short);
    assert_eq!(limit, dec("2315.6"));
}

#[test]
fn post_only_passive_long_already_inside_no_op() {
    // Lighter passive-slippage path: rounded already below ask. Untouched.
    let rounded = dec("77640.0");
    let touch = dec("77640.5");
    let tick = dec("0.1");
    let limit = enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Long);
    assert_eq!(limit, dec("77640.0"));
}

#[test]
fn post_only_passive_short_already_inside_no_op() {
    let rounded = dec("2315.6");
    let touch = dec("2315.5");
    let tick = dec("0.1");
    let limit = enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Short);
    assert_eq!(limit, dec("2315.6"));
}

#[test]
fn post_only_passive_long_above_touch_clamps() {
    // Defensive: if upstream produced a rounded price above ask (e.g.
    // aggressive slippage_bps>0 with should_post_only true), clamp it
    // back inside.
    let rounded = dec("77642");
    let touch = dec("77641");
    let tick = dec("1");
    let limit = enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Long);
    assert_eq!(limit, dec("77640"));
}

#[test]
fn post_only_passive_zero_tick_returns_input() {
    let rounded = dec("100");
    let touch = dec("100");
    let tick = dec("0");
    let limit = enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Long);
    assert_eq!(limit, dec("100"));
}

// bot-strategy#258: Extended reduce-only error classification

// bot-strategy#281: classify Lighter REST 429 / DexError::RateLimited so
// the step skips quietly instead of erroring out per cycle.

#[test]
fn quantize_size_by_step_uses_size_decimals() {
    let size = dec("0.0023");
    let step = dec("0.001");
    let quantized = quantize_size_by_step(size, step, None);
    assert_eq!(quantized, dec("0.002"));
}

#[test]
fn quantize_size_by_step_respects_min_order_floor() {
    let size = dec("0.0002");
    let step = dec("0.0001");
    let quantized = quantize_size_by_step(size, step, Some(dec("0.001")));
    assert_eq!(quantized, dec("0.001"));
}

#[test]
fn quantize_size_by_step_ceiling_rounds_up() {
    let size = dec("0.0023");
    let step = dec("0.001");
    let quantized = quantize_size_by_step_ceiling(size, step, None);
    assert_eq!(quantized, dec("0.003"));
}

// bot-strategy#185 Phase 3-1: rolling-peak DD calculations.

// bot-strategy#185 leverage-neutralization amendment:
// `max_daily_loss_bps` and `max_session_loss_bps` are interpreted as
// 1x-equivalent market-move bps and multiplied by `max_leverage` at
// comparison time. Same YAML value should produce the same trip
// behaviour at any leverage, so changing leverage doesn't silently
// relax the gates.

// bot-strategy#320: trade-stats fields round-trip through risk_state.json.

// bot-strategy#320: an older snapshot without the trade-stats fields
// must load cleanly with zeros, not panic on missing keys.

// bot-strategy#354: round_id round-trips through persist/load.
#[test]
fn risk_state_round_id_round_trip() {
    use std::collections::HashMap;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("risk_state.json");
    let instances: HashMap<String, risk_io::InstanceRiskState> = HashMap::new();

    risk_io::persist_risk_state(&path, Some("round-4"), &instances);
    let snapshot = risk_io::load_risk_state(&path);

    assert_eq!(snapshot.round_id.as_deref(), Some("round-4"));
    assert_eq!(snapshot.version, 2);
}

// bot-strategy#259: defensive cap on exit qty when exchange position
// size momentarily over-reports vs the bot-recorded entry size after
// partial-fill retry recovery on Tokyo Extended LongSpread.
#[test]
fn cap_exit_qty_caps_exchange_size_when_over_recorded() {
    let exch = Some(dec("0.092"));
    let recorded = Some(dec("0.046"));
    let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", exch, recorded);
    assert_eq!(
        q,
        dec("0.046"),
        "must cap to recorded entry size when exchange is 2x"
    );
}

#[test]
fn cap_exit_qty_passes_exchange_within_5pct_tolerance() {
    // 0.046 * 1.04 = 0.04784 — under 5% threshold, exchange wins.
    let exch = Some(dec("0.04784"));
    let recorded = Some(dec("0.046"));
    let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", exch, recorded);
    assert_eq!(q, dec("0.04784"), "small drift (<5%) should pass through");
}

#[test]
fn cap_exit_qty_caps_at_5pct_boundary() {
    // 0.046 * 1.06 = 0.04876 — over 5% threshold, cap.
    let exch = Some(dec("0.04876"));
    let recorded = Some(dec("0.046"));
    let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", exch, recorded);
    assert_eq!(q, dec("0.046"), "drift just over 5% must cap");
}

#[test]
fn cap_exit_qty_falls_back_to_recorded_when_exchange_missing() {
    // Exchange snapshot absent (e.g. WS lag): use recorded entry size.
    let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", None, Some(dec("0.046")));
    assert_eq!(q, dec("0.046"));
}

#[test]
fn cap_exit_qty_passes_exchange_when_no_recorded() {
    // Recovery on startup: recorded entry size unknown, trust exchange.
    let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", Some(dec("0.05")), None);
    assert_eq!(q, dec("0.05"));
}

#[test]
fn cap_exit_qty_zero_when_both_missing() {
    let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", None, None);
    assert_eq!(q, Decimal::ZERO);
}

#[test]
fn cap_exit_qty_passes_when_exchange_smaller_than_recorded() {
    // Partial close already happened on exchange — exit should use the
    // smaller exchange-reported residual, not the original recorded entry.
    let exch = Some(dec("0.020"));
    let recorded = Some(dec("0.046"));
    let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", exch, recorded);
    assert_eq!(
        q,
        dec("0.020"),
        "exchange-side close already happened; trust the smaller residual"
    );
}

// bot-strategy#470: entry-side cap covering the cancel-then-reissue
// race that produced the variant C ETH leg double-fill on Frankfurt
// 2026-05-22 06:27 UTC (target 0.8905 → actual 1.7810).

#[test]
fn cap_entry_reissue_returns_zero_when_exchange_already_at_target() {
    // The actual variant C scenario: local thinks 0 filled, exchange
    // already has the entire target sitting there. Must not reissue.
    let q = PairTradeEngine::cap_entry_reissue_remaining(
        dec("0.8905"),
        Decimal::ZERO,
        Some(dec("0.8905")),
    );
    assert_eq!(
        q,
        Decimal::ZERO,
        "exchange already at target — reissue must be 0 or we double the leg"
    );
}

#[test]
fn cap_entry_reissue_returns_remaining_when_exchange_partial() {
    // Normal partial fill — local lags, exchange shows the real
    // partial, reissue only the gap.
    let q =
        PairTradeEngine::cap_entry_reissue_remaining(dec("1.0"), Decimal::ZERO, Some(dec("0.4")));
    assert_eq!(q, dec("0.6"));
}

#[test]
fn cap_entry_reissue_falls_back_to_local_when_exchange_query_failed() {
    // Network / API hiccup: get_positions returned None. Fall back
    // to the existing local-only arithmetic (`target - local`).
    let q = PairTradeEngine::cap_entry_reissue_remaining(dec("1.0"), dec("0.3"), None);
    assert_eq!(q, dec("0.7"));
}

#[test]
fn cap_entry_reissue_trusts_higher_local_when_exchange_lags() {
    // The reverse race: local recorded fills via WS but exchange
    // /positions REST hasn't propagated yet. Trust the larger value
    // so we don't redundantly re-send already-filled qty.
    let q = PairTradeEngine::cap_entry_reissue_remaining(dec("1.0"), dec("0.7"), Some(dec("0.2")));
    assert_eq!(q, dec("0.3"));
}

#[test]
fn cap_entry_reissue_returns_zero_when_exchange_over_target() {
    // Exchange somehow shows more than target (e.g. pre-existing
    // residual not force-closed). Cap remaining at zero — operator
    // recovery should run, not another order.
    let q =
        PairTradeEngine::cap_entry_reissue_remaining(dec("1.0"), Decimal::ZERO, Some(dec("2.0")));
    assert_eq!(q, Decimal::ZERO);
}

#[test]
fn cap_entry_reissue_returns_full_target_when_nothing_filled() {
    // Cold-start reissue path: nothing on either side.
    let q = PairTradeEngine::cap_entry_reissue_remaining(
        dec("1.0"),
        Decimal::ZERO,
        Some(Decimal::ZERO),
    );
    assert_eq!(q, dec("1.0"));
}

fn make_leg(symbol: &str, target: Decimal) -> PendingLeg {
    PendingLeg {
        symbol: symbol.to_string(),
        order_id: format!("oid-{}-{}", symbol, target),
        exchange_order_id: None,
        target,
        filled: target,
        side: dex_connector::OrderSide::Long,
        submitted_qty: Decimal::ZERO,
        limit_price: None,
        reference_price: None,
        submit_ts_ms: 0,
        ack_ts_ms: None,
        submit_reference_price: None,
        submit_mid: None,
        submit_bid: None,
        submit_ask: None,
        client_order_id: None,
        reduce_only: false,
        post_only: false,
    }
}

// Pairtrade entry-size under-record fix (companion to bot-strategy#259):
// post-reissue pending.legs can hold two legs per symbol (kept + new);
// assignment-only recording leaks only the last leg's target into
// entry_size_a/b and breaks the cap_exit_qty invariant.
#[test]
fn sum_entry_sizes_simple_one_leg_per_symbol() {
    let legs = vec![
        make_leg("BTC", dec("0.0013")),
        make_leg("ETH", dec("0.046")),
    ];
    let (a, b) = PairTradeEngine::sum_entry_sizes_by_symbol(&legs, "BTC", "ETH");
    assert_eq!(a, Some(dec("0.0013")));
    assert_eq!(b, Some(dec("0.046")));
}

#[test]
fn sum_entry_sizes_partial_fill_reissue_two_legs_same_symbol() {
    // Real shape after reissue_partial_legs: BTC partial-filled then
    // reissued, so pending.legs has the kept leg (target=filled) plus
    // the new leg (target=remaining quantized). ETH was full-filled in
    // one shot, so a single leg.
    let legs = vec![
        make_leg("BTC", dec("0.0008")), // kept leg, filled portion
        make_leg("BTC", dec("0.0005")), // reissued leg, remaining
        make_leg("ETH", dec("0.046")),
    ];
    let (a, b) = PairTradeEngine::sum_entry_sizes_by_symbol(&legs, "BTC", "ETH");
    assert_eq!(
        a,
        Some(dec("0.0013")),
        "BTC must sum kept (0.0008) + reissued (0.0005), not last-write-wins to 0.0005"
    );
    assert_eq!(b, Some(dec("0.046")));
}

#[test]
fn sum_entry_sizes_both_symbols_reissued() {
    // Pathological case: both legs partial-filled and reissued.
    let legs = vec![
        make_leg("BTC", dec("0.0008")),
        make_leg("BTC", dec("0.0005")),
        make_leg("ETH", dec("0.030")),
        make_leg("ETH", dec("0.016")),
    ];
    let (a, b) = PairTradeEngine::sum_entry_sizes_by_symbol(&legs, "BTC", "ETH");
    assert_eq!(a, Some(dec("0.0013")));
    assert_eq!(b, Some(dec("0.046")));
}

#[test]
fn sum_entry_sizes_returns_none_for_missing_symbol() {
    // Defensive: if a symbol has zero legs (shouldn't happen in
    // practice), preserve the previous Option::None semantics rather
    // than silently writing Decimal::ZERO into entry_size.
    let legs = vec![make_leg("BTC", dec("0.0013"))];
    let (a, b) = PairTradeEngine::sum_entry_sizes_by_symbol(&legs, "BTC", "ETH");
    assert_eq!(a, Some(dec("0.0013")));
    assert_eq!(b, None);
}

#[test]
fn sum_entry_sizes_unknown_symbol_ignored() {
    // Legs for symbols outside the base/quote pair are not summed.
    let legs = vec![
        make_leg("BTC", dec("0.0013")),
        make_leg("SOL", dec("1.0")),
        make_leg("ETH", dec("0.046")),
    ];
    let (a, b) = PairTradeEngine::sum_entry_sizes_by_symbol(&legs, "BTC", "ETH");
    assert_eq!(a, Some(dec("0.0013")));
    assert_eq!(b, Some(dec("0.046")));
}
