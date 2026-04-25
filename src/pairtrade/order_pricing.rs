//! Order pricing/quantization helpers extracted from the monolithic
//! pairtrade module.

use std::collections::HashMap;

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use super::market::SymbolSnapshot;
use super::util::{quantize_size_by_step, quantize_size_by_step_ceiling};

pub(super) fn apply_slippage(
    slippage_bps: i32,
    price: Option<Decimal>,
    side: dex_connector::OrderSide,
) -> Option<Decimal> {
    let p = price?;
    if slippage_bps == 0 {
        return Some(p);
    }
    let factor =
        Decimal::from_f64((slippage_bps.abs() as f64) / 10_000.0).unwrap_or(Decimal::ZERO);
    let passive = slippage_bps < 0;
    match side {
        dex_connector::OrderSide::Long => {
            if passive {
                Some(p * (Decimal::ONE - factor))
            } else {
                Some(p * (Decimal::ONE + factor))
            }
        }
        dex_connector::OrderSide::Short => {
            if passive {
                Some(p * (Decimal::ONE + factor))
            } else {
                Some(p * (Decimal::ONE - factor))
            }
        }
    }
}

pub(super) fn quantize_order_size(
    symbol: &str,
    size: Decimal,
    prices: &HashMap<String, SymbolSnapshot>,
) -> Decimal {
    if size <= Decimal::ZERO {
        return size;
    }
    if let Some(snapshot) = prices.get(symbol) {
        let min_order = snapshot.min_order.clone();
        let step = snapshot
            .size_decimals
            .map(|d| Decimal::new(1, d.min(28)))
            .or_else(|| min_order.clone());
        if let Some(step) = step {
            let quantized = quantize_size_by_step(size, step, min_order);
            if quantized > Decimal::ZERO {
                return quantized;
            }
        }
    }
    size
}

pub(super) fn quantize_order_size_exit(
    symbol: &str,
    size: Decimal,
    prices: &HashMap<String, SymbolSnapshot>,
) -> Decimal {
    if size <= Decimal::ZERO {
        return size;
    }
    if let Some(snapshot) = prices.get(symbol) {
        let min_order = snapshot.min_order.clone();
        let step = snapshot
            .size_decimals
            .map(|d| Decimal::new(1, d.min(28)))
            .or_else(|| min_order.clone());
        if let Some(step) = step {
            let quantized = quantize_size_by_step_ceiling(size, step, min_order);
            if quantized > Decimal::ZERO {
                return quantized;
            }
        }
    }
    size
}

pub(super) fn quantize_order_size_close(
    symbol: &str,
    size: Decimal,
    prices: &HashMap<String, SymbolSnapshot>,
) -> Decimal {
    if size <= Decimal::ZERO {
        return size;
    }
    if let Some(snapshot) = prices.get(symbol) {
        let step = snapshot
            .size_decimals
            .map(|d| Decimal::new(1, d.min(28)))
            .or_else(|| snapshot.min_order.clone());
        if let Some(step) = step {
            let quantized = quantize_size_by_step_ceiling(size, step, None);
            if quantized > Decimal::ZERO {
                return quantized;
            }
        }
    }
    size
}

/// Pick the (qty_a, qty_b) pair from {floor, ceil}×{floor, ceil} candidates
/// that best preserves the requested hedge ratio qtys.0 / qtys.1. Returns the
/// chosen sizes and the resulting absolute deviation, or `None` when every
/// candidate is zero (e.g. both legs floor below their min lot). See
/// bot-strategy#211.
pub(super) fn pick_entry_quantize(
    qtys: (Decimal, Decimal),
    qa_floor: Decimal,
    qa_ceil: Decimal,
    qb_floor: Decimal,
    qb_ceil: Decimal,
) -> Option<(Decimal, Decimal, f64)> {
    if qtys.1.is_zero() {
        return None;
    }
    let target_ratio = qtys.0 / qtys.1;
    if target_ratio.is_zero() {
        return None;
    }
    let mut best: Option<(Decimal, Decimal, f64)> = None;
    for &a in &[qa_floor, qa_ceil] {
        for &b in &[qb_floor, qb_ceil] {
            if a.is_zero() || b.is_zero() {
                continue;
            }
            let actual_ratio = a / b;
            let dev = ((actual_ratio / target_ratio) - Decimal::ONE)
                .abs()
                .to_f64()
                .unwrap_or(f64::INFINITY);
            if best.map_or(true, |(_, _, d)| dev < d) {
                best = Some((a, b, dev));
            }
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn pick_entry_quantize_eth_011_to_floor() {
        // bot-strategy#211: 0.011 ETH at 0.01 step previously ceiled to 0.02
        // (81.8% dev). With both directions available, floor/floor wins.
        // Requested (0.00026 BTC, 0.011 ETH); BTC step 0.0001, ETH step 0.01.
        let res = pick_entry_quantize(
            (dec("0.00026"), dec("0.011")),
            /*a_floor*/ dec("0.0002"),
            /*a_ceil*/ dec("0.0003"),
            /*b_floor*/ dec("0.01"),
            /*b_ceil*/ dec("0.02"),
        )
        .expect("non-zero result");
        assert_eq!(res.0, dec("0.0002"));
        assert_eq!(res.1, dec("0.01"));
        assert!(res.2 < 0.20, "dev should be under 20%, got {}", res.2);
    }

    #[test]
    fn pick_entry_quantize_eth_010_picks_btc_ceil() {
        // 0.00026 BTC / 0.01 ETH: BTC ceil + ETH floor wins (15.4% dev) vs
        // BTC floor + ETH floor (23.1% dev).
        let res = pick_entry_quantize(
            (dec("0.00026"), dec("0.01")),
            dec("0.0002"),
            dec("0.0003"),
            dec("0.01"),
            dec("0.02"),
        )
        .expect("non-zero");
        assert_eq!(res.0, dec("0.0003"));
        assert_eq!(res.1, dec("0.01"));
        assert!(res.2 < 0.20);
    }

    #[test]
    fn pick_entry_quantize_zero_b_returns_none() {
        let res = pick_entry_quantize(
            (dec("0.00026"), dec("0")),
            dec("0.0002"),
            dec("0.0003"),
            dec("0"),
            dec("0"),
        );
        assert!(res.is_none());
    }

    #[test]
    fn pick_entry_quantize_all_zero_returns_none() {
        let res = pick_entry_quantize(
            (dec("0.00026"), dec("0.011")),
            dec("0"),
            dec("0"),
            dec("0"),
            dec("0"),
        );
        assert!(res.is_none());
    }

    fn snapshot_with(min_order: Option<&str>, size_decimals: Option<u32>) -> SymbolSnapshot {
        SymbolSnapshot {
            price: dec("1"),
            funding_rate: dec("0"),
            bid_price: None,
            ask_price: None,
            bid_size: dec("0"),
            ask_size: dec("0"),
            min_order: min_order.map(dec),
            min_tick: None,
            size_decimals,
            exchange_ts: None,
        }
    }

    fn prices_for(symbol: &str, snap: SymbolSnapshot) -> HashMap<String, SymbolSnapshot> {
        let mut m = HashMap::new();
        m.insert(symbol.to_string(), snap);
        m
    }

    // --- step selection: size_decimals takes priority over min_order ---

    #[test]
    fn quantize_extended_eth_011_keeps_011() {
        // Extended ETH: min_order=0.01, size_decimals=3 → step 0.001.
        // 0.011 ETH must stay at 0.011 (above min_order, on 0.001 grid).
        let prices = prices_for("ETH", snapshot_with(Some("0.01"), Some(3)));
        assert_eq!(quantize_order_size("ETH", dec("0.011"), &prices), dec("0.011"));
        assert_eq!(quantize_order_size_exit("ETH", dec("0.011"), &prices), dec("0.011"));
    }

    #[test]
    fn quantize_lighter_eth_012_keeps_012() {
        // Lighter ETH: min_order=0.005, size_decimals=4 → step 0.0001.
        // 0.012 must stay at 0.012, not floor to 0.010 / ceil to 0.015.
        let prices = prices_for("ETH", snapshot_with(Some("0.005"), Some(4)));
        assert_eq!(quantize_order_size("ETH", dec("0.012"), &prices), dec("0.012"));
        assert_eq!(quantize_order_size_exit("ETH", dec("0.012"), &prices), dec("0.012"));
    }

    #[test]
    fn quantize_lighter_btc_00026_keeps_00026() {
        // Lighter BTC: min_order=0.0002, size_decimals=5 → step 0.00001.
        // 0.00026 must stay at 0.00026, not snap to 0.0002 / 0.0004.
        let prices = prices_for("BTC", snapshot_with(Some("0.0002"), Some(5)));
        assert_eq!(
            quantize_order_size("BTC", dec("0.00026"), &prices),
            dec("0.00026")
        );
        assert_eq!(
            quantize_order_size_exit("BTC", dec("0.00026"), &prices),
            dec("0.00026")
        );
    }

    // --- min_order acts as a floor (bump small sizes up) ---

    #[test]
    fn quantize_size_below_min_order_bumps_to_min_order() {
        // 0.003 ETH < min_order 0.005 → bump up to 0.005 even on 0.0001 grid.
        let prices = prices_for("ETH", snapshot_with(Some("0.005"), Some(4)));
        assert_eq!(quantize_order_size("ETH", dec("0.003"), &prices), dec("0.005"));
        assert_eq!(
            quantize_order_size_exit("ETH", dec("0.003"), &prices),
            dec("0.005")
        );
    }

    // --- size_decimals absent: fall back to min_order as step (legacy path) ---

    #[test]
    fn quantize_falls_back_to_min_order_when_no_size_decimals() {
        // Snapshots without size_decimals (older connectors) keep current
        // behavior: step = min_order.
        let prices = prices_for("ETH", snapshot_with(Some("0.005"), None));
        // 0.012 → trunc(0.012/0.005)=2 → 0.010
        assert_eq!(quantize_order_size("ETH", dec("0.012"), &prices), dec("0.010"));
        // ceiling: ceil(0.012/0.005)=3 → 0.015
        assert_eq!(
            quantize_order_size_exit("ETH", dec("0.012"), &prices),
            dec("0.015")
        );
    }

    // --- end-to-end: pick_entry_quantize via real quantize functions ---

    #[test]
    fn entry_quantize_extended_eth_011_zero_deviation_after_fix() {
        // bot-strategy#217: with size_decimals-first step, the floor/ceil
        // search is moot — both directions land on 0.011.
        let prices = prices_for("ETH", snapshot_with(Some("0.01"), Some(3)));
        let qb_floor = quantize_order_size("ETH", dec("0.011"), &prices);
        let qb_ceil = quantize_order_size_exit("ETH", dec("0.011"), &prices);
        assert_eq!(qb_floor, dec("0.011"));
        assert_eq!(qb_ceil, dec("0.011"));

        let prices_btc = prices_for("BTC", snapshot_with(Some("0.0001"), Some(5)));
        let qa_floor = quantize_order_size("BTC", dec("0.00026"), &prices_btc);
        let qa_ceil = quantize_order_size_exit("BTC", dec("0.00026"), &prices_btc);
        assert_eq!(qa_floor, dec("0.00026"));
        assert_eq!(qa_ceil, dec("0.00026"));

        let res = pick_entry_quantize(
            (dec("0.00026"), dec("0.011")),
            qa_floor,
            qa_ceil,
            qb_floor,
            qb_ceil,
        )
        .expect("non-zero");
        assert_eq!(res.0, dec("0.00026"));
        assert_eq!(res.1, dec("0.011"));
        // Hedge ratio deviation should be ~0 (target ratio == actual ratio).
        assert!(res.2 < 1e-9, "dev should be ~0, got {}", res.2);
    }
}
