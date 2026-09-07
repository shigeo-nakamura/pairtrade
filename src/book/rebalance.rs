//! Pure rebalance planning, see `docs/book-runtime.md` §5.
//!
//! Turns a validated target-weight vector plus the current book into an
//! ordered list of IOC intents. No I/O, no venue knowledge beyond the lot
//! metadata passed in; fully unit-tested.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use serde::{Deserialize, Serialize};

use super::config::SizingConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => write!(f, "buy"),
            Side::Sell => write!(f, "sell"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentKind {
    /// Reduce the whole leg to zero.
    Close,
    /// Shrink a leg without crossing zero.
    Reduce,
    /// Open a new leg from flat.
    Open,
    /// Grow an existing leg on the same side.
    Increase,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub symbol: String,
    pub side: Side,
    /// Absolute base quantity, already rounded to the venue lot.
    pub qty: f64,
    /// Mid used for sizing; the executor reports slippage against it.
    pub reference_price: f64,
    pub notional_usd: f64,
    pub reduce_only: bool,
    pub kind: IntentKind,
}

impl OrderIntent {
    pub fn is_reducing(&self) -> bool {
        self.reduce_only
    }
}

/// Venue lot metadata per symbol.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct LotMeta {
    pub size_decimals: u32,
    /// Venue minimum base quantity per order, when known.
    pub min_order_qty: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Plan {
    pub intents: Vec<OrderIntent>,
    /// Rounded signed target quantity per symbol (zero targets included for
    /// symbols currently held).
    pub target_qty: BTreeMap<String, f64>,
    pub gross_target_usd: f64,
    pub net_target_usd: f64,
    /// Symbols whose diff was below the deadband / minimum and was skipped.
    pub skipped: Vec<SkippedDiff>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SkippedDiff {
    pub symbol: String,
    pub diff_qty: f64,
    pub diff_usd: f64,
    pub reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "reason", content = "detail")]
pub enum PlanReject {
    MissingPrice(String),
    MissingLotMeta(String),
    CapGross {
        gross_usd: f64,
        cap_usd: f64,
    },
    CapNet {
        net_usd: f64,
        cap_usd: f64,
    },
    CapSymbol {
        symbol: String,
        notional_usd: f64,
        cap_usd: f64,
    },
}

impl PlanReject {
    pub fn label(&self) -> &'static str {
        match self {
            PlanReject::MissingPrice(_) => "missing_price",
            PlanReject::MissingLotMeta(_) => "missing_lot_meta",
            PlanReject::CapGross { .. } => "cap_gross",
            PlanReject::CapNet { .. } => "cap_net",
            PlanReject::CapSymbol { .. } => "cap_symbol",
        }
    }
}

impl fmt::Display for PlanReject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap_or_default())
    }
}

/// Round an absolute quantity down to `decimals` places. A tiny epsilon
/// absorbs binary float noise (`0.3 * 10 = 2.9999999999999996`).
pub fn round_down_qty(qty: f64, decimals: u32) -> f64 {
    if !(qty.is_finite() && qty > 0.0) {
        return 0.0;
    }
    let scale = 10f64.powi(decimals as i32);
    ((qty * scale) + 1e-7).floor() / scale
}

fn sign(x: f64) -> i8 {
    if x > 0.0 {
        1
    } else if x < 0.0 {
        -1
    } else {
        0
    }
}

/// Signed, lot-rounded target quantity per symbol from a weight vector.
/// Symbols in `current` but absent from `weights` get a zero target.
pub fn targets_from_weights(
    weights: &BTreeMap<String, f64>,
    current: &BTreeMap<String, f64>,
    prices: &HashMap<String, f64>,
    lots: &HashMap<String, LotMeta>,
    sizing: &SizingConfig,
) -> Result<BTreeMap<String, f64>, PlanReject> {
    let mut out = BTreeMap::new();
    for sym in current.keys() {
        out.insert(sym.clone(), 0.0);
    }
    for (sym, w) in weights {
        if *w == 0.0 {
            out.insert(sym.clone(), 0.0);
            continue;
        }
        let price = match prices.get(sym).copied() {
            Some(p) if p.is_finite() && p > 0.0 => p,
            _ => return Err(PlanReject::MissingPrice(sym.clone())),
        };
        let lot = lots
            .get(sym)
            .copied()
            .ok_or_else(|| PlanReject::MissingLotMeta(sym.clone()))?;
        let tq_abs = round_down_qty(
            w.abs() * sizing.gross_notional_usd / price,
            lot.size_decimals,
        );
        out.insert(sym.clone(), if *w < 0.0 { -tq_abs } else { tq_abs });
    }
    Ok(out)
}

/// Plan the rebalance from `current` (signed base qty per symbol) to
/// `weights` (fraction of `sizing.gross_notional_usd`).
pub fn plan(
    weights: &BTreeMap<String, f64>,
    current: &BTreeMap<String, f64>,
    prices: &HashMap<String, f64>,
    lots: &HashMap<String, LotMeta>,
    sizing: &SizingConfig,
) -> Result<Plan, PlanReject> {
    let targets = targets_from_weights(weights, current, prices, lots, sizing)?;
    plan_targets(&targets, current, prices, lots, sizing)
}

/// Plan from already-rounded signed target quantities (a persisted
/// `DecisionRecord::target_qty` on a partial-fill retry, or a flatten).
/// Caps are re-checked so a stale target can never exceed them.
pub fn plan_targets(
    targets: &BTreeMap<String, f64>,
    current: &BTreeMap<String, f64>,
    prices: &HashMap<String, f64>,
    lots: &HashMap<String, LotMeta>,
    sizing: &SizingConfig,
) -> Result<Plan, PlanReject> {
    let mut symbols: Vec<&String> = targets.keys().chain(current.keys()).collect();
    symbols.sort();
    symbols.dedup();

    let mut target_qty = BTreeMap::new();
    let mut gross = 0.0;
    let mut net = 0.0;
    let mut reducing = Vec::new();
    let mut opening = Vec::new();
    let mut skipped = Vec::new();

    for sym in symbols {
        let tq = targets.get(sym).copied().unwrap_or(0.0);
        let cur = current.get(sym).copied().unwrap_or(0.0);
        if tq == 0.0 && cur == 0.0 {
            target_qty.insert(sym.clone(), 0.0);
            continue;
        }
        let price = match prices.get(sym).copied() {
            Some(p) if p.is_finite() && p > 0.0 => p,
            _ => return Err(PlanReject::MissingPrice(sym.clone())),
        };
        let lot = lots
            .get(sym)
            .copied()
            .ok_or_else(|| PlanReject::MissingLotMeta(sym.clone()))?;
        let tq_abs = tq.abs();
        target_qty.insert(sym.clone(), tq);
        let t_notional = tq_abs * price;
        gross += t_notional;
        net += tq * price;
        let one_lot_usd = 10f64.powi(-(lot.size_decimals as i32)) * price;
        let sym_cap = sizing.max_symbol_weight * sizing.gross_notional_usd + one_lot_usd;
        if t_notional > sym_cap + 1e-9 {
            return Err(PlanReject::CapSymbol {
                symbol: sym.clone(),
                notional_usd: t_notional,
                cap_usd: sym_cap,
            });
        }

        let diff = tq - cur;
        let diff_usd = diff.abs() * price;
        let mk = |side: Side, qty: f64, reduce_only: bool, kind: IntentKind| OrderIntent {
            symbol: sym.clone(),
            side,
            qty,
            reference_price: price,
            notional_usd: qty * price,
            reduce_only,
            kind,
        };
        let side_for = |delta: f64| if delta > 0.0 { Side::Buy } else { Side::Sell };

        if diff == 0.0 {
            continue;
        }
        if tq == 0.0 {
            // Go flat: close the whole leg, dust included (deadband and
            // minimum do not apply to a close).
            reducing.push(mk(side_for(-cur), cur.abs(), true, IntentKind::Close));
            continue;
        }
        if diff_usd < sizing.rebalance_deadband_usd {
            skipped.push(SkippedDiff {
                symbol: sym.clone(),
                diff_qty: diff,
                diff_usd,
                reason: "below_deadband",
            });
            continue;
        }
        if diff_usd < sizing.min_order_usd {
            skipped.push(SkippedDiff {
                symbol: sym.clone(),
                diff_qty: diff,
                diff_usd,
                reason: "below_min_order",
            });
            continue;
        }
        let s_cur = sign(cur);
        let s_tq = sign(tq);
        if s_cur != 0 && s_cur != s_tq {
            // Sign flip: close, then open the other way.
            reducing.push(mk(side_for(-cur), cur.abs(), true, IntentKind::Close));
            let open_qty = tq_abs;
            if open_qty * price >= sizing.min_order_usd
                && lot.min_order_qty.map_or(true, |m| open_qty >= m)
            {
                opening.push(mk(side_for(tq), open_qty, false, IntentKind::Open));
            } else {
                skipped.push(SkippedDiff {
                    symbol: sym.clone(),
                    diff_qty: tq,
                    diff_usd: open_qty * price,
                    reason: "flip_open_below_min",
                });
            }
            continue;
        }
        if s_cur != 0 && tq.abs() < cur.abs() {
            reducing.push(mk(side_for(diff), diff.abs(), true, IntentKind::Reduce));
            continue;
        }
        let qty = diff.abs();
        if lot.min_order_qty.map_or(false, |m| qty < m) {
            skipped.push(SkippedDiff {
                symbol: sym.clone(),
                diff_qty: diff,
                diff_usd,
                reason: "below_venue_min_qty",
            });
            continue;
        }
        let kind = if s_cur == 0 {
            IntentKind::Open
        } else {
            IntentKind::Increase
        };
        opening.push(mk(side_for(diff), qty, false, kind));
    }

    if gross > sizing.max_gross_usd + 1e-9 {
        return Err(PlanReject::CapGross {
            gross_usd: gross,
            cap_usd: sizing.max_gross_usd,
        });
    }
    if net.abs() > sizing.max_net_usd + 1e-9 {
        return Err(PlanReject::CapNet {
            net_usd: net,
            cap_usd: sizing.max_net_usd,
        });
    }

    reducing.sort_by(|a, b| b.notional_usd.partial_cmp(&a.notional_usd).unwrap());
    opening.sort_by(|a, b| b.notional_usd.partial_cmp(&a.notional_usd).unwrap());
    let mut intents = reducing;
    intents.extend(opening);
    Ok(Plan {
        intents,
        target_qty,
        gross_target_usd: gross,
        net_target_usd: net,
        skipped,
    })
}

/// Plan that closes every leg (halt flatten / fixed-window exit).
pub fn plan_flatten(
    current: &BTreeMap<String, f64>,
    prices: &HashMap<String, f64>,
    lots: &HashMap<String, LotMeta>,
    sizing: &SizingConfig,
) -> Result<Plan, PlanReject> {
    let empty = BTreeMap::new();
    let mut relaxed = sizing.clone();
    relaxed.rebalance_deadband_usd = 0.0;
    plan(&empty, current, prices, lots, &relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::config::{test_config_yaml, BookConfig};

    fn sizing() -> SizingConfig {
        BookConfig::from_yaml_str(&test_config_yaml())
            .unwrap()
            .sizing
    }

    fn prices() -> HashMap<String, f64> {
        [
            ("BTC", 100_000.0),
            ("ETH", 4_000.0),
            ("SOL", 200.0),
            ("DOT", 4.0),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), *v))
        .collect()
    }

    fn lots() -> HashMap<String, LotMeta> {
        [("BTC", 5u32), ("ETH", 4), ("SOL", 2), ("DOT", 1)]
            .iter()
            .map(|(k, d)| {
                (
                    k.to_string(),
                    LotMeta {
                        size_decimals: *d,
                        min_order_qty: None,
                    },
                )
            })
            .collect()
    }

    fn w(pairs: &[(&str, f64)]) -> BTreeMap<String, f64> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn round_down_handles_float_noise() {
        assert_eq!(round_down_qty(0.3, 1), 0.3);
        assert_eq!(round_down_qty(0.29999, 1), 0.2);
        assert_eq!(round_down_qty(2.5 / 200.0 * 100.0, 2), 1.25);
        assert_eq!(round_down_qty(0.0, 3), 0.0);
        assert_eq!(round_down_qty(-1.0, 3), 0.0);
    }

    #[test]
    fn opens_from_flat_with_rounded_lots_and_orders_reducing_first() {
        let p = plan(
            &w(&[("BTC", 0.25), ("ETH", 0.25), ("SOL", -0.25), ("DOT", -0.25)]),
            &BTreeMap::new(),
            &prices(),
            &lots(),
            &sizing(),
        )
        .unwrap();
        assert_eq!(p.intents.len(), 4);
        assert!(p
            .intents
            .iter()
            .all(|i| !i.reduce_only && i.kind == IntentKind::Open));
        let btc = p.intents.iter().find(|i| i.symbol == "BTC").unwrap();
        assert_eq!(btc.qty, 0.0025);
        assert_eq!(btc.side, Side::Buy);
        let dot = p.intents.iter().find(|i| i.symbol == "DOT").unwrap();
        assert_eq!(dot.qty, 62.5);
        assert_eq!(dot.side, Side::Sell);
        assert!((p.gross_target_usd - 1000.0).abs() < 1e-6);
        assert!(p.net_target_usd.abs() < 1e-6);
        // largest notional first inside the opening group
        assert!(p
            .intents
            .windows(2)
            .all(|x| x[0].notional_usd >= x[1].notional_usd));
    }

    #[test]
    fn go_flat_closes_dust_and_flip_splits_into_close_then_open() {
        let cur = w(&[("BTC", 0.0025), ("SOL", -1.25), ("DOT", -0.5)]); // DOT dust $2
        let p = plan(
            &w(&[("SOL", 0.25), ("ETH", -0.25)]),
            &cur,
            &prices(),
            &lots(),
            &sizing(),
        )
        .unwrap();
        let kinds: Vec<_> = p
            .intents
            .iter()
            .map(|i| (i.symbol.as_str(), i.kind, i.reduce_only, i.side))
            .collect();
        // reducing group first: BTC close ($250), SOL close ($250), DOT close ($2 dust,
        // still closed because target is zero); then opens: SOL open, ETH open.
        assert_eq!(kinds[0].2, true);
        assert_eq!(kinds[1].2, true);
        assert_eq!(kinds[2], ("DOT", IntentKind::Close, true, Side::Buy));
        // The two opens have equal notional; only the group order is fixed.
        let mut opens = vec![kinds[3], kinds[4]];
        opens.sort();
        assert_eq!(
            opens,
            vec![
                ("ETH", IntentKind::Open, false, Side::Sell),
                ("SOL", IntentKind::Open, false, Side::Buy)
            ]
        );
        assert_eq!(p.intents.len(), 5);
    }

    #[test]
    fn deadband_and_min_order_skip_small_diffs_but_not_closes() {
        // Single-leg book for readability: lift the net cap. SOL lot is
        // 0.01 = $2 so sub-lot diffs are visible.
        let mut sizing = sizing();
        sizing.max_net_usd = 10_000.0;
        // Held 1.25 SOL ($250); target 0.253 → 1.26 → diff $2 → below deadband.
        let cur = w(&[("SOL", 1.25)]);
        let p = plan(&w(&[("SOL", 0.253)]), &cur, &prices(), &lots(), &sizing).unwrap();
        assert!(p.intents.is_empty());
        assert_eq!(p.skipped[0].reason, "below_deadband");
        // diff $8 (< min_order 10, > deadband 5) → skipped as below_min_order.
        let p = plan(&w(&[("SOL", 0.258)]), &cur, &prices(), &lots(), &sizing).unwrap();
        assert!(p.intents.is_empty());
        assert_eq!(p.skipped[0].reason, "below_min_order");
        // reduce by $30 → reduce_only sell
        let p = plan(&w(&[("SOL", 0.22)]), &cur, &prices(), &lots(), &sizing).unwrap();
        assert_eq!(p.intents.len(), 1);
        assert_eq!(p.intents[0].kind, IntentKind::Reduce);
        assert!(p.intents[0].reduce_only);
        assert_eq!(p.intents[0].side, Side::Sell);
        assert!((p.intents[0].qty - 0.15).abs() < 1e-9);
        // increase by $50 → not reduce_only
        let p = plan(&w(&[("SOL", 0.30)]), &cur, &prices(), &lots(), &sizing).unwrap();
        assert_eq!(p.intents[0].kind, IntentKind::Increase);
        assert!(!p.intents[0].reduce_only);
        // a $2 dust leg with a zero target is still closed
        let dust = w(&[("SOL", 0.01)]);
        let p = plan(&BTreeMap::new(), &dust, &prices(), &lots(), &sizing).unwrap();
        assert_eq!(p.intents.len(), 1);
        assert_eq!(p.intents[0].kind, IntentKind::Close);
    }

    #[test]
    fn caps_reject_the_whole_plan() {
        let mut s = sizing();
        s.max_net_usd = 10.0;
        let e = plan(
            &w(&[("BTC", 0.5), ("ETH", -0.4)]),
            &BTreeMap::new(),
            &prices(),
            &lots(),
            &s,
        )
        .unwrap_err();
        assert_eq!(e.label(), "cap_net");
        let mut s = sizing();
        s.max_symbol_weight = 0.2;
        let e = plan(
            &w(&[("BTC", 0.25), ("ETH", -0.25)]),
            &BTreeMap::new(),
            &prices(),
            &lots(),
            &s,
        )
        .unwrap_err();
        assert_eq!(e.label(), "cap_symbol");
        let mut s = sizing();
        s.max_gross_usd = 900.0;
        let e = plan(
            &w(&[("BTC", 0.5), ("ETH", -0.5)]),
            &BTreeMap::new(),
            &prices(),
            &lots(),
            &s,
        )
        .unwrap_err();
        assert_eq!(e.label(), "cap_gross");
    }

    #[test]
    fn missing_price_or_lot_rejects_only_when_relevant() {
        let mut p = prices();
        p.remove("DOT");
        // DOT neither held nor targeted → fine
        assert!(plan(
            &w(&[("BTC", 0.1), ("ETH", -0.1)]),
            &BTreeMap::new(),
            &p,
            &lots(),
            &sizing()
        )
        .is_ok());
        // DOT held → must be priced
        let cur = w(&[("DOT", 10.0)]);
        assert_eq!(
            plan(
                &w(&[("BTC", 0.1), ("ETH", -0.1)]),
                &cur,
                &p,
                &lots(),
                &sizing()
            )
            .unwrap_err()
            .label(),
            "missing_price"
        );
        let mut l = lots();
        l.remove("ETH");
        assert_eq!(
            plan(
                &w(&[("BTC", 0.1), ("ETH", -0.1)]),
                &BTreeMap::new(),
                &prices(),
                &l,
                &sizing()
            )
            .unwrap_err()
            .label(),
            "missing_lot_meta"
        );
    }

    #[test]
    fn flatten_closes_everything_regardless_of_deadband() {
        let cur = w(&[("BTC", 0.00001), ("DOT", -0.1)]); // $1 and $0.4
        let p = plan_flatten(&cur, &prices(), &lots(), &sizing()).unwrap();
        assert_eq!(p.intents.len(), 2);
        assert!(p
            .intents
            .iter()
            .all(|i| i.reduce_only && i.kind == IntentKind::Close));
    }

    #[test]
    fn plan_targets_reproduces_plan_and_recomputes_caps() {
        let cur = w(&[("BTC", 0.001)]);
        let weights = w(&[("BTC", 0.25), ("DOT", -0.25)]);
        let a = plan(&weights, &cur, &prices(), &lots(), &sizing()).unwrap();
        let b = plan_targets(&a.target_qty, &cur, &prices(), &lots(), &sizing()).unwrap();
        assert_eq!(a, b);
        // A persisted target that no longer fits the caps is rejected.
        let mut tight = sizing();
        tight.max_gross_usd = tight.gross_notional_usd; // 1000
        tight.max_symbol_weight = 0.1;
        assert_eq!(
            plan_targets(&a.target_qty, &cur, &prices(), &lots(), &tight)
                .unwrap_err()
                .label(),
            "cap_symbol"
        );
    }

    #[test]
    fn venue_min_qty_blocks_opens_only() {
        let mut l = lots();
        l.get_mut("SOL").unwrap().min_order_qty = Some(2.0);
        let p = plan(
            &w(&[("SOL", 0.25), ("ETH", -0.25)]),
            &BTreeMap::new(),
            &prices(),
            &l,
            &sizing(),
        )
        .unwrap();
        // 0.25*1000/200 = 1.25 < 2.0 → skipped
        assert!(p.intents.iter().all(|i| i.symbol != "SOL"));
        assert_eq!(p.skipped[0].reason, "below_venue_min_qty");
    }
}
