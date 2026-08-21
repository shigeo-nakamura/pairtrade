//! Pair-evaluation and exit-sizing helpers for `PairTradeEngine`.
//!
//! Thin wrappers / lookups that the entry / exit / reconcile paths use
//! to translate between strategy state and order sizes:
//!
//! - `exit_sizes_for_pair` — pick exit qtys from the exchange snapshot
//!   when available, fall back to the bot-recorded entry sizes, and
//!   ultimately to `hedged_sizes` if both are missing.
//! - `sum_entry_sizes_by_symbol` — collapse `pending.legs` (which can
//!   hold two legs per symbol after a partial-fill reissue) to one
//!   total per symbol; static fn so reconcile can call it without
//!   borrowing the engine.
//! - `cap_exit_qty` — defensive cap when the exchange-reported size
//!   exceeds the bot-recorded entry by more than 5%; absorbs Extended
//!   partial-fill recovery drift (#259).
//! - `hedged_sizes` — fall-back sizing when the engine has no recorded
//!   position; shells out to `sizing::hedged_sizes` against the
//!   instance's `equity_reference_usd`.
//!
//! bot-strategy#413: the `evaluate_pair` delegate that used to live here
//! moved into `engine/step.rs::step_pair_shared` as part of the shared
//! per-pair pipeline. `pair_eval::evaluate_pair` is now called directly
//! at the single canonical site.

use anyhow::Result;
use rust_decimal::Decimal;

use super::super::config::PairSpec;
use super::super::market::SymbolSnapshot;
use super::super::sizing;
use super::super::state::PendingLeg;
use super::super::PairTradeEngine;

impl PairTradeEngine {
    pub(in crate::pairtrade) fn exit_sizes_for_pair(
        &self,
        inst_idx: usize,
        key: &str,
        pair: &PairSpec,
        beta: f64,
        p1: &SymbolSnapshot,
        p2: &SymbolSnapshot,
    ) -> Result<(Decimal, Decimal)> {
        let base_snapshot = self.open_positions.get(&pair.base);
        let quote_snapshot = self.open_positions.get(&pair.quote);
        let recorded = self.instances[inst_idx]
            .states
            .get(key)
            .and_then(|s| s.position.as_ref());
        let recorded_a = recorded.and_then(|p| p.entry_size_a);
        let recorded_b = recorded.and_then(|p| p.entry_size_b);

        if base_snapshot.is_some() || quote_snapshot.is_some() {
            let qty_a =
                Self::cap_exit_qty(key, &pair.base, base_snapshot.map(|p| p.size), recorded_a);
            let qty_b =
                Self::cap_exit_qty(key, &pair.quote, quote_snapshot.map(|p| p.size), recorded_b);
            return Ok((qty_a, qty_b));
        }

        let qty_a = recorded_a.unwrap_or(Decimal::ZERO);
        let qty_b = recorded_b.unwrap_or(Decimal::ZERO);

        if qty_a <= Decimal::ZERO && qty_b <= Decimal::ZERO {
            log::warn!(
                "[EXIT] {} missing position sizes from exchange/state; falling back to hedge sizing",
                key
            );
            // Exit fallback (no recorded sizes): unwind at base hedge ratio.
            //
            // Prefer `entry_sizing_beta` — the β this position's B leg was
            // ACTUALLY hedged against (floored at true entry, or the exact
            // notional_b/notional_a ratio when reconstructed from real fill/
            // exchange data; kept current by dispatch_rehedge on every
            // re-hedge fill). Using the caller's current `beta` instead
            // would under-close the B leg whenever it's evaluated below the
            // sizing floor (which a beta_floor close, by construction,
            // always is), and even `entry_beta` (the RAW β at entry, before
            // #798's floor is applied) can itself sit below the floor for a
            // position entered before this exit flag existed — re-flooring
            // that raw value with the CURRENT config is still only a guess
            // if the floor has since changed, and could equally over-close
            // (Codex review, bot-strategy#824). `entry_sizing_beta` is
            // `None` only for positions opened before this field existed;
            // for those this remains a best-effort guess.
            let unwind_beta = match recorded.and_then(|p| p.entry_sizing_beta) {
                Some(v) => v,
                None => {
                    let raw = recorded.and_then(|p| p.entry_beta).unwrap_or(beta);
                    let floor = self.pair_params_for(inst_idx, key).sizing_beta_floor;
                    sizing::resolve_sizing_beta(raw, floor)
                }
            };
            return self.hedged_sizes(inst_idx, pair, unwind_beta, p1, p2, 1.0, 0.0);
        }

        Ok((qty_a, qty_b))
    }

    /// Sum `target` across `pending.legs` per symbol so partial-fill
    /// reissue (which leaves a kept leg with `target=filled` and a new
    /// leg with `target=remaining` for the same symbol) records the full
    /// entry size. Returning the assignment-only last leg under-records
    /// and breaks the `cap_exit_qty` invariant (recorded ≈ true position).
    pub(in crate::pairtrade) fn sum_entry_sizes_by_symbol(
        legs: &[PendingLeg],
        base: &str,
        quote: &str,
    ) -> (Option<Decimal>, Option<Decimal>) {
        let (mut acc_a, mut acc_b) = (Decimal::ZERO, Decimal::ZERO);
        let (mut has_a, mut has_b) = (false, false);
        for leg in legs {
            if leg.symbol == base {
                acc_a += leg.target;
                has_a = true;
            } else if leg.symbol == quote {
                acc_b += leg.target;
                has_b = true;
            }
        }
        (
            if has_a { Some(acc_a) } else { None },
            if has_b { Some(acc_b) } else { None },
        )
    }

    /// Cap the entry reissue qty so a stale local `leg.filled` (e.g.
    /// across the cancel-then-reissue boundary in `reissue_partial_legs`)
    /// cannot cause a second full-size order on top of a leg the
    /// exchange has already filled. Returns the qty that should be
    /// reissued — `target - max(local_filled, exchange)` clamped to 0.
    /// `exchange = None` (query failed) falls back to local accounting.
    ///
    /// Entry-side analogue of [`Self::cap_exit_qty`] (bot-strategy#259) —
    /// the observed failure mode is the mirror image: bot thinks 0 was
    /// filled, exchange has `target` already filled, and the MARKET
    /// fallback path piles full-target on top, ending up at 2× target.
    /// First seen on Frankfurt 2026-05-22 06:27 UTC, variant C ETH leg
    /// (0.8905 target → 1.7810 actual). See bot-strategy#470.
    pub(in crate::pairtrade) fn cap_entry_reissue_remaining(
        target: Decimal,
        local_filled: Decimal,
        exchange: Option<Decimal>,
    ) -> Decimal {
        let effective_filled = match exchange {
            Some(exch) => local_filled.max(exch),
            None => local_filled,
        };
        (target - effective_filled).max(Decimal::ZERO)
    }

    /// Defensive cap for exit-leg sizing on Extended (and any other venue
    /// where `get_positions` can momentarily over-report after partial-fill
    /// retry recovery). When the exchange-reported size exceeds the
    /// bot-recorded entry size by more than 5%, log a WARN with both values
    /// and use the recorded size. Below 5% drift or when no recorded size
    /// exists (legacy startup), pass the exchange size through unchanged.
    /// See bot-strategy#259 for the original observation (LongSpread on
    /// Tokyo Extended exiting 2x the entry qty after partial-fill retry).
    pub(in crate::pairtrade) fn cap_exit_qty(
        key: &str,
        symbol: &str,
        exchange: Option<Decimal>,
        recorded: Option<Decimal>,
    ) -> Decimal {
        match (exchange, recorded) {
            (Some(exch), Some(rec)) if rec > Decimal::ZERO => {
                let cap = rec * Decimal::new(105, 2); // 1.05
                if exch > cap {
                    log::warn!(
                        "[EXIT_CAP] {} {} exchange size {} exceeds recorded entry {} by >5%; capping to recorded",
                        key,
                        symbol,
                        exch,
                        rec
                    );
                    rec
                } else {
                    exch
                }
            }
            (Some(exch), _) => exch,
            (None, Some(rec)) => rec,
            (None, None) => Decimal::ZERO,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::pairtrade) fn hedged_sizes(
        &self,
        inst_idx: usize,
        _pair: &PairSpec,
        beta: f64,
        p1: &SymbolSnapshot,
        p2: &SymbolSnapshot,
        notional_scale: f64,
        sizing_beta_floor: f64,
    ) -> Result<(Decimal, Decimal)> {
        let inst = &self.instances[inst_idx];
        let equity = inst.equity_reference_usd;
        let max_leverage = inst.max_leverage;
        sizing::hedged_sizes(
            &self.cfg,
            equity,
            max_leverage,
            beta,
            p1,
            p2,
            notional_scale,
            sizing_beta_floor,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Instant;

    use super::*;
    use crate::pairtrade::pending_tests::DummyConnector;
    use crate::pairtrade::state::{PairState, Position, PositionDirection};

    fn snapshot(price: &str) -> SymbolSnapshot {
        SymbolSnapshot {
            price: Decimal::from_str(price).unwrap(),
            funding_rate: Decimal::ZERO,
            bid_price: None,
            ask_price: None,
            bid_size: Decimal::ZERO,
            ask_size: Decimal::ZERO,
            min_order: None,
            min_tick: None,
            size_decimals: None,
            exchange_ts: None,
        }
    }

    fn engine_with_position(
        entry_beta: Option<f64>,
        entry_sizing_beta: Option<f64>,
    ) -> PairTradeEngine {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);
        engine.instances[0].default_pair_params.sizing_beta_floor = 0.5;
        let mut state = PairState::new(2.0);
        state.position = Some(Position {
            direction: PositionDirection::LongSpread,
            entered_at: Instant::now(),
            entered_ts: 1_000_000,
            entry_price_a: None,
            entry_price_b: None,
            // Both missing — the exact condition that reaches the
            // hedged_sizes fallback under test.
            entry_size_a: None,
            entry_size_b: None,
            entry_z: Some(2.0),
            entry_beta,
            entry_sizing_beta,
            last_rehedge_ts: None,
            rehedge_realized_pnl: None,
            prev_beta_for_velocity: None,
        });
        engine.instances[0]
            .states
            .insert("AAA/BBB".to_string(), state);
        engine
    }

    #[test]
    fn exit_fallback_uses_entry_sizing_beta_ignoring_caller_beta() {
        let engine = engine_with_position(Some(0.3), Some(0.8));
        let pair = PairSpec {
            base: "AAA".to_string(),
            quote: "BBB".to_string(),
        };
        let p1 = snapshot("100.0");
        let p2 = snapshot("50.0");
        // Two wildly different caller-supplied `beta` values (one of them
        // even below the configured floor, mimicking a beta_floor close)
        // must produce the SAME qty_b: entry_sizing_beta, not the caller's
        // beta, drives this fallback.
        let (_, qty_b_low) = engine
            .exit_sizes_for_pair(0, "AAA/BBB", &pair, 0.1, &p1, &p2)
            .unwrap();
        let (_, qty_b_high) = engine
            .exit_sizes_for_pair(0, "AAA/BBB", &pair, 0.95, &p1, &p2)
            .unwrap();
        assert_eq!(qty_b_low, qty_b_high);
        assert!(qty_b_low > Decimal::ZERO);
    }

    #[test]
    fn exit_fallback_without_entry_sizing_beta_floors_raw_entry_beta() {
        // entry_sizing_beta unavailable (legacy position) — falls back to
        // resolve_sizing_beta(entry_beta, current floor). entry_beta=0.3 is
        // below the configured floor (0.5), so the floored 0.5 must be
        // used, not the raw 0.3 nor the caller's beta.
        let with_floor = engine_with_position(Some(0.3), None);
        let without_floor_effect = engine_with_position(Some(0.5), None);
        let pair = PairSpec {
            base: "AAA".to_string(),
            quote: "BBB".to_string(),
        };
        let p1 = snapshot("100.0");
        let p2 = snapshot("50.0");
        let (_, qty_b_floored) = with_floor
            .exit_sizes_for_pair(0, "AAA/BBB", &pair, 0.99, &p1, &p2)
            .unwrap();
        let (_, qty_b_at_floor) = without_floor_effect
            .exit_sizes_for_pair(0, "AAA/BBB", &pair, 0.01, &p1, &p2)
            .unwrap();
        assert_eq!(qty_b_floored, qty_b_at_floor);
    }
}
