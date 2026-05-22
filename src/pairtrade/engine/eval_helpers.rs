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
            // bot-strategy#461's notional shrink is entry-side only — we
            // must not apply it here or we'd leave a residual position.
            return self.hedged_sizes(inst_idx, pair, beta, p1, p2, 1.0);
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

    pub(in crate::pairtrade) fn hedged_sizes(
        &self,
        inst_idx: usize,
        _pair: &PairSpec,
        beta: f64,
        p1: &SymbolSnapshot,
        p2: &SymbolSnapshot,
        notional_scale: f64,
    ) -> Result<(Decimal, Decimal)> {
        let inst = &self.instances[inst_idx];
        let equity = inst.equity_reference_usd;
        sizing::hedged_sizes(&self.cfg, equity, beta, p1, p2, notional_scale)
    }
}
