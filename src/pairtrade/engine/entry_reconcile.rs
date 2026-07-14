//! Post-entry venue-position reconciliation (bot-strategy#721).
//!
//! The amend → MARKET takeover path has a TOCTOU window: a late fill can
//! land on the old maker order after the remaining MARKET size was
//! computed, so the replacement MARKET order overfills the leg (Frankfurt
//! 2026-07-08 09:42:30 UTC, variant A ETH leg +7.10%). The cancel-ack +
//! refresh in `reconcile_entry` shrinks that window; this module is the
//! defense-in-depth invariant behind it:
//!
//! After every live entry reaches terminal execution state (`all_filled`),
//! the actual venue position per leg is reconciled against the intended
//! signed target. Entry completion is considered safe only when
//!
//! - the actual per-leg quantity is within one venue size tick of the
//!   intended target, or
//! - any excess has been closed with a reduce-only trim that preserves
//!   the intended pair position.
//!
//! Underfill is never trimmed. If the reconciliation or the trim fails,
//! NEW entries for the (variant, pair) fail closed — persisted via
//! `InstanceRiskState.entry_blocked_pairs`, surfaced in status.json and
//! Prometheus, cleared only by the RISK_ACK sentinel. Exit management is
//! unaffected.

use std::collections::HashMap;
use std::time::Duration;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tokio::time::sleep;

use super::super::execution_ledger::{self, ExecutionEntryReconcileRecord};
use super::super::market::SymbolSnapshot;
use super::super::prom;
use super::super::state::PendingLeg;
use super::super::PairTradeEngine;

/// Bounded stability polling for the initial per-symbol position read
/// (reads are served from the connector's WS-derived cache). Early settle
/// is allowed only for excess / sign-flip readings — a stale cache can
/// only under-report a just-filled entry, so clean readings (which lag
/// can fake, even repeatedly) always consume the full window. Guards
/// against the position endpoint lagging the fill endpoints that made
/// `all_filled` true (Codex review PR #168).
const POSITION_SETTLE_READ_ATTEMPTS: usize = 3;
const POSITION_SETTLE_READ_DELAY_MS: u64 = 250;

/// Bounded post-trim verification: the venue position endpoint can lag the
/// trim fill by a beat, so re-check a few times before declaring failure.
/// Every attempt (including the first) waits `TRIM_VERIFY_DELAY_MS` —
/// checking immediately after `create_order` returns would almost always
/// read the pre-fill position and burn an attempt (PR #168 review). Worst
/// case ~1s before the trim is declared unverified and entries fail closed.
const TRIM_VERIFY_ATTEMPTS: usize = 5;
const TRIM_VERIFY_DELAY_MS: u64 = 200;

/// Pure verdict of one per-symbol exposure comparison. All quantities are
/// signed (Long positive, Short negative).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pairtrade) enum ExposureVerdict {
    /// |actual − intended| within the size-tick tolerance.
    WithinTolerance,
    /// Same sign, |actual| exceeds |intended| by more than the tolerance.
    /// Payload is the excess magnitude to trim (always positive).
    TrimExcess(Decimal),
    /// Same sign (or flat), |actual| short of |intended| by more than the
    /// tolerance. Payload is the deficit magnitude. Never trimmed.
    Underfill(Decimal),
    /// The venue position points the opposite way from the intended entry —
    /// local state cannot be trusted; fail closed.
    SignFlip,
}

/// Compare a signed intended entry quantity against the signed venue
/// position. `tolerance` is one venue size tick (0 when unknown → exact
/// match required).
pub(in crate::pairtrade) fn exposure_verdict(
    intended: Decimal,
    actual: Decimal,
    tolerance: Decimal,
) -> ExposureVerdict {
    if !intended.is_zero()
        && !actual.is_zero()
        && intended.is_sign_positive() != actual.is_sign_positive()
    {
        return ExposureVerdict::SignFlip;
    }
    let excess = actual.abs() - intended.abs();
    if excess > tolerance {
        ExposureVerdict::TrimExcess(excess)
    } else if excess < -tolerance {
        ExposureVerdict::Underfill(-excess)
    } else {
        ExposureVerdict::WithinTolerance
    }
}

/// One venue size tick for `symbol`, mirroring the step used by the order
/// quantizers (`size_decimals` preferred, `min_order` fallback). Zero when
/// no snapshot is available — reconciliation then requires an exact match.
pub(in crate::pairtrade) fn venue_size_tick(
    symbol: &str,
    prices: &HashMap<String, SymbolSnapshot>,
) -> Decimal {
    prices
        .get(symbol)
        .and_then(|snapshot| {
            snapshot
                .size_decimals
                .map(|d| Decimal::new(1, d.min(28)))
                .or(snapshot.min_order)
        })
        .unwrap_or(Decimal::ZERO)
}

/// Sum the side-signed intended target per symbol over the entry legs
/// (Long positive, Short negative). Order-preserving so log/ledger rows
/// come out base-leg first.
fn intended_by_symbol(legs: &[PendingLeg]) -> Vec<(String, Decimal)> {
    let mut intended: Vec<(String, Decimal)> = Vec::new();
    for leg in legs {
        if leg.reduce_only {
            continue;
        }
        let signed = match leg.side {
            dex_connector::OrderSide::Long => leg.target,
            dex_connector::OrderSide::Short => -leg.target,
        };
        match intended.iter_mut().find(|(s, _)| s == &leg.symbol) {
            Some((_, acc)) => *acc += signed,
            None => intended.push((leg.symbol.clone(), signed)),
        }
    }
    intended.retain(|(_, qty)| !qty.is_zero());
    intended
}

/// Per-symbol outcome carried into the log / ledger / metric emission.
struct SymbolReconcileOutcome {
    action: &'static str,
    mismatch_kind: Option<&'static str>,
    trim_order_id: Option<String>,
    trim_qty: Option<Decimal>,
    residual_excess: Option<Decimal>,
    block_reason: Option<String>,
}

impl SymbolReconcileOutcome {
    fn clean(action: &'static str) -> Self {
        Self {
            action,
            mismatch_kind: None,
            trim_order_id: None,
            trim_qty: None,
            residual_excess: None,
            block_reason: None,
        }
    }
}

impl PairTradeEngine {
    /// Signed venue position for `symbol` (Long positive, Short negative).
    /// `Some(0)` = confirmed flat; `None` = the position fetch itself
    /// failed (treat as reconciliation failure, not as flat).
    async fn fetch_signed_position(&mut self, symbol: &str) -> Option<Decimal> {
        match self.connector.get_positions().await {
            Ok(positions) => Some(
                positions
                    .iter()
                    .find(|p| p.symbol == symbol && p.sign != 0 && p.size > Decimal::ZERO)
                    .map(|p| if p.sign < 0 { -p.size } else { p.size })
                    .unwrap_or(Decimal::ZERO),
            ),
            Err(err) => {
                log::warn!(
                    "[ENTRY_RECONCILE] signed-position fetch failed for {}: {:?}",
                    symbol,
                    err
                );
                None
            }
        }
    }

    /// Signed venue position, read until settled: the position endpoint can
    /// lag the fill/order endpoints that made `all_filled` true, so a
    /// single read could still show the pre-late-fill size and record a
    /// spurious `ok`/`underfill` — permanently missing the exact overfill
    /// this reconciliation exists to catch (Codex review PR #168).
    ///
    /// Settling is verdict-aware: during an entry the only recent venue
    /// events are fills that GROW the position, so a stale cache can only
    /// under-report — an excess or sign-flip reading is never a lag
    /// artifact and two consecutive identical bad readings settle early.
    /// Clean readings (ok/underfill) are exactly the ones lag can fake,
    /// even repeatedly from the same stale snapshot, so they must survive
    /// the full polling window; the last successful reading wins. `None`
    /// only when every read failed (treated as reconciliation failure
    /// upstream).
    async fn fetch_settled_signed_position(
        &mut self,
        symbol: &str,
        intended: Decimal,
        tolerance: Decimal,
    ) -> Option<Decimal> {
        let mut last_ok: Option<Decimal> = None;
        for attempt in 0..POSITION_SETTLE_READ_ATTEMPTS {
            if attempt > 0 {
                sleep(Duration::from_millis(POSITION_SETTLE_READ_DELAY_MS)).await;
            }
            if let Some(reading) = self.fetch_signed_position(symbol).await {
                if last_ok == Some(reading) {
                    match exposure_verdict(intended, reading, tolerance) {
                        ExposureVerdict::TrimExcess(_) | ExposureVerdict::SignFlip => {
                            return Some(reading)
                        }
                        ExposureVerdict::WithinTolerance | ExposureVerdict::Underfill(_) => {}
                    }
                }
                last_ok = Some(reading);
            }
        }
        last_ok
    }

    /// Reconcile the just-completed entry's per-leg venue exposure against
    /// the intended signed targets (bot-strategy#721). Called from the
    /// `all_filled` branch of `reconcile_entry`; live-trading only.
    pub(in crate::pairtrade) async fn reconcile_entry_exposure(
        &mut self,
        inst_idx: usize,
        key: &str,
        legs: &[PendingLeg],
        price_map: &HashMap<String, SymbolSnapshot>,
    ) {
        if self.cfg.dry_run || self.cfg.backtest_mode || self.cfg.observe_only {
            return;
        }
        let variant = self.instances[inst_idx].id.clone();
        for (symbol, intended) in intended_by_symbol(legs) {
            let tolerance = venue_size_tick(&symbol, price_map);
            let (actual, outcome) = match self
                .fetch_settled_signed_position(&symbol, intended, tolerance)
                .await
            {
                None => (
                    Decimal::ZERO,
                    SymbolReconcileOutcome {
                        action: "fetch_failed",
                        mismatch_kind: Some("fetch_failed"),
                        trim_order_id: None,
                        trim_qty: None,
                        residual_excess: None,
                        block_reason: Some(format!("entry_reconcile_fetch_failed_{}", symbol)),
                    },
                ),
                Some(actual) => {
                    let outcome = match exposure_verdict(intended, actual, tolerance) {
                        ExposureVerdict::WithinTolerance => SymbolReconcileOutcome::clean("ok"),
                        ExposureVerdict::Underfill(deficit) => SymbolReconcileOutcome {
                            action: "underfill",
                            mismatch_kind: Some("underfill"),
                            trim_order_id: None,
                            trim_qty: None,
                            residual_excess: Some(-deficit),
                            block_reason: None,
                        },
                        ExposureVerdict::SignFlip => SymbolReconcileOutcome {
                            action: "sign_flip",
                            mismatch_kind: Some("sign_flip"),
                            trim_order_id: None,
                            trim_qty: None,
                            residual_excess: None,
                            block_reason: Some(format!("entry_reconcile_sign_flip_{}", symbol)),
                        },
                        ExposureVerdict::TrimExcess(excess) => {
                            self.trim_entry_excess(TrimRequest {
                                variant: &variant,
                                key,
                                symbol: &symbol,
                                intended,
                                actual,
                                excess,
                                tolerance,
                                price_map,
                            })
                            .await
                        }
                    };
                    (actual, outcome)
                }
            };

            let excess = actual.abs() - intended.abs();
            let entries_blocked = outcome.block_reason.is_some();
            let log_line = format!(
                "[ENTRY_RECONCILE] {} {} {} intended={} actual={} excess={} tolerance={} action={}",
                variant, key, symbol, intended, actual, excess, tolerance, outcome.action
            );
            match outcome.action {
                "ok" => log::info!("{}", log_line),
                "trimmed" => log::warn!("{}", log_line),
                _ => log::error!("{}", log_line),
            }

            if let Some(kind) = outcome.mismatch_kind {
                prom::ENTRY_RECONCILE_MISMATCH_TOTAL
                    .with_label_values(&[variant.as_str(), key, symbol.as_str(), kind])
                    .inc();
            }
            let residual = outcome
                .residual_excess
                .unwrap_or(Decimal::ZERO)
                .max(Decimal::ZERO);
            prom::ENTRY_RECONCILE_RESIDUAL_EXCESS
                .with_label_values(&[variant.as_str(), key, symbol.as_str()])
                .set(residual.to_f64().unwrap_or(0.0));

            if let Some(ledger) = self.execution_ledger.as_mut() {
                ledger.write_entry_reconcile(&ExecutionEntryReconcileRecord {
                    event: "entry_reconcile",
                    ts_ms: execution_ledger::now_ms(),
                    variant: variant.clone(),
                    pair: key.to_string(),
                    symbol: symbol.clone(),
                    intended_qty: intended,
                    actual_qty: actual,
                    excess_qty: excess,
                    tolerance,
                    action: outcome.action.to_string(),
                    trim_order_id: outcome.trim_order_id.clone(),
                    trim_qty: outcome.trim_qty,
                    residual_excess: outcome.residual_excess,
                    entries_blocked,
                });
            }

            if let Some(reason) = outcome.block_reason {
                self.block_entries_for_exposure(inst_idx, key, &symbol, reason, intended, actual);
            }
        }
    }

    /// Close confirmed entry excess with a reduce-only MARKET order and
    /// verify the venue position converged back to the intended target.
    /// The trim quantity is floor-quantized and capped at the venue
    /// position so a reduce-only trim can never invert the position.
    async fn trim_entry_excess(&mut self, req: TrimRequest<'_>) -> SymbolReconcileOutcome {
        let TrimRequest {
            variant,
            key,
            symbol,
            intended,
            actual,
            excess,
            tolerance,
            price_map,
        } = req;
        let trim_qty = self.quantize_order_size(symbol, excess.min(actual.abs()), price_map);
        if trim_qty <= Decimal::ZERO || trim_qty > excess {
            // Excess above tolerance but not tradable as-is: either it
            // quantized to zero, or it sits below the venue min lot and
            // `quantize_order_size` rounded it UP to the minimum (Codex
            // review PR #168) — submitting that would trim more than the
            // confirmed excess and leave the leg under the intended
            // hedge. Leave the dust-level exposure in place, keep entries
            // open, but surface the residual for the operator.
            return SymbolReconcileOutcome {
                action: "excess_below_min_lot",
                mismatch_kind: Some("overfill"),
                trim_order_id: None,
                trim_qty: None,
                residual_excess: Some(excess),
                block_reason: None,
            };
        }
        let trim_side = if actual.is_sign_positive() {
            dex_connector::OrderSide::Short
        } else {
            dex_connector::OrderSide::Long
        };
        prom::ENTRY_RECONCILE_TRIM_TOTAL
            .with_label_values(&[variant, key, symbol, "attempted"])
            .inc();
        log::warn!(
            "[ENTRY_RECONCILE] {} {} {} trimming excess {} reduce-only ({:?}, intended={} actual={})",
            variant,
            key,
            symbol,
            trim_qty,
            trim_side,
            intended,
            actual
        );
        let trim_order_id = match self
            .connector
            .create_order(symbol, trim_qty, trim_side, None, None, true, None)
            .await
        {
            Ok(resp) => resp.order_id,
            Err(err) => {
                log::error!(
                    "[ENTRY_RECONCILE] {} {} {} excess trim submit failed: {:?}",
                    variant,
                    key,
                    symbol,
                    err
                );
                prom::ENTRY_RECONCILE_TRIM_TOTAL
                    .with_label_values(&[variant, key, symbol, "failed"])
                    .inc();
                return SymbolReconcileOutcome {
                    action: "trim_failed",
                    mismatch_kind: Some("overfill"),
                    trim_order_id: None,
                    trim_qty: Some(trim_qty),
                    residual_excess: Some(excess),
                    block_reason: Some(format!("entry_reconcile_trim_failed_{}", symbol)),
                };
            }
        };

        // Verify the venue converged back within tolerance. The trim is a
        // reduce-only MARKET order, so convergence is normally immediate;
        // the bounded re-check absorbs position-endpoint lag.
        let mut residual = excess;
        for _ in 0..TRIM_VERIFY_ATTEMPTS {
            sleep(Duration::from_millis(TRIM_VERIFY_DELAY_MS)).await;
            if let Some(after) = self.fetch_signed_position(symbol).await {
                match exposure_verdict(intended, after, tolerance) {
                    // Still over target — keep polling (position endpoint
                    // may lag the trim fill), fail after the last attempt.
                    ExposureVerdict::TrimExcess(remaining) => residual = remaining,
                    // The venue now points the opposite way from the
                    // intended entry — a reduce-only trim can never do
                    // that, so either the venue did not honor reduce-only
                    // semantics or something else moved the position mid-
                    // trim. Same fail-closed treatment as a pre-trim sign
                    // flip (Codex review PR #168).
                    ExposureVerdict::SignFlip => {
                        prom::ENTRY_RECONCILE_TRIM_TOTAL
                            .with_label_values(&[variant, key, symbol, "failed"])
                            .inc();
                        return SymbolReconcileOutcome {
                            action: "sign_flip",
                            mismatch_kind: Some("sign_flip"),
                            trim_order_id: Some(trim_order_id),
                            trim_qty: Some(trim_qty),
                            residual_excess: None,
                            block_reason: Some(format!(
                                "entry_reconcile_post_trim_sign_flip_{}",
                                symbol
                            )),
                        };
                    }
                    // Underfill after the trim is anomalous, not success:
                    // `trim_qty <= excess` by construction, so a reduce-
                    // only trim of our own can only land at-or-above the
                    // intended target. Dropping below it means something
                    // else moved the position mid-trim (external close,
                    // or the venue filled more than requested) — the
                    // model position now overstates the venue. Fail
                    // closed like the sign-flip path (Codex review
                    // PR #168).
                    ExposureVerdict::Underfill(deficit) => {
                        prom::ENTRY_RECONCILE_TRIM_TOTAL
                            .with_label_values(&[variant, key, symbol, "failed"])
                            .inc();
                        return SymbolReconcileOutcome {
                            action: "post_trim_underfill",
                            mismatch_kind: Some("underfill"),
                            trim_order_id: Some(trim_order_id),
                            trim_qty: Some(trim_qty),
                            residual_excess: Some(-deficit),
                            block_reason: Some(format!(
                                "entry_reconcile_post_trim_underfill_{}",
                                symbol
                            )),
                        };
                    }
                    // WithinTolerance is the expected convergence.
                    ExposureVerdict::WithinTolerance => {
                        prom::ENTRY_RECONCILE_TRIM_TOTAL
                            .with_label_values(&[variant, key, symbol, "succeeded"])
                            .inc();
                        return SymbolReconcileOutcome {
                            action: "trimmed",
                            mismatch_kind: Some("overfill"),
                            trim_order_id: Some(trim_order_id),
                            trim_qty: Some(trim_qty),
                            residual_excess: Some(Decimal::ZERO),
                            block_reason: None,
                        };
                    }
                }
            }
        }
        prom::ENTRY_RECONCILE_TRIM_TOTAL
            .with_label_values(&[variant, key, symbol, "failed"])
            .inc();
        SymbolReconcileOutcome {
            action: "trim_failed",
            mismatch_kind: Some("overfill"),
            trim_order_id: Some(trim_order_id),
            trim_qty: Some(trim_qty),
            residual_excess: Some(residual.max(Decimal::ZERO)),
            block_reason: Some(format!("entry_reconcile_trim_unverified_{}", symbol)),
        }
    }

    /// Fail closed for NEW entries on this (variant, pair): persist the
    /// block, surface it to Prometheus and the risk-event history, and
    /// tell the operator how to clear it. Exits keep running.
    fn block_entries_for_exposure(
        &mut self,
        inst_idx: usize,
        key: &str,
        symbol: &str,
        reason: String,
        intended: Decimal,
        actual: Decimal,
    ) {
        let variant = self.instances[inst_idx].id.clone();
        log::error!(
            "[ENTRY_RECONCILE] {} {} entries fail-closed: {} (intended={} actual={}); exits unaffected; clear via {}",
            variant,
            key,
            reason,
            intended,
            actual,
            super::super::risk_ack_path()
        );
        self.instances[inst_idx]
            .entry_blocked_pairs
            .insert(key.to_string(), reason.clone());
        prom::ENTRY_EXPOSURE_BLOCKED
            .with_label_values(&[variant.as_str(), key])
            .set(1);
        self.record_risk_event_for_instance(
            inst_idx,
            "entry_reconcile",
            "activated",
            Some(reason),
            Some(serde_json::json!({
                "pair": key,
                "symbol": symbol,
                "intended_qty": intended.to_f64(),
                "actual_qty": actual.to_f64(),
            })),
        );
        self.persist_risk_state();
    }
}

/// Inputs for `trim_entry_excess`, grouped to stay under the clippy
/// argument-count boundary.
struct TrimRequest<'a> {
    variant: &'a str,
    key: &'a str,
    symbol: &'a str,
    intended: Decimal,
    actual: Decimal,
    excess: Decimal,
    tolerance: Decimal,
    price_map: &'a HashMap<String, SymbolSnapshot>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(v: &str) -> Decimal {
        v.parse().unwrap()
    }

    #[test]
    fn verdict_within_tolerance_long_and_short() {
        // Exact match and one-tick drift both reconcile clean, in both
        // directions.
        let tol = dec("0.0001");
        assert_eq!(
            exposure_verdict(dec("2.4291"), dec("2.4291"), tol),
            ExposureVerdict::WithinTolerance
        );
        assert_eq!(
            exposure_verdict(dec("-2.4291"), dec("-2.4292"), tol),
            ExposureVerdict::WithinTolerance
        );
        assert_eq!(
            exposure_verdict(dec("2.4291"), dec("2.4290"), tol),
            ExposureVerdict::WithinTolerance
        );
    }

    #[test]
    fn verdict_trims_excess_on_both_sides() {
        // The 2026-07-08 event shape: intended short 2.4291, actual short
        // 2.6016 → excess 0.1725 to trim. The long mirror must behave
        // identically.
        let tol = dec("0.0001");
        assert_eq!(
            exposure_verdict(dec("-2.4291"), dec("-2.6016"), tol),
            ExposureVerdict::TrimExcess(dec("0.1725"))
        );
        assert_eq!(
            exposure_verdict(dec("2.4291"), dec("2.6016"), tol),
            ExposureVerdict::TrimExcess(dec("0.1725"))
        );
    }

    #[test]
    fn verdict_underfill_is_reported_not_trimmed() {
        let tol = dec("0.0001");
        assert_eq!(
            exposure_verdict(dec("-2.4291"), dec("-2.3"), tol),
            ExposureVerdict::Underfill(dec("0.1291"))
        );
        // Fully missing position is an underfill of the whole target,
        // never a sign flip.
        assert_eq!(
            exposure_verdict(dec("1.0"), Decimal::ZERO, tol),
            ExposureVerdict::Underfill(dec("1.0"))
        );
    }

    #[test]
    fn verdict_sign_flip_fails_closed() {
        let tol = dec("0.0001");
        assert_eq!(
            exposure_verdict(dec("-2.4291"), dec("0.5"), tol),
            ExposureVerdict::SignFlip
        );
        assert_eq!(
            exposure_verdict(dec("2.4291"), dec("-0.5"), tol),
            ExposureVerdict::SignFlip
        );
    }

    #[test]
    fn verdict_zero_tolerance_requires_exact_match() {
        assert_eq!(
            exposure_verdict(dec("1.0"), dec("1.0"), Decimal::ZERO),
            ExposureVerdict::WithinTolerance
        );
        assert_eq!(
            exposure_verdict(dec("1.0"), dec("1.0001"), Decimal::ZERO),
            ExposureVerdict::TrimExcess(dec("0.0001"))
        );
    }

    #[test]
    fn intended_by_symbol_signs_by_side_and_skips_reduce_only() {
        use dex_connector::OrderSide;
        let mk = |symbol: &str, target: &str, side: OrderSide, reduce_only: bool| PendingLeg {
            symbol: symbol.to_string(),
            order_id: "o".to_string(),
            exchange_order_id: None,
            target: dec(target),
            filled: Decimal::ZERO,
            side,
            submitted_qty: Decimal::ZERO,
            limit_price: None,
            reference_price: None,
            submit_ts_ms: 0,
            ack_ts_ms: None,
            decision_ts_ms: 0,
            submit_reference_price: None,
            submit_mid: None,
            submit_bid: None,
            submit_ask: None,
            client_order_id: None,
            reduce_only,
            post_only: false,
        };
        // Split-slice entry (settled slice + MARKET remainder) must sum
        // back to the full intended target per symbol.
        let legs = vec![
            mk("BTC", "0.04", OrderSide::Long, false),
            mk("ETH", "0.9433", OrderSide::Short, false),
            mk("ETH", "1.4858", OrderSide::Short, false),
            mk("ETH", "9.9", OrderSide::Long, true), // reduce-only ignored
        ];
        let intended = intended_by_symbol(&legs);
        assert_eq!(
            intended,
            vec![
                ("BTC".to_string(), dec("0.04")),
                ("ETH".to_string(), dec("-2.4291")),
            ]
        );
    }
}
