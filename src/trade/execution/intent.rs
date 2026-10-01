//! Intent-based execution layer (bot-strategy#1099, Step 2).
//!
//! A bot states WHAT it wants (symbol, side, quantity, reference price,
//! deadline, slippage bound) and an executor decides HOW. The default style
//! is [`ExecStyle::Taker`], today's immediate IOC path. `MakerFirst` (post
//! at/inside the touch, re-quote, then take the remainder at the deadline)
//! arrives in Step 2b.
//!
//! Step 2a is types plus a no-behaviour-change adapter: [`BookTaker`] runs a
//! book [`Executor`] exactly as before and returns its original
//! [`FillReport`] untouched. Only a derived [`ExecOutcome`] (per-fill rows
//! with role and cost) is added alongside it, so callers can migrate without
//! changing live order paths or the byte-identical book replay.

use anyhow::{bail, Result};

use crate::book::executor::{Executor, FillReport};
use crate::book::rebalance::{OrderIntent, Side as BookSide};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl From<BookSide> for Side {
    fn from(s: BookSide) -> Self {
        match s {
            BookSide::Buy => Side::Buy,
            BookSide::Sell => Side::Sell,
        }
    }
}

/// How the executor may work the order.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecStyle {
    /// Cross immediately (IOC within `max_slip_bps` of the reference).
    Taker,
    /// Rest post-only first, re-quote on moves, take the remainder by the
    /// deadline (Step 2b; not executable yet).
    MakerFirst(MakerFirstParams),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MakerFirstParams {
    /// Longest the order may rest before the remainder goes taker. Must fit
    /// inside the caller's tick budget (#1099 design invariant 6).
    pub maker_window_ms: u64,
    /// Re-quote when the touch moved by at least this many bps.
    pub requote_bps: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecIntent {
    pub symbol: String,
    pub side: Side,
    /// Absolute base quantity, already lot-rounded by the caller.
    pub qty: f64,
    /// Mid at decision time; costs are measured against it.
    pub reference_price: f64,
    pub reduce_only: bool,
    /// Hard completion bound for the whole intent (ms from start), if any.
    pub deadline_ms: Option<u64>,
    /// Slippage bound vs `reference_price` for any taker part.
    pub max_slip_bps: f64,
    pub style: ExecStyle,
}

impl ExecIntent {
    /// The equivalent intent for a book order (always `Taker`: the book
    /// runtime's live path is IOC-only today).
    pub fn from_book(intent: &OrderIntent, max_slip_bps: f64) -> Self {
        Self {
            symbol: intent.symbol.clone(),
            side: intent.side.into(),
            qty: intent.qty,
            reference_price: intent.reference_price,
            reduce_only: intent.reduce_only,
            deadline_ms: None,
            max_slip_bps,
            style: ExecStyle::Taker,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Maker,
    Taker,
}

/// One execution, with its cost measured against the intent's reference.
#[derive(Debug, Clone, PartialEq)]
pub struct FillRow {
    pub order_id: Option<String>,
    pub role: Role,
    pub price: f64,
    pub qty: f64,
    /// Venue fee in USD. `None` = real but unknown (never booked as 0).
    pub fee_usd: Option<f64>,
    /// Mid the cost is measured against (the intent's reference price).
    pub arrival_mid: f64,
    /// Signed slippage vs `arrival_mid` in bps; positive = paid (buy above
    /// / sell below the mid).
    pub slippage_bps: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecOutcome {
    pub fills: Vec<FillRow>,
    pub filled_qty: f64,
    pub position_after: Option<f64>,
    /// Orders whose state is unknown and must block re-sends on the symbol
    /// until resolved (always empty for a taker IOC).
    pub unresolved: Vec<String>,
}

impl ExecOutcome {
    /// Total paid cost in USD: slippage plus fees. `None` when any fee is
    /// unknown, so an unknown is never summed as zero.
    pub fn cost_usd(&self) -> Option<f64> {
        self.fills.iter().try_fold(0.0, |acc, f| {
            let slip = f.slippage_bps / 1e4 * f.arrival_mid * f.qty;
            f.fee_usd.map(|fee| acc + slip + fee)
        })
    }
}

/// Signed slippage in bps of `price` vs `mid` for `side` (positive = paid).
pub fn slippage_bps(side: Side, price: f64, mid: f64) -> f64 {
    if mid <= 0.0 {
        return 0.0;
    }
    let raw = (price - mid) / mid * 1e4;
    match side {
        Side::Buy => raw,
        Side::Sell => -raw,
    }
}

/// Derive the per-fill outcome of a book taker execution. A report with no
/// fill yields no rows.
pub fn outcome_from_fill_report(intent: &ExecIntent, report: &FillReport) -> ExecOutcome {
    let fills = if report.filled_qty > 0.0 {
        vec![FillRow {
            order_id: report.order_id.clone(),
            role: Role::Taker,
            price: report.fill_price,
            qty: report.filled_qty,
            fee_usd: report.fee_usd,
            arrival_mid: intent.reference_price,
            slippage_bps: slippage_bps(intent.side, report.fill_price, intent.reference_price),
        }]
    } else {
        Vec::new()
    };
    ExecOutcome {
        fills,
        filled_qty: report.filled_qty,
        position_after: report.position_after,
        unresolved: Vec::new(),
    }
}

/// No-behaviour-change adapter over a book [`Executor`]: the book order runs
/// exactly as `executor.execute(order)` would, and its [`FillReport`] is
/// returned untouched next to the derived [`ExecOutcome`]. The intent's
/// slippage bound is the executor's own ([`Executor::slippage_bound_bps`]),
/// never a separate value the execution path would ignore.
pub struct BookTaker<'a, E: Executor + ?Sized> {
    pub executor: &'a E,
}

impl<'a, E: Executor + ?Sized> BookTaker<'a, E> {
    /// The intent this adapter executes `order` as.
    pub fn intent_for(&self, order: &OrderIntent) -> ExecIntent {
        ExecIntent::from_book(order, self.executor.slippage_bound_bps())
    }

    pub async fn execute(&self, order: &OrderIntent) -> Result<(FillReport, ExecOutcome)> {
        let intent = self.intent_for(order);
        if intent.style != ExecStyle::Taker {
            bail!("BookTaker only executes Taker intents");
        }
        let report = self.executor.execute(order).await?;
        let outcome = outcome_from_fill_report(&intent, &report);
        Ok((report, outcome))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::executor::PaperExecutor;
    use crate::book::rebalance::IntentKind;

    fn book_order(side: BookSide, qty: f64, reduce_only: bool) -> OrderIntent {
        OrderIntent {
            symbol: "BTC".to_string(),
            side,
            qty,
            reference_price: 100.0,
            notional_usd: qty * 100.0,
            reduce_only,
            kind: if reduce_only {
                IntentKind::Reduce
            } else {
                IntentKind::Open
            },
        }
    }

    /// Codex on #371: the intent must carry the bound the wrapped executor
    /// really uses, not a separate number it ignores.
    #[tokio::test]
    async fn book_taker_reports_the_executors_own_slippage_bound() {
        let paper = PaperExecutor::new(100.0, 2.0);
        paper.set_price("BTC", 100.0).await;
        let taker = BookTaker { executor: &paper };
        let order = book_order(BookSide::Buy, 1.0, false);
        assert_eq!(taker.intent_for(&order).max_slip_bps, 100.0);
        let (_, outcome) = taker.execute(&order).await.unwrap();
        assert!((outcome.fills[0].slippage_bps - 100.0).abs() < 1e-6);
    }

    #[test]
    fn slippage_sign_is_cost_positive_for_both_sides() {
        assert!((slippage_bps(Side::Buy, 101.0, 100.0) - 100.0).abs() < 1e-9);
        assert!((slippage_bps(Side::Sell, 99.0, 100.0) - 100.0).abs() < 1e-9);
        assert!((slippage_bps(Side::Buy, 99.0, 100.0) + 100.0).abs() < 1e-9);
        assert_eq!(slippage_bps(Side::Buy, 1.0, 0.0), 0.0);
    }

    #[test]
    fn outcome_maps_one_taker_row_and_keeps_unknown_fees_unknown() {
        let intent = ExecIntent::from_book(&book_order(BookSide::Sell, 2.0, false), 25.0);
        let report = FillReport {
            requested_qty: 2.0,
            filled_qty: 1.5,
            fill_price: 99.5,
            fill_price_source: "venue_fills",
            fee_usd: None,
            order_id: Some("42".into()),
            venue_error: None,
            latency_ms: 300,
            position_after: Some(-1.5),
        };
        let o = outcome_from_fill_report(&intent, &report);
        assert_eq!(o.fills.len(), 1);
        let f = &o.fills[0];
        assert_eq!(
            (f.role, f.qty, f.price, f.order_id.as_deref()),
            (Role::Taker, 1.5, 99.5, Some("42"))
        );
        assert!((f.slippage_bps - 50.0).abs() < 1e-9);
        assert_eq!(f.fee_usd, None);
        assert_eq!(
            o.cost_usd(),
            None,
            "an unknown fee makes the cost unknown, not 0"
        );
        assert_eq!((o.filled_qty, o.position_after), (1.5, Some(-1.5)));
        assert!(o.unresolved.is_empty());

        let mut known = report.clone();
        known.fee_usd = Some(0.1);
        let c = outcome_from_fill_report(&intent, &known)
            .cost_usd()
            .unwrap();
        // 50 bp of 100 * 1.5 = 0.75, plus the fee.
        assert!((c - 0.85).abs() < 1e-9, "{c}");

        let mut none = report;
        none.filled_qty = 0.0;
        assert!(outcome_from_fill_report(&intent, &none).fills.is_empty());
    }

    /// The adapter must not change what the book executor does or reports:
    /// the FillReport it returns equals a direct call on an identical
    /// executor, field for field.
    #[tokio::test]
    async fn book_taker_returns_the_executors_report_untouched() {
        for (side, reduce) in [(BookSide::Buy, false), (BookSide::Sell, false)] {
            let order = book_order(side, 1.0, reduce);
            let direct = PaperExecutor::new(10.0, 2.0);
            direct.set_price("BTC", 100.0).await;
            let wrapped = PaperExecutor::new(10.0, 2.0);
            wrapped.set_price("BTC", 100.0).await;
            let expect = direct.execute(&order).await.unwrap();
            let (got, outcome) = BookTaker { executor: &wrapped }
                .execute(&order)
                .await
                .unwrap();
            assert_eq!(got, expect);
            assert_eq!(outcome.filled_qty, expect.filled_qty);
            assert_eq!(outcome.fills[0].fee_usd, expect.fee_usd);
            // Paper fills at mid ± 10 bp: the derived cost sees exactly that,
            // and it is within the executor's own bound.
            assert!((outcome.fills[0].slippage_bps - 10.0).abs() < 1e-6);
            assert!(outcome.fills[0].slippage_bps <= wrapped.slippage_bound_bps() + 1e-9);
        }
    }
}
