//! Position, pending-order, and related state types extracted from the
//! monolithic pairtrade module. Field visibility is `pub(super)` so that the
//! engine in `mod.rs` can keep accessing them as before; promotion to `pub`
//! is deferred until the engine itself migrates out.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::time::Instant;

use dex_connector::DexError;
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PositionDirection {
    LongSpread,
    ShortSpread,
}

#[derive(Debug, Clone)]
pub(super) struct Position {
    pub(super) direction: PositionDirection,
    pub(super) entered_at: Instant,
    /// Replay-aware entry timestamp (seconds). In live mode equals
    /// `chrono::Utc::now().timestamp()` at the moment of entry; in backtest
    /// mode equals the replay's logical timestamp. Used for all
    /// duration-based decisions (force_close, hold-time PnL, etc.) so they
    /// behave identically under replay.
    pub(super) entered_ts: i64,
    pub(super) entry_price_a: Option<Decimal>,
    pub(super) entry_price_b: Option<Decimal>,
    pub(super) entry_size_a: Option<Decimal>,
    pub(super) entry_size_b: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub(super) struct PendingLeg {
    pub(super) symbol: String,
    pub(super) order_id: String,
    pub(super) exchange_order_id: Option<String>,
    pub(super) target: Decimal,
    pub(super) filled: Decimal,
    pub(super) side: dex_connector::OrderSide,
    #[allow(dead_code)]
    pub(super) placed_price: Decimal,
}

#[derive(Debug)]
pub(super) struct PendingOrders {
    pub(super) legs: Vec<PendingLeg>,
    pub(super) direction: PositionDirection,
    pub(super) placed_at: Instant,
    pub(super) hedge_retry_count: u32,
    pub(super) post_only_hybrid: bool,
}

#[derive(Debug)]
pub(super) struct PendingStatus {
    pub(super) open_remaining: usize,
    pub(super) fills: HashMap<String, Decimal>,
    pub(super) open_ids: HashSet<String>,
}

#[derive(Debug)]
pub(super) struct PartialOrderPlacementError {
    pub(super) legs: Vec<PendingLeg>,
    pub(super) source: DexError,
}

impl PartialOrderPlacementError {
    pub(super) fn new(legs: Vec<PendingLeg>, source: DexError) -> Self {
        Self { legs, source }
    }

    pub(super) fn legs(&self) -> &[PendingLeg] {
        &self.legs
    }
}

impl std::fmt::Display for PartialOrderPlacementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to place all legs: {}", self.source)
    }
}

impl Error for PartialOrderPlacementError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
