//! Small diagnostic / error-classification helpers extracted from the
//! monolithic pairtrade module.

use std::cmp::Ordering;
use std::collections::HashMap;

use dex_connector::{DexError, PositionSnapshot};

use super::market::SymbolSnapshot;

pub(super) fn is_inconsistent_state(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains("Inconsistent state")
}

pub(super) fn format_positions_summary(positions: &[PositionSnapshot]) -> String {
    let mut parts = Vec::with_capacity(positions.len());
    for position in positions {
        let side = match position.sign.cmp(&0) {
            Ordering::Greater => "LONG",
            Ordering::Less => "SHORT",
            Ordering::Equal => "FLAT",
        };
        let entry = position
            .entry_price
            .map(|price| price.to_string())
            .unwrap_or_else(|| "n/a".to_string());
        parts.push(format!(
            "{} {} size={} entry={}",
            position.symbol, side, position.size, entry
        ));
    }
    parts.join(", ")
}

pub(super) fn is_dust_position(
    snapshot: &PositionSnapshot,
    prices: &HashMap<String, SymbolSnapshot>,
) -> bool {
    let Some(symbol_snapshot) = prices.get(&snapshot.symbol) else {
        return false;
    };
    let Some(min_order) = symbol_snapshot.min_order else {
        return false;
    };
    snapshot.size < min_order
}

pub(super) fn is_ticker_auth_error(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("403")
        || lower.contains("forbidden")
        || lower.contains("failed to deserialize response")
        || lower.contains("expected value at line 1 column 1")
}

pub(super) fn is_reduce_only_position_missing_error(err: &DexError) -> bool {
    let msg = match err {
        DexError::ServerResponse(message) | DexError::Other(message) => message,
        _ => return false,
    };
    let lower = msg.to_ascii_lowercase();
    lower.contains("position is missing for reduce-only order")
        || lower.contains("position is missing for reduce only order")
}
