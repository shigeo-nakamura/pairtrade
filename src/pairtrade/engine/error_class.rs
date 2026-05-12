use dex_connector::DexError;
use rust_decimal::Decimal;

use super::super::PairTradeEngine;

pub(in crate::pairtrade) fn is_inconsistent_state(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains("Inconsistent state")
}

pub(in crate::pairtrade) fn is_ticker_auth_error(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("403")
        || lower.contains("forbidden")
        || lower.contains("failed to deserialize response")
        || lower.contains("expected value at line 1 column 1")
}

pub(in crate::pairtrade) fn is_ticker_rate_limited(err: &DexError, msg: &str) -> bool {
    if matches!(err, DexError::RateLimited { .. }) {
        return true;
    }
    let lower = msg.to_ascii_lowercase();
    lower.contains("too many requests") || lower.contains("http 429")
}

pub(in crate::pairtrade) fn is_reduce_only_position_missing_error(err: &DexError) -> bool {
    let msg = match err {
        DexError::ServerResponse(message) | DexError::Other(message) => message,
        _ => return false,
    };
    let lower = msg.to_ascii_lowercase();
    lower.contains("position is missing for reduce-only order")
        || lower.contains("position is missing for reduce only order")
}

pub(in crate::pairtrade) fn is_reduce_only_size_mismatch_error(err: &DexError) -> bool {
    let msg = match err {
        DexError::ServerResponse(message) | DexError::Other(message) => message,
        _ => return false,
    };
    let lower = msg.to_ascii_lowercase();
    lower.contains("reduce-only order size exceeds position size")
        || lower.contains("reduce only order size exceeds position size")
}

pub(in crate::pairtrade) fn is_reduce_only_rejection(err: &DexError) -> bool {
    is_reduce_only_position_missing_error(err) || is_reduce_only_size_mismatch_error(err)
}

impl PairTradeEngine {
    pub(in crate::pairtrade) async fn log_inconsistent_state_debug(&mut self, err: &anyhow::Error) {
        if !is_inconsistent_state(err) {
            return;
        }

        // Log internal state for active pairs
        for inst in self.instances.iter() {
            for (key, state) in inst.states.iter() {
                let is_active = state.position.is_some()
                    || state.pending_entry.is_some()
                    || state.pending_exit.is_some()
                    || state.bt_deferred_exit.is_some()
                    || state.position_guard;
                if !is_active {
                    continue;
                }
                log::error!(
                    "[DEBUG][STATE] key={} position={:?} pending_entry={:?} pending_exit={:?} guard={} positions_ready={}",
                    key,
                    state.position,
                    state.pending_entry.as_ref().map(|p| p.legs.len()),
                    state.pending_exit.as_ref().map(|p| p.legs.len()),
                    state.position_guard,
                    self.positions_ready
                );
            }
        }

        // Log what the exchange reports for positions
        match self.connector.get_positions().await {
            Ok(pos) => {
                let filtered: Vec<_> = pos
                    .into_iter()
                    .filter(|p| p.sign != 0 && p.size > Decimal::ZERO)
                    .collect();
                log::error!("[DEBUG][EXCHANGE_POSITIONS] {:?}", filtered);
            }
            Err(get_err) => {
                log::error!(
                    "[DEBUG][EXCHANGE_POSITIONS] failed to fetch positions: {:?}",
                    get_err
                );
            }
        }
    }

    pub(in crate::pairtrade) async fn fetch_residual_position_size(
        &mut self,
        symbol: &str,
    ) -> Option<Decimal> {
        match self.connector.get_positions().await {
            Ok(positions) => {
                let pos = positions
                    .iter()
                    .find(|p| p.symbol == symbol && p.sign != 0 && p.size > Decimal::ZERO);
                match pos {
                    Some(p) => Some(p.size),
                    None => {
                        self.open_positions.remove(symbol);
                        Some(Decimal::ZERO)
                    }
                }
            }
            Err(err) => {
                log::warn!(
                    "[ORDER] residual-size check failed for {}: {:?}",
                    symbol,
                    err
                );
                None
            }
        }
    }

    pub(in crate::pairtrade) async fn confirm_reduce_only_position_missing(
        &mut self,
        symbol: &str,
    ) -> bool {
        let cached_has_position = self
            .open_positions
            .get(symbol)
            .map(|p| p.sign != 0 && p.size > Decimal::ZERO)
            .unwrap_or(false);
        if !cached_has_position && self.positions_ready {
            return true;
        }

        match self.connector.get_positions().await {
            Ok(positions) => {
                let has_position = positions
                    .iter()
                    .any(|p| p.symbol == symbol && p.sign != 0 && p.size > Decimal::ZERO);
                if !has_position {
                    self.open_positions.remove(symbol);
                    return true;
                }
            }
            Err(err) => {
                log::warn!(
                    "[ORDER] reduce-only missing check failed for {}: {:?}",
                    symbol,
                    err
                );
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dex_connector::DexError;

    // bot-strategy#258: Extended reduce-only error classification
    #[test]
    fn reduce_only_position_missing_matches_code_1137_message() {
        let err = DexError::ServerResponse(
            "Position is missing for reduce-only order".to_string(),
        );
        assert!(is_reduce_only_position_missing_error(&err));
        assert!(!is_reduce_only_size_mismatch_error(&err));
        assert!(is_reduce_only_rejection(&err));
    }

    #[test]
    fn reduce_only_size_mismatch_matches_code_1136_message() {
        let err = DexError::ServerResponse(
            "Reduce-only order size exceeds position size".to_string(),
        );
        assert!(!is_reduce_only_position_missing_error(&err));
        assert!(is_reduce_only_size_mismatch_error(&err));
        assert!(is_reduce_only_rejection(&err));
    }

    #[test]
    fn reduce_only_classifiers_ignore_unrelated_errors() {
        let err = DexError::ServerResponse("Insufficient balance".to_string());
        assert!(!is_reduce_only_position_missing_error(&err));
        assert!(!is_reduce_only_size_mismatch_error(&err));
        assert!(!is_reduce_only_rejection(&err));
    }

    // bot-strategy#281: classify Lighter REST 429 / DexError::RateLimited so
    // the step skips quietly instead of erroring out per cycle.
    #[test]
    fn ticker_rate_limited_matches_dex_error_variant() {
        let err = DexError::RateLimited { until_unix: 0 };
        let msg = err.to_string();
        assert!(is_ticker_rate_limited(&err, &msg));
        assert!(!is_ticker_auth_error(&msg));
    }

    #[test]
    fn ticker_rate_limited_matches_http_429_message() {
        let err = DexError::Other(
            "HTTP 429 Too Many Requests: {\"code\":23000,\"message\":\"Too Many Requests!\"}"
                .to_string(),
        );
        let msg = err.to_string();
        assert!(is_ticker_rate_limited(&err, &msg));
    }

    #[test]
    fn ticker_rate_limited_matches_too_many_requests_substring() {
        let err = DexError::Other("Other error: Too Many Requests!".to_string());
        let msg = err.to_string();
        assert!(is_ticker_rate_limited(&err, &msg));
    }

    #[test]
    fn ticker_rate_limited_ignores_unrelated_errors() {
        let err = DexError::Other("HTTP 500 Internal Server Error".to_string());
        let msg = err.to_string();
        assert!(!is_ticker_rate_limited(&err, &msg));
        let auth = DexError::Other("HTTP 403 Forbidden".to_string());
        let auth_msg = auth.to_string();
        assert!(!is_ticker_rate_limited(&auth, &auth_msg));
    }
}
