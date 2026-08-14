use super::engine::placement::ReissuePartialLegsRequest;
use super::pnl_log::PnlLogger;
use super::state::{PendingLeg, PendingOrders};
use super::*;
use async_trait::async_trait;
use dex_connector::{
    BalanceResponse, CanceledOrdersResponse, CreateOrderResponse, DexConnector, DexError,
    FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse, OrderBookLevel,
    OrderBookSnapshot, OrderSide, PositionSnapshot, TickerResponse, TpSl, TriggerOrderStyle,
};
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

fn dec(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

/// Per-call record captured by the test-only `DummyConnector`:
/// `(symbol, size, side, limit_price, post_only)`.
type DummyCall = (String, Decimal, OrderSide, Option<Decimal>, bool);
type ModifyCall = (
    String,
    String,
    Decimal,
    Decimal,
    Option<Decimal>,
    Option<i64>,
);

#[derive(Default)]
pub(in crate::pairtrade) struct DummyConnector {
    calls: Mutex<Vec<DummyCall>>,
    next_id: AtomicUsize,
    balance_calls: AtomicUsize,
    balance_equity: Mutex<Option<Decimal>>,
    /// bot-strategy#396: observe whether `force_close_on_startup` /
    /// `force_close_all_positions` reach the connector. Used to lock
    /// the dry_run short-circuit so a future refactor cannot silently
    /// turn it into a side-effecting path.
    positions_calls: AtomicUsize,
    close_all_calls: AtomicUsize,
    cancel_all_calls: AtomicUsize,
    /// bot-strategy#487: positions get_positions returns, and the venue
    /// min order size get_ticker advertises, so the startup force-close
    /// dust filter can be exercised. `close_all_positions` clears the
    /// matching entries to simulate a successful flatten.
    positions_to_return: Mutex<Vec<PositionSnapshot>>,
    min_order_to_return: Mutex<Option<Decimal>>,
    ticker_price_to_return: Mutex<Option<Decimal>>,
    order_book_to_return: Mutex<Option<OrderBookSnapshot>>,
    /// bot-strategy#471: per-call record of `modify_order`
    /// `(symbol, order_id, target_total, open_remaining, price, spread)`,
    /// the count of `cancel_order` calls, and a switch to force amend
    /// failure so the cancel+reissue fallback can be exercised.
    modify_calls: Mutex<Vec<ModifyCall>>,
    cancel_order_calls: AtomicUsize,
    modify_should_fail: AtomicBool,
    reject_priced_orders: AtomicBool,
    /// Codex review PR #159: count of `get_ticker` calls and an optional
    /// threshold after which `get_ticker` starts failing. With one ticker
    /// call per post-only retry attempt, this lets a test make the *first*
    /// refresh succeed and later/last attempts fail, exercising the
    /// "clear the cached submit snapshot when refresh fails" path.
    ticker_calls: AtomicUsize,
    ticker_fail_after_calls: Mutex<Option<usize>>,
    /// bot-strategy#721: scripted per-symbol responses so the cancel-ack
    /// wait + post-cancel fill refresh and the post-entry venue-position
    /// reconciliation can be driven deterministically. Each `get_*` call
    /// pops the front entry of its queue, holding on the last entry once
    /// the queue is down to one (steady state). Empty queue = the legacy
    /// default response.
    ///
    /// `open_ids_by_symbol`: sequences of still-open order-id sets.
    /// `filled_by_symbol`: sequences of `(order_id, filled_size)` rows.
    /// `positions_script`: sequences of full `get_positions` snapshots
    /// (falls back to `positions_to_return` when empty).
    open_ids_by_symbol: Mutex<HashMap<String, VecDeque<Vec<String>>>>,
    filled_by_symbol: Mutex<HashMap<String, VecDeque<FilledRows>>>,
    positions_script: Mutex<VecDeque<Vec<PositionSnapshot>>>,
    reject_reduce_only_orders: AtomicBool,
    /// Optional ordering assertion for bot-strategy#783: the capital guard
    /// must already be durable when the connector side effect begins.
    guard_path_expected_before_create: Mutex<Option<std::path::PathBuf>>,
    positions_should_fail: AtomicBool,
    /// bot-strategy#721: threshold after which `get_positions` starts
    /// failing (mirrors `ticker_fail_after_calls`), so a test can serve
    /// one successful stale read and then fail the settle re-reads.
    positions_fail_after_calls: Mutex<Option<usize>>,
    /// Codex P2 follow-up, bot-strategy#783: reject every create_order
    /// call with DexError::ServerResponse (a completed round trip
    /// carrying an explicit venue rejection) instead of the generic
    /// Transient `reject_priced_orders`/`reject_reduce_only_orders`
    /// produce, so tests can exercise the capital-guard unlatch path
    /// that only fires on a definitive no-order-created rejection.
    reject_orders_with_server_response: AtomicBool,
}

/// Scripted `(order_id, filled_size)` rows for one `get_filled_orders` call.
type FilledRows = Vec<(String, Decimal)>;

/// Pop the next scripted entry, holding on the last one (steady state).
fn pop_scripted<T: Clone>(queue: &mut VecDeque<T>) -> Option<T> {
    if queue.len() > 1 {
        queue.pop_front()
    } else {
        queue.front().cloned()
    }
}

#[async_trait]
impl DexConnector for DummyConnector {
    async fn start(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
        Ok(())
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        _test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        let calls = self.ticker_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(threshold) = *self.ticker_fail_after_calls.lock().unwrap() {
            if calls > threshold {
                return Err(DexError::Transient(format!(
                    "ticker refresh forced failure for {} (call {})",
                    symbol, calls
                )));
            }
        }
        Ok(TickerResponse {
            symbol: symbol.to_string(),
            price: self
                .ticker_price_to_return
                .lock()
                .unwrap()
                .unwrap_or_default(),
            min_order: *self.min_order_to_return.lock().unwrap(),
            ..Default::default()
        })
    }

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        if let Some(queue) = self.filled_by_symbol.lock().unwrap().get_mut(symbol) {
            if let Some(rows) = pop_scripted(queue) {
                return Ok(FilledOrdersResponse {
                    orders: rows
                        .into_iter()
                        .map(|(order_id, size)| dex_connector::FilledOrder {
                            order_id,
                            is_rejected: false,
                            trade_id: "trade".to_string(),
                            filled_side: None,
                            filled_size: Some(size),
                            filled_value: None,
                            filled_fee: None,
                            filled_ts_ms: None,
                        })
                        .collect(),
                });
            }
        }
        Ok(FilledOrdersResponse::default())
    }

    async fn get_canceled_orders(&self, _symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        Ok(CanceledOrdersResponse::default())
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        if let Some(queue) = self.open_ids_by_symbol.lock().unwrap().get_mut(symbol) {
            if let Some(ids) = pop_scripted(queue) {
                return Ok(OpenOrdersResponse {
                    orders: ids
                        .into_iter()
                        .map(|order_id| dex_connector::OpenOrder {
                            order_id,
                            symbol: symbol.to_string(),
                            side: OrderSide::Long,
                            size: Decimal::ZERO,
                            price: Decimal::ZERO,
                            status: "open".to_string(),
                        })
                        .collect(),
                });
            }
        }
        Ok(OpenOrdersResponse::default())
    }

    async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        self.balance_calls.fetch_add(1, Ordering::SeqCst);
        let equity = self.balance_equity.lock().unwrap().unwrap_or_default();
        Ok(BalanceResponse {
            equity,
            balance: equity,
            position_entry_price: None,
            position_sign: None,
        })
    }

    async fn get_combined_balance(
        &self,
    ) -> Result<dex_connector::CombinedBalanceResponse, DexError> {
        Ok(dex_connector::CombinedBalanceResponse::default())
    }

    async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
        let calls = self.positions_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.positions_should_fail.load(Ordering::SeqCst) {
            return Err(DexError::Transient(
                "positions fetch forced failure (test)".to_string(),
            ));
        }
        if let Some(threshold) = *self.positions_fail_after_calls.lock().unwrap() {
            if calls > threshold {
                return Err(DexError::Transient(format!(
                    "positions fetch forced failure (test, call {})",
                    calls
                )));
            }
        }
        if let Some(positions) = pop_scripted(&mut self.positions_script.lock().unwrap()) {
            return Ok(positions);
        }
        Ok(self.positions_to_return.lock().unwrap().clone())
    }

    async fn get_last_trades(&self, _symbol: &str) -> Result<LastTradesResponse, DexError> {
        Ok(LastTradesResponse::default())
    }

    async fn get_order_book(
        &self,
        _symbol: &str,
        _depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        Ok(self
            .order_book_to_return
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_default())
    }

    async fn clear_filled_order(&self, _symbol: &str, _trade_id: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_canceled_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        _spread: Option<i64>,
        reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        if let Some(path) = self
            .guard_path_expected_before_create
            .lock()
            .unwrap()
            .as_ref()
        {
            assert!(
                path.exists(),
                "capital guard must be persisted before connector create_order begins"
            );
        }
        let order_id = format!("test-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
        let ordered_price = price.unwrap_or(Decimal::ONE);
        self.calls
            .lock()
            .unwrap()
            .push((symbol.to_string(), size, side, price, reduce_only));
        if self.reject_orders_with_server_response.load(Ordering::SeqCst) {
            return Err(DexError::ServerResponse(
                "insufficient balance (test)".to_string(),
            ));
        }
        if self.reject_priced_orders.load(Ordering::SeqCst) && price.is_some() {
            return Err(DexError::Transient("priced order rejected".to_string()));
        }
        if self.reject_reduce_only_orders.load(Ordering::SeqCst) && reduce_only {
            return Err(DexError::Transient(
                "reduce-only order rejected (test)".to_string(),
            ));
        }
        Ok(CreateOrderResponse {
            order_id,
            exchange_order_id: None,
            ordered_price,
            ordered_size: size,
            client_order_id: None,
        })
    }

    async fn create_advanced_trigger_order(
        &self,
        _symbol: &str,
        _size: Decimal,
        _side: OrderSide,
        _trigger_px: Decimal,
        _limit_px: Option<Decimal>,
        _order_style: TriggerOrderStyle,
        _slippage_bps: Option<u32>,
        _tpsl: TpSl,
        _reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(DexError::Permanent("not used".to_string()))
    }

    // Required since bot-strategy#536 (no trait default): tests never use
    // the IOC taker path.
    async fn create_order_taker_ioc(
        &self,
        _symbol: &str,
        _size: Decimal,
        _side: OrderSide,
        _slippage_bps: u32,
        _reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(DexError::Permanent("not used".to_string()))
    }

    async fn modify_order(
        &self,
        symbol: &str,
        order_id: &str,
        _side: OrderSide,
        target_total_size: Decimal,
        open_remaining_size: Decimal,
        price: Option<Decimal>,
        spread: Option<i64>,
        _reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        self.modify_calls.lock().unwrap().push((
            symbol.to_string(),
            order_id.to_string(),
            target_total_size,
            open_remaining_size,
            price,
            spread,
        ));
        if self.modify_should_fail.load(Ordering::SeqCst) {
            return Err(DexError::Permanent("amend unsupported (test)".to_string()));
        }
        // Same order_id back == native in-place amend (Lighter shape).
        Ok(CreateOrderResponse {
            order_id: order_id.to_string(),
            exchange_order_id: None,
            ordered_price: price.unwrap_or(Decimal::ONE),
            ordered_size: open_remaining_size,
            client_order_id: None,
        })
    }

    async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        self.cancel_order_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
        self.cancel_all_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn cancel_orders(
        &self,
        _symbol: Option<String>,
        _order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        Ok(())
    }

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError> {
        self.close_all_calls.fetch_add(1, Ordering::SeqCst);
        // Faithfully model the Extended connector for bot-strategy#487:
        // a sub-min position aborts the close with InvalidInput
        // (round_size_for_market) before any later leg is flattened.
        // `None` therefore strands everything when dust is present;
        // `Some(dust_symbol)` rejects outright. Above-min closes clear.
        let min_order = *self.min_order_to_return.lock().unwrap();
        let is_dust = |p: &PositionSnapshot| min_order.is_some_and(|min| p.size < min);
        let reject = |p: &PositionSnapshot| DexError::InvalidInput {
            field: "size".to_string(),
            value: format!("{} below min for {}", p.size, p.symbol),
        };
        let mut positions = self.positions_to_return.lock().unwrap();
        match symbol {
            Some(sym) => {
                if let Some(dust) = positions.iter().find(|p| p.symbol == sym && is_dust(p)) {
                    return Err(reject(dust));
                }
                positions.retain(|p| p.symbol != sym);
            }
            None => {
                if let Some(dust) = positions.iter().find(|p| is_dust(p)) {
                    return Err(reject(dust));
                }
                positions.clear();
            }
        }
        Ok(())
    }

    async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
        false
    }

    async fn sign_evm_65b(&self, _message: &str) -> Result<String, DexError> {
        Ok("signed".to_string())
    }

    async fn sign_evm_65b_with_eip191(&self, _message: &str) -> Result<String, DexError> {
        Ok("signed".to_string())
    }

    // Required since bot-strategy#536 (no trait default): tests never
    // subscribe to the push price feed.
    fn subscribe_price_updates(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<dex_connector::PriceUpdate>, DexError> {
        Err(DexError::Permanent("not used".to_string()))
    }
}

#[tokio::test]
async fn reissue_partial_entry_leg_reorders_remaining() {
    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    let pending = PendingOrders {
        legs: vec![PendingLeg {
            symbol: "AAA".to_string(),
            order_id: "leg1".to_string(),
            exchange_order_id: None,
            target: dec("0.05"),
            filled: Decimal::ZERO,
            side: OrderSide::Long,
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
            reduce_only: false,
            post_only: false,
        }],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    };
    let mut price_map = HashMap::new();
    price_map.insert(
        "AAA".to_string(),
        SymbolSnapshot {
            price: dec("100.0"),
            funding_rate: Decimal::ZERO,
            bid_price: None,
            ask_price: None,
            bid_size: Decimal::ZERO,
            ask_size: Decimal::ZERO,
            min_order: Some(dec("0.001")),
            min_tick: Some(dec("0.001")),
            size_decimals: Some(3),
            exchange_ts: None,
        },
    );
    let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);

    let result = engine
        .reissue_partial_legs(ReissuePartialLegsRequest {
            pending: &pending,
            filled_qtys: &filled_qtys,
            price_map: &price_map,
            reduce_only: false,
            use_market: false,
            retry_count: 0,
            use_amend: false,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.legs.len(), 2);
    assert!(result
        .legs
        .iter()
        .any(|leg| leg.target == dec("0.02") && leg.filled == dec("0.02")));
    assert!(result
        .legs
        .iter()
        .any(|leg| leg.target == dec("0.03") && leg.filled == Decimal::ZERO));
    let calls = connector.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "AAA");
    assert_eq!(calls[0].3, Some(dec("100.0")));
    assert!(!calls[0].4);
}

// bot-strategy#471: with `use_amend=true` and a venue that supports a
// native in-place amend (same order_id back), the partial-fill reissue
// amends the resting order instead of cancel+reissue. The order keeps its
// identity and is recorded as a single continuing leg (not a settled
// leg + fresh remainder), and no `create_order` / `cancel_order` fires.
#[tokio::test]
async fn amend_partial_entry_leg_modifies_in_place() {
    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    let pending = PendingOrders {
        legs: vec![PendingLeg {
            symbol: "AAA".to_string(),
            order_id: "leg1".to_string(),
            exchange_order_id: None,
            target: dec("0.05"),
            filled: Decimal::ZERO,
            side: OrderSide::Long,
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
            reduce_only: false,
            post_only: false,
        }],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    };
    let mut price_map = HashMap::new();
    price_map.insert(
        "AAA".to_string(),
        SymbolSnapshot {
            price: dec("100.0"),
            funding_rate: Decimal::ZERO,
            bid_price: None,
            ask_price: None,
            bid_size: Decimal::ZERO,
            ask_size: Decimal::ZERO,
            min_order: Some(dec("0.001")),
            min_tick: Some(dec("0.001")),
            size_decimals: Some(3),
            exchange_ts: None,
        },
    );
    let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);

    let result = engine
        .reissue_partial_legs(ReissuePartialLegsRequest {
            pending: &pending,
            filled_qtys: &filled_qtys,
            price_map: &price_map,
            reduce_only: false,
            use_market: false,
            retry_count: 0,
            use_amend: true,
        })
        .await
        .unwrap()
        .unwrap();

    // Single continuing leg keeping the original order_id + total target.
    assert_eq!(result.legs.len(), 1);
    assert_eq!(result.legs[0].order_id, "leg1");
    assert_eq!(result.legs[0].target, dec("0.05"));
    assert_eq!(result.legs[0].filled, dec("0.02"));

    // Amend hit the connector with the original total + capped remainder.
    let modify_calls = connector.modify_calls.lock().unwrap();
    assert_eq!(modify_calls.len(), 1);
    assert_eq!(modify_calls[0].0, "AAA");
    assert_eq!(modify_calls[0].1, "leg1");
    assert_eq!(modify_calls[0].2, dec("0.05")); // target_total
    assert_eq!(modify_calls[0].3, dec("0.03")); // open_remaining (capped)
    assert_eq!(modify_calls[0].4, Some(dec("100.0")));
    assert_eq!(modify_calls[0].5, None); // non-post-only leg → plain limit

    // No cancel+reissue on the amend happy path.
    assert!(connector.calls.lock().unwrap().is_empty());
    assert_eq!(connector.cancel_order_calls.load(Ordering::SeqCst), 0);
}

// bot-strategy#471: Extended's edit endpoint only permits price/size
// changes — an edit that flips postOnly is rejected wholesale with 1133
// InvalidOrderParameters (every first-retry amend of a post-only entry
// leg failed this way on the 2026-06-09..12 Tokyo soak). The amend must
// re-assert the original order's post-only flag (spread Some(-2)) and
// carry it onto the rebuilt continuing leg so later amends of the same
// leg stay consistent.
#[tokio::test]
async fn amend_post_only_leg_reasserts_post_only() {
    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    let pending = PendingOrders {
        legs: vec![PendingLeg {
            symbol: "AAA".to_string(),
            order_id: "leg1".to_string(),
            exchange_order_id: None,
            target: dec("0.05"),
            filled: Decimal::ZERO,
            side: OrderSide::Long,
            submitted_qty: Decimal::ZERO,
            limit_price: Some(dec("100.0")),
            reference_price: None,
            submit_ts_ms: 0,
            ack_ts_ms: None,
            decision_ts_ms: 0,
            submit_reference_price: None,
            submit_mid: None,
            submit_bid: None,
            submit_ask: None,
            client_order_id: None,
            reduce_only: false,
            post_only: true,
        }],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: true,
        exit_taker_takeover_at: None,
    };
    let mut price_map = HashMap::new();
    price_map.insert(
        "AAA".to_string(),
        SymbolSnapshot {
            price: dec("100.0"),
            funding_rate: Decimal::ZERO,
            bid_price: None,
            ask_price: None,
            bid_size: Decimal::ZERO,
            ask_size: Decimal::ZERO,
            min_order: Some(dec("0.001")),
            min_tick: Some(dec("0.001")),
            size_decimals: Some(3),
            exchange_ts: None,
        },
    );
    let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);

    let result = engine
        .reissue_partial_legs(ReissuePartialLegsRequest {
            pending: &pending,
            filled_qtys: &filled_qtys,
            price_map: &price_map,
            reduce_only: false,
            use_market: false,
            retry_count: 0,
            use_amend: true,
        })
        .await
        .unwrap()
        .unwrap();

    let modify_calls = connector.modify_calls.lock().unwrap();
    assert_eq!(modify_calls.len(), 1);
    // Post-only re-asserted: spread Some(-2) with a maker price (the
    // mock ticker has no book, so the price_map limit is the fallback).
    assert_eq!(modify_calls[0].4, Some(dec("100.0")));
    assert_eq!(modify_calls[0].5, Some(-2));
    // The continuing leg still knows it rests post-only.
    assert_eq!(result.legs.len(), 1);
    assert!(result.legs[0].post_only);
    // Happy path: no cancel+reissue fired.
    assert!(connector.calls.lock().unwrap().is_empty());
    assert_eq!(connector.cancel_order_calls.load(Ordering::SeqCst), 0);
}

// bot-strategy#471: when the venue rejects the amend (or doesn't support
// it), the reissue falls back to cancel+reissue for that leg — cancelling
// the resting order and placing a fresh remainder order — so the end
// state matches the legacy path (settled filled leg + new remainder leg).
#[tokio::test]
async fn amend_falls_back_to_cancel_reissue_on_error() {
    let connector = Arc::new(DummyConnector::default());
    connector.modify_should_fail.store(true, Ordering::SeqCst);
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    let pending = PendingOrders {
        legs: vec![PendingLeg {
            symbol: "AAA".to_string(),
            order_id: "leg1".to_string(),
            exchange_order_id: None,
            target: dec("0.05"),
            filled: Decimal::ZERO,
            side: OrderSide::Long,
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
            reduce_only: false,
            post_only: false,
        }],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    };
    let mut price_map = HashMap::new();
    price_map.insert(
        "AAA".to_string(),
        SymbolSnapshot {
            price: dec("100.0"),
            funding_rate: Decimal::ZERO,
            bid_price: None,
            ask_price: None,
            bid_size: Decimal::ZERO,
            ask_size: Decimal::ZERO,
            min_order: Some(dec("0.001")),
            min_tick: Some(dec("0.001")),
            size_decimals: Some(3),
            exchange_ts: None,
        },
    );
    let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);

    let result = engine
        .reissue_partial_legs(ReissuePartialLegsRequest {
            pending: &pending,
            filled_qtys: &filled_qtys,
            price_map: &price_map,
            reduce_only: false,
            use_market: false,
            retry_count: 0,
            use_amend: true,
        })
        .await
        .unwrap()
        .unwrap();

    // Same two-leg shape as the legacy cancel+reissue path.
    assert_eq!(result.legs.len(), 2);
    assert!(result
        .legs
        .iter()
        .any(|leg| leg.target == dec("0.02") && leg.filled == dec("0.02")));
    assert!(result
        .legs
        .iter()
        .any(|leg| leg.target == dec("0.03") && leg.filled == Decimal::ZERO));

    // Amend was attempted, then fell back to cancel + create.
    assert_eq!(connector.modify_calls.lock().unwrap().len(), 1);
    assert_eq!(connector.cancel_order_calls.load(Ordering::SeqCst), 1);
    assert_eq!(connector.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn reissue_partial_entry_missing_price_keeps_pending() {
    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    let pending = PendingOrders {
        legs: vec![PendingLeg {
            symbol: "AAA".to_string(),
            order_id: "leg1".to_string(),
            exchange_order_id: None,
            target: dec("0.05"),
            filled: Decimal::ZERO,
            side: OrderSide::Long,
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
            reduce_only: false,
            post_only: false,
        }],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    };
    let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);
    let empty_price_map = HashMap::new();

    let result = engine
        .reissue_partial_legs(ReissuePartialLegsRequest {
            pending: &pending,
            filled_qtys: &filled_qtys,
            price_map: &empty_price_map,
            reduce_only: false,
            use_market: false,
            retry_count: 0,
            use_amend: false,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.legs.len(), 1);
    assert_eq!(result.legs[0].target, dec("0.05"));
    assert_eq!(result.legs[0].filled, dec("0.02"));
}

#[tokio::test]
async fn refresh_equity_if_needed_skips_when_cache_is_fresh() {
    let connector = Arc::new(DummyConnector::default());
    *connector.balance_equity.lock().unwrap() = Some(dec("1234.56"));
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.instances[0].last_equity_fetch = Some(Instant::now());
    let initial_equity = engine.instances[0].equity_cache;

    engine.refresh_equity_if_needed(0).await.unwrap();

    assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 0);
    assert_eq!(engine.instances[0].equity_cache, initial_equity);
}

#[tokio::test]
async fn refresh_equity_if_needed_fetches_when_cache_is_stale() {
    let connector = Arc::new(DummyConnector::default());
    *connector.balance_equity.lock().unwrap() = Some(dec("1234.56"));
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.instances[0].last_equity_fetch =
        Some(Instant::now() - Duration::from_secs(EQUITY_REFRESH_CACHE_SECS + 1));

    engine.refresh_equity_if_needed(0).await.unwrap();

    assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 1);
    assert!((engine.instances[0].equity_cache - 1234.56).abs() < 1e-6);
}

#[tokio::test]
async fn fetch_equity_rest_bypasses_cache() {
    // Pre-entry path must hit REST regardless of cache age so the
    // about-to-be-placed order is sized against a current value.
    let connector = Arc::new(DummyConnector::default());
    *connector.balance_equity.lock().unwrap() = Some(dec("777.0"));
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.instances[0].last_equity_fetch = Some(Instant::now());

    engine.fetch_equity_rest(0).await;

    assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 1);
    assert!((engine.instances[0].equity_cache - 777.0).abs() < 1e-6);
}

#[tokio::test]
async fn fetch_equity_rest_observe_only_skips_connector() {
    let connector = Arc::new(DummyConnector::default());
    *connector.balance_equity.lock().unwrap() = Some(dec("777.0"));
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.observe_only = true;
    engine.instances[0].last_equity_fetch = None;
    let seed_cache = engine.instances[0].equity_cache;

    engine.fetch_equity_rest(0).await;

    assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 0);
    assert!((engine.instances[0].equity_cache - seed_cache).abs() < 1e-9);
    assert!(!engine.instances[0].equity_initialized);
    assert!(engine.instances[0].last_equity_fetch.is_some());
}

// bot-strategy#366: reproduce the restart race that synthesised a 50%
// DD on Frankfurt Round 4 Step 4 partial. Persisted `equity_samples`
// hold yesterday's intraday peak (~$1003) but `equity_cache` is the
// seed `equity_reference_usd` (~$500) until the first WS account
// dump propagates. Pre-fix, `evaluate_session_dd` would trip
// `session_halted=true` against the synthetic 5018 bps reading.
// Post-fix, the `equity_initialized` gate suppresses the gate and
// sampling until a connector-sourced balance lands.
#[tokio::test]
async fn session_dd_gated_until_equity_initialized() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    // Match the live config that produced the incident.
    engine.cfg.risk.max_session_loss_bps = 500;
    engine.cfg.max_leverage = 10.0;
    // Redirect risk-state persistence to a temp dir so the test does
    // not litter the working directory if a trip ever does fire.
    let dir = TempDir::new().unwrap();
    engine.risk_state_path = dir.path().join("risk_state.json");

    // Simulate restart state: persisted samples carry a $1003 peak,
    // `equity_cache` is still the $500 seed, and the connector has
    // not yet pushed a balance update (`equity_initialized=false`).
    // Sample ts is anchored close to "now" so the lookback prune
    // inside `update_equity_sample` does not discard it.
    let now_ts = engine.current_now_ts();
    {
        let inst = &mut engine.instances[0];
        inst.equity_samples = vec![risk_io::EquitySample {
            ts: now_ts - 60,
            equity: 1003.45,
        }];
        inst.equity_cache = 500.0;
        inst.equity_initialized = false;
        inst.session_halted = false;
    }

    // Phase 1: pre-WS-dump tick. Without the gate, peak=1003.45 vs
    // current=500 would compute dd_bps ≈ 5018 against an effective
    // threshold of 5000 bps and trip the halt. The gate must
    // suppress the evaluation entirely.
    let halted = engine.evaluate_session_dd(0).await;
    assert!(
        !halted,
        "session_dd must not trip while equity_initialized=false"
    );
    assert!(!engine.instances[0].session_halted);
    assert!(engine.instances[0].session_halt_reason.is_none());

    // Sampling must also be suppressed so the deque is not polluted
    // with the seed equity, which would distort post-init peaks.
    let pre_len = engine.instances[0].equity_samples.len();
    engine.update_equity_sample(0);
    assert_eq!(
        engine.instances[0].equity_samples.len(),
        pre_len,
        "update_equity_sample must not append while equity_initialized=false"
    );

    // The status snapshot path is gated identically so dashboards
    // do not render a phantom 50% DD card during the race window.
    assert!(engine.session_risk_snapshot(0).is_none());

    // Phase 2: WS dump lands. `equity_cache` is now the real wallet
    // balance and the gate releases.
    {
        let inst = &mut engine.instances[0];
        inst.equity_cache = 1000.84;
        inst.equity_initialized = true;
    }
    let halted = engine.evaluate_session_dd(0).await;
    assert!(
        !halted,
        "real equity ($1000.84 vs peak $1003.45) is well below threshold"
    );
    assert!(!engine.instances[0].session_halted);

    // The snapshot now exposes the real, non-phantom DD reading,
    // computed against the persisted peak. Check this before
    // `update_equity_sample` runs so the hourly-bucket replacement
    // in the sampler does not rewrite the persisted entry.
    let snapshot = engine
        .session_risk_snapshot(0)
        .expect("snapshot should exist post-init");
    assert!((snapshot.current_equity - 1000.84).abs() < 1e-6);
    assert!((snapshot.peak_equity - 1003.45).abs() < 1e-6);
    assert!(snapshot.dd_bps < 100.0);

    // Sampling now records the real value (possibly by replacing
    // the persisted entry if it falls in the same bucket).
    engine.update_equity_sample(0);
    assert!(
        engine.instances[0]
            .equity_samples
            .iter()
            .any(|s| (s.equity - 1000.84).abs() < 1e-6),
        "post-init sample with real equity must be persisted to the deque"
    );
}

// bot-strategy#382: dex-connector's WS-derived balance cache can return
// Ok(equity=0) for the first few `get_balance` calls after restart,
// before the first account dump lands. Pre-fix this propagated to
// `reporter.update_equity(0)`, locking `equity_day_start = 0` for the
// rest of the UTC day, and surfacing `pnl_today = +<full equity>` on
// the dashboard once the real balance arrived. Observed live on Tokyo
// Lighter B/C after the 2026-05-13 06:50 UTC restart: pnl_today=+$150
// with no trades executed.
//
// The companion to bot-strategy#366 is to drop the 0-valued reading
// entirely during the pre-init window; once the gate flips on the
// first positive equity, subsequent 0 readings ARE accepted (a
// genuinely rekt bot should be reflected on dashboards).
#[tokio::test]
async fn fetch_equity_rest_drops_zero_reading_before_init() {
    let connector = Arc::new(DummyConnector::default());
    // Phase 1: WS cache empty — connector returns equity=0.
    *connector.balance_equity.lock().unwrap() = Some(Decimal::from(0));
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    let seed_cache = engine.instances[0].equity_cache;
    assert!(
        !engine.instances[0].equity_initialized,
        "test_instance must start uninitialized"
    );

    engine.fetch_equity_rest(0).await;

    // The connector was hit (the function did not short-circuit), but
    // the 0 reading must not have armed the init flag, must not have
    // overwritten the seed cache, and must have left `last_equity_fetch`
    // populated so the refresh-cooldown timer still advances.
    assert_eq!(
        connector.balance_calls.load(Ordering::SeqCst),
        1,
        "fetch_equity_rest must hit the connector"
    );
    assert!(
        !engine.instances[0].equity_initialized,
        "0-valued reading must not arm equity_initialized"
    );
    assert!(
        (engine.instances[0].equity_cache - seed_cache).abs() < 1e-9,
        "0-valued reading must not overwrite equity_cache"
    );
    assert!(
        engine.instances[0].last_equity_fetch.is_some(),
        "last_equity_fetch must still be updated to advance the cooldown",
    );

    // Phase 2: WS dump lands — connector returns equity=150. The gate
    // releases and the normal init path runs.
    *connector.balance_equity.lock().unwrap() = Some(Decimal::from(150));
    engine.fetch_equity_rest(0).await;

    assert_eq!(
        connector.balance_calls.load(Ordering::SeqCst),
        2,
        "second call also hits the connector"
    );
    assert!(
        engine.instances[0].equity_initialized,
        "post-init: equity_initialized must arm on first positive equity"
    );
    assert!(
        (engine.instances[0].equity_cache - 150.0).abs() < 1e-9,
        "equity_cache must hold the real balance"
    );

    // Phase 3: post-init, a 0 reading IS accepted — a rekt bot's
    // dashboard must reflect the loss rather than silently pin to
    // the last positive value.
    *connector.balance_equity.lock().unwrap() = Some(Decimal::from(0));
    engine.fetch_equity_rest(0).await;

    assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 3);
    assert!(
        (engine.instances[0].equity_cache - 0.0).abs() < 1e-9,
        "post-init: 0-valued reading IS accepted (bot may legitimately be at 0)"
    );
}

// bot-strategy#354: configured round_id != persisted round_id triggers a
// reset of round-bound per-instance fields at engine startup, while
// session-rolling fields (session_start_*, realized_pnl_today) survive.
#[test]
fn round_id_transition_zeros_round_bound_fields() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("risk_state.json");
    engine.risk_state_path = path.clone();
    engine.cfg.round_id = Some("round-4".to_string());

    let mut instances = HashMap::new();
    instances.insert(
        "default".to_string(),
        risk_io::InstanceRiskState {
            consecutive_losses: 5,
            circuit_breaker_until_ts: Some(9_999_999_999),
            session_start_equity: 1234.5,
            session_start_ts: 4242,
            realized_pnl_today: -12.0,
            equity_samples: vec![risk_io::EquitySample {
                ts: 10,
                equity: 100.0,
            }],
            session_halted: true,
            session_halt_reason: Some("session_dd_500bps".to_string()),
            session_halt_ts: Some(8888),
            total_trades: 42,
            total_wins: 24,
            total_pnl: 99.9,
            peak_pnl: 150.0,
            max_dd: 30.0,
            ..Default::default()
        },
    );
    risk_io::persist_risk_state(&path, Some("round-3"), &instances);

    engine.load_risk_state();

    let inst = &engine.instances[0];
    // Round-bound fields zeroed.
    assert_eq!(inst.consecutive_losses, 0);
    assert!(inst.circuit_breaker_until_ts.is_none());
    assert!(inst.equity_samples.is_empty());
    assert!(!inst.session_halted);
    assert!(inst.session_halt_reason.is_none());
    assert!(inst.session_halt_ts.is_none());
    assert_eq!(inst.total_trades, 0);
    assert_eq!(inst.total_wins, 0);
    assert_eq!(inst.total_pnl, 0.0);
    assert_eq!(inst.peak_pnl, 0.0);
    assert_eq!(inst.max_dd, 0.0);
    // Session-rolling fields survive — UTC midnight rolls them, not the
    // round boundary.
    assert!((inst.session_start_equity - 1234.5).abs() < 1e-9);
    assert_eq!(inst.session_start_ts, 4242);
    assert!((inst.realized_pnl_today - (-12.0)).abs() < 1e-9);
}

// bot-strategy#354: configured round_id == persisted round_id preserves
// all fields (the common in-round restart case).
#[test]
fn round_id_match_preserves_round_bound_fields() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("risk_state.json");
    engine.risk_state_path = path.clone();
    engine.cfg.round_id = Some("round-4".to_string());

    let mut instances = HashMap::new();
    instances.insert(
        "default".to_string(),
        risk_io::InstanceRiskState {
            consecutive_losses: 3,
            total_trades: 7,
            total_wins: 4,
            total_pnl: 11.5,
            peak_pnl: 20.0,
            max_dd: 8.5,
            ..Default::default()
        },
    );
    risk_io::persist_risk_state(&path, Some("round-4"), &instances);

    engine.load_risk_state();

    let inst = &engine.instances[0];
    assert_eq!(inst.consecutive_losses, 3);
    assert_eq!(inst.total_trades, 7);
    assert_eq!(inst.total_wins, 4);
    assert!((inst.total_pnl - 11.5).abs() < 1e-9);
    assert!((inst.peak_pnl - 20.0).abs() < 1e-9);
    assert!((inst.max_dd - 8.5).abs() < 1e-9);
}

// bot-strategy#469: on restart, the status reporter's `trade_stats`
// must be seeded from the persisted lifetime totals — not left at
// its `Some(zeros)` init value until the first post-restart trade.
// Frankfurt 2026-05-21 19:24 UTC restart surfaced this: A/B had 11
// lifetime trades each on disk, but dashboard showed 0/0 for ~10h
// until they next traded.
#[test]
fn status_reporter_seeded_from_persisted_totals_on_load() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("risk_state.json");
    engine.risk_state_path = path.clone();
    engine.cfg.round_id = Some("round-5".to_string());

    // Attach an in-memory StatusReporter to the default instance
    // (test_instance constructs one with status_reporter = None).
    engine.instances[0].status_reporter = Some(status::StatusReporter::for_test(
        dir.path().join("status.json"),
    ));

    let mut instances = HashMap::new();
    instances.insert(
        "default".to_string(),
        risk_io::InstanceRiskState {
            total_trades: 11,
            total_wins: 6,
            total_pnl: -1.6885,
            peak_pnl: 0.0,
            max_dd: 10.3857,
            ..Default::default()
        },
    );
    risk_io::persist_risk_state(&path, Some("round-5"), &instances);

    // Reporter pre-load: still at the zero-init value from for_test.
    let pre = engine.instances[0]
        .status_reporter
        .as_ref()
        .and_then(|r| r.trade_stats_for_test())
        .cloned()
        .expect("trade_stats initialised by for_test");
    assert_eq!(pre.trades, 0);
    assert_eq!(pre.pnl, 0.0);

    engine.load_risk_state();

    // In-memory totals applied …
    let inst = &engine.instances[0];
    assert_eq!(inst.total_trades, 11);
    assert_eq!(inst.total_wins, 6);
    // … and immediately surfaced on the status reporter, no need to
    // wait for a post-restart trade to call write_pnl_record.
    let stats = inst
        .status_reporter
        .as_ref()
        .and_then(|r| r.trade_stats_for_test())
        .expect("trade_stats reseeded by load_risk_state");
    assert_eq!(stats.trades, 11);
    assert_eq!(stats.wins, 6);
    assert!((stats.win_rate - (6.0 / 11.0 * 100.0)).abs() < 1e-9);
    assert!((stats.max_dd - 10.3857).abs() < 1e-9);
    assert!((stats.pnl - (-1.6885)).abs() < 1e-9);
}

// bot-strategy#354: unset configured round_id (legacy mode) never resets.
// Hosts without `round_id` in YAML (Tokyo Lighter / Extended /
// xvenue-arb at v2 launch) must not lose state on restart.
#[test]
fn round_id_unset_skips_auto_reset() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("risk_state.json");
    engine.risk_state_path = path.clone();
    engine.cfg.round_id = None;

    let mut instances = HashMap::new();
    instances.insert(
        "default".to_string(),
        risk_io::InstanceRiskState {
            total_trades: 9,
            ..Default::default()
        },
    );
    risk_io::persist_risk_state(&path, Some("round-3"), &instances);

    engine.load_risk_state();

    let inst = &engine.instances[0];
    assert_eq!(inst.total_trades, 9);
}

// ------------------------------------------------------------------
// bot-strategy#396: state-mutation coverage for engine cluster paths
// that previously had zero tests (reconcile / recovery / placement).
// ------------------------------------------------------------------

fn seed_state(engine: &mut PairTradeEngine, key: &str) {
    // Inserts an empty PairState for `key` on instance 0 + an empty
    // PairSharedState on the engine so the reconcile loop can find both.
    // Mirrors the production state-build path in `new_inner`.
    engine.instances[0]
        .states
        .insert(key.to_string(), state::PairState::new(2.0));
    engine
        .per_pair_state
        .entry(key.to_string())
        .or_insert_with(|| state::PairSharedState::new(8));
}

/// `reconcile_pending_orders` keys off the per-pair `states` map. A
/// missing key is a configuration / build-order bug, not a transient
/// runtime condition, and must surface loudly rather than silently
/// skip.
#[tokio::test]
async fn reconcile_pending_orders_errors_when_state_missing() {
    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    let price_map: HashMap<String, SymbolSnapshot> = HashMap::new();

    let result = engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await;

    assert!(result.is_err(), "missing state key must surface as error");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("AAA/BBB"),
        "error message must name the missing pair key: {msg}"
    );
}

/// With state present but no pending orders, reconcile is a no-op
/// (no connector calls, no state mutation). This is the steady-state
/// path on every tick when the bot is flat — must stay free of
/// side-effects, otherwise the per-tick connector RPC budget gets
/// burned for nothing.
#[tokio::test]
async fn reconcile_pending_orders_noop_when_no_pendings() {
    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    seed_state(&mut engine, "AAA/BBB");
    let price_map: HashMap<String, SymbolSnapshot> = HashMap::new();

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .expect("no pending = Ok");

    assert_eq!(
        connector.calls.lock().unwrap().len(),
        0,
        "no create_order calls"
    );
    assert_eq!(
        connector.cancel_all_calls.load(Ordering::SeqCst),
        0,
        "no cancel_all calls"
    );
    let state = engine.instances[0].states.get("AAA/BBB").unwrap();
    assert!(state.pending_entry.is_none());
    assert!(state.pending_exit.is_none());
    assert!(state.position.is_none());
}

/// Once entry preflight gates pass, the capital guard must be durable before
/// the first connector create_order call. The awaited venue request can be
/// accepted even if the process exits before its response reaches us.
#[tokio::test]
async fn place_pair_orders_persists_guard_before_first_submit() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    let dir = TempDir::new().unwrap();
    engine.risk_state_path = dir.path().join("risk_state.json");
    *connector.guard_path_expected_before_create.lock().unwrap() =
        Some(engine.risk_state_path.clone());

    let pair = super::config::PairSpec {
        base: "AAA".to_string(),
        quote: "BBB".to_string(),
    };
    let price_map = HashMap::from([
        ("AAA".to_string(), snapshot_721("100.0")),
        ("BBB".to_string(), snapshot_721("50.0")),
    ]);

    let legs = engine
        .place_pair_orders(
            0,
            &pair,
            PositionDirection::LongSpread,
            (dec("0.010"), dec("0.020")),
            &price_map,
        )
        .await
        .expect("entry placement should succeed");

    assert_eq!(legs.len(), 2);
    assert!(engine.instances[0].capital_position_seen_since_baseline);
    let persisted = risk_io::load_risk_state(&engine.risk_state_path);
    assert!(persisted.instances["default"].capital_position_seen_since_baseline);
}

/// A pre-submit post-only pricing failure proves no venue side effect happened.
/// It must not latch the capital guard or delay a later genuine transfer.
#[tokio::test]
async fn place_pair_orders_does_not_latch_guard_before_a_real_submit() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    *connector.ticker_fail_after_calls.lock().unwrap() = Some(0);
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    engine.cfg.dex_name = "lighter".to_string();
    engine.cfg.fee_bps = 1.0;
    let dir = TempDir::new().unwrap();
    engine.risk_state_path = dir.path().join("risk_state.json");

    let pair = super::config::PairSpec {
        base: "AAA".to_string(),
        quote: "BBB".to_string(),
    };
    // Quantization falls back to the requested sizes, but both the fresh
    // ticker read and the decision-time price lookup fail before create_order.
    let price_map = HashMap::new();
    let err = engine
        .place_pair_orders(
            0,
            &pair,
            PositionDirection::LongSpread,
            (dec("0.010"), dec("0.020")),
            &price_map,
        )
        .await
        .expect_err("missing post-only pricing must fail before submit");

    assert!(format!("{err:#}").contains("Missing reference price"));
    assert!(connector.calls.lock().unwrap().is_empty());
    assert!(!engine.instances[0].capital_position_seen_since_baseline);
    assert!(!engine.risk_state_path.exists());
}

/// Codex P2 follow-up, bot-strategy#783: create_order latches the guard
/// before every single-shot attempt so a crash mid-call can't hide a real
/// fill, but a DexError::ServerResponse means the venue definitively sent
/// back a rejection -- no order was created. That latch must unwind so a
/// later, genuine collateral top-up correctly reanchors instead of being
/// absorbed as position-ambiguous.
#[tokio::test]
async fn place_pair_orders_unlatches_guard_after_a_definitive_no_order_rejection() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    connector
        .reject_orders_with_server_response
        .store(true, Ordering::SeqCst);
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    let dir = TempDir::new().unwrap();
    engine.risk_state_path = dir.path().join("risk_state.json");

    let pair = super::config::PairSpec {
        base: "AAA".to_string(),
        quote: "BBB".to_string(),
    };
    let price_map = HashMap::from([
        ("AAA".to_string(), snapshot_721("100.0")),
        ("BBB".to_string(), snapshot_721("50.0")),
    ]);

    let err = engine
        .place_pair_orders(
            0,
            &pair,
            PositionDirection::LongSpread,
            (dec("0.010"), dec("0.020")),
            &price_map,
        )
        .await
        .expect_err("a definitive venue rejection must surface as an error");

    assert!(format!("{err:#}").contains("insufficient balance"));
    assert!(
        !engine.instances[0].capital_position_seen_since_baseline,
        "guard must unwind after a definitive no-order-created rejection"
    );
    let persisted = risk_io::load_risk_state(&engine.risk_state_path);
    assert!(!persisted.instances["default"].capital_position_seen_since_baseline);
}

/// The unlatch above must never clear a guard some *earlier*, genuine fill
/// already latched this session -- only the exact attempt that set it may
/// undo it.
#[tokio::test]
async fn place_pair_orders_definitive_rejection_does_not_clear_a_pre_existing_guard() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    let dir = TempDir::new().unwrap();
    engine.risk_state_path = dir.path().join("risk_state.json");
    // Simulate an earlier, genuine fill in this session before this
    // attempt's own latch/unlatch cycle runs.
    engine.latch_capital_position_activity(0);
    assert!(engine.instances[0].capital_position_seen_since_baseline);

    connector
        .reject_orders_with_server_response
        .store(true, Ordering::SeqCst);
    let pair = super::config::PairSpec {
        base: "AAA".to_string(),
        quote: "BBB".to_string(),
    };
    let price_map = HashMap::from([
        ("AAA".to_string(), snapshot_721("100.0")),
        ("BBB".to_string(), snapshot_721("50.0")),
    ]);

    let _ = engine
        .place_pair_orders(
            0,
            &pair,
            PositionDirection::LongSpread,
            (dec("0.010"), dec("0.020")),
            &price_map,
        )
        .await
        .expect_err("a definitive venue rejection must surface as an error");

    assert!(
        engine.instances[0].capital_position_seen_since_baseline,
        "a pre-existing guard from an earlier fill must survive untouched"
    );
}

#[tokio::test]
async fn close_pair_orders_records_taker_mode_after_post_only_fallback() {
    let connector = Arc::new(DummyConnector::default());
    connector.reject_priced_orders.store(true, Ordering::SeqCst);
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dex_name = "lighter".to_string();
    engine.cfg.fee_bps = 1.0;
    engine.cfg.default_pair_params.exit_post_only_timeout_secs = 30;

    let pair = super::config::PairSpec {
        base: "AAA".to_string(),
        quote: "BBB".to_string(),
    };
    let price_map = HashMap::from([
        (
            "AAA".to_string(),
            SymbolSnapshot {
                price: dec("100.0"),
                funding_rate: Decimal::ZERO,
                bid_price: Some(dec("99.0")),
                ask_price: Some(dec("101.0")),
                bid_size: Decimal::ONE,
                ask_size: Decimal::ONE,
                min_order: Some(dec("0.001")),
                min_tick: Some(dec("0.001")),
                size_decimals: Some(3),
                exchange_ts: None,
            },
        ),
        (
            "BBB".to_string(),
            SymbolSnapshot {
                price: dec("50.0"),
                funding_rate: Decimal::ZERO,
                bid_price: Some(dec("49.0")),
                ask_price: Some(dec("51.0")),
                bid_size: Decimal::ONE,
                ask_size: Decimal::ONE,
                min_order: Some(dec("0.001")),
                min_tick: Some(dec("0.001")),
                size_decimals: Some(3),
                exchange_ts: None,
            },
        ),
    ]);

    let (legs, takeover_at) = engine
        .close_pair_orders(
            &pair,
            PositionDirection::LongSpread,
            (dec("0.010"), dec("0.020")),
            &price_map,
            false,
        )
        .await
        .expect("fallback taker close should succeed");

    assert_eq!(legs.len(), 2);
    assert!(legs.iter().all(|leg| !leg.post_only));
    assert!(legs.iter().all(|leg| leg.limit_price.is_none()));
    assert!(takeover_at.is_none());

    let calls = connector.calls.lock().unwrap();
    assert!(calls
        .iter()
        .any(|(symbol, _, _, price, _)| symbol == "AAA" && price.is_some()));
    assert!(calls
        .iter()
        .any(|(symbol, _, _, price, _)| symbol == "AAA" && price.is_none()));
    assert!(calls
        .iter()
        .any(|(symbol, _, _, price, _)| symbol == "BBB" && price.is_some()));
    assert!(calls
        .iter()
        .any(|(symbol, _, _, price, _)| symbol == "BBB" && price.is_none()));
}

#[tokio::test]
async fn close_pair_orders_records_refreshed_post_only_submit_metadata() {
    let connector = Arc::new(DummyConnector::default());
    *connector.ticker_price_to_return.lock().unwrap() = Some(dec("200.0"));
    *connector.order_book_to_return.lock().unwrap() = Some(OrderBookSnapshot {
        bids: vec![OrderBookLevel {
            price: dec("199.0"),
            size: Decimal::ONE,
        }],
        asks: vec![OrderBookLevel {
            price: dec("201.0"),
            size: Decimal::ONE,
        }],
        book_ts_ms: Some(123),
    });
    let mut engine = PairTradeEngine::test_instance(connector);
    engine.cfg.dex_name = "lighter".to_string();
    engine.cfg.fee_bps = 1.0;

    let pair = super::config::PairSpec {
        base: "AAA".to_string(),
        quote: "BBB".to_string(),
    };
    let price_map = HashMap::from([
        (
            "AAA".to_string(),
            SymbolSnapshot {
                price: dec("100.0"),
                funding_rate: Decimal::ZERO,
                bid_price: Some(dec("99.0")),
                ask_price: Some(dec("101.0")),
                bid_size: Decimal::ONE,
                ask_size: Decimal::ONE,
                min_order: Some(dec("0.001")),
                min_tick: None,
                size_decimals: Some(3),
                exchange_ts: None,
            },
        ),
        (
            "BBB".to_string(),
            SymbolSnapshot {
                price: dec("50.0"),
                funding_rate: Decimal::ZERO,
                bid_price: Some(dec("49.0")),
                ask_price: Some(dec("51.0")),
                bid_size: Decimal::ONE,
                ask_size: Decimal::ONE,
                min_order: Some(dec("0.001")),
                min_tick: None,
                size_decimals: Some(3),
                exchange_ts: None,
            },
        ),
    ]);

    let (legs, _) = engine
        .close_pair_orders(
            &pair,
            PositionDirection::LongSpread,
            (dec("0.010"), dec("0.020")),
            &price_map,
            false,
        )
        .await
        .expect("post-only close should succeed");

    let base_leg = legs.iter().find(|leg| leg.symbol == "AAA").unwrap();
    assert!(base_leg.post_only);
    assert_eq!(base_leg.reference_price, Some(dec("99.0")));
    assert_eq!(base_leg.submit_reference_price, Some(dec("199.0")));
    assert_eq!(base_leg.submit_mid, Some(dec("200.0")));
    assert_eq!(base_leg.submit_bid, Some(dec("199.0")));
    assert_eq!(base_leg.submit_ask, Some(dec("201.0")));

    let quote_leg = legs.iter().find(|leg| leg.symbol == "BBB").unwrap();
    assert!(quote_leg.post_only);
    assert_eq!(quote_leg.reference_price, Some(dec("51.0")));
    assert_eq!(quote_leg.submit_reference_price, Some(dec("201.0")));
    assert_eq!(quote_leg.submit_mid, Some(dec("200.0")));
    assert_eq!(quote_leg.submit_bid, Some(dec("199.0")));
    assert_eq!(quote_leg.submit_ask, Some(dec("201.0")));
}

/// When post-only attempts exhaust and the order falls back to taker, the
/// submit metadata must come from the *last refreshed* book snapshot the retry
/// loop saw, not the stale decision-time `price_map`. Codex review PR #159:
/// otherwise `slippage_bps_vs_submit` folds in pre-submit market movement.
#[tokio::test]
async fn taker_fallback_records_refreshed_submit_metadata() {
    let connector = Arc::new(DummyConnector::default());
    // Force every priced (post-only) order to fail → exhaust retries → taker.
    connector.reject_priced_orders.store(true, Ordering::SeqCst);
    // Refreshed book/ticker differs from the decision-time price_map below.
    *connector.ticker_price_to_return.lock().unwrap() = Some(dec("200.0"));
    *connector.order_book_to_return.lock().unwrap() = Some(OrderBookSnapshot {
        bids: vec![OrderBookLevel {
            price: dec("199.0"),
            size: Decimal::ONE,
        }],
        asks: vec![OrderBookLevel {
            price: dec("201.0"),
            size: Decimal::ONE,
        }],
        book_ts_ms: Some(123),
    });
    let mut engine = PairTradeEngine::test_instance(connector);
    engine.cfg.dex_name = "lighter".to_string();
    engine.cfg.fee_bps = 1.0;
    engine.cfg.default_pair_params.exit_post_only_timeout_secs = 30;

    let pair = super::config::PairSpec {
        base: "AAA".to_string(),
        quote: "BBB".to_string(),
    };
    let price_map = HashMap::from([
        (
            "AAA".to_string(),
            SymbolSnapshot {
                price: dec("100.0"),
                funding_rate: Decimal::ZERO,
                bid_price: Some(dec("99.0")),
                ask_price: Some(dec("101.0")),
                bid_size: Decimal::ONE,
                ask_size: Decimal::ONE,
                min_order: Some(dec("0.001")),
                min_tick: None,
                size_decimals: Some(3),
                exchange_ts: None,
            },
        ),
        (
            "BBB".to_string(),
            SymbolSnapshot {
                price: dec("50.0"),
                funding_rate: Decimal::ZERO,
                bid_price: Some(dec("49.0")),
                ask_price: Some(dec("51.0")),
                bid_size: Decimal::ONE,
                ask_size: Decimal::ONE,
                min_order: Some(dec("0.001")),
                min_tick: None,
                size_decimals: Some(3),
                exchange_ts: None,
            },
        ),
    ]);

    let (legs, _) = engine
        .close_pair_orders(
            &pair,
            PositionDirection::LongSpread,
            (dec("0.010"), dec("0.020")),
            &price_map,
            false,
        )
        .await
        .expect("fallback taker close should succeed");

    // Legs actually fell back to taker (unpriced).
    assert!(legs.iter().all(|leg| !leg.post_only));
    assert!(legs.iter().all(|leg| leg.limit_price.is_none()));

    // submit_* reflect the refreshed book (199/200/201), NOT the stale
    // price_map (which would give bid 99 / ask 101 for the base leg).
    let base_leg = legs.iter().find(|leg| leg.symbol == "AAA").unwrap();
    assert_eq!(base_leg.submit_reference_price, Some(dec("199.0")));
    assert_eq!(base_leg.submit_mid, Some(dec("200.0")));
    assert_eq!(base_leg.submit_bid, Some(dec("199.0")));
    assert_eq!(base_leg.submit_ask, Some(dec("201.0")));

    let quote_leg = legs.iter().find(|leg| leg.symbol == "BBB").unwrap();
    assert_eq!(quote_leg.submit_reference_price, Some(dec("201.0")));
    assert_eq!(quote_leg.submit_mid, Some(dec("200.0")));
    assert_eq!(quote_leg.submit_bid, Some(dec("199.0")));
    assert_eq!(quote_leg.submit_ask, Some(dec("201.0")));
}

/// Codex review PR #159 (placement.rs:767): when a post-only leg refreshes its
/// book on an early retry but the *final* attempt's refresh fails, the taker
/// fallback must price submit metadata against the snapshot the last attempt
/// actually saw — i.e. drop to the decision-time `price_map` — not reuse the
/// earlier refresh across the retry gap. Otherwise `slippage_bps_vs_submit`
/// folds in market movement from before the final submit.
#[tokio::test]
async fn taker_fallback_drops_stale_refresh_when_final_attempt_refresh_fails() {
    let connector = Arc::new(DummyConnector::default());
    // Every priced (post-only) order fails → all 3 exit attempts exhaust → taker.
    connector.reject_priced_orders.store(true, Ordering::SeqCst);
    // Only the very first ticker refresh succeeds (base leg, attempt 1). Every
    // later attempt's refresh fails, so the base leg's *last* attempt sees no
    // fresh snapshot and the quote leg never refreshes at all.
    *connector.ticker_fail_after_calls.lock().unwrap() = Some(1);
    // The (single) successful refresh would report a book far from the
    // decision-time price_map, so a stale-reuse bug is visible.
    *connector.ticker_price_to_return.lock().unwrap() = Some(dec("200.0"));
    *connector.order_book_to_return.lock().unwrap() = Some(OrderBookSnapshot {
        bids: vec![OrderBookLevel {
            price: dec("199.0"),
            size: Decimal::ONE,
        }],
        asks: vec![OrderBookLevel {
            price: dec("201.0"),
            size: Decimal::ONE,
        }],
        book_ts_ms: Some(123),
    });
    let mut engine = PairTradeEngine::test_instance(connector);
    engine.cfg.dex_name = "lighter".to_string();
    engine.cfg.fee_bps = 1.0;
    engine.cfg.default_pair_params.exit_post_only_timeout_secs = 30;

    let pair = super::config::PairSpec {
        base: "AAA".to_string(),
        quote: "BBB".to_string(),
    };
    let price_map = HashMap::from([
        (
            "AAA".to_string(),
            SymbolSnapshot {
                price: dec("100.0"),
                funding_rate: Decimal::ZERO,
                bid_price: Some(dec("99.0")),
                ask_price: Some(dec("101.0")),
                bid_size: Decimal::ONE,
                ask_size: Decimal::ONE,
                min_order: Some(dec("0.001")),
                min_tick: None,
                size_decimals: Some(3),
                exchange_ts: None,
            },
        ),
        (
            "BBB".to_string(),
            SymbolSnapshot {
                price: dec("50.0"),
                funding_rate: Decimal::ZERO,
                bid_price: Some(dec("49.0")),
                ask_price: Some(dec("51.0")),
                bid_size: Decimal::ONE,
                ask_size: Decimal::ONE,
                min_order: Some(dec("0.001")),
                min_tick: None,
                size_decimals: Some(3),
                exchange_ts: None,
            },
        ),
    ]);

    let (legs, _) = engine
        .close_pair_orders(
            &pair,
            PositionDirection::LongSpread,
            (dec("0.010"), dec("0.020")),
            &price_map,
            false,
        )
        .await
        .expect("fallback taker close should succeed");

    assert!(legs.iter().all(|leg| !leg.post_only));
    assert!(legs.iter().all(|leg| leg.limit_price.is_none()));

    // Base leg refreshed on attempt 1 but its final attempt's refresh failed,
    // so submit_* must come from the decision-time price_map (bid 99 / ask 101),
    // NOT the earlier refresh (which would give 199/201).
    let base_leg = legs.iter().find(|leg| leg.symbol == "AAA").unwrap();
    assert_eq!(base_leg.submit_bid, Some(dec("99.0")));
    assert_eq!(base_leg.submit_ask, Some(dec("101.0")));

    // Quote leg never refreshed → also the decision-time price_map.
    let quote_leg = legs.iter().find(|leg| leg.symbol == "BBB").unwrap();
    assert_eq!(quote_leg.submit_bid, Some(dec("49.0")));
    assert_eq!(quote_leg.submit_ask, Some(dec("51.0")));
}

/// `register_partial_leg_failure` is the bridge from the engine's
/// place-leg error path back into per-pair pending state. An entry
/// failure must land in `pending_entry` so the next reconcile tick
/// can clean up the orphaned leg-A.
#[test]
fn register_partial_leg_failure_writes_pending_entry() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    engine.cfg.dry_run = false;
    let dir = TempDir::new().unwrap();
    engine.risk_state_path = dir.path().join("risk_state.json");
    seed_state(&mut engine, "AAA/BBB");
    let placed_legs = vec![PendingLeg {
        symbol: "AAA".to_string(),
        order_id: "leg-a".to_string(),
        exchange_order_id: None,
        target: dec("0.05"),
        filled: Decimal::ZERO,
        side: OrderSide::Long,
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
        reduce_only: false,
        post_only: false,
    }];
    let partial_err: anyhow::Error = state::PartialOrderPlacementError::new(
        placed_legs.clone(),
        DexError::Transient("leg B failed".to_string()),
    )
    .into();

    engine.register_partial_leg_failure(
        0,
        "AAA/BBB",
        PositionDirection::LongSpread,
        0,
        &partial_err,
        false, // is_exit
    );

    let pending = engine.instances[0]
        .states
        .get("AAA/BBB")
        .unwrap()
        .pending_entry
        .as_ref()
        .expect("pending_entry must be populated");
    assert_eq!(pending.legs.len(), 1);
    assert_eq!(pending.legs[0].symbol, "AAA");
    assert_eq!(pending.legs[0].order_id, "leg-a");
    assert_eq!(pending.direction, PositionDirection::LongSpread);
    assert!(
        engine.instances[0].capital_position_seen_since_baseline,
        "a placed entry leg creates venue exposure and must latch the capital guard"
    );
    assert!(
        engine.instances[0].flat_since.is_none(),
        "partial entry exposure must clear the flat-settlement dwell"
    );

    let persisted = risk_io::load_risk_state(&engine.risk_state_path);
    let persisted_inst = persisted
        .instances
        .get("default")
        .expect("partial-entry guard transition must be persisted before returning");
    assert!(persisted_inst.capital_position_seen_since_baseline);
}

/// Same surface, exit side: must land in `pending_exit` so the next
/// tick re-attempts to close the orphan leg, not re-open a fresh
/// entry.
#[test]
fn register_partial_leg_failure_writes_pending_exit() {
    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    seed_state(&mut engine, "AAA/BBB");
    let placed_legs = vec![PendingLeg {
        symbol: "AAA".to_string(),
        order_id: "exit-leg-a".to_string(),
        exchange_order_id: None,
        target: dec("0.05"),
        filled: Decimal::ZERO,
        side: OrderSide::Short,
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
        reduce_only: false,
        post_only: false,
    }];
    let partial_err: anyhow::Error = state::PartialOrderPlacementError::new(
        placed_legs,
        DexError::Transient("leg B failed".to_string()),
    )
    .into();

    engine.register_partial_leg_failure(
        0,
        "AAA/BBB",
        PositionDirection::ShortSpread,
        0,
        &partial_err,
        true, // is_exit
    );

    let state_ref = engine.instances[0].states.get("AAA/BBB").unwrap();
    assert!(
        state_ref.pending_entry.is_none(),
        "exit must not touch pending_entry"
    );
    let pending = state_ref
        .pending_exit
        .as_ref()
        .expect("pending_exit must be populated");
    assert_eq!(pending.direction, PositionDirection::ShortSpread);
    assert_eq!(pending.legs[0].order_id, "exit-leg-a");
}

/// Errors that aren't `PartialOrderPlacementError` carry no leg list
/// to recover (e.g. a pre-flight reference-price miss). The function
/// silently no-ops — *not* writing a synthetic empty `PendingOrders`
/// that the reconcile loop would then try to cancel.
#[test]
fn register_partial_leg_failure_ignores_non_partial_errors() {
    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    seed_state(&mut engine, "AAA/BBB");
    let plain_err: anyhow::Error = anyhow::anyhow!("missing reference price");

    engine.register_partial_leg_failure(
        0,
        "AAA/BBB",
        PositionDirection::LongSpread,
        0,
        &plain_err,
        false,
    );

    let state_ref = engine.instances[0].states.get("AAA/BBB").unwrap();
    assert!(state_ref.pending_entry.is_none());
    assert!(state_ref.pending_exit.is_none());
}

/// Unknown state key: write must be silently skipped (the function
/// uses `if let Some(state) = ... get_mut(key)`). Better to no-op
/// than panic — a stale pair key in flight should not crash a live
/// bot. Verifies no other instance's state is mutated.
#[test]
fn register_partial_leg_failure_silently_skips_unknown_pair() {
    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    seed_state(&mut engine, "AAA/BBB");
    let partial_err: anyhow::Error = state::PartialOrderPlacementError::new(
        vec![],
        DexError::Transient("placement failed".to_string()),
    )
    .into();

    engine.register_partial_leg_failure(
        0,
        "CCC/DDD",
        PositionDirection::LongSpread,
        0,
        &partial_err,
        false,
    );

    // Sibling key untouched.
    let other = engine.instances[0].states.get("AAA/BBB").unwrap();
    assert!(other.pending_entry.is_none());
    assert!(other.pending_exit.is_none());
}

/// `force_close_on_startup` is a no-op when `dry_run=true`
/// (matches DRY_RUN windows used during live-readiness validation).
/// The connector must not be touched — getting positions /
/// canceling / closing during DRY_RUN burns rate limit and pollutes
/// the live bot's order book if the same wallet is shared.
#[tokio::test]
async fn force_close_on_startup_dry_run_skips_connector_calls() {
    let connector = Arc::new(DummyConnector::default());
    let engine = PairTradeEngine::test_instance(connector.clone());
    // test_instance defaults dry_run=true.

    engine.force_close_on_startup().await.unwrap();

    assert_eq!(
        connector.positions_calls.load(Ordering::SeqCst),
        0,
        "dry_run must not query positions"
    );
    assert_eq!(
        connector.cancel_all_calls.load(Ordering::SeqCst),
        0,
        "dry_run must not issue cancel_all_orders"
    );
    assert_eq!(
        connector.close_all_calls.load(Ordering::SeqCst),
        0,
        "dry_run must not issue close_all_positions"
    );
}

/// bot-strategy#487: a position below the venue min order size (0.00001
/// BTC vs Extended's 0.0001 min) can never be submitted to
/// `close_all_positions` — the connector rejects sub-min sizes — so the
/// startup force-close must treat it as already flat: no close attempt,
/// no "still open" ERROR/email that re-fires error-watch on every restart.
#[tokio::test]
async fn force_close_on_startup_skips_sub_min_dust() {
    let connector = Arc::new(DummyConnector::default());
    *connector.positions_to_return.lock().unwrap() = vec![PositionSnapshot {
        symbol: "BTC".to_string(),
        size: dec("0.00001"),
        sign: -1,
        entry_price: Some(dec("61674")),
    }];
    *connector.min_order_to_return.lock().unwrap() = Some(dec("0.0001"));

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    engine.cfg.startup_force_close_wait_secs = 0;
    engine.cfg.startup_force_close_attempts = 1;

    engine.force_close_on_startup().await.unwrap();

    assert_eq!(
        connector.close_all_calls.load(Ordering::SeqCst),
        0,
        "sub-min dust must never be submitted to close_all_positions"
    );
}

/// Complement to the dust test: a position at or above the venue min is
/// genuinely force-closed, so the dust filter must not over-skip.
#[tokio::test]
async fn force_close_on_startup_closes_above_min_position() {
    let connector = Arc::new(DummyConnector::default());
    *connector.positions_to_return.lock().unwrap() = vec![PositionSnapshot {
        symbol: "BTC".to_string(),
        size: dec("0.05"),
        sign: 1,
        entry_price: Some(dec("70000")),
    }];
    *connector.min_order_to_return.lock().unwrap() = Some(dec("0.0001"));

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    engine.cfg.startup_force_close_wait_secs = 0;
    engine.cfg.startup_force_close_attempts = 1;

    engine.force_close_on_startup().await.unwrap();

    assert!(
        connector.close_all_calls.load(Ordering::SeqCst) >= 1,
        "an above-min position must be submitted to close_all_positions"
    );
}

/// bot-strategy#487 (review): the load-bearing mixed case. A dust leg
/// (sorted first) alongside a genuine above-min leg. The connector
/// rejects any close that includes the sub-min size, so a
/// `close_all_positions(None)` would abort on the dust and leave the
/// real leg open — startup would continue with live exposure. The fix
/// closes per-symbol, so the real leg is flattened and only the dust
/// remains (treated as flat, no ERROR/email).
#[tokio::test]
async fn force_close_on_startup_closes_real_leg_despite_dust() {
    let connector = Arc::new(DummyConnector::default());
    *connector.positions_to_return.lock().unwrap() = vec![
        PositionSnapshot {
            symbol: "BTC".to_string(),
            size: dec("0.00001"),
            sign: -1,
            entry_price: Some(dec("61674")),
        },
        PositionSnapshot {
            symbol: "ETH".to_string(),
            size: dec("1.5"),
            sign: 1,
            entry_price: Some(dec("3500")),
        },
    ];
    *connector.min_order_to_return.lock().unwrap() = Some(dec("0.0001"));

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    engine.cfg.startup_force_close_wait_secs = 0;
    engine.cfg.startup_force_close_attempts = 1;

    engine.force_close_on_startup().await.unwrap();

    // The genuine ETH leg must be flattened; only the dust BTC leg may
    // remain. With the old close_all_positions(None) path the connector
    // would reject on the dust and ETH would still be open here.
    let remaining = connector.positions_to_return.lock().unwrap().clone();
    assert_eq!(remaining.len(), 1, "only the dust leg should remain open");
    assert_eq!(
        remaining[0].symbol, "BTC",
        "the dust leg (not the real leg) is what remains"
    );
}

/// Normal exits must carry `close_reason` in the pnl record so
/// attribution tooling can recover close reasons beyond journal
/// retention (bot-strategy#514 / #531).
#[test]
fn exit_record_serializes_close_reason_without_recovery_fields() {
    let record = PnlLogRecord::new(
        "BTC",
        "ETH",
        PositionDirection::LongSpread,
        1.5,
        1_700_000_000,
        "exit_fill",
    )
    .with_close_reason("ineligible");
    let json: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&record).unwrap()).unwrap();
    assert_eq!(json["source"], "exit_fill");
    assert_eq!(json["close_reason"], "ineligible");
    assert!(json.get("recovery_reason").is_none());
    assert!(json.get("pnl_available").is_none());
}

#[test]
fn recovery_no_pnl_record_logs_context_without_trade_stats() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    let dir = TempDir::new().unwrap();
    engine.instances[0].pnl_logger = Some(PnlLogger::for_test(dir.path().to_path_buf()));
    engine.instances[0].total_trades = 7;
    engine.instances[0].total_pnl = 12.5;

    let key = "AAA/BBB";
    let now_ts = 1_700_000_300;
    let mut state = PairState::new(2.0);
    state.pending_exit_reason = Some("force_close");
    state.position = Some(super::state::Position {
        direction: PositionDirection::LongSpread,
        entered_at: Instant::now(),
        entered_ts: now_ts - 300,
        entry_price_a: Some(dec("100")),
        entry_price_b: Some(dec("50")),
        entry_size_a: Some(dec("0.01")),
        entry_size_b: Some(dec("0.02")),
        entry_z: Some(2.4),
        entry_beta: Some(1.2),
        last_rehedge_ts: None,
        rehedge_realized_pnl: None,
        prev_beta_for_velocity: None,
    });
    engine.instances[0].states.insert(key.to_string(), state);

    let price_map = HashMap::from([
        (
            "AAA".to_string(),
            SymbolSnapshot {
                price: dec("101"),
                funding_rate: Decimal::ZERO,
                bid_price: None,
                ask_price: None,
                bid_size: Decimal::ZERO,
                ask_size: Decimal::ZERO,
                min_order: None,
                min_tick: None,
                size_decimals: None,
                exchange_ts: None,
            },
        ),
        (
            "BBB".to_string(),
            SymbolSnapshot {
                price: dec("49"),
                funding_rate: Decimal::ZERO,
                bid_price: None,
                ask_price: None,
                bid_size: Decimal::ZERO,
                ask_size: Decimal::ZERO,
                min_order: None,
                min_tick: None,
                size_decimals: None,
                exchange_ts: None,
            },
        ),
    ]);

    engine.write_recovery_no_pnl_record(
        0,
        key,
        PositionDirection::ShortSpread,
        "timeout",
        now_ts,
        &price_map,
    );

    assert_eq!(engine.instances[0].total_trades, 7);
    assert!((engine.instances[0].total_pnl - 12.5).abs() < 1e-9);

    let path = std::fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let line = std::fs::read_to_string(path).unwrap();
    let json: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(json["source"], "recovery_no_pnl");
    assert_eq!(json["pnl_available"], false);
    assert_eq!(json["close_reason"], "force_close");
    assert_eq!(json["recovery_reason"], "timeout");
    assert_eq!(json["direction"], "long_spread");
    assert_eq!(json["pnl"], 0.0);
    assert_eq!(json["z_entry"], 2.4);
    assert_eq!(json["beta"], 1.2);
    assert_eq!(json["hold_secs"], 300.0);
    assert_eq!(json["exit_price_a"], 101.0);
    assert_eq!(json["exit_price_b"], 49.0);
}

/// `force_close_all_positions` (reconcile-loop emergency path) is
/// also gated by dry_run / observe_only. The reconcile loop calls
/// it after exit retries exhaust; on DRY_RUN we must not pretend
/// to flatten on the exchange.
#[tokio::test]
async fn force_close_all_positions_dry_run_skips_connector_calls() {
    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector.clone());

    let close_confirmed = engine.force_close_all_positions("AAA/BBB", "timeout").await;

    assert!(
        !close_confirmed,
        "dry_run skip must not report a confirmed close"
    );
    assert_eq!(
        connector.positions_calls.load(Ordering::SeqCst),
        0,
        "dry_run must not query positions"
    );
    assert_eq!(
        connector.close_all_calls.load(Ordering::SeqCst),
        0,
        "dry_run must not invoke close_all_positions"
    );
}

/// bot-strategy#514 review fix: the emergency close outcome decides
/// whether the later exchange-snapshot recovery record is suppressed.
/// A failed close must NOT report confirmation, an already-flat or
/// successfully submitted close must.
#[tokio::test]
async fn force_close_all_positions_reports_outcome() {
    // Failure: a dust position makes close_all_positions(None) error.
    let connector = Arc::new(DummyConnector::default());
    *connector.positions_to_return.lock().unwrap() = vec![PositionSnapshot {
        symbol: "AAA".to_string(),
        size: dec("0.00001"),
        sign: 1,
        entry_price: Some(dec("100")),
    }];
    *connector.min_order_to_return.lock().unwrap() = Some(dec("0.0001"));
    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    assert!(
        !engine.force_close_all_positions("AAA/BBB", "timeout").await,
        "failed close must not be confirmed"
    );

    // Success: above-min position clears.
    *connector.min_order_to_return.lock().unwrap() = None;
    *connector.positions_to_return.lock().unwrap() = vec![PositionSnapshot {
        symbol: "AAA".to_string(),
        size: dec("0.05"),
        sign: 1,
        entry_price: Some(dec("100")),
    }];
    assert!(
        engine.force_close_all_positions("AAA/BBB", "timeout").await,
        "submitted close must be confirmed"
    );

    // Already flat: confirmed without invoking close_all_positions again.
    let close_calls_before = connector.close_all_calls.load(Ordering::SeqCst);
    assert!(
        engine.force_close_all_positions("AAA/BBB", "timeout").await,
        "already-flat must be confirmed"
    );
    assert_eq!(
        connector.close_all_calls.load(Ordering::SeqCst),
        close_calls_before,
        "already-flat must not invoke close_all_positions"
    );
}

/// bot-strategy#514 helpers: a PairState holding an open LongSpread
/// position with full entry context, as the recovery-record paths see it.
fn seeded_position_state() -> PairState {
    let mut state = PairState::new(2.0);
    state.position = Some(super::state::Position {
        direction: PositionDirection::LongSpread,
        entered_at: Instant::now(),
        entered_ts: 1_700_000_000,
        entry_price_a: Some(dec("100")),
        entry_price_b: Some(dec("50")),
        entry_size_a: Some(dec("0.01")),
        entry_size_b: Some(dec("0.02")),
        entry_z: Some(2.4),
        entry_beta: Some(1.2),
        last_rehedge_ts: None,
        rehedge_realized_pnl: None,
        prev_beta_for_velocity: None,
    });
    state
}

fn read_single_pnl_record(dir: &std::path::Path) -> serde_json::Value {
    let path = std::fs::read_dir(dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let content = std::fs::read_to_string(path).unwrap();
    let mut lines = content.lines();
    let json: serde_json::Value = serde_json::from_str(lines.next().unwrap().trim()).unwrap();
    assert!(lines.next().is_none(), "expected exactly one pnl record");
    json
}

#[tokio::test]
async fn snapshot_clear_writes_recovery_record_and_clears_position() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    engine.cfg.dry_run = false;
    let dir = TempDir::new().unwrap();
    engine.instances[0].pnl_logger = Some(PnlLogger::for_test(dir.path().to_path_buf()));
    let mut state = seeded_position_state();
    state.pending_exit_reason = Some("ineligible");
    engine.instances[0]
        .states
        .insert("AAA/BBB".to_string(), state);

    let prices: HashMap<String, SymbolSnapshot> = HashMap::new();
    engine
        .sync_positions_from_exchange(0, &prices)
        .await
        .unwrap();

    let json = read_single_pnl_record(dir.path());
    assert_eq!(json["source"], "recovery_no_pnl");
    assert_eq!(json["pnl_available"], false);
    assert_eq!(json["recovery_reason"], "exchange_snapshot_clear");
    assert_eq!(json["close_reason"], "ineligible");
    assert_eq!(json["direction"], "long_spread");
    assert_eq!(json["z_entry"], 2.4);

    let state = engine.instances[0].states.get("AAA/BBB").unwrap();
    assert!(state.position.is_none());
    assert!(!state.position_guard);
    assert!(!state.recovery_recorded);
}

#[tokio::test]
async fn snapshot_clear_skips_duplicate_after_recovery_record() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    engine.cfg.dry_run = false;
    let dir = TempDir::new().unwrap();
    engine.instances[0].pnl_logger = Some(PnlLogger::for_test(dir.path().to_path_buf()));
    let mut state = seeded_position_state();
    // Reconcile's partial-fill / timeout recovery already wrote the
    // context record for this close.
    state.recovery_recorded = true;
    engine.instances[0]
        .states
        .insert("AAA/BBB".to_string(), state);

    let prices: HashMap<String, SymbolSnapshot> = HashMap::new();
    engine
        .sync_positions_from_exchange(0, &prices)
        .await
        .unwrap();

    assert!(
        std::fs::read_dir(dir.path()).unwrap().next().is_none(),
        "no duplicate record expected"
    );
    let state = engine.instances[0].states.get("AAA/BBB").unwrap();
    assert!(state.position.is_none());
    assert!(!state.recovery_recorded, "flag must reset on clear");
}

#[tokio::test]
async fn snapshot_clear_tags_external_flatten_reason() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    engine.cfg.dry_run = false;
    let dir = TempDir::new().unwrap();
    engine.instances[0].pnl_logger = Some(PnlLogger::for_test(dir.path().to_path_buf()));
    engine.instances[0]
        .states
        .insert("AAA/BBB".to_string(), seeded_position_state());
    engine.instances[0].external_flatten_reason = Some("session_dd_50bps_lev5.0".to_string());

    let prices: HashMap<String, SymbolSnapshot> = HashMap::new();
    engine
        .sync_positions_from_exchange(0, &prices)
        .await
        .unwrap();

    let json = read_single_pnl_record(dir.path());
    assert_eq!(json["recovery_reason"], "session_dd_50bps_lev5.0");
    assert!(
        engine.instances[0].external_flatten_reason.is_none(),
        "one-shot marker must be consumed"
    );
}

#[test]
fn stale_pending_clear_writes_recovery_record() {
    use tempfile::TempDir;

    let connector = Arc::new(DummyConnector::default());
    let mut engine = PairTradeEngine::test_instance(connector);
    let dir = TempDir::new().unwrap();
    engine.instances[0].pnl_logger = Some(PnlLogger::for_test(dir.path().to_path_buf()));
    let mut state = seeded_position_state();
    state.pending_exit = Some(PendingOrders {
        legs: Vec::new(),
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    engine.instances[0]
        .states
        .insert("AAA/BBB".to_string(), state);

    engine.clear_stale_pending(0, std::time::Duration::from_secs(0), "ws_not_ready");

    let json = read_single_pnl_record(dir.path());
    assert_eq!(json["source"], "recovery_no_pnl");
    assert_eq!(json["recovery_reason"], "stale_pending_ws_not_ready");

    let state = engine.instances[0].states.get("AAA/BBB").unwrap();
    assert!(state.position.is_none());
    assert!(state.pending_exit.is_none());
}

// === bot-strategy#721: late-fill overfill during amend → MARKET fallback ===

/// Entry-leg builder for the #721 reconciliation tests.
fn entry_leg_721(symbol: &str, order_id: &str, target: &str, side: OrderSide) -> PendingLeg {
    PendingLeg {
        symbol: symbol.to_string(),
        order_id: order_id.to_string(),
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
        reduce_only: false,
        post_only: false,
    }
}

fn snapshot_721(price: &str) -> SymbolSnapshot {
    SymbolSnapshot {
        price: dec(price),
        funding_rate: Decimal::ZERO,
        bid_price: None,
        ask_price: None,
        bid_size: Decimal::ZERO,
        ask_size: Decimal::ZERO,
        min_order: Some(dec("0.0001")),
        min_tick: Some(dec("0.001")),
        size_decimals: Some(4),
        exchange_ts: None,
    }
}

fn position_721(symbol: &str, size: &str, sign: i32) -> PositionSnapshot {
    PositionSnapshot {
        symbol: symbol.to_string(),
        size: dec(size),
        sign,
        entry_price: None,
    }
}

/// Deterministic regression for the 2026-07-08 09:42:30 UTC entry overfill
/// (bot-strategy#721), prevention layer: a late maker fill lands after the
/// initial fill snapshot but is visible once the cancel is acknowledged.
/// The MARKET remainder must be recomputed from the post-cancel refresh —
/// 2.4291 − 1.1158 = 1.3133 — not from the stale snapshot (1.4858, which
/// produced the live +0.1725 overfill). The venue position endpoint is
/// kept stale so the #470 cross-check alone cannot save the day.
#[tokio::test]
async fn market_takeover_recomputes_remaining_after_cancel_ack() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![
            // initial pending_status snapshot: pre-late-fill
            vec![("leg1".to_string(), dec("0.9433"))],
            // post-cancel-ack refresh: the late 0.1725 fill is now visible
            vec![("leg1".to_string(), dec("1.1158"))],
        ]),
    );
    connector.open_ids_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![
            vec!["leg1".to_string()], // initial pending_status open scan
            vec!["leg1".to_string()], // cancel-ack poll 1: still open
            vec![],                   // cancel-ack poll 2 → acknowledged
        ]),
    );
    // /positions is stale: it has not seen the late fill.
    connector
        .positions_to_return
        .lock()
        .unwrap()
        .push(position_721("BBB", "0.9433", -1));

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.entry_partial_fill_max_retries = 1;
    engine.cfg.entry_partial_fill_giveup_retries = 0;
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("BBB", "leg1", "2.4291", OrderSide::Short)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        // retry index 2 > max_retries 1 → MARKET takeover branch
        hedge_retry_count: 1,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    let calls = connector.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "exactly one MARKET reissue expected");
    let (symbol, size, side, price, reduce_only) = calls[0].clone();
    assert_eq!(symbol, "BBB");
    assert_eq!(
        size,
        dec("1.3133"),
        "MARKET remainder must come from the post-cancel refresh, not the stale snapshot (1.4858)"
    );
    assert_eq!(side, OrderSide::Short);
    assert_eq!(price, None, "takeover order must be MARKET");
    assert!(!reduce_only);
}

/// Defense layer, short-leg direction (the live 2026-07-08 shape): the
/// entry completes but the venue holds 2.6016 ETH short against an
/// intended 2.4291. The reconciliation must trim exactly the 0.1725
/// excess with a reduce-only buy and end with the venue back at
/// target ± one size tick, without blocking future entries.
#[tokio::test]
async fn entry_overfill_short_leg_trimmed_reduce_only() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "AAA".to_string(),
        VecDeque::from(vec![vec![("legA".to_string(), dec("0.04"))]]),
    );
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![vec![("legB".to_string(), dec("2.4291"))]]),
    );
    let overfilled = vec![
        position_721("AAA", "0.04", 1),
        position_721("BBB", "2.6016", -1),
    ];
    let trimmed = vec![
        position_721("AAA", "0.04", 1),
        position_721("BBB", "2.4291", -1),
    ];
    *connector.positions_script.lock().unwrap() = VecDeque::from(vec![
        overfilled.clone(), // AAA read 1 (clean leg → full window)
        overfilled.clone(), // AAA read 2
        overfilled.clone(), // AAA read 3 → settles clean
        overfilled.clone(), // BBB read 1
        overfilled,         // BBB read 2 → stable excess settles early
        trimmed,            // post-trim verification → within tolerance
    ]);

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![
            entry_leg_721("AAA", "legA", "0.04", OrderSide::Long),
            entry_leg_721("BBB", "legB", "2.4291", OrderSide::Short),
        ],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("AAA".to_string(), snapshot_721("100000.0"));
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    let calls = connector.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "exactly one trim order expected");
    let (symbol, size, side, price, reduce_only) = calls[0].clone();
    assert_eq!(symbol, "BBB");
    assert_eq!(size, dec("0.1725"), "trim must be exactly the excess");
    assert_eq!(side, OrderSide::Long, "short excess is bought back");
    assert_eq!(price, None, "trim is a MARKET order");
    assert!(reduce_only, "trim must be reduce-only");
    let state = engine.instances[0].states.get("AAA/BBB").unwrap();
    assert!(state.position.is_some(), "intended pair position retained");
    assert_eq!(
        state.position.as_ref().unwrap().entry_size_b,
        Some(dec("2.4291")),
        "model position keeps the intended target, not the overfilled qty"
    );
    assert!(
        engine.instances[0].entry_blocked_pairs.is_empty(),
        "successful trim must not fail-close entries"
    );
}

/// Defense layer, long-leg direction: excess on a long leg is trimmed
/// with a reduce-only sell.
#[tokio::test]
async fn entry_overfill_long_leg_trimmed_reduce_only() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "AAA".to_string(),
        VecDeque::from(vec![vec![("legA".to_string(), dec("1.0"))]]),
    );
    let overfilled = vec![position_721("AAA", "1.2", 1)];
    let trimmed = vec![position_721("AAA", "1.0", 1)];
    *connector.positions_script.lock().unwrap() =
        VecDeque::from(vec![overfilled.clone(), overfilled, trimmed]);

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("AAA", "legA", "1.0", OrderSide::Long)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("AAA".to_string(), snapshot_721("100000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    let calls = connector.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let (symbol, size, side, price, reduce_only) = calls[0].clone();
    assert_eq!(symbol, "AAA");
    assert_eq!(size, dec("0.2"));
    assert_eq!(side, OrderSide::Short, "long excess is sold back");
    assert_eq!(price, None);
    assert!(reduce_only);
    assert!(engine.instances[0].entry_blocked_pairs.is_empty());
}

/// Underfill is never trimmed: the venue holding LESS than the intended
/// target must produce no order (a reduce-only "trim" of a deficit would
/// make the position smaller still) and must not fail-close entries.
#[tokio::test]
async fn entry_underfill_is_never_trimmed() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![vec![("legB".to_string(), dec("2.4291"))]]),
    );
    *connector.positions_script.lock().unwrap() =
        VecDeque::from(vec![vec![position_721("BBB", "2.3", -1)]]);

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("BBB", "legB", "2.4291", OrderSide::Short)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    assert_eq!(
        connector.calls.lock().unwrap().len(),
        0,
        "underfill must never be trimmed"
    );
    assert!(
        engine.instances[0].entry_blocked_pairs.is_empty(),
        "underfill reports but does not fail-close entries"
    );
}

/// A failed excess trim fails closed: new entries for the pair are
/// blocked (persisted via entry_blocked_pairs), while the position and
/// normal exit management stay intact.
#[tokio::test]
async fn entry_trim_failure_blocks_new_entries() {
    let connector = Arc::new(DummyConnector::default());
    connector
        .reject_reduce_only_orders
        .store(true, Ordering::SeqCst);
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![vec![("legB".to_string(), dec("2.4291"))]]),
    );
    *connector.positions_script.lock().unwrap() =
        VecDeque::from(vec![vec![position_721("BBB", "2.6016", -1)]]);

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    engine.risk_state_path = std::env::temp_dir().join(format!(
        "pairtrade-721-trim-fail-risk-{}.json",
        std::process::id()
    ));
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("BBB", "legB", "2.4291", OrderSide::Short)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    let blocked = &engine.instances[0].entry_blocked_pairs;
    let reason = blocked
        .get("AAA/BBB")
        .expect("trim failure must fail-close entries for the pair");
    assert!(
        reason.contains("trim_failed"),
        "block reason must name the trim failure: {reason}"
    );
    let state = engine.instances[0].states.get("AAA/BBB").unwrap();
    assert!(
        state.position.is_some(),
        "position (and thus exit management) must survive the block"
    );
    // The block persisted to the risk-state file so a restart cannot
    // silently re-arm entries.
    let persisted = risk_io::load_risk_state(&engine.risk_state_path);
    let inst = persisted
        .instances
        .get("default")
        .expect("risk state persisted");
    assert!(inst.entry_blocked_pairs.contains_key("AAA/BBB"));
    let _ = std::fs::remove_file(&engine.risk_state_path);
}

/// A position fetch failure during reconciliation also fails closed —
/// "could not verify" must never be treated as "verified clean".
#[tokio::test]
async fn entry_reconcile_fetch_failure_blocks_new_entries() {
    let connector = Arc::new(DummyConnector::default());
    connector
        .positions_should_fail
        .store(true, Ordering::SeqCst);
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![vec![("legB".to_string(), dec("2.4291"))]]),
    );

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    engine.risk_state_path = std::env::temp_dir().join(format!(
        "pairtrade-721-fetch-fail-risk-{}.json",
        std::process::id()
    ));
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("BBB", "legB", "2.4291", OrderSide::Short)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    assert!(engine.instances[0]
        .entry_blocked_pairs
        .get("AAA/BBB")
        .is_some_and(|r| r.contains("fetch_failed")));
    assert_eq!(
        connector.calls.lock().unwrap().len(),
        0,
        "no trim can be attempted when the position is unknown"
    );
    let _ = std::fs::remove_file(&engine.risk_state_path);
}

/// dry_run keeps the legacy behaviour: entry completion never touches
/// get_positions, places no trim, and blocks nothing. Pins the
/// "existing paths unchanged" acceptance for BT / dry-run deployments.
#[tokio::test]
async fn entry_reconcile_skipped_in_dry_run() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![vec![("legB".to_string(), dec("2.4291"))]]),
    );

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("BBB", "legB", "2.4291", OrderSide::Short)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    assert_eq!(connector.positions_calls.load(Ordering::SeqCst), 0);
    assert_eq!(connector.calls.lock().unwrap().len(), 0);
    assert!(engine.instances[0].entry_blocked_pairs.is_empty());
}

/// Codex review PR #168 (P1): an excess between one size tick and the
/// venue min lot must NOT be trimmed — `quantize_order_size` rounds such
/// sizes UP to `min_order`, so the trim would exceed the confirmed excess
/// and leave the leg under the intended hedge. The dust is surfaced but
/// left in place, and entries stay open.
#[tokio::test]
async fn entry_overfill_below_min_lot_is_not_trimmed() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "AAA".to_string(),
        VecDeque::from(vec![vec![("legA".to_string(), dec("1.0"))]]),
    );
    // Excess 0.005: above the 0.0001 size tick, below the 0.01 min lot.
    *connector.positions_script.lock().unwrap() =
        VecDeque::from(vec![vec![position_721("AAA", "1.005", 1)]]);

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("AAA", "legA", "1.0", OrderSide::Long)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut snapshot = snapshot_721("100000.0");
    snapshot.min_order = Some(dec("0.01"));
    let mut price_map = HashMap::new();
    price_map.insert("AAA".to_string(), snapshot);

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    assert_eq!(
        connector.calls.lock().unwrap().len(),
        0,
        "a sub-min-lot excess must not be rounded up into an oversized trim"
    );
    assert!(
        engine.instances[0].entry_blocked_pairs.is_empty(),
        "dust-level excess is surfaced but does not fail-close entries"
    );
}

/// Codex review PR #168 (P2): a sign flip observed by the POST-trim
/// verification must fail closed exactly like a pre-trim sign flip — a
/// reduce-only trim can never invert the position, so an opposite-sided
/// verify readout means the venue/state cannot be trusted, not that the
/// excess was successfully trimmed.
#[tokio::test]
async fn post_trim_sign_flip_fails_closed() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![vec![("legB".to_string(), dec("2.4291"))]]),
    );
    let overfilled = vec![position_721("BBB", "2.6016", -1)];
    let flipped = vec![position_721("BBB", "0.5", 1)];
    *connector.positions_script.lock().unwrap() =
        VecDeque::from(vec![overfilled.clone(), overfilled, flipped]);

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    engine.risk_state_path = std::env::temp_dir().join(format!(
        "pairtrade-721-post-trim-flip-risk-{}.json",
        std::process::id()
    ));
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("BBB", "legB", "2.4291", OrderSide::Short)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    // The trim itself was placed (reduce-only buy of the excess)...
    let calls = connector.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].4, "trim must be reduce-only");
    // ...but the flipped verify readout must fail closed, not record a
    // successful trim.
    assert!(engine.instances[0]
        .entry_blocked_pairs
        .get("AAA/BBB")
        .is_some_and(|r| r.contains("post_trim_sign_flip")));
    let _ = std::fs::remove_file(&engine.risk_state_path);
}

/// Codex review PR #168 (P2, follow-up): an Underfill readout from the
/// POST-trim verification is anomalous — `trim_qty <= excess` means our
/// own reduce-only trim can only settle at-or-above the intended target.
/// Dropping below it (external close mid-trim, or the venue filling more
/// than requested) leaves the model position overstating the venue, so
/// it must fail closed like the post-trim sign flip, not count as a
/// successful trim.
#[tokio::test]
async fn post_trim_underfill_fails_closed() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![vec![("legB".to_string(), dec("2.4291"))]]),
    );
    let overfilled = vec![position_721("BBB", "2.6016", -1)];
    // After the 0.1725 reduce-only trim the venue reports 2.3 short —
    // 0.1291 UNDER the intended 2.4291 target.
    let under = vec![position_721("BBB", "2.3", -1)];
    *connector.positions_script.lock().unwrap() =
        VecDeque::from(vec![overfilled.clone(), overfilled, under]);

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    engine.risk_state_path = std::env::temp_dir().join(format!(
        "pairtrade-721-post-trim-under-risk-{}.json",
        std::process::id()
    ));
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("BBB", "legB", "2.4291", OrderSide::Short)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    let calls = connector.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "the trim itself was placed");
    assert!(calls[0].4, "trim must be reduce-only");
    assert!(engine.instances[0]
        .entry_blocked_pairs
        .get("AAA/BBB")
        .is_some_and(|r| r.contains("post_trim_underfill")));
    let _ = std::fs::remove_file(&engine.risk_state_path);
}

/// Codex review PR #168 (P1, follow-up): the initial reconciliation read
/// must not trust a single position readout. Here the position endpoint
/// lags the fill endpoints — the first read still shows the intended size
/// (looks `ok`), and only the second read reveals the late-fill excess.
/// The stability polling (two consecutive identical reads required) must
/// catch the overfill and trim it instead of recording a clean entry.
#[tokio::test]
async fn entry_reconcile_polls_lagging_position_endpoint() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![vec![("legB".to_string(), dec("2.4291"))]]),
    );
    let lagging = vec![position_721("BBB", "2.4291", -1)]; // pre-late-fill
    let overfilled = vec![position_721("BBB", "2.6016", -1)];
    let trimmed = vec![position_721("BBB", "2.4291", -1)];
    *connector.positions_script.lock().unwrap() = VecDeque::from(vec![
        lagging,            // read 1: endpoint not caught up — looks ok
        overfilled.clone(), // read 2: late fill visible — disagrees with read 1
        overfilled,         // read 3: stable → settled on the overfill
        trimmed,            // post-trim verification
    ]);

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("BBB", "legB", "2.4291", OrderSide::Short)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    let calls = connector.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "the lagged overfill must still be detected and trimmed"
    );
    let (symbol, size, side, price, reduce_only) = calls[0].clone();
    assert_eq!(symbol, "BBB");
    assert_eq!(size, dec("0.1725"));
    assert_eq!(side, OrderSide::Long);
    assert_eq!(price, None);
    assert!(reduce_only);
    assert!(engine.instances[0].entry_blocked_pairs.is_empty());
}

/// Codex review PR #168 (P1, follow-up ②): two identical CLEAN readings
/// must not settle early — a stale position cache returns the same
/// pre-late-fill snapshot repeatedly, and accepting it after one delay
/// would record the entry as ok and permanently skip the trim. Clean
/// reads consume the full polling window, so the excess that only
/// becomes visible on the final read is still caught and trimmed.
#[tokio::test]
async fn entry_reconcile_does_not_settle_on_repeated_stale_clean_reads() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![vec![("legB".to_string(), dec("2.4291"))]]),
    );
    let stale = vec![position_721("BBB", "2.4291", -1)]; // pre-late-fill snapshot
    let overfilled = vec![position_721("BBB", "2.6016", -1)];
    let trimmed = vec![position_721("BBB", "2.4291", -1)];
    *connector.positions_script.lock().unwrap() = VecDeque::from(vec![
        stale.clone(), // read 1: stale cache — looks ok
        stale,         // read 2: SAME stale snapshot — must not settle as ok
        overfilled,    // read 3: cache caught up — excess visible
        trimmed,       // post-trim verification
    ]);

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("BBB", "legB", "2.4291", OrderSide::Short)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    let calls = connector.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "the excess revealed on the final read must still be trimmed"
    );
    let (symbol, size, side, price, reduce_only) = calls[0].clone();
    assert_eq!(symbol, "BBB");
    assert_eq!(size, dec("0.1725"));
    assert_eq!(side, OrderSide::Long);
    assert_eq!(price, None);
    assert!(reduce_only);
    assert!(engine.instances[0].entry_blocked_pairs.is_empty());
}

/// Codex review PR #168 (P2, follow-up ③): a clean first read followed by
/// FAILED settle re-reads must not be accepted — the failed reads are
/// exactly where the late-fill overfill could have become visible, so the
/// stale clean value is unverified and the reconciliation fails closed.
#[tokio::test]
async fn entry_reconcile_clean_read_with_failed_rereads_fails_closed() {
    let connector = Arc::new(DummyConnector::default());
    connector.filled_by_symbol.lock().unwrap().insert(
        "BBB".to_string(),
        VecDeque::from(vec![vec![("legB".to_string(), dec("2.4291"))]]),
    );
    // Read 1 succeeds with a clean (pre-late-fill) snapshot; every later
    // get_positions call fails.
    *connector.positions_script.lock().unwrap() =
        VecDeque::from(vec![vec![position_721("BBB", "2.4291", -1)]]);
    *connector.positions_fail_after_calls.lock().unwrap() = Some(1);

    let mut engine = PairTradeEngine::test_instance(connector.clone());
    engine.cfg.dry_run = false;
    engine.risk_state_path = std::env::temp_dir().join(format!(
        "pairtrade-721-settle-fail-risk-{}.json",
        std::process::id()
    ));
    seed_state(&mut engine, "AAA/BBB");
    engine.instances[0]
        .states
        .get_mut("AAA/BBB")
        .unwrap()
        .pending_entry = Some(PendingOrders {
        legs: vec![entry_leg_721("BBB", "legB", "2.4291", OrderSide::Short)],
        direction: PositionDirection::LongSpread,
        placed_at: Instant::now(),
        placed_ts_ms: 0,
        hedge_retry_count: 0,
        post_only_hybrid: false,
        exit_taker_takeover_at: None,
    });
    let mut price_map = HashMap::new();
    price_map.insert("BBB".to_string(), snapshot_721("2000.0"));

    engine
        .reconcile_pending_orders(0, "AAA/BBB", &price_map)
        .await
        .unwrap();

    assert_eq!(
        connector.calls.lock().unwrap().len(),
        0,
        "no trim can be placed from an unverified stale reading"
    );
    assert!(
        engine.instances[0]
            .entry_blocked_pairs
            .get("AAA/BBB")
            .is_some_and(|r| r.contains("fetch_failed")),
        "clean-then-failed settle reads must fail closed, not record ok"
    );
    let _ = std::fs::remove_file(&engine.risk_state_path);
}
