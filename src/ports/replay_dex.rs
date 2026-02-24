use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dex_connector::{
    BalanceResponse, CanceledOrdersResponse, CombinedBalanceResponse, CreateOrderResponse,
    DexConnector, DexError, FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse,
    OrderBookLevel, OrderBookSnapshot, OrderSide, PositionSnapshot, TickerResponse, TpSl,
    TriggerOrderStyle,
};
use rand;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

// Data structures that mirror the JSONL dump file
#[derive(Debug, Clone, Deserialize)]
struct DumpedSymbolSnapshot {
    price: Decimal,
    funding_rate: Decimal,
    bid_size: Decimal,
    ask_size: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
struct DumpedDataEntry {
    timestamp: i64,
    prices: HashMap<String, DumpedSymbolSnapshot>,
}

#[derive(Debug)]
pub struct ReplayConnector {
    data: Vec<DumpedDataEntry>,
    cursor: AtomicUsize,
}

impl ReplayConnector {
    pub fn new(path: &str) -> Result<Self, DexError> {
        let file = File::open(path)
            .map_err(|e| DexError::Other(format!("failed to open replay file: {}", e)))?;
        let reader = BufReader::new(file);
        let mut data = Vec::new();

        for line in reader.lines() {
            let line =
                line.map_err(|e| DexError::Other(format!("failed to read replay line: {}", e)))?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: DumpedDataEntry = serde_json::from_str(&line).map_err(|e| {
                DexError::Other(format!("failed to parse replay entry '{}': {}", line, e))
            })?;
            data.push(entry);
        }

        if data.is_empty() {
            return Err(DexError::Other(
                anyhow!("Data dump file is empty or invalid").to_string(),
            ));
        }

        Ok(Self {
            data,
            cursor: AtomicUsize::new(0),
        })
    }

    // Advances the simulation by one step. Returns false if the end is reached.
    pub fn tick(&self) -> bool {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        if current_cursor < self.data.len() - 1 {
            self.cursor.fetch_add(1, AtomicOrdering::SeqCst);
            true
        } else {
            false
        }
    }

    pub fn current_timestamp_secs(&self) -> Option<i64> {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        self.data.get(current_cursor).map(|e| e.timestamp / 1000) // stored as ms
    }
}

#[async_trait]
impl DexConnector for ReplayConnector {
    async fn start(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn restart(&self, _within_hours: i32) -> Result<(), DexError> {
        Ok(())
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        let current_snapshot = self
            .data
            .get(current_cursor)
            .ok_or_else(|| DexError::Other("Cursor out of bounds".to_string()))?;

        let symbol_data = current_snapshot.prices.get(symbol).ok_or_else(|| {
            DexError::Other(format!(
                "Symbol '{}' not found in this data entry at cursor {}",
                symbol, current_cursor
            ))
        })?;

        let price = test_price.unwrap_or(symbol_data.price);

        Ok(TickerResponse {
            symbol: symbol.to_string(),
            price,
            min_tick: None,
            min_order: None,
            size_decimals: None,
            volume: None,
            num_trades: None,
            open_interest: None,
            funding_rate: Some(symbol_data.funding_rate),
            oracle_price: Some(symbol_data.price),
        })
    }

    async fn get_filled_orders(&self, _symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        Ok(FilledOrdersResponse { orders: vec![] })
    }

    async fn get_canceled_orders(&self, _symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        Ok(CanceledOrdersResponse { orders: vec![] })
    }

    async fn get_open_orders(&self, _symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        Ok(OpenOrdersResponse { orders: vec![] })
    }

    async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        Ok(BalanceResponse {
            equity: Decimal::new(10_000, 0),
            balance: Decimal::new(10_000, 0),
            position_entry_price: None,
            position_sign: None,
        })
    }

    async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
        Ok(CombinedBalanceResponse::default())
    }

    async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
        Ok(Vec::new())
    }

    async fn get_last_trades(&self, _symbol: &str) -> Result<LastTradesResponse, DexError> {
        Ok(LastTradesResponse { trades: vec![] })
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        _depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        let current_snapshot = self
            .data
            .get(current_cursor)
            .ok_or_else(|| DexError::Other("Cursor out of bounds".to_string()))?;

        let symbol_data = current_snapshot.prices.get(symbol).ok_or_else(|| {
            DexError::Other(format!(
                "Symbol '{}' not found in this data entry at cursor {}",
                symbol, current_cursor
            ))
        })?;

        Ok(OrderBookSnapshot {
            bids: vec![OrderBookLevel {
                price: symbol_data.price,
                size: symbol_data.bid_size,
            }],
            asks: vec![OrderBookLevel {
                price: symbol_data.price,
                size: symbol_data.ask_size,
            }],
        })
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
        _reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        let current_cursor = self.cursor.load(AtomicOrdering::SeqCst);
        let snapshot = self
            .data
            .get(current_cursor)
            .ok_or_else(|| DexError::Other("Cursor out of bounds".to_string()))?;
        let fallback_price = snapshot
            .prices
            .get(symbol)
            .ok_or_else(|| DexError::Other(format!("Symbol '{}' not found", symbol)))?
            .price;
        let fill_price = price.unwrap_or(fallback_price);

        log::info!(
            "[BACKTEST_FILL] symbol={}, side={:?}, size={}, price={}",
            symbol,
            side,
            size,
            fill_price
        );

        Ok(CreateOrderResponse {
            order_id: rand::random::<u64>().to_string(),
            exchange_order_id: None,
            ordered_price: fill_price,
            ordered_size: size,
        })
    }

    async fn create_advanced_trigger_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        trigger_px: Decimal,
        limit_px: Option<Decimal>,
        _order_style: TriggerOrderStyle,
        _slippage_bps: Option<u32>,
        _tpsl: TpSl,
        _reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        self.create_order(
            symbol,
            size,
            side,
            limit_px.or(Some(trigger_px)),
            None,
            false,
            None,
        )
        .await
    }

    async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Ok(())
    }

    async fn cancel_orders(
        &self,
        _symbol: Option<String>,
        _order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        Ok(())
    }

    async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn is_upcoming_maintenance(&self, _within_hours: i64) -> bool {
        false
    }

    async fn sign_evm_65b(&self, message: &str) -> Result<String, DexError> {
        Ok(format!("signed:{}", message))
    }

    async fn sign_evm_65b_with_eip191(&self, message: &str) -> Result<String, DexError> {
        Ok(format!("signed_eip191:{}", message))
    }
}
