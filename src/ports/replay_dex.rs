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
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

// Data structures that mirror the JSONL dump file
#[derive(Debug, Clone, Deserialize)]
struct DumpedSymbolSnapshot {
    price: Decimal,
    funding_rate: Decimal,
    #[serde(default)]
    bid_price: Option<Decimal>,
    #[serde(default)]
    ask_price: Option<Decimal>,
    bid_size: Decimal,
    ask_size: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
struct DumpedDataEntry {
    timestamp: i64,
    prices: HashMap<String, DumpedSymbolSnapshot>,
}

// Bincode-compatible representations using f64 (bincode doesn't support Decimal).
#[derive(Serialize, Deserialize)]
struct BincodeSymbolSnapshot {
    price: f64,
    funding_rate: f64,
    bid_price: f64,
    ask_price: f64,
    bid_size: f64,
    ask_size: f64,
}

#[derive(Serialize, Deserialize)]
struct BincodeDataEntry {
    timestamp: i64,
    prices: HashMap<String, BincodeSymbolSnapshot>,
}

impl From<&DumpedDataEntry> for BincodeDataEntry {
    fn from(e: &DumpedDataEntry) -> Self {
        Self {
            timestamp: e.timestamp,
            prices: e
                .prices
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        BincodeSymbolSnapshot {
                            price: v.price.to_f64().unwrap_or(0.0),
                            funding_rate: v.funding_rate.to_f64().unwrap_or(0.0),
                            bid_price: v.bid_price.and_then(|p| p.to_f64()).unwrap_or(0.0),
                            ask_price: v.ask_price.and_then(|p| p.to_f64()).unwrap_or(0.0),
                            bid_size: v.bid_size.to_f64().unwrap_or(0.0),
                            ask_size: v.ask_size.to_f64().unwrap_or(0.0),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<BincodeDataEntry> for DumpedDataEntry {
    fn from(e: BincodeDataEntry) -> Self {
        Self {
            timestamp: e.timestamp,
            prices: e
                .prices
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        DumpedSymbolSnapshot {
                            price: Decimal::from_f64(v.price).unwrap_or_default(),
                            funding_rate: Decimal::from_f64(v.funding_rate).unwrap_or_default(),
                            bid_price: if v.bid_price == 0.0 {
                                None
                            } else {
                                Decimal::from_f64(v.bid_price)
                            },
                            ask_price: if v.ask_price == 0.0 {
                                None
                            } else {
                                Decimal::from_f64(v.ask_price)
                            },
                            bid_size: Decimal::from_f64(v.bid_size).unwrap_or_default(),
                            ask_size: Decimal::from_f64(v.ask_size).unwrap_or_default(),
                        },
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct ReplayConnector {
    data: Vec<DumpedDataEntry>,
    cursor: AtomicUsize,
}

impl ReplayConnector {
    pub fn new(path: &str) -> Result<Self, DexError> {
        let data = if path.ends_with(".bin") {
            Self::load_bincode(path)?
        } else {
            Self::load_jsonl(path)?
        };

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

    fn load_jsonl(path: &str) -> Result<Vec<DumpedDataEntry>, DexError> {
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
        Ok(data)
    }

    fn load_bincode(path: &str) -> Result<Vec<DumpedDataEntry>, DexError> {
        let bytes = std::fs::read(path)
            .map_err(|e| DexError::Other(format!("failed to read bincode file: {}", e)))?;
        let bincode_data: Vec<BincodeDataEntry> = bincode::deserialize(&bytes)
            .map_err(|e| DexError::Other(format!("failed to deserialize bincode: {}", e)))?;
        Ok(bincode_data.into_iter().map(DumpedDataEntry::from).collect())
    }

    /// Convert a JSONL file to bincode format. Used by the convert-data tool.
    pub fn convert_jsonl_to_bincode(input: &str, output: &str) -> Result<(), DexError> {
        Self::convert_jsonl_to_bincode_with_interval(input, output, 0)
    }

    /// Convert JSONL to bincode with optional downsampling.
    /// `interval_secs`: minimum seconds between samples (0 = keep all).
    pub fn convert_jsonl_to_bincode_with_interval(
        input: &str,
        output: &str,
        interval_secs: u64,
    ) -> Result<(), DexError> {
        let data = Self::load_jsonl(input)?;
        let original_len = data.len();
        let filtered: Vec<&_> = if interval_secs > 0 {
            let interval_ms = (interval_secs * 1000) as i64;
            let mut last_ts: i64 = 0;
            data.iter()
                .filter(|e| {
                    if e.timestamp - last_ts >= interval_ms {
                        last_ts = e.timestamp;
                        true
                    } else {
                        false
                    }
                })
                .collect()
        } else {
            data.iter().collect()
        };
        eprintln!(
            "Records: {} -> {} (interval={}s)",
            original_len,
            filtered.len(),
            interval_secs
        );
        let bincode_data: Vec<BincodeDataEntry> =
            filtered.iter().map(|e| BincodeDataEntry::from(*e)).collect();
        let bytes = bincode::serialize(&bincode_data)
            .map_err(|e| DexError::Other(format!("failed to serialize bincode: {}", e)))?;
        std::fs::write(output, bytes)
            .map_err(|e| DexError::Other(format!("failed to write bincode file: {}", e)))?;
        Ok(())
    }

    /// Reset cursor to beginning for batch mode reuse.
    pub fn reset(&self) {
        self.cursor.store(0, AtomicOrdering::SeqCst);
    }

    /// Number of data entries.
    pub fn len(&self) -> usize {
        self.data.len()
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
                price: symbol_data.bid_price.unwrap_or(symbol_data.price),
                size: symbol_data.bid_size,
            }],
            asks: vec![OrderBookLevel {
                price: symbol_data.ask_price.unwrap_or(symbol_data.price),
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
        let symbol_data = snapshot
            .prices
            .get(symbol)
            .ok_or_else(|| DexError::Other(format!("Symbol '{}' not found", symbol)))?;

        // Fill at the appropriate side of the book (taker model):
        // buys fill at ask price, sells fill at bid price.
        let fill_price = match side {
            OrderSide::Long => symbol_data.ask_price.unwrap_or(symbol_data.price),
            OrderSide::Short => symbol_data.bid_price.unwrap_or(symbol_data.price),
        };

        log::info!(
            "[BACKTEST_FILL] symbol={}, side={:?}, size={}, price={} (limit={:?})",
            symbol,
            side,
            size,
            fill_price,
            price,
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
