//! Public `trades` tape for the DRY_RUN fill sim (bot-strategy#1093).
//!
//! The trait's `LastTrade` has no trade id or timestamp, so the sim cannot
//! tell a new print from one it already applied; this reads the venue's
//! `trades` channel directly (no auth, no snapshot on subscribe: frames are
//! arrays of fills from one taker match) and forwards each print once.

use dex_connector::OrderSide;
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone, PartialEq)]
pub struct Print {
    pub trade_id: String,
    pub ts_us: u64,
    pub px: Decimal,
    pub qty: Decimal,
    /// Aggressor side.
    pub taker: OrderSide,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TradeWire {
    trade_id: String,
    timestamp: u64,
    price: String,
    size: String,
    side: String,
}

/// Parse one WS text frame; non-trade frames yield nothing.
pub fn parse_frame(text: &str, market: &str) -> Vec<Print> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return Vec::new();
    };
    if v.get("channel").and_then(|c| c.as_str()) != Some("trades")
        || v.get("type").and_then(|t| t.as_str()) != Some("channel_data")
        || v.get("id").and_then(|i| i.as_str()) != Some(market)
    {
        return Vec::new();
    }
    let Some(rows) = v.get("contents").and_then(|c| c.as_array()) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| serde_json::from_value::<TradeWire>(row.clone()).ok())
        .filter_map(|w| {
            let taker = match w.side.as_str() {
                "BUY" => OrderSide::Long,
                "SELL" => OrderSide::Short,
                _ => return None,
            };
            Some(Print {
                trade_id: w.trade_id,
                ts_us: w.timestamp,
                px: Decimal::from_str(&w.price).ok()?,
                qty: Decimal::from_str(&w.size).ok()?,
                taker,
            })
        })
        .collect()
}

/// Bounded "seen" set: a reconnect never replays (no snapshot), but a
/// duplicate frame must not fill the sim twice.
pub struct Dedupe {
    seen: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl Dedupe {
    pub fn new(cap: usize) -> Self {
        Dedupe {
            seen: HashSet::new(),
            order: VecDeque::new(),
            cap,
        }
    }

    /// True the first time `id` is offered.
    pub fn first(&mut self, id: &str) -> bool {
        if self.seen.contains(id) {
            return false;
        }
        self.seen.insert(id.to_string());
        self.order.push_back(id.to_string());
        if self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        true
    }
}

/// Run forever: connect, subscribe, forward prints; reconnect with backoff.
pub async fn run(url: String, market: String, tx: mpsc::Sender<Print>) {
    let mut backoff = Duration::from_secs(1);
    let mut dedupe = Dedupe::new(20_000);
    loop {
        match tokio_tungstenite::connect_async(url.as_str()).await {
            Ok((mut ws, _)) => {
                let sub =
                    serde_json::json!({"type": "subscribe", "channel": "trades", "id": market});
                let subscribed = ws.send(Message::Text(sub.to_string())).await.is_ok();
                if subscribed {
                    log::info!("[ARCUS_VOL] trades tape subscribed ({market})");
                    backoff = Duration::from_secs(1);
                    loop {
                        let next = tokio::time::timeout(Duration::from_secs(60), ws.next()).await;
                        let msg = match next {
                            Ok(Some(Ok(m))) => m,
                            Ok(Some(Err(e))) => {
                                log::warn!("[ARCUS_VOL] trades tape error: {e}");
                                break;
                            }
                            Ok(None) => break,
                            Err(_) => {
                                // BTC prints every few seconds; a silent minute is a
                                // dead subscription, not a quiet market.
                                log::warn!("[ARCUS_VOL] trades tape silent 60s, reconnecting");
                                break;
                            }
                        };
                        match msg {
                            Message::Text(text) => {
                                for p in parse_frame(&text, &market) {
                                    if dedupe.first(&p.trade_id) && tx.send(p).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Message::Ping(data) => {
                                let _ = ws.send(Message::Pong(data)).await;
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                }
            }
            Err(e) => log::warn!("[ARCUS_VOL] trades tape connect failed: {e}"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_trade_frames_and_ignores_others() {
        let frame = r#"{"type":"channel_data","channel":"trades","id":"BTC-USD","contents":[
            {"marketDisplayName":"BTC-USD","marketId":1,"side":"BUY","price":"83643","size":"0.00179243",
             "tradeId":"8943469","timestamp":1790709184183723,"makerOrderId":"a","takerOrderId":"b",
             "makerAddress":"0x1","takerAddress":"0x2","sequenceNumber":1},
            {"side":"SELL","price":"83642.9","size":"0.5","tradeId":"8943470","timestamp":1790709184183724}]}"#;
        let prints = parse_frame(frame, "BTC-USD");
        assert_eq!(prints.len(), 2);
        assert_eq!(prints[0].taker, OrderSide::Long);
        assert_eq!(prints[0].px, Decimal::from_str("83643").unwrap());
        assert_eq!(prints[1].taker, OrderSide::Short);
        assert!(parse_frame(frame, "ETH-USD").is_empty());
        assert!(parse_frame(
            r#"{"type":"subscribed","channel":"trades","id":"BTC-USD","contents":{}}"#,
            "BTC-USD"
        )
        .is_empty());
        assert!(parse_frame("not json", "BTC-USD").is_empty());
    }

    #[test]
    fn dedupe_forwards_each_id_once_within_its_window() {
        let mut d = Dedupe::new(2);
        assert!(d.first("a"));
        assert!(!d.first("a"));
        assert!(d.first("b"));
        assert!(d.first("c")); // evicts "a"
        assert!(d.first("a"));
        assert!(!d.first("c"));
    }
}
