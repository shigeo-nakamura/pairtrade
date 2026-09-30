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

/// What the tape task tells the runtime: prints AND its own health
/// (Codex P1, pairtrade#361), so the paper sim never quotes blind.
#[derive(Debug, Clone, PartialEq)]
pub enum TapeEvent {
    /// The venue acked the `trades` subscription.
    Up,
    /// The subscription was lost (error, close, silence, or a malformed
    /// trade row); a reconnect follows. Carries the reason.
    Down(String),
    Print(Print),
}

/// The venue's ack for our `trades` subscription.
pub fn is_trades_ack(text: &str, market: &str) -> bool {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return false;
    };
    v.get("type").and_then(|t| t.as_str()) == Some("subscribed")
        && v.get("channel").and_then(|c| c.as_str()) == Some("trades")
        && v.get("id").and_then(|i| i.as_str()) == Some(market)
}

/// Paper-sim tape health. Starts not ready; ready only after an ack.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TapeHealth {
    pub ready: bool,
    /// Start (ms) of the current outage after a disconnect.
    pub gap_since_ms: Option<u64>,
    /// Why the current outage started.
    pub gap_reason: Option<String>,
}

/// What the runtime must do after a health event.
#[derive(Debug, Clone, PartialEq)]
pub enum HealthAction {
    None,
    /// Pull every virtual quote now: prints are no longer seen.
    PullQuotes,
    /// Back up after a gap: record [start, end] so the G1 readout can
    /// exclude or count it.
    GapEnded {
        start_ms: u64,
        end_ms: u64,
        reason: String,
    },
}

impl TapeHealth {
    pub fn on_event(&mut self, event: &TapeEvent, now_ms: u64) -> HealthAction {
        match event {
            TapeEvent::Print(_) => HealthAction::None,
            TapeEvent::Down(reason) => {
                let was_ready = self.ready;
                self.ready = false;
                if was_ready {
                    self.gap_reason = Some(reason.clone());
                    // Only the ready → down edge starts a gap, so the start
                    // of an outage is never moved by a repeated Down.
                    self.gap_since_ms = Some(now_ms);
                }
                HealthAction::PullQuotes
            }
            TapeEvent::Up => {
                self.ready = true;
                match self.gap_since_ms.take() {
                    Some(start_ms) => HealthAction::GapEnded {
                        start_ms,
                        end_ms: now_ms,
                        reason: self.gap_reason.take().unwrap_or_default(),
                    },
                    None => HealthAction::None,
                }
            }
        }
    }
}

/// Parse one WS text frame; non-trade frames yield `Ok(vec![])`. A row of
/// a valid `trades` frame for our market that does not parse (missing
/// field, unknown side, bad decimal) is an `Err` carrying the row
/// (truncated): the tape is no longer trustworthy, so the caller drops the
/// subscription instead of silently skipping a print (Codex P1,
/// pairtrade#361).
pub fn parse_frame(text: &str, market: &str) -> Result<Vec<Print>, String> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return Ok(Vec::new());
    };
    if v.get("channel").and_then(|c| c.as_str()) != Some("trades")
        || v.get("type").and_then(|t| t.as_str()) != Some("channel_data")
        || v.get("id").and_then(|i| i.as_str()) != Some(market)
    {
        return Ok(Vec::new());
    }
    let Some(rows) = v.get("contents").and_then(|c| c.as_array()) else {
        return Err(truncate(&v.to_string()));
    };
    rows.iter()
        .map(|row| parse_row(row).ok_or_else(|| truncate(&row.to_string())))
        .collect()
}

fn parse_row(row: &serde_json::Value) -> Option<Print> {
    let w = serde_json::from_value::<TradeWire>(row.clone()).ok()?;
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
}

fn truncate(s: &str) -> String {
    s.chars().take(200).collect()
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

/// Run forever: connect, subscribe, forward prints and health events;
/// reconnect with backoff. `Up` is sent on the venue's subscribe ack and
/// `Down` whenever an acked subscription is lost.
pub async fn run(url: String, market: String, tx: mpsc::Sender<TapeEvent>) {
    let mut backoff = Duration::from_secs(1);
    let mut dedupe = Dedupe::new(20_000);
    loop {
        match tokio_tungstenite::connect_async(url.as_str()).await {
            Ok((mut ws, _)) => {
                let sub =
                    serde_json::json!({"type": "subscribe", "channel": "trades", "id": market});
                let mut up = false;
                let mut down_reason = "disconnected";
                if ws.send(Message::Text(sub.to_string())).await.is_ok() {
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
                                if !up && is_trades_ack(&text, &market) {
                                    up = true;
                                    backoff = Duration::from_secs(1);
                                    log::info!("[ARCUS_VOL] trades tape subscribed ({market})");
                                    if tx.send(TapeEvent::Up).await.is_err() {
                                        return;
                                    }
                                    continue;
                                }
                                match parse_frame(&text, &market) {
                                    Ok(prints) => {
                                        for p in prints {
                                            if dedupe.first(&p.trade_id)
                                                && tx.send(TapeEvent::Print(p)).await.is_err()
                                            {
                                                return;
                                            }
                                        }
                                    }
                                    Err(row) => {
                                        log::warn!(
                                            "[ARCUS_VOL] malformed trade row, resubscribing: {row}"
                                        );
                                        down_reason = "malformed_trade";
                                        break;
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
                if up {
                    log::warn!(
                        "[ARCUS_VOL] trades tape down; paper quotes pulled until resubscribed"
                    );
                    if tx
                        .send(TapeEvent::Down(down_reason.to_string()))
                        .await
                        .is_err()
                    {
                        return;
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
        let prints = parse_frame(frame, "BTC-USD").unwrap();
        assert_eq!(prints.len(), 2);
        assert_eq!(prints[0].taker, OrderSide::Long);
        assert_eq!(prints[0].px, Decimal::from_str("83643").unwrap());
        assert_eq!(prints[1].taker, OrderSide::Short);
        assert!(parse_frame(frame, "ETH-USD").unwrap().is_empty());
        assert!(parse_frame(
            r#"{"type":"subscribed","channel":"trades","id":"BTC-USD","contents":{}}"#,
            "BTC-USD"
        )
        .unwrap()
        .is_empty());
        assert!(parse_frame("not json", "BTC-USD").unwrap().is_empty());
    }

    #[test]
    fn only_the_trades_ack_for_our_market_counts_as_up() {
        let ack = r#"{"type":"subscribed","channel":"trades","id":"BTC-USD","contents":{}}"#;
        assert!(is_trades_ack(ack, "BTC-USD"));
        assert!(!is_trades_ack(ack, "ETH-USD"));
        assert!(!is_trades_ack(
            r#"{"type":"subscribed","channel":"bbo","id":"BTC-USD"}"#,
            "BTC-USD"
        ));
        assert!(!is_trades_ack(r#"{"type":"connected"}"#, "BTC-USD"));
        // A data frame on the channel is not the subscription ack.
        assert!(!is_trades_ack(
            r#"{"type":"channel_data","channel":"trades","id":"BTC-USD","contents":[]}"#,
            "BTC-USD"
        ));
    }

    #[test]
    fn health_starts_not_ready_pulls_on_down_and_reports_the_gap() {
        let mut h = TapeHealth::default();
        assert!(!h.ready);
        // A drop before the first ack is not a gap in a running tape.
        assert_eq!(
            h.on_event(&TapeEvent::Down("disconnected".into()), 500),
            HealthAction::PullQuotes
        );
        assert_eq!(h.gap_since_ms, None);
        assert_eq!(h.on_event(&TapeEvent::Up, 1_000), HealthAction::None);
        assert!(h.ready);
        assert_eq!(
            h.on_event(&TapeEvent::Down("disconnected".into()), 5_000),
            HealthAction::PullQuotes
        );
        assert!(!h.ready);
        // A second Down during the same outage keeps the original start.
        assert_eq!(
            h.on_event(&TapeEvent::Down("disconnected".into()), 6_000),
            HealthAction::PullQuotes
        );
        assert_eq!(
            h.on_event(&TapeEvent::Up, 9_000),
            HealthAction::GapEnded {
                start_ms: 5_000,
                end_ms: 9_000,
                reason: "disconnected".into()
            }
        );
        assert!(h.ready);
        assert_eq!(h.gap_since_ms, None);
    }

    #[test]
    fn a_malformed_trade_row_downs_the_tape_and_resubscribe_brings_it_back() {
        let frame = r#"{"type":"channel_data","channel":"trades","id":"BTC-USD","contents":[
            {"side":"BUY","price":"83643","size":"0.1","tradeId":"1","timestamp":10},
            {"side":"HOLD","price":"83643","size":"0.1","tradeId":"2","timestamp":11}]}"#;
        let err = parse_frame(frame, "BTC-USD").unwrap_err();
        assert!(err.contains("HOLD"), "the offending row is surfaced: {err}");
        for bad in [
            r#"{"type":"channel_data","channel":"trades","id":"BTC-USD","contents":[{"side":"BUY","price":"x","size":"0.1","tradeId":"3","timestamp":1}]}"#,
            r#"{"type":"channel_data","channel":"trades","id":"BTC-USD","contents":[{"side":"BUY","size":"0.1","tradeId":"4","timestamp":1}]}"#,
        ] {
            assert!(parse_frame(bad, "BTC-USD").is_err(), "{bad}");
        }
        // The tape task then sends Down("malformed_trade"): quotes pulled,
        // and the resubscribe's Up records the gap with its reason.
        let mut h = TapeHealth::default();
        h.on_event(&TapeEvent::Up, 1_000);
        assert_eq!(
            h.on_event(&TapeEvent::Down("malformed_trade".into()), 2_000),
            HealthAction::PullQuotes
        );
        assert_eq!(
            h.on_event(&TapeEvent::Up, 3_500),
            HealthAction::GapEnded {
                start_ms: 2_000,
                end_ms: 3_500,
                reason: "malformed_trade".into()
            }
        );
        assert!(h.ready);
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
