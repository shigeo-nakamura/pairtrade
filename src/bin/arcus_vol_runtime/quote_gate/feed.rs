//! Reference feeds for the gate (design §2.1 / §2.3 v1): one Binance
//! USDT-M futures WebSocket (combined `<sym>@bookTicker` + `<sym>@aggTrade`
//! stream) and one Hyperliquid WebSocket (`bbo` + `trades` for the coin).
//! Each task stamps every frame with its own receive time and pushes it into
//! the shared `RefState` under the lock; `Up` / `Down` go the same way (gap
//! cooldown, warmup restart). Reconnect with backoff as the trades tape
//! (`tape.rs`), through `dex_connector::ws_connect::connect_ws` so a dead
//! address cannot hang a connect. A dead feed only ever makes the gate fall
//! back: nothing here can add a quote.
//!
//! Binance frames are sampled as the study collector recorded them
//! (`xtape_rf.py`): a price change at least 20 ms after the last kept
//! frame, or any change at least 250 ms after it. The features were trained
//! on that sampling.

use super::{now_ms, RefState};
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

pub const BINANCE_WS_BASE: &str = "wss://fstream.binance.com/stream";
pub const HYPERLIQUID_WS: &str = "wss://api.hyperliquid.xyz/ws";
/// Binance bookTicker streams continuously: a silent minute is a dead socket.
const BN_SILENCE: Duration = Duration::from_secs(60);
/// Hyperliquid closes idle sockets; we ping on 30 s of silence and give up
/// after two.
const HL_SILENCE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefFeed {
    Binance,
    Hyperliquid,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RefEvent {
    Up,
    Down(String),
    BnL1 {
        bid: f64,
        ask: f64,
        bsz: f64,
        asz: f64,
    },
    BnTrade {
        maker_is_buyer: bool,
        px: f64,
        qty: f64,
    },
    HlL1 {
        bid: f64,
        ask: f64,
    },
    HlTrade {
        buy: bool,
        px: f64,
        qty: f64,
    },
}

fn num(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    }
    .filter(|x| x.is_finite())
}

/// One Binance combined-stream frame: `{"stream": "...", "data": {...}}`.
/// `Ok(None)` for frames of other kinds; `Err` for a data frame that does
/// not parse (the feed resubscribes rather than skip a record silently).
pub fn parse_binance(text: &str) -> Result<Option<RefEvent>, String> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return Ok(None);
    };
    let Some(d) = v.get("data") else {
        return Ok(None);
    };
    match d.get("e").and_then(|e| e.as_str()) {
        Some("bookTicker") => match (num(&d["b"]), num(&d["a"]), num(&d["B"]), num(&d["A"])) {
            (Some(bid), Some(ask), Some(bsz), Some(asz)) if bid > 0.0 && ask > 0.0 => {
                Ok(Some(RefEvent::BnL1 { bid, ask, bsz, asz }))
            }
            _ => Err(truncate(text)),
        },
        Some("aggTrade") => match (num(&d["p"]), num(&d["q"]), d["m"].as_bool()) {
            (Some(px), Some(qty), Some(m)) if px > 0.0 => Ok(Some(RefEvent::BnTrade {
                maker_is_buyer: m,
                px,
                qty,
            })),
            _ => Err(truncate(text)),
        },
        _ => Ok(None),
    }
}

/// One Hyperliquid frame for `coin`: `bbo` → one L1, `trades` → one event
/// per trade of the coin, anything else → none. A malformed row is an
/// error.
pub fn parse_hyperliquid(text: &str, coin: &str) -> Result<Vec<RefEvent>, String> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return Ok(Vec::new());
    };
    match v.get("channel").and_then(|c| c.as_str()) {
        Some("bbo") => {
            let d = &v["data"];
            if d["coin"].as_str() != Some(coin) {
                return Ok(Vec::new());
            }
            let bbo = d["bbo"].as_array().ok_or_else(|| truncate(text))?;
            match (
                bbo.first().and_then(|b| num(&b["px"])),
                bbo.get(1).and_then(|a| num(&a["px"])),
            ) {
                (Some(bid), Some(ask)) if bid > 0.0 && ask > 0.0 => {
                    Ok(vec![RefEvent::HlL1 { bid, ask }])
                }
                // One side empty: no L1 (not an error, the book can be one-sided).
                _ => Ok(Vec::new()),
            }
        }
        Some("trades") => v["data"]
            .as_array()
            .ok_or_else(|| truncate(text))?
            .iter()
            .filter(|t| t["coin"].as_str() == Some(coin))
            .map(
                |t| match (t["side"].as_str(), num(&t["px"]), num(&t["sz"])) {
                    (Some(side @ ("B" | "A")), Some(px), Some(qty)) if px > 0.0 => {
                        Ok(RefEvent::HlTrade {
                            buy: side == "B",
                            px,
                            qty,
                        })
                    }
                    _ => Err(truncate(&t.to_string())),
                },
            )
            .collect(),
        _ => Ok(Vec::new()),
    }
}

fn truncate(s: &str) -> String {
    s.chars().take(200).collect()
}

/// The study collector's Binance sampling rule (`xtape_rf.py::binance`).
#[derive(Debug, Default)]
pub struct BnSampler {
    last_ms: u64,
    last_px: Option<(u64, u64)>,
    last_sz: Option<(u64, u64)>,
}

impl BnSampler {
    /// Whether to keep a bookTicker frame received at `now_ms`.
    pub fn keep(&mut self, now_ms: u64, bid: f64, ask: f64, bsz: f64, asz: f64) -> bool {
        let px = (bid.to_bits(), ask.to_bits());
        let sz = (bsz.to_bits(), asz.to_bits());
        let since = now_ms.saturating_sub(self.last_ms);
        let price_changed = self.last_px != Some(px);
        let any_changed = price_changed || self.last_sz != Some(sz);
        let keep = (price_changed && since >= 20)
            || (any_changed && since >= 250)
            || self.last_px.is_none();
        if keep {
            self.last_ms = now_ms;
            self.last_px = Some(px);
            self.last_sz = Some(sz);
        }
        keep
    }
}

fn push(shared: &Arc<Mutex<RefState>>, feed: RefFeed, ev: RefEvent) {
    let rx = now_ms();
    shared
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .on_ref(feed, rx, ev);
}

/// Binance: connect, read, sample, reconnect forever.
pub async fn run_binance(ws_base: String, symbol: String, shared: Arc<Mutex<RefState>>) {
    let url = format!("{ws_base}?streams={symbol}@bookTicker/{symbol}@aggTrade");
    let mut backoff = Duration::from_secs(1);
    loop {
        match dex_connector::ws_connect::connect_ws(url.as_str()).await {
            Ok(mut ws) => {
                let mut up = false;
                let mut sampler = BnSampler::default();
                let mut down_reason = "disconnected";
                loop {
                    let msg = match tokio::time::timeout(BN_SILENCE, ws.next()).await {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => {
                            log::warn!("[GATE] binance feed error: {e}");
                            break;
                        }
                        Ok(None) => break,
                        Err(_) => {
                            log::warn!("[GATE] binance feed silent 60s, reconnecting");
                            down_reason = "silent";
                            break;
                        }
                    };
                    match msg {
                        Message::Text(text) => match parse_binance(&text) {
                            Ok(Some(ev)) => {
                                if !up {
                                    up = true;
                                    backoff = Duration::from_secs(1);
                                    log::info!("[GATE] binance feed up ({symbol})");
                                    push(&shared, RefFeed::Binance, RefEvent::Up);
                                }
                                let keep = match &ev {
                                    RefEvent::BnL1 { bid, ask, bsz, asz } => {
                                        sampler.keep(now_ms(), *bid, *ask, *bsz, *asz)
                                    }
                                    _ => true,
                                };
                                if keep {
                                    push(&shared, RefFeed::Binance, ev);
                                }
                            }
                            Ok(None) => {}
                            Err(frame) => {
                                log::warn!("[GATE] malformed binance frame, reconnecting: {frame}");
                                down_reason = "malformed";
                                break;
                            }
                        },
                        Message::Ping(data) => {
                            let _ = ws.send(Message::Pong(data)).await;
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
                if up {
                    log::warn!("[GATE] binance feed down ({down_reason}); gate falls back until it is back");
                    push(
                        &shared,
                        RefFeed::Binance,
                        RefEvent::Down(down_reason.to_string()),
                    );
                }
            }
            Err(e) => log::warn!("[GATE] binance feed connect failed: {e}"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// Hyperliquid: subscribe bbo + trades for `coin`, read, reconnect forever.
pub async fn run_hyperliquid(url: String, coin: String, shared: Arc<Mutex<RefState>>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        match dex_connector::ws_connect::connect_ws(url.as_str()).await {
            Ok(mut ws) => {
                let mut up = false;
                let mut down_reason = "disconnected";
                let mut silent = 0u8;
                let subs = ["bbo", "trades"].map(|t| {
                    serde_json::json!({"method": "subscribe", "subscription": {"type": t, "coin": coin}})
                });
                let mut sent = true;
                for s in subs {
                    if ws.send(Message::Text(s.to_string())).await.is_err() {
                        sent = false;
                        break;
                    }
                }
                if sent {
                    loop {
                        let msg = match tokio::time::timeout(HL_SILENCE, ws.next()).await {
                            Ok(Some(Ok(m))) => {
                                silent = 0;
                                m
                            }
                            Ok(Some(Err(e))) => {
                                log::warn!("[GATE] hyperliquid feed error: {e}");
                                break;
                            }
                            Ok(None) => break,
                            Err(_) => {
                                silent += 1;
                                if silent >= 2 {
                                    log::warn!("[GATE] hyperliquid feed silent 60s, reconnecting");
                                    down_reason = "silent";
                                    break;
                                }
                                let ping = serde_json::json!({"method": "ping"}).to_string();
                                if ws.send(Message::Text(ping)).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        };
                        match msg {
                            Message::Text(text) => {
                                match parse_hyperliquid(&text, &coin) {
                                    Ok(events) => {
                                        if !up
                                            && (!events.is_empty()
                                                || text.contains("subscriptionResponse"))
                                        {
                                            up = true;
                                            backoff = Duration::from_secs(1);
                                            log::info!("[GATE] hyperliquid feed up ({coin})");
                                            push(&shared, RefFeed::Hyperliquid, RefEvent::Up);
                                        }
                                        for ev in events {
                                            push(&shared, RefFeed::Hyperliquid, ev);
                                        }
                                    }
                                    Err(frame) => {
                                        log::warn!("[GATE] malformed hyperliquid frame, reconnecting: {frame}");
                                        down_reason = "malformed";
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
                    log::warn!("[GATE] hyperliquid feed down ({down_reason}); gate falls back until it is back");
                    push(
                        &shared,
                        RefFeed::Hyperliquid,
                        RefEvent::Down(down_reason.to_string()),
                    );
                }
            }
            Err(e) => log::warn!("[GATE] hyperliquid feed connect failed: {e}"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binance_combined_stream_frames() {
        let bt = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"BTCUSDT","b":"84550.60","B":"1.582","a":"84550.70","A":"29.141","T":1,"E":1}}"#;
        assert_eq!(
            parse_binance(bt).unwrap(),
            Some(RefEvent::BnL1 {
                bid: 84550.6,
                ask: 84550.7,
                bsz: 1.582,
                asz: 29.141
            })
        );
        let at = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":1,"p":"84550.60","q":"1.739","f":1,"l":2,"T":1,"m":true}}"#;
        assert_eq!(
            parse_binance(at).unwrap(),
            Some(RefEvent::BnTrade {
                maker_is_buyer: true,
                px: 84550.6,
                qty: 1.739
            })
        );
        assert_eq!(parse_binance(r#"{"result":null,"id":1}"#).unwrap(), None);
        assert_eq!(parse_binance("nope").unwrap(), None);
        // A data frame with a bad number is an error, never a skipped record.
        let bad = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","b":"x","B":"1","a":"2","A":"3"}}"#;
        assert!(parse_binance(bad).is_err());
        let bad = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","p":"1","q":"1"}}"#;
        assert!(parse_binance(bad).is_err(), "missing m");
    }

    #[test]
    fn parses_hyperliquid_bbo_and_trades_for_the_coin_only() {
        let bbo = r#"{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":"84556.0","sz":"0.07","n":2},{"px":"84557.0","sz":"34.5","n":111}]}}"#;
        assert_eq!(
            parse_hyperliquid(bbo, "BTC").unwrap(),
            vec![RefEvent::HlL1 {
                bid: 84556.0,
                ask: 84557.0
            }]
        );
        assert!(parse_hyperliquid(bbo, "ETH").unwrap().is_empty());
        let one_sided = r#"{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":"1","sz":"1","n":1},null]}}"#;
        assert!(parse_hyperliquid(one_sided, "BTC").unwrap().is_empty());
        let tr = r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"84478.0","sz":"0.02","time":1,"tid":1},{"coin":"ETH","side":"A","px":"1","sz":"1","time":1,"tid":2},{"coin":"BTC","side":"A","px":"84479.0","sz":"0.5","time":1,"tid":3}]}"#;
        assert_eq!(
            parse_hyperliquid(tr, "BTC").unwrap(),
            vec![
                RefEvent::HlTrade {
                    buy: true,
                    px: 84478.0,
                    qty: 0.02
                },
                RefEvent::HlTrade {
                    buy: false,
                    px: 84479.0,
                    qty: 0.5
                }
            ]
        );
        assert!(
            parse_hyperliquid(r#"{"channel":"subscriptionResponse","data":{}}"#, "BTC")
                .unwrap()
                .is_empty()
        );
        assert!(parse_hyperliquid(r#"{"channel":"pong"}"#, "BTC")
            .unwrap()
            .is_empty());
        let bad = r#"{"channel":"trades","data":[{"coin":"BTC","side":"X","px":"1","sz":"1"}]}"#;
        assert!(parse_hyperliquid(bad, "BTC").is_err());
    }

    #[test]
    fn binance_sampling_matches_the_study_collector() {
        let mut s = BnSampler::default();
        assert!(s.keep(1_000, 1.0, 2.0, 1.0, 1.0), "first frame");
        assert!(!s.keep(1_010, 1.1, 2.0, 1.0, 1.0), "price change < 20 ms");
        assert!(s.keep(1_020, 1.1, 2.0, 1.0, 1.0), "price change >= 20 ms");
        assert!(
            !s.keep(1_100, 1.1, 2.0, 5.0, 1.0),
            "size-only change < 250 ms"
        );
        assert!(!s.keep(1_300, 1.1, 2.0, 1.0, 1.0), "unchanged");
        assert!(
            s.keep(1_300, 1.1, 2.0, 5.0, 1.0),
            "size-only change >= 250 ms"
        );
        assert!(
            !s.keep(2_000, 1.1, 2.0, 5.0, 1.0),
            "no change at all is never kept"
        );
    }
}
