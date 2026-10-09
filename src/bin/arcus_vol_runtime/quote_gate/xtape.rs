//! The study tape format (`xtape.py` / `xtape_rf.py`, bot-strategy#1093 /
//! #1113): one JSON object per line, `{"rx": <unix s>, "src": <stream>,
//! "d": <venue payload>}`. Used by the golden / no-lookahead tests and
//! available to a replay binary; the live feeds do not go through it.

use super::features::FeatureEngine;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq)]
pub enum Payload {
    OwnL1 {
        bid: f64,
        ask: f64,
        bsz: f64,
        asz: f64,
    },
    OwnL2 {
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    },
    OwnTrade {
        taker_buy: bool,
        px: f64,
        qty: f64,
        id: String,
    },
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
        tid: String,
    },
    /// `connected` / `gap` of a stream (`src`).
    Event {
        src: String,
        gap: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub rx_ms: u64,
    pub payload: Payload,
}

fn f(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn levels(v: &serde_json::Value) -> Vec<(f64, f64)> {
    v.as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|r| {
                    let r = r.as_array()?;
                    Some((f(r.first()?)?, f(r.get(1)?)?))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Parse one tape line into zero or more records (a trades frame carries
/// several). Lines of other sources, empty payloads and malformed rows yield
/// nothing, as `rf_common.parse_file` skips them.
pub fn parse_line(line: &str) -> Vec<Record> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
        return Vec::new();
    };
    let (Some(rx), Some(src)) = (v["rx"].as_f64(), v["src"].as_str()) else {
        return Vec::new();
    };
    let rx_ms = (rx * 1_000.0).round() as u64;
    let d = &v["d"];
    let rec = |payload| Record { rx_ms, payload };
    if let Some(ev) = d.get("event").and_then(|e| e.as_str()) {
        if src == "meta" {
            return Vec::new();
        }
        return vec![rec(Payload::Event {
            src: src.to_string(),
            gap: ev == "gap",
        })];
    }
    match src {
        "arcus_bbo" => {
            let (b, a) = (&d["bestBid"], &d["bestAsk"]);
            match (f(&b["price"]), f(&a["price"]), f(&b["size"]), f(&a["size"])) {
                (Some(bid), Some(ask), Some(bsz), Some(asz)) => {
                    vec![rec(Payload::OwnL1 { bid, ask, bsz, asz })]
                }
                _ => Vec::new(),
            }
        }
        "arcus_trades" => d
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|t| {
                        Some(Record {
                            rx_ms,
                            payload: Payload::OwnTrade {
                                taker_buy: t["side"].as_str()? == "BUY",
                                px: f(&t["price"])?,
                                qty: f(&t["size"])?,
                                id: t["tradeId"].to_string(),
                            },
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        "arcus_l2Orderbook" => {
            let (bids, asks) = (levels(&d["bids"]), levels(&d["asks"]));
            if bids.is_empty() || asks.is_empty() {
                return Vec::new();
            }
            vec![rec(Payload::OwnL2 { bids, asks })]
        }
        "bn" => match (f(&d["b"]), f(&d["a"])) {
            (Some(bid), Some(ask)) => vec![rec(Payload::BnL1 {
                bid,
                ask,
                bsz: f(&d["B"]).unwrap_or(0.0),
                asz: f(&d["A"]).unwrap_or(0.0),
            })],
            _ => Vec::new(),
        },
        "bn_agg" => match (f(&d["p"]), f(&d["q"])) {
            (Some(px), Some(qty)) => vec![rec(Payload::BnTrade {
                maker_is_buyer: d["m"].as_bool().unwrap_or(false),
                px,
                qty,
            })],
            _ => Vec::new(),
        },
        "hl" => {
            let bbo = d["bbo"].as_array();
            match bbo.and_then(|b| Some((f(&b.first()?["px"])?, f(&b.get(1)?["px"])?))) {
                Some((bid, ask)) => vec![rec(Payload::HlL1 { bid, ask })],
                None => Vec::new(),
            }
        }
        "hl_trades" => d
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter(|t| t["coin"].as_str() == Some("BTC"))
                    .filter_map(|t| {
                        Some(Record {
                            rx_ms,
                            payload: Payload::HlTrade {
                                buy: t["side"].as_str()? == "B",
                                px: f(&t["px"])?,
                                qty: f(&t["sz"])?,
                                tid: t["tid"].to_string(),
                            },
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// A whole tape (or slice) in rx order, trades de-duplicated by id (first
/// occurrence wins, as `rf_common.load_window`).
pub fn parse_jsonl(text: &str) -> Result<Vec<Record>, String> {
    let mut all: Vec<Record> = text.lines().flat_map(parse_line).collect();
    // Sort first (stable), then keep the earliest occurrence of a trade id.
    all.sort_by_key(|r| r.rx_ms);
    let mut seen_own: HashSet<String> = HashSet::new();
    let mut seen_hl: HashSet<String> = HashSet::new();
    let out: Vec<Record> = all
        .into_iter()
        .filter(|r| match &r.payload {
            Payload::OwnTrade { id, .. } => seen_own.insert(id.clone()),
            Payload::HlTrade { tid, .. } => seen_hl.insert(tid.clone()),
            _ => true,
        })
        .collect();
    if out.is_empty() {
        return Err("no records".to_string());
    }
    Ok(out)
}

/// Push one record into the engine (events are not features).
pub fn apply(engine: &mut FeatureEngine, r: &Record) {
    match &r.payload {
        Payload::OwnL1 { bid, ask, bsz, asz } => engine.on_own_l1(r.rx_ms, *bid, *ask, *bsz, *asz),
        Payload::OwnL2 { bids, asks } => engine.on_own_l2(r.rx_ms, bids, asks),
        Payload::OwnTrade {
            taker_buy, px, qty, ..
        } => engine.on_own_trade(r.rx_ms, *taker_buy, *px, *qty),
        Payload::BnL1 { bid, ask, bsz, asz } => engine.on_bn_l1(r.rx_ms, *bid, *ask, *bsz, *asz),
        Payload::BnTrade {
            maker_is_buyer,
            px,
            qty,
        } => engine.on_bn_trade(r.rx_ms, *maker_is_buyer, *px, *qty),
        Payload::HlL1 { bid, ask } => engine.on_hl_l1(r.rx_ms, *bid, *ask),
        Payload::HlTrade { buy, px, qty, .. } => engine.on_hl_trade(r.rx_ms, *buy, *px, *qty),
        Payload::Event { .. } => {}
    }
}

/// Test helper: the same record with absurd values (prices doubled, sizes
/// ×1000, sides flipped) so any leak into a feature is visible.
pub fn make_absurd(r: &Record) -> Record {
    let payload = match &r.payload {
        Payload::OwnL1 { bid, ask, bsz, asz } => Payload::OwnL1 {
            bid: bid * 2.0,
            ask: ask * 2.0,
            bsz: bsz * 1e3,
            asz: asz * 1e-3,
        },
        Payload::OwnL2 { bids, asks } => Payload::OwnL2 {
            bids: bids.iter().map(|(p, q)| (p * 2.0, q * 1e3)).collect(),
            asks: asks.iter().map(|(p, q)| (p * 2.0, q * 1e-3)).collect(),
        },
        Payload::OwnTrade {
            taker_buy,
            px,
            qty,
            id,
        } => Payload::OwnTrade {
            taker_buy: !taker_buy,
            px: px * 2.0,
            qty: qty * 1e3,
            id: format!("absurd-{id}"),
        },
        Payload::BnL1 { bid, ask, bsz, asz } => Payload::BnL1 {
            bid: bid * 2.0,
            ask: ask * 2.0,
            bsz: bsz * 1e3,
            asz: asz * 1e-3,
        },
        Payload::BnTrade {
            maker_is_buyer,
            px,
            qty,
        } => Payload::BnTrade {
            maker_is_buyer: !maker_is_buyer,
            px: px * 2.0,
            qty: qty * 1e3,
        },
        Payload::HlL1 { bid, ask } => Payload::HlL1 {
            bid: bid * 2.0,
            ask: ask * 2.0,
        },
        Payload::HlTrade { buy, px, qty, tid } => Payload::HlTrade {
            buy: !buy,
            px: px * 2.0,
            qty: qty * 1e3,
            tid: format!("absurd-{tid}"),
        },
        Payload::Event { src, gap } => Payload::Event {
            src: src.clone(),
            gap: !gap,
        },
    };
    Record {
        rx_ms: r.rx_ms,
        payload,
    }
}

/// Test helper: one absurd record of every stream stamped exactly `rx_ms`.
pub fn absurd_records(rx_ms: u64) -> Vec<Record> {
    let rec = |payload| Record { rx_ms, payload };
    vec![
        rec(Payload::OwnL1 {
            bid: 1.0,
            ask: 1e6,
            bsz: 1e9,
            asz: 1.0,
        }),
        rec(Payload::OwnL2 {
            bids: vec![(1.0, 1e9)],
            asks: vec![(1e6, 1.0)],
        }),
        rec(Payload::OwnTrade {
            taker_buy: true,
            px: 1e6,
            qty: 1e6,
            id: "absurd-own".into(),
        }),
        rec(Payload::BnL1 {
            bid: 1.0,
            ask: 1e6,
            bsz: 1e9,
            asz: 1.0,
        }),
        rec(Payload::BnTrade {
            maker_is_buyer: false,
            px: 1e6,
            qty: 1e6,
        }),
        rec(Payload::HlL1 { bid: 1.0, ask: 1e6 }),
        rec(Payload::HlTrade {
            buy: true,
            px: 1e6,
            qty: 1e6,
            tid: "absurd-hl".into(),
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_stream_of_the_study_tape_format() {
        let lines = [
            r#"{"rx":1791010405.1029,"src":"bn","d":{"b":"84550.60","a":"84550.70","B":"1.582","A":"29.141","T":1791010405050,"E":1791010405050}}"#,
            r#"{"rx":1791010405.1346595,"src":"bn_agg","d":{"p":"84550.60","q":"1.739","m":true,"T":1791010404994,"E":1791010405079,"a":3474160728}}"#,
            r#"{"rx":1791010411.2254136,"src":"hl","d":{"coin":"BTC","time":1791010410999,"bbo":[{"px":"84556.0","sz":"0.07466","n":2},{"px":"84557.0","sz":"34.51455","n":111}]}}"#,
            r#"{"rx":1790975152.8690252,"src":"hl_trades","d":[{"coin":"BTC","side":"B","px":"84478.0","sz":"0.02379","time":1790975120522,"tid":1048782882829072},{"coin":"ETH","side":"A","px":"1.0","sz":"1","time":1,"tid":2}]}"#,
            r#"{"rx":1790975153.4178355,"src":"arcus_bbo","d":{"bestBid":{"price":"84452","size":"0.10151566"},"bestAsk":{"price":"84452.1","size":"2.85250832"},"timestamp":1790975153182813}}"#,
            r#"{"rx":1790975153.4173594,"src":"arcus_trades","d":{}}"#,
            r#"{"rx":1790975153.5,"src":"arcus_trades","d":[{"side":"SELL","price":"84452","size":"0.5","tradeId":"7","timestamp":1}]}"#,
            r#"{"rx":1790975153.4179509,"src":"arcus_l2Orderbook","d":{"bids":[["84452","0.10151566"],["84451.8","0.00023683"]],"asks":[["84452.1","2.85250832"]]}}"#,
            r#"{"rx":1790975153.0237067,"src":"bn","d":{"event":"connected"}}"#,
            r#"{"rx":1790975152.1562696,"src":"meta","d":{"event":"start","pid":1}}"#,
            "not json",
        ];
        let recs: Vec<Record> = lines.iter().flat_map(|l| parse_line(l)).collect();
        assert_eq!(recs.len(), 8, "{recs:?}");
        assert_eq!(recs[0].rx_ms, 1_791_010_405_103);
        assert!(
            matches!(recs[0].payload, Payload::BnL1 { bid, bsz, .. } if bid == 84550.6 && bsz == 1.582)
        );
        assert!(
            matches!(recs[1].payload, Payload::BnTrade { maker_is_buyer: true, qty, .. } if qty == 1.739)
        );
        assert!(
            matches!(recs[2].payload, Payload::HlL1 { bid, ask } if bid == 84556.0 && ask == 84557.0)
        );
        assert!(
            matches!(&recs[3].payload, Payload::HlTrade { buy: true, .. }),
            "ETH row dropped"
        );
        assert!(matches!(recs[4].payload, Payload::OwnL1 { asz, .. } if asz == 2.85250832));
        assert!(
            matches!(&recs[5].payload, Payload::OwnTrade { taker_buy: false, qty, .. } if *qty == 0.5)
        );
        assert!(
            matches!(&recs[6].payload, Payload::OwnL2 { bids, asks } if bids.len() == 2 && asks.len() == 1)
        );
        assert!(matches!(&recs[7].payload, Payload::Event { src, gap: false } if src == "bn"));
    }

    #[test]
    fn duplicate_trades_are_dropped_and_records_sorted() {
        let text = r#"{"rx":2.0,"src":"arcus_trades","d":[{"side":"BUY","price":"1","size":"1","tradeId":"a","timestamp":1}]}
{"rx":1.0,"src":"arcus_trades","d":[{"side":"BUY","price":"1","size":"1","tradeId":"a","timestamp":1},{"side":"BUY","price":"1","size":"1","tradeId":"b","timestamp":1}]}"#;
        let recs = parse_jsonl(text).unwrap();
        assert_eq!(recs.len(), 2, "{recs:?}");
        // The earliest occurrence (by rx, not file order) of id "a" is kept.
        assert!(recs.iter().all(|r| r.rx_ms == 1_000));
        assert!(parse_jsonl("").is_err());
    }
}
