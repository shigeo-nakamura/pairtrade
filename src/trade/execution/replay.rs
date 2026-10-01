//! Replay recorded Lighter tape through the paper venue (bot-strategy#1099
//! Step 2d): compare `MakerFirst` with a taker IOC for the same intent.
//!
//! The tape is the #1082 collector's row format (`b` L1, `d` top-5 depth,
//! `t` prints, `c` connection events). Events are ordered as G1 §3.1 says:
//! by exchange time; on a tie, prints first, then book, then validity. A
//! `d` row carries no exchange time, so it is attached at the time of the
//! last `b` row of its market. Backlog prints (`bl:1`) never fill. A
//! connection `down` makes its markets' books invalid until the next `b` row
//! with `snap:1`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use serde_json::Value;

use super::intent::{ExecIntent, ExecOutcome, Side};
use super::maker_first::{MakerFirstExecutor, MakerFirstTiming, VenueFill};
use super::paper_venue::{Level, PaperBook, PaperClock, PaperParams, PaperVenue};

#[derive(Debug, Clone, PartialEq)]
pub enum TapeEv {
    Book { bids: Vec<Level>, asks: Vec<Level> },
    Trade { price: f64, size: f64, taker: Side },
    Valid(bool),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimedEv {
    pub ms: u64,
    pub ev: TapeEv,
}

fn tie_rank(ev: &TapeEv) -> u8 {
    match ev {
        TapeEv::Trade { .. } => 0,
        TapeEv::Book { .. } => 1,
        TapeEv::Valid(_) => 2,
    }
}

fn level_pairs(v: &Value) -> Vec<Level> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|p| {
                    Some(Level {
                        price: p.get(0)?.as_f64()?,
                        size: p.get(1)?.as_f64()?,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Merge an L1 side with the latest depth for that side: the L1 level, then
/// the depth levels strictly worse than it.
fn merge(l1: Level, depth: &[Level], bid: bool) -> Vec<Level> {
    let mut out = vec![l1];
    out.extend(depth.iter().copied().filter(|l| {
        if bid {
            l.price < l1.price
        } else {
            l.price > l1.price
        }
    }));
    out
}

/// The ordered events of one market from tape lines. A line that is not
/// valid JSON is an error (Codex on #377): corrupted input must never pass
/// for an absent event. Callers drop a still-being-written trailing line
/// before this.
pub fn parse_market(lines: impl Iterator<Item = String>, market: i64) -> Result<Vec<TimedEv>> {
    let mut out: Vec<(u64, u8, usize, TapeEv)> = Vec::new();
    let mut l1: Option<(Level, Level)> = None;
    let mut depth: (Vec<Level>, Vec<Level>) = (Vec::new(), Vec::new());
    let mut last_b_ms: Option<u64> = None;
    let mut seq = 0usize;
    let mut push = |out: &mut Vec<(u64, u8, usize, TapeEv)>, ms: u64, ev: TapeEv| {
        seq += 1;
        out.push((ms, tie_rank(&ev), seq, ev));
    };
    for (n, line) in lines.enumerate() {
        let r = serde_json::from_str::<Value>(&line).map_err(|e| {
            anyhow::anyhow!("malformed tape row {} for market {market}: {e}", n + 1)
        })?;
        let k = r.get("k").and_then(Value::as_str).unwrap_or("");
        if k == "c" {
            let down = r.get("ev").and_then(Value::as_str) == Some("down");
            let ours = r
                .get("ms")
                .and_then(Value::as_array)
                .is_some_and(|ms| ms.iter().any(|m| m.as_i64() == Some(market)));
            if down && ours {
                if let Some(t) = r.get("t").and_then(Value::as_u64) {
                    push(&mut out, t, TapeEv::Valid(false));
                }
            }
            continue;
        }
        if r.get("m").and_then(Value::as_i64) != Some(market) {
            continue;
        }
        match k {
            "b" => {
                let (Some(x), Some(bb), Some(bq), Some(ba), Some(aq)) = (
                    r.get("x").and_then(Value::as_u64),
                    r.get("bb").and_then(Value::as_f64),
                    r.get("bq").and_then(Value::as_f64),
                    r.get("ba").and_then(Value::as_f64),
                    r.get("aq").and_then(Value::as_f64),
                ) else {
                    continue;
                };
                let (bid, ask) = (
                    Level {
                        price: bb,
                        size: bq,
                    },
                    Level {
                        price: ba,
                        size: aq,
                    },
                );
                l1 = Some((bid, ask));
                last_b_ms = Some(x);
                if r.get("snap").and_then(Value::as_i64) == Some(1) {
                    push(&mut out, x, TapeEv::Valid(true));
                }
                push(
                    &mut out,
                    x,
                    TapeEv::Book {
                        bids: merge(bid, &depth.0, true),
                        asks: merge(ask, &depth.1, false),
                    },
                );
            }
            "d" => {
                depth = (level_pairs(&r["b"]), level_pairs(&r["a"]));
                if let (Some((bid, ask)), Some(ms)) = (l1, last_b_ms) {
                    push(
                        &mut out,
                        ms,
                        TapeEv::Book {
                            bids: merge(bid, &depth.0, true),
                            asks: merge(ask, &depth.1, false),
                        },
                    );
                }
            }
            "t" => {
                if r.get("bl").and_then(Value::as_i64) == Some(1) {
                    continue; // backlog prints never fill
                }
                let (Some(x), Some(p), Some(q), Some(mka)) = (
                    r.get("x").and_then(Value::as_u64),
                    r.get("p").and_then(Value::as_f64),
                    r.get("q").and_then(Value::as_f64),
                    r.get("mka").and_then(Value::as_bool),
                ) else {
                    continue;
                };
                // The maker was the ask → the aggressor bought.
                let taker = if mka { Side::Buy } else { Side::Sell };
                push(
                    &mut out,
                    x,
                    TapeEv::Trade {
                        price: p,
                        size: q,
                        taker,
                    },
                );
            }
            _ => {}
        }
    }
    out.sort_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
    Ok(out
        .into_iter()
        .map(|(ms, _, _, ev)| TimedEv { ms, ev })
        .collect())
}

fn apply(book: &mut PaperBook, e: &TimedEv) {
    match &e.ev {
        TapeEv::Book { bids, asks } => book.on_book(e.ms, bids.clone(), asks.clone()),
        TapeEv::Trade { price, size, taker } => book.on_trade(e.ms, *price, *size, *taker),
        TapeEv::Valid(v) => book.set_valid(e.ms, *v),
    }
}

/// A book warmed with every event before `decision_ms`. A reduce-only
/// intent starts from the opposite position of its size, so it has
/// something to reduce (Codex on #377); otherwise the book starts flat.
fn warm_book(
    events: &[TimedEv],
    intent: &ExecIntent,
    decision_ms: u64,
    params: PaperParams,
) -> (PaperBook, usize) {
    let mut book = PaperBook::new(params);
    if intent.reduce_only {
        book.set_position(match intent.side {
            Side::Buy => -intent.qty,
            Side::Sell => intent.qty,
        });
    }
    let mut i = 0;
    while i < events.len() && events[i].ms < decision_ms {
        apply(&mut book, &events[i]);
        i += 1;
    }
    (book, i)
}

/// Taker baseline: one IOC at `reference · (1 ± max_slip)` sent at the
/// decision; the fills it gets from the replayed book.
pub fn replay_taker(
    events: &[TimedEv],
    intent: &ExecIntent,
    decision_ms: u64,
    params: PaperParams,
) -> Vec<VenueFill> {
    let (mut book, mut i) = warm_book(events, intent, decision_ms, params);
    let b = intent.max_slip_bps.unwrap_or(f64::INFINITY) / 1e4;
    let limit = match intent.side {
        Side::Buy => intent.reference_price * (1.0 + b),
        Side::Sell => intent.reference_price * (1.0 - b).max(0.0),
    };
    let id = book.place_ioc(
        decision_ms,
        intent.side,
        intent.qty,
        limit,
        intent.reduce_only,
    );
    let until = decision_ms + params.d_ms + params.taker_ms;
    while i < events.len() && events[i].ms <= until {
        apply(&mut book, &events[i]);
        i += 1;
    }
    book.flush(until);
    book.fills()
        .into_iter()
        .filter(|f| f.order_id == id)
        .collect()
}

/// `MakerFirst` on the replayed book, in virtual time from the decision.
/// Must run on a current-thread tokio runtime with paused time.
pub async fn replay_maker_first(
    events: &[TimedEv],
    intent: &ExecIntent,
    decision_ms: u64,
    params: PaperParams,
    timing: MakerFirstTiming,
    horizon_ms: u64,
) -> Result<ExecOutcome> {
    let (book, i) = warm_book(events, intent, decision_ms, params);
    let book = Arc::new(Mutex::new(book));
    let t0 = tokio::time::Instant::now();
    let clock: PaperClock = Arc::new(move || decision_ms + t0.elapsed().as_millis() as u64);
    let venue = PaperVenue {
        book: book.clone(),
        clock,
    };
    let rest: Vec<TimedEv> = events[i..]
        .iter()
        .take_while(|e| e.ms <= decision_ms + horizon_ms)
        .cloned()
        .collect();
    let feeder = {
        let book = book.clone();
        tokio::spawn(async move {
            let mut idx = 0;
            while idx < rest.len() {
                let at = rest[idx].ms;
                // Advance scheduled effects (activations, cancels, IOCs) in
                // 10 ms steps while waiting for the next tape event, so a
                // long gap in the tape never stalls them (Codex on #377).
                loop {
                    let now = decision_ms + t0.elapsed().as_millis() as u64;
                    if now >= at {
                        break;
                    }
                    book.lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .flush(now.saturating_sub(1));
                    let step = (at - now).min(10);
                    tokio::time::sleep(Duration::from_millis(step)).await;
                }
                let mut b = book.lock().unwrap_or_else(|p| p.into_inner());
                while idx < rest.len() && rest[idx].ms == at {
                    apply(&mut b, &rest[idx]);
                    idx += 1;
                }
                b.flush(at);
            }
            // Keep scheduled effects (activations, cancels, IOCs) moving.
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let now = decision_ms + t0.elapsed().as_millis() as u64;
                book.lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .flush(now.saturating_sub(1));
            }
        })
    };
    let out = MakerFirstExecutor {
        venue: &venue,
        timing,
    }
    .execute(intent)
    .await;
    feeder.abort();
    out
}

/// Signed cost in bps of `fills` vs `reference` for `side` (positive =
/// paid), fees included. `None` when nothing filled or a fee is unknown.
pub fn cost_bps(side: Side, reference: f64, fills: &[(f64, f64, Option<f64>)]) -> Option<f64> {
    let qty: f64 = fills.iter().map(|f| f.1).sum();
    if qty <= 0.0 || reference <= 0.0 {
        return None;
    }
    let dir = match side {
        Side::Buy => 1.0,
        Side::Sell => -1.0,
    };
    let mut usd = 0.0;
    for (price, q, fee) in fills {
        usd += dir * (price - reference) * q + (*fee)?;
    }
    Some(usd / (reference * qty) * 1e4)
}

/// Market id → symbol from a #1082-style `universe.json`.
pub fn universe_ids(universe: &Value) -> HashMap<String, i64> {
    universe
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|m| m.get("collect").and_then(Value::as_bool) == Some(true))
                .filter_map(|m| {
                    Some((
                        m.get("symbol")?.as_str()?.to_string(),
                        m.get("market_id")?.as_i64()?,
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trade::execution::intent::{ExecStyle, MakerFirstParams, Role};

    fn lines(rows: &[&str]) -> impl Iterator<Item = String> {
        rows.iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn parse_orders_by_exchange_time_skips_backlog_and_other_markets() {
        let ev = parse_market(
            lines(&[
                r#"{"k":"b","m":7,"t":100,"x":50,"bb":9.9,"bq":5,"ba":10.1,"aq":4,"snap":1}"#,
                r#"{"k":"d","m":7,"t":100,"b":[[9.9,5],[9.8,7]],"a":[[10.1,4],[10.2,3]]}"#,
                r#"{"k":"t","m":7,"t":101,"x":40,"id":1,"p":10.0,"q":1,"mka":true,"ty":"trade","bl":1}"#,
                r#"{"k":"t","m":7,"t":102,"x":60,"id":2,"p":9.9,"q":2,"mka":false,"ty":"trade"}"#,
                r#"{"k":"b","m":8,"t":103,"x":55,"bb":1,"bq":1,"ba":2,"aq":1}"#,
                r#"{"k":"t","m":7,"t":104,"x":50,"id":3,"p":10.1,"q":1,"mka":true,"ty":"liquidation"}"#,
                r#"{"k":"c","c":0,"t":70,"ev":"down","ms":[7,9]}"#,
            ]),
            7,
        ).unwrap();
        let kinds: Vec<(u64, &str)> = ev
            .iter()
            .map(|e| {
                (
                    e.ms,
                    match &e.ev {
                        TapeEv::Book { .. } => "book",
                        TapeEv::Trade { .. } => "trade",
                        TapeEv::Valid(true) => "valid",
                        TapeEv::Valid(false) => "invalid",
                    },
                )
            })
            .collect();
        // At ms 50 the print comes first (tie rule), then the two book events
        // (b, then d attached at the b's time), then validity.
        assert_eq!(
            kinds,
            vec![
                (50, "trade"),
                (50, "book"),
                (50, "book"),
                (50, "valid"),
                (60, "trade"),
                (70, "invalid")
            ]
        );
        // mka=true → the aggressor bought; mka=false → sold.
        assert!(matches!(
            ev[0].ev,
            TapeEv::Trade {
                taker: Side::Buy,
                ..
            }
        ));
        assert!(matches!(
            ev[4].ev,
            TapeEv::Trade {
                taker: Side::Sell,
                ..
            }
        ));
        // The d row deepens the book.
        if let TapeEv::Book { bids, .. } = &ev[2].ev {
            assert_eq!(bids.len(), 2);
            assert_eq!(bids[1].price, 9.8);
        } else {
            panic!();
        }
    }

    #[test]
    fn cost_bps_signs_and_unknown_fees() {
        let buy = cost_bps(Side::Buy, 100.0, &[(100.1, 1.0, Some(0.0))]).unwrap();
        assert!((buy - 10.0).abs() < 1e-9);
        let sell_maker = cost_bps(Side::Sell, 100.0, &[(100.1, 1.0, Some(0.0))]).unwrap();
        assert!(
            (sell_maker + 10.0).abs() < 1e-9,
            "a maker sell above the mid earns"
        );
        assert_eq!(cost_bps(Side::Buy, 100.0, &[(100.1, 1.0, None)]), None);
        assert_eq!(cost_bps(Side::Buy, 100.0, &[]), None);
    }

    fn tape() -> Vec<TimedEv> {
        parse_market(
            lines(&[
                r#"{"k":"b","m":1,"t":0,"x":0,"bb":99.9,"bq":1,"ba":100.1,"aq":5,"snap":1}"#,
                // A seller hits 99.9 for 3 at +2 s (our bid joins behind 1).
                r#"{"k":"t","m":1,"t":2000,"x":2000,"id":1,"p":99.9,"q":3,"mka":false,"ty":"trade"}"#,
            ]),
            1,
        ).unwrap()
    }

    fn intent() -> ExecIntent {
        ExecIntent {
            symbol: "X".into(),
            side: Side::Buy,
            qty: 2.0,
            reference_price: 100.0,
            reduce_only: false,
            deadline_ms: None,
            max_slip_bps: Some(50.0),
            style: ExecStyle::MakerFirst(MakerFirstParams {
                maker_window_ms: 5_000,
                requote_bps: 5.0,
            }),
        }
    }

    #[test]
    fn the_taker_baseline_pays_the_half_spread() {
        let f = replay_taker(&tape(), &intent(), 1_000, PaperParams::lighter_standard());
        let c = cost_bps(
            Side::Buy,
            100.0,
            &f.iter()
                .map(|f| (f.price, f.qty, f.fee_usd))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!((c - 10.0).abs() < 1e-9, "{c}");
    }

    #[tokio::test(start_paused = true)]
    async fn maker_first_on_the_replayed_tape_fills_at_the_bid() {
        let o = replay_maker_first(
            &tape(),
            &intent(),
            1_000,
            PaperParams::lighter_standard(),
            MakerFirstTiming {
                poll: Duration::from_millis(100),
                cancel_confirm_polls: 30,
                settle_polls: 30,
                ioc_fill_polls: 30,
            },
            20_000,
        )
        .await
        .unwrap();
        assert!(o.unresolved.is_empty(), "{:?}", o.unresolved);
        assert!((o.filled_qty - 2.0).abs() < 1e-12, "{}", o.filled_qty);
        assert!(o
            .fills
            .iter()
            .all(|f| f.role == Role::Maker && f.price == 99.9));
        let c = cost_bps(
            Side::Buy,
            100.0,
            &o.fills
                .iter()
                .map(|f| (f.price, f.qty, f.fee_usd))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!((c + 10.0).abs() < 1e-9, "maker earns the half-spread: {c}");
    }

    /// Codex on #377: a reduce-only close replays from the opposite
    /// position, so the taker baseline (and maker-first) can fill it.
    #[test]
    fn a_reduce_only_intent_replays_from_the_position_it_closes() {
        let mut i = intent();
        i.side = Side::Sell;
        i.reduce_only = true;
        let f = replay_taker(&tape(), &i, 1_000, PaperParams::lighter_standard());
        let q: f64 = f.iter().map(|f| f.qty).sum();
        // The bid shows 1.0, so the IOC closes 1.0 (it would be 0 from flat).
        assert!((q - 1.0).abs() < 1e-12, "{q}");
    }

    /// Codex on #377: with the next tape event far away, scheduled effects
    /// (the cancel at the window end, the taker IOC) must still apply.
    #[tokio::test(start_paused = true)]
    async fn scheduled_effects_advance_across_a_tape_gap() {
        let tape = parse_market(
            lines(&[
                r#"{"k":"b","m":1,"t":0,"x":0,"bb":99.9,"bq":1,"ba":100.1,"aq":5,"snap":1}"#,
                r#"{"k":"t","m":1,"t":120000,"x":120000,"id":9,"p":100.0,"q":1,"mka":true,"ty":"trade"}"#,
            ]),
            1,
        ).unwrap();
        let mut i = intent();
        i.style = ExecStyle::MakerFirst(MakerFirstParams {
            maker_window_ms: 2_000,
            requote_bps: 5.0,
        });
        let o = replay_maker_first(
            &tape,
            &i,
            1_000,
            PaperParams::lighter_standard(),
            MakerFirstTiming {
                poll: Duration::from_millis(100),
                cancel_confirm_polls: 10,
                settle_polls: 10,
                ioc_fill_polls: 10,
            },
            200_000,
        )
        .await
        .unwrap();
        assert!(o.unresolved.is_empty(), "{:?}", o.unresolved);
        assert!(
            (o.filled_qty - 2.0).abs() < 1e-12,
            "the taker remainder filled at the ask"
        );
        assert!(o.fills.iter().all(|f| f.role == Role::Taker));
    }

    #[test]
    fn a_malformed_row_is_an_error_not_a_missing_event() {
        let r = parse_market(
            lines(&[
                r#"{"k":"b","m":1,"t":0,"x":0,"bb":99.9,"bq":1,"ba":100.1,"aq":5,"snap":1}"#,
                r#"{"k":"t","m":1,"t":5,"x":5,"id":1,"p":99"#,
            ]),
            1,
        );
        assert!(r.is_err());
    }
}
