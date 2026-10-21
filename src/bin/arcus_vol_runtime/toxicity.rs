//! Shadow toxicity (adverse-selection) feature log for the Arcus volume bot
//! (bot-strategy#1093, step 2 of the 2026-10-21 plan; pre-registration in
//! `deploy/arcus_vol/TOXICITY_PREREG.md`, feature mapping to
//! bot-strategy#1190 in `docs/arcus-vol-toxicity-log.md`).
//!
//! LOGGING ONLY. Nothing here is read back by the tick: no quote is added,
//! removed, moved or resized because of this module, in any mode. It
//! records, at every quote placement / re-peg and at every fill, the state
//! of the own book, the own trade flow, the own mid returns, the time to the
//! session boundaries and a reference price (Lighter Core, the same symbol,
//! public `order_book` WebSocket — never REST-polled) so a gate can later be
//! fitted OFFLINE against the fills' markouts. Fill rows carry the fill id,
//! so they join `fills.jsonl` (`markout` rows, 5 / 30 / 60 s on the own
//! mid); on top of that this module writes its own `markout_tox` rows at
//! 1 / 10 / 60 / 300 s against BOTH the own mid (`markout_mid_bps`) and the
//! reference mid (`markout_ref_bps`), the horizons of the #1082 CUTOFF §4.
//!
//! Every row carries `t` (host receive / decision time, ms) and `x` (the
//! venue's own time of the thing the row is about, ms, when the venue gave
//! one). Reference rows inside a feature row carry `ref_t` / `ref_x` the
//! same way.
//!
//! Rows go to `toxicity_YYYYMMDD.jsonl` in the state dir (UTC day of the
//! row, no fsync: a lost row is a lost sample, never a lost fill), like the
//! quote gate's `gate_YYYYMMDD.jsonl`.

use crate::logic::QSide;
use crate::offset_learner::window_at;
use crate::quote_gate::{GateLog, CANCEL_LATENCY_MS};
use crate::session::new_york_utc_offset_secs;
use futures::{SinkExt, StreamExt};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

/// Markout horizons (seconds) of the `markout_tox` rows: the #1082 CUTOFF
/// §4 set, kept apart from `ledger::MARKOUT_HORIZONS` (5 / 30 / 60 s), which
/// stays untouched.
pub const MARKOUT_HORIZONS_S: [u64; 4] = [1, 10, 60, 300];
/// A due horizon waits this long for a fresh own mid / reference mid, then
/// is recorded with that side `null`.
pub const MARKOUT_MAX_LATE_MS: u64 = 10_000;
/// A reference record older than this is not used for `ref_*` features
/// (the feed is up but the book is quiet / stale).
pub const REF_STALE_MS: u64 = 5_000;
/// Own-book / own-trade / own-mid ring retention.
const RETAIN_MS: u64 = 40_000;
/// Reference mid ring retention (the longest `ref_*` lookback is 5 s; the
/// markouts read the live top, not the ring).
const REF_RETAIN_MS: u64 = 15_000;
/// L2 imbalance depths.
const IMB_LEVELS: [usize; 2] = [5, 10];
/// Own-book ring retention: a fill booked after the fee wait (up to 30 s,
/// `FEE_WAIT_SECS`) is logged as of its first-seen time, so the book as of
/// then must still be here; 45 s covers it with slack (≈ 2 reads/s).
const BOOK_RETAIN_MS: u64 = 45_000;

pub const LIGHTER_MAINNET: &str = "https://mainnet.zklighter.elliot.ai";

// ---------------------------------------------------------------- settings

#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    pub on: bool,
    /// Lighter Core symbol of the reference book (`None` = no reference;
    /// every `ref_*` field is `null`).
    pub ref_symbol: Option<String>,
    /// Lighter REST base (`https://…`); the stream is `wss://…/stream`.
    pub lighter_base: String,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            on: false,
            ref_symbol: None,
            lighter_base: LIGHTER_MAINNET.to_string(),
        }
    }
}

/// The default reference symbol for an Arcus market: `SPY-USD` → `SPY`.
/// Whether Lighter lists it is only known at runtime (`orderBookDetails`);
/// an unlisted symbol leaves the reference `null` and says so in the log.
pub fn default_ref_symbol(market: &str) -> String {
    market
        .strip_suffix("-USD")
        .unwrap_or(market)
        .to_ascii_uppercase()
}

pub fn ws_url(base: &str) -> String {
    let b = base.trim_end_matches('/');
    let b = b
        .strip_prefix("https://")
        .map(|r| format!("wss://{r}"))
        .or_else(|| b.strip_prefix("http://").map(|r| format!("ws://{r}")))
        .unwrap_or_else(|| b.to_string());
    format!("{b}/stream")
}

/// The time a fill is logged at: the earliest of when the runtime first saw
/// it (`first_seen`, the fee-wait map) and the booking time `fill_ts`. A fill
/// whose fee arrives late is booked up to 30 s after it happened; its
/// features and markout clocks must not move with the booking.
pub fn fill_time(first_seen: Option<u64>, fill_ts: u64) -> u64 {
    first_seen.map_or(fill_ts, |f| f.min(fill_ts))
}

// ------------------------------------------------------------- boundaries

/// The session boundaries (New York clock) a quote may be exposed to:
/// weekday 09:30 cash open, 16:00 cash close, 20:00 overnight-session
/// start (Mon–Fri; Friday's is the weekend close), Sunday 18:00 (futures
/// reopen) and 20:00 (overnight start). Saturday has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Boundaries {
    pub prev_name: &'static str,
    pub prev_ms: u64,
    pub next_name: &'static str,
    pub next_ms: u64,
}

fn ny_day_start_utc_s(utc_s: i64) -> (i64, i64) {
    // (local day index, weekday 0=Mon..6=Sun) of the New York day holding utc_s
    let off = new_york_utc_offset_secs((utc_s.max(0) as u64) * 1_000);
    let local = utc_s + off;
    let day = local.div_euclid(86_400);
    let dow = (day + 3).rem_euclid(7);
    (day, dow)
}

/// UTC seconds of `sod` seconds into New York local `day`.
fn boundary_utc_s(day: i64, sod: i64) -> i64 {
    // Approximate the offset at that instant from local noon of the day.
    let approx = day * 86_400 + 12 * 3_600 + 5 * 3_600;
    let off = new_york_utc_offset_secs((approx.max(0) as u64) * 1_000);
    day * 86_400 + sod - off
}

pub fn boundaries(utc_ms: u64) -> Boundaries {
    let now_s = (utc_ms / 1_000) as i64;
    let (day0, _) = ny_day_start_utc_s(now_s);
    let mut prev: Option<(&'static str, i64)> = None;
    let mut next: Option<(&'static str, i64)> = None;
    for d in (day0 - 3)..=(day0 + 3) {
        let dow = (d + 3).rem_euclid(7); // 0 Mon … 6 Sun
        let cands: Vec<(&'static str, i64)> = match dow {
            0..=4 => vec![
                ("cash_open_0930", 9 * 3_600 + 1_800),
                ("cash_close_1600", 16 * 3_600),
                (
                    if dow == 4 {
                        "weekend_close_2000"
                    } else {
                        "overnight_start_2000"
                    },
                    20 * 3_600,
                ),
            ],
            6 => vec![
                ("sunday_reopen_1800", 18 * 3_600),
                ("overnight_start_2000", 20 * 3_600),
            ],
            _ => vec![],
        };
        for (name, sod) in cands {
            let b = boundary_utc_s(d, sod);
            if b <= now_s {
                if prev.is_none_or(|(_, p)| b > p) {
                    prev = Some((name, b));
                }
            } else if next.is_none_or(|(_, n)| b < n) {
                next = Some((name, b));
            }
        }
    }
    let (prev_name, prev_s) = prev.unwrap_or(("none", now_s));
    let (next_name, next_s) = next.unwrap_or(("none", now_s));
    Boundaries {
        prev_name,
        prev_ms: (prev_s.max(0) as u64) * 1_000,
        next_name,
        next_ms: (next_s.max(0) as u64) * 1_000,
    }
}

// --------------------------------------------------------------- reference

/// Shared state of the Lighter reference feed (written by the feed task,
/// read by the tick under the lock).
#[derive(Debug, Default)]
pub struct RefShared {
    pub symbol: String,
    pub market_id: Option<u32>,
    pub up: bool,
    pub last_err: Option<String>,
    /// Best bid / ask after the last applied frame.
    pub top: Option<(f64, f64)>,
    /// Host receive time of the last applied frame.
    pub last_rx_ms: u64,
    /// Venue time (`last_updated_at`, ms) of the last applied frame.
    pub last_x_ms: Option<u64>,
    /// `(rx_ms, mid)` ring, newest last.
    pub mids: VecDeque<(u64, f64)>,
    /// Outage epochs `(down_at_ms, up_again_at_ms)`; `None` = still down.
    pub outages: VecDeque<(u64, Option<u64>)>,
    pub frames: u64,
    pub reconnects: u64,
}

impl RefShared {
    pub fn new(symbol: &str) -> Self {
        RefShared {
            symbol: symbol.to_string(),
            ..Default::default()
        }
    }

    pub fn push_top(&mut self, rx_ms: u64, x_ms: Option<u64>, bid: f64, ask: f64) {
        self.top = Some((bid, ask));
        self.last_rx_ms = rx_ms;
        self.last_x_ms = x_ms;
        self.frames += 1;
        let mid = (bid + ask) / 2.0;
        self.mids.push_back((rx_ms, mid));
        while self
            .mids
            .front()
            .is_some_and(|(t, _)| rx_ms.saturating_sub(*t) > REF_RETAIN_MS)
        {
            if self.mids.len() <= 1 {
                break;
            }
            self.mids.pop_front();
        }
    }

    /// The feed went down at `now_ms` (socket lost, bad frame, unlisted …).
    /// Opens an outage epoch; `fresh_mid` / `up_at` consult the epochs so a
    /// row stamped at a historical `t` sees the feed's health AT `t`, not now.
    pub fn down(&mut self, now_ms: u64, why: &str) {
        if self.up {
            self.outages.push_back((now_ms, None));
            if self.outages.len() > 64 {
                self.outages.pop_front();
            }
        }
        self.up = false;
        self.last_err = Some(why.to_string());
        self.top = None;
    }

    /// The feed is up again at `now_ms` (first frame after an outage).
    pub fn up_again(&mut self, now_ms: u64) {
        if let Some((_, end)) = self.outages.back_mut() {
            if end.is_none() {
                *end = Some(now_ms);
            }
        }
        self.up = true;
        self.last_err = None;
    }

    /// Whether an outage epoch intersects `(from, to]`.
    pub fn outage_in(&self, from_ms: u64, to_ms: u64) -> bool {
        self.outages
            .iter()
            .any(|(start, end)| *start <= to_ms && end.is_none_or(|e| e > from_ms))
    }

    /// Whether the feed was up at `t`: at least one frame received at or
    /// before `t`, and `t` not inside an outage epoch (an open one counts).
    pub fn up_at(&self, t_ms: u64) -> bool {
        if self.asof(t_ms).is_none() {
            return false;
        }
        !self
            .outages
            .iter()
            .any(|(start, end)| *start <= t_ms && end.is_none_or(|e| t_ms < e))
    }

    /// Mid as of `t` (last record with `rx <= t`).
    pub fn mid_asof(&self, t_ms: u64) -> Option<f64> {
        self.mids
            .iter()
            .rev()
            .find(|(rx, _)| *rx <= t_ms)
            .map(|(_, m)| *m)
    }

    /// The last record received at or before `t` (as-of: a frame received
    /// after `t` is never used, so fill rows stamped before the latest frame
    /// stay look-ahead free).
    pub fn asof(&self, t_ms: u64) -> Option<(u64, f64)> {
        self.mids.iter().rev().find(|(rx, _)| *rx <= t_ms).copied()
    }

    /// Fresh mid at `t`: the as-of record, if the feed was up at `t` and
    /// that record is at most `REF_STALE_MS` old at `t`.
    pub fn fresh_mid(&self, t_ms: u64) -> Option<f64> {
        if !self.up_at(t_ms) {
            return None;
        }
        self.asof(t_ms)
            .filter(|(rx, _)| t_ms.saturating_sub(*rx) <= REF_STALE_MS)
            .map(|(_, m)| m)
    }

    /// `ref_ret_h` (bp, signed) and `ref_jump_h` (bp, max |move| over the
    /// window) against the mid as of `t − h`.
    pub fn ret_jump(&self, t_ms: u64, h_ms: u64) -> (Option<f64>, Option<f64>) {
        // No continuous reference over the window: a reconnect inside it would
        // turn the gap across the blind interval into a fake move.
        if self.outage_in(t_ms.saturating_sub(h_ms), t_ms) {
            return (None, None);
        }
        let Some(base) = self.mid_asof(t_ms.saturating_sub(h_ms)) else {
            return (None, None);
        };
        let Some(last) = self.mid_asof(t_ms) else {
            return (None, None);
        };
        if base <= 0.0 {
            return (None, None);
        }
        let bp = |m: f64| (m - base) / base * 1e4;
        let jump = self
            .mids
            .iter()
            .filter(|(rx, _)| *rx > t_ms.saturating_sub(h_ms) && *rx <= t_ms)
            .map(|(_, m)| bp(*m).abs())
            .fold(0.0_f64, f64::max);
        (Some(bp(last)), Some(jump))
    }
}

/// `(snapshot?, venue time ms, bids, asks)` of one parsed frame.
pub type Frame = (
    bool,
    Option<u64>,
    Vec<(Decimal, Decimal)>,
    Vec<(Decimal, Decimal)>,
);

/// A `subscribed/order_book` (snapshot) or `update/order_book` (delta)
/// frame of `market_id`. `Ok(None)` for anything else (acks, pongs, other
/// channels); `Err` for a frame of ours that does not parse.
pub fn parse_lighter_frame(text: &str, market_id: u32) -> Result<Option<Frame>, String> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return Ok(None);
    };
    let ty = v.get("type").and_then(Value::as_str).unwrap_or("");
    let snapshot = match ty {
        "subscribed/order_book" => true,
        "update/order_book" => false,
        _ => return Ok(None),
    };
    let channel = v.get("channel").and_then(Value::as_str).unwrap_or("");
    let id = channel
        .rsplit(['/', ':'])
        .next()
        .and_then(|s| s.parse::<u32>().ok());
    if id != Some(market_id) {
        return Ok(None);
    }
    let x_ms = v
        .get("last_updated_at")
        .and_then(Value::as_u64)
        .map(|us| us / 1_000);
    let ob = v.get("order_book").ok_or_else(|| truncate(text))?;
    let side = |k: &str| -> Result<Vec<(Decimal, Decimal)>, String> {
        let Some(arr) = ob.get(k).and_then(Value::as_array) else {
            return Ok(Vec::new());
        };
        arr.iter()
            .map(|e| {
                let p = e.get("price").and_then(Value::as_str).unwrap_or("");
                let s = e.get("size").and_then(Value::as_str).unwrap_or("");
                match (Decimal::from_str(p), Decimal::from_str(s)) {
                    (Ok(p), Ok(s)) => Ok((p, s)),
                    _ => Err(truncate(text)),
                }
            })
            .collect()
    };
    Ok(Some((snapshot, x_ms, side("bids")?, side("asks")?)))
}

fn truncate(s: &str) -> String {
    s.chars().take(200).collect()
}

/// A price-keyed book kept from one snapshot plus deltas (a level with
/// size 0 is removed). Best bid = highest key, best ask = lowest key.
#[derive(Debug, Default)]
pub struct RefBook {
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
}

impl RefBook {
    pub fn apply(
        &mut self,
        snapshot: bool,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
    ) {
        if snapshot {
            self.bids.clear();
            self.asks.clear();
        }
        for (p, s) in bids {
            if s.is_zero() {
                self.bids.remove(&p);
            } else {
                self.bids.insert(p, s);
            }
        }
        for (p, s) in asks {
            if s.is_zero() {
                self.asks.remove(&p);
            } else {
                self.asks.insert(p, s);
            }
        }
    }

    pub fn top(&self) -> Option<(f64, f64)> {
        let bid = self.bids.keys().next_back()?.to_f64()?;
        let ask = self.asks.keys().next()?.to_f64()?;
        (bid > 0.0 && ask > 0.0).then_some((bid, ask))
    }
}

/// `orderBookDetails` → market id of `symbol` (an active listing only).
pub fn market_id_of(body: &Value, symbol: &str) -> Option<u32> {
    body.get("order_book_details")?
        .as_array()?
        .iter()
        .find(|m| {
            m.get("symbol")
                .and_then(Value::as_str)
                .is_some_and(|s| s.eq_ignore_ascii_case(symbol))
        })
        .and_then(|m| m.get("market_id").and_then(Value::as_u64))
        .map(|id| id as u32)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Run forever: resolve the symbol once per connect (REST, backoff — a
/// single metadata read, not a poll), stream the public `order_book`
/// channel, keep the top of book in `shared`. A dead feed only ever leaves
/// `ref_*` fields `null`.
pub async fn run_lighter(base: String, symbol: String, shared: Arc<Mutex<RefShared>>) {
    let mut backoff = Duration::from_secs(2);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .ok();
    loop {
        let market_id = match &client {
            Some(c) => match c
                .get(format!(
                    "{}/api/v1/orderBookDetails",
                    base.trim_end_matches('/')
                ))
                .send()
                .await
            {
                Ok(r) => match r.json::<Value>().await {
                    Ok(body) => market_id_of(&body, &symbol),
                    Err(e) => {
                        shared
                            .lock()
                            .unwrap()
                            .down(now_ms(), &format!("orderBookDetails: {e}"));
                        None
                    }
                },
                Err(e) => {
                    shared
                        .lock()
                        .unwrap()
                        .down(now_ms(), &format!("orderBookDetails: {e}"));
                    None
                }
            },
            None => None,
        };
        let Some(market_id) = market_id else {
            {
                let mut s = shared.lock().unwrap();
                if s.last_err.is_none() {
                    log::warn!(
                        "[TOX] reference {symbol}: not listed on Lighter (orderBookDetails) — ref_* stays null"
                    );
                }
                s.down(now_ms(), "unlisted or metadata unavailable");
            }
            tokio::time::sleep(Duration::from_secs(3_600)).await;
            continue;
        };
        shared.lock().unwrap().market_id = Some(market_id);
        match dex_connector::ws_connect::connect_ws(ws_url(&base).as_str()).await {
            Ok(mut ws) => {
                let sub =
                    json!({"type": "subscribe", "channel": format!("order_book/{market_id}")});
                let mut book = RefBook::default();
                let mut last_ping = std::time::Instant::now();
                if ws.send(Message::Text(sub.to_string())).await.is_ok() {
                    loop {
                        if last_ping.elapsed() >= Duration::from_secs(25) {
                            last_ping = std::time::Instant::now();
                            if ws
                                .send(Message::Text(json!({"type": "ping"}).to_string()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        let next = tokio::time::timeout(Duration::from_secs(60), ws.next()).await;
                        let msg = match next {
                            Ok(Some(Ok(m))) => m,
                            Ok(Some(Err(e))) => {
                                shared.lock().unwrap().down(now_ms(), &format!("ws: {e}"));
                                break;
                            }
                            Ok(None) => {
                                shared.lock().unwrap().down(now_ms(), "ws closed");
                                break;
                            }
                            Err(_) => {
                                shared.lock().unwrap().down(now_ms(), "silent 60s");
                                break;
                            }
                        };
                        match msg {
                            Message::Text(text) => match parse_lighter_frame(&text, market_id) {
                                Ok(Some((snapshot, x_ms, bids, asks))) => {
                                    book.apply(snapshot, bids, asks);
                                    let rx = now_ms();
                                    let mut s = shared.lock().unwrap();
                                    if !s.up {
                                        s.up_again(rx);
                                        backoff = Duration::from_secs(2);
                                        log::info!(
                                            "[TOX] reference {symbol} (Lighter market {market_id}) up"
                                        );
                                    }
                                    if let Some((b, a)) = book.top() {
                                        s.push_top(rx, x_ms, b, a);
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    shared
                                        .lock()
                                        .unwrap()
                                        .down(now_ms(), &format!("bad frame: {e}"));
                                    break;
                                }
                            },
                            Message::Ping(p) => {
                                if ws.send(Message::Pong(p)).await.is_err() {
                                    break;
                                }
                            }
                            Message::Close(_) => {
                                shared.lock().unwrap().down(now_ms(), "ws close frame");
                                break;
                            }
                            _ => {}
                        }
                    }
                } else {
                    shared
                        .lock()
                        .unwrap()
                        .down(now_ms(), "subscribe send failed");
                }
            }
            Err(e) => {
                shared
                    .lock()
                    .unwrap()
                    .down(now_ms(), &format!("connect: {e}"));
            }
        }
        {
            let mut s = shared.lock().unwrap();
            s.reconnects += 1;
            if s.reconnects % 10 == 1 {
                log::warn!(
                    "[TOX] reference {symbol} feed down ({}); reconnect in {:?}",
                    s.last_err.as_deref().unwrap_or("?"),
                    backoff
                );
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

// ------------------------------------------------------------------ engine

#[derive(Debug, Clone)]
struct OwnBook {
    rx_ms: u64,
    x_ms: Option<u64>,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

/// Pure feature state: no IO, no clock (the caller stamps every record).
#[derive(Debug, Default)]
pub struct Engine {
    /// Own-book reads, oldest first (as-of lookups for fills stamped
    /// before the latest read).
    books: VecDeque<OwnBook>,
    /// `(rx_ms, mid)`.
    mids: VecDeque<(u64, f64)>,
    /// `(rx_ms, signed notional)`; taker buy positive.
    trades: VecDeque<(u64, f64)>,
}

fn imbalance(bids: &[(f64, f64)], asks: &[(f64, f64)], levels: usize) -> Option<f64> {
    let b: f64 = bids.iter().take(levels).map(|(_, s)| s).sum();
    let a: f64 = asks.iter().take(levels).map(|(_, s)| s).sum();
    let tot = b + a;
    (tot > 0.0).then(|| (b - a) / tot)
}

impl Engine {
    pub fn on_own_book(
        &mut self,
        rx_ms: u64,
        x_ms: Option<u64>,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
    ) {
        if let (Some((b, _)), Some((a, _))) = (bids.first(), asks.first()) {
            if *b > 0.0 && *a > 0.0 {
                self.mids.push_back((rx_ms, (b + a) / 2.0));
            }
        }
        self.books.push_back(OwnBook {
            rx_ms,
            x_ms,
            bids: bids.to_vec(),
            asks: asks.to_vec(),
        });
        self.prune(rx_ms);
    }

    pub fn on_own_trade(&mut self, rx_ms: u64, taker_buy: bool, px: f64, qty: f64) {
        let n = px * qty;
        self.trades
            .push_back((rx_ms, if taker_buy { n } else { -n }));
        self.prune(rx_ms);
    }

    fn prune(&mut self, now_ms: u64) {
        while self
            .mids
            .front()
            .is_some_and(|(t, _)| now_ms.saturating_sub(*t) > RETAIN_MS)
        {
            if self.mids.len() <= 1 {
                break;
            }
            self.mids.pop_front();
        }
        while self
            .trades
            .front()
            .is_some_and(|(t, _)| now_ms.saturating_sub(*t) > RETAIN_MS)
        {
            self.trades.pop_front();
        }
        while self
            .books
            .front()
            .is_some_and(|b| now_ms.saturating_sub(b.rx_ms) > BOOK_RETAIN_MS)
        {
            if self.books.len() <= 1 {
                break;
            }
            self.books.pop_front();
        }
    }

    /// The last own-book read at or before `t` (never one after it).
    fn book_asof(&self, t_ms: u64) -> Option<&OwnBook> {
        self.books.iter().rev().find(|b| b.rx_ms <= t_ms)
    }

    pub fn mid_asof(&self, t_ms: u64) -> Option<f64> {
        self.mids
            .iter()
            .rev()
            .find(|(rx, _)| *rx <= t_ms)
            .map(|(_, m)| *m)
    }

    /// Own-mid return (bp) from `t − h` to `t`.
    pub fn ret_bp(&self, t_ms: u64, h_ms: u64) -> Option<f64> {
        let base = self.mid_asof(t_ms.saturating_sub(h_ms))?;
        let last = self.mid_asof(t_ms)?;
        (base > 0.0).then(|| (last - base) / base * 1e4)
    }

    /// Signed taker flow (notional) and trade count in `(t − h, t]`.
    pub fn flow(&self, t_ms: u64, h_ms: u64) -> (f64, usize) {
        let mut sum = 0.0;
        let mut n = 0;
        for (rx, v) in &self.trades {
            if *rx > t_ms.saturating_sub(h_ms) && *rx <= t_ms {
                sum += v;
                n += 1;
            }
        }
        (sum, n)
    }

    /// Own-book fields at `t` (the last book read at or before `t`; a book
    /// read after `t` is not used — as-of, never look-ahead).
    fn book_fields(&self, t_ms: u64, row: &mut serde_json::Map<String, Value>) {
        let Some(b) = self.book_asof(t_ms) else {
            return;
        };
        let (Some((bid, bsz)), Some((ask, asz))) = (b.bids.first(), b.asks.first()) else {
            return;
        };
        let mid = (bid + ask) / 2.0;
        row.insert("arc_bid".into(), json!(bid));
        row.insert("arc_ask".into(), json!(ask));
        row.insert("arc_mid".into(), json!(mid));
        row.insert("arc_bid_sz1".into(), json!(bsz));
        row.insert("arc_ask_sz1".into(), json!(asz));
        if mid > 0.0 {
            row.insert("arc_spread_bp".into(), json!((ask - bid) / mid * 1e4));
        }
        row.insert("arc_top_imb".into(), json!(imbalance(&b.bids, &b.asks, 1)));
        for lv in IMB_LEVELS {
            row.insert(
                format!("arc_l2_imb_{lv}lv"),
                json!(imbalance(&b.bids, &b.asks, lv)),
            );
        }
        row.insert(
            "arc_book_age_ms".into(),
            json!(t_ms.saturating_sub(b.rx_ms)),
        );
        row.insert("x_book".into(), json!(b.x_ms));
    }

    /// Distance of `px` from the touch of `side` (bp, positive = behind the
    /// touch), on the book as of `t`.
    pub fn dist_touch_bps(&self, t_ms: u64, side: QSide, px: f64) -> Option<f64> {
        let b = self.book_asof(t_ms)?;
        let touch = match side {
            QSide::Bid => b.bids.first()?.0,
            QSide::Ask => b.asks.first()?.0,
        };
        if touch <= 0.0 {
            return None;
        }
        Some(match side {
            QSide::Bid => (touch - px) / touch * 1e4,
            QSide::Ask => (px - touch) / touch * 1e4,
        })
    }

    /// The side-independent feature block at `t` (own book, own flow, own
    /// returns, reference, boundaries). Pure.
    pub fn features_at(
        &self,
        t_ms: u64,
        reference: Option<&RefShared>,
        row: &mut serde_json::Map<String, Value>,
    ) {
        self.book_fields(t_ms, row);
        for h in [1u64, 5, 30] {
            let (f, n) = self.flow(t_ms, h * 1_000);
            row.insert(format!("arc_flow_{h}s"), json!(f));
            if h == 30 {
                row.insert("arc_ntr_30s".into(), json!(n));
            }
            row.insert(format!("arc_ret_{h}s"), json!(self.ret_bp(t_ms, h * 1_000)));
        }
        let b = boundaries(t_ms);
        row.insert("prev_boundary".into(), json!(b.prev_name));
        row.insert(
            "secs_since_prev".into(),
            json!(t_ms.saturating_sub(b.prev_ms) / 1_000),
        );
        row.insert("next_boundary".into(), json!(b.next_name));
        row.insert(
            "secs_to_next".into(),
            json!(b.next_ms.saturating_sub(t_ms) / 1_000),
        );
        row.insert(
            "window".into(),
            json!(format!("{:?}", window_at(t_ms)).to_ascii_lowercase()),
        );
        if let Some(r) = reference {
            row.insert("ref_symbol".into(), json!(r.symbol));
            row.insert("ref_up".into(), json!(r.up_at(t_ms)));
            let fresh = r.fresh_mid(t_ms);
            row.insert("ref_mid".into(), json!(fresh));
            let asof = r.asof(t_ms);
            row.insert(
                "ref_lead_ms".into(),
                json!(asof
                    .filter(|_| r.up_at(t_ms))
                    .map(|(rx, _)| t_ms as i64 - rx as i64)),
            );
            row.insert("ref_t".into(), json!(asof.map(|(rx, _)| rx)));
            // The venue time is known for the latest frame only.
            row.insert(
                "ref_x".into(),
                json!(asof.filter(|(rx, _)| *rx == r.last_rx_ms).and(r.last_x_ms)),
            );
            if let (Some(arc), Some(rm)) = (
                row.get("arc_mid").and_then(Value::as_f64),
                fresh.filter(|m| *m > 0.0),
            ) {
                row.insert("ref_dev_bp".into(), json!((arc - rm) / rm * 1e4));
            }
            for h in [1u64, 5] {
                let (ret, jump) = if r.up_at(t_ms) {
                    r.ret_jump(t_ms, h * 1_000)
                } else {
                    (None, None)
                };
                row.insert(format!("ref_ret_{h}s"), json!(ret));
                row.insert(format!("ref_jump_{h}s"), json!(jump));
            }
        }
    }
}

// ----------------------------------------------------------------- runtime

#[derive(Debug, Clone)]
struct PendingTox {
    fill_id: String,
    fill_ts_ms: u64,
    px: f64,
    buy: bool,
    ref_mid0: Option<f64>,
    horizons: Vec<u64>,
}

/// The runtime-facing object: settings, the writer, the engine, the
/// reference state and the pending `markout_tox` rows.
pub struct Toxicity {
    settings: Settings,
    engine: Engine,
    reference: Option<Arc<Mutex<RefShared>>>,
    log: Option<GateLog>,
    pending: Vec<PendingTox>,
    rows_today: u64,
    rows_day: String,
}

impl Toxicity {
    /// `now_ms`: the `markout_tox` rows still owed by fills logged on the
    /// last two UTC days are rebuilt from the log (a restart must not lose
    /// the 60 / 300 s horizons of the fills just before it).
    pub fn new(settings: Settings, state_dir: &Path, now_ms: u64) -> Self {
        let reference = if settings.on {
            settings
                .ref_symbol
                .as_deref()
                .map(|s| Arc::new(Mutex::new(RefShared::new(s))))
        } else {
            None
        };
        let log = if settings.on {
            Some(GateLog::with_prefix(state_dir, "toxicity"))
        } else {
            None
        };
        let mut t = Toxicity {
            settings,
            engine: Engine::default(),
            reference,
            log,
            pending: Vec::new(),
            rows_today: 0,
            rows_day: String::new(),
        };
        if t.on() {
            t.restore_pending(state_dir, now_ms);
        }
        t
    }

    /// Rebuild `pending` from the log of the last two UTC days: every
    /// `fill` row's horizons without a `markout_tox` row. A horizon past
    /// its lateness cutoff is written now as `late` with `restart: true`;
    /// the others wait as usual.
    fn restore_pending(&mut self, state_dir: &Path, now_ms: u64) {
        let mut fills: BTreeMap<String, Value> = BTreeMap::new();
        let mut done: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for back in [1u64, 0] {
            let day = chrono::DateTime::from_timestamp_millis(
                now_ms.saturating_sub(back * 86_400_000) as i64,
            )
            .map(|d| d.format("%Y%m%d").to_string())
            .unwrap_or_default();
            let Ok(text) =
                std::fs::read_to_string(GateLog::path_for_prefixed(state_dir, "toxicity", &day))
            else {
                continue;
            };
            for line in text.lines() {
                let Ok(v) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                let Some(id) = v.get("fill_id").and_then(Value::as_str) else {
                    continue;
                };
                match v.get("kind").and_then(Value::as_str) {
                    Some("fill") => {
                        fills.insert(id.to_string(), v);
                    }
                    Some("markout_tox") => {
                        if let Some(h) = v.get("horizon_s").and_then(Value::as_u64) {
                            done.entry(id.to_string()).or_default().push(h);
                        }
                    }
                    _ => {}
                }
            }
        }
        let mut late_rows = Vec::new();
        let mut restored = 0usize;
        for (id, f) in fills {
            let owed: Vec<u64> = MARKOUT_HORIZONS_S
                .iter()
                .copied()
                .filter(|h| !done.get(&id).is_some_and(|d| d.contains(h)))
                .collect();
            if owed.is_empty() {
                continue;
            }
            let (Some(t), Some(px), Some(side)) = (
                f.get("t_fill").or(f.get("t")).and_then(Value::as_u64),
                f.get("px")
                    .and_then(Value::as_str)
                    .and_then(|p| p.parse::<f64>().ok()),
                f.get("side").and_then(Value::as_str),
            ) else {
                continue;
            };
            let market = f
                .get("market")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let mut horizons = Vec::new();
            for h in owed {
                if now_ms > t + h * 1_000 + MARKOUT_MAX_LATE_MS {
                    late_rows.push(json!({
                        "kind": "markout_tox", "t": now_ms, "market": market,
                        "fill_id": id, "horizon_s": h, "late": true, "restart": true,
                        "markout_mid_bps": Value::Null, "markout_ref_bps": Value::Null,
                        "mid": Value::Null, "ref_mid": Value::Null,
                    }));
                } else {
                    horizons.push(h);
                }
            }
            if !horizons.is_empty() {
                restored += 1;
                self.pending.push(PendingTox {
                    fill_id: id,
                    fill_ts_ms: t,
                    px,
                    buy: side == "buy",
                    ref_mid0: f.get("ref_mid_at_fill").and_then(Value::as_f64),
                    horizons,
                });
            }
        }
        let n_late = late_rows.len();
        for r in late_rows {
            self.write(now_ms, r);
        }
        if restored > 0 || n_late > 0 {
            log::info!(
                "[TOX] markouts restored after restart: {restored} fill(s) pending, {n_late} horizon(s) late"
            );
        }
    }

    pub fn on(&self) -> bool {
        self.settings.on
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// The reference feed's shared state, for the feed task.
    pub fn shared(&self) -> Option<Arc<Mutex<RefShared>>> {
        self.reference.clone()
    }

    pub fn on_own_book(
        &mut self,
        rx_ms: u64,
        x_ms: Option<u64>,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
    ) {
        if self.on() {
            self.engine.on_own_book(rx_ms, x_ms, bids, asks);
        }
    }

    pub fn on_own_trade(&mut self, rx_ms: u64, taker_buy: bool, px: f64, qty: f64) {
        if self.on() {
            self.engine.on_own_trade(rx_ms, taker_buy, px, qty);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn base_row(
        &self,
        kind: &str,
        t_ms: u64,
        x_ms: Option<u64>,
        market: &str,
        side: QSide,
        offset_bps: Option<f64>,
        arm_bps: Option<f64>,
        band_bps: Option<f64>,
        in_session: Option<bool>,
    ) -> serde_json::Map<String, Value> {
        let mut row = serde_json::Map::new();
        row.insert("kind".into(), json!(kind));
        row.insert("t".into(), json!(t_ms));
        row.insert("x".into(), json!(x_ms));
        row.insert("market".into(), json!(market));
        row.insert(
            "side".into(),
            json!(match side {
                QSide::Bid => "buy",
                QSide::Ask => "sell",
            }),
        );
        row.insert("offset_bps".into(), json!(offset_bps));
        row.insert("arm_bps".into(), json!(arm_bps));
        row.insert("band_bps".into(), json!(band_bps));
        row.insert("session_in".into(), json!(in_session));
        let r = self.reference.as_ref().map(|r| r.lock().unwrap());
        self.engine.features_at(t_ms, r.as_deref(), &mut row);
        row
    }

    /// A quote placed / re-pegged / replaced at `px` for `qty`.
    #[allow(clippy::too_many_arguments)]
    pub fn log_quote(
        &mut self,
        t_ms: u64,
        market: &str,
        side: QSide,
        px: Decimal,
        qty: Decimal,
        offset_bps: Option<f64>,
        arm_bps: Option<f64>,
        band_bps: Option<f64>,
        in_session: Option<bool>,
    ) {
        if !self.on() {
            return;
        }
        let pxf = px.to_f64().unwrap_or(0.0);
        let x = self.engine.book_asof(t_ms).and_then(|b| b.x_ms);
        let mut row = self.base_row(
            "quote", t_ms, x, market, side, offset_bps, arm_bps, band_bps, in_session,
        );
        row.insert("px".into(), json!(px.to_string()));
        row.insert("qty".into(), json!(qty.to_string()));
        row.insert(
            "dist_touch_bps".into(),
            json!(self.engine.dist_touch_bps(t_ms, side, pxf)),
        );
        self.write(t_ms, Value::Object(row));
    }

    /// A booked fill (any role): the features as of the fill's venue time,
    /// `fill_id` for the join, and the `markout_tox` rows owed from now on.
    #[allow(clippy::too_many_arguments)]
    pub fn log_fill(
        &mut self,
        fill_ts_ms: u64,
        rx_ms: u64,
        market: &str,
        fill_id: &str,
        buy: bool,
        maker: bool,
        px: Decimal,
        qty: Decimal,
        offset_bps: Option<f64>,
        arm_bps: Option<f64>,
        band_bps: Option<f64>,
        in_session: Option<bool>,
    ) {
        if !self.on() {
            return;
        }
        let side = if buy { QSide::Bid } else { QSide::Ask };
        let pxf = px.to_f64().unwrap_or(0.0);
        // Features at τ = fill − cancel latency: the last instant a gate
        // cycle could still have cancelled the quote (`quote_gate::
        // CANCEL_LATENCY_MS`, the gate's own fill stamp). A move or flow
        // inside the last 300 ms before the fill is not creditable.
        let tau = fill_ts_ms.saturating_sub(CANCEL_LATENCY_MS);
        let mut row = self.base_row(
            "fill",
            tau,
            Some(fill_ts_ms),
            market,
            side,
            offset_bps,
            arm_bps,
            band_bps,
            in_session,
        );
        row.insert("t_fill".into(), json!(fill_ts_ms));
        row.insert("cancel_latency_ms".into(), json!(CANCEL_LATENCY_MS));
        row.insert("rx_t".into(), json!(rx_ms));
        row.insert("fill_id".into(), json!(fill_id));
        row.insert("role".into(), json!(if maker { "maker" } else { "taker" }));
        row.insert("px".into(), json!(px.to_string()));
        row.insert("qty".into(), json!(qty.to_string()));
        row.insert(
            "dist_touch_bps".into(),
            json!(self.engine.dist_touch_bps(tau, side, pxf)),
        );
        // As-of the fill's venue time and fresh then (a reference that was
        // down or quiet for > REF_STALE_MS gives no reference markouts).
        let ref_mid0 = self
            .reference
            .as_ref()
            .and_then(|r| r.lock().unwrap().fresh_mid(fill_ts_ms));
        row.insert("ref_mid_at_fill".into(), json!(ref_mid0));
        self.write(fill_ts_ms, Value::Object(row));
        self.pending.push(PendingTox {
            fill_id: fill_id.to_string(),
            fill_ts_ms,
            px: pxf,
            buy,
            ref_mid0,
            horizons: MARKOUT_HORIZONS_S.to_vec(),
        });
    }

    /// Write every due `markout_tox` row: `markout_mid_bps` from the fresh
    /// own mid (`None` = stale / no book), `markout_ref_bps` from the fresh
    /// reference mid; a horizon older than `MARKOUT_MAX_LATE_MS` past due
    /// is written with both `null` (`late: true`). Signed in our favour
    /// (buy: up is positive).
    pub fn flush_markouts(&mut self, now_ms: u64, own_mid: Option<f64>, market: &str) {
        if !self.on() || self.pending.is_empty() {
            return;
        }
        let ref_mid = self
            .reference
            .as_ref()
            .and_then(|r| r.lock().unwrap().fresh_mid(now_ms));
        let mut rows = Vec::new();
        for p in &mut self.pending {
            let mut keep = Vec::new();
            for h in p.horizons.drain(..) {
                let due = p.fill_ts_ms + h * 1_000;
                if now_ms < due {
                    keep.push(h);
                    continue;
                }
                let late = now_ms > due + MARKOUT_MAX_LATE_MS;
                let sign = if p.buy { 1.0 } else { -1.0 };
                let mid_bps = (!late)
                    .then_some(own_mid)
                    .flatten()
                    .filter(|_| p.px > 0.0)
                    .map(|m| sign * (m - p.px) / p.px * 1e4);
                let ref_bps = (!late)
                    .then_some(ref_mid)
                    .flatten()
                    .zip(p.ref_mid0.filter(|r| *r > 0.0))
                    .map(|(m, r0)| sign * (m - r0) / r0 * 1e4);
                // Not late yet: wait (up to MARKOUT_MAX_LATE_MS) for every
                // mid the row needs — the own mid, and the reference mid
                // when the fill had one (a stale reference must not
                // consume the horizon with `markout_ref_bps: null`).
                let needs_ref = p.ref_mid0.is_some_and(|r| r > 0.0);
                if !late && (own_mid.is_none() || (needs_ref && ref_mid.is_none())) {
                    keep.push(h);
                    continue;
                }
                rows.push(json!({
                    "kind": "markout_tox", "t": now_ms, "market": market,
                    "fill_id": p.fill_id, "horizon_s": h, "late": late,
                    "markout_mid_bps": mid_bps, "markout_ref_bps": ref_bps,
                    "mid": own_mid, "ref_mid": ref_mid,
                }));
            }
            p.horizons = keep;
        }
        self.pending.retain(|p| !p.horizons.is_empty());
        for r in rows {
            self.write(now_ms, r);
        }
    }

    fn write(&mut self, t_ms: u64, row: Value) {
        let day = chrono::DateTime::from_timestamp_millis(t_ms as i64)
            .map(|d| d.format("%Y%m%d").to_string())
            .unwrap_or_default();
        if day != self.rows_day {
            self.rows_day = day;
            self.rows_today = 0;
        }
        self.rows_today += 1;
        if let Some(l) = self.log.as_mut() {
            l.write(t_ms, &row);
        }
    }

    /// status.json block.
    pub fn status(&self, now_ms: u64) -> Value {
        if !self.on() {
            return json!({"on": false});
        }
        let r = self.reference.as_ref().map(|r| r.lock().unwrap());
        json!({
            "on": true,
            "rows_today": self.rows_today,
            "pending_markouts": self.pending.len(),
            "ref": r.as_deref().map(|r| json!({
                "symbol": r.symbol, "market_id": r.market_id, "up": r.up,
                "age_ms": (r.last_rx_ms > 0).then(|| now_ms.saturating_sub(r.last_rx_ms)),
                "mid": r.fresh_mid(now_ms), "frames": r.frames,
                "reconnects": r.reconnects, "last_err": r.last_err,
            })),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(v: &Value, x: f64) {
        let got = v.as_f64().unwrap_or(f64::NAN);
        assert!((got - x).abs() < 1e-6, "{got} != {x}");
    }

    fn ms(s: &str) -> u64 {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .timestamp_millis() as u64
    }

    #[test]
    fn boundaries_name_the_new_york_session_edges() {
        // Monday 2026-10-19 14:00Z (10:00 New York, EDT): prev = 09:30 open, next = 16:00 close.
        let b = boundaries(ms("2026-10-19T14:00:00Z"));
        assert_eq!(b.prev_name, "cash_open_0930");
        assert_eq!(b.prev_ms, ms("2026-10-19T13:30:00Z"));
        assert_eq!(b.next_name, "cash_close_1600");
        assert_eq!(b.next_ms, ms("2026-10-19T20:00:00Z"));
        // Sunday 2026-10-18 23:00Z (19:00 NY): prev = 18:00 Sunday reopen, next = 20:00 overnight.
        let b = boundaries(ms("2026-10-18T23:00:00Z"));
        assert_eq!(b.prev_name, "sunday_reopen_1800");
        assert_eq!(b.next_name, "overnight_start_2000");
        assert_eq!(b.next_ms, ms("2026-10-19T00:00:00Z"));
        // Saturday: prev = Friday 20:00 weekend close, next = Sunday 18:00.
        let b = boundaries(ms("2026-10-17T12:00:00Z"));
        assert_eq!(b.prev_name, "weekend_close_2000");
        assert_eq!(b.prev_ms, ms("2026-10-17T00:00:00Z"));
        assert_eq!(b.next_name, "sunday_reopen_1800");
        assert_eq!(b.next_ms, ms("2026-10-18T22:00:00Z"));
        // Exactly at a boundary it is the previous one.
        let b = boundaries(ms("2026-10-19T13:30:00Z"));
        assert_eq!(b.prev_name, "cash_open_0930");
        assert_eq!(b.prev_ms, ms("2026-10-19T13:30:00Z"));
        // EST (November): 09:30 NY = 14:30Z.
        let b = boundaries(ms("2026-11-16T15:00:00Z"));
        assert_eq!(b.prev_ms, ms("2026-11-16T14:30:00Z"));
    }

    #[test]
    fn engine_book_flow_and_return_features_are_as_of() {
        let mut e = Engine::default();
        let t0 = 1_000_000_000_000u64;
        e.on_own_book(
            t0,
            Some(t0 - 5),
            &[(100.0, 10.0), (99.9, 5.0)],
            &[(100.1, 20.0), (100.2, 5.0)],
        );
        e.on_own_trade(t0 + 500, true, 100.0, 3.0);
        e.on_own_trade(t0 + 1_500, false, 100.0, 1.0);
        e.on_own_book(t0 + 2_000, None, &[(100.2, 1.0)], &[(100.4, 1.0)]);
        let mut row = serde_json::Map::new();
        e.features_at(t0 + 2_000, None, &mut row);
        approx(&row["arc_mid"], 100.3);
        assert_eq!(row["arc_flow_1s"], json!(-100.0)); // only the sell at t0+1.5s
        assert_eq!(row["arc_flow_5s"], json!(200.0));
        assert_eq!(row["arc_ntr_30s"], json!(2));
        let r1 = row["arc_ret_1s"].as_f64().unwrap();
        assert!((r1 - (100.3 - 100.05) / 100.05 * 1e4).abs() < 1e-6, "{r1}");
        // At t0 the second book is not yet known: as-of, never look-ahead.
        let mut row0 = serde_json::Map::new();
        e.features_at(t0, None, &mut row0);
        approx(&row0["arc_mid"], 100.05);
        assert_eq!(row0["arc_top_imb"], json!((10.0 - 20.0) / 30.0));
        assert_eq!(row0["arc_l2_imb_5lv"], json!((15.0 - 25.0) / 40.0));
        assert_eq!(row0["x_book"], json!(t0 - 5));
        assert_eq!(
            e.dist_touch_bps(t0, QSide::Bid, 99.95).map(|d| d.round()),
            Some(5.0)
        );
        assert_eq!(
            e.dist_touch_bps(t0, QSide::Ask, 100.1 * (1.0 + 3e-4))
                .map(|d| d.round()),
            Some(3.0)
        );
    }

    #[test]
    fn reference_returns_jumps_and_lead() {
        let mut r = RefShared::new("SPY");
        let t0 = 1_000_000_000_000u64;
        r.up = true;
        r.push_top(t0, Some(t0 - 1), 100.0, 100.02);
        r.push_top(t0 + 2_000, None, 100.10, 100.12);
        r.push_top(t0 + 4_000, None, 100.04, 100.06);
        let (ret, jump) = r.ret_jump(t0 + 4_500, 5_000);
        // base = mid as of t0-500 → none before t0 ⇒ use t0 record? No: as-of t0-500 has no record.
        assert_eq!((ret, jump), (None, None));
        let (ret, jump) = r.ret_jump(t0 + 4_500, 4_000);
        // base = mid at t0 (100.01); last = 100.05; jump = max over (t0+500, t0+4500] = 100.11
        assert!((ret.unwrap() - (100.05 - 100.01) / 100.01 * 1e4).abs() < 1e-6);
        assert!((jump.unwrap() - (100.11 - 100.01) / 100.01 * 1e4).abs() < 1e-6);
        assert!((r.fresh_mid(t0 + 4_500).unwrap() - 100.05).abs() < 1e-9);
        assert_eq!(r.fresh_mid(t0 + 20_000), None, "stale");
        let mut row = serde_json::Map::new();
        let e = Engine::default();
        e.features_at(t0 + 4_500, Some(&r), &mut row);
        assert_eq!(row["ref_lead_ms"], json!(500));
        assert_eq!(row["ref_x"], json!(Value::Null)); // last frame had no venue time
        assert_eq!(row["ref_symbol"], json!("SPY"));
        // the feed goes down at t0+4.4 s: a row at t0+4.5 s is inside the outage
        r.down(t0 + 4_400, "test");
        let mut row = serde_json::Map::new();
        e.features_at(t0 + 4_500, Some(&r), &mut row);
        assert_eq!(row["ref_up"], json!(false));
        assert_eq!(row["ref_mid"], json!(Value::Null));
        assert_eq!(row["ref_jump_5s"], json!(Value::Null));
    }

    #[test]
    fn reference_health_is_evaluated_as_of_the_row_time() {
        let mut r = RefShared::new("SPY");
        let t0 = 1_000_000_000_000u64;
        r.up_again(t0 - 10_000);
        r.push_top(t0, None, 100.0, 100.02);
        r.down(t0 + 2_000, "ws closed");
        // a row stamped before the outage sees a fresh reference …
        assert!(r.up_at(t0 + 1_000));
        assert!((r.fresh_mid(t0 + 1_000).unwrap() - 100.01).abs() < 1e-9);
        // … a row inside the outage does not, even though a pre-gap record exists
        assert!(!r.up_at(t0 + 3_000));
        assert_eq!(r.fresh_mid(t0 + 3_000), None);
        // after the reconnect the pre-gap record is not reused across the gap
        r.up_again(t0 + 8_000);
        assert!(!r.up_at(t0 + 5_000));
        r.push_top(t0 + 8_500, None, 100.2, 100.22);
        assert!(r.up_at(t0 + 9_000));
        assert!((r.fresh_mid(t0 + 9_000).unwrap() - 100.21).abs() < 1e-9);
        // before the first frame: not up
        assert!(!r.up_at(t0 - 20_000));
        let mut row = serde_json::Map::new();
        Engine::default().features_at(t0 + 3_000, Some(&r), &mut row);
        assert_eq!(row["ref_up"], json!(false));
        assert_eq!(row["ref_mid"], json!(Value::Null));
    }

    #[test]
    fn a_reference_window_crossing_an_outage_gives_no_return_or_jump() {
        let mut r = RefShared::new("SPY");
        let t0 = 1_000_000_000_000u64;
        r.up_again(t0 - 10_000);
        r.push_top(t0, None, 100.0, 100.02);
        r.down(t0 + 1_000, "ws closed");
        r.up_again(t0 + 3_000);
        r.push_top(t0 + 3_100, None, 101.0, 101.02); // +99 bp across the gap
                                                     // 5 s window (t0-1.5s, t0+3.5s] crosses the outage: null
        assert_eq!(r.ret_jump(t0 + 3_500, 5_000), (None, None));
        // 1 s window after the reconnect, no outage inside: fine
        r.push_top(t0 + 3_900, None, 101.1, 101.12);
        let (ret, jump) = r.ret_jump(t0 + 4_000, 1_000);
        assert!(ret.is_some() && jump.is_some());
        // an open (still down) outage also blocks
        r.down(t0 + 5_000, "again");
        assert_eq!(r.ret_jump(t0 + 6_000, 5_000), (None, None));
        assert!(r.outage_in(t0 + 5_500, t0 + 6_000));
        assert!(!r.outage_in(t0 + 3_200, t0 + 4_000));
    }

    #[test]
    fn a_fill_booked_late_keeps_its_first_seen_time() {
        assert_eq!(fill_time(Some(10), 40), 10);
        assert_eq!(fill_time(None, 40), 40);
        assert_eq!(fill_time(Some(50), 40), 40, "never later than the booking");
        // booked 30 s late: features at first_seen − 300 ms, markout clocks from first_seen
        let dir = tempfile::tempdir().unwrap();
        let t0 = ms("2026-10-20T15:00:00Z");
        let mut tox = Toxicity::new(
            Settings {
                on: true,
                ..Default::default()
            },
            dir.path(),
            t0,
        );
        tox.on_own_book(t0 - 1_000, None, &[(99.9, 1.0)], &[(100.1, 1.0)]);
        tox.on_own_book(t0 + 20_000, None, &[(109.9, 1.0)], &[(110.1, 1.0)]); // after the fill
        let booked = t0 + 30_000;
        let ts = fill_time(Some(t0), booked);
        tox.log_fill(
            ts,
            booked,
            "SPY-USD",
            "f-late",
            true,
            true,
            Decimal::new(9990, 2),
            Decimal::ONE,
            None,
            None,
            None,
            None,
        );
        tox.flush_markouts(booked + 500, Some(110.0), "SPY-USD");
        let rows: Vec<Value> = std::fs::read_to_string(dir.path().join("toxicity_20261020.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(rows[0]["t_fill"], json!(t0));
        assert_eq!(rows[0]["t"], json!(t0 - CANCEL_LATENCY_MS));
        assert_eq!(rows[0]["rx_t"], json!(booked));
        approx(&rows[0]["arc_mid"], 100.0); // the book as of the fill, not the later one
                                            // 1 s and 10 s horizons are past their cutoff (clock started at t0): late
        let late: Vec<u64> = rows
            .iter()
            .filter(|r| r["kind"] == "markout_tox" && r["late"] == true)
            .map(|r| r["horizon_s"].as_u64().unwrap())
            .collect();
        assert_eq!(late, vec![1, 10]);
        assert_eq!(
            tox.status(booked + 500)["pending_markouts"],
            1,
            "60 s and 300 s still owed"
        );
    }

    #[test]
    fn lighter_frames_build_a_book_and_other_frames_are_ignored() {
        let snap = r#"{"type":"subscribed/order_book","channel":"order_book:7","last_updated_at":1792443600123456,"order_book":{"bids":[{"price":"100.0","size":"2.0"},{"price":"99.9","size":"1.0"}],"asks":[{"price":"100.2","size":"1.0"}]}}"#;
        let (s, x, b, a) = parse_lighter_frame(snap, 7).unwrap().unwrap();
        assert!(s);
        assert_eq!(x, Some(1_792_443_600_123));
        let mut book = RefBook::default();
        book.apply(s, b, a);
        assert_eq!(book.top(), Some((100.0, 100.2)));
        let upd = r#"{"type":"update/order_book","channel":"order_book/7","order_book":{"bids":[{"price":"100.0","size":"0.00000"},{"price":"100.1","size":"0.5"}],"asks":[]}}"#;
        let (s, _, b, a) = parse_lighter_frame(upd, 7).unwrap().unwrap();
        assert!(!s);
        book.apply(s, b, a);
        assert_eq!(book.top(), Some((100.1, 100.2)));
        assert!(
            parse_lighter_frame(upd, 8).unwrap().is_none(),
            "other market"
        );
        assert!(parse_lighter_frame(r#"{"type":"pong"}"#, 7)
            .unwrap()
            .is_none());
        assert!(parse_lighter_frame(r#"{"type":"update/order_book","channel":"order_book/7","order_book":{"bids":[{"price":"x","size":"1"}]}}"#, 7).is_err());
        let body = json!({"order_book_details": [{"market_id": 3, "symbol": "spy", "status": "active"}, {"market_id": 9, "symbol": "AAPL"}]});
        assert_eq!(market_id_of(&body, "SPY"), Some(3));
        assert_eq!(market_id_of(&body, "GLD"), None);
        assert_eq!(
            ws_url("https://mainnet.zklighter.elliot.ai/"),
            "wss://mainnet.zklighter.elliot.ai/stream"
        );
        assert_eq!(default_ref_symbol("AAPL-USD"), "AAPL");
    }

    #[test]
    fn fill_rows_carry_the_join_key_and_markouts_follow_at_the_cutoff_horizons() {
        let dir = tempfile::tempdir().unwrap();
        let settings = Settings {
            on: true,
            ref_symbol: Some("SPY".into()),
            lighter_base: LIGHTER_MAINNET.into(),
        };
        let mut tox = Toxicity::new(settings, dir.path(), 0);
        let t0 = ms("2026-10-20T15:00:00Z");
        {
            let sh = tox.shared().unwrap();
            let mut r = sh.lock().unwrap();
            r.up = true;
            r.push_top(t0 - 100, Some(t0 - 150), 200.0, 200.02);
        }
        tox.on_own_book(t0 - 50, Some(t0 - 60), &[(199.9, 1.0)], &[(200.1, 1.0)]);
        tox.log_quote(
            t0,
            "SPY-USD",
            QSide::Bid,
            Decimal::new(19980, 2),
            Decimal::ONE,
            Some(5.0),
            Some(8.0),
            Some(2.0),
            Some(true),
        );
        tox.log_fill(
            t0 + 1_000,
            t0 + 1_200,
            "SPY-USD",
            "f-1",
            true,
            true,
            Decimal::new(19980, 2),
            Decimal::ONE,
            Some(5.0),
            Some(8.0),
            Some(2.0),
            Some(true),
        );
        // 1 s horizon due: own mid 200.2 (+20 bp vs 199.8), ref 200.21 vs 200.01
        {
            let sh = tox.shared().unwrap();
            sh.lock().unwrap().push_top(t0 + 2_000, None, 200.2, 200.22);
        }
        tox.flush_markouts(t0 + 2_100, Some(200.2), "SPY-USD");
        // 10 s horizon: late (no fresh mid for a long time) → nulls, late=true
        tox.flush_markouts(t0 + 30_000, None, "SPY-USD");
        let day = chrono::DateTime::from_timestamp_millis(t0 as i64)
            .unwrap()
            .format("%Y%m%d")
            .to_string();
        let text =
            std::fs::read_to_string(dir.path().join(format!("toxicity_{day}.jsonl"))).unwrap();
        let rows: Vec<Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(rows[0]["kind"], "quote");
        assert_eq!(rows[0]["t"], json!(t0));
        assert_eq!(rows[0]["x"], json!(t0 - 60));
        assert_eq!(
            rows[0]["dist_touch_bps"].as_f64().map(|d| d.round()),
            Some(5.0)
        );
        approx(&rows[0]["ref_dev_bp"], (200.0 - 200.01) / 200.01 * 1e4);
        assert_eq!(rows[1]["kind"], "fill");
        assert_eq!(rows[1]["fill_id"], "f-1");
        assert_eq!(
            rows[1]["t"],
            json!(t0 + 1_000 - CANCEL_LATENCY_MS),
            "features at τ"
        );
        assert_eq!(rows[1]["t_fill"], json!(t0 + 1_000));
        assert_eq!(rows[1]["x"], json!(t0 + 1_000));
        assert_eq!(rows[1]["rx_t"], json!(t0 + 1_200));
        assert_eq!(rows[1]["role"], "maker");
        approx(&rows[1]["ref_mid_at_fill"], 200.01);
        assert_eq!(rows[1]["window"], "cash");
        let m1 = &rows[2];
        assert_eq!(m1["kind"], "markout_tox");
        assert_eq!(m1["fill_id"], "f-1");
        assert_eq!(m1["horizon_s"], 1);
        assert!(
            (m1["markout_mid_bps"].as_f64().unwrap() - (200.2 - 199.8) / 199.8 * 1e4).abs() < 1e-6
        );
        assert!(
            (m1["markout_ref_bps"].as_f64().unwrap() - (200.21 - 200.01) / 200.01 * 1e4).abs()
                < 1e-6
        );
        let m10 = &rows[3];
        assert_eq!(m10["horizon_s"], 10);
        assert_eq!(m10["late"], true);
        assert_eq!(m10["markout_mid_bps"], Value::Null);
        assert_eq!(rows.len(), 4, "60 s and 300 s are still pending");
        assert_eq!(tox.status(t0 + 30_000)["pending_markouts"], 1);
    }

    #[test]
    fn a_stale_reference_does_not_consume_a_horizon_before_the_cutoff() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = ms("2026-10-20T15:00:00Z");
        let settings = Settings {
            on: true,
            ref_symbol: Some("SPY".into()),
            ..Default::default()
        };
        let mut tox = Toxicity::new(settings, dir.path(), t0);
        {
            let sh = tox.shared().unwrap();
            let mut r = sh.lock().unwrap();
            r.up = true;
            r.push_top(t0 - 100, None, 200.0, 200.02);
        }
        tox.log_fill(
            t0,
            t0,
            "SPY-USD",
            "f-2",
            true,
            true,
            Decimal::new(200, 0),
            Decimal::ONE,
            None,
            None,
            None,
            None,
        );
        // 1 s due, own mid fresh, reference stale (> 5 s old): wait
        tox.flush_markouts(t0 + 7_000, Some(200.1), "SPY-USD");
        let path = dir.path().join("toxicity_20261020.jsonl");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().lines().count(),
            1,
            "fill row only"
        );
        // the reference recovers inside the grace period: both sides priced
        {
            let sh = tox.shared().unwrap();
            sh.lock().unwrap().push_top(t0 + 9_000, None, 200.3, 200.32);
        }
        tox.flush_markouts(t0 + 9_500, Some(200.1), "SPY-USD");
        let rows: Vec<Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1]["horizon_s"], 1);
        assert_eq!(rows[1]["late"], false);
        assert!(rows[1]["markout_ref_bps"].as_f64().is_some());
        // a fill without a reference mid does not wait for one
        tox.log_fill(
            t0 + 20_000,
            t0 + 20_000,
            "SPY-USD",
            "f-3",
            true,
            true,
            Decimal::new(200, 0),
            Decimal::ONE,
            None,
            None,
            None,
            None,
        );
        {
            let sh = tox.shared().unwrap();
            sh.lock().unwrap().down(t0 + 20_500, "test");
        }
        let before = std::fs::read_to_string(&path).unwrap().lines().count();
        tox.flush_markouts(t0 + 21_500, Some(200.1), "SPY-USD");
        let rows: Vec<Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        // f-3's 1 s horizon is priced without a reference (ref null, not late);
        // f-2's 10 s horizon, past its cutoff with the reference still down,
        // is written late at the same flush.
        let f3 = rows
            .iter()
            .find(|r| r["fill_id"] == "f-3" && r["kind"] == "markout_tox")
            .unwrap();
        assert_eq!(f3["horizon_s"], 1);
        assert_eq!(f3["late"], false);
        assert_eq!(f3["markout_ref_bps"], Value::Null);
        let f2_10 = rows
            .iter()
            .find(|r| r["fill_id"] == "f-2" && r["horizon_s"] == 10)
            .unwrap();
        assert_eq!(f2_10["late"], true);
        assert_eq!(rows.len(), before + 2);
    }

    #[test]
    fn pending_markouts_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = ms("2026-10-20T23:59:30Z"); // fill just before the UTC day rolls
        let s = Settings {
            on: true,
            ..Default::default()
        };
        let mut tox = Toxicity::new(s.clone(), dir.path(), t0);
        tox.log_fill(
            t0,
            t0,
            "SPY-USD",
            "f-r",
            false,
            true,
            Decimal::new(100, 0),
            Decimal::ONE,
            None,
            None,
            None,
            None,
        );
        tox.flush_markouts(t0 + 1_500, Some(100.0), "SPY-USD"); // 1 s done
        drop(tox);
        // restart 45 s later (next UTC day): 10 s is past its cutoff, 60 s and 300 s still owed
        let now = t0 + 45_000;
        let mut tox = Toxicity::new(s, dir.path(), now);
        assert_eq!(tox.status(now)["pending_markouts"], 1);
        let day_old = "20261020";
        let day_new = "20261021";
        let late: Vec<Value> =
            std::fs::read_to_string(dir.path().join(format!("toxicity_{day_new}.jsonl")))
                .unwrap()
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
        assert_eq!(late.len(), 1);
        assert_eq!(late[0]["horizon_s"], 10);
        assert_eq!(late[0]["restart"], true);
        assert_eq!(late[0]["late"], true);
        tox.flush_markouts(t0 + 61_000, Some(99.0), "SPY-USD");
        let _ = day_old;
        let rows: Vec<Value> =
            std::fs::read_to_string(dir.path().join(format!("toxicity_{day_new}.jsonl")))
                .unwrap()
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
        let m60 = rows
            .iter()
            .find(|r| r["horizon_s"] == 60)
            .expect("60 s written after restart");
        assert_eq!(m60["fill_id"], "f-r");
        assert!(
            (m60["markout_mid_bps"].as_f64().unwrap() - 100.0).abs() < 1e-9,
            "sell at 100, mid 99 = +100 bp"
        );
        assert_eq!(tox.status(now)["pending_markouts"], 1, "300 s still owed");
        // a second restart with everything written restores nothing
        let tox2 = Toxicity::new(
            Settings {
                on: true,
                ..Default::default()
            },
            dir.path(),
            now + 400_000,
        );
        assert_eq!(tox2.status(now + 400_000)["pending_markouts"], 0);
    }

    #[test]
    fn off_writes_nothing_and_a_sell_markout_is_signed_in_our_favour() {
        let dir = tempfile::tempdir().unwrap();
        let mut off = Toxicity::new(Settings::default(), dir.path(), 0);
        off.log_quote(
            1,
            "SPY-USD",
            QSide::Bid,
            Decimal::ONE,
            Decimal::ONE,
            None,
            None,
            None,
            None,
        );
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
        assert_eq!(off.status(1), json!({"on": false}));
        let mut tox = Toxicity::new(
            Settings {
                on: true,
                ..Default::default()
            },
            dir.path(),
            0,
        );
        let t0 = ms("2026-10-20T15:00:00Z");
        tox.log_fill(
            t0,
            t0,
            "SPY-USD",
            "s-1",
            false,
            true,
            Decimal::new(100, 0),
            Decimal::ONE,
            None,
            None,
            None,
            None,
        );
        tox.flush_markouts(t0 + 1_500, Some(99.9), "SPY-USD");
        let day = "20261020";
        let text =
            std::fs::read_to_string(dir.path().join(format!("toxicity_{day}.jsonl"))).unwrap();
        let rows: Vec<Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(rows[0]["kind"], "fill");
        assert!(
            rows[0].get("ref_symbol").is_none(),
            "no reference configured"
        );
        assert!(
            (rows[1]["markout_mid_bps"].as_f64().unwrap() - 10.0).abs() < 1e-9,
            "sell, mid fell 10 bp = +10 in our favour"
        );
        assert_eq!(rows[1]["markout_ref_bps"], Value::Null);
    }
}
