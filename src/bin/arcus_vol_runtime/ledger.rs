//! Position / PnL book-keeping, the UTC-day rollover and the stops.
//!
//! Net PnL counts fees as a cost: `realized − fees + unrealized`. The daily
//! figure measures unrealized PnL from the value it had at the day's start,
//! so an open position carried over midnight is not charged twice.
//!
//! Sticky halt (cumulative stop): the bot writes `HALT` in the state dir.
//! The runtime is halted whenever `HALT` exists OR state says
//! `sticky_halt` (Codex P2, pairtrade#361): a `HALT` file found at load or at
//! any tick halts it whatever the state says, and a sticky state whose file
//! went missing gets the file recreated. Clearing needs both, by hand, with
//! the bot stopped (it rewrites state every tick): delete `HALT` AND set
//! `"sticky_halt": false` in `state.json`. On the next start the bot sees the
//! agreed clear, re-bases the cumulative stop at the current net
//! (`cum_baseline`) so the next halt needs a further `CUM_STOP_USD` loss.
//!
//! Fills are booked durably (Codex P1, pairtrade#361): the fills.jsonl row
//! is appended and fsynced first, then the ledger books it and remembers its
//! trade id (`booked_ids`, persisted in state.json), so a retry after a
//! failed state write never counts a fill twice.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Position {
    /// Signed base quantity (+long).
    pub qty: Decimal,
    /// Average entry of the open quantity (0 when flat).
    pub avg_px: Decimal,
    /// When the position last left flat (ms); drives MAX_HOLD.
    pub opened_at_ms: Option<u64>,
}

impl Position {
    /// Apply a fill; returns the realized PnL (before fees).
    pub fn apply(&mut self, buy: bool, qty: Decimal, px: Decimal, now_ms: u64) -> Decimal {
        if qty <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let signed = if buy { qty } else { -qty };
        let old = self.qty;
        if old.is_zero() || (old > Decimal::ZERO) == buy {
            let total = old.abs() + qty;
            self.avg_px = (old.abs() * self.avg_px + qty * px) / total;
            self.qty = old + signed;
            if old.is_zero() {
                self.opened_at_ms = Some(now_ms);
            }
            return Decimal::ZERO;
        }
        let closed = qty.min(old.abs());
        let direction = if old > Decimal::ZERO {
            Decimal::ONE
        } else {
            Decimal::NEGATIVE_ONE
        };
        let realized = closed * (px - self.avg_px) * direction;
        self.qty = old + signed;
        if self.qty.is_zero() {
            self.avg_px = Decimal::ZERO;
            self.opened_at_ms = None;
        } else if (self.qty > Decimal::ZERO) != (old > Decimal::ZERO) {
            // Flipped through flat: the remainder opens fresh at this price.
            self.avg_px = px;
            self.opened_at_ms = Some(now_ms);
        }
        realized
    }

    pub fn unrealized(&self, mark: Decimal) -> Decimal {
        if self.qty.is_zero() {
            Decimal::ZERO
        } else {
            self.qty * (mark - self.avg_px)
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Ledger {
    /// "dry_run" / "live": a state file is never reused across modes.
    pub mode: String,
    /// UTC day (YYYY-MM-DD) the day_* fields belong to.
    pub day: String,
    pub position: Position,
    pub day_realized: Decimal,
    pub day_fees: Decimal,
    pub day_start_unrealized: Decimal,
    pub day_volume: Decimal,
    pub day_maker_volume: Decimal,
    pub day_taker_volume: Decimal,
    pub cum_realized: Decimal,
    pub cum_fees: Decimal,
    pub cum_volume: Decimal,
    pub cum_maker_volume: Decimal,
    pub cum_taker_volume: Decimal,
    pub fills: u64,
    /// Cumulative net at the last manual HALT clear (see module docs).
    pub cum_baseline: Decimal,
    pub day_halt: bool,
    pub sticky_halt: bool,
    pub sticky_reason: Option<String>,
    /// Cumulative net when the sticky halt engaged; `Some` until a manual
    /// clear is seen (then the baseline is re-based and this goes `None`).
    #[serde(default)]
    pub halted_at_net: Option<Decimal>,
    /// Trade ids already booked (bounded, oldest dropped first).
    #[serde(default)]
    pub booked_ids: std::collections::VecDeque<String>,
    /// ts_ms of the newest booked fill: replay never books anything older,
    /// so an id aged out of `booked_ids` cannot be booked twice.
    #[serde(default)]
    pub last_booked_ts_ms: u64,
}

/// How many booked trade ids state.json remembers.
pub const BOOKED_IDS_CAP: usize = 20_000;

impl Ledger {
    pub fn new(mode: &str, day: &str) -> Self {
        Ledger {
            mode: mode.to_string(),
            day: day.to_string(),
            ..Ledger::default()
        }
    }

    pub fn record_fill(
        &mut self,
        buy: bool,
        qty: Decimal,
        px: Decimal,
        fee: Decimal,
        maker: bool,
        now_ms: u64,
    ) -> Decimal {
        let realized = self.position.apply(buy, qty, px, now_ms);
        let notional = qty * px;
        self.day_realized += realized;
        self.cum_realized += realized;
        self.day_fees += fee;
        self.cum_fees += fee;
        self.day_volume += notional;
        self.cum_volume += notional;
        if maker {
            self.day_maker_volume += notional;
            self.cum_maker_volume += notional;
        } else {
            self.day_taker_volume += notional;
            self.cum_taker_volume += notional;
        }
        self.fills += 1;
        realized
    }

    /// Start a new UTC day. Returns true when it rolled.
    pub fn rollover(&mut self, today: &str, mark: Decimal) -> bool {
        if self.day == today {
            return false;
        }
        self.day = today.to_string();
        self.day_realized = Decimal::ZERO;
        self.day_fees = Decimal::ZERO;
        self.day_volume = Decimal::ZERO;
        self.day_maker_volume = Decimal::ZERO;
        self.day_taker_volume = Decimal::ZERO;
        self.day_start_unrealized = self.position.unrealized(mark);
        self.day_halt = false;
        true
    }

    pub fn daily_net(&self, mark: Decimal) -> Decimal {
        self.day_realized - self.day_fees + self.position.unrealized(mark)
            - self.day_start_unrealized
    }

    pub fn cum_net(&self, mark: Decimal) -> Decimal {
        self.cum_realized - self.cum_fees + self.position.unrealized(mark)
    }

    /// Net cost per $1M of volume (positive = we paid).
    pub fn cost_per_million(&self, mark: Decimal) -> Option<Decimal> {
        (self.cum_volume > Decimal::ZERO)
            .then(|| -self.cum_net(mark) / self.cum_volume * Decimal::from(1_000_000))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Halt {
    Sticky(String),
    Kill,
    Day,
}

impl Halt {
    pub fn label(&self) -> String {
        match self {
            Halt::Sticky(r) => format!("sticky: {r}"),
            Halt::Kill => "kill_switch".to_string(),
            Halt::Day => "daily_stop".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RiskOutcome {
    pub halt: Option<Halt>,
    pub events: Vec<String>,
    /// Sticky halt in force but `HALT` missing: the caller must (re)write it.
    pub write_halt_file: bool,
}

/// Update the stop flags from the current marks and return the halt in
/// force, if any (sticky > kill > day). `halt_file` is whether `HALT`
/// exists; see the module docs for how the file and `sticky_halt` interact.
pub fn risk_check(
    l: &mut Ledger,
    mark: Decimal,
    kill: bool,
    halt_file: bool,
    daily_stop: Decimal,
    cum_stop: Decimal,
) -> RiskOutcome {
    let mut events = Vec::new();
    if !halt_file && !l.sticky_halt && l.halted_at_net.is_some() {
        // Both cleared by hand (file deleted AND state edited): re-base.
        l.halted_at_net = None;
        l.sticky_reason = None;
        l.cum_baseline = l.cum_net(mark);
        events.push(format!(
            "HALT cleared by hand; cumulative stop re-based at net {}",
            l.cum_baseline.round_dp(2)
        ));
    }
    if halt_file && !l.sticky_halt {
        l.sticky_halt = true;
        l.halted_at_net.get_or_insert(l.cum_net(mark));
        let reason = "HALT file present".to_string();
        events.push(format!("STICKY HALT: {reason}"));
        l.sticky_reason = Some(reason);
    }
    let cum_loss = l.cum_baseline - l.cum_net(mark);
    if !l.sticky_halt && cum_loss > cum_stop {
        l.sticky_halt = true;
        l.halted_at_net = Some(l.cum_net(mark));
        let reason = format!(
            "cumulative net loss {} > {} (since baseline)",
            cum_loss.round_dp(2),
            cum_stop
        );
        events.push(format!("STICKY HALT: {reason}"));
        l.sticky_reason = Some(reason);
    }
    let write_halt_file = l.sticky_halt && !halt_file;
    let day_loss = -l.daily_net(mark);
    if !l.day_halt && day_loss > daily_stop {
        l.day_halt = true;
        events.push(format!(
            "DAILY STOP: net loss {} > {} until the next UTC day",
            day_loss.round_dp(2),
            daily_stop
        ));
    }
    let halt = if l.sticky_halt {
        Some(Halt::Sticky(l.sticky_reason.clone().unwrap_or_default()))
    } else if kill {
        Some(Halt::Kill)
    } else if l.day_halt {
        Some(Halt::Day)
    } else {
        None
    };
    RiskOutcome {
        halt,
        events,
        write_halt_file,
    }
}

/// One fill to book (live from the connector, or simulated).
#[derive(Debug, Clone)]
pub struct FillIn {
    pub trade_id: String,
    pub buy: bool,
    pub qty: Decimal,
    pub px: Decimal,
    pub fee: Decimal,
    pub maker: bool,
    pub order_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Booking {
    /// Already in `booked_ids`: nothing counted again.
    AlreadyBooked,
    /// Appended to fills.jsonl and booked; carries the realized PnL.
    Booked(Decimal),
}

impl Ledger {
    pub fn has_booked(&self, trade_id: &str) -> bool {
        self.booked_ids.iter().any(|id| id == trade_id)
    }

    fn mark_booked(&mut self, trade_id: &str, ts_ms: u64) {
        self.last_booked_ts_ms = self.last_booked_ts_ms.max(ts_ms);
        self.booked_ids.push_back(trade_id.to_string());
        while self.booked_ids.len() > BOOKED_IDS_CAP {
            self.booked_ids.pop_front();
        }
    }
}

/// Book `f` durably: the fills.jsonl row is written (via `append`, which
/// must fsync) BEFORE the ledger changes, so a failed write leaves the
/// ledger untouched and the fill can be retried; an id already booked is
/// never counted again. The caller persists state.json afterwards and only
/// then lets the connector forget the fill.
pub fn book_fill(
    l: &mut Ledger,
    f: &FillIn,
    now_ms: u64,
    append: impl FnOnce(&serde_json::Value) -> std::io::Result<()>,
) -> std::io::Result<Booking> {
    if l.has_booked(&f.trade_id) {
        return Ok(Booking::AlreadyBooked);
    }
    let mut preview = l.position.clone();
    let realized = preview.apply(f.buy, f.qty, f.px, now_ms);
    let row = serde_json::json!({
        "kind": "fill",
        "ts_ms": now_ms,
        "mode": l.mode,
        "side": if f.buy { "buy" } else { "sell" },
        "px": f.px.to_string(),
        "qty": f.qty.to_string(),
        "notional": (f.qty * f.px).round_dp(4).to_string(),
        "role": if f.maker { "maker" } else { "taker" },
        "fee": f.fee.round_dp(6).to_string(),
        "realized": realized.round_dp(6).to_string(),
        "order_id": f.order_id,
        "fill_id": f.trade_id,
        "inventory": preview.qty.to_string(),
    });
    append(&row)?;
    let booked = l.record_fill(f.buy, f.qty, f.px, f.fee, f.maker, now_ms);
    l.mark_booked(&f.trade_id, now_ms);
    Ok(Booking::Booked(booked))
}

/// Book every fills.jsonl row the state does not have yet (Codex P1,
/// pairtrade#361): the row is fsynced before the ledger moves, so a crash
/// between that and the state write leaves a row state.json never saw.
/// Only `kind: fill` rows of this ledger's mode count; a row older than
/// `last_booked_ts_ms` or whose id is already booked is skipped, so replay
/// is idempotent. Rows are applied in file (= booking) order. Returns the
/// number of rows booked; an unparsable fill row is an error (never guess).
pub fn replay_fills<'a>(
    l: &mut Ledger,
    rows: impl IntoIterator<Item = &'a serde_json::Value>,
) -> Result<usize, String> {
    use std::str::FromStr;
    let mut booked = 0;
    for row in rows {
        if row.get("kind").and_then(|k| k.as_str()) != Some("fill")
            || row.get("mode").and_then(|m| m.as_str()) != Some(l.mode.as_str())
        {
            continue;
        }
        let text = |k: &str| {
            row.get(k)
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("fill row without {k}: {row}"))
        };
        let dec = |k: &str| {
            text(k).and_then(|v| Decimal::from_str(v).map_err(|e| format!("{k}={v}: {e}")))
        };
        let id = text("fill_id")?;
        let ts = row
            .get("ts_ms")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| format!("fill row without ts_ms: {row}"))?;
        if ts < l.last_booked_ts_ms || l.has_booked(id) {
            continue;
        }
        let buy = match text("side")? {
            "buy" => true,
            "sell" => false,
            other => return Err(format!("fill {id}: side {other}")),
        };
        let maker = text("role")? == "maker";
        l.record_fill(buy, dec("qty")?, dec("px")?, dec("fee")?, maker, ts);
        l.mark_booked(id, ts);
        booked += 1;
    }
    Ok(booked)
}

/// The connector may forget a fill only when it is booked (or was already)
/// AND the state holding that booking reached disk; otherwise it stays in
/// the connector and the next tick retries.
pub fn may_forget_fill(booking: &std::io::Result<Booking>, state_persisted: bool) -> bool {
    booking.is_ok() && state_persisted
}

/// Append one JSON line and fsync it.
pub fn append_synced(path: &std::path::Path, row: &serde_json::Value) -> std::io::Result<()> {
    use std::io::Write as _;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(f, "{row}")?;
    f.sync_all()
}

/// A fill waiting for its +5/+30/+60 s markouts.
#[derive(Debug, Clone)]
pub struct PendingMarkout {
    pub fill_id: String,
    pub ts_ms: u64,
    pub px: Decimal,
    pub buy: bool,
    pub horizons: Vec<u64>,
}

/// Markout in bp from the fill's side: positive = the mid moved our way.
pub fn markout_bps(buy: bool, px: Decimal, mid: Decimal) -> Decimal {
    if px.is_zero() {
        return Decimal::ZERO;
    }
    let sign = if buy {
        Decimal::ONE
    } else {
        Decimal::NEGATIVE_ONE
    };
    sign * (mid - px) / px * Decimal::from(10_000)
}

/// Pop every horizon that is due; returns (fill_id, horizon_secs, bps).
pub fn due_markouts(
    pending: &mut Vec<PendingMarkout>,
    now_ms: u64,
    mid: Decimal,
) -> Vec<(String, u64, Decimal)> {
    let mut out = Vec::new();
    for p in pending.iter_mut() {
        p.horizons.retain(|h| {
            if now_ms >= p.ts_ms + h * 1_000 {
                out.push((p.fill_id.clone(), *h, markout_bps(p.buy, p.px, mid)));
                false
            } else {
                true
            }
        });
    }
    pending.retain(|p| !p.horizons.is_empty());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn position_averages_realizes_and_flips_with_non_binary_prices() {
        let mut p = Position::default();
        assert_eq!(p.apply(true, d("0.1"), d("83642.9"), 1), Decimal::ZERO);
        assert_eq!(p.apply(true, d("0.2"), d("83643.1"), 2), Decimal::ZERO);
        // avg = (0.1*83642.9 + 0.2*83643.1)/0.3 = 83643.0333…
        assert_eq!(p.avg_px.round_dp(4), d("83643.0333"));
        assert_eq!(p.opened_at_ms, Some(1));
        // sell 0.1 at 83643.3: realized 0.1 * (83643.3 − 83643.0333…) = 0.02666…
        let r = p.apply(false, d("0.1"), d("83643.3"), 3);
        assert_eq!(r.round_dp(5), d("0.02667"));
        assert_eq!(p.qty, d("0.2"));
        assert_eq!(p.opened_at_ms, Some(1));
        // sell 0.3: closes 0.2, flips 0.1 short at 83642.7
        let r = p.apply(false, d("0.3"), d("83642.7"), 4);
        assert_eq!(r.round_dp(5), d("-0.06667"));
        assert_eq!(p.qty, d("-0.1"));
        assert_eq!(p.avg_px, d("83642.7"));
        assert_eq!(p.opened_at_ms, Some(4));
        // buy back 0.1 at 83642.6: short gains 0.01
        assert_eq!(p.apply(true, d("0.1"), d("83642.6"), 5), d("0.01"));
        assert_eq!(p, Position::default());
    }

    #[test]
    fn ledger_nets_fees_and_splits_maker_taker_volume() {
        let mut l = Ledger::new("dry_run", "2026-09-30");
        l.record_fill(true, d("0.1"), d("83642.9"), Decimal::ZERO, true, 1);
        l.record_fill(false, d("0.1"), d("83642.8"), d("1.882"), false, 2);
        assert_eq!(l.cum_maker_volume, d("8364.29"));
        assert_eq!(l.cum_taker_volume, d("8364.28"));
        // realized −0.01, fees 1.882
        assert_eq!(l.cum_net(d("83642.8")), d("-1.892"));
        assert_eq!(l.daily_net(d("83642.8")), d("-1.892"));
        let cost = l.cost_per_million(d("83642.8")).unwrap();
        assert_eq!(cost.round_dp(2), d("113.10"));
    }

    #[test]
    fn rollover_resets_the_day_and_rebases_carried_unrealized() {
        let mut l = Ledger::new("dry_run", "2026-09-30");
        l.record_fill(true, d("0.1"), d("100000"), d("2"), false, 1);
        l.day_halt = true;
        // mark 99990: unrealized −1
        assert!(!l.rollover("2026-09-30", d("99990")));
        assert!(l.rollover("2026-10-01", d("99990")));
        assert!(!l.day_halt);
        assert_eq!(l.day_fees, Decimal::ZERO);
        assert_eq!(l.day_start_unrealized, d("-1"));
        assert_eq!(l.daily_net(d("99990")), Decimal::ZERO);
        assert_eq!(l.daily_net(d("99980")), d("-1"));
        // cumulative keeps everything: −2 fee −2 unrealized
        assert_eq!(l.cum_net(d("99980")), d("-4"));
    }

    #[test]
    fn daily_stop_halts_until_rollover_and_cum_stop_is_sticky() {
        let (daily, cum) = (d("50"), d("250"));
        let mut l = Ledger::new("dry_run", "2026-09-30");
        l.record_fill(true, d("1"), d("100000"), Decimal::ZERO, true, 1);
        // −49.9: nothing
        assert_eq!(
            risk_check(&mut l, d("99950.1"), false, false, daily, cum).halt,
            None
        );
        // −60: day halt
        let o = risk_check(&mut l, d("99940"), false, false, daily, cum);
        assert_eq!(o.halt, Some(Halt::Day));
        assert_eq!(o.events.len(), 1);
        // recovering intraday does not lift it
        assert_eq!(
            risk_check(&mut l, d("100000"), false, false, daily, cum).halt,
            Some(Halt::Day)
        );
        l.rollover("2026-10-01", d("100000"));
        assert_eq!(
            risk_check(&mut l, d("100000"), false, false, daily, cum).halt,
            None
        );
        // kill switch outranks a day halt
        assert_eq!(
            risk_check(&mut l, d("100000"), true, false, daily, cum).halt,
            Some(Halt::Kill)
        );
        // −300 cumulative: sticky, survives rollover and a recovery
        let o = risk_check(&mut l, d("99700"), false, false, daily, cum);
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
        assert!(o.write_halt_file);
        l.rollover("2026-10-02", d("100000"));
        assert!(matches!(
            risk_check(&mut l, d("100000"), false, true, daily, cum).halt,
            Some(Halt::Sticky(_))
        ));
    }

    #[test]
    fn halt_file_halts_whatever_state_says_and_is_recreated_when_missing() {
        let (daily, cum) = (d("1000"), d("250"));
        let mut l = Ledger::new("dry_run", "2026-09-30");
        // A HALT file present at load halts a clean state.
        let o = risk_check(&mut l, d("100000"), false, true, daily, cum);
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
        assert!(l.sticky_halt);
        assert!(!o.write_halt_file);
        // File deleted while state is still sticky: stays halted, file recreated.
        let o = risk_check(&mut l, d("100000"), false, false, daily, cum);
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
        assert!(o.write_halt_file);
        // State cleared but the file still there: still halted.
        l.sticky_halt = false;
        let o = risk_check(&mut l, d("100000"), false, true, daily, cum);
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
    }

    #[test]
    fn clearing_needs_file_and_state_and_rebases_the_cumulative_stop() {
        let (daily, cum) = (d("1000"), d("250"));
        let mut l = Ledger::new("dry_run", "2026-09-30");
        l.record_fill(true, d("1"), d("100000"), Decimal::ZERO, true, 1);
        let o = risk_check(&mut l, d("99700"), false, false, daily, cum);
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
        assert!(o.write_halt_file);
        // Hand clear: file deleted AND state edited → cleared, baseline −300.
        l.sticky_halt = false;
        let o = risk_check(&mut l, d("99700"), false, false, daily, cum);
        assert_eq!(o.halt, None);
        assert_eq!(o.events.len(), 1);
        assert!(!o.write_halt_file);
        assert_eq!(l.cum_baseline, d("-300"));
        // another −200 is inside the re-based stop, −260 is not
        assert_eq!(
            risk_check(&mut l, d("99500"), false, false, daily, cum).halt,
            None
        );
        assert!(matches!(
            risk_check(&mut l, d("99440"), false, false, daily, cum).halt,
            Some(Halt::Sticky(_))
        ));
    }

    fn fill_in(id: &str) -> FillIn {
        FillIn {
            trade_id: id.to_string(),
            buy: true,
            qty: d("0.1"),
            px: d("83642.9"),
            fee: Decimal::ZERO,
            maker: true,
            order_id: "o".to_string(),
        }
    }

    #[test]
    fn a_failed_fill_write_leaves_the_ledger_untouched_and_retries_count_once() {
        let mut l = Ledger::new("live", "2026-09-30");
        let err = book_fill(&mut l, &fill_in("t1"), 1, |_| {
            Err(std::io::Error::other("disk full"))
        });
        assert!(err.is_err());
        assert_eq!(l.fills, 0);
        assert!(l.position.qty.is_zero());
        assert!(!l.has_booked("t1"));
        let mut rows = Vec::new();
        let ok = book_fill(&mut l, &fill_in("t1"), 2, |r| {
            rows.push(r.clone());
            Ok(())
        })
        .unwrap();
        assert_eq!(ok, Booking::Booked(Decimal::ZERO));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["fill_id"], "t1");
        assert_eq!(rows[0]["inventory"], "0.1");
        // Retry after e.g. a failed state write: never counted again.
        let again = book_fill(&mut l, &fill_in("t1"), 3, |_| panic!("must not append")).unwrap();
        assert_eq!(again, Booking::AlreadyBooked);
        assert_eq!(l.fills, 1);
        assert_eq!(l.position.qty, d("0.1"));
    }

    #[test]
    fn a_fill_is_forgotten_only_after_booking_and_state_both_land() {
        let booked: std::io::Result<Booking> = Ok(Booking::Booked(Decimal::ZERO));
        let already: std::io::Result<Booking> = Ok(Booking::AlreadyBooked);
        let failed: std::io::Result<Booking> = Err(std::io::Error::other("disk"));
        assert!(may_forget_fill(&booked, true));
        assert!(may_forget_fill(&already, true));
        assert!(!may_forget_fill(&booked, false));
        assert!(!may_forget_fill(&already, false));
        assert!(!may_forget_fill(&failed, true));
    }

    #[test]
    fn booked_ids_survive_a_state_round_trip_and_are_bounded() {
        let mut l = Ledger::new("live", "2026-09-30");
        book_fill(&mut l, &fill_in("t1"), 1, |_| Ok(())).unwrap();
        let back: Ledger = serde_json::from_str(&serde_json::to_string(&l).unwrap()).unwrap();
        assert!(back.has_booked("t1"));
        let mut l = back;
        for i in 0..BOOKED_IDS_CAP {
            l.mark_booked(&format!("x{i}"), 1);
        }
        assert_eq!(l.booked_ids.len(), BOOKED_IDS_CAP);
        assert!(!l.has_booked("t1"));
    }

    #[test]
    fn replay_books_the_row_state_missed_exactly_once() {
        // t1 booked and persisted; t2 fsynced to fills.jsonl but the crash
        // came before state.json was written.
        let mut rows = Vec::new();
        let mut l = Ledger::new("live", "2026-09-30");
        book_fill(&mut l, &fill_in("t1"), 1_000, |r| {
            rows.push(r.clone());
            Ok(())
        })
        .unwrap();
        let state = l.clone();
        let mut t2 = fill_in("t2");
        t2.buy = false;
        t2.qty = d("0.04");
        t2.px = d("83643.1");
        t2.fee = d("0.75");
        t2.maker = false;
        book_fill(&mut l, &t2, 2_000, |r| {
            rows.push(r.clone());
            Ok(())
        })
        .unwrap();
        rows.push(serde_json::json!({"kind": "markout", "fill_id": "t2", "ts_ms": 7_000}));
        rows.push(
            serde_json::json!({"kind": "fill", "mode": "dry_run", "fill_id": "sim-1",
                                     "ts_ms": 3_000, "side": "buy", "qty": "1", "px": "1",
                                     "fee": "0", "role": "maker"}),
        );
        let mut restored = state;
        assert_eq!(replay_fills(&mut restored, &rows).unwrap(), 1);
        assert_eq!(restored.position, l.position);
        assert_eq!(restored.cum_fees, l.cum_fees);
        assert_eq!(restored.cum_taker_volume, l.cum_taker_volume);
        assert_eq!(restored.fills, 2);
        assert!(restored.has_booked("t2"));
        // Idempotent across repeated restarts.
        assert_eq!(replay_fills(&mut restored, &rows).unwrap(), 0);
        assert_eq!(restored.fills, 2);
    }

    #[test]
    fn replay_never_books_a_row_older_than_the_high_water_mark() {
        let mut l = Ledger::new("live", "2026-09-30");
        book_fill(&mut l, &fill_in("new"), 5_000, |_| Ok(())).unwrap();
        // An old row whose id aged out of booked_ids.
        let old = serde_json::json!({"kind": "fill", "mode": "live", "fill_id": "old",
                                     "ts_ms": 4_000, "side": "buy", "qty": "1", "px": "1",
                                     "fee": "0", "role": "maker"});
        assert_eq!(replay_fills(&mut l, [&old]).unwrap(), 0);
        assert_eq!(l.fills, 1);
        let bad = serde_json::json!({"kind": "fill", "mode": "live", "fill_id": "b",
                                     "ts_ms": 6_000, "side": "buy", "qty": "x", "px": "1",
                                     "fee": "0", "role": "maker"});
        assert!(replay_fills(&mut l, [&bad]).is_err());
    }

    #[test]
    fn append_synced_writes_one_line_per_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub/fills.jsonl");
        append_synced(&path, &serde_json::json!({"a": 1})).unwrap();
        append_synced(&path, &serde_json::json!({"a": 2})).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text, "{\"a\":1}\n{\"a\":2}\n");
    }

    #[test]
    fn markouts_are_signed_from_the_fill_side_and_pop_when_due() {
        assert_eq!(markout_bps(true, d("100000"), d("100010")), d("1"));
        assert_eq!(markout_bps(false, d("100000"), d("100010")), d("-1"));
        let mut pending = vec![PendingMarkout {
            fill_id: "f".into(),
            ts_ms: 1_000,
            px: d("100000"),
            buy: true,
            horizons: vec![5, 30, 60],
        }];
        assert!(due_markouts(&mut pending, 5_999, d("1")).is_empty());
        let out = due_markouts(&mut pending, 6_000, d("99990"));
        assert_eq!(out, vec![("f".to_string(), 5, d("-1"))]);
        due_markouts(&mut pending, 31_000, d("1"));
        assert_eq!(pending.len(), 1);
        due_markouts(&mut pending, 61_000, d("1"));
        assert!(pending.is_empty());
    }
}
