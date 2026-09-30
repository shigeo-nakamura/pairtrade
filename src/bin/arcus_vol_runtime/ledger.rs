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
//! Position mismatch: the ledger is never overwritten with the venue
//! quantity (see `logic::position_check`). A persistent difference sets a
//! sticky halt whose reason starts with "position_mismatch"; it pulls quotes
//! and does NOT auto-flatten. To recover (bot stopped): flatten or fix the
//! venue position by hand, set `position` in state.json to match the venue,
//! then clear the halt as above (delete `HALT`, `"sticky_halt": false`).
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
    /// The market this state belongs to (Codex P1, pairtrade#361); empty in
    /// state written before the binding, which then fails `check_market`.
    #[serde(default)]
    pub market: String,
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

    /// Start a new UTC day at `mark`, a price from a live book (Codex P2,
    /// pairtrade#361): with no mark the rollover is POSTPONED and the daily
    /// stop keeps using the previous day's baseline until one arrives; the
    /// entry price is never used as the mark.
    pub fn rollover(&mut self, today: &str, mark: Option<Decimal>) -> Rollover {
        if self.day == today {
            return Rollover::Same;
        }
        let Some(mark) = mark else {
            return Rollover::Postponed;
        };
        self.day = today.to_string();
        self.day_realized = Decimal::ZERO;
        self.day_fees = Decimal::ZERO;
        self.day_volume = Decimal::ZERO;
        self.day_maker_volume = Decimal::ZERO;
        self.day_taker_volume = Decimal::ZERO;
        self.day_start_unrealized = self.position.unrealized(mark);
        self.day_halt = false;
        Rollover::Rolled
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rollover {
    Same,
    Rolled,
    /// A new UTC day, but no live-book mark yet.
    Postponed,
}

/// Exclusive OS lock on `<dir>/runtime.lock`, held for the process lifetime
/// (Codex P1, pairtrade#361): two runtimes on one state dir would book the
/// same fills twice and fight over the venue. Taken before state.json is
/// read or the venue touched; a held lock is a startup error. Keep the
/// returned file alive (dropping it releases the lock).
pub fn acquire_state_lock(dir: &std::path::Path) -> Result<std::fs::File, String> {
    use fs2::FileExt;
    std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let path = dir.join("runtime.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| format!("open {}: {e}", path.display()))?;
    file.try_lock_exclusive().map_err(|e| {
        format!(
            "{} is locked by another arcus_vol_runtime ({e}); refusing to start",
            path.display()
        )
    })?;
    Ok(file)
}

/// Refuse a state dir that belongs to another market (Codex P1,
/// pairtrade#361): state.json must name `market`, and every fills.jsonl row
/// must carry `market` too. A row without the field (older runs) is only
/// accepted when state.json explicitly names this market; nothing is ever
/// isolated or merged silently.
pub fn check_market(
    state_market: Option<&str>,
    rows: &[serde_json::Value],
    market: &str,
) -> Result<(), String> {
    let hint = "use a separate ARCUS_VOL_STATE_DIR per market";
    if let Some(m) = state_market {
        if m != market {
            let owner = if m.is_empty() {
                "an unrecorded market (state.json predates market binding)"
            } else {
                m
            };
            return Err(format!(
                "state dir belongs to {owner}, not {market} (state.json); {hint}"
            ));
        }
    }
    let state_confirms = state_market == Some(market);
    for (i, row) in rows.iter().enumerate() {
        match row.get("market").and_then(|v| v.as_str()) {
            Some(m) if m == market => {}
            Some(m) => {
                return Err(format!(
                    "state dir belongs to {m}, not {market} (fills.jsonl line {}); {hint}",
                    i + 1
                ))
            }
            None if state_confirms => {}
            None => {
                return Err(format!(
                    "fills.jsonl line {} has no market and state.json does not confirm {market}; {hint}",
                    i + 1
                ))
            }
        }
    }
    Ok(())
}

/// Write `value` as JSON to `path` durably: temp file, fsync, rename,
/// fsync the directory.
pub fn persist_durable<T: Serialize>(path: &std::path::Path, value: &T) -> std::io::Result<()> {
    use std::io::Write as _;
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "state".to_string());
    let tmp = dir.join(format!(".{name}.tmp.{}", std::process::id()));
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(serde_json::to_string_pretty(value)?.as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(dir)?.sync_all()
}

/// The state a runtime starts from.
#[derive(Debug)]
pub struct OpenedState {
    pub ledger: Ledger,
    pub repair: JournalRepair,
    pub replayed: usize,
}

/// Open (or create) the state in `dir` for `mode` / `market`, BEFORE any
/// venue contact (the caller holds `runtime.lock`):
/// 1. load state.json (a mode mismatch is an error);
/// 2. repair and read fills.jsonl;
/// 3. check that state AND every journal row belong to `market`;
/// 4. only then create a fresh ledger for `market` if there was none, book
///    any journal rows state missed, and persist state.json durably
///    (Codex P1, pairtrade#361), so market ownership is on disk before the
///    first order can exist. Writing state before step 3 would let a crash
///    leave a state.json that vouches for foreign journal rows.
pub fn open_state(
    dir: &std::path::Path,
    mode: &str,
    market: &str,
    today: &str,
) -> Result<OpenedState, String> {
    let state_path = dir.join("state.json");
    let fills_path = dir.join("fills.jsonl");
    let loaded =
        debot::directional::load_json::<Ledger>(&state_path).map_err(|e| format!("{e:#}"))?;
    if let Some(l) = &loaded {
        if l.mode != mode {
            return Err(format!(
                "{} was written in mode {} but this run is {mode}; move it aside first",
                state_path.display(),
                l.mode
            ));
        }
    }
    let state_market = loaded.as_ref().map(|l| l.market.clone());
    let repair = repair_journal(&fills_path)
        .map_err(|e| format!("repair {} at startup: {e}", fills_path.display()))?;
    let rows = match read_journal(&fills_path)
        .map_err(|e| format!("read {} at startup: {e}", fills_path.display()))?
    {
        None => Vec::new(),
        Some(text) => text
            .lines()
            .filter(|line| !line.is_empty())
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("fills.jsonl row after repair: {e}"))?,
    };
    check_market(state_market.as_deref(), &rows, market)?;
    let fresh = loaded.is_none();
    let mut ledger = loaded.unwrap_or_else(|| {
        let mut l = Ledger::new(mode, today);
        l.market = market.to_string();
        l
    });
    let replayed =
        replay_fills(&mut ledger, &rows).map_err(|e| format!("fills.jsonl replay: {e}"))?;
    if fresh || replayed > 0 {
        persist_durable(&state_path, &ledger)
            .map_err(|e| format!("persist {}: {e}", state_path.display()))?;
    }
    Ok(OpenedState {
        ledger,
        repair,
        replayed,
    })
}

/// What `repair_journal` did to fills.jsonl.
#[derive(Debug, Clone, PartialEq)]
pub enum JournalRepair {
    /// Missing, empty, or every line complete and parsable.
    Clean,
    /// The final line parsed but had no trailing newline: one was appended.
    NewlineAdded,
    /// A torn, unparsable final fragment was cut off.
    Truncated { dropped: usize, hex_preview: String },
}

/// Make fills.jsonl safe to append to (Codex P1, pairtrade#361). A crash
/// mid-append leaves a torn final fragment; appending after it would glue
/// the next row onto the fragment and lose that row at the next replay.
/// Run at startup before any append:
/// - no trailing newline and the fragment after the last `\n` does not parse
///   → truncate to just after that `\n`, fsync the file and its directory;
/// - no trailing newline but the final line parses → append the `\n`;
/// - any COMPLETE line that does not parse is an error (never truncated:
///   only a crash-torn tail is ours to drop).
pub fn repair_journal(path: &std::path::Path) -> std::io::Result<JournalRepair> {
    use std::io::Write as _;
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(JournalRepair::Clean),
        Err(e) => return Err(e),
    };
    let complete_end = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
    for (n, line) in bytes[..complete_end].split(|b| *b == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        if serde_json::from_slice::<serde_json::Value>(line).is_err() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} line {} is malformed and not a torn tail; refusing to start",
                    path.display(),
                    n + 1
                ),
            ));
        }
    }
    let tail = &bytes[complete_end..];
    if tail.is_empty() {
        return Ok(JournalRepair::Clean);
    }
    if serde_json::from_slice::<serde_json::Value>(tail).is_ok() {
        let mut f = std::fs::OpenOptions::new().append(true).open(path)?;
        f.write_all(b"\n")?;
        f.sync_all()?;
        return Ok(JournalRepair::NewlineAdded);
    }
    let hex_preview = tail
        .iter()
        .take(32)
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let f = std::fs::OpenOptions::new().write(true).open(path)?;
    f.set_len(complete_end as u64)?;
    f.sync_all()?;
    if let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(JournalRepair::Truncated {
        dropped: tail.len(),
        hex_preview,
    })
}

/// Read fills.jsonl at startup: only a missing file counts as empty
/// (Codex P2, pairtrade#361); any other error (permission, I/O, not UTF-8)
/// is returned so startup fails instead of replaying nothing.
pub fn read_journal(path: &std::path::Path) -> std::io::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
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
    /// The venue gave no fee and `fee` is the conservative taker estimate.
    pub fee_estimated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Booking {
    /// Already in `booked_ids`: nothing counted again.
    AlreadyBooked,
    /// Appended to fills.jsonl and booked; carries the realized PnL.
    Booked(Decimal),
}

impl Ledger {
    /// Sticky-halt on a persistent ledger/venue difference. Returns true when
    /// this call engaged it (the reason then starts with
    /// `logic::POSITION_MISMATCH`).
    pub fn halt_position_mismatch(&mut self, venue_qty: Decimal, mark: Decimal) -> bool {
        if self.sticky_halt {
            return false;
        }
        self.sticky_halt = true;
        self.halted_at_net.get_or_insert(self.cum_net(mark));
        self.sticky_reason = Some(format!(
            "{}: ledger {} ≠ venue {}",
            crate::logic::POSITION_MISMATCH,
            self.position.qty,
            venue_qty
        ));
        true
    }

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
        "market": l.market,
        "side": if f.buy { "buy" } else { "sell" },
        "px": f.px.to_string(),
        "qty": f.qty.to_string(),
        "notional": (f.qty * f.px).round_dp(4).to_string(),
        "role": if f.maker { "maker" } else { "taker" },
        "fee": f.fee.round_dp(6).to_string(),
        "realized": realized.round_dp(6).to_string(),
        "order_id": f.order_id,
        "fill_id": f.trade_id,
        "fee_estimated": f.fee_estimated,
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

/// Append one JSON line and fsync it. When this call CREATED the file, the
/// parent directory is fsynced too (Codex P2, pairtrade#361). A failed write
/// or fsync is rolled back (Codex P1, pairtrade#361): the file is truncated
/// to its length before the append and fsynced, then the error returned; if
/// that rollback fails the error carries `JournalUnsafe` (see
/// `is_journal_unsafe`), and the caller must stop appending until a restart
/// repairs the tail.
pub fn append_synced(path: &std::path::Path, row: &serde_json::Value) -> std::io::Result<()> {
    use std::io::Write as _;
    append_journal(
        path,
        row,
        |f, bytes| f.write_all(bytes),
        |f, len| f.set_len(len),
        |dir| std::fs::File::open(dir)?.sync_all(),
    )
}

/// The rollback of a failed append itself failed: the journal may end in a
/// partial row.
#[derive(Debug)]
pub struct JournalUnsafe(pub String);

impl std::fmt::Display for JournalUnsafe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "journal unsafe: {}", self.0)
    }
}

impl std::error::Error for JournalUnsafe {}

pub fn is_journal_unsafe(e: &std::io::Error) -> bool {
    e.get_ref().is_some_and(|inner| inner.is::<JournalUnsafe>())
}

/// `append_synced` with the write, truncate and directory sync injected
/// (test seams).
pub fn append_journal(
    path: &std::path::Path,
    row: &serde_json::Value,
    write: impl FnOnce(&mut std::fs::File, &[u8]) -> std::io::Result<()>,
    truncate: impl FnOnce(&std::fs::File, u64) -> std::io::Result<()>,
    sync_dir: impl FnOnce(&std::path::Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty());
    if let Some(dir) = dir {
        std::fs::create_dir_all(dir)?;
    }
    // Single writer (runtime.lock), so "existed" cannot race another append.
    let existed = path.exists();
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let len_before = f.metadata()?.len();
    let line = format!("{row}\n");
    if let Err(e) = write(&mut f, line.as_bytes()).and_then(|_| f.sync_all()) {
        return match truncate(&f, len_before).and_then(|_| f.sync_all()) {
            Ok(()) => Err(e),
            Err(rollback) => Err(std::io::Error::other(JournalUnsafe(format!(
                "append failed ({e}) and truncating {} back to {len_before} bytes failed ({rollback})",
                path.display()
            )))),
        };
    }
    if !existed {
        sync_dir(dir.unwrap_or(std::path::Path::new(".")))?;
    }
    Ok(())
}

/// What to do with a live fill whose fee the venue may not have reported.
#[derive(Debug, Clone, PartialEq)]
pub enum FeeDecision {
    /// Fee unknown and still inside the wait: keep it in the connector.
    Wait,
    Exact(Decimal),
    /// Fee still unknown after the wait (or at shutdown): book the taker fee on the notional
    /// whatever the role (never understated), marked `fee_estimated`.
    Estimated(Decimal),
}

/// How long a missing fee may be waited for: nothing at shutdown, since the
/// connector's fill cache dies with the process (Codex P1, pairtrade#361).
pub fn fee_wait_ms(shutting_down: bool, configured_ms: u64) -> u64 {
    if shutting_down {
        0
    } else {
        configured_ms
    }
}

/// A missing fee is never booked as zero (Codex P1, pairtrade#361).
pub fn fee_decision(
    fee: Option<Decimal>,
    first_seen_ms: u64,
    now_ms: u64,
    wait_ms: u64,
    notional: Decimal,
    taker_fee_bps: Decimal,
) -> FeeDecision {
    match fee {
        Some(f) => FeeDecision::Exact(f),
        None if now_ms.saturating_sub(first_seen_ms) < wait_ms => FeeDecision::Wait,
        None => FeeDecision::Estimated(notional.abs() * taker_fee_bps / Decimal::from(10_000)),
    }
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
        assert_eq!(l.rollover("2026-09-30", Some(d("99990"))), Rollover::Same);
        assert_eq!(l.rollover("2026-10-01", Some(d("99990"))), Rollover::Rolled);
        assert!(!l.day_halt);
        assert_eq!(l.day_fees, Decimal::ZERO);
        assert_eq!(l.day_start_unrealized, d("-1"));
        assert_eq!(l.daily_net(d("99990")), Decimal::ZERO);
        assert_eq!(l.daily_net(d("99980")), d("-1"));
        // cumulative keeps everything: −2 fee −2 unrealized
        assert_eq!(l.cum_net(d("99980")), d("-4"));
    }

    #[test]
    fn rollover_waits_for_a_live_mark_and_never_uses_the_entry_price() {
        let mut l = Ledger::new("live", "2026-09-30");
        l.record_fill(true, d("0.1"), d("100000"), d("2"), false, 1);
        l.day_halt = true;
        // New day, no book: postponed, yesterday's figures and halt stay.
        assert_eq!(l.rollover("2026-10-01", None), Rollover::Postponed);
        assert_eq!(l.day, "2026-09-30");
        assert!(l.day_halt);
        assert_eq!(l.day_fees, d("2"));
        // The book returns: roll at THAT mark (unrealized −1), not at entry.
        assert_eq!(l.rollover("2026-10-01", Some(d("99990"))), Rollover::Rolled);
        assert_eq!(l.day, "2026-10-01");
        assert_eq!(l.day_start_unrealized, d("-1"));
        assert!(!l.day_halt);
    }

    #[test]
    fn a_second_runtime_on_the_same_state_dir_fails_fast() {
        let dir = tempfile::tempdir().unwrap();
        let first = acquire_state_lock(dir.path()).unwrap();
        let err = acquire_state_lock(dir.path()).unwrap_err();
        assert!(err.contains("locked"), "{err}");
        drop(first);
        assert!(acquire_state_lock(dir.path()).is_ok());
    }

    const ROW: &str = r#"{"kind":"fill","mode":"live","fill_id":"t1","ts_ms":1000,"side":"buy","qty":"0.1","px":"83642.9","fee":"0","role":"maker"}"#;

    #[test]
    fn a_torn_final_fragment_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fills.jsonl");
        std::fs::write(&p, format!("{ROW}\n{{\"kind\":\"fi")).unwrap();
        let r = repair_journal(&p).unwrap();
        assert_eq!(
            r,
            JournalRepair::Truncated {
                // `{"kind":"fi` = 11 bytes
                dropped: 11,
                hex_preview: "7b226b696e64223a226669".to_string()
            }
        );
        assert_eq!(std::fs::read_to_string(&p).unwrap(), format!("{ROW}\n"));
        // Idempotent.
        assert_eq!(repair_journal(&p).unwrap(), JournalRepair::Clean);
    }

    #[test]
    fn a_malformed_line_that_is_not_the_torn_tail_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fills.jsonl");
        let text = format!("{ROW}\nnot json\n{ROW}\n");
        std::fs::write(&p, &text).unwrap();
        assert!(repair_journal(&p).is_err());
        assert_eq!(
            std::fs::read_to_string(&p).unwrap(),
            text,
            "never truncated"
        );
        // Also when a torn tail follows it.
        let text = format!("{ROW}\nnot json\n{{\"to");
        std::fs::write(&p, &text).unwrap();
        assert!(repair_journal(&p).is_err());
        assert_eq!(std::fs::read_to_string(&p).unwrap(), text);
    }

    #[test]
    fn a_complete_final_row_without_newline_gets_one() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fills.jsonl");
        std::fs::write(&p, format!("{ROW}\n{ROW}")).unwrap();
        assert_eq!(repair_journal(&p).unwrap(), JournalRepair::NewlineAdded);
        assert_eq!(
            std::fs::read_to_string(&p).unwrap(),
            format!("{ROW}\n{ROW}\n")
        );
        assert_eq!(
            repair_journal(&dir.path().join("missing")).unwrap(),
            JournalRepair::Clean
        );
    }

    #[test]
    fn crash_torn_tail_then_restart_then_append_is_replayed() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fills.jsonl");
        // Crash mid-append of a second row.
        std::fs::write(&p, format!("{ROW}\n{{\"kind\":\"fill\",\"mode\":\"li")).unwrap();
        // Restart: repair, then a new fill is appended.
        repair_journal(&p).unwrap();
        let new_row = serde_json::json!({"kind": "fill", "mode": "live", "fill_id": "t2",
            "ts_ms": 2000, "side": "sell", "qty": "0.1", "px": "83643.1", "fee": "0",
            "role": "maker"});
        append_synced(&p, &new_row).unwrap();
        // Next restart: every line parses and replay books both rows.
        let text = std::fs::read_to_string(&p).unwrap();
        let rows: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).expect("every line parses"))
            .collect();
        assert_eq!(rows.len(), 2);
        let mut l = Ledger::new("live", "2026-09-30");
        assert_eq!(replay_fills(&mut l, &rows).unwrap(), 2);
        assert!(l.has_booked("t2"));
        assert!(l.position.qty.is_zero());
    }

    #[test]
    fn a_state_or_journal_from_another_market_stops_startup() {
        let btc = serde_json::json!({"kind": "fill", "market": "BTC-USD"});
        let eth = serde_json::json!({"kind": "fill", "market": "ETH-USD"});
        let bare = serde_json::json!({"kind": "fill"});
        assert!(check_market(Some("BTC-USD"), std::slice::from_ref(&btc), "BTC-USD").is_ok());
        assert!(check_market(None, &[], "BTC-USD").is_ok());
        // state.json for another market
        let err = check_market(Some("ETH-USD"), &[], "BTC-USD").unwrap_err();
        assert!(err.contains("belongs to ETH-USD"), "{err}");
        // pre-binding state (no market recorded): fail closed
        assert!(check_market(Some(""), std::slice::from_ref(&btc), "BTC-USD").is_err());
        // a journal row for another market, even with a matching state
        let err = check_market(Some("BTC-USD"), &[btc.clone(), eth], "BTC-USD").unwrap_err();
        assert!(
            err.contains("belongs to ETH-USD") && err.contains("line 2"),
            "{err}"
        );
        // rows without a market: only when state.json confirms the market
        assert!(check_market(Some("BTC-USD"), std::slice::from_ref(&bare), "BTC-USD").is_ok());
        assert!(check_market(None, &[bare], "BTC-USD").is_err());
    }

    #[test]
    fn fill_rows_carry_the_ledger_market() {
        let mut l = Ledger::new("live", "2026-09-30");
        l.market = "BTC-USD".to_string();
        let mut rows = Vec::new();
        book_fill(&mut l, &fill_in("t1"), 1, |r| {
            rows.push(r.clone());
            Ok(())
        })
        .unwrap();
        assert_eq!(rows[0]["market"], "BTC-USD");
        let back: Ledger = serde_json::from_str(&serde_json::to_string(&l).unwrap()).unwrap();
        assert_eq!(back.market, "BTC-USD");
    }

    #[test]
    fn a_fresh_state_dir_is_bound_to_its_market_on_disk_before_any_venue_call() {
        let dir = tempfile::tempdir().unwrap();
        let opened = open_state(dir.path(), "live", "BTC-USD", "2026-09-30").unwrap();
        assert_eq!(opened.ledger.market, "BTC-USD");
        let on_disk: Ledger =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join("state.json")).unwrap())
                .unwrap();
        assert_eq!(on_disk.market, "BTC-USD");
        assert_eq!(on_disk.mode, "live");
        // A restart for another market is now refused.
        let err = open_state(dir.path(), "live", "ETH-USD", "2026-09-30").unwrap_err();
        assert!(err.contains("belongs to BTC-USD"), "{err}");
        // Another mode too.
        assert!(open_state(dir.path(), "dry_run", "BTC-USD", "2026-09-30").is_err());
    }

    #[test]
    fn a_foreign_journal_is_refused_before_any_state_is_written() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("fills.jsonl"),
            "{\"kind\":\"markout\",\"market\":\"ETH-USD\"}\n",
        )
        .unwrap();
        assert!(open_state(dir.path(), "live", "BTC-USD", "2026-09-30").is_err());
        assert!(
            !dir.path().join("state.json").exists(),
            "no state vouching for it"
        );
    }

    #[test]
    fn a_partial_markout_write_is_rolled_back() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fills.jsonl");
        append_synced(&path, &serde_json::json!({"kind": "fill", "fill_id": "t1"})).unwrap();
        let before = std::fs::read(&path).unwrap();
        let markout = serde_json::json!({"kind": "markout", "fill_id": "t1", "horizon_s": 5,
                                         "bps": "-0.5", "mid": "83642.9", "market": "BTC-USD"});
        let err = append_journal(
            &path,
            &markout,
            |f, b| {
                f.write_all(&b[..9])?;
                Err(std::io::Error::other("ENOSPC"))
            },
            |f, l| f.set_len(l),
            |_| Ok(()),
        );
        assert!(err.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(repair_journal(&path).unwrap(), JournalRepair::Clean);
    }

    #[test]
    fn the_runtime_never_appends_to_the_journal_without_rollback() {
        // Every fills.jsonl append must go through append_synced /
        // append_journal (Codex P1, pairtrade#361).
        let main = include_str!("main.rs");
        assert!(
            !main.contains("append_jsonl"),
            "use append_synced for fills.jsonl"
        );
    }

    #[test]
    fn at_shutdown_a_fee_pending_fill_is_booked_at_once_with_the_taker_fee() {
        let bps = d("2.25");
        let wait = fee_wait_ms(true, 30_000);
        assert_eq!(wait, 0);
        // First seen during the shutdown harvest itself.
        assert_eq!(
            fee_decision(None, 5_000, 5_000, wait, d("10000"), bps),
            FeeDecision::Estimated(d("2.25"))
        );
        assert_eq!(fee_wait_ms(false, 30_000), 30_000);
    }

    #[test]
    fn only_a_missing_journal_counts_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_journal(&dir.path().join("fills.jsonl"))
            .unwrap()
            .is_none());
        std::fs::write(dir.path().join("ok.jsonl"), "{}\n").unwrap();
        assert_eq!(
            read_journal(&dir.path().join("ok.jsonl"))
                .unwrap()
                .as_deref(),
            Some("{}\n")
        );
        // Unreadable (a directory where the file should be): an error, not "empty".
        std::fs::create_dir(dir.path().join("bad.jsonl")).unwrap();
        assert!(read_journal(&dir.path().join("bad.jsonl")).is_err());
        // Not UTF-8: an error too.
        std::fs::write(dir.path().join("bin.jsonl"), [0xff, 0xfe]).unwrap();
        assert!(read_journal(&dir.path().join("bin.jsonl")).is_err());
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
        l.rollover("2026-10-01", Some(d("100000")));
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
        l.rollover("2026-10-02", Some(d("100000")));
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
            fee_estimated: false,
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
    fn a_position_mismatch_sticky_halts_without_touching_the_position() {
        let mut l = Ledger::new("live", "2026-09-30");
        book_fill(&mut l, &fill_in("t1"), 1, |_| Ok(())).unwrap();
        assert!(l.halt_position_mismatch(d("0.3"), d("83642.9")));
        assert!(l.sticky_halt);
        assert_eq!(l.position.qty, d("0.1"));
        assert!(l
            .sticky_reason
            .as_deref()
            .unwrap()
            .starts_with(crate::logic::POSITION_MISMATCH));
        // A sticky halt already in force is left alone.
        assert!(!l.halt_position_mismatch(d("0.5"), d("83642.9")));
        // risk_check keeps it and asks for the HALT file.
        let o = risk_check(&mut l, d("83642.9"), false, false, d("1000"), d("1000"));
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
        assert!(o.write_halt_file);
    }

    fn write_all(f: &mut std::fs::File, b: &[u8]) -> std::io::Result<()> {
        use std::io::Write as _;
        f.write_all(b)
    }

    #[test]
    fn creating_the_journal_fsyncs_its_directory_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fills.jsonl");
        let mut synced = Vec::new();
        append_journal(
            &path,
            &serde_json::json!({"a": 1}),
            write_all,
            |f, l| f.set_len(l),
            |d| {
                synced.push(d.to_path_buf());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(synced, vec![dir.path().to_path_buf()]);
        // Appending to an existing file does not.
        append_journal(
            &path,
            &serde_json::json!({"a": 2}),
            write_all,
            |f, l| f.set_len(l),
            |_| panic!("no dir sync for an existing file"),
        )
        .unwrap();
        // A failed directory sync is not reported as durable.
        let other = dir.path().join("other.jsonl");
        assert!(append_journal(
            &other,
            &serde_json::json!({}),
            write_all,
            |f, l| f.set_len(l),
            |_| { Err(std::io::Error::other("dir sync")) }
        )
        .is_err());
    }

    #[test]
    fn a_partial_append_is_rolled_back_before_the_error_returns() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fills.jsonl");
        append_synced(&path, &serde_json::json!({"a": 1})).unwrap();
        let before = std::fs::read(&path).unwrap();
        // The writer gets 7 bytes out, then fails.
        let err = append_journal(
            &path,
            &serde_json::json!({"kind": "fill", "fill_id": "t2"}),
            |f, b| {
                f.write_all(&b[..7])?;
                Err(std::io::Error::other("disk full"))
            },
            |f, l| f.set_len(l),
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(!is_journal_unsafe(&err));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "partial bytes rolled back"
        );
        // The next append lands on a clean line boundary.
        append_synced(&path, &serde_json::json!({"a": 3})).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\"a\":1}\n{\"a\":3}\n"
        );
    }

    #[test]
    fn a_failed_rollback_marks_the_journal_unsafe() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fills.jsonl");
        append_synced(&path, &serde_json::json!({"a": 1})).unwrap();
        let err = append_journal(
            &path,
            &serde_json::json!({"a": 2}),
            |f, b| {
                f.write_all(&b[..3])?;
                Err(std::io::Error::other("disk full"))
            },
            |_, _| Err(std::io::Error::other("truncate failed")),
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(is_journal_unsafe(&err), "{err}");
    }

    #[test]
    fn a_missing_fee_waits_then_books_the_taker_fee_never_zero() {
        let (wait, bps) = (30_000, d("2.25"));
        let notional = d("8364.29");
        // Unknown, inside the wait: keep it un-booked.
        assert_eq!(
            fee_decision(None, 1_000, 30_999, wait, notional, bps),
            FeeDecision::Wait
        );
        // Known later: the exact fee (maker 0 is fine when the venue says so).
        assert_eq!(
            fee_decision(Some(Decimal::ZERO), 1_000, 5_000, wait, notional, bps),
            FeeDecision::Exact(Decimal::ZERO)
        );
        // Timed out: the taker fee on the notional, whatever the role.
        assert_eq!(
            fee_decision(None, 1_000, 31_000, wait, notional, bps),
            FeeDecision::Estimated(d("1.88196525"))
        );
        // The estimated row says so.
        let mut l = Ledger::new("live", "2026-09-30");
        let mut f = fill_in("t9");
        f.fee = d("1.88196525");
        f.fee_estimated = true;
        let mut rows = Vec::new();
        book_fill(&mut l, &f, 1, |r| {
            rows.push(r.clone());
            Ok(())
        })
        .unwrap();
        assert_eq!(rows[0]["fee_estimated"], true);
        assert_eq!(l.cum_fees, d("1.88196525"));
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
