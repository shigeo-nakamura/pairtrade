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
    /// The mark the current day was rolled at (`day_start_unrealized`'s
    /// price); `None` before the first rollover / in older state.
    #[serde(default)]
    pub day_start_mark: Option<Decimal>,
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
    /// Next fills.jsonl row sequence number (Codex P2, pairtrade#361): every
    /// journal row carries a strictly increasing `seq`, so replay order and
    /// the "already booked" boundary never depend on clocks.
    #[serde(default)]
    pub next_seq: u64,
    /// `seq` of the newest booked fill row: replay never books a row at or
    /// below it, so an id aged out of `booked_ids` cannot be booked twice.
    #[serde(default)]
    pub last_booked_seq: Option<u64>,
}

/// Which day's counters a booking touches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayScope {
    /// The ledger's current UTC day (the normal case).
    Current,
    /// A fill from a UTC day the ledger has already rolled past (a spilled
    /// fill resolved after a restart across midnight): position and
    /// cumulative counters only, a late adjustment.
    CumulativeOnly,
}

/// The UTC day (YYYY-MM-DD) of an epoch-ms timestamp.
pub fn utc_day_of(ts_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ts_ms as i64)
        .map(|t| t.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

/// Day scope of a fill timed `fill_ts_ms` against the ledger's day: a fill
/// from before the ledger's day can only be a late cumulative adjustment.
pub fn day_scope(ledger_day: &str, fill_ts_ms: u64) -> DayScope {
    if utc_day_of(fill_ts_ms).as_str() < ledger_day {
        DayScope::CumulativeOnly
    } else {
        DayScope::Current
    }
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

    #[cfg(test)]
    pub fn record_fill(
        &mut self,
        buy: bool,
        qty: Decimal,
        px: Decimal,
        fee: Decimal,
        maker: bool,
        now_ms: u64,
    ) -> Decimal {
        self.record_fill_scoped(buy, qty, px, fee, maker, now_ms, DayScope::Current)
    }

    /// `record_fill`, where a fill from an already-rolled UTC day
    /// (`CumulativeOnly`) moves the position and the cumulative counters but
    /// never today's day_* counters (pre-G2, Codex P1 4152482567).
    #[allow(clippy::too_many_arguments)]
    pub fn record_fill_scoped(
        &mut self,
        buy: bool,
        qty: Decimal,
        px: Decimal,
        fee: Decimal,
        maker: bool,
        now_ms: u64,
        scope: DayScope,
    ) -> Decimal {
        // A late prior-day fill changes today's starting position: rebase the
        // day-start unrealized by the position's before/after value at the
        // rollover mark, so booking it doesn't move today's daily_net (pre-G2,
        // Codex P1 4152541373). Without a recorded mark, the fill price
        // (unrealized 0 at entry) is the neutral fallback.
        let rebase_mark =
            (scope == DayScope::CumulativeOnly).then(|| self.day_start_mark.unwrap_or(px));
        let before = rebase_mark.map(|m| self.position.unrealized(m));
        let realized = self.position.apply(buy, qty, px, now_ms);
        if let (Some(m), Some(b)) = (rebase_mark, before) {
            self.day_start_unrealized += self.position.unrealized(m) - b;
        }
        let notional = qty * px;
        let today = scope == DayScope::Current;
        self.cum_realized += realized;
        self.cum_fees += fee;
        self.cum_volume += notional;
        if today {
            self.day_realized += realized;
            self.day_fees += fee;
            self.day_volume += notional;
        }
        if maker {
            self.cum_maker_volume += notional;
            if today {
                self.day_maker_volume += notional;
            }
        } else {
            self.cum_taker_volume += notional;
            if today {
                self.day_taker_volume += notional;
            }
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
        self.day_start_mark = Some(mark);
        self.day_halt = false;
        Rollover::Rolled
    }

    pub fn daily_net(&self, mark: Decimal) -> Decimal {
        self.day_realized - self.day_fees + self.position.unrealized(mark)
            - self.day_start_unrealized
    }

    /// `daily_net` for risk (Codex P2, pairtrade#361): with no fresh mark the
    /// unrealized part is deferred (realized − fees only); never priced at
    /// the entry price.
    pub fn daily_net_at(&self, mark: Option<Decimal>) -> Decimal {
        match mark {
            Some(m) => self.daily_net(m),
            None => self.day_realized - self.day_fees,
        }
    }

    /// `cum_net` for risk; see `daily_net_at`.
    pub fn cum_net_at(&self, mark: Option<Decimal>) -> Decimal {
        match mark {
            Some(m) => self.cum_net(m),
            None => self.cum_realized - self.cum_fees,
        }
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

/// The one guard every fill-booking path goes through (Codex P2,
/// pairtrade#361): roll the UTC day if due and have that state on disk
/// before anything books. Returns whether booking may proceed now:
/// - same day and nothing pending → yes;
/// - rolled and persisted (or an earlier failed persist now succeeds) → yes;
/// - rollover postponed (no fresh mark yet) or the persist failed → no, and
///   the caller books nothing (live fills stay in the connector; a paper
///   print is dropped, the conservative direction).
///
/// `pending_persist` carries a failed persist to the next call.
pub fn ensure_rolled(
    l: &mut Ledger,
    today: &str,
    mark: Option<Decimal>,
    pending_persist: &mut bool,
    persist: impl FnOnce(&Ledger) -> std::io::Result<()>,
) -> Result<bool, String> {
    match l.rollover(today, mark) {
        Rollover::Postponed => Err(format!("rollover to {today} postponed: no fresh mark yet")),
        Rollover::Same if !*pending_persist => Ok(true),
        Rollover::Rolled | Rollover::Same => match persist(l) {
            Ok(()) => {
                *pending_persist = false;
                Ok(true)
            }
            Err(e) => {
                *pending_persist = true;
                Err(format!("rollover state not persisted: {e}"))
            }
        },
    }
}

/// Exclusive OS lock on `<dir>/runtime.lock`, held for the process lifetime
/// (Codex P1, pairtrade#361): two runtimes on one state dir would book the
/// same fills twice and fight over the venue. Taken before state.json is
/// read or the venue touched; a held lock is a startup error. Keep the
/// returned file alive (dropping it releases the lock).
pub fn acquire_state_lock(dir: &std::path::Path) -> Result<std::fs::File, String> {
    acquire_lock_at(&dir.join("runtime.lock"))
}

/// The account-wide lock path (Codex P1, pairtrade#361), in a namespace that
/// does not depend on the state dir: two runtimes with different state dirs
/// must not both trade one Arcus subaccount (the cap and the stray-order
/// cancel are account-wide). Per account, not per market. DRY_RUN has no
/// account and sends no venue orders, so it takes no account lock (`None`);
/// its state dir lock is enough for a paper run.
pub fn account_lock_path(
    lock_dir: &std::path::Path,
    dry_run: bool,
    address: Option<&str>,
    account_index: Option<u8>,
) -> Result<Option<std::path::PathBuf>, String> {
    if dry_run {
        return Ok(None);
    }
    let address = address
        .map(|a| a.trim().to_ascii_lowercase())
        .filter(|a| !a.is_empty())
        .ok_or("live needs ARCUS_ADDRESS for the account lock")?;
    let index = account_index.ok_or("live needs ARCUS_ACCOUNT_INDEX for the account lock")?;
    Ok(Some(lock_dir.join(format!("arcus_{address}_{index}.lock"))))
}

/// Exclusive flock on `path` (created if missing), held while the returned
/// file lives; a held lock is an error.
pub fn acquire_lock_at(path: &std::path::Path) -> Result<std::fs::File, String> {
    use fs2::FileExt;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
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
    persist_durable_str(path, &serde_json::to_string_pretty(value)?)
}

/// `persist_durable` for already-serialized JSON.
pub fn persist_durable_str(path: &std::path::Path, json: &str) -> std::io::Result<()> {
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
    f.write_all(json.as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(dir)?.sync_all()
}

/// Writes state.json, always durably (Codex P2, pairtrade#361), and only
/// when its contents changed since the last successful write, so the
/// per-tick fsync costs nothing on quiet ticks. A failed write leaves the
/// cache untouched, so the next call retries.
#[derive(Debug, Default)]
pub struct StateWriter {
    last: Option<String>,
}

impl StateWriter {
    /// Returns whether a write happened.
    pub fn write(&mut self, path: &std::path::Path, ledger: &Ledger) -> std::io::Result<bool> {
        self.write_with(path, ledger, persist_durable_str)
    }

    pub fn write_with(
        &mut self,
        path: &std::path::Path,
        ledger: &Ledger,
        persist: impl FnOnce(&std::path::Path, &str) -> std::io::Result<()>,
    ) -> std::io::Result<bool> {
        let json = serde_json::to_string_pretty(ledger)?;
        if self.last.as_deref() == Some(json.as_str()) {
            return Ok(false);
        }
        persist(path, &json)?;
        self.last = Some(json);
        Ok(true)
    }
}

/// The state a runtime starts from.
#[derive(Debug)]
pub struct OpenedState {
    pub ledger: Ledger,
    pub repair: JournalRepair,
    pub replayed: usize,
    /// The journal rows, for restoring pending markouts.
    pub rows: Vec<serde_json::Value>,
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
    // Day and stop state live only in state.json; never rebuild them from a
    // journal (Codex P2, pairtrade#361).
    if loaded.is_none() && !rows.is_empty() {
        return Err(format!(
            "{} has {} row(s) but there is no state.json: journal without state; restore state.json or move the journal aside",
            fills_path.display(),
            rows.len()
        ));
    }
    let fresh = loaded.is_none();
    let mut ledger = loaded.unwrap_or_else(|| {
        let mut l = Ledger::new(mode, today);
        l.market = market.to_string();
        l
    });
    let replayed =
        replay_fills(&mut ledger, &rows).map_err(|e| format!("fills.jsonl replay: {e}"))?;
    // seq = max(state.next_seq, max row seq + 1): never reused.
    let mut seq_bumped = false;
    if let Some(max) = rows
        .iter()
        .filter_map(|r| r.get("seq").and_then(|v| v.as_u64()))
        .max()
    {
        if max + 1 > ledger.next_seq {
            ledger.next_seq = max + 1;
            seq_bumped = true;
        }
    }
    if fresh || replayed > 0 || seq_bumped {
        persist_durable(&state_path, &ledger)
            .map_err(|e| format!("persist {}: {e}", state_path.display()))?;
    }
    Ok(OpenedState {
        ledger,
        repair,
        replayed,
        rows,
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
    /// Inventory is open but there is no fresh mark: the stops ran on
    /// realized − fees only.
    pub unrealized_deferred: bool,
}

/// Update the stop flags from the current marks and return the halt in
/// force, if any (sticky > kill > day). `halt_file` is whether `HALT`
/// exists; see the module docs for how the file and `sticky_halt` interact.
pub fn risk_check(
    l: &mut Ledger,
    mark: Option<Decimal>,
    kill: bool,
    halt_file: bool,
    daily_stop: Decimal,
    cum_stop: Decimal,
) -> RiskOutcome {
    let mut events = Vec::new();
    let unrealized_deferred = mark.is_none() && !l.position.qty.is_zero();
    if !halt_file && !l.sticky_halt && l.halted_at_net.is_some() {
        // Both cleared by hand (file deleted AND state edited): re-base.
        l.halted_at_net = None;
        l.sticky_reason = None;
        l.cum_baseline = l.cum_net_at(mark);
        events.push(format!(
            "HALT cleared by hand; cumulative stop re-based at net {}",
            l.cum_baseline.round_dp(2)
        ));
    }
    if halt_file && !l.sticky_halt {
        l.sticky_halt = true;
        l.halted_at_net.get_or_insert(l.cum_net_at(mark));
        let reason = "HALT file present".to_string();
        events.push(format!("STICKY HALT: {reason}"));
        l.sticky_reason = Some(reason);
    }
    let cum_loss = l.cum_baseline - l.cum_net_at(mark);
    if !l.sticky_halt && cum_loss > cum_stop {
        l.sticky_halt = true;
        l.halted_at_net = Some(l.cum_net_at(mark));
        let reason = format!(
            "cumulative net loss {} > {} (since baseline)",
            cum_loss.round_dp(2),
            cum_stop
        );
        events.push(format!("STICKY HALT: {reason}"));
        l.sticky_reason = Some(reason);
    }
    let write_halt_file = l.sticky_halt && !halt_file;
    let day_loss = -l.daily_net_at(mark);
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
        unrealized_deferred,
    }
}

/// One fill to book (live from the connector, or simulated).
#[derive(Debug, Clone, PartialEq)]
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
    pub fn halt_position_mismatch(&mut self, venue_qty: Decimal, mark: Option<Decimal>) -> bool {
        if self.sticky_halt {
            return false;
        }
        self.sticky_halt = true;
        self.halted_at_net.get_or_insert(self.cum_net_at(mark));
        self.sticky_reason = Some(format!(
            "{}: ledger {} ≠ venue {}",
            crate::logic::POSITION_MISMATCH,
            self.position.qty,
            venue_qty
        ));
        true
    }

    /// Sticky halt with `reason` (no-op when one is already in force).
    /// Returns true when this call engaged it.
    pub fn halt_sticky(&mut self, reason: String, mark: Option<Decimal>) -> bool {
        if self.sticky_halt {
            return false;
        }
        self.sticky_halt = true;
        self.halted_at_net.get_or_insert(self.cum_net_at(mark));
        self.sticky_reason = Some(reason);
        true
    }

    pub fn has_booked(&self, trade_id: &str) -> bool {
        self.booked_ids.iter().any(|id| id == trade_id)
    }

    /// The `seq` for a non-fill journal row (markout, tape_gap).
    pub fn take_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    fn mark_booked(&mut self, trade_id: &str, seq: u64) {
        self.last_booked_seq = Some(self.last_booked_seq.map_or(seq, |l| l.max(seq)));
        self.next_seq = self.next_seq.max(seq + 1);
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
#[cfg(test)]
pub fn book_fill(
    l: &mut Ledger,
    f: &FillIn,
    now_ms: u64,
    append: impl FnOnce(&serde_json::Value) -> std::io::Result<()>,
) -> std::io::Result<Booking> {
    book_fill_at(l, f, now_ms, now_ms, append)
}

/// `book_fill` with the fill's own time (Codex P2, pairtrade#361): `ts_ms`
/// is when the fill happened (a paper fill: the print's venue timestamp) and
/// drives the row's `ts_ms`, the position open time (max-hold) and the
/// markout horizons; `rx_ms` is only when we processed it (`rx_ms` field).
pub fn book_fill_at(
    l: &mut Ledger,
    f: &FillIn,
    ts_ms: u64,
    rx_ms: u64,
    append: impl FnOnce(&serde_json::Value) -> std::io::Result<()>,
) -> std::io::Result<Booking> {
    book_fill_scoped(l, f, ts_ms, rx_ms, DayScope::Current, append)
}

/// `book_fill_at` with an explicit day scope; a `CumulativeOnly` row is
/// marked `late_prior_day` so replay keeps it out of the day counters too.
pub fn book_fill_scoped(
    l: &mut Ledger,
    f: &FillIn,
    ts_ms: u64,
    rx_ms: u64,
    scope: DayScope,
    append: impl FnOnce(&serde_json::Value) -> std::io::Result<()>,
) -> std::io::Result<Booking> {
    if l.has_booked(&f.trade_id) {
        return Ok(Booking::AlreadyBooked);
    }
    let mut preview = l.position.clone();
    let realized = preview.apply(f.buy, f.qty, f.px, ts_ms);
    let seq = l.next_seq;
    let row = serde_json::json!({
        "kind": "fill",
        "seq": seq,
        "ts_ms": ts_ms,
        "rx_ms": rx_ms,
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
        "late_prior_day": scope == DayScope::CumulativeOnly,
    });
    append(&row)?;
    let booked = l.record_fill_scoped(f.buy, f.qty, f.px, f.fee, f.maker, ts_ms, scope);
    l.mark_booked(&f.trade_id, seq);
    Ok(Booking::Booked(booked))
}

/// The markouts a just-booked fill owes, timed from the fill's own time.
pub fn markouts_for(f: &FillIn, fill_ts_ms: u64) -> PendingMarkout {
    PendingMarkout {
        fill_id: f.trade_id.clone(),
        ts_ms: fill_ts_ms,
        px: f.px,
        buy: f.buy,
        horizons: MARKOUT_HORIZONS.to_vec(),
    }
}

/// Book every fills.jsonl row the state does not have yet (Codex P1,
/// pairtrade#361): the row is fsynced before the ledger moves, so a crash
/// between that and the state write leaves a row state.json never saw.
/// Only `kind: fill` rows of this ledger's mode count; a row whose `seq` is
/// at or below `last_booked_seq`, or whose id is already booked, is skipped,
/// so replay is idempotent, by sequence and never by timestamp (Codex P2,
/// pairtrade#361). A fill row without `seq` predates the sequence and is an
/// error (fail closed). Returns the number of rows booked; an unparsable
/// fill row is an error (never guess).
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
        let seq = row.get("seq").and_then(|v| v.as_u64()).ok_or_else(|| {
            format!("fill row {id} has no seq (predates the journal sequence); move the state dir aside")
        })?;
        let ts = row
            .get("ts_ms")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| format!("fill row without ts_ms: {row}"))?;
        if l.last_booked_seq.is_some_and(|last| seq <= last) || l.has_booked(id) {
            continue;
        }
        let buy = match text("side")? {
            "buy" => true,
            "sell" => false,
            other => return Err(format!("fill {id}: side {other}")),
        };
        let maker = text("role")? == "maker";
        let scope = if row.get("late_prior_day").and_then(|v| v.as_bool()) == Some(true) {
            DayScope::CumulativeOnly
        } else {
            DayScope::Current
        };
        l.record_fill_scoped(buy, dec("qty")?, dec("px")?, dec("fee")?, maker, ts, scope);
        l.mark_booked(id, seq);
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
    // The directory entry must be durable before the first row is reported
    // durable. An empty file means no row has been yet, even if it exists:
    // a failed first append (rolled back to 0 bytes) left it behind, so the
    // next successful append still syncs the parent (pre-G2, Codex P2
    // 4146418683).
    if !existed || len_before == 0 {
        if let Err(e) = sync_dir(dir.unwrap_or(std::path::Path::new("."))) {
            // The row is written but not durably reachable: roll it back
            // like a failed write, so a retry can't append it twice
            // (pre-G2, Codex P2 4152482586).
            return match truncate(&f, len_before).and_then(|_| f.sync_all()) {
                Ok(()) => Err(e),
                Err(rollback) => Err(std::io::Error::other(JournalUnsafe(format!(
                    "directory sync failed ({e}) and truncating {} back to {len_before} bytes failed ({rollback})",
                    path.display()
                )))),
            };
        }
    }
    Ok(())
}

/// A connector fill record still unbooked when the runtime shut down
/// (pre-G2, Codex P1 4146269818). The connector's fill cache dies with the
/// process and its REST cursor starts at construction, so such a record
/// would be gone after a restart: it is written durably to
/// `pending_fills.jsonl` instead and resolved first at the next start.
/// Never dropped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingFill {
    pub trade_id: String,
    pub order_id: String,
    pub is_rejected: bool,
    /// `None` when the venue hadn't reported the side yet.
    pub buy: Option<bool>,
    pub size: Option<Decimal>,
    pub value: Option<Decimal>,
    pub fee: Option<Decimal>,
    /// Our own IOC (false) / quote (true) order, when known.
    pub maker_hint: Option<bool>,
    pub spilled_at_ms: u64,
    /// The venue's fill time, when it reported one.
    #[serde(default)]
    pub fill_ts_ms: Option<u64>,
}

impl PendingFill {
    pub fn from_record(
        f: &dex_connector::FilledOrder,
        maker_hint: Option<bool>,
        now_ms: u64,
    ) -> Self {
        Self {
            trade_id: f.trade_id.clone(),
            order_id: f.order_id.clone(),
            is_rejected: f.is_rejected,
            buy: f
                .filled_side
                .map(|s| matches!(s, dex_connector::OrderSide::Long)),
            size: f.filled_size,
            value: f.filled_value,
            fee: f.filled_fee,
            maker_hint,
            spilled_at_ms: now_ms,
            fill_ts_ms: f.filled_ts_ms.and_then(|t| u64::try_from(t).ok()),
        }
    }

    /// When the fill happened, as best known: the venue's time, else the
    /// spill time (it existed before the shutdown). Never the restart time
    /// (pre-G2, Codex P1 4152482567).
    pub fn fill_time_ms(&self) -> u64 {
        self.fill_ts_ms.unwrap_or(self.spilled_at_ms)
    }
}

/// The markouts a late-booked fill (timed `fill_ts_ms`, booked at `now_ms`)
/// still owes, and the horizons already too late to price: those are
/// `Missing` with reason `restart`, never priced at a later mid.
pub fn late_markouts(
    f: &FillIn,
    fill_ts_ms: u64,
    now_ms: u64,
    max_late_ms: u64,
) -> (Option<PendingMarkout>, Vec<Markout>) {
    let mut owed = Vec::new();
    let mut missing = Vec::new();
    for h in MARKOUT_HORIZONS {
        if now_ms > fill_ts_ms + h * 1_000 + max_late_ms {
            missing.push(Markout::Missing {
                fill_id: f.trade_id.clone(),
                horizon_s: h,
                reason: "restart".to_string(),
            });
        } else {
            owed.push(h);
        }
    }
    let pending = (!owed.is_empty()).then(|| PendingMarkout {
        fill_id: f.trade_id.clone(),
        ts_ms: fill_ts_ms,
        px: f.px,
        buy: f.buy,
        horizons: owed,
    });
    (pending, missing)
}

/// Durably append every unbooked record (same rollback-safe journal append
/// as fills.jsonl). An error means some record may not be on disk: the
/// caller logs it loudly; the process is exiting anyway.
pub fn spill_pending(path: &std::path::Path, pending: &[PendingFill]) -> std::io::Result<()> {
    for p in pending {
        let row = serde_json::to_value(p).map_err(std::io::Error::other)?;
        append_synced(path, &row)?;
    }
    Ok(())
}

/// Read `pending_fills.jsonl` strictly: missing = nothing pending; any
/// unparsable line is a startup error (never skipped: it may be a fill).
pub fn read_pending(path: &std::path::Path) -> Result<Vec<PendingFill>, String> {
    let Some(text) = read_journal(path).map_err(|e| format!("{}: {e}", path.display()))? else {
        return Ok(Vec::new());
    };
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, l)| {
            serde_json::from_str(l).map_err(|e| format!("{} line {}: {e}", path.display(), i + 1))
        })
        .collect()
}

/// Rewrite the pending file after a resolution pass (pre-G2, Codex P1
/// 4152482577): `done` (booked or cleared) are appended to `archive`, then
/// the pending file is replaced durably by exactly the `unresolved` records
/// (removed when none remain). An unresolved record therefore stays pending
/// across every restart until it resolves; a crash between the two writes
/// only leaves a done record pending, which dedupe makes harmless.
pub fn settle_pending(
    path: &std::path::Path,
    archive: &std::path::Path,
    done: &[PendingFill],
    unresolved: &[PendingFill],
) -> std::io::Result<()> {
    if !done.is_empty() {
        spill_pending(archive, done)?;
    }
    if unresolved.is_empty() {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let dir = path
            .parent()
            .filter(|d| !d.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        return std::fs::File::open(dir)?.sync_all();
    }
    let mut body = String::new();
    for p in unresolved {
        body.push_str(&serde_json::to_string(p).map_err(std::io::Error::other)?);
        body.push('\n');
    }
    persist_durable_str(path, &body)
}

/// How a spilled record is resolved at startup.
#[derive(Debug, Clone, PartialEq)]
pub enum PendingResolution {
    /// Complete: book it (dedupe by trade id makes a repeat harmless). A
    /// missing fee is the conservative taker estimate, marked.
    Book(FillIn),
    /// Confirmed rejected / zero-size.
    Clear,
    /// Still incomplete: it can't complete any more (the connector that held
    /// it is gone), so a human must resolve it: sticky halt.
    Halt(String),
}

pub fn resolve_pending(p: &PendingFill, taker_fee_bps: Decimal) -> PendingResolution {
    if p.is_rejected || p.size.is_some_and(|q| q.is_zero()) {
        return PendingResolution::Clear;
    }
    match (p.buy, p.size, p.value) {
        (Some(buy), Some(qty), Some(value)) if qty > Decimal::ZERO => {
            let (fee, fee_estimated) = match p.fee {
                Some(f) => (f, false),
                None => (value.abs() * taker_fee_bps / Decimal::from(10_000), true),
            };
            PendingResolution::Book(FillIn {
                trade_id: p.trade_id.clone(),
                buy,
                qty,
                px: value / qty,
                fee,
                maker: p.maker_hint.unwrap_or(fee <= Decimal::ZERO),
                order_id: p.order_id.clone(),
                fee_estimated,
            })
        }
        _ => PendingResolution::Halt(format!(
            "fill_incomplete: trade {} was still incomplete at the last shutdown (pending_fills.jsonl); resolve by hand",
            p.trade_id
        )),
    }
}

/// What to do with one connector fill record (Codex P1, pairtrade#361).
#[derive(Debug, Clone, PartialEq)]
pub enum FillRecordAction {
    Book {
        side: dex_connector::OrderSide,
        qty: Decimal,
        value: Decimal,
    },
    /// Confirmed rejected or zero-size: nothing to book, clear it.
    Clear,
    /// Missing side / size / value: keep it in the connector, retry.
    Pending,
    /// Still incomplete after the wait: sticky halt `fill_incomplete`.
    Halt,
}

/// Only an explicitly rejected or zero-size record is dropped; an
/// incomplete one is never guessed at or cleared (it may be a real fill):
/// it waits `wait_ms` from `first_seen_ms`, then escalates to a halt.
pub fn classify_fill_record(
    is_rejected: bool,
    side: Option<dex_connector::OrderSide>,
    size: Option<Decimal>,
    value: Option<Decimal>,
    first_seen_ms: u64,
    now_ms: u64,
    wait_ms: u64,
) -> FillRecordAction {
    if is_rejected || size.is_some_and(|q| q.is_zero()) {
        return FillRecordAction::Clear;
    }
    match (side, size, value) {
        (Some(side), Some(qty), Some(value)) => FillRecordAction::Book { side, qty, value },
        _ if now_ms.saturating_sub(first_seen_ms) < wait_ms => FillRecordAction::Pending,
        _ => FillRecordAction::Halt,
    }
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

/// Markout horizons (seconds after the fill).
pub const MARKOUT_HORIZONS: [u64; 3] = [5, 30, 60];

/// Rebuild pending markouts from the journal at startup (Codex P2,
/// pairtrade#361): for each fill row, every horizon without a `markout` /
/// `markout_missing` row for that `fill_id` + `horizon_s` is still owed.
/// Those still inside `max_late_ms` after they were due go back to pending;
/// older ones come back as `Missing` with reason `restart`, for the caller
/// to record, so nothing is resolved twice or left open forever.
pub fn restore_markouts(
    rows: &[serde_json::Value],
    now_ms: u64,
    max_late_ms: u64,
) -> Result<(Vec<PendingMarkout>, Vec<Markout>), String> {
    use std::collections::HashSet;
    use std::str::FromStr;
    let str_of =
        |r: &serde_json::Value, k: &str| r.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let resolved: HashSet<(String, u64)> = rows
        .iter()
        .filter(|r| {
            matches!(
                r.get("kind").and_then(|k| k.as_str()),
                Some("markout") | Some("markout_missing")
            )
        })
        .filter_map(|r| Some((str_of(r, "fill_id")?, r.get("horizon_s")?.as_u64()?)))
        .collect();
    let mut pending = Vec::new();
    let mut missing = Vec::new();
    for r in rows
        .iter()
        .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some("fill"))
    {
        let id = str_of(r, "fill_id").ok_or_else(|| format!("fill row without fill_id: {r}"))?;
        let ts = r
            .get("ts_ms")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| format!("fill {id} without ts_ms"))?;
        let px = str_of(r, "px")
            .and_then(|p| Decimal::from_str(&p).ok())
            .ok_or_else(|| format!("fill {id} without px"))?;
        let buy = str_of(r, "side").as_deref() == Some("buy");
        let mut owed = Vec::new();
        for h in MARKOUT_HORIZONS {
            if resolved.contains(&(id.clone(), h)) {
                continue;
            }
            if now_ms > ts + h * 1_000 + max_late_ms {
                missing.push(Markout::Missing {
                    fill_id: id.clone(),
                    horizon_s: h,
                    reason: "restart".to_string(),
                });
            } else {
                owed.push(h);
            }
        }
        if !owed.is_empty() {
            pending.push(PendingMarkout {
                fill_id: id,
                ts_ms: ts,
                px,
                buy,
                horizons: owed,
            });
        }
    }
    Ok((pending, missing))
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

/// A resolved markout horizon.
#[derive(Debug, Clone, PartialEq)]
pub enum Markout {
    Priced {
        fill_id: String,
        horizon_s: u64,
        bps: Decimal,
    },
    /// No fresh mid within `max_late_ms` after the horizon: recorded as
    /// missing, never priced at a stale mid.
    Missing {
        fill_id: String,
        horizon_s: u64,
        reason: String,
    },
}

/// Resolve due horizons against `fresh_mid`, a mid from a timestamp-fresh
/// book only (`logic::fresh_mark`, Codex P2, pairtrade#361). While there is
/// none, a due horizon stays pending; past `max_late_ms` after it is due it
/// is dropped as `Missing`.
pub fn due_markouts(
    pending: &mut Vec<PendingMarkout>,
    now_ms: u64,
    fresh_mid: Option<Decimal>,
    max_late_ms: u64,
) -> Vec<Markout> {
    let mut out = Vec::new();
    for p in pending.iter_mut() {
        p.horizons.retain(|h| {
            let due = p.ts_ms + h * 1_000;
            if now_ms < due {
                return true;
            }
            match fresh_mid {
                Some(mid) => {
                    out.push(Markout::Priced {
                        fill_id: p.fill_id.clone(),
                        horizon_s: *h,
                        bps: markout_bps(p.buy, p.px, mid),
                    });
                    false
                }
                None if now_ms > due + max_late_ms => {
                    out.push(Markout::Missing {
                        fill_id: p.fill_id.clone(),
                        horizon_s: *h,
                        reason: format!(
                            "no fresh mid within {}s of the horizon",
                            max_late_ms / 1_000
                        ),
                    });
                    false
                }
                None => true,
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

    const ROW: &str = r#"{"kind":"fill","seq":0,"mode":"live","fill_id":"t1","ts_ms":1000,"side":"buy","qty":"0.1","px":"83642.9","fee":"0","role":"maker"}"#;

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
        let new_row = serde_json::json!({"kind": "fill", "seq": 1, "mode": "live", "fill_id": "t2",
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
    fn the_dead_mans_switch_is_per_market() {
        // bot-strategy#1093: arm, refresh and disarm target this runtime's
        // market only, so a crash or an outage never cancels orders in other
        // markets of the subaccount (the owner's manual orders) and several
        // runtimes can share one. The only account-wide call left is the
        // one-shot DISARM of a switch an older build may have left armed.
        let main = include_str!("main.rs");
        let code: String = main
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        let flat: String = code.split_whitespace().collect();
        assert!(
            flat.contains(".schedule_cancel_market(&self.cfg.market,Some(self.cfg.dms_secs))"),
            "arm/refresh must be per-market"
        );
        assert!(
            flat.contains(".schedule_cancel_market(&self.cfg.market,None)"),
            "shutdown disarm must be per-market"
        );
        assert_eq!(
            flat.matches(".schedule_cancel(").count(),
            1,
            "exactly one account-wide call (the legacy disarm)"
        );
        assert!(
            flat.contains(".schedule_cancel(None)"),
            "the account-wide call may only disarm"
        );
        assert!(
            !flat.contains(".schedule_cancel(Some("),
            "never arm the account-wide switch"
        );
        // The legacy disarm runs once, before the first per-market arm.
        let legacy = flat.find(".schedule_cancel(None)").unwrap();
        let arm = flat
            .find(".schedule_cancel_market(&self.cfg.market,Some(")
            .unwrap();
        assert!(legacy < arm);
        assert!(flat.contains("self.legacy_dms_cleared=true;"));
        // The legacy switch counts as cleared only after a confirmed disarm,
        // and the per-market arm waits for it (Codex P1 on #386).
        assert!(
            flat.contains("Ok(())=>{log::info!(\"[ARCUS_VOL]account-widedeadman'sswitchdisarmed")
        );
        assert!(flat.contains("ifdue&&self.legacy_dms_cleared{"));
        // An unconfirmed / failed per-market disarm at shutdown is a WARN and
        // the shutdown carries on (Codex P1 on dex-connector#131).
        let disarm = flat
            .find(".schedule_cancel_market(&self.cfg.market,None)")
            .unwrap();
        let branch = &flat[disarm..];
        let branch = &branch[..branch.find("}}}").unwrap()];
        assert!(
            branch.contains("log::warn!(\"{}\",dms_disarm_unconfirmed_note("),
            "{branch}"
        );
        for forbidden in ["return", "halt", "exit(", "panic!", "bail!", "?;"] {
            assert!(!branch.contains(forbidden), "{forbidden} in {branch}");
        }
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

    /// The body of `fn <name>` in main.rs (up to the next method).
    fn main_fn_body(name: &str) -> String {
        let main = include_str!("main.rs");
        let start = main
            .find(&format!("fn {name}("))
            .unwrap_or_else(|| panic!("fn {name} not found in main.rs"));
        let rest = &main[start..];
        let end = rest[1..]
            .find("\n    async fn ")
            .into_iter()
            .chain(rest[1..].find("\n    fn "))
            .min()
            .map_or(rest.len(), |i| i + 1);
        rest[..end].to_string()
    }

    fn before(body: &str, first: &str, then: &str) -> bool {
        match (body.find(first), body.find(then)) {
            (Some(a), Some(b)) => a < b,
            _ => false,
        }
    }

    #[test]
    fn the_venue_ticker_read_is_awaited_before_the_plan_is_decided() {
        // bot-strategy#1093 presence quoting, review 3 #4: the best-effort
        // ticker read (paper tick, venue minimums) sits with the tick's other
        // awaited reads, so nothing is awaited between the fresh-clock plan
        // decision and `paper_quote`.
        let tick = main_fn_body("tick");
        let read = "self.refresh_venue_meta(now).await";
        assert!(before(&tick, read, "decide on a fresh clock"));
        assert!(before(&tick, "decide on a fresh clock", "tick_plan("));
        assert!(before(&tick, "tick_plan(", "self.paper_quote("));
        // …and that is its only call site.
        let main = include_str!("main.rs");
        assert_eq!(main.matches(".refresh_venue_meta(").count(), 1);
    }

    #[test]
    fn the_session_offset_is_wired_into_every_quote() {
        // bot-strategy#1093 session offset: the tick reads the session, then
        // fixes it on the fresh clock before planning; the one QuoteParams
        // builder quotes with the session's presence pair.
        let tick = main_fn_body("tick");
        assert!(before(
            &tick,
            "self.refresh_session(now)",
            "self.update_session(now)"
        ));
        assert!(before(
            &tick,
            "self.update_session(now)",
            "self.quote_params()"
        ));
        let params = main_fn_body("quote_params");
        assert!(params.contains("presence: self.cfg.presence_for(self.in_session)"));
        assert!(!params.contains("self.cfg.presence()"));
        // The read is skipped entirely when no session offset is configured.
        let read = main_fn_body("refresh_session");
        assert!(read.contains("!self.cfg.session_switching()"));
    }

    #[test]
    fn a_just_placed_quote_is_not_forgotten_by_the_open_orders_sweep() {
        // bot-strategy#1093 2026-10-06 00:00:00Z: the sweep dropped two
        // just-placed quotes the read did not list yet, and the same tick
        // placed a second pair. The sweep must go through the grace-aware
        // `sweep_open_orders`, and every placement / modify must record its
        // placement time.
        let sweep: String = main_fn_body("live_reconcile_orders")
            .split_whitespace()
            .collect();
        assert!(sweep.contains("sweep_open_orders(&self.resting,&self.placed_at_ms,&ids,"));
        assert!(sweep.contains("PLACE_VISIBILITY_GRACE_MS"));
        assert!(!sweep.contains("self.resting.retain(|_,r|ids.contains(&r.order_id))"));
        for f in ["send_places", "send_modifies"] {
            let body: String = main_fn_body(f).split_whitespace().collect();
            assert!(
                body.contains("self.placed_at_ms.insert(resp.order_id.clone(),now_ms());"),
                "{f} must record the placement time"
            );
        }
    }

    #[test]
    fn dust_is_carried_on_every_runtime_path() {
        // bot-strategy#1093 dust fix (live 2026-10-03: 0.00046 SPY below the
        // venue minimum, 11,383 rejected flatten IOCs, quotes pulled 10 h).
        // The tick decides flatten from the dust-aware hold clock, startup
        // adopts dust instead of flattening it, a below-minimum rejection
        // marks the inventory as dust at once instead of retrying.
        let tick = main_fn_body("tick");
        assert!(before(
            &tick,
            "self.track_hold(now, mid)",
            "flatten_reason("
        ));
        assert!(before(
            &tick,
            "let dust = self.inventory_is_dust(mid)",
            "flatten_reason("
        ));
        let call = &tick[tick.find("flatten_reason(").unwrap()..];
        let call = &call[..call.find(");").unwrap()];
        assert!(
            call.contains("self.hold_since_ms"),
            "max-hold runs from hold_since_ms"
        );
        assert!(
            !call.contains("opened_at_ms"),
            "not from the ledger's opened_at_ms"
        );
        assert!(
            call.trim_end().ends_with("dust,"),
            "dust is the last argument"
        );
        assert!(tick.contains("&& !dust;"), "startup flatten skips dust");

        let startup = main_fn_body("startup_reconcile");
        assert!(before(
            &startup,
            "self.inventory_is_dust(None)",
            "self.startup_flatten = true"
        ));

        let flatten = main_fn_body("live_flatten");
        assert!(flatten.contains("self.on_flatten_error(now, qty, &e)"));
        assert!(!flatten.contains("self.on_error(\"flatten IOC\""));
        let on_err = main_fn_body("on_flatten_error");
        // State is applied FIRST, unconditionally; only the log line sits
        // behind the once-a-minute throttle (Codex P2 on pairtrade#382).
        assert!(before(
            &on_err,
            "let effect = self.apply_error(err);",
            "ErrorEffect::BelowMinimum"
        ));
        assert!(before(
            &on_err,
            "ErrorEffect::BelowMinimum",
            "self.forced_dust_qty = Some(inv)"
        ));
        assert!(before(
            &on_err,
            "self.forced_dust_qty = Some(inv)",
            "return;"
        ));
        assert!(
            before(&on_err, ">= 60_000", "self.log_error("),
            "only the log is throttled"
        );
        assert!(
            !on_err.contains("self.on_error("),
            "no classification behind the throttle"
        );
        let apply = main_fn_body("apply_error");
        assert!(
            apply.contains("self.backoff_until_ms = until_ms")
                && apply.contains("self.need_reconcile = true")
        );

        let is_dust = main_fn_body("inventory_is_dust");
        assert!(is_dust.contains("self.forced_dust_qty == Some(qty)"));
        // ...and the override is re-validated on every ticker refresh.
        let meta_body = main_fn_body("refresh_venue_meta");
        assert!(meta_body.contains("self.revalidate_forced_dust(ticker.price)"));
        let reval = main_fn_body("revalidate_forced_dust");
        assert!(before(
            &reval,
            "forced_dust_still_holds(",
            "self.forced_dust_qty = None"
        ));
        let hold = main_fn_body("track_hold");
        assert!(before(
            &hold,
            "if self.inventory_is_dust(mid)",
            "self.hold_since_ms = None"
        ));
        // The ticker read (venue minimums) is no longer DRY_RUN-only.
        let meta = main_fn_body("refresh_venue_meta");
        assert!(!meta.contains("dry_run)") && meta.contains("ticker_read_due(now,"));
        assert!(meta.contains("min_order_qty: ticker.min_order"));
    }

    #[test]
    fn every_booking_path_waits_for_a_durable_rollover() {
        // pre-G2, Codex 4146113066 / 4146548930: the live harvest (tick,
        // startup, shutdown), the paper flatten IOC and the pending-fill
        // resolution all book only after ensure_rolled.
        let harvest = main_fn_body("harvest_fills");
        assert!(
            before(&harvest, "ensure_rolled(", "get_filled_orders"),
            "harvest_fills must check the rollover before reading/booking fills"
        );
        let flatten = main_fn_body("paper_flatten");
        assert!(
            before(&flatten, "ensure_rolled(", "self.book(fill"),
            "paper_flatten must check the rollover before booking its IOC"
        );
        let resolve = main_fn_body("resolve_pending_fills");
        assert!(before(&resolve, "ensure_rolled(", "self.book_spilled("));
        // Spilled fills book at their own time, never the restart `now`
        // (4152482567), and only the resolved subset leaves the pending file
        // (4152482577).
        assert!(resolve.contains("self.book_spilled(fill, p.fill_time_ms(), now)"));
        assert!(resolve.contains("settle_pending(") && !resolve.contains("fs::rename"));
        // Startup resolves the last shutdown's leftovers before harvesting.
        let startup = main_fn_body("startup_reconcile");
        assert!(before(&startup, "resolve_pending_fills(", "live_fills("));
        // Shutdown spills whatever the harvest couldn't book (4146269818).
        let shutdown = main_fn_body("shutdown");
        assert!(before(&shutdown, "harvest_fills(", "spill_unbooked("));
        // The final spill read is bounded and falls back (4152541379).
        assert!(shutdown.contains("self.spill_unbooked(now_ms(), left)"));
        let spill = main_fn_body("spill_unbooked");
        assert!(spill.contains("read_within(") && spill.contains("spill_rows("));
        let harvest = main_fn_body("harvest_fills");
        assert!(harvest.contains("self.unbooked_seen ="));
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
    fn state_is_written_durably_only_when_changed_and_retried_after_a_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let mut w = StateWriter::default();
        let mut l = Ledger::new("live", "2026-09-30");
        assert!(w.write(&path, &l).unwrap());
        let on_disk: Ledger =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(on_disk, l);
        // Unchanged: no write at all.
        assert!(!w
            .write_with(&path, &l, |_, _| panic!(
                "unchanged state must not be rewritten"
            ))
            .unwrap());
        // Changed but the write fails: the next call must retry.
        l.fills = 1;
        assert!(w
            .write_with(&path, &l, |_, _| Err(std::io::Error::other("disk")))
            .is_err());
        let mut retried = false;
        assert!(w
            .write_with(&path, &l, |_, _| {
                retried = true;
                Ok(())
            })
            .unwrap());
        assert!(retried);
    }

    #[test]
    fn the_runtime_never_writes_state_json_non_durably() {
        // state.json only via StateWriter / persist_durable (Codex P2,
        // pairtrade#361); status.json is informational and may stay plain.
        let main = include_str!("main.rs");
        assert!(
            !main.contains("persist_json"),
            "state.json must be written durably"
        );
        assert!(!main.contains("atomic_write(&self.state_path"));
    }

    #[test]
    fn one_account_cannot_be_traded_from_two_state_dirs() {
        let locks = tempfile::tempdir().unwrap();
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let _sa = acquire_state_lock(a.path()).unwrap();
        let _sb = acquire_state_lock(b.path()).unwrap(); // different state dirs: fine
        let p1 = account_lock_path(locks.path(), false, Some("0xA2C7Ab"), Some(1))
            .unwrap()
            .unwrap();
        let p2 = account_lock_path(locks.path(), false, Some(" 0xa2c7ab "), Some(1))
            .unwrap()
            .unwrap();
        assert_eq!(p1, p2, "address case/whitespace must not split the lock");
        let first = acquire_lock_at(&p1).unwrap();
        assert!(
            acquire_lock_at(&p2).is_err(),
            "same account, second runtime refused"
        );
        // Another subaccount is another lock.
        let other = account_lock_path(locks.path(), false, Some("0xa2c7ab"), Some(2))
            .unwrap()
            .unwrap();
        assert!(acquire_lock_at(&other).is_ok());
        drop(first);
        assert!(acquire_lock_at(&p2).is_ok());
        // Live without an address or index: error; DRY_RUN: no account lock.
        assert!(account_lock_path(locks.path(), false, None, Some(1)).is_err());
        assert!(account_lock_path(locks.path(), false, Some("0xa"), None).is_err());
        assert_eq!(
            account_lock_path(locks.path(), true, None, None).unwrap(),
            None
        );
    }

    #[test]
    fn a_transient_book_failure_does_not_trip_the_daily_stop_on_entry_price() {
        let (daily, cum) = (d("50"), d("250"));
        let mut l = Ledger::new("live", "2026-09-30");
        l.record_fill(true, d("1"), d("100000"), Decimal::ZERO, true, 1);
        // Rolled over while BTC was up 60: the day starts at +60 unrealized.
        l.rollover("2026-10-01", Some(d("100060")));
        assert_eq!(l.day_start_unrealized, d("60"));
        // The book is gone: pricing at the entry price would read −60 today
        // and trip the $50 daily stop. No fresh mark → unrealized deferred.
        let o = risk_check(&mut l, None, false, false, daily, cum);
        assert_eq!(o.halt, None);
        assert!(o.unrealized_deferred);
        assert!(!l.day_halt);
        // Realized losses and fees still count without a mark.
        l.record_fill(false, d("1"), d("99940"), d("1"), false, 2);
        let o = risk_check(&mut l, None, false, false, daily, cum);
        assert_eq!(o.halt, Some(Halt::Day)); // realized −60 − fee 1
        assert!(!o.unrealized_deferred);
    }

    #[test]
    fn a_rollover_is_persisted_before_any_new_day_fill() {
        let mut l = Ledger::new("live", "2026-09-30");
        l.record_fill(true, d("0.1"), d("100000"), d("2"), false, 1);
        let old_day_volume = l.day_volume;
        let mut pending = false;
        let mut persisted: Option<Ledger> = None;
        // A paper print after midnight, before any tick: roll + persist first.
        let ok = ensure_rolled(&mut l, "2026-10-01", Some(d("100000")), &mut pending, |s| {
            persisted = Some(s.clone());
            Ok(())
        })
        .unwrap();
        assert!(ok);
        let on_disk = persisted.expect("rollover persisted");
        assert_eq!(on_disk.day, "2026-10-01");
        assert_eq!(on_disk.day_fees, Decimal::ZERO);
        // Then the fill books to the new day.
        book_fill(&mut l, &fill_in("n1"), 2, |_| Ok(())).unwrap();
        assert_eq!(l.day_volume, d("8364.29"));
        assert_ne!(l.day_volume, old_day_volume + d("8364.29"));
        // Same day afterwards: no write.
        assert!(
            ensure_rolled(&mut l, "2026-10-01", None, &mut pending, |_| panic!(
                "no write"
            ))
            .unwrap()
        );
    }

    #[test]
    fn no_booking_while_the_rollover_is_postponed_or_unpersisted() {
        let mut l = Ledger::new("live", "2026-09-30");
        let mut pending = false;
        // No fresh mark: postponed, nothing may book.
        assert!(ensure_rolled(&mut l, "2026-10-01", None, &mut pending, |_| Ok(())).is_err());
        // Persist fails: nothing may book, and the next call retries it.
        assert!(
            ensure_rolled(&mut l, "2026-10-01", Some(d("1")), &mut pending, |_| {
                Err(std::io::Error::other("disk"))
            })
            .is_err()
        );
        assert!(pending);
        let mut retried = false;
        assert!(
            ensure_rolled(&mut l, "2026-10-01", Some(d("1")), &mut pending, |_| {
                retried = true;
                Ok(())
            })
            .unwrap()
        );
        assert!(retried && !pending);
    }

    #[test]
    fn an_incomplete_fill_record_waits_then_books_once_or_halts() {
        use dex_connector::OrderSide;
        let wait = 60_000;
        // Incomplete (no value): pending, not cleared, not booked.
        assert_eq!(
            classify_fill_record(
                false,
                Some(OrderSide::Long),
                Some(d("0.1")),
                None,
                1_000,
                30_000,
                wait
            ),
            FillRecordAction::Pending
        );
        // Completed later: booked, exactly once.
        let act = classify_fill_record(
            false,
            Some(OrderSide::Long),
            Some(d("0.1")),
            Some(d("8364.29")),
            1_000,
            40_000,
            wait,
        );
        assert!(matches!(act, FillRecordAction::Book { .. }));
        let mut l = Ledger::new("live", "2026-09-30");
        book_fill(&mut l, &fill_in("t1"), 1, |_| Ok(())).unwrap();
        book_fill(&mut l, &fill_in("t1"), 2, |_| Ok(())).unwrap();
        assert_eq!(l.fills, 1);
        // Still incomplete past the wait: halt, never guessed or dropped.
        assert_eq!(
            classify_fill_record(false, None, Some(d("0.1")), None, 1_000, 61_000, wait),
            FillRecordAction::Halt
        );
        assert!(l.halt_sticky("fill_incomplete: trade t9".into(), None));
        assert!(l.sticky_halt);
        assert!(l
            .sticky_reason
            .as_deref()
            .unwrap()
            .starts_with("fill_incomplete"));
        // Only confirmed rejected / zero-size records are cleared.
        assert_eq!(
            classify_fill_record(true, None, None, None, 1_000, 1_000, wait),
            FillRecordAction::Clear
        );
        assert_eq!(
            classify_fill_record(false, None, Some(Decimal::ZERO), None, 1_000, 1_000, wait),
            FillRecordAction::Clear
        );
    }

    #[test]
    fn a_journal_without_state_is_refused_but_an_empty_one_is_a_fresh_start() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("fills.jsonl"),
            format!(
                "{}\n",
                ROW.replace(
                    "\"mode\":\"live\"",
                    "\"mode\":\"live\",\"market\":\"BTC-USD\""
                )
            ),
        )
        .unwrap();
        let err = open_state(dir.path(), "live", "BTC-USD", "2026-09-30").unwrap_err();
        assert!(err.contains("journal without state"), "{err}");
        assert!(!dir.path().join("state.json").exists());
        // Empty journal, no state: fresh start.
        std::fs::write(dir.path().join("fills.jsonl"), "").unwrap();
        assert!(open_state(dir.path(), "live", "BTC-USD", "2026-09-30").is_ok());
    }

    #[test]
    fn pending_markouts_survive_a_restart_and_resolve_once() {
        let fill = serde_json::json!({"kind": "fill", "seq": 0, "fill_id": "f1", "ts_ms": 1_000,
                                      "px": "100000", "side": "buy", "market": "BTC-USD"});
        let m5 = serde_json::json!({"kind": "markout", "seq": 1, "fill_id": "f1", "horizon_s": 5,
                                    "bps": "1", "market": "BTC-USD"});
        let mut rows = vec![fill, m5];
        // Restart at 20 s, before +30: +30 and +60 restored, +5 not.
        let (mut pending, missing) = restore_markouts(&rows, 20_000, 60_000).unwrap();
        assert!(missing.is_empty());
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].horizons, vec![30, 60]);
        // They resolve once each; record them as the runtime does.
        for now in [31_000, 61_000] {
            for m in due_markouts(&mut pending, now, Some(d("100010")), 60_000) {
                if let Markout::Priced {
                    fill_id, horizon_s, ..
                } = m
                {
                    rows.push(serde_json::json!({"kind": "markout", "fill_id": fill_id,
                                                 "horizon_s": horizon_s, "market": "BTC-USD"}));
                }
            }
        }
        assert!(pending.is_empty());
        assert_eq!(rows.iter().filter(|r| r["kind"] == "markout").count(), 3);
        // A second restart owes nothing: no duplicates.
        let (pending, missing) = restore_markouts(&rows, 70_000, 60_000).unwrap();
        assert!(pending.is_empty() && missing.is_empty());
    }

    #[test]
    fn markouts_too_late_at_restart_go_missing_with_reason_restart() {
        let fill = serde_json::json!({"kind": "fill", "seq": 0, "fill_id": "f1", "ts_ms": 1_000,
                                      "px": "100000", "side": "sell"});
        // Down for 10 minutes: every horizon is past its lateness.
        let (pending, missing) = restore_markouts(&[fill], 601_000, 60_000).unwrap();
        assert!(pending.is_empty());
        assert_eq!(missing.len(), 3);
        assert!(missing
            .iter()
            .all(|m| matches!(m, Markout::Missing { reason, .. } if reason == "restart")));
    }

    #[test]
    fn a_paper_fill_is_timed_by_the_print_not_by_processing_time() {
        let mut l = Ledger::new("dry_run", "2026-09-30");
        let f = fill_in("p1");
        let (print_ms, now) = (7_000, 10_000); // the print is 3 s old
        let mut rows = Vec::new();
        book_fill_at(&mut l, &f, print_ms, now, |r| {
            rows.push(r.clone());
            Ok(())
        })
        .unwrap();
        assert_eq!(rows[0]["ts_ms"], 7_000);
        assert_eq!(rows[0]["rx_ms"], 10_000);
        // Position opened at the print time: max-hold counts from there.
        assert_eq!(l.position.opened_at_ms, Some(7_000));
        // Markout horizons are due from the print time.
        let mut pending = vec![markouts_for(&f, print_ms)];
        assert!(due_markouts(&mut pending, 11_999, Some(d("1")), 60_000).is_empty());
        let out = due_markouts(&mut pending, 12_000, Some(d("83642.9")), 60_000);
        assert!(matches!(
            out.as_slice(),
            [Markout::Priced { horizon_s: 5, .. }]
        ));
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
            risk_check(&mut l, Some(d("99950.1")), false, false, daily, cum).halt,
            None
        );
        // −60: day halt
        let o = risk_check(&mut l, Some(d("99940")), false, false, daily, cum);
        assert_eq!(o.halt, Some(Halt::Day));
        assert_eq!(o.events.len(), 1);
        // recovering intraday does not lift it
        assert_eq!(
            risk_check(&mut l, Some(d("100000")), false, false, daily, cum).halt,
            Some(Halt::Day)
        );
        l.rollover("2026-10-01", Some(d("100000")));
        assert_eq!(
            risk_check(&mut l, Some(d("100000")), false, false, daily, cum).halt,
            None
        );
        // kill switch outranks a day halt
        assert_eq!(
            risk_check(&mut l, Some(d("100000")), true, false, daily, cum).halt,
            Some(Halt::Kill)
        );
        // −300 cumulative: sticky, survives rollover and a recovery
        let o = risk_check(&mut l, Some(d("99700")), false, false, daily, cum);
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
        assert!(o.write_halt_file);
        l.rollover("2026-10-02", Some(d("100000")));
        assert!(matches!(
            risk_check(&mut l, Some(d("100000")), false, true, daily, cum).halt,
            Some(Halt::Sticky(_))
        ));
    }

    #[test]
    fn halt_file_halts_whatever_state_says_and_is_recreated_when_missing() {
        let (daily, cum) = (d("1000"), d("250"));
        let mut l = Ledger::new("dry_run", "2026-09-30");
        // A HALT file present at load halts a clean state.
        let o = risk_check(&mut l, Some(d("100000")), false, true, daily, cum);
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
        assert!(l.sticky_halt);
        assert!(!o.write_halt_file);
        // File deleted while state is still sticky: stays halted, file recreated.
        let o = risk_check(&mut l, Some(d("100000")), false, false, daily, cum);
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
        assert!(o.write_halt_file);
        // State cleared but the file still there: still halted.
        l.sticky_halt = false;
        let o = risk_check(&mut l, Some(d("100000")), false, true, daily, cum);
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
    }

    #[test]
    fn clearing_needs_file_and_state_and_rebases_the_cumulative_stop() {
        let (daily, cum) = (d("1000"), d("250"));
        let mut l = Ledger::new("dry_run", "2026-09-30");
        l.record_fill(true, d("1"), d("100000"), Decimal::ZERO, true, 1);
        let o = risk_check(&mut l, Some(d("99700")), false, false, daily, cum);
        assert!(matches!(o.halt, Some(Halt::Sticky(_))));
        assert!(o.write_halt_file);
        // Hand clear: file deleted AND state edited → cleared, baseline −300.
        l.sticky_halt = false;
        let o = risk_check(&mut l, Some(d("99700")), false, false, daily, cum);
        assert_eq!(o.halt, None);
        assert_eq!(o.events.len(), 1);
        assert!(!o.write_halt_file);
        assert_eq!(l.cum_baseline, d("-300"));
        // another −200 is inside the re-based stop, −260 is not
        assert_eq!(
            risk_check(&mut l, Some(d("99500")), false, false, daily, cum).halt,
            None
        );
        assert!(matches!(
            risk_check(&mut l, Some(d("99440")), false, false, daily, cum).halt,
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
    fn replay_goes_by_sequence_not_by_timestamp() {
        let row = |id: &str, seq: Option<u64>, ts: u64| {
            let mut v = serde_json::json!({"kind": "fill", "mode": "live", "fill_id": id,
                "ts_ms": ts, "side": "buy", "qty": "1", "px": "1", "fee": "0", "role": "maker"});
            if let Some(seq) = seq {
                v["seq"] = serde_json::json!(seq);
            }
            v
        };
        let mut l = Ledger::new("live", "2026-09-30");
        book_fill(&mut l, &fill_in("a"), 5_000, |_| Ok(())).unwrap(); // seq 0
        assert_eq!(l.last_booked_seq, Some(0));
        // A later row whose clock ran BEHIND (earlier ts) is still replayed.
        let later = row("b", Some(1), 4_000);
        assert_eq!(replay_fills(&mut l, [&later]).unwrap(), 1);
        assert!(l.has_booked("b"));
        assert_eq!(l.next_seq, 2);
        // A row at/below the booked sequence whose id aged out: skipped.
        let old = row("old", Some(1), 9_000);
        assert_eq!(replay_fills(&mut l, [&old]).unwrap(), 0);
        // No seq: fail closed.
        assert!(replay_fills(&mut l, [&row("x", None, 9_000)]).is_err());
        let mut bad = row("bad", Some(7), 6_000);
        bad["qty"] = serde_json::json!("x");
        assert!(replay_fills(&mut l, [&bad]).is_err());
    }

    #[test]
    fn open_state_never_reuses_a_journal_sequence() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("fills.jsonl"),
            "{\"kind\":\"markout\",\"seq\":41,\"market\":\"BTC-USD\"}\n",
        )
        .unwrap();
        let mut l = Ledger::new("live", "2026-09-30");
        l.market = "BTC-USD".into();
        l.next_seq = 3;
        persist_durable(&dir.path().join("state.json"), &l).unwrap();
        let opened = open_state(dir.path(), "live", "BTC-USD", "2026-09-30").unwrap();
        assert_eq!(opened.ledger.next_seq, 42);
        // And it is on disk, so a crash right after cannot reuse 41.
        let on_disk: Ledger =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join("state.json")).unwrap())
                .unwrap();
        assert_eq!(on_disk.next_seq, 42);
    }

    #[test]
    fn a_position_mismatch_sticky_halts_without_touching_the_position() {
        let mut l = Ledger::new("live", "2026-09-30");
        book_fill(&mut l, &fill_in("t1"), 1, |_| Ok(())).unwrap();
        assert!(l.halt_position_mismatch(d("0.3"), Some(d("83642.9"))));
        assert!(l.sticky_halt);
        assert_eq!(l.position.qty, d("0.1"));
        assert!(l
            .sticky_reason
            .as_deref()
            .unwrap()
            .starts_with(crate::logic::POSITION_MISMATCH));
        // A sticky halt already in force is left alone.
        assert!(!l.halt_position_mismatch(d("0.5"), Some(d("83642.9"))));
        // risk_check keeps it and asks for the HALT file.
        let o = risk_check(
            &mut l,
            Some(d("83642.9")),
            false,
            false,
            d("1000"),
            d("1000"),
        );
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
    fn a_failed_first_append_still_dir_syncs_on_the_next_success() {
        // pre-G2, Codex P2 4146418683: the first append creates the file but
        // its write fails and is rolled back to 0 bytes; the retry sees the
        // file as existing, yet must still fsync the directory entry.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fills.jsonl");
        let err = append_journal(
            &path,
            &serde_json::json!({"a": 1}),
            |_, _| Err(std::io::Error::other("ENOSPC")),
            |f, l| f.set_len(l),
            |_| Ok(()),
        );
        assert!(err.is_err());
        assert!(path.exists());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        let mut synced = 0;
        append_journal(
            &path,
            &serde_json::json!({"a": 1}),
            write_all,
            |f, l| f.set_len(l),
            |_| {
                synced += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(synced, 1, "the retry must fsync the parent directory");
        // Once a row is durable, later appends don't.
        append_journal(
            &path,
            &serde_json::json!({"a": 2}),
            write_all,
            |f, l| f.set_len(l),
            |_| panic!("no dir sync once the file holds a row"),
        )
        .unwrap();
    }

    #[test]
    fn unbooked_fills_at_shutdown_are_spilled_and_resolved_never_dropped() {
        use std::str::FromStr;
        // pre-G2, Codex P1 4146269818.
        let d = |v: &str| Decimal::from_str(v).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pending_fills.jsonl");
        let complete = dex_connector::FilledOrder {
            trade_id: "t1".into(),
            order_id: "o1".into(),
            filled_side: Some(dex_connector::OrderSide::Short),
            filled_size: Some(d("0.002")),
            filled_value: Some(d("167.2")),
            filled_fee: None,
            ..Default::default()
        };
        let incomplete = dex_connector::FilledOrder {
            trade_id: "t2".into(),
            order_id: "o2".into(),
            filled_size: Some(d("0.001")),
            ..Default::default()
        };
        let rejected = dex_connector::FilledOrder {
            trade_id: "t3".into(),
            is_rejected: true,
            ..Default::default()
        };
        let spilled: Vec<PendingFill> = [&complete, &incomplete, &rejected]
            .iter()
            .map(|f| PendingFill::from_record(f, Some(true), 9))
            .collect();
        spill_pending(&path, &spilled).unwrap();
        let back = read_pending(&path).unwrap();
        assert_eq!(back, spilled, "every unbooked record survives the restart");
        let res: Vec<_> = back.iter().map(|p| resolve_pending(p, d("2.25"))).collect();
        match &res[0] {
            PendingResolution::Book(f) => {
                assert!(!f.buy);
                assert_eq!(f.qty, d("0.002"));
                assert_eq!(f.px, d("83600"));
                // Missing fee → conservative taker estimate, never zero.
                assert_eq!(f.fee, d("0.03762"));
                assert!(f.fee_estimated);
                assert!(f.maker);
            }
            other => panic!("complete record must book, got {other:?}"),
        }
        assert!(matches!(&res[1], PendingResolution::Halt(r) if r.starts_with("fill_incomplete")));
        assert_eq!(res[2], PendingResolution::Clear);
        // A torn/garbled line is a startup error, never skipped.
        std::fs::write(&path, "{\"trade_id\": \"t9\"\n").unwrap();
        assert!(read_pending(&path).is_err());
        // Missing file: nothing pending.
        assert!(read_pending(&dir.path().join("none.jsonl"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn unresolved_pending_fills_stay_pending_until_each_resolves() {
        use std::str::FromStr;
        // pre-G2, Codex P1 4152482577: two incomplete records with a sticky
        // halt already set; both must survive a restart, and when one
        // completes only that one leaves the pending file.
        let d = |v: &str| Decimal::from_str(v).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pending_fills.jsonl");
        let archive = dir.path().join("pending_fills.resolved.jsonl");
        let rec = |id: &str| PendingFill {
            trade_id: id.into(),
            order_id: "o".into(),
            is_rejected: false,
            buy: None,
            size: Some(d("0.001")),
            value: None,
            fee: None,
            maker_hint: None,
            spilled_at_ms: 1,
            fill_ts_ms: None,
        };
        spill_pending(&path, &[rec("t1"), rec("t2")]).unwrap();
        let mut l = {
            let mut x = Ledger::new("live", "2026-10-01");
            x.market = "BTC-USD".into();
            x
        };
        assert!(l.halt_sticky("some earlier reason".into(), None));
        // Restart 1: both still incomplete → both stay, nothing archived.
        let pending = read_pending(&path).unwrap();
        let unresolved: Vec<_> = pending
            .iter()
            .filter(|p| matches!(resolve_pending(p, d("2.25")), PendingResolution::Halt(_)))
            .cloned()
            .collect();
        assert_eq!(unresolved.len(), 2);
        settle_pending(&path, &archive, &[], &unresolved).unwrap();
        assert_eq!(read_pending(&path).unwrap(), unresolved);
        assert!(!archive.exists());
        // Restart 2: t1 completed (operator filled it in); t2 still not.
        let mut t1 = rec("t1");
        t1.buy = Some(true);
        t1.value = Some(d("83.6"));
        let pending = vec![t1.clone(), rec("t2")];
        let (mut done, mut still) = (Vec::new(), Vec::new());
        for p in &pending {
            match resolve_pending(p, d("2.25")) {
                PendingResolution::Book(f) => {
                    book_fill_at(&mut l, &f, p.fill_time_ms(), 99, |_| Ok(())).unwrap();
                    done.push(p.clone());
                }
                PendingResolution::Clear => done.push(p.clone()),
                PendingResolution::Halt(_) => still.push(p.clone()),
            }
        }
        settle_pending(&path, &archive, &done, &still).unwrap();
        assert_eq!(
            read_pending(&path).unwrap(),
            vec![rec("t2")],
            "t2 must stay pending"
        );
        assert_eq!(read_pending(&archive).unwrap(), vec![t1]);
        assert!(l.has_booked("t1") && !l.has_booked("t2"));
        // All resolved → the pending file goes away.
        settle_pending(&path, &archive, &[rec("t2")], &[]).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn a_spilled_fill_is_booked_at_its_own_time_and_day() {
        use std::str::FromStr;
        // pre-G2, Codex P1 4152482567: spilled before midnight, resolved after
        // the rollover → cumulative only, timed by its own time, and its
        // markouts are missing (restart), never priced later.
        let d = |v: &str| Decimal::from_str(v).unwrap();
        let mut p = PendingFill {
            trade_id: "t1".into(),
            order_id: "o".into(),
            is_rejected: false,
            buy: Some(true),
            size: Some(d("0.001")),
            value: Some(d("83.6")),
            fee: Some(d("0")),
            maker_hint: Some(true),
            spilled_at_ms: 1_790_812_000_000, // 2026-09-30T23:46:40Z
            fill_ts_ms: None,
        };
        assert_eq!(p.fill_time_ms(), p.spilled_at_ms);
        p.fill_ts_ms = Some(1_790_811_990_000);
        assert_eq!(p.fill_time_ms(), 1_790_811_990_000, "the venue time wins");
        assert_eq!(utc_day_of(p.fill_time_ms()), "2026-09-30");
        let mut l = {
            let mut x = Ledger::new("live", "2026-10-01");
            x.market = "BTC-USD".into();
            x
        };
        let scope = day_scope(&l.day, p.fill_time_ms());
        assert_eq!(scope, DayScope::CumulativeOnly);
        let PendingResolution::Book(f) = resolve_pending(&p, d("2.25")) else {
            panic!()
        };
        let mut row = None;
        book_fill_scoped(
            &mut l,
            &f,
            p.fill_time_ms(),
            1_790_813_000_000,
            scope,
            |r| {
                row = Some(r.clone());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(l.day_volume, Decimal::ZERO, "never in today's counters");
        assert_eq!(l.cum_volume, d("83.6"));
        assert_eq!(l.position.opened_at_ms, Some(1_790_811_990_000));
        let row = row.unwrap();
        assert_eq!(row["ts_ms"], 1_790_811_990_000u64);
        assert_eq!(row["late_prior_day"], true);
        // Replay keeps it out of the day counters too.
        let mut l2 = {
            let mut x = Ledger::new("live", "2026-10-01");
            x.market = "BTC-USD".into();
            x
        };
        replay_fills(&mut l2, [&row]).unwrap();
        assert_eq!(l2.day_volume, Decimal::ZERO);
        assert_eq!(l2.cum_volume, d("83.6"));
        // Same-day spill → today's counters.
        assert_eq!(
            day_scope("2026-10-01", 1_790_813_000_000),
            DayScope::Current
        );
        // Markouts from the fill's time; all three far too late → missing.
        let (owed, missing) = late_markouts(&f, p.fill_time_ms(), 1_790_813_000_000, 60_000);
        assert!(owed.is_none());
        assert_eq!(missing.len(), 3);
        assert!(missing
            .iter()
            .all(|m| matches!(m, Markout::Missing { reason, .. } if reason == "restart")));
        // A fresh one: only the expired horizons are missing.
        let (owed, missing) = late_markouts(&f, 1_000_000, 1_000_000 + 70_000, 60_000);
        assert_eq!(owed.unwrap().horizons, vec![30, 60]);
        assert_eq!(missing.len(), 1);
    }

    #[test]
    fn a_late_prior_day_fill_does_not_move_todays_daily_net() {
        use std::str::FromStr;
        // pre-G2, Codex P1 4152541373: spilled buy at 100, resolved after the
        // day rolled at a 110 mark → daily_net unchanged by the booking.
        let d = |v: &str| Decimal::from_str(v).unwrap();
        let mut l = Ledger::new("live", "2026-09-30");
        assert_eq!(l.rollover("2026-10-01", Some(d("110"))), Rollover::Rolled);
        assert_eq!(l.daily_net(d("110")), Decimal::ZERO);
        l.record_fill_scoped(
            true,
            d("1"),
            d("100"),
            d("0"),
            true,
            1,
            DayScope::CumulativeOnly,
        );
        assert_eq!(
            l.daily_net(d("110")),
            Decimal::ZERO,
            "booking the late fill moved daily_net"
        );
        // Today's later moves still count: mark 112 → +2.
        assert_eq!(l.daily_net(d("112")), d("2"));
        // A late fill that closes part of a carried position, too.
        let mut l = Ledger::new("live", "2026-09-30");
        l.record_fill(true, d("2"), d("100"), d("0"), true, 1);
        l.rollover("2026-10-01", Some(d("110")));
        let net0 = l.daily_net(d("110"));
        l.record_fill_scoped(
            false,
            d("1"),
            d("105"),
            d("0"),
            false,
            2,
            DayScope::CumulativeOnly,
        );
        assert_eq!(l.daily_net(d("110")), net0);
        assert_eq!(
            l.day_realized,
            Decimal::ZERO,
            "the late realized stays out of today"
        );
    }

    #[test]
    fn a_failed_dir_sync_rolls_the_row_back_so_a_retry_cannot_duplicate_it() {
        // pre-G2, Codex P2 4152482586: an empty journal (left by a failed
        // first append), the row write succeeds, the dir sync fails → the
        // row is rolled back; the retry then leaves exactly one row.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fills.jsonl");
        std::fs::write(&path, b"").unwrap();
        let err = append_journal(
            &path,
            &serde_json::json!({"a": 1}),
            write_all,
            |f, l| f.set_len(l),
            |_| Err(std::io::Error::other("dir sync EIO")),
        );
        assert!(err.is_err());
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            0,
            "row rolled back"
        );
        append_journal(
            &path,
            &serde_json::json!({"a": 1}),
            write_all,
            |f, l| f.set_len(l),
            |_| Ok(()),
        )
        .unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 1, "{text}");
        // A rollback that fails too is JournalUnsafe.
        let other = dir.path().join("other.jsonl");
        std::fs::write(&other, b"").unwrap();
        let err = append_journal(
            &other,
            &serde_json::json!({"b": 1}),
            write_all,
            |_, _| Err(std::io::Error::other("truncate failed")),
            |_| Err(std::io::Error::other("dir sync EIO")),
        )
        .unwrap_err();
        assert!(err.to_string().contains("truncating"), "{err}");
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
        assert!(due_markouts(&mut pending, 5_999, Some(d("1")), 60_000).is_empty());
        let out = due_markouts(&mut pending, 6_000, Some(d("99990")), 60_000);
        assert_eq!(
            out,
            vec![Markout::Priced {
                fill_id: "f".into(),
                horizon_s: 5,
                bps: d("-1")
            }]
        );
        due_markouts(&mut pending, 31_000, Some(d("1")), 60_000);
        assert_eq!(pending.len(), 1);
        due_markouts(&mut pending, 61_000, Some(d("1")), 60_000);
        assert!(pending.is_empty());
    }

    #[test]
    fn markouts_wait_for_a_fresh_mid_and_go_missing_past_the_lateness() {
        let mut pending = vec![PendingMarkout {
            fill_id: "f".into(),
            ts_ms: 1_000,
            px: d("100000"),
            buy: true,
            horizons: vec![5, 30],
        }];
        // 5 s horizon due at 6 000 with a stale book: stays pending.
        assert!(due_markouts(&mut pending, 6_000, None, 60_000).is_empty());
        assert_eq!(pending[0].horizons, vec![5, 30]);
        // Fresh mid later, within the lateness: priced at THAT mid.
        let out = due_markouts(&mut pending, 20_000, Some(d("100020")), 60_000);
        assert_eq!(
            out,
            vec![Markout::Priced {
                fill_id: "f".into(),
                horizon_s: 5,
                bps: d("2")
            }]
        );
        // 30 s horizon due at 31 000; stale until past 91 000 → missing.
        assert!(due_markouts(&mut pending, 91_000, None, 60_000).is_empty());
        let out = due_markouts(&mut pending, 91_001, None, 60_000);
        assert!(matches!(
            out.as_slice(),
            [Markout::Missing { horizon_s: 30, .. }]
        ));
        assert!(pending.is_empty());
    }
}
