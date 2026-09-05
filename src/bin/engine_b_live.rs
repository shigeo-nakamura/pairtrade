//! Engine B live experiment binary (bot-strategy#866, KR-US memory-stock
//! lead-lag) — PROTOTYPE, minimal-notional infrastructure smoke test.
//!
//! This is NOT the validated Phase 1/2 implementation the requirements
//! doc (`engine_b_requirements_0.3.md`) describes. The user explicitly
//! chose, on 2026-09-02, to skip Phase 0A/0B (statistical validation of
//! the H1 hypothesis) and Phase 1 (paper trading) to reach a live trade
//! by 2026-09-10 — see bot-strategy#866's "方針転換の記録" comment. This
//! binary places real orders with an unvalidated signal on a minimal
//! notional ($100/trade, 2x leverage, $1000 account equity) specifically
//! to smoke-test the infrastructure (calendar timing, order placement,
//! KMS credentials, risk rails), not to validate the trading strategy.
//!
//! Standalone binary, NOT part of the pairtrade `strategies:` engine, for
//! the same reason bot-strategy#816's `robinhood_dipgrid.rs` is standalone:
//! Engine B is single-symbol / once-daily discrete-signal / fixed-window
//! exit, architecturally incompatible with pairtrade's two-leg continuous
//! spread engine (bot-strategy#866 architecture decision). Reuses
//! `dex-connector` wiring via `DexConnectorBox`, but re-implements the
//! on-disk KILL_SWITCH / RISK_ACK / atomic-state-write conventions
//! independently (pairtrade's `risk_io`/`status` modules are private to
//! the `pairtrade` module tree, not reachable from `src/bin/`) — same
//! reasoning and pattern as `robinhood_dipgrid.rs`.
//!
//! ## Strategy shape
//!
//! H1: the KR-session residual (KR primary return not explained by the
//! concurrent US primary return) predicts the KRX-close -> US-cash-open
//! forward return on the US primary. The *traded* instrument is the US
//! primary symbol only (a directional bet on its forward return) — this
//! is not a pair/spread trade.
//!
//! `t0` = KRX cash open, `t1` = KRX cash close, `t2` = US cash open (all
//! from the frozen calendar produced by `scripts/engine_b_trading_calendar_freeze.py`,
//! same file the Python Phase 0 observer uses — this binary reads that
//! JSON directly rather than recomputing calendar logic in Rust).
//!
//! Each day: capture the KR/US primary mid price at/after `t0` and again
//! at/after `t1`; compute `epsilon = ln(kr_t1/kr_t0) - ln(us_t1/us_t0)`
//! (`signal_model = "diff"`, the only model implemented in this prototype
//! -- see KNOWN GAPS). If `|epsilon| >= epsilon_threshold`, enter within
//! `t1 .. t1 + entry_deadline_secs` in the direction `sign(epsilon) *
//! direction_multiplier`. Exit (reduce-only) within `t2 .. t2 +
//! exit_deadline_secs`.
//!
//! ## KNOWN GAPS before any live use (see bot-strategy#866, #872-879)
//!
//! - `signal_model = "diff"` is a two-term placeholder for the
//!   requirements doc's 5-coefficient regression (`R_kr = a + b1*R_us +
//!   b2*R_soxl + b3*R_nvda + b4*R_ewy + b5*R_fx + e`, §4.5.3). SOXL/NVDA/
//!   EWY/USDKRW are subscribed and their prices tracked (for a future
//!   `signal_model = "regression"` implementation) but not used by "diff".
//! - `epsilon_threshold` and `direction_multiplier` are operator-supplied
//!   guesses, not fit/frozen from Phase 0A data (that data does not exist
//!   yet at any meaningful sample size) -- see bot-strategy#872.
//! - Entry/exit price is the WS mid at/after the boundary, not a full
//!   top-5-depth VWAP walk (requirements doc §4.5.2's `P_exec_entry`/
//!   `P_exec_exit`). No slippage modeling beyond what
//!   `create_order(price=None)` (Lighter-native IOC + 20% protection
//!   price) already gives. Fill *quantity* is no longer assumed from the
//!   HTTP 200 (bot-strategy#875 G-2/G-4, `docs/engine-b-order-spec.md`
//!   §4 -- that document lands with pairtrade#272): live entries and
//!   exits are confirmed against the exchange's own
//!   position (`get_positions()`, WS-fed `account_all`) within
//!   `fill_confirm_timeout_secs` (polled once per 5 s tick, never a
//!   blocking wait in the select loop; the window starts from a clock
//!   read taken after the send returns), `RiskState.last_session_date` is
//!   persisted *before* the send so a restart mid-confirmation cannot
//!   re-submit, an IOC that leaves no position is
//!   treated as unfilled (no retry that day), at most one entry `sendTx`
//!   is ever sent per session day (a send error is followed by the same
//!   position watch, never by a re-submit -- REST and WS limits are
//!   coupled, so a position read right after a timeout can be a false
//!   negative), a position the exchange already reports before we submit
//!   is adopted instead of re-submitted, and every exit is sized to the
//!   exchange's current position, not to this process's memory of the
//!   entry. PnL still uses the WS mid as the
//!   exit price (the fill price itself is not read back).
//! - No out-of-sample validation (Phase 0B) or paper-trade rehearsal
//!   (Phase 1) of this code before it places real orders.
//! - KR/US primary symbols and `epsilon_threshold` are operator config,
//!   not the data-driven freeze bot-strategy#872 will eventually produce.
//!   A same-day eligibility check (`fetch_order_book_details`,
//!   bot-strategy#872 comment 2026-09-04) guards against entering on a
//!   symbol Lighter itself has gone `force_reduce_only` on or that has
//!   fallen below `min_daily_volume_usd`, but it does not pick the
//!   *better* of two candidates -- that freeze is still #872's job.
//! - `OpenPosition` (the in-flight entry/exit state) is in-memory only,
//!   not persisted to `state_path` -- a crash or restart between entry and
//!   exit loses track of the open position in this process's own state.
//!   `RiskState.last_session_date` prevents re-entering a day already
//!   acted on, but does not resume tracking an existing position for its
//!   scheduled exit. After any restart, check the real Lighter account
//!   position directly rather than trusting this process's state file.
//! - No SIGTERM-graceful-close handling: `systemctl stop` does not
//!   reduce-only-close an open position.
//! - `capture_t0_if_due` locks in `day.t0_prices` on the *first* tick at
//!   or after `t0`, complete or not, and (deliberately, see
//!   `capture_t0_if_due_never_overwrites_an_existing_snapshot`) never
//!   recaptures within the same process run. `RiskState.t0_prices`
//!   recovery (bot-strategy#872 PR #266/#270, the 2026-09-04
//!   silent-signal-loss fix) only helps a *second* same-day restart --
//!   the very first restart between `t0` and `t1` of the day can still
//!   land its first tick on an incomplete `latest_price` (missing
//!   `kr_primary`/`us_primary`, e.g. right after a WS reconnect) with
//!   nothing yet persisted to recover. That case now logs a clear
//!   `[DAY] ... but is missing kr_primary=.../us_primary=... prices`
//!   `WARN` instead of failing silently, but does not by itself recover
//!   the day -- an operator who sees that WARN should manually restart
//!   again (now that an incomplete snapshot is never persisted, a second
//!   attempt gets a fresh, hopefully-complete capture instead of
//!   re-trusting the bad one) rather than assume the gap fixes itself.
//!
//! DRY_RUN must stay on until a human explicitly flips the `refuse_live`
//! gate below (mirrors `robinhood_dipgrid.rs`'s pattern: flipping
//! `ENGINE_B_LIVE_DRY_RUN=false` alone is not enough).

use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use dex_connector::{DexConnector, OrderSide, PositionSnapshot, PriceUpdate};
use reqwest::Client;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use debot::pairtrade::s3_mirror::S3Mirror;

fn init_logger() {
    let offset_seconds = std::env::var("TIMEZONE_OFFSET")
        .unwrap_or_else(|_| "0".to_string())
        .parse::<i32>()
        .unwrap_or(0);
    let offset = FixedOffset::east_opt(offset_seconds).unwrap_or(FixedOffset::east_opt(0).unwrap());
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::Builder::from_env(env)
        .format(move |buf, record| {
            let utc_now: DateTime<Utc> = Utc::now();
            let local_now = utc_now.with_timezone(&offset);
            writeln!(
                buf,
                "{} [{}] - {}",
                local_now.format("%Y-%m-%dT%H:%M:%S%z"),
                record.level(),
                record.args()
            )
        })
        .init();
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

// ---------------------------------------------------------------------
// Trading calendar (reads the SAME frozen JSON the Python Phase 0
// observer uses; see scripts/engine_b_trading_calendar_freeze.py and
// TradingCalendar.load() in scripts/engine_b_phase0.py -- calendar logic
// lives in exactly one place, not duplicated here).
// ---------------------------------------------------------------------

#[derive(Deserialize, Debug, Clone)]
struct SessionEntry {
    krx_is_open: bool,
    krx_open_utc_us: Option<i64>,
    krx_close_utc_us: Option<i64>,
    us_is_open: bool,
    us_open_utc_us: Option<i64>,
}

#[derive(Deserialize, Debug)]
struct TradingCalendarDoc {
    calendar_version: String,
    sessions: HashMap<String, SessionEntry>,
}

struct TradingCalendar {
    calendar_version: String,
    sessions: HashMap<String, SessionEntry>,
}

impl TradingCalendar {
    fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading trading calendar {}", path.display()))?;
        let doc: TradingCalendarDoc =
            serde_json::from_str(&raw).context("parsing trading calendar JSON")?;
        for (date, entry) in &doc.sessions {
            if entry.krx_is_open
                && (entry.krx_open_utc_us.is_none() || entry.krx_close_utc_us.is_none())
            {
                anyhow::bail!("calendar entry {date} has krx_is_open=true but missing open/close");
            }
            if entry.us_is_open && entry.us_open_utc_us.is_none() {
                anyhow::bail!("calendar entry {date} has us_is_open=true but missing us_open");
            }
        }
        Ok(TradingCalendar {
            calendar_version: doc.calendar_version,
            sessions: doc.sessions,
        })
    }

    fn resolve(&self, date: NaiveDate) -> Option<&SessionEntry> {
        self.sessions.get(&date.format("%Y-%m-%d").to_string())
    }
}

/// Today's t0/t1/t2 in UTC microseconds, only if both markets are open.
fn resolve_session_window(calendar: &TradingCalendar, date: NaiveDate) -> Option<(i64, i64, i64)> {
    let entry = calendar.resolve(date)?;
    if !entry.krx_is_open || !entry.us_is_open {
        return None;
    }
    Some((
        entry.krx_open_utc_us?,
        entry.krx_close_utc_us?,
        entry.us_open_utc_us?,
    ))
}

// ---------------------------------------------------------------------
// Signal
// ---------------------------------------------------------------------

/// `signal_model = "diff"`: epsilon = ln(kr_t1/kr_t0) - ln(us_t1/us_t0).
/// Returns `None` (never trades) for any other `signal_model` value, a
/// missing price for either symbol at either timestamp, or a non-positive
/// price. Free function (no engine/connector dependency) so it is
/// directly unit-testable.
fn compute_epsilon(
    signal_model: &str,
    kr_symbol: &str,
    us_symbol: &str,
    t0: &HashMap<String, f64>,
    t1: &HashMap<String, f64>,
) -> Option<f64> {
    if signal_model != "diff" {
        log::error!(
            "[SIGNAL] signal_model={signal_model} not implemented in this prototype -- refusing to trade"
        );
        return None;
    }
    let kr0 = *t0.get(kr_symbol)?;
    let kr1 = *t1.get(kr_symbol)?;
    let us0 = *t0.get(us_symbol)?;
    let us1 = *t1.get(us_symbol)?;
    if kr0 <= 0.0 || kr1 <= 0.0 || us0 <= 0.0 || us1 <= 0.0 {
        return None;
    }
    Some((kr1 / kr0).ln() - (us1 / us0).ln())
}

// ---------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
struct EngineBLiveConfig {
    instance_id: String,
    dry_run: bool,
    kr_primary_symbol: String,
    us_primary_symbol: String,
    control_symbols: Vec<String>,
    lot_usd: f64,
    leverage: u32,
    epsilon_threshold: f64,
    direction_multiplier: f64,
    signal_model: String,
    entry_deadline_secs: i64,
    exit_deadline_secs: i64,
    /// How long to wait for the exchange's WS-fed position to reflect an
    /// accepted IOC before treating it as unfilled (entry) or as
    /// still-open (exit). Lighter's `account_all` update normally lands
    /// within ~1 s of the fill; 15 s leaves room for a WS hiccup without
    /// eating the 180 s entry window (bot-strategy#875 G-2).
    fill_confirm_timeout_secs: i64,
    lighter_rest_url: String,
    min_daily_volume_usd: f64,
    equity_usd_reference: f64,
    max_session_loss_bps: f64,
    trading_calendar_path: PathBuf,
    kill_switch_path: PathBuf,
    risk_ack_path: PathBuf,
    state_path: PathBuf,
    status_path: PathBuf,
    pnl_log_path: PathBuf,
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

fn env_symbol_list(name: &str, default: &[&str]) -> Vec<String> {
    std::env::var(name)
        .ok()
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_else(|| default.iter().map(|s| s.to_string()).collect())
}

impl EngineBLiveConfig {
    fn from_env() -> Self {
        let instance_id = env_string("ENGINE_B_LIVE_INSTANCE_ID", "engine-b-live");
        // Two distinct roots, matching engine-b-phase0's split: CODE_DIR is
        // root-owned/read-only (binary, trading_calendar.json, shipped by
        // the installer); STATE_DIR is this process's own writable area
        // (systemd StateDirectory=, /var/lib/... by convention). Mixing
        // writable state into the read-only code dir would force
        // ProtectSystem=full instead of the tighter =strict.
        let code_dir = env_string("ENGINE_B_LIVE_CODE_DIR", "/opt/engine-b-live");
        let base_dir = env_string("ENGINE_B_LIVE_BASE_DIR", "/var/lib/engine-b-live");
        EngineBLiveConfig {
            dry_run: env_bool("ENGINE_B_LIVE_DRY_RUN", true),
            kr_primary_symbol: env_string("ENGINE_B_LIVE_KR_PRIMARY", "SKHY"),
            us_primary_symbol: env_string("ENGINE_B_LIVE_US_PRIMARY", "SNDK"),
            control_symbols: env_symbol_list(
                "ENGINE_B_LIVE_CONTROL_SYMBOLS",
                &["SOXL", "NVDA", "EWY", "USDKRW"],
            ),
            lot_usd: env_f64("ENGINE_B_LIVE_LOT_USD", 100.0),
            leverage: env_u32("ENGINE_B_LIVE_LEVERAGE", 2),
            epsilon_threshold: env_f64("ENGINE_B_LIVE_EPSILON_THRESHOLD", 0.003),
            direction_multiplier: env_f64("ENGINE_B_LIVE_DIRECTION_MULTIPLIER", 1.0),
            signal_model: env_string("ENGINE_B_LIVE_SIGNAL_MODEL", "diff"),
            entry_deadline_secs: env_i64("ENGINE_B_LIVE_ENTRY_DEADLINE_SECS", 180),
            exit_deadline_secs: env_i64("ENGINE_B_LIVE_EXIT_DEADLINE_SECS", 900),
            fill_confirm_timeout_secs: env_i64("ENGINE_B_LIVE_FILL_CONFIRM_TIMEOUT_SECS", 15),
            lighter_rest_url: env_string(
                "ENGINE_B_LIVE_LIGHTER_REST_URL",
                "https://mainnet.zklighter.elliot.ai",
            ),
            // Same placeholder value as engine_b_phase0.py's
            // MIN_DAILY_VOLUME_USD (TBD-9, bot-strategy#872) -- not yet
            // data-driven-frozen, just the same operator guess reused here
            // so the two eligibility checks agree until #872 freezes it.
            min_daily_volume_usd: env_f64("ENGINE_B_LIVE_MIN_DAILY_VOLUME_USD", 100_000.0),
            equity_usd_reference: env_f64("ENGINE_B_LIVE_EQUITY_USD_REFERENCE", 1000.0),
            max_session_loss_bps: env_f64("ENGINE_B_LIVE_MAX_SESSION_LOSS_BPS", 500.0),
            trading_calendar_path: PathBuf::from(env_string(
                "ENGINE_B_LIVE_TRADING_CALENDAR_PATH",
                &format!("{code_dir}/trading_calendar.json"),
            )),
            kill_switch_path: PathBuf::from(env_string(
                "ENGINE_B_LIVE_KILL_SWITCH_PATH",
                &format!("{base_dir}/KILL_SWITCH"),
            )),
            risk_ack_path: PathBuf::from(env_string(
                "ENGINE_B_LIVE_RISK_ACK_PATH",
                &format!("{base_dir}/RISK_ACK_{}", instance_id.to_uppercase()),
            )),
            state_path: PathBuf::from(env_string(
                "ENGINE_B_LIVE_STATE_PATH",
                &format!("{base_dir}/risk_state.json"),
            )),
            status_path: PathBuf::from(env_string(
                "ENGINE_B_LIVE_STATUS_PATH",
                &format!("{base_dir}/status.json"),
            )),
            pnl_log_path: PathBuf::from(env_string(
                "ENGINE_B_LIVE_PNL_LOG_PATH",
                &format!("{base_dir}/pnl.jsonl"),
            )),
            instance_id,
        }
    }

    /// All symbols this process needs price updates for.
    fn all_symbols(&self) -> Vec<String> {
        let mut symbols = vec![
            self.kr_primary_symbol.clone(),
            self.us_primary_symbol.clone(),
        ];
        symbols.extend(self.control_symbols.iter().cloned());
        symbols
    }

    /// Hard notional cap derived from equity * leverage, independent of
    /// `lot_usd` misconfiguration -- mirrors pairtrade's
    /// `sizing.rs::cap_leg_notional` formula (equity * max_leverage *
    /// headroom). This is the last line of defense against a config typo
    /// sending an oversized order, not a substitute for getting
    /// `lot_usd`/`leverage` right in the first place.
    fn max_notional_usd(&self) -> f64 {
        self.equity_usd_reference * self.leverage as f64 * 0.9
    }
}

// ---------------------------------------------------------------------
// Risk state (same on-disk pattern as robinhood_dipgrid.rs: atomic
// tmp+rename JSON, sticky halt cleared only by RISK_ACK)
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct RiskState {
    #[serde(default)]
    session_start_equity: f64,
    #[serde(default)]
    peak_equity: f64,
    #[serde(default)]
    realized_pnl_session: f64,
    /// Realized PnL since the last `roll_day_if_needed` date change, reset
    /// to 0.0 there -- unlike `realized_pnl_session` (lifetime-since-halt-
    /// clear, never reset by a day roll), this is what the dashboard's
    /// `pnl_today` field means. Fixed after review caught it defaulting to
    /// a copy of the lifetime total (bot-strategy#866 PR #255 review) --
    /// same bug class `pairtrade::status.rs`'s `pnl_today`/`pnl_today_date`
    /// split was introduced to fix (single-instance -> A/B/C cutover
    /// incident referenced there).
    #[serde(default)]
    pnl_today: f64,
    /// UTC date (YYYY-MM-DD) `pnl_today` was last reset for. Persisted
    /// (unlike the engine's in-memory-only `current_date`) so a same-day
    /// restart can tell "still today" apart from a genuine day change --
    /// see `roll_day_if_needed`.
    #[serde(default)]
    pnl_today_date: Option<String>,
    #[serde(default)]
    total_trades: u64,
    #[serde(default)]
    total_wins: u64,
    #[serde(default)]
    max_dd_bps: f64,
    /// Same running-max-drawdown as `max_dd_bps`, in USD instead of bps
    /// (peak_equity - current_equity at each new max) -- the dashboard's
    /// `trade_stats.max_dd` expects a dollar amount, matching
    /// `pairtrade::mod.rs`'s `peak_pnl - total_pnl` convention.
    /// `max_dd_bps` stays authoritative for the `max_session_loss_bps`
    /// halt gate below; this field exists purely for dashboard display.
    #[serde(default)]
    max_dd_usd: f64,
    #[serde(default)]
    session_halted: bool,
    #[serde(default)]
    session_halt_reason: Option<String>,
    /// UTC date (YYYY-MM-DD) of the last session this process has already
    /// acted on (entered, or explicitly skipped), so a restart mid-day
    /// never re-evaluates a boundary it already passed.
    #[serde(default)]
    last_session_date: Option<String>,
    /// Persisted copy of `DaySnapshot.t0_prices` (KRX-open mid prices),
    /// keyed by `t0_snapshot_date` -- restores the *true* t0 snapshot
    /// across a restart that happens between `t0` and `t1`, instead of
    /// `capture_t0_if_due` silently re-capturing a wrong one from
    /// whatever `latest_price` happens to hold on the new process's
    /// first tick (which may be near-empty right after a WS reconnect).
    /// Discovered live 2026-09-04: a restart at 06:03 UTC (t0=00:00,
    /// t1=06:30) wiped the in-memory-only t0 snapshot, the fresh
    /// recapture silently missed kr_primary/us_primary (WS had not yet
    /// delivered a tick for them), and `compute_epsilon`'s `?`-early-
    /// returns on a missing key produce no log line at all -- the day's
    /// entry decision failed completely silently, surfacing only as
    /// "entry_deadline passed without a valid signal" at the deadline,
    /// indistinguishable in the log from a genuine no-signal day
    /// (bot-strategy#872 PR #266 follow-up).
    #[serde(default)]
    t0_snapshot_date: Option<String>,
    #[serde(default)]
    t0_prices: HashMap<String, f64>,
    /// A sendTx went out (or errored ambiguously) and the exchange position
    /// could not be read for the whole confirm window, so a live position
    /// may exist that this process does not track. Persisted (not
    /// day-scoped) so a midnight `roll_day_if_needed` cannot make the
    /// status look trustworthy again; cleared only by adopting the
    /// position from the exchange or by the operator's RISK_ACK, which
    /// also lifts the session halt raised at the same time
    /// (pairtrade#275 Codex review).
    #[serde(default)]
    position_unconfirmed: bool,
}

fn load_state(path: &Path) -> RiskState {
    match std::fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
        Err(_) => RiskState::default(),
    }
}

fn atomic_write_json(path: &Path, value: &impl Serialize) {
    match serde_json::to_string_pretty(value) {
        Ok(json) => atomic_write_bytes(path, json.as_bytes()),
        Err(_) => log::warn!("[STATE] serialize failed for {}", path.display()),
    }
}

/// Shared tmp+rename atomic-write primitive. Split out from
/// `atomic_write_json` so `write_status_if_due` can serialize the status
/// document exactly once and reuse the same bytes for both this local
/// write and the S3 mirror `put_async` call, instead of serializing twice
/// per tick (bot-strategy#866 PR #255 review round 2, nit 4).
fn atomic_write_bytes(path: &Path, bytes: &[u8]) {
    if let Err(e) = atomic_write_bytes_checked(path, bytes) {
        log::warn!("[STATE] write failed for {}: {e}", path.display());
    }
}

/// Same tmp+rename write, but reports failure to the caller. Used where a
/// durable write is a precondition for acting (the pre-send entry marker,
/// pairtrade#275 Codex review) rather than best-effort bookkeeping.
fn atomic_write_bytes_checked(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "no parent dir"))?;
    let tmp = dir.join(format!(
        ".{}.tmp.{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id()
    ));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

fn atomic_write_json_checked(path: &Path, value: &impl Serialize) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(value).map_err(std::io::Error::other)?;
    atomic_write_bytes_checked(path, json.as_bytes())
}

fn append_pnl_log(path: &Path, record: &serde_json::Value) {
    let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    else {
        log::warn!("[PNL_LOG] open failed: {}", path.display());
        return;
    };
    let _ = writeln!(f, "{record}");
}

// ---------------------------------------------------------------------
// Day-scoped state machine
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
struct OpenPosition {
    side: OrderSide,
    entry_price: f64,
    /// True when `entry_price` is the WS mid at (or near) entry rather
    /// than the exchange's own `avg_entry_price` -- the booked PnL is then
    /// an estimate (pairtrade#275 review finding 7).
    entry_price_estimated: bool,
    /// Quantity confirmed at entry. PnL is booked on this, not on whatever
    /// remains after partial exits (pairtrade#275 Codex review).
    size: f64,
    /// Quantity the exchange still reports open; shrinks across partial
    /// exit retries and is what the next reduce-only is sized from.
    open_size: f64,
    /// PnL already realized by partial reductions observed before the
    /// final flat, each booked at the WS mid of the attempt that closed
    /// it (pairtrade#275 Codex review): `on_exit` adds only the last
    /// open remainder at the final price.
    realized_partial_pnl: f64,
    entered_at_us: i64,
    /// This position does not belong to today's signal (adopted from the
    /// exchange with unknown origin, recovered after an UNCONFIRMED send,
    /// or carried over midnight because its exit kept failing): its
    /// intended exit window is unknown or already past, so `maybe_exit`
    /// flattens it on the next tick instead of waiting for today's `t2`
    /// (pairtrade#275 Codex review).
    flatten_asap: bool,
}

/// Fill / flat confirmation in flight, advanced by `poll_pending_confirm`
/// once per tick instead of a blocking wait (see that fn's doc).
#[derive(Debug, Clone)]
enum PendingConfirm {
    /// An entry sendTx was sent (or failed ambiguously); waiting for the
    /// exchange to show a `us_primary` position, or for the window to end.
    Entry {
        side: OrderSide,
        requested: f64,
        price: f64,
        epsilon: f64,
        notional_usd: f64,
        deadline_us: i64,
        /// `Some(err)` when the sendTx itself returned an error -- we still
        /// watch the exchange because the order may have been accepted.
        after_send_error: Option<String>,
        /// At least one successful `get_positions()` during the window.
        saw_reading: bool,
    },
    /// A reduce-only exit was accepted; waiting for the exchange to report
    /// flat, or for the window to end.
    Exit {
        exit_price: f64,
        deadline_us: i64,
        saw_reading: bool,
    },
}

/// Refuse to trust an exchange-reported position more than this many
/// times what we recorded at fill confirmation when sizing an exit
/// (pairtrade's `cap_exit_qty`, bot-strategy#259, exists for exactly such
/// a transient over-report). reduce_only still bounds the order on the
/// exchange side; this keeps our own accounting from swallowing the bad
/// number.
const EXIT_SIZE_CAP_RATIO: f64 = 1.5;

/// Returns the reduce-only size to send and whether the exchange's number
/// was capped. A tracked size of zero disables the cap (nothing to compare
/// against).
fn cap_exit_size(exchange_size: f64, tracked_size: f64) -> (f64, bool) {
    if tracked_size > 0.0 && exchange_size > tracked_size * EXIT_SIZE_CAP_RATIO + 1e-12 {
        (tracked_size, true)
    } else {
        (exchange_size, false)
    }
}

/// The exchange's own view of one symbol's position, reduced from the
/// connector's `PositionSnapshot` (abs `size` + `sign`, Lighter: `1` long,
/// `-1` short). This -- not `OpenPosition`, which is only this process's
/// memory -- is what live entry confirmation and exit sizing use
/// (bot-strategy#875 G-2/G-4).
#[derive(Debug, Clone, PartialEq)]
struct ExchangePosition {
    side: OrderSide,
    size: f64,
    entry_price: Option<f64>,
}

/// Find `symbol`'s open position in a `get_positions()` result. A zero /
/// negative size or a zero sign counts as flat (`None`), matching how the
/// Lighter connector drops zero-size positions from its snapshot.
fn exchange_position_for(positions: &[PositionSnapshot], symbol: &str) -> Option<ExchangePosition> {
    let p = positions.iter().find(|p| p.symbol == symbol)?;
    let size = p.size.abs().to_f64().unwrap_or(0.0);
    if size <= 0.0 {
        return None;
    }
    let side = match p.sign {
        s if s > 0 => OrderSide::Long,
        s if s < 0 => OrderSide::Short,
        _ => return None,
    };
    Some(ExchangePosition {
        side,
        size,
        entry_price: p.entry_price.and_then(|d| d.to_f64()).filter(|v| *v > 0.0),
    })
}

fn opposite(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Long => OrderSide::Short,
        OrderSide::Short => OrderSide::Long,
    }
}

#[derive(Debug, Clone, Default)]
struct DaySnapshot {
    t0_prices: Option<HashMap<String, f64>>,
    t1_prices: Option<HashMap<String, f64>>,
    entered: bool,
    exited: bool,
    /// True when `entered` was restored from `RiskState.last_session_date`
    /// after a restart (roll_day_if_needed's recovery branch), rather than
    /// decided by this process's own `maybe_enter` this run. `position` is
    /// in-memory only and never persisted/reconciled with the real
    /// exchange (see this file's KNOWN GAPS), so it cannot be trusted for
    /// the rest of the day once this is true -- drives `positions_ready`
    /// in the dashboard status payload (bot-strategy#866 PR #255 review
    /// round 2).
    restart_recovered: bool,
    /// Set only once the Lighter `orderBookDetails` eligibility check has
    /// produced a *definitive* answer for today (a response that parsed,
    /// whether it found both symbols eligible or not) -- deliberately
    /// **not** set on a fetch/parse error, so a transient network blip
    /// gets retried on the next 5s tick instead of permanently skipping
    /// the one entry opportunity for the day on a single hiccup
    /// (bot-strategy#872 PR #266 self-review, non-blocking finding).
    eligibility_confirmed: bool,
    /// One entry per ineligible symbol found (`kr_primary`/`us_primary`,
    /// either or both) -- kept as a `Vec` rather than the first-match-wins
    /// `Option<String>` this started as, so a same-day gate on an
    /// exchange-wide event (e.g. both symbols going `force_reduce_only`
    /// at once) still surfaces both reasons in the log/status instead of
    /// silently dropping the second (bot-strategy#872 PR #266
    /// self-review). Non-empty means `maybe_enter` skips today's entry.
    /// Stays empty on a fetch/parse failure (fail-open: see
    /// `eligibility_confirmed`'s doc comment) -- see bot-strategy#872.
    ineligible_reasons: Vec<String>,
}

/// Snapshot `prices` into `day.t0_prices` the first time `now_us` reaches
/// `t0` (KRX open), and never again for the same `DaySnapshot`. Free
/// function (no engine/connector dependency) so this timing-critical
/// capture is directly unit-testable independent of `maybe_enter`'s
/// entry-window gating -- see `EngineBLiveEngine::maybe_capture_t0`'s doc
/// comment for why this must never run only as a side effect of the
/// entry-window check.
fn capture_t0_if_due(
    window: Option<(i64, i64, i64)>,
    day: &mut DaySnapshot,
    now_us: i64,
    prices: &HashMap<String, f64>,
) {
    let Some((t0, _t1, _t2)) = window else { return };
    if day.t0_prices.is_none() && now_us >= t0 {
        day.t0_prices = Some(prices.clone());
    }
}

/// Whether `prices` actually has what `compute_epsilon` needs --
/// `kr_symbol` and `us_symbol` both present. A map that is merely
/// non-empty is not good enough: `all_symbols()` subscribes
/// `control_symbols` (SOXL/NVDA/EWY/USDKRW) alongside the two primaries,
/// and WS delivery order across symbols is not guaranteed, so a snapshot
/// taken moments after a (re)connect can easily hold only control-symbol
/// prices. Shared by the capture path (decides whether to persist/WARN)
/// and the recovery path (decides whether a persisted snapshot is safe
/// to trust) so both apply the exact same bar (PR #270 review finding:
/// the first cut of this fix checked only "non-empty", which would have
/// silently trusted/re-persisted exactly this kind of partial snapshot
/// on a second same-day restart).
fn t0_snapshot_has_required_symbols(
    prices: &HashMap<String, f64>,
    kr_symbol: &str,
    us_symbol: &str,
) -> bool {
    prices.contains_key(kr_symbol) && prices.contains_key(us_symbol)
}

/// `Some(prices)` when `RiskState`'s persisted t0 snapshot is usable for
/// `today_str` -- same date, and containing both `kr_symbol` and
/// `us_symbol` (see `t0_snapshot_has_required_symbols`). `None` otherwise
/// (no snapshot yet, a stale one from a previous day, or an incomplete
/// one). Free/pure so `EngineBLiveEngine::roll_day_if_needed`'s recovery
/// decision is unit-testable without a live `DexConnector`
/// (bot-strategy#872 PR #266 follow-up fix for the 2026-09-04
/// silent-signal-loss incident -- see `RiskState.t0_snapshot_date`'s doc
/// comment).
fn recoverable_t0_prices(
    today_str: &str,
    snapshot_date: Option<&str>,
    snapshot_prices: &HashMap<String, f64>,
    kr_symbol: &str,
    us_symbol: &str,
) -> Option<HashMap<String, f64>> {
    if snapshot_date == Some(today_str)
        && t0_snapshot_has_required_symbols(snapshot_prices, kr_symbol, us_symbol)
    {
        Some(snapshot_prices.clone())
    } else {
        None
    }
}

// ---------------------------------------------------------------------
// Same-day market eligibility gate (bot-strategy#872). Queries Lighter's
// public, unauthenticated `orderBookDetails` REST endpoint directly --
// the same endpoint and field names `engine_b_phase0.py`'s
// `poll_venue()` already uses to compute its own `is_eligible` column --
// rather than extending `DexConnector` for a read-only public-data need
// (would need a `DexConnectorBox` forwarding override for every other
// connector, see CLAUDE.md's dex-connector pitfall list, for a method
// only this prototype calls).
// ---------------------------------------------------------------------

#[derive(Deserialize, Debug)]
struct OrderBookDetailsResponse {
    #[serde(default)]
    order_book_details: Vec<OrderBookDetail>,
}

#[derive(Deserialize, Debug)]
struct OrderBookDetail {
    symbol: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    market_config: Option<MarketConfig>,
    /// A bare JSON number on the real endpoint (verified 2026-09-04 both
    /// against `~/bot/lighter-python/docs/PerpsOrderBookDetail.md`'s
    /// `float` type and a live curl of
    /// `mainnet.zklighter.elliot.ai/api/v1/orderBookDetails`), NOT a
    /// quoted string like most other decimal fields in this codebase --
    /// `Option<String>` here would make `serde_json`'s `.json()` call
    /// fail on every real response, silently fail-opening the gate on
    /// every single call (bot-strategy#872 PR #266 self-review, blocking
    /// finding). `engine_b_phase0.py`'s `poll_venue()` gets away with
    /// `canonical_decimal(detail.get(...))` because Python's `json`
    /// module hands it a `float` either way; Rust's static typing does
    /// not forgive the same assumption.
    #[serde(default)]
    daily_quote_token_volume: Option<f64>,
}

#[derive(Deserialize, Debug, Default)]
struct MarketConfig {
    #[serde(default)]
    force_reduce_only: bool,
}

/// `client` is reused across calls (built once in `main()`, same pattern
/// as `bull_holder.rs`'s `Engine.http`) rather than a fresh `Client::new()`
/// per call. This call is `.await`ed inline inside `tick()`'s single
/// `tokio::select!` loop (same as `create_order`/`set_leverage` already
/// are for the actual order submission a few lines below it in
/// `maybe_enter`) -- a slow response stalls price-feed draining and
/// KILL_SWITCH polling for up to `timeout`, so it is kept short (5s,
/// tighter than bull_holder.rs's 15s) rather than the crate default.
async fn fetch_order_book_details(client: &Client, rest_url: &str) -> Result<Vec<OrderBookDetail>> {
    let url = format!("{}/api/v1/orderBookDetails", rest_url.trim_end_matches('/'));
    let resp: OrderBookDetailsResponse = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .context("orderBookDetails request failed")?
        .error_for_status()
        .context("orderBookDetails non-2xx response")?
        .json()
        .await
        .context("orderBookDetails parse failed")?;
    Ok(resp.order_book_details)
}

/// Same three eligibility checks and reason strings as
/// `engine_b_phase0.py`'s `poll_venue()` (`status != "active"` /
/// `force_reduce_only` / `daily_volume_below_min`), so a symbol's
/// eligibility reads identically in both this binary's log and the
/// Phase 0A observer's `market_status` table.
fn evaluate_eligibility(detail: &OrderBookDetail, min_daily_volume_usd: f64) -> (bool, String) {
    let status = detail.status.as_deref().unwrap_or("unknown");
    let force_reduce_only = detail
        .market_config
        .as_ref()
        .map(|c| c.force_reduce_only)
        .unwrap_or(false);
    let volume = detail.daily_quote_token_volume.unwrap_or(0.0);
    let mut reasons = Vec::new();
    if status != "active" {
        reasons.push(format!("status={status}"));
    }
    if force_reduce_only {
        reasons.push("force_reduce_only".to_string());
    }
    if volume < min_daily_volume_usd {
        reasons.push("daily_volume_below_min".to_string());
    }
    (reasons.is_empty(), reasons.join(","))
}

#[derive(Serialize)]
struct DashboardPosition {
    symbol: String,
    side: &'static str,
    size: String,
    entry_price: String,
}

#[derive(Serialize)]
struct DashboardTradeStats {
    trades: u64,
    wins: u64,
    win_rate: f64,
    max_dd: f64,
    pnl: f64,
}

/// debot-dashboard's `StatusData` (`main.go`) fields this binary
/// populates. That struct lives in a different repo, so this is still a
/// by-hand sync, but a typed struct at least catches a field-name typo
/// at compile time instead of `serde_json::json!()`'s stringly-typed
/// keys silently producing a field the dashboard never reads
/// (bot-strategy#866 PR #255 review round 2, nit 3).
#[derive(Serialize)]
struct DashboardStatus {
    ts: i64,
    updated_at: String,
    id: String,
    dex: &'static str,
    dry_run: bool,
    has_position: bool,
    position_count: i32,
    positions_ready: bool,
    positions: Vec<DashboardPosition>,
    pnl_total: f64,
    pnl_today: f64,
    pnl_source: &'static str,
    kill_switch_active: bool,
    trade_stats: DashboardTradeStats,
}

/// engine_b_live-specific fields, not part of debot-dashboard's schema --
/// ignored by its Go `json.Unmarshal`, useful for journalctl/manual
/// inspection. Flattened into the same document as `DashboardStatus` via
/// `#[serde(flatten)]` so both shapes coexist in one `status.json`.
#[derive(Serialize)]
struct EngineStatusExtra {
    ts_us: i64,
    instance_id: String,
    current_date: Option<String>,
    window: Option<(i64, i64, i64)>,
    day_entered: bool,
    day_exited: bool,
    restart_recovered: bool,
    /// Mirrors `DaySnapshot.ineligible_reasons` (bot-strategy#872) --
    /// empty until proven otherwise (no eligibility check has run yet, or
    /// every check so far errored and is being retried) or once a
    /// definitive ineligible finding has been logged; non-empty means
    /// today's entry, if any, was skipped for this reason rather than an
    /// epsilon-below-threshold no-signal day. Lets an operator read
    /// `status.json` instead of grepping journalctl for `[ELIGIBILITY]`
    /// (bot-strategy#872 PR #266 self-review nit).
    eligibility_ineligible_reasons: Vec<String>,
    session_halted: bool,
    session_halt_reason: Option<String>,
    realized_pnl_session: f64,
    pnl_today_date: Option<String>,
    total_trades: u64,
    total_wins: u64,
    max_dd_bps: f64,
    max_dd_usd: f64,
    kill_switch: bool,
    calendar_version: String,
}

/// Engine-B-specific dashboard block, nested under a named `han_bridge`
/// key (same pattern as `hype-accumulator`'s `accumulator` block in
/// debot-dashboard's `StatusData`) rather than flattened alongside
/// `DashboardStatus`/`EngineStatusExtra` -- lets debot-dashboard's web UI
/// render an extra section only for this target (gated on the field's
/// presence, `omitempty` on the Go side) without touching how any other
/// bot's card renders. Answers "why didn't it trade today" without
/// grepping journalctl: which symbols this instance trades, whether
/// today's entry/exit decision has already been made, and why an entry
/// was skipped if it was (bot-strategy#872 dashboard follow-up).
#[derive(Serialize)]
struct HanBridgeStatus {
    kr_primary_symbol: String,
    us_primary_symbol: String,
    day_entered: bool,
    day_exited: bool,
    /// True while today's entry sendTx has an unknown outcome (see
    /// `RiskState.position_unconfirmed`); `positions_ready` is false too.
    position_unconfirmed: bool,
    ineligible_reasons: Vec<String>,
    session_halt_reason: Option<String>,
}

#[derive(Serialize)]
struct FullStatus {
    #[serde(flatten)]
    dashboard: DashboardStatus,
    #[serde(flatten)]
    extra: EngineStatusExtra,
    han_bridge: HanBridgeStatus,
}

struct EngineBLiveEngine {
    cfg: EngineBLiveConfig,
    connector: std::sync::Arc<dyn DexConnector + Send + Sync>,
    calendar: TradingCalendar,
    /// Reused across `fetch_order_book_details` calls (same pattern as
    /// `bull_holder.rs`'s `Engine.http`) instead of a fresh `Client::new()`
    /// per call.
    http_client: Client,
    latest_price: HashMap<String, f64>,
    current_date: Option<NaiveDate>,
    window: Option<(i64, i64, i64)>, // (t0, t1, t2) us epoch for current_date
    day: DaySnapshot,
    position: Option<OpenPosition>,
    /// Fill / flat confirmation awaiting the exchange (live only).
    pending: Option<PendingConfirm>,
    state: RiskState,
    last_status_write_us: i64,
    status_s3_mirror: Option<Arc<S3Mirror>>,
}

impl EngineBLiveEngine {
    fn kill_switch_engaged(&self) -> bool {
        self.cfg.kill_switch_path.exists()
    }

    fn maybe_clear_halt(&mut self) {
        if self.state.session_halted && self.cfg.risk_ack_path.exists() {
            log::warn!(
                "[RISK_ACK] clearing session halt (reason was: {:?}) via {}",
                self.state.session_halt_reason,
                self.cfg.risk_ack_path.display()
            );
            self.state.session_halted = false;
            self.state.session_halt_reason = None;
            if self.state.position_unconfirmed {
                log::warn!(
                    "[RISK_ACK] clearing position_unconfirmed -- operator asserts the exchange was \
                     reconciled"
                );
                self.state.position_unconfirmed = false;
            }
            self.state.peak_equity =
                self.state.session_start_equity + self.state.realized_pnl_session;
            atomic_write_json(&self.cfg.state_path, &self.state);
            if let Err(e) = std::fs::remove_file(&self.cfg.risk_ack_path) {
                log::warn!(
                    "[RISK_ACK] failed to remove {} after ack: {e:?}",
                    self.cfg.risk_ack_path.display()
                );
            }
        }
    }

    fn entries_allowed(&self) -> bool {
        !self.kill_switch_engaged() && !self.state.session_halted
    }

    /// Roll to a new UTC date's session window if the wall-clock date has
    /// advanced. A restart mid-day resumes the same date's DaySnapshot
    /// from scratch (in-memory only -- position/entry state does not
    /// survive a restart in this prototype; see KNOWN GAPS) but will not
    /// re-enter if `state.last_session_date` already covers today.
    fn roll_day_if_needed(&mut self, now_us: i64) {
        let today = DateTime::<Utc>::from_timestamp_micros(now_us)
            .expect("valid timestamp")
            .date_naive();
        if self.current_date == Some(today) {
            return;
        }
        self.current_date = Some(today);
        self.day = DaySnapshot::default();
        if self.position.is_some() || self.pending.is_some() {
            // Carry-over across midnight (an exit still failing or still
            // being confirmed): today's entry is blocked outright, and it
            // must be blocked *now* -- if the pending exit resolves flat
            // before t1 the position is gone and a t1-time check would
            // let a new entry through (pairtrade#275 Codex review).
            log::warn!(
                "[DAY] {today} starts with a carried-over position/exit in flight; no new entry today, \
                 and the position is flattened on the next tick rather than at today's t2"
            );
            if let Some(p) = self.position.as_mut() {
                p.flatten_asap = true;
            }
            self.day.entered = true;
            self.state.last_session_date = Some(today.to_string());
            atomic_write_json(&self.cfg.state_path, &self.state);
        }
        // Keyed off the *persisted* pnl_today_date, not the in-memory
        // current_date this function just reset -- current_date is always
        // None right after process start (main() initializes it that way),
        // so comparing against it would treat a same-day restart as a new
        // day and wipe pnl_today back to 0.0 for the rest of the day
        // (bot-strategy#866 PR #255 review round 2, bug 1).
        let today_str = today.to_string();
        if self.state.pnl_today_date.as_deref() != Some(today_str.as_str()) {
            self.state.pnl_today = 0.0;
            self.state.pnl_today_date = Some(today_str.clone());
        }
        self.window = resolve_session_window(&self.calendar, today);
        match self.window {
            Some((t0, t1, t2)) => log::info!(
                "[DAY] {today} calendar_version={} t0={t0} t1={t1} t2={t2}",
                self.calendar.calendar_version
            ),
            None => log::info!(
                "[DAY] {today} skipped: KRX and/or US cash market closed (calendar_version={})",
                self.calendar.calendar_version
            ),
        }
        if self.state.last_session_date.as_deref() == Some(today_str.as_str()) {
            log::info!("[DAY] {today} already acted on before a restart; not re-entering");
            self.day.entered = true;
            self.day.restart_recovered = true;
        }
        // Recover a t0 snapshot a prior run of this process already
        // captured and persisted today, rather than letting
        // `maybe_capture_t0` silently re-capture a wrong one from
        // whatever `latest_price` holds on this process's first tick
        // (see `RiskState.t0_snapshot_date`'s doc comment for the
        // 2026-09-04 incident this fixes).
        if let Some(prices) = recoverable_t0_prices(
            &today_str,
            self.state.t0_snapshot_date.as_deref(),
            &self.state.t0_prices,
            &self.cfg.kr_primary_symbol,
            &self.cfg.us_primary_symbol,
        ) {
            log::info!(
                "[DAY] {today} recovered t0 snapshot from persisted state ({} symbols) -- \
                 restart happened after t0, before a fresh capture would have run",
                prices.len()
            );
            self.day.t0_prices = Some(prices);
        }
    }

    fn snapshot_prices(&self) -> HashMap<String, f64> {
        self.latest_price.clone()
    }

    async fn submit_order(&self, side: OrderSide, size: f64, reduce_only: bool) -> Result<Decimal> {
        let size_dec = Decimal::from_str(&format!("{size:.8}")).context("size to Decimal")?;
        if self.cfg.dry_run {
            log::info!(
                "[DRY_RUN] would submit {side} size={size_dec} reduce_only={reduce_only} symbol={}",
                self.cfg.us_primary_symbol
            );
            return Ok(size_dec);
        }
        let resp = self
            .connector
            .create_order(
                &self.cfg.us_primary_symbol,
                size_dec,
                side,
                None,
                None,
                reduce_only,
                None,
            )
            .await
            .context("create_order failed")?;
        resp.ordered_size
            .to_f64()
            .map(|f| Decimal::from_str(&format!("{f:.8}")).unwrap_or(size_dec))
            .ok_or_else(|| anyhow::anyhow!("ordered_size not representable"))
    }

    /// Snapshot each subscribed symbol's current mid price once, the first
    /// tick at/after `t0` (KRX open) each day. Deliberately unconditional
    /// on the entry-window check in `maybe_enter` (`now_us < t1` gate) --
    /// t0 is normally hours before t1 (KRX open to close), and pulling this
    /// capture inside a function that returns early before t1 would mean
    /// it only ever runs once we are already at/past t1, collapsing the
    /// t0 and t1 snapshots into the same instant and making
    /// `compute_epsilon` return ~0.0 every day. Must run every tick
    /// regardless of `self.day.entered`/entry-window state.
    fn maybe_capture_t0(&mut self, now_us: i64) {
        let was_none = self.day.t0_prices.is_none();
        capture_t0_if_due(self.window, &mut self.day, now_us, &self.latest_price);
        if !was_none {
            return;
        }
        let Some(prices) = self.day.t0_prices.clone() else {
            return;
        };
        let Some((t0, _, _)) = self.window else {
            return;
        };
        let delay_secs = (now_us - t0) as f64 / 1_000_000.0;
        let complete = t0_snapshot_has_required_symbols(
            &prices,
            &self.cfg.kr_primary_symbol,
            &self.cfg.us_primary_symbol,
        );
        if !complete {
            // Gated on completeness, not `delay_secs`: a snapshot missing
            // kr_primary/us_primary is unusable to compute_epsilon
            // regardless of how soon after t0 it was taken (WS delivery
            // order across symbols is not guaranteed -- control_symbols
            // can easily arrive before the two primaries right after a
            // (re)connect). Deliberately does NOT persist to RiskState:
            // a future same-day restart must get another fresh-capture
            // attempt, not silently recover this same incomplete map
            // (PR #270 review finding).
            log::warn!(
                "[DAY] t0 snapshot captured {delay_secs:.0}s after t0 but is missing \
                 kr_primary={}/us_primary={} prices -- not persisted, today's epsilon signal \
                 will fail silently unless a later restart captures a complete one",
                self.cfg.kr_primary_symbol,
                self.cfg.us_primary_symbol
            );
            return;
        }
        if delay_secs > 300.0 {
            // Complete, but captured well after t0 -- most likely a
            // restart between t0 and t1 with nothing to recover from
            // RiskState.t0_prices (e.g. the first-ever start of the day
            // after t0, or a prior run that crashed before persisting).
            // `latest_price` this late may already have drifted from the
            // true KRX-open price even though every required key is
            // present (bot-strategy#872 PR #266 follow-up, see
            // RiskState.t0_snapshot_date's doc comment).
            log::warn!(
                "[DAY] t0 snapshot captured {delay_secs:.0}s after t0 with nothing to recover \
                 from persisted state -- may not reflect the true KRX-open price; today's \
                 epsilon signal is suspect"
            );
        } else {
            log::info!("[DAY] t0 snapshot captured ({delay_secs:.0}s after t0)");
        }
        if let Some(today) = self.current_date {
            self.state.t0_snapshot_date = Some(today.to_string());
            self.state.t0_prices = prices;
            atomic_write_json(&self.cfg.state_path, &self.state);
        }
    }

    /// While `RiskState.position_unconfirmed` is set and nothing is
    /// tracked, read the exchange once per tick: a `us_primary` position
    /// showing up is adopted (origin = the unconfirmed send) and clears the
    /// flag; a flat reading is *not* evidence of anything and leaves the
    /// flag (and the halt) for the operator. Runs on every tick including
    /// after a day roll, so the exposure never goes unmanaged.
    async fn try_adopt_unconfirmed(&mut self, now_us: i64) {
        let Ok(positions) = self.connector.get_positions().await else {
            return;
        };
        let Some(live) = exchange_position_for(&positions, &self.cfg.us_primary_symbol) else {
            return;
        };
        let ws_price = self
            .latest_price
            .get(&self.cfg.us_primary_symbol)
            .copied()
            .filter(|p| *p > 0.0);
        // Never adopt with a zero cost basis: without the exchange's
        // avg_entry_price and without a positive WS price yet (e.g. right
        // after a restart), wait for the next tick (pairtrade#275 Codex
        // review).
        let (entry_price, entry_price_estimated) = match (live.entry_price, ws_price) {
            (Some(e), _) => (e, false),
            (None, Some(w)) => (w, true),
            (None, None) => {
                log::warn!(
                    "[ENTRY] exchange shows {} {} size={:.6} after an UNCONFIRMED send but neither an \
                     exchange entry price nor a WS price is available yet -- deferring adoption",
                    live.side,
                    self.cfg.us_primary_symbol,
                    live.size
                );
                return;
            }
        };
        // `position_unconfirmed` deliberately stays set: the adopted
        // OpenPosition is memory-only, so a restart before the exit would
        // lose it again -- the persisted flag is what makes the restarted
        // process re-run this adoption. Only RISK_ACK clears it.
        log::warn!(
            "[ENTRY] exchange now shows {} {} size={:.6} after an UNCONFIRMED send -- adopting it; \
             position_unconfirmed and the session halt stay until RISK_ACK",
            live.side,
            self.cfg.us_primary_symbol,
            live.size
        );
        self.position = Some(OpenPosition {
            side: live.side,
            entry_price,
            entry_price_estimated,
            size: live.size,
            open_size: live.size,
            realized_partial_pnl: 0.0,
            entered_at_us: now_us,
            flatten_asap: true,
        });
        send_notification(
            format!("Han Bridge ENTRY ADOPTED {} {}", self.cfg.us_primary_symbol, live.side),
            format!(
                "unconfirmed send resolved: exchange holds size={:.6} entry_price={entry_price:.4} estimated={entry_price_estimated}",
                live.size
            ),
        );
    }

    /// One poll of the pending fill/exit confirmation, called from `tick`
    /// every 5 s while `self.pending` is set. Deliberately *not* a blocking
    /// wait inside `maybe_enter`/`maybe_exit`: a 15 s `await` there would
    /// stall the single `tokio::select!` loop that also drains the price
    /// feed and polls KILL_SWITCH (same design rule as `send_notification`
    /// and `fetch_order_book_details`), so the confirmation is a small
    /// state machine advanced one `get_positions()` read per tick
    /// (bot-strategy#875 G-2/G-4, pairtrade#275 review finding 4).
    async fn poll_pending_confirm(&mut self, now_us: i64) {
        let Some(pending) = self.pending.clone() else {
            return;
        };
        let reading = self.connector.get_positions().await;
        match pending {
            PendingConfirm::Entry {
                side,
                requested,
                price,
                epsilon,
                notional_usd,
                deadline_us,
                after_send_error,
                saw_reading,
            } => {
                let expired = now_us >= deadline_us;
                match reading {
                    Ok(positions) => {
                        match exchange_position_for(&positions, &self.cfg.us_primary_symbol) {
                            Some(filled) => {
                                self.pending = None;
                                self.record_confirmed_entry(
                                    filled,
                                    side,
                                    requested,
                                    price,
                                    epsilon,
                                    notional_usd,
                                    now_us,
                                    after_send_error.as_deref(),
                                );
                            }
                            None if expired => {
                                self.pending = None;
                                log::warn!(
                                    "[ENTRY] no {} position on the exchange within {}s of the sendTx \
                                     ({}) -- treating as unfilled; no retry today (requirements doc \
                                     §6.3 step 3)",
                                    self.cfg.us_primary_symbol,
                                    self.cfg.fill_confirm_timeout_secs,
                                    after_send_error
                                        .as_deref()
                                        .map(|e| format!("send error: {e}"))
                                        .unwrap_or_else(|| "HTTP 200 accepted".to_string())
                                );
                                send_notification(
                                    format!(
                                        "Han Bridge ENTRY {} {}",
                                        if after_send_error.is_some() { "FAILED" } else { "UNFILLED" },
                                        self.cfg.us_primary_symbol
                                    ),
                                    format!(
                                        "no position within {}s; epsilon={epsilon:.5} side={side} requested={requested:.6} send_error={:?}",
                                        self.cfg.fill_confirm_timeout_secs, after_send_error
                                    ),
                                );
                                self.record_no_position_today();
                            }
                            None => {
                                self.pending = Some(PendingConfirm::Entry {
                                    side,
                                    requested,
                                    price,
                                    epsilon,
                                    notional_usd,
                                    deadline_us,
                                    after_send_error,
                                    saw_reading: true,
                                });
                            }
                        }
                    }
                    Err(e) if expired => {
                        // An earlier flat reading does NOT prove the IOC
                        // stayed unfilled -- the fill update can land after
                        // it, exactly during the WS hiccup that made the last
                        // read fail. Unknown, not unfilled (pairtrade#275
                        // Codex review).
                        self.pending = None;
                        log::error!(
                            "[ENTRY] sendTx {} but the final exchange position read failed ({e:?}; \
                             saw_reading={saw_reading}) -- position UNKNOWN; marking today as acted, NOT \
                             tracking a position. Check the exchange account manually before the exit \
                             window.",
                            if after_send_error.is_some() { "errored" } else { "accepted" }
                        );
                        send_notification(
                            format!("Han Bridge ENTRY UNCONFIRMED {}", self.cfg.us_primary_symbol),
                            format!(
                                "get_positions failed at the end of the {}s window ({e:?}); saw_reading={saw_reading}; send_error={:?}. Manual check required.",
                                self.cfg.fill_confirm_timeout_secs, after_send_error
                            ),
                        );
                        self.state.position_unconfirmed = true;
                        self.halt_session(format!(
                            "entry_unconfirmed: sendTx sent but exchange position unreadable for {}s; \
                             a live {} position may exist untracked -- reconcile against the exchange, \
                             then RISK_ACK",
                            self.cfg.fill_confirm_timeout_secs, self.cfg.us_primary_symbol
                        ));
                        self.record_no_position_today();
                    }
                    Err(_) => {
                        // keep waiting; nothing to update
                    }
                }
            }
            PendingConfirm::Exit {
                exit_price,
                deadline_us,
                saw_reading,
            } => {
                let expired = now_us >= deadline_us;
                match reading {
                    Ok(positions) => {
                        match exchange_position_for(&positions, &self.cfg.us_primary_symbol) {
                            None => {
                                self.pending = None;
                                self.on_exit(exit_price, now_us);
                            }
                            Some(remaining) => {
                                // A side flip while the exit is pending is
                                // the same anomaly as in maybe_exit: halt and
                                // reconcile, whether or not the window ended
                                // (pairtrade#275 Codex review).
                                self.reconcile_side_flip_if_any(
                                    &remaining,
                                    exit_price,
                                    "exit confirm",
                                );
                                if expired {
                                    self.pending = None;
                                    log::error!(
                                        "[EXIT] reduce-only accepted but exchange still holds {} size={:.6} \
                                         after {}s; retrying next tick with the remainder (PnL stays booked \
                                         on the original size)",
                                        remaining.side,
                                        remaining.size,
                                        self.cfg.fill_confirm_timeout_secs
                                    );
                                    // Book whatever this attempt closed at
                                    // this attempt's price; only the open
                                    // remainder carries to the next tick.
                                    self.book_partial_close(
                                        &remaining,
                                        exit_price,
                                        "reduce-only partially filled",
                                    );
                                } else {
                                    self.pending = Some(PendingConfirm::Exit {
                                        exit_price,
                                        deadline_us,
                                        saw_reading: true,
                                    });
                                }
                            }
                        }
                    }
                    Err(e) if expired => {
                        self.pending = None;
                        log::error!(
                            "[EXIT] reduce-only accepted but exchange positions unreadable for {}s \
                             ({e:?}, saw_reading={saw_reading}); assuming NOT exited, retrying next tick",
                            self.cfg.fill_confirm_timeout_secs
                        );
                    }
                    Err(_) => {}
                }
            }
        }
    }

    /// The exchange showed a position after our entry sendTx: record it as
    /// today's position using the exchange's side / size / entry price. A
    /// side that differs from what we submitted is a hard anomaly (the
    /// order flipped, or something else traded this account): the position
    /// is still recorded so the exit path can flatten it, but new entries
    /// are blocked behind RISK_ACK, mirroring pairtrade's SignFlip verdict
    /// (pairtrade#275 review finding 5).
    #[allow(clippy::too_many_arguments)]
    fn record_confirmed_entry(
        &mut self,
        filled: ExchangePosition,
        submitted_side: OrderSide,
        requested: f64,
        ws_price: f64,
        epsilon: f64,
        notional_usd: f64,
        now_us: i64,
        after_send_error: Option<&str>,
    ) {
        let mut note = String::new();
        if let Some(e) = after_send_error {
            note.push_str(" adopted_after_send_error=true");
            log::warn!("[ENTRY] sendTx had errored ({e}) but the exchange shows the position -- adopting it");
        }
        if (filled.size - requested).abs() > 1e-9 {
            log::warn!(
                "[ENTRY] partial fill: requested={requested:.6} exchange size={:.6}",
                filled.size
            );
            note.push_str(&format!(" requested={requested:.6}"));
        }
        let entry_price_estimated = filled.entry_price.is_none();
        let entry_price = filled.entry_price.unwrap_or(ws_price);
        if entry_price_estimated {
            note.push_str(" entry_price_estimated=true");
        }
        if filled.side != submitted_side {
            let reason = format!(
                "entry_side_mismatch: submitted {submitted_side}, exchange holds {} size={:.6}",
                filled.side, filled.size
            );
            log::error!(
                "[ENTRY] {reason} -- recording the exchange's side and halting new entries"
            );
            self.halt_session(reason);
            note.push_str(" side_mismatch=true");
        }
        self.record_entry(
            OpenPosition {
                side: filled.side,
                entry_price,
                entry_price_estimated,
                size: filled.size,
                open_size: filled.size,
                realized_partial_pnl: 0.0,
                entered_at_us: now_us,
                flatten_asap: false,
            },
            epsilon,
            notional_usd,
            &format!("{note} confirmed_by=exchange_position"),
        );
    }

    /// The exchange reports a smaller open quantity than we track: book the
    /// difference as realized at `price` (the WS mid of the attempt that
    /// closed it) and shrink `open_size`. A *larger* quantity is not a
    /// close; it is only recorded (an external add or an over-report the
    /// cap handles at the next send).
    fn book_partial_close(&mut self, live: &ExchangePosition, price: f64, context: &str) {
        let new_open_size = live.size;
        let Some(p) = self.position.as_mut() else {
            return;
        };
        let closed = p.open_size - new_open_size;
        if closed > 1e-12 {
            let sign = match p.side {
                OrderSide::Long => 1.0,
                OrderSide::Short => -1.0,
            };
            let pnl = sign * (price - p.entry_price) * closed;
            p.realized_partial_pnl += pnl;
            log::warn!(
                "[EXIT] partial close ({context}): closed={closed:.6} at {price:.4} pnl=${pnl:.2} \
                 remaining_open={new_open_size:.6} realized_so_far=${:.2}",
                p.realized_partial_pnl
            );
        } else if new_open_size > p.open_size + 1e-12 {
            // Same-side growth outside our own orders (a late partial fill
            // of the entry IOC after its confirmation snapshot, or an
            // external add). Take the exchange's average entry price as
            // the new cost basis for the whole position and grow the
            // entry quantity, so on_exit does not apply the old basis to
            // quantity bought at another price; and halt new entries
            // because the growth cannot be attributed (pairtrade#275 Codex
            // review).
            let grown = new_open_size - p.open_size;
            let old_basis = p.entry_price;
            if let Some(e) = live.entry_price {
                p.entry_price = e;
                p.entry_price_estimated = false;
            } else {
                p.entry_price_estimated = true;
            }
            p.size += grown;
            let reason = format!(
                "position grew outside our orders ({context}): tracked open {:.6} -> exchange {new_open_size:.6} \
                 (+{grown:.6}); cost basis {old_basis:.4} -> {:.4} (estimated={})",
                p.open_size, p.entry_price, p.entry_price_estimated
            );
            log::error!("[EXIT] {reason}");
            p.open_size = new_open_size;
            self.halt_session(reason);
            return;
        }
        p.open_size = new_open_size;
    }

    /// If the exchange holds the opposite side of what we track, record the
    /// exchange's side / open size / entry price (WS mid when it gives
    /// none) and engage the session halt. Used by `maybe_exit` and by the
    /// pending-exit confirmation so a flip can never be silently absorbed.
    fn reconcile_side_flip_if_any(
        &mut self,
        live: &ExchangePosition,
        ws_price: f64,
        context: &str,
    ) {
        let Some(pos) = self.position.as_ref() else {
            return;
        };
        if live.side == pos.side {
            return;
        }
        let reason = format!(
            "{context}: side flip -- tracked {} size={:.6}, exchange holds {} size={:.6}",
            pos.side, pos.open_size, live.side, live.size
        );
        log::error!("[EXIT] {reason}");
        if let Some(p) = self.position.as_mut() {
            // The tracked leg was necessarily closed for the side to
            // flip: realize it at the current mid before installing the
            // exchange's replacement leg, so its loss/gain reaches
            // on_exit's drawdown accounting (pairtrade#275 Codex review).
            let old_sign = match p.side {
                OrderSide::Long => 1.0,
                OrderSide::Short => -1.0,
            };
            let old_leg_pnl = old_sign * (ws_price - p.entry_price) * p.open_size;
            p.realized_partial_pnl += old_leg_pnl;
            log::error!(
                "[EXIT] booked the flipped-away {} leg: size={:.6} entry={:.4} at mid {ws_price:.4} \
                 pnl=${old_leg_pnl:.2}; realized_so_far=${:.2}",
                p.side,
                p.open_size,
                p.entry_price,
                p.realized_partial_pnl
            );
            p.side = live.side;
            p.size = live.size;
            p.open_size = live.size;
            p.entry_price_estimated = live.entry_price.is_none();
            p.entry_price = live.entry_price.unwrap_or(ws_price);
        }
        self.halt_session(reason);
    }

    /// Sticky session halt (same on-disk RISK_ACK contract as the drawdown
    /// halt in `on_exit`): blocks new entries until the operator creates
    /// `risk_ack_path`. Exits keep running.
    fn halt_session(&mut self, reason: String) {
        if self.state.session_halted {
            return;
        }
        self.state.session_halted = true;
        self.state.session_halt_reason = Some(reason.clone());
        atomic_write_json(&self.cfg.state_path, &self.state);
        send_notification(
            format!("Han Bridge SESSION HALT {}", self.cfg.instance_id),
            format!(
                "{reason}. New entries blocked until RISK_ACK at {}",
                self.cfg.risk_ack_path.display()
            ),
        );
    }

    /// Shared tail of a successful (or adopted) entry: record the
    /// position, mark the day as acted on, persist, log, notify.
    fn record_entry(&mut self, pos: OpenPosition, epsilon: f64, notional_usd: f64, note: &str) {
        let side = pos.side;
        let price = pos.entry_price;
        let size = pos.size;
        self.position = Some(pos);
        self.day.entered = true;
        // Persist immediately, matching the no-entry and exit paths:
        // without this, a restart between entry and exit finds
        // state.last_session_date still pointing at a prior day, so
        // roll_day_if_needed does not set day.entered and maybe_enter
        // would re-evaluate and potentially re-enter the same day,
        // doubling notional exposure. This does not by itself recover the
        // in-memory OpenPosition after a restart (see
        // docs/engine-b-live-operations.md's Stop and recovery section)
        // -- it only prevents a second entry.
        self.state.last_session_date = self.current_date.map(|d| d.to_string());
        atomic_write_json(&self.cfg.state_path, &self.state);
        log::info!(
            "[ENTRY] side={side} epsilon={epsilon:.5} price={price:.4} notional=${notional_usd:.0} size={size:.6}{note}"
        );
        send_notification(
            format!("Han Bridge ENTRY {} {}", self.cfg.us_primary_symbol, side),
            format!(
                "epsilon={epsilon:.5} threshold={:.5} price={price:.4} notional=${notional_usd:.0} size={size:.6} dry_run={}{note}",
                self.cfg.epsilon_threshold, self.cfg.dry_run
            ),
        );
    }

    /// Mark today as acted on without a position (signal fired, order
    /// path ended flat): no retry today, same persistence as the
    /// no-signal path.
    fn record_no_position_today(&mut self) {
        self.position = None;
        self.day.entered = true;
        self.state.last_session_date = self.current_date.map(|d| d.to_string());
        atomic_write_json(&self.cfg.state_path, &self.state);
    }

    async fn maybe_enter(&mut self, now_us: i64) {
        let Some((_t0, t1, _t2)) = self.window else {
            return;
        };
        if self.day.entered || now_us < t1 {
            return;
        }
        if let Some(pos) = &self.position {
            // A position carried over from a previous session (its exit
            // kept failing past exit_deadline, so the day rolled with it
            // still open). Never open a second one on top of it and never
            // re-label it as today's entry; the exit path keeps trying at
            // today's t2 (pairtrade#275 review finding 1).
            log::warn!(
                "[ENTRY] position from a previous session still open ({} size={:.6}, entered {}s \
                 ago); no new entry today, exit path continues",
                pos.side,
                pos.size,
                (now_us - pos.entered_at_us) / 1_000_000
            );
            self.day.entered = true;
            self.state.last_session_date = self.current_date.map(|d| d.to_string());
            atomic_write_json(&self.cfg.state_path, &self.state);
            return;
        }
        if now_us > t1 + self.cfg.entry_deadline_secs * 1_000_000 {
            if !self.day.entered {
                log::warn!("[ENTRY] entry_deadline passed without a valid signal; skipping today");
                self.day.entered = true; // don't keep re-evaluating
                self.state.last_session_date = self.current_date.map(|d| d.to_string());
                atomic_write_json(&self.cfg.state_path, &self.state);
            }
            return;
        }
        if self.day.t1_prices.is_none() {
            self.day.t1_prices = Some(self.snapshot_prices());
        }
        let (Some(t0_prices), Some(t1_prices)) = (&self.day.t0_prices, &self.day.t1_prices) else {
            log::warn!("[ENTRY] t1 reached but t0 snapshot missing (process started after t0?); skipping today");
            return;
        };
        let Some(epsilon) = compute_epsilon(
            &self.cfg.signal_model,
            &self.cfg.kr_primary_symbol,
            &self.cfg.us_primary_symbol,
            t0_prices,
            t1_prices,
        ) else {
            return;
        };
        if epsilon.abs() < self.cfg.epsilon_threshold {
            log::info!(
                "[SIGNAL] |epsilon|={:.5} < threshold={:.5}; no entry today",
                epsilon.abs(),
                self.cfg.epsilon_threshold
            );
            self.day.entered = true;
            self.state.last_session_date = self.current_date.map(|d| d.to_string());
            atomic_write_json(&self.cfg.state_path, &self.state);
            return;
        }
        if !self.day.eligibility_confirmed {
            match fetch_order_book_details(&self.http_client, &self.cfg.lighter_rest_url).await {
                Ok(details) => {
                    // Only a successful (parsed) response counts as
                    // "confirmed" -- see the field's doc comment on why an
                    // Err below must NOT set this, so the next tick (5s
                    // later, still inside entry_deadline_secs) retries
                    // instead of giving up on today's only entry window
                    // over one transient failure.
                    self.day.eligibility_confirmed = true;
                    // Both symbols gate entry, not just us_primary (the
                    // only one actually traded -- see this file's module
                    // doc, "The *traded* instrument is the US primary
                    // symbol only"). Deliberate: kr_primary only feeds
                    // `compute_epsilon`'s signal, never an order, but a
                    // KR market Lighter itself has restricted
                    // (force_reduce_only) or that has gone extremely thin
                    // (below min_daily_volume_usd) casts doubt on that
                    // day's KR price observations feeding the signal, not
                    // just on KR's own tradeability -- distrust the
                    // signal input, not only the order leg (bot-
                    // strategy#872 PR #266 self-review, design question).
                    for (label, symbol) in [
                        ("kr_primary", &self.cfg.kr_primary_symbol),
                        ("us_primary", &self.cfg.us_primary_symbol),
                    ] {
                        match details.iter().find(|d| &d.symbol == symbol) {
                            Some(detail) => {
                                let (eligible, reason) =
                                    evaluate_eligibility(detail, self.cfg.min_daily_volume_usd);
                                if !eligible {
                                    log::warn!(
                                        "[ELIGIBILITY] {label}={symbol} ineligible ({reason})"
                                    );
                                    self.day
                                        .ineligible_reasons
                                        .push(format!("{label}={symbol}:{reason}"));
                                }
                            }
                            None => {
                                log::warn!(
                                    "[ELIGIBILITY] {label}={symbol} not found in orderBookDetails response"
                                );
                                self.day
                                    .ineligible_reasons
                                    .push(format!("{label}={symbol}:not_found"));
                            }
                        }
                    }
                }
                Err(e) => {
                    log::warn!(
                        "[ELIGIBILITY] orderBookDetails fetch failed: {e:?}; will retry next tick \
                         if still within the entry window, otherwise proceeding without the \
                         eligibility gate today (fail-open)"
                    );
                }
            }
        }
        if !self.day.ineligible_reasons.is_empty() {
            log::warn!(
                "[ENTRY] skipped: {}",
                self.day.ineligible_reasons.join("; ")
            );
            self.day.entered = true;
            self.state.last_session_date = self.current_date.map(|d| d.to_string());
            atomic_write_json(&self.cfg.state_path, &self.state);
            return;
        }
        if !self.entries_allowed() {
            log::warn!("[ENTRY] signal fired but entries blocked (kill_switch or session halt)");
            return;
        }
        let predicted_direction = epsilon.signum() * self.cfg.direction_multiplier.signum();
        let side = if predicted_direction >= 0.0 {
            OrderSide::Long
        } else {
            OrderSide::Short
        };
        let Some(price) = self.latest_price.get(&self.cfg.us_primary_symbol).copied() else {
            log::error!(
                "[ENTRY] no current price for {}; cannot size order",
                self.cfg.us_primary_symbol
            );
            return;
        };
        if price <= 0.0 {
            return;
        }
        let notional_usd = self.cfg.lot_usd.min(self.cfg.max_notional_usd());
        if notional_usd < self.cfg.lot_usd {
            log::warn!(
                "[RISK_NOTIONAL_CAP] clamped lot ${:.0} -> ${:.0} (equity=${:.0} leverage={})",
                self.cfg.lot_usd,
                notional_usd,
                self.cfg.equity_usd_reference,
                self.cfg.leverage
            );
        }
        let size = notional_usd / price;

        if !self.cfg.dry_run {
            // Exchange truth before we send anything (bot-strategy#875
            // G-4): a prior process (crash / restart between sendTx and
            // persisting `last_session_date`) may have left a position we
            // no longer track. If the account already holds `us_primary`,
            // adopt that position instead of submitting a second order.
            // Fail closed on an unreadable account -- the next 5 s tick
            // retries while still inside `entry_deadline_secs`.
            match self.connector.get_positions().await {
                Ok(positions) => {
                    if let Some(existing) =
                        exchange_position_for(&positions, &self.cfg.us_primary_symbol)
                    {
                        log::warn!(
                            "[ENTRY] exchange already holds {} {} size={:.6} before submit -- adopting it \
                             instead of sending a second order. Origin unknown to this process (a prior \
                             run's entry?): held= will be measured from now and today's epsilon is NOT \
                             the signal that opened it",
                            existing.side,
                            self.cfg.us_primary_symbol,
                            existing.size
                        );
                        let entry_price_estimated = existing.entry_price.is_none();
                        let entry_price = existing.entry_price.unwrap_or(price);
                        self.record_entry(
                            OpenPosition {
                                side: existing.side,
                                entry_price,
                                entry_price_estimated,
                                size: existing.size,
                                open_size: existing.size,
                                realized_partial_pnl: 0.0,
                                entered_at_us: now_us,
                                flatten_asap: true,
                            },
                            epsilon,
                            notional_usd,
                            &format!(
                                " adopted_from_exchange=true origin=unknown entry_price_estimated={entry_price_estimated}"
                            ),
                        );
                        return;
                    }
                }
                Err(e) => {
                    log::warn!(
                        "[ENTRY] cannot read exchange positions before submit ({e:?}); not sending \
                         this tick, will retry within the entry window"
                    );
                    return;
                }
            }
            // NOTE: a no-op on the Lighter connector (logs at debug and
            // returns Ok) -- `leverage` only feeds max_notional_usd().
            if let Err(e) = self
                .connector
                .set_leverage(&self.cfg.us_primary_symbol, self.cfg.leverage)
                .await
            {
                log::error!("[ENTRY] set_leverage failed: {e:?}");
                return;
            }
        }
        // At most ONE entry sendTx per session day (bot-strategy#875 G-4).
        // Whatever the outcome below -- accepted, timed out, 5xx, rate
        // limited, rejected -- `self.pending` is set and `day.entered`
        // becomes true when it resolves, so this block cannot run twice
        // today. A local error does not prove Lighter rejected the order,
        // and a single position read right after it can be a false
        // negative because REST and WS limits are coupled (the same stress
        // delays the account_all fill update), so the same tick-driven
        // confirmation watches the exchange either way.
        // Durable "today's entry was attempted" marker BEFORE the send
        // (pairtrade#275 Codex review): if the process dies between
        // sendTx reaching Lighter and the in-memory `pending` resolving,
        // a restart must not evaluate today again -- roll_day_if_needed
        // reads this and sets day.entered. The position itself is still
        // not persisted (KNOWN GAPS): after such a restart the operator
        // checks the exchange, which is the documented recovery.
        self.state.last_session_date = self.current_date.map(|d| d.to_string());
        if let Err(e) = atomic_write_json_checked(&self.cfg.state_path, &self.state) {
            // No durable marker, no order: sending now would make the
            // at-most-one guarantee depend on this process surviving.
            // Nothing was sent, so the next tick may simply try again.
            log::error!(
                "[ENTRY] cannot persist the entry-attempt marker to {} ({e}); NOT sending this tick",
                self.cfg.state_path.display()
            );
            return;
        }
        let submit = self.submit_order(side, size, false).await;
        // Fresh clock: `now_us` is the tick's start time and the
        // eligibility fetch / position read / sendTx above can take
        // seconds, so a deadline based on it could already be expired
        // when `pending` is installed (pairtrade#275 Codex review).
        let sent_at_us = crate::now_us(); // the `now_us` parameter shadows the fn
        if self.cfg.dry_run {
            match submit {
                Ok(requested) => self.record_entry(
                    OpenPosition {
                        side,
                        entry_price: price,
                        entry_price_estimated: false,
                        size: requested.to_f64().unwrap_or(size),
                        open_size: requested.to_f64().unwrap_or(size),
                        realized_partial_pnl: 0.0,
                        entered_at_us: sent_at_us,
                        flatten_asap: false,
                    },
                    epsilon,
                    notional_usd,
                    "",
                ),
                Err(e) => log::error!("[ENTRY] order failed: {e:?}"),
            }
            return;
        }
        let (requested, after_send_error) = match submit {
            Ok(requested) => (requested.to_f64().unwrap_or(size), None),
            Err(e) => {
                log::error!(
                    "[ENTRY] order failed ({e:?}); no re-submit today -- watching the exchange \
                     position for {}s in case Lighter accepted it anyway",
                    self.cfg.fill_confirm_timeout_secs
                );
                (size, Some(format!("{e:?}")))
            }
        };
        // HTTP 200 from sendTx means "accepted by the API servers", not
        // "executed" (Lighter docs; bot-strategy#875 G-2). The fill is
        // confirmed against the exchange's own position by
        // `poll_pending_confirm` on the following ticks; `day.entered`
        // stays false until then so a restart in between re-checks the
        // exchange (pre-submit block above) rather than re-sending.
        self.pending = Some(PendingConfirm::Entry {
            side,
            requested,
            price,
            epsilon,
            notional_usd,
            deadline_us: sent_at_us + self.cfg.fill_confirm_timeout_secs.max(1) * 1_000_000,
            after_send_error,
            saw_reading: false,
        });
    }

    async fn maybe_exit(&mut self, now_us: i64) {
        let Some(pos) = self.position.clone() else {
            return;
        };
        // A position that is not today's own entry (adopted / recovered /
        // carried over) has no valid window to wait for: flatten now, with
        // emergency semantics, even on a closed day.
        let emergency = if pos.flatten_asap {
            log::warn!(
                "[EXIT] flattening a position whose original exit window is unknown or past \
                 ({} size={:.6}); not waiting for today's t2",
                pos.side,
                pos.open_size
            );
            true
        } else {
            let Some((_t0, _t1, t2)) = self.window else {
                return;
            };
            if self.day.exited || now_us < t2 {
                return;
            }
            let emergency = now_us > t2 + self.cfg.exit_deadline_secs * 1_000_000;
            if emergency {
                log::warn!("[EXIT] exit_deadline passed; forcing emergency close");
            }
            emergency
        };
        let Some(price) = self.latest_price.get(&self.cfg.us_primary_symbol).copied() else {
            return;
        };
        if self.cfg.dry_run {
            match self.submit_order(opposite(pos.side), pos.size, true).await {
                Ok(_) => self.on_exit(price, now_us),
                Err(e) => log::error!("[EXIT] order failed, position still open: {e:?}"),
            }
            return;
        }
        // Size the reduce-only to the exchange's *current* position, not
        // to our memory of the entry (requirements doc §5.4 "再指値前に最新
        // 建玉を再照会", bot-strategy#875 G-2). reduce_only=true remains
        // the exchange-side guard against ever flipping.
        let (exit_side, exit_size) = match self.connector.get_positions().await {
            Ok(positions) => match exchange_position_for(&positions, &self.cfg.us_primary_symbol) {
                Some(live) => {
                    if live.side != pos.side {
                        // Only possible if reduce-only failed us or the
                        // account was traded from outside: fix side AND
                        // entry price so the PnL we book is at least the
                        // exchange's, and halt new entries (pairtrade#275
                        // review findings 2 and 5).
                        self.reconcile_side_flip_if_any(&live, price, "exit_side_mismatch");
                        (opposite(live.side), live.size)
                    } else {
                        let tracked_open = pos.open_size;
                        if (live.size - tracked_open).abs() > 1e-9 {
                            // Reduced or grown outside our own attempts:
                            // reconcile the accounting first (a reduction is
                            // booked at the current mid; growth re-bases the
                            // cost on the exchange's average entry price and
                            // halts), independent of how much we send below.
                            self.book_partial_close(
                                &live,
                                price,
                                "exchange open size differs from tracked before exit",
                            );
                        }
                        // The order itself is still capped against a
                        // transient over-report; a real add above the cap
                        // has been reconciled above and its remainder is
                        // re-read and re-sent on the next tick.
                        let (size, capped) = cap_exit_size(live.size, tracked_open);
                        if capped {
                            log::error!(
                                "[EXIT] exchange reports size={:.6}, more than {}x the previously tracked \
                                 open {:.6}; sending a reduce-only for {:.6} this tick (remainder next tick)",
                                live.size,
                                EXIT_SIZE_CAP_RATIO,
                                tracked_open,
                                size
                            );
                        }
                        (opposite(live.side), size)
                    }
                }
                None => {
                    log::warn!(
                        "[EXIT] exchange is already flat in {} -- recording the exit at mid without \
                         sending an order (closed externally, or the entry never filled)",
                        self.cfg.us_primary_symbol
                    );
                    self.on_exit(price, now_us);
                    return;
                }
            },
            Err(e) if emergency => {
                log::error!(
                    "[EXIT] exchange positions unreadable ({e:?}) past exit_deadline -- sending a \
                     reduce-only for the tracked size {:.6} anyway (reduce_only caps it at the real \
                     position)",
                    pos.size
                );
                (opposite(pos.side), pos.size)
            }
            Err(e) => {
                log::warn!(
                    "[EXIT] exchange positions unreadable ({e:?}); not sending a blind reduce-only this \
                     tick (emergency path takes over after exit_deadline)"
                );
                return;
            }
        };
        if let Err(e) = self.submit_order(exit_side, exit_size, true).await {
            log::error!("[EXIT] order failed, position still open: {e:?}");
            return;
        }
        // Same rule as entry: HTTP 200 is acceptance, not execution. Only
        // a flat exchange position ends the day (confirmed tick by tick in
        // `poll_pending_confirm`); anything else is retried on a later
        // tick with the then-current remainder.
        self.pending = Some(PendingConfirm::Exit {
            exit_price: price,
            // Fresh clock after the send, same reason as the entry path.
            deadline_us: crate::now_us() + self.cfg.fill_confirm_timeout_secs.max(1) * 1_000_000,
            saw_reading: false,
        });
    }

    fn on_exit(&mut self, exit_price: f64, now_us: i64) {
        let Some(pos) = self.position.take() else {
            return;
        };
        let sign = match pos.side {
            OrderSide::Long => 1.0,
            OrderSide::Short => -1.0,
        };
        // Final remainder at the final price, plus what earlier partial
        // reductions already realized at their own prices.
        let pnl = pos.realized_partial_pnl + sign * (exit_price - pos.entry_price) * pos.open_size;
        log::info!(
            "[EXIT] side={} entry={:.4} exit={:.4} size={:.6} final_open={:.6} pnl=${:.2} \
             (partials=${:.2}) held={}s entry_price_estimated={} (exit price is the WS mid, not the fill)",
            pos.side,
            pos.entry_price,
            exit_price,
            pos.size,
            pos.open_size,
            pnl,
            pos.realized_partial_pnl,
            (now_us - pos.entered_at_us) / 1_000_000,
            pos.entry_price_estimated
        );

        self.state.realized_pnl_session += pnl;
        self.state.pnl_today += pnl;
        self.state.total_trades += 1;
        if pnl > 0.0 {
            self.state.total_wins += 1;
        }
        let current_equity = self.state.session_start_equity + self.state.realized_pnl_session;
        if current_equity > self.state.peak_equity {
            self.state.peak_equity = current_equity;
        }
        let dd_usd = self.state.peak_equity - current_equity;
        if dd_usd > self.state.max_dd_usd {
            self.state.max_dd_usd = dd_usd;
        }
        let dd_bps = if self.state.peak_equity > 0.0 {
            dd_usd / self.state.peak_equity * 10_000.0
        } else {
            0.0
        };
        if dd_bps > self.state.max_dd_bps {
            self.state.max_dd_bps = dd_bps;
        }
        if dd_bps >= self.cfg.max_session_loss_bps && !self.state.session_halted {
            self.state.session_halted = true;
            self.state.session_halt_reason = Some(format!("session_dd_{dd_bps:.0}bps"));
            log::warn!(
                "[SESSION_DD] halt engaged: dd={:.0}bps >= {:.0}bps threshold -- clear via RISK_ACK at {}",
                dd_bps,
                self.cfg.max_session_loss_bps,
                self.cfg.risk_ack_path.display()
            );
        }
        self.day.exited = true;
        self.state.last_session_date = self.current_date.map(|d| d.to_string());
        atomic_write_json(&self.cfg.state_path, &self.state);

        append_pnl_log(
            &self.cfg.pnl_log_path,
            &serde_json::json!({
                "ts_us": now_us,
                "instance_id": self.cfg.instance_id,
                "symbol": self.cfg.us_primary_symbol,
                "side": pos.side.to_string(),
                "entry_price": pos.entry_price,
                "exit_price": exit_price,
                "size": pos.size,
                "pnl_usd": pnl,
                "held_secs": (now_us - pos.entered_at_us) / 1_000_000,
                "dry_run": self.cfg.dry_run,
            }),
        );
        send_notification(
            format!("Han Bridge EXIT {} pnl=${pnl:.2}", self.cfg.us_primary_symbol),
            format!(
                "entry={:.4} exit={exit_price:.4} size={:.6} dry_run={}",
                pos.entry_price, pos.size, self.cfg.dry_run
            ),
        );
    }

    async fn tick(&mut self) {
        let now = now_us();
        self.roll_day_if_needed(now);
        self.maybe_clear_halt();
        self.maybe_capture_t0(now);
        if self.state.position_unconfirmed && self.position.is_none() && self.pending.is_none() {
            self.try_adopt_unconfirmed(now).await;
        }
        if self.pending.is_some() {
            // One exchange read per tick until the in-flight entry/exit is
            // confirmed or its window ends; no new decisions meanwhile.
            self.poll_pending_confirm(now).await;
        } else {
            self.maybe_enter(now).await;
            self.maybe_exit(now).await;
        }
        self.write_status_if_due(now);
    }

    fn write_status_if_due(&mut self, now_us: i64) {
        if now_us - self.last_status_write_us < 30_000_000 {
            return;
        }
        self.last_status_write_us = now_us;
        // 0-100 scale, not 0.0-1.0 -- debot-dashboard's web/app.js renders
        // this as `${win_rate.toFixed(0)}%` with no *100 on the frontend,
        // matching pairtrade::status.rs::set_trade_stats_totals.
        let win_rate = if self.state.total_trades > 0 {
            self.state.total_wins as f64 / self.state.total_trades as f64 * 100.0
        } else {
            0.0
        };
        let positions: Vec<DashboardPosition> = match &self.position {
            Some(pos) => vec![DashboardPosition {
                symbol: self.cfg.us_primary_symbol.clone(),
                side: match pos.side {
                    dex_connector::OrderSide::Long => "long",
                    dex_connector::OrderSide::Short => "short",
                },
                // Live exposure (shrinks across partial exits); `size`
                // is the historical entry quantity used for PnL.
                size: pos.open_size.to_string(),
                entry_price: pos.entry_price.to_string(),
            }],
            None => vec![],
        };
        // Derived from `positions` itself rather than re-reading
        // `self.position` independently for each field -- one source of
        // truth for "do we think we're holding" (bot-strategy#866 PR #255
        // review round 2, nit 5).
        let has_position = !positions.is_empty();
        let position_count = positions.len() as i32;
        // Same one-source-of-truth treatment as `positions` above: a
        // fresh Path::exists() check, read once instead of independently
        // for kill_switch_active and kill_switch, so the two can't
        // disagree within a single status.json snapshot if the sentinel
        // file is created/removed between the two reads (review round 3
        // on this same PR, self-review finding).
        let kill_switch = self.kill_switch_engaged();
        // Built as a standalone binding, not inline in the `FullStatus`
        // literal below, so `han_bridge` can derive its four overlapping
        // fields (day_entered/day_exited/ineligible_reasons/
        // session_halt_reason) FROM this value instead of re-reading
        // self.day/self.state a second time -- one source of truth per
        // tick instead of two independent reads that could silently
        // drift apart on a future edit to only one of the two struct
        // literals (code-review finding on PR #271).
        let extra = EngineStatusExtra {
            ts_us: now_us,
            instance_id: self.cfg.instance_id.clone(),
            current_date: self.current_date.map(|d| d.to_string()),
            window: self.window,
            day_entered: self.day.entered,
            day_exited: self.day.exited,
            restart_recovered: self.day.restart_recovered,
            eligibility_ineligible_reasons: self.day.ineligible_reasons.clone(),
            session_halted: self.state.session_halted,
            session_halt_reason: self.state.session_halt_reason.clone(),
            realized_pnl_session: self.state.realized_pnl_session,
            pnl_today_date: self.state.pnl_today_date.clone(),
            total_trades: self.state.total_trades,
            total_wins: self.state.total_wins,
            max_dd_bps: self.state.max_dd_bps,
            max_dd_usd: self.state.max_dd_usd,
            kill_switch,
            calendar_version: self.calendar.calendar_version.clone(),
        };
        let han_bridge = HanBridgeStatus {
            kr_primary_symbol: self.cfg.kr_primary_symbol.clone(),
            us_primary_symbol: self.cfg.us_primary_symbol.clone(),
            day_entered: extra.day_entered,
            day_exited: extra.day_exited,
            position_unconfirmed: self.state.position_unconfirmed,
            ineligible_reasons: extra.eligibility_ineligible_reasons.clone(),
            session_halt_reason: extra.session_halt_reason.clone(),
        };
        let status = FullStatus {
            dashboard: DashboardStatus {
                ts: now_us / 1_000_000,
                updated_at: Utc::now().to_rfc3339(),
                id: self.cfg.instance_id.clone(),
                dex: "lighter",
                dry_run: self.cfg.dry_run,
                has_position,
                position_count,
                // false exactly when today's `entered` flag was restored
                // from persisted state after a restart -- our own
                // in-memory `position` was lost in that case (never
                // persisted, see KNOWN GAPS), so debot-dashboard's
                // "positions_ready !== false means trustworthy" read must
                // not be told otherwise.
                positions_ready: !(self.day.restart_recovered || self.state.position_unconfirmed),
                positions,
                pnl_total: self.state.realized_pnl_session,
                pnl_today: self.state.pnl_today,
                pnl_source: "engine_b_live_risk_state",
                kill_switch_active: kill_switch,
                trade_stats: DashboardTradeStats {
                    trades: self.state.total_trades,
                    wins: self.state.total_wins,
                    win_rate,
                    max_dd: self.state.max_dd_usd,
                    pnl: self.state.realized_pnl_session,
                },
            },
            extra,
            han_bridge,
        };
        // Serialized once and reused for both destinations (previously
        // two separate serde_json calls per tick -- bot-strategy#866 PR
        // #255 review round 2, nit 4). Compact, not to_vec_pretty: the
        // shared bytes go to the S3 mirror on every 30s tick for the life
        // of the process, and nobody reads that copy by eye (the Go
        // consumer's json.Unmarshal is whitespace-agnostic) -- pretty-
        // printing it would have been a silent ~25%+ size regression on
        // every PutObject with no benefit (self-review round on this PR
        // caught this; matches src/pairtrade/status.rs's own convention
        // of reusing compact serde_json::to_string for both destinations).
        // The local status.json file trades away pretty-printing for
        // this; inspect it with `python3 -m json.tool` or `jq` if needed.
        let Ok(body) = serde_json::to_vec(&status) else {
            log::warn!("[STATUS] serialize failed for {}", self.cfg.status_path.display());
            return;
        };
        atomic_write_bytes(&self.cfg.status_path, &body);
        if let Some(mirror) = &self.status_s3_mirror {
            mirror.put_async("status.json", body);
        }
    }
}

/// Fire-and-forget notification via `debot::email_client::EmailClient`
/// (`src/email_client.rs`, `pub mod` in `src/lib.rs`). `EmailClient::new()`
/// reads `GMAIL_USER`/`GMAIL_TO` (or legacy `TO_ADDRESS`)/`GMAIL_APP_PASSWORD`
/// from env itself and degrades to a warning-logged no-op `send()` if any
/// are missing. `EmailClient::send()` is a synchronous, blocking SMTP call
/// (`lettre::SmtpTransport::send`) -- run it on the blocking-task pool via
/// `spawn_blocking` rather than inline, so a slow/unreachable SMTP server
/// cannot stall the single `tokio::select!` loop that also drains price
/// updates and evaluates the entry/exit deadlines. Genuinely
/// fire-and-forget: the spawned task's JoinHandle is intentionally
/// dropped, matching EmailClient::send()'s own no-return-value contract.
fn send_notification(subject: impl Into<String>, body: impl Into<String>) {
    let subject = subject.into();
    let body = body.into();
    tokio::task::spawn_blocking(move || {
        debot::email_client::EmailClient::new().send(&subject, &body);
    });
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let cfg = EngineBLiveConfig::from_env();
    log::info!(
        "[CONFIG] instance={} dry_run={} kr_primary={} us_primary={} lot_usd=${:.0} leverage={} \
         epsilon_threshold={:.5} direction_multiplier={} signal_model={} entry_deadline={}s exit_deadline={}s \
         min_daily_volume_usd=${:.0}",
        cfg.instance_id,
        cfg.dry_run,
        cfg.kr_primary_symbol,
        cfg.us_primary_symbol,
        cfg.lot_usd,
        cfg.leverage,
        cfg.epsilon_threshold,
        cfg.direction_multiplier,
        cfg.signal_model,
        cfg.entry_deadline_secs,
        cfg.exit_deadline_secs,
        cfg.min_daily_volume_usd,
    );

    // Mirrors robinhood_dipgrid.rs's explicit live-refusal gate: flipping
    // ENGINE_B_LIVE_DRY_RUN=false alone is not enough. This prototype has
    // not been reviewed for live trading beyond what this session's PR
    // review covers -- remove this bail only as a deliberate, reviewed
    // code change once the operator has confirmed DRY_RUN behavior on the
    // real host and is ready to go live (bot-strategy#866).
    if !cfg.dry_run && std::env::var("ENGINE_B_LIVE_CONFIRM_LIVE").as_deref() != Ok("yes-i-mean-it")
    {
        anyhow::bail!(
            "ENGINE_B_LIVE_DRY_RUN=false requires ENGINE_B_LIVE_CONFIRM_LIVE=yes-i-mean-it as well \
             (deliberate double confirmation before real orders go out, bot-strategy#866)"
        );
    }

    // direction_multiplier only ever means "same as epsilon's sign" (1.0)
    // or "opposite" (-1.0) -- f64::signum() returns 1.0 for 0.0 (never
    // 0.0), so a mistyped ENGINE_B_LIVE_DIRECTION_MULTIPLIER=0 would
    // silently trade as if it were 1.0 instead of the operator's evident
    // intent to disable directional bias. Reject anything else outright
    // rather than guess.
    if cfg.direction_multiplier != 1.0 && cfg.direction_multiplier != -1.0 {
        anyhow::bail!(
            "ENGINE_B_LIVE_DIRECTION_MULTIPLIER must be exactly 1.0 or -1.0, got {} \
             (0.0 would silently behave as 1.0 via f64::signum(), not \"disabled\")",
            cfg.direction_multiplier
        );
    }

    let calendar = TradingCalendar::load(&cfg.trading_calendar_path)
        .context("failed to load trading calendar")?;
    log::info!(
        "[CALENDAR] loaded calendar_version={}",
        calendar.calendar_version
    );

    let symbols = cfg.all_symbols();
    let connector = DexConnectorBox::create(
        "lighter",
        cfg.dry_run,
        &symbols,
        Some(cfg.instance_id.as_str()),
    )
    .await
    .context("failed to initialize connector")?;
    connector
        .start()
        .await
        .context("failed to start connector")?;
    let connector: std::sync::Arc<dyn DexConnector + Send + Sync> = std::sync::Arc::new(connector);

    let mut price_rx = connector
        .subscribe_price_updates()
        .context("subscribe_price_updates failed")?;

    let mut state = load_state(&cfg.state_path);
    if state.session_start_equity <= 0.0 {
        state.session_start_equity = cfg.equity_usd_reference;
        state.peak_equity = cfg.equity_usd_reference;
    }
    if state.session_halted {
        log::warn!(
            "[STARTUP] resuming with session_halted=true (reason: {:?}) -- new entries blocked until RISK_ACK at {}",
            state.session_halt_reason,
            cfg.risk_ack_path.display()
        );
    }

    let mut engine = EngineBLiveEngine {
        cfg,
        connector,
        calendar,
        http_client: Client::new(),
        latest_price: HashMap::new(),
        current_date: None,
        window: None,
        day: DaySnapshot::default(),
        position: None,
        pending: None,
        state,
        last_status_write_us: 0,
        status_s3_mirror: S3Mirror::from_env(),
    };

    let mut tick_interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        tokio::select! {
            update = price_rx.recv() => {
                match update {
                    Ok(PriceUpdate { symbol, mid_price, .. }) => {
                        if let Some(price) = mid_price.to_f64() {
                            if price > 0.0 {
                                engine.latest_price.insert(symbol, price);
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("[WS] price feed lagged, dropped {n} updates");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        log::error!("[WS] price feed closed, exiting");
                        break;
                    }
                }
            }
            _ = tick_interval.tick() => {
                engine.tick().await;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_config() -> EngineBLiveConfig {
        EngineBLiveConfig {
            instance_id: "test".to_string(),
            dry_run: true,
            kr_primary_symbol: "SKHY".to_string(),
            us_primary_symbol: "SNDK".to_string(),
            control_symbols: vec!["SOXL".to_string(), "NVDA".to_string()],
            lot_usd: 100.0,
            leverage: 2,
            epsilon_threshold: 0.003,
            direction_multiplier: 1.0,
            signal_model: "diff".to_string(),
            entry_deadline_secs: 180,
            exit_deadline_secs: 900,
            fill_confirm_timeout_secs: 15,
            lighter_rest_url: "https://mainnet.zklighter.elliot.ai".to_string(),
            min_daily_volume_usd: 100_000.0,
            equity_usd_reference: 1000.0,
            max_session_loss_bps: 500.0,
            trading_calendar_path: PathBuf::from("/nonexistent"),
            kill_switch_path: PathBuf::from("/nonexistent/KILL_SWITCH"),
            risk_ack_path: PathBuf::from("/nonexistent/RISK_ACK"),
            state_path: PathBuf::from("/nonexistent/state.json"),
            status_path: PathBuf::from("/nonexistent/status.json"),
            pnl_log_path: PathBuf::from("/nonexistent/pnl.jsonl"),
        }
    }

    // -------------------------------------------------------------
    // compute_epsilon
    // -------------------------------------------------------------

    #[test]
    fn epsilon_positive_when_kr_outperforms_us() {
        let t0 = HashMap::from([("SKHY".to_string(), 100.0), ("SNDK".to_string(), 50.0)]);
        let t1 = HashMap::from([("SKHY".to_string(), 102.0), ("SNDK".to_string(), 50.0)]);
        let eps = compute_epsilon("diff", "SKHY", "SNDK", &t0, &t1).unwrap();
        assert!(eps > 0.0, "expected positive epsilon, got {eps}");
        // ln(102/100) - ln(50/50) == ln(1.02)
        assert!((eps - (1.02f64).ln()).abs() < 1e-9);
    }

    #[test]
    fn epsilon_zero_when_returns_match() {
        let t0 = HashMap::from([("SKHY".to_string(), 100.0), ("SNDK".to_string(), 50.0)]);
        let t1 = HashMap::from([("SKHY".to_string(), 105.0), ("SNDK".to_string(), 52.5)]);
        let eps = compute_epsilon("diff", "SKHY", "SNDK", &t0, &t1).unwrap();
        assert!(eps.abs() < 1e-9, "expected ~0 epsilon, got {eps}");
    }

    #[test]
    fn epsilon_none_for_unimplemented_signal_model() {
        let t0 = HashMap::from([("SKHY".to_string(), 100.0), ("SNDK".to_string(), 50.0)]);
        let t1 = HashMap::from([("SKHY".to_string(), 102.0), ("SNDK".to_string(), 50.0)]);
        assert!(compute_epsilon("regression", "SKHY", "SNDK", &t0, &t1).is_none());
    }

    #[test]
    fn epsilon_none_when_a_symbol_price_is_missing() {
        let t0 = HashMap::from([("SKHY".to_string(), 100.0)]);
        let t1 = HashMap::from([("SKHY".to_string(), 102.0), ("SNDK".to_string(), 50.0)]);
        assert!(compute_epsilon("diff", "SKHY", "SNDK", &t0, &t1).is_none());
    }

    #[test]
    fn epsilon_none_for_non_positive_price() {
        let t0 = HashMap::from([("SKHY".to_string(), 0.0), ("SNDK".to_string(), 50.0)]);
        let t1 = HashMap::from([("SKHY".to_string(), 102.0), ("SNDK".to_string(), 50.0)]);
        assert!(compute_epsilon("diff", "SKHY", "SNDK", &t0, &t1).is_none());
    }

    // -------------------------------------------------------------
    // evaluate_eligibility
    // -------------------------------------------------------------

    fn fixture_detail(status: &str, force_reduce_only: bool, volume: f64) -> OrderBookDetail {
        OrderBookDetail {
            symbol: "SNDK".to_string(),
            status: Some(status.to_string()),
            market_config: Some(MarketConfig { force_reduce_only }),
            daily_quote_token_volume: Some(volume),
        }
    }

    #[test]
    fn eligibility_ok_when_active_and_liquid() {
        let detail = fixture_detail("active", false, 500_000.0);
        let (eligible, reason) = evaluate_eligibility(&detail, 100_000.0);
        assert!(eligible, "reason={reason}");
        assert_eq!(reason, "");
    }

    #[test]
    fn eligibility_fails_on_force_reduce_only() {
        let detail = fixture_detail("active", true, 500_000.0);
        let (eligible, reason) = evaluate_eligibility(&detail, 100_000.0);
        assert!(!eligible);
        assert!(reason.contains("force_reduce_only"), "reason={reason}");
    }

    #[test]
    fn eligibility_fails_below_min_volume() {
        let detail = fixture_detail("active", false, 1_000.0);
        let (eligible, reason) = evaluate_eligibility(&detail, 100_000.0);
        assert!(!eligible);
        assert!(reason.contains("daily_volume_below_min"), "reason={reason}");
    }

    #[test]
    fn eligibility_fails_when_status_not_active() {
        let detail = fixture_detail("inactive", false, 500_000.0);
        let (eligible, reason) = evaluate_eligibility(&detail, 100_000.0);
        assert!(!eligible);
        assert!(reason.contains("status=inactive"), "reason={reason}");
    }

    /// Regression test for bot-strategy#872 PR #266's self-review blocking
    /// finding: `daily_quote_token_volume` must deserialize as a bare
    /// JSON number (the real endpoint's shape), not a quoted string --
    /// the fixture-only tests above construct `OrderBookDetail` directly
    /// and would not have caught a field-type mismatch that only breaks
    /// `serde_json::from_str`/`Response::json()`. This string is a
    /// trimmed real response captured 2026-09-04 from
    /// `mainnet.zklighter.elliot.ai/api/v1/orderBookDetails`.
    #[test]
    fn order_book_details_response_deserializes_from_real_shaped_json() {
        let raw = r#"{
            "order_book_details": [
                {
                    "symbol": "SNDK",
                    "status": "active",
                    "daily_quote_token_volume": 15381082.456101,
                    "open_interest": 1288.0688,
                    "market_config": {
                        "market_margin_mode": 0,
                        "force_reduce_only": false,
                        "hidden": false
                    }
                },
                {
                    "symbol": "WDC",
                    "status": "active",
                    "daily_quote_token_volume": 0.0,
                    "market_config": {
                        "force_reduce_only": true
                    }
                }
            ]
        }"#;
        let resp: OrderBookDetailsResponse =
            serde_json::from_str(raw).expect("real-shaped orderBookDetails response must parse");
        assert_eq!(resp.order_book_details.len(), 2);
        let sndk = &resp.order_book_details[0];
        assert_eq!(sndk.daily_quote_token_volume, Some(15381082.456101));
        let (eligible, _) = evaluate_eligibility(sndk, 100_000.0);
        assert!(eligible);
        let wdc = &resp.order_book_details[1];
        let (eligible, reason) = evaluate_eligibility(wdc, 100_000.0);
        assert!(!eligible, "reason={reason}");
    }

    #[test]
    fn eligibility_defaults_missing_fields_to_ineligible() {
        let detail = OrderBookDetail {
            symbol: "SNDK".to_string(),
            status: None,
            market_config: None,
            daily_quote_token_volume: None,
        };
        let (eligible, reason) = evaluate_eligibility(&detail, 100_000.0);
        assert!(!eligible, "reason={reason}");
        // status defaults to "unknown" (!= "active") and volume defaults
        // to 0.0 (< any positive min_daily_volume_usd) -- a response
        // missing these fields must fail closed, not read as eligible.
        assert!(reason.contains("status=unknown"));
        assert!(reason.contains("daily_volume_below_min"));
    }

    // -------------------------------------------------------------
    // TradingCalendar / resolve_session_window
    // -------------------------------------------------------------

    fn fixture_calendar() -> TradingCalendar {
        let mut sessions = HashMap::new();
        sessions.insert(
            "2026-09-02".to_string(),
            SessionEntry {
                krx_is_open: true,
                krx_open_utc_us: Some(1),
                krx_close_utc_us: Some(2),
                us_is_open: true,
                us_open_utc_us: Some(3),
            },
        );
        sessions.insert(
            "2026-09-05".to_string(),
            SessionEntry {
                krx_is_open: false,
                krx_open_utc_us: None,
                krx_close_utc_us: None,
                us_is_open: true,
                us_open_utc_us: Some(30),
            },
        );
        TradingCalendar {
            calendar_version: "test-v1".to_string(),
            sessions,
        }
    }

    #[test]
    fn resolve_session_window_returns_window_for_both_open() {
        let calendar = fixture_calendar();
        let date = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        assert_eq!(resolve_session_window(&calendar, date), Some((1, 2, 3)));
    }

    #[test]
    fn resolve_session_window_none_when_krx_closed() {
        let calendar = fixture_calendar();
        let date = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        assert_eq!(resolve_session_window(&calendar, date), None);
    }

    #[test]
    fn resolve_session_window_none_when_date_not_in_calendar() {
        let calendar = fixture_calendar();
        let date = NaiveDate::from_ymd_opt(2030, 1, 1).unwrap();
        assert_eq!(resolve_session_window(&calendar, date), None);
    }

    // -------------------------------------------------------------
    // capture_t0_if_due -- regression coverage for the bug caught by
    // review: t0 capture must happen independently of (well before) the
    // t1/entry-window check, otherwise both snapshots collapse into the
    // same instant and epsilon is always ~0.
    // -------------------------------------------------------------

    #[test]
    fn capture_t0_if_due_captures_at_t0_before_t1_is_reached() {
        let window = Some((100, 200, 300)); // (t0, t1, t2)
        let mut day = DaySnapshot::default();
        let prices_at_t0 = HashMap::from([("SKHY".to_string(), 100.0)]);
        // now_us is between t0 and t1 -- must still capture.
        capture_t0_if_due(window, &mut day, 150, &prices_at_t0);
        assert_eq!(day.t0_prices, Some(prices_at_t0));
        assert!(
            day.t1_prices.is_none(),
            "t0 capture must not touch t1_prices"
        );
    }

    #[test]
    fn capture_t0_if_due_does_nothing_before_t0() {
        let window = Some((100, 200, 300));
        let mut day = DaySnapshot::default();
        let prices = HashMap::from([("SKHY".to_string(), 100.0)]);
        capture_t0_if_due(window, &mut day, 50, &prices);
        assert!(day.t0_prices.is_none());
    }

    #[test]
    fn capture_t0_if_due_never_overwrites_an_existing_snapshot() {
        let window = Some((100, 200, 300));
        let mut day = DaySnapshot::default();
        let early_prices = HashMap::from([("SKHY".to_string(), 100.0)]);
        let later_prices = HashMap::from([("SKHY".to_string(), 999.0)]);
        capture_t0_if_due(window, &mut day, 100, &early_prices);
        capture_t0_if_due(window, &mut day, 250, &later_prices);
        assert_eq!(
            day.t0_prices,
            Some(early_prices),
            "a second call (e.g. at/after t1) must not clobber the real t0 snapshot with a later price"
        );
    }

    // -------------------------------------------------------------
    // t0_snapshot_has_required_symbols / recoverable_t0_prices
    // (bot-strategy#872 PR #266 follow-up: 2026-09-04 silent-signal-loss
    // incident, PR #270 review: the first cut only checked "non-empty")
    // -------------------------------------------------------------

    #[test]
    fn snapshot_complete_when_both_primaries_present() {
        let prices = HashMap::from([
            ("SKHY".to_string(), 100.0),
            ("SNDK".to_string(), 50.0),
            ("SOXL".to_string(), 20.0),
        ]);
        assert!(t0_snapshot_has_required_symbols(&prices, "SKHY", "SNDK"));
    }

    #[test]
    fn snapshot_incomplete_when_missing_a_primary() {
        // Only a control symbol arrived -- e.g. right after a WS
        // reconnect, before kr_primary/us_primary's own first tick.
        let prices = HashMap::from([("SOXL".to_string(), 20.0)]);
        assert!(!t0_snapshot_has_required_symbols(&prices, "SKHY", "SNDK"));

        let kr_only = HashMap::from([("SKHY".to_string(), 100.0)]);
        assert!(!t0_snapshot_has_required_symbols(&kr_only, "SKHY", "SNDK"));
    }

    #[test]
    fn t0_recovery_uses_persisted_snapshot_for_same_day() {
        let prices = HashMap::from([("SKHY".to_string(), 100.0), ("SNDK".to_string(), 50.0)]);
        let recovered =
            recoverable_t0_prices("2026-09-04", Some("2026-09-04"), &prices, "SKHY", "SNDK");
        assert_eq!(recovered, Some(prices));
    }

    #[test]
    fn t0_recovery_ignores_a_stale_prior_day_snapshot() {
        let prices = HashMap::from([("SKHY".to_string(), 100.0), ("SNDK".to_string(), 50.0)]);
        // Restart lands on a new calendar day -- yesterday's t0 price is
        // not today's KRX-open price and must not be reused.
        let recovered =
            recoverable_t0_prices("2026-09-04", Some("2026-09-03"), &prices, "SKHY", "SNDK");
        assert_eq!(recovered, None);
    }

    #[test]
    fn t0_recovery_ignores_no_snapshot_and_empty_snapshot() {
        assert_eq!(
            recoverable_t0_prices("2026-09-04", None, &HashMap::new(), "SKHY", "SNDK"),
            None
        );
        // Same-day date match but an empty map (e.g. a still-default
        // RiskState that was never actually populated) must not be
        // treated as a usable recovery either.
        assert_eq!(
            recoverable_t0_prices(
                "2026-09-04",
                Some("2026-09-04"),
                &HashMap::new(),
                "SKHY",
                "SNDK"
            ),
            None
        );
    }

    #[test]
    fn t0_recovery_ignores_a_same_day_but_incomplete_snapshot() {
        // Same-day date match, non-empty, but missing us_primary -- must
        // not be trusted (PR #270 review finding: a partial snapshot from
        // control_symbols winning the WS delivery race must not be
        // silently re-recovered on a second same-day restart).
        let partial = HashMap::from([("SKHY".to_string(), 100.0), ("SOXL".to_string(), 20.0)]);
        let recovered =
            recoverable_t0_prices("2026-09-04", Some("2026-09-04"), &partial, "SKHY", "SNDK");
        assert_eq!(recovered, None);
    }

    #[test]
    fn t0_and_t1_snapshots_stay_genuinely_distinct_across_a_price_move() {
        // End-to-end reproduction of the bug: t0 is captured on its own
        // tick while the price is still 100.0; the price then moves to
        // 105.0 before t1 is reached and captured separately (mirroring
        // how `tick()` now calls maybe_capture_t0 unconditionally, before
        // maybe_enter's t1-gated capture). epsilon over this pair must be
        // nonzero, unlike the pre-fix behavior where both ended up equal.
        let window = Some((100, 200, 300));
        let mut day = DaySnapshot::default();
        let t0_prices = HashMap::from([("SKHY".to_string(), 100.0), ("SNDK".to_string(), 50.0)]);
        capture_t0_if_due(window, &mut day, 100, &t0_prices);

        // Price moves between t0 and t1 (the whole point of the KR session).
        let t1_prices = HashMap::from([("SKHY".to_string(), 105.0), ("SNDK".to_string(), 50.0)]);
        // t1 capture is unconditional-once, mirroring maybe_enter's own
        // `if self.day.t1_prices.is_none() { ... }` line.
        day.t1_prices = Some(t1_prices.clone());

        let epsilon = compute_epsilon(
            "diff",
            "SKHY",
            "SNDK",
            day.t0_prices.as_ref().unwrap(),
            &t1_prices,
        )
        .unwrap();
        assert!(
            epsilon.abs() > 1e-6,
            "epsilon must not collapse to ~0 when t0 and t1 prices genuinely differ"
        );
    }

    // -------------------------------------------------------------
    // max_notional_usd
    // -------------------------------------------------------------

    #[test]
    fn max_notional_usd_matches_equity_leverage_headroom_formula() {
        let cfg = fixture_config();
        // equity=1000 * leverage=2 * headroom=0.9
        assert!((cfg.max_notional_usd() - 1800.0).abs() < 1e-9);
    }

    #[test]
    fn lot_usd_is_clamped_by_max_notional_usd() {
        let mut cfg = fixture_config();
        cfg.lot_usd = 5000.0; // deliberately oversized
        let notional = cfg.lot_usd.min(cfg.max_notional_usd());
        assert!((notional - 1800.0).abs() < 1e-9);
    }

    #[test]
    fn lot_usd_under_cap_is_unaffected() {
        let cfg = fixture_config();
        let notional = cfg.lot_usd.min(cfg.max_notional_usd());
        assert!((notional - cfg.lot_usd).abs() < 1e-9);
    }

    // -------------------------------------------------------------
    // FullStatus / dashboard JSON shape (bot-strategy#866 PR #255
    // review round 2, nit 3 -- guards the flatten actually merging both
    // structs into one flat document, and pins the exact key names
    // debot-dashboard's web/app.js and main.go's StatusData expect).
    // -------------------------------------------------------------

    fn fixture_status() -> FullStatus {
        FullStatus {
            dashboard: DashboardStatus {
                ts: 1_000,
                updated_at: "2026-09-03T00:00:00+00:00".to_string(),
                id: "engine-b-live".to_string(),
                dex: "lighter",
                dry_run: true,
                has_position: false,
                position_count: 0,
                positions_ready: true,
                positions: vec![],
                pnl_total: 0.0,
                pnl_today: 0.0,
                pnl_source: "engine_b_live_risk_state",
                kill_switch_active: false,
                trade_stats: DashboardTradeStats {
                    trades: 0,
                    wins: 0,
                    win_rate: 0.0,
                    max_dd: 0.0,
                    pnl: 0.0,
                },
            },
            extra: EngineStatusExtra {
                ts_us: 1_000_000,
                instance_id: "engine-b-live".to_string(),
                current_date: None,
                window: None,
                day_entered: false,
                day_exited: false,
                restart_recovered: false,
                eligibility_ineligible_reasons: Vec::new(),
                session_halted: false,
                session_halt_reason: None,
                realized_pnl_session: 0.0,
                pnl_today_date: None,
                total_trades: 0,
                total_wins: 0,
                max_dd_bps: 0.0,
                max_dd_usd: 0.0,
                kill_switch: false,
                calendar_version: "test".to_string(),
            },
            han_bridge: HanBridgeStatus {
                kr_primary_symbol: "SKHY".to_string(),
                us_primary_symbol: "SNDK".to_string(),
                day_entered: false,
                day_exited: false,
                position_unconfirmed: false,
                ineligible_reasons: Vec::new(),
                session_halt_reason: None,
            },
        }
    }

    #[test]
    fn full_status_flattens_both_structs_into_one_flat_object() {
        let value = serde_json::to_value(fixture_status()).unwrap();
        let obj = value.as_object().expect("status must serialize to a JSON object");

        // debot-dashboard's StatusData (main.go) field names.
        for key in [
            "ts", "updated_at", "id", "dex", "dry_run", "has_position",
            "position_count", "positions_ready", "positions", "pnl_total",
            "pnl_today", "pnl_source", "kill_switch_active", "trade_stats",
        ] {
            assert!(obj.contains_key(key), "missing dashboard field: {key}");
        }
        let trade_stats = obj["trade_stats"].as_object().unwrap();
        for key in ["trades", "wins", "win_rate", "max_dd", "pnl"] {
            assert!(trade_stats.contains_key(key), "missing trade_stats field: {key}");
        }

        // engine_b_live-specific fields, flattened alongside the above,
        // not nested under a separate "extra" key.
        for key in [
            "ts_us", "instance_id", "current_date", "window", "day_entered",
            "day_exited", "restart_recovered", "session_halted",
            "session_halt_reason", "realized_pnl_session", "pnl_today_date",
            "total_trades", "total_wins", "max_dd_bps", "max_dd_usd",
            "kill_switch", "calendar_version",
        ] {
            assert!(obj.contains_key(key), "missing engine_b_live field: {key}");
        }
        assert!(!obj.contains_key("dashboard"), "flatten must not leave a nested \"dashboard\" key");
        assert!(!obj.contains_key("extra"), "flatten must not leave a nested \"extra\" key");

        // han_bridge is deliberately NOT flattened -- debot-dashboard
        // gates an extra UI section on this key's presence (same pattern
        // as hype-accumulator's "accumulator" block), so it must stay a
        // nested object, not spread across the top level like the other
        // two.
        let han_bridge = obj
            .get("han_bridge")
            .and_then(|v| v.as_object())
            .expect("han_bridge must be present as a nested object");
        for key in [
            "kr_primary_symbol",
            "us_primary_symbol",
            "day_entered",
            "day_exited",
            "ineligible_reasons",
            "session_halt_reason",
        ] {
            assert!(
                han_bridge.contains_key(key),
                "missing han_bridge field: {key}"
            );
        }
    }

    // -------------------------------------------------------------
    // exchange_position_for (bot-strategy#875 G-2/G-4)
    // -------------------------------------------------------------

    fn snap(symbol: &str, size: &str, sign: i32, entry: Option<&str>) -> PositionSnapshot {
        PositionSnapshot {
            symbol: symbol.to_string(),
            size: Decimal::from_str(size).unwrap(),
            sign,
            entry_price: entry.map(|e| Decimal::from_str(e).unwrap()),
        }
    }

    #[test]
    fn exchange_position_long_with_entry_price() {
        let ps = vec![
            snap("SOXL", "1.5", 1, None),
            snap("SNDK", "0.0582", 1, Some("1600.64")),
        ];
        let got = exchange_position_for(&ps, "SNDK").unwrap();
        assert_eq!(got.side, OrderSide::Long);
        assert!((got.size - 0.0582).abs() < 1e-12);
        assert!((got.entry_price.unwrap() - 1600.64).abs() < 1e-9);
    }

    #[test]
    fn exchange_position_short_uses_sign_not_size_sign() {
        // Lighter reports abs size + sign=-1 for shorts; a defensive
        // negative size must still resolve to the same short.
        for size in ["0.0582", "-0.0582"] {
            let got = exchange_position_for(&[snap("SNDK", size, -1, None)], "SNDK").unwrap();
            assert_eq!(got.side, OrderSide::Short);
            assert!((got.size - 0.0582).abs() < 1e-12);
            assert!(got.entry_price.is_none());
        }
    }

    #[test]
    fn exchange_position_flat_cases() {
        assert!(exchange_position_for(&[], "SNDK").is_none());
        assert!(exchange_position_for(&[snap("MU", "1", 1, None)], "SNDK").is_none());
        assert!(exchange_position_for(&[snap("SNDK", "0", 1, None)], "SNDK").is_none());
        assert!(exchange_position_for(&[snap("SNDK", "0.01", 0, None)], "SNDK").is_none());
    }

    #[test]
    fn exchange_position_ignores_non_positive_entry_price() {
        let got = exchange_position_for(&[snap("SNDK", "0.01", 1, Some("0"))], "SNDK").unwrap();
        assert!(got.entry_price.is_none());
    }

    #[test]
    fn cap_exit_size_passes_through_plausible_exchange_sizes() {
        assert_eq!(cap_exit_size(0.0582, 0.0582), (0.0582, false));
        assert_eq!(cap_exit_size(0.05, 0.0582), (0.05, false)); // partial remainder
        assert_eq!(cap_exit_size(0.0873, 0.0582), (0.0873, false)); // exactly 1.5x is allowed
    }

    #[test]
    fn cap_exit_size_caps_a_gross_over_report_to_the_tracked_size() {
        assert_eq!(cap_exit_size(0.2, 0.0582), (0.0582, true));
    }

    #[test]
    fn cap_exit_size_disabled_without_a_tracked_size() {
        assert_eq!(cap_exit_size(0.2, 0.0), (0.2, false));
    }

    #[test]
    fn opposite_flips_side() {
        assert_eq!(opposite(OrderSide::Long), OrderSide::Short);
        assert_eq!(opposite(OrderSide::Short), OrderSide::Long);
    }
}
