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
//!   `P_exec_exit`). No slippage/partial-fill modeling beyond what
//!   `create_order(price=None)` (Lighter-native IOC + 20% protection
//!   price) already gives.
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
//!
//! DRY_RUN must stay on until a human explicitly flips the `refuse_live`
//! gate below (mirrors `robinhood_dipgrid.rs`'s pattern: flipping
//! `ENGINE_B_LIVE_DRY_RUN=false` alone is not enough).

use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use dex_connector::{DexConnector, OrderSide, PriceUpdate};
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
    let Some(dir) = path.parent() else { return };
    let tmp = dir.join(format!(
        ".{}.tmp.{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id()
    ));
    if std::fs::write(&tmp, bytes).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    } else {
        log::warn!("[STATE] write failed for {}", path.display());
    }
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
    size: f64,
    entered_at_us: i64,
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
    /// Set once `maybe_enter` has attempted the Lighter `orderBookDetails`
    /// eligibility check for today, so a failed/successful check is never
    /// re-fetched every 5s tick for the rest of the entry window.
    eligibility_checked: bool,
    /// `Some(reason)` when the eligibility check found `kr_primary` or
    /// `us_primary` ineligible (force_reduce_only / below
    /// `min_daily_volume_usd` / not found) -- `maybe_enter` skips today's
    /// entry when this is set. Stays `None` on a fetch/parse failure
    /// (fail-open: a network blip must not silently kill the one entry
    /// opportunity for the day) -- see bot-strategy#872.
    ineligible_reason: Option<String>,
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
    #[serde(default)]
    daily_quote_token_volume: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
struct MarketConfig {
    #[serde(default)]
    force_reduce_only: bool,
}

async fn fetch_order_book_details(rest_url: &str) -> Result<Vec<OrderBookDetail>> {
    let url = format!("{}/api/v1/orderBookDetails", rest_url.trim_end_matches('/'));
    let resp: OrderBookDetailsResponse = Client::new()
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
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
    let volume: f64 = detail
        .daily_quote_token_volume
        .as_deref()
        .unwrap_or("0")
        .parse()
        .unwrap_or(0.0);
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

#[derive(Serialize)]
struct FullStatus {
    #[serde(flatten)]
    dashboard: DashboardStatus,
    #[serde(flatten)]
    extra: EngineStatusExtra,
}

struct EngineBLiveEngine {
    cfg: EngineBLiveConfig,
    connector: std::sync::Arc<dyn DexConnector + Send + Sync>,
    calendar: TradingCalendar,
    latest_price: HashMap<String, f64>,
    current_date: Option<NaiveDate>,
    window: Option<(i64, i64, i64)>, // (t0, t1, t2) us epoch for current_date
    day: DaySnapshot,
    position: Option<OpenPosition>,
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
        capture_t0_if_due(self.window, &mut self.day, now_us, &self.latest_price);
    }

    async fn maybe_enter(&mut self, now_us: i64) {
        let Some((_t0, t1, _t2)) = self.window else {
            return;
        };
        if self.day.entered || now_us < t1 {
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
        if !self.day.eligibility_checked {
            self.day.eligibility_checked = true;
            match fetch_order_book_details(&self.cfg.lighter_rest_url).await {
                Ok(details) => {
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
                                    self.day.ineligible_reason.get_or_insert_with(|| {
                                        format!("{label}={symbol}:{reason}")
                                    });
                                }
                            }
                            None => {
                                log::warn!(
                                    "[ELIGIBILITY] {label}={symbol} not found in orderBookDetails response"
                                );
                                self.day
                                    .ineligible_reason
                                    .get_or_insert_with(|| format!("{label}={symbol}:not_found"));
                            }
                        }
                    }
                }
                Err(e) => {
                    log::warn!(
                        "[ELIGIBILITY] orderBookDetails fetch failed: {e:?}; proceeding without \
                         the eligibility gate today (fail-open)"
                    );
                }
            }
        }
        if let Some(reason) = self.day.ineligible_reason.clone() {
            log::warn!("[ENTRY] skipped: {reason}");
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
            if let Err(e) = self
                .connector
                .set_leverage(&self.cfg.us_primary_symbol, self.cfg.leverage)
                .await
            {
                log::error!("[ENTRY] set_leverage failed: {e:?}");
                return;
            }
        }
        match self.submit_order(side, size, false).await {
            Ok(filled) => {
                let filled_f = filled.to_f64().unwrap_or(size);
                self.position = Some(OpenPosition {
                    side,
                    entry_price: price,
                    size: filled_f,
                    entered_at_us: now_us,
                });
                self.day.entered = true;
                // Persist immediately, matching the no-entry and exit
                // paths: without this, a restart between entry and exit
                // finds state.last_session_date still pointing at a prior
                // day, so roll_day_if_needed does not set day.entered and
                // maybe_enter would re-evaluate and potentially re-enter
                // the same day, doubling notional exposure. This does not
                // by itself recover the in-memory OpenPosition after a
                // restart (see docs/engine-b-live-operations.md's Stop and
                // recovery section) -- it only prevents a second entry.
                self.state.last_session_date = self.current_date.map(|d| d.to_string());
                atomic_write_json(&self.cfg.state_path, &self.state);
                log::info!(
                    "[ENTRY] side={side} epsilon={epsilon:.5} price={price:.4} notional=${notional_usd:.0} size={filled_f:.6}"
                );
                send_notification(
                    format!("Han Bridge ENTRY {} {}", self.cfg.us_primary_symbol, side),
                    format!(
                        "epsilon={epsilon:.5} threshold={:.5} price={price:.4} notional=${notional_usd:.0} dry_run={}",
                        self.cfg.epsilon_threshold, self.cfg.dry_run
                    ),
                );
            }
            Err(e) => log::error!("[ENTRY] order failed: {e:?}"),
        }
    }

    async fn maybe_exit(&mut self, now_us: i64) {
        let Some((_t0, _t1, t2)) = self.window else {
            return;
        };
        let Some(pos) = self.position.clone() else {
            return;
        };
        if self.day.exited || now_us < t2 {
            return;
        }
        let emergency = now_us > t2 + self.cfg.exit_deadline_secs * 1_000_000;
        if emergency {
            log::warn!("[EXIT] exit_deadline passed; forcing emergency close");
        }
        let Some(price) = self.latest_price.get(&self.cfg.us_primary_symbol).copied() else {
            return;
        };
        let exit_side = match pos.side {
            OrderSide::Long => OrderSide::Short,
            OrderSide::Short => OrderSide::Long,
        };
        match self.submit_order(exit_side, pos.size, true).await {
            Ok(_) => self.on_exit(price, now_us),
            Err(e) => log::error!("[EXIT] order failed, position still open: {e:?}"),
        }
    }

    fn on_exit(&mut self, exit_price: f64, now_us: i64) {
        let Some(pos) = self.position.take() else {
            return;
        };
        let sign = match pos.side {
            OrderSide::Long => 1.0,
            OrderSide::Short => -1.0,
        };
        let pnl = sign * (exit_price - pos.entry_price) * pos.size;
        log::info!(
            "[EXIT] side={} entry={:.4} exit={:.4} size={:.6} pnl=${:.2} held={}s",
            pos.side,
            pos.entry_price,
            exit_price,
            pos.size,
            pnl,
            (now_us - pos.entered_at_us) / 1_000_000
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
        self.maybe_enter(now).await;
        self.maybe_exit(now).await;
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
                size: pos.size.to_string(),
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
                positions_ready: !self.day.restart_recovered,
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
            extra: EngineStatusExtra {
                ts_us: now_us,
                instance_id: self.cfg.instance_id.clone(),
                current_date: self.current_date.map(|d| d.to_string()),
                window: self.window,
                day_entered: self.day.entered,
                day_exited: self.day.exited,
                restart_recovered: self.day.restart_recovered,
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
            },
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
        latest_price: HashMap::new(),
        current_date: None,
        window: None,
        day: DaySnapshot::default(),
        position: None,
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

    fn fixture_detail(status: &str, force_reduce_only: bool, volume: &str) -> OrderBookDetail {
        OrderBookDetail {
            symbol: "SNDK".to_string(),
            status: Some(status.to_string()),
            market_config: Some(MarketConfig { force_reduce_only }),
            daily_quote_token_volume: Some(volume.to_string()),
        }
    }

    #[test]
    fn eligibility_ok_when_active_and_liquid() {
        let detail = fixture_detail("active", false, "500000");
        let (eligible, reason) = evaluate_eligibility(&detail, 100_000.0);
        assert!(eligible, "reason={reason}");
        assert_eq!(reason, "");
    }

    #[test]
    fn eligibility_fails_on_force_reduce_only() {
        let detail = fixture_detail("active", true, "500000");
        let (eligible, reason) = evaluate_eligibility(&detail, 100_000.0);
        assert!(!eligible);
        assert!(reason.contains("force_reduce_only"), "reason={reason}");
    }

    #[test]
    fn eligibility_fails_below_min_volume() {
        let detail = fixture_detail("active", false, "1000");
        let (eligible, reason) = evaluate_eligibility(&detail, 100_000.0);
        assert!(!eligible);
        assert!(reason.contains("daily_volume_below_min"), "reason={reason}");
    }

    #[test]
    fn eligibility_fails_when_status_not_active() {
        let detail = fixture_detail("inactive", false, "500000");
        let (eligible, reason) = evaluate_eligibility(&detail, 100_000.0);
        assert!(!eligible);
        assert!(reason.contains("status=inactive"), "reason={reason}");
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
    }
}
