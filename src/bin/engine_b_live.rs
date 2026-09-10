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
//!   `P_exec_exit`), but the *send* is now bounded: both legs go out as
//!   `create_order_taker_ioc` marketable limits capped `slippage_bps`
//!   from the touch (default 50 bps, bot-strategy#918), replacing
//!   `create_order(price=None)`'s ±20% protection price -- which is a
//!   fill guarantee, not a price constraint. Fill *quantity* is no longer assumed from the
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
//!   *better* of two candidates -- that freeze is still #872's job. The
//!   check is fail-closed as of bot-strategy#916: an unreadable
//!   endpoint blocks the entry instead of waving it through.
//! - `OpenPosition` is persisted to `state_path` as
//!   `RiskState.open_position` and reconciled against the exchange's own
//!   position on the first tick after a start (`reconcile_startup`,
//!   bot-strategy#917): a matching position is resumed for its scheduled
//!   exit, one the exchange holds that the record does not explain is
//!   adopted for immediate close *and* halts new entries, and a persisted
//!   position the exchange no longer holds halts too (it was closed at a
//!   price this process never saw, so its PnL is unbooked). No entry goes
//!   out before that comparison succeeds; a failing `get_positions()`
//!   keeps entries blocked rather than letting one through blind. Under
//!   DRY_RUN the exchange is not the authority -- the simulated position
//!   is resumed from state, and a real position on the account is only
//!   reported, never adopted.
//! - No SIGTERM-graceful-close handling: `systemctl stop` does not
//!   reduce-only-close an open position. It now logs and notifies exactly
//!   what stays open and leaves the persisted record for the next start
//!   to resume (bot-strategy#917), but flattening on shutdown is still
//!   the operator's call.
//! - `maybe_capture_t0` locks in `day.t0_prices` on the first tick at or
//!   after `t0` that actually has a *usable* (fresh, current-generation)
//!   price for both primaries, retries every tick until then, and never
//!   recaptures once set (see
//!   `capture_t0_if_due_never_overwrites_an_existing_snapshot`). Past
//!   `t0_capture_grace_secs` the day is abandoned with a recorded
//!   `skip_reason` rather than snapshotting a mid-session price as if it
//!   were the KRX open (bot-strategy#916; before that this captured
//!   whatever `latest_price` held and only WARNed about it, leaving the
//!   day running on a snapshot the WARN itself called suspect).
//!   `RiskState.t0_prices` recovery (bot-strategy#872 PR #266/#270, the
//!   2026-09-04 silent-signal-loss fix) still covers a same-day restart
//!   after a good capture.
//!
//! ## Fail-closed inputs (bot-strategy#916)
//!
//! Every *entry* decision is made from `usable_prices`: observations
//! that passed ingest validation (positive mid inside a two-sided,
//! uncrossed book; a venue timestamp that is either plausible-and-recent
//! or ignored as a broken clock), arrived on the current feed
//! generation, and are no older than `max_price_staleness_secs`. A
//! missing or unusable input never becomes an entry -- it becomes a
//! retry inside the entry deadline and then a `skip_day` with a reason
//! visible in the log and in `status.json`.
//!
//! The gates are one-directional on purpose: `maybe_exit` and
//! `try_adopt_unconfirmed` read the last price raw (see
//! `exit_accounting_price`), so a stale feed can never keep an open
//! position from being closed or an unknown exposure from being adopted.
//! Fail-closed on entry, fail-open on getting flat.
//!
//! The feed itself runs on its own task (`spawn_price_feed`) writing into
//! a shared `PriceFeed`, not as an arm of the tick loop: `tick()` awaits
//! the exchange for seconds at a time, and a `Lagged` queued behind it
//! would not bump the generation until after the order had been sent.
//!
//! DRY_RUN must stay on until a human explicitly flips the `refuse_live`
//! gate below (mirrors `robinhood_dipgrid.rs`'s pattern: flipping
//! `ENGINE_B_LIVE_DRY_RUN=false` alone is not enough).

use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use debot::trade::execution::slippage::{send_limit_price, SendLimit};
use dex_connector::{DexConnector, OrderSide, PositionSnapshot, PriceUpdate};
use reqwest::Client;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use debot::infra::s3_mirror::S3Mirror;

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

/// The connector's minimum allowance, i.e. "cross at your own touch".
/// What [`SendLimit::AtTouch`] asks for (bot-strategy#978).
const AT_TOUCH_BPS: u32 = 1;

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
    /// How far from the touch, in basis points, an entry or exit send may
    /// cross before the remainder is cancelled rather than filled
    /// (bot-strategy#918). The predecessor path, `create_order(price =
    /// None)`, priced its IOC off the ticker with a ±20% protection
    /// price: that bounds nothing a thin book can do to a $100 lot, and
    /// the requirements ask for a marketable limit of <=50 bps. The
    /// connector itself rejects anything outside `1..=1000`, so the old
    /// 2,000 bps is unreachable through this path; `validate` refuses the
    /// same range at startup rather than letting a typo surface as a
    /// failed send in the 180 s entry window.
    slippage_bps: u32,
    /// Maximum age (seconds) a price observation may have and still be
    /// usable for an *entry* decision -- boundary capture (t0/t1), the
    /// `compute_epsilon` inputs and the order-sizing price
    /// (bot-strategy#916). Checked against local receive time, and also
    /// against the venue's own timestamp when that timestamp is
    /// plausible (see `price_obs_from_update`). The exit path never
    /// consults it -- see `usable_prices`.
    max_price_staleness_secs: i64,
    /// How many consecutive failed `orderBookDetails` fetches are
    /// tolerated before today's entry is abandoned outright rather than
    /// retried again (bot-strategy#916). `entry_deadline_secs` already
    /// bounds the retry *window*; this bounds the request count inside
    /// it too (a 5 s tick over a 180 s deadline would otherwise allow 36
    /// REST calls against a Standard-tier 60 req/min account -- see
    /// `docs/engine-b-order-spec.md` G-5).
    max_eligibility_attempts: u32,
    /// How long after `t0` a first t0 capture may still be taken before
    /// the day is abandoned, instead of snapshotting an ever-later price
    /// as if it were the KRX open (bot-strategy#916). This is the hard
    /// bound behind "never backfill a missing boundary with a later
    /// price"; it replaces the previous soft `delay_secs > 300` WARN.
    t0_capture_grace_secs: i64,
    lighter_rest_url: String,
    min_daily_volume_usd: f64,
    equity_usd_reference: f64,
    max_session_loss_bps: f64,
    /// How often (seconds) the venue's own account equity is re-read for
    /// `status.json` (bot-strategy#919). This is an operator-visibility
    /// read, never an input to a trading decision: nothing gates on it,
    /// so a failed or stale read can only blank a dashboard row, never
    /// block or allow an order. `dex-connector`'s Lighter connector
    /// already caches `get_balance` for 300 s and invalidates that cache
    /// on a WS fill, so a refresh more often than that costs nothing on
    /// the wire and still updates promptly right after an entry or exit.
    venue_equity_refresh_secs: i64,
    trading_calendar_path: PathBuf,
    kill_switch_path: PathBuf,
    risk_ack_path: PathBuf,
    state_path: PathBuf,
    status_path: PathBuf,
    pnl_log_path: PathBuf,
    /// Append-only record of every exchange fill this process observed
    /// (bot-strategy#919). One JSON object per fill, deduped by the
    /// venue's own `trade_id`, written the moment the fill is seen --
    /// so the settled account of a trade survives a crash between the
    /// fill and the close that would have summarised it.
    fills_log_path: PathBuf,
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
            // The requirements doc's marketable-limit bound (<=50 bps).
            // The 2026-09-05 feasibility study measured a round trip on
            // SNDK at 2.86-3.42 bps, so 50 bps is ~15x the observed cost
            // -- loose enough not to reject a normal fill, tight enough
            // that a torn book costs $0.50 on a $100 lot rather than $20.
            slippage_bps: env_u32("ENGINE_B_LIVE_SLIPPAGE_BPS", 50),
            // 30 s is the requirements doc's staleness bound for a
            // boundary price (bot-strategy#916).
            max_price_staleness_secs: env_i64("ENGINE_B_LIVE_MAX_PRICE_STALENESS_SECS", 30),
            max_eligibility_attempts: env_u32("ENGINE_B_LIVE_MAX_ELIGIBILITY_ATTEMPTS", 6),
            t0_capture_grace_secs: env_i64("ENGINE_B_LIVE_T0_CAPTURE_GRACE_SECS", 300),
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
            venue_equity_refresh_secs: env_i64("ENGINE_B_LIVE_VENUE_EQUITY_REFRESH_SECS", 300),
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
            fills_log_path: PathBuf::from(env_string(
                "ENGINE_B_LIVE_FILLS_LOG_PATH",
                &format!("{base_dir}/fills.jsonl"),
            )),
            pnl_log_path: PathBuf::from(env_string(
                "ENGINE_B_LIVE_PNL_LOG_PATH",
                &format!("{base_dir}/pnl.jsonl"),
            )),
            instance_id,
        }
    }

    /// Refuse a configuration the order path cannot honour, at startup
    /// rather than at 06:30 UTC. `create_order_taker_ioc` rejects a
    /// `slippage_bps` outside `1..=1000` itself, but discovering that
    /// from a send means the entry window is already open and burning:
    /// `entry_deadline_secs` is 180 s and bot-strategy#875 G-4 allows at
    /// most one entry `sendTx` per session day, so a rejected send is a
    /// lost day, not a retry.
    fn validate(&self) -> Result<()> {
        if !(1..=1000).contains(&self.slippage_bps) {
            anyhow::bail!(
                "ENGINE_B_LIVE_SLIPPAGE_BPS={} is outside the connector's accepted 1..=1000 \
                 (bot-strategy#918); 50 is the requirements doc's marketable-limit bound",
                self.slippage_bps
            );
        }
        Ok(())
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
    /// `DaySnapshot.skip_reason` for `last_session_date`'s day
    /// (pairtrade#289 Codex round 3). Persisted beside the day marker so
    /// a same-day restart restores *why* the day was settled, not only
    /// that it was: `roll_day_if_needed` rebuilds `day.entered` from
    /// `last_session_date`, and without this `status.json` would report
    /// an already-decided day with `skip_reason: null`. `None` means the
    /// day was settled by an entry rather than by a skip.
    #[serde(default)]
    last_session_skip_reason: Option<String>,
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
    /// The open position this process last knew about, persisted so a
    /// restart between entry and exit can resume tracking it for its
    /// scheduled exit instead of leaving it stranded on the exchange
    /// (bot-strategy#917). `last_session_date` only stops the day being
    /// re-entered; it says nothing about what is still open. Written on
    /// every change to `EngineBLiveEngine.position` and cleared when the
    /// exit books flat. Reconciled against the exchange's own position at
    /// startup by `reconcile_startup` -- this record is a *claim*, never
    /// evidence: the exchange decides.
    #[serde(default)]
    open_position: Option<PersistedPosition>,
    /// Records this instance cannot manage any more -- positions saved
    /// under a `us_primary` this engine no longer trades. They are moved
    /// here rather than left in `open_position`, so the configured
    /// symbol's own exposure can still be tracked and closed while the
    /// operator retains the durable trace of the others. A **list**,
    /// because two `us_primary` changes with both older positions still
    /// open would otherwise let the second overwrite the first and take
    /// a live exposure out of every report (pairtrade#300 Codex
    /// review).
    #[serde(default)]
    unmanaged_positions: Vec<PersistedPosition>,
    /// An entry order was sent and its outcome is not yet established.
    /// Written durably *before* `sendTx` and cleared when the pending
    /// confirmation resolves, because `PendingConfirm::Entry` lives only
    /// in memory: a crash inside the confirmation window would otherwise
    /// leave a restart with nothing to look for, and a single account
    /// read that comes back flat -- a false negative this file already
    /// documents -- would be taken as a clean start while
    /// `last_session_date` blocks any further entry. It carries the
    /// **symbol** the order was sent for, not just a flag: a
    /// `us_primary` change after the crash would otherwise rebind the
    /// marker to the new symbol and leave the old exposure with no
    /// record at all (pairtrade#300 Codex review).
    #[serde(default)]
    entry_in_flight: Option<String>,
}

/// `OpenPosition` reduced to what survives a restart. Deliberately a
/// separate type rather than `#[derive(Serialize)]` on `OpenPosition`:
/// `OrderSide` (dex-connector) is `Deserialize`-only, and the persisted
/// record carries two things the in-memory one does not need -- the
/// symbol it belongs to (so a config change to `us_primary` is detected
/// rather than silently applied to someone else's position) and the
/// session boundaries the exit was scheduled against.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct PersistedPosition {
    symbol: String,
    /// `"long"` / `"short"` (`OrderSide`'s own `Display`).
    side: String,
    entry_price: f64,
    entry_price_estimated: bool,
    /// True when adoption found **no** cost basis at all -- neither the
    /// exchange's `avg_entry_price` nor a WS mid. `entry_price` is then a
    /// placeholder that must never be arithmetic'd against a real price
    /// (pairtrade#300 Codex review).
    #[serde(default)]
    entry_price_unknown: bool,
    size: f64,
    open_size: f64,
    realized_partial_pnl: f64,
    entered_at_us: i64,
    flatten_asap: bool,
    /// UTC date (YYYY-MM-DD) of the session this position was entered
    /// for. A restart on a later date resumes it as `flatten_asap`.
    session_date: String,
    /// `t2 + exit_deadline_secs` for that session, when the window was
    /// known at entry. Past it, a resumed position is flattened at once
    /// instead of waiting for a window that has already gone.
    exit_deadline_us: Option<i64>,
}

/// Bring a parked record up to what the exchange currently reports, so
/// the status document and the shutdown alert describe the exposure an
/// operator would actually find. Side and size come from the venue; the
/// cost basis and already-realized PnL are kept only while the record
/// still describes the same position (same side), because a flip makes
/// the basis meaningless for what is there now (pairtrade#300 Codex
/// review).
/// A parked record for an exposure this engine has no record of -- an
/// order sent for a symbol `us_primary` no longer names, whose fill
/// this process never saw. It is unmanaged by construction: nothing here
/// can close that symbol (pairtrade#300 Codex review).
fn unmanaged_from_live(
    symbol: &str,
    live: &ExchangePosition,
    ws_price: Option<f64>,
    session_date: &str,
) -> PersistedPosition {
    let (entry_price, estimated, unknown) = match (live.entry_price, ws_price) {
        (Some(e), _) => (e, false, false),
        (None, Some(mid)) if mid > 0.0 => (mid, true, false),
        _ => (0.0, true, true),
    };
    PersistedPosition {
        symbol: symbol.to_string(),
        side: live.side.to_string(),
        entry_price,
        entry_price_estimated: estimated,
        entry_price_unknown: unknown,
        size: live.size,
        open_size: live.size,
        realized_partial_pnl: 0.0,
        entered_at_us: 0,
        flatten_asap: true,
        session_date: session_date.to_string(),
        exit_deadline_us: None,
    }
}

fn refreshed_unmanaged(
    record: PersistedPosition,
    live: &ExchangePosition,
    ws_price: Option<f64>,
) -> PersistedPosition {
    let same_side = side_from_str(&record.side) == Some(live.side);
    // Growth carries the same trap as adoption: keeping the old basis
    // while taking the larger size makes the record look fully
    // reconciled, so a later `us_primary` change back to this symbol
    // resumes it as `Resume` -- bypassing the growth handling entirely --
    // and the exit prices the externally added quantity at the original
    // entry (pairtrade#300 Codex review).
    let grown = same_side && live.size > record.open_size + 1e-12;
    let (entry_price, estimated, unknown) = match (live.entry_price, same_side, grown) {
        (Some(e), _, _) => (e, false, false),
        (None, true, false) => (
            record.entry_price,
            record.entry_price_estimated,
            record.entry_price_unknown,
        ),
        (None, true, true) => match ws_price.filter(|m| *m > 0.0) {
            Some(mid) if !record.entry_price_unknown => {
                let added = live.size - record.open_size;
                (
                    (record.entry_price * record.open_size + mid * added) / live.size,
                    true,
                    false,
                )
            }
            _ => (0.0, true, true),
        },
        (None, false, _) => (0.0, true, true),
    };
    PersistedPosition {
        side: live.side.to_string(),
        size: if same_side {
            record.size.max(live.size)
        } else {
            live.size
        },
        open_size: live.size,
        entry_price,
        entry_price_estimated: estimated,
        entry_price_unknown: unknown,
        realized_partial_pnl: if same_side {
            record.realized_partial_pnl
        } else {
            0.0
        },
        // A flip is a different position, so it must not inherit the old
        // one's schedule: promoting such a record later would otherwise
        // match the venue exactly, select `Resume { flatten_asap: false }`
        // and wait for an exit window that belonged to the position it
        // replaced (pairtrade#300 Codex review).
        flatten_asap: if same_side { record.flatten_asap } else { true },
        exit_deadline_us: if same_side {
            record.exit_deadline_us
        } else {
            None
        },
        session_date: if same_side {
            record.session_date
        } else {
            String::new()
        },
        ..record
    }
}

fn side_from_str(s: &str) -> Option<OrderSide> {
    match s.to_ascii_lowercase().as_str() {
        "long" => Some(OrderSide::Long),
        "short" => Some(OrderSide::Short),
        _ => None,
    }
}

fn load_state(path: &Path) -> RiskState {
    match std::fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
        Err(_) => RiskState::default(),
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

/// Append one JSON line. Returns whether the whole line reached the
/// file: a caller that treats a fill as counted only once it is durable
/// needs to know, and a best-effort write that silently drops a row is
/// how a "durable ledger" quietly stops being one (pairtrade#320 Codex
/// review round 2).
fn append_pnl_log(path: &Path, record: &serde_json::Value) -> bool {
    let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    else {
        log::warn!("[PNL_LOG] open failed: {}", path.display());
        return false;
    };
    match writeln!(f, "{record}") {
        Ok(()) => true,
        Err(e) => {
            log::warn!("[PNL_LOG] write failed for {}: {e}", path.display());
            false
        }
    }
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
    /// No cost basis was available at all when this position was adopted:
    /// neither `avg_entry_price` nor a WS mid. `entry_price` is a
    /// placeholder, and the close books only what was already realized
    /// rather than measuring against it (pairtrade#300 Codex review).
    entry_price_unknown: bool,
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
    /// `t2 + exit_deadline_secs` of the session this position was entered
    /// for, carried through a restart. `maybe_exit` reads *this*, not
    /// today's recomputed window: a calendar edit or an
    /// `exit_deadline_secs` change between entry and restart would
    /// otherwise move the deadline out from under an already-open
    /// position and leave it waiting past the boundary it was actually
    /// scheduled against (pairtrade#300 Codex review).
    exit_deadline_us: Option<i64>,
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
        /// True when `exit_price` came from the feed (validated mid or
        /// last raw mid). False when `maybe_exit` had no price of any
        /// kind and fell back to the tracked leg's own entry price: that
        /// number books PnL as zero on purpose, and must never be handed
        /// to `reconcile_side_flip_if_any` as the replacement leg's cost
        /// basis (pairtrade#300 Codex review).
        price_is_market: bool,
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

/// Two position quantities agree if they differ by less than a part per
/// million of the larger (plus an absolute floor for dust): the persisted
/// f64 and the exchange's decimal are the same number rendered twice, so
/// anything above this is a real divergence -- a fill, a partial close or
/// an external order that happened while this process was down -- not
/// rounding.
fn sizes_agree(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-9 + 1e-6 * a.abs().max(b.abs())
}

/// What `note_shutdown_signal` had to report. Returned rather than only
/// logged so the "nothing tracked in memory is not nothing open"
/// distinction is testable (pairtrade#300 Codex review).
#[derive(Debug, Clone, PartialEq)]
enum ShutdownReport {
    /// A position this process was actively tracking is left open.
    TrackedPosition,
    /// Nothing in memory, but the persisted record still claims one this
    /// process never reconciled against the exchange.
    UnreconciledClaim,
    /// Nothing tracked or claimed, but an order was sent whose fill this
    /// process never confirmed -- the exchange may hold a position that
    /// no record describes.
    UnconfirmedOrder,
    /// Nothing tracked and nothing claimed.
    Nothing,
}

/// What startup reconciliation decided about the account (bot-strategy#917).
/// A pure function of (what we persisted, what the exchange reports) so the
/// table below is testable without a connector.
#[derive(Debug, Clone, PartialEq)]
enum ReconcileAction {
    /// Nothing tracked, exchange flat. The normal start.
    Clean,
    /// The persisted position is still there, unchanged: resume tracking
    /// it. `flatten_asap` is true when its own exit window is unknown or
    /// already past (a prior session, or past `exit_deadline_us`), in
    /// which case `maybe_exit` closes it on the next tick instead of
    /// waiting for today's `t2`.
    Resume { flatten_asap: bool },
    /// The exchange holds a position this process has no record of.
    /// Adopted (so it gets closed) but halted: its origin is unknown.
    AdoptUnknown,
    /// We persisted a position and the exchange is flat. Something closed
    /// it while we were down (operator, liquidation, another process) at a
    /// price we never saw, so its PnL cannot be booked.
    Vanished,
    /// Both exist but disagree on symbol, side or quantity.
    Mismatch { reason: String },
}

/// The reconciliation decision table. `today` is the current UTC date,
/// `symbol` the instance's configured `us_primary`.
fn reconcile_action(
    persisted: Option<&PersistedPosition>,
    live: Option<&ExchangePosition>,
    symbol: &str,
    today: &str,
    now_us: i64,
) -> ReconcileAction {
    match (persisted, live) {
        (None, None) => ReconcileAction::Clean,
        (None, Some(_)) => ReconcileAction::AdoptUnknown,
        (Some(_), None) => ReconcileAction::Vanished,
        (Some(p), Some(live)) => {
            if p.symbol != symbol {
                return ReconcileAction::Mismatch {
                    reason: format!(
                        "persisted position is on {} but this instance trades {symbol}",
                        p.symbol
                    ),
                };
            }
            let Some(side) = side_from_str(&p.side) else {
                return ReconcileAction::Mismatch {
                    reason: format!("persisted side {:?} is not long/short", p.side),
                };
            };
            if side != live.side {
                return ReconcileAction::Mismatch {
                    reason: format!(
                        "persisted side {side} but the exchange holds {} size={:.6}",
                        live.side, live.size
                    ),
                };
            }
            // Compared against the *open* remainder, not the entry size:
            // a partial exit already booked before the restart shrinks
            // `open_size` and the exchange agrees with that, not with the
            // original fill.
            if !sizes_agree(p.open_size, live.size) {
                return ReconcileAction::Mismatch {
                    reason: format!(
                        "persisted open_size={:.8} but the exchange holds {:.8}",
                        p.open_size, live.size
                    ),
                };
            }
            let window_gone = p.session_date != today
                || p.exit_deadline_us.is_some_and(|deadline| now_us > deadline);
            ReconcileAction::Resume {
                flatten_asap: p.flatten_asap || window_gone,
            }
        }
    }
}

// ---------------------------------------------------------------------
// Price freshness (bot-strategy#916). `latest_price` used to be a bare
// `HashMap<String, f64>`: it kept the number and threw away *when* it
// arrived and *what book* produced it, so a feed that stalled (or a
// venue replaying an old snapshot -- exactly the failure the Phase 0
// observer hit in bot-strategy#908 item 7) still looked like a live
// price to every entry decision. Every observation now carries its
// receive time, the venue's own timestamp and the feed generation it
// arrived on, and every *entry* decision goes through `usable_prices`.
// ---------------------------------------------------------------------

/// How far *ahead* of local receive time a venue timestamp may be before
/// it is treated as a broken clock rather than as skew. A timestamp from
/// the future cannot be evidence of stale data, so beyond this it is
/// ignored (the local receive clock alone applies) and logged. Old
/// timestamps get no such leniency: see `venue_timestamp_us`.
const EXCHANGE_TS_MAX_FUTURE_SKEW_US: i64 = 86_400_000_000; // 1 day

/// Normalise `PriceUpdate.timestamp` to epoch micros by its magnitude.
/// The field is documented as milliseconds (the Lighter WS path divides
/// an alleged-microsecond `last_updated_at` by 1000), but a unit change
/// upstream must not either brick the engine (every update rejected as
/// decades stale) or, worse, get waved through as "no clock" -- a venue
/// replaying a snapshot from *days* ago carries a perfectly valid old
/// timestamp, and that is exactly the data this check exists to reject
/// (pairtrade#289 Codex round 2). So: seconds / millis / micros are
/// recognised by their epoch-range magnitude and converted; anything
/// outside every plausible range is `None` (genuinely meaningless).
fn venue_timestamp_us(raw: u64) -> Option<i64> {
    // Epoch ranges for 2001-09..2286-11 in each unit: 1e9..1e10 s,
    // 1e12..1e13 ms, 1e15..1e16 us. Disjoint, so magnitude is decisive.
    let raw = raw as i128;
    let us = match raw {
        1_000_000_000..=9_999_999_999 => raw * 1_000_000,
        1_000_000_000_000..=9_999_999_999_999 => raw * 1_000,
        1_000_000_000_000_000..=9_999_999_999_999_999 => raw,
        _ => return None,
    };
    i64::try_from(us).ok()
}

/// One accepted price observation, with everything needed to decide
/// later whether it is still fit to trade on (bot-strategy#916).
#[derive(Debug, Clone, Copy, PartialEq)]
struct PriceObs {
    mid: f64,
    best_bid: f64,
    best_ask: f64,
    /// Local wall clock when this update was accepted.
    received_at_us: i64,
    /// The venue's own timestamp, in micros, when it was plausible
    /// enough to compare against (see `EXCHANGE_TS_PLAUSIBILITY_US`).
    exchange_ts_us: Option<i64>,
    /// Feed generation this arrived on. Anything from an older
    /// generation is unusable: a `Lagged` broadcast means updates were
    /// dropped, so what we hold may be arbitrarily behind the book.
    generation: u64,
}

impl PriceObs {
    fn age_secs(&self, now_us: i64) -> f64 {
        (now_us - self.received_at_us) as f64 / 1_000_000.0
    }

    /// Fit to base an entry decision on: same feed generation, not
    /// future-dated (a backwards clock step must fail closed, not
    /// produce a negative age that passes every bound), and no older
    /// than `max_staleness_secs` -- measured from local receipt *and*,
    /// when the venue clock was plausible, from the venue's own
    /// timestamp. Ingest only bounds the venue age at arrival; without
    /// the second check here a quote that arrived 29 s late would stay
    /// usable for another 30 s, i.e. an entry on ~59 s-old venue data
    /// under a 30 s bound (pairtrade#289 Codex review).
    fn is_usable(&self, now_us: i64, generation: u64, max_staleness_secs: i64) -> bool {
        let bound_us = max_staleness_secs.saturating_mul(1_000_000);
        self.generation == generation
            && self.received_at_us <= now_us
            && now_us - self.received_at_us <= bound_us
            && self
                .exchange_ts_us
                .is_none_or(|ts_us| now_us - ts_us <= bound_us)
    }

    /// The older of the two ages this observation carries, for logs.
    fn effective_age_secs(&self, now_us: i64) -> f64 {
        let venue_age = self
            .exchange_ts_us
            .map(|ts_us| (now_us - ts_us) as f64 / 1_000_000.0)
            .unwrap_or(f64::MIN);
        self.age_secs(now_us).max(venue_age)
    }
}

/// Validate one incoming `PriceUpdate` and turn it into a `PriceObs`.
///
/// `Err(reason)` means the update is not trustworthy and must be
/// dropped rather than stored. Dropping (rather than storing a bad
/// value) is what makes this fail closed: whatever was held before
/// simply keeps ageing, and once it crosses `max_staleness_secs` every
/// entry path stops finding a usable price for that symbol.
///
/// The book checks matter because `mid` is derived from the top of book
/// (`(bid + ask) / 2` on the Lighter WS path): a crossed or one-sided
/// book yields a mid that is not a tradeable price even though it is a
/// positive, recent-looking number.
fn price_obs_from_update(
    update: &PriceUpdate,
    received_at_us: i64,
    generation: u64,
    max_staleness_secs: i64,
) -> Result<PriceObs, &'static str> {
    let (Some(mid), Some(best_bid), Some(best_ask)) = (
        update.mid_price.to_f64(),
        update.best_bid.to_f64(),
        update.best_ask.to_f64(),
    ) else {
        return Err("not_representable_as_f64");
    };
    if !(mid.is_finite() && best_bid.is_finite() && best_ask.is_finite()) {
        return Err("non_finite");
    }
    if mid <= 0.0 {
        return Err("non_positive_mid");
    }
    if best_bid <= 0.0 || best_ask <= 0.0 {
        return Err("one_sided_book");
    }
    if best_bid >= best_ask {
        return Err("crossed_or_locked_book");
    }
    if mid < best_bid || mid > best_ask {
        // Cannot happen on the Lighter path (mid is the arithmetic
        // midpoint of the same two levels) but is the invariant that
        // makes "mid is a tradeable price" true for any venue whose mid
        // is reported rather than derived.
        return Err("mid_outside_book");
    }
    let exchange_ts_us = venue_timestamp_us(update.timestamp)
        // Beyond this far in the future the clock is broken, not skewed;
        // ignore it rather than reject the update (a future stamp cannot
        // mean stale data).
        .filter(|ts_us| ts_us - received_at_us <= EXCHANGE_TS_MAX_FUTURE_SKEW_US);
    if let Some(ts_us) = exchange_ts_us {
        // Any recognisable timestamp older than the bound -- 31 seconds
        // or 3 weeks -- means the venue is handing us old data on a
        // healthy-looking connection (the stale-snapshot shape of
        // bot-strategy#908 item 7). Future-dated within the skew bound
        // is accepted as clock skew.
        if received_at_us - ts_us > max_staleness_secs.saturating_mul(1_000_000) {
            return Err("exchange_timestamp_stale");
        }
    }
    Ok(PriceObs {
        mid,
        best_bid,
        best_ask,
        received_at_us,
        exchange_ts_us,
        generation,
    })
}

/// The price feed's shared view: the latest accepted observation per
/// symbol, the last raw mid from *any* update, and the feed generation.
///
/// Owned by a dedicated task (`spawn_price_feed`) rather than updated
/// from the main `select!` arm, so the broadcast keeps being drained
/// while `tick()` is awaiting the exchange. Under the old shape a
/// `Lagged` that happened *during* entry preparation (eligibility fetch,
/// position read, set_leverage) sat queued behind the running `tick()`
/// and could not bump `generation` until after the order had been sent,
/// so the send-time freshness check would have accepted pre-lag
/// observations as current (pairtrade#289 Codex round 4).
///
/// Guarded by a `std::sync::Mutex`: every critical section here is a few
/// map operations with no `.await` inside, and the engine's accessors
/// return owned values so no guard ever crosses an await point.
#[derive(Debug, Default)]
struct PriceFeed {
    latest: HashMap<String, PriceObs>,
    /// Last positive mid seen per symbol from any update, accepted or
    /// rejected at ingest (bot-strategy#916). Exit accounting only: when
    /// every update is being rejected (a venue replaying a stale
    /// snapshot, say) `latest` can be empty and an open position must
    /// still be closable with *some* mid to book against. Never read by
    /// an entry decision.
    last_raw_mid: HashMap<String, f64>,
    /// Bumped every time the broadcast reports dropped updates
    /// (`Lagged`). Observations from an older generation are never
    /// usable for entry: after a drop, what we hold may be arbitrarily
    /// behind the book, and only a fresh update per symbol clears that.
    generation: u64,
}

impl PriceFeed {
    /// Validate and store one update, or return why it was dropped.
    /// Dropping (rather than storing a bad value) is what makes this
    /// fail closed: whatever was held keeps ageing until it is stale.
    fn ingest(
        &mut self,
        update: &PriceUpdate,
        received_at_us: i64,
        max_staleness_secs: i64,
    ) -> Result<(), &'static str> {
        if let Some(mid) = update.mid_price.to_f64().filter(|m| *m > 0.0) {
            self.last_raw_mid.insert(update.symbol.clone(), mid);
        }
        let obs =
            price_obs_from_update(update, received_at_us, self.generation, max_staleness_secs)?;
        self.latest.insert(update.symbol.clone(), obs);
        Ok(())
    }

    /// Returns the new generation.
    fn note_lag(&mut self) -> u64 {
        self.generation += 1;
        self.generation
    }

    fn usable(&self, now_us: i64, max_staleness_secs: i64) -> HashMap<String, f64> {
        self.latest
            .iter()
            .filter(|(_, obs)| obs.is_usable(now_us, self.generation, max_staleness_secs))
            .map(|(symbol, obs)| (symbol.clone(), obs.mid))
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
struct DaySnapshot {
    t0_prices: Option<HashMap<String, f64>>,
    t1_prices: Option<HashMap<String, f64>>,
    /// The feed generation `t1_prices` was captured on
    /// (pairtrade#289 Codex round 5). t1 is meant to be "the price right
    /// now, at the KRX close", so unlike t0 -- a historical boundary
    /// reference -- it stops being valid the moment the feed drops
    /// updates: the KR leg may be arbitrarily behind even if the US leg
    /// has since re-reported. A generation change discards the capture
    /// and forces a full recapture of *both* legs, and also blocks the
    /// send if it happens during the entry awaits.
    t1_generation: Option<u64>,
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
    /// whether it found both symbols eligible or not) -- **not** set on a
    /// fetch/parse error, so a transient network blip gets retried on the
    /// next 5 s tick rather than burning the day on one hiccup
    /// (bot-strategy#872 PR #266 self-review).
    ///
    /// Entry is gated on this being `true` (bot-strategy#916): until a
    /// response has actually been parsed, the gate's answer is unknown
    /// and no order is sent. It used to fall through to the entry on a
    /// fetch error -- a fail-open path that would trade straight through
    /// a `force_reduce_only` market whenever the REST call happened to
    /// fail. `eligibility_attempts` bounds how long that retry runs
    /// before `skip_day` ends the day instead.
    eligibility_confirmed: bool,
    /// One entry per ineligible symbol found (`kr_primary`/`us_primary`,
    /// either or both) -- kept as a `Vec` rather than the first-match-wins
    /// `Option<String>` this started as, so a same-day gate on an
    /// exchange-wide event (e.g. both symbols going `force_reduce_only`
    /// at once) still surfaces both reasons in the log/status instead of
    /// silently dropping the second (bot-strategy#872 PR #266
    /// self-review). Non-empty means `maybe_enter` skips today's entry.
    /// Stays empty on a fetch/parse failure -- that failure blocks entry
    /// through `eligibility_confirmed` instead of through this list (see
    /// bot-strategy#872, #916).
    ineligible_reasons: Vec<String>,
    /// Why today produced no entry, once that is settled
    /// (bot-strategy#916). Logged once, surfaced in `status.json`, and
    /// set by `skip_day` for every terminal no-entry path, so a DRY_RUN
    /// day that did nothing can be told apart from a day that was never
    /// evaluated at all. `None` while the day is still live.
    skip_reason: Option<String>,
    /// Failed `orderBookDetails` fetches so far today, bounded by
    /// `EngineBLiveConfig.max_eligibility_attempts` (bot-strategy#916).
    eligibility_attempts: u32,
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

/// The venue's own view of the account, as of `fetched_at_us`
/// (bot-strategy#919). Read-only operator visibility: `engine_b_live`
/// sizes and halts off `equity_usd_reference`, a config constant, and
/// nothing here feeds a trading decision -- so a stale or missing
/// reading blanks a dashboard row and never changes what the engine
/// does.
///
/// Kept with its own timestamp rather than as a bare number because a
/// number with no age cannot be told apart from a number that stopped
/// updating an hour ago; `HanBridgeStatus` publishes the age alongside
/// the value so a reader can apply their own bound (same reasoning as
/// `stale_or_missing_symbols`, bot-strategy#916).
#[derive(Clone, Copy, Debug)]
struct VenueEquity {
    /// `total_asset_value` -- the whole account, collateral plus the
    /// mark value of open positions.
    equity_usd: f64,
    /// `available_balance` -- what is free of margin. Falling toward
    /// zero while equity holds is the shape of an account that cannot
    /// open the next lot.
    available_usd: f64,
    /// When *this process* obtained the reading -- not when the venue
    /// sampled it. dex-connector serves `get_balance` from a cache with
    /// its own TTL, so a successful call can return a value the venue
    /// produced up to that TTL earlier and this timestamp does not know
    /// it (pairtrade#316 Codex review, P2). The refresh interval
    /// defaults to that same TTL to keep the gap small in the steady
    /// state, and `VenueEquityCell::failing` carries the "reads are
    /// failing" signal exactly, so nothing has to infer it from an
    /// approximate age.
    fetched_at_us: i64,
}

/// How long a single venue equity read may take before it is treated as
/// failed. Comfortably longer than a healthy REST round trip and far
/// shorter than the refresh interval, so a slow venue degrades to
/// "stale" rather than to "silently wedged".
const VENUE_EQUITY_READ_TIMEOUT_SECS: u64 = 30;

/// What the exchange says a leg actually traded, accumulated from its
/// own fill events (bot-strategy#919).
///
/// This exists because the engine books PnL off the WS mid, and the mid
/// is not the fill. On 2026-09-10's first live cycle the two differed by
/// $0.07 on a $2.5 result -- 2.8% -- entirely because the exit was
/// booked at a mid of 1717.855 while the fill was 1719.14. A mark is a
/// fine thing to *watch*; it is not what the account did.
///
/// Deliberately separate from the mid-based figures rather than
/// replacing them: until coverage is complete the settled numbers are
/// not knowable, and the issue's own rule is that an incomplete
/// settlement stays unknown rather than being topped up with a mid.
#[derive(Debug, Clone, Default, Serialize)]
struct LegFills {
    /// Quantity the venue reported filled, summed across partial fills.
    size: f64,
    /// Notional the venue reported, summed. `size * price` per fill, so
    /// `value / size` is the true quantity-weighted average price
    /// rather than an average of prices.
    value: f64,
    /// Fees the venue reported, summed. Meaningful only while
    /// `fee_unknown` is false.
    fee_reported_usd: f64,
    /// Some observed fill did not carry a fee, so this leg's total is
    /// unknowable -- and stays that way. **Sticky on purpose**: a
    /// fee-less fill followed by one that does report a fee must not
    /// resurrect a total that is missing the first fill's cost
    /// (pairtrade#320 Codex review round 2). Today it is set by every
    /// Lighter fill, since `FilledOrder::filled_fee` is hard-coded
    /// `None` in the connector's WS and REST parsers alike; it will
    /// start mattering the moment that stops being true for only some
    /// fills.
    fee_unknown: bool,
    fills: u32,
}

impl LegFills {
    /// The leg's total fee, or `None` when any observed fill did not
    /// report one. Never 0.0 for "unknown": reading a missing fee as
    /// zero would quietly overstate every settled result.
    fn fee_usd(&self) -> Option<f64> {
        (!self.fee_unknown).then_some(self.fee_reported_usd)
    }

    /// Quantity-weighted average price, or `None` when nothing has been
    /// observed yet.
    fn vwap(&self) -> Option<f64> {
        (self.size > 0.0).then(|| self.value / self.size)
    }

    /// Whether the venue's fills account for the whole quantity this
    /// leg is believed to have traded. A fill stream that is still
    /// catching up, or a leg whose events were missed entirely, must
    /// not produce a settled figure for part of a position and let it
    /// pass as the whole.
    ///
    /// The tolerance is one part in ten thousand of the expected size,
    /// which absorbs decimal rounding without absorbing a missing fill:
    /// the smallest tradable increment on these markets is orders of
    /// magnitude larger.
    fn covers(&self, expected_size: f64) -> bool {
        expected_size > 0.0 && (self.size - expected_size).abs() <= expected_size * 1e-4
    }
}

/// Shared cell the off-tick equity reader publishes into and the status
/// writer samples (bot-strategy#919, pairtrade#316 Codex P1).
#[derive(Default)]
struct VenueEquityCell {
    reading: Option<VenueEquity>,
    /// The most recent attempt failed. This -- not the reading's age --
    /// is the failure signal a reader should key on. The age is an
    /// approximation (see `VenueEquity::fetched_at_us`); whether the
    /// last read succeeded is exact.
    failing: bool,
    /// A read is out and has not come back. One at a time, so a venue
    /// that hangs cannot accumulate a task per refresh interval.
    in_flight: bool,
}

impl VenueEquityCell {
    /// WARN once on the ok -> failed edge, then stay quiet. The previous
    /// reading, its age and the `failing` flag are what a reader should
    /// be looking at; repeating the same line every interval would only
    /// bury the lines that matter.
    fn note_failure(&mut self, why: &str) {
        if !self.failing {
            log::warn!(
                "[EQUITY] venue equity unreadable ({why}); status.json keeps the last reading, \
                 its age, and venue_equity_stale=true. Nothing gates on this value."
            );
            self.failing = true;
        }
    }
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
    /// Why today produced no entry, once settled (bot-strategy#916) --
    /// lets a DRY_RUN day that did nothing be told apart from one still
    /// waiting for its window.
    skip_reason: Option<String>,
    /// Symbols whose latest price is missing or too stale/pre-lag to
    /// base an entry on right now (bot-strategy#916). Empty is the
    /// healthy state; a non-empty list at t1 is why an entry did not
    /// happen.
    stale_or_missing_symbols: Vec<String>,
    /// Current price-feed generation; increments on every broadcast
    /// `Lagged` (dropped updates).
    price_feed_generation: u64,
    /// The venue's own total account value in USD, and the age of that
    /// reading (bot-strategy#919). `None` until the first successful
    /// `get_balance`, and the age is what makes a frozen reading
    /// visible rather than silently authoritative.
    ///
    /// These live in the `han_bridge` block, not in `DashboardStatus`,
    /// on purpose. debot-dashboard blinds the generic trading view's
    /// equity headline and PnL rows for `alpha_candidate` targets --
    /// running performance is peeking on a pre-registered study. Account
    /// solvency is not performance: it answers "can this thing still
    /// place its next order", the same class of question as the halt
    /// pills that blinding already exempts. Publishing it here lets the
    /// dashboard render solvency without reopening the performance
    /// fields (see debot-dashboard `deploy/alpha-gate.md`).
    /// This build reports venue solvency at all. Always `true` here --
    /// it exists so a consumer can tell "producer has no such concept"
    /// from "producer has not read the account yet" **without relying
    /// on JSON key presence**, which does not survive a round trip
    /// through a consumer that re-encodes with `omitempty` (the
    /// debot-dashboard `/api/status` path does exactly that, PR #46
    /// Codex review). Presence has to be data, not schema.
    venue_solvency_reported: bool,
    venue_equity_usd: Option<f64>,
    venue_available_usd: Option<f64>,
    /// How long ago *this process* last obtained the reading above. It
    /// is an approximation of the venue sample's own age -- the
    /// connector may have served a cached value -- so it is a rough
    /// "how current is this", never the failure signal. That is
    /// `venue_equity_stale`, which is exact (pairtrade#316 Codex P2).
    venue_equity_age_secs: Option<i64>,
    /// The most recent read attempt failed, so the figures above are
    /// the last known ones rather than current. A reader that wants one
    /// bit for "is this trustworthy right now" wants this bit, not a
    /// threshold on the age.
    venue_equity_stale: bool,
    /// Mark-to-mid PnL of the position this engine manages, or `None`
    /// when flat, when the cost basis was never known
    /// (`entry_price_unknown`), or when no fresh US primary price is
    /// available. Deliberately `None` rather than a stale figure: the
    /// same fail-closed rule the entry path uses for boundary prices
    /// (bot-strategy#916).
    ///
    /// The name says `mid_estimate` because that is exactly what it is
    /// -- a WS mid against the recorded entry, with no fees and no
    /// funding. It is not the exchange's unrealized PnL and must not be
    /// reconciled against an account statement; the settled ledger is
    /// bot-strategy#919's remaining work. `unmanaged_positions` are
    /// excluded: this engine has no cost basis for exposure it did not
    /// open.
    unrealized_pnl_usd_mid_estimate: Option<f64>,
    /// The **emergency-close threshold** for the open position
    /// (`t2 + exit_deadline_secs`), or `None` when nothing is open or
    /// the position carries no scheduled boundary.
    ///
    /// Explicitly *not* "when retrying stops". Past this point
    /// `maybe_exit` stops waiting for the scheduled boundary and forces
    /// a close, and `poll_pending_confirm` clears an expired attempt so
    /// the next tick sends another -- the engine tries harder here, not
    /// less (pairtrade#319 Codex review). A consumer that renders this
    /// as "abandoned" would report an active close loop as a stopped
    /// one.
    ///
    /// Published so a dashboard can say a scheduled exit is late, and
    /// distinguish "still inside its window" from "past it and
    /// force-closing". bot-strategy#917 leaves an unconfirmable close
    /// open on purpose -- a documented open position beats a close
    /// nobody can verify -- and that outcome had no signal anywhere but
    /// an e-mail and the journal.
    exit_deadline_us: Option<i64>,
    /// A position **this engine opened and manages** is open.
    ///
    /// Not the same question as the document's top-level
    /// `has_position`, which counts `unmanaged_positions` too: an
    /// exposure adopted from the exchange, or left behind by a former
    /// `us_primary`, makes that flag true on a day Engine B opened
    /// nothing. A consumer that read it as "Engine B is holding" would
    /// announce an exit for a position Engine B never took
    /// (debot-dashboard#47 Codex review).
    managed_position_open: bool,
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
    /// Shared with the price-feed task, which keeps draining the
    /// broadcast while this engine is awaiting the exchange -- see
    /// `PriceFeed`.
    feed: Arc<std::sync::Mutex<PriceFeed>>,
    /// Wall clock, injectable so the tests can drive the send-time
    /// freshness re-check (which must read a *fresh* clock, not the
    /// tick's start time -- pairtrade#289 Codex review) against
    /// synthetic timestamps. Production uses `now_us`.
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    current_date: Option<NaiveDate>,
    window: Option<(i64, i64, i64)>, // (t0, t1, t2) us epoch for current_date
    day: DaySnapshot,
    position: Option<OpenPosition>,
    /// Fill / flat confirmation awaiting the exchange (live only).
    pending: Option<PendingConfirm>,
    state: RiskState,
    /// False until `reconcile_startup` has compared `state.open_position`
    /// with the exchange's own position once (bot-strategy#917). No new
    /// entry goes out while this is false: entering blind to what the
    /// account already holds is how a restart doubles exposure.
    reconciled: bool,
    /// A state write is owed to disk: either one failed, or a field was
    /// changed by a caller that persists other state in the same breath.
    /// `tick`'s backstop retries until one succeeds (pairtrade#300 Codex
    /// review).
    state_write_pending: bool,
    /// This process wrote the current `entry_in_flight` marker (rather
    /// than finding it on disk at startup). Only such a marker is
    /// cleared when the tick's confirmation resolves; a restored one is
    /// resolved by reconciliation or by seeing the exposure, never by a
    /// tick that simply has nothing pending (pairtrade#300 Codex
    /// review).
    entry_marker_owned: bool,
    /// Successful `state_path` writes, so a test can assert that a change
    /// which must be atomic really did land in **one** write rather than
    /// in two with a crash window between them (pairtrade#300 Codex
    /// review). Never read by the engine itself.
    state_writes: u64,
    last_status_write_us: i64,
    status_s3_mirror: Option<Arc<S3Mirror>>,
    /// Venue equity, published by an off-tick reader (bot-strategy#919).
    /// Shared rather than owned because the read must not run on the
    /// trading tick (pairtrade#316 Codex P1). The last good reading is
    /// kept across a failure so the dashboard can show it with its age
    /// rather than dropping to "-" on one transient REST error.
    venue_equity: Arc<std::sync::Mutex<VenueEquityCell>>,
    /// When the last refresh was *attempted*, successful or not, so a
    /// failing venue cannot turn into a retry-every-tick REST loop
    /// against a 60 req/min account.
    last_venue_equity_attempt_us: i64,
    /// A hash of what the account looked like at the last equity
    /// refresh decision. A change means a fill landed, which is exactly
    /// when the published balance is most wrong and when dex-connector's
    /// own cache has just been invalidated -- so the engine-level
    /// throttle must not sit on the next read for up to a full interval
    /// (pairtrade#316 Codex review round 2).
    last_position_fingerprint: Option<u64>,
    /// Settled fills for the position currently open (or the one just
    /// closed, until its close is booked), keyed by leg
    /// (bot-strategy#919).
    entry_fills: LegFills,
    exit_fills: LegFills,
    /// Venue trade ids already folded into the two accumulators above.
    /// The connector re-serves its whole fill cache on every call, so
    /// without this every harvest would count the same fills again.
    ///
    /// **Never cleared while the process lives.** Nothing prunes the
    /// connector's cache -- no production path calls
    /// `clear_filled_order` -- so clearing this between trades would
    /// make every historical fill look new at the next entry, pile
    /// yesterday's quantities into both of today's legs, and leave
    /// `covers()` failing for every remaining trade in the process
    /// (pairtrade#320 Codex review, P1). It grows by a handful of
    /// entries per session day.
    seen_trade_ids: std::collections::HashSet<String>,
    /// The side the open (or just-closed) trade entered on. Held here
    /// rather than read from `self.position` so a fill can still be
    /// attributed after the close has removed it -- the exit fill is
    /// routinely seen on the very tick that observes the account flat
    /// (pairtrade#320 Codex review, P1).
    fill_ledger_entry_side: Option<OrderSide>,
    /// The ledger is accumulating for a trade that has not been
    /// summarised yet.
    ///
    /// The lifecycle this makes explicit: the ledger opens at the
    /// **earliest moment a fill for the trade can exist** -- the entry
    /// send, before any harvest can run -- and closes when the trade's
    /// settled record is written. Opening it later loses fills to the
    /// wrong trade: an entry fill arriving while the send is still
    /// `PendingConfirm` would be counted against the previous trade's
    /// still-active accumulators, marked seen process-wide, and then be
    /// unavailable to the trade it actually belongs to, leaving every
    /// second-and-later trade permanently unsettled (pairtrade#320
    /// Codex review round 4).
    ///
    /// Between close and the next open the side is deliberately kept,
    /// so a fill that straggles in after the close still lands in the
    /// durable ledger with the right leg.
    fill_ledger_trade_open: bool,
}

impl EngineBLiveEngine {
    fn now(&self) -> i64 {
        (self.clock)()
    }

    /// Mid to book an exit or an adoption against, in order of trust:
    /// the validated observation, then the last raw mid from any update,
    /// then none. Deliberately never gated on freshness -- see
    /// `usable_prices` -- and deliberately never used for entry.
    fn exit_accounting_price(&self, symbol: &str) -> Option<(f64, &'static str)> {
        let feed = self.feed.lock().expect("price feed mutex");
        if let Some(obs) = feed.latest.get(symbol) {
            return Some((obs.mid, "ws_mid"));
        }
        feed.last_raw_mid
            .get(symbol)
            .copied()
            .map(|mid| (mid, "raw_last_mid_rejected_at_ingest"))
    }

    fn feed_generation(&self) -> u64 {
        self.feed.lock().expect("price feed mutex").generation
    }

    fn latest_obs(&self, symbol: &str) -> Option<PriceObs> {
        self.feed
            .lock()
            .expect("price feed mutex")
            .latest
            .get(symbol)
            .copied()
    }

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
            self.persist_state();
            if let Err(e) = std::fs::remove_file(&self.cfg.risk_ack_path) {
                log::warn!(
                    "[RISK_ACK] failed to remove {} after ack: {e:?}",
                    self.cfg.risk_ack_path.display()
                );
            }
        }
    }

    fn entries_allowed(&self) -> bool {
        // `entry_in_flight` is part of the gate, not only a restart hint.
        // `maybe_clear_halt` clears `session_halted` and
        // `position_unconfirmed` on a RISK_ACK file but never the marker,
        // so a former-symbol claim can outlive the halt it caused. With
        // only the halt checked, another flat poll would let `maybe_enter`
        // submit for the configured symbol while that older order is still
        // live; the delayed fill then lands between polls and the account
        // holds two exposures before the next tick re-engages the halt.
        // Blocking here is a no-op for this process's own entries -- the
        // marker is written immediately before `sendTx`, and `pending`
        // already suppresses `maybe_enter` until the confirmation resolves
        // and clears it (pairtrade#300 Codex review).
        //
        // A parked exposure is a gate of its own for the same reason, and
        // `session_halted` cannot stand in for it: `halt_session` returns
        // early when a halt is already engaged, so in the A -> B -> C
        // flow the reason and the alert keep naming A while B is parked
        // behind them. One `RISK_ACK` for A then cleared the only gate B
        // had. The record itself is the durable claim, and only
        // `reconcile_unmanaged` retires it -- when the venue says that
        // exposure is gone (pairtrade#300 Codex review).
        // And a saved record naming another symbol, for the third time
        // the same shape: DRY_RUN's reconciliation deliberately keeps
        // such a record in `open_position` rather than relabelling it,
        // and nothing parks it, so `session_halted` was its only gate --
        // one RISK_ACK and the next simulated entry overwrote it, losing
        // that exposure and its PnL. The live path already parks it into
        // `unmanaged_positions` during reconciliation, so this clause is
        // false there by the time it matters; it holds the DRY_RUN case
        // and the pre-reconciliation window (pairtrade#300 Codex review).
        !self.kill_switch_engaged()
            && !self.state.session_halted
            && self.reconciled
            && self.state.entry_in_flight.is_none()
            && self.state.unmanaged_positions.is_empty()
            && !self
                .state
                .open_position
                .as_ref()
                .is_some_and(|p| p.symbol != self.cfg.us_primary_symbol)
    }

    /// `t2 + exit_deadline_secs` for the session currently in `window` --
    /// the boundary an entry made *now* is scheduled against. Stamped onto
    /// the position at entry so it survives a restart, a calendar edit or
    /// a config change (pairtrade#300 Codex review).
    fn scheduled_exit_deadline_us(&self) -> Option<i64> {
        self.window
            .map(|(_t0, _t1, t2)| t2 + self.cfg.exit_deadline_secs * 1_000_000)
    }

    /// Mirror `self.position` into `state.open_position` and persist, so a
    /// restart can resume tracking it (bot-strategy#917). Called from
    /// every place that changes the position -- entry, adoption, partial
    /// close, exit, the midnight carry-over flag -- and once more at the
    /// end of each tick as a backstop. Writes only on an actual change:
    /// the common case is an unchanged `None`.
    fn persist_position(&mut self) {
        if self.sync_open_position_field() {
            self.state_write_pending = true;
        }
        if self.state_write_pending {
            self.persist_state();
        }
    }

    /// The engine's only writer of `state_path`, so that a failed write is
    /// retried rather than lost. `sync_open_position_field` has already
    /// updated the in-memory record by the time the write is attempted, so
    /// a transient failure used to be permanent: every later backstop saw
    /// the field equal to what it wanted and skipped the write, and a
    /// partial-close update -- which has no second write behind it -- could
    /// never reach disk again. A crash then restored a stale size and PnL,
    /// which is the exact failure the recovery record exists to prevent.
    /// The flag stays set until some write succeeds, and `tick`'s backstop
    /// retries every tick (pairtrade#300 Codex review).
    fn persist_state(&mut self) {
        match atomic_write_json_checked(&self.cfg.state_path, &self.state) {
            Ok(()) => {
                self.state_write_pending = false;
                self.state_writes += 1;
            }
            Err(e) => {
                self.state_write_pending = true;
                log::warn!(
                    "[STATE] write failed for {}: {e} -- retrying on the next tick",
                    self.cfg.state_path.display()
                );
            }
        }
    }

    /// The in-memory half of `persist_position`: update
    /// `state.open_position` and report whether it changed, without
    /// writing. Callers that are about to persist other state in the same
    /// breath (`on_exit` -> `mark_day_acted`) use this so the close and
    /// its accounting land in **one** write -- clearing the recovery
    /// marker first and booking the PnL second leaves a crash window
    /// where disk says "flat, nothing to reconcile" while the trade was
    /// never counted (pairtrade#300 Codex review).
    fn sync_open_position_field(&mut self) -> bool {
        // Never let a `None` erase the record before startup
        // reconciliation has run: between process start and the first
        // successful `get_positions()`, `self.position` is `None` because
        // nothing has been restored yet, not because the account is flat.
        // Clearing it there would turn a resumable position into an
        // unexplained one on the next read -- flattened and halted
        // instead of exited on schedule (pairtrade#300 Codex review).
        if self.position.is_none() {
            if !self.reconciled {
                return false;
            }
            // Nor a record for a symbol this instance no longer trades:
            // `self.position` is `None` there because reconciliation
            // refused to manage an exposure it cannot send orders for,
            // not because anything closed. Discarding it would lose the
            // only durable trace of that position (pairtrade#300 Codex
            // review).
            if self
                .state
                .open_position
                .as_ref()
                .is_some_and(|p| p.symbol != self.cfg.us_primary_symbol)
            {
                return false;
            }
        }
        let persisted = self.position.as_ref().map(|p| PersistedPosition {
            symbol: self.cfg.us_primary_symbol.clone(),
            side: p.side.to_string(),
            entry_price: p.entry_price,
            entry_price_estimated: p.entry_price_estimated,
            entry_price_unknown: p.entry_price_unknown,
            size: p.size,
            open_size: p.open_size,
            realized_partial_pnl: p.realized_partial_pnl,
            entered_at_us: p.entered_at_us,
            flatten_asap: p.flatten_asap,
            session_date: self.current_date.map(|d| d.to_string()).unwrap_or_default(),
            // The position's own boundary, not one recomputed from
            // whatever `self.window` says now (pairtrade#300 Codex review).
            exit_deadline_us: p.exit_deadline_us,
        });
        if self.state.open_position == persisted {
            return false;
        }
        self.state.open_position = persisted;
        true
    }

    /// Compare the persisted position with the exchange's own once, before
    /// this process is allowed to enter anything (bot-strategy#917).
    ///
    /// Under DRY_RUN the exchange is *not* the authority: this process
    /// never sent the orders behind whatever the account holds, so it
    /// resumes its own simulated position from state and only reports a
    /// real position it can see but does not own. Live, the exchange
    /// decides and `reconcile_action`'s table applies.
    ///
    /// A failed read leaves `reconciled` false: entries stay blocked and
    /// the next tick retries. That is the fail-closed direction -- an
    /// account we cannot read is not an account we can safely add to.
    async fn reconcile_startup(&mut self, now_us: i64) {
        let symbol = self.cfg.us_primary_symbol.clone();
        let today = self.current_date.map(|d| d.to_string()).unwrap_or_default();
        if self.cfg.dry_run {
            match self.connector.get_positions().await {
                Ok(positions) => {
                    if let Some(live) = exchange_position_for(&positions, &symbol) {
                        log::error!(
                            "[RECONCILE] DRY_RUN, but the exchange holds {} {symbol} size={:.6} -- \
                             this process did not open it and will not close it; check the account",
                            live.side,
                            live.size
                        );
                    }
                }
                Err(e) => log::warn!(
                    "[RECONCILE] DRY_RUN: get_positions failed ({e:?}); continuing on persisted \
                     state alone"
                ),
            }
            if let Some(p) = self.state.open_position.clone() {
                // A `us_primary` change must not relabel the saved
                // position onto the new instrument: `restore_persisted`
                // would then price and close it as that other symbol,
                // corrupting the simulated trade and its PnL. Keep the
                // record, refuse to manage it, and say so
                // (pairtrade#300 Codex review).
                if p.symbol != symbol {
                    let reason = format!(
                        "dry_run_position_on_another_symbol: the saved simulated position is on \
                         {} but this instance now trades {symbol} -- it is neither resumed nor \
                         relabelled, and the record is kept",
                        p.symbol
                    );
                    log::error!("[RECONCILE] {reason}");
                    // And no *new* simulated entry may join it. Marking
                    // this reconciled without halting let the next session
                    // enter on the new symbol, whose `persist_position`
                    // then overwrote the kept record -- losing the very
                    // trade this branch refused to relabel
                    // (pairtrade#300 Codex review).
                    self.halt_session(reason);
                    self.reconciled = true;
                    return;
                }
                let window_gone =
                    p.session_date != today || p.exit_deadline_us.is_some_and(|d| now_us > d);
                match self.restore_persisted(&p, p.flatten_asap || window_gone) {
                    Ok(()) => log::warn!(
                        "[RECONCILE] DRY_RUN: resumed the simulated {} {symbol} size={:.6} from \
                         state (session {}, flatten_asap={})",
                        p.side,
                        p.open_size,
                        p.session_date,
                        p.flatten_asap || window_gone
                    ),
                    Err(reason) => {
                        log::error!(
                            "[RECONCILE] DRY_RUN: dropping unusable persisted position: {reason}"
                        );
                        self.state.open_position = None;
                        self.persist_state();
                    }
                }
            }
            self.reconciled = true;
            return;
        }
        let positions = match self.connector.get_positions().await {
            Ok(p) => p,
            Err(e) => {
                log::warn!(
                    "[RECONCILE] get_positions failed ({e:?}); entries stay blocked until the \
                     account can be read"
                );
                return;
            }
        };
        let live = exchange_position_for(&positions, &symbol);
        // Before `persisted` is read: a record parked in
        // `unmanaged_positions` is still an exposure claim, nothing else
        // looks at it (`reconcile_action`, `write_status_if_due` and the
        // shutdown report all read `open_position` / `self.position`),
        // and a symbol that has come back under management has to be in
        // the managed slot by the time the decision table runs -- else it
        // is adopted from the exchange as an unknown position and its
        // basis and partial PnL are lost (pairtrade#300 Codex review).
        self.reconcile_unmanaged(&positions);
        // An order was sent whose outcome this process never established.
        // A single read that comes back flat proves nothing (the file's
        // own false-negative caveat), so this becomes the sticky
        // `position_unconfirmed` claim: `try_adopt_unconfirmed` then
        // re-checks the exchange every tick and adopts the position if it
        // appears, and only RISK_ACK clears it
        // (pairtrade#300 Codex review).
        if let Some(sent_for) = self.state.entry_in_flight.take() {
            self.state_write_pending = true;
            if sent_for == symbol {
                let reason = format!(
                    "entry_in_flight_across_restart: an entry order for {symbol} was sent and \
                     this process died before its fill was confirmed -- the account may hold a \
                     position no record describes; reconcile against the exchange, then RISK_ACK"
                );
                log::error!("[RECONCILE] {reason}");
                self.state.position_unconfirmed = true;
                self.halt_session(reason);
            } else {
                // The order went out for a symbol this instance no longer
                // trades, so `position_unconfirmed` -- which drives an
                // adoption poll against `us_primary` -- would watch the
                // wrong instrument. Whatever it created has to be handled
                // as an unmanaged exposure (pairtrade#300 Codex review).
                let live = exchange_position_for(&positions, &sent_for);
                // Only a *sighting* resolves this. A read that shows
                // nothing proves nothing this soon after a send, so the
                // marker is put back and `poll_foreign_in_flight` keeps
                // looking every tick -- otherwise one flat snapshot
                // retired the claim and nothing ever watched that symbol
                // again (pairtrade#300 Codex review).
                if live.is_none() {
                    self.state.entry_in_flight = Some(sent_for.clone());
                }
                let reason = format!(
                    "entry_in_flight_on_a_former_symbol: an entry order for {sent_for} was sent \
                     and never confirmed, and us_primary is now {symbol}. This engine cannot \
                     close {sent_for}{} -- flatten it by hand",
                    match live.as_ref() {
                        Some(live) => format!(
                            "; the venue reports {} size={:.6} there",
                            live.side, live.size
                        ),
                        None => ", and the venue reports nothing there right now (a single read \
                             proves nothing this soon after a send)"
                            .to_string(),
                    }
                );
                log::error!("[RECONCILE] {reason}");
                if let Some(live) = live {
                    let mid = self.exit_accounting_price(&sent_for).map(|(mid, _)| mid);
                    let record = unmanaged_from_live(&sent_for, &live, mid, &today);
                    self.state
                        .unmanaged_positions
                        .retain(|q| q.symbol != record.symbol);
                    self.state.unmanaged_positions.push(record);
                }
                self.halt_session(reason);
            }
        }
        let persisted = self.state.open_position.clone();
        // A change to `us_primary` while a position is still open on the
        // *old* symbol would otherwise look like `Vanished` -- the lookup
        // above only asks about the configured symbol -- and the record
        // would be discarded while the exposure stayed on the exchange.
        // This engine sends every order for `us_primary`, so it cannot
        // close the old symbol either: halt, keep the record, and say
        // exactly what a human has to do (pairtrade#300 Codex review).
        if let Some(stale) = persisted
            .as_ref()
            .filter(|p| p.symbol != symbol)
            .and_then(|p| {
                exchange_position_for(&positions, &p.symbol).map(|live| (p.symbol.clone(), live))
            })
        {
            // `stale_live`, not `live`: the outer binding is the
            // *configured* symbol's position, which is handled below.
            let (old_symbol, stale_live) = stale;
            let reason = format!(
                "us_primary is now {symbol} but a {} {old_symbol} position size={:.6} is still \
                 open; this engine only ever sends orders for {symbol}, so it cannot close that \
                 one -- flatten {old_symbol} by hand (or set us_primary back to {old_symbol}). \
                 The record is kept, not discarded",
                stale_live.side, stale_live.size
            );
            log::error!("[RECONCILE] {reason}");
            self.halt_session(reason);
            // The record is kept -- but out of `open_position`, because
            // the configured symbol may have an exposure of its own that
            // has to be tracked and closed. Leaving the old record in the
            // managed slot meant either losing it to the next
            // `persist_position`, or leaving a live position on the
            // configured symbol with nothing managing it: the halt blocks
            // entries, and `maybe_exit` only ever closes `self.position`
            // (pairtrade#300 Codex review).
            if let Some(stale_record) = self.state.open_position.take() {
                // Through the same refresh every other parked record
                // gets: this restart may be the one on which the old
                // exposure changed side or size, and `reconcile_unmanaged`
                // has already run for this startup, so an unrefreshed
                // record would describe the wrong exposure for the whole
                // process lifetime (pairtrade#300 Codex review).
                if side_from_str(&stale_record.side) != Some(stale_live.side) {
                    self.book_orphaned_partial_pnl(
                        &stale_record,
                        "the venue reports the opposite side on the symbol being parked",
                    );
                }
                let mid = self
                    .exit_accounting_price(&stale_record.symbol)
                    .map(|(mid, _)| mid);
                let refreshed = refreshed_unmanaged(stale_record, &stale_live, mid);
                self.state
                    .unmanaged_positions
                    .retain(|q| q.symbol != refreshed.symbol);
                self.state.unmanaged_positions.push(refreshed);
            }
            self.state_write_pending = true;
            if let Some(live) = live.as_ref() {
                self.adopt_from_exchange(
                    live,
                    now_us,
                    &format!(
                        "us_primary is now {symbol} and the exchange holds a position in it, \
                         beside the unmanaged {old_symbol} exposure"
                    ),
                );
            } else {
                self.persist_state();
            }
            self.reconciled = true;
            return;
        }
        let action = reconcile_action(persisted.as_ref(), live.as_ref(), &symbol, &today, now_us);
        match action {
            ReconcileAction::Clean => {
                log::info!("[RECONCILE] account flat and nothing tracked -- clean start");
            }
            ReconcileAction::Resume { flatten_asap } => {
                let p = persisted.expect("Resume implies a persisted position");
                match self.restore_persisted(&p, flatten_asap) {
                    Ok(()) => log::warn!(
                        "[RECONCILE] resumed the open {} {symbol} size={:.6} entered {} \
                         (session {}, flatten_asap={flatten_asap}) -- its scheduled exit is back \
                         under management",
                        p.side,
                        p.open_size,
                        p.entered_at_us,
                        p.session_date
                    ),
                    Err(reason) => {
                        // Only reachable if the record is corrupt in a way
                        // the decision table cannot see (it validates side
                        // and symbol first); treat it as unknown origin.
                        log::error!("[RECONCILE] persisted record unusable ({reason})");
                        self.adopt_from_exchange(
                            live.as_ref().expect("Resume implies a live position"),
                            now_us,
                            &format!("unusable persisted record: {reason}"),
                        );
                    }
                }
            }
            ReconcileAction::AdoptUnknown => {
                let live = live.as_ref().expect("AdoptUnknown implies a live position");
                self.adopt_from_exchange(
                    live,
                    now_us,
                    "the exchange holds a position this process has no record of",
                );
            }
            ReconcileAction::Mismatch { ref reason } => {
                let live = live.as_ref().expect("Mismatch implies a live position");
                self.adopt_from_exchange(live, now_us, reason);
            }
            ReconcileAction::Vanished => {
                let p = persisted.expect("Vanished implies a persisted position");
                let reason = format!(
                    "position_vanished_while_down: tracked {} {symbol} size={:.6} entered {} but \
                     the exchange is flat -- closed at a price this process never saw, so its PnL \
                     is unbooked",
                    p.side, p.open_size, p.entered_at_us
                );
                log::error!("[RECONCILE] {reason}");
                // The remainder's result is genuinely unknown, but any
                // reductions that closed before the shutdown were
                // realized at their own prices. Discarding the record is
                // the only copy of that going, so it is booked first --
                // otherwise the session PnL and the drawdown it drives
                // stay permanently short of money that was measured
                // (pairtrade#300 Codex review).
                self.book_orphaned_partial_pnl(
                    &p,
                    "the exchange is flat, so only its final remainder is unbooked",
                );
                // Both fields in one write. Clearing the record first and
                // halting second leaves a crash window in which disk says
                // "flat, nothing to reconcile" while the close this
                // process never saw is still unbooked, so the next start
                // would enter as if nothing had happened
                // (pairtrade#300 Codex review).
                self.state.open_position = None;
                self.state_write_pending = true;
                self.halt_session(reason);
                if self.state_write_pending {
                    // Already halted, so `halt_session` returned without
                    // writing; the cleared record still has to land.
                    self.persist_state();
                }
            }
        }
        self.reconciled = true;
    }

    /// Book the reductions a record already realized, at the moment that
    /// record stops being the thing that carries them.
    ///
    /// Every path that discards or replaces a `PersistedPosition` needs
    /// this, and they kept being found one at a time: the remainder's
    /// result may be unknown (closed while we were down, flattened by
    /// hand, flipped away), but reductions that completed *before* that
    /// were realized at their own prices and this record is the only
    /// copy. No trade counter moves -- nothing closed here -- only the
    /// session and day PnL the drawdown halt is measured against
    /// (pairtrade#300 Codex review).
    fn book_orphaned_partial_pnl(&mut self, record: &PersistedPosition, why: &str) {
        if record.realized_partial_pnl == 0.0 {
            return;
        }
        log::error!(
            "[RECONCILE] booking ${:.2} of partial PnL already realized on the {} {} record \
             ({why}); only its final remainder is unbooked",
            record.realized_partial_pnl,
            record.side,
            record.symbol
        );
        self.state.realized_pnl_session += record.realized_partial_pnl;
        self.state.pnl_today += record.realized_partial_pnl;
        // Same accounting a close runs: this moves the number the
        // session-loss halt is measured against.
        self.reprice_equity_and_drawdown();
        self.state_write_pending = true;
    }

    /// Keep the two durable claims that gate entry in step with the
    /// venue, on every tick rather than only at startup.
    ///
    /// Both are held by `entries_allowed`, and neither can be retired by
    /// an operator's `RISK_ACK`: the in-flight marker is retired by a
    /// sighting, and a parked record by the venue reporting that symbol
    /// flat. `reconcile_unmanaged` used to run only from
    /// `reconcile_startup`, which stops once `reconciled` is true, so an
    /// operator who flattened a parked exposure by hand had no way to
    /// re-open entries short of a restart (pairtrade#300 Codex review).
    ///
    /// One account read serves both, deliberately: the two conditions
    /// coincide exactly when the account is in its worst state, and two
    /// serial REST awaits in a tick is how the loop's latency budget gets
    /// spent.
    async fn poll_unresolved_claims(&mut self, now_us: i64) {
        let foreign = self
            .state
            .entry_in_flight
            .clone()
            .filter(|s| *s != self.cfg.us_primary_symbol);
        if foreign.is_none() && self.state.unmanaged_positions.is_empty() {
            return;
        }
        let positions = match self.connector.get_positions().await {
            Ok(positions) => positions,
            Err(e) => {
                log::warn!(
                    "[RECONCILE] get_positions failed while watching unresolved claims ({e:?})"
                );
                return;
            }
        };
        if let Some(sent_for) = foreign {
            self.sight_foreign_in_flight(&positions, &sent_for, now_us);
        }
        // After the sighting, so an exposure recorded above is checked
        // against this same snapshot rather than waiting a tick.
        if !self.state.unmanaged_positions.is_empty() {
            self.reconcile_unmanaged(&positions);
        }
        if self.state_write_pending {
            self.persist_state();
        }
    }

    /// The in-flight half of `poll_unresolved_claims`: an order went out
    /// for a symbol `us_primary` no longer names, and only a *sighting*
    /// resolves it. `try_adopt_unconfirmed` cannot do this -- that polls
    /// the configured symbol (pairtrade#300 Codex review).
    fn sight_foreign_in_flight(
        &mut self,
        positions: &[PositionSnapshot],
        sent_for: &str,
        now_us: i64,
    ) {
        let Some(live) = exchange_position_for(positions, sent_for) else {
            return;
        };
        log::error!(
            "[RECONCILE] the unconfirmed {sent_for} order did create a position ({} size={:.6}); \
             recording it as an unmanaged exposure -- this engine cannot close it, flatten it \
             by hand",
            live.side,
            live.size
        );
        let mid = self.exit_accounting_price(sent_for).map(|(mid, _)| mid);
        let today = self.current_date.map(|d| d.to_string()).unwrap_or_default();
        let record = unmanaged_from_live(sent_for, &live, mid, &today);
        self.state
            .unmanaged_positions
            .retain(|q| q.symbol != record.symbol);
        self.state.unmanaged_positions.push(record);
        // Resolved as a *claim*: the exposure now has a record that
        // every start re-checks. The halt has to be re-engaged rather
        // than assumed -- `maybe_clear_halt` only needs a RISK_ACK file,
        // and it clears `session_halted` without touching
        // `entry_in_flight`, so an operator who acknowledged the
        // "an order went out for a symbol we no longer trade" halt
        // before the fill was visible leaves this arriving exposure with
        // entries unblocked (pairtrade#300 Codex review).
        self.state.entry_in_flight = None;
        self.state_write_pending = true;
        self.halt_session(format!(
            "unmanaged_position_sighted: the unconfirmed {sent_for} order filled after \
             us_primary moved to {}; {} size={:.6} is open on a symbol this engine cannot \
             close -- flatten it by hand, then RISK_ACK",
            self.cfg.us_primary_symbol, live.side, live.size
        ));
        let _ = now_us;
    }

    /// Check the exposure this engine cannot manage against the exchange:
    /// cleared when the venue says it is gone, and kept with the session
    /// halted while it is still there. Without this the claim survives on
    /// disk but nothing ever looks at it again (pairtrade#300 Codex
    /// review).
    ///
    /// Run from `reconcile_startup` *and* from every tick via
    /// `poll_unresolved_claims`, because the record gates entry and only
    /// the venue can retire it. Safe to repeat: the promotion branch
    /// needs `p.symbol == us_primary`, which cannot become true while the
    /// process runs, so a tick only refreshes side/size from the venue,
    /// re-engages the halt (a no-op when one is engaged) or retires a
    /// record the venue no longer reports.
    fn reconcile_unmanaged(&mut self, positions: &[PositionSnapshot]) {
        if self.state.unmanaged_positions.is_empty() {
            return;
        }
        let mut kept: Vec<PersistedPosition> = Vec::new();
        let mut halt_reason: Option<String> = None;
        for p in std::mem::take(&mut self.state.unmanaged_positions) {
            // `us_primary` came back to this symbol: the engine can send
            // orders for it again, so the record belongs in the managed
            // slot. Left here it would be adopted from the exchange as
            // an unknown position -- losing its basis and its partial
            // PnL -- and then linger as a duplicate claim after that
            // adopted position closed (pairtrade#300 Codex review).
            if p.symbol == self.cfg.us_primary_symbol {
                // The managed slot may be held by a record that is
                // itself foreign now (B was configured, A was parked,
                // and us_primary just went back to A). Parking B first
                // frees the slot for A's own record, basis and partial
                // PnL included -- otherwise A was discarded here and
                // then re-adopted from the exchange a moment later
                // without any of it (pairtrade#300 Codex review).
                let occupied_by_foreign = self
                    .state
                    .open_position
                    .as_ref()
                    .is_some_and(|q| q.symbol != self.cfg.us_primary_symbol);
                if occupied_by_foreign {
                    let displaced = self.state.open_position.take().expect("checked");
                    let live = exchange_position_for(positions, &displaced.symbol);
                    log::warn!(
                        "[RECONCILE] parking the {} record to make room for {}, which is \
                         us_primary again",
                        displaced.symbol,
                        p.symbol
                    );
                    match live {
                        Some(live) => {
                            if side_from_str(&displaced.side) != Some(live.side) {
                                self.book_orphaned_partial_pnl(
                                    &displaced,
                                    "the venue reports the opposite side on the symbol \
                                     being parked",
                                );
                            }
                            // A live exposure this engine can no longer
                            // close is a halt in its own right. Without
                            // it, an earlier halt already cleared by
                            // RISK_ACK would let the promoted symbol
                            // trade on while this one stayed open and
                            // unmanaged (pairtrade#300 Codex review).
                            halt_reason.get_or_insert(format!(
                                "unmanaged_position_still_open: {} {} size={:.6} was displaced \
                                 when us_primary returned to {}, and this engine cannot close \
                                 it -- flatten it by hand",
                                live.side, displaced.symbol, live.size, p.symbol
                            ));
                            let mid = self
                                .exit_accounting_price(&displaced.symbol)
                                .map(|(mid, _)| mid);
                            kept.push(refreshed_unmanaged(displaced, &live, mid));
                        }
                        None => {
                            // Gone from the venue: nothing left to park,
                            // but what it realized is still money.
                            self.book_orphaned_partial_pnl(
                                &displaced,
                                "it is flat on the venue and its record is being retired",
                            );
                        }
                    }
                }
                if self.state.open_position.is_none() {
                    log::warn!(
                        "[RECONCILE] us_primary is {} again -- taking the parked {} record \
                         (size={:.6}) back under management",
                        p.symbol,
                        p.side,
                        p.open_size
                    );
                    self.state.open_position = Some(p);
                    self.state_write_pending = true;
                } else {
                    // A managed record for this same symbol already
                    // exists; two claims on one symbol cannot both be
                    // resumed.
                    self.book_orphaned_partial_pnl(
                        &p,
                        "a managed record already exists for the same symbol",
                    );
                    self.state_write_pending = true;
                }
                continue;
            }
            match exchange_position_for(positions, &p.symbol) {
                Some(live) => {
                    let reason = format!(
                        "unmanaged_position_still_open: {} {} size={:.6} is open on a symbol \
                         this engine no longer trades (us_primary={}), so it cannot be closed \
                         from here -- flatten it by hand",
                        live.side, p.symbol, live.size, self.cfg.us_primary_symbol
                    );
                    log::error!("[RECONCILE] {reason}");
                    halt_reason.get_or_insert(reason);
                    // The exchange is the authority on what is open, and
                    // this record is what `status.json` and the shutdown
                    // alert show. Keeping the pre-shutdown side and size
                    // pointed the operator following the flatten warning
                    // at the wrong exposure (pairtrade#300 Codex review).
                    //
                    // A flip retires the old leg, so what it realized is
                    // booked here rather than carried onto the new one --
                    // the same rule every other replacement follows.
                    if side_from_str(&p.side) != Some(live.side) {
                        self.book_orphaned_partial_pnl(
                            &p,
                            "the venue now reports the opposite side on this parked symbol",
                        );
                    }
                    self.state_write_pending = true;
                    let mid = self.exit_accounting_price(&p.symbol).map(|(mid, _)| mid);
                    kept.push(refreshed_unmanaged(p, &live, mid));
                }
                None => {
                    log::warn!(
                        "[RECONCILE] the unmanaged {} record ({} size={:.6}) is gone from the \
                         exchange -- clearing it; whatever closed its remainder did so at a \
                         price this process never saw",
                        p.symbol,
                        p.side,
                        p.open_size
                    );
                    self.book_orphaned_partial_pnl(&p, "its unmanaged record was flattened");
                    self.state_write_pending = true;
                }
            }
        }
        self.state.unmanaged_positions = kept;
        if let Some(reason) = halt_reason {
            self.halt_session(reason);
        }
    }

    /// Rebuild `self.position` from a persisted record. `Err` only when
    /// the record cannot be turned into a position at all (unparseable
    /// side); the caller decides what that means.
    fn restore_persisted(
        &mut self,
        p: &PersistedPosition,
        flatten_asap: bool,
    ) -> Result<(), String> {
        let side = side_from_str(&p.side).ok_or_else(|| format!("side {:?}", p.side))?;
        self.position = Some(OpenPosition {
            side,
            entry_price: p.entry_price,
            entry_price_estimated: p.entry_price_estimated,
            entry_price_unknown: p.entry_price_unknown,
            size: p.size,
            open_size: p.open_size,
            realized_partial_pnl: p.realized_partial_pnl,
            entered_at_us: p.entered_at_us,
            flatten_asap,
            exit_deadline_us: p.exit_deadline_us,
        });
        self.persist_position();
        Ok(())
    }

    /// Take over a position the exchange reports but our own record does
    /// not explain: track it so it gets closed (`flatten_asap`), and halt
    /// so no *new* entry joins it until an operator has looked
    /// (bot-strategy#917). The cost basis is the exchange's own
    /// `avg_entry_price` when it has one, else the last WS mid -- and then
    /// the booked PnL is flagged as an estimate.
    fn adopt_from_exchange(&mut self, live: &ExchangePosition, now_us: i64, reason: &str) {
        // A mismatch on quantity alone still describes the *same* position:
        // same symbol, same side, with reductions already booked before the
        // shutdown. Dropping `realized_partial_pnl` there would silently
        // omit that PnL when the adopted remainder closes, so carry it (and
        // the cost basis it was booked against) forward. A side or symbol
        // disagreement is a different position and carries nothing
        // (pairtrade#300 Codex review).
        // Identity alone decides this. Requiring nonzero partial PnL also
        // threw away the *cost basis* of a same-symbol, same-side position
        // whose size merely changed while the process was down: with no
        // `avg_entry_price` from the exchange the adoption then priced it
        // at the current mid (or zero), and the immediate close omitted
        // every dollar between the real entry and the restart
        // (pairtrade#300 Codex review).
        let carried = self.state.open_position.as_ref().filter(|p| {
            p.symbol == self.cfg.us_primary_symbol && side_from_str(&p.side) == Some(live.side)
        });
        let carried_partial_pnl = carried.map(|p| p.realized_partial_pnl).unwrap_or(0.0);
        // The persisted basis describes the quantity that was already
        // there. It carries onto a *reduction* unchanged, but applying it
        // to quantity added while the process was down would price the
        // added part at the original entry -- `on_exit` multiplies the
        // basis by the whole `open_size`. Growth blends the added part in
        // at whatever price is available, and admits an unknown basis
        // when there is none (pairtrade#300 Codex review).
        let carried_basis = carried
            .filter(|p| !p.entry_price_unknown)
            .map(|p| (p.entry_price, p.entry_price_estimated, p.open_size));
        if carried_partial_pnl != 0.0 {
            log::warn!(
                "[RECONCILE] carrying ${carried_partial_pnl:.2} of already-booked partial PnL \
                 into the adopted position"
            );
        }
        // A side flip makes the old leg's *remainder* unreconcilable, but
        // the reductions that closed before the shutdown were realized at
        // their own prices and are not in doubt. Dropping them silently
        // loses money from the session's books, so they are booked here
        // instead of carried onto a position they did not come from
        // (pairtrade#300 Codex review).
        //
        // Any record about to be replaced that this adoption cannot carry
        // forward -- a different side, or a different symbol entirely
        // (reachable when the old symbol is already flat, so the
        // stale-symbol branch above did not fire). Both lose the record
        // to the `sync_open_position_field` below, so what it already
        // realized is booked first (pairtrade#300 Codex review).
        let replaced = self
            .state
            .open_position
            .as_ref()
            .filter(|p| {
                p.symbol != self.cfg.us_primary_symbol || side_from_str(&p.side) != Some(live.side)
            })
            .cloned();
        if let Some(record) = replaced {
            let why = if record.symbol != self.cfg.us_primary_symbol {
                "its symbol is no longer the configured one and the record is being replaced"
            } else {
                "the exchange reports the opposite side, so the record cannot be resumed"
            };
            self.book_orphaned_partial_pnl(&record, why);
        }
        let ws_price = self
            .exit_accounting_price(&self.cfg.us_primary_symbol)
            .map(|(mid, _)| mid);
        let (entry_price, entry_price_estimated, entry_price_unknown) =
            match (live.entry_price, ws_price) {
                (Some(e), _) => (e, false, false),
                // The record's own basis beats a current mid when it
                // describes this same position: it is what the carried
                // partial PnL was booked against.
                (None, ws) if carried_basis.is_some() => {
                    let (price, estimated, tracked_size) = carried_basis.expect("checked");
                    let grown = live.size - tracked_size;
                    if grown <= 1e-12 {
                        // A reduction (or the same size): the basis still
                        // describes every unit that is left.
                        (price, estimated, false)
                    } else if let Some(mid) = ws.filter(|m| *m > 0.0) {
                        let blended = (price * tracked_size + mid * grown) / live.size;
                        log::warn!(
                            "[RECONCILE] the position grew by {grown:.6} while this process was \
                             down and the exchange reports no entry price -- blending the added \
                             quantity in at the current mid {mid:.4}: basis {price:.4} -> \
                             {blended:.4}"
                        );
                        (blended, true, false)
                    } else {
                        log::error!(
                            "[RECONCILE] the position grew by {grown:.6} while this process was \
                             down, with neither an exchange entry price nor a mid to value the \
                             added quantity -- adopting with NO known basis rather than \
                             pricing it at the old entry"
                        );
                        (0.0, true, true)
                    }
                }
                (None, Some(w)) => (w, true, false),
                // No cost basis of any kind. Still adopt -- getting flat
                // matters more than the PnL number -- but say so, rather
                // than storing 0.0 as if it were a price. A quote
                // arriving before the close would otherwise make
                // `on_exit` book `(quote - 0) * size`: the position's
                // entire notional as profit, which then feeds the
                // drawdown halt and the session PnL
                // (pairtrade#300 Codex review).
                (None, None) => (0.0, true, true),
            };
        log::error!(
            "[RECONCILE] adopting {} {} size={:.6} for immediate close -- {reason}",
            live.side,
            self.cfg.us_primary_symbol,
            live.size
        );
        self.position = Some(OpenPosition {
            side: live.side,
            entry_price,
            entry_price_estimated,
            entry_price_unknown,
            size: live.size,
            open_size: live.size,
            realized_partial_pnl: carried_partial_pnl,
            entered_at_us: now_us,
            flatten_asap: true,
            // Flattened on the next tick; there is no window to wait for.
            exit_deadline_us: None,
        });
        // The adopted record and the halt it demands land in one write.
        // Two writes leave a crash window where disk holds a matching
        // `flatten_asap` position with no halt: the next start reads it
        // as a plain `Resume`, flattens it, and then allows entries again
        // without the RISK_ACK this anomaly requires
        // (pairtrade#300 Codex review).
        self.sync_open_position_field();
        self.state_write_pending = true;
        self.halt_session(format!("reconcile_adopted_position: {reason}"));
        if self.state_write_pending {
            self.persist_state();
        }
    }

    /// An order this process sent whose outcome it never established, or
    /// `None` when nothing is in flight. Both an in-flight confirmation
    /// and the sticky `position_unconfirmed` marker mean the same thing
    /// for shutdown: the account may hold something no record describes.
    fn unconfirmed_order_note(&self) -> Option<String> {
        // Every live claim, not the first one. The three are
        // independent: `pending` is this process's memory, and both
        // `position_unconfirmed` and `entry_in_flight` are durable
        // markers that can be set while `pending` is `None` (a restart)
        // or alongside it. Reporting only the first hides the others
        // from the operator the shutdown alert is written for
        // (pairtrade#300 Codex review).
        let mut notes: Vec<String> = Vec::new();
        match self.pending.as_ref() {
            Some(PendingConfirm::Entry {
                side,
                requested,
                after_send_error,
                ..
            }) => notes.push(format!(
                "an entry ({side} size={requested:.6} {}) was sent and its fill was never \
                 confirmed (send_error={after_send_error:?})",
                self.cfg.us_primary_symbol
            )),
            Some(PendingConfirm::Exit { exit_price, .. }) => notes.push(format!(
                "a reduce-only exit at {exit_price:.4} {} was accepted and the account was never \
                 seen flat",
                self.cfg.us_primary_symbol
            )),
            None => {}
        }
        if self.state.position_unconfirmed {
            notes.push(format!(
                "state carries position_unconfirmed for {}",
                self.cfg.us_primary_symbol
            ));
        }
        // The durable marker outlives the `pending` that created it and
        // is the *only* record of a former-symbol order whose first
        // account read came back flat: no tracked position, no persisted
        // record and no unmanaged record exists yet in that state, so
        // without this the shutdown report says nothing is open while an
        // order on a symbol this engine cannot close may still fill
        // (pairtrade#300 Codex review).
        if let Some(sent_for) = self.state.entry_in_flight.as_deref() {
            notes.push(if sent_for == self.cfg.us_primary_symbol {
                format!("state carries entry_in_flight for {sent_for}")
            } else {
                format!(
                    "state carries entry_in_flight for {sent_for}, a symbol us_primary no longer \
                     names ({}) -- this engine cannot close it",
                    self.cfg.us_primary_symbol
                )
            });
        }
        (!notes.is_empty()).then(|| notes.join("; "))
    }

    /// The saved record the shutdown alert should describe as
    /// "unreconciled", or `None`.
    ///
    /// Deliberately only `open_position`. A parked record is a claim too,
    /// but it is already logged and alerted by the aggregate block, which
    /// names *every* parked symbol and describes them for what they are;
    /// routing one through the unreconciled arm as well sent a second,
    /// contradictory alert calling a record that reconciliation
    /// deliberately parked one this process "never confirmed"
    /// (pairtrade#300 Codex review).
    fn unreconciled_saved_record(&self) -> Option<&PersistedPosition> {
        self.state.open_position.as_ref()
    }

    /// The unconfirmed claim that the shutdown match below would *not*
    /// report, because some other exposure claims the arm first.
    ///
    /// `None` when there is nothing in flight, and also when nothing else
    /// is claimed -- the match's last arm reports that case itself and
    /// classifies it as `UnconfirmedOrder`, so reporting it here too would
    /// double the alert (pairtrade#300 Codex review).
    fn coexisting_unconfirmed_note(&self) -> Option<String> {
        let note = self.unconfirmed_order_note()?;
        let also_claimed = self.position.is_some()
            || self.state.open_position.is_some()
            || !self.state.unmanaged_positions.is_empty();
        also_claimed.then_some(note)
    }

    /// SIGTERM / stop policy (bot-strategy#917): this prototype does not
    /// reduce-only-close on shutdown, so say so loudly and leave the
    /// persisted record behind for the next process to resume from --
    /// never a close this process did not actually see.
    fn note_shutdown_signal(&mut self, signal: &str) -> ShutdownReport {
        self.persist_position();
        // Every parked exposure, not the first one: an operator who
        // flattens the single position an alert names would leave the
        // others live and undisclosed (pairtrade#300 Codex review). This
        // is reported alongside whatever the match below decides, since
        // a tracked position does not make these go away.
        if !self.state.unmanaged_positions.is_empty() {
            let listed = self
                .state
                .unmanaged_positions
                .iter()
                .map(|p| format!("{} {} size={:.6}", p.side, p.symbol, p.open_size))
                .collect::<Vec<_>>()
                .join("; ");
            log::error!(
                "[SHUTDOWN] {signal}: {} exposure(s) this engine cannot close are still claimed: \
                 {listed}",
                self.state.unmanaged_positions.len()
            );
            send_notification(
                format!(
                    "Han Bridge SHUTDOWN with {} unmanaged exposure(s) ({signal})",
                    self.state.unmanaged_positions.len()
                ),
                format!(
                    "{listed} -- on symbols this engine no longer trades, so it never sent a \
                     close for them. Flatten each by hand."
                ),
            );
        }
        // Same rule, same reason as the parked exposures above: an
        // unconfirmed order is not made harmless by a tracked position
        // existing beside it. A managed position and a former-symbol
        // claim genuinely coexist -- startup keeps A's flat-read claim
        // while restoring or adopting configured symbol B -- and the
        // match below returns on the first arm that applies, so without
        // this the alert named B and said nothing about the A order that
        // may still fill. Reported here for the arms that return early;
        // the "nothing tracked, nothing saved" arm below still classifies
        // it as `UnconfirmedOrder` (pairtrade#300 Codex review).
        let unconfirmed = self.unconfirmed_order_note();
        if let Some(note) = self.coexisting_unconfirmed_note() {
            log::error!(
                "[SHUTDOWN] {signal}: {note} -- in addition to the exposure(s) reported \
                 alongside; the exchange may hold more than this process tracks"
            );
            send_notification(
                format!("Han Bridge SHUTDOWN with an unconfirmed order ({signal})"),
                format!(
                    "{note}. This is on top of the position(s) reported separately. Verify the \
                     account before restarting."
                ),
            );
        }
        match self.position.as_ref() {
            Some(p) => {
                log::error!(
                    "[SHUTDOWN] {signal}: {} {} size={:.6} is STILL OPEN and is NOT being closed \
                     here; it is persisted and the next start resumes it (flatten_asap={})",
                    p.side,
                    self.cfg.us_primary_symbol,
                    p.open_size,
                    p.flatten_asap
                );
                send_notification(
                    format!("Han Bridge SHUTDOWN with an open position ({signal})"),
                    format!(
                        "{} {} size={:.6} left open; no reduce-only was sent. Restart resumes it, \
                         or flatten manually.",
                        p.side, self.cfg.us_primary_symbol, p.open_size
                    ),
                );
                ShutdownReport::TrackedPosition
            }
            // Nothing tracked in memory is not the same as nothing open:
            // before reconciliation succeeds (or after it refused to
            // manage a foreign-symbol record), the persisted claim is the
            // only thing that knows about an exposure. Reporting "no open
            // position" there would be falsely reassuring
            // (pairtrade#300 Codex review).
            // A parked exposure is a claim too, and it is still reported
            // as one -- but by the aggregate block at the top, which names
            // *every* parked symbol and describes them accurately. This
            // arm is only for `open_position`: routing a parked record
            // through it sent a second, contradictory alert calling a
            // record that reconciliation deliberately parked one this
            // process "never confirmed" (pairtrade#300 Codex review).
            None => match self.unreconciled_saved_record() {
                Some(p) => {
                    log::error!(
                        "[SHUTDOWN] {signal}: nothing is tracked in memory, but the saved record \
                         still claims an open {} {} size={:.6} from session {} that this process \
                         never reconciled -- check the exchange",
                        p.side,
                        p.symbol,
                        p.open_size,
                        p.session_date
                    );
                    send_notification(
                        format!("Han Bridge SHUTDOWN with an unreconciled position ({signal})"),
                        format!(
                            "saved record: {} {} size={:.6} session={} -- this process never \
                             confirmed it against the exchange. Verify the account.",
                            p.side, p.symbol, p.open_size, p.session_date
                        ),
                    );
                    ShutdownReport::UnreconciledClaim
                }
                // Parked exposures with nothing else claimed: already
                // logged and alerted above, so the classification is
                // returned without a second notification.
                None if !self.state.unmanaged_positions.is_empty() => {
                    ShutdownReport::UnreconciledClaim
                }
                // An accepted order whose fill was never confirmed leaves
                // both of the above empty while the exchange may well hold
                // the position: `pending` is the only thing that knows an
                // order went out. Reporting "nothing open" there sends an
                // operator away from a live exposure (pairtrade#300 Codex
                // review).
                None => match unconfirmed {
                    Some(note) => {
                        log::error!(
                            "[SHUTDOWN] {signal}: nothing is tracked and nothing is saved, but \
                             {note} -- the exchange may hold a position this process never \
                             recorded; check the account"
                        );
                        send_notification(
                            format!("Han Bridge SHUTDOWN with an unconfirmed order ({signal})"),
                            format!(
                                "{note}. No position is tracked or saved, so the next start has \
                                 nothing to resume. Verify the account before restarting."
                            ),
                        );
                        ShutdownReport::UnconfirmedOrder
                    }
                    None => {
                        log::warn!("[SHUTDOWN] {signal}: no open position tracked; exiting");
                        ShutdownReport::Nothing
                    }
                },
            },
        }
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
            // Carry the flag into the persisted record too, so a restart
            // during the carry-over also flattens at once instead of
            // waiting for a window that belongs to the previous session
            // (bot-strategy#917).
            self.persist_position();
            self.day.entered = true;
            self.day.skip_reason = Some("carried_over_position".to_string());
            self.state.last_session_date = Some(today.to_string());
            self.state.last_session_skip_reason = self.day.skip_reason.clone();
            self.persist_state();
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
            // Restore *why* the day was settled, not only that it was
            // (pairtrade#289 Codex round 3). Deliberately after the
            // carry-over branch above, which sets its own reason.
            if self.day.skip_reason.is_none() {
                self.day.skip_reason = self.state.last_session_skip_reason.clone();
            }
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

    /// Mid prices safe to base an *entry* decision on right now: accepted
    /// at ingest, observed on the current feed generation, and no older
    /// than `max_price_staleness_secs` (bot-strategy#916). Anything else
    /// is dropped rather than returned stale, so every downstream
    /// consumer -- t0/t1 capture, `compute_epsilon`'s inputs, the
    /// order-sizing price -- fails closed by simply not finding the
    /// symbol.
    ///
    /// Deliberately **not** used by the exit path or by
    /// `try_adopt_unconfirmed`: an open position must stay closable (and
    /// an unknown one must stay adoptable) even while the feed is stale.
    /// The exit is sized from the exchange's own position, not from this
    /// price -- the price only books PnL. Fail-closed on entry must never
    /// mean fail-closed on getting flat.
    fn usable_prices(&self, now_us: i64) -> HashMap<String, f64> {
        self.usable_prices_with_generation(now_us).0
    }

    /// `usable_prices` plus the generation those prices were read on,
    /// under ONE lock acquisition (pairtrade#289 Codex round 6). Taking
    /// the generation and the prices separately is a race on the
    /// multi-threaded runtime: the feed task can process a `Lagged` and
    /// then a fresh US quote in between, so the generation compares equal
    /// to the t1 capture while the price map already holds a post-lag
    /// observation. Any decision that compares a generation against the
    /// prices it is about to act on must use this.
    fn usable_prices_with_generation(&self, now_us: i64) -> (HashMap<String, f64>, u64) {
        let feed = self.feed.lock().expect("price feed mutex");
        (
            feed.usable(now_us, self.cfg.max_price_staleness_secs),
            feed.generation,
        )
    }

    /// Symbols whose latest observation is missing or not usable for an
    /// entry decision right now. Drives both the human-readable log line
    /// and `status.json`, so the two can never disagree.
    fn stale_or_missing_symbols(&self, now_us: i64) -> Vec<String> {
        let usable = self.usable_prices(now_us);
        self.cfg
            .all_symbols()
            .into_iter()
            .filter(|symbol| !usable.contains_key(symbol))
            .collect()
    }

    /// One-line, per-symbol account of what the engine was actually
    /// looking at when it made (or refused) a decision: price, age, feed
    /// generation, staleness verdict (bot-strategy#916 -- "healthyな
    /// DRY_RUNでdecisionに使った価格・時刻・鮮度・skip理由を確認できる").
    fn freshness_debug(&self, now_us: i64) -> String {
        // One lock for the whole line, so every symbol is described
        // against the same generation.
        let feed = self.feed.lock().expect("price feed mutex");
        let generation = feed.generation;
        let parts: Vec<String> = self
            .cfg
            .all_symbols()
            .into_iter()
            .map(|symbol| match feed.latest.get(&symbol) {
                None => format!("{symbol}=never_observed"),
                Some(obs) => {
                    let verdict = if obs.generation != generation {
                        "PRE_LAG"
                    } else if obs.received_at_us > now_us {
                        "FUTURE_DATED"
                    } else if obs.effective_age_secs(now_us)
                        > self.cfg.max_price_staleness_secs as f64
                    {
                        "STALE"
                    } else {
                        "ok"
                    };
                    format!(
                        "{symbol}={:.4}@{:.1}s/gen{}[{verdict}]",
                        obs.mid,
                        obs.effective_age_secs(now_us),
                        obs.generation
                    )
                }
            })
            .collect();
        format!(
            "gen={generation} staleness_bound={}s {}",
            self.cfg.max_price_staleness_secs,
            parts.join(" ")
        )
    }

    /// Settle today as "no entry", with a reason, exactly once
    /// (bot-strategy#916). Marks the day acted-on and persists
    /// `last_session_date` the same way the entry-deadline path always
    /// did, so a restart does not re-evaluate a day already decided.
    ///
    /// Touches entry state only: `position`/`pending` and the exit path
    /// are untouched, so skipping an entry can never strand an open
    /// position.
    fn skip_day(&mut self, reason: String) {
        if self.day.skip_reason.is_some() {
            return;
        }
        log::warn!("[SKIP] {reason}");
        self.day.skip_reason = Some(reason.clone());
        self.day.entered = true;
        self.mark_day_acted(Some(reason));
    }

    /// Record that `current_date` has been acted on -- entered, or
    /// settled as a no-entry carrying `skip_reason` -- and persist both
    /// facts in one write. Single writer for `last_session_date` so the
    /// reason can never drift from the day marker it describes
    /// (pairtrade#289 Codex round 3).
    fn mark_day_acted(&mut self, skip_reason: Option<String>) {
        self.state.last_session_date = self.current_date.map(|d| d.to_string());
        self.state.last_session_skip_reason = skip_reason;
        self.persist_state();
    }

    /// The price bound one send carries, from the configured mid-relative
    /// `slippage_bps` and the book this process observed itself
    /// (bot-strategy#918, #978). Computed before the DRY_RUN branch so a
    /// DRY_RUN session exercises the same refusal the live one would.
    ///
    /// The quote is read from this process's own feed, not the
    /// connector's cache: this is the side that knows how fresh its
    /// observation is, and an absolute limit derived from it is not
    /// re-anchored on whatever the venue's touch has become by submit
    /// time -- which is the window #918 had to leave open.
    fn send_limit(&self, side: OrderSide, reduce_only: bool) -> Result<SendLimit> {
        // A *fresh* clock, not the tick's start time: the eligibility
        // fetch, the position read and the awaits before this can take
        // seconds, and the question here is whether the observation is
        // still good at the moment of the send.
        let now_us = self.now();
        // Usability is not optional on this path, and it is the whole of
        // pairtrade#315's P1. `maybe_exit` deliberately closes on prices
        // too stale to *enter* on -- but an absolute limit derived from a
        // stale mid does not re-anchor the way the old percentage did: if
        // the market has left it behind, every reduce-only IOC comes back
        // unmarketable and the next tick reuses the same dead quote,
        // because a stopped feed keeps handing out the same observation.
        // So an observation that is stale, future-dated or from an older
        // feed generation is treated as no book, which routes the exit to
        // the connector's own live touch (`Unchecked`).
        //
        // Generation is read under the same lock as the observation
        // (pairtrade#289 Codex round 6): taking them separately races the
        // feed task.
        let observed = {
            let feed = self.feed.lock().expect("price feed mutex");
            let generation = feed.generation;
            feed.latest
                .get(&self.cfg.us_primary_symbol)
                .filter(|o| o.is_usable(now_us, generation, self.cfg.max_price_staleness_secs))
                .map(|o| (o.mid, o.best_bid, o.best_ask))
        };
        // Nothing usable: `mid` is absent alongside the touch, and the
        // helper reads that as the no-usable-book case. An entry cannot
        // normally reach it (`maybe_enter` requires fresh prices for the
        // traded symbol before it sizes anything), and if the quote went
        // stale between that sizing and this send, refusing is the point.
        let (mid, touch) = match observed {
            Some((mid, bid, ask)) => (mid, Some((bid, ask))),
            None => (f64::NAN, None),
        };
        let limit = send_limit_price(self.cfg.slippage_bps, mid, touch, side, reduce_only)
            .map_err(|reason| anyhow::anyhow!("{}: {reason}", self.cfg.us_primary_symbol))?;
        match limit {
            SendLimit::AtTouch => {
                // Crossing at the touch is the true cost of immediacy on
                // a torn book, and it is still a bound: the best offer,
                // not the +/-20% the venue would have allowed. Holding an
                // unclosed position through a tear is the worse outcome,
                // which is why the connector -- not this snapshot --
                // prices it.
                log::warn!(
                    "[EXIT] {} the {}bps bound from mid={mid} does not reach the touch \
                     ({touch:?}); crossing at the venue's own touch to get flat",
                    self.cfg.us_primary_symbol,
                    self.cfg.slippage_bps
                );
            }
            SendLimit::Unchecked => {
                log::warn!(
                    "[EXIT] no usable book for {} ({}); sending the configured {}bps against \
                     the connector's own live touch instead of an absolute limit",
                    self.cfg.us_primary_symbol,
                    self.freshness_debug(now_us),
                    self.cfg.slippage_bps
                );
            }
            SendLimit::Bounded(_) => {}
        }
        Ok(limit)
    }

    async fn submit_order(&self, side: OrderSide, size: f64, reduce_only: bool) -> Result<Decimal> {
        let size_dec = Decimal::from_str(&format!("{size:.8}")).context("size to Decimal")?;
        let bound = self.send_limit(side, reduce_only)?;
        let described = match bound {
            SendLimit::Bounded(p) => format!(
                "as a taker IOC limited at {p} ({}bps from the mid)",
                self.cfg.slippage_bps
            ),
            SendLimit::AtTouch => format!(
                "as a taker IOC bounded {AT_TOUCH_BPS}bps from the connector's own touch \
                 (the {}bps mid bound does not reach it)",
                self.cfg.slippage_bps
            ),
            SendLimit::Unchecked => format!(
                "as a taker IOC bounded {}bps from the connector's own touch",
                self.cfg.slippage_bps
            ),
        };
        if self.cfg.dry_run {
            log::info!(
                "[DRY_RUN] would submit {side} size={size_dec} reduce_only={reduce_only} symbol={} {described}",
                self.cfg.us_primary_symbol,
            );
            return Ok(size_dec);
        }
        let resp = match bound {
            SendLimit::Bounded(price) => {
                // `from_f64`, not `from_f64_retain`: the latter hands over
                // the f64's full binary expansion, and the connector's
                // inward tick rounding then drops a whole tick off any
                // value sitting an ULP under a tick boundary
                // (`1700.0999999999999090505298222` -> `1700.0` at one
                // price decimal), turning a marketable limit into a
                // resting one (pairtrade#315 Codex round 2).
                let limit = Decimal::from_f64(price).with_context(|| {
                    format!("limit price {price} is not representable as Decimal")
                })?;
                self.connector
                    .create_order_taker_ioc_at(
                        &self.cfg.us_primary_symbol,
                        size_dec,
                        side,
                        limit,
                        reduce_only,
                    )
                    .await
                    .context("create_order_taker_ioc_at failed")?
            }
            // Both remaining branches are percentage sends against the
            // connector's own live touch: `AtTouch` at its minimum
            // allowance because the mid bound does not reach the book,
            // `Unchecked` at the configured one because there is no book
            // here to price against at all.
            SendLimit::AtTouch | SendLimit::Unchecked => {
                let bps = if matches!(bound, SendLimit::AtTouch) {
                    AT_TOUCH_BPS
                } else {
                    self.cfg.slippage_bps
                };
                self.connector
                    .create_order_taker_ioc(
                        &self.cfg.us_primary_symbol,
                        size_dec,
                        side,
                        bps,
                        reduce_only,
                    )
                    .await
                    .context("create_order_taker_ioc failed")?
            }
        };
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
        if self.day.t0_prices.is_some() {
            return;
        }
        // A day that is already settled has no use for a t0 -- `maybe_enter`
        // returns on `day.entered` before it ever reads one -- and so must
        // not be *abandoned* for lacking one. Without this, restarting after
        // a completed cycle recorded a spurious `no_usable_t0` over a day
        // that had actually entered and exited: `roll_day_if_needed`
        // restores `entered` from `last_session_date`, the persisted t0 is
        // hours stale (or, after a `kr_primary` change, for the wrong
        // symbol, so `recoverable_t0_prices` declines it), and the grace
        // window below is long past. Observed live 2026-09-08 13:33 UTC
        // after the +$0.69 cycle -- `last_session_skip_reason` and the
        // dashboard then reported a traded day as skipped
        // (bot-strategy#965).
        //
        // Deliberately here rather than in the grace branch: capturing a t0
        // for a settled day would be equally pointless, and persisting one
        // would only invite a later restart to trust it.
        if self.day.entered {
            return;
        }
        let Some((t0, _, _)) = self.window else {
            return;
        };
        if now_us < t0 {
            return;
        }
        let delay_secs = (now_us - t0) as f64 / 1_000_000.0;
        // Only *usable* observations (bot-strategy#916): a stale or
        // pre-lag price is not evidence of the KRX-open level just
        // because the key is present in the map.
        let prices = self.usable_prices(now_us);
        let complete = t0_snapshot_has_required_symbols(
            &prices,
            &self.cfg.kr_primary_symbol,
            &self.cfg.us_primary_symbol,
        );
        // The grace bound applies to ANY capture, complete or not
        // (pairtrade#289 Codex round 3). A feed that recovers at
        // t0 + grace + 1 s hands us a perfectly complete snapshot -- of
        // mid-session prices, which is exactly what this bound exists to
        // refuse. Checking it only on the incomplete path let that late
        // recovery through and persisted those prices as the KRX open.
        if delay_secs > self.cfg.t0_capture_grace_secs as f64 {
            let detail = self.freshness_debug(now_us);
            let reason = if complete {
                format!(
                    "late_t0: fresh {}/{} prices only became usable {delay_secs:.0}s after t0 \
                     (grace {}s); refusing to label a mid-session price as the KRX open -- {detail}",
                    self.cfg.kr_primary_symbol,
                    self.cfg.us_primary_symbol,
                    self.cfg.t0_capture_grace_secs
                )
            } else {
                format!(
                    "no_usable_t0: no fresh {}/{} price within {}s of t0 -- {detail}",
                    self.cfg.kr_primary_symbol,
                    self.cfg.us_primary_symbol,
                    self.cfg.t0_capture_grace_secs
                )
            };
            self.skip_day(reason);
            return;
        }
        if !complete {
            // Gated on completeness, not `delay_secs`: a snapshot missing
            // kr_primary/us_primary is unusable to compute_epsilon
            // regardless of how soon after t0 it was taken (WS delivery
            // order across symbols is not guaranteed -- control_symbols
            // can easily arrive before the two primaries right after a
            // (re)connect).
            //
            // Nothing partial is captured or persisted: the next tick
            // tries again with whatever has arrived since, and
            // `t0_capture_grace_secs` bounds how late that may still
            // count as a t0. Past that bound the day is abandoned rather
            // than backfilled with a mid-session price wearing a
            // KRX-open label (bot-strategy#916; this replaces the old
            // "capture whatever is there, WARN if it is incomplete or
            // >300 s late" behaviour, which left the day running on a
            // snapshot the WARN itself called suspect). The grace bound
            // is enforced above, for complete and partial alike.
            return;
        }
        log::info!(
            "[SIGNAL_INPUTS] t0 snapshot captured ({delay_secs:.0}s after t0) -- {}",
            self.freshness_debug(now_us)
        );
        capture_t0_if_due(self.window, &mut self.day, now_us, &prices);
        if let Some(today) = self.current_date {
            self.state.t0_snapshot_date = Some(today.to_string());
            self.state.t0_prices = prices;
            self.persist_state();
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
        // Raw `latest_price`, not `usable_prices` (bot-strategy#916):
        // adopting an exposure the exchange already reports is a
        // get-flat path, and refusing to adopt on a stale feed would
        // leave a real position untracked -- the opposite of safe. A
        // stale mid only makes the *cost basis* an estimate, which
        // `entry_price_estimated` already records.
        let ws_price = self
            .exit_accounting_price(&self.cfg.us_primary_symbol)
            .map(|(mid, _)| mid);
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
            entry_price_unknown: false,
            size: live.size,
            open_size: live.size,
            realized_partial_pnl: 0.0,
            entered_at_us: now_us,
            flatten_asap: true,
            exit_deadline_us: None,
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
                price_is_market,
                deadline_us,
                saw_reading,
            } => {
                let expired = now_us >= deadline_us;
                match reading {
                    Ok(positions) => {
                        match exchange_position_for(&positions, &self.cfg.us_primary_symbol) {
                            None => {
                                self.pending = None;
                                self.on_exit(exit_price, now_us).await;
                            }
                            Some(remaining) => {
                                // A side flip while the exit is pending is
                                // the same anomaly as in maybe_exit: halt and
                                // reconcile, whether or not the window ended
                                // (pairtrade#275 Codex review).
                                // `Some` only when the exit was priced
                                // off the feed: `maybe_exit`'s
                                // no-price-of-any-kind fallback is the
                                // tracked leg's own entry, and installing
                                // *that* as the replacement leg's basis
                                // prices the new leg against the one it
                                // replaced (pairtrade#300 Codex review).
                                self.reconcile_side_flip_if_any(
                                    &remaining,
                                    price_is_market.then_some(exit_price),
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
                                        price_is_market,
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
                entry_price_unknown: false,
                size: filled.size,
                open_size: filled.size,
                realized_partial_pnl: 0.0,
                entered_at_us: now_us,
                flatten_asap: false,
                exit_deadline_us: self.scheduled_exit_deadline_us(),
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
            // Same guard as `on_exit`, and it has to be here too: a
            // placeholder basis run through this arithmetic fabricates
            // roughly the closed notional as *realized* PnL, which
            // `on_exit` then books as already-realized money
            // (pairtrade#300 Codex review).
            let pnl = if p.entry_price_unknown {
                log::error!(
                    "[EXIT] partial close ({context}): closed={closed:.6} at {price:.4} with NO \
                     known cost basis -- booking $0.00 for it; this reduction's PnL is not \
                     measurable from what this process saw"
                );
                0.0
            } else {
                sign * (price - p.entry_price) * closed
            };
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
            match live.entry_price {
                Some(e) => {
                    // The exchange's average covers the whole position,
                    // so this is a real basis even for a position that
                    // had none.
                    p.entry_price = e;
                    p.entry_price_estimated = false;
                    p.entry_price_unknown = false;
                }
                None => {
                    // No exchange basis: assume the added quantity was
                    // bought near the current mid and blend, rather than
                    // valuing it at the old basis (pairtrade#275 Codex
                    // review). Nothing to blend when the old basis is a
                    // placeholder -- averaging a real price with 0.0 is
                    // not a better guess than admitting the position has
                    // no basis (pairtrade#300 Codex review).
                    if !p.entry_price_unknown && new_open_size > 0.0 && price > 0.0 {
                        p.entry_price =
                            (p.entry_price * p.open_size + price * grown) / new_open_size;
                    }
                    p.entry_price_estimated = true;
                }
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
        // `None` when no market price exists. It used to be a bare
        // `f64`, and `maybe_exit` passes the tracked position's own
        // entry price when the feed has nothing -- installing *that* as
        // the replacement leg's basis, and clearing the unknown flag,
        // prices the new leg against the old one's entry
        // (pairtrade#300 Codex review).
        market_price: Option<f64>,
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
            // Third place this arithmetic lives, and the same rule
            // applies: a placeholder basis measures nothing, and
            // `(ws_price - 0.0) * size` would book the flipped-away
            // leg's whole notional as realized (pairtrade#300 Codex
            // review).
            let old_leg_pnl = match market_price.filter(|m| *m > 0.0) {
                _ if p.entry_price_unknown => {
                    log::error!(
                        "[EXIT] the flipped-away {} leg had NO known cost basis -- booking \
                         $0.00 for it; its PnL is not measurable from what this process saw",
                        p.side
                    );
                    0.0
                }
                Some(mid) => old_sign * (mid - p.entry_price) * p.open_size,
                None => {
                    log::error!(
                        "[EXIT] the flipped-away {} leg cannot be valued: no market price of \
                         any kind -- booking $0.00 for it",
                        p.side
                    );
                    0.0
                }
            };
            p.realized_partial_pnl += old_leg_pnl;
            log::error!(
                "[EXIT] booked the flipped-away {} leg: size={:.6} entry={:.4} at mid {:?} \
                 pnl=${old_leg_pnl:.2}; realized_so_far=${:.2}",
                p.side,
                p.open_size,
                p.entry_price,
                market_price,
                p.realized_partial_pnl
            );
            p.side = live.side;
            p.size = live.size;
            p.open_size = live.size;
            // The replacement leg has a basis of its own, so the flag
            // must not survive onto it -- leaving it set would suppress
            // the *new* leg's PnL at exit. But only the venue's own
            // price, or an actual market observation, may become that
            // basis: with neither, the leg is adopted as unknown rather
            // than valued at the leg it replaced.
            let market = market_price.filter(|m| *m > 0.0);
            p.entry_price_estimated = live.entry_price.is_none();
            p.entry_price = live.entry_price.or(market).unwrap_or(0.0);
            p.entry_price_unknown = live.entry_price.is_none() && market.is_none();
        }
        // Durable before the caller does anything else. The old leg's
        // PnL and the replacement leg exist only in `self.position` at
        // this point; `halt_session` would otherwise write the *stale*
        // record, and `maybe_exit` goes straight on to submit the
        // replacement leg's close. A crash -- or an accepted order whose
        // outcome is ambiguous -- in that window restores the pre-flip
        // record and the booked leg is gone for good
        // (pairtrade#300 Codex review).
        self.sync_open_position_field();
        self.state_write_pending = true;
        self.halt_session(reason);
        if self.state_write_pending {
            self.persist_state();
        }
    }

    /// Re-derive equity, the peak and the drawdown from
    /// `realized_pnl_session`, engaging the session-loss halt if the
    /// threshold is crossed.
    ///
    /// Every path that changes realized PnL runs this, not just
    /// `on_exit`: booking an orphaned record's realized reductions moves
    /// the same number the halt is measured against, and skipping it
    /// left a loss able to pass the threshold unnoticed -- and a gain
    /// unable to advance the peak, which understates every later
    /// drawdown (pairtrade#300 Codex review).
    fn reprice_equity_and_drawdown(&mut self) {
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
            // Its own alert. `halt_session` returns early once the halt
            // is set, so a reconciliation anomaly whose booked PnL
            // crossed this threshold would otherwise be the *only* kind
            // that produces no push alert -- exactly the loudest case
            // (pairtrade#300 Codex review).
            send_notification(
                format!("Han Bridge SESSION DD HALT {}", self.cfg.instance_id),
                format!(
                    "drawdown {dd_bps:.0}bps >= {:.0}bps. New entries blocked until RISK_ACK at {}",
                    self.cfg.max_session_loss_bps,
                    self.cfg.risk_ack_path.display()
                ),
            );
        }
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
        self.persist_state();
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
        // After the position is installed, so the ledger keys off the
        // side the exchange confirmed rather than the one submitted.
        self.align_fill_ledger_with_position();
        // Durable before anything else: from here on a restart resumes
        // this position for its scheduled exit (bot-strategy#917).
        self.persist_position();
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
        self.mark_day_acted(None);
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
        self.mark_day_acted(None);
    }

    async fn maybe_enter(&mut self, now_us: i64) {
        let Some((_t0, t1, _t2)) = self.window else {
            return;
        };
        if self.day.entered || now_us < t1 {
            return;
        }
        // A position carried over from a previous session (its exit kept
        // failing past exit_deadline, so the day rolled with it still
        // open). Never open a second one on top of it and never re-label
        // it as today's entry; the exit path keeps trying at today's t2
        // (pairtrade#275 review finding 1). Copied out of `self.position`
        // before `skip_day` so the shared borrow ends first.
        if let Some((side, size, held_secs)) = self
            .position
            .as_ref()
            .map(|pos| (pos.side, pos.size, (now_us - pos.entered_at_us) / 1_000_000))
        {
            log::warn!(
                "[ENTRY] position from a previous session still open ({side} size={size:.6}, \
                 entered {held_secs}s ago); no new entry today, exit path continues"
            );
            self.skip_day(format!(
                "carried_over_position: {side} size={size:.6} open for {held_secs}s"
            ));
            return;
        }
        if now_us > t1 + self.cfg.entry_deadline_secs * 1_000_000 {
            let detail = self.freshness_debug(now_us);
            self.skip_day(format!(
                "entry_deadline: {}s passed after t1 without a valid signal -- t0={} t1={} \
                 eligibility_confirmed={} -- {detail}",
                self.cfg.entry_deadline_secs,
                if self.day.t0_prices.is_some() {
                    "captured"
                } else {
                    "MISSING"
                },
                if self.day.t1_prices.is_some() {
                    "captured"
                } else {
                    "MISSING"
                },
                self.day.eligibility_confirmed
            ));
            return;
        }
        // A t1 captured before a feed lag is not a t1 any more: the KR
        // leg feeding `compute_epsilon` may be arbitrarily behind even
        // once the US leg has re-reported, and `t1_prices` is a bare
        // map with no per-symbol metadata left to check (pairtrade#289
        // Codex round 5). Discard the whole capture and take it again.
        let (usable_now, generation_now) = self.usable_prices_with_generation(now_us);
        if self.day.t1_generation.is_some_and(|g| g != generation_now) {
            log::warn!(
                "[ENTRY] discarding the t1 snapshot captured on feed generation {:?} (now \
                 {generation_now}); recapturing both legs before any signal is computed",
                self.day.t1_generation
            );
            self.day.t1_prices = None;
            self.day.t1_generation = None;
        }
        if self.day.t1_prices.is_none() {
            // Same bar as t0 (bot-strategy#916): capture only usable
            // observations, and only once both primaries are present.
            // A partial t1 would silently feed compute_epsilon a
            // control-symbol-only map; a stale one would compare the
            // KRX close against a price from before the feed stalled.
            // Returning here retries on the next 5 s tick, and the
            // entry-deadline branch above is what eventually turns a
            // feed that never recovers into an explicit skip.
            let prices = usable_now;
            if !t0_snapshot_has_required_symbols(
                &prices,
                &self.cfg.kr_primary_symbol,
                &self.cfg.us_primary_symbol,
            ) {
                log::warn!(
                    "[ENTRY] t1 reached but no fresh {}/{} price yet; retrying within the entry \
                     deadline -- {}",
                    self.cfg.kr_primary_symbol,
                    self.cfg.us_primary_symbol,
                    self.freshness_debug(now_us)
                );
                return;
            }
            log::info!(
                "[SIGNAL_INPUTS] t1 snapshot captured on feed generation {generation_now} -- {}",
                self.freshness_debug(now_us)
            );
            self.day.t1_prices = Some(prices);
            self.day.t1_generation = Some(generation_now);
        }
        let (Some(t0_prices), Some(t1_prices)) = (&self.day.t0_prices, &self.day.t1_prices) else {
            // t0 never captured. `maybe_capture_t0` keeps trying until
            // `t0_capture_grace_secs` and then ends the day itself, so
            // this only logs while that grace window is still open.
            log::warn!(
                "[ENTRY] t1 reached but t0 snapshot missing (process started after t0?); no entry \
                 until a t0 exists or the day is abandoned"
            );
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
            self.skip_day(format!(
                "below_threshold: |epsilon|={:.5} < threshold={:.5}",
                epsilon.abs(),
                self.cfg.epsilon_threshold
            ));
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
                    // Fail closed (bot-strategy#916): an unreadable
                    // eligibility endpoint means the gate's answer is
                    // unknown, and "unknown" must never be spent as
                    // "eligible" -- this branch used to fall through to
                    // the entry, so a REST failure would have traded
                    // straight through a force_reduce_only market. Retry
                    // on the next 5 s tick, bounded both by
                    // `entry_deadline_secs` (the window) and by
                    // `max_eligibility_attempts` (the request count, so a
                    // hard-down endpoint is not hammered 36 times against
                    // a 60 req/min account).
                    self.day.eligibility_attempts += 1;
                    if self.day.eligibility_attempts >= self.cfg.max_eligibility_attempts {
                        self.skip_day(format!(
                            "eligibility_unavailable: {} orderBookDetails attempts failed, last \
                             error {e:?}",
                            self.day.eligibility_attempts
                        ));
                    } else {
                        log::warn!(
                            "[ELIGIBILITY] orderBookDetails fetch failed (attempt {}/{}): {e:?}; \
                             no entry until a response parses (fail-closed)",
                            self.day.eligibility_attempts,
                            self.cfg.max_eligibility_attempts
                        );
                    }
                    return;
                }
            }
        }
        if !self.day.eligibility_confirmed {
            // Unreachable in practice (the Ok arm sets it, the Err arm
            // returns) but stated explicitly so a future edit to the
            // match above cannot reintroduce the fail-open path by
            // accident -- entry requires a *confirmed* answer, never a
            // merely absent negative (bot-strategy#916).
            log::warn!("[ENTRY] eligibility not confirmed for today; no entry");
            return;
        }
        if !self.day.ineligible_reasons.is_empty() {
            self.skip_day(format!(
                "ineligible_symbol: {}",
                self.day.ineligible_reasons.join("; ")
            ));
            return;
        }
        if !self.entries_allowed() {
            log::warn!(
                "[ENTRY] signal fired but entries blocked (kill_switch={}, session_halted={}, \
                 reconciled={}, entry_in_flight={:?}, unmanaged={:?}, saved_symbol={:?})",
                self.kill_switch_engaged(),
                self.state.session_halted,
                self.reconciled,
                self.state.entry_in_flight,
                self.state
                    .unmanaged_positions
                    .iter()
                    .map(|p| p.symbol.as_str())
                    .collect::<Vec<_>>(),
                self.state.open_position.as_ref().map(|p| p.symbol.as_str())
            );
            return;
        }
        let predicted_direction = epsilon.signum() * self.cfg.direction_multiplier.signum();
        let side = if predicted_direction >= 0.0 {
            OrderSide::Long
        } else {
            OrderSide::Short
        };
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
                        // Adoption is a get-flat path: never blocked by
                        // the entry freshness gates, so the cost basis
                        // falls back to whatever mid exists (see
                        // `exit_accounting_price`) and is flagged.
                        let (entry_price, entry_price_estimated) = match (
                            existing.entry_price,
                            self.exit_accounting_price(&self.cfg.us_primary_symbol),
                        ) {
                            (Some(e), _) => (e, false),
                            (None, Some((mid, _))) => (mid, true),
                            (None, None) => {
                                log::warn!(
                                    "[ENTRY] exchange holds {} {} size={:.6} but no entry price and \
                                     no mid of any kind yet -- deferring adoption to the next tick",
                                    existing.side,
                                    self.cfg.us_primary_symbol,
                                    existing.size
                                );
                                return;
                            }
                        };
                        self.record_entry(
                            OpenPosition {
                                side: existing.side,
                                entry_price,
                                entry_price_estimated,
                                entry_price_unknown: false,
                                size: existing.size,
                                open_size: existing.size,
                                realized_partial_pnl: 0.0,
                                entered_at_us: now_us,
                                flatten_asap: true,
                                exit_deadline_us: None,
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
        // Sized immediately before the send, after every await above
        // (eligibility fetch, position read, set_leverage), and against a
        // *fresh* clock rather than the tick's start time `now_us`: those
        // awaits can take seconds, so a quote near the staleness bound at
        // tick start can be past it by now (bot-strategy#916 "送信直前に
        // 鮮度を検査"; pairtrade#289 Codex review). Nothing below this
        // point awaits anything before `submit_order`.
        let send_now_us = self.now();
        // The signal is only as good as the generation it was computed
        // on. Checking the US price alone is not enough: one US update
        // arriving on the new generation during the awaits above would
        // satisfy `usable_prices` while epsilon still rests on a KR value
        // captured before the drop (pairtrade#289 Codex round 5).
        // One lock for both, so the generation cannot advance between
        // the check and the price it authorises (Codex round 6).
        let (usable_at_send, send_generation) = self.usable_prices_with_generation(send_now_us);
        // The signal is only sendable while *every* leg it rests on is
        // still live. Two ways that can stop being true while the awaits
        // above run, and both must invalidate the whole capture:
        //   - the feed dropped updates (generation moved), or
        //   - a leg simply stopped reporting and aged past the staleness
        //     bound with no lag at all (Codex round 7) -- the KR leg can
        //     stall while the US leg keeps ticking, and checking only the
        //     traded symbol would send an epsilon whose KR half is no
        //     longer backed by a live observation.
        // `t1_prices` is a bare map with no per-symbol metadata left, so
        // the check has to be "are the underlying observations still
        // usable", not "how old is the map".
        let signal_legs_live = t0_snapshot_has_required_symbols(
            &usable_at_send,
            &self.cfg.kr_primary_symbol,
            &self.cfg.us_primary_symbol,
        );
        if self.day.t1_generation != Some(send_generation) || !signal_legs_live {
            log::warn!(
                "[ENTRY] today's signal is no longer backed by live observations (t1 generation \
                 {:?}, now {send_generation}; legs_live={signal_legs_live}); not sending -- t1 is \
                 recaptured and epsilon recomputed on the next tick -- {}",
                self.day.t1_generation,
                self.freshness_debug(send_now_us)
            );
            self.day.t1_prices = None;
            self.day.t1_generation = None;
            return;
        }
        let Some(price) = usable_at_send.get(&self.cfg.us_primary_symbol).copied() else {
            log::error!(
                "[ENTRY] no fresh price for {} at send time; not sending this tick -- {}",
                self.cfg.us_primary_symbol,
                self.freshness_debug(send_now_us)
            );
            return;
        };
        let size = notional_usd / price;
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
        self.state.last_session_skip_reason = None;
        // Same write, same reason: from here on, a restart has to know an
        // order may exist even though nothing is persisted about a
        // position yet (pairtrade#300 Codex review).
        self.state.entry_in_flight = Some(self.cfg.us_primary_symbol.clone());
        self.entry_marker_owned = true;
        if let Err(e) = atomic_write_json_checked(&self.cfg.state_path, &self.state) {
            // No durable marker, no order: sending now would make the
            // at-most-one guarantee depend on this process surviving.
            // Nothing was sent, so the next tick may simply try again.
            log::error!(
                "[ENTRY] cannot persist the entry-attempt marker to {} ({e}); NOT sending this tick",
                self.cfg.state_path.display()
            );
            self.state.entry_in_flight = None;
            self.entry_marker_owned = false;
            return;
        }
        let submit = self.submit_order(side, size, false).await;
        // Fresh clock: `now_us` is the tick's start time and the
        // eligibility fetch / position read / sendTx above can take
        // seconds, so a deadline based on it could already be expired
        // when `pending` is installed (pairtrade#275 Codex review).
        let sent_at_us = self.now(); // the `now_us` parameter shadows the fn
        if self.cfg.dry_run {
            match submit {
                Ok(requested) => self.record_entry(
                    OpenPosition {
                        side,
                        entry_price: price,
                        entry_price_estimated: false,
                        entry_price_unknown: false,
                        size: requested.to_f64().unwrap_or(size),
                        open_size: requested.to_f64().unwrap_or(size),
                        realized_partial_pnl: 0.0,
                        entered_at_us: sent_at_us,
                        flatten_asap: false,
                        exit_deadline_us: self.scheduled_exit_deadline_us(),
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
        // Open the ledger before the first harvest can run: the fill
        // for this send may already be in the connector's cache, and
        // counting it against the previous trade would consume it for
        // good (pairtrade#320 Codex review round 4).
        self.begin_fill_ledger(side);
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
        } else if pos
            .exit_deadline_us
            .is_some_and(|deadline| now_us > deadline)
        {
            // The boundary this position was actually scheduled against
            // has passed. Read from the position, not from today's
            // recomputed window: a calendar edit or an
            // `exit_deadline_secs` change across a restart must not move
            // the deadline out from under an open position and leave it
            // waiting (pairtrade#300 Codex review).
            log::warn!(
                "[EXIT] the position's own exit deadline has passed; forcing emergency close"
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
        // Deliberately reads `latest_price` directly rather than
        // `usable_prices` (bot-strategy#916): the entry-side freshness
        // gates must never keep an open position from being closed. The
        // exit is sized from the exchange's own position below; this
        // price only books PnL, so a stale one costs accuracy, not
        // safety. The age is logged so a PnL booked off a stale mid is
        // identifiable after the fact.
        //
        // And when even that is empty (every update since a restart
        // rejected at ingest, e.g. a venue replaying a stale snapshot),
        // fall back to the last raw mid, and failing that to the entry
        // price itself: the reduce-only is sent regardless, the only
        // thing that degrades is which number the PnL is booked at, and
        // the log says which (pairtrade#289 Codex review, P1).
        let (price, price_source) = match self.exit_accounting_price(&self.cfg.us_primary_symbol) {
            Some((price, source)) => (price, source),
            None => {
                log::error!(
                    "[EXIT] no price of any kind seen for {} this process -- closing anyway; the \
                     final remainder's PnL is booked at the entry price (i.e. 0) and must be \
                     reconciled from the exchange fill",
                    self.cfg.us_primary_symbol
                );
                (pos.entry_price, "entry_price_pnl_unknown")
            }
        };
        let feed_generation = self.feed_generation();
        match self.latest_obs(&self.cfg.us_primary_symbol) {
            Some(obs)
                if obs.generation != feed_generation
                    || obs.effective_age_secs(now_us)
                        > self.cfg.max_price_staleness_secs as f64 =>
            {
                log::warn!(
                    "[EXIT] booking PnL off a stale mid for {} (age={:.1}s, obs_gen={} \
                     feed_gen={}); exit still proceeds, sized from the exchange position",
                    self.cfg.us_primary_symbol,
                    obs.effective_age_secs(now_us),
                    obs.generation,
                    feed_generation
                );
            }
            Some(_) => {}
            None => log::warn!(
                "[EXIT] no validated price for {}; booking PnL at {price:.4} (source={price_source})",
                self.cfg.us_primary_symbol
            ),
        }
        if self.cfg.dry_run {
            match self.submit_order(opposite(pos.side), pos.size, true).await {
                Ok(_) => self.on_exit(price, now_us).await,
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
                        // `price` is only a market price when the feed
                        // gave one; the fallback above is the position's
                        // own entry, which must not become the
                        // replacement leg's basis.
                        self.reconcile_side_flip_if_any(
                            &live,
                            (price_source != "entry_price_pnl_unknown").then_some(price),
                            "exit_side_mismatch",
                        );
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
                    self.on_exit(price, now_us).await;
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
            price_is_market: price_source != "entry_price_pnl_unknown",
            // Fresh clock after the send, same reason as the entry path.
            deadline_us: self.now() + self.cfg.fill_confirm_timeout_secs.max(1) * 1_000_000,
            saw_reading: false,
        });
    }

    async fn on_exit(&mut self, exit_price: f64, now_us: i64) {
        let Some(pos) = self.position.take() else {
            return;
        };
        // Clear the persisted record as part of booking the close, not at
        // the end of the tick: a crash in between would otherwise leave a
        // record of a position the exchange no longer holds, which the
        // next start would read as `Vanished` and halt on
        // (bot-strategy#917). Field only -- `mark_day_acted` below writes
        // it together with the PnL, trade counters and drawdown this
        // close produced, so the two can never disagree on disk
        // (pairtrade#300 Codex review).
        self.sync_open_position_field();
        let sign = match pos.side {
            OrderSide::Long => 1.0,
            OrderSide::Short => -1.0,
        };
        // Final remainder at the final price, plus what earlier partial
        // reductions already realized at their own prices.
        //
        // Unless there is no basis to measure against: an adopted position
        // whose cost basis was never known carries a placeholder
        // `entry_price`, and `(exit_price - 0.0) * size` would book the
        // position's whole notional as profit -- inflating the session
        // PnL, the peak equity and therefore the drawdown halt. Only what
        // was actually realized is booked, and the unmeasurable part is
        // said out loud (pairtrade#300 Codex review).
        let pnl = if pos.entry_price_unknown {
            log::error!(
                "[EXIT] {} closed at {exit_price:.4} with NO known cost basis (adopted from the \
                 exchange with neither an entry price nor a mid): only the ${:.2} already \
                 realized is booked, and the PnL of this position is not measurable from what \
                 this process saw",
                self.cfg.us_primary_symbol,
                pos.realized_partial_pnl
            );
            pos.realized_partial_pnl
        } else {
            pos.realized_partial_pnl + sign * (exit_price - pos.entry_price) * pos.open_size
        };
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
        self.reprice_equity_and_drawdown();
        self.day.exited = true;
        self.mark_day_acted(self.day.skip_reason.clone());

        // One last harvest before the account is summarised. The exit
        // fill is routinely visible in the connector's cache on the very
        // tick that observes the account flat -- the account is flat
        // *because* of it -- and the tick's own harvest runs after the
        // decisions, so without this the closing fill would never be
        // recorded and every trade would settle as unknown
        // (pairtrade#320 Codex review, P1).
        self.harvest_fills(now_us).await;
        // The settled account of this trade, from the venue's own fills
        // (bot-strategy#919). Published beside the mid-based figures,
        // never in place of them: `pnl_usd` keeps meaning exactly what
        // it meant before, and a reader that wants what the account did
        // reads `settled`. When coverage is incomplete the whole block
        // is null rather than a partial number wearing a settled label.
        let settled = self.settled_trade_record(&pos, sign, pnl);
        // Best-effort here: the close has already been booked into the
        // durable state, and nothing re-derives it from this line.
        let _ = append_pnl_log(
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
                "pnl_source": "ws_mid_estimate",
                "settled": settled,
                "held_secs": (now_us - pos.entered_at_us) / 1_000_000,
                "dry_run": self.cfg.dry_run,
            }),
        );
        // The trade is summarised; the ledger closes. Its accumulators
        // and side are deliberately kept, so a fill that straggles in
        // after the close still reaches the durable record with the
        // right leg. The next entry opens a fresh one.
        self.fill_ledger_trade_open = false;
        send_notification(
            format!(
                "Han Bridge EXIT {} pnl=${pnl:.2}",
                self.cfg.us_primary_symbol
            ),
            format!(
                "entry={:.4} exit={exit_price:.4} size={:.6} dry_run={}",
                pos.entry_price, pos.size, self.cfg.dry_run
            ),
        );
    }

    async fn tick(&mut self) {
        let now = self.now();
        self.roll_day_if_needed(now);
        self.maybe_clear_halt();
        // Before anything can enter: what does the account actually hold?
        // Retried every tick until one read succeeds (bot-strategy#917).
        if !self.reconciled {
            self.reconcile_startup(now).await;
        }
        self.maybe_capture_t0(now);
        if self.state.position_unconfirmed && self.position.is_none() && self.pending.is_none() {
            self.try_adopt_unconfirmed(now).await;
        }
        // Fold in whatever the venue has reported so far, *before* the
        // decisions that may close the position (pairtrade#320 Codex
        // review round 2). Two harvests, and both earn their place: this
        // one keeps the durable ledger current through the holding
        // period, so a crash mid-hold does not lose the entry fill; the
        // one inside `on_exit` catches the closing fill, which lands on
        // the very tick that observes the account flat and would
        // otherwise be summarised before it was ever seen. Neither
        // subsumes the other. Costs nothing on the wire -- the connector
        // serves this from its WS-populated cache.
        self.harvest_fills(now).await;
        self.poll_unresolved_claims(now).await;
        if self.pending.is_some() {
            // One exchange read per tick until the in-flight entry/exit is
            // confirmed or its window ends; no new decisions meanwhile.
            self.poll_pending_confirm(now).await;
        } else {
            self.maybe_enter(now).await;
            self.maybe_exit(now).await;
        }
        // The confirmation has resolved (or was never installed), so the
        // durable in-flight marker is no longer owed to a restart.
        if self.pending.is_none() && self.entry_marker_owned {
            self.state.entry_in_flight = None;
            self.entry_marker_owned = false;
            self.state_write_pending = true;
        }
        // Backstop for any path above that changed the position without
        // persisting it itself (bot-strategy#917); a no-op when nothing
        // changed.
        self.persist_position();
        // And once more, because an entry confirmed *during* this tick
        // initialised the ledger only after the harvest above had
        // already run and found no side. Its fill is sitting in the
        // connector's cache right now; waiting 5 s for the next tick
        // would lose it to a crash in between (pairtrade#320 Codex
        // review round 3). A no-op when nothing new arrived.
        self.harvest_fills(now).await;
        // Observational only (bot-strategy#919). Spawned, never
        // awaited: a hung venue read must not hold the tick that drives
        // confirmation, exit and shutdown (pairtrade#316 Codex P1).
        self.force_venue_equity_refresh_after_a_fill();
        self.spawn_venue_equity_refresh(now);
        self.write_status_if_due(now);
    }

    /// Re-read the venue's own account equity for `status.json`
    /// (bot-strategy#919), at most once per
    /// `cfg.venue_equity_refresh_secs`.
    ///
    /// Purely observational. No caller gates on the result, so this
    /// deliberately has no failure path that touches trading state: a
    /// venue that stops answering leaves the previous reading in place
    /// and lets its published age grow, which is the honest rendering
    /// of "we last saw $X, N seconds ago". Replacing it with `None` on
    /// the first error would throw away a good reading over one blip;
    /// replacing it with a fresh timestamp would be a lie.
    ///
    /// `get_balance(None)` returns account-level USD on the Lighter
    /// connector (`equity` = `total_asset_value`, `balance` =
    /// `available_balance`), served from a 300 s cache that a WS fill
    /// invalidates eagerly -- so this is at most one REST call per five
    /// minutes in the steady state, and refreshes promptly right after
    /// an entry or exit fill.
    /// Kick off a venue equity read for `status.json` (bot-strategy#919)
    /// **without awaiting it on the trading tick**.
    ///
    /// The first cut of this awaited `get_balance` inline. That was
    /// wrong in a way that only shows up during a venue outage: an
    /// uncached REST read that hangs holds the tick, and the tick is
    /// what drives `poll_pending_confirm`, `maybe_exit` and the
    /// shutdown path. A purely observational read could then stop an
    /// open position from being confirmed or closed (pairtrade#316
    /// Codex review, P1). Nothing observational may sit on that path,
    /// so the read runs in its own task and publishes into a shared
    /// cell the status writer samples.
    ///
    /// At most one read is in flight at a time: a venue that hangs must
    /// not accumulate a task per interval behind it.
    /// A hash of every exposure this process believes the account
    /// holds -- the managed position's side and open size, and each
    /// unmanaged record's symbol, side and open size. Compared, never
    /// published: only its *changing* matters.
    ///
    /// Every field earns its place. Counting the unmanaged records
    /// instead of describing them missed a partial flatten or a side
    /// flip, where `reconcile_unmanaged` rewrites a record in place and
    /// the count stays put -- a fill that dropped the connector's cache
    /// while this fingerprint compared equal, leaving the pre-fill
    /// balance on the dashboard for a full interval (pairtrade#316
    /// Codex review round 3). Sizes are hashed by their bit pattern
    /// because that is what "the same number as last time" means here;
    /// this is a change detector, not an arithmetic comparison.
    fn position_fingerprint(&self) -> Option<u64> {
        use std::hash::{Hash, Hasher};
        if self.position.is_none() && self.state.unmanaged_positions.is_empty() {
            return None;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match self.position.as_ref() {
            Some(p) => {
                matches!(p.side, OrderSide::Short).hash(&mut hasher);
                p.open_size.to_bits().hash(&mut hasher);
            }
            None => "flat".hash(&mut hasher),
        }
        for u in self.state.unmanaged_positions.iter() {
            u.symbol.hash(&mut hasher);
            u.side.hash(&mut hasher);
            u.open_size.to_bits().hash(&mut hasher);
        }
        Some(hasher.finish())
    }

    /// Clear the refresh throttle when the account has changed shape.
    ///
    /// dex-connector invalidates its `get_balance` cache on a WS fill,
    /// but that only helps if somebody asks. With the engine-level
    /// throttle at the connector's own TTL, an entry landing just after
    /// a refresh would keep publishing pre-fill equity for up to a full
    /// interval -- while the docs promised the reading right after an
    /// entry or exit is fresh (pairtrade#316 Codex review round 2).
    fn force_venue_equity_refresh_after_a_fill(&mut self) {
        let fingerprint = self.position_fingerprint();
        if fingerprint == self.last_position_fingerprint {
            return;
        }
        self.last_position_fingerprint = fingerprint;
        // 0 is the "never attempted" sentinel the throttle already
        // treats as due.
        self.last_venue_equity_attempt_us = 0;
    }

    /// Fold every fill the venue has reported and this process has not
    /// seen into the leg it belongs to, and append it to the durable
    /// ledger (bot-strategy#919).
    ///
    /// Cheap enough to run on every tick: the Lighter connector serves
    /// `get_filled_orders` from its own WS-populated cache and issues no
    /// request, so this adds nothing to the account's rate budget. It is
    /// awaited on the tick for the same reason -- there is no network
    /// call to hang on. (The venue *equity* read, which does go to REST,
    /// is deliberately off-tick; see `spawn_venue_equity_refresh`.)
    ///
    /// Which leg a fill belongs to is decided by its own side, not by
    /// timing: the entry and the exit are opposite sides, and only one
    /// entry may be sent per session day, so within a session the side
    /// is unambiguous. A fill whose side the venue did not report is
    /// counted in neither leg -- it would otherwise land in whichever
    /// leg happened to be open and silently corrupt a VWAP.
    async fn harvest_fills(&mut self, now_us: i64) {
        self.align_fill_ledger_with_position();
        let Some(entry_side) = self.fill_ledger_entry_side else {
            return;
        };
        let Ok(response) = self
            .connector
            .get_filled_orders(&self.cfg.us_primary_symbol)
            .await
        else {
            // The cache read does not fail in practice; if it ever
            // does, coverage simply stays incomplete and the settled
            // figures stay unknown. Nothing to fail closed *on* -- this
            // path books nothing and gates nothing.
            return;
        };
        for fill in response.orders.iter() {
            if fill.is_rejected || self.seen_trade_ids.contains(&fill.trade_id) {
                continue;
            }
            let (Some(side), Some(size), Some(value)) =
                (fill.filled_side, fill.filled_size, fill.filled_value)
            else {
                continue;
            };
            let (Some(size), Some(value)) = (size.to_f64(), value.to_f64()) else {
                continue;
            };
            let leg_name = if side == entry_side { "entry" } else { "exit" };
            let fee = fill.filled_fee.and_then(|f| f.to_f64());
            // Durable first, counted second. The connector keeps
            // re-serving this fill, so a row that failed to reach the
            // ledger can still be retried on a later harvest -- but only
            // if it was never marked seen, and only if it was not
            // already folded into a total (pairtrade#320 Codex review
            // round 2). Marking it seen before the write turned a
            // transient filesystem error into a permanently missing row.
            if !append_pnl_log(
                &self.cfg.fills_log_path,
                &serde_json::json!({
                    "ts_us": now_us,
                    "instance_id": self.cfg.instance_id,
                    "symbol": self.cfg.us_primary_symbol,
                    "leg": leg_name,
                    "order_id": fill.order_id,
                    "trade_id": fill.trade_id,
                    "side": side.to_string(),
                    "size": size,
                    "value": value,
                    "price": if size > 0.0 { Some(value / size) } else { None },
                    "fee_usd": fee,
                    "dry_run": self.cfg.dry_run,
                }),
            ) {
                continue;
            }
            let leg = if side == entry_side {
                &mut self.entry_fills
            } else {
                &mut self.exit_fills
            };
            leg.size += size;
            leg.value += value;
            leg.fills += 1;
            match fee {
                Some(fee) => leg.fee_reported_usd += fee,
                // Sticky: once a fill arrives without a fee this leg's
                // total is unknowable, and a later fill that does report
                // one must not resurrect a total missing this cost.
                None => leg.fee_unknown = true,
            }
            self.seen_trade_ids.insert(fill.trade_id.clone());
        }
    }

    /// The settled account of a closed trade, or `None` when the
    /// venue's fills do not cover both legs (bot-strategy#919).
    ///
    /// `None` rather than a partial figure, on purpose. A settled PnL
    /// computed from half the exit fills is not a smaller truth, it is
    /// a wrong number with an authoritative name, and this is the field
    /// a reconciliation would trust over the mid.
    ///
    /// What it reports is *gross*: fees are not in it, because Lighter
    /// does not surface a per-fill fee through the connector today
    /// (`filled_fee` is hard-coded `None`), and `fee_usd` says so by
    /// being null rather than zero. Gross-settled is still the number
    /// that would have caught 2026-09-10's $0.07 mid-vs-fill gap; the
    /// fee half needs Lighter's authenticated `/api/v1/trades`, which
    /// is dex-connector work.
    fn settled_trade_record(
        &self,
        pos: &OpenPosition,
        sign: f64,
        mid_pnl: f64,
    ) -> Option<serde_json::Value> {
        // `size` is the quantity confirmed at entry; the exit leg has to
        // account for the same quantity for the pair to describe one
        // round trip.
        if !self.entry_fills.covers(pos.size) || !self.exit_fills.covers(pos.size) {
            return None;
        }
        let entry_vwap = self.entry_fills.vwap()?;
        let exit_vwap = self.exit_fills.vwap()?;
        let gross = sign * (exit_vwap - entry_vwap) * pos.size;
        Some(serde_json::json!({
            "entry_vwap": entry_vwap,
            "exit_vwap": exit_vwap,
            "entry_size": self.entry_fills.size,
            "exit_size": self.exit_fills.size,
            "entry_fills": self.entry_fills.fills,
            "exit_fills": self.exit_fills.fills,
            "gross_pnl_usd": gross,
            // Null, not zero: see the doc comment. A reader must be able
            // to tell "no fees" from "fees unknown".
            "entry_fee_usd": self.entry_fills.fee_usd(),
            "exit_fee_usd": self.exit_fills.fee_usd(),
            // The gap this record exists to make visible: how far the
            // mid-based figure the engine actually booked sits from what
            // the account did. Both describe the same round trip -- the
            // exit leg aggregates every partial reduction, and `mid_pnl`
            // already carries `realized_partial_pnl` -- so the
            // difference is the mark-versus-fill error and nothing else.
            "mid_estimate_error_usd": gross - mid_pnl,
        }))
    }

    /// Keep the ledger's key in step with the position it is recording.
    ///
    /// Enforced here, at the single point every fill passes through,
    /// rather than at each site that installs a position. There are
    /// four of those -- the confirmed entry, the unconfirmed-send
    /// adoption, the exchange adoption, and the restart restore -- and
    /// patching them one at a time is how the same defect kept coming
    /// back in review (pairtrade#320 rounds 3, 5 and 6). While a
    /// position exists its side *is* the entry side, so the invariant
    /// is simply that the two agree.
    ///
    /// Two ways they can disagree:
    ///
    /// - **No key at all.** A position restored after a restart or
    ///   adopted from the exchange never went through an entry. Its
    ///   pre-existing fills are not recoverable, so that trade settles
    ///   as unknown -- correct -- but everything from here on is
    ///   recorded.
    /// - **The wrong key.** The exchange confirmed the opposite side
    ///   from the one submitted, and its answer is authoritative.
    ///   Everything harvested so far was sorted against the submitted
    ///   side, so the two legs are inverted, not lost: swapping them is
    ///   the whole correction. Left alone, `settled` would publish
    ///   reversed VWAPs and a gross PnL with the wrong sign -- the one
    ///   number here that is meant to be more trustworthy than the mid.
    ///
    /// Rows already written keep a `leg` derived from the mistaken
    /// side; an append-only log cannot be rewritten. Their `side` field
    /// is the venue's own and stays correct, so reconstruct from that.
    fn align_fill_ledger_with_position(&mut self) {
        let Some(pos_side) = self.position.as_ref().map(|p| p.side) else {
            return;
        };
        if !self.fill_ledger_trade_open {
            self.begin_fill_ledger(pos_side);
        } else if self.fill_ledger_entry_side != Some(pos_side) {
            log::warn!(
                "[FILLS] the position's side {} differs from the ledger's {:?}; re-keying and \
                 swapping its legs. Rows already written carry the pre-swap `leg` label -- \
                 reconstruct from `side`.",
                pos_side,
                self.fill_ledger_entry_side
            );
            std::mem::swap(&mut self.entry_fills, &mut self.exit_fills);
            self.fill_ledger_entry_side = Some(pos_side);
        }
    }

    /// Start a fresh settled account for a trade entering on `side`.
    ///
    /// Clears the two accumulators so one day's fills cannot be folded
    /// into the next day's VWAP -- but deliberately **not**
    /// `seen_trade_ids`, which must outlive every trade in the process:
    /// the connector never prunes its fill cache, so a forgotten trade
    /// id is a fill counted a second time (pairtrade#320 Codex review,
    /// P1).
    fn begin_fill_ledger(&mut self, side: OrderSide) {
        self.entry_fills = LegFills::default();
        self.exit_fills = LegFills::default();
        self.fill_ledger_entry_side = Some(side);
        self.fill_ledger_trade_open = true;
    }

    fn spawn_venue_equity_refresh(&mut self, now_us: i64) {
        let interval_us = self
            .cfg
            .venue_equity_refresh_secs
            .max(1)
            .saturating_mul(1_000_000);
        if self.last_venue_equity_attempt_us != 0
            && now_us - self.last_venue_equity_attempt_us < interval_us
        {
            return;
        }
        {
            let mut cell = self.venue_equity.lock().expect("venue equity mutex");
            if cell.in_flight {
                // The previous read has not come back. Leave the
                // throttle timestamp alone so the next tick re-checks
                // rather than silently skipping a whole interval.
                return;
            }
            cell.in_flight = true;
        }
        self.last_venue_equity_attempt_us = now_us;
        let connector = Arc::clone(&self.connector);
        let cell = Arc::clone(&self.venue_equity);
        // The injectable clock, not `Utc::now()`: the reader stamps the
        // moment the answer arrived, which is after the await and so
        // later than the tick's `now_us`, and the tests drive both from
        // the same synthetic source.
        let clock = Arc::clone(&self.clock);
        tokio::spawn(async move {
            // Bounded, because "the read never returns" is the shape of
            // the outage this row exists to surface. Without it the task
            // never reaches the line that clears `in_flight`, every
            // later refresh is refused by the guard, and the last
            // reading keeps publishing `venue_equity_stale=false` --
            // trustworthy-looking, forever, during precisely the failure
            // it should be reporting (pairtrade#316 Codex review round
            // 2). A timeout is a failed read like any other.
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(VENUE_EQUITY_READ_TIMEOUT_SECS),
                connector.get_balance(None),
            )
            .await;
            let mut cell = cell.lock().expect("venue equity mutex");
            cell.in_flight = false;
            let Ok(result) = result else {
                cell.note_failure(&format!(
                    "no answer within {VENUE_EQUITY_READ_TIMEOUT_SECS}s"
                ));
                return;
            };
            match result {
                Ok(balance) => {
                    // A venue that answers with an unrepresentable
                    // number is not a venue that answered. Treat it as a
                    // failure rather than publishing 0.0 as if the
                    // account were empty -- an equity row reading $0.00
                    // is exactly the alarm an operator must be able to
                    // trust.
                    let (Some(equity_usd), Some(available_usd)) =
                        (balance.equity.to_f64(), balance.balance.to_f64())
                    else {
                        cell.note_failure("balance was not representable as f64");
                        return;
                    };
                    if cell.failing {
                        log::info!(
                            "[EQUITY] venue equity readable again: equity=${equity_usd:.2} \
                             available=${available_usd:.2}"
                        );
                        cell.failing = false;
                    }
                    cell.reading = Some(VenueEquity {
                        equity_usd,
                        available_usd,
                        fetched_at_us: clock(),
                    });
                }
                Err(e) => cell.note_failure(&format!("{e:?}")),
            }
        });
    }

    /// Mark-to-mid PnL of the position this engine manages, or `None`
    /// when there is nothing trustworthy to compute it from
    /// (bot-strategy#919).
    ///
    /// `None` rather than `0.0` in every degenerate case: flat, no cost
    /// basis (`entry_price_unknown`), or no US primary price fresh
    /// enough to mark against. A dashboard can render "-" for `None`;
    /// it cannot tell a real zero from a fabricated one.
    ///
    /// This is a WS mid against the recorded entry price -- no fees, no
    /// funding, and `entry_price` is itself an estimate whenever
    /// `entry_price_estimated` is set. It exists so an operator can see
    /// roughly where an open position stands, not so anyone can book it.
    fn unrealized_pnl_mid_estimate(&self, now_us: i64) -> Option<f64> {
        let pos = self.position.as_ref()?;
        if pos.entry_price_unknown {
            return None;
        }
        let mid = *self
            .usable_prices(now_us)
            .get(&self.cfg.us_primary_symbol)?;
        let direction = match pos.side {
            OrderSide::Long => 1.0,
            OrderSide::Short => -1.0,
        };
        Some((mid - pos.entry_price) * pos.open_size * direction)
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
        let mut positions: Vec<DashboardPosition> = match &self.position {
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
        // A saved claim this process has not yet reconciled -- the
        // account could not be read at startup, say -- is not evidence
        // of a flat account either. `positions_ready` is false while
        // that is true, but a consumer reading the position fields
        // alone would see nothing at all during a venue outage
        // (pairtrade#300 Codex review).
        if self.position.is_none() {
            if let Some(p) = self.state.open_position.as_ref() {
                positions.push(DashboardPosition {
                    symbol: p.symbol.clone(),
                    side: match side_from_str(&p.side) {
                        Some(dex_connector::OrderSide::Short) => "short",
                        _ => "long",
                    },
                    size: p.open_size.to_string(),
                    entry_price: p.entry_price.to_string(),
                });
            }
        }
        // An exposure this engine cannot send orders for is still an
        // exposure the account holds. Omitting it reported
        // `has_position=false` on a dashboard while the position was
        // sitting on the exchange (pairtrade#300 Codex review).
        for p in self.state.unmanaged_positions.iter() {
            positions.push(DashboardPosition {
                symbol: p.symbol.clone(),
                side: match side_from_str(&p.side) {
                    Some(dex_connector::OrderSide::Short) => "short",
                    _ => "long",
                },
                size: p.open_size.to_string(),
                entry_price: p.entry_price.to_string(),
            });
        }
        let positions = positions;
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
        let stale_or_missing_symbols = self.stale_or_missing_symbols(now_us);
        // Mirrors the `positions` fallback above, and for the same
        // reason: a saved claim this process has not yet reconciled --
        // `get_positions()` failed at startup, say -- is not evidence of
        // a flat account. Reporting `managed_position_open=false` and no
        // deadline through a venue outage would hide the hold exactly
        // when it matters most, while the same document is still listing
        // that exposure in `positions` (pairtrade#319 Codex review
        // round 2). `positions_ready` stays false throughout, which is
        // where the uncertainty belongs.
        let (managed_claim_open, managed_claim_exit_deadline_us) = match self.position.as_ref() {
            Some(p) => (true, p.exit_deadline_us),
            // Both conditions earn their place. `!reconciled` keeps
            // this to the window where the account genuinely could not
            // be read: once reconciliation has run and still left the
            // slot empty, that is a decision, not an outage. And the
            // symbol must be the one this instance trades -- when
            // `us_primary` changes with a saved position on the old
            // symbol, DRY_RUN deliberately retains the record while
            // refusing to manage it, and publishing it here would
            // present an intentionally unmanaged exposure as a current
            // hold, complete with the old symbol's deadline
            // (pairtrade#319 Codex review round 3).
            None => match self.state.open_position.as_ref() {
                Some(p) if !self.reconciled && p.symbol == self.cfg.us_primary_symbol => {
                    (true, p.exit_deadline_us)
                }
                _ => (false, None),
            },
        };
        // One lock for both fields so a refresh landing between two
        // reads cannot publish a reading against the wrong staleness
        // verdict.
        let (venue_equity, venue_equity_stale) = {
            let cell = self.venue_equity.lock().expect("venue equity mutex");
            (cell.reading, cell.failing)
        };
        let han_bridge = HanBridgeStatus {
            kr_primary_symbol: self.cfg.kr_primary_symbol.clone(),
            us_primary_symbol: self.cfg.us_primary_symbol.clone(),
            day_entered: extra.day_entered,
            day_exited: extra.day_exited,
            position_unconfirmed: self.state.position_unconfirmed,
            ineligible_reasons: extra.eligibility_ineligible_reasons.clone(),
            session_halt_reason: extra.session_halt_reason.clone(),
            skip_reason: self.day.skip_reason.clone(),
            stale_or_missing_symbols,
            price_feed_generation: self.feed_generation(),
            venue_solvency_reported: true,
            venue_equity_usd: venue_equity.map(|v| v.equity_usd),
            venue_available_usd: venue_equity.map(|v| v.available_usd),
            // Clamped at zero: the reader stamps its own wall clock
            // while the tick carries `now_us`, so a reading taken
            // microseconds after this tick's timestamp must not publish
            // a negative age.
            venue_equity_age_secs: venue_equity
                .map(|v| ((now_us - v.fetched_at_us) / 1_000_000).max(0)),
            venue_equity_stale,
            unrealized_pnl_usd_mid_estimate: self.unrealized_pnl_mid_estimate(now_us),
            exit_deadline_us: managed_claim_exit_deadline_us,
            managed_position_open: managed_claim_open,
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
                // False until this process has compared the account with
                // its own persisted record once (bot-strategy#917): before
                // that the position list is this process's guess, and
                // debot-dashboard's "positions_ready !== false means
                // trustworthy" read must not be told otherwise. It used to
                // be false for the whole day after any restart, because
                // the position was in-memory only and could not be
                // recovered at all; a successful reconcile now restores it.
                // Not trustworthy while a sendTx is awaiting exchange
                // confirmation either: the exchange may already differ from
                // the in-memory list (pairtrade#275 Codex review).
                positions_ready: !(!self.reconciled
                    || self.state.position_unconfirmed
                    || self.state.entry_in_flight.is_some()
                    || self.pending.is_some()),
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
            log::warn!(
                "[STATUS] serialize failed for {}",
                self.cfg.status_path.display()
            );
            return;
        };
        atomic_write_bytes(&self.cfg.status_path, &body);
        if let Some(mirror) = &self.status_s3_mirror {
            mirror.put_async("status.json", body);
        }
    }
}

/// Whether this instance may run with no working notification channel
/// (bot-strategy#968). Under DRY_RUN it may -- nothing is at risk of going
/// unnoticed but a simulated trade. A live run may not: entry, exit and
/// session-halt notifications are the only push path this service has, and
/// `EmailClient::send()` drops them with a single `WARN` when the
/// credentials are absent, which is how `engine-b-live` ran from
/// 2026-09-01 to 2026-09-08 notifying nobody of anything. An operator who
/// genuinely wants to run live without them says so explicitly, and the
/// refusal below turns into a logged `Some(reason)` instead.
///
/// `Ok(None)` = configured, `Ok(Some(reason))` = missing but allowed,
/// `Err(reason)` = missing and not allowed.
fn notification_gate(
    dry_run: bool,
    configured: bool,
    opt_out: bool,
) -> Result<Option<String>, String> {
    if configured {
        return Ok(None);
    }
    let reason = "no e-mail credentials (GMAIL_USER + GMAIL_TO/TO_ADDRESS + GMAIL_APP_PASSWORD): \
                  every ENTRY / EXIT / SESSION HALT notification is dropped"
        .to_string();
    if dry_run || opt_out {
        Ok(Some(reason))
    } else {
        Err(reason)
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
         slippage_bps={} min_daily_volume_usd=${:.0}",
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
        cfg.slippage_bps,
        cfg.min_daily_volume_usd,
    );
    cfg.validate()?;

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

    // The only push alert path this service has (bot-strategy#968).
    match notification_gate(
        cfg.dry_run,
        debot::email_client::EmailClient::is_configured(),
        std::env::var("ENGINE_B_LIVE_ALLOW_NO_NOTIFICATIONS").as_deref() == Ok("yes-i-know"),
    ) {
        Ok(None) => log::info!("[NOTIFY] e-mail notifications configured"),
        Ok(Some(reason)) => log::error!(
            "[NOTIFY] {reason} -- running anyway (DRY_RUN or explicit opt-out); journalctl and \
             status.json are the only ways to see what this instance does"
        ),
        Err(reason) => anyhow::bail!(
            "{reason}. Provision them in /etc/engine-b-live/live-secrets.env (see \
             docs/engine-b-live-operations.md), or set \
             ENGINE_B_LIVE_ALLOW_NO_NOTIFICATIONS=yes-i-know to run live without any alert path"
        ),
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

    let price_rx = connector
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
        feed: Arc::new(std::sync::Mutex::new(PriceFeed::default())),
        clock: Arc::new(now_us),
        current_date: None,
        window: None,
        day: DaySnapshot::default(),
        position: None,
        pending: None,
        state,
        // The first tick reconciles against the exchange before any entry
        // is allowed (bot-strategy#917).
        reconciled: false,
        state_write_pending: false,
        entry_marker_owned: false,
        state_writes: 0,
        last_status_write_us: 0,
        status_s3_mirror: S3Mirror::from_env(),
        venue_equity: Arc::new(std::sync::Mutex::new(VenueEquityCell::default())),
        last_venue_equity_attempt_us: 0,
        last_position_fingerprint: None,
        entry_fills: LegFills::default(),
        exit_fills: LegFills::default(),
        seen_trade_ids: std::collections::HashSet::new(),
        fill_ledger_entry_side: None,
        fill_ledger_trade_open: false,
    };
    if let Some(p) = engine.state.open_position.as_ref() {
        log::warn!(
            "[STARTUP] persisted open position: {} {} size={:.6} (session {}, flatten_asap={}) -- \
             reconciling against the exchange before any entry",
            p.side,
            p.symbol,
            p.open_size,
            p.session_date,
            p.flatten_asap
        );
    }

    // The feed runs in its own task rather than as an arm of the tick
    // loop's `select!` (pairtrade#289 Codex round 4): `tick()` awaits the
    // exchange for seconds at a time, and a `Lagged` queued behind it
    // would not bump the generation until after the order had been sent,
    // so the send-time freshness check would accept pre-lag observations
    // as current. With the feed on its own task, the generation moves
    // while entry preparation is in flight and that check sees it.
    let feed_closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    spawn_price_feed(
        price_rx,
        engine.feed.clone(),
        engine.clock.clone(),
        engine.cfg.max_price_staleness_secs,
        feed_closed.clone(),
    );

    let mut tick_interval = tokio::time::interval(std::time::Duration::from_secs(5));
    // `systemctl stop`/`restart` sends SIGTERM. This prototype does not
    // reduce-only-close on the way out (that is deliberate -- a close it
    // cannot confirm is worse than a documented open position), so the
    // handler exists to make the state durable and say plainly what is
    // still open, instead of the process vanishing mid-tick
    // (bot-strategy#917).
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install the SIGTERM handler")?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("failed to install the SIGINT handler")?;
    // The tick is deliberately **not** awaited inside the `select!`. A
    // signal arriving while `tick()` is inside `submit_order` would drop
    // that future mid-request: the order may already be at the exchange,
    // but `PendingConfirm::Entry` was never installed, so shutdown would
    // report an account with nothing open. The select only decides what
    // to do next; the tick then runs to completion, and a signal that
    // arrives during it is delivered on the next `recv()`
    // (pairtrade#300 Codex review).
    enum Wake {
        Tick,
        Signal(&'static str),
    }
    loop {
        let wake = tokio::select! {
            _ = tick_interval.tick() => Wake::Tick,
            _ = sigterm.recv() => Wake::Signal("SIGTERM"),
            _ = sigint.recv() => Wake::Signal("SIGINT"),
        };
        match wake {
            Wake::Tick => {
                if feed_closed.load(std::sync::atomic::Ordering::SeqCst) {
                    log::error!("[WS] price feed closed, exiting");
                    break;
                }
                engine.tick().await;
            }
            Wake::Signal(signal) => {
                let _ = engine.note_shutdown_signal(signal);
                break;
            }
        }
    }

    // Every way out of the loop, not just the signal one: the
    // feed-closed branch above breaks immediately too (pairtrade#320
    // Codex review rounds 6 and 7). A fill can reach the connector's
    // cache after the last tick, and nothing else will ever write it --
    // on restart the cache is gone and the row is unrecoverable,
    // leaving that trade permanently unsettled. One last read of an
    // in-memory cache on the way out.
    engine.harvest_fills(engine.now()).await;

    Ok(())
}

/// Drain the price broadcast into `feed` for as long as it is open,
/// independently of the tick loop. Sets `closed` when the sender is gone
/// so the tick loop can exit -- the task itself never decides to stop the
/// process.
fn spawn_price_feed(
    mut price_rx: tokio::sync::broadcast::Receiver<PriceUpdate>,
    feed: Arc<std::sync::Mutex<PriceFeed>>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    max_staleness_secs: i64,
    closed: Arc<std::sync::atomic::AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match price_rx.recv().await {
                Ok(update) => {
                    let received_at_us = clock();
                    // Locked only for the map writes; nothing is awaited
                    // while the guard is alive.
                    let result = {
                        let mut feed = feed.lock().expect("price feed mutex");
                        feed.ingest(&update, received_at_us, max_staleness_secs)
                    };
                    if let Err(reason) = result {
                        // Rejected, not stored: whatever was held for this
                        // symbol keeps ageing, so a book that stays
                        // untrustworthy turns into a stale-price skip
                        // rather than into an entry on a bad mid
                        // (bot-strategy#916).
                        log::warn!(
                            "[WS] {} price update rejected ({reason}): mid={} bid={} ask={} ts_ms={}",
                            update.symbol,
                            update.mid_price,
                            update.best_bid,
                            update.best_ask,
                            update.timestamp
                        );
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    // Dropped updates mean every price now held may be
                    // arbitrarily behind the book. Bump the generation so
                    // no entry decision uses a pre-lag observation until
                    // that symbol reports again (bot-strategy#916).
                    let generation = feed.lock().expect("price feed mutex").note_lag();
                    log::warn!(
                        "[WS] price feed lagged, dropped {n} updates -- feed generation now \
                         {generation}; held prices are unusable for entry until re-observed"
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    closed.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
            }
        }
    })
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
            max_price_staleness_secs: 30,
            max_eligibility_attempts: 6,
            t0_capture_grace_secs: 300,
            direction_multiplier: 1.0,
            signal_model: "diff".to_string(),
            entry_deadline_secs: 180,
            exit_deadline_secs: 900,
            fill_confirm_timeout_secs: 15,
            slippage_bps: 50,
            lighter_rest_url: "https://mainnet.zklighter.elliot.ai".to_string(),
            min_daily_volume_usd: 100_000.0,
            equity_usd_reference: 1000.0,
            venue_equity_refresh_secs: 60,
            max_session_loss_bps: 500.0,
            trading_calendar_path: PathBuf::from("/nonexistent"),
            kill_switch_path: PathBuf::from("/nonexistent/KILL_SWITCH"),
            risk_ack_path: PathBuf::from("/nonexistent/RISK_ACK"),
            state_path: PathBuf::from("/nonexistent/state.json"),
            status_path: PathBuf::from("/nonexistent/status.json"),
            pnl_log_path: PathBuf::from("/nonexistent/pnl.jsonl"),
            fills_log_path: PathBuf::from("/nonexistent/fills.jsonl"),
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
                skip_reason: None,
                stale_or_missing_symbols: Vec::new(),
                price_feed_generation: 0,
                venue_solvency_reported: true,
                venue_equity_usd: None,
                venue_available_usd: None,
                venue_equity_age_secs: None,
                venue_equity_stale: false,
                unrealized_pnl_usd_mid_estimate: None,
                exit_deadline_us: None,
                managed_position_open: false,
            },
        }
    }

    #[test]
    fn full_status_flattens_both_structs_into_one_flat_object() {
        let value = serde_json::to_value(fixture_status()).unwrap();
        let obj = value
            .as_object()
            .expect("status must serialize to a JSON object");

        // debot-dashboard's StatusData (main.go) field names.
        for key in [
            "ts",
            "updated_at",
            "id",
            "dex",
            "dry_run",
            "has_position",
            "position_count",
            "positions_ready",
            "positions",
            "pnl_total",
            "pnl_today",
            "pnl_source",
            "kill_switch_active",
            "trade_stats",
        ] {
            assert!(obj.contains_key(key), "missing dashboard field: {key}");
        }
        let trade_stats = obj["trade_stats"].as_object().unwrap();
        for key in ["trades", "wins", "win_rate", "max_dd", "pnl"] {
            assert!(
                trade_stats.contains_key(key),
                "missing trade_stats field: {key}"
            );
        }

        // engine_b_live-specific fields, flattened alongside the above,
        // not nested under a separate "extra" key.
        for key in [
            "ts_us",
            "instance_id",
            "current_date",
            "window",
            "day_entered",
            "day_exited",
            "restart_recovered",
            "session_halted",
            "session_halt_reason",
            "realized_pnl_session",
            "pnl_today_date",
            "total_trades",
            "total_wins",
            "max_dd_bps",
            "max_dd_usd",
            "kill_switch",
            "calendar_version",
        ] {
            assert!(obj.contains_key(key), "missing engine_b_live field: {key}");
        }
        assert!(
            !obj.contains_key("dashboard"),
            "flatten must not leave a nested \"dashboard\" key"
        );
        assert!(
            !obj.contains_key("extra"),
            "flatten must not leave a nested \"extra\" key"
        );

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

    // -------------------------------------------------------------
    // Price ingest validation + freshness (bot-strategy#916)
    // -------------------------------------------------------------

    fn update(mid: &str, bid: &str, ask: &str, timestamp_ms: u64) -> PriceUpdate {
        PriceUpdate {
            symbol: "SNDK".to_string(),
            mid_price: Decimal::from_str(mid).unwrap(),
            best_bid: Decimal::from_str(bid).unwrap(),
            best_ask: Decimal::from_str(ask).unwrap(),
            timestamp: timestamp_ms,
        }
    }

    /// Local receive clock used by the ingest tests: 2026-09-08T00:00:00Z
    /// in micros, with the matching millisecond value for the venue field.
    const NOW_US: i64 = 1_788_825_600_000_000;
    const NOW_MS: u64 = 1_788_825_600_000;

    #[test]
    fn ingest_accepts_a_healthy_two_sided_book() {
        let obs = price_obs_from_update(&update("100.5", "100.0", "101.0", NOW_MS), NOW_US, 3, 30)
            .expect("healthy book must be accepted");
        assert_eq!(obs.mid, 100.5);
        assert_eq!(obs.best_bid, 100.0);
        assert_eq!(obs.best_ask, 101.0);
        assert_eq!(obs.received_at_us, NOW_US);
        assert_eq!(obs.generation, 3);
        assert_eq!(obs.exchange_ts_us, Some(NOW_US));
    }

    #[test]
    fn ingest_rejects_crossed_locked_one_sided_and_non_positive_books() {
        // Crossed: bid above ask.
        assert_eq!(
            price_obs_from_update(&update("100.5", "101.0", "100.0", NOW_MS), NOW_US, 0, 30),
            Err("crossed_or_locked_book")
        );
        // Locked: bid == ask. A mid is computable but there is no spread
        // to cross, so it is not a book we will size an order against.
        assert_eq!(
            price_obs_from_update(&update("100.0", "100.0", "100.0", NOW_MS), NOW_US, 0, 30),
            Err("crossed_or_locked_book")
        );
        // One-sided: no bid.
        assert_eq!(
            price_obs_from_update(&update("100.5", "0", "101.0", NOW_MS), NOW_US, 0, 30),
            Err("one_sided_book")
        );
        // One-sided: no ask.
        assert_eq!(
            price_obs_from_update(&update("100.5", "100.0", "0", NOW_MS), NOW_US, 0, 30),
            Err("one_sided_book")
        );
        // Non-positive mid (the only check the old ingest path had).
        assert_eq!(
            price_obs_from_update(&update("0", "100.0", "101.0", NOW_MS), NOW_US, 0, 30),
            Err("non_positive_mid")
        );
        // Mid outside its own book: impossible on the Lighter path,
        // guarded for any venue that reports rather than derives it.
        assert_eq!(
            price_obs_from_update(&update("105.0", "100.0", "101.0", NOW_MS), NOW_US, 0, 30),
            Err("mid_outside_book")
        );
    }

    #[test]
    fn ingest_rejects_a_venue_timestamp_older_than_the_staleness_bound() {
        // The bot-strategy#908 item 7 shape: a healthy-looking connection
        // replaying an old snapshot. 10 minutes back, 30 s bound.
        let stale_ms = NOW_MS - 600_000;
        assert_eq!(
            price_obs_from_update(&update("100.5", "100.0", "101.0", stale_ms), NOW_US, 0, 30),
            Err("exchange_timestamp_stale")
        );
        // Just inside the bound is accepted.
        let ok = price_obs_from_update(
            &update("100.5", "100.0", "101.0", NOW_MS - 29_000),
            NOW_US,
            0,
            30,
        );
        assert!(ok.is_ok(), "29s-old venue timestamp must pass a 30s bound");
    }

    #[test]
    fn ingest_rejects_a_valid_venue_timestamp_from_days_ago() {
        // A venue replaying a snapshot from last week carries a perfectly
        // valid old timestamp. That is stale data, not a broken clock,
        // and must be rejected -- not waved through on local age alone
        // (pairtrade#289 Codex round 2).
        let week_ago_ms = NOW_MS - 7 * 86_400_000;
        assert_eq!(
            price_obs_from_update(
                &update("100.5", "100.0", "101.0", week_ago_ms),
                NOW_US,
                0,
                30
            ),
            Err("exchange_timestamp_stale")
        );
    }

    #[test]
    fn venue_timestamp_units_are_recognised_by_magnitude() {
        // seconds / millis / micros all normalise to the same instant
        assert_eq!(venue_timestamp_us(NOW_MS / 1_000), Some(NOW_US));
        assert_eq!(venue_timestamp_us(NOW_MS), Some(NOW_US));
        assert_eq!(venue_timestamp_us(NOW_MS * 1_000), Some(NOW_US));
        // ...so a stale stamp is caught whichever unit it arrives in
        let stale_secs = (NOW_MS - 600_000) / 1_000;
        assert_eq!(
            price_obs_from_update(
                &update("100.5", "100.0", "101.0", stale_secs),
                NOW_US,
                0,
                30
            ),
            Err("exchange_timestamp_stale")
        );
        // outside every epoch range: meaningless, not a clock
        assert_eq!(venue_timestamp_us(0), None);
        assert_eq!(venue_timestamp_us(1_788_825), None);
        assert_eq!(venue_timestamp_us(u64::MAX), None);
    }

    #[test]
    fn ingest_ignores_a_meaningless_venue_clock_instead_of_failing_closed_forever() {
        // A value outside every plausible epoch range is not a timestamp
        // in any unit. That must degrade to "use the local receive
        // clock", not reject every update for the life of the process.
        let obs =
            price_obs_from_update(&update("100.5", "100.0", "101.0", 1_788_825), NOW_US, 0, 30)
                .expect("a meaningless venue clock must not reject the update");
        assert_eq!(obs.exchange_ts_us, None);
        // A stamp a year in the future is a broken clock, not stale
        // data: ignored, update kept.
        let far_future = price_obs_from_update(
            &update("100.5", "100.0", "101.0", NOW_MS + 365 * 86_400_000),
            NOW_US,
            0,
            30,
        )
        .expect("a far-future venue clock must not reject the update");
        assert_eq!(far_future.exchange_ts_us, None);
        // Clock skew a few seconds into the future is accepted as skew.
        let future = price_obs_from_update(
            &update("100.5", "100.0", "101.0", NOW_MS + 5_000),
            NOW_US,
            0,
            30,
        )
        .expect("small forward skew must be accepted");
        assert_eq!(future.exchange_ts_us, Some(NOW_US + 5_000_000));
    }

    #[test]
    fn price_obs_is_usable_only_when_fresh_current_generation_and_not_future_dated() {
        let obs = PriceObs {
            mid: 100.0,
            best_bid: 99.5,
            best_ask: 100.5,
            received_at_us: NOW_US,
            exchange_ts_us: Some(NOW_US),
            generation: 2,
        };
        assert!(obs.is_usable(NOW_US, 2, 30));
        assert!(
            obs.is_usable(NOW_US + 30_000_000, 2, 30),
            "exactly at the bound is usable"
        );
        assert!(
            !obs.is_usable(NOW_US + 30_000_001, 2, 30),
            "one micro past the bound is stale"
        );
        assert!(
            !obs.is_usable(NOW_US, 3, 30),
            "an observation from before a feed lag is never usable"
        );
        assert!(
            !obs.is_usable(NOW_US - 1, 2, 30),
            "a future-dated observation (backwards clock step) must fail closed"
        );
        // Arrived 29 s after the venue stamped it: usable for 1 more
        // second, not for another 30 (pairtrade#289 Codex review).
        let late = PriceObs {
            received_at_us: NOW_US + 29_000_000,
            ..obs
        };
        assert!(late.is_usable(NOW_US + 30_000_000, 2, 30));
        assert!(
            !late.is_usable(NOW_US + 31_000_000, 2, 30),
            "venue age must be bounded at decision time, not only at ingest"
        );
        // No plausible venue clock: only the local age counts.
        let no_venue_clock = PriceObs {
            exchange_ts_us: None,
            ..late
        };
        assert!(no_venue_clock.is_usable(NOW_US + 59_000_000, 2, 30));
    }

    // -------------------------------------------------------------
    // Engine-level fail-closed behaviour (bot-strategy#916).
    //
    // These drive the real `maybe_capture_t0` / `maybe_enter` /
    // `maybe_exit` against a stub connector that counts
    // `create_order_taker_ioc` calls, so "no order was sent" is asserted
    // against the send itself rather than against an intermediate flag.
    // The stub's `create_order` panics on purpose (bot-strategy#918):
    // the unbounded ±20% path must stay unreachable from this binary.
    // -------------------------------------------------------------

    #[derive(Default)]
    struct StubConnector {
        orders: std::sync::Mutex<Vec<(String, Decimal, OrderSide, bool)>>,
        /// The `slippage_bps` each touch-relative send carried, in send
        /// order. Separate from `orders` so the existing order assertions
        /// keep their shape while bot-strategy#918's bound is asserted on
        /// its own. Only the no-usable-book exit path still uses it.
        taker_ioc_bps: std::sync::Mutex<Vec<u32>>,
        /// The absolute limit price each send carried (bot-strategy#978),
        /// which is every send that had a book to price against.
        taker_ioc_limits: std::sync::Mutex<Vec<Decimal>>,
        positions: std::sync::Mutex<Vec<PositionSnapshot>>,
        /// Runs inside `get_positions`, standing in for whatever the
        /// feed task does to the shared `PriceFeed` while `maybe_enter`
        /// is awaiting the exchange (pairtrade#289 rounds 4-5).
        #[allow(clippy::type_complexity)]
        on_get_positions: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
        /// Makes `get_positions` fail, standing in for an account this
        /// process cannot read at startup (bot-strategy#917).
        fail_get_positions: std::sync::atomic::AtomicBool,
        /// How many times `get_balance` was called, so the refresh
        /// throttle is asserted against the call itself rather than
        /// against a timestamp field (bot-strategy#919).
        balance_calls: std::sync::atomic::AtomicUsize,
        /// `(equity, available)` the stub reports, and whether the call
        /// fails instead.
        balance: std::sync::Mutex<Option<(Decimal, Decimal)>>,
        /// Makes `get_balance` never return, standing in for a venue
        /// outage where the REST read hangs. The tick must survive it
        /// (pairtrade#316 Codex P1).
        hang_balance: std::sync::atomic::AtomicBool,
        /// Fills the venue reports. The real connector re-serves its
        /// whole cache on every call, so this does too -- which is what
        /// makes trade-id dedupe load-bearing (bot-strategy#919).
        fills: std::sync::Mutex<Vec<dex_connector::FilledOrder>>,
    }

    impl StubConnector {
        fn order_count(&self) -> usize {
            self.orders.lock().unwrap().len()
        }

        fn taker_ioc_bounds(&self) -> Vec<u32> {
            self.taker_ioc_bps.lock().unwrap().clone()
        }

        fn taker_ioc_limit_decimals(&self) -> Vec<Decimal> {
            self.taker_ioc_limits.lock().unwrap().clone()
        }

        fn taker_ioc_limit_prices(&self) -> Vec<f64> {
            self.taker_ioc_limits
                .lock()
                .unwrap()
                .iter()
                .map(|d| d.to_f64().unwrap())
                .collect()
        }

        fn balance_call_count(&self) -> usize {
            self.balance_calls.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn set_balance(&self, equity: &str, available: &str) {
            *self.balance.lock().unwrap() = Some((
                Decimal::from_str(equity).unwrap(),
                Decimal::from_str(available).unwrap(),
            ));
        }

        fn fail_balance(&self) {
            *self.balance.lock().unwrap() = None;
        }

        /// `fee` is `Option` because Lighter reports none today; the
        /// tests cover both what the venue does now and what it would
        /// look like once it reports one.
        fn push_fill(
            &self,
            trade_id: &str,
            side: OrderSide,
            size: &str,
            price: &str,
            fee: Option<&str>,
        ) {
            let size = Decimal::from_str(size).unwrap();
            let price = Decimal::from_str(price).unwrap();
            self.fills.lock().unwrap().push(dex_connector::FilledOrder {
                order_id: format!("o-{trade_id}"),
                is_rejected: false,
                trade_id: trade_id.to_string(),
                filled_side: Some(side),
                filled_size: Some(size),
                filled_value: Some(size * price),
                filled_fee: fee.map(|f| Decimal::from_str(f).unwrap()),
                // Engine B trades Lighter perps, where the fee is charged
                // in the quote asset; the base-fee field exists for
                // Hyperliquid spot (dex-connector v4.7.25,
                // bot-strategy#998). `None` here says "not applicable",
                // which is what this stub means.
                filled_base_fee: None,
                filled_ts_ms: None,
                tx_hash: None,
            });
        }

        fn hang_balance(&self) {
            self.hang_balance
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl DexConnector for StubConnector {
        async fn start(&self) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        /// Required since dex-connector v4.7.26 (bot-strategy#963). The
        /// stub has no funding history to answer with, and an empty list
        /// would be the wrong answer: the trait's contract is that a
        /// connector which cannot report funding says so rather than
        /// implying nothing was charged.
        async fn get_funding_payments(
            &self,
            _since_secs: i64,
        ) -> Result<Vec<dex_connector::FundingPayment>, dex_connector::DexError> {
            Err(dex_connector::DexError::Permanent(
                "StubConnector reports no funding history".to_string(),
            ))
        }
        async fn stop(&self) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn restart(&self, _max_retries: i32) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn set_leverage(
            &self,
            _symbol: &str,
            _leverage: u32,
        ) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn get_ticker(
            &self,
            _symbol: &str,
            _test_price: Option<Decimal>,
        ) -> Result<dex_connector::TickerResponse, dex_connector::DexError> {
            unimplemented!("engine_b_live does not call get_ticker")
        }
        async fn get_filled_orders(
            &self,
            _symbol: &str,
        ) -> Result<dex_connector::FilledOrdersResponse, dex_connector::DexError> {
            Ok(dex_connector::FilledOrdersResponse {
                orders: self.fills.lock().unwrap().clone(),
            })
        }
        async fn get_funding_payments(
            &self,
            _since_secs: i64,
        ) -> Result<Vec<dex_connector::FundingPayment>, dex_connector::DexError> {
            Err(dex_connector::DexError::Permanent(
                "stub connector keeps no funding history".to_string(),
            ))
        }
        async fn get_canceled_orders(
            &self,
            _symbol: &str,
        ) -> Result<dex_connector::CanceledOrdersResponse, dex_connector::DexError> {
            unimplemented!("engine_b_live does not call get_canceled_orders")
        }
        async fn get_open_orders(
            &self,
            _symbol: &str,
        ) -> Result<dex_connector::OpenOrdersResponse, dex_connector::DexError> {
            unimplemented!("engine_b_live does not call get_open_orders")
        }
        async fn get_balance(
            &self,
            _symbol: Option<&str>,
        ) -> Result<dex_connector::BalanceResponse, dex_connector::DexError> {
            self.balance_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.hang_balance.load(std::sync::atomic::Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            match *self.balance.lock().unwrap() {
                Some((equity, balance)) => Ok(dex_connector::BalanceResponse {
                    equity,
                    balance,
                    position_entry_price: None,
                    position_sign: None,
                }),
                None => Err(dex_connector::DexError::Transient(
                    "stub balance unavailable".to_string(),
                )),
            }
        }
        async fn get_combined_balance(
            &self,
        ) -> Result<dex_connector::CombinedBalanceResponse, dex_connector::DexError> {
            unimplemented!("engine_b_live does not call get_combined_balance")
        }
        async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, dex_connector::DexError> {
            if let Some(hook) = self.on_get_positions.lock().unwrap().as_ref() {
                hook();
            }
            if self
                .fail_get_positions
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(dex_connector::DexError::ServerResponse(
                    "stub: account unreadable".to_string(),
                ));
            }
            Ok(self
                .positions
                .lock()
                .unwrap()
                .iter()
                .map(|p| PositionSnapshot {
                    symbol: p.symbol.clone(),
                    size: p.size,
                    sign: p.sign,
                    entry_price: p.entry_price,
                })
                .collect())
        }
        async fn get_last_trades(
            &self,
            _symbol: &str,
        ) -> Result<dex_connector::LastTradesResponse, dex_connector::DexError> {
            unimplemented!("engine_b_live does not call get_last_trades")
        }
        async fn get_order_book(
            &self,
            _symbol: &str,
            _depth: usize,
        ) -> Result<dex_connector::OrderBookSnapshot, dex_connector::DexError> {
            unimplemented!("engine_b_live does not call get_order_book")
        }
        async fn clear_filled_order(
            &self,
            _symbol: &str,
            _trade_id: &str,
        ) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn clear_all_filled_orders(&self) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn clear_canceled_order(
            &self,
            _symbol: &str,
            _order_id: &str,
        ) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn clear_all_canceled_orders(&self) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn create_order(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _price: Option<Decimal>,
            _spread: Option<i64>,
            _reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<dex_connector::CreateOrderResponse, dex_connector::DexError> {
            // Deliberately fatal: every order this binary sends must carry
            // a price bound (bot-strategy#918). Reintroducing a
            // `create_order` send anywhere in the entry/exit path fails
            // the whole suite here rather than silently restoring the
            // ±20% protection price.
            unimplemented!("engine_b_live must send bounded taker IOCs, not create_order")
        }
        #[allow(clippy::too_many_arguments)]
        async fn create_advanced_trigger_order(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _trigger_px: Decimal,
            _limit_px: Option<Decimal>,
            _order_style: dex_connector::TriggerOrderStyle,
            _slippage_bps: Option<u32>,
            _tpsl: dex_connector::TpSl,
            _reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<dex_connector::CreateOrderResponse, dex_connector::DexError> {
            unimplemented!("engine_b_live does not call create_advanced_trigger_order")
        }
        async fn create_order_taker_ioc(
            &self,
            symbol: &str,
            size: Decimal,
            side: OrderSide,
            slippage_bps: u32,
            reduce_only: bool,
        ) -> Result<dex_connector::CreateOrderResponse, dex_connector::DexError> {
            self.orders
                .lock()
                .unwrap()
                .push((symbol.to_string(), size, side, reduce_only));
            self.taker_ioc_bps.lock().unwrap().push(slippage_bps);
            Ok(dex_connector::CreateOrderResponse {
                order_id: "stub".to_string(),
                exchange_order_id: None,
                ordered_price: Decimal::ZERO,
                ordered_size: size,
                client_order_id: None,
            })
        }
        async fn create_order_taker_ioc_at(
            &self,
            symbol: &str,
            size: Decimal,
            side: OrderSide,
            limit_price: Decimal,
            reduce_only: bool,
        ) -> Result<dex_connector::CreateOrderResponse, dex_connector::DexError> {
            self.orders
                .lock()
                .unwrap()
                .push((symbol.to_string(), size, side, reduce_only));
            self.taker_ioc_limits.lock().unwrap().push(limit_price);
            Ok(dex_connector::CreateOrderResponse {
                order_id: "stub".to_string(),
                exchange_order_id: None,
                ordered_price: limit_price,
                ordered_size: size,
                client_order_id: None,
            })
        }
        #[allow(clippy::too_many_arguments)]
        async fn modify_order(
            &self,
            _symbol: &str,
            _order_id: &str,
            _side: OrderSide,
            _target_total_size: Decimal,
            _open_remaining_size: Decimal,
            _price: Option<Decimal>,
            _spread: Option<i64>,
            _reduce_only: bool,
        ) -> Result<dex_connector::CreateOrderResponse, dex_connector::DexError> {
            unimplemented!("engine_b_live does not call modify_order")
        }
        async fn cancel_order(
            &self,
            _symbol: &str,
            _order_id: &str,
        ) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn cancel_all_orders(
            &self,
            _symbol: Option<String>,
        ) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn cancel_orders(
            &self,
            _symbol: Option<String>,
            _order_ids: Vec<String>,
        ) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn close_all_positions(
            &self,
            _symbol: Option<String>,
        ) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn clear_last_trades(&self, _symbol: &str) -> Result<(), dex_connector::DexError> {
            Ok(())
        }
        async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
            false
        }
        async fn sign_evm_65b(&self, _message: &str) -> Result<String, dex_connector::DexError> {
            unimplemented!("engine_b_live does not sign EVM messages")
        }
        async fn sign_evm_65b_with_eip191(
            &self,
            _message: &str,
        ) -> Result<String, dex_connector::DexError> {
            unimplemented!("engine_b_live does not sign EVM messages")
        }
        fn subscribe_price_updates(
            &self,
        ) -> Result<tokio::sync::broadcast::Receiver<PriceUpdate>, dex_connector::DexError>
        {
            unimplemented!("the test drives latest_price directly")
        }
    }

    struct Harness {
        engine: EngineBLiveEngine,
        connector: Arc<StubConnector>,
        /// What `engine.now()` returns; tests move it explicitly so the
        /// send-time re-check is exercised against synthetic timestamps.
        clock: Arc<std::sync::atomic::AtomicI64>,
        // Kept alive for the lifetime of the harness: dropping it removes
        // the state/status/pnl files the engine writes.
        _dir: tempfile::TempDir,
    }

    /// t0 = 2026-09-08T00:00Z, t1 = +6.5 h (KRX close), t2 = +13.5 h
    /// (US cash open) -- the real window shape for a session day.
    const T0_US: i64 = 1_788_825_600_000_000;
    const T1_US: i64 = T0_US + 23_400_000_000;
    const T2_US: i64 = T1_US + 25_200_000_000;

    /// `dry_run: false` so the send actually reaches the stub connector:
    /// under DRY_RUN `submit_order` returns before touching it, and every
    /// "no order was sent" assertion would pass for the wrong reason.
    fn harness() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let connector = Arc::new(StubConnector::default());
        let clock = Arc::new(std::sync::atomic::AtomicI64::new(T0_US));
        let clock_for_engine = clock.clone();
        let mut cfg = fixture_config();
        cfg.dry_run = false;
        cfg.state_path = dir.path().join("state.json");
        cfg.status_path = dir.path().join("status.json");
        cfg.pnl_log_path = dir.path().join("pnl.jsonl");
        cfg.fills_log_path = dir.path().join("fills.jsonl");
        // Unroutable: any eligibility fetch fails fast and offline. Tests
        // that need the gate satisfied set `eligibility_confirmed`.
        cfg.lighter_rest_url = "http://127.0.0.1:1".to_string();
        let engine = EngineBLiveEngine {
            cfg,
            connector: connector.clone(),
            calendar: TradingCalendar {
                calendar_version: "test".to_string(),
                // Must resolve 2026-09-08: `roll_day_if_needed` recomputes
                // `window` from the calendar, so an empty one silently
                // turns it to None and every window-gated path below
                // becomes unreachable -- a test that then proves nothing.
                sessions: HashMap::from([(
                    "2026-09-08".to_string(),
                    SessionEntry {
                        krx_is_open: true,
                        krx_open_utc_us: Some(T0_US),
                        krx_close_utc_us: Some(T1_US),
                        us_is_open: true,
                        us_open_utc_us: Some(T2_US),
                    },
                )]),
            },
            http_client: Client::new(),
            feed: Arc::new(std::sync::Mutex::new(PriceFeed::default())),
            clock: Arc::new(move || clock_for_engine.load(std::sync::atomic::Ordering::SeqCst)),
            current_date: Some(NaiveDate::from_ymd_opt(2026, 9, 8).unwrap()),
            window: Some((T0_US, T1_US, T2_US)),
            day: DaySnapshot::default(),
            position: None,
            pending: None,
            state: RiskState {
                session_start_equity: 1000.0,
                peak_equity: 1000.0,
                ..RiskState::default()
            },
            // The harness stands for an already-running process; the
            // startup reconciliation has its own tests below, which set
            // this back to false.
            reconciled: true,
            state_write_pending: false,
            entry_marker_owned: false,
            state_writes: 0,
            last_status_write_us: 0,
            status_s3_mirror: None,
            venue_equity: Arc::new(std::sync::Mutex::new(VenueEquityCell::default())),
            last_venue_equity_attempt_us: 0,
            last_position_fingerprint: None,
            entry_fills: LegFills::default(),
            exit_fills: LegFills::default(),
            seen_trade_ids: std::collections::HashSet::new(),
            fill_ledger_entry_side: None,
            fill_ledger_trade_open: false,
        };
        Harness {
            engine,
            connector,
            clock,
            _dir: dir,
        }
    }

    impl Harness {
        fn set_now(&self, now_us: i64) {
            self.clock
                .store(now_us, std::sync::atomic::Ordering::SeqCst);
        }

        /// Put one observation in the feed as if it had been accepted at
        /// `received_at_us` on generation `generation`.
        fn observe_at(&mut self, symbol: &str, mid: f64, received_at_us: i64, generation: u64) {
            self.engine.feed.lock().unwrap().latest.insert(
                symbol.to_string(),
                PriceObs {
                    mid,
                    best_bid: mid * 0.999,
                    best_ask: mid * 1.001,
                    received_at_us,
                    exchange_ts_us: Some(received_at_us),
                    generation,
                },
            );
        }

        /// Spawn an equity refresh and wait for the spawned task to
        /// publish. The production path deliberately never awaits it
        /// (pairtrade#316 Codex P1), so a test that wants to assert on
        /// the result has to wait here instead. Bounded: a refresh that
        /// never lands is a hang, and the test should fail rather than
        /// spin forever.
        async fn harvest(&mut self, now_us: i64) {
            self.set_now(now_us);
            self.engine.harvest_fills(now_us).await;
        }

        async fn refresh_equity(&mut self, now_us: i64) {
            // The reader stamps the clock when the answer lands, so the
            // harness clock has to be where the test says it is.
            self.set_now(now_us);
            self.engine.spawn_venue_equity_refresh(now_us);
            for _ in 0..1000 {
                if !self
                    .engine
                    .venue_equity
                    .lock()
                    .expect("venue equity mutex")
                    .in_flight
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
            panic!("venue equity refresh never completed");
        }

        /// Every subscribed symbol observed at `received_at_us` on the
        /// engine's current generation.
        fn observe_all(&mut self, received_at_us: i64, kr: f64, us: f64) {
            let generation = self.engine.feed_generation();
            self.observe_at("SKHY", kr, received_at_us, generation);
            self.observe_at("SNDK", us, received_at_us, generation);
            self.observe_at("SOXL", 100.0, received_at_us, generation);
            self.observe_at("NVDA", 200.0, received_at_us, generation);
        }
    }

    #[test]
    fn t0_capture_waits_rather_than_locking_in_a_partial_snapshot() {
        let mut h = harness();
        // Only control symbols have arrived -- the WS-delivery-order case.
        h.observe_at("SOXL", 100.0, T0_US, 0);
        h.observe_at("NVDA", 200.0, T0_US, 0);
        h.engine.maybe_capture_t0(T0_US + 1_000_000);
        assert!(
            h.engine.day.t0_prices.is_none(),
            "a snapshot without both primaries must not be captured"
        );
        assert!(
            h.engine.day.skip_reason.is_none(),
            "still inside the grace window"
        );
        // The primaries arrive a minute later, still inside the grace
        // window: that is the capture.
        h.observe_all(T0_US + 60_000_000, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US + 60_000_000);
        let captured = h
            .engine
            .day
            .t0_prices
            .clone()
            .expect("capture once complete");
        assert_eq!(captured.get("SKHY"), Some(&180.0));
        assert_eq!(captured.get("SNDK"), Some(&1700.0));
    }

    #[test]
    fn t0_capture_never_uses_a_stale_price_and_abandons_the_day_past_the_grace_window() {
        let mut h = harness();
        // Both primaries present but observed well before t0 and long
        // since gone stale -- the "feed died before the boundary" case.
        h.observe_all(T0_US - 3_600_000_000, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US + 1_000_000);
        assert!(
            h.engine.day.t0_prices.is_none(),
            "stale prices must not be captured as the KRX open"
        );
        assert!(h.engine.day.skip_reason.is_none());
        // Past the grace window the day is abandoned outright rather than
        // backfilled from a much later price.
        h.engine
            .maybe_capture_t0(T0_US + (h.engine.cfg.t0_capture_grace_secs + 1) * 1_000_000);
        assert!(h.engine.day.t0_prices.is_none());
        let reason = h
            .engine
            .day
            .skip_reason
            .clone()
            .expect("day must be abandoned");
        assert!(
            reason.starts_with("no_usable_t0"),
            "unexpected skip reason: {reason}"
        );
        assert!(
            h.engine.day.entered,
            "an abandoned day must not be re-evaluated"
        );
    }

    #[test]
    fn t0_capture_refuses_a_complete_snapshot_that_only_became_fresh_after_the_grace() {
        // The feed recovers at t0 + grace + 1 s: both primaries now have
        // perfectly fresh observations, and that is exactly the case the
        // grace bound exists to refuse -- they are mid-session prices,
        // not the KRX open (pairtrade#289 Codex round 3).
        let mut h = harness();
        let late = T0_US + (h.engine.cfg.t0_capture_grace_secs + 1) * 1_000_000;
        h.observe_all(late, 180.0, 1700.0);
        h.engine.maybe_capture_t0(late);
        assert!(
            h.engine.day.t0_prices.is_none(),
            "a complete but late snapshot must not become t0"
        );
        assert!(
            h.engine.state.t0_prices.is_empty(),
            "and must not be persisted for a later restart to recover"
        );
        let reason = h
            .engine
            .day
            .skip_reason
            .clone()
            .expect("day must be abandoned");
        assert!(
            reason.starts_with("late_t0"),
            "unexpected skip reason: {reason}"
        );
        // One second earlier, the same snapshot is accepted.
        let mut h2 = harness();
        let in_time = T0_US + h2.engine.cfg.t0_capture_grace_secs * 1_000_000;
        h2.observe_all(in_time, 180.0, 1700.0);
        h2.engine.maybe_capture_t0(in_time);
        assert!(
            h2.engine.day.t0_prices.is_some(),
            "inside the grace it is still a valid t0"
        );
        assert!(h2.engine.day.skip_reason.is_none());
    }

    #[tokio::test]
    async fn restarting_after_a_completed_cycle_does_not_record_a_skip() {
        // The 2026-09-08 13:33 UTC incident, reproduced (bot-strategy#965):
        // the day entered at t1 and exited at t2, the process restarted,
        // and `maybe_capture_t0` -- which deliberately ignores the entry
        // window -- found no usable t0 hours past the grace and recorded
        // `no_usable_t0` over a day that had actually traded.
        let mut h = harness();
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        h.observe_all(T1_US, 190.0, 1700.0);
        h.engine.day.eligibility_confirmed = true;
        h.set_now(T1_US + 1_000_000);
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        assert_eq!(h.connector.order_count(), 1, "the day must actually trade");
        let after_trade = load_state(&h.engine.cfg.state_path);
        assert_eq!(after_trade.last_session_date.as_deref(), Some("2026-09-08"));
        assert!(after_trade.last_session_skip_reason.is_none());

        // Restart: same state file, nothing in memory, and (as after a
        // kr_primary change) no persisted t0 this config can use.
        let mut h2 = harness();
        h2.engine.cfg.state_path = h.engine.cfg.state_path.clone();
        h2.engine.state = RiskState {
            t0_prices: HashMap::new(),
            t0_snapshot_date: None,
            ..after_trade
        };
        h2.engine.current_date = None;
        let after_t2 = T2_US + 120_000_000;
        h2.set_now(after_t2);
        h2.engine.roll_day_if_needed(after_t2);
        assert!(h2.engine.day.entered && h2.engine.day.restart_recovered);
        h2.engine.maybe_capture_t0(after_t2);
        assert!(
            h2.engine.day.skip_reason.is_none(),
            "a settled day must not be recorded as skipped, got {:?}",
            h2.engine.day.skip_reason
        );
        let persisted = load_state(&h2.engine.cfg.state_path);
        assert!(
            persisted.last_session_skip_reason.is_none(),
            "and the persisted reason must stay clean, got {:?}",
            persisted.last_session_skip_reason
        );
        // The trade itself is untouched.
        assert_eq!(persisted.last_session_date.as_deref(), Some("2026-09-08"));

        // A day that has NOT been acted on is still abandoned as before.
        let mut h3 = harness();
        h3.engine
            .maybe_capture_t0(T0_US + (h3.engine.cfg.t0_capture_grace_secs + 1) * 1_000_000);
        assert!(h3
            .engine
            .day
            .skip_reason
            .as_deref()
            .is_some_and(|r| r.starts_with("no_usable_t0")));
    }

    #[test]
    fn a_settled_days_skip_reason_survives_a_same_day_restart() {
        // roll_day_if_needed rebuilds `entered` from last_session_date;
        // the reason must come back with it or status.json reports an
        // already-decided day with skip_reason: null (pairtrade#289
        // Codex round 3).
        let mut h = harness();
        h.engine
            .skip_day("below_threshold: |epsilon|=0.00010 < threshold=0.00300".to_string());
        let persisted = load_state(&h.engine.cfg.state_path);
        assert_eq!(persisted.last_session_date.as_deref(), Some("2026-09-08"));
        assert!(persisted
            .last_session_skip_reason
            .as_deref()
            .is_some_and(|r| r.starts_with("below_threshold")));
        // A fresh process on the same day, same state file.
        let mut h2 = harness();
        h2.engine.cfg.state_path = h.engine.cfg.state_path.clone();
        h2.engine.state = persisted;
        h2.engine.current_date = None;
        h2.engine.roll_day_if_needed(T1_US + 60_000_000);
        assert!(h2.engine.day.entered && h2.engine.day.restart_recovered);
        assert!(
            h2.engine
                .day
                .skip_reason
                .as_deref()
                .is_some_and(|r| r.starts_with("below_threshold")),
            "the reason must be restored, got {:?}",
            h2.engine.day.skip_reason
        );
        // A day settled by an entry carries no reason.
        let mut h3 = harness();
        h3.engine.mark_day_acted(None);
        let after_entry = load_state(&h3.engine.cfg.state_path);
        assert!(after_entry.last_session_skip_reason.is_none());
    }

    #[tokio::test]
    async fn entry_sends_nothing_while_the_t1_prices_are_stale() {
        let mut h = harness();
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        assert!(h.engine.day.t0_prices.is_some());
        // Feed stalled an hour before the KRX close.
        h.observe_all(T1_US - 3_600_000_000, 190.0, 1700.0);
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        assert!(
            h.engine.day.t1_prices.is_none(),
            "a stale t1 must not be captured"
        );
        assert_eq!(h.connector.order_count(), 0);
        assert!(h.engine.position.is_none());
    }

    #[tokio::test]
    async fn entry_sends_nothing_until_symbols_are_re_observed_after_a_feed_lag() {
        let mut h = harness();
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        // Fresh by the clock, but observed before the broadcast reported
        // dropped updates: what we hold may be arbitrarily behind.
        h.observe_all(T1_US, 190.0, 1700.0);
        h.engine.feed.lock().unwrap().note_lag();
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        assert!(h.engine.day.t1_prices.is_none());
        assert_eq!(h.connector.order_count(), 0);
        // Re-observed on the new generation, the same tick would proceed.
        h.observe_all(T1_US + 2_000_000, 190.0, 1700.0);
        h.engine.day.eligibility_confirmed = true;
        h.set_now(T1_US + 2_000_000);
        h.engine.maybe_enter(T1_US + 2_000_000).await;
        assert!(
            h.engine.day.t1_prices.is_some(),
            "re-observed prices must unblock the same day"
        );
        assert_eq!(
            h.connector.order_count(),
            1,
            "positive control: the gates above are what blocked the send, not the fixture"
        );
    }

    #[tokio::test]
    async fn entry_sends_nothing_when_the_eligibility_endpoint_cannot_be_read() {
        let mut h = harness();
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        // A KR move with the US leg flat: |epsilon| well over the 0.003
        // threshold, so only the eligibility gate can stop this.
        h.observe_all(T1_US, 190.0, 1700.0);
        for attempt in 1..=h.engine.cfg.max_eligibility_attempts {
            h.engine
                .maybe_enter(T1_US + attempt as i64 * 5_000_000)
                .await;
            assert_eq!(
                h.connector.order_count(),
                0,
                "no order may be sent while eligibility is unknown (attempt {attempt})"
            );
        }
        let reason = h
            .engine
            .day
            .skip_reason
            .clone()
            .expect("the day must be abandoned once the attempts are spent");
        assert!(
            reason.starts_with("eligibility_unavailable"),
            "unexpected skip reason: {reason}"
        );
        assert!(!h.engine.day.eligibility_confirmed);
    }

    #[tokio::test]
    async fn entry_sends_nothing_when_the_price_goes_stale_between_signal_and_send() {
        let mut h = harness();
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        h.observe_all(T1_US, 190.0, 1700.0);
        h.engine.day.eligibility_confirmed = true;
        // t1 captured from fresh prices, then the feed stalls before the
        // order is sized: the send-time re-check is the last gate. The
        // tick *started* 1 s after t1 (every price fresh by that clock);
        // by the time the awaits are done the wall clock says 120 s, and
        // that is the clock the re-check must read (pairtrade#289 Codex
        // review).
        h.engine.day.t1_prices = Some(h.engine.usable_prices(T1_US));
        h.set_now(T1_US + 120_000_000);
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        assert_eq!(h.connector.order_count(), 0);
        assert!(h.engine.position.is_none());
    }

    #[tokio::test]
    async fn exit_still_closes_when_every_update_was_rejected_at_ingest() {
        // A restart into a venue replaying a stale snapshot: nothing ever
        // passes ingest, `latest_price` stays empty, `last_raw_mid` holds
        // whatever the rejected updates carried. The reduce-only must go
        // out regardless (pairtrade#289 Codex review, P1).
        let mut h = harness();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Short,
            entry_price: 1700.0,
            entry_price_estimated: true,
            entry_price_unknown: false,
            size: 0.058,
            open_size: 0.058,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: true,
            exit_deadline_us: None,
        });
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(PositionSnapshot {
                symbol: "SNDK".to_string(),
                size: Decimal::from_str("0.058").unwrap(),
                sign: -1,
                entry_price: None,
            });
        h.engine
            .feed
            .lock()
            .unwrap()
            .last_raw_mid
            .insert("SNDK".to_string(), 1690.0);
        assert!(h.engine.feed.lock().unwrap().latest.is_empty());
        h.set_now(T1_US + 60_000_000);
        h.engine.maybe_exit(T1_US + 60_000_000).await;
        assert_eq!(
            h.connector.order_count(),
            1,
            "exit must not depend on a validated price"
        );
        {
            let orders = h.connector.orders.lock().unwrap();
            assert_eq!(orders[0].2, OrderSide::Long, "reduce-only close of a short");
            assert!(orders[0].3);
        }
        // And with no mid of any kind at all, still closes.
        let mut h2 = harness();
        h2.engine.position = h.engine.position.clone();
        h2.connector
            .positions
            .lock()
            .unwrap()
            .push(PositionSnapshot {
                symbol: "SNDK".to_string(),
                size: Decimal::from_str("0.058").unwrap(),
                sign: -1,
                entry_price: None,
            });
        h2.set_now(T1_US + 60_000_000);
        h2.engine.maybe_exit(T1_US + 60_000_000).await;
        assert_eq!(
            h2.connector.order_count(),
            1,
            "no price at all must not strand exposure"
        );
    }

    #[tokio::test]
    async fn entry_sends_nothing_when_the_feed_lags_during_the_entry_awaits() {
        // The eligibility fetch / position read / set_leverage awaits can
        // take seconds. A `Lagged` during them means everything held may
        // be behind the book, and because the feed runs on its own task
        // the generation moves while those awaits are in flight -- the
        // send-time check must see it (pairtrade#289 Codex round 4).
        let mut h = harness();
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        h.observe_all(T1_US, 190.0, 1700.0);
        h.engine.day.eligibility_confirmed = true;
        h.set_now(T1_US + 1_000_000);
        let feed = h.engine.feed.clone();
        *h.connector.on_get_positions.lock().unwrap() = Some(Box::new(move || {
            feed.lock().unwrap().note_lag();
        }));
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        assert_eq!(
            h.connector.order_count(),
            0,
            "a lag observed during entry preparation must block the send"
        );
        assert!(h.engine.position.is_none());
        assert!(
            h.engine.day.t1_prices.is_none(),
            "the pre-lag t1 must be discarded, not reused"
        );
        // Re-observed on the new generation, the next tick proceeds.
        *h.connector.on_get_positions.lock().unwrap() = None;
        h.observe_all(T1_US + 2_000_000, 190.0, 1700.0);
        h.set_now(T1_US + 2_000_000);
        h.engine.maybe_enter(T1_US + 2_000_000).await;
        assert_eq!(h.connector.order_count(), 1);
    }

    #[tokio::test]
    async fn a_partial_refresh_after_a_lag_does_not_revive_a_stale_t1() {
        // The nastier shape of the same bug: the feed lags during the
        // entry awaits and the *US* leg re-reports on the new generation
        // before the send. `usable_prices` is then satisfied for the
        // traded symbol while epsilon still rests on a KR value captured
        // before the drop, so the generation of the signal itself has to
        // be the gate (pairtrade#289 Codex round 5).
        let mut h = harness();
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        h.observe_all(T1_US, 190.0, 1700.0);
        h.engine.day.eligibility_confirmed = true;
        h.set_now(T1_US + 1_000_000);
        let feed = h.engine.feed.clone();
        *h.connector.on_get_positions.lock().unwrap() = Some(Box::new(move || {
            let mut f = feed.lock().unwrap();
            let generation = f.note_lag();
            // only the US leg comes back on the new generation
            f.latest.insert(
                "SNDK".to_string(),
                PriceObs {
                    mid: 1700.0,
                    best_bid: 1699.0,
                    best_ask: 1701.0,
                    received_at_us: T1_US + 1_000_000,
                    exchange_ts_us: Some(T1_US + 1_000_000),
                    generation,
                },
            );
        }));
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        assert_eq!(
            h.connector.order_count(),
            0,
            "a fresh US price does not make a pre-lag KR signal sendable"
        );
        assert!(
            h.engine.day.t1_prices.is_none(),
            "the stale t1 must be discarded"
        );
        assert!(
            !h.engine.day.entered,
            "the day is still open for a recomputed signal"
        );
        // Both legs back on the current generation: recaptured and sent.
        *h.connector.on_get_positions.lock().unwrap() = None;
        h.observe_all(T1_US + 2_000_000, 190.0, 1700.0);
        h.set_now(T1_US + 2_000_000);
        h.engine.maybe_enter(T1_US + 2_000_000).await;
        assert_eq!(h.connector.order_count(), 1);
    }

    #[tokio::test]
    async fn a_stalled_kr_leg_blocks_the_send_even_without_a_feed_lag() {
        // No `Lagged` at all: the US leg keeps ticking through the entry
        // awaits while the KR leg simply stops reporting and ages past
        // the staleness bound. The generation still matches, so only a
        // per-leg liveness check catches it (pairtrade#289 Codex round 7).
        let mut h = harness();
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        h.observe_all(T1_US, 190.0, 1700.0);
        h.engine.day.eligibility_confirmed = true;
        h.set_now(T1_US + 1_000_000);
        let feed = h.engine.feed.clone();
        let bound_us = h.engine.cfg.max_price_staleness_secs * 1_000_000;
        *h.connector.on_get_positions.lock().unwrap() = Some(Box::new(move || {
            // Only SNDK comes back, well after the KR leg went stale.
            let mut f = feed.lock().unwrap();
            f.latest.insert(
                "SNDK".to_string(),
                PriceObs {
                    mid: 1700.0,
                    best_bid: 1699.0,
                    best_ask: 1701.0,
                    received_at_us: T1_US + bound_us + 2_000_000,
                    exchange_ts_us: Some(T1_US + bound_us + 2_000_000),
                    generation: 0,
                },
            );
        }));
        // Wall clock at send time is past the KR observation's bound.
        h.set_now(T1_US + bound_us + 2_000_000);
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        assert_eq!(
            h.engine.feed_generation(),
            0,
            "this case has no feed lag -- the generation gate cannot be what catches it"
        );
        assert_eq!(
            h.connector.order_count(),
            0,
            "a stale KR leg must block the send even though the traded symbol is fresh"
        );
        assert!(
            h.engine.day.t1_prices.is_none(),
            "the stale signal must be discarded"
        );
        assert!(!h.engine.day.entered);
        // Both legs live again: recaptured and sent.
        *h.connector.on_get_positions.lock().unwrap() = None;
        let later = T1_US + bound_us + 3_000_000;
        h.observe_all(later, 190.0, 1700.0);
        h.set_now(later);
        h.engine.maybe_enter(later).await;
        assert_eq!(h.connector.order_count(), 1);
    }

    #[tokio::test]
    async fn entry_proceeds_when_every_input_is_fresh() {
        let mut h = harness();
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        h.observe_all(T1_US, 190.0, 1700.0);
        h.engine.day.eligibility_confirmed = true;
        h.set_now(T1_US + 1_000_000);
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        assert_eq!(
            h.connector.order_count(),
            1,
            "the fixture must be able to produce an entry, or every no-send \
             assertion above proves nothing"
        );
        let orders = h.connector.orders.lock().unwrap();
        let (symbol, _size, side, reduce_only) = &orders[0];
        assert_eq!(symbol, "SNDK");
        assert_eq!(*side, OrderSide::Long, "KR outperformed, epsilon > 0");
        assert!(!*reduce_only);
        // The order is out and its fill is not confirmed, so disk has to
        // say so: a crash here must not restart as a clean start
        // (pairtrade#300 Codex review).
        assert!(
            load_state(&h.engine.cfg.state_path)
                .entry_in_flight
                .as_deref()
                == Some("SNDK"),
            "the in-flight marker, with its symbol, must be durable before the send"
        );
    }

    #[tokio::test]
    async fn exit_still_closes_on_a_stale_price() {
        let mut h = harness();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1700.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.058,
            open_size: 0.058,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(PositionSnapshot {
                symbol: "SNDK".to_string(),
                size: Decimal::from_str("0.058").unwrap(),
                sign: 1,
                entry_price: Some(Decimal::from_str("1700").unwrap()),
            });
        // The only price we have is hours old and from before a feed lag:
        // unusable for any entry, and deliberately still good enough to
        // get flat on.
        h.observe_at("SNDK", 1710.0, T1_US, 0);
        h.engine.feed.lock().unwrap().generation = 5;
        h.set_now(T2_US + 1_000_000);
        h.engine.maybe_exit(T2_US + 1_000_000).await;
        assert_eq!(
            h.connector.order_count(),
            1,
            "a stale feed must never strand an open position"
        );
        let orders = h.connector.orders.lock().unwrap();
        let (symbol, _size, side, reduce_only) = &orders[0];
        assert_eq!(symbol, "SNDK");
        assert_eq!(*side, OrderSide::Short, "reduce-only close of a long");
        assert!(*reduce_only);
    }

    // -------------------------------------------------------------
    // Bounded taker IOC (bot-strategy#918)
    // -------------------------------------------------------------

    #[tokio::test]
    async fn an_entry_send_carries_the_configured_price_bound() {
        let mut h = harness();
        h.engine.cfg.slippage_bps = 25;
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        h.observe_all(T1_US, 190.0, 1700.0);
        h.engine.day.eligibility_confirmed = true;
        h.set_now(T1_US + 1_000_000);
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        // The bound is the mid-relative one outright now
        // (bot-strategy#978): 1700 * 1.0025 = 1704.25, not a bps figure
        // the venue would re-anchor on its own touch at submit time. The
        // fixture ask is 1701.70, so the limit crosses.
        let sent = h.connector.taker_ioc_limit_prices();
        assert_eq!(sent.len(), 1, "one send");
        assert!(
            (sent[0] - 1704.25).abs() < 1e-6,
            "the entry must cross as a marketable limit at the mid-relative \
             bound itself, not an unbounded protection-price IOC: {}",
            sent[0]
        );
        assert!(
            h.connector.taker_ioc_bounds().is_empty(),
            "and not through the touch-relative path"
        );
    }

    #[tokio::test]
    async fn a_limit_that_is_not_exactly_representable_survives_the_venue_tick() {
        // pairtrade#315 Codex round 2. At 10 bps off a 1700 mid the bound
        // is 1701.7, but the f64 is 1701.6999999999998181..., and
        // `Decimal::from_f64_retain` hands that expansion straight to the
        // connector -- whose *inward* tick rounding then truncates it to
        // 1701.6, a whole tick below the ask, an order that rests instead
        // of crossing. `from_f64` recovers the decimal the number means.
        let mut h = harness();
        h.engine.cfg.slippage_bps = 10;
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        h.observe_all(T1_US, 190.0, 1700.0);
        h.engine.day.eligibility_confirmed = true;
        h.set_now(T1_US + 1_000_000);
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        let sent = h.connector.taker_ioc_limit_decimals();
        assert_eq!(sent.len(), 1, "one send");
        assert_eq!(
            sent[0].round_dp_with_strategy(1, rust_decimal::RoundingStrategy::ToZero),
            Decimal::from_str("1701.7").unwrap(),
            "the venue's inward tick rounding must not drop a tick: {}",
            sent[0]
        );
    }

    #[tokio::test]
    async fn an_exit_send_carries_the_same_bound_as_the_entry() {
        // The exit is the leg that gets sent on a torn book -- a t2 that
        // lands during a gap is exactly when the +/-20% protection price
        // used to be able to pay 20%.
        let mut h = harness();
        h.engine.cfg.slippage_bps = 25;
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1700.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.058,
            open_size: 0.058,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(PositionSnapshot {
                symbol: "SNDK".to_string(),
                size: Decimal::from_str("0.058").unwrap(),
                sign: 1,
                entry_price: Some(Decimal::from_str("1700").unwrap()),
            });
        h.observe_at("SNDK", 1710.0, T2_US, 0);
        h.set_now(T2_US + 1_000_000);
        h.engine.maybe_exit(T2_US + 1_000_000).await;
        // The same 25 bps, against the t2 mid of 1710: 1710 * 0.9975 =
        // 1705.725, inside the fixture bid of 1708.29 so it crosses.
        let sent = h.connector.taker_ioc_limit_prices();
        assert_eq!(sent.len(), 1, "one send");
        assert!((sent[0] - 1705.725).abs() < 1e-6, "{}", sent[0]);
        assert!(h.connector.taker_ioc_bounds().is_empty());
        let orders = h.connector.orders.lock().unwrap();
        assert!(orders[0].3, "and it is still the reduce-only close");
    }

    #[tokio::test]
    async fn a_book_wider_than_the_bound_blocks_an_entry() {
        let mut h = harness();
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        h.observe_all(T1_US, 190.0, 1700.0);
        // A torn SNDK book, fresh enough to pass every staleness gate:
        // the mid still says 1700, so only a spread-aware bound catches
        // it. 1530/1870 is a 1000 bps half-spread against the 50 bps
        // configured here.
        // Read the generation *before* taking the lock: `feed_generation`
        // locks the same non-reentrant mutex, and calling it inside the
        // insert expression deadlocks the test.
        let generation = h.engine.feed_generation();
        h.engine.feed.lock().unwrap().latest.insert(
            "SNDK".to_string(),
            PriceObs {
                mid: 1700.0,
                best_bid: 1530.0,
                best_ask: 1870.0,
                received_at_us: T1_US,
                exchange_ts_us: Some(T1_US),
                generation,
            },
        );
        h.engine.day.eligibility_confirmed = true;
        h.set_now(T1_US + 1_000_000);
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        assert_eq!(
            h.connector.order_count(),
            0,
            "no entry price inside the bound means no entry"
        );
        assert!(
            h.connector.taker_ioc_bounds().is_empty()
                && h.connector.taker_ioc_limit_prices().is_empty(),
            "and nothing was sent under a wider bound either"
        );
    }

    #[test]
    fn the_same_torn_book_still_lets_an_exit_out_at_the_touch() {
        // The asymmetry is deliberate: declining to enter costs a
        // session, declining to exit leaves an unmanaged position
        // through the tear.
        let h = harness();
        h.engine.feed.lock().unwrap().latest.insert(
            "SNDK".to_string(),
            PriceObs {
                mid: 1700.0,
                best_bid: 1530.0,
                best_ask: 1870.0,
                received_at_us: T2_US,
                exchange_ts_us: Some(T2_US),
                generation: 0,
            },
        );
        // Within the staleness bound, so the tear is what decides here
        // and not the freshness gate (see
        // `a_stale_or_lagged_quote_hands_the_exit_back_to_the_connector`).
        h.set_now(T2_US + 1_000_000);
        assert!(
            h.engine.send_limit(OrderSide::Long, false).is_err(),
            "an entry has no price inside the bound"
        );
        assert_eq!(
            h.engine.send_limit(OrderSide::Short, true).unwrap(),
            SendLimit::AtTouch,
            "the exit crosses at the venue's touch rather than stranding the position"
        );
    }

    #[tokio::test]
    async fn a_torn_book_exit_goes_out_priced_by_the_connector() {
        // The `AtTouch` branch end to end: the bound does not reach the
        // book, so the send must be the *percentage* one at the
        // connector's minimum allowance -- it prices off the venue's own
        // book at submit time and adds its own tick, which is the only
        // way an IOC is guaranteed to cross. An absolute limit built from
        // this snapshot could rest one tick short of a touch that has
        // moved, or of one that is not exactly representable in f64.
        let mut h = harness();
        h.engine.cfg.slippage_bps = 25;
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1700.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.058,
            open_size: 0.058,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(PositionSnapshot {
                symbol: "SNDK".to_string(),
                size: Decimal::from_str("0.058").unwrap(),
                sign: 1,
                entry_price: Some(Decimal::from_str("1700").unwrap()),
            });
        let generation = h.engine.feed_generation();
        h.engine.feed.lock().unwrap().latest.insert(
            "SNDK".to_string(),
            PriceObs {
                mid: 1700.0,
                best_bid: 1530.0,
                best_ask: 1870.0,
                received_at_us: T2_US,
                exchange_ts_us: Some(T2_US),
                generation,
            },
        );
        h.set_now(T2_US + 1_000_000);
        h.engine.maybe_exit(T2_US + 1_000_000).await;
        assert_eq!(
            h.connector.taker_ioc_bounds(),
            vec![AT_TOUCH_BPS],
            "the torn-book exit must be priced by the connector, not from this snapshot"
        );
        assert!(
            h.connector.taker_ioc_limit_prices().is_empty(),
            "and not as an absolute limit"
        );
        assert!(
            h.connector.orders.lock().unwrap()[0].3,
            "still the reduce-only close"
        );
    }

    #[test]
    fn a_stale_or_lagged_quote_hands_the_exit_back_to_the_connector() {
        // pairtrade#315 P1. `maybe_exit` deliberately closes on prices
        // too stale to enter on, and an absolute limit off such a mid
        // does not re-anchor: if the market has left it behind, every
        // reduce-only IOC comes back unmarketable and the next tick
        // reuses the same dead quote. The connector's own live touch is
        // the only thing that still gets the position flat.
        for (label, obs) in [
            (
                "stale",
                PriceObs {
                    mid: 1700.0,
                    best_bid: 1699.9,
                    best_ask: 1700.1,
                    received_at_us: T2_US,
                    exchange_ts_us: Some(T2_US),
                    generation: 0,
                },
            ),
            (
                "older generation",
                PriceObs {
                    mid: 1700.0,
                    best_bid: 1699.9,
                    best_ask: 1700.1,
                    received_at_us: T2_US + 60_000_000,
                    exchange_ts_us: Some(T2_US + 60_000_000),
                    generation: 0,
                },
            ),
        ] {
            let h = harness();
            let lagged = label == "older generation";
            if lagged {
                h.engine.feed.lock().unwrap().note_lag();
            }
            h.engine
                .feed
                .lock()
                .unwrap()
                .latest
                .insert("SNDK".to_string(), obs);
            // A minute past the observation, i.e. past the 30 s
            // `max_price_staleness_secs` for the stale case; the lagged
            // case is unusable at any age.
            h.set_now(T2_US + 60_000_000);
            assert_eq!(
                h.engine.send_limit(OrderSide::Short, true).unwrap(),
                SendLimit::Unchecked,
                "{label}: the exit must fall back to the connector's live touch"
            );
            assert!(
                h.engine.send_limit(OrderSide::Long, false).is_err(),
                "{label}: and an entry priced off it must not go out at all"
            );
        }
        // The control: the same book, fresh and on the current
        // generation, is priced here rather than by the connector.
        let mut h = harness();
        h.observe_at("SNDK", 1700.0, T2_US, h.engine.feed_generation());
        h.set_now(T2_US + 1_000_000);
        assert!(matches!(
            h.engine.send_limit(OrderSide::Short, true).unwrap(),
            SendLimit::Bounded(_)
        ));
    }

    #[test]
    fn an_unobserved_book_refuses_an_entry_and_still_permits_an_exit() {
        let h = harness();
        assert!(h.engine.send_limit(OrderSide::Long, false).is_err());
        assert_eq!(
            h.engine.send_limit(OrderSide::Short, true).unwrap(),
            SendLimit::Unchecked,
            "with no book of our own the connector's touch is the only bound left"
        );
    }

    #[test]
    fn usable_prices_and_their_generation_come_from_one_lock() {
        // The pair must always describe the same instant: a generation
        // that says "current" alongside prices from before the lag is
        // exactly the race this accessor exists to remove
        // (pairtrade#289 Codex round 6).
        let mut h = harness();
        h.observe_all(T1_US, 190.0, 1700.0);
        let (prices, generation) = h.engine.usable_prices_with_generation(T1_US);
        assert_eq!(generation, 0);
        assert_eq!(prices.len(), 4);
        h.engine.feed.lock().unwrap().note_lag();
        let (prices, generation) = h.engine.usable_prices_with_generation(T1_US);
        assert_eq!(
            generation, 1,
            "the reported generation must move with the feed"
        );
        assert!(
            prices.is_empty(),
            "and the prices reported with it must already exclude the pre-lag ones"
        );
    }

    #[test]
    fn status_reports_the_stale_symbols_and_the_skip_reason() {
        let mut h = harness();
        h.observe_all(T1_US, 190.0, 1700.0);
        assert!(h.engine.stale_or_missing_symbols(T1_US).is_empty());
        // 60 s later, past the 30 s bound, every symbol is stale.
        let stale = h.engine.stale_or_missing_symbols(T1_US + 60_000_000);
        assert_eq!(stale.len(), 4);
        assert!(stale.contains(&"SKHY".to_string()));
        assert!(stale.contains(&"SNDK".to_string()));
        // A symbol that never arrived is reported too.
        h.engine.feed.lock().unwrap().latest.remove("SNDK");
        assert!(h
            .engine
            .stale_or_missing_symbols(T1_US)
            .contains(&"SNDK".to_string()));
        let debug = h.engine.freshness_debug(T1_US);
        assert!(debug.contains("SNDK=never_observed"), "unexpected: {debug}");
        assert!(
            debug.contains("SKHY=190.0000@0.0s/gen0[ok]"),
            "unexpected: {debug}"
        );
    }

    // -----------------------------------------------------------------
    // Startup reconciliation (bot-strategy#917)
    // -----------------------------------------------------------------

    const TODAY: &str = "2026-09-08";

    fn unmanaged_symbols(state: &RiskState) -> Vec<String> {
        state
            .unmanaged_positions
            .iter()
            .map(|p| p.symbol.clone())
            .collect()
    }

    fn persisted_long(open_size: f64, session_date: &str) -> PersistedPosition {
        PersistedPosition {
            symbol: "SNDK".to_string(),
            side: "long".to_string(),
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: open_size,
            open_size,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            session_date: session_date.to_string(),
            exit_deadline_us: Some(T2_US + 900_000_000),
        }
    }

    fn live_long(size: f64) -> ExchangePosition {
        ExchangePosition {
            side: OrderSide::Long,
            size,
            entry_price: Some(1756.92),
        }
    }

    #[test]
    fn reconcile_is_clean_when_nothing_is_tracked_and_the_account_is_flat() {
        assert_eq!(
            reconcile_action(None, None, "SNDK", TODAY, T2_US),
            ReconcileAction::Clean
        );
    }

    #[test]
    fn reconcile_adopts_an_exchange_position_the_record_does_not_explain() {
        assert_eq!(
            reconcile_action(None, Some(&live_long(0.057)), "SNDK", TODAY, T2_US),
            ReconcileAction::AdoptUnknown
        );
    }

    #[test]
    fn reconcile_flags_a_persisted_position_the_exchange_no_longer_holds() {
        assert_eq!(
            reconcile_action(
                Some(&persisted_long(0.057, TODAY)),
                None,
                "SNDK",
                TODAY,
                T2_US
            ),
            ReconcileAction::Vanished
        );
    }

    #[test]
    fn reconcile_resumes_a_same_day_position_for_its_own_scheduled_exit() {
        // Before t2: the position keeps its window, it is not flattened
        // on sight.
        assert_eq!(
            reconcile_action(
                Some(&persisted_long(0.057, TODAY)),
                Some(&live_long(0.057)),
                "SNDK",
                TODAY,
                T1_US + 60_000_000,
            ),
            ReconcileAction::Resume {
                flatten_asap: false
            }
        );
    }

    #[test]
    fn reconcile_resumes_a_position_whose_window_is_gone_as_flatten_asap() {
        // Yesterday's session.
        assert_eq!(
            reconcile_action(
                Some(&persisted_long(0.057, "2026-09-07")),
                Some(&live_long(0.057)),
                "SNDK",
                TODAY,
                T1_US,
            ),
            ReconcileAction::Resume { flatten_asap: true }
        );
        // Today, but past t2 + exit_deadline.
        assert_eq!(
            reconcile_action(
                Some(&persisted_long(0.057, TODAY)),
                Some(&live_long(0.057)),
                "SNDK",
                TODAY,
                T2_US + 900_000_001,
            ),
            ReconcileAction::Resume { flatten_asap: true }
        );
        // Already flagged before the restart (midnight carry-over).
        let mut carried = persisted_long(0.057, TODAY);
        carried.flatten_asap = true;
        assert_eq!(
            reconcile_action(
                Some(&carried),
                Some(&live_long(0.057)),
                "SNDK",
                TODAY,
                T1_US,
            ),
            ReconcileAction::Resume { flatten_asap: true }
        );
    }

    #[test]
    fn reconcile_reports_side_size_and_symbol_disagreements() {
        let short = ExchangePosition {
            side: OrderSide::Short,
            size: 0.057,
            entry_price: None,
        };
        let side = reconcile_action(
            Some(&persisted_long(0.057, TODAY)),
            Some(&short),
            "SNDK",
            TODAY,
            T2_US,
        );
        assert!(
            matches!(&side, ReconcileAction::Mismatch { reason } if reason.contains("side")),
            "unexpected: {side:?}"
        );
        let size = reconcile_action(
            Some(&persisted_long(0.057, TODAY)),
            Some(&live_long(0.114)),
            "SNDK",
            TODAY,
            T2_US,
        );
        assert!(
            matches!(&size, ReconcileAction::Mismatch { reason } if reason.contains("open_size")),
            "unexpected: {size:?}"
        );
        // A config change to us_primary must not silently apply the old
        // symbol's record to the new symbol's position.
        let symbol = reconcile_action(
            Some(&persisted_long(0.057, TODAY)),
            Some(&live_long(0.057)),
            "MU",
            TODAY,
            T2_US,
        );
        assert!(
            matches!(&symbol, ReconcileAction::Mismatch { reason } if reason.contains("SNDK")),
            "unexpected: {symbol:?}"
        );
        // An unparseable side is a mismatch, never a silent resume.
        let mut corrupt = persisted_long(0.057, TODAY);
        corrupt.side = "sideways".to_string();
        assert!(matches!(
            reconcile_action(
                Some(&corrupt),
                Some(&live_long(0.057)),
                "SNDK",
                TODAY,
                T2_US
            ),
            ReconcileAction::Mismatch { .. }
        ));
    }

    #[test]
    fn sizes_agree_only_within_a_part_per_million() {
        assert!(sizes_agree(0.056918, 0.056918));
        assert!(sizes_agree(0.056918, 0.056918 + 1e-11));
        assert!(!sizes_agree(0.056918, 0.056918 + 1e-6));
        assert!(!sizes_agree(0.056918, 0.113836));
        // Dust on both sides still compares.
        assert!(sizes_agree(0.0, 0.0));
    }

    /// The whole point of #917: a restart between entry and exit resumes
    /// the position and closes it at its own t2, instead of leaving it
    /// stranded on the exchange.
    #[tokio::test]
    async fn a_restart_resumes_the_persisted_position_and_exits_it_at_t2() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.position = None;
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        h.engine.state.last_session_date = Some(TODAY.to_string());
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        // A tick before t2: reconciled, tracked again, but not yet closed.
        h.set_now(T1_US + 60_000_000);
        h.observe_all(T1_US + 60_000_000, 190.0, 1760.0);
        h.engine.tick().await;
        assert!(h.engine.reconciled);
        let resumed = h.engine.position.clone().expect("position resumed");
        assert_eq!(resumed.side, OrderSide::Long);
        assert!((resumed.open_size - 0.057).abs() < 1e-12);
        assert!(!resumed.flatten_asap, "its own window is still ahead");
        assert_eq!(h.connector.order_count(), 0, "nothing closes before t2");
        // At t2 the resumed position exits like any other.
        h.set_now(T2_US);
        h.observe_all(T2_US, 190.0, 1769.1);
        h.engine.tick().await;
        assert_eq!(h.connector.order_count(), 1, "the resumed position exits");
        {
            let orders = h.connector.orders.lock().unwrap();
            assert_eq!(orders[0].2, OrderSide::Short, "reduce-only close of a long");
            assert!(orders[0].3, "reduce_only");
        }
    }

    #[tokio::test]
    async fn an_unreadable_account_blocks_entries_instead_of_guessing() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.connector
            .fail_get_positions
            .store(true, std::sync::atomic::Ordering::SeqCst);
        h.set_now(T1_US);
        h.observe_all(T1_US, 190.0, 1760.0);
        h.engine.tick().await;
        assert!(
            !h.engine.reconciled,
            "a failed read must not count as reconciled"
        );
        assert!(!h.engine.entries_allowed());
        assert_eq!(h.connector.order_count(), 0);
        // Once the account can be read, the block lifts.
        h.connector
            .fail_get_positions
            .store(false, std::sync::atomic::Ordering::SeqCst);
        h.engine.tick().await;
        assert!(h.engine.reconciled);
        assert!(h.engine.entries_allowed());
    }

    #[tokio::test]
    async fn an_unexplained_exchange_position_is_adopted_and_halts_entries() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.031", -1, Some("1700.00")));
        h.set_now(T1_US);
        h.engine.tick().await;
        let adopted = h.engine.position.clone().expect("adopted");
        assert_eq!(adopted.side, OrderSide::Short);
        assert!(adopted.flatten_asap, "unknown origin closes at once");
        assert!(h.engine.state.session_halted, "halt, do not add to it");
        assert!(!h.engine.entries_allowed());
        assert_eq!(
            h.engine
                .state
                .open_position
                .as_ref()
                .map(|p| p.side.clone()),
            Some("short".to_string()),
            "the adoption is persisted too"
        );
    }

    #[tokio::test]
    async fn a_position_that_vanished_while_we_were_down_halts_and_clears() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        // Exchange flat.
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(h.engine.position.is_none());
        assert!(h.engine.state.open_position.is_none(), "record cleared");
        assert!(h.engine.state.session_halted);
        let reason = h
            .engine
            .state
            .session_halt_reason
            .clone()
            .unwrap_or_default();
        assert!(
            reason.contains("position_vanished_while_down"),
            "unexpected: {reason}"
        );
    }

    #[tokio::test]
    async fn dry_run_resumes_its_own_simulated_position_and_never_adopts() {
        let mut h = harness();
        h.engine.cfg.dry_run = true;
        h.engine.reconciled = false;
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        h.engine.state.last_session_date = Some(TODAY.to_string());
        // A real position on the account that DRY_RUN did not open: it is
        // reported, never taken over, and never doubled up on.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.900", -1, Some("1700.00")));
        h.set_now(T1_US + 60_000_000);
        h.engine.tick().await;
        let resumed = h
            .engine
            .position
            .clone()
            .expect("simulated position resumed");
        assert_eq!(resumed.side, OrderSide::Long, "ours, not the exchange's");
        assert!((resumed.open_size - 0.057).abs() < 1e-12);
        assert!(
            !h.engine.state.session_halted,
            "DRY_RUN does not halt on it"
        );
    }

    #[tokio::test]
    async fn an_entry_persists_the_position_and_the_exit_clears_the_record() {
        let mut h = harness();
        h.engine.record_entry(
            OpenPosition {
                side: OrderSide::Long,
                entry_price: 1756.92,
                entry_price_estimated: false,
                entry_price_unknown: false,
                size: 0.057,
                open_size: 0.057,
                realized_partial_pnl: 0.0,
                entered_at_us: T1_US,
                flatten_asap: false,
                exit_deadline_us: h.engine.scheduled_exit_deadline_us(),
            },
            0.0142,
            100.0,
            "",
        );
        let saved = h
            .engine
            .state
            .open_position
            .clone()
            .expect("entry persists the position");
        assert_eq!(saved.side, "long");
        assert_eq!(saved.symbol, "SNDK");
        assert_eq!(saved.session_date, TODAY);
        assert_eq!(saved.exit_deadline_us, Some(T2_US + 900_000_000));
        // It survives a reload of the state file, not just the in-memory copy.
        assert_eq!(
            load_state(&h.engine.cfg.state_path).open_position,
            Some(saved)
        );
        h.engine.on_exit(1769.1, T2_US).await;
        assert!(h.engine.state.open_position.is_none());
        assert!(load_state(&h.engine.cfg.state_path).open_position.is_none());
    }

    /// pairtrade#300 Codex review, P1: the end-of-tick backstop must not
    /// erase the recovery record just because the account could not be
    /// read yet.
    #[tokio::test]
    async fn a_failed_first_read_does_not_erase_the_recovery_record() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        h.engine.state.last_session_date = Some(TODAY.to_string());
        h.connector
            .fail_get_positions
            .store(true, std::sync::atomic::Ordering::SeqCst);
        h.set_now(T1_US + 60_000_000);
        h.engine.tick().await;
        assert!(!h.engine.reconciled);
        assert!(
            h.engine.state.open_position.is_some(),
            "the record must survive a failed read"
        );
        // And once the account reads, the position is resumed normally --
        // not adopted as unknown, not halted.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        h.connector
            .fail_get_positions
            .store(false, std::sync::atomic::Ordering::SeqCst);
        h.engine.tick().await;
        assert!(h.engine.reconciled);
        let resumed = h.engine.position.clone().expect("resumed");
        assert!(!resumed.flatten_asap, "resumed on schedule, not flattened");
        assert!(!h.engine.state.session_halted);
    }

    // -------------------------------------------------------------
    // Venue equity / unrealized visibility (bot-strategy#919).
    //
    // Everything here is observational: the assertions are about what
    // reaches `status.json`, and each test also pins that no order was
    // sent, because a visibility feature that can move the trading path
    // is a bug, not a feature.
    // -------------------------------------------------------------

    fn equity_failing(h: &Harness) -> bool {
        h.engine
            .venue_equity
            .lock()
            .expect("venue equity mutex")
            .failing
    }

    fn read_status(h: &Harness) -> serde_json::Value {
        serde_json::from_str(&std::fs::read_to_string(&h.engine.cfg.status_path).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn venue_equity_reaches_status_with_its_age() {
        let mut h = harness();
        h.connector.set_balance("5000.5", "4900.25");
        h.refresh_equity(T1_US).await;
        // 30 s later the value has not been re-read (the throttle), so
        // the published age must have grown rather than reset.
        h.engine.write_status_if_due(T1_US + 30_000_000);
        let status = read_status(&h);
        assert_eq!(
            status["han_bridge"]["venue_solvency_reported"],
            serde_json::json!(true),
            "presence must be data: a consumer that re-encodes with omitempty \
             drops a null field, and key presence stops being readable"
        );
        assert_eq!(
            status["han_bridge"]["venue_equity_usd"],
            serde_json::json!(5000.5)
        );
        assert_eq!(
            status["han_bridge"]["venue_available_usd"],
            serde_json::json!(4900.25)
        );
        assert_eq!(
            status["han_bridge"]["venue_equity_age_secs"],
            serde_json::json!(30),
            "an equity figure with no age cannot be told apart from a frozen one"
        );
        assert_eq!(h.connector.order_count(), 0);
    }

    #[tokio::test]
    async fn venue_equity_is_null_until_the_first_successful_read() {
        let mut h = harness();
        h.connector.fail_balance();
        h.refresh_equity(T1_US).await;
        h.engine.write_status_if_due(T1_US);
        let status = read_status(&h);
        assert_eq!(
            status["han_bridge"]["venue_equity_usd"],
            serde_json::json!(null),
            "never publish 0.0 for an account that was never read"
        );
        assert_eq!(
            status["han_bridge"]["venue_equity_stale"],
            serde_json::json!(true),
            "the exact failure signal, not a threshold on the age"
        );
        assert!(equity_failing(&h));
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_the_last_reading_and_lets_its_age_grow() {
        let mut h = harness();
        h.connector.set_balance("5000", "5000");
        h.refresh_equity(T1_US).await;
        h.connector.fail_balance();
        h.refresh_equity(T1_US + 600_000_000).await;
        h.engine.write_status_if_due(T1_US + 600_000_000);
        let status = read_status(&h);
        assert_eq!(
            status["han_bridge"]["venue_equity_usd"],
            serde_json::json!(5000.0),
            "one transient error must not discard a good reading"
        );
        assert_eq!(
            status["han_bridge"]["venue_equity_age_secs"],
            serde_json::json!(600),
            "but it must be visibly 10 minutes old"
        );
        assert_eq!(
            status["han_bridge"]["venue_equity_stale"],
            serde_json::json!(true),
            "and flagged as not current, independent of any age threshold"
        );
        assert!(equity_failing(&h));
    }

    #[tokio::test]
    async fn venue_equity_refresh_is_throttled_to_its_interval() {
        let mut h = harness();
        h.connector.set_balance("5000", "5000");
        h.engine.cfg.venue_equity_refresh_secs = 60;
        h.refresh_equity(T1_US).await;
        h.refresh_equity(T1_US + 59_000_000).await;
        assert_eq!(
            h.connector.balance_call_count(),
            1,
            "a 5 s tick must not turn into a 12/min REST loop"
        );
        h.refresh_equity(T1_US + 60_000_000).await;
        assert_eq!(h.connector.balance_call_count(), 2);
    }

    /// pairtrade#316 Codex review, P1. The failure this pins is not
    /// "the refresh is slow" but "a slow refresh stops the engine from
    /// closing a position", so the assertion is on the tick completing
    /// while the venue read is still outstanding.
    /// `has_position` counts unmanaged exposures, so it cannot answer
    /// "is Engine B holding" -- a consumer that read it that way would
    /// announce an exit for a position Engine B never took
    /// (debot-dashboard#47 Codex review).
    #[tokio::test]
    async fn managed_and_account_wide_position_flags_are_not_the_same_question() {
        let mut h = harness();
        h.engine
            .state
            .unmanaged_positions
            .push(persisted_long(0.04, TODAY));
        h.engine.write_status_if_due(T1_US);
        let status = read_status(&h);
        assert_eq!(
            status["has_position"],
            serde_json::json!(true),
            "the account does hold something"
        );
        assert_eq!(
            status["han_bridge"]["managed_position_open"],
            serde_json::json!(false),
            "but this engine opened nothing, so it has no exit to announce"
        );
    }

    /// pairtrade#319 Codex review round 2: a venue outage at startup is
    /// exactly when the persisted claim must not read as flat. The
    /// `positions` list already falls back to it; these fields have to
    /// agree with the document they sit in.
    #[tokio::test]
    async fn an_unreconciled_claim_still_reports_a_managed_hold_and_its_deadline() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.position = None;
        h.engine.state.open_position = Some(PersistedPosition {
            exit_deadline_us: Some(T2_US + 900_000_000),
            ..persisted_long(0.05, TODAY)
        });
        h.engine.write_status_if_due(T1_US);
        let status = read_status(&h);
        assert_eq!(
            status["han_bridge"]["managed_position_open"],
            serde_json::json!(true),
            "the account could not be read; that is not evidence of flat"
        );
        assert_eq!(
            status["han_bridge"]["exit_deadline_us"],
            serde_json::json!(T2_US + 900_000_000),
            "and the saved exit must not vanish for the outage"
        );
        assert_eq!(
            status["positions_ready"],
            serde_json::json!(false),
            "the uncertainty belongs here, not in a claim of flatness"
        );
        assert_eq!(
            status["positions"].as_array().map(|a| a.len()),
            Some(1),
            "consistent with the list the same document publishes"
        );
    }

    /// pairtrade#319 Codex review round 3: the fallback is for an
    /// outage, not for every empty slot. A record kept on purpose while
    /// refusing to manage it is not a managed hold.
    #[tokio::test]
    async fn a_retained_foreign_symbol_record_is_not_a_managed_hold() {
        let mut h = harness();
        h.engine.position = None;
        h.engine.state.open_position = Some(PersistedPosition {
            symbol: "MU".to_string(),
            exit_deadline_us: Some(T2_US + 900_000_000),
            ..persisted_long(0.05, TODAY)
        });

        // Reconciliation ran and left the slot empty on purpose.
        h.engine.reconciled = true;
        h.engine.write_status_if_due(T1_US);
        let settled = read_status(&h);
        assert_eq!(
            settled["han_bridge"]["managed_position_open"],
            serde_json::json!(false),
            "a decision to not manage it is not an outage"
        );
        assert_eq!(
            settled["han_bridge"]["exit_deadline_us"],
            serde_json::json!(null),
            "and the old symbol's deadline must not surface beside the new primary"
        );

        // Even mid-outage, a record for a symbol this instance does not
        // trade is not this instance's hold.
        h.engine.reconciled = false;
        h.engine.write_status_if_due(T1_US + 60_000_000);
        assert_eq!(
            read_status(&h)["han_bridge"]["managed_position_open"],
            serde_json::json!(false)
        );

        // The two conditions are independent, so the matching-symbol
        // half is pinned too: reconciliation having *run* and still
        // left the slot empty is a decision about this exposure, and
        // the fallback exists for the case where the account could not
        // be read at all.
        h.engine.reconciled = true;
        h.engine.state.open_position = Some(PersistedPosition {
            exit_deadline_us: Some(T2_US + 900_000_000),
            ..persisted_long(0.05, TODAY)
        });
        assert_eq!(
            h.engine.state.open_position.as_ref().unwrap().symbol,
            h.engine.cfg.us_primary_symbol,
            "this case is only meaningful with the symbol matching"
        );
        h.engine.write_status_if_due(T1_US + 120_000_000);
        assert_eq!(
            read_status(&h)["han_bridge"]["managed_position_open"],
            serde_json::json!(false),
            "reconciled and still empty is not an outage"
        );
    }

    /// The exit deadline is what lets a card say a scheduled exit is
    /// late rather than reading "Entered, holding" forever after an
    /// unconfirmable close (bot-strategy#917).
    #[tokio::test]
    async fn the_exit_deadline_is_published_only_while_something_is_open() {
        let mut h = harness();
        h.engine.write_status_if_due(T1_US);
        assert_eq!(
            read_status(&h)["han_bridge"]["exit_deadline_us"],
            serde_json::json!(null),
            "flat: there is no exit to be late for"
        );

        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1700.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.05,
            open_size: 0.05,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: Some(T2_US + 900_000_000),
        });
        h.engine.write_status_if_due(T1_US + 60_000_000);
        let status = read_status(&h);
        assert_eq!(
            status["han_bridge"]["exit_deadline_us"],
            serde_json::json!(T2_US + 900_000_000)
        );
        assert_eq!(
            status["han_bridge"]["managed_position_open"],
            serde_json::json!(true)
        );
    }

    // -------------------------------------------------------------
    // Settled fills (bot-strategy#919).
    //
    // The engine books PnL off the WS mid. These pin that the venue's
    // own fills are recorded beside it, and that a settlement which
    // cannot be completed says so instead of borrowing the mid.
    // -------------------------------------------------------------

    fn open_short(h: &mut Harness, size: f64, entry_price: f64) {
        h.engine.begin_fill_ledger(OrderSide::Short);
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Short,
            entry_price,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size,
            open_size: size,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: Some(T2_US + 900_000_000),
        });
    }

    fn last_pnl_record(h: &Harness) -> serde_json::Value {
        let raw = std::fs::read_to_string(&h.engine.cfg.pnl_log_path).unwrap();
        serde_json::from_str(raw.lines().last().unwrap()).unwrap()
    }

    /// 2026-09-10's live cycle, to the tick: short 0.0566 filled at
    /// 1763.60, closed at 1719.14, while the engine booked the close off
    /// a mid of 1717.855. The settled record must show the fills, and
    /// the error against the mid-based figure the engine actually used.
    #[tokio::test]
    async fn the_settled_record_reproduces_the_first_live_cycle() {
        let mut h = harness();
        open_short(&mut h, 0.0566, 1763.60);
        h.connector
            .push_fill("t1", OrderSide::Short, "0.0566", "1763.60", None);
        h.harvest(T1_US).await;
        h.connector
            .push_fill("t2", OrderSide::Long, "0.0566", "1719.14", None);
        h.harvest(T2_US).await;

        h.engine.on_exit(1717.855, T2_US).await;
        let rec = last_pnl_record(&h);

        // Unchanged meaning: still the mid-based figure, now labelled.
        assert_eq!(rec["pnl_source"], serde_json::json!("ws_mid_estimate"));
        let mid_pnl = rec["pnl_usd"].as_f64().unwrap();
        assert!((mid_pnl - 2.589).abs() < 1e-3, "mid pnl: {mid_pnl}");

        let settled = &rec["settled"];
        assert!((settled["entry_vwap"].as_f64().unwrap() - 1763.60).abs() < 1e-9);
        assert!((settled["exit_vwap"].as_f64().unwrap() - 1719.14).abs() < 1e-9);
        let gross = settled["gross_pnl_usd"].as_f64().unwrap();
        assert!((gross - 2.516).abs() < 1e-3, "settled gross: {gross}");
        // The $0.07 that started this issue.
        let err = settled["mid_estimate_error_usd"].as_f64().unwrap();
        assert!(
            (err + 0.0729).abs() < 1e-3,
            "the mid overstated the result by ~$0.07; got {err}"
        );
        // Lighter reports no per-fill fee: null, never 0.0.
        assert_eq!(settled["entry_fee_usd"], serde_json::json!(null));
        assert_eq!(settled["exit_fee_usd"], serde_json::json!(null));
    }

    #[tokio::test]
    async fn partial_fills_average_by_quantity_and_are_deduped_by_trade_id() {
        let mut h = harness();
        open_short(&mut h, 1.0, 100.0);
        // Two entry fills at different sizes: a mean of prices (101.0)
        // would be wrong; the quantity-weighted price is 100.75.
        h.connector
            .push_fill("e1", OrderSide::Short, "0.25", "102.00", None);
        h.connector
            .push_fill("e2", OrderSide::Short, "0.75", "100.333333333", None);
        h.harvest(T1_US).await;
        // The connector re-serves its whole cache every call.
        h.harvest(T1_US + 1_000_000).await;
        h.harvest(T1_US + 2_000_000).await;

        assert_eq!(h.engine.entry_fills.fills, 2, "counted once, not six times");
        assert!((h.engine.entry_fills.size - 1.0).abs() < 1e-9);
        let vwap = h.engine.entry_fills.vwap().unwrap();
        assert!((vwap - 100.75).abs() < 1e-6, "vwap {vwap}");
    }

    #[tokio::test]
    async fn an_incomplete_settlement_is_null_rather_than_a_partial_number() {
        let mut h = harness();
        open_short(&mut h, 0.0566, 1763.60);
        h.connector
            .push_fill("t1", OrderSide::Short, "0.0566", "1763.60", None);
        // Only half the exit came back before the close was booked.
        h.connector
            .push_fill("t2", OrderSide::Long, "0.0283", "1719.14", None);
        h.harvest(T2_US).await;

        h.engine.on_exit(1717.855, T2_US).await;
        let rec = last_pnl_record(&h);
        assert_eq!(
            rec["settled"],
            serde_json::json!(null),
            "half an exit is not a settled trade -- a wrong number with an authoritative name"
        );
        assert!(
            rec["pnl_usd"].as_f64().unwrap() > 0.0,
            "the mid-based figure is unaffected: it is what the engine books"
        );
    }

    #[tokio::test]
    async fn one_unreported_fee_leaves_the_leg_fee_unknown() {
        let mut h = harness();
        open_short(&mut h, 1.0, 100.0);
        h.connector
            .push_fill("e1", OrderSide::Short, "0.5", "100.0", Some("0.02"));
        h.harvest(T1_US).await;
        assert_eq!(h.engine.entry_fills.fee_usd(), Some(0.02));
        // A second fill with no fee makes the leg total unknowable --
        // it must not read as the 0.02 already seen.
        h.connector
            .push_fill("e2", OrderSide::Short, "0.5", "100.0", None);
        h.harvest(T1_US + 1_000_000).await;
        assert_eq!(h.engine.entry_fills.fee_usd(), None);
    }

    #[tokio::test]
    async fn every_fill_reaches_the_durable_ledger_as_it_is_seen() {
        let mut h = harness();
        open_short(&mut h, 1.0, 100.0);
        h.connector
            .push_fill("e1", OrderSide::Short, "0.4", "100.0", None);
        h.connector
            .push_fill("e2", OrderSide::Short, "0.6", "101.0", None);
        h.harvest(T1_US).await;
        let raw = std::fs::read_to_string(&h.engine.cfg.fills_log_path).unwrap();
        let lines: Vec<serde_json::Value> = raw
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2, "one line per fill, written when seen");
        assert_eq!(lines[0]["trade_id"], serde_json::json!("e1"));
        assert_eq!(lines[0]["leg"], serde_json::json!("entry"));
        assert!((lines[1]["price"].as_f64().unwrap() - 101.0).abs() < 1e-9);
    }

    /// pairtrade#320 Codex review, P1. The exit fill is routinely
    /// visible on the very tick that observes the account flat -- the
    /// account is flat *because* of it -- and the tick's own harvest
    /// runs after the decisions. Booking the close without harvesting
    /// first left every trade permanently unsettled and the closing
    /// fill missing from the ledger entirely.
    #[tokio::test]
    async fn a_fill_seen_only_at_the_close_still_settles_the_trade() {
        let mut h = harness();
        open_short(&mut h, 0.0566, 1763.60);
        h.connector
            .push_fill("t1", OrderSide::Short, "0.0566", "1763.60", None);
        h.harvest(T1_US).await;

        // The exit fill appears with no harvest tick of its own before
        // the close is booked.
        h.connector
            .push_fill("t2", OrderSide::Long, "0.0566", "1719.14", None);
        h.set_now(T2_US);
        h.engine.on_exit(1717.855, T2_US).await;

        let rec = last_pnl_record(&h);
        assert!(
            !rec["settled"].is_null(),
            "the closing fill must be harvested before the account is summarised"
        );
        let raw = std::fs::read_to_string(&h.engine.cfg.fills_log_path).unwrap();
        assert_eq!(
            raw.lines().count(),
            2,
            "and it must reach the durable ledger, not only the summary"
        );
    }

    /// pairtrade#320 Codex review, P1. Nothing prunes the connector's
    /// fill cache, so forgetting which trade ids were already counted
    /// makes every historical fill look new at the next entry -- which
    /// would pile the previous trade's quantity into both of this
    /// trade's legs and leave `covers()` failing for the rest of the
    /// process.
    #[tokio::test]
    async fn a_second_trade_settles_despite_the_first_trades_fills_still_being_cached() {
        let mut h = harness();

        // Trade 1, closed.
        open_short(&mut h, 0.0566, 1763.60);
        h.connector
            .push_fill("t1", OrderSide::Short, "0.0566", "1763.60", None);
        h.connector
            .push_fill("t2", OrderSide::Long, "0.0566", "1719.14", None);
        h.harvest(T1_US).await;
        h.engine.on_exit(1717.855, T2_US).await;
        assert!(!last_pnl_record(&h)["settled"].is_null());

        // Trade 2. The connector still serves trade 1's fills, exactly
        // as it does in production.
        h.engine.begin_fill_ledger(OrderSide::Short);
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Short,
            entry_price: 1700.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.05,
            open_size: 0.05,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        h.connector
            .push_fill("t3", OrderSide::Short, "0.05", "1700.00", None);
        h.connector
            .push_fill("t4", OrderSide::Long, "0.05", "1690.00", None);
        h.harvest(T2_US + 60_000_000).await;

        assert_eq!(
            h.engine.entry_fills.size, 0.05,
            "yesterday's 0.0566 must not be in this leg"
        );
        assert_eq!(h.engine.exit_fills.size, 0.05);
        h.engine.on_exit(1690.0, T2_US + 120_000_000).await;
        let rec = last_pnl_record(&h);
        assert!(
            !rec["settled"].is_null(),
            "the second trade of a process must settle too"
        );
        let gross = rec["settled"]["gross_pnl_usd"].as_f64().unwrap();
        assert!(
            (gross - 0.5).abs() < 1e-9,
            "short 1700 -> 1690 on 0.05: {gross}"
        );
    }

    /// pairtrade#320 Codex review round 2. The pre-close harvest is not
    /// a substitute for a periodic one: a ledger that only gets written
    /// when a trade closes loses the entry fill to any crash during the
    /// holding period, which is most of the day.
    #[tokio::test]
    async fn the_entry_fill_is_durable_before_the_close() {
        let mut h = harness();
        h.engine.reconciled = true;
        open_short(&mut h, 0.0566, 1763.60);
        h.connector
            .push_fill("t1", OrderSide::Short, "0.0566", "1763.60", None);

        // A plain tick during the holding period -- no close, no
        // confirmation pending.
        h.set_now(T1_US + 60_000_000);
        h.engine.tick().await;

        let raw = std::fs::read_to_string(&h.engine.cfg.fills_log_path)
            .expect("the entry fill must be on disk long before the exit");
        assert_eq!(raw.lines().count(), 1);
        assert!(h.engine.position.is_some(), "still holding");
    }

    /// pairtrade#320 Codex review round 2: one missing fee makes the
    /// leg unknown *for good*. A later fill that does report one must
    /// not resurrect a total that is missing the first fill's cost.
    #[tokio::test]
    async fn an_unknown_fee_stays_unknown_when_a_later_fill_reports_one() {
        let mut h = harness();
        open_short(&mut h, 1.0, 100.0);
        h.connector
            .push_fill("e1", OrderSide::Short, "0.5", "100.0", None);
        h.harvest(T1_US).await;
        assert_eq!(h.engine.entry_fills.fee_usd(), None);

        h.connector
            .push_fill("e2", OrderSide::Short, "0.5", "100.0", Some("0.02"));
        h.harvest(T1_US + 1_000_000).await;
        assert_eq!(
            h.engine.entry_fills.fee_usd(),
            None,
            "$0.02 is not this leg's fee total -- the first fill's is still unknown"
        );
    }

    /// pairtrade#320 Codex review round 2: a fill counts only once it is
    /// durable. Marking it seen before the write turned a transient
    /// filesystem error into a permanently missing ledger row, while the
    /// totals moved anyway.
    #[tokio::test]
    async fn a_fill_that_could_not_be_written_is_retried_not_lost() {
        let mut h = harness();
        open_short(&mut h, 1.0, 100.0);
        // An unwritable path: the directory does not exist.
        h.engine.cfg.fills_log_path = h
            .engine
            .cfg
            .state_path
            .parent()
            .unwrap()
            .join("no-such-dir")
            .join("fills.jsonl");
        h.connector
            .push_fill("e1", OrderSide::Short, "1.0", "100.0", None);
        h.harvest(T1_US).await;
        assert_eq!(
            h.engine.entry_fills.fills, 0,
            "a fill that never reached the ledger must not be counted either"
        );
        assert!(!h.engine.seen_trade_ids.contains("e1"));

        // The connector still serves it; once the path works the fill
        // lands exactly once.
        h.engine.cfg.fills_log_path = h
            .engine
            .cfg
            .state_path
            .parent()
            .unwrap()
            .join("fills.jsonl");
        h.harvest(T1_US + 1_000_000).await;
        h.harvest(T1_US + 2_000_000).await;
        assert_eq!(h.engine.entry_fills.fills, 1);
        let raw = std::fs::read_to_string(&h.engine.cfg.fills_log_path).unwrap();
        assert_eq!(raw.lines().count(), 1);
    }

    /// pairtrade#320 Codex review round 3. A position restored after a
    /// restart never goes through `record_entry`, so the ledger had no
    /// side and every harvest returned immediately -- for the whole
    /// remaining life of that position, including its exit.
    #[tokio::test]
    async fn a_restored_position_still_records_its_exit_fill() {
        let mut h = harness();
        h.engine.reconciled = true;
        // As a restart leaves it: a position, and a ledger that was
        // never told about it.
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Short,
            entry_price: 1763.60,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.0566,
            open_size: 0.0566,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: Some(T2_US + 900_000_000),
        });
        assert!(h.engine.fill_ledger_entry_side.is_none());

        h.connector
            .push_fill("x1", OrderSide::Long, "0.0566", "1719.14", None);
        h.harvest(T2_US).await;

        let raw = std::fs::read_to_string(&h.engine.cfg.fills_log_path)
            .expect("a restarted process must still record what it sees");
        assert_eq!(raw.lines().count(), 1);
        assert_eq!(h.engine.exit_fills.fills, 1);
        // The entry fills predate this process, so the settlement is
        // honestly unknown rather than wrong.
        h.engine.on_exit(1717.855, T2_US).await;
        assert_eq!(
            last_pnl_record(&h)["settled"],
            serde_json::json!(null),
            "an entry this process never saw cannot be settled"
        );
    }

    /// pairtrade#320 Codex review round 3: an entry confirmed during a
    /// tick initialises the ledger *after* that tick's pre-decision
    /// harvest has already run and found no side. Its fill is in the
    /// connector cache at that moment; waiting for the next 5 s tick
    /// loses it to a crash in between.
    ///
    /// Driven through the real confirmation path -- a `PendingConfirm`
    /// the tick resolves -- because that is the ordering under test.
    /// Installing the position by hand before the tick would let the
    /// pre-decision harvest catch it and the assertion would pass with
    /// the post-decision harvest removed.
    #[tokio::test]
    async fn an_entry_confirmed_mid_tick_is_durable_on_that_tick() {
        let mut h = harness();
        h.engine.reconciled = true;
        h.engine.day.entered = false;
        // The order is out and its fill is already on the venue.
        h.engine.pending = Some(PendingConfirm::Entry {
            side: OrderSide::Short,
            requested: 0.05,
            price: 1700.0,
            epsilon: -0.01,
            notional_usd: 100.0,
            deadline_us: T1_US + 180_000_000,
            after_send_error: None,
            saw_reading: false,
        });
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.05", -1, Some("1700.00")));
        h.connector
            .push_fill("e1", OrderSide::Short, "0.05", "1700.00", None);

        h.set_now(T1_US + 10_000_000);
        h.engine.tick().await;

        assert!(h.engine.position.is_some(), "the entry was confirmed");
        let raw = std::fs::read_to_string(&h.engine.cfg.fills_log_path)
            .expect("the entry fill must not wait a tick to become durable");
        assert_eq!(raw.lines().count(), 1);
        assert_eq!(h.engine.entry_fills.fills, 1);
    }

    /// pairtrade#320 Codex review round 4, and the reason the ledger
    /// now has an explicit lifecycle rather than an implicit one.
    ///
    /// After a completed trade the accumulators and side stay behind so
    /// a straggling fill still lands somewhere sensible. The next
    /// session's entry fill can reach the connector cache while the send
    /// is still `PendingConfirm` -- and a harvest then counted it
    /// against the *previous* trade and marked it seen process-wide, so
    /// `record_entry`'s reset left the new trade with a fill it could
    /// never obtain again. Every second-and-later trade in a process
    /// would have been permanently unsettled.
    ///
    /// Driven through `maybe_enter`, because the fix is that the ledger
    /// opens inside the send.
    #[tokio::test]
    async fn a_second_trade_settles_when_its_fill_arrives_before_confirmation() {
        let mut h = harness();

        // Trade 1, complete.
        open_short(&mut h, 0.0566, 1763.60);
        h.connector
            .push_fill("t1", OrderSide::Short, "0.0566", "1763.60", None);
        h.connector
            .push_fill("t2", OrderSide::Long, "0.0566", "1719.14", None);
        h.harvest(T1_US).await;
        h.engine.on_exit(1717.855, T2_US).await;
        assert!(!last_pnl_record(&h)["settled"].is_null(), "trade 1 settles");

        // Trade 2's send. Its fill is already on the venue by the time
        // the order returns -- the ordering that broke this.
        h.engine.position = None;
        h.engine.day.entered = false;
        h.engine.day.exited = false;
        h.engine.state.last_session_date = None;
        h.observe_all(T0_US, 180.0, 1700.0);
        h.engine.maybe_capture_t0(T0_US);
        h.observe_all(T1_US, 190.0, 1700.0);
        h.engine.day.eligibility_confirmed = true;
        h.set_now(T1_US + 1_000_000);
        h.engine.maybe_enter(T1_US + 1_000_000).await;
        assert_eq!(h.connector.order_count(), 1, "trade 2 was sent");

        // kr up, us flat -> epsilon positive -> this entry is a long,
        // the opposite side from trade 1. That is the case that hurts:
        // attributed against trade 1's still-open ledger it would land
        // in its *exit* leg.
        h.connector
            .push_fill("t3", OrderSide::Long, "0.05", "1700.00", None);
        h.harvest(T1_US + 2_000_000).await;
        assert_eq!(
            h.engine.entry_fills.fills, 1,
            "the fill belongs to the trade being sent, not the one just closed"
        );
        assert!(
            (h.engine.entry_fills.size - 0.05).abs() < 1e-9,
            "and trade 1's quantity is not in it"
        );
        assert_eq!(h.engine.exit_fills.fills, 0);
    }

    /// The closing half of the lifecycle. Once a trade is summarised the
    /// ledger is closed, so a position that turns up afterwards without
    /// going through an entry -- adopted from the exchange -- starts its
    /// own account instead of accumulating into the finished trade's.
    #[tokio::test]
    async fn a_position_adopted_after_a_close_starts_its_own_account() {
        let mut h = harness();
        open_short(&mut h, 0.0566, 1763.60);
        h.connector
            .push_fill("t1", OrderSide::Short, "0.0566", "1763.60", None);
        h.connector
            .push_fill("t2", OrderSide::Long, "0.0566", "1719.14", None);
        h.harvest(T1_US).await;
        h.engine.on_exit(1717.855, T2_US).await;
        assert!(h.engine.entry_fills.fills > 0, "the closed trade's totals");

        // An exposure appears that this process did not open: no entry,
        // no send, just a position.
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1700.0,
            entry_price_estimated: true,
            entry_price_unknown: false,
            size: 0.05,
            open_size: 0.05,
            realized_partial_pnl: 0.0,
            entered_at_us: T2_US,
            flatten_asap: true,
            exit_deadline_us: None,
        });
        h.harvest(T2_US + 1_000_000).await;
        assert_eq!(
            h.engine.entry_fills.fills, 0,
            "the finished trade's fills must not carry into this one"
        );
        assert_eq!(h.engine.exit_fills.fills, 0);
        assert_eq!(h.engine.fill_ledger_entry_side, Some(OrderSide::Long));
    }

    /// pairtrade#320 Codex review round 5: when the exchange holds the
    /// opposite side from the one submitted, its answer is
    /// authoritative -- and a ledger still keyed to the submitted side
    /// files the entry fill as the close and the close as the entry,
    /// publishing reversed VWAPs and a gross PnL with the wrong sign.
    #[tokio::test]
    async fn a_confirmed_side_mismatch_re_keys_the_ledger() {
        let mut h = harness();
        // Submitted long; the fill that came back is a short.
        h.engine.begin_fill_ledger(OrderSide::Long);
        h.connector
            .push_fill("e1", OrderSide::Short, "0.05", "1700.00", None);
        h.harvest(T1_US).await;
        assert_eq!(
            h.engine.exit_fills.fills, 1,
            "sorted against the submitted side, it lands in the wrong leg"
        );

        // The exchange's side is what gets recorded.
        h.engine.record_entry(
            OpenPosition {
                side: OrderSide::Short,
                entry_price: 1700.0,
                entry_price_estimated: false,
                entry_price_unknown: false,
                size: 0.05,
                open_size: 0.05,
                realized_partial_pnl: 0.0,
                entered_at_us: T1_US,
                flatten_asap: false,
                exit_deadline_us: Some(T2_US + 900_000_000),
            },
            -0.01,
            100.0,
            "side_mismatch=true",
        );
        assert_eq!(h.engine.fill_ledger_entry_side, Some(OrderSide::Short));
        assert_eq!(
            h.engine.entry_fills.fills, 1,
            "the legs are swapped, not lost"
        );
        assert_eq!(h.engine.exit_fills.fills, 0);

        // And the settled result now has the right sign: short
        // 1700 -> 1690 on 0.05 is +$0.50, not -$0.50.
        h.connector
            .push_fill("x1", OrderSide::Long, "0.05", "1690.00", None);
        h.harvest(T2_US).await;
        h.engine.on_exit(1690.0, T2_US).await;
        let gross = last_pnl_record(&h)["settled"]["gross_pnl_usd"]
            .as_f64()
            .unwrap();
        assert!((gross - 0.5).abs() < 1e-9, "gross {gross}");
    }

    /// pairtrade#320 Codex review round 6: the same inversion as the
    /// confirmed-entry case, reached through a path that never touches
    /// `record_entry` -- an adoption after the confirmation window
    /// expired. Pinned here because the invariant is now enforced where
    /// fills are classified, not at each site that installs a position.
    #[tokio::test]
    async fn a_position_adopted_on_the_other_side_re_keys_the_ledger_too() {
        let mut h = harness();
        // Submitted long; the venue filled a short and the confirmation
        // window expired, so the position is installed directly.
        h.engine.begin_fill_ledger(OrderSide::Long);
        h.connector
            .push_fill("e1", OrderSide::Short, "0.05", "1700.00", None);
        h.harvest(T1_US).await;
        assert_eq!(
            h.engine.exit_fills.fills, 1,
            "sorted against the submitted side"
        );

        h.engine.position = Some(OpenPosition {
            side: OrderSide::Short,
            entry_price: 1700.0,
            entry_price_estimated: true,
            entry_price_unknown: false,
            size: 0.05,
            open_size: 0.05,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: Some(T2_US + 900_000_000),
        });
        h.connector
            .push_fill("x1", OrderSide::Long, "0.05", "1690.00", None);
        h.harvest(T2_US).await;
        assert_eq!(h.engine.fill_ledger_entry_side, Some(OrderSide::Short));
        assert_eq!(h.engine.entry_fills.fills, 1, "legs swapped, not lost");

        h.engine.on_exit(1690.0, T2_US).await;
        let gross = last_pnl_record(&h)["settled"]["gross_pnl_usd"]
            .as_f64()
            .unwrap();
        assert!(
            (gross - 0.5).abs() < 1e-9,
            "short 1700 -> 1690 on 0.05 is +$0.50, not -$0.50: {gross}"
        );
    }

    #[tokio::test]
    async fn a_hung_venue_read_does_not_hold_the_trading_tick() {
        let mut h = harness();
        h.connector.hang_balance();
        h.set_now(T1_US);
        // Would hang forever if the tick awaited the balance read.
        tokio::time::timeout(std::time::Duration::from_secs(5), h.engine.tick())
            .await
            .expect("tick must not wait on an observational venue read");
        assert!(
            h.engine
                .venue_equity
                .lock()
                .expect("venue equity mutex")
                .in_flight,
            "the read was claimed before the tick returned"
        );
        // The tick finished without ever polling the spawned reader --
        // on a current-thread runtime the task has not even started. A
        // yield lets it reach the (hanging) venue call, which is what
        // the in-flight guard below is about.
        tokio::task::yield_now().await;
        assert_eq!(
            h.connector.balance_call_count(),
            1,
            "the read was started, just not awaited"
        );
        // Later ticks keep working, and do not pile a second read up
        // behind the stuck one.
        h.set_now(T1_US + 3_600_000_000);
        tokio::time::timeout(std::time::Duration::from_secs(5), h.engine.tick())
            .await
            .expect("a stuck read must not wedge every later tick");
        // Same reason as above: without this, a second reader that *was*
        // spawned would not have run yet and the count below would pass
        // for the wrong reason. (Removing the in-flight guard must fail
        // this test; it did not until this yield was added.)
        tokio::task::yield_now().await;
        assert_eq!(
            h.connector.balance_call_count(),
            1,
            "one read in flight at a time, however long the venue takes"
        );
    }

    /// pairtrade#316 Codex review round 2. Surviving the tick is not
    /// enough: a read that never returns also never releases the
    /// in-flight guard, so every later refresh is refused and the last
    /// reading keeps publishing itself as trustworthy -- during exactly
    /// the outage it should be reporting.
    #[tokio::test(start_paused = true)]
    async fn a_venue_read_that_never_returns_is_a_failed_read() {
        let mut h = harness();
        h.connector.set_balance("5000", "5000");
        h.refresh_equity(T1_US).await;
        assert!(!equity_failing(&h), "a good reading first");

        h.connector.hang_balance();
        h.set_now(T1_US + 3_600_000_000);
        h.engine.spawn_venue_equity_refresh(T1_US + 3_600_000_000);
        // Paused clock: this advances time rather than sleeping, so the
        // reader's own timeout fires. Deliberately an absolute duration
        // and not `VENUE_EQUITY_READ_TIMEOUT_SECS + n` -- a bound
        // expressed in terms of the constant it is testing passes for
        // any value of that constant, including a useless one (caught by
        // mutating the constant to 24 h, which this test did not notice
        // until it stopped referring to it).
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;

        let cell = h.engine.venue_equity.lock().expect("venue equity mutex");
        assert!(!cell.in_flight, "the guard must be released, not wedged");
        assert!(
            cell.failing,
            "and the row must say so instead of looking current"
        );
        assert!(
            cell.reading.is_some(),
            "the last good reading is still shown -- with its age"
        );
    }

    /// The timeout is only useful between those two bounds: long enough
    /// that a healthy REST round trip is never cut off, short enough
    /// that a stuck read cannot outlive the interval that would have
    /// replaced it.
    #[test]
    fn the_venue_read_timeout_is_bounded_by_its_own_refresh_interval() {
        assert!(VENUE_EQUITY_READ_TIMEOUT_SECS >= 5);
        assert!(
            (VENUE_EQUITY_READ_TIMEOUT_SECS as i64) < fixture_config().venue_equity_refresh_secs,
            "a read that outlives its refresh interval wedges the schedule"
        );
    }

    /// pairtrade#316 Codex review round 2: a fill is when the published
    /// balance is most wrong, and it is also when dex-connector drops
    /// its own cache. The engine-level throttle must not sit on the next
    /// read for a full interval.
    #[tokio::test]
    async fn a_fill_bypasses_the_refresh_throttle() {
        let mut h = harness();
        h.connector.set_balance("5000", "5000");
        h.engine.cfg.venue_equity_refresh_secs = 300;
        h.refresh_equity(T1_US).await;
        assert_eq!(h.connector.balance_call_count(), 1);

        // Well inside the throttle window: nothing changed, no read.
        h.refresh_equity(T1_US + 10_000_000).await;
        assert_eq!(h.connector.balance_call_count(), 1);

        // A fill lands.
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1700.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.05,
            open_size: 0.05,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        h.engine.force_venue_equity_refresh_after_a_fill();
        h.refresh_equity(T1_US + 20_000_000).await;
        assert_eq!(
            h.connector.balance_call_count(),
            2,
            "the balance after an entry must not be the one from before it"
        );

        // And a partial exit is a change too.
        h.engine.position.as_mut().unwrap().open_size = 0.02;
        h.engine.force_venue_equity_refresh_after_a_fill();
        h.refresh_equity(T1_US + 30_000_000).await;
        assert_eq!(h.connector.balance_call_count(), 3);

        // A tick that changed nothing still respects the throttle.
        h.engine.force_venue_equity_refresh_after_a_fill();
        h.refresh_equity(T1_US + 40_000_000).await;
        assert_eq!(
            h.connector.balance_call_count(),
            3,
            "an unchanged account is not a reason to re-read"
        );

        // An exposure this engine cannot trade is still an exposure
        // whose fills move the balance. Adding one is a change...
        h.engine
            .state
            .unmanaged_positions
            .push(persisted_long(0.04, TODAY));
        h.engine.force_venue_equity_refresh_after_a_fill();
        h.refresh_equity(T1_US + 50_000_000).await;
        assert_eq!(h.connector.balance_call_count(), 4);

        // ...and so is partially flattening it, which `reconcile_unmanaged`
        // does by rewriting the record in place. Counting the records
        // instead of describing them missed exactly this (pairtrade#316
        // Codex review round 3).
        h.engine.state.unmanaged_positions[0].open_size = 0.01;
        h.engine.force_venue_equity_refresh_after_a_fill();
        h.refresh_equity(T1_US + 60_000_000).await;
        assert_eq!(
            h.connector.balance_call_count(),
            5,
            "a partial flatten of an unmanaged exposure is a fill too"
        );

        // Same for a side flip, which also leaves the count alone.
        h.engine.state.unmanaged_positions[0].side = "short".to_string();
        h.engine.force_venue_equity_refresh_after_a_fill();
        h.refresh_equity(T1_US + 70_000_000).await;
        assert_eq!(h.connector.balance_call_count(), 6);
    }

    #[tokio::test]
    async fn unrealized_is_signed_by_side_and_null_without_a_trustworthy_mark() {
        let mut h = harness();
        h.observe_all(T1_US, 1000.0, 1750.0);
        assert_eq!(
            h.engine.unrealized_pnl_mid_estimate(T1_US),
            None,
            "flat is not a zero unrealized"
        );

        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1700.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.05,
            open_size: 0.05,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        let long = h.engine.unrealized_pnl_mid_estimate(T1_US).unwrap();
        assert!((long - 2.5).abs() < 1e-9, "long: {long}");

        h.engine.position.as_mut().unwrap().side = OrderSide::Short;
        let short = h.engine.unrealized_pnl_mid_estimate(T1_US).unwrap();
        assert!((short + 2.5).abs() < 1e-9, "short: {short}");

        // No cost basis at all -- an adopted position of unknown origin.
        h.engine.position.as_mut().unwrap().entry_price_unknown = true;
        assert_eq!(h.engine.unrealized_pnl_mid_estimate(T1_US), None);
        h.engine.position.as_mut().unwrap().entry_price_unknown = false;

        // The mark went stale: fail closed to `None` rather than mark
        // against a price the entry path would itself refuse.
        let stale_now = T1_US + 31_000_000;
        assert_eq!(
            h.engine.unrealized_pnl_mid_estimate(stale_now),
            None,
            "a stale mark must blank the row, not quietly age into it"
        );
        assert_eq!(h.connector.order_count(), 0);
    }

    /// pairtrade#300 Codex review, P1: a `us_primary` change must not
    /// orphan a position still open on the old symbol.
    #[tokio::test]
    async fn a_position_on_a_previous_us_primary_halts_and_keeps_its_record() {
        let mut h = harness();
        h.engine.reconciled = false;
        // Persisted on SNDK; the instance now trades MU.
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(h.engine.state.session_halted, "halt");
        let reason = h
            .engine
            .state
            .session_halt_reason
            .clone()
            .unwrap_or_default();
        assert!(reason.contains("SNDK"), "unexpected: {reason}");
        // Kept -- out of the managed slot, so the configured symbol's own
        // exposure can still be tracked, and still on disk for the operator.
        assert!(h.engine.state.open_position.is_none());
        assert_eq!(
            unmanaged_symbols(&h.engine.state),
            vec!["SNDK".to_string()],
            "the record must be kept, not discarded"
        );
        assert_eq!(
            unmanaged_symbols(&load_state(&h.engine.cfg.state_path)),
            vec!["SNDK".to_string()],
            "and it must be on disk, not only in memory"
        );
        assert!(
            h.engine.position.is_none(),
            "never adopt a symbol this engine cannot send orders for"
        );
        assert_eq!(
            h.connector.order_count(),
            0,
            "and never close the wrong one"
        );
    }

    /// pairtrade#300 Codex review, P2: clearing the record and booking the
    /// close must reach disk together.
    #[tokio::test]
    async fn the_close_and_its_accounting_reach_disk_in_one_write() {
        let mut h = harness();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        h.engine.persist_position();
        h.engine.on_exit(1769.1, T2_US).await;
        // Whatever is on disk must never show "flat" without also showing
        // the trade that made it flat.
        let on_disk = load_state(&h.engine.cfg.state_path);
        assert!(on_disk.open_position.is_none());
        assert_eq!(on_disk.total_trades, 1);
        assert!(on_disk.realized_pnl_session > 0.0);
    }

    /// pairtrade#300 Codex review round 12, P1: a crash inside the
    /// confirmation window must not restart as a clean start.
    #[tokio::test]
    async fn an_entry_in_flight_across_a_restart_is_not_a_clean_start() {
        let mut h = harness();
        h.engine.reconciled = false;
        // What the pre-send marker leaves on disk, with the process
        // having died before the fill was confirmed.
        h.engine.state.entry_in_flight = Some("SNDK".to_string());
        h.engine.state.last_session_date = Some(TODAY.to_string());
        // The account read comes back flat -- which proves nothing.
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(
            h.engine.state.position_unconfirmed,
            "a sent order with no confirmed outcome is an unconfirmed position, not a clean start"
        );
        assert!(h.engine.state.session_halted);
        assert!(
            h.engine.state.entry_in_flight.is_none(),
            "the marker is consumed, not left to repeat"
        );
        assert!(
            load_state(&h.engine.cfg.state_path).position_unconfirmed,
            "and it reaches disk"
        );
        // The position appearing late is then adopted by the existing
        // unconfirmed machinery rather than being missed.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        h.engine.tick().await;
        assert!(h.engine.position.is_some(), "the delayed fill is picked up");
    }

    /// pairtrade#300 Codex review round 13, P1: the in-flight marker
    /// names the symbol it was sent for.
    #[tokio::test]
    async fn an_in_flight_entry_on_a_former_symbol_is_parked_not_rebound() {
        let mut h = harness();
        h.engine.reconciled = false;
        // The order went out for SNDK; us_primary is MU by the time this
        // process starts again.
        h.engine.state.entry_in_flight = Some("SNDK".to_string());
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(
            !h.engine.state.position_unconfirmed,
            "watching MU for an order sent on SNDK would be the wrong instrument"
        );
        assert_eq!(
            unmanaged_symbols(&h.engine.state),
            vec!["SNDK".to_string()],
            "the exposure it may have created must have a record"
        );
        assert!(h.engine.state.session_halted);
        let reason = h
            .engine
            .state
            .session_halt_reason
            .clone()
            .unwrap_or_default();
        assert!(reason.contains("SNDK"), "unexpected: {reason}");
        // And it is visible where an operator looks.
        h.engine.last_status_write_us = 0;
        h.engine.write_status_if_due(T1_US + 60_000_000);
        let status: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&h.engine.cfg.status_path).unwrap())
                .unwrap();
        assert_eq!(status["positions"][0]["symbol"], serde_json::json!("SNDK"));
    }

    /// pairtrade#300 Codex review round 14, P1: one flat snapshot must
    /// not retire the claim for a former symbol.
    #[tokio::test]
    async fn a_former_symbol_claim_is_polled_until_the_exposure_is_seen() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.state.entry_in_flight = Some("SNDK".to_string());
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        // The first read shows nothing -- which proves nothing.
        h.set_now(T1_US);
        h.engine.tick().await;
        assert_eq!(
            h.engine.state.entry_in_flight.as_deref(),
            Some("SNDK"),
            "the claim survives a read that has not seen the fill yet"
        );
        assert!(h.engine.state.session_halted);
        assert!(h.engine.state.unmanaged_positions.is_empty());
        // The status must not read as a trustworthy empty account.
        h.engine.last_status_write_us = 0;
        h.engine.write_status_if_due(T1_US + 60_000_000);
        let status: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&h.engine.cfg.status_path).unwrap())
                .unwrap();
        assert_eq!(status["positions_ready"], serde_json::json!(false));

        // The fill lands late; the next tick finds and parks it.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        h.engine.tick().await;
        assert_eq!(unmanaged_symbols(&h.engine.state), vec!["SNDK".to_string()]);
        assert!(
            h.engine.state.entry_in_flight.is_none(),
            "a sighting resolves the claim into a record"
        );
    }

    /// pairtrade#300 Codex review round 14, P2: only a real market price
    /// may become the replacement leg's basis.
    #[tokio::test]
    async fn a_side_flip_without_a_market_price_adopts_an_unknown_basis() {
        let mut h = harness();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1700.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: true,
            exit_deadline_us: None,
        });
        // The venue gives no entry price and there is no market price:
        // `maybe_exit` would have passed the position's own entry here.
        h.engine.reconcile_side_flip_if_any(
            &ExchangePosition {
                side: OrderSide::Short,
                size: 0.030,
                entry_price: None,
            },
            None,
            "test",
        );
        let pos = h.engine.position.clone().expect("tracked");
        assert_eq!(pos.side, OrderSide::Short);
        assert!(
            pos.entry_price_unknown,
            "the old leg's entry price is not the new leg's basis"
        );
        assert_eq!(
            pos.realized_partial_pnl, 0.0,
            "and the flipped-away leg cannot be valued either"
        );
    }

    /// pairtrade#300 Codex review round 13, P1: parking a live displaced
    /// position halts, even if the earlier halt was already acknowledged.
    #[tokio::test]
    async fn parking_a_displaced_live_position_engages_its_own_halt() {
        let mut h = harness();
        h.engine.reconciled = false;
        let parked = persisted_long(0.057, TODAY); // SNDK
        h.engine.state.unmanaged_positions = vec![parked];
        let mut managed = persisted_long(0.020, TODAY);
        managed.symbol = "BBB".to_string();
        h.engine.state.open_position = Some(managed);
        // An operator already cleared the previous halt.
        h.engine.state.session_halted = false;
        h.engine.state.session_halt_reason = None;
        {
            let mut positions = h.connector.positions.lock().unwrap();
            positions.push(snap("SNDK", "0.057", 1, Some("1756.92")));
            positions.push(snap("BBB", "0.020", 1, Some("20.0")));
        }
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(
            h.engine.state.session_halted,
            "BBB is now unmanaged and still open, which is a halt of its own"
        );
        let reason = h
            .engine
            .state
            .session_halt_reason
            .clone()
            .unwrap_or_default();
        assert!(reason.contains("BBB"), "unexpected: {reason}");
        assert_eq!(unmanaged_symbols(&h.engine.state), vec!["BBB".to_string()]);
    }

    /// pairtrade#300 Codex review round 12, P2: a flipped parked record
    /// is an unknown-origin exposure, not the old one on its old clock.
    #[test]
    fn a_flipped_parked_record_does_not_inherit_the_old_schedule() {
        let record = PersistedPosition {
            symbol: "AAA".to_string(),
            side: "long".to_string(),
            entry_price: 100.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 1.0,
            open_size: 1.0,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            session_date: TODAY.to_string(),
            exit_deadline_us: Some(T2_US),
        };
        let flipped = ExchangePosition {
            side: OrderSide::Short,
            size: 1.0,
            entry_price: Some(90.0),
        };
        let refreshed = refreshed_unmanaged(record.clone(), &flipped, None);
        assert_eq!(refreshed.side, "short");
        assert!(
            refreshed.flatten_asap,
            "the replacement leg is closed at once"
        );
        assert!(refreshed.exit_deadline_us.is_none());
        assert!(refreshed.session_date.is_empty());
        // A same-side refresh keeps the schedule it was entered against.
        let same = ExchangePosition {
            side: OrderSide::Long,
            size: 0.5,
            entry_price: None,
        };
        let kept = refreshed_unmanaged(record, &same, None);
        assert!(!kept.flatten_asap);
        assert_eq!(kept.exit_deadline_us, Some(T2_US));
    }

    /// pairtrade#300 Codex review round 12, P2: the drawdown halt must
    /// not swallow the alert of the anomaly that caused it.
    #[tokio::test]
    async fn a_drawdown_halt_from_booked_pnl_still_alerts() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.state.session_start_equity = 1_000.0;
        h.engine.state.peak_equity = 1_000.0;
        h.engine.cfg.max_session_loss_bps = 50.0;
        let mut saved = persisted_long(0.057, TODAY);
        saved.realized_partial_pnl = -20.0;
        h.engine.state.open_position = Some(saved);
        h.set_now(T1_US);
        h.engine.tick().await;
        // The halt reason is the drawdown (it engaged first), which is
        // exactly why that transition has to announce itself.
        let reason = h
            .engine
            .state
            .session_halt_reason
            .clone()
            .unwrap_or_default();
        assert!(reason.starts_with("session_dd_"), "unexpected: {reason}");
        assert!(h.engine.state.session_halted);
    }

    /// pairtrade#300 Codex review round 11: coming back to a parked
    /// symbol must not lose its record to the one now occupying the slot.
    #[tokio::test]
    async fn returning_to_a_parked_symbol_keeps_its_saved_basis() {
        let mut h = harness();
        h.engine.reconciled = false;
        // A was parked while B was traded; both are still open, and
        // us_primary has just gone back to A (SNDK).
        let mut parked = persisted_long(0.057, TODAY); // SNDK, basis 1756.92
        parked.realized_partial_pnl = 0.42;
        h.engine.state.unmanaged_positions = vec![parked];
        let mut managed = persisted_long(0.020, TODAY);
        managed.symbol = "BBB".to_string();
        h.engine.state.open_position = Some(managed);
        {
            let mut positions = h.connector.positions.lock().unwrap();
            // The venue gives no entry price for SNDK, so a blind
            // adoption would price it at the mid or not at all.
            positions.push(snap("SNDK", "0.057", 1, None));
            positions.push(snap("BBB", "0.020", 1, Some("20.0")));
        }
        h.set_now(T1_US);
        h.engine.tick().await;
        let resumed = h.engine.position.clone().expect("resumed under management");
        assert!(
            (resumed.entry_price - 1756.92).abs() < 1e-9,
            "the saved basis must survive the swap of slots, got {}",
            resumed.entry_price
        );
        assert!((resumed.realized_partial_pnl - 0.42).abs() < 1e-12);
        assert_eq!(
            unmanaged_symbols(&h.engine.state),
            vec!["BBB".to_string()],
            "and the displaced record is parked, not dropped"
        );
    }

    /// pairtrade#300 Codex review round 11: a saved claim this process
    /// has not reconciled is not an empty account.
    #[tokio::test]
    async fn status_shows_a_claim_that_reconciliation_has_not_confirmed() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        h.connector
            .fail_get_positions
            .store(true, std::sync::atomic::Ordering::SeqCst);
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(
            !h.engine.reconciled,
            "the read failed, so it is still unresolved"
        );

        h.engine.last_status_write_us = 0;
        h.engine.write_status_if_due(T1_US + 60_000_000);
        let status: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&h.engine.cfg.status_path).unwrap())
                .unwrap();
        assert_eq!(status["has_position"], serde_json::json!(true));
        assert_eq!(status["positions"][0]["symbol"], serde_json::json!("SNDK"));
        assert_eq!(status["positions_ready"], serde_json::json!(false));
    }

    /// pairtrade#300 Codex review round 10: the record is parked through
    /// the same refresh every other one gets, growth included.
    #[tokio::test]
    async fn parking_a_stale_symbol_records_what_the_venue_now_holds() {
        let mut h = harness();
        h.engine.reconciled = false;
        // Saved long 0.057 on SNDK; the venue now shows a short 0.030,
        // and this is the restart that moves us_primary to MU.
        let mut saved = persisted_long(0.057, TODAY);
        saved.realized_partial_pnl = 0.42;
        h.engine.state.open_position = Some(saved);
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.030", -1, Some("1750.0")));
        h.set_now(T1_US);
        let before = h.engine.state.realized_pnl_session;
        h.engine.tick().await;
        let parked = h
            .engine
            .state
            .unmanaged_positions
            .first()
            .cloned()
            .expect("parked");
        assert_eq!(parked.side, "short", "the alert must name what is there");
        assert!((parked.open_size - 0.030).abs() < 1e-12);
        assert!(
            ((h.engine.state.realized_pnl_session - before) - 0.42).abs() < 1e-12,
            "the retired leg's realized PnL is booked, not carried onto the flip"
        );
        assert_eq!(parked.realized_partial_pnl, 0.0);
    }

    /// pairtrade#300 Codex review round 10: a parked record that grew must
    /// not look fully reconciled, or a later `Resume` prices the added
    /// quantity at the old entry.
    #[test]
    fn refreshing_a_grown_parked_record_does_not_keep_the_old_basis() {
        let record = PersistedPosition {
            symbol: "AAA".to_string(),
            side: "long".to_string(),
            entry_price: 100.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 1.0,
            open_size: 1.0,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            session_date: TODAY.to_string(),
            exit_deadline_us: None,
        };
        let grew = ExchangePosition {
            side: OrderSide::Long,
            size: 2.0,
            entry_price: None,
        };
        // With a mid, the added unit is blended in.
        let blended = refreshed_unmanaged(record.clone(), &grew, Some(120.0));
        assert!(
            (blended.entry_price - 110.0).abs() < 1e-9,
            "{}",
            blended.entry_price
        );
        assert!(blended.entry_price_estimated && !blended.entry_price_unknown);
        // Without one, the basis is unknown rather than the old entry.
        let blind = refreshed_unmanaged(record.clone(), &grew, None);
        assert!(blind.entry_price_unknown);
        // A reduction still keeps the known basis exactly.
        let shrank = ExchangePosition {
            side: OrderSide::Long,
            size: 0.5,
            entry_price: None,
        };
        let kept = refreshed_unmanaged(record, &shrank, Some(120.0));
        assert!((kept.entry_price - 100.0).abs() < 1e-9);
        assert!(!kept.entry_price_estimated && !kept.entry_price_unknown);
    }

    /// pairtrade#300 Codex review round 9, P1: booking an orphaned
    /// record's PnL moves the number the session-loss halt measures.
    #[tokio::test]
    async fn booking_orphaned_pnl_runs_the_drawdown_accounting() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.state.session_start_equity = 1_000.0;
        h.engine.state.peak_equity = 1_000.0;
        h.engine.cfg.max_session_loss_bps = 50.0; // $5 on $1,000
        let mut saved = persisted_long(0.057, TODAY);
        saved.realized_partial_pnl = -20.0; // a 200bps session loss
        h.engine.state.open_position = Some(saved);
        // The exchange is flat: `Vanished`, so the record is booked out.
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!((h.engine.state.realized_pnl_session + 20.0).abs() < 1e-12);
        assert!(
            h.engine.state.max_dd_usd >= 20.0,
            "the drawdown must see it, got {}",
            h.engine.state.max_dd_usd
        );
        assert!(h.engine.state.max_dd_bps >= 50.0);
        assert!(h.engine.state.session_halted);
    }

    /// pairtrade#300 Codex review round 9, P2: a position that grew while
    /// the process was down must not price the added quantity at the old
    /// entry.
    #[tokio::test]
    async fn adopting_a_grown_position_does_not_apply_the_old_basis_to_new_quantity() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.state.open_position = Some(persisted_long(0.050, TODAY)); // basis 1756.92
        h.observe_at("SNDK", 1800.0, T1_US, 0);
        // Same symbol and side, larger, and the exchange gives no basis.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.100", 1, None));
        h.set_now(T1_US);
        h.engine.tick().await;
        let adopted = h.engine.position.clone().expect("adopted");
        let mid = h
            .engine
            .exit_accounting_price("SNDK")
            .expect("the harness feeds a mid")
            .0;
        let expected = (1756.92 * 0.050 + mid * 0.050) / 0.100;
        assert!(
            (adopted.entry_price - expected).abs() < 1e-6,
            "expected the blended basis {expected}, got {}",
            adopted.entry_price
        );
        assert!(adopted.entry_price_estimated);
        assert!(!adopted.entry_price_unknown);

        // With no price of any kind for the added quantity, the basis is
        // unknown rather than the old entry applied to all of it.
        let mut bare = harness();
        bare.engine.reconciled = false;
        bare.engine.state.open_position = Some(persisted_long(0.050, TODAY));
        {
            let mut feed = bare.engine.feed.lock().unwrap();
            feed.latest.clear();
            feed.last_raw_mid.clear();
        }
        bare.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.100", 1, None));
        bare.set_now(T1_US);
        bare.engine.tick().await;
        let blind = bare.engine.position.clone().expect("adopted");
        assert!(
            blind.entry_price_unknown,
            "no price to value the growth with"
        );
    }

    /// pairtrade#300 Codex review round 8: a parked record is what the
    /// operator is told to flatten, so it has to describe what is there.
    #[tokio::test]
    async fn a_parked_record_is_refreshed_from_the_exchange() {
        let mut h = harness();
        h.engine.reconciled = false;
        let mut parked = persisted_long(0.057, TODAY);
        parked.symbol = "AAA".to_string();
        parked.realized_partial_pnl = 0.42;
        h.engine.state.unmanaged_positions = vec![parked];
        // While the service was stopped it flipped and shrank.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("AAA", "0.030", -1, Some("11.0")));
        h.set_now(T1_US);
        let before = h.engine.state.realized_pnl_session;
        h.engine.tick().await;
        let kept = h
            .engine
            .state
            .unmanaged_positions
            .first()
            .cloned()
            .expect("still open, so still kept");
        assert_eq!(kept.side, "short", "the venue decides the side");
        assert!((kept.open_size - 0.030).abs() < 1e-12, "and the size");
        assert!(
            (kept.entry_price - 11.0).abs() < 1e-9,
            "and the basis it reports"
        );
        assert_eq!(
            kept.realized_partial_pnl, 0.0,
            "a flip retires the old leg, so its PnL does not ride along"
        );
        assert!(
            ((h.engine.state.realized_pnl_session - before) - 0.42).abs() < 1e-12,
            "it is booked instead of dropped"
        );
        // What the operator is shown matches.
        h.engine.last_status_write_us = 0;
        h.engine.write_status_if_due(T1_US + 60_000_000);
        let status: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&h.engine.cfg.status_path).unwrap())
                .unwrap();
        assert_eq!(status["positions"][0]["side"], serde_json::json!("short"));
        assert_eq!(status["positions"][0]["size"], serde_json::json!("0.03"));
    }

    /// pairtrade#300 Codex review round 7: every path that discards a
    /// record books what it already realized, and a symbol coming back
    /// under management is resumed rather than re-adopted blind.
    #[tokio::test]
    async fn a_parked_symbol_that_becomes_primary_again_is_resumed() {
        let mut h = harness();
        h.engine.reconciled = false;
        let mut parked = persisted_long(0.057, TODAY);
        parked.realized_partial_pnl = 0.42;
        h.engine.state.unmanaged_positions = vec![parked];
        // us_primary is SNDK again, and the position is still there.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(
            h.engine.state.unmanaged_positions.is_empty(),
            "the record belongs in the managed slot once the engine can trade it again"
        );
        let resumed = h
            .engine
            .position
            .clone()
            .expect("resumed, not adopted blind");
        assert!(
            (resumed.entry_price - 1756.92).abs() < 1e-9,
            "its saved basis survives"
        );
        assert!(
            (resumed.realized_partial_pnl - 0.42).abs() < 1e-12,
            "and so does its partial PnL, rather than being booked and lost"
        );
        assert!(
            !h.engine.state.session_halted,
            "this is a resume, not an anomaly"
        );
    }

    #[tokio::test]
    async fn a_manually_flattened_unmanaged_record_books_what_it_realized() {
        let mut h = harness();
        h.engine.reconciled = false;
        let mut parked = persisted_long(0.057, TODAY);
        parked.symbol = "AAA".to_string();
        parked.realized_partial_pnl = 0.42;
        h.engine.state.unmanaged_positions = vec![parked];
        // The operator flattened it: the venue reports nothing.
        h.set_now(T1_US);
        let before = h.engine.state.realized_pnl_session;
        h.engine.tick().await;
        assert!(h.engine.state.unmanaged_positions.is_empty());
        assert!(
            ((h.engine.state.realized_pnl_session - before) - 0.42).abs() < 1e-12,
            "reductions realized before the symbol change are still money"
        );
    }

    #[tokio::test]
    async fn adopting_over_a_foreign_symbol_record_books_its_partial_pnl() {
        let mut h = harness();
        h.engine.reconciled = false;
        // Saved on AAA (now flat, so the stale-symbol branch does not
        // fire); the configured symbol has an unexplained position.
        let mut saved = persisted_long(0.057, TODAY);
        saved.symbol = "AAA".to_string();
        saved.realized_partial_pnl = 0.42;
        h.engine.state.open_position = Some(saved);
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.030", 1, Some("1756.92")));
        h.set_now(T1_US);
        let before = h.engine.state.realized_pnl_session;
        h.engine.tick().await;
        assert!(h.engine.position.is_some(), "the live position is adopted");
        assert!(
            ((h.engine.state.realized_pnl_session - before) - 0.42).abs() < 1e-12,
            "the replaced record's realized PnL must not vanish with it"
        );
    }

    #[tokio::test]
    async fn a_side_flip_is_durable_before_the_replacement_close_is_sent() {
        let mut h = harness();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1700.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: true,
            exit_deadline_us: None,
        });
        h.engine.persist_position();
        h.engine.reconcile_side_flip_if_any(
            &ExchangePosition {
                side: OrderSide::Short,
                size: 0.030,
                entry_price: Some(1750.0),
            },
            Some(1760.0),
            "test",
        );
        // Disk must already carry the flipped record, not the pre-flip one.
        let on_disk = load_state(&h.engine.cfg.state_path)
            .open_position
            .expect("still claimed");
        assert_eq!(on_disk.side, "short");
        assert!(
            (on_disk.realized_partial_pnl - (1760.0 - 1700.0) * 0.057).abs() < 1e-9,
            "the booked old leg must survive a crash before the close is sent, got {}",
            on_disk.realized_partial_pnl
        );
    }

    #[tokio::test]
    async fn shutdown_names_every_unmanaged_exposure() {
        let mut h = harness();
        let mut a = persisted_long(0.010, TODAY);
        a.symbol = "AAA".to_string();
        let mut b = persisted_long(0.020, TODAY);
        b.symbol = "BBB".to_string();
        h.engine.state.unmanaged_positions = vec![a, b];
        h.engine.position = None;
        h.engine.state.open_position = None;
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::UnreconciledClaim,
            "parked exposures are still claims"
        );
        // Both are named; the assertion that matters is that the count is
        // not 1 (the previous `.first()` behaviour).
        assert_eq!(h.engine.state.unmanaged_positions.len(), 2);
    }

    /// pairtrade#300 Codex review round 6, P1: a second `us_primary`
    /// change must not evict the first stranded exposure.
    #[tokio::test]
    async fn a_second_symbol_change_keeps_both_unmanaged_records() {
        let mut h = harness();
        h.engine.reconciled = false;
        // A is already stranded; B is the position this instance tracked
        // until `us_primary` moved to C.
        let mut a = persisted_long(0.010, TODAY);
        a.symbol = "AAA".to_string();
        h.engine.state.unmanaged_positions = vec![a];
        let mut b = persisted_long(0.020, TODAY);
        b.symbol = "BBB".to_string();
        h.engine.state.open_position = Some(b);
        h.engine.cfg.us_primary_symbol = "CCC".to_string();
        {
            let mut positions = h.connector.positions.lock().unwrap();
            positions.push(snap("AAA", "0.010", 1, Some("10.0")));
            positions.push(snap("BBB", "0.020", 1, Some("20.0")));
        }
        h.set_now(T1_US);
        h.engine.tick().await;
        let mut symbols = unmanaged_symbols(&h.engine.state);
        symbols.sort();
        assert_eq!(
            symbols,
            vec!["AAA".to_string(), "BBB".to_string()],
            "the earlier stranded exposure must not be overwritten by the new one"
        );
        assert!(h.engine.state.session_halted);
        // Both are visible on the dashboard.
        h.engine.last_status_write_us = 0;
        h.engine.write_status_if_due(T1_US + 60_000_000);
        let status: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&h.engine.cfg.status_path).unwrap())
                .unwrap();
        assert_eq!(status["position_count"], serde_json::json!(2));
    }

    /// pairtrade#300 Codex review round 6, P1: the side-flip path is the
    /// third place basis arithmetic happens, and it needs the same guard.
    #[tokio::test]
    async fn a_side_flip_with_no_basis_books_nothing_and_takes_the_new_basis() {
        let mut h = harness();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 0.0,
            entry_price_estimated: true,
            entry_price_unknown: true,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: true,
            exit_deadline_us: None,
        });
        h.engine.reconcile_side_flip_if_any(
            &ExchangePosition {
                side: OrderSide::Short,
                size: 0.030,
                entry_price: Some(1750.0),
            },
            Some(1756.92),
            "test",
        );
        let pos = h.engine.position.clone().expect("still tracked");
        assert_eq!(
            pos.realized_partial_pnl, 0.0,
            "the flipped-away leg had no basis, so its PnL is not measurable"
        );
        assert_eq!(pos.side, OrderSide::Short);
        assert!((pos.entry_price - 1750.0).abs() < 1e-9);
        assert!(
            !pos.entry_price_unknown,
            "the replacement leg has a basis of its own -- the flag must not stick to it"
        );
    }

    /// pairtrade#300 Codex review round 6, P2: reductions realized before
    /// the record vanished are known money.
    #[tokio::test]
    async fn a_vanished_position_books_the_partial_pnl_it_already_realized() {
        let mut h = harness();
        h.engine.reconciled = false;
        let mut saved = persisted_long(0.057, TODAY);
        saved.realized_partial_pnl = 0.42;
        h.engine.state.open_position = Some(saved);
        // The exchange is flat: the remainder closed at a price this
        // process never saw, but the reductions did not.
        h.set_now(T1_US);
        let before = h.engine.state.realized_pnl_session;
        h.engine.tick().await;
        assert!(
            ((h.engine.state.realized_pnl_session - before) - 0.42).abs() < 1e-12,
            "already-realized PnL must not be discarded with the record"
        );
        assert!(h.engine.state.open_position.is_none());
        assert!(h.engine.state.session_halted);
    }

    /// pairtrade#300 Codex review round 5, P1: the unknown-basis guard has
    /// to cover partial reductions, not only the final remainder.
    #[tokio::test]
    async fn a_partial_close_with_no_basis_books_nothing() {
        let mut h = harness();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 0.0,
            entry_price_estimated: true,
            entry_price_unknown: true,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: true,
            exit_deadline_us: None,
        });
        // A reduce-only partially fills once a quote exists.
        h.engine.book_partial_close(
            &ExchangePosition {
                side: OrderSide::Long,
                size: 0.030,
                entry_price: None,
            },
            1756.92,
            "test",
        );
        let pos = h.engine.position.clone().expect("still open");
        assert_eq!(
            pos.realized_partial_pnl, 0.0,
            "a placeholder basis must not fabricate realized PnL for the closed part"
        );
        assert!(
            (pos.open_size - 0.030).abs() < 1e-12,
            "but the size still moves"
        );
        // And the final close books nothing either.
        let before = h.engine.state.realized_pnl_session;
        h.engine.on_exit(1769.10, T2_US).await;
        assert!((h.engine.state.realized_pnl_session - before).abs() < 1e-9);
    }

    /// pairtrade#300 Codex review round 5, P1: a record parked as
    /// unmanaged must stay visible until the exchange says it is gone.
    #[tokio::test]
    async fn an_unmanaged_position_is_rechecked_reported_and_shown() {
        let mut h = harness();
        h.engine.reconciled = false;
        let mut stale = persisted_long(0.057, TODAY);
        stale.symbol = "SNDK".to_string();
        h.engine.state.unmanaged_positions = vec![stale];
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        // The old symbol is still open; the configured one is flat.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(
            h.engine.state.session_halted,
            "a flat configured symbol is not a clean start while this is open"
        );
        assert!(
            !h.engine.state.unmanaged_positions.is_empty(),
            "and it is kept"
        );

        // The dashboard must see the exposure rather than has_position=false.
        h.engine.last_status_write_us = 0;
        h.engine.write_status_if_due(T1_US + 60_000_000);
        let status: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&h.engine.cfg.status_path).unwrap())
                .unwrap();
        assert_eq!(status["has_position"], serde_json::json!(true));
        assert_eq!(status["positions"][0]["symbol"], serde_json::json!("SNDK"));

        // SIGTERM must not say "nothing open" either.
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::UnreconciledClaim
        );

        // Once the venue reports it gone, the claim is cleared.
        h.connector.positions.lock().unwrap().clear();
        h.engine.reconciled = false;
        h.engine.tick().await;
        assert!(h.engine.state.unmanaged_positions.is_empty());
        assert!(
            load_state(&h.engine.cfg.state_path)
                .unmanaged_positions
                .is_empty(),
            "and the clearing reaches disk"
        );
    }

    /// pairtrade#300 Codex review round 4, P1: the configured symbol's own
    /// exposure must still be managed when an old-symbol record is stuck.
    #[tokio::test]
    async fn a_stale_record_does_not_strand_the_configured_symbols_position() {
        let mut h = harness();
        h.engine.reconciled = false;
        // Saved on SNDK; the instance now trades MU -- and the account
        // holds *both*.
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        {
            let mut positions = h.connector.positions.lock().unwrap();
            positions.push(snap("SNDK", "0.057", 1, Some("1756.92")));
            positions.push(snap("MU", "0.030", 1, Some("120.50")));
        }
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(h.engine.state.session_halted, "both anomalies halt");
        let adopted = h
            .engine
            .position
            .clone()
            .expect("the configured symbol's position must be adopted, not stranded");
        assert!(
            adopted.flatten_asap,
            "an unexplained configured-symbol position is closed at once"
        );
        assert!((adopted.size - 0.030).abs() < 1e-12);
        // And the record this engine cannot act on is still kept.
        assert_eq!(unmanaged_symbols(&h.engine.state), vec!["SNDK".to_string()]);
    }

    /// pairtrade#300 Codex review round 4, P1: a placeholder basis must
    /// never be arithmetic'd against a real price.
    #[tokio::test]
    async fn an_adopted_position_with_no_basis_books_no_fabricated_pnl() {
        let mut h = harness();
        h.engine.reconciled = false;
        // Nothing persisted, the exchange reports no entry price, and no
        // WS mid has arrived: adoption has no basis of any kind.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, None));
        {
            let mut feed = h.engine.feed.lock().unwrap();
            feed.latest.clear();
            feed.last_raw_mid.clear();
        }
        h.set_now(T1_US);
        h.engine.tick().await;
        let adopted = h.engine.position.clone().expect("adopted");
        assert!(
            adopted.entry_price_unknown,
            "an absent basis must be recorded as absent, not as 0.0"
        );
        // A quote then arrives and the position closes against it. The
        // old code booked (quote - 0.0) * size -- the entire notional as
        // profit, straight into peak equity and the drawdown halt.
        let before = h.engine.state.realized_pnl_session;
        h.engine.on_exit(1769.10, T2_US).await;
        let booked = h.engine.state.realized_pnl_session - before;
        assert!(
            booked.abs() < 1e-9,
            "only realized PnL may be booked without a basis, got ${booked:.2}"
        );
    }

    /// pairtrade#300 Codex review round 4, P2: a side flip does not
    /// invalidate reductions that were already realized.
    #[tokio::test]
    async fn a_side_flip_books_the_partial_pnl_it_cannot_carry() {
        let mut h = harness();
        h.engine.reconciled = false;
        let mut saved = persisted_long(0.057, TODAY);
        saved.realized_partial_pnl = 0.42;
        h.engine.state.open_position = Some(saved);
        // The exchange now reports the opposite side.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.030", -1, Some("1750.00")));
        h.set_now(T1_US);
        let before = h.engine.state.realized_pnl_session;
        h.engine.tick().await;
        let adopted = h.engine.position.clone().expect("adopted");
        assert_eq!(
            adopted.realized_partial_pnl, 0.0,
            "the old leg's PnL does not belong to the new position"
        );
        assert!(
            ((h.engine.state.realized_pnl_session - before) - 0.42).abs() < 1e-12,
            "but it is money already realized, so it is booked rather than dropped"
        );
    }

    /// pairtrade#300 Codex review round 4, P2: the adopted record and its
    /// halt must land together, as the vanished path already does.
    #[tokio::test]
    async fn an_adopted_position_and_its_halt_reach_disk_in_one_write() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        h.set_now(T1_US);
        let before = h.engine.state_writes;
        h.engine.reconcile_startup(T1_US).await;
        assert_eq!(
            h.engine.state_writes - before,
            1,
            "a crash between the two writes leaves a flatten_asap position with no halt, which \
             the next start reads as a plain Resume"
        );
        let on_disk = load_state(&h.engine.cfg.state_path);
        assert!(on_disk.open_position.is_some());
        assert!(on_disk.session_halted);
    }

    /// pairtrade#300 Codex review round 3, P1: an order was sent and its
    /// outcome never established -- that is not "nothing open".
    #[tokio::test]
    async fn shutdown_reports_an_entry_whose_fill_was_never_confirmed() {
        let mut h = harness();
        h.engine.position = None;
        h.engine.state.open_position = None;
        h.engine.pending = Some(PendingConfirm::Entry {
            side: OrderSide::Long,
            requested: 0.057,
            price: 1756.92,
            epsilon: 0.01,
            notional_usd: 100.0,
            deadline_us: T1_US + 60_000_000,
            after_send_error: None,
            saw_reading: false,
        });
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::UnconfirmedOrder,
            "an accepted order with no confirmed fill must not be reported as nothing open"
        );
        // The sticky marker means the same thing once the window has
        // already given up on the confirmation.
        h.engine.pending = None;
        h.engine.state.position_unconfirmed = true;
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::UnconfirmedOrder
        );
        // Genuinely idle is still reported as such.
        h.engine.state.position_unconfirmed = false;
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::Nothing
        );
    }

    /// pairtrade#300 Codex review round 3, P1: a failed state write is
    /// owed to disk, not forgotten because memory already changed.
    #[tokio::test]
    async fn a_failed_position_write_is_retried_until_it_lands() {
        let mut h = harness();
        // Point the state file at a path that cannot be written (its
        // parent is a file, not a directory), then change the position.
        let blocked = h.engine.cfg.state_path.with_extension("blocked");
        std::fs::write(&blocked, b"not a directory").unwrap();
        let good_path = h.engine.cfg.state_path.clone();
        h.engine.cfg.state_path = blocked.join("state.json");
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        h.engine.persist_position();
        assert!(
            h.engine.state_write_pending,
            "a failed write must leave the state owed to disk"
        );
        // The in-memory field is already updated, so the old code's
        // change test would skip every later write. Storage recovers:
        h.engine.cfg.state_path = good_path.clone();
        h.engine.persist_position();
        assert!(!h.engine.state_write_pending);
        let on_disk = load_state(&good_path);
        assert_eq!(
            on_disk.open_position.as_ref().map(|p| p.open_size),
            Some(0.057),
            "the position must reach disk once storage recovers"
        );
    }

    /// pairtrade#300 Codex review round 3, P2: the known basis carries on
    /// position identity, not on whether partial PnL happens to be zero.
    #[tokio::test]
    async fn adopting_a_resized_position_keeps_its_basis_without_partial_pnl() {
        let mut h = harness();
        h.engine.reconciled = false;
        // Same symbol and side, different size, nothing booked yet, and
        // the exchange reports no `avg_entry_price`.
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.030", 1, None));
        h.set_now(T1_US);
        h.engine.tick().await;
        let adopted = h.engine.position.clone().expect("adopted");
        assert!(
            (adopted.entry_price - 1756.92).abs() < 1e-9,
            "the record's own basis describes this same position, got {}",
            adopted.entry_price
        );
        assert!(
            !adopted.entry_price_estimated,
            "and it is a known entry price, not an estimate"
        );
    }

    /// pairtrade#300 Codex review round 3, P2: a DRY_RUN record for
    /// another symbol must block entries, not just decline to resume.
    #[tokio::test]
    async fn dry_run_halts_while_a_foreign_symbol_record_is_unresolved() {
        let mut h = harness();
        h.engine.cfg.dry_run = true;
        h.engine.reconciled = false;
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(
            h.engine.state.session_halted,
            "entries must stay blocked until the incompatible record is resolved"
        );
        assert!(
            !h.engine.entries_allowed(),
            "otherwise a new simulated entry overwrites the kept record"
        );
        assert_eq!(
            h.engine
                .state
                .open_position
                .as_ref()
                .map(|p| p.symbol.clone()),
            Some("SNDK".to_string())
        );
    }

    /// pairtrade#300 Codex review round 3, P2: the cleared record and the
    /// halt it demands must reach disk in the same write.
    #[tokio::test]
    async fn a_vanished_position_clears_and_halts_in_one_write() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        // The exchange is flat: closed at a price this process never saw.
        h.set_now(T1_US);
        let before = h.engine.state_writes;
        h.engine.reconcile_startup(T1_US).await;
        assert_eq!(
            h.engine.state_writes - before,
            1,
            "the cleared record and the halt must land together: two writes leave a crash \
             window where disk says 'flat, nothing to reconcile'"
        );
        let on_disk = load_state(&h.engine.cfg.state_path);
        assert!(on_disk.open_position.is_none(), "the record is cleared");
        assert!(
            on_disk.session_halted,
            "and disk must never show a clean flat start without the halt"
        );
        assert!(on_disk.session_halt_reason.is_some());
    }

    /// pairtrade#300 Codex review round 2, P2: a calendar or
    /// `exit_deadline_secs` change must not move the deadline out from
    /// under an already-open position.
    #[tokio::test]
    async fn a_resumed_position_keeps_its_own_exit_deadline() {
        let mut h = harness();
        h.engine.reconciled = false;
        // Saved against a deadline much earlier than the one today's
        // window would produce (t2 + 900 s): the restart happens *before*
        // that saved deadline, so the position resumes normally rather
        // than as `flatten_asap` -- and then the saved deadline passes
        // while today's t2 is still hours away.
        let mut saved = persisted_long(0.057, TODAY);
        saved.exit_deadline_us = Some(T1_US + 120_000_000);
        h.engine.state.open_position = Some(saved);
        h.engine.state.last_session_date = Some(TODAY.to_string());
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        let before = T1_US + 60_000_000;
        h.set_now(before);
        h.observe_all(before, 190.0, 1760.0);
        h.engine.tick().await;
        let resumed = h.engine.position.clone().expect("resumed");
        assert!(
            !resumed.flatten_asap,
            "resumed on schedule: its own deadline has not passed yet"
        );
        assert_eq!(h.connector.order_count(), 0, "and nothing closes yet");
        // Past the saved deadline, still long before today's t2.
        let after = T1_US + 180_000_000;
        assert!(after < T2_US, "the test only means anything before t2");
        h.set_now(after);
        h.observe_all(after, 190.0, 1760.0);
        h.engine.tick().await;
        assert_eq!(
            h.connector.order_count(),
            1,
            "its own deadline passed, so it must close rather than wait for today's t2"
        );
        let orders = h.connector.orders.lock().unwrap();
        assert_eq!(orders[0].2, OrderSide::Short);
        assert!(orders[0].3, "reduce_only");
    }

    /// pairtrade#300 Codex review round 2, P2: PnL booked by reductions
    /// before a shutdown must not vanish when the remainder is adopted.
    #[tokio::test]
    async fn adopting_a_resized_position_keeps_the_partial_pnl_already_booked() {
        let mut h = harness();
        h.engine.reconciled = false;
        let mut saved = persisted_long(0.057, TODAY);
        saved.realized_partial_pnl = 0.42;
        h.engine.state.open_position = Some(saved);
        // The exchange holds a different quantity: same symbol, same side.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.030", 1, None));
        h.set_now(T1_US);
        h.engine.tick().await;
        let adopted = h.engine.position.clone().expect("adopted");
        assert!(
            (adopted.realized_partial_pnl - 0.42).abs() < 1e-12,
            "booked partial PnL must survive the adoption, got {}",
            adopted.realized_partial_pnl
        );
        assert!(
            (adopted.entry_price - 1756.92).abs() < 1e-9,
            "and the basis it was booked against, got {}",
            adopted.entry_price
        );
        assert!(h.engine.state.session_halted, "a resize still halts");
    }

    /// pairtrade#300 Codex review round 2, P2: DRY_RUN must not relabel a
    /// saved position onto a newly configured symbol.
    #[tokio::test]
    async fn dry_run_refuses_to_relabel_a_saved_position_onto_a_new_symbol() {
        let mut h = harness();
        h.engine.cfg.dry_run = true;
        h.engine.reconciled = false;
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.set_now(T1_US);
        h.engine.tick().await;
        assert!(
            h.engine.position.is_none(),
            "never resume a position as a different instrument"
        );
        assert_eq!(
            h.engine
                .state
                .open_position
                .as_ref()
                .map(|p| p.symbol.clone()),
            Some("SNDK".to_string()),
            "and never rewrite the record's symbol"
        );
    }

    /// pairtrade#300 Codex review round 2, P2: "no open position" on
    /// shutdown must not be said while an unreconciled claim exists.
    #[tokio::test]
    async fn shutdown_reports_a_saved_position_that_was_never_reconciled() {
        let mut h = harness();
        h.engine.reconciled = false;
        h.engine.position = None;
        h.engine.state.open_position = Some(persisted_long(0.057, TODAY));
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::UnreconciledClaim,
            "an unreconciled claim must not be reported as 'no open position'"
        );
        // The claim survives the shutdown untouched -- the next start is
        // what resolves it against the exchange.
        assert_eq!(
            h.engine.state.open_position.as_ref().map(|p| p.open_size),
            Some(0.057)
        );
        assert_eq!(h.connector.order_count(), 0, "and nothing was closed");
    }

    /// The mechanism behind the test above: the field sync on its own must
    /// leave the state file untouched, so a crash between clearing the
    /// record and booking the close cannot land the first without the
    /// second (pairtrade#300 Codex review, P2).
    #[tokio::test]
    async fn syncing_the_field_alone_does_not_touch_the_state_file() {
        let mut h = harness();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        h.engine.persist_position();
        let before = std::fs::read_to_string(&h.engine.cfg.state_path).unwrap();
        assert!(before.contains("open_position"));
        h.engine.position = None;
        assert!(h.engine.sync_open_position_field(), "the field did change");
        let after = std::fs::read_to_string(&h.engine.cfg.state_path).unwrap();
        assert_eq!(before, after, "but nothing was written");
    }

    #[tokio::test]
    async fn a_shutdown_signal_leaves_the_open_position_recorded_and_says_so() {
        let mut h = harness();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::TrackedPosition
        );
        // No close was invented, and the record is durable for the next
        // process to resume from.
        assert_eq!(h.connector.order_count(), 0);
        assert_eq!(
            load_state(&h.engine.cfg.state_path)
                .open_position
                .map(|p| p.open_size),
            Some(0.057)
        );
    }

    // Notification channel gate (bot-strategy#968)
    // -----------------------------------------------------------------

    #[test]
    fn a_configured_channel_passes_the_gate_in_both_modes() {
        assert_eq!(notification_gate(true, true, false), Ok(None));
        assert_eq!(notification_gate(false, true, false), Ok(None));
    }

    #[test]
    fn dry_run_may_run_without_notifications_but_says_so() {
        let allowed = notification_gate(true, false, false).expect("DRY_RUN is allowed");
        let reason = allowed.expect("and it reports why");
        assert!(reason.contains("GMAIL_USER"), "unexpected: {reason}");
    }

    #[test]
    fn a_live_run_refuses_to_start_with_no_alert_path() {
        let refused = notification_gate(false, false, false).expect_err("live must refuse");
        assert!(refused.contains("SESSION HALT"), "unexpected: {refused}");
    }

    #[test]
    fn a_live_run_may_opt_out_explicitly_and_it_is_on_the_record() {
        let allowed =
            notification_gate(false, false, true).expect("an explicit opt-out is allowed");
        assert!(allowed.is_some(), "the reason is still reported");
    }

    /// pairtrade#300 Codex review round 14, P1: an operator can RISK_ACK
    /// the "an order went out on a symbol we no longer trade" halt while
    /// the fill is still invisible. When it does appear, the sighting has
    /// to engage its own halt -- otherwise entries are open alongside an
    /// exposure this engine cannot close.
    #[tokio::test]
    async fn a_delayed_former_symbol_fill_re_engages_the_halt() {
        let mut h = harness();
        h.engine.reconciled = true;
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        // The claim survived a flat startup read, and the operator then
        // acknowledged the halt (which clears session_halted and
        // position_unconfirmed, but never the marker).
        h.engine.state.entry_in_flight = Some("SNDK".to_string());
        h.engine.state.session_halted = false;
        h.engine.state.session_halt_reason = None;
        assert!(
            !h.engine.state.session_halted,
            "precondition: the halt this sighting must re-engage is gone"
        );
        // Now the fill shows up.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        h.set_now(T1_US);
        h.engine.poll_unresolved_claims(T1_US).await;
        assert_eq!(
            unmanaged_symbols(&h.engine.state),
            vec!["SNDK".to_string()],
            "the sighted exposure is recorded"
        );
        assert!(
            h.engine.state.session_halted,
            "and the session must be halted again, not assumed to still be"
        );
        assert!(
            !h.engine.entries_allowed(),
            "the halt blocks entries on its own, whatever the marker now says"
        );
        let reason = h
            .engine
            .state
            .session_halt_reason
            .clone()
            .unwrap_or_default();
        assert!(reason.contains("SNDK"), "unexpected: {reason}");
        assert!(
            load_state(&h.engine.cfg.state_path).session_halted,
            "and it reaches disk"
        );
    }

    /// pairtrade#300 Codex review round 14, P1: after a flat first read
    /// the durable marker is the *only* record of a former-symbol order.
    /// Shutdown must name it rather than report an idle process.
    #[tokio::test]
    async fn shutdown_reports_a_retained_former_symbol_claim() {
        let mut h = harness();
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.engine.position = None;
        h.engine.state.open_position = None;
        h.engine.state.unmanaged_positions.clear();
        h.engine.pending = None;
        h.engine.state.position_unconfirmed = false;
        h.engine.state.entry_in_flight = Some("SNDK".to_string());
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::UnconfirmedOrder,
            "nothing tracked, nothing saved, but an order on SNDK may still fill"
        );
        let note = h
            .engine
            .unconfirmed_order_note()
            .expect("the marker is a claim");
        assert!(note.contains("SNDK"), "the stored symbol is named: {note}");
        assert!(note.contains("MU"), "and the one it is not: {note}");
        // Both markers at once are both reported, not just the first.
        h.engine.state.position_unconfirmed = true;
        let note = h.engine.unconfirmed_order_note().expect("still a claim");
        assert!(
            note.contains("position_unconfirmed") && note.contains("SNDK"),
            "unexpected: {note}"
        );
        // Genuinely idle is still reported as such.
        h.engine.state.position_unconfirmed = false;
        h.engine.state.entry_in_flight = None;
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::Nothing
        );
    }

    /// pairtrade#300 Codex review round 15, P1: the marker is part of the
    /// entry gate, not only a restart hint. RISK_ACK clears the halt but
    /// never the claim, so `maybe_enter` could open a second exposure
    /// while the first order was still live.
    #[tokio::test]
    async fn a_foreign_in_flight_claim_blocks_entries_even_after_risk_ack() {
        let mut h = harness();
        h.engine.reconciled = true;
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.engine.state.entry_in_flight = Some("SNDK".to_string());
        // Exactly what maybe_clear_halt leaves behind: halt gone,
        // position_unconfirmed gone, marker untouched.
        h.engine.state.session_halted = false;
        h.engine.state.session_halt_reason = None;
        h.engine.state.position_unconfirmed = false;
        assert!(
            !h.engine.entries_allowed(),
            "an unresolved submission must keep entries blocked"
        );
        // The claim resolving is what re-opens them.
        h.engine.state.entry_in_flight = None;
        assert!(h.engine.entries_allowed());
    }

    /// pairtrade#300 Codex review round 18, P1: `halt_session` returns
    /// early when a halt already exists, so in the A -> B -> C flow the
    /// halt naming A was the only gate parked B ever got, and one
    /// RISK_ACK for A released it.
    #[tokio::test]
    async fn a_parked_exposure_blocks_entries_on_its_own() {
        let mut h = harness();
        h.engine.reconciled = true;
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.engine.state.entry_in_flight = None;
        // What the operator's RISK_ACK for A leaves behind: no halt, and
        // B still parked because the venue still reports it.
        h.engine.state.session_halted = false;
        h.engine.state.session_halt_reason = None;
        h.engine.state.position_unconfirmed = false;
        h.engine.state.unmanaged_positions = vec![PersistedPosition {
            symbol: "SNDK".to_string(),
            side: OrderSide::Long.to_string(),
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: 0,
            flatten_asap: true,
            session_date: "2026-09-08".to_string(),
            exit_deadline_us: None,
        }];
        assert!(
            !h.engine.entries_allowed(),
            "an exposure this engine cannot close must gate entries by itself"
        );
        // Only the venue reporting it gone retires the claim.
        h.engine.state.unmanaged_positions.clear();
        assert!(h.engine.entries_allowed());
    }

    /// pairtrade#300 Codex review round 20, P2: DRY_RUN keeps a
    /// foreign-symbol record in `open_position` rather than relabelling
    /// it, and nothing parks it -- so the halt was its only gate, and a
    /// RISK_ACK let the next simulated entry overwrite it.
    #[tokio::test]
    async fn a_saved_record_for_another_symbol_blocks_entries_on_its_own() {
        let mut h = harness();
        h.engine.reconciled = true;
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.engine.state.entry_in_flight = None;
        h.engine.state.unmanaged_positions.clear();
        h.engine.state.session_halted = false;
        h.engine.state.session_halt_reason = None;
        h.engine.state.open_position = Some(PersistedPosition {
            symbol: "SNDK".to_string(),
            side: OrderSide::Long.to_string(),
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: -3.5,
            entered_at_us: T1_US,
            flatten_asap: false,
            session_date: "2026-09-08".to_string(),
            exit_deadline_us: None,
        });
        assert!(
            !h.engine.entries_allowed(),
            "an entry here would overwrite the saved SNDK record and its PnL"
        );
        // A record for the configured symbol is the normal case and does
        // not gate anything.
        if let Some(p) = h.engine.state.open_position.as_mut() {
            p.symbol = "MU".to_string();
        }
        assert!(h.engine.entries_allowed());
    }

    /// pairtrade#300 Codex review round 19, P2: the round-18 gate is
    /// retired by the venue, not by RISK_ACK -- so the venue has to be
    /// asked on every tick, not only at startup. Without this an operator
    /// who flattened a parked exposure by hand could not re-open entries
    /// short of a restart.
    #[tokio::test]
    async fn a_hand_flattened_parked_exposure_retires_without_a_restart() {
        let mut h = harness();
        h.engine.reconciled = true;
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.engine.state.unmanaged_positions = vec![PersistedPosition {
            symbol: "SNDK".to_string(),
            side: OrderSide::Long.to_string(),
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: 0,
            flatten_asap: true,
            session_date: "2026-09-08".to_string(),
            exit_deadline_us: None,
        }];
        h.engine.state.session_halted = false;
        h.engine.state.session_halt_reason = None;
        h.set_now(T1_US);

        // Still open on the venue: the claim is kept and re-halted.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", 1, Some("1756.92")));
        h.engine.poll_unresolved_claims(T1_US).await;
        assert_eq!(unmanaged_symbols(&h.engine.state), vec!["SNDK".to_string()]);
        assert!(h.engine.state.session_halted, "still open, still halted");
        assert!(!h.engine.entries_allowed());

        // The operator flattens it by hand, and RISK_ACKs.
        h.connector.positions.lock().unwrap().clear();
        h.engine.state.session_halted = false;
        h.engine.state.session_halt_reason = None;
        h.engine.poll_unresolved_claims(T1_US).await;
        assert!(
            h.engine.state.unmanaged_positions.is_empty(),
            "the venue says it is gone, so the claim is retired without a restart"
        );
        assert!(
            load_state(&h.engine.cfg.state_path)
                .unmanaged_positions
                .is_empty(),
            "and that reaches disk"
        );
        assert!(h.engine.entries_allowed(), "entries re-open");
    }

    /// pairtrade#300 Codex review round 19, P2: the aggregate block
    /// already names every parked exposure, so the saved-record arm must
    /// not send a second, contradictory alert about the same one.
    #[tokio::test]
    async fn a_parked_exposure_is_not_also_reported_as_unreconciled() {
        let mut h = harness();
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.engine.position = None;
        h.engine.state.open_position = None;
        h.engine.state.entry_in_flight = None;
        h.engine.state.position_unconfirmed = false;
        h.engine.state.unmanaged_positions = vec![PersistedPosition {
            symbol: "SNDK".to_string(),
            side: OrderSide::Long.to_string(),
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: 0,
            flatten_asap: true,
            session_date: "2026-09-08".to_string(),
            exit_deadline_us: None,
        }];
        // The exposure is still classified as a claim, so the exit code
        // and the operator's checklist are unchanged ...
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::UnreconciledClaim
        );
        // ... but nothing invents a second story about it. The
        // unreconciled-record arm must not see it at all: the aggregate
        // block above already named it, accurately.
        assert!(
            h.engine.unreconciled_saved_record().is_none(),
            "a parked record must not also be alerted as never-confirmed"
        );
        // And there is no unconfirmed order here either, so that alert is
        // silent too.
        assert!(h.engine.coexisting_unconfirmed_note().is_none());
        // A genuinely unreconciled *managed* record still takes the arm.
        // `reconciled = false` is what makes it unreconciled: with it
        // true and nothing tracked, `persist_position` would rightly
        // clear the record before the match ever sees it.
        h.engine.reconciled = false;
        h.engine.state.unmanaged_positions.clear();
        h.engine.state.open_position = Some(PersistedPosition {
            symbol: "MU".to_string(),
            side: OrderSide::Long.to_string(),
            entry_price: 100.0,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 1.0,
            open_size: 1.0,
            realized_partial_pnl: 0.0,
            entered_at_us: 0,
            flatten_asap: false,
            session_date: "2026-09-08".to_string(),
            exit_deadline_us: None,
        });
        assert!(h.engine.unreconciled_saved_record().is_some());
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::UnreconciledClaim
        );
    }

    /// pairtrade#300 Codex review round 15, P1: a tracked position does
    /// not make a coexisting in-flight claim go away, and the match arms
    /// return on the first one that applies.
    #[tokio::test]
    async fn shutdown_reports_an_in_flight_claim_alongside_a_tracked_position() {
        let mut h = harness();
        h.engine.cfg.us_primary_symbol = "MU".to_string();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        h.engine.sync_open_position_field();
        // Startup kept old symbol A's flat-read claim while B is managed.
        h.engine.state.entry_in_flight = Some("SNDK".to_string());
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::TrackedPosition,
            "the tracked position still classifies the report"
        );
        // ... and the claim is reported alongside it rather than being
        // swallowed by the arm the tracked position took.
        let note = h
            .engine
            .coexisting_unconfirmed_note()
            .expect("the claim is reported next to the tracked position");
        assert!(note.contains("SNDK"), "unexpected: {note}");
        // With nothing else claimed the last arm reports it itself, so
        // this must stay silent rather than double the alert.
        h.engine.position = None;
        h.engine.state.open_position = None;
        assert!(h.engine.coexisting_unconfirmed_note().is_none());
        assert_eq!(
            h.engine.note_shutdown_signal("SIGTERM"),
            ShutdownReport::UnconfirmedOrder
        );
    }

    /// pairtrade#300 Codex review round 14, P2: `maybe_exit`'s
    /// no-price-of-any-kind fallback is the tracked leg's own entry
    /// price. Confirmation must not hand that to the side-flip path as
    /// the replacement leg's cost basis.
    #[tokio::test]
    async fn an_exit_priced_off_the_fallback_leaves_the_replacement_basis_unknown() {
        let mut h = harness();
        h.engine.position = Some(OpenPosition {
            side: OrderSide::Long,
            entry_price: 1756.92,
            entry_price_estimated: false,
            entry_price_unknown: false,
            size: 0.057,
            open_size: 0.057,
            realized_partial_pnl: 0.0,
            entered_at_us: T1_US,
            flatten_asap: false,
            exit_deadline_us: None,
        });
        h.engine.sync_open_position_field();
        // Exactly what maybe_exit installs when no price of any kind was
        // ever seen: the entry price, flagged as not a market price.
        h.engine.pending = Some(PendingConfirm::Exit {
            exit_price: 1756.92,
            price_is_market: false,
            deadline_us: T1_US + 60_000_000,
            saw_reading: false,
        });
        // The venue reports the opposite side with no cost basis of its own.
        h.connector
            .positions
            .lock()
            .unwrap()
            .push(snap("SNDK", "0.057", -1, None));
        h.set_now(T1_US);
        h.engine.poll_pending_confirm(T1_US).await;
        let pos = h.engine.position.as_ref().expect("the flip is adopted");
        assert_eq!(pos.side, OrderSide::Short, "the replacement leg is taken");
        assert!(
            pos.entry_price_unknown,
            "the old leg's entry must not become the new leg's basis"
        );
    }
}
