//! Cross-venue hedge holder (bot-strategy#1046) — Lighter on Robinhood Chain
//! long / Lighter Core short, held for the weekly points drop.
//!
//! One symbol, two legs of equal size on two Lighter deployments:
//!
//! - **long leg** (default instance `rh`): perp long on `api.rh.lighter.xyz`,
//!   the leg the points program rewards ("hedge your Robinhood Chain longs
//!   on Lighter Core" — the venue's own weekly announcement).
//! - **short leg** (default instance `core`): perp short of the same size on
//!   `mainnet.zklighter.elliot.ai`, so the book carries no BTC delta.
//!
//! Funding on the two deployments has been identical to the hour (bot-
//! strategy#1046 Phase 0: 473/838 h equal, |diff| p95 0.07 bps/h), so the
//! long pays what the short receives and holding costs ~nothing; the only
//! costs are the two taker round trips (~1 bps of one leg) and the venue
//! basis (mean −1.3 bps, sd 1 bps). The bot therefore does as little as
//! possible: build both legs, keep them equal, and get out cleanly.
//!
//! Operator surface (files under `HEDGE_BASE_DIR/hedge`, default
//! /opt/debot-hedge-holder/hedge):
//! - `ARM`         touch (optionally containing a per-leg notional in USD,
//!   else `HEDGE_TARGET_NOTIONAL_USD`) → build the book (consumed).
//! - `DISARM`      touch → close both legs and go to `Exited` (consumed).
//!   DISARM beats ARM when both are present. A halted bot still honours
//!   DISARM: closing is the protective direction.
//! - `KILL_SWITCH` exists → no growth on either leg. Reductions still run.
//! - `RISK_ACK`    touch → clear a halt (consumed).
//!
//! Invariants the tick loop enforces, in this order:
//! 1. **Net exposure**: |long − short| × mark must stay under
//!    `HEDGE_NET_TOLERANCE_USD`. While it is over, exactly one order runs
//!    per tick and it levels the book: while building, the smaller leg is
//!    grown up to the larger one; otherwise (or while halted / KILL_SWITCH)
//!    the larger leg is reduced down to the smaller one. One IOC clip is
//!    the most the book can be lopsided by design: the legs are built and
//!    unwound in alternating clips of at most `HEDGE_CLIP_USD`, and the
//!    second leg of a tick is not sent when the first did not fill. Over
//!    tolerance for `HEDGE_NET_BREACH_TICKS` consecutive ticks → halt.
//! 2. **Liquidation guard**: each venue's equity is compared with its leg's
//!    notional. Below `HEDGE_LIQ_GUARD_PCT` of headroom on either side both
//!    legs are closed and the bot halts (`liq_guard`). Cross-venue margin is
//!    not shared: a long-side gain cannot rescue a short-side liquidation,
//!    so a lopsided liquidation is the one way this book can lose real
//!    money, and the guard fires well before the venue would.
//! 3. **Leverage guard**: growth is refused (halt `leverage`) when a leg's
//!    notional after the order would exceed `HEDGE_MAX_LEVERAGE` × equity.
//!    Checked for the whole tick's orders before the first is sent.
//!
//! `Off` never trades: whatever the venues hold in `Off` (an operator's
//! own position, a live book whose `state.json` was lost) is left alone
//! until an ARM adopts it. A `state.json` written in the other mode
//! (DRY_RUN ↔ live) is dropped to `Off` at startup. While the two venues'
//! marks disagree by more than 2 % nothing is sent and status says why.
//!
//! DRY_RUN (default) never sends an order: fills are assumed at the mark and
//! the book is kept in `state.json`; venue equity is `HEDGE_DRY_RUN_EQUITY_USD`.
//! Live needs `HEDGE_DRY_RUN=false` **and** `HEDGE_LIVE_CONFIRM=1046-G0-PASSED`
//! (the pre-registered G0 readout, bot-strategy#1046), so a stray env flip
//! cannot go live on its own.
//!
//! Status: `status.json` next to the sentinels every tick, mirrored to
//! `HEDGE_STATUS_S3_URI` (if set) every `HEDGE_STATUS_S3_EVERY_SECS`. Fills,
//! arms, halts and exits are appended to `events.jsonl`.

use anyhow::{anyhow, bail, Context, Result};
use debot::directional::{append_jsonl, config_fingerprint, load_json, persist_json, Sentinels};
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use dex_connector::{DexConnector, OrderSide};
use env_logger::Builder;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const BOT: &str = "xvenue_hedge_holder";
const LIVE_CONFIRM_TOKEN: &str = "1046-G0-PASSED";

fn init_logger() {
    Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
                record.level(),
                record.args()
            )
        })
        .init();
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `true` only for a finite value strictly above zero (NaN is not).
fn positive(v: f64) -> bool {
    v.is_finite() && v > 0.0
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

// ------------------------------------------------------------------ config

#[derive(Debug, Clone)]
struct Config {
    dry_run: bool,
    live_confirm: String,
    symbol: String,
    /// Lighter env suffix of the long leg (credentials + endpoints).
    long_instance: String,
    /// Lighter env suffix of the short leg.
    short_instance: String,
    /// Per-leg notional built on ARM (USD) unless the ARM file overrides it.
    target_notional_usd: f64,
    /// Hard cap on the per-leg notional any ARM may request (USD).
    max_notional_usd: f64,
    /// Largest single IOC (USD); the legs grow/shrink in alternating clips.
    clip_usd: f64,
    /// Taker IOC bound, bps past the touch (dex-connector rounds inward).
    taker_slippage_bps: u32,
    /// |long − short| × mark tolerated before growth stops (USD).
    net_tolerance_usd: f64,
    /// Consecutive ticks over tolerance that halt the bot.
    net_breach_ticks: u32,
    /// Venue maintenance-margin fraction (%) — BTC/ETH 1.20 on Lighter
    /// (bot-strategy#909); headroom is measured above it.
    mmr_pct: f64,
    /// Headroom (%) below which both legs are closed.
    liq_guard_pct: f64,
    /// Refuse growth beyond this notional / equity per venue.
    max_leverage: f64,
    dry_run_equity_usd: f64,
    tick_secs: u64,
    status_s3_uri: String,
    status_s3_every_secs: u64,
    /// The points collector's history file (bot-strategy#938); the row
    /// series for the long leg's account feeds the `subsidy` block.
    points_history_path: PathBuf,
    /// Account index of the long leg (`LIGHTER_ACCOUNT_INDEX_<LONG>`),
    /// used only to pick that account's rows out of the points history.
    long_account_index: Option<u64>,
    arm_path: PathBuf,
    disarm_path: PathBuf,
    kill_switch_path: PathBuf,
    risk_ack_path: PathBuf,
    state_path: PathBuf,
    status_path: PathBuf,
    events_path: PathBuf,
}

impl Config {
    fn from_env() -> Result<Self> {
        let base_dir = PathBuf::from(env_string("HEDGE_BASE_DIR", "/opt/debot-hedge-holder"));
        let dir = base_dir.join("hedge");
        let cfg = Self {
            dry_run: env_bool("HEDGE_DRY_RUN", true),
            live_confirm: env_string("HEDGE_LIVE_CONFIRM", ""),
            symbol: env_string("HEDGE_SYMBOL", "BTC").to_ascii_uppercase(),
            long_instance: env_string("HEDGE_LONG_INSTANCE", "rh"),
            short_instance: env_string("HEDGE_SHORT_INSTANCE", "core"),
            target_notional_usd: env_f64("HEDGE_TARGET_NOTIONAL_USD", 20_000.0),
            max_notional_usd: env_f64("HEDGE_MAX_NOTIONAL_USD", 30_000.0),
            clip_usd: env_f64("HEDGE_CLIP_USD", 10_000.0),
            taker_slippage_bps: env_u32("HEDGE_TAKER_SLIPPAGE_BPS", 3),
            net_tolerance_usd: env_f64("HEDGE_NET_TOLERANCE_USD", 500.0),
            net_breach_ticks: env_u32("HEDGE_NET_BREACH_TICKS", 3),
            mmr_pct: env_f64("HEDGE_MMR_PCT", 1.2),
            liq_guard_pct: env_f64("HEDGE_LIQ_GUARD_PCT", 8.0),
            max_leverage: env_f64("HEDGE_MAX_LEVERAGE", 6.0),
            dry_run_equity_usd: env_f64("HEDGE_DRY_RUN_EQUITY_USD", 4_000.0),
            tick_secs: env_u64("HEDGE_TICK_SECS", 30),
            status_s3_uri: env_string("HEDGE_STATUS_S3_URI", ""),
            status_s3_every_secs: env_u64("HEDGE_STATUS_S3_EVERY_SECS", 60),
            points_history_path: PathBuf::from(env_string(
                "HEDGE_POINTS_HISTORY_PATH",
                "/home/ec2-user/debot_status/robinhood-points/points_history.jsonl",
            )),
            long_account_index: {
                let suffix = env_string("HEDGE_LONG_INSTANCE", "rh")
                    .to_uppercase()
                    .replace('-', "_");
                std::env::var(format!("LIGHTER_ACCOUNT_INDEX_{suffix}"))
                    .ok()
                    .and_then(|v| v.trim().parse().ok())
            },
            arm_path: dir.join("ARM"),
            disarm_path: dir.join("DISARM"),
            kill_switch_path: dir.join("KILL_SWITCH"),
            risk_ack_path: dir.join("RISK_ACK"),
            state_path: dir.join("state.json"),
            status_path: dir.join("status.json"),
            events_path: dir.join("events.jsonl"),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self
            .long_instance
            .eq_ignore_ascii_case(&self.short_instance)
        {
            bail!("HEDGE_LONG_INSTANCE and HEDGE_SHORT_INSTANCE must differ (two accounts, two venues)");
        }
        if !positive(self.target_notional_usd) || !positive(self.max_notional_usd) {
            bail!("HEDGE_TARGET_NOTIONAL_USD and HEDGE_MAX_NOTIONAL_USD must be > 0");
        }
        if self.target_notional_usd > self.max_notional_usd {
            bail!("HEDGE_TARGET_NOTIONAL_USD exceeds HEDGE_MAX_NOTIONAL_USD");
        }
        if !positive(self.clip_usd) {
            bail!("HEDGE_CLIP_USD must be > 0");
        }
        if self.taker_slippage_bps == 0 || self.taker_slippage_bps > 50 {
            bail!("HEDGE_TAKER_SLIPPAGE_BPS must be 1..=50");
        }
        if !positive(self.liq_guard_pct - self.mmr_pct) {
            bail!("HEDGE_LIQ_GUARD_PCT must exceed HEDGE_MMR_PCT");
        }
        if !self.max_leverage.is_finite() || self.max_leverage < 1.0 {
            bail!("HEDGE_MAX_LEVERAGE must be >= 1");
        }
        if self.tick_secs == 0 || self.net_breach_ticks == 0 {
            bail!("HEDGE_TICK_SECS and HEDGE_NET_BREACH_TICKS must be > 0");
        }
        if !self.dry_run && self.live_confirm != LIVE_CONFIRM_TOKEN {
            bail!(
                "HEDGE_DRY_RUN=false needs HEDGE_LIVE_CONFIRM={LIVE_CONFIRM_TOKEN} \
                 (the bot-strategy#1046 G0 readout is the gate)"
            );
        }
        Ok(())
    }

    fn fingerprint(&self) -> String {
        config_fingerprint(&[
            ("symbol", self.symbol.clone()),
            ("long", self.long_instance.clone()),
            ("short", self.short_instance.clone()),
            ("target", format!("{:.2}", self.target_notional_usd)),
            ("max_notional", format!("{:.2}", self.max_notional_usd)),
            ("clip", format!("{:.2}", self.clip_usd)),
            ("slip_bps", self.taker_slippage_bps.to_string()),
            ("net_tol", format!("{:.2}", self.net_tolerance_usd)),
            ("mmr", format!("{:.3}", self.mmr_pct)),
            ("liq_guard", format!("{:.3}", self.liq_guard_pct)),
            ("max_lev", format!("{:.2}", self.max_leverage)),
        ])
    }
}

// ------------------------------------------------------------------- state

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
enum Mode {
    /// Nothing held, nothing wanted.
    #[default]
    Off,
    /// Book wanted at `target_qty`; the tick loop keeps both legs there.
    On,
    /// Closed by DISARM or a guard; stays here until the next ARM.
    Exited,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    mode: Mode,
    /// Per-leg size the book is being held at (base units).
    target_qty: f64,
    /// Per-leg notional the operator asked for (USD, at ARM).
    target_notional_usd: f64,
    /// DRY_RUN book (live reads the venues instead).
    dry_long_qty: f64,
    dry_short_qty: f64,
    halted: bool,
    halt_reason: Option<String>,
    armed_at: Option<u64>,
    exited_at: Option<u64>,
    exit_reason: Option<String>,
    /// Sum of both venues' equity when the current book was armed.
    equity_at_arm_usd: Option<f64>,
    cycles: u64,
    net_breach_ticks: u32,
    /// Long-leg live points when the current book was armed (from the
    /// collector's history), so `subsidy.units_total` counts this book only.
    points_at_arm: Option<f64>,
    /// UTC day the day-start equity below belongs to, and that equity.
    day_key: Option<String>,
    equity_day_start_usd: Option<f64>,
    process_started_at: Option<u64>,
    /// The mode this state was written in. A state armed under DRY_RUN
    /// must not carry its target into a live start (or vice versa).
    dry_run: Option<bool>,
}

// --------------------------------------------------------------- planning

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq)]
struct Order {
    leg: Leg,
    /// Positive = grow the leg (open more), negative = reduce it.
    qty: f64,
}

/// Held sizes: `long` and `short` are both magnitudes (≥ 0).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Book {
    long: f64,
    short: f64,
}

impl Book {
    fn net(&self) -> f64 {
        self.long - self.short
    }
}

/// The orders one tick may send, in order, given the wanted size and the
/// held sizes. Pure so the invariants can be tested without a venue.
///
/// - A lopsided book (|net| over `net_tol_qty`) is levelled before anything
///   else, with ONE order: while building (the smaller leg is under
///   `target`) the smaller leg is grown up to the larger one; otherwise, or
///   whenever `restricted`, the larger leg is reduced down to the smaller
///   one. A leg the venue just liquidated cannot be grown (the leverage
///   guard halts on it), and the halt is what flips this to the reduction.
/// - With the book level, both legs move toward `target` in the same
///   direction, one clip each, growth long-first and unwinding short-leg
///   first (so an unfilled second order leaves the book long the points
///   leg, never short it).
/// - `restricted` (halt or KILL_SWITCH) permits reductions only.
/// - Sizes under `min_qty` are ignored (dust the venue would reject).
fn plan_orders(
    target: f64,
    book: Book,
    clip_qty: f64,
    min_qty: f64,
    net_tol_qty: f64,
    restricted: bool,
) -> Vec<Order> {
    let clip = |q: f64| q.min(clip_qty);
    let net = book.net();
    if net.abs() > net_tol_qty.max(min_qty) {
        let (larger, smaller, small_held, gap) = if net > 0.0 {
            (Leg::Long, Leg::Short, book.short, net)
        } else {
            (Leg::Short, Leg::Long, book.long, -net)
        };
        let order = if !restricted && small_held < target {
            Order {
                leg: smaller,
                qty: clip(gap.min(target - small_held)),
            }
        } else {
            Order {
                leg: larger,
                qty: -clip(gap),
            }
        };
        return if order.qty.abs() < min_qty {
            vec![]
        } else {
            vec![order]
        };
    }
    let mut out = Vec::new();
    let d_long = target - book.long;
    let d_short = target - book.short;
    if d_long > 0.0 || d_short > 0.0 {
        if restricted {
            return out;
        }
        // Growth: long first, then short, so a partial tick is long-heavy.
        for (leg, d) in [(Leg::Long, d_long), (Leg::Short, d_short)] {
            let q = clip(d.max(0.0));
            if q >= min_qty {
                out.push(Order { leg, qty: q });
            }
        }
    } else {
        // Unwind: short leg first, then long.
        for (leg, d) in [(Leg::Short, d_short), (Leg::Long, d_long)] {
            let q = clip((-d).max(0.0));
            if q >= min_qty {
                out.push(Order { leg, qty: -q });
            }
        }
    }
    out
}

/// A persisted book from the other mode (DRY_RUN ↔ live) is dropped to
/// `Off`: the live venues never held a DRY_RUN book, and a live book must
/// be re-adopted by an explicit ARM after a flip back. Nothing is traded
/// by this — `Off` sends no orders.
fn reconcile_state_mode(mut state: State, dry_run: bool) -> State {
    if let Some(was) = state.dry_run {
        if was != dry_run && state.mode != Mode::Off {
            log::warn!(
                "[STARTUP] state.json was written with dry_run={was}, now dry_run={dry_run}: dropping mode {:?} / target {} to Off — ARM again to (re)build or adopt the book",
                state.mode, state.target_qty
            );
            state.mode = Mode::Off;
            state.target_qty = 0.0;
            state.halted = false;
            state.halt_reason = None;
        }
    }
    state.dry_run = Some(dry_run);
    state
}

/// The second order of a pair, cut to the first leg's actual fill so the
/// book is never more lopsided than what really traded.
fn size_second_leg(next: &Order, first_filled: f64) -> Order {
    let q = next.qty.abs().min(first_filled.max(0.0));
    Order {
        leg: next.leg,
        qty: if next.qty < 0.0 { -q } else { q },
    }
}

/// Growth guard: the leg's notional after the order must stay within
/// `max_leverage` × the venue's equity.
fn leverage_ok(held: f64, add: f64, mark: f64, equity_usd: f64, max_leverage: f64) -> bool {
    (held + add) * mark <= max_leverage * equity_usd
}

/// Base-unit size for `notional_usd` at `mark`, floored to `size_decimals`.
fn qty_for_notional(notional_usd: f64, mark: f64, size_decimals: u32) -> f64 {
    if !positive(mark) || !positive(notional_usd) {
        return 0.0;
    }
    let scale = 10f64.powi(size_decimals as i32);
    ((notional_usd / mark) * scale).floor() / scale
}

/// Distance (percentage points of notional) between the venue's equity and
/// its maintenance requirement on this leg. `None` when nothing is held.
fn liq_headroom_pct(equity_usd: f64, notional_usd: f64, mmr_pct: f64) -> Option<f64> {
    if !positive(notional_usd) {
        return None;
    }
    Some(equity_usd / notional_usd * 100.0 - mmr_pct)
}

/// One account's live-points series out of the collector's JSONL
/// (`robinhood_points_collector.py`): `(ts_unix, live_points_total)` in
/// file order. Rows of other accounts and unparsable lines are skipped.
fn points_series(lines: &str, account_index: u64) -> Vec<(u64, f64)> {
    lines
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("account_index").and_then(|a| a.as_u64()) == Some(account_index))
        .filter_map(|v| {
            Some((
                v.get("ts_unix")?.as_u64()?,
                v.get("live_points_total")?.as_f64()?,
            ))
        })
        .collect()
}

/// Latest value, and the value in force at `at` (last row at or before
/// it), of a points series.
fn points_latest_and_at(series: &[(u64, f64)], at: u64) -> (Option<(u64, f64)>, Option<f64>) {
    let latest = series.last().copied();
    let at_val = series
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= at)
        .map(|(_, v)| *v);
    (latest, at_val)
}

/// The dashboard's `subsidy` block (debot-dashboard `deploy/subsidy-kpi.md`):
/// units and cost since ARM, both counted for this book only. Cost is
/// positive when money was given up. `None` until armed with a points
/// baseline.
fn subsidy_block(
    series: &[(u64, f64)],
    points_at_arm: Option<f64>,
    armed_at: Option<u64>,
    pnl_since_arm_usd: Option<f64>,
    now: u64,
) -> Option<serde_json::Value> {
    let (latest, _) = points_latest_and_at(series, now);
    let (as_of, latest_pts) = latest?;
    let base = points_at_arm?;
    let armed_at = armed_at?;
    let week_ago = now.saturating_sub(7 * 86_400).max(armed_at);
    let (_, at_week) = points_latest_and_at(series, week_ago);
    let units_7d = at_week.map(|v| latest_pts - v.max(base));
    Some(serde_json::json!({
        "unit": "points",
        "units_total": latest_pts - base,
        "units_7d": units_7d,
        "cost_total_usd": pnl_since_arm_usd.map(|p| -p),
        "as_of_ts": as_of,
    }))
}

fn utc_day_key(now: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from(UNIX_EPOCH + Duration::from_secs(now))
        .format("%Y-%m-%d")
        .to_string()
}

fn decimal(v: f64, field: &str) -> Result<Decimal> {
    Decimal::from_f64(v).ok_or_else(|| anyhow!("{field} {v} is not representable"))
}

// ---------------------------------------------------------------- venues

#[derive(Debug, Clone, Copy, Default, Serialize)]
struct VenueSnapshot {
    /// Signed held size on `symbol` (+ long, − short).
    qty: f64,
    equity_usd: f64,
    mark: f64,
}

struct Venue {
    name: &'static str,
    instance: String,
    dex: Arc<DexConnectorBox>,
}

impl Venue {
    async fn mark(&self, symbol: &str) -> Result<(f64, u32, f64)> {
        let t = self
            .dex
            .get_ticker(symbol, None)
            .await
            .map_err(|e| anyhow!("{} get_ticker {symbol}: {e:?}", self.name))?;
        let price = t.price.to_f64().unwrap_or(0.0);
        if !positive(price) {
            bail!("{} ticker {symbol}: non-positive price", self.name);
        }
        Ok((
            price,
            t.size_decimals.unwrap_or(5),
            t.min_order.and_then(|d| d.to_f64()).unwrap_or(0.0),
        ))
    }

    async fn signed_qty(&self, symbol: &str) -> Result<f64> {
        let positions = self
            .dex
            .get_positions()
            .await
            .map_err(|e| anyhow!("{} get_positions: {e:?}", self.name))?;
        Ok(positions
            .iter()
            .filter(|p| p.symbol.eq_ignore_ascii_case(symbol))
            .map(|p| p.size.to_f64().unwrap_or(0.0).abs() * if p.sign < 0 { -1.0 } else { 1.0 })
            .sum())
    }

    async fn equity(&self) -> Result<f64> {
        let b = self
            .dex
            .get_balance(None)
            .await
            .map_err(|e| anyhow!("{} get_balance: {e:?}", self.name))?;
        let e = b.equity.to_f64().unwrap_or(f64::NAN);
        if !e.is_finite() {
            bail!("{} equity {} not finite", self.name, b.equity);
        }
        Ok(e)
    }
}

// ---------------------------------------------------------------- engine

struct Engine {
    cfg: Config,
    state: State,
    sentinels: Sentinels,
    long: Venue,
    short: Venue,
    size_decimals: u32,
    min_qty: f64,
    last_s3_mirror: u64,
    last_snapshot: (VenueSnapshot, VenueSnapshot),
    /// When `last_snapshot` was read from the venues; `None` until the
    /// first successful read. Status carries it so marks frozen by an
    /// outage are datable from the card.
    last_snapshot_at: Option<u64>,
    /// Set while a venue is unreachable or the two marks disagree;
    /// reported in status (`hedge_holder.feed_problem`).
    feed_problem: Option<String>,
}

impl Engine {
    fn persist(&self) {
        if let Err(e) = persist_json(&self.cfg.state_path, &self.state) {
            log::error!("[STATE] persist failed: {e:?}");
        }
    }

    fn event(&self, kind: &str, fields: serde_json::Value) {
        let mut rec =
            serde_json::json!({ "ts": now_secs(), "event": kind, "dry_run": self.cfg.dry_run });
        if let (Some(dst), Some(src)) = (rec.as_object_mut(), fields.as_object()) {
            for (k, v) in src {
                dst.insert(k.clone(), v.clone());
            }
        }
        if let Err(e) = append_jsonl(&self.cfg.events_path, &rec) {
            log::warn!("[EVENT] append failed: {e:?}");
        }
    }

    fn halt(&mut self, reason: String) {
        if self.state.halted {
            return;
        }
        log::error!(
            "[HALT] {reason} — growth blocked until RISK_ACK at {}",
            self.cfg.risk_ack_path.display()
        );
        self.state.halted = true;
        self.state.halt_reason = Some(reason.clone());
        self.event("halt", serde_json::json!({ "reason": reason }));
        self.persist();
    }

    async fn snapshot(&self, venue: &Venue, dry_qty: f64) -> Result<VenueSnapshot> {
        let (mark, _, _) = venue.mark(&self.cfg.symbol).await?;
        if self.cfg.dry_run {
            return Ok(VenueSnapshot {
                qty: dry_qty,
                equity_usd: self.cfg.dry_run_equity_usd,
                mark,
            });
        }
        Ok(VenueSnapshot {
            qty: venue.signed_qty(&self.cfg.symbol).await?,
            equity_usd: venue.equity().await?,
            mark,
        })
    }

    async fn book(&self) -> Result<(Book, VenueSnapshot, VenueSnapshot)> {
        let l = self.snapshot(&self.long, self.state.dry_long_qty).await?;
        let s = self
            .snapshot(&self.short, -self.state.dry_short_qty)
            .await?;
        // The long venue must hold ≥ 0, the short venue ≤ 0: anything else
        // is a foreign position on the account and is treated as zero for
        // the hedge (and reported), never traded against.
        if l.qty < 0.0 {
            log::warn!(
                "[BOOK] long venue holds a SHORT {} {}: not part of the hedge",
                l.qty,
                self.cfg.symbol
            );
        }
        if s.qty > 0.0 {
            log::warn!(
                "[BOOK] short venue holds a LONG {} {}: not part of the hedge",
                s.qty,
                self.cfg.symbol
            );
        }
        Ok((
            Book {
                long: l.qty.max(0.0),
                short: (-s.qty).max(0.0),
            },
            l,
            s,
        ))
    }

    /// One taker IOC on one leg; returns the size the venue reports filled
    /// (position delta), which is what the book is re-planned from.
    async fn execute(&mut self, order: &Order, mark: f64) -> Result<f64> {
        let (venue, side, reduce_only) = match (order.leg, order.qty > 0.0) {
            (Leg::Long, true) => (&self.long, OrderSide::Long, false),
            (Leg::Long, false) => (&self.long, OrderSide::Short, true),
            (Leg::Short, true) => (&self.short, OrderSide::Short, false),
            (Leg::Short, false) => (&self.short, OrderSide::Long, true),
        };
        let qty = order.qty.abs();
        let symbol = self.cfg.symbol.clone();
        if self.cfg.dry_run {
            log::info!(
                "[DRY_RUN] {} {side} {symbol} qty={qty} reduce_only={reduce_only} @~{mark:.1}",
                venue.name
            );
            let signed = if order.qty > 0.0 { qty } else { -qty };
            match order.leg {
                Leg::Long => self.state.dry_long_qty = (self.state.dry_long_qty + signed).max(0.0),
                Leg::Short => {
                    self.state.dry_short_qty = (self.state.dry_short_qty + signed).max(0.0)
                }
            }
            self.event("fill", serde_json::json!({
                "leg": format!("{:?}", order.leg), "venue": venue.instance, "side": format!("{side}"),
                "qty": qty, "reduce_only": reduce_only, "price": mark, "dry_run": true }));
            self.persist();
            return Ok(qty);
        }
        let before = venue.signed_qty(&symbol).await?;
        let size = decimal(qty, "qty")?.round_dp(self.size_decimals);
        let resp = venue
            .dex
            .create_order_taker_ioc(
                &symbol,
                size,
                side,
                self.cfg.taker_slippage_bps,
                reduce_only,
            )
            .await
            .map_err(|e| anyhow!("{} IOC {side} {symbol} {size}: {e:?}", venue.name))?;
        // IOC is final on ack; read twice so a not-yet-visible fill is not
        // mistaken for none (re-requesting it is the one way to overshoot).
        let mut filled = 0.0;
        for wait in [2u64, 4] {
            tokio::time::sleep(Duration::from_secs(wait)).await;
            let after = venue.signed_qty(&symbol).await?;
            filled = (after - before).abs();
            if filled > 0.0 {
                break;
            }
        }
        log::info!(
            "[FILL] {} {side} {symbol} req={qty} filled={filled:.5} reduce_only={reduce_only} limit={} order_id={}",
            venue.name, resp.ordered_price, resp.order_id
        );
        self.event("fill", serde_json::json!({
            "leg": format!("{:?}", order.leg), "venue": venue.instance, "side": format!("{side}"),
            "req": qty, "filled": filled, "reduce_only": reduce_only, "limit": resp.ordered_price.to_string(),
            "order_id": resp.order_id, "mark": mark }));
        Ok(filled)
    }

    fn take_operator_files(&mut self) -> (Option<f64>, bool) {
        let disarm = self.cfg.disarm_path.exists();
        if disarm {
            let _ = std::fs::remove_file(&self.cfg.disarm_path);
        }
        let mut arm = None;
        if self.cfg.arm_path.exists() {
            let body = std::fs::read_to_string(&self.cfg.arm_path).unwrap_or_default();
            let _ = std::fs::remove_file(&self.cfg.arm_path);
            let notional = body.trim().parse::<f64>().ok().filter(|v| *v > 0.0);
            arm = Some(notional.unwrap_or(self.cfg.target_notional_usd));
        }
        if disarm && arm.is_some() {
            log::warn!("[OPERATOR] ARM and DISARM both present: DISARM wins, ARM discarded");
            arm = None;
        }
        (arm, disarm)
    }

    async fn tick(&mut self) -> Result<()> {
        let now = now_secs();
        let kill = self.sentinels.kill_switch_engaged();
        if self.state.halted && self.sentinels.take_risk_ack() {
            log::warn!("[RISK_ACK] halt cleared ({:?})", self.state.halt_reason);
            self.state.halted = false;
            self.state.halt_reason = None;
            self.state.net_breach_ticks = 0;
            self.event("risk_ack", serde_json::json!({}));
            self.persist();
        }

        // A venue that cannot be read (REST 5xx, WS down — Lighter Core
        // was 502/503 for ten minutes on 2026-09-20) leaves the book
        // untouched: no guard can be evaluated, so nothing is sent. Status
        // is still written, from the last good snapshot and with the
        // reason, so the card says why the bot is idle instead of going
        // stale. Operator files stay on disk for the next readable tick.
        let (book, l, s) = match self.book().await {
            Ok(v) => v,
            Err(e) => {
                let reason = format!("venue unreachable: {e}");
                log::error!("[FEED] {reason}");
                self.feed_problem = Some(reason);
                // Only from a real snapshot: before the first successful
                // read the default (all-zero) legs would publish a flat
                // book and a PnL of −equity as fresh figures, over the
                // previous process's last honest status.json. Leaving that
                // file alone (stale) is the truthful option there.
                if self.last_snapshot_at.is_some() {
                    self.write_status(now, kill, &[]);
                }
                return Ok(());
            }
        };
        self.last_snapshot = (l, s);
        self.last_snapshot_at = Some(now);
        let mark = l.mark;
        let day = utc_day_key(now);
        if self.state.day_key.as_deref() != Some(day.as_str()) {
            self.state.day_key = Some(day);
            self.state.equity_day_start_usd = Some(l.equity_usd + s.equity_usd);
            self.persist();
        }

        // Operator files first: a DISARM must register even on a tick that
        // ends up sending nothing (feed check below), so it is acted on as
        // soon as the venues are readable again.
        let (arm, disarm) = self.take_operator_files();
        if disarm && self.state.mode != Mode::Off {
            log::warn!("[DISARM] closing both legs");
            self.state.mode = Mode::Exited;
            self.state.target_qty = 0.0;
            self.state.exited_at = Some(now);
            self.state.exit_reason = Some("disarm".into());
            self.event("disarm", serde_json::json!({}));
            self.persist();
        } else if let Some(notional) = arm {
            if self.state.halted || kill {
                log::warn!(
                    "[ARM] ignored: halted={} kill_switch={kill}",
                    self.state.halted
                );
            } else if notional > self.cfg.max_notional_usd {
                log::warn!(
                    "[ARM] ignored: ${notional:.0} exceeds HEDGE_MAX_NOTIONAL_USD ${:.0}",
                    self.cfg.max_notional_usd
                );
            } else {
                let qty = qty_for_notional(notional, mark, self.size_decimals);
                if qty < self.min_qty {
                    log::warn!(
                        "[ARM] ignored: ${notional:.0} at {mark:.1} is below the venue minimum"
                    );
                } else {
                    // A book the venues already hold (e.g. one opened by
                    // hand) is adopted: the legs are only topped up or
                    // trimmed to `qty` from here on.
                    log::info!(
                        "[ARM] target ${notional:.0}/leg = {qty} {} at {mark:.1} (venues hold long {} / short {})",
                        self.cfg.symbol, book.long, book.short
                    );
                    self.state.mode = Mode::On;
                    self.state.target_qty = qty;
                    self.state.target_notional_usd = notional;
                    self.state.armed_at = Some(now);
                    self.state.exited_at = None;
                    self.state.exit_reason = None;
                    self.state.equity_at_arm_usd = Some(l.equity_usd + s.equity_usd);
                    self.state.points_at_arm = points_latest_and_at(&self.points_series(), now)
                        .0
                        .map(|(_, v)| v);
                    self.state.cycles += 1;
                    self.event(
                        "arm",
                        serde_json::json!({ "notional_usd": notional, "qty": qty, "mark": mark,
                            "adopted_long": book.long, "adopted_short": book.short }),
                    );
                    self.persist();
                }
            }
        }

        // Feed sanity: with one venue's mark off (REST fallback, placeholder
        // ticker) neither the guards nor the sizes can be trusted, so no
        // order goes out. Status still gets written, with the reason, so
        // the card says why the bot is idle instead of going stale.
        let divergence = (l.mark / s.mark - 1.0).abs();
        if divergence > 0.02 {
            let reason = format!(
                "venue marks diverge {:.2}%: long {} vs short {} — no orders on a broken feed",
                divergence * 100.0,
                l.mark,
                s.mark
            );
            log::error!("[FEED] {reason}");
            self.feed_problem = Some(reason);
            self.write_status(now, kill, &[]);
            return Ok(());
        }
        self.feed_problem = None;

        // `Off` never trades. Whatever the venues hold in this mode is not
        // the bot's book: an operator's own position, or a live book whose
        // state.json was lost — either way it waits for an ARM (adopt) or
        // is closed by hand, never unwound because a default target is 0.
        if self.state.mode == Mode::Off {
            if book.long >= self.min_qty || book.short >= self.min_qty {
                log::info!(
                    "[OFF] venues hold long {} / short {} {}; not touched while Off (ARM adopts it)",
                    book.long, book.short, self.cfg.symbol
                );
            }
            self.write_status(now, kill, &[]);
            return Ok(());
        }

        // Liquidation guard: either venue short of headroom → close both.
        for (name, snap, held) in [("long", &l, book.long), ("short", &s, book.short)] {
            if let Some(h) = liq_headroom_pct(snap.equity_usd, held * snap.mark, self.cfg.mmr_pct) {
                if h < self.cfg.liq_guard_pct && self.state.target_qty > 0.0 {
                    let reason = format!(
                        "liq_guard: {name} venue headroom {h:.2}% < {:.2}% (equity ${:.2} vs notional ${:.0}) — closing both legs",
                        self.cfg.liq_guard_pct, snap.equity_usd, held * snap.mark
                    );
                    self.state.mode = Mode::Exited;
                    self.state.target_qty = 0.0;
                    self.state.exited_at = Some(now);
                    self.state.exit_reason = Some("liq_guard".into());
                    self.halt(reason);
                    break;
                } else if h < self.cfg.liq_guard_pct * 2.0 {
                    log::warn!(
                        "[MARGIN] {name} venue headroom {h:.2}% (guard {:.2}%): top up the account",
                        self.cfg.liq_guard_pct
                    );
                }
            }
        }

        // Net exposure watch.
        let net_usd = book.net() * mark;
        if net_usd.abs() > self.cfg.net_tolerance_usd {
            self.state.net_breach_ticks += 1;
            log::warn!(
                "[NET] |long − short| = {:.5} {} (${:.0}) over tolerance ${:.0}, tick {}/{}",
                book.net(),
                self.cfg.symbol,
                net_usd,
                self.cfg.net_tolerance_usd,
                self.state.net_breach_ticks,
                self.cfg.net_breach_ticks
            );
            if self.state.net_breach_ticks >= self.cfg.net_breach_ticks {
                self.halt(format!(
                    "net exposure ${net_usd:.0} over tolerance for {} ticks",
                    self.state.net_breach_ticks
                ));
            }
        } else {
            self.state.net_breach_ticks = 0;
        }

        // Plan (at most one clip per leg per tick).
        let restricted = self.state.halted || kill;
        let clip_qty =
            qty_for_notional(self.cfg.clip_usd, mark, self.size_decimals).max(self.min_qty);
        let net_tol_qty = self.cfg.net_tolerance_usd / mark;
        let mut orders = plan_orders(
            self.state.target_qty,
            book,
            clip_qty,
            self.min_qty,
            net_tol_qty,
            restricted,
        );

        // Leverage guard on every growth order BEFORE anything is sent: a
        // pair whose second leg would be refused must not have its first
        // leg bought (that first leg would only be sold back next tick).
        if !self.cfg.dry_run {
            let equity = |leg: Leg| match leg {
                Leg::Long => l.equity_usd,
                Leg::Short => s.equity_usd,
            };
            let held = |leg: Leg| match leg {
                Leg::Long => book.long,
                Leg::Short => book.short,
            };
            if let Some(o) = orders.iter().find(|o| {
                o.qty > 0.0
                    && !leverage_ok(
                        held(o.leg),
                        o.qty,
                        mark,
                        equity(o.leg),
                        self.cfg.max_leverage,
                    )
            }) {
                self.halt(format!(
                    "leverage: {:?} leg ${:.0} after order > {}x equity ${:.2} — deposit, then RISK_ACK",
                    o.leg,
                    (held(o.leg) + o.qty) * mark,
                    self.cfg.max_leverage,
                    equity(o.leg)
                ));
                orders.clear();
            }
        }

        // Execute. The second order of a pair is cut to what the first
        // actually filled, so a partial on the thin RH book never leaves a
        // full-clip naked leg on the other venue.
        let mut i = 0;
        while i < orders.len() {
            let order = orders[i].clone();
            match self.execute(&order, mark).await {
                Ok(filled) if filled < self.min_qty => {
                    log::warn!(
                        "[EXEC] {:?} {} unfilled (got {filled}) — will retry next tick",
                        order.leg,
                        order.qty
                    );
                    orders.truncate(i + 1);
                    break;
                }
                Ok(filled) => {
                    if let Some(next) = orders.get_mut(i + 1) {
                        *next = size_second_leg(next, filled);
                        if next.qty.abs() < self.min_qty {
                            orders.truncate(i + 1);
                        }
                    }
                }
                Err(e) => {
                    log::error!("[EXEC] {:?} {} failed: {e:?}", order.leg, order.qty);
                    orders.truncate(i + 1);
                    break;
                }
            }
            i += 1;
        }

        // Settle Exited → Off once flat. The re-read can fail like the
        // first one; the orders already sent this tick are still reported
        // and the settle simply waits for the next readable tick.
        if self.state.mode == Mode::Exited && self.state.target_qty == 0.0 {
            match self.book().await {
                Ok((b2, _, _)) => {
                    if b2.long < self.min_qty && b2.short < self.min_qty {
                        log::info!("[EXIT] flat on both venues ({:?})", self.state.exit_reason);
                        self.event(
                            "flat",
                            serde_json::json!({ "reason": self.state.exit_reason }),
                        );
                        self.state.mode = Mode::Off;
                        self.persist();
                    }
                }
                Err(e) => {
                    let reason = format!("venue unreachable: {e}");
                    log::error!("[FEED] {reason}");
                    self.feed_problem = Some(reason);
                }
            }
        }
        self.write_status(now, kill, &orders);
        Ok(())
    }

    fn points_series(&self) -> Vec<(u64, f64)> {
        let Some(idx) = self.cfg.long_account_index else {
            return vec![];
        };
        match std::fs::read_to_string(&self.cfg.points_history_path) {
            Ok(text) => points_series(&text, idx),
            Err(_) => vec![],
        }
    }

    fn write_status(&mut self, now: u64, kill: bool, orders: &[Order]) {
        let (l, s) = self.last_snapshot;
        let series = self.points_series();
        let feed = FeedStatus {
            problem: self.feed_problem.as_deref(),
            snapshot_at: self.last_snapshot_at,
        };
        let status = status_value(
            &self.cfg,
            &self.state,
            &l,
            &s,
            &series,
            feed,
            now,
            kill,
            orders.len(),
        );
        if let Err(e) = persist_json(&self.cfg.status_path, &status) {
            log::warn!("[STATUS] write failed: {e:?}");
            return;
        }
        if !self.cfg.status_s3_uri.is_empty()
            && now.saturating_sub(self.last_s3_mirror) >= self.cfg.status_s3_every_secs
        {
            self.last_s3_mirror = now;
            let out = std::process::Command::new("aws")
                .args([
                    "s3",
                    "cp",
                    &self.cfg.status_path.to_string_lossy(),
                    &self.cfg.status_s3_uri,
                    "--only-show-errors",
                ])
                .output();
            match out {
                Ok(o) if o.status.success() => {}
                Ok(o) => log::warn!(
                    "[STATUS] s3 mirror failed: {}",
                    String::from_utf8_lossy(&o.stderr).trim()
                ),
                Err(e) => log::warn!("[STATUS] s3 mirror spawn failed: {e}"),
            }
        }
    }
}

/// The monitoring projection. Top level follows the pairtrade-like shape
/// debot-dashboard already renders (`ts`/`updated_at`, `pnl_total` =
/// venue equity, `positions`, `subsidy`); everything hedge-specific sits
/// under `hedge_holder`.
/// Whether the venue reads behind `l`/`s` are current: `problem` is set
/// while a venue is unreachable or the marks diverge (no orders go out),
/// `snapshot_at` dates the snapshot the legs/marks come from.
#[derive(Debug, Clone, Copy, Default)]
struct FeedStatus<'a> {
    problem: Option<&'a str>,
    snapshot_at: Option<u64>,
}

#[allow(clippy::too_many_arguments)] // one projection, all of its inputs
fn status_value(
    cfg: &Config,
    state: &State,
    l: &VenueSnapshot,
    s: &VenueSnapshot,
    points: &[(u64, f64)],
    feed: FeedStatus<'_>,
    now: u64,
    kill: bool,
    orders_this_tick: usize,
) -> serde_json::Value {
    let long_qty = l.qty.max(0.0);
    let short_qty = (-s.qty).max(0.0);
    let equity_total = l.equity_usd + s.equity_usd;
    let pnl_since_arm = state.equity_at_arm_usd.map(|e0| equity_total - e0);
    let leg = |name: &str, instance: &str, held: f64, snap: &VenueSnapshot| {
        serde_json::json!({
            "instance": instance,
            "side": name,
            "qty": held,
            "notional_usd": held * snap.mark,
            "mark": snap.mark,
            "equity_usd": snap.equity_usd,
            "liq_headroom_pct": liq_headroom_pct(snap.equity_usd, held * snap.mark, cfg.mmr_pct),
        })
    };
    let mut positions = Vec::new();
    if long_qty > 0.0 {
        positions.push(serde_json::json!({
            "symbol": format!("{} ({})", cfg.symbol, cfg.long_instance), "side": "long",
            "size": format!("{long_qty}"), "entry_price": serde_json::Value::Null }));
    }
    if short_qty > 0.0 {
        positions.push(serde_json::json!({
            "symbol": format!("{} ({})", cfg.symbol, cfg.short_instance), "side": "short",
            "size": format!("{short_qty}"), "entry_price": serde_json::Value::Null }));
    }
    // Split from the top-level literal: one `json!` this deep trips the
    // macro recursion limit.
    let hedge_holder = serde_json::json!({
        "mode": state.mode,
        "halted": state.halted,
        "halt_reason": state.halt_reason,
        "kill_switch": kill,
        "target_qty": state.target_qty,
        "target_notional_usd": state.target_notional_usd,
        "armed_at": state.armed_at,
        "exited_at": state.exited_at,
        "exit_reason": state.exit_reason,
        "cycles": state.cycles,
        "net_qty": long_qty - short_qty,
        "net_usd": (long_qty - short_qty) * l.mark,
        "net_tolerance_usd": cfg.net_tolerance_usd,
        "basis_bps": if l.mark > 0.0 && s.mark > 0.0 { (l.mark / s.mark - 1.0) * 1e4 } else { 0.0 },
        "equity_total_usd": equity_total,
        "equity_at_arm_usd": state.equity_at_arm_usd,
        "pnl_since_arm_usd": pnl_since_arm,
        "points_at_arm": state.points_at_arm,
        "orders_this_tick": orders_this_tick,
        "feed_problem": feed.problem,
        "snapshot_at": feed.snapshot_at,
        "config_fp": cfg.fingerprint(),
        "legs": {
            "long": leg("long", &cfg.long_instance, long_qty, l),
            "short": leg("short", &cfg.short_instance, short_qty, s),
        },
    });
    serde_json::json!({
        "ts": now,
        "updated_at": chrono::DateTime::<chrono::Utc>::from(UNIX_EPOCH + Duration::from_secs(now)).to_rfc3339(),
        "process_started_at": state.process_started_at,
        "bot": BOT,
        "id": BOT,
        "dex": "Lighter (Robinhood Chain) / Lighter Core",
        "symbol": cfg.symbol,
        "dry_run": cfg.dry_run,
        "kill_switch_active": kill,
        "pnl_total": equity_total,
        "pnl_today": state.equity_day_start_usd.map(|e| equity_total - e).unwrap_or(0.0),
        "pnl_source": "equity",
        "positions_ready": true,
        "position_count": positions.len(),
        "has_position": !positions.is_empty(),
        "positions": positions,
        "subsidy": subsidy_block(points, state.points_at_arm, state.armed_at, pnl_since_arm, now),
        "hedge_holder": hedge_holder,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let cfg = Config::from_env()?;
    log::info!(
        "[CONFIG] bot={BOT} dry_run={} symbol={} long={} short={} target=${:.0} max=${:.0} clip=${:.0} slip={}bps net_tol=${:.0} liq_guard={}% max_lev={}x fp={}",
        cfg.dry_run, cfg.symbol, cfg.long_instance, cfg.short_instance, cfg.target_notional_usd,
        cfg.max_notional_usd, cfg.clip_usd, cfg.taker_slippage_bps, cfg.net_tolerance_usd,
        cfg.liq_guard_pct, cfg.max_leverage, cfg.fingerprint()
    );
    let symbols = vec![cfg.symbol.clone()];
    let long_dex = DexConnectorBox::create(
        "lighter",
        cfg.dry_run,
        &symbols,
        Some(cfg.long_instance.as_str()),
    )
    .await
    .with_context(|| format!("init long-leg Lighter connector ({})", cfg.long_instance))?;
    let short_dex = DexConnectorBox::create(
        "lighter",
        cfg.dry_run,
        &symbols,
        Some(cfg.short_instance.as_str()),
    )
    .await
    .with_context(|| format!("init short-leg Lighter connector ({})", cfg.short_instance))?;
    long_dex.start().await.context("start long-leg connector")?;
    short_dex
        .start()
        .await
        .context("start short-leg connector")?;

    let long = Venue {
        name: "long",
        instance: cfg.long_instance.clone(),
        dex: Arc::new(long_dex),
    };
    let short = Venue {
        name: "short",
        instance: cfg.short_instance.clone(),
        dex: Arc::new(short_dex),
    };
    let (mark, size_decimals, min_qty) = long
        .mark(&cfg.symbol)
        .await
        .context("initial long-leg quote")?;
    let (_, sd2, min2) = short
        .mark(&cfg.symbol)
        .await
        .context("initial short-leg quote")?;
    let size_decimals = size_decimals.min(sd2);
    let min_qty = min_qty.max(min2);
    let mut state: State = load_json(&cfg.state_path)?.unwrap_or_default();
    state = reconcile_state_mode(state, cfg.dry_run);
    state.process_started_at = Some(now_secs());
    log::info!(
        "[STARTUP] mode={:?} target_qty={} halted={} mark={mark:.1} size_decimals={size_decimals} min_qty={min_qty}",
        state.mode, state.target_qty, state.halted
    );
    let mut engine = Engine {
        sentinels: Sentinels::new(cfg.kill_switch_path.clone(), cfg.risk_ack_path.clone()),
        cfg,
        state,
        long,
        short,
        size_decimals,
        min_qty,
        last_s3_mirror: 0,
        last_snapshot: Default::default(),
        last_snapshot_at: None,
        feed_problem: None,
    };
    let tick = Duration::from_secs(engine.cfg.tick_secs);
    loop {
        if let Err(e) = engine.tick().await {
            log::error!("[TICK] {e:?}");
        }
        tokio::time::sleep(tick).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(long: f64, short: f64) -> Book {
        Book { long, short }
    }

    #[test]
    fn growth_from_flat_is_long_first_then_short_one_clip_each() {
        let o = plan_orders(0.25, book(0.0, 0.0), 0.12, 0.0002, 0.006, false);
        assert_eq!(
            o,
            vec![
                Order {
                    leg: Leg::Long,
                    qty: 0.12
                },
                Order {
                    leg: Leg::Short,
                    qty: 0.12
                }
            ]
        );
    }

    #[test]
    fn growth_is_blocked_while_restricted_but_unwind_is_not() {
        assert!(plan_orders(0.25, book(0.0, 0.0), 0.12, 0.0002, 0.006, true).is_empty());
        let o = plan_orders(0.0, book(0.25, 0.25), 0.12, 0.0002, 0.006, true);
        assert_eq!(
            o,
            vec![
                Order {
                    leg: Leg::Short,
                    qty: -0.12
                },
                Order {
                    leg: Leg::Long,
                    qty: -0.12
                }
            ]
        );
    }

    #[test]
    fn lopsided_book_is_levelled_before_anything_else() {
        // Mid-build (first clip filled long only): grow the short, one clip.
        let o = plan_orders(0.25, book(0.12, 0.0), 0.12, 0.0002, 0.006, false);
        assert_eq!(
            o,
            vec![Order {
                leg: Leg::Short,
                qty: 0.12
            }]
        );
        // Never past the larger leg, even if target is higher.
        let o = plan_orders(0.25, book(0.05, 0.0), 0.12, 0.0002, 0.006, false);
        assert_eq!(
            o,
            vec![Order {
                leg: Leg::Short,
                qty: 0.05
            }]
        );
        // Restricted (halt / KILL_SWITCH): the larger leg comes down instead.
        let o = plan_orders(0.25, book(0.25, 0.0), 0.12, 0.0002, 0.006, true);
        assert_eq!(
            o,
            vec![Order {
                leg: Leg::Long,
                qty: -0.12
            }]
        );
        // Over-held on one side with the other already at target: reduce.
        let o = plan_orders(0.1, book(0.25, 0.1), 0.12, 0.0002, 0.006, false);
        assert_eq!(
            o,
            vec![Order {
                leg: Leg::Long,
                qty: -0.12
            }]
        );
        // Mirror: long leg gone while restricted → reduce the short, whole gap if within a clip.
        let o = plan_orders(0.25, book(0.0, 0.25), 0.5, 0.0002, 0.006, true);
        assert_eq!(
            o,
            vec![Order {
                leg: Leg::Short,
                qty: -0.25
            }]
        );
        // Exactly one order in every lopsided case (never a second leg).
        for (b, r) in [
            (book(0.2, 0.0), false),
            (book(0.0, 0.2), true),
            (book(0.3, 0.1), false),
        ] {
            assert_eq!(plan_orders(0.25, b, 0.12, 0.0002, 0.006, r).len(), 1);
        }
    }

    #[test]
    fn small_imbalance_inside_tolerance_is_not_repaired_and_growth_tops_up_each_leg() {
        // long 0.12, short 0.115: within 0.006 tolerance → top up both toward 0.25.
        let o = plan_orders(0.25, book(0.12, 0.115), 0.12, 0.0002, 0.006, false);
        assert_eq!(o.len(), 2);
        assert_eq!(o[0].leg, Leg::Long);
        assert!((o[0].qty - 0.12).abs() < 1e-12);
        assert_eq!(o[1].leg, Leg::Short);
        assert!((o[1].qty - 0.12).abs() < 1e-12);
    }

    #[test]
    fn dust_is_ignored_and_final_top_up_is_exact() {
        let o = plan_orders(0.25, book(0.2499, 0.25), 0.12, 0.0002, 0.006, false);
        assert!(o.is_empty(), "0.0001 is below min_qty");
        // 0.01 short of target on the long leg (over tolerance): level it up, exactly.
        let o = plan_orders(0.25, book(0.24, 0.25), 0.12, 0.0002, 0.006, false);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0].leg, Leg::Long);
        assert!((o[0].qty - 0.01).abs() < 1e-12);
        // Same gap inside tolerance: ordinary growth path, still exact.
        let o = plan_orders(0.25, book(0.245, 0.25), 0.12, 0.0002, 0.006, false);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0].leg, Leg::Long);
        assert!((o[0].qty - 0.005).abs() < 1e-12);
    }

    #[test]
    fn unwind_never_overshoots_and_stops_at_flat() {
        let o = plan_orders(0.0, book(0.05, 0.05), 0.12, 0.0002, 0.006, false);
        assert_eq!(
            o,
            vec![
                Order {
                    leg: Leg::Short,
                    qty: -0.05
                },
                Order {
                    leg: Leg::Long,
                    qty: -0.05
                }
            ]
        );
        assert!(plan_orders(0.0, book(0.0, 0.0), 0.12, 0.0002, 0.006, false).is_empty());
    }

    #[test]
    fn second_leg_is_cut_to_the_first_fill() {
        let short = Order {
            leg: Leg::Short,
            qty: 0.12,
        };
        assert_eq!(size_second_leg(&short, 0.12), short);
        assert_eq!(
            size_second_leg(&short, 0.001),
            Order {
                leg: Leg::Short,
                qty: 0.001
            }
        );
        assert_eq!(size_second_leg(&short, 0.5), short);
        let unwind = Order {
            leg: Leg::Long,
            qty: -0.12,
        };
        assert_eq!(
            size_second_leg(&unwind, 0.05),
            Order {
                leg: Leg::Long,
                qty: -0.05
            }
        );
        assert_eq!(size_second_leg(&unwind, -1.0).qty, 0.0);
    }

    #[test]
    fn leverage_guard_uses_notional_after_the_order() {
        // $20k leg on $4,000: 5.0x — refused at 5x, allowed at 6x.
        assert!(!leverage_ok(0.0, 0.247, 81_000.0, 4_000.0, 5.0));
        assert!(leverage_ok(0.0, 0.247, 81_000.0, 4_000.0, 6.0));
        // Held 0.12, adding 0.12 at 81k = $19.4k on $4k → 4.86x ok at 5x.
        assert!(leverage_ok(0.12, 0.12, 81_000.0, 4_000.0, 5.0));
    }

    #[test]
    fn state_from_the_other_mode_is_dropped_to_off() {
        let armed = State {
            mode: Mode::On,
            target_qty: 0.247,
            dry_run: Some(true),
            ..Default::default()
        };
        let live = reconcile_state_mode(armed.clone(), false);
        assert_eq!(live.mode, Mode::Off);
        assert_eq!(live.target_qty, 0.0);
        assert_eq!(live.dry_run, Some(false));
        // Same mode: untouched.
        let same = reconcile_state_mode(armed.clone(), true);
        assert_eq!(same.mode, Mode::On);
        assert_eq!(same.target_qty, 0.247);
        // Pre-field state (no dry_run recorded): kept, now stamped.
        let legacy = State {
            mode: Mode::On,
            target_qty: 0.1,
            dry_run: None,
            ..Default::default()
        };
        let kept = reconcile_state_mode(legacy, false);
        assert_eq!(kept.mode, Mode::On);
        assert_eq!(kept.dry_run, Some(false));
    }

    #[test]
    fn qty_for_notional_floors_to_size_decimals() {
        assert!((qty_for_notional(20_000.0, 81_000.0, 5) - 0.24691).abs() < 1e-12);
        assert_eq!(qty_for_notional(20_000.0, 0.0, 5), 0.0);
        assert_eq!(qty_for_notional(-1.0, 81_000.0, 5), 0.0);
    }

    #[test]
    fn liq_headroom_is_equity_over_notional_less_mmr() {
        assert!((liq_headroom_pct(4_000.0, 20_000.0, 1.2).unwrap() - 18.8).abs() < 1e-9);
        assert_eq!(liq_headroom_pct(4_000.0, 0.0, 1.2), None);
        // 8% guard on a $20k leg fires when equity falls to $1,840.
        assert!(liq_headroom_pct(1_839.0, 20_000.0, 1.2).unwrap() < 8.0);
        assert!(liq_headroom_pct(1_841.0, 20_000.0, 1.2).unwrap() > 8.0);
    }

    fn cfg_for_test() -> Config {
        let dir = std::env::temp_dir().join(format!("hedge-test-{}", std::process::id()));
        Config {
            dry_run: true,
            live_confirm: String::new(),
            symbol: "BTC".into(),
            long_instance: "rh".into(),
            short_instance: "core".into(),
            target_notional_usd: 20_000.0,
            max_notional_usd: 30_000.0,
            clip_usd: 10_000.0,
            taker_slippage_bps: 3,
            net_tolerance_usd: 500.0,
            net_breach_ticks: 3,
            mmr_pct: 1.2,
            liq_guard_pct: 8.0,
            max_leverage: 5.0,
            dry_run_equity_usd: 4_000.0,
            tick_secs: 30,
            status_s3_uri: String::new(),
            status_s3_every_secs: 60,
            points_history_path: dir.join("points_history.jsonl"),
            long_account_index: Some(3209),
            arm_path: dir.join("ARM"),
            disarm_path: dir.join("DISARM"),
            kill_switch_path: dir.join("KILL_SWITCH"),
            risk_ack_path: dir.join("RISK_ACK"),
            state_path: dir.join("state.json"),
            status_path: dir.join("status.json"),
            events_path: dir.join("events.jsonl"),
        }
    }

    #[test]
    fn live_requires_the_g0_token() {
        let mut c = cfg_for_test();
        c.dry_run = false;
        assert!(c.validate().is_err());
        c.live_confirm = LIVE_CONFIRM_TOKEN.into();
        assert!(c.validate().is_ok());
    }

    #[test]
    fn config_rejects_same_instance_for_both_legs() {
        let mut c = cfg_for_test();
        c.short_instance = "RH".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn status_reports_net_basis_and_pnl_since_arm() {
        let cfg = cfg_for_test();
        let state = State {
            mode: Mode::On,
            target_qty: 0.247,
            target_notional_usd: 20_000.0,
            equity_at_arm_usd: Some(8_494.0),
            ..Default::default()
        };
        let l = VenueSnapshot {
            qty: 0.247,
            equity_usd: 4_480.0,
            mark: 81_349.4,
        };
        let s = VenueSnapshot {
            qty: -0.247,
            equity_usd: 4_010.0,
            mark: 81_324.2,
        };
        let feed = FeedStatus {
            problem: None,
            snapshot_at: Some(1_789_812_142),
        };
        let v = status_value(&cfg, &state, &l, &s, &[], feed, 1_789_812_142, false, 0);
        let h = &v["hedge_holder"];
        assert_eq!(h["mode"], "On");
        assert!((h["net_qty"].as_f64().unwrap()).abs() < 1e-12);
        assert!((h["basis_bps"].as_f64().unwrap() - 3.0987).abs() < 0.01);
        assert!((h["pnl_since_arm_usd"].as_f64().unwrap() - (-4.0)).abs() < 1e-9);
        assert_eq!(h["legs"]["short"]["qty"], 0.247);
        assert!(
            (h["legs"]["short"]["liq_headroom_pct"].as_f64().unwrap()
                - (4_010.0 / (0.247 * 81_324.2) * 100.0 - 1.2))
                .abs()
                < 1e-9
        );
        // Dashboard-generic top level: equity as pnl_total, two positions,
        // no subsidy block without a points baseline.
        assert!((v["pnl_total"].as_f64().unwrap() - 8_490.0).abs() < 1e-9);
        assert_eq!(v["position_count"], 2);
        assert_eq!(v["positions"][0]["symbol"], "BTC (rh)");
        assert_eq!(v["positions"][1]["side"], "short");
        assert!(v["subsidy"].is_null());
        assert_eq!(v["updated_at"], "2026-09-19T10:02:22+00:00");
        // A healthy feed: no problem, snapshot dated by this tick.
        assert!(h["feed_problem"].is_null());
        assert_eq!(h["snapshot_at"], 1_789_812_142);
    }

    #[test]
    fn status_carries_the_feed_problem_and_dates_the_frozen_snapshot() {
        // A venue outage (Core 502/503, 2026-09-20 11:02–11:12Z): the tick
        // sends nothing but still publishes, from the last good snapshot,
        // with the reason and the snapshot's own timestamp so the card can
        // say "idle: venue unreachable, marks as of 11:01Z" instead of
        // going stale without a word.
        let cfg = cfg_for_test();
        let state = State {
            mode: Mode::On,
            target_qty: 0.247,
            ..Default::default()
        };
        let l = VenueSnapshot {
            qty: 0.247,
            equity_usd: 4_480.0,
            mark: 81_349.4,
        };
        let s = VenueSnapshot {
            qty: -0.247,
            equity_usd: 4_010.0,
            mark: 81_324.2,
        };
        let reason =
            "venue unreachable: short get_ticker BTC: Transient(\"recentTrades HTTP 503\")";
        let feed = FeedStatus {
            problem: Some(reason),
            snapshot_at: Some(1_789_812_142),
        };
        let v = status_value(&cfg, &state, &l, &s, &[], feed, 1_789_812_742, false, 0);
        let h = &v["hedge_holder"];
        assert_eq!(h["feed_problem"], reason);
        // The frozen snapshot keeps its own date; `ts` is the write.
        assert_eq!(h["snapshot_at"], 1_789_812_142);
        assert_eq!(v["ts"], 1_789_812_742);
        // The legs shown are the last good read, not zeros.
        assert_eq!(h["legs"]["long"]["qty"], 0.247);
        assert_eq!(h["orders_this_tick"], 0);

        // Before any successful read `tick()` does not publish at all (the
        // default snapshot would show a flat book and −equity PnL as fresh
        // figures); a healthy tick then dates itself.
        let healthy = FeedStatus {
            problem: None,
            snapshot_at: Some(1_789_812_742),
        };
        let v1 = status_value(&cfg, &state, &l, &s, &[], healthy, 1_789_812_742, false, 0);
        assert!(v1["hedge_holder"]["feed_problem"].is_null());
        assert_eq!(v1["hedge_holder"]["snapshot_at"], 1_789_812_742);
    }

    const POINTS: &str = concat!(
        "{\"account_index\":3209,\"arm\":\"freq\",\"ts_unix\":100,\"live_points_total\":72.0}\n",
        "{\"account_index\":281474976710500,\"arm\":\"b\",\"ts_unix\":100,\"live_points_total\":4.4}\n",
        "not json\n",
        "{\"account_index\":3209,\"arm\":\"freq\",\"ts_unix\":200,\"live_points_total\":72.5}\n",
        "{\"account_index\":3209,\"arm\":\"freq\",\"ts_unix\":300,\"live_points_total\":79.0}\n",
    );

    #[test]
    fn points_series_picks_one_account_and_skips_junk() {
        let s = points_series(POINTS, 3209);
        assert_eq!(s, vec![(100, 72.0), (200, 72.5), (300, 79.0)]);
        assert_eq!(points_series(POINTS, 281474976710500), vec![(100, 4.4)]);
        assert!(points_series(POINTS, 1).is_empty());
        let (latest, at) = points_latest_and_at(&s, 250);
        assert_eq!(latest, Some((300, 79.0)));
        assert_eq!(at, Some(72.5));
        assert_eq!(points_latest_and_at(&s, 50).1, None);
    }

    #[test]
    fn subsidy_block_counts_points_and_cost_since_arm_only() {
        let s = points_series(POINTS, 3209);
        // Armed at ts 150 with baseline 72.0 (the row in force then); equity −$4 since.
        let v = subsidy_block(&s, Some(72.0), Some(150), Some(-4.0), 300).unwrap();
        assert_eq!(v["unit"], "points");
        assert!((v["units_total"].as_f64().unwrap() - 7.0).abs() < 1e-9);
        assert!((v["cost_total_usd"].as_f64().unwrap() - 4.0).abs() < 1e-9);
        assert_eq!(v["as_of_ts"], 300);
        // 7d window starts at max(now − 7d, armed_at) = armed_at here → same as total.
        assert!((v["units_7d"].as_f64().unwrap() - 7.0).abs() < 1e-9);
        // No baseline (never armed / no collector rows at ARM) → no block.
        assert!(subsidy_block(&s, None, Some(150), Some(-4.0), 300).is_none());
        assert!(subsidy_block(&[], Some(72.0), Some(150), Some(-4.0), 300).is_none());
        // A profitable book reports a negative cost.
        let v = subsidy_block(&s, Some(72.0), Some(150), Some(2.5), 300).unwrap();
        assert!((v["cost_total_usd"].as_f64().unwrap() + 2.5).abs() < 1e-9);
    }

    #[test]
    fn utc_day_key_formats_the_date() {
        assert_eq!(utc_day_key(1_789_812_142), "2026-09-19");
    }
}
