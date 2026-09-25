//! Cross-venue hedge holder (bot-strategy#1046) — Lighter on Robinhood Chain
//! long / Lighter Core short, held for the weekly points drop.
//!
//! Per symbol, two legs of equal size on two Lighter deployments:
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
//! Several symbols (`HEDGE_SYMBOLS=BTC:1.2,META:3,...`, each with the
//! venue's maintenance-margin % for it) are held as independent books on
//! the same two accounts. Lighter margins cross-collateral, so the
//! liquidation and leverage guards are per ACCOUNT over every hedged book
//! (notional weighted by each symbol's MMR), never per symbol; a breach
//! closes every armed book. The legacy one-symbol env (`HEDGE_SYMBOL` +
//! `HEDGE_MMR_PCT`) and its state.json still load unchanged.
//!
//! Operator surface (files under `HEDGE_BASE_DIR/hedge`, default
//! /opt/debot-hedge-holder/hedge):
//! - `ARM`         one symbol: touch (optionally containing a per-leg
//!   notional in USD, else `HEDGE_TARGET_NOTIONAL_USD`); several symbols:
//!   one `SYMBOL USD` line per book to (re)arm → build (consumed). Any bad
//!   line rejects the whole file.
//! - `DISARM`      touch → close every book; `SYMBOL` lines → close those
//!   (consumed). DISARM beats ARM when both are present. A halted bot still
//!   honours DISARM: closing is the protective direction.
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

/// One hedged market and the maintenance-margin fraction the venue charges
/// on it (%, `maintenance_margin_fraction` / 100 in Lighter's
/// orderBookDetails: BTC/ETH 1.2, most single stocks 3 or 6). The guards
/// weight each symbol's notional by it, so it is required per symbol —
/// a stock booked at the BTC rate would overstate the headroom 2.5–5x.
#[derive(Debug, Clone, PartialEq)]
struct SymbolCfg {
    symbol: String,
    mmr_pct: f64,
}

/// `HEDGE_SYMBOLS` = `SYM:MMR[,SYM:MMR...]`, e.g. `BTC:1.2,META:3,AMZN:6`.
/// Symbols are upper-cased; the first one is the primary (the one the
/// single-symbol status fields describe).
fn parse_symbols(spec: &str) -> Result<Vec<SymbolCfg>> {
    let mut out: Vec<SymbolCfg> = Vec::new();
    for item in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (sym, mmr) = item
            .split_once(':')
            .ok_or_else(|| anyhow!("HEDGE_SYMBOLS entry '{item}' needs SYMBOL:MMR_PCT"))?;
        let symbol = sym.trim().to_ascii_uppercase();
        let mmr_pct: f64 = mmr
            .trim()
            .parse()
            .map_err(|_| anyhow!("HEDGE_SYMBOLS entry '{item}': MMR_PCT is not a number"))?;
        if symbol.is_empty() || !positive(mmr_pct) {
            bail!("HEDGE_SYMBOLS entry '{item}': empty symbol or MMR_PCT <= 0");
        }
        if out.iter().any(|c| c.symbol == symbol) {
            bail!("HEDGE_SYMBOLS lists {symbol} twice");
        }
        out.push(SymbolCfg { symbol, mmr_pct });
    }
    if out.is_empty() {
        bail!("HEDGE_SYMBOLS is empty");
    }
    Ok(out)
}

#[derive(Debug, Clone)]
struct Config {
    dry_run: bool,
    live_confirm: String,
    /// Hedged markets, primary first (`HEDGE_SYMBOLS`, or the legacy
    /// `HEDGE_SYMBOL` + `HEDGE_MMR_PCT` pair as a one-symbol list).
    symbols: Vec<SymbolCfg>,
    /// Lighter env suffix of the long leg (credentials + endpoints).
    long_instance: String,
    /// Lighter env suffix of the short leg.
    short_instance: String,
    /// Per-leg notional built on a bare ARM (USD); single-symbol only.
    target_notional_usd: f64,
    /// Hard cap on the per-leg notional any ARM may request, per symbol (USD).
    max_notional_usd: f64,
    /// Largest single IOC (USD); the legs grow/shrink in alternating clips.
    clip_usd: f64,
    /// Taker IOC bound, bps past the touch (dex-connector rounds inward).
    taker_slippage_bps: u32,
    /// |long − short| × mark tolerated per symbol before growth stops (USD).
    net_tolerance_usd: f64,
    /// Consecutive ticks over tolerance that halt the bot.
    net_breach_ticks: u32,
    /// Account headroom (%) below which every armed book is closed.
    liq_guard_pct: f64,
    /// Refuse growth beyond this gross notional / equity per venue.
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
        let symbols = match std::env::var("HEDGE_SYMBOLS")
            .ok()
            .filter(|v| !v.trim().is_empty())
        {
            Some(spec) => parse_symbols(&spec)?,
            None => vec![SymbolCfg {
                symbol: env_string("HEDGE_SYMBOL", "BTC").to_ascii_uppercase(),
                mmr_pct: env_f64("HEDGE_MMR_PCT", 1.2),
            }],
        };
        let cfg = Self {
            dry_run: env_bool("HEDGE_DRY_RUN", true),
            live_confirm: env_string("HEDGE_LIVE_CONFIRM", ""),
            symbols,
            long_instance: env_string("HEDGE_LONG_INSTANCE", "rh"),
            short_instance: env_string("HEDGE_SHORT_INSTANCE", "core"),
            target_notional_usd: env_f64("HEDGE_TARGET_NOTIONAL_USD", 20_000.0),
            max_notional_usd: env_f64("HEDGE_MAX_NOTIONAL_USD", 30_000.0),
            clip_usd: env_f64("HEDGE_CLIP_USD", 10_000.0),
            taker_slippage_bps: env_u32("HEDGE_TAKER_SLIPPAGE_BPS", 3),
            net_tolerance_usd: env_f64("HEDGE_NET_TOLERANCE_USD", 500.0),
            net_breach_ticks: env_u32("HEDGE_NET_BREACH_TICKS", 3),
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
        if self.symbols.is_empty() {
            bail!("no hedged symbol configured");
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
        for c in &self.symbols {
            if !positive(self.liq_guard_pct - c.mmr_pct) {
                bail!(
                    "HEDGE_LIQ_GUARD_PCT {} must exceed {}'s MMR {}",
                    self.liq_guard_pct,
                    c.symbol,
                    c.mmr_pct
                );
            }
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

    fn primary(&self) -> &str {
        &self.symbols[0].symbol
    }

    fn symbol_names(&self) -> Vec<String> {
        self.symbols.iter().map(|c| c.symbol.clone()).collect()
    }

    fn fingerprint(&self) -> String {
        // A one-symbol config hashes exactly the fields it did before the
        // multi-symbol change, so an unchanged BTC deployment keeps its fp.
        let mut fields = vec![
            ("symbol", self.primary().to_string()),
            ("long", self.long_instance.clone()),
            ("short", self.short_instance.clone()),
            ("target", format!("{:.2}", self.target_notional_usd)),
            ("max_notional", format!("{:.2}", self.max_notional_usd)),
            ("clip", format!("{:.2}", self.clip_usd)),
            ("slip_bps", self.taker_slippage_bps.to_string()),
            ("net_tol", format!("{:.2}", self.net_tolerance_usd)),
            ("mmr", format!("{:.3}", self.symbols[0].mmr_pct)),
            ("liq_guard", format!("{:.3}", self.liq_guard_pct)),
            ("max_lev", format!("{:.2}", self.max_leverage)),
        ];
        if self.symbols.len() > 1 {
            fields.push((
                "symbols",
                self.symbols
                    .iter()
                    .map(|c| format!("{}:{:.3}", c.symbol, c.mmr_pct))
                    .collect::<Vec<_>>()
                    .join(","),
            ));
        }
        config_fingerprint(&fields)
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

/// One symbol's book.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
struct SymBook {
    mode: Mode,
    /// Per-leg size the book is being held at (base units).
    target_qty: f64,
    /// Per-leg notional the operator asked for (USD, at ARM).
    target_notional_usd: f64,
    /// DRY_RUN book (live reads the venues instead).
    dry_long_qty: f64,
    dry_short_qty: f64,
    armed_at: Option<u64>,
    exited_at: Option<u64>,
    exit_reason: Option<String>,
    net_breach_ticks: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    /// Per-symbol books, keyed by upper-case symbol.
    #[serde(default)]
    books: std::collections::BTreeMap<String, SymBook>,
    halted: bool,
    halt_reason: Option<String>,
    /// When the current campaign was armed: the first ARM while no book
    /// was On. The equity and points baselines below belong to it.
    armed_at: Option<u64>,
    /// Sum of both venues' equity when the current campaign was armed.
    equity_at_arm_usd: Option<f64>,
    cycles: u64,
    /// Long-leg live points when the current campaign was armed (from the
    /// collector's history), so `subsidy.units_total` counts it only.
    points_at_arm: Option<f64>,
    /// UTC day the day-start equity below belongs to, and that equity.
    day_key: Option<String>,
    equity_day_start_usd: Option<f64>,
    process_started_at: Option<u64>,
    /// The mode this state was written in. A state armed under DRY_RUN
    /// must not carry its target into a live start (or vice versa).
    dry_run: Option<bool>,
    // Single-symbol state.json (before `books`): read once by
    // `migrate_legacy`, never written back.
    #[serde(default, rename = "mode", skip_serializing)]
    legacy_mode: Option<Mode>,
    #[serde(default, rename = "target_qty", skip_serializing)]
    legacy_target_qty: Option<f64>,
    #[serde(default, rename = "target_notional_usd", skip_serializing)]
    legacy_target_notional_usd: Option<f64>,
    #[serde(default, rename = "dry_long_qty", skip_serializing)]
    legacy_dry_long_qty: Option<f64>,
    #[serde(default, rename = "dry_short_qty", skip_serializing)]
    legacy_dry_short_qty: Option<f64>,
    #[serde(default, rename = "exited_at", skip_serializing)]
    legacy_exited_at: Option<u64>,
    #[serde(default, rename = "exit_reason", skip_serializing)]
    legacy_exit_reason: Option<String>,
    #[serde(default, rename = "net_breach_ticks", skip_serializing)]
    legacy_net_breach_ticks: Option<u32>,
}

impl State {
    fn book(&self, symbol: &str) -> SymBook {
        self.books.get(symbol).cloned().unwrap_or_default()
    }

    fn book_mut(&mut self, symbol: &str) -> &mut SymBook {
        self.books.entry(symbol.to_string()).or_default()
    }

    fn any_mode(&self, mode: Mode) -> bool {
        self.books.values().any(|b| b.mode == mode)
    }

    /// Aggregate mode for the card: On if any book is On, else Exited if
    /// any is still closing, else Off.
    fn overall_mode(&self) -> Mode {
        if self.any_mode(Mode::On) {
            Mode::On
        } else if self.any_mode(Mode::Exited) {
            Mode::Exited
        } else {
            Mode::Off
        }
    }
}

/// A pre-`books` state.json described one book, the (then only) symbol's.
/// It becomes that symbol's book — the primary one — so a BTC book armed
/// before the upgrade stays armed across it.
fn migrate_legacy(mut state: State, primary: &str) -> State {
    let legacy = SymBook {
        mode: state.legacy_mode.take().unwrap_or_default(),
        target_qty: state.legacy_target_qty.take().unwrap_or(0.0),
        target_notional_usd: state.legacy_target_notional_usd.take().unwrap_or(0.0),
        dry_long_qty: state.legacy_dry_long_qty.take().unwrap_or(0.0),
        dry_short_qty: state.legacy_dry_short_qty.take().unwrap_or(0.0),
        armed_at: state.armed_at,
        exited_at: state.legacy_exited_at.take(),
        exit_reason: state.legacy_exit_reason.take(),
        net_breach_ticks: state.legacy_net_breach_ticks.take().unwrap_or(0),
    };
    if state.books.is_empty() && legacy != SymBook::default() {
        let armed_at = legacy.armed_at;
        state.books.insert(primary.to_string(), legacy);
        state.armed_at = armed_at;
    }
    state
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
        if was != dry_run && state.books.values().any(|b| b.mode != Mode::Off) {
            for (sym, b) in state.books.iter_mut() {
                if b.mode != Mode::Off {
                    log::warn!(
                        "[STARTUP] state.json was written with dry_run={was}, now dry_run={dry_run}: dropping {sym} mode {:?} / target {} to Off — ARM again to (re)build or adopt the book",
                        b.mode, b.target_qty
                    );
                }
                b.mode = Mode::Off;
                b.target_qty = 0.0;
            }
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

/// Growth guard: the venue's gross notional after the tick's growth must
/// stay within `max_leverage` × its equity.
fn leverage_ok(gross_after_usd: f64, equity_usd: f64, max_leverage: f64) -> bool {
    gross_after_usd <= max_leverage * equity_usd
}

/// USD notional the tick's growth orders add to one venue, over every
/// symbol (each at that venue's own mark). Reductions do not count.
fn planned_growth_usd(plan: &[(String, Vec<Order>)], snap: &Snapshot, leg: Leg) -> f64 {
    plan.iter()
        .flat_map(|(sym, os)| os.iter().map(move |o| (sym, o)))
        .filter(|(_, o)| o.leg == leg && o.qty > 0.0)
        .map(|(sym, o)| {
            o.qty
                * match leg {
                    Leg::Long => snap.long.mark(sym),
                    Leg::Short => snap.short.mark(sym),
                }
        })
        .sum()
}

/// Base-unit size for `notional_usd` at `mark`, floored to `size_decimals`.
fn qty_for_notional(notional_usd: f64, mark: f64, size_decimals: u32) -> f64 {
    if !positive(mark) || !positive(notional_usd) {
        return 0.0;
    }
    let scale = 10f64.powi(size_decimals as i32);
    ((notional_usd / mark) * scale).floor() / scale
}

/// Distance (percentage points of gross notional) between an account's
/// equity and its maintenance requirement over every hedged position:
/// `(equity − Σ notional_i × mmr_i) / Σ notional_i`. With one symbol this
/// is `equity / notional − mmr`. `None` when nothing is held.
fn liq_headroom_pct(equity_usd: f64, legs: &[(f64, f64)]) -> Option<f64> {
    let gross: f64 = legs.iter().map(|(n, _)| n).sum();
    if !positive(gross) {
        return None;
    }
    let maint: f64 = legs.iter().map(|(n, mmr)| n * mmr).sum();
    Some((equity_usd * 100.0 - maint) / gross)
}

/// Per-leg notional (USD) per symbol, as an ARM file asks for it.
type ArmRequest = Vec<(String, f64)>;

/// What an ARM file asks for: per-leg notional (USD) per symbol.
///
/// - one symbol configured: empty → `HEDGE_TARGET_NOTIONAL_USD`, a bare
///   number → that notional (the single-symbol format, unchanged);
/// - otherwise one `SYMBOL USD` line per book (`SYMBOL=USD` and
///   `SYMBOL:USD` also accepted; a bare `SYMBOL` takes the default).
///
/// Any bad line rejects the whole file: a half-applied ARM would build a
/// book the operator did not ask for.
fn parse_arm(body: &str, cfg: &Config) -> std::result::Result<ArmRequest, String> {
    let lines: Vec<&str> = body
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    let single = cfg.symbols.len() == 1;
    if lines.is_empty() {
        return if single {
            Ok(vec![(cfg.primary().to_string(), cfg.target_notional_usd)])
        } else {
            Err("empty ARM with several symbols configured — write `SYMBOL USD` lines".into())
        };
    }
    let mut out: Vec<(String, f64)> = Vec::new();
    for line in lines {
        if let Ok(usd) = line.parse::<f64>() {
            if !single {
                return Err(format!(
                    "bare notional '{line}' is ambiguous with several symbols"
                ));
            }
            if !positive(usd) {
                return Err(format!("notional '{line}' must be > 0"));
            }
            out.push((cfg.primary().to_string(), usd));
            continue;
        }
        let parts: Vec<&str> = line
            .split(|c: char| c.is_whitespace() || c == '=' || c == ':')
            .filter(|p| !p.is_empty())
            .collect();
        let Some(first) = parts.first() else {
            return Err(format!("'{line}': want `SYMBOL USD`"));
        };
        let sym = first.to_ascii_uppercase();
        if !cfg.symbols.iter().any(|c| c.symbol == sym) {
            return Err(format!("{sym} is not in HEDGE_SYMBOLS"));
        }
        let usd = match parts.get(1) {
            None => cfg.target_notional_usd,
            Some(v) => v
                .parse::<f64>()
                .map_err(|_| format!("'{line}': notional is not a number"))?,
        };
        if !positive(usd) || parts.len() > 2 {
            return Err(format!("'{line}': want `SYMBOL USD` with USD > 0"));
        }
        if out.iter().any(|(s, _)| *s == sym) {
            return Err(format!("{sym} appears twice"));
        }
        out.push((sym, usd));
    }
    Ok(out)
}

/// Which books a DISARM file closes: empty → all; else one symbol per
/// line. A line naming no configured symbol closes ALL books — DISARM is
/// the protective direction, so an unreadable one errs towards flat.
fn parse_disarm(body: &str, cfg: &Config) -> Vec<String> {
    let all = cfg.symbol_names();
    let mut out = Vec::new();
    for line in body
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let sym = line.to_ascii_uppercase();
        if !all.contains(&sym) {
            log::warn!("[DISARM] '{line}' is not a configured symbol: closing ALL books");
            return all;
        }
        if !out.contains(&sym) {
            out.push(sym);
        }
    }
    if out.is_empty() {
        all
    } else {
        out
    }
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
/// units and cost since ARM, both counted for this campaign only. Cost is
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

/// One venue read: account equity, and per hedged symbol the signed held
/// size (+ long, − short) and the mark.
#[derive(Debug, Clone, Default, Serialize)]
struct VenueSnapshot {
    equity_usd: f64,
    qty: std::collections::BTreeMap<String, f64>,
    mark: std::collections::BTreeMap<String, f64>,
}

impl VenueSnapshot {
    fn qty(&self, symbol: &str) -> f64 {
        self.qty.get(symbol).copied().unwrap_or(0.0)
    }

    fn mark(&self, symbol: &str) -> f64 {
        self.mark.get(symbol).copied().unwrap_or(0.0)
    }
}

/// Both venues, read in one go.
#[derive(Debug, Clone, Default)]
struct Snapshot {
    long: VenueSnapshot,
    short: VenueSnapshot,
}

impl Snapshot {
    /// Held sizes of one symbol's book. The long venue must hold ≥ 0, the
    /// short venue ≤ 0: anything else is a foreign position on the account,
    /// treated as zero for the hedge and never traded against.
    fn book(&self, symbol: &str) -> Book {
        Book {
            long: self.long.qty(symbol).max(0.0),
            short: (-self.short.qty(symbol)).max(0.0),
        }
    }

    /// Per-venue (notional, mmr) of every hedged book, for the guards.
    fn legs(&self, cfg: &Config, leg: Leg) -> Vec<(f64, f64)> {
        cfg.symbols
            .iter()
            .map(|c| {
                let b = self.book(&c.symbol);
                match leg {
                    Leg::Long => (b.long * self.long.mark(&c.symbol), c.mmr_pct),
                    Leg::Short => (b.short * self.short.mark(&c.symbol), c.mmr_pct),
                }
            })
            .collect()
    }

    fn gross(&self, cfg: &Config, leg: Leg) -> f64 {
        self.legs(cfg, leg).iter().map(|(n, _)| n).sum()
    }

    fn equity(&self, leg: Leg) -> f64 {
        match leg {
            Leg::Long => self.long.equity_usd,
            Leg::Short => self.short.equity_usd,
        }
    }
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

    /// Signed held size per symbol (upper-case), every market on the account.
    async fn positions(&self) -> Result<std::collections::BTreeMap<String, f64>> {
        let positions = self
            .dex
            .get_positions()
            .await
            .map_err(|e| anyhow!("{} get_positions: {e:?}", self.name))?;
        let mut out = std::collections::BTreeMap::new();
        for p in positions {
            let q = p.size.to_f64().unwrap_or(0.0).abs() * if p.sign < 0 { -1.0 } else { 1.0 };
            *out.entry(p.symbol.to_ascii_uppercase()).or_insert(0.0) += q;
        }
        Ok(out)
    }

    async fn signed_qty(&self, symbol: &str) -> Result<f64> {
        Ok(self.positions().await?.get(symbol).copied().unwrap_or(0.0))
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

/// Per-symbol order sizing, the coarser of the two venues.
#[derive(Debug, Clone, Copy)]
struct SymMeta {
    size_decimals: u32,
    min_qty: f64,
}

struct Engine {
    cfg: Config,
    state: State,
    sentinels: Sentinels,
    long: Venue,
    short: Venue,
    meta: std::collections::BTreeMap<String, SymMeta>,
    last_s3_mirror: u64,
    last_snapshot: Snapshot,
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

    fn meta(&self, symbol: &str) -> SymMeta {
        self.meta.get(symbol).copied().unwrap_or(SymMeta {
            size_decimals: 5,
            min_qty: 0.0,
        })
    }

    async fn venue_snapshot(&self, venue: &Venue, leg: Leg) -> Result<VenueSnapshot> {
        let mut snap = VenueSnapshot::default();
        for c in &self.cfg.symbols {
            let (mark, _, _) = venue.mark(&c.symbol).await?;
            snap.mark.insert(c.symbol.clone(), mark);
        }
        if self.cfg.dry_run {
            snap.equity_usd = self.cfg.dry_run_equity_usd;
            for c in &self.cfg.symbols {
                let b = self.state.book(&c.symbol);
                let q = match leg {
                    Leg::Long => b.dry_long_qty,
                    Leg::Short => -b.dry_short_qty,
                };
                snap.qty.insert(c.symbol.clone(), q);
            }
            return Ok(snap);
        }
        let held = venue.positions().await?;
        for c in &self.cfg.symbols {
            snap.qty.insert(
                c.symbol.clone(),
                held.get(&c.symbol).copied().unwrap_or(0.0),
            );
        }
        snap.equity_usd = venue.equity().await?;
        Ok(snap)
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        let snap = Snapshot {
            long: self.venue_snapshot(&self.long, Leg::Long).await?,
            short: self.venue_snapshot(&self.short, Leg::Short).await?,
        };
        for c in &self.cfg.symbols {
            let (l, s) = (snap.long.qty(&c.symbol), snap.short.qty(&c.symbol));
            if l < 0.0 {
                log::warn!(
                    "[BOOK] long venue holds a SHORT {l} {}: not part of the hedge",
                    c.symbol
                );
            }
            if s > 0.0 {
                log::warn!(
                    "[BOOK] short venue holds a LONG {s} {}: not part of the hedge",
                    c.symbol
                );
            }
        }
        Ok(snap)
    }

    /// One taker IOC on one leg of one symbol; returns the size the venue
    /// reports filled (position delta), which is what the book is
    /// re-planned from.
    async fn execute(&mut self, symbol: &str, order: &Order, mark: f64) -> Result<f64> {
        let (venue, side, reduce_only) = match (order.leg, order.qty > 0.0) {
            (Leg::Long, true) => (&self.long, OrderSide::Long, false),
            (Leg::Long, false) => (&self.long, OrderSide::Short, true),
            (Leg::Short, true) => (&self.short, OrderSide::Short, false),
            (Leg::Short, false) => (&self.short, OrderSide::Long, true),
        };
        let qty = order.qty.abs();
        if self.cfg.dry_run {
            log::info!(
                "[DRY_RUN] {} {side} {symbol} qty={qty} reduce_only={reduce_only} @~{mark:.2}",
                venue.name
            );
            let instance = venue.instance.clone();
            let signed = if order.qty > 0.0 { qty } else { -qty };
            let b = self.state.book_mut(symbol);
            match order.leg {
                Leg::Long => b.dry_long_qty = (b.dry_long_qty + signed).max(0.0),
                Leg::Short => b.dry_short_qty = (b.dry_short_qty + signed).max(0.0),
            }
            self.event(
                "fill",
                serde_json::json!({
                "symbol": symbol, "leg": format!("{:?}", order.leg), "venue": instance,
                "side": format!("{side}"), "qty": qty, "reduce_only": reduce_only,
                "price": mark, "dry_run": true }),
            );
            self.persist();
            return Ok(qty);
        }
        let before = venue.signed_qty(symbol).await?;
        let size = decimal(qty, "qty")?.round_dp(self.meta(symbol).size_decimals);
        let resp = venue
            .dex
            .create_order_taker_ioc(symbol, size, side, self.cfg.taker_slippage_bps, reduce_only)
            .await
            .map_err(|e| anyhow!("{} IOC {side} {symbol} {size}: {e:?}", venue.name))?;
        // IOC is final on ack; read twice so a not-yet-visible fill is not
        // mistaken for none (re-requesting it is the one way to overshoot).
        let mut filled = 0.0;
        for wait in [2u64, 4] {
            tokio::time::sleep(Duration::from_secs(wait)).await;
            let after = venue.signed_qty(symbol).await?;
            filled = (after - before).abs();
            if filled > 0.0 {
                break;
            }
        }
        log::info!(
            "[FILL] {} {side} {symbol} req={qty} filled={filled:.5} reduce_only={reduce_only} limit={} order_id={}",
            venue.name, resp.ordered_price, resp.order_id
        );
        self.event(
            "fill",
            serde_json::json!({
            "symbol": symbol, "leg": format!("{:?}", order.leg), "venue": venue.instance,
            "side": format!("{side}"), "req": qty, "filled": filled, "reduce_only": reduce_only,
            "limit": resp.ordered_price.to_string(), "order_id": resp.order_id, "mark": mark }),
        );
        Ok(filled)
    }

    /// (ARM request, DISARM symbols). DISARM beats ARM when both are present.
    fn take_operator_files(&mut self) -> (Option<ArmRequest>, Option<Vec<String>>) {
        let disarm = if self.cfg.disarm_path.exists() {
            let body = std::fs::read_to_string(&self.cfg.disarm_path).unwrap_or_default();
            let _ = std::fs::remove_file(&self.cfg.disarm_path);
            Some(parse_disarm(&body, &self.cfg))
        } else {
            None
        };
        let mut arm = None;
        if self.cfg.arm_path.exists() {
            let body = std::fs::read_to_string(&self.cfg.arm_path).unwrap_or_default();
            let _ = std::fs::remove_file(&self.cfg.arm_path);
            match parse_arm(&body, &self.cfg) {
                Ok(v) => arm = Some(v),
                Err(e) => log::warn!("[ARM] ignored: {e}"),
            }
        }
        if disarm.is_some() && arm.is_some() {
            log::warn!("[OPERATOR] ARM and DISARM both present: DISARM wins, ARM discarded");
            arm = None;
        }
        (arm, disarm)
    }

    fn close_book(&mut self, symbol: &str, now: u64, reason: &str) {
        let b = self.state.book_mut(symbol);
        b.mode = Mode::Exited;
        b.target_qty = 0.0;
        b.exited_at = Some(now);
        b.exit_reason = Some(reason.to_string());
    }

    async fn tick(&mut self) -> Result<()> {
        let now = now_secs();
        let kill = self.sentinels.kill_switch_engaged();
        if self.state.halted && self.sentinels.take_risk_ack() {
            log::warn!("[RISK_ACK] halt cleared ({:?})", self.state.halt_reason);
            self.state.halted = false;
            self.state.halt_reason = None;
            for b in self.state.books.values_mut() {
                b.net_breach_ticks = 0;
            }
            self.event("risk_ack", serde_json::json!({}));
            self.persist();
        }

        // A venue that cannot be read (REST 5xx, WS down — Lighter Core
        // was 502/503 for ten minutes on 2026-09-20) leaves every book
        // untouched: no guard can be evaluated, so nothing is sent. Status
        // is still written, from the last good snapshot and with the
        // reason, so the card says why the bot is idle instead of going
        // stale. Operator files stay on disk for the next readable tick.
        let snap = match self.snapshot().await {
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
                    self.write_status(now, kill, 0);
                }
                return Ok(());
            }
        };
        self.last_snapshot = snap.clone();
        self.last_snapshot_at = Some(now);
        let day = utc_day_key(now);
        if self.state.day_key.as_deref() != Some(day.as_str()) {
            self.state.day_key = Some(day);
            self.state.equity_day_start_usd = Some(snap.long.equity_usd + snap.short.equity_usd);
            self.persist();
        }

        // Operator files first: a DISARM must register even on a tick that
        // ends up sending nothing (feed check below), so it is acted on as
        // soon as the venues are readable again.
        let (arm, disarm) = self.take_operator_files();
        if let Some(syms) = disarm {
            for sym in syms {
                if self.state.book(&sym).mode != Mode::Off {
                    log::warn!("[DISARM] closing both {sym} legs");
                    self.close_book(&sym, now, "disarm");
                    self.event("disarm", serde_json::json!({ "symbol": sym }));
                }
            }
            self.persist();
        } else if let Some(reqs) = arm {
            if self.state.halted || kill {
                log::warn!(
                    "[ARM] ignored: halted={} kill_switch={kill}",
                    self.state.halted
                );
            } else if let Some((sym, usd)) = reqs
                .iter()
                .find(|(_, usd)| *usd > self.cfg.max_notional_usd)
            {
                log::warn!(
                    "[ARM] ignored: {sym} ${usd:.0} exceeds HEDGE_MAX_NOTIONAL_USD ${:.0}",
                    self.cfg.max_notional_usd
                );
            } else {
                let sized: Vec<(String, f64, f64)> = reqs
                    .iter()
                    .map(|(sym, usd)| {
                        let q = qty_for_notional(
                            *usd,
                            snap.long.mark(sym),
                            self.meta(sym).size_decimals,
                        );
                        (sym.clone(), *usd, q)
                    })
                    .collect();
                if let Some((sym, usd, _)) = sized
                    .iter()
                    .find(|(sym, _, q)| *q < self.meta(sym).min_qty || *q <= 0.0)
                {
                    log::warn!("[ARM] ignored: {sym} ${usd:.0} is below the venue minimum");
                } else {
                    // A new campaign (no book On) resets the baselines the
                    // subsidy / PnL-since-ARM figures are measured from.
                    if !self.state.any_mode(Mode::On) {
                        self.state.armed_at = Some(now);
                        self.state.equity_at_arm_usd =
                            Some(snap.long.equity_usd + snap.short.equity_usd);
                        self.state.points_at_arm = points_latest_and_at(&self.points_series(), now)
                            .0
                            .map(|(_, v)| v);
                        self.state.cycles += 1;
                    }
                    for (sym, usd, qty) in sized {
                        // A book the venues already hold (e.g. one opened
                        // by hand) is adopted: the legs are only topped up
                        // or trimmed to `qty` from here on.
                        let held = snap.book(&sym);
                        log::info!(
                            "[ARM] {sym} target ${usd:.0}/leg = {qty} at {:.2} (venues hold long {} / short {})",
                            snap.long.mark(&sym), held.long, held.short
                        );
                        let b = self.state.book_mut(&sym);
                        b.mode = Mode::On;
                        b.target_qty = qty;
                        b.target_notional_usd = usd;
                        b.armed_at = Some(now);
                        b.exited_at = None;
                        b.exit_reason = None;
                        self.event(
                            "arm",
                            serde_json::json!({ "symbol": sym, "notional_usd": usd, "qty": qty,
                                "mark": snap.long.mark(&sym),
                                "adopted_long": held.long, "adopted_short": held.short }),
                        );
                    }
                    self.persist();
                }
            }
        }

        // Feed sanity: with one venue's mark off (REST fallback, placeholder
        // ticker) neither the guards nor the sizes can be trusted, so no
        // order goes out on any book. Status still gets written, with the
        // reason, so the card says why the bot is idle instead of going
        // stale.
        for c in &self.cfg.symbols {
            let (lm, sm) = (snap.long.mark(&c.symbol), snap.short.mark(&c.symbol));
            let divergence = (lm / sm - 1.0).abs();
            // NaN (a zero mark) counts as divergent.
            if divergence.is_nan() || divergence > 0.02 {
                let reason = format!(
                    "venue marks diverge {:.2}% on {}: long {lm} vs short {sm} — no orders on a broken feed",
                    divergence * 100.0,
                    c.symbol
                );
                log::error!("[FEED] {reason}");
                self.feed_problem = Some(reason);
                self.write_status(now, kill, 0);
                return Ok(());
            }
        }
        self.feed_problem = None;

        // `Off` never trades. Whatever the venues hold on an Off symbol is
        // not the bot's book: an operator's own position, or a live book
        // whose state.json was lost — either way it waits for an ARM
        // (adopt) or is closed by hand, never unwound because a default
        // target is 0.
        for c in &self.cfg.symbols {
            let held = snap.book(&c.symbol);
            if self.state.book(&c.symbol).mode == Mode::Off
                && (held.long >= self.meta(&c.symbol).min_qty
                    || held.short >= self.meta(&c.symbol).min_qty)
                && held.long + held.short > 0.0
            {
                log::info!(
                    "[OFF] venues hold long {} / short {} {}; not touched while Off (ARM adopts it)",
                    held.long, held.short, c.symbol
                );
            }
        }
        if !self.state.books.values().any(|b| b.mode != Mode::Off) {
            self.write_status(now, kill, 0);
            return Ok(());
        }

        // Liquidation guard, per account over every hedged position (the
        // venues margin cross-collateral: one symbol's loss eats the
        // headroom of all). Either venue short of headroom → close every
        // armed book.
        let armed = self.state.books.values().any(|b| b.target_qty > 0.0);
        for leg in [Leg::Long, Leg::Short] {
            let equity = snap.equity(leg);
            let legs = snap.legs(&self.cfg, leg);
            let gross: f64 = legs.iter().map(|(n, _)| n).sum();
            if let Some(h) = liq_headroom_pct(equity, &legs) {
                if h < self.cfg.liq_guard_pct && armed {
                    let reason = format!(
                        "liq_guard: {leg:?} venue headroom {h:.2}% < {:.2}% (equity ${equity:.2} vs gross ${gross:.0}) — closing every book",
                        self.cfg.liq_guard_pct
                    );
                    let syms: Vec<String> = self
                        .state
                        .books
                        .iter()
                        .filter(|(_, b)| b.mode != Mode::Off)
                        .map(|(s, _)| s.clone())
                        .collect();
                    for sym in syms {
                        self.close_book(&sym, now, "liq_guard");
                    }
                    self.halt(reason);
                    break;
                } else if h < self.cfg.liq_guard_pct * 2.0 {
                    log::warn!(
                        "[MARGIN] {leg:?} venue headroom {h:.2}% (guard {:.2}%): top up the account",
                        self.cfg.liq_guard_pct
                    );
                }
            }
        }

        // Net exposure watch, per symbol.
        let mut breach: Option<String> = None;
        for c in &self.cfg.symbols {
            if self.state.book(&c.symbol).mode == Mode::Off {
                continue;
            }
            let held = snap.book(&c.symbol);
            let net_usd = held.net() * snap.long.mark(&c.symbol);
            let limit = self.cfg.net_breach_ticks;
            let b = self.state.book_mut(&c.symbol);
            if net_usd.abs() > self.cfg.net_tolerance_usd {
                b.net_breach_ticks += 1;
                log::warn!(
                    "[NET] {} |long − short| = {:.5} (${net_usd:.0}) over tolerance ${:.0}, tick {}/{limit}",
                    c.symbol,
                    held.net(),
                    self.cfg.net_tolerance_usd,
                    b.net_breach_ticks,
                );
                if b.net_breach_ticks >= limit && breach.is_none() {
                    breach = Some(format!(
                        "net exposure {} ${net_usd:.0} over tolerance for {} ticks",
                        c.symbol, b.net_breach_ticks
                    ));
                }
            } else {
                b.net_breach_ticks = 0;
            }
        }
        if let Some(reason) = breach {
            self.halt(reason);
        }

        // Plan (at most one clip per leg per symbol per tick).
        let restricted = self.state.halted || kill;
        let mut plan: Vec<(String, Vec<Order>)> = Vec::new();
        for c in &self.cfg.symbols {
            let b = self.state.book(&c.symbol);
            if b.mode == Mode::Off {
                continue;
            }
            let mark = snap.long.mark(&c.symbol);
            let m = self.meta(&c.symbol);
            let clip_qty =
                qty_for_notional(self.cfg.clip_usd, mark, m.size_decimals).max(m.min_qty);
            let orders = plan_orders(
                b.target_qty,
                snap.book(&c.symbol),
                clip_qty,
                m.min_qty,
                self.cfg.net_tolerance_usd / mark,
                restricted,
            );
            if !orders.is_empty() {
                plan.push((c.symbol.clone(), orders));
            }
        }

        // Leverage guard over the whole tick BEFORE anything is sent: each
        // venue's gross notional after every growth order of every symbol
        // must stay within max_leverage × its equity. A pair whose second
        // leg would be refused must not have its first leg bought.
        if !self.cfg.dry_run {
            for leg in [Leg::Long, Leg::Short] {
                let add = planned_growth_usd(&plan, &snap, leg);
                let after = snap.gross(&self.cfg, leg) + add;
                if add > 0.0 && !leverage_ok(after, snap.equity(leg), self.cfg.max_leverage) {
                    self.halt(format!(
                        "leverage: {leg:?} venue gross ${after:.0} after this tick > {}x equity ${:.2} — deposit, then RISK_ACK",
                        self.cfg.max_leverage,
                        snap.equity(leg)
                    ));
                    plan.clear();
                    break;
                }
            }
        }

        // Execute, symbol by symbol. The second order of a pair is cut to
        // what the first actually filled, so a partial on the thin RH book
        // never leaves a full-clip naked leg on the other venue. A venue
        // error ends the tick (the next one re-reads everything).
        let mut sent = 0usize;
        'symbols: for (sym, mut orders) in plan {
            let mark = snap.long.mark(&sym);
            let min_qty = self.meta(&sym).min_qty;
            let mut i = 0;
            while i < orders.len() {
                let order = orders[i].clone();
                sent += 1;
                match self.execute(&sym, &order, mark).await {
                    Ok(filled) if filled < min_qty || filled <= 0.0 => {
                        log::warn!(
                            "[EXEC] {sym} {:?} {} unfilled (got {filled}) — no further orders this tick",
                            order.leg,
                            order.qty
                        );
                        // Stop every symbol, not just this one: an unfilled second
                        // leg means the other venue is not taking orders, and
                        // carrying on would open one naked first leg per
                        // remaining symbol.
                        break 'symbols;
                    }
                    Ok(filled) => {
                        if let Some(next) = orders.get_mut(i + 1) {
                            *next = size_second_leg(next, filled);
                            if next.qty.abs() < min_qty {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("[EXEC] {sym} {:?} {} failed: {e:?}", order.leg, order.qty);
                        break 'symbols;
                    }
                }
                i += 1;
            }
        }

        // Settle Exited → Off once flat. The re-read can fail like the
        // first one; the orders already sent this tick are still reported
        // and the settle simply waits for the next readable tick.
        let exited: Vec<String> = self
            .state
            .books
            .iter()
            .filter(|(_, b)| b.mode == Mode::Exited && b.target_qty == 0.0)
            .map(|(s, _)| s.clone())
            .collect();
        if !exited.is_empty() {
            match self.snapshot().await {
                Ok(s2) => {
                    for sym in exited {
                        let b2 = s2.book(&sym);
                        let min_qty = self.meta(&sym).min_qty;
                        if b2.long < min_qty.max(1e-12) && b2.short < min_qty.max(1e-12) {
                            let reason = self.state.book(&sym).exit_reason;
                            log::info!("[EXIT] {sym} flat on both venues ({reason:?})");
                            self.event(
                                "flat",
                                serde_json::json!({ "symbol": sym, "reason": reason }),
                            );
                            self.state.book_mut(&sym).mode = Mode::Off;
                        }
                    }
                    self.persist();
                }
                Err(e) => {
                    let reason = format!("venue unreachable: {e}");
                    log::error!("[FEED] {reason}");
                    self.feed_problem = Some(reason);
                }
            }
        }
        self.persist();
        self.write_status(now, kill, sent);
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

    fn write_status(&mut self, now: u64, kill: bool, orders_this_tick: usize) {
        let series = self.points_series();
        let feed = FeedStatus {
            problem: self.feed_problem.as_deref(),
            snapshot_at: self.last_snapshot_at,
        };
        let status = status_value(
            &self.cfg,
            &self.state,
            &self.last_snapshot,
            &series,
            feed,
            now,
            kill,
            orders_this_tick,
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

/// Whether the venue reads behind the snapshot are current: `problem` is
/// set while a venue is unreachable or the marks diverge (no orders go
/// out), `snapshot_at` dates the snapshot the legs/marks come from.
#[derive(Debug, Clone, Copy, Default)]
struct FeedStatus<'a> {
    problem: Option<&'a str>,
    snapshot_at: Option<u64>,
}

/// The monitoring projection. Top level follows the pairtrade-like shape
/// debot-dashboard already renders (`ts`/`updated_at`, `pnl_total` =
/// venue equity, `positions`, `subsidy`); everything hedge-specific sits
/// under `hedge_holder`.
///
/// The single-symbol fields the card reads (`target_qty`, `net_qty`,
/// `net_usd`, `basis_bps`, `legs.*.qty`, `legs.*.mark`) describe the
/// primary symbol; `legs.*.notional_usd` / `equity_usd` /
/// `liq_headroom_pct` are per account over every hedged symbol (what the
/// guards act on). `books` carries every symbol's own figures.
#[allow(clippy::too_many_arguments)] // one projection, all of its inputs
fn status_value(
    cfg: &Config,
    state: &State,
    snap: &Snapshot,
    points: &[(u64, f64)],
    feed: FeedStatus<'_>,
    now: u64,
    kill: bool,
    orders_this_tick: usize,
) -> serde_json::Value {
    let primary = cfg.primary();
    let pb = snap.book(primary);
    let pbook = state.book(primary);
    let equity_total = snap.long.equity_usd + snap.short.equity_usd;
    let pnl_since_arm = state.equity_at_arm_usd.map(|e0| equity_total - e0);
    let basis = |sym: &str| {
        let (l, s) = (snap.long.mark(sym), snap.short.mark(sym));
        if l > 0.0 && s > 0.0 {
            (l / s - 1.0) * 1e4
        } else {
            0.0
        }
    };
    let leg = |name: &str, instance: &str, which: Leg| {
        let (qty, vs) = match which {
            Leg::Long => (pb.long, &snap.long),
            Leg::Short => (pb.short, &snap.short),
        };
        serde_json::json!({
            "instance": instance,
            "side": name,
            "qty": qty,
            "notional_usd": snap.gross(cfg, which),
            "mark": vs.mark(primary),
            "equity_usd": vs.equity_usd,
            "liq_headroom_pct": liq_headroom_pct(vs.equity_usd, &snap.legs(cfg, which)),
        })
    };
    let mut positions = Vec::new();
    let mut books = serde_json::Map::new();
    for c in &cfg.symbols {
        let held = snap.book(&c.symbol);
        if held.long > 0.0 {
            positions.push(serde_json::json!({
                "symbol": format!("{} ({})", c.symbol, cfg.long_instance), "side": "long",
                "size": format!("{}", held.long), "entry_price": serde_json::Value::Null }));
        }
        if held.short > 0.0 {
            positions.push(serde_json::json!({
                "symbol": format!("{} ({})", c.symbol, cfg.short_instance), "side": "short",
                "size": format!("{}", held.short), "entry_price": serde_json::Value::Null }));
        }
        let b = state.book(&c.symbol);
        books.insert(
            c.symbol.clone(),
            serde_json::json!({
                "mode": b.mode,
                "target_qty": b.target_qty,
                "target_notional_usd": b.target_notional_usd,
                "armed_at": b.armed_at,
                "exit_reason": b.exit_reason,
                "long_qty": held.long,
                "short_qty": held.short,
                "net_qty": held.net(),
                "net_usd": held.net() * snap.long.mark(&c.symbol),
                "mark_long": snap.long.mark(&c.symbol),
                "mark_short": snap.short.mark(&c.symbol),
                "basis_bps": basis(&c.symbol),
                "mmr_pct": c.mmr_pct,
            }),
        );
    }
    // Split from the top-level literal: one `json!` this deep trips the
    // macro recursion limit.
    let hedge_holder = serde_json::json!({
        "mode": state.overall_mode(),
        "halted": state.halted,
        "halt_reason": state.halt_reason,
        "kill_switch": kill,
        "symbols": cfg.symbol_names(),
        "target_qty": pbook.target_qty,
        "target_notional_usd": state
            .books
            .values()
            .filter(|b| b.mode != Mode::Off)
            .map(|b| b.target_notional_usd)
            .sum::<f64>(),
        "armed_at": state.armed_at,
        "exited_at": pbook.exited_at,
        "exit_reason": pbook.exit_reason,
        "cycles": state.cycles,
        "net_qty": pb.net(),
        "net_usd": pb.net() * snap.long.mark(primary),
        "net_tolerance_usd": cfg.net_tolerance_usd,
        "basis_bps": basis(primary),
        "equity_total_usd": equity_total,
        "equity_at_arm_usd": state.equity_at_arm_usd,
        "pnl_since_arm_usd": pnl_since_arm,
        "points_at_arm": state.points_at_arm,
        "orders_this_tick": orders_this_tick,
        "feed_problem": feed.problem,
        "snapshot_at": feed.snapshot_at,
        "config_fp": cfg.fingerprint(),
        "legs": {
            "long": leg("long", &cfg.long_instance, Leg::Long),
            "short": leg("short", &cfg.short_instance, Leg::Short),
        },
        "books": books,
    });
    serde_json::json!({
        "ts": now,
        "updated_at": chrono::DateTime::<chrono::Utc>::from(UNIX_EPOCH + Duration::from_secs(now)).to_rfc3339(),
        "process_started_at": state.process_started_at,
        "bot": BOT,
        "id": BOT,
        "dex": "Lighter (Robinhood Chain) / Lighter Core",
        "symbol": primary,
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
        "[CONFIG] bot={BOT} dry_run={} symbols={} long={} short={} target=${:.0} max=${:.0} clip=${:.0} slip={}bps net_tol=${:.0} liq_guard={}% max_lev={}x fp={}",
        cfg.dry_run,
        cfg.symbols
            .iter()
            .map(|c| format!("{}:{}", c.symbol, c.mmr_pct))
            .collect::<Vec<_>>()
            .join(","),
        cfg.long_instance, cfg.short_instance, cfg.target_notional_usd,
        cfg.max_notional_usd, cfg.clip_usd, cfg.taker_slippage_bps, cfg.net_tolerance_usd,
        cfg.liq_guard_pct, cfg.max_leverage, cfg.fingerprint()
    );
    let symbols = cfg.symbol_names();
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
    let mut meta = std::collections::BTreeMap::new();
    for sym in &symbols {
        let (mark, sd1, min1) = long
            .mark(sym)
            .await
            .with_context(|| format!("initial long-leg quote {sym}"))?;
        let (_, sd2, min2) = short
            .mark(sym)
            .await
            .with_context(|| format!("initial short-leg quote {sym}"))?;
        let m = SymMeta {
            size_decimals: sd1.min(sd2),
            min_qty: min1.max(min2),
        };
        log::info!(
            "[STARTUP] {sym} mark={mark:.2} size_decimals={} min_qty={}",
            m.size_decimals,
            m.min_qty
        );
        meta.insert(sym.clone(), m);
    }
    let mut state: State = load_json(&cfg.state_path)?.unwrap_or_default();
    state = migrate_legacy(state, cfg.primary());
    state = reconcile_state_mode(state, cfg.dry_run);
    state.process_started_at = Some(now_secs());
    for (sym, b) in &state.books {
        if !symbols.contains(sym) && b.mode != Mode::Off {
            // The config dropped a symbol whose book is still armed: it is
            // no longer read or traded. Say so loudly; the operator closes
            // it by hand or puts the symbol back.
            log::error!(
                "[STARTUP] {sym} book is {:?} (target {}) but {sym} is not in HEDGE_SYMBOLS — it will not be managed",
                b.mode, b.target_qty
            );
        }
    }
    log::info!(
        "[STARTUP] mode={:?} books={} halted={}",
        state.overall_mode(),
        state
            .books
            .iter()
            .map(|(s, b)| format!("{s}:{:?}:{}", b.mode, b.target_qty))
            .collect::<Vec<_>>()
            .join(","),
        state.halted
    );
    let mut engine = Engine {
        sentinels: Sentinels::new(cfg.kill_switch_path.clone(), cfg.risk_ack_path.clone()),
        cfg,
        state,
        long,
        short,
        meta,
        last_s3_mirror: 0,
        last_snapshot: Snapshot::default(),
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
    fn leverage_guard_uses_gross_after_the_tick() {
        // $20k leg on $4,000: 5.0x — refused at 4.9x, allowed at 5x.
        assert!(!leverage_ok(20_000.0, 4_000.0, 4.9));
        assert!(leverage_ok(20_000.0, 4_000.0, 5.0));
        // BTC $20k + META $10k on $8k = 3.75x ok at 5x, refused at 3.5x.
        assert!(leverage_ok(30_000.0, 8_000.0, 5.0));
        assert!(!leverage_ok(30_000.0, 8_000.0, 3.5));
    }

    fn sym_book(mode: Mode, target_qty: f64) -> SymBook {
        SymBook {
            mode,
            target_qty,
            ..Default::default()
        }
    }

    #[test]
    fn state_from_the_other_mode_is_dropped_to_off() {
        let mut armed = State {
            dry_run: Some(true),
            ..Default::default()
        };
        armed.books.insert("BTC".into(), sym_book(Mode::On, 0.247));
        armed
            .books
            .insert("META".into(), sym_book(Mode::Exited, 0.0));
        let live = reconcile_state_mode(armed.clone(), false);
        assert!(live
            .books
            .values()
            .all(|b| b.mode == Mode::Off && b.target_qty == 0.0));
        assert_eq!(live.dry_run, Some(false));
        // Same mode: untouched.
        let same = reconcile_state_mode(armed.clone(), true);
        assert_eq!(same.book("BTC").mode, Mode::On);
        assert_eq!(same.book("BTC").target_qty, 0.247);
        // Pre-field state (no dry_run recorded): kept, now stamped.
        let mut legacy = armed;
        legacy.dry_run = None;
        let kept = reconcile_state_mode(legacy, false);
        assert_eq!(kept.book("BTC").mode, Mode::On);
        assert_eq!(kept.dry_run, Some(false));
    }

    #[test]
    fn single_symbol_state_json_migrates_to_the_primary_book() {
        // The Tokyo holder's state.json as written by 3e78706 (one book).
        let old = r#"{"mode":"On","target_qty":0.19685,"target_notional_usd":16000.0,
            "dry_long_qty":0.19685,"dry_short_qty":0.19685,"halted":false,"halt_reason":null,
            "armed_at":1789820643,"exited_at":null,"exit_reason":null,"equity_at_arm_usd":8000.0,
            "cycles":1,"net_breach_ticks":0,"points_at_arm":72.19526,"day_key":"2026-09-25",
            "equity_day_start_usd":8000.0,"process_started_at":1790000000,"dry_run":true}"#;
        let st: State = serde_json::from_str(old).unwrap();
        let st = migrate_legacy(st, "BTC");
        let b = st.book("BTC");
        assert_eq!(b.mode, Mode::On);
        assert_eq!(b.target_qty, 0.19685);
        assert_eq!(b.dry_long_qty, 0.19685);
        assert_eq!(b.armed_at, Some(1789820643));
        assert_eq!(st.armed_at, Some(1789820643));
        assert_eq!(st.points_at_arm, Some(72.19526));
        assert_eq!(st.books.len(), 1);
        // Written back without the legacy top-level keys; reads back the same.
        let json = serde_json::to_value(&st).unwrap();
        assert!(json.get("mode").is_none() && json.get("target_qty").is_none());
        let again = migrate_legacy(serde_json::from_value::<State>(json).unwrap(), "BTC");
        assert_eq!(again.book("BTC"), b);
        // A fresh (flat, never armed) legacy file creates no book.
        let flat: State =
            serde_json::from_str(r#"{"mode":"Off","target_qty":0.0,"halted":false,"cycles":0}"#)
                .unwrap();
        assert!(migrate_legacy(flat, "BTC").books.is_empty());
    }

    #[test]
    fn qty_for_notional_floors_to_size_decimals() {
        assert!((qty_for_notional(20_000.0, 81_000.0, 5) - 0.24691).abs() < 1e-12);
        assert_eq!(qty_for_notional(20_000.0, 0.0, 5), 0.0);
        assert_eq!(qty_for_notional(-1.0, 81_000.0, 5), 0.0);
    }

    #[test]
    fn liq_headroom_is_equity_over_gross_less_weighted_mmr() {
        // One symbol: equity / notional − mmr, as before.
        assert!((liq_headroom_pct(4_000.0, &[(20_000.0, 1.2)]).unwrap() - 18.8).abs() < 1e-9);
        assert_eq!(liq_headroom_pct(4_000.0, &[]), None);
        assert_eq!(liq_headroom_pct(4_000.0, &[(0.0, 1.2), (0.0, 3.0)]), None);
        // 8% guard on a $20k BTC leg fires when equity falls to $1,840.
        assert!(liq_headroom_pct(1_839.0, &[(20_000.0, 1.2)]).unwrap() < 8.0);
        assert!(liq_headroom_pct(1_841.0, &[(20_000.0, 1.2)]).unwrap() > 8.0);
        // BTC $20k @1.2 + AMZN $10k @6 on $6k: (600000 − 24000 − 60000) / 30000 = 17.2.
        let h = liq_headroom_pct(6_000.0, &[(20_000.0, 1.2), (10_000.0, 6.0)]).unwrap();
        assert!((h - 17.2).abs() < 1e-9);
        // Venue liquidation (equity = Σ n·mmr = $840) is headroom 0.
        let h = liq_headroom_pct(840.0, &[(20_000.0, 1.2), (10_000.0, 6.0)]).unwrap();
        assert!(h.abs() < 1e-9);
    }

    fn cfg_for_test() -> Config {
        let dir = std::env::temp_dir().join(format!("hedge-test-{}", std::process::id()));
        Config {
            dry_run: true,
            live_confirm: String::new(),
            symbols: vec![SymbolCfg {
                symbol: "BTC".into(),
                mmr_pct: 1.2,
            }],
            long_instance: "rh".into(),
            short_instance: "core".into(),
            target_notional_usd: 20_000.0,
            max_notional_usd: 30_000.0,
            clip_usd: 10_000.0,
            taker_slippage_bps: 3,
            net_tolerance_usd: 500.0,
            net_breach_ticks: 3,
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

    fn multi_cfg() -> Config {
        let mut c = cfg_for_test();
        c.symbols = parse_symbols("BTC:1.2,META:3,AMZN:6").unwrap();
        c
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
    fn symbols_parse_with_a_required_mmr_each() {
        let s = parse_symbols(" btc:1.2 , META:3,amzn:6 ").unwrap();
        assert_eq!(
            s.iter()
                .map(|c| (c.symbol.as_str(), c.mmr_pct))
                .collect::<Vec<_>>(),
            vec![("BTC", 1.2), ("META", 3.0), ("AMZN", 6.0)]
        );
        assert!(parse_symbols("BTC").is_err(), "MMR is required");
        assert!(parse_symbols("BTC:1.2,btc:1.2").is_err(), "duplicate");
        assert!(parse_symbols("BTC:0").is_err());
        assert!(parse_symbols("BTC:x").is_err());
        assert!(parse_symbols(" , ").is_err());
        // The liquidation guard must sit above every symbol's MMR.
        let mut c = cfg_for_test();
        c.symbols = parse_symbols("BTC:1.2,XYZ:10").unwrap();
        assert!(c.validate().is_err());
    }

    #[test]
    fn single_symbol_fingerprint_is_unchanged_by_the_multi_symbol_code() {
        // The field list 3e78706 hashed; the Tokyo DRY_RUN holder's fp must
        // not move just because the binary learned several symbols.
        let c = cfg_for_test();
        let before = config_fingerprint(&[
            ("symbol", "BTC".to_string()),
            ("long", "rh".to_string()),
            ("short", "core".to_string()),
            ("target", "20000.00".to_string()),
            ("max_notional", "30000.00".to_string()),
            ("clip", "10000.00".to_string()),
            ("slip_bps", "3".to_string()),
            ("net_tol", "500.00".to_string()),
            ("mmr", "1.200".to_string()),
            ("liq_guard", "8.000".to_string()),
            ("max_lev", "5.00".to_string()),
        ]);
        assert_eq!(c.fingerprint(), before);
        assert_ne!(multi_cfg().fingerprint(), before);
    }

    #[test]
    fn arm_file_single_symbol_format_is_unchanged() {
        let c = cfg_for_test();
        assert_eq!(parse_arm("", &c), Ok(vec![("BTC".into(), 20_000.0)]));
        assert_eq!(parse_arm("20642\n", &c), Ok(vec![("BTC".into(), 20_642.0)]));
        assert_eq!(
            parse_arm("btc 25000", &c),
            Ok(vec![("BTC".into(), 25_000.0)])
        );
        assert!(parse_arm("-5", &c).is_err());
        assert!(parse_arm("META 10000", &c).is_err(), "not configured");
    }

    #[test]
    fn arm_file_multi_symbol_needs_explicit_lines_and_rejects_any_bad_one() {
        let c = multi_cfg();
        assert_eq!(
            parse_arm("# campaign 2\nBTC 40000\nmeta=10000\nAMZN:10000\n", &c),
            Ok(vec![
                ("BTC".into(), 40_000.0),
                ("META".into(), 10_000.0),
                ("AMZN".into(), 10_000.0)
            ])
        );
        assert_eq!(parse_arm("META", &c), Ok(vec![("META".into(), 20_000.0)]));
        assert!(parse_arm("", &c).is_err(), "empty is ambiguous");
        assert!(parse_arm("10000", &c).is_err(), "bare number is ambiguous");
        assert!(
            parse_arm("META 10000\nGOOGL 10000", &c).is_err(),
            "one bad line → none"
        );
        assert!(parse_arm("META 10000\nMETA 5000", &c).is_err());
        assert!(parse_arm("META ten", &c).is_err());
        assert!(parse_arm("META 10000 extra", &c).is_err());
        // Delimiter-only lines are rejected, not a panic (Codex P2 on #352).
        for junk in [":", "=", " : = ", "META 10000\n=="] {
            assert!(parse_arm(junk, &c).is_err(), "{junk:?}");
        }
        assert!(parse_arm(":", &cfg_for_test()).is_err());
    }

    #[test]
    fn disarm_file_closes_named_books_and_errs_towards_all() {
        let c = multi_cfg();
        let all = vec!["BTC".to_string(), "META".into(), "AMZN".into()];
        assert_eq!(parse_disarm("", &c), all);
        assert_eq!(
            parse_disarm("meta\n\nAMZN\nmeta", &c),
            vec!["META".to_string(), "AMZN".into()]
        );
        assert_eq!(
            parse_disarm("META\nGOOGL", &c),
            all,
            "unknown symbol → close everything"
        );
    }

    fn snap(pairs: &[(&str, f64, f64, f64, f64)], eq_l: f64, eq_s: f64) -> Snapshot {
        let mut s = Snapshot::default();
        s.long.equity_usd = eq_l;
        s.short.equity_usd = eq_s;
        for (sym, ql, qs, ml, ms) in pairs {
            s.long.qty.insert(sym.to_string(), *ql);
            s.short.qty.insert(sym.to_string(), *qs);
            s.long.mark.insert(sym.to_string(), *ml);
            s.short.mark.insert(sym.to_string(), *ms);
        }
        s
    }

    #[test]
    fn planned_growth_sums_every_symbol_at_its_own_venue_mark() {
        let s = snap(
            &[
                ("BTC", 0.1, -0.1, 80_000.0, 80_100.0),
                ("META", 0.0, 0.0, 750.0, 752.0),
                ("AMZN", 0.0, 0.0, 250.0, 250.0),
            ],
            4_000.0,
            4_000.0,
        );
        let plan = vec![
            (
                "BTC".to_string(),
                vec![
                    Order {
                        leg: Leg::Long,
                        qty: 0.1,
                    },
                    Order {
                        leg: Leg::Short,
                        qty: 0.1,
                    },
                ],
            ),
            (
                "META".to_string(),
                vec![
                    Order {
                        leg: Leg::Long,
                        qty: 10.0,
                    },
                    Order {
                        leg: Leg::Short,
                        qty: 10.0,
                    },
                ],
            ),
            // A reduction elsewhere frees nothing up front: not counted.
            (
                "AMZN".to_string(),
                vec![Order {
                    leg: Leg::Short,
                    qty: -5.0,
                }],
            ),
        ];
        assert!((planned_growth_usd(&plan, &s, Leg::Long) - (8_000.0 + 7_500.0)).abs() < 1e-9);
        assert!((planned_growth_usd(&plan, &s, Leg::Short) - (8_010.0 + 7_520.0)).abs() < 1e-9);
        // Held $8,010 + $15,530 growth = $23,540 on $4,000: over 5x.
        let after = s.gross(&cfg_for_test_multi_btc_meta(), Leg::Short)
            + planned_growth_usd(&plan, &s, Leg::Short);
        assert!(!leverage_ok(after, 4_000.0, 5.0));
        assert!(leverage_ok(after, 4_000.0, 6.0));
    }

    fn cfg_for_test_multi_btc_meta() -> Config {
        let mut c = cfg_for_test();
        c.symbols = parse_symbols("BTC:1.2,META:3").unwrap();
        c
    }

    #[test]
    fn snapshot_books_ignore_wrong_side_positions() {
        let s = snap(
            &[
                ("BTC", 0.247, -0.247, 81_000.0, 81_000.0),
                ("META", -1.0, 2.0, 750.0, 750.0),
            ],
            4_000.0,
            4_000.0,
        );
        assert_eq!(s.book("BTC"), book(0.247, 0.247));
        assert_eq!(
            s.book("META"),
            book(0.0, 0.0),
            "long venue short / short venue long are foreign"
        );
        assert_eq!(s.book("AMZN"), book(0.0, 0.0));
    }

    #[test]
    fn status_reports_net_basis_and_pnl_since_arm() {
        let cfg = cfg_for_test();
        let mut state = State {
            equity_at_arm_usd: Some(8_494.0),
            ..Default::default()
        };
        state.books.insert(
            "BTC".into(),
            SymBook {
                mode: Mode::On,
                target_qty: 0.247,
                target_notional_usd: 20_000.0,
                ..Default::default()
            },
        );
        let s = snap(
            &[("BTC", 0.247, -0.247, 81_349.4, 81_324.2)],
            4_480.0,
            4_010.0,
        );
        let feed = FeedStatus {
            problem: None,
            snapshot_at: Some(1_789_812_142),
        };
        let v = status_value(&cfg, &state, &s, &[], feed, 1_789_812_142, false, 0);
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
        assert_eq!(h["target_qty"], 0.247);
        assert_eq!(h["books"]["BTC"]["mode"], "On");
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
    fn multi_symbol_status_keeps_primary_fields_and_reports_account_margin() {
        let cfg = multi_cfg();
        let mut state = State::default();
        state.books.insert("BTC".into(), sym_book(Mode::On, 0.247));
        state.books.insert("META".into(), sym_book(Mode::On, 13.3));
        let s = snap(
            &[
                ("BTC", 0.247, -0.247, 81_000.0, 81_000.0),
                ("META", 13.3, -13.3, 750.0, 751.0),
                ("AMZN", 0.0, 0.0, 250.0, 250.0),
            ],
            6_000.0,
            6_000.0,
        );
        let v = status_value(
            &cfg,
            &state,
            &s,
            &[],
            FeedStatus::default(),
            1_789_812_142,
            false,
            0,
        );
        let h = &v["hedge_holder"];
        // Card fields: the primary (BTC) book.
        assert_eq!(h["target_qty"], 0.247);
        assert_eq!(h["legs"]["long"]["qty"], 0.247);
        assert_eq!(h["legs"]["long"]["mark"], 81_000.0);
        // Account figures: gross over both symbols, MMR-weighted headroom.
        let gross = 0.247 * 81_000.0 + 13.3 * 750.0;
        assert!((h["legs"]["long"]["notional_usd"].as_f64().unwrap() - gross).abs() < 1e-6);
        let want = (6_000.0 * 100.0 - 0.247 * 81_000.0 * 1.2 - 13.3 * 750.0 * 3.0) / gross;
        assert!((h["legs"]["long"]["liq_headroom_pct"].as_f64().unwrap() - want).abs() < 1e-9);
        // Every symbol in `books`, four positions (AMZN flat).
        assert_eq!(h["books"]["META"]["long_qty"], 13.3);
        assert!(
            (h["books"]["META"]["basis_bps"].as_f64().unwrap() - (750.0 / 751.0 - 1.0) * 1e4).abs()
                < 1e-9
        );
        assert_eq!(h["books"]["AMZN"]["mode"], "Off");
        assert_eq!(v["position_count"], 4);
        assert_eq!(h["symbols"], serde_json::json!(["BTC", "META", "AMZN"]));
        assert_eq!(h["mode"], "On");
    }

    #[test]
    fn status_carries_the_feed_problem_and_dates_the_frozen_snapshot() {
        // A venue outage (Core 502/503, 2026-09-20 11:02–11:12Z): the tick
        // sends nothing but still publishes, from the last good snapshot,
        // with the reason and the snapshot's own timestamp so the card can
        // say "idle: venue unreachable, marks as of 11:01Z" instead of
        // going stale without a word.
        let cfg = cfg_for_test();
        let mut state = State::default();
        state.books.insert("BTC".into(), sym_book(Mode::On, 0.247));
        let s = snap(
            &[("BTC", 0.247, -0.247, 81_349.4, 81_324.2)],
            4_480.0,
            4_010.0,
        );
        let reason =
            "venue unreachable: short get_ticker BTC: Transient(\"recentTrades HTTP 503\")";
        let feed = FeedStatus {
            problem: Some(reason),
            snapshot_at: Some(1_789_812_142),
        };
        let v = status_value(&cfg, &state, &s, &[], feed, 1_789_812_742, false, 0);
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
        let v1 = status_value(&cfg, &state, &s, &[], healthy, 1_789_812_742, false, 0);
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
