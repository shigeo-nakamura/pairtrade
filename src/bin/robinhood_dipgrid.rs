//! Robinhood Chain Lighter BTC dip-buy grid bot — PROTOTYPE (bot-strategy#816).
//!
//! Standalone binary, NOT part of the pairtrade `strategies:` engine: this is
//! a single-instrument, directional, multi-add-pyramiding strategy (buy
//! further chunks on drops from a rolling-max reference, single-sweep exit
//! at target/stop/timeout), architecturally incompatible with pairtrade's
//! two-leg z-scored spread engine. See bot-strategy#816 for the backtest
//! that motivated this (144/144 param combos profitable on a test window
//! with zero overlap with the original discovery episode) and the
//! architecture decision (same-repo binary, reusing `dex-connector` wiring
//! via `debot::trade::execution::dex_connector_box::DexConnectorBox`, but
//! NOT pairtrade's risk_io/status/pnl_log modules — those are private to
//! the `pairtrade` module tree (`mod risk_io;` etc. in
//! `src/pairtrade/mod.rs`, not `pub mod`) and are not reachable from a
//! separate `src/bin/` binary crate without widening pairtrade's public
//! surface. This file re-implements the same on-disk conventions
//! (KILL_SWITCH / RISK_ACK sentinel files, atomic tmp+rename state writes)
//! independently rather than importing pairtrade's private internals.
//!
//! KNOWN GAPS before any live use (see bot-strategy#816):
//! - Backtest was L1-top-of-book only; entry/exit here assume the taker IOC
//!   fills at the observed touch price, no slippage/partial-fill modeling.
//! - No shutdown-grace / force-close-on-SIGTERM handling (pairtrade has
//!   this; this prototype does not).
//! - Only ~4 clean out-of-sample backtest days exist, and none of them
//!   cover a down-trend regime for this long-only strategy.
//! - `require_uptrend` gate is untested — see bot-strategy#816 discussion.
//!
//! DRY_RUN must stay on until these are explicitly addressed.

use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset, Utc};
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use dex_connector::{DexConnector, OrderSide, PriceUpdate};
use env_logger::Builder;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

fn init_logger() {
    let offset_seconds = std::env::var("TIMEZONE_OFFSET")
        .unwrap_or_else(|_| "3600".to_string())
        .parse::<i32>()
        .unwrap_or(3600);
    let offset = FixedOffset::east_opt(offset_seconds).unwrap_or(FixedOffset::east_opt(0).unwrap());
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    Builder::from_env(env)
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

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// ---------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
struct DipGridConfig {
    instance_id: String,
    symbol: String,
    dry_run: bool,
    chunk_notional_usd: f64,
    max_notional_cap_usd: f64,
    add_bps: f64,
    target_bps: f64,
    stop_loss_bps: Option<f64>,
    max_hold_secs: u64,
    lookback_secs: u64,
    require_uptrend: bool,
    uptrend_lookback_secs: u64,
    uptrend_min_return_bps: f64,
    equity_usd_reference: f64,
    max_session_loss_bps: f64,
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

fn env_opt_f64(name: &str) -> Option<f64> {
    std::env::var(name).ok().and_then(|v| v.parse::<f64>().ok())
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

impl DipGridConfig {
    fn from_env() -> Self {
        let instance_id = env_string("DIPGRID_INSTANCE_ID", "dipgrid");
        let base_dir = env_string("DIPGRID_BASE_DIR", "/opt/debot");
        DipGridConfig {
            symbol: env_string("DIPGRID_SYMBOL", "BTC"),
            dry_run: env_bool("DIPGRID_DRY_RUN", true),
            chunk_notional_usd: env_f64("DIPGRID_CHUNK_NOTIONAL_USD", 5000.0),
            max_notional_cap_usd: env_f64("DIPGRID_MAX_NOTIONAL_CAP_USD", 50000.0),
            add_bps: env_f64("DIPGRID_ADD_BPS", 8.0),
            target_bps: env_f64("DIPGRID_TARGET_BPS", 20.0),
            stop_loss_bps: env_opt_f64("DIPGRID_STOP_LOSS_BPS"),
            max_hold_secs: env_u64("DIPGRID_MAX_HOLD_SECS", 1800),
            lookback_secs: env_u64("DIPGRID_LOOKBACK_SECS", 60),
            require_uptrend: env_bool("DIPGRID_REQUIRE_UPTREND", false),
            uptrend_lookback_secs: env_u64("DIPGRID_UPTREND_LOOKBACK_SECS", 14400),
            uptrend_min_return_bps: env_f64("DIPGRID_UPTREND_MIN_RETURN_BPS", 0.0),
            equity_usd_reference: env_f64("DIPGRID_EQUITY_USD_REFERENCE", 1000.0),
            max_session_loss_bps: env_f64("DIPGRID_MAX_SESSION_LOSS_BPS", 500.0),
            kill_switch_path: PathBuf::from(env_string(
                "DIPGRID_KILL_SWITCH_PATH",
                &format!("{base_dir}/KILL_SWITCH"),
            )),
            risk_ack_path: PathBuf::from(env_string(
                "DIPGRID_RISK_ACK_PATH",
                &format!("{base_dir}/RISK_ACK_{}", instance_id.to_uppercase()),
            )),
            state_path: PathBuf::from(env_string(
                "DIPGRID_STATE_PATH",
                &format!("{base_dir}/dipgrid_risk_state_{instance_id}.json"),
            )),
            status_path: PathBuf::from(env_string(
                "DIPGRID_STATUS_PATH",
                &format!("{base_dir}/dipgrid_status_{instance_id}.json"),
            )),
            pnl_log_path: PathBuf::from(env_string(
                "DIPGRID_PNL_LOG_PATH",
                &format!("{base_dir}/dipgrid_pnl_{instance_id}.jsonl"),
            )),
            instance_id,
        }
    }
}

// ---------------------------------------------------------------------
// Risk state (independent re-implementation of pairtrade's risk_io
// pattern — atomic tmp+rename JSON, sticky halt cleared only by RISK_ACK)
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct RiskState {
    #[serde(default)]
    session_start_equity: f64,
    #[serde(default)]
    peak_equity: f64,
    #[serde(default)]
    realized_pnl_session: f64,
    #[serde(default)]
    total_trades: u64,
    #[serde(default)]
    total_wins: u64,
    #[serde(default)]
    total_pnl: f64,
    #[serde(default)]
    max_dd_bps: f64,
    #[serde(default)]
    session_halted: bool,
    #[serde(default)]
    session_halt_reason: Option<String>,
}

fn load_state(path: &Path) -> RiskState {
    match std::fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
        Err(_) => RiskState::default(),
    }
}

fn persist_state(path: &Path, state: &RiskState) {
    let Ok(json) = serde_json::to_string_pretty(state) else {
        log::warn!("[STATE] serialize failed");
        return;
    };
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = dir.join(format!(
        ".{}.tmp.{}",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    if std::fs::write(&tmp, json).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    } else {
        log::warn!("[STATE] write failed");
    }
}

fn write_status(path: &Path, status: &serde_json::Value) {
    let Ok(json) = serde_json::to_string_pretty(status) else {
        return;
    };
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = dir.join(format!(
        ".{}.tmp.{}",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    if std::fs::write(&tmp, json).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

fn append_pnl_log(path: &Path, record: &serde_json::Value) {
    use std::io::Write;
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
// Episode (one flat-to-flat grid position)
// ---------------------------------------------------------------------

struct Episode {
    first_add_ts: u64,
    cost_usd: f64,
    size: f64,
    last_add_price: f64,
    n_adds: u32,
}

impl Episode {
    fn avg_entry(&self) -> f64 {
        self.cost_usd / self.size
    }
}

// ---------------------------------------------------------------------
// Main engine
// ---------------------------------------------------------------------

struct DipGridEngine {
    cfg: DipGridConfig,
    connector: std::sync::Arc<dyn DexConnector + Send + Sync>,
    ask_window: VecDeque<(u64, f64)>,
    trend_window: VecDeque<(u64, f64)>,
    episode: Option<Episode>,
    state: RiskState,
    last_status_write: u64,
}

impl DipGridEngine {
    fn rolling_ask_max(&self) -> Option<f64> {
        self.ask_window
            .iter()
            .map(|(_, p)| *p)
            .fold(None, |acc, p| Some(acc.map_or(p, |m: f64| m.max(p))))
    }

    fn uptrend_ok(&self, current_price: f64) -> bool {
        if !self.cfg.require_uptrend {
            return true;
        }
        let Some((_, oldest_price)) = self.trend_window.front() else {
            // Not enough history yet — fail closed, don't trade blind.
            return false;
        };
        if *oldest_price <= 0.0 {
            return false;
        }
        let ret_bps = (current_price - oldest_price) / oldest_price * 10_000.0;
        ret_bps >= self.cfg.uptrend_min_return_bps
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
            self.state.peak_equity =
                self.state.session_start_equity + self.state.realized_pnl_session;
            persist_state(&self.cfg.state_path, &self.state);
            // The file is unconditionally removed so a stale ack from a prior
            // incident never silently re-arms and clears the next halt too.
            if let Err(e) = std::fs::remove_file(&self.cfg.risk_ack_path) {
                log::warn!(
                    "[RISK_ACK] failed to remove {} after ack: {:?}",
                    self.cfg.risk_ack_path.display(),
                    e
                );
            }
        }
    }

    fn entries_allowed(&self) -> bool {
        !self.kill_switch_engaged() && !self.state.session_halted
    }

    async fn submit_taker(&self, side: OrderSide, size: f64, reduce_only: bool) -> Result<Decimal> {
        let size_dec = Decimal::from_str(&format!("{size:.8}")).context("size to Decimal")?;
        if self.cfg.dry_run {
            return Ok(size_dec);
        }
        // `create_order_taker_ioc` is unimplemented for the Lighter connector
        // (hard `Err`, see dex-connector's dex_impl.rs) — Lighter's native
        // `create_order(price=None)` already gives IOC + 20% protection-price
        // taker semantics, so route through that instead, matching every
        // other pairtrade taker caller (entry/exit/hedge recovery).
        let resp = self
            .connector
            .create_order(
                &self.cfg.symbol,
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

    fn on_exit(&mut self, exit_price: f64, reason: &str) {
        let Some(ep) = self.episode.take() else {
            return;
        };
        let avg_entry = ep.avg_entry();
        let pnl = (exit_price - avg_entry) * ep.size;
        let now = now_secs();
        log::info!(
            "[EXIT] reason={reason} n_adds={} size={:.6} avg_entry={:.2} exit={:.2} pnl=${:.2} held={}s",
            ep.n_adds,
            ep.size,
            avg_entry,
            exit_price,
            pnl,
            now.saturating_sub(ep.first_add_ts)
        );

        self.state.realized_pnl_session += pnl;
        self.state.total_trades += 1;
        if pnl > 0.0 {
            self.state.total_wins += 1;
        }
        self.state.total_pnl += pnl;
        let current_equity = self.state.session_start_equity + self.state.realized_pnl_session;
        if current_equity > self.state.peak_equity {
            self.state.peak_equity = current_equity;
        }
        let dd_bps = if self.state.peak_equity > 0.0 {
            (self.state.peak_equity - current_equity) / self.state.peak_equity * 10_000.0
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
                "[SESSION_DD] halt engaged: dd={:.0}bps >= {:.0}bps threshold — clear via RISK_ACK at {}",
                dd_bps,
                self.cfg.max_session_loss_bps,
                self.cfg.risk_ack_path.display()
            );
        }
        persist_state(&self.cfg.state_path, &self.state);

        append_pnl_log(
            &self.cfg.pnl_log_path,
            &serde_json::json!({
                "ts": now,
                "instance_id": self.cfg.instance_id,
                "symbol": self.cfg.symbol,
                "n_adds": ep.n_adds,
                "size": ep.size,
                "avg_entry": avg_entry,
                "exit_price": exit_price,
                "close_reason": reason,
                "pnl_usd": pnl,
                "held_secs": now.saturating_sub(ep.first_add_ts),
                "dry_run": self.cfg.dry_run,
            }),
        );
    }

    async fn handle_tick(&mut self, update: PriceUpdate) {
        let now = now_secs();
        let ask = update.best_ask.to_f64().unwrap_or(0.0);
        let bid = update.best_bid.to_f64().unwrap_or(0.0);
        if ask <= 0.0 || bid <= 0.0 {
            return;
        }

        self.ask_window.push_back((now, ask));
        while self
            .ask_window
            .front()
            .is_some_and(|(t, _)| now.saturating_sub(*t) > self.cfg.lookback_secs)
        {
            self.ask_window.pop_front();
        }
        self.trend_window.push_back((now, ask));
        while self
            .trend_window
            .front()
            .is_some_and(|(t, _)| now.saturating_sub(*t) > self.cfg.uptrend_lookback_secs)
        {
            self.trend_window.pop_front();
        }

        self.maybe_clear_halt();

        if let Some(ep) = &self.episode {
            let avg_entry = ep.avg_entry();
            let mut exit_reason: Option<&'static str> = None;
            if let Some(stop_bps) = self.cfg.stop_loss_bps {
                if bid <= avg_entry * (1.0 - stop_bps / 10_000.0) {
                    exit_reason = Some("stop_loss");
                }
            }
            if exit_reason.is_none() && bid >= avg_entry * (1.0 + self.cfg.target_bps / 10_000.0) {
                exit_reason = Some("target");
            }
            if exit_reason.is_none()
                && now.saturating_sub(ep.first_add_ts) >= self.cfg.max_hold_secs
            {
                exit_reason = Some("max_hold");
            }

            if let Some(reason) = exit_reason {
                match self.submit_taker(OrderSide::Short, ep.size, true).await {
                    Ok(_) => self.on_exit(bid, reason),
                    Err(e) => log::error!("[EXIT] order failed, position still open: {e:?}"),
                }
                self.write_status_if_due(now);
                return;
            }

            // Add another chunk?
            let cum_notional = ep.cost_usd;
            if self.entries_allowed() && cum_notional < self.cfg.max_notional_cap_usd {
                let ref_price = ep.last_add_price;
                if ask <= ref_price * (1.0 - self.cfg.add_bps / 10_000.0) {
                    let remaining = self.cfg.max_notional_cap_usd - cum_notional;
                    let add_notional = self.cfg.chunk_notional_usd.min(remaining);
                    let add_size = add_notional / ask;
                    match self.submit_taker(OrderSide::Long, add_size, false).await {
                        Ok(filled) => {
                            let filled_f = filled.to_f64().unwrap_or(add_size);
                            if let Some(ep) = &mut self.episode {
                                ep.cost_usd += filled_f * ask;
                                ep.size += filled_f;
                                ep.last_add_price = ask;
                                ep.n_adds += 1;
                            }
                            log::info!(
                                "[ADD] n_adds={} ask={:.2} add_notional=${:.0} cum_notional=${:.0}",
                                self.episode.as_ref().map(|e| e.n_adds).unwrap_or(0),
                                ask,
                                add_notional,
                                self.episode.as_ref().map(|e| e.cost_usd).unwrap_or(0.0)
                            );
                        }
                        Err(e) => log::error!("[ADD] order failed: {e:?}"),
                    }
                }
            }
        } else if self.entries_allowed() && self.uptrend_ok(ask) {
            if let Some(roll_max) = self.rolling_ask_max() {
                if ask <= roll_max * (1.0 - self.cfg.add_bps / 10_000.0) {
                    let add_size = self.cfg.chunk_notional_usd / ask;
                    match self.submit_taker(OrderSide::Long, add_size, false).await {
                        Ok(filled) => {
                            let filled_f = filled.to_f64().unwrap_or(add_size);
                            self.episode = Some(Episode {
                                first_add_ts: now,
                                cost_usd: filled_f * ask,
                                size: filled_f,
                                last_add_price: ask,
                                n_adds: 1,
                            });
                            log::info!(
                                "[ENTRY] ask={:.2} roll_max={:.2} notional=${:.0}",
                                ask,
                                roll_max,
                                self.cfg.chunk_notional_usd
                            );
                        }
                        Err(e) => log::error!("[ENTRY] order failed: {e:?}"),
                    }
                }
            }
        }

        self.write_status_if_due(now);
    }

    fn write_status_if_due(&mut self, now: u64) {
        if now.saturating_sub(self.last_status_write) < 30 {
            return;
        }
        self.last_status_write = now;
        let status = serde_json::json!({
            "ts": now,
            "instance_id": self.cfg.instance_id,
            "dry_run": self.cfg.dry_run,
            "has_position": self.episode.is_some(),
            "n_adds": self.episode.as_ref().map(|e| e.n_adds),
            "cum_notional_usd": self.episode.as_ref().map(|e| e.cost_usd),
            "session_halted": self.state.session_halted,
            "session_halt_reason": self.state.session_halt_reason,
            "realized_pnl_session": self.state.realized_pnl_session,
            "total_trades": self.state.total_trades,
            "total_wins": self.state.total_wins,
            "max_dd_bps": self.state.max_dd_bps,
            "kill_switch": self.kill_switch_engaged(),
        });
        write_status(&self.cfg.status_path, &status);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let cfg = DipGridConfig::from_env();
    log::info!(
        "[CONFIG] instance={} symbol={} dry_run={} chunk=${:.0} cap=${:.0} add={}bps target={}bps stop={:?}bps hold={}s lookback={}s require_uptrend={}",
        cfg.instance_id,
        cfg.symbol,
        cfg.dry_run,
        cfg.chunk_notional_usd,
        cfg.max_notional_cap_usd,
        cfg.add_bps,
        cfg.target_bps,
        cfg.stop_loss_bps,
        cfg.max_hold_secs,
        cfg.lookback_secs,
        cfg.require_uptrend,
    );

    if !cfg.dry_run {
        anyhow::bail!(
            "DIPGRID_DRY_RUN=false refused: this prototype has not been reviewed for live trading \
             (see bot-strategy#816 KNOWN GAPS in this file's module doc). Flip only after that review."
        );
    }

    let connector = DexConnectorBox::create(
        "lighter",
        cfg.dry_run,
        std::slice::from_ref(&cfg.symbol),
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
            "[STARTUP] resuming with session_halted=true (reason: {:?}) — new entries blocked until RISK_ACK at {}",
            state.session_halt_reason,
            cfg.risk_ack_path.display()
        );
    }

    let mut engine = DipGridEngine {
        cfg,
        connector,
        ask_window: VecDeque::new(),
        trend_window: VecDeque::new(),
        episode: None,
        state,
        last_status_write: 0,
    };

    loop {
        match price_rx.recv().await {
            Ok(update) if update.symbol == engine.cfg.symbol => {
                engine.handle_tick(update).await;
            }
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                log::warn!("[WS] price feed lagged, dropped {n} updates");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                log::error!("[WS] price feed closed, exiting");
                break;
            }
        }
    }

    Ok(())
}
