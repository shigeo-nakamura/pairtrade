//! PnL logging and lifetime stats persistence extracted from the monolithic
//! pairtrade module.

use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};

use chrono::Utc;
use dex_connector::PositionSnapshot;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

use super::config::PairTradeConfig;
use super::state::PositionDirection;

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct PnlLogRecord {
    pub(super) ts: i64,
    pub(super) pair: String,
    pub(super) base: String,
    pub(super) quote: String,
    pub(super) direction: String,
    pub(super) pnl: f64,
    pub(super) source: String,
    // Trade log fields for backtest calibration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) entry_price_a: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) entry_price_b: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) exit_price_a: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) exit_price_b: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) beta: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) z_entry: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) z_exit: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) hold_secs: Option<f64>,
    /// Position size at the time of the trade. Set for `startup_force_close`
    /// records (single-leg positions detected at boot) so operators can
    /// reconcile against the DEX-side `realized_pnl.csv`. None for normal
    /// strategy entries/exits since size is implied by the strategy state.
    /// bot-strategy#269 Phase 3.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) size: Option<f64>,
    /// Net funding paid (negative) or received (positive) over the cycle.
    /// Computed by walking the rolling WS funding-rate history per leg
    /// during `[entry_ts, exit_ts)`; per-leg notionals captured at entry.
    /// Sign convention matches the exchange `Payment` column (CSV
    /// funding-history export): positive ⇒ strategy received. Note this
    /// is the opposite sign of `market::net_funding_for_direction`, which
    /// is left as-is for the (latent) entry-threshold filter only — the
    /// two log lines no longer share a convention as of
    /// bot-strategy#414. Field is added at exit time only — `None` on
    /// records that pre-date the cycle's entry or on
    /// `startup_force_close`. bot-strategy#364 / #414.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) funding_carry_usd: Option<f64>,
    /// Number of hourly funding ticks observed across both legs during the
    /// hold (`base_ticks + quote_ticks`). Divide by 2 to approximate hours
    /// of coverage. Useful for spotting cycles where the WS lost ticks and
    /// `funding_carry_usd` undercounts. bot-strategy#364.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) funding_ticks_observed: Option<u32>,
}

pub(super) struct PnlLogger {
    dir: PathBuf,
    tag: Option<String>,
    retain_days: u64,
    last_cleanup: Option<Instant>,
}

impl PnlLogger {
    /// Per-instance variant of `from_env` for the multi-strategy
    /// single-process architecture (shigeo-nakamura/bot-strategy#25).
    ///
    /// When `multi_instance == false`, behavior is identical to
    /// `from_env(cfg)` so existing single-bot deployments keep writing
    /// to and reading from the same `pnl-<tag>-<date>.jsonl` files,
    /// preserving the lifetime-stats restore path.
    ///
    /// When `multi_instance == true`, the resolved tag is suffixed with
    /// `-{instance_id}` so each variant gets its own log files and
    /// `load_lifetime_stats()` only sees its own history.
    pub(super) fn from_env_for_instance(
        cfg: &PairTradeConfig,
        instance_id: &str,
        multi_instance: bool,
    ) -> Option<Self> {
        let mut logger = Self::from_env(cfg)?;
        if multi_instance {
            let suffix = sanitize_pnl_tag(instance_id);
            if !suffix.is_empty() {
                logger.tag = Some(match logger.tag {
                    Some(base) if !base.is_empty() => format!("{base}-{suffix}"),
                    _ => suffix,
                });
            }
        }
        Some(logger)
    }

    pub(super) fn from_env(cfg: &PairTradeConfig) -> Option<Self> {
        let enabled = env::var("DEBOT_PNL_LOG")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                !(v == "0" || v == "false" || v == "no")
            })
            .unwrap_or(true);
        if !enabled {
            return None;
        }
        let dir = env::var("DEBOT_PNL_DIR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                env::var("HOME")
                    .ok()
                    .map(|home| PathBuf::from(home).join("debot_pnl"))
            })
            .unwrap_or_else(|| PathBuf::from("debot_pnl"));
        let tag = env::var("DEBOT_PNL_TAG")
            .ok()
            .or_else(|| env::var("AGENT_NAME").ok())
            .or_else(|| cfg.agent_name.clone())
            .or_else(|| env::var("DEX_NAME").ok())
            .or_else(|| Some(cfg.dex_name.clone()))
            .map(|v| sanitize_pnl_tag(&v))
            .filter(|v| !v.is_empty());
        let retain_days = env::var("DEBOT_PNL_RETAIN_DAYS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(7)
            .max(1);
        Some(Self {
            dir,
            tag,
            retain_days,
            last_cleanup: None,
        })
    }

    pub(super) fn log(&mut self, record: PnlLogRecord) -> std::io::Result<()> {
        fs::create_dir_all(&self.dir)?;
        let path = self.log_path();
        let line = serde_json::to_string(&record)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        writeln!(file, "{line}")?;
        self.maybe_cleanup();
        Ok(())
    }

    fn log_path(&self) -> PathBuf {
        let date = Utc::now().format("%Y%m%d").to_string();
        let mut name = String::from("pnl");
        if let Some(tag) = &self.tag {
            name.push('-');
            name.push_str(tag);
        }
        name.push('-');
        name.push_str(&date);
        name.push_str(".jsonl");
        self.dir.join(name)
    }

    fn maybe_cleanup(&mut self) {
        let due = self
            .last_cleanup
            .map(|t| t.elapsed() >= Duration::from_secs(21_600))
            .unwrap_or(true);
        if !due {
            return;
        }
        self.last_cleanup = Some(Instant::now());
        let cutoff = SystemTime::now()
            .checked_sub(Duration::from_secs(self.retain_days.saturating_mul(86_400)))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !is_pnl_log_file(&path) {
                continue;
            }
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            let Ok(modified) = metadata.modified() else {
                continue;
            };
            if modified < cutoff {
                let _ = fs::remove_file(path);
            }
        }
    }
}

/// Append one record per pre-existing position to a dedicated
/// `pnl-startup_close-<tag>-<date>.jsonl` file (bot-strategy#269 Phase 3).
///
/// `[Startup] Force closing` runs before the strategy loop and uses the
/// connector's bulk `close_all_positions` path, which does not flow through
/// `write_pnl_record`. As a result kill events were invisible to anything
/// downstream of the strategy pnl log, and only surfaced via the DEX-side
/// `realized_pnl.csv`. This helper records what was about to be killed so
/// the event is durable beyond journalctl's 7-day retention.
///
/// The output filename matches the `pnl-*.jsonl` glob, so it is picked up
/// by `is_pnl_log_file` for retention cleanup and by any tooling that
/// already aggregates `debot_pnl/pnl-*.jsonl`. Records carry
/// `source = "startup_force_close"` so downstream filters can split them
/// from strategy-driven trades.
pub(super) fn log_startup_force_close(
    cfg: &PairTradeConfig,
    positions: &[PositionSnapshot],
) -> std::io::Result<()> {
    if positions.is_empty() {
        return Ok(());
    }
    let enabled = env::var("DEBOT_PNL_LOG")
        .ok()
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            !(v == "0" || v == "false" || v == "no")
        })
        .unwrap_or(true);
    if !enabled {
        return Ok(());
    }

    let dir = env::var("DEBOT_PNL_DIR")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            env::var("HOME")
                .ok()
                .map(|home| PathBuf::from(home).join("debot_pnl"))
        })
        .unwrap_or_else(|| PathBuf::from("debot_pnl"));
    let tag = env::var("DEBOT_PNL_TAG")
        .ok()
        .or_else(|| env::var("AGENT_NAME").ok())
        .or_else(|| cfg.agent_name.clone())
        .or_else(|| env::var("DEX_NAME").ok())
        .or_else(|| Some(cfg.dex_name.clone()))
        .map(|v| sanitize_pnl_tag(&v))
        .filter(|v| !v.is_empty());

    fs::create_dir_all(&dir)?;
    let date = Utc::now().format("%Y%m%d").to_string();
    let mut name = String::from("pnl-startup_close");
    if let Some(t) = &tag {
        name.push('-');
        name.push_str(t);
    }
    name.push('-');
    name.push_str(&date);
    name.push_str(".jsonl");
    let path = dir.join(name);

    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    let ts = Utc::now().timestamp();
    for p in positions {
        let direction = if p.sign >= 0 { "long" } else { "short" };
        let entry = p.entry_price.and_then(|d| d.to_f64());
        let size = p.size.to_f64().unwrap_or(0.0);
        let record = PnlLogRecord {
            ts,
            pair: p.symbol.clone(),
            base: p.symbol.clone(),
            quote: String::new(),
            direction: direction.to_string(),
            pnl: 0.0,
            source: "startup_force_close".to_string(),
            entry_price_a: entry,
            entry_price_b: None,
            exit_price_a: None,
            exit_price_b: None,
            beta: None,
            z_entry: None,
            z_exit: None,
            hold_secs: None,
            size: Some(size),
            funding_carry_usd: None,
            funding_ticks_observed: None,
        };
        let line = serde_json::to_string(&record)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        writeln!(file, "{line}")?;
    }
    Ok(())
}

pub(super) fn sanitize_pnl_tag(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub(super) fn is_pnl_log_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    name.starts_with("pnl-") && name.ends_with(".jsonl")
}

pub(super) fn direction_label(direction: PositionDirection) -> &'static str {
    match direction {
        PositionDirection::LongSpread => "long_spread",
        PositionDirection::ShortSpread => "short_spread",
    }
}

impl PnlLogRecord {
    pub(super) fn new(
        base: &str,
        quote: &str,
        direction: PositionDirection,
        pnl: f64,
        ts: i64,
        source: &str,
    ) -> Self {
        Self {
            ts,
            pair: format!("{}/{}", base, quote),
            base: base.to_string(),
            quote: quote.to_string(),
            direction: direction_label(direction).to_string(),
            pnl,
            source: source.to_string(),
            entry_price_a: None,
            entry_price_b: None,
            exit_price_a: None,
            exit_price_b: None,
            beta: None,
            z_entry: None,
            z_exit: None,
            hold_secs: None,
            size: None,
            funding_carry_usd: None,
            funding_ticks_observed: None,
        }
    }

    pub(super) fn with_funding(mut self, carry_usd: f64, ticks_observed: u32) -> Self {
        self.funding_carry_usd = Some(carry_usd);
        self.funding_ticks_observed = Some(ticks_observed);
        self
    }

    pub(super) fn with_trade_details(
        mut self,
        entry_a: Option<f64>,
        entry_b: Option<f64>,
        exit_a: Option<f64>,
        exit_b: Option<f64>,
        beta: Option<f64>,
        z_entry: Option<f64>,
        z_exit: Option<f64>,
        hold_secs: Option<f64>,
    ) -> Self {
        self.entry_price_a = entry_a;
        self.entry_price_b = entry_b;
        self.exit_price_a = exit_a;
        self.exit_price_b = exit_b;
        self.beta = beta;
        self.z_entry = z_entry;
        self.z_exit = z_exit;
        self.hold_secs = hold_secs;
        self
    }
}
