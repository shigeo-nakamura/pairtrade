//! Book runtime configuration (`configs/book/<instance>.yaml`), see
//! `docs/book-runtime.md` §2.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::directional::config_fingerprint;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BookConfig {
    pub schema_version: u32,
    pub instance_id: String,
    pub venue: String,
    pub dry_run: bool,
    pub universe: UniverseConfig,
    pub schedule: ScheduleConfig,
    pub signal: SignalConfig,
    pub sizing: SizingConfig,
    pub execution: ExecutionConfig,
    pub risk: RiskConfig,
    pub paths: PathsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct UniverseConfig {
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    IntervalDays,
    Daily,
    Calendar,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ScheduleConfig {
    pub kind: ScheduleKind,
    #[serde(default)]
    pub anchor_date: Option<NaiveDate>,
    #[serde(default)]
    pub every_days: Option<u32>,
    /// `HH:MM` UTC.
    #[serde(default)]
    pub decision_time_utc: Option<String>,
    pub signal_grace_secs: i64,
    #[serde(default)]
    pub flatten_after_secs: Option<i64>,
    #[serde(default)]
    pub calendar_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SignalConfig {
    pub path: PathBuf,
    pub producer_id: String,
    pub max_age_secs: i64,
    #[serde(default = "default_true")]
    pub require_dollar_neutral: bool,
    #[serde(default = "default_net_tolerance")]
    pub net_tolerance: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SizingConfig {
    pub gross_notional_usd: f64,
    pub max_symbol_weight: f64,
    pub max_gross_usd: f64,
    pub max_net_usd: f64,
    pub min_order_usd: f64,
    pub rebalance_deadband_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub slippage_bps: u32,
    pub max_attempts: u32,
    pub fill_confirm_timeout_secs: i64,
    /// Lighter has no price-capped IOC in dex-connector v4.7.20
    /// (`create_order_taker_ioc` is `Permanent`; bot-strategy#918): when
    /// true, a live order falls back to `create_order(price=None)`, i.e.
    /// the venue's own ±20 % protection price instead of `slippage_bps`.
    /// Default false = fail closed (the order is not sent).
    #[serde(default)]
    pub allow_venue_protection_fallback: bool,
    pub paper_slippage_bps: f64,
    #[serde(default)]
    pub paper_fee_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RiskConfig {
    pub equity_reference_usd: f64,
    pub max_session_loss_bps: f64,
    pub max_daily_loss_bps: f64,
    pub kill_switch_path: PathBuf,
    pub risk_ack_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PathsConfig {
    pub state: PathBuf,
    pub ledger: PathBuf,
    pub pnl: PathBuf,
    pub status: PathBuf,
}

fn default_true() -> bool {
    true
}

fn default_net_tolerance() -> f64 {
    0.05
}

/// Parse `HH:MM` into seconds after midnight.
pub fn parse_hhmm(s: &str) -> Result<i64> {
    let (h, m) = s
        .trim()
        .split_once(':')
        .with_context(|| format!("decision_time_utc must be HH:MM, got {s:?}"))?;
    let h: i64 = h.parse().with_context(|| format!("bad hour in {s:?}"))?;
    let m: i64 = m.parse().with_context(|| format!("bad minute in {s:?}"))?;
    if !(0..24).contains(&h) || !(0..60).contains(&m) {
        bail!("decision_time_utc out of range: {s:?}");
    }
    Ok(h * 3600 + m * 60)
}

impl BookConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read book config {}", path.display()))?;
        let cfg: BookConfig = serde_yaml::from_str(&text)
            .with_context(|| format!("parse book config {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn from_yaml_str(text: &str) -> Result<Self> {
        let cfg: BookConfig = serde_yaml::from_str(text).context("parse book config")?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != SCHEMA_VERSION {
            bail!(
                "book config schema_version {} unsupported (expected {})",
                self.schema_version,
                SCHEMA_VERSION
            );
        }
        if self.instance_id.trim().is_empty() {
            bail!("instance_id must not be empty");
        }
        if !matches!(
            self.venue.as_str(),
            "lighter" | "hyperliquid" | "hyperliquid-account"
        ) {
            bail!(
                "venue must be lighter | hyperliquid | hyperliquid-account, got {:?}",
                self.venue
            );
        }
        if self.universe.symbols.is_empty() {
            bail!("universe.symbols must not be empty");
        }
        let mut seen = std::collections::HashSet::new();
        for s in &self.universe.symbols {
            if s.trim().is_empty() || s != s.trim() {
                bail!("universe symbol {s:?} is empty or has surrounding whitespace");
            }
            if !seen.insert(s.clone()) {
                bail!("universe symbol {s:?} listed twice");
            }
        }
        let sc = &self.schedule;
        match sc.kind {
            ScheduleKind::IntervalDays => {
                if sc.anchor_date.is_none() {
                    bail!("schedule.anchor_date is required for interval_days");
                }
                match sc.every_days {
                    Some(d) if d >= 1 => {}
                    _ => bail!("schedule.every_days must be >= 1 for interval_days"),
                }
                parse_hhmm(
                    sc.decision_time_utc
                        .as_deref()
                        .context("schedule.decision_time_utc is required for interval_days")?,
                )?;
            }
            ScheduleKind::Daily => {
                parse_hhmm(
                    sc.decision_time_utc
                        .as_deref()
                        .context("schedule.decision_time_utc is required for daily")?,
                )?;
            }
            ScheduleKind::Calendar => {
                if sc.calendar_path.is_none() {
                    bail!("schedule.calendar_path is required for calendar");
                }
                if sc.flatten_after_secs.is_some() {
                    bail!("schedule.flatten_after_secs is not used with calendar (flatten_at comes from the calendar)");
                }
            }
        }
        if sc.signal_grace_secs <= 0 {
            bail!("schedule.signal_grace_secs must be > 0");
        }
        if let Some(f) = sc.flatten_after_secs {
            if f <= sc.signal_grace_secs {
                bail!("schedule.flatten_after_secs must exceed signal_grace_secs");
            }
        }
        if self.signal.producer_id.trim().is_empty() {
            bail!("signal.producer_id must not be empty");
        }
        if self.signal.max_age_secs <= 0 {
            bail!("signal.max_age_secs must be > 0");
        }
        if !(0.0..=1.0).contains(&self.signal.net_tolerance) {
            bail!("signal.net_tolerance must be within [0, 1]");
        }
        let sz = &self.sizing;
        if sz.gross_notional_usd <= 0.0 {
            bail!("sizing.gross_notional_usd must be > 0");
        }
        if !(0.0 < sz.max_symbol_weight && sz.max_symbol_weight <= 1.0) {
            bail!("sizing.max_symbol_weight must be within (0, 1]");
        }
        if sz.max_gross_usd < sz.gross_notional_usd {
            bail!("sizing.max_gross_usd must be >= gross_notional_usd");
        }
        if sz.max_net_usd < 0.0 || sz.min_order_usd < 0.0 || sz.rebalance_deadband_usd < 0.0 {
            bail!("sizing caps must be >= 0");
        }
        let ex = &self.execution;
        if ex.max_attempts == 0 {
            bail!("execution.max_attempts must be >= 1");
        }
        if ex.fill_confirm_timeout_secs <= 0 {
            bail!("execution.fill_confirm_timeout_secs must be > 0");
        }
        if ex.paper_slippage_bps < 0.0 || ex.paper_fee_bps < 0.0 {
            bail!("execution.paper_* bps must be >= 0");
        }
        let r = &self.risk;
        if r.equity_reference_usd <= 0.0 {
            bail!("risk.equity_reference_usd must be > 0");
        }
        if r.max_session_loss_bps <= 0.0 || r.max_daily_loss_bps <= 0.0 {
            bail!("risk.max_*_loss_bps must be > 0");
        }
        Ok(())
    }

    /// Seconds after midnight of the configured decision time.
    pub fn decision_time_secs(&self) -> i64 {
        self.schedule
            .decision_time_utc
            .as_deref()
            .and_then(|s| parse_hhmm(s).ok())
            .unwrap_or(0)
    }

    /// sha256-12 over the canonical JSON of the whole effective config.
    pub fn fingerprint(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_default();
        config_fingerprint(&[("book_config", json)])
    }

    /// One-line `[CONFIG]` summary for the startup log.
    pub fn log_line(&self) -> String {
        format!(
            "[CONFIG] instance={} venue={} dry_run={} schedule={:?} universe={} gross=${:.0} max_w={} max_gross=${:.0} max_net=${:.0} session_loss_bps={} daily_loss_bps={} producer={} fp={}",
            self.instance_id,
            self.venue,
            self.dry_run,
            self.schedule.kind,
            self.universe.symbols.len(),
            self.sizing.gross_notional_usd,
            self.sizing.max_symbol_weight,
            self.sizing.max_gross_usd,
            self.sizing.max_net_usd,
            self.risk.max_session_loss_bps,
            self.risk.max_daily_loss_bps,
            self.signal.producer_id,
            self.fingerprint()
        )
    }
}

#[cfg(test)]
pub(crate) fn test_config_yaml() -> String {
    r#"
schema_version: 1
instance_id: test-book
venue: lighter
dry_run: true
universe:
  symbols: [BTC, ETH, SOL, DOT]
schedule:
  kind: interval_days
  anchor_date: 2026-07-03
  every_days: 5
  decision_time_utc: "00:30"
  signal_grace_secs: 3600
signal:
  path: /tmp/does-not-matter/signal.json
  producer_id: test_producer
  max_age_secs: 7200
sizing:
  gross_notional_usd: 1000
  max_symbol_weight: 0.5
  max_gross_usd: 1100
  max_net_usd: 150
  min_order_usd: 10
  rebalance_deadband_usd: 5
execution:
  slippage_bps: 50
  max_attempts: 3
  fill_confirm_timeout_secs: 15
  paper_slippage_bps: 5
risk:
  equity_reference_usd: 1000
  max_session_loss_bps: 500
  max_daily_loss_bps: 300
  kill_switch_path: /tmp/does-not-matter/KILL_SWITCH
  risk_ack_path: /tmp/does-not-matter/RISK_ACK
paths:
  state: /tmp/does-not-matter/state.json
  ledger: /tmp/does-not-matter/ledger.jsonl
  pnl: /tmp/does-not-matter/pnl.jsonl
  status: /tmp/does-not-matter/status.json
"#
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_validates_the_reference_config() {
        let cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        assert_eq!(cfg.instance_id, "test-book");
        assert_eq!(cfg.decision_time_secs(), 30 * 60);
        assert!(cfg.signal.require_dollar_neutral);
        assert_eq!(cfg.fingerprint().len(), 12);
    }

    #[test]
    fn fingerprint_changes_with_any_field() {
        let a = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        let mut b = a.clone();
        b.sizing.max_net_usd += 1.0;
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn rejects_unknown_fields_and_bad_values() {
        let bad = test_config_yaml().replace("dry_run: true", "dry_run: true\nbogus: 1");
        assert!(BookConfig::from_yaml_str(&bad).is_err());
        let bad = test_config_yaml().replace("max_symbol_weight: 0.5", "max_symbol_weight: 1.5");
        assert!(BookConfig::from_yaml_str(&bad).is_err());
        let bad =
            test_config_yaml().replace("symbols: [BTC, ETH, SOL, DOT]", "symbols: [BTC, BTC]");
        assert!(BookConfig::from_yaml_str(&bad).is_err());
        let bad = test_config_yaml().replace(
            "decision_time_utc: \"00:30\"",
            "decision_time_utc: \"24:30\"",
        );
        assert!(BookConfig::from_yaml_str(&bad).is_err());
        let bad = test_config_yaml().replace(
            "signal_grace_secs: 3600",
            "signal_grace_secs: 3600\n  flatten_after_secs: 60",
        );
        assert!(BookConfig::from_yaml_str(&bad).is_err());
    }

    #[test]
    fn calendar_kind_requires_a_path_and_no_flatten_after() {
        let y = test_config_yaml()
            .replace("kind: interval_days", "kind: calendar")
            .replace(
                "  anchor_date: 2026-07-03\n  every_days: 5\n  decision_time_utc: \"00:30\"\n",
                "",
            );
        assert!(BookConfig::from_yaml_str(&y).is_err());
        let y = y.replace(
            "signal_grace_secs: 3600",
            "signal_grace_secs: 3600\n  calendar_path: /tmp/cal.json",
        );
        assert!(BookConfig::from_yaml_str(&y).is_ok());
    }
}
