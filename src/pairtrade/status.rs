//! Status snapshot/equity reporting and shutdown-status types extracted
//! from the monolithic pairtrade module. The reporter writes a JSON status
//! file consumed by the dashboard, plus an equity history JSONL.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use chrono::{NaiveDate, Utc};
use dex_connector::PositionSnapshot;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::config::PairTradeConfig;
use super::pnl_log::sanitize_pnl_tag;
use super::s3_mirror::S3Mirror;
use crate::error_counter::{self, ErrorSummary};

use std::env;

/// Process-wide capture of bot start time. Lazily initialized on the
/// first `StatusReporter::from_env` call (= during engine boot) and
/// shared across all per-instance reporters so every status object
/// reports the same value. See bot-strategy#343.
static PROCESS_STARTED_AT: OnceLock<i64> = OnceLock::new();

pub(in crate::pairtrade) fn process_started_at() -> i64 {
    *PROCESS_STARTED_AT.get_or_init(|| Utc::now().timestamp())
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct EquityBaseline {
    pub(super) date: String,
    pub(super) equity: f64,
}

#[derive(Debug, Serialize)]
pub(super) struct EquityHistoryPoint {
    pub(super) ts: i64,
    pub(super) equity: f64,
}

#[derive(Debug)]
pub(super) struct StatusReporter {
    pub(super) path: PathBuf,
    pub(super) id: Option<String>,
    pub(super) agent: Option<String>,
    pub(super) dex: String,
    pub(super) dry_run: bool,
    pub(super) backtest_mode: bool,
    pub(super) interval_secs: u64,
    pub(super) snapshot_every: Duration,
    pub(super) pnl_total: f64,
    pub(super) pnl_today: f64,
    pub(super) pnl_today_date: NaiveDate,
    /// Today's per-cycle funding carry sum, pushed each tick by the
    /// engine from `StrategyInstance.funding_carry_today`. Surfaced on
    /// status.json so the dashboard can render funding attribution
    /// alongside `pnl_today`. bot-strategy#371.
    pub(super) funding_carry_today: f64,
    pub(super) equity_day_start: f64,
    pub(super) equity_day_start_set: bool,
    pub(super) equity_baseline_path: PathBuf,
    pub(super) equity_history_path: PathBuf,
    pub(super) last_equity_history_ts: Option<i64>,
    pub(super) last_snapshot: Option<Instant>,
    pub(super) trade_stats: Option<PairTradeStats>,
    pub(super) maintenance: Option<String>,
    pub(super) shutdown: Option<ShutdownStatus>,
    /// Daily DD snapshot surfaced to the dashboard. Set by the engine
    /// every tick via `set_daily_risk`. None until the first rollover
    /// runs (i.e. `session_start_equity > 0`). See bot-strategy#185
    /// Phase 2-4.
    pub(super) daily_risk: Option<DailyRiskSnapshot>,
    /// Phase 3-1 session-DD snapshot. None until the threshold is
    /// enabled and the first equity sample is taken.
    pub(super) session_risk: Option<SessionRiskSnapshot>,
    /// Circuit-breaker view (consecutive losses + cooldown). Always
    /// present once the engine has run at least one tick on the
    /// instance; the field is None only briefly at startup.
    pub(super) circuit_breaker: Option<CircuitBreakerSnapshot>,
    /// Bounded ring buffer (capacity 200) of recent halt transitions
    /// across all gates. Filled on startup from `risk_history_path`,
    /// pushed on each `record_risk_event` call, and serialised inline
    /// into `status.json` so the dashboard renders the strip without
    /// an extra SSM round trip. See bot-strategy#231 Phase B.
    pub(super) risk_history: std::collections::VecDeque<RiskHistoryEvent>,
    /// Sibling jsonl file storing a longer audit log of halt events.
    /// Append-only — bot startup re-loads the tail into `risk_history`
    /// so a restart preserves recent context.
    pub(super) risk_history_path: PathBuf,
    /// Epoch seconds at which `from_env` constructed this reporter
    /// (= process startup). Surfaced as `process_started_at` in the
    /// snapshot so the dashboard can show service-uptime without an
    /// SSM `systemctl ActiveEnterTimestamp` round-trip (#343).
    pub(super) process_started_at: i64,
    /// Cached value of the most recent KILL_SWITCH sentinel-file
    /// existence check, mirrored from `PairTradeEngine::kill_switch_active`
    /// via `set_kill_switch`. Replaces the dashboard's
    /// `cat /opt/debot/KILL_SWITCH` SSM probe (#343).
    pub(super) kill_switch_active: bool,
    /// Optional S3 mirror; populated when STATUS_S3_BUCKET /
    /// STATUS_S3_KEY_PREFIX are set (#343). None → local-only writes.
    pub(super) s3_mirror: Option<Arc<S3Mirror>>,
    /// Byte length of `equity_history.jsonl` at the most recent
    /// successful S3 put. Re-uploads only fire when the file has grown,
    /// avoiding wasted bytes on the per-tick write_snapshot cadence.
    /// `None` until the first put. See bot-strategy#343 Phase 3.
    pub(super) last_equity_history_uploaded_len: Option<u64>,
    /// Same idea as `last_equity_history_uploaded_len`, for the rare
    /// `backtest_alert.json` sibling file.
    pub(super) last_backtest_alert_uploaded_len: Option<u64>,
}

/// Halt-history entry emitted in `status.json` and persisted to
/// `risk_history.jsonl`. One row per state transition (activate /
/// clear / ack) for any of the four risk gates. See bot-strategy#231
/// Phase B.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct RiskHistoryEvent {
    pub(super) ts: i64,
    pub(super) instance_id: String,
    /// Which gate fired: "kill_switch" | "daily_dd" | "session_dd"
    /// | "circuit_breaker".
    pub(super) kind: String,
    /// State transition direction: "activated" | "cleared" | "ack".
    /// "ack" applies only to session_dd (manual clear via RISK_ACK).
    pub(super) event_type: String,
    /// Human-readable reason where one is available (e.g. session DD
    /// reason string, KILL_SWITCH path). Optional so circuit-breaker
    /// auto-clear can omit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reason: Option<String>,
    /// Free-form JSON detail (observed bps, threshold, cooldown_secs,
    /// etc.). Renderer is best-effort; missing fields don't break.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) detail: Option<serde_json::Value>,
}

const RISK_HISTORY_BUFFER_CAP: usize = 200;

/// Per-instance realized daily-DD view emitted in `status.json` so the
/// dashboard can surface the live halt state without duplicating the
/// threshold calculation. `daily_pnl` is realized-only (closed trades);
/// the equity-delta-based `pnl_today` above stays for backward compat.
///
/// `max_daily_loss_bps` is the raw config value (1x-equivalent market-move
/// bps); `effective_max_daily_loss_bps` is the value after the
/// `× max_leverage` scaling that the bot actually compares `daily_pnl_bps`
/// against. Dashboards comparing dd-vs-threshold should use the effective
/// field.
#[derive(Debug, Clone, Serialize)]
pub(super) struct DailyRiskSnapshot {
    pub(super) daily_pnl: f64,
    pub(super) daily_pnl_bps: f64,
    pub(super) session_start_equity: f64,
    pub(super) session_start_ts: i64,
    pub(super) max_daily_loss_bps: u32,
    pub(super) effective_max_daily_loss_bps: f64,
    pub(super) risk_halted: bool,
}

/// Per-instance circuit-breaker view emitted in `status.json` so the
/// dashboard can render the live cooldown countdown without duplicating
/// the threshold logic. `consecutive_losses` resets on a winning trade
/// (early de-arming) and `until_ts` is set on threshold breach with the
/// configured cooldown. `active=true` when `until_ts` is in the future
/// — entries are blocked until either cooldown expires (auto) or a
/// winning trade resets the counter (rare in single-position-per-instance
/// mode; usually cooldown is the only path back). See bot-strategy#185
/// Phase 1-3.
#[derive(Debug, Clone, Serialize)]
pub(super) struct CircuitBreakerSnapshot {
    pub(super) consecutive_losses: u32,
    pub(super) active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) until_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cooldown_remaining_secs: Option<i64>,
    pub(super) tier1_threshold: u32,
    pub(super) tier2_threshold: u32,
}

/// Phase 3-1 rolling-peak DD view. Only present once the bot has at
/// least one persisted equity sample and the threshold is enabled.
/// `session_halted` distinguishes the sticky Phase 3 halt (cleared by
/// manual ack) from the auto-clearing daily-DD halt above.
///
/// `max_session_loss_bps` is the raw config value (1x-equivalent
/// market-move bps); `effective_max_session_loss_bps` is the value after
/// the `× max_leverage` scaling that the bot actually compares `dd_bps`
/// against. Dashboards comparing dd-vs-threshold should use the
/// effective field.
#[derive(Debug, Clone, Serialize)]
pub(super) struct SessionRiskSnapshot {
    pub(super) current_equity: f64,
    pub(super) peak_equity: f64,
    pub(super) dd_bps: f64,
    pub(super) max_session_loss_bps: u32,
    pub(super) effective_max_session_loss_bps: f64,
    pub(super) lookback_secs: u64,
    pub(super) sample_count: usize,
    pub(super) session_halted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) halt_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) halt_ts: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(super) struct StatusPosition {
    pub(super) symbol: String,
    pub(super) side: String,
    pub(super) size: String,
    pub(super) entry_price: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct StatusSnapshot {
    pub(super) ts: i64,
    pub(super) updated_at: String,
    /// Epoch seconds at which the bot process booted. Replaces the
    /// dashboard's `systemctl ActiveEnterTimestamp` SSM probe — see
    /// bot-strategy#343. Captured once at `StatusReporter` construction
    /// (process startup).
    pub(super) process_started_at: i64,
    pub(super) id: Option<String>,
    pub(super) agent: Option<String>,
    pub(super) dex: String,
    pub(super) dry_run: bool,
    pub(super) backtest_mode: bool,
    pub(super) interval_secs: u64,
    pub(super) positions_ready: bool,
    pub(super) position_count: usize,
    pub(super) has_position: bool,
    pub(super) positions: Vec<StatusPosition>,
    pub(super) pnl_total: f64,
    pub(super) pnl_today: f64,
    pub(super) pnl_source: String,
    /// Today's per-cycle funding carry sum. Same UTC-day window as
    /// `pnl_today`. Always emitted (defaults to 0.0 when no cycles have
    /// closed today) so the dashboard distinguishes "0 funding today"
    /// from "field missing on an older binary". bot-strategy#371.
    pub(super) funding_carry_today: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) trade_stats: Option<PairTradeStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) maintenance: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) shutdown: Option<ShutdownStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) error_summary: Option<ErrorSummary>,
    /// Mirror of the dashboard's old journalctl `Connection reset...`
    /// counter, sampled from `error_counter::ws_reset_24h_count` at
    /// snapshot time (#343).
    pub(super) ws_reset_24h_count: u64,
    /// Mirror of the dashboard's old `cat /opt/debot/KILL_SWITCH` probe
    /// (#343). Set by the engine via `set_kill_switch` once per tick.
    pub(super) kill_switch_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) daily_risk: Option<DailyRiskSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) session_risk: Option<SessionRiskSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) circuit_breaker: Option<CircuitBreakerSnapshot>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(super) risk_history: Vec<RiskHistoryEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct PairTradeStats {
    pub(super) trades: u64,
    pub(super) wins: u64,
    pub(super) win_rate: f64,
    pub(super) max_dd: f64,
    pub(super) pnl: f64,
}

/// Graceful shutdown status surfaced in the status snapshot so the
/// dashboard can show when the bot is winding down and when each open
/// leg will be auto-flushed by `force_close_secs`. See pairtrade#6.
#[derive(Debug, Clone, Serialize)]
pub(super) struct ShutdownStatus {
    pub(super) pending: bool,
    /// Unix timestamp (s) at which the grace window expires and any
    /// remaining positions will be force-closed unconditionally.
    pub(super) grace_deadline_ts: i64,
    /// Earliest force_close ETA across all open positions (Unix ts, s).
    /// None when there are no open positions at shutdown start.
    pub(super) force_close_eta_ts: Option<i64>,
    pub(super) positions: Vec<ShutdownPosition>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ShutdownPosition {
    pub(super) key: String,
    pub(super) entered_ts: i64,
    pub(super) force_close_eta_ts: i64,
}

impl StatusReporter {
    /// Per-instance variant of `from_env` for the multi-strategy
    /// single-process architecture (shigeo-nakamura/bot-strategy#25).
    ///
    /// When `multi_instance == false`, returns exactly what `from_env`
    /// returns so single-bot deployments keep writing to the same
    /// `<dir>/<DEBOT_STATUS_ID>/status.json` path and the dashboard
    /// keeps finding it.
    ///
    /// When `multi_instance == true`, the on-disk directory is
    /// suffixed with `-{instance_id}` so each strategy variant has its
    /// own status.json that the dashboard can subscribe to via a
    /// separate `status_path` entry.
    pub(super) fn from_env_for_instance(
        cfg: &PairTradeConfig,
        instance_id: &str,
        multi_instance: bool,
    ) -> Option<Self> {
        let reporter = Self::from_env(cfg)?;
        if !multi_instance {
            return Some(reporter);
        }
        let suffix = sanitize_pnl_tag(instance_id);
        if suffix.is_empty() {
            return Some(reporter);
        }
        let mut reporter = reporter;
        // Rewrite the on-disk parent directory to include the instance
        // suffix. The original layout is `<dir>/<id>/status.json`; the
        // new layout is `<dir>/<id>-<instance>/status.json`. When `id`
        // is None we degrade to `<dir>/<instance>/status.json`.
        if let Some(parent) = reporter.path.parent() {
            let last = parent
                .file_name()
                .map(|os| os.to_string_lossy().into_owned())
                .unwrap_or_default();
            let new_last = if last.is_empty() {
                suffix.clone()
            } else {
                format!("{last}-{suffix}")
            };
            let grand = parent.parent().map(PathBuf::from).unwrap_or_default();
            let new_parent = grand.join(new_last);
            let file_name = reporter
                .path
                .file_name()
                .map(|os| os.to_string_lossy().into_owned())
                .unwrap_or_else(|| "status.json".to_string());
            reporter.path = new_parent.join(file_name);
        }
        // Keep auxiliary files (`equity.json`, `equity_history.jsonl`,
        // `risk_history.jsonl`) co-located with the rewritten status.json.
        reporter.equity_baseline_path = reporter.path.with_extension("equity.json");
        reporter.equity_history_path = reporter.path.with_extension("equity_history.jsonl");
        reporter.risk_history_path = reporter
            .path
            .parent()
            .map(|dir| dir.join("risk_history.jsonl"))
            .unwrap_or_else(|| PathBuf::from("risk_history.jsonl"));
        reporter.id = Some(match reporter.id.take() {
            Some(prev) if !prev.is_empty() => format!("{prev}-{suffix}"),
            _ => suffix,
        });
        // Reload risk history from the rewritten path so each instance's
        // jsonl populates its own ring buffer (not the shared parent's).
        reporter.risk_history.clear();
        reporter.load_risk_history();
        // bot-strategy#382 follow-up: discard any baseline loaded by
        // `from_env` from the pre-suffix parent directory and reload from
        // the per-instance subdir. Without this reset, a stale parent
        // `status.equity.json` (left from a pre-multi-instance deployment)
        // populates `equity_day_start_set=true` with an old `pnl_today_date`,
        // and the first `write_snapshot_if_due` — which can race
        // `fetch_equity_rest` on the per-account stagger — fires
        // `reset_daily_if_needed`'s reset block with `pnl_total=0`,
        // poisoning the per-instance subdir baseline with `equity=0` for
        // the rest of the UTC day. Observed live on Tokyo Lighter B/C on
        // 2026-05-14 after a 4/9-era parent baseline survived the
        // single-instance → A/B/C cutover: dashboard showed pnl_today=+$150
        // each (sum +$300) with no trades. Frankfurt was unaffected because
        // its parent baseline does not exist.
        reporter.equity_day_start = 0.0;
        reporter.equity_day_start_set = false;
        reporter.pnl_today = 0.0;
        reporter.pnl_today_date = Utc::now().date_naive();
        reporter.load_equity_baseline();
        Some(reporter)
    }

    pub(super) fn from_env(cfg: &PairTradeConfig) -> Option<Self> {
        let enabled = env::var("DEBOT_STATUS_ENABLED")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                !(v == "0" || v == "false" || v == "no")
            })
            .unwrap_or(true);
        if !enabled {
            return None;
        }

        let id = env::var("DEBOT_STATUS_ID")
            .ok()
            .map(|v| sanitize_pnl_tag(&v))
            .filter(|v| !v.is_empty());

        let path = env::var("DEBOT_STATUS_PATH")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                env::var("DEBOT_STATUS_DIR")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
                    .map(PathBuf::from)
                    .map(|dir| match &id {
                        Some(id) => dir.join(id).join("status.json"),
                        None => dir.join("status.json"),
                    })
            })
            .or_else(|| {
                env::var("HOME")
                    .ok()
                    .map(|home| PathBuf::from(home).join("debot_status"))
                    .map(|base| match &id {
                        Some(id) => base.join(id).join("status.json"),
                        None => base.join("status.json"),
                    })
            })
            .unwrap_or_else(|| PathBuf::from("status.json"));

        let equity_baseline_path = path.with_extension("equity.json");
        let equity_history_path = path.with_extension("equity_history.jsonl");
        let risk_history_path = path
            .parent()
            .map(|dir| dir.join("risk_history.jsonl"))
            .unwrap_or_else(|| PathBuf::from("risk_history.jsonl"));
        let interval_secs = cfg.interval_secs.max(1);
        let snapshot_every = {
            let target_secs = 60_u64;
            let n = target_secs.div_ceil(interval_secs).max(1);
            Duration::from_secs(interval_secs.saturating_mul(n).max(1))
        };

        let mut reporter = Self {
            path,
            id,
            agent: cfg.agent_name.clone(),
            dex: cfg.dex_name.clone(),
            dry_run: cfg.dry_run,
            backtest_mode: cfg.backtest_mode,
            interval_secs: cfg.interval_secs,
            snapshot_every,
            pnl_total: 0.0,
            pnl_today: 0.0,
            pnl_today_date: Utc::now().date_naive(),
            funding_carry_today: 0.0,
            equity_day_start: 0.0,
            equity_day_start_set: false,
            equity_baseline_path,
            equity_history_path,
            last_equity_history_ts: None,
            last_snapshot: None,
            process_started_at: process_started_at(),
            kill_switch_active: false,
            s3_mirror: S3Mirror::from_env(),
            last_equity_history_uploaded_len: None,
            last_backtest_alert_uploaded_len: None,
            trade_stats: Some(PairTradeStats {
                trades: 0,
                wins: 0,
                win_rate: 0.0,
                max_dd: 0.0,
                pnl: 0.0,
            }),
            maintenance: None,
            shutdown: None,
            daily_risk: None,
            session_risk: None,
            circuit_breaker: None,
            risk_history: std::collections::VecDeque::with_capacity(RISK_HISTORY_BUFFER_CAP),
            risk_history_path,
        };
        reporter.load_equity_baseline();
        reporter.load_risk_history();
        if let Err(err) = reporter.ensure_status_file() {
            log::warn!(
                "[STATUS] failed to create status file {}: {:?}",
                reporter.path.display(),
                err
            );
        }
        Some(reporter)
    }

    pub(super) fn ensure_status_file(&self) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        Ok(())
    }

    pub(super) fn load_equity_baseline(&mut self) {
        let Ok(payload) = fs::read_to_string(&self.equity_baseline_path) else {
            return;
        };
        let Ok(baseline) = serde_json::from_str::<EquityBaseline>(&payload) else {
            return;
        };
        let Ok(date) = NaiveDate::parse_from_str(&baseline.date, "%Y-%m-%d") else {
            return;
        };
        self.equity_day_start = baseline.equity;
        self.pnl_today_date = date;
        self.equity_day_start_set = true;
    }

    pub(super) fn persist_equity_baseline(&self) {
        let baseline = EquityBaseline {
            date: self.pnl_today_date.format("%Y-%m-%d").to_string(),
            equity: self.equity_day_start,
        };
        let payload = match serde_json::to_string(&baseline) {
            Ok(v) => v,
            Err(err) => {
                log::warn!("[STATUS] failed to encode equity baseline: {:?}", err);
                return;
            }
        };
        if let Some(parent) = self.equity_baseline_path.parent() {
            if let Err(err) = fs::create_dir_all(parent) {
                log::warn!("[STATUS] failed to create equity baseline dir: {:?}", err);
                return;
            }
        }
        let tmp_path = self.equity_baseline_path.with_extension("equity.json.tmp");
        if let Err(err) = fs::write(&tmp_path, payload) {
            log::warn!("[STATUS] failed to write equity baseline: {:?}", err);
            return;
        }
        if let Err(err) = fs::rename(&tmp_path, &self.equity_baseline_path) {
            log::warn!("[STATUS] failed to finalize equity baseline: {:?}", err);
        }
    }

    pub(super) fn append_equity_history(&mut self, equity: f64) {
        let ts = Utc::now().timestamp_millis();
        if self.last_equity_history_ts == Some(ts) {
            return;
        }
        self.last_equity_history_ts = Some(ts);
        let point = EquityHistoryPoint { ts, equity };
        let line = match serde_json::to_string(&point) {
            Ok(v) => v,
            Err(err) => {
                log::warn!("[STATUS] failed to encode equity history: {:?}", err);
                return;
            }
        };
        if let Some(parent) = self.equity_history_path.parent() {
            if let Err(err) = fs::create_dir_all(parent) {
                log::warn!("[STATUS] failed to create equity history dir: {:?}", err);
                return;
            }
        }
        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.equity_history_path)
        {
            Ok(f) => f,
            Err(err) => {
                log::warn!("[STATUS] failed to open equity history: {:?}", err);
                return;
            }
        };
        if writeln!(file, "{line}").is_err() {
            log::warn!("[STATUS] failed to write equity history");
        }
    }

    /// Append a single risk history event to the in-memory ring buffer
    /// and the on-disk jsonl. Caller is responsible for not emitting
    /// duplicate events for the same transition (we don't dedupe here
    /// — call sites are gated on edge-triggered "old != new" checks).
    /// See bot-strategy#231 Phase B.
    pub(super) fn record_risk_event(&mut self, event: RiskHistoryEvent) {
        // Bounded ring buffer: drop the oldest when full.
        if self.risk_history.len() >= RISK_HISTORY_BUFFER_CAP {
            self.risk_history.pop_front();
        }
        // Best-effort persistence: serialise as a single JSON line.
        // Failures are warned but do not block the in-memory path so
        // the dashboard still gets the live event.
        if let Err(err) = self.append_risk_history_line(&event) {
            log::warn!("[STATUS] failed to persist risk_history event: {:?}", err);
        }
        self.risk_history.push_back(event);
    }

    fn append_risk_history_line(&self, event: &RiskHistoryEvent) -> std::io::Result<()> {
        if let Some(parent) = self.risk_history_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let line = serde_json::to_string(event)
            .map_err(std::io::Error::other)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.risk_history_path)?;
        writeln!(file, "{line}")
    }

    /// Load the tail of `risk_history.jsonl` into the ring buffer on
    /// startup so a bot restart preserves recent halt context. Best
    /// effort — file missing / parse errors are silent (the buffer
    /// just starts empty in that case).
    fn load_risk_history(&mut self) {
        let raw = match fs::read_to_string(&self.risk_history_path) {
            Ok(s) => s,
            Err(_) => return,
        };
        // Take the last RISK_HISTORY_BUFFER_CAP non-empty lines.
        let mut lines: Vec<&str> = raw.lines().filter(|l| !l.trim().is_empty()).collect();
        let drop_n = lines.len().saturating_sub(RISK_HISTORY_BUFFER_CAP);
        if drop_n > 0 {
            lines.drain(..drop_n);
        }
        for line in lines {
            if let Ok(event) = serde_json::from_str::<RiskHistoryEvent>(line) {
                self.risk_history.push_back(event);
            }
        }
    }

    pub(super) fn update_equity(&mut self, equity: f64) {
        let today = Utc::now().date_naive();
        self.pnl_total = equity;
        if !self.equity_day_start_set || self.pnl_today_date != today {
            self.pnl_today_date = today;
            self.equity_day_start = equity;
            self.equity_day_start_set = true;
            self.persist_equity_baseline();
        }
        if self.equity_day_start_set {
            self.pnl_today = equity - self.equity_day_start;
        }
        self.append_equity_history(equity);
    }

    pub(super) fn set_maintenance(&mut self, status: Option<String>) {
        self.maintenance = status;
    }

    pub(super) fn set_shutdown_status(&mut self, status: Option<ShutdownStatus>) {
        self.shutdown = status;
    }

    pub(super) fn set_daily_risk(&mut self, risk: Option<DailyRiskSnapshot>) {
        self.daily_risk = risk;
    }

    pub(super) fn set_session_risk(&mut self, risk: Option<SessionRiskSnapshot>) {
        self.session_risk = risk;
    }

    pub(super) fn set_circuit_breaker(&mut self, cb: Option<CircuitBreakerSnapshot>) {
        self.circuit_breaker = cb;
    }

    /// Mirror the engine's KILL_SWITCH sentinel-file state into the
    /// next status snapshot. Called once per tick alongside the other
    /// risk setters. See bot-strategy#343.
    pub(super) fn set_kill_switch(&mut self, active: bool) {
        self.kill_switch_active = active;
    }

    /// Push today's per-cycle funding carry sum from
    /// `StrategyInstance.funding_carry_today` into the next status
    /// snapshot. Called once per tick from the engine. See
    /// bot-strategy#371.
    pub(super) fn set_funding_today(&mut self, value: f64) {
        self.funding_carry_today = value;
    }

    pub(super) fn write_snapshot(
        &mut self,
        open_positions: &HashMap<String, PositionSnapshot>,
        positions_ready: bool,
    ) -> std::io::Result<()> {
        self.reset_daily_if_needed();
        let positions: Vec<StatusPosition> = open_positions
            .values()
            .filter(|pos| pos.sign != 0 && pos.size > Decimal::ZERO)
            .map(|pos| StatusPosition {
                symbol: pos.symbol.clone(),
                side: match pos.sign.cmp(&0) {
                    Ordering::Greater => "LONG".to_string(),
                    Ordering::Less => "SHORT".to_string(),
                    Ordering::Equal => "FLAT".to_string(),
                },
                size: pos.size.to_string(),
                entry_price: pos.entry_price.map(|v| v.to_string()),
            })
            .collect();
        let snapshot = StatusSnapshot {
            ts: Utc::now().timestamp(),
            updated_at: Utc::now().to_rfc3339(),
            process_started_at: self.process_started_at,
            id: self.id.clone(),
            agent: self.agent.clone(),
            dex: self.dex.clone(),
            dry_run: self.dry_run,
            backtest_mode: self.backtest_mode,
            interval_secs: self.interval_secs,
            positions_ready,
            position_count: positions.len(),
            has_position: !positions.is_empty(),
            positions,
            pnl_total: self.pnl_total,
            pnl_today: self.pnl_today,
            pnl_source: "equity".to_string(),
            funding_carry_today: self.funding_carry_today,
            trade_stats: self.trade_stats.clone(),
            maintenance: self.maintenance.clone(),
            shutdown: self.shutdown.clone(),
            // Per-instance error attribution (bot-strategy#367): the
            // engine wraps `step_for_instance` and the status-writer
            // loop with a `CurrentInstanceGuard`, so reading
            // `snapshot_for(self.id)` here returns this variant's
            // bucket merged with the shared/None bucket (connector
            // WS resets, account refresh failures, …). Variants that
            // never logged anything stay clean even when a sibling
            // is tripping risk halts.
            error_summary: error_counter::global().map(|h| h.snapshot_for(self.id.as_deref())),
            ws_reset_24h_count: error_counter::global()
                .map(|h| h.ws_reset_24h_count())
                .unwrap_or(0),
            kill_switch_active: self.kill_switch_active,
            daily_risk: self.daily_risk.clone(),
            session_risk: self.session_risk.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
            risk_history: self.risk_history.iter().cloned().collect(),
        };
        let payload = serde_json::to_string(&snapshot)
            .map_err(std::io::Error::other)?;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = self.path.with_extension("json.tmp");
        fs::write(&tmp_path, &payload)?;
        fs::rename(tmp_path, &self.path)?;
        // Mirror to S3 when configured (#343). Fire-and-forget — failures
        // are logged but never block the local write. Skipped silently
        // when the reporter has no `id` because the S3 key shape (one
        // object per per-instance status) requires a stable identifier.
        if let (Some(mirror), Some(id)) = (&self.s3_mirror, self.id.clone()) {
            let mirror = Arc::clone(mirror);
            mirror.put_async(&format!("{id}.json"), payload.into_bytes());
            // Phase 3: sibling files. Re-uploaded only when their byte
            // length changed since the last successful put, so we keep
            // S3 in sync without paying for an upload every minute.
            self.maybe_mirror_sibling(
                &mirror,
                &id,
                "equity_history.jsonl",
                "application/x-ndjson",
                self.equity_history_path.clone(),
                |this| &mut this.last_equity_history_uploaded_len,
            );
            let alert_path = self
                .path
                .parent()
                .map(|d| d.join("backtest_alert.json"))
                .unwrap_or_else(|| PathBuf::from("backtest_alert.json"));
            self.maybe_mirror_sibling(
                &mirror,
                &id,
                "backtest_alert.json",
                "application/json",
                alert_path,
                |this| &mut this.last_backtest_alert_uploaded_len,
            );
        }
        Ok(())
    }

    /// Read `local_path` and PutObject it to `<key_prefix>/<id>.<suffix>`
    /// when (a) the file exists and (b) its byte length differs from the
    /// last successful upload tracked via `len_field`. Missing files and
    /// empty files are silently skipped — equity_history.jsonl exists
    /// only after the first equity sample, and backtest_alert.json only
    /// in the rare BT-replay alert path. See bot-strategy#343 Phase 3.
    fn maybe_mirror_sibling(
        &mut self,
        mirror: &Arc<S3Mirror>,
        id: &str,
        suffix: &'static str,
        content_type: &'static str,
        local_path: PathBuf,
        len_field: impl FnOnce(&mut Self) -> &mut Option<u64>,
    ) {
        let meta = match fs::metadata(&local_path) {
            Ok(m) => m,
            Err(_) => return,
        };
        let len = meta.len();
        if len == 0 {
            return;
        }
        let slot = len_field(self);
        if *slot == Some(len) {
            return;
        }
        let body = match fs::read(&local_path) {
            Ok(b) => b,
            Err(err) => {
                log::warn!(
                    "[STATUS_S3] read sibling {} failed: {:?}",
                    local_path.display(),
                    err
                );
                return;
            }
        };
        // We optimistically commit `last_..._uploaded_len = len` before
        // the spawn returns: a put failure logs but does not roll back
        // (next change re-uploads). Avoids holding `&mut self` across
        // an awaitable.
        *slot = Some(len);
        mirror.put_async_with_content_type(&format!("{id}.{suffix}"), body, content_type);
    }

    pub(super) fn write_snapshot_if_due(
        &mut self,
        open_positions: &HashMap<String, PositionSnapshot>,
        positions_ready: bool,
    ) -> std::io::Result<bool> {
        let due = self
            .last_snapshot
            .map(|t| t.elapsed() >= self.snapshot_every)
            .unwrap_or(true);
        if !due {
            return Ok(false);
        }
        self.write_snapshot(open_positions, positions_ready)?;
        self.last_snapshot = Some(Instant::now());
        Ok(true)
    }

    pub(super) fn reset_daily_if_needed(&mut self) {
        if !self.equity_day_start_set {
            return;
        }
        let today = Utc::now().date_naive();
        if today != self.pnl_today_date {
            self.pnl_today_date = today;
            self.equity_day_start = self.pnl_total;
            self.persist_equity_baseline();
        }
        self.pnl_today = self.pnl_total - self.equity_day_start;
    }
}
