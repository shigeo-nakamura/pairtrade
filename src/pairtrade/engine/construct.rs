//! `PairTradeEngine` construction: connector wiring, per-pair shared-state
//! seeding, and per-strategy `StrategyInstance` build-out. Moved out of
//! `pairtrade/mod.rs` per bot-strategy#444 (no behavior change).

use anyhow::{anyhow, Context, Result};
use dex_connector::DexConnector;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::Duration;

use crate::ports::replay_dex::ReplayConnector;

use super::super::bar::BarBuilder;
use super::super::config::{PairParams, PairTradeConfig};
use super::super::instance::{StrategyInstance, EQUITY_REFRESH_CACHE_SECS};
use super::super::pnl_log::PnlLogger;
use super::super::state::{PairSharedState, PairState};
use super::super::status::StatusReporter;
use super::super::{backtest, data_dump, funding_history, kalman, prom, status};
use super::risk::risk_state_path_for;
use super::PairTradeEngine;

impl PairTradeEngine {
    /// Create a new engine with a pre-loaded ReplayConnector (for batch mode).
    pub async fn new_with_replay(
        cfg: PairTradeConfig,
        replay: Arc<ReplayConnector>,
    ) -> Result<Self> {
        replay.reset();
        let primary: Arc<dyn DexConnector + Send + Sync> = replay.clone();
        let n = cfg.strategies.len().max(1);
        let instance_connectors = std::iter::repeat_n(primary.clone(), n).collect();
        Self::new_inner(cfg, primary, instance_connectors, Some(replay)).await
    }

    pub async fn new(cfg: PairTradeConfig) -> Result<Self> {
        let (connector, instance_connectors, replay_connector) =
            backtest::create_connector(&cfg).await?;
        Self::new_inner(cfg, connector, instance_connectors, replay_connector).await
    }

    async fn new_inner(
        cfg: PairTradeConfig,
        connector: Arc<dyn DexConnector + Send + Sync>,
        instance_connectors: Vec<Arc<dyn DexConnector + Send + Sync>>,
        replay_connector: Option<Arc<ReplayConnector>>,
    ) -> Result<Self> {
        let mut history = HashMap::new();
        let mut bar_builders = HashMap::new();
        let mut per_pair_state: HashMap<String, PairSharedState> = HashMap::new();
        for pair in &cfg.universe {
            history.insert(pair.base.clone(), VecDeque::new());
            history.insert(pair.quote.clone(), VecDeque::new());
            bar_builders.insert(pair.base.clone(), BarBuilder::new(cfg.trading_period_secs));
            bar_builders.insert(pair.quote.clone(), BarBuilder::new(cfg.trading_period_secs));
            let pair_key = format!("{}/{}", pair.base, pair.quote);
            let mut shared = PairSharedState::new(cfg.metrics_window);
            if cfg.use_kalman_beta {
                shared.kalman = Some(kalman::KalmanBeta::new(
                    1.0,
                    cfg.kalman_initial_p,
                    cfg.kalman_q,
                    cfg.kalman_r,
                ));
            }
            per_pair_state.insert(pair_key, shared);
        }

        let history_path = PathBuf::from(cfg.history_file.as_str());
        let risk_state_path = risk_state_path_for(&history_path);

        let min_order_warned = HashSet::new();
        let min_tick_warned = HashSet::new();
        let data_dump_writer = if cfg.enable_data_dump {
            let file_path = cfg.data_dump_file.as_ref().unwrap(); // is_none checked in from_env
            Some(data_dump::RotatingDumpWriter::new(file_path)?)
        } else {
            None
        };
        let regime_series_writer = match cfg.bt_regime_series_file.as_deref() {
            Some(path) => {
                // Backtest-only output: a non-rotating per-tick file must not
                // be creatable by an accidentally inherited env in live mode.
                if !cfg.backtest_mode {
                    return Err(anyhow!(
                        "BT_REGIME_SERIES_FILE is set but BACKTEST_MODE is not — refusing to \
                         write a non-rotating per-tick series file in live mode"
                    ));
                }
                use std::io::Write;
                let mut writer = std::io::BufWriter::new(
                    std::fs::File::create(path)
                        .with_context(|| format!("create BT_REGIME_SERIES_FILE {path}"))?,
                );
                writeln!(writer, "ts,key,innovation,beta,scale,norm,cusum,active")
                    .context("write BT_REGIME_SERIES_FILE header")?;
                Some(writer)
            }
            None => None,
        };

        let backtest_mode = cfg.backtest_mode;
        let _ = backtest_mode;
        let multi_instance = cfg.strategies.len() > 1;

        // Build one StrategyInstance per entry in cfg.strategies. For legacy
        // single-strategy YAML this is exactly one instance whose parameters
        // match today's behavior (golden-test stable). For multi-strategy
        // YAML this produces N instances that share the engine's history /
        // bar_builders but each hold their own pair_params overlay,
        // connector, PnL log, and status reporter.
        let mut built_instances: Vec<StrategyInstance> = Vec::new();
        let strategies = cfg.strategies.clone();
        for (i, strategy) in strategies.iter().enumerate() {
            // Overlay per-strategy exit_z / stop_loss_z / max_loss_r_mult on
            // top of the engine's default_pair_params and per-pair overrides
            // so each variant evaluates z-exits at its own thresholds.
            let mut inst_default = cfg.default_pair_params.clone();
            strategy.apply_pair_param_overrides(&mut inst_default);

            let mut inst_pair_params: HashMap<String, PairParams> = HashMap::new();
            for (k, v) in cfg.pair_params.iter() {
                let mut pp = v.clone();
                strategy.apply_pair_param_overrides(&mut pp);
                inst_pair_params.insert(k.clone(), pp);
            }

            let mut states = HashMap::new();
            for pair in &cfg.universe {
                let pair_key = format!("{}/{}", pair.base, pair.quote);
                let pp = inst_pair_params.get(&pair_key).unwrap_or(&inst_default);
                let ps = PairState::new(pp.entry_z_base);
                prom::init_close_reason_series(&strategy.id, &pair_key);
                prom::init_entry_reject_series(&strategy.id, &pair_key);
                states.insert(pair_key, ps);
            }

            let instance_connector = instance_connectors
                .get(i)
                .cloned()
                .unwrap_or_else(|| connector.clone());
            let pnl_logger = PnlLogger::from_env_for_instance(&cfg, &strategy.id, multi_instance);
            let status_reporter =
                StatusReporter::from_env_for_instance(&cfg, &strategy.id, multi_instance);

            // Stagger the per-instance equity-refresh cycle so N instances
            // don't all hit `/account` inside the same 5-min expiry boundary.
            // Each instance is phase-shifted by i * (CACHE_SECS / N) so over
            // a 5-min window the N calls are spread evenly (~100s apart for
            // N=3) instead of back-to-back. Avoids Lighter's short-window
            // 429 on the burst head (bot-strategy#142).
            let instance_count = strategies.len();
            let last_equity_fetch = if i == 0 || instance_count <= 1 {
                None
            } else {
                let offset_secs = (EQUITY_REFRESH_CACHE_SECS * i as u64) / instance_count as u64;
                let phase = EQUITY_REFRESH_CACHE_SECS.saturating_sub(offset_secs);
                Some(Instant::now() - Duration::from_secs(phase))
            };

            built_instances.push(StrategyInstance {
                id: strategy.id.clone(),
                connector: instance_connector,
                equity_cache: strategy.equity_reference_usd,
                last_equity_fetch,
                equity_initialized: false,
                equity_reference_usd: strategy.equity_reference_usd,
                states,
                pnl_logger,
                status_reporter,
                consecutive_losses: 0,
                circuit_breaker_until: None,
                circuit_breaker_until_ts: None,
                session_start_equity: 0.0,
                session_start_ts: 0,
                realized_pnl_today: 0.0,
                funding_carry_today: 0.0,
                daily_loss_halted: false,
                equity_samples: Vec::new(),
                session_halted: false,
                session_halt_reason: None,
                session_halt_ts: None,
                total_trades: 0,
                total_wins: 0,
                total_pnl: 0.0,
                peak_pnl: 0.0,
                max_dd: 0.0,
                pair_params: inst_pair_params,
                default_pair_params: inst_default,
                external_flatten_reason: None,
            });
        }

        // Stamp Prometheus process-info gauges once per variant so the
        // `pairtrade_bot_version_info` series shows up on /metrics even
        // before the first tick. bot-strategy#409.
        let process_started = status::process_started_at();
        for inst in &built_instances {
            prom::record_process_info(inst.id.as_str(), process_started);
        }

        Ok(Self {
            cfg,
            connector,
            replay_connector,
            instances: built_instances,
            history,
            per_pair_state,
            bar_builders,
            last_metrics_log: None,
            last_ob_warn: HashMap::new(),
            last_ticker_warn: HashMap::new(),
            last_position_warn: HashMap::new(),
            min_order_warned,
            min_tick_warned,
            positions_ready: backtest_mode,
            open_positions: HashMap::new(),
            history_path,
            risk_state_path,
            kill_switch_active: false,
            data_dump_writer,
            regime_series_writer,
            funding_history: funding_history::FundingHistory::new(),
            shutdown_pending: false,
            bar_emit_log: HashMap::new(),
            last_bar_rate_warn: HashMap::new(),
            last_warm_start_key: None,
        })
    }
}
