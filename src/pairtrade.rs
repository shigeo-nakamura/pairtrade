use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, Utc};
use dex_connector::{DexConnector, DexError, PositionSnapshot};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::error::Error;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::time::{sleep, Duration};

use crate::email_client::EmailClient;
use crate::ports::replay_dex::ReplayConnector;
use crate::trade::execution::dex_connector_box::DexConnectorBox;

const DEFAULT_INTERVAL_SECS: u64 = 20;
const DEFAULT_TRADING_PERIOD_SECS: u64 = 60;
const DEFAULT_METRICS_WINDOW: usize = 240;
const DEFAULT_ENTRY_Z_BASE: f64 = 2.0;
const DEFAULT_ENTRY_Z_MIN: f64 = 1.8;
const DEFAULT_ENTRY_Z_MAX: f64 = 2.3;
const DEFAULT_EXIT_Z: f64 = 0.5;
const DEFAULT_STOP_LOSS_Z: f64 = 3.3;
const DEFAULT_FORCE_CLOSE_SECS: u64 = 3600;
const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 3660; // DEFAULT_FORCE_CLOSE_SECS + 60s buffer
const DEFAULT_COOLDOWN_SECS: u64 = 30;
const MAX_EXIT_RETRIES: u32 = 3;
const DEFAULT_NET_FUNDING_MIN_PER_HOUR: f64 = -0.005;
const DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN: f64 = 0.1;
const DEFAULT_NOTIONAL_PER_LEG: f64 = 100.0;
const DEFAULT_RISK_PCT_PER_TRADE: f64 = 0.01;
const DEFAULT_MAX_LOSS_R_MULT: f64 = 1.0;
const DEFAULT_EQUITY_USD: f64 = 10_000.0;
const DEFAULT_LOOKBACK_HOURS_SHORT: u64 = 4;
const DEFAULT_LOOKBACK_HOURS_LONG: u64 = 24;
const DEFAULT_HALF_LIFE_MAX_HOURS: f64 = 1.5;
const DEFAULT_ADF_P_THRESHOLD: f64 = 0.05;
const PAIR_SELECTION_INTERVAL_SECS: u64 = 3600;
const DEFAULT_ENTRY_VOL_LOOKBACK_HOURS: u64 = 24;
const DEFAULT_SLIPPAGE_BPS: i32 = 0;
const DEFAULT_FEE_BPS: f64 = 0.0;
const DEFAULT_MAX_LEVERAGE: f64 = 5.0;
const DEFAULT_REEVAL_JUMP_Z_MULT: f64 = 1.5;
const DEFAULT_VOL_SPIKE_MULT: f64 = 2.5;
const DEFAULT_MAX_ACTIVE_PAIRS: usize = 3;
const DEFAULT_WARM_START_MODE: &str = "strict";
const DEFAULT_ORDER_TIMEOUT_SECS: u64 = 120;
const DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES: u32 = 3;
const DEFAULT_FORCE_CLOSE_ON_STARTUP: bool = true;
const DEFAULT_STARTUP_FORCE_CLOSE_ATTEMPTS: u32 = 3;
const DEFAULT_STARTUP_FORCE_CLOSE_WAIT_SECS: u64 = 3;
const POST_ONLY_ENTRY_ATTEMPTS: usize = 3;
const POST_ONLY_EXIT_ATTEMPTS: usize = 3;
const POST_ONLY_RETRY_DELAY_MS: u64 = 200;
const POST_ONLY_RETRY_MAX_ELAPSED_MS: u64 = 1500;
const DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA: f64 = 0.5;
const DEFAULT_BETA_DIVERGENCE_MAX: f64 = 0.15;
const DEFAULT_CIRCUIT_BREAKER_CONSECUTIVE_LOSSES: u32 = 3;
const DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SECS: u64 = 1800;
const DEFAULT_CB_TIER1_LOSSES: u32 = 0;
const DEFAULT_CB_TIER1_COOLDOWN_SECS: u64 = 0;
const DEFAULT_CB_TIER2_LOSSES: u32 = 0;
const DEFAULT_CB_TIER2_COOLDOWN_SECS: u64 = 0;
const DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS: u64 = 0;

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
enum StringOrVec {
    String(String),
    Vec(Vec<String>),
}

impl StringOrVec {
    fn into_vec(self) -> Vec<String> {
        match self {
            StringOrVec::String(value) => value
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect(),
            StringOrVec::Vec(values) => values
                .into_iter()
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct PairTradeYaml {
    dex_name: Option<String>,
    rest_endpoint: Option<String>,
    web_socket_endpoint: Option<String>,
    dry_run: Option<bool>,
    agent_name: Option<String>,
    interval_secs: Option<u64>,
    trading_period_secs: Option<u64>,
    metrics_window_length: Option<usize>,
    entry_z_score_base: Option<f64>,
    entry_z_score_min: Option<f64>,
    entry_z_score_max: Option<f64>,
    exit_z_score: Option<f64>,
    stop_loss_z_score: Option<f64>,
    force_close_time_secs: Option<u64>,
    cooldown_secs: Option<u64>,
    net_funding_min_per_hour: Option<f64>,
    spread_velocity_max_sigma_per_min: Option<f64>,
    notional_per_leg_usd: Option<f64>,
    risk_pct_per_trade: Option<f64>,
    max_loss_r_mult: Option<f64>,
    equity_usd_fallback: Option<f64>,
    universe_pairs: Option<StringOrVec>,
    universe_symbols: Option<StringOrVec>,
    pair_selection_lookback_hours_short: Option<u64>,
    pair_selection_lookback_hours_long: Option<u64>,
    half_life_max_hours: Option<f64>,
    adf_p_threshold: Option<f64>,
    entry_vol_lookback_hours: Option<u64>,
    slippage_bps: Option<i32>,
    fee_bps: Option<f64>,
    max_leverage: Option<f64>,
    reeval_jump_z_mult: Option<f64>,
    vol_spike_mult: Option<f64>,
    max_active_pairs: Option<usize>,
    warm_start_mode: Option<String>,
    warm_start_min_bars: Option<usize>,
    order_timeout_secs: Option<u64>,
    entry_partial_fill_max_retries: Option<u32>,
    startup_force_close_attempts: Option<u32>,
    startup_force_close_wait_secs: Option<u64>,
    force_close_on_startup: Option<bool>,
    enable_data_dump: Option<bool>,
    data_dump_file: Option<String>,
    observe_only: Option<bool>,
    disable_history_persist: Option<bool>,
    history_file: Option<String>,
    backtest_mode: Option<bool>,
    backtest_file: Option<String>,
    spread_trend_max_slope_sigma: Option<f64>,
    beta_divergence_max: Option<f64>,
    beta_min: Option<f64>,
    hedge_ratio_max_deviation: Option<f64>,
    circuit_breaker_consecutive_losses: Option<u32>,
    circuit_breaker_cooldown_secs: Option<u64>,
    circuit_breaker_tier1_losses: Option<u32>,
    circuit_breaker_tier1_cooldown_secs: Option<u64>,
    circuit_breaker_tier2_losses: Option<u32>,
    circuit_breaker_tier2_cooldown_secs: Option<u64>,
    entry_post_only_timeout_secs: Option<u64>,
    // Phase 2 filters (default off: 0.0 disables)
    entry_velocity_block_sigma_per_min: Option<f64>,
    funding_entry_z_scale: Option<f64>,
    beta_gap_entry_z_scale: Option<f64>,
    pair_overrides: Option<HashMap<String, PairOverrideYaml>>,
    /// Graceful shutdown: max seconds to wait for natural exit on SIGTERM before
    /// force-closing both legs. 0 = immediate force close (legacy behavior).
    shutdown_grace_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
struct PairOverrideYaml {
    entry_z_score_base: Option<f64>,
    entry_z_score_min: Option<f64>,
    entry_z_score_max: Option<f64>,
    exit_z_score: Option<f64>,
    stop_loss_z_score: Option<f64>,
    force_close_time_secs: Option<u64>,
    cooldown_secs: Option<u64>,
    max_loss_r_mult: Option<f64>,
    half_life_max_hours: Option<f64>,
    adf_p_threshold: Option<f64>,
    spread_velocity_max_sigma_per_min: Option<f64>,
    spread_trend_max_slope_sigma: Option<f64>,
    beta_divergence_max: Option<f64>,
    beta_min: Option<f64>,
    hedge_ratio_max_deviation: Option<f64>,
    pair_selection_lookback_hours_short: Option<u64>,
    pair_selection_lookback_hours_long: Option<u64>,
    entry_vol_lookback_hours: Option<u64>,
    warm_start_min_bars: Option<usize>,
    reeval_jump_z_mult: Option<f64>,
    vol_spike_mult: Option<f64>,
    circuit_breaker_tier1_losses: Option<u32>,
    circuit_breaker_tier1_cooldown_secs: Option<u64>,
    circuit_breaker_tier2_losses: Option<u32>,
    circuit_breaker_tier2_cooldown_secs: Option<u64>,
    entry_post_only_timeout_secs: Option<u64>,
}

/// Resolved per-pair parameters (global defaults merged with any pair-specific overrides).
#[derive(Debug, Clone)]
pub struct PairParams {
    pub entry_z_base: f64,
    pub entry_z_min: f64,
    pub entry_z_max: f64,
    pub exit_z: f64,
    pub stop_loss_z: f64,
    pub force_close_secs: u64,
    pub cooldown_secs: u64,
    pub max_loss_r_mult: f64,
    pub half_life_max_hours: f64,
    pub adf_p_threshold: f64,
    pub spread_velocity_max_sigma_per_min: f64,
    pub spread_trend_max_slope_sigma: f64,
    pub beta_divergence_max: f64,
    pub beta_min: f64,
    pub hedge_ratio_max_deviation: f64,
    pub lookback_hours_short: u64,
    pub lookback_hours_long: u64,
    pub entry_vol_lookback_hours: u64,
    pub warm_start_min_bars: usize,
    pub reeval_jump_z_mult: f64,
    pub vol_spike_mult: f64,
    pub circuit_breaker_tier1_losses: u32,
    pub circuit_breaker_tier1_cooldown_secs: u64,
    pub circuit_breaker_tier2_losses: u32,
    pub circuit_breaker_tier2_cooldown_secs: u64,
    pub entry_post_only_timeout_secs: u64,
    // Phase 2 filters (0.0 = disabled)
    pub entry_velocity_block_sigma_per_min: f64,
    pub funding_entry_z_scale: f64,
    pub beta_gap_entry_z_scale: f64,
}

#[derive(Debug, Clone)]
pub struct PairTradeConfig {
    pub dex_name: String,
    pub rest_endpoint: String,
    pub web_socket_endpoint: String,
    pub dry_run: bool,
    pub agent_name: Option<String>,
    pub interval_secs: u64,
    pub trading_period_secs: u64,
    pub metrics_window: usize,
    pub entry_z_base: f64,
    pub entry_z_min: f64,
    pub entry_z_max: f64,
    pub exit_z: f64,
    pub stop_loss_z: f64,
    pub force_close_secs: u64,
    pub cooldown_secs: u64,
    pub net_funding_min_per_hour: f64,
    pub spread_velocity_max_sigma_per_min: f64,
    pub notional_per_leg: f64,
    pub risk_pct_per_trade: f64,
    pub max_loss_r_mult: f64,
    pub equity_usd: f64,
    pub universe: Vec<PairSpec>,
    pub lookback_hours_short: u64,
    pub lookback_hours_long: u64,
    pub half_life_max_hours: f64,
    pub adf_p_threshold: f64,
    pub entry_vol_lookback_hours: u64,
    pub slippage_bps: i32,
    pub fee_bps: f64,
    pub max_leverage: f64,
    pub reeval_jump_z_mult: f64,
    pub vol_spike_mult: f64,
    pub max_active_pairs: usize,
    pub warm_start_mode: WarmStartMode,
    pub warm_start_min_bars: usize,
    pub order_timeout_secs: u64,
    pub entry_partial_fill_max_retries: u32,
    pub startup_force_close_attempts: u32,
    pub startup_force_close_wait_secs: u64,
    pub force_close_on_startup: bool,
    // For data dump feature
    pub enable_data_dump: bool,
    pub data_dump_file: Option<String>,
    // Safety guard to avoid real orders while observing market data
    pub observe_only: bool,
    pub disable_history_persist: bool,
    pub history_file: String,
    // For backtest feature
    pub backtest_mode: bool,
    pub backtest_file: Option<String>,
    pub spread_trend_max_slope_sigma: f64,
    pub beta_divergence_max: f64,
    pub beta_min: f64,
    pub hedge_ratio_max_deviation: f64,
    pub circuit_breaker_consecutive_losses: u32,
    pub circuit_breaker_cooldown_secs: u64,
    pub circuit_breaker_tier1_losses: u32,
    pub circuit_breaker_tier1_cooldown_secs: u64,
    pub circuit_breaker_tier2_losses: u32,
    pub circuit_breaker_tier2_cooldown_secs: u64,
    pub entry_post_only_timeout_secs: u64,
    // Phase 2 filters (0.0 = disabled)
    pub entry_velocity_block_sigma_per_min: f64,
    pub funding_entry_z_scale: f64,
    pub beta_gap_entry_z_scale: f64,
    pub pair_params: HashMap<String, PairParams>,
    pub default_pair_params: PairParams,
    /// Graceful shutdown: max seconds to wait for natural pair exit on SIGTERM
    /// before force-closing both legs. 0 = immediate force close (legacy).
    pub shutdown_grace_secs: u64,
}

#[derive(Debug, Clone)]
pub struct PairSpec {
    pub base: String,
    pub quote: String,
}

impl PairTradeConfig {
    pub fn params_for(&self, pair_key: &str) -> &PairParams {
        self.pair_params
            .get(pair_key)
            .unwrap_or(&self.default_pair_params)
    }

    fn build_default_pair_params(&self) -> PairParams {
        PairParams {
            entry_z_base: self.entry_z_base,
            entry_z_min: self.entry_z_min,
            entry_z_max: self.entry_z_max,
            exit_z: self.exit_z,
            stop_loss_z: self.stop_loss_z,
            force_close_secs: self.force_close_secs,
            cooldown_secs: self.cooldown_secs,
            max_loss_r_mult: self.max_loss_r_mult,
            half_life_max_hours: self.half_life_max_hours,
            adf_p_threshold: self.adf_p_threshold,
            spread_velocity_max_sigma_per_min: self.spread_velocity_max_sigma_per_min,
            spread_trend_max_slope_sigma: self.spread_trend_max_slope_sigma,
            beta_divergence_max: self.beta_divergence_max,
            beta_min: self.beta_min,
            hedge_ratio_max_deviation: self.hedge_ratio_max_deviation,
            lookback_hours_short: self.lookback_hours_short,
            lookback_hours_long: self.lookback_hours_long,
            entry_vol_lookback_hours: self.entry_vol_lookback_hours,
            warm_start_min_bars: self.warm_start_min_bars,
            reeval_jump_z_mult: self.reeval_jump_z_mult,
            vol_spike_mult: self.vol_spike_mult,
            circuit_breaker_tier1_losses: self.circuit_breaker_tier1_losses,
            circuit_breaker_tier1_cooldown_secs: self.circuit_breaker_tier1_cooldown_secs,
            circuit_breaker_tier2_losses: self.circuit_breaker_tier2_losses,
            circuit_breaker_tier2_cooldown_secs: self.circuit_breaker_tier2_cooldown_secs,
            entry_post_only_timeout_secs: self.entry_post_only_timeout_secs,
            entry_velocity_block_sigma_per_min: self.entry_velocity_block_sigma_per_min,
            funding_entry_z_scale: self.funding_entry_z_scale,
            beta_gap_entry_z_scale: self.beta_gap_entry_z_scale,
        }
    }

    fn build_pair_params_map(
        &self,
        overrides: &Option<HashMap<String, PairOverrideYaml>>,
    ) -> HashMap<String, PairParams> {
        let mut map = HashMap::new();
        if let Some(overrides) = overrides {
            for (pair_key, ovr) in overrides {
                let pp = PairParams {
                    entry_z_base: ovr.entry_z_score_base.unwrap_or(self.entry_z_base),
                    entry_z_min: ovr.entry_z_score_min.unwrap_or(self.entry_z_min),
                    entry_z_max: ovr.entry_z_score_max.unwrap_or(self.entry_z_max),
                    exit_z: ovr.exit_z_score.unwrap_or(self.exit_z),
                    stop_loss_z: ovr.stop_loss_z_score.unwrap_or(self.stop_loss_z),
                    force_close_secs: ovr.force_close_time_secs.unwrap_or(self.force_close_secs),
                    cooldown_secs: ovr.cooldown_secs.unwrap_or(self.cooldown_secs),
                    max_loss_r_mult: ovr.max_loss_r_mult.unwrap_or(self.max_loss_r_mult),
                    half_life_max_hours: ovr
                        .half_life_max_hours
                        .unwrap_or(self.half_life_max_hours),
                    adf_p_threshold: ovr.adf_p_threshold.unwrap_or(self.adf_p_threshold),
                    spread_velocity_max_sigma_per_min: ovr
                        .spread_velocity_max_sigma_per_min
                        .unwrap_or(self.spread_velocity_max_sigma_per_min),
                    spread_trend_max_slope_sigma: ovr
                        .spread_trend_max_slope_sigma
                        .unwrap_or(self.spread_trend_max_slope_sigma),
                    beta_divergence_max: ovr
                        .beta_divergence_max
                        .unwrap_or(self.beta_divergence_max),
                    beta_min: ovr.beta_min.unwrap_or(self.beta_min),
                    hedge_ratio_max_deviation: ovr
                        .hedge_ratio_max_deviation
                        .unwrap_or(self.hedge_ratio_max_deviation),
                    lookback_hours_short: ovr
                        .pair_selection_lookback_hours_short
                        .unwrap_or(self.lookback_hours_short),
                    lookback_hours_long: ovr
                        .pair_selection_lookback_hours_long
                        .unwrap_or(self.lookback_hours_long),
                    entry_vol_lookback_hours: ovr
                        .entry_vol_lookback_hours
                        .unwrap_or(self.entry_vol_lookback_hours),
                    warm_start_min_bars: ovr
                        .warm_start_min_bars
                        .unwrap_or(self.warm_start_min_bars),
                    reeval_jump_z_mult: ovr.reeval_jump_z_mult.unwrap_or(self.reeval_jump_z_mult),
                    vol_spike_mult: ovr.vol_spike_mult.unwrap_or(self.vol_spike_mult),
                    circuit_breaker_tier1_losses: ovr
                        .circuit_breaker_tier1_losses
                        .unwrap_or(self.circuit_breaker_tier1_losses),
                    circuit_breaker_tier1_cooldown_secs: ovr
                        .circuit_breaker_tier1_cooldown_secs
                        .unwrap_or(self.circuit_breaker_tier1_cooldown_secs),
                    circuit_breaker_tier2_losses: ovr
                        .circuit_breaker_tier2_losses
                        .unwrap_or(self.circuit_breaker_tier2_losses),
                    circuit_breaker_tier2_cooldown_secs: ovr
                        .circuit_breaker_tier2_cooldown_secs
                        .unwrap_or(self.circuit_breaker_tier2_cooldown_secs),
                    entry_post_only_timeout_secs: ovr
                        .entry_post_only_timeout_secs
                        .unwrap_or(self.entry_post_only_timeout_secs),
                    entry_velocity_block_sigma_per_min: self.entry_velocity_block_sigma_per_min,
                    funding_entry_z_scale: self.funding_entry_z_scale,
                    beta_gap_entry_z_scale: self.beta_gap_entry_z_scale,
                };
                map.insert(pair_key.clone(), pp);
            }
        }
        map
    }

    pub fn from_env_or_yaml() -> Result<Self> {
        let config_path = env::var("PAIRTRADE_CONFIG_PATH")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                env::var("DEBOT_CONFIG")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            });
        if let Some(path) = config_path {
            return Self::from_yaml_path(path);
        }
        Self::from_env()
    }

    pub fn from_yaml_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let file = File::open(path_ref)
            .with_context(|| format!("failed to open PairTrade config {}", path_ref.display()))?;
        let yaml: PairTradeYaml = serde_yaml::from_reader(file)
            .with_context(|| format!("failed to parse PairTrade config {}", path_ref.display()))?;

        let history_file_from_yaml = yaml.history_file.is_some();
        let warm_start_min_from_yaml = yaml.warm_start_min_bars.is_some();

        let universe = resolve_universe_from_yaml(&yaml)?;
        let metrics_window = yaml.metrics_window_length.unwrap_or(DEFAULT_METRICS_WINDOW);
        let warm_start_mode = yaml
            .warm_start_mode
            .as_deref()
            .unwrap_or(DEFAULT_WARM_START_MODE)
            .parse()
            .unwrap_or(WarmStartMode::Strict);
        let warm_start_min_bars = yaml.warm_start_min_bars.unwrap_or(metrics_window);
        let history_file = yaml
            .history_file
            .clone()
            .unwrap_or_else(|| default_history_file(&universe, yaml.agent_name.as_deref()));

        let mut cfg = PairTradeConfig {
            dex_name: yaml.dex_name.unwrap_or_else(|| "hyperliquid".to_string()),
            rest_endpoint: yaml
                .rest_endpoint
                .unwrap_or_else(|| "https://api.hyperliquid.xyz".to_string()),
            web_socket_endpoint: yaml
                .web_socket_endpoint
                .unwrap_or_else(|| "wss://api.hyperliquid.xyz/ws".to_string()),
            dry_run: yaml.dry_run.unwrap_or(true),
            agent_name: yaml.agent_name,
            interval_secs: yaml.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS),
            trading_period_secs: yaml
                .trading_period_secs
                .unwrap_or(DEFAULT_TRADING_PERIOD_SECS),
            metrics_window,
            entry_z_base: yaml.entry_z_score_base.unwrap_or(DEFAULT_ENTRY_Z_BASE),
            entry_z_min: yaml.entry_z_score_min.unwrap_or(DEFAULT_ENTRY_Z_MIN),
            entry_z_max: yaml.entry_z_score_max.unwrap_or(DEFAULT_ENTRY_Z_MAX),
            exit_z: yaml.exit_z_score.unwrap_or(DEFAULT_EXIT_Z),
            stop_loss_z: yaml.stop_loss_z_score.unwrap_or(DEFAULT_STOP_LOSS_Z),
            force_close_secs: yaml
                .force_close_time_secs
                .unwrap_or(DEFAULT_FORCE_CLOSE_SECS),
            cooldown_secs: yaml.cooldown_secs.unwrap_or(DEFAULT_COOLDOWN_SECS),
            net_funding_min_per_hour: yaml
                .net_funding_min_per_hour
                .unwrap_or(DEFAULT_NET_FUNDING_MIN_PER_HOUR),
            spread_velocity_max_sigma_per_min: yaml
                .spread_velocity_max_sigma_per_min
                .unwrap_or(DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN),
            notional_per_leg: yaml
                .notional_per_leg_usd
                .unwrap_or(DEFAULT_NOTIONAL_PER_LEG),
            risk_pct_per_trade: yaml
                .risk_pct_per_trade
                .unwrap_or(DEFAULT_RISK_PCT_PER_TRADE),
            max_loss_r_mult: yaml.max_loss_r_mult.unwrap_or(DEFAULT_MAX_LOSS_R_MULT),
            equity_usd: yaml.equity_usd_fallback.unwrap_or(DEFAULT_EQUITY_USD),
            universe,
            lookback_hours_short: yaml
                .pair_selection_lookback_hours_short
                .unwrap_or(DEFAULT_LOOKBACK_HOURS_SHORT),
            lookback_hours_long: yaml
                .pair_selection_lookback_hours_long
                .unwrap_or(DEFAULT_LOOKBACK_HOURS_LONG),
            half_life_max_hours: yaml
                .half_life_max_hours
                .unwrap_or(DEFAULT_HALF_LIFE_MAX_HOURS),
            adf_p_threshold: yaml.adf_p_threshold.unwrap_or(DEFAULT_ADF_P_THRESHOLD),
            entry_vol_lookback_hours: yaml
                .entry_vol_lookback_hours
                .unwrap_or(DEFAULT_ENTRY_VOL_LOOKBACK_HOURS),
            slippage_bps: yaml.slippage_bps.unwrap_or(DEFAULT_SLIPPAGE_BPS),
            fee_bps: yaml.fee_bps.unwrap_or(DEFAULT_FEE_BPS),
            max_leverage: yaml.max_leverage.unwrap_or(DEFAULT_MAX_LEVERAGE),
            reeval_jump_z_mult: yaml
                .reeval_jump_z_mult
                .unwrap_or(DEFAULT_REEVAL_JUMP_Z_MULT),
            vol_spike_mult: yaml.vol_spike_mult.unwrap_or(DEFAULT_VOL_SPIKE_MULT),
            max_active_pairs: yaml.max_active_pairs.unwrap_or(DEFAULT_MAX_ACTIVE_PAIRS),
            warm_start_mode,
            warm_start_min_bars,
            order_timeout_secs: yaml
                .order_timeout_secs
                .unwrap_or(DEFAULT_ORDER_TIMEOUT_SECS),
            entry_partial_fill_max_retries: yaml
                .entry_partial_fill_max_retries
                .unwrap_or(DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES),
            startup_force_close_attempts: yaml
                .startup_force_close_attempts
                .unwrap_or(DEFAULT_STARTUP_FORCE_CLOSE_ATTEMPTS),
            startup_force_close_wait_secs: yaml
                .startup_force_close_wait_secs
                .unwrap_or(DEFAULT_STARTUP_FORCE_CLOSE_WAIT_SECS),
            force_close_on_startup: yaml
                .force_close_on_startup
                .unwrap_or(DEFAULT_FORCE_CLOSE_ON_STARTUP),
            enable_data_dump: yaml.enable_data_dump.unwrap_or(false),
            data_dump_file: yaml.data_dump_file,
            observe_only: yaml.observe_only.unwrap_or(false),
            disable_history_persist: yaml.disable_history_persist.unwrap_or(false),
            history_file,
            backtest_mode: yaml.backtest_mode.unwrap_or(false),
            backtest_file: yaml.backtest_file,
            spread_trend_max_slope_sigma: yaml
                .spread_trend_max_slope_sigma
                .unwrap_or(DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA),
            beta_divergence_max: yaml
                .beta_divergence_max
                .unwrap_or(DEFAULT_BETA_DIVERGENCE_MAX),
            beta_min: yaml.beta_min.unwrap_or(0.0),
            hedge_ratio_max_deviation: yaml.hedge_ratio_max_deviation.unwrap_or(1.0),
            circuit_breaker_consecutive_losses: yaml
                .circuit_breaker_consecutive_losses
                .unwrap_or(DEFAULT_CIRCUIT_BREAKER_CONSECUTIVE_LOSSES),
            circuit_breaker_cooldown_secs: yaml
                .circuit_breaker_cooldown_secs
                .unwrap_or(DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SECS),
            circuit_breaker_tier1_losses: yaml
                .circuit_breaker_tier1_losses
                .unwrap_or(DEFAULT_CB_TIER1_LOSSES),
            circuit_breaker_tier1_cooldown_secs: yaml
                .circuit_breaker_tier1_cooldown_secs
                .unwrap_or(DEFAULT_CB_TIER1_COOLDOWN_SECS),
            circuit_breaker_tier2_losses: yaml
                .circuit_breaker_tier2_losses
                .unwrap_or(DEFAULT_CB_TIER2_LOSSES),
            circuit_breaker_tier2_cooldown_secs: yaml
                .circuit_breaker_tier2_cooldown_secs
                .unwrap_or(DEFAULT_CB_TIER2_COOLDOWN_SECS),
            entry_post_only_timeout_secs: yaml
                .entry_post_only_timeout_secs
                .unwrap_or(DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS),
            entry_velocity_block_sigma_per_min: yaml
                .entry_velocity_block_sigma_per_min
                .unwrap_or(0.0),
            funding_entry_z_scale: yaml.funding_entry_z_scale.unwrap_or(0.0),
            beta_gap_entry_z_scale: yaml.beta_gap_entry_z_scale.unwrap_or(0.0),
            shutdown_grace_secs: yaml
                .shutdown_grace_secs
                .unwrap_or(DEFAULT_SHUTDOWN_GRACE_SECS),
            pair_params: HashMap::new(),
            default_pair_params: PairParams {
                entry_z_base: 0.0, // placeholder, rebuilt below
                entry_z_min: 0.0,
                entry_z_max: 0.0,
                exit_z: 0.0,
                stop_loss_z: 0.0,
                force_close_secs: 0,
                cooldown_secs: 0,
                max_loss_r_mult: 0.0,
                half_life_max_hours: 0.0,
                adf_p_threshold: 0.0,
                spread_velocity_max_sigma_per_min: 0.0,
                spread_trend_max_slope_sigma: 0.0,
                beta_divergence_max: 0.0,
                beta_min: 0.0,
                hedge_ratio_max_deviation: 1.0,
                lookback_hours_short: 0,
                lookback_hours_long: 0,
                entry_vol_lookback_hours: 0,
                warm_start_min_bars: 0,
                reeval_jump_z_mult: 0.0,
                vol_spike_mult: 0.0,
                circuit_breaker_tier1_losses: 0,
                circuit_breaker_tier1_cooldown_secs: 0,
                circuit_breaker_tier2_losses: 0,
                circuit_breaker_tier2_cooldown_secs: 0,
                entry_post_only_timeout_secs: 0,
                entry_velocity_block_sigma_per_min: 0.0,
                funding_entry_z_scale: 0.0,
                beta_gap_entry_z_scale: 0.0,
            },
        };

        cfg.default_pair_params = cfg.build_default_pair_params();
        cfg.pair_params = cfg.build_pair_params_map(&yaml.pair_overrides);
        cfg.apply_env_overrides(history_file_from_yaml, warm_start_min_from_yaml)?;
        // Rebuild pair params after env overrides may have changed global defaults
        cfg.default_pair_params = cfg.build_default_pair_params();
        // Re-merge: env overrides update globals, but pair_overrides from YAML still take precedence
        let pair_params_rebuilt = cfg.build_pair_params_map(&yaml.pair_overrides);
        if !pair_params_rebuilt.is_empty() {
            cfg.pair_params = pair_params_rebuilt;
        }
        Ok(cfg)
    }

    pub fn from_env() -> Result<Self> {
        let dex_name = env::var("DEX_NAME").unwrap_or_else(|_| "hyperliquid".to_string());
        let rest_endpoint =
            env::var("REST_ENDPOINT").unwrap_or_else(|_| "https://api.hyperliquid.xyz".to_string());
        let web_socket_endpoint = env::var("WEB_SOCKET_ENDPOINT")
            .unwrap_or_else(|_| "wss://api.hyperliquid.xyz/ws".to_string());
        let dry_run = env::var("DRY_RUN")
            .unwrap_or_else(|_| "true".to_string())
            .to_lowercase()
            == "true";
        let agent_name = env::var("AGENT_NAME").ok();
        let interval_secs = env::var("INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_INTERVAL_SECS);
        let trading_period_secs = env::var("TRADING_PERIOD_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_TRADING_PERIOD_SECS);
        let metrics_window = env::var("METRICS_WINDOW_LENGTH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_METRICS_WINDOW);
        let entry_z_base = env::var("ENTRY_Z_SCORE_BASE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_ENTRY_Z_BASE);
        let entry_z_min = env::var("ENTRY_Z_SCORE_MIN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_ENTRY_Z_MIN);
        let entry_z_max = env::var("ENTRY_Z_SCORE_MAX")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_ENTRY_Z_MAX);
        let exit_z = env::var("EXIT_Z_SCORE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_EXIT_Z);
        let stop_loss_z = env::var("STOP_LOSS_Z_SCORE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_STOP_LOSS_Z);
        let force_close_secs = env::var("FORCE_CLOSE_TIME_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_FORCE_CLOSE_SECS);
        let cooldown_secs = env::var("COOLDOWN_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_COOLDOWN_SECS);
        let net_funding_min_per_hour = env::var("NET_FUNDING_MIN_PER_HOUR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_NET_FUNDING_MIN_PER_HOUR);
        let spread_velocity_max_sigma_per_min = env::var("SPREAD_VELOCITY_MAX_SIGMA_PER_MIN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN);
        let notional_per_leg = env::var("NOTIONAL_PER_LEG_USD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_NOTIONAL_PER_LEG);
        let risk_pct_per_trade = env::var("RISK_PCT_PER_TRADE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_RISK_PCT_PER_TRADE);
        let max_loss_r_mult = env::var("MAX_LOSS_R_MULT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_LOSS_R_MULT);
        let equity_usd = env::var("EQUITY_USD_FALLBACK")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_EQUITY_USD);
        let universe = parse_universe_pairs()?;
        let lookback_hours_short = env::var("PAIR_SELECTION_LOOKBACK_HOURS_SHORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_LOOKBACK_HOURS_SHORT);
        let lookback_hours_long = env::var("PAIR_SELECTION_LOOKBACK_HOURS_LONG")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_LOOKBACK_HOURS_LONG);
        let half_life_max_hours = env::var("HALF_LIFE_MAX_HOURS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_HALF_LIFE_MAX_HOURS);
        let adf_p_threshold = env::var("ADF_P_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_ADF_P_THRESHOLD);
        let entry_vol_lookback_hours = env::var("ENTRY_VOL_LOOKBACK_HOURS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_ENTRY_VOL_LOOKBACK_HOURS);
        let slippage_bps = env::var("SLIPPAGE_BPS")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(DEFAULT_SLIPPAGE_BPS);
        let fee_bps = env::var("FEE_BPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_FEE_BPS);
        let max_leverage = env::var("MAX_LEVERAGE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_LEVERAGE);
        let reeval_jump_z_mult = env::var("REEVAL_JUMP_Z_MULT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_REEVAL_JUMP_Z_MULT);
        let vol_spike_mult = env::var("VOL_SPIKE_MULT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_VOL_SPIKE_MULT);
        let max_active_pairs = env::var("MAX_ACTIVE_PAIRS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_ACTIVE_PAIRS);
        let warm_start_mode = env::var("WARM_START_MODE")
            .ok()
            .unwrap_or_else(|| DEFAULT_WARM_START_MODE.to_string())
            .parse()
            .unwrap_or(WarmStartMode::Strict);
        let warm_start_min_bars = env::var("WARM_START_MIN_BARS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(metrics_window);
        let order_timeout_secs = env::var("ORDER_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_ORDER_TIMEOUT_SECS);
        let entry_partial_fill_max_retries = env::var("ENTRY_PARTIAL_FILL_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES);
        let startup_force_close_attempts = env::var("STARTUP_FORCE_CLOSE_ATTEMPTS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_STARTUP_FORCE_CLOSE_ATTEMPTS);
        let startup_force_close_wait_secs = env::var("STARTUP_FORCE_CLOSE_WAIT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_STARTUP_FORCE_CLOSE_WAIT_SECS);
        let force_close_on_startup = env::var("FORCE_CLOSE_ON_STARTUP")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                v == "1" || v == "true" || v == "yes"
            })
            .unwrap_or(DEFAULT_FORCE_CLOSE_ON_STARTUP);

        let enable_data_dump = env::var("ENABLE_DATA_DUMP")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true";
        let data_dump_file = env::var("DATA_DUMP_FILE").ok();
        if enable_data_dump && data_dump_file.is_none() {
            return Err(anyhow!(
                "DATA_DUMP_FILE must be set if ENABLE_DATA_DUMP is true"
            ));
        }
        let observe_only = env::var("OBSERVE_ONLY")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true"
            || enable_data_dump;
        let disable_history_persist = env::var("DISABLE_HISTORY_PERSIST")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                v == "1" || v == "true" || v == "yes"
            })
            .unwrap_or(false);
        let history_file = env::var("PAIRTRADE_HISTORY_FILE")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| default_history_file(&universe, agent_name.as_deref()));

        let backtest_mode = env::var("BACKTEST_MODE")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true";
        let backtest_file = env::var("BACKTEST_FILE").ok();
        if backtest_mode && backtest_file.is_none() {
            return Err(anyhow!(
                "BACKTEST_FILE must be set if BACKTEST_MODE is true"
            ));
        }

        let mut cfg = Self {
            dex_name,
            rest_endpoint,
            web_socket_endpoint,
            dry_run,
            agent_name,
            interval_secs,
            trading_period_secs,
            metrics_window,
            entry_z_base,
            entry_z_min,
            entry_z_max,
            exit_z,
            stop_loss_z,
            force_close_secs,
            cooldown_secs,
            net_funding_min_per_hour,
            spread_velocity_max_sigma_per_min,
            notional_per_leg,
            risk_pct_per_trade,
            max_loss_r_mult,
            equity_usd,
            universe,
            lookback_hours_short,
            lookback_hours_long,
            half_life_max_hours,
            adf_p_threshold,
            entry_vol_lookback_hours,
            slippage_bps,
            fee_bps,
            max_leverage,
            reeval_jump_z_mult,
            vol_spike_mult,
            max_active_pairs,
            warm_start_mode,
            warm_start_min_bars,
            order_timeout_secs,
            entry_partial_fill_max_retries,
            startup_force_close_attempts,
            startup_force_close_wait_secs,
            force_close_on_startup,
            enable_data_dump,
            data_dump_file,
            observe_only,
            disable_history_persist,
            history_file,
            backtest_mode,
            backtest_file,
            spread_trend_max_slope_sigma: env::var("SPREAD_TREND_MAX_SLOPE_SIGMA")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA),
            beta_divergence_max: env::var("BETA_DIVERGENCE_MAX")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_BETA_DIVERGENCE_MAX),
            beta_min: env::var("BETA_MIN")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
            hedge_ratio_max_deviation: env::var("HEDGE_RATIO_MAX_DEVIATION")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1.0),
            circuit_breaker_consecutive_losses: env::var("CIRCUIT_BREAKER_CONSECUTIVE_LOSSES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_CIRCUIT_BREAKER_CONSECUTIVE_LOSSES),
            circuit_breaker_cooldown_secs: env::var("CIRCUIT_BREAKER_COOLDOWN_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SECS),
            circuit_breaker_tier1_losses: env::var("CIRCUIT_BREAKER_TIER1_LOSSES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_CB_TIER1_LOSSES),
            circuit_breaker_tier1_cooldown_secs: env::var("CIRCUIT_BREAKER_TIER1_COOLDOWN_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_CB_TIER1_COOLDOWN_SECS),
            circuit_breaker_tier2_losses: env::var("CIRCUIT_BREAKER_TIER2_LOSSES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_CB_TIER2_LOSSES),
            circuit_breaker_tier2_cooldown_secs: env::var("CIRCUIT_BREAKER_TIER2_COOLDOWN_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_CB_TIER2_COOLDOWN_SECS),
            entry_post_only_timeout_secs: env::var("ENTRY_POST_ONLY_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS),
            entry_velocity_block_sigma_per_min: env::var("ENTRY_VELOCITY_BLOCK_SIGMA_PER_MIN")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
            funding_entry_z_scale: env::var("FUNDING_ENTRY_Z_SCALE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
            beta_gap_entry_z_scale: env::var("BETA_GAP_ENTRY_Z_SCALE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
            shutdown_grace_secs: env::var("SHUTDOWN_GRACE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_SHUTDOWN_GRACE_SECS),
            pair_params: HashMap::new(),
            default_pair_params: PairParams {
                entry_z_base: 0.0,
                entry_z_min: 0.0,
                entry_z_max: 0.0,
                exit_z: 0.0,
                stop_loss_z: 0.0,
                force_close_secs: 0,
                cooldown_secs: 0,
                max_loss_r_mult: 0.0,
                half_life_max_hours: 0.0,
                adf_p_threshold: 0.0,
                spread_velocity_max_sigma_per_min: 0.0,
                spread_trend_max_slope_sigma: 0.0,
                beta_divergence_max: 0.0,
                beta_min: 0.0,
                hedge_ratio_max_deviation: 1.0,
                lookback_hours_short: 0,
                lookback_hours_long: 0,
                entry_vol_lookback_hours: 0,
                warm_start_min_bars: 0,
                reeval_jump_z_mult: 0.0,
                vol_spike_mult: 0.0,
                circuit_breaker_tier1_losses: 0,
                circuit_breaker_tier1_cooldown_secs: 0,
                circuit_breaker_tier2_losses: 0,
                circuit_breaker_tier2_cooldown_secs: 0,
                entry_post_only_timeout_secs: 0,
                entry_velocity_block_sigma_per_min: 0.0,
                funding_entry_z_scale: 0.0,
                beta_gap_entry_z_scale: 0.0,
            },
        };
        cfg.default_pair_params = cfg.build_default_pair_params();
        Ok(cfg)
    }

    fn apply_env_overrides(
        &mut self,
        history_file_from_yaml: bool,
        warm_start_min_from_yaml: bool,
    ) -> Result<()> {
        if let Ok(value) = env::var("DEX_NAME") {
            if !value.trim().is_empty() {
                self.dex_name = value;
            }
        }
        if let Ok(value) = env::var("REST_ENDPOINT") {
            if !value.trim().is_empty() {
                self.rest_endpoint = value;
            }
        }
        if let Ok(value) = env::var("WEB_SOCKET_ENDPOINT") {
            if !value.trim().is_empty() {
                self.web_socket_endpoint = value;
            }
        }
        if let Ok(value) = env::var("DRY_RUN") {
            self.dry_run = value.to_lowercase() == "true";
        }
        if let Ok(value) = env::var("AGENT_NAME") {
            if !value.trim().is_empty() {
                self.agent_name = Some(value);
            }
        }

        let prev_metrics_window = self.metrics_window;
        if let Ok(value) = env::var("INTERVAL_SECS") {
            if let Ok(parsed) = value.parse() {
                self.interval_secs = parsed;
            }
        }
        if let Ok(value) = env::var("TRADING_PERIOD_SECS") {
            if let Ok(parsed) = value.parse() {
                self.trading_period_secs = parsed;
            }
        }
        if let Ok(value) = env::var("METRICS_WINDOW_LENGTH") {
            if let Ok(parsed) = value.parse() {
                self.metrics_window = parsed;
            }
        }
        if let Ok(value) = env::var("ENTRY_Z_SCORE_BASE") {
            if let Ok(parsed) = value.parse() {
                self.entry_z_base = parsed;
            }
        }
        if let Ok(value) = env::var("ENTRY_Z_SCORE_MIN") {
            if let Ok(parsed) = value.parse() {
                self.entry_z_min = parsed;
            }
        }
        if let Ok(value) = env::var("ENTRY_Z_SCORE_MAX") {
            if let Ok(parsed) = value.parse() {
                self.entry_z_max = parsed;
            }
        }
        if let Ok(value) = env::var("EXIT_Z_SCORE") {
            if let Ok(parsed) = value.parse() {
                self.exit_z = parsed;
            }
        }
        if let Ok(value) = env::var("STOP_LOSS_Z_SCORE") {
            if let Ok(parsed) = value.parse() {
                self.stop_loss_z = parsed;
            }
        }
        if let Ok(value) = env::var("FORCE_CLOSE_TIME_SECS") {
            if let Ok(parsed) = value.parse() {
                self.force_close_secs = parsed;
            }
        }
        if let Ok(value) = env::var("COOLDOWN_SECS") {
            if let Ok(parsed) = value.parse() {
                self.cooldown_secs = parsed;
            }
        }
        if let Ok(value) = env::var("NET_FUNDING_MIN_PER_HOUR") {
            if let Ok(parsed) = value.parse() {
                self.net_funding_min_per_hour = parsed;
            }
        }
        if let Ok(value) = env::var("SPREAD_VELOCITY_MAX_SIGMA_PER_MIN") {
            if let Ok(parsed) = value.parse() {
                self.spread_velocity_max_sigma_per_min = parsed;
            }
        }
        if let Ok(value) = env::var("NOTIONAL_PER_LEG_USD") {
            if let Ok(parsed) = value.parse() {
                self.notional_per_leg = parsed;
            }
        }
        if let Ok(value) = env::var("RISK_PCT_PER_TRADE") {
            if let Ok(parsed) = value.parse() {
                self.risk_pct_per_trade = parsed;
            }
        }
        if let Ok(value) = env::var("MAX_LOSS_R_MULT") {
            if let Ok(parsed) = value.parse() {
                self.max_loss_r_mult = parsed;
            }
        }
        if let Ok(value) = env::var("EQUITY_USD_FALLBACK") {
            if let Ok(parsed) = value.parse() {
                self.equity_usd = parsed;
            }
        }
        if let Ok(value) = env::var("PAIR_SELECTION_LOOKBACK_HOURS_SHORT") {
            if let Ok(parsed) = value.parse() {
                self.lookback_hours_short = parsed;
            }
        }
        if let Ok(value) = env::var("PAIR_SELECTION_LOOKBACK_HOURS_LONG") {
            if let Ok(parsed) = value.parse() {
                self.lookback_hours_long = parsed;
            }
        }
        if let Ok(value) = env::var("HALF_LIFE_MAX_HOURS") {
            if let Ok(parsed) = value.parse() {
                self.half_life_max_hours = parsed;
            }
        }
        if let Ok(value) = env::var("ADF_P_THRESHOLD") {
            if let Ok(parsed) = value.parse() {
                self.adf_p_threshold = parsed;
            }
        }
        if let Ok(value) = env::var("ENTRY_VOL_LOOKBACK_HOURS") {
            if let Ok(parsed) = value.parse() {
                self.entry_vol_lookback_hours = parsed;
            }
        }
        if let Ok(value) = env::var("SLIPPAGE_BPS") {
            if let Ok(parsed) = value.parse::<i32>() {
                self.slippage_bps = parsed;
            }
        }
        if let Ok(value) = env::var("FEE_BPS") {
            if let Ok(parsed) = value.parse() {
                self.fee_bps = parsed;
            }
        }
        if let Ok(value) = env::var("MAX_LEVERAGE") {
            if let Ok(parsed) = value.parse() {
                self.max_leverage = parsed;
            }
        }
        if let Ok(value) = env::var("REEVAL_JUMP_Z_MULT") {
            if let Ok(parsed) = value.parse() {
                self.reeval_jump_z_mult = parsed;
            }
        }
        if let Ok(value) = env::var("VOL_SPIKE_MULT") {
            if let Ok(parsed) = value.parse() {
                self.vol_spike_mult = parsed;
            }
        }
        if let Ok(value) = env::var("MAX_ACTIVE_PAIRS") {
            if let Ok(parsed) = value.parse() {
                self.max_active_pairs = parsed;
            }
        }
        if let Ok(value) = env::var("WARM_START_MODE") {
            if let Ok(parsed) = value.parse() {
                self.warm_start_mode = parsed;
            }
        }
        let mut warm_start_min_overridden = false;
        if let Ok(value) = env::var("WARM_START_MIN_BARS") {
            if let Ok(parsed) = value.parse() {
                self.warm_start_min_bars = parsed;
                warm_start_min_overridden = true;
            }
        }
        if !warm_start_min_overridden
            && !warm_start_min_from_yaml
            && self.warm_start_min_bars == prev_metrics_window
            && self.metrics_window != prev_metrics_window
        {
            self.warm_start_min_bars = self.metrics_window;
        }
        if let Ok(value) = env::var("ORDER_TIMEOUT_SECS") {
            if let Ok(parsed) = value.parse() {
                self.order_timeout_secs = parsed;
            }
        }
        if let Ok(value) = env::var("ENTRY_PARTIAL_FILL_MAX_RETRIES") {
            if let Ok(parsed) = value.parse() {
                self.entry_partial_fill_max_retries = parsed;
            }
        }
        if let Ok(value) = env::var("STARTUP_FORCE_CLOSE_ATTEMPTS") {
            if let Ok(parsed) = value.parse::<u32>() {
                if parsed > 0 {
                    self.startup_force_close_attempts = parsed;
                }
            }
        }
        if let Ok(value) = env::var("STARTUP_FORCE_CLOSE_WAIT_SECS") {
            if let Ok(parsed) = value.parse::<u64>() {
                self.startup_force_close_wait_secs = parsed;
            }
        }
        if let Ok(value) = env::var("FORCE_CLOSE_ON_STARTUP") {
            let lower = value.trim().to_ascii_lowercase();
            self.force_close_on_startup = lower == "1" || lower == "true" || lower == "yes";
        }

        let env_pairs = env::var("UNIVERSE_PAIRS")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let env_symbols = env::var("UNIVERSE_SYMBOLS")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let universe_overridden = env_pairs.is_some() || env_symbols.is_some();
        if universe_overridden {
            self.universe = parse_universe_pairs()?;
        }

        if let Ok(value) = env::var("ENABLE_DATA_DUMP") {
            self.enable_data_dump = value.to_lowercase() == "true";
        }
        if let Ok(value) = env::var("DATA_DUMP_FILE") {
            if !value.trim().is_empty() {
                self.data_dump_file = Some(value);
            }
        }
        if self.enable_data_dump && self.data_dump_file.is_none() {
            return Err(anyhow!(
                "DATA_DUMP_FILE must be set if ENABLE_DATA_DUMP is true"
            ));
        }

        if let Ok(value) = env::var("OBSERVE_ONLY") {
            self.observe_only = value.to_lowercase() == "true";
        }
        // Note: enable_data_dump no longer forces observe_only. Data dump
        // is just JSONL writes to disk and is independent of trading.
        // The previous forced linkage prevented running a bot that both
        // collects data and trades live (e.g. debot-pair-btceth running
        // as the data collector while the A leg of an A/B test).

        if let Ok(value) = env::var("DISABLE_HISTORY_PERSIST") {
            let lower = value.trim().to_ascii_lowercase();
            self.disable_history_persist = lower == "1" || lower == "true" || lower == "yes";
        }
        if let Ok(value) = env::var("PAIRTRADE_HISTORY_FILE") {
            if !value.trim().is_empty() {
                self.history_file = value.trim().to_string();
            }
        } else if universe_overridden && !history_file_from_yaml {
            self.history_file = default_history_file(&self.universe, self.agent_name.as_deref());
        }

        if let Ok(value) = env::var("BACKTEST_MODE") {
            self.backtest_mode = value.to_lowercase() == "true";
        }
        if let Ok(value) = env::var("BACKTEST_FILE") {
            if !value.trim().is_empty() {
                self.backtest_file = Some(value);
            }
        }
        if self.backtest_mode && self.backtest_file.is_none() {
            return Err(anyhow!(
                "BACKTEST_FILE must be set if BACKTEST_MODE is true"
            ));
        }

        if let Ok(value) = env::var("SPREAD_TREND_MAX_SLOPE_SIGMA") {
            if let Ok(parsed) = value.parse() {
                self.spread_trend_max_slope_sigma = parsed;
            }
        }
        if let Ok(value) = env::var("BETA_DIVERGENCE_MAX") {
            if let Ok(parsed) = value.parse() {
                self.beta_divergence_max = parsed;
            }
        }
        if let Ok(value) = env::var("CIRCUIT_BREAKER_CONSECUTIVE_LOSSES") {
            if let Ok(parsed) = value.parse() {
                self.circuit_breaker_consecutive_losses = parsed;
            }
        }
        if let Ok(value) = env::var("CIRCUIT_BREAKER_COOLDOWN_SECS") {
            if let Ok(parsed) = value.parse() {
                self.circuit_breaker_cooldown_secs = parsed;
            }
        }
        if let Ok(value) = env::var("CIRCUIT_BREAKER_TIER1_LOSSES") {
            if let Ok(parsed) = value.parse() {
                self.circuit_breaker_tier1_losses = parsed;
            }
        }
        if let Ok(value) = env::var("CIRCUIT_BREAKER_TIER1_COOLDOWN_SECS") {
            if let Ok(parsed) = value.parse() {
                self.circuit_breaker_tier1_cooldown_secs = parsed;
            }
        }
        if let Ok(value) = env::var("CIRCUIT_BREAKER_TIER2_LOSSES") {
            if let Ok(parsed) = value.parse() {
                self.circuit_breaker_tier2_losses = parsed;
            }
        }
        if let Ok(value) = env::var("CIRCUIT_BREAKER_TIER2_COOLDOWN_SECS") {
            if let Ok(parsed) = value.parse() {
                self.circuit_breaker_tier2_cooldown_secs = parsed;
            }
        }
        if let Ok(value) = env::var("ENTRY_POST_ONLY_TIMEOUT_SECS") {
            if let Ok(parsed) = value.parse() {
                self.entry_post_only_timeout_secs = parsed;
            }
        }
        if let Ok(value) = env::var("ENTRY_VELOCITY_BLOCK_SIGMA_PER_MIN") {
            if let Ok(parsed) = value.parse() {
                self.entry_velocity_block_sigma_per_min = parsed;
            }
        }
        if let Ok(value) = env::var("FUNDING_ENTRY_Z_SCALE") {
            if let Ok(parsed) = value.parse() {
                self.funding_entry_z_scale = parsed;
            }
        }
        if let Ok(value) = env::var("BETA_GAP_ENTRY_Z_SCALE") {
            if let Ok(parsed) = value.parse() {
                self.beta_gap_entry_z_scale = parsed;
            }
        }

        Ok(())
    }

    fn slippage_cost_bps(&self) -> f64 {
        self.slippage_bps.max(0) as f64
    }

    fn circuit_breaker_cooldown_for(&self, losses: u32) -> Option<Duration> {
        // Graduated tiers (check tier2 first as higher threshold)
        if self.circuit_breaker_tier2_losses > 0 && losses >= self.circuit_breaker_tier2_losses {
            return Some(Duration::from_secs(
                self.circuit_breaker_tier2_cooldown_secs,
            ));
        }
        if self.circuit_breaker_tier1_losses > 0 && losses >= self.circuit_breaker_tier1_losses {
            return Some(Duration::from_secs(
                self.circuit_breaker_tier1_cooldown_secs,
            ));
        }
        // Legacy fallback
        if self.circuit_breaker_consecutive_losses > 0
            && losses >= self.circuit_breaker_consecutive_losses
        {
            return Some(Duration::from_secs(self.circuit_breaker_cooldown_secs));
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmStartMode {
    Strict,
    Relaxed,
}

impl std::str::FromStr for WarmStartMode {
    type Err = ();
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "strict" => Ok(WarmStartMode::Strict),
            "relaxed" => Ok(WarmStartMode::Relaxed),
            _ => Err(()),
        }
    }
}

fn env_has_universe_override() -> bool {
    env::var("UNIVERSE_PAIRS")
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        || env::var("UNIVERSE_SYMBOLS")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
}

fn parse_pairs_vec(pairs: &[String]) -> Result<Vec<PairSpec>> {
    let joined = pairs.join(",");
    parse_pairs_list(&joined)
}

fn parse_symbols_vec(symbols: &[String]) -> Result<Vec<PairSpec>> {
    let syms: Vec<String> = symbols
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if syms.is_empty() {
        return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
    }
    let mut pairs = Vec::new();
    for i in 0..syms.len() {
        for j in (i + 1)..syms.len() {
            let a = syms[i].clone();
            let b = syms[j].clone();
            let (base, quote) = if a < b { (a, b) } else { (b, a) };
            pairs.push(PairSpec { base, quote });
        }
    }
    if pairs.is_empty() {
        return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
    }
    Ok(pairs)
}

fn resolve_universe_from_yaml(yaml: &PairTradeYaml) -> Result<Vec<PairSpec>> {
    if env_has_universe_override() {
        return parse_universe_pairs();
    }
    if let Some(pairs) = yaml.universe_pairs.clone() {
        let pairs = pairs.into_vec();
        if pairs.is_empty() {
            return Err(anyhow!("universe_pairs produced no valid pairs"));
        }
        return parse_pairs_vec(&pairs);
    }
    if let Some(symbols) = yaml.universe_symbols.clone() {
        let symbols = symbols.into_vec();
        if symbols.is_empty() {
            return Err(anyhow!("universe_symbols produced no valid pairs"));
        }
        return parse_symbols_vec(&symbols);
    }
    let raw = "BTC/ETH,BTC/SOL,ETH/SOL".to_string();
    parse_pairs_list(&raw)
}

fn parse_universe_pairs() -> Result<Vec<PairSpec>> {
    if let Ok(raw_pairs) = env::var("UNIVERSE_PAIRS") {
        if !raw_pairs.trim().is_empty() {
            return parse_pairs_list(&raw_pairs);
        }
    }
    if let Ok(symbols_raw) = env::var("UNIVERSE_SYMBOLS") {
        if !symbols_raw.trim().is_empty() {
            let syms: Vec<String> = symbols_raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let mut pairs = Vec::new();
            if syms.len() == 1 {
                // Single-symbol mode: create a self-pair for data-dump collection
                pairs.push(PairSpec {
                    base: syms[0].clone(),
                    quote: syms[0].clone(),
                });
            } else {
                for i in 0..syms.len() {
                    for j in (i + 1)..syms.len() {
                        let a = syms[i].clone();
                        let b = syms[j].clone();
                        let (base, quote) = if a < b { (a, b) } else { (b, a) };
                        pairs.push(PairSpec { base, quote });
                    }
                }
            }
            if pairs.is_empty() {
                return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
            }
            return Ok(pairs);
        }
    }
    let raw = "BTC/ETH,BTC/SOL,ETH/SOL".to_string();
    parse_pairs_list(&raw)
}

fn default_history_file(universe: &[PairSpec], _agent_name: Option<&str>) -> String {
    let mut symbols: Vec<String> = universe
        .iter()
        .flat_map(|p| [p.base.clone(), p.quote.clone()])
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    // A/B/C bots watching the same pair on the same host intentionally share
    // one history file so their rolling regression windows stay identical
    // (pairtrade#4). The shared file is written atomically via tmpfile+rename
    // in persist_history_to_disk to avoid torn reads under concurrent writers.
    if symbols.is_empty() {
        return "pairtrade_history.json".to_string();
    }
    symbols.sort();
    let parts: Vec<String> = symbols
        .into_iter()
        .map(|sym| sanitize_symbol_for_filename(&sym))
        .filter(|sym| !sym.is_empty())
        .collect();
    if parts.is_empty() {
        return "pairtrade_history.json".to_string();
    }
    format!("pairtrade_history_{}.json", parts.join("_"))
}

fn sanitize_symbol_for_filename(symbol: &str) -> String {
    let mut out = String::with_capacity(symbol.len());
    for ch in symbol.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}

fn parse_pairs_list(raw: &str) -> Result<Vec<PairSpec>> {
    let mut pairs = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut split = trimmed.split('/');
        let base = split
            .next()
            .ok_or_else(|| anyhow!("invalid pair: {}", trimmed))?;
        let quote = split
            .next()
            .ok_or_else(|| anyhow!("invalid pair: {}", trimmed))?;
        pairs.push(PairSpec {
            base: base.to_string(),
            quote: quote.to_string(),
        });
    }
    if pairs.is_empty() {
        return Err(anyhow!("UNIVERSE_PAIRS produced no valid pairs"));
    }
    Ok(pairs)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PositionDirection {
    LongSpread,
    ShortSpread,
}

#[derive(Debug, Clone)]
struct Position {
    direction: PositionDirection,
    entered_at: Instant,
    /// Replay-aware entry timestamp (seconds). In live mode equals
    /// `chrono::Utc::now().timestamp()` at the moment of entry; in backtest
    /// mode equals the replay's logical timestamp. Used for all
    /// duration-based decisions (force_close, hold-time PnL, etc.) so they
    /// behave identically under replay.
    entered_ts: i64,
    entry_price_a: Option<Decimal>,
    entry_price_b: Option<Decimal>,
    entry_size_a: Option<Decimal>,
    entry_size_b: Option<Decimal>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PnlLogRecord {
    ts: i64,
    pair: String,
    base: String,
    quote: String,
    direction: String,
    pnl: f64,
    source: String,
    // Trade log fields for backtest calibration
    #[serde(skip_serializing_if = "Option::is_none")]
    entry_price_a: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entry_price_b: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_price_a: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_price_b: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    beta: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    z_entry: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    z_exit: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hold_secs: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EquityBaseline {
    date: String,
    equity: f64,
}

#[derive(Debug, Serialize)]
struct EquityHistoryPoint {
    ts: i64,
    equity: f64,
}

struct PnlLogger {
    dir: PathBuf,
    tag: Option<String>,
    retain_days: u64,
    last_cleanup: Option<Instant>,
}

#[derive(Debug)]
struct StatusReporter {
    path: PathBuf,
    id: Option<String>,
    agent: Option<String>,
    dex: String,
    dry_run: bool,
    backtest_mode: bool,
    interval_secs: u64,
    snapshot_every: Duration,
    pnl_total: f64,
    pnl_today: f64,
    pnl_today_date: NaiveDate,
    equity_day_start: f64,
    equity_day_start_set: bool,
    equity_baseline_path: PathBuf,
    equity_history_path: PathBuf,
    last_equity_history_ts: Option<i64>,
    last_snapshot: Option<Instant>,
    trade_stats: Option<PairTradeStats>,
    maintenance: Option<String>,
}

#[derive(Debug, Serialize)]
struct StatusPosition {
    symbol: String,
    side: String,
    size: String,
    entry_price: Option<String>,
}

#[derive(Debug, Serialize)]
struct StatusSnapshot {
    ts: i64,
    updated_at: String,
    id: Option<String>,
    agent: Option<String>,
    dex: String,
    dry_run: bool,
    backtest_mode: bool,
    interval_secs: u64,
    positions_ready: bool,
    position_count: usize,
    has_position: bool,
    positions: Vec<StatusPosition>,
    pnl_total: f64,
    pnl_today: f64,
    pnl_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    trade_stats: Option<PairTradeStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maintenance: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PairTradeStats {
    trades: u64,
    wins: u64,
    win_rate: f64,
    max_dd: f64,
    pnl: f64,
}

impl PnlLogger {
    fn from_env(cfg: &PairTradeConfig) -> Option<Self> {
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

    fn log(&mut self, record: PnlLogRecord) -> std::io::Result<()> {
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

impl StatusReporter {
    fn from_env(cfg: &PairTradeConfig) -> Option<Self> {
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
        let interval_secs = cfg.interval_secs.max(1);
        let snapshot_every = {
            let target_secs = 60_u64;
            let n = ((target_secs + interval_secs - 1) / interval_secs).max(1);
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
            equity_day_start: 0.0,
            equity_day_start_set: false,
            equity_baseline_path,
            equity_history_path,
            last_equity_history_ts: None,
            last_snapshot: None,
            trade_stats: None,
            maintenance: None,
        };
        reporter.load_equity_baseline();
        if let Err(err) = reporter.ensure_status_file() {
            log::warn!(
                "[STATUS] failed to create status file {}: {:?}",
                reporter.path.display(),
                err
            );
        }
        Some(reporter)
    }

    fn ensure_status_file(&self) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        Ok(())
    }

    fn load_equity_baseline(&mut self) {
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

    fn persist_equity_baseline(&self) {
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

    fn append_equity_history(&mut self, equity: f64) {
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

    fn update_equity(&mut self, equity: f64) {
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

    fn set_maintenance(&mut self, status: Option<String>) {
        self.maintenance = status;
    }

    fn write_snapshot(
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
            trade_stats: self.trade_stats.clone(),
            maintenance: self.maintenance.clone(),
        };
        let payload = serde_json::to_string(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = self.path.with_extension("json.tmp");
        fs::write(&tmp_path, payload)?;
        fs::rename(tmp_path, &self.path)?;
        Ok(())
    }

    fn write_snapshot_if_due(
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

    fn reset_daily_if_needed(&mut self) {
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

fn sanitize_pnl_tag(raw: &str) -> String {
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

fn is_pnl_log_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    name.starts_with("pnl-") && name.ends_with(".jsonl")
}

fn direction_label(direction: PositionDirection) -> &'static str {
    match direction {
        PositionDirection::LongSpread => "long_spread",
        PositionDirection::ShortSpread => "short_spread",
    }
}

impl PnlLogRecord {
    fn new(
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
        }
    }

    fn with_trade_details(
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

#[derive(Debug)]
struct PairState {
    beta: f64,
    z_entry: f64,
    spread_history: VecDeque<f64>,
    last_spread: Option<f64>,
    last_velocity_sigma_per_min: f64,
    position: Option<Position>,
    last_exit_at: Option<Instant>,
    /// Replay-aware companion to `last_exit_at`. Drives the should_enter
    /// cooldown and unhedged-close cooldown so they fire correctly under
    /// backtest replay.
    last_exit_ts: Option<i64>,
    beta_short: f64,
    beta_long: f64,
    half_life_hours: f64,
    adf_p_value: f64,
    eligible: bool,
    last_evaluated: Option<Instant>,
    /// Replay-aware companion to `last_evaluated`. Drives the periodic
    /// pair re-evaluation interval (PAIR_SELECTION_INTERVAL_SECS).
    last_evaluated_ts: Option<i64>,
    p_value_weighted_score: f64,
    beta_gap: f64,
    pending_entry: Option<PendingOrders>,
    pending_exit: Option<PendingOrders>,
    position_guard: bool,
}

#[derive(Debug, Clone)]
struct PendingLeg {
    symbol: String,
    order_id: String,
    exchange_order_id: Option<String>,
    target: Decimal,
    filled: Decimal,
    side: dex_connector::OrderSide,
    #[allow(dead_code)]
    placed_price: Decimal,
}

#[derive(Debug)]
struct PendingOrders {
    legs: Vec<PendingLeg>,
    direction: PositionDirection,
    placed_at: Instant,
    hedge_retry_count: u32,
    post_only_hybrid: bool,
}

#[derive(Debug)]
struct PendingStatus {
    open_remaining: usize,
    fills: HashMap<String, Decimal>,
    open_ids: HashSet<String>,
}

#[derive(Debug)]
struct PartialOrderPlacementError {
    legs: Vec<PendingLeg>,
    source: DexError,
}

impl PartialOrderPlacementError {
    fn new(legs: Vec<PendingLeg>, source: DexError) -> Self {
        Self { legs, source }
    }

    fn legs(&self) -> &[PendingLeg] {
        &self.legs
    }
}

impl std::fmt::Display for PartialOrderPlacementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to place all legs: {}", self.source)
    }
}

impl Error for PartialOrderPlacementError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug, Clone)]
struct BarBuilder {
    window_secs: u64,
    start_ts: Option<i64>,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    /// Exchange timestamp of the tick currently used as `close`. Used to keep
    /// the bar close monotonic with respect to the exchange clock so that two
    /// bots observing the same WS feed converge on the same close price for
    /// the same bucket. Updates from older ts are ignored even if they arrive
    /// later in wall-clock time, and the open price is locked to the
    /// earliest tick of the bucket. See pairtrade#4.
    close_ts: Option<i64>,
    open_ts: Option<i64>,
}

impl BarBuilder {
    fn new(window_secs: u64) -> Self {
        Self {
            window_secs,
            start_ts: None,
            open: Decimal::ZERO,
            high: Decimal::ZERO,
            low: Decimal::ZERO,
            close: Decimal::ZERO,
            close_ts: None,
            open_ts: None,
        }
    }

    /// Align a timestamp down to the wall-clock bucket boundary.
    ///
    /// Buckets are anchored to the Unix epoch (`floor(ts / window) * window`),
    /// so all bots observing the same stream produce identical bucket IDs
    /// regardless of their own startup phase. This is required for multi-bot
    /// A/B fairness: without this, each process anchors its first bar to its
    /// own first tick, causing beta/mean/std/z to diverge across bots even
    /// though they share the same price feed. See pairtrade#4.
    fn bucket_start(&self, ts: i64) -> i64 {
        let w = self.window_secs as i64;
        if w <= 0 {
            return ts;
        }
        ts - ts.rem_euclid(w)
    }

    fn push(&mut self, ts: i64, price: Decimal) -> Option<(Decimal, i64)> {
        let current_bucket = self.bucket_start(ts);
        match self.start_ts {
            None => {
                self.start_ts = Some(current_bucket);
                self.open = price;
                self.high = price;
                self.low = price;
                self.close = price;
                self.close_ts = Some(ts);
                self.open_ts = Some(ts);
                None
            }
            Some(start) => {
                if current_bucket > start {
                    let prev_close = self.close;
                    let bar_close_ts = start.saturating_add(self.window_secs as i64);
                    self.start_ts = Some(current_bucket);
                    self.open = price;
                    self.high = price;
                    self.low = price;
                    self.close = price;
                    self.close_ts = Some(ts);
                    self.open_ts = Some(ts);
                    Some((prev_close, bar_close_ts))
                } else {
                    // Within the same bucket: pick the tick with the largest
                    // exchange ts as the canonical close (deterministic across
                    // processes); fall back to last-write-wins if ts info is
                    // missing. The open price is locked to the earliest ts.
                    if price > self.high {
                        self.high = price;
                    }
                    if price < self.low || self.low.is_zero() {
                        self.low = price;
                    }
                    match self.close_ts {
                        Some(prev_close_ts) if ts < prev_close_ts => {
                            // older tick — leave close unchanged
                        }
                        _ => {
                            self.close = price;
                            self.close_ts = Some(ts);
                        }
                    }
                    match self.open_ts {
                        Some(prev_open_ts) if ts >= prev_open_ts => {
                            // newer tick — open already locked to earlier ts
                        }
                        _ => {
                            self.open = price;
                            self.open_ts = Some(ts);
                        }
                    }
                    None
                }
            }
        }
    }
}

impl PairState {
    fn new(window: usize, z_entry: f64) -> Self {
        Self {
            beta: 1.0,
            z_entry,
            spread_history: VecDeque::with_capacity(window),
            last_spread: None,
            last_velocity_sigma_per_min: 0.0,
            position: None,
            last_exit_at: None,
            last_exit_ts: None,
            beta_short: 1.0,
            beta_long: 1.0,
            half_life_hours: 0.0,
            adf_p_value: 1.0,
            eligible: false,
            last_evaluated: None,
            last_evaluated_ts: None,
            p_value_weighted_score: 0.0,
            beta_gap: 0.0,
            pending_entry: None,
            pending_exit: None,
            position_guard: false,
        }
    }

    fn push_spread(&mut self, spread: f64, window: usize, config: &PairTradeConfig) {
        if self.spread_history.len() >= window {
            self.spread_history.pop_front();
        }
        self.spread_history.push_back(spread);
        self.last_spread = Some(spread);

        // velocity uses bar-to-bar move (1-minute bars) normalized by std dev
        let k = 1_usize;
        if self.spread_history.len() > k {
            if let (Some(&latest), Some(&past)) = (
                self.spread_history.back(),
                self.spread_history.get(self.spread_history.len() - k - 1),
            ) {
                let delta = latest - past; // per-bar move
                let per_min = delta / ((k as f64 * config.trading_period_secs as f64) / 60.0);
                if let Some((_z, std)) = self.z_score() {
                    if std > 1e-9 {
                        self.last_velocity_sigma_per_min = per_min / std;
                    }
                }
            }
        }
    }

    fn z_score(&self) -> Option<(f64, f64)> {
        self.z_score_details().map(|(z, std, _, _)| (z, std))
    }

    fn z_score_details(&self) -> Option<(f64, f64, f64, f64)> {
        if self.spread_history.len() < 2 {
            return None;
        }
        let (mean, std) = mean_std(&self.spread_history)?;
        let latest = *self.spread_history.back().unwrap();
        let z = if std < 1e-9 {
            0.0
        } else {
            (latest - mean) / std
        };
        Some((z, std, mean, latest))
    }
}

pub struct PairTradeEngine {
    cfg: PairTradeConfig,
    connector: Arc<dyn DexConnector + Send + Sync>,
    states: HashMap<String, PairState>,
    history: HashMap<String, VecDeque<PriceSample>>,
    bar_builders: HashMap<String, BarBuilder>,
    equity_cache: f64,
    last_equity_fetch: Option<Instant>,
    last_metrics_log: Option<Instant>,
    last_ob_warn: HashMap<String, Instant>,
    last_ticker_warn: HashMap<String, Instant>,
    last_position_warn: HashMap<String, Instant>,
    min_order_warned: HashSet<String>,
    min_tick_warned: HashSet<String>,
    positions_ready: bool,
    open_positions: HashMap<String, PositionSnapshot>,
    history_path: PathBuf,
    data_dump_writer: Option<BufWriter<File>>,
    replay_connector: Option<Arc<ReplayConnector>>,
    pnl_logger: Option<PnlLogger>,
    status_reporter: Option<StatusReporter>,
    consecutive_losses: u32,
    circuit_breaker_until: Option<Instant>,
    /// Replay-aware companion to `circuit_breaker_until`. Compared against
    /// the per-step `now_ts` so backtest replays can honour the same
    /// cool-down logic as live.
    circuit_breaker_until_ts: Option<i64>,
    total_trades: u64,
    total_wins: u64,
    total_pnl: f64,
    peak_pnl: f64,
    max_dd: f64,
    /// Graceful shutdown flag. When true:
    ///   - new entries are blocked
    ///   - existing exit logic (exit_z / stop_loss_z / force_close_secs) runs normally
    ///   - live loop exits as soon as open_positions is empty, or after shutdown_grace_secs
    shutdown_pending: bool,
}

struct PlannedAction {
    pair: PairSpec,
    key: String,
    action: TradeAction,
    net_funding_per_hour: f64,
    abs_z: f64,
    liquidity_score: f64,
    p1: SymbolSnapshot,
    p2: SymbolSnapshot,
}

enum TradeAction {
    Open {
        direction: PositionDirection,
        z: f64,
        beta: f64,
    },
    Close {
        direction: PositionDirection,
        z: f64,
        beta: f64,
        force: bool,
    },
    None,
}

impl PairTradeEngine {
    /// Create a new engine with a pre-loaded ReplayConnector (for batch mode).
    pub async fn new_with_replay(
        cfg: PairTradeConfig,
        replay: Arc<ReplayConnector>,
    ) -> Result<Self> {
        replay.reset();
        Self::new_inner(cfg, replay.clone(), Some(replay)).await
    }

    pub async fn new(cfg: PairTradeConfig) -> Result<Self> {
        let (connector, replay_connector): (
            Arc<dyn DexConnector + Send + Sync>,
            Option<Arc<ReplayConnector>>,
        ) = if cfg.backtest_mode {
            // Backtest mode: use the ReplayConnector
            let replay = Arc::new(ReplayConnector::new(
                cfg.backtest_file.as_ref().unwrap().as_str(),
            )?);
            (replay.clone(), Some(replay))
        } else {
            // Live mode: use the DexConnectorBox
            let tokens: Vec<String> = cfg
                .universe
                .iter()
                .flat_map(|p| [p.base.clone(), p.quote.clone()])
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            let live_connector = DexConnectorBox::create(
                &cfg.dex_name,
                &cfg.rest_endpoint,
                &cfg.web_socket_endpoint,
                cfg.dry_run,
                cfg.agent_name.clone(),
                &tokens,
            )
            .await
            .context("failed to initialize connector")?;
            live_connector
                .start()
                .await
                .context("failed to start connector")?;
            (Arc::new(live_connector), None)
        };

        Self::new_inner(cfg, connector, replay_connector).await
    }

    async fn new_inner(
        cfg: PairTradeConfig,
        connector: Arc<dyn DexConnector + Send + Sync>,
        replay_connector: Option<Arc<ReplayConnector>>,
    ) -> Result<Self> {
        let mut states = HashMap::new();
        let mut history = HashMap::new();
        let mut bar_builders = HashMap::new();
        for pair in &cfg.universe {
            let pair_key = format!("{}/{}", pair.base, pair.quote);
            let pp = cfg.params_for(&pair_key);
            states.insert(
                pair_key,
                PairState::new(cfg.metrics_window, pp.entry_z_base),
            );
            history.insert(pair.base.clone(), VecDeque::new());
            history.insert(pair.quote.clone(), VecDeque::new());
            bar_builders.insert(pair.base.clone(), BarBuilder::new(cfg.trading_period_secs));
            bar_builders.insert(pair.quote.clone(), BarBuilder::new(cfg.trading_period_secs));
        }

        let equity_cache = cfg.equity_usd;
        let history_path = PathBuf::from(cfg.history_file.as_str());

        let min_order_warned = HashSet::new();
        let min_tick_warned = HashSet::new();
        let data_dump_writer = if cfg.enable_data_dump {
            let file_path = cfg.data_dump_file.as_ref().unwrap(); // is_none checked in from_env
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_path)?;
            Some(BufWriter::new(file))
        } else {
            None
        };

        let backtest_mode = cfg.backtest_mode;
        let pnl_logger = PnlLogger::from_env(&cfg);
        let status_reporter = StatusReporter::from_env(&cfg);
        Ok(Self {
            cfg,
            connector,
            replay_connector,
            states,
            history,
            bar_builders,
            equity_cache,
            last_equity_fetch: None,
            last_metrics_log: None,
            last_ob_warn: HashMap::new(),
            last_ticker_warn: HashMap::new(),
            last_position_warn: HashMap::new(),
            min_order_warned,
            min_tick_warned,
            positions_ready: backtest_mode,
            open_positions: HashMap::new(),
            history_path,
            data_dump_writer,
            pnl_logger,
            status_reporter,
            consecutive_losses: 0,
            circuit_breaker_until: None,
            circuit_breaker_until_ts: None,
            total_trades: 0,
            total_wins: 0,
            total_pnl: 0.0,
            peak_pnl: 0.0,
            max_dd: 0.0,
            shutdown_pending: false,
        })
    }

    fn write_pnl_record(&mut self, record: PnlLogRecord) {
        // Update trade stats
        self.total_trades += 1;
        self.total_pnl += record.pnl;
        if record.pnl > 0.0 {
            self.total_wins += 1;
        }
        if self.total_pnl > self.peak_pnl {
            self.peak_pnl = self.total_pnl;
        }
        let dd = self.peak_pnl - self.total_pnl;
        if dd > self.max_dd {
            self.max_dd = dd;
        }

        // Update status reporter
        if let Some(reporter) = &mut self.status_reporter {
            let wr = if self.total_trades > 0 {
                self.total_wins as f64 / self.total_trades as f64 * 100.0
            } else { 0.0 };
            reporter.trade_stats = Some(PairTradeStats {
                trades: self.total_trades,
                wins: self.total_wins,
                win_rate: wr,
                max_dd: self.max_dd,
                pnl: self.total_pnl,
            });
        }

        if let Some(logger) = &mut self.pnl_logger {
            if let Err(err) = logger.log(record) {
                log::warn!("[PNL] failed to write pnl log: {:?}", err);
            }
        }
    }

    fn is_inconsistent_state(err: &anyhow::Error) -> bool {
        let msg = err.to_string();
        msg.contains("Inconsistent state")
    }

    async fn log_inconsistent_state_debug(&mut self, err: &anyhow::Error) {
        if !Self::is_inconsistent_state(err) {
            return;
        }

        // Log internal state for active pairs
        for (key, state) in self.states.iter() {
            let is_active = state.position.is_some()
                || state.pending_entry.is_some()
                || state.pending_exit.is_some()
                || state.position_guard;
            if !is_active {
                continue;
            }
            log::error!(
                "[DEBUG][STATE] key={} position={:?} pending_entry={:?} pending_exit={:?} guard={} positions_ready={}",
                key,
                state.position,
                state.pending_entry.as_ref().map(|p| p.legs.len()),
                state.pending_exit.as_ref().map(|p| p.legs.len()),
                state.position_guard,
                self.positions_ready
            );
        }

        // Log what the exchange reports for positions
        match self.connector.get_positions().await {
            Ok(pos) => {
                let filtered: Vec<_> = pos
                    .into_iter()
                    .filter(|p| p.sign != 0 && p.size > Decimal::ZERO)
                    .collect();
                log::error!("[DEBUG][EXCHANGE_POSITIONS] {:?}", filtered);
            }
            Err(get_err) => {
                log::error!(
                    "[DEBUG][EXCHANGE_POSITIONS] failed to fetch positions: {:?}",
                    get_err
                );
            }
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        log::info!("[CONFIG] DEX_NAME is: {}", self.cfg.dex_name);
        log::info!(
            "[CONFIG] FEE_BPS={} SLIPPAGE_BPS={} post_only_supported={} post_only_enabled={}",
            self.cfg.fee_bps,
            self.cfg.slippage_bps,
            self.post_only_supported(),
            self.should_post_only()
        );
        self.load_history_from_disk();
        self.warm_start_states_from_history();

        if self.replay_connector.is_some() {
            // --- Backtest Mode ---
            log::info!("[BACKTEST] Running in backtest mode.");
            loop {
                if let Err(e) = self.step().await {
                    // In backtest, we might want to stop on error. For now, just log it.
                    log::error!("[BACKTEST] Step failed: {:?}", e);
                }
                // Advance the replay connector to the next data point
                let has_more = {
                    let replay = self
                        .replay_connector
                        .as_ref()
                        .expect("replay connector should exist in backtest mode");
                    replay.tick()
                };
                if !has_more {
                    log::info!("[BACKTEST] End of data file reached. Backtest finished.");
                    break;
                }
            }
        } else {
            // --- Live Mode ---
            log::info!("[LIVE] Running in live mode.");
            if self.cfg.force_close_on_startup {
                self.force_close_on_startup().await?;
            }
            // allow connector/WS to warm up before first step to reduce spurious logs
            sleep(Duration::from_secs(5)).await;
            // Wall-clock aligned ticker: fires at floor(now/interval)*interval + interval boundaries
            // so every bot process observing the same stream ticks at identical wall-clock seconds.
            // This is required on top of the BarBuilder bucket alignment (pairtrade#4): without
            // aligning the tick phase itself, two bots would sample the last tick of a 60s bucket
            // at different wall-clock seconds and therefore see slightly different close prices,
            // which cascades into divergent beta/mean/std/z.
            let interval_secs = self.cfg.interval_secs.max(1);
            fn next_wall_clock_boundary(interval_secs: u64) -> tokio::time::Instant {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now_unix_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let interval_ms = interval_secs.saturating_mul(1000);
                let next_boundary_ms = ((now_unix_ms / interval_ms) + 1) * interval_ms;
                let wait_ms = next_boundary_ms.saturating_sub(now_unix_ms);
                tokio::time::Instant::now() + Duration::from_millis(wait_ms)
            }
            let mut next_tick = next_wall_clock_boundary(interval_secs);
            let mut sigterm = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            )
            .expect("failed to register SIGTERM handler");
            let mut sigint = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::interrupt(),
            )
            .expect("failed to register SIGINT handler");

            let grace = Duration::from_secs(self.cfg.shutdown_grace_secs);
            let mut shutdown_deadline: Option<Instant> = None;
            let mut force_shutdown = false;
            loop {
                // Graceful shutdown: exit as soon as positions are flat, or after grace expires.
                if self.shutdown_pending {
                    if self.open_positions.is_empty() {
                        log::info!("[PAIR] Shutdown: all positions flat, exiting");
                        break;
                    }
                    if let Some(dl) = shutdown_deadline {
                        if Instant::now() >= dl {
                            log::warn!(
                                "[PAIR] Shutdown grace ({}s) expired with {} open positions, force-closing",
                                self.cfg.shutdown_grace_secs,
                                self.open_positions.len()
                            );
                            force_shutdown = true;
                            break;
                        }
                    }
                }

                tokio::select! {
                    _ = tokio::time::sleep_until(next_tick) => {
                        next_tick = next_wall_clock_boundary(interval_secs);
                        // Monitor step() execution time. If it exceeds interval_secs,
                        // the next wall-clock boundary will be skipped, causing tick
                        // phase to drift across A/B/C bots and breaking bar alignment
                        // (pairtrade#4). WARN so we can spot it in production logs.
                        let step_start = Instant::now();
                        if let Err(e) = self.step().await {
                            self.log_inconsistent_state_debug(&e).await;
                            log::error!("pairtrade step failed: {:?}", e);
                        }
                        let step_elapsed = step_start.elapsed();
                        if step_elapsed >= Duration::from_secs(interval_secs) {
                            log::warn!(
                                "[STEP_OVERRUN] step() took {:.2}s >= interval_secs={}; \
                                 next wall-clock tick will be skipped, A/B/C alignment may drift",
                                step_elapsed.as_secs_f64(),
                                interval_secs
                            );
                        }
                    }
                    _ = sigterm.recv() => {
                        if !self.shutdown_pending {
                            if self.open_positions.is_empty() || self.cfg.shutdown_grace_secs == 0 {
                                log::info!(
                                    "[PAIR] SIGTERM received, shutting down (flat={}, grace={}s)",
                                    self.open_positions.is_empty(),
                                    self.cfg.shutdown_grace_secs
                                );
                                force_shutdown = !self.open_positions.is_empty();
                                break;
                            }
                            log::info!(
                                "[PAIR] SIGTERM received, entering graceful shutdown: \
                                 waiting for natural exit of {} open positions (grace={}s). \
                                 Send SIGTERM/SIGINT again to force.",
                                self.open_positions.len(),
                                self.cfg.shutdown_grace_secs
                            );
                            self.shutdown_pending = true;
                            shutdown_deadline = Some(Instant::now() + grace);
                        } else {
                            log::warn!("[PAIR] Second SIGTERM received, force-closing immediately");
                            force_shutdown = true;
                            break;
                        }
                    }
                    _ = sigint.recv() => {
                        if self.shutdown_pending {
                            log::warn!("[PAIR] SIGINT received during graceful shutdown, force-closing");
                            force_shutdown = true;
                            break;
                        } else {
                            log::info!("[PAIR] SIGINT received, shutting down...");
                            force_shutdown = !self.open_positions.is_empty();
                            break;
                        }
                    }
                }
            }

            if force_shutdown {
                log::warn!("[PAIR] Force-closing all open positions on shutdown");
                if let Err(e) = self.connector.close_all_positions(None).await {
                    log::error!("[PAIR] close_all_positions on shutdown failed: {:?}", e);
                }
            }
        }
        if let Some(reporter) = &mut self.status_reporter {
            if let Err(err) = reporter.write_snapshot(&self.open_positions, self.positions_ready) {
                log::warn!("[STATUS] failed to write status: {:?}", err);
            }
        }
        Ok(())
    }

    async fn reissue_partial_legs(
        &mut self,
        pending: &PendingOrders,
        filled_qtys: &HashMap<String, Decimal>,
        price_map: &HashMap<String, SymbolSnapshot>,
        reduce_only: bool,
        use_market: bool,
        retry_count: u32,
    ) -> Result<Option<PendingOrders>> {
        let mut new_legs = Vec::new();
        let stage = if reduce_only { "exit" } else { "entry" };
        for leg in &pending.legs {
            let filled = filled_qtys
                .get(&leg.order_id)
                .cloned()
                .unwrap_or(Decimal::ZERO)
                .max(leg.filled)
                .min(leg.target);
            let remaining = (leg.target - filled).max(Decimal::ZERO);
            if remaining <= Decimal::ZERO {
                let mut kept = leg.clone();
                kept.filled = filled;
                new_legs.push(kept);
                continue;
            }
            if !use_market {
                let has_price = price_map
                    .get(&leg.symbol)
                    .map(|s| s.price > Decimal::ZERO)
                    .unwrap_or(false);
                if !has_price {
                    log::warn!(
                        "[ORDER] Cannot reissue {} leg {}: missing price",
                        stage,
                        leg.symbol
                    );
                    let mut kept = leg.clone();
                    kept.filled = filled;
                    new_legs.push(kept);
                    continue;
                }
            }
            let quantized_size = if reduce_only {
                self.quantize_order_size_close(&leg.symbol, remaining, price_map)
            } else {
                self.quantize_order_size(&leg.symbol, remaining, price_map)
            };
            if quantized_size <= Decimal::ZERO {
                log::warn!(
                    "[ORDER] {} leg {} remaining {} below tick; skipping",
                    stage,
                    leg.symbol,
                    remaining
                );
                let mut kept = leg.clone();
                kept.filled = filled;
                new_legs.push(kept);
                continue;
            }
            let limit = if use_market {
                None
            } else {
                self.limit_price_for(&leg.symbol, leg.side, price_map)
            };
            if !use_market && limit.is_none() {
                log::warn!(
                    "[ORDER] Cannot reissue {} leg {}: missing reference price",
                    stage,
                    leg.symbol
                );
                let mut kept = leg.clone();
                kept.filled = filled;
                new_legs.push(kept);
                continue;
            }
            let spread = self.order_spread_param(limit, false);
            match self
                .connector
                .create_order(
                    &leg.symbol,
                    quantized_size,
                    leg.side,
                    limit,
                    spread,
                    reduce_only,
                    None,
                )
                .await
            {
                Ok(resp) => {
                    log::warn!(
                        "[ORDER] Reissued {} leg {} size={}",
                        stage,
                        leg.symbol,
                        quantized_size
                    );
                    if filled > Decimal::ZERO {
                        new_legs.push(PendingLeg {
                            symbol: leg.symbol.clone(),
                            order_id: leg.order_id.clone(),
                            exchange_order_id: leg.exchange_order_id.clone(),
                            target: filled,
                            filled,
                            side: leg.side,
                            placed_price: leg.placed_price,
                        });
                    }
                    new_legs.push(PendingLeg {
                        symbol: leg.symbol.clone(),
                        order_id: resp.order_id,
                        exchange_order_id: resp.exchange_order_id,
                        target: quantized_size,
                        filled: Decimal::ZERO,
                        side: leg.side,
                        placed_price: resp.ordered_price,
                    });
                }
                Err(e) => {
                    let symbol = leg.symbol.clone();
                    if reduce_only && Self::is_reduce_only_position_missing_error(&e) {
                        if self.confirm_reduce_only_position_missing(&symbol).await {
                            log::info!(
                                "[ORDER] {} leg {} already closed; skipping reissue",
                                stage,
                                symbol
                            );
                            let mut kept = leg.clone();
                            kept.filled = leg.target;
                            new_legs.push(kept);
                        } else {
                            log::error!(
                                "[ORDER] Failed to reissue {} leg {}: {:?}",
                                stage,
                                symbol,
                                e
                            );
                            let mut kept = leg.clone();
                            kept.filled = filled;
                            new_legs.push(kept);
                        }
                    } else {
                        log::error!(
                            "[ORDER] Failed to reissue {} leg {}: {:?}",
                            stage,
                            symbol,
                            e
                        );
                        let mut kept = leg.clone();
                        kept.filled = filled;
                        new_legs.push(kept);
                    }
                }
            }
        }
        if new_legs.is_empty() {
            return Ok(None);
        }
        Ok(Some(PendingOrders {
            legs: new_legs,
            direction: pending.direction,
            placed_at: Instant::now(),
            hedge_retry_count: retry_count,
            post_only_hybrid: false,
        }))
    }

    async fn reissue_entry_as_taker(
        &mut self,
        key: &str,
        pending: &PendingOrders,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Result<Option<PendingOrders>> {
        let mut new_legs = Vec::new();
        for leg in &pending.legs {
            let size = self.quantize_order_size(&leg.symbol, leg.target, price_map);
            if size <= Decimal::ZERO {
                log::warn!(
                    "[ORDER] {} taker reissue leg {} below min size; skipping",
                    key,
                    leg.symbol
                );
                continue;
            }
            match self
                .connector
                .create_order(
                    &leg.symbol,
                    size,
                    leg.side,
                    None, // no limit price = market/taker
                    None,
                    false,
                    None,
                )
                .await
            {
                Ok(resp) => {
                    log::info!(
                        "[ORDER] {} taker reissue leg {} size={}",
                        key,
                        leg.symbol,
                        size
                    );
                    new_legs.push(PendingLeg {
                        symbol: leg.symbol.clone(),
                        order_id: resp.order_id,
                        exchange_order_id: resp.exchange_order_id,
                        target: size,
                        filled: Decimal::ZERO,
                        side: leg.side,
                        placed_price: resp.ordered_price,
                    });
                }
                Err(e) => {
                    log::error!(
                        "[ORDER] {} taker reissue failed for {}: {:?}",
                        key,
                        leg.symbol,
                        e
                    );
                }
            }
        }
        if new_legs.is_empty() {
            return Ok(None);
        }
        Ok(Some(PendingOrders {
            legs: new_legs,
            direction: pending.direction,
            placed_at: Instant::now(),
            hedge_retry_count: 0,
            post_only_hybrid: false,
        }))
    }

    fn format_positions_summary(positions: &[PositionSnapshot]) -> String {
        let mut parts = Vec::with_capacity(positions.len());
        for position in positions {
            let side = match position.sign.cmp(&0) {
                Ordering::Greater => "LONG",
                Ordering::Less => "SHORT",
                Ordering::Equal => "FLAT",
            };
            let entry = position
                .entry_price
                .map(|price| price.to_string())
                .unwrap_or_else(|| "n/a".to_string());
            parts.push(format!(
                "{} {} size={} entry={}",
                position.symbol, side, position.size, entry
            ));
        }
        parts.join(", ")
    }

    async fn force_close_on_startup(&self) -> Result<()> {
        if self.cfg.dry_run || self.cfg.observe_only {
            log::info!(
                "[Startup] DRY RUN/OBSERVE ONLY: Would cancel all orders and close all positions"
            );
            return Ok(());
        }
        let attempts = self.cfg.startup_force_close_attempts.max(1);
        let wait_secs = self.cfg.startup_force_close_wait_secs;
        log::info!(
            "[Startup] Force closing any existing orders/positions (attempts={}, wait_secs={})",
            attempts,
            wait_secs
        );
        if let Err(err) = self.connector.cancel_all_orders(None).await {
            log::warn!("[Startup] cancel_all_orders failed: {:?}", err);
        }
        for attempt in 1..=attempts {
            let positions_result = self.connector.get_positions().await;
            match positions_result {
                Ok(positions) if positions.is_empty() => {
                    if attempt == 1 {
                        log::info!("[Startup] No open positions detected");
                    } else {
                        log::info!("[Startup] All positions closed");
                    }
                    return Ok(());
                }
                Ok(positions) => {
                    log::info!(
                        "[Startup] close attempt {}/{}: {}",
                        attempt,
                        attempts,
                        Self::format_positions_summary(&positions)
                    );
                }
                Err(err) => {
                    log::warn!(
                        "[Startup] get_positions failed on attempt {}/{}: {:?}",
                        attempt,
                        attempts,
                        err
                    );
                }
            }

            if let Err(err) = self.connector.close_all_positions(None).await {
                log::error!("[Startup] close_all_positions failed: {:?}", err);
            }

            if attempt < attempts && wait_secs > 0 {
                sleep(Duration::from_secs(wait_secs)).await;
            }
        }

        if wait_secs > 0 {
            sleep(Duration::from_secs(wait_secs)).await;
        }
        match self.connector.get_positions().await {
            Ok(positions) if positions.is_empty() => {
                log::info!("[Startup] All positions closed");
            }
            Ok(positions) => {
                let summary = Self::format_positions_summary(&positions);
                log::error!(
                    "[Startup] positions still open after {} attempts: {}",
                    attempts,
                    summary
                );
                let subject = match self.cfg.agent_name.as_deref() {
                    Some(name) => format!("[{}] Startup close failed", name),
                    None => format!(
                        "[Startup] Failed to close positions (dex={})",
                        self.cfg.dex_name
                    ),
                };
                let body = format!(
                    "Startup force close failed after {} attempts.\nOpen positions: {}",
                    attempts, summary
                );
                EmailClient::new().send(&subject, &body);
            }
            Err(err) => {
                log::error!(
                    "[Startup] get_positions failed after {} attempts: {:?}",
                    attempts,
                    err
                );
            }
        }
        Ok(())
    }

    async fn force_close_all_positions(&self, key: &str, reason: &str) {
        if self.cfg.dry_run || self.cfg.observe_only {
            log::warn!(
                "[EXIT] {} force close skipped (mode) reason={}",
                key,
                reason
            );
            return;
        }
        log::error!(
            "[EXIT] {} exceeded exit retries; invoking close_all_positions reason={}",
            key,
            reason
        );
        if let Err(err) = self.connector.close_all_positions(None).await {
            log::error!("[EXIT] close_all_positions failed: {:?}", err);
        }
    }

    async fn step(&mut self) -> Result<()> {
        // Skip new entries if maintenance is upcoming within 1 hour
        let maintenance_block_entries = self.connector.is_upcoming_maintenance(1).await;
        if maintenance_block_entries {
            log::warn!("Upcoming maintenance detected; blocking new entries this cycle");
        }
        if let Some(reporter) = &mut self.status_reporter {
            reporter.set_maintenance(if maintenance_block_entries {
                Some("blocking_entries".to_string())
            } else {
                None
            });
        }

        self.refresh_equity_if_needed().await?;
        let price_map = self.fetch_latest_prices().await?;
        self.sync_positions_from_exchange(&price_map).await?;

        if let Some(writer) = &mut self.data_dump_writer {
            let dump_entry = DataDumpEntry {
                timestamp: Utc::now().timestamp_millis(),
                prices: &price_map,
            };
            if let Ok(json_string) = serde_json::to_string(&dump_entry) {
                if writeln!(writer, "{}", json_string).is_err() {
                    log::error!("[DataDump] Failed to write to dump file");
                }
            }
        }

        let vol_median = self.compute_vol_median();
        let positions_clear = self.open_positions.is_empty();
        let has_pending_orders = self
            .states
            .values()
            .any(|state| state.pending_entry.is_some() || state.pending_exit.is_some());
        if !positions_clear && !has_pending_orders && self.should_log_position_warn("entry_block") {
            log::info!(
                "[POSITION] open positions detected ({} symbols) with no pending orders; blocking new entries",
                self.open_positions.len()
            );
            self.last_position_warn
                .insert("entry_block".to_string(), Instant::now());
        }
        let mut planned: Vec<PlannedAction> = Vec::new();

        // Push history samples for symbols using 1-minute bars.
        //
        // Bar timestamps come from the exchange-side `last_updated_at` of each
        // tick (see SymbolSnapshot::exchange_ts), NOT from the local wall clock.
        // This is required for multi-bot A/B fairness (pairtrade#4): all bots
        // observing the same WS feed see identical event timestamps for the
        // same update, so they place ticks into identical buckets and produce
        // identical bar closes regardless of their own scheduling jitter.
        // Falls back to local wall clock only when the connector cannot supply
        // an exchange timestamp (e.g. REST fallback path).
        let max_history_len = self.max_history_len();
        let now_ts = self.current_now_ts();
        // Merge bars written by other bots since the last tick. Without this
        // each process's `history` only ever holds bars from its own WS feed,
        // and ms-level differences in tick aggregation get amplified by the
        // regression into z-value divergence across A/B/C (pairtrade#4).
        // Reloading before bar-build means every bot reads the most recent
        // consensus state before pushing its own latest bar.
        self.load_history_from_disk();
        let mut updated = HashSet::new();
        for (symbol, snapshot) in price_map.iter() {
            if let Some(builder) = self.bar_builders.get_mut(symbol) {
                let tick_ts = snapshot.exchange_ts.unwrap_or(now_ts);
                if let Some((close_price, close_ts)) = builder.push(tick_ts, snapshot.price) {
                    let entry = self
                        .history
                        .entry(symbol.clone())
                        .or_insert_with(VecDeque::new);
                    let log_price = close_price
                        .to_f64()
                        .ok_or_else(|| anyhow!("invalid price for {}", symbol))?
                        .ln();
                    // First writer wins for a given bucket_ts: if another bot
                    // has already persisted this bar, leave the shared value
                    // in place. Once a bar is recorded it becomes immutable,
                    // so all three bots converge on one canonical series.
                    if entry.back().map(|s| s.ts) != Some(close_ts) {
                        if entry.len() >= max_history_len {
                            entry.pop_front();
                        }
                        entry.push_back(PriceSample {
                            log_price,
                            ts: close_ts,
                        });
                    }
                    updated.insert(symbol.clone());
                }
            } else {
                log::debug!("no bar builder for {}", symbol);
            }
        }
        self.persist_history_to_disk();

        let universe = self.cfg.universe.clone();
        for pair in &universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            let (p1, p2) = match (price_map.get(&pair.base), price_map.get(&pair.quote)) {
                (Some(a), Some(b)) => (a, b),
                _ => continue,
            };
            if !(updated.contains(&pair.base) && updated.contains(&pair.quote)) {
                continue;
            }

            // First, reconcile any pending entry/exit orders for this pair
            self.reconcile_pending_orders(&key, &price_map).await?;

            let mut action = TradeAction::None;
            let log_a = self
                .latest_log_price(&pair.base)
                .ok_or_else(|| anyhow!("no bar for {}", pair.base))?;
            let log_b = self
                .latest_log_price(&pair.quote)
                .ok_or_else(|| anyhow!("no bar for {}", pair.quote))?;

            let (
                prev_eligible,
                z_snapshot,
                last_eval_ts,
                z_entry_copy,
                spread_len,
                position_state,
                velocity,
                beta_eff,
                beta_short,
                beta_long,
            ) = {
                let state = self
                    .states
                    .get_mut(&key)
                    .ok_or_else(|| anyhow!("missing state for {}", key))?;
                let prev_eligible = state.eligible;
                let spread = log_a - state.beta * log_b;
                state.push_spread(spread, self.cfg.metrics_window, &self.cfg);
                (
                    prev_eligible,
                    state.z_score_details(),
                    state.last_evaluated_ts,
                    state.z_entry,
                    state.spread_history.len(),
                    state.position.clone(),
                    state.last_velocity_sigma_per_min,
                    state.beta,
                    state.beta_short,
                    state.beta_long,
                )
            };

            // [ZCHECK] Per-step alignment audit log. Designed for side-by-side
            // comparison across A/B/C bots running the same pair: if buckets are
            // properly aligned, identical bucket_ts rows should show identical
            // close/beta/mean/std/z values across processes. See pairtrade#4.
            let base_bar = self.history.get(&pair.base).and_then(|h| h.back()).cloned();
            let quote_bar = self.history.get(&pair.quote).and_then(|h| h.back()).cloned();
            if let (Some(ba), Some(bq)) = (base_bar, quote_bar) {
                if let Some((z, std, mean, latest)) = z_snapshot {
                    log::info!(
                        "[ZCHECK] {} bucket_ts={} close_a={:.6} close_b={:.6} \
                         beta_eff={:.4} beta_s={:.4} beta_l={:.4} mean={:.6} std={:.6} \
                         spread={:.6} z={:.4} hist={}",
                        key,
                        ba.ts,
                        ba.log_price,
                        bq.log_price,
                        beta_eff,
                        beta_short,
                        beta_long,
                        mean,
                        std,
                        latest,
                        z,
                        spread_len,
                    );
                }
            }

            let pp = self.cfg.params_for(&key);
            let force_close_due = position_state
                .as_ref()
                .map(|pos| now_ts.saturating_sub(pos.entered_ts) >= pp.force_close_secs as i64)
                .unwrap_or(false);
            if force_close_due {
                if let Some(pos) = &position_state {
                    log::info!("[EXIT_CHECK] {} reason=force_close", key);
                    action = TradeAction::Close {
                        direction: pos.direction,
                        z: 0.0,
                        beta: beta_eff,
                        force: true,
                    };
                }
            }

            if self.states[&key].pending_entry.is_some() || self.states[&key].pending_exit.is_some()
            {
                if !matches!(action, TradeAction::None) {
                    log::debug!("[ORDER] {} has pending orders; skipping new actions", key);
                }
                continue;
            }
            if self.states[&key].position_guard {
                if matches!(action, TradeAction::None) {
                    if self.should_log_position_warn(&key) {
                        log::warn!(
                            "[POSITION] {} in unhedged/mismatch state; skipping new actions",
                            key
                        );
                        self.last_position_warn.insert(key.clone(), Instant::now());
                    }
                    continue;
                }
            }

            let needs_eval_interval = last_eval_ts
                .map(|t| now_ts.saturating_sub(t) >= PAIR_SELECTION_INTERVAL_SECS as i64)
                .unwrap_or(true);
            let needs_eval_jump = z_snapshot
                .map(|(z, _, _, _)| z.abs() >= z_entry_copy * pp.reeval_jump_z_mult)
                .unwrap_or(false);
            let needs_eval_velocity =
                velocity.abs() >= pp.spread_velocity_max_sigma_per_min * pp.reeval_jump_z_mult;
            let vol_spike = z_snapshot
                .and_then(|(_, std, _, _)| {
                    tail_std(&self.states[&key].spread_history, self.cfg.metrics_window).map(
                        |base_std| {
                            if base_std <= 1e-9 {
                                0.0
                            } else {
                                std / base_std
                            }
                        },
                    )
                })
                .map(|ratio| ratio >= pp.vol_spike_mult)
                .unwrap_or(false);

            let eval = if needs_eval_interval || needs_eval_jump || needs_eval_velocity || vol_spike
            {
                let res = self.evaluate_pair(pair);
                if let Some(ref e) = res {
                    log::info!(
                        "[EVAL] {} beta_s={:.3} beta_l={:.3} beta={:.3} hl={:.2}h p={:.3} eligible={} score={:.3}",
                        key,
                        e.beta_short,
                        e.beta_long,
                        e.beta_eff,
                        e.half_life_hours,
                        e.adf_p_value,
                        e.eligible,
                        e.score
                    );
                } else {
                    let (avail_a, avail_b) = (
                        self.history.get(&pair.base).map(|h| h.len()).unwrap_or(0),
                        self.history.get(&pair.quote).map(|h| h.len()).unwrap_or(0),
                    );
                    log::debug!(
                        "[EVAL] {} insufficient history ({}:{}, need long/short (strict) {} / {}, mode={:?})",
                        key,
                        pair.base,
                        avail_a,
                        pp.lookback_hours_long
                            .max(pp.lookback_hours_short)
                            * 3600
                            / self.cfg.trading_period_secs,
                        (pp.lookback_hours_short * 3600) / self.cfg.trading_period_secs,
                        self.cfg.warm_start_mode
                    );
                    log::debug!(
                        "[EVAL] {} insufficient history ({}:{}, need long/short (strict) {} / {}, mode={:?})",
                        key,
                        pair.quote,
                        avail_b,
                        pp.lookback_hours_long
                            .max(pp.lookback_hours_short)
                            * 3600
                            / self.cfg.trading_period_secs,
                        (pp.lookback_hours_short * 3600) / self.cfg.trading_period_secs,
                        self.cfg.warm_start_mode
                    );
                }
                res
            } else {
                None
            };

            let mut log_positions_not_ready = false;
            {
                let state = self
                    .states
                    .get_mut(&key)
                    .ok_or_else(|| anyhow!("missing state for {}", key))?;
                if let Some(ref eval) = eval {
                    state.beta = eval.beta_eff;
                    state.beta_short = eval.beta_short;
                    state.beta_long = eval.beta_long;
                    state.half_life_hours = eval.half_life_hours;
                    state.adf_p_value = eval.adf_p_value;
                    state.eligible = eval.eligible;
                    state.p_value_weighted_score = eval.score;
                    state.beta_gap = eval.beta_gap;
                    state.last_evaluated = Some(Instant::now());
                    state.last_evaluated_ts = Some(now_ts);
                }
                if prev_eligible != state.eligible {
                    log::info!(
                        "[ELIGIBILITY] {} -> {} (p={:.3} hl={:.2}h beta_gap={:.3})",
                        key,
                        state.eligible,
                        state.adf_p_value,
                        state.half_life_hours,
                        (state.beta_short - state.beta_long).abs()
                    );
                }

                let z_entry = entry_z_for_pair(&self.cfg, pp, state, vol_median);
                state.z_entry = z_entry;

                let min_points = (self.cfg.metrics_window / 2).max(10);
                if matches!(action, TradeAction::None) {
                    if state.eligible && spread_len >= min_points {
                        if let Some((z, std, mean, latest_spread)) = z_snapshot {
                            let net_funding = net_funding_for_direction(z, p1, p2);
                            if let Some(pos) = &state.position {
                                let equity_base = self.equity_cache.max(self.cfg.equity_usd);
                                if let Some(reason) =
                                    exit_reason(&self.cfg, pp, state, z, std, p1, p2, equity_base, now_ts)
                                {
                                    log::info!(
                                    "[EXIT_CHECK] {} reason={} z={:.2} exit_z={:.2} stop_z={:.2} vel={:.3} max_vel={:.3}",
                                    key,
                                    reason,
                                    z,
                                    pp.exit_z,
                                    pp.stop_loss_z,
                                    state.last_velocity_sigma_per_min,
                                    pp.spread_velocity_max_sigma_per_min
                                );
                                    action = TradeAction::Close {
                                        direction: pos.direction,
                                        z,
                                        beta: state.beta,
                                        force: false,
                                    };
                                }
                            } else if !self.positions_ready {
                                log_positions_not_ready = true;
                            } else if self
                                .circuit_breaker_until_ts
                                .map_or(false, |until| now_ts < until)
                            {
                                // entry blocked by circuit breaker; logged via ZCHECK
                            } else if last_eval_ts.is_none() {
                                // Block entry until first evaluate_pair() completes,
                                // because beta is still at its initial value (1.0).
                            } else if should_enter(&self.cfg, pp, state, z, std, net_funding, now_ts) {
                                let direction = if z > 0.0 {
                                    PositionDirection::ShortSpread
                                } else {
                                    PositionDirection::LongSpread
                                };
                                action = TradeAction::Open {
                                    direction,
                                    z,
                                    beta: state.beta,
                                };
                            }
                            let slope_sig =
                                spread_slope_sigma(&state.spread_history, self.cfg.metrics_window);
                            log::debug!(
                            "[ZCHECK] {} z={:.2} entry={:.2} std={:.4} mean={:.4} spread={:.4} hist={} beta_s={:.3} beta_l={:.3} funding={:.5} eligible={} beta_gap={:.3} slope_sigma={:.3} consec_loss={}",
                            key,
                            z,
                            state.z_entry,
                            std,
                            mean,
                            latest_spread,
                            spread_len,
                            beta_short,
                            beta_long,
                            net_funding,
                            state.eligible,
                            state.beta_gap,
                            slope_sig.unwrap_or(0.0),
                            self.consecutive_losses
                        );
                        }
                    } else if state.eligible && spread_len < min_points {
                        log::debug!(
                            "[ZCHECK] {} skipped (spread history too short: {} < {})",
                            key,
                            spread_len,
                            min_points
                        );
                    } else if position_state.is_some() && !state.eligible {
                        // If pair falls out of eligibility, flatten
                        if let Some(pos) = &state.position {
                            log::info!("[EXIT_CHECK] {} reason=ineligible", key);
                            action = TradeAction::Close {
                                direction: pos.direction,
                                z: 0.0,
                                beta: state.beta,
                                force: false,
                            };
                        }
                    }
                }
            }
            if !positions_clear && matches!(action, TradeAction::Open { .. }) {
                log::debug!("[ENTRY] blocked due to open positions; key={}", key);
                action = TradeAction::None;
            }
            if maintenance_block_entries && matches!(action, TradeAction::Open { .. }) {
                action = TradeAction::None;
            }
            if self.shutdown_pending && matches!(action, TradeAction::Open { .. }) {
                log::debug!("[ENTRY] blocked by graceful shutdown; key={}", key);
                action = TradeAction::None;
            }

            if log_positions_not_ready && self.should_log_position_warn(&self.cfg.dex_name) {
                log::warn!("[POSITION] positions not synced yet; skipping entry");
                self.last_position_warn
                    .insert(self.cfg.dex_name.clone(), Instant::now());
            }

            if !matches!(action, TradeAction::None) {
                let net_funding = net_funding_for_direction(
                    match &action {
                        TradeAction::Open { z, .. } => *z,
                        TradeAction::Close { z, .. } => *z,
                        TradeAction::None => 0.0,
                    },
                    p1,
                    p2,
                );
                let abs_z = match &action {
                    TradeAction::Open { z, .. } | TradeAction::Close { z, .. } => z.abs(),
                    TradeAction::None => 0.0,
                };
                planned.push(PlannedAction {
                    pair: pair.clone(),
                    key: key.clone(),
                    action,
                    net_funding_per_hour: net_funding,
                    abs_z,
                    liquidity_score: liquidity_score(p1, p2),
                    p1: p1.clone(),
                    p2: p2.clone(),
                });
            }
        }

        self.maybe_log_metrics();
        // Process exits first
        for plan in planned.iter() {
            if let TradeAction::Close {
                direction,
                z,
                beta,
                force,
            } = plan.action
            {
                let qtys = self
                    .exit_sizes_for_pair(&plan.key, &plan.pair, beta, &plan.p1, &plan.p2)
                    .context("exit_sizes_for_pair")?;
                if qtys.0 <= Decimal::ZERO && qtys.1 <= Decimal::ZERO {
                    log::warn!(
                        "[EXIT] {} no open position sizes available; clearing state",
                        plan.key
                    );
                    if let Some(state) = self.states.get_mut(&plan.key) {
                        state.position = None;
                        state.pending_exit = None;
                        state.position_guard = false;
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                    }
                    continue;
                }
                if qtys.0 <= Decimal::ZERO || qtys.1 <= Decimal::ZERO {
                    log::warn!(
                        "[EXIT] {} missing leg size (base={}, quote={}); closing available legs only",
                        plan.key,
                        qtys.0,
                        qtys.1
                    );
                }
                if self.cfg.dry_run {
                    let price_a = price_map
                        .get(&plan.pair.base)
                        .map(|s| s.price)
                        .unwrap_or_default();
                    let price_b = price_map
                        .get(&plan.pair.quote)
                        .map(|s| s.price)
                        .unwrap_or_default();
                    let pnl = self
                        .states
                        .get(&plan.key)
                        .and_then(|s| s.position.as_ref())
                        .and_then(|pos| compute_pnl(pos, price_a, price_b));
                    if let Some(pnl) = pnl {
                        if let Some(pnl_value) = pnl.to_f64() {
                            let pos_ref = self.states.get(&plan.key)
                                .and_then(|s| s.position.as_ref());
                            let hold_secs = pos_ref
                                .map(|p| now_ts.saturating_sub(p.entered_ts).max(0) as f64);
                            let entry_a = pos_ref
                                .and_then(|p| p.entry_price_a)
                                .and_then(|v| v.to_f64());
                            let entry_b = pos_ref
                                .and_then(|p| p.entry_price_b)
                                .and_then(|v| v.to_f64());
                            let record = PnlLogRecord::new(
                                &plan.pair.base,
                                &plan.pair.quote,
                                direction,
                                pnl_value,
                                now_ts,
                                "exit_dry_run",
                            ).with_trade_details(
                                entry_a, entry_b,
                                price_a.to_f64(), price_b.to_f64(),
                                Some(beta), Some(z),
                                self.states.get(&plan.key)
                                    .and_then(|s| s.last_spread.map(|_| z)),
                                hold_secs,
                            );
                            self.write_pnl_record(record);
                            if pnl_value < 0.0 {
                                self.consecutive_losses += 1;
                                if let Some(cooldown) = self
                                    .cfg
                                    .circuit_breaker_cooldown_for(self.consecutive_losses)
                                {
                                    self.circuit_breaker_until = Some(Instant::now() + cooldown);
                                    self.circuit_breaker_until_ts =
                                        Some(now_ts + cooldown.as_secs() as i64);
                                    log::warn!(
                                        "[CIRCUIT_BREAKER] activated after {} consecutive losses, cooldown {}s",
                                        self.consecutive_losses, cooldown.as_secs()
                                    );
                                }
                            } else if pnl_value > 0.0 {
                                if self.consecutive_losses > 0 {
                                    log::info!("[CIRCUIT_BREAKER] reset after win (was {} consecutive losses)", self.consecutive_losses);
                                }
                                self.consecutive_losses = 0;
                                self.circuit_breaker_until = None;
                                self.circuit_breaker_until_ts = None;
                            }
                        }
                        log::info!(
                            "[EXIT] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} force={} pnl={} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            force,
                            pnl,
                            now_ts
                        );
                    } else {
                        log::info!(
                            "[EXIT] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} force={} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            force,
                            now_ts
                        );
                    }
                    if let Some(state) = self.states.get_mut(&plan.key) {
                        state.position = None;
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                    }
                } else if self.cfg.observe_only {
                    log::info!(
                        "[EXIT] observe-only mode; skipping close orders for {}/{}",
                        plan.pair.base,
                        plan.pair.quote
                    );
                } else {
                    let legs = match self
                        .close_pair_orders(&plan.pair, direction, qtys, &price_map, force)
                        .await
                    {
                        Ok(legs) => legs,
                        Err(err) => {
                            self.register_partial_leg_failure(&plan.key, direction, &err, true);
                            return Err(err);
                        }
                    };
                    if let Some(state) = self.states.get_mut(&plan.key) {
                        state.pending_exit = Some(PendingOrders {
                            legs,
                            direction,
                            placed_at: Instant::now(),
                            hedge_retry_count: 0,
                            post_only_hybrid: false,
                        });
                    }
                }
            }
        }

        let mut active_symbols: HashSet<String> = self
            .cfg
            .universe
            .iter()
            .filter_map(|pair| {
                let key = format!("{}/{}", pair.base, pair.quote);
                let state = self.states.get(&key)?;
                let is_active = state.position.is_some()
                    || state.pending_entry.is_some()
                    || state.pending_exit.is_some()
                    || state.position_guard;
                if is_active {
                    let mut symbols = HashSet::new();
                    symbols.insert(pair.base.clone());
                    symbols.insert(pair.quote.clone());
                    Some(symbols)
                } else {
                    None
                }
            })
            .flatten()
            .collect();
        for symbol in self.open_positions.keys() {
            if self.history.contains_key(symbol) {
                active_symbols.insert(symbol.clone());
            }
        }

        // Among entry candidates, shortlist by model score then pick best by funding->score->liquidity->|z|
        let mut entry_candidates: Vec<&PlannedAction> = planned
            .iter()
            .filter(|p| matches!(p.action, TradeAction::Open { .. }))
            .filter(|p| {
                if active_symbols.is_empty() {
                    return true;
                }
                let overlaps =
                    active_symbols.contains(&p.pair.base) || active_symbols.contains(&p.pair.quote);
                if overlaps {
                    log::debug!(
                        "[OVERLAP] skipping {}/{} due to active symbol overlap",
                        p.pair.base,
                        p.pair.quote
                    );
                }
                !overlaps
            })
            .collect();
        entry_candidates.sort_by(|a, b| {
            self.state_score(&b.key)
                .partial_cmp(&self.state_score(&a.key))
                .unwrap_or(Ordering::Equal)
        });
        let shortlisted: Vec<&PlannedAction> = entry_candidates
            .into_iter()
            .take(self.cfg.max_active_pairs.max(1))
            .collect();
        let best_entry = shortlisted.into_iter().max_by(|a, b| {
            a.net_funding_per_hour
                .partial_cmp(&b.net_funding_per_hour)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    self.state_score(&a.key)
                        .partial_cmp(&self.state_score(&b.key))
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| {
                    a.liquidity_score
                        .partial_cmp(&b.liquidity_score)
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| a.abs_z.partial_cmp(&b.abs_z).unwrap_or(Ordering::Equal))
        });
        if let Some(plan) = best_entry {
            if let TradeAction::Open { direction, z, beta } = plan.action {
                let qtys = self
                    .hedged_sizes(&plan.pair, beta, &plan.p1, &plan.p2)
                    .context("hedged_sizes")?;
                let price_a = price_map
                    .get(&plan.pair.base)
                    .map(|s| s.price)
                    .unwrap_or_default();
                let price_b = price_map
                    .get(&plan.pair.quote)
                    .map(|s| s.price)
                    .unwrap_or_default();
                if self.cfg.dry_run {
                    log::info!(
                            "[ENTRY] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} carry={:.4} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            plan.net_funding_per_hour,
                            now_ts
                        );
                    if let Some(state) = self.states.get_mut(&plan.key) {
                        state.position = Some(Position {
                            direction,
                            entered_at: Instant::now(),
                            entered_ts: now_ts,
                            entry_price_a: Some(price_a),
                            entry_price_b: Some(price_b),
                            entry_size_a: Some(qtys.0),
                            entry_size_b: Some(qtys.1),
                        });
                    }
                } else if self.cfg.observe_only {
                    log::info!(
                        "[ENTRY] observe-only mode; skipping entry orders for {}/{}",
                        plan.pair.base,
                        plan.pair.quote
                    );
                } else {
                    log::info!(
                        "[ENTRY] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} carry={:.4} ts={}",
                        plan.pair.base,
                        plan.pair.quote,
                        direction,
                        qtys.0,
                        price_a,
                        qtys.1,
                        price_b,
                        z,
                        beta,
                        plan.net_funding_per_hour,
                        now_ts
                    );
                    let legs = match self
                        .place_pair_orders(&plan.pair, direction, qtys, &price_map)
                        .await
                    {
                        Ok(legs) => legs,
                        Err(err) => {
                            self.register_partial_leg_failure(&plan.key, direction, &err, false);
                            return Err(err);
                        }
                    };
                    let entry_pp = self.cfg.params_for(&plan.key);
                    let hybrid =
                        entry_pp.entry_post_only_timeout_secs > 0 && self.post_only_supported();
                    if let Some(state) = self.states.get_mut(&plan.key) {
                        state.pending_entry = Some(PendingOrders {
                            legs,
                            direction,
                            placed_at: Instant::now(),
                            hedge_retry_count: 0,
                            post_only_hybrid: hybrid,
                        });
                    }
                }
            }
        }

        if let Some(reporter) = &mut self.status_reporter {
            if let Err(err) =
                reporter.write_snapshot_if_due(&self.open_positions, self.positions_ready)
            {
                log::warn!("[STATUS] failed to write status: {:?}", err);
            }
        }
        Ok(())
    }

    fn latest_log_price(&self, symbol: &str) -> Option<f64> {
        self.history
            .get(symbol)
            .and_then(|h| h.back())
            .map(|p| p.log_price)
    }

    async fn refresh_equity_if_needed(&mut self) -> Result<()> {
        const CACHE_SECS: u64 = 300;
        if self
            .last_equity_fetch
            .map(|t| t.elapsed() < Duration::from_secs(CACHE_SECS))
            .unwrap_or(false)
        {
            return Ok(());
        }
        match self.connector.get_balance(None).await {
            Ok(resp) => {
                if let Some(eq) = resp.equity.to_f64() {
                    self.equity_cache = eq.max(0.0);
                    self.last_equity_fetch = Some(Instant::now());
                    if let Some(reporter) = &mut self.status_reporter {
                        reporter.update_equity(self.equity_cache);
                    }
                }
            }
            Err(err) => {
                log::warn!("equity refresh failed, using fallback: {:?}", err);
                self.last_equity_fetch = Some(Instant::now());
            }
        }
        Ok(())
    }

    async fn sync_positions_from_exchange(
        &mut self,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Result<()> {
        if self.replay_connector.is_some() {
            return Ok(());
        }
        let now_ts = self.current_now_ts();
        let positions = match self.connector.get_positions().await {
            Ok(v) => v,
            Err(err) => {
                let err_msg = err.to_string();
                if err_msg.contains("positions not ready from websocket") {
                    let stale_clear_secs = self.cfg.order_timeout_secs.max(1).saturating_mul(6);
                    self.clear_stale_pending(Duration::from_secs(stale_clear_secs), "ws_not_ready");
                    if self.should_log_position_warn(&self.cfg.dex_name) {
                        log::warn!(
                            "[POSITION] waiting for initial WS positions on {}",
                            self.cfg.dex_name
                        );
                        self.last_position_warn
                            .insert(self.cfg.dex_name.clone(), Instant::now());
                    }
                    self.positions_ready = false;
                    return Ok(());
                }
                if self.should_log_position_warn(&self.cfg.dex_name) {
                    log::warn!(
                        "[POSITION] get_positions not available for {}: {:?}",
                        self.cfg.dex_name,
                        err
                    );
                    self.last_position_warn
                        .insert(self.cfg.dex_name.clone(), Instant::now());
                }
                return Ok(());
            }
        };
        self.positions_ready = true;

        let mut snapshots: HashMap<String, PositionSnapshot> = HashMap::new();
        for snapshot in positions {
            if snapshot.sign == 0 || snapshot.size <= Decimal::ZERO {
                continue;
            }
            if self.is_dust_position(&snapshot, prices) {
                continue;
            }
            snapshots.insert(snapshot.symbol.clone(), snapshot);
        }
        self.open_positions = snapshots.clone();

        let mut unhedged_attempted: HashSet<String> = HashSet::new();
        let mut unhedged_closures: Vec<(String, String, i32, Decimal)> = Vec::new();
        for pair in &self.cfg.universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            let log_warn = self.should_log_position_warn(&key);

            let Some(state) = self.states.get_mut(&key) else {
                continue;
            };

            let base = snapshots.get(&pair.base);
            let quote = snapshots.get(&pair.quote);

            if state.pending_entry.is_some() || state.pending_exit.is_some() {
                // Keep pending orders; reconciliation handles timeouts/hedging.
                continue;
            }

            match (base, quote) {
                (None, None) => {
                    if state.position.is_some() || state.position_guard {
                        log::info!("[POSITION] {} cleared by exchange snapshot", key);
                    }
                    state.position = None;
                    state.position_guard = false;
                }
                (Some(b), Some(q)) => {
                    if b.sign * q.sign >= 0 {
                        if log_warn {
                            log::warn!(
                                "[POSITION] {} has mismatched legs (signs {} / {})",
                                key,
                                b.sign,
                                q.sign
                            );
                        }
                        if log_warn {
                            self.last_position_warn.insert(key.clone(), Instant::now());
                        }
                        state.position = None;
                        state.position_guard = true;
                        continue;
                    }

                    let direction = if b.sign > 0 {
                        PositionDirection::LongSpread
                    } else {
                        PositionDirection::ShortSpread
                    };
                    let (entered_at, entered_ts) = state
                        .position
                        .as_ref()
                        .map(|p| (p.entered_at, p.entered_ts))
                        .unwrap_or((Instant::now(), now_ts));
                    state.position = Some(Position {
                        direction,
                        entered_at,
                        entered_ts,
                        entry_price_a: b.entry_price,
                        entry_price_b: q.entry_price,
                        entry_size_a: Some(b.size),
                        entry_size_b: Some(q.size),
                    });
                    state.position_guard = false;
                }
                _ => {
                    let active_for_warn = state.position.is_some()
                        || state.pending_entry.is_some()
                        || state.pending_exit.is_some();
                    if state.pending_entry.is_none() && state.pending_exit.is_none() {
                        if let Some((symbol, snapshot)) = base
                            .map(|b| (pair.base.clone(), b))
                            .or_else(|| quote.map(|q| (pair.quote.clone(), q)))
                        {
                            if unhedged_attempted.insert(symbol.clone()) {
                                unhedged_closures.push((
                                    key.clone(),
                                    symbol.clone(),
                                    snapshot.sign,
                                    snapshot.size,
                                ));
                            }
                        }
                    }
                    if log_warn && active_for_warn {
                        log::warn!(
                            "[POSITION] {} has unhedged leg (base={}, quote={})",
                            key,
                            base.is_some(),
                            quote.is_some()
                        );
                        self.last_position_warn.insert(key.clone(), Instant::now());
                        state.position_guard = true;
                    } else {
                        state.position_guard = false;
                    }
                    if !active_for_warn {
                        state.position = None;
                    }
                }
            }
        }

        for (key, symbol, sign, size) in unhedged_closures {
            self.try_close_unhedged_leg(&key, &symbol, sign, size, prices)
                .await;
        }

        Ok(())
    }

    async fn try_close_unhedged_leg(
        &mut self,
        key: &str,
        symbol: &str,
        sign: i32,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) {
        let now_ts = self.current_now_ts();
        if self.cfg.dry_run || self.cfg.observe_only {
            log::warn!(
                "[UNHEDGED] {} close skipped (mode) symbol={} size={}",
                key,
                symbol,
                size
            );
            return;
        }

        const UNHEDGED_CLOSE_COOLDOWN_SECS: u64 = 30;
        let last_exit = self.states.get(key).and_then(|state| state.last_exit_at);
        if let Some(last_exit) = last_exit {
            if last_exit.elapsed() < Duration::from_secs(UNHEDGED_CLOSE_COOLDOWN_SECS) {
                return;
            }
        }

        let side = if sign >= 0 {
            dex_connector::OrderSide::Short
        } else {
            dex_connector::OrderSide::Long
        };
        let qty = self.quantize_order_size_close(symbol, size, prices);
        if qty <= Decimal::ZERO {
            log::warn!(
                "[UNHEDGED] {} close skipped (qty=0) symbol={} size={}",
                key,
                symbol,
                size
            );
            return;
        }

        log::warn!(
            "[UNHEDGED] {} closing lone leg symbol={} sign={} size={} qty={} side={:?}",
            key,
            symbol,
            sign,
            size,
            qty,
            side
        );

        let res = self
            .connector
            .create_order(symbol, qty, side, None, None, true, None)
            .await;

        match res {
            Ok(res) => {
                log::info!(
                    "[UNHEDGED] {} close submitted symbol={} order_id={}",
                    key,
                    symbol,
                    res.order_id
                );
                if let Some(state) = self.states.get_mut(key) {
                    state.last_exit_at = Some(Instant::now());
                    state.last_exit_ts = Some(now_ts);
                }
            }
            Err(err) => {
                if Self::is_reduce_only_position_missing_error(&err)
                    && self.confirm_reduce_only_position_missing(symbol).await
                {
                    log::info!(
                        "[UNHEDGED] {} close skipped; position already closed symbol={}",
                        key,
                        symbol
                    );
                    if let Some(state) = self.states.get_mut(key) {
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                    }
                } else {
                    log::error!(
                        "[UNHEDGED] {} close failed symbol={} err={:?}",
                        key,
                        symbol,
                        err
                    );
                }
            }
        }
    }

    fn clear_stale_pending(&mut self, max_age: Duration, reason: &str) {
        let now_ts = self.current_now_ts();
        for (key, state) in self.states.iter_mut() {
            let entry_age = state.pending_entry.as_ref().map(|p| p.placed_at.elapsed());
            let exit_age = state.pending_exit.as_ref().map(|p| p.placed_at.elapsed());
            let age = match (entry_age, exit_age) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            if let Some(age) = age {
                if age >= max_age {
                    log::warn!(
                        "[POSITION] {} pending cleared (reason={}, age={}s)",
                        key,
                        reason,
                        age.as_secs()
                    );
                    state.pending_entry = None;
                    state.pending_exit = None;
                    state.position = None;
                    state.position_guard = false;
                    state.last_exit_at = Some(Instant::now());
                    state.last_exit_ts = Some(now_ts);
                }
            }
        }
    }

    fn compute_vol_median(&self) -> f64 {
        let tail_len = self.entry_vol_window();
        let mut vols: Vec<f64> = self
            .states
            .values()
            .filter_map(|s| tail_std(&s.spread_history, tail_len))
            .collect();
        if vols.is_empty() {
            return 1.0;
        }
        vols.sort_by(|a, b| a.partial_cmp(b).unwrap());
        vols[vols.len() / 2].max(1e-9)
    }

    fn maybe_log_metrics(&mut self) {
        const LOG_INTERVAL: u64 = 300;
        if self
            .last_metrics_log
            .map(|t| t.elapsed() < Duration::from_secs(LOG_INTERVAL))
            .unwrap_or(false)
        {
            return;
        }
        let mut lines = Vec::new();
        for (k, s) in &self.states {
            let z = s.z_score().map(|(z, _)| z).unwrap_or(0.0);
            lines.push(format!(
                "{} elig={} z={:.2} beta={:.2} hl={:.2}h p={:.3}",
                k, s.eligible, z, s.beta, s.half_life_hours, s.adf_p_value
            ));
        }
        lines.sort();
        if !lines.is_empty() {
            log::info!("[METRICS] {}", lines.join(" | "));
        }
        self.last_metrics_log = Some(Instant::now());
    }

    fn state_score(&self, key: &str) -> f64 {
        self.states
            .get(key)
            .map(|s| s.p_value_weighted_score)
            .unwrap_or(0.0)
    }

    fn should_log_ob_warn(&self, symbol: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_ob_warn
            .get(symbol)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    fn should_log_ticker_warn(&self, symbol: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_ticker_warn
            .get(symbol)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    fn should_log_position_warn(&self, key: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_position_warn
            .get(key)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    fn is_dust_position(
        &self,
        snapshot: &PositionSnapshot,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> bool {
        let Some(symbol_snapshot) = prices.get(&snapshot.symbol) else {
            return false;
        };
        let Some(min_order) = symbol_snapshot.min_order else {
            return false;
        };
        snapshot.size < min_order
    }

    fn is_ticker_auth_error(msg: &str) -> bool {
        let lower = msg.to_ascii_lowercase();
        lower.contains("403")
            || lower.contains("forbidden")
            || lower.contains("failed to deserialize response")
            || lower.contains("expected value at line 1 column 1")
    }

    fn is_reduce_only_position_missing_error(err: &DexError) -> bool {
        let msg = match err {
            DexError::ServerResponse(message) | DexError::Other(message) => message,
            _ => return false,
        };
        let lower = msg.to_ascii_lowercase();
        lower.contains("position is missing for reduce-only order")
            || lower.contains("position is missing for reduce only order")
    }

    async fn confirm_reduce_only_position_missing(&mut self, symbol: &str) -> bool {
        let cached_has_position = self
            .open_positions
            .get(symbol)
            .map(|p| p.sign != 0 && p.size > Decimal::ZERO)
            .unwrap_or(false);
        if !cached_has_position && self.positions_ready {
            return true;
        }

        match self.connector.get_positions().await {
            Ok(positions) => {
                let has_position = positions
                    .iter()
                    .any(|p| p.symbol == symbol && p.sign != 0 && p.size > Decimal::ZERO);
                if !has_position {
                    self.open_positions.remove(symbol);
                    return true;
                }
            }
            Err(err) => {
                log::warn!(
                    "[ORDER] reduce-only missing check failed for {}: {:?}",
                    symbol,
                    err
                );
            }
        }
        false
    }

    fn persist_history_to_disk(&self) {
        if self.cfg.disable_history_persist {
            return;
        }
        // Backtest replay re-drives this per tick, producing hundreds of
        // thousands of disk writes per run. That serialises a grid of
        // concurrent backtest processes on ext4 and leaves them wedged in
        // `Dl` state. The persisted file is only consumed by peer live bots
        // for A/B/C alignment, which is irrelevant under replay.
        if self.cfg.backtest_mode {
            return;
        }
        let mut snapshot: HashMap<String, Vec<(f64, i64)>> = HashMap::new();
        for (sym, deque) in &self.history {
            let v: Vec<(f64, i64)> = deque.iter().map(|p| (p.log_price, p.ts)).collect();
            snapshot.insert(sym.clone(), v);
        }
        if let Ok(json) = serde_json::to_string(&snapshot) {
            // Atomic write: tmpfile in the same directory + rename. Multiple
            // bots may be writing this shared file concurrently (pairtrade#4);
            // rename guarantees readers never observe a torn JSON document.
            let path = std::path::Path::new(&self.history_path);
            let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
            let file_name = path
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "pairtrade_history.json".to_string());
            let tmp = dir.join(format!(".{}.tmp.{}", file_name, std::process::id()));
            if let Err(e) = fs::write(&tmp, json) {
                log::debug!("persist history tmp write failed: {:?}", e);
                return;
            }
            if let Err(e) = fs::rename(&tmp, path) {
                log::debug!("persist history rename failed: {:?}", e);
                let _ = fs::remove_file(&tmp);
            }
        }
    }

    fn load_history_from_disk(&mut self) {
        if self.cfg.disable_history_persist {
            return;
        }
        // Skip persisted-history loading entirely under backtest replay: the
        // file's timestamps reflect the wall clock at dump time and would
        // always look stale relative to the replayed cursor, producing
        // millions of WARN lines without contributing anything useful (the
        // replay data already supplies a clean, gap-free history).
        if self.cfg.backtest_mode {
            return;
        }
        let path = &self.history_path;
        let Ok(content) = std::fs::read_to_string(path) else {
            return;
        };
        let parsed: Result<HashMap<String, Vec<(f64, i64)>>, _> = serde_json::from_str(&content);
        let Ok(map) = parsed else {
            return;
        };
        let now = self.current_now_ts();
        let max_age_secs =
            (self.max_history_len() as i64).saturating_mul(self.cfg.trading_period_secs as i64);
        // Stale-history guard (pairtrade#4): if the newest sample for a symbol
        // is older than a few bars, the persisted file is from a stopped bot
        // and replaying it would freeze a stale rolling window. Drop it and
        // let the live feed warm up from scratch.
        let stale_threshold_secs =
            (self.cfg.trading_period_secs as i64).saturating_mul(5).max(60);
        for (sym, entries) in map {
            let newest_ts = entries.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
            if now.saturating_sub(newest_ts) > stale_threshold_secs {
                log::warn!(
                    "discarding stale persisted history for {}: newest sample {}s old",
                    sym,
                    now.saturating_sub(newest_ts)
                );
                continue;
            }
            let mut deque = VecDeque::new();
            for (log_price, ts) in entries {
                if now.saturating_sub(ts) > max_age_secs {
                    continue;
                }
                deque.push_back(PriceSample { log_price, ts });
            }
            if !deque.is_empty() {
                self.history.insert(sym, deque);
            }
        }
    }

    /// Rebuild each pair's beta and spread_history from the shared on-disk
    /// price history so A/B/C bots have identical regression windows the
    /// instant they start, instead of waiting metrics_window live bars to
    /// converge (pairtrade#4). Computes beta directly from whatever bars
    /// are available — does not go through evaluate_pair() because that
    /// path enforces full lookback_hours_long under Strict warm-start and
    /// would skip the seed when the loaded history is shorter than the
    /// configured long window.
    fn warm_start_states_from_history(&mut self) {
        if self.cfg.disable_history_persist {
            return;
        }
        for pair in self.cfg.universe.clone() {
            let key = format!("{}/{}", pair.base, pair.quote);
            let (Some(hist_a), Some(hist_b)) =
                (self.history.get(&pair.base), self.history.get(&pair.quote))
            else { continue };
            let take = self.cfg.metrics_window.min(hist_a.len()).min(hist_b.len());
            if take < 2 { continue }
            let tail_a = tail_samples(hist_a, take);
            let tail_b = tail_samples(hist_b, take);
            let beta = regression_beta(&tail_b, &tail_a);
            let spreads: VecDeque<f64> = tail_a
                .iter()
                .zip(tail_b.iter())
                .map(|(sa, sb)| sa.log_price - beta * sb.log_price)
                .collect();
            let Some(state) = self.states.get_mut(&key) else { continue };
            state.beta = beta;
            state.beta_short = beta;
            state.beta_long = beta;
            state.last_spread = spreads.back().copied();
            state.spread_history = spreads;
            log::info!(
                "[WARM_START] {} seeded spread_history len={} beta={:.4}",
                key, state.spread_history.len(), state.beta
            );
        }
    }

    fn entry_vol_window(&self) -> usize {
        ((self.cfg.entry_vol_lookback_hours * 3600) / self.cfg.trading_period_secs).max(1) as usize
    }

    /// Virtual clock used by all duration-based decisions. In live mode this
    /// is the wall-clock UTC second; in backtest mode it tracks the replay
    /// connector's logical timestamp so cooldown / force_close /
    /// circuit_breaker / re-eval intervals fire correctly under replay.
    fn current_now_ts(&self) -> i64 {
        if self.cfg.backtest_mode {
            self.replay_connector
                .as_ref()
                .and_then(|r| r.current_timestamp_secs())
                .unwrap_or_else(|| chrono::Utc::now().timestamp())
        } else {
            chrono::Utc::now().timestamp()
        }
    }

    fn max_history_len(&self) -> usize {
        let mut max_needed = 0usize;
        // Consider all per-pair params and the default
        let all_params =
            std::iter::once(&self.cfg.default_pair_params).chain(self.cfg.pair_params.values());
        for pp in all_params {
            let max_hrs = pp.lookback_hours_long.max(pp.lookback_hours_short);
            let needed = (max_hrs * 3600 / self.cfg.trading_period_secs) as usize;
            let vol_needed = ((pp.entry_vol_lookback_hours * 3600) / self.cfg.trading_period_secs)
                .max(1) as usize;
            max_needed = max_needed.max(needed).max(vol_needed);
        }
        max_needed.max(self.cfg.metrics_window)
    }

    async fn reconcile_pending_orders(
        &mut self,
        key: &str,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Result<()> {
        let timeout = Duration::from_secs(self.cfg.order_timeout_secs.max(1));
        let now_ts = self.current_now_ts();
        let (pending_entry, pending_exit) = {
            let state = self
                .states
                .get_mut(key)
                .ok_or_else(|| anyhow!("missing state for {}", key))?;
            (state.pending_entry.take(), state.pending_exit.take())
        };

        if let Some(mut pending) = pending_entry {
            let status = self.pending_status(&pending).await?;
            self.update_pending_fills(&mut pending, &status.fills);
            let filled_qtys = self.filled_by_leg(&pending, &status.fills);
            if self.all_filled(&pending, &status.fills) {
                if let Some(state) = self.states.get_mut(key) {
                    let (mut ep_a, mut ep_b, mut es_a, mut es_b) = (None, None, None, None);
                    if let Some((base, quote)) = key.split_once('/') {
                        for leg in &pending.legs {
                            if leg.symbol == base {
                                ep_a = price_map.get(base).map(|s| s.price);
                                es_a = Some(leg.target);
                            } else if leg.symbol == quote {
                                ep_b = price_map.get(quote).map(|s| s.price);
                                es_b = Some(leg.target);
                            }
                        }
                    }
                    state.position = Some(Position {
                        direction: pending.direction,
                        entered_at: Instant::now(),
                        entered_ts: now_ts,
                        entry_price_a: ep_a,
                        entry_price_b: ep_b,
                        entry_size_a: es_a,
                        entry_size_b: es_b,
                    });
                    state.pending_entry = None;
                }
                log::info!("[ORDER] {} entry orders filled", key);
            } else if filled_qtys.values().any(|qty| *qty > Decimal::ZERO) {
                let next_retry = pending.hedge_retry_count.saturating_add(1);
                let max_retries = self.cfg.entry_partial_fill_max_retries;
                let use_market = max_retries > 0 && next_retry > max_retries;
                if use_market {
                    log::warn!(
                        "[ORDER] {} entry leg partially filled, retries exceeded ({} > {}); reissuing remaining legs as MARKET",
                        key,
                        next_retry,
                        max_retries
                    );
                } else if max_retries > 0 {
                    log::warn!(
                        "[ORDER] {} entry leg partially filled, reissuing remaining legs (retry {}/{})",
                        key,
                        next_retry,
                        max_retries
                    );
                } else {
                    log::warn!(
                        "[ORDER] {} entry leg partially filled, reissuing remaining legs",
                        key
                    );
                }
                self.cancel_pending_orders(&pending).await?;
                if let Some(new_pending) = self
                    .reissue_partial_legs(
                        &pending,
                        &filled_qtys,
                        price_map,
                        false,
                        use_market,
                        next_retry,
                    )
                    .await?
                {
                    if let Some(state) = self.states.get_mut(key) {
                        state.pending_entry = Some(new_pending);
                    }
                } else if let Some(state) = self.states.get_mut(key) {
                    state.pending_entry = None;
                }
                return Ok(());
            } else if pending.post_only_hybrid {
                let recon_pp = self.cfg.params_for(key);
                if recon_pp.entry_post_only_timeout_secs > 0
                    && pending.placed_at.elapsed()
                        >= Duration::from_secs(recon_pp.entry_post_only_timeout_secs)
                {
                    // Post-only entry timed out; cancel and reissue as taker
                    log::info!(
                        "[ORDER] {} post-only entry timeout ({}s), falling back to taker",
                        key,
                        recon_pp.entry_post_only_timeout_secs
                    );
                    self.cancel_pending_orders(&pending).await?;
                    let new_pending = self
                        .reissue_entry_as_taker(key, &pending, price_map)
                        .await?;
                    if let Some(state) = self.states.get_mut(key) {
                        state.pending_entry = new_pending;
                    }
                }
            } else if pending.placed_at.elapsed() >= timeout {
                // Partial fill or stuck orders; cancel and flatten any filled leg
                if status.open_remaining > 0 {
                    log::warn!(
                        "[ORDER] {} entry orders stale ({}s), cancelling {} legs",
                        key,
                        pending.placed_at.elapsed().as_secs(),
                        status.open_remaining
                    );
                    for leg in &pending.legs {
                        let filled = filled_qtys
                            .get(&leg.order_id)
                            .cloned()
                            .unwrap_or(Decimal::ZERO);
                        let is_open = status.open_ids.contains(&leg.order_id);
                        log::warn!(
                            "[ORDER] {} entry leg status symbol={} order_id={} target={} filled={} open={}",
                            key,
                            leg.symbol,
                            leg.order_id,
                            leg.target,
                            filled,
                            is_open
                        );
                    }
                    self.cancel_pending_orders(&pending).await?;
                }
                let filled_qtys = self.filled_by_leg(&pending, &status.fills);
                let mut flattened_any = false;
                let mut hedge_failed = false;
                let mut retry_count = pending.hedge_retry_count;
                let max_retries = 3u32;
                for leg in &pending.legs {
                    let filled = filled_qtys
                        .get(&leg.order_id)
                        .cloned()
                        .unwrap_or(Decimal::ZERO);
                    if filled > Decimal::ZERO {
                        if price_map.contains_key(&leg.symbol) {
                            let hedge_side = match leg.side {
                                dex_connector::OrderSide::Long => dex_connector::OrderSide::Short,
                                dex_connector::OrderSide::Short => dex_connector::OrderSide::Long,
                            };
                            let use_market = retry_count + 1 >= max_retries;
                            let limit = if use_market {
                                None
                            } else {
                                self.limit_price_for(&leg.symbol, hedge_side, price_map)
                            };
                            if !use_market && limit.is_none() {
                                log::warn!(
                                    "[ORDER] Missing reference price for hedge {} leg {}",
                                    leg.symbol,
                                    leg.order_id
                                );
                                hedge_failed = true;
                                continue;
                            }
                            let spread = self.order_spread_param(limit, false);
                            if let Err(e) = self
                                .connector
                                .create_order(
                                    &leg.symbol,
                                    filled,
                                    hedge_side,
                                    limit,
                                    spread,
                                    true,
                                    None,
                                )
                                .await
                            {
                                log::error!(
                                    "[ORDER] Failed to hedge partial entry {} ({}): {:?}",
                                    leg.symbol,
                                    leg.order_id,
                                    e
                                );
                                hedge_failed = true;
                            } else {
                                flattened_any = true;
                                let mode = if use_market { "MARKET" } else { "LIMIT" };
                                log::warn!(
                                    "[ORDER] Hedged partial entry on {} size={} mode={} retries={}",
                                    leg.symbol,
                                    filled,
                                    mode,
                                    retry_count
                                );
                            }
                        } else {
                            log::warn!(
                                "[ORDER] Missing price map entry for hedge {} leg {}",
                                leg.symbol,
                                leg.order_id
                            );
                            hedge_failed = true;
                        }
                    }
                }
                if let Some(state) = self.states.get_mut(key) {
                    if hedge_failed {
                        retry_count = retry_count.saturating_add(1);
                        pending.hedge_retry_count = retry_count;
                        log::warn!(
                            "[ORDER] Hedge retry scheduled for {} (retry {} of {})",
                            key,
                            retry_count,
                            max_retries
                        );
                        pending.placed_at = Instant::now();
                        state.pending_entry = Some(pending);
                    } else {
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                        state.pending_entry = None;
                        if flattened_any {
                            state.position = None;
                        }
                    }
                }
            } else if let Some(state) = self.states.get_mut(key) {
                state.pending_entry = Some(pending);
            }
        }

        if let Some(pending) = pending_exit {
            let status = self.pending_status(&pending).await?;
            let mut pending = pending;
            self.update_pending_fills(&mut pending, &status.fills);
            let filled_qtys = self.filled_by_leg(&pending, &status.fills);
            let mut pnl_record: Option<(PnlLogRecord, f64)> = None;
            if status.open_remaining == 0 && self.all_filled(&pending, &status.fills) {
                if let Some(state) = self.states.get_mut(key) {
                    if let Some(pos) = state.position.as_ref() {
                        if let Some((base, quote)) = key.split_once('/') {
                            if let (Some(p1), Some(p2)) =
                                (price_map.get(base), price_map.get(quote))
                            {
                                if let Some(pnl) =
                                    compute_pnl(pos, p1.price, p2.price).and_then(|p| p.to_f64())
                                {
                                    pnl_record = Some((
                                        PnlLogRecord::new(
                                            base,
                                            quote,
                                            pos.direction,
                                            pnl,
                                            Utc::now().timestamp(),
                                            "exit_fill",
                                        ),
                                        pnl,
                                    ));
                                }
                            }
                        }
                    }
                    state.position = None;
                    state.last_exit_at = Some(Instant::now());
                    state.last_exit_ts = Some(now_ts);
                    state.pending_exit = None;
                }
                log::info!("[ORDER] {} exit orders filled", key);
                if let Some((record, pnl_value)) = pnl_record {
                    self.write_pnl_record(record);
                    if pnl_value < 0.0 {
                        self.consecutive_losses += 1;
                        if let Some(cooldown) = self
                            .cfg
                            .circuit_breaker_cooldown_for(self.consecutive_losses)
                        {
                            self.circuit_breaker_until = Some(Instant::now() + cooldown);
                            self.circuit_breaker_until_ts =
                                Some(now_ts + cooldown.as_secs() as i64);
                            log::warn!(
                                "[CIRCUIT_BREAKER] activated after {} consecutive losses, cooldown {}s",
                                self.consecutive_losses, cooldown.as_secs()
                            );
                        }
                    } else if pnl_value > 0.0 {
                        if self.consecutive_losses > 0 {
                            log::info!(
                                "[CIRCUIT_BREAKER] reset after win (was {} consecutive losses)",
                                self.consecutive_losses
                            );
                        }
                        self.consecutive_losses = 0;
                        self.circuit_breaker_until = None;
                        self.circuit_breaker_until_ts = None;
                    }
                }
            } else if filled_qtys.values().any(|qty| *qty > Decimal::ZERO) {
                let next_retry = pending.hedge_retry_count.saturating_add(1);
                if next_retry > MAX_EXIT_RETRIES {
                    self.force_close_all_positions(key, "partial_fill").await;
                    if let Some(state) = self.states.get_mut(key) {
                        state.pending_exit = None;
                    }
                    return Ok(());
                }
                log::warn!(
                    "[ORDER] {} exit leg partially filled, reissuing remaining legs",
                    key
                );
                self.cancel_pending_orders(&pending).await?;
                if let Some(new_pending) = self
                    .reissue_partial_legs(&pending, &filled_qtys, price_map, true, true, next_retry)
                    .await?
                {
                    if let Some(state) = self.states.get_mut(key) {
                        state.pending_exit = Some(new_pending);
                    }
                } else if let Some(state) = self.states.get_mut(key) {
                    state.pending_exit = None;
                }
                return Ok(());
            } else if pending.placed_at.elapsed() >= timeout || status.open_remaining == 0 {
                let next_retry = pending.hedge_retry_count.saturating_add(1);
                if next_retry > MAX_EXIT_RETRIES {
                    self.force_close_all_positions(key, "timeout").await;
                    if let Some(state) = self.states.get_mut(key) {
                        state.pending_exit = None;
                    }
                    return Ok(());
                }
                if status.open_remaining > 0 {
                    log::warn!(
                        "[ORDER] {} exit orders stale ({}s), cancelling {} legs",
                        key,
                        pending.placed_at.elapsed().as_secs(),
                        status.open_remaining
                    );
                    for leg in &pending.legs {
                        let filled = filled_qtys
                            .get(&leg.order_id)
                            .cloned()
                            .unwrap_or(Decimal::ZERO);
                        let is_open = status.open_ids.contains(&leg.order_id);
                        log::warn!(
                            "[ORDER] {} exit leg status symbol={} order_id={} target={} filled={} open={}",
                            key,
                            leg.symbol,
                            leg.order_id,
                            leg.target,
                            filled,
                            is_open
                        );
                    }
                    self.cancel_pending_orders(&pending).await?;
                }
                // Re-attempt closing missing legs based on filled qty
                // reusing filled_qtys defined earlier
                let mut new_legs = Vec::new();
                for leg in &pending.legs {
                    let filled = filled_qtys
                        .get(&leg.order_id)
                        .cloned()
                        .unwrap_or(Decimal::ZERO);
                    let remaining_qty = (leg.target - filled).max(Decimal::ZERO);
                    if remaining_qty > Decimal::ZERO {
                        let quantized =
                            self.quantize_order_size_exit(&leg.symbol, remaining_qty, price_map);
                        if quantized <= Decimal::ZERO {
                            continue;
                        }
                        let limit = None;
                        match self
                            .connector
                            .create_order(&leg.symbol, quantized, leg.side, limit, None, true, None)
                            .await
                        {
                            Ok(resp) => {
                                new_legs.push(PendingLeg {
                                    symbol: leg.symbol.clone(),
                                    order_id: resp.order_id,
                                    exchange_order_id: resp.exchange_order_id,
                                    target: quantized,
                                    filled: Decimal::ZERO,
                                    side: leg.side,
                                    placed_price: resp.ordered_price,
                                });
                                log::warn!(
                                    "[ORDER] Retrying exit leg {} size={} mode=MARKET",
                                    leg.symbol,
                                    quantized
                                );
                            }
                            Err(e) => log::error!(
                                "[ORDER] Failed to retry exit leg {}: {:?}",
                                leg.symbol,
                                e
                            ),
                        }
                    }
                }
                if let Some(state) = self.states.get_mut(key) {
                    if new_legs.is_empty() {
                        state.pending_exit = None;
                        // Keep position state unchanged; will retry next loop
                    } else {
                        state.pending_exit = Some(PendingOrders {
                            legs: new_legs,
                            direction: pending.direction,
                            placed_at: Instant::now(),
                            hedge_retry_count: next_retry,
                            post_only_hybrid: false,
                        });
                    }
                }
            } else if let Some(state) = self.states.get_mut(key) {
                state.pending_exit = Some(pending);
            }
        }

        Ok(())
    }

    async fn cancel_pending_orders(&self, pending: &PendingOrders) -> Result<()> {
        let mut by_symbol: HashMap<String, Vec<String>> = HashMap::new();
        for leg in &pending.legs {
            by_symbol
                .entry(leg.symbol.clone())
                .or_default()
                .push(leg.order_id.clone());
        }
        for (symbol, order_ids) in by_symbol {
            if let Err(e) = self
                .connector
                .cancel_orders(Some(symbol.clone()), order_ids.clone())
                .await
            {
                log::error!(
                    "[ORDER] cancel failed for {} ({} ids): {:?}",
                    symbol,
                    order_ids.len(),
                    e
                );
            }
        }
        Ok(())
    }

    async fn pending_status(&self, pending: &PendingOrders) -> Result<PendingStatus> {
        let mut open_remaining = 0;
        let mut fills: HashMap<String, Decimal> = HashMap::new();
        let mut open_ids: HashSet<String> = HashSet::new();
        let mut per_symbol_open: HashMap<String, HashSet<String>> = HashMap::new();
        let mut per_symbol_fill: HashMap<String, HashSet<String>> = HashMap::new();
        for leg in &pending.legs {
            per_symbol_open
                .entry(leg.symbol.clone())
                .or_default()
                .insert(leg.order_id.clone());
            let fill_ids = per_symbol_fill.entry(leg.symbol.clone()).or_default();
            fill_ids.insert(leg.order_id.clone());
            if let Some(exchange_id) = &leg.exchange_order_id {
                fill_ids.insert(exchange_id.clone());
            }
        }
        for (symbol, open_ids_filter) in per_symbol_open.iter() {
            let fill_ids_filter = per_symbol_fill.get(symbol).cloned().unwrap_or_default();
            let open = self
                .connector
                .get_open_orders(symbol)
                .await
                .with_context(|| format!("open orders {}", symbol))?;
            let mut open_count = 0;
            for order in open
                .orders
                .iter()
                .filter(|o| open_ids_filter.contains(&o.order_id))
            {
                open_ids.insert(order.order_id.clone());
                open_count += 1;
            }
            open_remaining += open_count;

            let filled = self
                .connector
                .get_filled_orders(symbol)
                .await
                .with_context(|| format!("filled orders {}", symbol))?;
            for order in filled.orders {
                if fill_ids_filter.contains(&order.order_id) {
                    let sz = order.filled_size.unwrap_or(Decimal::ZERO);
                    *fills.entry(order.order_id.clone()).or_default() += sz;
                    log::debug!(
                        "[ORDER][FILLED] symbol={} order_id={} side={:?} size={} value={:?} fee={:?} trade_id={}",
                        symbol,
                        order.order_id,
                        order.filled_side,
                        sz,
                        order.filled_value,
                        order.filled_fee,
                        order.trade_id
                    );
                }
            }
            log::debug!(
                "[ORDER][PENDING_STATUS] symbol={} open_orders={} tracked_orders={} filled_entries={}",
                symbol,
                open_count,
                open_ids_filter.len(),
                fills.len()
            );
        }
        Ok(PendingStatus {
            open_remaining,
            fills,
            open_ids,
        })
    }

    fn leg_fill_from_map(&self, leg: &PendingLeg, fills: &HashMap<String, Decimal>) -> Decimal {
        fills
            .get(&leg.order_id)
            .cloned()
            .or_else(|| {
                leg.exchange_order_id
                    .as_ref()
                    .and_then(|id| fills.get(id).cloned())
            })
            .unwrap_or(Decimal::ZERO)
    }

    fn update_pending_fills(&self, pending: &mut PendingOrders, fills: &HashMap<String, Decimal>) {
        for leg in &mut pending.legs {
            let filled = self.leg_fill_from_map(leg, fills);
            if filled > leg.filled {
                leg.filled = filled.min(leg.target);
            }
        }
    }

    fn filled_for_leg(&self, leg: &PendingLeg, fills: &HashMap<String, Decimal>) -> Decimal {
        let filled = self.leg_fill_from_map(leg, fills);
        filled.max(leg.filled).min(leg.target)
    }

    fn filled_by_leg(
        &self,
        pending: &PendingOrders,
        fills: &HashMap<String, Decimal>,
    ) -> HashMap<String, Decimal> {
        let mut map = HashMap::new();
        for leg in &pending.legs {
            let filled = self.filled_for_leg(leg, fills);
            map.insert(leg.order_id.clone(), filled);
        }
        map
    }

    fn all_filled(&self, pending: &PendingOrders, fills: &HashMap<String, Decimal>) -> bool {
        pending
            .legs
            .iter()
            .all(|leg| self.filled_for_leg(leg, fills) >= leg.target)
    }

    fn evaluate_pair(&self, pair: &PairSpec) -> Option<PairEvaluation> {
        let key = format!("{}/{}", pair.base, pair.quote);
        let pp = self.cfg.params_for(&key);
        let hist_a = self.history.get(&pair.base)?;
        let hist_b = self.history.get(&pair.quote)?;
        let available = hist_a.len().min(hist_b.len());
        let desired_long =
            ((pp.lookback_hours_long * 3600) / self.cfg.trading_period_secs).max(1) as usize;
        let desired_short =
            ((pp.lookback_hours_short * 3600) / self.cfg.trading_period_secs).max(1) as usize;
        let (long_len, short_len) = match self.cfg.warm_start_mode {
            WarmStartMode::Strict => {
                if available < desired_long {
                    return None;
                }
                (desired_long, desired_short)
            }
            WarmStartMode::Relaxed => {
                let min_bars = pp.warm_start_min_bars.max(1);
                if available < min_bars {
                    return None;
                }
                let long_len = desired_long.min(available);
                let short_len = desired_short.min(long_len);
                (long_len, short_len)
            }
        };

        let tail_a = tail_samples(hist_a, long_len);
        let tail_b = tail_samples(hist_b, long_len);
        let beta_long = regression_beta(&tail_b, &tail_a);
        let beta_short = regression_beta(
            &tail_b[tail_b.len() - short_len..],
            &tail_a[tail_a.len() - short_len..],
        );
        let beta_eff = 0.7 * beta_short + 0.3 * beta_long;

        // Build spread series for diagnostics using long window
        let spreads: Vec<f64> = tail_a
            .iter()
            .zip(tail_b.iter())
            .map(|(sa, sb)| sa.log_price - beta_eff * sb.log_price)
            .collect();
        let (half_life_samples, adf_p_value) = half_life_and_p(&spreads);
        let half_life_hours = half_life_samples * (self.cfg.trading_period_secs as f64) / 3600.0;
        let beta_gap = ((beta_short - beta_long) / beta_eff.max(1e-6)).abs();
        let half_ok = half_life_hours <= pp.half_life_max_hours;
        let adf_ok = adf_p_value <= pp.adf_p_threshold;
        let beta_ok = beta_gap <= 0.2;
        let score = half_ok as u8 + adf_ok as u8 + beta_ok as u8;
        let eligible = score >= 2;
        // softer ranking: weight lower p and faster half-life
        let continuous_score =
            (1.0 - adf_p_value.min(1.0)) * 0.6 + (1.0 / (1.0 + half_life_hours)) * 0.4;

        Some(PairEvaluation {
            beta_short,
            beta_long,
            beta_eff,
            half_life_hours,
            adf_p_value,
            eligible,
            score: continuous_score,
            beta_gap,
        })
    }

    fn exit_sizes_for_pair(
        &self,
        key: &str,
        pair: &PairSpec,
        beta: f64,
        p1: &SymbolSnapshot,
        p2: &SymbolSnapshot,
    ) -> Result<(Decimal, Decimal)> {
        let base_snapshot = self.open_positions.get(&pair.base);
        let quote_snapshot = self.open_positions.get(&pair.quote);
        if base_snapshot.is_some() || quote_snapshot.is_some() {
            let qty_a = base_snapshot.map(|p| p.size).unwrap_or(Decimal::ZERO);
            let qty_b = quote_snapshot.map(|p| p.size).unwrap_or(Decimal::ZERO);
            return Ok((qty_a, qty_b));
        }

        let mut qty_a = Decimal::ZERO;
        let mut qty_b = Decimal::ZERO;
        if let Some(state) = self.states.get(key).and_then(|s| s.position.as_ref()) {
            qty_a = state.entry_size_a.unwrap_or(Decimal::ZERO);
            qty_b = state.entry_size_b.unwrap_or(Decimal::ZERO);
        }

        if qty_a <= Decimal::ZERO && qty_b <= Decimal::ZERO {
            log::warn!(
                "[EXIT] {} missing position sizes from exchange/state; falling back to hedge sizing",
                key
            );
            return self.hedged_sizes(pair, beta, p1, p2);
        }

        Ok((qty_a, qty_b))
    }

    fn hedged_sizes(
        &self,
        _pair: &PairSpec,
        beta: f64,
        p1: &SymbolSnapshot,
        p2: &SymbolSnapshot,
    ) -> Result<(Decimal, Decimal)> {
        let total_risk = self.equity_cache.max(self.cfg.equity_usd)
            * self.cfg.risk_pct_per_trade
            * self.cfg.max_leverage;
        let leg_notional = (total_risk / 2.0).max(10.0);
        let notional =
            Decimal::from_f64(leg_notional).ok_or_else(|| anyhow!("invalid notional"))?;

        let qty_a = if p1.price == Decimal::ZERO {
            Decimal::ZERO
        } else {
            let mut qty = notional / p1.price;
            if let Some(decimals) = p1.size_decimals {
                qty = qty.round_dp(decimals);
            }
            if let Some(min_ord) = p1.min_order {
                if qty > Decimal::ZERO && qty < min_ord {
                    qty = min_ord;
                }
            }
            qty
        };
        // Compute qty_b from the actual notional of leg A (after min_order adjustment)
        // so that the hedge ratio matches beta: notional_b = notional_a * beta
        let actual_notional_a = qty_a * p1.price;
        let qty_b = if p2.price == Decimal::ZERO {
            Decimal::ZERO
        } else {
            let beta_dec = Decimal::from_f64(beta.abs()).unwrap_or(Decimal::ONE);
            let mut qty = (actual_notional_a * beta_dec) / p2.price;
            if let Some(decimals) = p2.size_decimals {
                qty = qty.round_dp(decimals);
            }
            if let Some(min_ord) = p2.min_order {
                if qty > Decimal::ZERO && qty < min_ord {
                    qty = min_ord;
                }
            }
            qty
        };
        Ok((qty_a, qty_b))
    }

    fn post_only_supported(&self) -> bool {
        let dex = self.cfg.dex_name.to_ascii_lowercase();
        dex.contains("lighter")
    }

    fn should_post_only(&self) -> bool {
        self.cfg.fee_bps > 0.0 && self.post_only_supported()
    }

    fn order_reference_price_from_snapshot(
        &self,
        symbol: &str,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> Decimal {
        let use_book = self.cfg.slippage_bps < 0 || self.should_post_only();
        if use_book {
            let side_price = match side {
                dex_connector::OrderSide::Long => snapshot.ask_price,
                dex_connector::OrderSide::Short => snapshot.bid_price,
            };
            if side_price.is_none() {
                log::debug!(
                    "[ORDER] {} missing top-of-book price; using ticker price",
                    symbol
                );
            }
            return side_price.unwrap_or(snapshot.price);
        }
        snapshot.price
    }

    fn order_reference_price(
        &self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        let snapshot = prices.get(symbol)?;
        Some(self.order_reference_price_from_snapshot(symbol, side, snapshot))
    }

    fn limit_price_for(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        let snapshot = prices.get(symbol)?;
        let reference = self.order_reference_price_from_snapshot(symbol, side, snapshot);
        let adjusted = self.apply_slippage(Some(reference), side)?;
        Some(self.quantize_order_price_with_snapshot(symbol, adjusted, side, snapshot))
    }

    fn limit_price_for_snapshot(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> Option<Decimal> {
        let reference = self.order_reference_price_from_snapshot(symbol, side, snapshot);
        let adjusted = self.apply_slippage(Some(reference), side)?;
        Some(self.quantize_order_price_with_snapshot(symbol, adjusted, side, snapshot))
    }

    async fn refreshed_limit_price(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        match self.refresh_symbol_snapshot(symbol).await {
            Ok(snapshot) => self.limit_price_for_snapshot(symbol, side, &snapshot),
            Err(err) => {
                log::debug!(
                    "[ORDER] Failed to refresh price snapshot for {}: {:?}",
                    symbol,
                    err
                );
                self.limit_price_for(symbol, side, prices)
            }
        }
    }

    async fn refresh_symbol_snapshot(&mut self, symbol: &str) -> Result<SymbolSnapshot> {
        let ticker = self
            .connector
            .get_ticker(symbol, None)
            .await
            .with_context(|| format!("ticker {}", symbol))?;
        let (bid_price, ask_price, bid_size, ask_size) =
            match self.connector.get_order_book(symbol, 1).await {
                Ok(ob) => (
                    ob.bids.first().map(|l| l.price),
                    ob.asks.first().map(|l| l.price),
                    ob.bids.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                    ob.asks.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                ),
                Err(err) => {
                    log::debug!(
                        "[ORDER] orderbook {} unavailable during retry: {:?}",
                        symbol,
                        err
                    );
                    (None, None, Decimal::ZERO, Decimal::ZERO)
                }
            };
        Ok(SymbolSnapshot {
            price: ticker.price,
            funding_rate: ticker.funding_rate.unwrap_or(Decimal::ZERO),
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            min_order: ticker.min_order,
            min_tick: ticker.min_tick,
            size_decimals: ticker.size_decimals,
            exchange_ts: ticker.exchange_ts.map(|v| v as i64),
        })
    }

    fn order_spread_param(&self, limit: Option<Decimal>, allow_post_only: bool) -> Option<i64> {
        if allow_post_only && limit.is_some() && self.should_post_only() {
            Some(-2)
        } else {
            None
        }
    }

    fn apply_slippage(
        &self,
        price: Option<Decimal>,
        side: dex_connector::OrderSide,
    ) -> Option<Decimal> {
        let bps = self.cfg.slippage_bps;
        let p = price?;
        if bps == 0 {
            return Some(p);
        }
        let factor = Decimal::from_f64((bps.abs() as f64) / 10_000.0).unwrap_or(Decimal::ZERO);
        let passive = bps < 0;
        match side {
            dex_connector::OrderSide::Long => {
                if passive {
                    Some(p * (Decimal::ONE - factor))
                } else {
                    Some(p * (Decimal::ONE + factor))
                }
            }
            dex_connector::OrderSide::Short => {
                if passive {
                    Some(p * (Decimal::ONE + factor))
                } else {
                    Some(p * (Decimal::ONE - factor))
                }
            }
        }
    }

    fn quantize_order_size(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        if size <= Decimal::ZERO {
            return size;
        }
        if let Some(snapshot) = prices.get(symbol) {
            let min_order = snapshot.min_order.clone();
            let step = min_order
                .clone()
                .or_else(|| snapshot.size_decimals.map(|d| Decimal::new(1, d.min(28))));
            if let Some(step) = step {
                let quantized = quantize_size_by_step(size, step, min_order);
                if quantized > Decimal::ZERO {
                    return quantized;
                }
            }
        }
        size
    }

    fn quantize_order_size_exit(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        if size <= Decimal::ZERO {
            return size;
        }
        if let Some(snapshot) = prices.get(symbol) {
            let min_order = snapshot.min_order.clone();
            let step = min_order
                .clone()
                .or_else(|| snapshot.size_decimals.map(|d| Decimal::new(1, d.min(28))));
            if let Some(step) = step {
                let quantized = quantize_size_by_step_ceiling(size, step, min_order);
                if quantized > Decimal::ZERO {
                    return quantized;
                }
            }
        }
        size
    }

    fn quantize_order_size_close(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        if size <= Decimal::ZERO {
            return size;
        }
        if let Some(snapshot) = prices.get(symbol) {
            let step = snapshot
                .size_decimals
                .map(|d| Decimal::new(1, d.min(28)))
                .or_else(|| snapshot.min_order.clone());
            if let Some(step) = step {
                let quantized = quantize_size_by_step_ceiling(size, step, None);
                if quantized > Decimal::ZERO {
                    return quantized;
                }
            }
        }
        size
    }

    fn quantize_order_price_with_snapshot(
        &mut self,
        symbol: &str,
        price: Decimal,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> Decimal {
        let effective_tick_size = snapshot.min_tick;

        let Some(tick_size) = effective_tick_size else {
            if !self.min_tick_warned.contains(symbol) {
                log::warn!(
                    "[ORDER] No min tick for {}; price rounding disabled",
                    symbol
                );

                self.min_tick_warned.insert(symbol.to_string());
            }

            return price;
        };

        if tick_size <= Decimal::ZERO {
            return price;
        }

        round_price_by_tick(price, tick_size, side)
    }

    async fn create_order_with_post_only_retry(
        &mut self,
        symbol: &str,
        size: Decimal,
        side: dex_connector::OrderSide,
        reduce_only: bool,
        prices: &HashMap<String, SymbolSnapshot>,
        allow_post_only: bool,
        max_post_only_attempts: usize,
        fallback_to_taker: bool,
    ) -> Result<dex_connector::CreateOrderResponse, DexError> {
        let use_post_only = allow_post_only && self.should_post_only();
        let max_attempts = max_post_only_attempts.max(1);
        let max_elapsed = Duration::from_millis(POST_ONLY_RETRY_MAX_ELAPSED_MS);
        let start = Instant::now();
        let mut attempt = 0usize;

        let last_err = loop {
            attempt += 1;
            let limit = if use_post_only {
                self.refreshed_limit_price(symbol, side, prices).await
            } else {
                self.limit_price_for(symbol, side, prices)
            };
            if use_post_only && limit.is_none() {
                return Err(DexError::Other(format!(
                    "[ORDER] Missing reference price for post-only {}",
                    symbol
                )));
            }
            let spread = self.order_spread_param(limit, use_post_only);
            match self
                .connector
                .create_order(symbol, size, side, limit, spread, reduce_only, None)
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    if !use_post_only {
                        return Err(err);
                    }
                    if attempt >= max_attempts || start.elapsed() >= max_elapsed {
                        break err;
                    }
                }
            }

            log::info!(
                "[ORDER] {} post-only attempt {} failed; retrying",
                symbol,
                attempt
            );
            sleep(Duration::from_millis(POST_ONLY_RETRY_DELAY_MS)).await;
        };

        if use_post_only && fallback_to_taker {
            log::warn!(
                "[ORDER] {} post-only attempts exhausted; falling back to taker",
                symbol
            );
            return self
                .connector
                .create_order(symbol, size, side, None, None, reduce_only, None)
                .await;
        }

        Err(last_err)
    }

    async fn place_pair_orders(
        &mut self,
        pair: &PairSpec,
        direction: PositionDirection,
        qtys: (Decimal, Decimal),
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Result<Vec<PendingLeg>> {
        let (side_a, side_b) = match direction {
            PositionDirection::LongSpread => (
                dex_connector::OrderSide::Long,
                dex_connector::OrderSide::Short,
            ),
            PositionDirection::ShortSpread => (
                dex_connector::OrderSide::Short,
                dex_connector::OrderSide::Long,
            ),
        };
        let ref_price_a = self.order_reference_price(&pair.base, side_a, prices);
        let ref_price_b = self.order_reference_price(&pair.quote, side_b, prices);
        let qty_a = self.quantize_order_size_exit(&pair.base, qtys.0, prices);
        let qty_b = self.quantize_order_size_exit(&pair.quote, qtys.1, prices);
        if qty_a != qtys.0 {
            log::debug!(
                "[ORDER_ADJUST][ENTRY] {} settled qty_a {} -> {}",
                pair.base,
                qtys.0,
                qty_a
            );
        }
        if qty_b != qtys.1 {
            log::debug!(
                "[ORDER_ADJUST][ENTRY] {} settled qty_b {} -> {}",
                pair.quote,
                qtys.1,
                qty_b
            );
        }
        // Check hedge ratio deviation after size rounding
        let pair_key_for_dev = format!("{}/{}", pair.base, pair.quote);
        let pp_for_dev = self.cfg.params_for(&pair_key_for_dev);
        if pp_for_dev.hedge_ratio_max_deviation < 1.0 {
            let dev_a = if qtys.0.is_zero() {
                0.0
            } else {
                ((qty_a / qtys.0) - Decimal::ONE).abs().to_f64().unwrap_or(0.0)
            };
            let dev_b = if qtys.1.is_zero() {
                0.0
            } else {
                ((qty_b / qtys.1) - Decimal::ONE).abs().to_f64().unwrap_or(0.0)
            };
            let max_dev = dev_a.max(dev_b);
            if max_dev > pp_for_dev.hedge_ratio_max_deviation {
                log::info!(
                    "[ORDER_ADJUST][ENTRY] {}/{} BLOCKED: size rounding deviation {:.1}% exceeds limit {:.1}%",
                    pair.base, pair.quote, max_dev * 100.0, pp_for_dev.hedge_ratio_max_deviation * 100.0
                );
                return Ok(Vec::new());
            }
        }
        let limit_a = self.limit_price_for(&pair.base, side_a, prices);
        let limit_b = self.limit_price_for(&pair.quote, side_b, prices);
        let pair_key_for_hybrid = format!("{}/{}", pair.base, pair.quote);
        let pp_for_hybrid = self.cfg.params_for(&pair_key_for_hybrid);
        let hybrid_active =
            pp_for_hybrid.entry_post_only_timeout_secs > 0 && self.post_only_supported();
        let post_only = self.should_post_only();
        let entry_attempts = if hybrid_active {
            1
        } else {
            POST_ONLY_ENTRY_ATTEMPTS
        };
        log::debug!(
            "[ORDER_PARAMS][ENTRY] pair={}/{} side_a={:?} qty_a={} ref_price_a={} limit_a={:?} side_b={:?} qty_b={} ref_price_b={} limit_b={:?} post_only={} hybrid={}",
            pair.base,
            pair.quote,
            side_a,
            qty_a,
            ref_price_a.unwrap_or(Decimal::ZERO),
            limit_a,
            side_b,
            qty_b,
            ref_price_b.unwrap_or(Decimal::ZERO),
            limit_b,
            post_only,
            hybrid_active
        );
        let mut legs: Vec<PendingLeg> = Vec::new();
        let res_a = self
            .create_order_with_post_only_retry(
                &pair.base,
                qty_a,
                side_a,
                false,
                prices,
                true,
                entry_attempts,
                false,
            )
            .await
            .context("place leg A")?;
        let target_a = if res_a.ordered_size > Decimal::ZERO {
            if res_a.ordered_size != qtys.0 {
                log::debug!(
                    "[ORDER_PARAMS][ENTRY] size adjusted by exchange for {}: requested={} ordered={}",
                    pair.base,
                    qtys.0,
                    res_a.ordered_size
                );
            }
            res_a.ordered_size
        } else {
            qtys.0
        };
        legs.push(PendingLeg {
            symbol: pair.base.clone(),
            order_id: res_a.order_id.clone(),
            exchange_order_id: res_a.exchange_order_id.clone(),
            target: target_a,
            filled: Decimal::ZERO,
            side: side_a,
            placed_price: res_a.ordered_price,
        });

        let res_b = match self
            .create_order_with_post_only_retry(
                &pair.quote,
                qty_b,
                side_b,
                false,
                prices,
                true,
                entry_attempts,
                false,
            )
            .await
        {
            Ok(res) => res,
            Err(e) => {
                log::error!(
                    "[ORDER] Failed to place leg B for {}/{} (leg A={}): {:?}",
                    pair.base,
                    pair.quote,
                    res_a.order_id,
                    e
                );

                // Attempt to cancel leg A, but proceed even if it fails
                if let Err(cancel_err) = self
                    .connector
                    .cancel_order(&pair.base, &res_a.order_id)
                    .await
                {
                    log::warn!(
                        "[SAFETY] Failed to cancel leg A {} after leg B failed: {:?}",
                        res_a.order_id,
                        cancel_err
                    );
                } else {
                    log::info!(
                        "[SAFETY] Canceled leg A {} after leg B failed.",
                        res_a.order_id
                    );
                }

                // Give some time for the fill to be processed by the exchange
                sleep(Duration::from_secs(5)).await;

                // Check if leg A was filled despite the cancellation attempt
                match self.connector.get_filled_orders(&pair.base).await {
                    Ok(filled_orders) => {
                        let matches_order = |order_id: &str| {
                            order_id == res_a.order_id
                                || res_a
                                    .exchange_order_id
                                    .as_ref()
                                    .map_or(false, |id| order_id == id)
                        };
                        if let Some(filled_order) = filled_orders
                            .orders
                            .iter()
                            .find(|o| matches_order(&o.order_id))
                        {
                            let filled_size = filled_order.filled_size.unwrap_or(Decimal::ZERO);
                            if filled_size > Decimal::ZERO {
                                log::warn!(
                                    "[SAFETY] Leg A {} was filled for {}. Hedging immediately.",
                                    res_a.order_id,
                                    pair.base
                                );
                                let hedge_side = match side_a {
                                    dex_connector::OrderSide::Long => {
                                        dex_connector::OrderSide::Short
                                    }
                                    dex_connector::OrderSide::Short => {
                                        dex_connector::OrderSide::Long
                                    }
                                };

                                // Create a market order to close the partial position
                                if let Err(hedge_err) = self
                                    .connector
                                    .create_order(
                                        &pair.base,
                                        filled_size,
                                        hedge_side,
                                        None,
                                        None,
                                        true,
                                        None,
                                    )
                                    .await
                                {
                                    log::error!(
                                        "[SAFETY] FAILED TO HEDGE partial fill for {}: {:?}",
                                        pair.base,
                                        hedge_err
                                    );
                                } else {
                                    log::info!("[SAFETY] Successfully submitted hedge order for partial fill on {}", pair.base);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "[SAFETY] Could not check for filled orders for {}: {:?}",
                            pair.base,
                            e
                        );
                    }
                }

                return Err(PartialOrderPlacementError::new(legs.clone(), e).into());
            }
        };
        let target_b = if res_b.ordered_size > Decimal::ZERO {
            if res_b.ordered_size != qtys.1 {
                log::debug!(
                    "[ORDER_PARAMS][ENTRY] size adjusted by exchange for {}: requested={} ordered={}",
                    pair.quote,
                    qtys.1,
                    res_b.ordered_size
                );
            }
            res_b.ordered_size
        } else {
            qtys.1
        };
        legs.push(PendingLeg {
            symbol: pair.quote.clone(),
            order_id: res_b.order_id.clone(),
            exchange_order_id: res_b.exchange_order_id.clone(),
            target: target_b,
            filled: Decimal::ZERO,
            side: side_b,
            placed_price: res_b.ordered_price,
        });
        Ok(legs)
    }

    async fn close_pair_orders(
        &mut self,
        pair: &PairSpec,
        direction: PositionDirection,
        qtys: (Decimal, Decimal),
        prices: &HashMap<String, SymbolSnapshot>,
        use_market: bool,
    ) -> Result<Vec<PendingLeg>> {
        let (side_a, side_b) = match direction {
            PositionDirection::LongSpread => (
                dex_connector::OrderSide::Short,
                dex_connector::OrderSide::Long,
            ),
            PositionDirection::ShortSpread => (
                dex_connector::OrderSide::Long,
                dex_connector::OrderSide::Short,
            ),
        };
        let ref_price_a = self.order_reference_price(&pair.base, side_a, prices);
        let ref_price_b = self.order_reference_price(&pair.quote, side_b, prices);
        let qty_a = self.quantize_order_size_close(&pair.base, qtys.0, prices);
        let qty_b = self.quantize_order_size_close(&pair.quote, qtys.1, prices);
        if qty_a != qtys.0 {
            log::debug!(
                "[ORDER_ADJUST][EXIT] {} settled qty_a {} -> {}",
                pair.base,
                qtys.0,
                qty_a
            );
        }
        if qty_b != qtys.1 {
            log::debug!(
                "[ORDER_ADJUST][EXIT] {} settled qty_b {} -> {}",
                pair.quote,
                qtys.1,
                qty_b
            );
        }
        let limit_a = if use_market {
            None
        } else {
            self.limit_price_for(&pair.base, side_a, prices)
        };
        let limit_b = if use_market {
            None
        } else {
            self.limit_price_for(&pair.quote, side_b, prices)
        };
        let post_only = !use_market && self.should_post_only();
        log::debug!(
            "[ORDER_PARAMS][EXIT] pair={}/{} side_a={:?} qty_a={} ref_price_a={} limit_a={:?} side_b={:?} qty_b={} ref_price_b={} limit_b={:?} post_only={}",
            pair.base,
            pair.quote,
            side_a,
            qty_a,
            ref_price_a.unwrap_or(Decimal::ZERO),
            limit_a,
            side_b,
            qty_b,
            ref_price_b.unwrap_or(Decimal::ZERO),
            limit_b,
            post_only
        );
        let mut legs: Vec<PendingLeg> = Vec::new();
        let mut res_a = None;
        if qty_a > Decimal::ZERO {
            let res = if use_market {
                self.connector
                    .create_order(&pair.base, qty_a, side_a, None, None, true, None)
                    .await
            } else {
                self.create_order_with_post_only_retry(
                    &pair.base,
                    qty_a,
                    side_a,
                    true,
                    prices,
                    true,
                    POST_ONLY_EXIT_ATTEMPTS,
                    true,
                )
                .await
            };
            match res {
                Ok(res) => {
                    if res.ordered_size > Decimal::ZERO && res.ordered_size != qty_a {
                        log::debug!(
                            "[ORDER_PARAMS][EXIT] size adjusted by exchange for {}: requested={} ordered={}",
                            pair.base,
                            qty_a,
                            res.ordered_size
                        );
                    }
                    legs.push(PendingLeg {
                        symbol: pair.base.clone(),
                        order_id: res.order_id.clone(),
                        exchange_order_id: res.exchange_order_id.clone(),
                        target: qty_a,
                        filled: Decimal::ZERO,
                        side: side_a,
                        placed_price: res.ordered_price,
                    });
                    res_a = Some(res);
                }
                Err(err) => {
                    if Self::is_reduce_only_position_missing_error(&err) {
                        let symbol = pair.base.clone();
                        if self.confirm_reduce_only_position_missing(&symbol).await {
                            log::info!(
                                "[ORDER] {} reduce-only close skipped; position already closed",
                                symbol
                            );
                        } else {
                            return Err(err).context("close leg A");
                        }
                    } else {
                        return Err(err).context("close leg A");
                    }
                }
            }
        }

        if qty_b > Decimal::ZERO {
            let res_b = if use_market {
                self.connector
                    .create_order(&pair.quote, qty_b, side_b, None, None, true, None)
                    .await
            } else {
                self.create_order_with_post_only_retry(
                    &pair.quote,
                    qty_b,
                    side_b,
                    true,
                    prices,
                    true,
                    POST_ONLY_EXIT_ATTEMPTS,
                    true,
                )
                .await
            };
            let res_b = match res_b {
                Ok(res) => Some(res),
                Err(e) => {
                    let mut skip = false;
                    if Self::is_reduce_only_position_missing_error(&e) {
                        let symbol = pair.quote.clone();
                        if self.confirm_reduce_only_position_missing(&symbol).await {
                            log::info!(
                                "[ORDER] {} reduce-only close skipped; position already closed",
                                symbol
                            );
                            skip = true;
                        }
                    }
                    if skip {
                        None
                    } else {
                        if let Some(ref res_a) = res_a {
                            log::error!(
                                "[ORDER] Failed to close leg B for {}/{} (leg A={}): {:?}",
                                pair.base,
                                pair.quote,
                                res_a.order_id,
                                e
                            );

                            // Attempt to cancel leg A, but proceed even if it fails
                            if let Err(cancel_err) = self
                                .connector
                                .cancel_order(&pair.base, &res_a.order_id)
                                .await
                            {
                                log::warn!(
                                    "[SAFETY] Failed to cancel leg A {} after leg B failed: {:?}",
                                    res_a.order_id,
                                    cancel_err
                                );
                            } else {
                                log::info!(
                                    "[SAFETY] Canceled leg A {} after leg B failed.",
                                    res_a.order_id
                                );
                            }

                            // Give some time for the fill to be processed by the exchange
                            sleep(Duration::from_secs(5)).await;

                            // Check if leg A was filled despite the cancellation attempt
                            match self.connector.get_filled_orders(&pair.base).await {
                                Ok(filled_orders) => {
                                    let matches_order = |order_id: &str| {
                                        order_id == res_a.order_id
                                            || res_a
                                                .exchange_order_id
                                                .as_ref()
                                                .map_or(false, |id| order_id == id)
                                    };
                                    if let Some(filled_order) = filled_orders
                                        .orders
                                        .iter()
                                        .find(|o| matches_order(&o.order_id))
                                    {
                                        let filled_size =
                                            filled_order.filled_size.unwrap_or(Decimal::ZERO);
                                        if filled_size > Decimal::ZERO {
                                            log::warn!(
                                                "[SAFETY] Leg A {} was filled for {}. Hedging immediately.",
                                                res_a.order_id,
                                                pair.base
                                            );
                                            let hedge_side = match side_a {
                                                dex_connector::OrderSide::Long => {
                                                    dex_connector::OrderSide::Short
                                                }
                                                dex_connector::OrderSide::Short => {
                                                    dex_connector::OrderSide::Long
                                                }
                                            };

                                            // Create a market order to close the partial position
                                            if let Err(hedge_err) = self
                                                .connector
                                                .create_order(
                                                    &pair.base,
                                                    filled_size,
                                                    hedge_side,
                                                    None,
                                                    None,
                                                    true,
                                                    None,
                                                )
                                                .await
                                            {
                                                log::error!(
                                                    "[SAFETY] FAILED TO HEDGE partial fill for {}: {:?}",
                                                    pair.base,
                                                    hedge_err
                                                );
                                            } else {
                                                log::info!("[SAFETY] Successfully submitted hedge order for partial fill on {}", pair.base);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::error!(
                                        "[SAFETY] Could not check for filled orders for {}: {:?}",
                                        pair.base,
                                        e
                                    );
                                }
                            }
                        } else {
                            log::error!(
                                "[ORDER] Failed to close leg B for {}/{}: {:?}",
                                pair.base,
                                pair.quote,
                                e
                            );
                        }

                        return Err(PartialOrderPlacementError::new(legs.clone(), e).into());
                    }
                }
            };
            if let Some(res_b) = res_b {
                if res_b.ordered_size > Decimal::ZERO && res_b.ordered_size != qty_b {
                    log::debug!(
                        "[ORDER_PARAMS][EXIT] size adjusted by exchange for {}: requested={} ordered={}",
                        pair.quote,
                        qty_b,
                        res_b.ordered_size
                    );
                }
                legs.push(PendingLeg {
                    symbol: pair.quote.clone(),
                    order_id: res_b.order_id.clone(),
                    exchange_order_id: res_b.exchange_order_id.clone(),
                    target: qty_b,
                    filled: Decimal::ZERO,
                    side: side_b,
                    placed_price: res_b.ordered_price,
                });
            }
        }

        if legs.is_empty() {
            log::warn!(
                "[ORDER] No exit legs placed for {}/{} (qty_a={}, qty_b={})",
                pair.base,
                pair.quote,
                qty_a,
                qty_b
            );
        }
        Ok(legs)
    }

    fn register_partial_leg_failure(
        &mut self,
        key: &str,
        direction: PositionDirection,
        err: &anyhow::Error,
        is_exit: bool,
    ) {
        if let Some(partial) = err.downcast_ref::<PartialOrderPlacementError>() {
            if let Some(state) = self.states.get_mut(key) {
                let pending = PendingOrders {
                    legs: partial.legs().to_vec(),
                    direction,
                    placed_at: Instant::now(),
                    hedge_retry_count: 0,
                    post_only_hybrid: false,
                };
                if is_exit {
                    state.pending_exit = Some(pending);
                } else {
                    state.pending_entry = Some(pending);
                }
            }
        }
    }

    async fn fetch_latest_prices(&mut self) -> Result<HashMap<String, SymbolSnapshot>> {
        let mut map = HashMap::new();
        for symbol in self
            .cfg
            .universe
            .iter()
            .flat_map(|p| [p.base.clone(), p.quote.clone()])
        {
            if map.contains_key(&symbol) {
                continue;
            }
            let ticker = match self.connector.get_ticker(&symbol, None).await {
                Ok(ticker) => ticker,
                Err(e) => {
                    let msg = e.to_string();
                    if Self::is_ticker_auth_error(&msg) {
                        if self.should_log_ticker_warn(&symbol) {
                            log::warn!("ticker {} unavailable: {}", symbol, msg);
                            self.last_ticker_warn.insert(symbol.clone(), Instant::now());
                        } else {
                            log::debug!("ticker {} unavailable: {}", symbol, msg);
                        }
                        continue;
                    }
                    return Err(e).with_context(|| format!("ticker {}", symbol));
                }
            };
            let (top_bid_price, top_ask_price, top_bid_size, top_ask_size) =
                match self.connector.get_order_book(&symbol, 1).await {
                    Ok(ob) => (
                        ob.bids.first().map(|l| l.price),
                        ob.asks.first().map(|l| l.price),
                        ob.bids.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                        ob.asks.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                    ),
                    Err(e) => {
                        let msg = format!("{:?}", e);
                        let is_stale = msg.contains("order book snapshot unavailable");
                        if is_stale {
                            log::debug!("orderbook {} unavailable: {}", symbol, msg);
                        } else if self.should_log_ob_warn(&symbol) {
                            log::warn!("orderbook {} unavailable: {}", symbol, msg);
                            self.last_ob_warn.insert(symbol.clone(), Instant::now());
                        } else {
                            log::debug!("orderbook {} unavailable: {}", symbol, msg);
                        }
                        (None, None, Decimal::ZERO, Decimal::ZERO)
                    }
                };
            if ticker.min_order.is_none() && !self.min_order_warned.contains(&symbol) {
                let size_decimals_desc = ticker
                    .size_decimals
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "none".into());
                log::warn!(
                    "[TICKER] {} missing min_order (size_decimals={}); using fallback step",
                    symbol,
                    size_decimals_desc
                );
                self.min_order_warned.insert(symbol.clone());
            }
            if ticker.min_tick.is_none() && !self.min_tick_warned.contains(&symbol) {
                let min_tick_desc = ticker
                    .min_tick
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "none".into());
                log::warn!(
                    "[TICKER] {} missing min_tick (ticker reports {}); price will be rounded with fallback",
                    symbol,
                    min_tick_desc
                );
                self.min_tick_warned.insert(symbol.clone());
            }
            map.insert(
                symbol.clone(),
                SymbolSnapshot {
                    price: ticker.price,
                    funding_rate: ticker.funding_rate.unwrap_or(Decimal::ZERO),
                    bid_price: top_bid_price,
                    ask_price: top_ask_price,
                    bid_size: top_bid_size,
                    ask_size: top_ask_size,
                    min_order: ticker.min_order,
                    min_tick: ticker.min_tick,
                    size_decimals: ticker.size_decimals,
                    exchange_ts: ticker.exchange_ts.map(|v| v as i64),
                },
            );
            log::debug!(
                "[PRICE_SNAPSHOT] {} price={} bid={:?} ask={:?} bid_sz={} ask_sz={} min_order={:?} min_tick={:?}",
                symbol,
                ticker.price,
                top_bid_price,
                top_ask_price,
                top_bid_size,
                top_ask_size,
                ticker.min_order,
                ticker.min_tick
            );
            // avoid hammering (skipped in backtest: ReplayConnector is synchronous)
            if !self.cfg.backtest_mode {
                sleep(Duration::from_millis(50)).await;
            }
        }
        Ok(map)
    }
}

#[cfg(test)]
impl PairTradeEngine {
    fn test_instance(connector: Arc<dyn DexConnector + Send + Sync>) -> Self {
        let mut cfg = PairTradeConfig {
            dex_name: "test".to_string(),
            rest_endpoint: "http://localhost".to_string(),
            web_socket_endpoint: "ws://localhost".to_string(),
            dry_run: true,
            agent_name: None,
            interval_secs: 1,
            trading_period_secs: 1,
            metrics_window: 1,
            entry_z_base: 2.0,
            entry_z_min: 1.8,
            entry_z_max: 2.3,
            exit_z: 0.5,
            stop_loss_z: 3.0,
            force_close_secs: 60,
            cooldown_secs: 1,
            net_funding_min_per_hour: 0.0,
            spread_velocity_max_sigma_per_min: 0.1,
            notional_per_leg: 1.0,
            risk_pct_per_trade: 0.01,
            max_loss_r_mult: DEFAULT_MAX_LOSS_R_MULT,
            equity_usd: DEFAULT_EQUITY_USD,
            universe: vec![PairSpec {
                base: "AAA".to_string(),
                quote: "BBB".to_string(),
            }],
            lookback_hours_short: 1,
            lookback_hours_long: 1,
            half_life_max_hours: 1.0,
            adf_p_threshold: 0.05,
            entry_vol_lookback_hours: 1,
            slippage_bps: 0,
            fee_bps: 0.0,
            max_leverage: 1.0,
            reeval_jump_z_mult: 1.0,
            vol_spike_mult: 1.0,
            max_active_pairs: 1,
            warm_start_mode: WarmStartMode::Strict,
            warm_start_min_bars: 1,
            order_timeout_secs: DEFAULT_ORDER_TIMEOUT_SECS,
            entry_partial_fill_max_retries: DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES,
            startup_force_close_attempts: DEFAULT_STARTUP_FORCE_CLOSE_ATTEMPTS,
            startup_force_close_wait_secs: DEFAULT_STARTUP_FORCE_CLOSE_WAIT_SECS,
            force_close_on_startup: false,
            enable_data_dump: false,
            data_dump_file: None,
            observe_only: false,
            disable_history_persist: true,
            history_file: "test-history.json".to_string(),
            backtest_mode: false,
            backtest_file: None,
            spread_trend_max_slope_sigma: DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA,
            beta_divergence_max: DEFAULT_BETA_DIVERGENCE_MAX,
            beta_min: 0.0,
            hedge_ratio_max_deviation: 1.0,
            circuit_breaker_consecutive_losses: DEFAULT_CIRCUIT_BREAKER_CONSECUTIVE_LOSSES,
            circuit_breaker_cooldown_secs: DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SECS,
            circuit_breaker_tier1_losses: DEFAULT_CB_TIER1_LOSSES,
            circuit_breaker_tier1_cooldown_secs: DEFAULT_CB_TIER1_COOLDOWN_SECS,
            circuit_breaker_tier2_losses: DEFAULT_CB_TIER2_LOSSES,
            circuit_breaker_tier2_cooldown_secs: DEFAULT_CB_TIER2_COOLDOWN_SECS,
            entry_post_only_timeout_secs: DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS,
            entry_velocity_block_sigma_per_min: 0.0,
            funding_entry_z_scale: 0.0,
            beta_gap_entry_z_scale: 0.0,
            shutdown_grace_secs: 0,
            pair_params: HashMap::new(),
            default_pair_params: PairParams {
                entry_z_base: 0.0,
                entry_z_min: 0.0,
                entry_z_max: 0.0,
                exit_z: 0.0,
                stop_loss_z: 0.0,
                force_close_secs: 0,
                cooldown_secs: 0,
                max_loss_r_mult: 0.0,
                half_life_max_hours: 0.0,
                adf_p_threshold: 0.0,
                spread_velocity_max_sigma_per_min: 0.0,
                spread_trend_max_slope_sigma: 0.0,
                beta_divergence_max: 0.0,
                lookback_hours_short: 0,
                beta_min: 0.0,
                hedge_ratio_max_deviation: 1.0,
                lookback_hours_long: 0,
                entry_vol_lookback_hours: 0,
                warm_start_min_bars: 0,
                reeval_jump_z_mult: 0.0,
                vol_spike_mult: 0.0,
                circuit_breaker_tier1_losses: 0,
                circuit_breaker_tier1_cooldown_secs: 0,
                circuit_breaker_tier2_losses: 0,
                circuit_breaker_tier2_cooldown_secs: 0,
                entry_post_only_timeout_secs: 0,
                entry_velocity_block_sigma_per_min: 0.0,
                funding_entry_z_scale: 0.0,
                beta_gap_entry_z_scale: 0.0,
            },
        };
        cfg.default_pair_params = cfg.build_default_pair_params();

        let history_path = PathBuf::from(cfg.history_file.as_str());

        Self {
            cfg,
            connector,
            states: HashMap::new(),
            history: HashMap::new(),
            bar_builders: HashMap::new(),
            equity_cache: DEFAULT_EQUITY_USD,
            last_equity_fetch: None,
            last_metrics_log: None,
            last_ob_warn: HashMap::new(),
            last_ticker_warn: HashMap::new(),
            last_position_warn: HashMap::new(),
            min_order_warned: HashSet::new(),
            min_tick_warned: HashSet::new(),
            positions_ready: false,
            open_positions: HashMap::new(),
            history_path,
            data_dump_writer: None,
            replay_connector: None,
            pnl_logger: None,
            status_reporter: None,
            consecutive_losses: 0,
            circuit_breaker_until: None,
            circuit_breaker_until_ts: None,
            total_trades: 0,
            total_wins: 0,
            total_pnl: 0.0,
            peak_pnl: 0.0,
            max_dd: 0.0,
            shutdown_pending: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SymbolSnapshot {
    price: Decimal,
    funding_rate: Decimal,
    bid_price: Option<Decimal>,
    ask_price: Option<Decimal>,
    bid_size: Decimal,
    ask_size: Decimal,
    min_order: Option<Decimal>,
    min_tick: Option<Decimal>,
    size_decimals: Option<u32>,
    /// Exchange-side timestamp (Unix seconds) for the most recent price update
    /// from the connector. When `Some`, all bots observing the same feed see
    /// identical values for the same update — used to align bar buckets across
    /// processes (pairtrade#4).
    #[serde(default)]
    exchange_ts: Option<i64>,
}

#[derive(Serialize)]
struct DataDumpEntry<'a> {
    timestamp: i64,
    prices: &'a HashMap<String, SymbolSnapshot>,
}

#[derive(Debug, Clone)]
struct PriceSample {
    log_price: f64,
    ts: i64,
}

fn tail_samples(history: &VecDeque<PriceSample>, len: usize) -> Vec<PriceSample> {
    let take = len.min(history.len());
    let mut v: Vec<PriceSample> = history.iter().rev().take(take).cloned().collect();
    v.reverse();
    v
}

#[derive(Debug)]
struct PairEvaluation {
    beta_short: f64,
    beta_long: f64,
    beta_eff: f64,
    half_life_hours: f64,
    adf_p_value: f64,
    eligible: bool,
    score: f64,
    beta_gap: f64,
}

fn regression_beta(x: &[PriceSample], y: &[PriceSample]) -> f64 {
    let n = x.len().min(y.len());
    if n < 2 {
        return 1.0;
    }
    let (mut sum_x, mut sum_y) = (0.0, 0.0);
    for i in 0..n {
        sum_x += x[i].log_price;
        sum_y += y[i].log_price;
    }
    let mean_x = sum_x / n as f64;
    let mean_y = sum_y / n as f64;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    for i in 0..n {
        let dx = x[i].log_price - mean_x;
        let dy = y[i].log_price - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
    }
    if var_x.abs() < 1e-9 {
        1.0
    } else {
        (cov / var_x).clamp(0.1, 10.0)
    }
}

fn entry_z_for_pair(
    cfg: &PairTradeConfig,
    pp: &PairParams,
    state: &PairState,
    vol_median: f64,
) -> f64 {
    let entry_vol_len =
        ((pp.entry_vol_lookback_hours * 3600) / cfg.trading_period_secs).max(1) as usize;
    let vol_pair = tail_std(&state.spread_history, entry_vol_len).unwrap_or(1.0);
    let alpha = (vol_pair / vol_median).clamp(0.5, 2.0);
    let z = pp.entry_z_base * alpha;
    z.clamp(pp.entry_z_min, pp.entry_z_max)
}

fn spread_slope_sigma(history: &VecDeque<f64>, window: usize) -> Option<f64> {
    let len = history.len().min(window);
    if len < 3 {
        return None;
    }
    let start = history.len() - len;
    let n = len as f64;
    let mean_i = (n - 1.0) / 2.0;
    let (mut mean_x, mut cov, mut var_i) = (0.0, 0.0, 0.0);
    for j in 0..len {
        mean_x += history[start + j];
    }
    mean_x /= n;
    for j in 0..len {
        let di = j as f64 - mean_i;
        let dx = history[start + j] - mean_x;
        cov += di * dx;
        var_i += di * di;
    }
    if var_i.abs() < 1e-15 {
        return None;
    }
    let slope = cov / var_i;
    // std of history slice
    let mut sum_sq = 0.0;
    for j in 0..len {
        let dx = history[start + j] - mean_x;
        sum_sq += dx * dx;
    }
    let std = (sum_sq / n).max(0.0).sqrt();
    if std < 1e-9 {
        return None;
    }
    Some((slope / std).abs())
}

fn should_enter(
    cfg: &PairTradeConfig,
    pp: &PairParams,
    state: &PairState,
    z: f64,
    std: f64,
    net_funding: f64,
    now_ts: i64,
) -> bool {
    if let Some(last_exit_ts) = state.last_exit_ts {
        if now_ts.saturating_sub(last_exit_ts) < pp.cooldown_secs as i64 {
            return false;
        }
    }

    // --- Phase 2 filter: spread momentum block ---
    // Block entry when spread is moving fast (likely trending, not mean-reverting).
    // Disabled when entry_velocity_block_sigma_per_min == 0.0.
    if pp.entry_velocity_block_sigma_per_min > 0.0
        && state.last_velocity_sigma_per_min.abs() >= pp.entry_velocity_block_sigma_per_min
    {
        return false;
    }

    let mut entry_threshold = if net_funding > 0.0 {
        // prefer positive carry by easing the required entry slightly
        state.z_entry * 0.9
    } else {
        state.z_entry
    };

    // --- Phase 2 filter: funding rate continuous scaling ---
    // Scale entry_z based on funding magnitude (beyond the simple 0.9x above).
    // funding_entry_z_scale > 0: entry_z *= 1.0 - scale * net_funding
    //   positive funding → lower threshold (easier entry)
    //   negative funding → higher threshold (harder entry)
    // Disabled when funding_entry_z_scale == 0.0.
    if pp.funding_entry_z_scale > 0.0 {
        let adjustment = 1.0 - pp.funding_entry_z_scale * net_funding;
        entry_threshold *= adjustment.clamp(0.5, 2.0);
    }

    // --- Phase 2 filter: beta gap dynamic adjustment ---
    // Raise entry threshold when beta_s and beta_l diverge (hedge unreliable).
    // entry_z *= 1.0 + scale * beta_gap
    // Disabled when beta_gap_entry_z_scale == 0.0.
    if pp.beta_gap_entry_z_scale > 0.0 {
        entry_threshold *= 1.0 + pp.beta_gap_entry_z_scale * state.beta_gap;
    }

    // Avoid entering when the current z already triggers stop-loss exit.
    if z.abs() >= pp.stop_loss_z {
        return false;
    }
    // Spread trend filter: block entry if spread is trending
    if let Some(slope_sigma) = spread_slope_sigma(&state.spread_history, cfg.metrics_window) {
        if slope_sigma > pp.spread_trend_max_slope_sigma {
            return false;
        }
    }
    // Beta stability filter: block entry if beta_s and beta_l diverge
    if state.beta_gap > pp.beta_divergence_max {
        return false;
    }
    // Beta minimum filter: block entry if beta is too low (hedge leg too small)
    if pp.beta_min > 0.0 && state.beta < pp.beta_min {
        return false;
    }
    // Account for estimated cost (fees + slippage) in sigma units
    let total_cost_bps = cfg.fee_bps * 2.0 + cfg.slippage_cost_bps() * 2.0; // two legs
    let cost_ratio = total_cost_bps / 10_000.0;
    let cost_in_sigma = if std <= 1e-9 { 0.0 } else { cost_ratio / std };
    if z.abs() < entry_threshold {
        return false;
    }

    z.abs() >= entry_threshold + cost_in_sigma && net_funding >= cfg.net_funding_min_per_hour
}

fn exit_reason(
    cfg: &PairTradeConfig,
    pp: &PairParams,
    state: &PairState,
    z: f64,
    std: f64,
    p1: &SymbolSnapshot,
    p2: &SymbolSnapshot,
    equity_base: f64,
    now_ts: i64,
) -> Option<&'static str> {
    let pos = state.position.as_ref()?;
    if z.abs() >= pp.stop_loss_z {
        return Some("stop_loss_z");
    }
    if now_ts.saturating_sub(pos.entered_ts) >= pp.force_close_secs as i64 {
        return Some("force_close");
    }
    if pp.exit_z > 0.0 && z.abs() <= pp.exit_z {
        return Some("exit_z");
    }
    let pnl = compute_pnl(pos, p1.price, p2.price);
    if let Some(pnl) = pnl {
        let risk_budget = equity_base * cfg.risk_pct_per_trade;
        if let Some(target) = Decimal::from_f64(risk_budget) {
            if target > Decimal::ZERO {
                if pp.max_loss_r_mult > 0.0 {
                    let loss_mult = Decimal::from_f64(pp.max_loss_r_mult).unwrap_or(Decimal::ONE);
                    let max_loss = -target * loss_mult;
                    if pnl <= max_loss {
                        return Some("max_loss_r");
                    }
                }
                if pnl >= target {
                    return Some("risk_budget");
                }
            }
        }
    }
    if std > 1e-9 {
        if let Some(pnl) = pnl {
            if pnl > Decimal::ZERO {
                let half_life_hours = state.half_life_hours;
                if half_life_hours.is_finite() && half_life_hours > 0.0 {
                    let elapsed_secs = now_ts.saturating_sub(pos.entered_ts).max(0) as f64;
                    let remaining_secs = (pp.force_close_secs as f64) - elapsed_secs;
                    if remaining_secs > 0.0 {
                        let half_life_secs = half_life_hours * 3600.0;
                        let k = (2.0_f64).ln() / half_life_secs;
                        let decay = (-k * remaining_secs).exp();
                        let expected_improvement = z.abs() * (1.0 - decay);
                        let total_cost_bps = cfg.fee_bps * 2.0 + cfg.slippage_cost_bps() * 2.0;
                        let cost_ratio = total_cost_bps / 10_000.0;
                        let cost_in_sigma = cost_ratio / std;
                        if expected_improvement <= cost_in_sigma {
                            return Some("expected_value");
                        }
                    }
                }
            }
        }
    }
    None
}

fn compute_pnl(pos: &Position, exit_price_a: Decimal, exit_price_b: Decimal) -> Option<Decimal> {
    let entry_price_a = pos.entry_price_a?;
    let entry_price_b = pos.entry_price_b?;
    let entry_size_a = pos.entry_size_a?;
    let entry_size_b = pos.entry_size_b?;
    let (pnl_a, pnl_b) = match pos.direction {
        PositionDirection::LongSpread => (
            (exit_price_a - entry_price_a) * entry_size_a,
            (entry_price_b - exit_price_b) * entry_size_b,
        ),
        PositionDirection::ShortSpread => (
            (entry_price_a - exit_price_a) * entry_size_a,
            (exit_price_b - entry_price_b) * entry_size_b,
        ),
    };
    Some(pnl_a + pnl_b)
}

fn net_funding_for_direction(z: f64, p1: &SymbolSnapshot, p2: &SymbolSnapshot) -> f64 {
    if z > 0.0 {
        // plan to short base (p1) and long quote (p2)
        (p2.funding_rate - p1.funding_rate).to_f64().unwrap_or(0.0) / 24.0
    } else {
        // plan to long base (p1) and short quote (p2)
        (p1.funding_rate - p2.funding_rate).to_f64().unwrap_or(0.0) / 24.0
    }
}

fn liquidity_score(p1: &SymbolSnapshot, p2: &SymbolSnapshot) -> f64 {
    let s1 = p1.bid_size.min(p1.ask_size).to_f64().unwrap_or(0.0);
    let s2 = p2.bid_size.min(p2.ask_size).to_f64().unwrap_or(0.0);
    (s1 + s2).max(0.0)
}

fn mean_std(window: &VecDeque<f64>) -> Option<(f64, f64)> {
    if window.is_empty() {
        return None;
    }
    let mean = window.iter().copied().sum::<f64>() / window.len() as f64;
    let var = window
        .iter()
        .map(|v| {
            let d = v - mean;
            d * d
        })
        .sum::<f64>()
        / window.len().max(1) as f64;
    Some((mean, var.sqrt()))
}

fn half_life_and_p(spreads: &[f64]) -> (f64, f64) {
    // ADF-style AR(1) on levels: dY_t = phi * Y_{t-1} + eps
    if spreads.len() < 5 {
        return (f64::INFINITY, 1.0);
    }
    let mut x: Vec<f64> = Vec::with_capacity(spreads.len() - 1);
    let mut dy: Vec<f64> = Vec::with_capacity(spreads.len() - 1);
    for win in spreads.windows(2) {
        let prev = win[0];
        let curr = win[1];
        x.push(prev);
        dy.push(curr - prev);
    }
    let n = x.len();
    let mean_x = x.iter().sum::<f64>() / n as f64;
    let mean_dy = dy.iter().sum::<f64>() / n as f64;
    let mut num = 0.0;
    let mut den = 0.0;
    for i in 0..n {
        let dx = x[i] - mean_x;
        let ddy = dy[i] - mean_dy;
        num += dx * ddy;
        den += dx * dx;
    }
    if den.abs() < 1e-12 {
        return (f64::INFINITY, 1.0);
    }
    let phi = (num / den).clamp(-0.999, 0.999);

    // residual variance and standard error of phi
    let mut rss = 0.0;
    for i in 0..n {
        let fit = phi * (x[i] - mean_x) + mean_dy;
        let err = dy[i] - fit;
        rss += err * err;
    }
    let sigma2 = rss / (n.saturating_sub(2)).max(1) as f64;
    let se_phi = (sigma2 / den).sqrt();
    let t_stat = if se_phi < 1e-12 { 0.0 } else { phi / se_phi };
    let p_value: f64 = df_p_value(t_stat, n);

    let ar_coef = 1.0 + phi;
    let half_life = if ar_coef <= 0.0 || ar_coef >= 1.0 {
        f64::INFINITY
    } else {
        -((2.0_f64).ln()) / ar_coef.ln()
    };

    (half_life, p_value.clamp(0.0, 1.0))
}

fn df_p_value(t_stat: f64, n: usize) -> f64 {
    // Interpolated Dickey-Fuller critical values (with constant), approximate
    const CRITS: &[(usize, f64, f64, f64)] = &[
        (25, -3.75, -3.00, -2.63),
        (50, -3.58, -2.93, -2.60),
        (100, -3.51, -2.89, -2.58),
        (250, -3.46, -2.88, -2.57),
        (500, -3.44, -2.87, -2.57),
    ];
    let (c1, c5, c10) = interpolate_crits(n, CRITS);
    if t_stat < c1 {
        0.005
    } else if t_stat < c5 {
        0.025
    } else if t_stat < c10 {
        0.075
    } else {
        0.5
    }
}

fn interpolate_crits(n: usize, table: &[(usize, f64, f64, f64)]) -> (f64, f64, f64) {
    if n <= table[0].0 {
        return (table[0].1, table[0].2, table[0].3);
    }
    for w in table.windows(2) {
        let (n1, c1_1, c5_1, c10_1) = w[0];
        let (n2, c1_2, c5_2, c10_2) = w[1];
        if n >= n1 && n <= n2 {
            let t = (n - n1) as f64 / (n2 - n1) as f64;
            let lerp = |a: f64, b: f64| a + t * (b - a);
            return (lerp(c1_1, c1_2), lerp(c5_1, c5_2), lerp(c10_1, c10_2));
        }
    }
    let last = table.last().unwrap();
    (last.1, last.2, last.3)
}

fn tail_std(window: &VecDeque<f64>, len: usize) -> Option<f64> {
    if window.is_empty() || len == 0 {
        return None;
    }
    let start = window.len().saturating_sub(len);
    let mut sum = 0.0;
    let mut sum_sq = 0.0;
    let mut count = 0;
    for v in window.iter().skip(start) {
        sum += *v;
        sum_sq += v * v;
        count += 1;
    }
    if count == 0 {
        return None;
    }
    let mean = sum / count as f64;
    let var = (sum_sq / count as f64) - mean * mean;
    Some(var.max(0.0).sqrt())
}

/// Helper to round a price into `step` multiples according to the required direction.
fn round_price_by_tick(price: Decimal, step: Decimal, side: dex_connector::OrderSide) -> Decimal {
    if step <= Decimal::ZERO {
        return price;
    }
    let rounding = match side {
        dex_connector::OrderSide::Long => RoundingStrategy::ToNegativeInfinity,
        dex_connector::OrderSide::Short => RoundingStrategy::ToPositiveInfinity,
    };
    let mut multiples = (price / step).round_dp_with_strategy(0, rounding);
    if multiples < Decimal::ONE {
        multiples = Decimal::ONE;
    }
    let rounded = multiples * step;
    let step_scale = step.scale();
    rounded.round_dp_with_strategy(step_scale, RoundingStrategy::ToZero)
}

fn quantize_size_by_step(size: Decimal, step: Decimal, min_order: Option<Decimal>) -> Decimal {
    if step <= Decimal::ZERO {
        return size;
    }
    let mut multiples = (size / step).trunc();
    if let Some(mo) = min_order {
        if mo > Decimal::ZERO {
            let min_multiplier = (mo / step).ceil();
            if min_multiplier > multiples {
                multiples = min_multiplier;
            }
        }
    }
    let multiplier = if multiples >= Decimal::ONE {
        multiples
    } else {
        Decimal::ONE
    };
    multiplier * step
}

fn quantize_size_by_step_ceiling(
    size: Decimal,
    step: Decimal,
    min_order: Option<Decimal>,
) -> Decimal {
    if step <= Decimal::ZERO {
        return size;
    }
    let mut multiples =
        (size / step).round_dp_with_strategy(0, RoundingStrategy::ToPositiveInfinity);
    if let Some(mo) = min_order {
        if mo > Decimal::ZERO {
            let min_multiplier = (mo / step).ceil();
            if min_multiplier > multiples {
                multiples = min_multiplier;
            }
        }
    }
    let multiplier = if multiples >= Decimal::ONE {
        multiples
    } else {
        Decimal::ONE
    };
    let rounded = multiplier * step;
    rounded.round_dp_with_strategy(step.scale(), RoundingStrategy::ToZero)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn round_price_by_tick_rounds_long_down() {
        let price = dec("100.123");
        let step = dec("0.01");
        let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Long);
        assert_eq!(quantized, dec("100.12"));
    }

    #[test]
    fn round_price_by_tick_rounds_short_up() {
        let price = dec("100.123");
        let step = dec("0.01");
        let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Short);
        assert_eq!(quantized, dec("100.13"));
    }

    #[test]
    fn round_price_by_tick_enforces_minimum_step() {
        let price = dec("0.0001");
        let step = dec("0.005");
        let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Long);
        assert_eq!(quantized, step);
    }

    #[test]
    fn quantize_size_by_step_uses_size_decimals() {
        let size = dec("0.0023");
        let step = dec("0.001");
        let quantized = quantize_size_by_step(size, step, None);
        assert_eq!(quantized, dec("0.002"));
    }

    #[test]
    fn quantize_size_by_step_respects_min_order_floor() {
        let size = dec("0.0002");
        let step = dec("0.0001");
        let quantized = quantize_size_by_step(size, step, Some(dec("0.001")));
        assert_eq!(quantized, dec("0.001"));
    }

    #[test]
    fn quantize_size_by_step_ceiling_rounds_up() {
        let size = dec("0.0023");
        let step = dec("0.001");
        let quantized = quantize_size_by_step_ceiling(size, step, None);
        assert_eq!(quantized, dec("0.003"));
    }
}

#[cfg(test)]
mod pending_tests {
    use super::*;
    use async_trait::async_trait;
    use dex_connector::{
        BalanceResponse, CanceledOrdersResponse, CreateOrderResponse, DexConnector, DexError,
        FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse, OrderBookSnapshot, OrderSide,
        PositionSnapshot, TickerResponse, TpSl, TriggerOrderStyle,
    };
    use rust_decimal::Decimal;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[derive(Default)]
    struct DummyConnector {
        calls: Mutex<Vec<(String, Decimal, OrderSide, Option<Decimal>, bool)>>,
        next_id: AtomicUsize,
    }

    #[async_trait]
    impl DexConnector for DummyConnector {
        async fn start(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn stop(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
            Ok(())
        }

        async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
            Ok(())
        }

        async fn get_ticker(
            &self,
            _symbol: &str,
            _test_price: Option<Decimal>,
        ) -> Result<TickerResponse, DexError> {
            Err(DexError::Other("not used".to_string()))
        }

        async fn get_filled_orders(&self, _symbol: &str) -> Result<FilledOrdersResponse, DexError> {
            Ok(FilledOrdersResponse::default())
        }

        async fn get_canceled_orders(
            &self,
            _symbol: &str,
        ) -> Result<CanceledOrdersResponse, DexError> {
            Ok(CanceledOrdersResponse::default())
        }

        async fn get_open_orders(&self, _symbol: &str) -> Result<OpenOrdersResponse, DexError> {
            Ok(OpenOrdersResponse::default())
        }

        async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
            Ok(BalanceResponse::default())
        }

        async fn get_combined_balance(
            &self,
        ) -> Result<dex_connector::CombinedBalanceResponse, DexError> {
            Ok(dex_connector::CombinedBalanceResponse::default())
        }

        async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
            Ok(vec![])
        }

        async fn get_last_trades(&self, _symbol: &str) -> Result<LastTradesResponse, DexError> {
            Ok(LastTradesResponse::default())
        }

        async fn get_order_book(
            &self,
            _symbol: &str,
            _depth: usize,
        ) -> Result<OrderBookSnapshot, DexError> {
            Ok(OrderBookSnapshot::default())
        }

        async fn clear_filled_order(&self, _symbol: &str, _trade_id: &str) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_canceled_order(
            &self,
            _symbol: &str,
            _order_id: &str,
        ) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn create_order(
            &self,
            symbol: &str,
            size: Decimal,
            side: OrderSide,
            price: Option<Decimal>,
            _spread: Option<i64>,
            reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            let order_id = format!("test-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
            let ordered_price = price.unwrap_or_else(|| Decimal::ONE);
            self.calls
                .lock()
                .unwrap()
                .push((symbol.to_string(), size, side, price, reduce_only));
            Ok(CreateOrderResponse {
                order_id,
                exchange_order_id: None,
                ordered_price,
                ordered_size: size,
            })
        }

        async fn create_advanced_trigger_order(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _trigger_px: Decimal,
            _limit_px: Option<Decimal>,
            _order_style: TriggerOrderStyle,
            _slippage_bps: Option<u32>,
            _tpsl: TpSl,
            _reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            Err(DexError::Other("not used".to_string()))
        }

        async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
            Ok(())
        }

        async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
            Ok(())
        }

        async fn cancel_orders(
            &self,
            _symbol: Option<String>,
            _order_ids: Vec<String>,
        ) -> Result<(), DexError> {
            Ok(())
        }

        async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
            Ok(())
        }

        async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
            false
        }

        async fn sign_evm_65b(&self, _message: &str) -> Result<String, DexError> {
            Ok("signed".to_string())
        }

        async fn sign_evm_65b_with_eip191(&self, _message: &str) -> Result<String, DexError> {
            Ok("signed".to_string())
        }
    }

    #[tokio::test]
    async fn reissue_partial_entry_leg_reorders_remaining() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        let pending = PendingOrders {
            legs: vec![PendingLeg {
                symbol: "AAA".to_string(),
                order_id: "leg1".to_string(),
                exchange_order_id: None,
                target: dec("0.05"),
                filled: Decimal::ZERO,
                side: OrderSide::Long,
                placed_price: dec("0.10"),
            }],
            direction: PositionDirection::LongSpread,
            placed_at: Instant::now(),
            hedge_retry_count: 0,
            post_only_hybrid: false,
        };
        let mut price_map = HashMap::new();
        price_map.insert(
            "AAA".to_string(),
            SymbolSnapshot {
                price: dec("100.0"),
                funding_rate: Decimal::ZERO,
                bid_price: None,
                ask_price: None,
                bid_size: Decimal::ZERO,
                ask_size: Decimal::ZERO,
                min_order: Some(dec("0.001")),
                min_tick: Some(dec("0.001")),
                size_decimals: Some(3),
                exchange_ts: None,
            },
        );
        let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);

        let result = engine
            .reissue_partial_legs(&pending, &filled_qtys, &price_map, false, false, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.legs.len(), 2);
        assert!(result
            .legs
            .iter()
            .any(|leg| leg.target == dec("0.02") && leg.filled == dec("0.02")));
        assert!(result
            .legs
            .iter()
            .any(|leg| leg.target == dec("0.03") && leg.filled == Decimal::ZERO));
        let calls = connector.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "AAA");
        assert_eq!(calls[0].3, Some(dec("100.0")));
        assert!(!calls[0].4);
    }

    #[tokio::test]
    async fn reissue_partial_entry_missing_price_keeps_pending() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);
        let pending = PendingOrders {
            legs: vec![PendingLeg {
                symbol: "AAA".to_string(),
                order_id: "leg1".to_string(),
                exchange_order_id: None,
                target: dec("0.05"),
                filled: Decimal::ZERO,
                side: OrderSide::Long,
                placed_price: dec("0.10"),
            }],
            direction: PositionDirection::LongSpread,
            placed_at: Instant::now(),
            hedge_retry_count: 0,
            post_only_hybrid: false,
        };
        let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);

        let result = engine
            .reissue_partial_legs(&pending, &filled_qtys, &HashMap::new(), false, false, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.legs.len(), 1);
        assert_eq!(result.legs[0].target, dec("0.05"));
        assert_eq!(result.legs[0].filled, dec("0.02"));
    }
}

#[cfg(test)]
mod shutdown_grace_tests {
    use super::*;

    fn config_path(name: &str) -> String {
        format!("{}/configs/pairtrade/{}", env!("CARGO_MANIFEST_DIR"), name)
    }

    #[test]
    fn default_when_yaml_omits_key() {
        // from_env() path with no env var set = default
        // Use a scoped env guard to avoid bleeding into other tests.
        let prev = std::env::var("SHUTDOWN_GRACE_SECS").ok();
        std::env::remove_var("SHUTDOWN_GRACE_SECS");
        // Also ensure required env vars have sensible fallbacks.
        std::env::set_var("DEX_NAME", "hyperliquid");
        std::env::set_var("UNIVERSE_PAIRS", "BTC/ETH");
        let cfg = PairTradeConfig::from_env().expect("from_env failed");
        assert_eq!(cfg.shutdown_grace_secs, DEFAULT_SHUTDOWN_GRACE_SECS);
        assert_eq!(cfg.shutdown_grace_secs, 3660);
        if let Some(v) = prev {
            std::env::set_var("SHUTDOWN_GRACE_SECS", v);
        }
    }

    #[test]
    fn live_btceth_configs_pin_grace_above_force_close() {
        for name in &[
            "debot-pair-btceth.yaml",
            "debot-pair-btceth-b.yaml",
            "debot-pair-btceth-c.yaml",
            "debot-pair-solhype.yaml",
        ] {
            let path = config_path(name);
            let cfg = PairTradeConfig::from_yaml_path(&path)
                .unwrap_or_else(|e| panic!("failed to load {path}: {e}"));
            assert_eq!(
                cfg.shutdown_grace_secs, 3660,
                "{name}: expected shutdown_grace_secs=3660, got {}",
                cfg.shutdown_grace_secs
            );
            assert!(
                cfg.shutdown_grace_secs >= cfg.force_close_secs + 60,
                "{name}: grace ({}) must be >= force_close_secs ({}) + 60",
                cfg.shutdown_grace_secs,
                cfg.force_close_secs
            );
        }
    }
}
