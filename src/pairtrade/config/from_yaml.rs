use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};

use super::super::defaults::*;
use super::risk::resolve_risk_config;
use super::schema::PairTradeYaml;
use super::strategy::resolve_strategies;
use super::universe::{default_history_file, resolve_universe_from_yaml};
use super::{PairParams, PairTradeConfig, WarmStartMode};

impl PairTradeConfig {
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
        let history_file = yaml
            .history_file
            .clone()
            .unwrap_or_else(|| default_history_file(&universe, yaml.agent_name.as_deref()));

        // Build the resolved per-pair defaults before consuming `yaml` into the
        // PairTradeConfig literal below.
        let mut default_pair_params = default_pair_params_from_yaml(&yaml);
        if default_pair_params.warm_start_min_bars == 0 {
            default_pair_params.warm_start_min_bars = metrics_window;
        }

        let mut cfg = PairTradeConfig {
            dex_name: yaml.dex_name.unwrap_or_else(|| "lighter".to_string()),
            rest_endpoint: yaml.rest_endpoint.unwrap_or_default(),
            web_socket_endpoint: yaml.web_socket_endpoint.unwrap_or_default(),
            dry_run: yaml.dry_run.unwrap_or(true),
            agent_name: yaml.agent_name,
            interval_secs: yaml.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS),
            trading_period_secs: yaml
                .trading_period_secs
                .unwrap_or(DEFAULT_TRADING_PERIOD_SECS),
            metrics_window,
            net_funding_min_per_hour: yaml
                .net_funding_min_per_hour
                .unwrap_or(DEFAULT_NET_FUNDING_MIN_PER_HOUR),
            risk_pct_per_trade: yaml
                .risk_pct_per_trade
                .unwrap_or(DEFAULT_RISK_PCT_PER_TRADE),
            equity_reference_usd: yaml.equity_usd_reference.unwrap_or(DEFAULT_EQUITY_USD),
            universe,
            slippage_bps: yaml.slippage_bps.unwrap_or(DEFAULT_SLIPPAGE_BPS),
            fee_bps: yaml.fee_bps.unwrap_or(DEFAULT_FEE_BPS),
            max_leverage: yaml.max_leverage.unwrap_or(DEFAULT_MAX_LEVERAGE),
            max_active_pairs: yaml.max_active_pairs.unwrap_or(DEFAULT_MAX_ACTIVE_PAIRS),
            warm_start_mode,
            order_timeout_secs: yaml
                .order_timeout_secs
                .unwrap_or(DEFAULT_ORDER_TIMEOUT_SECS),
            entry_partial_fill_max_retries: yaml
                .entry_partial_fill_max_retries
                .unwrap_or(DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES),
            entry_partial_fill_giveup_retries: yaml
                .entry_partial_fill_giveup_retries
                .unwrap_or(DEFAULT_ENTRY_PARTIAL_FILL_GIVEUP_RETRIES),
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
            history_archive_dir: yaml.history_archive_dir,
            history_archive_retention_days: yaml.history_archive_retention_days.unwrap_or(360),
            backtest_mode: yaml.backtest_mode.unwrap_or(false),
            backtest_file: yaml.backtest_file,
            bt_warm_start_snapshot: None, // env-only, not in YAML
            bt_eval_timestamps: None,     // env-only, not in YAML
            bt_restart_timestamps: None,  // env-only, not in YAML
            bt_fill_delay_secs: 0,        // env-only, not in YAML
            shutdown_grace_secs: yaml
                .shutdown_grace_secs
                .unwrap_or(DEFAULT_SHUTDOWN_GRACE_SECS),
            pair_params: HashMap::new(),
            default_pair_params,
            strategies: Vec::new(),
            use_kalman_beta: yaml.use_kalman_beta.unwrap_or(DEFAULT_USE_KALMAN_BETA),
            kalman_q: yaml.kalman_q.unwrap_or(DEFAULT_KALMAN_Q),
            kalman_r: yaml.kalman_r.unwrap_or(DEFAULT_KALMAN_R),
            kalman_initial_p: yaml.kalman_initial_p.unwrap_or(DEFAULT_KALMAN_INITIAL_P),
            kalman_min_updates: yaml
                .kalman_min_updates
                .unwrap_or(DEFAULT_KALMAN_MIN_UPDATES),
            regime_vol_window: yaml.regime_vol_window.unwrap_or(DEFAULT_REGIME_VOL_WINDOW),
            regime_vol_max: yaml.regime_vol_max.unwrap_or(DEFAULT_REGIME_VOL_MAX),
            regime_trend_window: yaml
                .regime_trend_window
                .unwrap_or(DEFAULT_REGIME_TREND_WINDOW),
            regime_trend_max: yaml.regime_trend_max.unwrap_or(DEFAULT_REGIME_TREND_MAX),
            regime_reference_symbol: yaml
                .regime_reference_symbol
                .clone()
                .unwrap_or_else(|| DEFAULT_REGIME_REFERENCE_SYMBOL.to_string()),
            risk: resolve_risk_config(yaml.risk.as_ref())?,
            round_id: yaml.round_id.clone(),
        };

        cfg.apply_env_overrides(history_file_from_yaml, warm_start_min_from_yaml)?;
        cfg.strategies = resolve_strategies(&cfg, yaml.strategies.as_deref());
        cfg.validate()?;
        Ok(cfg)
    }
}

/// Resolve global per-pair defaults directly from a YAML document, falling
/// back to compile-time `DEFAULT_*` constants for any missing fields.
fn default_pair_params_from_yaml(yaml: &PairTradeYaml) -> PairParams {
    PairParams {
        entry_z_base: yaml.entry_z_score_base.unwrap_or(DEFAULT_ENTRY_Z_BASE),
        entry_z_min: yaml.entry_z_score_min.unwrap_or(DEFAULT_ENTRY_Z_MIN),
        entry_z_max: yaml.entry_z_score_max.unwrap_or(DEFAULT_ENTRY_Z_MAX),
        exit_z: yaml.exit_z_score.unwrap_or(DEFAULT_EXIT_Z),
        stop_loss_z: yaml.stop_loss_z_score.unwrap_or(DEFAULT_STOP_LOSS_Z),
        force_close_secs: yaml
            .force_close_time_secs
            .unwrap_or(DEFAULT_FORCE_CLOSE_SECS),
        cooldown_secs: yaml.cooldown_secs.unwrap_or(DEFAULT_COOLDOWN_SECS),
        stop_loss_cooldown_secs: yaml
            .stop_loss_cooldown_secs
            .unwrap_or(DEFAULT_STOP_LOSS_COOLDOWN_SECS),
        max_loss_r_mult: yaml.max_loss_r_mult.unwrap_or(DEFAULT_MAX_LOSS_R_MULT),
        half_life_max_hours: yaml
            .half_life_max_hours
            .unwrap_or(DEFAULT_HALF_LIFE_MAX_HOURS),
        adf_p_threshold: yaml.adf_p_threshold.unwrap_or(DEFAULT_ADF_P_THRESHOLD),
        spread_velocity_max_sigma_per_min: yaml
            .spread_velocity_max_sigma_per_min
            .unwrap_or(DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN),
        spread_trend_max_slope_sigma: yaml
            .spread_trend_max_slope_sigma
            .unwrap_or(DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA),
        beta_divergence_max: yaml
            .beta_divergence_max
            .unwrap_or(DEFAULT_BETA_DIVERGENCE_MAX),
        beta_min: yaml.beta_min.unwrap_or(0.0),
        hedge_ratio_max_deviation: yaml.hedge_ratio_max_deviation.unwrap_or(1.0),
        lookback_hours_short: yaml
            .pair_selection_lookback_hours_short
            .unwrap_or(DEFAULT_LOOKBACK_HOURS_SHORT),
        lookback_hours_long: yaml
            .pair_selection_lookback_hours_long
            .unwrap_or(DEFAULT_LOOKBACK_HOURS_LONG),
        entry_vol_lookback_hours: yaml
            .entry_vol_lookback_hours
            .unwrap_or(DEFAULT_ENTRY_VOL_LOOKBACK_HOURS),
        // Caller is responsible for clamping warm_start_min_bars to
        // metrics_window when omitted (it has a cross-field default).
        warm_start_min_bars: yaml.warm_start_min_bars.unwrap_or(0),
        reeval_jump_z_mult: yaml
            .reeval_jump_z_mult
            .unwrap_or(DEFAULT_REEVAL_JUMP_Z_MULT),
        vol_spike_mult: yaml.vol_spike_mult.unwrap_or(DEFAULT_VOL_SPIKE_MULT),
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
        exit_post_only_timeout_secs: yaml
            .exit_post_only_timeout_secs
            .unwrap_or(DEFAULT_EXIT_POST_ONLY_TIMEOUT_SECS),
        entry_velocity_block_sigma_per_min: yaml.entry_velocity_block_sigma_per_min.unwrap_or(0.0),
        funding_entry_z_scale: yaml.funding_entry_z_scale.unwrap_or(0.0),
        beta_gap_entry_z_scale: yaml.beta_gap_entry_z_scale.unwrap_or(0.0),
        beta_gap_notional_scale: yaml.beta_gap_notional_scale.unwrap_or(0.0),
        beta_gap_notional_floor: yaml.beta_gap_notional_floor.unwrap_or(0.5),
        // bot-strategy#515 defaults — disabled.
        depth_size_slope: yaml.depth_size_slope.unwrap_or(0.0),
        depth_size_min: yaml.depth_size_min.unwrap_or(0.5),
        depth_size_max: yaml.depth_size_max.unwrap_or(1.5),
        // bot-strategy#463 Phase 1 defaults — disabled.
        rehedge_drift_threshold_pct: yaml.rehedge_drift_threshold_pct.unwrap_or(0.0),
        rehedge_cooldown_secs: yaml.rehedge_cooldown_secs.unwrap_or(1800),
        rehedge_min_qty_notional_usd: yaml.rehedge_min_qty_notional_usd.unwrap_or(50.0),
        rehedge_live_enabled: yaml.rehedge_live_enabled.unwrap_or(false),
        use_amend_on_partial_fill: yaml.use_amend_on_partial_fill.unwrap_or(false),
        rehedge_require_no_revert: yaml.rehedge_require_no_revert.unwrap_or(false),
        rehedge_z_no_revert_factor: yaml.rehedge_z_no_revert_factor.unwrap_or(1.0),
        rehedge_velocity_projected_drift_min: yaml
            .rehedge_velocity_projected_drift_min
            .unwrap_or(0.0),
        beta_uncertainty_max: yaml.beta_uncertainty_max.unwrap_or(0.0),
        entry_z_short_multiplier: yaml.entry_z_short_multiplier.unwrap_or(1.0),
        mtf_windows: yaml.mtf_windows.clone().unwrap_or_default(),
        mtf_z_min: yaml.mtf_z_min.unwrap_or(DEFAULT_MTF_Z_MIN),
        std_collapse_window_bars: yaml
            .std_collapse_window_bars
            .unwrap_or(DEFAULT_STD_COLLAPSE_WINDOW_BARS),
        std_collapse_min_ratio: yaml
            .std_collapse_min_ratio
            .unwrap_or(DEFAULT_STD_COLLAPSE_MIN_RATIO),
        std_collapse_hold_down_secs: yaml
            .std_collapse_hold_down_secs
            .unwrap_or(DEFAULT_STD_COLLAPSE_HOLD_DOWN_SECS),
        std_collapse_observe_only: yaml
            .std_collapse_observe_only
            .unwrap_or(DEFAULT_STD_COLLAPSE_OBSERVE_ONLY),
        use_frozen_beta_exit_z: yaml
            .use_frozen_beta_exit_z
            .unwrap_or(DEFAULT_USE_FROZEN_BETA_EXIT_Z),
        regime_block_entries: yaml
            .regime_block_entries
            .unwrap_or(DEFAULT_REGIME_BLOCK_ENTRIES),
    }
}
