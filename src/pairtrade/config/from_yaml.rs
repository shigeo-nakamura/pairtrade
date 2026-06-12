use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};

use super::super::defaults::*;

use super::params::default_pair_params_from_yaml;
use super::risk::resolve_risk_config;
use super::schema::PairTradeYaml;
use super::strategy::resolve_strategies;
use super::universe::{default_history_file, resolve_universe_from_yaml};
use super::{PairTradeConfig, WarmStartMode};

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
            bt_regime_series_file: None,  // env-only, not in YAML
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
