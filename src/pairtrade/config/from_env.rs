use std::collections::HashMap;
use std::env;

use anyhow::{anyhow, Result};

use super::super::defaults::*;
use super::env_util::{env_parse, load_bt_eval_timestamps, load_bt_restart_timestamps};
use super::params::default_pair_params_from_env;
use super::strategy::resolve_strategies;
use super::universe::{default_history_file, parse_universe_pairs};
use super::{PairParams, PairTradeConfig, RiskConfig, WarmStartMode};

impl PairTradeConfig {
    pub fn from_env() -> Result<Self> {
        let dex_name = env::var("DEX_NAME").unwrap_or_else(|_| "lighter".to_string());
        let rest_endpoint = env::var("REST_ENDPOINT").unwrap_or_default();
        let web_socket_endpoint = env::var("WEB_SOCKET_ENDPOINT").unwrap_or_default();
        // bot-strategy#439: DRY_RUN is trading-critical — a silent typo here
        // (e.g. `DRY_RUN=ture`) would flip the bot live. Accept only an
        // unambiguous "true" / "false" (case-insensitive) and refuse to start
        // on anything else.
        let dry_run = match env::var("DRY_RUN") {
            Err(_) => true,
            Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
                "true" => true,
                "false" => false,
                other => panic!(
                    "[CONFIG] trading-critical env DRY_RUN={:?} is not 'true' or 'false'; \
                     refusing to start. Fix the env var or unset it (default = true). (bot-strategy#439)",
                    other
                ),
            },
        };
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
        let net_funding_min_per_hour = env::var("NET_FUNDING_MIN_PER_HOUR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_NET_FUNDING_MIN_PER_HOUR);
        let risk_pct_per_trade = env::var("RISK_PCT_PER_TRADE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_RISK_PCT_PER_TRADE);
        // bot-strategy#439: trading-critical. Hard-fail on parse error.
        let equity_reference_usd = match env::var("EQUITY_REFERENCE_USD") {
            Err(_) => DEFAULT_EQUITY_USD,
            Ok(value) => value.parse::<f64>().unwrap_or_else(|e| panic!(
                "[CONFIG] trading-critical env EQUITY_REFERENCE_USD={:?} failed to parse ({}); refusing to start. (bot-strategy#439)",
                value, e
            )),
        };
        let universe = parse_universe_pairs()?;
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
        let max_active_pairs = env::var("MAX_ACTIVE_PAIRS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_ACTIVE_PAIRS);
        let warm_start_mode = env::var("WARM_START_MODE")
            .ok()
            .unwrap_or_else(|| DEFAULT_WARM_START_MODE.to_string())
            .parse()
            .unwrap_or(WarmStartMode::Strict);
        let order_timeout_secs = env::var("ORDER_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_ORDER_TIMEOUT_SECS);
        let entry_partial_fill_max_retries = env::var("ENTRY_PARTIAL_FILL_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES);
        let entry_partial_fill_giveup_retries = env::var("ENTRY_PARTIAL_FILL_GIVEUP_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_ENTRY_PARTIAL_FILL_GIVEUP_RETRIES);
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
            net_funding_min_per_hour,
            risk_pct_per_trade,
            equity_reference_usd,
            universe,
            slippage_bps,
            fee_bps,
            max_leverage,
            max_active_pairs,
            warm_start_mode,
            order_timeout_secs,
            entry_partial_fill_max_retries,
            entry_partial_fill_giveup_retries,
            startup_force_close_attempts,
            startup_force_close_wait_secs,
            force_close_on_startup,
            enable_data_dump,
            data_dump_file,
            observe_only,
            disable_history_persist,
            history_file,
            history_archive_dir: env::var("HISTORY_ARCHIVE_DIR")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            history_archive_retention_days: env::var("HISTORY_ARCHIVE_RETENTION_DAYS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(360),
            backtest_mode,
            backtest_file,
            bt_warm_start_snapshot: env::var("BT_WARM_START_SNAPSHOT")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            bt_eval_timestamps: load_bt_eval_timestamps(),
            bt_restart_timestamps: load_bt_restart_timestamps(),
            bt_fill_delay_secs: env::var("BT_FILL_DELAY_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            bt_regime_series_file: env::var("BT_REGIME_SERIES_FILE")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            // bot-strategy#531 ineligible-close book-quality guard
            ineligible_close_defer_cap_secs: env::var("INELIGIBLE_CLOSE_DEFER_CAP_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            ineligible_close_defer_spread_bps: env::var("INELIGIBLE_CLOSE_DEFER_SPREAD_BPS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(20.0),
            ineligible_close_defer_stale_secs: env::var("INELIGIBLE_CLOSE_DEFER_STALE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            shutdown_grace_secs: env::var("SHUTDOWN_GRACE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_SHUTDOWN_GRACE_SECS),
            pair_params: HashMap::new(),
            // Placeholder rebuilt immediately below.
            default_pair_params: PairParams::default(),
            strategies: Vec::new(),
            use_kalman_beta: env::var("USE_KALMAN_BETA")
                .ok()
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(DEFAULT_USE_KALMAN_BETA),
            kalman_q: env_parse("KALMAN_Q", DEFAULT_KALMAN_Q),
            kalman_r: env_parse("KALMAN_R", DEFAULT_KALMAN_R),
            kalman_initial_p: env_parse("KALMAN_INITIAL_P", DEFAULT_KALMAN_INITIAL_P),
            kalman_min_updates: env_parse("KALMAN_MIN_UPDATES", DEFAULT_KALMAN_MIN_UPDATES),
            regime_vol_window: env_parse("REGIME_VOL_WINDOW", DEFAULT_REGIME_VOL_WINDOW),
            regime_vol_max: env_parse("REGIME_VOL_MAX", DEFAULT_REGIME_VOL_MAX),
            regime_trend_window: env_parse("REGIME_TREND_WINDOW", DEFAULT_REGIME_TREND_WINDOW),
            regime_trend_max: env_parse("REGIME_TREND_MAX", DEFAULT_REGIME_TREND_MAX),
            regime_reference_symbol: env::var("REGIME_REFERENCE_SYMBOL")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_REGIME_REFERENCE_SYMBOL.to_string()),
            risk: RiskConfig::default(),
            round_id: env::var("ROUND_ID").ok().filter(|v| !v.trim().is_empty()),
            config_source_path: None,
        };
        cfg.default_pair_params = default_pair_params_from_env();
        if cfg.default_pair_params.warm_start_min_bars == 0 {
            cfg.default_pair_params.warm_start_min_bars = cfg.metrics_window;
        }
        cfg.strategies = resolve_strategies(&cfg, None);
        cfg.validate()?;
        Ok(cfg)
    }
}
