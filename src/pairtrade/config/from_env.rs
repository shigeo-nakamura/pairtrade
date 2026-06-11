use std::collections::HashMap;
use std::env;

use anyhow::{anyhow, Result};

use super::super::defaults::*;
use super::env_util::{
    env_parse, env_parse_critical, load_bt_eval_timestamps, load_bt_restart_timestamps,
};
use super::universe::{default_history_file, parse_universe_pairs};
use super::{resolve_strategies, PairParams, PairTradeConfig, RiskConfig, WarmStartMode};

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

/// Resolve global per-pair defaults from environment variables, falling back
/// to compile-time `DEFAULT_*` constants for any missing entries.
fn default_pair_params_from_env() -> PairParams {
    PairParams {
        entry_z_base: env_parse("ENTRY_Z_SCORE_BASE", DEFAULT_ENTRY_Z_BASE),
        entry_z_min: env_parse("ENTRY_Z_SCORE_MIN", DEFAULT_ENTRY_Z_MIN),
        entry_z_max: env_parse("ENTRY_Z_SCORE_MAX", DEFAULT_ENTRY_Z_MAX),
        exit_z: env_parse("EXIT_Z_SCORE", DEFAULT_EXIT_Z),
        stop_loss_z: env_parse("STOP_LOSS_Z_SCORE", DEFAULT_STOP_LOSS_Z),
        force_close_secs: env_parse("FORCE_CLOSE_TIME_SECS", DEFAULT_FORCE_CLOSE_SECS),
        cooldown_secs: env_parse("COOLDOWN_SECS", DEFAULT_COOLDOWN_SECS),
        stop_loss_cooldown_secs: env_parse(
            "STOP_LOSS_COOLDOWN_SECS",
            DEFAULT_STOP_LOSS_COOLDOWN_SECS,
        ),
        // bot-strategy#439: stop-loss multiplier — silent default revert
        // could place a trade with a much wider stop than intended.
        max_loss_r_mult: env_parse_critical("MAX_LOSS_R_MULT", DEFAULT_MAX_LOSS_R_MULT),
        half_life_max_hours: env_parse("HALF_LIFE_MAX_HOURS", DEFAULT_HALF_LIFE_MAX_HOURS),
        adf_p_threshold: env_parse("ADF_P_THRESHOLD", DEFAULT_ADF_P_THRESHOLD),
        spread_velocity_max_sigma_per_min: env_parse(
            "SPREAD_VELOCITY_MAX_SIGMA_PER_MIN",
            DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN,
        ),
        spread_trend_max_slope_sigma: env_parse(
            "SPREAD_TREND_MAX_SLOPE_SIGMA",
            DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA,
        ),
        beta_divergence_max: env_parse("BETA_DIVERGENCE_MAX", DEFAULT_BETA_DIVERGENCE_MAX),
        beta_min: env_parse("BETA_MIN", 0.0),
        hedge_ratio_max_deviation: env_parse("HEDGE_RATIO_MAX_DEVIATION", 1.0),
        lookback_hours_short: env_parse(
            "PAIR_SELECTION_LOOKBACK_HOURS_SHORT",
            DEFAULT_LOOKBACK_HOURS_SHORT,
        ),
        lookback_hours_long: env_parse(
            "PAIR_SELECTION_LOOKBACK_HOURS_LONG",
            DEFAULT_LOOKBACK_HOURS_LONG,
        ),
        entry_vol_lookback_hours: env_parse(
            "ENTRY_VOL_LOOKBACK_HOURS",
            DEFAULT_ENTRY_VOL_LOOKBACK_HOURS,
        ),
        // Caller is responsible for filling warm_start_min_bars from
        // metrics_window when omitted.
        warm_start_min_bars: env_parse::<usize>("WARM_START_MIN_BARS", 0),
        reeval_jump_z_mult: env_parse("REEVAL_JUMP_Z_MULT", DEFAULT_REEVAL_JUMP_Z_MULT),
        vol_spike_mult: env_parse("VOL_SPIKE_MULT", DEFAULT_VOL_SPIKE_MULT),
        circuit_breaker_tier1_losses: env_parse(
            "CIRCUIT_BREAKER_TIER1_LOSSES",
            DEFAULT_CB_TIER1_LOSSES,
        ),
        circuit_breaker_tier1_cooldown_secs: env_parse(
            "CIRCUIT_BREAKER_TIER1_COOLDOWN_SECS",
            DEFAULT_CB_TIER1_COOLDOWN_SECS,
        ),
        circuit_breaker_tier2_losses: env_parse(
            "CIRCUIT_BREAKER_TIER2_LOSSES",
            DEFAULT_CB_TIER2_LOSSES,
        ),
        circuit_breaker_tier2_cooldown_secs: env_parse(
            "CIRCUIT_BREAKER_TIER2_COOLDOWN_SECS",
            DEFAULT_CB_TIER2_COOLDOWN_SECS,
        ),
        entry_post_only_timeout_secs: env_parse(
            "ENTRY_POST_ONLY_TIMEOUT_SECS",
            DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS,
        ),
        exit_post_only_timeout_secs: env_parse(
            "EXIT_POST_ONLY_TIMEOUT_SECS",
            DEFAULT_EXIT_POST_ONLY_TIMEOUT_SECS,
        ),
        entry_velocity_block_sigma_per_min: env_parse("ENTRY_VELOCITY_BLOCK_SIGMA_PER_MIN", 0.0),
        funding_entry_z_scale: env_parse("FUNDING_ENTRY_Z_SCALE", 0.0),
        beta_gap_entry_z_scale: env_parse("BETA_GAP_ENTRY_Z_SCALE", 0.0),
        beta_gap_notional_scale: env_parse("BETA_GAP_NOTIONAL_SCALE", 0.0),
        beta_gap_notional_floor: env_parse("BETA_GAP_NOTIONAL_FLOOR", 0.5),
        // bot-strategy#515 — default DISABLED (slope=0.0).
        depth_size_slope: env_parse("DEPTH_SIZE_SLOPE", 0.0),
        depth_size_min: env_parse("DEPTH_SIZE_MIN", 0.5),
        depth_size_max: env_parse("DEPTH_SIZE_MAX", 1.5),
        // bot-strategy#463 Phase 1 — default DISABLED (threshold=0.0).
        rehedge_drift_threshold_pct: env_parse("REHEDGE_DRIFT_THRESHOLD_PCT", 0.0),
        rehedge_cooldown_secs: env_parse("REHEDGE_COOLDOWN_SECS", 1800u64),
        rehedge_min_qty_notional_usd: env_parse("REHEDGE_MIN_QTY_NOTIONAL_USD", 50.0),
        // bot-strategy#463 Phase 2 — live order placement is OFF by
        // default. BT / dry_run always simulate; live opt-in requires
        // this to be flipped (env or per-strategy YAML).
        rehedge_live_enabled: env_parse("REHEDGE_LIVE_ENABLED", false),
        // bot-strategy#471 — amend-on-partial-fill is OFF by default; flip
        // per host (env or per-strategy YAML) after the Tokyo Extended
        // dry_run soak.
        use_amend_on_partial_fill: env_parse("USE_AMEND_ON_PARTIAL_FILL", false),
        rehedge_require_no_revert: env_parse("REHEDGE_REQUIRE_NO_REVERT", false),
        rehedge_z_no_revert_factor: env_parse("REHEDGE_Z_NO_REVERT_FACTOR", 1.0),
        rehedge_velocity_projected_drift_min: env_parse(
            "REHEDGE_VELOCITY_PROJECTED_DRIFT_MIN",
            0.0,
        ),
        beta_uncertainty_max: env_parse("BETA_UNCERTAINTY_MAX", 0.0),
        entry_z_short_multiplier: env_parse("ENTRY_Z_SHORT_MULTIPLIER", 1.0),
        mtf_windows: env::var("MTF_WINDOWS")
            .ok()
            .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
            .unwrap_or_default(),
        mtf_z_min: env_parse("MTF_Z_MIN", DEFAULT_MTF_Z_MIN),
        std_collapse_window_bars: env_parse(
            "STD_COLLAPSE_WINDOW_BARS",
            DEFAULT_STD_COLLAPSE_WINDOW_BARS,
        ),
        std_collapse_min_ratio: env_parse("STD_COLLAPSE_MIN_RATIO", DEFAULT_STD_COLLAPSE_MIN_RATIO),
        std_collapse_hold_down_secs: env_parse(
            "STD_COLLAPSE_HOLD_DOWN_SECS",
            DEFAULT_STD_COLLAPSE_HOLD_DOWN_SECS,
        ),
        std_collapse_observe_only: env::var("STD_COLLAPSE_OBSERVE_ONLY")
            .ok()
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(DEFAULT_STD_COLLAPSE_OBSERVE_ONLY),
        use_frozen_beta_exit_z: env::var("USE_FROZEN_BETA_EXIT_Z")
            .ok()
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(DEFAULT_USE_FROZEN_BETA_EXIT_Z),
        regime_block_entries: env::var("REGIME_BLOCK_ENTRIES")
            .ok()
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(DEFAULT_REGIME_BLOCK_ENTRIES),
    }
}
