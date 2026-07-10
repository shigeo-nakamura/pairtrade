use std::env;

use anyhow::{anyhow, Result};

use super::env_util::{
    env_override, env_override_critical, load_bt_eval_timestamps, load_bt_restart_timestamps,
};
use super::universe::{default_history_file, parse_universe_pairs};
use super::PairTradeConfig;

impl PairTradeConfig {
    pub(super) fn apply_env_overrides(
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
            // bot-strategy#439: same strict parse as the constructor. Reject
            // typos rather than silently flipping live.
            self.dry_run = match value.trim().to_ascii_lowercase().as_str() {
                "true" => true,
                "false" => false,
                other => panic!(
                    "[CONFIG] trading-critical env DRY_RUN={:?} is not 'true' or 'false'; \
                     refusing to start. (bot-strategy#439)",
                    other
                ),
            };
        }
        if let Ok(value) = env::var("AGENT_NAME") {
            if !value.trim().is_empty() {
                self.agent_name = Some(value);
            }
        }

        let prev_metrics_window = self.metrics_window;
        env_override("INTERVAL_SECS", &mut self.interval_secs);
        env_override("TRADING_PERIOD_SECS", &mut self.trading_period_secs);
        env_override("METRICS_WINDOW_LENGTH", &mut self.metrics_window);
        env_override(
            "ENTRY_Z_SCORE_BASE",
            &mut self.default_pair_params.entry_z_base,
        );
        env_override(
            "ENTRY_Z_SCORE_MIN",
            &mut self.default_pair_params.entry_z_min,
        );
        env_override(
            "ENTRY_Z_SCORE_MAX",
            &mut self.default_pair_params.entry_z_max,
        );
        env_override("EXIT_Z_SCORE", &mut self.default_pair_params.exit_z);
        env_override(
            "STOP_LOSS_Z_SCORE",
            &mut self.default_pair_params.stop_loss_z,
        );
        env_override(
            "FORCE_CLOSE_TIME_SECS",
            &mut self.default_pair_params.force_close_secs,
        );
        env_override("COOLDOWN_SECS", &mut self.default_pair_params.cooldown_secs);
        env_override(
            "STOP_LOSS_COOLDOWN_SECS",
            &mut self.default_pair_params.stop_loss_cooldown_secs,
        );
        env_override(
            "NET_FUNDING_MIN_PER_HOUR",
            &mut self.net_funding_min_per_hour,
        );
        env_override(
            "SPREAD_VELOCITY_MAX_SIGMA_PER_MIN",
            &mut self.default_pair_params.spread_velocity_max_sigma_per_min,
        );
        // bot-strategy#439: hard-fail on a parse error for the trading-
        // critical knobs. A typo on RISK_PCT_PER_TRADE / EQUITY_REFERENCE_USD
        // / MAX_LEVERAGE / FEE_BPS / SLIPPAGE_BPS used to silently revert to
        // the default and place real orders against the wrong risk model.
        env_override_critical("RISK_PCT_PER_TRADE", &mut self.risk_pct_per_trade);
        env_override(
            "MAX_LOSS_R_MULT",
            &mut self.default_pair_params.max_loss_r_mult,
        );
        env_override_critical("EQUITY_REFERENCE_USD", &mut self.equity_reference_usd);
        env_override(
            "PAIR_SELECTION_LOOKBACK_HOURS_SHORT",
            &mut self.default_pair_params.lookback_hours_short,
        );
        env_override(
            "PAIR_SELECTION_LOOKBACK_HOURS_LONG",
            &mut self.default_pair_params.lookback_hours_long,
        );
        env_override(
            "HALF_LIFE_MAX_HOURS",
            &mut self.default_pair_params.half_life_max_hours,
        );
        env_override(
            "ADF_P_THRESHOLD",
            &mut self.default_pair_params.adf_p_threshold,
        );
        env_override(
            "ENTRY_VOL_LOOKBACK_HOURS",
            &mut self.default_pair_params.entry_vol_lookback_hours,
        );
        // bot-strategy#439: SLIPPAGE_BPS feeds the size calculation; a silent
        // parse failure used to revert to default and place orders against the
        // wrong cost model.
        env_override_critical("SLIPPAGE_BPS", &mut self.slippage_bps);
        env_override_critical("FEE_BPS", &mut self.fee_bps);
        env_override_critical("MAX_LEVERAGE", &mut self.max_leverage);
        env_override(
            "REEVAL_JUMP_Z_MULT",
            &mut self.default_pair_params.reeval_jump_z_mult,
        );
        env_override(
            "VOL_SPIKE_MULT",
            &mut self.default_pair_params.vol_spike_mult,
        );
        env_override("MAX_ACTIVE_PAIRS", &mut self.max_active_pairs);
        env_override("WARM_START_MODE", &mut self.warm_start_mode);
        let mut warm_start_min_overridden = false;
        if let Ok(value) = env::var("WARM_START_MIN_BARS") {
            if let Ok(parsed) = value.parse() {
                self.default_pair_params.warm_start_min_bars = parsed;
                warm_start_min_overridden = true;
            }
        }
        if !warm_start_min_overridden
            && !warm_start_min_from_yaml
            && self.default_pair_params.warm_start_min_bars == prev_metrics_window
            && self.metrics_window != prev_metrics_window
        {
            self.default_pair_params.warm_start_min_bars = self.metrics_window;
        }
        env_override("ORDER_TIMEOUT_SECS", &mut self.order_timeout_secs);
        env_override(
            "ENTRY_PARTIAL_FILL_MAX_RETRIES",
            &mut self.entry_partial_fill_max_retries,
        );
        env_override(
            "ENTRY_PARTIAL_FILL_GIVEUP_RETRIES",
            &mut self.entry_partial_fill_giveup_retries,
        );
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
        if let Ok(value) = env::var("HISTORY_ARCHIVE_DIR") {
            self.history_archive_dir = if value.trim().is_empty() {
                None
            } else {
                Some(value.trim().to_string())
            };
        }
        env_override(
            "HISTORY_ARCHIVE_RETENTION_DAYS",
            &mut self.history_archive_retention_days,
        );

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
        if let Ok(value) = env::var("BT_WARM_START_SNAPSHOT") {
            if !value.trim().is_empty() {
                self.bt_warm_start_snapshot = Some(value);
            }
        }
        // BT eval-timestamp replay — see struct field doc.
        if env::var("BT_EVAL_TIMESTAMPS_FILE").is_ok() {
            self.bt_eval_timestamps = load_bt_eval_timestamps();
        }
        // BT restart-timestamp replay — see struct field doc.
        if env::var("BT_RESTART_TIMESTAMPS_FILE").is_ok() {
            self.bt_restart_timestamps = load_bt_restart_timestamps();
        }
        env_override("BT_FILL_DELAY_SECS", &mut self.bt_fill_delay_secs);
        // bot-strategy#531 ineligible-close book-quality guard.
        env_override(
            "INELIGIBLE_CLOSE_DEFER_CAP_SECS",
            &mut self.ineligible_close_defer_cap_secs,
        );
        env_override(
            "INELIGIBLE_CLOSE_DEFER_SPREAD_BPS",
            &mut self.ineligible_close_defer_spread_bps,
        );
        env_override(
            "INELIGIBLE_CLOSE_DEFER_STALE_SECS",
            &mut self.ineligible_close_defer_stale_secs,
        );
        // Per-tick regime series dump — see struct field doc.
        if let Ok(value) = env::var("BT_REGIME_SERIES_FILE") {
            if !value.trim().is_empty() {
                self.bt_regime_series_file = Some(value);
            }
        }

        env_override(
            "SPREAD_TREND_MAX_SLOPE_SIGMA",
            &mut self.default_pair_params.spread_trend_max_slope_sigma,
        );
        env_override(
            "BETA_DIVERGENCE_MAX",
            &mut self.default_pair_params.beta_divergence_max,
        );
        env_override(
            "CIRCUIT_BREAKER_TIER1_LOSSES",
            &mut self.default_pair_params.circuit_breaker_tier1_losses,
        );
        env_override(
            "CIRCUIT_BREAKER_TIER1_COOLDOWN_SECS",
            &mut self.default_pair_params.circuit_breaker_tier1_cooldown_secs,
        );
        env_override(
            "CIRCUIT_BREAKER_TIER2_LOSSES",
            &mut self.default_pair_params.circuit_breaker_tier2_losses,
        );
        env_override(
            "CIRCUIT_BREAKER_TIER2_COOLDOWN_SECS",
            &mut self.default_pair_params.circuit_breaker_tier2_cooldown_secs,
        );
        env_override(
            "ENTRY_POST_ONLY_TIMEOUT_SECS",
            &mut self.default_pair_params.entry_post_only_timeout_secs,
        );
        env_override(
            "EXIT_POST_ONLY_TIMEOUT_SECS",
            &mut self.default_pair_params.exit_post_only_timeout_secs,
        );
        // bot-strategy#463 Phase 1/2: re-hedge knobs. Without these
        // overrides the YAML-load path always uses the default
        // (`unwrap_or(0.0)`) and ignores the env, breaking BT sweeps.
        env_override(
            "REHEDGE_DRIFT_THRESHOLD_PCT",
            &mut self.default_pair_params.rehedge_drift_threshold_pct,
        );
        env_override(
            "REHEDGE_COOLDOWN_SECS",
            &mut self.default_pair_params.rehedge_cooldown_secs,
        );
        env_override(
            "REHEDGE_MIN_QTY_NOTIONAL_USD",
            &mut self.default_pair_params.rehedge_min_qty_notional_usd,
        );
        env_override(
            "REHEDGE_LIVE_ENABLED",
            &mut self.default_pair_params.rehedge_live_enabled,
        );
        env_override(
            "USE_AMEND_ON_PARTIAL_FILL",
            &mut self.default_pair_params.use_amend_on_partial_fill,
        );
        env_override(
            "REHEDGE_REQUIRE_NO_REVERT",
            &mut self.default_pair_params.rehedge_require_no_revert,
        );
        env_override(
            "REHEDGE_Z_NO_REVERT_FACTOR",
            &mut self.default_pair_params.rehedge_z_no_revert_factor,
        );
        env_override(
            "REHEDGE_VELOCITY_PROJECTED_DRIFT_MIN",
            &mut self
                .default_pair_params
                .rehedge_velocity_projected_drift_min,
        );
        env_override(
            "BETA_UNCERTAINTY_MAX",
            &mut self.default_pair_params.beta_uncertainty_max,
        );
        env_override(
            "ENTRY_VELOCITY_BLOCK_SIGMA_PER_MIN",
            &mut self.default_pair_params.entry_velocity_block_sigma_per_min,
        );
        env_override(
            "FUNDING_ENTRY_Z_SCALE",
            &mut self.default_pair_params.funding_entry_z_scale,
        );
        env_override(
            "BETA_GAP_ENTRY_Z_SCALE",
            &mut self.default_pair_params.beta_gap_entry_z_scale,
        );
        env_override(
            "BETA_GAP_NOTIONAL_SCALE",
            &mut self.default_pair_params.beta_gap_notional_scale,
        );
        env_override(
            "BETA_GAP_NOTIONAL_FLOOR",
            &mut self.default_pair_params.beta_gap_notional_floor,
        );
        env_override(
            "ENTRY_Z_SHORT_MULTIPLIER",
            &mut self.default_pair_params.entry_z_short_multiplier,
        );
        env_override(
            "DEPTH_SIZE_SLOPE",
            &mut self.default_pair_params.depth_size_slope,
        );
        env_override(
            "DEPTH_SIZE_MIN",
            &mut self.default_pair_params.depth_size_min,
        );
        env_override(
            "DEPTH_SIZE_MAX",
            &mut self.default_pair_params.depth_size_max,
        );
        if let Ok(value) = env::var("MTF_WINDOWS") {
            self.default_pair_params.mtf_windows = value
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
        }
        env_override("MTF_Z_MIN", &mut self.default_pair_params.mtf_z_min);
        env_override(
            "STD_COLLAPSE_WINDOW_BARS",
            &mut self.default_pair_params.std_collapse_window_bars,
        );
        env_override(
            "STD_COLLAPSE_MIN_RATIO",
            &mut self.default_pair_params.std_collapse_min_ratio,
        );
        env_override(
            "STD_COLLAPSE_HOLD_DOWN_SECS",
            &mut self.default_pair_params.std_collapse_hold_down_secs,
        );
        if let Ok(value) = env::var("STD_COLLAPSE_OBSERVE_ONLY") {
            let lower = value.trim().to_ascii_lowercase();
            self.default_pair_params.std_collapse_observe_only =
                matches!(lower.as_str(), "1" | "true" | "yes");
        }

        // bot-strategy#473: env override on the YAML-loaded path. Applies
        // to default_pair_params; per-strategy overrides still win at the
        // strategy override loop in mod.rs.
        if let Ok(value) = env::var("USE_FROZEN_BETA_EXIT_Z") {
            let lower = value.trim().to_ascii_lowercase();
            self.default_pair_params.use_frozen_beta_exit_z =
                matches!(lower.as_str(), "1" | "true" | "yes");
        }

        // bot-strategy#494: env override on the YAML-loaded path for the
        // persistent-regime entry gate. Default stays shadow-only.
        if let Ok(value) = env::var("REGIME_BLOCK_ENTRIES") {
            let lower = value.trim().to_ascii_lowercase();
            self.default_pair_params.regime_block_entries =
                matches!(lower.as_str(), "1" | "true" | "yes");
        }

        // Kalman filter
        if let Ok(value) = env::var("USE_KALMAN_BETA") {
            self.use_kalman_beta = value.to_lowercase() == "true";
        }
        env_override("KALMAN_Q", &mut self.kalman_q);
        env_override("KALMAN_R", &mut self.kalman_r);
        env_override("KALMAN_INITIAL_P", &mut self.kalman_initial_p);
        env_override("KALMAN_MIN_UPDATES", &mut self.kalman_min_updates);

        // Regime filter
        env_override("REGIME_VOL_WINDOW", &mut self.regime_vol_window);
        env_override("REGIME_VOL_MAX", &mut self.regime_vol_max);
        env_override("REGIME_TREND_WINDOW", &mut self.regime_trend_window);
        env_override("REGIME_TREND_MAX", &mut self.regime_trend_max);
        if let Ok(value) = env::var("REGIME_REFERENCE_SYMBOL") {
            if !value.trim().is_empty() {
                self.regime_reference_symbol = value;
            }
        }

        Ok(())
    }
}
