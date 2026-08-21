//! Cross-field config validation, split out of `config/mod.rs` (bot-strategy#502).
//!
//! `validate` enforces that `shutdown_grace_secs` covers the longest
//! `force_close_secs` (bot-strategy#50) and warns when a std-collapse
//! hold-down exceeds the retained std-history span (bot-strategy#500). Its
//! two private helpers are only reached from `validate`.

use anyhow::{anyhow, Result};

use super::super::defaults::ELIGIBILITY_BETA_GAP_MAX as RAW_BETA_GAP_MAX;
use super::PairTradeConfig;

impl PairTradeConfig {
    /// Largest `force_close_secs` across the resolved default, per-pair
    /// overrides, and per-strategy overrides. The graceful-shutdown grace
    /// window must exceed this, or a position can be prematurely flushed by
    /// shutdown before its own `force_close` would have closed it.
    fn max_force_close_secs(&self) -> u64 {
        let mut m = self.default_pair_params.force_close_secs;
        // `pair_params` is currently always empty in prod, but keep the
        // sweep so re-introducing per-pair overrides cannot silently break
        // shutdown_grace validation.
        for p in self.pair_params.values() {
            m = m.max(p.force_close_secs);
        }
        for s in &self.strategies {
            if let Some(fc) = s.force_close_time_secs {
                m = m.max(fc);
            }
        }
        m
    }

    fn warn_std_collapse_hold_down_cap(&self) {
        fn warn_scope(
            scope: &str,
            hold_down_secs: u64,
            window_bars: usize,
            trading_period_secs: u64,
        ) {
            if hold_down_secs == 0 || window_bars == 0 || trading_period_secs == 0 {
                return;
            }
            let max_effective_secs = (window_bars as u64).saturating_mul(trading_period_secs);
            if hold_down_secs > max_effective_secs {
                log::warn!(
                    "{} std_collapse_hold_down_secs={} exceeds retained std-history span {}s \
                     (std_collapse_window_bars={} * trading_period_secs={}); effective hold-down is capped. \
                     See bot-strategy#500.",
                    scope,
                    hold_down_secs,
                    max_effective_secs,
                    window_bars,
                    trading_period_secs,
                );
            }
        }

        warn_scope(
            "default_pair_params",
            self.default_pair_params.std_collapse_hold_down_secs,
            self.default_pair_params.std_collapse_window_bars,
            self.trading_period_secs,
        );
        for (pair, pp) in &self.pair_params {
            warn_scope(
                &format!("pair_params.{pair}"),
                pp.std_collapse_hold_down_secs,
                pp.std_collapse_window_bars,
                self.trading_period_secs,
            );
        }
        for strategy in &self.strategies {
            if let Some(hold_down_secs) = strategy.std_collapse_hold_down_secs {
                warn_scope(
                    &format!("strategies.{}", strategy.id),
                    hold_down_secs,
                    self.default_pair_params.std_collapse_window_bars,
                    self.trading_period_secs,
                );
            }
        }
    }

    /// Assert that `shutdown_grace_secs` covers the longest per-strategy /
    /// per-pair `force_close_secs` plus a small buffer. Catches config drift
    /// like bot-strategy#50, where a strategy's `force_close_time_secs` was
    /// extended without raising the global shutdown grace.
    pub(super) fn validate(&self) -> Result<()> {
        const BUFFER_SECS: u64 = 60;
        self.warn_std_collapse_hold_down_cap();
        if self.eligibility_margin_grace_secs < 0 {
            return Err(anyhow!(
                "eligibility_margin_grace_secs ({}) must be >= 0",
                self.eligibility_margin_grace_secs
            ));
        }
        if !self.eligibility_beta_gap_exit.is_finite()
            || self.eligibility_beta_gap_exit <= RAW_BETA_GAP_MAX
        {
            return Err(anyhow!(
                "eligibility_beta_gap_exit ({}) must be finite and > {:.2}",
                self.eligibility_beta_gap_exit,
                RAW_BETA_GAP_MAX
            ));
        }
        let beta_floor_enabled = self.strategies.iter().any(|strategy| {
            strategy
                .exit_on_sizing_beta_floor
                .unwrap_or(self.default_pair_params.exit_on_sizing_beta_floor)
        });
        if beta_floor_enabled {
            let validate_floor = |scope: &str, floor: f64| -> Result<()> {
                if !floor.is_finite() || floor <= 0.0 {
                    return Err(anyhow!(
                        "{} enables exit_on_sizing_beta_floor but sizing_beta_floor ({}) must be finite and > 0",
                        scope,
                        floor
                    ));
                }
                Ok(())
            };
            validate_floor(
                "default_pair_params",
                self.default_pair_params.sizing_beta_floor,
            )?;
            for (pair, params) in &self.pair_params {
                validate_floor(&format!("pair_params.{pair}"), params.sizing_beta_floor)?;
            }
        }
        // 0 = legacy immediate force-close on SIGTERM; no grace window to
        // validate.
        if self.shutdown_grace_secs == 0 {
            return Ok(());
        }
        let max_fc = self.max_force_close_secs();
        let required = max_fc.saturating_add(BUFFER_SECS);
        if self.shutdown_grace_secs < required {
            return Err(anyhow!(
                "shutdown_grace_secs ({}) is shorter than max force_close_time_secs ({}) + {}s buffer = {}. \
                 Graceful shutdown would force-close a position before its own force_close window expires (see bot-strategy#50).",
                self.shutdown_grace_secs,
                max_fc,
                BUFFER_SECS,
                required,
            ));
        }
        Ok(())
    }
}
