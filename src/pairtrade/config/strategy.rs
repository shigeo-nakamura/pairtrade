use std::env;

use super::schema::StrategyYaml;
use super::PairTradeConfig;

/// Resolved per-strategy config for one A/B/C variant. Fields here override
/// the top-level scalar of the same name when an instance runs.
#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub id: String,
    /// Per-instance agent name. Optional because legacy single-bot YAML
    /// (no `strategies:` block) inherits the top-level `agent_name`, which
    /// itself is optional. Drives status-file and PnL-log directory naming.
    pub agent_name: Option<String>,
    pub exit_z: f64,
    pub stop_loss_z: f64,
    pub max_loss_r_mult: f64,
    pub equity_reference_usd: f64,
    // Per-strategy PairParams overrides. `None` = inherit from top-level
    // resolved value; `Some` wins over the top-level scalar at instance build
    // time (see `pairtrade::mod::build_instance_default_params`).
    pub force_close_time_secs: Option<u64>,
    pub mtf_windows: Option<Vec<usize>>,
    pub mtf_z_min: Option<f64>,
    pub entry_z_base: Option<f64>,
    pub entry_z_min: Option<f64>,
    pub entry_z_max: Option<f64>,
    /// Per-strategy override of the global Phase 2 β-handling parameters
    /// (bot-strategy#461). `None` = inherit; `Some` overrides at instance
    /// build time in `pairtrade::mod`.
    pub beta_gap_entry_z_scale: Option<f64>,
    pub beta_gap_notional_scale: Option<f64>,
    pub beta_gap_notional_floor: Option<f64>,
    /// Per-strategy overrides for #515 signal-depth sizing.
    pub depth_size_slope: Option<f64>,
    pub depth_size_min: Option<f64>,
    pub depth_size_max: Option<f64>,
    /// Per-strategy overrides for #463 mid-hold re-hedge.
    pub rehedge_drift_threshold_pct: Option<f64>,
    pub rehedge_cooldown_secs: Option<u64>,
    pub rehedge_min_qty_notional_usd: Option<f64>,
    pub rehedge_live_enabled: Option<bool>,
    /// bot-strategy#471 per-strategy override for the entry partial-fill
    /// amend path (see PairParams::use_amend_on_partial_fill).
    pub use_amend_on_partial_fill: Option<bool>,
    pub rehedge_require_no_revert: Option<bool>,
    pub rehedge_z_no_revert_factor: Option<f64>,
    pub rehedge_velocity_projected_drift_min: Option<f64>,
    pub beta_uncertainty_max: Option<f64>,
    /// Per-strategy override of `std_collapse_hold_down_secs` (bot-strategy#500).
    pub std_collapse_hold_down_secs: Option<u64>,
    /// Per-strategy override of `use_frozen_beta_exit_z` (bot-strategy#473).
    pub use_frozen_beta_exit_z: Option<bool>,
    /// Per-strategy override of `regime_block_entries` (bot-strategy#494).
    pub regime_block_entries: Option<bool>,
}

/// Build the resolved `strategies: Vec<StrategyConfig>` for a `PairTradeConfig`.
///
/// If the YAML supplied a `strategies:` list, every entry becomes one
/// `StrategyConfig` and any unset field falls back to the resolved
/// top-level value already on `cfg`. If `yaml_strategies` is `None`
/// (legacy single-bot YAML, or env-only `from_env`), this returns a
/// single `StrategyConfig` derived entirely from the top-level scalars,
/// preserving today's behavior.
///
/// commit 2 of shigeo-nakamura/bot-strategy#25: parsing only — the
/// engine still runs `instances.len() == 1` and does not yet branch on
/// per-strategy values.
pub(super) fn resolve_strategies(
    cfg: &PairTradeConfig,
    yaml_strategies: Option<&[StrategyYaml]>,
) -> Vec<StrategyConfig> {
    let default_id = cfg
        .agent_name
        .clone()
        .unwrap_or_else(|| "default".to_string());
    match yaml_strategies {
        Some(list) if !list.is_empty() => list
            .iter()
            .enumerate()
            .map(|(idx, s)| {
                let id =
                    s.id.clone()
                        .or_else(|| s.agent_name.clone())
                        .unwrap_or_else(|| format!("strategy-{}", idx));
                // Per-strategy equity reference env override:
                // `EQUITY_REFERENCE_USD_<ID>` (id uppercased) takes precedence
                // over both the per-strategy yaml field and the top-level
                // reference. Lets one shared yaml deploy with different
                // per-instance reference equity per region.
                // bot-strategy#439: per-variant equity reference is trading-
                // critical (drives position sizing); a silent parse failure on
                // `EQUITY_REFERENCE_USD_<ID>` used to revert to whichever yaml
                // / top-level default landed first and could place 7× the
                // intended notional. Hard-fail on parse error instead.
                let equity_env_key = format!("EQUITY_REFERENCE_USD_{}", id.to_ascii_uppercase());
                let equity_reference_usd = match env::var(&equity_env_key) {
                    Err(_) => s.equity_usd_reference.unwrap_or(cfg.equity_reference_usd),
                    Ok(value) => match value.parse::<f64>() {
                        Ok(parsed) => parsed,
                        Err(e) => panic!(
                            "[CONFIG] trading-critical env {}={:?} failed to parse ({}); refusing to start. \
                             Fix the env var or unset it explicitly. (bot-strategy#439)",
                            equity_env_key, value, e
                        ),
                    },
                };
                StrategyConfig {
                    id,
                    agent_name: s.agent_name.clone().or_else(|| cfg.agent_name.clone()),
                    exit_z: s.exit_z_score.unwrap_or(cfg.default_pair_params.exit_z),
                    stop_loss_z: s
                        .stop_loss_z_score
                        .unwrap_or(cfg.default_pair_params.stop_loss_z),
                    max_loss_r_mult: s
                        .max_loss_r_mult
                        .unwrap_or(cfg.default_pair_params.max_loss_r_mult),
                    equity_reference_usd,
                    force_close_time_secs: s.force_close_time_secs,
                    mtf_windows: s.mtf_windows.clone(),
                    mtf_z_min: s.mtf_z_min,
                    entry_z_base: s.entry_z_score_base,
                    entry_z_min: s.entry_z_score_min,
                    entry_z_max: s.entry_z_score_max,
                    beta_gap_entry_z_scale: s.beta_gap_entry_z_scale,
                    beta_gap_notional_scale: s.beta_gap_notional_scale,
                    beta_gap_notional_floor: s.beta_gap_notional_floor,
                    depth_size_slope: s.depth_size_slope,
                    depth_size_min: s.depth_size_min,
                    depth_size_max: s.depth_size_max,
                    rehedge_drift_threshold_pct: s.rehedge_drift_threshold_pct,
                    rehedge_cooldown_secs: s.rehedge_cooldown_secs,
                    rehedge_min_qty_notional_usd: s.rehedge_min_qty_notional_usd,
                    rehedge_live_enabled: s.rehedge_live_enabled,
                    use_amend_on_partial_fill: s.use_amend_on_partial_fill,
                    rehedge_require_no_revert: s.rehedge_require_no_revert,
                    rehedge_z_no_revert_factor: s.rehedge_z_no_revert_factor,
                    rehedge_velocity_projected_drift_min: s.rehedge_velocity_projected_drift_min,
                    beta_uncertainty_max: s.beta_uncertainty_max,
                    std_collapse_hold_down_secs: s.std_collapse_hold_down_secs,
                    use_frozen_beta_exit_z: s.use_frozen_beta_exit_z,
                    regime_block_entries: s.regime_block_entries,
                }
            })
            .collect(),
        _ => vec![StrategyConfig {
            id: default_id,
            agent_name: cfg.agent_name.clone(),
            exit_z: cfg.default_pair_params.exit_z,
            stop_loss_z: cfg.default_pair_params.stop_loss_z,
            max_loss_r_mult: cfg.default_pair_params.max_loss_r_mult,
            equity_reference_usd: cfg.equity_reference_usd,
            force_close_time_secs: None,
            mtf_windows: None,
            mtf_z_min: None,
            entry_z_base: None,
            entry_z_min: None,
            entry_z_max: None,
            beta_gap_entry_z_scale: None,
            beta_gap_notional_scale: None,
            beta_gap_notional_floor: None,
            depth_size_slope: None,
            depth_size_min: None,
            depth_size_max: None,
            rehedge_drift_threshold_pct: None,
            rehedge_cooldown_secs: None,
            rehedge_min_qty_notional_usd: None,
            rehedge_live_enabled: None,
            use_amend_on_partial_fill: None,
            rehedge_require_no_revert: None,
            rehedge_z_no_revert_factor: None,
            rehedge_velocity_projected_drift_min: None,
            beta_uncertainty_max: None,
            std_collapse_hold_down_secs: None,
            use_frozen_beta_exit_z: None,
            regime_block_entries: None,
        }],
    }
}
