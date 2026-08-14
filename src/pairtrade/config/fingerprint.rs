//! Effective per-variant config fingerprint (bot-strategy#580).
//!
//! The 2026-06-15 silent config-drift incident (#491) showed there was no
//! observable signal anywhere that the *running* process's effective config
//! differed from the deployed/intended round config: the bot had been running
//! the pre-Round-6 `force_close_time_secs` for ~8 days because pairtrade CI
//! deploys configs without restarting (#269), so a freshly-deployed YAML was
//! never loaded by the running process.
//!
//! This module turns the resolved, effective per-variant trading parameters
//! (after YAML + env + per-strategy overrides) into a stable, human-comparable
//! summary string plus a short `sha256-12` fingerprint over a canonical
//! serialization, so the running config becomes observable via the `[CONFIG]`
//! startup log line and the `pairtrade_config_fingerprint` /
//! `pairtrade_effective_*` Prometheus gauges. A drift monitor / round-eval
//! preflight compares these against the intended round config and alerts within
//! minutes instead of at readout.

use sha2::{Digest, Sha256};

use super::{PairParams, StrategyConfig};

/// The trading-critical effective parameters that define a "round config" for
/// one A/B/C variant. Built from the fully-resolved `PairParams` overlay the
/// engine actually trades with (`StrategyConfig::apply_pair_param_overrides`
/// already applied) plus the per-strategy equity reference.
#[derive(Debug, Clone)]
pub struct EffectiveConfig {
    pub variant: String,
    pub force_close_secs: u64,
    pub exit_z: f64,
    pub stop_loss_z: f64,
    pub use_frozen_beta_exit_z: bool,
    pub equity_reference_usd: f64,
    pub max_leverage: f64,
    pub dry_run: bool,
    pub entry_z_base: f64,
    pub entry_z_min: f64,
    pub entry_z_max: f64,
    pub mtf_windows: Vec<usize>,
    pub mtf_z_min: f64,
    pub max_loss_r_mult: f64,
    pub regime_block_entries: bool,
    pub std_collapse_hold_down_secs: u64,
    pub use_amend_on_partial_fill: bool,
    pub beta_gap_entry_z_scale: f64,
    pub beta_gap_notional_scale: f64,
    pub beta_gap_notional_floor: f64,
    pub ineligible_close_defer_cap_secs: i64,
    pub ineligible_close_defer_spread_bps: f64,
    pub ineligible_close_defer_stale_secs: i64,
    pub eligibility_margin_grace_secs: i64,
    pub eligibility_beta_gap_exit: f64,
}

impl EffectiveConfig {
    /// Build from the effective per-variant `PairParams` (overrides already
    /// applied) and the variant's `StrategyConfig` / process-wide scalars.
    pub fn from_resolved(
        strategy: &StrategyConfig,
        effective: &PairParams,
        max_leverage: f64,
        dry_run: bool,
        ineligible_close_defer_cap_secs: i64,
        ineligible_close_defer_spread_bps: f64,
        ineligible_close_defer_stale_secs: i64,
    ) -> Self {
        Self {
            variant: strategy.id.clone(),
            force_close_secs: effective.force_close_secs,
            exit_z: effective.exit_z,
            stop_loss_z: effective.stop_loss_z,
            use_frozen_beta_exit_z: effective.use_frozen_beta_exit_z,
            equity_reference_usd: strategy.equity_reference_usd,
            max_leverage,
            dry_run,
            entry_z_base: effective.entry_z_base,
            entry_z_min: effective.entry_z_min,
            entry_z_max: effective.entry_z_max,
            mtf_windows: effective.mtf_windows.clone(),
            mtf_z_min: effective.mtf_z_min,
            max_loss_r_mult: effective.max_loss_r_mult,
            regime_block_entries: effective.regime_block_entries,
            std_collapse_hold_down_secs: effective.std_collapse_hold_down_secs,
            use_amend_on_partial_fill: effective.use_amend_on_partial_fill,
            beta_gap_entry_z_scale: effective.beta_gap_entry_z_scale,
            beta_gap_notional_scale: effective.beta_gap_notional_scale,
            beta_gap_notional_floor: effective.beta_gap_notional_floor,
            ineligible_close_defer_cap_secs,
            ineligible_close_defer_spread_bps,
            ineligible_close_defer_stale_secs,
            // Defaults preserve legacy behavior; the engine constructor
            // overwrites these from the fully resolved process config.
            eligibility_margin_grace_secs: 0,
            eligibility_beta_gap_exit: 0.25,
        }
    }

    pub fn with_eligibility_margin_grace(mut self, grace_secs: i64, beta_gap_exit: f64) -> Self {
        self.eligibility_margin_grace_secs = grace_secs;
        self.eligibility_beta_gap_exit = beta_gap_exit;
        self
    }

    /// Canonical, deterministic key=value serialization over the fingerprinted
    /// fields. Floats are formatted at fixed precision so representation noise
    /// can't perturb the hash; the field order is fixed and append-only so a
    /// committed round fingerprint stays comparable across releases. The
    /// `variant` id is intentionally excluded — the fingerprint describes the
    /// *parameter set*, so A and B are expected to differ only when their
    /// parameters differ.
    fn canonical(&self) -> String {
        let mtf = self
            .mtf_windows
            .iter()
            .map(|w| w.to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "force_close_secs={};exit_z={:.6};stop_loss_z={:.6};use_frozen_beta_exit_z={};\
             equity_reference_usd={:.6};max_leverage={:.6};dry_run={};\
             entry_z_base={:.6};entry_z_min={:.6};entry_z_max={:.6};\
             mtf_windows={};mtf_z_min={:.6};max_loss_r_mult={:.6};\
             regime_block_entries={};std_collapse_hold_down_secs={};\
             use_amend_on_partial_fill={};beta_gap_entry_z_scale={:.6};\
             beta_gap_notional_scale={:.6};beta_gap_notional_floor={:.6};\
             ineligible_close_defer_cap_secs={};\
             ineligible_close_defer_spread_bps={:.6};\
             ineligible_close_defer_stale_secs={};\
             eligibility_margin_grace_secs={};\
             eligibility_beta_gap_exit={:.6}",
            self.force_close_secs,
            self.exit_z,
            self.stop_loss_z,
            self.use_frozen_beta_exit_z,
            self.equity_reference_usd,
            self.max_leverage,
            self.dry_run,
            self.entry_z_base,
            self.entry_z_min,
            self.entry_z_max,
            mtf,
            self.mtf_z_min,
            self.max_loss_r_mult,
            self.regime_block_entries,
            self.std_collapse_hold_down_secs,
            self.use_amend_on_partial_fill,
            self.beta_gap_entry_z_scale,
            self.beta_gap_notional_scale,
            self.beta_gap_notional_floor,
            self.ineligible_close_defer_cap_secs,
            self.ineligible_close_defer_spread_bps,
            self.ineligible_close_defer_stale_secs,
            self.eligibility_margin_grace_secs,
            self.eligibility_beta_gap_exit,
        )
    }

    /// First 12 hex chars of the SHA-256 over `canonical()`. Short enough to
    /// eyeball in a log line / Prometheus label, wide enough (48 bits) that an
    /// accidental config collision is not a practical concern.
    pub fn fingerprint(&self) -> String {
        sha256_12(self.canonical().as_bytes())
    }

    /// The human-readable `[CONFIG]` startup line for this variant. Matches the
    /// format documented in bot-strategy#580.
    pub fn log_line(&self) -> String {
        format!(
            "[CONFIG] variant={} force_close={} exit_z={} stop_loss_z={} frozen_beta={} \
             equity_ref={} max_leverage={} dry_run={} inelig_defer_cap={} elig_margin_grace={} elig_beta_exit={} fp={}",
            self.variant,
            self.force_close_secs,
            self.exit_z,
            self.stop_loss_z,
            self.use_frozen_beta_exit_z,
            self.equity_reference_usd,
            self.max_leverage,
            self.dry_run,
            self.ineligible_close_defer_cap_secs,
            self.eligibility_margin_grace_secs,
            self.eligibility_beta_gap_exit,
            self.fingerprint(),
        )
    }
}

/// First 12 hex chars of `sha256(bytes)`. Shared by the effective-config
/// fingerprint and the on-disk config-file hash so both surface in the same
/// short form.
pub fn sha256_12(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut s = String::with_capacity(12);
    for b in digest.iter().take(6) {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut s = String::with_capacity(64);
    for b in digest {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_params() -> PairParams {
        let mut p = PairParams::default();
        p.force_close_secs = 10800;
        p.exit_z = 0.2;
        p.stop_loss_z = 6.0;
        p.use_frozen_beta_exit_z = false;
        p
    }

    fn sample_strategy(id: &str) -> StrategyConfig {
        StrategyConfig {
            id: id.to_string(),
            agent_name: None,
            exit_z: 0.2,
            stop_loss_z: 6.0,
            max_loss_r_mult: 2.0,
            equity_reference_usd: 1000.0,
            force_close_time_secs: Some(10800),
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
            use_frozen_beta_exit_z: Some(false),
            regime_block_entries: None,
        }
    }

    #[test]
    fn sha256_12_is_twelve_hex_chars_and_matches_known_vector() {
        // echo -n "" | sha256sum -> e3b0c44298fc...
        assert_eq!(sha256_12(b""), "e3b0c44298fc");
        assert_eq!(sha256_12(b"abc"), "ba7816bf8f01");
    }

    #[test]
    fn fingerprint_is_stable_and_variant_independent() {
        let p = sample_params();
        let a = EffectiveConfig::from_resolved(&sample_strategy("a"), &p, 5.0, true, 0, 20.0, 30);
        let b = EffectiveConfig::from_resolved(&sample_strategy("b"), &p, 5.0, true, 0, 20.0, 30);
        // Same parameter set under different variant ids -> identical fp.
        assert_eq!(a.fingerprint(), b.fingerprint());
        assert_eq!(a.fingerprint().len(), 12);
    }

    #[test]
    fn force_close_change_moves_the_fingerprint() {
        // The exact drift that went undetected for 8 days in #491: an fc flip
        // must change the fingerprint.
        let mut p7200 = sample_params();
        p7200.force_close_secs = 7200;
        let p10800 = sample_params();
        let fp_7200 =
            EffectiveConfig::from_resolved(&sample_strategy("a"), &p7200, 5.0, true, 0, 20.0, 30)
                .fingerprint();
        let fp_10800 =
            EffectiveConfig::from_resolved(&sample_strategy("a"), &p10800, 5.0, true, 0, 20.0, 30)
                .fingerprint();
        assert_ne!(fp_7200, fp_10800);
    }

    #[test]
    fn frozen_beta_change_moves_the_fingerprint() {
        let p_off = sample_params();
        let mut p_on = sample_params();
        p_on.use_frozen_beta_exit_z = true;
        let fp_off =
            EffectiveConfig::from_resolved(&sample_strategy("c"), &p_off, 5.0, true, 0, 20.0, 30)
                .fingerprint();
        let fp_on =
            EffectiveConfig::from_resolved(&sample_strategy("c"), &p_on, 5.0, true, 0, 20.0, 30)
                .fingerprint();
        assert_ne!(fp_off, fp_on);
    }

    #[test]
    fn ineligible_defer_change_moves_the_fingerprint() {
        // The guard's knobs change live close timing (0 disables it, 300 can
        // hold closes), so guard-off vs guard-on must never share a
        // fingerprint (PR #166 Codex review).
        let p = sample_params();
        let fp_off =
            EffectiveConfig::from_resolved(&sample_strategy("a"), &p, 5.0, true, 0, 20.0, 30)
                .fingerprint();
        let fp_on =
            EffectiveConfig::from_resolved(&sample_strategy("a"), &p, 5.0, true, 300, 20.0, 30)
                .fingerprint();
        let fp_spread =
            EffectiveConfig::from_resolved(&sample_strategy("a"), &p, 5.0, true, 300, 40.0, 30)
                .fingerprint();
        let fp_stale =
            EffectiveConfig::from_resolved(&sample_strategy("a"), &p, 5.0, true, 300, 20.0, 60)
                .fingerprint();
        assert_ne!(fp_off, fp_on);
        assert_ne!(fp_on, fp_spread);
        assert_ne!(fp_on, fp_stale);
    }

    #[test]
    fn eligibility_margin_change_moves_the_fingerprint() {
        let p = sample_params();
        let base =
            EffectiveConfig::from_resolved(&sample_strategy("a"), &p, 5.0, true, 0, 20.0, 30);
        let fp_off = base
            .clone()
            .with_eligibility_margin_grace(0, 0.25)
            .fingerprint();
        let fp_grace = base
            .clone()
            .with_eligibility_margin_grace(60, 0.25)
            .fingerprint();
        let fp_exit = base.with_eligibility_margin_grace(60, 0.30).fingerprint();
        assert_ne!(fp_off, fp_grace);
        assert_ne!(fp_grace, fp_exit);
    }

    #[test]
    fn log_line_carries_the_documented_fields() {
        let p = sample_params();
        let line =
            EffectiveConfig::from_resolved(&sample_strategy("a"), &p, 5.0, true, 0, 20.0, 30)
                .log_line();
        assert!(line.starts_with("[CONFIG] variant=a "));
        assert!(line.contains("force_close=10800"));
        assert!(line.contains("frozen_beta=false"));
        assert!(line.contains("equity_ref=1000"));
        assert!(line.contains("inelig_defer_cap=0"));
        assert!(line.contains("fp="));
    }
}
