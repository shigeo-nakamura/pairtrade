use super::risk::resolve_risk_config;
use super::schema::RiskYaml;
use super::*;

#[test]
fn risk_config_defaults_when_block_absent() {
    let cfg = resolve_risk_config(None).unwrap();
    assert_eq!(cfg.max_daily_loss_bps, 0);
    assert_eq!(cfg.max_session_loss_bps, 0);
    assert_eq!(cfg.max_notional_headroom, 0.0);
    // bot-strategy#575 ①: capital-event detection is on by default at a
    // small USD floor, with a 60 s settle dwell.
    assert!((cfg.session_dd_capital_event_min_usd - 5.0).abs() < 1e-9);
    assert_eq!(cfg.session_dd_capital_settle_secs, 60);
    assert!(matches!(cfg.max_daily_loss_action, DailyLossAction::Block));
}

#[test]
fn hyperliquid_observer_config_parses() {
    let vars = [
        "DEX_NAME",
        "DRY_RUN",
        "FEE_BPS",
        "REST_ENDPOINT",
        "UNIVERSE_PAIRS",
        "UNIVERSE_SYMBOLS",
    ];
    let saved: Vec<_> = vars
        .iter()
        .map(|name| ((*name).to_string(), std::env::var(name).ok()))
        .collect();
    for name in vars {
        std::env::remove_var(name);
    }

    let cfg =
        PairTradeConfig::from_yaml_path("configs/pairtrade/debot-pair-hyperliquid-observe.yaml")
            .expect("hyperliquid observer yaml load");

    assert_eq!(cfg.dex_name, "hyperliquid");
    assert!(cfg.dry_run);
    assert!(cfg.observe_only);
    assert!(!cfg.force_close_on_startup);
    assert_eq!(cfg.max_active_pairs, 2);
    assert!(
        cfg.fee_bps > 0.0,
        "observer must keep post-only pricing enabled"
    );
    assert_eq!(cfg.universe.len(), 2);
    assert_eq!(cfg.universe[0].base, "BTC");
    assert_eq!(cfg.universe[0].quote, "ETH");
    assert_eq!(cfg.universe[1].base, "SOL");
    assert_eq!(cfg.universe[1].quote, "ETH");

    for (name, value) in saved {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }
}

#[test]
fn eligibility_margin_grace_yaml_resolves_defaults_and_validates_bounds() {
    let env_names = ["ELIGIBILITY_MARGIN_GRACE_SECS", "ELIGIBILITY_BETA_GAP_EXIT"];
    let saved: Vec<_> = env_names
        .iter()
        .map(|name| ((*name).to_string(), std::env::var(name).ok()))
        .collect();
    for name in env_names {
        std::env::remove_var(name);
    }

    let path = std::env::temp_dir().join(format!(
        "pairtrade_eligibility_margin_grace_{}.yaml",
        std::process::id()
    ));
    let base = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
"#;
    std::fs::write(&path, base).unwrap();
    let defaults = PairTradeConfig::from_yaml_path(&path).expect("default yaml load");
    assert_eq!(defaults.eligibility_margin_grace_secs, 0);
    assert!((defaults.eligibility_beta_gap_exit - 0.25).abs() < 1e-12);

    std::fs::write(
        &path,
        format!("{base}eligibility_margin_grace_secs: 60\neligibility_beta_gap_exit: 0.25\n"),
    )
    .unwrap();
    let enabled = PairTradeConfig::from_yaml_path(&path).expect("enabled yaml load");
    assert_eq!(enabled.eligibility_margin_grace_secs, 60);
    assert!((enabled.eligibility_beta_gap_exit - 0.25).abs() < 1e-12);

    std::fs::write(
        &path,
        format!("{base}eligibility_margin_grace_secs: 60\neligibility_beta_gap_exit: 0.20\n"),
    )
    .unwrap();
    assert!(
        PairTradeConfig::from_yaml_path(&path).is_err(),
        "the raw 0.20 boundary would make the grace interval empty"
    );

    std::fs::write(
        &path,
        format!("{base}eligibility_margin_grace_secs: 60\neligibility_beta_gap_exit: 0.19\n"),
    )
    .unwrap();
    assert!(PairTradeConfig::from_yaml_path(&path).is_err());

    let _ = std::fs::remove_file(&path);
    for (name, value) in saved {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }
}

#[test]
fn risk_config_resolves_capital_event_fields() {
    let yaml = RiskYaml {
        session_dd_capital_event_min_usd: Some(25.0),
        session_dd_capital_settle_secs: Some(0),
        ..RiskYaml::default()
    };
    let cfg = resolve_risk_config(Some(&yaml)).unwrap();
    assert!((cfg.session_dd_capital_event_min_usd - 25.0).abs() < 1e-9);
    assert_eq!(cfg.session_dd_capital_settle_secs, 0);
}

#[test]
fn risk_config_rejects_negative_capital_event_min() {
    let yaml = RiskYaml {
        session_dd_capital_event_min_usd: Some(-1.0),
        ..RiskYaml::default()
    };
    assert!(resolve_risk_config(Some(&yaml)).is_err());
}

#[test]
fn risk_config_resolves_phase3_fields() {
    let yaml = RiskYaml {
        max_session_loss_bps: Some(500),
        session_dd_lookback_secs: Some(1_209_600), // 14 d
        session_dd_sample_secs: Some(1_800),       // 30 m
        max_notional_headroom: Some(1.1),
        ..RiskYaml::default()
    };
    let cfg = resolve_risk_config(Some(&yaml)).unwrap();
    assert_eq!(cfg.max_session_loss_bps, 500);
    assert_eq!(cfg.session_dd_lookback_secs, 1_209_600);
    assert_eq!(cfg.session_dd_sample_secs, 1_800);
    assert!((cfg.max_notional_headroom - 1.1).abs() < 1e-9);
}

#[test]
fn risk_config_rejects_negative_headroom() {
    let yaml = RiskYaml {
        max_notional_headroom: Some(-1.0),
        ..RiskYaml::default()
    };
    assert!(resolve_risk_config(Some(&yaml)).is_err());
}

#[test]
fn risk_config_rejects_headroom_that_looks_like_dollars() {
    // Old schema took an absolute USD cap (e.g. 5000). Catch operators
    // copy-pasting the old value into the new field name.
    let yaml = RiskYaml {
        max_notional_headroom: Some(5_000.0),
        ..RiskYaml::default()
    };
    assert!(resolve_risk_config(Some(&yaml)).is_err());
}

#[test]
fn risk_config_rejects_zero_sample_cadence() {
    let yaml = RiskYaml {
        session_dd_sample_secs: Some(0),
        ..RiskYaml::default()
    };
    assert!(resolve_risk_config(Some(&yaml)).is_err());
}

#[test]
fn risk_config_rejects_lookback_smaller_than_sample() {
    let yaml = RiskYaml {
        session_dd_sample_secs: Some(3_600),
        session_dd_lookback_secs: Some(60), // would never include even one sample
        ..RiskYaml::default()
    };
    assert!(resolve_risk_config(Some(&yaml)).is_err());
}

#[test]
fn risk_config_still_rejects_phase3_flatten_action() {
    // Sanity check: Phase 3 plumbing didn't accidentally enable
    // `max_daily_loss_action: flatten` (kept as Phase-3 follow-up
    // separate from session DD halt; daily DD remains block-only).
    let yaml = RiskYaml {
        max_daily_loss_action: Some("flatten".to_string()),
        ..RiskYaml::default()
    };
    assert!(resolve_risk_config(Some(&yaml)).is_err());
}

#[test]
fn history_archive_env_overrides_yaml() {
    use std::io::Write;
    let dir = std::env::temp_dir();
    let path = dir.join("pairtrade_history_archive_env.yaml");
    let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
history_archive_dir: /yaml/archive
history_archive_retention_days: 12
"#;
    std::fs::File::create(&path)
        .unwrap()
        .write_all(yaml.as_bytes())
        .unwrap();

    let prev_dir = std::env::var("HISTORY_ARCHIVE_DIR").ok();
    let prev_retention = std::env::var("HISTORY_ARCHIVE_RETENTION_DAYS").ok();
    std::env::set_var("HISTORY_ARCHIVE_DIR", "/env/archive");
    std::env::set_var("HISTORY_ARCHIVE_RETENTION_DAYS", "34");

    let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");
    assert_eq!(
        cfg.history_archive_dir.as_deref(),
        Some("/env/archive"),
        "env archive dir overrides yaml"
    );
    assert_eq!(cfg.history_archive_retention_days, 34);

    match prev_dir {
        Some(v) => std::env::set_var("HISTORY_ARCHIVE_DIR", v),
        None => std::env::remove_var("HISTORY_ARCHIVE_DIR"),
    }
    match prev_retention {
        Some(v) => std::env::set_var("HISTORY_ARCHIVE_RETENTION_DAYS", v),
        None => std::env::remove_var("HISTORY_ARCHIVE_RETENTION_DAYS"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn per_strategy_equity_env_override() {
    use std::io::Write;
    let dir = std::env::temp_dir();
    let path = dir.join("pairtrade_per_strategy_equity_env.yaml");
    let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
equity_usd_reference: 1000
strategies:
  - id: a
    equity_usd_reference: 1000
  - id: b
    equity_usd_reference: 500
  - id: c
    equity_usd_reference: 500
"#;
    std::fs::File::create(&path)
        .unwrap()
        .write_all(yaml.as_bytes())
        .unwrap();

    let prev_a = std::env::var("EQUITY_REFERENCE_USD_A").ok();
    let prev_b = std::env::var("EQUITY_REFERENCE_USD_B").ok();
    let prev_c = std::env::var("EQUITY_REFERENCE_USD_C").ok();

    std::env::set_var("EQUITY_REFERENCE_USD_A", "250");
    std::env::set_var("EQUITY_REFERENCE_USD_B", "250");
    std::env::remove_var("EQUITY_REFERENCE_USD_C");

    let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");
    let by_id = |id: &str| {
        cfg.strategies
            .iter()
            .find(|s| s.id == id)
            .unwrap_or_else(|| panic!("missing strategy {id}"))
            .equity_reference_usd
    };
    assert!((by_id("a") - 250.0).abs() < 1e-9, "A env override applied");
    assert!((by_id("b") - 250.0).abs() < 1e-9, "B env override applied");
    assert!(
        (by_id("c") - 500.0).abs() < 1e-9,
        "C unset env falls through to yaml per-strategy value"
    );

    // Restore so other tests in the same process see clean state.
    match prev_a {
        Some(v) => std::env::set_var("EQUITY_REFERENCE_USD_A", v),
        None => std::env::remove_var("EQUITY_REFERENCE_USD_A"),
    }
    match prev_b {
        Some(v) => std::env::set_var("EQUITY_REFERENCE_USD_B", v),
        None => std::env::remove_var("EQUITY_REFERENCE_USD_B"),
    }
    if let Some(v) = prev_c {
        std::env::set_var("EQUITY_REFERENCE_USD_C", v);
    }
    let _ = std::fs::remove_file(&path);
}

// bot-strategy#814: per-strategy max_leverage, mirroring
// per_strategy_equity_env_override above (YAML per-arm value, env override
// takes precedence, unset arm falls back to top-level).
#[test]
fn per_strategy_leverage_yaml_and_env_override() {
    use std::io::Write;
    let dir = std::env::temp_dir();
    let path = dir.join("pairtrade_per_strategy_leverage.yaml");
    let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
max_leverage: 20
strategies:
  - id: a
    max_leverage: 30
  - id: b
    max_leverage: 50
  - id: c
"#;
    std::fs::File::create(&path)
        .unwrap()
        .write_all(yaml.as_bytes())
        .unwrap();

    let prev_a = std::env::var("MAX_LEVERAGE_A").ok();
    std::env::set_var("MAX_LEVERAGE_A", "40");
    std::env::remove_var("MAX_LEVERAGE_B");
    std::env::remove_var("MAX_LEVERAGE_C");

    let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");
    let by_id = |id: &str| {
        cfg.strategies
            .iter()
            .find(|s| s.id == id)
            .unwrap_or_else(|| panic!("missing strategy {id}"))
            .max_leverage
    };
    assert!(
        (by_id("a") - 40.0).abs() < 1e-9,
        "A: env override wins over yaml per-strategy value"
    );
    assert!(
        (by_id("b") - 50.0).abs() < 1e-9,
        "B: no env override, yaml per-strategy value applies"
    );
    assert!(
        (by_id("c") - 20.0).abs() < 1e-9,
        "C: no override anywhere, inherits top-level max_leverage"
    );

    match prev_a {
        Some(v) => std::env::set_var("MAX_LEVERAGE_A", v),
        None => std::env::remove_var("MAX_LEVERAGE_A"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn per_strategy_entry_z_override_resolves() {
    use std::io::Write;
    let dir = std::env::temp_dir();
    let path = dir.join("pairtrade_per_strategy_entry_z.yaml");
    let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
entry_z_score_base: 1.5
entry_z_score_min: 1.0
entry_z_score_max: 2.0
strategies:
  - id: a
  - id: c
    entry_z_score_base: 2.5
    entry_z_score_min: 2.0
    entry_z_score_max: 3.0
"#;
    std::fs::File::create(&path)
        .unwrap()
        .write_all(yaml.as_bytes())
        .unwrap();

    let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");
    let by_id = |id: &str| {
        cfg.strategies
            .iter()
            .find(|s| s.id == id)
            .unwrap_or_else(|| panic!("missing strategy {id}"))
            .clone()
    };
    let a = by_id("a");
    assert!(a.entry_z_base.is_none(), "A inherits top-level (None)");
    assert!(a.entry_z_min.is_none());
    assert!(a.entry_z_max.is_none());
    let c = by_id("c");
    assert_eq!(c.entry_z_base, Some(2.5), "C overrides entry_z_base");
    assert_eq!(c.entry_z_min, Some(2.0));
    assert_eq!(c.entry_z_max, Some(3.0));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn per_strategy_std_collapse_hold_down_override_resolves() {
    use std::io::Write;
    let dir = std::env::temp_dir();
    let path = dir.join("pairtrade_per_strategy_std_hold_down.yaml");
    let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
std_collapse_hold_down_secs: 0
strategies:
  - id: a
  - id: c
    std_collapse_hold_down_secs: 3600
"#;
    std::fs::File::create(&path)
        .unwrap()
        .write_all(yaml.as_bytes())
        .unwrap();

    let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");
    assert_eq!(cfg.default_pair_params.std_collapse_hold_down_secs, 0);

    let by_id = |id: &str| {
        cfg.strategies
            .iter()
            .find(|s| s.id == id)
            .unwrap_or_else(|| panic!("missing strategy {id}"))
            .clone()
    };

    assert_eq!(by_id("c").std_collapse_hold_down_secs, Some(3600));
    assert!(by_id("a").std_collapse_hold_down_secs.is_none());

    let global = cfg.default_pair_params.std_collapse_hold_down_secs;
    let resolved = |id: &str| by_id(id).std_collapse_hold_down_secs.unwrap_or(global);
    assert_eq!(resolved("a"), 0);
    assert_eq!(resolved("c"), 3600);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn per_strategy_regime_block_entries_override_resolves() {
    // bot-strategy#494 Phase 1: on the single-process A/B/C layout, a single
    // challenger must be able to opt into the regime entry-gate while the
    // control variants stay on the global default (false). This guards the
    // 4-site plumbing (StrategyYaml -> StrategyConfig -> mod.rs overlay)
    // against the silent-global-inherit trap (memory: strategy_yaml_silent_drop).
    use std::io::Write;
    let dir = std::env::temp_dir();
    let path = dir.join("pairtrade_per_strategy_regime_block.yaml");
    let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
strategies:
  - id: a
  - id: b
  - id: c
    regime_block_entries: true
"#;
    std::fs::File::create(&path)
        .unwrap()
        .write_all(yaml.as_bytes())
        .unwrap();

    let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");

    // Top-level default stays false (shadow-only) when no global override set.
    assert!(
        !cfg.default_pair_params.regime_block_entries,
        "global default must be false (shadow-only)"
    );

    let by_id = |id: &str| {
        cfg.strategies
            .iter()
            .find(|s| s.id == id)
            .unwrap_or_else(|| panic!("missing strategy {id}"))
            .clone()
    };

    // Only the challenger carries the per-strategy override; controls inherit.
    assert_eq!(
        by_id("c").regime_block_entries,
        Some(true),
        "C opts in via per-strategy override"
    );
    assert!(
        by_id("a").regime_block_entries.is_none(),
        "A inherits the global default (None at the override layer)"
    );
    assert!(
        by_id("b").regime_block_entries.is_none(),
        "B inherits the global default (None at the override layer)"
    );

    // Reproduce the mod.rs overlay resolution to assert the final per-variant
    // boolean: C blocks while A/B remain false.
    let global = cfg.default_pair_params.regime_block_entries;
    let resolved = |id: &str| by_id(id).regime_block_entries.unwrap_or(global);
    assert!(resolved("c"), "C resolves to regime_block_entries = true");
    assert!(!resolved("a"), "A resolves to false (control)");
    assert!(!resolved("b"), "B resolves to false (control)");

    let _ = std::fs::remove_file(&path);
}

fn strategy_config_for_overlay_test() -> StrategyConfig {
    StrategyConfig {
        id: "test".to_string(),
        agent_name: None,
        exit_z: 1.25,
        stop_loss_z: 6.5,
        max_loss_r_mult: 2.5,
        equity_reference_usd: 100.0,
        max_leverage: 1.0,
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
    }
}

#[test]
fn strategy_pair_param_overlay_applies_all_strategy_overrides() {
    let mut strategy = strategy_config_for_overlay_test();
    strategy.force_close_time_secs = Some(123);
    strategy.mtf_windows = Some(vec![5, 15]);
    strategy.mtf_z_min = Some(0.7);
    strategy.entry_z_base = Some(2.1);
    strategy.entry_z_min = Some(1.2);
    strategy.entry_z_max = Some(3.4);
    strategy.beta_gap_entry_z_scale = Some(0.11);
    strategy.beta_gap_notional_scale = Some(0.22);
    strategy.beta_gap_notional_floor = Some(0.33);
    strategy.depth_size_slope = Some(0.44);
    strategy.depth_size_min = Some(0.55);
    strategy.depth_size_max = Some(1.66);
    strategy.rehedge_drift_threshold_pct = Some(0.77);
    strategy.rehedge_cooldown_secs = Some(88);
    strategy.rehedge_min_qty_notional_usd = Some(99.0);
    strategy.rehedge_live_enabled = Some(true);
    strategy.use_amend_on_partial_fill = Some(true);
    strategy.rehedge_require_no_revert = Some(true);
    strategy.rehedge_z_no_revert_factor = Some(1.2);
    strategy.rehedge_velocity_projected_drift_min = Some(0.03);
    strategy.beta_uncertainty_max = Some(0.04);
    strategy.std_collapse_hold_down_secs = Some(3600);
    strategy.use_frozen_beta_exit_z = Some(true);
    strategy.regime_block_entries = Some(true);

    let mut params = PairParams::default();
    strategy.apply_pair_param_overrides(&mut params);

    assert_eq!(params.exit_z, 1.25);
    assert_eq!(params.stop_loss_z, 6.5);
    assert_eq!(params.max_loss_r_mult, 2.5);
    assert_eq!(params.force_close_secs, 123);
    assert_eq!(params.mtf_windows, vec![5, 15]);
    assert_eq!(params.mtf_z_min, 0.7);
    assert_eq!(params.entry_z_base, 2.1);
    assert_eq!(params.entry_z_min, 1.2);
    assert_eq!(params.entry_z_max, 3.4);
    assert_eq!(params.beta_gap_entry_z_scale, 0.11);
    assert_eq!(params.beta_gap_notional_scale, 0.22);
    assert_eq!(params.beta_gap_notional_floor, 0.33);
    assert_eq!(params.depth_size_slope, 0.44);
    assert_eq!(params.depth_size_min, 0.55);
    assert_eq!(params.depth_size_max, 1.66);
    assert_eq!(params.rehedge_drift_threshold_pct, 0.77);
    assert_eq!(params.rehedge_cooldown_secs, 88);
    assert_eq!(params.rehedge_min_qty_notional_usd, 99.0);
    assert!(params.rehedge_live_enabled);
    assert!(params.use_amend_on_partial_fill);
    assert!(params.rehedge_require_no_revert);
    assert_eq!(params.rehedge_z_no_revert_factor, 1.2);
    assert_eq!(params.rehedge_velocity_projected_drift_min, 0.03);
    assert_eq!(params.beta_uncertainty_max, 0.04);
    assert_eq!(params.std_collapse_hold_down_secs, 3600);
    assert!(params.use_frozen_beta_exit_z);
    assert!(params.regime_block_entries);
}

#[test]
fn strategy_pair_param_overlay_preserves_inherited_optionals() {
    let strategy = strategy_config_for_overlay_test();
    let mut params = PairParams {
        force_close_secs: 777,
        mtf_windows: vec![60],
        entry_z_base: 1.8,
        beta_gap_entry_z_scale: 0.5,
        rehedge_live_enabled: true,
        std_collapse_hold_down_secs: 120,
        use_frozen_beta_exit_z: true,
        regime_block_entries: true,
        ..PairParams::default()
    };

    strategy.apply_pair_param_overrides(&mut params);

    assert_eq!(params.exit_z, 1.25);
    assert_eq!(params.stop_loss_z, 6.5);
    assert_eq!(params.max_loss_r_mult, 2.5);
    assert_eq!(params.force_close_secs, 777);
    assert_eq!(params.mtf_windows, vec![60]);
    assert_eq!(params.entry_z_base, 1.8);
    assert_eq!(params.beta_gap_entry_z_scale, 0.5);
    assert!(params.rehedge_live_enabled);
    assert_eq!(params.std_collapse_hold_down_secs, 120);
    assert!(params.use_frozen_beta_exit_z);
    assert!(params.regime_block_entries);
}
