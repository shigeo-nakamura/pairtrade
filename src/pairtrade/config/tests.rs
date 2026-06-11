use super::risk::resolve_risk_config;
use super::schema::RiskYaml;
use super::*;

#[test]
fn risk_config_defaults_when_block_absent() {
    let cfg = resolve_risk_config(None).unwrap();
    assert_eq!(cfg.max_daily_loss_bps, 0);
    assert_eq!(cfg.max_session_loss_bps, 0);
    assert_eq!(cfg.max_notional_headroom, 0.0);
    assert!(matches!(cfg.max_daily_loss_action, DailyLossAction::Block));
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
