use super::defaults::DEFAULT_SHUTDOWN_GRACE_SECS;
use super::*;

fn config_path(name: &str) -> String {
    format!("{}/configs/pairtrade/{}", env!("CARGO_MANIFEST_DIR"), name)
}

#[test]
fn default_when_yaml_omits_key() {
    // from_env() path with no env var set = default
    // Use a scoped env guard to avoid bleeding into other tests.
    let prev = std::env::var("SHUTDOWN_GRACE_SECS").ok();
    std::env::remove_var("SHUTDOWN_GRACE_SECS");
    // Also ensure required env vars have sensible fallbacks.
    std::env::set_var("DEX_NAME", "lighter");
    std::env::set_var("UNIVERSE_PAIRS", "BTC/ETH");
    let cfg = PairTradeConfig::from_env().expect("from_env failed");
    assert_eq!(cfg.shutdown_grace_secs, DEFAULT_SHUTDOWN_GRACE_SECS);
    assert_eq!(cfg.shutdown_grace_secs, 3660);
    if let Some(v) = prev {
        std::env::set_var("SHUTDOWN_GRACE_SECS", v);
    }
}

#[test]
fn live_btceth_configs_pin_grace_above_force_close() {
    // The -b / -c YAMLs were folded into the single multi-strategy
    // debot-pair-btceth.yaml in commit 7 of #25; only the consolidated
    // file is checked here.
    //
    // Asserts the bot-strategy#50 invariant directly:
    //   shutdown_grace_secs >= max(force_close_time_secs across resolved
    //                              default + per-pair + per-strategy)
    //                          + 60s buffer
    // (Or shutdown_grace_secs == 0, the legacy immediate-close mode that
    // validate() also accepts.)
    //
    // The same check runs inside PairTradeConfig::validate() during
    // from_yaml_path, so a YAML drift will already block load. Asserting
    // here serves as documentation and a defense against accidental
    // validate() bypass. Pinning the literal expected value (e.g. 7260,
    // 10860) was the prior implementation but coupled the test to YAML
    // edits — every per-strategy fc bump (#278 Round 4 fc=10800 was the
    // first to hit this) needed a matching test edit. The invariant
    // form survives any YAML change that respects the rule.
    const BUFFER_SECS: u64 = 60;
    let configs = &["debot-pair-btceth.yaml"];
    for name in configs {
        let path = config_path(name);
        let cfg = PairTradeConfig::from_yaml_path(&path)
            .unwrap_or_else(|e| panic!("failed to load {path}: {e}"));
        if cfg.shutdown_grace_secs == 0 {
            continue;
        }
        let max_fc = std::iter::once(cfg.default_pair_params.force_close_secs)
            .chain(cfg.pair_params.values().map(|p| p.force_close_secs))
            .chain(
                cfg.strategies
                    .iter()
                    .filter_map(|s| s.force_close_time_secs),
            )
            .max()
            .expect("at least default_pair_params.force_close_secs");
        let required = max_fc + BUFFER_SECS;
        assert!(
                cfg.shutdown_grace_secs >= required,
                "{name}: shutdown_grace_secs={} must be >= max(force_close_time_secs)={} + {}s buffer = {}",
                cfg.shutdown_grace_secs, max_fc, BUFFER_SECS, required
            );
    }
}

/// Regression guard for bot-strategy#50: if any strategy raises
/// `force_close_time_secs` above `shutdown_grace_secs - 60s`, config load
/// must fail rather than silently shipping a config that would
/// prematurely force-close positions on SIGTERM.
#[test]
fn validate_rejects_strategy_force_close_exceeding_grace() {
    use std::io::Write;
    let dir = std::env::temp_dir();
    let path = dir.join("pairtrade_validate_regression.yaml");
    let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
force_close_time_secs: 3600
shutdown_grace_secs: 3660
strategies:
  - id: a
    force_close_time_secs: 7200
"#;
    std::fs::File::create(&path)
        .unwrap()
        .write_all(yaml.as_bytes())
        .unwrap();
    let err = PairTradeConfig::from_yaml_path(&path)
        .expect_err("validate() must reject grace=3660 when strategy A force_close=7200");
    let msg = format!("{err}");
    assert!(
        msg.contains("shutdown_grace_secs"),
        "error should mention shutdown_grace_secs, got: {msg}"
    );
    let _ = std::fs::remove_file(&path);
}
