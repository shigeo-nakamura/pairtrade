//! Operator sentinel-file paths: the kill-switch and the session-DD
//! risk-ack files. Both are consumed at the top of `step_shared`. Split
//! out of the monolithic `pairtrade::mod` as a stable, leaf concern
//! (bot-strategy#502); the resolver is pure and unit-tested here.

/// Sentinel file that, when present, blocks all new entries without
/// requiring `systemctl stop`. Existing positions still exit normally.
/// Manage via `ssh debot "sudo touch /opt/debot/KILL_SWITCH"` to engage
/// and `sudo rm /opt/debot/KILL_SWITCH` to release. Engages at the top
/// of every `step_shared` tick, so reaction latency matches
/// `interval_secs`. See bot-strategy#185 Phase 1-2.
///
/// Overridable via the `KILL_SWITCH_PATH` env var (same pattern as
/// `RISK_ACK_PATH`, bot-strategy#488) so multi-bot hosts can engage a
/// per-bot kill switch and the halt-gate integration tests
/// (bot-strategy#537) can exercise the sentinel without touching
/// `/opt/debot/`. Resolved once at process start.
const DEFAULT_KILL_SWITCH_PATH: &str = "/opt/debot/KILL_SWITCH";

pub(in crate::pairtrade) fn kill_switch_path() -> &'static str {
    static PATH: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PATH.get_or_init(|| resolve_kill_switch_path(std::env::var("KILL_SWITCH_PATH").ok()))
        .as_str()
}

/// Pure resolver for the kill-switch sentinel path; same env precedence
/// rules as `resolve_risk_ack_path` below (present + non-blank env wins,
/// unset or blank falls back to the default).
fn resolve_kill_switch_path(env_val: Option<String>) -> String {
    match env_val {
        Some(v) if !v.trim().is_empty() => v,
        _ => DEFAULT_KILL_SWITCH_PATH.to_string(),
    }
}

/// Manual-ack sentinel for clearing a session-DD halt (Phase 3-2). Drop
/// this file (any contents) on the host to lift the halt; the bot
/// consumes it at the top of `step_shared` so the file is removed even
/// if all instances were already clear. See bot-strategy#185 Phase 3-2.
///
/// Defaults to `/opt/debot/RISK_ACK` for backwards compatibility with the
/// main bot's deploy. Overridable via the `RISK_ACK_PATH` env var so
/// multi-bot hosts (canary + main, Extended + main) can each consume an
/// independent ack file and avoid the "drop one file, release every bot"
/// footgun. Resolved once at process start. bot-strategy#488.
const DEFAULT_RISK_ACK_PATH: &str = "/opt/debot/RISK_ACK";

pub(in crate::pairtrade) fn risk_ack_path() -> &'static str {
    static PATH: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PATH.get_or_init(|| resolve_risk_ack_path(std::env::var("RISK_ACK_PATH").ok()))
        .as_str()
}

/// Pure resolver split out of `risk_ack_path` so the env precedence is
/// unit-testable without touching the process-global `OnceLock`. A
/// present, non-blank `RISK_ACK_PATH` wins; unset *or* blank falls back to
/// the default. The blank guard stops an accidental `RISK_ACK_PATH=`
/// (empty export) from silently pointing the ack file at the process cwd.
/// bot-strategy#488.
fn resolve_risk_ack_path(env_val: Option<String>) -> String {
    match env_val {
        Some(v) if !v.trim().is_empty() => v,
        _ => DEFAULT_RISK_ACK_PATH.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_kill_switch_path, resolve_risk_ack_path, DEFAULT_KILL_SWITCH_PATH,
        DEFAULT_RISK_ACK_PATH,
    };

    #[test]
    fn resolve_kill_switch_path_prefers_env_then_falls_back() {
        // bot-strategy#537: env override for per-bot kill switches and
        // for the halt-gate integration tests...
        assert_eq!(
            resolve_kill_switch_path(Some("/opt/debot-canary/KILL_SWITCH".to_string())),
            "/opt/debot-canary/KILL_SWITCH"
        );
        // ...while unset / blank preserves the historical path.
        assert_eq!(resolve_kill_switch_path(None), DEFAULT_KILL_SWITCH_PATH);
        assert_eq!(
            resolve_kill_switch_path(Some(String::new())),
            DEFAULT_KILL_SWITCH_PATH
        );
        assert_eq!(
            resolve_kill_switch_path(Some("   ".to_string())),
            DEFAULT_KILL_SWITCH_PATH
        );
    }

    #[test]
    fn resolve_risk_ack_path_prefers_env_then_falls_back() {
        // bot-strategy#488: a non-blank override wins so co-located bots
        // (canary / Extended) consume their own ack file...
        assert_eq!(
            resolve_risk_ack_path(Some("/opt/debot-canary/RISK_ACK".to_string())),
            "/opt/debot-canary/RISK_ACK"
        );
        // ...while unset preserves the main bot's historical path.
        assert_eq!(resolve_risk_ack_path(None), DEFAULT_RISK_ACK_PATH);
        // Blank / whitespace-only exports fall back rather than aiming the
        // ack file at the process cwd.
        assert_eq!(
            resolve_risk_ack_path(Some(String::new())),
            DEFAULT_RISK_ACK_PATH
        );
        assert_eq!(
            resolve_risk_ack_path(Some("   ".to_string())),
            DEFAULT_RISK_ACK_PATH
        );
    }
}
