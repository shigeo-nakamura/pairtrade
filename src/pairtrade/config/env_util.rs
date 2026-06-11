//! Environment-variable parsing helpers for pairtrade config.
//!
//! Generic `env_parse`/`env_override` helpers (plus trading-critical
//! panic-on-bad-value variants) and the BT timestamp-file loaders. These are
//! pure, config-type-agnostic utilities split out of `config/mod.rs` to keep
//! the schema/normalization/validation logic readable (bot-strategy#502).

use std::env;

/// Parse an env var, falling back to a default on unset OR parse failure.
///
/// Distinguishes the two failure modes:
/// - **Unset** — silent (this is the documented expectation; the bot keeps
///   running with the compile-time / YAML default).
/// - **Set but parse fails** — `log::warn!` at WARN level naming the env var
///   and the rejected value, then return the fallback. Pre bot-strategy#439
///   this case was silent and indistinguishable from "unset", so a typo in
///   a deploy script could silently revert the field to its default.
///
/// Trading-critical fields (max_leverage, equity reference, risk caps, etc.)
/// should use `env_parse_critical` / `env_override_critical` instead — those
/// hard-fail at startup on parse failure rather than reverting to a default
/// that may be unsafe in production.
pub(super) fn env_parse<T: std::str::FromStr>(key: &str, fallback: T) -> T
where
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(value) => match value.parse::<T>() {
            Ok(parsed) => parsed,
            Err(e) => {
                log::warn!(
                    "[CONFIG] env {}={:?} failed to parse ({}); using fallback",
                    key,
                    value,
                    e
                );
                fallback
            }
        },
        Err(_) => fallback,
    }
}

/// If `key` is set AND parses, overwrite `target`. Mirrors `env_parse`'s
/// silent-vs-warn distinction: unset is silent, parse failure logs a WARN
/// and leaves `target` untouched.
pub(super) fn env_override<T: std::str::FromStr>(key: &str, target: &mut T)
where
    T::Err: std::fmt::Display,
{
    if let Ok(value) = env::var(key) {
        match value.parse::<T>() {
            Ok(parsed) => *target = parsed,
            Err(e) => log::warn!(
                "[CONFIG] env {}={:?} failed to parse ({}); leaving previous value",
                key,
                value,
                e
            ),
        }
    }
}

/// Trading-critical version of `env_parse`. Behaviour matches `env_parse`
/// for unset and success cases, but on parse failure it panics at startup
/// rather than reverting to the fallback. Use for fields whose silent
/// revert could place an unsafe trade (max_leverage, equity reference,
/// risk caps, dry_run, etc.). bot-strategy#439.
pub(super) fn env_parse_critical<T: std::str::FromStr>(key: &str, fallback: T) -> T
where
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(value) => match value.parse::<T>() {
            Ok(parsed) => parsed,
            Err(e) => panic!(
                "[CONFIG] trading-critical env {}={:?} failed to parse ({}); refusing to start with default fallback. \
                 Fix the env var or unset it explicitly. (bot-strategy#439)",
                key, value, e
            ),
        },
        Err(_) => fallback,
    }
}

/// Trading-critical version of `env_override`. Panics on parse failure.
pub(super) fn env_override_critical<T: std::str::FromStr>(key: &str, target: &mut T)
where
    T::Err: std::fmt::Display,
{
    if let Ok(value) = env::var(key) {
        match value.parse::<T>() {
            Ok(parsed) => *target = parsed,
            Err(e) => panic!(
                "[CONFIG] trading-critical env {}={:?} failed to parse ({}); refusing to start. \
                 Fix the env var or unset it explicitly. (bot-strategy#439)",
                key, value, e
            ),
        }
    }
}

/// Load the BT eval-timestamps file (one UNIX second per line) referenced by
/// the `BT_EVAL_TIMESTAMPS_FILE` env var. Ignored silently when the env var
/// is unset, the path is unreadable, or no numeric lines are found — live
/// mode and vanilla BT (without the override) must stay unchanged.
pub(super) fn load_bt_eval_timestamps() -> Option<std::collections::HashSet<i64>> {
    load_ts_set("BT_EVAL_TIMESTAMPS_FILE", "[BT_EVAL_TIMESTAMPS]")
}

/// Load BT restart timestamps (one UNIX second per line). See
/// `PairTradeConfig::bt_restart_timestamps` for semantics.
pub(super) fn load_bt_restart_timestamps() -> Option<std::collections::HashSet<i64>> {
    load_ts_set("BT_RESTART_TIMESTAMPS_FILE", "[BT_RESTART_TIMESTAMPS]")
}

fn load_ts_set(env_key: &str, tag: &str) -> Option<std::collections::HashSet<i64>> {
    use std::collections::HashSet;
    let path = env::var(env_key).ok()?;
    let path = path.trim();
    if path.is_empty() {
        return None;
    }
    let contents = std::fs::read_to_string(path)
        .map_err(|e| log::warn!("{} failed to read {}: {}", tag, path, e))
        .ok()?;
    let mut set: HashSet<i64> = HashSet::new();
    for line in contents.lines() {
        if let Ok(ts) = line.trim().parse::<i64>() {
            set.insert(ts);
        }
    }
    if set.is_empty() {
        log::warn!("{} {} contained no parseable timestamps", tag, path);
        return None;
    }
    log::info!("{} loaded {} timestamps from {}", tag, set.len(), path);
    Some(set)
}
