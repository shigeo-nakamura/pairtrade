use chrono::{DateTime, FixedOffset, Utc};
use debot::error_counter::{self, ErrorCountingLogger};
use debot::pairtrade::{PairTradeConfig, PairTradeEngine};
use debot::ports::replay_dex::ReplayConnector;
use env_logger::Builder;
use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::sync::Arc;

mod fd_redirect;

fn init_logger() {
    let offset_seconds = env::var("TIMEZONE_OFFSET")
        .unwrap_or_else(|_| "3600".to_string())
        .parse::<i32>()
        .expect("Invalid TIMEZONE_OFFSET");
    let offset = FixedOffset::east_opt(offset_seconds).expect("Invalid offset");
    // RUST_LOG must go through env_logger's parser to honor per-module specs like
    // "info,pairtrade=debug". Prior impl parsed it with LevelFilter::from_str
    // (single-level only → fell back to Info) and then appended .filter(None, _),
    // which overrode the module directives. See bot-strategy#194.
    let env = env_logger::Env::default()
        .filter_or("RUST_LOG", "info,tokio_tungstenite=info,tungstenite=info");
    let inner = Builder::from_env(env)
        .format(move |buf, record| {
            let utc_now: DateTime<Utc> = Utc::now();
            let local_now = utc_now.with_timezone(&offset);
            writeln!(
                buf,
                "{} [{}] - {}",
                local_now.format("%Y-%m-%dT%H:%M:%S%z"),
                record.level(),
                record.args()
            )
        })
        .build();
    let max_level = inner.filter();
    let (logger, handle) = ErrorCountingLogger::wrap(Box::new(inner));
    error_counter::install_global(handle);
    if log::set_boxed_logger(Box::new(logger)).is_ok() {
        log::set_max_level(max_level);
    }
}

async fn run_single() -> std::io::Result<()> {
    let dex_connector_git = option_env!("DEX_CONNECTOR_GIT_HASH").unwrap_or("unknown");
    log::info!("dex-connector git: {}", dex_connector_git);
    log::info!("Starting pair-trade loop...");
    // Prometheus exporter is opt-in via `PROM_LISTEN`; safe to call
    // before engine boot so a slow startup still exposes the
    // process_start / version_info gauges via `/metrics`.
    debot::pairtrade::start_metrics_exporter();
    let cfg = PairTradeConfig::from_env_or_yaml().expect("invalid pair trade config");
    let loaded_config_sha = cfg.config_source_sha256.clone();
    let mut engine = init_engine_with_retry(cfg)
        .await
        .expect("failed to initialize pair trade engine");
    clear_restart_pending_after_initialization(loaded_config_sha.as_deref())?;
    engine.run().await.map_err(std::io::Error::other)
}

/// A config deploy leaves this marker behind while the running service still
/// uses the old file. Only acknowledge it after the complete engine startup
/// path (including credential decryption and connector initialization) has
/// succeeded. An unset variable keeps this behavior opt-in for the dedicated
/// deployment that owns the marker.
fn clear_restart_pending_after_initialization(
    loaded_config_sha: Option<&str>,
) -> std::io::Result<()> {
    let Some(path) = env::var_os("RESTART_PENDING_PATH").filter(|path| !path.is_empty()) else {
        return Ok(());
    };
    clear_restart_pending_marker(std::path::Path::new(&path), loaded_config_sha)
}

fn clear_restart_pending_marker(
    path: &std::path::Path,
    loaded_config_sha: Option<&str>,
) -> std::io::Result<()> {
    let marker = match std::fs::read_to_string(path) {
        Ok(marker) => marker,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    let marker_sha = marker
        .lines()
        .find_map(|line| line.strip_prefix("new_sha="));
    if marker_sha.is_none() || marker_sha != loaded_config_sha {
        log::warn!(
            "[STARTUP] preserving restart-pending marker: deployed sha does not match loaded config"
        );
        return Ok(());
    }

    match std::fs::remove_file(path) {
        Ok(()) => {
            log::info!("[STARTUP] cleared restart-pending marker after initialization");
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod restart_pending_tests {
    use super::clear_restart_pending_marker;

    #[test]
    fn clears_only_a_marker_for_the_loaded_config() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("RESTART_PENDING");

        std::fs::write(&marker, "new_sha=new\n").unwrap();
        clear_restart_pending_marker(&marker, Some("old")).unwrap();
        assert!(marker.exists());

        clear_restart_pending_marker(&marker, Some("new")).unwrap();
        assert!(!marker.exists());
    }
}

// Startup hardening for transient Lighter errors (bot-strategy#120). If
// Lighter is rate-limited when the bot comes up (e.g. after a WAF episode),
// the connector.start() and account-discovery paths surface the error to
// main.rs and used to panic immediately. Under systemd Restart=on-failure
// that became a tight crash-loop whose re-login attempts themselves kept
// the cooldown active. Now we retry the whole engine init with backoff
// inside the process for transient signatures; permanent errors (bad
// config, missing keys, unexpected shapes) still propagate straight out.
async fn init_engine_with_retry(cfg: PairTradeConfig) -> Result<PairTradeEngine, anyhow::Error> {
    const MAX_ATTEMPTS: u32 = 20;
    let mut attempt: u32 = 0;
    let mut backoff = std::time::Duration::from_secs(3);
    loop {
        attempt += 1;
        match PairTradeEngine::new(cfg.clone()).await {
            Ok(e) => {
                if attempt > 1 {
                    log::info!("[INIT_RETRY] engine initialized on attempt {}", attempt);
                }
                return Ok(e);
            }
            Err(e) => {
                let chain = format!("{:?}", e);
                // Match:
                //   - raw Lighter 429 JSON still present in a stringified error
                //   - dex-connector's `DexError::RateLimited` (Display:
                //     `Lighter WAF cooldown active until unix=... (rate-limited)`).
                //     `CheckClient` 429 now returns this variant after
                //     engaging the shared cooldown (bot-strategy#151), so
                //     the rate-limit shape is always the full 75s wait
                //     rather than a 3s retry storm.
                let transient_429 = chain.contains("Too Many Requests")
                    || chain.contains("\"code\":23000")
                    || chain.contains(" 429 ")
                    || chain.contains("rate-limited")
                    || chain.contains("WAF cooldown");
                let transient = transient_429
                    || (chain.contains("Could not find account for api_key_index=")
                        && chain.contains("Set LIGHTER_ACCOUNT_INDEX"));
                if !transient || attempt >= MAX_ATTEMPTS {
                    return Err(e);
                }
                // Lighter's per-IP /account short-window is ~60s. Retrying
                // inside that window just re-burns the budget; wait past it
                // on the 429 path before retrying. Other transient shapes
                // (account-index rediscovery) keep the fast backoff.
                // See bot-strategy#127.
                let sleep_for = if transient_429 {
                    backoff.max(std::time::Duration::from_secs(75))
                } else {
                    backoff
                };
                log::warn!(
                    "[INIT_RETRY] transient startup error (attempt {}/{}), sleeping {}s. Reason: {}",
                    attempt,
                    MAX_ATTEMPTS,
                    sleep_for.as_secs(),
                    chain.lines().next().unwrap_or(&chain),
                );
                tokio::time::sleep(sleep_for).await;
                backoff = (sleep_for * 2).min(std::time::Duration::from_secs(60));
            }
        }
    }
}

async fn run_batch(batch_file: &str) -> std::io::Result<()> {
    let param_sets = load_batch_params(batch_file)?;

    if param_sets.is_empty() {
        eprintln!("[BATCH] No param sets found in {}", batch_file);
        return Ok(());
    }

    // Load replay data once.
    let backtest_file = env::var("BACKTEST_FILE").map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "BACKTEST_FILE must be set for batch mode",
        )
    })?;
    eprintln!(
        "[BATCH] Loading data from {} ({} param sets)...",
        backtest_file,
        param_sets.len()
    );
    let replay = Arc::new(
        ReplayConnector::new(&backtest_file)
            .map_err(|e| std::io::Error::other(format!("failed to load replay data: {}", e)))?,
    );
    eprintln!("[BATCH] Data loaded: {} entries.", replay.len());

    // Output dir for per-run log files.
    let log_dir = env::var("BATCH_LOG_DIR").unwrap_or_else(|_| "/tmp/batch_logs".to_string());
    std::fs::create_dir_all(&log_dir)?;

    // Save original env vars that will be overridden.
    let override_keys: Vec<String> = param_sets
        .iter()
        .flat_map(|ps| ps.keys().cloned())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let original_env: HashMap<String, Option<String>> = override_keys
        .iter()
        .map(|k| (k.clone(), env::var(k).ok()))
        .collect();

    for (idx, params) in param_sets.iter().enumerate() {
        // Set env vars for this param set.
        for (k, v) in params {
            env::set_var(k, v);
        }

        // Build config from env (picks up the overridden vars).
        let cfg = match PairTradeConfig::from_env_or_yaml() {
            Ok(c) => c,
            Err(e) => {
                let result = serde_json::json!({
                    "index": idx,
                    "log_file": serde_json::Value::Null,
                    "error": format!("{}", e),
                });
                println!("{}", result);
                // Restore env vars before continuing.
                for (k, orig) in &original_env {
                    match orig {
                        Some(v) => env::set_var(k, v),
                        None => env::remove_var(k),
                    }
                }
                continue;
            }
        };

        // Redirect log output to a per-run file.
        let log_file_path = format!("{}/batch_{}.log", log_dir, idx);

        // Create engine with shared replay data.
        let mut engine = match PairTradeEngine::new_with_replay(cfg, replay.clone()).await {
            Ok(e) => e,
            Err(e) => {
                let result = serde_json::json!({
                    "index": idx,
                    "log_file": log_file_path,
                    "error": format!("{}", e),
                });
                println!("{}", result);
                for (k, orig) in &original_env {
                    match orig {
                        Some(v) => env::set_var(k, v),
                        None => env::remove_var(k),
                    }
                }
                continue;
            }
        };

        // Run backtest, capturing log output to a file.
        {
            let log_file = std::fs::File::create(&log_file_path)?;
            let _redirect = fd_redirect::StdioRedirect::to_file(&log_file)?;
            let _result = engine.run().await;
        }

        // Output result as JSON to stdout.
        let result = serde_json::json!({
            "index": idx,
            "log_file": log_file_path,
        });
        println!("{}", result);

        // Restore env vars for next iteration.
        for (k, orig) in &original_env {
            match orig {
                Some(v) => env::set_var(k, v),
                None => env::remove_var(k),
            }
        }
    }

    Ok(())
}

const BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV: &str = "BATCH_JSONL_MAX_PARSE_ERROR_PCT";

#[derive(Debug)]
struct BatchJsonlParseError {
    line_no: usize,
    error: serde_json::Error,
}

fn load_batch_params(batch_file: &str) -> std::io::Result<Vec<HashMap<String, String>>> {
    use std::io::{BufRead, BufReader};

    let max_parse_error_pct = batch_jsonl_max_parse_error_pct()?;
    let file = std::fs::File::open(batch_file).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("failed to open batch file {}: {}", batch_file, e),
        )
    })?;
    let reader = BufReader::new(file);
    let mut param_sets = Vec::new();
    let mut parse_errors = Vec::new();
    let mut non_empty_lines = 0usize;

    for (line_idx, line) in reader.lines().enumerate() {
        let line_no = line_idx + 1;
        let line = line.map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to read batch file {batch_file} line {line_no}: {e}"),
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }

        non_empty_lines += 1;
        match serde_json::from_str::<HashMap<String, String>>(&line) {
            Ok(params) => param_sets.push(params),
            Err(error) => parse_errors.push(BatchJsonlParseError { line_no, error }),
        }
    }

    if !parse_errors.is_empty() {
        report_batch_parse_errors(&parse_errors, non_empty_lines, max_parse_error_pct);
        let parse_error_pct = parse_errors.len() as f64 * 100.0 / non_empty_lines as f64;
        if parse_error_pct > max_parse_error_pct {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "batch JSONL parse error rate {:.2}% exceeds allowed {:.2}% ({} errors / {} non-empty lines; lines: {})",
                    parse_error_pct,
                    max_parse_error_pct,
                    parse_errors.len(),
                    non_empty_lines,
                    parse_errors
                        .iter()
                        .map(|err| err.line_no.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
    }

    Ok(param_sets)
}

fn batch_jsonl_max_parse_error_pct() -> std::io::Result<f64> {
    match env::var(BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV) {
        Ok(raw) => {
            let parsed = raw.parse::<f64>().map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "{BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV} must be a percentage between 0 and 100: {e}"
                    ),
                )
            })?;
            if !(0.0..=100.0).contains(&parsed) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "{BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV} must be a percentage between 0 and 100"
                    ),
                ));
            }
            Ok(parsed)
        }
        Err(env::VarError::NotPresent) => Ok(0.0),
        Err(e) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV} is invalid: {e}"),
        )),
    }
}

fn report_batch_parse_errors(
    parse_errors: &[BatchJsonlParseError],
    non_empty_lines: usize,
    max_parse_error_pct: f64,
) {
    let parse_error_pct = parse_errors.len() as f64 * 100.0 / non_empty_lines as f64;
    eprintln!(
        "[BATCH] {} malformed JSONL line(s) in {} non-empty lines ({:.2}%; allowed {:.2}%)",
        parse_errors.len(),
        non_empty_lines,
        parse_error_pct,
        max_parse_error_pct
    );
    for err in parse_errors {
        eprintln!(
            "[BATCH] malformed JSONL at line {}: {}",
            err.line_no, err.error
        );
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_logger();

    if let Ok(batch_file) = env::var("BATCH_PARAMS_FILE") {
        run_batch(&batch_file).await
    } else {
        run_single().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_parse_error_threshold<T>(value: Option<&str>, f: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap();
        let old_value = env::var(BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV).ok();
        match value {
            Some(value) => env::set_var(BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV, value),
            None => env::remove_var(BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV),
        }
        let result = f();
        match old_value {
            Some(value) => env::set_var(BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV, value),
            None => env::remove_var(BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV),
        }
        result
    }

    #[test]
    fn load_batch_params_rejects_malformed_jsonl_by_default() {
        with_parse_error_threshold(None, || {
            let dir = tempfile::tempdir().unwrap();
            let batch_file = dir.path().join("batch.jsonl");
            std::fs::write(
                &batch_file,
                "{\"ENTRY_Z_SCORE\":\"0.3\"}\nnot-json\n\n{\"STOP_LOSS_Z\":\"6\"}\n",
            )
            .unwrap();

            let err = load_batch_params(batch_file.to_str().unwrap()).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            let message = err.to_string();
            assert!(message.contains("33.33%"));
            assert!(message.contains("1 errors / 3 non-empty lines"));
            assert!(message.contains("lines: 2"));
        });
    }

    #[test]
    fn load_batch_params_allows_malformed_jsonl_under_threshold() {
        with_parse_error_threshold(Some("50"), || {
            let dir = tempfile::tempdir().unwrap();
            let batch_file = dir.path().join("batch.jsonl");
            std::fs::write(
                &batch_file,
                "{\"ENTRY_Z_SCORE\":\"0.3\"}\nnot-json\n{\"STOP_LOSS_Z\":\"6\"}\n",
            )
            .unwrap();

            let params = load_batch_params(batch_file.to_str().unwrap()).unwrap();
            assert_eq!(params.len(), 2);
            assert_eq!(params[0].get("ENTRY_Z_SCORE").unwrap(), "0.3");
            assert_eq!(params[1].get("STOP_LOSS_Z").unwrap(), "6");
        });
    }

    #[test]
    fn load_batch_params_rejects_invalid_parse_error_threshold() {
        with_parse_error_threshold(Some("not-a-number"), || {
            let err = batch_jsonl_max_parse_error_pct().unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert!(err
                .to_string()
                .contains(BATCH_JSONL_MAX_PARSE_ERROR_PCT_ENV));
        });
    }
}
