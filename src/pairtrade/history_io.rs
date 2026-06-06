//! On-disk history persistence helpers extracted from the monolithic
//! pairtrade module.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::config::PairTradeConfig;
use super::stats::PriceSample;

/// Per-pair Kalman filter snapshot (bot-strategy#413). `q` / `r` are
/// tuning knobs from the YAML, not state — they're restored from the
/// current config rather than persisted.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub(super) struct KalmanSnapshot {
    pub(super) beta: f64,
    pub(super) p: f64,
    pub(super) updates: u64,
}

/// On-disk snapshot schema used by the live bot.
///
/// Version 1 (no `_v` field) was a bare `HashMap<String, Vec<(f64, i64)>>`
/// with `ts` in Unix seconds.
///
/// Version 2 added `spread_histories` — the per-pair `state.spread_history`
/// that the engine accumulates at runtime — so that at restart we can restore
/// the real spread series instead of rebuilding a synthetic one via
/// `warm_start_states_from_history` (which applies a single OLS beta to the
/// full log_price window and produces an artificially low-variance
/// spread_history, the mechanism behind the 2026-04-15 06:02 UTC "std
/// collapse" incident — bot-strategy#62). v2 still stored `ts` in seconds.
///
/// Version 3 (bot-strategy#274 / #276) bumps `ts` to Unix milliseconds so
/// the on-disk timestamps match the BarBuilder bucketing layer after its
/// ms-precision migration. Older v1 / v2 snapshots are auto-detected and
/// migrated (`ts × 1000`) on load.
///
/// Version 4 (bot-strategy#413) adds `betas` (the eval-committed β per
/// pair) and `kalman_states` (online Kalman filter state — β / p / updates)
/// so a restart preserves the live β trajectory instead of reverting to a
/// fresh OLS warm-start. The new fields default to empty when reading a
/// pre-v4 file, so v3 snapshots auto-migrate (Kalman gets a fresh filter,
/// committed β re-derived from OLS as before).
///
/// The loader parses the explicit struct first and falls back to v1 (bare
/// per-symbol map) on failure, so pre-existing history files keep working.
#[derive(Serialize, Deserialize, Default)]
struct SnapshotV4 {
    #[serde(rename = "_v")]
    version: u32,
    prices: HashMap<String, Vec<(f64, i64)>>,
    /// Pair key (e.g. "BTC/ETH") → the engine's per-pair `spread_history`
    /// as a plain `Vec<f64>`. Missing in pre-v2 files; defaulted to empty.
    #[serde(default)]
    spread_histories: HashMap<String, Vec<f64>>,
    /// Eval-committed β per pair (was `state.beta` pre-#413). Missing in
    /// pre-v4 snapshots; the engine recomputes from OLS at warm-start when
    /// empty, matching pre-#413 behaviour.
    #[serde(default)]
    betas: HashMap<String, f64>,
    /// Per-pair Kalman filter state. Missing in pre-v4 snapshots; the
    /// engine constructs a fresh `KalmanBeta` from the YAML defaults when
    /// empty.
    #[serde(default)]
    kalman_states: HashMap<String, KalmanSnapshot>,
}

const SNAPSHOT_VERSION: u32 = 4;
/// Snapshot versions whose `ts` field is in seconds — they need a × 1000
/// migration at load time so the post bot-strategy#274 / #276 BarBuilder
/// bucket math lines up.
const SECONDS_TS_VERSIONS_MAX: u32 = 2;

pub(super) fn persist_history_to_disk(
    cfg: &PairTradeConfig,
    history: &HashMap<String, VecDeque<PriceSample>>,
    spread_histories: &HashMap<String, VecDeque<f64>>,
    betas: &HashMap<String, f64>,
    kalman_states: &HashMap<String, KalmanSnapshot>,
    history_path: &std::path::Path,
) {
    if cfg.disable_history_persist {
        return;
    }
    // Backtest replay re-drives this per tick, producing hundreds of
    // thousands of disk writes per run. That serialises a grid of
    // concurrent backtest processes on ext4 and leaves them wedged in
    // `Dl` state. The persisted file is only consumed by peer live bots
    // for A/B/C alignment, which is irrelevant under replay.
    if cfg.backtest_mode {
        return;
    }
    let prices: HashMap<String, Vec<(f64, i64)>> = history
        .iter()
        .map(|(sym, deque)| {
            let v: Vec<(f64, i64)> = deque.iter().map(|p| (p.log_price, p.ts)).collect();
            (sym.clone(), v)
        })
        .collect();
    let spread_histories: HashMap<String, Vec<f64>> = spread_histories
        .iter()
        .map(|(k, deque)| (k.clone(), deque.iter().copied().collect()))
        .collect();
    let snapshot = SnapshotV4 {
        version: SNAPSHOT_VERSION,
        prices,
        spread_histories,
        betas: betas.clone(),
        kalman_states: kalman_states.clone(),
    };
    if let Ok(json) = serde_json::to_string(&snapshot) {
        // Atomic write: tmpfile in the same directory + rename. Multiple
        // bots may be writing this shared file concurrently (pairtrade#4);
        // rename guarantees readers never observe a torn JSON document.
        let path = history_path;
        let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let file_name = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "pairtrade_history.json".to_string());
        let tmp = dir.join(format!(".{}.tmp.{}", file_name, std::process::id()));
        if let Err(e) = fs::write(&tmp, json) {
            log::debug!("persist history tmp write failed: {:?}", e);
            return;
        }
        if let Err(e) = fs::rename(&tmp, path) {
            log::debug!("persist history rename failed: {:?}", e);
            let _ = fs::remove_file(&tmp);
        }
    }

    archive_snapshot_hourly(cfg, history_path);
}

fn archive_snapshot_hourly(cfg: &PairTradeConfig, history_path: &Path) {
    let Some(archive_dir) = &cfg.history_archive_dir else {
        return;
    };
    let archive_dir = Path::new(archive_dir);
    if let Err(e) = fs::create_dir_all(archive_dir) {
        log::debug!("archive dir create failed: {:?}", e);
        return;
    }
    let stem = history_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    let hour_tag = Utc::now().format("%Y%m%dT%H00Z");
    let archive_path = archive_dir.join(format!("{}.{}.json", stem, hour_tag));
    if archive_path.exists() {
        return;
    }
    if let Err(e) = fs::copy(history_path, &archive_path) {
        log::debug!("archive snapshot copy failed: {:?}", e);
        return;
    }
    log::info!(
        "[HISTORY_ARCHIVE] saved {}",
        archive_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
    );
    cleanup_old_archives(archive_dir, cfg.history_archive_retention_days);
}

fn cleanup_old_archives(dir: &Path, retention_days: u32) {
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(retention_days as u64 * 86400))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        let Ok(modified) = meta.modified() else {
            continue;
        };
        if modified < cutoff {
            let _ = fs::remove_file(entry.path());
            log::info!(
                "[HISTORY_ARCHIVE] removed expired {}",
                entry.file_name().to_string_lossy()
            );
        }
    }
}

/// Result of a successful snapshot parse, carrying the detected schema
/// version so the caller can include it in its `[WARM_START]` log line.
#[derive(Debug)]
struct ParsedSnapshot {
    version: u32,
    prices: HashMap<String, Vec<(f64, i64)>>,
    spread_histories: HashMap<String, Vec<f64>>,
    betas: HashMap<String, f64>,
    kalman_states: HashMap<String, KalmanSnapshot>,
}

/// Parse the persisted history file, accepting v4 (explicit struct with
/// ms-precision `ts` + betas + kalman_states), v3 (explicit struct with
/// ms-precision `ts`, betas/kalman default to empty), v2 (explicit struct
/// with seconds `ts`, auto-migrated by × 1000) and legacy v1 (bare per-symbol
/// map, also seconds, auto-migrated). Returns a `ParsedSnapshot` with `ts`
/// always normalized to ms, or a human-readable error string for the caller
/// to log (bot-strategy#370 — every load outcome must be greppable from
/// journalctl).
fn parse_snapshot_file(path: &std::path::Path) -> Result<ParsedSnapshot, String> {
    let content = fs::read_to_string(path).map_err(|e| format!("read failed: {}", e))?;
    // Try the explicit struct first (has `_v` and `prices`). `SnapshotV4`
    // is a superset of v3 / v2 / v3 — the missing-field defaults pick up
    // empty `spread_histories` / `betas` / `kalman_states` for older
    // snapshots.
    if let Ok(snap) = serde_json::from_str::<SnapshotV4>(&content) {
        if snap.version >= 2 {
            let prices = if snap.version <= SECONDS_TS_VERSIONS_MAX {
                migrate_prices_seconds_to_ms(snap.prices)
            } else {
                snap.prices
            };
            return Ok(ParsedSnapshot {
                version: snap.version,
                prices,
                spread_histories: snap.spread_histories,
                betas: snap.betas,
                kalman_states: snap.kalman_states,
            });
        }
        return Err(format!(
            "schema _v={} not supported (expected 2 / 3 / 4, or v1 bare-map)",
            snap.version
        ));
    }
    // Fall back to v1 (bare `HashMap<String, Vec<(f64, i64)>>` in seconds).
    match serde_json::from_str::<HashMap<String, Vec<(f64, i64)>>>(&content) {
        Ok(prices) => Ok(ParsedSnapshot {
            version: 1,
            prices: migrate_prices_seconds_to_ms(prices),
            spread_histories: HashMap::new(),
            betas: HashMap::new(),
            kalman_states: HashMap::new(),
        }),
        Err(e) => Err(format!(
            "JSON did not match v2/v3/v4 struct or v1 bare-map shape: {}",
            e
        )),
    }
}

/// Convert a per-symbol price history whose `ts` field is in Unix seconds
/// into the ms representation expected post bot-strategy#274 / #276. Used
/// for one-way migration of v1 / v2 snapshot files at load time.
fn migrate_prices_seconds_to_ms(
    prices: HashMap<String, Vec<(f64, i64)>>,
) -> HashMap<String, Vec<(f64, i64)>> {
    prices
        .into_iter()
        .map(|(sym, samples)| {
            let migrated = samples
                .into_iter()
                .map(|(lp, ts)| (lp, ts.saturating_mul(1000)))
                .collect();
            (sym, migrated)
        })
        .collect()
}

/// Load a history snapshot for backtest warm-start. Unlike
/// `load_history_from_disk`, this skips the stale-guard check (the
/// snapshot is always older than the replay cursor) and instead accepts
/// all samples within `max_history_len` bars of the *newest* sample in
/// each symbol, regardless of `now_ts`. Also populates `spread_histories_out`
/// (v2+), `betas_out` and `kalman_states_out` (v4+) when present.
pub(super) fn load_history_snapshot_for_bt(
    history: &mut HashMap<String, VecDeque<PriceSample>>,
    spread_histories_out: &mut HashMap<String, VecDeque<f64>>,
    betas_out: &mut HashMap<String, f64>,
    kalman_states_out: &mut HashMap<String, KalmanSnapshot>,
    snapshot_path: &std::path::Path,
    max_history_len: usize,
) {
    let snap = match parse_snapshot_file(snapshot_path) {
        Ok(s) => s,
        Err(e) => {
            log::warn!(
                "[BT_WARM_START] failed to read or parse snapshot {}: {}",
                snapshot_path.display(),
                e,
            );
            return;
        }
    };
    log::info!(
        "[BT_WARM_START] parsed v{} snapshot from {}",
        snap.version,
        snapshot_path.display()
    );
    for (sym, entries) in snap.prices {
        if entries.is_empty() {
            continue;
        }
        let newest_ts = entries.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
        // Snapshot ts is ms post bot-strategy#274 / #276; assume 60s bars.
        let max_age_ms = (max_history_len as i64) * 60 * 1000;
        let mut deque = VecDeque::new();
        for (log_price, ts) in entries {
            if newest_ts.saturating_sub(ts) <= max_age_ms {
                deque.push_back(PriceSample { log_price, ts });
            }
        }
        if !deque.is_empty() {
            log::info!(
                "[BT_WARM_START] loaded {} bars for {} from snapshot",
                deque.len(),
                sym
            );
            history.insert(sym, deque);
        }
    }
    for (pair_key, series) in snap.spread_histories {
        if series.is_empty() {
            continue;
        }
        let len = series.len();
        let deque: VecDeque<f64> = series.into_iter().collect();
        log::info!(
            "[BT_WARM_START] loaded {} persisted spread_history bars for {}",
            len,
            pair_key
        );
        spread_histories_out.insert(pair_key, deque);
    }
    for (pair_key, beta) in snap.betas {
        log::info!(
            "[BT_WARM_START] loaded persisted β={:.4} for {}",
            beta,
            pair_key
        );
        betas_out.insert(pair_key, beta);
    }
    for (pair_key, kalman) in snap.kalman_states {
        log::info!(
            "[BT_WARM_START] loaded Kalman state β={:.4} p={:.6} updates={} for {}",
            kalman.beta,
            kalman.p,
            kalman.updates,
            pair_key
        );
        kalman_states_out.insert(pair_key, kalman);
    }
}

pub(super) fn load_history_from_disk(
    cfg: &PairTradeConfig,
    history: &mut HashMap<String, VecDeque<PriceSample>>,
    spread_histories_out: &mut HashMap<String, VecDeque<f64>>,
    betas_out: &mut HashMap<String, f64>,
    kalman_states_out: &mut HashMap<String, KalmanSnapshot>,
    history_path: &std::path::Path,
    now_ts: i64,
    max_history_len: usize,
    last_logged_key: &mut Option<String>,
) {
    if cfg.disable_history_persist {
        return;
    }
    // Skip persisted-history loading entirely under backtest replay: the
    // file's timestamps reflect the wall clock at dump time and would
    // always look stale relative to the replayed cursor, producing
    // millions of WARN lines without contributing anything useful (the
    // replay data already supplies a clean, gap-free history).
    if cfg.backtest_mode {
        return;
    }
    // bot-strategy#370: every load outcome (no-file / parse-error /
    // stale-guard rejection / success) must emit an explicit log line, so
    // operators rolling back a snapshot can confirm at a glance whether
    // it took. Pre-#370 the success path was silent and the stale-guard
    // used `log::debug!`, making a rejected rollback indistinguishable
    // from a successful one without comparing `[ZCHECK] hist=` over time.
    if !history_path.exists() {
        // bot-strategy#377: dedup the no-file INFO. On a fresh state dir
        // the file does not exist until the first persist tick lands, so
        // the per-tick loader would emit this INFO every 5 s in between
        // (≥ once before the first save). Operators only need one
        // "starting cold" line per missing-file episode.
        let key = String::from("no_snapshot");
        if last_logged_key.as_deref() != Some(key.as_str()) {
            log::info!(
                "[WARM_START] no snapshot at {} — cold start",
                history_path.display()
            );
            *last_logged_key = Some(key);
        }
        return;
    }
    let snap = match parse_snapshot_file(history_path) {
        Ok(s) => s,
        Err(e) => {
            // bot-strategy#377: dedup parse-error WARN. The function runs
            // per-tick (engine/step.rs:511) so a corrupt file would emit
            // 12 lines/min until manually replaced; only re-emit when the
            // error text actually changes.
            let key = format!("parse_error:{}", e);
            if last_logged_key.as_deref() != Some(key.as_str()) {
                log::warn!(
                    "[WARM_START] snapshot at {} could not be loaded: {} — cold start",
                    history_path.display(),
                    e,
                );
                *last_logged_key = Some(key);
            }
            return;
        }
    };
    // Snapshot ts is ms post bot-strategy#274 / #276; lift `now_ts` (wall-clock
    // seconds) into the same unit before comparing.
    let now_ts_ms = now_ts.saturating_mul(1000);
    let max_age_ms = (max_history_len as i64)
        .saturating_mul(cfg.trading_period_secs as i64)
        .saturating_mul(1000);
    // Stale-history guard (pairtrade#4): if the newest sample for a symbol
    // is older than a few bars, the persisted file is from a stopped bot
    // and replaying it would freeze a stale rolling window. Drop it and
    // let the live feed warm up from scratch. bot-strategy#341 widened the
    // window from 5×period to max(5min, 30×period) so a routine restart
    // (CI deploy + manual stop typically takes 5-15min) admits the warm
    // start; only genuinely hours-old files fall through.
    let stale_threshold_ms = (cfg.trading_period_secs as i64)
        .saturating_mul(30)
        .max(300)
        .saturating_mul(1000);
    let mut any_stale = false;
    let mut loaded_summary: Vec<(String, usize)> = Vec::new();
    let mut stale_summary: Vec<(String, i64)> = Vec::new();
    for (sym, entries) in snap.prices {
        let newest_ts = entries.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
        let age_ms = now_ts_ms.saturating_sub(newest_ts);
        if age_ms > stale_threshold_ms {
            stale_summary.push((sym, age_ms));
            any_stale = true;
            continue;
        }
        let mut deque = VecDeque::new();
        for (log_price, ts) in entries {
            if now_ts_ms.saturating_sub(ts) > max_age_ms {
                continue;
            }
            deque.push_back(PriceSample { log_price, ts });
        }
        if !deque.is_empty() {
            loaded_summary.push((sym.clone(), deque.len()));
            history.insert(sym, deque);
        }
    }
    // If any symbol was discarded as stale, the persisted spread_history
    // is also stale — discard it rather than pairing it with a
    // freshly-built log_price window. This triggers the cold-start
    // synthesis path in `warm_start_states_from_history`, which is still
    // the fallback for genuinely stale files.
    let mut spreads_loaded: Vec<(String, usize)> = Vec::new();
    if !any_stale {
        for (pair_key, series) in snap.spread_histories {
            if series.is_empty() {
                continue;
            }
            let len = series.len();
            let deque: VecDeque<f64> = series.into_iter().collect();
            spreads_loaded.push((pair_key.clone(), len));
            spread_histories_out.insert(pair_key, deque);
        }
        // bot-strategy#413: lift persisted β and Kalman state into the
        // engine's shared per-pair store. Empty for pre-v4 snapshots.
        for (pair_key, beta) in snap.betas {
            betas_out.insert(pair_key, beta);
        }
        for (pair_key, kalman) in snap.kalman_states {
            kalman_states_out.insert(pair_key, kalman);
        }
    }
    // bot-strategy#370: emit one of three terminal log lines so operators
    // can grep on `[WARM_START]` to confirm intent.
    // bot-strategy#377: dedup non-success WARNs the same way as the
    // success path below. `load_history_from_disk` runs per-tick, so a
    // cold-start / partial-stale / empty-snapshot state would otherwise
    // emit 12 lines/min × hours until a fresher snapshot lands. Each
    // outcome picks a fingerprint that captures the operationally
    // meaningful signal (outcome category + version + symbol counts)
    // so a genuine transition still re-emits.
    if !stale_summary.is_empty() && loaded_summary.is_empty() {
        // Every symbol failed the stale-guard — the canonical failure
        // mode behind the 2026-05-12 03:51 UTC silent-reject incident.
        let key = format!("rejected_all:v{}:n{}", snap.version, stale_summary.len(),);
        if last_logged_key.as_deref() != Some(key.as_str()) {
            let oldest_min = stale_summary.iter().map(|(_, a)| *a).max().unwrap_or(0) / 60_000;
            let threshold_min = stale_threshold_ms / 60_000;
            log::warn!(
                "[WARM_START] snapshot at {} rejected as stale: v{}, {} symbol(s) all older than {}min (oldest {}min) — cold start. \
                 Roll-back beyond stale-guard is intentional but loses warm start; restart with a fresher backup or accept the warm-up cost.",
                history_path.display(),
                snap.version,
                stale_summary.len(),
                threshold_min,
                oldest_min,
            );
            *last_logged_key = Some(key);
        }
    } else if !stale_summary.is_empty() {
        let key = format!(
            "partial_stale:v{}:k{}:d{}",
            snap.version,
            loaded_summary.len(),
            stale_summary.len(),
        );
        if last_logged_key.as_deref() != Some(key.as_str()) {
            log::warn!(
                "[WARM_START] snapshot at {} partial-stale: kept {} fresh symbol(s), dropped {} stale; spread_histories discarded (would mismatch refreshed bars)",
                history_path.display(),
                loaded_summary.len(),
                stale_summary.len(),
            );
            *last_logged_key = Some(key);
        }
    } else if loaded_summary.is_empty() && spreads_loaded.is_empty() {
        let key = format!("empty:v{}", snap.version);
        if last_logged_key.as_deref() != Some(key.as_str()) {
            log::warn!(
                "[WARM_START] snapshot at {} parsed (v{}) but contained no usable bars — cold start",
                history_path.display(),
                snap.version,
            );
            *last_logged_key = Some(key);
        }
    } else {
        // bot-strategy#370 follow-up: dedup the success-path INFO so the
        // per-tick reload in `engine/step.rs:511` doesn't fire ~12 lines/min
        // on the 5 s polling cadence. Stale-guard and parse-error paths
        // above stay unconditional so operator-facing failure modes never
        // get muted; only the steady-state success spam is suppressed.
        //
        // Two sources of fingerprint instability defeat a naive dedup:
        //
        //   1. HashMap iteration order — `snap.prices` /
        //      `snap.spread_histories` are `HashMap`s, so the raw Debug
        //      representation rotates symbol order tick-to-tick. Sort by
        //      symbol before serialising.
        //
        //   2. Per-tick count drift — `load_history_from_disk` filters
        //      samples by `max_age_ms`, so as wall-clock advances the
        //      oldest sample slides out of the window and the loaded
        //      count oscillates by ±1 between persists.
        //
        // bot-strategy#466: collapse the count into a single "is the
        // window full?" boolean (`>= 90 % of max_history_len`) instead
        // of the prior `c / 10 * 10` floor bucket. The floor approach
        // still had boundaries every 10 samples, and in production the
        // typical count sits **on** the boundary (`max_history_len = 240`):
        // a 239 ↔ 240 oscillation flipped between buckets 230 and 240
        // tick after tick, defeating dedup entirely. Frankfurt emitted
        // 2.6 lines/min, Tokyo Extended 3.7/min, both saturating
        // `[WARM_START]` log volume to no operator value. The 90 %
        // threshold makes routine ± 1 wobble at any point inside the
        // full / not-full band collapse to a single key. The emitted log
        // line still surfaces the actual count for operator visibility —
        // only the dedup key uses the boolean.
        //
        // A meaningful rollback (e.g. 240-sample → 50-sample snapshot,
        // or symbol set change) crosses the full/not-full boundary or
        // changes the symbol set and emits as intended; routine ± 1
        // wobble stays quiet.
        let mut sorted_prices = loaded_summary.clone();
        sorted_prices.sort_by(|a, b| a.0.cmp(&b.0));
        let mut sorted_spreads = spreads_loaded.clone();
        sorted_spreads.sort_by(|a, b| a.0.cmp(&b.0));
        let key = warm_start_success_dedup_key(
            snap.version,
            &sorted_prices,
            &sorted_spreads,
            max_history_len,
        );
        if last_logged_key.as_deref() != Some(key.as_str()) {
            log::info!(
                "[WARM_START] snapshot loaded from {}: v{}, prices={:?}, spread_histories={:?}",
                history_path.display(),
                snap.version,
                sorted_prices,
                sorted_spreads,
            );
            *last_logged_key = Some(key);
        }
    }
}

/// Dedup fingerprint for the `[WARM_START] snapshot loaded ...` success
/// log. `load_history_from_disk` runs every tick (see `engine/step.rs`),
/// so the success path would emit ~12 lines/min without dedup. The
/// fingerprint must be stable under per-tick noise (count drift, HashMap
/// iteration order — handled by the caller sorting before invoking
/// this) and only change on operationally meaningful transitions
/// (snapshot version change, symbol-set change, full ↔ not-full).
///
/// `max_history_len` is the size of the rolling window the engine
/// expects; counts ≥ 90 % of that are reported as "full". See
/// bot-strategy#466 for the boundary bug that motivated the boolean
/// representation (the prior `c / 10 * 10` floor flipped buckets every
/// time the count crossed a multiple of 10, defeating dedup whenever
/// the typical count sat on a multiple — which is the production case
/// at `max_history_len = 240`).
pub(super) fn warm_start_success_dedup_key(
    version: u32,
    sorted_prices: &[(String, usize)],
    sorted_spreads: &[(String, usize)],
    max_history_len: usize,
) -> String {
    let full_threshold = max_history_len.saturating_mul(9) / 10;
    let bucket = |v: &[(String, usize)]| -> Vec<(String, bool)> {
        v.iter()
            .map(|(s, c)| (s.clone(), *c >= full_threshold))
            .collect()
    };
    format!(
        "v{} {:?} {:?}",
        version,
        bucket(sorted_prices),
        bucket(sorted_spreads),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn write_snapshot(content: &str) -> NamedTempFile {
        let f = NamedTempFile::new().unwrap();
        std::fs::write(f.path(), content).unwrap();
        f
    }

    #[test]
    fn parse_v3_snapshot_passes_ts_through_unchanged() {
        let json = r#"{
            "_v": 3,
            "prices": {"BTC": [[10.5, 1776232919000], [10.6, 1776232979000]]},
            "spread_histories": {"BTC/ETH": [0.1, 0.2]}
        }"#;
        let f = write_snapshot(json);
        let snap = parse_snapshot_file(f.path()).unwrap();
        assert_eq!(snap.version, 3);
        assert_eq!(
            snap.prices.get("BTC").unwrap(),
            &vec![(10.5, 1776232919000), (10.6, 1776232979000)]
        );
        assert_eq!(
            snap.spread_histories.get("BTC/ETH").unwrap(),
            &vec![0.1, 0.2]
        );
        // bot-strategy#413: pre-v4 snapshots default the new fields to empty
        // so the engine falls back to OLS warm-start / fresh Kalman, matching
        // pre-#413 behaviour.
        assert!(snap.betas.is_empty());
        assert!(snap.kalman_states.is_empty());
    }

    #[test]
    fn parse_v4_snapshot_round_trips_betas_and_kalman() {
        let json = r#"{
            "_v": 4,
            "prices": {"BTC": [[10.5, 1776232919000]]},
            "spread_histories": {"BTC/ETH": [0.1, 0.2]},
            "betas": {"BTC/ETH": 0.8123},
            "kalman_states": {
                "BTC/ETH": {"beta": 0.812, "p": 1.5e-4, "updates": 9876}
            }
        }"#;
        let f = write_snapshot(json);
        let snap = parse_snapshot_file(f.path()).unwrap();
        assert_eq!(snap.version, 4);
        assert_eq!(*snap.betas.get("BTC/ETH").unwrap(), 0.8123);
        let kf = snap.kalman_states.get("BTC/ETH").unwrap();
        assert_eq!(kf.beta, 0.812);
        assert_eq!(kf.p, 1.5e-4);
        assert_eq!(kf.updates, 9876);
    }

    #[test]
    fn parse_v2_snapshot_migrates_ts_seconds_to_ms() {
        // v2 stored ts in seconds. After bot-strategy#274 / #276 the loader
        // must lift each (log_price, ts) pair into ms by × 1000 so it lines
        // up with the post-bump BarBuilder bucket math.
        let json = r#"{
            "_v": 2,
            "prices": {"BTC": [[10.5, 1776232919], [10.6, 1776232979]]},
            "spread_histories": {"BTC/ETH": [0.1, 0.2]}
        }"#;
        let f = write_snapshot(json);
        let snap = parse_snapshot_file(f.path()).unwrap();
        assert_eq!(snap.version, 2);
        assert_eq!(
            snap.prices.get("BTC").unwrap(),
            &vec![(10.5, 1776232919000), (10.6, 1776232979000)],
            "v2 ts must be migrated to ms",
        );
        // spread_histories carry no timestamp, just pass through.
        assert_eq!(
            snap.spread_histories.get("BTC/ETH").unwrap(),
            &vec![0.1, 0.2]
        );
    }

    #[test]
    fn parse_v1_snapshot_migrates_ts_seconds_to_ms() {
        // v1 was a bare per-symbol map with seconds ts. Same migration path.
        let json = r#"{"BTC": [[10.5, 1776232919], [10.6, 1776232979]]}"#;
        let f = write_snapshot(json);
        let snap = parse_snapshot_file(f.path()).unwrap();
        assert_eq!(snap.version, 1);
        assert_eq!(
            snap.prices.get("BTC").unwrap(),
            &vec![(10.5, 1776232919000), (10.6, 1776232979000)]
        );
        assert!(snap.spread_histories.is_empty());
    }

    #[test]
    fn parse_unsupported_struct_version_returns_err() {
        // A struct-shaped file with _v=0 (or any explicit version < 2)
        // must surface a parse error so the WARM_START log records why.
        let json = r#"{
            "_v": 0,
            "prices": {"BTC": [[10.5, 1776232919]]}
        }"#;
        let f = write_snapshot(json);
        let err = parse_snapshot_file(f.path()).unwrap_err();
        assert!(
            err.contains("_v=0") || err.contains("not supported"),
            "got: {}",
            err
        );
    }

    #[test]
    fn parse_garbage_returns_err() {
        let f = write_snapshot("not json at all");
        let err = parse_snapshot_file(f.path()).unwrap_err();
        assert!(
            err.contains("JSON did not match"),
            "expected v1/v2/v3/v4-shape error, got: {}",
            err
        );
    }

    #[test]
    fn parse_missing_file_returns_err() {
        let path = std::path::Path::new("/tmp/pairtrade_history_io_test_missing_xyz.json");
        let _ = std::fs::remove_file(path);
        let err = parse_snapshot_file(path).unwrap_err();
        assert!(err.contains("read failed"), "got: {}", err);
    }

    // bot-strategy#466: production observation — Frankfurt + Tokyo
    // Extended both emit `[WARM_START] snapshot loaded ...` at 2-4 / min
    // because the loaded count oscillates 239 ↔ 240 (one sample slides
    // out of the `max_age_ms` window each ~30 s persist cycle) and the
    // pre-fix `c / 10 * 10` bucket flipped between 230 and 240 each
    // time, never matching the previous key. Locks in the boolean
    // "full / not full" key so 239 ↔ 240 stays in the same fingerprint.
    #[test]
    fn warm_start_dedup_key_stable_across_one_sample_drift_at_full_window() {
        let max_len = 240;
        let prices_239 = vec![
            ("BTC".to_string(), 239),
            ("ETH".to_string(), 239),
        ];
        let prices_240 = vec![
            ("BTC".to_string(), 240),
            ("ETH".to_string(), 240),
        ];
        let prices_mixed = vec![
            ("BTC".to_string(), 239),
            ("ETH".to_string(), 240),
        ];
        let spreads = vec![("BTC/ETH".to_string(), 240)];
        let key_239 = warm_start_success_dedup_key(4, &prices_239, &spreads, max_len);
        let key_240 = warm_start_success_dedup_key(4, &prices_240, &spreads, max_len);
        let key_mixed = warm_start_success_dedup_key(4, &prices_mixed, &spreads, max_len);
        assert_eq!(key_239, key_240, "239 vs 240 must dedup");
        assert_eq!(key_239, key_mixed, "(239, 240) mixed must dedup with (239, 239)");
    }

    // The dedup must still fire on meaningful transitions, otherwise
    // operators rolling back a snapshot from 240 → 50 samples would not
    // see confirmation in the log.
    #[test]
    fn warm_start_dedup_key_changes_on_full_to_partial_rollback() {
        let max_len = 240;
        let prices_full = vec![("BTC".to_string(), 240), ("ETH".to_string(), 240)];
        let prices_partial = vec![("BTC".to_string(), 50), ("ETH".to_string(), 50)];
        let spreads = vec![("BTC/ETH".to_string(), 240)];
        let key_full = warm_start_success_dedup_key(4, &prices_full, &spreads, max_len);
        let key_partial = warm_start_success_dedup_key(4, &prices_partial, &spreads, max_len);
        assert_ne!(
            key_full, key_partial,
            "240→50 rollback must cross the full/not-full boundary and re-emit"
        );
    }

    #[test]
    fn warm_start_dedup_key_changes_on_version_bump() {
        let max_len = 240;
        let prices = vec![("BTC".to_string(), 240), ("ETH".to_string(), 240)];
        let spreads = vec![("BTC/ETH".to_string(), 240)];
        let key_v3 = warm_start_success_dedup_key(3, &prices, &spreads, max_len);
        let key_v4 = warm_start_success_dedup_key(4, &prices, &spreads, max_len);
        assert_ne!(key_v3, key_v4, "snapshot version change must re-emit");
    }

    #[test]
    fn warm_start_dedup_key_changes_on_symbol_set_change() {
        let max_len = 240;
        let spreads = vec![("BTC/ETH".to_string(), 240)];
        let prices_two = vec![("BTC".to_string(), 240), ("ETH".to_string(), 240)];
        let prices_one = vec![("BTC".to_string(), 240)];
        let key_two = warm_start_success_dedup_key(4, &prices_two, &spreads, max_len);
        let key_one = warm_start_success_dedup_key(4, &prices_one, &spreads, max_len);
        assert_ne!(
            key_two, key_one,
            "ETH dropping out of the snapshot must re-emit"
        );
    }
}
