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
/// migrated (`ts × 1000`) on load. This file is rewritten as v3 on the next
/// `persist_history_to_disk` call, so the migration is one-way.
///
/// The loader parses the explicit struct first and falls back to v1 (bare
/// per-symbol map) on failure, so pre-existing history files keep working.
#[derive(Serialize, Deserialize, Default)]
struct SnapshotV3 {
    #[serde(rename = "_v")]
    version: u32,
    prices: HashMap<String, Vec<(f64, i64)>>,
    /// Pair key (e.g. "BTC/ETH") → the live engine's
    /// `state.spread_history` as a plain `Vec<f64>`. Missing in older
    /// files; defaulted to empty by `#[serde(default)]`.
    #[serde(default)]
    spread_histories: HashMap<String, Vec<f64>>,
}

const SNAPSHOT_VERSION: u32 = 3;

pub(super) fn persist_history_to_disk(
    cfg: &PairTradeConfig,
    history: &HashMap<String, VecDeque<PriceSample>>,
    spread_histories: &HashMap<String, VecDeque<f64>>,
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
    let snapshot = SnapshotV3 {
        version: SNAPSHOT_VERSION,
        prices,
        spread_histories,
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
        archive_path.file_name().unwrap_or_default().to_string_lossy()
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
        let Ok(modified) = meta.modified() else { continue };
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
}

/// Parse the persisted history file, accepting v3 (explicit struct with
/// ms-precision `ts`), v2 (explicit struct with seconds `ts`, auto-migrated
/// by × 1000) and legacy v1 (bare per-symbol map, also seconds, auto-migrated).
/// Returns a `ParsedSnapshot` with `ts` always normalized to ms, or a
/// human-readable error string for the caller to log (bot-strategy#370 —
/// every load outcome must be greppable from journalctl).
fn parse_snapshot_file(path: &std::path::Path) -> Result<ParsedSnapshot, String> {
    let content = fs::read_to_string(path).map_err(|e| format!("read failed: {}", e))?;
    // Try the explicit struct first (has `_v` and `prices`).
    if let Ok(snap) = serde_json::from_str::<SnapshotV3>(&content) {
        if snap.version >= 2 {
            let prices = if snap.version < SNAPSHOT_VERSION {
                migrate_prices_seconds_to_ms(snap.prices)
            } else {
                snap.prices
            };
            return Ok(ParsedSnapshot {
                version: snap.version,
                prices,
                spread_histories: snap.spread_histories,
            });
        }
        return Err(format!(
            "schema _v={} not supported (expected 2 or 3, or v1 bare-map)",
            snap.version
        ));
    }
    // Fall back to v1 (bare `HashMap<String, Vec<(f64, i64)>>` in seconds).
    match serde_json::from_str::<HashMap<String, Vec<(f64, i64)>>>(&content) {
        Ok(prices) => Ok(ParsedSnapshot {
            version: 1,
            prices: migrate_prices_seconds_to_ms(prices),
            spread_histories: HashMap::new(),
        }),
        Err(e) => Err(format!(
            "JSON did not match v2/v3 struct or v1 bare-map shape: {}",
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
/// each symbol, regardless of `now_ts`. Also populates
/// `spread_histories_out` when the snapshot is v2.
pub(super) fn load_history_snapshot_for_bt(
    history: &mut HashMap<String, VecDeque<PriceSample>>,
    spread_histories_out: &mut HashMap<String, VecDeque<f64>>,
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
}

pub(super) fn load_history_from_disk(
    cfg: &PairTradeConfig,
    history: &mut HashMap<String, VecDeque<PriceSample>>,
    spread_histories_out: &mut HashMap<String, VecDeque<f64>>,
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
        let key = format!(
            "rejected_all:v{}:n{}",
            snap.version,
            stale_summary.len(),
        );
        if last_logged_key.as_deref() != Some(key.as_str()) {
            let oldest_min = stale_summary
                .iter()
                .map(|(_, a)| *a)
                .max()
                .unwrap_or(0)
                / 60_000;
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
        //      count oscillates by ±1 between persists. Bucket the count
        //      to the nearest 10 in the fingerprint so this micro-drift
        //      doesn't keep flipping the key. The emitted log line still
        //      shows the actual count for operator visibility — only the
        //      dedup decision uses the bucket.
        //
        // A meaningful rollback (e.g. 240-sample → 50-sample snapshot,
        // or symbol set change) crosses bucket boundaries and emits as
        // intended; routine ±1 wobble stays quiet.
        let mut sorted_prices = loaded_summary.clone();
        sorted_prices.sort_by(|a, b| a.0.cmp(&b.0));
        let mut sorted_spreads = spreads_loaded.clone();
        sorted_spreads.sort_by(|a, b| a.0.cmp(&b.0));
        let bucket = |v: &[(String, usize)]| -> Vec<(String, usize)> {
            v.iter().map(|(s, c)| (s.clone(), c / 10 * 10)).collect()
        };
        let key = format!(
            "v{} {:?} {:?}",
            snap.version,
            bucket(&sorted_prices),
            bucket(&sorted_spreads),
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
        assert!(err.contains("_v=0"), "got: {}", err);
    }

    #[test]
    fn parse_garbage_returns_err() {
        let f = write_snapshot("not json at all");
        let err = parse_snapshot_file(f.path()).unwrap_err();
        assert!(
            err.contains("JSON did not match"),
            "expected v1/v2/v3-shape error, got: {}",
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
}
