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

/// Parse the persisted history file, accepting v3 (explicit struct with
/// ms-precision `ts`), v2 (explicit struct with seconds `ts`, auto-migrated
/// by × 1000) and legacy v1 (bare per-symbol map, also seconds, auto-migrated).
/// Returns (prices, spread_histories) with `ts` always normalized to ms.
fn parse_snapshot_file(
    path: &std::path::Path,
) -> Option<(
    HashMap<String, Vec<(f64, i64)>>,
    HashMap<String, Vec<f64>>,
)> {
    let content = fs::read_to_string(path).ok()?;
    // Try the explicit struct first (has `_v` and `prices`).
    if let Ok(snap) = serde_json::from_str::<SnapshotV3>(&content) {
        if snap.version >= 2 {
            let prices = if snap.version < SNAPSHOT_VERSION {
                migrate_prices_seconds_to_ms(snap.prices)
            } else {
                snap.prices
            };
            return Some((prices, snap.spread_histories));
        }
    }
    // Fall back to v1 (bare `HashMap<String, Vec<(f64, i64)>>` in seconds).
    let prices: HashMap<String, Vec<(f64, i64)>> = serde_json::from_str(&content).ok()?;
    Some((migrate_prices_seconds_to_ms(prices), HashMap::new()))
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
    let Some((prices, spreads)) = parse_snapshot_file(snapshot_path) else {
        log::warn!(
            "[BT_WARM_START] failed to read or parse snapshot {}",
            snapshot_path.display()
        );
        return;
    };
    for (sym, entries) in prices {
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
    for (pair_key, series) in spreads {
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
    let Some((prices, spreads)) = parse_snapshot_file(history_path) else {
        return;
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
    // let the live feed warm up from scratch.
    let stale_threshold_ms = (cfg.trading_period_secs as i64)
        .saturating_mul(5)
        .max(60)
        .saturating_mul(1000);
    let mut any_stale = false;
    for (sym, entries) in prices {
        let newest_ts = entries.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
        if now_ts_ms.saturating_sub(newest_ts) > stale_threshold_ms {
            log::debug!(
                "discarding stale persisted history for {}: newest sample {}ms old",
                sym,
                now_ts_ms.saturating_sub(newest_ts)
            );
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
            history.insert(sym, deque);
        }
    }
    // If any symbol was discarded as stale, the persisted spread_history
    // is also stale — discard it rather than pairing it with a
    // freshly-built log_price window. This triggers the cold-start
    // synthesis path in `warm_start_states_from_history`, which is still
    // the fallback for genuinely stale files.
    if !any_stale {
        for (pair_key, series) in spreads {
            if series.is_empty() {
                continue;
            }
            let deque: VecDeque<f64> = series.into_iter().collect();
            spread_histories_out.insert(pair_key, deque);
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
        let (prices, spreads) = parse_snapshot_file(f.path()).unwrap();
        assert_eq!(
            prices.get("BTC").unwrap(),
            &vec![(10.5, 1776232919000), (10.6, 1776232979000)]
        );
        assert_eq!(spreads.get("BTC/ETH").unwrap(), &vec![0.1, 0.2]);
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
        let (prices, spreads) = parse_snapshot_file(f.path()).unwrap();
        assert_eq!(
            prices.get("BTC").unwrap(),
            &vec![(10.5, 1776232919000), (10.6, 1776232979000)],
            "v2 ts must be migrated to ms",
        );
        // spread_histories carry no timestamp, just pass through.
        assert_eq!(spreads.get("BTC/ETH").unwrap(), &vec![0.1, 0.2]);
    }

    #[test]
    fn parse_v1_snapshot_migrates_ts_seconds_to_ms() {
        // v1 was a bare per-symbol map with seconds ts. Same migration path.
        let json = r#"{"BTC": [[10.5, 1776232919], [10.6, 1776232979]]}"#;
        let f = write_snapshot(json);
        let (prices, spreads) = parse_snapshot_file(f.path()).unwrap();
        assert_eq!(
            prices.get("BTC").unwrap(),
            &vec![(10.5, 1776232919000), (10.6, 1776232979000)]
        );
        assert!(spreads.is_empty());
    }
}
