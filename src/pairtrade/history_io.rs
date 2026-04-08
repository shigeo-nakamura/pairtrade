//! On-disk history persistence helpers extracted from the monolithic
//! pairtrade module.

use std::collections::{HashMap, VecDeque};
use std::fs;

use super::config::PairTradeConfig;
use super::stats::PriceSample;

pub(super) fn persist_history_to_disk(
    cfg: &PairTradeConfig,
    history: &HashMap<String, VecDeque<PriceSample>>,
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
    let mut snapshot: HashMap<String, Vec<(f64, i64)>> = HashMap::new();
    for (sym, deque) in history {
        let v: Vec<(f64, i64)> = deque.iter().map(|p| (p.log_price, p.ts)).collect();
        snapshot.insert(sym.clone(), v);
    }
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
}

pub(super) fn load_history_from_disk(
    cfg: &PairTradeConfig,
    history: &mut HashMap<String, VecDeque<PriceSample>>,
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
    let Ok(content) = std::fs::read_to_string(history_path) else {
        return;
    };
    let parsed: Result<HashMap<String, Vec<(f64, i64)>>, _> = serde_json::from_str(&content);
    let Ok(map) = parsed else {
        return;
    };
    let max_age_secs =
        (max_history_len as i64).saturating_mul(cfg.trading_period_secs as i64);
    // Stale-history guard (pairtrade#4): if the newest sample for a symbol
    // is older than a few bars, the persisted file is from a stopped bot
    // and replaying it would freeze a stale rolling window. Drop it and
    // let the live feed warm up from scratch.
    let stale_threshold_secs = (cfg.trading_period_secs as i64).saturating_mul(5).max(60);
    for (sym, entries) in map {
        let newest_ts = entries.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
        if now_ts.saturating_sub(newest_ts) > stale_threshold_secs {
            log::warn!(
                "discarding stale persisted history for {}: newest sample {}s old",
                sym,
                now_ts.saturating_sub(newest_ts)
            );
            continue;
        }
        let mut deque = VecDeque::new();
        for (log_price, ts) in entries {
            if now_ts.saturating_sub(ts) > max_age_secs {
                continue;
            }
            deque.push_back(PriceSample { log_price, ts });
        }
        if !deque.is_empty() {
            history.insert(sym, deque);
        }
    }
}
