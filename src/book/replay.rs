//! Deterministic replay: daily bars + per-decision signal files through the
//! same engine with a synthetic clock (`docs/book-runtime.md` §9).
//!
//! Input directory layout:
//!
//! ```text
//! <replay_dir>/bars.jsonl          {"date":"2026-07-03","symbol":"SOL","close":150.2,"funding_rate_hourly":0.00001}
//! <replay_dir>/lots.json           {"SOL":{"size_decimals":2,"min_order_qty":null}, ...}   (optional)
//! <replay_dir>/signals/<key>.json  signal files, one per decision key
//! ```
//!
//! For each bar date `D` (ascending): closes of `D` become the prices, every
//! decision / flatten scheduled inside `D` is ticked at its exact time, and
//! after the final tick at `D 23:59:59` the daily mark (labelled `D`) is
//! written. Fills are paper fills at the close of `D`. A decision at the
//! next midnight is ticked in the next iteration, after that date's closes
//! are loaded.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use serde::Deserialize;

use super::config::BookConfig;
use super::engine::{BookEngine, DirSignalSource};
use super::executor::{Executor, PaperExecutor};
use super::rebalance::LotMeta;
use super::schedule::Scheduler;
use super::status::StatusWriter;

#[derive(Debug, Clone, Deserialize)]
pub struct BarRow {
    pub date: NaiveDate,
    pub symbol: String,
    pub close: f64,
    #[serde(default)]
    pub funding_rate_hourly: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct ReplaySummary {
    pub days: usize,
    pub ticks: usize,
    pub final_equity: f64,
    pub cum_realized_usd: f64,
    pub cum_fees_usd: f64,
    pub cum_funding_est_usd: f64,
    pub trades_closed: u64,
}

pub fn load_bars(path: &Path) -> Result<BTreeMap<NaiveDate, HashMap<String, BarRow>>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut out: BTreeMap<NaiveDate, HashMap<String, BarRow>> = BTreeMap::new();
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let row: BarRow =
            serde_json::from_str(line).with_context(|| format!("{}:{}", path.display(), i + 1))?;
        if !(row.close.is_finite() && row.close > 0.0) {
            bail!("{}:{}: non-positive close", path.display(), i + 1);
        }
        let day = out.entry(row.date).or_default();
        if let Some(prev) = day.get(&row.symbol) {
            // Silently keeping the last row would let a duplicated or
            // concatenated dataset change fills, funding and the final
            // equity by row order alone.
            bail!(
                "{}:{}: duplicate row for {} on {} (close {} then {})",
                path.display(),
                i + 1,
                row.symbol,
                row.date,
                prev.close,
                row.close
            );
        }
        day.insert(row.symbol.clone(), row);
    }
    if out.is_empty() {
        bail!("{} has no bars", path.display());
    }
    Ok(out)
}

/// Rewrite every on-disk path of `cfg` into `out_dir` and force paper mode.
pub fn sandbox_config(mut cfg: BookConfig, out_dir: &Path) -> BookConfig {
    cfg.dry_run = true;
    cfg.paths.state = out_dir.join("state.json");
    cfg.paths.ledger = out_dir.join("ledger.jsonl");
    cfg.paths.pnl = out_dir.join("pnl.jsonl");
    cfg.paths.status = out_dir.join("status.json");
    cfg.risk.kill_switch_path = out_dir.join("KILL_SWITCH");
    cfg.risk.risk_ack_path = out_dir.join("RISK_ACK");
    cfg
}

/// Files a replay writes into `--out`; removed before every run so a
/// rerun never resumes from a previous run's state or appends to its
/// ledgers (the byte-identical guarantee depends on this).
pub const OUTPUT_FILES: &[&str] = &[
    "state.json",
    "ledger.jsonl",
    "pnl.jsonl",
    "status.json",
    "KILL_SWITCH",
    "RISK_ACK",
];

/// The most decimals a venue lot size can plausibly carry; anything above
/// this in a fixture is a typo, and above `i32::MAX` it would wrap.
const MAX_SIZE_DECIMALS: u32 = 12;

pub async fn run(cfg: BookConfig, replay_dir: &Path, out_dir: &Path) -> Result<ReplaySummary> {
    std::fs::create_dir_all(out_dir).with_context(|| format!("create {}", out_dir.display()))?;
    for f in OUTPUT_FILES {
        let p = out_dir.join(f);
        match std::fs::remove_file(&p) {
            Ok(()) => log::info!("[REPLAY] removed previous {}", p.display()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("remove {}", p.display())),
        }
    }
    let cfg = sandbox_config(cfg, out_dir);
    let bars = load_bars(&replay_dir.join("bars.jsonl"))?;
    let lots: HashMap<String, LotMeta> = match std::fs::read_to_string(replay_dir.join("lots.json"))
    {
        Ok(t) => serde_json::from_str(&t).context("parse lots.json")?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
        Err(e) => return Err(e).context("read lots.json"),
    };
    // Rounding casts `size_decimals` to i32, so an out-of-range fixture
    // value would wrap to a negative one and quietly round every quantity
    // to tens instead of failing. No venue quotes more than 12 decimals.
    for (sym, l) in &lots {
        if l.size_decimals > MAX_SIZE_DECIMALS {
            bail!(
                "lots.json {sym}: size_decimals {} is out of range (0..={MAX_SIZE_DECIMALS})",
                l.size_decimals
            );
        }
        if let Some(m) = l.min_order_qty {
            if !m.is_finite() || m < 0.0 {
                bail!("lots.json {sym}: min_order_qty {m} must be finite and >= 0");
            }
        }
    }
    let exec = Arc::new(PaperExecutor::new_for_replay(
        cfg.execution.paper_slippage_bps,
        cfg.execution.paper_fee_bps,
    ));
    for s in &cfg.universe.symbols {
        let lot = lots.get(s).copied().unwrap_or(LotMeta {
            size_decimals: 4,
            min_order_qty: None,
        });
        exec.set_lot(s, lot).await;
    }
    let scheduler = Scheduler::from_config(&cfg.schedule)?;
    let status = StatusWriter::new(cfg.paths.status.clone(), None);
    let signals = Box::new(DirSignalSource::new(replay_dir.join("signals")));
    let mut engine = BookEngine::new(
        cfg.clone(),
        scheduler.clone(),
        exec.clone(),
        signals,
        status,
    )?;
    engine.status_interval_secs = 0;
    engine.mark_on_date_change = false;

    // Bar dates must be continuous. A missing date gets no ticks at all,
    // so a decision or flatten inside it silently vanishes, and even when
    // nothing is scheduled there the day's mark and funding accrual are
    // skipped while the book is still open -- the next date's rate would
    // then be applied across the whole multi-day interval.
    if let (Some(first), Some(last)) = (bars.keys().next(), bars.keys().next_back()) {
        let mut d = *first;
        while d < *last {
            d += Duration::days(1);
            if !bars.contains_key(&d) {
                bail!(
                    "bars.jsonl skips {d} ({} .. {} must be continuous: a gap loses that day's decisions, flattens, mark and funding)",
                    first,
                    last
                );
            }
        }
    }

    let mut summary = ReplaySummary::default();
    for (date, rows) in &bars {
        // Every leg the book still holds must be marked on this date:
        // without a price the mark, funding and equity would silently fall
        // back to the entry basis, reporting zero movement and possibly
        // suppressing a drawdown halt.
        for sym in engine.state.positions.keys() {
            if !rows.contains_key(sym) {
                bail!("bars.jsonl has no {sym} row for {date}, but the book still holds that leg");
            }
        }
        // Each date is a complete snapshot: nothing carries over from the
        // previous date, so an omitted symbol or funding rate is absent
        // (a decision on it is rejected with `missing_price`) rather than
        // silently reusing yesterday's value.
        exec.clear_observations().await;
        for (sym, row) in rows {
            exec.set_price(sym, row.close).await;
            if let Some(fr) = row.funding_rate_hourly {
                exec.set_funding_rate_hourly(sym, fr).await;
            }
        }
        let day_start = Utc
            .from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap())
            .timestamp();
        let day_end = day_start + 86_400;
        let mut ticks: Vec<i64> = Vec::new();
        // A decision whose grace window was inherited from an earlier
        // date (decision_at before today, window_end landing inside
        // today) has no tick of its own scheduled today otherwise: the
        // decisions_between loop below only sees decisions whose
        // decision_at falls on *this* date. Live, the 5 s loop ticks
        // through window_end regardless; without an explicit tick here,
        // a later date's own decision can silently supersede this key
        // once its own decision_at arrives, and the expired window is
        // never recorded as `skipped` at all.
        if let Some(d) = scheduler.current(day_start) {
            if d.decision_at < day_start && (day_start..day_end).contains(&d.window_end) {
                ticks.push((d.window_end + 1).min(day_end - 1));
            }
        }
        for d in scheduler.decisions_between(day_start, day_end - 1) {
            ticks.push(d.decision_at);
            if let Some(f) = d.flatten_at {
                if (day_start..day_end).contains(&f) {
                    ticks.push(f);
                }
            }
        }
        // A producer may publish after the decision instant, anywhere
        // inside the grace window. Live, the fetch timer delivers the file
        // and the next 5 s tick applies it. Here the following tick would
        // otherwise be the end of the day, so anything generated more than
        // the clock-skew allowance after the decision would be rejected
        // once as future-generated and never retried. Tick at its stated
        // arrival instead. A window that crosses midnight puts that
        // arrival on the *next* bar date, so decisions reaching back a
        // whole grace period are scanned, not just this date's.
        for d in
            scheduler.decisions_between(day_start - cfg.schedule.signal_grace_secs, day_end - 1)
        {
            if let Some(arrival) = signal_generated_at(replay_dir, &d.key) {
                if arrival > d.decision_at
                    && arrival <= d.window_end
                    && (day_start..day_end).contains(&arrival)
                {
                    ticks.push(arrival);
                }
            }
        }
        // Flattens scheduled inside this day may belong to an earlier decision.
        let mut probe = day_start;
        while let Some(d) = scheduler.current(probe) {
            if let Some(f) = d.flatten_at {
                if (day_start..day_end).contains(&f) {
                    ticks.push(f);
                }
            }
            match scheduler.next_after(probe) {
                Some(n) if n.decision_at < day_end => probe = n.decision_at,
                _ => break,
            }
        }
        // Final engine tick at the last second of the bar date (the daily
        // mark is written right after it, see below); a decision at the
        // next midnight is ticked in the next iteration with that date's
        // own closes loaded.
        ticks.push(day_end - 1);
        ticks.sort_unstable();
        ticks.dedup();
        for t in ticks {
            engine.tick(t).await?;
            summary.ticks += 1;
        }
        // The mark belongs to the last instant of the bar date, after every
        // decision/flatten of the date, so funding intervals line up with
        // the dates whose rates they use.
        engine.daily_mark_now(day_end - 1).await;
        summary.days += 1;
    }
    let prices = exec.prices(&cfg.universe.symbols).await;
    summary.final_equity = cfg.risk.equity_reference_usd + engine.state.cum_realized_usd
        - engine.state.cum_fees_usd
        + engine.state.cum_funding_est_usd
        + engine.state.unrealized_usd(&prices);
    summary.cum_realized_usd = engine.state.cum_realized_usd;
    summary.cum_fees_usd = engine.state.cum_fees_usd;
    summary.cum_funding_est_usd = engine.state.cum_funding_est_usd;
    summary.trades_closed = engine.state.trades_closed;
    Ok(summary)
}

/// `generated_at` of the signal file for `key`, as unix seconds. `None`
/// when there is no file, or it cannot be read or parsed: this only picks
/// tick times, every check stays with the engine.
fn signal_generated_at(replay_dir: &Path, key: &str) -> Option<i64> {
    let text =
        std::fs::read_to_string(replay_dir.join("signals").join(format!("{key}.json"))).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    let raw = v.get("generated_at")?.as_str()?;
    // RFC 3339, matching how `SignalFile::generated_at: DateTime<Utc>`
    // itself deserializes: a strict `%Y-%m-%dT%H:%M:%SZ`-only parser
    // would reject a value with fractional seconds or a numeric `+00:00`
    // offset that normal signal validation accepts, hiding it from the
    // arrival-tick schedule and letting its grace window close (recorded
    // as skipped) even though the engine would have accepted it.
    Some(
        DateTime::parse_from_rfc3339(raw)
            .ok()?
            .with_timezone(&Utc)
            .timestamp(),
    )
}

/// Convenience for callers that hold a config path.
pub async fn run_from_paths(
    config: &Path,
    replay_dir: &Path,
    out_dir: &Path,
) -> Result<ReplaySummary> {
    let cfg = BookConfig::load(config)?;
    run(cfg, replay_dir, out_dir).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::config::test_config_yaml;
    use crate::book::config::ScheduleKind;
    use crate::book::signal::testutil::signal_json;
    use chrono::{DateTime, Duration};

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// 12 days of bars for BTC/ETH/SOL/DOT starting on the anchor
    /// (2026-07-03, decision days 07-03 / 07-08). Prices drift so the
    /// long BTC / short DOT book gains and the flipped book loses.
    fn write_bars(dir: &Path, crash_on: Option<NaiveDate>) {
        let mut lines = Vec::new();
        let start = NaiveDate::from_ymd_opt(2026, 7, 3).unwrap();
        for i in 0..12 {
            let d = start + Duration::days(i);
            let f = i as f64;
            let mut btc = 100_000.0 + 500.0 * f;
            let eth = 4_000.0 - 10.0 * f;
            let sol = 200.0 + 1.0 * f;
            let dot = 4.0 - 0.02 * f;
            if Some(d) == crash_on {
                btc *= 0.5; // -50% on the long leg: session halt
            }
            for (sym, px) in [("BTC", btc), ("ETH", eth), ("SOL", sol), ("DOT", dot)] {
                lines.push(
                    serde_json::json!({"date": d.format("%Y-%m-%d").to_string(), "symbol": sym, "close": px, "funding_rate_hourly": 0.00001})
                        .to_string(),
                );
            }
        }
        std::fs::write(dir.join("bars.jsonl"), lines.join("\n") + "\n").unwrap();
        std::fs::write(
            dir.join("lots.json"),
            r#"{"BTC":{"size_decimals":5,"min_order_qty":null},"ETH":{"size_decimals":4,"min_order_qty":null},"SOL":{"size_decimals":2,"min_order_qty":null},"DOT":{"size_decimals":1,"min_order_qty":null}}"#,
        )
        .unwrap();
    }

    fn write_signal(dir: &Path, key: &str, weights: &[(&str, f64)]) {
        let decision_at = ts(&format!("{key}T00:30:00Z"));
        let body = signal_json(
            "test_producer",
            decision_at - Duration::minutes(10),
            decision_at - Duration::minutes(30),
            key,
            weights,
        );
        std::fs::create_dir_all(dir.join("signals")).unwrap();
        std::fs::write(dir.join("signals").join(format!("{key}.json")), body).unwrap();
    }

    fn cfg() -> BookConfig {
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.sizing.max_symbol_weight = 0.5;
        c
    }

    fn read_rows(path: &Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn replay_applies_flips_marks_and_is_byte_reproducible() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        write_signal(dir.path(), "2026-07-03", &[("BTC", 0.5), ("DOT", -0.5)]);
        write_signal(dir.path(), "2026-07-08", &[("BTC", -0.5), ("DOT", 0.5)]);
        let out1 = dir.path().join("out1");
        let s1 = run(cfg(), dir.path(), &out1).await.unwrap();
        assert_eq!(s1.days, 12);

        let ledger = read_rows(&out1.join("ledger.jsonl"));
        let decisions: Vec<_> = ledger.iter().filter(|r| r["event"] == "decision").collect();
        // 07-03 applied, 07-08 applied, 07-13 skipped (no file in the window).
        assert_eq!(decisions.len(), 3, "{decisions:?}");
        assert_eq!(decisions[0]["decision_key"], "2026-07-03");
        assert_eq!(decisions[0]["outcome"], "applied");
        assert_eq!(decisions[0]["intents"], 2);
        assert_eq!(decisions[1]["decision_key"], "2026-07-08");
        assert_eq!(decisions[1]["outcome"], "applied");
        // The flip is close+open per leg: 4 intents.
        assert_eq!(decisions[1]["intents"], 4);
        assert_eq!(decisions[2]["decision_key"], "2026-07-13");
        assert_eq!(decisions[2]["outcome"], "skipped");
        assert_eq!(decisions[2]["reason"], "window_missed");
        let fills: Vec<_> = ledger.iter().filter(|r| r["event"] == "fill").collect();
        assert_eq!(fills.len(), 6);
        assert!(fills.iter().all(|f| f["result"] == "filled"));

        // First book: long 0.005 BTC @ 100k (+5bps), short 125 DOT.
        let first_btc = fills
            .iter()
            .find(|f| f["intent"]["symbol"] == "BTC")
            .unwrap();
        assert_eq!(first_btc["intent"]["qty"], 0.005);
        assert!((first_btc["fill"]["fill_price"].as_f64().unwrap() - 100_050.0).abs() < 1e-6);

        let pnl = read_rows(&out1.join("pnl.jsonl"));
        let marks: Vec<_> = pnl.iter().filter(|r| r["event"] == "mark").collect();
        // Exactly one mark per bar date, written at 23:59:59 after every
        // decision of the date (never at the decision tick).
        assert_eq!(marks.len(), 12);
        assert_eq!(marks[0]["date"], "2026-07-03");
        assert_eq!(
            marks[0]["ts_ms"],
            ts("2026-07-03T23:59:59Z").timestamp_millis()
        );
        assert_eq!(marks[0]["n_positions"], 2);
        assert_eq!(marks[1]["date"], "2026-07-04");
        assert_eq!(marks[11]["date"], "2026-07-14");
        assert!(marks
            .iter()
            .all(|m| m["ts_ms"].as_i64().unwrap() % 86_400_000 == 86_399_000));
        // Funding estimate accrues on both legs (rate 1e-5/h, ~24h): the long
        // pays, the short receives; net is non-zero because notionals differ
        // after rounding.
        assert!(marks[2]["cum_funding_est_usd"].as_f64().unwrap() != 0.0);
        let exits: Vec<_> = pnl.iter().filter(|r| r["event"] == "exit").collect();
        assert_eq!(exits.len(), 2);
        // Long BTC 100050 → sold at 102500*(1-5bps) on 07-08: positive.
        let btc_exit = exits.iter().find(|e| e["symbol"] == "BTC").unwrap();
        assert!(btc_exit["realized_usd"].as_f64().unwrap() > 0.0);
        assert_eq!(s1.trades_closed, 2);

        let state: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out1.join("state.json")).unwrap())
                .unwrap();
        assert_eq!(state["last_decision"]["key"], "2026-07-13");
        assert_eq!(state["last_decision"]["outcome"], "skipped");
        assert!(state["positions"]["BTC"]["qty"].as_f64().unwrap() < 0.0);
        assert!(state["positions"]["DOT"]["qty"].as_f64().unwrap() > 0.0);
        let status: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out1.join("status.json")).unwrap())
                .unwrap();
        assert_eq!(status["id"], "test-book");
        assert_eq!(status["dry_run"], true);
        assert_eq!(status["position_count"], 2);
        assert_eq!(status["book"]["signal_status"], "skipped:window_missed");
        assert_eq!(status["book"]["next_decision_key"], "2026-07-18");

        // Byte-exact reproducibility, both into a fresh directory and when
        // rerunning into the same one (previous outputs are removed first).
        let out2 = dir.path().join("out2");
        run(cfg(), dir.path(), &out2).await.unwrap();
        let files = ["ledger.jsonl", "pnl.jsonl", "state.json"];
        let first: Vec<Vec<u8>> = files
            .iter()
            .map(|f| std::fs::read(out1.join(f)).unwrap())
            .collect();
        for (i, f) in files.iter().enumerate() {
            assert_eq!(
                first[i],
                std::fs::read(out2.join(f)).unwrap(),
                "{f} differs between runs"
            );
        }
        run(cfg(), dir.path(), &out1).await.unwrap();
        for (i, f) in files.iter().enumerate() {
            assert_eq!(
                first[i],
                std::fs::read(out1.join(f)).unwrap(),
                "{f} differs on rerun into the same dir"
            );
        }
    }

    #[tokio::test]
    async fn a_bar_date_gap_fails_the_replay() {
        for missing in ["2026-07-08", "2026-07-09"] {
            // 07-08 is an interval-schedule decision day; 07-09 is not, but
            // the book is open across it and its mark/funding would be lost.
            let dir = tempfile::tempdir().unwrap();
            write_bars(dir.path(), None);
            let bars = std::fs::read_to_string(dir.path().join("bars.jsonl")).unwrap();
            let kept: Vec<&str> = bars.lines().filter(|l| !l.contains(missing)).collect();
            std::fs::write(dir.path().join("bars.jsonl"), kept.join("\n") + "\n").unwrap();
            write_signal(dir.path(), "2026-07-03", &[("BTC", 0.5), ("DOT", -0.5)]);
            let err = run(cfg(), dir.path(), &dir.path().join("out"))
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains(missing), "{err}");
        }
    }

    #[tokio::test]
    async fn an_out_of_range_lot_precision_fails_the_replay() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        write_signal(dir.path(), "2026-07-03", &[("BTC", 0.5), ("DOT", -0.5)]);
        // u32::MAX would cast to -1 and round every quantity to tens.
        std::fs::write(
            dir.path().join("lots.json"),
            r#"{"BTC":{"size_decimals":4294967295,"min_order_qty":null}}"#,
        )
        .unwrap();
        let e = run(cfg(), dir.path(), &dir.path().join("out"))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("size_decimals"), "{e}");
        std::fs::write(
            dir.path().join("lots.json"),
            r#"{"BTC":{"size_decimals":4,"min_order_qty":-1.0}}"#,
        )
        .unwrap();
        assert!(run(cfg(), dir.path(), &dir.path().join("out2"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn duplicate_bar_rows_fail_the_replay() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        let bars = std::fs::read_to_string(dir.path().join("bars.jsonl")).unwrap();
        let dup = bars
            .lines()
            .find(|l| l.contains("2026-07-05") && l.contains("\"BTC\""))
            .unwrap()
            .to_string();
        std::fs::write(dir.path().join("bars.jsonl"), format!("{bars}{dup}\n")).unwrap();
        let err = run(cfg(), dir.path(), &dir.path().join("out"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("duplicate row"), "{err}");
        assert!(err.contains("BTC"), "{err}");
    }

    #[tokio::test]
    async fn a_held_leg_missing_from_a_later_bar_date_fails_the_replay() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        // Hold BTC/DOT from 07-03, then drop DOT's 07-06 row: that leg
        // would otherwise be marked at its entry basis for the day.
        let bars = std::fs::read_to_string(dir.path().join("bars.jsonl")).unwrap();
        let kept: Vec<&str> = bars
            .lines()
            .filter(|l| !(l.contains("2026-07-06") && l.contains("DOT")))
            .collect();
        std::fs::write(dir.path().join("bars.jsonl"), kept.join("\n") + "\n").unwrap();
        write_signal(dir.path(), "2026-07-03", &[("BTC", 0.5), ("DOT", -0.5)]);
        let err = run(cfg(), dir.path(), &dir.path().join("out"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("DOT"), "{err}");
        assert!(err.contains("2026-07-06"), "{err}");
    }

    #[tokio::test]
    async fn a_symbol_missing_from_a_bar_date_rejects_the_decision_instead_of_using_a_stale_price()
    {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        // Drop DOT from 07-08 only. The book holds BTC/SOL at that point,
        // so the held-leg guard does not fire; the 07-08 decision, which
        // wants DOT, must be rejected for want of a price rather than
        // sized off the previous day's close.
        let bars = std::fs::read_to_string(dir.path().join("bars.jsonl")).unwrap();
        let kept: Vec<&str> = bars
            .lines()
            .filter(|l| !(l.contains("2026-07-08") && l.contains("DOT")))
            .collect();
        std::fs::write(dir.path().join("bars.jsonl"), kept.join("\n") + "\n").unwrap();
        write_signal(dir.path(), "2026-07-03", &[("BTC", 0.5), ("SOL", -0.5)]);
        write_signal(dir.path(), "2026-07-08", &[("BTC", 0.5), ("DOT", -0.5)]);
        let out = dir.path().join("out");
        run(cfg(), dir.path(), &out).await.unwrap();
        let ledger = read_rows(&out.join("ledger.jsonl"));
        let d0708 = ledger
            .iter()
            .find(|r| r["event"] == "decision" && r["decision_key"] == "2026-07-08")
            .unwrap();
        assert_eq!(d0708["outcome"], "rejected");
        assert_eq!(d0708["reason"], "missing_price");
    }

    #[tokio::test]
    async fn missing_and_stale_signals_skip_without_touching_the_book() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        write_signal(dir.path(), "2026-07-03", &[("BTC", 0.5), ("DOT", -0.5)]);
        // 07-08: a file for the wrong key → rejected, then the window closes → skipped.
        let decision_at = ts("2026-07-08T00:30:00Z");
        let body = signal_json(
            "test_producer",
            decision_at - Duration::minutes(10),
            decision_at - Duration::minutes(30),
            "2026-07-07",
            &[("ETH", 0.5), ("SOL", -0.5)],
        );
        std::fs::write(dir.path().join("signals").join("2026-07-08.json"), body).unwrap();
        let out = dir.path().join("out");
        run(cfg(), dir.path(), &out).await.unwrap();
        let ledger = read_rows(&out.join("ledger.jsonl"));
        let decisions: Vec<_> = ledger.iter().filter(|r| r["event"] == "decision").collect();
        let outcomes: Vec<_> = decisions
            .iter()
            .map(|d| d["outcome"].as_str().unwrap())
            .collect();
        assert_eq!(outcomes, vec!["applied", "rejected", "skipped", "skipped"]);
        assert_eq!(decisions[1]["reason"], "decision_key_mismatch");
        assert_eq!(decisions[2]["reason"], "window_closed_after_reject");
        let state: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("state.json")).unwrap())
                .unwrap();
        // The 07-03 book is still on.
        assert!(state["positions"]["BTC"]["qty"].as_f64().unwrap() > 0.0);
        assert_eq!(state["last_decision"]["outcome"], "skipped");
    }

    #[tokio::test]
    async fn a_signal_published_late_in_the_window_is_still_applied() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        write_signal(dir.path(), "2026-07-03", &[("BTC", 0.5), ("DOT", -0.5)]);
        // 07-08 is published 30 minutes after the decision instant: the
        // fixture is gated by its own `generated_at`, so the decision tick
        // never even sees it (no early reject), and it is inside the
        // 3600 s grace window, so the added arrival tick applies it -- the
        // same as live would once the fetch delivers the file.
        let decision_at = ts("2026-07-08T00:30:00Z");
        let arrival = decision_at + Duration::minutes(30);
        let body = signal_json(
            "test_producer",
            arrival,
            decision_at - Duration::minutes(30),
            "2026-07-08",
            &[("BTC", -0.5), ("DOT", 0.5)],
        );
        std::fs::write(dir.path().join("signals").join("2026-07-08.json"), body).unwrap();
        let out = dir.path().join("out");
        run(cfg(), dir.path(), &out).await.unwrap();
        let ledger = read_rows(&out.join("ledger.jsonl"));
        let rows: Vec<_> = ledger
            .iter()
            .filter(|r| r["event"] == "decision" && r["decision_key"] == "2026-07-08")
            .collect();
        let outcomes: Vec<_> = rows
            .iter()
            .map(|r| r["outcome"].as_str().unwrap())
            .collect();
        assert_eq!(outcomes, vec!["applied"]);
        // Applied at its stated arrival, never before it.
        assert_eq!(rows[0]["ts_ms"], arrival.timestamp_millis());
        let state: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("state.json")).unwrap())
                .unwrap();
        assert!(state["positions"]["BTC"]["qty"].as_f64().unwrap() < 0.0);
    }

    #[tokio::test]
    async fn a_late_arrival_after_midnight_is_ticked_on_the_next_bar_date() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        // Decide at 23:30 with an hour of grace: the window runs into the
        // next bar date, and so may the producer's publish.
        let mut c = cfg();
        c.schedule.kind = ScheduleKind::Daily;
        c.schedule.decision_time_utc = Some("23:30".into());
        let decision_at = ts("2026-07-05T23:30:00Z");
        let arrival = ts("2026-07-06T00:10:00Z");
        let body = signal_json(
            "test_producer",
            arrival,
            decision_at - Duration::minutes(30),
            "2026-07-05",
            &[("BTC", 0.5), ("DOT", -0.5)],
        );
        std::fs::create_dir_all(dir.path().join("signals")).unwrap();
        std::fs::write(dir.path().join("signals").join("2026-07-05.json"), body).unwrap();
        let out = dir.path().join("out");
        run(c, dir.path(), &out).await.unwrap();
        let ledger = read_rows(&out.join("ledger.jsonl"));
        let rows: Vec<_> = ledger
            .iter()
            .filter(|r| r["event"] == "decision" && r["decision_key"] == "2026-07-05")
            .collect();
        let outcomes: Vec<_> = rows
            .iter()
            .map(|r| r["outcome"].as_str().unwrap())
            .collect();
        // Not yet arrived at the decision or at the end of that bar date,
        // so neither tick even sees the file (no early reject); applied on
        // the next date at the stated arrival, which is what live would do.
        assert_eq!(outcomes, vec!["applied"]);
        assert_eq!(rows[0]["ts_ms"], arrival.timestamp_millis());
    }

    #[tokio::test]
    async fn an_arrival_with_fractional_seconds_or_a_numeric_offset_is_still_ticked() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        let mut c = cfg();
        c.schedule.kind = ScheduleKind::Daily;
        c.schedule.decision_time_utc = Some("23:30".into());
        let decision_at = ts("2026-07-05T23:30:00Z");
        let arrival = ts("2026-07-06T00:10:00Z");
        let body = signal_json(
            "test_producer",
            arrival,
            decision_at - Duration::minutes(30),
            "2026-07-05",
            &[("BTC", 0.5), ("DOT", -0.5)],
        );
        // generated_at is not part of the hashed payload, so rewriting
        // just this field to a form SignalFile's DateTime<Utc> field
        // still deserializes (fractional seconds, numeric offset) --
        // which the engine's own signal validation accepts -- but the
        // replay's own arrival-tick parser previously rejected with its
        // stricter, hand-rolled format string.
        let mut v: serde_json::Value = serde_json::from_str(&body).unwrap();
        v["generated_at"] = serde_json::Value::String("2026-07-06T00:10:00.250+00:00".into());
        std::fs::create_dir_all(dir.path().join("signals")).unwrap();
        std::fs::write(
            dir.path().join("signals").join("2026-07-05.json"),
            v.to_string(),
        )
        .unwrap();
        let out = dir.path().join("out");
        run(c, dir.path(), &out).await.unwrap();
        let ledger = read_rows(&out.join("ledger.jsonl"));
        let rows: Vec<_> = ledger
            .iter()
            .filter(|r| r["event"] == "decision" && r["decision_key"] == "2026-07-05")
            .collect();
        let outcomes: Vec<_> = rows
            .iter()
            .map(|r| r["outcome"].as_str().unwrap())
            .collect();
        assert_eq!(
            outcomes,
            vec!["applied"],
            "an unparseable arrival timestamp would leave this window never ticked before it \
             closes, recording it skipped instead"
        );
        assert_eq!(rows[0]["ts_ms"], arrival.timestamp_millis());
    }

    #[tokio::test]
    async fn a_window_inherited_across_midnight_is_recorded_skipped_before_the_next_key_starts() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        // Same cross-midnight setup as the late-arrival case above, but no
        // signal ever arrives for 2026-07-05: its window (decision 23:30,
        // grace 3600s -> closes 00:30 on 07-06) must still be explicitly
        // recorded `skipped` on the 07-06 date, before 2026-07-06's own
        // decision_at (23:30 same day) would otherwise silently supersede
        // `last_decision` without ever recording the expiry.
        let mut c = cfg();
        c.schedule.kind = ScheduleKind::Daily;
        c.schedule.decision_time_utc = Some("23:30".into());
        let out = dir.path().join("out");
        run(c, dir.path(), &out).await.unwrap();
        let ledger = read_rows(&out.join("ledger.jsonl"));
        let rows: Vec<_> = ledger
            .iter()
            .filter(|r| r["event"] == "decision" && r["decision_key"] == "2026-07-05")
            .collect();
        let outcomes: Vec<_> = rows
            .iter()
            .map(|r| r["outcome"].as_str().unwrap())
            .collect();
        assert!(
            outcomes.contains(&"skipped"),
            "2026-07-05 must be explicitly recorded skipped, not silently dropped: {outcomes:?}"
        );
    }

    #[tokio::test]
    async fn session_drawdown_halts_flattens_and_blocks_the_next_decision() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(
            dir.path(),
            Some(NaiveDate::from_ymd_opt(2026, 7, 5).unwrap()),
        );
        write_signal(dir.path(), "2026-07-03", &[("BTC", 0.5), ("DOT", -0.5)]);
        write_signal(dir.path(), "2026-07-08", &[("BTC", 0.5), ("DOT", -0.5)]);
        let out = dir.path().join("out");
        run(cfg(), dir.path(), &out).await.unwrap();
        let ledger = read_rows(&out.join("ledger.jsonl"));
        let halts: Vec<_> = ledger.iter().filter(|r| r["event"] == "halt").collect();
        assert!(
            halts.iter().any(|h| h["risk"]["kind"] == "session_halt"),
            "{halts:?}"
        );
        let flatten: Vec<_> = ledger.iter().filter(|r| r["event"] == "flatten").collect();
        assert_eq!(flatten[0]["reason"], "session_halt");
        assert_eq!(flatten[0]["flat"], true);
        // 07-08: opens are blocked while halted → every intent blocked.
        let blocked: Vec<_> = ledger
            .iter()
            .filter(|r| r["event"] == "order_blocked")
            .collect();
        assert!(!blocked.is_empty());
        assert_eq!(blocked[0]["reason"], "session_halted");
        let decisions: Vec<_> = ledger.iter().filter(|r| r["event"] == "decision").collect();
        let d0708 = decisions
            .iter()
            .find(|d| d["decision_key"] == "2026-07-08")
            .unwrap();
        assert_eq!(d0708["outcome"], "partial");
        assert_eq!(d0708["blocked"], 2);
        let state: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("state.json")).unwrap())
                .unwrap();
        assert_eq!(state["session"]["halted"], true);
        assert!(state["positions"].as_object().unwrap().is_empty());
    }

    #[tokio::test]
    async fn fixed_window_flatten_closes_the_book_at_flatten_at() {
        let dir = tempfile::tempdir().unwrap();
        write_bars(dir.path(), None);
        let mut c = cfg();
        c.schedule.kind = crate::book::config::ScheduleKind::Daily;
        c.schedule.flatten_after_secs = Some(6 * 3600);
        c.signal.require_dollar_neutral = false;
        c.sizing.max_net_usd = 1_000.0;
        write_signal(dir.path(), "2026-07-04", &[("SOL", 0.5)]);
        let out = dir.path().join("out");
        run(c, dir.path(), &out).await.unwrap();
        let ledger = read_rows(&out.join("ledger.jsonl"));
        let flatten: Vec<_> = ledger.iter().filter(|r| r["event"] == "flatten").collect();
        assert_eq!(flatten.len(), 1);
        assert_eq!(flatten[0]["reason"], "fixed_window");
        assert_eq!(flatten[0]["decision_key"], "2026-07-04");
        assert_eq!(
            flatten[0]["ts_ms"],
            ts("2026-07-04T06:30:00Z").timestamp_millis()
        );
        let state: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("state.json")).unwrap())
                .unwrap();
        assert!(state["positions"].as_object().unwrap().is_empty());
        // The ~6 h of funding on the closed leg is booked at the close.
        assert!(state["cum_funding_est_usd"].as_f64().unwrap() < 0.0);
        // Days without a file are skipped, not errors.
        let decisions: Vec<_> = ledger.iter().filter(|r| r["event"] == "decision").collect();
        assert!(decisions.iter().any(|d| d["outcome"] == "skipped"));
    }
}
