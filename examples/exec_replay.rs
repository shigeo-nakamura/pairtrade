//! bot-strategy#1099 Step 2d study tool: replay book-runtime order intents
//! against a recorded Lighter tape, `MakerFirst` vs a taker IOC, on the
//! deterministic paper venue (G1 §3 rules). Read-only; never touches a venue.
//!
//! ```text
//! cargo run --example exec_replay -- \
//!   --tape-dir ~/bot/logs/studies/2026-10-01-xsmom-exec-tape-1099/tape \
//!   --universe ~/bot/logs/studies/2026-10-01-xsmom-exec-tape-1099/universe.json \
//!   --intents intents.jsonl   # book ledger rows with "event":"order_intent"
//!   [--tier standard|premium] [--maker-window-ms 60000] [--requote-bps 5]
//!   [--max-slip-bps 25] [--out results.jsonl]
//! ```
//!
//! Prints one JSON row per intent and a summary. Intents whose decision time
//! is outside the tape (or whose symbol is not collected) are skipped and
//! counted.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use debot::trade::execution::intent::{ExecIntent, ExecStyle, MakerFirstParams, Role, Side};
use debot::trade::execution::maker_first::MakerFirstTiming;
use debot::trade::execution::paper_venue::PaperParams;
use debot::trade::execution::replay::{
    cost_bps, parse_market, replay_maker_first, replay_taker, universe_ids, TimedEv,
};
use serde_json::{json, Value};

struct Args {
    tape_dir: PathBuf,
    universe: PathBuf,
    intents: PathBuf,
    tier: String,
    maker_window_ms: u64,
    requote_bps: f64,
    max_slip_bps: f64,
    out: Option<PathBuf>,
}

fn args() -> Result<Args> {
    let mut a = std::env::args().skip(1);
    let mut m: HashMap<String, String> = HashMap::new();
    while let Some(k) = a.next() {
        let v = a.next().with_context(|| format!("{k} needs a value"))?;
        m.insert(k.trim_start_matches("--").to_string(), v);
    }
    let req = |k: &str| {
        m.get(k)
            .cloned()
            .with_context(|| format!("--{k} is required"))
    };
    Ok(Args {
        tape_dir: req("tape-dir")?.into(),
        universe: req("universe")?.into(),
        intents: req("intents")?.into(),
        tier: m.get("tier").cloned().unwrap_or_else(|| "standard".into()),
        maker_window_ms: m.get("maker-window-ms").map_or(Ok(60_000), |v| v.parse())?,
        requote_bps: m.get("requote-bps").map_or(Ok(5.0), |v| v.parse())?,
        max_slip_bps: m.get("max-slip-bps").map_or(Ok(25.0), |v| v.parse())?,
        out: m.get("out").map(PathBuf::from),
    })
}

/// Lines of a plain or gzipped tape file.
fn read_lines(path: &Path) -> Result<Vec<String>> {
    if path.extension().is_some_and(|e| e == "gz") {
        let out = Command::new("gzip")
            .arg("-dc")
            .arg(path)
            .stdout(Stdio::piped())
            .output()
            .with_context(|| format!("gzip -dc {}", path.display()))?;
        // A truncated or corrupt file must fail, not replay its prefix as if
        // it were complete (Codex on #377).
        if !out.status.success() {
            bail!("gzip -dc {} failed: {}", path.display(), out.status);
        }
        Ok(String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::to_string)
            .collect())
    } else {
        Ok(BufReader::new(std::fs::File::open(path)?)
            .lines()
            .collect::<std::io::Result<_>>()?)
    }
}

/// Tape lines for the wanted markets (plus connection rows).
fn load_tape(dir: &Path, markets: &HashSet<i64>) -> Result<HashMap<i64, Vec<String>>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("tape_"))
        })
        .collect();
    files.sort();
    let mut per: HashMap<i64, Vec<String>> = markets.iter().map(|m| (*m, Vec::new())).collect();
    for f in files {
        let mut file_lines = read_lines(&f)?;
        let plain = !f.extension().is_some_and(|e| e == "gz");
        // The current hour's plain file may end in a row still being
        // written: only that one trailing line may be incomplete.
        if plain
            && file_lines
                .last()
                .is_some_and(|l| serde_json::from_str::<Value>(l).is_err())
        {
            file_lines.pop();
        }
        for (n, line) in file_lines.into_iter().enumerate() {
            let r = serde_json::from_str::<Value>(&line)
                .with_context(|| format!("malformed tape row {}:{}", f.display(), n + 1))?;
            if r.get("k").and_then(Value::as_str) == Some("c") {
                for v in per.values_mut() {
                    v.push(line.clone());
                }
                continue;
            }
            let Some(m) = r.get("m").and_then(Value::as_i64) else {
                continue;
            };
            if let Some(v) = per.get_mut(&m) {
                v.push(line);
            }
        }
    }
    Ok(per)
}

fn main() -> Result<()> {
    let a = args()?;
    let params = match a.tier.as_str() {
        "standard" => PaperParams::lighter_standard(),
        "premium" => PaperParams::lighter_premium(),
        t => bail!("unknown tier {t}"),
    };
    let ids = universe_ids(&serde_json::from_str(&std::fs::read_to_string(
        &a.universe,
    )?)?);
    let intents: Vec<Value> = BufReader::new(std::fs::File::open(&a.intents)?)
        .lines()
        .map_while(Result::ok)
        .filter_map(|l| serde_json::from_str::<Value>(&l).ok())
        .filter(|r| r.get("event").and_then(Value::as_str) == Some("order_intent"))
        .collect();
    let wanted: HashSet<i64> = intents
        .iter()
        .filter_map(|r| {
            r["intent"]["symbol"]
                .as_str()
                .and_then(|s| ids.get(s))
                .copied()
        })
        .collect();
    let tape = load_tape(&a.tape_dir, &wanted)?;
    let events: HashMap<i64, Vec<TimedEv>> = tape
        .into_iter()
        .map(|(m, lines)| Ok((m, parse_market(lines.into_iter(), m)?)))
        .collect::<Result<_>>()?;
    let timing = MakerFirstTiming {
        poll: Duration::from_millis(500),
        cancel_confirm_polls: 20,
        settle_polls: 20,
        ioc_fill_polls: 20,
    };
    let mut out: Box<dyn Write> = match &a.out {
        Some(p) => Box::new(std::fs::File::create(p)?),
        None => Box::new(std::io::stdout()),
    };
    let (mut done, mut skipped) = (0usize, 0usize);
    let (mut t_sum, mut m_sum, mut n_both, mut incomplete) = (0.0, 0.0, 0usize, 0usize);
    for r in &intents {
        let i = &r["intent"];
        let (Some(sym), Some(qty), Some(refp), Some(ts)) = (
            i["symbol"].as_str(),
            i["qty"].as_f64(),
            i["reference_price"].as_f64(),
            r["ts_ms"].as_u64(),
        ) else {
            skipped += 1;
            continue;
        };
        let side = match i["side"].as_str() {
            Some("buy") => Side::Buy,
            Some("sell") => Side::Sell,
            _ => {
                skipped += 1;
                continue;
            }
        };
        let Some(ev) = ids.get(sym).and_then(|m| events.get(m)) else {
            skipped += 1;
            continue;
        };
        // The tape must cover the decision and the whole execution horizon
        // (maker window + cancel/settle/IOC), not just the maker window,
        // or the tail would run against a frozen book (Codex on #377).
        let horizon = a.maker_window_ms + 120_000;
        let covered =
            ev.first().is_some_and(|e| e.ms < ts) && ev.last().is_some_and(|e| e.ms > ts + horizon);
        if !covered {
            skipped += 1;
            continue;
        }
        let intent = ExecIntent {
            symbol: sym.to_string(),
            side,
            qty,
            reference_price: refp,
            reduce_only: i["reduce_only"].as_bool().unwrap_or(false),
            deadline_ms: None,
            max_slip_bps: Some(a.max_slip_bps),
            style: ExecStyle::MakerFirst(MakerFirstParams {
                maker_window_ms: a.maker_window_ms,
                requote_bps: a.requote_bps,
            }),
        };
        let taker = replay_taker(ev, &intent, ts, params);
        let t_rows: Vec<(f64, f64, Option<f64>)> =
            taker.iter().map(|f| (f.price, f.qty, f.fee_usd)).collect();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()?;
        let mk = rt.block_on(replay_maker_first(ev, &intent, ts, params, timing, horizon))?;
        let m_rows: Vec<(f64, f64, Option<f64>)> = mk
            .fills
            .iter()
            .map(|f| (f.price, f.qty, f.fee_usd))
            .collect();
        let maker_qty: f64 = mk
            .fills
            .iter()
            .filter(|f| f.role == Role::Maker)
            .map(|f| f.qty)
            .sum();
        let t_cost = cost_bps(side, refp, &t_rows);
        let m_cost = cost_bps(side, refp, &m_rows);
        // Compare only like with like: both executions completed the full
        // quantity and nothing is unresolved. A partial fill's cost covers a
        // different (often the cheapest) part of the order (Codex on #377).
        let eps = qty * 1e-9 + 1e-12;
        let t_filled: f64 = t_rows.iter().map(|r| r.1).sum();
        let comparable = (t_filled - qty).abs() <= eps
            && (mk.filled_qty - qty).abs() <= eps
            && mk.unresolved.is_empty();
        match (comparable, t_cost, m_cost) {
            (true, Some(t), Some(m)) => {
                t_sum += t;
                m_sum += m;
                n_both += 1;
            }
            _ => incomplete += 1,
        }
        writeln!(
            out,
            "{}",
            json!({
                "ts_ms": ts, "symbol": sym, "side": i["side"], "qty": qty,
                "comparable": comparable,
                "reference_price": refp,
                "taker": {"filled": t_rows.iter().map(|r| r.1).sum::<f64>(), "cost_bps": t_cost},
                "maker_first": {
                    "filled": mk.filled_qty, "maker_qty": maker_qty,
                    "taker_qty": mk.filled_qty - maker_qty, "cost_bps": m_cost,
                    "unresolved": mk.unresolved,
                },
            })
        )?;
        done += 1;
    }
    eprintln!(
        "replayed {done}, skipped {skipped}; comparable (both complete, none unresolved) {n_both}, incomplete {incomplete}: mean taker {:.2} bp, mean maker-first {:.2} bp",
        if n_both > 0 { t_sum / n_both as f64 } else { f64::NAN },
        if n_both > 0 { m_sum / n_both as f64 } else { f64::NAN },
    );
    Ok(())
}
