//! Book runtime binary (bot-strategy#937): slow cross-sectional book on one
//! venue, driven by an external signal file. See `docs/book-runtime.md`.
//!
//! ```text
//! book-runtime --config configs/book/xsmom-695.yaml            # live loop (DRY_RUN per config)
//! book-runtime --config ... --replay <dir> --out <dir>         # deterministic replay
//! book-runtime --config ... --validate                          # parse + fingerprint (+ calendar)
//! book-runtime --config ... --validate --calendar <json>        # validate a not-yet-installed calendar
//! book-runtime --config ... --print-fetch-env <path>            # emit <instance>.fetch.env from the parsed config
//! ```
//!
//! Live orders are refused unless `dry_run: false` in the config AND
//! `BOOK_CONFIRM_LIVE=yes-i-mean-it` is set (two-variable rule, same as
//! `engine_b_live`). Live execution is Lighter-only until dex-connector
//! grows a perp IOC path for Hyperliquid (its `create_order_taker_ioc` is
//! spot-only as of v4.7.20).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use debot::book::config::{BookConfig, ScheduleKind};
use debot::book::engine::{BookEngine, FileSignalSource};
use debot::book::executor::{Executor, LiveExecutor, PaperExecutor, VenuePosition};
use debot::book::rebalance::LotMeta;
use debot::book::replay;
use debot::book::schedule::Scheduler;
use debot::book::status::StatusWriter;
use debot::infra::logger::init_logger;
use debot::infra::prom;
use debot::infra::s3_mirror::S3Mirror;
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use dex_connector::{DexConnector, PriceUpdate};
use fs2::FileExt;
use rust_decimal::prelude::ToPrimitive;

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

struct Args {
    config: PathBuf,
    replay: Option<PathBuf>,
    out: Option<PathBuf>,
    validate: bool,
    /// Write the signal fetcher's environment file from the parsed
    /// config and exit (bot-strategy#948). Deliberately does NOT build
    /// the scheduler, so `install_book_runtime.sh` can run it before the
    /// calendar has been staged.
    print_fetch_env: Option<PathBuf>,
    /// `--validate` only: the calendar to check instead of
    /// `schedule.calendar_path` (bot-strategy#952).
    calendar: Option<PathBuf>,
}

fn parse_args() -> Result<Args> {
    let mut config = std::env::var("BOOK_CONFIG").ok().map(PathBuf::from);
    let mut replay = None;
    let mut out = None;
    let mut validate = false;
    let mut print_fetch_env = None;
    let mut calendar = None;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" => config = Some(PathBuf::from(it.next().context("--config needs a path")?)),
            "--replay" => replay = Some(PathBuf::from(it.next().context("--replay needs a dir")?)),
            "--out" => out = Some(PathBuf::from(it.next().context("--out needs a dir")?)),
            "--validate" => validate = true,
            "--print-fetch-env" => {
                print_fetch_env = Some(PathBuf::from(
                    it.next().context("--print-fetch-env needs a path")?,
                ))
            }
            "--calendar" => {
                calendar = Some(PathBuf::from(it.next().context("--calendar needs a path")?))
            }
            "-h" | "--help" => {
                eprintln!(
                    "usage: book-runtime --config <yaml> [--replay <dir> --out <dir>] [--validate [--calendar <json>]] [--print-fetch-env <path>]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }
    // The running service always loads `schedule.calendar_path`; an
    // override outside `--validate` would check one file and run another.
    if calendar.is_some() && !validate {
        bail!(
            "--calendar is only valid with --validate (the runtime loads schedule.calendar_path)"
        );
    }
    Ok(Args {
        config: config.context("--config <yaml> (or BOOK_CONFIG) is required")?,
        replay,
        out,
        validate,
        print_fetch_env,
        calendar,
    })
}

/// Characters a value may contain and still mean exactly the same thing
/// to **both** readers of this file. The units load it with systemd
/// `EnvironmentFile=`, and `book_signal_fetch.sh` reads the same names as
/// plain environment variables. The two parsers agree on no quoting
/// idiom -- bash's `'\''` splice, for one, is not what systemd's
/// environment-file parser decodes it to (pairtrade#293 Codex) -- so
/// rather than pick a scheme and hope, a value that would need quoting is
/// refused outright. Every field here is an identifier, a path, a date, a
/// time, a number or a comma-joined symbol list, so nothing legitimate is
/// excluded; anything else is a config mistake worth failing the install
/// for, before promotion, instead of a fetcher that silently rejects
/// every signal the runtime accepts.
fn env_safe(value: &str) -> bool {
    value.chars().all(|c| {
        if c.is_ascii() {
            // All the syntax both parsers know lives in ASCII, so this
            // half stays an allowlist. `=` is in it because both split
            // the assignment at the *first* `=` and keep the rest
            // verbatim. Everything left out is left out for a reason and
            // not merely unlisted: `$` backtick `\` `"` `'` are
            // expansion or quoting; ` ` and tab are word separators;
            // `~` is expanded by bash after an `=` and not by systemd;
            // `#` starts a comment for one reader and not the other; and
            // `* ? [ ] { } ( ) ; & | < > !` are shell syntax a future
            // reader of this file might well subject them to.
            c.is_ascii_alphanumeric() || "_-.:,/+@=".contains(c)
        } else {
            // Nothing outside ASCII is syntax to either parser, and the
            // runtime itself accepts such values, so refusing them would
            // block a config the service runs happily (pairtrade#293
            // Codex). Only separators and control characters could still
            // confuse a line-based reader.
            !c.is_control() && !c.is_whitespace()
        }
    })
}

/// One bare `KEY=value` line. Both parsers read it identically because
/// `env_safe` has ruled out everything they disagree about.
fn env_line(key: &str, value: &str) -> Result<String> {
    if !env_safe(value) {
        bail!(
            "{key}={value:?} contains characters systemd's EnvironmentFile= parser and a shell do \
             not read identically; the fetch environment only carries identifiers, paths, dates, \
             times, numbers and comma-joined symbol lists"
        );
    }
    Ok(format!("{key}={value}\n"))
}

/// The signal fetcher's environment, rendered from the *parsed* config
/// (bot-strategy#948, pairtrade#293). `install_book_runtime.sh` used to
/// scrape these ten values out of the YAML with `awk`, which is not a
/// YAML parser: quoted scalars kept their quotes, values with spaces were
/// truncated at the first token, trailing `# comments` leaked in, and
/// each fix uncovered the next case. The binary that will *run* the
/// config is the only thing that reads it correctly, so it renders the
/// file -- the same reasoning that moved calendar validation here in
/// bot-strategy#952.
///
/// `book_signal_fetch.sh` treats any unset value as "skip that check",
/// so an optional field simply comes through empty.
fn fetch_env(cfg: &BookConfig) -> Result<String> {
    let kind = match cfg.schedule.kind {
        ScheduleKind::IntervalDays => "interval_days",
        ScheduleKind::Daily => "daily",
        ScheduleKind::Calendar => "calendar",
    };
    // Only a date-keyed schedule lets the fetcher derive a decision time
    // from the file's own decision_key; a calendar schedule leaves this
    // empty and the fetcher skips that one check.
    let decision_time = match cfg.schedule.kind {
        ScheduleKind::IntervalDays | ScheduleKind::Daily => {
            cfg.schedule.decision_time_utc.as_deref().unwrap_or("")
        }
        ScheduleKind::Calendar => "",
    };
    let calendar_path = cfg
        .schedule
        .calendar_path
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let anchor_date = cfg
        .schedule
        .anchor_date
        .map(|d| d.to_string())
        .unwrap_or_default();
    let every_days = cfg
        .schedule
        .every_days
        .map(|n| n.to_string())
        .unwrap_or_default();
    let pairs: [(&str, &str); 11] = [
        ("BOOK_SCHEDULE_KIND", kind),
        ("BOOK_CALENDAR_PATH", &calendar_path),
        ("BOOK_ANCHOR_DATE", &anchor_date),
        ("BOOK_EVERY_DAYS", &every_days),
        ("BOOK_DECISION_TIME_UTC", decision_time),
        ("BOOK_SIGNAL_PRODUCER_ID", &cfg.signal.producer_id),
        (
            "BOOK_SIGNAL_MAX_AGE_SECS",
            &cfg.signal.max_age_secs.to_string(),
        ),
        (
            "BOOK_REQUIRE_DOLLAR_NEUTRAL",
            &cfg.signal.require_dollar_neutral.to_string(),
        ),
        ("BOOK_NET_TOLERANCE", &cfg.signal.net_tolerance.to_string()),
        (
            "BOOK_MAX_SYMBOL_WEIGHT",
            &cfg.sizing.max_symbol_weight.to_string(),
        ),
        ("BOOK_UNIVERSE", &cfg.universe.symbols.join(",")),
    ];
    let mut out = String::new();
    for (key, value) in pairs {
        out.push_str(&env_line(key, value)?);
    }
    Ok(out)
}

async fn fetch_lot(
    connector: &Arc<dyn DexConnector + Send + Sync>,
    symbol: &str,
) -> Option<LotMeta> {
    match connector.get_ticker(symbol, None).await {
        Ok(t) => t.size_decimals.map(|d| LotMeta {
            size_decimals: d,
            min_order_qty: t.min_order.and_then(|m| m.to_f64()).filter(|m| *m > 0.0),
        }),
        Err(e) => {
            log::warn!("[LOT] get_ticker {symbol} failed: {e:?}");
            None
        }
    }
}

/// Process-wide ownership of one instance's state: held for the lifetime
/// of `main` (dropping the file releases the OS-level `flock`, including
/// on a crash). Two `book-runtime` processes accidentally started against
/// the same config would otherwise both load the same state, reconcile
/// the same pre-trade venue snapshot, and submit the same decision
/// concurrently -- each recording it Applied, so the resulting doubled
/// exposure would only ever be adopted on a later tick, not prevented.
struct InstanceLock {
    _file: std::fs::File,
}

fn acquire_instance_lock(lock_path: &Path) -> Result<InstanceLock> {
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open instance lock {}", lock_path.display()))?;
    file.try_lock_exclusive().with_context(|| {
        format!(
            "{} is already locked -- another book-runtime process is running against this state; refusing to start a second instance",
            lock_path.display()
        )
    })?;
    Ok(InstanceLock { _file: file })
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let args = parse_args()?;
    let cfg = BookConfig::load(&args.config)?;
    log::info!("{}", cfg.log_line());
    if let Some(path) = &args.print_fetch_env {
        std::fs::write(path, fetch_env(&cfg)?)
            .with_context(|| format!("write fetch env {}", path.display()))?;
        return Ok(());
    }
    if args.validate {
        // Build the scheduler too, so a calendar-kind config is validated
        // by the same code the service starts with (bot-strategy#952):
        // `install_book_runtime.sh` runs this with the staged binary and
        // the staged calendar before promoting either, which is what
        // replaced the Python re-implementation of these rules.
        let sched = Scheduler::from_config_with_calendar(&cfg.schedule, args.calendar.as_deref())
            .context("validate schedule")?;
        println!(
            "ok fp={} calendar_entries={}",
            cfg.fingerprint(),
            sched.calendar().len()
        );
        return Ok(());
    }
    if let (Some(replay_dir), Some(out_dir)) = (&args.replay, &args.out) {
        let s = replay::run(cfg, replay_dir, out_dir).await?;
        log::info!(
            "[REPLAY] days={} ticks={} final_equity=${:.2} realized=${:.2} fees=${:.2} funding_est=${:.2} trades_closed={}",
            s.days, s.ticks, s.final_equity, s.cum_realized_usd, s.cum_fees_usd, s.cum_funding_est_usd, s.trades_closed
        );
        return Ok(());
    }
    if args.replay.is_some() || args.out.is_some() {
        bail!("--replay and --out must be given together");
    }

    if !cfg.dry_run {
        if std::env::var("BOOK_CONFIRM_LIVE").as_deref() != Ok("yes-i-mean-it") {
            bail!("dry_run: false requires BOOK_CONFIRM_LIVE=yes-i-mean-it as well (bot-strategy#937)");
        }
        if cfg.venue != "lighter" {
            bail!(
                "live execution is Lighter-only for now (dex-connector has no perp IOC path for {})",
                cfg.venue
            );
        }
    }

    // Held for the rest of `main`: refuses a second process against the
    // same state before it can ever load it or reach the venue.
    let _instance_lock = acquire_instance_lock(&cfg.instance_lock_path())?;

    prom::maybe_start_exporter();
    let process_started_at = now_secs();
    prom::record_process_info(&cfg.instance_id, process_started_at);

    // Subscribe the universe plus every symbol the persisted book still
    // holds (a leg removed from the universe must stay priceable so it can
    // be closed); legs adopted from the venue at runtime fall back to the
    // ticker price inside LiveExecutor. A symbol can also carry a
    // `pending_funding_qty_hours` balance with no open position (the leg
    // closed before its funding settled) -- without a price/rate feed for
    // it, that carry can never resolve, so it needs the same subscription.
    let persisted_state =
        debot::book::state::BookState::load_or_new(&cfg.paths.state, &cfg.instance_id)?;
    let mut symbols = cfg.universe.symbols.clone();
    for s in persisted_state
        .positions
        .keys()
        .chain(persisted_state.pending_funding_qty_hours.keys())
    {
        if !symbols.contains(s) {
            symbols.push(s.clone());
        }
    }
    let connector = DexConnectorBox::create(
        &cfg.venue,
        cfg.dry_run,
        &symbols,
        Some(cfg.instance_id.as_str()),
    )
    .await
    .map_err(|e| anyhow::anyhow!("init connector {}: {e:?}", cfg.venue))?;
    connector
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("start connector: {e:?}"))?;
    let connector: Arc<dyn DexConnector + Send + Sync> = Arc::new(connector);
    let mut price_rx = connector
        .subscribe_price_updates()
        .map_err(|e| anyhow::anyhow!("subscribe_price_updates: {e:?}"))?;

    let scheduler = Scheduler::from_config(&cfg.schedule)?;
    let status = StatusWriter::new(cfg.paths.status.clone(), S3Mirror::from_env());
    let signals = Box::new(FileSignalSource::new(cfg.signal.path.clone()));

    let paper: Option<Arc<PaperExecutor>>;
    let live: Option<Arc<LiveExecutor>>;
    let exec: Arc<dyn Executor> = if cfg.dry_run {
        let p = Arc::new(PaperExecutor::new(
            cfg.execution.paper_slippage_bps,
            cfg.execution.paper_fee_bps,
        ));
        paper = Some(p.clone());
        live = None;
        p
    } else {
        let l = Arc::new(LiveExecutor::new(
            connector.clone(),
            cfg.execution.fill_confirm_timeout_secs,
            cfg.execution.slippage_bps,
            cfg.execution.allow_venue_protection_fallback,
        ));
        paper = None;
        live = Some(l.clone());
        l
    };

    let mut engine = BookEngine::new(cfg.clone(), scheduler, exec.clone(), signals, status)?;
    if let Some(p) = &paper {
        // Restart: the paper book continues from state.json.
        let seed: BTreeMap<String, VenuePosition> = engine
            .state
            .positions
            .iter()
            .map(|(s, pos)| {
                (
                    s.clone(),
                    VenuePosition {
                        qty: pos.qty,
                        entry_price: Some(pos.avg_price),
                    },
                )
            })
            .collect();
        p.seed_positions(seed).await;
    }
    // Concurrent, not sequential: this runs before the select! loop even
    // starts, so a restarted process with persisted exposure and a slow or
    // timing-out venue would otherwise be unable to reconcile positions,
    // evaluate risk, process an overdue flatten, or handle SIGTERM for the
    // sum of every symbol's request latency. Same strategy as the retry
    // path below.
    let mut lot_fetches = tokio::task::JoinSet::new();
    for s in symbols.clone() {
        let connector = connector.clone();
        lot_fetches.spawn(async move {
            let lot = fetch_lot(&connector, &s).await;
            (s, lot)
        });
    }
    let mut missing_lots: Vec<String> = Vec::new();
    while let Some(res) = lot_fetches.join_next().await {
        match res {
            Ok((s, Some(lot))) => {
                engine.set_lot(&s, lot);
                if let Some(p) = &paper {
                    p.set_lot(&s, lot).await;
                }
            }
            Ok((s, None)) => missing_lots.push(s),
            Err(e) => log::error!("[LOT] startup fetch task panicked: {e}"),
        }
    }
    if !missing_lots.is_empty() {
        log::warn!("[LOT] no lot metadata yet for {missing_lots:?}; will retry");
    }
    log::info!(
        "[STARTUP] instance={} mode={} positions={} last_decision={:?}",
        cfg.instance_id,
        if cfg.dry_run { "DRY_RUN" } else { "LIVE" },
        engine.state.positions.len(),
        engine
            .state
            .last_decision
            .as_ref()
            .map(|r| (&r.key, r.outcome))
    );

    // Drain the price feed on its own task. The select! loop below awaits
    // `engine.tick()`, which can hold it for the fill-confirm timeout plus
    // venue calls; updates that queue up meanwhile would otherwise be
    // stamped with the moment the loop drained them, so a touch observed
    // before a 20 s confirm would look fresh for another 30 s and the
    // mid-relative bound (bot-strategy#971) would be derived from a
    // narrower book than the venue is about to apply it to. Stamping on
    // arrival keeps `WS_PRICE_MAX_AGE_SECS` an age of the observation.
    let feed_paper = paper.clone();
    let feed_live = live.clone();
    let mut feed = tokio::spawn(async move {
        loop {
            match price_rx.recv().await {
                Ok(PriceUpdate {
                    symbol,
                    mid_price,
                    best_bid,
                    best_ask,
                    ..
                }) => {
                    if let Some(px) = mid_price.to_f64() {
                        if let Some(p) = &feed_paper {
                            p.set_price(&symbol, px).await;
                        }
                        if let Some(l) = &feed_live {
                            // The touch is what bounds a live send against
                            // the mid (bot-strategy#971); an update without
                            // one still refreshes the mid.
                            match (best_bid.to_f64(), best_ask.to_f64()) {
                                (Some(b), Some(a)) => l.set_quote(&symbol, px, b, a).await,
                                _ => l.set_price(&symbol, px).await,
                            }
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("[WS] price feed lagged, dropped {n} updates");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let mut tick = tokio::time::interval(Duration::from_secs(5));
    let mut lot_retry = tokio::time::interval(Duration::from_secs(600));
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            // The feed is drained by its own task (see `feed` above) so a
            // quote is stamped when it arrives, not when this loop gets
            // back from a long tick; this arm only notices the feed dying.
            _ = &mut feed => {
                log::error!("[WS] price feed closed, exiting");
                break;
            }
            _ = tick.tick() => {
                if let Err(e) = engine.tick(now_secs()).await {
                    log::error!("[TICK] {e:#}");
                }
            }
            _ = lot_retry.tick() => {
                if !missing_lots.is_empty() {
                    // Concurrent for the same reason as the paper-funding
                    // refresh below: this arm runs inside the outer
                    // `select!`, so fetching one symbol after another
                    // would hold it for the sum of every request's
                    // latency, blocking ticks, WS handling and SIGTERM
                    // while lot metadata is unavailable for many symbols.
                    let mut fetches = tokio::task::JoinSet::new();
                    for s in missing_lots.clone() {
                        let connector = connector.clone();
                        fetches.spawn(async move {
                            let lot = fetch_lot(&connector, &s).await;
                            (s, lot)
                        });
                    }
                    let mut still = Vec::new();
                    while let Some(res) = fetches.join_next().await {
                        match res {
                            Ok((s, Some(lot))) => {
                                engine.set_lot(&s, lot);
                                if let Some(p) = &paper { p.set_lot(&s, lot).await; }
                            }
                            Ok((s, None)) => still.push(s),
                            Err(e) => log::error!("[LOT] fetch task panicked: {e}"),
                        }
                    }
                    missing_lots = still;
                }
                if let Some(p) = &paper {
                    // Keep paper funding rates fresh from the venue
                    // ticker. A failed refresh (request error, or no rate
                    // in the response) must clear the prior observation
                    // rather than leave it in place: an indefinitely
                    // stale rate would otherwise keep charging marks and
                    // closes at a number the venue no longer reports,
                    // instead of taking the unavailable-rate path that
                    // preserves a pending funding obligation.
                    //
                    // Fetched concurrently, not one symbol after another:
                    // this arm runs inside the outer `select!`, so a
                    // sequential loop would hold it for the *sum* of every
                    // symbol's request latency, during which the runtime
                    // cannot receive WS prices, run its 5s engine tick, or
                    // handle SIGTERM. Concurrent requests bound the stall
                    // to the slowest single one instead.
                    let mut refreshes = tokio::task::JoinSet::new();
                    for s in symbols.clone() {
                        let connector = connector.clone();
                        let p = p.clone();
                        refreshes.spawn(async move {
                            match connector.get_ticker(&s, None).await {
                                Ok(t) => match t.funding_rate.and_then(|r| r.to_f64()) {
                                    Some(r) => p.set_funding_rate_hourly(&s, r).await,
                                    None => p.clear_funding_rate_hourly(&s).await,
                                },
                                Err(_) => p.clear_funding_rate_hourly(&s).await,
                            }
                        });
                    }
                    while refreshes.join_next().await.is_some() {}
                }
            }
            _ = sigterm.recv() => {
                log::warn!("[SHUTDOWN] SIGTERM: positions are left as they are (no reduce-only close on stop); state persisted");
                break;
            }
        }
    }
    engine.state.persist(engine.state_path())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialises tests that hold an `flock` against tests that spawn a
    /// child process.
    ///
    /// `Command` forks, and the child inherits every open descriptor until
    /// it execs. A fork racing `acquire_instance_lock` therefore keeps that
    /// lock's *open file description* alive past the `drop` that should
    /// have released it, and the next acquisition fails with "already
    /// locked" -- the whole binary's tests share one process, so a bash
    /// spawn in one test can do this to a lock in another. Reproduced at
    /// 5/20 runs with just this test and the two shell round-trips
    /// scheduled together; 0/25 with either group alone.
    ///
    /// A test-harness constraint, not a product one: the runtime acquires
    /// the instance lock once at startup and never forks while holding it.
    static FORK_VS_FLOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Poison-tolerant: an unrelated panic inside the guard must not turn
    /// every other test in the group into a failure.
    fn no_fork_while_locked() -> std::sync::MutexGuard<'static, ()> {
        FORK_VS_FLOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn a_second_instance_lock_on_the_same_state_is_refused() {
        let _serialised = no_fork_while_locked();
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("state.json");
        let first = acquire_instance_lock(&state_path).unwrap();
        assert!(
            acquire_instance_lock(&state_path).is_err(),
            "a second process against the same state must be refused"
        );
        drop(first);
        // Releasing the first must let a fresh process start.
        assert!(acquire_instance_lock(&state_path).is_ok());
    }

    #[test]
    fn instance_locks_for_different_state_paths_do_not_interfere() {
        let _serialised = no_fork_while_locked();
        let dir = tempfile::tempdir().unwrap();
        let a = acquire_instance_lock(&dir.path().join("a").join("state.json")).unwrap();
        let b = acquire_instance_lock(&dir.path().join("b").join("state.json")).unwrap();
        drop(a);
        drop(b);
    }

    // ------------------------------------------------------------------
    // fetch env rendering (bot-strategy#948 / pairtrade#293)
    // ------------------------------------------------------------------

    #[test]
    fn env_line_accepts_what_the_fetch_env_carries_and_refuses_the_rest() {
        // Every shape the real configs produce.
        for v in [
            "xsmom_695_L28_H5_q20_riskadj",
            "/opt/book-runtime/exdiv-lighter.calendar.json",
            "2026-07-03",
            "00:30",
            "0.05",
            "true",
            "SPY,QQQ,US500",
            "",
            // both parsers split at the first `=` and keep the rest
            "desk=one",
            // Non-ASCII is not syntax to either parser and the runtime
            // accepts it, so it must not block a deploy.
            "prîd_日本語",
        ] {
            assert!(env_safe(v), "{v:?} must be accepted");
            assert_eq!(env_line("K", v).unwrap(), format!("K={v}\n"));
        }
        // Anything systemd's EnvironmentFile= parser and a shell would
        // read differently is refused rather than quoted: there is no
        // quoting idiom both accept identically.
        for v in [
            "desk's",
            "desk one",
            "a\"b",
            "a\\b",
            "a#b",
            "$HOME",
            "a`b`",
            "a\nb",
            "a;b",
            // a non-ASCII *separator* is still a separator
            "a\u{00a0}b",
        ] {
            assert!(!env_safe(v), "{v:?} must be refused");
            let err = env_line("BOOK_SIGNAL_PRODUCER_ID", v)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("BOOK_SIGNAL_PRODUCER_ID") && err.contains("EnvironmentFile"),
                "unhelpful error for {v:?}: {err}"
            );
        }
    }

    fn cfg_yaml(schedule: &str, signal_extra: &str) -> String {
        format!(
            r#"
schema_version: 1
instance_id: t
venue: lighter
dry_run: true
universe:
  symbols: [BTC, ETH]
schedule:
{schedule}
signal:
  path: /var/lib/book-runtime/t/signal.json
  producer_id: {signal_extra}
  max_age_secs: 7200
sizing:
  gross_notional_usd: 1000.0
  max_symbol_weight: 0.15
  max_gross_usd: 1000.0
  max_net_usd: 100.0
  min_order_usd: 10.0
  rebalance_deadband_usd: 5.0
execution:
  slippage_bps: 50
  max_attempts: 3
  fill_confirm_timeout_secs: 15
  paper_slippage_bps: 5.0
risk:
  equity_reference_usd: 1000.0
  max_session_loss_bps: 500.0
  max_daily_loss_bps: 500.0
  kill_switch_path: /var/lib/book-runtime/t/KILL_SWITCH
  risk_ack_path: /var/lib/book-runtime/t/RISK_ACK
paths:
  state: /var/lib/book-runtime/t/state.json
  ledger: /var/lib/book-runtime/t/ledger.jsonl
  pnl: /var/lib/book-runtime/t/pnl.jsonl
  status: /var/lib/book-runtime/t/status.json
"#
        )
    }

    fn env_of(yaml: &str) -> std::collections::HashMap<String, String> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.yaml");
        std::fs::write(&path, yaml).unwrap();
        let cfg = BookConfig::load(&path).expect("fixture config must parse");
        fetch_env(&cfg)
            .expect("fixture config must render")
            .lines()
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn fetch_env_renders_a_date_keyed_schedule() {
        let e = env_of(&cfg_yaml(
            "  kind: interval_days\n  anchor_date: 2026-07-03\n  every_days: 5\n  decision_time_utc: \"00:30\"\n  signal_grace_secs: 5400\n",
            "xsmom_695",
        ));
        assert_eq!(e["BOOK_SCHEDULE_KIND"], "interval_days");
        assert_eq!(e["BOOK_ANCHOR_DATE"], "2026-07-03");
        assert_eq!(e["BOOK_EVERY_DAYS"], "5");
        // quoted in the YAML; the fetcher must see the decoded value
        assert_eq!(e["BOOK_DECISION_TIME_UTC"], "00:30");
        assert_eq!(e["BOOK_SIGNAL_PRODUCER_ID"], "xsmom_695");
        assert_eq!(e["BOOK_SIGNAL_MAX_AGE_SECS"], "7200");
        assert_eq!(e["BOOK_MAX_SYMBOL_WEIGHT"], "0.15");
        assert_eq!(e["BOOK_UNIVERSE"], "BTC,ETH");
        assert_eq!(e["BOOK_CALENDAR_PATH"], "");
    }

    #[test]
    fn fetch_env_leaves_the_decision_time_empty_for_a_calendar_schedule() {
        // A calendar schedule derives no decision time from the key, and
        // the fetcher skips that check on an empty value. Emitting one
        // (even if the YAML carried a stray decision_time_utc) would make
        // the fetcher reject signals the runtime accepts.
        let e = env_of(&cfg_yaml(
            "  kind: calendar\n  calendar_path: /opt/book-runtime/t.calendar.json\n  decision_time_utc: \"09:29\"\n  signal_grace_secs: 45\n",
            "exdiv_v1",
        ));
        assert_eq!(e["BOOK_SCHEDULE_KIND"], "calendar");
        assert_eq!(e["BOOK_DECISION_TIME_UTC"], "");
        assert_eq!(e["BOOK_CALENDAR_PATH"], "/opt/book-runtime/t.calendar.json");
        assert_eq!(e["BOOK_ANCHOR_DATE"], "");
        assert_eq!(e["BOOK_EVERY_DAYS"], "");
    }

    #[test]
    fn fetch_env_refuses_a_producer_id_the_two_parsers_would_disagree_on() {
        // A YAML-valid scalar that no quoting scheme renders identically
        // for systemd's EnvironmentFile= parser and a shell. Failing here
        // is the point: the installer runs this before promotion, so the
        // deploy stops instead of shipping a fetcher that rejects every
        // signal the runtime accepts.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.yaml");
        std::fs::write(
            &path,
            cfg_yaml(
                "  kind: daily\n  decision_time_utc: \"00:30\"\n  signal_grace_secs: 60\n",
                "\"desk one's #1 feed\"",
            ),
        )
        .unwrap();
        let cfg = BookConfig::load(&path).expect("the config itself is valid YAML");
        let err = fetch_env(&cfg).unwrap_err().to_string();
        assert!(err.contains("BOOK_SIGNAL_PRODUCER_ID"), "unhelpful: {err}");
    }

    #[test]
    fn an_equals_sign_in_a_value_survives_the_split() {
        let _serialised = no_fork_while_locked(); // spawns bash; see the lock
                                                  // Both readers split the assignment at the first `=` only, so a
                                                  // value containing one needs no quoting and must not block a
                                                  // deploy (pairtrade#293 Codex).
        let line = env_line("BOOK_SIGNAL_PRODUCER_ID", "desk=one").unwrap();
        assert_eq!(line, "BOOK_SIGNAL_PRODUCER_ID=desk=one\n");
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join("fetch.env");
        std::fs::write(&env_path, &line).unwrap();
        let out = std::process::Command::new("bash")
            .arg("-c")
            .arg(format!(
                ". {}; printf '%s' \"$BOOK_SIGNAL_PRODUCER_ID\"",
                env_path.display()
            ))
            .output()
            .expect("bash must be available");
        assert_eq!(String::from_utf8_lossy(&out.stdout), "desk=one");
    }

    #[test]
    fn every_rendered_line_is_read_back_verbatim_by_a_shell() {
        let _serialised = no_fork_while_locked(); // spawns bash; see the lock
                                                  // The rendering has to survive its consumers unchanged. bash is
                                                  // the one available here; systemd's parser is the other, and the
                                                  // charset `env_safe` enforces is exactly the set the two read
                                                  // identically (unquoted, no expansion, no escapes).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.yaml");
        std::fs::write(
            &path,
            cfg_yaml(
                "  kind: interval_days\n  anchor_date: 2026-07-03\n  every_days: 5\n  decision_time_utc: \"00:30\"\n  signal_grace_secs: 5400\n",
                // non-ASCII included, since it is allowed through
                "prîd_日本語_695",
            ),
        )
        .unwrap();
        let cfg = BookConfig::load(&path).unwrap();
        let rendered = fetch_env(&cfg).unwrap();
        assert!(rendered.contains("BOOK_SIGNAL_PRODUCER_ID=prîd_日本語_695"));
        let env_path = dir.path().join("fetch.env");
        std::fs::write(&env_path, &rendered).unwrap();
        for line in rendered.lines() {
            let (key, value) = line.split_once('=').unwrap();
            let out = std::process::Command::new("bash")
                .arg("-c")
                .arg(format!(". {}; printf '%s' \"${key}\"", env_path.display()))
                .output()
                .expect("bash must be available");
            assert!(out.status.success(), "sourcing failed for {key}: {out:?}");
            assert_eq!(String::from_utf8_lossy(&out.stdout), value, "{key} changed");
        }
    }
}
