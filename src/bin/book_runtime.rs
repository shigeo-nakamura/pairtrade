//! Book runtime binary (bot-strategy#937): slow cross-sectional book on one
//! venue, driven by an external signal file. See `docs/book-runtime.md`.
//!
//! ```text
//! book-runtime --config configs/book/xsmom-695.yaml            # live loop (DRY_RUN per config)
//! book-runtime --config ... --replay <dir> --out <dir>         # deterministic replay
//! book-runtime --config ... --validate                          # parse + fingerprint only
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
use debot::book::config::BookConfig;
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
}

fn parse_args() -> Result<Args> {
    let mut config = std::env::var("BOOK_CONFIG").ok().map(PathBuf::from);
    let mut replay = None;
    let mut out = None;
    let mut validate = false;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" => config = Some(PathBuf::from(it.next().context("--config needs a path")?)),
            "--replay" => replay = Some(PathBuf::from(it.next().context("--replay needs a dir")?)),
            "--out" => out = Some(PathBuf::from(it.next().context("--out needs a dir")?)),
            "--validate" => validate = true,
            "-h" | "--help" => {
                eprintln!(
                    "usage: book-runtime --config <yaml> [--replay <dir> --out <dir>] [--validate]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }
    Ok(Args {
        config: config.context("--config <yaml> (or BOOK_CONFIG) is required")?,
        replay,
        out,
        validate,
    })
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

fn acquire_instance_lock(state_path: &Path) -> Result<InstanceLock> {
    if let Some(parent) = state_path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let lock_path = state_path.with_extension("lock");
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
    if args.validate {
        println!("ok fp={}", cfg.fingerprint());
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
    let _instance_lock = acquire_instance_lock(&cfg.paths.state)?;

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
    let mut missing_lots: Vec<String> = Vec::new();
    for s in &symbols {
        match fetch_lot(&connector, s).await {
            Some(lot) => {
                engine.set_lot(s, lot);
                if let Some(p) = &paper {
                    p.set_lot(s, lot).await;
                }
            }
            None => missing_lots.push(s.clone()),
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

    let mut tick = tokio::time::interval(Duration::from_secs(5));
    let mut lot_retry = tokio::time::interval(Duration::from_secs(600));
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            update = price_rx.recv() => {
                match update {
                    Ok(PriceUpdate { symbol, mid_price, .. }) => {
                        if let Some(px) = mid_price.to_f64() {
                            if let Some(p) = &paper { p.set_price(&symbol, px).await; }
                            if let Some(l) = &live { l.set_price(&symbol, px).await; }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("[WS] price feed lagged, dropped {n} updates");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        log::error!("[WS] price feed closed, exiting");
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                if let Err(e) = engine.tick(now_secs()).await {
                    log::error!("[TICK] {e:#}");
                }
            }
            _ = lot_retry.tick() => {
                if !missing_lots.is_empty() {
                    let mut still = Vec::new();
                    for s in &missing_lots {
                        match fetch_lot(&connector, s).await {
                            Some(lot) => {
                                engine.set_lot(s, lot);
                                if let Some(p) = &paper { p.set_lot(s, lot).await; }
                            }
                            None => still.push(s.clone()),
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
                    for s in &symbols {
                        match connector.get_ticker(s, None).await {
                            Ok(t) => match t.funding_rate.and_then(|r| r.to_f64()) {
                                Some(r) => p.set_funding_rate_hourly(s, r).await,
                                None => p.clear_funding_rate_hourly(s).await,
                            },
                            Err(_) => p.clear_funding_rate_hourly(s).await,
                        }
                    }
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

    #[test]
    fn a_second_instance_lock_on_the_same_state_is_refused() {
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
        let dir = tempfile::tempdir().unwrap();
        let a = acquire_instance_lock(&dir.path().join("a").join("state.json")).unwrap();
        let b = acquire_instance_lock(&dir.path().join("b").join("state.json")).unwrap();
        drop(a);
        drop(b);
    }
}
