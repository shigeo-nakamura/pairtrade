//! Engine core: the `PairTradeEngine` state struct and its submodules
//! (constructor, per-tick orchestration, planning, execution, risk,
//! reconciliation, persistence). Split out of `pairtrade/mod.rs` per
//! bot-strategy#444.

pub(super) mod construct;
pub(super) mod error_class;
pub(super) mod eval_helpers;
pub(super) mod execute;
pub(super) mod fetch_prices;
pub(super) mod gating;
pub(super) mod order_price;
pub(super) mod persistence;
pub(super) mod placement;
pub(super) mod plan;
pub(super) mod prom_metrics;
pub(super) mod reconcile;
pub(super) mod recovery;
pub(super) mod risk;
pub(super) mod shared_tick;
pub(super) mod status_snapshot;
pub(super) mod step;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use dex_connector::{DexConnector, PositionSnapshot};

use crate::ports::replay_dex::ReplayConnector;

use super::bar::BarBuilder;
use super::config::PairTradeConfig;
use super::instance::StrategyInstance;
use super::market::FeedHealth;
use super::state::PairSharedState;
use super::stats::PriceSample;
use super::{data_dump, execution_ledger, funding_history};

pub struct PairTradeEngine {
    pub(in crate::pairtrade) cfg: PairTradeConfig,
    pub(in crate::pairtrade) connector: Arc<dyn DexConnector + Send + Sync>,
    pub(in crate::pairtrade) instances: Vec<StrategyInstance>,
    pub(in crate::pairtrade) history: HashMap<String, VecDeque<PriceSample>>,
    /// Per-pair quantities (β / spread / z / Kalman / eval result) shared
    /// across every `StrategyInstance` on the same pair. Computed exactly
    /// once per tick in `step_pair_shared`, so A/B/C variants observe
    /// byte-identical β / std / z. See bot-strategy#413.
    pub(in crate::pairtrade) per_pair_state: HashMap<String, PairSharedState>,
    pub(in crate::pairtrade) bar_builders: HashMap<String, BarBuilder>,
    /// Per-symbol accepted-tick freshness for the #531 ineligible-close
    /// guard's `stale` signal: the raw snapshot's `exchange_ts` stays
    /// fresh through a tick-filter rejection storm (corrupt frames keep
    /// arriving), so the guard needs the accepted-tick clock plus a
    /// post-gap recovery marker. Maintained in `step_shared_tick`; not
    /// persisted (a restart force-closes positions anyway).
    pub(in crate::pairtrade) tick_feed_health: HashMap<String, FeedHealth>,
    pub(in crate::pairtrade) last_metrics_log: Option<Instant>,
    pub(in crate::pairtrade) last_ob_warn: HashMap<String, Instant>,
    pub(in crate::pairtrade) last_ticker_warn: HashMap<String, Instant>,
    pub(in crate::pairtrade) last_position_warn: HashMap<String, Instant>,
    pub(in crate::pairtrade) min_order_warned: HashSet<String>,
    pub(in crate::pairtrade) min_tick_warned: HashSet<String>,
    pub(in crate::pairtrade) positions_ready: bool,
    pub(in crate::pairtrade) open_positions: HashMap<String, PositionSnapshot>,
    pub(in crate::pairtrade) history_path: PathBuf,
    /// Path for the risk-state persistence file (circuit breaker counters
    /// + cool-down deadline). Sibling of `history_path`. See bot-strategy#185.
    pub(in crate::pairtrade) risk_state_path: PathBuf,
    /// Cached result of the most recent `kill_switch_path()` existence check.
    /// Refreshed at the top of every `step_shared` tick. True blocks new
    /// entries across all instances.
    pub(in crate::pairtrade) kill_switch_active: bool,
    pub(in crate::pairtrade) data_dump_writer: Option<data_dump::RotatingDumpWriter>,
    pub(in crate::pairtrade) execution_ledger: Option<execution_ledger::ExecutionLedger>,
    /// Per-tick regime-detector series CSV writer (BT calibration aid for
    /// bot-strategy#534/#494). `Some` only when `cfg.bt_regime_series_file`
    /// is set; flushed at backtest end-of-data.
    pub(in crate::pairtrade) regime_series_writer: Option<std::io::BufWriter<std::fs::File>>,
    pub(in crate::pairtrade) replay_connector: Option<Arc<ReplayConnector>>,
    /// Rolling per-symbol funding-rate history observed from WS, used by
    /// `exit_fill` to compute `funding_carry_usd` on each cycle without an
    /// external REST fetch. bot-strategy#364.
    pub(in crate::pairtrade) funding_history: funding_history::FundingHistory,
    /// Graceful shutdown flag. When true:
    ///   - new entries are blocked
    ///   - existing exit logic (exit_z / stop_loss_z / force_close_secs) runs normally
    ///   - live loop exits as soon as open_positions is empty, or after shutdown_grace_secs
    pub(in crate::pairtrade) shutdown_pending: bool,
    /// Recent bar-emit timestamps per symbol, for the [BAR_RATE] canary
    /// (bot-strategy#341). Trimmed to the trailing 120 s; the canary warns
    /// if sustained < 0.8 emits/min, which would have caught the original
    /// Phase 2 β-freeze in <30 min instead of 78 h.
    pub(in crate::pairtrade) bar_emit_log: HashMap<String, VecDeque<Instant>>,
    /// Last time a [BAR_RATE] WARN fired per symbol, used to rate-limit
    /// the warning to ~once per minute so a sustained low rate doesn't
    /// flood the journal.
    pub(in crate::pairtrade) last_bar_rate_warn: HashMap<String, Instant>,
    /// Fingerprint of the most recently emitted `[WARM_START] snapshot
    /// loaded ...` INFO line. `load_history_from_disk` runs on every
    /// polling tick (engine/step.rs:511), so a naive INFO emit fires
    /// ~12×/min on the typical 5 s polling cadence. We dedup on this
    /// key so an operator rolling back a snapshot still sees the
    /// "loaded" line in journalctl (content changes → key differs →
    /// emit) while steady-state per-tick reloads stay quiet. WARN
    /// paths (stale-guard, parse-error, partial) are always emitted.
    pub(in crate::pairtrade) last_warm_start_key: Option<String>,
}
