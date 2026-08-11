//! Integration tests for the risk-halt entry gates (bot-strategy#537).
//!
//! The risk layer's snapshot math (daily_loss_breaches_threshold,
//! session_dd_breaches_threshold, rolling_peak) is unit-tested in
//! `engine/risk.rs`, but nothing exercised the gates' core contract at the
//! tick level: **when a halt is active, new entries must be suppressed
//! while exits keep flowing**. These tests drive the real production tick
//! entry point — `PairTradeEngine::step()` — end to end:
//!
//!   step() → step_shared (kill-switch refresh, risk-ack consume, daily
//!   session refresh, price fetch, bar emit, shared pair eval) →
//!   step_for_instance (setup → plan → execute exits → execute entry).
//!
//! So they fail if someone removes `update_kill_switch_state()` /
//! `consume_risk_ack()` from `engine/shared_tick.rs` or the
//! `pre_reject_reason` halt chain from `engine/plan.rs` — not just if the
//! threshold helpers in `engine/risk.rs` regress.
//!
//! Determinism notes:
//! - The engine runs in live mode (not backtest) with `dry_run=true`, so
//!   entries/exits are paper-filled in-process and the halt paths
//!   (`refresh_daily_session`, `consume_risk_ack`, ...) are NOT skipped by
//!   their `backtest_mode` early-returns.
//! - `prime_bars()` re-arms each BarBuilder with a tick in an old 1-second
//!   bucket, so the polling push inside the very next `step()` crosses the
//!   bucket boundary and deterministically emits one bar per symbol — no
//!   wall-clock sleeping between steps.
//! - The shared pair state is seeded with 29 zero spreads; each emitted
//!   bar pushes one `ln(105.127) − ln(100) ≈ +0.05` outlier, yielding
//!   z ≈ 5.39 / 3.74 / 3.00 on the first three steps — always above the
//!   entry threshold (2.0) and below the stop gate (stop_loss_z = 10),
//!   so an entry fires on every step unless a halt gate blocks it.
//! - Sentinel paths are process-global `OnceLock`s (resolved once at boot,
//!   like live). `sentinel_dir()` pins both `KILL_SWITCH_PATH` and
//!   `RISK_ACK_PATH` to a temp dir before first resolution, and the tests
//!   serialize on `gate_lock()` because they share those sentinel files
//!   and read the process-global Prometheus reject counters.

use super::bar::BarBuilder;
use super::defaults::DEFAULT_EQUITY_USD;
use super::risk_io::EquitySample;
use super::state::{PairSharedState, PairState, Position, PositionDirection};
use super::*;
use async_trait::async_trait;
use dex_connector::{
    BalanceResponse, CanceledOrdersResponse, CreateOrderResponse, DexConnector, DexError,
    FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse, OrderBookLevel,
    OrderBookSnapshot, OrderSide, PositionSnapshot, TickerResponse, TpSl, TriggerOrderStyle,
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn dec(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

const BASE: &str = "AAA";
const QUOTE: &str = "BBB";
const PAIR_KEY: &str = "AAA/BBB";
/// ln(105.127) − ln(100) ≈ 0.05 — the per-bar spread outlier (see module
/// docs for the resulting z trajectory across steps).
const PRICE_A: &str = "105.127";
const PRICE_B: &str = "100.0";

/// Minimal `DexConnector` mock for full-`step()` drives. Unlike the
/// `DummyConnector` in `pending_tests.rs` (which targets the placement /
/// reconcile internals), this one serves a coherent ticker + order book so
/// `fetch_latest_prices` passes `tick_sanity_check` and the bar pipeline
/// runs; everything order-side is irrelevant because the engine runs with
/// `dry_run=true` (paper fills only).
#[derive(Default)]
struct GateConnector {
    prices: Mutex<HashMap<String, Decimal>>,
    equity: Mutex<Decimal>,
    /// Per-side book half-spread as a fraction of price. `None` keeps the
    /// historical ±10 bps book; the #531 guard tests widen it to a
    /// degraded-but-not-corrupt level (above the 20 bps guard threshold,
    /// below the 200 bps tick-filter reject).
    half_spread_frac: Mutex<Option<Decimal>>,
}

impl GateConnector {
    fn set_price(&self, symbol: &str, price: Decimal) {
        self.prices
            .lock()
            .unwrap()
            .insert(symbol.to_string(), price);
    }

    fn price_of(&self, symbol: &str) -> Decimal {
        self.prices
            .lock()
            .unwrap()
            .get(symbol)
            .copied()
            .unwrap_or_default()
    }

    fn set_half_spread_frac(&self, frac: Decimal) {
        *self.half_spread_frac.lock().unwrap() = Some(frac);
    }

    fn half_spread_of(&self, price: Decimal) -> Decimal {
        let frac = self
            .half_spread_frac
            .lock()
            .unwrap()
            .unwrap_or_else(|| dec("0.001"));
        price * frac
    }
}

#[async_trait]
impl DexConnector for GateConnector {
    async fn start(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
        Ok(())
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        _test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        Ok(TickerResponse {
            symbol: symbol.to_string(),
            price: self.price_of(symbol),
            min_order: Some(dec("0.001")),
            min_tick: Some(dec("0.001")),
            size_decimals: Some(3),
            funding_rate: Some(Decimal::ZERO),
            ..Default::default()
        })
    }

    async fn get_filled_orders(&self, _symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        Ok(FilledOrdersResponse::default())
    }

    async fn get_canceled_orders(&self, _symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        Ok(CanceledOrdersResponse::default())
    }

    async fn get_open_orders(&self, _symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        Ok(OpenOrdersResponse::default())
    }

    async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        let equity = *self.equity.lock().unwrap();
        Ok(BalanceResponse {
            equity,
            balance: equity,
            position_entry_price: None,
            position_sign: None,
        })
    }

    async fn get_combined_balance(
        &self,
    ) -> Result<dex_connector::CombinedBalanceResponse, DexError> {
        Ok(dex_connector::CombinedBalanceResponse::default())
    }

    async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
        Ok(Vec::new())
    }

    async fn get_last_trades(&self, _symbol: &str) -> Result<LastTradesResponse, DexError> {
        Ok(LastTradesResponse::default())
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        _depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        // ±10 bps book around the ticker price by default: comfortably
        // inside MAX_TICK_SPREAD_BPS / MAX_TICK_PRICE_ENVELOPE_BPS so the
        // polled snapshot survives tick_sanity_check and reaches the
        // BarBuilder. Tests may widen it via `set_half_spread_frac`.
        let price = self.price_of(symbol);
        let spread = self.half_spread_of(price);
        Ok(OrderBookSnapshot {
            bids: vec![OrderBookLevel {
                price: price - spread,
                size: dec("100"),
            }],
            asks: vec![OrderBookLevel {
                price: price + spread,
                size: dec("100"),
            }],
            // dex-connector v4.7.1 (bot-strategy#552): compat-only.
            book_ts_ms: None,
        })
    }

    async fn clear_filled_order(&self, _symbol: &str, _trade_id: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_canceled_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn create_order(
        &self,
        _symbol: &str,
        size: Decimal,
        _side: OrderSide,
        price: Option<Decimal>,
        _spread: Option<i64>,
        _reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        // dry_run paper-fills in-process; nothing should reach the venue.
        Ok(CreateOrderResponse {
            order_id: "halt-gate-test".to_string(),
            exchange_order_id: None,
            ordered_price: price.unwrap_or(Decimal::ONE),
            ordered_size: size,
            client_order_id: None,
        })
    }

    async fn create_advanced_trigger_order(
        &self,
        _symbol: &str,
        _size: Decimal,
        _side: OrderSide,
        _trigger_px: Decimal,
        _limit_px: Option<Decimal>,
        _order_style: TriggerOrderStyle,
        _slippage_bps: Option<u32>,
        _tpsl: TpSl,
        _reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(DexError::Permanent("not used".to_string()))
    }

    async fn create_order_taker_ioc(
        &self,
        _symbol: &str,
        _size: Decimal,
        _side: OrderSide,
        _slippage_bps: u32,
        _reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(DexError::Permanent("not used".to_string()))
    }

    async fn modify_order(
        &self,
        _symbol: &str,
        _order_id: &str,
        _side: OrderSide,
        _target_total_size: Decimal,
        _open_remaining_size: Decimal,
        _price: Option<Decimal>,
        _spread: Option<i64>,
        _reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(DexError::Permanent("not used".to_string()))
    }

    async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Ok(())
    }

    async fn cancel_orders(
        &self,
        _symbol: Option<String>,
        _order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        Ok(())
    }

    async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Ok(())
    }

    async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
        false
    }

    async fn sign_evm_65b(&self, _message: &str) -> Result<String, DexError> {
        Ok("signed".to_string())
    }

    async fn sign_evm_65b_with_eip191(&self, _message: &str) -> Result<String, DexError> {
        Ok("signed".to_string())
    }

    fn subscribe_price_updates(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<dex_connector::PriceUpdate>, DexError> {
        Err(DexError::Permanent("not used".to_string()))
    }
}

/// Redirect both operator sentinels to a per-process temp dir and pin the
/// process-global `OnceLock` resolution (bot-strategy#488 / #537). Mirrors
/// live behavior — paths resolve once at boot — so every test in this
/// binary shares the redirected paths.
fn sentinel_dir() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("pairtrade-halt-gates-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create sentinel dir");
        std::env::set_var("KILL_SWITCH_PATH", dir.join("KILL_SWITCH"));
        std::env::set_var("RISK_ACK_PATH", dir.join("RISK_ACK"));
        // Resolve NOW and verify the override landed. If some other test
        // ever resolves these OnceLocks first, fail loudly here instead of
        // letting the suite touch /opt/debot.
        assert_eq!(
            kill_switch_path(),
            dir.join("KILL_SWITCH").to_str().unwrap(),
            "kill_switch_path resolved before the test override was set"
        );
        assert_eq!(
            risk_ack_path(),
            dir.join("RISK_ACK").to_str().unwrap(),
            "risk_ack_path resolved before the test override was set"
        );
        dir
    })
    .as_path()
}

/// Serializes the halt-gate tests: they share the sentinel files above and
/// assert on absolute values of per-(variant, pair) Prometheus counters.
fn gate_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Remove any sentinel left behind by a previously failed test so one
/// assertion failure does not cascade into unrelated gate trips. Pins the
/// env override first — this is the first sentinel-path access in every
/// test, so resolving before `sentinel_dir()` would latch the production
/// `/opt/debot/...` defaults into the process-global OnceLocks.
fn clear_sentinels() {
    sentinel_dir();
    let _ = std::fs::remove_file(kill_switch_path());
    let _ = std::fs::remove_file(risk_ack_path());
}

fn reject_count(instance_id: &str, reason: &str) -> u64 {
    prom::ENTRY_REJECT_TOTAL
        .with_label_values(&[instance_id, PAIR_KEY, reason])
        .get()
}

struct Harness {
    engine: PairTradeEngine,
    _connector: Arc<GateConnector>,
    _state_dir: TempDir,
    prime_seq: i64,
}

impl Harness {
    /// Live-mode dry_run engine, one instance named `instance_id` (unique
    /// per test so the shared Prometheus reject counters don't collide),
    /// AAA/BBB seeded one push away from a valid entry signal.
    fn new(instance_id: &str) -> Self {
        sentinel_dir();
        let connector = Arc::new(GateConnector::default());
        connector.set_price(BASE, dec(PRICE_A));
        connector.set_price(QUOTE, dec(PRICE_B));
        *connector.equity.lock().unwrap() = Decimal::from(10_000);

        let mut engine = PairTradeEngine::test_instance(connector.clone());
        let state_dir = TempDir::new().unwrap();
        engine.risk_state_path = state_dir.path().join("risk_state.json");
        engine.cfg.metrics_window = 30;

        // test_instance leaves the *instance* params at the all-zero
        // `PairParams::default()`, which would gate every entry through
        // `stop_loss_z=0` and force-close any position instantly. Use the
        // production-shaped cfg defaults, adjusted so the gates NOT under
        // test stay out of the way:
        //   stop_loss_z=10    — the seeded outlier z (3.0–5.4) must not
        //                       trip the stop gate;
        //   spread_trend=1e9  — the outlier gives the seeded history a
        //                       positive slope by construction;
        //   cooldown_secs=0   — re-entry directly after an exit step.
        let mut pp = engine.cfg.default_pair_params.clone();
        pp.stop_loss_z = 10.0;
        pp.spread_trend_max_slope_sigma = 1e9;
        pp.cooldown_secs = 0;
        engine.cfg.default_pair_params = pp.clone();

        let now_ts = chrono::Utc::now().timestamp();
        {
            let inst = &mut engine.instances[0];
            inst.id = instance_id.to_string();
            inst.default_pair_params = pp;
            // Anchor the daily session at "now" so refresh_daily_session
            // does not treat the first tick as a rollover and wipe the
            // seeded realized_pnl_today.
            inst.session_start_ts = now_ts;
            inst.session_start_equity = DEFAULT_EQUITY_USD;
            inst.states
                .insert(PAIR_KEY.to_string(), PairState::new(2.0));
        }

        // Shared pair state: eligible, already evaluated (so the
        // `waiting_first_eval` pre-reject does not fire), and 29 zero
        // spreads waiting for the per-step outlier push.
        let mut shared = PairSharedState::new(engine.cfg.metrics_window);
        shared.eligible = true;
        shared.last_evaluated = Some(Instant::now());
        shared.last_evaluated_ts = Some(now_ts);
        shared.spread_history = std::iter::repeat(0.0).take(29).collect();
        engine.per_pair_state.insert(PAIR_KEY.to_string(), shared);

        Harness {
            engine,
            _connector: connector,
            _state_dir: state_dir,
            prime_seq: 0,
        }
    }

    /// Append a peer instance (multi-variant A/B/C shape) sharing the same
    /// connector and per-pair state, mirroring `new_inner`'s layout.
    fn add_peer_instance(&mut self, instance_id: &str) {
        let now_ts = chrono::Utc::now().timestamp();
        let pp = self.engine.cfg.default_pair_params.clone();
        let mut states = HashMap::new();
        states.insert(PAIR_KEY.to_string(), PairState::new(2.0));
        self.engine.instances.push(StrategyInstance {
            id: instance_id.to_string(),
            connector: self._connector.clone(),
            equity_cache: DEFAULT_EQUITY_USD,
            last_equity_fetch: None,
            equity_initialized: false,
            equity_reference_usd: DEFAULT_EQUITY_USD,
            states,
            pnl_logger: None,
            status_reporter: None,
            consecutive_losses: 0,
            circuit_breaker_until: None,
            circuit_breaker_until_ts: None,
            session_start_equity: DEFAULT_EQUITY_USD,
            session_equity_reference_usd: DEFAULT_EQUITY_USD,
            session_start_ts: now_ts,
            realized_pnl_today: 0.0,
            funding_carry_today: 0.0,
            total_funding_carry: 0.0,
            daily_loss_halted: false,
            equity_samples: Vec::new(),
            capital_baseline_equity: 0.0,
            capital_baseline_accounted_pnl: Some(0.0),
            capital_position_seen_since_baseline: false,
            capital_rebaseline_deferred: false,
            capital_rebaseline_deferred_since: None,
            flat_since: None,
            session_halted: false,
            session_halt_reason: None,
            session_halt_ts: None,
            total_trades: 0,
            total_wins: 0,
            total_pnl: 0.0,
            peak_pnl: 0.0,
            max_dd: 0.0,
            pair_params: HashMap::new(),
            default_pair_params: pp,
            external_flatten_reason: None,
            entry_blocked_pairs: HashMap::new(),
        });
    }

    /// Re-arm both BarBuilders with a tick in an old (distinct per call)
    /// 1-second bucket so the polling push inside the next `step()`
    /// crosses the bucket boundary and emits exactly one bar per symbol.
    /// Without this, `updated` stays empty and the planner skips the pair
    /// entirely — entries would be absent for the wrong reason.
    fn prime_bars(&mut self) {
        self.prime_seq += 1;
        let tick_ts_ms = (chrono::Utc::now().timestamp() - 120 + self.prime_seq) * 1000;
        for (symbol, price) in [(BASE, dec(PRICE_A)), (QUOTE, dec(PRICE_B))] {
            let mut builder = BarBuilder::new(self.engine.cfg.trading_period_secs);
            builder.push(tick_ts_ms, price);
            self.engine.bar_builders.insert(symbol.to_string(), builder);
        }
    }

    /// One real production tick: bar emit + shared eval + per-instance
    /// plan/execute. With no halt active this opens a dry-run position.
    async fn step(&mut self) {
        self.prime_bars();
        self.engine.step().await.expect("step() must succeed");
    }

    fn position(&self, inst_idx: usize) -> Option<Position> {
        self.engine.instances[inst_idx]
            .states
            .get(PAIR_KEY)
            .and_then(|s| s.position.clone())
    }

    /// Seed an open position whose hold time is far past
    /// `force_close_secs` (60s), so the next planning pass must schedule a
    /// forced exit regardless of any active entry halt.
    fn seed_aged_position(&mut self, inst_idx: usize) {
        self.seed_position(inst_idx, 3600);
    }

    /// Seed an open position `age_secs` into its hold. The #531 guard
    /// tests use a fresh position (age well below `force_close_secs`) so
    /// the ineligible-flatten branch — not the force-close branch — is
    /// the one that decides the exit.
    fn seed_position(&mut self, inst_idx: usize, age_secs: i64) {
        let now_ts = chrono::Utc::now().timestamp();
        let state = self.engine.instances[inst_idx]
            .states
            .get_mut(PAIR_KEY)
            .unwrap();
        state.position = Some(Position {
            direction: PositionDirection::LongSpread,
            entered_at: Instant::now(),
            entered_ts: now_ts - age_secs,
            entry_price_a: Some(dec(PRICE_A)),
            entry_price_b: Some(dec(PRICE_B)),
            entry_size_a: Some(dec("0.476")),
            entry_size_b: Some(dec("0.5")),
            entry_z: Some(2.4),
            entry_beta: Some(1.0),
            last_rehedge_ts: None,
            rehedge_realized_pnl: None,
            prev_beta_for_velocity: None,
        });
    }
}

// ---------------------------------------------------------------------
// Scenario 1 (bot-strategy#537): daily-loss halt. Entries suppressed via
// the plan-path `daily_loss` pre-reject while the breach stands; the gate
// releases without a restart once the session PnL no longer breaches
// (live: the UTC session rollover resets realized_pnl_today).
// ---------------------------------------------------------------------
#[tokio::test]
async fn daily_loss_halt_blocks_new_entries_and_releases_after_reset() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-daily");
    h.engine.cfg.risk.max_daily_loss_bps = 300; // 1x-equivalent; max_leverage = 1.0
    h.engine.instances[0].realized_pnl_today = -400.0; // 400 bps loss on $10k ≥ 300 bps

    h.step().await;

    assert!(
        h.position(0).is_none(),
        "entry must be suppressed while the daily loss breaches the threshold"
    );
    assert!(
        h.engine.instances[0].daily_loss_halted,
        "refresh_daily_session must have latched the halt flag"
    );
    assert_eq!(
        reject_count("hg-daily", "daily_loss"),
        1,
        "the plan-path pre-reject must attribute the block to daily_loss"
    );

    // Simulate the UTC session rollover effect: realized PnL no longer
    // breaches → the very next tick clears the halt and the queued entry
    // signal goes through, all without a process restart.
    h.engine.instances[0].realized_pnl_today = 0.0;
    h.step().await;

    assert!(
        !h.engine.instances[0].daily_loss_halted,
        "halt flag must clear once the breach is gone"
    );
    assert!(
        h.position(0).is_some(),
        "entry must resume on the first tick after the halt clears"
    );
}

#[tokio::test]
async fn daily_loss_halt_still_allows_open_position_to_exit() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-daily-exit");
    h.engine.cfg.risk.max_daily_loss_bps = 300;
    h.engine.instances[0].realized_pnl_today = -400.0;
    h.seed_aged_position(0);

    h.step().await;

    // The halt blocks Open actions only: the aged position must still be
    // force-closed (dry-run paper fill) on the same tick.
    assert!(
        h.position(0).is_none(),
        "force_close exit must execute despite the active daily-loss halt"
    );
    assert_eq!(
        h.engine.instances[0].total_trades, 1,
        "the exit must flow through write_pnl_record (a real close, not a state wipe)"
    );
    assert!(
        h.engine.instances[0]
            .states
            .get(PAIR_KEY)
            .unwrap()
            .last_exit_ts
            .is_some(),
        "post-exit state must record the exit timestamp"
    );
    // And the halt must still keep new entries out on the next tick.
    h.step().await;
    assert!(
        h.position(0).is_none(),
        "no re-entry while the daily-loss halt is still active"
    );
}

// ---------------------------------------------------------------------
// Scenario 2 (bot-strategy#537): sticky session-DD halt + RISK_ACK file.
// The halt blocks entries via the `session_halted` pre-reject; dropping
// the ack file (env-overridden RISK_ACK_PATH → temp dir) is consumed by
// step_shared on the next tick and releases the gate without a restart.
// ---------------------------------------------------------------------
#[tokio::test]
async fn session_dd_halt_blocks_entries_until_risk_ack_consumed() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-session");
    {
        let inst = &mut h.engine.instances[0];
        inst.session_halted = true;
        inst.session_halt_reason = Some("session_dd_500bps_lev1.0".to_string());
        inst.session_halt_ts = Some(chrono::Utc::now().timestamp());
    }

    h.step().await;

    assert!(
        h.position(0).is_none(),
        "entry must be suppressed while session_halted is set"
    );
    assert!(h.engine.instances[0].session_halted, "halt is sticky");
    assert_eq!(reject_count("hg-session", "session_halted"), 1);

    // Operator drops the ack file. step_shared's consume_risk_ack must
    // pick it up at the top of the next tick, clear the halt, delete the
    // file, and let the same tick's entry signal through.
    std::fs::write(risk_ack_path(), "ack: bot-strategy#537 test").unwrap();
    h.step().await;

    assert!(
        !h.engine.instances[0].session_halted,
        "risk-ack consumption must clear the session halt without a restart"
    );
    assert!(
        h.engine.instances[0].session_halt_reason.is_none(),
        "halt reason must be wiped together with the halt"
    );
    assert!(
        !Path::new(risk_ack_path()).exists(),
        "the ack sentinel must be consumed (removed) so a stale ack cannot re-fire"
    );
    assert!(
        h.position(0).is_some(),
        "entry must resume on the tick that consumed the ack"
    );
}

// ---------------------------------------------------------------------
// Scenario 3 (bot-strategy#537): KILL_SWITCH sentinel file. Present →
// entries blocked, exits still flow; removed → entries resume. Path is
// env-overridden (KILL_SWITCH_PATH → temp dir, see sentinel.rs).
// ---------------------------------------------------------------------
#[tokio::test]
async fn kill_switch_blocks_entries_allows_exits_until_removed() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-kill");
    std::fs::write(kill_switch_path(), "").unwrap();

    // Phase 1: sentinel present → no entry, attributed to kill_switch.
    h.step().await;
    assert!(
        h.engine.kill_switch_active,
        "step_shared must latch kill_switch_active from the sentinel file"
    );
    assert!(
        h.position(0).is_none(),
        "entry must be suppressed while the kill switch is engaged"
    );
    assert_eq!(reject_count("hg-kill", "kill_switch"), 1);

    // Phase 2: existing positions still exit normally under the switch.
    h.seed_aged_position(0);
    h.step().await;
    assert!(
        h.position(0).is_none(),
        "force_close exit must execute despite the kill switch"
    );
    assert_eq!(
        h.engine.instances[0].total_trades, 1,
        "the exit must be a real paper close (write_pnl_record ran)"
    );

    // Phase 3: sentinel removed → entries resume on the next tick, no
    // restart required.
    std::fs::remove_file(kill_switch_path()).unwrap();
    h.step().await;
    assert!(
        !h.engine.kill_switch_active,
        "kill_switch_active must drop once the sentinel is removed"
    );
    assert!(
        h.position(0).is_some(),
        "entry must resume after the kill switch is released"
    );
}

// ---------------------------------------------------------------------
// Scenario 4 (bot-strategy#537): circuit breaker. A future
// `circuit_breaker_until_ts` blocks entries via the plan-path pre-reject;
// once expired the same signal enters.
// ---------------------------------------------------------------------
#[tokio::test]
async fn circuit_breaker_blocks_entries_until_expiry() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cb");
    let now_ts = chrono::Utc::now().timestamp();
    {
        let inst = &mut h.engine.instances[0];
        inst.consecutive_losses = 3;
        inst.circuit_breaker_until = Some(Instant::now() + Duration::from_secs(3600));
        inst.circuit_breaker_until_ts = Some(now_ts + 3600);
    }

    h.step().await;
    assert!(
        h.position(0).is_none(),
        "entry must be suppressed while the circuit-breaker cool-down is active"
    );
    assert_eq!(reject_count("hg-cb", "circuit_breaker"), 1);

    // Cool-down expiry: the gate compares now_ts < until_ts, so a past
    // deadline stops blocking with no other state change.
    {
        let inst = &mut h.engine.instances[0];
        inst.circuit_breaker_until = Some(Instant::now());
        inst.circuit_breaker_until_ts = Some(chrono::Utc::now().timestamp() - 1);
    }
    h.step().await;
    assert!(
        h.position(0).is_some(),
        "entry must resume once the circuit-breaker cool-down has expired"
    );
}

// ---------------------------------------------------------------------
// Scenario 5 (bot-strategy#537): halts are per-instance. A session halt
// on variant B must not gate entries on variant A in the same process.
// (The kill switch, by contrast, is engine-wide by design.)
// ---------------------------------------------------------------------
#[tokio::test]
async fn variant_halt_does_not_block_peer_instances() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-multi-a");
    h.add_peer_instance("hg-multi-b");
    {
        let inst_b = &mut h.engine.instances[1];
        inst_b.session_halted = true;
        inst_b.session_halt_reason = Some("session_dd_500bps_lev1.0".to_string());
    }

    h.step().await;

    assert!(
        h.position(0).is_some(),
        "the un-halted variant must enter normally"
    );
    assert!(
        h.position(1).is_none(),
        "the halted variant must not enter even though its peer did"
    );
    assert_eq!(
        reject_count("hg-multi-b", "session_halted"),
        1,
        "variant B's block must be attributed to its own session halt"
    );
    assert_eq!(
        reject_count("hg-multi-a", "session_halted"),
        0,
        "variant A must not be touched by B's halt"
    );
}

// ---------------------------------------------------------------------
// Scenario 6 (bot-strategy#575 ③): a halted instance keeps refreshing its
// equity from live collateral — it must NOT freeze at the persisted sample.
// A deposit that lands while the variant is halted is reflected in
// `equity_cache` and the latest `equity_samples` entry on the next refresh,
// without a restart. (① is disabled here via min_usd=0 to isolate the
// pure equity-tracking path.)
// ---------------------------------------------------------------------
#[tokio::test]
async fn halted_instance_equity_tracks_collateral_without_restart() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-halt-equity");
    h.engine.cfg.risk.max_session_loss_bps = 500; // session DD enabled → sampling on
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 0.0; // isolate ③ from ①
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_sample_secs = 1; // a fresh sample bucket each second
    {
        let inst = &mut h.engine.instances[0];
        inst.session_halted = true;
        inst.session_halt_reason = Some("session_dd_500bps_lev1.0".to_string());
        inst.session_halt_ts = Some(chrono::Utc::now().timestamp());
    }

    *h._connector.equity.lock().unwrap() = Decimal::from(950);
    h.step().await;
    assert!(
        (h.engine.instances[0].equity_cache - 950.0).abs() < 1e-6,
        "first refresh seeds equity from collateral"
    );

    // Operator tops up while the variant is still halted. Production refreshes
    // every EQUITY_REFRESH_CACHE_SECS (300 s); clear the cache stamp so the
    // next tick refetches deterministically in-test.
    *h._connector.equity.lock().unwrap() = Decimal::from(960);
    h.engine.instances[0].last_equity_fetch = None;
    h.step().await;

    let inst = &h.engine.instances[0];
    assert!(
        inst.session_halted,
        "③ refreshes equity while halted; it does not resume trading"
    );
    assert!(
        (inst.equity_cache - 960.0).abs() < 1e-6,
        "halted instance equity must follow live collateral, not freeze at the persisted sample"
    );
    let last = inst
        .equity_samples
        .last()
        .expect("a rolling-peak sample exists");
    assert!(
        (last.equity - 960.0).abs() < 1e-6,
        "the latest equity sample tracks live collateral while halted"
    );
}

// ---------------------------------------------------------------------
// Scenario 7 (bot-strategy#575 ①): a deposit detected while flat + settled
// rebaselines the rolling peak to current equity (DD → 0) and shifts the
// daily-DD denominator — but does NOT clear the sticky halt (resuming stays
// an explicit operator ack). A sub-threshold drift does not fire.
// ---------------------------------------------------------------------
#[test]
fn deposit_while_flat_rebaselines_peak_without_clearing_halt() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-deposit");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;

    let now = chrono::Utc::now().timestamp();
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_cache = 950.0;
        // Sticky inflated peak from before the halt (the #471 artifact shape).
        inst.equity_samples = vec![EquitySample {
            ts: now - 100,
            equity: 1_003.0,
        }];
        inst.capital_baseline_equity = 950.0;
        inst.session_start_equity = 1_000.0;
        inst.session_halted = true;
        inst.session_halt_reason = Some("session_dd_500bps_lev1.0".to_string());
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    // No deposit yet: equity == baseline, so nothing is rebaselined.
    h.engine.detect_capital_event_and_rebaseline(0);
    assert!(
        (h.engine.instances[0].equity_samples[0].equity - 1_003.0).abs() < 1e-9,
        "no rebaseline before a real capital event"
    );

    // A $3 sub-threshold drift must also not fire.
    h.engine.instances[0].equity_cache = 953.0;
    h.engine.detect_capital_event_and_rebaseline(0);
    assert!(
        (h.engine.instances[0].equity_samples[0].equity - 1_003.0).abs() < 1e-9,
        "sub-threshold drift is not a capital event"
    );

    // Deposit lands: collateral 953 -> 1003 (+$50).
    h.engine.instances[0].equity_cache = 1_003.0;
    h.engine.detect_capital_event_and_rebaseline(0);

    let inst = &h.engine.instances[0];
    assert_eq!(
        inst.equity_samples.len(),
        1,
        "the rolling peak collapses to a single current-equity sample"
    );
    assert!(
        (inst.equity_samples[0].equity - 1_003.0).abs() < 1e-9,
        "peak rebaselined to the topped-up equity"
    );
    // session_start_equity shifted by the full deposit delta (+$50 from the
    // 953 baseline). realized_pnl_today is untouched by a capital event.
    assert!(
        (inst.session_start_equity - 1_050.0).abs() < 1e-9,
        "daily-DD denominator shifts by the deposit delta"
    );
    assert!(
        inst.session_halted,
        "① restores headroom but never auto-resumes — the operator still acks"
    );
    let (peak, dd) = PairTradeEngine::rolling_peak(&inst.equity_samples, 1_003.0).unwrap();
    assert!((peak - 1_003.0).abs() < 1e-9);
    assert_eq!(dd, 0.0, "DD is reset to 0 at the new base");
}

// bot-strategy#783: Round 9 repeatedly observed a close PnL/funding movement
// in account equity only after pairtrade had already recorded the realized
// accounting. Those delayed balance updates must advance the paired baseline
// without ever collapsing the pre-existing DD peak. A later genuine transfer
// with no accounting movement must retain the automatic #575 rebaseline.
#[test]
fn round9_delayed_settlement_sequence_never_reanchors_without_transfer() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-round9-settlement");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;

    let now = chrono::Utc::now().timestamp();
    let mut settled_equity = 6_000.0;
    let mut total_trade_pnl = 0.0;
    let mut total_funding = 0.0;
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_reference_usd = 6_000.0;
        inst.session_equity_reference_usd = 6_000.0;
        inst.equity_cache = settled_equity;
        inst.equity_samples = vec![EquitySample {
            ts: now - 100,
            equity: 6_088.33,
        }];
        inst.capital_baseline_equity = settled_equity;
        inst.capital_baseline_accounted_pnl = Some(0.0);
        inst.session_start_equity = 6_000.0;
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    // The five false A-arm deltas recorded in Round 9. Split a small part of
    // each movement into funding so both accounting components are covered.
    let movements = [
        (6.50, 0.1256),
        (17.50, 0.0557),
        (-14.00, 0.0221),
        (9.60, 0.0866),
        (-7.10, 0.0252),
    ];
    for (trade_pnl, funding) in movements {
        let movement = trade_pnl + funding;
        total_trade_pnl += trade_pnl;
        total_funding += funding;
        {
            let inst = &mut h.engine.instances[0];
            inst.total_pnl = total_trade_pnl;
            inst.total_funding_carry = total_funding;
            // Connector/account cache has not reflected the close yet.
            inst.equity_cache = settled_equity;
        }
        h.engine.detect_capital_event_and_rebaseline(0);
        {
            let inst = &h.engine.instances[0];
            assert!(
                inst.capital_rebaseline_deferred,
                "material accounted PnL with stale equity must be deferred"
            );
            assert_eq!(inst.equity_samples.len(), 1);
            assert!((inst.equity_samples[0].equity - 6_088.33).abs() < 1e-9);
            assert!((inst.session_start_equity - 6_000.0).abs() < 1e-9);
        }

        // The exchange balance catches up. Raw equity and accounted PnL now
        // agree, so the baseline advances without DD reanchor.
        settled_equity += movement;
        h.engine.instances[0].equity_cache = settled_equity;
        h.engine.detect_capital_event_and_rebaseline(0);
        let inst = &h.engine.instances[0];
        assert!(!inst.capital_rebaseline_deferred);
        assert!((inst.capital_baseline_equity - settled_equity).abs() < 1e-9);
        assert_eq!(inst.equity_samples.len(), 1);
        assert!((inst.equity_samples[0].equity - 6_088.33).abs() < 1e-9);
        assert!((inst.session_start_equity - 6_000.0).abs() < 1e-9);
    }

    // A genuine $500 transfer after accounting has reconciled remains a
    // verified capital event and preserves the original #575 behavior.
    h.engine.instances[0].equity_cache = settled_equity + 500.0;
    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(inst.equity_samples.len(), 1);
    assert!((inst.equity_samples[0].equity - (settled_equity + 500.0)).abs() < 1e-9);
    assert!((inst.session_start_equity - 6_500.0).abs() < 1e-9);
}

// A pre-#783 snapshot has no accounted-PnL half for its persisted equity
// baseline. Upgrade migration must therefore establish a guarded candidate,
// not infer capital. If a delayed close settlement lands after migration, the
// candidate follows it and clears only after a stable flat observation.
#[test]
fn pre_783_snapshot_migration_never_reanchors_delayed_settlement() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-pre-783-migration");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;

    let now = chrono::Utc::now().timestamp();
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_reference_usd = 6_000.0;
        inst.session_equity_reference_usd = 6_000.0;
        inst.session_start_equity = 6_000.0;
        inst.equity_cache = 6_000.0;
        inst.equity_samples = vec![EquitySample {
            ts: now - 100,
            equity: 6_088.33,
        }];
        inst.capital_baseline_equity = 6_000.0;
        inst.capital_baseline_accounted_pnl = None;
        inst.total_pnl = 10.0;
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    {
        let inst = &h.engine.instances[0];
        assert_eq!(inst.capital_baseline_accounted_pnl, Some(10.0));
        assert!(inst.capital_position_seen_since_baseline);
        assert_eq!(inst.equity_samples[0].equity, 6_088.33);
        assert_eq!(inst.session_start_equity, 6_000.0);
    }

    // The exchange/account cache then reflects the already-accounted close.
    h.engine.instances[0].equity_cache = 6_010.0;
    h.engine.detect_capital_event_and_rebaseline(0);
    {
        let inst = &h.engine.instances[0];
        assert!(inst.capital_rebaseline_deferred);
        assert!(inst.capital_position_seen_since_baseline);
        assert_eq!(inst.capital_baseline_equity, 6_010.0);
        assert_eq!(inst.equity_samples[0].equity, 6_088.33);
        assert_eq!(inst.session_start_equity, 6_000.0);
    }

    // A second stable observation clears the guard without touching DD.
    h.engine.detect_capital_event_and_rebaseline(0);
    {
        let inst = &h.engine.instances[0];
        assert!(!inst.capital_rebaseline_deferred);
        assert!(!inst.capital_position_seen_since_baseline);
        assert_eq!(inst.equity_samples[0].equity, 6_088.33);
        assert_eq!(inst.session_start_equity, 6_000.0);
    }

    // A later clean transfer remains eligible for the normal #575 reanchor.
    h.engine.instances[0].equity_cache = 6_510.0;
    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(inst.equity_samples[0].equity, 6_510.0);
    assert_eq!(inst.session_start_equity, 6_500.0);
}

// bot-strategy#752: a full withdrawal can include accumulated PnL, so its
// delta need not equal the configured reference. The zero-clamped daily
// denominator must survive UTC rollover; otherwise the old reference is
// resurrected and the later redeposit is added on top of it.
#[test]
fn withdrawal_rollover_redeposit_counts_new_capital_once() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-rollover");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;

    let now = chrono::Utc::now().timestamp();
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        // $53.27 of accumulated PnL makes the withdrawal deliberately
        // non-round relative to the $1,000 configured denominator.
        inst.equity_cache = 1_053.27;
        inst.equity_samples = vec![EquitySample {
            ts: now - 100,
            equity: 1_053.27,
        }];
        inst.capital_baseline_equity = 1_053.27;
        inst.session_start_equity = 1_000.0;
        inst.session_equity_reference_usd = 1_000.0;
        inst.realized_pnl_today = 53.27;
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    // Withdraw everything except exchange dust. The accumulated PnL makes
    // delta=-$1,053.26, so the denominator clamps at zero.
    h.engine.instances[0].equity_cache = 0.01;
    h.engine.detect_capital_event_and_rebaseline(0);
    assert_eq!(h.engine.instances[0].session_start_equity, 0.0);
    assert!((h.engine.instances[0].realized_pnl_today - 53.27).abs() < 1e-9);

    // Cross a real UTC session bucket. Rollover clears daily PnL but must
    // preserve the capital-adjusted zero denominator.
    h.engine.instances[0].session_start_ts = now - 86_400;
    h.engine.refresh_daily_session();
    assert_eq!(h.engine.instances[0].session_start_equity, 0.0);
    assert_eq!(h.engine.instances[0].realized_pnl_today, 0.0);

    // A loss booked after rollover remains intact across the capital event.
    h.engine.instances[0].realized_pnl_today = -7.25;
    h.engine.instances[0].equity_cache = 6_000.01;
    h.engine.detect_capital_event_and_rebaseline(0);

    let inst = &h.engine.instances[0];
    assert!(
        (inst.session_start_equity - 6_000.0).abs() < 1e-9,
        "the $6,000 redeposit is counted exactly once after rollover"
    );
    assert!(
        (inst.realized_pnl_today - (-7.25)).abs() < 1e-9,
        "a capital event never erases current-session realized PnL"
    );
    assert!((inst.capital_baseline_equity - 6_000.01).abs() < 1e-9);
}

// bot-strategy#783 Codex P2 follow-up: a real transfer landing exactly when
// a position closes with material PnL can never satisfy baseline_advanced
// (it requires the accounted delta itself to be sub-threshold, which a
// genuine material close PnL never is). Left unresolved, the account would
// stay deferred forever, blocking detection of every later, unrelated
// capital event too -- not just this one.
#[test]
fn ambiguous_deferral_gives_up_after_the_giveup_window_and_stays_detectable() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-giveup");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;

    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_cache = 1_000.0;
        inst.capital_baseline_equity = 1_000.0;
        inst.capital_baseline_accounted_pnl = Some(0.0);
        inst.session_start_equity = 1_000.0;
        inst.session_equity_reference_usd = 1_000.0;
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    // A $10 close settles at the same instant a $100 deposit lands: equity
    // moves to $1,110 while accounted PnL moves by $10. Neither
    // Reconciled nor Verified applies (accounted_pnl_delta exceeds min_usd
    // on its own), so this is Ambiguous and cannot naturally clear.
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_cache = 1_110.0;
        inst.total_pnl = 10.0;
    }
    h.engine.detect_capital_event_and_rebaseline(0);
    assert!(
        h.engine.instances[0].capital_rebaseline_deferred,
        "an unresolvable ambiguity defers rather than guessing"
    );
    assert!(h.engine.instances[0].capital_rebaseline_deferred_since.is_some());

    // Simulate the give-up window having elapsed without needing to
    // actually sleep: back-date the deferred-since clock.
    h.engine.instances[0].capital_rebaseline_deferred_since = Some(
        Instant::now() - Duration::from_secs(engine::risk::CAPITAL_REBASELINE_GIVEUP_SECS + 1),
    );
    h.engine.detect_capital_event_and_rebaseline(0);

    let inst = &h.engine.instances[0];
    assert!(
        !inst.capital_rebaseline_deferred,
        "the deferral gives up after the timeout instead of staying stuck forever"
    );
    assert!(inst.capital_rebaseline_deferred_since.is_none());
    assert!(
        (inst.capital_baseline_equity - 1_110.0).abs() < 1e-9,
        "the anchor force-advances to the current reading"
    );

    // A later, genuinely clean $100 deposit must now be detectable again --
    // the whole point of giving up instead of staying stuck.
    h.engine.instances[0].equity_cache = 1_210.0;
    h.engine.detect_capital_event_and_rebaseline(0);
    assert!(
        (h.engine.instances[0].session_start_equity - 1_100.0).abs() < 1e-9,
        "capital-event detection recovered after the give-up and caught the later deposit"
    );
}

// bot-strategy#783 (Codex P1 follow-up, third round): a position closing
// (or a pre-#783 migration, which seeds the guard unconditionally) does not
// guarantee equity_cache has caught up yet -- it only refreshes every
// EQUITY_REFRESH_CACHE_SECS. A quiet first observation right after the
// guard latches must not clear it before that window has had a chance to
// elapse, or a later, unrelated capital event landing once the cache
// finally refreshes gets misclassified.
#[test]
fn position_guard_survives_a_quiet_tick_until_the_equity_cache_lag_window_elapses() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-guard-timing");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;

    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        // Equity and accounted PnL already match the anchor exactly --
        // nothing to reconcile on its own -- but the guard is latched true,
        // as if a position just closed (or a pre-#783 migration just ran).
        inst.equity_cache = 1_000.0;
        inst.capital_baseline_equity = 1_000.0;
        inst.capital_baseline_accounted_pnl = Some(0.0);
        inst.capital_position_seen_since_baseline = true;
        // Just became flat: settle_secs=0 makes this tick eligible to
        // reconcile immediately, but the guard's own timing gate is
        // independent of settle_secs.
        inst.flat_since = Some(Instant::now());
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    assert!(
        h.engine.instances[0].capital_position_seen_since_baseline,
        "a quiet reading right after the guard latches must not clear it yet"
    );

    // Back-date flat_since past the equity-cache lag window without
    // actually sleeping.
    h.engine.instances[0].flat_since =
        Some(Instant::now() - Duration::from_secs(EQUITY_REFRESH_CACHE_SECS + 1));
    h.engine.detect_capital_event_and_rebaseline(0);
    assert!(
        !h.engine.instances[0].capital_position_seen_since_baseline,
        "the guard clears once the equity-cache lag window has elapsed"
    );
}

// bot-strategy#783 Codex P1 follow-up: the false-to-true
// capital_position_seen_since_baseline transition must be persisted so a
// startup force-close (which can flatten a still-open position without
// recording realized PnL) does not restore a stale `false` and get its
// resulting equity settlement misclassified as a verified deposit/
// withdrawal. But only that transition -- not every tick a position stays
// open (Codex P2 follow-up: constant per-tick rewrites while a position is
// simply held would defeat the sampling policy's deliberate avoidance of
// per-tick disk writes).
#[test]
fn position_activity_latch_persists_only_on_the_false_to_true_transition() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-latch");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;

    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_cache = 1_000.0;
        inst.capital_baseline_equity = 1_000.0;
        inst.capital_baseline_accounted_pnl = Some(0.0);
    }
    h.seed_aged_position(0);
    assert!(
        !h.engine.risk_state_path.exists(),
        "no risk state written before the first capital-event tick"
    );

    h.engine.detect_capital_event_and_rebaseline(0);
    let persisted = risk_io::load_risk_state(&h.engine.risk_state_path);
    let persisted_inst = persisted
        .instances
        .get("hg-cap-latch")
        .expect("the false-to-true latch transition is persisted");
    assert!(persisted_inst.capital_position_seen_since_baseline);

    // Still in the same position on the next tick: the latch is already
    // true, so nothing changed and this tick must not rewrite the file.
    std::fs::remove_file(&h.engine.risk_state_path).unwrap();
    h.engine.detect_capital_event_and_rebaseline(0);
    assert!(
        !h.engine.risk_state_path.exists(),
        "no redundant persist while the latch is already true and the position is still open"
    );
}

// bot-strategy#783 Codex P2 follow-up: a settled, flat account whose equity
// and accounting have not moved since the last reconciliation must not
// rewrite risk_state.json on every tick -- only a real change to either half
// of the paired baseline (or a latch) is worth the write.
#[test]
fn reconciled_quiet_tick_does_not_persist_when_nothing_changed() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-idle");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;

    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        // A real, one-time trade settlement: equity and accounted PnL move
        // together by the same $10, so this reconciles cleanly and the
        // paired baseline genuinely advances -- the first tick must persist.
        inst.equity_cache = 1_010.0;
        inst.total_pnl = 10.0;
        inst.capital_baseline_equity = 1_000.0;
        inst.capital_baseline_accounted_pnl = Some(0.0);
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }
    h.engine.detect_capital_event_and_rebaseline(0);
    assert!(
        h.engine.risk_state_path.exists(),
        "a genuine baseline advance is persisted"
    );

    // Nothing moved since: same cached equity, same accounted PnL, already
    // reconciled. Must not rewrite the file.
    std::fs::remove_file(&h.engine.risk_state_path).unwrap();
    h.engine.detect_capital_event_and_rebaseline(0);
    assert!(
        !h.engine.risk_state_path.exists(),
        "an idle, already-reconciled tick must not rewrite risk_state.json"
    );
}

// A reference change at the restart boundary stays pending until the same
// flat/settled observation used for capital detection. When a matching
// transfer is present, the observed delta wins and the new reference is only
// recorded — it is not independently added to the denominator.
#[test]
fn reference_change_and_redeposit_at_restart_reconcile_once() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut before = Harness::new("hg-cap-ref-restart");
    before.engine.cfg.risk.max_session_loss_bps = 500;
    before.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    before.engine.cfg.dry_run = false;
    before.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    let path = before.engine.risk_state_path.clone();
    let now = chrono::Utc::now().timestamp();
    {
        let inst = &mut before.engine.instances[0];
        inst.equity_reference_usd = 2_000.0;
        inst.session_equity_reference_usd = 2_000.0;
        inst.session_start_equity = 0.0;
        inst.session_start_ts = now;
        inst.realized_pnl_today = -12.5;
        inst.capital_baseline_equity = 0.01;
        inst.equity_samples = vec![EquitySample {
            ts: now - 100,
            equity: 0.01,
        }];
    }
    before.engine.persist_risk_state();

    let mut restarted = Harness::new("hg-cap-ref-restart");
    restarted.engine.risk_state_path = path;
    restarted.engine.cfg.risk.max_session_loss_bps = 500;
    restarted.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    restarted.engine.cfg.dry_run = false;
    restarted.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    restarted.engine.instances[0].equity_reference_usd = 6_000.0;
    restarted.engine.load_risk_state();

    // Loading a new config does not inject $6,000 before the actual capital
    // observation, and the old reference remains as a persisted pending mark.
    assert_eq!(restarted.engine.instances[0].session_start_equity, 0.0);
    assert_eq!(
        restarted.engine.instances[0].session_equity_reference_usd,
        2_000.0
    );

    {
        let inst = &mut restarted.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_cache = 6_000.01;
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }
    restarted.engine.detect_capital_event_and_rebaseline(0);

    let inst = &restarted.engine.instances[0];
    assert!((inst.session_start_equity - 6_000.0).abs() < 1e-9);
    assert_eq!(inst.session_equity_reference_usd, 6_000.0);
    assert!((inst.realized_pnl_today - (-12.5)).abs() < 1e-9);
    assert_eq!(inst.equity_samples.len(), 1);
    assert!((inst.equity_samples[0].equity - 6_000.01).abs() < 1e-9);
}

// PR #175 review / bot-strategy#752: the first binary carrying #752 may load
// a legacy snapshot that has no `session_equity_reference_usd`. It must keep
// that absence as migration-pending until a safe live observation instead of
// stamping the current config and preserving a stale $8,000 denominator.
#[test]
fn legacy_snapshot_stale_denominator_reconciles_when_flat_settled() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-legacy");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    h.engine.instances[0].equity_reference_usd = 6_000.0;
    let now = chrono::Utc::now().timestamp();
    let legacy = serde_json::json!({
        "_v": 2,
        "instances": {
            "hg-cap-legacy": {
                "session_start_equity": 8_000.0,
                "session_start_ts": now,
                "realized_pnl_today": -12.5,
                "capital_baseline_equity": 6_000.01,
                "equity_samples": [{ "ts": now - 100, "equity": 6_000.01 }]
            }
        }
    });
    std::fs::write(
        &h.engine.risk_state_path,
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();

    h.engine.load_risk_state();
    assert_eq!(h.engine.instances[0].session_start_equity, 8_000.0);
    assert_eq!(
        h.engine.instances[0].session_equity_reference_usd, 0.0,
        "missing legacy field stays pending after load"
    );

    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_cache = 6_000.01;
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }
    h.engine.detect_capital_event_and_rebaseline(0);

    let inst = &h.engine.instances[0];
    assert_eq!(inst.session_start_equity, 6_000.0);
    assert_eq!(inst.session_equity_reference_usd, 6_000.0);
    assert_eq!(inst.realized_pnl_today, -12.5);
    let persisted = risk_io::load_risk_state(&h.engine.risk_state_path);
    let persisted_inst = persisted
        .instances
        .get("hg-cap-legacy")
        .expect("legacy instance persisted after reconciliation");
    assert_eq!(persisted_inst.session_start_equity, 6_000.0);
    assert_eq!(persisted_inst.session_equity_reference_usd, 6_000.0);
}

// Codex review on PR #175 / bot-strategy#752: a legacy snapshot whose
// persisted session_start_equity already lines up with the tracked capital
// baseline (e.g. both near-zero after a same-session withdrawal, with no new
// transfer since) must NOT be reset to the fresh configured reference. Doing
// so would resurrect the withdrawn capital, and a later redeposit would then
// double-count on top of it.
#[test]
fn legacy_snapshot_consistent_denominator_preserved_when_flat_settled() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-legacy-withdrawn");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_reference_usd = 6_000.0;
        // Old code already zeroed the denominator when the withdrawal
        // happened; capital_baseline_equity (tracked since #575, unaffected
        // by the #752 rollover bug) confirms it — both sit near zero.
        inst.equity_cache = 0.01;
        inst.capital_baseline_equity = 0.01;
        inst.session_start_equity = 0.0;
        inst.session_equity_reference_usd = 0.0; // pre-#752 snapshot: legacy
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(
        inst.session_start_equity, 0.0,
        "consistent legacy denominator must be preserved, not resurrected to the full reference"
    );
    assert_eq!(
        inst.session_equity_reference_usd, 6_000.0,
        "migration still stamps the reference so future ticks are treated as reconciled"
    );
}

// Codex review on PR #175 / bot-strategy#752: a legacy snapshot whose
// persisted session_start_equity already tracked the capital baseline before
// this tick (the ordinary pre-#752 case) must still have a genuinely
// detected delta applied on top of it, not discarded. Scenario: both were
// $1,000 before restart, $500 was withdrawn while the bot was stopped, so
// the first flat/settled tick observes equity=$500 against baseline=$1,000.
#[test]
fn legacy_snapshot_trustworthy_denominator_applies_detected_delta() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-legacy-delta-trustworthy");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_reference_usd = 1_000.0;
        inst.session_equity_reference_usd = 0.0; // pre-#752 snapshot: legacy
        inst.capital_baseline_equity = 1_000.0;
        inst.session_start_equity = 1_000.0; // consistent with baseline
        inst.equity_cache = 500.0; // $500 withdrawn while stopped
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(
        inst.session_start_equity, 500.0,
        "a real detected withdrawal must be applied, not discarded, on a trustworthy legacy denominator"
    );
    assert_eq!(inst.session_equity_reference_usd, 1_000.0);
}

// Companion case: the persisted denominator is untrustworthy (diverges from
// the tracked capital baseline by more than min_usd — the #752 rollover-drift
// shape), so the old discard-and-replace behavior is kept: there is no way to
// tell a real capital delta apart from more #752 drift noise, so the delta is
// not applied and the fresh configured reference is adopted directly.
#[test]
fn legacy_snapshot_untrustworthy_denominator_discards_delta() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-legacy-delta-untrustworthy");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_reference_usd = 1_000.0;
        inst.session_equity_reference_usd = 0.0; // pre-#752 snapshot: legacy
        inst.capital_baseline_equity = 5_000.0;
        inst.session_start_equity = 8_000.0; // diverges from baseline: #752 drift
        inst.equity_cache = 4_500.0; // $500 withdrawn since the tracked baseline
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(
        inst.session_start_equity, 1_000.0,
        "an untrustworthy legacy denominator keeps the discard-and-replace fallback"
    );
    assert_eq!(inst.session_equity_reference_usd, 1_000.0);
}

// Codex review on PR #175 / bot-strategy#752: `capital_baseline_equity` keeps
// accruing realized trading PnL between capital events (each closed trade
// reseeds it to the post-trade settled equity), while `session_start_equity`
// deliberately excludes that PnL. A legitimate deposit followed by ordinary
// profit must not look like #752 rollover drift and get discarded: $1,000
// config + $500 deposit = $1,500 denominator, then $100 of realized profit
// (tracked in `total_pnl`) pushes the settled baseline to $1,600. Comparing
// the denominator to the raw baseline would see a $100 gap (over the $5
// threshold) and wrongly reset to $1,000; backing `total_pnl` out of the
// baseline first must recover the $1,500 comparison and preserve the deposit.
#[test]
fn legacy_snapshot_preserved_despite_realized_pnl_baseline_drift() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-legacy-pnl-drift");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_reference_usd = 1_000.0;
        inst.session_equity_reference_usd = 0.0; // pre-#752 snapshot: legacy
        inst.session_start_equity = 1_500.0; // config $1,000 + legitimate $500 deposit
        inst.total_pnl = 100.0; // realized profit since the deposit
        inst.capital_baseline_equity = 1_600.0; // $1,500 basis + $100 realized profit
        inst.capital_baseline_accounted_pnl = Some(100.0);
        inst.equity_cache = 1_600.0; // stable — no new capital event this tick
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(
        inst.session_start_equity, 1_500.0,
        "realized PnL drift in the baseline must not be mistaken for #752 rollover drift"
    );
    assert_eq!(inst.session_equity_reference_usd, 1_000.0);
}

// Companion to the above for the same-tick delta path: the persisted
// denominator ($1,500 = $1,000 config + $500 deposit) still matches the
// baseline once $100 of realized profit is backed out, so a genuinely
// detected $500 withdrawal (equity now $1,100 against a $1,600 baseline)
// must be applied on top of it rather than discarded.
#[test]
fn legacy_snapshot_trustworthy_denominator_applies_delta_despite_realized_pnl() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-legacy-delta-pnl-drift");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_reference_usd = 1_000.0;
        inst.session_equity_reference_usd = 0.0; // pre-#752 snapshot: legacy
        inst.session_start_equity = 1_500.0; // config $1,000 + legitimate $500 deposit
        inst.total_pnl = 100.0; // realized profit since the deposit
        inst.capital_baseline_equity = 1_600.0; // $1,500 basis + $100 realized profit
        inst.capital_baseline_accounted_pnl = Some(100.0);
        inst.equity_cache = 1_100.0; // $500 withdrawn while stopped
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(
        inst.session_start_equity, 1_000.0,
        "a real withdrawal must apply on top of a denominator that only looks stale due to realized PnL"
    );
    assert_eq!(inst.session_equity_reference_usd, 1_000.0);
}

// Codex review on PR #175 / bot-strategy#752: InstanceRiskState::reset_round_bound
// zeroes capital_baseline_equity on a round transition while deliberately
// preserving session_start_equity (bot-strategy#354 — session-rolling fields
// have their own lifecycle, separate from round-bound ones). A legacy
// snapshot carrying a real deposit adjustment can therefore land on the
// baseline<=0 "first settled reading" branch with no baseline to validate
// against; it must not be treated differently from the baseline-available
// branches above and overwritten with the fresh configured reference.
#[test]
fn legacy_snapshot_with_cleared_baseline_preserves_denominator_on_round_transition() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-legacy-cleared-baseline");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_reference_usd = 1_000.0;
        inst.session_equity_reference_usd = 0.0; // pre-#752 snapshot: legacy
        inst.session_start_equity = 1_500.0; // config $1,000 + legitimate $500 deposit
                                             // reset_round_bound() already zeroed this on the round transition;
                                             // session_start_equity survived it untouched.
        inst.capital_baseline_equity = 0.0;
        inst.equity_cache = 1_500.0;
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(
        inst.session_start_equity, 1_500.0,
        "a cleared baseline gives no signal to distrust a legacy denominator by"
    );
    assert_eq!(inst.session_equity_reference_usd, 1_000.0);
    assert_eq!(inst.capital_baseline_equity, 1_500.0);
}

// Reference migration is needed for daily DD even when rolling session DD is
// disabled. A simultaneous deposit still establishes the current reference,
// but the otherwise-inert rolling-peak samples must not be rewritten.
#[test]
fn legacy_reference_reconciles_without_reanchoring_disabled_session_dd() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-legacy-disabled");
    h.engine.cfg.risk.max_session_loss_bps = 0;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    let now = chrono::Utc::now().timestamp();
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_reference_usd = 6_000.0;
        inst.equity_cache = 6_000.01;
        inst.capital_baseline_equity = 0.01;
        inst.session_start_equity = 2_000.0;
        inst.session_equity_reference_usd = 0.0;
        inst.realized_pnl_today = -3.0;
        inst.equity_samples = vec![EquitySample {
            ts: now - 100,
            equity: 9_000.0,
        }];
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(inst.session_start_equity, 6_000.0);
    assert_eq!(inst.session_equity_reference_usd, 6_000.0);
    assert_eq!(inst.realized_pnl_today, -3.0);
    assert_eq!(inst.equity_samples.len(), 1);
    assert_eq!(
        inst.equity_samples[0].equity, 9_000.0,
        "disabled rolling session DD keeps its inert sample history untouched"
    );
}

// PR #175 review: `max_session_loss_bps` and `max_daily_loss_bps` are
// independent knobs, but detect_capital_event_and_rebaseline used to gate
// all capital tracking on max_session_loss_bps alone. With rolling session
// DD disabled but daily DD still enabled, a genuine deposit must still
// update session_start_equity — otherwise the daily-DD denominator sticks
// at whatever it was when session DD was turned off.
#[test]
fn capital_delta_updates_daily_dd_denominator_when_session_dd_disabled() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-daily-only");
    h.engine.cfg.risk.max_session_loss_bps = 0;
    h.engine.cfg.risk.max_daily_loss_bps = 300;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_reference_usd = 1_000.0;
        inst.session_equity_reference_usd = 1_000.0; // already reconciled, not legacy
        inst.equity_cache = 2_000.0;
        inst.capital_baseline_equity = 1_000.0;
        inst.session_start_equity = 1_000.0;
        inst.realized_pnl_today = -5.0;
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(
        inst.session_start_equity, 2_000.0,
        "daily-DD denominator must track a deposit even with rolling session DD disabled"
    );
    assert_eq!(inst.capital_baseline_equity, 2_000.0);
    assert_eq!(inst.realized_pnl_today, -5.0);
}

// If only the config reference changes and collateral is stable, the next
// flat/settled observation adopts the new configured denominator. This makes
// the no-transfer side of restart reconciliation explicit.
#[test]
fn reference_change_without_capital_event_adopts_new_reference_when_settled() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-ref-only");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_cache = 1_003.0;
        inst.capital_baseline_equity = 1_003.0;
        inst.session_start_equity = 1_000.0;
        inst.session_equity_reference_usd = 1_000.0;
        inst.equity_reference_usd = 2_000.0;
        inst.realized_pnl_today = -4.0;
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }

    h.engine.detect_capital_event_and_rebaseline(0);
    let inst = &h.engine.instances[0];
    assert_eq!(inst.session_start_equity, 2_000.0);
    assert_eq!(inst.session_equity_reference_usd, 2_000.0);
    assert_eq!(inst.realized_pnl_today, -4.0);
}

// A trading loss (equity moving while a position is OPEN) must never be
// mistaken for a withdrawal: detection only runs when flat.
#[test]
fn unrealized_pnl_while_in_position_does_not_rebaseline() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-cap-trading");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    h.engine.cfg.risk.session_dd_capital_event_min_usd = 5.0;
    h.engine.cfg.dry_run = false;
    h.engine.cfg.risk.session_dd_capital_settle_secs = 0;

    let now = chrono::Utc::now().timestamp();
    {
        let inst = &mut h.engine.instances[0];
        inst.equity_initialized = true;
        inst.equity_cache = 1_000.0;
        inst.equity_samples = vec![EquitySample {
            ts: now - 100,
            equity: 1_000.0,
        }];
        inst.capital_baseline_equity = 1_000.0;
        inst.session_start_equity = 1_000.0;
        inst.flat_since = Some(Instant::now() - Duration::from_secs(120));
    }
    // Open a position → not flat.
    h.seed_aged_position(0);
    // A big adverse mark: equity drops $50 (unrealized), NOT a capital event.
    h.engine.instances[0].equity_cache = 950.0;
    h.engine.detect_capital_event_and_rebaseline(0);

    let inst = &h.engine.instances[0];
    assert!(
        (inst.equity_samples[0].equity - 1_000.0).abs() < 1e-9,
        "an unrealized trading loss while in a position must not rebaseline the peak"
    );
    assert!(
        (inst.session_start_equity - 1_000.0).abs() < 1e-9,
        "the daily-DD denominator is untouched by trading PnL"
    );
    assert!(
        inst.flat_since.is_none(),
        "detection disarms while a position is open"
    );
}

// ---------------------------------------------------------------------
// Scenario 8 (bot-strategy#575 ②): a RISK_ACK payload carrying
// `reanchor=true` clears the halt AND collapses the rolling peak to current
// equity, so the ack does not re-breach at the boundary. A plain ack clears
// the halt but leaves the peak intact (today's behaviour).
// ---------------------------------------------------------------------
#[test]
fn risk_ack_reanchor_clears_halt_and_resets_peak() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-ack-reanchor");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    let now = chrono::Utc::now().timestamp();
    {
        let inst = &mut h.engine.instances[0];
        inst.session_halted = true;
        inst.session_halt_reason = Some("session_dd_500bps_lev1.0".to_string());
        inst.session_halt_ts = Some(now);
        inst.equity_initialized = true;
        inst.equity_cache = 950.0;
        inst.equity_samples = vec![EquitySample {
            ts: now - 100,
            equity: 1_003.0,
        }];
    }

    std::fs::write(risk_ack_path(), "ack by op: reanchor=true").unwrap();
    h.engine.consume_risk_ack();

    let inst = &h.engine.instances[0];
    assert!(!inst.session_halted, "ack clears the halt");
    assert!(inst.session_halt_reason.is_none());
    assert_eq!(
        inst.equity_samples.len(),
        1,
        "reanchor collapses the rolling-peak window"
    );
    assert!(
        (inst.equity_samples[0].equity - 950.0).abs() < 1e-9,
        "peak reanchored to current equity"
    );
    let (_, dd) = PairTradeEngine::rolling_peak(&inst.equity_samples, 950.0).unwrap();
    assert_eq!(
        dd, 0.0,
        "no residual DD → the ack will not re-breach at the boundary"
    );
    assert!(
        !Path::new(risk_ack_path()).exists(),
        "the ack sentinel is consumed"
    );
}

#[test]
fn risk_ack_without_reanchor_leaves_peak_intact() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-ack-plain");
    h.engine.cfg.risk.max_session_loss_bps = 500;
    let now = chrono::Utc::now().timestamp();
    {
        let inst = &mut h.engine.instances[0];
        inst.session_halted = true;
        inst.session_halt_reason = Some("session_dd_500bps_lev1.0".to_string());
        inst.equity_initialized = true;
        inst.equity_cache = 950.0;
        inst.equity_samples = vec![EquitySample {
            ts: now - 100,
            equity: 1_003.0,
        }];
    }

    std::fs::write(risk_ack_path(), "ack: manual review complete").unwrap();
    h.engine.consume_risk_ack();

    let inst = &h.engine.instances[0];
    assert!(!inst.session_halted, "plain ack still clears the halt");
    assert_eq!(
        inst.equity_samples.len(),
        1,
        "plain ack must NOT collapse the peak window"
    );
    assert!(
        (inst.equity_samples[0].equity - 1_003.0).abs() < 1e-9,
        "the inflated peak survives a plain ack (today's behaviour preserved)"
    );
}

// ---------------------------------------------------------------------
// Scenario 5 (bot-strategy#531): ineligible-close book-quality guard.
// When a held pair loses eligibility, the flatten normally fires on the
// same tick. With INELIGIBLE_CLOSE_DEFER_CAP_SECS > 0, a degraded book
// (leg spread above the guard threshold, or a stale engine view) defers
// the close — re-checked every tick — until the book recovers or the
// deferral cap runs out. The guard must never *prevent* the close, only
// re-time it; disabled (cap 0, the default) must preserve today's
// immediate-close behaviour bit for bit.
// ---------------------------------------------------------------------

fn defer_count(instance_id: &str, reason: &str) -> u64 {
    prom::INELIGIBLE_CLOSE_DEFER_TOTAL
        .with_label_values(&[instance_id, PAIR_KEY, reason])
        .get()
}

/// Shared setup: engine with the guard enabled, a fresh held position
/// (age 5s, far below force_close_secs=60), the pair forced ineligible,
/// and a 40 bps book (above the 20 bps guard threshold, below the 200 bps
/// tick-filter reject so the polled snapshot still reaches the planner).
fn ineligible_guard_harness(instance_id: &str) -> Harness {
    let mut h = Harness::new(instance_id);
    h.engine.cfg.ineligible_close_defer_cap_secs = 300;
    h.seed_position(0, 5);
    h._connector.set_half_spread_frac(dec("0.002"));
    h.engine.per_pair_state.get_mut(PAIR_KEY).unwrap().eligible = false;
    h
}

#[tokio::test]
async fn ineligible_close_deferred_on_degraded_book_then_fires_on_recovery() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = ineligible_guard_harness("hg-inelig-defer");
    let deferred_before = defer_count("hg-inelig-defer", "spread");

    h.step().await;
    assert!(
        h.position(0).is_some(),
        "ineligible close must be deferred while the book is degraded"
    );
    assert_eq!(
        defer_count("hg-inelig-defer", "spread"),
        deferred_before + 1,
        "the deferral must be attributed to the spread check"
    );
    assert!(
        h.engine.instances[0].states[PAIR_KEY]
            .ineligible_defer_since_ts
            .is_some(),
        "the deferral window must have started"
    );

    // Book recovers to a ±5 bps shape (clearly below the 20 bps guard
    // threshold — the historical ±10 bps default lands exactly ON the
    // threshold, where f64 rounding makes the comparison unstable) → the
    // very next tick must flatten (pair is still ineligible).
    h._connector.set_half_spread_frac(dec("0.0005"));
    h.step().await;
    assert!(
        h.position(0).is_none(),
        "close must fire as soon as the book recovers"
    );
    assert!(
        h.engine.instances[0].states[PAIR_KEY]
            .ineligible_defer_since_ts
            .is_none(),
        "firing the close must clear the deferral window"
    );
}

#[tokio::test]
async fn ineligible_close_fires_unconditionally_once_cap_exceeded() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = ineligible_guard_harness("hg-inelig-cap");

    h.step().await;
    assert!(
        h.position(0).is_some(),
        "first tick defers while the book is degraded"
    );

    // Age the deferral start past the 300s cap; the book stays degraded.
    let now_ts = chrono::Utc::now().timestamp();
    h.engine.instances[0]
        .states
        .get_mut(PAIR_KEY)
        .unwrap()
        .ineligible_defer_since_ts = Some(now_ts - 301);
    let cap_before = defer_count("hg-inelig-cap", "cap_exceeded");

    h.step().await;
    assert!(
        h.position(0).is_none(),
        "cap exceeded must close even into the still-degraded book"
    );
    assert_eq!(
        defer_count("hg-inelig-cap", "cap_exceeded"),
        cap_before + 1,
        "the forced fire must be attributed to cap_exceeded"
    );
}

/// The 06-10 rejection-storm shape (Codex review on PR #166): while the
/// tick filter rejects everything, no bar updates and the planner never
/// reaches the pair; when the first valid tick lands, the snapshot is
/// fresh and sane — the guard must still defer via the accepted-feed gap
/// recovery holddown instead of firing into the just-recovered book.
#[tokio::test]
async fn ineligible_close_deferred_through_gap_recovery_holddown() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = ineligible_guard_harness("hg-inelig-gap");
    // Tight book: the spread signal must NOT be the one deferring.
    h._connector.set_half_spread_frac(dec("0.0005"));
    // Simulate "no accepted tick for 100s" (the storm): the next step's
    // accepted tick then ends the gap and must arm the recovery holddown.
    let now_ts = chrono::Utc::now().timestamp();
    for symbol in [BASE, QUOTE] {
        h.engine.tick_feed_health.insert(
            symbol.to_string(),
            market::FeedHealth {
                last_accepted_ts: now_ts - 100,
                gap_recovered_ts: None,
            },
        );
    }
    let stale_before = defer_count("hg-inelig-gap", "stale");

    h.step().await;
    assert!(
        h.position(0).is_some(),
        "first valid tick after a rejection storm must defer, not close"
    );
    assert_eq!(
        defer_count("hg-inelig-gap", "stale"),
        stale_before + 1,
        "the deferral must be attributed to the stale check"
    );
    assert!(
        h.engine
            .tick_feed_health
            .get(BASE)
            .and_then(|f| f.gap_recovered_ts)
            .is_some(),
        "the accepted tick that ended the gap must arm the recovery holddown"
    );

    // Holddown elapsed (backdate the recovery past the 30s threshold; the
    // accepted feed itself stays fresh): the close must now go through.
    let now_ts = chrono::Utc::now().timestamp();
    for symbol in [BASE, QUOTE] {
        if let Some(f) = h.engine.tick_feed_health.get_mut(symbol) {
            f.gap_recovered_ts = Some(now_ts - 31);
        }
    }
    h.step().await;
    assert!(
        h.position(0).is_none(),
        "close must fire once the recovery holddown has elapsed"
    );
}

#[tokio::test]
async fn ineligible_close_immediate_when_guard_disabled() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    // Same degraded book, guard left at the cap=0 default: today's
    // immediate flatten must be unchanged.
    let mut h = ineligible_guard_harness("hg-inelig-off");
    h.engine.cfg.ineligible_close_defer_cap_secs = 0;

    h.step().await;
    assert!(
        h.position(0).is_none(),
        "with the guard disabled the ineligible close fires on the same tick"
    );
}

#[tokio::test]
async fn eligibility_margin_grace_holds_until_event_time_deadline() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = ineligible_guard_harness("hg-elig-margin");
    h.engine.cfg.ineligible_close_defer_cap_secs = 0;
    h.engine.cfg.eligibility_margin_grace_secs = 60;
    h._connector.set_half_spread_frac(dec("0.0005"));
    let now_ts = chrono::Utc::now().timestamp();
    h.engine
        .per_pair_state
        .get_mut(PAIR_KEY)
        .unwrap()
        .eligibility_margin_grace_until_ts = Some(now_ts + 60);

    h.step().await;
    assert!(
        h.position(0).is_some(),
        "held position must use exit eligibility while raw entry eligibility is false"
    );
    assert!(
        !h.engine.per_pair_state[PAIR_KEY].eligible,
        "raw entry eligibility must remain false during the grace"
    );

    h.engine
        .per_pair_state
        .get_mut(PAIR_KEY)
        .unwrap()
        .eligibility_margin_grace_until_ts = Some(chrono::Utc::now().timestamp() - 1);
    h.step().await;
    assert!(
        h.position(0).is_none(),
        "deadline expiry must release the ineligible close without another evaluation"
    );
}

#[tokio::test]
async fn eligibility_margin_grace_does_not_block_force_close() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = ineligible_guard_harness("hg-elig-margin-force");
    h.engine.cfg.ineligible_close_defer_cap_secs = 0;
    h.seed_position(0, 61);
    h.engine
        .per_pair_state
        .get_mut(PAIR_KEY)
        .unwrap()
        .eligibility_margin_grace_until_ts = Some(chrono::Utc::now().timestamp() + 60);

    h.step().await;
    assert!(
        h.position(0).is_none(),
        "force-close must bypass the eligibility margin grace"
    );
}

/// A started deferral is a close obligation (PR #166 Codex review): if
/// eligibility flips back to true while the close is still deferred, the
/// guard must not drop the already-triggered flatten — it re-times it,
/// firing as soon as the book recovers (or the cap expires), exactly as
/// if the pair had stayed ineligible.
#[tokio::test]
async fn ineligible_close_survives_eligibility_recovery() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = ineligible_guard_harness("hg-inelig-flip");
    // Park every eligible-branch exit gate out of reach so a close after
    // the flip can only come from the deferred ineligible flatten — not
    // from stop_loss_z / exit_z happening to fire on the same tick.
    {
        let pp = &mut h.engine.instances[0].default_pair_params;
        pp.stop_loss_z = 50.0;
        pp.exit_z = 0.0;
    }

    h.step().await;
    assert!(
        h.position(0).is_some(),
        "first tick defers while the book is degraded"
    );
    assert!(
        h.engine.instances[0].states[PAIR_KEY]
            .ineligible_defer_since_ts
            .is_some(),
        "the deferral window must have started"
    );

    // Eligibility recovers AND the book heals before the cap expires.
    h.engine.per_pair_state.get_mut(PAIR_KEY).unwrap().eligible = true;
    h._connector.set_half_spread_frac(dec("0.0005"));
    h.step().await;
    assert!(
        h.position(0).is_none(),
        "the deferred close must still fire after eligibility recovers"
    );
    assert!(
        h.engine.instances[0].states[PAIR_KEY]
            .ineligible_defer_since_ts
            .is_none(),
        "firing the close must clear the deferral window"
    );
}

/// A risk-triggered exit must never be deferred (PR #166 Codex review):
/// once a held pair turns ineligible, this flatten is the only path that
/// realizes `stop_loss_z` / `max_loss_r` / `risk_budget`, so a degraded
/// book must not hold an already-breached loss budget open for up to the
/// deferral cap.
#[tokio::test]
async fn ineligible_close_bypasses_deferral_when_risk_exit_pending() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    // Guard enabled + degraded 40 bps book, exactly like the defer test —
    // but the position is deep underwater: leg-A entry far above the
    // current book puts the PnL (≈ -$140) past the max_loss_r budget
    // (equity $10k × risk 1% × mult 1.0 = -$100).
    let mut h = ineligible_guard_harness("hg-inelig-risk");
    {
        let state = h.engine.instances[0].states.get_mut(PAIR_KEY).unwrap();
        state.position.as_mut().unwrap().entry_price_a = Some(dec("400"));
    }
    let deferred_before = defer_count("hg-inelig-risk", "spread");

    h.step().await;
    assert!(
        h.position(0).is_none(),
        "a pending risk exit must close immediately even into the degraded book"
    );
    assert_eq!(
        defer_count("hg-inelig-risk", "spread"),
        deferred_before,
        "the bypass must not count as a deferral"
    );
    assert!(
        h.engine.instances[0].states[PAIR_KEY]
            .ineligible_defer_since_ts
            .is_none(),
        "no deferral window may be opened for a risk-exit bypass"
    );
}

// ---------------------------------------------------------------------
// bot-strategy#732: the β-clamp structural guard must run BEFORE the
// tunable β gates. In the observed 2026-07-15 single-component collapse
// (β_s floor-pinned at 0.10, β_l ≈ 0.25) the composite stays interior
// (~0.145) but beta_gap ≈ 1.03 — if `beta_divergence` ran first it would
// absorb the reject and the `beta_clamp` Prometheus label would never
// surface the collapse (PR #169 Codex review).
// ---------------------------------------------------------------------
#[tokio::test]
async fn beta_component_clamp_fires_before_divergence_gate() {
    let _serial = gate_lock().lock().unwrap_or_else(|e| e.into_inner());
    clear_sentinels();

    let mut h = Harness::new("hg-beta-clamp");
    {
        let shared = h.engine.per_pair_state.get_mut(PAIR_KEY).unwrap();
        shared.beta_short = 0.10; // BETA_CLAMP_MIN floor
        shared.beta_long = 0.25;
        shared.beta = 0.7 * shared.beta_short + 0.3 * shared.beta_long;
        shared.beta_gap = (shared.beta_short - shared.beta_long).abs() / shared.beta;
    }

    h.step().await;

    assert!(
        h.position(0).is_none(),
        "entry must be suppressed while a β component sits at the clamp floor"
    );
    assert_eq!(
        reject_count("hg-beta-clamp", "beta_clamp"),
        1,
        "the structural clamp guard must attribute the reject, not a tunable gate"
    );
    assert_eq!(
        reject_count("hg-beta-clamp", "beta_divergence"),
        0,
        "beta_divergence must not absorb the collapse-shape reject"
    );

    // Healthy interior components on the next tick → the guard releases
    // and the queued entry signal goes through without a restart. Restore
    // the exact harness preconditions (β=1.0 all around, zero history) so
    // the clamped step's polluted spread push does not distort the signal.
    {
        let shared = h.engine.per_pair_state.get_mut(PAIR_KEY).unwrap();
        shared.beta_short = 1.0;
        shared.beta_long = 1.0;
        shared.beta = 1.0;
        shared.beta_gap = 0.0;
        shared.spread_history = std::iter::repeat(0.0).take(29).collect();
    }
    h.step().await;
    assert!(
        h.position(0).is_some(),
        "entry must resume once every β estimate is interior again"
    );
    assert_eq!(
        reject_count("hg-beta-clamp", "beta_clamp"),
        1,
        "no further beta_clamp reject once the components recover"
    );
}
