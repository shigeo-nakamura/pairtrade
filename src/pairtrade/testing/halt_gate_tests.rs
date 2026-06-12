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

use super::defaults::DEFAULT_EQUITY_USD;
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
        // ±10 bps book around the ticker price: comfortably inside
        // MAX_TICK_SPREAD_BPS / MAX_TICK_PRICE_ENVELOPE_BPS so the polled
        // snapshot survives tick_sanity_check and reaches the BarBuilder.
        let price = self.price_of(symbol);
        let spread = price * dec("0.001");
        Ok(OrderBookSnapshot {
            bids: vec![OrderBookLevel {
                price: price - spread,
                size: dec("100"),
            }],
            asks: vec![OrderBookLevel {
                price: price + spread,
                size: dec("100"),
            }],
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
            session_start_ts: now_ts,
            realized_pnl_today: 0.0,
            funding_carry_today: 0.0,
            daily_loss_halted: false,
            equity_samples: Vec::new(),
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
        let now_ts = chrono::Utc::now().timestamp();
        let state = self.engine.instances[inst_idx]
            .states
            .get_mut(PAIR_KEY)
            .unwrap();
        state.position = Some(Position {
            direction: PositionDirection::LongSpread,
            entered_at: Instant::now(),
            entered_ts: now_ts - 3600,
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
