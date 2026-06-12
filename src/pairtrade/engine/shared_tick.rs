//! Shared tick/bar/evaluation phase for the pairtrade engine.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use anyhow::{anyhow, Result};
use chrono::Utc;
use dex_connector::PriceUpdate;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::time::Duration;

use super::super::config::PairSpec;
use super::super::defaults::PAIR_SELECTION_INTERVAL_SECS;
use super::super::market::{
    quote_sanity_check, tick_sanity_check, SymbolSnapshot, MAX_TICK_PRICE_ENVELOPE_BPS,
    MAX_TICK_SPREAD_BPS,
};
use super::super::pair_eval;
use super::super::regime;
use super::super::stats::PriceSample;
use super::super::util::tail_std;
use super::super::PairTradeEngine;

#[derive(Serialize)]
struct DataDumpEntry<'a> {
    timestamp: i64,
    prices: &'a HashMap<String, SymbolSnapshot>,
}

impl PairTradeEngine {
    /// Shared phase: run once per outer step. Fetches the canonical price
    /// tick, advances the ReplayConnector clock exactly once, updates the
    /// engine-wide history + bar builders, and returns the `(price_map,
    /// updated)` pair for the per-instance phase. Returns `Ok(None)` when a
    /// host-shared cooldown is active and every instance should skip.
    pub(super) async fn step_shared(
        &mut self,
    ) -> Result<Option<(HashMap<String, SymbolSnapshot>, HashSet<String>)>> {
        // Lighter WAF cooldown is host-shared. Any REST call we make here
        // would be rejected anyway and would refresh the rolling window,
        // turning a 60s cooldown into a permanent block. Skip silently.
        // dex-connector logs once on engagement; the email goes out via
        // report_rate_limit. See bot-strategy#35.
        #[cfg(feature = "lighter-sdk")]
        if dex_connector::lighter_waf_cooldown::cooldown_remaining().is_some() {
            return Ok(None);
        }

        self.update_kill_switch_state();
        self.consume_risk_ack();
        self.refresh_daily_session();

        let price_map = self.fetch_latest_prices().await?;

        if let Some(writer) = &mut self.data_dump_writer {
            let dump_entry = DataDumpEntry {
                timestamp: Utc::now().timestamp_millis(),
                prices: &price_map,
            };
            if let Ok(json_string) = serde_json::to_string(&dump_entry) {
                if writer.write_line(&json_string).is_err() {
                    log::error!("[DataDump] Failed to write to dump file");
                }
            }
        }

        // Bar build + history update is engine-wide: all instances read
        // from the same `self.history`, so we must do it exactly once per
        // outer tick before any per-instance decision logic runs.
        let max_history_len = self.max_history_len();
        let now_ts = self.current_now_ts();
        self.load_history_from_disk();

        // BT restart simulation (bot-strategy#27 comment 2026-04-16): when
        // the replay crosses a timestamp listed in
        // `BT_RESTART_TIMESTAMPS_FILE`, re-run `warm_start_states_from_history`
        // to mirror what the live bot does at each systemd restart —
        // re-compute `state.beta` from OLS and re-seed `spread_history`
        // with 240 single-beta spreads. That seeded low-variance history
        // is the mechanism behind the 2026-04-15 06:02 UTC "std collapse"
        // (bot-strategy#62 — now known to be a restart artifact, not a
        // regime break). We fire on crossing, not exact match, because
        // the live dump has a gap (WS down) around the restart second, so
        // the exact `restart_ts` often has no replay record. Each matched
        // ts is removed from the set, so each restart fires at most once.
        let restart_passed = self
            .cfg
            .bt_restart_timestamps
            .as_mut()
            .map(|set| {
                let passed: Vec<i64> = set.iter().filter(|&&t| t <= now_ts).copied().collect();
                for t in &passed {
                    set.remove(t);
                }
                !passed.is_empty()
            })
            .unwrap_or(false);
        if restart_passed {
            log::warn!(
                "[BT_RESTART] simulating live service restart (now_ts={})",
                now_ts
            );
            self.warm_start_states_from_history();
        }
        let mut updated = HashSet::new();
        // Sort symbols before processing so [TICK_FILTER] / [BAR_FORCE_CLOSE]
        // log ordering and bar push ordering are deterministic across
        // builds — HashMap iteration order is intentionally randomized,
        // which previously caused intermittent golden_baseline mismatches.
        let mut sorted_symbols: Vec<&String> = price_map.keys().collect();
        sorted_symbols.sort();
        for symbol in sorted_symbols {
            let snapshot = price_map
                .get(symbol)
                .expect("just enumerated from price_map");
            // bot-strategy#346: drop corrupt orderbook frames before they
            // poison the bar builder / regression history. The data dump
            // above already recorded the raw frame for diagnostics.
            //
            // Logged at INFO (not WARN) because trips are designed-
            // informational — the filter is *protecting* against bad ticks,
            // not raising an actionable alarm. WARN-level was polluting the
            // dashboard's `warn_count_30m` (200-500/30min on Frankfurt due
            // to Lighter's hourly funding-cycle orderbook noise) and
            // burying genuinely-actionable WARNs. error-watch's TICK_FILTER
            // skip pattern (bot-strategy#356) becomes redundant after this
            // but is left in place as a safety net.
            if let Err(reason) =
                tick_sanity_check(snapshot, MAX_TICK_SPREAD_BPS, MAX_TICK_PRICE_ENVELOPE_BPS)
            {
                log::info!(
                    "[TICK_FILTER] rejected {} reason={} price={} bid={:?} ask={:?} bid_size={} ask_size={}",
                    symbol,
                    reason.as_str(),
                    snapshot.price,
                    snapshot.bid_price,
                    snapshot.ask_price,
                    snapshot.bid_size,
                    snapshot.ask_size,
                );
                continue;
            }
            // bot-strategy#364: record realized funding rate into the
            // rolling history so exit_fill can attribute per-cycle carry
            // without an external REST fetch. Lighter settles funding
            // hourly; `observe` dedupes unchanged rates so the buffer
            // averages 1 push per symbol per hour.
            self.funding_history
                .observe(symbol, now_ts, snapshot.funding_rate);
            if let Some(builder) = self.bar_builders.get_mut(symbol) {
                // `snapshot.exchange_ts` is ms post bot-strategy#274 / #276;
                // `now_ts` is wall-clock seconds, lift it to ms when the
                // connector did not surface an exchange timestamp.
                let tick_ts = snapshot
                    .exchange_ts
                    .unwrap_or_else(|| now_ts.saturating_mul(1000));
                let mut emits: Vec<(Decimal, i64)> = Vec::new();
                if let Some(close) = builder.push(tick_ts, snapshot.price) {
                    emits.push(close);
                }
                // Defensive backstop (bot-strategy#341): if the bucket has
                // been open longer than 1.5 × window without an emit (e.g.,
                // both WS and polling went quiet), force-close so the bar
                // stream doesn't stall. Live-only — backtest replays must
                // reproduce live-at-the-time behavior byte-exactly, and a
                // synthetic bar in BT would not match a pre-v2 production
                // run.
                if !self.cfg.backtest_mode {
                    let now_ms = now_ts.saturating_mul(1000);
                    if let Some(close) = builder.force_close_if_stale(now_ms) {
                        log::warn!(
                            "[BAR_FORCE_CLOSE] {} synthetic close at ts={} \
                             (no tick advanced bucket within 1.5 × window)",
                            symbol,
                            close.1
                        );
                        emits.push(close);
                    }
                }
                for (close_price, close_ts) in emits {
                    let entry = self.history.entry(symbol.clone()).or_default();
                    let log_price = close_price
                        .to_f64()
                        .ok_or_else(|| anyhow!("invalid price for {}", symbol))?
                        .ln();
                    if entry.back().map(|s| s.ts) != Some(close_ts) {
                        if entry.len() >= max_history_len {
                            entry.pop_front();
                        }
                        entry.push_back(PriceSample {
                            log_price,
                            ts: close_ts,
                        });
                    }
                    updated.insert(symbol.clone());
                    if !self.cfg.backtest_mode {
                        self.bar_emit_log
                            .entry(symbol.clone())
                            .or_default()
                            .push_back(Instant::now());
                    }
                }
            } else {
                log::debug!("no bar builder for {}", symbol);
            }
        }
        if !self.cfg.backtest_mode {
            self.check_bar_rate_canary();
        }

        // bot-strategy#413: run the spread / Kalman / eval pipeline once per
        // pair before the per-instance loop. All variants on a pair share
        // `self.per_pair_state[pair]`, so A/B/C observe byte-identical
        // β / std / z. The eval gate is OR'd across instances so eval
        // cadence matches the most-reactive variant's signal.
        let universe = self.cfg.universe.clone();
        let now_ts_shared = self.current_now_ts();
        for pair in &universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            if !(updated.contains(&pair.base) && updated.contains(&pair.quote)) {
                continue;
            }
            self.step_pair_shared(pair, &key, now_ts_shared);
        }

        self.persist_history_to_disk();

        Ok(Some((price_map, updated)))
    }

    /// Per-pair shared phase (bot-strategy#413). Runs the Kalman update,
    /// pushes the new spread, computes the OR'd re-eval gate across all
    /// `StrategyInstance`s on this pair, and commits the eval result into
    /// `self.per_pair_state[key]` so every variant reads the same
    /// β / spread_history / std / z. Emits the canonical [ZCHECK] +
    /// [KALMAN] diagnostic logs once per pair per tick (was 3× per tick
    /// pre-#413).
    fn step_pair_shared(&mut self, pair: &PairSpec, key: &str, now_ts: i64) {
        let Some(log_a) = self.latest_log_price(&pair.base) else {
            return;
        };
        let Some(log_b) = self.latest_log_price(&pair.quote) else {
            return;
        };
        let hist_a_prev = self
            .history
            .get(&pair.base)
            .and_then(|h| h.iter().rev().nth(1).map(|s| s.log_price));
        let hist_b_prev = self
            .history
            .get(&pair.quote)
            .and_then(|h| h.iter().rev().nth(1).map(|s| s.log_price));

        // Kalman update + spread push must run before we read z_snapshot
        // back out, because push_spread also recomputes
        // last_velocity_sigma_per_min and std_history.
        let metrics_window = self.cfg.metrics_window;
        {
            let Some(shared) = self.per_pair_state.get_mut(key) else {
                return;
            };
            // Per-bar log-return deltas, shared by the Kalman update and the
            // innovation-responsive regime detector. Needs a prior bar
            // (`last_spread`) and both legs' previous log prices.
            let deltas = if shared.last_spread.is_some() {
                match (hist_a_prev, hist_b_prev) {
                    (Some(a_prev), Some(b_prev)) => Some((log_b - b_prev, log_a - a_prev)),
                    _ => None,
                }
            } else {
                None
            };
            if let Some((dx, dy)) = deltas {
                if let Some(ref mut kf) = shared.kalman {
                    kf.update(dx, dy);
                }
                // bot-strategy#494 Phase 1 (shadow): feed the persistent-regime
                // detector the model's one-step innovation = Δspread under the
                // hedging β (`dy − β·dx`), independent of whether the Kalman
                // path is enabled. Capture the active duration before the
                // update so a `Cleared` transition can log how long the shift
                // lasted.
                let innovation = dy - shared.beta * dx;
                let beta = shared.beta;
                let active_secs_before = shared.regime.active_secs(now_ts);
                match shared.regime.update(innovation, now_ts) {
                    regime::RegimeTransition::Activated => log::warn!(
                        "[REGIME] {} persistent-shift ACTIVE event_ts={} cusum={:.2} scale={:.6} norm={:.2} beta={:.4}",
                        key,
                        now_ts,
                        shared.regime.cusum(),
                        shared.regime.residual_scale(),
                        shared.regime.last_normalized(),
                        beta,
                    ),
                    regime::RegimeTransition::Cleared => log::info!(
                        "[REGIME] {} persistent-shift CLEARED event_ts={} after {:.0}s cusum={:.2} scale={:.6}",
                        key,
                        now_ts,
                        active_secs_before,
                        shared.regime.cusum(),
                        shared.regime.residual_scale(),
                    ),
                    regime::RegimeTransition::None => {}
                }
                // bot-strategy#494: periodic CUSUM series log for threshold
                // calibration. Transition logs alone only show `h_on`
                // crossings; calibrating an alternative threshold needs the
                // statistic's envelope over time, so emit the point value
                // plus the interval high-water mark every 5 event-time
                // minutes. Event-time gating keeps byte-exact replay output
                // at the same cadence as live.
                const REGIME_SHADOW_LOG_INTERVAL_SECS: i64 = 300;
                let shadow_due = shared
                    .last_regime_shadow_ts
                    .map(|t| now_ts - t >= REGIME_SHADOW_LOG_INTERVAL_SECS)
                    .unwrap_or(true);
                if shadow_due {
                    log::info!(
                        "[REGIME_SHADOW] {} event_ts={} cusum={:.3} peak={:.3} scale={:.6} norm={:.2} active={}",
                        key,
                        now_ts,
                        shared.regime.cusum(),
                        shared.regime.take_interval_peak(),
                        shared.regime.residual_scale(),
                        shared.regime.last_normalized(),
                        shared.regime.is_active(),
                    );
                    shared.last_regime_shadow_ts = Some(now_ts);
                }
                // bot-strategy#534: per-tick raw series for offline governor
                // calibration. The 300s shadow cadence cannot rebuild an
                // alternative (e.g. dual-timescale) statistic; that needs
                // the raw innovation at every tick.
                if let Some(writer) = self.regime_series_writer.as_mut() {
                    use std::io::Write;
                    let _ = writeln!(
                        writer,
                        "{},{},{:.6e},{:.6},{:.6e},{:.4},{:.4},{}",
                        now_ts,
                        key,
                        innovation,
                        beta,
                        shared.regime.residual_scale(),
                        shared.regime.last_normalized(),
                        shared.regime.cusum(),
                        u8::from(shared.regime.is_active()),
                    );
                }
            }
            let spread = log_a - shared.beta * log_b;
            shared.push_spread(spread, metrics_window, &self.cfg);
        }

        // Snapshot derived state post-push.
        let (z_snapshot, velocity, prev_eligible, last_eval_ts) = {
            let Some(shared) = self.per_pair_state.get(key) else {
                return;
            };
            (
                shared.z_score_details(),
                shared.last_velocity_sigma_per_min,
                shared.eligible,
                shared.last_evaluated_ts,
            )
        };
        let current_std = z_snapshot.map(|(_, std, _, _)| std).unwrap_or(0.0);
        let base_std = self
            .per_pair_state
            .get(key)
            .and_then(|s| tail_std(&s.spread_history, metrics_window));
        let z_abs = z_snapshot.map(|(z, _, _, _)| z.abs()).unwrap_or(0.0);

        // OR'd re-eval gate across every StrategyInstance on this pair.
        // Eval cadence is a pair-level concern post-#413; the variant
        // that would have triggered eval drives the cadence for all.
        let bt_eval_force = self
            .cfg
            .bt_eval_timestamps
            .as_ref()
            .map(|set| set.contains(&now_ts));
        let needs_eval_interval = last_eval_ts
            .map(|t| now_ts.saturating_sub(t) >= PAIR_SELECTION_INTERVAL_SECS as i64)
            .unwrap_or(true);
        let mut needs_eval_jump_any = false;
        let mut needs_eval_velocity_any = false;
        let mut vol_spike_any = false;
        let cfg_entry_z_base = self.cfg.default_pair_params.entry_z_base;
        for inst_idx in 0..self.instances.len() {
            let pp = self.pair_params_for(inst_idx, key);
            let z_entry = self.instances[inst_idx]
                .states
                .get(key)
                .map(|s| s.z_entry)
                .unwrap_or(cfg_entry_z_base);
            if z_abs >= z_entry * pp.reeval_jump_z_mult {
                needs_eval_jump_any = true;
            }
            if velocity.abs() >= pp.spread_velocity_max_sigma_per_min * pp.reeval_jump_z_mult {
                needs_eval_velocity_any = true;
            }
            if let Some(bs) = base_std {
                if bs > 1e-9 && current_std / bs >= pp.vol_spike_mult {
                    vol_spike_any = true;
                }
            }
        }
        let should_eval = match bt_eval_force {
            Some(force) => force,
            None => {
                needs_eval_interval
                    || needs_eval_jump_any
                    || needs_eval_velocity_any
                    || vol_spike_any
            }
        };

        let eval = if should_eval {
            let res = pair_eval::evaluate_pair(&self.cfg, &self.history, pair);
            if let Some(ref e) = res {
                log::info!(
                    "[EVAL] {} beta_s={:.3} beta_l={:.3} beta={:.3} hl={:.2}h p={:.3} eligible={} score={:.3}",
                    key,
                    e.beta_short,
                    e.beta_long,
                    e.beta_eff,
                    e.half_life_hours,
                    e.adf_p_value,
                    e.eligible,
                    e.score
                );
            } else {
                let (avail_a, avail_b) = (
                    self.history.get(&pair.base).map(|h| h.len()).unwrap_or(0),
                    self.history.get(&pair.quote).map(|h| h.len()).unwrap_or(0),
                );
                let pp = &self.cfg.default_pair_params;
                log::debug!(
                    "[EVAL] {} insufficient history ({}:{}, need long/short (strict) {} / {}, mode={:?})",
                    key,
                    pair.base,
                    avail_a,
                    pp.lookback_hours_long.max(pp.lookback_hours_short) * 3600
                        / self.cfg.trading_period_secs,
                    (pp.lookback_hours_short * 3600) / self.cfg.trading_period_secs,
                    self.cfg.warm_start_mode
                );
                log::debug!(
                    "[EVAL] {} insufficient history ({}:{}, need long/short (strict) {} / {}, mode={:?})",
                    key,
                    pair.quote,
                    avail_b,
                    pp.lookback_hours_long.max(pp.lookback_hours_short) * 3600
                        / self.cfg.trading_period_secs,
                    (pp.lookback_hours_short * 3600) / self.cfg.trading_period_secs,
                    self.cfg.warm_start_mode
                );
            }
            res
        } else {
            None
        };

        let use_kalman_beta = self.cfg.use_kalman_beta;
        let kalman_min_updates = self.cfg.kalman_min_updates;
        if let Some(eval) = eval {
            if let Some(shared) = self.per_pair_state.get_mut(key) {
                let kf_beta_warm = if use_kalman_beta {
                    shared
                        .kalman
                        .as_ref()
                        .filter(|kf| kf.is_warm(kalman_min_updates))
                        .map(|kf| kf.beta)
                } else {
                    None
                };
                let new_beta = kf_beta_warm.unwrap_or(eval.beta_eff);
                // bot-strategy#472 defense-in-depth — surface a single
                // collapsing-β tick as a WARN + Prom counter. Threshold
                // is "healthy interior" (> 0.5) to "near-floor" (≤ 0.15)
                // in one eval. Caught 5/22 06:30 in retrospect; with
                // this counter wired, a future event surfaces in the
                // dashboard error-watch (#168) without waiting for the
                // operator to notice a PnL anomaly.
                const BETA_COLLAPSE_PREV_FLOOR: f64 = 0.5;
                const BETA_COLLAPSE_NEW_CEILING: f64 = 0.15;
                if shared.beta > BETA_COLLAPSE_PREV_FLOOR && new_beta <= BETA_COLLAPSE_NEW_CEILING {
                    log::warn!(
                        "[BETA_COLLAPSE] {} beta {:.4} -> {:.4} \
                         (beta_short={:.4} beta_long={:.4}) — possible corrupt-bar event; \
                         see bot-strategy#472",
                        key,
                        shared.beta,
                        new_beta,
                        eval.beta_short,
                        eval.beta_long,
                    );
                    // β is per-pair (shared across A/B/C variants), so
                    // we use "*" for the variant label — matches the
                    // convention used by ENTRY_OVERSIZE_CAPPED_TOTAL
                    // for pair-level events.
                    crate::pairtrade::prom::BETA_COLLAPSE_EVENT_TOTAL
                        .with_label_values(&["*", key])
                        .inc();
                }
                shared.beta = new_beta;
                shared.beta_short = eval.beta_short;
                shared.beta_long = eval.beta_long;
                shared.half_life_hours = eval.half_life_hours;
                shared.adf_p_value = eval.adf_p_value;
                shared.eligible = eval.eligible;
                shared.p_value_weighted_score = eval.score;
                shared.beta_gap = eval.beta_gap;
                shared.last_evaluated = Some(Instant::now());
                shared.last_evaluated_ts = Some(now_ts);
                if prev_eligible != shared.eligible {
                    log::info!(
                        "[ELIGIBILITY] {} -> {} (p={:.3} hl={:.2}h beta_gap={:.3})",
                        key,
                        shared.eligible,
                        shared.adf_p_value,
                        shared.half_life_hours,
                        (shared.beta_short - shared.beta_long).abs()
                    );
                }
            }
        }

        // Canonical [ZCHECK] + [KALMAN] diagnostics (one emit per pair,
        // pre-#413 was once per StrategyInstance — 3× spam on A/B/C).
        let base_first_ts = self
            .history
            .get(&pair.base)
            .and_then(|h| h.front())
            .map(|s| s.ts);
        let quote_first_ts = self
            .history
            .get(&pair.quote)
            .and_then(|h| h.front())
            .map(|s| s.ts);
        let base_bar = self.history.get(&pair.base).and_then(|h| h.back()).cloned();
        let quote_bar = self
            .history
            .get(&pair.quote)
            .and_then(|h| h.back())
            .cloned();
        if let (Some(ba), Some(bq), Some(shared)) =
            (base_bar, quote_bar, self.per_pair_state.get(key))
        {
            if let Some((z, std, mean, latest)) = shared.z_score_details() {
                log::info!(
                    "[ZCHECK] {} bucket_ts={} bar_first_a={} bar_first_b={} bar_last_b={} \
                     close_a={:.6} close_b={:.6} \
                     beta_eff={:.4} beta_s={:.4} beta_l={:.4} mean={:.6} std={:.6} \
                     spread={:.6} z={:.4} hist={}",
                    key,
                    ba.ts,
                    base_first_ts.unwrap_or(0),
                    quote_first_ts.unwrap_or(0),
                    bq.ts,
                    ba.log_price,
                    bq.log_price,
                    shared.beta,
                    shared.beta_short,
                    shared.beta_long,
                    mean,
                    std,
                    latest,
                    z,
                    shared.spread_history.len(),
                );
            }
            if use_kalman_beta {
                if let Some(ref kf) = shared.kalman {
                    log::info!(
                        "[KALMAN] {} kalman_beta={:.4} ols_beta={:.4} diff={:.4} p={:.6} warm={}",
                        key,
                        kf.beta,
                        shared.beta,
                        kf.beta - shared.beta,
                        kf.p,
                        kf.is_warm(kalman_min_updates),
                    );
                }
            }
        }
    }

    /// Feed a single WebSocket price tick into the BarBuilder for `symbol`,
    /// refining the in-progress bucket close via `update_close_only`. The
    /// polling arm in `step_shared` retains exclusive bucket-emit authority
    /// — this fn never appends to `self.history` and never advances the
    /// builder past the current bucket.
    ///
    /// Phase 2 v2 (bot-strategy#341): keeping bucket-emit single-sourced
    /// avoids the original Phase 2 class of bugs where the WS arm could
    /// silently fail to emit across a bucket boundary while the polling
    /// arm was disabled, freezing β for hours.
    pub(in crate::pairtrade) fn ingest_price_update(&mut self, update: PriceUpdate) {
        // bot-strategy#472 — the WS arm previously passed `update.mid_price`
        // straight to `update_close_only` with zero sanity checking. The
        // polling arm above runs the same Lighter book through
        // `tick_sanity_check`, but the WS arm bypassed it. Result: a
        // corrupt orderbook frame (Frankfurt 2026-05-22 06:31 UTC, ETH
        // bid=$1770 ask=$3188, 5,700 bps spread) committed an outlier
        // bar close that dominated `var(ETH log-price)` in the 240-bar
        // OLS regression and pinned β to the floor clamp for ~1h47m
        // (issue #472 RCA). `PriceUpdate` doesn't carry order sizes, so
        // we use the price-only `quote_sanity_check` here — same
        // spread / envelope / crossed-book constants as the polling
        // path; the empty_size gates that only the polling path
        // surfaces aren't reachable from WS data.
        if let Err(reason) = quote_sanity_check(
            Some(update.best_bid),
            Some(update.best_ask),
            update.mid_price,
            MAX_TICK_SPREAD_BPS,
            MAX_TICK_PRICE_ENVELOPE_BPS,
        ) {
            log::info!(
                "[TICK_FILTER_WS] rejected {} reason={} mid={} bid={} ask={}",
                update.symbol,
                reason.as_str(),
                update.mid_price,
                update.best_bid,
                update.best_ask,
            );
            return;
        }
        let Some(builder) = self.bar_builders.get_mut(&update.symbol) else {
            log::debug!("[WS_BARS] no bar builder for {}", update.symbol);
            return;
        };
        builder.update_close_only(update.timestamp as i64, update.mid_price);
    }

    /// Sustained bar-emit-rate canary (bot-strategy#341). Walks
    /// `bar_emit_log`, drops entries older than the rolling window, and
    /// warns when emit-rate falls below the jitter floor. Rate-limited
    /// to one WARN per symbol per 60 s. Designed to surface the original
    /// Phase 2 β-freeze symptom (≤1 bar / 4 min for 78 h) within
    /// minutes, but tuned wider than 0.8× expected to ride out 60 s-bar
    /// cadence jitter (Tokyo Lighter Phase B canary on master 3e997d4,
    /// 2026-05-12 13:43–15:13 UTC, 2 spurious WARNs at n=1 over 90 s,
    /// no [BAR_FORCE_CLOSE]).
    fn check_bar_rate_canary(&mut self) {
        let now = Instant::now();
        // 180 s window + 180 s minimum observation: with Lighter polling
        // jitter (3.5–6.5 s) the bucket-crossing tick does not always
        // arrive in the same 60 s slice, so a 120 s window observes n=1
        // at the jitter floor. 180 s ⇒ jitter floor is n=2 (≈ 0.67 /min).
        let window = Duration::from_secs(180);
        let warn_cooldown = Duration::from_secs(60);
        let min_observation = Duration::from_secs(180);

        // Threshold is 2/3 of expected: n=2 over 180 s ≈ 0.67 /min ⇒
        // healthy (jitter floor); n=1 over 180 s ≈ 0.33 /min ⇒ WARN
        // (real stall — would also trigger [BAR_FORCE_CLOSE] downstream).
        let period_secs = self.cfg.trading_period_secs.max(1);
        let expected_per_min = 60.0 / period_secs as f64;
        let threshold_per_min = (expected_per_min * 2.0 / 3.0).max(0.05);

        let symbols: Vec<String> = self.bar_emit_log.keys().cloned().collect();
        for symbol in symbols {
            let log = self.bar_emit_log.get_mut(&symbol).expect("just enumerated");
            while let Some(front) = log.front() {
                if now.duration_since(*front) > window {
                    log.pop_front();
                } else {
                    break;
                }
            }
            let count = log.len();
            let oldest = log.front().copied();
            let observed_for = oldest.map(|t| now.duration_since(t)).unwrap_or_default();
            if observed_for < min_observation {
                continue;
            }
            let rate_per_min = count as f64 / (observed_for.as_secs_f64() / 60.0).max(1e-9);
            if rate_per_min >= threshold_per_min {
                continue;
            }
            let last_warn = self.last_bar_rate_warn.get(&symbol).copied();
            if last_warn
                .map(|t| now.duration_since(t) < warn_cooldown)
                .unwrap_or(false)
            {
                continue;
            }
            log::warn!(
                "[BAR_RATE] {} rate={:.2}/min over {:.0}s (n={}, threshold={:.2}/min, \
                 expected={:.2}/min) — bar emission stalled, investigate WS/polling",
                symbol,
                rate_per_min,
                observed_for.as_secs_f64(),
                count,
                threshold_per_min,
                expected_per_min,
            );
            self.last_bar_rate_warn.insert(symbol, now);
        }
    }
}
