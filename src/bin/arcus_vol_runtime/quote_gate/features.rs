//! Feature engine for the quote gate (bot-strategy#1120): the 37 features of
//! the #1113 study (`rf_common.FEATURE_NAMES` minus `side`), computed at a
//! decision time τ from records received STRICTLY before τ (`rx < τ`).
//!
//! Semantics are 1:1 with `rf_common.features()` (CUTOFF §4):
//! - every stream is a ring of `(rx_ms, value)`; an as-of read at `t` is the
//!   last record with `rx < t` (`searchsorted(side="left") - 1`), never one
//!   at `t`;
//! - premium-deviation and realized-vol features live on a 0.1 s grid whose
//!   points are at `origin + k * 100 ms`; grid point k uses records with
//!   `rx < origin + k * 100 ms`, and a decision at τ reads the last grid
//!   point ≤ τ (`floor((τ - origin) / dt)`);
//! - flows sum records with `t - h <= rx < t`;
//! - a value that cannot be formed (no record before `t - h`, EMA not yet
//!   initialised) is NOT zero-filled: `features_at` returns the fallback
//!   reason naming the feature (design D6; `rf_common`'s `nan_to_num` is a
//!   training-time convenience only). The two exceptions are the L2
//!   imbalances, which the training data itself defines as 0 when no L2
//!   snapshot precedes τ.
//!
//! Everything here is pure: no IO, no clock. The feed tasks push records with
//! the receive time they stamped; the tick calls `features_at(τ)`.

use std::collections::VecDeque;

/// Grid step for EMA / realized-vol features (CUTOFF §4: 0.1 s).
pub const GRID_DT_MS: u64 = 100;
/// EMA spans (seconds) of the Binance premium deviation, in feature order.
pub const PDEV_BN_SPANS_S: [u64; 3] = [10, 60, 300];
/// EMA span (seconds) of the Hyperliquid premium deviation.
pub const PDEV_HL_SPAN_S: u64 = 60;
/// Realized-vol windows (seconds) on the own mid, in feature order.
pub const RV_ARC_WINDOWS_S: [u64; 2] = [10, 60];
/// Realized-vol window (seconds) on the Binance mid.
pub const RV_BN_WINDOW_S: u64 = 60;
/// Lookback the study reads before a window (seconds); model files must
/// declare the same.
pub const LOOKBACK_S: u64 = 900;
/// Own L2 depth the imbalance features are defined on.
pub const OWN_L2_LEVELS: usize = 20;

/// Ring retention: the longest as-of lookback (120 s) plus slack. One record
/// older than the cutoff is always kept so an as-of at the cutoff resolves.
const RETAIN_MS: u64 = 150_000;
/// Longest realized-vol window in grid steps (60 s).
const RV_STEPS: usize = 600;

/// Directional features (sign-flipped per side by the model input), in the
/// exact order of `rf_common.DIR_NAMES`.
pub const DIR_NAMES: [&str; 29] = [
    "bn_ret_0.5s",
    "bn_ret_1s",
    "bn_ret_3s",
    "bn_ret_10s",
    "bn_ret_30s",
    "hl_ret_1s",
    "hl_ret_3s",
    "hl_ret_10s",
    "hl_ret_30s",
    "arc_ret_1s",
    "arc_ret_3s",
    "arc_ret_10s",
    "arc_ret_30s",
    "arc_ret_120s",
    "pdev_bn_10s",
    "pdev_bn_60s",
    "pdev_bn_300s",
    "pdev_hl_60s",
    "lead_bn_1s",
    "arc_top_imb",
    "arc_l2_imb_5bp",
    "arc_l2_imb_20lv",
    "arc_flow_5s",
    "arc_flow_30s",
    "bn_flow_1s",
    "bn_flow_5s",
    "bn_flow_30s",
    "bn_top_imb",
    "hl_flow_30s",
];
/// Non-directional features, in the exact order of `rf_common.ND_NAMES`.
pub const ND_NAMES: [&str; 8] = [
    "arc_spread_bp",
    "rv_arc_10s",
    "rv_arc_60s",
    "rv_bn_60s",
    "arc_ntr_30s",
    "bn_absflow_30s",
    "tod_sin",
    "tod_cos",
];
pub const N_DIR: usize = DIR_NAMES.len();
pub const N_ND: usize = ND_NAMES.len();
pub const N_FEATURES: usize = N_DIR + N_ND;

/// The model's input names: directional, non-directional, then `side`
/// (`rf_common.FEATURE_NAMES`).
pub fn model_feature_names() -> Vec<&'static str> {
    DIR_NAMES
        .iter()
        .chain(ND_NAMES.iter())
        .copied()
        .chain(std::iter::once("side"))
        .collect()
}

/// Side-independent raw features at one τ (`[F_dir, F_nd]`, f64 as computed).
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureVector {
    pub values: [f64; N_FEATURES],
}

impl FeatureVector {
    /// `rf_common.side_X`: `[s * F_dir, F_nd, s]` as f32 (sklearn casts its
    /// input to float32). `s` = +1 bid, −1 ask.
    pub fn side_input(&self, s: f64) -> Vec<f32> {
        let mut x: Vec<f32> = Vec::with_capacity(N_FEATURES + 1);
        for (i, v) in self.values.iter().enumerate() {
            let v = if i < N_DIR { s * v } else { *v };
            x.push(v as f32);
        }
        x.push(s as f32);
        x
    }
}

/// Why no feature vector could be formed at τ.
#[derive(Debug, Clone, PartialEq)]
pub enum FeatureGap {
    /// A stream has no record before `t` (named feature).
    NoRecord { name: &'static str },
    /// The grid EMA state is not initialised (no complete grid point yet),
    /// or τ precedes the grid origin.
    GridNotReady,
    /// A computed value is NaN / infinite.
    NonFinite { name: &'static str },
}

impl FeatureGap {
    pub fn feature(&self) -> &'static str {
        match self {
            FeatureGap::NoRecord { name } | FeatureGap::NonFinite { name } => name,
            FeatureGap::GridNotReady => "pdev",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct L1 {
    rx: u64,
    bid: f64,
    ask: f64,
    bsz: f64,
    asz: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Mid {
    rx: u64,
    mid: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Trade {
    rx: u64,
    /// +1 taker buy, −1 taker sell.
    sign: f64,
    px: f64,
    qty: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct L2 {
    rx: u64,
    /// `(price, size)` best first; sizes already rounded through f32 as the
    /// study stored them.
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

/// One row of the 0.1 s grid: log mids (own / Binance / HL).
#[derive(Debug, Clone, Copy)]
struct GridPoint {
    la: f64,
    lb: f64,
    lh: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct Ema {
    span_steps: u64,
    alpha: f64,
    y: f64,
    init: bool,
}

impl Ema {
    fn new(span_steps: u64) -> Self {
        Ema {
            span_steps,
            alpha: 2.0 / (span_steps as f64 + 1.0),
            y: 0.0,
            init: false,
        }
    }

    /// `lfilter([a], [1, a-1], x, zi=(1-a)*x[0])`: y[0] = x[0], then
    /// y[n] = a*x[n] + (1-a)*y[n-1].
    fn push(&mut self, x: f64) -> f64 {
        if !self.init {
            self.y = x;
            self.init = true;
        } else {
            self.y = self.alpha * x + (1.0 - self.alpha) * self.y;
        }
        self.y
    }
}

/// The engine. `origin_ms` anchors the 0.1 s grid (process start live; the
/// study's `lo - 899 s` in the golden test).
pub struct FeatureEngine {
    origin_ms: u64,
    own_l1: VecDeque<L1>,
    own_l2: VecDeque<L2>,
    own_trades: VecDeque<Trade>,
    bn_l1: VecDeque<L1>,
    bn_trades: VecDeque<Trade>,
    hl_l1: VecDeque<Mid>,
    hl_trades: VecDeque<Trade>,
    /// Grid points computed so far: index of the next one.
    grid_next: u64,
    /// The last COMPLETE grid point's inputs (for the diffs) and state.
    last_grid: Option<GridPoint>,
    pdev_bn: Option<[f64; 3]>,
    pdev_hl: Option<f64>,
    ema_bn: [Ema; 3],
    ema_hl: Ema,
    /// Squared grid diffs (bp²) of the own and Binance log mids, newest last,
    /// `RV_STEPS` long (zero for incomplete points, as the study's
    /// forward-filled grid has).
    rv_a: VecDeque<f64>,
    rv_b: VecDeque<f64>,
}

impl FeatureEngine {
    pub fn new(origin_ms: u64) -> Self {
        FeatureEngine {
            origin_ms,
            own_l1: VecDeque::new(),
            own_l2: VecDeque::new(),
            own_trades: VecDeque::new(),
            bn_l1: VecDeque::new(),
            bn_trades: VecDeque::new(),
            hl_l1: VecDeque::new(),
            hl_trades: VecDeque::new(),
            grid_next: 0,
            last_grid: None,
            pdev_bn: None,
            pdev_hl: None,
            ema_bn: [
                Ema::new(PDEV_BN_SPANS_S[0] * 1_000 / GRID_DT_MS),
                Ema::new(PDEV_BN_SPANS_S[1] * 1_000 / GRID_DT_MS),
                Ema::new(PDEV_BN_SPANS_S[2] * 1_000 / GRID_DT_MS),
            ],
            ema_hl: Ema::new(PDEV_HL_SPAN_S * 1_000 / GRID_DT_MS),
            rv_a: VecDeque::with_capacity(RV_STEPS + 1),
            rv_b: VecDeque::with_capacity(RV_STEPS + 1),
        }
    }

    // ------------------------------------------------------------ ingestion

    /// Own top of book: `rx_ms` is when the runtime received it.
    pub fn on_own_l1(&mut self, rx_ms: u64, bid: f64, ask: f64, bid_sz: f64, ask_sz: f64) {
        push_sorted(
            &mut self.own_l1,
            L1 {
                rx: rx_ms,
                bid,
                ask,
                bsz: bid_sz,
                asz: ask_sz,
            },
            |r| r.rx,
        );
    }

    /// Own L2 (best first, up to `OWN_L2_LEVELS` a side). Sizes are rounded
    /// through f32 here, as `rf_common` stores them.
    pub fn on_own_l2(&mut self, rx_ms: u64, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        let f32_sizes = |lv: &[(f64, f64)]| -> Vec<(f64, f64)> {
            lv.iter()
                .take(OWN_L2_LEVELS)
                .map(|(p, q)| (*p, f64::from(*q as f32)))
                .collect()
        };
        push_sorted(
            &mut self.own_l2,
            L2 {
                rx: rx_ms,
                bids: f32_sizes(bids),
                asks: f32_sizes(asks),
            },
            |r| r.rx,
        );
    }

    /// Own print; `taker_buy` is the aggressor side.
    pub fn on_own_trade(&mut self, rx_ms: u64, taker_buy: bool, px: f64, qty: f64) {
        push_sorted(
            &mut self.own_trades,
            Trade {
                rx: rx_ms,
                sign: if taker_buy { 1.0 } else { -1.0 },
                px,
                qty,
            },
            |r| r.rx,
        );
    }

    pub fn on_bn_l1(&mut self, rx_ms: u64, bid: f64, ask: f64, bid_sz: f64, ask_sz: f64) {
        push_sorted(
            &mut self.bn_l1,
            L1 {
                rx: rx_ms,
                bid,
                ask,
                bsz: bid_sz,
                asz: ask_sz,
            },
            |r| r.rx,
        );
    }

    /// Binance aggTrade; `maker_is_buyer` is the frame's `m` (true = the
    /// taker sold).
    pub fn on_bn_trade(&mut self, rx_ms: u64, maker_is_buyer: bool, px: f64, qty: f64) {
        push_sorted(
            &mut self.bn_trades,
            Trade {
                rx: rx_ms,
                sign: if maker_is_buyer { -1.0 } else { 1.0 },
                px,
                qty,
            },
            |r| r.rx,
        );
    }

    pub fn on_hl_l1(&mut self, rx_ms: u64, bid: f64, ask: f64) {
        push_sorted(
            &mut self.hl_l1,
            Mid {
                rx: rx_ms,
                mid: (bid + ask) / 2.0,
            },
            |r| r.rx,
        );
    }

    /// Hyperliquid trade; `buy` = side "B".
    pub fn on_hl_trade(&mut self, rx_ms: u64, buy: bool, px: f64, qty: f64) {
        push_sorted(
            &mut self.hl_trades,
            Trade {
                rx: rx_ms,
                sign: if buy { 1.0 } else { -1.0 },
                px,
                qty,
            },
            |r| r.rx,
        );
    }

    /// Receive time of the newest record per stream (for staleness).
    pub fn last_rx(&self) -> LastRx {
        LastRx {
            own_l1: self.own_l1.back().map(|r| r.rx),
            own_trade: self.own_trades.back().map(|r| r.rx),
            bn_l1: self.bn_l1.back().map(|r| r.rx),
            hl_l1: self.hl_l1.back().map(|r| r.rx),
        }
    }

    // --------------------------------------------------------------- grid

    /// Bring the grid up to the last point at or before `t_ms`. A point
    /// whose own / Binance / HL mids are not all available (no record before
    /// it) is incomplete: the EMAs are not advanced, a zero diff is recorded
    /// for the vols. Grid point k only ever uses records with
    /// `rx < origin + k·dt`, so calling this early (every gate cycle, warmup
    /// included — Codex round 2 on pairtrade#390) yields exactly the values a
    /// later call would: it keeps the EMA state accumulating while the
    /// rings are trimmed behind it, instead of initialising the 300 s EMA
    /// from the last 150 s of history at the first score.
    pub fn advance_grid(&mut self, t_ms: u64) {
        if t_ms < self.origin_ms {
            return;
        }
        let last_k = (t_ms - self.origin_ms) / GRID_DT_MS;
        while self.grid_next <= last_k {
            let g = self.origin_ms + self.grid_next * GRID_DT_MS;
            self.grid_next += 1;
            let point = match (
                asof(&self.own_l1, g, |r| r.rx).map(|r| ((r.bid + r.ask) / 2.0).ln()),
                asof(&self.bn_l1, g, |r| r.rx).map(|r| ((r.bid + r.ask) / 2.0).ln()),
                asof(&self.hl_l1, g, |r| r.rx).map(|r| r.mid.ln()),
            ) {
                (Some(la), Some(lb), Some(lh)) => GridPoint { la, lb, lh },
                _ => {
                    push_capped(&mut self.rv_a, 0.0, RV_STEPS);
                    push_capped(&mut self.rv_b, 0.0, RV_STEPS);
                    continue;
                }
            };
            let (da, db) = match self.last_grid {
                Some(prev) => ((point.la - prev.la) * 1e4, (point.lb - prev.lb) * 1e4),
                None => (0.0, 0.0),
            };
            push_capped(&mut self.rv_a, da * da, RV_STEPS);
            push_capped(&mut self.rv_b, db * db, RV_STEPS);
            let pb = (point.la - point.lb) * 1e4;
            let ph = (point.la - point.lh) * 1e4;
            let mut pdev = [0.0; 3];
            for (i, ema) in self.ema_bn.iter_mut().enumerate() {
                // Sign: + = own venue cheap vs the reference = bullish for it.
                pdev[i] = -(pb - ema.push(pb));
            }
            self.pdev_bn = Some(pdev);
            self.pdev_hl = Some(-(ph - self.ema_hl.push(ph)));
            self.last_grid = Some(point);
        }
    }

    /// Restart the grid at `origin_ms`: EMA / vol state is dropped (a feed
    /// gap forward-filled stale values into it) and warms up again from the
    /// first complete point after the new origin. The rings are kept.
    pub fn reset_grid(&mut self, origin_ms: u64) {
        self.origin_ms = origin_ms;
        self.grid_next = 0;
        self.last_grid = None;
        self.pdev_bn = None;
        self.pdev_hl = None;
        for e in self.ema_bn.iter_mut() {
            *e = Ema::new(e.span_steps);
        }
        self.ema_hl = Ema::new(self.ema_hl.span_steps);
        self.rv_a.clear();
        self.rv_b.clear();
    }

    fn rv(ring: &VecDeque<f64>, window_s: u64) -> f64 {
        let n = (window_s * 1_000 / GRID_DT_MS) as usize;
        let take = n.min(ring.len());
        let s: f64 = ring.iter().rev().take(take).sum();
        s.max(0.0).sqrt()
    }

    // ------------------------------------------------------------ features

    /// The raw feature vector at τ from records with `rx < τ` only. Also
    /// trims the rings to the retention window.
    pub fn features_at(&mut self, tau_ms: u64) -> Result<FeatureVector, FeatureGap> {
        self.advance_grid(tau_ms);
        self.trim(tau_ms);
        let mut v = [0.0f64; N_FEATURES];
        let mut i = 0usize;
        let mut put = |name: &'static str, x: f64| -> Result<(), FeatureGap> {
            if !x.is_finite() {
                return Err(FeatureGap::NonFinite { name });
            }
            v[i] = x;
            i += 1;
            Ok(())
        };
        let own_log =
            |t: u64| asof(&self.own_l1, t, |r| r.rx).map(|r| ((r.bid + r.ask) / 2.0).ln());
        let bn_log = |t: u64| asof(&self.bn_l1, t, |r| r.rx).map(|r| ((r.bid + r.ask) / 2.0).ln());
        let hl_log = |t: u64| asof(&self.hl_l1, t, |r| r.rx).map(|r| r.mid.ln());
        let ret = |f: &dyn Fn(u64) -> Option<f64>, h_ms: u64, name: &'static str| {
            let now = f(tau_ms).ok_or(FeatureGap::NoRecord { name })?;
            let then = f(tau_ms.saturating_sub(h_ms)).ok_or(FeatureGap::NoRecord { name })?;
            Ok::<f64, FeatureGap>((now - then) * 1e4)
        };
        let bn_ret = [
            ret(&bn_log, 500, "bn_ret_0.5s")?,
            ret(&bn_log, 1_000, "bn_ret_1s")?,
            ret(&bn_log, 3_000, "bn_ret_3s")?,
            ret(&bn_log, 10_000, "bn_ret_10s")?,
            ret(&bn_log, 30_000, "bn_ret_30s")?,
        ];
        let hl_ret = [
            ret(&hl_log, 1_000, "hl_ret_1s")?,
            ret(&hl_log, 3_000, "hl_ret_3s")?,
            ret(&hl_log, 10_000, "hl_ret_10s")?,
            ret(&hl_log, 30_000, "hl_ret_30s")?,
        ];
        let arc_ret = [
            ret(&own_log, 1_000, "arc_ret_1s")?,
            ret(&own_log, 3_000, "arc_ret_3s")?,
            ret(&own_log, 10_000, "arc_ret_10s")?,
            ret(&own_log, 30_000, "arc_ret_30s")?,
            ret(&own_log, 120_000, "arc_ret_120s")?,
        ];
        for (k, x) in bn_ret.iter().enumerate() {
            put(DIR_NAMES[k], *x)?;
        }
        for (k, x) in hl_ret.iter().enumerate() {
            put(DIR_NAMES[5 + k], *x)?;
        }
        for (k, x) in arc_ret.iter().enumerate() {
            put(DIR_NAMES[9 + k], *x)?;
        }
        let (pdev_bn, pdev_hl) = match (self.pdev_bn, self.pdev_hl) {
            (Some(b), Some(h)) if tau_ms >= self.origin_ms => (b, h),
            _ => return Err(FeatureGap::GridNotReady),
        };
        put("pdev_bn_10s", pdev_bn[0])?;
        put("pdev_bn_60s", pdev_bn[1])?;
        put("pdev_bn_300s", pdev_bn[2])?;
        put("pdev_hl_60s", pdev_hl)?;
        put("lead_bn_1s", bn_ret[1] - arc_ret[0])?;
        let l1 = asof(&self.own_l1, tau_ms, |r| r.rx).ok_or(FeatureGap::NoRecord {
            name: "arc_top_imb",
        })?;
        put(
            "arc_top_imb",
            (l1.bsz - l1.asz) / (l1.bsz + l1.asz).max(1e-12),
        )?;
        let (im5, im20) = match asof(&self.own_l2, tau_ms, |r| r.rx) {
            Some(l2) => l2_imbalances(l2),
            None => (0.0, 0.0),
        };
        put("arc_l2_imb_5bp", im5)?;
        put("arc_l2_imb_20lv", im20)?;
        put("arc_flow_5s", flow(&self.own_trades, tau_ms, 5_000, true))?;
        put("arc_flow_30s", flow(&self.own_trades, tau_ms, 30_000, true))?;
        put("bn_flow_1s", flow(&self.bn_trades, tau_ms, 1_000, true))?;
        put("bn_flow_5s", flow(&self.bn_trades, tau_ms, 5_000, true))?;
        put("bn_flow_30s", flow(&self.bn_trades, tau_ms, 30_000, true))?;
        let bn = asof(&self.bn_l1, tau_ms, |r| r.rx)
            .ok_or(FeatureGap::NoRecord { name: "bn_top_imb" })?;
        put(
            "bn_top_imb",
            (bn.bsz - bn.asz) / (bn.bsz + bn.asz).max(1e-12),
        )?;
        put("hl_flow_30s", flow(&self.hl_trades, tau_ms, 30_000, true))?;
        // Non-directional.
        put("arc_spread_bp", (l1.ask - l1.bid) / l1.bid * 1e4)?;
        put("rv_arc_10s", Self::rv(&self.rv_a, RV_ARC_WINDOWS_S[0]))?;
        put("rv_arc_60s", Self::rv(&self.rv_a, RV_ARC_WINDOWS_S[1]))?;
        put("rv_bn_60s", Self::rv(&self.rv_b, RV_BN_WINDOW_S))?;
        put("arc_ntr_30s", count(&self.own_trades, tau_ms, 30_000))?;
        put(
            "bn_absflow_30s",
            flow(&self.bn_trades, tau_ms, 30_000, false),
        )?;
        let hr = ((tau_ms as f64 / 1_000.0) % 86_400.0) / 3_600.0;
        let ang = 2.0 * std::f64::consts::PI * hr / 24.0;
        put("tod_sin", ang.sin())?;
        put("tod_cos", ang.cos())?;
        debug_assert_eq!(i, N_FEATURES);
        Ok(FeatureVector { values: v })
    }

    /// Drop history older than the retention window behind `tau_ms`. Called
    /// by `features_at` and by the gate on EVERY cycle, scored or not, so a
    /// gate that never scores (no model, long fallback) does not grow the
    /// rings without bound (Codex P1 on pairtrade#390).
    pub fn trim(&mut self, tau_ms: u64) {
        let cutoff = tau_ms.saturating_sub(RETAIN_MS);
        trim_before(&mut self.own_l1, cutoff, |r| r.rx);
        trim_before(&mut self.own_l2, cutoff, |r| r.rx);
        trim_before(&mut self.own_trades, cutoff, |r| r.rx);
        trim_before(&mut self.bn_l1, cutoff, |r| r.rx);
        trim_before(&mut self.bn_trades, cutoff, |r| r.rx);
        trim_before(&mut self.hl_l1, cutoff, |r| r.rx);
        trim_before(&mut self.hl_trades, cutoff, |r| r.rx);
    }
}

impl FeatureEngine {
    /// `(grid origin, next grid index, EMA initialised)` (tests).
    #[cfg(test)]
    pub fn grid_state(&self) -> (u64, u64, bool) {
        (self.origin_ms, self.grid_next, self.ema_bn[2].init)
    }

    /// Records held across all rings (tests: retention bound).
    #[cfg(test)]
    pub fn record_count(&self) -> usize {
        self.own_l1.len()
            + self.own_l2.len()
            + self.own_trades.len()
            + self.bn_l1.len()
            + self.bn_trades.len()
            + self.hl_l1.len()
            + self.hl_trades.len()
    }
}

/// Newest receive time per stream.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LastRx {
    pub own_l1: Option<u64>,
    pub own_trade: Option<u64>,
    pub bn_l1: Option<u64>,
    pub hl_l1: Option<u64>,
}

/// The last record with `rx < t`; `None` if there is none.
fn asof<T>(ring: &VecDeque<T>, t: u64, rx: impl Fn(&T) -> u64) -> Option<&T> {
    let n = ring.partition_point(|r| rx(r) < t);
    if n == 0 {
        None
    } else {
        ring.get(n - 1)
    }
}

/// Insert keeping the ring sorted by rx (records normally arrive in order;
/// a late one is placed where it belongs so the as-of stays correct).
fn push_sorted<T>(ring: &mut VecDeque<T>, item: T, rx: impl Fn(&T) -> u64) {
    let key = rx(&item);
    if ring.back().is_none_or(|b| rx(b) <= key) {
        ring.push_back(item);
        return;
    }
    let pos = ring.partition_point(|r| rx(r) <= key);
    ring.insert(pos, item);
}

fn push_capped(ring: &mut VecDeque<f64>, x: f64, cap: usize) {
    ring.push_back(x);
    while ring.len() > cap {
        ring.pop_front();
    }
}

/// Drop records older than `cutoff`, keeping the newest of them (the as-of
/// at the cutoff still resolves).
fn trim_before<T>(ring: &mut VecDeque<T>, cutoff: u64, rx: impl Fn(&T) -> u64) {
    while ring.len() >= 2 && rx(&ring[1]) < cutoff {
        ring.pop_front();
    }
}

/// Signed (or absolute) notional / 1e3 of trades with `t - h <= rx < t`.
fn flow(ring: &VecDeque<Trade>, t: u64, h_ms: u64, signed: bool) -> f64 {
    let lo = t.saturating_sub(h_ms);
    let mut s = 0.0;
    for r in ring.iter().rev() {
        if r.rx >= t {
            continue;
        }
        if r.rx < lo {
            break;
        }
        let v = r.px * r.qty / 1e3;
        s += if signed { r.sign * v } else { v };
    }
    s
}

fn count(ring: &VecDeque<Trade>, t: u64, h_ms: u64) -> f64 {
    let lo = t.saturating_sub(h_ms);
    ring.iter()
        .rev()
        .filter(|r| r.rx < t)
        .take_while(|r| r.rx >= lo)
        .count() as f64
}

/// `(mid ± 5 bp imbalance, all-level imbalance)` of one L2 snapshot, as
/// `rf_common` computes them (mid from the best levels; a side with no level
/// contributes 0).
fn l2_imbalances(l2: &L2) -> (f64, f64) {
    let (Some(bb), Some(ba)) = (l2.bids.first(), l2.asks.first()) else {
        return (0.0, 0.0);
    };
    let mid = (bb.0 + ba.0) / 2.0;
    let bd5: f64 = l2
        .bids
        .iter()
        .filter(|(p, _)| *p >= mid * (1.0 - 5e-4))
        .map(|(_, q)| q)
        .sum();
    let ad5: f64 = l2
        .asks
        .iter()
        .filter(|(p, _)| *p <= mid * (1.0 + 5e-4))
        .map(|(_, q)| q)
        .sum();
    let bd: f64 = l2.bids.iter().map(|(_, q)| q).sum();
    let ad: f64 = l2.asks.iter().map(|(_, q)| q).sum();
    (
        (bd5 - ad5) / (bd5 + ad5).max(1e-12),
        (bd - ad) / (bd + ad).max(1e-12),
    )
}

#[cfg(test)]
mod tests {
    use super::super::xtape::{self, Record};
    use super::*;

    const FIXTURE: &str = include_str!("testdata/xtape_slice.jsonl");
    const GOLDEN: &str = include_str!("testdata/golden_features.json");

    fn feed(engine: &mut FeatureEngine, records: &[Record]) {
        for r in records {
            xtape::apply(engine, r);
        }
    }

    fn golden() -> serde_json::Value {
        serde_json::from_str(GOLDEN).unwrap()
    }

    #[test]
    fn feature_names_match_the_study() {
        let names = model_feature_names();
        assert_eq!(names.len(), 38);
        let g = golden();
        let study: Vec<&str> = g["feature_names"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(&names[..37], &study[..]);
        assert_eq!(names[37], "side");
    }

    /// Golden test (design §4.3 a): the study's `rf_common.features()` on a
    /// real tape slice vs this engine fed the same records.
    #[test]
    fn golden_features_match_rf_common_on_a_real_tape_slice() {
        let g = golden();
        let origin = g["grid_origin_ms"].as_u64().unwrap();
        let records = xtape::parse_jsonl(FIXTURE).unwrap();
        assert!(records.len() > 1_000);
        let mut engine = FeatureEngine::new(origin);
        feed(&mut engine, &records);
        let rows = g["rows"].as_array().unwrap();
        assert!(rows.len() >= 60);
        let mut max_abs = 0.0f64;
        let mut nonzero = [false; N_FEATURES];
        let mut first_bid_x: Option<Vec<f32>> = None;
        for row in rows {
            let tau = row["tau_ms"].as_u64().unwrap();
            let want: Vec<f64> = row["features"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect();
            let got = engine
                .features_at(tau)
                .unwrap_or_else(|e| panic!("τ {tau}: {e:?}"));
            if first_bid_x.is_none() {
                first_bid_x = Some(got.side_input(1.0));
            }
            for (k, (a, b)) in got.values.iter().zip(want.iter()).enumerate() {
                let d = (a - b).abs();
                max_abs = max_abs.max(d);
                let tol = 1e-6 * b.abs().max(1.0);
                assert!(
                    d <= tol,
                    "τ {tau} {}: rust {a} vs python {b} (|Δ| {d})",
                    DIR_NAMES.iter().chain(ND_NAMES.iter()).nth(k).unwrap()
                );
                if *b != 0.0 {
                    nonzero[k] = true;
                }
            }
        }
        // Every feature is exercised by a non-zero golden value somewhere.
        for (k, nz) in nonzero.iter().enumerate() {
            assert!(
                nz,
                "feature {} is zero in every golden row: weak fixture",
                DIR_NAMES.iter().chain(ND_NAMES.iter()).nth(k).unwrap()
            );
        }
        assert!(max_abs < 1e-6, "max |Δ| {max_abs}");
        // The side-oriented f32 input for the first τ (bid) as sklearn sees it
        // (taken inside the loop: τ is monotone live, and the engine trims
        // history behind it).
        let x = first_bid_x.unwrap();
        let want: Vec<f32> = g["side_x_bid_first"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap() as f32)
            .collect();
        assert_eq!(x.len(), 38);
        for (a, b) in x.iter().zip(want.iter()) {
            assert!((a - b).abs() <= 1e-6 * b.abs().max(1.0), "{a} vs {b}");
        }
        assert_eq!(x[37], 1.0);
        let ask: Vec<f32> = x
            .iter()
            .enumerate()
            .map(|(i, v)| if i < N_DIR || i == 37 { -v } else { *v })
            .collect();
        let f = FeatureVector {
            values: rows[0]["features"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect::<Vec<f64>>()
                .try_into()
                .unwrap(),
        };
        assert_eq!(
            f.side_input(-1.0),
            ask,
            "ask input = directional (and side) flipped, non-directional kept"
        );
        assert_eq!(ask[37], -1.0);
        let x: Vec<f32> = x;
        assert_eq!(x.len(), 38);
    }

    /// No-lookahead (design §4.3 b, the port of `test_rf.py`): records at or
    /// after τ, however extreme, never change the features at τ. Covers a
    /// record stamped EXACTLY τ (`rx == τ` is excluded by the strict rule).
    #[test]
    fn records_at_or_after_tau_never_influence_features_at_tau() {
        let g = golden();
        let origin = g["grid_origin_ms"].as_u64().unwrap();
        let all = xtape::parse_jsonl(FIXTURE).unwrap();
        let tau = g["rows"][10]["tau_ms"].as_u64().unwrap();
        let before: Vec<Record> = all.iter().filter(|r| r.rx_ms < tau).cloned().collect();
        let mut a = FeatureEngine::new(origin);
        feed(&mut a, &before);
        let base = a.features_at(tau).unwrap();
        // Same records plus the future, with every future value mutated to
        // something absurd (price ×2, size ×1000, flipped sides), plus one
        // record of every stream stamped exactly at τ.
        let mut b = FeatureEngine::new(origin);
        feed(&mut b, &before);
        let at_tau = xtape::absurd_records(tau);
        assert!(at_tau.len() >= 7, "every stream gets a record at τ");
        for r in &at_tau {
            xtape::apply(&mut b, r);
        }
        for r in all.iter().filter(|r| r.rx_ms >= tau) {
            xtape::apply(&mut b, &xtape::make_absurd(r));
        }
        let got = b.features_at(tau).unwrap();
        assert_eq!(got, base, "a record with rx >= τ influenced the features");
        // ... and an engine fed the whole (unmodified) tape, future included,
        // before its first query answers the same at τ.
        let mut c = FeatureEngine::new(origin);
        feed(&mut c, &all);
        assert_eq!(c.features_at(tau).unwrap(), base);
        // The future really would have changed things had it leaked: one
        // grid step later the absurd records are inside the window.
        assert_ne!(b.features_at(tau + 100).unwrap(), base);
    }

    #[test]
    fn missing_history_is_a_gap_not_a_zero() {
        let g = golden();
        let origin = g["grid_origin_ms"].as_u64().unwrap();
        let all = xtape::parse_jsonl(FIXTURE).unwrap();
        let mut e = FeatureEngine::new(origin);
        // Only the first 20 s: a 30 s return has no record before τ − 30 s.
        let cut = origin + 20_000;
        let early: Vec<Record> = all.iter().filter(|r| r.rx_ms < cut).cloned().collect();
        feed(&mut e, &early);
        let err = e.features_at(cut - 50).unwrap_err();
        assert!(
            matches!(err, FeatureGap::NoRecord { name } if name.ends_with("_30s") || name.ends_with("_10s")),
            "{err:?}"
        );
        // Before the grid origin: no pdev.
        let mut e2 = FeatureEngine::new(origin + 400_000);
        feed(&mut e2, &all);
        assert!(matches!(
            e2.features_at(origin + 150_050),
            Err(FeatureGap::GridNotReady)
        ));
        // No records at all.
        let mut e3 = FeatureEngine::new(origin);
        assert!(matches!(
            e3.features_at(origin + 150_050),
            Err(FeatureGap::NoRecord { .. })
        ));
    }

    #[test]
    fn asof_is_strict_and_late_records_are_sorted_in() {
        let mut e = FeatureEngine::new(0);
        e.on_bn_l1(1_000, 100.0, 101.0, 1.0, 1.0);
        e.on_bn_l1(3_000, 102.0, 103.0, 1.0, 1.0);
        e.on_bn_l1(2_000, 104.0, 105.0, 1.0, 1.0); // late
        let mids: Vec<u64> = e.bn_l1.iter().map(|r| r.rx).collect();
        assert_eq!(mids, vec![1_000, 2_000, 3_000]);
        assert_eq!(asof(&e.bn_l1, 3_000, |r| r.rx).unwrap().rx, 2_000);
        assert_eq!(asof(&e.bn_l1, 3_001, |r| r.rx).unwrap().rx, 3_000);
        assert!(asof(&e.bn_l1, 1_000, |r| r.rx).is_none());
        // Trim keeps the newest record older than the cutoff.
        e.on_bn_l1(RETAIN_MS + 2_500, 1.0, 1.0, 1.0, 1.0);
        e.trim(RETAIN_MS + 2_500);
        let left: Vec<u64> = e.bn_l1.iter().map(|r| r.rx).collect();
        assert_eq!(left, vec![2_000, 3_000, RETAIN_MS + 2_500]);
    }

    #[test]
    fn ema_matches_lfilter_initialisation() {
        let mut e = Ema::new(600);
        assert_eq!(e.push(3.0), 3.0);
        let a = 2.0 / 601.0;
        assert!((e.push(5.0) - (a * 5.0 + (1.0 - a) * 3.0)).abs() < 1e-15);
    }
}
