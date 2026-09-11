//! Slow cross-sectional book runtime (bot-strategy#937).
//!
//! Holds a book of single-symbol perp legs on one venue, rebalances it to
//! an externally produced target-weight vector at discrete decision times,
//! confirms fills against the venue position, and records every step on
//! disk. The strategy-shaped pieces are pure and unit-tested:
//!
//! - [`config`]: YAML schema + validation + fingerprint.
//! - [`signal`]: the producer → runtime file contract (schema v1) and its
//!   fail-closed validation.
//! - [`schedule`]: decision windows (`interval_days` / `daily` / `calendar`)
//!   and fixed-window flattens.
//! - [`rebalance`]: target weights + current book + prices → ordered
//!   `OrderIntent`s, with lot rounding, dust handling, and portfolio caps.
//! - [`risk`]: kill switch, session drawdown halt (sticky, RISK_ACK), daily
//!   loss halt.
//!
//! The side-effecting layers are thin and shared between DRY_RUN, live and
//! replay:
//!
//! - [`executor`]: `Executor` trait with a paper implementation and a
//!   `DexConnector`-backed live implementation.
//! - [`state`], [`ledger`], [`status`]: `state.json`, `ledger.jsonl` /
//!   `pnl.jsonl`, `status.json` (+ S3 mirror).
//! - [`engine`]: the tick orchestration used by `src/bin/book_runtime.rs`
//!   and by [`replay`].
//!
//! The spec this implements is `docs/book-runtime.md`; keep them in sync.

pub mod config;
pub mod engine;
pub mod executor;
pub mod ledger;
pub mod rebalance;
pub mod replay;
pub mod risk;
pub mod schedule;
pub mod signal;
pub mod state;
pub mod status;

pub use config::BookConfig;
pub use engine::BookEngine;
