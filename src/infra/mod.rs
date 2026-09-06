//! Runtime-agnostic operational plumbing shared by every bot binary in this
//! crate (bot-strategy#937).
//!
//! The BTC/ETH pairtrade engine that used to own these pieces was sunset on
//! 2026-09-06 and removed from the tree. What survives here is the part of
//! its operational stack that is independent of the strategy shape:
//!
//! - [`logger`]: the `TIMEZONE_OFFSET` / `RUST_LOG` aware `env_logger`
//!   setup wrapped in the [`crate::error_counter`] counting layer.
//! - [`prom`]: process-wide Prometheus registry, the opt-in `PROM_LISTEN`
//!   `/metrics` exporter, and the process-start / version gauges.
//! - [`s3_mirror`]: fire-and-forget S3 mirror for `status.json`-style
//!   files consumed by `debot-dashboard`.
//!
//! On-disk conventions (atomic JSON writes, JSONL append, KILL_SWITCH /
//! RISK_ACK sentinels, the DRY_RUN refusal guard, config fingerprints) live
//! in [`crate::directional`], which predates this module and is re-exported
//! here as [`fs`] so new code has one obvious import path.

pub mod logger;
pub mod prom;
pub mod s3_mirror;

pub use crate::directional as fs;
