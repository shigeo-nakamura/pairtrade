//! Inventory-funded Arcus Spot strategy runtime.
//!
//! The first vertical slice is deliberately incapable of signing or submitting
//! a swap. It consumes the public, indicative recorder schema from
//! dex-connector and can either emit read-only plans or apply those plans to
//! an isolated replay inventory.

#[cfg(feature = "arcus-spot-live")]
mod chain;
mod config;
#[cfg(feature = "arcus-spot-live")]
mod execution_ledger;
#[cfg(feature = "arcus-spot-live")]
mod kms;
mod replay;
mod runtime;

#[cfg(feature = "arcus-spot-live")]
pub use chain::*;
pub use config::*;
#[cfg(feature = "arcus-spot-live")]
pub use execution_ledger::*;
#[cfg(feature = "arcus-spot-live")]
pub use kms::*;
pub use replay::*;
pub use runtime::*;
