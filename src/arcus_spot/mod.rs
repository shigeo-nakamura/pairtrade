//! Inventory-funded Arcus Spot strategy runtime.
//!
//! The first vertical slice is deliberately incapable of signing or submitting
//! a swap. It consumes the public, indicative recorder schema from
//! dex-connector and can either emit read-only plans or apply those plans to
//! an isolated replay inventory.

#[cfg(feature = "arcus-spot-live")]
mod chain;
#[cfg(feature = "arcus-spot-live")]
mod checkpoint;
mod config;
#[cfg(feature = "arcus-spot-live")]
mod event_stream;
#[cfg(feature = "arcus-spot-live")]
mod execution_ledger;
#[cfg(feature = "arcus-spot-live")]
mod kms;
#[cfg(feature = "arcus-spot-live")]
mod live_executor;
mod replay;
mod runtime;

#[cfg(feature = "arcus-spot-live")]
pub use chain::*;
#[cfg(feature = "arcus-spot-live")]
pub use checkpoint::*;
pub use config::*;
#[cfg(feature = "arcus-spot-live")]
pub use event_stream::*;
#[cfg(feature = "arcus-spot-live")]
pub use execution_ledger::*;
#[cfg(feature = "arcus-spot-live")]
pub use kms::*;
#[cfg(feature = "arcus-spot-live")]
pub use live_executor::*;
pub use replay::*;
pub use runtime::*;
