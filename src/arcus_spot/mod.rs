//! Inventory-funded Arcus Spot strategy runtime.
//!
//! The first vertical slice is deliberately incapable of signing or submitting
//! a swap. It consumes the public, indicative recorder schema from
//! dex-connector and can either emit read-only plans or apply those plans to
//! an isolated replay inventory.

mod config;
mod replay;
mod runtime;

pub use config::*;
pub use replay::*;
pub use runtime::*;
