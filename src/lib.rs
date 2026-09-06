// src/lib.rs
#[cfg(feature = "arcus-spot-sdk")]
pub mod arcus_spot;
pub mod book;
pub mod config;
pub mod directional;
pub mod email_client;
pub mod error_counter;
pub mod infra;
pub mod rate_limit_notifier;
pub mod trade;
