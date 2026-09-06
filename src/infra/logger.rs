//! Shared `env_logger` initialisation.
//!
//! Every binary in this crate used to carry its own copy of this function
//! (`main.rs`, `engine_b_live.rs`, `bull_holder.rs`, ...). The behaviour is
//! identical across them: timestamps are rendered in the fixed offset given
//! by `TIMEZONE_OFFSET` (seconds east of UTC, default `0`), the filter comes
//! from `RUST_LOG` (default `info`, per-module directives honoured, see
//! bot-strategy#194), and the logger is wrapped in
//! [`crate::error_counter::ErrorCountingLogger`] so `status.json` writers can
//! surface error / warn counts.

use std::io::Write as _;

use chrono::{DateTime, FixedOffset, Utc};

use crate::error_counter::{self, ErrorCountingLogger};

/// Default `RUST_LOG` filter when the variable is unset.
pub const DEFAULT_FILTER: &str = "info,tokio_tungstenite=info,tungstenite=info";

/// Install the process-global logger. Safe to call more than once: a second
/// call is a no-op because `log::set_boxed_logger` rejects it.
pub fn init_logger() {
    init_logger_with_default_offset(0);
}

/// Same as [`init_logger`], with a different fallback for `TIMEZONE_OFFSET`.
pub fn init_logger_with_default_offset(default_offset_secs: i32) {
    let offset_seconds = std::env::var("TIMEZONE_OFFSET")
        .ok()
        .and_then(|v| v.trim().parse::<i32>().ok())
        .unwrap_or(default_offset_secs);
    let offset = FixedOffset::east_opt(offset_seconds)
        .unwrap_or_else(|| FixedOffset::east_opt(0).expect("zero offset is valid"));
    let env = env_logger::Env::default().filter_or("RUST_LOG", DEFAULT_FILTER);
    let inner = env_logger::Builder::from_env(env)
        .format(move |buf, record| {
            let utc_now: DateTime<Utc> = Utc::now();
            let local_now = utc_now.with_timezone(&offset);
            writeln!(
                buf,
                "{} [{}] - {}",
                local_now.format("%Y-%m-%dT%H:%M:%S%z"),
                record.level(),
                record.args()
            )
        })
        .build();
    let max_level = inner.filter();
    let (logger, handle) = ErrorCountingLogger::wrap(Box::new(inner));
    error_counter::install_global(handle);
    if log::set_boxed_logger(Box::new(logger)).is_ok() {
        log::set_max_level(max_level);
    }
}
