//! Backtest plumbing extracted from the monolithic pairtrade module. Right
//! now this only owns the connector construction fork (replay vs live), but
//! is the natural home for any future backtest-only helpers (data loading,
//! replay-clock policy, …).

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use dex_connector::DexConnector;

use super::config::PairTradeConfig;
use crate::ports::replay_dex::ReplayConnector;
use crate::trade::execution::dex_connector_box::DexConnectorBox;

/// Build the connector pair for `PairTradeEngine::new`. In backtest mode this
/// returns the same `ReplayConnector` as both the active connector and the
/// replay handle so the engine can advance the replay clock through it; in
/// live mode the second slot is `None`.
pub(super) async fn create_connector(
    cfg: &PairTradeConfig,
) -> Result<(
    Arc<dyn DexConnector + Send + Sync>,
    Option<Arc<ReplayConnector>>,
)> {
    if cfg.backtest_mode {
        let replay = Arc::new(ReplayConnector::new(
            cfg.backtest_file.as_ref().unwrap().as_str(),
        )?);
        return Ok((replay.clone(), Some(replay)));
    }

    let tokens: Vec<String> = cfg
        .universe
        .iter()
        .flat_map(|p| [p.base.clone(), p.quote.clone()])
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let live_connector = DexConnectorBox::create(
        &cfg.dex_name,
        &cfg.rest_endpoint,
        &cfg.web_socket_endpoint,
        cfg.dry_run,
        cfg.agent_name.clone(),
        &tokens,
    )
    .await
    .context("failed to initialize connector")?;
    live_connector
        .start()
        .await
        .context("failed to start connector")?;
    Ok((Arc::new(live_connector), None))
}
