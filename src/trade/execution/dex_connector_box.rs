use async_trait::async_trait;
#[cfg(feature = "extended-sdk")]
use dex_connector::create_extended_connector;
#[cfg(feature = "arcus-sdk")]
use dex_connector::{create_arcus_connector, ArcusConnectorConfig};
#[cfg(feature = "hyperliquid-sdk")]
use dex_connector::{create_hyperliquid_connector, HyperliquidConnectorConfig};
#[cfg(feature = "lighter-sdk")]
use dex_connector::{create_lighter_connector, LighterConnector, LighterConnectorConfig};
use dex_connector::{
    BalanceResponse, CanceledOrdersResponse, CreateOrderResponse, DexConnector, DexError,
    FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse, OrderBookSnapshot, OrderSide,
    TickerResponse, TpSl, TriggerOrderStyle,
};

use rust_decimal::Decimal;

#[cfg(feature = "extended-sdk")]
use crate::config::get_extended_config_from_env;
#[cfg(feature = "lighter-sdk")]
use crate::config::get_lighter_config_from_env;
use crate::rate_limit_notifier::{notify_lighter_waf_cooldown, notify_rate_limit};
use lazy_static::lazy_static;
use std::env;

lazy_static! {
    static ref FILLED_PROBABILITY_IN_EMULATION: Decimal = {
        match env::var("FILLED_PROBABILITY_IN_EMULATION") {
            Ok(val) => val.parse::<Decimal>().unwrap_or(Decimal::new(1, 0)),
            Err(_) => Decimal::new(1, 0),
        }
    };
}

/// True only when the error text refers to HTTP 429: either the canonical
/// reason phrase, or "429" as a standalone number. Digits embedded in a
/// larger number must NOT match — dex-connector's stale-price transient
/// error ("websocket price for GBPUSD is stale (429785ms old) and REST
/// fallback is disabled") kept tripping the old `contains("429")` check
/// whenever the millisecond age happened to contain the digits 429, spamming
/// false "HTTP 429" alert emails.
fn mentions_http_429(text: &str) -> bool {
    if text.contains("Too Many Requests") {
        return true;
    }
    let bytes = text.as_bytes();
    let mut start = 0;
    while let Some(pos) = text[start..].find("429") {
        let i = start + pos;
        let digit_before = i > 0 && bytes[i - 1].is_ascii_digit();
        let digit_after = bytes.get(i + 3).is_some_and(|b| b.is_ascii_digit());
        if !digit_before && !digit_after {
            return true;
        }
        start = i + 3;
    }
    false
}

pub struct DexConnectorBox {
    pub inner: Box<dyn DexConnector>,
}

impl DexConnectorBox {
    fn report_rate_limit(&self, operation: &str, detail: &str, err: &DexError) {
        // New structured form of the Lighter WAF cooldown (HTTP 405 +
        // x-amzn-waf-action: captcha or HTTP 429). Send a single deduped email
        // per engagement event across all bot processes on this host. See
        // bot-strategy#35.
        if let DexError::RateLimited { until_unix } = err {
            let context = format!("{} ({})", operation, detail);
            notify_lighter_waf_cooldown(*until_unix, &context);
            return;
        }
        let err_text = err.to_string();
        if mentions_http_429(&err_text) {
            let context = format!("{} ({})", operation, detail);
            notify_rate_limit(&context, &err_text);
        }
    }

    // instance_id is only read from the lighter-sdk arm; extended-sdk-only
    // builds (Tokyo, bot-strategy#123) don't consume it.
    #[cfg_attr(not(feature = "lighter-sdk"), allow(unused_variables))]
    pub async fn create(
        dex_name: &str,
        dry_run: bool,
        token_list: &[String],
        // Optional instance id for the multi-strategy single-process
        // architecture (shigeo-nakamura/bot-strategy#25). When `Some`, the
        // Lighter env loader prefers credentials suffixed with this id so
        // each strategy variant can target its own sub-account. `None`
        // preserves single-instance behavior.
        instance_id: Option<&str>,
    ) -> Result<Self, DexError> {
        match dex_name {
            #[cfg(feature = "lighter-sdk")]
            "lighter" => {
                let lighter_config = match get_lighter_config_from_env(instance_id).await {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(DexError::Permanent(e.to_string()));
                    }
                };

                let mut account_index = lighter_config.account_index;

                // Auto-discover account_index if not set (0 = not configured)
                if account_index == 0 {
                    let wallet_address =
                        lighter_config.wallet_address.as_deref().ok_or_else(|| {
                            DexError::Permanent(
                                "LIGHTER_ACCOUNT_INDEX not set and LIGHTER_WALLET_ADDRESS not set. \
                                 Set one of them to enable account discovery."
                                    .to_string(),
                            )
                        })?;
                    log::info!(
                        "LIGHTER_ACCOUNT_INDEX not set, discovering for api_key_index={}...",
                        lighter_config.api_key_index
                    );
                    let tmp_config = LighterConnectorConfig {
                        api_key_public: lighter_config.api_key.clone(),
                        api_key_index: lighter_config.api_key_index,
                        api_private_key_hex: lighter_config.private_key.clone(),
                        evm_wallet_private_key: lighter_config.evm_wallet_private_key.clone(),
                        account_index: 0,
                        base_url: lighter_config.base_url.clone(),
                        websocket_url: lighter_config.websocket_url.clone(),
                        tracked_symbols: vec![],
                        ob_stale_secs: None,
                    };
                    let tmp_connector = LighterConnector::new(tmp_config)?;
                    account_index = tmp_connector.discover_account_index(wallet_address).await?;
                }

                let connector_config = LighterConnectorConfig {
                    api_key_public: lighter_config.api_key,
                    api_key_index: lighter_config.api_key_index,
                    api_private_key_hex: lighter_config.private_key,
                    evm_wallet_private_key: lighter_config.evm_wallet_private_key,
                    account_index,
                    base_url: lighter_config.base_url,
                    websocket_url: lighter_config.websocket_url,
                    tracked_symbols: token_list.to_vec(),
                    ob_stale_secs: None, // use default
                };

                if dry_run {
                    let connector = LighterConnector::new(connector_config)?;
                    Ok(DexConnectorBox {
                        inner: Box::new(connector),
                    })
                } else {
                    let connector = create_lighter_connector(connector_config)?;
                    Ok(DexConnectorBox { inner: connector })
                }
            }
            #[cfg(feature = "extended-sdk")]
            "extended" => {
                let extended_config = get_extended_config_from_env()
                    .await
                    .map_err(|e| DexError::Permanent(e.to_string()))?;

                let connector = create_extended_connector(
                    extended_config.api_key,
                    extended_config.public_key,
                    extended_config.private_key,
                    extended_config.vault,
                    extended_config.base_url,
                    extended_config.websocket_url,
                    token_list.to_vec(),
                )
                .await?;

                Ok(DexConnectorBox { inner: connector })
            }
            #[cfg(feature = "arcus-sdk")]
            "arcus" => {
                if !dry_run {
                    return Err(DexError::Permanent(
                        "Arcus support is read-only; set DRY_RUN=true".to_string(),
                    ));
                }
                let base_url = env::var("ARCUS_REST_ENDPOINT")
                    .ok()
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "https://api.arcus.xyz".to_string());
                let websocket_url = env::var("ARCUS_WEBSOCKET_ENDPOINT")
                    .ok()
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "wss://api.arcus.xyz/v1/ws".to_string());
                let connector = create_arcus_connector(ArcusConnectorConfig {
                    base_url,
                    websocket_url,
                    tracked_symbols: token_list.to_vec(),
                    ob_stale_secs: None,
                })?;
                Ok(DexConnectorBox { inner: connector })
            }
            #[cfg(feature = "hyperliquid-sdk")]
            "hyperliquid" => {
                let base_url = env::var("REST_ENDPOINT")
                    .ok()
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| "https://api.hyperliquid.xyz".to_string());
                let connector = create_hyperliquid_connector(HyperliquidConnectorConfig {
                    base_url,
                    tracked_symbols: token_list.to_vec(),
                })?;
                Ok(DexConnectorBox { inner: connector })
            }
            _ => Err(DexError::Permanent(format!("Unsupported dex: {dex_name}"))),
        }
    }
}

#[async_trait]
impl DexConnector for DexConnectorBox {
    async fn start(&self) -> Result<(), DexError> {
        let result = self.inner.start().await;
        if let Err(ref err) = result {
            self.report_rate_limit("start", "connector", err);
        }
        result
    }

    async fn stop(&self) -> Result<(), DexError> {
        let result = self.inner.stop().await;
        if let Err(ref err) = result {
            self.report_rate_limit("stop", "connector", err);
        }
        result
    }

    async fn restart(&self, max_retries: i32) -> Result<(), DexError> {
        let result = self.inner.restart(max_retries).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "restart",
                &format!("connector | retries={}", max_retries),
                err,
            );
        }
        result
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), DexError> {
        let result = self.inner.set_leverage(symbol, leverage).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "set_leverage",
                &format!("{} | leverage={}", symbol, leverage),
                err,
            );
        }
        result
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        let result = self.inner.get_ticker(symbol, test_price).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_ticker", symbol, err);
        }
        result
    }

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        let result = self.inner.get_filled_orders(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_filled_orders", symbol, err);
        }
        result
    }

    async fn get_canceled_orders(&self, symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        let result = self.inner.get_canceled_orders(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_canceled_orders", symbol, err);
        }
        result
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        let result = self.inner.get_open_orders(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_open_orders", symbol, err);
        }
        result
    }

    async fn get_balance(&self, symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        let detail = symbol.unwrap_or("ALL");
        let result = self.inner.get_balance(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_balance", detail, err);
        }
        result
    }

    async fn get_last_trades(&self, symbol: &str) -> Result<LastTradesResponse, DexError> {
        let result = self.inner.get_last_trades(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_last_trades", symbol, err);
        }
        result
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        let result = self.inner.get_order_book(symbol, depth).await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_order_book", symbol, err);
        }
        result
    }

    async fn clear_filled_order(&self, symbol: &str, trade_id: &str) -> Result<(), DexError> {
        let result = self.inner.clear_filled_order(symbol, trade_id).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "clear_filled_order",
                &format!("{} | trade_id={}", symbol, trade_id),
                err,
            );
        }
        result
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        let result = self.inner.clear_all_filled_orders().await;
        if let Err(ref err) = result {
            self.report_rate_limit("clear_all_filled_orders", "all", err);
        }
        result
    }

    async fn clear_canceled_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let result = self.inner.clear_canceled_order(symbol, order_id).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "clear_canceled_order",
                &format!("{} | order_id={}", symbol, order_id),
                err,
            );
        }
        result
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        let result = self.inner.clear_all_canceled_orders().await;
        if let Err(ref err) = result {
            self.report_rate_limit("clear_all_canceled_orders", "all", err);
        }
        result
    }

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        spread: Option<i64>,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        let result = self
            .inner
            .create_order(symbol, size, side, price, spread, reduce_only, expiry_secs)
            .await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "create_order",
                &format!("{} | side={:?} size={}", symbol, side, size),
                err,
            );
        }
        result
    }

    async fn create_advanced_trigger_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        trigger_px: Decimal,
        limit_px: Option<Decimal>,
        order_style: TriggerOrderStyle,
        slippage_bps: Option<u32>,
        tpsl: TpSl,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        let result = self
            .inner
            .create_advanced_trigger_order(
                symbol,
                size,
                side,
                trigger_px,
                limit_px,
                order_style,
                slippage_bps,
                tpsl,
                reduce_only,
                expiry_secs,
            )
            .await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "create_advanced_trigger_order",
                &format!(
                    "{} | side={:?} size={} trigger_px={}",
                    symbol, side, size, trigger_px
                ),
                err,
            );
        }
        result
    }

    // bot-strategy#397: explicit forward so the wrapper does not silently
    // fall back to the trait default (`DexError::Permanent("not
    // implemented for this connector")`) the moment pairtrade wires this
    // path in. No active caller in pairtrade today — Extended-only at the
    // connector layer (bot-strategy#302) — but the latent trap is
    // exactly the class of bug flagged by feedback_dex_connector_box_forward.md
    // (auto-memory).
    async fn create_order_taker_ioc(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        slippage_bps: u32,
        reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        let result = self
            .inner
            .create_order_taker_ioc(symbol, size, side, slippage_bps, reduce_only)
            .await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "create_order_taker_ioc",
                &format!(
                    "{} | side={:?} size={} slippage_bps={}",
                    symbol, side, size, slippage_bps
                ),
                err,
            );
        }
        result
    }

    // bot-strategy#471: explicit forward so the amend path reaches the
    // connector impl instead of silently degrading to the trait default
    // (`DexError::Permanent`). Per feedback_dex_connector_box_forward.md.
    #[allow(clippy::too_many_arguments)] // mirrors the DexConnector::modify_order signature.
    async fn modify_order(
        &self,
        symbol: &str,
        order_id: &str,
        side: OrderSide,
        target_total_size: Decimal,
        open_remaining_size: Decimal,
        price: Option<Decimal>,
        spread: Option<i64>,
        reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        let result = self
            .inner
            .modify_order(
                symbol,
                order_id,
                side,
                target_total_size,
                open_remaining_size,
                price,
                spread,
                reduce_only,
            )
            .await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "modify_order",
                &format!(
                    "{} | order_id={} side={:?} remaining={}",
                    symbol, order_id, side, open_remaining_size
                ),
                err,
            );
        }
        result
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let result = self.inner.cancel_order(symbol, order_id).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "cancel_order",
                &format!("{} | order_id={}", symbol, order_id),
                err,
            );
        }
        result
    }

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError> {
        let detail = symbol.as_deref().unwrap_or("ALL").to_string();
        let result = self.inner.cancel_all_orders(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("cancel_all_orders", &detail, err);
        }
        result
    }

    async fn cancel_orders(
        &self,
        symbol: Option<String>,
        order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        let order_count = order_ids.len();
        let detail = format!(
            "{} | orders={}",
            symbol.as_deref().unwrap_or("ALL"),
            order_count
        );
        let result = self.inner.cancel_orders(symbol, order_ids).await;
        if let Err(ref err) = result {
            self.report_rate_limit("cancel_orders", &detail, err);
        }
        result
    }

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError> {
        let detail = symbol.as_deref().unwrap_or("ALL").to_string();
        let result = self.inner.close_all_positions(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("close_all_positions", &detail, err);
        }
        result
    }

    async fn clear_last_trades(&self, symbol: &str) -> Result<(), DexError> {
        let result = self.inner.clear_last_trades(symbol).await;
        if let Err(ref err) = result {
            self.report_rate_limit("clear_last_trades", symbol, err);
        }
        result
    }

    async fn is_upcoming_maintenance(&self, hours_ahead: i64) -> bool {
        self.inner.is_upcoming_maintenance(hours_ahead).await
    }

    async fn maintenance_status(&self, hours_ahead: i64) -> Option<String> {
        self.inner.maintenance_status(hours_ahead).await
    }

    async fn sign_evm_65b(&self, message: &str) -> Result<String, DexError> {
        let result = self.inner.sign_evm_65b(message).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "sign_evm_65b",
                &format!("message_len={}", message.len()),
                err,
            );
        }
        result
    }

    async fn sign_evm_65b_with_eip191(&self, message: &str) -> Result<String, DexError> {
        let result = self.inner.sign_evm_65b_with_eip191(message).await;
        if let Err(ref err) = result {
            self.report_rate_limit(
                "sign_evm_65b_with_eip191",
                &format!("message_len={}", message.len()),
                err,
            );
        }
        result
    }

    async fn get_combined_balance(
        &self,
    ) -> Result<dex_connector::CombinedBalanceResponse, DexError> {
        let result = self.inner.get_combined_balance().await;
        if let Err(ref err) = result {
            self.report_rate_limit("get_combined_balance", "all", err);
        }
        result
    }

    async fn get_positions(&self) -> Result<Vec<dex_connector::PositionSnapshot>, DexError> {
        self.inner.get_positions().await
    }

    fn subscribe_price_updates(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<dex_connector::PriceUpdate>, DexError> {
        self.inner.subscribe_price_updates()
    }
}

#[cfg(test)]
mod tests {
    use super::mentions_http_429;
    #[cfg(feature = "arcus-sdk")]
    use super::DexConnectorBox;

    // Regression: the stale-WS-price transient error must never be reported
    // as a rate limit, no matter what the millisecond age happens to be.
    #[test]
    fn stale_price_ages_containing_429_digits_do_not_match() {
        for age in ["429785", "42992", "34294", "44295", "54294", "14298023"] {
            let msg = format!(
                "websocket price for GBPUSD is stale ({age}ms old) and REST fallback is disabled"
            );
            assert!(!mentions_http_429(&msg), "false positive for {msg}");
        }
    }

    #[test]
    fn genuine_http_429_shapes_match() {
        assert!(mentions_http_429("HTTP 429: rate limited"));
        assert!(mentions_http_429("HTTP error: 429 Too Many Requests"));
        assert!(mentions_http_429("Too Many Requests"));
        assert!(mentions_http_429("server returned status 429"));
        assert!(mentions_http_429("(429)"));
    }

    #[test]
    fn unrelated_text_does_not_match() {
        assert!(!mentions_http_429("filled 1429 lots"));
        assert!(!mentions_http_429("order id 42900 accepted"));
        assert!(!mentions_http_429("everything is fine"));
    }

    #[cfg(feature = "arcus-sdk")]
    #[tokio::test]
    async fn arcus_factory_accepts_read_only_dry_run() {
        let connector = DexConnectorBox::create(
            "arcus",
            true,
            &["BTC-USD".to_string(), "ETH".to_string()],
            None,
        )
        .await;

        assert!(connector.is_ok());
    }

    #[cfg(feature = "arcus-sdk")]
    #[tokio::test]
    async fn arcus_factory_rejects_live_mode() {
        let error = DexConnectorBox::create("arcus", false, &["BTC-USD".to_string()], None)
            .await
            .err()
            .expect("live Arcus factory must fail");

        assert!(error.to_string().contains("read-only"));
    }
}
