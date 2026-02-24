use crate::email_client::EmailClient;
use once_cell::sync::Lazy;

static RATE_LIMIT_NOTIFIER: Lazy<RateLimitNotifier> = Lazy::new(RateLimitNotifier::new);

pub fn notify_rate_limit(context: &str, detail: &str) {
    RATE_LIMIT_NOTIFIER.notify(context, detail);
}

struct RateLimitNotifier {
    token_name: String,
}

impl RateLimitNotifier {
    fn new() -> Self {
        let token_name = std::env::var("SYMBOLS")
            .or_else(|_| std::env::var("SYMBOL"))
            .unwrap_or_default();
        Self { token_name }
    }

    fn notify(&self, context: &str, detail: &str) {
        let subject = if self.token_name.is_empty() {
            format!("[RateLimit] {}", context)
        } else {
            format!("[{}] Rate limit - {}", self.token_name, context)
        };
        let body = format!(
            "HTTP 429 Too Many Requests detected while {}.\nDetail: {}",
            context, detail
        );

        EmailClient::new().send(&subject, &body);
        log::warn!(
            "📧 [RateLimit] Email notification sent for '{}' (detail: {})",
            context,
            detail
        );
    }
}
