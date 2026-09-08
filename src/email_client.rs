use lettre::message::{Message, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{SmtpTransport, Transport};
use std::env;

pub struct EmailClient {
    mailer: Option<SmtpTransport>,
    from_address: Option<String>,
    to_address: Option<String>,
}

impl Default for EmailClient {
    fn default() -> Self {
        Self::new()
    }
}

impl EmailClient {
    /// Whether the three environment variables `new()` needs are all
    /// present. Callers that treat notifications as a precondition (an
    /// unattended bot whose only alert path is e-mail) can check this at
    /// startup instead of discovering it from a `WARN` at the first send
    /// that mattered -- bot-strategy#968, where `engine-b-live` ran for
    /// days silently dropping every entry, exit and halt notification.
    pub fn is_configured() -> bool {
        env::var("GMAIL_USER").is_ok()
            && (env::var("GMAIL_TO").is_ok() || env::var("TO_ADDRESS").is_ok())
            && env::var("GMAIL_APP_PASSWORD").is_ok()
    }

    pub fn new() -> Self {
        let from_address = env::var("GMAIL_USER").ok();
        // Prefer GMAIL_TO (matches stock-signal-bot convention); fall back to
        // legacy TO_ADDRESS for backwards compatibility with older env files.
        let to_address = env::var("GMAIL_TO")
            .ok()
            .or_else(|| env::var("TO_ADDRESS").ok());
        let app_password = env::var("GMAIL_APP_PASSWORD").ok();

        if let (Some(from_address), Some(to_address), Some(app_password)) =
            (from_address, to_address, app_password)
        {
            let creds = Credentials::new(from_address.clone(), app_password);
            let mailer = SmtpTransport::starttls_relay("smtp.gmail.com")
                .unwrap()
                .credentials(creds)
                .build();

            EmailClient {
                mailer: Some(mailer),
                from_address: Some(from_address),
                to_address: Some(to_address),
            }
        } else {
            log::warn!("Failed to create EmailClient: missing credentials");
            EmailClient {
                mailer: None,
                from_address: None,
                to_address: None,
            }
        }
    }

    pub fn send(&self, subject: &str, body: &str) {
        if let Some(mailer) = &self.mailer {
            let from_address = self.from_address.as_ref().expect("from_address is missing");
            let to_address = self.to_address.as_ref().expect("to_address is missing");
            let email = Message::builder()
                .from(from_address.parse().unwrap())
                .to(to_address.parse().unwrap())
                .subject(subject)
                .singlepart(SinglePart::plain(body.to_string()))
                .unwrap();

            if let Err(e) = mailer.send(&email) {
                log::warn!("Failed to send an e-mail: {:?}", e);
            }
        } else {
            log::warn!("No mailer available to send the email");
        }
    }
}
