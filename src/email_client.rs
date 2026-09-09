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

/// A set environment variable whose value is blank is not a credential.
/// `EnvironmentFile=` lines like `GMAIL_TO=` (a placeholder someone meant
/// to fill in) set the variable to the empty string, which `env::var` then
/// reports as present -- so treating "set" as "configured" would pass the
/// startup gate and hand `EmailClient` an empty recipient that fails at
/// send time, which is the exact silence bot-strategy#968 is about
/// (pairtrade#301 Codex review).
fn non_blank_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.trim().is_empty())
}

impl EmailClient {
    /// Whether the three environment variables `new()` needs are all
    /// present *and* non-blank. Callers that treat notifications as a
    /// precondition (an unattended bot whose only alert path is e-mail)
    /// can check this at startup instead of discovering it from a `WARN`
    /// at the first send that mattered -- bot-strategy#968, where
    /// `engine-b-live` ran for days silently dropping every entry, exit
    /// and halt notification. Uses exactly the same rule as `new()`, so
    /// the two can never disagree about whether this client can send.
    pub fn is_configured() -> bool {
        non_blank_env("GMAIL_USER").is_some()
            && (non_blank_env("GMAIL_TO").is_some() || non_blank_env("TO_ADDRESS").is_some())
            && non_blank_env("GMAIL_APP_PASSWORD").is_some()
    }

    pub fn new() -> Self {
        let from_address = non_blank_env("GMAIL_USER");
        // Prefer GMAIL_TO (matches stock-signal-bot convention); fall back to
        // legacy TO_ADDRESS for backwards compatibility with older env files.
        let to_address = non_blank_env("GMAIL_TO").or_else(|| non_blank_env("TO_ADDRESS"));
        let app_password = non_blank_env("GMAIL_APP_PASSWORD");

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

#[cfg(test)]
mod tests {
    use super::non_blank_env;
    use std::env;

    /// Env vars are process-global, so this walks every case inside one
    /// test rather than racing sibling tests over the same key.
    #[test]
    fn a_blank_value_is_not_a_credential() {
        const KEY: &str = "PAIRTRADE_TEST_NON_BLANK_ENV";
        // Safety: single-threaded within this test, and the key is unique
        // to it -- no other test reads or writes it.
        unsafe {
            env::remove_var(KEY);
            assert_eq!(non_blank_env(KEY), None, "unset");
            env::set_var(KEY, "");
            assert_eq!(non_blank_env(KEY), None, "empty");
            env::set_var(KEY, "   ");
            assert_eq!(non_blank_env(KEY), None, "whitespace only");
            env::set_var(KEY, "someone@example.com");
            assert_eq!(
                non_blank_env(KEY),
                Some("someone@example.com".to_string()),
                "a real value passes through unchanged"
            );
            env::remove_var(KEY);
        }
    }
}
