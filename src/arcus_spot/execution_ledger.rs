use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use dex_connector::ArcusSpotSwapStatus;
use ethers::types::{Address, H256, U256};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    str::FromStr,
};

const EXECUTION_LEDGER_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotExecutionPhase {
    Prepared,
    Dispatching,
    Submitted,
    Confirmed,
    Rejected,
    Failed,
    Unknown,
    OperatorHold,
    Reconciled,
}

impl ArcusSpotExecutionPhase {
    pub fn blocks_new_execution(self) -> bool {
        !matches!(self, Self::Reconciled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotExecutionIntent {
    pub venue: String,
    pub sell_token: String,
    pub buy_token: String,
    pub sell_amount_raw: String,
    pub minimum_buy_amount_raw: String,
}

impl ArcusSpotExecutionIntent {
    fn validate(&self) -> Result<(Address, Address, U256, U256)> {
        if !self.venue.eq_ignore_ascii_case("arcus") {
            bail!("initial live execution requires venue=arcus");
        }
        let sell_token =
            Address::from_str(&self.sell_token).context("invalid execution sell_token")?;
        let buy_token =
            Address::from_str(&self.buy_token).context("invalid execution buy_token")?;
        if sell_token == Address::zero() || buy_token == Address::zero() {
            bail!("execution token addresses must not be zero");
        }
        if sell_token == buy_token {
            bail!("execution token addresses must be distinct");
        }
        let sell_amount = U256::from_dec_str(&self.sell_amount_raw)
            .context("invalid execution sell_amount_raw")?;
        let minimum_buy = U256::from_dec_str(&self.minimum_buy_amount_raw)
            .context("invalid execution minimum_buy_amount_raw")?;
        if sell_amount.is_zero() || minimum_buy.is_zero() {
            bail!("execution amounts must be positive");
        }
        Ok((sell_token, buy_token, sell_amount, minimum_buy))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotBalanceSnapshot {
    pub observed_at: DateTime<Utc>,
    pub sell_token: String,
    pub buy_token: String,
    pub sell_balance_raw: String,
    pub buy_balance_raw: String,
    pub gas_balance_wei: String,
}

impl ArcusSpotBalanceSnapshot {
    fn validate_for(&self, sell_token: Address, buy_token: Address) -> Result<(U256, U256, U256)> {
        let observed_sell =
            Address::from_str(&self.sell_token).context("invalid balance sell_token")?;
        let observed_buy =
            Address::from_str(&self.buy_token).context("invalid balance buy_token")?;
        if observed_sell != sell_token || observed_buy != buy_token {
            bail!("balance snapshot token addresses do not match execution intent");
        }
        Ok((
            U256::from_dec_str(&self.sell_balance_raw).context("invalid sell_balance_raw")?,
            U256::from_dec_str(&self.buy_balance_raw).context("invalid buy_balance_raw")?,
            U256::from_dec_str(&self.gas_balance_wei).context("invalid gas_balance_wei")?,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotExecutionAttempt {
    pub sequence: u64,
    pub idempotency_key: String,
    pub payload_hash: String,
    pub prepared_at: DateTime<Utc>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub phase: ArcusSpotExecutionPhase,
    pub intent: ArcusSpotExecutionIntent,
    pub pre_balances: ArcusSpotBalanceSnapshot,
    pub post_balances: Option<ArcusSpotBalanceSnapshot>,
    pub tx_hash: Option<String>,
    pub router_status: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotExecutionLedger {
    pub schema_version: u32,
    pub next_sequence: u64,
    pub active: Option<ArcusSpotExecutionAttempt>,
    pub history: Vec<ArcusSpotExecutionAttempt>,
}

fn validate_payload_hash(payload_hash: &str) -> Result<()> {
    let digest = payload_hash
        .strip_prefix("sha256:")
        .context("payload hash must use sha256: prefix")?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("payload hash must contain exactly 64 lowercase hexadecimal characters");
    }
    Ok(())
}

impl Default for ArcusSpotExecutionLedger {
    fn default() -> Self {
        Self {
            schema_version: EXECUTION_LEDGER_SCHEMA_VERSION,
            next_sequence: 1,
            active: None,
            history: Vec::new(),
        }
    }
}

impl ArcusSpotExecutionLedger {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != EXECUTION_LEDGER_SCHEMA_VERSION {
            bail!(
                "unsupported Arcus execution ledger schema {}; expected {}",
                self.schema_version,
                EXECUTION_LEDGER_SCHEMA_VERSION
            );
        }
        if self.next_sequence == 0 {
            bail!("Arcus execution ledger next_sequence must be non-zero");
        }
        let mut previous = 0;
        for attempt in self.history.iter().chain(self.active.iter()) {
            if attempt.sequence <= previous {
                bail!("Arcus execution attempt sequence is not strictly increasing");
            }
            previous = attempt.sequence;
            attempt.intent.validate()?;
            let (sell_token, buy_token, _, _) = attempt.intent.validate()?;
            attempt.pre_balances.validate_for(sell_token, buy_token)?;
            if let Some(post) = &attempt.post_balances {
                post.validate_for(sell_token, buy_token)?;
            }
            validate_payload_hash(&attempt.payload_hash)?;
            let hash_suffix = &attempt.payload_hash["sha256:".len()..][..16];
            let expected_key = format!("arcus-spot-{:020}-{hash_suffix}", attempt.sequence);
            if attempt.idempotency_key != expected_key {
                bail!("Arcus execution attempt has invalid identity fields");
            }
        }
        if self.next_sequence <= previous {
            bail!("Arcus execution ledger next_sequence is not ahead of attempts");
        }
        Ok(())
    }

    pub fn prepare(
        &mut self,
        payload_hash: impl Into<String>,
        intent: ArcusSpotExecutionIntent,
        pre_balances: ArcusSpotBalanceSnapshot,
        now: DateTime<Utc>,
    ) -> Result<&ArcusSpotExecutionAttempt> {
        if let Some(active) = &self.active {
            bail!(
                "Arcus execution blocked by active sequence {} in phase {:?}",
                active.sequence,
                active.phase
            );
        }
        let payload_hash = payload_hash.into();
        validate_payload_hash(&payload_hash)?;
        let (sell_token, buy_token, _, _) = intent.validate()?;
        pre_balances.validate_for(sell_token, buy_token)?;
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .context("Arcus execution sequence overflow")?;
        let hash_suffix = payload_hash
            .strip_prefix("sha256:")
            .unwrap_or_default()
            .chars()
            .take(16)
            .collect::<String>();
        self.active = Some(ArcusSpotExecutionAttempt {
            sequence,
            idempotency_key: format!("arcus-spot-{sequence:020}-{hash_suffix}"),
            payload_hash,
            prepared_at: now,
            dispatched_at: None,
            updated_at: now,
            phase: ArcusSpotExecutionPhase::Prepared,
            intent,
            pre_balances,
            post_balances: None,
            tx_hash: None,
            router_status: None,
            detail: None,
        });
        Ok(self.active.as_ref().expect("active set above"))
    }

    /// Must be persisted before calling POST /v1/submit.
    pub fn mark_dispatching(&mut self, now: DateTime<Utc>) -> Result<()> {
        let active = self.active_mut()?;
        if active.phase != ArcusSpotExecutionPhase::Prepared {
            bail!("only a prepared Arcus attempt can dispatch");
        }
        active.phase = ArcusSpotExecutionPhase::Dispatching;
        active.dispatched_at = Some(now);
        active.updated_at = now;
        Ok(())
    }

    pub fn record_submit_status(
        &mut self,
        status: &ArcusSpotSwapStatus,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let active = self.active_mut()?;
        if active.phase != ArcusSpotExecutionPhase::Dispatching {
            bail!("submit response requires a dispatching Arcus attempt");
        }
        apply_status(active, status, now)?;
        Ok(())
    }

    pub fn record_submit_unknown(
        &mut self,
        detail: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let active = self.active_mut()?;
        if active.phase != ArcusSpotExecutionPhase::Dispatching {
            bail!("unknown submit result requires a dispatching Arcus attempt");
        }
        active.phase = ArcusSpotExecutionPhase::Unknown;
        active.updated_at = now;
        active.detail = Some(detail.into());
        Ok(())
    }

    pub fn record_submit_rejected(
        &mut self,
        detail: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let active = self.active_mut()?;
        if active.phase != ArcusSpotExecutionPhase::Dispatching {
            bail!("submit rejection requires a dispatching Arcus attempt");
        }
        active.phase = ArcusSpotExecutionPhase::Rejected;
        active.updated_at = now;
        active.detail = Some(detail.into());
        Ok(())
    }

    /// Status GETs may resume after restart, but submission never does.
    pub fn record_polled_status(
        &mut self,
        status: &ArcusSpotSwapStatus,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let active = self.active_mut()?;
        if !matches!(
            active.phase,
            ArcusSpotExecutionPhase::Submitted | ArcusSpotExecutionPhase::Confirmed
        ) {
            bail!(
                "status polling is not allowed in Arcus phase {:?}",
                active.phase
            );
        }
        apply_status(active, status, now)?;
        Ok(())
    }

    /// Reconcile exact sold amount and minimum received amount against wallet
    /// balances. Any mismatch is converted to sticky UNKNOWN before returning.
    pub fn reconcile_balances(
        &mut self,
        post: ArcusSpotBalanceSnapshot,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let active = self.active_mut()?;
        if active.phase != ArcusSpotExecutionPhase::Confirmed {
            bail!("only a confirmed Arcus attempt can reconcile balances");
        }
        let (sell_token, buy_token, expected_sell, minimum_buy) = active.intent.validate()?;
        let (pre_sell, pre_buy, _) = active.pre_balances.validate_for(sell_token, buy_token)?;
        let (post_sell, post_buy, _) = post.validate_for(sell_token, buy_token)?;
        active.post_balances = Some(post);
        active.updated_at = now;

        let result = (|| -> Result<()> {
            let sold = pre_sell
                .checked_sub(post_sell)
                .context("sell balance increased or underflowed")?;
            let bought = post_buy
                .checked_sub(pre_buy)
                .context("buy balance decreased or underflowed")?;
            if sold != expected_sell {
                bail!("sell balance delta {sold} does not equal signed amount {expected_sell}");
            }
            if bought < minimum_buy {
                bail!("buy balance delta {bought} is below signed minimum {minimum_buy}");
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                active.phase = ArcusSpotExecutionPhase::Reconciled;
                active.detail = None;
                Ok(())
            }
            Err(error) => {
                active.phase = ArcusSpotExecutionPhase::Unknown;
                active.detail = Some(format!("balance reconciliation failed: {error:#}"));
                Err(error)
            }
        }
    }

    /// Explicitly archive a reconciled attempt. UNKNOWN and failed/rejected
    /// attempts remain active until an operator resolves them.
    pub fn archive_reconciled(&mut self) -> Result<()> {
        let active = self
            .active
            .take()
            .context("no active Arcus execution attempt")?;
        if active.phase != ArcusSpotExecutionPhase::Reconciled {
            self.active = Some(active);
            bail!("only a reconciled Arcus attempt can be archived");
        }
        self.history.push(active);
        Ok(())
    }

    /// Startup is non-mutating: a prepared signed payload is held for an
    /// operator, while an in-flight POST becomes sticky UNKNOWN.
    pub fn recover_after_restart(&mut self, now: DateTime<Utc>) -> bool {
        let Some(active) = self.active.as_mut() else {
            return false;
        };
        match active.phase {
            ArcusSpotExecutionPhase::Prepared => {
                active.phase = ArcusSpotExecutionPhase::OperatorHold;
                active.updated_at = now;
                active.detail = Some(
                    "restart found a prepared signed payload; automatic submission is forbidden"
                        .to_string(),
                );
                true
            }
            ArcusSpotExecutionPhase::Dispatching => {
                active.phase = ArcusSpotExecutionPhase::Unknown;
                active.updated_at = now;
                active.detail = Some(
                    "restart occurred after durable dispatch marker and before a submit response"
                        .to_string(),
                );
                true
            }
            _ => false,
        }
    }

    fn active_mut(&mut self) -> Result<&mut ArcusSpotExecutionAttempt> {
        self.active
            .as_mut()
            .context("no active Arcus execution attempt")
    }
}

fn apply_status(
    active: &mut ArcusSpotExecutionAttempt,
    status: &ArcusSpotSwapStatus,
    now: DateTime<Utc>,
) -> Result<()> {
    if !status.venue.eq_ignore_ascii_case(&active.intent.venue) {
        bail!("router status venue does not match execution intent");
    }
    let tx_hash = H256::from_str(&status.tx_hash).context("invalid router txHash")?;
    if tx_hash == H256::zero() {
        bail!("router returned a zero txHash for an attempted swap");
    }
    if let Some(previous) = &active.tx_hash {
        let previous = H256::from_str(previous).context("invalid stored txHash")?;
        if previous != tx_hash {
            active.phase = ArcusSpotExecutionPhase::Unknown;
            active.detail = Some("router changed txHash for an active attempt".to_string());
            bail!("router txHash changed across status observations");
        }
    }
    active.tx_hash = Some(format!("{tx_hash:#x}"));
    active.router_status = Some(status.status.clone());
    active.updated_at = now;
    active.detail = status.reason.clone().or_else(|| status.error_code.clone());
    active.phase = if status.is_confirmed() {
        ArcusSpotExecutionPhase::Confirmed
    } else if status.is_failed() {
        ArcusSpotExecutionPhase::Failed
    } else if status.is_unknown() {
        ArcusSpotExecutionPhase::Unknown
    } else {
        ArcusSpotExecutionPhase::Submitted
    };
    Ok(())
}

/// Process-wide ownership of one execution ledger.
pub struct ArcusSpotExecutionLedgerLock {
    _file: File,
}

pub struct ArcusSpotExecutionLedgerStore {
    path: PathBuf,
}

impl ArcusSpotExecutionLedgerStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Claim the ledger for one executor process. The separate lock inode is
    /// stable even though ledger persistence atomically renames the JSON file.
    pub fn acquire_exclusive_lock(&self) -> Result<ArcusSpotExecutionLedgerLock> {
        let parent = self
            .path
            .parent()
            .context("Arcus execution ledger path has no parent")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .context("Arcus execution ledger path has no valid file name")?;
        let lock_path = parent.join(format!(".{file_name}.lock"));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&lock_path)
            .with_context(|| format!("failed to open Arcus ledger lock {}", lock_path.display()))?;
        let metadata = file.metadata().with_context(|| {
            format!(
                "failed to inspect Arcus ledger lock {}",
                lock_path.display()
            )
        })?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
            bail!(
                "Arcus execution ledger lock {} must be a mode-0600 regular file",
                lock_path.display()
            );
        }
        FileExt::try_lock_exclusive(&file).with_context(|| {
            format!(
                "another Arcus executor already holds ledger lock {}",
                lock_path.display()
            )
        })?;
        Ok(ArcusSpotExecutionLedgerLock { _file: file })
    }

    pub fn load_or_create(&self, now: DateTime<Utc>) -> Result<ArcusSpotExecutionLedger> {
        if !self.path.exists() {
            let ledger = ArcusSpotExecutionLedger::default();
            self.persist(&ledger)?;
            return Ok(ledger);
        }
        let metadata = fs::symlink_metadata(&self.path)
            .with_context(|| format!("failed to inspect {}", self.path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!(
                "Arcus execution ledger {} must be a regular non-symlink file",
                self.path.display()
            );
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!(
                "Arcus execution ledger {} must not be readable or writable by group/other",
                self.path.display()
            );
        }
        let bytes = fs::read(&self.path)
            .with_context(|| format!("failed to read {}", self.path.display()))?;
        let mut ledger: ArcusSpotExecutionLedger = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid ledger {}", self.path.display()))?;
        ledger.validate()?;
        if ledger.recover_after_restart(now) {
            self.persist(&ledger)?;
        }
        Ok(ledger)
    }

    pub fn persist(&self, ledger: &ArcusSpotExecutionLedger) -> Result<()> {
        ledger.validate()?;
        let parent = self
            .path
            .parent()
            .context("Arcus execution ledger path has no parent")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let temp = parent.join(format!(
            ".{}.tmp.{}.{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("ledger"),
            std::process::id(),
            stamp
        ));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&temp)
                .with_context(|| format!("failed to create {}", temp.display()))?;
            serde_json::to_writer_pretty(&mut file, ledger)
                .context("failed to serialize Arcus execution ledger")?;
            file.write_all(b"\n")
                .context("failed to terminate Arcus execution ledger")?;
            file.sync_all()
                .context("failed to fsync Arcus execution ledger")?;
            fs::rename(&temp, &self.path).with_context(|| {
                format!(
                    "failed to atomically replace {} with {}",
                    self.path.display(),
                    temp.display()
                )
            })?;
            File::open(parent)
                .with_context(|| format!("failed to open {}", parent.display()))?
                .sync_all()
                .context("failed to fsync Arcus execution ledger directory")?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn intent() -> ArcusSpotExecutionIntent {
        ArcusSpotExecutionIntent {
            venue: "arcus".to_string(),
            sell_token: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            buy_token: "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string(),
            sell_amount_raw: "1000".to_string(),
            minimum_buy_amount_raw: "980".to_string(),
        }
    }

    fn balances(sell: &str, buy: &str, at: DateTime<Utc>) -> ArcusSpotBalanceSnapshot {
        ArcusSpotBalanceSnapshot {
            observed_at: at,
            sell_token: intent().sell_token,
            buy_token: intent().buy_token,
            sell_balance_raw: sell.to_string(),
            buy_balance_raw: buy.to_string(),
            gas_balance_wei: "1000000000000000".to_string(),
        }
    }

    fn status(kind: &str) -> ArcusSpotSwapStatus {
        ArcusSpotSwapStatus {
            venue: "arcus".to_string(),
            status: kind.to_string(),
            tx_hash: format!("{:#x}", H256::from_low_u64_be(1)),
            reason: None,
            error_code: None,
            swap: None,
            extra: Default::default(),
        }
    }

    #[test]
    fn restart_after_dispatch_is_sticky_unknown() {
        let now = Utc::now();
        let mut ledger = ArcusSpotExecutionLedger::default();
        ledger
            .prepare(
                format!("sha256:{}", "a".repeat(64)),
                intent(),
                balances("5000", "2000", now),
                now,
            )
            .unwrap();
        ledger.mark_dispatching(now).unwrap();
        assert!(ledger.recover_after_restart(now));
        assert_eq!(
            ledger.active.unwrap().phase,
            ArcusSpotExecutionPhase::Unknown
        );
    }

    #[test]
    fn duplicate_prepare_is_blocked() {
        let now = Utc::now();
        let mut ledger = ArcusSpotExecutionLedger::default();
        ledger
            .prepare(
                format!("sha256:{}", "a".repeat(64)),
                intent(),
                balances("5000", "2000", now),
                now,
            )
            .unwrap();
        assert!(ledger
            .prepare(
                format!("sha256:{}", "b".repeat(64)),
                intent(),
                balances("5000", "2000", now),
                now,
            )
            .is_err());
    }

    #[test]
    fn exact_balance_delta_reconciles() {
        let now = Utc::now();
        let mut ledger = ArcusSpotExecutionLedger::default();
        ledger
            .prepare(
                format!("sha256:{}", "a".repeat(64)),
                intent(),
                balances("5000", "2000", now),
                now,
            )
            .unwrap();
        ledger.mark_dispatching(now).unwrap();
        ledger
            .record_submit_status(&status("confirmed"), now)
            .unwrap();
        ledger
            .reconcile_balances(balances("4000", "2985", now), now)
            .unwrap();
        assert_eq!(
            ledger.active.as_ref().unwrap().phase,
            ArcusSpotExecutionPhase::Reconciled
        );
    }

    #[test]
    fn partial_balance_change_becomes_unknown() {
        let now = Utc::now();
        let mut ledger = ArcusSpotExecutionLedger::default();
        ledger
            .prepare(
                format!("sha256:{}", "a".repeat(64)),
                intent(),
                balances("5000", "2000", now),
                now,
            )
            .unwrap();
        ledger.mark_dispatching(now).unwrap();
        ledger
            .record_submit_status(&status("confirmed"), now)
            .unwrap();
        assert!(ledger
            .reconcile_balances(balances("4500", "2985", now), now)
            .is_err());
        assert_eq!(
            ledger.active.as_ref().unwrap().phase,
            ArcusSpotExecutionPhase::Unknown
        );
    }

    #[test]
    fn store_lock_serializes_executor_processes() {
        let dir = tempdir().unwrap();
        let store = ArcusSpotExecutionLedgerStore::new(dir.path().join("ledger.json"));
        let first = store.acquire_exclusive_lock().unwrap();

        let error = store.acquire_exclusive_lock().err().unwrap();
        assert!(error
            .to_string()
            .contains("another Arcus executor already holds"));
        drop(first);

        let second = store.acquire_exclusive_lock().unwrap();
        let mode = fs::metadata(dir.path().join(".ledger.json.lock"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        drop(second);
    }

    #[test]
    fn store_round_trip_preserves_and_recovers_state() {
        let dir = tempdir().unwrap();
        let store = ArcusSpotExecutionLedgerStore::new(dir.path().join("ledger.json"));
        let now = Utc::now();
        let mut ledger = store.load_or_create(now).unwrap();
        ledger
            .prepare(
                format!("sha256:{}", "a".repeat(64)),
                intent(),
                balances("5000", "2000", now),
                now,
            )
            .unwrap();
        ledger.mark_dispatching(now).unwrap();
        store.persist(&ledger).unwrap();

        let loaded = store.load_or_create(now).unwrap();
        assert_eq!(
            loaded.active.as_ref().unwrap().phase,
            ArcusSpotExecutionPhase::Unknown
        );
        assert_eq!(
            fs::metadata(store.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    #[test]
    fn store_rejects_group_readable_ledger() {
        let dir = tempdir().unwrap();
        let store = ArcusSpotExecutionLedgerStore::new(dir.path().join("ledger.json"));
        let now = Utc::now();
        store.load_or_create(now).unwrap();
        let mut permissions = fs::metadata(store.path()).unwrap().permissions();
        permissions.set_mode(0o640);
        fs::set_permissions(store.path(), permissions).unwrap();
        assert!(store.load_or_create(now).is_err());
    }

    #[test]
    fn rejects_truncated_payload_hash() {
        assert!(validate_payload_hash("sha256:abc").is_err());
        assert!(validate_payload_hash(&format!("sha256:{}", "A".repeat(64))).is_err());
    }
}
