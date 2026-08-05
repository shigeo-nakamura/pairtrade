use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use ethers::{
    signers::Signer,
    types::{
        transaction::{eip2718::TypedTransaction, eip712::Eip712},
        Address, Signature as EthereumSignature, U256,
    },
    utils::keccak256,
};
use k256::ecdsa::{RecoveryId, Signature as K256Signature, VerifyingKey};
use rusoto_core::Region;
use rusoto_kms::{GetPublicKeyRequest, Kms, KmsClient, SignRequest};
use serde::{Deserialize, Serialize};
use spki::SubjectPublicKeyInfoRef;
use std::{fmt, str::FromStr, sync::Arc};
use thiserror::Error;

/// Dedicated asymmetric AWS KMS signer configuration for Arcus Spot.
///
/// The key must be an ECC_SECG_P256K1 SIGN_VERIFY key. Its private key never
/// leaves KMS; only GetPublicKey and Sign permissions are required.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotKmsConfig {
    pub region: String,
    pub key_id: String,
    pub chain_id: u64,
    pub expected_address: String,
}

impl fmt::Debug for ArcusSpotKmsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArcusSpotKmsConfig")
            .field("region", &self.region)
            .field("key_id", &"<redacted>")
            .field("chain_id", &self.chain_id)
            .field("expected_address", &self.expected_address)
            .finish()
    }
}

impl ArcusSpotKmsConfig {
    pub fn validate(&self) -> Result<(Region, Address)> {
        if self.key_id.trim().is_empty() {
            bail!("Arcus KMS key_id must not be empty");
        }
        if self.chain_id == 0 {
            bail!("Arcus KMS chain_id must be non-zero");
        }
        let region = Region::from_str(self.region.trim())
            .with_context(|| format!("invalid Arcus KMS region {:?}", self.region))?;
        let expected_address = Address::from_str(self.expected_address.trim())
            .context("invalid Arcus KMS expected_address")?;
        if expected_address == Address::zero() {
            bail!("Arcus KMS expected_address must not be zero");
        }
        Ok((region, expected_address))
    }
}

#[derive(Debug, Error)]
pub enum ArcusSpotKmsSignerError {
    #[error("{0}")]
    Operation(String),
    #[error("Arcus KMS signer refuses non-EIP-712 signing")]
    UnsupportedSigningMode,
}

/// Minimal EIP-712-only secp256k1 signer backed by AWS KMS.
///
/// Unlike ethers' broad AWS signer, this intentionally refuses arbitrary
/// messages and transactions. Arcus execution needs only the validated quote
/// and exact-value EIP-2612 typed data.
#[derive(Clone)]
pub struct ArcusSpotKmsSigner {
    kms: Arc<KmsClient>,
    key_id: String,
    chain_id: u64,
    verifying_key: VerifyingKey,
    address: Address,
}

impl fmt::Debug for ArcusSpotKmsSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArcusSpotKmsSigner")
            .field("key_id", &"<redacted>")
            .field("chain_id", &self.chain_id)
            .field("address", &self.address)
            .finish()
    }
}

impl ArcusSpotKmsSigner {
    async fn new(
        kms: KmsClient,
        key_id: impl Into<String>,
        chain_id: u64,
    ) -> std::result::Result<Self, ArcusSpotKmsSignerError> {
        let key_id = key_id.into();
        let response = kms
            .get_public_key(GetPublicKeyRequest {
                grant_tokens: None,
                key_id: key_id.clone(),
            })
            .await
            .map_err(|error| {
                ArcusSpotKmsSignerError::Operation(format!("KMS GetPublicKey failed: {error}"))
            })?;
        let raw = response.public_key.ok_or_else(|| {
            ArcusSpotKmsSignerError::Operation("KMS GetPublicKey omitted public_key".to_string())
        })?;
        let public_key = SubjectPublicKeyInfoRef::try_from(raw.as_ref()).map_err(|error| {
            ArcusSpotKmsSignerError::Operation(format!("invalid KMS SubjectPublicKeyInfo: {error}"))
        })?;
        let verifying_key = VerifyingKey::from_sec1_bytes(
            public_key.subject_public_key.raw_bytes(),
        )
        .map_err(|error| {
            ArcusSpotKmsSignerError::Operation(format!("invalid KMS secp256k1 public key: {error}"))
        })?;
        let address = verifying_key_to_address(&verifying_key);
        Ok(Self {
            kms: Arc::new(kms),
            key_id,
            chain_id,
            verifying_key,
            address,
        })
    }

    async fn sign_digest(
        &self,
        digest: [u8; 32],
    ) -> std::result::Result<EthereumSignature, ArcusSpotKmsSignerError> {
        let response = self
            .kms
            .sign(SignRequest {
                grant_tokens: None,
                key_id: self.key_id.clone(),
                message: digest.to_vec().into(),
                message_type: Some("DIGEST".to_string()),
                signing_algorithm: "ECDSA_SHA_256".to_string(),
            })
            .await
            .map_err(|error| {
                ArcusSpotKmsSignerError::Operation(format!("KMS Sign failed: {error}"))
            })?;
        let raw = response.signature.ok_or_else(|| {
            ArcusSpotKmsSignerError::Operation("KMS Sign omitted signature".to_string())
        })?;
        let signature = K256Signature::from_der(&raw).map_err(|error| {
            ArcusSpotKmsSignerError::Operation(format!("invalid KMS DER signature: {error}"))
        })?;
        let signature = signature.normalize_s().unwrap_or(signature);
        let recovery_id = [0_u8, 1_u8]
            .into_iter()
            .filter_map(RecoveryId::from_byte)
            .find(|recovery_id| {
                VerifyingKey::recover_from_prehash(digest.as_slice(), &signature, *recovery_id)
                    .is_ok_and(|candidate| candidate == self.verifying_key)
            })
            .ok_or_else(|| {
                ArcusSpotKmsSignerError::Operation(
                    "KMS signature could not recover the configured public key".to_string(),
                )
            })?;
        let r_bytes = signature.r().to_bytes();
        let s_bytes = signature.s().to_bytes();
        Ok(EthereumSignature {
            r: U256::from_big_endian(r_bytes.as_slice()),
            s: U256::from_big_endian(s_bytes.as_slice()),
            v: ethereum_v(recovery_id),
        })
    }
}

/// `RecoveryId::to_byte()` returns the raw 0/1 recovery id, not the
/// Ethereum-form 27/28 that ecrecover-based verifiers (the Arcus router,
/// and EIP-2612 token permit contracts) expect; `ethers::Signer`
/// implementations like LocalWallet already return 27/28. A serialized 0/1
/// v would still let our own local recovery-id search above succeed (it
/// tries both raw values), but be rejected downstream (Codex P1 follow-up,
/// pairtrade#181).
fn ethereum_v(recovery_id: RecoveryId) -> u64 {
    u64::from(recovery_id.to_byte()) + 27
}

#[async_trait]
impl Signer for ArcusSpotKmsSigner {
    type Error = ArcusSpotKmsSignerError;

    async fn sign_message<S: Send + Sync + AsRef<[u8]>>(
        &self,
        _message: S,
    ) -> std::result::Result<EthereumSignature, Self::Error> {
        Err(ArcusSpotKmsSignerError::UnsupportedSigningMode)
    }

    async fn sign_transaction(
        &self,
        _transaction: &TypedTransaction,
    ) -> std::result::Result<EthereumSignature, Self::Error> {
        Err(ArcusSpotKmsSignerError::UnsupportedSigningMode)
    }

    async fn sign_typed_data<T: Eip712 + Send + Sync>(
        &self,
        payload: &T,
    ) -> std::result::Result<EthereumSignature, Self::Error> {
        let digest = payload.encode_eip712().map_err(|error| {
            ArcusSpotKmsSignerError::Operation(format!(
                "could not encode Arcus EIP-712 payload: {error}"
            ))
        })?;
        self.sign_digest(digest).await
    }

    fn address(&self) -> Address {
        self.address
    }

    fn chain_id(&self) -> u64 {
        self.chain_id
    }

    fn with_chain_id<T: Into<u64>>(mut self, chain_id: T) -> Self {
        self.chain_id = chain_id.into();
        self
    }
}

fn verifying_key_to_address(key: &VerifyingKey) -> Address {
    let uncompressed = key.to_encoded_point(false);
    let bytes = uncompressed.as_bytes();
    debug_assert_eq!(bytes.first(), Some(&0x04));
    let hash = keccak256(&bytes[1..]);
    Address::from_slice(&hash[12..])
}

/// Resolve and verify the dedicated Arcus wallet without materializing its
/// private key. This performs only KMS GetPublicKey; no message is signed.
pub async fn build_arcus_spot_kms_signer(
    config: &ArcusSpotKmsConfig,
) -> Result<ArcusSpotKmsSigner> {
    let (region, expected_address) = config.validate()?;
    let signer = ArcusSpotKmsSigner::new(
        KmsClient::new(region),
        config.key_id.trim(),
        config.chain_id,
    )
    .await
    .context("failed to initialize Arcus AWS KMS signer")?;
    if signer.address() != expected_address {
        bail!(
            "Arcus KMS key resolves to {:#x}, expected {expected_address:#x}",
            signer.address()
        );
    }
    Ok(signer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ArcusSpotKmsConfig {
        ArcusSpotKmsConfig {
            region: "eu-central-1".to_string(),
            key_id: "alias/debot-arcus-spot".to_string(),
            chain_id: 4663,
            expected_address: "0x7600000000000000000000000000000000000001".to_string(),
        }
    }

    #[test]
    fn recovery_id_maps_to_ethereum_form_v() {
        assert_eq!(ethereum_v(RecoveryId::from_byte(0).unwrap()), 27);
        assert_eq!(ethereum_v(RecoveryId::from_byte(1).unwrap()), 28);
    }

    #[test]
    fn validates_without_contacting_kms() {
        let (region, address) = config().validate().unwrap();
        assert_eq!(region, Region::EuCentral1);
        assert_ne!(address, Address::zero());
    }

    #[test]
    fn debug_redacts_key_id() {
        let rendered = format!("{:?}", config());
        assert!(!rendered.contains("alias/debot-arcus-spot"));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn rejects_zero_expected_address() {
        let mut value = config();
        value.expected_address = format!("{:#x}", Address::zero());
        assert!(value.validate().unwrap_err().to_string().contains("zero"));
    }
}
