//! Signal file contract (schema v1), see `docs/book-runtime.md` §3.
//!
//! The runtime never computes a signal. It reads a file the producer wrote,
//! recomputes the payload hash, and applies a fixed validation order. Any
//! failure rejects the whole file: there is no partial application.

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::config::{SignalConfig, SizingConfig, UniverseConfig};

/// `t` rounded *up* to the next whole second when it carries a fractional
/// remainder, otherwise unchanged. Every engine tick time, window end,
/// arrival check and schedule boundary is a whole second, and flooring
/// (`timestamp()`) would silently move an instant *earlier* than
/// configured or observed: a signal file generated at `T + 0.5s` would
/// appear to a tick at `T` as already arrived (letting replay accept it,
/// or fill it, a bar date early), and a calendar `decision_at` or
/// `flatten_at` of `T + 0.5s` would let the tick at `T` select or flatten
/// it half a second before the configured instant, submitting a
/// prepublished signal or closing early. Used for both.
pub fn ceil_secs(t: DateTime<Utc>) -> i64 {
    let secs = t.timestamp();
    if t.timestamp_subsec_nanos() > 0 {
        secs + 1
    } else {
        secs
    }
}

pub const SCHEMA_VERSION: u32 = 1;

/// Raw on-disk shape. `weights` is a `BTreeMap` so the canonical JSON used
/// for hashing is key-sorted regardless of the producer's ordering.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SignalFile {
    pub schema_version: u32,
    pub producer_id: String,
    pub generated_at: DateTime<Utc>,
    pub as_of: DateTime<Utc>,
    pub decision_key: String,
    pub weights: BTreeMap<String, f64>,
    #[serde(default)]
    pub meta: serde_json::Value,
    pub payload_sha256: String,
}

/// A signal that passed every check, ready for the rebalancer.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidSignal {
    pub producer_id: String,
    pub decision_key: String,
    pub generated_at: DateTime<Utc>,
    pub as_of: DateTime<Utc>,
    pub weights: BTreeMap<String, f64>,
    pub payload_sha256: String,
}

/// Why a signal file was rejected. Serialized (snake_case) into the
/// `decision` ledger row.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "reason", content = "detail")]
pub enum SignalReject {
    Unreadable(String),
    Unparseable(String),
    SchemaVersion(u32),
    ProducerMismatch(String),
    HashMismatch {
        expected: String,
        actual: String,
    },
    Stale {
        age_secs: i64,
        max_age_secs: i64,
    },
    FutureGenerated {
        ahead_secs: i64,
    },
    LookAhead {
        as_of: String,
        decision_at: String,
    },
    AsOfAfterGenerated,
    DecisionKeyMismatch {
        expected: String,
        actual: String,
    },
    UnknownSymbol(String),
    NonFiniteWeight(String),
    WeightAboveCap {
        symbol: String,
        weight: f64,
        cap: f64,
    },
    GrossAboveOne(f64),
    NotDollarNeutral {
        net: f64,
        tolerance: f64,
    },
}

impl fmt::Display for SignalReject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap_or_default())
    }
}

impl SignalReject {
    /// Short label for metrics / log lines.
    pub fn label(&self) -> &'static str {
        match self {
            SignalReject::Unreadable(_) => "unreadable",
            SignalReject::Unparseable(_) => "unparseable",
            SignalReject::SchemaVersion(_) => "schema_version",
            SignalReject::ProducerMismatch(_) => "producer_mismatch",
            SignalReject::HashMismatch { .. } => "hash_mismatch",
            SignalReject::Stale { .. } => "stale",
            SignalReject::FutureGenerated { .. } => "future_generated",
            SignalReject::LookAhead { .. } => "look_ahead",
            SignalReject::AsOfAfterGenerated => "as_of_after_generated",
            SignalReject::DecisionKeyMismatch { .. } => "decision_key_mismatch",
            SignalReject::UnknownSymbol(_) => "unknown_symbol",
            SignalReject::NonFiniteWeight(_) => "non_finite_weight",
            SignalReject::WeightAboveCap { .. } => "weight_above_cap",
            SignalReject::GrossAboveOne(_) => "gross_above_one",
            SignalReject::NotDollarNeutral { .. } => "not_dollar_neutral",
        }
    }
}

/// Canonical JSON of the hashed payload: `{"as_of","decision_key",
/// "producer_id","weights"}` with sorted keys and no whitespace, matching
/// Python's `json.dumps(obj, sort_keys=True, separators=(",", ":"))`.
/// `as_of` is hashed as the exact string the producer wrote (whatever
/// valid RFC 3339 spelling that was), never re-derived from the parsed
/// `DateTime<Utc>`: `book_signal_file.py` hashes the string it itself
/// wrote into the file, so re-formatting it here (fractional seconds
/// dropped, a numeric offset forced to `Z`) before hashing would report
/// `hash_mismatch` on a payload nothing had actually altered. Weights use
/// Rust/Python shortest round-trip float formatting.
pub fn canonical_payload(
    producer_id: &str,
    as_of: &str,
    decision_key: &str,
    weights: &BTreeMap<String, f64>,
) -> String {
    let mut out = String::new();
    out.push_str("{\"as_of\":");
    out.push_str(&serde_json::to_string(as_of).unwrap());
    out.push_str(",\"decision_key\":");
    out.push_str(&serde_json::to_string(decision_key).unwrap());
    out.push_str(",\"producer_id\":");
    out.push_str(&serde_json::to_string(producer_id).unwrap());
    out.push_str(",\"weights\":{");
    let mut first = true;
    for (k, v) in weights {
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&serde_json::to_string(k).unwrap());
        out.push(':');
        out.push_str(&format_weight(*v));
    }
    out.push_str("}}");
    out
}

/// Python `json.dumps` float formatting, i.e. `repr(float)`: shortest
/// round-trip digits, fixed notation when the decimal exponent is in
/// `[-4, 16)`, otherwise scientific with a signed two-digit-minimum
/// exponent (`1e-05`, `1.5e-07`, `1e+16`). Rust's `{}` already yields the
/// shortest digits but never switches to exponent form for small values,
/// so a producer weight of `1e-7` would hash differently; this matches
/// Python for the full finite range.
pub fn format_weight(v: f64) -> String {
    if !v.is_finite() {
        return format!("{v}");
    }
    if v == 0.0 {
        return if v.is_sign_negative() {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    }
    let sign = if v < 0.0 { "-" } else { "" };
    let sci = format!("{:e}", v.abs()); // e.g. "1.5e-7", "1e16", "1.23456e2"
    let (mant, exp) = sci.split_once('e').expect("{:e} always has an exponent");
    let exp: i32 = exp.parse().expect("exponent is an integer");
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    if (-4..16).contains(&exp) {
        // Fixed notation: place the decimal point after `exp + 1` digits.
        let point = exp + 1;
        let body = if point <= 0 {
            format!("0.{}{}", "0".repeat((-point) as usize), digits)
        } else if (point as usize) >= digits.len() {
            format!("{}{}.0", digits, "0".repeat(point as usize - digits.len()))
        } else {
            let (a, b) = digits.split_at(point as usize);
            format!("{a}.{b}")
        };
        format!("{sign}{body}")
    } else {
        let exp_s = if exp < 0 {
            format!("-{:02}", -exp)
        } else {
            format!("+{:02}", exp)
        };
        format!("{sign}{mant}e{exp_s}")
    }
}

pub fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

pub fn payload_sha256(
    producer_id: &str,
    as_of: &str,
    decision_key: &str,
    weights: &BTreeMap<String, f64>,
) -> String {
    sha256_hex(&canonical_payload(
        producer_id,
        as_of,
        decision_key,
        weights,
    ))
}

/// Everything the validator needs to know about the decision the file is
/// being read for.
#[derive(Debug, Clone)]
pub struct DecisionContext<'a> {
    pub now: DateTime<Utc>,
    pub decision_key: &'a str,
    pub decision_at: DateTime<Utc>,
}

/// Validate a raw file body against the config and the current decision.
/// Order (first failure wins): parse → schema_version → producer_id → hash
/// → freshness → look-ahead → decision_key → universe → weight bounds →
/// neutrality.
pub fn validate(
    body: &str,
    signal_cfg: &SignalConfig,
    sizing: &SizingConfig,
    universe: &UniverseConfig,
    ctx: &DecisionContext<'_>,
) -> Result<ValidSignal, SignalReject> {
    let file: SignalFile =
        serde_json::from_str(body).map_err(|e| SignalReject::Unparseable(e.to_string()))?;
    if file.schema_version != SCHEMA_VERSION {
        return Err(SignalReject::SchemaVersion(file.schema_version));
    }
    if file.producer_id != signal_cfg.producer_id {
        return Err(SignalReject::ProducerMismatch(file.producer_id));
    }
    // The exact string the producer wrote, not `file.as_of` re-formatted
    // from the parsed DateTime<Utc> (see `canonical_payload`'s doc): both
    // parses read the same already-validated body, so this one failing
    // when the first succeeded is not reachable in practice.
    let as_of_raw: String = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("as_of")?.as_str().map(str::to_string))
        .ok_or_else(|| SignalReject::Unparseable("as_of is not a string".into()))?;
    let expected = payload_sha256(
        &file.producer_id,
        &as_of_raw,
        &file.decision_key,
        &file.weights,
    );
    if !expected.eq_ignore_ascii_case(file.payload_sha256.trim()) {
        return Err(SignalReject::HashMismatch {
            expected,
            actual: file.payload_sha256,
        });
    }
    let age = (ctx.now - file.generated_at).num_seconds();
    if age < -60 {
        return Err(SignalReject::FutureGenerated { ahead_secs: -age });
    }
    if age > signal_cfg.max_age_secs {
        return Err(SignalReject::Stale {
            age_secs: age,
            max_age_secs: signal_cfg.max_age_secs,
        });
    }
    if file.as_of > file.generated_at {
        return Err(SignalReject::AsOfAfterGenerated);
    }
    if file.as_of > ctx.decision_at {
        return Err(SignalReject::LookAhead {
            as_of: file.as_of.to_rfc3339(),
            decision_at: ctx.decision_at.to_rfc3339(),
        });
    }
    if file.decision_key != ctx.decision_key {
        return Err(SignalReject::DecisionKeyMismatch {
            expected: ctx.decision_key.to_string(),
            actual: file.decision_key,
        });
    }
    let mut gross = 0.0;
    let mut net = 0.0;
    for (sym, w) in &file.weights {
        if !universe.symbols.iter().any(|u| u == sym) {
            return Err(SignalReject::UnknownSymbol(sym.clone()));
        }
        if !w.is_finite() {
            return Err(SignalReject::NonFiniteWeight(sym.clone()));
        }
        if w.abs() > sizing.max_symbol_weight + 1e-12 {
            return Err(SignalReject::WeightAboveCap {
                symbol: sym.clone(),
                weight: *w,
                cap: sizing.max_symbol_weight,
            });
        }
        gross += w.abs();
        net += w;
    }
    if gross > 1.0 + 1e-9 {
        return Err(SignalReject::GrossAboveOne(gross));
    }
    if signal_cfg.require_dollar_neutral && net.abs() > signal_cfg.net_tolerance + 1e-12 {
        return Err(SignalReject::NotDollarNeutral {
            net,
            tolerance: signal_cfg.net_tolerance,
        });
    }
    Ok(ValidSignal {
        producer_id: file.producer_id,
        decision_key: file.decision_key,
        generated_at: file.generated_at,
        as_of: file.as_of,
        weights: file.weights,
        payload_sha256: expected,
    })
}

#[cfg(test)]
pub(crate) mod testutil {
    use super::*;

    /// Build a valid signal body for tests. `generated_at` / `as_of` are
    /// rendered in the producer's `YYYY-MM-DDTHH:MM:SSZ` form.
    pub fn signal_json(
        producer_id: &str,
        generated_at: DateTime<Utc>,
        as_of: DateTime<Utc>,
        decision_key: &str,
        weights: &[(&str, f64)],
    ) -> String {
        let w: BTreeMap<String, f64> = weights.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        let as_of_s = as_of.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let sha = payload_sha256(producer_id, &as_of_s, decision_key, &w);
        serde_json::json!({
            "schema_version": 1,
            "producer_id": producer_id,
            "generated_at": generated_at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "as_of": as_of_s,
            "decision_key": decision_key,
            "weights": w,
            "meta": {"test": true},
            "payload_sha256": sha,
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::signal_json;
    use super::*;
    use crate::book::config::{test_config_yaml, BookConfig};

    fn cfg() -> BookConfig {
        BookConfig::from_yaml_str(&test_config_yaml()).unwrap()
    }

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn ctx<'a>(key: &'a str) -> DecisionContext<'a> {
        DecisionContext {
            now: ts("2026-09-06T00:40:00Z"),
            decision_key: key,
            decision_at: ts("2026-09-06T00:30:00Z"),
        }
    }

    fn good() -> String {
        signal_json(
            "test_producer",
            ts("2026-09-06T00:20:00Z"),
            ts("2026-09-06T00:00:00Z"),
            "2026-09-06",
            &[("BTC", 0.25), ("ETH", 0.25), ("SOL", -0.25), ("DOT", -0.25)],
        )
    }

    #[test]
    fn canonical_payload_matches_python_json_dumps() {
        // python3 -c 'import json,hashlib; p={"producer_id":"p","as_of":"2026-09-06T00:00:00Z","decision_key":"2026-09-06","weights":{"SOL":-0.1,"BTC":0.1,"ETH":1.0}}; s=json.dumps(p,sort_keys=True,separators=(",",":")); print(s); print(hashlib.sha256(s.encode()).hexdigest())'
        let w: BTreeMap<String, f64> = [("SOL", -0.1), ("BTC", 0.1), ("ETH", 1.0)]
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect();
        let s = canonical_payload("p", "2026-09-06T00:00:00Z", "2026-09-06", &w);
        assert_eq!(
            s,
            r#"{"as_of":"2026-09-06T00:00:00Z","decision_key":"2026-09-06","producer_id":"p","weights":{"BTC":0.1,"ETH":1.0,"SOL":-0.1}}"#
        );
        assert_eq!(
            sha256_hex(&s),
            "5554a39df2be5d06c9da208795db70f46379fc1dc8f5eb02351cd6ce9f010f1a"
        );
    }

    #[test]
    fn canonical_payload_matches_python_for_a_non_ascii_producer_id() {
        // Shared vector with
        // scripts/test_book_signal_file.py::test_hash_vector_with_non_ascii_producer_id_matches_rust
        // (produced there with ensure_ascii=False, since serde_json never
        // \u-escapes non-ASCII text -- a naive json.dumps() would hash
        // different bytes than this for the same logical payload).
        let w: BTreeMap<String, f64> = [("BTC", 0.1)]
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect();
        let s = canonical_payload("prîd_日本語", "2026-09-06T00:00:00Z", "2026-09-06", &w);
        assert_eq!(
            s,
            "{\"as_of\":\"2026-09-06T00:00:00Z\",\"decision_key\":\"2026-09-06\",\"producer_id\":\"prîd_日本語\",\"weights\":{\"BTC\":0.1}}"
        );
        assert_eq!(
            sha256_hex(&s),
            "739a42e64055240a8c9be1dd93422d58a4e8e326574464bee1ed4fd9cfc4acfc"
        );
    }

    #[test]
    fn accepts_a_well_formed_signal() {
        let c = cfg();
        let v = validate(
            &good(),
            &c.signal,
            &c.sizing,
            &c.universe,
            &ctx("2026-09-06"),
        )
        .unwrap();
        assert_eq!(v.decision_key, "2026-09-06");
        assert_eq!(v.weights.len(), 4);
        assert_eq!(v.payload_sha256.len(), 64);
    }

    #[test]
    fn accepts_a_hash_computed_from_the_producers_own_as_of_spelling() {
        // A numeric UTC offset instead of "Z": a different, equally valid
        // RFC 3339 spelling of the exact same instant. The producer
        // hashed this string as written -- re-deriving a canonical
        // "...Z" form from the parsed DateTime<Utc> before hashing (the
        // bug) would report hash_mismatch on this untampered payload.
        let c = cfg();
        let w: BTreeMap<String, f64> =
            [("BTC", 0.25), ("ETH", 0.25), ("SOL", -0.25), ("DOT", -0.25)]
                .iter()
                .map(|(k, v)| (k.to_string(), *v))
                .collect();
        let as_of_raw = "2026-09-06T00:00:00+00:00";
        let sha = payload_sha256("test_producer", as_of_raw, "2026-09-06", &w);
        let body = serde_json::json!({
            "schema_version": 1,
            "producer_id": "test_producer",
            "generated_at": "2026-09-06T00:20:00Z",
            "as_of": as_of_raw,
            "decision_key": "2026-09-06",
            "weights": w,
            "payload_sha256": sha,
        })
        .to_string();
        let v = validate(&body, &c.signal, &c.sizing, &c.universe, &ctx("2026-09-06")).unwrap();
        assert_eq!(v.decision_key, "2026-09-06");
    }

    #[test]
    fn rejects_in_documented_order() {
        let c = cfg();
        let k = ctx("2026-09-06");
        // unparseable
        assert_eq!(
            validate("{", &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "unparseable"
        );
        // schema version
        let b = good().replace("\"schema_version\":1", "\"schema_version\":2");
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "schema_version"
        );
        // producer mismatch
        let b = good().replace(
            "\"producer_id\":\"test_producer\"",
            "\"producer_id\":\"other\"",
        );
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "producer_mismatch"
        );
        // hash mismatch: tamper with a weight after hashing
        let b = good().replace("\"BTC\":0.25", "\"BTC\":0.26");
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "hash_mismatch"
        );
        // stale
        let b = signal_json(
            "test_producer",
            ts("2026-09-05T00:00:00Z"),
            ts("2026-09-04T00:00:00Z"),
            "2026-09-06",
            &[("BTC", 0.1), ("ETH", -0.1)],
        );
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "stale"
        );
        // generated in the future
        let b = signal_json(
            "test_producer",
            ts("2026-09-06T01:00:00Z"),
            ts("2026-09-06T00:00:00Z"),
            "2026-09-06",
            &[("BTC", 0.1), ("ETH", -0.1)],
        );
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "future_generated"
        );
        // as_of after generated_at
        let b = signal_json(
            "test_producer",
            ts("2026-09-06T00:20:00Z"),
            ts("2026-09-06T00:25:00Z"),
            "2026-09-06",
            &[("BTC", 0.1), ("ETH", -0.1)],
        );
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "as_of_after_generated"
        );
        // look-ahead: as_of after decision_at (00:30) but before generated_at
        let b = signal_json(
            "test_producer",
            ts("2026-09-06T00:39:00Z"),
            ts("2026-09-06T00:35:00Z"),
            "2026-09-06",
            &[("BTC", 0.1), ("ETH", -0.1)],
        );
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "look_ahead"
        );
        // decision key
        let b = signal_json(
            "test_producer",
            ts("2026-09-06T00:20:00Z"),
            ts("2026-09-06T00:00:00Z"),
            "2026-09-01",
            &[("BTC", 0.1), ("ETH", -0.1)],
        );
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "decision_key_mismatch"
        );
        // unknown symbol
        let b = signal_json(
            "test_producer",
            ts("2026-09-06T00:20:00Z"),
            ts("2026-09-06T00:00:00Z"),
            "2026-09-06",
            &[("BTC", 0.1), ("XYZ", -0.1)],
        );
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "unknown_symbol"
        );
        // weight cap (0.5 in the test config)
        let b = signal_json(
            "test_producer",
            ts("2026-09-06T00:20:00Z"),
            ts("2026-09-06T00:00:00Z"),
            "2026-09-06",
            &[("BTC", 0.6), ("ETH", -0.4)],
        );
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "weight_above_cap"
        );
        // gross above one
        let b = signal_json(
            "test_producer",
            ts("2026-09-06T00:20:00Z"),
            ts("2026-09-06T00:00:00Z"),
            "2026-09-06",
            &[("BTC", 0.5), ("ETH", -0.5), ("SOL", 0.5)],
        );
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "gross_above_one"
        );
        // not dollar neutral
        let b = signal_json(
            "test_producer",
            ts("2026-09-06T00:20:00Z"),
            ts("2026-09-06T00:00:00Z"),
            "2026-09-06",
            &[("BTC", 0.5), ("ETH", -0.2)],
        );
        assert_eq!(
            validate(&b, &c.signal, &c.sizing, &c.universe, &k)
                .unwrap_err()
                .label(),
            "not_dollar_neutral"
        );
    }

    #[test]
    fn empty_weights_is_a_valid_go_flat() {
        let c = cfg();
        let b = signal_json(
            "test_producer",
            ts("2026-09-06T00:20:00Z"),
            ts("2026-09-06T00:00:00Z"),
            "2026-09-06",
            &[],
        );
        let v = validate(&b, &c.signal, &c.sizing, &c.universe, &ctx("2026-09-06")).unwrap();
        assert!(v.weights.is_empty());
    }

    #[test]
    fn format_weight_matches_python_repr() {
        // Reference: python3 -c 'print(repr(v))' for each value.
        for (v, want) in [
            (0.1, "0.1"),
            (-0.25, "-0.25"),
            (1.0, "1.0"),
            (0.0, "0.0"),
            (100.0, "100.0"),
            (123.456, "123.456"),
            (0.045454545454545456, "0.045454545454545456"),
            (0.1 + 0.2, "0.30000000000000004"),
            (1e-4, "0.0001"),
            (0.000123, "0.000123"),
            (9.9e-5, "9.9e-05"),
            (4.5e-5, "4.5e-05"),
            (1e-5, "1e-05"),
            (1e-7, "1e-07"),
            (1.5e-7, "1.5e-07"),
            (-2.5e-9, "-2.5e-09"),
            (1e15, "1000000000000000.0"),
            (1234567890123456.0, "1234567890123456.0"),
            (1e16, "1e+16"),
            (1.2345678901234568e17, "1.2345678901234568e+17"),
            (1e22, "1e+22"),
            (5e-324, "5e-324"),
        ] {
            assert_eq!(format_weight(v), want, "v={v:e}");
        }
    }

    #[test]
    fn tiny_weight_hash_matches_python() {
        // python3 -c 'import json,hashlib; s=json.dumps({"as_of":"2026-09-06T00:00:00Z","decision_key":"k","producer_id":"p","weights":{"A":1e-07,"B":-4.5e-05}},sort_keys=True,separators=(",",":")); print(s, hashlib.sha256(s.encode()).hexdigest())'
        let w: BTreeMap<String, f64> = [("A", 1e-7), ("B", -4.5e-5)]
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect();
        let s = canonical_payload("p", "2026-09-06T00:00:00Z", "k", &w);
        assert_eq!(
            s,
            r#"{"as_of":"2026-09-06T00:00:00Z","decision_key":"k","producer_id":"p","weights":{"A":1e-07,"B":-4.5e-05}}"#
        );
        assert_eq!(
            sha256_hex(&s),
            "201ad4492e66594c520ce52afba3d269c61d1b7158e334a4d0ec008ab14258af"
        );
    }
}
