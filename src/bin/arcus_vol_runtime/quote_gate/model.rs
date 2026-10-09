//! Model file (design §5.2, schema v1): one JSON per venue × market × cell.
//! `kind` is `linear` | `forest` | `lookup` in one envelope; this build
//! evaluates `linear` and refuses the other two at load (the forest / lookup
//! evaluators are the #1120 follow-up once #1113 is read out). Every check
//! here fails closed at startup: a model that does not match this runtime's
//! venue, market or feature engine is never loaded, in shadow as in enforce.
//!
//! `payload_sha256` is the SHA-256 of the canonical JSON (keys sorted
//! recursively, no whitespace, `ensure_ascii=False`; Python:
//! `json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`)
//! of the whole document without the `payload_sha256` key. Floats must be
//! written as plain decimals (no exponent) so both serialisers agree.

use super::features::{
    self, GRID_DT_MS, LOOKBACK_S, OWN_L2_LEVELS, PDEV_BN_SPANS_S, RV_ARC_WINDOWS_S,
};
use sha2::{Digest, Sha256};
use std::path::Path;

pub const SCHEMA_VERSION: u64 = 1;

/// Thresholds frozen with the model (design D5): the runtime never adjusts
/// them.
#[derive(Debug, Clone, PartialEq)]
pub struct Thresholds {
    /// Quote when `pred_bps >= quote_bps`, else pull.
    pub quote_bps: f64,
    pub frozen_at: String,
    pub validation_window: [String; 2],
}

/// Reference feeds the model was trained on (`feature_spec.ref_feeds`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RefFeeds {
    /// Binance USDT-M symbol, lower case (`btcusdt`).
    pub binance_symbol: Option<String>,
    /// Hyperliquid coin (`BTC`).
    pub hl_coin: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct Linear {
    intercept: f64,
    coef: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GateModel {
    pub kind: &'static str,
    pub venue: String,
    pub market: String,
    pub cell: String,
    /// Hex SHA-256 of the canonical payload (= `payload_sha256`).
    pub sha256_hex: String,
    pub thresholds: Thresholds,
    pub ref_feeds: RefFeeds,
    pub feature_names: Vec<String>,
    linear: Linear,
}

impl GateModel {
    pub fn load(path: &Path, venue: &str, market: &str) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read gate model {}: {e}", path.display()))?;
        Self::parse(&text, venue, market).map_err(|e| format!("gate model {}: {e}", path.display()))
    }

    /// Parse and validate against this runtime (`venue`, `market`) and the
    /// feature engine. Any mismatch is an error (fail closed on config).
    pub fn parse(text: &str, venue: &str, market: &str) -> Result<Self, String> {
        let doc: serde_json::Value =
            serde_json::from_str(text).map_err(|e| format!("not JSON: {e}"))?;
        let obj = doc.as_object().ok_or("not a JSON object")?;
        let str_field = |k: &str| -> Result<String, String> {
            obj.get(k)
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .ok_or_else(|| format!("missing string field `{k}`"))
        };
        if obj.get("schema_version").and_then(|v| v.as_u64()) != Some(SCHEMA_VERSION) {
            return Err(format!("schema_version must be {SCHEMA_VERSION}"));
        }
        // Integrity first: nothing else is trusted before the sha matches.
        let claimed = str_field("payload_sha256")?;
        let actual = payload_sha256(&doc);
        if claimed != actual {
            return Err(format!(
                "payload_sha256 mismatch: file says {claimed}, canonical payload is {actual}"
            ));
        }
        let kind = match str_field("kind")?.as_str() {
            "linear" => "linear",
            k @ ("forest" | "lookup") => {
                return Err(format!(
                    "kind `{k}` is not implemented in this build (bot-strategy#1120 follow-up)"
                ))
            }
            k => return Err(format!("unknown kind `{k}`")),
        };
        let file_venue = str_field("venue")?;
        let file_market = str_field("market")?;
        if file_venue != venue || file_market != market {
            return Err(format!(
                "model is for {file_venue} {file_market}, this runtime is {venue} {market}"
            ));
        }
        let cell = str_field("cell")?;
        let names: Vec<String> = obj
            .get("feature_names")
            .and_then(|v| v.as_array())
            .ok_or("missing feature_names")?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .ok_or("feature_names: not a string")
            })
            .collect::<Result<_, _>>()?;
        let expected = features::model_feature_names();
        if names != expected {
            let first = names
                .iter()
                .zip(expected.iter())
                .position(|(a, b)| a != b)
                .map(|i| {
                    format!(
                        " (first difference at {i}: `{}` vs `{}`)",
                        names[i], expected[i]
                    )
                })
                .unwrap_or_default();
            return Err(format!(
                "feature_names do not match this runtime's feature engine: {} names vs {}{first}",
                names.len(),
                expected.len()
            ));
        }
        if obj.get("input_dtype").and_then(|v| v.as_str()) != Some("f32") {
            return Err("input_dtype must be f32".into());
        }
        if obj.get("side_oriented").and_then(|v| v.as_bool()) != Some(true) {
            return Err("side_oriented must be true".into());
        }
        let spec = obj
            .get("feature_spec")
            .and_then(|v| v.as_object())
            .ok_or("missing feature_spec")?;
        let want_f = |k: &str, want: f64| -> Result<(), String> {
            let got = spec.get(k).and_then(|v| v.as_f64());
            if got.is_some_and(|g| (g - want).abs() < 1e-9) {
                Ok(())
            } else {
                Err(format!("feature_spec.{k} must be {want} (got {got:?})"))
            }
        };
        want_f("grid_dt", GRID_DT_MS as f64 / 1_000.0)?;
        want_f("lookback_s", LOOKBACK_S as f64)?;
        want_f("own_l2_levels", OWN_L2_LEVELS as f64)?;
        let want_list = |k: &str, want: &[u64]| -> Result<(), String> {
            let got: Option<Vec<u64>> = spec
                .get(k)
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_u64()).collect());
            if got.as_deref() == Some(want) {
                Ok(())
            } else {
                Err(format!("feature_spec.{k} must be {want:?} (got {got:?})"))
            }
        };
        want_list("ema_spans_s", &PDEV_BN_SPANS_S)?;
        want_list("rv_windows_s", &RV_ARC_WINDOWS_S)?;
        let mut ref_feeds = RefFeeds::default();
        for feed in spec
            .get("ref_feeds")
            .and_then(|v| v.as_array())
            .ok_or("missing feature_spec.ref_feeds")?
        {
            let s = feed.as_str().ok_or("ref_feeds: not a string")?;
            match s.split_once(':') {
                Some(("binance", sym)) if !sym.is_empty() => {
                    ref_feeds.binance_symbol = Some(sym.to_ascii_lowercase())
                }
                Some(("hyperliquid", coin)) if !coin.is_empty() => {
                    ref_feeds.hl_coin = Some(coin.to_string())
                }
                _ => return Err(format!("unknown reference feed `{s}`")),
            }
        }
        if ref_feeds.binance_symbol.is_none() || ref_feeds.hl_coin.is_none() {
            return Err(
                "feature_spec.ref_feeds must name both a binance:<symbol> and a hyperliquid:<coin> feed (the features need both)"
                    .into(),
            );
        }
        let th = obj
            .get("thresholds")
            .and_then(|v| v.as_object())
            .ok_or("missing thresholds")?;
        let quote_bps = th
            .get("quote_bps")
            .and_then(|v| v.as_f64())
            .filter(|q| q.is_finite())
            .ok_or("thresholds.quote_bps must be a finite number")?;
        // v1 (design D2): Widen is not generated; a file that asks for it is
        // refused rather than silently ignored.
        if th.get("widen_bps").is_some_and(|v| !v.is_null()) {
            return Err("thresholds.widen_bps must be null (Widen is not enabled in v1)".into());
        }
        let frozen_at = th
            .get("frozen_at")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or("thresholds.frozen_at is required (design D5)")?
            .to_string();
        let window: Vec<String> = th
            .get("validation_window")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let validation_window: [String; 2] = window
            .try_into()
            .map_err(|_| "thresholds.validation_window must be [start, end] (design D5)")?;
        let model = obj
            .get("model")
            .and_then(|v| v.as_object())
            .ok_or("missing model")?;
        let intercept = model
            .get("intercept")
            .and_then(|v| v.as_f64())
            .filter(|x| x.is_finite())
            .ok_or("model.intercept must be a finite number")?;
        let coef: Vec<f64> = model
            .get("coef")
            .and_then(|v| v.as_array())
            .ok_or("missing model.coef")?
            .iter()
            .map(|v| {
                v.as_f64()
                    .filter(|x| x.is_finite())
                    .ok_or("model.coef: not a finite number")
            })
            .collect::<Result<_, _>>()?;
        if coef.len() != names.len() {
            return Err(format!(
                "model.coef has {} entries for {} feature names",
                coef.len(),
                names.len()
            ));
        }
        Ok(GateModel {
            kind,
            venue: file_venue,
            market: file_market,
            cell,
            sha256_hex: actual,
            thresholds: Thresholds {
                quote_bps,
                frozen_at,
                validation_window,
            },
            ref_feeds,
            feature_names: names,
            linear: Linear { intercept, coef },
        })
    }

    /// Predicted side-signed 30 s markout (bp) for one side-oriented input
    /// (`FeatureVector::side_input`). f32 in, f64 coefficients, f32 out, as
    /// sklearn's `predict` on a float32 design matrix.
    pub fn predict_bps(&self, x_side: &[f32]) -> f32 {
        debug_assert_eq!(x_side.len(), self.linear.coef.len());
        let mut y = self.linear.intercept;
        for (c, x) in self.linear.coef.iter().zip(x_side) {
            y += c * f64::from(*x);
        }
        y as f32
    }

    /// First 12 hex digits, for log lines.
    pub fn sha_short(&self) -> &str {
        &self.sha256_hex[..12.min(self.sha256_hex.len())]
    }
}

/// Canonical JSON: keys sorted recursively, no whitespace.
pub fn canonical_json(v: &serde_json::Value) -> String {
    fn write(v: &serde_json::Value, out: &mut String) {
        match v {
            serde_json::Value::Object(m) => {
                let mut keys: Vec<&String> = m.keys().collect();
                keys.sort();
                out.push('{');
                for (i, k) in keys.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::Value::String((*k).clone()).to_string());
                    out.push(':');
                    write(&m[*k], out);
                }
                out.push('}');
            }
            serde_json::Value::Array(a) => {
                out.push('[');
                for (i, x) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write(x, out);
                }
                out.push(']');
            }
            other => out.push_str(&other.to_string()),
        }
    }
    let mut s = String::new();
    write(v, &mut s);
    s
}

/// SHA-256 (hex) of the canonical document without `payload_sha256`.
pub fn payload_sha256(doc: &serde_json::Value) -> String {
    let mut payload = doc.clone();
    if let Some(o) = payload.as_object_mut() {
        o.remove("payload_sha256");
    }
    let digest = Sha256::digest(canonical_json(&payload).as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The shipped Arcus BTC-USD linear model (10-01 premium-dev rule).
    pub const SHIPPED: &str =
        include_str!("../../../../configs/quote_gate/quote_gate_linear_arcus_btc.json");

    /// The shipped file with one JSON path changed and the sha recomputed
    /// (a valid file that differs in exactly that respect).
    pub fn variant(mutate: impl FnOnce(&mut serde_json::Value)) -> String {
        let mut doc: serde_json::Value = serde_json::from_str(SHIPPED).unwrap();
        mutate(&mut doc);
        let sha = payload_sha256(&doc);
        doc["payload_sha256"] = serde_json::Value::String(sha);
        doc.to_string()
    }

    #[test]
    fn the_shipped_model_loads_for_arcus_btc_and_encodes_the_10_01_rule() {
        let m = GateModel::parse(SHIPPED, "arcus", "BTC-USD").unwrap();
        assert_eq!(m.kind, "linear");
        assert_eq!(m.cell, "A_at_touch_clip500");
        assert_eq!(m.thresholds.quote_bps, -0.25);
        assert_eq!(m.ref_feeds.binance_symbol.as_deref(), Some("btcusdt"));
        assert_eq!(m.ref_feeds.hl_coin.as_deref(), Some("BTC"));
        assert_eq!(m.feature_names.len(), 38);
        // The only non-zero coefficient is pdev_bn_60s (= 1): pred is the
        // side-oriented premium deviation itself.
        let i = m
            .feature_names
            .iter()
            .position(|n| n == "pdev_bn_60s")
            .unwrap();
        let mut x = vec![0.0f32; 38];
        x[i] = 0.3;
        x[37] = 1.0;
        assert_eq!(m.predict_bps(&x), 0.3);
        x[i] = -0.3;
        assert_eq!(m.predict_bps(&x), -0.3);
        // Every other feature is ignored.
        for (k, v) in x.iter_mut().enumerate() {
            if k != i {
                *v = 1e3;
            }
        }
        assert_eq!(m.predict_bps(&x), -0.3);
        // The sha in the file is the canonical-payload sha (checked against
        // Python's json.dumps(sort_keys=True, separators=(",", ":")) when the
        // file was written).
        assert_eq!(m.sha_short().len(), 12);
    }

    #[test]
    fn venue_market_feature_names_and_sha_mismatches_are_refused() {
        let err = GateModel::parse(SHIPPED, "arcus", "SPY-USD").unwrap_err();
        assert!(err.contains("model is for arcus BTC-USD"), "{err}");
        let err = GateModel::parse(SHIPPED, "lighter", "BTC-USD").unwrap_err();
        assert!(err.contains("this runtime is lighter"), "{err}");
        // A swapped pair of feature names (same set, wrong order).
        let swapped = variant(|d| {
            let a = d["feature_names"][0].clone();
            let b = d["feature_names"][1].clone();
            d["feature_names"][0] = b;
            d["feature_names"][1] = a;
        });
        let err = GateModel::parse(&swapped, "arcus", "BTC-USD").unwrap_err();
        assert!(
            err.contains("feature_names do not match") && err.contains("first difference at 0"),
            "{err}"
        );
        // A coefficient edited without updating the sha.
        let tampered = SHIPPED.replacen("\"intercept\": 0.0", "\"intercept\": 5.0", 1);
        assert_ne!(tampered, SHIPPED);
        let err = GateModel::parse(&tampered, "arcus", "BTC-USD").unwrap_err();
        assert!(err.contains("payload_sha256 mismatch"), "{err}");
        // Missing / wrong sha field.
        let no_sha = {
            let mut d: serde_json::Value = serde_json::from_str(SHIPPED).unwrap();
            d.as_object_mut().unwrap().remove("payload_sha256");
            d.to_string()
        };
        assert!(GateModel::parse(&no_sha, "arcus", "BTC-USD").is_err());
    }

    #[test]
    fn unimplemented_kinds_widen_and_unfrozen_thresholds_are_refused() {
        for kind in ["forest", "lookup"] {
            let f = variant(|d| d["kind"] = kind.into());
            let err = GateModel::parse(&f, "arcus", "BTC-USD").unwrap_err();
            assert!(err.contains("not implemented"), "{kind}: {err}");
        }
        let f = variant(|d| d["kind"] = "neural".into());
        assert!(GateModel::parse(&f, "arcus", "BTC-USD")
            .unwrap_err()
            .contains("unknown kind"));
        let f = variant(|d| d["thresholds"]["widen_bps"] = 0.1.into());
        assert!(GateModel::parse(&f, "arcus", "BTC-USD")
            .unwrap_err()
            .contains("Widen"));
        let f = variant(|d| d["thresholds"]["frozen_at"] = "".into());
        assert!(GateModel::parse(&f, "arcus", "BTC-USD")
            .unwrap_err()
            .contains("frozen_at"));
        let f = variant(|d| d["thresholds"]["validation_window"] = serde_json::json!(["x"]));
        assert!(GateModel::parse(&f, "arcus", "BTC-USD")
            .unwrap_err()
            .contains("validation_window"));
        let f = variant(|d| d["feature_spec"]["grid_dt"] = 0.5.into());
        assert!(GateModel::parse(&f, "arcus", "BTC-USD")
            .unwrap_err()
            .contains("grid_dt"));
        let f =
            variant(|d| d["feature_spec"]["ref_feeds"] = serde_json::json!(["binance:btcusdt"]));
        assert!(GateModel::parse(&f, "arcus", "BTC-USD")
            .unwrap_err()
            .contains("hyperliquid"));
        let f = variant(|d| {
            d["feature_spec"]["ref_feeds"] = serde_json::json!(["binance:btcusdt", "pyth:BTC"])
        });
        assert!(GateModel::parse(&f, "arcus", "BTC-USD")
            .unwrap_err()
            .contains("unknown reference feed"));
        let f = variant(|d| d["model"]["coef"] = serde_json::json!([1.0, 2.0]));
        assert!(GateModel::parse(&f, "arcus", "BTC-USD")
            .unwrap_err()
            .contains("coef has 2"));
        let f = variant(|d| d["schema_version"] = 2.into());
        assert!(GateModel::parse(&f, "arcus", "BTC-USD")
            .unwrap_err()
            .contains("schema_version"));
        let f = variant(|d| d["side_oriented"] = false.into());
        assert!(GateModel::parse(&f, "arcus", "BTC-USD")
            .unwrap_err()
            .contains("side_oriented"));
    }

    #[test]
    fn canonical_json_sorts_keys_recursively_and_is_compact() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"b": [1, {"z": 1, "a": "é"}], "a": 0.25, "c": null}"#)
                .unwrap();
        assert_eq!(
            canonical_json(&v),
            r#"{"a":0.25,"b":[1,{"a":"é","z":1}],"c":null}"#
        );
        // `payload_sha256` is excluded from its own hash; the value is
        // Python's hashlib.sha256(b'{"a":1}').hexdigest().
        let doc: serde_json::Value = serde_json::json!({"a": 1, "payload_sha256": "ignored"});
        assert_eq!(
            payload_sha256(&doc),
            "015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862"
        );
    }
}
