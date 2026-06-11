use std::collections::HashSet;
use std::env;

use anyhow::{anyhow, Result};

use super::schema::PairTradeYaml;

#[derive(Debug, Clone)]
pub struct PairSpec {
    pub base: String,
    pub quote: String,
}

pub(super) fn env_has_universe_override() -> bool {
    env::var("UNIVERSE_PAIRS")
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        || env::var("UNIVERSE_SYMBOLS")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
}

pub(super) fn parse_pairs_vec(pairs: &[String]) -> Result<Vec<PairSpec>> {
    let joined = pairs.join(",");
    parse_pairs_list(&joined)
}

pub(super) fn parse_symbols_vec(symbols: &[String]) -> Result<Vec<PairSpec>> {
    let syms: Vec<String> = symbols
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if syms.is_empty() {
        return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
    }
    let mut pairs = Vec::new();
    for i in 0..syms.len() {
        for j in (i + 1)..syms.len() {
            let a = syms[i].clone();
            let b = syms[j].clone();
            let (base, quote) = if a < b { (a, b) } else { (b, a) };
            pairs.push(PairSpec { base, quote });
        }
    }
    if pairs.is_empty() {
        return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
    }
    Ok(pairs)
}

pub(super) fn resolve_universe_from_yaml(yaml: &PairTradeYaml) -> Result<Vec<PairSpec>> {
    if env_has_universe_override() {
        return parse_universe_pairs();
    }
    if let Some(pairs) = yaml.universe_pairs.clone() {
        let pairs = pairs.into_vec();
        if pairs.is_empty() {
            return Err(anyhow!("universe_pairs produced no valid pairs"));
        }
        return parse_pairs_vec(&pairs);
    }
    if let Some(symbols) = yaml.universe_symbols.clone() {
        let symbols = symbols.into_vec();
        if symbols.is_empty() {
            return Err(anyhow!("universe_symbols produced no valid pairs"));
        }
        return parse_symbols_vec(&symbols);
    }
    let raw = "BTC/ETH,BTC/SOL,ETH/SOL".to_string();
    parse_pairs_list(&raw)
}

pub(super) fn parse_universe_pairs() -> Result<Vec<PairSpec>> {
    if let Ok(raw_pairs) = env::var("UNIVERSE_PAIRS") {
        if !raw_pairs.trim().is_empty() {
            return parse_pairs_list(&raw_pairs);
        }
    }
    if let Ok(symbols_raw) = env::var("UNIVERSE_SYMBOLS") {
        if !symbols_raw.trim().is_empty() {
            let syms: Vec<String> = symbols_raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let mut pairs = Vec::new();
            if syms.len() == 1 {
                // Single-symbol mode: create a self-pair for data-dump collection
                pairs.push(PairSpec {
                    base: syms[0].clone(),
                    quote: syms[0].clone(),
                });
            } else {
                for i in 0..syms.len() {
                    for j in (i + 1)..syms.len() {
                        let a = syms[i].clone();
                        let b = syms[j].clone();
                        let (base, quote) = if a < b { (a, b) } else { (b, a) };
                        pairs.push(PairSpec { base, quote });
                    }
                }
            }
            if pairs.is_empty() {
                return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
            }
            return Ok(pairs);
        }
    }
    let raw = "BTC/ETH,BTC/SOL,ETH/SOL".to_string();
    parse_pairs_list(&raw)
}

pub(super) fn parse_pairs_list(raw: &str) -> Result<Vec<PairSpec>> {
    let mut pairs = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut split = trimmed.split('/');
        let base = split
            .next()
            .ok_or_else(|| anyhow!("invalid pair: {}", trimmed))?;
        let quote = split
            .next()
            .ok_or_else(|| anyhow!("invalid pair: {}", trimmed))?;
        pairs.push(PairSpec {
            base: base.to_string(),
            quote: quote.to_string(),
        });
    }
    if pairs.is_empty() {
        return Err(anyhow!("UNIVERSE_PAIRS produced no valid pairs"));
    }
    Ok(pairs)
}

pub(super) fn default_history_file(universe: &[PairSpec], _agent_name: Option<&str>) -> String {
    let mut symbols: Vec<String> = universe
        .iter()
        .flat_map(|p| [p.base.clone(), p.quote.clone()])
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    // A/B/C bots watching the same pair on the same host intentionally share
    // one history file so their rolling regression windows stay identical
    // (pairtrade#4). The shared file is written atomically via tmpfile+rename
    // in persist_history_to_disk to avoid torn reads under concurrent writers.
    if symbols.is_empty() {
        return "pairtrade_history.json".to_string();
    }
    symbols.sort();
    let parts: Vec<String> = symbols
        .into_iter()
        .map(|sym| sanitize_symbol_for_filename(&sym))
        .filter(|sym| !sym.is_empty())
        .collect();
    if parts.is_empty() {
        return "pairtrade_history.json".to_string();
    }
    format!("pairtrade_history_{}.json", parts.join("_"))
}

fn sanitize_symbol_for_filename(symbol: &str) -> String {
    let mut out = String::with_capacity(symbol.len());
    for ch in symbol.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}
