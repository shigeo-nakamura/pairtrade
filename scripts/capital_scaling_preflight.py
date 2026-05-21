#!/usr/bin/env python3
"""
capital_scaling_preflight.py — verify the $10k YAML loads cleanly and
the implied notionals / risk caps line up with intent (bot-strategy#468).

Doesn't TRADE. Doesn't TOUCH the live bot. Just:
  1. Asserts the $10k YAML parses without panic
  2. Computes the implied per-leg notional + max_notional_cap at $10k × max_leverage
  3. Sanity-checks the bps-based risk gates translate to sensible dollar values
  4. Prints a checklist with PASS / FLAG / TODO per item

Run before the actual capital deposit. Print everything; the operator
reads + signs off in the issue.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BINARY = REPO / "target/release/debot"
DEFAULT_YAML = REPO / "configs/pairtrade/scenarios/capital-scaling/debot-pair-btceth-10k.yaml"
DATA_BIN = Path("/tmp/phase_b_grid/shared/live.bin")  # reuse from grid sweep
EVENTS = Path("/tmp/bt_rehedge_p2_extended/events")
SNAPSHOT = Path("/tmp/bt_r3r4/pairtrade_history_BTC_ETH.json")


def parse_yaml_dollars(yaml_path: Path) -> dict:
    """Pull the dollar-relevant fields out of the YAML without loading
    a yaml library (keeps this script dep-free)."""
    text = yaml_path.read_text()
    out = {}
    for key in [
        "equity_usd_reference",
        "max_leverage",
        "risk_pct_per_trade",
        "max_daily_loss_bps",
        "max_session_loss_bps",
        "max_notional_headroom",
    ]:
        m = re.search(rf"^\s*{re.escape(key)}:\s*([0-9.]+)", text, flags=re.MULTILINE)
        if m:
            out[key] = float(m.group(1))
    # Per-variant equity_usd_reference (each strategy block)
    variants = []
    for stanza in re.split(r"^\s*-\s*id:\s*", text, flags=re.MULTILINE)[1:]:
        id_match = re.match(r"(\S+)", stanza)
        eq_match = re.search(r"equity_usd_reference:\s*([0-9.]+)", stanza)
        if id_match and eq_match:
            variants.append((id_match.group(1), float(eq_match.group(1))))
    out["variants"] = variants
    return out


def parse_check(yaml_path: Path) -> tuple[bool, str]:
    """Run the bot binary briefly against the YAML to ensure it parses."""
    if not BINARY.exists():
        return False, f"Binary not built ({BINARY})"
    if not DATA_BIN.exists():
        return False, f"No live.bin at {DATA_BIN} — run phase_b_grid.py first"
    env = {
        "PATH": "/usr/bin:/usr/local/bin",
        "HOME": str(Path.home()),
        "LD_LIBRARY_PATH": "/home/shigeo/bot/lighter-go",
        "BT_WARM_START_SNAPSHOT": str(SNAPSHOT),
        "BT_EVAL_TIMESTAMPS_FILE": str(EVENTS / "eval_ts.txt"),
        "BT_RESTART_TIMESTAMPS_FILE": str(EVENTS / "restart_ts.txt"),
        "BACKTEST_MODE": "true",
        "BACKTEST_FILE": str(DATA_BIN),
        "DRY_RUN": "true",
        "ENABLE_DATA_DUMP": "false",
        "RUST_LOG": "error",
        "UNIVERSE_PAIRS": "BTC/ETH",
        "PAIRTRADE_CONFIG_PATH": str(yaml_path),
    }
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
        log_path = Path(f.name)
    try:
        subprocess.run([str(BINARY)], env=env, stdout=open(log_path, "wb"),
                       stderr=subprocess.STDOUT, cwd=REPO, timeout=3)
    except subprocess.TimeoutExpired:
        pass
    output = log_path.read_text()
    log_path.unlink()
    if any(x in output for x in ("failed to parse", "panic", "invalid pair trade config",
                                  "deny_unknown_fields", "refusing to start")):
        return False, output
    return True, ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yaml", type=Path, default=DEFAULT_YAML)
    args = parser.parse_args()

    print(f"# Capital scaling pre-flight — {args.yaml.name}\n")

    # 1. Parse check
    print("## 1. YAML parses cleanly\n")
    ok, err = parse_check(args.yaml)
    if ok:
        print("  ✅ PASS — binary loads the config without panic\n")
    else:
        print(f"  ❌ FAIL — {err[:500]}\n")
        return 1

    # 2. Dollar math
    cfg = parse_yaml_dollars(args.yaml)
    print("## 2. Notional math at the new equity\n")
    eq = cfg.get("equity_usd_reference", 0)
    lev = cfg.get("max_leverage", 0)
    risk_pct = cfg.get("risk_pct_per_trade", 0)
    headroom = cfg.get("max_notional_headroom", 0)
    leg_notional = eq * risk_pct * lev / 2  # `(total_risk = eq * risk_pct * lev) / 2 legs`
    notional_cap = eq * lev * headroom
    print(f"  equity_reference_usd       : ${eq:>10.2f}")
    print(f"  max_leverage               : {lev:>10.1f}x")
    print(f"  risk_pct_per_trade         : {risk_pct:>10.2f}")
    print(f"  max_notional_headroom      : {headroom:>10.2f}")
    print(f"  → per-leg notional target  : ${leg_notional:>10.2f}")
    print(f"  → per-leg notional cap     : ${notional_cap:>10.2f}")
    print(f"  → 10x larger than $1k base : {'YES (per spec)' if eq == 10000 else '⚠ NO (=' + str(eq) + ')'}")
    print()

    # 3. Risk gate bps → dollars at this equity
    print("## 3. Risk gates translated to dollars at $10k × leverage\n")
    daily = cfg.get("max_daily_loss_bps", 0)
    session = cfg.get("max_session_loss_bps", 0)
    daily_dollars = eq * lev * daily / 10000
    session_dollars = eq * lev * session / 10000
    print(f"  max_daily_loss_bps={daily:.0f}    → daily DD trigger ${daily_dollars:.0f}")
    print(f"  max_session_loss_bps={session:.0f}  → session DD trigger ${session_dollars:.0f}")
    print()
    print("  Verify these are tolerable for the scaled capital:")
    print(f"  - 3% of $10k × 10x notional = $3000 daily, $500 session. Plausible.")
    print()

    # 4. Per-variant equity sanity
    print("## 4. Per-variant equity reference\n")
    variants = cfg.get("variants", [])
    for vid, veq in variants:
        marker = "✅" if veq == 10000 else "⚠"
        print(f"  {marker} variant {vid}: ${veq:.0f}")
    if not variants:
        print("  ⚠ no per-variant equity_usd_reference found — check YAML structure")
    print()

    # 5. Things this script CAN'T verify (human checklist)
    print("## 5. Human checklist (cannot be auto-verified)\n")
    print("  [ ] Each Frankfurt sub-account (api_key_index A/B/C) has > $10k USDC available")
    print("  [ ] Lighter account-level max_position_notional ≥ $100k per leg")
    print("  [ ] EQUITY_REFERENCE_USD_{A,B,C}=10000 env override works through #439 hard-fail")
    print("  [ ] entry_post_only_timeout_secs is sufficient for 10x larger orders to fill")
    print("  [ ] Larger orders don't trip Lighter rate limits during entry/exit bursts")
    print("  [ ] Rollback PR pre-written (one-line revert of equity_usd_reference)")
    print("  [ ] Pre-flight done on Tokyo Extended as a dry run (config bump only, no real money)")
    print()
    print("  When all 7 are checked, sign off in bot-strategy#468 with 'pre-flight clean'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
