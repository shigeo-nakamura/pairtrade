# Round 8 scenario configs (bot-strategy#530 / #664, readout #581 on 2026-07-05)

Pre-drafted decision-tree branches for the Frankfurt `debot-pair-btceth` Round-8
rollout, following the round6 scenario pattern (#467). **Nothing here is loaded by
the service** — a scenario ships by copying it over the live files in ONE commit:

```bash
cp configs/pairtrade/scenarios/round8/round8-<CHOSEN>.yaml       configs/pairtrade/debot-pair-btceth.yaml
cp configs/pairtrade/scenarios/round8/round8-<CHOSEN>.round.json configs/pairtrade/round.json
# -> PR -> merge (Deploy Configs syncs to the host, does NOT restart — #269/#580)
```

## Decision tree (07-05 readout, judged on normalized metrics: bps of ref, per-trade Sharpe, Calmar, MaxDD%, full window + last-7d holdout)

| readout outcome | scenario | A (eq2000) | B (eq1000) | C (eq1000) |
|---|---|---|---|---|
| C (sl-widen) confirms ≥ A | **round8-promote-sl8** | ez0.2/sl8 (C promoted) | ez0.1/sl8 (exit_z knob) | ez0.2/sl4 (old-A control) |
| C does not confirm | **round8-hold-incumbent** | ez0.2/sl4 (unchanged) | ez0.1/sl4 (#664 exact) | ez0.2/sl8 (isolate cont.) |
| persistent-drift regime active at readout (#664 note 2) | either, with **B := clone of A** (defer exit_z one round) | — | — | — |

All arms `fc=10800` (fc-extension rejected, #664), `max_leverage=10`. Capital
$4000 = 2000/1000/1000 per the #530 owner decision (2026-07-02): the raise rides
only on the incumbent; challengers stay $1000 (session-DD worst case ≈ $50 each).

Prep-time status (2026-07-03, data through 07-03):
- Live: C ≥ A on BOTH full window and holdout (A +105.6 bps / Sharpe 0.036 vs
  C +343.1 bps / 0.132 full; holdout A 127.3/0.127 vs C 233.3/0.224). B halted 06-22, frozen.
- Forward OOS 06-27..07-03 (byte-exact): exit_z 0.1 −$6.78 vs 0.2 (5/8 windows win
  overall, +15–18% aggregate). Regime detector inactive (cusum 0.0) at 07-03 —
  defer condition currently NOT met.

## Restart prerequisites (same restart = execution-ledger unlock, #613)

1. **Collateral to $4000 total** (2000/1000/1000). Real deposits ~$2950 as of
   07-01 → needs ~+$1050 fresh deposit + sub-account rebalance BEFORE live-flip
   (#439: references must be backed).
2. **Fresh binary**: live process (started 06-16 05:55Z) predates the #155
   execution-ledger merge. Deploy current master binary; ledger is
   enabled-by-default (`execution_ledger.rs`), no config needed.
3. **`USE_AMEND_ON_PARTIAL_FILL` drop-in** must survive the restart (code default
   is false; the #471 fix lives in a systemd drop-in — verify
   `systemctl cat debot-pair-btceth` shows it, and journal shows amend, not reissue).
4. Config via repo → master → CI Deploy Configs (no manual scp).
5. **Frankfurt restart requires explicit owner approval** (feedback_no_frankfurt_restart).
   Before restart: check open positions ([METRICS] elig+z) — startup force-close
   costs ~50 bps slippage on open positions.
6. After restart: `[CONFIG]` fingerprint per variant + `scripts/check_config_drift.sh
   --round-json configs/pairtrade/round.json` (exit 0) + `execution-debot-pair-btceth-*.jsonl`
   appearing under the PnL dir. Reset round state (reset-round-state.sh) so the
   window starts clean.
