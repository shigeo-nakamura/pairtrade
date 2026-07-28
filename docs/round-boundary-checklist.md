# Round-boundary restart checklist (Frankfurt `debot-pair-btceth`)

Steps specific to rolling from one evaluation round to the next (e.g. Round 9 → Round 10). This does not replace the per-round issue's own "Boundary rollout" section (see #751 for the Round 9 template) — it's the generic part that applies every time, so it stops depending on each round's author remembering it from scratch.

## Before restart

- [ ] `configs/pairtrade/round.json` and the live YAML(s) updated together in the same commit (see `round.json`'s own header note)
- [ ] confirm on-venue collateral matches every arm's `equity_reference_usd` exactly, and all arms are flat (`position_count=0`)
- [ ] explicit Frankfurt restart approval (see project CLAUDE.md — restart always requires operator sign-off, never automated)
- [ ] **bump `ROUND_ID`** in `/etc/systemd/system/debot-pair-btceth.service.d/round-id.conf` on the host to the new round string (e.g. `round10`), then `sudo systemctl daemon-reload`
  - This is what makes `apply_round_transition()` (pairtrade `risk_io.rs`, bot-strategy#354) fire on the upcoming restart and clear per-instance `total_trades`/`total_wins`/`total_pnl`/`peak_pnl`/`max_dd`/`consecutive_losses`/`session_halted` — the dashboard's "(lifetime)" stats otherwise carry over unchanged, as happened at the Round 9 boundary (bot-strategy#767)
  - If a mid-round phase reset is needed instead (no round change, e.g. a rollout step transition), use `scripts/reset-round-state.sh` directly rather than bumping `ROUND_ID`

## After restart

- [ ] config drift preflight PASS (`scripts/check_config_drift.sh --round-json configs/pairtrade/round.json`)
- [ ] `[CONFIG] variant=... fp=<sha>` startup log lines match the intended round's fingerprints
- [ ] `/opt/debot/risk_state.json`'s `round_id` now reads the new round string, and `total_trades`/`total_wins`/`max_dd`/`peak_pnl` are `0` for every instance
- [ ] no open positions were force-closed by the restart (check status immediately before/after)

## Related

- bot-strategy#354 (round-bound reset mechanism)
- bot-strategy#767 (ROUND_ID was never wired up until 2026-07-28; this checklist exists so it isn't forgotten again)
- bot-strategy#320 (why trade_stats persists across a plain restart in the first place)
