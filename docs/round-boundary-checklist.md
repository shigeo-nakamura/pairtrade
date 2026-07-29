# Round-boundary restart checklist (Frankfurt `debot-pair-btceth`)

Steps specific to rolling from one evaluation round to the next (e.g. Round 9 → Round 10). This does not replace the per-round issue's own "Boundary rollout" section (see #751 for the Round 9 template) — it's the generic part that applies every time, so it stops depending on each round's author remembering it from scratch.

## Before restart

- [ ] `configs/pairtrade/round.json` and the live YAML(s) updated together in the same commit (see `round.json`'s own header note)
- [ ] confirm on-venue collateral matches every arm's `equity_reference_usd` exactly, and all arms are flat (`position_count=0`)
- [ ] explicit Frankfurt restart approval (see project CLAUDE.md — restart always requires operator sign-off, never automated)
- [ ] **bump `round_id`** in the live YAML(s) (`/opt/debot/configs/pairtrade/debot-pair-btceth*.yaml`) to match `round.json`'s `round` field (e.g. `round10`)
  - `round_id` is a **YAML-only** field (`src/pairtrade/config/from_yaml.rs`, `yaml.round_id`) — Frankfurt's `debot-pair-btceth.sh` exports `PAIRTRADE_CONFIG_PATH`, so config loads via `from_yaml_path`, and `apply_env_overrides` has no `ROUND_ID` handling. A systemd `Environment=ROUND_ID=...` drop-in does **not** reach `cfg.round_id` on this path and must not be relied on
  - Setting `round_id` is what makes `apply_round_transition()` (pairtrade `risk_io.rs`, bot-strategy#354) fire on the upcoming restart and clear per-instance `total_trades`/`total_wins`/`total_pnl`/`peak_pnl`/`max_dd`/`consecutive_losses`/`session_halted` — the dashboard's "(lifetime)" stats otherwise carry over unchanged, as happened at the Round 9 boundary (bot-strategy#767)
  - If a mid-round phase reset is needed instead (no round change, e.g. a rollout step transition), use `scripts/reset-round-state.sh` directly rather than bumping `round_id`
- [ ] **first time `round_id` is set at all** (persisted `risk_state.json`'s `round_id` is currently `null` for every instance): `apply_round_transition()` treats configured-but-no-persisted-value as the initial opt-in and does **not** reset anything on that transition (`src/pairtrade/risk_io.rs`, `RiskStateSnapshot::apply_round_transition`). Before this restart, with the service stopped, run `scripts/reset-round-state.sh` (add `--dry-run` first to preview) to zero the per-instance fields directly, then start the service. Skip this step on every subsequent round boundary once a persisted `round_id` already exists

## After restart

- [ ] config drift preflight PASS (`scripts/check_config_drift.sh --round-json configs/pairtrade/round.json`)
- [ ] `[CONFIG] variant=... fp=<sha>` startup log lines match the intended round's fingerprints
- [ ] `/opt/debot/risk_state.json`'s `round_id` now reads the new round string, and `total_trades`/`total_wins`/`max_dd`/`peak_pnl` are `0` for every instance
- [ ] no open positions were force-closed by the restart (check status immediately before/after)

## Related

- bot-strategy#354 (round-bound reset mechanism)
- bot-strategy#767 (round_id was never wired up in the live YAML; this checklist exists so it isn't forgotten again — note a prior systemd-env-only attempt at this fix was ineffective on the YAML config path, see above)
- bot-strategy#320 (why trade_stats persists across a plain restart in the first place)
