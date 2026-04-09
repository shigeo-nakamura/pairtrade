# Issue #25 cutover runbook — single-process A/B/C

This runbook walks through migrating the three live BTC/ETH bots
(`debot-pair-btceth`, `-b`, `-c`) from three independent systemd
services into a single `debot-pair-btceth` process that runs all three
strategy variants as `StrategyInstance`s sharing one WS feed,
BarBuilder, and spread history.

Background: shigeo-nakamura/bot-strategy#25.

## Status (as of commit 7)

**Not yet ready to flip — one more engine commit needed.** Commits 1–7
of #25 land all the supporting plumbing:

- `StrategyInstance` struct, `Vec<StrategyInstance>` on the engine
- Multi-strategy YAML schema (`strategies:` list) with legacy fallback
- Per-instance Lighter env credential lookup (`LIGHTER_*_<ID_UPPER>`)
- Per-instance pnl/status file keying (`-<id>` suffix)
- Graceful shutdown that waits for every instance × every pair
- An engine `step()` wrapper that loops over `self.instances`
- Removed the legacy `debot-pair-btceth-{b,c}` YAML and unit files

But the engine still runs `instances.len() == 1`. The reason is that
`step_for_instance` calls `fetch_latest_prices()` per iteration, which
on a `ReplayConnector` advances the clock — so iterating per instance
would have each variant see a different replay tick instead of the
same shared tick. Splitting `step()` into a **shared phase**
(price/bar/history update + `evaluate_pair()`) and a **per-instance
phase** (decision + order placement) is its own follow-up commit.

The new YAML in `configs/pairtrade/debot-pair-btceth.yaml` ships the
multi-strategy `strategies:` block commented out, with the variant
parameters preserved verbatim from the old `-b.yaml` / `-c.yaml`
files so the cutover only requires uncommenting once the engine split
lands.

## Pre-flight

1. **Tag the master commit** that contains commits 1–7 of #25
   (`git log --oneline | head -10` on `pairtrade`). Roll-back means
   reverting to the parent of commit 1 (`c981bf4^`).
2. **Snapshot the EC2 host's pnl/status directories** so the migration
   is reversible:
   ```sh
   ssh debot 'sudo tar -czf /tmp/pnl-status-pre-25.tgz \
     /home/ec2-user/debot_pnl /home/ec2-user/debot_status'
   scp debot:/tmp/pnl-status-pre-25.tgz ./backups/
   ```
3. **Confirm the three current services are running** and capture their
   open positions for diff against the new process:
   ```sh
   ssh debot 'for s in debot-pair-btceth debot-pair-btceth-b \
     debot-pair-btceth-c; do echo == $s ==; \
     sudo journalctl -u $s -n 5 --no-pager; done'
   ```

## Phase 1 — Shadow run (dry-run, no real orders)

The new build defaults to a single `StrategyInstance` if the YAML has
no `strategies:` block, so we can safely deploy the binary first and
shadow-run the new YAML out-of-band.

1. Deploy the new binary via the existing CI workflow. The
   single-bot YAML on disk still has the legacy scalars, so the live
   `debot-pair-btceth` keeps running unchanged.
2. Stage the multi-strategy YAML side by side:
   ```sh
   ssh debot 'sudo cp /opt/debot/configs/pairtrade/debot-pair-btceth.yaml \
     /opt/debot/configs/pairtrade/debot-pair-btceth.shadow.yaml'
   ```
   Verify the file already contains the `strategies:` block from
   commit 7 of #25; if not, manually copy it from the repo.
3. Provision per-instance Lighter credentials in `/etc/debot.env` (or
   wherever systemd reads them) using the `_<INSTANCE_ID_UPPER>`
   suffix convention from commit 3:
   ```
   LIGHTER_PUBLIC_API_KEY_A=<...>
   LIGHTER_PRIVATE_API_KEY_A=<...>
   LIGHTER_ACCOUNT_INDEX_A=<sub-account A id>
   LIGHTER_PUBLIC_API_KEY_B=<...>
   LIGHTER_PRIVATE_API_KEY_B=<...>
   LIGHTER_ACCOUNT_INDEX_B=<sub-account B id>
   LIGHTER_PUBLIC_API_KEY_C=<...>
   LIGHTER_PRIVATE_API_KEY_C=<...>
   LIGHTER_ACCOUNT_INDEX_C=<sub-account C id>
   ```
   The unsuffixed `LIGHTER_*` env vars stay populated as the fallback
   for the legacy single-instance path until phase 3.
4. Start a one-shot dry-run process from a screen/tmux:
   ```sh
   ssh debot
   sudo systemctl set-environment DRY_RUN=true
   sudo -u ec2-user bash -lc '
     DRY_RUN=true \
     PAIRTRADE_CONFIG_PATH=/opt/debot/configs/pairtrade/debot-pair-btceth.shadow.yaml \
     DEBOT_PNL_DIR=/home/ec2-user/debot_pnl_shadow \
     DEBOT_STATUS_DIR=/home/ec2-user/debot_status_shadow \
     /opt/debot/debot 2>&1 | tee /tmp/shadow-run.log
   '
   ```
   The shadow process writes to dedicated `debot_pnl_shadow/` and
   `debot_status_shadow/` directories so it can't collide with the
   three production services.
5. Let the shadow run for **at least 4 hours during active market
   hours**. Compare per-instance ZCHECK / ENTRY / EXIT lines against
   the production services' journals:
   ```sh
   for inst in a b c; do
     grep "\\[ZCHECK\\] BTC/ETH" /tmp/shadow-run.log \
       | grep -F "[$inst]" | tail -20
   done
   diff <(...production...) <(...shadow...)
   ```
   `z`, `entry`, `std`, `mean`, `spread`, `eligible`, and `beta_gap`
   should be **byte-identical** across all three shadow instances at
   any given timestamp (that is the property #25 was filed to fix).

   ZCHECK divergence is the abort signal. If any instance disagrees by
   more than rounding noise, stop and investigate before phase 2.

## Phase 2 — Drain the legacy services

Once the shadow run has been clean for several hours:

1. Stop new entries on the three legacy services by setting
   `OBSERVE_ONLY=true` in their environment files and restarting:
   ```sh
   ssh debot 'for s in debot-pair-btceth debot-pair-btceth-b debot-pair-btceth-c; do
     sudo systemctl set-environment OBSERVE_ONLY=true
     sudo systemctl restart $s
   done'
   ```
   Existing positions will continue to follow exit_z / stop_loss_z /
   force_close_secs as today.
2. **Wait for all three services to flatten** their positions. Watch
   each one's `status.json`:
   ```sh
   ssh debot 'watch -n 5 "jq .open_positions \
     /home/ec2-user/debot_status/{debot-pair-btceth,debot-pair-btceth-b,debot-pair-btceth-c}/status.json"'
   ```
3. When all three are flat, stop and disable the b/c services
   permanently. Leave the primary service (`debot-pair-btceth`)
   stopped but enabled, ready for the new YAML:
   ```sh
   ssh debot 'sudo systemctl stop debot-pair-btceth-b debot-pair-btceth-c \
     && sudo systemctl disable debot-pair-btceth-b debot-pair-btceth-c \
     && sudo systemctl stop debot-pair-btceth'
   ```
4. (Optional cleanup) Remove the now-unused unit files:
   ```sh
   ssh debot 'sudo rm /etc/systemd/system/debot-pair-btceth-b.service \
     /etc/systemd/system/debot-pair-btceth-c.service \
     && sudo systemctl daemon-reload'
   ```
   Commit 7 of #25 already removed these from the repo's `deploy/`
   directory; this is the matching change on the host.

## Phase 3 — Migrate pnl history and cut over

Lifetime stats (`Trades`, `Win Rate`, `Max DD`) are reconstructed on
boot by replaying `~/debot_pnl/<tag>/pnl-<tag>-*.jsonl`. The new
process writes with tag `debot-pair-btceth-<id>` (commit 4), so
without a rename it would not find the existing files for variant A
and the dashboard would show 0 trades.

1. **Rename the existing pnl files for variant A** (the only one whose
   history we want to inherit, since A is the production champion;
   B/C history was already an A/B test and can be archived):
   ```sh
   ssh debot
   cd /home/ec2-user/debot_pnl
   for f in pnl-debot-pair-btceth-*.jsonl; do
     # Skip files already suffixed (-a/-b/-c) from a prior run
     case "$f" in *-a-*.jsonl|*-b-*.jsonl|*-c-*.jsonl) continue;; esac
     new=$(echo "$f" | sed 's/^pnl-debot-pair-btceth-/pnl-debot-pair-btceth-a-/')
     sudo mv "$f" "$new"
   done
   ```
   Archive B/C history out of the way:
   ```sh
   sudo mkdir -p /home/ec2-user/debot_pnl_archive_pre25
   sudo mv pnl-debot-pair-btceth-b-*.jsonl pnl-debot-pair-btceth-c-*.jsonl \
     /home/ec2-user/debot_pnl_archive_pre25/ 2>/dev/null || true
   ```
2. **Move the existing status directory** to match the new
   `<dir>/<id>-<instance>/status.json` layout (commit 4):
   ```sh
   sudo mkdir -p /home/ec2-user/debot_status/debot-pair-btceth-a
   sudo mv /home/ec2-user/debot_status/debot-pair-btceth/status.json \
     /home/ec2-user/debot_status/debot-pair-btceth-a/status.json
   sudo mv /home/ec2-user/debot_status/debot-pair-btceth/equity.json \
     /home/ec2-user/debot_status/debot-pair-btceth-a/equity.json 2>/dev/null || true
   sudo mv /home/ec2-user/debot_status/debot-pair-btceth/equity_history.jsonl \
     /home/ec2-user/debot_status/debot-pair-btceth-a/equity_history.jsonl 2>/dev/null || true
   ```
3. **Update the dashboard config** (`debot-dashboard/config.yaml`) to
   point at three `status_path`s instead of one. See the example diff
   in the dashboard repo's same-day commit. Restart the dashboard.
4. **Promote the shadow YAML** to the production path and start the
   single combined service in real-trade mode:
   ```sh
   ssh debot 'sudo cp /opt/debot/configs/pairtrade/debot-pair-btceth.shadow.yaml \
     /opt/debot/configs/pairtrade/debot-pair-btceth.yaml \
     && sudo systemctl unset-environment DRY_RUN OBSERVE_ONLY \
     && sudo systemctl restart debot-pair-btceth'
   ```
5. **Confirm**:
   - `journalctl -u debot-pair-btceth -f` shows `[a]`, `[b]`, `[c]`
     prefixes interleaved on shutdown / position log lines.
   - `ls /home/ec2-user/debot_pnl/` shows fresh
     `pnl-debot-pair-btceth-{a,b,c}-<date>.jsonl` files appearing on
     the next exit.
   - Dashboard shows three cards (a/b/c) with non-zero lifetime stats
     for `a` (inherited) and zero for `b`/`c` (fresh).
   - Three sub-account balances on the Lighter side update
     independently.

## Roll-back

If anything goes wrong in phase 3:

1. Stop the combined service:
   `sudo systemctl stop debot-pair-btceth`
2. Restore the legacy YAML from
   `/opt/debot/configs/pairtrade/debot-pair-btceth.yaml.legacy`
   (snapshot before promoting the shadow), and rename back:
   ```sh
   for f in pnl-debot-pair-btceth-a-*.jsonl; do
     sudo mv "$f" "${f/-a-/-}"
   done
   sudo mv /home/ec2-user/debot_status/debot-pair-btceth-a/status.json \
     /home/ec2-user/debot_status/debot-pair-btceth/status.json
   ```
3. Revert the binary on the host to the snapshot taken in pre-flight
   step 2:
   ```sh
   sudo tar -xzf ./backups/pnl-status-pre-25.tgz -C /
   ```
4. Re-enable the b/c services and restart all three.
5. Open a follow-up issue describing what diverged.

## Out of scope

- `slow-mm` is unaffected (separate engine, separate story — see
  pairtrade#11).
- Hyperliquid path was not touched; only Lighter env loading gained
  the suffix lookup.
