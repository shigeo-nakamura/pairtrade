# Robinhood Lighter operations

This runbook covers the dedicated `debot-pair-robinhood-lighter` deployment.
Use UTC in every check and preserve venue exports under `~/bot/logs/`.

## Safe stop and rollback

1. Block new entries with `/opt/debot/KILL_SWITCH`. `scripts/debot-pair-robinhood-lighter.sh`
   does not set `KILL_SWITCH_PATH`, so `sentinel.rs` resolves the default global path, not a
   path under `STATE_DIR` (`/opt/debot-robinhood-lighter`) — creating that directory's
   `KILL_SWITCH` file does **not** block entries on this deployment.
2. Confirm each arm's latest `[METRICS]` line reports `elig=true`, then verify
   BTC and ETH positions and pending orders directly against the venue. Do not
   infer flatness from `ENTRY` logs alone.
3. If either arm is open, allow its normal exit unless an explicit emergency
   close is authorized. A restart invokes account-wide startup cleanup and can
   MARKET-close unrelated positions.
4. When venue ground truth is flat and pending orders are zero, stop the unit.
5. Restore a reviewed config from `/opt/debot/backups/`, or deploy the desired
   repository revision through the no-restart workflow. Compare the deployed
   config SHA before starting.
6. Start the unit only after the rollback target, binary SHA, credentials,
   endpoint, and flat account state are independently verified. Keep the kill
   switch engaged through startup checks.
7. Verify `[CONFIG]` fingerprints for every arm, `dry_run`, equity reference,
   leverage, WebSocket subscriptions, status S3, Prometheus, risk gates, and
   `No open positions detected`. Release the kill switch only by explicit
   operator decision.

## Withdrawal

1. Engage the kill switch and wait for venue-confirmed BTC/ETH flatness and
   zero pending orders.
2. Stop the service so no order can race the withdrawal.
3. In the Robinhood Lighter UI, verify the source account/sub-account, USDG
   token, destination chain, destination address, bridge/withdrawal route,
   minimum, fees, and required ETH gas. Never reuse assumptions from the
   standard Lighter deployment.
4. Send a small test withdrawal first. Record the venue transaction/order ID,
   chain transaction hash, amount, destination, and UTC timestamp.
5. Confirm finality and the exact credited asset/amount at the destination
   before withdrawing the remainder.
6. Re-read venue equity, positions, and pending orders. Keep the service stopped
   or kill-switched until its equity reference and risk limits are deliberately
   updated for the remaining collateral.

Never store API keys, wallet secrets, or encrypted credential payloads in this
repository or an issue comment.
