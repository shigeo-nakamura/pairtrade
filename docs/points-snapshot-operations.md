# Points snapshot operations (bot-strategy#938)

The subsidy KPI is cost per point. `scripts/subsidy_ledger.py` supplies the
cost; the points denominator comes from the venue itself, snapshotted hourly
by `scripts/robinhood_points_collector.py`.

## What runs where

| Host | Timer | Arms | S3 object |
|---|---|---|---|
| Tokyo `i-0095af4fe0efbc5dd` | `robinhood-points-snapshot.timer` (:05 UTC) | `freq`, `b` (Robinhood chain), `core-canary` (the #1046 hedge's Core leg) | `s3://debot-dashboard/debot/status/robinhood-lighter/points_history.jsonl` |
| Frankfurt `i-0c08fba996bc21879` | `lighter-core-points-snapshot.timer` (:10 UTC) | `bull-holder` (Lighter Core) | `s3://debot-dashboard/debot/status/lighter-core/points_history.jsonl` |

**The two hosts must never share one S3 URI.** The mirror is a whole-file
`aws s3 cp` of the host's local history, so a shared object would mean each
host overwriting the other's series.

Both timers are installed once by the owner and never by CI:

```bash
sudo bash /opt/debot/scripts/install_robinhood_points_snapshot.sh      # Tokyo
sudo bash /opt/debot/scripts/install_lighter_core_points_snapshot.sh   # Frankfurt
systemctl list-timers '*points-snapshot.timer'
journalctl -u lighter-core-points-snapshot.service --no-pager | tail -20
```

Neither installer touches a trading service. The Frankfurt unit runs as root
because `debot-bull-holder.env` is 0600 root and the holder reads it as root:
relaxing it to 0640 would widen who can read a live trading key in order to
run a KPI collector.

## Which tally to difference

`robinhood_points_daily.py --tally` is required, not defaulted, because the
venue publishes two series and does not document how they relate. What the
data has shown so far:

- **Robinhood chain → `live_points_total`.** Over the Friday drop of
  2026-09-18, `user_total_points` stayed 0 on both arms while
  `live_points_total` jumped (`freq` +6.6684, `b` +0.0573) and never reset.
- **Lighter Core → `total_points`.** `livePoints/total` answers 403 on
  mainnet; only `referral/points` is readable there.
- **The weekly credit lands on the main account.** The sub-account's
  activity is aggregated into it, so a per-arm cost per point is not
  meaningful on the Robinhood side — read the program-level number.

## Readout

```bash
aws s3 cp s3://debot-dashboard/debot/status/robinhood-lighter/points_history.jsonl .
scripts/robinhood_points_daily.py --tally live_points_total --instance rh \
  points_history.jsonl --out pts_rh.jsonl
scripts/subsidy_ledger.py \
  --exec-glob 'execution-debot-pair-robinhood-lighter_*.jsonl' \
  --pnl-glob 'pnl-*.jsonl' --points pts_rh.jsonl --out ledger_rh.jsonl
```

The execution and PnL ledgers live on the trading host under
`/home/ec2-user/debot_pnl/` (Tokyo). Equity histories, which bound the cost
exactly because they include everything, are in the same S3 status prefix as
the points.

A window that contains a Friday credit but not the whole week of activity
that earned it prices points too cheaply: the credit rewards ~7 days while
the measured cost covers only the window. Say which window a number came
from whenever one is quoted.
