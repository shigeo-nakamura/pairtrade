# debot-xvenue-hedge-arcus — second hedge-holder instance (bot-strategy#1123 → #1228)

Second instance of `xvenue_hedge_holder` on Tokyo `debot-main`, separate from
`debot-xvenue-hedge-holder` (#1046). Long leg on **Arcus Perps** (wallet
0x812B…DF05, subaccount 0), short leg on **Lighter Core** sub-account
281474976574527. Operator-armed; it never closes on stop.

| file | host path |
|---|---|
| `debot-xvenue-hedge-arcus.sh` | `/opt/debot/scripts/debot-xvenue-hedge-arcus.sh` (launcher; refuses the #1093 wallet 0xA2C7, a non-#1123 Core account, roll on, a shared status object) |
| `debot-xvenue-hedge-arcus.service` | `/etc/systemd/system/debot-xvenue-hedge-arcus.service` (75 s start stagger behind the first holder) |
| `debot-xvenue-hedge-arcus.env.example` | `/opt/debot/scripts/debot-xvenue-hedge-arcus.env` (root 0600; key lines stay host-only ciphertext) |
| — | `/opt/debot-hedge-arcus/{bin,lib,hedge}` (binary pinned at pairtrade 091fbea, sha256 `39c3d02d…`; state/ARM/DISARM) |

History: #1123 ran QQQ $7.5k/leg 2026-10-04 → 10-07 (DISARMed, unit disabled).
#1228 reuses it for the **one-week Arcus points hold test**: AAPL $20k/leg,
ARM after the 2026-10-28 19:00Z points drop, DISARM after the 11-04 drop, with
the AAPL `arcus-vol` instance on the same wallet disabled for the window
(`deploy/arcus_vol/instances.tsv`).

Operator files under `/opt/debot-hedge-arcus/hedge/`: `ARM` (`AAPL 20000`),
`DISARM` (close both legs, short first), `KILL_SWITCH` (no growth),
`RISK_ACK` (clear a halt). The owner-run scripts for #1228 are listed on the issue.
