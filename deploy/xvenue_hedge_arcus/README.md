# debot-xvenue-hedge-arcus* — Arcus-long / Lighter-Core-short hedge-holder instances (bot-strategy#1123 → #1228 → #1293)

Instances of `xvenue_hedge_holder` on Tokyo `debot-main`, separate from
`debot-xvenue-hedge-holder` (#1046). Long leg on **Arcus Perps**, short leg on
**Lighter Core**. Operator-armed; they never close on stop. Since 2026-11-12 they
are the **#1293 standing Arcus points holds** (held OI earns the weekly drop; the
volume bots on both wallets are stopped). One market per wallet, never the same
market on both wallets.

| instance (launcher argument) | unit | market | Arcus wallet (sub 0) | Lighter Core sub-account | state / status |
|---|---|---|---|---|---|
| (none) | `debot-xvenue-hedge-arcus` | AAPL $40k/leg | 0x812B…DF05 | 281474976574527 | `/opt/debot-hedge-arcus`, `…/xvenue-hedge-arcus.json` |
| `nvda` | `debot-xvenue-hedge-arcus-nvda` | NVDA $20k/leg | 0xA2C7…66d6 (the retired #1093 SPY wallet) | 281474976624889 | `/opt/debot-hedge-arcus-nvda`, `…/xvenue-hedge-arcus-nvda.json` |

| file | host path |
|---|---|
| `debot-xvenue-hedge-arcus.sh` | `/opt/debot/scripts/debot-xvenue-hedge-arcus.sh` — shared launcher; the instance table above is hard-coded in it and it refuses any env whose wallet, Core sub-account, market, status object or roll setting does not match the instance (exit 64, no crash loop) |
| `debot-xvenue-hedge-arcus.service` | `/etc/systemd/system/debot-xvenue-hedge-arcus.service` (75 s start stagger behind the first holder) |
| `debot-xvenue-hedge-arcus-nvda.service` | `/etc/systemd/system/debot-xvenue-hedge-arcus-nvda.service` (105 s stagger; `ExecStart … debot-xvenue-hedge-arcus.sh nvda`) |
| `debot-xvenue-hedge-arcus.env.example` | `/opt/debot/scripts/debot-xvenue-hedge-arcus.env` (root 0600; key lines stay host-only ciphertext) |
| `debot-xvenue-hedge-arcus-nvda.env.example` | `/opt/debot/scripts/debot-xvenue-hedge-arcus-nvda.env` (root 0600; built on the host by the installer, see below) |
| `install_hedge_arcus_nvda.sh` | runs on the host as root: `--verify` reports drift, `--apply --fragment FILE` installs the nvda instance and starts it in DRY_RUN |
| — | `/opt/debot-hedge-arcus*/{bin,lib,hedge}` (binary pinned at pairtrade v4.7.45 build `44787b26…`, copied from the AAPL instance for nvda; state/ARM/DISARM) |

Tests: `scripts/test_xvenue_hedge_arcus_launcher.sh`, `scripts/test_install_hedge_arcus_nvda.sh`
(run by `.github/workflows/test-scripts.yml`).

## Keys (nvda instance)

Nothing is decrypted on the workstation or printed anywhere. The installer fills the
env on the host:

- **Arcus wallet 0xA2C7**: `ARCUS_API_KEY` / `ARCUS_API_PRIVATE_KEY` are copied from
  `/etc/debot-arcus-vol/live.env` (the stopped SPY volume bot's credentials, already
  `scripts/encrypt.py`-shaped ciphertext under the host data key — the installer checks
  that `live.env` carries the same `ENCRYPTED_DATA_KEY` as `debot_secrets_common.env`).
  The volume units on that wallet must be stopped **and disabled** or the installer refuses.
- **Lighter Core 281474976624889**: the owner creates the API key and hands the installer a
  mode-600 **fragment** with exactly `LIGHTER_ACCOUNT_INDEX_CORE`, `LIGHTER_API_KEY_INDEX_CORE`,
  `LIGHTER_PUBLIC_API_KEY_CORE`, `LIGHTER_PRIVATE_API_KEY_CORE` (the two keys as ciphertext,
  raw 40-byte form as `decrypt_data_with_kms(.., output_as_hex=true)` expects). The owner-run
  workstation script that builds the fragment is listed on bot-strategy#1293.

## History

#1123 ran QQQ $7.5k/leg 2026-10-04 → 10-07 (DISARMed). #1228 reused the instance for the
Arcus points hold test: AAPL $20k/leg 10-28 → 11-04 (+3.27 pt, INCONCLUSIVE), $40k/leg
11-04 → 11-11 (+9.10 pt, INCONCLUSIVE → DISARMed 11-12 07:46Z). Owner decision 11-12
(#1293): held OI beats volume for points ($9.7/pt on the SPY bot), so AAPL $40k/leg
becomes a standing hold and the idle SPY wallet gets a second hold, NVDA $20k/leg
(Lighter Core NVDA funding received ≈ +0.4 %/week vs Arcus 0.084 %/week paid).

Operator files under `<state dir>/hedge/`: `ARM` (`AAPL 40000` / `NVDA 20000`),
`DISARM` (close both legs, short first), `KILL_SWITCH` (no growth), `RISK_ACK` (clear a
halt). ARM / DISARM / size changes are owner-run (scripts on the issue).
