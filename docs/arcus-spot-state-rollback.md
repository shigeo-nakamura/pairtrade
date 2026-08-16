# Arcus Spot live-tick state preservation and binary rollback

This runbook covers the `arcus-spot-live-tick` checkpoint/recovery boundary on
the dedicated Arcus host. It is deliberately separate from unit installation
and Health Watch configuration. The commands added here are offline state
inspection tools; they do not create an RPC client, load a KMS signer, invoke
the executor, submit a transaction, restore state, or alter a service/timer.

Running a live acceptance window is a different operation. Starting the
oneshot service executes one `live-tick`, which can sign and submit a swap if
the strategy produces a valid entry/exit plan. Do not perform the acceptance
section without explicit approval for that window.

## Recovery boundary

The approved config currently binds the state paths below. The config itself
is not copied into a backup because it may contain operational RPC endpoints;
instead the manifest binds its canonical SHA-256 digest.

| Artifact | Current path | Format / role |
|---|---|---|
| Runtime checkpoint | `/var/lib/debot-arcus/spot-execute-once/runtime_state.json` | schema 1; exact runtime config plus signal history, last token A/B reference prices, inventory, regime, risk state and last committed execution key |
| Execution ledger | `/var/lib/debot-arcus/spot-execute-once/ledger.json` | schema 2; monotonic attempt sequence, immutable archive and any active recovery state |
| Recovery evidence | `/var/lib/debot-arcus/spot-execute-once/live-tick-pending-plan.json` | optional schema-1 envelope: exact plan, recorder snapshot and evaluation time needed by `auto-resume` and continuity verification |
| Observation evidence | `/var/lib/debot-arcus/spot-execute-once/live-tick-observation-evidence.json` | optional schema-2 sidecar: latest sequence-advancing recorder snapshot, evaluation time and resulting runtime sequence/watermark; schema 1 remains readable only as an unchanged backup baseline |
| Namespace lock | `/var/lib/debot-arcus/spot-execute-once/.runtime_state.json.lock` | mode-0600 process lock shared by live-tick, execute/resume, proposer and state tooling |

Checkpoint and ledger stores already use create-new temporary files, file
`fsync`, atomic rename and parent-directory `fsync`. `live-tick` writes the
pending recovery and observation evidence while holding the checkpoint
namespace lock; `arcus-spot-propose-plan propose` writes the same observation
evidence whenever it advances that shared checkpoint. A backup therefore
observes a lock-consistent boundary. Backup schema 3 hashes and copies both
optional sidecars and rejects observation evidence whose schema-1 snapshot
watermark or schema-2 result boundary does not match the checkpoint. If evidence
publication succeeded but checkpoint publication failed, state tooling safely
omits only an exactly one-sequence-newer schema-2 orphan; every other mismatch
remains a hard error.

The state tool uses strict `load_existing` reads. A missing file is an error;
it is never accepted as first-run state. Ledger inspection does not run the
normal restart recovery that rewrites `Dispatching` to `Unknown`. The tool
also refuses to create a missing lock, preventing a root-run inspection from
leaving behind a root-owned lock that the `arcus` service could not reopen.

## Offline commands

```text
arcus-spot-execute-once state-backup CONFIG_YAML BACKUP_DIR
arcus-spot-execute-once state-verify-exact CONFIG_YAML BACKUP_DIR
arcus-spot-execute-once state-verify-continuity CONFIG_YAML BACKUP_DIR
```

`state-backup` requires an absolute, nonexistent destination whose parent
already exists. It takes the existing executor lock, canonically validates the
checkpoint and ledger, validates the optional recovery/observation evidence,
and publishes a mode-0700 directory through a hidden staging directory plus
atomic rename.
Every copied file and the manifest are mode 0600 and fsynced. The manifest
records:

- the UTC capture time that opens the approved one-tick verification window;
- canonical approved-config digest;
- byte length and SHA-256 of checkpoint, ledger and both optional evidence
  sidecars;
- non-secret continuity summaries (runtime sequence/history/watermark,
  inventory/regime/open quantity, ledger next sequence/archive/active phase).

`state-verify-exact` validates the backup first, then requires every current
state artifact to match its recorded bytes. Use this while the timer remains
stopped, before and after a binary-only rollback.

`state-verify-continuity` is the post-start check. It allows normal tick/fill
advancement but rejects:

- runtime sequence or last-observation regression, more than one observation
  advance, a watermark outside the backup-to-verification window, or
  corruption/removal/reordering of retained signal-history samples (a full
  window may shift by exactly one sample for the approved tick). A successful
  no-swap advance must provide schema-2 result-bound evidence, independently
  replay to `Observe` from its preserved recorder snapshot/evaluation time and
  exactly reproduce checkpoint state;
- cumulative-equity baseline changes, mismatched first equity baselines,
  sticky or newly-required risk-halt loss, same-day daily baseline resets,
  UTC rollover days outside the backup-to-verification window, invalid
  rollover baselines, or loss of the last equity mark;
- a backup that was not neutral with no active attempt, ledger regression,
  changed archived history, more than one new acceptance attempt, or an
  acceptance that exceeds the preserved UTC-day swap allowance;
- an unresolved/non-reconciled acceptance attempt, one without a confirmed
  router status, or one not bound to the preserved pending plan and schema-2
  recorder evidence. The verifier reruns `step_at`
  from the backup checkpoint with that snapshot/evaluation time and requires
  an exact plan match, independently recomputing route linkage/loss, quote
  freshness, signal, sizing and inventory projection before checking the
  configured sell ceiling and slippage buy floor,
  its exact reconciled wallet-balance delta, both configured raw inventory
  floors and minimum gas balance, a genuine entry-signal crossing, the
  supported entry direction, runtime quote freshness, a non-negative route
  cost, configured all-in-cost and strategy notional/rotation/imbalance caps,
  and attempt chronology following the accepted observation. Notional is
  recomputed from the accepted tick's checkpointed reference prices after
  those prices are cross-checked against both its signal sample and equity
  mark;
- inventory/regime/rotation/open-quantity/execution-key changes that do not
  exactly equal applying that one reconciled fill to the backup position.

Continuity is intentionally weaker than exact identity. Always obtain an
exact pass while stopped; continuity is supplementary evidence after a tick.

## Preflight (read-only)

Access the host through SSM; inbound SSH remains closed. Record all timestamps
in UTC.

1. Confirm the timer/service names and current binary provenance:

   ```bash
   sudo systemctl status arcus-spot-live-tick.timer --no-pager
   sudo systemctl status arcus-spot-live-tick.service --no-pager
   sudo systemctl cat arcus-spot-live-tick.service
   sudo sha256sum /usr/local/bin/arcus-spot-execute-once
   sudo find /usr/local/share/arcus-spot-execute-once/releases -maxdepth 2 -name manifest.json -type f -print
   ```

2. Do not open a rollback window when the ledger has an active attempt or the
   runtime is rotated. Use the most recent previously recorded state report;
   if no report exists, create the backup only after the timer stop in the
   approved procedure below and inspect `ledger.active_sequence` and
   `runtime.regime` in its JSON output. Required safe baseline:

   - `ledger.active_sequence: null`
   - `runtime.regime: "neutral"`

   If either condition is false, wait for normal reconciliation/exit. Do not
   delete or edit a checkpoint, ledger or evidence sidecar to force the gate.

3. Identify three explicit immutable releases: the currently installed
   release, the rollback candidate, and a known-good verifier release. The
   verifier may be the current release, but it must be distinct from the
   rollback candidate and must contain the current `state-backup`,
   `state-verify-exact`, and `state-verify-continuity` checks. Compare each
   `manifest.json` with its binary and checksum and record the verifier's fully
   resolved release directory outside the host. Do not select by `ls | tail`,
   mtime, a glob, or an unresolved symlink. The rollback candidate must support
   the on-disk checkpoint schema 1 and ledger schema 2 **and must deserialize
   and re-persist both `last_token_a_reference_price_usd` and
   `last_token_b_reference_price_usd` without dropping them**. Schema-number
   compatibility alone is insufficient: an older schema-1 binary that predates
   those fields is not an eligible live rollback candidate because continuity
   cannot independently verify its accepted price/equity/notional state. The
   candidate must also write schema-2
   `live-tick-observation-evidence.json` for every sequence-advancing
   observation before persisting its checkpoint, including structurally invalid
   ticks, and bind the resulting runtime sequence/watermark; a binary that only
   writes the rotation-plan sidecar cannot certify no-swap state. If the
   candidate release's
   source/provenance cannot demonstrate that
   capability, use a forward fix. Every state backup or verification below
   runs the manifest-verified
   verifier binary from its immutable release directory, never the mutable
   `/usr/local/bin` path or the rollback candidate under test.

## Approved stop / exact backup / binary rollback

These steps mutate service scheduling and the installed binary. They are not
authorized by merely merging this code or by a read-only audit.

1. Before changing scheduling, run both queries below and record their literal
   outputs outside the host as `TIMER_ENABLED_BEFORE` and
   `TIMER_ACTIVE_BEFORE`:

   ```bash
   sudo systemctl is-enabled arcus-spot-live-tick.timer
   sudo systemctl is-active arcus-spot-live-tick.timer
   ```

   Continue only when the enabled value is exactly `enabled` or `disabled` and
   the active value is exactly `active` or `inactive`. A different value such
   as `masked`, `activating`, `deactivating` or `failed` requires diagnosis;
   do not normalize it as part of this procedure.

   Temporarily disable the timer first so an EC2 restart cannot reactivate it
   before exact verification, then stop it and poll for at most 120 seconds
   for any in-flight oneshot to become naturally inactive:

   ```bash
   sudo systemctl disable arcus-spot-live-tick.timer
   sudo systemctl stop arcus-spot-live-tick.timer
   for attempt in $(seq 1 60); do
     service_state="$(sudo systemctl show arcus-spot-live-tick.service \
       -p ActiveState --value)"
     if [ "$service_state" = "inactive" ]; then
       break
     fi
     if [ "$attempt" -eq 60 ]; then
       echo "oneshot did not become inactive within 120 seconds; abort" >&2
       exit 1
     fi
     sleep 2
   done
   sudo systemctl is-active arcus-spot-live-tick.timer
   sudo systemctl show arcus-spot-live-tick.service \
     -p ActiveState -p SubState -p Result -p ExecMainStatus
   ```

   The timer and service must both report `inactive` before continuing. This
   procedure must never run `systemctl stop` or `systemctl kill` against the
   service: if the bounded wait expires, leave the timer stopped and diagnose
   the still-running oneshot without signalling it. It may be between dispatch
   and ledger persistence.

2. Create a root-owned backup parent once, then choose an explicit UTC-stamped
   destination (replace the placeholder before running):

   ```bash
   sudo install -d -o root -g root -m 0700 /var/lib/debot-arcus/spot-state-backups
   sudo /usr/local/share/arcus-spot-execute-once/releases/<known-good-verifier-release>/arcus-spot-execute-once state-backup \
     /etc/arcus-spot/config.yaml \
     /var/lib/debot-arcus/spot-state-backups/<YYYYMMDDTHHMMSSZ>-pre-rollback
   sudo /usr/local/share/arcus-spot-execute-once/releases/<known-good-verifier-release>/arcus-spot-execute-once state-verify-exact \
     /etc/arcus-spot/config.yaml \
     /var/lib/debot-arcus/spot-state-backups/<YYYYMMDDTHHMMSSZ>-pre-rollback
   ```

   Save both JSON outputs. Reconfirm the neutral/no-active baseline. A backup
   failure leaves only a hidden staging directory; it never alters live state.
   Remove a failed staging directory only after resolving its exact generated
   path and confirming no state file points there.

3. Validate the explicit rollback release before installation. Substitute the
   fully resolved immutable release directory from preflight:

   ```bash
   cd /usr/local/share/arcus-spot-execute-once/releases/<release-id>
   sudo sha256sum -c arcus-spot-execute-once.sha256
   sudo jq . manifest.json
   ```

4. Install only the binary through a same-filesystem staging file and atomic
   rename. Do not copy, delete or restore anything under the live state path:

   ```bash
   sudo install -o root -g root -m 0755 \
     /usr/local/share/arcus-spot-execute-once/releases/<release-id>/arcus-spot-execute-once \
     /usr/local/bin/arcus-spot-execute-once.rollback-new
   sudo sha256sum /usr/local/bin/arcus-spot-execute-once.rollback-new
   sudo mv -f /usr/local/bin/arcus-spot-execute-once.rollback-new \
     /usr/local/bin/arcus-spot-execute-once
   ```

5. While the timer is still stopped, prove the rollback did not touch state:

   ```bash
   sudo /usr/local/share/arcus-spot-execute-once/releases/<known-good-verifier-release>/arcus-spot-execute-once state-verify-exact \
     /etc/arcus-spot/config.yaml \
     /var/lib/debot-arcus/spot-state-backups/<YYYYMMDDTHHMMSSZ>-pre-rollback
   ```

   Do not substitute `/usr/local/bin/arcus-spot-execute-once` here: it now
   names the rollback candidate and cannot be trusted to certify itself. Do
   not start the candidate until this independent exact check passes.

At this point binary rollback and byte-exact state preservation are proven
without invoking the executor. It is safe to stop here and leave the timer
inactive pending a separately approved live acceptance.

## Explicitly approved one-tick/start acceptance

Starting the service can trade. Reconfirm explicit approval, neutral runtime,
no active ledger attempt, current gas/risk gates, and the exact intended
binary SHA before continuing.

1. Invoke exactly one tick with the timer still stopped:

   ```bash
   sudo systemctl start arcus-spot-live-tick.service
   sudo systemctl show arcus-spot-live-tick.service \
     -p ActiveState -p SubState -p Result -p ExecMainStatus
   sudo journalctl -u arcus-spot-live-tick.service --since '<UTC-start>' --no-pager
   ```

2. If the ledger reports `Submitted`, `Unknown`, `OperatorHold`, or another
   active phase, stop. Keep the timer disabled and follow `auto-resume` using
   the preserved pending plan. Never replace the ledger/checkpoint from the
   backup to make the active attempt disappear.

3. With no unresolved attempt, require the one-shot itself to have completed
   successfully before accepting any continuity result. Read the two values
   explicitly and also reject systemd's failed state:

   ```bash
   SERVICE_RESULT_AFTER="$(sudo systemctl show arcus-spot-live-tick.service -p Result --value)"
   SERVICE_STATUS_AFTER="$(sudo systemctl show arcus-spot-live-tick.service -p ExecMainStatus --value)"
   if [ "$SERVICE_RESULT_AFTER" != "success" ] || [ "$SERVICE_STATUS_AFTER" != "0" ]; then
     echo "one-tick acceptance did not complete successfully; keep timer disabled" >&2
     exit 1
   fi
   if sudo systemctl is-failed --quiet arcus-spot-live-tick.service; then
     echo "one-tick acceptance unit is failed; keep timer disabled" >&2
     exit 1
   fi
   ```

   A failed candidate that made no state transition is not an accepted
   rollback; do not use continuity's valid zero-advance result to restore its
   timer.

4. When the tick succeeds with no unresolved attempt, prove continuity:

   ```bash
   sudo /usr/local/share/arcus-spot-execute-once/releases/<known-good-verifier-release>/arcus-spot-execute-once state-verify-continuity \
     /etc/arcus-spot/config.yaml \
     /var/lib/debot-arcus/spot-state-backups/<YYYYMMDDTHHMMSSZ>-pre-rollback
   ```

5. Only after the continuity report is saved may the timer be restored. Use
   the two values recorded before the stop; enabled state and active state are
   independent. Apply exactly one row from this table:

   | `TIMER_ENABLED_BEFORE` | `TIMER_ACTIVE_BEFORE` | Restore action |
   |---|---|---|
   | `enabled` | `active` | `sudo systemctl enable arcus-spot-live-tick.timer` then `sudo systemctl start arcus-spot-live-tick.timer` |
   | `enabled` | `inactive` | `sudo systemctl enable arcus-spot-live-tick.timer`; do not start it |
   | `disabled` | `active` | `sudo systemctl disable arcus-spot-live-tick.timer` then `sudo systemctl start arcus-spot-live-tick.timer` |
   | `disabled` | `inactive` | `sudo systemctl disable arcus-spot-live-tick.timer`; do not start it |

   Re-run both queries and require their outputs to match the recorded values
   exactly:

   ```bash
   sudo systemctl is-enabled arcus-spot-live-tick.timer
   sudo systemctl is-active arcus-spot-live-tick.timer
   ```

   If the active state was restored to `active`, observe the next natural timer
   result and Health Watch signal before closing the acceptance window. Never
   enable or start the timer merely because that is the expected production
   default.

## EC2 stop/start variant

Issue #758 also requires the fixed network identity to survive an EC2
stop/start. That is a separate production mutation from a systemd stop/start.
Use the same stopped-timer exact backup before stopping the instance. After SSM
returns, verify the Elastic IP association and instance ID read-only, run
`state-verify-exact` before starting any unit, then follow the one-tick
acceptance above. Preserve `TIMER_ENABLED_BEFORE` and `TIMER_ACTIVE_BEFORE`
outside the instance across the stop/start, and restore both values with the
same table rather than assuming the timer should be enabled and active. This
variant requires its own explicit approval and is not performed by CI or this
PR. Immediately before the EC2 stop, require
`systemctl is-enabled arcus-spot-live-tick.timer` to report `disabled`; an
`enabled` result means the temporary-disable step did not take effect and the
instance must not be restarted.

## Failure and restore policy

- Missing checkpoint, ledger, lock or backup file is a hard failure. Do not
  accept `load_or_create` output as rollback evidence.
- A config-digest mismatch means the backup belongs to a different approved
  runtime. Do not edit the manifest to make it pass.
- A checksum/summary mismatch means the backup or live state changed. Keep the
  timer stopped and diagnose before any invocation.
- This tooling intentionally has no state-restore command. Restoring an old
  ledger can erase evidence of an on-chain attempt or reset a daily cap;
  restoring an old checkpoint can resurrect stale inventory/regime state.
  Any state restore therefore requires a separately reviewed recovery plan,
  explicit approval, current on-chain balance reconciliation and a new backup
  of the damaged state before replacement.
- Binary rollback must be forward-compatible with checkpoint schema 1 and
  ledger schema 2, preserve both checkpointed reference-price fields, and
  write the schema-2 sequence/watermark-bound observation evidence sidecar on
  every sequence-advancing tick, including structurally invalid observations.
  If any part of that capability is uncertain, forward-fix the binary.

Evidence to attach to bot-strategy #758 consists of the selected release
manifests/SHA-256 values, pre-rollback backup manifest, exact verification
before and after binary replacement, service result/journal for the approved
one tick, post-start continuity report, both pre-stop timer values and both
post-restore timer values, and (for the EC2 variant) EIP association
before/after. Never attach config contents, RPC credentials, signing material
or KMS-sensitive output.
