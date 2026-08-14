# Arcus Spot inventory runtime

The Arcus Spot path is intentionally separate from the leveraged BTC/ETH
engine. Base builds consume schema-v3 public recorder snapshots and expose two
non-live modes:

- read_only emits a would-rotate plan and never changes inventory;
- replay_simulation applies the recorded indicative route only to isolated
  in-memory inventory.

The `arcus-spot-live` compile-time feature adds an explicitly gated `live`
mode plus execution safety primitives. It is not enabled by any binary or
service: live mode still emits `would_rotate`, and inventory changes only when
a caller supplies an exactly balance-reconciled confirmed fill. Default and
`arcus-spot-sdk` builds retain no signer or submit surface.

## Safety gates

Before a plan is emitted, the runtime verifies the chain, verified token and
contract identity, exact pair/notional row, both direct routes, matching
reference prices, route age, recommended venue amounts, and recorder schema.
It then enforces:

- a hard floor for each token;
- a per-rotation fraction of inventory above the floor;
- a maximum post-rotation USD inventory imbalance;
- optimistic round-trip loss plus explicit gas and settlement buffers;
- sticky daily and cumulative mark-to-market loss halts;
- maximum hold and mean-reversion exit behavior.

The log-price ratio signal is evaluated against prior samples only. The current
sample is appended after z-score calculation, avoiding same-tick look-ahead.
This is a runtime/replay seam, not evidence that NVDA/AMD is economically
qualified.

## Authenticated execution foundation

With `arcus-spot-live`, the library provides:

- a dedicated asymmetric AWS KMS signer restricted to EIP-712 typed data;
- direct Arcus routing only with `allowWrapped=false`;
- exact-value EIP-2612 authorization when the existing Permit2 allowance is
  insufficient, and refusal of an allowance larger than the exact sell amount;
- independent chain, wallet balance, gas, inventory-floor, token, spender,
  amount, deadline, and signer recovery checks;
- an atomic mode-0600 execution ledger with a pre-POST dispatch marker;
- a library coordinator that connects fresh quote, chain preflight, signing,
  one-shot submit, status polling, and balance reconciliation;
- a feature-gated one-shot CLI that requires a mode-0600 config and a
  mode-0600 fresh plan on every invocation; `execute`/`resume` additionally
  require an Ed25519 signature over the canonical SHA-256 digest of the
  validated config and plan, verified against a public key read from
  `/etc/arcus-spot/approval_public_key`, a fixed path this process must not
  itself own or be able to write (checked via file ownership/mode, not an
  inherited environment variable or a config/plan field either of which the
  same identity running `execute` could set); the matching private key must
  never exist on this host, so the CLI can request approval but cannot mint
  it itself. `auto-execute` (see below) intentionally skips this signature
  requirement -- a narrow, explicit exception, not a change to `execute`/
  `resume` themselves;
- hard coordinator caps of at most 60-second-old plans, 100 bps slippage, ten
  reconciled swaps per UTC day, and deployer-pinned raw sell maxima;
- exactly one submit attempt, sticky `UNKNOWN` on ambiguous delivery, safe
  status GETs, and exact pre/post wallet-balance reconciliation;
- a runtime commit seam that refuses fills inconsistent with the genuine
  strategy plan.

The components intentionally do not form an enabled daemon. The one-shot CLI
is never invoked or scheduled automatically: its hash mode is non-signing, and
its execute and resume modes refuse a changed config or plan because the
supplied Ed25519 signature no longer verifies against the recomputed
canonical digest. No KMS key is created, no wallet is funded, and no message
or transaction is signed by adding this feature. The first real signature
remains blocked on the exact one-swap approval in bot-strategy #772.

Validate the gated foundation without network or wallet access:

    cargo test --lib --no-default-features --features arcus-spot-live arcus_spot
    cargo test --no-default-features --features arcus-spot-live \
      --bin arcus-spot-execute-once

For a future explicitly approved probe: generate an approval keypair once, on
a machine that will never run `execute`/`resume` (the private key file must
never be copied to that host), and have an administrator deploy the printed
public key to `/etc/arcus-spot/approval_public_key` on the `execute`/`resume`
host, owned by a different uid than the one running this binary and not
group- or other-writable -- deliberately *not* a field in `CONFIG_YAML` and
*not* an inherited environment variable: a host that can write the
routinely-deployed config, or that controls its own process environment,
could otherwise generate its own keypair, put its own public half there, and
sign its own "approval" with the matching private key it also holds.

    arcus-spot-execute-once keygen APPROVAL_KEY_FILE

Then, for each execution envelope: compute its digest, sign that digest
*offline* with the private key, and only then supply the resulting signature
to execute:

    arcus-spot-execute-once hash CONFIG_YAML PLAN_JSON
    arcus-spot-execute-once sign-approval DIGEST APPROVAL_KEY_FILE
    arcus-spot-execute-once execute CONFIG_YAML PLAN_JSON APPROVAL_SIGNATURE_HEX
    arcus-spot-execute-once resume CONFIG_YAML PLAN_JSON APPROVAL_SIGNATURE_HEX

## auto-execute: no offline approval signature

    arcus-spot-execute-once auto-execute CONFIG_YAML PLAN_JSON

Runs the exact same path as `execute` -- plan/config validation, runtime
checkpoint consistency, on-chain preflight, exact-value Permit2, KMS
signing, submission, and ledger persistence -- except it does not require
`hash`/`sign-approval`/an Ed25519 signature at all. This is an explicit,
narrow owner decision (bot-strategy#772, 2026-08-12): the offline-signing
round trip exists to validate this execution path against the real Arcus
API before trusting it unattended, and that validation already happened
across the signed one-swap acceptance test attempts. While total
inventory at risk stays small, the per-swap human-signing step is pure
friction with no proportionate safety benefit. Every other gate `execute`
enforces -- `max_plan_age_secs`/`max_quote_age_secs`, inventory floors,
daily/cumulative loss stops, exact-value-only Permit2, slippage -- is
unchanged. `execute`/`resume` themselves, and the approval-key/public-key
trust model described above, are untouched and still available. Revisit
this decision (return to requiring a signed `execute`, or add a
scale-dependent threshold) before any inventory scale-up beyond what is
currently approved on #772.

`auto-execute` refuses a caller-supplied `entry_signal`-triggered PLAN_JSON
outright. Every check above authenticates *the execution* (fresh-quote
matching, inventory floors, staleness, slippage) but none of them re-derive
whether `entry_z_score` was genuinely crossed, or re-check the round-trip-
cost, rotation-fraction, or inventory-imbalance gates `step_at` itself
enforces when it proposes a plan -- `execute`'s offline signature used to
be what vouched for that underlying strategy decision, and `auto-execute`
has nothing in its place. Only `execute` (signed) or `live-tick` (which
builds its own entry plan from `step_at` under the checkpoint lock,
immediately before dispatch, so provenance is inherent rather than merely
asserted) may dispatch an entry. A `mean_reversion_exit`/`max_hold_exit`
plan is still accepted through `auto-execute` -- it is risk-reducing and
already bounded by the runtime checkpoint's own genuinely-open rotated
quantity.

In place of the signature, `auto-execute`/`auto-resume`/`live-tick` require
CONFIG_YAML to match an administrator-approved digest recorded in a fixed,
administrator-owned file at `/etc/arcus-spot/auto_execute_policy.json`
(same ownership/mode trust model as `approval_public_key` above: not owned
by the uid running this binary, not group- or other-writable, not a
symlink). Its schema is a single field:

    {"approved_config_sha256": "sha256:<hex>"}

An earlier version of this policy compared `ledger_path`, `runtime_state_path`,
and `maximum_sell_amount_raw` individually. That left every other field --
`inventory_floor_raw`, `max_swaps_per_utc_day`, router/chain/token identities,
gas/slippage buffers, and any field added later -- fully executor-controlled;
a lowered `inventory_floor_raw`, for example, could let an unsigned plan
violate the real floor, discoverable only after the on-chain swap. Binding
the whole config by digest closes that class of gap by construction: these
three commands only ever run against the byte-for-byte exact configuration
an administrator approved.

To provision or rotate `approved_config_sha256`, run on the execute host
(or against an identical copy of CONFIG_YAML), then have an administrator
write the printed digest into the policy file:

    arcus-spot-execute-once hash-config CONFIG_YAML

Any change to CONFIG_YAML -- intentional or not -- requires recomputing and
redeploying this digest, or `auto-execute`/`auto-resume`/`live-tick` refuse
to run against it.

## auto-resume: signatureless recovery

    arcus-spot-execute-once auto-resume CONFIG_YAML PLAN_JSON

The signatureless counterpart to `resume`, gated by the same
`auto_execute_policy.json` digest as `auto-execute`. Needed because an
`auto-execute` or `live-tick` attempt that comes back `Submitted` (not yet
confirmed), or that crashes before runtime commit, otherwise has no recovery
path that doesn't require the offline signature this command family exists
to skip. For a `live-tick`-originated attempt, PLAN_JSON is the file
`live-tick` itself wrote before dispatching -- see below.

## live-tick: the unattended-probe entry point

    arcus-spot-execute-once live-tick CONFIG_YAML RECORDER_SNAPSHOT_JSON

Evaluates the strategy signal (`ArcusSpotRuntime::step_at`) against one
fresh recorder snapshot at the current wall-clock time -- the "future
read-only daemon [that] must call step_at with the current UTC time" flagged
above as not yet built. Meant to be invoked on a timer shortly after each
recorder snapshot lands. Every tick persists the resulting runtime
checkpoint (under the same exclusive lock `execute`/`auto-execute` hold
around their own dispatch, so a racing invocation can't clobber a
just-reconciled fill with stale state); only when the tick genuinely decides
`WouldRotate` does it go on to build and dispatch a plan, through the same
policy-gated path as `auto-execute`. Most ticks decide `Observe` and never
touch the KMS signer or the network beyond the on-disk snapshot.

Before dispatching, `live-tick` durably writes the plan it built, at mode
0600, to `<runtime_state_path's directory>/live-tick-pending-plan.json`.
If the process exits after the swap is `Submitted` but before it is
confirmed, recover with:

    arcus-spot-execute-once auto-resume CONFIG_YAML \
      <runtime_state_path's directory>/live-tick-pending-plan.json

Config validation rejects `ledger_path`/`runtime_state_path` values that
would resolve to this same derived path -- otherwise `live-tick`'s
atomic-replace write would destroy the checkpoint or ledger file it was
supposed to be sitting alongside.

### Known limitation: the executor identity can reset its own state

`live-tick`/`auto-execute`/`auto-resume` run as the same OS identity
(`arcus`) that owns `ledger_path` and `runtime_state_path`, because that
identity must be able to write those files to persist state at all --
that write access is not narrower than delete access. The
`auto_execute_policy.json` digest pins *which config* this identity may run
(closing the path-redirection and field-tampering gaps above), but it
cannot stop the identity from deleting the ledger or checkpoint file
directly: `load_or_create()` treats a missing file as first-run and
silently recreates empty/default state, which would reset the accumulated
daily swap count and runtime history (inventory, regime, frozen beta) an
attacker or a bug in this identity's own tooling could otherwise not touch
through CONFIG_YAML.

A genuine fix requires privilege separation -- a distinct, more-privileged
process or service owning the actual writes, with the executor only able to
request them -- which is disproportionate engineering effort at the current
inventory scale. This is an explicit, accepted owner decision (bot-strategy
#772, PR #186, 2026-08-14), mirroring the earlier decision to skip the
offline approval signature: while total inventory at risk stays small, this
residual risk is accepted without further mitigation. Revisit alongside the
`auto-execute` signature-skip decision before any inventory scale-up beyond
what is currently approved on #772.

## Deterministic replay

Build with the pinned Arcus connector feature and replay the recorder archive:

    cargo run --no-default-features --features arcus-spot-sdk \
      --bin arcus-spot-runtime -- \
      configs/pairtrade/arcus-spot-runtime.example.yaml \
      /path/to/samples.jsonl \
      /tmp/arcus-events.jsonl

The optional third argument is the event JSONL path. Without it, events go to
stdout. A compact final state summary goes to stderr. Replay evaluates quote
freshness at each snapshot event timestamp, never at the current wall clock.
A future read-only daemon must call step_at with the current UTC time.

The example config remains read-only and is not wired to systemd or deployment.
Provisioning custody, enabling a service, or permitting a signed swap remains
blocked on bot-strategy #772 and fresh approval of the exact wallet, pair,
amount, gas budget, floors, and maximum accepted loss.
