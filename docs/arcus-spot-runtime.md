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
- sticky daily and cumulative loss halts, measured against the strategy
  rather than the market (see below);
- maximum hold and mean-reversion exit behavior.

### What the loss stops measure

`daily_loss_limit_usd` and `cumulative_loss_limit_usd` ask "how much has
*rotating* cost", not "how much is the inventory worth".

Each stop records the basket held when its baseline was taken -- the day's
opening inventory, and the inventory at probe start -- and re-prices that
basket at every tick. The gap between that buy-and-hold counterfactual and
actual equity is what rotating added or destroyed. While the bot has not
traded, the two are the same basket at the same prices, so the measured loss
is exactly zero no matter what prices did.

These stops were originally marked against fixed dollar baselines, which
conflated the two questions. This bot pre-funds both legs and has no native
short on Spot, so it carries their beta whether or not it ever trades: on
2026-08-18 a 4.1% NVDA/AMD down day moved probe equity $100.58 -> $96.46 and
engaged the $2 daily halt without a single swap having been made
(bot-strategy#813). Halting on that was never protective — stopping rotation
does not shed inventory, so the exposure is identical halted or not, and
shedding it is an operator decision.

The beta itself is still reported, as `inventory_drawdown_usd` on every risk
mark; it is simply never compared against a limit.

Two consequences worth knowing:

- **Path independence.** Prices move the benchmark and actual equity
  together, so only a rotation can move the difference. An earlier guard
  compared the rollover tick against the previous day's closing mark, so
  that an intraday gain could not mask an overnight decline; that failure
  mode is structural to equity marks and does not exist here, so the guard
  is gone.
- **No drawdown control.** `daily_loss_limit_usd` is a loss limit, so a day
  that gives back part of a gain but ends net positive does not halt. If a
  peak-to-trough drawdown stop is wanted, it is a separate limit and is not
  implemented.

While a halt stands, the daily basket stops rolling at UTC midnight. It is
the evidence of what is still owed, not a per-day convenience, so rebasing
it onto the still-impaired inventory would report the loss as settled
without anything having been remediated. The day and its equity mark roll
as usual -- the rollover and continuity checks read those -- and the basket
unfreezes on the first rollover after the halt is lifted. Lifting a halt
does not rebase it either: a halt lifted at a partially-remediated loss
should re-engage promptly if the rest of the day's budget goes too.

A checkpoint written before the baskets existed carries none; the next tick
seeds them and the stops are unmeasurable (reported as zero) until it does.
Seeding happens even while halted, or such a checkpoint could never become
measurable again.

`state-verify-continuity` re-derives the same measure independently, so the
runtime and the verifier agree about when a halt was required.

### Lifting an engaged halt

    arcus-spot-execute-once clear-risk-halt CONFIG_YAML

A halt is sticky: nothing in the runtime ever lifts one, because no later
tick is evidence that whatever caused it was dealt with. Until
bot-strategy#813 there was no sanctioned way to lift one at all, which left
a halted bot permanently stopped short of hand-editing checkpoint state.

This command is that way, and only that: it takes the same
administrator-approved config digest `auto-execute`/`live-tick` require
(resuming re-enables exactly the dispatch path that gate governs), takes the
same exclusive checkpoint lock a dispatching tick takes, and prints what it
cleared alongside the marks at the moment of clearing, so the disarming of a
risk control leaves a record in the journal.

It **refuses while the halt's own condition still holds** -- a check that
lives on `ArcusSpotRuntime::clear_risk_halt` itself, not in this command, so
no other caller can reach past it. Clearing while it holds would be worse
than useless — the next tick re-engages immediately, and an operator
watching only the exit status would read an ongoing breach as handled. Repeating the command would walk straight past the limit. A halt is
liftable only once nothing is actually owed, which is exactly the case a
beta-driven halt from before the measurement change falls into.

It deliberately does *not* require the offline Ed25519 approval signature:
requiring more to resume dispatching than to dispatch would be theatre while
`auto-execute` itself runs signatureless.

Take a fresh `state-backup` afterwards — backups from before the clear no
longer verify, since continuity checks treat a lost halt as a real state
change.

### Routes this executor may not take

Only a direct Arcus route is dispatchable (`allowWrapped=false`, per the
approved envelope on bot-strategy#772). The router, though, recommends
whichever venue prices best, and that is frequently not Arcus: over one
sample of the recorder archive on 2026-08-19, Rialto won about two thirds of
the routes and Arcus about one third, with Arcus quoting fine and simply
being outbid (by ~0.37% on the tick inspected).

So a would-rotate plan the executor must refuse is an ordinary market
outcome, not a fault. `live-tick` checks `is_direct_arcus_route` before
building or writing anything, logs one `[arcus-route] ...` line naming the
venue, and exits successfully; `validate_plan` still enforces the same
predicate independently, for the caller-supplied plans `execute`/
`auto-execute` take.

Each decline appends one line to `declined-routes.jsonl`, next to the
runtime checkpoint: when, which way, how strong the signal was, at what size
and marks. Counting declines does not say what they were worth — if the
declined signals were the weak ones the surviving third flatters the
strategy, and if they were the strong ones it understates it — so the record
carries enough to price the counterfactual offline against the recorder
archive, rather than putting a shadow position tracker inside a signing bot
(bot-strategy#818, owner chose to keep the constraint and measure its cost).
A failed write is reported and ignored: this file is analysis, not safety.

This used to fail the unit instead. On 2026-08-19 twelve consecutive ticks
exited non-zero while the bot was behaving exactly as designed
(bot-strategy#817), which is the same signal a real fault would have had to
stand out from. The economics of the constraint -- roughly two thirds of
entry opportunities declined -- are tracked separately.

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

## Changing `runtime:` under a live checkpoint

Re-approving the digest above is necessary but not sufficient. The runtime
checkpoint at `runtime_state_path` stores the `runtime:` config it was
written under alongside the state itself, and every load compares the two.
This is a **state-coherence** check, not a second authorization check --
authorization already happened, at the signature (`execute`/`resume`) or the
policy digest (`auto-execute`/`auto-resume`/`live-tick`), and the runtime
always executes against that authenticated config rather than the
checkpoint's stored copy. What is left to decide here is only whether the
accumulated state still describes the new config. (The checkpoint never was
a barrier against the executor identity in the first place -- that identity
can delete it outright, an accepted limitation recorded under "the executor
identity can reset its own state" below.)

So the comparison is by field, not byte-for-byte (bot-strategy#809):

- **State-invalidating** -- `mode`, `chain_id`, `pair`, `initial_inventory`,
  `signal_window_samples`. Changing any of these makes the stored signal
  window, regime, inventory, or risk baselines describe something other than
  what they now claim to, so the load fails and names the field. Clear it
  deliberately: stop the timer, take a `state-backup` (see
  `docs/arcus-spot-state-rollback.md`), remove the checkpoint file, and let
  the next tick start a fresh window. Expect to re-serve the full
  `min_signal_samples` warmup before entries resume.
- **State-preserving** -- every other field, including `notional_usd`,
  `inventory_floors`, `max_rotation_fraction`, `min_signal_samples`,
  `entry_z_score`/`exit_z_score`, the age/hold limits, the cost buffers, and
  the loss limits. These re-aim future decisions without changing what any
  stored value means, so the state carries over untouched and the next
  `persist` writes the new config through. The load prints one
  `[arcus-checkpoint] ...` line to stderr (so it lands in the journal)
  naming the changed fields and the state it kept -- an adopted change to a
  live, KMS-signing bot is never silent.

Before this split, retuning one forward-looking cap cost the entire
accumulated window; on the live probe that meant days of warmup to move
`max_rotation_fraction`, which is what #809 was filed about.

Raising `inventory_floors` above the currently tracked inventory is still
refused, by `ArcusSpotRuntime::from_state`'s own floor check rather than
here, with a message that names that as the problem.

One consequence to plan for: `state-verify-exact`/`state-verify-continuity`
compare a backup's whole-config digest against the config supplied to them,
so **state backups taken before a retune no longer verify against the
retuned config**. Take a fresh `state-backup` once the change is live, and
keep the pre-change backup only as a rollback target for the pre-change
config.

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

    arcus-spot-execute-once live-tick CONFIG_YAML

Fetches exactly one live snapshot itself -- the same public, read-only
recorder client `arcus-spot-propose-plan` and the archival collector use --
and evaluates the strategy signal (`ArcusSpotRuntime::step_at`) against it
at the current wall-clock time. This is the "future read-only daemon [that]
must call step_at with the current UTC time" flagged above as not yet
built. Meant to be invoked on a timer. Every tick persists the resulting
runtime checkpoint (under the same exclusive lock `execute`/`auto-execute`
hold around their own dispatch, so a racing invocation can't clobber a
just-reconciled fill with stale state); only when the tick genuinely decides
`WouldRotate` does it go on to build and dispatch a plan, through the same
policy-gated path as `auto-execute`. Most ticks decide `Observe` and never
touch the KMS signer or the submission network.

An earlier version took `RECORDER_SNAPSHOT_JSON` as a second argument.
`read_private_regular_file` only checks a file's mode and type, not its
origin, so the executor identity could fabricate an internally-consistent
snapshot (prices, route records) that drives `step_at` to `EntrySignal`
even though the real market never crossed the threshold, dispatched
through this exact signatureless path (Codex P1 follow-up, pairtrade#186).
Fetching the snapshot live, the same way `propose-plan` does, means its
provenance is inherent rather than merely asserted.

Every call to `step_at` mutates the signal window and sequence
unconditionally, whether or not it decides to rotate, so re-evaluating the
exact same (or an out-of-order) observation twice would artificially
reweight the z-score history. `step_at` itself tracks
`collection_finished_at` of the last snapshot it genuinely advanced on, in
`ArcusSpotRuntimeState::last_observation_at` -- part of the checkpointed
state, not any individual caller's own bookkeeping -- and rejects a
snapshot whose `collection_finished_at` is not strictly newer as a no-op
`Observe { hold: StaleOrDuplicateObservation }` tick. Tracking this inside
the shared step, under whichever lock the caller already holds around
load/step_at/persist, is what correctly orders concurrent writers of the
same checkpoint against each other: `live-tick` and `arcus-spot-propose-plan`
both call `step_at`, and a per-binary sidecar could not see a fresher
snapshot the other one had just persisted.

The checkpoint lock is dropped between `step_at`/persist and the dispatch
below (re-acquired fresh inside `executor_from_config`, since a second
acquisition by the same process from a separate open file description
would conflict rather than nest). A concurrent `live-tick` or
`arcus-spot-propose-plan` can advance the checkpoint to a newer observation
in that window; `validate_plan_consistent_with_state` alone only checks
regime/trigger/direction/open-quantity structural consistency, not that
the plan corresponds to the checkpoint's *current* observation. Before
`validate_plan_consistent_with_state`, `live-tick` therefore also compares
the freshly re-read checkpoint's `last_observation_at` against the value
captured when this plan was computed, and refuses to dispatch if the
checkpoint has moved on -- otherwise an entry could be submitted based on
a signal state a newer tick has already superseded (Codex P1 follow-up,
pairtrade#186).

For every sequence-advancing observation, including structurally invalid
`Observe` ticks that cannot advance `last_observation_at`, both `live-tick` and
`arcus-spot-propose-plan propose` durably write a schema-2 evidence sidecar at
mode 0600 to
`<runtime_state_path's directory>/live-tick-observation-evidence.json`. It
contains the exact recorder snapshot, `step_at` evaluation time, and resulting
runtime sequence/watermark, so the post-rollback continuity verifier can
independently replay no-swap signal, invalid-snapshot, reference-price, equity
and risk state from the pre-start checkpoint. Schema-1 sidecars remain readable
as an unchanged backup baseline during a rolling upgrade, but continuity
requires schema 2 for any current sequence advance. Evidence is atomically
published before its checkpoint; if checkpoint publication fails, state tooling
recognizes only
an exactly one-sequence-newer schema-2 sidecar as an orphan and omits it from
the captured boundary. All other evidence/checkpoint mismatches remain errors.

Before dispatching a rotation, `live-tick` also writes a schema-1 recovery
envelope, at mode 0600, to
`<runtime_state_path's directory>/live-tick-pending-plan.json`. The envelope
contains the plan, the exact recorder snapshot that produced it, and the
`step_at` evaluation time. `auto-resume` accepts this envelope and extracts
the plan; standalone operator-supplied plan JSON remains supported by the
other execution/recovery commands. The preserved snapshot also lets the
post-rollback continuity verifier recompute route linkage/loss and the full
plan from the pre-start checkpoint instead of trusting strategy fields
reported by the candidate binary. Config validation prevents either derived
sidecar path from aliasing the checkpoint or ledger.

If the process exits after the swap is `Submitted` but before it is
confirmed, recover with:

    arcus-spot-execute-once auto-resume CONFIG_YAML \
      <runtime_state_path's directory>/live-tick-pending-plan.json

Config validation rejects `ledger_path`/`runtime_state_path` values that
would resolve to this derived path -- otherwise `live-tick`'s
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

## Traceable executor deployment

The operator procedure for checkpoint/ledger backup, byte-exact binary
rollback verification, and separately approved post-start continuity evidence
is documented in [Arcus Spot live-tick state preservation and binary
rollback](arcus-spot-state-rollback.md). The state tooling is offline and has
no restore operation; starting the live-tick service remains an explicitly
approved action because one tick can submit a swap.

`.github/workflows/deploy-arcus-spot-executor.yml` is the aarch64 build and
install path for `arcus-spot-execute-once`. It runs automatically when Arcus
executor/runtime source, Cargo inputs, the build script, or the workflow itself
changes on `master`, and it also retains an explicit `workflow_dispatch` entry
point. Unrelated pairtrade-only changes do not trigger the arm64 build. The same
binary contains the `live-tick` subcommand, so a deploy covers both the approved
one-shot flow and the timer-invoked probe flow without deploying a second
executable.

The workflow is accepted only from `master`/`main`. It checks out the pinned
`DEX_CONNECTOR_REF` (or the explicitly supplied manual-dispatch override), uses
`Cargo.lock`, runs the Arcus live library and binary tests inside an arm64
Amazon Linux 2023 container, and records the exact pairtrade commit,
dex-connector commit, Rust toolchain, resolved container image, lockfile hash,
and binary SHA-256 in a manifest. The binary, checksum, and manifest are stored
under the existing least-privilege S3 prefix at an immutable
content-addressed key:

    arcus-spot-execute-once/releases/<pairtrade-sha>/<dex-sha>/<binary-sha256>/runs/<run-id>-<attempt>/

The SSM install downloads that exact key, verifies both its uploaded checksum
and the workflow's expected SHA-256, and stages a complete root-owned release
under `/usr/local/share/arcus-spot-execute-once/releases/`. The run ID and
attempt make each manifest key immutable even when identical sources reproduce
the same binary. Only after the staged executable and provenance pass every
check does the workflow atomically replace
`/usr/local/bin/arcus-spot-execute-once`; that rename is the final install
operation, so any earlier failure leaves the timer-visible executable
unchanged. The `arcus` service identity remains the unprivileged caller and
owns only its writable state directories. The workflow never starts or
restarts a service/timer and never invokes the installed binary. Scheduling
and manual one-shot approval therefore remain separate operator actions.

The unattended probe's current unit definitions are source-controlled as
`deploy/arcus-spot-live-tick.service` and
`deploy/arcus-spot-live-tick.timer`. The separate
`deploy-arcus-spot-live-tick.yml` workflow validates and installs those
definitions from an immutable, checksummed release. It may reload systemd only
when their contents change, but it preserves and verifies the timer's observed
active/enabled state; it never enables, disables, starts, stops, or restarts
the timer/service and never invokes the executor. A new host therefore remains
inactive until an operator makes a separate, explicit activation decision.

`arcus-spot-runtime` is deliberately excluded. It remains the deterministic
archive replay CLI below; if it later gains a distinct live-daemon role, add a
separate artifact and lifecycle only when that runtime contract exists.

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
