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
- maximum hold and mean-reversion exit behavior;
- declared corporate-action and token-lifecycle windows (see below).

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

### Supported and declined routes

Direct-token Arcus and Rialto routes are dispatchable through the official
Arcus hosted router (`allowWrapped=false`). LI.FI and every unknown venue stay
fail-closed. The hosted router recommends whichever venue returns the best
eligible quote, so Rialto being selected is an execution-price decision, not
an Arcus points or airdrop decision. No rewards program is assumed by this
runtime or by the acceptance criteria in bot-strategy#818.

Rialto uses a different signed envelope from direct Arcus: the EIP-712 Permit2
witness and its prepared transaction are both validated, the signature is
spliced only into the quoted placeholder, and the target is pinned to the
canonical RialtoRouter. Submission and status polling still use Arcus's
authenticated `/v1/submit` and `/v1/status` endpoints.

A hosted `confirmed` status is not sufficient to reconcile either venue.
The transaction receipt must be successful and target the canonical
SwapShell (`0x4262efBd176F02824af27010bEa218429c33c7E8`), with exactly one matching
`SwapExecuted` event. The event must bind the original taker, tokens, signed
input and minimum output, and must identify the canonical route:

- Arcus: router `0x006102b16A04c20306A28b652745D3973D7D24fa`, tag `ARCUS`;
- Rialto: router `0xC94135b63772b91D79d0A2DaAb2a8801f32359bD`, tag `RIALTO`.

Only after that event passes are the existing EIP-1898 canonical-block wallet
reads and exact balance reconciliation allowed to complete. Configuration
must pin exactly those Arcus and Rialto Permit2 spenders; missing or extra
addresses fail startup.

Both legs of that reconciliation are anchored to the settlement transaction's
own logs, not to the signed plan: the buy delta must equal the
`SwapExecuted` event's `amount_out` (bot-strategy#883), and the sell delta
must equal the net sell-token outflow its ERC-20 `Transfer` logs report --
everything that left the taker minus anything the same transaction refunded
back (bot-strategy#979). The signed amount remains the ceiling on the sell
side (Permit2 can authorise no more), but a route may take less of it and
refund the remainder in the same transaction, which a pre/post balance delta
nets out on its own. The runtime is then credited with what the wallet
actually moved, on both legs.

When LI.FI or an unknown venue wins, `live-tick` logs one `[arcus-route] ...`
line and exits successfully. Each decline also appends one analysis-only line
to `declined-routes.jsonl`, next to the runtime checkpoint. A failed write is
reported and ignored because this file is observability, not a safety gate.

The log-price ratio signal is evaluated against prior samples only. The current
sample is appended after z-score calculation, avoiding same-tick look-ahead.
This is a runtime/replay seam, not evidence that NVDA/AMD is economically
qualified.

## Authenticated execution foundation

With `arcus-spot-live`, the library provides:

- a dedicated asymmetric AWS KMS signer restricted to EIP-712 typed data;
- direct Arcus and Rialto routing only with `allowWrapped=false`; LI.FI and
  unknown venues are refused;
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
- hard coordinator caps of at most 60-second-old plans, 100 bps slippage,
  twenty reconciled swaps per UTC day (ten round trips -- raised from ten in
  bot-strategy#823, since the cap rather than the signal was deciding how
  much the bot traded), and deployer-pinned raw sell maxima;
- exactly one submit attempt, sticky `UNKNOWN` on ambiguous delivery, safe
  venue-specific status GETs, canonical SwapShell event verification, and
  exact pre/post wallet-balance reconciliation against the settlement
  transaction's own logs;
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
asserted) may dispatch an entry. A
`mean_reversion_exit`/`max_hold_exit`/`corporate_action_exit`
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

`hash-config` also prints the config's cost budget to stderr (stdout stays
exactly the digest, so it can still be piped):

    [arcus-config] cost budget: max_all_in_round_trip_cost_bps 60 bps is
    compared against quoted round-trip loss + gas_buffer_bps 10 +
    settlement_buffer_bps 10, so a quote clears the gate only at or below
    40 bps

`max_all_in_round_trip_cost_bps` is the **all-in** limit: the quoted
round-trip loss plus both fixed buffers is charged against it, not the
quoted loss alone. Sizing it as if it capped the quoted loss is how a
config ends up holding every tick on `cost_limit` for no visible reason
(bot-strategy#903). A config whose buffers alone already exceed the cap is
rejected outright at load, naming both figures. The comparison is
inclusive -- `build_plan` rejects on `all_in_cost > cap` -- so a quote
landing exactly on the residual budget passes. That matters most when the
buffers equal the cap: the residual is then 0 bps, and a zero-loss quote
still clears.

## Corporate actions and token lifecycle (bot-strategy#853)

A split, merger, symbol change or redemption in an underlying equity
invalidates the relative-price history and can rewrite the wallet balance
with no swap. The recorder payload carries token identity, `addedTimestamp`,
prices and financial fields, but **no corporate-action calendar, effective
time, halt flag or settlement status**, so the runtime cannot infer any of
this. A price-jump threshold is not a substitute either: it cannot separate a
split from a legitimate gap, and it sees nothing at all for the lifecycle
events that leave price untouched.

So the calendar is operator-supplied, under `runtime.corporate_actions`, and
the runtime only ever applies it. Empty (the default) is exactly the previous
behaviour.

```yaml
runtime:
  # ... every existing field ...
  corporate_actions:
    - event_id: SPY-2026-11-SPLIT        # stable; recorded once handled
      symbols: [SPY]                     # must be in the configured pair
      entry_block_at:   2026-11-20T00:00:00Z
      reduce_exit_at:   2026-11-20T20:00:00Z
      effective_at:     2026-11-21T13:30:00Z
      resume_not_before: 2026-11-24T14:30:00Z
      source: "issuer notice 2026-11-05, <url>"   # required
      # Added by the operator *after* effective_at, from the wallet:
      # post_event_inventory: {token_a: "7.748102027952155828", token_b: "2.087371762778876145"}
```

### The four phases

| From | Entries | Open rotation | Signal window |
|---|---|---|---|
| `entry_block_at` | blocked (`corporate_action_block`) | held; mean-reversion and max-hold exits still fire | still accumulating |
| `reduce_exit_at` | blocked | **forced unwind** (`corporate_action_exit`), stopping `corporate_action_settlement_margin_secs` before `effective_at` | still accumulating |
| `effective_at` | blocked | **no exit of any kind** -- holds `corporate_action_unresolved`; operator reconciles | discarded **once**; nothing accumulates |
| `resume_not_before` | blocked until the resume completes | must be flat | empty; warm-up gates the first new entry |

Before `effective_at`, exits are never blocked by a window: a guard that could
trap an open position through the event it exists to protect against would be
worse than no guard. The forced exit in the reduce phase clears **every**
ordinary gate -- quote freshness, venue, cost, token floor, gas, signing,
reconciliation. If it cannot be quoted it holds (`route_unavailable`, etc.)
and the position stays open; it never bypasses a gate and never pretends to
have closed.

It also stops early: exit dispatch is refused within
`corporate_action_settlement_margin_secs` (300s by default) of
`effective_at`, because submission is not execution -- an order sent just
before the cutoff can still be mined after it, selling the pre-event
quantity at the post-event denomination, and no in-process check covers the
venue round trip. Size `reduce_exit_at` to leave room for that margin as
well as for the venue not quoting -- `hash-config` refuses a window whose
reduce phase begins inside the margin, since its forced unwind could never
be submitted.

**From `effective_at` the opposite holds: the runtime submits no exit at
all** -- not the forced one, not max-hold, not mean-reversion. The venue is
quoting the post-event instrument while `rotated_quantity` is still in
pre-event units, so any exit sized from it is wrong in a direction that
depends on the event: after a 4-for-1 split it sells one old unit as one new
unit and marks the rotation flat with three split-adjusted units still held;
after a reverse split it submits an unfillable oversized order every tick. A
rotation still open at `effective_at` therefore holds
`corporate_action_unresolved` and stays there. **Do not wait for an exit that
will not come**; see "Reconciling an open rotation" below.

### Completing the resume

At `resume_not_before` the runtime resumes only when all three hold:

1. **Flat.** A rotation still open at `effective_at` is not unwound by the
   runtime (see above); it holds `corporate_action_unresolved` until the
   operator has reconciled both the position and the runtime state.
   `post_event_inventory` describes a wallet, and adopting it over an open
   position would overwrite the holding `rotated_quantity` refers to.

   **Reconciling an open rotation.** Close it at the venue yourself, then
   tell the bot with `reconcile-position` (bot-strategy#977):

       arcus-spot-execute-once reconcile-position CONFIG_YAML \
           SETTLED_SELL_AMOUNT_RAW SETTLED_BUY_AMOUNT_RAW \
           OBSERVED_SELL_BALANCE_RAW OBSERVED_BUY_BALANCE_RAW \
           OBSERVED_GAS_BALANCE_WEI TX_HASH_OR_none DETAIL

   One transition, under the same administrator policy digest and exclusive
   lock as `clear-risk-halt`/`reset-window`: it writes an execution-ledger
   entry for the close (phase `ManuallyClosed`, never `Reconciled` --
   nothing about a manual close can be reproduced from recorder evidence,
   and every check that consumes a reconciled attempt is entitled to assume
   it can), and a checkpoint that is flat with the wallet's post-close
   holdings. The balances you pass are read the same way the resume's are
   (`eth_call balanceOf` against `chain.rpc_urls[0]`), **after** the close,
   in whatever units the venue quotes now -- nothing recomputes them from
   the pre-event quantities, which is the whole reason the rotation could
   not be exited normally.

   The corporate-action progress is left in place: the window is not over,
   and the ordinary resume (flat, identity intact, `post_event_inventory`
   declared) is what ends it on a later tick. So this is step 1 of
   "Completing the resume", not a replacement for it.

   Because that resume adopts `post_event_inventory` **unconditionally**,
   the command refuses when the declaration already carries one that
   disagrees with the balances you pass: leaving the window pending with
   the two out of step would hand the runtime holdings the wallet does not
   have. Either declare the observed holdings first, or leave
   `post_event_inventory` unset and fill it in afterwards.

   It refuses unless a corporate-action window is open -- it is not a
   general position editor -- and, like `reset-window`, unless the bot is
   idle: no pending durable event, no active ledger attempt, no on-disk
   pending plan, and a checkpoint in step with the event-stream tail. It is
   also refused when the checkpoint is already flat.

   The amounts you pass are in the units the venue quotes **now**, which
   for a split is not what the runtime tracked: across a 4-for-1, a tracked
   `rotated_quantity` of 2 is 8 units sold. The report says both, labelled
   -- `settled_sell_quantity`/`settled_buy_quantity` for what actually
   changed hands, `tracked_pre_event_quantity` for the stale figure the
   runtime was carrying. A leg that ends at exactly zero is accepted
   (subject to its inventory floor).

   **Re-running it is safe.** The ledger commits before the checkpoint, so
   an interruption in between leaves the close recorded against a
   checkpoint that still shows the rotation. Running the command again with
   the same amounts finishes that transition -- it recognises its own
   half-committed close and writes only the checkpoint, rather than
   appending a second one and advancing the sequence again. The report says
   `resumed_a_recorded_close: true` when that happened.

   **Take a fresh `state-backup` afterwards.** Like `reset-window`, this
   writes over the record that earlier backups verify against, so those no
   longer verify. Continuity verification compares a neutral, no-active
   baseline against one later tick; the new backup is that baseline.

   The mitigation is still upstream of all this: set
   `reduce_exit_at` with **real margin** before `effective_at` for the venue
   not quoting (the forced exit retries every tick through that phase, and
   stops `corporate_action_settlement_margin_secs` -- 300s by default --
   before the cutoff, because a submitted swap can still be mined after it),
   and
   do not open a window with a rotation you cannot afford to have stuck. If
   it happens anyway, close the position on the venue by hand, in post-event
   units, keep the evidence -- and run `reconcile-position` with it, as
   described above. Never edit the state files directly.
2. **Unchanged token identity.** Each affected symbol's contract address and
   decimals are compared against what they were on the last observation
   *before* the window opened. A mismatch holds on
   `corporate_action_unresolved` -- a relisted ticker is a different
   instrument and needs a new `pair`, i.e. a config change and
   `reset-window`, not a resume. If the runtime never observed the pre-event
   side (it was down, or the window was declared after the fact) there is
   nothing to compare and it says so rather than inventing a comparison.
   **With no pinned identity an open rotation is not unwound either**: the
   ticker may already have been repointed, and an exit sized from
   `rotated_quantity` would route the old instrument's quantity to whatever
   the symbol now resolves to. Declaring a window a runtime never observed
   the other side of therefore hands an open position to the operator
   (bot-strategy#977) -- declare windows before `entry_block_at`. Only
   an observation taken **strictly before** `entry_block_at` counts: a
   calendar installed mid-window on a runtime that kept ticking holds an
   identity from inside the event, and pinning that would compare the new
   contract against itself.
3. **`post_event_inventory` supplied, at or above `inventory_floors`.**
   Otherwise it holds on `corporate_action_resume_pending` -- for a missing
   holding, and equally for one below a floor. The floor comparison is made
   **at the resume, not at install**: `hash-config` and deploy accept a
   reconciliation under a floor (a completed event's historical holding must
   not veto a later floor increase), and the runtime then declines to persist
   it, naming both the holding and the floors in the hold. A reverse split or
   partial redemption that shrinks a leg below its floor therefore needs the
   floor lowered -- or the quantity corrected -- in a follow-up install; the
   resume completes on the next tick after it. Watch the hold code after
   installing a reconciliation rather than assuming the install validated it.

On resume the reconciled holding replaces the tracked inventory **and both
risk baskets are re-anchored to it**. They are buy-and-hold counterfactuals
(see "What the loss stops measure"), so leaving them on a basket that no
longer exists would report the corporate action itself as a rotation loss and
engage the sticky stop on it -- bot-strategy#813's failure mode reached by a
different road.

Entries are then gated by the ordinary warm-up: the window was emptied at
`effective_at`, so `min_signal_samples` fresh post-resume observations must
accumulate first. There is no second counter that could disagree with it.

### Operator runbook

1. **Learn of the event** from an authoritative source -- the issuer's
   notice, the exchange calendar, or the token issuer's announcement. Never
   from a price move. Record the URL in `source`; the config is refused
   without one.
2. **Add the window** to `runtime.corporate_actions`, leaving
   `post_event_inventory` unset. Windows must be ordered by `entry_block_at`
   and must not overlap; two simultaneous events are declared as one window
   naming both symbols.
3. **Install it** the ordinary way (`hash-config` -> policy digest -> deploy
   config -> restart). `corporate_actions` is **state-preserving**: no window
   reset, the signal history and regime survive, and the runtime keeps
   marking and risk-accounting throughout. `hash-config` echoes every
   declared window to stderr -- check the list matches what you intended.
4. **Watch the window open.** The hold code becomes `corporate_action_block`.
   If a rotation is open, confirm it unwinds at `reduce_exit_at`
   (`corporate_action_exit` in the ledger); if the venue cannot quote it,
   that is the ordinary hold code and the position stays open. **The reduce
   phase is the only time the runtime will exit** -- if it is still open
   when `effective_at` arrives, the hold becomes
   `corporate_action_unresolved` and the position is yours to reconcile
   (step 1 under "Completing the resume"). Size `reduce_exit_at` with a
   realistic margin for the venue not quoting.
5. **After `effective_at`, read the wallet.** Use the same `eth_call
   balanceOf` path the cut-over used (`chain.rpc_urls[0]`), for both tokens,
   and put the raw human quantities in `post_event_inventory`.
6. **Install again.** The next tick at or after `resume_not_before` adopts
   the holding, re-anchors the baselines, records the `event_id` as handled,
   and returns to warm-up. The event can stay in the config forever; it is
   never applied twice.

### state-verify-continuity across a window

Both no-swap transitions a window makes -- the discard at `effective_at` and
the resume -- are recognised by `state-verify-continuity`, so a backup taken
on either side of one still verifies. They are not exemptions: the checker
re-derives the transition from the **approved config** and requires the
checkpoint to have landed on exactly what it implies. A resume must name an
event the config declares, with a `post_event_inventory`; the tracked
inventory and both risk baskets must equal that holding; the regime must be
flat; and the cumulative, daily and last equity marks must all be that
holding priced at the tick's own reference marks. A discard must have
actually emptied the window. Anything else is still a continuity violation.

If the token was relisted at a different contract, stop: that is a new
instrument. Change `pair` and use `reset-window` (see below) rather than
trying to resume the old window onto it.

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
  what they now claim to, so the load fails and names the field. Start a
  fresh window deliberately, with `reset-window` (below). Expect to re-serve
  the full `min_signal_samples` warmup before entries resume.
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

### reset-window: starting a fresh window without breaking the event stream

    arcus-spot-execute-once reset-window CONFIG_YAML

This is the *only* sanctioned way to start a fresh window under a
state-invalidating change (bot-strategy#903).

The procedure this replaces -- "remove the checkpoint file and let the next
tick start a fresh window" -- predates the hash-chained durable event stream
(#825) and has not worked since. A fresh checkpoint numbers its first event
1 while the stream tail is at N, `validate_event_continuity` refuses the
discontinuity, and the tick exits non-zero *after* staging its pending
event; every later tick then refuses that incompatible pending event too.
The 2026-09-04 NVDA/AMD -> SPY/QQQ change hit exactly this and was only
recovered by hand-editing executor state, which the rollback runbook
otherwise forbids.

`reset-window` instead writes a fresh checkpoint whose sequence continues
from the stream's verified tail, so the audit chain stays contiguous across
the strategy change and the events' own `pair`/`mode` fields mark the
boundary. It never deletes, truncates, or renumbers the stream.

It is gated exactly like `clear-risk-halt` -- the administrator-owned
`approved_config_sha256` policy digest, plus the same exclusive lock a
dispatching tick takes -- and refuses unless the bot is genuinely idle:

- a staged pending durable event (run a live-tick first; it recovers one),
- an unresolved ledger attempt (`auto-resume`, `archive-rejected-apply`,
  `manual-reconcile-apply`, or a live-tick run for a reconciled one),
- an on-disk live-tick pending plan,
- an open rotation -- exit it under the config it was entered under, since
  a fresh window has no record of what is still held,
- an engaged risk halt -- a fresh state has no halt, so allowing this would
  make `reset-window` a second, undocumented way to disarm the sticky stop
  `clear-risk-halt` exists to gate (#813),
- a config whose state-invalidating fields all still match the checkpoint's.
  There is then nothing for a fresh window to be about, and the reset would
  only discard the accumulated window and restart the loss baselines below
  -- which, repeated before the limit engages, is a way never to reach the
  cumulative halt at all. The approval gate authorises *this config*, not an
  unlimited number of baseline erasures under it. Deploy the changed
  CONFIG_YAML first,
- a missing checkpoint, unconditionally. Every check above reads the
  checkpoint, so without one none of them can run and the reset would be a
  bare re-anchoring of the loss baselines below. The `#902` runbook that
  removed the checkpoint is exactly the case that must not be served this
  way: gating it on "the execution ledger shows no fund-moving attempt"
  covers positions only, while the risk marks are priced against the
  baseline inventory, so daily and cumulative loss accrue -- and a halt can
  engage -- with zero swaps ever dispatched. Put the checkpoint back first,
  from the `.pre-reset` copy beside it or from a `state-backup` directory.
  Restore the copy matching the stream's **current tail**: a checkpoint
  behind the stream is refused too (below), and a live-tick must not be
  used to rebuild one -- against a non-empty stream it stages a sequence-1
  event and checkpoints it before the append rejects the discontinuity,
  leaving a pending event no later tick can recover, which is the wedged
  state this command exists to avoid.
- a checkpoint whose reconciled inventory differs from the config's
  `initial_inventory`, unless `initial_inventory` is itself one of the
  changed fields. The fresh runtime takes its inventory from the
  declaration, while confirmed fills have been adjusting the checkpoint's
  inventory ever since funding -- so a window-length change on a bot that
  has traded would roll the realized deltas back and size later swaps
  against balances the wallet does not have. Update `initial_inventory` to
  the reconciled holdings first; that is also what makes the re-anchored
  risk baselines below mean something. A reset that *does* change
  `initial_inventory` is the re-funding case and is unaffected.
- a checkpoint whose sequence is not exactly the stream's tail, in either
  direction. Ahead of the stream is a recovery case (`repair-report`), not
  a reset. Behind it is the more dangerous one: the checkpoint reads as
  valid while the events it has not seen may hold a completed entry fill or
  an engaged halt whose attempt the ledger has already archived, so every
  other check above would pass on stale state and the reset would replace
  the authoritative record rather than continue it.

The replaced checkpoint is copied aside and the stale observation-evidence
sidecar is moved aside, both as `<name>.pre-reset.<nanos>` in the state
directory. Neither is a verified backup (no manifest, nothing reads them
back), so the surrounding procedure is unchanged:

1. stop the timer,
2. `state-backup` **before** the config swap (it verifies against the *old*
   config),
3. deploy the new CONFIG_YAML and its `hash-config` digest,
4. `reset-window CONFIG_YAML`,
5. a fresh `state-backup`, then re-enable the timer.

The risk baselines restart with the window: `initial_equity_usd` and the
buy-and-hold basket (#813) are re-marked on the next tick against the new
inventory, so cumulative-loss accounting starts from the new baseline. That
is inherent to re-funding or re-pairing the bot, but decide the loss limits
with it in mind.

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

### Exit sizing: the open-quantity row (bot-strategy#906)

While the checkpoint is rotated, `live-tick` requests one extra recorder
row in addition to the bidirectional notional-sized cycles: the exit
direction (held token -> other token) quoted at **exactly the tracked open
rotation quantity** in raw units (`ArcusSpotRecorderConfig::
fixed_sell_amount_rows`, `ArcusSpotRoundTripRecord::fixed_sell_amount`).
`step_at` selects that row by exact raw amount and executes its forward
leg, so a `mean_reversion_exit`/`max_hold_exit`/`corporate_action_exit`
plan always sells the
whole open quantity and the regime returns to `neutral` on fill. The
row's chained reverse leg only supplies the informational round-trip cost
figure; as with every exit, only the leg actually executed is checked for
freshness, and the cost cap is not applied.

Before this, exits were taken from the reverse leg of the entry-direction
notional cycle -- i.e. "sell `notional_usd` worth of the held token at
today's price" -- bounded to the open quantity. The entry had acquired
`notional_usd` worth *minus costs* at the *entry* price, so that leg only
fitted once the held token's USD price had risen above the entry's cost
basis: a hidden "exit only at a gain on the held leg" gate that stalled
both exit triggers indefinitely (the first SPY/QQQ rotation of #902 sat on
`rotation_limit` with z back at 0 and `max_hold_secs` unable to unwind
it). Requesting the exit-sized quote is what removes the gate; prorating
the notional leg down would instead synthesize a fill price the venue
never quoted (the reason that earlier check was written as a hard hold).

The unlocked read of the checkpoint that decides whether to request the
row happens before the snapshot fetch; the decision itself is still made
against the locked load, and a checkpoint that moves in between (a
concurrent fill) simply leaves the row unmatched. A snapshot that carries
no such row at all -- an archival recorder replay, or one collected before
this change -- falls back to the entry-cycle reverse leg with the previous
bound, so replays of old archives are unchanged; a row that is present but
unusable (errored, stale, quoted at a different amount) is a hold, never a
silent fallback. The pinned `router.trusted_token_decimals` entry for the
held token is required to size the request; a missing pin fails the tick.

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

The workflow is accepted only from `master`/`main`. It resolves the
dex-connector ref from `Cargo.lock` through the shared
`.github/workflows/_resolve-dex-connector-ref.yml` (or uses the explicitly
supplied manual-dispatch override), uses
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

Every decision checkpointed by `live-tick` or the shared-state
`arcus-spot-propose-plan propose` command is also appended while holding the
checkpoint namespace lock to a private, daily, hash-chained event stream under
`/var/lib/debot-arcus/spot-execute-once/live-tick-events/`. Checkpoint
publication is wrapped in a recoverable protocol: the writer first atomically
stages the exact event and its hash in the sibling mode-0600
`live-tick-event-pending.json`, publishes the checkpoint, appends and fsyncs the
event stream, then removes and directory-fsyncs the pending sidecar. Before any
new snapshot, the next writer completes an event whose checkpoint committed,
discards an advancing event whose checkpoint did not commit, or only clears an
event already present at the stream tail. An empty or partial final append is
truncated only when its bytes are an exact prefix of the staged, hashed record;
unrelated corruption remains a hard error. State backup refuses an unresolved
sidecar. A crash can therefore neither replace the checkpointed payload with a
later stale observation nor duplicate an already-appended event. The separate
`archive-arcus-live-tick-events.timer` verifies each closed UTC day and writes
an immutable compressed segment plus integrity manifest to the private
`debot-dashboard/arcus-archive/live-tick-events/debot-arcus/` prefix.
Before publishing a segment, the archiver briefly takes the shared checkpoint
lock and refuses to proceed while a pending event exists, preventing an
interrupted checkpoint/append boundary from being frozen into an incomplete
immutable archive. It releases the lock before verification/compression/S3 so
normal trade evaluation is not held behind network I/O.
Its catch-up scan walks every UTC date from the first segment through yesterday
using a private, fsynced start-date marker in the archive service's dedicated
`/var/lib/debot-arcus-archive/` systemd state directory. It remains outside the
segment directory, so the event writer still sees only daily JSONL files, and
the read-only archive service never needs write access to those segments. The
marker is not recalculated from the remaining segment filenames. The scan fails
closed if a date is missing, rather than misrepresenting lost evidence as an
empty observation day. Before publishing a later day it also verifies that
day's head against both the preceding local segment and the preceding immutable
archive manifest. A second fsynced marker advances only after a day's manifest
is published, so later timer runs cheaply scan local calendar continuity but do
hash/compression/S3 work only after the archive high-water date.
`deploy-arcus-live-tick-event-archive.yml` installs and enables only that
archive timer; it never invokes the executor or changes the trading timer.
Current local/S3 data is retained indefinitely, while noncurrent S3 versions
expire after 90 days. See `docs/arcus-live-tick-signal-replay.md` for fetch,
verification, and replay commands.

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
