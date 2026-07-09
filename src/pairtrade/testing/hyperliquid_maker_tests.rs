// Executable design spec for bot-strategy#709. This is deliberately test-only:
// live Hyperliquid placement must not land until the ALO asymmetric-fill
// recovery policy is explicit and regression-tested.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    A,
    B,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MakerOnlyAction {
    WaitForBothAloFills,
    CancelBothAloOrders,
    CompleteMissingLegWithBoundedIoc { leg: Leg },
    FlattenFilledLegWithBoundedIoc { leg: Leg },
    Done,
}

#[derive(Debug, Clone, Copy)]
struct MakerOnlyPairState {
    leg_a_filled: bool,
    leg_b_filled: bool,
    elapsed_ms: u64,
    maker_fill_timeout_ms: u64,
    taker_hedge_timeout_ms: u64,
    signal_still_valid: bool,
    taker_budget_available: bool,
}

fn decide_maker_only_recovery(state: MakerOnlyPairState) -> MakerOnlyAction {
    match (state.leg_a_filled, state.leg_b_filled) {
        (true, true) => return MakerOnlyAction::Done,
        (false, false) => {
            return if state.elapsed_ms < state.maker_fill_timeout_ms {
                MakerOnlyAction::WaitForBothAloFills
            } else {
                MakerOnlyAction::CancelBothAloOrders
            };
        }
        _ => {}
    }

    if state.elapsed_ms < state.maker_fill_timeout_ms {
        return MakerOnlyAction::WaitForBothAloFills;
    }

    let filled_leg = if state.leg_a_filled { Leg::A } else { Leg::B };
    let missing_leg = if state.leg_a_filled { Leg::B } else { Leg::A };
    let taker_hedge_deadline_ms = state
        .maker_fill_timeout_ms
        .saturating_add(state.taker_hedge_timeout_ms);

    if state.signal_still_valid
        && state.taker_budget_available
        && state.elapsed_ms < taker_hedge_deadline_ms
    {
        MakerOnlyAction::CompleteMissingLegWithBoundedIoc { leg: missing_leg }
    } else {
        MakerOnlyAction::FlattenFilledLegWithBoundedIoc { leg: filled_leg }
    }
}

fn base_state() -> MakerOnlyPairState {
    MakerOnlyPairState {
        leg_a_filled: false,
        leg_b_filled: false,
        elapsed_ms: 1_000,
        maker_fill_timeout_ms: 5_000,
        taker_hedge_timeout_ms: 2_000,
        signal_still_valid: true,
        taker_budget_available: true,
    }
}

#[test]
fn alo_pair_waits_for_symmetric_fills_before_timeout() {
    assert_eq!(
        decide_maker_only_recovery(MakerOnlyPairState {
            leg_a_filled: true,
            ..base_state()
        }),
        MakerOnlyAction::WaitForBothAloFills
    );
}

#[test]
fn alo_pair_cancels_both_orders_when_neither_leg_fills_by_timeout() {
    assert_eq!(
        decide_maker_only_recovery(MakerOnlyPairState {
            elapsed_ms: 5_000,
            ..base_state()
        }),
        MakerOnlyAction::CancelBothAloOrders
    );
}

#[test]
fn one_leg_fill_completes_missing_hedge_with_bounded_ioc_inside_deadline() {
    assert_eq!(
        decide_maker_only_recovery(MakerOnlyPairState {
            leg_a_filled: true,
            elapsed_ms: 5_500,
            ..base_state()
        }),
        MakerOnlyAction::CompleteMissingLegWithBoundedIoc { leg: Leg::B }
    );
}

#[test]
fn one_leg_fill_flattens_when_signal_is_no_longer_valid() {
    assert_eq!(
        decide_maker_only_recovery(MakerOnlyPairState {
            leg_b_filled: true,
            elapsed_ms: 5_500,
            signal_still_valid: false,
            ..base_state()
        }),
        MakerOnlyAction::FlattenFilledLegWithBoundedIoc { leg: Leg::B }
    );
}

#[test]
fn one_leg_fill_flattens_when_taker_budget_is_not_available() {
    assert_eq!(
        decide_maker_only_recovery(MakerOnlyPairState {
            leg_a_filled: true,
            elapsed_ms: 5_500,
            taker_budget_available: false,
            ..base_state()
        }),
        MakerOnlyAction::FlattenFilledLegWithBoundedIoc { leg: Leg::A }
    );
}

#[test]
fn one_leg_fill_flattens_at_or_after_strict_hedge_deadline() {
    assert_eq!(
        decide_maker_only_recovery(MakerOnlyPairState {
            leg_a_filled: true,
            elapsed_ms: 7_000,
            ..base_state()
        }),
        MakerOnlyAction::FlattenFilledLegWithBoundedIoc { leg: Leg::A }
    );
}
