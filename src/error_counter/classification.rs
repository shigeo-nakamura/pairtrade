//! Pattern-matching predicates used by the error counter to detect
//! transient WS events, STEP_OVERRUN warns, and their recovery markers.
//! Co-located with the substring constants they reference.

/// Match log lines that represent a WS reset event for the 24h counter.
/// Covers two distinct connector vocabularies:
///
/// - Extended SDK emits `... Connection reset without closing handshake ...`
///   (orderbook / public trades / account stream errors)
/// - Lighter (tungstenite) emits the primary error as
///   `WebSocket error: IO error: Connection reset (by peer|...)` followed by
///   a `WebSocket IO error detail:` line. Only the primary line counts —
///   the detail line is intentionally excluded so each reset increments the
///   counter exactly once (bot-strategy#486).
pub(super) fn is_ws_reset_event(msg: &str) -> bool {
    msg.contains("Connection reset without closing handshake")
        || msg.starts_with("WebSocket error: IO error: Connection reset")
}

/// Match log lines that signal a transient connectivity event whose effect
/// should be suppressed if the bot recovers within `WS_DEFER_WINDOW_SECS`.
/// Covers (1) the connector ERROR raised by the tungstenite WS layer when
/// the upstream RST-resets, and (2) the WARN downstream of that — the
/// xvenue-arb tick error and the pairtrade orderbook-stale signals — that
/// fire while the reconnect is in progress.
pub(super) fn is_ws_transient_event(msg: &str) -> bool {
    msg.starts_with("WebSocket error:")
        || msg.starts_with("WebSocket IO error detail:")
        || msg.contains("tick error: read_mid")
        || msg.contains("order book snapshot unavailable")
        || msg.contains("waiting for websocket data")
        || msg.starts_with("orderbook stream error:")
        || msg.starts_with("public trades stream error:")
        || msg.starts_with("account stream error:")
}

/// Match log lines that signal a successful WS reconnect. Drains pending
/// transient entries logged within the past `WS_DEFER_WINDOW_SECS`.
pub(super) fn is_ws_recovery_event(msg: &str) -> bool {
    msg.starts_with("WebSocket connected successfully")
        || msg.contains("WebSocket subscriptions sent successfully")
}

/// Match the critical `[STEP_OVERRUN]` warn (mild overruns log at INFO and
/// don't reach this path). Bot-strategy#267 traced one such warn to a
/// normal partial-fill chain: ENTRY started, ETH leg full-filled, BTC leg
/// chained 8 partial fills + reissues, step() returned 12s late, but the
/// trade itself completed cleanly. The warn is observational rather than a
/// failure signal — defer it until the matching completion log lands.
pub(super) fn is_step_overrun_event(msg: &str) -> bool {
    msg.contains("[STEP_OVERRUN]")
}

/// Match the `[ORDER] X entry orders filled` / `[ORDER] X exit orders filled`
/// log lines that drain pending STEP_OVERRUN entries. A successful trade
/// completion within the defer window is taken as proof the slow step()
/// was waiting on order management (not a real stall).
pub(super) fn is_step_overrun_recovery_event(msg: &str) -> bool {
    msg.contains("entry orders filled") || msg.contains("exit orders filled")
}
