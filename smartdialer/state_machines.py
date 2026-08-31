"""Explicit state machines for agents and calls.

Both machines expose a pure `validate()` (raises on an illegal edge) and the
call machine additionally exposes `reconcile()`, which decides what an
incoming (possibly duplicate / out-of-order) provider event should actually
do to a call's current state without ever regressing it illegally.
"""

from __future__ import annotations

from .models import CALL_STATE_RANK, TERMINAL_CALL_STATES, AgentState, CallState


class IllegalTransition(ValueError):
    pass


# ---------------------------------------------------------------------------
# Agent state machine
# ---------------------------------------------------------------------------

AGENT_TRANSITIONS: dict[AgentState, set[AgentState]] = {
    AgentState.OFFLINE: {AgentState.AVAILABLE},
    AgentState.AVAILABLE: {AgentState.RESERVED, AgentState.PAUSED, AgentState.OFFLINE},
    # RESERVED -> CONNECTED covers predictive "bridge" calls: the callee is
    # already answered and waiting, so the newly reserved agent connects
    # directly without a DIALING (agent's own outbound ring) phase.
    AgentState.RESERVED: {AgentState.DIALING, AgentState.CONNECTED, AgentState.AVAILABLE, AgentState.OFFLINE},
    AgentState.DIALING: {AgentState.CONNECTED, AgentState.AVAILABLE, AgentState.WRAP_UP, AgentState.OFFLINE},
    AgentState.CONNECTED: {AgentState.WRAP_UP, AgentState.OFFLINE},
    AgentState.WRAP_UP: {AgentState.AVAILABLE, AgentState.PAUSED, AgentState.OFFLINE},
    AgentState.PAUSED: {AgentState.AVAILABLE, AgentState.OFFLINE},
}


class AgentStateMachine:
    @staticmethod
    def validate(current: AgentState, target: AgentState) -> None:
        if target == current:
            return
        allowed = AGENT_TRANSITIONS.get(current, set())
        if target not in allowed:
            raise IllegalTransition(f"agent transition {current} -> {target} is not allowed")


# ---------------------------------------------------------------------------
# Call state machine
# ---------------------------------------------------------------------------

CALL_TRANSITIONS: dict[CallState, set[CallState]] = {
    CallState.QUEUED: {CallState.RESERVED, CallState.CANCELLED},
    CallState.RESERVED: {CallState.INITIATED, CallState.CANCELLED, CallState.FAILED},
    CallState.INITIATED: {CallState.RINGING, CallState.FAILED, CallState.CANCELLED},
    CallState.RINGING: {CallState.ANSWERED, CallState.FAILED, CallState.CANCELLED},
    CallState.ANSWERED: {CallState.CONNECTED, CallState.FAILED},
    CallState.CONNECTED: {CallState.COMPLETED, CallState.FAILED},
    CallState.COMPLETED: set(),
    CallState.FAILED: set(),
    CallState.CANCELLED: set(),
}


class CallStateMachine:
    @staticmethod
    def validate(current: CallState, target: CallState) -> None:
        if target == current:
            return
        allowed = CALL_TRANSITIONS.get(current, set())
        if target not in allowed:
            raise IllegalTransition(f"call transition {current} -> {target} is not allowed")

    @staticmethod
    def reconcile(current: CallState, incoming: CallState) -> CallState | None:
        """Decide the effective next state for an out-of-order/duplicate event.

        Returns the state to move to, or None if the event should be dropped
        (a no-op — e.g. a stale event arriving after a later one, or a
        duplicate terminal event).
        """
        if current in TERMINAL_CALL_STATES:
            # Terminal states are sticky: once COMPLETED/FAILED/CANCELLED,
            # nothing (including a duplicate of the same terminal event)
            # changes the call again.
            return None

        if incoming in TERMINAL_CALL_STATES:
            # A terminal event always wins, even if it arrives "early"
            # (e.g. COMPLETED before ANSWERED on a flaky provider).
            return incoming

        if CALL_STATE_RANK[incoming] <= CALL_STATE_RANK[current]:
            # Stale or duplicate non-terminal event — drop it.
            return None

        return incoming
