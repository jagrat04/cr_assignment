import pytest

from smartdialer.models import CallState
from smartdialer.state_machines import AgentStateMachine, CallStateMachine, IllegalTransition
from smartdialer.models import AgentState


def test_agent_legal_transitions_pass():
    AgentStateMachine.validate(AgentState.OFFLINE, AgentState.AVAILABLE)
    AgentStateMachine.validate(AgentState.AVAILABLE, AgentState.RESERVED)
    AgentStateMachine.validate(AgentState.RESERVED, AgentState.DIALING)
    AgentStateMachine.validate(AgentState.DIALING, AgentState.CONNECTED)
    AgentStateMachine.validate(AgentState.CONNECTED, AgentState.WRAP_UP)
    AgentStateMachine.validate(AgentState.WRAP_UP, AgentState.AVAILABLE)
    AgentStateMachine.validate(AgentState.AVAILABLE, AgentState.PAUSED)
    AgentStateMachine.validate(AgentState.PAUSED, AgentState.AVAILABLE)


def test_agent_illegal_transitions_raise():
    with pytest.raises(IllegalTransition):
        AgentStateMachine.validate(AgentState.OFFLINE, AgentState.CONNECTED)
    with pytest.raises(IllegalTransition):
        AgentStateMachine.validate(AgentState.AVAILABLE, AgentState.CONNECTED)
    with pytest.raises(IllegalTransition):
        AgentStateMachine.validate(AgentState.WRAP_UP, AgentState.DIALING)


def test_agent_self_transition_is_noop():
    AgentStateMachine.validate(AgentState.AVAILABLE, AgentState.AVAILABLE)


def test_call_legal_transitions_pass():
    CallStateMachine.validate(CallState.QUEUED, CallState.RESERVED)
    CallStateMachine.validate(CallState.RESERVED, CallState.INITIATED)
    CallStateMachine.validate(CallState.INITIATED, CallState.RINGING)
    CallStateMachine.validate(CallState.RINGING, CallState.ANSWERED)
    CallStateMachine.validate(CallState.ANSWERED, CallState.CONNECTED)
    CallStateMachine.validate(CallState.CONNECTED, CallState.COMPLETED)


def test_call_illegal_transitions_raise():
    with pytest.raises(IllegalTransition):
        CallStateMachine.validate(CallState.QUEUED, CallState.CONNECTED)
    with pytest.raises(IllegalTransition):
        CallStateMachine.validate(CallState.COMPLETED, CallState.RINGING)


def test_reconcile_normal_forward_progression():
    assert CallStateMachine.reconcile(CallState.RINGING, CallState.ANSWERED) == CallState.ANSWERED


def test_reconcile_terminal_before_answered_wins():
    # Out-of-order: provider says COMPLETED before we ever saw ANSWERED.
    assert CallStateMachine.reconcile(CallState.RINGING, CallState.COMPLETED) == CallState.COMPLETED


def test_reconcile_terminal_is_sticky():
    # Once terminal, nothing (including a duplicate terminal event) changes it again.
    assert CallStateMachine.reconcile(CallState.COMPLETED, CallState.FAILED) is None
    assert CallStateMachine.reconcile(CallState.COMPLETED, CallState.COMPLETED) is None
    assert CallStateMachine.reconcile(CallState.FAILED, CallState.ANSWERED) is None


def test_reconcile_drops_stale_or_duplicate_nonterminal_event():
    # We're already at ANSWERED; a late-arriving RINGING (duplicate/stale) is dropped.
    assert CallStateMachine.reconcile(CallState.ANSWERED, CallState.RINGING) is None
    assert CallStateMachine.reconcile(CallState.ANSWERED, CallState.ANSWERED) is None


def test_reconcile_accepts_progression_skip():
    # Not literally adjacent, but forward — reconcile is rank-based, not adjacency-based.
    assert CallStateMachine.reconcile(CallState.QUEUED, CallState.ANSWERED) == CallState.ANSWERED
