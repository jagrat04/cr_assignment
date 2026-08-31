"""Core domain models: agent/call states and the dataclasses that carry them."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class AgentState(str, enum.Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


class CallState(str, enum.Enum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# Non-terminal call states are ranked so provider events can be reconciled
# when they arrive out of order. Terminal states (COMPLETED/FAILED/CANCELLED)
# are handled specially: a terminal state always wins and applying one twice
# is a no-op, regardless of rank.
CALL_STATE_RANK: dict[CallState, int] = {
    CallState.QUEUED: 0,
    CallState.RESERVED: 1,
    CallState.INITIATED: 2,
    CallState.RINGING: 3,
    CallState.ANSWERED: 4,
    CallState.CONNECTED: 5,
    CallState.COMPLETED: 6,
    CallState.FAILED: 6,
    CallState.CANCELLED: 6,
}

TERMINAL_CALL_STATES = frozenset({CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED})


class FailureReason(str, enum.Enum):
    NO_ANSWER = "no_answer"
    BUSY = "busy"
    PROVIDER_TIMEOUT = "provider_timeout"
    PROVIDER_ERROR = "provider_error"
    ABANDONED_NO_AGENT = "abandoned_no_agent"


@dataclass
class Agent:
    id: str
    campaign_id: str
    state: AgentState = AgentState.OFFLINE
    version: int = 0
    reserved_by: str | None = None
    lease_expires_at: float | None = None
    current_call_id: str | None = None


@dataclass
class Contact:
    id: str
    campaign_id: str
    phone_number: str


@dataclass
class CallJob:
    id: str
    campaign_id: str
    contact_id: str
    phone_number: str
    idempotency_key: str
    state: CallState = CallState.QUEUED
    reason: FailureReason | None = None
    agent_id: str | None = None
    reserved_by: str | None = None
    lease_expires_at: float | None = None
    provider_call_ref: str | None = None
    created_at: float = 0.0
    answered_at: float | None = None
    connected_at: float | None = None
    ended_at: float | None = None
    version: int = 0


@dataclass
class ProviderEventRecord:
    event_id: str
    call_id: str
    seen_at: float = field(default_factory=float)
