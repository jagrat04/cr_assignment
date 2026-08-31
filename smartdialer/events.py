"""Provider event envelope used by mock (and, in principle, real) telecom providers."""

from __future__ import annotations

import enum
from dataclasses import dataclass

from .models import FailureReason


class EventType(str, enum.Enum):
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ProviderEvent:
    event_id: str
    call_id: str
    provider_call_ref: str
    type: EventType
    reason: FailureReason | None = None
    at: float = 0.0
