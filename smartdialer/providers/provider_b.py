"""Provider B: slower, with timeouts, duplicate events, and out-of-order events.

Used to exercise the reconciliation logic in state_machines.reconcile() and
the worker's lease-based timeout recovery.
"""

from __future__ import annotations

import uuid

from ..events import EventType
from ..models import FailureReason
from .base import Provider


class ProviderB(Provider):
    name = "provider_b"

    def __init__(
        self,
        *args,
        mean_talk_time: float = 150.0,
        failure_rate: float = 0.08,
        timeout_rate: float = 0.12,
        duplicate_rate: float = 0.15,
        out_of_order_rate: float = 0.10,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.mean_talk_time = mean_talk_time
        self.failure_rate = failure_rate
        self.timeout_rate = timeout_rate
        self.duplicate_rate = duplicate_rate
        self.out_of_order_rate = out_of_order_rate

    async def _simulate_call(self, call_id: str, provider_call_ref: str, phone_number: str) -> None:
        await self.clock.sleep(self.rng.uniform(0.5, 1.5))

        ring_id = uuid.uuid4().hex
        self._emit(call_id, provider_call_ref, EventType.RINGING, event_id=ring_id)
        if self.rng.random() < self.duplicate_rate:
            # exact duplicate: same event_id, must be deduped by seen_events.
            self._emit(call_id, provider_call_ref, EventType.RINGING, event_id=ring_id)

        await self.clock.sleep(self.rng.uniform(2.0, 5.0))

        roll = self.rng.random()
        if roll < self.timeout_rate:
            # Provider silently drops the call: no further event ever arrives.
            # The worker's lease-expiry reaper must be the thing that recovers.
            return
        if roll < self.timeout_rate + self.failure_rate:
            reason = self.rng.choice([FailureReason.NO_ANSWER, FailureReason.BUSY])
            self._emit(call_id, provider_call_ref, EventType.FAILED, reason=reason)
            return

        talk = max(5.0, self.rng.gauss(self.mean_talk_time, self.mean_talk_time * 0.3))

        if self.rng.random() < self.out_of_order_rate:
            # Out-of-order: terminal event arrives before ANSWERED.
            self._emit(call_id, provider_call_ref, EventType.COMPLETED)
            self._emit(call_id, provider_call_ref, EventType.ANSWERED)
            return

        answered_id = uuid.uuid4().hex
        self._emit(call_id, provider_call_ref, EventType.ANSWERED, event_id=answered_id)
        if self.rng.random() < self.duplicate_rate:
            # Semantic duplicate: new event_id, same event type -> dropped by rank check.
            self._emit(call_id, provider_call_ref, EventType.ANSWERED)

        await self.clock.sleep(talk)
        completed_id = uuid.uuid4().hex
        self._emit(call_id, provider_call_ref, EventType.COMPLETED, event_id=completed_id)
        if self.rng.random() < self.duplicate_rate:
            self._emit(call_id, provider_call_ref, EventType.COMPLETED, event_id=completed_id)
