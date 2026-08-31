"""Provider A: fast, reliable. Clean in-order events, low failure rate."""

from __future__ import annotations

from ..events import EventType
from ..models import FailureReason
from .base import Provider


class ProviderA(Provider):
    name = "provider_a"

    def __init__(self, *args, mean_talk_time: float = 150.0, failure_rate: float = 0.04, **kwargs):
        super().__init__(*args, **kwargs)
        self.mean_talk_time = mean_talk_time
        self.failure_rate = failure_rate

    async def _simulate_call(self, call_id: str, provider_call_ref: str, phone_number: str) -> None:
        await self.clock.sleep(self.rng.uniform(0.05, 0.3))
        self._emit(call_id, provider_call_ref, EventType.RINGING)

        await self.clock.sleep(self.rng.uniform(1.0, 3.0))
        if self.rng.random() < self.failure_rate:
            reason = self.rng.choice([FailureReason.NO_ANSWER, FailureReason.BUSY])
            self._emit(call_id, provider_call_ref, EventType.FAILED, reason=reason)
            return

        self._emit(call_id, provider_call_ref, EventType.ANSWERED)

        talk = max(5.0, self.rng.gauss(self.mean_talk_time, self.mean_talk_time * 0.25))
        await self.clock.sleep(talk)
        self._emit(call_id, provider_call_ref, EventType.COMPLETED)
