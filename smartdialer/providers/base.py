"""Mock telecom provider interface.

A real integration (e.g. Plivo) would deliver these same events via webhooks;
here a provider pushes ProviderEvent objects onto an asyncio.Queue that the
worker drains, so the rest of the system doesn't care whether events came
from a webhook handler or a simulator.
"""

from __future__ import annotations

import abc
import asyncio
import random
import uuid

from ..clock import RealClock
from ..events import EventType, ProviderEvent
from ..models import FailureReason


class Provider(abc.ABC):
    name: str = "base"

    def __init__(self, clock=None, rng: random.Random | None = None):
        self.clock = clock or RealClock()
        self.rng = rng or random.Random()
        self.event_queue: asyncio.Queue[ProviderEvent] = asyncio.Queue()
        self._idempotency_seen: dict[str, str] = {}
        self._tasks: set[asyncio.Task] = set()

    async def place_call(self, call_id: str, phone_number: str, idempotency_key: str) -> str:
        """Start (or, for a repeated idempotency key, no-op re-acknowledge) a call.
        Returns the provider-side call reference."""
        if idempotency_key in self._idempotency_seen:
            return self._idempotency_seen[idempotency_key]
        provider_call_ref = f"{self.name}_{uuid.uuid4().hex[:10]}"
        self._idempotency_seen[idempotency_key] = provider_call_ref
        task = asyncio.create_task(self._simulate_call(call_id, provider_call_ref, phone_number))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return provider_call_ref

    async def get_event(self) -> ProviderEvent:
        return await self.event_queue.get()

    def _emit(self, call_id: str, provider_call_ref: str, type: EventType, reason: FailureReason | None = None,
               event_id: str | None = None) -> None:
        self.event_queue.put_nowait(
            ProviderEvent(
                event_id=event_id or uuid.uuid4().hex,
                call_id=call_id,
                provider_call_ref=provider_call_ref,
                type=type,
                reason=reason,
                at=self.clock.now(),
            )
        )

    @abc.abstractmethod
    async def _simulate_call(self, call_id: str, provider_call_ref: str, phone_number: str) -> None:
        """Background task that pushes the event sequence for one call."""
        raise NotImplementedError
