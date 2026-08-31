import asyncio
import random

import pytest

from smartdialer.clock import ScaledClock
from smartdialer.events import EventType
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.provider_b import ProviderB


async def _drain(provider, n: int, timeout: float = 5.0):
    events = []
    for _ in range(n):
        events.append(await asyncio.wait_for(provider.get_event(), timeout=timeout))
    return events


@pytest.mark.asyncio
async def test_provider_a_answered_call_sequence():
    clock = ScaledClock(speed_factor=200.0)
    provider = ProviderA(clock=clock, rng=random.Random(1), failure_rate=0.0, mean_talk_time=30)
    await provider.place_call("call_1", "555-0001", "idem_1")
    events = await _drain(provider, 3)
    types = [e.type for e in events]
    assert types == [EventType.RINGING, EventType.ANSWERED, EventType.COMPLETED]
    assert all(e.call_id == "call_1" for e in events)


@pytest.mark.asyncio
async def test_provider_a_failure_sequence():
    clock = ScaledClock(speed_factor=200.0)
    provider = ProviderA(clock=clock, rng=random.Random(2), failure_rate=1.0)
    await provider.place_call("call_1", "555-0001", "idem_1")
    events = await _drain(provider, 2)
    assert events[0].type == EventType.RINGING
    assert events[1].type == EventType.FAILED
    assert events[1].reason is not None


@pytest.mark.asyncio
async def test_provider_idempotent_place_call_returns_same_ref():
    clock = ScaledClock(speed_factor=200.0)
    provider = ProviderA(clock=clock, rng=random.Random(3))
    ref1 = await provider.place_call("call_1", "555-0001", "idem_shared")
    ref2 = await provider.place_call("call_1", "555-0001", "idem_shared")
    assert ref1 == ref2


@pytest.mark.asyncio
async def test_provider_b_can_timeout_silently():
    # Force the timeout branch deterministically.
    clock = ScaledClock(speed_factor=500.0)
    provider = ProviderB(clock=clock, rng=random.Random(4), timeout_rate=1.0, failure_rate=0.0,
                          duplicate_rate=0.0, out_of_order_rate=0.0)
    await provider.place_call("call_1", "555-0001", "idem_1")
    ringing = await asyncio.wait_for(provider.get_event(), timeout=5.0)
    assert ringing.type == EventType.RINGING
    # No further event should ever arrive.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(provider.get_event(), timeout=0.3)


@pytest.mark.asyncio
async def test_provider_b_out_of_order_terminal_before_answered():
    clock = ScaledClock(speed_factor=500.0)
    provider = ProviderB(clock=clock, rng=random.Random(5), timeout_rate=0.0, failure_rate=0.0,
                          duplicate_rate=0.0, out_of_order_rate=1.0)
    await provider.place_call("call_1", "555-0001", "idem_1")
    events = await _drain(provider, 3)
    types = [e.type for e in events]
    assert types[0] == EventType.RINGING
    assert types[1] == EventType.COMPLETED
    assert types[2] == EventType.ANSWERED  # arrives *after* the terminal event


@pytest.mark.asyncio
async def test_provider_b_can_duplicate_events():
    clock = ScaledClock(speed_factor=500.0)
    provider = ProviderB(clock=clock, rng=random.Random(6), timeout_rate=0.0, failure_rate=0.0,
                          duplicate_rate=1.0, out_of_order_rate=0.0)
    await provider.place_call("call_1", "555-0001", "idem_1")
    # RINGING, RINGING(dup), ANSWERED, ANSWERED(dup), COMPLETED, COMPLETED(dup)
    events = await _drain(provider, 6, timeout=10.0)
    ringing_events = [e for e in events if e.type == EventType.RINGING]
    assert len(ringing_events) >= 2
    assert ringing_events[0].event_id == ringing_events[1].event_id  # exact duplicate
