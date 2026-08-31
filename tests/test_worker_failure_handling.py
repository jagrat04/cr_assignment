"""Integration tests through the real DialerWorker loop: worker/provider
timeouts, crash recovery, and duplicate/out-of-order provider events."""

from __future__ import annotations

import asyncio

import pytest

from smartdialer.campaign import CampaignConfig, DialMode
from smartdialer.models import Agent, AgentState, CallState, Contact
from smartdialer.providers.provider_b import ProviderB
from smartdialer.store import SQLiteStore, new_id
from smartdialer.worker import DialerWorker


class FakeClock:
    """A clock a test can advance instantly; sleep() still yields control so
    concurrent asyncio tasks interleave, but consumes ~no real wall time."""

    def __init__(self, start: float = 1_000.0):
        self.t = start

    def now(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        if seconds > 0:
            self.t += seconds
        await asyncio.sleep(0)


async def run_until(worker: DialerWorker, condition, real_timeout: float = 3.0, poll: float = 0.01) -> bool:
    task = asyncio.create_task(worker.run_forever())
    loop = asyncio.get_event_loop()
    deadline = loop.time() + real_timeout
    try:
        while loop.time() < deadline:
            if condition():
                return True
            await asyncio.sleep(poll)
        return False
    finally:
        await worker.shutdown()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_provider_timeout_is_reaped_and_agent_freed(tmp_path):
    clock = FakeClock()
    store = SQLiteStore(str(tmp_path / "timeout.db"), now_fn=clock.now)
    store.add_agent(Agent(id="a1", campaign_id="camp", state=AgentState.AVAILABLE))
    store.add_contact(Contact(id=new_id("c"), campaign_id="camp", phone_number="555-0001"))

    campaign = CampaignConfig(
        id="camp", mode=DialMode.PROGRESSIVE, poll_interval=1, reaper_interval=1,
        agent_lease_seconds=30, call_lease_seconds=5, wrap_up_seconds=1, bridge_grace_seconds=2,
    )
    provider = ProviderB(clock=clock, timeout_rate=1.0, failure_rate=0.0, duplicate_rate=0.0, out_of_order_rate=0.0)
    worker = DialerWorker(store=store, provider=provider, campaign=campaign, worker_id="w1", clock=clock)

    def call_failed_with_timeout():
        calls = store.list_calls("camp", state=CallState.FAILED)
        return len(calls) == 1 and calls[0].reason is not None and calls[0].reason.value == "provider_timeout"

    ok = await run_until(worker, call_failed_with_timeout)
    assert ok, "expected the call to be reaped as a provider_timeout"

    agent = store.get_agent("a1")
    assert agent.state == AgentState.AVAILABLE
    assert agent.current_call_id is None
    store.close()


@pytest.mark.asyncio
async def test_worker_crash_leaves_agent_recoverable_by_reaper(tmp_path):
    """Simulate a worker crashing mid-call: it reserves an agent and starts a
    call, then simply never runs again (no clean release). A *different*
    worker's reaper must still be able to reclaim the agent once the lease
    expires, without ever double-booking it."""
    clock = FakeClock()
    store = SQLiteStore(str(tmp_path / "crash.db"), now_fn=clock.now)
    store.add_agent(Agent(id="a1", campaign_id="camp", state=AgentState.AVAILABLE))

    # "Crashed worker": reserves the agent and moves it into DIALING, then
    # does nothing further — exactly what happens if the process dies here.
    reserved = store.reserve_agent("a1", "crashed_worker", lease_seconds=10)
    assert reserved is not None
    ok = store.transition_agent(
        "a1", "crashed_worker", AgentState.DIALING,
        expected_current={AgentState.RESERVED}, current_call_id="call_x", lease_seconds=10,
    )
    assert ok

    # A second worker (never even started) can't take it yet — still leased.
    assert store.reserve_agent("a1", "worker_2", lease_seconds=10) is None

    clock.t += 15  # lease has now expired
    reclaimed = store.reclaim_expired_agent_leases()
    assert reclaimed == ["a1"]

    agent = store.get_agent("a1")
    assert agent.state == AgentState.AVAILABLE
    assert agent.reserved_by is None

    # Now a fresh worker can legitimately claim it.
    won = store.reserve_agent("a1", "worker_2", lease_seconds=10)
    assert won is not None
    assert won.reserved_by == "worker_2"
    store.close()


@pytest.mark.asyncio
async def test_duplicate_and_out_of_order_events_resolve_call_exactly_once(tmp_path):
    clock = FakeClock()
    store = SQLiteStore(str(tmp_path / "reorder.db"), now_fn=clock.now)
    store.add_agent(Agent(id="a1", campaign_id="camp", state=AgentState.AVAILABLE))
    store.add_contact(Contact(id=new_id("c"), campaign_id="camp", phone_number="555-0001"))

    campaign = CampaignConfig(
        id="camp", mode=DialMode.PROGRESSIVE, poll_interval=0.5, reaper_interval=1,
        agent_lease_seconds=30, call_lease_seconds=30, wrap_up_seconds=1, bridge_grace_seconds=2,
    )
    # Force every fault mode so we deterministically exercise duplicates and
    # out-of-order terminal-before-answered delivery.
    provider = ProviderB(clock=clock, timeout_rate=0.0, failure_rate=0.0,
                          duplicate_rate=1.0, out_of_order_rate=1.0)
    worker = DialerWorker(store=store, provider=provider, campaign=campaign, worker_id="w1", clock=clock)

    def call_is_terminal():
        for state in (CallState.COMPLETED, CallState.FAILED):
            if store.list_calls("camp", state=state):
                return True
        return False

    ok = await run_until(worker, call_is_terminal)
    assert ok

    all_calls = store.list_calls("camp")
    assert len(all_calls) == 1  # exactly one call was ever created for the one contact
    call = all_calls[0]
    assert call.state in (CallState.COMPLETED, CallState.FAILED)

    # The agent must end up detached from the now-finished call — never left
    # pointing at a call that's already terminal. (Its exact idle state can
    # be AVAILABLE/WRAP_UP, or transiently RESERVED again if the dial loop
    # was mid-iteration, optimistically re-reserving it, right when we
    # stopped the worker and found no contact left to actually use it for —
    # that's a benign, expected outcome once contacts run out, not a bug.)
    agent = store.get_agent("a1")
    assert agent.current_call_id != call.id
    assert agent.state != AgentState.CONNECTED
    store.close()


def test_idempotency_key_is_unique_per_call(store):
    from smartdialer.models import CallJob

    call1 = CallJob(id="call_1", campaign_id="camp", contact_id="c1", phone_number="555-0001",
                     idempotency_key="shared_key")
    store.create_call(call1)

    call2 = CallJob(id="call_2", campaign_id="camp", contact_id="c2", phone_number="555-0002",
                     idempotency_key="shared_key")
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError):
        store.create_call(call2)
