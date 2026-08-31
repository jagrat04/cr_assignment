"""End-to-end campaign runs through the real worker + provider + store stack."""

from __future__ import annotations

import asyncio
import random

import pytest

from smartdialer.campaign import CampaignConfig, DialMode
from smartdialer.clock import ScaledClock
from smartdialer.models import Agent, AgentState, CallState, Contact
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.provider_b import ProviderB
from smartdialer.store import SQLiteStore, new_id
from smartdialer.worker import DialerWorker


def seed_campaign(store: SQLiteStore, campaign_id: str, n_agents: int, n_contacts: int) -> None:
    for i in range(n_agents):
        store.add_agent(Agent(id=f"agent_{i}", campaign_id=campaign_id, state=AgentState.AVAILABLE))
    for i in range(n_contacts):
        store.add_contact(Contact(id=new_id("contact"), campaign_id=campaign_id, phone_number=f"555-{i:04d}"))


async def run_worker_for(worker: DialerWorker, seconds: float) -> None:
    task = asyncio.create_task(worker.run_forever())
    await asyncio.sleep(seconds)
    await worker.shutdown()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def assert_no_agent_double_booked(store: SQLiteStore, campaign_id: str) -> None:
    """Every busy agent must point at a call that itself points back at that
    same agent — no two calls can believe they own the same agent."""
    agents = store.list_agents(campaign_id)
    busy = [a for a in agents if a.state in (AgentState.DIALING, AgentState.CONNECTED) and a.current_call_id]
    seen_calls = set()
    for agent in busy:
        assert agent.current_call_id not in seen_calls
        seen_calls.add(agent.current_call_id)
        call = store.get_call(agent.current_call_id)
        assert call is not None
        assert call.agent_id == agent.id


@pytest.mark.asyncio
async def test_progressive_campaign_runs_cleanly(tmp_path):
    clock = ScaledClock(speed_factor=60.0)
    store = SQLiteStore(str(tmp_path / "e2e_progressive.db"), now_fn=clock.now)
    seed_campaign(store, "camp", n_agents=5, n_contacts=100)

    campaign = CampaignConfig(id="camp", mode=DialMode.PROGRESSIVE, poll_interval=0.3,
                               reaper_interval=1.0, call_lease_seconds=300, wrap_up_seconds=3,
                               bridge_grace_seconds=3)
    provider = ProviderA(clock=clock, rng=random.Random(42), mean_talk_time=60)
    worker = DialerWorker(store=store, provider=provider, campaign=campaign, worker_id="w1", clock=clock)

    await run_worker_for(worker, seconds=8)

    assert worker.stats["dials_placed"] > 0
    assert worker.stats["abandoned"] == 0  # progressive never over-dials, so it can never abandon
    assert_no_agent_double_booked(store, "camp")
    store.close()


@pytest.mark.asyncio
async def test_predictive_campaign_respects_hard_cap_and_stays_bounded(tmp_path):
    clock = ScaledClock(speed_factor=60.0)
    store = SQLiteStore(str(tmp_path / "e2e_predictive.db"), now_fn=clock.now)
    seed_campaign(store, "camp", n_agents=8, n_contacts=400)

    campaign = CampaignConfig(id="camp", mode=DialMode.PREDICTIVE, poll_interval=0.3,
                               reaper_interval=1.0, call_lease_seconds=300, wrap_up_seconds=3,
                               bridge_grace_seconds=4)
    from smartdialer.dialing import SafetyController
    safety = SafetyController(hard_max_line_ratio=2.0)
    provider = ProviderB(clock=clock, rng=random.Random(7), mean_talk_time=45)
    worker = DialerWorker(store=store, provider=provider, campaign=campaign, worker_id="w1",
                           clock=clock, safety_controller=safety)

    await run_worker_for(worker, seconds=12)

    assert worker.stats["dials_placed"] > 0
    # Hard safety invariant: total in-flight + connected lines can never
    # meaningfully exceed hard_max_line_ratio * active agents.
    in_flight_states = (CallState.RESERVED, CallState.INITIATED, CallState.RINGING,
                         CallState.ANSWERED, CallState.CONNECTED)
    open_lines = sum(len(store.list_calls("camp", state=s)) for s in in_flight_states)
    active_agents = store.count_active_agents("camp")
    assert open_lines <= active_agents * safety.hard_max_line_ratio + 1  # +1 rounding slack

    assert_no_agent_double_booked(store, "camp")
    store.close()


@pytest.mark.asyncio
async def test_multiple_workers_share_one_campaign_without_double_booking(tmp_path):
    clock = ScaledClock(speed_factor=60.0)
    store = SQLiteStore(str(tmp_path / "e2e_multiworker.db"), now_fn=clock.now)
    seed_campaign(store, "camp", n_agents=6, n_contacts=200)

    campaign = CampaignConfig(id="camp", mode=DialMode.PROGRESSIVE, poll_interval=0.3,
                               reaper_interval=1.0, call_lease_seconds=300, wrap_up_seconds=2,
                               bridge_grace_seconds=3)

    workers = []
    for i in range(3):
        provider = ProviderA(clock=clock, rng=random.Random(100 + i), mean_talk_time=40)
        workers.append(DialerWorker(store=store, provider=provider, campaign=campaign,
                                     worker_id=f"w{i}", clock=clock))

    tasks = [asyncio.create_task(w.run_forever()) for w in workers]
    await asyncio.sleep(8)
    for w in workers:
        await w.shutdown()
    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await t
        except asyncio.CancelledError:
            pass

    total_dials = sum(w.stats["dials_placed"] for w in workers)
    assert total_dials > 0
    assert_no_agent_double_booked(store, "camp")
    store.close()
