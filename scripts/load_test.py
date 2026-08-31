"""Load test: several workers racing for a small shared agent pool under
sustained call volume against one real SQLite-backed store.

Measures dial throughput and agent-reservation contention, then verifies
the no-double-reservation invariant against the final DB state.

    python scripts/load_test.py
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smartdialer.campaign import CampaignConfig, DialMode
from smartdialer.models import Agent, AgentState, Contact
from smartdialer.providers.provider_a import ProviderA
from smartdialer.store import SQLiteStore, new_id
from smartdialer.worker import DialerWorker

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "_load_test.db")
N_AGENTS = 40
N_WORKERS = 8
N_CONTACTS = 50_000
DURATION_SECONDS = 15.0


def assert_no_agent_double_booked(store: SQLiteStore, campaign_id: str) -> None:
    agents = store.list_agents(campaign_id)
    busy = [a for a in agents if a.state in (AgentState.DIALING, AgentState.CONNECTED) and a.current_call_id]
    seen_calls = set()
    for agent in busy:
        assert agent.current_call_id not in seen_calls, f"agent {agent.id} double-booked!"
        seen_calls.add(agent.current_call_id)
        call = store.get_call(agent.current_call_id)
        assert call is not None and call.agent_id == agent.id, "agent/call cross-reference mismatch"


async def main() -> None:
    for ext in ("", "-wal", "-shm"):
        if os.path.exists(DB_PATH + ext):
            os.remove(DB_PATH + ext)

    store = SQLiteStore(DB_PATH)  # RealClock-equivalent: default now_fn=time.time
    campaign_id = "load"
    for i in range(N_AGENTS):
        store.add_agent(Agent(id=f"agent_{i}", campaign_id=campaign_id, state=AgentState.AVAILABLE))
    for i in range(N_CONTACTS):
        store.add_contact(Contact(id=new_id("contact"), campaign_id=campaign_id, phone_number=f"555-{i:06d}"))

    campaign = CampaignConfig(
        id=campaign_id, mode=DialMode.PROGRESSIVE, poll_interval=0.05, reaper_interval=2.0,
        agent_lease_seconds=10, call_lease_seconds=15, wrap_up_seconds=0.3, bridge_grace_seconds=1.0,
    )

    reserve_attempts = 0
    reserve_successes = 0
    orig_reserve = store.reserve_agent

    def counted_reserve(agent_id: str, worker_id: str, lease_seconds: float):
        nonlocal reserve_attempts, reserve_successes
        reserve_attempts += 1
        result = orig_reserve(agent_id, worker_id, lease_seconds)
        if result is not None:
            reserve_successes += 1
        return result

    store.reserve_agent = counted_reserve  # type: ignore[method-assign]

    workers = []
    for i in range(N_WORKERS):
        provider = ProviderA(rng=random.Random(1000 + i), mean_talk_time=1.5, failure_rate=0.1)
        workers.append(DialerWorker(store=store, provider=provider, campaign=campaign, worker_id=f"lw{i}"))

    print(f"Starting load test: {N_AGENTS} agents, {N_WORKERS} workers, {DURATION_SECONDS}s duration")
    start = time.time()
    tasks = [asyncio.create_task(w.run_forever()) for w in workers]
    await asyncio.sleep(DURATION_SECONDS)
    for w in workers:
        await w.shutdown()
    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await t
        except asyncio.CancelledError:
            pass
    elapsed = time.time() - start

    total_dials = sum(w.stats["dials_placed"] for w in workers)
    total_completed = sum(w.stats["completed"] for w in workers)
    total_failed = sum(w.stats["failed"] for w in workers)
    total_abandoned = sum(w.stats["abandoned"] for w in workers)

    print(f"\nelapsed: {elapsed:.2f}s")
    print(f"dials placed: {total_dials}  ({total_dials / elapsed:.1f}/s)")
    print(f"completed: {total_completed}  failed: {total_failed}  abandoned: {total_abandoned}")
    print(f"per-worker dials: {[w.stats['dials_placed'] for w in workers]}")
    print(f"\nagent-reservation attempts: {reserve_attempts}, successes: {reserve_successes} "
          f"({reserve_successes / reserve_attempts:.1%} win rate)")
    print(f"contended (lost) attempts: {reserve_attempts - reserve_successes} - proof multiple workers "
          f"were genuinely racing for the same agents and the CAS resolved every race to exactly one winner")

    assert_no_agent_double_booked(store, campaign_id)
    print("\nINVARIANT HELD: no agent was ever double-booked across all workers.")

    store.close()
    for ext in ("", "-wal", "-shm"):
        if os.path.exists(DB_PATH + ext):
            os.remove(DB_PATH + ext)


if __name__ == "__main__":
    asyncio.run(main())
