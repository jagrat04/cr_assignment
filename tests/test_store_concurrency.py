"""Proves agent reservation is safe under real concurrent access.

Unlike an asyncio-based test (single-threaded cooperative scheduling, which
would prove nothing about real cross-process races), this spawns actual OS
threads, each with its own SQLite connection to the same file, and has them
all race to reserve the same single agent. Only one may win — that's the
guarantee multiple independent dialer worker processes depend on.
"""

import threading

from smartdialer.models import Agent, AgentState, Contact
from smartdialer.store import SQLiteStore, new_id


def test_concurrent_agent_reservation_has_exactly_one_winner(tmp_path):
    db_path = str(tmp_path / "concurrency.db")
    setup_store = SQLiteStore(db_path)
    setup_store.add_agent(Agent(id="agent_x", campaign_id="camp", state=AgentState.AVAILABLE))
    setup_store.close()

    n_threads = 25
    results: list[Agent | None] = [None] * n_threads
    barrier = threading.Barrier(n_threads)

    def worker(idx: int):
        store = SQLiteStore(db_path)
        barrier.wait()  # maximize actual overlap
        results[idx] = store.reserve_agent("agent_x", f"worker_{idx}", lease_seconds=30)
        store.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    winners = [r for r in results if r is not None]
    assert len(winners) == 1, f"expected exactly 1 winner, got {len(winners)}"

    verify_store = SQLiteStore(db_path)
    final = verify_store.get_agent("agent_x")
    assert final.state == AgentState.RESERVED
    assert final.reserved_by == winners[0].reserved_by
    verify_store.close()


def test_concurrent_contact_claims_never_double_assign(tmp_path):
    db_path = str(tmp_path / "concurrency_contacts.db")
    setup_store = SQLiteStore(db_path)
    contact_id = new_id("contact")
    setup_store.add_contact(Contact(id=contact_id, campaign_id="camp", phone_number="555-0001"))
    setup_store.close()

    n_threads = 20
    results: list = [None] * n_threads
    barrier = threading.Barrier(n_threads)

    def worker(idx: int):
        store = SQLiteStore(db_path)
        barrier.wait()
        results[idx] = store.claim_contact("camp", f"worker_{idx}")
        store.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert winners[0].id == contact_id


def test_reserve_agent_fails_when_not_available(store):
    store.add_agent(Agent(id="a1", campaign_id="camp", state=AgentState.OFFLINE))
    assert store.reserve_agent("a1", "worker_1", lease_seconds=30) is None


def test_transition_agent_rejects_illegal_edge(store):
    store.add_agent(Agent(id="a1", campaign_id="camp", state=AgentState.AVAILABLE))
    ok = store.transition_agent("a1", "worker_1", AgentState.CONNECTED, require_holder=False)
    assert ok is False
    assert store.get_agent("a1").state == AgentState.AVAILABLE


def test_reclaim_expired_agent_leases(tmp_path):
    now = [1000.0]
    store = SQLiteStore(str(tmp_path / "lease.db"), now_fn=lambda: now[0])
    store.add_agent(Agent(id="a1", campaign_id="camp", state=AgentState.AVAILABLE))
    store.reserve_agent("a1", "worker_1", lease_seconds=10)
    assert store.get_agent("a1").state == AgentState.RESERVED

    now[0] += 5
    assert store.reclaim_expired_agent_leases() == []  # not expired yet

    now[0] += 10
    reclaimed = store.reclaim_expired_agent_leases()
    assert reclaimed == ["a1"]
    assert store.get_agent("a1").state == AgentState.AVAILABLE
    store.close()
