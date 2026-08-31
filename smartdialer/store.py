"""Shared SQLite-backed store.

This stands in for a shared database that multiple independent dialer worker
processes would hit in a real deployment. Every operation that must be safe
under concurrent access (agent reservation, contact claiming, call state
transitions) is a single atomic SQL statement guarded by SQLite's own write
locking (WAL journal + busy_timeout), *not* a Python-level lock — that way
the correctness proof in tests/test_store_concurrency.py (real OS threads,
independent connections) actually demonstrates DB-level safety rather than
relying on the GIL or an in-process mutex.
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict

from .models import TERMINAL_CALL_STATES, Agent, AgentState, CallJob, CallState, Contact, FailureReason
from .state_machines import AgentStateMachine, CallStateMachine, IllegalTransition

SCHEMA = """
CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    state TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    reserved_by TEXT,
    lease_expires_at REAL,
    current_call_id TEXT
);

CREATE TABLE IF NOT EXISTS contacts (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    claimed INTEGER NOT NULL DEFAULT 0,
    reserved_by TEXT,
    lease_expires_at REAL
);

CREATE TABLE IF NOT EXISTS calls (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    contact_id TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    idempotency_key TEXT UNIQUE NOT NULL,
    state TEXT NOT NULL,
    reason TEXT,
    agent_id TEXT,
    reserved_by TEXT,
    lease_expires_at REAL,
    provider_call_ref TEXT,
    created_at REAL,
    answered_at REAL,
    connected_at REAL,
    ended_at REAL,
    version INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS seen_events (
    event_id TEXT PRIMARY KEY,
    call_id TEXT NOT NULL,
    seen_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS reservation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    action TEXT NOT NULL,
    at REAL NOT NULL
);
"""


def _row_to_agent(row: sqlite3.Row) -> Agent:
    return Agent(
        id=row["id"],
        campaign_id=row["campaign_id"],
        state=AgentState(row["state"]),
        version=row["version"],
        reserved_by=row["reserved_by"],
        lease_expires_at=row["lease_expires_at"],
        current_call_id=row["current_call_id"],
    )


def _row_to_call(row: sqlite3.Row) -> CallJob:
    return CallJob(
        id=row["id"],
        campaign_id=row["campaign_id"],
        contact_id=row["contact_id"],
        phone_number=row["phone_number"],
        idempotency_key=row["idempotency_key"],
        state=CallState(row["state"]),
        reason=FailureReason(row["reason"]) if row["reason"] else None,
        agent_id=row["agent_id"],
        reserved_by=row["reserved_by"],
        lease_expires_at=row["lease_expires_at"],
        provider_call_ref=row["provider_call_ref"],
        created_at=row["created_at"] or 0.0,
        answered_at=row["answered_at"],
        connected_at=row["connected_at"],
        ended_at=row["ended_at"],
        version=row["version"],
    )


class SQLiteStore:
    def __init__(self, path: str, now_fn: Callable[[], float] = time.time):
        self.path = path
        self.now = now_fn
        # check_same_thread=False: multiple threads/tasks share one process-local
        # handle in the async worker path; the real cross-process race is proven
        # separately using one connection per OS thread in the concurrency test.
        self._conn = sqlite3.connect(path, timeout=5.0, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA busy_timeout=5000;")
        # NORMAL is safe (not just fast) under WAL: a crash can't corrupt the
        # database, it can only lose the last few committed transactions,
        # which is an acceptable tradeoff for this workload and meaningfully
        # cuts per-commit fsync latency versus the FULL default.
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # -- retry helper -----------------------------------------------------
    def _execute_retrying(self, sql: str, params: tuple, attempts: int = 10) -> sqlite3.Cursor:
        delay = 0.01
        for attempt in range(attempts):
            try:
                cur = self._conn.execute(sql, params)
                self._conn.commit()
                return cur
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) or attempt == attempts - 1:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.2)
        raise RuntimeError("unreachable")

    # -- agents -------------------------------------------------------------
    def add_agent(self, agent: Agent) -> None:
        self._execute_retrying(
            "INSERT INTO agents (id, campaign_id, state, version, reserved_by, "
            "lease_expires_at, current_call_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (agent.id, agent.campaign_id, agent.state.value, agent.version,
             agent.reserved_by, agent.lease_expires_at, agent.current_call_id),
        )

    def get_agent(self, agent_id: str) -> Agent | None:
        row = self._conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
        return _row_to_agent(row) if row else None

    def list_agents(self, campaign_id: str, state: AgentState | None = None) -> list[Agent]:
        if state is None:
            rows = self._conn.execute(
                "SELECT * FROM agents WHERE campaign_id=? ORDER BY id", (campaign_id,)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM agents WHERE campaign_id=? AND state=? ORDER BY id",
                (campaign_id, state.value),
            ).fetchall()
        return [_row_to_agent(r) for r in rows]

    def count_active_agents(self, campaign_id: str) -> int:
        """Agents currently staffing the campaign (logged in, not paused/offline)
        — the denominator predictive pacing tries to keep continuously busy."""
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM agents WHERE campaign_id=? AND state NOT IN (?, ?)",
            (campaign_id, AgentState.OFFLINE.value, AgentState.PAUSED.value),
        ).fetchone()
        return row["n"]

    def reserve_agent(self, agent_id: str, worker_id: str, lease_seconds: float) -> Agent | None:
        """Atomically claim an AVAILABLE agent. Returns the reserved Agent, or
        None if someone else won the race (or the agent wasn't AVAILABLE)."""
        now = self.now()
        cur = self._execute_retrying(
            "UPDATE agents SET state=?, version=version+1, reserved_by=?, lease_expires_at=? "
            "WHERE id=? AND state=?",
            (AgentState.RESERVED.value, worker_id, now + lease_seconds, agent_id, AgentState.AVAILABLE.value),
        )
        if cur.rowcount != 1:
            return None
        self._execute_retrying(
            "INSERT INTO reservation_log (agent_id, worker_id, action, at) VALUES (?, ?, 'reserve', ?)",
            (agent_id, worker_id, now),
        )
        return self.get_agent(agent_id)

    def transition_agent(
        self,
        agent_id: str,
        worker_id: str,
        target: AgentState,
        expected_current: set[AgentState] | None = None,
        current_call_id: str | None = "__unset__",
        lease_seconds: float | None = None,
        require_holder: bool = True,
    ) -> bool:
        """Generic CAS lifecycle transition, validated against the state machine."""
        agent = self.get_agent(agent_id)
        if agent is None:
            return False
        if expected_current is not None and agent.state not in expected_current:
            return False
        if require_holder and agent.reserved_by is not None and agent.reserved_by != worker_id:
            return False
        try:
            AgentStateMachine.validate(agent.state, target)
        except IllegalTransition:
            return False

        lease = (self.now() + lease_seconds) if lease_seconds is not None else None
        holder = worker_id if target not in (AgentState.AVAILABLE, AgentState.OFFLINE, AgentState.PAUSED) else None
        call_ref = agent.current_call_id if current_call_id == "__unset__" else current_call_id

        cur = self._execute_retrying(
            "UPDATE agents SET state=?, version=version+1, reserved_by=?, lease_expires_at=?, "
            "current_call_id=? WHERE id=? AND state=? AND version=?",
            (target.value, holder, lease, call_ref, agent_id, agent.state.value, agent.version),
        )
        if cur.rowcount == 1:
            self._execute_retrying(
                "INSERT INTO reservation_log (agent_id, worker_id, action, at) VALUES (?, ?, ?, ?)",
                (agent_id, worker_id, f"-> {target.value}", self.now()),
            )
        return cur.rowcount == 1

    def force_release_agent(self, agent_id: str, only_if_call_id: str | None = None) -> bool:
        """Unconditionally return an agent to AVAILABLE. Used by crash/timeout
        recovery once we already know the call it was on has died."""
        agent = self.get_agent(agent_id)
        if agent is None:
            return False
        if only_if_call_id is not None and agent.current_call_id != only_if_call_id:
            return False
        if agent.state == AgentState.AVAILABLE:
            return True
        cur = self._execute_retrying(
            "UPDATE agents SET state=?, version=version+1, reserved_by=NULL, "
            "lease_expires_at=NULL, current_call_id=NULL WHERE id=? AND version=?",
            (AgentState.AVAILABLE.value, agent_id, agent.version),
        )
        return cur.rowcount == 1

    def attach_agent_to_call(self, call_id: str, agent_id: str) -> bool:
        cur = self._execute_retrying(
            "UPDATE calls SET agent_id=?, version=version+1 WHERE id=?",
            (agent_id, call_id),
        )
        return cur.rowcount == 1

    def reclaim_expired_agent_leases(self) -> list[str]:
        """Crash-recovery reaper: agents stuck holding a lease past expiry are
        forced back to AVAILABLE so another worker can pick them up."""
        now = self.now()
        rows = self._conn.execute(
            "SELECT id FROM agents WHERE lease_expires_at IS NOT NULL AND lease_expires_at < ? "
            "AND state IN (?, ?, ?)",
            (now, AgentState.RESERVED.value, AgentState.DIALING.value, AgentState.CONNECTED.value),
        ).fetchall()
        reclaimed = []
        for row in rows:
            cur = self._execute_retrying(
                "UPDATE agents SET state=?, version=version+1, reserved_by=NULL, "
                "lease_expires_at=NULL, current_call_id=NULL WHERE id=? AND lease_expires_at<?",
                (AgentState.AVAILABLE.value, row["id"], now),
            )
            if cur.rowcount == 1:
                reclaimed.append(row["id"])
        return reclaimed

    # -- contacts -------------------------------------------------------------
    def add_contact(self, contact: Contact) -> None:
        self._execute_retrying(
            "INSERT INTO contacts (id, campaign_id, phone_number, claimed) VALUES (?, ?, ?, 0)",
            (contact.id, contact.campaign_id, contact.phone_number),
        )

    def claim_contact(self, campaign_id: str, worker_id: str) -> Contact | None:
        row = self._conn.execute(
            "SELECT id FROM contacts WHERE campaign_id=? AND claimed=0 LIMIT 1", (campaign_id,)
        ).fetchone()
        if row is None:
            return None
        cur = self._execute_retrying(
            "UPDATE contacts SET claimed=1, reserved_by=? WHERE id=? AND claimed=0",
            (worker_id, row["id"]),
        )
        if cur.rowcount != 1:
            return None  # lost the race to another worker
        crow = self._conn.execute("SELECT * FROM contacts WHERE id=?", (row["id"],)).fetchone()
        return Contact(id=crow["id"], campaign_id=crow["campaign_id"], phone_number=crow["phone_number"])

    def requeue_contact(self, contact_id: str) -> None:
        self._execute_retrying(
            "UPDATE contacts SET claimed=0, reserved_by=NULL WHERE id=?", (contact_id,)
        )

    def remaining_contacts(self, campaign_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM contacts WHERE campaign_id=? AND claimed=0", (campaign_id,)
        ).fetchone()
        return row["n"]

    # -- calls -------------------------------------------------------------
    def create_call(self, call: CallJob) -> None:
        d = asdict(call)
        self._execute_retrying(
            "INSERT INTO calls (id, campaign_id, contact_id, phone_number, idempotency_key, state, "
            "reason, agent_id, reserved_by, lease_expires_at, provider_call_ref, created_at, "
            "answered_at, connected_at, ended_at, version) VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                d["id"], d["campaign_id"], d["contact_id"], d["phone_number"], d["idempotency_key"],
                call.state.value, call.reason.value if call.reason else None, d["agent_id"],
                d["reserved_by"], d["lease_expires_at"], d["provider_call_ref"], d["created_at"] or self.now(),
                d["answered_at"], d["connected_at"], d["ended_at"], d["version"],
            ),
        )

    def get_call(self, call_id: str) -> CallJob | None:
        row = self._conn.execute("SELECT * FROM calls WHERE id=?", (call_id,)).fetchone()
        return _row_to_call(row) if row else None

    def list_calls(self, campaign_id: str, state: CallState | None = None) -> list[CallJob]:
        if state is None:
            rows = self._conn.execute("SELECT * FROM calls WHERE campaign_id=?", (campaign_id,)).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM calls WHERE campaign_id=? AND state=?", (campaign_id, state.value)
            ).fetchall()
        return [_row_to_call(r) for r in rows]

    def mark_call_initiated(self, call_id: str, provider_call_ref: str, lease_seconds: float) -> bool:
        cur = self._execute_retrying(
            "UPDATE calls SET state=?, provider_call_ref=?, lease_expires_at=?, version=version+1 "
            "WHERE id=? AND state=?",
            (CallState.INITIATED.value, provider_call_ref, self.now() + lease_seconds,
             call_id, CallState.RESERVED.value),
        )
        return cur.rowcount == 1

    def try_record_event(self, event_id: str, call_id: str) -> bool:
        """Returns True if this event_id is new (should be processed),
        False if it's a duplicate we've already applied."""
        try:
            self._execute_retrying(
                "INSERT INTO seen_events (event_id, call_id, seen_at) VALUES (?, ?, ?)",
                (event_id, call_id, self.now()),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def apply_call_transition(
        self,
        call_id: str,
        new_state: CallState,
        reason: FailureReason | None = None,
        timestamp_field: str | None = None,
        renew_lease_seconds: float | None = None,
    ) -> CallJob | None:
        """Apply a validated state change (caller has already run it through
        CallStateMachine.reconcile). Returns the updated call, or None if the
        row changed under us and the CAS lost (caller should retry/reread).

        The call's lease is a liveness timeout, not a fixed deadline: a
        terminal state always clears it, and a non-terminal state renews it
        to `now + renew_lease_seconds` (when given) so a long CONNECTED call
        isn't falsely reaped mid-conversation just because it outlived the
        lease window that was set back when it was first INITIATED."""
        call = self.get_call(call_id)
        if call is None:
            return None
        if call.state in TERMINAL_CALL_STATES:
            # Terminal states are sticky at the data layer too (not just in
            # the caller's reconcile() logic) so a slow in-flight bridge
            # attempt can never resurrect a call the reaper already reaped.
            return None
        extra_col = f", {timestamp_field}=?" if timestamp_field else ""
        extra_val = (self.now(),) if timestamp_field else ()

        if new_state in TERMINAL_CALL_STATES:
            lease_val = None
        elif renew_lease_seconds is not None:
            lease_val = self.now() + renew_lease_seconds
        else:
            lease_val = call.lease_expires_at

        cur = self._execute_retrying(
            f"UPDATE calls SET state=?, reason=?, lease_expires_at=?, version=version+1{extra_col} "
            f"WHERE id=? AND version=?",
            (new_state.value, reason.value if reason else None, lease_val, *extra_val, call_id, call.version),
        )
        if cur.rowcount != 1:
            return None
        return self.get_call(call_id)

    def reclaim_expired_call_leases(self) -> list[str]:
        now = self.now()
        rows = self._conn.execute(
            "SELECT id FROM calls WHERE lease_expires_at IS NOT NULL AND lease_expires_at < ? "
            "AND state IN (?, ?, ?, ?, ?)",
            (now, CallState.RESERVED.value, CallState.INITIATED.value, CallState.RINGING.value,
             CallState.ANSWERED.value, CallState.CONNECTED.value),
        ).fetchall()
        reclaimed = []
        for row in rows:
            cur = self._execute_retrying(
                "UPDATE calls SET state=?, reason=?, version=version+1, lease_expires_at=NULL "
                "WHERE id=? AND lease_expires_at<?",
                (CallState.FAILED.value, FailureReason.PROVIDER_TIMEOUT.value, row["id"], now),
            )
            if cur.rowcount == 1:
                reclaimed.append(row["id"])
        return reclaimed


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"
