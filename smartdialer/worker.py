"""DialerWorker: the async loop that ties everything together.

Multiple DialerWorker instances (one per "worker process" in a real
deployment) share one SQLiteStore. Correctness under concurrency comes
entirely from the store's atomic CAS operations, not from anything worker
-local, so it's safe to run many of these against the same campaign.
"""

from __future__ import annotations

import asyncio
import logging
import uuid

from .campaign import CampaignConfig, DialMode
from .clock import RealClock
from .dialing import PredictivePacingEngine, ProgressiveDialer, SafetyController, SafetyDecision
from .events import EventType, ProviderEvent
from .models import (
    TERMINAL_CALL_STATES,
    Agent,
    AgentState,
    CallJob,
    CallState,
    FailureReason,
)
from .providers.base import Provider
from .state_machines import CallStateMachine
from .store import SQLiteStore, new_id

logger = logging.getLogger("smartdialer.worker")

_EVENT_TO_STATE = {
    EventType.RINGING: CallState.RINGING,
    EventType.ANSWERED: CallState.ANSWERED,
    EventType.CONNECTED: CallState.CONNECTED,
    EventType.COMPLETED: CallState.COMPLETED,
    EventType.FAILED: CallState.FAILED,
}


class DialerWorker:
    def __init__(
        self,
        store: SQLiteStore,
        provider: Provider,
        campaign: CampaignConfig,
        worker_id: str | None = None,
        predictive_engine: PredictivePacingEngine | None = None,
        safety_controller: SafetyController | None = None,
        progressive_dialer: ProgressiveDialer | None = None,
        clock=None,
    ):
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:8]}"
        self.store = store
        self.provider = provider
        self.campaign = campaign
        self.predictive_engine = predictive_engine or PredictivePacingEngine()
        self.safety_controller = safety_controller or SafetyController()
        self.progressive_dialer = progressive_dialer or ProgressiveDialer()
        self.clock = clock or RealClock()
        self._running = False
        self._background: set[asyncio.Task] = set()
        # One lock per call so events for the same call are always processed
        # in strict sequence (bridging can involve retry sleeps), while
        # events for *different* calls still run fully concurrently instead
        # of queueing up behind whichever call is slowest to bridge.
        self._call_locks: dict[str, asyncio.Lock] = {}
        # Speculative calls that just got ANSWERED and need an agent bridged
        # onto them *now*. _dial_loop drains this ahead of handing any
        # freshly-available agent to a brand new baseline call: an answered
        # call sitting unbridged is exactly the abandonment risk the whole
        # safety story is about, so it always wins the race for a free agent.
        self._pending_bridges: dict[str, CallJob] = {}
        self._bridge_results: dict[str, str] = {}
        self.stats = {"dials_placed": 0, "abandoned": 0, "completed": 0, "failed": 0}

    # -- lifecycle ------------------------------------------------------------
    async def run_forever(self) -> None:
        self._running = True
        tasks = [
            asyncio.create_task(self._dial_loop()),
            asyncio.create_task(self._event_loop()),
            asyncio.create_task(self._reaper_loop()),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass

    def stop(self) -> None:
        self._running = False

    async def shutdown(self) -> None:
        """stop() only flips the run flag for the three main loops; this
        also cancels/awaits any still-running background tasks (e.g. a
        wrap-up-then-release in progress) so nothing touches the store after
        a caller closes it right after shutdown returns."""
        self.stop()
        pending = list(self._background)
        for task in pending:
            task.cancel()
        for task in pending:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    # -- dialing loop ------------------------------------------------------------
    async def _dial_loop(self) -> None:
        # sqlite3 calls are synchronous and provider.place_call() never
        # actually awaits anything either, so without explicit yield points
        # one worker's whole tick (scan agents, reserve each, dial each) runs
        # as a single uninterrupted chunk on the shared event loop — starving
        # every other worker's task until it finishes. asyncio.sleep(0)
        # forces a real yield after each unit of work so multiple workers
        # interleave fairly, the way separate OS processes naturally would.
        while self._running:
            await self._serve_pending_bridges()

            reserved_this_tick: list[Agent] = []
            for agent in self.store.list_agents(self.campaign.id, state=AgentState.AVAILABLE):
                won = self.store.reserve_agent(agent.id, self.worker_id, self.campaign.agent_lease_seconds)
                if won:
                    reserved_this_tick.append(won)
                await asyncio.sleep(0)

            for agent in reserved_this_tick:
                await self._dial_for_agent(agent)
                await asyncio.sleep(0)

            if self.campaign.mode == DialMode.PREDICTIVE:
                await self._predictive_tick()

            await self.clock.sleep(self.campaign.poll_interval)

    async def _serve_pending_bridges(self) -> None:
        """Highest-priority use of a freshly-available agent: bridge it onto
        a call that's already been answered and is waiting, before any agent
        is handed to _dial_for_agent to start a brand new call."""
        for call_id in list(self._pending_bridges.keys()):
            if call_id not in self._pending_bridges:
                continue  # served by an earlier iteration of this same pass
            bridged = False
            for candidate in self.store.list_agents(self.campaign.id, state=AgentState.AVAILABLE):
                reserved = self.store.reserve_agent(candidate.id, self.worker_id, self.campaign.call_lease_seconds)
                if not reserved:
                    continue
                ok = self.store.transition_agent(
                    reserved.id, self.worker_id, AgentState.CONNECTED,
                    expected_current={AgentState.RESERVED}, current_call_id=call_id,
                    lease_seconds=self.campaign.call_lease_seconds,
                )
                if ok:
                    self.store.attach_agent_to_call(call_id, reserved.id)
                    self._bridge_results[call_id] = reserved.id
                    del self._pending_bridges[call_id]
                    bridged = True
                    break
            if not bridged:
                break  # no free agents left this tick; try the rest next tick
            await asyncio.sleep(0)

    async def _predictive_tick(self) -> None:
        # Total staffed headcount (not just agents freed up this tick) is the
        # denominator predictive pacing is trying to keep continuously busy —
        # the whole point is to dial *ahead* of agents becoming free.
        available_agents = self.store.count_active_agents(self.campaign.id)
        # "In flight" must mean every line currently open and consuming
        # capacity — including CONNECTED. A connected call spends the vast
        # majority of its life in that state (talk time dwarfs ring time),
        # so leaving it out would make the pacing math think an agent mid
        # conversation is free capacity and dial straight through it.
        in_flight = sum(
            len(self.store.list_calls(self.campaign.id, state=s))
            for s in (CallState.RESERVED, CallState.INITIATED, CallState.RINGING,
                      CallState.ANSWERED, CallState.CONNECTED)
        )
        request = self.predictive_engine.request(available_agents=available_agents, in_flight=in_flight)
        verdict = self.safety_controller.evaluate(request, available_agents=available_agents, in_flight=in_flight)

        extra = 0 if verdict.decision == SafetyDecision.FALLBACK_TO_PROGRESSIVE else verdict.approved_calls
        if verdict.decision != SafetyDecision.APPROVE:
            logger.info("[%s] safety controller %s: %s", self.worker_id, verdict.decision.value, verdict.reason)

        for _ in range(extra):
            await self._dial_speculative()
            await asyncio.sleep(0)

    async def _dial_for_agent(self, agent: Agent) -> None:
        contact = self.store.claim_contact(self.campaign.id, self.worker_id)
        if contact is None:
            self.store.transition_agent(agent.id, self.worker_id, AgentState.AVAILABLE,
                                         expected_current={AgentState.RESERVED}, current_call_id=None)
            return

        call = CallJob(
            id=new_id("call"),
            campaign_id=self.campaign.id,
            contact_id=contact.id,
            phone_number=contact.phone_number,
            idempotency_key=new_id("idem"),
            state=CallState.RESERVED,
            agent_id=agent.id,
            reserved_by=self.worker_id,
            created_at=self.clock.now(),
        )
        self.store.create_call(call)
        self.store.transition_agent(
            agent.id, self.worker_id, AgentState.DIALING,
            expected_current={AgentState.RESERVED}, current_call_id=call.id,
            lease_seconds=self.campaign.call_lease_seconds,
        )
        await self._place(call)

    async def _dial_speculative(self) -> None:
        contact = self.store.claim_contact(self.campaign.id, self.worker_id)
        if contact is None:
            return
        call = CallJob(
            id=new_id("call"),
            campaign_id=self.campaign.id,
            contact_id=contact.id,
            phone_number=contact.phone_number,
            idempotency_key=new_id("idem"),
            state=CallState.RESERVED,
            agent_id=None,
            reserved_by=self.worker_id,
            created_at=self.clock.now(),
        )
        self.store.create_call(call)
        await self._place(call)

    async def _place(self, call: CallJob) -> None:
        provider_call_ref = await self.provider.place_call(call.id, call.phone_number, call.idempotency_key)
        self.store.mark_call_initiated(call.id, provider_call_ref, self.campaign.call_lease_seconds)
        self.stats["dials_placed"] += 1

    # -- event processing ------------------------------------------------------------
    async def _event_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self.provider.get_event(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            # Dispatch rather than await inline: a slow bridge-retry for one
            # call must never delay processing of every other call's events.
            self._spawn(self._handle_event(event))

    async def _handle_event(self, event: ProviderEvent) -> None:
        lock = self._call_locks.setdefault(event.call_id, asyncio.Lock())
        async with lock:
            await self._handle_event_locked(event)

    async def _handle_event_locked(self, event: ProviderEvent) -> None:
        if not self.store.try_record_event(event.event_id, event.call_id):
            return  # exact duplicate, already applied

        call = self.store.get_call(event.call_id)
        if call is None or call.reserved_by != self.worker_id:
            return  # not ours (e.g. another worker's call in a shared-log test setup)

        incoming_state = _EVENT_TO_STATE[event.type]
        target = CallStateMachine.reconcile(call.state, incoming_state)
        if target is None:
            return  # stale/out-of-order relative to what we already applied

        if target == CallState.ANSWERED:
            await self._handle_answered(call)
            return

        ts_field = {CallState.COMPLETED: "ended_at", CallState.FAILED: "ended_at"}.get(target)
        lease = None if target in TERMINAL_CALL_STATES else self.campaign.call_lease_seconds
        updated = self.store.apply_call_transition(
            call.id, target, reason=event.reason, timestamp_field=ts_field, renew_lease_seconds=lease
        )
        if updated is None:
            return
        if updated.state in TERMINAL_CALL_STATES:
            await self._on_call_terminal(updated)

    async def _handle_answered(self, call: CallJob) -> None:
        bridged_agent_id = await self._bridge_agent(call)
        agent_ready = bridged_agent_id is not None
        self.safety_controller.record_answered_call(agent_ready)

        if agent_ready:
            answered = self.store.apply_call_transition(
                call.id, CallState.ANSWERED, timestamp_field="answered_at",
                renew_lease_seconds=self.campaign.call_lease_seconds,
            )
            if answered is None:
                # Call already went terminal (e.g. reaped as a timeout) while
                # we were bridging — don't strand the agent we just grabbed.
                self.store.force_release_agent(bridged_agent_id, only_if_call_id=call.id)
                return
            self.store.apply_call_transition(
                call.id, CallState.CONNECTED, timestamp_field="connected_at",
                renew_lease_seconds=self.campaign.call_lease_seconds,
            )
        else:
            self.stats["abandoned"] += 1
            answered = self.store.apply_call_transition(
                call.id, CallState.ANSWERED, timestamp_field="answered_at",
                renew_lease_seconds=self.campaign.call_lease_seconds,
            )
            if answered is None:
                return
            updated = self.store.apply_call_transition(
                call.id, CallState.FAILED, reason=FailureReason.ABANDONED_NO_AGENT, timestamp_field="ended_at"
            )
            if updated is not None:
                await self._on_call_terminal(updated)

    async def _bridge_agent(self, call: CallJob) -> str | None:
        """Try to get a live agent onto this (already-answered) call.
        Returns the bridged agent's id, or None if none could be found
        within the grace window."""
        if call.agent_id:
            ok = self.store.transition_agent(
                call.agent_id, self.worker_id, AgentState.CONNECTED,
                expected_current={AgentState.DIALING}, current_call_id=call.id,
                lease_seconds=self.campaign.call_lease_seconds,
            )
            return call.agent_id if ok else None

        # No agent attached (a speculative predictive line). Register with
        # _dial_loop, which owns all agent-reservation decisions and serves
        # this ahead of starting any new call, then just wait for a result.
        self._pending_bridges[call.id] = call
        try:
            deadline = self.clock.now() + self.campaign.bridge_grace_seconds
            while self.clock.now() < deadline:
                agent_id = self._bridge_results.get(call.id)
                if agent_id:
                    return agent_id
                await self.clock.sleep(0.05)
            return None
        finally:
            self._pending_bridges.pop(call.id, None)
            self._bridge_results.pop(call.id, None)

    async def _on_call_terminal(self, call: CallJob) -> None:
        answered = call.answered_at is not None
        talk_time = None
        if call.connected_at is not None and call.ended_at is not None:
            talk_time = call.ended_at - call.connected_at
        self.predictive_engine.record_outcome(answered=answered, talk_time=talk_time)
        self.safety_controller.record_provider_outcome(
            errored=call.reason in (FailureReason.PROVIDER_TIMEOUT, FailureReason.PROVIDER_ERROR)
        )

        if call.state == CallState.COMPLETED:
            self.stats["completed"] += 1
        elif call.state == CallState.FAILED:
            self.stats["failed"] += 1

        self._call_locks.pop(call.id, None)

        if call.agent_id:
            agent = self.store.get_agent(call.agent_id)
            if agent is not None and agent.reserved_by == self.worker_id and agent.current_call_id == call.id:
                if call.connected_at is not None:
                    self._spawn(self._wrap_up_then_release(agent.id))
                else:
                    self.store.transition_agent(
                        agent.id, self.worker_id, AgentState.AVAILABLE,
                        expected_current={AgentState.DIALING, AgentState.RESERVED, AgentState.CONNECTED},
                        current_call_id=None,
                    )

    async def _wrap_up_then_release(self, agent_id: str) -> None:
        self.store.transition_agent(
            agent_id, self.worker_id, AgentState.WRAP_UP,
            expected_current={AgentState.CONNECTED}, lease_seconds=self.campaign.wrap_up_seconds + 5,
        )
        await self.clock.sleep(self.campaign.wrap_up_seconds)
        self.store.transition_agent(agent_id, self.worker_id, AgentState.AVAILABLE,
                                     expected_current={AgentState.WRAP_UP}, current_call_id=None)

    # -- crash / timeout recovery ------------------------------------------------------------
    async def _reaper_loop(self) -> None:
        while self._running:
            reclaimed_calls = self.store.reclaim_expired_call_leases()
            for call_id in reclaimed_calls:
                call = self.store.get_call(call_id)
                if call is None:
                    continue
                if call.agent_id:
                    self.store.force_release_agent(call.agent_id, only_if_call_id=call.id)
                await self._on_call_terminal(call)

            self.store.reclaim_expired_agent_leases()
            await self.clock.sleep(self.campaign.reaper_interval)
