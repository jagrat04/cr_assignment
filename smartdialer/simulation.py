"""Scenario runner used by scripts/run_simulation.py.

Runs one campaign end-to-end against a real DialerWorker + SQLiteStore +
mock provider, with the provider's answer rate / talk time driven by a
schedule function of *virtual* time so a scenario can hold them constant or
change them mid-run, and reports utilization/abandonment/pacing metrics.
"""

from __future__ import annotations

import asyncio
import os
import random
from collections.abc import Callable
from dataclasses import dataclass, field

from .campaign import CampaignConfig, DialMode
from .clock import ScaledClock
from .dialing import PredictivePacingEngine, SafetyController
from .models import Agent, AgentState, CallState, Contact
from .providers.base import Provider
from .providers.provider_a import ProviderA
from .store import SQLiteStore, new_id
from .worker import DialerWorker

Schedule = Callable[[float], float]


def constant(value: float) -> Schedule:
    return lambda _t: value


def steps(duration: float, values: list[float]) -> Schedule:
    """Divide `duration` into len(values) equal segments, stepping through them."""
    n = len(values)

    def f(t: float) -> float:
        idx = min(n - 1, int(t / duration * n))
        return values[idx]

    return f


@dataclass
class ScenarioSpec:
    name: str
    mode: DialMode
    answer_rate_schedule: Schedule
    talk_time_schedule: Schedule
    n_agents: int = 12
    n_contacts: int = 5000
    duration_seconds: float = 1800.0  # virtual seconds
    speed_factor: float = 90.0
    provider_cls: type[Provider] = ProviderA
    seed: int = 0
    hard_max_line_ratio: float = 2.0
    target_abandon_rate: float = 0.03
    schedule_check_interval: float = 0.1  # real seconds


@dataclass
class ScenarioResult:
    name: str
    mode: str
    n_agents: int
    elapsed_virtual_seconds: float
    dials_placed: int
    completed: int
    failed: int
    abandoned: int
    connect_rate: float
    abandonment_rate: float
    utilization_estimate: float
    final_pacing_scale: float
    final_answer_rate_estimate: float
    fallback_engaged: bool
    reason_breakdown: dict[str, int] = field(default_factory=dict)


async def _drive_schedule(provider: Provider, clock: ScaledClock, spec: ScenarioSpec, stop_flag: list[bool]) -> None:
    while not stop_flag[0]:
        t = clock.elapsed()
        answer_rate = spec.answer_rate_schedule(t)
        talk_time = spec.talk_time_schedule(t)
        provider.failure_rate = max(0.0, min(0.95, 1.0 - answer_rate))
        provider.mean_talk_time = talk_time
        await asyncio.sleep(spec.schedule_check_interval)


async def run_scenario(spec: ScenarioSpec, db_path: str | None = None) -> ScenarioResult:
    own_path = db_path is None
    db_path = db_path or f"_sim_{spec.name}.db"
    for ext in ("", "-wal", "-shm"):
        if os.path.exists(db_path + ext):
            os.remove(db_path + ext)

    clock = ScaledClock(speed_factor=spec.speed_factor)
    store = SQLiteStore(db_path, now_fn=clock.now)
    try:
        campaign_id = "sim"
        for i in range(spec.n_agents):
            store.add_agent(Agent(id=f"agent_{i}", campaign_id=campaign_id, state=AgentState.AVAILABLE))
        for i in range(spec.n_contacts):
            store.add_contact(Contact(id=new_id("contact"), campaign_id=campaign_id, phone_number=f"555-{i:05d}"))

        campaign = CampaignConfig(
            id=campaign_id, mode=spec.mode, poll_interval=0.3, reaper_interval=1.0,
            agent_lease_seconds=60, call_lease_seconds=600, wrap_up_seconds=4, bridge_grace_seconds=4,
        )
        rng = random.Random(spec.seed)
        provider = spec.provider_cls(
            clock=clock, rng=rng,
            mean_talk_time=spec.talk_time_schedule(0),
            failure_rate=max(0.0, min(0.95, 1.0 - spec.answer_rate_schedule(0))),
        )
        predictive_engine = PredictivePacingEngine()
        safety = SafetyController(hard_max_line_ratio=spec.hard_max_line_ratio,
                                   target_abandon_rate=spec.target_abandon_rate)
        worker = DialerWorker(
            store=store, provider=provider, campaign=campaign, worker_id="sim_worker",
            clock=clock, predictive_engine=predictive_engine, safety_controller=safety,
        )

        stop_flag = [False]
        schedule_task = asyncio.create_task(_drive_schedule(provider, clock, spec, stop_flag))
        worker_task = asyncio.create_task(worker.run_forever())

        real_seconds = spec.duration_seconds / spec.speed_factor
        await asyncio.sleep(real_seconds)

        stop_flag[0] = True
        await worker.shutdown()
        for t in (worker_task, schedule_task):
            t.cancel()
        for t in (worker_task, schedule_task):
            try:
                await t
            except asyncio.CancelledError:
                pass

        completed = len(store.list_calls(campaign_id, state=CallState.COMPLETED))
        failed_calls = store.list_calls(campaign_id, state=CallState.FAILED)
        reason_breakdown: dict[str, int] = {}
        for c in failed_calls:
            key = c.reason.value if c.reason else "unknown"
            reason_breakdown[key] = reason_breakdown.get(key, 0) + 1
        abandoned = reason_breakdown.get("abandoned_no_agent", 0)
        answered_total = completed + abandoned
        dials = worker.stats["dials_placed"]

        # Utilization estimate: fraction of total available agent-seconds
        # actually spent talking (COMPLETED calls only — abandoned calls
        # never reach an agent).
        elapsed = clock.elapsed()
        completed_talk_seconds = sum(
            (c.ended_at - c.connected_at) for c in store.list_calls(campaign_id, state=CallState.COMPLETED)
            if c.connected_at is not None and c.ended_at is not None
        )
        total_agent_seconds = spec.n_agents * elapsed
        utilization = completed_talk_seconds / total_agent_seconds if total_agent_seconds > 0 else 0.0

        return ScenarioResult(
            name=spec.name, mode=spec.mode.value, n_agents=spec.n_agents,
            elapsed_virtual_seconds=elapsed, dials_placed=dials, completed=completed,
            failed=len(failed_calls), abandoned=abandoned,
            connect_rate=(answered_total / dials) if dials else 0.0,
            abandonment_rate=(abandoned / answered_total) if answered_total else 0.0,
            utilization_estimate=utilization,
            final_pacing_scale=safety.pacing_scale,
            final_answer_rate_estimate=predictive_engine.answer_rate,
            fallback_engaged=(predictive_engine.sample_size < safety.min_samples_for_predictive),
            reason_breakdown=reason_breakdown,
        )
    finally:
        store.close()
        if own_path:
            for ext in ("", "-wal", "-shm"):
                if os.path.exists(db_path + ext):
                    os.remove(db_path + ext)
