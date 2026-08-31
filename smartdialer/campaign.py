"""Campaign configuration: dialing mode and the knobs a worker needs."""

from __future__ import annotations

import enum
from dataclasses import dataclass


class DialMode(str, enum.Enum):
    PROGRESSIVE = "PROGRESSIVE"
    PREDICTIVE = "PREDICTIVE"


@dataclass
class CampaignConfig:
    id: str
    mode: DialMode = DialMode.PROGRESSIVE
    poll_interval: float = 0.5
    reaper_interval: float = 1.0
    agent_lease_seconds: float = 30.0
    # A liveness timeout, renewed at each state transition (see
    # store.apply_call_transition) — must comfortably exceed the longest
    # expected single phase (ring time, or talk time / AHT) rather than the
    # whole call, but for this prototype we size it for the whole call to
    # avoid needing a separate mid-call heartbeat.
    call_lease_seconds: float = 300.0
    wrap_up_seconds: float = 5.0
    bridge_grace_seconds: float = 5.0
