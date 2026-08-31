"""Safety Controller.

The only component in the system authorized to hand an approved dial count
to whatever actually calls the telecom provider. The PredictivePacingEngine
produces a *request*; this controller is a deterministic admission-control
layer in front of it, structurally identical in role to the progressive
dialer's 1:1 executor. It can APPROVE, REDUCE, REJECT, or force a
FALLBACK_TO_PROGRESSIVE, and it reacts to real observed abandonment and
provider error rates, not just to what the pacing engine predicted.
"""

from __future__ import annotations

import enum
from collections import deque
from dataclasses import dataclass

from .predictive import PacingRequest


class SafetyDecision(str, enum.Enum):
    APPROVE = "APPROVE"
    REDUCE = "REDUCE"
    REJECT = "REJECT"
    FALLBACK_TO_PROGRESSIVE = "FALLBACK_TO_PROGRESSIVE"


@dataclass(frozen=True)
class SafetyVerdict:
    decision: SafetyDecision
    approved_calls: int
    reason: str


class SafetyController:
    def __init__(
        self,
        hard_max_line_ratio: float = 2.0,
        min_samples_for_predictive: int = 20,
        max_provider_error_rate: float = 0.30,
        target_abandon_rate: float = 0.03,
        critical_abandon_rate: float | None = None,
        scale_down_factor: float = 0.7,
        scale_up_factor: float = 1.03,
        min_pacing_scale: float = 0.1,
        feedback_window: int = 25,
    ):
        self.hard_max_line_ratio = hard_max_line_ratio
        self.min_samples_for_predictive = min_samples_for_predictive
        self.max_provider_error_rate = max_provider_error_rate
        self.target_abandon_rate = target_abandon_rate
        # min_pacing_scale is a *floor*, not a guarantee of safety: in a
        # tight-capacity scenario (few agents, low answer rate, long AHT)
        # even the floor's residual speculative volume can keep abandonment
        # elevated indefinitely. Past this critical threshold we stop
        # trusting the ratchet to eventually catch up and cut over to a full
        # progressive fallback (zero speculative lines) until it recovers.
        self.critical_abandon_rate = critical_abandon_rate or target_abandon_rate * 3
        self.scale_down_factor = scale_down_factor
        self.scale_up_factor = scale_up_factor
        self.min_pacing_scale = min_pacing_scale
        self._abandon_outcomes: deque[bool] = deque(maxlen=feedback_window)
        self._provider_errors: deque[bool] = deque(maxlen=feedback_window)
        # Persistent multiplicative-decrease/additive-recovery control state:
        # a single bad tick can't fix a persistently over-aggressive pacing
        # ratio, so instead of discounting each request from scratch we
        # ratchet a scale factor down across ticks while abandonment stays
        # above target, and only let it climb back up slowly once it doesn't.
        self.pacing_scale = 1.0

    # -- feedback intake -----------------------------------------------------
    def record_answered_call(self, agent_was_ready: bool) -> None:
        """Call the moment a call is ANSWERED: True if an agent was bridged
        immediately, False if the call had to be abandoned for lack of an
        agent. This is the ground truth the adaptive loop reacts to."""
        self._abandon_outcomes.append(not agent_was_ready)

    def record_provider_outcome(self, errored: bool) -> None:
        """errored=True for a provider timeout/error, False for a clean
        terminal outcome (answered-or-not doesn't matter here)."""
        self._provider_errors.append(errored)

    @property
    def observed_abandon_rate(self) -> float:
        if not self._abandon_outcomes:
            return 0.0
        return sum(self._abandon_outcomes) / len(self._abandon_outcomes)

    @property
    def observed_provider_error_rate(self) -> float:
        if not self._provider_errors:
            return 0.0
        return sum(self._provider_errors) / len(self._provider_errors)

    # -- the gate -----------------------------------------------------------
    def evaluate(self, request: PacingRequest, available_agents: int, in_flight: int) -> SafetyVerdict:
        hard_cap = max(0, round(available_agents * self.hard_max_line_ratio) - in_flight)

        if request.sample_size < self.min_samples_for_predictive:
            approved = min(available_agents, hard_cap)
            return SafetyVerdict(
                SafetyDecision.FALLBACK_TO_PROGRESSIVE, approved,
                f"insufficient samples ({request.sample_size} < {self.min_samples_for_predictive}); "
                f"falling back to 1:1 progressive pacing",
            )

        if self.observed_provider_error_rate > self.max_provider_error_rate:
            approved = min(available_agents, hard_cap)
            return SafetyVerdict(
                SafetyDecision.FALLBACK_TO_PROGRESSIVE, approved,
                f"provider error rate {self.observed_provider_error_rate:.0%} exceeds "
                f"{self.max_provider_error_rate:.0%}; circuit breaker open, falling back",
            )

        # Update the persistent control state from ground-truth feedback
        # before applying it, so the effect of past over-pacing compounds
        # instead of resetting every tick.
        if self._abandon_outcomes:
            if self.observed_abandon_rate > self.target_abandon_rate:
                self.pacing_scale = max(self.min_pacing_scale, self.pacing_scale * self.scale_down_factor)
            elif self.observed_abandon_rate < self.target_abandon_rate / 2:
                self.pacing_scale = min(1.0, self.pacing_scale * self.scale_up_factor)

        if self._abandon_outcomes and self.observed_abandon_rate > self.critical_abandon_rate:
            approved = min(available_agents, hard_cap)
            return SafetyVerdict(
                SafetyDecision.FALLBACK_TO_PROGRESSIVE, approved,
                f"observed abandonment {self.observed_abandon_rate:.0%} exceeds critical threshold "
                f"{self.critical_abandon_rate:.0%} even at pacing scale {self.pacing_scale:.0%}; "
                f"pausing all speculative lines until it recovers",
            )

        # Scale the *speculative portion of the pacing ratio itself* (the
        # part of "lines per agent" beyond a safe 1:1), not just this tick's
        # incremental request. Discounting only the incremental delta would
        # merely slow how fast we ramp up to the same steady-state number of
        # concurrent speculative lines — it would never actually lower it,
        # since once in_flight catches up to the (unscaled) target the delta
        # goes to zero on its own regardless of how badly abandonment is
        # trending. Scaling the ratio changes the target itself.
        extra_ratio = max(0.0, request.ratio - 1.0)
        effective_ratio = 1.0 + extra_ratio * self.pacing_scale
        target_lines = available_agents * effective_ratio
        requested = max(0, round(target_lines) - in_flight)

        if requested > hard_cap:
            approved = hard_cap
            decision = SafetyDecision.REDUCE if approved > 0 else SafetyDecision.REJECT
            return SafetyVerdict(
                decision, approved,
                f"requested {request.requested_calls} exceeds hard cap {hard_cap} "
                f"({self.hard_max_line_ratio}x available agents)",
            )

        if requested < request.requested_calls:
            decision = SafetyDecision.REDUCE if requested > 0 else SafetyDecision.REJECT
            return SafetyVerdict(
                decision, requested,
                f"adaptive pacing scale at {self.pacing_scale:.0%} (observed abandonment "
                f"{self.observed_abandon_rate:.0%}, target {self.target_abandon_rate:.0%})",
            )

        return SafetyVerdict(SafetyDecision.APPROVE, requested, "within safety limits")
