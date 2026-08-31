"""Predictive pacing engine.

Rule-based, no ML: keeps a rolling window of recent call outcomes and uses
the classic "lines-per-agent" heuristic used by real predictive dialers —
if only `answer_rate` of dialed lines connect, dial `1/answer_rate` lines
per agent you want kept busy.

Crucially, this engine never talks to a provider. `request()` returns a
*recommendation* (a PacingRequest); the SafetyController decides whether,
and how much of, that recommendation is actually allowed to happen.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class PacingRequest:
    requested_calls: int
    ratio: float
    sample_size: int
    answer_rate: float
    avg_talk_time: float | None


class PredictivePacingEngine:
    def __init__(
        self,
        window_size: int = 50,
        min_answer_rate: float = 0.10,
        max_answer_rate: float = 0.95,
        default_answer_rate: float = 0.30,
        engine_max_line_ratio: float = 3.0,
    ):
        self.window_size = window_size
        self.min_answer_rate = min_answer_rate
        self.max_answer_rate = max_answer_rate
        self.default_answer_rate = default_answer_rate
        self.engine_max_line_ratio = engine_max_line_ratio
        self._outcomes: deque[bool] = deque(maxlen=window_size)
        self._talk_times: deque[float] = deque(maxlen=window_size)

    def record_outcome(self, answered: bool, talk_time: float | None = None) -> None:
        self._outcomes.append(answered)
        if answered and talk_time is not None:
            self._talk_times.append(talk_time)

    @property
    def sample_size(self) -> int:
        return len(self._outcomes)

    @property
    def answer_rate(self) -> float:
        if not self._outcomes:
            return self.default_answer_rate
        return sum(self._outcomes) / len(self._outcomes)

    @property
    def avg_talk_time(self) -> float | None:
        if not self._talk_times:
            return None
        return sum(self._talk_times) / len(self._talk_times)

    def request(self, available_agents: int, in_flight: int) -> PacingRequest:
        """`available_agents` = agents currently AVAILABLE/about to free up that
        we want kept busy. `in_flight` = calls already dialing/ringing that
        haven't resolved yet (so we don't double-count them)."""
        clamped_rate = min(max(self.answer_rate, self.min_answer_rate), self.max_answer_rate)
        ratio = 1.0 / clamped_rate
        ratio = min(ratio, self.engine_max_line_ratio)

        target_lines = available_agents * ratio
        requested = max(0, round(target_lines) - in_flight)

        return PacingRequest(
            requested_calls=requested,
            ratio=ratio,
            sample_size=self.sample_size,
            answer_rate=self.answer_rate,
            avg_talk_time=self.avg_talk_time,
        )
