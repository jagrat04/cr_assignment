"""Progressive dialing: exactly one outbound call per available agent.

This is the deterministic safety baseline, and the fallback target the
Safety Controller reaches for whenever predictive pacing can't be trusted.
"""

from __future__ import annotations


class ProgressiveDialer:
    def calls_to_place(self, newly_reserved_agents: int) -> int:
        """1:1 — every agent this worker just reserved gets exactly one call."""
        return max(0, newly_reserved_agents)
