"""Clock abstraction so simulations/load tests can run compressed time
instead of waiting through real 90-180s talk times."""

from __future__ import annotations

import asyncio
import time


class RealClock:
    def now(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        if seconds > 0:
            await asyncio.sleep(seconds)


class ScaledClock:
    """A clock whose 'now' advances `speed_factor`x faster than real time.

    Implemented by deriving virtual time from real elapsed time rather than
    manually bumping a counter, so it stays correct under concurrent callers
    without any extra locking.
    """

    def __init__(self, speed_factor: float = 1.0):
        self.speed_factor = speed_factor
        self._real_start = time.time()
        self._virtual_start = self._real_start

    def now(self) -> float:
        real_elapsed = time.time() - self._real_start
        return self._virtual_start + real_elapsed * self.speed_factor

    def elapsed(self) -> float:
        """Virtual seconds elapsed since this clock was created."""
        return self.now() - self._virtual_start

    async def sleep(self, seconds: float) -> None:
        real_seconds = seconds / self.speed_factor
        if real_seconds > 0:
            await asyncio.sleep(real_seconds)
