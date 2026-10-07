"""Incoming sample flood control.

A token bucket per probe tolerates short bursts (several probes genuinely
report together) but collapses a runaway reporter down to its configured steady
rate. Buckets live in memory keyed by probe: throttling is a reception concern,
not historical state, so it is deliberately not persisted.
"""

from __future__ import annotations

import time


class RateLimiter:
    """Token bucket limiter, one bucket per probe id."""

    def __init__(self, rate: float, burst: int, clock=time.monotonic) -> None:
        self._rate = rate
        self._burst = burst
        self._clock = clock
        self._tokens: dict[str, float] = {}
        self._updated: dict[str, float] = {}

    def reconfigure(self, rate: float, burst: int) -> None:
        self._rate = rate
        self._burst = burst

    def allow(self, probe: str) -> bool:
        now = self._clock()
        last = self._updated.get(probe, now)
        available = self._tokens.get(probe, float(self._burst))
        available = min(float(self._burst), available + (now - last) * self._rate)
        self._updated[probe] = now
        if available >= 1.0:
            self._tokens[probe] = available - 1.0
            return True
        self._tokens[probe] = available
        return False

    def reset(self, probe: str | None = None) -> None:
        if probe is None:
            self._tokens.clear()
            self._updated.clear()
        else:
            self._tokens.pop(probe, None)
            self._updated.pop(probe, None)
