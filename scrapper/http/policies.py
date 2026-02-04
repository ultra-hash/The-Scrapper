from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse

from scrapper.exceptions import CircuitOpenError


def domain_of(url: str) -> str:
    return urlparse(url).netloc.lower()


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    retry_on_status: tuple[int, ...]
    base_backoff_s: float
    max_backoff_s: float

    def should_retry_status(self, status: int) -> bool:
        return status in self.retry_on_status

    def backoff_s(self, attempt: int) -> float:
        # attempt is 1-based
        exp = self.base_backoff_s * (2 ** max(0, attempt - 1))
        exp = min(exp, self.max_backoff_s)
        jitter = random.uniform(0, exp * 0.25)
        return exp + jitter


class RateLimiter:
    """
    Per-domain token bucket rate limiter.
    Async-friendly via busy-sleeping with short intervals (simple, effective).
    """

    def __init__(self, rps: float, burst: int) -> None:
        self.rps = max(0.01, float(rps))
        self.burst = max(1, int(burst))
        self._state: dict[str, tuple[float, float]] = {}
        # state: domain -> (tokens, last_ts)

    async def acquire(self, domain: str) -> None:
        # Simple implementation: compute tokens, wait until >=1.
        # This avoids an asyncio.Lock per domain and is sufficient for I/O bound workloads.
        while True:
            now = time.monotonic()
            tokens, last = self._state.get(domain, (float(self.burst), now))
            tokens = min(float(self.burst), tokens + (now - last) * self.rps)
            if tokens >= 1.0:
                self._state[domain] = (tokens - 1.0, now)
                return
            self._state[domain] = (tokens, now)
            # Sleep for a small slice; compute time to next token.
            to_next = max(0.0, (1.0 - tokens) / self.rps)
            # clamp to keep loop responsive
            await _sleep(min(0.25, max(0.01, to_next)))


class CircuitBreaker:
    def __init__(self, fail_threshold: int, cooldown_s: float) -> None:
        self.fail_threshold = int(fail_threshold)
        self.cooldown_s = float(cooldown_s)
        self._fails: dict[str, int] = {}
        self._open_until: dict[str, float] = {}

    def pre_request(self, domain: str) -> None:
        until = self._open_until.get(domain)
        if until is None:
            return
        if time.monotonic() < until:
            raise CircuitOpenError(f"Circuit open for domain={domain} until={until}")
        # cooldown passed; close circuit
        self._open_until.pop(domain, None)
        self._fails[domain] = 0

    def on_success(self, domain: str) -> None:
        self._fails[domain] = 0
        self._open_until.pop(domain, None)

    def on_failure(self, domain: str) -> None:
        n = self._fails.get(domain, 0) + 1
        self._fails[domain] = n
        if n >= self.fail_threshold:
            self._open_until[domain] = time.monotonic() + self.cooldown_s


async def _sleep(seconds: float) -> None:
    # isolated to avoid importing asyncio at module import time for sync-only callers
    import asyncio

    await asyncio.sleep(seconds)


def retry_after_seconds(headers: dict[str, str] | Iterable[tuple[str, str]]) -> float | None:
    # Support dict-like or iterable
    if isinstance(headers, dict):
        v = headers.get("Retry-After") or headers.get("retry-after")
    else:
        v = None
        for k, vv in headers:
            if k.lower() == "retry-after":
                v = vv
                break
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None

