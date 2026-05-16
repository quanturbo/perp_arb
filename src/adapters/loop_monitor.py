"""Event-loop stall watchdog.

Detects pauses in the asyncio event loop (garbage collection, sync I/O,
CPU-heavy sync code) and emits structured log records.

How it works:
  A coroutine awaits `asyncio.sleep(check_interval)` in a loop. If the
  actual wall-clock elapsed time between awakenings is much larger than
  the sleep interval, the loop was stalled for the excess time.

Thresholds:
  * < stall_warn_ms:  silent (healthy)
  * >= stall_warn_ms: WARNING (noisy diagnostic, not alerting)
  * >= stall_error_ms: ERROR (escalates to Telegram via loguru sink)

This module is purely diagnostic — it never mutates trading state.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from loguru import logger


@dataclass
class LoopMonitorStats:
    """Exposed via /api/exchange_stats (or similar) for the dashboard."""

    total_checks: int = 0
    total_stalls: int = 0
    max_stall_ms: float = 0.0
    last_stall_ms: float = 0.0
    last_stall_ts: float = 0.0
    stalls_recent: int = 0           # count in last `_recent_window_sec`
    # Bounded deque — in-place popleft is O(1) and never re-allocates
    # the whole container on every prune (50ms loop = 20 prunes/s).
    _recent_stalls: deque = field(default_factory=deque)


class EventLoopMonitor:
    """Watchdog for asyncio event-loop stalls.

    Typical usage:
        monitor = EventLoopMonitor()
        asyncio.create_task(monitor.run())
        ...
        stats = monitor.stats
    """

    def __init__(
        self,
        check_interval_ms: float = 50.0,
        stall_warn_ms: float = 200.0,
        stall_error_ms: float = 1000.0,
        recent_window_sec: float = 300.0,
        error_cooldown_sec: float = 60.0,
    ):
        self._check_interval = check_interval_ms / 1000.0
        self._stall_warn_ms = stall_warn_ms
        self._stall_error_ms = stall_error_ms
        self._recent_window_sec = recent_window_sec
        self._error_cooldown_sec = error_cooldown_sec

        self.stats = LoopMonitorStats()
        self._last_error_emit: float = 0.0
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def start(self) -> asyncio.Task:
        """Start the monitor as a background task. Returns the task handle."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run(), name="loop-monitor")
        return self._task

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await self._task
            except Exception:
                pass

    async def run(self) -> None:
        """Main loop. Never raises — stops silently on cancel."""
        last_tick = time.monotonic()
        interval_ms = self._check_interval * 1000.0
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self._check_interval)
                now = time.monotonic()
                elapsed_ms = (now - last_tick) * 1000.0
                excess_ms = elapsed_ms - interval_ms
                last_tick = now

                self.stats.total_checks += 1
                self._prune_recent(now)

                if excess_ms < self._stall_warn_ms:
                    continue

                # Stall detected.
                self.stats.total_stalls += 1
                self.stats.last_stall_ms = excess_ms
                self.stats.last_stall_ts = time.time()
                self.stats.max_stall_ms = max(self.stats.max_stall_ms, excess_ms)
                self.stats._recent_stalls.append(now)

                if excess_ms >= self._stall_error_ms:
                    if (time.time() - self._last_error_emit) >= self._error_cooldown_sec:
                        self._last_error_emit = time.time()
                        logger.error(
                            "HANDLED LOOP STALL: Event-loop STALL {:.0f}ms (threshold {:.0f}ms) — "
                            "likely GC pause or sync I/O blocking asyncio. "
                            "Total stalls this session: {} (max {:.0f}ms).",
                            excess_ms, self._stall_error_ms,
                            self.stats.total_stalls, self.stats.max_stall_ms,
                        )
                    else:
                        logger.warning(
                            "Event-loop stall {:.0f}ms (ERROR cooldown active)",
                            excess_ms,
                        )
                else:
                    logger.warning(
                        "Event-loop stall {:.0f}ms (>= {:.0f}ms warn threshold)",
                        excess_ms, self._stall_warn_ms,
                    )
        except asyncio.CancelledError:
            return

    def _prune_recent(self, now_monotonic: float) -> None:
        cutoff = now_monotonic - self._recent_window_sec
        recent = self.stats._recent_stalls
        # In-place O(k) prune — no list re-allocation on every check.
        while recent and recent[0] < cutoff:
            recent.popleft()
        self.stats.stalls_recent = len(recent)

    def to_dict(self) -> dict:
        """Serialise stats for dashboard / API."""
        s = self.stats
        return {
            "total_checks": s.total_checks,
            "total_stalls": s.total_stalls,
            "max_stall_ms": round(s.max_stall_ms, 1),
            "last_stall_ms": round(s.last_stall_ms, 1),
            "last_stall_ts": s.last_stall_ts,
            "stalls_recent": s.stalls_recent,
            "recent_window_sec": self._recent_window_sec,
            "warn_threshold_ms": self._stall_warn_ms,
            "error_threshold_ms": self._stall_error_ms,
        }
