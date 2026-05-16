"""Periodic process-memory diagnostic.

Pure observer — never mutates trading state. Logs RSS once per interval
and a WARNING when growth crosses a threshold so OOM kills can be
correlated with workload changes after the fact.

Linux-only: uses /proc/self/status. On other OSes the loop quietly idles.
"""

from __future__ import annotations

import asyncio
import os
from typing import Optional

from loguru import logger


def _read_rss_kb() -> Optional[int]:
    """Return current RSS in KB by parsing /proc/self/status; None on failure."""
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    # 'VmRSS:\t  123456 kB'
                    return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        return None
    return None


class MemoryMonitor:
    """Logs RSS every `interval_sec` and warns on sudden growth.

    Pure diagnostic. Never raises on the loop, never blocks.
    """

    def __init__(
        self,
        interval_sec: float = 60.0,
        warn_growth_mb: float = 50.0,
        critical_growth_mb: float = 100.0,
    ) -> None:
        self._interval = interval_sec
        self._warn_growth_kb = int(warn_growth_mb * 1024)
        self._critical_growth_kb = int(critical_growth_mb * 1024)
        self._baseline_kb: Optional[int] = None
        self._last_kb: Optional[int] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    def start(self) -> Optional[asyncio.Task]:
        """Launch the background loop. Returns None on non-Linux platforms."""
        if _read_rss_kb() is None:
            logger.debug("MemoryMonitor: /proc/self/status unavailable, skipping")
            return None
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="memory-monitor")
        return self._task

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await self._task
            except Exception:
                pass

    async def _run(self) -> None:
        # Take baseline after a short warm-up so post-startup allocs settle.
        await asyncio.sleep(min(15.0, self._interval))
        self._baseline_kb = _read_rss_kb()
        self._last_kb = self._baseline_kb
        if self._baseline_kb is not None:
            logger.info(
                "MEM baseline RSS={:.0f} MB pid={}",
                self._baseline_kb / 1024,
                os.getpid(),
            )

        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._interval
                )
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                return

            rss = _read_rss_kb()
            if rss is None:
                continue
            # Late-bind baseline if first read failed (e.g. transient /proc
            # error during warm-up) — prevents "Δ since start" being
            # permanently undefined.
            if self._baseline_kb is None:
                self._baseline_kb = rss
                self._last_kb = rss
                logger.info(
                    "MEM baseline RSS={:.0f} MB pid={} (late-bound)",
                    rss / 1024, os.getpid(),
                )
                continue

            delta = rss - self._baseline_kb
            since_last = rss - (self._last_kb or rss)
            self._last_kb = rss

            logger.info(
                "MEM RSS={:.0f} MB (Δ since start {:+.0f} MB, Δ since last {:+.0f} MB)",
                rss / 1024, delta / 1024, since_last / 1024,
            )

            if delta >= self._critical_growth_kb:
                logger.warning(
                    "MEM growth {:+.0f} MB since baseline — observe only; "
                    "full GC is disabled because it can stall trading I/O",
                    delta / 1024,
                )
            elif since_last >= self._warn_growth_kb:
                logger.warning(
                    "MEM jumped {:+.0f} MB in last {:.0f}s",
                    since_last / 1024, self._interval,
                )
