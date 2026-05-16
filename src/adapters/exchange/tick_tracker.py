"""Tick statistics tracker — counts, latency, event signaling.

Single source of truth for tick counting — used by both raw WS and CCXT paths.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Optional

from src.domain.models import PriceTick


class TickTracker:
    """Tracks tick statistics, latency, and signals consumers.

    Eliminates duplication between _raw_ws_loop and _watch_ticker.
    """

    def __init__(self) -> None:
        self.ticks_received: int = 0
        self.ticks_errors: int = 0
        self.consecutive_errors: int = 0
        self.max_consecutive_errors: int = 0
        self.last_tick_time: float = 0.0
        self.last_error: str = ""
        self.last_latency_ms: float = -1.0  # sentinel: no data yet
        # Bounded deques — O(1) append + auto-eviction. Avoids
        # `[t for t in xs if ...]` re-allocation on every tick (which used
        # to allocate ~hundreds of lists/sec under load and put real
        # pressure on the GC).
        self._tick_timestamps: deque[float] = deque(maxlen=2048)
        self._latencies: deque[float] = deque(maxlen=200)
        self._latest_ticks: dict[str, PriceTick] = {}
        self._tick_events: dict[str, asyncio.Event] = {}

    def init_symbol(self, symbol: str) -> None:
        self._tick_events[symbol] = asyncio.Event()

    def record_tick(
        self,
        tick: PriceTick,
        clock_offset: float,
        clock_synced: bool,
    ) -> None:
        """Record a new tick: update counters, latency, store tick, signal consumer."""
        now = tick.receive_time
        self.ticks_received += 1
        self.last_tick_time = now
        self.consecutive_errors = 0  # reset on success

        # Sliding window for ticks/sec calculation — prune in-place.
        self._tick_timestamps.append(now)
        cutoff = now - 5.0
        ts = self._tick_timestamps
        while ts and ts[0] < cutoff:
            ts.popleft()

        # Latency tracking (clock-corrected, capped at 120 ms)
        if tick.timestamp > 0 and clock_synced:
            corrected = tick.tick_age_ms
            if 1.0 <= corrected <= 120.0:
                self.last_latency_ms = round(corrected, 1)
                # deque(maxlen=200) auto-evicts; no manual slice needed.
                self._latencies.append(corrected)

        # Store latest tick and signal consumer
        self._latest_ticks[tick.symbol] = tick
        event = self._tick_events.get(tick.symbol)
        if event:
            event.set()

    def record_error(self, error: str) -> None:
        self.ticks_errors += 1
        self.consecutive_errors += 1
        if self.consecutive_errors > self.max_consecutive_errors:
            self.max_consecutive_errors = self.consecutive_errors
        self.last_error = error

    def get_latest(self, symbol: str) -> Optional[PriceTick]:
        return self._latest_ticks.get(symbol)

    def get_event(self, symbol: str) -> asyncio.Event:
        return self._tick_events[symbol]

    def ticks_per_sec(self) -> float:
        now = time.time()
        cutoff = now - 5.0
        ts = self._tick_timestamps
        # Prune lazily on read so stale window doesn't inflate count.
        while ts and ts[0] < cutoff:
            ts.popleft()
        return len(ts) / 5.0 if ts else 0.0

    def percentile(self, p: int) -> float:
        """Return p-th latency percentile in ms.

        Sentinel: -1.0 when no samples yet (clock not synced or no ticks with
        usable timestamps). 0.0 is a valid result for sub-millisecond latency
        (e.g. same-AZ exchange) and must NOT be conflated with "no data".
        """
        if not self._latencies:
            return -1.0
        s = sorted(self._latencies)
        idx = min(int(len(s) * p / 100), len(s) - 1)
        return round(s[idx], 1)
