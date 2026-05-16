"""Clock offset calibration — NTP-like min-RTT method."""

from __future__ import annotations

import asyncio
import time
from typing import Optional

from loguru import logger

# How often to recalibrate (seconds)
SYNC_INTERVAL = 120.0
_SAMPLES = 5


class ClockSync:
    """Calibrates local-vs-exchange clock offset using min-RTT sampling.

    Usage:
        sync = ClockSync("binanceusdm")
        await sync.calibrate(exchange)   # initial sync
        asyncio.create_task(sync.run_loop(exchange, running_flag))
    """

    def __init__(self, exchange_id: str) -> None:
        self._exchange_id = exchange_id
        self.offset: float = 0.0  # local_time - exchange_time (sec)
        self.synced: bool = False

    async def calibrate(self, exchange) -> None:
        """One-shot calibration: take N samples, keep the one with lowest RTT."""
        best_offset: Optional[float] = None
        best_rtt = float("inf")

        old_rl = exchange.enableRateLimit
        exchange.enableRateLimit = False
        try:
            # Warm-up request (DNS, TLS)
            try:
                await exchange.fetch_time()
            except Exception:
                pass

            for _ in range(_SAMPLES):
                try:
                    t0 = time.time()
                    mono0 = time.monotonic()
                    server_ms = await exchange.fetch_time()
                    mono1 = time.monotonic()
                    t1 = time.time()

                    rtt = mono1 - mono0
                    server_sec = server_ms / 1000.0
                    local_midpoint = (t0 + t1) / 2.0
                    offset = local_midpoint - server_sec

                    if rtt < best_rtt:
                        best_rtt = rtt
                        best_offset = offset
                except Exception as e:
                    logger.debug(
                        "Clock sync failed for {}: {}", self._exchange_id, e
                    )
                await asyncio.sleep(0.05)
        finally:
            exchange.enableRateLimit = old_rl

        if best_offset is not None:
            self.offset = best_offset
            self.synced = True
            logger.info(
                "Clock sync {}: offset={:+.1f}ms (RTT={:.1f}ms)",
                self._exchange_id,
                best_offset * 1000,
                best_rtt * 1000,
            )
        else:
            logger.warning(
                "Clock sync FAILED for {} — using raw timestamps",
                self._exchange_id,
            )

    async def run_loop(self, exchange, running: asyncio.Event | None = None) -> None:
        """Periodically recalibrate. Stops when cancelled or running is cleared."""
        while True:
            await asyncio.sleep(SYNC_INTERVAL)
            if running is not None and not running.is_set():
                break
            try:
                await self.calibrate(exchange)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(
                    "Clock resync error {}: {}", self._exchange_id, e
                )
