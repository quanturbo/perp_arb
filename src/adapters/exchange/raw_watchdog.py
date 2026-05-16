from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from typing import Optional

from loguru import logger

from src.adapters.exchange.symbol_quarantine import (
    REASON_WS_NO_INITIAL_TICK,
    SymbolQuarantine,
)
from src.adapters.exchange.tick_tracker import TickTracker
from src.adapters.ws.base import RawTickerStream


def find_stalled_raw_symbol(
    *,
    tracker: TickTracker,
    active_symbols: Sequence[str],
    now: float,
    stream_started_at: float,
    timeout_sec: float,
) -> tuple[str, float] | None:
    """Return a symbol that never produced any raw tick after startup grace.

    Book-ticker streams are not guaranteed to emit continuously for low-volume
    contracts. Once a symbol has produced a tick, later quiet periods are not a
    subscription failure; socket health is handled by websocket ping/pong and
    receive timeouts.
    """
    for symbol in active_symbols:
        tick = tracker.get_latest(symbol)
        if tick is not None:
            continue
        age_sec = max(0.0, now - stream_started_at)
        if age_sec > timeout_sec:
            return symbol, round(age_sec, 3)
    return None


async def run_raw_symbol_stall_watchdog(
    *,
    exchange_id: str,
    stream: RawTickerStream,
    tracker: TickTracker,
    active_symbols: list[str],
    ws_timeout: float,
    is_running: Callable[[], bool],
    quarantine: Optional[SymbolQuarantine] = None,
) -> None:
    """Watch for symbols whose ticks go stale.

    If a ``SymbolQuarantine`` is supplied, three consecutive stall
    detections on the same symbol move it to the persistent quarantine
    and drop it from ``active_symbols`` (mutated in place) so the next
    reconnect resubscribes WITHOUT it. The associated reconnect is
    flagged ``quarantine:<symbol>`` so the WS layer does not raise the
    UNSTABLE alert for a known, handled cause.
    """
    stream_started_at = time.time()
    timeout_sec = max(1.0, float(ws_timeout or 10.0))
    interval_sec = min(5.0, max(1.0, timeout_sec / 2.0))
    # Per-symbol last observed tick time. We reset the stall counter as soon
    # as it ADVANCES — independent of `timeout_sec`. A low-volume pair whose
    # ticks arrive every 11s on a 10s timeout used to count as a permanent
    # stall (recovery only fired when age <= timeout); now any forward
    # progress clears the strike count.
    last_seen: dict[str, float] = {}
    while is_running():
        try:
            await asyncio.sleep(interval_sec)
            if quarantine is not None:
                for sym in list(active_symbols):
                    tick = tracker.get_latest(sym)
                    if tick is None:
                        continue
                    rt = float(tick.receive_time or 0.0)
                    prev = last_seen.get(sym, 0.0)
                    if rt > prev:
                        last_seen[sym] = rt
                        quarantine.record_recovery(exchange_id, sym)
            stalled = find_stalled_raw_symbol(
                tracker=tracker,
                active_symbols=active_symbols,
                now=time.time(),
                stream_started_at=stream_started_at,
                timeout_sec=timeout_sec,
            )
            if stalled is None:
                continue
            symbol, age_sec = stalled
            reason = (
                f"raw WS no initial tick for {symbol}: waited {age_sec:.1f}s "
                f"> {timeout_sec:.1f}s"
            )
            tracker.record_error(reason + "; reconnecting")
            logger.warning("{} {}; reconnecting", exchange_id, reason)

            quarantined_now = False
            if quarantine is not None:
                quarantined_now = quarantine.record_stall(
                    exchange_id,
                    symbol,
                    reason_code=REASON_WS_NO_INITIAL_TICK,
                    detail=reason,
                )
                if quarantined_now:
                    try:
                        active_symbols.remove(symbol)
                    except ValueError:
                        pass
                    logger.warning(
                        "{} quarantined {}: no initial raw WS ticker after "
                        "3 reconnect attempts. Continuing without it; will "
                        "skip on every restart until reinstated.",
                        exchange_id, symbol,
                    )

            reconnect_reason = (
                f"quarantine:{symbol}" if quarantined_now else f"stall:{symbol}"
            )
            reconnected = await stream.request_reconnect(reason=reconnect_reason)
            if reconnected:
                stream_started_at = time.time()
            await asyncio.sleep(timeout_sec)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            tracker.record_error(f"raw WS watchdog error: {exc}")
            logger.warning("Raw WS watchdog error on {}: {}", exchange_id, exc)
            await asyncio.sleep(timeout_sec)
