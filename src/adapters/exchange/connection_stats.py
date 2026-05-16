from __future__ import annotations

from typing import Any

from src.adapters.exchange.funding_poller import FundingPoller
from src.adapters.exchange.tick_tracker import TickTracker
from src.adapters.ws.base import RawTickerStream


def build_connection_stats(
    *,
    exchange_id: str,
    tracker: TickTracker,
    funding: FundingPoller,
    exchange: Any,
    running: bool,
    requested_symbols: list[str],
    active_symbols: list[str],
    missing_symbols: list[str],
    raw_stream: RawTickerStream | None,
) -> dict:
    out = {
        "exchange_id": exchange_id,
        "ticks_received": tracker.ticks_received,
        "ticks_errors": tracker.ticks_errors,
        "consecutive_errors": tracker.consecutive_errors,
        "max_consecutive_errors": tracker.max_consecutive_errors,
        "funding_received": funding.received,
        "funding_errors": funding.errors,
        "last_tick_time": tracker.last_tick_time,
        "last_error": tracker.last_error,
        "connected": exchange is not None and running,
        "ticks_per_sec": round(tracker.ticks_per_sec(), 1),
        "last_latency_ms": tracker.last_latency_ms,
        "latency_p50_ms": tracker.percentile(50),
        "latency_p99_ms": tracker.percentile(99),
        "symbols_requested": list(requested_symbols),
        "active_symbols": list(active_symbols),
        "missing_symbols": list(missing_symbols),
    }
    if raw_stream is not None and hasattr(raw_stream, "stats"):
        try:
            out["ws"] = raw_stream.stats()
        except Exception:
            out["ws"] = {}
    return out
