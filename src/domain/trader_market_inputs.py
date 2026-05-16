from __future__ import annotations

import time
from math import isfinite
from typing import Callable


def extract_quote(prices: dict, ex_id: str, *, use_ask: bool) -> tuple[float, float, float]:
    p = prices.get(ex_id, {})
    quote = float(p.get("ask") or p.get("last", 0)) if use_ask else float(p.get("bid") or p.get("last", 0))
    tick_age_ms = float(p.get("tick_age_ms", 0) or 0)
    server_age = p.get("server_age_ms")
    data_age = p.get("data_age_ms")
    if server_age is not None and data_age is not None:
        raw_quote_age = max(float(server_age or 0.0), float(data_age or 0.0))
    elif server_age is not None:
        raw_quote_age = server_age
    else:
        raw_quote_age = data_age
    if raw_quote_age is not None:
        quote_age_ms = float(raw_quote_age)
    else:
        receive_time = float(p.get("receive_time", 0) or 0)
        quote_age_ms = max(0.0, (time.time() - receive_time) * 1000.0) if receive_time > 0 else tick_age_ms
    if not isfinite(quote_age_ms):
        quote_age_ms = 0.0
    return quote, tick_age_ms, quote_age_ms


def extract_top_book_qty(prices: dict, ex_id: str, *, use_ask: bool) -> float | None:
    value = prices.get(ex_id, {}).get("ask_qty" if use_ask else "bid_qty")
    if value is None:
        return None
    try:
        qty = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(qty) or qty <= 0:
        return None
    return qty


def passes_top_book_depth_gate(
    *,
    symbol: str,
    exchange_id: str,
    side: str,
    base_qty: float,
    top_qty: float | None,
    max_top_book_usage_pct: float,
    record_skip: Callable[[str], None],
) -> bool:
    if not max_top_book_usage_pct or top_qty is None:
        return True
    max_allowed_qty = top_qty * max_top_book_usage_pct / 100.0
    if base_qty <= max_allowed_qty:
        return True
    record_skip(
        (
            f"top book depth on {exchange_id} {side} {symbol}: "
            f"need {base_qty:.6f}, allowed {max_allowed_qty:.6f} "
            f"({max_top_book_usage_pct:.0f}% of {top_qty:.6f})"
        )
    )
    return False
