from __future__ import annotations

import time
from collections.abc import Callable

from loguru import logger

from src.domain.models import OrderExecutor, OrderResult


async def open_leg_with_timing(
    *,
    conn: OrderExecutor,
    symbol: str,
    side: str,
    base_qty: float,
    quoted_price: float,
    order_type: str,
    decision_mono: float,
    quote_age_ms: float = 0.0,
    max_quote_to_order_age_ms: float = 0.0,
    time_in_force: str | None = None,
    limit_price_slippage_pct: float | None = None,
    record_skip: Callable[[str], None],
) -> tuple[OrderResult, float, float]:
    submit_mono = time.monotonic()
    decision_to_order_ms = (submit_mono - decision_mono) * 1000
    quote_order_age_ms = quote_age_ms + decision_to_order_ms
    if max_quote_to_order_age_ms and quote_order_age_ms > max_quote_to_order_age_ms:
        record_skip(
            f"quote-to-order age {quote_order_age_ms:.0f}ms > "
            f"max {max_quote_to_order_age_ms:.0f}ms on {conn.exchange_id}"
        )
        return OrderResult.empty(conn.exchange_id, symbol, side), 0.0, decision_to_order_ms
    t_start = time.monotonic()
    result = await conn.place_market_order(
        symbol=symbol,
        side=side,
        base_qty=base_qty,
        price=quoted_price,
        order_type=order_type,
        time_in_force=time_in_force,
        limit_price_slippage_pct=limit_price_slippage_pct,
    )
    return result, (time.monotonic() - t_start) * 1000, decision_to_order_ms


async def close_leg_with_retry(
    *,
    conn: OrderExecutor,
    symbol: str,
    side: str,
    qty: float,
    quoted_price: float,
    order_type: str,
    time_in_force: str | None,
    limit_price_slippage_pct: float | None,
) -> tuple[OrderResult, float]:
    t_start = time.monotonic()
    result = await conn.place_market_order(
        symbol=symbol,
        side=side,
        base_qty=qty,
        price=quoted_price,
        order_type=order_type,
        is_close=True,
        time_in_force=time_in_force,
        limit_price_slippage_pct=limit_price_slippage_pct,
    )
    if result.filled_amount < qty * 0.95:
        logger.warning(
            "HANDLED CLOSE PARTIAL: {} {} filled {:.4f}/{:.4f} — "
            "leaving residue (no retry, manual close if needed)",
            side,
            symbol,
            result.filled_amount,
            qty,
        )
    return result, (time.monotonic() - t_start) * 1000
