from __future__ import annotations

import asyncio
import time

from loguru import logger

from src.domain.trader_ports import TraderWorkflowPort
from src.domain.trade_position import _log_trade_summary, _make_leg
from src.domain.trade_state import TradeState


async def close_position(trader: TraderWorkflowPort, snapshot) -> None:
    if not trader._position:
        return

    trader._state = TradeState.CLOSING
    pos = trader._position
    quoted_long, tick_age_long, quote_age_long = trader._extract_quote(
        snapshot.prices, pos.exchange_long, use_ask=False,
    )
    quoted_short, tick_age_short, quote_age_short = trader._extract_quote(
        snapshot.prices, pos.exchange_short, use_ask=True,
    )

    long_base_qty = pos.open_long.filled_amount
    short_base_qty = pos.open_short.filled_amount
    if not long_base_qty and not short_base_qty:
        logger.error("Cannot close: no filled_amount on either leg")
        trader._state = TradeState.OPEN
        return
    if not long_base_qty:
        long_base_qty = short_base_qty
    if not short_base_qty:
        short_base_qty = long_base_qty

    logger.info(
        "TRADE CLOSING {} long_qty={:.2f} short_qty={:.2f} long={}(bid={:.6f} age={}ms) short={}(ask={:.6f} age={}ms) "
        "spread={:.4f}%",
        pos.symbol,
        long_base_qty,
        short_base_qty,
        pos.exchange_long,
        quoted_long,
        f"{quote_age_long:.0f}",
        pos.exchange_short,
        quoted_short,
        f"{quote_age_short:.0f}",
        snapshot.price_spread_pct,
    )

    long_conn = trader._streams.get_connection(pos.exchange_long)
    short_conn = trader._streams.get_connection(pos.exchange_short)
    if not long_conn or not short_conn:
        logger.error("Cannot close: missing connection")
        trader._state = TradeState.OPEN
        return

    try:
        start_mono = time.monotonic()
        results = await asyncio.gather(
            asyncio.create_task(trader._close_leg_with_retry(
                long_conn, pos.symbol, "sell", long_base_qty, quoted_long,
            )),
            asyncio.create_task(trader._close_leg_with_retry(
                short_conn, pos.symbol, "buy", short_base_qty, quoted_short,
            )),
            return_exceptions=True,
        )
        long_result, long_dur = trader._unwrap_close_result(
            results[0], pos.exchange_long, pos.symbol, "sell",
        )
        short_result, short_dur = trader._unwrap_close_result(
            results[1], pos.exchange_short, pos.symbol, "buy",
        )
        total_latency_ms = (time.monotonic() - start_mono) * 1000

        pos.close_long = _make_leg(
            pos.exchange_long, "sell", quoted_long,
            long_result, long_dur, tick_age_long, quote_age_long,
        )
        pos.close_short = _make_leg(
            pos.exchange_short, "buy", quoted_short,
            short_result, short_dur, tick_age_short, quote_age_short,
        )
        pos.close_latency_ms = total_latency_ms
        pos.close_spread_pct = snapshot.price_spread_pct
        pos.closed_at = time.time()

        trader._trades_done += 1
        _log_trade_summary(
            "CLOSED", pos.symbol,
            pos.close_long, pos.close_short,
            total_latency_ms, snapshot.price_spread_pct,
            trade_num=f"{trader._trades_done}/{trader._max_trades}",
        )
        if trader._notifier:
            trader._notifier.notify_trade_closed(
                pos.symbol, pos.exchange_long, pos.exchange_short,
                pos.entry_spread_pct, snapshot.price_spread_pct,
                total_latency_ms, f"{trader._trades_done}/{trader._max_trades}",
            )

        await _close_deal(trader, pos, snapshot, total_latency_ms)
        trader._current_deal_id = None
        trader._position = None
        trader._last_logged_skip_key = ""

        if trader._trades_done >= trader._max_trades:
            trader._state = TradeState.EXHAUSTED
            logger.info("MAX TRADES REACHED — switching to read-only mode")
        else:
            resolved = trader._resolve(pos.symbol)
            delay = max(0.0, float(resolved["post_trade_delay_sec"] or 0.0))
            trader._next_trade_allowed_ts = time.time() + delay if delay > 0 else 0.0
            trader._state = TradeState.IDLE
    except Exception as exc:
        logger.critical("CRITICAL UNHANDLED TRADE CLOSE ERROR: {}", exc)
        if trader._notifier:
            trader._notifier.notify_trade_critical_error(pos.symbol, "CLOSE", str(exc))
        trader._state = TradeState.OPEN


async def _close_deal(
    trader: TraderWorkflowPort, pos, snapshot, total_latency_ms: float,
) -> None:
    if not trader._storage or trader._current_deal_id is None:
        return
    try:
        await trader._storage.close_deal(
            trader._current_deal_id,
            {
                "closed_at": pos.closed_at,
                "close_spread_pct": snapshot.price_spread_pct,
                **pos.close_long.to_deal_fields("close_long"),
                **pos.close_short.to_deal_fields("close_short"),
                "close_latency_ms": total_latency_ms,
            },
        )
        logger.info("Deal id={} closed in DB", trader._current_deal_id)
    except Exception as exc:
        logger.error("Failed to close deal in DB: {}", exc)
