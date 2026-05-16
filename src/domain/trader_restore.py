from __future__ import annotations

from loguru import logger

from src.domain.trader_ports import TraderRestorePort
from src.domain.trade_position import Position
from src.domain.trade_state import TradeState


async def restore_from_db(trader: TraderRestorePort) -> None:
    if not trader._storage:
        return
    try:
        deals = await trader._storage.get_open_deals()
    except Exception as exc:
        logger.error("Failed to query open deals: {} — starting fresh", exc)
        return
    if not deals:
        return
    deal = deals[0]
    try:
        position = Position.from_deal_row(deal)
    except Exception as exc:
        logger.error(
            "Failed to restore deal id={}: {} — marking cancelled",
            deal.get("id"),
            exc,
        )
        if trader._storage and deal.get("id"):
            await trader._storage.cancel_deal(deal["id"], reason="restore_failed")
        return

    long_fill = position.open_long.filled_amount if position.open_long else 0.0
    short_fill = position.open_short.filled_amount if position.open_short else 0.0
    if long_fill <= 0 and short_fill <= 0:
        logger.error(
            "Deal id={} has zero fills on both legs — cancelling ghost deal",
            deal["id"],
        )
        if trader._storage:
            await trader._storage.cancel_deal(deal["id"], reason="ghost_zero_fills")
        return

    trader._position = position
    trader._current_deal_id = deal["id"]
    trader._state = TradeState.OPEN
    logger.info(
        "TRADER RESTORED open position: {} long={} short={} entry_spread={:.4f}% filled_qty={:.4f}",
        deal["symbol"],
        deal["exchange_long"],
        deal["exchange_short"],
        deal["entry_spread_pct"],
        long_fill,
    )
