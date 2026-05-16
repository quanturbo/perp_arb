from __future__ import annotations

import time
from typing import Callable, Awaitable

import aiosqlite
from loguru import logger

from src.adapters.storage_schema import DEAL_COLUMNS

QueryRows = Callable[[str, tuple, list[str]], Awaitable[list[dict]]]


async def save_deal(
    db: aiosqlite.Connection | None,
    deal: dict,
) -> int | None:
    if not db:
        return None
    cursor = await db.execute(
        """INSERT INTO deals
           (symbol, exchange_long, exchange_short, opened_at,
            entry_spread_pct, amount_usdt,
            open_long_quoted, open_long_fill, open_long_filled_qty, open_long_slip_pct,
            open_long_latency_ms, open_long_ack_latency_ms, open_long_decision_to_order_ms, open_long_tick_age_ms, open_long_quote_age_ms,
            open_short_quoted, open_short_fill, open_short_filled_qty, open_short_slip_pct,
            open_short_latency_ms, open_short_ack_latency_ms, open_short_decision_to_order_ms, open_short_tick_age_ms, open_short_quote_age_ms,
            total_latency_ms, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')""",
        (
            deal["symbol"],
            deal["exchange_long"],
            deal["exchange_short"],
            deal["opened_at"],
            deal["entry_spread_pct"],
            deal["amount_usdt"],
            deal.get("open_long_quoted", 0),
            deal.get("open_long_fill", 0),
            deal.get("open_long_filled_qty", 0),
            deal.get("open_long_slip_pct", 0),
            deal.get("open_long_latency_ms", 0),
            deal.get("open_long_ack_latency_ms", 0),
            deal.get("open_long_decision_to_order_ms", 0),
            deal.get("open_long_tick_age_ms", 0),
            deal.get("open_long_quote_age_ms", 0),
            deal.get("open_short_quoted", 0),
            deal.get("open_short_fill", 0),
            deal.get("open_short_filled_qty", 0),
            deal.get("open_short_slip_pct", 0),
            deal.get("open_short_latency_ms", 0),
            deal.get("open_short_ack_latency_ms", 0),
            deal.get("open_short_decision_to_order_ms", 0),
            deal.get("open_short_tick_age_ms", 0),
            deal.get("open_short_quote_age_ms", 0),
            deal.get("total_latency_ms", 0),
        ),
    )
    await db.commit()
    return cursor.lastrowid


async def close_deal(
    db: aiosqlite.Connection | None,
    deal_id: int,
    close_data: dict,
) -> None:
    if not db:
        return
    await db.execute(
        """UPDATE deals SET
           closed_at = ?, close_spread_pct = ?,
           close_long_quoted = ?, close_long_fill = ?, close_long_filled_qty = ?,
           close_long_slip_pct = ?, close_long_latency_ms = ?, close_long_ack_latency_ms = ?,
           close_long_decision_to_order_ms = ?, close_long_tick_age_ms = ?, close_long_quote_age_ms = ?,
           close_short_quoted = ?, close_short_fill = ?, close_short_filled_qty = ?,
           close_short_slip_pct = ?, close_short_latency_ms = ?, close_short_ack_latency_ms = ?,
           close_short_decision_to_order_ms = ?, close_short_tick_age_ms = ?, close_short_quote_age_ms = ?,
           close_latency_ms = ?,
           status = 'closed'
           WHERE id = ?""",
        (
            close_data["closed_at"],
            close_data["close_spread_pct"],
            close_data.get("close_long_quoted", 0),
            close_data.get("close_long_fill", 0),
            close_data.get("close_long_filled_qty", 0),
            close_data.get("close_long_slip_pct", 0),
            close_data.get("close_long_latency_ms", 0),
            close_data.get("close_long_ack_latency_ms", 0),
            close_data.get("close_long_decision_to_order_ms", 0),
            close_data.get("close_long_tick_age_ms", 0),
            close_data.get("close_long_quote_age_ms", 0),
            close_data.get("close_short_quoted", 0),
            close_data.get("close_short_fill", 0),
            close_data.get("close_short_filled_qty", 0),
            close_data.get("close_short_slip_pct", 0),
            close_data.get("close_short_latency_ms", 0),
            close_data.get("close_short_ack_latency_ms", 0),
            close_data.get("close_short_decision_to_order_ms", 0),
            close_data.get("close_short_tick_age_ms", 0),
            close_data.get("close_short_quote_age_ms", 0),
            close_data.get("close_latency_ms", 0),
            deal_id,
        ),
    )
    await db.commit()


async def force_close_deal(
    db: aiosqlite.Connection | None,
    deal_id: int,
) -> None:
    if not db:
        return
    await db.execute(
        "UPDATE deals SET status = 'closed', closed_at = ? WHERE id = ? AND status = 'open'",
        (time.time(), deal_id),
    )
    await db.commit()
    logger.info("Deal id={} force-closed (manual)", deal_id)


async def cancel_deal(
    db: aiosqlite.Connection | None,
    deal_id: int,
    reason: str = "orphaned",
) -> None:
    if not db:
        return
    await db.execute(
        "UPDATE deals SET status = 'cancelled', closed_at = ?, close_spread_pct = 0 WHERE id = ?",
        (time.time(), deal_id),
    )
    await db.commit()
    logger.warning("Deal id={} marked as cancelled ({})", deal_id, reason)


async def get_deals(
    query_rows: QueryRows,
    symbol: str,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    select = ", ".join(DEAL_COLUMNS)
    return await query_rows(
        f"SELECT {select} FROM deals WHERE symbol = ? ORDER BY opened_at DESC LIMIT ? OFFSET ?",
        (symbol, limit, offset),
        DEAL_COLUMNS,
    )


async def get_deals_count(query_rows: QueryRows, symbol: str) -> int:
    rows = await query_rows(
        "SELECT COUNT(*) FROM deals WHERE symbol = ?",
        (symbol,),
        ["cnt"],
    )
    return rows[0]["cnt"] if rows else 0


async def get_deals_cumulative(
    query_rows: QueryRows,
    symbol: str,
    deal_ids: set[int] | None = None,
) -> dict:
    cols = [
        "id",
        "status",
        "amount_usdt",
        "open_long_fill",
        "open_long_filled_qty",
        "open_short_fill",
        "open_short_filled_qty",
        "close_long_fill",
        "close_short_fill",
    ]
    select = ", ".join(cols)
    all_deals = await query_rows(
        f"SELECT {select} FROM deals WHERE symbol = ? ORDER BY opened_at ASC",
        (symbol,),
        cols,
    )
    return _cumulative_deal_rows(all_deals, deal_ids)


async def get_open_deals(query_rows: QueryRows) -> list[dict]:
    keys = [
        "id",
        "symbol",
        "exchange_long",
        "exchange_short",
        "opened_at",
        "entry_spread_pct",
        "amount_usdt",
        "open_long_quoted",
        "open_long_fill",
        "open_long_filled_qty",
        "open_long_slip_pct",
        "open_long_latency_ms",
        "open_long_ack_latency_ms",
        "open_long_decision_to_order_ms",
        "open_long_tick_age_ms",
        "open_long_quote_age_ms",
        "open_short_quoted",
        "open_short_fill",
        "open_short_filled_qty",
        "open_short_slip_pct",
        "open_short_latency_ms",
        "open_short_ack_latency_ms",
        "open_short_decision_to_order_ms",
        "open_short_tick_age_ms",
        "open_short_quote_age_ms",
        "total_latency_ms",
    ]
    return await query_rows(
        """SELECT id, symbol, exchange_long, exchange_short,
                  opened_at, entry_spread_pct, amount_usdt,
                  open_long_quoted, open_long_fill, open_long_filled_qty,
                 open_long_slip_pct, open_long_latency_ms, open_long_ack_latency_ms,
                 open_long_decision_to_order_ms,
                 open_long_tick_age_ms, open_long_quote_age_ms,
                  open_short_quoted, open_short_fill, open_short_filled_qty,
                 open_short_slip_pct, open_short_latency_ms, open_short_ack_latency_ms,
                 open_short_decision_to_order_ms,
                 open_short_tick_age_ms, open_short_quote_age_ms,
                  total_latency_ms
           FROM deals WHERE status = 'open' ORDER BY opened_at ASC""",
        (),
        keys,
    )


def _cumulative_deal_rows(
    all_deals: list[dict],
    deal_ids: set[int] | None,
) -> dict:
    result: dict = {}
    visible_ids = set(deal_ids) if deal_ids is not None else None
    cum_pnl = 0.0
    cum_capital = 0.0

    for deal in all_deals:
        pnl_usd, pnl_pct = _deal_pnl(deal)
        if pnl_usd is not None:
            cum_pnl += pnl_usd
            cum_capital += deal["amount_usdt"] * 2
        cum_pct = (cum_pnl / cum_capital * 100) if cum_capital > 0 else 0.0

        if visible_ids is None or deal["id"] in visible_ids:
            result[str(deal["id"])] = {
                "cum_pnl": round(cum_pnl, 6),
                "cum_pct": round(cum_pct, 6),
                "pnl_usd": round(pnl_usd, 6) if pnl_usd is not None else None,
                "pnl_pct": round(pnl_pct, 6) if pnl_pct is not None else None,
            }

    result["_summary"] = {
        "total_pnl": round(cum_pnl, 6),
        "total_pct": round(
            (cum_pnl / cum_capital * 100) if cum_capital > 0 else 0.0,
            6,
        ),
        "total_count": len(all_deals),
        "closed_count": sum(1 for deal in all_deals if deal["status"] != "open"),
    }
    return result


def _deal_pnl(deal: dict) -> tuple[float | None, float | None]:
    if deal["status"] == "open":
        return None, None
    open_long_fill = deal["open_long_fill"]
    open_short_fill = deal["open_short_fill"]
    close_long_fill = deal["close_long_fill"]
    close_short_fill = deal["close_short_fill"]
    has_close_long = close_long_fill and close_long_fill > 0
    has_close_short = close_short_fill and close_short_fill > 0
    if not (open_long_fill and open_short_fill and has_close_long and has_close_short):
        return None, None

    long_qty = deal["open_long_filled_qty"] or (deal["amount_usdt"] / open_long_fill)
    short_qty = deal["open_short_filled_qty"] or (deal["amount_usdt"] / open_short_fill)
    long_pnl = (close_long_fill - open_long_fill) * long_qty
    short_pnl = (open_short_fill - close_short_fill) * short_qty
    pnl_usd = long_pnl + short_pnl
    capital = (long_qty * open_long_fill) + (short_qty * open_short_fill)
    pnl_pct = (pnl_usd / capital * 100) if capital > 0 else 0.0
    return pnl_usd, pnl_pct
