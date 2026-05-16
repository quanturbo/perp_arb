from __future__ import annotations

import asyncio
import time
from typing import Any

from loguru import logger

from src.domain.models import OrderResult


async def resolve_order_result(
    *,
    exchange: Any,
    exchange_id: str,
    symbol: str,
    side: str,
    price: float,
    order: dict,
    contract_size: float,
    amount: float,
    actual_base_qty: float,
    ack_latency_ms: float,
    order_start: float,
) -> OrderResult:
    fill_price, filled_contracts, order_id = await _resolve_fill(
        exchange=exchange,
        exchange_id=exchange_id,
        symbol=symbol,
        price=price,
        order=order,
        contract_size=contract_size,
    )
    return _order_result(
        exchange_id=exchange_id,
        symbol=symbol,
        side=side,
        order_id=order_id,
        fill_price=fill_price,
        filled_contracts=filled_contracts,
        contract_size=contract_size,
        amount=amount,
        actual_base_qty=actual_base_qty,
        ack_latency_ms=ack_latency_ms,
        order_start=order_start,
    )


async def _resolve_fill(
    *,
    exchange: Any,
    exchange_id: str,
    symbol: str,
    price: float,
    order: dict,
    contract_size: float,
) -> tuple[float, float, str]:
    order_id = str(order.get("id", ""))
    fill_price = float(order.get("average") or 0)
    filled_contracts = float(order.get("filled") or 0)
    status = order.get("status", "")
    fetch_params = {"acknowledged": True} if exchange_id == "bybit" else {}

    if (not fill_price or not filled_contracts) and order_id:
        fill_price, filled_contracts, status = await _poll_order_fill(
            exchange,
            exchange_id,
            symbol,
            order_id,
            fetch_params,
            fill_price,
            filled_contracts,
            status,
        )
    if status in ("open", "new", "partially_filled"):
        fill_price, filled_contracts = await _cancel_hanging_order(
            exchange,
            exchange_id,
            symbol,
            order_id,
            fetch_params,
            fill_price,
            filled_contracts,
        )
    if not fill_price and filled_contracts > 0:
        fill_price = _fallback_fill_price(order, price, filled_contracts, contract_size)
    return fill_price, filled_contracts, order_id


async def _poll_order_fill(
    exchange: Any,
    exchange_id: str,
    symbol: str,
    order_id: str,
    fetch_params: dict,
    fill_price: float,
    filled_contracts: float,
    status: str,
) -> tuple[float, float, str]:
    for _ in range(20):
        await asyncio.sleep(0.25)
        try:
            fetched = await exchange.fetch_order(order_id, symbol, params=fetch_params)
            fill_price = float(fetched.get("average") or 0)
            filled_contracts = float(fetched.get("filled") or 0)
            status = fetched.get("status", "")
            if fill_price and filled_contracts:
                break
            if status in ("canceled", "closed"):
                break
        except Exception as poll_err:
            logger.warning("ORDER POLL {} id={}: {}", exchange_id, order_id, poll_err)
    return fill_price, filled_contracts, status


async def _cancel_hanging_order(
    exchange: Any,
    exchange_id: str,
    symbol: str,
    order_id: str,
    fetch_params: dict,
    fill_price: float,
    filled_contracts: float,
) -> tuple[float, float]:
    logger.warning("ORDER {} id={} still open — CANCELLING...", exchange_id, order_id)
    try:
        await exchange.cancel_order(order_id, symbol)
        await asyncio.sleep(0.1)
        fetched = await exchange.fetch_order(order_id, symbol, params=fetch_params)
        fill_price = float(fetched.get("average") or 0)
        filled_contracts = float(fetched.get("filled") or 0)
    except Exception as exc:
        logger.error("Failed to cancel hang {}: {}", order_id, exc)
    return fill_price, filled_contracts


def _fallback_fill_price(
    order: dict,
    price: float,
    filled_contracts: float,
    contract_size: float,
) -> float:
    if order.get("cost") and float(order["cost"]) > 0:
        cost = float(order["cost"])
        return cost / (filled_contracts * contract_size)
    if order.get("info"):
        info = order["info"]
        if "priceAvg" in info and float(info["priceAvg"] or 0) > 0:
            return float(info["priceAvg"])
        if "fill_price" in info and float(info["fill_price"] or 0) > 0:
            return float(info["fill_price"])
    return float(order.get("price") or price)


def _order_result(
    *,
    exchange_id: str,
    symbol: str,
    side: str,
    order_id: str,
    fill_price: float,
    filled_contracts: float,
    contract_size: float,
    amount: float,
    actual_base_qty: float,
    ack_latency_ms: float,
    order_start: float,
) -> OrderResult:
    filled_base_qty = filled_contracts * contract_size
    if filled_contracts < amount:
        msg = (
            f"Order {side} {symbol} partially or 0-filled "
            f"(got {filled_base_qty}/{actual_base_qty} base tokens)"
        )
        logger.critical("{} on {}", msg, exchange_id)
    final_latency_ms = (time.monotonic() - order_start) * 1000
    logger.info(
        "ORDER FILLED {} id={} avg_price={:.6f} filled={:.6f} base tokens ack={:.0f}ms final={:.0f}ms",
        exchange_id,
        order_id,
        fill_price,
        filled_base_qty,
        ack_latency_ms,
        final_latency_ms,
    )
    return OrderResult(
        exchange_id=exchange_id,
        symbol=symbol,
        side=side,
        fill_price=fill_price,
        filled_amount=filled_base_qty,
        order_id=order_id,
        timestamp=time.time(),
        ack_latency_ms=ack_latency_ms,
        final_latency_ms=final_latency_ms,
    )
