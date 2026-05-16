from __future__ import annotations

import time
from typing import Any, Callable, Literal, cast

from loguru import logger

from src.adapters.exchange.direct_order import (
    DirectOrderClient,
    DirectOrderUnavailable,
    DirectOrderUnknownState,
)
from src.adapters.exchange import order_fill
from src.domain.models import OrderResult
from src.settings import MAX_LIMIT_PRICE_SLIPPAGE_PCT


def is_leverage_limit_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "-2027" in text
        or "leverage exceeds the maximum" in text
        or "maximum allowable position at current leverage" in text
    )


def normalize_limit_price_slippage_pct(value: float | int | str) -> float:
    return min(MAX_LIMIT_PRICE_SLIPPAGE_PCT, max(0.0, float(value)))


async def setup_margin(
    exchange: Any,
    exchange_id: str,
    symbols: list[str],
    leverage: int,
    margin_mode: str,
) -> None:
    for symbol in symbols:
        if margin_mode:
            await _set_margin_mode(exchange, exchange_id, symbol, margin_mode)
        if leverage > 0:
            await _set_startup_leverage(exchange, exchange_id, symbol, leverage)


async def set_safe_leverage_for_order(
    exchange: Any,
    exchange_id: str,
    symbol: str,
    side: str,
    exc: Exception,
) -> bool:
    if not is_leverage_limit_error(exc):
        return False
    try:
        await exchange.set_leverage(1, symbol)
        logger.warning(
            "TRADE ISSUE | level=WARNING | type=LEVERAGE_FALLBACK | phase=ORDER | "
            "exchange={} | symbol={} | side={} | reason=order rejected by leverage/position limit; set 1x and retrying ({})",
            exchange_id,
            symbol,
            side.upper(),
            _one_line(exc),
        )
        return True
    except Exception as fallback_err:
        logger.error(
            "TRADE ISSUE | level=ERROR | type=LEVERAGE_FALLBACK_FAILED | phase=ORDER | "
            "exchange={} | symbol={} | side={} | reason={}",
            exchange_id,
            symbol,
            side.upper(),
            _one_line(fallback_err),
        )
        return False


async def create_order_with_leverage_recovery(
    *,
    exchange: Any,
    exchange_id: str,
    direct_order_client: DirectOrderClient | None,
    symbol: str,
    order_type: str,
    side: str,
    amount: float,
    price: float | None,
    params: dict,
) -> dict:
    async def create() -> dict:
        if direct_order_client is not None:
            try:
                return await _create_direct_order(
                    exchange=exchange,
                    direct_order_client=direct_order_client,
                    symbol=symbol,
                    order_type=order_type,
                    side=side,
                    amount=amount,
                    price=price,
                    params=params,
                )
            except DirectOrderUnavailable as exc:
                logger.warning(
                    "Direct WS order unavailable on {} ({}); falling back to CCXT",
                    exchange_id,
                    exc,
                )
        return await _create_ccxt_order(
            exchange=exchange,
            symbol=symbol,
            order_type=order_type,
            side=side,
            amount=amount,
            price=price,
            params=params,
        )

    try:
        return await create()
    except DirectOrderUnknownState:
        logger.error(
            "TRADE ISSUE | level=ERROR | type=DIRECT_ORDER_UNKNOWN_STATE | phase=ORDER | "
            "exchange={} | symbol={} | side={} | reason=direct order state unknown; not retrying to avoid duplicate order",
            exchange_id,
            symbol,
            side.upper(),
        )
        raise
    except Exception as exc:
        if await set_safe_leverage_for_order(exchange, exchange_id, symbol, side, exc):
            return await create()
        raise


async def place_market_order(
    *,
    exchange: Any,
    exchange_id: str,
    direct_order_client: DirectOrderClient | None,
    get_time_in_force: Callable[[], str],
    limit_price_slippage: float,
    symbol: str,
    side: str,
    base_qty: float,
    price: float = 0.0,
    order_type: str = "market",
    is_close: bool = False,
    time_in_force: str | None = None,
    limit_price_slippage_pct: float | None = None,
) -> OrderResult:
    assert side in ("buy", "sell"), f"Invalid side: {side}"
    contract_size = _contract_size(exchange, symbol)
    amount, actual_base_qty = _precise_amount(
        exchange,
        exchange_id,
        symbol,
        base_qty,
        contract_size,
    )
    logger.info(
        "ORDER {} {} {} base_qty={:.6f} ({} contracts) on {}",
        side.upper(),
        symbol,
        order_type.upper(),
        actual_base_qty,
        amount,
        exchange_id,
    )

    params: dict = {"reduceOnly": True} if is_close else {}
    order_start = time.monotonic()
    order = await _submit_order(
        exchange=exchange,
        exchange_id=exchange_id,
        direct_order_client=direct_order_client,
        get_time_in_force=get_time_in_force,
        limit_price_slippage=limit_price_slippage,
        symbol=symbol,
        side=side,
        amount=amount,
        price=price,
        order_type=order_type,
        time_in_force=time_in_force,
        limit_price_slippage_pct=limit_price_slippage_pct,
        params=params,
    )
    ack_latency_ms = (time.monotonic() - order_start) * 1000
    return await order_fill.resolve_order_result(
        exchange=exchange,
        exchange_id=exchange_id,
        symbol=symbol,
        side=side,
        price=price,
        order=order,
        contract_size=contract_size,
        amount=amount,
        actual_base_qty=actual_base_qty,
        ack_latency_ms=ack_latency_ms,
        order_start=order_start,
    )


async def _set_margin_mode(
    exchange: Any,
    exchange_id: str,
    symbol: str,
    margin_mode: str,
) -> None:
    try:
        await exchange.set_margin_mode(margin_mode, symbol)
        logger.info("Set margin mode {} for {} on {}", margin_mode, symbol, exchange_id)
    except Exception as exc:
        logger.warning(
            "Cannot set margin mode {} for {} on {}: {}",
            margin_mode,
            symbol,
            exchange_id,
            exc,
        )


async def _set_startup_leverage(
    exchange: Any,
    exchange_id: str,
    symbol: str,
    leverage: int,
) -> None:
    try:
        await exchange.set_leverage(leverage, symbol)
        logger.info("Set leverage {}x for {} on {}", leverage, symbol, exchange_id)
    except Exception as exc:
        logger.warning(
            "Cannot set leverage {}x for {} on {}: {}",
            leverage,
            symbol,
            exchange_id,
            exc,
        )
        await _fallback_startup_leverage(exchange, exchange_id, symbol, leverage, exc)


async def _fallback_startup_leverage(
    exchange: Any,
    exchange_id: str,
    symbol: str,
    leverage: int,
    exc: Exception,
) -> None:
    if leverage == 1 or not is_leverage_limit_error(exc):
        return
    try:
        await exchange.set_leverage(1, symbol)
        logger.warning(
            "TRADE ISSUE | level=WARNING | type=LEVERAGE_FALLBACK | phase=SETUP | "
            "exchange={} | symbol={} | side=- | reason=configured leverage {}x rejected; set 1x ({})",
            exchange_id,
            symbol,
            leverage,
            _one_line(exc),
        )
    except Exception as fallback_err:
        logger.error(
            "TRADE ISSUE | level=ERROR | type=LEVERAGE_FALLBACK_FAILED | phase=SETUP | "
            "exchange={} | symbol={} | side=- | reason={}",
            exchange_id,
            symbol,
            _one_line(fallback_err),
        )


async def _create_direct_order(
    *,
    exchange: Any,
    direct_order_client: DirectOrderClient,
    symbol: str,
    order_type: str,
    side: str,
    amount: float,
    price: float | None,
    params: dict,
) -> dict:
    market = exchange.market(symbol)
    native_symbol = str(market.get("id") or symbol)
    return await direct_order_client.place_order(
        native_symbol=native_symbol,
        side=side,
        order_type=order_type,
        quantity=str(amount),
        price=str(price) if price is not None else None,
        time_in_force=params.get("timeInForce"),
        reduce_only=bool(params.get("reduceOnly")),
    )


async def _create_ccxt_order(
    *,
    exchange: Any,
    symbol: str,
    order_type: str,
    side: str,
    amount: float,
    price: float | None,
    params: dict,
) -> dict:
    order_side = cast(Literal["buy", "sell"], side)
    if order_type == "limit":
        return await exchange.create_order(
            symbol=symbol,
            type="limit",
            side=order_side,
            amount=amount,
            price=price,
            params=params,
        )
    return await exchange.create_order(
        symbol=symbol,
        type="market",
        side=order_side,
        amount=amount,
        params=params,
    )


def _contract_size(exchange: Any, symbol: str) -> float:
    return float(exchange.market(symbol).get("contractSize", 1) or 1)


def _precise_amount(
    exchange: Any,
    exchange_id: str,
    symbol: str,
    base_qty: float,
    contract_size: float,
) -> tuple[float, float]:
    raw_amount = base_qty / contract_size
    precise_amount = exchange.amount_to_precision(symbol, raw_amount)
    if precise_amount is None:
        raise ValueError(f"Exchange {exchange_id} returned no amount precision for {symbol}")
    amount = float(precise_amount)
    return amount, amount * contract_size


async def _submit_order(
    *,
    exchange: Any,
    exchange_id: str,
    direct_order_client: DirectOrderClient | None,
    get_time_in_force: Callable[[], str],
    limit_price_slippage: float,
    symbol: str,
    side: str,
    amount: float,
    price: float,
    order_type: str,
    time_in_force: str | None,
    limit_price_slippage_pct: float | None,
    params: dict,
) -> dict:
    if order_type != "limit":
        return await create_order_with_leverage_recovery(
            exchange=exchange,
            exchange_id=exchange_id,
            direct_order_client=direct_order_client,
            symbol=symbol,
            order_type="market",
            side=side,
            amount=amount,
            price=None,
            params=params,
        )

    limit_price = _limit_price(
        exchange=exchange,
        exchange_id=exchange_id,
        get_time_in_force=get_time_in_force,
        limit_price_slippage=limit_price_slippage,
        symbol=symbol,
        side=side,
        price=price,
        time_in_force=time_in_force,
        limit_price_slippage_pct=limit_price_slippage_pct,
        params=params,
    )
    return await create_order_with_leverage_recovery(
        exchange=exchange,
        exchange_id=exchange_id,
        direct_order_client=direct_order_client,
        symbol=symbol,
        order_type="limit",
        side=side,
        amount=amount,
        price=limit_price,
        params=params,
    )


def _limit_price(
    *,
    exchange: Any,
    exchange_id: str,
    get_time_in_force: Callable[[], str],
    limit_price_slippage: float,
    symbol: str,
    side: str,
    price: float,
    time_in_force: str | None,
    limit_price_slippage_pct: float | None,
    params: dict,
) -> float:
    slippage_pct = (
        limit_price_slippage
        if limit_price_slippage_pct is None
        else normalize_limit_price_slippage_pct(limit_price_slippage_pct) / 100.0
    )
    slippage_mult = 1.0 + slippage_pct if side == "buy" else max(0.0, 1.0 - slippage_pct)
    precise_price = exchange.price_to_precision(symbol, price * slippage_mult)
    if precise_price is None:
        raise ValueError(f"Exchange {exchange_id} returned no price precision for {symbol}")
    params["timeInForce"] = (time_in_force or get_time_in_force()).upper()
    return float(precise_price)


def _one_line(exc: Exception) -> str:
    return " ".join(str(exc).split())
