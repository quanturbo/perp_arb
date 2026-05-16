"""Direct WebSocket order placement facade and support registry."""

from __future__ import annotations

from src.adapters.exchange.direct_order_base import (
    DirectOrderClient,
    DirectOrderRejected,
    DirectOrderSupport,
    DirectOrderUnavailable,
    DirectOrderUnknownState,
    JsonWsOrderClient,
)
from src.adapters.exchange.direct_order_binance import BinanceFuturesWsOrderClient
from src.adapters.exchange.direct_order_bybit import BybitWsOrderClient
from src.adapters.exchange.direct_order_gateio import GateioFuturesWsOrderClient
from src.adapters.exchange.direct_order_okx import OkxWsOrderClient

_UNSUPPORTED = DirectOrderSupport(
    exchange_id="",
    supported=False,
    route="ccxt",
    label="CCXT",
    reason="No confirmed direct WebSocket order-submit route in this bot.",
)

DIRECT_ORDER_SUPPORT: dict[str, DirectOrderSupport] = {
    "binanceusdm": DirectOrderSupport(
        exchange_id="binanceusdm",
        supported=True,
        route="ws",
        label="WS",
        reason="Binance USD-M WebSocket API supports order.place.",
    ),
    "bybit": DirectOrderSupport(
        exchange_id="bybit",
        supported=True,
        route="ws",
        label="WS",
        reason="Bybit V5 WebSocket trade API supports order.create.",
    ),
    "gateio": DirectOrderSupport(
        exchange_id="gateio",
        supported=True,
        route="ws",
        label="WS",
        reason="Gate futures WebSocket API supports futures.order_place with integer contract size.",
    ),
    "okx": DirectOrderSupport(
        exchange_id="okx",
        supported=True,
        route="ws",
        label="WS",
        reason="OKX V5 private WebSocket API supports op=order.",
        requires_password=True,
    ),
    "aster": DirectOrderSupport(
        exchange_id="aster",
        supported=False,
        route="ccxt",
        label="CCXT",
        reason="Aster probes did not confirm a Binance-compatible private WS order API.",
    ),
    "bingx": DirectOrderSupport(
        exchange_id="bingx",
        supported=False,
        route="ccxt",
        label="CCXT",
        reason="BingX docs confirm REST order placement and private update streams, not WS submit.",
    ),
    "bitget": DirectOrderSupport(
        exchange_id="bitget",
        supported=False,
        route="ccxt",
        label="CCXT",
        reason="Bitget WS place-order is permission-gated and documents size as base coin, not this bot's native contract amount.",
    ),
    "mexc": DirectOrderSupport(
        exchange_id="mexc",
        supported=False,
        route="ccxt",
        label="CCXT",
        reason="MEXC futures WS docs confirm private order updates, not a WS order-submit command.",
    ),
    "htx": DirectOrderSupport(
        exchange_id="htx",
        supported=False,
        route="ccxt",
        label="CCXT",
        reason="HTX direct futures WS order-submit route was not confirmed for this adapter.",
    ),
}


def get_direct_order_support(exchange_id: str) -> DirectOrderSupport:
    return DIRECT_ORDER_SUPPORT.get(
        exchange_id,
        DirectOrderSupport(
            exchange_id=exchange_id,
            supported=False,
            route=_UNSUPPORTED.route,
            label=_UNSUPPORTED.label,
            reason=_UNSUPPORTED.reason,
        ),
    )


def direct_order_support_snapshot(exchange_id: str, *, enabled: bool = False) -> dict:
    return get_direct_order_support(exchange_id).to_dict(enabled=enabled)


def create_direct_order_client(config) -> DirectOrderClient | None:
    if not config.extra.get("direct_order_ws"):
        return None
    support = get_direct_order_support(config.id)
    if not support.supported or not config.api_key or not config.secret:
        return None

    endpoint = config.extra.get("direct_order_ws_endpoint")
    if config.id == "binanceusdm":
        return BinanceFuturesWsOrderClient(
            api_key=config.api_key,
            secret=config.secret,
            endpoint=endpoint,
        )
    if config.id == "bybit":
        return BybitWsOrderClient(
            api_key=config.api_key,
            secret=config.secret,
            endpoint=endpoint,
            category=str(config.extra.get("direct_order_ws_category", "linear")),
        )
    if config.id == "gateio":
        return GateioFuturesWsOrderClient(
            api_key=config.api_key,
            secret=config.secret,
            endpoint=endpoint,
        )
    if config.id == "okx":
        if not config.password:
            return None
        return OkxWsOrderClient(
            api_key=config.api_key,
            secret=config.secret,
            passphrase=config.password,
            endpoint=endpoint,
            td_mode=str(config.extra.get("direct_order_ws_td_mode", "cross")),
        )
    return None


__all__ = [
    "BinanceFuturesWsOrderClient",
    "BybitWsOrderClient",
    "DirectOrderClient",
    "DirectOrderRejected",
    "DirectOrderSupport",
    "DirectOrderUnavailable",
    "DirectOrderUnknownState",
    "GateioFuturesWsOrderClient",
    "JsonWsOrderClient",
    "OkxWsOrderClient",
    "create_direct_order_client",
    "direct_order_support_snapshot",
    "get_direct_order_support",
]
