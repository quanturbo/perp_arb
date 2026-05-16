"""Static UACryptoInvest ids and chart naming helpers."""

from __future__ import annotations

from dataclasses import dataclass


TOKEN_TYPE_FUTURES = 0
STREAM_TYPE_FUNDING = 0
STREAM_TYPE_OPEN_INTEREST = 1
STREAM_TYPE_DEPTH = 2


@dataclass(frozen=True)
class UACIExchange:
    source_id: int
    canonical: str
    chart_name: str
    aliases: tuple[str, ...]


_EXCHANGES: tuple[UACIExchange, ...] = (
    UACIExchange(0, "mexc", "Mexc", ("mexc",)),
    UACIExchange(5, "whitebit", "Whitebit", ("whitebit",)),
    UACIExchange(8, "gate", "Gate", ("gate", "gateio")),
    UACIExchange(10, "bybit", "Bybit", ("bybit",)),
    UACIExchange(11, "kucoin", "Kucoin", ("kucoin",)),
    UACIExchange(12, "bingx", "Bingx", ("bingx",)),
    UACIExchange(13, "bitget", "Bitget", ("bitget",)),
    UACIExchange(14, "okx", "Okx", ("okx", "okex")),
    UACIExchange(16, "hyperliquid", "Hyperliquid", ("hyperliquid",)),
    UACIExchange(17, "binance", "Binance", ("binance", "binanceusdm")),
    UACIExchange(18, "aster", "Aster", ("aster",)),
    UACIExchange(21, "htx", "HTX", ("htx", "huobi")),
    UACIExchange(28, "bybitfi", "BybitFi", ("bybitfi", "bybit-fi")),
    UACIExchange(29, "gatefi", "GateFi", ("gatefi", "gate-fi")),
)

_BY_ALIAS: dict[str, UACIExchange] = {
    alias: exchange
    for exchange in _EXCHANGES
    for alias in (exchange.canonical, *exchange.aliases)
}
_BY_ID: dict[int, UACIExchange] = {exchange.source_id: exchange for exchange in _EXCHANGES}

ALL_EXCHANGES: tuple[UACIExchange, ...] = _EXCHANGES


def canonical_exchange(value: str) -> str:
    key = (value or "").strip().lower().replace("_", "-")
    exchange = _BY_ALIAS.get(key)
    if exchange is None:
        raise ValueError(f"unsupported UACryptoInvest exchange: {value!r}")
    return exchange.canonical


def exchange_id_for(value: str) -> int:
    return _BY_ALIAS[canonical_exchange(value)].source_id


def exchange_name_for_id(source_id: int) -> str:
    exchange = _BY_ID.get(int(source_id))
    if exchange is None:
        raise ValueError(f"unsupported UACryptoInvest exchange id: {source_id!r}")
    return exchange.chart_name


def chart_exchange_name(value: str) -> str:
    return _BY_ALIAS[canonical_exchange(value)].chart_name