"""Raw WebSocket ticker stream adapters — one file per exchange.

Public API:
    RawTick, RawTickerStream  — data + port interface
    create_raw_stream()       — factory (returns adapter or None → CCXT fallback)
    BaseExchangeWS            — template base for custom adapters
    Individual adapters       — BinanceWS, GateioWS, BitgetWS, etc.
"""

from src.adapters.ws.base import (
    BaseExchangeWS,
    OnRawTick,
    RawTick,
    RawTickerStream,
)
from src.adapters.ws.factory import create_raw_stream

# Individual adapters (re-export for tests that import by name)
from src.adapters.ws.binance import BinanceWS
from src.adapters.ws.bingx import BingxWS
from src.adapters.ws.bitget import BitgetWS
from src.adapters.ws.bybit import BybitWS
from src.adapters.ws.gateio import GateioWS
from src.adapters.ws.kucoin import KucoinWS
from src.adapters.ws.mexc import MexcWS
from src.adapters.ws.okx import OkxWS

__all__ = [
    "BaseExchangeWS",
    "BinanceWS",
    "BingxWS",
    "BitgetWS",
    "BybitWS",
    "GateioWS",
    "KucoinWS",
    "MexcWS",
    "OkxWS",
    "OnRawTick",
    "RawTick",
    "RawTickerStream",
    "create_raw_stream",
]
