"""Factory: picks the right raw WS adapter for an exchange, or None → CCXT fallback."""

from __future__ import annotations

from typing import Any, Optional

from loguru import logger

from src.adapters.http import HttpClient
from src.adapters.ws.base import BaseExchangeWS, RawTickerStream
from src.adapters.ws.binance import BinanceWS
from src.adapters.ws.bingx import BingxWS
from src.adapters.ws.bitget import BitgetWS
from src.adapters.ws.bybit import BybitWS
from src.adapters.ws.gateio import GateioWS
from src.adapters.ws.kucoin import KucoinWS
from src.adapters.ws.mexc import MexcWS
from src.adapters.ws.okx import OkxWS

_BINANCE_WS_BASES: dict[str, str] = {
    "binanceusdm": "wss://fstream.binance.com",
    "binance": "wss://fstream.binance.com",
    "binancecoinm": "wss://dstream.binance.com",
}

_ADAPTERS: dict[str, type[BaseExchangeWS]] = {
    "gateio": GateioWS,
    "bitget": BitgetWS,
    "bybit": BybitWS,
    "okx": OkxWS,
    "bingx": BingxWS,
    "mexc": MexcWS,
}


def _detect_binance_ws_base(ccxt_exchange: Any) -> Optional[str]:
    """Try to extract WS base URL from a CCXT Binance-compatible exchange.

    Detects via two methods:
    1. MRO contains 'binance' (direct forks)
    2. WS URL contains 'fstream'/'dstream' pattern (rebranded forks like Aster)
    """
    try:
        bases = [b.__name__.lower() for b in type(ccxt_exchange).__mro__]
        is_binance_mro = any("binance" in b for b in bases)

        urls = getattr(ccxt_exchange, "urls", {})
        api = urls.get("api", {})
        ws = api.get("ws", {})

        def _find_fstream_url(obj: Any) -> Optional[str]:
            """Recursively find a wss:// URL with fstream/dstream pattern."""
            if isinstance(obj, str) and obj.startswith("wss://"):
                if "fstream" in obj or "dstream" in obj:
                    return obj.rsplit("/stream", 1)[0] if "/stream" in obj else obj
            elif isinstance(obj, dict):
                for val in obj.values():
                    found = _find_fstream_url(val)
                    if found:
                        return found
            return None

        # Method 1: MRO confirms Binance — use standard key lookup
        if is_binance_mro and isinstance(ws, dict):
            for key in ("public", "ws", "fapi", "fapiPublic"):
                val = ws.get(key, "")
                if isinstance(val, str) and val.startswith("wss://"):
                    return val.rsplit("/ws", 1)[0] if "/ws" in val else val

        # Method 2: Find fstream/dstream URL pattern (works for rebranded forks)
        fstream = _find_fstream_url(ws)
        if fstream:
            return fstream

        # Fallback: MRO match with any string WS URL
        if is_binance_mro:
            if isinstance(ws, str) and ws.startswith("wss://"):
                return ws.rsplit("/ws", 1)[0] if "/ws" in ws else ws
    except Exception:
        pass
    return None


def create_raw_stream(
    exchange_id: str,
    symbol_map: dict[str, str],
    ccxt_exchange: Any = None,
    http: HttpClient | None = None,
) -> Optional[RawTickerStream]:
    """Create a raw WS stream for the given exchange.

    Returns None if unsupported → caller should fall back to CCXT Pro.
    """
    if exchange_id in _BINANCE_WS_BASES:
        return BinanceWS(symbol_map, _BINANCE_WS_BASES[exchange_id], exchange_id=exchange_id)

    if exchange_id == "kucoinfutures":
        if http is None:
            raise RuntimeError("KucoinWS requires injected HttpClient")
        return KucoinWS(symbol_map, http=http, exchange_id=exchange_id)

    cls = _ADAPTERS.get(exchange_id)
    if cls:
        return cls(symbol_map, exchange_id=exchange_id)

    # Auto-detect Binance-compatible forks (e.g. aster)
    if ccxt_exchange:
        ws_base = _detect_binance_ws_base(ccxt_exchange)
        if ws_base:
            logger.info(
                "Auto-detected Binance-compatible WS for {}: {}",
                exchange_id,
                ws_base,
            )
            return BinanceWS(symbol_map, ws_base, exchange_id=exchange_id)

    return None
