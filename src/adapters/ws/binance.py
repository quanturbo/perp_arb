"""Binance USDM Futures @bookTicker via combined stream.

Combined stream URL = one WS connection for all symbols.
Works for any Binance-compatible fork by changing ws_base.
"""

from __future__ import annotations

from typing import Optional

from src.adapters.ws.base import BaseExchangeWS, RawTick, _loads, _maybe_float


class BinanceWS(BaseExchangeWS):
    """Binance USDM Futures @bookTicker via combined stream."""

    def __init__(
        self,
        symbol_map: dict[str, str],
        ws_base: str = "wss://fstream.binance.com",
        exchange_id: str = "",
    ):
        super().__init__({k.lower(): v for k, v in symbol_map.items()}, exchange_id=exchange_id)
        self._ws_base = ws_base.rstrip("/")

    def _ws_url(self) -> str:
        streams = "/".join(f"{s}@bookTicker" for s in self._symbol_map)
        return f"{self._ws_base}/stream?streams={streams}"

    def _subscribe_payload(self) -> list[dict]:
        return []  # Combined stream doesn't need subscription messages

    # NOTE: do NOT override _on_connected — base class tracks reconnect_count,
    # first_connect_time, and triggers the "UNSTABLE" ERROR → Telegram alert
    # when reconnects spike. A silent override here previously broke telemetry
    # for BinanceWS subclasses (binanceusdm, aster).

    def _parse_message(self, data: bytes | str, recv_time: float) -> Optional[RawTick]:
        try:
            msg = _loads(data)
            inner = msg.get("data")
            if not inner:
                return None
            native = inner.get("s", "").lower()
            unified = self._symbol_map.get(native)
            if not unified:
                return None
            return RawTick(
                symbol=unified,
                bid=float(inner["b"]),
                ask=float(inner["a"]),
                timestamp_ms=int(inner.get("T") or inner.get("E", 0)),
                receive_time=recv_time,
                bid_qty=_maybe_float(inner.get("B")),
                ask_qty=_maybe_float(inner.get("A")),
            )
        except (KeyError, ValueError, TypeError):
            return None
