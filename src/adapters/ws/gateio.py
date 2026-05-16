"""Gate.io USDT futures book_ticker stream."""

from __future__ import annotations

import time
from typing import Optional

from src.adapters.ws.base import BaseExchangeWS, RawTick, _loads, _maybe_float


class GateioWS(BaseExchangeWS):
    """Gate.io USDT futures book_ticker stream."""

    _WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    _ping_timeout_sec = 10.0
    _heartbeat_sec = None

    def _ws_url(self) -> str:
        return self._WS_URL

    def _subscribe_payload(self) -> list[dict]:
        return [
            {
                "time": int(time.time()),
                "channel": "futures.book_ticker",
                "event": "subscribe",
                "payload": list(self._symbol_map.keys()),
            }
        ]

    def _ping_payload(self) -> dict:
        return {"time": int(time.time()), "channel": "futures.ping"}

    def _parse_message(self, data: bytes | str, recv_time: float) -> Optional[RawTick]:
        try:
            msg = _loads(data)
            if (
                msg.get("event") != "update"
                or msg.get("channel") != "futures.book_ticker"
            ):
                return None
            result = msg.get("result", {})
            native = result.get("s", "")
            unified = self._symbol_map.get(native)
            if not unified:
                return None
            return RawTick(
                symbol=unified,
                bid=float(result.get("b", 0)),
                ask=float(result.get("a", 0)),
                timestamp_ms=int(result.get("t", 0)),
                receive_time=recv_time,
                bid_qty=_maybe_float(result.get("B") or result.get("bid_size")),
                ask_qty=_maybe_float(result.get("A") or result.get("ask_size")),
            )
        except (KeyError, ValueError, TypeError):
            return None
