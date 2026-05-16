"""Bybit V5 linear (USDT perps) tickers. Manual ping every 18s."""

from __future__ import annotations

from typing import Optional

from src.adapters.ws.base import BaseExchangeWS, RawTick, _loads, _maybe_float


class BybitWS(BaseExchangeWS):
    """Bybit V5 linear (USDT perps) tickers. Manual ping every 18s."""

    _WS_URL = "wss://stream.bybit.com/v5/public/linear"
    _ping_timeout_sec = 18.0
    _heartbeat_sec = None

    def _ws_url(self) -> str:
        return self._WS_URL

    def _subscribe_payload(self) -> list[dict]:
        args = [f"tickers.{sid}" for sid in self._symbol_map]
        return [{"op": "subscribe", "args": args}]

    def _ping_payload(self) -> dict:
        return {"op": "ping"}

    def _parse_message(self, data: bytes | str, recv_time: float) -> Optional[RawTick]:
        try:
            msg = _loads(data)
            topic = msg.get("topic", "")
            if not topic.startswith("tickers."):
                return None
            d = msg.get("data", {})
            native = d.get("symbol", "")
            unified = self._symbol_map.get(native)
            if not unified:
                return None
            bid = d.get("bid1Price")
            ask = d.get("ask1Price")
            if not bid or not ask:
                return None
            return RawTick(
                symbol=unified,
                bid=float(bid),
                ask=float(ask),
                timestamp_ms=int(msg.get("ts", 0)),
                receive_time=recv_time,
                bid_qty=_maybe_float(d.get("bid1Size")),
                ask_qty=_maybe_float(d.get("ask1Size")),
            )
        except (KeyError, ValueError, TypeError):
            return None
