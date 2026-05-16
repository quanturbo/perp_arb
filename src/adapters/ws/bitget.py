"""Bitget USDT-FUTURES ticker (v2 API). Manual ping every 25s."""

from __future__ import annotations

from typing import Optional

from src.adapters.ws.base import BaseExchangeWS, RawTick, _loads, _maybe_float


class BitgetWS(BaseExchangeWS):
    """Bitget USDT-FUTURES ticker (v2 API). Manual ping every 25s."""

    _WS_URL = "wss://ws.bitget.com/v2/ws/public"
    _ping_timeout_sec = 25.0
    _heartbeat_sec = None

    def _ws_url(self) -> str:
        return self._WS_URL

    def _subscribe_payload(self) -> list[dict]:
        args = [
            {"instType": "USDT-FUTURES", "channel": "ticker", "instId": sid}
            for sid in self._symbol_map
        ]
        return [{"op": "subscribe", "args": args}]

    def _ping_payload(self) -> str:
        return "ping"

    def _parse_message(self, data: bytes | str, recv_time: float) -> Optional[RawTick]:
        try:
            msg = _loads(data)
            action = msg.get("action")
            if action not in ("snapshot", "update"):
                return None
            arg = msg.get("arg", {})
            if arg.get("channel") != "ticker":
                return None
            for item in msg.get("data", []):
                native = item.get("instId", "")
                unified = self._symbol_map.get(native)
                if not unified:
                    continue
                return RawTick(
                    symbol=unified,
                    bid=float(item.get("bidPr", 0) or 0),
                    ask=float(item.get("askPr", 0) or 0),
                    timestamp_ms=int(item.get("ts", 0) or 0),
                    receive_time=recv_time,
                    bid_qty=_maybe_float(item.get("bidSz")),
                    ask_qty=_maybe_float(item.get("askSz")),
                )
        except (KeyError, ValueError, TypeError):
            pass
        return None
