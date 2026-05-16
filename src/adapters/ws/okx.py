"""OKX bbo-tbt (best bid/offer tick-by-tick). Manual ping every 25s."""

from __future__ import annotations

from typing import Optional

from src.adapters.ws.base import BaseExchangeWS, RawTick, _loads, _maybe_float


class OkxWS(BaseExchangeWS):
    """OKX bbo-tbt (best bid/offer tick-by-tick). Manual ping every 25s."""

    _WS_URL = "wss://ws.okx.com/ws/v5/public"
    _ping_timeout_sec = 25.0
    _heartbeat_sec = None

    def _ws_url(self) -> str:
        return self._WS_URL

    def _subscribe_payload(self) -> list[dict]:
        args = [{"channel": "bbo-tbt", "instId": sid} for sid in self._symbol_map]
        return [{"op": "subscribe", "args": args}]

    def _ping_payload(self) -> str:
        return "ping"

    def _parse_message(self, data: bytes | str, recv_time: float) -> Optional[RawTick]:
        try:
            msg = _loads(data)
            arg = msg.get("arg")
            if not arg or arg.get("channel") != "bbo-tbt":
                return None
            for item in msg.get("data", []):
                bids = item.get("bids", [])
                asks = item.get("asks", [])
                if not bids or not asks:
                    continue
                native = arg.get("instId", "")
                unified = self._symbol_map.get(native)
                if not unified:
                    continue
                return RawTick(
                    symbol=unified,
                    bid=float(bids[0][0]),
                    ask=float(asks[0][0]),
                    timestamp_ms=int(item.get("ts", 0)),
                    receive_time=recv_time,
                    bid_qty=_maybe_float(bids[0][1] if len(bids[0]) > 1 else None),
                    ask_qty=_maybe_float(asks[0][1] if len(asks[0]) > 1 else None),
                )
        except (KeyError, ValueError, TypeError, IndexError):
            pass
        return None
