"""MEXC futures (contract) depth-snapshot stream.

Endpoint: wss://contract.mexc.com/edge
Subscribe per symbol: sub.depth.full with limit=1 for best bid/ask.
Pushes full L2 snapshot every ~300ms (vs ~2.4s for sub.ticker).
Best bid = bids[0][0], best ask = asks[0][0].

MEXC requires application-level {"method":"ping"} every <60s. We let
the base class manual-ping loop fire {"method":"ping"} every 15s on a
background task — it runs regardless of data flow.
"""

from __future__ import annotations

from typing import Optional

from src.adapters.ws.base import BaseExchangeWS, RawTick, _loads, _maybe_float


class MexcWS(BaseExchangeWS):
    """MEXC futures depth-snapshot stream (~300ms updates)."""

    _WS_URL = "wss://contract.mexc.com/edge"
    _ping_timeout_sec = 15.0
    _heartbeat_sec = None

    def _ws_url(self) -> str:
        return self._WS_URL

    def _subscribe_payload(self) -> list[dict]:
        return [
            {
                "method": "sub.depth.full",
                "param": {"symbol": sid, "limit": 5, "compress": False},
                "gzip": False,
            }
            for sid in self._symbol_map
        ]

    def _ping_payload(self) -> dict:
        return {"method": "ping"}

    def _parse_message(self, data: bytes | str, recv_time: float) -> Optional[RawTick]:
        try:
            msg = _loads(data)
            if msg.get("channel") != "push.depth.full":
                return None
            native = msg.get("symbol", "")
            unified = self._symbol_map.get(native)
            if not unified:
                return None
            d = msg.get("data")
            if not d:
                return None
            bids = d.get("bids")
            asks = d.get("asks")
            if not bids or not asks:
                return None
            return RawTick(
                symbol=unified,
                bid=float(bids[0][0]),
                ask=float(asks[0][0]),
                timestamp_ms=int(msg.get("ts", 0)),
                receive_time=recv_time,
                bid_qty=_maybe_float(bids[0][1] if len(bids[0]) > 1 else None),
                ask_qty=_maybe_float(asks[0][1] if len(asks[0]) > 1 else None),
            )
        except (KeyError, ValueError, TypeError, IndexError):
            return None
