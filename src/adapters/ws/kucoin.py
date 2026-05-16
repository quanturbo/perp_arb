"""Kucoin Futures tickerV2 stream.

Kucoin requires a REST token from POST /api/v1/bullet-public before
connecting to WebSocket. Token + endpoint are refreshed via the
`_pre_connect` hook before every ws_connect so expired tokens after a
disconnect trigger a fresh fetch automatically.
Manual ping every 18s with JSON {"id":"p","type":"ping"}.
Timestamp comes in nanoseconds.
"""

from __future__ import annotations

from typing import Optional

from src.adapters.http import HttpClient
from src.adapters.ws.base import BaseExchangeWS, RawTick, _loads, _maybe_float


class KucoinWS(BaseExchangeWS):
    """Kucoin Futures tickerV2 stream with REST token auth."""

    _REST_URL = "https://api-futures.kucoin.com/api/v1/bullet-public"
    _DEFAULT_WS_ENDPOINT = "wss://ws-api-futures.kucoin.com"
    _ping_timeout_sec = 18.0
    _heartbeat_sec = None

    def __init__(self, symbol_map: dict[str, str], *, http: HttpClient, exchange_id: str = ""):
        super().__init__(symbol_map, exchange_id=exchange_id)
        self._ws_endpoint: str = self._DEFAULT_WS_ENDPOINT
        self._token: str = ""
        self._http = http

    async def _pre_connect(self) -> None:
        """Fetch a fresh WS token + endpoint before each connect."""
        body = await self._http.post_json(self._REST_URL)
        data = body.get("data", {})
        self._token = data.get("token", "")
        servers = data.get("instanceServers") or []
        self._ws_endpoint = (
            servers[0].get("endpoint", self._DEFAULT_WS_ENDPOINT)
            if servers
            else self._DEFAULT_WS_ENDPOINT
        )

    def _ws_url(self) -> str:
        return f"{self._ws_endpoint}?token={self._token}&connectId=arb"

    def _subscribe_payload(self) -> list[dict]:
        return [
            {
                "id": f"sub-{sid}",
                "type": "subscribe",
                "topic": f"/contractMarket/tickerV2:{sid}",
                "privateChannel": False,
                "response": True,
            }
            for sid in self._symbol_map
        ]

    def _ping_payload(self) -> dict:
        return {"id": "p", "type": "ping"}

    def _parse_message(self, data: bytes | str, recv_time: float) -> Optional[RawTick]:
        try:
            msg = _loads(data)
            if msg.get("type") != "message":
                return None
            topic = msg.get("topic", "")
            if "/contractMarket/tickerV2:" not in topic:
                return None
            d = msg.get("data", {})
            native = d.get("symbol", "")
            unified = self._symbol_map.get(native)
            if not unified:
                return None
            bid = d.get("bestBidPrice")
            ask = d.get("bestAskPrice")
            if not bid or not ask:
                return None
            # Kucoin ts is in nanoseconds
            ts_ns = int(d.get("ts", 0))
            ts_ms = ts_ns // 1_000_000 if ts_ns > 1e15 else ts_ns
            return RawTick(
                symbol=unified,
                bid=float(bid),
                ask=float(ask),
                timestamp_ms=ts_ms,
                receive_time=recv_time,
                bid_qty=_maybe_float(d.get("bestBidSize")),
                ask_qty=_maybe_float(d.get("bestAskSize")),
            )
        except (KeyError, ValueError, TypeError):
            return None

    async def close(self) -> None:
        await super().close()
