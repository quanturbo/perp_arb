"""BingX USDT-M perpetual bookTicker stream.

Messages are gzip-compressed binary frames.
Server sends "Ping" → client must respond "Pong".
Symbol format uses hyphens: BTC-USDT.
"""

from __future__ import annotations

import asyncio
import gzip
import time
from typing import Optional

from src.adapters.ws.base import BaseExchangeWS, OnRawTick, RawTick, _loads, _maybe_float
from src.adapters.ws.transport import WSConnection, WSFrame


class BingxWS(BaseExchangeWS):
    """BingX USDT-M perpetual bookTicker stream (gzip compressed)."""

    _WS_URL = "wss://open-api-swap.bingx.com/swap-market"
    _ping_timeout_sec = 0  # Ping/pong handled inline
    _heartbeat_sec = 20

    def _ws_url(self) -> str:
        return self._WS_URL

    def _subscribe_payload(self) -> list[dict]:
        return [
            {"id": "bt1", "reqType": "sub", "dataType": f"{sid}@bookTicker"}
            for sid in self._symbol_map
        ]

    def _parse_message(self, data: bytes | str, recv_time: float) -> Optional[RawTick]:
        try:
            msg = _loads(data)
            d = msg.get("data")
            if not d:
                return None
            native = d.get("s", "")
            unified = self._symbol_map.get(native)
            if not unified:
                return None
            # BingX bookTicker uses Binance-like short keys: b, a, T
            bid = d.get("b") or d.get("bestBidPrice") or d.get("bidPrice")
            ask = d.get("a") or d.get("bestAskPrice") or d.get("askPrice")
            if not bid or not ask:
                return None
            ts = int(d.get("T", 0) or d.get("E", 0) or d.get("time", 0) or 0)
            return RawTick(
                symbol=unified,
                bid=float(bid),
                ask=float(ask),
                timestamp_ms=ts,
                receive_time=recv_time,
                bid_qty=_maybe_float(d.get("B") or d.get("bidQty")),
                ask_qty=_maybe_float(d.get("A") or d.get("askQty")),
            )
        except (KeyError, ValueError, TypeError):
            return None

    async def _loop_with_heartbeat(
        self,
        ws: WSConnection,
        on_tick: OnRawTick,
        stop: asyncio.Event,
    ) -> None:
        """BingX sends gzip-compressed binary frames + Ping/Pong."""

        async for msg in ws:
            if stop.is_set():
                break
            if msg.kind == WSFrame.BINARY:
                try:
                    text = gzip.decompress(msg.data).decode("utf-8")
                except Exception:
                    continue
                if text == "Ping":
                    await ws.send_text("Pong")
                    continue
                tick = self._parse_message(text, time.time())
                if tick:
                    on_tick(tick)
            elif msg.kind == WSFrame.TEXT:
                text = msg.text
                if text == "Ping":
                    await ws.send_text("Pong")
                    continue
                tick = self._parse_message(text, time.time())
                if tick:
                    on_tick(tick)
            elif msg.kind in (WSFrame.ERROR, WSFrame.CLOSED):
                break
