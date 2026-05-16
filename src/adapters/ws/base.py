"""Base classes for raw WebSocket ticker streams.

Architecture (Template Method + Ports & Adapters):
  RawTickerStream (Port/ABC)  — contract: run() + close()
  BaseExchangeWS (Template)   — handles reconnection, backoff, heartbeats

The transport layer (picows) is hidden behind ``src.adapters.ws.transport``.
This file contains zero references to picows or aiohttp; it speaks
``WSConnection`` and ``WSFrame`` only.

Subclasses override 3 methods:
  _ws_url()            → connection URL
  _subscribe_payload() → subscription message(s) to send after connect
  _parse_message()     → extract RawTick from a WS text message, or None

Optional hooks:
  _pre_connect()        → coroutine run before each connect (refresh tokens etc.)
  _ping_payload()       → manual app-level ping payload (None disables)
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Optional

from loguru import logger

from src.adapters.ws.transport import WSConnection, WSFrame, ws_connect

try:
    import orjson

    _loads = orjson.loads
except ImportError:
    import json as _json

    _loads = _json.loads


def _maybe_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


OnRawTick = Callable[["RawTick"], None]


@dataclass
class RawTick:
    """Minimal tick from raw WebSocket — zero CCXT overhead."""

    symbol: str  # unified CCXT symbol (e.g. "RAVE/USDT:USDT")
    bid: float
    ask: float
    timestamp_ms: int  # exchange timestamp in milliseconds
    receive_time: float  # local time.time() at WS message receipt
    bid_qty: float | None = None
    ask_qty: float | None = None


class RawTickerStream(ABC):
    """Port: raw WebSocket ticker stream for one exchange.

    One connection handles all subscribed symbols.
    run() blocks until stop event is set. Handles reconnection internally.
    """

    @abstractmethod
    async def run(self, on_tick: OnRawTick, stop: asyncio.Event) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    def stats(self) -> dict: ...

    @abstractmethod
    async def request_reconnect(self, reason: str = "manual_reconnect") -> bool: ...


class BaseExchangeWS(RawTickerStream):
    """Template Method: handles reconnection, backoff, heartbeats.

    Subclasses override:
      _ws_url()            → str (WebSocket URL to connect to)
      _subscribe_payload() → list[dict] (messages to send after connect)
      _parse_message()     → Optional[RawTick] (parse one WS text frame)
      _ping_payload()      → Optional[str|dict] (manual keep-alive, or None)
      _ping_timeout_sec    → float (seconds between manual pings; 0 = disabled)
      _heartbeat_sec       → Optional[float] (transport-level WS ping interval)

    The transport (picows) handles standard WS PING/PONG control frames
    for ``_heartbeat_sec``. Manual app-level pings (Bitget JSON, Kucoin, MEXC)
    use ``_ping_payload()`` + ``_ping_timeout_sec`` and run on a background
    task so they fire even when the data stream is busy.
    """

    _ping_timeout_sec: float = 0
    _heartbeat_sec: float | None = 20
    _max_backoff: float = 15.0
    _RECEIVE_TIMEOUT_FLOOR: float = 8.0

    _RECONNECT_ALERT_COUNT: int = 3
    _RECONNECT_ALERT_WINDOW_SEC: float = 300.0

    def __init__(self, symbol_map: dict[str, str], exchange_id: str = ""):
        self._symbol_map = symbol_map
        self.exchange_id = exchange_id
        # Health telemetry exposed via public attributes for stats/dashboard.
        self.reconnect_count: int = 0
        self.first_connect_time: float = 0.0
        self.last_connect_time: float = 0.0
        self.last_disconnect_reason: str = ""
        self._recent_reconnects: list[float] = []
        self._alert_cooldown_until: float = 0.0
        self._current_ws: WSConnection | None = None
        self._manual_reconnect_requested = False
        # Set by request_reconnect() when the caller already knows the
        # cause (e.g. symbol stall → quarantine). The next reconnect
        # then skips the UNSTABLE counter so we don't double-alert.
        self._suppress_next_alert_count = False

    @abstractmethod
    def _ws_url(self) -> str: ...

    @abstractmethod
    def _subscribe_payload(self) -> list[dict]: ...

    @abstractmethod
    def _parse_message(
        self, data: bytes | str, recv_time: float
    ) -> Optional[RawTick]: ...

    def _ping_payload(self) -> str | dict | None:
        return None

    async def _pre_connect(self) -> None:
        """Template hook: runs before each connect attempt.

        Override to fetch tokens, resolve endpoints, refresh auth, etc.
        Default: no-op. Raise on failure — the outer loop will back off
        and retry like any other connection error.
        """
        return None

    # ── reconnect telemetry ──────────────────────────────────────────

    def _on_connected(self) -> None:
        now = time.time()
        self.last_connect_time = now
        if self.first_connect_time == 0.0:
            self.first_connect_time = now
            logger.info(
                "{} connected, symbols: {}",
                type(self).__name__,
                list(self._symbol_map.keys()),
            )
            return

        self.reconnect_count += 1
        suppress = self._suppress_next_alert_count
        self._suppress_next_alert_count = False
        if not suppress:
            self._recent_reconnects.append(now)
        cutoff = now - self._RECONNECT_ALERT_WINDOW_SEC
        self._recent_reconnects = [t for t in self._recent_reconnects if t >= cutoff]
        recent = len(self._recent_reconnects)

        name = type(self).__name__
        logger.info(
            "{} reconnected (#{}, {} in last {:.0f}s), symbols: {}",
            name, self.reconnect_count, recent,
            self._RECONNECT_ALERT_WINDOW_SEC, list(self._symbol_map.keys()),
        )

        if recent >= self._RECONNECT_ALERT_COUNT and now >= self._alert_cooldown_until:
            self._alert_cooldown_until = now + self._RECONNECT_ALERT_WINDOW_SEC
            logger.error(
                "BOT ISSUE | level=ERROR | type=WS_UNSTABLE | exchange={} | symbol= | "
                "reason=shared websocket reconnected {} times in {:.0f}s; last_disconnect={}",
                self.exchange_id or name,
                recent,
                self._RECONNECT_ALERT_WINDOW_SEC,
                self.last_disconnect_reason or "unknown",
            )

    def stats(self) -> dict:
        """Health telemetry for dashboards / /api/exchange_stats."""
        now = time.time()
        cutoff = now - self._RECONNECT_ALERT_WINDOW_SEC
        recent = sum(1 for t in self._recent_reconnects if t >= cutoff)
        return {
            "ws_class": type(self).__name__,
            "reconnect_count": self.reconnect_count,
            "reconnects_recent": recent,
            "reconnect_window_sec": self._RECONNECT_ALERT_WINDOW_SEC,
            "last_connect_age_sec": (
                round(now - self.last_connect_time, 1) if self.last_connect_time else None
            ),
            "uptime_sec": (
                round(now - self.first_connect_time, 1) if self.first_connect_time else None
            ),
            "last_disconnect_reason": self.last_disconnect_reason,
        }

    def _compute_receive_timeout(self) -> float:
        """Fail reads within ~2 keep-alive cycles so half-dead TCP recovers fast."""
        if self._heartbeat_sec and not self._ping_timeout_sec and self._ping_payload() is None:
            return 0.0
        candidates = [self._RECEIVE_TIMEOUT_FLOOR]
        if self._heartbeat_sec:
            candidates.append(self._heartbeat_sec * 2.0)
        if self._ping_timeout_sec and self._ping_timeout_sec > 0:
            candidates.append(self._ping_timeout_sec * 2.0)
        return max(candidates)

    async def request_reconnect(self, reason: str = "manual_reconnect") -> bool:
        """Ask the active websocket to close so the outer run loop reconnects.

        ``reason`` is recorded in ``last_disconnect_reason`` so the
        UNSTABLE alert (and the dashboard) can show *why* the socket
        was kicked. Reconnects with a known, non-infrastructure cause
        (anything that is not ``manual_reconnect``) do NOT count toward
        the UNSTABLE alert window — the watchdog already logs the real
        problem (e.g. stalled symbol → quarantine).
        """
        ws = self._current_ws
        if ws is None or ws.closed:
            return False
        self._manual_reconnect_requested = True
        self._suppress_next_alert_count = reason != "manual_reconnect"
        self.last_disconnect_reason = reason or "manual_reconnect"
        await ws.close(code=1000, message=b"manual reconnect")
        return True

    # ── main loop ────────────────────────────────────────────────────

    async def run(self, on_tick: OnRawTick, stop: asyncio.Event) -> None:
        backoff = 1.0

        while not stop.is_set():
            try:
                await self._pre_connect()
                url = self._ws_url()
                ws = await ws_connect(
                    url,
                    heartbeat=self._heartbeat_sec,
                    receive_timeout=self._compute_receive_timeout(),
                )
                async with ws:
                    self._current_ws = ws
                    for payload in self._subscribe_payload():
                        await ws.send_json(payload)
                    self._on_connected()
                    backoff = 1.0

                    try:
                        if self._ping_timeout_sec > 0 or self._ping_payload() is not None:
                            await self._loop_with_manual_ping(ws, on_tick, stop)
                        else:
                            await self._loop_with_heartbeat(ws, on_tick, stop)
                    finally:
                        if self._current_ws is ws:
                            self._current_ws = None

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.last_disconnect_reason = f"{type(e).__name__}: {e}"[:200]
                if not stop.is_set():
                    logger.warning(
                        "{}: {} — reconnecting in {:.0f}s",
                        type(self).__name__,
                        e,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, self._max_backoff)
            else:
                if not stop.is_set():
                    if self._manual_reconnect_requested:
                        self.last_disconnect_reason = "manual_reconnect"
                        self._manual_reconnect_requested = False
                    else:
                        self.last_disconnect_reason = "clean_close"

    # ── subclass-overridable message loops ──────────────────────────

    async def _loop_with_heartbeat(
        self,
        ws: WSConnection,
        on_tick: OnRawTick,
        stop: asyncio.Event,
    ) -> None:
        """Default loop: route TEXT frames to ``_parse_message``.

        Suitable for exchanges that use the standard WS ping/pong
        (heartbeat handled by transport).
        """
        async for msg in ws:
            if stop.is_set():
                break
            if msg.kind == WSFrame.TEXT:
                tick = self._parse_message(msg.text, time.time())
                if tick:
                    on_tick(tick)
            elif msg.kind in (WSFrame.CLOSED, WSFrame.ERROR):
                break

    async def _loop_with_manual_ping(
        self,
        ws: WSConnection,
        on_tick: OnRawTick,
        stop: asyncio.Event,
    ) -> None:
        """Loop with an app-level periodic ping task.

        Required for Bitget/Bybit/OKX/Kucoin where the server expects a
        JSON or string ping at a fixed cadence regardless of data flow.
        """
        interval = self._ping_timeout_sec if self._ping_timeout_sec > 0 else 20.0
        has_ping = self._ping_payload() is not None

        async def _ping_timer():
            while not stop.is_set():
                await asyncio.sleep(interval)
                if ws.closed or stop.is_set():
                    return
                ping = self._ping_payload()
                if ping is None:
                    continue
                try:
                    if isinstance(ping, dict):
                        await ws.send_json(ping)
                    else:
                        await ws.send_text(str(ping))
                except Exception:
                    return  # connection dying; outer loop will reconnect

        ping_task = asyncio.create_task(_ping_timer()) if has_ping else None
        try:
            async for msg in ws:
                if stop.is_set():
                    break
                if msg.kind == WSFrame.TEXT:
                    text = msg.text
                    low = text.lower()
                    if low in ("pong", "ping"):
                        if low == "ping":
                            try:
                                await ws.send_text("pong")
                            except Exception:
                                break
                        continue
                    tick = self._parse_message(text, time.time())
                    if tick:
                        on_tick(tick)
                elif msg.kind in (WSFrame.CLOSED, WSFrame.ERROR):
                    break
        finally:
            if ping_task:
                ping_task.cancel()
                try:
                    await ping_task
                except (asyncio.CancelledError, Exception):
                    pass

    async def close(self) -> None:
        ws = self._current_ws
        if ws is not None and not ws.closed:
            await ws.close()
