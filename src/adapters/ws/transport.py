"""Unified WebSocket transport — picows-backed, transport-neutral port.

Why this exists:
  Every exchange WS adapter and the Binance ws-fapi direct-order client used
  to import `aiohttp` directly and pass `aiohttp.WSMsgType` constants around.
  That's a transport leak across 4+ files. This module is the single seam:
  callers depend on `WSFrame` / `WSConnection` / `ws_connect` only, and
  swapping the underlying library means changing this one file.

Backed by `picows` (https://github.com/tarasko/picows) for ~3-5× lower
overhead than aiohttp on hot streams. picows uses a callback-based
``WSListener``; this module bridges callbacks → an ``async for`` iterator
via an internal ``asyncio.Queue`` so calling code reads like idiomatic
async I/O.

Usage::

    async with ws_connect(url, heartbeat=20) as ws:
        async for msg in ws:
            if msg.kind == WSFrame.TEXT:
                handle(msg.text)

Frame data:
  - TEXT  → ``msg.text`` is ``str``  (also ``msg.data`` is ``bytes`` UTF-8)
  - BINARY→ ``msg.data`` is ``bytes``
  - PING  → ``msg.data`` is the ping payload (``bytes``)
  - PONG  → ``msg.data`` is the pong payload (``bytes``)
"""

from __future__ import annotations

import asyncio
import json as _stdlib_json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional

import picows
from loguru import logger

try:  # orjson is a project dep but tolerate absence in tests
    import orjson

    def _dumps_bytes(obj: Any) -> bytes:
        return orjson.dumps(obj)

except ImportError:  # pragma: no cover

    def _dumps_bytes(obj: Any) -> bytes:
        return _stdlib_json.dumps(obj, separators=(",", ":")).encode("utf-8")


class WSFrame(Enum):
    """Transport-neutral frame kind. Map your subclass logic to these."""

    TEXT = "text"
    BINARY = "binary"
    PING = "ping"
    PONG = "pong"
    CLOSED = "closed"
    ERROR = "error"


@dataclass(frozen=True)
class WSMessage:
    """A frame popped from the transport queue."""

    kind: WSFrame
    data: bytes  # raw bytes; for TEXT this is the UTF-8 encoding

    @property
    def text(self) -> str:
        """Decode TEXT payload as str. Safe to call on BINARY (utf-8 strict)."""
        return self.data.decode("utf-8", errors="replace")


# ── picows bridge ────────────────────────────────────────────────────────


class _QueueListener(picows.WSListener):
    """Pump picows callbacks onto an asyncio.Queue.

    NOTE: ``WSFrame`` (picows class, different name from our enum) hands a
    zero-copy buffer view that is *invalidated* when ``on_ws_frame`` returns,
    so we MUST copy the payload eagerly here.
    """

    # Reasonable bound; if downstream stalls we drop frames rather than
    # ballooning memory. 10k frames is several seconds of even very chatty
    # streams (MEXC depth.full ~3 fps/symbol × 30 symbols ≈ 90 fps).
    _QUEUE_MAXSIZE = 10_000

    def __init__(self) -> None:
        # Queue is created on the event loop where the listener is built —
        # which is always the same loop that ws_connect() runs on.
        self.queue: asyncio.Queue[WSMessage] = asyncio.Queue(maxsize=self._QUEUE_MAXSIZE)
        self.transport: Optional[picows.WSTransport] = None
        self.disconnected = asyncio.Event()
        self._dropped = 0

    # picows callbacks -----------------------------------------------------

    def on_ws_connected(self, transport: picows.WSTransport) -> None:
        self.transport = transport

    def on_ws_frame(self, transport: picows.WSTransport, frame) -> None:  # noqa: ANN001
        mt = frame.msg_type
        if mt == picows.WSMsgType.TEXT:
            kind = WSFrame.TEXT
            data = frame.get_payload_as_bytes()
        elif mt == picows.WSMsgType.BINARY:
            kind = WSFrame.BINARY
            data = frame.get_payload_as_bytes()
        elif mt == picows.WSMsgType.PING:
            kind = WSFrame.PING
            data = frame.get_payload_as_bytes()
        elif mt == picows.WSMsgType.PONG:
            kind = WSFrame.PONG
            data = frame.get_payload_as_bytes()
        elif mt == picows.WSMsgType.CLOSE:
            kind = WSFrame.CLOSED
            data = b""
        else:
            return  # CONTINUATION etc. — picows reassembles for us
        try:
            self.queue.put_nowait(WSMessage(kind, data))
        except asyncio.QueueFull:  # pragma: no cover - load-only path
            self._dropped += 1
            if self._dropped == 1 or self._dropped % 1000 == 0:
                logger.warning(
                    "WS receive queue full; dropping frames (total dropped: {})",
                    self._dropped,
                )

    def on_ws_disconnected(self, transport: picows.WSTransport) -> None:
        self.disconnected.set()
        # Wake any pending receiver.
        try:
            self.queue.put_nowait(WSMessage(WSFrame.CLOSED, b""))
        except asyncio.QueueFull:  # pragma: no cover
            pass


# ── Public connection ────────────────────────────────────────────────────


class WSConnection:
    """Async-iterable WebSocket connection.

    The contract is intentionally small: iterate frames, send frames,
    close. No exchange-specific knowledge.
    """

    def __init__(
        self,
        transport: picows.WSTransport,
        listener: _QueueListener,
        *,
        receive_timeout: float,
    ) -> None:
        self._transport = transport
        self._listener = listener
        self._receive_timeout = float(receive_timeout)
        self._closed = False

    # context manager — mirrors the old `async with session.ws_connect(...)`

    async def __aenter__(self) -> "WSConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # status

    @property
    def closed(self) -> bool:
        return self._closed or self._listener.disconnected.is_set()

    # iteration --------------------------------------------------------

    def __aiter__(self) -> "WSConnection":
        return self

    async def __anext__(self) -> WSMessage:
        if self._closed:
            raise StopAsyncIteration
        if self._receive_timeout > 0:
            try:
                msg = await asyncio.wait_for(
                    self._listener.queue.get(), timeout=self._receive_timeout,
                )
            except asyncio.TimeoutError:
                self._closed = True
                self._transport.disconnect(graceful=False)
                raise
        else:
            msg = await self._listener.queue.get()
        if msg.kind in (WSFrame.CLOSED, WSFrame.ERROR):
            self._closed = True
            raise StopAsyncIteration
        return msg

    async def receive(self, timeout: float | None = None) -> WSMessage:
        """Pull a single frame (used by request/response WS clients)."""
        t = float(timeout) if timeout is not None else self._receive_timeout
        return await asyncio.wait_for(self._listener.queue.get(), timeout=t)

    # sending ---------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._closed or self._listener.disconnected.is_set():
            raise ConnectionResetError("websocket is closed")

    async def send_text(self, s: str) -> None:
        self._ensure_open()
        self._transport.send(picows.WSMsgType.TEXT, s.encode("utf-8"))

    async def send_str(self, s: str) -> None:
        """Alias matching aiohttp's name — kept to ease the migration."""
        await self.send_text(s)

    async def send_bytes(self, b: bytes) -> None:
        self._ensure_open()
        self._transport.send(picows.WSMsgType.BINARY, bytes(b))

    async def send_json(self, obj: Any) -> None:
        self._ensure_open()
        self._transport.send(picows.WSMsgType.TEXT, _dumps_bytes(obj))

    async def send_pong(self, payload: bytes = b"") -> None:
        self._ensure_open()
        self._transport.send_pong(payload)

    async def send_ping(self, payload: bytes = b"") -> None:
        self._ensure_open()
        self._transport.send_ping(payload)

    async def close(self, code: int = 1000, message: bytes = b"") -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._transport.send_close(code, message)
        except Exception:
            pass
        try:
            self._transport.disconnect(graceful=True)
        except Exception:
            pass


# ── Connection factory ───────────────────────────────────────────────────


async def ws_connect(
    url: str,
    *,
    heartbeat: float | None = 20.0,
    receive_timeout: float = 30.0,
    headers: Mapping[str, str] | None = None,
    handshake_timeout: float = 10.0,
) -> WSConnection:
    """Open a WebSocket connection.

    Args:
        url: ``ws://`` or ``wss://``.
        heartbeat: Seconds between auto-pings; ``None`` or ``0`` disables.
            picows pings only when idle (PING_WHEN_IDLE strategy), matching
            aiohttp's ``heartbeat=`` semantics.
        receive_timeout: Seconds to wait for the next frame; on timeout the
            connection is forced closed so the outer reconnect loop kicks in.
            Set generously: heartbeat handles liveness.
        headers: Extra HTTP headers for the upgrade request.
        handshake_timeout: Seconds to wait for the WS handshake response.

    Returns:
        ``WSConnection`` ready for ``async for`` iteration.
    """
    listener = _QueueListener()
    auto_ping = bool(heartbeat and heartbeat > 0)
    hb = float(heartbeat) if auto_ping else 20.0
    transport, _ = await picows.ws_connect(
        lambda: listener,
        url,
        enable_auto_ping=auto_ping,
        auto_ping_idle_timeout=hb,
        auto_ping_reply_timeout=max(5.0, hb / 2),
        enable_auto_pong=True,
        extra_headers=dict(headers) if headers else None,
        websocket_handshake_timeout=handshake_timeout,
    )
    return WSConnection(transport, listener, receive_timeout=receive_timeout)
