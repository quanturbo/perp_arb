"""Unified async HTTP client — one process-wide aiohttp.ClientSession.

Why this exists:
  Telegram, UAInvest scanner, Bitget direct-order, exchange.connection and
  market_loader each used to spin up their own ``aiohttp.ClientSession``.
  Three problems:

    1. Telegram created a fresh session per send (≈10 ms TCP connect setup
       overhead per alert).
    2. Timeout / header / connector configuration was duplicated 5 places.
    3. Forgetting to close one leaked sockets and produced "Unclosed client
       session" warnings on shutdown.

  This module owns one session per ``HttpClient`` instance, exposes a tiny
  JSON-friendly façade, and is injected into every consumer that talks
  HTTP. ccxt's session lifecycle is owned by ccxt itself so we provide a
  separate ``make_ccxt_session()`` helper that keeps the connector config
  in one place.
"""

from __future__ import annotations

import asyncio
from typing import Any, Mapping, Optional

import aiohttp
from loguru import logger


_DEFAULT_TIMEOUT_SEC = 15.0


class HttpClient:
    """Process-wide async HTTP client around a single ``aiohttp.ClientSession``.

    Usage::

        http = HttpClient()
        try:
            data = await http.get_json("https://...", params={"q": "x"})
        finally:
            await http.aclose()

    Or as an async context manager::

        async with HttpClient() as http:
            data = await http.get_json(...)
    """

    def __init__(
        self,
        *,
        default_timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
        default_headers: Optional[Mapping[str, str]] = None,
        connector: aiohttp.BaseConnector | None = None,
    ) -> None:
        self._timeout = aiohttp.ClientTimeout(total=float(default_timeout_sec))
        self._default_headers: dict[str, str] = (
            dict(default_headers) if default_headers else {}
        )
        self._connector = connector
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()

    # lifecycle --------------------------------------------------------

    async def __aenter__(self) -> "HttpClient":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        async with self._lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=self._timeout,
                    connector=self._connector,
                    headers=self._default_headers or None,
                )
            return self._session

    async def aclose(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    @property
    def closed(self) -> bool:
        return self._session is None or self._session.closed

    # request primitives ----------------------------------------------

    def _merged_headers(
        self, headers: Optional[Mapping[str, str]],
    ) -> dict[str, str] | None:
        if not headers:
            return None
        merged = dict(self._default_headers)
        merged.update(headers)
        return merged

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        data: Any = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> aiohttp.ClientResponse:
        """Low-level wrapper. The caller MUST ``await resp.read()`` (or use
        the JSON/text helpers) and dispose of the response. Prefer the
        ``get_json`` / ``post_json`` / ``request_text`` helpers for safety.
        """
        session = await self._ensure_session()
        kwargs: dict[str, Any] = {}
        if params is not None:
            kwargs["params"] = params
        if json is not None:
            kwargs["json"] = json
        if data is not None:
            kwargs["data"] = data
        if headers:
            kwargs["headers"] = self._merged_headers(headers)
        if timeout is not None:
            kwargs["timeout"] = aiohttp.ClientTimeout(total=float(timeout))
        return await session.request(method.upper(), url, **kwargs)

    async def get_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        content_type: str | None = None,
    ) -> Any:
        """GET → JSON. Raises ``aiohttp.ClientResponseError`` on HTTP ≥ 400."""
        async with await self.request(
            "GET", url, params=params, headers=headers, timeout=timeout,
        ) as resp:
            resp.raise_for_status()
            return await resp.json(content_type=content_type)

    async def post_json(
        self,
        url: str,
        *,
        json: Any = None,
        data: Any = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        content_type: str | None = None,
    ) -> Any:
        """POST → JSON. Raises on HTTP ≥ 400."""
        async with await self.request(
            "POST", url, params=params, json=json, data=data,
            headers=headers, timeout=timeout,
        ) as resp:
            resp.raise_for_status()
            return await resp.json(content_type=content_type)

    async def request_text(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        data: Any = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, str]:
        """Return ``(status, body_text)`` regardless of HTTP code.

        Used by Telegram / fire-and-forget callers that want to log on
        failure but never raise.
        """
        async with await self.request(
            method, url, params=params, json=json, data=data,
            headers=headers, timeout=timeout,
        ) as resp:
            body = await resp.text()
            return resp.status, body


# ── ccxt session helper ──────────────────────────────────────────────────

def make_ccxt_session(
    *,
    limit: int = 64,
    limit_per_host: int = 32,
    enable_cleanup_closed: bool = True,
    timeout_sec: float | None = None,
    force_ipv4: bool = True,
    keepalive_timeout: float = 75.0,
    ttl_dns_cache: int = 300,
) -> aiohttp.ClientSession:
    """Build a ``ClientSession`` configured for ccxt's heavy REST traffic.

    ccxt assigns the session to ``exchange.session`` and owns its lifecycle
    thereafter; we don't share a session with our own ``HttpClient`` because
    ccxt closes the session itself on ``exchange.close()``.

    Defaults (force_ipv4, keepalive_timeout=75s, limit=64, limit_per_host=32,
    ttl_dns_cache=300) match what ``connection.py`` and ``market_loader.py``
    each set up independently before — now in one place.
    """
    import socket

    connector_kwargs: dict = dict(
        limit=limit,
        limit_per_host=limit_per_host,
        enable_cleanup_closed=enable_cleanup_closed,
        keepalive_timeout=keepalive_timeout,
        ttl_dns_cache=ttl_dns_cache,
    )
    if force_ipv4:
        connector_kwargs["family"] = socket.AF_INET
    connector = aiohttp.TCPConnector(**connector_kwargs)
    if timeout_sec is None:
        return aiohttp.ClientSession(connector=connector)
    timeout = aiohttp.ClientTimeout(total=float(timeout_sec))
    return aiohttp.ClientSession(connector=connector, timeout=timeout)


__all__ = ["HttpClient", "make_ccxt_session"]
