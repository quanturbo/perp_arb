"""HTTP client for ``uainvest.com.ua/api/arbitrage/offers``.

The endpoint is a public-ish dashboard backend protected by a session
cookie + XSRF token; both are passed verbatim from the operator's browser
because there's no documented login flow. Both can be overridden via env::

    UAINVEST_COOKIE         (full cookie header)
    UAINVEST_USER_AGENT     (defaults to a recent Chrome UA)
    UAINVEST_EXCHANGES      (underscore-joined list, default = full set)

The HTTP transport is the shared ``HttpClient`` (single aiohttp session
across the whole app). If none is injected we lazily create one and own
its lifecycle, so this module also works in standalone scripts.
"""

from __future__ import annotations

import os
from typing import Any

from loguru import logger

from src.adapters.http import HttpClient

_DEFAULT_EXCHANGES = (
    "aster_binance_bitget_bybit_bybitfi_edgex_extended_gate_gatefi_htx_"
    "hyperliquid_mexc_nado_okx_whitebit"
)
_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)


class UAInvestClient:
    """Thin async HTTP client. One instance per scanner."""

    BASE_URL = "https://uainvest.com.ua"

    def __init__(
        self,
        *,
        exchanges: str | None = None,
        cookie: str | None = None,
        user_agent: str | None = None,
        timeout_sec: float = 15.0,
        http: HttpClient | None = None,
    ) -> None:
        self._exchanges = exchanges or os.environ.get(
            "UAINVEST_EXCHANGES", _DEFAULT_EXCHANGES,
        )
        self._cookie = cookie or os.environ.get("UAINVEST_COOKIE", "")
        self._user_agent = user_agent or os.environ.get(
            "UAINVEST_USER_AGENT", _DEFAULT_USER_AGENT,
        )
        self._timeout_sec = timeout_sec
        self._http = http
        self._owns_http = http is None

    async def aclose(self) -> None:
        if self._owns_http and self._http is not None:
            await self._http.aclose()
            self._http = None

    def _headers(self) -> dict[str, str]:
        # Reproduces the curl recipe verbatim. Most are ignored by the
        # server but keeping them keeps us anonymous-looking and reduces
        # the chance of a future 403 from header sniffing.
        h = {
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "user-agent": self._user_agent,
            "referer": (
                f"{self.BASE_URL}/arbitrage?exchanges={self._exchanges}"
                "&open_spread=1&sort_by=funding_spread&sort_dir=desc"
            ),
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }
        if self._cookie:
            h["cookie"] = self._cookie
        return h

    async def fetch_offers(
        self, *, sort_by: str = "funding_spread", sort_dir: str = "desc",
    ) -> list[dict[str, Any]]:
        """Fetch raw offers list. Returns ``[]`` on any network/parse error."""
        url = f"{self.BASE_URL}/api/arbitrage/offers"
        params = {
            "exchanges": self._exchanges,
            "open_spread": "1",
            "sort_by": sort_by,
            "sort_dir": sort_dir,
        }
        if self._http is None:
            self._http = HttpClient(default_timeout_sec=self._timeout_sec)
        try:
            status, body = await self._http.request_text(
                "GET", url, params=params, headers=self._headers(),
                timeout=self._timeout_sec,
            )
        except Exception as e:  # noqa: BLE001 — defensive at I/O boundary
            logger.warning("UAInvest network error: {}", e)
            return []

        if status != 200:
            logger.warning("UAInvest fetch failed (HTTP {}): {}", status, body[:200])
            return []
        try:
            import json as _json
            payload = _json.loads(body)
        except Exception as e:
            logger.warning("UAInvest parse error: {}", e)
            return []

        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if not isinstance(data, list):
            return []
        return data
