"""HTTP pieces for UACryptoInvest chart and live hub access."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from src.adapters.http import HttpClient

from .config import UACryptoInvestPair
from .history import UACryptoInvestHistoryClient
from .snapshot import parse_chart_snapshot


class UACryptoInvestClient:
    def __init__(
        self,
        *,
        base_url: str = "https://uacryptoinvest.com",
        http: HttpClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._http = http or HttpClient(default_headers={
            "accept": "application/json, text/plain, */*",
            "origin": self.base_url,
            "referer": f"{self.base_url}/arbitrage",
            "user-agent": "perp-arb-uacryptoinvest-scanner/1.0",
        })
        self._own_http = http is None

    async def negotiate(self) -> dict[str, Any]:
        return await self._http.post_json(
            f"{self.base_url}/hubs/live/negotiate",
            params={"negotiateVersion": "1"},
            data=b"",
            content_type=None,
        )

    async def search_tokens(
        self,
        *,
        exchange_id: int,
        search: str,
        count: int = 10,
        token_type: int = 0,
    ) -> Any:
        return await self._http.get_json(
            f"{self.base_url}/api/external/data/charts/searchTokens",
            params={
                "type": token_type,
                "exchangeId": int(exchange_id),
                "search": search,
                "count": int(count),
            },
            content_type=None,
        )

    async def chart_snapshot(self, pair: UACryptoInvestPair) -> dict[str, float]:
        out: dict[str, float] = {}
        long_prices = await self._search_exact_price(
            exchange_id=pair.long_exchange_id,
            token=pair.token,
        )
        if long_prices is not None:
            out["long_bid"] = long_prices[0]
            out["long_ask"] = long_prices[1]
        short_prices = await self._search_exact_price(
            exchange_id=pair.short_exchange_id,
            token=pair.token,
        )
        if short_prices is not None:
            out["short_bid"] = short_prices[0]
            out["short_ask"] = short_prices[1]
        try:
            status, body = await self._http.request_text("GET", pair.chart_url, timeout=20.0)
            if status == 200:
                out.update(parse_chart_snapshot(body, pair))
        except Exception:
            pass
        return out

    async def chart_history(
        self,
        pair: UACryptoInvestPair,
        *,
        older_than: int | None = None,
        interval: int = 1,
        range_sec: int = 86400,
    ) -> dict[str, Any]:
        session = await self._http._ensure_session()
        return await UACryptoInvestHistoryClient(
            base_url=self.base_url,
            session=session,
        ).fetch(pair, older_than=older_than, interval=interval, range_sec=range_sec)

    async def _search_exact_price(
        self,
        *,
        exchange_id: int,
        token: str,
    ) -> tuple[float, float] | None:
        payload = await self.search_tokens(
            exchange_id=exchange_id,
            search=token,
            count=10,
        )
        rows = payload.get("result", []) if isinstance(payload, dict) else []
        token_upper = token.upper()
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("tokenName") or "").upper() != token_upper:
                continue
            try:
                bid = float(row["bidPrice"])
                ask = float(row["askPrice"])
            except (KeyError, TypeError, ValueError):
                return None
            return bid, ask
        return None

    def websocket_url(self, negotiate_payload: dict[str, Any]) -> str:
        token = negotiate_payload.get("connectionToken") or negotiate_payload.get("connectionId")
        if not token:
            raise ValueError("UACryptoInvest negotiate response did not include a connection token")
        ws_base = self.base_url.replace("https://", "wss://").replace("http://", "ws://")
        return f"{ws_base}/hubs/live?id={quote(str(token), safe='')}"

    async def aclose(self) -> None:
        if self._own_http:
            await self._http.aclose()