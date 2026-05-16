"""HTTP routes for the scanner module — registered only when the service is on.

Self-contained: deleting ``src/scanners/`` plus the 3-line wire in
``src/web/app.py`` removes everything cleanly.
"""

from __future__ import annotations

import os
import time
from dataclasses import asdict

import orjson
from aiohttp import web

from src.scanners.base import ScanOffer
from src.scanners.filter_store import ScannerFilter
from src.scanners.service import ScannerService


def _json_response(data, status: int = 200) -> web.Response:
    return web.Response(
        body=orjson.dumps(data),
        status=status,
        content_type="application/json",
    )


def _offer_dict(offer: ScanOffer) -> dict:
    """Serialize a ``ScanOffer`` plus the derived per-hour fields the UI
    actually displays/sorts on. Keys are stable contract for the JS table."""
    now = time.time()
    d = asdict(offer)
    d["funding_per_hour_long_pct"] = offer.funding_per_hour_long_pct
    d["funding_per_hour_short_pct"] = offer.funding_per_hour_short_pct
    d["funding_diff_pct"] = offer.funding_diff_pct
    d["funding_diff_pct_per_hour"] = offer.funding_diff_pct_per_hour_at(now)
    d["next_funding_ts_long"] = offer.next_funding_ts_long(now)
    d["next_funding_ts_short"] = offer.next_funding_ts_short(now)
    d["seconds_to_next_funding_long"] = offer.seconds_to_next_funding_long(now)
    d["seconds_to_next_funding_short"] = offer.seconds_to_next_funding_short(now)
    d["seconds_to_next_funding"] = offer.seconds_to_next_funding(now)
    d["intervals_match"] = offer.intervals_match
    d["minutes_to_next_funding"] = offer.minutes_to_next_funding(now)
    d["tradeable"] = offer.is_tradeable_by_bot()
    return d


def _symbol_token(value: str) -> str:
    text = (value or "").strip().upper()
    if not text:
        return ""
    if "/" in text:
        return text.split("/", 1)[0]
    if text.endswith("USDT") and len(text) > 4:
        return text[:-4]
    return text


def _offer_chart_code(offer: ScanOffer | None) -> str:
    if offer is None or not offer.chart_url:
        return ""
    try:
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(offer.chart_url)
        return (parse_qs(parsed.query).get("charts") or [""])[0]
    except Exception:  # noqa: BLE001
        return ""


def _best_chart_offer_for_symbol(
    offers: list[ScanOffer],
    symbol: str,
) -> ScanOffer | None:
    token = _symbol_token(symbol)
    if not token:
        return None
    matches = [
        offer for offer in offers
        if _offer_chart_code(offer)
        and (_symbol_token(offer.symbol) == token or _symbol_token(offer.coin) == token)
    ]
    if not matches:
        return None

    def score(offer: ScanOffer) -> tuple[int, int, float, float]:
        return (
            1 if offer.source == "uacryptoinvest" else 0,
            1 if offer.bot_exchange_long and offer.bot_exchange_short else 0,
            abs(offer.funding_diff_pct_per_hour_at()),
            abs(offer.open_spread_pct),
        )

    return max(matches, key=score)


def _apply_offer_history_exchanges(payload: dict, offer: ScanOffer | None) -> None:
    if offer is None:
        return
    payload["source_long_exchange"] = offer.source_exchange_long
    payload["source_short_exchange"] = offer.source_exchange_short
    payload["long_exchange"] = offer.bot_exchange_long or offer.source_exchange_long
    payload["short_exchange"] = offer.bot_exchange_short or offer.source_exchange_short


def _apply_pair_history_exchanges(payload: dict, pair, offer: ScanOffer | None) -> None:
    """Override payload exchanges from a discovered UACI pair when no
    scanner offer was matched. Bot ids preferred so the dashboard chart
    can plot on the right datasets."""
    if pair is None or offer is not None:
        return
    from src.scanners.exchange_map import map_source_to_bot

    payload["source_long_exchange"] = pair.long_exchange
    payload["source_short_exchange"] = pair.short_exchange
    payload["long_exchange"] = map_source_to_bot(pair.long_exchange) or pair.long_exchange
    payload["short_exchange"] = map_source_to_bot(pair.short_exchange) or pair.short_exchange


def _payload_float(value, default: float) -> float:
    if value is None or value == "":
        return default
    return float(value)


def _payload_bool(value, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
    raise ValueError("notify_telegram must be boolean")


class ScannerController:
    """Routes under ``/api/scanners/...``."""

    PREFIX = "/api/scanners"

    def __init__(self, service: ScannerService) -> None:
        self._service = service
        self._discovery = None  # lazy: built on first symbol miss

    def _get_discovery(self):
        if self._discovery is None:
            from src.scanners.uacryptoinvest.discovery import UACryptoInvestPairDiscovery
            self._discovery = UACryptoInvestPairDiscovery()
        return self._discovery

    def register_routes(self, app: web.Application) -> None:
        r = app.router
        p = self.PREFIX
        r.add_get(f"{p}/health", self.health)
        r.add_get(f"{p}/offers", self.offers)
        r.add_get(f"{p}/variants", self.variants)
        r.add_get(f"{p}/chart", self.chart)
        r.add_get(f"{p}/chart_options", self.chart_options)
        r.add_get(f"{p}/filter", self.filter_get)
        r.add_put(f"{p}/filter", self.filter_put)

    # ── Handlers ──────────────────────────────────────────────────────

    async def health(self, request: web.Request) -> web.Response:
        h = self._service.health.to_dict()
        h["sources"] = self._service.sources
        return _json_response(h)

    async def offers(self, request: web.Request) -> web.Response:
        source = request.query.get("source") or None
        offers = self._service.snapshot(source)
        return _json_response({
            "count": len(offers),
            "offers": [_offer_dict(o) for o in offers],
        })

    async def variants(self, request: web.Request) -> web.Response:
        symbol = (request.query.get("symbol") or request.query.get("token") or "").strip()
        token = _symbol_token(symbol)
        if not token:
            return _json_response({"error": "symbol or token is required"}, status=400)
        try:
            limit = max(1, min(80, int(float(request.query.get("limit") or 40))))
        except (TypeError, ValueError):
            limit = 40
        try:
            from src.scanners.uacryptoinvest.arbitrage import UACryptoInvestArbitrageClient

            client = UACryptoInvestArbitrageClient(
                base_url=os.environ.get("UACRYPTOINVEST_BASE_URL") or "https://uacryptoinvest.com",
                timeout_sec=float(os.environ.get("UACRYPTOINVEST_DISCOVERY_TIMEOUT_SEC", "25") or 25),
            )
            offers = await client.fetch_token_offers(token, limit=limit)
        except Exception as e:  # noqa: BLE001
            return _json_response({"error": f"variants failed: {e}"}, status=502)
        return _json_response({
            "symbol": symbol,
            "token": token,
            "count": len(offers),
            "offers": [_offer_dict(o) for o in offers],
        })

    async def chart_options(self, request: web.Request) -> web.Response:
        symbol = (request.query.get("symbol") or request.query.get("token") or "").strip()
        token = _symbol_token(symbol)
        if not token:
            return _json_response({"error": "symbol or token is required"}, status=400)
        try:
            options = await self._get_discovery().discover_options(token)
        except Exception as e:  # noqa: BLE001
            return _json_response({"error": f"chart options failed: {e}"}, status=502)
        return _json_response({
            "symbol": symbol,
            "token": token,
            "count": len(options),
            "options": [option.to_dict() for option in options],
        })

    async def chart(self, request: web.Request) -> web.Response:
        chart_code = (request.query.get("chart") or "").strip()
        resolved_offer: ScanOffer | None = None
        if not chart_code:
            token = (request.query.get("token") or "").strip()
            long_exchange = (request.query.get("long") or "").strip()
            short_exchange = (request.query.get("short") or "").strip()
            if token and long_exchange and short_exchange:
                chart_code = f"{token}:{long_exchange}:{short_exchange}"
        symbol = (request.query.get("symbol") or "").strip()
        discovered_pair = None
        if symbol:
            resolved_offer = _best_chart_offer_for_symbol(self._service.snapshot(), symbol)
            if not chart_code and resolved_offer is not None:
                chart_code = _offer_chart_code(resolved_offer)
            if not chart_code and resolved_offer is None:
                try:
                    discovered_pair = await self._get_discovery().discover(_symbol_token(symbol))
                except Exception:  # noqa: BLE001
                    discovered_pair = None
                if discovered_pair is not None:
                    chart_code = discovered_pair.chart_code
        if not chart_code:
            return _json_response({
                "error": "chart, symbol, or token+long+short, is required",
            }, status=400)

        try:
            from src.scanners.uacryptoinvest.client import UACryptoInvestClient
            from src.scanners.uacryptoinvest.config import parse_pairs

            pair = parse_pairs(chart_code)[0]
            older_than_raw = request.query.get("older_than")
            older_than = int(float(older_than_raw)) if older_than_raw else None
            interval = max(1, int(float(request.query.get("interval") or 1)))
            range_raw = request.query.get("range_sec") or request.query.get("lookback_sec") or 86400
            range_sec = max(60, min(604800, int(float(range_raw))))
            client = UACryptoInvestClient(base_url=pair.base_url)
            try:
                payload = await client.chart_history(
                    pair, older_than=older_than, interval=interval, range_sec=range_sec,
                )
            finally:
                await client.aclose()
            _apply_offer_history_exchanges(payload, resolved_offer)
            _apply_pair_history_exchanges(payload, discovered_pair or pair, resolved_offer)
            return _json_response(payload)
        except ValueError as e:
            return _json_response({"error": str(e)}, status=400)
        except Exception as e:  # noqa: BLE001
            return _json_response({"error": f"chart history failed: {e}"}, status=502)

    async def filter_get(self, request: web.Request) -> web.Response:
        flt = await self._service.filter_store.get()
        return _json_response(flt.to_dict())

    async def filter_put(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json(loads=orjson.loads)
        except Exception:  # noqa: BLE001
            return _json_response({"error": "invalid json"}, status=400)
        try:
            current = await self._service.filter_store.get()
            new_flt = ScannerFilter(
                min_spread_pct=max(
                    0.0,
                    _payload_float(
                        payload.get("min_spread_pct"), current.min_spread_pct,
                    ),
                ),
                min_funding_diff_pct_per_hour=max(
                    0.0,
                    _payload_float(
                        payload.get("min_funding_diff_pct_per_hour"),
                        current.min_funding_diff_pct_per_hour,
                    ),
                ),
                notify_telegram=_payload_bool(
                    payload.get("notify_telegram"), current.notify_telegram,
                ),
                renotify_funding_change_pct=max(
                    0.0,
                    _payload_float(
                        payload.get("renotify_funding_change_pct"),
                        current.renotify_funding_change_pct,
                    ),
                ),
                renotify_spread_change_pct=max(
                    0.0,
                    _payload_float(
                        payload.get("renotify_spread_change_pct"),
                        current.renotify_spread_change_pct,
                    ),
                ),
            )
        except (TypeError, ValueError) as e:
            return _json_response({"error": f"bad params: {e}"}, status=400)
        saved = await self._service.filter_store.set(new_flt)
        return _json_response(saved.to_dict())
