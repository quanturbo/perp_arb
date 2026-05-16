"""JSON API controller — aiohttp handlers (fully async, non-blocking)."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

import orjson
from aiohttp import web

from src.adapters.exchange.direct_order import direct_order_support_snapshot
from src.adapters.exchange.symbol_quarantine import REASON_MANUAL_FAKE_SIGNAL
from src.web.ports import OrchestratorControlPort

if TYPE_CHECKING:
    from src.adapters.exchange.stream_manager import ExchangeStreamManager
    from src.adapters.loop_monitor import EventLoopMonitor
    from src.adapters.storage import SpreadStorage
    from src.config import AppConfig
    from src.domain.models import ArbitrageState
    from src.domain.trader import ArbitrageTrader

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


def _json_response(
    data: Any, status: int = 200, request: web.Request | None = None,
) -> web.Response:
    """Fast JSON response using orjson (no app-level gzip — reverse proxy handles it)."""
    body = orjson.dumps(data)
    return web.Response(
        body=body, status=status, content_type="application/json",
    )


class ApiController:
    """Encapsulates all API routes with explicit deps."""

    def __init__(
        self,
        states: dict[str, "ArbitrageState"],
        storage: "SpreadStorage",
        config: dict,
        trader: "ArbitrageTrader | None" = None,
        streams: "ExchangeStreamManager | None" = None,
        loop_monitor: "EventLoopMonitor | None" = None,
        config_obj: "AppConfig | None" = None,
        orchestrator: OrchestratorControlPort | None = None,
    ):
        self._states = states
        self._storage = storage
        self._config = config
        self._trader = trader
        self._streams = streams
        self._loop_monitor = loop_monitor
        self._template_cache: dict[str, str] = {}
        # Composition: delegate config-edit handlers to a focused controller
        # so this class stays a router/composer, not a god-class.
        self._config_ctl = None
        if config_obj is not None and trader is not None and orchestrator is not None:
            from src.web.controllers.config_controller import ConfigController
            self._config_ctl = ConfigController(
                config=config_obj,
                trader=trader,
                orchestrator=orchestrator,
                config_dict=config,
            )

    def register_routes(self, app: web.Application) -> None:
        """Register all routes on the aiohttp Application."""
        app.router.add_get("/", self.dashboard)
        app.router.add_get("/dashboard_v2", self.dashboard_v2)
        app.router.add_get("/api/markets/search", self.api_markets_search)
        app.router.add_get("/api/state", self.api_state)
        app.router.add_get("/api/history", self.api_history)
        app.router.add_get("/api/funding_history", self.api_funding_history)
        app.router.add_get("/api/exchanges", self.api_exchanges)
        app.router.add_get("/api/trader", self.api_trader)
        app.router.add_post("/api/trader/reset_session", self.api_trader_reset_session)
        app.router.add_post("/api/toggle_exchange", self.api_toggle_exchange)
        app.router.add_post("/api/exchanges/{exchange_id}/reconnect", self.api_exchange_reconnect)
        app.router.add_get("/api/exchange_stats", self.api_exchange_stats)
        app.router.add_get("/api/max_spread", self.api_max_spread)
        app.router.add_get("/api/deals", self.api_deals)
        app.router.add_post("/api/force_close_deal", self.api_force_close_deal)
        app.router.add_get("/api/health", self.api_health)
        app.router.add_get("/api/config", self.api_config)
        app.router.add_post("/api/config", self.api_config_post)
        app.router.add_post("/api/config/apply_defaults", self.api_config_apply_defaults)
        app.router.add_post("/api/symbols/add", self.api_symbols_add)
        app.router.add_post("/api/symbols/remove", self.api_symbols_remove)
        app.router.add_get("/api/quarantine", self.api_quarantine_list)
        app.router.add_post("/api/quarantine/reinstate", self.api_quarantine_reinstate)
        app.router.add_post("/api/quarantine/manual_fake", self.api_quarantine_manual_fake)

    def _render_template(self, filename: str) -> str:
        """Render an HTML template with config substitution (cached per file).

        Both the classic dashboard and dashboard_v2 share the same
        Jinja-style placeholder set, so a single render path is enough.
        """
        cached = self._template_cache.get(filename)
        if cached is not None:
            return cached
        path = os.path.join(_TEMPLATE_DIR, filename)
        with open(path, encoding="utf-8") as f:
            html = f.read()
        substitutions = {
            "{{ poll_state_ms | default(1000) }}":
                str(self._config.get("web_poll_state_ms", 1000)),
            "{{ poll_history_ms | default(10000) }}":
                str(self._config.get("web_poll_history_ms", 10000)),
            "{{ dashboard_max_latency_ms | default(1000) }}":
                str(self._config.get("dashboard_max_latency_ms", 1000)),
            "{{ dashboard_exchange_latency_ms_json | default('{}') | safe }}":
                json.dumps(self._config.get("dashboard_exchange_latency_ms", {})),
        }
        for placeholder, value in substitutions.items():
            html = html.replace(placeholder, value)
        self._template_cache[filename] = html
        return html

    async def dashboard(self, request: web.Request) -> web.Response:
        return web.Response(
            text=self._render_template("dashboard_v2.html"),
            content_type="text/html",
        )

    async def dashboard_v2(self, request: web.Request) -> web.Response:
        return web.Response(
            text=self._render_template("dashboard_v2.html"),
            content_type="text/html",
        )

    async def api_markets_search(self, request: web.Request) -> web.Response:
        """Return USDT-perp symbols available across loaded exchanges.

        Used by the v2 dashboard symbol picker. Reads markets through the
        public ExchangeStreamManager iterator — no private attribute access.
        """
        q = (request.query.get("q", "") or "").strip().upper()
        try:
            limit = max(1, min(500, int(request.query.get("limit", "100"))))
        except ValueError:
            limit = 100

        index: dict[str, list[str]] = {}
        if self._streams is not None:
            for ex_id, sym, m in self._streams.iter_loaded_markets():
                if not isinstance(m, dict):
                    continue
                if m.get("type") != "swap":
                    continue
                if m.get("settle") != "USDT" and m.get("quote") != "USDT":
                    continue
                if m.get("linear") is False:
                    continue
                index.setdefault(sym, []).append(ex_id)

        if q:
            items = [(s, ex) for s, ex in index.items() if q in s.upper()]
        else:
            items = list(index.items())
        items.sort(key=lambda kv: (-len(kv[1]), kv[0]))
        out = [{"symbol": s, "exchanges": ex} for s, ex in items[:limit]]
        return _json_response(
            {"markets": out, "total": len(items)}, request=request,
        )

    async def api_state(self, request: web.Request) -> web.Response:
        result: dict[str, Any] = {}
        for symbol, state in self._states.items():
            result[symbol] = state.to_dict()
        if self._trader:
            result["_trader"] = self._trader.to_dict()
        return _json_response(result, request=request)

    async def api_history(self, request: web.Request) -> web.Response:
        symbol = request.query.get("symbol", "")
        limit = self._parse_limit(request, 300, 300)
        since = self._parse_since(request)
        bucket_sec = self._parse_int(request, "bucket", 0)
        slim = request.query.get("slim", "") == "1"
        rows = await self._storage.get_history(
            symbol, limit, since=since, bucket_sec=bucket_sec, slim=slim
        )
        return _json_response(rows, request=request)

    async def api_funding_history(self, request: web.Request) -> web.Response:
        symbol = request.query.get("symbol", "")
        limit = self._parse_limit(request, 500, 500)
        since = self._parse_since(request)
        rows = await self._storage.get_funding_history(symbol, limit, since=since)
        return _json_response(rows, request=request)

    async def api_exchanges(self, request: web.Request) -> web.Response:
        exchanges = self._config.get("exchanges_info", [])
        result = []
        for ex in exchanges:
            ex_id = ex["id"]
            has_data = any(ex_id in state.ticks for state in self._states.values())
            result.append(
                {
                    "id": ex_id,
                    "enabled": ex.get("enabled", True),
                    "connected": has_data,
                    "direct_order": direct_order_support_snapshot(
                        ex_id,
                        enabled=bool(ex.get("direct_order_ws")),
                    ),
                }
            )
        return _json_response(result, request=request)

    async def api_trader(self, request: web.Request) -> web.Response:
        if not self._trader:
            return _json_response({"enabled": False, "state": "disabled"}, request=request)
        return _json_response(self._trader.to_dict(), request=request)

    async def api_trader_reset_session(self, request: web.Request) -> web.Response:
        if not self._trader:
            return _json_response({"error": "trader unavailable"}, status=503)
        result = self._trader.reset_trade_session()
        if "error" in result:
            return _json_response(result, status=409)
        return _json_response(result, request=request)

    async def api_config(self, request: web.Request) -> web.Response:
        """Live trading-config snapshot for dashboard header."""
        if self._config_ctl is not None:
            return await self._config_ctl.get(request)
        cfg = self._config.get("trading_config", {})
        return _json_response(cfg, request=request)

    async def api_config_post(self, request: web.Request) -> web.Response:
        if self._config_ctl is None:
            return _json_response({"error": "config edits unavailable"}, status=503)
        return await self._config_ctl.post(request)

    async def api_config_apply_defaults(self, request: web.Request) -> web.Response:
        if self._config_ctl is None:
            return _json_response({"error": "config edits unavailable"}, status=503)
        return await self._config_ctl.apply_defaults(request)

    async def api_symbols_add(self, request: web.Request) -> web.Response:
        if self._config_ctl is None:
            return _json_response({"error": "config edits unavailable"}, status=503)
        return await self._config_ctl.add_symbol(request)

    async def api_symbols_remove(self, request: web.Request) -> web.Response:
        if self._config_ctl is None:
            return _json_response({"error": "config edits unavailable"}, status=503)
        return await self._config_ctl.remove_symbol(request)

    async def api_toggle_exchange(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
        except Exception:
            data = {}
        exchange_id = data.get("exchange_id", "")
        enabled = data.get("enabled", True)
        if not exchange_id:
            return _json_response({"error": "exchange_id required"}, status=400)

        disabled: set = self._config.setdefault("disabled_exchanges", set())
        if enabled:
            disabled.discard(exchange_id)
            if self._trader:
                self._trader.reset_exchange_failures(exchange_id)
        else:
            disabled.add(exchange_id)

        for ex in self._config.get("exchanges_info", []):
            if ex["id"] == exchange_id:
                ex["enabled"] = enabled

        return _json_response({"exchange_id": exchange_id, "enabled": enabled}, request=request)

    async def api_exchange_stats(self, request: web.Request) -> web.Response:
        if not self._streams:
            return _json_response([], request=request)
        return _json_response(self._streams.get_all_stats(), request=request)

    async def api_exchange_reconnect(self, request: web.Request) -> web.Response:
        exchange_id = request.match_info.get("exchange_id", "")
        if not exchange_id:
            return _json_response({"error": "exchange_id required"}, status=400)
        if not self._streams:
            return _json_response({"error": "streams unavailable"}, status=503)
        result = await self._streams.reconnect_exchange(exchange_id)
        if result.get("error"):
            return _json_response(result, status=404)
        if self._trader:
            self._trader.reset_exchange_failures(exchange_id)
        return _json_response(result, request=request)

    async def api_health(self, request: web.Request) -> web.Response:
        """Expose event-loop stall stats + aggregate WS reconnect summary.

        Dashboard can use this for a single-pane-of-glass runtime health view.
        """
        loop = self._loop_monitor.to_dict() if self._loop_monitor else {}
        ws_summary: list[dict] = []
        if self._streams:
            for stats in self._streams.get_all_stats():
                ws = stats.get("ws") or {}
                if ws:
                    ws_summary.append({
                        "exchange_id": stats.get("exchange_id"),
                        "reconnect_count": ws.get("reconnect_count", 0),
                        "reconnects_recent": ws.get("reconnects_recent", 0),
                        "uptime_sec": ws.get("uptime_sec"),
                        "last_disconnect_reason": ws.get("last_disconnect_reason"),
                    })
        return _json_response(
            {"event_loop": loop, "ws": ws_summary}, request=request,
        )

    async def api_quarantine_list(self, request: web.Request) -> web.Response:
        """List quarantined symbols grouped by exchange."""
        q = self._streams.symbol_quarantine if self._streams else None
        if q is None:
            return _json_response({"quarantined": {}}, request=request)
        snapshot = q.snapshot()
        requested_symbol = self._normalize_quarantine_symbol(request.query.get("symbol") or "")
        if requested_symbol:
            snapshot = {
                exchange: {
                    symbol: meta
                    for symbol, meta in symbols.items()
                    if symbol == requested_symbol
                }
                for exchange, symbols in snapshot.items()
            }
            snapshot = {exchange: symbols for exchange, symbols in snapshot.items() if symbols}
            return _json_response({"quarantined": snapshot}, request=request)
        configured_symbols = set((self._config.get("trading_config") or {}).get("symbols") or [])
        if configured_symbols:
            snapshot = {
                exchange: {
                    symbol: meta
                    for symbol, meta in symbols.items()
                    if symbol in configured_symbols
                }
                for exchange, symbols in snapshot.items()
            }
            snapshot = {exchange: symbols for exchange, symbols in snapshot.items() if symbols}
        return _json_response({"quarantined": snapshot}, request=request)

    async def api_quarantine_reinstate(self, request: web.Request) -> web.Response:
        """Reinstate one symbol on one exchange. Body: {exchange, symbol}."""
        q = self._streams.symbol_quarantine if self._streams else None
        if q is None:
            return _json_response({"error": "quarantine unavailable"}, status=503)
        try:
            data = await request.json()
        except Exception:
            data = {}
        exchange = (data.get("exchange") or "").strip()
        symbol = (data.get("symbol") or "").strip()
        if not exchange or not symbol:
            return _json_response(
                {"error": "exchange and symbol required"}, status=400,
            )
        ok = q.reinstate(exchange, symbol)
        if not ok:
            return _json_response(
                {"error": "not quarantined", "exchange": exchange, "symbol": symbol},
                status=404,
            )
        return _json_response(
            {"reinstated": True, "exchange": exchange, "symbol": symbol},
            request=request,
        )

    async def api_quarantine_manual_fake(self, request: web.Request) -> web.Response:
        """Persist an operator-marked fake scanner signal."""
        q = self._streams.symbol_quarantine if self._streams else None
        if q is None:
            return _json_response({"error": "quarantine unavailable"}, status=503)
        try:
            data = await request.json()
        except Exception:
            data = {}
        symbol = self._normalize_quarantine_symbol(data.get("symbol") or "")
        exchanges = [str(x).strip() for x in (data.get("exchanges") or []) if str(x).strip()]
        detail = str(data.get("detail") or "manual fake scanner signal")[:240]
        if not symbol or not exchanges:
            return _json_response(
                {"error": "symbol and exchanges required"}, status=400,
            )
        recorded = []
        for exchange in sorted(set(exchanges)):
            q.record_manual_fake_signal(exchange, symbol, detail=detail)
            recorded.append({"exchange": exchange, "symbol": symbol})
        return _json_response(
            {
                "recorded": recorded,
                "reason": REASON_MANUAL_FAKE_SIGNAL,
            },
            request=request,
        )

    @staticmethod
    def _normalize_quarantine_symbol(symbol: str) -> str:
        text = str(symbol or "").strip().upper()
        if "/" in text and ":" in text:
            return text
        if text.endswith("USDT") and len(text) > 4:
            return f"{text[:-4]}/USDT:USDT"
        return text

    async def api_max_spread(self, request: web.Request) -> web.Response:
        symbol = request.query.get("symbol", "")
        hours = 8.0
        try:
            hours = float(request.query.get("hours", 8.0))
        except (ValueError, TypeError):
            pass
        hours = min(hours, 720)
        result = await self._storage.get_max_spread(symbol, hours)
        if result is None:
            return _json_response(
                {"hours": hours, "symbol": symbol, "max_spread": None}
            )
        result["hours"] = hours
        return _json_response(result, request=request)

    async def api_deals(self, request: web.Request) -> web.Response:
        symbol = request.query.get("symbol", "")
        limit = self._parse_limit(request, 100, 500)
        offset = self._parse_int(request, "offset", 0)
        rows = await self._storage.get_deals(symbol, limit, offset)
        total = await self._storage.get_deals_count(symbol)
        cumulative = await self._storage.get_deals_cumulative(
            symbol,
            deal_ids={row["id"] for row in rows},
        )
        return _json_response(
            {
                "deals": rows,
                "total": total,
                "offset": offset,
                "limit": limit,
                "cumulative": cumulative,
            },
            request=request,
        )

    async def api_force_close_deal(self, request: web.Request) -> web.Response:
        """Mark an open deal as manually closed (positions closed outside bot)."""
        try:
            data = await request.json()
        except Exception:
            data = {}
        deal_id = data.get("deal_id")
        if not deal_id:
            return _json_response({"error": "deal_id required"}, status=400)
        try:
            deal_id = int(deal_id)
        except (ValueError, TypeError):
            return _json_response({"error": "deal_id must be integer"}, status=400)

        await self._storage.force_close_deal(deal_id)

        # Reset trader state if it's tracking this deal
        if self._trader and self._trader.current_deal_id == deal_id:
            self._trader.clear_position()

        return _json_response({"ok": True, "deal_id": deal_id})

    # ── Helpers ──

    @staticmethod
    def _parse_limit(request: web.Request, default: int, maximum: int) -> int:
        try:
            return min(int(request.query.get("limit", default)), maximum)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _parse_since(request: web.Request) -> float | None:
        raw = request.query.get("since")
        if raw is None:
            return None
        try:
            return float(raw)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_int(request: web.Request, name: str, default: int) -> int:
        try:
            return int(request.query.get(name, default))
        except (ValueError, TypeError):
            return default
