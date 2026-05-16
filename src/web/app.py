"""aiohttp web application — async, non-blocking."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aiohttp import web
from loguru import logger

from src.web.ports import OrchestratorControlPort

if TYPE_CHECKING:
    from src.adapters.exchange.stream_manager import ExchangeStreamManager
    from src.adapters.storage import SpreadStorage
    from src.config import AppConfig
    from src.domain.models import ArbitrageState
    from src.domain.trader import ArbitrageTrader


class WebApp:
    """Wraps aiohttp app creation and lifecycle."""

    def __init__(
        self,
        states: dict[str, "ArbitrageState"],
        storage: "SpreadStorage",
        config_dict: dict | None = None,
        trader: "ArbitrageTrader | None" = None,
        streams: "ExchangeStreamManager | None" = None,
        loop_monitor=None,
        config: "AppConfig | None" = None,
        orchestrator: OrchestratorControlPort | None = None,
        scanner_service=None,
    ):
        self._states = states
        self._storage = storage
        self._config_dict = config_dict or {}
        self._trader = trader
        self._streams = streams
        self._loop_monitor = loop_monitor
        self._config = config
        self._orchestrator = orchestrator
        self._scanner_service = scanner_service
        self.app = self._create_app()
        self._runner: web.AppRunner | None = None

    def _create_app(self) -> web.Application:
        app = web.Application(middlewares=[self._main_middleware])

        from src.web.api import ApiController

        controller = ApiController(
            self._states,
            self._storage,
            self._config_dict,
            trader=self._trader,
            streams=self._streams,
            loop_monitor=self._loop_monitor,
            config_obj=self._config,
            orchestrator=self._orchestrator,
        )
        controller.register_routes(app)
        # Optional: register scanner routes only when service is wired.
        # Keeping the import inside the branch means a missing
        # ``src/scanners/`` package never breaks dashboard startup.
        if self._scanner_service is not None:
            from src.scanners.api import ScannerController
            ScannerController(self._scanner_service).register_routes(app)
        return app

    @web.middleware
    async def _main_middleware(self, request: web.Request, handler):
        import time as _time

        # Token check
        token = self._config_dict.get("web_token", "")
        if token and request.query.get("token") != token:
            raise web.HTTPForbidden()

        t0 = _time.monotonic()
        response = await handler(request)
        elapsed = (_time.monotonic() - t0) * 1000

        # No-cache headers for API/dashboard (prevent stale browser cache)
        if request.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
            response.headers["Access-Control-Allow-Origin"] = "*"
        elif request.path in ("/", "/dashboard_v2"):
            response.headers["Cache-Control"] = "no-store"

        # Log slow requests
        if elapsed > 500:
            logger.warning("Slow request: {} {} {:.0f}ms", request.method, request.path, elapsed)

        return response

    async def start(self, host: str = "0.0.0.0", port: int = 5000) -> None:
        """Start the web server as an async task (non-blocking)."""
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, host, port)
        await site.start()
        logger.info("Dashboard: http://{}:{}", host, port)

    async def stop(self) -> None:
        """Graceful shutdown."""
        if self._runner:
            await self._runner.cleanup()
