"""Coordinates independent ExchangeConnection instances.

Each exchange runs as its own async loop — failures are isolated.
"""

from __future__ import annotations

import asyncio
from typing import Optional

from loguru import logger

from src.adapters.exchange.connection_factory import create_exchange_connection
from src.adapters.exchange.symbol_quarantine import SymbolQuarantine
from src.adapters.http import HttpClient
from src.settings import DEFAULT_LIMIT_PRICE_SLIPPAGE_PCT
from src.adapters.exchange.connection import (
    ExchangeConnection,
    OnFundingCallback,
    OnTickCallback,
    OnVolumeCallback,
)
from src.config import AppConfig, ExchangeConfig


class ExchangeStreamManager:
    """Coordinates independent ExchangeConnection instances.

    Each exchange runs as its own async loop — failures are isolated.
    """

    def __init__(
        self,
        configs: list[ExchangeConfig],
        app_config: AppConfig | None = None,
        http: HttpClient | None = None,
    ):
        self._configs = configs
        self._app_config = app_config
        self._http = http
        self._connections: list[ExchangeConnection] = []
        self._symbol_quarantine: SymbolQuarantine | None = None
        if app_config is not None:
            quarantine_path = getattr(app_config, "symbol_quarantine_path", "")
            if quarantine_path:
                self._symbol_quarantine = SymbolQuarantine(quarantine_path)
        self._trade_exchanges: set[str] = set()
        self._leverage: int = 0
        self._margin_mode: str = ""
        # Hot-restart support (set on first start())
        self._symbols: list[str] = []
        self._on_tick: OnTickCallback | None = None
        self._on_funding: OnFundingCallback | None = None
        self._on_volume: OnVolumeCallback | None = None
        self._funding_poll_sec: float = 60.0
        self._volume_poll_sec: float = 300.0
        self._restart_signal: asyncio.Event | None = None
        self._task_connections: dict[asyncio.Task, ExchangeConnection] = {}

    async def start(
        self,
        symbols: list[str],
        on_tick: OnTickCallback,
        on_funding: OnFundingCallback,
        funding_poll_sec: float = 60.0,
        trade_exchanges: list[str] | None = None,
        leverage: int = 0,
        margin_mode: str = "",
        on_volume: OnVolumeCallback | None = None,
        volume_poll_sec: float = 300.0,
    ) -> None:
        # Cache args so replace_symbols() can re-spawn connections.
        self._symbols = list(symbols)
        self._on_tick = on_tick
        self._on_funding = on_funding
        self._on_volume = on_volume
        self._funding_poll_sec = funding_poll_sec
        self._volume_poll_sec = volume_poll_sec
        self._trade_exchanges = set(trade_exchanges or [])
        self._leverage = leverage
        self._margin_mode = margin_mode
        self._restart_signal = asyncio.Event()

        # Outer loop: spawn connections; on restart signal, stop and respawn.
        while True:
            self._restart_signal.clear()
            await self._spawn_connections()

            if not self._connections:
                logger.warning(
                    "No active exchange streams; waiting for config changes"
                )
                if self._restart_signal is None:
                    return
                try:
                    await self._restart_signal.wait()
                except asyncio.CancelledError:
                    logger.info("Stream manager cancelled while idle")
                    raise
                continue

            restart_task = asyncio.create_task(
                self._restart_signal.wait(), name="stream-restart-wait"
            )
            active_tasks = set(self._conn_tasks)

            try:
                while active_tasks:
                    done, _pending = await asyncio.wait(
                        active_tasks | {restart_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if restart_task in done:
                        # Hot restart requested: stop existing connections cleanly,
                        # then loop to spawn with the updated _symbols list.
                        logger.info(
                            "Stream restart requested -> rebuilding with symbols={}",
                            self._symbols,
                        )
                        for task in active_tasks:
                            task.cancel()
                        await asyncio.gather(*active_tasks, return_exceptions=True)
                        await self._stop_connections()
                        break

                    for task in done & active_tasks:
                        active_tasks.remove(task)
                        await self._handle_connection_task_done(task)
                else:
                    # Every exchange stream ended. Stay alive and wait for a
                    # config change so the operator can fix symbols/keys via
                    # the dashboard without a watchdog respawn cycle.
                    logger.warning(
                        "All exchange streams ended early; idling until config changes"
                    )
                    await self._stop_connections()
                    try:
                        await restart_task
                    except (asyncio.CancelledError, Exception):
                        pass
                    continue
            except asyncio.CancelledError:
                logger.info("Stream tasks cancelled")
                for task in active_tasks:
                    task.cancel()
                restart_task.cancel()
                await self._stop_connections()
                raise

            continue

    async def _spawn_connections(self) -> None:
        ac = self._app_config
        self._connections = []

        self._conn_tasks: list[asyncio.Task] = []
        self._task_connections = {}
        for cfg in self._configs:
            symbols_for_exchange = self._symbols_for_exchange(cfg.id)
            if not symbols_for_exchange:
                logger.info(
                    "Skipping {} stream: no symbols enabled for read",
                    cfg.id,
                )
                continue
            conn = create_exchange_connection(
                cfg,
                ws_timeout_sec=ac.ws_timeout_sec if ac else 10.0,
                ws_max_backoff_sec=ac.ws_max_backoff_sec if ac else 60.0,
                market_load_retries=ac.market_load_retries if ac else 3,
                limit_price_slippage_pct=(
                    ac.trading.limit_price_slippage_pct if ac else DEFAULT_LIMIT_PRICE_SLIPPAGE_PCT
                ),
                http=self._http,
                symbol_quarantine=self._symbol_quarantine,
            )
            self._connections.append(conn)
            is_trade = conn.exchange_id in self._trade_exchanges
            task = asyncio.create_task(
                conn.start(
                    symbols_for_exchange,
                    self._on_tick,
                    self._on_funding,
                    self._funding_poll_sec,
                    leverage=self._leverage if is_trade else 0,
                    margin_mode=self._margin_mode if is_trade else "",
                    validation_symbols=list(self._symbols),
                    on_volume=self._on_volume,
                    volume_poll_sec=self._volume_poll_sec,
                ),
                name=f"exchange-{conn.exchange_id}",
            )
            self._conn_tasks.append(task)
            self._task_connections[task] = conn

    async def _handle_connection_task_done(self, task: asyncio.Task) -> None:
        conn = self._task_connections.pop(task, None)
        exchange_id = conn.exchange_id if conn is not None else task.get_name()
        try:
            task.result()
            logger.warning("Exchange stream ended: {}", exchange_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Exchange stream failed: {}: {}", exchange_id, exc)
        if conn is not None:
            try:
                await conn.stop()
            except Exception as exc:  # pragma: no cover — defensive
                logger.warning("Error stopping {}: {}", conn.exchange_id, exc)
            if conn in self._connections:
                self._connections.remove(conn)

    def _symbols_for_exchange(self, exchange_id: str) -> list[str]:
        """Return symbols this exchange should actually subscribe to.

        Per-symbol read_exchanges is not just a UI/trader filter: it also
        trims websocket subscriptions, so disabling read for a symbol/exchange
        stops spending network and CPU on that feed.
        """
        if self._app_config is None:
            return list(self._symbols)

        result: list[str] = []
        for symbol in self._symbols:
            overrides = self._app_config.symbol_overrides(symbol)
            if "read_exchanges" in overrides:
                read_exchanges = set(overrides.get("read_exchanges") or [])
                if exchange_id not in read_exchanges:
                    continue
            result.append(symbol)
        return result

    async def _stop_connections(self) -> None:
        for conn in self._connections:
            try:
                await conn.stop()
            except Exception as e:  # pragma: no cover — defensive
                logger.warning("Error stopping {}: {}", conn.exchange_id, e)
        self._connections.clear()

    async def replace_symbols(self, new_symbols: list[str]) -> None:
        """Hot-replace the active symbol set without restarting the process.

        The outer loop in `start()` observes the restart signal, stops all
        connections cleanly, and re-spawns them with the new symbol set.
        Web/trader/storage state remain alive — only WS subscriptions cycle.
        """
        self._symbols = list(new_symbols)
        if self._restart_signal is not None:
            self._restart_signal.set()

    async def stop(self) -> None:
        for conn in self._connections:
            await conn.stop()
        self._connections.clear()

    def get_connection(self, exchange_id: str) -> Optional[ExchangeConnection]:
        """Get a live connection by exchange ID."""
        for conn in self._connections:
            if conn.exchange_id == exchange_id:
                return conn
        return None

    async def reconnect_exchange(self, exchange_id: str) -> dict:
        """Request a reconnect for one live exchange connection."""
        conn = self.get_connection(exchange_id)
        if conn is None:
            return {"exchange_id": exchange_id, "reconnect": False, "error": "not connected"}
        ok = await conn.reconnect()
        result = {"exchange_id": exchange_id, "reconnect": bool(ok)}
        if not ok:
            result["error"] = "manual reconnect unavailable"
        return result

    def get_all_stats(self) -> list[dict]:
        """Return data quality stats for all connections."""
        result = []
        for conn in self._connections:
            s = conn.stats()
            s["is_trade"] = conn.exchange_id in self._trade_exchanges
            result.append(s)
        return result

    @property
    def symbol_quarantine(self) -> SymbolQuarantine | None:
        """Public accessor for the shared quarantine registry (read-only port)."""
        return self._symbol_quarantine

    def iter_loaded_markets(self):
        """Yield (exchange_id, symbol, market_dict) for every loaded market.

        Public iterator over ccxt.pro markets across all connected
        exchanges — kept on the manager so callers don't need to touch
        ExchangeConnection internals.
        """
        for conn in self._connections:
            for sym, m in conn.loaded_markets().items():
                yield conn.exchange_id, sym, m
