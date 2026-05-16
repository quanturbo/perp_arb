"""Per-exchange CCXT connection lifecycle and stream orchestration."""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Optional

import ccxt.pro as ccxtpro
from loguru import logger

from src.adapters.exchange.clock_sync import ClockSync
from src.adapters.exchange.connection_http import patch_exchange_ipv4
from src.adapters.exchange.connection_stats import build_connection_stats
from src.adapters.exchange.direct_order import (
    DirectOrderClient,
    create_direct_order_client,
)
from src.adapters.exchange.funding_poller import FundingPoller
from src.adapters.exchange.market_loader import (
    create_exchange,
    load_and_validate,
    validate_loaded_symbols,
)
from src.adapters.exchange import order_execution
from src.adapters.exchange.raw_watchdog import run_raw_symbol_stall_watchdog
from src.adapters.exchange.symbol_quarantine import SymbolQuarantine
from src.adapters.exchange.tick_tracker import TickTracker
from src.adapters.exchange.volume_poller import VolumePoller
from src.adapters.http import HttpClient
from src.adapters.ws.base import RawTick, RawTickerStream
from src.adapters.ws.factory import create_raw_stream
from src.config import ExchangeConfig
from src.domain.models import FundingInfo, OrderResult, PriceTick, VolumeInfo
from src.settings import DEFAULT_LIMIT_PRICE_SLIPPAGE_PCT

OnTickCallback = Callable[[PriceTick], Awaitable[None]]
OnFundingCallback = Callable[[FundingInfo], Awaitable[None]]
OnVolumeCallback = Callable[[VolumeInfo], Awaitable[None]]

# Defaults (overridden by AppConfig at runtime)
_DEFAULT_WS_TIMEOUT = 10.0
_DEFAULT_MAX_BACKOFF = 60.0
_DEFAULT_MARKET_RETRIES = 3


class ExchangeConnection:
    """Owns a single CCXT exchange instance and manages its full lifecycle.

    Handles websocket ticker streaming + funding rate polling for all
    assigned symbols on this exchange.
    """

    def __init__(
        self,
        config: ExchangeConfig,
        ws_timeout_sec: float = _DEFAULT_WS_TIMEOUT,
        ws_max_backoff_sec: float = _DEFAULT_MAX_BACKOFF,
        market_load_retries: int = _DEFAULT_MARKET_RETRIES,
        limit_price_slippage_pct: float = DEFAULT_LIMIT_PRICE_SLIPPAGE_PCT,
        http: HttpClient | None = None,
        symbol_quarantine: SymbolQuarantine | None = None,
    ):
        self._config = config
        self._ws_timeout = float(config.extra.get("ws_timeout_sec", ws_timeout_sec))
        self._max_backoff = ws_max_backoff_sec
        self._market_retries = market_load_retries
        self._http = http
        self._limit_price_slippage = order_execution.normalize_limit_price_slippage_pct(
            limit_price_slippage_pct
        ) / 100.0
        self._exchange: Optional[ccxtpro.Exchange] = None
        self._running = False
        self._clock = ClockSync(config.id)
        self._funding = FundingPoller(config.id, max_backoff=ws_max_backoff_sec)
        self._volume = VolumePoller(config.id, max_backoff=300.0)
        self._tracker = TickTracker()
        self._raw_stream: Optional[RawTickerStream] = None
        self._direct_order_client: Optional[DirectOrderClient] = (
            self._create_direct_order_client()
        )
        self._requested_symbols: list[str] = []
        self._active_symbols: list[str] = []
        self._missing_symbols: list[str] = []
        self._symbol_quarantine = symbol_quarantine

    @property
    def exchange_id(self) -> str:
        return self._config.id

    def _create_direct_order_client(self) -> Optional[DirectOrderClient]:
        return create_direct_order_client(self._config)

    def loaded_markets(self) -> dict:
        """Return the ccxt.pro markets dict (empty if not yet loaded).

        Public read-only accessor — exposed so interface-layer endpoints
        (e.g. /api/markets/search) don't reach into private attributes.
        """
        if self._exchange is None:
            return {}
        return getattr(self._exchange, "markets", None) or {}

    # ── Lifecycle ─────────────────────────────────────────────────

    async def start(
        self,
        symbols: list[str],
        on_tick: OnTickCallback,
        on_funding: OnFundingCallback,
        funding_poll_sec: float = 60.0,
        leverage: int = 0,
        margin_mode: str = "",
        validation_symbols: list[str] | None = None,
        on_volume: "OnVolumeCallback | None" = None,
        volume_poll_sec: float = 300.0,
    ) -> None:
        """Connect to exchange and stream all symbols. Blocks until cancelled."""
        self._exchange = create_exchange(self._config)
        self._running = True
        self._requested_symbols = list(symbols)
        symbols_to_validate = list(validation_symbols or symbols)

        if self._symbol_quarantine is not None:
            symbols, blocked = self._symbol_quarantine.filter_stream_blocking(self.exchange_id, symbols)
            if blocked:
                logger.warning("{}: skipping stream-blocking quarantined symbols: {}", self.exchange_id, blocked)

        # Force IPv4 for exchanges that don't support IPv6 (e.g. Binance)
        if self._config.extra.get("force_ipv4"):
            patch_exchange_ipv4(self._exchange)
            logger.info("Forced IPv4 for {}", self.exchange_id)

        valid_symbols = await load_and_validate(
            self._exchange, self._config, symbols, self._market_retries
        )
        self._active_symbols = list(valid_symbols)
        valid_for_exchange = set(validate_loaded_symbols(self._exchange, symbols_to_validate))
        self._missing_symbols = [s for s in symbols_to_validate if s not in valid_for_exchange]
        if not valid_symbols:
            logger.warning(
                "No valid symbols for {} — skipping exchange", self.exchange_id
            )
            await self.stop()
            return

        if leverage > 0 or margin_mode:
            await self._setup_margin(valid_symbols, leverage, margin_mode)

        await self._clock.calibrate(self._exchange)

        tasks: list[asyncio.Task] = []

        tasks.append(
            asyncio.create_task(
                self._clock.run_loop(self._exchange),
                name=f"clock-sync-{self.exchange_id}",
            )
        )

        for symbol in valid_symbols:
            self._tracker.init_symbol(symbol)

        raw_stream = self._create_raw_stream(valid_symbols)
        if raw_stream:
            self._raw_stream = raw_stream
            tasks.append(
                asyncio.create_task(
                    self._raw_ws_loop(raw_stream),
                    name=f"raw-ws-{self.exchange_id}",
                )
            )
            tasks.append(
                asyncio.create_task(
                    self._raw_symbol_stall_watchdog(raw_stream),
                    name=f"raw-ws-watchdog-{self.exchange_id}",
                )
            )
        else:
            for symbol in valid_symbols:
                tasks.append(
                    asyncio.create_task(
                        self._watch_ticker(symbol),
                        name=f"ticker-{self.exchange_id}-{symbol}",
                    )
                )

        for symbol in valid_symbols:
            tasks.append(
                asyncio.create_task(
                    self._tick_consumer(symbol, on_tick),
                    name=f"tick-consumer-{self.exchange_id}-{symbol}",
                )
            )
            tasks.append(
                asyncio.create_task(
                    self._funding.poll_symbol(
                        self._exchange, symbol, on_funding, funding_poll_sec,
                        is_running=lambda: self._running,
                    ),
                    name=f"funding-{self.exchange_id}-{symbol}",
                )
            )
            if on_volume is not None:
                tasks.append(
                    asyncio.create_task(
                        self._volume.poll_symbol(
                            self._exchange, symbol, on_volume, volume_poll_sec,
                            is_running=lambda: self._running,
                            # Volume poller's REST ticker fills silent WS feeds.
                            tick_fallback=on_tick,
                        ),
                        name=f"volume-{self.exchange_id}-{symbol}",
                    )
                )

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Exchange {} tasks cancelled", self.exchange_id)
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._running = False
        if self._raw_stream:
            try:
                await self._raw_stream.close()
            except Exception:
                pass
            self._raw_stream = None
        if self._direct_order_client:
            try:
                await self._direct_order_client.close()
            except Exception:
                pass
        if self._exchange:
            try:
                await self._exchange.close()
                logger.info("Closed exchange: {}", self.exchange_id)
            except Exception as e:
                logger.warning("Error closing {}: {}", self.exchange_id, e)
            self._exchange = None

    async def reconnect(self) -> bool:
        """Reconnect the live market-data stream without changing subscriptions."""
        if self._raw_stream and hasattr(self._raw_stream, "request_reconnect"):
            return bool(await self._raw_stream.request_reconnect())
        self._tracker.record_error("manual reconnect unavailable for this stream")
        return False

    async def _raw_symbol_stall_watchdog(self, stream: RawTickerStream) -> None:
        await run_raw_symbol_stall_watchdog(
            exchange_id=self.exchange_id,
            stream=stream,
            tracker=self._tracker,
            active_symbols=self._active_symbols,
            ws_timeout=self._ws_timeout,
            is_running=lambda: self._running,
            quarantine=self._symbol_quarantine,
        )

    # ── Margin / leverage setup ───────────────────────────────────

    async def _setup_margin(
        self,
        symbols: list[str],
        leverage: int,
        margin_mode: str,
    ) -> None:
        assert self._exchange is not None
        await order_execution.setup_margin(
            self._exchange,
            self.exchange_id,
            symbols,
            leverage,
            margin_mode,
        )

    async def _set_safe_leverage_for_order(
        self, symbol: str, side: str, exc: Exception,
    ) -> bool:
        assert self._exchange is not None
        return await order_execution.set_safe_leverage_for_order(
            self._exchange, self.exchange_id, symbol, side, exc,
        )

    async def _create_order_with_leverage_recovery(
        self,
        *,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        price: float | None,
        params: dict,
    ) -> dict:
        assert self._exchange is not None
        return await order_execution.create_order_with_leverage_recovery(
            exchange=self._exchange,
            exchange_id=self.exchange_id,
            direct_order_client=self._direct_order_client,
            symbol=symbol,
            order_type=order_type,
            side=side,
            amount=amount,
            price=price,
            params=params,
        )

    # ── Order execution ───────────────────────────────────────────

    def get_contract_size(self, symbol: str) -> float:
        if not self._exchange: return 1.0
        return float(self._exchange.market(symbol).get("contractSize", 1) or 1)

    def _get_time_in_force(self) -> str:
        """Default to Immediate-Or-Cancel. Override via subclasses if FOK or other is preferred."""
        return "IOC"

    async def place_market_order(
        self,
        symbol: str,
        side: str,
        base_qty: float,
        price: float = 0.0,
        order_type: str = "market",
        is_close: bool = False,
        time_in_force: str | None = None,
        limit_price_slippage_pct: float | None = None,
    ) -> OrderResult:
        """Place a market or limit order with precision mathematically standardized."""
        assert self._exchange is not None, f"Exchange {self.exchange_id} not connected"
        return await order_execution.place_market_order(
            exchange=self._exchange,
            exchange_id=self.exchange_id,
            direct_order_client=self._direct_order_client,
            get_time_in_force=self._get_time_in_force,
            limit_price_slippage=self._limit_price_slippage,
            symbol=symbol,
            side=side,
            base_qty=base_qty,
            price=price,
            order_type=order_type,
            is_close=is_close,
            time_in_force=time_in_force,
            limit_price_slippage_pct=limit_price_slippage_pct,
        )

    # ── Tick streaming ────────────────────────────────────────────

    def _record_tick(
        self, symbol: str, bid: float, ask: float, last: float,
        exchange_ts_sec: float, receive_time: float,
        bid_qty: float | None = None, ask_qty: float | None = None,
    ) -> None:
        """Create PriceTick and record it — single path for raw WS and CCXT."""
        tick = PriceTick(
            exchange_id=self.exchange_id,
            symbol=symbol,
            bid=bid,
            ask=ask,
            bid_qty=bid_qty,
            ask_qty=ask_qty,
            last=last,
            timestamp=exchange_ts_sec,
            receive_time=receive_time,
            clock_offset=self._clock.offset,
            clock_synced=self._clock.synced,
        )
        self._tracker.record_tick(tick, self._clock.offset, self._clock.synced)

    def _create_raw_stream(self, valid_symbols: list[str]) -> Optional[RawTickerStream]:
        """Create a raw WS stream if supported for this exchange."""
        assert self._exchange is not None
        symbol_map: dict[str, str] = {}
        for symbol in valid_symbols:
            try:
                market = self._exchange.market(symbol)
                symbol_map[market["id"]] = symbol
            except Exception:
                logger.warning(
                    "Cannot map {} to native symbol on {}",
                    symbol, self.exchange_id,
                )
        if not symbol_map:
            return None

        stream = create_raw_stream(
            self.exchange_id, symbol_map, self._exchange, http=self._http,
        )
        if stream:
            logger.info(
                "Raw WS for {} ({}) — native symbols: {}",
                self.exchange_id, type(stream).__name__, list(symbol_map.keys()),
            )
        else:
            logger.info("Using CCXT Pro for {} (no raw WS adapter)", self.exchange_id)
        return stream

    async def _raw_ws_loop(self, stream: RawTickerStream) -> None:
        """Run raw WebSocket stream, dispatching ticks via _record_tick."""
        stop = asyncio.Event()

        def _on_tick(raw: RawTick) -> None:
            self._record_tick(
                raw.symbol, raw.bid, raw.ask,
                (raw.bid + raw.ask) / 2.0,
                raw.timestamp_ms / 1000.0,
                raw.receive_time,
                raw.bid_qty, raw.ask_qty,
            )

        try:
            await stream.run(on_tick=_on_tick, stop=stop)
        except asyncio.CancelledError:
            stop.set()
        finally:
            await stream.close()

    async def _watch_ticker(self, symbol: str) -> None:
        """CCXT Pro fallback — used only when no raw WS adapter exists."""
        assert self._exchange is not None
        backoff = 1.0
        logger.info("Using CCXT watch_ticker for {}/{}", self.exchange_id, symbol)

        while self._running:
            try:
                ticker = await asyncio.wait_for(
                    self._exchange.watch_ticker(symbol),
                    timeout=self._ws_timeout,
                )
                backoff = 1.0

                raw_bid = ticker.get("bid")
                raw_ask = ticker.get("ask")
                last = float(ticker.get("last", 0) or 0)
                bid = float(raw_bid) if raw_bid else last
                ask = float(raw_ask) if raw_ask else last
                if not last and bid and ask:
                    last = (bid + ask) / 2.0

                exchange_ts = float(ticker.get("timestamp", 0) or 0) / 1000.0
                self._record_tick(
                    symbol, bid, ask, last, exchange_ts, time.time(),
                    ticker.get("bidVolume"), ticker.get("askVolume"),
                )

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                self._tracker.record_error(
                    f"watchTicker timeout ({self._ws_timeout}s)"
                )
                logger.warning(
                    "WS timeout {}/{} — no data for {}s, reconnecting...",
                    self.exchange_id, symbol, self._ws_timeout,
                )
                await asyncio.sleep(1.0)
            except Exception as e:
                self._tracker.record_error(str(e))
                logger.warning("Ticker reconnect {}/{}: {}", self.exchange_id, symbol, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._max_backoff)

    # ── Tick consumer ─────────────────────────────────────────────

    async def _tick_consumer(self, symbol: str, on_tick: OnTickCallback) -> None:
        """Consume latest tick for a symbol. Skips stale ticks automatically."""
        event = self._tracker.get_event(symbol)
        while self._running:
            try:
                await event.wait()
                event.clear()
                tick = self._tracker.get_latest(symbol)
                if tick is not None:
                    await on_tick(tick)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Tick consumer error {}/{}: {}",
                    self.exchange_id, symbol, e,
                )

    # ── Stats ─────────────────────────────────────────────────────

    def stats(self) -> dict:
        return build_connection_stats(
            exchange_id=self.exchange_id,
            tracker=self._tracker,
            funding=self._funding,
            exchange=self._exchange,
            running=self._running,
            requested_symbols=self._requested_symbols,
            active_symbols=self._active_symbols,
            missing_symbols=self._missing_symbols,
            raw_stream=self._raw_stream,
        )
