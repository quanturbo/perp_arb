"""Wires streams → state → spread calc → storage → trader → web dashboard."""

from __future__ import annotations

import asyncio
import os
import time
from itertools import combinations

from loguru import logger

from src.adapters.exchange.market_checker import MarketChecker
from src.adapters.exchange.stream_manager import ExchangeStreamManager
from src.adapters.http import HttpClient
from src.adapters.loop_monitor import EventLoopMonitor
from src.adapters.memory_monitor import MemoryMonitor
from src.adapters.storage import SpreadStorage
from src.config import AppConfig
from src.domain.models import ArbitrageState, FundingInfo, PriceTick, VolumeInfo
from src.domain.spread_calc import SpreadCalculator
from src.adapters.telegram import TelegramNotifier
from src.domain.trader import ArbitrageTrader
from src.settings import TradingSettings


def _build_trading_config_view(cfg: AppConfig) -> dict:
    """Snapshot of all trading-relevant config for dashboard display.

    Read-only view; UI uses this to render the live config header so the
    operator can verify what the bot is actually trading with.
    """
    t = cfg.trading
    return {
        "symbols": list(cfg.symbols),
        "read_exchanges": list(cfg.read_exchanges),
        "trade_exchanges": list(cfg.trade_exchanges),
        "enabled": t.enabled,
        "entry_spread_pct": t.entry_spread_pct,
        "close_spread_pct": t.close_spread_pct,
        "min_spread_persistence_ms": t.min_spread_persistence_ms,
        "max_entry_spread_pct": t.max_entry_spread_pct,
        "amount_usdt": t.amount_usdt,
        "leverage": t.leverage,
        "margin_mode": t.margin_mode,
        "order_type": t.order_type,
        "limit_price_slippage_pct": t.limit_price_slippage_pct,
        "time_in_force": t.time_in_force,
        "max_quote_to_order_age_ms": t.max_quote_to_order_age_ms,
        "max_top_book_usage_pct": t.max_top_book_usage_pct,
        "max_consecutive_failures": t.max_consecutive_failures,
        "fail_cooldown_sec": t.fail_cooldown_sec,
        "post_trade_delay_sec": t.post_trade_delay_sec,
        "max_open_positions": t.max_open_positions,
        "max_trades_per_session": t.max_trades_per_session,
        "max_latency_ms": t.max_latency_ms,
        # Implementation invariant: legs always placed via asyncio.gather.
        "parallel_legs": True,
        "symbol_entry_thresholds": dict(t.symbol_entry_spread_pct or {}),
        # Liquidity-aware filtering — used by the Opportunity Board to badge
        # low-liquidity legs and exclude them from ★ BEST scoring.
        "min_quote_volume_usd": cfg.min_quote_volume_usd,
    }


class Orchestrator:
    """Wires streams → state → spread calc → storage → web."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.states: dict[str, ArbitrageState] = {}
        self._last_logged_spread: dict[str, float] = {}  # per-symbol
        self._recalc_tasks: dict[str, asyncio.Task] = {}
        self._recalc_pending: set[str] = set()
        self.disabled_exchanges: set[str] = set()
        # Set by config controller when a restart-required override (e.g.
        # symbol change) was just persisted to runtime_config.json. The
        # main run-loop watches this event and shuts down cleanly so the
        # watchdog respawns the process.
        self.restart_event: asyncio.Event = asyncio.Event()
        self.storage = SpreadStorage(
            db_path=config.db_path,
            interval_sec=config.storage_interval_sec,
            save_on_improvement=config.save_on_spread_improvement,
            spread_retention_hours=config.spread_retention_hours,
            funding_retention_days=config.funding_retention_days,
            max_size_mb=config.db_max_size_mb,
        )
        # Single shared HTTP session: used by Telegram, UAInvest scanner and
        # any other future REST consumer. ccxt keeps its own session per
        # exchange (created via make_ccxt_session()) — those are not shared
        # because ccxt closes the session itself on exchange.close().
        self._http = HttpClient()
        self.streams = ExchangeStreamManager(
            config.exchanges, app_config=config, http=self._http,
        )
        self._notifier = TelegramNotifier(
            config.tg_bot_token, config.tg_chat_id, http=self._http,
        )
        # Event-loop stall monitor. Exposed publicly so WebApp can surface
        # its stats on the dashboard and tests can poll them.
        #
        # Thresholds rationale:
        #   warn  200ms  -> diagnostic, goes to log file only
        #   error 2000ms -> Telegram alert; sub-2s pauses on a long-running
        #                   Python process with ~400MB of WS state are normal
        #                   GC behaviour and NOT actionable. Only real outliers
        #                   (sync I/O on the loop, swap thrashing) should page.
        self.loop_monitor = EventLoopMonitor(
            check_interval_ms=50.0,
            stall_warn_ms=200.0,
            stall_error_ms=2000.0,
        )
        self._web = None
        # Process-memory diagnostic — logs RSS every 60s and forces a GC if
        # the process grows >100 MB above the post-warmup baseline. Pure
        # observer; never affects trading. Used to correlate OOM-kills with
        # workload changes (added symbols, reconnect storms, etc.).
        self.memory_monitor = MemoryMonitor(
            interval_sec=60.0,
            warn_growth_mb=50.0,
            critical_growth_mb=100.0,
        )
        self.trader = ArbitrageTrader(
            streams=self.streams,
            settings=config.trading,
            trade_exchanges=config.trade_exchanges,
            storage=self.storage,
            notifier=self._notifier,
            symbol_overrides=config.per_symbol,
        )

        # Optional external scanners (UAInvest, …). Built behind an env
        # switch; ``None`` when disabled — orchestrator code below stays
        # branch-free at the use site by checking ``is not None``.
        from src.scanners import build_scanner_service
        self.scanner_service = build_scanner_service(
            self._notifier,
            http=self._http,
            runtime_config_path=config.runtime_config_path,
            symbol_quarantine=self.streams.symbol_quarantine,
        )

        for symbol in config.symbols:
            self.states[symbol] = ArbitrageState(symbol=symbol)

    async def on_tick(self, tick: PriceTick) -> None:
        state = self.states.get(tick.symbol)
        if not state:
            return

        state.ticks[tick.exchange_id] = tick
        state.updated_at = tick.receive_time or time.time()
        has_enough = state.has_enough_data()

        if self.config.log_ticks:
            logger.debug(
                "TICK {} {} bid={:.6f} ask={:.6f}",
                tick.exchange_id,
                tick.symbol,
                tick.bid,
                tick.ask,
            )

        if has_enough:
            await self._request_recalc(state)

    async def on_funding(self, info: FundingInfo) -> None:
        state = self.states.get(info.symbol)
        if not state:
            return

        state.funding[info.exchange_id] = info

        logger.info(
            "FUNDING {} {} rate={:.6f}% next={}",
            info.exchange_id,
            info.symbol,
            info.funding_rate * 100,
            info.next_funding_time,
        )

        await self.storage.save_funding(
            info.exchange_id,
            info.symbol,
            info.funding_rate,
            info.next_funding_time,
            info.interval_hours,
            info.timestamp,
        )

        if state.has_enough_data():
            await self._request_recalc(state)

    async def _request_recalc(self, state: ArbitrageState) -> None:
        """Schedule one recalculation per symbol, coalescing bursts.

        Tick/funding callbacks can arrive faster than a full spread/trader
        pass can finish. Keep at most one active recalc task per symbol; if
        more requests arrive while it is running, perform exactly one more
        pass with the latest state after the active pass completes.
        """
        symbol = state.symbol
        task = self._recalc_tasks.get(symbol)
        if task is not None and not task.done():
            self._recalc_pending.add(symbol)
            return
        self._recalc_tasks[symbol] = asyncio.create_task(
            self._run_recalc_loop(symbol, state),
            name=f"recalc-{symbol}",
        )

    async def _run_recalc_loop(self, symbol: str, state: ArbitrageState) -> None:
        while True:
            await self._recalc(state)
            if symbol not in self._recalc_pending:
                return
            self._recalc_pending.discard(symbol)
            state = self.states.get(symbol, state)

    async def on_volume(self, info: VolumeInfo) -> None:
        state = self.states.get(info.symbol)
        if not state:
            return
        state.volumes[info.exchange_id] = info
        threshold = self._min_quote_volume_usd(info.symbol, info.exchange_id)
        quarantine = self.streams.symbol_quarantine
        if quarantine is None:
            return
        if threshold >= 0 and info.quote_volume_24h < threshold:
            if quarantine.record_low_liquidity(
                info.exchange_id,
                info.symbol,
                quote_volume_24h=info.quote_volume_24h,
                min_quote_volume_usd=threshold,
            ):
                logger.warning(
                    "{} {} marked low-liquidity: 24h quote volume {:.0f} < min {:.0f}; "
                    "excluded from trading and skipped after restart until reinstated",
                    info.exchange_id,
                    info.symbol,
                    info.quote_volume_24h,
                    threshold,
                )
        else:
            if quarantine.clear_low_liquidity(info.exchange_id, info.symbol):
                logger.info(
                    "{} {} liquidity recovered: 24h quote volume {:.0f} >= min {:.0f}; "
                    "removed low-liquidity quarantine",
                    info.exchange_id,
                    info.symbol,
                    info.quote_volume_24h,
                    threshold,
                )

    async def _recalc(self, state: ArbitrageState) -> None:
        filters = self.trader.exchange_filters_for_symbol(state.symbol)
        read_set = set(filters["read_exchanges"])
        read_active = bool(filters["read_filter_active"])
        now = time.time()
        max_data_age_ms = self._max_tick_data_age_ms()
        exchange_ids = [
            eid for eid in state.ticks if eid not in self.disabled_exchanges
            and state.ticks[eid].data_age_ms(now) <= max_data_age_ms
        ]
        if read_active:
            exchange_ids = [eid for eid in exchange_ids if eid in read_set]
        if len(exchange_ids) < 2:
            state.latest_spread = None
            state.trade_opportunities = []
            return
        ticks_snap = {eid: state.ticks[eid] for eid in exchange_ids}
        funding_snap = dict(state.funding)

        best_snapshot = None
        all_snapshots: list = []
        for eid_a, eid_b in combinations(exchange_ids, 2):
            tick_a = ticks_snap[eid_a]
            tick_b = ticks_snap[eid_b]
            fund_a = funding_snap.get(eid_a)
            fund_b = funding_snap.get(eid_b)

            snapshot = SpreadCalculator.determine_best_direction(
                tick_a,
                tick_b,
                fund_a,
                fund_b,
                holding_hours=self.config.holding_period_hours,
            )

            all_snapshots.append(snapshot)

            if best_snapshot is None or abs(snapshot.real_spread_pct) > abs(
                best_snapshot.real_spread_pct
            ):
                best_snapshot = snapshot

        if best_snapshot:
            now = time.time()
            best_snapshot.prices = {
                eid: {
                    "bid": ticks_snap[eid].bid,
                    "ask": ticks_snap[eid].ask,
                    "last": ticks_snap[eid].last,
                    "timestamp": ticks_snap[eid].timestamp,
                    "receive_time": ticks_snap[eid].receive_time,
                    "tick_age_ms": round(ticks_snap[eid].tick_age_ms, 1),
                    "data_age_ms": round(ticks_snap[eid].data_age_ms(now), 1),
                    "server_age_ms": round(ticks_snap[eid].server_age_ms(now), 1),
                }
                for eid in ticks_snap
            }
            state.latest_spread = best_snapshot
            spread_now = best_snapshot.price_spread_pct
            threshold = self.config.log_spread_change_pct
            sym = best_snapshot.symbol
            last = self._last_logged_spread.get(sym)
            if last is None or abs(spread_now - last) >= threshold:
                self._last_logged_spread[sym] = spread_now
                logger.info(
                    "SPREAD {} {} real={:.4f}% price={:.4f}% fund={:.4f}%",
                    best_snapshot.symbol,
                    best_snapshot.direction,
                    best_snapshot.real_spread_pct,
                    best_snapshot.price_spread_pct,
                    best_snapshot.funding_spread_pct,
                )

            # Filter trade-eligible snapshots using the trader's effective
            # per-symbol config. Do not use AppConfig.trade_exchanges here:
            # that is only the universal default, while each symbol may have
            # its own read/trade exchange lists.
            trade_set = set(filters["trade_exchanges"])
            all_prices = best_snapshot.prices  # all exchange prices
            if trade_set:
                trade_snaps = [
                    s for s in all_snapshots
                    if s.exchange_long in trade_set and s.exchange_short in trade_set
                    and (
                        not read_active
                        or (s.exchange_long in read_set and s.exchange_short in read_set)
                    )
                ]
                for s in trade_snaps:
                    s.prices = all_prices
                trade_snaps.sort(key=lambda s: s.price_spread_pct, reverse=True)
                state.trade_opportunities = trade_snaps
                liquid_trade_snaps = [
                    s for s in trade_snaps
                    if self._snapshot_has_liquid_legs(state, s)
                ]
                if liquid_trade_snaps:
                    await self.trader.on_spread_update(liquid_trade_snaps)
            else:
                state.trade_opportunities = []
            await self.storage.save_spread(best_snapshot)

    def _snapshot_has_liquid_legs(
        self, state: ArbitrageState, snapshot,
    ) -> bool:
        for exchange_id in (snapshot.exchange_long, snapshot.exchange_short):
            threshold = self._min_quote_volume_usd(state.symbol, exchange_id)
            if threshold < 0:
                continue
            volume = state.volumes.get(exchange_id)
            if volume is None or volume.quote_volume_24h < threshold:
                return False
        return True

    def _min_quote_volume_usd(self, symbol: str, exchange_id: str) -> float:
        per_symbol = getattr(self.config, "per_symbol", {}) or {}
        override_block = per_symbol.get(symbol, {}) if isinstance(per_symbol, dict) else {}
        override = (
            override_block.get("min_quote_volume_usd")
            if isinstance(override_block, dict) else None
        )
        if isinstance(override, dict) and exchange_id in override:
            return float(override[exchange_id])
        if isinstance(override, (int, float)) and not isinstance(override, bool):
            return float(override)
        return float(getattr(self.config, "min_quote_volume_usd", 0.0) or 0.0)

    def _max_tick_data_age_ms(self) -> float:
        timeout = float(getattr(self.config, "ws_timeout_sec", 10.0) or 10.0)
        return max(timeout * 2.0, 20.0) * 1000.0

    async def _periodic_cleanup(self) -> None:
        """Periodically delete old spread_snapshots and funding_log rows."""
        # Run first cleanup shortly after startup (don't block init)
        await asyncio.sleep(10)
        try:
            await self.storage.cleanup_old_data()
        except Exception as e:
            logger.warning("Initial cleanup error: {}", e)

        interval = self.config.cleanup_interval_sec
        while True:
            await asyncio.sleep(interval)
            try:
                await self.storage.cleanup_old_data()
            except Exception as e:
                logger.warning("Cleanup error: {}", e)

    async def run(self) -> None:
        await self.storage.init_db()
        await self.trader.restore_from_db()
        # Forward any ERROR-level log to Telegram (dedup + noise-filter applied).
        # OCP: new error sites are covered automatically without changing call sites.
        self._notifier.enable_loguru_error_sink()

        if self.config.web_enabled:
            await self._start_web()

        # Start periodic cleanup task + event-loop stall monitor + RSS logger.
        cleanup_task = asyncio.create_task(self._periodic_cleanup())
        monitor_task = self.loop_monitor.start()
        memory_task = self.memory_monitor.start()

        # Optional scanner service — runs on the same loop, polls upstream
        # APIs every ``SCANNER_POLL_SEC``. Failures don't propagate.
        if self.scanner_service is not None:
            await self.scanner_service.start()

        logger.info(
            "Starting streams: exchanges={} symbols={}",
            [c.id for c in self.config.exchanges],
            self.config.symbols,
        )

        try:
            await self.streams.start(
                symbols=self.config.symbols,
                on_tick=self.on_tick,
                on_funding=self.on_funding,
                funding_poll_sec=self.config.funding_poll_interval_sec,
                trade_exchanges=self.config.trade_exchanges,
                leverage=self.config.trading.leverage,
                margin_mode=self.config.trading.margin_mode,
                on_volume=self.on_volume,
                volume_poll_sec=self.config.volume_poll_interval_sec,
            )
        finally:
            cleanup_task.cancel()
            monitor_task.cancel()
            if memory_task is not None:
                memory_task.cancel()
            await self.shutdown()

    async def shutdown(self) -> None:
        await self.streams.stop()
        if self.scanner_service is not None:
            await self.scanner_service.stop()
        if self._web is not None:
            await self._web.stop()
            self._web = None
        await self.storage.close()
        await self._http.aclose()

    async def request_restart(self) -> None:
        """Signal a graceful restart for restart-required config changes.

        Watchdog (_watchdog.sh) respawns the process on clean exit. Caller
        (config controller) returns 202 to the dashboard; user sees a
        ~15s 'restarting' chip then the new symbol set is live.
        """
        logger.warning(
            "RESTART REQUESTED — graceful shutdown for runtime config change",
        )
        self.restart_event.set()

        async def _delayed_exit():
            await asyncio.sleep(2.0)
            logger.info("Exiting for watchdog respawn")
            # os._exit() short-circuits asyncio's exception handling, which
            # otherwise swallows sys.exit() inside a task and keeps the bot
            # alive. Watchdog respawns on any non-zero/zero exit code.
            os._exit(0)
        # Hold a reference so the task is not GC'd before it runs.
        self._exit_task = asyncio.create_task(
            _delayed_exit(), name="delayed-exit",
        )

    async def replace_symbols(self, new_symbols: list[str]) -> dict:
        """Hot-replace tracked symbols without restarting the process.

        Updates `self.states`, `self.config.symbols`, and signals the
        ExchangeStreamManager to cycle WS subscriptions in-place. Web,
        trader, and storage layers stay alive — only the streaming WS
        connections briefly recycle (~3-5s).

        Returns: {"added": [...], "removed": [...], "active": [...]}
        """
        old = set(self.config.symbols)
        new = set(new_symbols)
        added = sorted(new - old)
        removed = sorted(old - new)

        for sym in added:
            self.states[sym] = ArbitrageState(symbol=sym)
        for sym in removed:
            self.states.pop(sym, None)
            self._last_logged_spread.pop(sym, None)
        for sym in new_symbols:
            self.states.setdefault(sym, ArbitrageState(symbol=sym))

        self.config.symbols = list(new_symbols)

        # Trigger in-place WS resubscription via stream manager hot-restart.
        await self.streams.replace_symbols(list(new_symbols))

        logger.info(
            "Symbols hot-replaced: added={} removed={} active={}",
            added,
            removed,
            list(new_symbols),
        )
        return {"added": added, "removed": removed, "active": list(new_symbols)}

    async def _start_web(self) -> None:
        from src.web.app import WebApp

        cfg_dict = {
            "web_poll_state_ms": self.config.web_poll_state_ms,
            "web_poll_history_ms": self.config.web_poll_history_ms,
            "web_token": self.config.web_token,
            "disabled_exchanges": self.disabled_exchanges,
            "exchanges_info": [
                {
                    "id": c.id,
                    "enabled": True,
                    "direct_order_ws": bool(c.extra.get("direct_order_ws")),
                }
                for c in self.config.exchanges
            ],
            "dashboard_max_latency_ms": self.config.dashboard_max_latency_ms,
            "dashboard_exchange_latency_ms": self.config.dashboard_exchange_latency_ms,
            "trading_config": _build_trading_config_view(self.config),
        }
        self._web = WebApp(
            self.states,
            self.storage,
            cfg_dict,
            trader=self.trader,
            streams=self.streams,
            loop_monitor=self.loop_monitor,
            config=self.config,
            orchestrator=self,
            scanner_service=self.scanner_service,
        )
        await self._web.start(
            host=self.config.web_host,
            port=self.config.web_port,
        )
