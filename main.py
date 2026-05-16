"""Entry point — start exchange streams + optional Flask dashboard.

Usage:
    python main.py                  # run with defaults
    python main.py --no-web         # streams only, no dashboard
    python main.py --check-markets  # print common pairs across exchanges and exit
"""

from __future__ import annotations

# ─── Performance patches (must be before any other imports) ───
import json as _json_mod

try:
    import orjson

    # Monkey-patch stdlib json.loads → orjson for CCXT speed-up
    _json_mod.loads = orjson.loads  # type: ignore[assignment]
    _ORJSON = True
except ImportError:
    _ORJSON = False

try:
    import uvloop

    uvloop.install()
    _UVLOOP = True
except ImportError:
    _UVLOOP = False

# GC tuning — this process is long-lived, builds up a lot of short-lived
# dicts/strings from WS frames, and a default gen-2 collection periodically
# freezes the event loop for 1–2s (visible as "HANDLED LOOP STALL").
# Raising gen-2 threshold ~10× cuts those freezes without growing memory in
# practice (gen-0/1 still run normally).
import gc as _gc

_gc.set_threshold(700, 50, 100)  # defaults are (700, 10, 10)
# ─────────────────────────────────────────────────────────

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from loguru import logger

from src.adapters.asyncio_exceptions import install_loop_exception_handler
from src.adapters.exchange.market_checker import MarketChecker
from src.config import AppConfig, ExchangeConfig
from src.orchestrator import Orchestrator


class CliRunner:
    """Parses CLI arguments, builds config, and runs the appropriate entrypoint."""

    def __init__(self):
        self._args = self._parse_args()
        self._config = self._build_config()

    @staticmethod
    def _parse_args() -> argparse.Namespace:
        p = argparse.ArgumentParser(description="Perpetual Futures Arbitrage Monitor")
        p.add_argument("--no-web", action="store_true", help="Disable Flask dashboard")
        p.add_argument(
            "--check-markets", action="store_true", help="Print common pairs and exit"
        )
        p.add_argument("--port", type=int, default=None, help="Web dashboard port")
        p.add_argument(
            "--symbols", nargs="+", default=None, help="Override symbols list"
        )
        p.add_argument(
            "--exchanges", nargs="+", default=None, help="Override exchange IDs"
        )
        p.add_argument("--db", default=None, help="SQLite database path")
        p.add_argument(
            "--funding-poll",
            type=float,
            default=None,
            help="Funding rate poll interval (sec)",
        )
        p.add_argument(
            "--storage-interval",
            type=float,
            default=None,
            help="Min seconds between saves",
        )
        p.add_argument(
            "--log-level", default=None, help="Log level (DEBUG, INFO, WARNING)"
        )
        p.add_argument(
            "--log-ticks", action="store_true", help="Log every tick (verbose)"
        )
        return p.parse_args()

    def _build_config(self) -> AppConfig:
        config = AppConfig()
        args = self._args

        if args.no_web:
            config.web_enabled = False
        if args.port is not None:
            config.web_port = args.port
        if args.symbols:
            config.symbols = args.symbols
        if args.exchanges:
            config.exchanges = [ExchangeConfig(id=eid) for eid in args.exchanges]
        if args.db:
            config.db_path = args.db
        if args.funding_poll is not None:
            config.funding_poll_interval_sec = args.funding_poll
        if args.storage_interval is not None:
            config.storage_interval_sec = args.storage_interval
        if args.log_level:
            config.log_level = args.log_level
        if args.log_ticks:
            config.log_ticks = True

        # Operator-edited runtime overrides — overlay LAST so they win
        # over CLI defaults but allow CLI args earlier in this method
        # (e.g. --symbols) to be the explicit override.
        # CLI args take priority only when explicitly passed.
        from src.adapters.runtime_store import RuntimeConfigStore
        store = RuntimeConfigStore(config.runtime_config_path)
        overrides = store.load()
        if overrides:
            # Don't let runtime overrides clobber explicit CLI overrides.
            if args.symbols:
                overrides.pop("symbols", None)
            config.apply_runtime_overrides(overrides)
            # One-time on-disk migration: rewrite the persisted file with
            # the sanitized per-symbol blocks so legacy keys (e.g. stale
            # `trading.order_type: "market"`) don't keep coming back from
            # disk on the next startup. Only writes if something changed.
            try:
                migrated = dict(overrides)
                if config.per_symbol:
                    migrated["per_symbol"] = {
                        sym: dict(block)
                        for sym, block in config.per_symbol.items()
                    }
                if migrated != overrides:
                    store.save(migrated)
                    logger.info(
                        "runtime_config.json migrated: stripped legacy "
                        "per-symbol execution-mode keys"
                    )
            except Exception as exc:  # pragma: no cover — defensive
                logger.warning("runtime_config migration skipped: {}", exc)

        return config

    @staticmethod
    def _setup_logging(config: AppConfig) -> None:
        # ── Loguru sinks ──
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        logger.remove()  # drop default stderr handler
        log_level = config.log_level.upper()

        logger.add(
            sys.stderr,
            level=log_level,
            enqueue=True,
            format="<green>{time:HH:mm:ss}</green> <cyan>{name:<12}</cyan> <level>{level:<7}</level> {message}",
        )
        logger.add(
            log_dir / "perp_arb.log",
            level=log_level,
            enqueue=True,
            # Rotate at midnight, keep a single previous day so the log dir
            # never grows unbounded on long-lived servers.
            rotation="00:00",
            retention="1 day",
            compression="gz",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} {name:<12} {level:<7} {message}",
        )
        logger.add(
            log_dir / "errors.log",
            level="ERROR",
            enqueue=True,
            rotation="00:00",
            retention="1 day",
            compression="gz",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} {name:<12} {level:<7} {message}",
        )

        # Intercept stdlib logging → loguru
        class _InterceptHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                try:
                    level = logger.level(record.levelname).name
                except ValueError:
                    level = record.levelno
                logger.opt(depth=6, exception=record.exc_info).log(
                    level, record.getMessage()
                )

        logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
        for name in ("werkzeug", "ccxt", "ccxt.base.exchange", "aiosqlite"):
            logging.getLogger(name).setLevel(logging.WARNING)
        # aiohttp's server logger emits ERROR for every malformed request from
        # internet scanners (HTTP/2 preface, bad methods, etc.). Those are not
        # bot bugs — silence them at the source instead of pattern-matching in
        # the Telegram sink. aiohttp.web_log stays at INFO so real access logs
        # still appear in the file.
        for name in ("aiohttp.server", "aiohttp.http", "aiohttp.http_parser"):
            logging.getLogger(name).setLevel(logging.CRITICAL)

    def run(self) -> None:
        self._setup_logging(self._config)

        # Log performance patches
        logger.info(
            "uvloop: {} | orjson: {}",
            "ON" if _UVLOOP else "OFF",
            "ON" if _ORJSON else "OFF",
        )

        if self._args.check_markets:
            asyncio.run(MarketChecker(self._config).run())
            return

        orchestrator = Orchestrator(self._config)

        try:
            if hasattr(_gc, "freeze"):
                _gc.freeze()
                logger.info("gc freeze: ON | thresholds: {}", _gc.get_threshold())
        except Exception as exc:
            logger.warning("gc freeze failed: {}", exc)

        loop = asyncio.new_event_loop()
        install_loop_exception_handler(loop)

        def shutdown_handler():
            logger.info("Shutting down...")
            for task in asyncio.all_tasks(loop):
                task.cancel()

        if sys.platform != "win32":
            loop.add_signal_handler(signal.SIGINT, shutdown_handler)
            loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

        try:
            loop.run_until_complete(orchestrator.run())
        except KeyboardInterrupt:
            logger.info("Interrupted, shutting down...")
            loop.run_until_complete(orchestrator.shutdown())
        except asyncio.CancelledError:
            logger.info("Shutdown complete")
        finally:
            try:
                logger.complete()
            except Exception:
                pass
            loop.close()


if __name__ == "__main__":
    CliRunner().run()
