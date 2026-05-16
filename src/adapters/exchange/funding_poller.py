"""Funding rate polling — independent of tick streaming."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from typing import Awaitable, Callable, Optional

from loguru import logger

from src.domain.models import FundingInfo

OnFundingCallback = Callable[[FundingInfo], Awaitable[None]]


class FundingPoller:
    """Polls funding rates for one exchange, all symbols.

    Owns interval inference + caching so ExchangeConnection doesn't have to.
    """

    # Consecutive failures are tracked for health/log context only. Funding
    # polling is market-data enrichment; transient upstream/REST failures must
    # not page Telegram like order execution failures do.
    _ERROR_THRESHOLD = 3
    _DEFAULT_REQUEST_TIMEOUT_SEC = 20.0
    _DEFAULT_INITIAL_STAGGER_SEC = 30.0

    def __init__(
        self,
        exchange_id: str,
        max_backoff: float = 60.0,
        request_timeout_sec: float = _DEFAULT_REQUEST_TIMEOUT_SEC,
        initial_stagger_sec: float = _DEFAULT_INITIAL_STAGGER_SEC,
    ) -> None:
        self._exchange_id = exchange_id
        self._max_backoff = max_backoff
        self._request_timeout_sec = max(0.0, float(request_timeout_sec))
        self._initial_stagger_sec = max(0.0, float(initial_stagger_sec))
        self._interval_cache: dict[str, float] = {}
        self._consecutive_failures: dict[str, int] = {}
        self.received: int = 0
        self.errors: int = 0

    async def poll_symbol(
        self,
        exchange,
        symbol: str,
        callback: OnFundingCallback,
        poll_sec: float,
        is_running: Callable[[], bool] = lambda: True,
    ) -> None:
        """Poll funding for one symbol. Blocks until cancelled."""
        backoff = poll_sec
        await self._sleep_initial_stagger(symbol, poll_sec)
        while is_running():
            try:
                data = await self._with_request_timeout(
                    exchange.fetch_funding_rate(symbol)
                )
                backoff = poll_sec
                self._consecutive_failures[symbol] = 0

                interval = self._parse_interval(data.get("interval"))
                if interval is None:
                    interval = self._interval_cache.get(symbol)
                if interval is None:
                    interval = await self._infer_interval(exchange, symbol)
                    self._interval_cache[symbol] = interval

                next_ts = data.get("fundingTimestamp")
                if not next_ts:
                    next_ts = data.get("info", {}).get("nextFundingTime")

                info = FundingInfo(
                    exchange_id=self._exchange_id,
                    symbol=symbol,
                    funding_rate=float(data.get("fundingRate", 0) or 0),
                    next_funding_time=(
                        float(next_ts) / 1000.0 if next_ts else None
                    ),
                    interval_hours=interval,
                    timestamp=self._timestamp_seconds(data.get("timestamp")),
                )
                self.received += 1
                await callback(info)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.errors += 1
                fails = self._consecutive_failures.get(symbol, 0) + 1
                self._consecutive_failures[symbol] = fails
                if self._is_terminal_funding_error(e):
                    logger.warning(
                        self._format_funding_issue(symbol, fails, e, level="WARNING", terminal=True)
                    )
                    self._consecutive_failures[symbol] = 0
                    break
                logger.warning(
                    self._format_funding_issue(symbol, fails, e, level="WARNING")
                )
                backoff = min(backoff * 2, self._max_backoff)
            await asyncio.sleep(backoff)

    async def _infer_interval(self, exchange, symbol: str) -> float:
        """Infer funding interval from recent history timestamps."""
        try:
            hist = await self._with_request_timeout(
                exchange.fetch_funding_rate_history(symbol, limit=3)
            )
            if len(hist) >= 2:
                times = sorted(
                    h["timestamp"] for h in hist if h.get("timestamp")
                )
                if len(times) >= 2:
                    diff_h = (times[-1] - times[-2]) / 3_600_000
                    if 0.5 <= diff_h <= 24:
                        logger.info(
                            "Inferred funding interval for {}/{}: {:.1f}h",
                            self._exchange_id,
                            symbol,
                            diff_h,
                        )
                        return round(diff_h)
        except Exception as e:
            logger.debug(
                "Cannot infer funding interval for {}/{}: {}",
                self._exchange_id,
                symbol,
                e,
            )
        logger.info(
            "Using default 8h funding interval for {}/{}",
            self._exchange_id,
            symbol,
        )
        return 8.0

    async def _with_request_timeout(self, awaitable):
        if self._request_timeout_sec <= 0:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout=self._request_timeout_sec)

    async def _sleep_initial_stagger(self, symbol: str, poll_sec: float) -> None:
        delay = self._initial_stagger_delay(symbol, poll_sec)
        if delay > 0:
            await asyncio.sleep(delay)

    def _initial_stagger_delay(self, symbol: str, poll_sec: float) -> float:
        window = min(self._initial_stagger_sec, max(0.0, float(poll_sec) * 0.8))
        if window <= 0:
            return 0.0
        digest = hashlib.blake2b(
            f"{self._exchange_id}:{symbol}".encode("utf-8"), digest_size=2,
        ).digest()
        return (int.from_bytes(digest, "big") / 65535.0) * window

    @staticmethod
    def _parse_interval(interval: Optional[str]) -> Optional[float]:
        if not interval:
            return None
        val = interval.replace("h", "")
        try:
            return float(val)
        except ValueError:
            return None

    @staticmethod
    def _timestamp_seconds(value, now: float | None = None) -> float:
        fallback = time.time() if now is None else now
        try:
            raw = float(value or 0)
        except (TypeError, ValueError):
            return fallback
        if raw <= 0:
            return fallback
        return raw / 1000.0 if raw > 10_000_000_000 else raw

    @staticmethod
    def _is_terminal_funding_error(exc: Exception) -> bool:
        """True for funding responses where retrying this symbol just spams."""
        cls_name = exc.__class__.__name__.lower()
        text = str(exc).lower()
        if "badsymbol" in cls_name or "invalidsymbol" in cls_name:
            return True
        markers = (
            "109415",
            "pause currently",
            "validted symbols",
            "validated symbols",
            "valid symbols",
            "please verify it",
            "invalid symbol",
            "unknown symbol",
            "symbol not found",
            "does not have market",
        )
        return any(marker in text for marker in markers)

    def _format_funding_issue(
        self,
        symbol: str,
        fails: int,
        exc: Exception,
        *,
        level: str,
        terminal: bool = False,
    ) -> str:
        payload = self._extract_error_payload(exc)
        reason = self._funding_error_reason(exc, payload)
        extra = payload if payload is not None else {"error": str(exc)}
        if terminal and isinstance(extra, dict):
            extra = {**extra, "terminal": True}
        return (
            "EXCHANGE ISSUE | "
            f"level={self._clean_field(level)} | "
            "type=FUNDING_POLL_ERROR | "
            f"exchange={self._clean_field(self._exchange_id)} | "
            f"symbol={self._clean_field(symbol)} | "
            f"reason={self._clean_field(reason)} | "
            f"fails={fails} | "
            f"extra={json.dumps(extra, ensure_ascii=False, separators=(',', ':'))}"
        )

    @staticmethod
    def _extract_error_payload(exc: Exception) -> dict | None:
        text = str(exc)
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            payload = json.loads(text[start:end + 1])
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _funding_error_reason(exc: Exception, payload: dict | None) -> str:
        msg = payload.get("msg") if isinstance(payload, dict) else None
        if not msg:
            msg = str(exc)
        return str(msg).split(",", 1)[0].strip() or str(exc)

    @staticmethod
    def _clean_field(value: object) -> str:
        text = str(value).replace("|", "/").strip()
        return re.sub(r"\s+", " ", text)
