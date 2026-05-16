"""24h quote-volume polling — independent of tick streaming.

Uses CCXT's `fetch_ticker(symbol).quoteVolume` field, which every supported
exchange exposes. We poll on a slow cadence (default 5 min) because 24h
volume changes slowly and we don't want to spend rate-limit budget on it.

Mirrors the shape of `funding_poller.FundingPoller` so wiring through
`ExchangeConnection.start` looks identical for the operator.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Awaitable, Callable

from loguru import logger

from src.domain.models import PriceTick, VolumeInfo

OnVolumeCallback = Callable[[VolumeInfo], Awaitable[None]]
OnTickFallback = Callable[[PriceTick], Awaitable[None]]


class VolumePoller:
    """Polls 24h quote volume for one exchange, all symbols.

    Independent from FundingPoller so a slow ticker endpoint can't slow down
    funding updates (and vice versa).

    The same fetch_ticker() round-trip we use for the 24h volume number
    also carries bid/ask/last. For *low-liquidity* perps (e.g. illiquid
    new listings, microcaps) the WebSocket ticker stream is silent for
    minutes at a time — the exchange only pushes on trade events. To keep
    the dashboard usable on those markets we re-emit the REST ticker as a
    PriceTick whenever a `tick_fallback` callback is supplied. It's free
    (no extra HTTP call) and runs at the same slow cadence as the volume
    poll, so it never competes with the live WS feed.
    """

    _ERROR_THRESHOLD = 3

    def __init__(self, exchange_id: str, max_backoff: float = 300.0) -> None:
        self._exchange_id = exchange_id
        self._max_backoff = max_backoff
        self._consecutive_failures: dict[str, int] = {}
        self.received: int = 0
        self.errors: int = 0

    async def poll_symbol(
        self,
        exchange,
        symbol: str,
        callback: OnVolumeCallback,
        poll_sec: float,
        is_running: Callable[[], bool] = lambda: True,
        tick_fallback: "OnTickFallback | None" = None,
    ) -> None:
        """Poll quote volume for one symbol. Blocks until cancelled."""
        backoff = poll_sec
        while is_running():
            try:
                ticker = await exchange.fetch_ticker(symbol)
                backoff = poll_sec
                self._consecutive_failures[symbol] = 0

                ts = self._timestamp_seconds(ticker.get("timestamp"))
                quote_vol = self._extract_quote_volume(ticker)
                info = VolumeInfo(
                    exchange_id=self._exchange_id,
                    symbol=symbol,
                    quote_volume_24h=quote_vol,
                    timestamp=ts,
                )
                self.received += 1
                await callback(info)

                # REST tick fallback. Only emits when the ticker carries
                # a usable bid AND ask — partial data (e.g. last-only)
                # would let stale WS prices be silently overwritten.
                if tick_fallback is not None:
                    bid = self._safe_float(ticker.get("bid"))
                    ask = self._safe_float(ticker.get("ask"))
                    last = self._safe_float(ticker.get("last")) or self._safe_float(ticker.get("close"))
                    if bid > 0 and ask > 0:
                        tick = PriceTick(
                            exchange_id=self._exchange_id,
                            symbol=symbol,
                            bid=bid, ask=ask,
                            last=last or (bid + ask) / 2,
                            timestamp=ts,
                            receive_time=time.time(),
                        )
                        await tick_fallback(tick)
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._is_paused_symbol_error(e):
                    self.errors += 1
                    fails = self._consecutive_failures.get(symbol, 0) + 1
                    self._consecutive_failures[symbol] = 0
                    logger.warning(
                        self._format_volume_issue(symbol, fails, e, level="WARNING", terminal=True)
                    )
                    break
                if self._is_invalid_symbol_error(e):
                    self._consecutive_failures[symbol] = 0
                    logger.warning(
                        "Volume polling disabled {}/{}: invalid symbol for ticker endpoint ({})",
                        self._exchange_id,
                        symbol,
                        e,
                    )
                    break
                self.errors += 1
                fails = self._consecutive_failures.get(symbol, 0) + 1
                self._consecutive_failures[symbol] = fails
                log_fn = logger.error if fails >= self._ERROR_THRESHOLD else logger.warning
                log_fn(
                    "Volume error {}/{} (fails={}): {}",
                    self._exchange_id,
                    symbol,
                    fails,
                    e,
                )
                backoff = min(backoff * 2, self._max_backoff)
            await asyncio.sleep(backoff)

    @staticmethod
    def _extract_quote_volume(ticker: dict) -> float:
        """Pull quote-currency 24h volume from a CCXT ticker.

        CCXT canonical field is `quoteVolume`. Some exchanges return it under
        `info` only; we fall back to base * last as a last resort so the
        dashboard always has *something* to show rather than zero.
        """
        if not isinstance(ticker, dict):
            return 0.0
        for key in ("quoteVolume", "quoteVolume24h", "vol_24h_quote"):
            v = ticker.get(key)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        info = ticker.get("info") or {}
        for key in ("quoteVolume", "turnover24h", "quote_volume", "qv"):
            v = info.get(key) if isinstance(info, dict) else None
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        # Last-resort estimate: base × last.
        try:
            base = float(ticker.get("baseVolume") or 0)
            last = float(ticker.get("last") or ticker.get("close") or 0)
            return base * last
        except (TypeError, ValueError):
            return 0.0

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
    def _safe_float(value) -> float:
        try:
            return float(value) if value is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _is_invalid_symbol_error(exc: Exception) -> bool:
        """True for terminal exchange responses where retrying won't help."""
        cls_name = exc.__class__.__name__.lower()
        text = str(exc).lower()
        if "badsymbol" in cls_name or "invalidsymbol" in cls_name:
            return True
        if "200003" in text:
            return True
        markers = (
            "invalid symbol",
            "unknown symbol",
            "symbol not found",
            "market symbol",
            "does not have market",
        )
        return any(marker in text for marker in markers)

    @staticmethod
    def _is_paused_symbol_error(exc: Exception) -> bool:
        text = str(exc).lower()
        markers = (
            "109415",
            "pause currently",
            "validted symbols",
            "validated symbols",
            "please verify it",
        )
        return any(marker in text for marker in markers)

    def _format_volume_issue(
        self,
        symbol: str,
        fails: int,
        exc: Exception,
        *,
        level: str,
        terminal: bool = False,
    ) -> str:
        payload = self._extract_error_payload(exc)
        reason = self._volume_error_reason(exc, payload)
        extra = payload if payload is not None else {"error": str(exc)}
        if terminal and isinstance(extra, dict):
            extra = {**extra, "terminal": True}
        return (
            "EXCHANGE ISSUE | "
            f"level={self._clean_field(level)} | "
            "type=VOLUME_POLL_ERROR | "
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
    def _volume_error_reason(exc: Exception, payload: dict | None) -> str:
        msg = payload.get("msg") if isinstance(payload, dict) else None
        if not msg:
            msg = str(exc)
        return str(msg).split(",", 1)[0].strip() or str(exc)

    @staticmethod
    def _clean_field(value: object) -> str:
        text = str(value).replace("|", "/").strip()
        return re.sub(r"\s+", " ", text)
