"""ScannerService — periodic poll loop, cache, alert dispatch.

Composition (DIP): takes pre-built scanners + filter store + alerter so
tests can swap stubs in without touching the network or filesystem.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable

from loguru import logger

from src.adapters.exchange.symbol_quarantine import REASON_MANUAL_FAKE_SIGNAL
from src.scanners.alerts import ScannerAlerter
from src.scanners.base import ScanOffer, Scanner
from src.scanners.filter_store import ScannerFilterStore

if TYPE_CHECKING:
    from src.adapters.exchange.symbol_quarantine import SymbolQuarantine


SIGNAL_ABSENT_GRACE_TICKS = 3


@dataclass
class ScannerHealth:
    enabled: bool = True
    last_fetch_at: float | None = None
    last_fetch_duration_ms: float | None = None
    last_offer_count: int = 0
    last_match_count: int = 0
    fetches_total: int = 0
    matches_total: int = 0
    errors_recent: int = 0
    error_log: deque = field(default_factory=lambda: deque(maxlen=20))

    def to_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "last_fetch_at": self.last_fetch_at,
            "last_fetch_age_sec": (
                None if self.last_fetch_at is None
                else max(0.0, time.time() - self.last_fetch_at)
            ),
            "last_fetch_duration_ms": self.last_fetch_duration_ms,
            "last_offer_count": self.last_offer_count,
            "last_match_count": self.last_match_count,
            "fetches_total": self.fetches_total,
            "matches_total": self.matches_total,
            "errors_recent": self.errors_recent,
            "error_log": list(self.error_log),
        }


class ScannerService:
    """One poll loop, N scanners, one global filter, one alerter."""

    def __init__(
        self,
        scanners: Iterable[Scanner],
        filter_store: ScannerFilterStore,
        alerter: ScannerAlerter,
        *,
        poll_interval_sec: float = 30.0,
        symbol_quarantine: "SymbolQuarantine | None" = None,
    ) -> None:
        self._scanners: list[Scanner] = list(scanners)
        self._filter = filter_store
        self._alerter = alerter
        self._symbol_quarantine = symbol_quarantine
        self._poll_interval = max(5.0, poll_interval_sec)

        self._cache: dict[str, list[ScanOffer]] = {}
        self.health = ScannerHealth(enabled=True)
        self._last_notify_enabled: bool | None = None
        self._previous_match_keys: set[str] = set()
        self._missing_match_counts: dict[str, int] = {}

        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="scanner-service")
        logger.info(
            "ScannerService started: scanners={} interval={}s",
            [s.name for s in self._scanners], self._poll_interval,
        )

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop.set()
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        self._task = None
        for s in self._scanners:
            await s.aclose()

    # ── Poll loop ──────────────────────────────────────────────────────

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self._record_error(str(e))
                logger.exception("ScannerService tick failed: {}", e)
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._poll_interval,
                )
            except asyncio.TimeoutError:
                continue

    async def _tick(self) -> None:
        for scanner in self._scanners:
            t0 = time.monotonic()
            offers = self._filter_quarantined_offers(await scanner.fetch())
            duration_ms = (time.monotonic() - t0) * 1000.0

            self._cache[scanner.name] = offers
            self.health.last_fetch_at = time.time()
            self.health.last_fetch_duration_ms = duration_ms
            self.health.last_offer_count = len(offers)
            self.health.fetches_total += 1

            if offers:
                await self._evaluate_and_alert(offers)
            else:
                self.health.last_match_count = 0

    async def _evaluate_and_alert(self, offers: list[ScanOffer]) -> None:
        flt = await self._filter.get()
        arm_notifications = flt.notify_telegram and self._last_notify_enabled is not True
        self._last_notify_enabled = flt.notify_telegram

        match_count = 0
        matched_offers: list[ScanOffer] = []
        for offer in offers:
            if not flt.passes(offer):
                continue
            match_count += 1
            self.health.matches_total += 1
            matched_offers.append(offer)

        all_offer_keys = {self._offer_key(offer) for offer in offers}
        current_match_keys = {self._offer_key(offer) for offer in matched_offers}
        if not flt.notify_telegram:
            self._previous_match_keys = current_match_keys
            self._missing_match_counts.clear()
            self.health.last_match_count = match_count
            return

        if arm_notifications:
            observe = getattr(self._alerter, "observe_many", None)
            if callable(observe):
                observe(matched_offers)
            self._previous_match_keys = current_match_keys
            self._missing_match_counts.clear()
            self.health.last_match_count = match_count
            return

        active_previous = self._active_previous_match_keys(
            current_match_keys=current_match_keys,
            all_offer_keys=all_offer_keys,
        )

        crossing_offers = [
            offer for offer in matched_offers
            if self._offer_key(offer) not in active_previous
        ]
        steady_offers = [
            offer for offer in matched_offers
            if self._offer_key(offer) in active_previous
        ]

        if crossing_offers:
            try:
                await self._alerter.alert_many(
                    crossing_offers,
                    renotify_funding_change_pct=flt.renotify_funding_change_pct,
                    renotify_spread_change_pct=flt.renotify_spread_change_pct,
                    force=True,
                )
            except Exception as e:  # noqa: BLE001
                self._record_error(f"alert failed: {e}")

        if steady_offers:
            try:
                await self._alerter.alert_many(
                    steady_offers,
                    renotify_funding_change_pct=flt.renotify_funding_change_pct,
                    renotify_spread_change_pct=flt.renotify_spread_change_pct,
                )
            except Exception as e:  # noqa: BLE001
                self._record_error(f"alert failed: {e}")
        self._previous_match_keys = active_previous | current_match_keys
        self.health.last_match_count = match_count

    def _active_previous_match_keys(
        self,
        *,
        current_match_keys: set[str],
        all_offer_keys: set[str],
    ) -> set[str]:
        active: set[str] = set()
        for key in self._previous_match_keys:
            if key in current_match_keys:
                self._missing_match_counts.pop(key, None)
                active.add(key)
            elif key in all_offer_keys:
                self._missing_match_counts.pop(key, None)
            else:
                misses = self._missing_match_counts.get(key, 0) + 1
                if misses < SIGNAL_ABSENT_GRACE_TICKS:
                    self._missing_match_counts[key] = misses
                    active.add(key)
                else:
                    self._missing_match_counts.pop(key, None)
        return active

    def _filter_quarantined_offers(self, offers: list[ScanOffer]) -> list[ScanOffer]:
        quarantine = self._symbol_quarantine
        if quarantine is None:
            return offers
        return [offer for offer in offers if not self._is_manual_fake(offer)]

    def _is_manual_fake(self, offer: ScanOffer) -> bool:
        quarantine = self._symbol_quarantine
        if quarantine is None:
            return False
        symbol = self._normalized_symbol(offer.symbol)
        exchanges = {
            offer.bot_exchange_long,
            offer.bot_exchange_short,
            offer.source_exchange_long,
            offer.source_exchange_short,
        }
        return any(
            bool(exchange)
            and quarantine.has_reason(exchange, symbol, REASON_MANUAL_FAKE_SIGNAL)
            for exchange in exchanges
        )

    @staticmethod
    def _offer_key(offer: ScanOffer) -> str:
        return (
            f"{offer.symbol}:"
            f"{offer.source_exchange_long}-{offer.source_exchange_short}"
        )

    @staticmethod
    def _normalized_symbol(symbol: str) -> str:
        text = str(symbol or "").strip().upper()
        if "/" in text and ":" in text:
            return text
        if text.endswith("USDT") and len(text) > 4:
            return f"{text[:-4]}/USDT:USDT"
        return text

    # ── Read-side API for controllers ──────────────────────────────────

    def snapshot(self, source: str | None = None) -> list[ScanOffer]:
        if source is None:
            out: list[ScanOffer] = []
            for v in self._cache.values():
                out.extend(v)
            return out
        return list(self._cache.get(source, []))

    @property
    def filter_store(self) -> ScannerFilterStore:
        return self._filter

    @property
    def sources(self) -> list[str]:
        return [s.name for s in self._scanners]

    # ── Internals ──────────────────────────────────────────────────────

    def _record_error(self, msg: str) -> None:
        self.health.errors_recent += 1
        self.health.error_log.append({"ts": time.time(), "msg": msg[:200]})
