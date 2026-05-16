"""Normalize UACryptoInvest chart-stream ticks into ``ScanOffer`` rows."""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import Protocol

from src.scanners.base import ScanOffer, Scanner
from src.scanners.exchange_map import map_source_to_bot

from .config import UACryptoInvestPair
from .stream import UACryptoInvestStream


class _Stream(Protocol):
    async def start(self) -> None: ...
    def snapshot(self) -> dict[str, dict[str, float]]: ...
    async def aclose(self) -> None: ...


class _PairSource(Protocol):
    async def fetch_pairs(self) -> list[UACryptoInvestPair]: ...


class _OfferSource(Protocol):
    async def fetch_offers(self) -> list[ScanOffer]: ...


class UACryptoInvestScanner(Scanner):
    name = "uacryptoinvest"

    def __init__(
        self,
        pairs: Iterable[UACryptoInvestPair],
        *,
        stream: _Stream | None = None,
        stream_factory=UACryptoInvestStream,
        offer_source: _OfferSource | None = None,
        pair_source: _PairSource | None = None,
        pair_refresh_sec: float = 300.0,
    ) -> None:
        self._configured_pairs = list(pairs)
        self._pairs = list(self._configured_pairs)
        self._pairs_by_key = {pair.key: pair for pair in self._pairs}
        self._stream_factory = stream_factory
        self._stream = stream
        self._own_stream = stream is None
        self._offer_source = offer_source
        self._pair_source = pair_source
        self._pair_refresh_sec = max(30.0, float(pair_refresh_sec))
        self._last_pair_refresh = 0.0
        self._started = False

    async def fetch(self) -> list[ScanOffer]:
        if self._offer_source is not None:
            offers = await self._offer_source.fetch_offers()
            if offers:
                return offers
        await self._refresh_pairs_if_needed()
        if not self._pairs:
            return []
        if self._stream is None:
            self._stream = self._stream_factory(self._pairs)
        if not self._started:
            await self._stream.start()
            self._started = True
        fetched_at = time.time()
        snapshot = self._stream.snapshot()
        offers: list[ScanOffer] = []
        for key, state in snapshot.items():
            pair = self._pairs_by_key.get(key)
            if pair is None:
                continue
            offer = self._offer_from_state(pair, state, fetched_at)
            if offer is not None:
                offers.append(offer)
        return offers

    async def aclose(self) -> None:
        if self._stream is not None:
            await self._stream.aclose()

    async def _refresh_pairs_if_needed(self) -> None:
        if self._pair_source is None:
            return
        now = time.monotonic()
        if self._last_pair_refresh and (now - self._last_pair_refresh) < self._pair_refresh_sec:
            return
        self._last_pair_refresh = now
        dynamic_pairs = await self._pair_source.fetch_pairs()
        pairs = _dedupe_pairs([*dynamic_pairs, *self._configured_pairs])
        if [pair.key for pair in pairs] == [pair.key for pair in self._pairs]:
            return
        self._pairs = pairs
        self._pairs_by_key = {pair.key: pair for pair in self._pairs}
        if self._own_stream and self._stream is not None:
            await self._stream.aclose()
            self._stream = None
            self._started = False

    @staticmethod
    def _offer_from_state(
        pair: UACryptoInvestPair,
        state: dict[str, float],
        fetched_at: float,
    ) -> ScanOffer | None:
        long_ask = _positive_float(state.get("long_ask"))
        short_bid = _positive_float(state.get("short_bid"))
        if long_ask is None or short_bid is None:
            return None
        long_bid = _positive_float(state.get("long_bid")) or long_ask
        short_ask = _positive_float(state.get("short_ask")) or short_bid
        long_funding = _float_or_zero(state.get("long_funding_pct"))
        short_funding = _float_or_zero(state.get("short_funding_pct"))
        long_interval_h = _positive_int(state.get("long_interval_h"), pair.long_interval_h)
        short_interval_h = _positive_int(state.get("short_interval_h"), pair.short_interval_h)
        open_spread_pct = ((short_bid - long_ask) / long_ask) * 100.0
        funding_per_hour = (
            short_funding / max(1, short_interval_h)
            - long_funding / max(1, long_interval_h)
        )
        return ScanOffer(
            source=UACryptoInvestScanner.name,
            symbol=pair.symbol,
            coin=pair.token,
            source_exchange_long=pair.long_exchange,
            source_exchange_short=pair.short_exchange,
            bot_exchange_long=map_source_to_bot(pair.long_exchange),
            bot_exchange_short=map_source_to_bot(pair.short_exchange),
            long_price=long_ask,
            short_price=short_bid,
            open_spread_pct=open_spread_pct,
            funding_long_pct=long_funding,
            funding_short_pct=short_funding,
            funding_interval_h_long=long_interval_h,
            funding_interval_h_short=short_interval_h,
            next_funding_ts=None,
            apr_pct=funding_per_hour * 24.0 * 365.0,
            volume_24h_usdt_long=_float_or_zero(state.get("long_volume_24h_usdt")),
            volume_24h_usdt_short=_float_or_zero(state.get("short_volume_24h_usdt")),
            chart_url=pair.chart_url,
            fetched_at=fetched_at,
        )


def _positive_float(value: float | None) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out > 0 else None


def _float_or_zero(value: float | None) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _positive_int(value: float | None, default: int) -> int:
    try:
        out = int(float(value))
    except (TypeError, ValueError):
        return max(1, int(default))
    return max(1, out)


def _dedupe_pairs(pairs: Iterable[UACryptoInvestPair]) -> list[UACryptoInvestPair]:
    out: list[UACryptoInvestPair] = []
    seen: set[str] = set()
    for pair in pairs:
        if pair.key in seen:
            continue
        seen.add(pair.key)
        out.append(pair)
    return out