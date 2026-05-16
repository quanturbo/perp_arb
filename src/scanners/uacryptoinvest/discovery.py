"""Discover a UACryptoInvest pair for ANY token via searchTokens.

The scanner snapshot only contains currently profitable opportunities
(~71 rows). User-added symbols (1INCH, BTC, …) live outside that set,
so chart history must fall back to a generic UACI exchange probe.

This module is a thin lookup: given a token, ask UACI which exchanges
quote it, then pick two exchanges (preferring bot-tradeable ones) so a
``UACryptoInvestPair`` can be built and history loaded normally.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Iterable, Sequence

from src.scanners.exchange_map import map_source_to_bot

from .catalog import ALL_EXCHANGES
from .client import UACryptoInvestClient
from .config import UACryptoInvestPair


_DEFAULT_TTL_SEC = 300.0


@dataclass(frozen=True)
class UACryptoInvestHistoryOption:
    token: str
    long_exchange: str
    short_exchange: str

    @property
    def bot_long_exchange(self) -> str:
        return map_source_to_bot(self.long_exchange) or self.long_exchange

    @property
    def bot_short_exchange(self) -> str:
        return map_source_to_bot(self.short_exchange) or self.short_exchange

    @property
    def label(self) -> str:
        return f"{self.bot_long_exchange} / {self.bot_short_exchange}"

    @property
    def chart_code(self) -> str:
        return self.to_pair().chart_code

    def to_pair(self) -> UACryptoInvestPair:
        return UACryptoInvestPair(
            token=self.token,
            long_exchange=self.long_exchange,
            short_exchange=self.short_exchange,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "token": self.token,
            "chart_code": self.chart_code,
            "label": self.label,
            "source_long_exchange": self.long_exchange,
            "source_short_exchange": self.short_exchange,
            "long_exchange": self.bot_long_exchange,
            "short_exchange": self.bot_short_exchange,
        }


class UACryptoInvestPairDiscovery:
    """Resolve a ``UACryptoInvestPair`` by token across UACI exchanges.

    Results are cached for ``ttl_sec`` to avoid hammering the upstream
    on every chart fetch — the same dashboard symbol triggers many
    history calls (initial load, timeframe switches, retries).
    """

    def __init__(
        self,
        client_factory=UACryptoInvestClient,
        *,
        exchanges: Sequence = ALL_EXCHANGES,
        ttl_sec: float = _DEFAULT_TTL_SEC,
        clock=time.monotonic,
    ) -> None:
        self._client_factory = client_factory
        self._exchanges = tuple(exchanges)
        self._ttl_sec = float(ttl_sec)
        self._clock = clock
        self._cache: dict[str, tuple[float, tuple[UACryptoInvestHistoryOption, ...]]] = {}
        self._pair_cache: dict[str, UACryptoInvestPair | None] = {}
        self._inflight: dict[str, asyncio.Future[tuple[UACryptoInvestHistoryOption, ...]]] = {}

    async def discover(self, token: str) -> UACryptoInvestPair | None:
        key = (token or "").strip().upper().removesuffix("USDT")
        cached = self._cache.get(key)
        if cached is not None and (self._clock() - cached[0]) < self._ttl_sec and key in self._pair_cache:
            return self._pair_cache[key]
        options = await self.discover_options(token)
        pair = options[0].to_pair() if options else None
        self._pair_cache[key] = pair
        return pair

    async def discover_options(self, token: str) -> tuple[UACryptoInvestHistoryOption, ...]:
        key = (token or "").strip().upper().removesuffix("USDT")
        if not key:
            return ()
        cached = self._cache.get(key)
        if cached is not None and (self._clock() - cached[0]) < self._ttl_sec:
            return cached[1]
        inflight = self._inflight.get(key)
        if inflight is not None:
            return await inflight

        loop = asyncio.get_running_loop()
        future: asyncio.Future[tuple[UACryptoInvestHistoryOption, ...]] = loop.create_future()
        self._inflight[key] = future
        try:
            options = await self._discover_options_uncached(key)
            self._cache[key] = (self._clock(), options)
            future.set_result(options)
            return options
        except Exception as exc:  # noqa: BLE001
            future.set_exception(exc)
            raise
        finally:
            self._inflight.pop(key, None)

    async def _discover_uncached(self, token: str) -> UACryptoInvestPair | None:
        options = await self._discover_options_uncached(token)
        return options[0].to_pair() if options else None

    async def _discover_options_uncached(self, token: str) -> tuple[UACryptoInvestHistoryOption, ...]:
        client = self._client_factory()
        try:
            results = await asyncio.gather(*[
                _safe_search(client, ex.source_id, token)
                for ex in self._exchanges
            ])
        finally:
            await client.aclose()
        carriers = [
            ex.canonical
            for ex, hit in zip(self._exchanges, results)
            if hit
        ]
        return _build_options(token, carriers)


async def _safe_search(client: UACryptoInvestClient, exchange_id: int, token: str) -> bool:
    try:
        payload = await client.search_tokens(
            exchange_id=exchange_id,
            search=token,
            count=5,
        )
    except Exception:  # noqa: BLE001
        return False
    rows = payload.get("result", []) if isinstance(payload, dict) else []
    needle = token.upper()
    for row in rows:
        if isinstance(row, dict) and str(row.get("tokenName") or "").upper() == needle:
            return True
    return False


def _select_pair(token: str, carriers: Iterable[str]) -> UACryptoInvestPair | None:
    seq = list(carriers)
    if len(seq) < 2:
        return None
    tradeable = [name for name in seq if map_source_to_bot(name) is not None]
    if len(tradeable) >= 2:
        long_ex, short_ex = tradeable[0], tradeable[1]
    else:
        long_ex, short_ex = seq[0], seq[1]
    return UACryptoInvestPair(token=token, long_exchange=long_ex, short_exchange=short_ex)


def _build_options(token: str, carriers: Iterable[str]) -> tuple[UACryptoInvestHistoryOption, ...]:
    seq = list(carriers)
    if len(seq) < 2:
        return ()
    tradeable = [name for name in seq if map_source_to_bot(name) is not None]
    usable = tradeable if len(tradeable) >= 2 else seq
    return tuple(
        UACryptoInvestHistoryOption(token=token, long_exchange=long_ex, short_exchange=short_ex)
        for long_ex in usable
        for short_ex in usable
        if long_ex != short_ex
    )
