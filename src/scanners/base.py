"""Scanner abstractions — pure model + ABC, no I/O.

Why a base class:
  * ``UAInvestScanner`` is the first source, but the bot will plausibly want
    others (Coinglass, in-house funding scanner). Coding the service against
    a ``Scanner`` interface keeps that door open without speculative work
    today (OCP without YAGNI).
  * ``ScanOffer`` is the *normalized* row the rest of the system consumes.
    Source-specific quirks (UAInvest's stringly-typed numbers, name
    mismatches) live in the source's ``parse`` step, not in callers.
"""

from __future__ import annotations

import abc
import time
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ScanOffer:
    """One arbitrage opportunity surfaced by an external scanner.

    All numeric fields are floats in their natural units (percent for
    spreads/funding, seconds for timestamps, USDT for prices/volumes).

    ``source_exchange_long/short`` keep the scanner's raw identifiers so
    the UI can display them when no bot mapping exists. ``bot_exchange_*``
    are the local identifiers (e.g. ``binanceusdm``) when the source ids
    map to one of our connected exchanges; otherwise ``None``.
    """

    source: str
    symbol: str
    coin: str

    source_exchange_long: str
    source_exchange_short: str
    bot_exchange_long: str | None
    bot_exchange_short: str | None

    long_price: float
    short_price: float
    open_spread_pct: float

    funding_long_pct: float
    funding_short_pct: float
    funding_interval_h_long: int
    funding_interval_h_short: int
    next_funding_ts: float | None

    apr_pct: float
    volume_24h_usdt_long: float
    volume_24h_usdt_short: float

    chart_url: str | None = None
    source_rank: int | None = None
    source_pair_count: int | None = None

    fetched_at: float = field(default_factory=time.time)

    # ── Derived helpers (no external state, safe to compute on demand) ──

    @staticmethod
    def _per_hour(rate_pct: float, interval_h: int) -> float:
        # Defensive: UAInvest sometimes returns 0/None for interval — treat
        # as 1h to avoid div-by-zero. Operator sees raw rate verbatim, this
        # is only used for the comparable hourly view.
        ih = max(1, int(interval_h))
        return rate_pct / ih

    @property
    def funding_per_hour_long_pct(self) -> float:
        return self._per_hour(self.funding_long_pct, self.funding_interval_h_long)

    @property
    def funding_per_hour_short_pct(self) -> float:
        return self._per_hour(self.funding_short_pct, self.funding_interval_h_short)

    @property
    def funding_diff_pct(self) -> float:
        """Raw funding benefit for the next funding event: short - long."""
        return self.funding_short_pct - self.funding_long_pct

    @staticmethod
    def _next_interval_boundary_ts(
        interval_h: int, now: float | None = None,
    ) -> float:
        ih = max(1, int(interval_h))
        ref = now if now is not None else time.time()
        interval_sec = ih * 3600
        return float((int(ref // interval_sec) + 1) * interval_sec)

    @staticmethod
    def _seconds_until(ts: float, now: float | None = None) -> float:
        ref = now if now is not None else time.time()
        return max(0.0, ts - ref)

    def next_funding_ts_long(self, now: float | None = None) -> float:
        if self.next_funding_ts is not None:
            return self.next_funding_ts
        return self._next_interval_boundary_ts(self.funding_interval_h_long, now)

    def next_funding_ts_short(self, now: float | None = None) -> float:
        if self.next_funding_ts is not None:
            return self.next_funding_ts
        return self._next_interval_boundary_ts(self.funding_interval_h_short, now)

    def seconds_to_next_funding_long(self, now: float | None = None) -> float:
        return self._seconds_until(self.next_funding_ts_long(now), now)

    def seconds_to_next_funding_short(self, now: float | None = None) -> float:
        return self._seconds_until(self.next_funding_ts_short(now), now)

    def seconds_to_next_funding(self, now: float | None = None) -> float:
        return min(
            self.seconds_to_next_funding_long(now),
            self.seconds_to_next_funding_short(now),
        )

    def funding_diff_pct_per_hour_at(self, now: float | None = None) -> float:
        return self.funding_per_hour_short_pct - self.funding_per_hour_long_pct

    @property
    def funding_diff_pct_per_hour(self) -> float:
        """Per-hour funding benefit, signed: short − long.

        Each leg's raw cycle rate is normalized by its own funding interval
        before subtraction so e.g. a 1h rate is comparable to a 4h/8h rate.
        This matches the dashboard opportunity board: positive funding on
        the short leg is income, positive funding on the long leg is cost.
        """
        return self.funding_diff_pct_per_hour_at()

    @property
    def intervals_match(self) -> bool:
        """True when both legs share the same funding interval — UI bolds
        the funding-time cells in that case so the operator can see at a
        glance that the two rates are directly comparable."""
        return self.funding_interval_h_long == self.funding_interval_h_short

    def minutes_to_next_funding(self, now: float | None = None) -> float | None:
        return self.seconds_to_next_funding(now) / 60.0

    def is_tradeable_by_bot(self) -> bool:
        """True when both legs map to exchanges this bot knows how to trade."""
        return bool(self.bot_exchange_long and self.bot_exchange_short)


class Scanner(abc.ABC):
    """Source of ``ScanOffer`` rows. One implementation per upstream API."""

    #: Stable identifier used as ``ScanOffer.source`` and in API URLs.
    name: str = ""

    @abc.abstractmethod
    async def fetch(self) -> list[ScanOffer]:
        """Return current snapshot. Implementations should swallow transient
        network errors and return ``[]`` rather than raising — the service
        loop relies on this to keep polling forward."""

    async def aclose(self) -> None:
        """Optional cleanup hook (e.g. close aiohttp session)."""
