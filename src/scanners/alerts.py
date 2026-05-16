"""Telegram alert wrapper for scanner offers that pass the global filter.

Wraps the existing ``TelegramNotifier`` rather than reinventing dedup. The
extra layer exists because:
  * scanner alerts have a different cooldown (long, per symbol+pair) than
    trade open/close events
  * we want a single-line public method that takes a ``ScanOffer`` so the
    service stays declarative
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from itertools import islice
from typing import Protocol

from src.adapters.telegram import format_operator_notification
from src.scanners.base import ScanOffer


class _SendThrottled(Protocol):
    """Minimal slice of TelegramNotifier we depend on (ISP)."""

    async def send_throttled(
        self, key: str, message: str, cooldown_sec: float = 300.0,
    ) -> None: ...


@dataclass
class _AlertState:
    funding_diff_pct_per_hour: float
    open_spread_pct: float
    renotify_count: int = 0


class ScannerAlerter:
    """Render + dispatch Telegram messages for scanner matches."""

    DEFAULT_COOLDOWN_SEC = 30 * 60.0  # 30 minutes per (symbol, pair)
    MAX_OFFERS_PER_MESSAGE = 12

    def __init__(
        self,
        notifier: _SendThrottled,
        *,
        cooldown_sec: float | None = None,
    ) -> None:
        self._notifier = notifier
        self._cooldown = cooldown_sec or self.DEFAULT_COOLDOWN_SEC
        self._last: dict[str, _AlertState] = {}
        self._batch_count = 0

    async def alert(
        self,
        offer: ScanOffer,
        *,
        renotify_funding_change_pct: float = 0.1,
        renotify_spread_change_pct: float = 0.5,
    ) -> bool:
        msg = self.format(offer)
        base_key = self._key(offer)
        key = self._notification_key(
            base_key,
            offer.funding_diff_pct_per_hour,
            offer.open_spread_pct,
            renotify_funding_change_pct,
            renotify_spread_change_pct,
        )
        if key is None:
            return False
        await self._notifier.send_throttled(key, msg, self._cooldown)
        return True

    async def alert_many(
        self,
        offers: list[ScanOffer],
        *,
        renotify_funding_change_pct: float = 0.1,
        renotify_spread_change_pct: float = 0.5,
        force: bool = False,
    ) -> int:
        selected: list[tuple[ScanOffer, str]] = []
        for offer in offers:
            base_key = self._key(offer)
            if force:
                key = self._forced_notification_key(offer, base_key)
            else:
                key = self._notification_key(
                    base_key,
                    offer.funding_diff_pct_per_hour,
                    offer.open_spread_pct,
                    renotify_funding_change_pct,
                    renotify_spread_change_pct,
                )
            if key is not None:
                selected.append((offer, key))

        sent_count = 0
        iterator = iter(selected)
        while True:
            chunk = list(islice(iterator, self.MAX_OFFERS_PER_MESSAGE))
            if not chunk:
                break
            if len(chunk) == 1:
                offer, key = chunk[0]
                await self._notifier.send_throttled(
                    key, self.format(offer), self._cooldown,
                )
            else:
                self._batch_count += 1
                await self._notifier.send_throttled(
                    self._batch_key([key for _offer, key in chunk]),
                    self.format_batch([offer for offer, _key in chunk]),
                    self._cooldown,
                )
            sent_count += len(chunk)
        return sent_count

    def observe_many(self, offers: list[ScanOffer]) -> None:
        for offer in offers:
            self._remember(offer)

    def reset(self) -> None:
        self._last.clear()
        self._batch_count = 0

    @staticmethod
    def _key(offer: ScanOffer) -> str:
        return (
            f"scanner:{offer.symbol}:"
            f"{offer.source_exchange_long}-{offer.source_exchange_short}"
        )

    @staticmethod
    def _batch_key(keys: list[str]) -> str:
        raw = "|".join(sorted(keys))
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        return f"scanner:batch:{digest}"

    def _remember(self, offer: ScanOffer) -> None:
        self._last[self._key(offer)] = _AlertState(
            funding_diff_pct_per_hour=offer.funding_diff_pct_per_hour,
            open_spread_pct=offer.open_spread_pct,
        )

    def _forced_notification_key(self, offer: ScanOffer, base_key: str) -> str:
        previous = self._last.get(base_key)
        if previous is None:
            self._remember(offer)
            return base_key
        previous.funding_diff_pct_per_hour = offer.funding_diff_pct_per_hour
        previous.open_spread_pct = offer.open_spread_pct
        previous.renotify_count += 1
        return f"{base_key}:signal:{previous.renotify_count}"

    def _notification_key(
        self,
        base_key: str,
        funding_diff_pct_per_hour: float,
        open_spread_pct: float,
        renotify_funding_change_pct: float,
        renotify_spread_change_pct: float,
    ) -> str | None:
        previous = self._last.get(base_key)
        if previous is None:
            self._last[base_key] = _AlertState(
                funding_diff_pct_per_hour=funding_diff_pct_per_hour,
                open_spread_pct=open_spread_pct,
            )
            return base_key

        funding_threshold = max(0.0, float(renotify_funding_change_pct or 0.0))
        spread_threshold = max(0.0, float(renotify_spread_change_pct or 0.0))
        funding_changed = (
            funding_threshold > 0.0
            and abs(funding_diff_pct_per_hour - previous.funding_diff_pct_per_hour)
            >= funding_threshold
        )
        spread_changed = (
            spread_threshold > 0.0
            and abs(open_spread_pct - previous.open_spread_pct) >= spread_threshold
        )
        if not funding_changed and not spread_changed:
            return None

        previous.funding_diff_pct_per_hour = funding_diff_pct_per_hour
        previous.open_spread_pct = open_spread_pct
        previous.renotify_count += 1
        return f"{base_key}:renotify:{previous.renotify_count}"

    @staticmethod
    def format_batch(offers: list[ScanOffer]) -> str:
        lines = []
        for offer in offers:
            lines.append(
                f"<b>{offer.symbol}</b> {offer.source_exchange_long}/"
                f"{offer.source_exchange_short}: funding/h "
                f"{offer.funding_diff_pct_per_hour:+.4f}%; "
                f"spread {offer.open_spread_pct:.2f}%"
            )
        token = offers[0].coin if len(offers) == 1 else f"{len(offers)} tokens"
        return format_operator_notification(
            level="INFO",
            type_="SCANNER_OFFERS",
            exchange="uainvest",
            symbol=token,
            reason="<br>".join(lines),
            reason_html=True,
        )

    @staticmethod
    def format(offer: ScanOffer) -> str:
        """Build the Telegram message body — ticker, both legs, fundings,
        benefit (per-hour), spread. No volume (operator dropped it). Funding
        time is shown bold when both legs share the interval."""
        same = offer.intervals_match
        long_t = (
            f"<b>{offer.funding_interval_h_long}h</b>" if same
            else f"{offer.funding_interval_h_long}h"
        )
        short_t = (
            f"<b>{offer.funding_interval_h_short}h</b>" if same
            else f"{offer.funding_interval_h_short}h"
        )
        return format_operator_notification(
            level="INFO",
            type_="SCANNER_OFFER",
            exchange=f"{offer.source_exchange_long}/{offer.source_exchange_short}",
            symbol=offer.coin,
            reason=(
                f"{offer.source} {offer.symbol}; "
                f"long {offer.source_exchange_long} @ {offer.long_price:g}, "
                f"fund {offer.funding_long_pct:+.4f}% / {long_t}; "
                f"short {offer.source_exchange_short} @ {offer.short_price:g}, "
                f"fund {offer.funding_short_pct:+.4f}% / {short_t}; "
                f"funding/h {offer.funding_diff_pct_per_hour:+.4f}%; "
                f"spread {offer.open_spread_pct:.2f}%"
            ),
            reason_html=True,
        )
