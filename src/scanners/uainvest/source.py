"""UAInvest scanner — fetches offers, normalizes to ``ScanOffer``."""

from __future__ import annotations

import time
from typing import Any

from loguru import logger

from src.scanners.base import ScanOffer, Scanner
from src.scanners.exchange_map import map_source_to_bot

from .client import UAInvestClient


def _to_float(value: Any, default: float = 0.0) -> float:
    """Best-effort float parse. UAInvest sends numbers as strings sometimes
    (``"0.025250000000"``) and occasionally as null. Accept both, never raise."""
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _funding_to_pct(value: Any) -> float:
    """UAInvest funding fields are decimal rates; the bot stores percent."""
    return _to_float(value) * 100.0


def _normalize_token(value: Any) -> str:
    text = str(value or "").strip().upper()
    return "".join(ch for ch in text if ch.isalnum())


def _symbol_coin(symbol: str) -> str:
    text = _normalize_token(symbol)
    for suffix in ("USDT", "USDC", "USD"):
        if text.endswith(suffix):
            return text[:-len(suffix)]
    return text


class UAInvestScanner(Scanner):
    """Adapter from UAInvest's response shape to ``ScanOffer``."""

    name = "uainvest"

    def __init__(self, client: UAInvestClient | None = None) -> None:
        self._client = client or UAInvestClient()

    async def fetch(self) -> list[ScanOffer]:
        raw = await self._client.fetch_offers()
        offers: list[ScanOffer] = []
        now = time.time()
        for row in raw:
            try:
                offer = self._parse(row, fetched_at=now)
            except Exception as e:  # noqa: BLE001 — never let one bad row kill batch
                logger.debug("UAInvest row parse error: {} (row={})", e, row)
                continue
            if offer is not None:
                offers.append(offer)
        return offers

    async def aclose(self) -> None:
        await self._client.aclose()

    # ── Internals ───────────────────────────────────────────────────────

    @staticmethod
    def _parse(row: dict[str, Any], *, fetched_at: float) -> ScanOffer | None:
        long_item = row.get("long_item") or {}
        short_item = row.get("short_item") or {}
        if not long_item or not short_item:
            return None

        symbol = (row.get("symbol") or long_item.get("symbol") or "").strip()
        if not symbol:
            return None

        coin = (long_item.get("coin") or "").strip() or symbol.replace("USDT", "")
        expected_symbol = _normalize_token(symbol)
        expected_coin = _symbol_coin(symbol)
        leg_symbols = [long_item.get("symbol"), short_item.get("symbol")]
        for leg_symbol in leg_symbols:
            normalized = _normalize_token(leg_symbol)
            if normalized and normalized != expected_symbol:
                return None
        for leg_coin in (long_item.get("coin"), short_item.get("coin")):
            normalized_coin = _normalize_token(leg_coin)
            if normalized_coin and normalized_coin != expected_coin:
                return None
        long_ex = (row.get("long") or long_item.get("exchange") or "").strip().lower()
        short_ex = (
            row.get("short") or short_item.get("exchange") or ""
        ).strip().lower()
        if not long_ex or not short_ex:
            return None

        # ``open_spread_percentage`` is the canonical scanner spread; falls
        # back to ``delta``/price-derived only when the API omits it.
        spread = _to_float(row.get("open_spread_percentage"))
        if spread == 0.0 and row.get("delta") is not None:
            long_price = _to_float(long_item.get("price"))
            if long_price > 0:
                spread = (_to_float(row.get("delta")) / long_price) * 100.0

        # next_funding_ts: UAInvest doesn't expose it directly. Best we can
        # do is leave it None; the matcher treats None as "unknown".
        return ScanOffer(
            source="uainvest",
            symbol=symbol,
            coin=coin,
            source_exchange_long=long_ex,
            source_exchange_short=short_ex,
            bot_exchange_long=map_source_to_bot(long_ex),
            bot_exchange_short=map_source_to_bot(short_ex),
            long_price=_to_float(long_item.get("price")),
            short_price=_to_float(short_item.get("price")),
            open_spread_pct=spread,
            funding_long_pct=_funding_to_pct(long_item.get("funding")),
            funding_short_pct=_funding_to_pct(short_item.get("funding")),
            funding_interval_h_long=_to_int(long_item.get("funding_interval"), 8),
            funding_interval_h_short=_to_int(short_item.get("funding_interval"), 8),
            next_funding_ts=None,
            apr_pct=_to_float(row.get("apr")),
            volume_24h_usdt_long=_to_float(long_item.get("24usdt")),
            volume_24h_usdt_short=_to_float(short_item.get("24usdt")),
            fetched_at=fetched_at,
        )
