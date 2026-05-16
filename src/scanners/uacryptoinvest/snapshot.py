"""No-browser seed snapshot helpers for UACryptoInvest chart pages."""

from __future__ import annotations

import html
import re

from .catalog import chart_exchange_name
from .config import UACryptoInvestPair


_LIVE_FUND_RE = re.compile(
    r"Live\s+Fund:\s*</span>\s*<span[^>]*>\s*([+-]?\d+(?:[\.,]\d+)?)%",
    re.IGNORECASE,
)
_INTERVAL_RE = re.compile(r">\s*(\d+)\s*h\s*</span>", re.IGNORECASE)
_VOLUME_RE = re.compile(
    r"Volume:\s*</span>\s*<span[^>]*>\s*\$?\s*([^<]+?)\s*</span>",
    re.IGNORECASE,
)


def parse_chart_funding(page_html: str, pair: UACryptoInvestPair) -> dict[str, float]:
    return {
        key: value
        for key, value in parse_chart_snapshot(page_html, pair).items()
        if key.endswith("_funding_pct")
    }


def parse_chart_snapshot(page_html: str, pair: UACryptoInvestPair) -> dict[str, float]:
    text = html.unescape(page_html or "")
    out: dict[str, float] = {}
    for side, exchange in (("long", pair.long_exchange), ("short", pair.short_exchange)):
        block = _exchange_block(text, chart_exchange_name(exchange))
        if not block:
            continue
        funding = _funding_from_block(block)
        if funding is not None:
            out[f"{side}_funding_pct"] = funding
        interval = _interval_from_block(block)
        if interval is not None:
            out[f"{side}_interval_h"] = float(interval)
        volume = _volume_from_block(block)
        if volume is not None:
            out[f"{side}_volume_24h_usdt"] = volume
    return out


def _exchange_block(page_html: str, chart_name: str) -> str:
    idx = page_html.find(f'alt="{chart_name}"')
    if idx < 0:
        idx = page_html.find(f">{chart_name}</span>")
    if idx < 0:
        return ""
    return page_html[idx:idx + 3000]


def _funding_from_block(block: str) -> float | None:
    match = _LIVE_FUND_RE.search(block)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def _interval_from_block(block: str) -> int | None:
    match = _INTERVAL_RE.search(block)
    if not match:
        return None
    try:
        return max(1, int(match.group(1)))
    except ValueError:
        return None


def _volume_from_block(block: str) -> float | None:
    match = _VOLUME_RE.search(block)
    if not match:
        return None
    return _parse_compact_number(match.group(1))


def _parse_compact_number(raw: str) -> float | None:
    text = raw.strip().replace(" ", "").replace(",", ".")
    multiplier = 1.0
    if text.endswith(("M", "m")):
        multiplier = 1_000_000.0
        text = text[:-1]
    elif text.endswith(("K", "k")):
        multiplier = 1_000.0
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None