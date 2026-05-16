"""Scanner global filter — single set of thresholds, persisted on disk.

Replaces the old watchlist/rule model. The operator picks two numbers:

    * ``min_spread_pct`` — alert when ``open_spread_pct >=`` this
    * ``min_funding_diff_pct_per_hour`` — alert when
        ``funding_diff_per_hour >=`` this

Telegram delivery is opt-in and stored beside those thresholds so the scanner
module can be removed without touching the core trading code.

Persisted under the ``"scanner"`` key of ``runtime_config.json`` so the
dashboard's other config edits (symbols, trading params, …) and the
scanner's filter live in the same atomic-write file.

A read-modify-write merge is used on each save so changes from the
config controller and the scanner controller don't clobber each other,
which is safe inside a single asyncio loop with our local lock.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from src.adapters.runtime_store import RuntimeConfigStore


@dataclass(frozen=True)
class ScannerFilter:
    """Scanner thresholds plus optional Telegram notification controls."""

    min_spread_pct: float = 0.0
    min_funding_diff_pct_per_hour: float = 0.0
    notify_telegram: bool = False
    renotify_funding_change_pct: float = 0.1
    renotify_spread_change_pct: float = 0.5

    def to_dict(self) -> dict[str, float | bool]:
        return {
            "min_spread_pct": float(self.min_spread_pct),
            "min_funding_diff_pct_per_hour": float(
                self.min_funding_diff_pct_per_hour,
            ),
            "notify_telegram": bool(self.notify_telegram),
            "renotify_funding_change_pct": max(
                0.0, float(self.renotify_funding_change_pct),
            ),
            "renotify_spread_change_pct": max(
                0.0, float(self.renotify_spread_change_pct),
            ),
        }

    @classmethod
    def from_dict(cls, raw: Any) -> "ScannerFilter":
        if not isinstance(raw, dict):
            return cls()
        return cls(
            min_spread_pct=_to_float(raw.get("min_spread_pct"), 0.0),
            min_funding_diff_pct_per_hour=_to_float(
                raw.get("min_funding_diff_pct_per_hour"), 0.0,
            ),
            notify_telegram=_to_bool(raw.get("notify_telegram"), False),
            renotify_funding_change_pct=max(
                0.0,
                _to_float(raw.get("renotify_funding_change_pct"), 0.1),
            ),
            renotify_spread_change_pct=max(
                0.0,
                _to_float(raw.get("renotify_spread_change_pct"), 0.5),
            ),
        )

    def passes(self, offer) -> bool:
        """True when offer satisfies both visible-table thresholds."""
        if offer.open_spread_pct < self.min_spread_pct:
            return False
        if offer.funding_diff_pct_per_hour < self.min_funding_diff_pct_per_hour:
            return False
        return True


def _to_float(value: Any, default: float) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value: Any, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
    return default


class ScannerFilterStore:
    """Async-safe getter/setter backed by ``RuntimeConfigStore``.

    Caches the last loaded value so the poll loop doesn't hit disk on
    every tick. ``set()`` is the only write path; reads after a write
    return the new value immediately.
    """

    KEY = "scanner"

    def __init__(self, store: RuntimeConfigStore) -> None:
        self._store = store
        self._lock = asyncio.Lock()
        self._cached: ScannerFilter = self._load_sync()

    def _load_sync(self) -> ScannerFilter:
        raw = self._store.load()
        return ScannerFilter.from_dict((raw or {}).get(self.KEY))

    async def get(self) -> ScannerFilter:
        return self._cached

    async def set(self, flt: ScannerFilter) -> ScannerFilter:
        async with self._lock:
            # Read-modify-write so we don't clobber unrelated keys
            # (symbols, trading, per_symbol, …).
            data = self._store.load() or {}
            data[self.KEY] = flt.to_dict()
            self._store.save(data)
            self._cached = flt
            return flt
