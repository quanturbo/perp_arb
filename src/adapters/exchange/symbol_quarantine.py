"""Per-(exchange, symbol) quarantine with disk persistence.

Single responsibility: remember symbols that should be skipped on a
given exchange, with a precise reason code. A symbol can be skipped
because the raw WS feed never delivered an initial ticker, because the
exchange says the market is unavailable, or because configured liquidity
rules make the leg ineligible.

Trigger rule: ``QUARANTINE_THRESHOLD`` consecutive stall detections from
the watchdog → quarantine. The watchdog calls ``record_stall`` every
time it sees a symbol whose age exceeds ``ws_timeout``; reaching the
threshold returns ``True`` and persists the symbol so it is filtered
out on every subsequent ``start()``.

Persistence format (JSON):

    {
            "<exchange_id>": {
                "<symbol>": {"reason": str, "label": str, "detail": str, "ts": float}
            }
    }

Atomic writes via tempfile + ``os.replace``.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from typing import Any

from loguru import logger


QUARANTINE_THRESHOLD: int = 3

REASON_WS_NO_INITIAL_TICK = "ws_no_initial_tick"
REASON_LOW_LIQUIDITY = "low_liquidity"
REASON_INVALID_SYMBOL = "invalid_symbol"
REASON_MANUAL_FAKE_SIGNAL = "manual_fake_signal"

REASON_LABELS: dict[str, str] = {
    REASON_WS_NO_INITIAL_TICK: "No initial WS book ticker after repeated reconnects",
    REASON_LOW_LIQUIDITY: "Below configured 24h quote-volume threshold",
    REASON_INVALID_SYMBOL: "Exchange reports symbol unavailable",
    REASON_MANUAL_FAKE_SIGNAL: "Manually marked fake scanner signal",
}

STREAM_BLOCKING_REASONS = frozenset({
    REASON_WS_NO_INITIAL_TICK,
    REASON_INVALID_SYMBOL,
})

_LEGACY_REASON_MAP = {
    "stall_threshold": REASON_WS_NO_INITIAL_TICK,
    "stall_threshold (likely delisted)": REASON_WS_NO_INITIAL_TICK,
}


def normalize_quarantine_meta(meta: dict[str, Any] | None) -> dict[str, Any]:
    """Return UI/API-safe metadata without old misleading reason text."""
    meta = meta if isinstance(meta, dict) else {}
    raw_reason = str(meta.get("reason", "") or "")
    reason = str(meta.get("reason_code", "") or raw_reason or REASON_WS_NO_INITIAL_TICK)
    reason = _LEGACY_REASON_MAP.get(reason, reason)
    if "likely delisted" in raw_reason.lower():
        reason = REASON_WS_NO_INITIAL_TICK

    label = str(meta.get("label", "") or REASON_LABELS.get(reason, reason))
    detail = str(meta.get("detail", "") or "")
    if not detail and raw_reason and raw_reason not in _LEGACY_REASON_MAP:
        detail = raw_reason
    return {
        "reason": reason,
        "label": label,
        "detail": detail,
        "ts": float(meta.get("ts", 0.0) or 0.0),
    }


class SymbolQuarantine:
    """Thread-safe (asyncio-safe) quarantine registry.

    Mutations are guarded by a regular ``threading.Lock``. The bot is
    asyncio-single-threaded so the lock is mostly redundant, but it
    keeps tests and any future thread access correct without leaking
    asyncio types into a pure data class.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.Lock()
        # exchange -> symbol -> consecutive stall count
        self._stalls: dict[str, dict[str, int]] = {}
        # exchange -> symbol -> {reason, label, detail, ts}
        self._quarantined: dict[str, dict[str, dict[str, Any]]] = self._load()

    # ── persistence ──────────────────────────────────────────────────

    def _load(self) -> dict[str, dict[str, dict[str, Any]]]:
        if not os.path.exists(self._path):
            return {}
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.loads(f.read())
        except (OSError, json.JSONDecodeError) as e:
            logger.error(
                "symbol_quarantine: failed to load {}: {} — starting empty",
                self._path, e,
            )
            return {}
        if not isinstance(data, dict):
            return {}
        out: dict[str, dict[str, dict[str, Any]]] = {}
        for exchange, symbols in data.items():
            if not isinstance(symbols, dict):
                continue
            inner: dict[str, dict[str, Any]] = {}
            for symbol, meta in symbols.items():
                if isinstance(meta, dict):
                    inner[symbol] = normalize_quarantine_meta(meta)
            if inner:
                out[exchange] = inner
        return out

    def _save_locked(self) -> None:
        directory = os.path.dirname(os.path.abspath(self._path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".symbol_quarantine.", suffix=".tmp", dir=directory,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._quarantined, f, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    # ── public API ───────────────────────────────────────────────────

    def is_quarantined(self, exchange: str, symbol: str) -> bool:
        with self._lock:
            return symbol in self._quarantined.get(exchange, {})

    def has_reason(self, exchange: str, symbol: str, reason: str) -> bool:
        with self._lock:
            meta = self._quarantined.get(exchange, {}).get(symbol)
            return bool(meta and meta.get("reason") == reason)

    def quarantined_for(self, exchange: str) -> dict[str, dict[str, Any]]:
        with self._lock:
            return dict(self._quarantined.get(exchange, {}))

    def filter(
        self,
        exchange: str,
        symbols: list[str],
        *,
        reasons: frozenset[str] | set[str] | None = None,
    ) -> tuple[list[str], list[str]]:
        """Return ``(allowed, blocked)`` partition for ``symbols``."""
        with self._lock:
            entries = self._quarantined.get(exchange, {})
            if reasons is None:
                blocked_set = set(entries.keys())
            else:
                blocked_set = {
                    symbol for symbol, meta in entries.items()
                    if meta.get("reason") in reasons
                }
        allowed = [s for s in symbols if s not in blocked_set]
        blocked = [s for s in symbols if s in blocked_set]
        return allowed, blocked

    def filter_stream_blocking(
        self, exchange: str, symbols: list[str],
    ) -> tuple[list[str], list[str]]:
        """Filter only quarantine reasons that should suppress subscriptions."""
        return self.filter(exchange, symbols, reasons=STREAM_BLOCKING_REASONS)

    def record_stall(
        self,
        exchange: str,
        symbol: str,
        *,
        reason: str = "",
        reason_code: str = REASON_WS_NO_INITIAL_TICK,
        detail: str = "",
    ) -> bool:
        """Bump consecutive stall counter; return True if newly quarantined."""
        with self._lock:
            if symbol in self._quarantined.get(exchange, {}):
                return False
            counts = self._stalls.setdefault(exchange, {})
            counts[symbol] = counts.get(symbol, 0) + 1
            if counts[symbol] < QUARANTINE_THRESHOLD:
                return False
            counts.pop(symbol, None)
            normalized = normalize_quarantine_meta({
                "reason": reason_code or reason or REASON_WS_NO_INITIAL_TICK,
                "detail": detail or reason,
                "ts": time.time(),
            })
            self._quarantined.setdefault(exchange, {})[symbol] = normalized
            try:
                self._save_locked()
            except Exception as exc:  # pragma: no cover - persistence best-effort
                logger.error(
                    "symbol_quarantine: failed to persist {}/{}: {}",
                    exchange, symbol, exc,
                )
            return True

    def record_low_liquidity(
        self,
        exchange: str,
        symbol: str,
        *,
        quote_volume_24h: float,
        min_quote_volume_usd: float,
    ) -> bool:
        """Persist a configured liquidity exclusion without strike counting."""
        with self._lock:
            existing = self._quarantined.get(exchange, {}).get(symbol)
            if existing and existing.get("reason") == REASON_LOW_LIQUIDITY:
                return False
            detail = (
                f"24h quote volume {quote_volume_24h:.0f} < "
                f"min {min_quote_volume_usd:.0f}"
            )
            self._quarantined.setdefault(exchange, {})[symbol] = normalize_quarantine_meta({
                "reason": REASON_LOW_LIQUIDITY,
                "detail": detail,
                "ts": time.time(),
            })
            try:
                self._save_locked()
            except Exception as exc:  # pragma: no cover - persistence best-effort
                logger.error(
                    "symbol_quarantine: failed to persist low-liquidity {}/{}: {}",
                    exchange, symbol, exc,
                )
            return True

    def clear_low_liquidity(self, exchange: str, symbol: str) -> bool:
        """Remove only an automatically-created low-liquidity quarantine."""
        with self._lock:
            ex = self._quarantined.get(exchange)
            meta = ex.get(symbol) if ex else None
            if not meta or meta.get("reason") != REASON_LOW_LIQUIDITY:
                return False
            ex.pop(symbol, None)
            if not ex:
                self._quarantined.pop(exchange, None)
            try:
                self._save_locked()
            except Exception as exc:  # pragma: no cover
                logger.error(
                    "symbol_quarantine: failed to persist liquidity recovery {}/{}: {}",
                    exchange, symbol, exc,
                )
            return True

    def record_manual_fake_signal(
        self,
        exchange: str,
        symbol: str,
        *,
        detail: str = "",
    ) -> bool:
        """Persist an operator-marked fake scanner signal."""
        with self._lock:
            existing = self._quarantined.get(exchange, {}).get(symbol)
            if existing and existing.get("reason") == REASON_MANUAL_FAKE_SIGNAL:
                return False
            self._quarantined.setdefault(exchange, {})[symbol] = normalize_quarantine_meta({
                "reason": REASON_MANUAL_FAKE_SIGNAL,
                "detail": detail,
                "ts": time.time(),
            })
            try:
                self._save_locked()
            except Exception as exc:  # pragma: no cover
                logger.error(
                    "symbol_quarantine: failed to persist manual fake {}/{}: {}",
                    exchange, symbol, exc,
                )
            return True

    def record_recovery(self, exchange: str, symbol: str) -> None:
        """Symbol delivered a tick — reset its stall counter."""
        with self._lock:
            counts = self._stalls.get(exchange)
            if counts and symbol in counts:
                counts.pop(symbol, None)

    def reinstate(self, exchange: str, symbol: str) -> bool:
        with self._lock:
            ex = self._quarantined.get(exchange)
            if not ex or symbol not in ex:
                return False
            ex.pop(symbol, None)
            if not ex:
                self._quarantined.pop(exchange, None)
            try:
                self._save_locked()
            except Exception as exc:  # pragma: no cover
                logger.error(
                    "symbol_quarantine: failed to persist reinstate {}/{}: {}",
                    exchange, symbol, exc,
                )
        return True

    def snapshot(self) -> dict[str, dict[str, dict[str, Any]]]:
        with self._lock:
            return {
                ex: {sym: normalize_quarantine_meta(meta) for sym, meta in syms.items()}
                for ex, syms in self._quarantined.items()
            }
