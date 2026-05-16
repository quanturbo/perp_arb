"""Validation rules for runtime-mutable config.

Pure domain — no I/O, no framework deps. Defines:
- which fields are editable at runtime,
- which fields require a graceful bot restart (vs hot-reload),
- value-range and shape validators.

Consumers:
- adapters/runtime_store.py persists validated overrides to disk.
- web/controllers/config_controller.py validates HTTP payloads.
- orchestrator.py applies overrides on startup + on POST.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from src.settings import MAX_LIMIT_PRICE_SLIPPAGE_PCT


# Fields that require a bot restart to take effect.
# Empty: symbols are now hot-reloadable via Orchestrator.replace_symbols()
# which cycles WS subscriptions in-place without dropping web/trader/storage.
RESTART_REQUIRED_FIELDS: frozenset[str] = frozenset()

# Symbols are hot-reloadable but require a dedicated apply path (not the
# generic update_settings flow). Listed separately so the controller can
# dispatch to Orchestrator.replace_symbols().
HOT_RELOAD_SYMBOL_FIELDS: frozenset[str] = frozenset({"symbols"})

# Trading-block fields that hot-reload via trader.update_settings().
HOT_RELOAD_TRADING_FIELDS: frozenset[str] = frozenset({
    "enabled",
    "entry_spread_pct",
    "close_spread_pct",
    "min_spread_persistence_ms",
    "max_entry_spread_pct",
    "amount_usdt",
    "leverage",
    "order_type",
    "time_in_force",
    "limit_price_slippage_pct",
    "max_quote_to_order_age_ms",
    "max_top_book_usage_pct",
    "max_consecutive_failures",
    "fail_cooldown_sec",
    "post_trade_delay_sec",
    "max_open_positions",
    "max_trades_per_session",
    "max_latency_ms",
})

# Top-level (AppConfig) fields that hot-reload (no restart needed).
HOT_RELOAD_TOP_FIELDS: frozenset[str] = frozenset({
    "trade_exchanges",
    "read_exchanges",
    "min_quote_volume_usd",
})

_SYMBOL_RE = re.compile(r"^[A-Z0-9]+/USDT:USDT$")
_BASE_RE = re.compile(r"^[A-Z0-9]+$")
_VALID_ORDER_TYPES = {"market", "limit"}
_VALID_TIME_IN_FORCE = {"IOC", "FOK", "GTC"}


def normalize_symbol(raw: Any) -> str | None:
    """Coerce a user-entered token into canonical 'BASE/USDT:USDT' form.

    Accepts:
      - 'APE'             -> 'APE/USDT:USDT'
      - 'ape'             -> 'APE/USDT:USDT'
      - 'APE/USDT'        -> 'APE/USDT:USDT'
      - 'APE/USDT:USDT'   -> 'APE/USDT:USDT'

    Returns None if input cannot be normalized (caller surfaces validation error).
    """
    if not isinstance(raw, str):
        return None
    s = raw.strip().upper()
    if not s:
        return None
    if _SYMBOL_RE.match(s):
        return s
    # 'APE/USDT' -> append ':USDT'
    if "/" in s and ":" not in s:
        base, _, quote = s.partition("/")
        if _BASE_RE.match(base) and quote == "USDT":
            return f"{base}/USDT:USDT"
        return None
    # Bare base ticker
    if _BASE_RE.match(s):
        return f"{s}/USDT:USDT"
    return None


def normalize_symbols(values: Any) -> list[str] | None:
    """Normalize a list of symbols. Returns None if input shape is invalid."""
    if not isinstance(values, list):
        return None
    result: list[str] = []
    for v in values:
        norm = normalize_symbol(v)
        if norm is None:
            return None
        if norm not in result:
            result.append(norm)
    return result


@dataclass(frozen=True)
class ValidationError:
    field: str
    message: str


def _err(field: str, msg: str) -> ValidationError:
    return ValidationError(field=field, message=msg)


def _validate_symbols(symbols: Any, errors: list[ValidationError]) -> None:
    if not isinstance(symbols, list) or not symbols:
        errors.append(_err("symbols", "must be a non-empty list"))
        return
    for s in symbols:
        if not isinstance(s, str) or not _SYMBOL_RE.match(s):
            errors.append(_err("symbols", f"invalid symbol {s!r}; expected e.g. 'APE/USDT:USDT'"))


def _validate_trade_exchanges(
    value: Any, available: set[str], errors: list[ValidationError]
) -> None:
    if not isinstance(value, list):
        errors.append(_err("trade_exchanges", "must be a list"))
        return
    for ex in value:
        if not isinstance(ex, str):
            errors.append(_err("trade_exchanges", f"non-string entry: {ex!r}"))
        elif available and ex not in available:
            errors.append(
                _err("trade_exchanges", f"{ex!r} not in available exchanges {sorted(available)}")
            )


def _validate_default_read_exchanges(
    value: Any, available: set[str], errors: list[ValidationError]
) -> None:
    if not isinstance(value, list):
        errors.append(_err("read_exchanges", "must be a list"))
        return
    for ex in value:
        if not isinstance(ex, str):
            errors.append(_err("read_exchanges", f"non-string entry: {ex!r}"))
        elif available and ex not in available:
            errors.append(
                _err("read_exchanges", f"{ex!r} not in available exchanges {sorted(available)}")
            )


def _validate_number(
    field: str, value: Any, lo: float, hi: float, errors: list[ValidationError],
    *, integer: bool = False,
) -> None:
    if integer:
        if not isinstance(value, int) or isinstance(value, bool):
            errors.append(_err(field, f"must be an integer, got {type(value).__name__}"))
            return
    else:
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            errors.append(_err(field, f"must be a number, got {type(value).__name__}"))
            return
    if not (lo <= value <= hi):
        errors.append(_err(field, f"must be in [{lo}, {hi}], got {value}"))


_TRADING_NUM_RULES: dict[str, tuple[float, float, bool]] = {
    # field: (min, max, integer)
    "entry_spread_pct":         (0.001, 100.0, False),
    "close_spread_pct":         (-10.0, 100.0, False),
    "min_spread_persistence_ms":(0.0, 60000.0, False),
    "max_entry_spread_pct":     (1.0, 100.0, False),
    "amount_usdt":              (1.0, 10000.0, False),
    "limit_price_slippage_pct": (0.0, MAX_LIMIT_PRICE_SLIPPAGE_PCT, False),
    "max_quote_to_order_age_ms":(0.0, 60000.0, False),
    "max_top_book_usage_pct":   (0.0, 100.0, False),
    "leverage":                 (1, 25, True),
    "max_consecutive_failures": (1, 100, True),
    "fail_cooldown_sec":        (0.0, 86400.0, False),
    "post_trade_delay_sec":     (0.0, 86400.0, False),
    "max_open_positions":       (1, 1, True),
    "max_trades_per_session":   (1, 10000, True),
    "max_latency_ms":           (1.0, 60000.0, False),
}


def _validate_trading_block(
    trading: Any, errors: list[ValidationError],
) -> None:
    if not isinstance(trading, dict):
        errors.append(_err("trading", "must be an object"))
        return
    for key, val in trading.items():
        if key not in HOT_RELOAD_TRADING_FIELDS:
            errors.append(_err(f"trading.{key}", f"unknown or non-editable field"))
            continue
        if key == "enabled":
            if not isinstance(val, bool):
                errors.append(_err("trading.enabled", "must be a boolean"))
        elif key == "order_type":
            if val not in _VALID_ORDER_TYPES:
                errors.append(
                    _err("trading.order_type", f"must be one of {sorted(_VALID_ORDER_TYPES)}")
                )
        elif key == "time_in_force":
            if val not in _VALID_TIME_IN_FORCE:
                errors.append(
                    _err("trading.time_in_force", f"must be one of {sorted(_VALID_TIME_IN_FORCE)}")
                )
        elif key in _TRADING_NUM_RULES:
            lo, hi, integer = _TRADING_NUM_RULES[key]
            _validate_number(f"trading.{key}", val, lo, hi, errors, integer=integer)

    # Cross-field: close < entry
    entry = trading.get("entry_spread_pct")
    close = trading.get("close_spread_pct")
    if isinstance(entry, (int, float)) and isinstance(close, (int, float)):
        if close >= entry:
            errors.append(
                _err("trading.close_spread_pct", "must be strictly less than entry_spread_pct")
            )


def validate_overrides(
    payload: dict[str, Any],
    *,
    available_exchanges: set[str],
) -> list[ValidationError]:
    """Validate a partial overrides payload. Returns list of errors (empty = OK)."""
    errors: list[ValidationError] = []
    if not isinstance(payload, dict):
        return [_err("", "payload must be a JSON object")]

    allowed_top = {
        "symbols",
        "trade_exchanges",
        "read_exchanges",
        "trading",
        "min_quote_volume_usd",
    }
    for key in payload:
        if key not in allowed_top:
            errors.append(_err(key, "unknown top-level field"))

    if "symbols" in payload:
        _validate_symbols(payload["symbols"], errors)
    if "trade_exchanges" in payload:
        _validate_trade_exchanges(payload["trade_exchanges"], available_exchanges, errors)
    if "read_exchanges" in payload:
        _validate_default_read_exchanges(payload["read_exchanges"], available_exchanges, errors)
    if "min_quote_volume_usd" in payload:
        _validate_global_min_quote_volume(payload["min_quote_volume_usd"], errors)
    trade_list = payload.get("trade_exchanges")
    read_list = payload.get("read_exchanges")
    if (
        isinstance(trade_list, list) and trade_list
        and isinstance(read_list, list)
    ):
        missing = [t for t in trade_list if t not in read_list]
        if missing:
            errors.append(_err(
                "trade_exchanges",
                f"trade exchanges {missing} are not in read_exchanges; "
                "every default trade leg must also be a default read leg",
            ))
    if "trading" in payload:
        _validate_trading_block(payload["trading"], errors)
    return errors


def validate_symbol_overrides(
    payload: dict[str, Any],
    *,
    connected_exchanges: set[str],
) -> list[ValidationError]:
    """Validate a per-symbol overrides patch.

    Differs from `validate_overrides`:
      - `read_exchanges` is allowed (only meaningful per-symbol)
      - `symbols` is NOT allowed (the symbol set is global)
      - exchange membership is checked against the *connected* set
        (operators can only choose from exchanges actually wired up)
    """
    errors: list[ValidationError] = []
    if not isinstance(payload, dict):
        return [_err("", "payload must be a JSON object")]

    allowed_top = {"trade_exchanges", "read_exchanges", "trading", "min_quote_volume_usd"}
    for key in payload:
        if key not in allowed_top:
            errors.append(_err(key, "unknown per-symbol field"))

    if "trade_exchanges" in payload:
        # Per-symbol semantics: empty list is intentional (read/trade none).
        # Differs from the global validator which requires non-empty.
        _validate_per_symbol_exchange_list(
            "trade_exchanges",
            payload["trade_exchanges"], connected_exchanges, errors,
        )
    if "read_exchanges" in payload:
        _validate_per_symbol_exchange_list(
            "read_exchanges",
            payload["read_exchanges"], connected_exchanges, errors,
        )
    if "min_quote_volume_usd" in payload:
        _validate_min_quote_volume(
            payload["min_quote_volume_usd"], connected_exchanges, errors,
        )
    # Cross-field invariant: trader can only execute on a leg whose feed
    # is being read. If the operator set both lists explicitly and there
    # is a trade-exchange that isn't in read_exchanges, the trader would
    # silently never trade it (read filter blocks the snapshot). Reject
    # the patch so the inconsistency is caught at submit time. Empty
    # read_exchanges is valid only with an empty trade list.
    trade_list = payload.get("trade_exchanges")
    read_list = payload.get("read_exchanges")
    if (
        isinstance(trade_list, list) and trade_list
        and isinstance(read_list, list)
    ):
        missing = [t for t in trade_list if t not in read_list]
        if missing:
            errors.append(_err(
                "trade_exchanges",
                f"trade exchanges {missing} are not in read_exchanges; "
                "every trade leg must also be a read leg",
            ))
    if "trading" in payload:
        _validate_trading_block(payload["trading"], errors)
    return errors


def _validate_per_symbol_exchange_list(
    field: str, value: Any, connected: set[str], errors: list[ValidationError],
) -> None:
    """Per-symbol exchange list: empty list = clear override (allowed)."""
    if not isinstance(value, list):
        errors.append(_err(field, "must be a list"))
        return
    for ex in value:
        if not isinstance(ex, str):
            errors.append(_err(field, f"non-string entry: {ex!r}"))
        elif connected and ex not in connected:
            errors.append(
                _err(field, f"{ex!r} not connected; pick from {sorted(connected)}")
            )


def _validate_min_quote_volume(
    value: Any, connected: set[str], errors: list[ValidationError],
) -> None:
    """Minimum 24h quote-volume threshold for liquidity-aware filtering.

    Two accepted shapes:
      - number  → applies to every exchange for this symbol (e.g. 5_000_000)
      - object  → per-exchange map, e.g. {"kucoinfutures": 1_000_000}.
                  Missing exchanges fall through to the global default.
    A negative number disables the filter for the leg(s) it applies to.
    """
    field = "min_quote_volume_usd"
    if isinstance(value, bool):  # bool is subclass of int — reject explicitly
        errors.append(_err(field, "must be a number or {exchange_id: number} object"))
        return
    if isinstance(value, (int, float)):
        return
    if not isinstance(value, dict):
        errors.append(_err(field, "must be a number or {exchange_id: number} object"))
        return
    for ex, amount in value.items():
        if not isinstance(ex, str) or (connected and ex not in connected):
            errors.append(_err(
                field, f"{ex!r} not connected; pick from {sorted(connected)}",
            ))
            continue
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            errors.append(_err(field, f"{ex}: must be a number"))


def _validate_global_min_quote_volume(value: Any, errors: list[ValidationError]) -> None:
    field = "min_quote_volume_usd"
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        errors.append(_err(field, "must be a number"))
        return
    if value < -1:
        errors.append(_err(field, "must be >= -1 (-1 disables liquidity filtering)"))
def requires_restart(payload: dict[str, Any]) -> bool:
    """Return True if any restart-required field is present in the payload."""
    return any(k in payload for k in RESTART_REQUIRED_FIELDS)


def merge_overrides(
    existing: dict[str, Any], patch: dict[str, Any],
) -> dict[str, Any]:
    """Shallow merge top-level keys; deep-merge `trading` block."""
    merged: dict[str, Any] = {**existing}
    for key, val in patch.items():
        if key == "trading" and isinstance(val, dict) and isinstance(merged.get("trading"), dict):
            merged_trading = {**merged["trading"], **val}
            merged["trading"] = merged_trading
        else:
            merged[key] = val
    return merged
