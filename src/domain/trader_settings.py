from __future__ import annotations

from typing import Any

from src.settings import TradingSettings


TRADER_SETTING_ATTRS = {
    "enabled": "_enabled",
    "entry_spread_pct": "_entry_threshold",
    "close_spread_pct": "_close_threshold",
    "amount_usdt": "_amount_usdt",
    "max_trades_per_session": "_max_trades",
    "max_latency_ms": "_max_latency_ms",
    "fail_cooldown_sec": "_fail_cooldown_sec",
    "post_trade_delay_sec": "_post_trade_delay_sec",
    "max_consecutive_failures": "_max_consecutive_failures",
    "order_type": "_order_type",
    "time_in_force": "_time_in_force",
    "limit_price_slippage_pct": "_limit_price_slippage_pct",
    "max_quote_to_order_age_ms": "_max_quote_to_order_age_ms",
    "max_top_book_usage_pct": "_max_top_book_usage_pct",
    "max_entry_spread_pct": "_max_entry_spread_pct",
}

TRADING_PASSTHROUGH_FIELDS = (
    "max_open_positions",
    "leverage",
    "margin_mode",
)


def apply_trading_settings_update(
    *,
    trader: object,
    settings: TradingSettings,
    trading: dict[str, Any],
) -> dict[str, tuple]:
    changes: dict[str, tuple] = {}

    def set_trader_attr(attr: str, new_val: Any) -> None:
        old_val = getattr(trader, attr)
        if old_val != new_val:
            setattr(trader, attr, new_val)
            changes[attr.lstrip("_")] = (old_val, new_val)

    for key, attr in TRADER_SETTING_ATTRS.items():
        if key in trading:
            set_trader_attr(attr, trading[key])

    if "min_spread_persistence_ms" in trading:
        old = settings.min_spread_persistence_ms
        settings.min_spread_persistence_ms = float(trading["min_spread_persistence_ms"])
        if old != settings.min_spread_persistence_ms:
            changes["min_spread_persistence_ms"] = (
                old,
                settings.min_spread_persistence_ms,
            )

    for passthrough in TRADING_PASSTHROUGH_FIELDS:
        if passthrough in trading:
            old = getattr(settings, passthrough)
            new = trading[passthrough]
            if old != new:
                setattr(settings, passthrough, new)
                changes[passthrough] = (old, new)

    return changes


def trade_exchange_update(
    current: set[str], new_values: list[str],
) -> tuple[set[str], tuple[list[str], list[str]] | None]:
    old = sorted(current)
    new_set = set(new_values)
    new = sorted(new_set)
    if old == new:
        return current, None
    return new_set, (old, new)
