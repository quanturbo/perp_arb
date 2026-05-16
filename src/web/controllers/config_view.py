from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.config import _sanitize_per_symbol_override
from src.domain.runtime_config import (
    HOT_RELOAD_SYMBOL_FIELDS,
    HOT_RELOAD_TOP_FIELDS,
    HOT_RELOAD_TRADING_FIELDS,
)

if TYPE_CHECKING:
    from src.config import AppConfig


SYMBOL_TRADING_DEFAULT_FIELDS = (
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
)

EFFECTIVE_VIEW_TRADING_FIELDS = (
    "enabled",
    "entry_spread_pct",
    "close_spread_pct",
    "min_spread_persistence_ms",
    "max_entry_spread_pct",
    "amount_usdt",
    "leverage",
    "margin_mode",
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
)


def build_effective_config_view(
    config: "AppConfig",
    *,
    symbol: str | None = None,
) -> dict[str, Any]:
    trading_settings = config.trading
    override = config.symbol_overrides(symbol) if symbol else {}
    override_trading = override.get("trading") or {}

    view: dict[str, Any] = {
        "symbols": list(config.symbols),
        "trade_exchanges": list(
            override["trade_exchanges"] if "trade_exchanges" in override
            else config.trade_exchanges
        ),
        "read_exchanges": list(
            override["read_exchanges"] if "read_exchanges" in override
            else config.read_exchanges
        ),
        "available_exchanges": list(config.available_exchanges),
        "connected_exchanges": list(config.connected_exchange_ids),
        "parallel_legs": True,
        "min_quote_volume_usd": (
            override["min_quote_volume_usd"] if "min_quote_volume_usd" in override
            else config.min_quote_volume_usd
        ),
        "hot_reload_fields": sorted(
            HOT_RELOAD_TRADING_FIELDS | HOT_RELOAD_TOP_FIELDS | HOT_RELOAD_SYMBOL_FIELDS
        ),
        "restart_required_fields": [],
    }
    for field in EFFECTIVE_VIEW_TRADING_FIELDS:
        view[field] = override_trading.get(field, getattr(trading_settings, field))

    if symbol:
        view["symbol"] = symbol
        view["override"] = _override_block(override, override_trading)
    return view


def default_symbol_overrides(config: "AppConfig") -> dict[str, Any]:
    return _sanitize_per_symbol_override({
        "read_exchanges": list(config.read_exchanges),
        "trade_exchanges": list(config.trade_exchanges),
        "trading": _symbol_trading_defaults(config),
    })


def symbol_defaults_from_global(
    config: "AppConfig",
    *,
    defaults_patch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    defaults_patch = defaults_patch or {}
    trading_patch = defaults_patch.get("trading") or {}
    read_exchanges = defaults_patch.get("read_exchanges", config.read_exchanges)
    trade_exchanges = defaults_patch.get("trade_exchanges", config.trade_exchanges)
    return _sanitize_per_symbol_override({
        "read_exchanges": list(read_exchanges),
        "trade_exchanges": list(trade_exchanges),
        "trading": _symbol_trading_defaults(config, trading_patch=trading_patch),
    })


def _symbol_trading_defaults(
    config: "AppConfig",
    *,
    trading_patch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    trading_patch = trading_patch or {}
    trading_settings = config.trading
    return {
        field: trading_patch.get(field, getattr(trading_settings, field))
        for field in SYMBOL_TRADING_DEFAULT_FIELDS
    }


def _override_block(
    override: dict[str, Any],
    override_trading: dict[str, Any],
) -> dict[str, Any]:
    block: dict[str, Any] = {}
    for field in ("trade_exchanges", "read_exchanges"):
        if field in override:
            block[field] = list(override[field])
    if "min_quote_volume_usd" in override:
        block["min_quote_volume_usd"] = override["min_quote_volume_usd"]
    if override_trading:
        block["trading"] = dict(override_trading)
    return block
