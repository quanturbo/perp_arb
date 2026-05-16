from __future__ import annotations

from src.domain.trader_ports import TraderConfigPort


def resolve_symbol_config(trader: TraderConfigPort, symbol: str) -> dict:
    ov = trader._symbol_overrides.get(symbol) or {}
    trading = ov.get("trading") or {}
    trade_override_present = "trade_exchanges" in ov
    read_override_present = "read_exchanges" in ov
    return {
        "entry_spread_pct": trading.get(
            "entry_spread_pct",
            trader._symbol_entry_thresholds.get(symbol, trader._entry_threshold),
        ),
        "close_spread_pct": trading.get("close_spread_pct", trader._close_threshold),
        "amount_usdt": trading.get("amount_usdt", trader._amount_usdt),
        "max_entry_spread_pct": trading.get(
            "max_entry_spread_pct", trader._max_entry_spread_pct,
        ),
        "order_type": trading.get("order_type", trader._order_type),
        "time_in_force": trading.get("time_in_force", trader._time_in_force),
        "limit_price_slippage_pct": trading.get(
            "limit_price_slippage_pct", trader._limit_price_slippage_pct,
        ),
        "max_quote_to_order_age_ms": trading.get(
            "max_quote_to_order_age_ms", trader._max_quote_to_order_age_ms,
        ),
        "max_top_book_usage_pct": trading.get(
            "max_top_book_usage_pct", trader._max_top_book_usage_pct,
        ),
        "max_consecutive_failures": trading.get(
            "max_consecutive_failures", trader._max_consecutive_failures,
        ),
        "fail_cooldown_sec": trading.get(
            "fail_cooldown_sec", trader._fail_cooldown_sec,
        ),
        "post_trade_delay_sec": trading.get(
            "post_trade_delay_sec", trader._post_trade_delay_sec,
        ),
        "trade_exchanges": (
            set(ov["trade_exchanges"])
            if trade_override_present else trader._trade_exchanges
        ),
        "trade_filter_active": trade_override_present or bool(trader._trade_exchanges),
        "read_exchanges": (
            set(ov["read_exchanges"])
            if read_override_present else set()
        ),
        "read_filter_active": read_override_present,
    }


def exchange_filters_for_symbol(trader: TraderConfigPort, symbol: str) -> dict:
    resolved = resolve_symbol_config(trader, symbol)
    return {
        "trade_exchanges": set(resolved["trade_exchanges"]),
        "trade_filter_active": bool(resolved["trade_filter_active"]),
        "read_exchanges": set(resolved["read_exchanges"]),
        "read_filter_active": bool(resolved["read_filter_active"]),
    }
