from __future__ import annotations

import time
from typing import Any

from src.domain.trader_ports import TraderSnapshotPort
from src.domain.trade_position import LegInfo, Position


def build_trader_snapshot(trader: TraderSnapshotPort) -> dict[str, Any]:
    blocked_exchanges = trader._open_failure_backoff.blocked_exchanges()
    result: dict[str, Any] = {
        "enabled": trader._enabled,
        "state": trader._state.value,
        "trades_done": trader._trades_done,
        "max_trades": trader._max_trades,
        "trade_exchanges": sorted(trader._trade_exchanges),
        "entry_threshold": trader._entry_threshold,
        "symbol_entry_thresholds": trader._symbol_entry_thresholds,
        "close_threshold": trader._close_threshold,
        "amount_usdt": trader._amount_usdt,
        "max_latency_ms": trader._max_latency_ms,
        "post_trade_delay_sec": trader._post_trade_delay_sec,
        "post_trade_wait_sec": max(
            0.0,
            round(trader._next_trade_allowed_ts - time.time(), 1),
        ),
        "open_failures": trader._consecutive_open_failures,
        "max_consecutive_failures": trader._max_consecutive_failures,
        "fail_cooldown_sec": trader._fail_cooldown_sec,
        "blocked_exchanges": blocked_exchanges,
        "open_failure_blocked_reason": _last_open_block_reason(trader),
        "last_skip_reason": trader._last_skip_reason,
    }
    if trader._position:
        result["position"] = _position_snapshot(trader._position)
    return result


def _last_open_block_reason(trader: TraderSnapshotPort) -> str:
    last_key = trader._last_open_attempt_key
    if last_key is None:
        return ""
    blocked_ex, blocked_reason = trader._open_failure_backoff.blocked_for_pair(
        last_key.symbol,
        last_key.exchange_long,
        last_key.exchange_short,
    )
    if not blocked_reason:
        return ""
    return f"{blocked_ex}: {blocked_reason}"


def _position_snapshot(position: Position) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "symbol": position.symbol,
        "exchange_long": position.exchange_long,
        "exchange_short": position.exchange_short,
        "entry_spread_pct": position.entry_spread_pct,
        "amount_usdt": position.amount_usdt,
        "opened_at": position.opened_at,
        "open_latency_ms": round(position.open_latency_ms, 1),
    }
    for key, leg in (
        ("open_long", position.open_long),
        ("open_short", position.open_short),
    ):
        if leg:
            snapshot[key] = _leg_snapshot(leg)
    return snapshot


def _leg_snapshot(leg: LegInfo) -> dict[str, Any]:
    return {
        "exchange": leg.exchange_id,
        "side": leg.side,
        "quoted_price": leg.quoted_price,
        "fill_price": leg.fill_price,
        "filled_amount": leg.filled_amount,
        "slippage_pct": round(leg.slippage_pct, 4),
        "tick_age_ms": round(leg.tick_age_ms, 1),
        "latency_ms": round(leg.latency_ms, 1),
        "order_id": leg.order_id,
    }
