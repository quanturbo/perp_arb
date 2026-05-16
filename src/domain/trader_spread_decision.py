from __future__ import annotations

import time

from src.domain.open_failure_backoff import OpenAttemptKey
from src.domain.trader_ports import TraderWorkflowPort
from src.domain.trade_state import TradeState


async def on_spread(trader: TraderWorkflowPort, snapshot) -> None:
    if not trader._enabled:
        return
    if trader._state == TradeState.EXHAUSTED:
        return
    if trader._state == TradeState.IDLE:
        await _handle_idle_spread(trader, snapshot)
        return
    if trader._state == TradeState.OPEN:
        await _handle_open_spread(trader, snapshot)


async def _handle_idle_spread(trader: TraderWorkflowPort, snapshot) -> None:
    now = time.time()
    resolved = trader._resolve(snapshot.symbol)
    attempt_key = OpenAttemptKey.from_snapshot(snapshot)
    if now < trader._next_trade_allowed_ts:
        wait_left = trader._next_trade_allowed_ts - now
        trader._record_skip(
            f"post-trade delay: wait {max(0.0, wait_left):.1f}s",
            key="post_trade_delay",
        )
        return

    if _blocked_by_open_failure(trader, snapshot, attempt_key, resolved, now):
        return
    if _blocked_by_read_filter(snapshot, resolved):
        return

    entry_thresh = resolved["entry_spread_pct"]
    entry_spread = snapshot.price_spread_pct
    if entry_spread < entry_thresh:
        trader._pair_above_threshold_since.pop(
            (snapshot.exchange_long, snapshot.exchange_short), None,
        )
        return

    if _blocked_by_entry_sanity(trader, snapshot, resolved, entry_spread):
        return
    if _blocked_by_trade_filter(trader, snapshot, resolved):
        return
    if trader._check_tick_freshness(snapshot):
        return
    if trader._check_spread_persistence(snapshot):
        return
    await trader._open_position(snapshot)


async def _handle_open_spread(trader: TraderWorkflowPort, snapshot) -> None:
    pos = trader._position
    if not pos:
        return
    if {snapshot.exchange_long, snapshot.exchange_short} != {pos.exchange_long, pos.exchange_short}:
        return
    close_thresh = trader._resolve(snapshot.symbol)["close_spread_pct"]
    if snapshot.price_spread_pct <= close_thresh:
        await trader._close_position(snapshot)


def _blocked_by_open_failure(
    trader: TraderWorkflowPort,
    snapshot,
    attempt_key: OpenAttemptKey,
    resolved: dict,
    now: float,
) -> bool:
    blocked_ex, blocked_reason = trader._open_failure_backoff.blocked_for_pair(
        snapshot.symbol, snapshot.exchange_long, snapshot.exchange_short,
    )
    if blocked_reason:
        trader._last_open_attempt_key = attempt_key
        trader._consecutive_open_failures = 0
        trader._last_open_failure_ts = 0.0
        trader._record_skip(
            f"open blocked for {blocked_ex} on {snapshot.symbol}: {blocked_reason}",
            key=f"order-block:{snapshot.symbol}:{blocked_ex}",
        )
        return True

    in_cooldown, failures, wait_left = trader._open_failure_backoff.cooldown(
        attempt_key,
        max_failures=max(1, int(resolved["max_consecutive_failures"])),
        cooldown_sec=float(resolved["fail_cooldown_sec"]),
        now=now,
    )
    if not in_cooldown:
        return False
    trader._sync_open_failure_state(attempt_key)
    trader._record_skip(
        (
            f"open fail cooldown: {failures} fails for {snapshot.symbol} "
            f"{snapshot.exchange_long}/{snapshot.exchange_short}, "
            f"wait {max(0.0, wait_left):.1f}s"
        ),
        key=f"cooldown:{attempt_key}",
    )
    return True


def _blocked_by_read_filter(snapshot, resolved: dict) -> bool:
    if not resolved["read_filter_active"]:
        return False
    read_exchanges = resolved["read_exchanges"]
    return (
        snapshot.exchange_long not in read_exchanges
        or snapshot.exchange_short not in read_exchanges
    )


def _blocked_by_entry_sanity(
    trader: TraderWorkflowPort, snapshot, resolved: dict, entry_spread: float,
) -> bool:
    if entry_spread <= resolved["max_entry_spread_pct"]:
        return False
    pair = f"{snapshot.exchange_long}/{snapshot.exchange_short}"
    trader._record_skip(
        (
            f"price spread {entry_spread:.2f}% for {snapshot.symbol} "
            f"exceeds sanity cap {resolved['max_entry_spread_pct']:.1f}% "
            f"on {pair} — likely feed bug"
        ),
        key=f"insane_spread:{snapshot.symbol}:{pair}",
    )
    return True


def _blocked_by_trade_filter(
    trader: TraderWorkflowPort, snapshot, resolved: dict,
) -> bool:
    trade_exchanges = resolved["trade_exchanges"]
    if not resolved["trade_filter_active"]:
        return False
    if (
        snapshot.exchange_long in trade_exchanges
        and snapshot.exchange_short in trade_exchanges
    ):
        return False
    pair = f"{snapshot.exchange_long}/{snapshot.exchange_short}"
    trader._record_skip(
        f"non-trade exchange pair: {pair}",
        key=f"non_trade:{pair}",
    )
    return True
