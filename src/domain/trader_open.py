from __future__ import annotations

import asyncio
import math
import time

from loguru import logger

from src.domain.fill_reconciler import FillReconciler, ReconcileOutcome
from src.domain.open_failure_backoff import OpenAttemptKey
from src.domain.trader_ports import TraderWorkflowPort
from src.domain.trade_position import Position, _log_trade_summary, _make_leg
from src.domain.trade_state import TradeState


async def open_position(trader: TraderWorkflowPort, snapshot) -> None:
    trader._state = TradeState.OPENING
    trader._last_skip_reason = ""
    attempt_key = OpenAttemptKey.from_snapshot(snapshot)
    resolved = trader._resolve(snapshot.symbol)
    amount_usdt = float(resolved["amount_usdt"])
    order_type = resolved["order_type"]
    time_in_force = resolved["time_in_force"]
    limit_price_slippage_pct = float(resolved["limit_price_slippage_pct"])
    max_quote_to_order_age_ms = float(resolved["max_quote_to_order_age_ms"] or 0.0)
    max_top_book_usage_pct = float(resolved["max_top_book_usage_pct"] or 0.0)

    quoted_long, tick_age_long, quote_age_long = trader._extract_quote(
        snapshot.prices, snapshot.exchange_long, use_ask=True,
    )
    quoted_short, tick_age_short, quote_age_short = trader._extract_quote(
        snapshot.prices, snapshot.exchange_short, use_ask=False,
    )
    long_ask_qty = trader._extract_top_book_qty(
        snapshot.prices, snapshot.exchange_long, use_ask=True,
    )
    short_bid_qty = trader._extract_top_book_qty(
        snapshot.prices, snapshot.exchange_short, use_ask=False,
    )

    long_conn = trader._streams.get_connection(snapshot.exchange_long)
    short_conn = trader._streams.get_connection(snapshot.exchange_short)
    if not long_conn or not short_conn:
        logger.error(
            "Cannot trade: missing connection for {} or {}",
            snapshot.exchange_long,
            snapshot.exchange_short,
        )
        trader._state = TradeState.IDLE
        return

    if quoted_long <= 0 or quoted_short <= 0:
        logger.error(
            "Cannot trade {}: zero quoted price (long={}, short={})",
            snapshot.symbol,
            quoted_long,
            quoted_short,
        )
        trader._state = TradeState.IDLE
        return

    base_qty_raw = amount_usdt / ((quoted_long + quoted_short) / 2.0)
    long_contract_size = long_conn.get_contract_size(snapshot.symbol) if hasattr(long_conn, "get_contract_size") else 1.0
    short_contract_size = short_conn.get_contract_size(snapshot.symbol) if hasattr(short_conn, "get_contract_size") else 1.0
    common_multiple = float(math.lcm(
        max(1, int(long_contract_size * 1000)),
        max(1, int(short_contract_size * 1000)),
    )) / 1000.0
    if common_multiple <= 0:
        common_multiple = 1.0
    base_qty = math.floor(base_qty_raw / common_multiple) * common_multiple

    if base_qty <= 0:
        trader._record_skip(
            (
                f"{snapshot.symbol} base_qty rounds to 0 "
                f"(raw={base_qty_raw:.6f}, step={common_multiple})"
            ),
            key=f"lot_size:{snapshot.symbol}",
        )
        trader._state = TradeState.IDLE
        return

    if not trader._passes_top_book_depth_gate(
        symbol=snapshot.symbol,
        exchange_id=snapshot.exchange_long,
        side="buy",
        base_qty=base_qty,
        top_qty=long_ask_qty,
        max_top_book_usage_pct=max_top_book_usage_pct,
    ) or not trader._passes_top_book_depth_gate(
        symbol=snapshot.symbol,
        exchange_id=snapshot.exchange_short,
        side="sell",
        base_qty=base_qty,
        top_qty=short_bid_qty,
        max_top_book_usage_pct=max_top_book_usage_pct,
    ):
        trader._state = TradeState.IDLE
        return

    logger.info(
        "TRADE OPENING {} target_tokens={:.2f} long={}(ask={:.6f} age={}ms) short={}(bid={:.6f} age={}ms) "
        "spread={:.4f}% amount_usdt=${:.2f}",
        snapshot.symbol,
        base_qty,
        snapshot.exchange_long,
        quoted_long,
        f"{quote_age_long:.0f}",
        snapshot.exchange_short,
        quoted_short,
        f"{quote_age_short:.0f}",
        snapshot.price_spread_pct,
        amount_usdt,
    )

    try:
        start_mono = time.monotonic()
        results = await asyncio.gather(
            asyncio.create_task(trader._open_leg_with_timing(
                long_conn, snapshot.symbol, "buy", base_qty, quoted_long, order_type,
                start_mono, quote_age_long, max_quote_to_order_age_ms,
                time_in_force, limit_price_slippage_pct,
            )),
            asyncio.create_task(trader._open_leg_with_timing(
                short_conn, snapshot.symbol, "sell", base_qty, quoted_short, order_type,
                start_mono, quote_age_short, max_quote_to_order_age_ms,
                time_in_force, limit_price_slippage_pct,
            )),
            return_exceptions=True,
        )
        total_latency_ms = (time.monotonic() - start_mono) * 1000
        long_result, long_dur, long_submit_ms = trader._unwrap_timed_order_result(
            results[0], snapshot.exchange_long, snapshot.symbol, "buy",
        )
        short_result, short_dur, short_submit_ms = trader._unwrap_timed_order_result(
            results[1], snapshot.exchange_short, snapshot.symbol, "sell",
        )

        outcome = await FillReconciler.reconcile(
            long_conn=long_conn,
            short_conn=short_conn,
            long_result=long_result,
            short_result=short_result,
            symbol=snapshot.symbol,
            quoted_long=quoted_long,
            quoted_short=quoted_short,
        )
        if outcome is ReconcileOutcome.ABORTED:
            await _handle_open_abort(
                trader, snapshot, attempt_key, long_result, short_result,
            )
            return

        open_long = _make_leg(
            snapshot.exchange_long, "buy", quoted_long,
            long_result, long_dur, tick_age_long, quote_age_long, long_submit_ms,
        )
        open_short = _make_leg(
            snapshot.exchange_short, "sell", quoted_short,
            short_result, short_dur, tick_age_short, quote_age_short, short_submit_ms,
        )
        trader._position = Position(
            symbol=snapshot.symbol,
            exchange_long=snapshot.exchange_long,
            exchange_short=snapshot.exchange_short,
            entry_spread_pct=snapshot.price_spread_pct,
            amount_usdt=amount_usdt,
            opened_at=time.time(),
            open_long=open_long,
            open_short=open_short,
            open_latency_ms=total_latency_ms,
        )
        trader._state = TradeState.OPEN
        _log_trade_summary(
            "OPENED", snapshot.symbol,
            open_long, open_short,
            total_latency_ms, snapshot.price_spread_pct,
        )

        if trader._notifier:
            trader._notifier.notify_trade_opened(
                snapshot.symbol, snapshot.exchange_long, snapshot.exchange_short,
                snapshot.price_spread_pct, amount_usdt, total_latency_ms,
            )
        trader._clear_open_failures(attempt_key)
        await _save_open_deal(trader, snapshot, amount_usdt, open_long, open_short, total_latency_ms)
    except Exception as exc:
        logger.critical("CRITICAL UNHANDLED TRADE OPEN ERROR: {}", exc)
        if trader._notifier:
            trader._notifier.notify_trade_critical_error(snapshot.symbol, "OPEN", str(exc))
        trader._record_open_failure(attempt_key)
        trader._state = TradeState.IDLE


async def _handle_open_abort(
    trader: TraderWorkflowPort, snapshot, attempt_key, long_result, short_result,
) -> None:
    blocked_legs = [r for r in (long_result, short_result) if not r.retryable]
    nonretryable_reason = (
        blocked_legs[0].failure_reason
        or blocked_legs[0].failure_code
        or "non-retryable order failure"
    ) if blocked_legs else ""
    if trader._notifier:
        trader._notifier.notify_trade_filtered(
            snapshot.symbol,
            f"open blocked: {nonretryable_reason}"
            if nonretryable_reason else "open aborted: legs unbalanced - rolled back",
        )
    if blocked_legs:
        for leg in blocked_legs:
            reason = leg.failure_reason or leg.failure_code or "non-retryable order failure"
            trader._block_open_exchange(snapshot.symbol, leg.exchange_id, reason)
    else:
        trader._record_open_failure(attempt_key)
    trader._state = TradeState.IDLE


async def _save_open_deal(
    trader: TraderWorkflowPort,
    snapshot,
    amount_usdt: float,
    open_long,
    open_short,
    total_latency_ms: float,
) -> None:
    if not trader._storage:
        return
    try:
        trader._current_deal_id = await trader._storage.save_deal({
            "symbol": snapshot.symbol,
            "exchange_long": snapshot.exchange_long,
            "exchange_short": snapshot.exchange_short,
            "opened_at": trader._position.opened_at,
            "entry_spread_pct": snapshot.price_spread_pct,
            "amount_usdt": amount_usdt,
            **open_long.to_deal_fields("open_long"),
            **open_short.to_deal_fields("open_short"),
            "total_latency_ms": total_latency_ms,
        })
        logger.info("Deal saved to DB: id={}", trader._current_deal_id)
    except Exception as exc:
        logger.error("Failed to save deal: {}", exc)
