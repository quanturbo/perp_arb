"""Reconciliation of mismatched fills after a two-leg arb open.

Responsibility (SRP): given the two `OrderResult`s returned by concurrent
long/short orders, bring the filled amounts back to a matched state by
rolling back the excess side, and signal whether the trade can proceed.

The old inline implementation in `ArbitrageTrader._open_position` mixed
rollback logic with persistence and state transitions. It also silently
entered `OPEN` state when *both* legs 0-filled (connection crash on both
sides), creating a "ghost deal" in the DB. That bug is fixed here: the
both-zero case now returns `ABORTED` explicitly, and the caller must NOT
create a Position.
"""

from __future__ import annotations

import enum
from typing import Protocol

from loguru import logger

from src.domain.models import OrderResult


class _OrderPlacer(Protocol):
    """Minimal port: anything that can place a rollback close order.

    Declared structurally to keep this module decoupled from the concrete
    `ExchangeConnection` implementation (DIP).
    """

    exchange_id: str

    async def place_market_order(
        self,
        *,
        symbol: str,
        side: str,
        base_qty: float,
        price: float = 0.0,
        order_type: str = "market",
        is_close: bool = False,
        time_in_force: str | None = None,
        limit_price_slippage_pct: float | None = None,
    ) -> OrderResult: ...


class ReconcileOutcome(enum.Enum):
    OK = "ok"  # legs are balanced (possibly after a rollback) and trade can proceed
    ABORTED = "aborted"  # one or both legs failed; caller must NOT create a Position


class FillReconciler:
    """Bring two mismatched fill amounts back to parity.

    Policy (matches the pre-refactor behaviour for the happy cases,
    plus an explicit fix for the both-zero case):

      * both zero                                     -> ABORT  (no rollback; no ghost deal)
      * long_filled > short_filled > 0                -> sell excess on long leg  -> OK
      * short_filled > long_filled > 0                -> buy  excess on short leg -> OK
      * exactly one side is zero (the other > 0)      -> close the non-zero leg   -> ABORT
      * both equal (including both non-zero)          -> no-op                    -> OK

    Mutates the input `OrderResult.filled_amount` in place after a
    successful rollback so the caller sees the reconciled amounts.
    """

    @staticmethod
    async def reconcile(
        *,
        long_conn: _OrderPlacer,
        short_conn: _OrderPlacer,
        long_result: OrderResult,
        short_result: OrderResult,
        symbol: str,
        quoted_long: float,
        quoted_short: float,
    ) -> ReconcileOutcome:
        long_filled = long_result.filled_amount
        short_filled = short_result.filled_amount

        # Fast paths
        if long_filled == short_filled and long_filled > 0:
            return ReconcileOutcome.OK

        if long_filled == 0 and short_filled == 0:
            # BUG FIX: both legs failed to fill — abort without creating a position.
            logger.warning(
                "HANDLED OPEN ABORTED: both legs got 0 fill on {} ({}, {}) — no position created",
                symbol, long_conn.exchange_id, short_conn.exchange_id,
            )
            return ReconcileOutcome.ABORTED

        # One side totally zero, the other got something -> close the non-zero leg.
        if long_filled == 0 or short_filled == 0:
            if long_filled > 0:
                rolled_back = await FillReconciler._safe_close(
                    conn=long_conn, symbol=symbol, side="sell",
                    qty=long_filled, price=quoted_long,
                )
                unhedged_conn, unhedged_side, unhedged_qty = long_conn, "long", long_filled
            else:
                rolled_back = await FillReconciler._safe_close(
                    conn=short_conn, symbol=symbol, side="buy",
                    qty=short_filled, price=quoted_short,
                )
                unhedged_conn, unhedged_side, unhedged_qty = short_conn, "short", short_filled

            if not rolled_back:
                # Rollback on the filled side failed → operator has OPEN unhedged
                # exposure. Escalate loudly — this is NOT a handled operational
                # event, it requires manual intervention.
                logger.critical(
                    "CRITICAL UNHANDLED UNHEDGED EXPOSURE on {}: {} {} leg holds {:.6f} "
                    "after rollback rejected — MANUAL CLOSE REQUIRED",
                    symbol, unhedged_conn.exchange_id, unhedged_side, unhedged_qty,
                )
                return ReconcileOutcome.ABORTED
            logger.warning(
                "HANDLED OPEN ABORTED: one side got 0 fill on {} (long={:.6f} short={:.6f}) — rolled back the other side",
                symbol, long_filled, short_filled,
            )
            return ReconcileOutcome.ABORTED

        # Both non-zero but unequal — rollback the excess on the larger side.
        logger.warning(
            "OPEN FILL MISMATCH on {}: long={:.6f} short={:.6f} — rolling back excess",
            symbol, long_filled, short_filled,
        )
        if long_filled > short_filled:
            excess = long_filled - short_filled
            if await FillReconciler._safe_close(
                conn=long_conn, symbol=symbol, side="sell",
                qty=excess, price=quoted_long,
            ):
                long_result.filled_amount = short_filled
                logger.info("Rolled back {:.6f} excess on long side", excess)
            else:
                # Rollback failed — unhedged exposure. Close BOTH sides to zero.
                logger.error(
                    "HANDLED ROLLBACK FAILURE on {} — closing both sides to avoid unhedged exposure",
                    symbol,
                )
                await FillReconciler._safe_close(
                    conn=long_conn, symbol=symbol, side="sell",
                    qty=long_filled, price=quoted_long,
                )
                await FillReconciler._safe_close(
                    conn=short_conn, symbol=symbol, side="buy",
                    qty=short_filled, price=quoted_short,
                )
                return ReconcileOutcome.ABORTED
        else:
            excess = short_filled - long_filled
            if await FillReconciler._safe_close(
                conn=short_conn, symbol=symbol, side="buy",
                qty=excess, price=quoted_short,
            ):
                short_result.filled_amount = long_filled
                logger.info("Rolled back {:.6f} excess on short side", excess)
            else:
                logger.error(
                    "HANDLED ROLLBACK FAILURE on {} — closing both sides to avoid unhedged exposure",
                    symbol,
                )
                await FillReconciler._safe_close(
                    conn=long_conn, symbol=symbol, side="sell",
                    qty=long_filled, price=quoted_long,
                )
                await FillReconciler._safe_close(
                    conn=short_conn, symbol=symbol, side="buy",
                    qty=short_filled, price=quoted_short,
                )
                return ReconcileOutcome.ABORTED
        return ReconcileOutcome.OK

    @staticmethod
    async def _safe_close(
        *,
        conn: _OrderPlacer,
        symbol: str,
        side: str,
        qty: float,
        price: float,
    ) -> bool:
        """Place a rollback/close order, swallowing exceptions. Returns success."""
        try:
            await conn.place_market_order(
                symbol=symbol, side=side, base_qty=qty, price=price,
                order_type="market", is_close=True,
            )
            return True
        except Exception as err:
            logger.error(
                "HANDLED ROLLBACK ERROR: failed to rollback {} excess on {}: {}",
                side, conn.exchange_id, err,
            )
            return False
