from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from loguru import logger

from src.domain.models import OrderResult


@dataclass
class LegInfo:
    """Price and execution information for one trade leg."""

    exchange_id: str
    side: str
    quoted_price: float
    fill_price: float = 0.0
    filled_amount: float = 0.0
    slippage_pct: float = 0.0
    latency_ms: float = 0.0
    ack_latency_ms: float = 0.0
    tick_age_ms: float = 0.0
    quote_age_ms: float = 0.0
    decision_to_order_ms: float = 0.0
    order_id: str = ""

    def to_deal_fields(self, prefix: str) -> dict:
        return {
            f"{prefix}_quoted": self.quoted_price,
            f"{prefix}_fill": self.fill_price,
            f"{prefix}_filled_qty": self.filled_amount,
            f"{prefix}_slip_pct": self.slippage_pct,
            f"{prefix}_latency_ms": self.latency_ms,
            f"{prefix}_ack_latency_ms": self.ack_latency_ms,
            f"{prefix}_tick_age_ms": self.tick_age_ms,
            f"{prefix}_quote_age_ms": self.quote_age_ms,
            f"{prefix}_decision_to_order_ms": self.decision_to_order_ms,
        }


@dataclass
class Position:
    symbol: str
    exchange_long: str
    exchange_short: str
    entry_spread_pct: float
    amount_usdt: float
    opened_at: float = 0.0

    open_long: Optional[LegInfo] = None
    open_short: Optional[LegInfo] = None
    open_latency_ms: float = 0.0

    close_long: Optional[LegInfo] = None
    close_short: Optional[LegInfo] = None
    close_latency_ms: float = 0.0
    close_spread_pct: float = 0.0
    closed_at: float = 0.0

    @classmethod
    def from_deal_row(cls, deal: dict) -> "Position":
        long_filled = deal.get("open_long_filled_qty", 0) or 0
        short_filled = deal.get("open_short_filled_qty", 0) or 0
        if not long_filled and deal["open_long_fill"]:
            long_filled = deal["amount_usdt"] / deal["open_long_fill"]
        if not short_filled and deal["open_short_fill"]:
            short_filled = deal["amount_usdt"] / deal["open_short_fill"]
        return cls(
            symbol=deal["symbol"],
            exchange_long=deal["exchange_long"],
            exchange_short=deal["exchange_short"],
            entry_spread_pct=deal["entry_spread_pct"],
            amount_usdt=deal["amount_usdt"],
            opened_at=deal["opened_at"],
            open_long=LegInfo(
                exchange_id=deal["exchange_long"],
                side="buy",
                quoted_price=deal["open_long_quoted"],
                fill_price=deal["open_long_fill"],
                filled_amount=long_filled,
                slippage_pct=deal["open_long_slip_pct"],
                latency_ms=deal["open_long_latency_ms"],
                ack_latency_ms=deal.get("open_long_ack_latency_ms", 0) or 0,
                tick_age_ms=deal.get("open_long_tick_age_ms", 0) or 0,
                quote_age_ms=deal.get("open_long_quote_age_ms", 0) or 0,
                decision_to_order_ms=deal.get("open_long_decision_to_order_ms", 0) or 0,
                order_id="",
            ),
            open_short=LegInfo(
                exchange_id=deal["exchange_short"],
                side="sell",
                quoted_price=deal["open_short_quoted"],
                fill_price=deal["open_short_fill"],
                filled_amount=short_filled,
                slippage_pct=deal["open_short_slip_pct"],
                latency_ms=deal["open_short_latency_ms"],
                ack_latency_ms=deal.get("open_short_ack_latency_ms", 0) or 0,
                tick_age_ms=deal.get("open_short_tick_age_ms", 0) or 0,
                quote_age_ms=deal.get("open_short_quote_age_ms", 0) or 0,
                decision_to_order_ms=deal.get("open_short_decision_to_order_ms", 0) or 0,
                order_id="",
            ),
            open_latency_ms=deal["total_latency_ms"],
        )


def _calc_slippage(quoted: float, fill: float) -> float:
    if quoted == 0:
        return 0.0
    return (fill - quoted) / quoted * 100.0


def _make_leg(
    exchange_id: str,
    side: str,
    quoted_price: float,
    result: OrderResult,
    latency_ms: float,
    tick_age_ms: float = 0.0,
    quote_age_ms: float = 0.0,
    decision_to_order_ms: float = 0.0,
) -> LegInfo:
    return LegInfo(
        exchange_id=exchange_id,
        side=side,
        quoted_price=quoted_price,
        fill_price=result.fill_price,
        filled_amount=result.filled_amount,
        slippage_pct=_calc_slippage(quoted_price, result.fill_price),
        latency_ms=latency_ms,
        ack_latency_ms=getattr(result, "ack_latency_ms", 0.0) or 0.0,
        tick_age_ms=tick_age_ms,
        quote_age_ms=quote_age_ms,
        decision_to_order_ms=decision_to_order_ms,
        order_id=result.order_id,
    )


def _log_trade_summary(
    action: str,
    symbol: str,
    leg_long: LegInfo,
    leg_short: LegInfo,
    total_ms: float,
    spread_pct: float,
    trade_num: str = "",
) -> None:
    header = f"{'='*60}"
    trade_label = f" (trade {trade_num})" if trade_num else ""
    lines = [
        header,
        f"TRADE {action} {symbol}{trade_label}",
        f"  LONG  {leg_long.exchange_id:<16} "
        f"ideal={leg_long.quoted_price:<12.6f} real={leg_long.fill_price:<12.6f} "
        f"diff={leg_long.slippage_pct:+.4f}%  price_age={leg_long.quote_age_ms:.0f}ms  "
        f"submit={leg_long.decision_to_order_ms:.0f}ms  ack={leg_long.ack_latency_ms:.0f}ms  order={leg_long.latency_ms:.0f}ms",
        f"  SHORT {leg_short.exchange_id:<16} "
        f"ideal={leg_short.quoted_price:<12.6f} real={leg_short.fill_price:<12.6f} "
        f"diff={leg_short.slippage_pct:+.4f}%  price_age={leg_short.quote_age_ms:.0f}ms  "
        f"submit={leg_short.decision_to_order_ms:.0f}ms  ack={leg_short.ack_latency_ms:.0f}ms  order={leg_short.latency_ms:.0f}ms",
        f"  Total: {total_ms:.0f}ms | Spread: {spread_pct:.4f}%",
        header,
    ]
    logger.info("\n".join(lines))
