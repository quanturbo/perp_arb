"""Pure spread calculation logic. No side effects, no external deps."""

from __future__ import annotations

import time
from typing import Optional

from src.domain.models import FundingInfo, PriceTick, SpreadSnapshot


class SpreadCalculator:
    """Stateless calculator for arbitrage spread analysis.

    All methods are static — no instance state. Grouped as a class
    for namespace clarity and consistent OOP style.
    """

    @staticmethod
    def calc_price_spread(tick_a: PriceTick, tick_b: PriceTick) -> tuple[float, float]:
        """Calculate price spread both directions.

        Returns (spread_long_a_short_b, spread_long_b_short_a) as percentages.
        Spread = (bid_seller - ask_buyer) / midpoint * 100
        Positive = profitable, negative = underwater.
        """
        mid = (tick_a.ask + tick_a.bid + tick_b.ask + tick_b.bid) / 4.0
        if mid == 0:
            return 0.0, 0.0

        spread_ab = (tick_b.bid - tick_a.ask) / mid * 100.0
        spread_ba = (tick_a.bid - tick_b.ask) / mid * 100.0
        return spread_ab, spread_ba

    @staticmethod
    def calc_funding_benefit(
        funding_long: Optional[FundingInfo],
        funding_short: Optional[FundingInfo],
        holding_hours: Optional[float] = None,
    ) -> float:
        """Net funding benefit as percentage.

        Long position: pays funding if rate > 0, receives if rate < 0.
        Short position: receives funding if rate > 0, pays if rate < 0.

        If holding_hours is None, returns hourly rate.
        If holding_hours is given, returns benefit over that period.
        """
        long_rate_per_h = funding_long.rate_per_hour() if funding_long else 0.0
        short_rate_per_h = funding_short.rate_per_hour() if funding_short else 0.0

        net_per_hour = (short_rate_per_h - long_rate_per_h) * 100.0
        if holding_hours is None:
            return net_per_hour
        return net_per_hour * holding_hours

    @staticmethod
    def calc_funding_spread_pct(
        funding_a: Optional[FundingInfo],
        funding_b: Optional[FundingInfo],
    ) -> float:
        """Raw funding rate difference: rate_a - rate_b, as percentage."""
        rate_a = funding_a.funding_rate if funding_a else 0.0
        rate_b = funding_b.funding_rate if funding_b else 0.0
        return (rate_a - rate_b) * 100.0

    @classmethod
    def determine_best_direction(
        cls,
        tick_a: PriceTick,
        tick_b: PriceTick,
        funding_a: Optional[FundingInfo],
        funding_b: Optional[FundingInfo],
        holding_hours: float = 8.0,
    ) -> SpreadSnapshot:
        """Pick best direction and build full SpreadSnapshot."""
        now = time.time()
        spread_ab, spread_ba = cls.calc_price_spread(tick_a, tick_b)

        benefit_long_a = cls.calc_funding_benefit(funding_a, funding_b, holding_hours)
        benefit_long_b = cls.calc_funding_benefit(funding_b, funding_a, holding_hours)

        real_ab = spread_ab + benefit_long_a
        real_ba = spread_ba + benefit_long_b

        if real_ab >= real_ba:
            long_tick, short_tick = tick_a, tick_b
            long_fund, short_fund = funding_a, funding_b
            price_spread = spread_ab
            real_spread = real_ab
        else:
            long_tick, short_tick = tick_b, tick_a
            long_fund, short_fund = funding_b, funding_a
            price_spread = spread_ba
            real_spread = real_ba

        return SpreadSnapshot(
            symbol=tick_a.symbol,
            timestamp=time.time(),
            exchange_long=long_tick.exchange_id,
            exchange_short=short_tick.exchange_id,
            price_spread_pct=price_spread,
            funding_long=long_fund.funding_rate if long_fund else 0.0,
            funding_short=short_fund.funding_rate if short_fund else 0.0,
            funding_spread_pct=cls.calc_funding_spread_pct(long_fund, short_fund),
            funding_benefit_pct=cls.calc_funding_benefit(long_fund, short_fund, holding_hours),
            real_spread_pct=real_spread,
            direction=f"\u25b2 LONG {long_tick.exchange_id} / SHORT {short_tick.exchange_id} \u25bc",
            prices={
                tick_a.exchange_id: {
                    "bid": tick_a.bid,
                    "ask": tick_a.ask,
                    "bid_qty": tick_a.bid_qty,
                    "ask_qty": tick_a.ask_qty,
                    "last": tick_a.last,
                    "timestamp": tick_a.timestamp,
                    "receive_time": tick_a.receive_time,
                    "tick_age_ms": round(tick_a.tick_age_ms, 1),
                    "data_age_ms": round(tick_a.data_age_ms(now), 1),
                    "server_age_ms": round(tick_a.server_age_ms(now), 1),
                },
                tick_b.exchange_id: {
                    "bid": tick_b.bid,
                    "ask": tick_b.ask,
                    "bid_qty": tick_b.bid_qty,
                    "ask_qty": tick_b.ask_qty,
                    "last": tick_b.last,
                    "timestamp": tick_b.timestamp,
                    "receive_time": tick_b.receive_time,
                    "tick_age_ms": round(tick_b.tick_age_ms, 1),
                    "data_age_ms": round(tick_b.data_age_ms(now), 1),
                    "server_age_ms": round(tick_b.server_age_ms(now), 1),
                },
            },
        )
