from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, runtime_checkable


@dataclass
class PriceTick:
    exchange_id: str
    symbol: str
    bid: float
    ask: float
    last: float
    timestamp: float  # unix seconds (exchange timestamp)
    receive_time: float = 0.0  # unix seconds (local clock when received)
    clock_offset: float = 0.0  # local_time - exchange_time (seconds)
    clock_synced: bool = False
    bid_qty: float | None = None
    ask_qty: float | None = None

    @property
    def tick_age_ms(self) -> float:
        """Milliseconds between exchange timestamp and our receive time."""
        if self.receive_time <= 0 or self.timestamp <= 0:
            return 0.0
        receive_on_exchange_clock = (
            self.receive_time - self.clock_offset
            if self.clock_synced else self.receive_time
        )
        return max(0.0, (receive_on_exchange_clock - self.timestamp) * 1000)

    def data_age_ms(self, now: float | None = None) -> float:
        """Milliseconds since this tick was received by the bot."""
        if self.receive_time <= 0:
            return float("inf")
        reference = time.time() if now is None else now
        return max(0.0, (reference - self.receive_time) * 1000)

    def server_age_ms(self, now: float | None = None) -> float:
        """Milliseconds from exchange timestamp to the reference local time."""
        if self.timestamp <= 0:
            return self.data_age_ms(now)
        reference = time.time() if now is None else now
        reference_on_exchange_clock = (
            reference - self.clock_offset if self.clock_synced else reference
        )
        return max(0.0, (reference_on_exchange_clock - self.timestamp) * 1000)


@dataclass
class OrderResult:
    """Parsed result from a market order execution."""

    exchange_id: str
    symbol: str
    side: str  # "buy" or "sell"
    fill_price: float  # average fill price
    filled_amount: float  # amount filled in base currency
    order_id: str
    timestamp: float  # unix seconds when order was placed
    ack_latency_ms: float = 0.0  # create_order round trip, before fill polling
    final_latency_ms: float = 0.0  # full adapter duration including fill polling
    retryable: bool = True
    failure_code: str = ""
    failure_reason: str = ""

    @classmethod
    def empty(
        cls,
        exchange_id: str,
        symbol: str,
        side: str,
        *,
        retryable: bool = True,
        failure_code: str = "",
        failure_reason: str = "",
    ) -> "OrderResult":
        """Zero-fill placeholder used when a connection throws before returning.

        Keeps downstream reconciliation code uniform (no None-checks scattered)
        while clearly marking an unfilled order via filled_amount == 0.
        """
        return cls(
            exchange_id=exchange_id,
            symbol=symbol,
            side=side,
            fill_price=0.0,
            filled_amount=0.0,
            order_id="",
            timestamp=time.time(),
            retryable=retryable,
            failure_code=failure_code,
            failure_reason=failure_reason,
        )


@dataclass
class FundingInfo:
    exchange_id: str
    symbol: str
    funding_rate: float  # e.g. 0.0001 = 0.01%
    next_funding_time: Optional[float] = None  # unix seconds
    interval_hours: float = 8.0
    timestamp: float = 0.0

    def rate_per_hour(self) -> float:
        return self.funding_rate / self.interval_hours if self.interval_hours else 0.0


@dataclass
class VolumeInfo:
    """Rolling 24h quote-currency volume for one (exchange, symbol).

    Polled separately from price ticks because 24h volume changes slowly
    and is exposed by every CCXT exchange via `fetch_ticker(...).quoteVolume`.
    Used by the dashboard and trader to flag low-liquidity legs.
    """

    exchange_id: str
    symbol: str
    quote_volume_24h: float  # in USDT (or quote currency) over the last 24h
    timestamp: float = 0.0  # unix seconds when sampled


@dataclass
class SpreadSnapshot:
    symbol: str
    timestamp: float

    # Price spread (buy low sell high)
    exchange_long: str  # buy here (lower ask)
    exchange_short: str  # sell here (higher bid)
    price_spread_pct: float  # (bid_short - ask_long) / mid * 100

    # Funding spread
    funding_long: float  # funding rate on long exchange (cost if positive)
    funding_short: float  # funding rate on short exchange (income if positive)
    funding_spread_pct: float  # net funding per interval as pct

    # Combined
    real_spread_pct: float  # price spread + funding benefit over holding period

    # Direction label for UI
    direction: str  # e.g. "▲ LONG aster / SHORT binanceusdm ▼"

    # Fields with defaults must come after required fields
    funding_benefit_pct: float = (
        0.0  # net hourly benefit for chosen direction (positive=favorable)
    )

    # Raw prices for display
    prices: dict = field(default_factory=dict)
    # { "binanceusdm": {"bid": 1.23, "ask": 1.24}, "aster": {"bid": 1.25, "ask": 1.26} }


@dataclass
class ArbitrageState:
    """In-memory live state per symbol.

    All access is from the single asyncio event loop thread,
    so no locking is needed (no preemption between await points).
    """

    symbol: str
    ticks: dict[str, PriceTick] = field(default_factory=dict)
    funding: dict[str, FundingInfo] = field(default_factory=dict)
    volumes: dict[str, VolumeInfo] = field(default_factory=dict)
    latest_spread: Optional[SpreadSnapshot] = None
    trade_opportunities: list[SpreadSnapshot] = field(default_factory=list)
    updated_at: float = 0.0

    def has_enough_data(self) -> bool:
        return len(self.ticks) >= 2

    def to_dict(self) -> dict:
        result: dict = {
            "symbol": self.symbol,
            "updated_at": self.updated_at,
            "ticks": {},
            "funding": {},
            "volumes": {},
            "spread": None,
        }
        for eid, t in self.ticks.items():
            now = time.time()
            result["ticks"][eid] = {
                "bid": t.bid,
                "ask": t.ask,
                "last": t.last,
                "timestamp": t.timestamp,
                "receive_time": t.receive_time,
                "tick_age_ms": round(t.tick_age_ms, 1),
                "data_age_ms": round(t.data_age_ms(now), 1),
                "server_age_ms": round(t.server_age_ms(now), 1),
            }
        for eid, f in self.funding.items():
            result["funding"][eid] = {
                "funding_rate": f.funding_rate,
                "next_funding_time": f.next_funding_time,
                "interval_hours": f.interval_hours,
                "timestamp": f.timestamp,
            }
        for eid, v in self.volumes.items():
            result["volumes"][eid] = {
                "quote_volume_24h": v.quote_volume_24h,
                "timestamp": v.timestamp,
            }
        if self.latest_spread:
            s = self.latest_spread
            result["spread"] = {
                "exchange_long": s.exchange_long,
                "exchange_short": s.exchange_short,
                "price_spread_pct": round(s.price_spread_pct, 6),
                "funding_long": round(s.funding_long, 8),
                "funding_short": round(s.funding_short, 8),
                "funding_spread_pct": round(s.funding_spread_pct, 6),
                "funding_benefit_pct": round(s.funding_benefit_pct, 6),
                "real_spread_pct": round(s.real_spread_pct, 6),
                "direction": s.direction,
                "prices": s.prices,
                "timestamp": s.timestamp,
            }
        if self.trade_opportunities:
            result["trade_opportunities"] = [
                {
                    "exchange_long": s.exchange_long,
                    "exchange_short": s.exchange_short,
                    "price_spread_pct": round(s.price_spread_pct, 4),
                    "funding_benefit_pct": round(s.funding_benefit_pct, 4),
                    "real_spread_pct": round(s.real_spread_pct, 4),
                }
                for s in self.trade_opportunities
            ]
        return result


# ── Ports (protocols for dependency inversion) ──────────────────


@runtime_checkable
class OrderExecutor(Protocol):
    """Port: anything that can place a market order on one exchange."""

    @property
    def exchange_id(self) -> str: ...

    def get_contract_size(self, symbol: str) -> float: ...

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


@runtime_checkable
class ConnectionProvider(Protocol):
    """Port: lookup an order executor by exchange id."""

    def get_connection(self, exchange_id: str) -> Optional["OrderExecutor"]: ...


@runtime_checkable
class DealStorePort(Protocol):
    """Port: persistence operations used by the trading state machine."""

    async def save_deal(self, deal: dict[str, Any]) -> int | None: ...

    async def close_deal(self, deal_id: int, close_data: dict[str, Any]) -> None: ...

    async def get_open_deals(self) -> list[dict[str, Any]]: ...

    async def cancel_deal(self, deal_id: int, reason: str = "") -> None: ...


@runtime_checkable
class NotifierPort(Protocol):
    """Port: outbound operator notifications emitted by the trader."""

    def notify_trade_filtered(self, symbol: str, details: str) -> None: ...

    def notify_trade_opened(
        self,
        symbol: str,
        exchange_long: str,
        exchange_short: str,
        spread_pct: float,
        amount_usdt: float,
        latency_ms: float,
    ) -> None: ...

    def notify_trade_closed(
        self,
        symbol: str,
        exchange_long: str,
        exchange_short: str,
        entry_spread: float,
        close_spread: float,
        latency_ms: float,
        trade_num: str = "",
    ) -> None: ...

    def notify_trade_critical_error(
        self, symbol: str, action: str, error: str,
    ) -> None: ...
