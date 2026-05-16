"""Runtime-tunable trading parameters.

Separated from AppConfig (infrastructure/structural) to follow SRP:
- AppConfig: WHAT to connect to (exchanges, symbols, DB, web server)
- TradingSettings: HOW to trade (thresholds, amounts, limits, safety)
"""

from __future__ import annotations

from dataclasses import dataclass, field


DEFAULT_ORDER_TYPE = "limit"
DEFAULT_TIME_IN_FORCE = "IOC"
DEFAULT_LIMIT_PRICE_SLIPPAGE_PCT = 0.0
MAX_LIMIT_PRICE_SLIPPAGE_PCT = 1.0


@dataclass
class TradingSettings:
    enabled: bool = True
    entry_spread_pct: float = 2.0
    close_spread_pct: float = 1.0
    symbol_entry_spread_pct: dict[str, float] = field(default_factory=dict)
    amount_usdt: float = 20.0
    leverage: int = 1
    margin_mode: str = "isolated"
    max_open_positions: int = 1
    max_trades_per_session: int = 20
    max_latency_ms: float = 100.0
    exchange_max_latency_ms: dict[str, float] = field(
        default_factory=lambda: {
            "binanceusdm": 120.0,
            "aster": 120.0,
            "gateio": 120.0,
            "bitget": 120.0,
            "bybit": 120.0,
            "okx": 120.0,
            "kucoinfutures": 120.0,
            "bingx": 120.0,
            "htx": 120.0,
        }
    )
    order_type: str = DEFAULT_ORDER_TYPE  # "market" or "limit"
    # Slippage tolerance for limit IOC/FOK (only used when order_type="limit").
    # Market orders bypass this and always fill at exchange book price.
    limit_price_slippage_pct: float = DEFAULT_LIMIT_PRICE_SLIPPAGE_PCT
    # Limit-order time in force. IOC can partially fill immediately; FOK
    # requires the full order to fill immediately; GTC can rest on the book.
    time_in_force: str = DEFAULT_TIME_IN_FORCE
    # Final pre-submit safety gate: quote_age_ms + decision_to_order_ms must
    # stay below this value. 0 disables this extra gate.
    max_quote_to_order_age_ms: float = 350.0
    # Conservative depth gate: when bid/ask size is available from the stream,
    # use at most this percent of visible best-level quantity. 0 disables.
    max_top_book_usage_pct: float = 100.0
    fail_cooldown_sec: float = 5.0
    # Delay after a completed close before opening the next deal. This is a
    # session safety brake, separate from fail_cooldown_sec (which is only
    # for failed opens). 0 = disabled.
    post_trade_delay_sec: float = 0.0
    # After this many consecutive open failures on the same symbol/exchange
    # pair, trader cools down for `fail_cooldown_sec`.
    max_consecutive_failures: int = 3

    # Sanity cap: refuse to trade when the apparent spread exceeds this.
    # A cross-exchange spread >30% on the same perp is almost certainly a
    # stale feed, symbol-mismatch, or delisting artefact — not real arbitrage.
    # Opening a leg on a phantom 20% spread left us with unhedged exposure.
    max_entry_spread_pct: float = 30.0

    # Require an opportunity pair (ex_long, ex_short) to stay above
    # entry_spread_pct for this many ms continuously before firing a trade.
    # 0 = disabled (instant trade on first crossing, legacy behaviour).
    # Default 1000ms: filters phantom single-tick spikes caused by event-loop
    # stalls (GC pauses) and stale-tick races between feeds. Real sustained
    # opportunities still trade; single-tick artifacts get filtered out.
    min_spread_persistence_ms: float = 1000.0
