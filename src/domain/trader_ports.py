from __future__ import annotations

from typing import Any, Protocol

from src.domain.models import (
    ConnectionProvider,
    DealStorePort,
    NotifierPort,
    OrderExecutor,
    OrderResult,
    SpreadSnapshot,
)
from src.domain.open_failure_backoff import OpenAttemptKey, OpenFailureBackoff
from src.domain.trade_position import Position
from src.domain.trade_state import TradeState


class TraderConfigPort(Protocol):
    _symbol_overrides: dict[str, dict]
    _symbol_entry_thresholds: dict[str, float]
    _entry_threshold: float
    _close_threshold: float
    _amount_usdt: float
    _max_entry_spread_pct: float
    _order_type: str
    _time_in_force: str
    _limit_price_slippage_pct: float
    _max_quote_to_order_age_ms: float
    _max_top_book_usage_pct: float
    _max_consecutive_failures: int
    _fail_cooldown_sec: float
    _post_trade_delay_sec: float
    _trade_exchanges: set[str]


class TraderSnapshotPort(TraderConfigPort, Protocol):
    _enabled: bool
    _state: TradeState
    _position: Position | None
    _trades_done: int
    _max_trades: int
    _max_latency_ms: float
    _next_trade_allowed_ts: float
    _consecutive_open_failures: int
    _open_failure_backoff: OpenFailureBackoff
    _last_open_attempt_key: OpenAttemptKey | None
    _last_skip_reason: str


class TraderRestorePort(Protocol):
    _storage: DealStorePort | None
    _position: Position | None
    _current_deal_id: int | None
    _state: TradeState


class TraderWorkflowPort(TraderSnapshotPort, TraderRestorePort, Protocol):
    _streams: ConnectionProvider
    _notifier: NotifierPort | None
    _last_logged_skip_key: str
    _last_open_failure_ts: float
    _pair_above_threshold_since: dict[tuple[str, str], float]

    def _resolve(self, symbol: str) -> dict[str, Any]: ...

    def _record_skip(self, reason: str, *, key: str | None = None) -> None: ...

    def _sync_open_failure_state(self, key: OpenAttemptKey | None) -> None: ...

    def _record_open_failure(self, key: OpenAttemptKey) -> None: ...

    def _block_open_exchange(self, symbol: str, exchange_id: str, reason: str) -> None: ...

    def _clear_open_failures(self, key: OpenAttemptKey) -> None: ...

    def _check_tick_freshness(self, snapshot: SpreadSnapshot) -> bool: ...

    def _check_spread_persistence(self, snapshot: SpreadSnapshot) -> bool: ...

    def _extract_quote(
        self,
        prices: dict,
        ex_id: str,
        *,
        use_ask: bool,
    ) -> tuple[float, float, float]: ...

    def _extract_top_book_qty(
        self,
        prices: dict,
        ex_id: str,
        *,
        use_ask: bool,
    ) -> float | None: ...

    def _passes_top_book_depth_gate(
        self,
        *,
        symbol: str,
        exchange_id: str,
        side: str,
        base_qty: float,
        top_qty: float | None,
        max_top_book_usage_pct: float,
    ) -> bool: ...

    async def _open_leg_with_timing(
        self,
        conn: OrderExecutor,
        symbol: str,
        side: str,
        base_qty: float,
        quoted_price: float,
        order_type: str,
        decision_mono: float,
        quote_age_ms: float = 0.0,
        max_quote_to_order_age_ms: float = 0.0,
        time_in_force: str | None = None,
        limit_price_slippage_pct: float | None = None,
    ) -> tuple[OrderResult, float, float]: ...

    async def _close_leg_with_retry(
        self,
        conn: OrderExecutor,
        symbol: str,
        side: str,
        qty: float,
        quoted_price: float,
    ) -> tuple[OrderResult, float]: ...

    def _unwrap_timed_order_result(
        self,
        result_or_exc: Any,
        exchange_id: str,
        symbol: str,
        side: str,
    ) -> tuple[OrderResult, float, float]: ...

    def _unwrap_close_result(
        self,
        result_or_exc: Any,
        exchange_id: str,
        symbol: str,
        side: str,
    ) -> tuple[OrderResult, float]: ...

    async def _open_position(self, snapshot: SpreadSnapshot) -> None: ...

    async def _close_position(self, snapshot: SpreadSnapshot) -> None: ...