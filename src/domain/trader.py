"""Arbitrage trading engine — state machine for opening/closing positions."""

from __future__ import annotations

import time
from typing import Optional

from loguru import logger

from src.settings import TradingSettings
from src.domain.models import (
    ConnectionProvider,
    DealStorePort,
    NotifierPort,
    OrderResult,
    SpreadSnapshot,
)
from src.domain.open_failure_backoff import (
    OpenAttemptKey,
    OpenFailureBackoff,
    OpenFailureState,
)
from src.domain.order_result_normalizer import (
    normalize_close_result,
    normalize_order_result,
    normalize_timed_order_result,
)
from src.domain.trader_market_inputs import (
    extract_quote,
    extract_top_book_qty,
    passes_top_book_depth_gate,
)
from src.domain.trader_order_legs import (
    close_leg_with_retry,
    open_leg_with_timing,
)
from src.domain.trader_close import close_position
from src.domain.trader_open import open_position
from src.domain.trader_restore import restore_from_db
from src.domain.trade_position import Position
from src.domain.trader_settings import (
    apply_trading_settings_update,
    trade_exchange_update,
)
from src.domain.trader_snapshot import build_trader_snapshot
from src.domain.trader_spread_decision import on_spread as handle_spread_decision
from src.domain.trader_symbol_config import (
    exchange_filters_for_symbol,
    resolve_symbol_config,
)
from src.domain.trade_state import TradeState


class ArbitrageTrader:
    """Watches spread snapshots and executes arb trades.

    State machine: IDLE → OPENING → OPEN → CLOSING → IDLE (or EXHAUSTED).
    """

    def __init__(
        self,
        streams: ConnectionProvider,
        settings: TradingSettings,
        trade_exchanges: list[str] | None = None,
        storage: DealStorePort | None = None,
        notifier: NotifierPort | None = None,
        symbol_overrides: dict[str, dict] | None = None,
    ):
        self._streams = streams
        self._settings = settings
        self._notifier = notifier
        self._entry_threshold = settings.entry_spread_pct
        self._symbol_entry_thresholds = settings.symbol_entry_spread_pct or {}
        self._close_threshold = settings.close_spread_pct
        self._amount_usdt = settings.amount_usdt
        self._max_trades = settings.max_trades_per_session
        self._max_latency_ms = settings.max_latency_ms
        self._exchange_max_latency_ms = settings.exchange_max_latency_ms or {}
        self._enabled = settings.enabled
        self._fail_cooldown_sec = settings.fail_cooldown_sec
        self._post_trade_delay_sec = getattr(settings, "post_trade_delay_sec", 0.0)
        self._next_trade_allowed_ts = 0.0
        self._max_consecutive_failures = settings.max_consecutive_failures
        self._order_type = settings.order_type
        self._time_in_force = getattr(settings, "time_in_force", "IOC")
        self._limit_price_slippage_pct = getattr(settings, "limit_price_slippage_pct", 0.5)
        self._max_quote_to_order_age_ms = getattr(settings, "max_quote_to_order_age_ms", 750.0)
        self._max_top_book_usage_pct = getattr(settings, "max_top_book_usage_pct", 100.0)
        self._max_entry_spread_pct = getattr(settings, "max_entry_spread_pct", 30.0)
        self._trade_exchanges: set[str] = set(trade_exchanges or [])
        # Mutable reference to the AppConfig per-symbol overrides dict.
        # The controller mutates this dict in place; the trader reads it on
        # every spread tick. Keeping it as a shared reference (not a copy)
        # is the simplest hot-reload path and keeps the trader lock-free.
        self._symbol_overrides: dict[str, dict] = (
            symbol_overrides if symbol_overrides is not None else {}
        )
        self._storage = storage  # for saving deals

        self._state = TradeState.IDLE
        self._position: Optional[Position] = None
        self._trades_done: int = 0
        self._last_skip_reason: str = ""  # for UI: why trade was skipped
        # Dedup key for the *last* WARNING we emitted. A new WARNING fires
        # only when this key changes (prevents the lot-size / stale-tick
        # spam that produced 340 identical warnings in a single day).
        self._last_logged_skip_key: str = ""
        self._current_deal_id: int | None = None  # DB row id of current open deal
        self._open_failure_backoff = OpenFailureBackoff()
        self._last_open_attempt_key: OpenAttemptKey | None = None
        self._consecutive_open_failures: int = 0
        self._last_open_failure_ts: float = 0.0
        # Persistence tracker: for each (ex_long, ex_short) pair currently
        # sitting above entry threshold, records the first-crossing wall-time.
        # Cleared for a pair whenever its spread drops back below threshold.
        # Used with `min_spread_persistence_ms` to filter phantom single-tick
        # spikes caused by event-loop stalls / stale-tick races.
        self._pair_above_threshold_since: dict[tuple[str, str], float] = {}

    # ── skip reason management (SRP: single owner of skip state) ──
    def _record_skip(self, reason: str, *, key: str | None = None) -> None:
        """Set the skip reason shown in the UI and emit a WARNING exactly
        once per distinct `key` (defaults to `reason` itself).

        Rationale: `on_spread()` fires several times per second, and a
        single blocking condition (e.g. insufficient `amount_usdt` for the
        exchange lot size) would otherwise flood bot.log with hundreds of
        identical WARNING lines.
        """
        self._last_skip_reason = reason
        dedup_key = key if key is not None else reason
        if dedup_key != self._last_logged_skip_key:
            logger.warning("FILTER RULE: NOT TRADE — {}", reason)
            self._last_logged_skip_key = dedup_key

    @property
    def state(self) -> TradeState:
        return self._state

    @property
    def current_deal_id(self) -> int | None:
        return self._current_deal_id

    @property
    def position(self) -> Optional[Position]:
        return self._position

    def clear_position(self) -> None:
        """Reset trader state after manual close on exchange."""
        logger.info("TRADER position cleared (manual force-close), deal_id={}", self._current_deal_id)
        self._position = None
        self._current_deal_id = None
        self._state = TradeState.IDLE

    def reset_trade_session(self) -> dict:
        """Reset the in-memory session trade counter when no exposure is open."""
        if self.is_busy:
            return {"error": f"bot is {self._state.value}"}
        if self._position is not None:
            return {"error": "position open"}
        self._trades_done = 0
        self._next_trade_allowed_ts = 0.0
        if self._state == TradeState.EXHAUSTED:
            self._state = TradeState.IDLE
        self._last_skip_reason = ""
        logger.info("TRADER session counter reset; max_trades={}", self._max_trades)
        return {
            "trades_done": self._trades_done,
            "max_trades": self._max_trades,
            "state": self._state.value,
        }

    def reset_exchange_failures(self, exchange_id: str = "") -> None:
        """Clear open-failure backoff after an operator reconnects an exchange."""
        self._open_failure_backoff.reset(exchange_id)
        self._last_open_attempt_key = None
        self._consecutive_open_failures = 0
        self._last_open_failure_ts = 0.0
        self._last_skip_reason = ""
        logger.info("TRADER exchange failure cooldown reset for {}", exchange_id or "all")

    def _sync_open_failure_state(self, key: OpenAttemptKey | None) -> None:
        state = self._open_failure_backoff.state_for(key)
        self._last_open_attempt_key = key
        self._consecutive_open_failures = state.count
        self._last_open_failure_ts = state.last_failure_ts

    def _record_open_failure(self, key: OpenAttemptKey) -> None:
        state = self._open_failure_backoff.record_failure(key, now=time.time())
        self._last_open_attempt_key = key
        self._consecutive_open_failures = state.count
        self._last_open_failure_ts = state.last_failure_ts

    def _block_open_exchange(self, symbol: str, exchange_id: str, reason: str) -> None:
        """Block ``exchange_id`` for ``symbol`` due to a non-retryable rejection.

        Other exchanges remain tradeable for the same token; only pairs whose
        long or short leg is the offending exchange are skipped.
        """
        self._open_failure_backoff.block_exchange(symbol, exchange_id, reason)
        self._consecutive_open_failures = 0
        self._last_open_failure_ts = 0.0
        self._record_skip(
            f"open blocked for {exchange_id} on {symbol}: {reason}",
            key=f"order-block:{symbol}:{exchange_id}",
        )

    def _clear_open_failures(self, key: OpenAttemptKey) -> None:
        self._open_failure_backoff.clear(key)
        self._sync_open_failure_state(key)

    @property
    def is_busy(self) -> bool:
        """True while a leg is being opened or closed (unsafe to mutate config)."""
        return self._state in (TradeState.OPENING, TradeState.CLOSING)

    def held_symbols(self) -> set[str]:
        """Symbols with an active position (used by config guard before symbol removal)."""
        if self._position:
            return {self._position.symbol}
        return set()

    # ── Per-symbol resolution ────────────────────────────────────────
    def _resolve(self, symbol: str) -> dict:
        return resolve_symbol_config(self, symbol)

    def exchange_filters_for_symbol(self, symbol: str) -> dict:
        """Return effective exchange filters for orchestration/UI surfaces."""
        return exchange_filters_for_symbol(self, symbol)

    def update_settings(
        self,
        *,
        trading: dict | None = None,
        trade_exchanges: list[str] | None = None,
    ) -> dict[str, tuple]:
        """Hot-reload trading parameters in place.

        Caller (controller) MUST have validated the payload via
        domain/runtime_config.validate_overrides() and confirmed not is_busy.

        Returns a dict of {field: (old, new)} for changed fields, used by the
        UI to render confirmation toasts.
        """
        changes: dict[str, tuple] = {}

        if trading:
            changes.update(apply_trading_settings_update(
                trader=self,
                settings=self._settings,
                trading=trading,
            ))
            if (
                self._state == TradeState.EXHAUSTED
                and self._trades_done < self._max_trades
            ):
                self._state = TradeState.IDLE
                logger.info(
                    "TRADER returned to IDLE after max_trades raised to {}", self._max_trades,
                )

        if trade_exchanges is not None:
            new_set, exchange_change = trade_exchange_update(
                self._trade_exchanges, trade_exchanges,
            )
            if exchange_change is not None:
                self._trade_exchanges = new_set
                changes["trade_exchanges"] = exchange_change

        if changes:
            logger.info("TRADER settings updated: {}", changes)
        return changes

    @property
    def trades_done(self) -> int:
        return self._trades_done

    async def restore_from_db(self) -> None:
        """Recover an open position from the DB after restart."""
        await restore_from_db(self)

    def to_dict(self) -> dict:
        return build_trader_snapshot(self)

    async def on_spread_update(self, snapshots: list[SpreadSnapshot]) -> None:
        for snap in snapshots:
            prev_state = self._state
            await self.on_spread(snap)
            # Break early if state changed (e.g. IDLE→OPENING) to avoid
            # feeding remaining snapshots into a non-IDLE state.
            if self._state != prev_state:
                break

    async def on_spread(self, snapshot: SpreadSnapshot) -> None:
        """Called by Orchestrator on every spread recalc."""
        await handle_spread_decision(self, snapshot)

    def _check_tick_freshness(self, snapshot: SpreadSnapshot) -> bool:
        """Return True if ticks are too stale to trade on.

        Only checks the two exchanges involved in this trade pair,
        not all exchanges in the snapshot.
        """
        now = time.time()
        for ex_id in (snapshot.exchange_long, snapshot.exchange_short):
            prices = snapshot.prices.get(ex_id, {})
            server_age = prices.get("server_age_ms")
            data_age = prices.get("data_age_ms")
            if server_age is not None and data_age is not None:
                age_ms = max(float(server_age or 0.0), float(data_age or 0.0))
            elif server_age is not None:
                age_ms = float(server_age or 0.0)
            elif data_age is not None:
                age_ms = float(data_age or 0.0)
            elif prices.get("receive_time"):
                age_ms = max(0.0, (now - float(prices["receive_time"])) * 1000.0)
            else:
                tick_ts = prices.get("timestamp", 0)
                if tick_ts <= 0:
                    continue
                age_ms = max(0.0, (now - float(tick_ts)) * 1000.0)

            if age_ms <= 0:
                continue
            allowed_max = self._exchange_max_latency_ms.get(ex_id, self._max_latency_ms)
            if age_ms > allowed_max:
                self._record_skip(
                    f"stale tick: {ex_id} age={age_ms:.0f}ms > {allowed_max:.0f}ms",
                    key=f"stale:{ex_id}",
                )
                return True
        return False

    def _check_spread_persistence(self, snapshot: SpreadSnapshot) -> bool:
        """Return True if trade should be skipped because the spread has
        not been above threshold long enough.

        Disabled when `min_spread_persistence_ms == 0` (default).

        Rationale: event-loop stalls (GC pauses, sync I/O) cause all cached
        WS ticks to simultaneously become stale. When the loop resumes, a
        fresh tick from one exchange computed against stale ticks from the
        others can produce a phantom 3%+ spread that vanishes on the very
        next cycle. Requiring the spread to stay above threshold for 500-
        1500 ms filters these out without missing real sustained spreads.
        """
        min_persist = getattr(self._settings, "min_spread_persistence_ms", 0.0)
        if min_persist <= 0:
            return False  # feature disabled

        pair = (snapshot.exchange_long, snapshot.exchange_short)
        now = time.time()
        first_seen = self._pair_above_threshold_since.get(pair)
        if first_seen is None:
            self._pair_above_threshold_since[pair] = now
            elapsed_ms = 0.0
        else:
            elapsed_ms = (now - first_seen) * 1000.0

        if elapsed_ms < min_persist:
            self._record_skip(
                f"persistence: {pair[0]}/{pair[1]} "
                f"{elapsed_ms:.0f}/{min_persist:.0f}ms",
                key=f"persist:{pair[0]}:{pair[1]}",
            )
            return True
        return False

    @staticmethod
    def _extract_quote(
        prices: dict, ex_id: str, *, use_ask: bool,
    ) -> tuple[float, float, float]:
        return extract_quote(prices, ex_id, use_ask=use_ask)

    @staticmethod
    def _extract_top_book_qty(prices: dict, ex_id: str, *, use_ask: bool) -> float | None:
        return extract_top_book_qty(prices, ex_id, use_ask=use_ask)

    def _passes_top_book_depth_gate(
        self,
        *,
        symbol: str,
        exchange_id: str,
        side: str,
        base_qty: float,
        top_qty: float | None,
        max_top_book_usage_pct: float,
    ) -> bool:
        return passes_top_book_depth_gate(
            symbol=symbol,
            exchange_id=exchange_id,
            side=side,
            base_qty=base_qty,
            top_qty=top_qty,
            max_top_book_usage_pct=max_top_book_usage_pct,
            record_skip=lambda reason: self._record_skip(
                reason,
                key=f"top_book_depth:{symbol}:{exchange_id}:{side}",
            ),
        )

    async def _open_leg_with_timing(
        self, conn, symbol: str, side: str, base_qty: float,
        quoted_price: float, order_type: str, decision_mono: float,
        quote_age_ms: float = 0.0,
        max_quote_to_order_age_ms: float = 0.0,
        time_in_force: str | None = None,
        limit_price_slippage_pct: float | None = None,
    ) -> tuple[OrderResult, float, float]:
        return await open_leg_with_timing(
            conn=conn,
            symbol=symbol,
            side=side,
            base_qty=base_qty,
            quoted_price=quoted_price,
            order_type=order_type,
            decision_mono=decision_mono,
            quote_age_ms=quote_age_ms,
            max_quote_to_order_age_ms=max_quote_to_order_age_ms,
            time_in_force=time_in_force,
            limit_price_slippage_pct=limit_price_slippage_pct,
            record_skip=lambda reason: self._record_skip(
                reason, key="quote_to_order_age",
            ),
        )

    async def _open_position(self, snapshot: SpreadSnapshot) -> None:
        await open_position(self, snapshot)

    @staticmethod
    def _unwrap_order_result(
        result_or_exc, exchange_id: str, symbol: str, side: str,
    ) -> OrderResult:
        """Normalize asyncio.gather(return_exceptions=True) entries to OrderResult."""
        return normalize_order_result(result_or_exc, exchange_id, symbol, side)

    @staticmethod
    def _unwrap_timed_order_result(
        result_or_exc, exchange_id: str, symbol: str, side: str,
    ) -> tuple[OrderResult, float, float]:
        return normalize_timed_order_result(result_or_exc, exchange_id, symbol, side)

    @staticmethod
    def _unwrap_close_result(
        result_or_exc, exchange_id: str, symbol: str, side: str,
    ) -> tuple[OrderResult, float]:
        """Normalize close-leg results to ``(OrderResult, duration_ms)``.

        Close legs return ``(result, elapsed_ms)`` tuples from
        ``_close_leg_with_retry`` — this mirrors ``_unwrap_order_result``
        for that shape so the close path has no bespoke error handling.
        """
        return normalize_close_result(result_or_exc, exchange_id, symbol, side)

    async def _close_leg_with_retry(
        self, conn, symbol: str, side: str, qty: float, quoted_price: float,
    ) -> tuple[OrderResult, float]:
        """Close one leg with the configured order type.

        Per the simple-trading principle: one order, one attempt. If the
        exchange rejects or under-fills, we log it and move on — retry
        storms caused the Apr-24 unhedged-exposure incident.

        Returns (result, elapsed_ms).
        """
        # Resolve per-symbol order_type so symbol-specific overrides apply
        # to the close leg too (consistent with open). Falls back to global.
        resolved = self._resolve(symbol)
        order_type = resolved["order_type"]
        return await close_leg_with_retry(
            conn=conn,
            symbol=symbol,
            side=side,
            qty=qty,
            quoted_price=quoted_price,
            order_type=order_type,
            time_in_force=resolved["time_in_force"],
            limit_price_slippage_pct=resolved["limit_price_slippage_pct"],
        )

    async def _close_position(self, snapshot: SpreadSnapshot) -> None:
        await close_position(self, snapshot)



