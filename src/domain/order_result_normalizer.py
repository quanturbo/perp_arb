from __future__ import annotations

from src.domain.models import OrderResult
from src.domain.order_errors import OrderErrorClassifier, _log_trade_issue


def normalize_order_result(
    result_or_exc,
    exchange_id: str,
    symbol: str,
    side: str,
) -> OrderResult:
    if isinstance(result_or_exc, Exception):
        _log_order_exception("OPEN", result_or_exc, exchange_id, symbol, side)
        return OrderResult.empty(exchange_id, symbol, side)
    if result_or_exc is None:
        return OrderResult.empty(exchange_id, symbol, side)
    return result_or_exc


def normalize_timed_order_result(
    result_or_exc,
    exchange_id: str,
    symbol: str,
    side: str,
) -> tuple[OrderResult, float, float]:
    if isinstance(result_or_exc, Exception):
        decision = OrderErrorClassifier.classify(
            exchange_id=exchange_id,
            symbol=symbol,
            side=side,
            error=result_or_exc,
        )
        _log_order_exception("OPEN", result_or_exc, exchange_id, symbol, side)
        return OrderResult.empty(
            exchange_id,
            symbol,
            side,
            retryable=decision.retryable,
            failure_code=decision.code,
            failure_reason=decision.reason,
        ), 0.0, 0.0
    if result_or_exc is None:
        return OrderResult.empty(exchange_id, symbol, side), 0.0, 0.0
    return result_or_exc


def normalize_close_result(
    result_or_exc,
    exchange_id: str,
    symbol: str,
    side: str,
) -> tuple[OrderResult, float]:
    if isinstance(result_or_exc, Exception):
        _log_order_exception("CLOSE", result_or_exc, exchange_id, symbol, side)
        return OrderResult.empty(exchange_id, symbol, side), 0.0
    if result_or_exc is None:
        return OrderResult.empty(exchange_id, symbol, side), 0.0
    return result_or_exc


def _log_order_exception(
    phase: str,
    reason: Exception,
    exchange_id: str,
    symbol: str,
    side: str,
) -> None:
    _log_trade_issue(
        issue_type="HANDLED_ORDER_LEG_ERROR",
        phase=phase,
        exchange_id=exchange_id,
        symbol=symbol,
        side=side,
        reason=reason,
    )
