from __future__ import annotations

import json
from dataclasses import dataclass

from loguru import logger


def _clean_issue_value(value: object) -> str:
    return " ".join(str(value).split())


def _log_trade_issue(
    *,
    issue_type: str,
    phase: str,
    exchange_id: str,
    symbol: str,
    side: str,
    reason: object,
) -> None:
    logger.error(
        "TRADE ISSUE | level=ERROR | type={} | phase={} | exchange={} | symbol={} | side={} | reason={}",
        issue_type,
        phase,
        exchange_id,
        symbol,
        side.upper(),
        _clean_issue_value(reason),
    )


@dataclass(frozen=True)
class OrderFailureDecision:
    retryable: bool
    code: str
    reason: str


class OrderErrorClassifier:
    NON_RETRYABLE_CODES_BY_EXCHANGE = {
        "gateio": {"RISK_CHECK_MARKET_FORBIDDEN"},
    }

    @classmethod
    def classify(
        cls,
        *,
        exchange_id: str,
        symbol: str,
        side: str,
        error: Exception,
    ) -> OrderFailureDecision:
        text = _clean_issue_value(error)
        payload = cls._json_payload(text)
        code = str(payload.get("label") or payload.get("code") or "")
        message = str(payload.get("message") or payload.get("msg") or text)

        if not code and "RISK_CHECK_MARKET_FORBIDDEN" in text:
            code = "RISK_CHECK_MARKET_FORBIDDEN"
        if code in cls.NON_RETRYABLE_CODES_BY_EXCHANGE.get(exchange_id.lower(), set()):
            reason = f"{exchange_id} {side.upper()} {symbol} rejected: {code} - {message}"
            return OrderFailureDecision(retryable=False, code=code, reason=reason)
        return OrderFailureDecision(retryable=True, code=code, reason=text)

    @staticmethod
    def _json_payload(text: str) -> dict:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            return {}
        try:
            payload = json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}
