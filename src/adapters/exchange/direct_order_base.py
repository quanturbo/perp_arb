"""Shared direct WebSocket order transport primitives."""

from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Callable, Protocol

from src.adapters.ws.transport import WSConnection, WSFrame, ws_connect


class DirectOrderUnavailable(RuntimeError):
    """Direct transport was unavailable before an order could be submitted."""


class DirectOrderRejected(RuntimeError):
    """Exchange rejected a direct order request with a definite response."""

    def __init__(self, code: int | str | None, message: str):
        self.code = code
        self.message = message
        super().__init__(f"direct order rejected {code}: {message}")


class DirectOrderUnknownState(RuntimeError):
    """Order may have reached the exchange, so automatic fallback is unsafe."""


class DirectOrderClient(Protocol):
    async def close(self) -> None: ...

    async def place_order(
        self,
        *,
        native_symbol: str,
        side: str,
        order_type: str,
        quantity: str,
        price: str | None = None,
        time_in_force: str | None = None,
        reduce_only: bool = False,
    ) -> dict: ...


@dataclass(frozen=True)
class DirectOrderSupport:
    exchange_id: str
    supported: bool
    route: str
    label: str
    reason: str
    requires_password: bool = False

    def to_dict(self, *, enabled: bool = False) -> dict:
        return {
            "exchange_id": self.exchange_id,
            "supported": self.supported,
            "enabled": bool(enabled and self.supported),
            "route": self.route,
            "label": self.label,
            "reason": self.reason,
        }


class JsonWsOrderClient:
    """Shared JSON-over-WebSocket plumbing for exchange order clients."""

    def __init__(
        self,
        *,
        endpoint: str,
        now_ms: Callable[[], int] | None = None,
        id_factory: Callable[[], str] | None = None,
        receive_timeout_sec: float = 5.0,
    ):
        self._endpoint = endpoint
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))
        self._id_factory = id_factory or (lambda: str(time.time_ns()))
        self._receive_timeout_sec = float(receive_timeout_sec)
        self._ws: WSConnection | None = None
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    def _on_new_connection(self) -> None:
        return None

    async def _connect(self) -> WSConnection:
        if self._ws is not None and not self._ws.closed:
            return self._ws
        try:
            self._ws = await ws_connect(
                self._endpoint,
                heartbeat=10.0,
                receive_timeout=max(self._receive_timeout_sec * 2, 20.0),
                handshake_timeout=10.0,
            )
            self._on_new_connection()
        except Exception as exc:
            raise DirectOrderUnavailable(str(exc)) from exc
        return self._ws

    async def _send_receive(self, request: dict, *, sent_order: bool) -> dict:
        ws = await self._connect()
        try:
            await ws.send_text(json.dumps(request, separators=(",", ":")))
            msg = await ws.receive(self._receive_timeout_sec)
        except asyncio.TimeoutError as exc:
            error = "timed out waiting for direct order response"
            if sent_order:
                raise DirectOrderUnknownState(error) from exc
            raise DirectOrderUnavailable(error) from exc
        except Exception as exc:
            if sent_order:
                raise DirectOrderUnknownState(str(exc)) from exc
            raise DirectOrderUnavailable(str(exc)) from exc

        if msg.kind != WSFrame.TEXT:
            error = f"unexpected ws message kind {msg.kind}"
            if sent_order:
                raise DirectOrderUnknownState(error)
            raise DirectOrderUnavailable(error)
        try:
            return json.loads(msg.text)
        except json.JSONDecodeError as exc:
            error = "exchange returned malformed direct order response"
            if sent_order:
                raise DirectOrderUnknownState(error) from exc
            raise DirectOrderUnavailable(error) from exc


def validate_positive_decimal(value: str, name: str) -> None:
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0:
        raise ValueError(f"{name} must be a positive finite value")


def positive_integer_contracts(value: str) -> int:
    try:
        contracts = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("quantity must be integer contracts for Gate.io direct orders") from exc
    if not contracts.is_finite() or contracts <= 0 or contracts != contracts.to_integral_value():
        raise ValueError("quantity must be integer contracts for Gate.io direct orders")
    return int(contracts)


def status_from_exchange(status: str) -> str:
    status_raw = str(status or "").upper()
    status_map = {
        "NEW": "open",
        "LIVE": "open",
        "PARTIALLY_FILLED": "open",
        "PARTIALLYFILLED": "open",
        "FILLED": "closed",
        "CANCELED": "canceled",
        "CANCELLED": "canceled",
        "EXPIRED": "canceled",
        "REJECTED": "rejected",
    }
    return status_map.get(status_raw, status_raw.lower())
