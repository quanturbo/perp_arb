from __future__ import annotations

import hashlib
import hmac
from typing import Callable

from src.adapters.exchange.direct_order_base import (
    DirectOrderRejected,
    DirectOrderUnavailable,
    JsonWsOrderClient,
    validate_positive_decimal,
)


class BybitWsOrderClient(JsonWsOrderClient):
    """Bybit V5 WebSocket trade client for `order.create`."""

    _DEFAULT_ENDPOINT = "wss://stream.bybit.com/v5/trade"

    def __init__(
        self,
        api_key: str,
        secret: str,
        *,
        endpoint: str | None = None,
        category: str = "linear",
        recv_window: int = 5000,
        now_ms: Callable[[], int] | None = None,
        id_factory: Callable[[], str] | None = None,
    ):
        super().__init__(endpoint=endpoint or self._DEFAULT_ENDPOINT, now_ms=now_ms, id_factory=id_factory)
        self._api_key = api_key
        self._secret = secret.encode("utf-8")
        self._category = category
        self._recv_window = int(recv_window)
        self._authenticated = False

    def _on_new_connection(self) -> None:
        self._authenticated = False

    def build_auth_request(self) -> dict:
        expires = self._now_ms() + 10_000
        signature = hmac.new(
            self._secret, f"GET/realtime{expires}".encode("utf-8"), hashlib.sha256,
        ).hexdigest()
        return {"op": "auth", "args": [self._api_key, expires, signature]}

    def build_order_request(
        self,
        *,
        native_symbol: str,
        side: str,
        order_type: str,
        quantity: str,
        price: str | None = None,
        time_in_force: str | None = None,
        reduce_only: bool = False,
    ) -> dict:
        validate_positive_decimal(quantity, "quantity")
        order_type_title = "Market" if order_type.lower() == "market" else "Limit"
        args: dict[str, str | bool] = {
            "category": self._category,
            "symbol": native_symbol,
            "side": "Buy" if side.lower() == "buy" else "Sell",
            "orderType": order_type_title,
            "qty": str(quantity),
        }
        if order_type.lower() == "limit":
            if price is None:
                raise ValueError("price is required for direct limit orders")
            validate_positive_decimal(price, "price")
            args["price"] = str(price)
            args["timeInForce"] = (time_in_force or "IOC").upper()
        if reduce_only:
            args["reduceOnly"] = True
        return {
            "reqId": self._id_factory()[:36],
            "header": {
                "X-BAPI-TIMESTAMP": str(self._now_ms()),
                "X-BAPI-RECV-WINDOW": str(self._recv_window),
            },
            "op": "order.create",
            "args": [args],
        }

    async def _ensure_authenticated(self) -> None:
        if self._authenticated:
            return
        payload = await self._send_receive(self.build_auth_request(), sent_order=False)
        if int(payload.get("retCode") or -1) == 0:
            self._authenticated = True
            return
        raise DirectOrderUnavailable(str(payload.get("retMsg") or payload))

    @staticmethod
    def parse_order_result(payload: dict) -> dict:
        data = payload.get("data") or {}
        return {
            "id": str(data.get("orderId") or data.get("orderLinkId") or ""),
            "average": 0.0,
            "filled": 0.0,
            "status": "open",
            "price": 0.0,
            "cost": 0.0,
            "info": payload,
        }

    async def place_order(self, **kwargs) -> dict:
        async with self._lock:
            await self._ensure_authenticated()
            request = self.build_order_request(**kwargs)
            payload = await self._send_receive(request, sent_order=True)
            code = int(payload.get("retCode") or -1)
            if code != 0:
                raise DirectOrderRejected(code, str(payload.get("retMsg", "")))
            return self.parse_order_result(payload)
