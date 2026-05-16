from __future__ import annotations

import hashlib
import hmac
from typing import Callable

from src.adapters.exchange.direct_order_base import (
    DirectOrderRejected,
    JsonWsOrderClient,
    status_from_exchange,
    validate_positive_decimal,
)


class BinanceFuturesWsOrderClient(JsonWsOrderClient):
    """Binance USD-M WebSocket API client for `order.place`."""

    _DEFAULT_ENDPOINT = "wss://ws-fapi.binance.com/ws-fapi/v1?returnRateLimits=false"

    def __init__(
        self,
        api_key: str,
        secret: str,
        *,
        endpoint: str | None = None,
        recv_window: int = 5000,
        now_ms: Callable[[], int] | None = None,
        id_factory: Callable[[], str] | None = None,
    ):
        super().__init__(endpoint=endpoint or self._DEFAULT_ENDPOINT, now_ms=now_ms, id_factory=id_factory)
        self._api_key = api_key
        self._secret = secret.encode("utf-8")
        self._recv_window = int(recv_window)

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
        params: dict[str, str | int] = {
            "apiKey": self._api_key,
            "newOrderRespType": "RESULT",
            "quantity": str(quantity),
            "recvWindow": self._recv_window,
            "side": side.upper(),
            "symbol": native_symbol,
            "timestamp": self._now_ms(),
            "type": order_type.upper(),
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        if order_type.lower() == "limit":
            if price is None:
                raise ValueError("price is required for direct limit orders")
            validate_positive_decimal(price, "price")
            params["price"] = str(price)
            params["timeInForce"] = (time_in_force or "IOC").upper()

        payload = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
        params["signature"] = hmac.new(
            self._secret, payload.encode("utf-8"), hashlib.sha256,
        ).hexdigest()
        return {"id": self._id_factory(), "method": "order.place", "params": params}

    @staticmethod
    def parse_order_result(result: dict) -> dict:
        filled = float(result.get("executedQty") or result.get("cumQty") or 0)
        cost = float(result.get("cumQuote") or 0)
        average = float(result.get("avgPrice") or 0)
        if average <= 0 and filled > 0 and cost > 0:
            average = cost / filled
        return {
            "id": str(result.get("orderId", "")),
            "average": average,
            "filled": filled,
            "status": status_from_exchange(str(result.get("status", ""))),
            "price": float(result.get("price") or 0),
            "cost": cost,
            "info": result,
        }

    async def place_order(self, **kwargs) -> dict:
        async with self._lock:
            request = self.build_order_request(**kwargs)
            payload = await self._send_receive(request, sent_order=True)
            status = int(payload.get("status") or 0)
            if status != 200:
                error = payload.get("error") or {}
                raise DirectOrderRejected(error.get("code"), str(error.get("msg", "")))
            return self.parse_order_result(payload.get("result") or {})
