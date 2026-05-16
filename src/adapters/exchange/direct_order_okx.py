from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Callable

from src.adapters.exchange.direct_order_base import (
    DirectOrderRejected,
    DirectOrderUnavailable,
    JsonWsOrderClient,
    validate_positive_decimal,
)


class OkxWsOrderClient(JsonWsOrderClient):
    """OKX V5 private WebSocket trade client for `op=order`."""

    _DEFAULT_ENDPOINT = "wss://ws.okx.com:8443/ws/v5/private"

    def __init__(
        self,
        api_key: str,
        secret: str,
        passphrase: str,
        *,
        endpoint: str | None = None,
        td_mode: str = "cross",
        now_ms: Callable[[], int] | None = None,
        id_factory: Callable[[], str] | None = None,
    ):
        super().__init__(endpoint=endpoint or self._DEFAULT_ENDPOINT, now_ms=now_ms, id_factory=id_factory)
        self._api_key = api_key
        self._secret = secret.encode("utf-8")
        self._passphrase = passphrase
        self._td_mode = td_mode
        self._authenticated = False

    def _on_new_connection(self) -> None:
        self._authenticated = False

    def build_login_request(self) -> dict:
        timestamp = str(self._now_ms() // 1000)
        message = f"{timestamp}GET/users/self/verify"
        signature = base64.b64encode(
            hmac.new(self._secret, message.encode("utf-8"), hashlib.sha256).digest()
        ).decode("ascii")
        return {
            "op": "login",
            "args": [{
                "apiKey": self._api_key,
                "passphrase": self._passphrase,
                "timestamp": timestamp,
                "sign": signature,
            }],
        }

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
        order_type_l = order_type.lower()
        tif = (time_in_force or "").upper()
        okx_order_type = "market" if order_type_l == "market" else "limit"
        if order_type_l == "limit" and tif in {"IOC", "FOK"}:
            okx_order_type = tif.lower()
        args: dict[str, str] = {
            "instId": native_symbol,
            "tdMode": self._td_mode,
            "side": side.lower(),
            "ordType": okx_order_type,
            "sz": str(quantity),
        }
        if order_type_l == "limit":
            if price is None:
                raise ValueError("price is required for direct limit orders")
            validate_positive_decimal(price, "price")
            args["px"] = str(price)
        if reduce_only:
            args["reduceOnly"] = "true"
        return {"id": self._id_factory(), "op": "order", "args": [args]}

    async def _ensure_authenticated(self) -> None:
        if self._authenticated:
            return
        payload = await self._send_receive(self.build_login_request(), sent_order=False)
        if payload.get("event") == "login" and str(payload.get("code")) == "0":
            self._authenticated = True
            return
        raise DirectOrderUnavailable(str(payload.get("msg") or payload))

    @staticmethod
    def parse_order_result(payload: dict) -> dict:
        data = (payload.get("data") or [{}])[0]
        order_id = str(data.get("ordId") or data.get("clOrdId") or "")
        return {
            "id": order_id,
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
            if str(payload.get("code", "0")) != "0":
                raise DirectOrderRejected(payload.get("code"), str(payload.get("msg", "")))
            for item in payload.get("data") or []:
                if str(item.get("sCode", "0")) != "0":
                    raise DirectOrderRejected(item.get("sCode"), str(item.get("sMsg", "")))
            return self.parse_order_result(payload)
