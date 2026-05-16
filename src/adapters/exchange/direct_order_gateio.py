from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from typing import Callable

from src.adapters.exchange.direct_order_base import (
    DirectOrderRejected,
    DirectOrderUnavailable,
    DirectOrderUnknownState,
    JsonWsOrderClient,
    positive_integer_contracts,
    status_from_exchange,
    validate_positive_decimal,
)
from src.adapters.ws.transport import WSFrame


class GateioFuturesWsOrderClient(JsonWsOrderClient):
    """Gate.io futures WebSocket API client for `futures.order_place`."""

    _DEFAULT_ENDPOINT = "wss://fx-ws.gateio.ws/v4/ws/usdt"

    def __init__(
        self,
        api_key: str,
        secret: str,
        *,
        endpoint: str | None = None,
        now_ms: Callable[[], int] | None = None,
        id_factory: Callable[[], str] | None = None,
    ):
        super().__init__(endpoint=endpoint or self._DEFAULT_ENDPOINT, now_ms=now_ms, id_factory=id_factory)
        self._api_key = api_key
        self._secret = secret.encode("utf-8")
        self._authenticated = False

    def _on_new_connection(self) -> None:
        self._authenticated = False

    def _signature(self, *, channel: str, req_param_json: str, timestamp: str) -> str:
        message = f"api\n{channel}\n{req_param_json}\n{timestamp}"
        return hmac.new(self._secret, message.encode("utf-8"), hashlib.sha512).hexdigest()

    def build_login_request(self) -> dict:
        request_id = self._id_factory()
        timestamp = str(self._now_ms() // 1000)
        channel = "futures.login"
        return {
            "time": int(timestamp),
            "channel": channel,
            "event": "api",
            "payload": {
                "req_id": request_id,
                "api_key": self._api_key,
                "timestamp": timestamp,
                "signature": self._signature(
                    channel=channel,
                    req_param_json="",
                    timestamp=timestamp,
                ),
            },
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
        contracts = positive_integer_contracts(quantity)
        signed_size = contracts if side.lower() == "buy" else -contracts
        request_id = self._id_factory()
        timestamp = str(self._now_ms() // 1000)
        order_type_l = order_type.lower()
        tif = (time_in_force or ("IOC" if order_type_l == "market" else "IOC")).lower()
        order: dict[str, str | int | bool] = {
            "contract": native_symbol,
            "size": signed_size,
            "price": "0" if order_type_l == "market" else str(price),
            "tif": tif,
            "text": f"t-{request_id[:26]}",
        }
        if order_type_l == "limit":
            if price is None:
                raise ValueError("price is required for direct limit orders")
            validate_positive_decimal(price, "price")
            order["price"] = str(price)
        if reduce_only:
            order["reduce_only"] = True

        channel = "futures.order_place"
        req_param_json = json.dumps(order, separators=(",", ":"))
        return {
            "time": int(timestamp),
            "channel": channel,
            "event": "api",
            "payload": {
                "req_id": request_id,
                "api_key": self._api_key,
                "timestamp": timestamp,
                "signature": self._signature(
                    channel=channel,
                    req_param_json=req_param_json,
                    timestamp=timestamp,
                ),
                "req_param": order,
            },
        }

    async def _send_receive_api_result(self, request: dict, *, sent_order: bool) -> dict:
        ws = await self._connect()
        request_id = request["payload"]["req_id"]
        try:
            await ws.send_text(json.dumps(request, separators=(",", ":")))
            deadline = time.monotonic() + self._receive_timeout_sec
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise asyncio.TimeoutError()
                msg = await ws.receive(remaining)
                if msg.kind != WSFrame.TEXT:
                    raise DirectOrderUnknownState(f"unexpected ws message kind {msg.kind}")
                payload = json.loads(msg.text)
                if str(payload.get("request_id")) != str(request_id):
                    continue
                if payload.get("ack") is True:
                    continue
                return payload
        except asyncio.TimeoutError as exc:
            error = "timed out waiting for Gate.io direct WS API result"
            if sent_order:
                raise DirectOrderUnknownState(error) from exc
            raise DirectOrderUnavailable(error) from exc
        except json.JSONDecodeError as exc:
            error = "Gate.io returned malformed direct WS API response"
            if sent_order:
                raise DirectOrderUnknownState(error) from exc
            raise DirectOrderUnavailable(error) from exc
        except DirectOrderUnknownState:
            raise
        except Exception as exc:
            if sent_order:
                raise DirectOrderUnknownState(str(exc)) from exc
            raise DirectOrderUnavailable(str(exc)) from exc

    async def _ensure_authenticated(self) -> None:
        if self._authenticated:
            return
        payload = await self._send_receive_api_result(self.build_login_request(), sent_order=False)
        errs = ((payload.get("data") or {}).get("errs") or {})
        if errs:
            raise DirectOrderUnavailable(f"{errs.get('label')}: {errs.get('message')}")
        self._authenticated = True

    @staticmethod
    def parse_order_result(payload: dict) -> dict:
        data = payload.get("data") or {}
        result = data.get("result") or {}
        filled_contracts = max(0.0, float(result.get("size") or 0) - float(result.get("left") or 0))
        average = float(result.get("fill_price") or 0)
        return {
            "id": str(result.get("id") or ""),
            "average": average,
            "filled": filled_contracts,
            "status": status_from_exchange(str(result.get("status", ""))),
            "price": float(result.get("price") or 0),
            "cost": filled_contracts * average,
            "info": payload,
        }

    async def place_order(self, **kwargs) -> dict:
        async with self._lock:
            await self._ensure_authenticated()
            request = self.build_order_request(**kwargs)
            payload = await self._send_receive_api_result(request, sent_order=True)
            errs = ((payload.get("data") or {}).get("errs") or {})
            if errs:
                raise DirectOrderRejected(errs.get("label"), str(errs.get("message", "")))
            return self.parse_order_result(payload)
