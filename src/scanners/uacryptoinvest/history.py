"""No-browser UACryptoInvest Blazor chart history client.

UACI serves old chart ticks through a Blazor Server circuit.  This module
speaks the same SignalR/MessagePack protocol directly so the production bot
does not need a browser or headless runtime on Linux.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import aiohttp
import msgpack

from .config import UACryptoInvestPair


RECORD_SEPARATOR = b"\x1e"


@dataclass(frozen=True)
class BlazorBootState:
    components: list[dict[str, Any]]
    persisted_state: str


def _write_varint(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(data):
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            return value, offset
        shift += 7
    raise ValueError("incomplete MessagePack length prefix")


def _pack_blazor_message(message: list[Any]) -> bytes:
    payload = msgpack.packb(message, use_bin_type=True)
    return _write_varint(len(payload)) + payload


def _decode_blazor_messages(data: bytes) -> list[list[Any]]:
    messages: list[list[Any]] = []
    offset = 0
    while offset < len(data):
        length, offset = _read_varint(data, offset)
        if offset + length > len(data):
            raise ValueError("incomplete MessagePack frame")
        messages.append(msgpack.unpackb(
            data[offset:offset + length], raw=False, strict_map_key=False,
        ))
        offset += length
    return messages


def parse_blazor_boot_state(html: str) -> tuple[list[dict[str, Any]], str]:
    components: list[dict[str, Any]] = []
    persisted_state = ""
    for match in re.finditer(r"<!--(.*?)-->", html, re.S):
        marker = match.group(1).strip()
        if marker.startswith("Blazor:{"):
            data = json.loads(marker[len("Blazor:"):])
            if data.get("type") == "server" and data.get("descriptor"):
                components.append(data)
        elif marker.startswith("Blazor-Server-Component-State:"):
            persisted_state = marker.split(":", 1)[1]
    components.sort(key=lambda item: int(item.get("sequence", 0)))
    if not components:
        raise ValueError("UACryptoInvest chart page did not include Blazor server components")
    return components, persisted_state


def build_history_request_payload(
    pair: UACryptoInvestPair,
    *,
    chart_id: str,
    older_than: int,
    interval: int = 1,
) -> dict[str, Any]:
    return {
        "chartId": chart_id,
        "sources": [
            {
                "sourceId": "price-a",
                "historyParams": {
                    "tokenName": pair.token,
                    "tokenType": 0,
                    "exchange": pair.long_exchange_id,
                },
                "olderThan": int(older_than),
            },
            {
                "sourceId": "price-b",
                "historyParams": {
                    "tokenName": pair.token,
                    "tokenType": 0,
                    "exchange": pair.short_exchange_id,
                },
                "olderThan": int(older_than),
            },
        ],
        "interval": int(interval),
        "olderThan": int(older_than),
    }


def parse_load_history_arguments(raw_args: str) -> tuple[str, dict[str, Any]]:
    args = json.loads(raw_args)
    if not isinstance(args, list) or len(args) < 2:
        raise ValueError("ChartApi.loadHistory arguments had unexpected shape")
    chart_id = str(args[0])
    payload = args[1]
    if not isinstance(payload, dict):
        raise ValueError("ChartApi.loadHistory payload was not an object")
    return chart_id, payload


def _parse_uaci_time(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        head, tail = text.split(".", 1)
        fraction = tail
        zone = ""
        if "+" in fraction:
            fraction, zone = fraction.split("+", 1)
            zone = "+" + zone
        elif "-" in fraction:
            fraction, zone = fraction.split("-", 1)
            zone = "-" + zone
        text = f"{head}.{fraction[:6].ljust(6, '0')}{zone}"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _float_or_none(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _normalize_price_points(rows: Any) -> list[dict[str, float]]:
    if not isinstance(rows, list):
        return []
    out: list[dict[str, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts = _parse_uaci_time(row.get("timestamp") or row.get("time"))
        bid = _float_or_none(row.get("bidPrice") or row.get("bid"))
        ask = _float_or_none(row.get("askPrice") or row.get("ask"))
        if ts is None or bid is None or ask is None:
            continue
        out.append({"time": ts, "bid": bid, "ask": ask})
    return out


def _dedupe_sorted_price_points(
    rows: list[dict[str, float]],
    *,
    since: float | None = None,
) -> list[dict[str, float]]:
    by_time: dict[float, dict[str, float]] = {}
    for row in rows:
        point_time = row["time"]
        if since is not None and point_time < since:
            continue
        by_time[point_time] = row
    return [by_time[key] for key in sorted(by_time)]


def _spread_points(price_a: list[dict[str, float]], price_b: list[dict[str, float]]) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    for left, right in zip(price_a, price_b):
        long_ask = left.get("ask")
        short_bid = right.get("bid")
        if not long_ask or short_bid is None:
            continue
        out.append({
            "time": min(left["time"], right["time"]),
            "value": ((short_bid - long_ask) / long_ask) * 100.0,
        })
    return out


def normalize_history_payload(
    pair: UACryptoInvestPair,
    chart_id: str,
    payload: dict[str, Any],
    *,
    since: float | None = None,
) -> dict[str, Any]:
    price_a = _dedupe_sorted_price_points(
        _normalize_price_points(
            payload.get("bidExchangeData") or payload.get("priceA") or payload.get("price-a"),
        ),
        since=since,
    )
    price_b = _dedupe_sorted_price_points(
        _normalize_price_points(
            payload.get("askExchangeData") or payload.get("priceB") or payload.get("price-b"),
        ),
        since=since,
    )
    return {
        "source": "uacryptoinvest",
        "chart_id": chart_id,
        "chart_code": pair.chart_code,
        "chart_url": pair.chart_url,
        "token": pair.token,
        "symbol": pair.symbol,
        "long_exchange": pair.long_exchange,
        "short_exchange": pair.short_exchange,
        "price_a": price_a,
        "price_b": price_b,
        "spread": _spread_points(price_a, price_b),
    }


def normalize_history_pages(
    pair: UACryptoInvestPair,
    chart_id: str,
    pages: list[dict[str, Any]],
    *,
    since: float | None = None,
) -> dict[str, Any]:
    merged = {"bidExchangeData": [], "askExchangeData": []}
    for page in pages:
        bid_rows = page.get("bidExchangeData") or page.get("priceA") or page.get("price-a") or []
        ask_rows = page.get("askExchangeData") or page.get("priceB") or page.get("price-b") or []
        if isinstance(bid_rows, list):
            merged["bidExchangeData"].extend(bid_rows)
        if isinstance(ask_rows, list):
            merged["askExchangeData"].extend(ask_rows)
    return normalize_history_payload(pair, chart_id, merged, since=since)


def oldest_history_time(payload: dict[str, Any]) -> float | None:
    points = _normalize_price_points(payload.get("bidExchangeData"))
    points.extend(_normalize_price_points(payload.get("askExchangeData")))
    if not points:
        return None
    return min(point["time"] for point in points)


class UACryptoInvestHistoryClient:
    def __init__(self, *, base_url: str, session: aiohttp.ClientSession) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = session

    async def fetch(
        self,
        pair: UACryptoInvestPair,
        *,
        older_than: int | None = None,
        interval: int = 1,
        range_sec: int = 86400,
        max_pages: int = 10,
        timeout_sec: float = 25.0,
    ) -> dict[str, Any]:
        older = int(older_than or time.time())
        since = max(0, older - max(60, int(range_sec)))
        async with self._session.get(pair.chart_url, timeout=timeout_sec) as response:
            response.raise_for_status()
            html = await response.text()
        components, persisted_state = parse_blazor_boot_state(html)
        init_payload = json.dumps(components, separators=(",", ":"))

        async with self._session.post(
            f"{self.base_url}/_blazor/negotiate",
            params={"negotiateVersion": "1"},
            timeout=timeout_sec,
        ) as response:
            response.raise_for_status()
            negotiate = await response.json(content_type=None)
        token = negotiate.get("connectionToken") or negotiate.get("connectionId")
        if not token:
            raise ValueError("UACryptoInvest Blazor negotiate response had no token")
        ws_base = self.base_url.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_base}/_blazor?id={quote(str(token), safe='')}"

        async with self._session.ws_connect(
            ws_url, autoping=True, heartbeat=20, max_msg_size=0, timeout=timeout_sec,
        ) as ws:
            await ws.send_bytes(b'{"protocol":"blazorpack","version":2}' + RECORD_SEPARATOR)
            await asyncio.wait_for(ws.receive(), timeout=timeout_sec)
            await ws.send_bytes(_pack_blazor_message([
                1, {}, "1", "StartCircuit",
                [f"{self.base_url}/", pair.chart_url, init_payload, persisted_state],
                [],
            ]))
            runtime = await self._wait_for_chart_runtime(ws, timeout_sec=timeout_sec)
            chart_id = runtime["chart_id"]
            pages: list[dict[str, Any]] = []
            current_older = older
            for page_index in range(max(1, int(max_pages))):
                history_payload = await self._request_history_page(
                    ws,
                    pair,
                    chart_id=str(chart_id),
                    dotnet_id=runtime["dotnet_id"],
                    invocation_id=f"history-{page_index + 1}",
                    older_than=current_older,
                    interval=interval,
                    timeout_sec=timeout_sec,
                )
                pages.append(history_payload)
                oldest = oldest_history_time(history_payload)
                if oldest is None or oldest <= since:
                    break
                next_older = int(oldest) - 1
                if next_older >= current_older:
                    break
                current_older = next_older
        return normalize_history_pages(pair, str(chart_id), pages, since=since)

    async def _request_history_page(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        pair: UACryptoInvestPair,
        *,
        chart_id: str,
        dotnet_id: Any,
        invocation_id: str,
        older_than: int,
        interval: int,
        timeout_sec: float,
    ) -> dict[str, Any]:
        payload = build_history_request_payload(
            pair, chart_id=chart_id, older_than=older_than, interval=interval,
        )
        await ws.send_bytes(_pack_blazor_message([
            1, {}, None, "BeginInvokeDotNetFromJS",
            [
                invocation_id, None, "OnHistoryRequestedAsync", dotnet_id,
                json.dumps([payload], separators=(",", ":")),
            ],
            [],
        ]))
        raw_args = await self._wait_for_history(ws, timeout_sec=timeout_sec)
        _, history_payload = parse_load_history_arguments(raw_args)
        return history_payload

    async def _wait_for_chart_runtime(
        self, ws: aiohttp.ClientWebSocketResponse, *, timeout_sec: float,
    ) -> dict[str, Any]:
        runtime: dict[str, Any] = {"chart_id": None, "dotnet_id": None}
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            msg = await asyncio.wait_for(ws.receive(), timeout=max(0.1, deadline - time.monotonic()))
            if msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                raise ConnectionError("UACryptoInvest Blazor websocket closed before chart initialized")
            if not isinstance(msg.data, (bytes, bytearray)):
                continue
            for frame in _decode_blazor_messages(bytes(msg.data)):
                await self._handle_runtime_frame(ws, frame, runtime)
            if runtime["chart_id"] and runtime["dotnet_id"] is not None:
                return runtime
        raise TimeoutError("UACryptoInvest chart did not initialize in time")

    async def _wait_for_history(
        self, ws: aiohttp.ClientWebSocketResponse, *, timeout_sec: float,
    ) -> str:
        deadline = time.monotonic() + timeout_sec
        runtime: dict[str, Any] = {"chart_id": None, "dotnet_id": None}
        while time.monotonic() < deadline:
            msg = await asyncio.wait_for(ws.receive(), timeout=max(0.1, deadline - time.monotonic()))
            if msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                raise ConnectionError("UACryptoInvest Blazor websocket closed before history loaded")
            if not isinstance(msg.data, (bytes, bytearray)):
                continue
            for frame in _decode_blazor_messages(bytes(msg.data)):
                raw_args = await self._handle_history_frame(ws, frame, runtime)
                if raw_args is not None:
                    return raw_args
        raise TimeoutError("UACryptoInvest history did not load in time")

    async def _handle_runtime_frame(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        frame: list[Any],
        runtime: dict[str, Any],
    ) -> None:
        await self._handle_history_frame(ws, frame, runtime)

    async def _handle_history_frame(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        frame: list[Any],
        runtime: dict[str, Any],
    ) -> str | None:
        if not isinstance(frame, list) or len(frame) < 5 or frame[0] != 1:
            return None
        target = frame[3]
        args = frame[4]
        if target == "JS.RenderBatch":
            batch_id = args[0] if args else None
            await ws.send_bytes(_pack_blazor_message([
                1, {}, None, "OnRenderCompleted", [batch_id, None], [],
            ]))
            return None
        if target != "JS.BeginInvokeJS" or not isinstance(args, list) or len(args) < 3:
            return None
        call_id = args[0]
        identifier = args[1]
        raw_args = args[2]
        if identifier == "ChartApi.initialize":
            parsed = json.loads(raw_args)
            if parsed and isinstance(parsed[0], dict):
                runtime["dotnet_id"] = parsed[0].get("__dotNetObject")
            await self._end_js(ws, call_id, None)
        elif identifier == "ChartApi.createChart":
            parsed = json.loads(raw_args)
            if parsed:
                runtime["chart_id"] = str(parsed[0])
            await self._end_js(ws, call_id, {"success": True, "chartId": runtime.get("chart_id")})
        elif identifier == "ChartApi.loadHistory":
            return str(raw_args)
        elif call_id is not None:
            await self._end_js(ws, call_id, None)
        return None

    async def _end_js(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        call_id: Any,
        result: Any,
    ) -> None:
        await ws.send_bytes(_pack_blazor_message([
            1, {}, None, "EndInvokeJSFromDotNet", [str(call_id), True, json.dumps(result)], [],
        ]))