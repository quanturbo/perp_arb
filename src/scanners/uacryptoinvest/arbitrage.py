"""No-browser UACryptoInvest arbitrage table reader.

UACI's arbitrage board is a Blazor Server component, not a plain REST API.
This client speaks the same SignalR protocol as a browser just far enough to
request the first virtualized rows and extract chart codes from render batches.
"""

from __future__ import annotations

import asyncio
import json
import re
import struct
import time
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import quote

import aiohttp
from loguru import logger

from src.scanners.base import ScanOffer
from src.scanners.exchange_map import map_source_to_bot

from .config import DEFAULT_BASE_URL, UACryptoInvestPair, parse_pairs
from .history import (
    RECORD_SEPARATOR,
    _decode_blazor_messages,
    _pack_blazor_message,
    parse_blazor_boot_state,
)


DEFAULT_ARBITRAGE_FILTERS: dict[str, Any] = {
    "MinFuturesVolume": None,
    "MinFSpreadApr": None,
    "DifferentIntervals": False,
    "MetalsAllowed": False,
    "StocksAllowed": False,
    "ShowClosedTradfi": False,
    "SortPropertyName": "FSpreadApr",
    "Descending": True,
    "SelectedExchanges": [],
    "TokenName": "",
    "GroupTokenName": "",
    "MinSpread": None,
    "Enabled": True,
    "IsValid": True,
    "ValidationMessage": "",
    "FavoriteIds": [],
}

_CHART_CODE_RE = re.compile(
    r"charts=([A-Za-z0-9]+-[A-Za-z0-9]+-Futures-[A-Za-z0-9]+-Futures)"
)
_CHART_URL_RE = re.compile(
    r"https://uacryptoinvest\.com/charts\?charts="
    r"([A-Za-z0-9]+-[A-Za-z0-9]+-Futures-[A-Za-z0-9]+-Futures)"
)
_SPACER_BEFORE_ARGS = [0, 0, 641.6279296875]
_SPACER_AFTER_ARGS = [16377.358154296875, 0, 641.6279296875]


@dataclass(frozen=True)
class _CollapsedSummary:
    source_pair_count: int | None = None
    funding_long_pct: float | None = None
    funding_short_pct: float | None = None
    funding_interval_h_long: int | None = None
    funding_interval_h_short: int | None = None


def browser_end_invoke_payload(call_id: Any, result: Any) -> str:
    return json.dumps([call_id, True, result], separators=(",", ":"))


def chart_codes_from_render_strings(strings: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for text in strings:
        for match in _CHART_CODE_RE.finditer(str(text or "")):
            code = match.group(1)
            if code not in seen:
                seen.add(code)
                out.append(code)
    return out


def pairs_from_render_strings(strings: Iterable[str], *, limit: int | None = None) -> list[UACryptoInvestPair]:
    pairs: list[UACryptoInvestPair] = []
    for code in chart_codes_from_render_strings(strings):
        try:
            pair = parse_pairs(code)[0]
        except ValueError:
            continue
        pairs.append(pair)
        if limit is not None and len(pairs) >= limit:
            break
    return pairs


def offers_from_render_strings(
    strings: Iterable[str],
    *,
    fetched_at: float | None = None,
    limit: int | None = None,
) -> list[ScanOffer]:
    values = [str(value or "") for value in strings]
    anchors: list[tuple[int, str, str]] = []
    for index, text in enumerate(values):
        match = _CHART_URL_RE.search(text)
        if match:
            anchors.append((index, match.group(0), match.group(1)))
    summaries: dict[str, _CollapsedSummary] = {}
    for rank, (index, _chart_url, chart_code) in enumerate(anchors):
        next_index = anchors[rank + 1][0] if rank + 1 < len(anchors) else len(values)
        summary = _collapsed_summary_from_segment(chart_code, values[index:next_index])
        if summary is not None and chart_code not in summaries:
            summaries[chart_code] = summary
    offers: list[ScanOffer] = []
    seen: set[str] = set()
    ts = time.time() if fetched_at is None else float(fetched_at)
    for rank, (index, chart_url, chart_code) in enumerate(anchors):
        if chart_code in seen:
            continue
        next_index = anchors[rank + 1][0] if rank + 1 < len(anchors) else len(values)
        segment = values[index:next_index]
        offer = _offer_from_segment(
            chart_url, chart_code, segment, len(offers), ts,
            summary=summaries.get(chart_code),
        )
        if offer is not None:
            seen.add(chart_code)
            offers.append(offer)
            if limit is not None and len(offers) >= limit:
                break
    return offers


def group_arbitrage_filters(token: str, base_filters: dict[str, Any] | None = None) -> dict[str, Any]:
    filters = dict(DEFAULT_ARBITRAGE_FILTERS if base_filters is None else base_filters)
    filters["GroupTokenName"] = _token_key(token)
    filters["TokenName"] = ""
    return filters


def offers_for_token_from_render_strings(
    strings: Iterable[str],
    token: str,
    *,
    fetched_at: float | None = None,
    limit: int | None = None,
) -> list[ScanOffer]:
    wanted = _token_key(token)
    if not wanted:
        return []
    offers = [
        offer for offer in offers_from_render_strings(strings, fetched_at=fetched_at)
        if _token_key(offer.coin or offer.symbol) == wanted
    ]
    return offers[:limit] if limit is not None else offers


def strings_from_render_batch(batch: bytes) -> list[str]:
    if len(batch) < 24:
        return []
    try:
        string_table_start = _read_i32(batch, len(batch) - 4)
    except struct.error:
        return []
    if string_table_start < 0 or string_table_start >= len(batch) - 4:
        return []
    count = (len(batch) - 4 - string_table_start) // 4
    strings: list[str] = []
    for index in range(count):
        try:
            offset = _read_i32(batch, string_table_start + index * 4)
            length, body_offset = _read_varint(batch, offset)
        except (struct.error, ValueError):
            continue
        if length < 0 or body_offset + length > len(batch):
            continue
        text = batch[body_offset:body_offset + length].decode("utf-8", errors="ignore")
        if text:
            strings.append(text)
    return strings


class UACryptoInvestArbitrageClient:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        filters: dict[str, Any] | None = None,
        timeout_sec: float = 25.0,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._filters = dict(filters) if filters is not None else None
        self._timeout_sec = float(timeout_sec)
        self._session = session
        self._own_session = session is None

    async def fetch_pairs(self, *, limit: int = 80) -> list[UACryptoInvestPair]:
        session = self._session or aiohttp.ClientSession(headers={"user-agent": "Mozilla/5.0"})
        if self._session is None:
            self._session = session
        try:
            strings = await self._fetch_render_strings(session, min_codes=min(limit, 20))
            return pairs_from_render_strings(strings, limit=limit)
        finally:
            if self._own_session:
                await session.close()
                self._session = None

    async def fetch_offers(self, *, limit: int = 80) -> list[ScanOffer]:
        session = self._session or aiohttp.ClientSession(headers={"user-agent": "Mozilla/5.0"})
        if self._session is None:
            self._session = session
        try:
            strings = await self._fetch_render_strings(session, min_codes=min(limit, 20))
            return offers_from_render_strings(strings, limit=limit)
        finally:
            if self._own_session:
                await session.close()
                self._session = None

    async def fetch_token_offers(self, token: str, *, limit: int = 80) -> list[ScanOffer]:
        token_key = _token_key(token)
        if not token_key:
            return []
        session = self._session or aiohttp.ClientSession(headers={"user-agent": "Mozilla/5.0"})
        if self._session is None:
            self._session = session
        previous_filters = self._filters
        self._filters = group_arbitrage_filters(token_key, previous_filters)
        try:
            strings = await self._fetch_render_strings(session, min_codes=min(max(limit, 20), 80))
            return offers_for_token_from_render_strings(strings, token_key, limit=limit)
        finally:
            self._filters = previous_filters
            if self._own_session:
                await session.close()
                self._session = None

    async def _fetch_render_strings(
        self,
        session: aiohttp.ClientSession,
        *,
        min_codes: int = 20,
    ) -> list[str]:
        page_url = f"{self.base_url}/arbitrage"
        async with session.get(page_url, timeout=self._timeout_sec) as response:
            response.raise_for_status()
            html = await response.text()
        components, persisted_state = parse_blazor_boot_state(html)
        init_payload = json.dumps(components, separators=(",", ":"))
        negotiate = await self._negotiate(session)
        token = negotiate.get("connectionToken") or negotiate.get("connectionId")
        if not token:
            raise ValueError("UACryptoInvest Blazor negotiate response had no token")
        ws_base = self.base_url.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_base}/_blazor?id={quote(str(token), safe='')}"

        strings: list[str] = []
        async with session.ws_connect(
            ws_url,
            autoping=True,
            heartbeat=20,
            max_msg_size=0,
            timeout=self._timeout_sec,
        ) as ws:
            await ws.send_bytes(b'{"protocol":"blazorpack","version":1}' + RECORD_SEPARATOR)
            await asyncio.wait_for(ws.receive(), timeout=self._timeout_sec)
            await self._send(ws, [
                1, {}, "1", "StartCircuit",
                [f"{self.base_url}/", page_url, init_payload, persisted_state],
                [],
            ])
            deadline = time.monotonic() + self._timeout_sec
            spacer_sent = False
            invoke_id = 1
            while time.monotonic() < deadline:
                try:
                    msg = await asyncio.wait_for(
                        ws.receive(), timeout=max(0.1, deadline - time.monotonic()),
                    )
                except asyncio.TimeoutError:
                    break
                if msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                    break
                if not isinstance(msg.data, (bytes, bytearray)):
                    continue
                for frame in _decode_blazor_messages(bytes(msg.data)):
                    next_id, sent = await self._handle_frame(
                        ws, frame, strings, invoke_id=invoke_id, spacer_sent=spacer_sent,
                    )
                    invoke_id = next_id
                    spacer_sent = spacer_sent or sent
                    if len(chart_codes_from_render_strings(strings)) >= min_codes:
                        return strings
        return strings

    async def _negotiate(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        async with session.post(
            f"{self.base_url}/_blazor/negotiate",
            params={"negotiateVersion": "1"},
            timeout=self._timeout_sec,
        ) as response:
            response.raise_for_status()
            return await response.json(content_type=None)

    async def _handle_frame(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        frame: Any,
        strings: list[str],
        *,
        invoke_id: int,
        spacer_sent: bool,
    ) -> tuple[int, bool]:
        if not isinstance(frame, list) or len(frame) < 5 or frame[0] != 1:
            return invoke_id, False
        target = frame[3]
        args = frame[4]
        if target == "JS.RenderBatch" and isinstance(args, list) and len(args) >= 2:
            batch_id = args[0]
            batch = args[1]
            if isinstance(batch, (bytes, bytearray)):
                strings.extend(strings_from_render_batch(bytes(batch)))
            await self._send(ws, [1, {}, None, "OnRenderCompleted", [batch_id, None], []])
            return invoke_id, False
        if target != "JS.BeginInvokeJS" or not isinstance(args, list) or len(args) < 2:
            return invoke_id, False
        call_id = args[0]
        identifier = args[1]
        raw_args = args[2] if len(args) > 2 else None
        result = self._js_result(identifier, raw_args)
        await self._end_js(ws, call_id, result)
        if identifier == "Blazor._internal.Virtualize.init" and not spacer_sent:
            dotnet_id = _dotnet_object_id(raw_args) or 3
            await self._begin_dotnet(
                ws, invoke_id, "OnSpacerBeforeVisible", dotnet_id, _SPACER_BEFORE_ARGS,
            )
            invoke_id += 1
            await self._begin_dotnet(
                ws, invoke_id, "OnSpacerAfterVisible", dotnet_id, _SPACER_AFTER_ARGS,
            )
            invoke_id += 1
            return invoke_id, True
        return invoke_id, False

    def _js_result(self, identifier: Any, raw_args: Any) -> Any:
        if identifier == "localStorage.getItem" and "arb_filters_Futures" in str(raw_args or ""):
            if self._filters is None:
                return None
            return json.dumps(self._filters, separators=(",", ":"))
        if identifier in {"popupButtonInterop.init", "import"}:
            return {"__jsObjectId": 2}
        if identifier == "initDropdown":
            return {"savedValue": 0}
        return None

    async def _end_js(self, ws: aiohttp.ClientWebSocketResponse, call_id: Any, result: Any) -> None:
        await self._send(ws, [
            1, {}, None, "EndInvokeJSFromDotNet",
            [call_id, True, browser_end_invoke_payload(call_id, result)],
            [],
        ])

    async def _begin_dotnet(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        invoke_id: int,
        method: str,
        dotnet_id: int,
        args: list[Any],
    ) -> None:
        await self._send(ws, [
            1, {}, None, "BeginInvokeDotNetFromJS",
            [str(invoke_id), None, method, dotnet_id, json.dumps(args, separators=(",", ":"))],
            [],
        ])

    async def _send(self, ws: aiohttp.ClientWebSocketResponse, frame: list[Any]) -> None:
        await ws.send_bytes(_pack_blazor_message(frame))


def _dotnet_object_id(raw_args: Any) -> int | None:
    try:
        parsed = json.loads(raw_args or "[]")
    except (TypeError, ValueError):
        return None
    if not parsed or not isinstance(parsed[0], dict):
        return None
    value = parsed[0].get("__dotNetObject")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _token_key(value: str) -> str:
    text = (value or "").strip().upper()
    if "/" in text:
        text = text.split("/", 1)[0]
    if text.endswith("USDT") and len(text) > 4:
        text = text[:-4]
    return text


def _read_i32(data: bytes, offset: int) -> int:
    return struct.unpack_from("<i", data, offset)[0]


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(data):
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, offset
        shift += 7
        if shift > 35:
            raise ValueError("render batch string length varint is too large")
    raise ValueError("incomplete render batch string length")


async def safe_fetch_arbitrage_pairs(client: UACryptoInvestArbitrageClient, *, limit: int) -> list[UACryptoInvestPair]:
    try:
        return await client.fetch_pairs(limit=limit)
    except Exception as exc:  # noqa: BLE001
        logger.warning("UACryptoInvest arbitrage table fetch failed: {}", exc)
        return []


async def safe_fetch_arbitrage_offers(client: UACryptoInvestArbitrageClient, *, limit: int) -> list[ScanOffer]:
    try:
        return await client.fetch_offers(limit=limit)
    except Exception as exc:  # noqa: BLE001
        logger.warning("UACryptoInvest arbitrage table offer fetch failed: {}", exc)
        return []


def _offer_from_segment(
    chart_url: str,
    chart_code: str,
    segment: list[str],
    source_rank: int,
    fetched_at: float,
    *,
    summary: _CollapsedSummary | None = None,
) -> ScanOffer | None:
    parts = chart_code.split("-")
    if len(parts) != 5:
        return None
    token, long_exchange_raw, _, short_exchange_raw, _ = parts
    long_exchange = _source_exchange(long_exchange_raw)
    short_exchange = _source_exchange(short_exchange_raw)
    percent_values = _percent_values(segment)
    if len(percent_values) >= 3:
        funding_spread, apr_pct, open_spread_pct = percent_values[-3:]
    elif percent_values:
        funding_spread = 0.0
        apr_pct = 0.0
        open_spread_pct = percent_values[-1]
    else:
        funding_spread = 0.0
        apr_pct = 0.0
        open_spread_pct = 0.0
    long_price, long_volume = _leg_price_and_volume(segment, long_exchange_raw)
    short_price, short_volume = _leg_price_and_volume(segment, short_exchange_raw)
    if long_price is None or short_price is None or len(percent_values) < 3:
        return None
    funding_long_pct = 0.0
    funding_short_pct = funding_spread
    funding_interval_h_long = 8
    funding_interval_h_short = 8
    source_pair_count = None
    if summary is not None:
        funding_long_pct = summary.funding_long_pct if summary.funding_long_pct is not None else funding_long_pct
        funding_short_pct = summary.funding_short_pct if summary.funding_short_pct is not None else funding_short_pct
        funding_interval_h_long = summary.funding_interval_h_long or funding_interval_h_long
        funding_interval_h_short = summary.funding_interval_h_short or funding_interval_h_short
        source_pair_count = summary.source_pair_count
    return ScanOffer(
        source="uacryptoinvest",
        symbol=f"{token.upper()}USDT",
        coin=token.upper(),
        source_exchange_long=long_exchange,
        source_exchange_short=short_exchange,
        bot_exchange_long=map_source_to_bot(long_exchange),
        bot_exchange_short=map_source_to_bot(short_exchange),
        long_price=long_price or 0.0,
        short_price=short_price or 0.0,
        open_spread_pct=open_spread_pct,
        funding_long_pct=funding_long_pct,
        funding_short_pct=funding_short_pct,
        funding_interval_h_long=funding_interval_h_long,
        funding_interval_h_short=funding_interval_h_short,
        next_funding_ts=None,
        apr_pct=apr_pct,
        volume_24h_usdt_long=long_volume or 0.0,
        volume_24h_usdt_short=short_volume or 0.0,
        chart_url=chart_url,
        source_rank=source_rank,
        source_pair_count=source_pair_count,
        fetched_at=fetched_at,
    )


def _collapsed_summary_from_segment(chart_code: str, segment: list[str]) -> _CollapsedSummary | None:
    parts = chart_code.split("-")
    if len(parts) != 5:
        return None
    token = parts[0].upper()
    token_index = None
    for index, text in enumerate(segment[:12]):
        if text.strip().upper() == token:
            token_index = index
            break
    if token_index is None:
        return None
    additional_pairs = _plus_count_after(segment, token_index)
    pair_count = additional_pairs + 1 if additional_pairs is not None else None
    funding_values = _funding_values_with_intervals(segment[token_index + 1:])
    funding_long = funding_values[0][0] if len(funding_values) >= 1 else None
    interval_long = funding_values[0][1] if len(funding_values) >= 1 else None
    funding_short = funding_values[1][0] if len(funding_values) >= 2 else None
    interval_short = funding_values[1][1] if len(funding_values) >= 2 else None
    if pair_count is None and funding_long is None and funding_short is None:
        return None
    return _CollapsedSummary(
        source_pair_count=pair_count,
        funding_long_pct=funding_long,
        funding_short_pct=funding_short,
        funding_interval_h_long=interval_long,
        funding_interval_h_short=interval_short,
    )


def _plus_count_after(segment: list[str], start_index: int) -> int | None:
    for index in range(start_index + 1, min(len(segment) - 1, start_index + 8)):
        if segment[index].strip() != "+":
            continue
        value = _parse_display_number(segment[index + 1])
        if value is not None and value >= 0:
            return int(value)
    return None


def _funding_values_with_intervals(segment: list[str]) -> list[tuple[float, int | None]]:
    out: list[tuple[float, int | None]] = []
    for index, text in enumerate(segment[:-1]):
        if segment[index + 1].strip() != "%":
            continue
        value = _parse_display_number(text)
        if value is None:
            continue
        interval = None
        for lookahead in range(index + 2, min(len(segment) - 1, index + 8)):
            if segment[lookahead + 1].strip().lower().startswith("h"):
                raw_interval = _parse_display_number(segment[lookahead])
                if raw_interval is not None and raw_interval > 0:
                    interval = int(raw_interval)
                break
        out.append((value, interval))
        if len(out) >= 2:
            break
    return out


def _source_exchange(value: str) -> str:
    return (value or "").strip().replace("_", "-").lower()


def _percent_values(segment: list[str]) -> list[float]:
    out: list[float] = []
    for index, text in enumerate(segment[:-1]):
        if segment[index + 1].strip() != "%":
            continue
        value = _parse_display_number(text)
        if value is not None:
            out.append(value)
    return out


def _leg_price_and_volume(segment: list[str], exchange_name: str) -> tuple[float | None, float | None]:
    start = _find_exchange_index(segment, exchange_name)
    if start is None:
        return None, None
    volume: float | None = None
    skip_volume = False
    for text in segment[start + 1:start + 60]:
        stripped = text.strip()
        if stripped == "$":
            skip_volume = True
            continue
        value = _parse_display_number(stripped)
        if value is None:
            continue
        if skip_volume:
            volume = value
            skip_volume = False
            continue
        return value, volume
    return None, volume


def _find_exchange_index(segment: list[str], exchange_name: str) -> int | None:
    wanted = (exchange_name or "").strip().lower()
    for index, text in enumerate(segment):
        if text.strip().lower() == wanted:
            return index
    return None


def _parse_display_number(raw: str) -> float | None:
    text = (raw or "").strip()
    if not text:
        return None
    text = text.replace(" ", "").replace(",", ".")
    multiplier = 1.0
    if text[-1:] in {"K", "k"}:
        multiplier = 1_000.0
        text = text[:-1]
    elif text[-1:] in {"M", "m"}:
        multiplier = 1_000_000.0
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None