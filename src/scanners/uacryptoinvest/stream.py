"""No-browser live stream adapter for UACryptoInvest chart ticks."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Any

from loguru import logger

from src.adapters.ws.transport import WSFrame, ws_connect

from .catalog import STREAM_TYPE_DEPTH, STREAM_TYPE_FUNDING, TOKEN_TYPE_FUTURES
from .client import UACryptoInvestClient
from .config import UACryptoInvestPair
from .protocol import SignalRMessagePackCodec


class UACryptoInvestStream:
    def __init__(
        self,
        pairs: Iterable[UACryptoInvestPair],
        *,
        client: UACryptoInvestClient | None = None,
        reconnect_delay_sec: float = 5.0,
    ) -> None:
        self._pairs = list(pairs)
        self._pairs_by_key = {pair.key: pair for pair in self._pairs}
        self._client = client or UACryptoInvestClient()
        self._own_client = client is None
        self._codec = SignalRMessagePackCodec()
        self._reconnect_delay_sec = float(reconnect_delay_sec)
        self._task: asyncio.Task | None = None
        self._closed = False
        self._latest: dict[str, dict[str, float]] = {pair.key: {} for pair in self._pairs}
        self._binary_buffer = b""

    async def start(self) -> None:
        if self._closed or self._task is not None or not self._pairs:
            return
        await self._seed_initial_snapshot()
        self._task = asyncio.create_task(self._run(), name="uacryptoinvest-stream")

    def snapshot(self) -> dict[str, dict[str, float]]:
        return {key: dict(value) for key, value in self._latest.items()}

    async def aclose(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._own_client:
            await self._client.aclose()

    async def _run(self) -> None:
        while not self._closed:
            try:
                await self._connect_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("UACryptoInvest stream reconnect after error: {}", exc)
                await asyncio.sleep(self._reconnect_delay_sec)

    async def _seed_initial_snapshot(self) -> None:
        semaphore = asyncio.Semaphore(8)

        async def seed_pair(pair: UACryptoInvestPair) -> None:
            async with semaphore:
                try:
                    seed = await self._client.chart_snapshot(pair)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("UACryptoInvest seed skipped for {}: {}", pair.key, exc)
                    return
                if seed:
                    self._latest.setdefault(pair.key, {}).update(seed)

        await asyncio.gather(*(seed_pair(pair) for pair in self._pairs))

    async def _connect_once(self) -> None:
        negotiate = await self._client.negotiate()
        url = self._client.websocket_url(negotiate)
        self._binary_buffer = b""
        async with await ws_connect(
            url,
            heartbeat=20.0,
            receive_timeout=90.0,
            headers={"origin": self._client.base_url, "referer": f"{self._client.base_url}/charts"},
        ) as ws:
            await ws.send_text(self._codec.HANDSHAKE_TEXT)
            await self._wait_for_handshake(ws)
            await self._send_subscriptions(ws)
            async for msg in ws:
                if msg.kind == WSFrame.TEXT:
                    continue
                if msg.kind != WSFrame.BINARY:
                    continue
                if self._codec.is_handshake_response(msg.data):
                    continue
                try:
                    messages = self._decode_stream_frames(msg.data)
                except Exception as exc:  # noqa: BLE001
                    self._binary_buffer = b""
                    logger.debug("UACryptoInvest skipped binary frame: {}", exc)
                    continue
                for message in messages:
                    self._handle_hub_message(message)

    async def _wait_for_handshake(self, ws) -> None:  # noqa: ANN001
        while True:
            msg = await ws.receive(timeout=10.0)
            if msg.kind == WSFrame.TEXT and self._codec.is_handshake_response(msg.text):
                return
            if msg.kind == WSFrame.BINARY:
                if self._codec.is_handshake_response(msg.data):
                    return
                for message in self._codec.decode_binary_frames(msg.data):
                    self._handle_hub_message(message)

    def _decode_stream_frames(self, data: bytes) -> list[Any]:
        messages, remainder = self._codec.decode_binary_frames_partial(self._binary_buffer + data)
        self._binary_buffer = remainder
        return messages

    async def _send_subscriptions(self, ws) -> None:  # noqa: ANN001
        invocation = 0
        sent: set[tuple[int, int, str, int]] = set()
        for pair in self._pairs:
            try:
                subscriptions = (
                    (TOKEN_TYPE_FUTURES, pair.long_exchange_id, pair.token, STREAM_TYPE_DEPTH),
                    (TOKEN_TYPE_FUTURES, pair.short_exchange_id, pair.token, STREAM_TYPE_DEPTH),
                    (TOKEN_TYPE_FUTURES, pair.long_exchange_id, pair.token, STREAM_TYPE_FUNDING),
                    (TOKEN_TYPE_FUTURES, pair.short_exchange_id, pair.token, STREAM_TYPE_FUNDING),
                )
            except ValueError as exc:
                logger.debug("UACryptoInvest subscription skipped for {}: {}", pair.key, exc)
                continue
            for subscription in subscriptions:
                if subscription in sent:
                    continue
                sent.add(subscription)
                invocation += 1
                await ws.send_bytes(
                    self._codec.encode_invocation(str(invocation), "Subscribe", [list(subscription)])
                )

    def _handle_hub_message(self, message: Any) -> None:
        if not isinstance(message, list) or not message:
            return
        message_type = message[0]
        if message_type != 1 or len(message) < 5:
            return
        arguments = message[4]
        if not isinstance(arguments, list) or not arguments:
            return
        for tick in arguments:
            self._handle_tick(tick)

    def _handle_tick(self, tick: Any) -> None:
        if not isinstance(tick, list) or len(tick) < 4:
            return
        token_type, exchange_id, token, value = tick[:4]
        if int(token_type) != TOKEN_TYPE_FUTURES:
            return
        token = str(token).upper().removesuffix("USDT")
        for pair in self._pairs_by_key.values():
            if pair.token != token:
                continue
            side = None
            if int(exchange_id) == pair.long_exchange_id:
                side = "long"
            elif int(exchange_id) == pair.short_exchange_id:
                side = "short"
            if side is None:
                continue
            state = self._latest.setdefault(pair.key, {})
            if isinstance(value, list):
                self._apply_depth(state, side, value)
            else:
                self._apply_funding(state, side, value)

    @staticmethod
    def _apply_depth(state: dict[str, float], side: str, value: list[Any]) -> None:
        bid = _first_number(value, 4)
        ask = _first_number(value, 5)
        if bid is None:
            bid = _first_number(value, 0)
        if ask is None:
            ask = _first_number(value, 2)
        if bid is not None:
            state[f"{side}_bid"] = bid
        if ask is not None:
            state[f"{side}_ask"] = ask

    @staticmethod
    def _apply_funding(state: dict[str, float], side: str, value: Any) -> None:
        try:
            state[f"{side}_funding_pct"] = float(value)
        except (TypeError, ValueError):
            return


def _first_number(values: list[Any], index: int) -> float | None:
    if index >= len(values):
        return None
    raw = values[index]
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None