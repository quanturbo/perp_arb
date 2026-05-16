"""SignalR MessagePack framing for UACryptoInvest live hub."""

from __future__ import annotations

from typing import Any

import msgpack


class SignalRMessagePackCodec:
    HANDSHAKE_TEXT = '{"protocol":"messagepack","version":1}\x1e'
    HANDSHAKE_ACK = b"{}\x1e"

    def is_handshake_response(self, data: bytes | str) -> bool:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return data.strip() == self.HANDSHAKE_ACK

    def encode_invocation(
        self,
        invocation_id: str | None,
        target: str,
        arguments: list[Any],
    ) -> bytes:
        return self.encode_raw_message([1, {}, invocation_id, target, arguments, []])

    def encode_raw_message(self, message: list[Any]) -> bytes:
        payload = msgpack.packb(message, use_bin_type=True)
        return self._write_varint(len(payload)) + payload

    def decode_binary_frames(self, data: bytes) -> list[Any]:
        messages, remainder = self.decode_binary_frames_partial(data)
        if remainder:
            raise ValueError("incomplete SignalR MessagePack frame")
        return messages

    def decode_binary_frames_partial(self, data: bytes) -> tuple[list[Any], bytes]:
        messages: list[Any] = []
        offset = 0
        while offset < len(data):
            frame_start = offset
            try:
                length, offset = self._read_varint(data, offset)
            except ValueError as exc:
                if str(exc) == "incomplete SignalR frame length":
                    return messages, data[frame_start:]
                raise
            end = offset + length
            if end > len(data):
                return messages, data[frame_start:]
            messages.append(msgpack.unpackb(data[offset:end], raw=False, strict_map_key=False))
            offset = end
        return messages, b""

    @staticmethod
    def _write_varint(value: int) -> bytes:
        if value < 0:
            raise ValueError("varint cannot be negative")
        out = bytearray()
        remaining = int(value)
        while remaining >= 0x80:
            out.append((remaining & 0x7F) | 0x80)
            remaining >>= 7
        out.append(remaining)
        return bytes(out)

    @staticmethod
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
            if shift > 35:
                raise ValueError("SignalR frame length varint is too large")
        raise ValueError("incomplete SignalR frame length")