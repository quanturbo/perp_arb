"""Asyncio loop exception handling helpers."""

from __future__ import annotations

import asyncio
import errno
from collections.abc import Mapping
from typing import Any

from loguru import logger


def should_downgrade_loop_exception(context: Mapping[str, Any]) -> bool:
    """Return True for known transient socket cleanup errors.

    The exchange/http layers already log actionable request failures with
    exchange/symbol context. These loop-level callbacks are uvloop/aiohttp
    cleanup races after a remote peer resets a connecting TLS socket.
    """
    message = str(context.get("message") or "").lower()
    handle = str(context.get("handle") or "").lower()
    exception = context.get("exception")
    exception_text = str(exception).lower()

    if isinstance(exception, ConnectionResetError) and (
        "future exception was never retrieved" in message
        or "ssl" in handle
        or "handshake" in exception_text
    ):
        return True

    if isinstance(exception, RuntimeError):
        return (
            "sock_connect" in message
            or "sock_connect" in handle
        ) and "is used by transport" in exception_text

    if isinstance(exception, OSError) and exception.errno == errno.EBADF:
        return (
            "connection_made" in message
            or "connection_made" in handle
            or "bad file descriptor" in exception_text
        )

    return False


def install_loop_exception_handler(loop: asyncio.AbstractEventLoop) -> None:
    """Install a loop exception handler that keeps real errors loud."""
    default_handler = loop.default_exception_handler

    def _handler(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        if should_downgrade_loop_exception(context):
            exception = context.get("exception")
            logger.warning(
                "Transient asyncio socket cleanup ignored: {} ({})",
                context.get("message") or "loop callback",
                exception.__class__.__name__ if exception else "no exception",
            )
            return
        default_handler(context)

    loop.set_exception_handler(_handler)