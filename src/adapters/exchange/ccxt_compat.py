from __future__ import annotations

from typing import Any

from loguru import logger


def patch_ws_client_reset(client_cls: type[Any] | None = None) -> bool:
    if client_cls is None:
        from ccxt.async_support.base.ws.client import Client as client_cls

    if hasattr(client_cls, "reset"):
        return False
    if not callable(getattr(client_cls, "reject", None)):
        return False

    def reset(self: Any, error: Exception) -> Any:
        return self.reject(error)

    setattr(client_cls, "reset", reset)
    return True


def apply_ccxt_compat_patches() -> None:
    if patch_ws_client_reset():
        logger.info("Applied CCXT websocket Client.reset compatibility patch")
