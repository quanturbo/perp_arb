"""Public factory: build the scanner service from env, return None when off."""

from __future__ import annotations

import os
from typing import Mapping, Protocol

from src.adapters.http import HttpClient
from src.adapters.runtime_store import RuntimeConfigStore

from .alerts import ScannerAlerter
from .filter_store import ScannerFilterStore
from .service import ScannerService
from .uainvest import UAInvestScanner
from .uainvest.client import UAInvestClient


class _Notifier(Protocol):
    async def send_throttled(
        self, key: str, message: str, cooldown_sec: float = ...,
    ) -> None: ...


def _bool_env(env: Mapping[str, str], key: str, default: bool = False) -> bool:
    raw = env.get(key)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def build_scanner_service(
    notifier: _Notifier,
    *,
    env: Mapping[str, str] | None = None,
    runtime_config_path: str = "runtime_config.json",
    http: HttpClient | None = None,
    symbol_quarantine=None,
) -> ScannerService | None:
    """Return a configured ``ScannerService`` or ``None`` if disabled.

    Env switches:
      * ``SCANNERS_ENABLED``     master switch (default off)
      * ``UAINVEST_ENABLED``     enable the UAInvest source (default on)
      * ``SCANNER_POLL_SEC``     poll interval (default 30)
      * ``UAINVEST_COOKIE``      cookie passed verbatim to UAInvest

    The global filter and notification settings are read from the
    ``"scanner"`` block of ``runtime_config.json``; the dashboard PUTs updates
    back to the same file. Telegram is still quiet by default because
    ``ScannerFilter.notify_telegram`` defaults to false.
    """
    env = env if env is not None else os.environ
    if not _bool_env(env, "SCANNERS_ENABLED"):
        return None

    sources = []
    if _bool_env(env, "UAINVEST_ENABLED", default=True):
        sources.append(UAInvestScanner(UAInvestClient(http=http)))

    if not sources:
        return None

    poll = float(env.get("SCANNER_POLL_SEC", "30") or 30)
    filter_store = ScannerFilterStore(RuntimeConfigStore(runtime_config_path))
    alerter = ScannerAlerter(notifier)
    return ScannerService(
        scanners=sources,
        filter_store=filter_store,
        alerter=alerter,
        poll_interval_sec=poll,
        symbol_quarantine=symbol_quarantine,
    )
