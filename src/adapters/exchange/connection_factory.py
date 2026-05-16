from __future__ import annotations

from typing import TYPE_CHECKING

from src.adapters.exchange.connection import ExchangeConnection
from src.adapters.exchange.symbol_quarantine import SymbolQuarantine
from src.adapters.http import HttpClient
from src.settings import DEFAULT_LIMIT_PRICE_SLIPPAGE_PCT

if TYPE_CHECKING:
    from src.config import ExchangeConfig

class BitgetConnection(ExchangeConnection):
    def _get_time_in_force(self) -> str:
        return "FOK"  # Bitget Fill-Or-Kill (strictly no partials)

class GateioConnection(ExchangeConnection):
    def _get_time_in_force(self) -> str:
        # Gateio supports 'fok' (lowercase usually passed dynamically through CCXT unification)
        # but 'FOK' translates correctly in ccxt gateio mappings.
        return "FOK"

class MexcConnection(ExchangeConnection):
    def _get_time_in_force(self) -> str:
        # MEXC commonly only officially lists 'IOC' for basic V3 limit endpoints,
        # so keeping IOC fallback just in case FOK is rejected.
        return "IOC"

def create_exchange_connection(
    config: "ExchangeConfig",
    ws_timeout_sec: float = 10.0,
    ws_max_backoff_sec: float = 60.0,
    market_load_retries: int = 3,
    limit_price_slippage_pct: float = DEFAULT_LIMIT_PRICE_SLIPPAGE_PCT,
    http: HttpClient | None = None,
    symbol_quarantine: SymbolQuarantine | None = None,
) -> ExchangeConnection:
    kwargs = dict(
        ws_timeout_sec=ws_timeout_sec,
        ws_max_backoff_sec=ws_max_backoff_sec,
        market_load_retries=market_load_retries,
        limit_price_slippage_pct=limit_price_slippage_pct,
        http=http,
        symbol_quarantine=symbol_quarantine,
    )
    if config.id == "bitget":
        return BitgetConnection(config, **kwargs)
    if config.id == "gateio":
        return GateioConnection(config, **kwargs)
    if config.id == "mexc":
        return MexcConnection(config, **kwargs)
    return ExchangeConnection(config, **kwargs)
