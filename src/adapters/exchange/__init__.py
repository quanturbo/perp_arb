"""Exchange connectivity — connection lifecycle, streaming, orders."""

from src.adapters.exchange.connection import ExchangeConnection, OnFundingCallback, OnTickCallback
from src.adapters.exchange.connection_factory import create_exchange_connection
from src.adapters.exchange.stream_manager import ExchangeStreamManager

__all__ = [
    "create_exchange_connection",
    "ExchangeConnection",
    "ExchangeStreamManager",
    "OnFundingCallback",
    "OnTickCallback",
]
