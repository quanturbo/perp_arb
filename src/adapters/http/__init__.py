"""Unified HTTP client (aiohttp-backed). One session per process."""
from src.adapters.http.client import HttpClient, make_ccxt_session

__all__ = ["HttpClient", "make_ccxt_session"]
