from __future__ import annotations

import ccxt.pro as ccxtpro

from src.adapters.http import make_ccxt_session


def patch_exchange_ipv4(exchange: ccxtpro.Exchange) -> None:
    """Patch fetch() to ensure an IPv4 keep-alive session before REST calls."""
    original_fetch = exchange.fetch

    async def patched_fetch(*args, **kwargs):
        if exchange.session is None or exchange.session.closed:
            exchange.session = make_ccxt_session()
        return await original_fetch(*args, **kwargs)

    exchange.fetch = patched_fetch
