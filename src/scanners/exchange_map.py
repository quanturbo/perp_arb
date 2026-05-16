"""Map third-party scanner exchange ids → local bot exchange ids.

The bot uses ccxt-style identifiers (``binanceusdm``, ``gateio``, ``htx``).
Scanner sources publish friendlier short names (``binance``, ``gate``,
``huobi``). When the watchlist user filters by "I only care about pairs
on bybit + gate", they're using the SOURCE name (since that's what they
see in the offers list). The matcher compares against the BOT name, so we
need a deterministic, single-place translation table.

Add new mappings here only. Unknown source names round-trip unchanged so
the UI still shows them; they simply won't equal any bot exchange id.
"""

from __future__ import annotations

# Lower-cased on both sides. Update when a new source/exchange shows up.
_SOURCE_TO_BOT: dict[str, str] = {
    "binance": "binanceusdm",
    "binance_futures": "binanceusdm",
    "binanceusdm": "binanceusdm",
    "bybit": "bybit",
    "bybit_futures": "bybit",
    "bybitfi": "bybit",
    "okx": "okx",
    "okex": "okx",
    "bitget": "bitget",
    "kucoin": "kucoinfutures",
    "kucoin_futures": "kucoinfutures",
    "kucoinfutures": "kucoinfutures",
    "gate": "gateio",
    "gateio": "gateio",
    "gate_futures": "gateio",
    "gatefi": "gateio",
    "mexc": "mexc",
    "huobi": "htx",
    "htx": "htx",
    "bingx": "bingx",
    "aster": "aster",
}


def map_source_to_bot(source_id: str) -> str | None:
    """Return the local bot exchange id for ``source_id``, or ``None``.

    Returning ``None`` (rather than the raw value) lets callers test
    tradability with a single ``is None`` check.
    """
    if not source_id:
        return None
    return _SOURCE_TO_BOT.get(source_id.strip().lower())
