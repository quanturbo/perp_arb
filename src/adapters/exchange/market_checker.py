from __future__ import annotations

from src.config import AppConfig


class MarketChecker:
    """Utility to check common symbols across configured exchanges."""

    def __init__(self, config: AppConfig):
        self._config = config

    async def run(self) -> None:
        import ccxt.pro as ccxtpro

        markets_per_exchange: dict[str, set[str]] = {}

        for cfg in self._config.exchanges:
            cls = getattr(ccxtpro, cfg.id, None)
            if not cls:
                print(f"Unknown exchange: {cfg.id}")
                continue
            exchange = cls()
            try:
                await exchange.load_markets()
                perps = {
                    symbol for symbol, market in exchange.markets.items()
                    if market.get("swap") or market.get("future")
                }
                markets_per_exchange[cfg.id] = perps
                print(f"{cfg.id}: {len(perps)} perp symbols loaded")
            finally:
                await exchange.close()

        if len(markets_per_exchange) < 2:
            print("Need at least 2 exchanges to find common symbols")
            return

        common = set.intersection(*markets_per_exchange.values())
        print(f"\nCommon perp symbols ({len(common)}):")
        for symbol in sorted(common):
            print(f"  {symbol}")