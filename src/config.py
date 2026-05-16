from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


@dataclass
class ExchangeConfig:
    id: str  # ccxt exchange id: "binanceusdm", "aster", etc.
    api_key: str = ""
    secret: str = ""
    password: str = ""
    sandbox: bool = False
    extra: dict = field(default_factory=dict)


def _default_exchanges() -> list[ExchangeConfig]:
    """All exchanges to connect to (trade + watch)."""
    return [
        ExchangeConfig(
            id="binanceusdm",
            api_key=_env("BINANCE_API_KEY"),
            secret=_env("BINANCE_API_SECRET"),
            extra={"force_ipv4": True, "direct_order_ws": True},
        ),
        ExchangeConfig(
            id="aster",
            api_key=_env("ASTER_API_KEY"),
            secret=_env("ASTER_API_SECRET"),
            extra={"force_ipv4": True},
        ),
        ExchangeConfig(
            id="gateio",
            api_key=_env("GATEIO_API_KEY"),
            secret=_env("GATEIO_API_SECRET"),
        ),
        ExchangeConfig(
            id="bitget",
            api_key=_env("BITGET_API_KEY"),
            secret=_env("BITGET_API_SECRET"),
            password=_env("BITGET_PASSWORD"),
            extra={"force_ipv4": True},
        ),
        ExchangeConfig(
            id="bybit",
            api_key=_env("BYBIT_API_KEY"),
            secret=_env("BYBIT_API_SECRET"),
            extra={"direct_order_ws": True},
        ),
        ExchangeConfig(
            id="okx",
            api_key=_env("OKX_API_KEY"),
            secret=_env("OKX_API_SECRET"),
            password=_env("OKX_PASSWORD"),
            extra={"direct_order_ws": True},
        ),
        ExchangeConfig(
            id="mexc",
            api_key=_env("MEXC_API_KEY"),
            secret=_env("MEXC_API_SECRET"),
        ),
        ExchangeConfig(
            id="kucoinfutures",
            api_key=_env("KUCOIN_API_KEY"),
            secret=_env("KUCOIN_API_SECRET"),
            password=_env("KUCOIN_PASSWORD"),
        ),
        ExchangeConfig(
            id="bingx",
            api_key=_env("BINGX_API_KEY"),
            secret=_env("BINGX_API_SECRET"),
        ),
        ExchangeConfig(
            id="htx",
            api_key=_env("HTX_API_KEY"),
            secret=_env("HTX_API_SECRET"),
            extra={"ws_timeout_sec": 30.0},
        ),
    ]


def _default_exchange_ids() -> list[str]:
    return [
        "binanceusdm",
        "aster",
        "bybit",
        "okx",
        "bitget",
        "mexc",
        "bingx",
        "kucoinfutures",
        "htx",
        "gateio",
    ]


from src.settings import MAX_LIMIT_PRICE_SLIPPAGE_PCT, TradingSettings


def _sanitize_trading_overrides(values: dict) -> dict:
    sanitized = dict(values)
    if "limit_price_slippage_pct" in sanitized:
        try:
            raw_slippage = float(sanitized["limit_price_slippage_pct"])
        except (TypeError, ValueError):
            sanitized.pop("limit_price_slippage_pct", None)
        else:
            sanitized["limit_price_slippage_pct"] = min(
                MAX_LIMIT_PRICE_SLIPPAGE_PCT,
                max(0.0, raw_slippage),
            )
    return sanitized


# Per-symbol overrides MUST NOT carry execution-mode keys — those are
# global-only (one wallet, one execution path). Legacy dashboard builds
# may still post them; we strip silently so the global slippage cap and
# order-type policy can never be subverted on a per-symbol basis.
_PER_SYMBOL_FORBIDDEN_KEYS = (
    "order_type",
    "time_in_force",
    "limit_price_slippage_pct",
)


def _sanitize_per_symbol_override(values: dict) -> dict:
    """Strip global-only execution keys from a per-symbol override patch."""
    sanitized = dict(values)
    for k in _PER_SYMBOL_FORBIDDEN_KEYS:
        sanitized.pop(k, None)
    # Recurse into a nested ``trading`` block — the dashboard may post
    # ``{"trading": {...}}`` for clarity; same forbidden keys apply.
    trading = sanitized.get("trading")
    if isinstance(trading, dict):
        cleaned = {k: v for k, v in trading.items() if k not in _PER_SYMBOL_FORBIDDEN_KEYS}
        sanitized["trading"] = cleaned
    return sanitized


@dataclass
class AppConfig:
    # --- Exchanges ---
    exchanges: list[ExchangeConfig] = field(default_factory=_default_exchanges)

    # Exchanges allowed to place trades (open/close positions)
    trade_exchanges: list[str] = field(
        default_factory=lambda: [
            "bybit",
            "aster",
        ]
    )

    # Default exchanges to read for newly-created/reset symbol overrides.
    read_exchanges: list[str] = field(default_factory=_default_exchange_ids)

    # All supported exchanges (superset for UI and --exchanges selection)
    available_exchanges: list[str] = field(default_factory=_default_exchange_ids)

    # --- Symbols ---
    symbols: list[str] = field(
        default_factory=lambda: [
            "APE/USDT:USDT",
        ]
    )

    # --- Per-symbol overrides (operator-edited via dashboard) ---
    # Shape:
    #   {
    #     "BTC/USDT:USDT": {
    #       "trade_exchanges": ["bybit"],            # subset of connected; trader uses
    #                                                #   only these for entry on this symbol
    #       "read_exchanges":  ["bybit", "bitget"],  # subset of connected; spread engine
    #                                                #   considers only these legs
    #       "trading": {"entry_spread_pct": 1.5, "amount_usdt": 100, ...},
    #     },
    #     ...
    #   }
    # Missing/empty fields fall through to globals. Resolution lives in
    # `effective_for(symbol)` so the trader has a single read site.
    per_symbol: dict[str, dict] = field(default_factory=dict)

    # --- Storage ---
    db_path: str = "spreads.db"
    storage_interval_sec: float = 5.0  # min seconds between saves per symbol
    save_on_spread_improvement: bool = True  # also save when spread beats previous
    spread_retention_hours: float = 24.0  # auto-delete snapshots older than this
    funding_retention_days: float = 1.0  # auto-delete funding logs older than this
    cleanup_interval_sec: float = 3600.0  # run cleanup every N seconds
    # Hard cap on the SQLite file size. After time-based pruning, if the file
    # is still above this, oldest spread_snapshots are dropped in batches until
    # the file shrinks under the cap (then incremental_vacuum reclaims pages).
    # Set to 0 to disable the size cap entirely.
    db_max_size_mb: float = 1024.0

    # --- Funding ---
    funding_poll_interval_sec: float = 60.0  # how often to poll funding rates
    funding_interval_hours: float = 1.0  # default funding interval for spread calc

    # --- Liquidity / volume ---
    # 24h quote-currency volume polling cadence (per exchange × symbol).
    # Slow on purpose: 24h volume changes slowly and we don't want to spend
    # rate-limit budget. 5 min is enough granularity for the dashboard's
    # low-liquidity warning.
    volume_poll_interval_sec: float = 300.0
    # Default minimum 24h quote volume for a leg to count as "liquid enough"
    # to participate in the ★ BEST opportunity scoring. Per-symbol-per-exchange
    # overrides live in `per_symbol[symbol]["min_quote_volume_usd"]` and
    # take precedence. UI uses this to badge low-liquidity legs.
    min_quote_volume_usd: float = 5_000_000.0

    # --- Spread calc ---
    holding_period_hours: float = 1.0  # for real spread = price + funding over period

    # --- Web dashboard ---
    web_enabled: bool = True
    web_host: str = "0.0.0.0"
    web_port: int = 8080
    web_token: str = _env(
        "WEB_TOKEN", ""
    )  # if set, require ?token=<value> on all requests
    web_poll_state_ms: int = 1000  # JS poll interval for live state
    web_poll_history_ms: int = 10000  # JS poll interval for history
    history_limit: int = 200  # max rows returned in history API

    # Dashboard latency filtering (separate from trading latency)
    dashboard_max_latency_ms: float = 1000.0  # default max latency for chart filtering
    dashboard_exchange_latency_ms: dict[str, float] = field(
        default_factory=lambda: {
            "mexc": 3000.0,  # mexc is slow, needs more headroom
        }
    )

    # --- Logging ---
    log_level: str = "INFO"
    log_ticks: bool = False  # verbose: log every tick
    log_spread_change_pct: float = 0.1  # only log SPREAD when it changes by this much

    # --- Telegram ---
    tg_bot_token: str = field(default_factory=lambda: _env("TG_BOT_TOKEN", ""))
    tg_chat_id: str = field(default_factory=lambda: _env("TG_CHAT_ID", ""))

    # --- Trading ---
    trading: TradingSettings = field(default_factory=TradingSettings)

    # --- Runtime overrides (operator-edited via dashboard) ---
    runtime_config_path: str = "runtime_config.json"
    # --- Symbol quarantine (auto-skip persistently stalled symbols) ---
    symbol_quarantine_path: str = "symbol_quarantine.json"

    # --- Exchange connectivity ---
    ws_timeout_sec: float = 10.0  # reconnect if no tick for this long
    ws_max_backoff_sec: float = 60.0  # max delay between WS reconnect attempts
    market_load_retries: int = 3  # retries for load_markets on startup

    # ─── Runtime override application ───
    def apply_runtime_overrides(self, overrides: dict) -> None:
        """Overlay operator-edited overrides on top of defaults.

        Called once at startup after AppConfig is constructed and
        runtime_config.json has been loaded. Mutates self in place.

        Validation MUST have run beforehand (see domain/runtime_config.py).
        Unknown/invalid fields are silently skipped to keep startup robust.
        """
        if not isinstance(overrides, dict):
            return
        if isinstance(overrides.get("symbols"), list) and overrides["symbols"]:
            self.symbols = list(overrides["symbols"])
        if isinstance(overrides.get("trade_exchanges"), list):
            self.trade_exchanges = list(overrides["trade_exchanges"])
        if isinstance(overrides.get("read_exchanges"), list):
            self.read_exchanges = list(overrides["read_exchanges"])
        min_volume = overrides.get("min_quote_volume_usd")
        if isinstance(min_volume, (int, float)) and not isinstance(min_volume, bool):
            self.min_quote_volume_usd = float(min_volume)
        trading = overrides.get("trading")
        if isinstance(trading, dict):
            for key, val in _sanitize_trading_overrides(trading).items():
                if hasattr(self.trading, key):
                    setattr(self.trading, key, val)

        # Per-symbol overrides: stored as a dict keyed by canonical symbol.
        # We accept it verbatim from disk; validation has already run in the
        # controller path. On startup load, we just keep the dict around for
        # the trader to consult via `effective_for(symbol)`.
        per_sym = overrides.get("per_symbol")
        if isinstance(per_sym, dict):
            self.per_symbol = {
                str(k): _sanitize_per_symbol_override(v)
                for k, v in per_sym.items()
                if isinstance(v, dict)
            }

    # ─── Resolution helpers ───
    @property
    def connected_exchange_ids(self) -> list[str]:
        """IDs of exchanges configured for stream startup.

        This should normally match `available_exchanges`. Live read health
        belongs to exchange stats (`connected`, ticks/sec, errors), not a
        hidden runtime disable list.
        """
        return [e.id for e in self.exchanges]

    def symbol_overrides(self, symbol: str) -> dict:
        """Raw override dict for a symbol (or empty dict). Use `effective_for`
        to get the merged view; this returns just the override layer."""
        return dict(self.per_symbol.get(symbol, {}))

    def set_symbol_overrides(self, symbol: str, overrides: dict | None) -> None:
        """Replace (or clear) the per-symbol override block in memory.

        Persistence is the controller's responsibility; this only updates
        the in-memory snapshot the trader consults each tick.
        """
        if not overrides:
            self.per_symbol.pop(symbol, None)
        else:
            self.per_symbol[symbol] = dict(overrides)
