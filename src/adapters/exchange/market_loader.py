"""Exchange creation + market loading with retry and public fallback."""

from __future__ import annotations

import asyncio

import ccxt.pro as ccxtpro
from loguru import logger

from src.adapters.exchange.ccxt_compat import apply_ccxt_compat_patches
from src.adapters.http import make_ccxt_session
from src.config import ExchangeConfig


_NON_TRADING_STATUSES = {
    "BREAK",
    "CLOSED",
    "DELISTED",
    "DELIVERED",
    "DELIVERING",
    "EXPIRED",
    "HALT",
    "HALTED",
    "OFFLINE",
    "PENDING_TRADING",
    "PRE_DELIVERING",
    "SETTLING",
    "STOPPED",
    "SUSPENDED",
}

apply_ccxt_compat_patches()


def patch_exchange_ipv4(exchange: ccxtpro.Exchange) -> None:
    """Patch fetch() to ensure IPv4 session before every REST call.

    CCXT creates sessions lazily in fetch() — we intercept to force IPv4.
    This survives session recreation (unlike setting session once at init).
    Connector is configured with explicit HTTP keep-alive so warm TLS is
    reused for back-to-back REST calls (matters most on REST-only
    exchanges: bingx, bitget, mexc, htx, kucoinfutures).
    """
    original_fetch = exchange.fetch

    async def patched_fetch(*args, **kwargs):
        if exchange.session is None or exchange.session.closed:
            exchange.session = make_ccxt_session()
        return await original_fetch(*args, **kwargs)

    exchange.fetch = patched_fetch


def create_exchange(config: ExchangeConfig) -> ccxtpro.Exchange:
    """Create a CCXT Pro exchange instance from config."""
    cls = getattr(ccxtpro, config.id, None)
    if cls is None:
        raise ValueError(f"Unknown exchange: {config.id}")
    params: dict = {"enableRateLimit": True}
    if config.api_key:
        params["apiKey"] = config.api_key
    if config.secret:
        params["secret"] = config.secret
    if config.password:
        params["password"] = config.password
    # Remove internal adapter options before passing to CCXT.
    internal_extra = {"force_ipv4", "ws_timeout_sec"}
    extra = {k: v for k, v in config.extra.items() if k not in internal_extra}
    params.update(extra)
    exchange = cls(params)
    if config.sandbox:
        exchange.set_sandbox_mode(True)
    if config.extra.get("force_ipv4"):
        patch_exchange_ipv4(exchange)
    return exchange



def _market_status(market: dict) -> str:
    info = market.get("info") or {}
    value = (
        market.get("status")
        or info.get("status")
        or info.get("contractStatus")
        or info.get("state")
    )
    return str(value).strip().upper() if value is not None else ""


def _market_is_usable(market: dict | None) -> tuple[bool, str]:
    if not market:
        return False, "market metadata missing"
    if market.get("active") is False:
        return False, "market inactive"
    status = _market_status(market)
    if status in _NON_TRADING_STATUSES:
        return False, f"market status {status}"
    return True, ""


def validate_loaded_symbols(exchange: ccxtpro.Exchange, symbols: list[str]) -> list[str]:
    """Validate symbols against markets that are already loaded on exchange."""
    available = set(getattr(exchange, "symbols", None) or [])
    markets = getattr(exchange, "markets", None) or {}
    valid: list[str] = []
    for sym in symbols:
        usable, _reason = _market_is_usable(markets.get(sym))
        if sym in available and usable:
            valid.append(sym)
    return valid


async def _load_markets_public(
    exchange: ccxtpro.Exchange,
    config: ExchangeConfig,
) -> bool:
    """Fallback: copy markets from an unauthenticated CCXT instance."""
    cls = getattr(ccxtpro, config.id, None)
    if cls is None:
        return False
    logger.warning(
        "Retrying load_markets for {} WITHOUT auth (public fallback)", config.id
    )
    public = cls({"enableRateLimit": True})
    if config.extra.get("force_ipv4"):
        patch_exchange_ipv4(public)
    try:
        await public.load_markets()
        exchange.markets = public.markets
        exchange.markets_by_id = public.markets_by_id
        exchange.symbols = public.symbols
        exchange.currencies = public.currencies
        logger.info(
            "{}: {} symbols loaded via public fallback (trading may fail — "
            "add server IP to API key whitelist for full access)",
            config.id,
            len(public.symbols),
        )
        return True
    except Exception as e:
        logger.error("Public load_markets also failed for {}: {}", config.id, e)
        return False
    finally:
        try:
            await public.close()
        except Exception:
            pass


async def load_and_validate(
    exchange: ccxtpro.Exchange,
    config: ExchangeConfig,
    symbols: list[str],
    retries: int = 3,
) -> list[str]:
    """Load markets with retry + public fallback, return valid symbols."""
    logger.info("Loading markets for {} ...", config.id)
    loaded = False

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            await exchange.load_markets()
            loaded = True
            break
        except Exception as e:
            last_error = e
            # Log attempts as WARNING — they're noisy and recoverable via the
            # public-fallback path below. Only final, unrecoverable failures
            # (neither auth nor public works) should escalate to ERROR and
            # reach the Telegram sink.
            logger.warning(
                "load_markets {} attempt {}/{} failed: {}",
                config.id, attempt, retries, e,
            )
            if attempt < retries:
                wait = attempt * 5
                logger.info("Retrying {} in {}s ...", config.id, wait)
                await asyncio.sleep(wait)
                try:
                    await exchange.close()
                except Exception:
                    pass
                # Re-set IPv4 session after close destroyed it
                if config.extra.get("force_ipv4"):
                    patch_exchange_ipv4(exchange)

    if not loaded:
        loaded = await _load_markets_public(exchange, config)

    if not loaded:
        # Truly unrecoverable — escalate once, with the auth failure context.
        logger.error(
            "load_markets {} UNRECOVERABLE (auth + public both failed). Last auth error: {}",
            config.id, last_error,
        )
        return []

    available = set(exchange.symbols)
    markets = getattr(exchange, "markets", None) or {}
    logger.info("{}: {} symbols loaded", config.id, len(available))

    valid: list[str] = []
    for sym in symbols:
        usable, reason = _market_is_usable(markets.get(sym))
        if sym in available and usable:
            valid.append(sym)
            logger.info("  ✓ {} found on {}", sym, config.id)
        else:
            detail = reason if sym in available else "not found"
            logger.warning("  ✗ {} NOT valid on {} ({})", sym, config.id, detail)
            base = sym.split("/")[0] if "/" in sym else sym
            matches = [s for s in available if base in s][:5]
            if matches:
                logger.info("    Similar on {}: {}", config.id, matches)
    return valid
