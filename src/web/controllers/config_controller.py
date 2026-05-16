"""HTTP controller for live config edits.

Thin orchestration layer: parse → validate (domain) → persist (adapter)
→ apply (trader) OR signal restart (orchestrator). No domain logic here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aiohttp import web
from loguru import logger

from src.adapters.runtime_store import RuntimeConfigStore
from src.config import _sanitize_per_symbol_override
from src.domain.runtime_config import (
    merge_overrides,
    normalize_symbol,
    normalize_symbols,
    requires_restart,
    validate_overrides,
    validate_symbol_overrides,
)
from src.web.controllers.config_view import (
    build_effective_config_view,
    default_symbol_overrides,
    symbol_defaults_from_global,
)
from src.web.ports import OrchestratorControlPort

if TYPE_CHECKING:
    from src.config import AppConfig
    from src.domain.trader import ArbitrageTrader


class ConfigController:
    """Encapsulates POST /api/config and symbol add/remove sugar.

    Owns the runtime store (single I/O surface for overrides). The web
    middleware applies token auth before reaching us; we trust the request.
    """

    def __init__(
        self,
        config: "AppConfig",
        trader: "ArbitrageTrader",
        orchestrator: OrchestratorControlPort,
        config_dict: dict[str, Any],
    ) -> None:
        self._config = config
        self._trader = trader
        self._orchestrator = orchestrator
        self._config_dict = config_dict
        self._store = RuntimeConfigStore(config.runtime_config_path)

    # ─────── handlers ───────

    async def get(self, request: web.Request) -> web.Response:
        """Return the live effective config (defaults + applied overrides).

        ``?symbol=X`` returns the *effective* view for symbol X (global
        merged with that symbol's overrides). Without ``symbol``, returns
        the global view.
        """
        symbol_param = ""
        query = getattr(request, "query", None)
        if query is not None:
            try:
                symbol_param = (query.get("symbol", "") or "").strip()
            except (AttributeError, TypeError):
                symbol_param = ""
        if symbol_param:
            sym = normalize_symbol(symbol_param) or symbol_param
            return web.json_response(self._effective_view(symbol=sym))
        return web.json_response(self._effective_view())

    async def post(self, request: web.Request) -> web.Response:
        """Apply a partial config patch.

        Body shape:
          - Global patch:    {"read_exchanges": [...],
                              "trade_exchanges": [...], "trading": {...}}
          - Per-symbol:      {"symbol": "BTC/USDT:USDT",
                              "trade_exchanges": [...],
                              "read_exchanges":  [...],
                              "trading": {...}}

        Returns:
            200 + {applied:[...]}            — hot-reloaded, no restart
            202 + {restart:true, eta_sec:15} — restart-required, scheduled
            400 + {errors:[...]}             — validation failure
            409 + {error:"..."}              — position-safety guard
        """
        try:
            patch = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)
        try:
            # Per-symbol patch routing: caller may send `{"symbol": X, ...}`
            # in the body. We strip it from the patch and route via the
            # per-symbol code path.
            symbol = None
            if isinstance(patch, dict) and "symbol" in patch:
                raw_symbol = patch.pop("symbol")
                if raw_symbol:
                    symbol = normalize_symbol(raw_symbol)
                    if not symbol:
                        return web.json_response(
                            {"errors": [{"field": "symbol",
                                          "message": "invalid symbol"}]},
                            status=400,
                        )
            if symbol is not None:
                return await self._apply_symbol(symbol, patch)
            return await self._apply(patch)
        except Exception as e:  # noqa: BLE001 — surface for debugging
            logger.exception("Config apply failed: {}", e)
            return web.json_response(
                {"error": f"{type(e).__name__}: {e}"}, status=500,
            )

    async def add_symbol(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            body = {}
        symbol = normalize_symbol(body.get("symbol", ""))
        if not symbol:
            return web.json_response(
                {"error": "symbol required (e.g. 'APE' or 'APE/USDT:USDT')"},
                status=400,
            )
        current = list(self._config.symbols)
        if symbol in current:
            return web.json_response(
                {"error": f"symbol {symbol} already present"}, status=409,
            )
        return await self._apply({"symbols": current + [symbol]})

    async def remove_symbol(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            body = {}
        symbol = normalize_symbol(body.get("symbol", ""))
        if not symbol:
            return web.json_response(
                {"error": "symbol required"}, status=400,
            )
        current = list(self._config.symbols)
        if symbol not in current:
            return web.json_response(
                {"error": f"symbol {symbol} not present"}, status=404,
            )
        new_symbols = [s for s in current if s != symbol]
        if not new_symbols:
            return web.json_response(
                {"error": "cannot remove last symbol — bot needs at least one"}, status=409,
            )
        return await self._apply({"symbols": new_symbols})

    async def apply_defaults(self, request: web.Request) -> web.Response:
        """Copy current global defaults into one or all symbol overrides.

        The dashboard may include a ``defaults`` patch so pressing
        "Apply to all" uses the values currently visible in DEFAULT mode in
        the same request, instead of relying on a previous autosave round-trip.
        """
        try:
            body = await request.json()
        except Exception:
            body = {}

        scope = (body.get("scope") or "symbol").strip().lower()
        requested_symbol = None
        raw_symbol = body.get("symbol", "")
        if raw_symbol:
            requested_symbol = normalize_symbol(raw_symbol)
            if not requested_symbol:
                return web.json_response({"error": "invalid symbol"}, status=400)
            if requested_symbol not in self._config.symbols:
                return web.json_response(
                    {"error": f"symbol {requested_symbol} not currently tracked"},
                    status=404,
                )

        if scope == "all":
            symbols = list(self._config.symbols)
        else:
            if not requested_symbol:
                return web.json_response({"error": "symbol required"}, status=400)
            symbols = [requested_symbol]

        defaults_patch = body.get("defaults") or {}
        if defaults_patch and not isinstance(defaults_patch, dict):
            return web.json_response({"error": "defaults must be an object"}, status=400)
        unknown_defaults = set(defaults_patch) - {
            "read_exchanges",
            "trade_exchanges",
            "trading",
            "min_quote_volume_usd",
        }
        if unknown_defaults:
            return web.json_response(
                {"errors": [
                    {"field": key, "message": "unknown defaults field"}
                    for key in sorted(unknown_defaults)
                ]},
                status=400,
            )
        if defaults_patch:
            errors = validate_overrides(
                defaults_patch,
                available_exchanges=set(self._config.available_exchanges),
            )
            if errors:
                return web.json_response(
                    {"errors": [{"field": e.field, "message": e.message} for e in errors]},
                    status=400,
                )

        if self._trader.is_busy:
            return web.json_response(
                {"error": f"bot is {self._trader.state.value} — wait for IDLE/OPEN before editing"},
                status=409,
            )

        guard_err = self._safety_guard(defaults_patch)
        if guard_err:
            return web.json_response({"error": guard_err}, status=409)

        default_block = self._symbol_defaults_from_global(defaults_patch=defaults_patch)
        if self._trader.position and self._trader.position.symbol in symbols:
            pos = self._trader.position
            missing = {pos.exchange_long, pos.exchange_short} - set(default_block["trade_exchanges"])
            if missing:
                return web.json_response(
                    {"error": (
                        f"cannot apply defaults removing trade exchange(s) {sorted(missing)} "
                        f"while position is open on them"
                    )},
                    status=409,
                )

        existing = self._store.load()
        new_overrides = merge_overrides(existing, defaults_patch) if defaults_patch else dict(existing)
        all_per_symbol = dict(new_overrides.get("per_symbol") or {})
        blocks_by_symbol: dict[str, dict[str, Any]] = {}
        for symbol in symbols:
            block = self._symbol_defaults_from_global(defaults_patch=defaults_patch)
            all_per_symbol[symbol] = block
            blocks_by_symbol[symbol] = block

        new_overrides = {**new_overrides, "per_symbol": all_per_symbol}
        try:
            self._store.save(new_overrides)
        except OSError as e:
            logger.error("Failed to persist default application: {}", e)
            return web.json_response({"error": "persist failed"}, status=500)

        if "read_exchanges" in defaults_patch:
            self._config.read_exchanges = list(defaults_patch["read_exchanges"])
        if "trade_exchanges" in defaults_patch:
            self._config.trade_exchanges = list(defaults_patch["trade_exchanges"])
        if "min_quote_volume_usd" in defaults_patch:
            self._config.min_quote_volume_usd = float(defaults_patch["min_quote_volume_usd"])
        if "trading" in defaults_patch and isinstance(defaults_patch["trading"], dict):
            for key, val in defaults_patch["trading"].items():
                if hasattr(self._config.trading, key):
                    setattr(self._config.trading, key, val)

        changes = self._trader.update_settings(
            trading=defaults_patch.get("trading"),
            trade_exchanges=defaults_patch.get("trade_exchanges"),
        )
        for symbol, block in blocks_by_symbol.items():
            self._config.set_symbol_overrides(symbol, block)

        self._config_dict["trading_config"] = self._effective_view()
        symbol_result = await self._orchestrator.replace_symbols(
            list(self._config.symbols),
        )
        response: dict[str, Any] = {
            "applied": "defaults",
            "applied_symbols": symbols,
            "changes": {k: list(v) for k, v in changes.items()},
            "symbols_diff": symbol_result,
        }
        if requested_symbol:
            response["symbol"] = requested_symbol
            response["effective"] = self._effective_view(symbol=requested_symbol)
        return web.json_response(response, status=200)

    # ─────── core ───────

    async def _apply_symbol(self, symbol: str, patch: dict[str, Any]) -> web.Response:
        """Apply a per-symbol overrides patch.

        Merges into AppConfig.per_symbol[symbol] and persists under
        runtime_config.json's `per_symbol` block. The trader picks up
        new values on the next spread tick (no restart).
        """
        if symbol not in self._config.symbols:
            return web.json_response(
                {"error": f"symbol {symbol} not currently tracked"}, status=404,
            )

        # 1) Validate the per-symbol patch shape.
        #
        #   Validate against the *implemented* (`available_exchanges`)
        #   universe — not the currently-connected subset. The operator
        #   may legitimately persist preferences for an exchange that is
        #   temporarily disconnected (e.g. while rotating API keys); the
        #   trader filters disabled IDs out at runtime via `_resolve`.
        # Strip legacy execution-mode keys before validation/persist —
        # per-symbol overrides MUST NOT change order_type/time_in_force/
        # limit_price_slippage_pct (those are global-only). Old dashboard
        # builds may still post them; drop silently to preserve the
        # global slippage protection.
        patch = _sanitize_per_symbol_override(patch)

        available = set(self._config.available_exchanges)
        errors = validate_symbol_overrides(patch, connected_exchanges=available)
        if errors:
            return web.json_response(
                {"errors": [{"field": e.field, "message": e.message} for e in errors]},
                status=400,
            )

        # 2) Position-safety guard: cannot remove the exchange of an open
        #    position from a symbol's trade_exchanges (we'd be unable to close).
        if self._trader.is_busy:
            return web.json_response(
                {"error": f"bot is {self._trader.state.value} — wait for IDLE/OPEN before editing"},
                status=409,
            )
        if "trade_exchanges" in patch and self._trader.position:
            pos = self._trader.position
            if pos.symbol == symbol:
                missing = {pos.exchange_long, pos.exchange_short} - set(patch["trade_exchanges"])
                if missing:
                    return web.json_response(
                        {"error": (
                            f"cannot remove trade exchange(s) {sorted(missing)} "
                            f"while position is open on them"
                        )},
                        status=409,
                    )

        # 3) Merge into the existing per-symbol overrides (deep-merge for
        #    `trading`; shallow-replace for the lists). Keys are preserved
        #    even when the value is an empty list — explicit empty has
        #    real semantics for the operator ("trade nowhere" / "read
        #    nothing"), distinct from "no override at all".
        existing_block = dict(self._config.per_symbol.get(symbol, {}))
        merged_block = dict(existing_block)
        for key, val in patch.items():
            if key == "trading" and isinstance(val, dict) and isinstance(merged_block.get("trading"), dict):
                merged_block["trading"] = {**merged_block["trading"], **val}
            else:
                merged_block[key] = val
        # The incoming patch is sanitized above, but the existing persisted
        # symbol block may still contain legacy execution-mode fields. Strip
        # again after merge so a harmless edit cannot keep them alive.
        merged_block = _sanitize_per_symbol_override(merged_block)
        # Strip an empty trading sub-dict (no real overrides inside) so the
        # persisted JSON stays compact. Lists are preserved verbatim.
        if merged_block.get("trading") == {}:
            merged_block.pop("trading", None)

        # 4) Persist: store the entire per_symbol map under the top-level
        #    `per_symbol` key in runtime_config.json. Atomic via the store.
        existing = self._store.load()
        all_per_symbol = dict(existing.get("per_symbol") or {})
        if merged_block:
            all_per_symbol[symbol] = merged_block
        else:
            # Nothing left after merge — drop the entry entirely.
            all_per_symbol.pop(symbol, None)
        new_overrides = {**existing, "per_symbol": all_per_symbol}
        try:
            self._store.save(new_overrides)
        except OSError as e:
            logger.error("Failed to persist per-symbol config: {}", e)
            return web.json_response({"error": "persist failed"}, status=500)

        # 5) Apply in-memory: trader reads from the same dict reference.
        #    `set_symbol_overrides(symbol, None | {})` pops the entry so the
        #    in-memory state matches what we just persisted.
        self._config.set_symbol_overrides(
            symbol, merged_block if merged_block else None,
        )
        # Also keep the cached /api/state header view fresh.
        self._config_dict["trading_config"] = self._effective_view()

        symbol_result: dict[str, Any] | None = None
        if "read_exchanges" in patch:
            symbol_result = await self._orchestrator.replace_symbols(
                list(self._config.symbols),
            )

        logger.info("Per-symbol config applied for {}: {}", symbol, list(patch.keys()))
        response: dict[str, Any] = {
            "applied": list(patch.keys()),
            "symbol": symbol,
            "effective": self._effective_view(symbol=symbol),
        }
        if symbol_result is not None:
            response["symbols_diff"] = symbol_result
        return web.json_response(response, status=200)

    async def _apply(self, patch: dict[str, Any]) -> web.Response:
        # 0) normalize symbol input (accept 'APE' or 'APE/USDT:USDT')
        if "symbols" in patch:
            normalized = normalize_symbols(patch["symbols"])
            if normalized is None:
                return web.json_response(
                    {"errors": [{"field": "symbols",
                                  "message": "invalid entry; use 'APE' or 'APE/USDT:USDT'"}]},
                    status=400,
                )
            patch = {**patch, "symbols": normalized}

        # 1) domain validation
        # Validate against the full known exchange universe. Live read health
        # is shown in the dashboard; no hidden disabled-exchange override is
        # applied at startup.
        available = set(self._config.available_exchanges)
        errors = validate_overrides(patch, available_exchanges=available)
        if errors:
            return web.json_response(
                {"errors": [{"field": e.field, "message": e.message} for e in errors]},
                status=400,
            )

        # 2) position-safety guard
        guard_err = self._safety_guard(patch)
        if guard_err:
            return web.json_response({"error": guard_err}, status=409)

        # 3) persist (atomic)
        existing = self._store.load()
        merged = merge_overrides(existing, patch)
        added_symbols: list[str] = []
        if "symbols" in patch:
            added_symbols = [s for s in patch["symbols"] if s not in self._config.symbols]
            if added_symbols:
                per_symbol = dict(merged.get("per_symbol") or {})
                for sym in added_symbols:
                    per_symbol.setdefault(sym, self._default_symbol_overrides())
                merged["per_symbol"] = per_symbol
        try:
            self._store.save(merged)
        except OSError as e:
            logger.error("Failed to persist runtime config: {}", e)
            return web.json_response({"error": "persist failed"}, status=500)

        # 4a) restart-required path (currently empty — kept for future fields)
        if requires_restart(patch):
            logger.warning(
                "Config change requires restart: {}", list(patch.keys()),
            )
            await self._orchestrator.request_restart()
            return web.json_response(
                {"restart": True, "eta_sec": 15, "applied": list(patch.keys())},
                status=202,
            )

        # 4b) hot-reload path
        applied: dict[str, Any] = {}
        symbol_result: dict[str, Any] | None = None

        # Symbols: dispatch to orchestrator hot-replace (cycles WS subscriptions).
        if "symbols" in patch:
            symbol_result = await self._orchestrator.replace_symbols(patch["symbols"])
            applied["symbols"] = patch["symbols"]
            for sym in added_symbols:
                if sym not in self._config.per_symbol:
                    self._config.set_symbol_overrides(sym, self._default_symbol_overrides())

        if "trade_exchanges" in patch:
            self._config.trade_exchanges = list(patch["trade_exchanges"])
            applied["trade_exchanges"] = patch["trade_exchanges"]
        if "read_exchanges" in patch:
            self._config.read_exchanges = list(patch["read_exchanges"])
            applied["read_exchanges"] = patch["read_exchanges"]
        if "min_quote_volume_usd" in patch:
            self._config.min_quote_volume_usd = float(patch["min_quote_volume_usd"])
            applied["min_quote_volume_usd"] = self._config.min_quote_volume_usd
        if "trading" in patch and isinstance(patch["trading"], dict):
            # Mutate AppConfig.trading too so a subsequent restart preserves
            # the value (already on disk, this is just for in-memory parity).
            for k, v in patch["trading"].items():
                if hasattr(self._config.trading, k):
                    setattr(self._config.trading, k, v)
            applied["trading"] = patch["trading"]

        changes = self._trader.update_settings(
            trading=patch.get("trading"),
            trade_exchanges=patch.get("trade_exchanges"),
        )

        # 5) refresh the cached view used by /api/state -> dashboard header
        self._config_dict["trading_config"] = self._effective_view()

        logger.info("Config applied: {}", applied)
        response: dict[str, Any] = {
            "applied": applied,
            "changes": {k: list(v) for k, v in changes.items()},
        }
        if symbol_result is not None:
            response["symbols_diff"] = symbol_result
        return web.json_response(response, status=200)

    def _safety_guard(self, patch: dict[str, Any]) -> str | None:
        """Return error message if the patch would put the bot in unsafe state."""
        if self._trader.is_busy:
            return f"bot is {self._trader.state.value} — wait for IDLE/OPEN before editing"

        # Removing a symbol with an open position is forbidden.
        if "symbols" in patch:
            new_set = set(patch["symbols"])
            held = self._trader.held_symbols()
            removing = held - new_set
            if removing:
                return f"cannot remove symbol(s) {sorted(removing)} with open position"

        # If trade_exchanges change drops the exchange of an open position,
        # we'd be unable to close it cleanly. Reject.
        if "trade_exchanges" in patch and self._trader.position:
            new_ex = set(patch["trade_exchanges"])
            pos = self._trader.position
            missing = {pos.exchange_long, pos.exchange_short} - new_ex
            if missing:
                return (
                    f"cannot remove trade exchange(s) {sorted(missing)} "
                    f"while position is open on them"
                )

        return None

    def _effective_view(self, *, symbol: str | None = None) -> dict[str, Any]:
        return build_effective_config_view(self._config, symbol=symbol)

    def _default_symbol_overrides(self) -> dict[str, Any]:
        return default_symbol_overrides(self._config)

    def _symbol_defaults_from_global(
        self,
        *,
        defaults_patch: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return symbol_defaults_from_global(
            self._config,
            defaults_patch=defaults_patch,
        )
