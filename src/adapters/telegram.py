"""Telegram notification adapter — throttled, fire-and-forget trade alerts.

Responsibilities (SRP):
  * send a message to Telegram, swallowing network errors (fire-and-forget)
  * dedup identical alert keys within a cooldown window (anti-spam)
  * provide a loguru sink that forwards ERROR-level logs through the same
    dedup filter, so ANY new error site gets a TG alert without us having
    to thread `notifier` everywhere (OCP: open for new error sites, closed
    for modification)

Design notes (applying /class):
  * Anti-spam is kept as a private helper on this class, not a separate
    ThrottledNotifier decorator — the scope is small (~20 LoC of state) and
    the two concerns are tightly coupled. YAGNI on extra layers.
  * The loguru sink has its own noise filter for pre-existing infra errors
    (IP-whitelist, margin-mode warnings) that the operator already knows
    about and doesn't want pinged for.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re

from loguru import logger

from src.adapters.http import HttpClient


# Substrings that identify pre-existing infra warnings we do NOT want on TG.
# Matched case-insensitively against the loguru record message.
_NOISE_PATTERNS: tuple[str, ...] = (
    "ip white",
    "ip not in",
    "not in the ip",
    "whitelist",
    "unmatched ip",
    "margin type",
    "no need to change",
    "cannot set margin mode",
    "insufficient_available",
    "no valid symbols for",
    "connection closed by remote server, closing code 1006",
    "client.receive_loop.<locals>.after_interrupt",
    "asyncio.exceptions.cancellederror",
    "coroutine ignored generatorexit",
    "slow request: get",  # dashboard latency warnings
    # aiohttp scanner-bot noise: HTTP/2 preface, malformed requests, random
    # scanners hitting the dashboard port. These are NOT bot errors.
    "error handling request from",
    "badhttpmessage",
    "pri * http",
    "pause on pri/upgrade",
    "bad status line",
    "invalid method encountered",
    "400, message",
)


class TelegramNotifier:
    """Sends rate-limited trade notifications to a Telegram chat."""

    # Default cooldown keys
    _OPEN_COOLDOWN_SEC = 30.0   # consecutive trades on same symbol usually legit
    _CLOSE_COOLDOWN_SEC = 30.0
    _ERROR_COOLDOWN_SEC = 300.0  # 5 min — prevents flood on repeated failures
    _LOG_SINK_COOLDOWN_SEC = 600.0  # 10 min — logs can be very repetitive

    def __init__(self, bot_token: str, chat_id: str, http: HttpClient | None = None):
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._base_url = f"https://api.telegram.org/bot{bot_token}"
        self._enabled = bool(bot_token and chat_id)
        # Optional injected shared HttpClient. If None, we lazily create our
        # own and own its lifecycle (so tests / standalone usage still work).
        self._http = http
        self._owns_http = http is None
        # key -> last-sent-unix-seconds. Bounded growth capped on access.
        self._last_sent: dict[str, float] = {}
        self._now = __import__("time").time  # overridable for tests

    @staticmethod
    def _fire_and_forget(coro) -> None:
        """Schedule a coroutine on the running loop, swallowing RuntimeError."""
        try:
            asyncio.ensure_future(coro)
        except RuntimeError:
            pass  # no running event loop (shutdown / thread context)

    # ── core send ───────────────────────────────────────────────────────

    async def send(self, message: str) -> None:
        """Send message to Telegram. Never raises — logs errors instead."""
        if not self._enabled:
            return
        try:
            if self._http is None:
                self._http = HttpClient(default_timeout_sec=10.0)
            status, body = await self._http.request_text(
                "POST",
                f"{self._base_url}/sendMessage",
                json={
                    "chat_id": self._chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=10.0,
            )
            if status != 200:
                logger.warning("Telegram send failed ({}): {}", status, body)
        except Exception as e:
            logger.warning("Telegram send error: {}", e)

    async def aclose(self) -> None:
        """Close the owned HttpClient (no-op if one was injected)."""
        if self._owns_http and self._http is not None:
            await self._http.aclose()
            self._http = None

    # ── dedup / throttle ────────────────────────────────────────────────

    def _should_emit(self, key: str, cooldown_sec: float) -> bool:
        """Return True and record the emit time, else False. Not thread-safe
        (single-asyncio-loop usage is assumed)."""
        now = self._now()
        last = self._last_sent.get(key, 0.0)
        if (now - last) < cooldown_sec:
            return False
        self._last_sent[key] = now
        # Bound memory: periodically prune keys older than 24h
        if len(self._last_sent) > 256:
            cutoff = now - 86400
            self._last_sent = {
                k: t for k, t in self._last_sent.items() if t >= cutoff
            }
        return True

    async def send_throttled(
        self, key: str, message: str, cooldown_sec: float = 300.0,
    ) -> None:
        """Send `message` only if `key` has not fired within `cooldown_sec`."""
        if not self._enabled:
            return
        if not self._should_emit(key, cooldown_sec):
            return
        await self.send(message)

    # ── typed helpers (public API preserved) ────────────────────────────

    def notify_trade_opened(
        self, symbol: str, exchange_long: str, exchange_short: str,
        spread_pct: float, amount_usdt: float, latency_ms: float,
    ) -> None:
        msg = _format_operator_notification(
            level="TRADE",
            type_="TRADE_OPENED",
            exchange=f"{exchange_long}/{exchange_short}",
            symbol=symbol,
            reason=(
                f"long {exchange_long}; short {exchange_short}; "
                f"spread {spread_pct:.2f}%; amount ${amount_usdt:.2f}; "
                f"latency {latency_ms:.0f}ms"
            ),
        )
        key = f"trade_open:{symbol}"
        self._fire_and_forget(
            self.send_throttled(key, msg, self._OPEN_COOLDOWN_SEC)
        )

    def notify_trade_closed(
        self, symbol: str, exchange_long: str, exchange_short: str,
        entry_spread: float, close_spread: float, latency_ms: float,
        trade_num: str = "",
    ) -> None:
        reason = (
            f"long {exchange_long}; short {exchange_short}; "
            f"entry {entry_spread:.2f}%; close {close_spread:.2f}%; "
            f"latency {latency_ms:.0f}ms"
        )
        if trade_num:
            reason += f"; trade #{trade_num}"
        msg = _format_operator_notification(
            level="TRADE",
            type_="TRADE_CLOSED",
            exchange=f"{exchange_long}/{exchange_short}",
            symbol=symbol,
            reason=reason,
        )
        key = f"trade_close:{symbol}"
        self._fire_and_forget(
            self.send_throttled(key, msg, self._CLOSE_COOLDOWN_SEC)
        )

    def _notify_trade_event(
        self,
        *,
        title: str,
        key_prefix: str,
        symbol: str,
        detail_label: str,
        detail: str,
        cooldown_sec: float,
        action: str = "",
    ) -> None:
        msg = _format_operator_notification(
            level=_notification_level_from_title(title),
            type_=_notification_type_from_title(title, action),
            exchange="Bot",
            symbol=symbol,
            reason=f"{detail_label}: {detail}",
        )
        parts = [key_prefix, symbol]
        if action:
            parts.append(action)
        parts.append(detail[:60])
        key = ":".join(parts)
        self._fire_and_forget(self.send_throttled(key, msg, cooldown_sec))

    def notify_trade_filtered(self, symbol: str, details: str) -> None:
        self._notify_trade_event(
            title="\u2139\ufe0f <b>FILTER RULE: NOT TRADE</b>",
            key_prefix="trade_filter",
            symbol=symbol,
            detail_label="Details",
            detail=details,
            cooldown_sec=self._ERROR_COOLDOWN_SEC,
        )

    def notify_trade_handled_error(self, symbol: str, action: str, error: str) -> None:
        self._notify_trade_event(
            title=f"\u26a0\ufe0f <b>HANDLED TRADE {action} ISSUE</b>",
            key_prefix="trade_handled",
            symbol=symbol,
            detail_label="Error",
            detail=error,
            cooldown_sec=self._ERROR_COOLDOWN_SEC,
            action=action,
        )

    def notify_trade_critical_error(self, symbol: str, action: str, error: str) -> None:
        self._notify_trade_event(
            title=f"\U0001f525 <b>CRITICAL UNHANDLED TRADE {action} ERROR</b>",
            key_prefix="trade_critical",
            symbol=symbol,
            detail_label="Error",
            detail=error,
            cooldown_sec=self._ERROR_COOLDOWN_SEC,
            action=action,
        )

    def notify_trade_error(self, symbol: str, action: str, error: str) -> None:
        """Backward-compatible alias for critical trade failures."""
        self.notify_trade_critical_error(symbol, action, error)

    # ── loguru sink (OCP: any future error site auto-notifies) ──────────

    def _build_loguru_sink(self, cooldown_sec: float | None = None):
        """Return a callable suitable for `logger.add(sink, level='ERROR')`.

        The sink:
          * filters out known-noise messages (IP whitelist, margin-mode...)
          * dedups on a hash of the normalized message body
          * formats a compact TG message
        """
        cooldown = cooldown_sec if cooldown_sec is not None else self._LOG_SINK_COOLDOWN_SEC

        def parse_structured_issue(text: str) -> tuple[str, dict[str, str]] | None:
            if text.startswith("TRADE ISSUE |"):
                issue_kind = "TRADE"
            elif text.startswith("EXCHANGE ISSUE |"):
                issue_kind = "EXCHANGE"
            elif text.startswith("BOT ISSUE |"):
                issue_kind = "BOT"
            else:
                return None
            fields: dict[str, str] = {}
            for part in text.split("|")[1:]:
                part = part.strip()
                if "=" not in part:
                    continue
                key, value = part.split("=", 1)
                fields[key.strip()] = value.strip()
            return issue_kind, fields

        def issue_key(issue_kind: str, fields: dict[str, str]) -> str:
            stable = "|".join(
                f"{name}={fields.get(name, '')}"
                for name in ("type", "exchange", "symbol", "phase", "side", "reason")
            )
            digest = hashlib.sha1(f"{issue_kind}|{stable}".encode("utf-8", "replace")).hexdigest()[:16]
            return "log:issue:" + digest

        def exchange_symbol_title(exchange: str, symbol: str) -> str:
            ex = exchange.strip() or "Exchange"
            ex_title = ex[:1].upper() + ex[1:]
            token = symbol.split("/", 1)[0].split(":", 1)[0].strip()
            return f"{ex_title} ${token}" if token else ex_title

        def short_message(value: str) -> str:
            return value.split(",", 1)[0].strip() or value

        def parse_extra(raw: str) -> dict:
            if not raw:
                return {}
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}

        def terminal_reason(issue_type: str) -> str:
            low_type = issue_type.lower()
            if "funding" in low_type:
                return "funding polling stopped for this exchange/symbol"
            if "volume" in low_type:
                return "volume polling stopped for this exchange/symbol"
            return "polling stopped for this exchange/symbol"

        def exchange_reason(fields: dict[str, str]) -> str:
            issue_type = fields.get("type", "")
            extra = parse_extra(fields.get("extra", ""))
            reason = short_message(str(extra.get("msg") or fields.get("reason") or "-"))
            parts = [reason]
            if extra.get("terminal") is True:
                parts.append(terminal_reason(issue_type))
            return "; ".join(part for part in parts if part)

        def _sink(record_str: str) -> None:
            # Loguru passes a fully-formatted string here (format={message} or similar).
            text = str(record_str).strip()
            low = text.lower()
            for pat in _NOISE_PATTERNS:
                if pat in low:
                    return
            # Normalize: drop leading ISO-like timestamp + level tokens so
            # two identical errors minutes apart dedup correctly.
            normalized = re.sub(r"^\d{2}:\d{2}:\d{2}.*?(ERROR|WARNING|CRITICAL)\s*", "", text)
            normalized = re.sub(r"\s+", " ", normalized)
            structured_issue = parse_structured_issue(normalized)
            if structured_issue:
                issue_kind, fields = structured_issue
                key = issue_key(issue_kind, fields)
                if issue_kind == "EXCHANGE":
                    msg = _format_operator_notification(
                        level=fields.get("level", "ERROR"),
                        type_=fields.get("type", "UNKNOWN"),
                        exchange=fields.get("exchange", ""),
                        symbol=fields.get("symbol", ""),
                        reason=exchange_reason(fields),
                    )
                    self._fire_and_forget(self.send_throttled(key, msg, cooldown))
                    return
                issue_level = fields.get("level", "ERROR")
                issue_type = fields.get("type", "UNKNOWN")
                reason = fields.get("reason", "-")
                if fields.get("phase"):
                    reason = f"phase={fields.get('phase')}; {reason}"
                if fields.get("side"):
                    reason = f"side={fields.get('side')}; {reason}"
                msg = _format_operator_notification(
                    level=issue_level,
                    type_=issue_type,
                    exchange=fields.get("exchange", "Bot"),
                    symbol=fields.get("symbol", ""),
                    reason=reason,
                )
                self._fire_and_forget(self.send_throttled(key, msg, cooldown))
                return
            generic_normalized = normalized[:200]
            key = "log:" + hashlib.sha1(generic_normalized.encode("utf-8", "replace")).hexdigest()[:16]
            body = _compact_log_reason(text)
            if "CRITICAL UNHANDLED" in text:
                level = "CRITICAL"
                type_ = "UNHANDLED_ERROR"
            elif "FILTER RULE:" in text:
                level = "WARNING"
                type_ = "FILTER_RULE"
            elif "HANDLED" in text:
                level = "WARNING"
                type_ = "HANDLED_ERROR"
            else:
                level = "ERROR"
                type_ = "BOT_ERROR"
            msg = _format_operator_notification(
                level=level,
                type_=type_,
                exchange="Bot",
                symbol="",
                reason=body,
            )
            self._fire_and_forget(self.send_throttled(key, msg, cooldown))

        return _sink

    def enable_loguru_error_sink(
        self, min_level: str = "ERROR", cooldown_sec: float | None = None,
    ) -> int:
        """Register a loguru sink that forwards filtered errors to TG.

        Returns the sink handler id (for removal in tests or teardown).
        Safe to call multiple times (no-op if notifier disabled).
        """
        if not self._enabled:
            return -1
        sink = self._build_loguru_sink(cooldown_sec)
        # format="{message}" keeps the sink input minimal and stable.
        return logger.add(sink, level=min_level, format="{message}", enqueue=False)


def _html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )


def _token_from_symbol(symbol: str) -> str:
    symbol = (symbol or "").strip()
    if not symbol:
        return "-"
    return symbol.split("/", 1)[0].split(":", 1)[0] or symbol


def _notification_icon(level: str) -> str:
    normalized = (level or "INFO").upper()
    if normalized == "TRADE":
        return "✅"
    if normalized == "CRITICAL":
        return "🔥"
    if normalized == "INFO":
        return "ℹ️"
    return "⚠️"


def _compact_log_reason(text: str, limit: int = 900) -> str:
    """Keep Telegram log alerts readable by summarizing traceback dumps."""
    clean = text.strip()
    if not clean:
        return "-"

    lines = [line.strip() for line in clean.splitlines() if line.strip()]
    if not lines:
        return clean[:limit]

    headline = lines[0]
    final_error = ""
    for line in reversed(lines):
        if re.match(r"^[A-Za-z_][\w.]*Error\s*:", line) or re.match(
            r"^[A-Za-z_][\w.]*Exception\s*:", line
        ):
            final_error = line
            break

    if final_error and final_error != headline:
        clean = f"{headline}; {final_error}"
    else:
        clean = re.sub(r"\s+", " ", clean)

    return clean if len(clean) <= limit else clean[:limit].rstrip() + "..."


def _format_operator_notification(
    *,
    level: str,
    type_: str,
    exchange: str,
    symbol: str,
    reason: str,
    reason_html: bool = False,
) -> str:
    normalized_level = (level or "INFO").upper()
    exchange_value = (exchange or "").strip() or "Bot"
    reason_value = (reason or "-").strip() or "-"
    rendered_reason = reason_value if reason_html else _html_escape(reason_value)
    return (
        f"{_notification_icon(normalized_level)} <b>{_html_escape(normalized_level)}</b>\n"
        f"Exchange: {_html_escape(exchange_value)}\n"
        f"Token: {_html_escape(_token_from_symbol(symbol))}\n"
        f"Type: {_html_escape(type_ or 'UNKNOWN')}\n"
        f"Reason: {rendered_reason}"
    )


def format_operator_notification(
    *,
    level: str,
    type_: str,
    exchange: str,
    symbol: str,
    reason: str,
    reason_html: bool = False,
) -> str:
    return _format_operator_notification(
        level=level,
        type_=type_,
        exchange=exchange,
        symbol=symbol,
        reason=reason,
        reason_html=reason_html,
    )


def _notification_level_from_title(title: str) -> str:
    if "CRITICAL" in title:
        return "CRITICAL"
    if "FILTER" in title:
        return "WARNING"
    if "TRADE" in title:
        return "TRADE"
    return "ERROR"


def _notification_type_from_title(title: str, action: str = "") -> str:
    suffix = f"_{action}" if action else ""
    if "CRITICAL" in title:
        return f"CRITICAL_TRADE{suffix}_ERROR"
    if "HANDLED" in title:
        return f"HANDLED_TRADE{suffix}_ISSUE"
    if "FILTER" in title:
        return "TRADE_FILTERED"
    return f"TRADE{suffix}_ISSUE"
