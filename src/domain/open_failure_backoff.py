from __future__ import annotations

from dataclasses import dataclass

from src.domain.models import SpreadSnapshot


@dataclass(frozen=True)
class OpenAttemptKey:
    symbol: str
    exchange_long: str
    exchange_short: str

    @classmethod
    def from_snapshot(cls, snapshot: SpreadSnapshot) -> "OpenAttemptKey":
        return cls(
            symbol=snapshot.symbol,
            exchange_long=snapshot.exchange_long,
            exchange_short=snapshot.exchange_short,
        )

    def touches_exchange(self, exchange_id: str) -> bool:
        return exchange_id in (self.exchange_long, self.exchange_short)


@dataclass
class OpenFailureState:
    count: int = 0
    last_failure_ts: float = 0.0


class OpenFailureBackoff:
    """Track open-attempt cooldowns and exchange-scoped blocks."""

    def __init__(self) -> None:
        self._states: dict[OpenAttemptKey, OpenFailureState] = {}
        self._blocked_exchanges: dict[tuple[str, str], str] = {}

    def cooldown(
        self,
        key: OpenAttemptKey,
        *,
        max_failures: int,
        cooldown_sec: float,
        now: float,
    ) -> tuple[bool, int, float]:
        state = self._states.get(key)
        if state is None or state.count < max_failures:
            return False, state.count if state else 0, 0.0
        wait_left = cooldown_sec - (now - state.last_failure_ts)
        if wait_left <= 0:
            self._states.pop(key, None)
            return False, 0, 0.0
        return True, state.count, wait_left

    def record_failure(self, key: OpenAttemptKey, *, now: float) -> OpenFailureState:
        state = self._states.setdefault(key, OpenFailureState())
        state.count += 1
        state.last_failure_ts = now
        return state

    def clear(self, key: OpenAttemptKey) -> None:
        self._states.pop(key, None)

    def block_exchange(self, symbol: str, exchange_id: str, reason: str) -> None:
        self._blocked_exchanges[(symbol, exchange_id)] = reason

    def is_exchange_blocked(self, symbol: str, exchange_id: str) -> str:
        return self._blocked_exchanges.get((symbol, exchange_id), "")

    def blocked_for_pair(
        self, symbol: str, exchange_long: str, exchange_short: str,
    ) -> tuple[str, str]:
        for exchange_id in (exchange_long, exchange_short):
            reason = self._blocked_exchanges.get((symbol, exchange_id), "")
            if reason:
                return exchange_id, reason
        return "", ""

    def blocked_exchanges(self) -> dict[str, dict[str, str]]:
        out: dict[str, dict[str, str]] = {}
        for (symbol, exchange_id), reason in self._blocked_exchanges.items():
            out.setdefault(symbol, {})[exchange_id] = reason
        return out

    def reset(self, exchange_id: str = "") -> None:
        if not exchange_id:
            self._states.clear()
            self._blocked_exchanges.clear()
            return
        for key in list(self._states):
            if key.touches_exchange(exchange_id):
                self._states.pop(key, None)
        for symbol_exchange in list(self._blocked_exchanges):
            if symbol_exchange[1] == exchange_id:
                self._blocked_exchanges.pop(symbol_exchange, None)

    def state_for(self, key: OpenAttemptKey | None) -> OpenFailureState:
        if key is None:
            return OpenFailureState()
        return self._states.get(key, OpenFailureState())
