"""Configuration parser for the no-browser UACryptoInvest scanner."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .catalog import canonical_exchange, chart_exchange_name, exchange_id_for


DEFAULT_FUNDING_INTERVAL_H = 8
DEFAULT_BASE_URL = "https://uacryptoinvest.com"


@dataclass(frozen=True)
class UACryptoInvestPair:
    token: str
    long_exchange: str
    short_exchange: str
    long_interval_h: int = DEFAULT_FUNDING_INTERVAL_H
    short_interval_h: int = DEFAULT_FUNDING_INTERVAL_H
    base_url: str = DEFAULT_BASE_URL

    def __post_init__(self) -> None:
        object.__setattr__(self, "token", self.token.strip().upper().removesuffix("USDT"))
        object.__setattr__(self, "long_exchange", canonical_exchange(self.long_exchange))
        object.__setattr__(self, "short_exchange", canonical_exchange(self.short_exchange))
        object.__setattr__(self, "long_interval_h", max(1, int(self.long_interval_h)))
        object.__setattr__(self, "short_interval_h", max(1, int(self.short_interval_h)))
        if not self.token:
            raise ValueError("UACryptoInvest token cannot be empty")

    @property
    def symbol(self) -> str:
        return f"{self.token}USDT"

    @property
    def key(self) -> str:
        return f"{self.token}:{self.long_exchange}:{self.short_exchange}"

    @property
    def long_exchange_id(self) -> int:
        return exchange_id_for(self.long_exchange)

    @property
    def short_exchange_id(self) -> int:
        return exchange_id_for(self.short_exchange)

    @property
    def chart_code(self) -> str:
        long_name = chart_exchange_name(self.long_exchange)
        short_name = chart_exchange_name(self.short_exchange)
        return f"{self.token}-{long_name}-Futures-{short_name}-Futures"

    @property
    def chart_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/charts?charts={self.chart_code}"


def _parse_interval(value: str | None) -> int:
    if value is None or value == "":
        return DEFAULT_FUNDING_INTERVAL_H
    return max(1, int(float(value)))


def _parse_entry(raw: str, *, base_url: str) -> UACryptoInvestPair:
    text = raw.strip()
    if not text:
        raise ValueError("empty UACryptoInvest pair")

    compact = text.split(":")
    if len(compact) in {3, 5}:
        return UACryptoInvestPair(
            token=compact[0],
            long_exchange=compact[1],
            short_exchange=compact[2],
            long_interval_h=_parse_interval(compact[3] if len(compact) == 5 else None),
            short_interval_h=_parse_interval(compact[4] if len(compact) == 5 else None),
            base_url=base_url,
        )

    parts = text.split("-")
    if len(parts) == 5 and parts[2].lower() == "futures" and parts[4].lower() == "futures":
        return UACryptoInvestPair(
            token=parts[0],
            long_exchange=parts[1],
            short_exchange=parts[3],
            base_url=base_url,
        )

    raise ValueError(
        "UACRYPTOINVEST_PAIRS entries must be TOKEN:LONG:SHORT, "
        "TOKEN:LONG:SHORT:LONG_INTERVAL:SHORT_INTERVAL, or chart code form"
    )


def parse_pairs(raw: str | None, *, base_url: str = DEFAULT_BASE_URL) -> list[UACryptoInvestPair]:
    if not raw:
        return []
    pairs: list[UACryptoInvestPair] = []
    for entry in raw.replace(";", ",").split(","):
        if entry.strip():
            pairs.append(_parse_entry(entry, base_url=base_url))
    return pairs


def pairs_from_env(env: Mapping[str, str]) -> list[UACryptoInvestPair]:
    base_url = env.get("UACRYPTOINVEST_BASE_URL") or DEFAULT_BASE_URL
    return parse_pairs(env.get("UACRYPTOINVEST_PAIRS"), base_url=base_url)