from __future__ import annotations

import enum


class TradeState(enum.Enum):
    IDLE = "idle"
    OPENING = "opening"
    OPEN = "open"
    CLOSING = "closing"
    EXHAUSTED = "exhausted"
