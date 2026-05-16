from __future__ import annotations

from typing import Any, Protocol


class OrchestratorControlPort(Protocol):
    async def request_restart(self) -> None: ...

    async def replace_symbols(self, new_symbols: list[str]) -> dict[str, Any]: ...