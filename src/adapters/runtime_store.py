"""Atomic load/save of runtime_config.json overrides.

Pure adapter — file I/O only, no business rules. Validation lives in
src/domain/runtime_config.py and is the controller's responsibility before
calling save().
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any

from loguru import logger


class RuntimeConfigStore:
    """Read/write a JSON file holding operator-edited config overrides.

    Atomic write via tempfile + os.replace. Tolerant load: malformed or
    missing file yields {} so the bot still starts on defaults.
    """

    def __init__(self, path: str) -> None:
        self._path = path

    @property
    def path(self) -> str:
        return self._path

    def load(self) -> dict[str, Any]:
        if not os.path.exists(self._path):
            return {}
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                # Use loads(str) — main.py monkey-patches json.loads to
                # orjson.loads which doesn't accept the kwargs json.load() passes.
                data = json.loads(f.read())
        except (OSError, json.JSONDecodeError) as e:
            logger.error(
                "Failed to load runtime config from {}: {} — using defaults",
                self._path, e,
            )
            return {}
        if not isinstance(data, dict):
            logger.error(
                "Runtime config at {} is not a JSON object — ignoring", self._path,
            )
            return {}
        return data

    def save(self, overrides: dict[str, Any]) -> None:
        directory = os.path.dirname(os.path.abspath(self._path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".runtime_config.", suffix=".tmp", dir=directory,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(overrides, f, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._path)
        except Exception:
            # Cleanup tmp on any failure to avoid orphaned files.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
