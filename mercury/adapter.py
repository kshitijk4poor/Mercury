"""Inbound adapter primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from mercury.parse import parse_inbound_event


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass
class CLIAdapter:
    """Single-message CLI adapter that yields one canonical inbound event."""

    message: str
    session_id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    source: str = "cli"
    timestamp: str | None = None

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        yield parse_inbound_event(
            {
                "source": self.source,
                "session_id": self.session_id,
                "message": self.message,
                "metadata": self.metadata,
                "timestamp": self.timestamp or _utc_now_iso(),
            }
        )
