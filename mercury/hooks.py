"""Read-only lifecycle hooks for observability."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass, field

from mercury.types import HookCallable, LifecycleEvent


@dataclass
class HookRegistry:
    hooks: dict[str, HookCallable] = field(default_factory=dict)

    def register(self, name: str, fn: HookCallable) -> None:
        if name in self.hooks:
            raise ValueError(f"hook '{name}' already registered")
        self.hooks[name] = fn

    async def emit(self, event: LifecycleEvent) -> None:
        for fn in self.hooks.values():
            # Hooks are read-only by contract; pass a deep copy so mutation is isolated.
            cloned = LifecycleEvent(
                event_type=event.event_type,
                payload=copy.deepcopy(event.payload),
                timestamp=event.timestamp,
                tick=event.tick,
            )
            try:
                result = fn(cloned)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:  # noqa: BLE001
                continue
