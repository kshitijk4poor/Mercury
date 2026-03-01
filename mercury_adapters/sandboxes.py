"""Built-in sandbox adapters."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from mercury.types import TaskKind


class HostSandboxConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HostSandbox:
    def parse_config(self, raw):
        return HostSandboxConfig.model_validate(raw or {})

    async def execute(
        self, *, kind: TaskKind, target, handler, task_input, ctx, config
    ):
        del target, config
        if kind == TaskKind.AGENT:
            result = handler(ctx)
        elif kind == TaskKind.TOOL:
            result = handler(task_input, ctx)
        else:
            result = handler(ctx)

        if asyncio.iscoroutine(result):
            result = await result
        return result


class DockerSandboxConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    container: str | None = None
    executor: Any | None = None


class DockerSandbox:
    def __init__(self) -> None:
        self._host = HostSandbox()

    def parse_config(self, raw):
        return DockerSandboxConfig.model_validate(raw or {})

    async def execute(
        self, *, kind: TaskKind, target, handler, task_input, ctx, config
    ):
        if config.executor is None:
            return await self._host.execute(
                kind=kind,
                target=target,
                handler=handler,
                task_input=task_input,
                ctx=ctx,
                config=self._host.parse_config({}),
            )

        result = config.executor(kind.value, target, task_input, ctx)
        if asyncio.iscoroutine(result):
            result = await result
        return result
