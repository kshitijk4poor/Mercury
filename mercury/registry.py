"""Runtime registries for callables and plugins."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mercury.types import (
    AgentCallable,
    HitlPlugin,
    InboundAdapterFactory,
    PlannerPlugin,
    SandboxPlugin,
    SchedulerPlugin,
    SkillCallable,
    ToolCallable,
)


@dataclass
class RuntimeRegistry:
    agents: dict[str, AgentCallable] = field(default_factory=dict)
    tools: dict[str, ToolCallable] = field(default_factory=dict)
    skills: dict[str, SkillCallable] = field(default_factory=dict)
    planners: dict[str, PlannerPlugin] = field(default_factory=dict)
    schedulers: dict[str, SchedulerPlugin] = field(default_factory=dict)
    sandboxes: dict[str, SandboxPlugin] = field(default_factory=dict)
    hitl: dict[str, HitlPlugin] = field(default_factory=dict)
    inbound_adapters: dict[str, InboundAdapterFactory] = field(default_factory=dict)

    @staticmethod
    def _register(table: dict[str, Any], kind: str, name: str, value: Any) -> None:
        if name in table:
            raise ValueError(f"{kind} '{name}' already registered")
        table[name] = value

    def register_agent(self, name: str, fn: AgentCallable) -> None:
        self._register(self.agents, "agent", name, fn)

    def register_tool(self, name: str, fn: ToolCallable) -> None:
        self._register(self.tools, "tool", name, fn)

    def register_skill(self, name: str, fn: SkillCallable) -> None:
        self._register(self.skills, "skill", name, fn)

    def register_planner(self, name: str, plugin: PlannerPlugin) -> None:
        self._register(self.planners, "planner", name, plugin)

    def register_scheduler(self, name: str, plugin: SchedulerPlugin) -> None:
        self._register(self.schedulers, "scheduler", name, plugin)

    def register_sandbox(self, name: str, plugin: SandboxPlugin) -> None:
        self._register(self.sandboxes, "sandbox", name, plugin)

    def register_hitl(self, name: str, plugin: HitlPlugin) -> None:
        self._register(self.hitl, "hitl", name, plugin)

    def register_inbound_adapter(
        self, name: str, factory: InboundAdapterFactory
    ) -> None:
        self._register(self.inbound_adapters, "inbound adapter", name, factory)

    @staticmethod
    def resolve(table: dict[str, Any], kind: str, name: str) -> Any:
        if name not in table:
            raise ValueError(f"unknown {kind} '{name}'")
        return table[name]
