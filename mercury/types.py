"""Core domain types and extension contracts for Mercury v2."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Protocol


class ParseError(ValueError):
    """Raised when boundary data cannot be parsed into domain types."""

    def __init__(self, message: str, *, path: str | None = None) -> None:
        self.path = path
        text = f"{message} @ {path}" if path else message
        super().__init__(text)


class PlannerContractError(RuntimeError):
    """Raised when planner emits actions violating runtime contracts."""


class SchedulerContractError(RuntimeError):
    """Raised when scheduler emits invalid decisions."""


class TaskKind(str, Enum):
    AGENT = "agent"
    TOOL = "tool"
    SKILL = "skill"


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"
    PAUSED = "paused"


class DurabilityMode(str, Enum):
    SYNC = "sync"
    ASYNC = "async"
    EXIT = "exit"


class PlannerActionType(str, Enum):
    ENQUEUE = "ENQUEUE"
    NOOP = "NOOP"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True)
class TaskSpec:
    id: str
    kind: TaskKind
    target: str
    input: dict[str, Any] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    needs_reasoning: bool = False
    max_retries: int = 0
    fallback_output: dict[str, Any] | None = None


@dataclass(frozen=True)
class WorkflowSpec:
    workflow_id: str
    tasks: dict[str, TaskSpec]


@dataclass(frozen=True)
class PlannerAction:
    action: PlannerActionType
    task_ids: tuple[str, ...] = ()
    final_artifact_id: str | None = None


@dataclass(frozen=True)
class SchedulerDecision:
    task_ids: tuple[str, ...]
    state: Any = None


@dataclass(frozen=True)
class InboundEvent:
    source: str
    session_id: str
    message: str
    metadata: dict[str, Any]
    timestamp: str


@dataclass(frozen=True)
class LifecycleEvent:
    event_type: str
    payload: dict[str, Any]
    timestamp: str
    tick: int


@dataclass
class EventRecord:
    event_type: str
    payload: dict[str, Any]
    timestamp: str
    tick: int | None = None


@dataclass
class ArtifactRecord:
    artifact_id: str
    task_id: str
    data: dict[str, Any]
    timestamp: str


@dataclass
class MemoryContext:
    working: dict[str, Any] = field(default_factory=dict)
    episodic: list[EventRecord] = field(default_factory=list)
    artifacts: dict[str, ArtifactRecord] = field(default_factory=dict)


@dataclass
class TaskRecord:
    spec: TaskSpec
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    error: str | None = None
    artifact_id: str | None = None


@dataclass
class RunResult:
    run_id: str
    status: str
    final_artifact_id: str | None
    checkpoint_path: str | None


@dataclass(frozen=True)
class AgentContext:
    run_id: str
    task_id: str
    working: Mapping[str, Any]
    input: Mapping[str, Any]


@dataclass(frozen=True)
class ToolContext:
    run_id: str
    task_id: str
    working: Mapping[str, Any]
    input: Mapping[str, Any]


@dataclass(frozen=True)
class SkillContext:
    run_id: str
    task_id: str
    working: Mapping[str, Any]
    input: Mapping[str, Any]


@dataclass(frozen=True)
class AgentResult:
    output: dict[str, Any]


@dataclass(frozen=True)
class ToolResult:
    output: dict[str, Any]


@dataclass(frozen=True)
class SkillResult:
    output: dict[str, Any]


@dataclass(frozen=True)
class HitlDecision:
    pause: bool
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PlannerStateView:
    run_id: str
    workflow_id: str
    tick: int
    final_artifact_id: str | None
    task_specs: dict[str, TaskSpec]
    task_statuses: dict[str, TaskStatus]
    task_order: tuple[str, ...]
    pending_approval: dict[str, Any] | None


class AgentCallable(Protocol):
    async def __call__(self, ctx: AgentContext) -> AgentResult | dict[str, Any]: ...


class ToolCallable(Protocol):
    async def __call__(
        self, inp: dict[str, Any], ctx: ToolContext
    ) -> ToolResult | dict[str, Any]: ...


class SkillCallable(Protocol):
    async def __call__(self, ctx: SkillContext) -> SkillResult | dict[str, Any]: ...


class PlannerPlugin(Protocol):
    def parse_config(self, raw: Mapping[str, Any] | None) -> Any: ...

    async def plan(
        self, state_view: PlannerStateView, config: Any
    ) -> PlannerAction | dict[str, Any]: ...


class SchedulerPlugin(Protocol):
    def parse_config(self, raw: Mapping[str, Any] | None) -> Any: ...

    def init_state(self, config: Any) -> Any: ...

    def parse_state(self, raw: Any) -> Any: ...

    async def pick(
        self,
        ready_task_ids: tuple[str, ...],
        state_view: PlannerStateView,
        scheduler_state: Any,
        config: Any,
    ) -> SchedulerDecision | dict[str, Any]: ...


class SandboxPlugin(Protocol):
    def parse_config(self, raw: Mapping[str, Any] | None) -> Any: ...

    async def execute(
        self,
        *,
        kind: TaskKind,
        target: str,
        handler: Any,
        task_input: dict[str, Any],
        ctx: AgentContext | ToolContext | SkillContext,
        config: Any,
    ) -> dict[str, Any]: ...


class HitlPlugin(Protocol):
    def parse_config(self, raw: Mapping[str, Any] | None) -> Any: ...

    def subscribed_events(self, config: Any) -> frozenset[str] | None: ...

    async def maybe_pause(
        self,
        event: LifecycleEvent,
        state_view: PlannerStateView,
        config: Any,
    ) -> HitlDecision | dict[str, Any]: ...


class HookCallable(Protocol):
    async def __call__(self, event: LifecycleEvent) -> Any: ...


class InboundAdapter(Protocol):
    def __aiter__(self): ...


class InboundAdapterFactory(Protocol):
    def __call__(self, raw: Mapping[str, Any]) -> InboundAdapter: ...
