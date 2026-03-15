"""Adapter-driven Mercury v2 runtime kernel."""

from __future__ import annotations

import asyncio
import copy
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from mercury.hooks import HookRegistry
from mercury.parse import (
    parse_inbound_event,
    parse_planner_action,
    parse_scheduler_decision,
    parse_workflow,
)
from mercury.registry import RuntimeRegistry
from mercury.schemas import CheckpointModel, TaskInputModel, TaskRecordModel
from mercury.state import (
    add_artifact,
    add_event,
    append_event_journal,
    checkpoint_to_model,
    create_memory,
    ensure_workspace,
    load_checkpoint,
    memory_from_checkpoint,
    save_checkpoint,
)
from mercury.types import (
    AgentContext,
    DurabilityMode,
    HitlDecision,
    InboundEvent,
    LifecycleEvent,
    ParseError,
    PlannerAction,
    PlannerActionType,
    PlannerContractError,
    PlannerStateView,
    RunResult,
    SchedulerContractError,
    SchedulerDecision,
    SkillContext,
    TaskKind,
    TaskRecord,
    TaskSpec,
    TaskStatus,
    ToolContext,
)


def _snapshot_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(k): _snapshot_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_snapshot_value(v) for v in value]
    try:
        json.dumps(value)
        return value
    except TypeError:
        return f"<non-serializable:{type(value).__name__}>"


def _has_non_serializable_marker(value: Any) -> bool:
    if isinstance(value, str):
        return value.startswith("<non-serializable:")
    if isinstance(value, dict):
        return any(_has_non_serializable_marker(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(_has_non_serializable_marker(v) for v in value)
    return False


def _parse_durability_mode(value: str) -> DurabilityMode:
    try:
        return DurabilityMode(value)
    except ValueError as exc:
        raise ValueError(
            f"invalid durability_mode '{value}', expected one of: sync, async, exit"
        ) from exc


def _immutable_view(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({k: _immutable_view(v) for k, v in value.items()})
    if isinstance(value, (list, tuple, set)):
        return tuple(_immutable_view(v) for v in value)
    return value


@dataclass
class _CheckpointWriter:
    path: Path
    mode: DurabilityMode
    _latest: CheckpointModel | None = None
    _pending: asyncio.Task | None = None

    async def _flush_latest_async(self) -> None:
        while self._latest is not None:
            checkpoint = self._latest
            self._latest = None
            await asyncio.to_thread(save_checkpoint, checkpoint, self.path)

    async def write(self, checkpoint: CheckpointModel, *, force: bool = False) -> None:
        if force or self.mode == DurabilityMode.SYNC:
            await self.flush()
            save_checkpoint(checkpoint, self.path)
            return

        self._latest = checkpoint
        if self.mode == DurabilityMode.EXIT:
            return

        if self._pending is None or self._pending.done():
            self._pending = asyncio.create_task(self._flush_latest_async())

    async def flush(self) -> None:
        if self._pending is not None:
            await self._pending
            self._pending = None
        if self._latest is not None:
            checkpoint = self._latest
            self._latest = None
            if self.mode == DurabilityMode.ASYNC:
                await asyncio.to_thread(save_checkpoint, checkpoint, self.path)
            else:
                save_checkpoint(checkpoint, self.path)


@dataclass
class _ResolvedPlugins:
    planner_id: str
    planner: Any
    planner_config_typed: Any
    planner_config_raw: dict[str, Any]

    scheduler_id: str
    scheduler: Any
    scheduler_config_typed: Any
    scheduler_config_raw: dict[str, Any]

    sandbox_id: str
    sandbox: Any
    sandbox_config_typed: Any
    sandbox_config_raw: dict[str, Any]

    hitl_id: str | None
    hitl: Any
    hitl_config_typed: Any
    hitl_config_raw: dict[str, Any] | None


@dataclass
class _RunState:
    run_id: str
    workflow_id: str
    checkpoint_path: Path
    event_log_path: Path
    task_specs: dict[str, TaskSpec]
    task_order: tuple[str, ...]
    task_records: dict[str, TaskRecord]
    max_concurrency: int
    durability_mode: DurabilityMode

    planner_id: str
    planner_config: dict[str, Any]
    scheduler_id: str
    scheduler_config: dict[str, Any]
    scheduler_state: Any
    sandbox_id: str
    sandbox_config: dict[str, Any]
    hitl_id: str | None
    hitl_config: dict[str, Any] | None

    final_artifact_id: str | None = None
    cancelled: bool = False
    paused: bool = False
    pending_approval: dict[str, Any] | None = None
    tick: int = 0


FAILED_STATES = {TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.BLOCKED}


class Orchestrator:
    def __init__(self) -> None:
        self._registry = RuntimeRegistry()
        self._hooks = HookRegistry()
        self._run_cancel: dict[str, asyncio.Event] = {}

    def register_agent(self, name: str, fn) -> None:
        self._registry.register_agent(name, fn)

    def register_tool(self, name: str, fn) -> None:
        self._registry.register_tool(name, fn)

    def register_skill(self, name: str, fn) -> None:
        self._registry.register_skill(name, fn)

    def register_planner(self, name: str, plugin) -> None:
        self._registry.register_planner(name, plugin)

    def register_scheduler(self, name: str, plugin) -> None:
        self._registry.register_scheduler(name, plugin)

    def register_sandbox(self, name: str, plugin) -> None:
        self._registry.register_sandbox(name, plugin)

    def register_hitl(self, name: str, plugin) -> None:
        self._registry.register_hitl(name, plugin)

    def register_inbound_adapter(self, name: str, factory) -> None:
        self._registry.register_inbound_adapter(name, factory)

    def register_hook(self, name: str, fn) -> None:
        self._hooks.register(name, fn)

    def cancel_run(self, run_id: str) -> None:
        event = self._run_cancel.get(run_id)
        if event:
            event.set()

    def inspect_run(self, checkpoint_path: str | Path) -> dict[str, Any]:
        return load_checkpoint(checkpoint_path).model_dump(mode="json")

    @staticmethod
    def _checkpoint_path(workspace_root: str | Path, run_id: str) -> Path:
        workspace = ensure_workspace(workspace_root)
        return workspace.checkpoints / f"{run_id}.json"

    @staticmethod
    def _event_log_path(workspace_root: str | Path, run_id: str) -> Path:
        workspace = ensure_workspace(workspace_root)
        return workspace.events / f"{run_id}.jsonl"

    @staticmethod
    def _task_input_model(spec: TaskSpec) -> TaskInputModel:
        return TaskInputModel(
            id=spec.id,
            kind=spec.kind.value,
            target=spec.target,
            input=spec.input,
            depends_on=list(spec.depends_on),
            needs_reasoning=spec.needs_reasoning,
            max_retries=spec.max_retries,
            fallback_output=spec.fallback_output,
        )

    @staticmethod
    def _task_record_model(record: TaskRecord) -> TaskRecordModel:
        return TaskRecordModel(
            status=record.status.value,
            attempts=record.attempts,
            error=record.error,
            artifact_id=record.artifact_id,
        )

    @staticmethod
    def _deps_satisfied(task: TaskSpec, task_records: dict[str, TaskRecord]) -> bool:
        return all(
            task_records[dep_id].status == TaskStatus.SUCCEEDED
            for dep_id in task.depends_on
        )

    @staticmethod
    def _has_failed_dependency(
        task: TaskSpec, task_records: dict[str, TaskRecord]
    ) -> bool:
        return any(
            task_records[dep_id].status in FAILED_STATES for dep_id in task.depends_on
        )

    @staticmethod
    def _planner_view(state: _RunState) -> PlannerStateView:
        return PlannerStateView(
            run_id=state.run_id,
            workflow_id=state.workflow_id,
            tick=state.tick,
            final_artifact_id=state.final_artifact_id,
            task_specs=state.task_specs,
            task_statuses={
                task_id: record.status for task_id, record in state.task_records.items()
            },
            task_order=state.task_order,
            pending_approval=state.pending_approval,
        )

    def _build_checkpoint(self, state: _RunState, memory) -> CheckpointModel:
        return checkpoint_to_model(
            run_id=state.run_id,
            workflow_id=state.workflow_id,
            planner_id=state.planner_id,
            planner_config=state.planner_config,
            scheduler_id=state.scheduler_id,
            scheduler_config=state.scheduler_config,
            scheduler_state=_snapshot_value(state.scheduler_state),
            sandbox_id=state.sandbox_id,
            sandbox_config=state.sandbox_config,
            hitl_id=state.hitl_id,
            hitl_config=state.hitl_config,
            max_concurrency=state.max_concurrency,
            durability_mode=state.durability_mode.value,
            tick=state.tick,
            final_artifact_id=state.final_artifact_id,
            cancelled=state.cancelled,
            paused=state.paused,
            pending_approval=state.pending_approval,
            working=memory.working,
            episodic=memory.episodic,
            artifacts=memory.artifacts,
            task_specs=[
                self._task_input_model(spec) for spec in state.task_specs.values()
            ],
            task_records={
                task_id: self._task_record_model(record)
                for task_id, record in state.task_records.items()
            },
        )

    def _resolve_plugins(
        self,
        *,
        planner_id: str,
        planner_config: Mapping[str, Any] | None,
        scheduler_id: str,
        scheduler_config: Mapping[str, Any] | None,
        sandbox_id: str,
        sandbox_config: Mapping[str, Any] | None,
        hitl_id: str | None,
        hitl_config: Mapping[str, Any] | None,
    ) -> _ResolvedPlugins:
        planner = self._registry.resolve(self._registry.planners, "planner", planner_id)
        scheduler = self._registry.resolve(
            self._registry.schedulers, "scheduler", scheduler_id
        )
        sandbox = self._registry.resolve(
            self._registry.sandboxes, "sandbox", sandbox_id
        )

        resolved_hitl_id = hitl_id or "none"
        hitl = self._registry.resolve(self._registry.hitl, "hitl", resolved_hitl_id)

        planner_config_raw = dict(planner_config or {})
        scheduler_config_raw = dict(scheduler_config or {})
        sandbox_config_raw = dict(sandbox_config or {})
        hitl_config_raw = dict(hitl_config or {})

        return _ResolvedPlugins(
            planner_id=planner_id,
            planner=planner,
            planner_config_typed=planner.parse_config(planner_config_raw),
            planner_config_raw=_snapshot_value(planner_config_raw),
            scheduler_id=scheduler_id,
            scheduler=scheduler,
            scheduler_config_typed=scheduler.parse_config(scheduler_config_raw),
            scheduler_config_raw=_snapshot_value(scheduler_config_raw),
            sandbox_id=sandbox_id,
            sandbox=sandbox,
            sandbox_config_typed=sandbox.parse_config(sandbox_config_raw),
            sandbox_config_raw=_snapshot_value(sandbox_config_raw),
            hitl_id=resolved_hitl_id,
            hitl=hitl,
            hitl_config_typed=hitl.parse_config(hitl_config_raw),
            hitl_config_raw=_snapshot_value(hitl_config_raw),
        )

    async def _consume_inbound_events(
        self,
        *,
        inbound_adapter_id: str | None,
        inbound_adapter_config: Mapping[str, Any] | None,
    ) -> tuple[InboundEvent, ...]:
        if inbound_adapter_id is None:
            return ()

        factory = self._registry.resolve(
            self._registry.inbound_adapters, "inbound adapter", inbound_adapter_id
        )
        adapter = factory(dict(inbound_adapter_config or {}))
        events: list[InboundEvent] = []
        async for raw in adapter:
            event = raw if isinstance(raw, InboundEvent) else parse_inbound_event(raw)
            events.append(event)
        return tuple(events)

    @staticmethod
    def _require_resume_config_override(
        *,
        config_name: str,
        stored: Mapping[str, Any] | None,
        provided: Mapping[str, Any] | None,
    ) -> None:
        if provided is not None:
            return
        if stored and _has_non_serializable_marker(stored):
            raise ValueError(
                f"checkpoint {config_name} contains non-serializable values; "
                f"provide {config_name} when resuming"
            )

    @staticmethod
    def _hitl_subscriptions(
        plugins: _ResolvedPlugins,
    ) -> frozenset[str] | None:
        if not hasattr(plugins.hitl, "subscribed_events"):
            return None
        raw = plugins.hitl.subscribed_events(plugins.hitl_config_typed)
        if raw is None:
            return None
        return frozenset(raw)

    async def _emit_event(
        self,
        *,
        state: _RunState,
        memory,
        event_type: str,
        payload: dict[str, Any],
        plugins: _ResolvedPlugins,
    ) -> None:
        record = add_event(memory, event_type, payload, tick=state.tick)
        append_event_journal(
            state.event_log_path,
            run_id=state.run_id,
            workflow_id=state.workflow_id,
            tick=state.tick,
            event=record,
        )

        event = LifecycleEvent(
            event_type=record.event_type,
            payload=dict(record.payload),
            timestamp=record.timestamp,
            tick=state.tick,
        )
        await self._hooks.emit(event)

        subscriptions = self._hitl_subscriptions(plugins)
        if subscriptions is not None and event_type not in subscriptions:
            return

        decision_raw = await plugins.hitl.maybe_pause(
            event,
            self._planner_view(state),
            plugins.hitl_config_typed,
        )
        if isinstance(decision_raw, HitlDecision):
            decision = decision_raw
        else:
            decision = HitlDecision(
                pause=bool(decision_raw.get("pause", False)),
                metadata=dict(decision_raw.get("metadata") or {}),
            )

        if decision.pause:
            state.paused = True
            state.pending_approval = (
                decision.metadata
                if decision.metadata
                else {"event_type": event_type, "tick": state.tick}
            )

    @staticmethod
    def _ensure_event_journal(state: _RunState, memory) -> None:
        if state.event_log_path.exists():
            return
        for event in memory.episodic:
            append_event_journal(
                state.event_log_path,
                run_id=state.run_id,
                workflow_id=state.workflow_id,
                tick=event.tick,
                event=event,
            )

    async def _execute_task(
        self,
        state: _RunState,
        task_id: str,
        memory,
        checkpoint_writer: _CheckpointWriter,
        plugins: _ResolvedPlugins,
    ) -> None:
        spec = state.task_specs[task_id]
        record = state.task_records[task_id]
        record.status = TaskStatus.RUNNING
        await self._emit_event(
            state=state,
            memory=memory,
            event_type="task_transition",
            payload={"task_id": task_id, "status": TaskStatus.RUNNING.value},
            plugins=plugins,
        )
        await checkpoint_writer.write(self._build_checkpoint(state, memory))

        try:
            working_snapshot = _immutable_view(copy.deepcopy(memory.working))
            task_input = copy.deepcopy(spec.input)
            input_snapshot = _immutable_view(copy.deepcopy(spec.input))

            if spec.kind == TaskKind.AGENT:
                handler = self._registry.agents.get(spec.target)
                if not handler:
                    raise ParseError(f"missing agent '{spec.target}'")
                ctx = AgentContext(
                    run_id=state.run_id,
                    task_id=task_id,
                    working=working_snapshot,
                    input=input_snapshot,
                )
            elif spec.kind == TaskKind.TOOL:
                handler = self._registry.tools.get(spec.target)
                if not handler:
                    raise ParseError(f"missing tool '{spec.target}'")
                ctx = ToolContext(
                    run_id=state.run_id,
                    task_id=task_id,
                    working=working_snapshot,
                    input=input_snapshot,
                )
            else:
                handler = self._registry.skills.get(spec.target)
                if not handler:
                    raise ParseError(f"missing skill '{spec.target}'")
                ctx = SkillContext(
                    run_id=state.run_id,
                    task_id=task_id,
                    working=working_snapshot,
                    input=input_snapshot,
                )

            res = await plugins.sandbox.execute(
                kind=spec.kind,
                target=spec.target,
                handler=handler,
                task_input=task_input,
                ctx=ctx,
                config=plugins.sandbox_config_typed,
            )

            if not isinstance(res, dict):
                raise ParseError("task handlers must return dictionaries")
            output = res.get("output", res)
            if not isinstance(output, dict):
                raise ParseError("task output must be a dictionary")
            output = _snapshot_value(output)

            artifact = add_artifact(memory, task_id, output)
            memory.working[task_id] = output
            record.status = TaskStatus.SUCCEEDED
            record.artifact_id = artifact.artifact_id
            state.final_artifact_id = artifact.artifact_id
            await self._emit_event(
                state=state,
                memory=memory,
                event_type="task_transition",
                payload={"task_id": task_id, "status": TaskStatus.SUCCEEDED.value},
                plugins=plugins,
            )
        except Exception as exc:  # noqa: BLE001
            record.error = str(exc)
            record.attempts += 1
            if record.attempts <= spec.max_retries:
                await asyncio.sleep(0.01 * (2 ** (record.attempts - 1)))
                record.status = TaskStatus.PENDING
                await self._emit_event(
                    state=state,
                    memory=memory,
                    event_type="task_transition",
                    payload={"task_id": task_id, "status": TaskStatus.PENDING.value},
                    plugins=plugins,
                )
            elif spec.fallback_output is not None:
                fallback_output = _snapshot_value(copy.deepcopy(spec.fallback_output))
                artifact = add_artifact(memory, task_id, fallback_output)
                memory.working[task_id] = fallback_output
                record.status = TaskStatus.SUCCEEDED
                record.artifact_id = artifact.artifact_id
                state.final_artifact_id = artifact.artifact_id
                await self._emit_event(
                    state=state,
                    memory=memory,
                    event_type="task_fallback",
                    payload={"task_id": task_id, "error": record.error},
                    plugins=plugins,
                )
                await self._emit_event(
                    state=state,
                    memory=memory,
                    event_type="task_transition",
                    payload={"task_id": task_id, "status": TaskStatus.SUCCEEDED.value},
                    plugins=plugins,
                )
            else:
                record.status = TaskStatus.FAILED
                await self._emit_event(
                    state=state,
                    memory=memory,
                    event_type="task_transition",
                    payload={"task_id": task_id, "status": TaskStatus.FAILED.value},
                    plugins=plugins,
                )
                for dep_id, dep_spec in state.task_specs.items():
                    if (
                        task_id in dep_spec.depends_on
                        and state.task_records[dep_id].status == TaskStatus.PENDING
                    ):
                        state.task_records[dep_id].status = TaskStatus.BLOCKED
                        await self._emit_event(
                            state=state,
                            memory=memory,
                            event_type="task_transition",
                            payload={
                                "task_id": dep_id,
                                "status": TaskStatus.BLOCKED.value,
                            },
                            plugins=plugins,
                        )
        finally:
            await checkpoint_writer.write(self._build_checkpoint(state, memory))

    async def _run_state(
        self,
        state: _RunState,
        *,
        plugins: _ResolvedPlugins,
        initial_memory=None,
    ) -> RunResult:
        memory = initial_memory or create_memory()
        self._ensure_event_journal(state, memory)
        checkpoint_writer = _CheckpointWriter(
            path=state.checkpoint_path,
            mode=state.durability_mode,
        )
        cancel_event = self._run_cancel.setdefault(state.run_id, asyncio.Event())
        stall_cycles = 0

        await checkpoint_writer.write(self._build_checkpoint(state, memory), force=True)

        try:
            while True:
                state.tick += 1

                if cancel_event.is_set():
                    state.cancelled = True
                    for rec in state.task_records.values():
                        if rec.status == TaskStatus.PENDING:
                            rec.status = TaskStatus.CANCELLED
                            await self._emit_event(
                                state=state,
                                memory=memory,
                                event_type="task_transition",
                                payload={
                                    "task_id": rec.spec.id,
                                    "status": TaskStatus.CANCELLED.value,
                                },
                                plugins=plugins,
                            )
                    await checkpoint_writer.write(
                        self._build_checkpoint(state, memory),
                        force=True,
                    )
                    return RunResult(
                        run_id=state.run_id,
                        status="cancelled",
                        final_artifact_id=state.final_artifact_id,
                        checkpoint_path=str(state.checkpoint_path),
                    )

                planner_raw = await plugins.planner.plan(
                    self._planner_view(state), plugins.planner_config_typed
                )
                action = (
                    planner_raw
                    if isinstance(planner_raw, PlannerAction)
                    else parse_planner_action(planner_raw)
                )
                await self._emit_event(
                    state=state,
                    memory=memory,
                    event_type="planner_action",
                    payload={
                        "action": action.action.value,
                        "task_ids": list(action.task_ids),
                        "final_artifact_id": action.final_artifact_id,
                    },
                    plugins=plugins,
                )
                if state.paused:
                    await checkpoint_writer.write(
                        self._build_checkpoint(state, memory),
                        force=True,
                    )
                    return RunResult(
                        run_id=state.run_id,
                        status="paused",
                        final_artifact_id=state.final_artifact_id,
                        checkpoint_path=str(state.checkpoint_path),
                    )

                if action.action == PlannerActionType.COMPLETE:
                    if any(
                        record.status == TaskStatus.PENDING
                        for record in state.task_records.values()
                    ):
                        raise RuntimeError(
                            "planner emitted COMPLETE while tasks are pending"
                        )
                    if any(
                        record.status == TaskStatus.RUNNING
                        for record in state.task_records.values()
                    ):
                        raise RuntimeError(
                            "planner emitted COMPLETE while tasks are running"
                        )
                    status = "completed"
                    if any(
                        record.status == TaskStatus.FAILED
                        for record in state.task_records.values()
                    ):
                        status = "failed"
                    if state.cancelled:
                        status = "cancelled"
                    if state.paused:
                        status = "paused"
                    state.final_artifact_id = (
                        action.final_artifact_id or state.final_artifact_id
                    )
                    await checkpoint_writer.write(
                        self._build_checkpoint(state, memory),
                        force=True,
                    )
                    return RunResult(
                        run_id=state.run_id,
                        status=status,
                        final_artifact_id=state.final_artifact_id,
                        checkpoint_path=str(state.checkpoint_path),
                    )

                candidates: list[str] = []
                if action.action == PlannerActionType.ENQUEUE:
                    for task_id in action.task_ids:
                        if task_id not in state.task_records:
                            raise PlannerContractError(
                                "planner contract violation: unknown task id "
                                f"'{task_id}'"
                            )
                        if state.task_records[task_id].status != TaskStatus.PENDING:
                            continue
                        if self._has_failed_dependency(
                            state.task_specs[task_id], state.task_records
                        ):
                            state.task_records[task_id].status = TaskStatus.BLOCKED
                            await self._emit_event(
                                state=state,
                                memory=memory,
                                event_type="task_transition",
                                payload={
                                    "task_id": task_id,
                                    "status": TaskStatus.BLOCKED.value,
                                },
                                plugins=plugins,
                            )
                            continue
                        if self._deps_satisfied(
                            state.task_specs[task_id], state.task_records
                        ):
                            candidates.append(task_id)

                decision_raw = await plugins.scheduler.pick(
                    tuple(candidates),
                    self._planner_view(state),
                    state.scheduler_state,
                    plugins.scheduler_config_typed,
                )
                decision = (
                    decision_raw
                    if isinstance(decision_raw, SchedulerDecision)
                    else parse_scheduler_decision(decision_raw)
                )

                unknown = set(decision.task_ids) - set(candidates)
                if unknown:
                    raise SchedulerContractError(
                        f"scheduler selected non-ready task ids: {sorted(unknown)}"
                    )

                if decision.state is not None:
                    state.scheduler_state = decision.state

                runnable = list(decision.task_ids)
                if not runnable:
                    stall_cycles += 1
                    if stall_cycles > max(3, len(state.task_specs) * 2):
                        raise RuntimeError("planner stalled without emitting COMPLETE")
                    await asyncio.sleep(0)
                    continue
                stall_cycles = 0

                semaphore = asyncio.Semaphore(max(1, state.max_concurrency))

                async def run_single(task_id: str) -> None:
                    async with semaphore:
                        if cancel_event.is_set() or state.paused:
                            return
                        await self._execute_task(
                            state, task_id, memory, checkpoint_writer, plugins
                        )

                await asyncio.gather(*(run_single(task_id) for task_id in runnable))

                if state.paused:
                    await checkpoint_writer.write(
                        self._build_checkpoint(state, memory),
                        force=True,
                    )
                    return RunResult(
                        run_id=state.run_id,
                        status="paused",
                        final_artifact_id=state.final_artifact_id,
                        checkpoint_path=str(state.checkpoint_path),
                    )
        except Exception:  # noqa: BLE001
            await checkpoint_writer.write(
                self._build_checkpoint(state, memory),
                force=True,
            )
            raise
        finally:
            await checkpoint_writer.flush()
            self._run_cancel.pop(state.run_id, None)

    async def run_flow(
        self,
        workflow: dict[str, Any],
        *,
        planner_id: str,
        planner_config: Mapping[str, Any] | None = None,
        scheduler_id: str = "superstep",
        scheduler_config: Mapping[str, Any] | None = None,
        sandbox_id: str = "host",
        sandbox_config: Mapping[str, Any] | None = None,
        hitl_id: str | None = None,
        hitl_config: Mapping[str, Any] | None = None,
        inbound_adapter_id: str | None = None,
        inbound_adapter_config: Mapping[str, Any] | None = None,
        max_concurrency: int = 4,
        durability_mode: str = "sync",
        workspace: str | None = None,
    ) -> RunResult:
        parsed = parse_workflow(workflow)
        inbound_events = await self._consume_inbound_events(
            inbound_adapter_id=inbound_adapter_id,
            inbound_adapter_config=inbound_adapter_config,
        )
        plugins = self._resolve_plugins(
            planner_id=planner_id,
            planner_config=planner_config,
            scheduler_id=scheduler_id,
            scheduler_config=scheduler_config,
            sandbox_id=sandbox_id,
            sandbox_config=sandbox_config,
            hitl_id=hitl_id,
            hitl_config=hitl_config,
        )
        parsed_durability = _parse_durability_mode(durability_mode)
        run_id = f"run-{uuid.uuid4().hex[:12]}"
        workspace_root = workspace or "."
        initial_memory = None
        if inbound_events:
            event_payloads = [
                {
                    "source": event.source,
                    "session_id": event.session_id,
                    "message": event.message,
                    "metadata": dict(event.metadata),
                    "timestamp": event.timestamp,
                }
                for event in inbound_events
            ]
            initial_memory = create_memory(
                initial_working={"inbound_events": copy.deepcopy(event_payloads)}
            )
            for payload in event_payloads:
                add_event(
                    initial_memory,
                    "inbound_event",
                    payload,
                    timestamp=payload["timestamp"],
                    tick=0,
                )

        state = _RunState(
            run_id=run_id,
            workflow_id=parsed.workflow_id,
            checkpoint_path=self._checkpoint_path(workspace_root, run_id),
            event_log_path=self._event_log_path(workspace_root, run_id),
            task_specs=parsed.tasks,
            task_order=tuple(parsed.tasks.keys()),
            task_records={
                task_id: TaskRecord(spec=spec) for task_id, spec in parsed.tasks.items()
            },
            max_concurrency=max(1, max_concurrency),
            durability_mode=parsed_durability,
            planner_id=plugins.planner_id,
            planner_config=plugins.planner_config_raw,
            scheduler_id=plugins.scheduler_id,
            scheduler_config=plugins.scheduler_config_raw,
            scheduler_state=plugins.scheduler.init_state(
                plugins.scheduler_config_typed
            ),
            sandbox_id=plugins.sandbox_id,
            sandbox_config=plugins.sandbox_config_raw,
            hitl_id=plugins.hitl_id,
            hitl_config=plugins.hitl_config_raw,
        )
        return await self._run_state(
            state,
            plugins=plugins,
            initial_memory=initial_memory,
        )

    async def resume_flow(
        self,
        checkpoint_path: str | Path,
        *,
        planner_id: str | None = None,
        planner_config: Mapping[str, Any] | None = None,
        scheduler_id: str | None = None,
        scheduler_config: Mapping[str, Any] | None = None,
        sandbox_id: str | None = None,
        sandbox_config: Mapping[str, Any] | None = None,
        hitl_id: str | None = None,
        hitl_config: Mapping[str, Any] | None = None,
        durability_mode: str | None = None,
    ) -> RunResult:
        checkpoint = load_checkpoint(checkpoint_path)
        parsed_workflow = parse_workflow(
            {
                "workflow_id": checkpoint.workflow_id,
                "tasks": [
                    task.model_dump(mode="json") for task in checkpoint.task_specs
                ],
            }
        )

        effective_planner_id = planner_id or checkpoint.planner_id
        effective_scheduler_id = scheduler_id or checkpoint.scheduler_id
        effective_sandbox_id = sandbox_id or checkpoint.sandbox_id
        effective_hitl_id = hitl_id if hitl_id is not None else checkpoint.hitl_id
        effective_durability = _parse_durability_mode(
            durability_mode or checkpoint.durability_mode
        )

        self._require_resume_config_override(
            config_name="planner_config",
            stored=checkpoint.planner_config,
            provided=planner_config,
        )
        self._require_resume_config_override(
            config_name="scheduler_config",
            stored=checkpoint.scheduler_config,
            provided=scheduler_config,
        )
        self._require_resume_config_override(
            config_name="sandbox_config",
            stored=checkpoint.sandbox_config,
            provided=sandbox_config,
        )
        self._require_resume_config_override(
            config_name="hitl_config",
            stored=checkpoint.hitl_config,
            provided=hitl_config,
        )

        plugins = self._resolve_plugins(
            planner_id=effective_planner_id,
            planner_config=planner_config
            if planner_config is not None
            else checkpoint.planner_config,
            scheduler_id=effective_scheduler_id,
            scheduler_config=scheduler_config
            if scheduler_config is not None
            else checkpoint.scheduler_config,
            sandbox_id=effective_sandbox_id,
            sandbox_config=sandbox_config
            if sandbox_config is not None
            else checkpoint.sandbox_config,
            hitl_id=effective_hitl_id,
            hitl_config=hitl_config
            if hitl_config is not None
            else (checkpoint.hitl_config or {}),
        )
        workspace_root = Path(checkpoint_path).resolve().parent.parent.parent
        event_log_path = self._event_log_path(workspace_root, checkpoint.run_id)

        state = _RunState(
            run_id=checkpoint.run_id,
            workflow_id=checkpoint.workflow_id,
            checkpoint_path=Path(checkpoint_path),
            event_log_path=event_log_path,
            task_specs=parsed_workflow.tasks,
            task_order=tuple(parsed_workflow.tasks.keys()),
            task_records={
                task_id: TaskRecord(spec=spec)
                for task_id, spec in parsed_workflow.tasks.items()
            },
            max_concurrency=checkpoint.max_concurrency,
            durability_mode=effective_durability,
            planner_id=plugins.planner_id,
            planner_config=plugins.planner_config_raw,
            scheduler_id=plugins.scheduler_id,
            scheduler_config=plugins.scheduler_config_raw,
            scheduler_state=plugins.scheduler.parse_state(checkpoint.scheduler_state),
            sandbox_id=plugins.sandbox_id,
            sandbox_config=plugins.sandbox_config_raw,
            hitl_id=plugins.hitl_id,
            hitl_config=plugins.hitl_config_raw,
            final_artifact_id=checkpoint.final_artifact_id,
            cancelled=False,
            paused=False,
            pending_approval=None,
            tick=checkpoint.tick,
        )

        for task_id, record_model in checkpoint.task_records.items():
            record = state.task_records[task_id]
            record.status = TaskStatus(record_model.status)
            record.attempts = record_model.attempts
            record.error = record_model.error
            record.artifact_id = record_model.artifact_id
            if record.status == TaskStatus.CANCELLED:
                record.status = TaskStatus.PENDING
            if (
                record.status == TaskStatus.FAILED
                and record.attempts <= record.spec.max_retries
            ):
                record.status = TaskStatus.PENDING

        return await self._run_state(
            state,
            plugins=plugins,
            initial_memory=memory_from_checkpoint(checkpoint),
        )


_DEFAULT_ORCHESTRATOR = Orchestrator()
_RUN_CANCEL = _DEFAULT_ORCHESTRATOR._run_cancel


def register_agent(name: str, fn) -> None:
    _DEFAULT_ORCHESTRATOR.register_agent(name, fn)


def register_tool(name: str, fn) -> None:
    _DEFAULT_ORCHESTRATOR.register_tool(name, fn)


def register_skill(name: str, fn) -> None:
    _DEFAULT_ORCHESTRATOR.register_skill(name, fn)


def register_planner(name: str, plugin) -> None:
    _DEFAULT_ORCHESTRATOR.register_planner(name, plugin)


def register_scheduler(name: str, plugin) -> None:
    _DEFAULT_ORCHESTRATOR.register_scheduler(name, plugin)


def register_sandbox(name: str, plugin) -> None:
    _DEFAULT_ORCHESTRATOR.register_sandbox(name, plugin)


def register_hitl(name: str, plugin) -> None:
    _DEFAULT_ORCHESTRATOR.register_hitl(name, plugin)


def register_inbound_adapter(name: str, factory) -> None:
    _DEFAULT_ORCHESTRATOR.register_inbound_adapter(name, factory)


def register_hook(name: str, fn) -> None:
    _DEFAULT_ORCHESTRATOR.register_hook(name, fn)


def cancel_run(run_id: str) -> None:
    _DEFAULT_ORCHESTRATOR.cancel_run(run_id)


def inspect_run(checkpoint_path: str | Path) -> dict[str, Any]:
    return _DEFAULT_ORCHESTRATOR.inspect_run(checkpoint_path)


async def run_flow(
    workflow: dict[str, Any],
    *,
    planner_id: str,
    planner_config: Mapping[str, Any] | None = None,
    scheduler_id: str = "superstep",
    scheduler_config: Mapping[str, Any] | None = None,
    sandbox_id: str = "host",
    sandbox_config: Mapping[str, Any] | None = None,
    hitl_id: str | None = None,
    hitl_config: Mapping[str, Any] | None = None,
    inbound_adapter_id: str | None = None,
    inbound_adapter_config: Mapping[str, Any] | None = None,
    max_concurrency: int = 4,
    durability_mode: str = "sync",
    workspace: str | None = None,
) -> RunResult:
    return await _DEFAULT_ORCHESTRATOR.run_flow(
        workflow,
        planner_id=planner_id,
        planner_config=planner_config,
        scheduler_id=scheduler_id,
        scheduler_config=scheduler_config,
        sandbox_id=sandbox_id,
        sandbox_config=sandbox_config,
        hitl_id=hitl_id,
        hitl_config=hitl_config,
        inbound_adapter_id=inbound_adapter_id,
        inbound_adapter_config=inbound_adapter_config,
        max_concurrency=max_concurrency,
        durability_mode=durability_mode,
        workspace=workspace,
    )


async def resume_flow(
    checkpoint_path: str | Path,
    *,
    planner_id: str | None = None,
    planner_config: Mapping[str, Any] | None = None,
    scheduler_id: str | None = None,
    scheduler_config: Mapping[str, Any] | None = None,
    sandbox_id: str | None = None,
    sandbox_config: Mapping[str, Any] | None = None,
    hitl_id: str | None = None,
    hitl_config: Mapping[str, Any] | None = None,
    durability_mode: str | None = None,
) -> RunResult:
    return await _DEFAULT_ORCHESTRATOR.resume_flow(
        checkpoint_path,
        planner_id=planner_id,
        planner_config=planner_config,
        scheduler_id=scheduler_id,
        scheduler_config=scheduler_config,
        sandbox_id=sandbox_id,
        sandbox_config=sandbox_config,
        hitl_id=hitl_id,
        hitl_config=hitl_config,
        durability_mode=durability_mode,
    )
