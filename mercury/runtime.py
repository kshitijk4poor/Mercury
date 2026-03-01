"""Runtime execution engine for Mercury workflows."""

from __future__ import annotations

import asyncio
import copy
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mercury.parse import parse_workflow
from mercury.planner import HybridPlanner, PlannerStateView
from mercury.schemas import CheckpointModel, TaskInputModel, TaskRecordModel
from mercury.state import (
    add_artifact,
    add_event,
    create_memory,
    ensure_workspace,
    load_checkpoint,
    memory_from_checkpoint,
    save_checkpoint,
)
from mercury.types import (
    AgentCallable,
    AgentContext,
    ParseError,
    PlannerActionType,
    PlannerContractError,
    RunResult,
    SkillCallable,
    SkillContext,
    TaskKind,
    TaskRecord,
    TaskSpec,
    TaskStatus,
    ToolCallable,
    ToolContext,
)


@dataclass
class _RunState:
    run_id: str
    workflow_id: str
    task_specs: dict[str, TaskSpec]
    task_records: dict[str, TaskRecord]
    max_concurrency: int
    final_artifact_id: str | None = None
    cancelled: bool = False


class Orchestrator:
    def __init__(self) -> None:
        self._agents: dict[str, AgentCallable] = {}
        self._tools: dict[str, ToolCallable] = {}
        self._skills: dict[str, SkillCallable] = {}
        self._run_cancel: dict[str, asyncio.Event] = {}

    def register_agent(self, name: str, fn: AgentCallable) -> None:
        if name in self._agents:
            raise ValueError(f"agent '{name}' already registered")
        self._agents[name] = fn

    def register_tool(self, name: str, fn: ToolCallable) -> None:
        if name in self._tools:
            raise ValueError(f"tool '{name}' already registered")
        self._tools[name] = fn

    def register_skill(self, name: str, fn: SkillCallable) -> None:
        if name in self._skills:
            raise ValueError(f"skill '{name}' already registered")
        self._skills[name] = fn

    def cancel_run(self, run_id: str) -> None:
        event = self._run_cancel.get(run_id)
        if event:
            event.set()

    def inspect_run(self, checkpoint_path: str | Path) -> dict[str, Any]:
        return load_checkpoint(checkpoint_path).model_dump(mode="json")

    def _task_input_model(self, spec: TaskSpec) -> TaskInputModel:
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
    def _checkpoint_path(workspace_root: str | Path, run_id: str) -> Path:
        workspace = ensure_workspace(workspace_root)
        return workspace.checkpoints / f"{run_id}.json"

    @staticmethod
    def _planner_view(state: _RunState) -> PlannerStateView:
        return PlannerStateView(
            run_id=state.run_id,
            workflow_id=state.workflow_id,
            final_artifact_id=state.final_artifact_id,
            task_specs=state.task_specs,
            task_statuses={
                task_id: record.status for task_id, record in state.task_records.items()
            },
        )

    def _build_checkpoint(self, state: _RunState, memory) -> CheckpointModel:
        return CheckpointModel(
            version=1,
            run_id=state.run_id,
            workflow_id=state.workflow_id,
            max_concurrency=state.max_concurrency,
            final_artifact_id=state.final_artifact_id,
            cancelled=state.cancelled,
            working=memory.working,
            episodic=[
                {
                    "event_type": e.event_type,
                    "payload": e.payload,
                    "timestamp": e.timestamp,
                }
                for e in memory.episodic
            ],
            artifacts={
                key: {
                    "artifact_id": value.artifact_id,
                    "task_id": value.task_id,
                    "data": value.data,
                    "timestamp": value.timestamp,
                }
                for key, value in memory.artifacts.items()
            },
            task_specs=[
                self._task_input_model(spec) for spec in state.task_specs.values()
            ],
            task_records={
                task_id: self._task_record_model(record)
                for task_id, record in state.task_records.items()
            },
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
        failed_states = {TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.BLOCKED}
        return any(
            task_records[dep_id].status in failed_states for dep_id in task.depends_on
        )

    async def _execute_task(
        self, state: _RunState, task_id: str, memory, checkpoint_path: Path
    ) -> None:
        spec = state.task_specs[task_id]
        record = state.task_records[task_id]
        record.status = TaskStatus.RUNNING
        add_event(
            memory,
            "task_transition",
            {"task_id": task_id, "status": TaskStatus.RUNNING.value},
        )
        save_checkpoint(self._build_checkpoint(state, memory), checkpoint_path)

        try:
            working_view = copy.deepcopy(memory.working)
            input_view = copy.deepcopy(spec.input)
            if spec.kind == TaskKind.AGENT:
                handler = self._agents.get(spec.target)
                if not handler:
                    raise ParseError(f"missing agent '{spec.target}'")
                res = await handler(
                    AgentContext(
                        run_id=state.run_id,
                        task_id=task_id,
                        working=working_view,
                        input=input_view,
                    )
                )
            elif spec.kind == TaskKind.TOOL:
                handler = self._tools.get(spec.target)
                if not handler:
                    raise ParseError(f"missing tool '{spec.target}'")
                res = await handler(
                    input_view,
                    ToolContext(
                        run_id=state.run_id,
                        task_id=task_id,
                        working=working_view,
                        input=input_view,
                    ),
                )
            else:
                handler = self._skills.get(spec.target)
                if not handler:
                    raise ParseError(f"missing skill '{spec.target}'")
                res = await handler(
                    SkillContext(
                        run_id=state.run_id,
                        task_id=task_id,
                        working=working_view,
                        input=input_view,
                    )
                )

            if not isinstance(res, dict):
                raise ParseError("task handlers must return dictionaries")
            output = res.get("output", res)
            if not isinstance(output, dict):
                raise ParseError("task output must be a dictionary")

            artifact = add_artifact(memory, task_id, output)
            memory.working[task_id] = output
            record.status = TaskStatus.SUCCEEDED
            record.artifact_id = artifact.artifact_id
            state.final_artifact_id = artifact.artifact_id
            add_event(
                memory,
                "task_transition",
                {"task_id": task_id, "status": TaskStatus.SUCCEEDED.value},
            )
        except Exception as exc:  # noqa: BLE001
            record.error = str(exc)
            record.attempts += 1
            if record.attempts <= spec.max_retries:
                await asyncio.sleep(0.01 * (2 ** (record.attempts - 1)))
                record.status = TaskStatus.PENDING
                add_event(
                    memory,
                    "task_transition",
                    {"task_id": task_id, "status": TaskStatus.PENDING.value},
                )
            elif spec.fallback_output is not None:
                fallback_output = copy.deepcopy(spec.fallback_output)
                artifact = add_artifact(memory, task_id, fallback_output)
                memory.working[task_id] = fallback_output
                record.status = TaskStatus.SUCCEEDED
                record.artifact_id = artifact.artifact_id
                state.final_artifact_id = artifact.artifact_id
                add_event(
                    memory, "task_fallback", {"task_id": task_id, "error": record.error}
                )
                add_event(
                    memory,
                    "task_transition",
                    {"task_id": task_id, "status": TaskStatus.SUCCEEDED.value},
                )
            else:
                record.status = TaskStatus.FAILED
                add_event(
                    memory,
                    "task_transition",
                    {"task_id": task_id, "status": TaskStatus.FAILED.value},
                )
                for dep_id, dep_spec in state.task_specs.items():
                    if (
                        task_id in dep_spec.depends_on
                        and state.task_records[dep_id].status == TaskStatus.PENDING
                    ):
                        state.task_records[dep_id].status = TaskStatus.BLOCKED
                        add_event(
                            memory,
                            "task_transition",
                            {"task_id": dep_id, "status": TaskStatus.BLOCKED.value},
                        )
        finally:
            save_checkpoint(self._build_checkpoint(state, memory), checkpoint_path)

    async def _run_state(
        self,
        state: _RunState,
        *,
        planner_model: Any,
        workspace: str | Path,
        initial_memory=None,
    ) -> RunResult:
        memory = initial_memory or create_memory()
        checkpoint_path = self._checkpoint_path(workspace, state.run_id)
        cancel_event = self._run_cancel.setdefault(state.run_id, asyncio.Event())
        planner = HybridPlanner(planner_model)
        stall_cycles = 0
        save_checkpoint(self._build_checkpoint(state, memory), checkpoint_path)

        try:
            while True:
                if cancel_event.is_set():
                    state.cancelled = True
                    for rec in state.task_records.values():
                        if rec.status == TaskStatus.PENDING:
                            rec.status = TaskStatus.CANCELLED
                            add_event(
                                memory,
                                "task_transition",
                                {
                                    "task_id": rec.spec.id,
                                    "status": TaskStatus.CANCELLED.value,
                                },
                            )
                    save_checkpoint(
                        self._build_checkpoint(state, memory), checkpoint_path
                    )
                    return RunResult(
                        run_id=state.run_id,
                        status="cancelled",
                        final_artifact_id=state.final_artifact_id,
                        checkpoint_path=str(checkpoint_path),
                    )

                action = await planner.plan(self._planner_view(state))
                add_event(
                    memory,
                    "planner_action",
                    {
                        "action": action.action.value,
                        "tasks": [task.id for task in action.tasks],
                        "final_artifact_id": action.final_artifact_id,
                    },
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
                    state.final_artifact_id = (
                        action.final_artifact_id or state.final_artifact_id
                    )
                    save_checkpoint(
                        self._build_checkpoint(state, memory), checkpoint_path
                    )
                    return RunResult(
                        run_id=state.run_id,
                        status=status,
                        final_artifact_id=state.final_artifact_id,
                        checkpoint_path=str(checkpoint_path),
                    )

                runnable: list[str] = []
                if action.action == PlannerActionType.ENQUEUE:
                    for planned in action.tasks:
                        if planned.id not in state.task_records:
                            raise PlannerContractError(
                                f"planner contract violation: unknown task id '{planned.id}'"
                            )
                        if state.task_records[planned.id].status != TaskStatus.PENDING:
                            continue
                        if self._has_failed_dependency(
                            state.task_specs[planned.id], state.task_records
                        ):
                            state.task_records[planned.id].status = TaskStatus.BLOCKED
                            add_event(
                                memory,
                                "task_transition",
                                {
                                    "task_id": planned.id,
                                    "status": TaskStatus.BLOCKED.value,
                                },
                            )
                            continue
                        if self._deps_satisfied(
                            state.task_specs[planned.id], state.task_records
                        ):
                            runnable.append(planned.id)

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
                        if cancel_event.is_set():
                            record = state.task_records[task_id]
                            if record.status == TaskStatus.PENDING:
                                record.status = TaskStatus.CANCELLED
                                add_event(
                                    memory,
                                    "task_transition",
                                    {
                                        "task_id": task_id,
                                        "status": TaskStatus.CANCELLED.value,
                                    },
                                )
                                save_checkpoint(
                                    self._build_checkpoint(state, memory),
                                    checkpoint_path,
                                )
                            return
                        await self._execute_task(
                            state, task_id, memory, checkpoint_path
                        )

                await asyncio.gather(*(run_single(task_id) for task_id in runnable))
        finally:
            self._run_cancel.pop(state.run_id, None)

    async def run_flow(
        self,
        workflow: dict[str, Any],
        *,
        planner_model: Any,
        max_concurrency: int = 4,
        workspace: str | None = None,
    ) -> RunResult:
        parsed = parse_workflow(workflow)
        run_id = f"run-{uuid.uuid4().hex[:12]}"
        state = _RunState(
            run_id=run_id,
            workflow_id=parsed.workflow_id,
            task_specs=parsed.tasks,
            task_records={
                task_id: TaskRecord(spec=spec) for task_id, spec in parsed.tasks.items()
            },
            max_concurrency=max(1, max_concurrency),
        )
        return await self._run_state(
            state,
            planner_model=planner_model,
            workspace=workspace or ".",
        )

    async def resume_flow(
        self, checkpoint_path: str | Path, *, planner_model: Any
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
        state = _RunState(
            run_id=checkpoint.run_id,
            workflow_id=checkpoint.workflow_id,
            task_specs=parsed_workflow.tasks,
            task_records={
                task_id: TaskRecord(spec=spec)
                for task_id, spec in parsed_workflow.tasks.items()
            },
            final_artifact_id=checkpoint.final_artifact_id,
            cancelled=False,
            max_concurrency=checkpoint.max_concurrency,
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
            planner_model=planner_model,
            workspace=Path(checkpoint_path).resolve().parent.parent.parent,
            initial_memory=memory_from_checkpoint(checkpoint),
        )


_DEFAULT_ORCHESTRATOR = Orchestrator()
_RUN_CANCEL = _DEFAULT_ORCHESTRATOR._run_cancel


def register_agent(name: str, fn: AgentCallable) -> None:
    _DEFAULT_ORCHESTRATOR.register_agent(name, fn)


def register_tool(name: str, fn: ToolCallable) -> None:
    _DEFAULT_ORCHESTRATOR.register_tool(name, fn)


def register_skill(name: str, fn: SkillCallable) -> None:
    _DEFAULT_ORCHESTRATOR.register_skill(name, fn)


def cancel_run(run_id: str) -> None:
    _DEFAULT_ORCHESTRATOR.cancel_run(run_id)


def inspect_run(checkpoint_path: str | Path) -> dict[str, Any]:
    return _DEFAULT_ORCHESTRATOR.inspect_run(checkpoint_path)


async def run_flow(
    workflow: dict[str, Any],
    *,
    planner_model: Any,
    max_concurrency: int = 4,
    workspace: str | None = None,
) -> RunResult:
    return await _DEFAULT_ORCHESTRATOR.run_flow(
        workflow,
        planner_model=planner_model,
        max_concurrency=max_concurrency,
        workspace=workspace,
    )


async def resume_flow(checkpoint_path: str | Path, *, planner_model: Any) -> RunResult:
    return await _DEFAULT_ORCHESTRATOR.resume_flow(
        checkpoint_path, planner_model=planner_model
    )
