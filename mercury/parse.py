"""Boundary parsing and schema-to-domain conversion."""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from mercury.schemas import (
    InboundEventModel,
    PlannerActionModel,
    SchedulerDecisionModel,
    WorkflowInputModel,
)
from mercury.types import (
    InboundEvent,
    ParseError,
    PlannerAction,
    PlannerActionType,
    SchedulerDecision,
    TaskKind,
    TaskSpec,
    WorkflowSpec,
)


def _to_parse_error(exc: ValidationError) -> ParseError:
    first = exc.errors()[0] if exc.errors() else {"loc": (), "msg": str(exc)}
    loc = ".".join(str(part) for part in first.get("loc", ()))
    msg = first.get("msg", "invalid input")
    return ParseError(msg, path=loc or None)


def _task_from_model(task) -> TaskSpec:
    return TaskSpec(
        id=task.id,
        kind=TaskKind(task.kind),
        target=task.target,
        input=dict(task.input),
        depends_on=tuple(task.depends_on),
        needs_reasoning=task.needs_reasoning,
        max_retries=task.max_retries,
        fallback_output=dict(task.fallback_output)
        if task.fallback_output is not None
        else None,
    )


def _validate_graph(model: WorkflowInputModel) -> None:
    id_to_index: dict[str, int] = {}
    for index, task in enumerate(model.tasks):
        if task.id in id_to_index:
            raise ParseError(f"duplicate task id '{task.id}'", path=f"tasks.{index}.id")
        id_to_index[task.id] = index

    task_ids = set(id_to_index.keys())
    for index, task in enumerate(model.tasks):
        for dep_index, dep_id in enumerate(task.depends_on):
            if dep_id not in task_ids:
                raise ParseError(
                    f"unknown dependency '{dep_id}' for task '{task.id}'",
                    path=f"tasks.{index}.depends_on.{dep_index}",
                )

    edges = {task.id: tuple(task.depends_on) for task in model.tasks}
    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(task_id: str) -> None:
        if task_id in visited:
            return
        if task_id in visiting:
            raise ParseError(
                "cycle detected in task dependencies",
                path=f"tasks.{id_to_index[task_id]}.depends_on",
            )
        visiting.add(task_id)
        for dep in edges[task_id]:
            dfs(dep)
        visiting.remove(task_id)
        visited.add(task_id)

    for task_id in edges:
        dfs(task_id)


def parse_workflow(raw: dict[str, Any]) -> WorkflowSpec:
    try:
        model = WorkflowInputModel.model_validate(raw)
    except ValidationError as exc:
        raise _to_parse_error(exc) from exc

    _validate_graph(model)
    tasks = {task.id: _task_from_model(task) for task in model.tasks}
    return WorkflowSpec(workflow_id=model.workflow_id, tasks=tasks)


def parse_planner_action(raw: dict[str, Any]) -> PlannerAction:
    try:
        model = PlannerActionModel.model_validate(raw)
    except ValidationError as exc:
        raise _to_parse_error(exc) from exc

    return PlannerAction(
        action=PlannerActionType(model.action),
        task_ids=tuple(model.task_ids),
        final_artifact_id=model.final_artifact_id,
    )


def parse_scheduler_decision(raw: dict[str, Any]) -> SchedulerDecision:
    try:
        model = SchedulerDecisionModel.model_validate(raw)
    except ValidationError as exc:
        raise _to_parse_error(exc) from exc

    return SchedulerDecision(task_ids=tuple(model.task_ids), state=model.state)


def parse_inbound_event(raw: dict[str, Any]) -> InboundEvent:
    try:
        model = InboundEventModel.model_validate(raw)
    except ValidationError as exc:
        raise _to_parse_error(exc) from exc

    return InboundEvent(
        source=model.source,
        session_id=model.session_id,
        message=model.message,
        metadata=dict(model.metadata),
        timestamp=model.timestamp,
    )
