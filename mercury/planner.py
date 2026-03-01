"""Planner implementations for Mercury."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from mercury.parse import parse_planner_action
from mercury.schemas import PlannerActionModel
from mercury.types import (
    PlannerAction,
    PlannerActionType,
    TaskSpec,
    TaskStatus,
)

FAILED_STATES = {TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.BLOCKED}


@dataclass
class PlannerStateView:
    run_id: str
    workflow_id: str
    final_artifact_id: str | None
    task_specs: dict[str, TaskSpec]
    task_statuses: dict[str, TaskStatus]


def _deps_satisfied(task: TaskSpec, statuses: dict[str, TaskStatus]) -> bool:
    return all(statuses[dep] == TaskStatus.SUCCEEDED for dep in task.depends_on)


def _has_failed_dependency(task: TaskSpec, statuses: dict[str, TaskStatus]) -> bool:
    return any(statuses[dep] in FAILED_STATES for dep in task.depends_on)


class RulePlanner:
    async def plan(self, state: PlannerStateView) -> PlannerAction:
        pending = [
            t
            for t in state.task_specs.values()
            if state.task_statuses[t.id] == TaskStatus.PENDING
        ]
        running = any(
            status == TaskStatus.RUNNING for status in state.task_statuses.values()
        )
        if not pending and not running:
            return PlannerAction(
                action=PlannerActionType.COMPLETE,
                final_artifact_id=state.final_artifact_id or "final-artifact-missing",
            )

        runnable = [
            t
            for t in pending
            if not _has_failed_dependency(t, state.task_statuses)
            and _deps_satisfied(t, state.task_statuses)
        ]
        if not runnable:
            return PlannerAction(action=PlannerActionType.NOOP)
        return PlannerAction(action=PlannerActionType.ENQUEUE, tasks=tuple(runnable))


class HybridPlanner:
    def __init__(self, planner_model: Any) -> None:
        self._rule = RulePlanner()
        self._planner_model = planner_model

    async def _call_reasoning_model(
        self, state: PlannerStateView, reasoning_tasks: list[TaskSpec]
    ) -> PlannerAction:
        payload = {
            "run_id": state.run_id,
            "workflow_id": state.workflow_id,
            "tasks": [
                {
                    "id": task.id,
                    "kind": task.kind.value,
                    "target": task.target,
                    "input": task.input,
                    "depends_on": list(task.depends_on),
                    "needs_reasoning": task.needs_reasoning,
                    "max_retries": task.max_retries,
                }
                for task in reasoning_tasks
            ],
        }

        if hasattr(self._planner_model, "mercury_plan"):
            raw = await self._planner_model.mercury_plan(payload)
            return parse_planner_action(raw)

        # Optional direct PydanticAI path when no helper method is provided.
        from pydantic_ai import Agent

        agent = Agent(model=self._planner_model, result_type=PlannerActionModel)
        result = await agent.run(json.dumps(payload))
        data = getattr(result, "data", result)
        if isinstance(data, PlannerActionModel):
            raw = data.model_dump(mode="json")
        else:
            raw = data
        return parse_planner_action(raw)

    async def plan(self, state: PlannerStateView) -> PlannerAction:
        base = await self._rule.plan(state)
        if base.action != PlannerActionType.ENQUEUE:
            return base

        non_reasoning = [t for t in base.tasks if not t.needs_reasoning]
        reasoning = [t for t in base.tasks if t.needs_reasoning]
        if not reasoning:
            return base
        if self._planner_model is None:
            return base

        model_action = await self._call_reasoning_model(state, reasoning)
        if model_action.action == PlannerActionType.NOOP and non_reasoning:
            return PlannerAction(
                action=PlannerActionType.ENQUEUE, tasks=tuple(non_reasoning)
            )
        if model_action.action == PlannerActionType.ENQUEUE:
            combined = list(non_reasoning)
            # model is only allowed to schedule agent/tool/skill tasks
            combined.extend(model_action.tasks)
            return PlannerAction(
                action=PlannerActionType.ENQUEUE, tasks=tuple(combined)
            )
        return model_action
