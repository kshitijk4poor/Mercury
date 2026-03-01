"""Built-in planner adapters."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict

from mercury.parse import parse_planner_action
from mercury.schemas import PlannerActionModel
from mercury.types import PlannerAction, PlannerActionType, PlannerStateView, TaskStatus

FAILED_STATES = {TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.BLOCKED}


class RulesPlannerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RulesPlanner:
    def parse_config(self, raw):
        return RulesPlannerConfig.model_validate(raw or {})

    async def plan(self, state_view: PlannerStateView, config: RulesPlannerConfig):
        del config
        pending = [
            task_id
            for task_id in state_view.task_order
            if state_view.task_statuses[task_id] == TaskStatus.PENDING
        ]
        running = any(
            status == TaskStatus.RUNNING for status in state_view.task_statuses.values()
        )
        if not pending and not running:
            return PlannerAction(
                action=PlannerActionType.COMPLETE,
                final_artifact_id=state_view.final_artifact_id
                or "final-artifact-missing",
            )

        runnable: list[str] = []
        for task_id in pending:
            task = state_view.task_specs[task_id]
            if any(
                state_view.task_statuses[dep] in FAILED_STATES
                for dep in task.depends_on
            ):
                continue
            if all(
                state_view.task_statuses[dep] == TaskStatus.SUCCEEDED
                for dep in task.depends_on
            ):
                runnable.append(task_id)

        if not runnable:
            return PlannerAction(action=PlannerActionType.NOOP)
        return PlannerAction(action=PlannerActionType.ENQUEUE, task_ids=tuple(runnable))


class RulesPydanticAIPlannerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    model: Any | None = None
    helper_method: str = "mercury_plan"


class RulesPydanticAIPlanner:
    def __init__(self) -> None:
        self._rules = RulesPlanner()

    def parse_config(self, raw):
        cfg = RulesPydanticAIPlannerConfig.model_validate(raw or {})
        if isinstance(cfg.model, str) and cfg.model.startswith("<non-serializable:"):
            return cfg.model_copy(update={"model": None})
        return cfg

    async def _call_model(
        self,
        state_view: PlannerStateView,
        task_ids: list[str],
        config: RulesPydanticAIPlannerConfig,
    ) -> PlannerAction:
        payload = {
            "run_id": state_view.run_id,
            "workflow_id": state_view.workflow_id,
            "tasks": [
                {
                    "id": task_id,
                    "kind": state_view.task_specs[task_id].kind.value,
                    "target": state_view.task_specs[task_id].target,
                    "input": state_view.task_specs[task_id].input,
                    "depends_on": list(state_view.task_specs[task_id].depends_on),
                    "needs_reasoning": state_view.task_specs[task_id].needs_reasoning,
                    "max_retries": state_view.task_specs[task_id].max_retries,
                }
                for task_id in task_ids
            ],
        }

        model = config.model
        if model is None:
            return PlannerAction(action=PlannerActionType.NOOP)

        if hasattr(model, config.helper_method):
            raw = await getattr(model, config.helper_method)(payload)
            return parse_planner_action(raw)

        from pydantic_ai import Agent

        agent = Agent(model=model, result_type=PlannerActionModel)
        result = await agent.run(json.dumps(payload))
        data = getattr(result, "data", result)
        if isinstance(data, PlannerActionModel):
            raw = data.model_dump(mode="json")
        else:
            raw = data
        return parse_planner_action(raw)

    async def plan(
        self, state_view: PlannerStateView, config: RulesPydanticAIPlannerConfig
    ):
        base = await self._rules.plan(state_view, RulesPlannerConfig())
        if base.action != PlannerActionType.ENQUEUE:
            return base
        if config.model is None:
            return base

        reasoning_ids = [
            task_id
            for task_id in base.task_ids
            if state_view.task_specs[task_id].needs_reasoning
        ]
        if not reasoning_ids:
            return base

        non_reasoning = [
            task_id for task_id in base.task_ids if task_id not in reasoning_ids
        ]
        model_action = await self._call_model(state_view, reasoning_ids, config)

        if model_action.action == PlannerActionType.NOOP:
            if non_reasoning:
                return PlannerAction(
                    action=PlannerActionType.ENQUEUE,
                    task_ids=tuple(non_reasoning),
                )
            return model_action

        if model_action.action == PlannerActionType.ENQUEUE:
            merged: list[str] = []
            for task_id in [*non_reasoning, *model_action.task_ids]:
                if task_id not in merged:
                    merged.append(task_id)
            return PlannerAction(
                action=PlannerActionType.ENQUEUE,
                task_ids=tuple(merged),
            )

        return model_action
