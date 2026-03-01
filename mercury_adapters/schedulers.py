"""Built-in scheduler adapters."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from mercury.types import PlannerStateView, SchedulerDecision


class SuperstepSchedulerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SuperstepScheduler:
    def parse_config(self, raw):
        return SuperstepSchedulerConfig.model_validate(raw or {})

    def init_state(self, config):
        del config
        return {}

    def parse_state(self, raw):
        return dict(raw or {})

    async def pick(
        self, ready_task_ids, state_view: PlannerStateView, scheduler_state, config
    ):
        del scheduler_state, config
        ordered = [
            task_id for task_id in state_view.task_order if task_id in ready_task_ids
        ]
        return SchedulerDecision(task_ids=tuple(ordered), state={})


class ReadyQueueSchedulerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_size: int | None = Field(default=None, ge=1)


class ReadyQueueSchedulerState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    queue: list[str] = Field(default_factory=list)


class ReadyQueueScheduler:
    def parse_config(self, raw):
        return ReadyQueueSchedulerConfig.model_validate(raw or {})

    def init_state(self, config):
        del config
        return ReadyQueueSchedulerState().model_dump(mode="json")

    def parse_state(self, raw):
        return ReadyQueueSchedulerState.model_validate(raw or {}).model_dump(
            mode="json"
        )

    async def pick(
        self, ready_task_ids, state_view: PlannerStateView, scheduler_state, config
    ):
        del state_view
        state = ReadyQueueSchedulerState.model_validate(scheduler_state or {})
        ready_set = set(ready_task_ids)
        queue = [task_id for task_id in state.queue if task_id in ready_set]
        queued = set(queue)
        for task_id in ready_task_ids:
            if task_id not in queued:
                queue.append(task_id)
                queued.add(task_id)

        if not queue:
            return SchedulerDecision(task_ids=(), state=state.model_dump(mode="json"))

        n = config.batch_size if config.batch_size is not None else len(queue)
        selected = tuple(queue[:n])
        remaining = queue[n:]
        next_state = ReadyQueueSchedulerState(queue=remaining)
        return SchedulerDecision(
            task_ids=selected,
            state=next_state.model_dump(mode="json"),
        )
