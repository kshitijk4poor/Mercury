"""Boundary schemas for parse-first orchestration inputs."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TaskInputModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    kind: Literal["agent", "tool", "skill"]
    target: str = Field(min_length=1)
    input: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    needs_reasoning: bool = False
    max_retries: int = Field(default=0, ge=0)
    fallback_output: dict[str, Any] | None = None


class WorkflowInputModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str = Field(min_length=1)
    tasks: list[TaskInputModel] = Field(min_length=1)


class PlannerActionModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["ENQUEUE", "NOOP", "COMPLETE"]
    task_ids: list[str] = Field(default_factory=list)
    final_artifact_id: str | None = None

    @model_validator(mode="after")
    def validate_shape(self) -> "PlannerActionModel":
        if self.action == "ENQUEUE" and not self.task_ids:
            raise ValueError("ENQUEUE requires non-empty task_ids")
        if self.action != "ENQUEUE" and self.task_ids:
            raise ValueError("task_ids only allowed for ENQUEUE")
        if self.action == "COMPLETE" and not self.final_artifact_id:
            raise ValueError("COMPLETE requires final_artifact_id")
        return self


class SchedulerDecisionModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_ids: list[str] = Field(default_factory=list)
    state: Any = None


class InboundEventModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    message: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: str = Field(min_length=1)


class TaskRecordModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    attempts: int = Field(default=0, ge=0)
    error: str | None = None
    artifact_id: str | None = None


class ArtifactModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifact_id: str
    task_id: str
    data: dict[str, Any]
    timestamp: str


class EventRecordModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: str
    payload: dict[str, Any]
    timestamp: str
    tick: int | None = Field(default=None, ge=0)


class CheckpointModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(default=1)
    run_id: str
    workflow_id: str

    planner_id: str
    planner_config: dict[str, Any] = Field(default_factory=dict)
    scheduler_id: str
    scheduler_config: dict[str, Any] = Field(default_factory=dict)
    scheduler_state: Any = Field(default_factory=dict)
    sandbox_id: str
    sandbox_config: dict[str, Any] = Field(default_factory=dict)
    hitl_id: str | None = None
    hitl_config: dict[str, Any] | None = None

    max_concurrency: int = Field(default=4, ge=1)
    durability_mode: Literal["sync", "async", "exit"] = "sync"
    tick: int = Field(default=0, ge=0)
    final_artifact_id: str | None = None
    cancelled: bool = False
    paused: bool = False
    pending_approval: dict[str, Any] | None = None

    working: dict[str, Any] = Field(default_factory=dict)
    episodic: list[EventRecordModel] = Field(default_factory=list)
    artifacts: dict[str, ArtifactModel] = Field(default_factory=dict)
    task_specs: list[TaskInputModel] = Field(default_factory=list)
    task_records: dict[str, TaskRecordModel] = Field(default_factory=dict)
