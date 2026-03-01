"""State helpers for memory, workspace, and checkpoints."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from mercury.schemas import ArtifactModel, CheckpointModel, EventRecordModel
from mercury.types import ArtifactRecord, EventRecord, MemoryContext, ParseError


WORKSPACE_DIRS = (
    "checkpoints",
    "traces",
    "artifacts",
    "context",
    "events",
    "skills",
)


@dataclass(frozen=True)
class WorkspacePaths:
    root: Path
    checkpoints: Path
    traces: Path
    artifacts: Path
    context: Path
    events: Path
    skills: Path


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def create_memory(initial_working: dict | None = None) -> MemoryContext:
    return MemoryContext(working=dict(initial_working or {}))


def add_event(
    memory: MemoryContext,
    event_type: str,
    payload: dict,
    *,
    timestamp: str | None = None,
    tick: int | None = None,
) -> EventRecord:
    record = EventRecord(
        event_type=event_type,
        payload=dict(payload),
        timestamp=timestamp or utc_now(),
        tick=tick,
    )
    memory.episodic.append(record)
    return record


def add_artifact(
    memory: MemoryContext,
    task_id: str,
    data: dict,
    *,
    artifact_id: str | None = None,
    timestamp: str | None = None,
) -> ArtifactRecord:
    artifact_key = artifact_id or f"{task_id}-{len(memory.artifacts) + 1}"
    record = ArtifactRecord(
        artifact_id=artifact_key,
        task_id=task_id,
        data=dict(data),
        timestamp=timestamp or utc_now(),
    )
    memory.artifacts[artifact_key] = record
    return record


def ensure_workspace(root: str | Path) -> WorkspacePaths:
    base = Path(root).expanduser().resolve() / ".mercury"
    base.mkdir(parents=True, exist_ok=True)
    folders: dict[str, Path] = {}
    for name in WORKSPACE_DIRS:
        path = base / name
        path.mkdir(parents=True, exist_ok=True)
        folders[name] = path
    return WorkspacePaths(
        root=base,
        checkpoints=folders["checkpoints"],
        traces=folders["traces"],
        artifacts=folders["artifacts"],
        context=folders["context"],
        events=folders["events"],
        skills=folders["skills"],
    )


def checkpoint_to_model(
    *,
    run_id: str,
    workflow_id: str,
    planner_id: str,
    planner_config: dict[str, Any],
    scheduler_id: str,
    scheduler_config: dict[str, Any],
    scheduler_state: Any,
    sandbox_id: str,
    sandbox_config: dict[str, Any],
    hitl_id: str | None,
    hitl_config: dict[str, Any] | None,
    max_concurrency: int,
    durability_mode: str,
    tick: int,
    final_artifact_id: str | None,
    cancelled: bool,
    paused: bool,
    pending_approval: dict[str, Any] | None,
    working: dict,
    episodic: list[EventRecord],
    artifacts: dict[str, ArtifactRecord],
    task_specs: list,
    task_records: dict,
) -> CheckpointModel:
    return CheckpointModel(
        version=1,
        run_id=run_id,
        workflow_id=workflow_id,
        planner_id=planner_id,
        planner_config=dict(planner_config),
        scheduler_id=scheduler_id,
        scheduler_config=dict(scheduler_config),
        scheduler_state=scheduler_state,
        sandbox_id=sandbox_id,
        sandbox_config=dict(sandbox_config),
        hitl_id=hitl_id,
        hitl_config=dict(hitl_config) if hitl_config is not None else None,
        max_concurrency=max_concurrency,
        durability_mode=durability_mode,
        tick=tick,
        final_artifact_id=final_artifact_id,
        cancelled=cancelled,
        paused=paused,
        pending_approval=dict(pending_approval) if pending_approval else None,
        working=dict(working),
        episodic=[
            EventRecordModel(
                event_type=e.event_type,
                payload=e.payload,
                timestamp=e.timestamp,
                tick=e.tick,
            )
            for e in episodic
        ],
        artifacts={
            key: ArtifactModel(
                artifact_id=value.artifact_id,
                task_id=value.task_id,
                data=value.data,
                timestamp=value.timestamp,
            )
            for key, value in artifacts.items()
        },
        task_specs=task_specs,
        task_records=task_records,
    )


def memory_from_checkpoint(checkpoint: CheckpointModel) -> MemoryContext:
    return MemoryContext(
        working=dict(checkpoint.working),
        episodic=[
            EventRecord(
                event_type=event.event_type,
                payload=dict(event.payload),
                timestamp=event.timestamp,
                tick=event.tick,
            )
            for event in checkpoint.episodic
        ],
        artifacts={
            artifact_id: ArtifactRecord(
                artifact_id=artifact.artifact_id,
                task_id=artifact.task_id,
                data=dict(artifact.data),
                timestamp=artifact.timestamp,
            )
            for artifact_id, artifact in checkpoint.artifacts.items()
        },
    )


def save_checkpoint(checkpoint: CheckpointModel, path: str | Path) -> Path:
    checkpoint_path = Path(path)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(checkpoint.model_dump_json(indent=2), encoding="utf-8")
    return checkpoint_path


def append_event_journal(
    path: str | Path,
    *,
    run_id: str,
    workflow_id: str,
    tick: int | None,
    event: EventRecord,
) -> Path:
    event_path = Path(path)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    event_tick = tick if tick is not None else event.tick
    if event_tick is None:
        event_tick = 0
    payload = {
        "run_id": run_id,
        "workflow_id": workflow_id,
        "tick": event_tick,
        "event_type": event.event_type,
        "payload": event.payload,
        "timestamp": event.timestamp,
    }
    with event_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
    return event_path


def load_checkpoint(path: str | Path, *, expected_version: int = 1) -> CheckpointModel:
    checkpoint_path = Path(path)
    try:
        model = CheckpointModel.model_validate_json(
            checkpoint_path.read_text(encoding="utf-8")
        )
    except (ValidationError, OSError) as exc:
        raise ParseError("invalid checkpoint payload") from exc

    if model.version != expected_version:
        raise ParseError(
            f"unsupported checkpoint version {model.version}, expected {expected_version}",
            path="version",
        )
    return model
