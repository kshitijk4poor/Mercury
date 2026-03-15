# Memory and Workspace

## Memory Compartments
- `working`: latest structured task outputs
- `episodic`: append-only list of lifecycle events
- `artifacts`: immutable outputs keyed by artifact ID

`MemoryContext` lives in `mercury/types.py` and is built via `create_memory()` as runs start.

## Workspace Layout
Each workspace contains a `.mercury/` directory with:

- `checkpoints/`
- `traces/`
- `artifacts/`
- `context/`
- `events/`
- `skills/`

`ensure_workspace()` (see `mercury/state.py`) creates the `.mercury/` directory and all subfolders. Today Mercury writes checkpoints, events, and artifacts; `traces`, `context`, and `skills` are available for future integrations.

## Event Journal
Events are appended to `.mercury/events/<run_id>.jsonl` with:

```json
{
  "run_id": "run-...",
  "workflow_id": "...",
  "tick": 12,
  "event_type": "task_transition",
  "payload": {...},
  "timestamp": "..."
}
```

This journal captures every planner, scheduler, and task transition event for observability and resume verification.
