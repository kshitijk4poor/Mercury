# Runtime Model

## Task Lifecycle

Mercury tracks each task through a status machine: `pending`, `running`, `succeeded`, `failed`, `cancelled`, `blocked`, `paused`. When dependencies succeed the task becomes ready, the planner enqueues it, the scheduler orders it, and the sandbox (via `AgentContext`/`ToolContext`/`SkillContext`) executes the handler. `_execute_task` snapshots inputs/working state, validates handler output, stores artifacts, updates working, and emits lifecycle events for auditing.

## Retries and Blocking

- tasks retry with exponential backoff based on their retry count
- fallback outputs can resolve failures without blocking dependents
- dependents of failed tasks become `blocked`

## Durability Modes

Runs can opt into `sync`, `async`, or `exit` durability. Checkpoints are written through `_CheckpointWriter`, which ensures:

- `sync`: every checkpoint writes before continuing
- `async`: writes happen in a background task but we eventually flush
- `exit`: last checkpoint is durable before exit

## Checkpoint and Resume

Each run persists a checkpoint containing:

- workflow metadata (planner/scheduler/sandbox/HITL configs)
- memory (`working`, `episodic`, `artifacts`)
- scheduler state

Mercury serializes checkpoints through Pydantic (`CheckpointModel`) so that resuming can reconstitute plugin configs, scheduler state, and checkpoint metadata deterministically.

Checkpoints serialize through Pydantic and are written into `<workspace>/.mercury/checkpoints`. When resuming, Mercury restores scheduler state, plugin configs, and workspace paths for deterministic continuation.

## Event Journal

Every lifecycle event (planner decisions, task transitions, HITL actions) is appended to `.mercury/events/<run_id>.jsonl` as JSON. The journal provides an audit trail for debugging and pause/resume scenarios.
