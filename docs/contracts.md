# Contracts

## Planner Contract
- planners must emit `ENQUEUE`, `NOOP`, or `COMPLETE`
- `ENQUEUE` must include at least one ready task ID
- unknown task IDs or violations throw `PlannerContractError`

Mercury ships with `rules` (deterministic order) and `rules_pydanticai` (LLM-augmented reasoning) planners.

Planners operate on `PlannerStateView` (see `mercury/types.py`) and must respect the `PlannerActionType` exhaustiveness enforced by `Mercury.parse_planner_action`.

## Scheduler Contract
- schedulers receive ready task IDs and must pick a subset
- scheduler state is persisted across checkpoints
- violations raise `SchedulerContractError`

Built-in schedulers: `superstep` (in-order) and `ready_queue` (batch with queue state). Scheduler decisions and state pass through `SchedulerDecisionModel` for serialization.

## Sandbox Contract
- sandbox executes handlers via `AgentContext`, `ToolContext`, or `SkillContext`
- `HostSandbox` runs handlers directly; `DockerSandbox` delegates to an injected executor
- handlers must return dict outputs; Mercury snapshots these outputs before persisting

## HITL Contract
- HITL adapters can subscribe to lifecycle event types and return `pause` decisions
- built-in `cli_gate` pauses on subscribed events and can auto-approve

## Inbound Contract
- inbound adapters produce `InboundEvent` instances consumed when the run begins
- adapters implement an async iterator that yields raw events, which Mercury validates via `InboundEventModel`
- these events are appended to `memory.episodic` so handlers can react to external inputs
- sandbox executes handlers via `AgentContext`, `ToolContext`, or `SkillContext`
- `HostSandbox` runs handlers directly; `DockerSandbox` delegates to an injected executor
- handlers must return dict outputs; Mercury snapshots these outputs before persisting
- sandbox configs are typed via Pydantic (`HostSandboxConfig`, `DockerSandboxConfig`)
