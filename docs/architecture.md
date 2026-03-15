# Architecture

Mercury remains kernel-first internally. This document explains how the runtime separates the kernel from configurable adapters and how execution flows through checkpoints, events, and plugin seams.

## Kernel Responsibilities
- parse and validate workflow boundaries
- maintain run state and task lifecycle transitions
- enforce planner, scheduler, and runtime contracts
- own retries, blocking, cancellation, checkpointing, and resume
- persist checkpoints and event journals

## Extension Responsibilities
- handlers implement business behavior (agents/tools/skills)
- planners decide what to enqueue and when to complete
- schedulers choose among ready task IDs
- runtime plugins shape execution policy around the kernel

## Plugin Resolution
Mercury resolves planners, schedulers, sandboxes, and HITL adapters from `RuntimeRegistry` (see `mercura/runtime.py`). Each run calls `_resolve_plugins`, which types the configs, snapshots them for checkpoints, and keeps typed adapters (`_ResolvedPlugins`) ready for the tick loop.

## Execution Topology
```mermaid
flowchart LR
    U[CLI / App / SDK] --> K

    subgraph K[Mercury Kernel]
      P[Parse Boundary]
      L[Runtime Loop]
      S[State Machine]
      C[Checkpoint / Resume]
      J[Event Journal + Hooks]
      P --> L --> S --> C
      L --> J
    end

    subgraph E[Injectables: BYO or Built-in]
      H[Handlers: Agents / Tools / Skills]
      A1[Planner Adapter]
      A2[Scheduler Adapter]
      A3[Sandbox Adapter]
      A4[HITL Adapter]
      A5[Inbound Adapter]
    end

    L --> H
    L <--> A1
    L <--> A2
    L <--> A3
    L <--> A4
    L <--> A5

    subgraph W[Workspace: .mercury/]
      W1[checkpoints]
      W2[events]
      W3[artifacts]
      W4[context]
      W5[traces]
      W6[skills]
    end

    C --> W1
    J --> W2
    L --> W3
    L --> W4
    L --> W5
    L --> W6
```

## Runtime Tick Lifecycle
```mermaid
flowchart TD
    A[Parse workflow + run config] --> B[Resolve adapters from registries]
    B --> C[Initialize run-state + first checkpoint]
    C --> D[Tick loop]
    D --> E[Call planner adapter]
    E --> F[Validate planner action]
    F --> G[Compute ready task IDs]
    G --> H[Call scheduler adapter]
    H --> I[Execute selected tasks via sandbox]
    I --> J[Apply transitions, retries, blocking, artifacts, memory]
    J --> K[Emit events and optional HITL gate]
    K --> L[Persist checkpoint per durability mode]
    L --> M{Complete / Pause / Cancel?}
    M -- no --> D
    M -- yes --> N[Finalize and return RunResult]
```

## Planner / Scheduler Contract Boundary
```mermaid
flowchart LR
    K[Kernel]
    P[Planner Adapter]
    S[Scheduler Adapter]

    K -->|state_view| P
    P -->|PlannerAction| K
    K -->|ready_task_ids + state_view + scheduler_state| S
    S -->|SchedulerDecision: task_ids subset of ready| K
```

The diagrams synthesize how Mercury keeps the kernel small while letting adapters drive behavior.

## Checkpoint Writer
Mercury wraps checkpoint persistence inside `_CheckpointWriter` (`mercury/runtime.py:_CheckpointWriter`). It flushes checkpoints synchronously for `sync`/forced writes and batches writes asynchronously (or skips them in `exit` mode) while still saving the latest data before termination.
