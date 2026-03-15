# Mercury

Mercury lets you build agent workflows your way.

Compose agents, tools, and skills in a workflow, configure the runtime however you need, and let Mercury handle checkpoints, retries, resume, and execution semantics underneath.

## Why Mercury

Most workflow systems make you choose between ease of use and runtime control.
Mercury is built to give you both:

- A simple workflow model built from agents, tools, and skills.
- A runtime that handles the hard parts underneath.
- Configurable execution when you need more control.
- A stable core you can build on without locking into one stack.

Mercury is for teams that want workflows to stay easy to author while the runtime takes responsibility for the heavy lifting.

## Core Mental Model

A Mercury workflow is a graph of tasks.

Each task is one of:

- `agent`
- `tool`
- `skill`

Tasks can depend on earlier tasks.
Mercury figures out what is ready to run, executes tasks in order, tracks outputs, records events, and persists enough state to resume later.

You can start with the workflow itself and ignore deeper runtime controls until you need them.

## Quick Start

### Install

```bash
uv venv --python 3.12
uv sync --extra dev
```

### Define a workflow

```python
import asyncio

from mercury import register_tool, run_flow


async def echo_tool(inp, ctx):
    return {"output": {"text": inp["text"], "task_id": ctx.task_id}}


async def main():
    register_tool("echo_tool", echo_tool)

    result = await run_flow(
        {
            "workflow_id": "hello-flow",
            "tasks": [
                {
                    "id": "task_a",
                    "kind": "tool",
                    "target": "echo_tool",
                    "input": {"text": "hello mercury"},
                }
            ],
        },
        planner_id="rules",
        workspace=".",
    )
    print(result)


asyncio.run(main())
```

### CLI

Run:

```bash
mercury run \
  --workflow workflow.json \
  --planner-id rules \
  --workspace .
```

Resume:

```bash
mercury resume --checkpoint .mercury/checkpoints/<run_id>.json
```

Inspect:

```bash
mercury inspect --checkpoint .mercury/checkpoints/<run_id>.json --json
```

## Cookbook

Mercury includes cookbook-style examples that show how product use cases map onto the same runtime:

- [research_write.py](packages/mercury-examples/examples/research_write.py): simple research and writing workflow
- [examples/cookbook/rag/flow.py](examples/cookbook/rag/flow.py): retrieval-augmented workflow backed by Convex
- [examples/cookbook/nlp2sql/flow.py](examples/cookbook/nlp2sql/flow.py): text-to-SQL workflow backed by Convex
- [examples/cookbook/README.md](examples/cookbook/README.md): cookbook setup and run instructions

The cookbook is the main place to see how Mercury maps to real use cases rather than abstract patterns.

## Philosophy

Mercury is designed around a simple idea:

- Users should think in workflows, not runtime internals.
- Workflows should be easy to compose from agents, tools, and skills.
- Runtime control should be available without becoming mandatory.
- The system should stay configurable without forcing one planner, scheduler, sandbox, model stack, or tool stack.

In practice that means Mercury aims to feel lightweight at the surface while taking responsibility for the difficult runtime behavior underneath.

## What Mercury Handles For You

Mercury keeps the runtime burden in the engine so workflow code can stay focused on behavior.

Built-in runtime responsibilities include:

- dependency-aware task execution
- retries with exponential backoff
- fallback outputs
- failure propagation to dependent tasks
- checkpoint and resume
- cancellation
- append-only event journaling
- scheduler state restoration on resume
- contract enforcement around planners and schedulers

## Architecture

Mercury is kernel-first internally.
The kernel is the source of truth for execution correctness, while runtime behavior remains configurable.

Kernel responsibilities:

- parse and validate workflow boundaries
- maintain run state and task lifecycle transitions
- own retries, blocking, cancellation, checkpointing, and resume
- enforce planner, scheduler, and runtime contracts
- persist checkpoints and event journals

Extension responsibilities:

- handlers implement business behavior
- planners decide what to enqueue and when to complete
- schedulers choose among ready task IDs
- runtime plugins shape execution policy around the kernel

This split is what lets Mercury stay simple to use while remaining deeply configurable.

## Advanced Runtime Controls

When you need more control, Mercury lets you configure runtime behavior per run.

Current runtime controls include:

- `planner_id` + `planner_config`
- `scheduler_id` + `scheduler_config`
- `sandbox_id` + `sandbox_config`
- `hitl_id` + `hitl_config`
- `inbound_adapter_id` + `inbound_adapter_config`
- `max_concurrency`
- `durability_mode` (`sync`, `async`, `exit`)

Built-in adapters today:

- planners: `rules`, `rules_pydanticai`
- schedulers: `superstep`, `ready_queue`
- sandboxes: `host`, `docker`
- hitl: `none`, `cli_gate`

These are runtime controls, not the beginner mental model.

## Public API

From `mercury`:

- `run_flow(...) -> RunResult`
- `resume_flow(...) -> RunResult`
- `inspect_run(checkpoint_path) -> dict`
- `cancel_run(run_id) -> None`
- registrations:
  - `register_agent`
  - `register_tool`
  - `register_skill`
  - `register_planner`
  - `register_scheduler`
  - `register_sandbox`
  - `register_hitl`
  - `register_inbound_adapter`
  - `register_hook`

## Memory and Workspace Model

Canonical memory compartments:

- `working`: latest structured outputs for runtime lookups
- `episodic`: append-only lifecycle and event records
- `artifacts`: immutable task outputs keyed by artifact ID

Workspace layout under `<workspace>/.mercury/`:

- `checkpoints/`
- `traces/`
- `artifacts/`
- `context/`
- `events/`
- `skills/`

Event journal contract:

- path: `.mercury/events/<run_id>.jsonl`
- one JSON object per line with:
  - `run_id`
  - `workflow_id`
  - `tick`
  - `event_type`
  - `payload`
  - `timestamp`

## Extending Mercury

To add a custom adapter or handler:

1. Implement the contract.
2. Register it by ID.
3. Reference that ID from `run_flow(...)` or the CLI.

Mercury keeps the kernel stable while letting the surrounding runtime evolve.

## Future Work

Mercury's near-term direction is to make the runtime more capable without making the user model heavier.

Planned areas include:

- more cookbook coverage for product use cases
- real Docker-backed sandboxing
- more meaningful `traces/` and `context/` outputs
- stronger productized entrypoints beyond the current CLI/runtime surface
- retrieval-oriented memory integrations that remain adapter/config driven
- richer reasoning scratchpads with checkpoint-aware persistence
- deeper runtime surfaces for pause/resume and human review flows
