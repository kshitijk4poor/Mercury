"""Mercury v2 research/write example flow."""

from __future__ import annotations

import asyncio

from mercury import register_agent, register_skill, run_flow


async def research(ctx):
    await asyncio.sleep(0.01)
    return {"output": {"facts": "fact-1"}}


async def summarize(ctx):
    return {"output": {"summary": ctx.working.get("t1", {})}}


async def write(ctx):
    return {"output": {"draft": f"article:{ctx.working.get('t2', {})}"}}


async def main() -> None:
    register_agent("research-example", research)
    register_skill("summarize-example", summarize)
    register_agent("write-example", write)

    result = await run_flow(
        {
            "workflow_id": "wf-example",
            "tasks": [
                {"id": "t1", "kind": "agent", "target": "research-example"},
                {
                    "id": "t2",
                    "kind": "skill",
                    "target": "summarize-example",
                    "depends_on": ["t1"],
                },
                {
                    "id": "t3",
                    "kind": "agent",
                    "target": "write-example",
                    "depends_on": ["t2"],
                    "needs_reasoning": True,
                },
            ],
        },
        planner_id="rules",
        scheduler_id="superstep",
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
