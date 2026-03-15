"""Cookbook: simple RAG flow on top of Mercury using Convex as the store.

Expected Convex public functions:
- query    `rag:search`       args: { query: string, limit?: number }
- mutation `rag:logAnswer`    args: { question: string, answer: string, citations: string[] }
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
COOKBOOK_ROOT = Path(__file__).resolve().parents[1]
if str(COOKBOOK_ROOT) not in sys.path:
    sys.path.insert(0, str(COOKBOOK_ROOT))


def build_workflow(question: str, top_k: int) -> dict:
    return {
        "workflow_id": "cookbook-rag-convex",
        "tasks": [
            {
                "id": "retrieve_context",
                "kind": "tool",
                "target": "cookbook_rag_retrieve",
                "input": {"question": question, "top_k": top_k},
            },
            {
                "id": "compose_answer",
                "kind": "agent",
                "target": "cookbook_rag_compose",
                "depends_on": ["retrieve_context"],
                "input": {"question": question},
            },
            {
                "id": "persist_answer",
                "kind": "tool",
                "target": "cookbook_rag_persist",
                "depends_on": ["compose_answer"],
                "input": {"question": question},
            },
        ],
    }


async def main() -> None:
    from mercury import inspect_run, register_agent, register_tool, run_flow

    from helpers import (
        make_compose_answer_agent,
        make_persist_answer_tool,
        make_retrieve_context_tool,
    )
    from shared.convex_http import ConvexHTTPClient, ConvexSettings

    parser = argparse.ArgumentParser(description="Mercury + Convex RAG cookbook")
    parser.add_argument("--question", required=True)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--workspace", default=".")
    args = parser.parse_args()

    convex = ConvexHTTPClient(ConvexSettings.from_env())

    register_tool("cookbook_rag_retrieve", make_retrieve_context_tool(convex))
    register_agent("cookbook_rag_compose", make_compose_answer_agent())
    register_tool("cookbook_rag_persist", make_persist_answer_tool(convex))

    result = await run_flow(
        build_workflow(args.question, args.top_k),
        planner_id="rules",
        scheduler_id="superstep",
        workspace=args.workspace,
    )
    snapshot = inspect_run(result.checkpoint_path)
    final = snapshot["artifacts"][result.final_artifact_id]["data"]

    print("run_id:", result.run_id)
    print("status:", result.status)
    print("answer:\n", final.get("answer", ""))
    print("citations:", ", ".join(final.get("citations", [])))


if __name__ == "__main__":
    asyncio.run(main())
