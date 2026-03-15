"""Cookbook: NLP-to-SQL agent flow with Convex as execution backend.

Expected Convex public functions:
- query  `sqlMeta:getSchema`  args: {}
- action `sql:execute`        args: { sql: string }
- mutation `sql:logQuery`     args: { question: string, sql: string, rowCount: number }
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


def build_workflow(question: str) -> dict:
    return {
        "workflow_id": "cookbook-nlp2sql-convex",
        "tasks": [
            {"id": "fetch_schema", "kind": "tool", "target": "cookbook_sql_schema"},
            {
                "id": "generate_sql",
                "kind": "agent",
                "target": "cookbook_sql_generate",
                "depends_on": ["fetch_schema"],
                "input": {"question": question},
                "needs_reasoning": True,
            },
            {
                "id": "execute_sql",
                "kind": "tool",
                "target": "cookbook_sql_execute",
                "depends_on": ["generate_sql"],
            },
            {
                "id": "summarize",
                "kind": "agent",
                "target": "cookbook_sql_summarize",
                "depends_on": ["execute_sql"],
            },
        ],
    }


async def main() -> None:
    from mercury import inspect_run, register_agent, register_tool, run_flow

    from helpers import (
        make_execute_sql_tool,
        make_fetch_schema_tool,
        make_generate_sql_agent,
        make_summarize_and_log_agent,
    )
    from shared.convex_http import ConvexHTTPClient, ConvexSettings

    parser = argparse.ArgumentParser(description="Mercury + Convex NLP2SQL cookbook")
    parser.add_argument("--question", required=True)
    parser.add_argument("--workspace", default=".")
    args = parser.parse_args()

    convex = ConvexHTTPClient(ConvexSettings.from_env())

    register_tool("cookbook_sql_schema", make_fetch_schema_tool(convex))
    register_agent("cookbook_sql_generate", make_generate_sql_agent())
    register_tool("cookbook_sql_execute", make_execute_sql_tool(convex))
    register_agent("cookbook_sql_summarize", make_summarize_and_log_agent(convex))

    result = await run_flow(
        build_workflow(args.question),
        planner_id="rules",
        scheduler_id="ready_queue",
        scheduler_config={"batch_size": 1},
        workspace=args.workspace,
    )
    snapshot = inspect_run(result.checkpoint_path)
    final = snapshot["artifacts"][result.final_artifact_id]["data"]

    print("run_id:", result.run_id)
    print("status:", result.status)
    print("sql:", final.get("sql", ""))
    print("answer:\n", final.get("answer", ""))


if __name__ == "__main__":
    asyncio.run(main())
