"""Helpers for the Convex-backed NLP2SQL cookbook flow."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

COOKBOOK_ROOT = Path(__file__).resolve().parents[1]
if str(COOKBOOK_ROOT) not in sys.path:
    sys.path.insert(0, str(COOKBOOK_ROOT))

from shared.convex_http import ConvexHTTPClient  # noqa: E402

SQL_SCHEMA_FN = os.environ.get("SQL_SCHEMA_FN", "sqlMeta:getSchema")
SQL_EXEC_FN = os.environ.get("SQL_EXEC_FN", "sql:execute")
SQL_LOG_FN = os.environ.get("SQL_LOG_FN", "sql:logQuery")


def heuristic_sql(question: str, schema: str) -> str:
    del schema
    q = question.lower().strip()
    if "count" in q and "users" in q:
        return "SELECT COUNT(*) AS user_count FROM users;"
    if "top" in q and "customers" in q and "revenue" in q:
        return (
            "SELECT customer_id, SUM(amount) AS revenue "
            "FROM orders GROUP BY customer_id ORDER BY revenue DESC LIMIT 10;"
        )
    if "daily" in q and "orders" in q:
        return (
            "SELECT DATE(created_at) AS day, COUNT(*) AS order_count "
            "FROM orders GROUP BY DATE(created_at) ORDER BY day DESC LIMIT 30;"
        )

    # Safe fallback for demos: bounded SELECT only.
    return "SELECT * FROM orders LIMIT 20;"


def render_answer(question: str, sql: str, rows: list[dict[str, Any]]) -> str:
    preview = rows[:5]
    return (
        f"Question: {question}\n"
        f"SQL: {sql}\n"
        f"Rows returned: {len(rows)}\n"
        f"Preview: {preview}"
    )


def make_fetch_schema_tool(convex: ConvexHTTPClient):
    async def fetch_schema(inp: dict[str, Any], ctx) -> dict[str, Any]:
        del inp, ctx
        schema = convex.query(SQL_SCHEMA_FN, {})
        return {"output": {"schema": str(schema)}}

    return fetch_schema


def make_generate_sql_agent():
    async def generate_sql(ctx) -> dict[str, Any]:
        schema = str(ctx.working.get("fetch_schema", {}).get("schema", ""))
        question = str(ctx.input["question"])
        sql = heuristic_sql(question, schema)
        return {"output": {"sql": sql, "question": question, "schema": schema}}

    return generate_sql


def make_execute_sql_tool(convex: ConvexHTTPClient):
    async def execute_sql(inp: dict[str, Any], ctx) -> dict[str, Any]:
        del inp
        sql = str(ctx.working.get("generate_sql", {}).get("sql", ""))
        result = convex.action(SQL_EXEC_FN, {"sql": sql})

        rows: list[dict[str, Any]]
        if isinstance(result, list):
            rows = [r for r in result if isinstance(r, dict)]
        elif isinstance(result, dict) and isinstance(result.get("rows"), list):
            rows = [r for r in result["rows"] if isinstance(r, dict)]
        else:
            rows = []

        return {"output": {"rows": rows, "sql": sql}}

    return execute_sql


def make_summarize_and_log_agent(convex: ConvexHTTPClient):
    async def summarize_and_log(ctx) -> dict[str, Any]:
        question = str(ctx.working.get("generate_sql", {}).get("question", ""))
        sql = str(ctx.working.get("execute_sql", {}).get("sql", ""))
        rows = [dict(row) for row in ctx.working.get("execute_sql", {}).get("rows", [])]

        convex.mutation(
            SQL_LOG_FN,
            {"question": question, "sql": sql, "rowCount": len(rows)},
        )

        return {
            "output": {
                "answer": render_answer(question, sql, rows),
                "sql": sql,
                "rows": rows,
            }
        }

    return summarize_and_log
