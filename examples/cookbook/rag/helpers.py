"""Helpers for the Convex-backed RAG cookbook flow."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

COOKBOOK_ROOT = Path(__file__).resolve().parents[1]
if str(COOKBOOK_ROOT) not in sys.path:
    sys.path.insert(0, str(COOKBOOK_ROOT))

from shared.convex_http import ConvexHTTPClient  # noqa: E402

RAG_SEARCH_FN = os.environ.get("RAG_SEARCH_FN", "rag:search")
RAG_LOG_FN = os.environ.get("RAG_LOG_FN", "rag:logAnswer")


def normalize_hits(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []

    hits: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        hits.append(
            {
                "id": str(item.get("id", "unknown")),
                "text": text,
                "score": float(item.get("score", 0.0)),
            }
        )
    return hits


def synthesize_answer(question: str, hits: list[dict[str, Any]]) -> dict[str, Any]:
    if not hits:
        return {
            "answer": "I could not find relevant context in Convex.",
            "citations": [],
        }

    context_lines = [f"[{h['id']}] {h['text']}" for h in hits[:5]]
    answer = f"Question: {question}\n\nGrounded Answer:\n" + "\n".join(
        f"- {line}" for line in context_lines
    )
    return {
        "answer": answer,
        "citations": [h["id"] for h in hits[:5]],
    }


def make_retrieve_context_tool(convex: ConvexHTTPClient):
    async def retrieve_context(inp: dict[str, Any], ctx) -> dict[str, Any]:
        del ctx
        raw = convex.query(
            RAG_SEARCH_FN,
            {"query": str(inp["question"]), "limit": int(inp["top_k"])},
        )
        return {"output": {"hits": normalize_hits(raw)}}

    return retrieve_context


def make_compose_answer_agent():
    async def compose_answer(ctx) -> dict[str, Any]:
        retrieved = ctx.working.get("retrieve_context", {})
        hits = list(retrieved.get("hits", []))
        answer = synthesize_answer(str(ctx.input["question"]), hits)
        return {"output": answer}

    return compose_answer


def make_persist_answer_tool(convex: ConvexHTTPClient):
    async def persist_answer(inp: dict[str, Any], ctx) -> dict[str, Any]:
        answer_payload = dict(ctx.working.get("compose_answer", {}))
        convex.mutation(
            RAG_LOG_FN,
            {
                "question": str(inp["question"]),
                "answer": str(answer_payload.get("answer", "")),
                "citations": list(answer_payload.get("citations", [])),
            },
        )
        return {"output": {"logged": True, **answer_payload}}

    return persist_answer
