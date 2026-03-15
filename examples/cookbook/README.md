# Mercury Cookbook: Convex-backed RAG + NLP2SQL

These examples show how to build task graphs on Mercury where Convex is the backing data layer.

## Files

- `shared/convex_http.py`: tiny Convex HTTP API wrapper used by cookbook flows
- `rag/flow.py`: RAG workflow entrypoint
- `rag/helpers.py`: RAG helper functions and task factories
- `nlp2sql/flow.py`: NLP2SQL workflow entrypoint
- `nlp2sql/helpers.py`: NLP2SQL helper functions and task factories

## Prerequisites

1. Convex deployment URL
2. Public Convex functions (or authenticated functions with token)
3. Environment variables:

```bash
export CONVEX_URL="https://your-deployment.convex.cloud"
# Optional: for protected functions
export CONVEX_ACCESS_TOKEN="..."
```

## Convex function contracts used by examples

### RAG

- Query: `rag:search`

Input:
```json
{ "query": "what is mercury", "limit": 5 }
```

Output (example):
```json
[
  { "id": "doc_1", "text": "Mercury is a minimal DAG runtime", "score": 0.92 },
  { "id": "doc_2", "text": "Schedulers are swappable via registry", "score": 0.78 }
]
```

- Mutation: `rag:logAnswer`

Input:
```json
{
  "question": "...",
  "answer": "...",
  "citations": ["doc_1", "doc_2"]
}
```

### NLP2SQL

- Query: `sqlMeta:getSchema`
- Action: `sql:execute` with `{ "sql": "SELECT ..." }`
- Mutation: `sql:logQuery` with `{ "question": "...", "sql": "...", "rowCount": 10 }`

`sql:execute` may call into your own SQL proxy logic inside Convex (for example, querying a warehouse, SQLite replica, or service).

## Run

### RAG

```bash
UV_CACHE_DIR=.uv-cache uv run --no-sync --python .venv/bin/python \
  examples/cookbook/rag/flow.py \
  --question "What changed in Mercury v2?" \
  --top-k 5
```

### NLP2SQL

```bash
UV_CACHE_DIR=.uv-cache uv run --no-sync --python .venv/bin/python \
  examples/cookbook/nlp2sql/flow.py \
  --question "Top 10 customers by revenue"
```

## Notes

- These are cookbook examples: intentionally minimal and easy to adapt.
- Replace heuristic SQL generation with an LLM-based agent for production.
- Keep SQL execution sandboxed and permissioned in Convex actions.
