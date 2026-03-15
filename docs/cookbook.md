# Cookbook

Mercury's cookbook demonstrates product use cases built on the same kernel and runtime:

- **Research + Write** (`packages/mercury-examples/examples/research_write.py`): a research agent → summarizer skill → writer agent flow that highlights dependency chaining and checkpoint/resume semantics.
- **Convex-backed RAG** (`examples/cookbook/rag/flow.py`): retrieval, composing, and logging tasks show how tools and agents orchestrate Convex-backed search while the kernel manages retries and artifacts.
- **Convex-backed NLP2SQL** (`examples/cookbook/nlp2sql/flow.py`): schema fetch, SQL generation, execution, and summarization tasks demonstrate tool-agent cooperation, fallback outputs, and logging actions.

Each example registers handlers, builds workflow graphs, and lets Mercury handle planners, schedulers, checkpoints, and the event journal so the handlers stay focused on behavior.
