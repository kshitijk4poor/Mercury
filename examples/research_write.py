"""Example research-and-write workflow for Mercury."""

WORKFLOW = {
    "workflow_id": "research-write",
    "tasks": [
        {"id": "t1", "kind": "agent", "target": "researcher"},
        {"id": "t2", "kind": "skill", "target": "summarizer", "depends_on": ["t1"]},
        {
            "id": "t3",
            "kind": "agent",
            "target": "writer",
            "depends_on": ["t2"],
            "needs_reasoning": True,
        },
    ],
}
