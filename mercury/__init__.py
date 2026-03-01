"""Mercury public package interface."""

from mercury.runtime import (
    _DEFAULT_ORCHESTRATOR,
    cancel_run,
    inspect_run,
    register_agent,
    register_hitl,
    register_hook,
    register_inbound_adapter,
    register_planner,
    register_sandbox,
    register_scheduler,
    register_skill,
    register_tool,
    resume_flow,
    run_flow,
)
from mercury_adapters import register_builtin_plugins

register_builtin_plugins(_DEFAULT_ORCHESTRATOR)

__all__ = [
    "inspect_run",
    "cancel_run",
    "register_agent",
    "register_tool",
    "register_skill",
    "register_planner",
    "register_scheduler",
    "register_sandbox",
    "register_hitl",
    "register_inbound_adapter",
    "register_hook",
    "resume_flow",
    "run_flow",
    "__version__",
]

__version__ = "0.2.0"
