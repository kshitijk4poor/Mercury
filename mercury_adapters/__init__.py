"""Built-in adapter package for Mercury v2."""

from mercury_adapters.hitl import CliGateHitl, NoneHitl
from mercury_adapters.planners import RulesPlanner, RulesPydanticAIPlanner
from mercury_adapters.sandboxes import DockerSandbox, HostSandbox
from mercury_adapters.schedulers import ReadyQueueScheduler, SuperstepScheduler

__all__ = [
    "register_builtin_plugins",
    "RulesPlanner",
    "RulesPydanticAIPlanner",
    "SuperstepScheduler",
    "ReadyQueueScheduler",
    "HostSandbox",
    "DockerSandbox",
    "NoneHitl",
    "CliGateHitl",
]


def register_builtin_plugins(orchestrator) -> None:
    registrations = (
        (orchestrator.register_planner, "rules", RulesPlanner()),
        (
            orchestrator.register_planner,
            "rules_pydanticai",
            RulesPydanticAIPlanner(),
        ),
        (orchestrator.register_scheduler, "superstep", SuperstepScheduler()),
        (orchestrator.register_scheduler, "ready_queue", ReadyQueueScheduler()),
        (orchestrator.register_sandbox, "host", HostSandbox()),
        (orchestrator.register_sandbox, "docker", DockerSandbox()),
        (orchestrator.register_hitl, "none", NoneHitl()),
        (orchestrator.register_hitl, "cli_gate", CliGateHitl()),
    )

    for register, name, value in registrations:
        try:
            register(name, value)
        except ValueError:
            continue
