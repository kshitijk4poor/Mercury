"""Mercury v2 CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence

from mercury import inspect_run, resume_flow, run_flow


def _load_json_path(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mercury")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_cmd = subparsers.add_parser("run")
    run_cmd.add_argument("--workflow", required=True, help="Path to workflow JSON file")
    run_cmd.add_argument("--planner-id", required=True)
    run_cmd.add_argument("--planner-config")
    run_cmd.add_argument("--scheduler-id", default="superstep")
    run_cmd.add_argument("--scheduler-config")
    run_cmd.add_argument("--sandbox-id", default="host")
    run_cmd.add_argument("--sandbox-config")
    run_cmd.add_argument("--hitl-id")
    run_cmd.add_argument("--hitl-config")
    run_cmd.add_argument("--inbound-adapter-id")
    run_cmd.add_argument("--inbound-adapter-config")
    run_cmd.add_argument("--workspace", required=False, default=".")
    run_cmd.add_argument("--max-concurrency", type=int, default=4)
    run_cmd.add_argument("--durability-mode", default="sync")

    resume_cmd = subparsers.add_parser("resume")
    resume_cmd.add_argument("--checkpoint", required=True)
    resume_cmd.add_argument("--planner-id")
    resume_cmd.add_argument("--planner-config")
    resume_cmd.add_argument("--scheduler-id")
    resume_cmd.add_argument("--scheduler-config")
    resume_cmd.add_argument("--sandbox-id")
    resume_cmd.add_argument("--sandbox-config")
    resume_cmd.add_argument("--hitl-id")
    resume_cmd.add_argument("--hitl-config")
    resume_cmd.add_argument("--durability-mode")

    inspect_cmd = subparsers.add_parser("inspect")
    inspect_cmd.add_argument("--checkpoint", required=True)
    inspect_cmd.add_argument("--json", action="store_true")

    return parser


async def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "run":
        workflow = json.loads(Path(args.workflow).read_text(encoding="utf-8"))
        result = await run_flow(
            workflow,
            planner_id=args.planner_id,
            planner_config=_load_json_path(args.planner_config),
            scheduler_id=args.scheduler_id,
            scheduler_config=_load_json_path(args.scheduler_config),
            sandbox_id=args.sandbox_id,
            sandbox_config=_load_json_path(args.sandbox_config),
            hitl_id=args.hitl_id,
            hitl_config=_load_json_path(args.hitl_config),
            inbound_adapter_id=args.inbound_adapter_id,
            inbound_adapter_config=_load_json_path(args.inbound_adapter_config),
            max_concurrency=args.max_concurrency,
            durability_mode=args.durability_mode,
            workspace=args.workspace,
        )
        print(json.dumps(asdict(result)))
        return 0

    if args.command == "resume":
        result = await resume_flow(
            args.checkpoint,
            planner_id=args.planner_id,
            planner_config=_load_json_path(args.planner_config),
            scheduler_id=args.scheduler_id,
            scheduler_config=_load_json_path(args.scheduler_config),
            sandbox_id=args.sandbox_id,
            sandbox_config=_load_json_path(args.sandbox_config),
            hitl_id=args.hitl_id,
            hitl_config=_load_json_path(args.hitl_config),
            durability_mode=args.durability_mode,
        )
        print(json.dumps(asdict(result)))
        return 0

    if args.command == "inspect":
        snapshot = inspect_run(args.checkpoint)
        if args.json:
            print(json.dumps(snapshot))
        else:
            print(json.dumps(snapshot, indent=2))
        return 0

    return 1


def entrypoint() -> int:
    return asyncio.run(main())


if __name__ == "__main__":
    raise SystemExit(entrypoint())
