"""Mercury CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from mercury.runtime import inspect_run, resume_flow, run_flow


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mercury")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_cmd = subparsers.add_parser("run")
    run_cmd.add_argument("--workflow", required=True, help="Path to workflow JSON file")
    run_cmd.add_argument("--workspace", required=False, default=".")
    run_cmd.add_argument("--max-concurrency", type=int, default=4)

    resume_cmd = subparsers.add_parser("resume")
    resume_cmd.add_argument("--checkpoint", required=True)

    inspect_cmd = subparsers.add_parser("inspect")
    inspect_cmd.add_argument("--checkpoint", required=True)
    inspect_cmd.add_argument("--json", action="store_true")
    return parser


async def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "run":
        workflow_path = Path(args.workflow)
        workflow = json.loads(workflow_path.read_text(encoding="utf-8"))
        result = await run_flow(
            workflow,
            planner_model=None,
            max_concurrency=args.max_concurrency,
            workspace=args.workspace,
        )
        print(json.dumps(asdict(result)))
        return 0

    if args.command == "resume":
        result = await resume_flow(args.checkpoint, planner_model=None)
        print(json.dumps(asdict(result)))
        return 0

    if args.command == "inspect":
        snapshot = inspect_run(args.checkpoint)
        print(json.dumps(snapshot))
        return 0

    return 1


def entrypoint() -> int:
    return asyncio.run(main())


if __name__ == "__main__":
    raise SystemExit(entrypoint())
