"""Command-line tools for validating manifests and WALLABY output evidence."""

from __future__ import annotations

import argparse
import json
import sys
from importlib.resources import files
from pathlib import Path
from typing import Sequence

from .funcs import ManifestValidationMode, validate_manifest
from .outputs import (
    OutputValidationError,
    build_output_inventory,
    build_staging_output_inventory,
    publish_output_inventory,
    verify_output_inventory,
)
from .slurm import (
    SlurmImagerResources,
    SlurmInterrupted,
    SlurmLifecycleError,
    run_setonix_imager,
)


def package_version() -> str:
    return (
        files("wallaby_hires")
        .joinpath("VERSION")
        .read_text(encoding="utf-8")
        .strip()
        .lstrip("v")
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wallaby_hires",
        description="Validate WALLABY Beampipe inputs and output evidence.",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {package_version()}"
    )
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser(
        "validate-manifest", help="validate a manifest JSON file"
    )
    validate.add_argument("manifest", type=Path)
    validate.add_argument(
        "--mode",
        choices=[mode.value for mode in ManifestValidationMode],
        default=ManifestValidationMode.SETONIX_PRODUCTION.value,
        help="admission policy (default: fail-closed Setonix production)",
    )

    inventory = commands.add_parser(
        "inventory-outputs", help="validate final products and write SHA-256 evidence"
    )
    inventory.add_argument("output_root", type=Path)
    inventory.add_argument(
        "--inventory",
        type=Path,
        help="output JSON path (default: OUTPUT_ROOT/wallaby-output-inventory.json)",
    )
    inventory.add_argument(
        "--pattern",
        action="append",
        dest="patterns",
        help="required glob; repeat for multiple product classes",
    )

    staging_inventory = commands.add_parser(
        "inventory-staging-outputs",
        help="validate products in WALLABY_HIRES_STAGING_ROOT",
    )
    staging_inventory.add_argument(
        "--pattern",
        action="append",
        dest="patterns",
        help="required glob; repeat for multiple product classes",
    )

    verify = commands.add_parser("verify-inventory", help="re-hash an output inventory")
    verify.add_argument("output_root", type=Path)
    verify.add_argument("inventory", type=Path)

    publish = commands.add_parser(
        "publish-local", help="publish verified outputs to a mounted filesystem"
    )
    publish.add_argument("source_root", type=Path)
    publish.add_argument("inventory", type=Path)
    publish.add_argument("destination_root", type=Path)

    setonix_imager = commands.add_parser(
        "run-setonix-imager",
        help="submit and supervise one nested Setonix imager job",
    )
    setonix_imager.add_argument("--workdir", type=Path, required=True)
    setonix_imager.add_argument("--config", type=Path, required=True)
    setonix_imager.add_argument("--partition", default="work")
    setonix_imager.add_argument("--nodes", type=int, default=1)
    setonix_imager.add_argument("--ntasks", type=int, default=6)
    setonix_imager.add_argument("--ntasks-per-node", type=int, default=6)
    setonix_imager.add_argument("--cpus-per-task", type=int, default=1)
    setonix_imager.add_argument("--memory", default="4G")
    setonix_imager.add_argument("--time-limit", default="00:40:00")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "validate-manifest":
            with arguments.manifest.open("r", encoding="utf-8") as stream:
                manifest = json.load(stream)
            validate_manifest(manifest, arguments.mode)
            summary = {
                "mode": arguments.mode,
                "valid": True,
                "sources": len(manifest.get("sources") or []),
            }
            print(json.dumps(summary, sort_keys=True))
        elif arguments.command == "inventory-outputs":
            inventory_path = arguments.inventory or (
                arguments.output_root / "wallaby-output-inventory.json"
            )
            patterns = tuple(arguments.patterns) if arguments.patterns else None
            kwargs = {"inventory_path": inventory_path}
            if patterns is not None:
                kwargs["patterns"] = patterns
            document = build_output_inventory(arguments.output_root, **kwargs)
            print(json.dumps(document, sort_keys=True))
        elif arguments.command == "inventory-staging-outputs":
            patterns = tuple(arguments.patterns) if arguments.patterns else None
            kwargs = {}
            if patterns is not None:
                kwargs["patterns"] = patterns
            document = build_staging_output_inventory(**kwargs)
            print(json.dumps(document, sort_keys=True))
        elif arguments.command == "verify-inventory":
            document = verify_output_inventory(arguments.output_root, arguments.inventory)
            print(json.dumps(document, sort_keys=True))
        elif arguments.command == "publish-local":
            document = publish_output_inventory(
                arguments.source_root,
                arguments.destination_root,
                arguments.inventory,
            )
            print(json.dumps(document, sort_keys=True))
        elif arguments.command == "run-setonix-imager":
            resources = SlurmImagerResources(
                partition=arguments.partition,
                nodes=arguments.nodes,
                ntasks=arguments.ntasks,
                ntasks_per_node=arguments.ntasks_per_node,
                cpus_per_task=arguments.cpus_per_task,
                memory=arguments.memory,
                time_limit=arguments.time_limit,
            )
            run_setonix_imager(
                arguments.workdir,
                arguments.config,
                resources=resources,
            )
        else:  # pragma: no cover - argparse requires one of the commands
            parser.error("a command is required")
    except SlurmInterrupted as error:
        print(f"wallaby_hires: {error}", file=sys.stderr)
        return 128 + error.signal_number
    except (OSError, ValueError, OutputValidationError, SlurmLifecycleError) as error:
        print(f"wallaby_hires: {error}", file=sys.stderr)
        return 2
    return 0


def entrypoint() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    entrypoint()
