#!/usr/bin/env python3
"""Keep pyproject metadata, VERSION, and release tags consistent."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
VERSION_FILE = ROOT / "wallaby_hires" / "VERSION"
SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:[-+][0-9A-Za-z.-]+)?$")


def metadata_version() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    section_start = text.index("[project]")
    section_end = text.find("\n[", section_start + 1)
    section = text[section_start : section_end if section_end != -1 else len(text)]
    match = re.search(r'(?m)^version\s*=\s*"([^"]+)"$', section)
    if not match:
        raise RuntimeError("could not locate project version")
    return match.group(1)


def file_version() -> str:
    return VERSION_FILE.read_text(encoding="utf-8").strip().lstrip("v")


def set_version(version: str) -> None:
    if not SEMVER.fullmatch(version):
        raise ValueError("version must be semantic x.y.z syntax")
    text = PYPROJECT.read_text(encoding="utf-8")
    section_start = text.index("[project]")
    section_end = text.find("\n[", section_start + 1)
    if section_end == -1:
        section_end = len(text)
    section = text[section_start:section_end]
    updated, count = re.subn(
        r'(?m)^version\s*=\s*"[^"]+"$', f'version = "{version}"', section, count=1
    )
    if count != 1:
        raise RuntimeError("could not locate project version")
    PYPROJECT.write_text(
        text[:section_start] + updated + text[section_end:], encoding="utf-8"
    )
    VERSION_FILE.write_text(f"{version}\n", encoding="utf-8")


def check_version(tag: str = "") -> str:
    metadata = metadata_version()
    packaged = file_version()
    if metadata != packaged:
        raise RuntimeError(
            f"version mismatch: pyproject={metadata!r}, VERSION={packaged!r}"
        )
    if tag and metadata != tag.strip().lstrip("v"):
        raise RuntimeError(f"tag {tag!r} does not match package version {metadata!r}")
    return metadata


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--check", action="store_true")
    operation.add_argument("--set", metavar="VERSION")
    parser.add_argument("--tag", default="")
    arguments = parser.parse_args(argv)
    if arguments.set:
        set_version(arguments.set)
    print(check_version(arguments.tag))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
