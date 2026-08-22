"""Machine-verifiable output inventory and mounted-filesystem publication."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterable, Sequence

INVENTORY_SCHEMA = "wallaby-hires-output-inventory/v1"
DEFAULT_OUTPUT_PATTERNS = (
    "**/image.*.10arc.final_mosaic.fits",
    "**/weights.*.10arc.final_mosaic.fits",
)


class OutputValidationError(RuntimeError):
    """Raised when expected science outputs are absent, empty, or changed."""


def _sha256(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _path_below(root: Path, candidate: Path) -> Path:
    try:
        return candidate.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (FileNotFoundError, ValueError) as error:
        raise OutputValidationError(
            f"output escapes root or is missing: {candidate}"
        ) from error


def _write_json_atomic(path: Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(document, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def build_output_inventory(
    output_root: str | os.PathLike[str],
    patterns: Sequence[str] = DEFAULT_OUTPUT_PATTERNS,
    inventory_path: str | os.PathLike[str] | None = None,
) -> dict:
    """Validate final products and optionally write their deterministic inventory."""
    root = Path(output_root).resolve(strict=True)
    if not root.is_dir():
        raise OutputValidationError(f"output root is not a directory: {root}")
    if not patterns:
        raise OutputValidationError("at least one output pattern is required")

    selected: dict[str, Path] = {}
    pattern_counts: dict[str, int] = {}
    for pattern in patterns:
        if Path(pattern).is_absolute() or ".." in Path(pattern).parts:
            raise OutputValidationError(f"unsafe output pattern: {pattern!r}")
        matches = []
        for candidate in root.glob(pattern):
            if candidate.is_symlink() or not candidate.is_file():
                continue
            relative = _path_below(root, candidate)
            if candidate.absolute() != root / relative:
                raise OutputValidationError(
                    f"output path contains a symbolic link: {candidate}"
                )
            if candidate.stat().st_size <= 0:
                raise OutputValidationError(f"output is empty: {relative.as_posix()}")
            selected[relative.as_posix()] = candidate
            matches.append(candidate)
        pattern_counts[pattern] = len(matches)
        if not matches:
            raise OutputValidationError(
                f"no outputs matched required pattern {pattern!r}"
            )

    products = [
        {
            "path": relative,
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for relative, path in sorted(selected.items())
    ]
    inventory_digest = hashlib.sha256(
        json.dumps(products, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    inventory = {
        "schema": INVENTORY_SCHEMA,
        "patterns": list(patterns),
        "pattern_counts": pattern_counts,
        "products": products,
        "inventory_sha256": inventory_digest,
    }
    if inventory_path is not None:
        _write_json_atomic(Path(inventory_path), inventory)
    return inventory


def verify_output_inventory(
    output_root: str | os.PathLike[str],
    inventory: dict | str | os.PathLike[str],
) -> dict:
    """Re-hash every inventoried output and reject missing, altered, or unsafe files."""
    root = Path(output_root).resolve(strict=True)
    document: dict
    if isinstance(inventory, dict):
        document = inventory
    else:
        with Path(inventory).open("r", encoding="utf-8") as stream:
            document = json.load(stream)
    if document.get("schema") != INVENTORY_SCHEMA:
        raise OutputValidationError("unsupported output inventory schema")
    products = document.get("products")
    if not isinstance(products, list) or not products:
        raise OutputValidationError("output inventory has no products")

    verified = []
    for index, product in enumerate(products):
        if not isinstance(product, dict):
            raise OutputValidationError(f"inventory product {index} is not an object")
        relative = Path(str(product.get("path") or ""))
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise OutputValidationError(f"unsafe inventory path at product {index}")
        candidate = root / relative
        resolved_relative = _path_below(root, candidate)
        if candidate.absolute() != root / resolved_relative:
            raise OutputValidationError(
                f"output path contains a symbolic link: {relative}"
            )
        if candidate.is_symlink() or not candidate.is_file():
            raise OutputValidationError(f"output is not a regular file: {relative}")
        size = candidate.stat().st_size
        digest = _sha256(candidate)
        if size != product.get("bytes") or digest != product.get("sha256"):
            raise OutputValidationError(f"output changed after inventory: {relative}")
        verified.append(product)

    expected_inventory_digest = hashlib.sha256(
        json.dumps(verified, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    if expected_inventory_digest != document.get("inventory_sha256"):
        raise OutputValidationError("output inventory digest is invalid")
    return document


def publish_output_inventory(
    source_root: str | os.PathLike[str],
    destination_root: str | os.PathLike[str],
    inventory: dict | str | os.PathLike[str],
) -> dict:
    """Atomically copy verified products to a mounted durable filesystem."""
    source = Path(source_root).resolve(strict=True)
    destination = Path(destination_root).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    document = verify_output_inventory(source, inventory)

    for product in document["products"]:
        relative = Path(product["path"])
        source_path = source / relative
        destination_path = destination / relative
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            destination_path.parent.resolve(strict=True).relative_to(destination)
        except ValueError as error:
            raise OutputValidationError(
                f"publication destination escapes root: {relative.as_posix()}"
            ) from error
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination_path.name}.",
            suffix=".part",
            dir=destination_path.parent,
        )
        os.close(descriptor)
        try:
            shutil.copyfile(source_path, temporary_name)
            with open(temporary_name, "rb") as stream:
                os.fsync(stream.fileno())
            if _sha256(Path(temporary_name)) != product["sha256"]:
                raise OutputValidationError(
                    f"published checksum mismatch: {relative.as_posix()}"
                )
            os.replace(temporary_name, destination_path)
        finally:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass

    published_inventory = destination / "wallaby-output-inventory.json"
    _write_json_atomic(published_inventory, document)
    return verify_output_inventory(destination, published_inventory)


def verify_output_products(
    output_root: str,
    expected_patterns_json: str = "",
    inventory_path: str = "",
) -> str:
    """DALiuGE PyFunc wrapper returning the inventory as canonical JSON text."""
    patterns: Iterable[str] = DEFAULT_OUTPUT_PATTERNS
    if expected_patterns_json:
        decoded = json.loads(expected_patterns_json)
        if not isinstance(decoded, list) or not all(
            isinstance(item, str) for item in decoded
        ):
            raise OutputValidationError(
                "expected_patterns_json must be a JSON string array"
            )
        patterns = decoded
    inventory = build_output_inventory(
        output_root,
        tuple(patterns),
        inventory_path or str(Path(output_root) / "wallaby-output-inventory.json"),
    )
    return json.dumps(inventory, separators=(",", ":"), sort_keys=True)


def build_staging_output_inventory(
    patterns: Sequence[str] = DEFAULT_OUTPUT_PATTERNS,
    inventory_path: str | os.PathLike[str] | None = None,
) -> dict:
    """Validate final products only in the configured production workspace."""
    # Local import avoids coupling the generic output module to manifest parsing at
    # import time while keeping one authoritative root validator.
    from .funcs import resolve_staging_root

    root = Path(resolve_staging_root())
    return build_output_inventory(
        root,
        patterns=patterns,
        inventory_path=inventory_path or root / "wallaby-output-inventory.json",
    )
