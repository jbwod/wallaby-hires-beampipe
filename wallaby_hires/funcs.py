"""
Start date: 10/04/24
End date:
Description: All functions neeeded for the WALLABY hires test & deploy pipelines to
             process the HIPASS sources.
"""

import ast
import base64
import binascii
import concurrent.futures
import csv
import glob
import hashlib
import io
import json
import math

# Importing required modules
import os
import pickle
import re
import shutil
import tarfile
import tempfile
import urllib
import urllib.request
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Callable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlparse

PRESTAGE_INPUTS_DIR = "inputs"
CHECKSUMS_DIR = os.path.join(PRESTAGE_INPUTS_DIR, "checksums")


class ManifestDownloadError(RuntimeError):
    """
    Raised when a manifest-URL download fails (HTTP error, timeout, incomplete file).
    Propagates to DALiuGE so the run is marked failed for tracking.
    """


class ManifestValidationError(ValueError):
    """Raised when a Beampipe execution manifest is unsafe or incomplete."""


class ManifestValidationMode(str, Enum):
    """Admission policies for graph execution manifests."""

    SETONIX_PRODUCTION = "setonix-production"
    STRUCTURAL_NO_DOWNLOAD = "structural-no-download"


_SAFE_PATH_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,254}$")
_EVALUATION_FIELDS = (
    "evaluation_file",
    "evaluation_file_url",
    "evaluation_file_checksum_url",
)


def _redact_url(url: str) -> str:
    """Return a URL safe for logs by removing credentials, query, and fragment."""
    try:
        parsed = urlparse(str(url))
    except Exception:
        return "<invalid-url>"
    if not parsed.scheme or not parsed.netloc:
        return "<invalid-url>"
    host = parsed.hostname or "<redacted-host>"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return parsed._replace(netloc=host, query="", fragment="").geturl()


def _safe_path_segment(value: object, field_name: str) -> str:
    """Validate an identifier before it is used as one filesystem component."""
    segment = str(value or "").strip()
    if not _SAFE_PATH_SEGMENT.fullmatch(segment) or segment in {".", ".."}:
        raise ManifestValidationError(
            f"{field_name} must be a single portable path segment"
        )
    return segment


def _safe_download_filename(value: object, field_name: str = "filename") -> str:
    """Reject absolute, traversal, drive-qualified, and control-character names."""
    filename = unquote(str(value or "")).strip()
    normalized = filename.replace("\\", "/")
    parts = PurePosixPath(normalized).parts
    if (
        not filename
        or normalized.startswith("/")
        or len(parts) != 1
        or parts[0] in {".", ".."}
        or re.match(r"^[A-Za-z]:", normalized)
        or any(ord(char) < 32 or ord(char) == 127 for char in filename)
    ):
        raise ManifestValidationError(
            f"{field_name} must be a safe basename without path components"
        )
    return filename


def _safe_join(root: str, *segments: str) -> str:
    """Join already validated segments and prove the result remains below root."""
    root_path = os.path.abspath(root)
    candidate = os.path.abspath(os.path.join(root_path, *segments))
    if os.path.commonpath([root_path, candidate]) != root_path:
        raise ManifestValidationError("resolved path escapes the staging root")
    return candidate


def _validate_remote_url(value: object, field_name: str, required: bool = True) -> str:
    url = str(value or "").strip()
    if not url and not required:
        return ""
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ManifestValidationError(f"{field_name} must be an HTTP(S) URL")
    return url


def _validate_https_url(value: object, field_name: str) -> str:
    """Validate a required HTTPS URL for production data-plane access."""
    if not str(value or "").strip():
        raise ManifestValidationError(
            f"{field_name} is required in setonix-production mode"
        )
    url = _validate_remote_url(value, field_name)
    if urlparse(url).scheme != "https":
        raise ManifestValidationError(f"{field_name} must be an HTTPS URL")
    return url


def _coerce_manifest_validation_mode(
    validation_mode: ManifestValidationMode | str,
) -> ManifestValidationMode:
    if isinstance(validation_mode, ManifestValidationMode):
        return validation_mode
    try:
        return ManifestValidationMode(str(validation_mode))
    except ValueError:
        choices = ", ".join(mode.value for mode in ManifestValidationMode)
        raise ManifestValidationError(
            f"validation_mode must be one of: {choices}"
        ) from None


def _filename_from_url(url: str) -> str:
    """Resolve a safe filename from a URL without opening the remote resource."""
    parsed = urlparse(url)
    for value in parse_qs(parsed.query).get("response-content-disposition", []):
        match = re.search(r"filename\*?=(?:UTF-8''|\")?([^\";]+)", value, re.I)
        if match:
            return _safe_download_filename(match.group(1), "URL filename")
    return _safe_download_filename(
        os.path.basename(parsed.path.rstrip("/")) or "download", "URL filename"
    )


def _atomic_write_bytes(path: str, data: bytes) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    handle, temporary_path = tempfile.mkstemp(prefix=".beampipe-", dir=directory)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    except Exception:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise


def _select_primary_beam_cube_fits_in_directory(
    dirpath: str, filename_hint: str = ""
) -> str:
    if not dirpath or not os.path.isdir(dirpath):
        return ""
    candidates = sorted(glob.glob(os.path.join(dirpath, "akpb.iquv.*.cube.fits")))
    if not candidates:
        candidates = sorted(glob.glob(os.path.join(dirpath, "*.cube.fits")))
    if not candidates:
        return ""
    if len(candidates) == 1:
        return candidates[0]
    hint = os.path.basename(filename_hint) if filename_hint else ""
    if hint and ".SB" in hint:
        prefix = hint.split(".SB", 1)[0]
        matches = [c for c in candidates if os.path.basename(c).startswith(prefix)]
        if len(matches) == 1:
            return matches[0]
    raise ManifestValidationError(
        f"expected exactly one primary-beam *.cube.fits in {dirpath!r}; "
        f"found {len(candidates)}"
    )


def _resolve_primary_beam_cube_path(requested_abs_path: str) -> str:
    if not requested_abs_path:
        return requested_abs_path
    p = os.path.normpath(requested_abs_path)
    if os.path.isdir(p):
        chosen = _select_primary_beam_cube_fits_in_directory(p)
        return chosen if chosen else requested_abs_path
    if os.path.isfile(p):
        return p
    parent = os.path.dirname(p)
    if os.path.isdir(parent):
        chosen = _select_primary_beam_cube_fits_in_directory(parent, os.path.basename(p))
        return chosen if chosen else requested_abs_path
    return requested_abs_path


def _unwrap_dlg_port_layer(value):
    """
    Decode for DALiuGE Memory
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, tuple):
        return value
    if isinstance(value, memoryview):
        value = value.tobytes()
    elif isinstance(value, bytearray):
        value = bytes(value)
    elif not isinstance(value, bytes):
        raise TypeError(f"expected str, tuple, or buffer, got {type(value).__name__}")
    if len(value) >= 2 and value[0] == 0x80:
        return pickle.loads(value)
    return value.decode("utf-8")


def _peel_dlg_port_to_str_or_tuple(value):
    """
    Follow pickle
    """
    while True:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, tuple):
            return value
        value = _unwrap_dlg_port_layer(value)


def _normalize_dlg_csv_string(csv_string) -> str:
    """
    CSV DLG Handler

    Memory drops and PyFunc ports supply a str, UTF-8, or a memoryview
    encoding of pickled or raw bytes this makes it a consistent input.
    """
    if csv_string is None:
        return ""
    obj = _peel_dlg_port_to_str_or_tuple(csv_string)
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj
    if isinstance(obj, tuple):
        if len(obj) == 4 and isinstance(obj[0], str) and isinstance(obj[1], str):
            return obj[1]
        raise TypeError(
            "csv_string out prestage_manifest_inputs, " f"got length {len(obj)}"
        )
    raise TypeError(f"csv_string must decode to str or tuple, got {type(obj).__name__}")


def _normalize_urls_json_arg(value, tuple_index: int) -> str:
    """
    DLG handler for JSON
    """
    if value is None:
        return ""
    obj = _peel_dlg_port_to_str_or_tuple(value)
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj
    if isinstance(obj, tuple):
        if len(obj) == 4 and tuple_index in (2, 3):
            cell = obj[tuple_index]
            return cell if isinstance(cell, str) else str(cell)
        raise TypeError(
            "Expected prestage 4-tuple (credentials, csv, ms_urls_json, eval_urls_json) "
            f"or a JSON str; got tuple of length {len(obj)}"
        )
    raise TypeError(f"URL input must decode to str or tuple, got {type(obj).__name__}")


def _download_url_to_path(url: str, path: str, timeout: int = 300) -> None:
    safe_url = _validate_remote_url(url, "download URL")
    try:
        with urllib.request.urlopen(safe_url, timeout=timeout) as response:
            _atomic_write_bytes(path, response.read())
    except (HTTPError, URLError, TimeoutError) as error:
        raise ManifestDownloadError(
            f"Unable to download {_redact_url(safe_url)} ({type(error).__name__})"
        ) from None
    print(f"Downloaded {_redact_url(safe_url)} -> {path}")


def _write_text_atomic(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def _fetch_checksum_to_workspace(
    checksum_url: str, timeout: int = 300
) -> tuple[str, str]:
    """
    Download checksum text into the current workspace and return
    (expected_hex, checksum_file_path). If checksum_url is empty, returns ("","").
    """
    if not checksum_url or not str(checksum_url).strip():
        return "", ""
    checksum_url = _validate_remote_url(checksum_url, "checksum URL")
    parsed = urlparse(checksum_url)
    name = _safe_download_filename(
        os.path.basename(parsed.path) or "download.checksum", "checksum filename"
    )
    local_path = os.path.join(os.getcwd(), CHECKSUMS_DIR, name)
    try:
        with urllib.request.urlopen(checksum_url, timeout=timeout) as response:
            content = response.read().decode("utf-8", errors="replace").strip()
    except (HTTPError, URLError, TimeoutError) as error:
        raise ManifestDownloadError(
            f"Unable to download checksum {_redact_url(checksum_url)} "
            f"({type(error).__name__})"
        ) from None
    _write_text_atomic(local_path, content + "\n")
    parts = content.split()
    expected = (parts[0] if parts else "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{32}", expected):
        raise ManifestDownloadError(
            f"Invalid MD5 checksum returned by {_redact_url(checksum_url)}"
        )
    return expected, local_path


def _hash_file(filepath: str, algo: str, chunk_size: int = 1 << 20) -> str:
    algo = (algo or "").lower()
    if algo == "md5":
        h = hashlib.md5()
    else:
        raise ValueError(f"Unsupported chk: {algo!r}")
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest().lower()


def _infer_hash_algo_from_hex(expected_hex: str) -> str:
    """
    CASDA checksum
    """
    n = len(expected_hex or "")
    if n != 32:
        raise ValueError(f"wrong chksm (32 hex), got length {n} for {expected_hex!r}")
    return "md5"


def _resolve_staging_root_arg(staging_root: Optional[str]) -> str:
    if staging_root is None:
        staging_root = ""
    staging_root = _peel_dlg_port_to_str_or_tuple(staging_root)
    if staging_root is None:
        staging_root = ""
    if isinstance(staging_root, tuple):
        staging_root = str(staging_root)
    staging_root = str(staging_root).strip()
    if staging_root:
        return staging_root
    return os.environ.get("WALLABY_HIRES_STAGING_ROOT", "").strip()


def _validate_production_sbid_evaluation(
    sbid_group: dict, sbid_prefix: str, datasets: list
) -> None:
    """Require one consistently described evaluation archive for an SBID."""
    effective_values = {}
    for field_name in _EVALUATION_FIELDS:
        group_value = str(sbid_group.get(field_name) or "").strip()
        dataset_values = [
            str(dataset.get(field_name) or "").strip() for dataset in datasets
        ]
        supplied_values = {value for value in [group_value, *dataset_values] if value}
        if len(supplied_values) > 1:
            raise ManifestValidationError(
                f"{sbid_prefix}.{field_name} must identify exactly one "
                "evaluation archive per SBID"
            )
        if group_value:
            effective_value = group_value
        else:
            if any(not value for value in dataset_values):
                raise ManifestValidationError(
                    f"{sbid_prefix}.{field_name} is required on the SBID or every "
                    "dataset in setonix-production mode"
                )
            effective_value = dataset_values[0]
        effective_values[field_name] = effective_value

    evaluation_file = _safe_download_filename(
        effective_values["evaluation_file"], f"{sbid_prefix}.evaluation_file"
    )
    if not evaluation_file.endswith(".tar"):
        raise ManifestValidationError(
            f"{sbid_prefix}.evaluation_file must name a .tar archive"
        )
    _validate_https_url(
        effective_values["evaluation_file_url"],
        f"{sbid_prefix}.evaluation_file_url",
    )
    _validate_https_url(
        effective_values["evaluation_file_checksum_url"],
        f"{sbid_prefix}.evaluation_file_checksum_url",
    )


def validate_manifest(
    manifest: dict,
    validation_mode: ManifestValidationMode | str = (
        ManifestValidationMode.SETONIX_PRODUCTION
    ),
) -> dict:
    """
    Validate the nested Beampipe manifest consumed by the staging graph.

    Setonix production is the fail-closed default. Legacy and URL-optional
    manifests are accepted only when ``structural-no-download`` is selected
    explicitly.
    """
    mode = _coerce_manifest_validation_mode(validation_mode)
    production = mode is ManifestValidationMode.SETONIX_PRODUCTION
    if not isinstance(manifest, dict):
        raise ManifestValidationError("manifest must be a JSON object")

    sources = manifest.get("sources")
    if sources is None:
        if production:
            raise ManifestValidationError(
                "sources is required in setonix-production mode"
            )
        return manifest
    if not isinstance(sources, list) or not sources:
        raise ManifestValidationError("sources must be a non-empty array")
    if production and len(sources) != 1:
        raise ManifestValidationError(
            "setonix-production mode requires exactly one source"
        )

    dataset_count = 0
    for source_index, source in enumerate(sources):
        prefix = f"sources[{source_index}]"
        if not isinstance(source, dict):
            raise ManifestValidationError(f"{prefix} must be an object")
        _safe_path_segment(source.get("source_identifier"), f"{prefix}.source_identifier")
        for required_field in ("ra_string", "dec_string"):
            if not str(source.get(required_field) or "").strip():
                raise ManifestValidationError(f"{prefix}.{required_field} is required")
        vsys = source.get("vsys")
        if vsys is None or isinstance(vsys, bool):
            raise ManifestValidationError(f"{prefix}.vsys must be a number")
        try:
            numeric_vsys = float(vsys)
        except (TypeError, ValueError):
            raise ManifestValidationError(f"{prefix}.vsys must be a number")
        if not math.isfinite(numeric_vsys):
            raise ManifestValidationError(f"{prefix}.vsys must be a finite number")

        sbids = source.get("sbids")
        if not isinstance(sbids, list) or not sbids:
            raise ManifestValidationError(f"{prefix}.sbids must be a non-empty array")
        seen_sbids = set()
        for sbid_index, sbid_group in enumerate(sbids):
            sbid_prefix = f"{prefix}.sbids[{sbid_index}]"
            if not isinstance(sbid_group, dict):
                raise ManifestValidationError(f"{sbid_prefix} must be an object")
            sbid = _safe_path_segment(sbid_group.get("sbid"), f"{sbid_prefix}.sbid")
            if production and sbid in seen_sbids:
                raise ManifestValidationError(
                    f"{prefix}.sbids contains duplicate SBID {sbid!r}"
                )
            seen_sbids.add(sbid)
            evaluation_file = sbid_group.get("evaluation_file")
            evaluation_url = sbid_group.get("evaluation_file_url")
            if evaluation_file:
                _safe_download_filename(evaluation_file, f"{sbid_prefix}.evaluation_file")
            if evaluation_url:
                _validate_remote_url(evaluation_url, f"{sbid_prefix}.evaluation_file_url")
            checksum_url = sbid_group.get("evaluation_file_checksum_url")
            if checksum_url:
                _validate_remote_url(
                    checksum_url, f"{sbid_prefix}.evaluation_file_checksum_url"
                )

            datasets = sbid_group.get("datasets")
            if not isinstance(datasets, list) or not datasets:
                raise ManifestValidationError(
                    f"{sbid_prefix}.datasets must be a non-empty array"
                )
            dataset_count += len(datasets)
            for dataset_index, dataset in enumerate(datasets):
                dataset_prefix = f"{sbid_prefix}.datasets[{dataset_index}]"
                if not isinstance(dataset, dict):
                    raise ManifestValidationError(f"{dataset_prefix} must be an object")
                dataset_name = (
                    dataset.get("name")
                    or dataset.get("dataset_id")
                    or dataset.get("visibility_filename")
                )
                _safe_download_filename(dataset_name, f"{dataset_prefix}.name")
                staged_url = dataset.get("staged_url")
                if staged_url:
                    _validate_remote_url(staged_url, f"{dataset_prefix}.staged_url")
                if production:
                    _validate_https_url(staged_url, f"{dataset_prefix}.staged_url")
                dataset_checksum_url = dataset.get("checksum_url")
                if dataset_checksum_url:
                    _validate_remote_url(
                        dataset_checksum_url, f"{dataset_prefix}.checksum_url"
                    )
                if production:
                    _validate_https_url(
                        dataset_checksum_url, f"{dataset_prefix}.checksum_url"
                    )
                dataset_evaluation_file = dataset.get("evaluation_file")
                if dataset_evaluation_file:
                    _safe_download_filename(
                        dataset_evaluation_file,
                        f"{dataset_prefix}.evaluation_file",
                    )
                dataset_evaluation_url = dataset.get("evaluation_file_url")
                if dataset_evaluation_url:
                    _validate_remote_url(
                        dataset_evaluation_url,
                        f"{dataset_prefix}.evaluation_file_url",
                    )
                dataset_evaluation_checksum_url = dataset.get(
                    "evaluation_file_checksum_url"
                )
                if dataset_evaluation_checksum_url:
                    _validate_remote_url(
                        dataset_evaluation_checksum_url,
                        f"{dataset_prefix}.evaluation_file_checksum_url",
                    )
            if production:
                _validate_production_sbid_evaluation(sbid_group, sbid_prefix, datasets)
    if production and dataset_count < 1:
        raise ManifestValidationError(
            "setonix-production mode requires at least one dataset"
        )
    return manifest


# def _verify_checksum(filepath: str, checksum_url: str, timeout: int = 300) -> None:
#     """
#     Fetch CASDA checksum file and verify downloaded file via MD5.
#     Checksum format: md5_hex (32 hex chars) typically as first field, e.g. "md5hash  filename".
#     Uses chunked reading to avoid loading large files into memory.
#     """
#     if not checksum_url or not checksum_url.strip():
#         return
#     with urllib.request.urlopen(checksum_url, timeout=timeout) as r:
#         content = r.read().decode("utf-8").strip()
#     parts = content.split()
#     if not parts:
#         raise ValueError(f"Invalid checksum format: {content[:80]}")
#     expected_md5 = parts[0].lower()
#     hasher = hashlib.md5()
#     with open(filepath, "rb") as f:
#         for chunk in iter(lambda: f.read(1 << 20), b""):
#             hasher.update(chunk)
#     actual_md5 = hasher.hexdigest().lower()
#     if actual_md5 != expected_md5:
#         raise ValueError(
#             f"Checksum mismatch for {os.path.basename(filepath)}: "
#             f"expected {expected_md5}, got {actual_md5}"
#         )
#     print(f"Checksum verified: {os.path.basename(filepath)}")


def _flatten_sources_to_dataset_rows(
    manifest: dict,
    validation_mode: ManifestValidationMode | str = (
        ManifestValidationMode.SETONIX_PRODUCTION
    ),
) -> list:
    """
    Flatten manifest sources[].sbids[].datasets[] into a list of dataset rows.
    Each row has name, ra_string, dec_string, vsys, evaluation_file, staged_url,
    checksum_url, evaluation_file_url, evaluation_file_checksum_url.
    """
    validate_manifest(manifest, validation_mode)
    rows = []
    sources = manifest.get("sources") or []
    for src in sources:
        source_identifier = src["source_identifier"]
        ra = src.get("ra_string") or ""
        dec = src.get("dec_string") or ""
        vsys = src.get("vsys")
        for sbid_group in src.get("sbids") or []:
            sbid = sbid_group["sbid"]
            evaluation_file = sbid_group.get("evaluation_file") or ""
            evaluation_file_url = sbid_group.get("evaluation_file_url") or ""
            evaluation_file_checksum_url = (
                sbid_group.get("evaluation_file_checksum_url") or ""
            )
            for ds in sbid_group.get("datasets") or []:
                name = (
                    ds.get("name")
                    or ds.get("dataset_id")
                    or ds.get("visibility_filename")
                )
                rows.append(
                    {
                        "source_identifier": source_identifier,
                        "sbid": str(sbid) if sbid is not None else "",
                        "name": name,
                        "ra_string": ra or ds.get("ra_string") or "",
                        "dec_string": dec or ds.get("dec_string") or "",
                        "vsys": vsys if vsys is not None else ds.get("vsys"),
                        "evaluation_file": evaluation_file
                        or ds.get("evaluation_file")
                        or "",
                        "staged_url": ds.get("staged_url") or "",
                        "checksum_url": ds.get("checksum_url") or "",
                        "evaluation_file_url": evaluation_file_url
                        or ds.get("evaluation_file_url")
                        or "",
                        "evaluation_file_checksum_url": evaluation_file_checksum_url
                        or ds.get("evaluation_file_checksum_url")
                        or "",
                    }
                )
    return rows


def _build_csv_string_from_dataset_rows(rows: list) -> str:
    """
    Build CSV string with header Name,RA_string,Dec_string,Vsys,,evaluation_file_path,source_identifier,sbid
    so process_CSV_str (column index 5 = evaluation_file_path) and process_CSV_mosaic_str
    get expected format. Column 5 is the path to the primary beam FITS inside the tar,
    matching original process_data/process_SOURCE output.
    Uses csv.writer for correct escaping of commas in values.
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "Name",
            "RA_string",
            "Dec_string",
            "Vsys",
            "",
            "evaluation_file",
            "source_identifier",
            "sbid",
        ]
    )
    for r in rows:
        vsys = r.get("vsys")
        vsys_str = "" if vsys is None else str(vsys)
        eval_file = r.get("evaluation_file", "")
        eval_file_path = "LinmosBeamImages" if eval_file else ""
        writer.writerow(
            [
                r.get("name", ""),
                r.get("ra_string", ""),
                r.get("dec_string", ""),
                vsys_str,
                "",
                eval_file_path,
                r.get("source_identifier", ""),
                r.get("sbid", ""),
            ]
        )
    return buf.getvalue()


def prestage_manifest_inputs(
    manifest_bytes: bytes,
    validation_mode: ManifestValidationMode | str = (
        ManifestValidationMode.SETONIX_PRODUCTION
    ),
) -> tuple:
    """
    Parse manifest JSON; download only credentials (casda.ini); build csv_string and
    URL lists from sources/datasets. Returns 4-tuple for manifest-driven pipeline.

    Manifest format (preferred): inputs.credentials_ini_url; sources[] (nested) or
    datasets[] (flat). See wallaby-hires-beampipe/manifest_schema.md.

    Setonix production admission is the default. Legacy and URL-optional
    manifests require :func:`prestage_manifest_inputs_no_download` or an explicit
    ``structural-no-download`` validation mode.

    Returns
    -------
    tuple[str, str, str, str]
        (credentials_path, csv_string, ms_urls_json, eval_urls_json)
    """
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    mode = _coerce_manifest_validation_mode(validation_mode)
    validate_manifest(manifest, mode)
    inputs = manifest.get("inputs") or {}
    staged = manifest.get("staged") or {}

    cred_url = inputs.get("credentials_ini_url") or inputs.get("casda_ini_url") or ""
    base = os.path.join(os.getcwd(), PRESTAGE_INPUTS_DIR)
    credentials_path = os.path.join(base, "casda.ini")
    if cred_url:
        _download_url_to_path(cred_url, credentials_path)

    rows = _flatten_sources_to_dataset_rows(manifest, mode)
    if rows:
        csv_string = _build_csv_string_from_dataset_rows(rows)
        ms_urls = [
            {
                "url": r["staged_url"],
                "name": r["name"],
                "checksum_url": r.get("checksum_url") or "",
                "source_identifier": r.get("source_identifier") or "",
                "sbid": r.get("sbid") or "",
            }
            for r in rows
            if r.get("staged_url")
        ]
        seen_eval: set[tuple[str, str, str]] = set()
        eval_urls = []
        for r in rows:
            url = r.get("evaluation_file_url") or ""
            eval_key = (
                r.get("source_identifier") or "",
                r.get("sbid") or "",
                url,
            )
            if url and eval_key not in seen_eval:
                seen_eval.add(eval_key)
                eval_urls.append(
                    {
                        "url": url,
                        "name": r.get("evaluation_file") or _filename_from_url(url),
                        "checksum_url": r.get("evaluation_file_checksum_url") or "",
                        "source_identifier": r.get("source_identifier") or "",
                        "sbid": r.get("sbid") or "",
                    }
                )
        if mode is ManifestValidationMode.SETONIX_PRODUCTION:
            expected_eval_count = sum(
                len(source.get("sbids") or []) for source in manifest["sources"]
            )
            if len(ms_urls) != len(rows) or len(eval_urls) != expected_eval_count:
                raise ManifestValidationError(
                    "setonix-production manifest did not produce one visibility URL "
                    "per dataset and one evaluation URL per SBID"
                )
    else:
        # Legacy: download input_csv and use its content; use staged URL lists
        raw_ms = staged.get("ms_urls") or staged.get("ms") or []
        raw_eval = staged.get("eval_urls") or staged.get("eval") or []
        ms_urls = [
            {"url": u, "checksum_url": ""}
            for u in raw_ms
            if isinstance(u, str) and not u.endswith("checksum")
        ]
        eval_urls = [
            {"url": u, "checksum_url": ""} if isinstance(u, str) else u for u in raw_eval
        ]
        input_csv_url = inputs.get("input_csv_url") or ""
        if input_csv_url:
            input_csv_path = os.path.join(base, "input.csv")
            _download_url_to_path(input_csv_url, input_csv_path)
            with open(input_csv_path, "r", encoding="utf-8") as f:
                csv_string = f.read()
        else:
            csv_string = "Name,RA_string,Dec_string,Vsys,,evaluation_file\n"

    return (
        credentials_path,
        csv_string,
        json.dumps(ms_urls),
        json.dumps(eval_urls),
    )


def prestage_manifest_inputs_no_download(manifest_bytes: bytes) -> tuple:
    """Pre-stage with the explicit structural/no-download admission policy."""
    return prestage_manifest_inputs(
        manifest_bytes,
        validation_mode=ManifestValidationMode.STRUCTURAL_NO_DOWNLOAD,
    )


# Updated read_and_process which also includes the evaluation file locations
def read_and_process_csv(filename: str) -> list:
    """
    Reads a CSV file and processes its contents, returning a list of dictionaries.

    Parameters
    ----------
    filename:
        The name of the CSV file to be read.

    Returns
    -------
    list
        A list of dictionaries representing each processed row of the CSV file.
    """

    # List to store the source dictionary
    data = []

    # Opening the .csv file
    with open(filename, "r") as file:
        # Create a CSV reader
        reader = csv.reader(file)

        # Read and process each row, including the header
        for row in reader:
            # Extract individual parameters
            name = str(row[0]).strip()
            RA = str(row[1]).strip()
            RA_split = RA.split(":")
            RA_hh, RA_mm, RA_ss = map(str.strip, RA_split)
            RA_string = f"{RA_hh}h{RA_mm}m{RA_ss}s"

            Dec = str(row[2]).strip()
            Dec_split = Dec.split(":")
            Dec_dd, Dec_mm, Dec_ss = map(str.strip, Dec_split)
            Dec_string = f"{Dec_dd}.{Dec_mm}.{Dec_ss}"

            Vsys = float(row[3])

            # Read the evaluation file parameter
            evaluation_file = str(row[5]).strip()

            # Additional parameters
            RA_beam_string = RA_string
            Dec_beam_string = Dec_string

            # Create the desired output dictionary
            fid = _logical_field_id(name)
            output_dict = {
                "Cimager.dataset": f"$DLG_ROOT/testdata/{_ms_dataset_basename(name)}",
                "Cimager.Images.Names": f"[image.{fid}]",
                "Cimager.Images.direction": f"[{RA_string},{Dec_string}, J2000]",
                "Cimager.write.weightsimage": "true",
                "Vsys": Vsys,
                "imcontsub.inputfitscube": _restored_cube_base(name),
                "imcontsub.outputfitscube": _contsub_cube_base(name),
                "linmos.names": f"[{_contsub_cube_base(name)}]",
                "linmos.weights": f"[weights.{fid}]",
                "linmos.outname": _contsub_holo_outname(name),
                "linmos.outweight": f"weights.{fid}.contsub_holo",
                "linmos.feeds.centre": f"[{RA_beam_string},{Dec_beam_string}]",
                f"linmos.feeds.image.restored.{fid}.contsub": "[0.0,0.0]",
                "linmos.primarybeam.ASKAP_PB.image": evaluation_file,
            }

            data.append(output_dict)

        # Check if the file is empty
        if not data:
            print(f"Warning: CSV file '{filename}' is empty.")
        else:
            print(f"CSV file '{filename}' successfully read and processed into a list of \
                    dictionaries.")

    return data


def _dynamic_buffer_to_obj(buf: bytes):
    if len(buf) >= 1 and buf[0:1] == b"\x80":
        return pickle.loads(buf)
    if len(buf) >= 2 and (buf.startswith(b"b'") or buf.startswith(b'b"')):
        inner = ast.literal_eval(buf.decode("utf-8"))
        if isinstance(inner, (bytes, bytearray)):
            return _dynamic_buffer_to_obj(bytes(inner))
        raise TypeError(
            "dynamic_parset bytes-literal payload must decode to bytes, "
            f"not {type(inner).__name__}"
        )
    return json.loads(buf.decode("utf-8"))


def _dynamic_str_to_obj(text: str):
    s = text.strip()
    if not s:
        raise TypeError("dynamic_parset string is empty")
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    pad = (-len(s)) % 4
    try:
        raw = base64.b64decode(s + "=" * pad)
    except binascii.Error as e:
        raise ValueError("dynamic_parset is not JSON and is not valid base64") from e
    return _dynamic_buffer_to_obj(raw)


def _drop_conflicting_parent_cimager_image_keys(static_parset: dict, prefix: str) -> None:
    """
    Remove parent-level ``<prefix>.Images.{nchan,polarisation,nterms}`` when the
    parset names an image (``<prefix>.Images.Names``) and/or has per-image keys
    under ``<prefix>.Images.image.<name>.*``. Those parent keys duplicate cube
    semantics and can break CASA coordinate setup during cimager restore.
    """
    p = (prefix or "").strip()
    if not p:
        return
    pimg = f"{p}.Images.image."
    pnames = f"{p}.Images.Names"
    if not (
        pnames in static_parset or any(str(k).startswith(pimg) for k in static_parset)
    ):
        return
    for bad in (
        f"{p}.Images.nchan",
        f"{p}.Images.polarisation",
        f"{p}.Images.polarization",
        f"{p}.Images.nterms",
    ):
        static_parset.pop(bad, None)


def parset_mixing(static_parset: dict, dynamic_parset, prefix: str = "") -> str:
    """
    Update parset with dict values.

    Parameters
    ----------
    static_parset:
        Standard parset dictionary
    dynamic_parset:
        List of dictionaries containing key-value pairs to update parset.
    prefix:
        Prefix to filter which keys should be updated.

    Returns
    -------
    str
        One ``key=value`` per line, UTF-8 text. Use graph ``merged_dict`` port
        encoding ``utf-8`` so PyFunc writes text (not ``repr`` of bytes).
    """
    if isinstance(dynamic_parset, (bytes, memoryview, bytearray)):
        dynamic_parset = _dynamic_buffer_to_obj(bytes(dynamic_parset))
    elif isinstance(dynamic_parset, str):
        dynamic_parset = _dynamic_str_to_obj(dynamic_parset)

    tolist = getattr(dynamic_parset, "tolist", None)
    if callable(tolist):
        dynamic_parset = tolist()

    if not isinstance(dynamic_parset, list):
        raise TypeError(
            f"dynamic_parset must be list or pickled/JSON buffer, not {type(dynamic_parset).__name__}"
        )

    beam_root = ""
    if prefix:
        key = f"{prefix}.beam_root"
        for item in dynamic_parset:
            if isinstance(item, dict) and key in item:
                beam_root = str(item.get(key) or "").strip()
                break

    if beam_root:
        tool_dir = {
            "Cimager": "imager",
            "imcontsub": "imcontsub",
            "linmos": "linmos",
        }.get(prefix, "")
        if tool_dir:
            os.makedirs(os.path.join(beam_root, tool_dir), exist_ok=True)

    for item in dynamic_parset:
        for key, value in item.items():
            if prefix:
                # Update only if key starts with prefix
                if key.startswith(prefix):
                    if key in static_parset:
                        static_parset[key]["value"] = value
                    else:
                        static_parset[key] = {
                            "value": value,
                            "type": "string",
                            "description": "",
                        }
            else:
                # Update all keys if no prefix is provided
                if key in static_parset:
                    static_parset[key]["value"] = value
                else:
                    static_parset[key] = {
                        "value": value,
                        "type": "string",
                        "description": "",
                    }

    _drop_conflicting_parent_cimager_image_keys(static_parset, prefix)

    serialp = "\n".join([f"{x}={y['value']}" for x, y in static_parset.items()])

    return serialp


def extract_beam_root(dynamic_parset, prefix: str) -> str:
    prefix = (prefix or "").strip()
    tolist = getattr(dynamic_parset, "tolist", None)
    if callable(tolist):
        dynamic_parset = tolist()

    dp = dynamic_parset
    if not isinstance(dp, (list, dict)):
        dp = _unwrap_dlg_port_layer(dp)

    if isinstance(dp, (bytes, bytearray, memoryview)):
        dp = _unwrap_dlg_port_layer(dp)
    if isinstance(dp, str):
        try:
            dp = json.loads(dp)
        except Exception:
            pass

    key = f"{prefix}.beam_root"
    beam_root = ""
    if isinstance(dp, list):
        for item in dp:
            if isinstance(item, dict) and key in item:
                beam_root = str(item.get(key) or "").strip()
                break
    elif isinstance(dp, dict) and key in dp:
        beam_root = str(dp.get(key) or "").strip()

    if not beam_root:
        return ""

    allowed_root = os.path.abspath(_resolve_staging_root_arg(None) or os.getcwd())
    # Make absolute inside the DALiuGE session workspace.
    if not os.path.isabs(beam_root):
        beam_root = _safe_join(allowed_root, beam_root)
    else:
        beam_root = os.path.abspath(beam_root)
        if os.path.commonpath([allowed_root, beam_root]) != allowed_root:
            raise ManifestValidationError("beam_root escapes the DALiuGE workspace")

    out_dir = beam_root
    os.makedirs(out_dir, exist_ok=True)
    if not os.path.isdir(out_dir):
        raise RuntimeError(f"beam_root directory was not created: {out_dir!r}")

    # FileDROP wants a directory path ending with "/" if we want DALiuGE to
    # auto-generate a unique filename in that directory.
    if not out_dir.endswith(os.sep):
        out_dir = out_dir + os.sep
    return out_dir


# Code to download files from casda
def download_file(
    url: str,
    check_exists: bool,
    output: str,
    timeout: int,
    buffer: int = 4194304,
    checksum_url: Optional[str] = None,
    expected_filename: Optional[str] = None,
) -> str:
    """
    Downloads a file from the specified URL to the given output directory.
    Downloads atomically and validates any manifest-supplied MD5 checksum. If
    ``expected_filename`` is available, a valid local file is reused before the
    signed data URL is opened, so an expired URL does not break a safe retry.

    Parameters
    ----------
    url:
        URL of the file to download.
    check_exists:
        If True, checks if the file already exists in the output directory and has the
        same size; skips download if so.
    output:
        Path to the directory where the file will be saved.
    timeout:
        Maximum time in seconds to wait for a server response.
    buffer:
        Buffer size for reading data in chunks during download (default is 4MB).
    checksum_url:
        Optional URL to a CASDA MD5 ``.checksum`` file.
    expected_filename:
        Manifest filename. Preferred over untrusted response headers and used for
        the pre-network idempotency check.

    Returns
    -------
    str
        The path of the downloaded file.

    Raises
    ------
    ManifestDownloadError
        On HTTP errors (e.g. 403 expired URL), network/URL errors, timeout, or
        incomplete transfer. Unhandled, this fails the DALiuGE app so the run is
        marked failed.
    """

    safe_url = _validate_remote_url(url, "download URL")
    if buffer <= 0:
        raise ValueError("buffer must be greater than zero")
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")

    output = os.path.abspath(str(output or "."))
    os.makedirs(output, exist_ok=True)
    if not os.path.isdir(output):
        raise ManifestDownloadError("download output is not a directory")

    filename = _safe_download_filename(
        expected_filename or _filename_from_url(safe_url), "download filename"
    )
    filepath = _safe_join(output, filename)
    expected_hex = ""
    if checksum_url and str(checksum_url).strip():
        expected_hex, _local_checksum_path = _fetch_checksum_to_workspace(
            str(checksum_url), timeout=timeout
        )

    if check_exists and os.path.isfile(filepath) and expected_hex:
        algo = _infer_hash_algo_from_hex(expected_hex)
        actual_hex = _hash_file(filepath, algo)
        if actual_hex == expected_hex:
            print(f"File exists with matching checksum, ignoring: {filename}")
            return filepath

    temporary_path = ""
    try:
        with urllib.request.urlopen(safe_url, timeout=timeout) as response:
            response_filename = response.info().get_filename()
            if expected_filename is None and response_filename:
                response_filename = _safe_download_filename(
                    response_filename, "response filename"
                )
                filepath = _safe_join(output, response_filename)
                filename = response_filename

            content_length = response.info().get("Content-Length")
            expected_size = int(content_length) if content_length is not None else None
            file_handle, temporary_path = tempfile.mkstemp(
                prefix=f".{filename}.", suffix=".part", dir=output
            )
            count = 0
            with os.fdopen(file_handle, "wb") as stream:
                while True:
                    chunk = response.read(buffer)
                    if not chunk:
                        break
                    stream.write(chunk)
                    count += len(chunk)
                stream.flush()
                os.fsync(stream.fileno())

        if expected_size is not None and count != expected_size:
            raise ManifestDownloadError(
                f"Incomplete download for {_redact_url(safe_url)}: got {count} bytes, "
                f"expected {expected_size}"
            )
        if expected_hex:
            algo = _infer_hash_algo_from_hex(expected_hex)
            actual_hex = _hash_file(temporary_path, algo)
            if actual_hex != expected_hex:
                raise ManifestDownloadError(
                    f"Checksum mismatch for {filename}: expected {expected_hex}, "
                    f"got {actual_hex}"
                )
        os.replace(temporary_path, filepath)
        temporary_path = ""
        print(f"Download complete: {filename} ({count} bytes)")
        return filepath
    except ManifestDownloadError:
        raise
    except HTTPError as error:
        raise ManifestDownloadError(
            f"HTTP {error.code} downloading {_redact_url(safe_url)}"
        ) from None
    except (URLError, TimeoutError) as error:
        raise ManifestDownloadError(
            f"Network failure downloading {_redact_url(safe_url)} "
            f"({type(error).__name__})"
        ) from None
    except Exception as error:
        raise ManifestDownloadError(
            f"Download failed for {_redact_url(safe_url)} ({type(error).__name__})"
        ) from None
    finally:
        if temporary_path:
            try:
                os.unlink(temporary_path)
            except FileNotFoundError:
                pass


def _tar_member_name_safe(name: str) -> bool:
    n = (name or "").replace("\\", "/").strip()
    path = PurePosixPath(n)
    if (
        not n
        or not path.parts
        or n.startswith("/")
        or ".." in path.parts
        or re.match(r"^[A-Za-z]:", n)
        or any(ord(char) < 32 or ord(char) == 127 for char in n)
    ):
        return False
    return True


def _validate_tar_member(info: tarfile.TarInfo) -> None:
    if not _tar_member_name_safe(info.name):
        raise ManifestDownloadError(f"Unsafe archive member name: {info.name!r}")
    if info.issym() or info.islnk():
        raise ManifestDownloadError(f"Archive links are not permitted: {info.name!r}")
    if not (info.isdir() or info.isfile()):
        raise ManifestDownloadError(f"Unsupported archive member type: {info.name!r}")


def _extract_regular_tar_member(
    archive: tarfile.TarFile, info: tarfile.TarInfo, staging_dir: str
) -> None:
    normalized_name = (info.name or "").replace("\\", "/")
    path_parts = [
        part for part in PurePosixPath(normalized_name).parts if part not in {"", "."}
    ]
    destination = _safe_join(staging_dir, *path_parts)
    if info.isdir():
        os.makedirs(destination, exist_ok=True)
        return

    os.makedirs(os.path.dirname(destination), exist_ok=True)
    source = archive.extractfile(info)
    if source is None:
        raise ManifestDownloadError(f"Unable to read archive member: {info.name!r}")
    temporary_path = f"{destination}.part"
    try:
        with source, open(temporary_path, "wb") as target:
            shutil.copyfileobj(source, target, length=4 * 1024 * 1024)
            target.flush()
            os.fsync(target.fileno())
        if os.path.getsize(temporary_path) != info.size:
            raise ManifestDownloadError(
                f"Incomplete archive member {info.name!r}: expected {info.size} bytes"
            )
        os.replace(temporary_path, destination)
    finally:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass


def _publish_extracted_tree(staging_dir: str, output_dir: str) -> None:
    """Merge a fully validated temporary extraction into its final workspace."""
    output_root = Path(output_dir).resolve()
    for source_path in Path(staging_dir).rglob("*"):
        relative_path = source_path.relative_to(staging_dir)
        destination = Path(output_dir, relative_path)
        current = Path(output_dir)
        for component in relative_path.parts:
            current = current / component
            if current.is_symlink():
                raise ManifestDownloadError(
                    f"Extraction destination contains a symbolic link: {relative_path}"
                )
        if os.path.commonpath([str(output_root), str(destination.resolve())]) != str(
            output_root
        ):
            raise ManifestDownloadError("Extraction destination escapes output root")
    for child in Path(staging_dir).iterdir():
        destination = Path(output_dir, child.name)
        if child.is_dir():
            shutil.copytree(child, destination, dirs_exist_ok=True)
        else:
            os.replace(child, destination)


def _eval_calibration_tar_wanted_member(info: tarfile.TarInfo) -> bool:
    name = (info.name or "").replace("\\", "/").lstrip("./")
    if not _tar_member_name_safe(name):
        return False
    if info.isdir():
        return False
    # Match exactly one non-empty .../LinmosBeamImages/<something>.cube.fits.
    if "/LinmosBeamImages/" not in name and not name.startswith("LinmosBeamImages/"):
        return False
    return info.size > 0 and name.lower().endswith(".cube.fits")


# Function to un-tar files
def untar_file(
    tar_file: str,
    output_dir: str = ".",
    member_filter: Optional[Callable[[tarfile.TarInfo], bool]] = None,
    expected_member_count: Optional[int] = None,
):
    """
    Extracts a tar file (.tar, .tar.gz, .tar.bz2) to the specified output directory.

    Parameters
    ----------
    tar_file:
        Path to the tar file to extract.
    output_dir:
        Directory where the contents will be extracted. Defaults to the current directory.
    Returns
    -------
    None

    """
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    try:
        with tarfile.open(tar_file) as archive:
            members = archive.getmembers()
            for member in members:
                _validate_tar_member(member)
            to_extract = (
                members
                if member_filter is None
                else [member for member in members if member_filter(member)]
            )
            if not to_extract:
                raise ManifestDownloadError(
                    f"No matching members in archive {os.path.basename(tar_file)!r}"
                )
            if (
                expected_member_count is not None
                and len(to_extract) != expected_member_count
            ):
                raise ManifestDownloadError(
                    f"Expected exactly {expected_member_count} matching member(s) in "
                    f"archive {os.path.basename(tar_file)!r}; found {len(to_extract)}"
                )
            with tempfile.TemporaryDirectory(
                prefix=".beampipe-extract-", dir=output_dir
            ) as staging_dir:
                for member in to_extract:
                    _extract_regular_tar_member(archive, member, staging_dir)
                _publish_extracted_tree(staging_dir, output_dir)
        print(
            f"Extracted {len(to_extract)} member(s) from "
            f"{os.path.basename(tar_file)!r} to {output_dir}"
        )
    except ManifestDownloadError:
        raise
    except Exception as error:
        raise ManifestDownloadError(
            f"Failed to extract {os.path.basename(tar_file)!r} "
            f"({type(error).__name__})"
        ) from None


def _beam_dir_from_ms_tar_name(name: str) -> str:
    """
    Infer the beam directory name (beamNN) from an MS tar filename.

    Supports:
    - pilot: "..._beam25_..." or "..._beam25..." (case-insensitive)
    - WALLABY: "..._B00..." / "..._B14..." (case-insensitive)
    """
    s = (name or "").strip()

    # Legacy
    m = re.search(r"_beam(\d+)(?:_|\b)", s, flags=re.IGNORECASE)
    if m:
        return f"beam{int(m.group(1))}"

    # New
    matches = re.findall(r"(?:^|_)B(\d{2})(?:_|\b)", s, flags=re.IGNORECASE)
    if matches:
        return f"beam{int(matches[-1])}"

    return "beam"


def _ms_dataset_basename(name: str) -> str:
    s = (name or "").strip()
    if s.endswith(".tar"):
        s = s[: -len(".tar")]
    base = os.path.basename(s)
    if base.lower().endswith(".ms"):
        return base
    return f"{base}.ms" if base else ""


def _logical_field_id(name: str) -> str:
    s = (name or "").strip()
    if s.endswith(".tar"):
        s = s[: -len(".tar")]
    s = os.path.basename(s)
    if s.lower().endswith(".ms"):
        s = s[: -len(".ms")]
    if s.lower().startswith("image."):
        s = s[len("image.") :]
    if s.lower().endswith(".ms"):
        s = s[: -len(".ms")]
    return s


def _restored_cube_base(image_stem: str) -> str:
    fid = _logical_field_id(image_stem)
    return f"image.restored.{fid}"


def _contsub_cube_base(image_stem: str) -> str:
    return f"{_restored_cube_base(image_stem)}.contsub"


def _contsub_holo_outname(image_stem: str) -> str:
    return f"{_restored_cube_base(image_stem)}.contsub_holo"


def _ms_directory_complete(path: str) -> bool:
    """Recognise a completed MeasurementSet, including pre-marker legacy runs."""
    if not os.path.isdir(path):
        return False
    return os.path.isfile(os.path.join(path, ".beampipe-extracted")) or os.path.isfile(
        os.path.join(path, "table.dat")
    )


def _mark_ms_directory_complete(path: str) -> None:
    if not os.path.isdir(path):
        raise ManifestDownloadError(
            f"Archive did not create expected MeasurementSet {os.path.basename(path)!r}"
        )
    _atomic_write_bytes(os.path.join(path, ".beampipe-extracted"), b"ok\n")


def _normalize_evaluation_layout(path: str) -> None:
    """Place selected PB FITS at ``eval/LinmosBeamImages`` for parset lookup."""
    if not os.path.isdir(path):
        return
    target_dir = _safe_join(path, "LinmosBeamImages")
    candidates = glob.glob(
        os.path.join(path, "**", "LinmosBeamImages", "*.cube.fits"), recursive=True
    )
    for candidate in candidates:
        if not os.path.isfile(candidate) or os.path.getsize(candidate) <= 0:
            continue
        os.makedirs(target_dir, exist_ok=True)
        destination = _safe_join(
            target_dir, _safe_download_filename(os.path.basename(candidate), "FITS name")
        )
        if os.path.abspath(candidate) == destination:
            continue
        if os.path.exists(destination):
            if _hash_file(destination, "md5") != _hash_file(candidate, "md5"):
                raise ManifestDownloadError(
                    f"Conflicting evaluation FITS filename: {os.path.basename(candidate)!r}"
                )
            continue
        temporary_path = f"{destination}.part"
        try:
            shutil.copyfile(candidate, temporary_path)
            os.replace(temporary_path, destination)
        finally:
            try:
                os.unlink(temporary_path)
            except FileNotFoundError:
                pass


def _evaluation_already_extracted(path: str) -> bool:
    _normalize_evaluation_layout(path)
    candidates = [
        candidate
        for candidate in glob.glob(os.path.join(path, "LinmosBeamImages", "*.cube.fits"))
        if os.path.isfile(candidate) and os.path.getsize(candidate) > 0
    ]
    if len(candidates) > 1:
        raise ManifestDownloadError(
            "Expected exactly one non-empty LinmosBeamImages/*.cube.fits file; "
            f"found {len(candidates)}"
        )
    return bool(candidates)


def degrees_to_hms(degrees: float) -> tuple:
    """
    Convert RA given in degrees to hours-minutes-seconds.

    Parameters
    ----------
    degrees:
        The RA angle in degrees to be converted.

    Returns
    -------
    tuple
        A tuple (h, m, s) where, h (int): hours component of RA, m (int): minutes
        component of RA and s (float): seconds component of RA.
    """

    hours = degrees / 15.0  # Convert degrees to hours
    h = int(hours)  # Integer part of hours
    m = int((hours - h) * 60)  # Integer part of minutes
    s = (hours - h - m / 60.0) * 3600  # Seconds

    return h, m, s


def degrees_to_dms(degrees) -> tuple:
    """
    Convert DEC given in degrees to degrees-minutes-seconds.

    Parameters
    ----------
    degrees:
        The DEC angle in degrees to be converted.

    Returns
    -------
    tuple
        A tuple (d, m, s) where d (int): degrees component of the angle, m (int): minutes
        component of the angle and s (float): seconds component of the angle.
    """

    d = int(degrees)  # Integer part of degrees
    m = int(abs(degrees - d) * 60)  # Integer part of minutes
    s = (abs(degrees) - abs(d) - m / 60.0) * 3600  # Seconds

    return d, m, s


# Test imager
def imager():
    """
    Generates a unique filename for the imager output with a 'image_N.fits' format,
    creates the file and prints a confirmation message with the filename created.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    # Base filename
    base_name = "image"
    extension = ".fits"
    filename = f"{base_name}{extension}"
    counter = 1

    # Check if the file already exists and find the next available filename
    while os.path.exists(filename):
        counter += 1
        filename = f"{base_name}_{counter}{extension}"

    # Create the new file
    with open(filename, "w") as file:
        file.write("")

    print("imager step complete")
    print(f"Output file created: {filename}")


# Test imcontsub
def imcontsub():
    """
    Generates a unique filename for the imcontsub output with a 'image_N.contsub.fits'
     extension, creates the file and prints a confirmation message with the filename
     created.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    # Base filename
    base_name = "image"
    extension = ".contsub.fits"
    filename = f"{base_name}{extension}"
    counter = 1

    # Check if the file already exists and find the next available filename
    while os.path.exists(filename):
        counter += 1
        filename = f"{base_name}_{counter}{extension}"

    # Create the new file
    with open(filename, "w") as file:
        file.write("")

    print("imcontsub step complete")
    print(f"Output file created: {filename}")


# Test linmos
def linmos():
    """
    Generates a unique filename for the imcontsub output with a
    'image_N.contsub_holo.fits' extension,
    creates the file and prints a confirmation message with the filename created.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    # Base filename
    base_name = "image"
    extension = ".contsub_holo.fits"
    filename = f"{base_name}{extension}"
    counter = 1

    # Check if the file already exists and find the next available filename
    while os.path.exists(filename):
        counter += 1
        filename = f"{base_name}_{counter}{extension}"

    # Create the new file
    with open(filename, "w") as file:
        file.write("")

    print("linmos step complete")
    print(f"Output file created: {filename}")


# Test mosaic
def mosaic():
    """
    Generates unique filenames for the final mosaic output and corresponding weights
    file, both with a '.10arc.final_mosaic.fits' extension, creates each file and prints
    confirmation messages with the filenames created.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    # Base filename
    base_name = "image"
    extension = ".10arc.final_mosaic.fits"
    filename = f"{base_name}{extension}"
    counter = 1

    # Check if the file already exists and find the next available filename
    while os.path.exists(filename):
        counter += 1
        filename = f"{base_name}_{counter}{extension}"

    # Create the new file
    with open(filename, "w") as file:
        file.write("")

    # Repeat the same for weights
    weights_name = "weights"
    weights_filename = f"{weights_name}{extension}"
    weights_counter = 1

    # Check if the file already exists and find the next available filename
    while os.path.exists(weights_filename):
        weights_counter += 1
        weights_filename = f"{weights_name}_{weights_counter}{extension}"

    # Create the new file
    with open(weights_filename, "w") as file:
        file.write("")

    print("mosaic step complete")
    print(f"Output file created: {filename}")
    print(f"Output file created: {weights_filename}")


def download_data_ms(
    credentials: str,
    input_csv: str,
    catalogue_name: str,
    timeout_seconds: int,
    project_code: str,
    ms_urls_json: Optional[str] = None,
    staging_root: Optional[str] = None,
):
    """
    Downloads and untars the .ms files for a given HIPASS source.

    If ms_urls_json is provided (JSON list of staged URLs), downloads from those
    URLs and skips TAP query and CASDA staging. Otherwise uses existing TAP/stage flow.

    Parameters
    ----------
    credentials:
        Path to the CASDA credentials file.
    input_csv:
        Path to the input CSV file with source names.
    catalogue_name:
        Path to the catalogue of already processed sources.
    timeout_seconds:
        Timeout setting in seconds for download operations.
    project_code:
        Code of the project.
    ms_urls_json:
        JSON string list of staged MS/tar URLs (from manifest). Required; TAP/CASDA staging
        is handled by beampipe-core.

    Returns
    -------
    None

    Raises
    ------
    ManifestDownloadError
        If any manifest URL fails (HTTP error, timeout, etc.). Stops other parallel
        downloads and fails the step for run tracking.
    """
    ms_urls_json = _normalize_urls_json_arg(ms_urls_json, 2)
    if not ms_urls_json:
        raise ValueError("manifest input required; ms_urls_json must be provided")
    raw = json.loads(ms_urls_json)
    staging_root = _resolve_staging_root_arg(staging_root)
    workspace_root = os.path.abspath(staging_root or os.getcwd())
    items = []
    for u in raw:
        if isinstance(u, str):
            if not u.endswith("checksum"):
                items.append(
                    {
                        "url": _validate_remote_url(u, "MS URL"),
                        "name": _filename_from_url(u),
                        "checksum_url": "",
                        "source_identifier": "",
                        "sbid": "",
                    }
                )
        elif isinstance(u, dict) and u.get("url"):
            source_identifier = str(u.get("source_identifier") or "").strip()
            sbid = str(u.get("sbid") or "").strip()
            if source_identifier or sbid:
                source_identifier = _safe_path_segment(
                    source_identifier, "source_identifier"
                )
                sbid = _safe_path_segment(sbid, "sbid")
            items.append(
                {
                    "url": _validate_remote_url(u["url"], "MS URL"),
                    "name": _safe_download_filename(
                        u.get("name") or _filename_from_url(u["url"]),
                        "MS filename",
                    ),
                    "checksum_url": u.get("checksum_url") or "",
                    "source_identifier": source_identifier,
                    "sbid": sbid,
                }
            )
    file_records = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    failed = False
    try:
        futures = {}
        for item in items:
            stage_dir = workspace_root
            expected_ms = ""
            if item["source_identifier"] and item["sbid"]:
                stage_dir = _safe_join(
                    workspace_root, item["source_identifier"], item["sbid"]
                )
                ms_name = item["name"]
                if ms_name.endswith(".tar"):
                    ms_name = ms_name[: -len(".tar")]
                extract_root = _safe_join(stage_dir, _beam_dir_from_ms_tar_name(ms_name))
                expected_ms = _safe_join(extract_root, _ms_dataset_basename(ms_name))
                if _ms_directory_complete(expected_ms):
                    print(
                        f"MeasurementSet already extracted, skipping remote access: "
                        f"{os.path.basename(expected_ms)}"
                    )
                    continue
            future = executor.submit(
                download_file,
                url=item["url"],
                check_exists=True,
                output=stage_dir,
                timeout=timeout_seconds,
                checksum_url=item["checksum_url"] or None,
                expected_filename=item["name"],
            )
            futures[future] = (item, expected_ms)
        for future in concurrent.futures.as_completed(futures):
            item, expected_ms = futures[future]
            file_records.append((future.result(), item, expected_ms))
    except Exception:
        failed = True
        raise
    finally:
        executor.shutdown(wait=not failed, cancel_futures=failed)
    for file, item, expected_ms in file_records:
        if not file.endswith(".tar") or not tarfile.is_tarfile(file):
            raise ManifestDownloadError(
                f"MS download is not a readable tar archive: {os.path.basename(file)!r}"
            )
        ms_name = item["name"]
        if ms_name.endswith(".tar"):
            ms_name = ms_name[: -len(".tar")]
        extract_root = workspace_root
        if item["source_identifier"] and item["sbid"]:
            extract_root = _safe_join(
                workspace_root,
                item["source_identifier"],
                item["sbid"],
                _beam_dir_from_ms_tar_name(ms_name),
            )
        if expected_ms and _ms_directory_complete(expected_ms):
            continue
        untar_file(file, extract_root)
        if expected_ms:
            _mark_ms_directory_complete(expected_ms)
    print(".ms files downloaded (from manifest URLs)")


def download_data_eval(
    credentials: str,
    input_csv: str,
    catalogue_name: str,
    timeout_seconds: int,
    project_code: str,
    eval_urls_json: Optional[str] = None,
    staging_root: Optional[str] = None,
):
    """
    Downloads and untars the evaluation files for a given HIPASS source.

    If eval_urls_json is provided (JSON list of staged eval URLs), downloads from
    those URLs and skips TAP queries and CASDA staging. Otherwise uses existing flow.

    Parameters
    ----------
    credentials:
        Path to the CASDA credentials file.
    input_csv:
        Path to the input CSV file with source names.
    catalogue_name:
        Path to the catalogue of already processed sources.
    timeout_seconds:
        Timeout setting in seconds for download operations.
    project_code:
        Code of the project.
    eval_urls_json:
        JSON string list of staged evaluation tar URLs (from manifest). Required; TAP/CASDA
        staging is handled by beampipe-core.

    Returns
    -------
    None

    Raises
    ------
    ManifestDownloadError
        If any manifest URL fails (HTTP error, timeout, etc.).
    """
    eval_urls_json = _normalize_urls_json_arg(eval_urls_json, 3)
    if not eval_urls_json:
        raise ValueError("manifest input required; eval_urls_json must be provided")
    raw = json.loads(eval_urls_json)
    staging_root = _resolve_staging_root_arg(staging_root)
    workspace_root = os.path.abspath(staging_root or os.getcwd())
    items = []
    for u in raw:
        if isinstance(u, str):
            items.append(
                {
                    "url": _validate_remote_url(u, "evaluation URL"),
                    "name": _filename_from_url(u),
                    "checksum_url": "",
                    "source_identifier": "",
                    "sbid": "",
                }
            )
        elif isinstance(u, dict) and u.get("url"):
            source_identifier = str(u.get("source_identifier") or "").strip()
            sbid = str(u.get("sbid") or "").strip()
            if source_identifier or sbid:
                source_identifier = _safe_path_segment(
                    source_identifier, "source_identifier"
                )
                sbid = _safe_path_segment(sbid, "sbid")
            items.append(
                {
                    "url": _validate_remote_url(u["url"], "evaluation URL"),
                    "name": _safe_download_filename(
                        u.get("name") or _filename_from_url(u["url"]),
                        "evaluation filename",
                    ),
                    "checksum_url": u.get("checksum_url") or "",
                    "source_identifier": source_identifier,
                    "sbid": sbid,
                }
            )
    for item in items:
        download_dir = workspace_root
        extract_root = workspace_root
        if item["source_identifier"] and item["sbid"]:
            download_dir = _safe_join(
                workspace_root, item["source_identifier"], item["sbid"]
            )
            extract_root = _safe_join(download_dir, "eval")
            if _evaluation_already_extracted(extract_root):
                print(
                    "Evaluation FITS already extracted, skipping remote access: "
                    f"{item['source_identifier']}/{item['sbid']}"
                )
                continue
        path = download_file(
            url=item["url"],
            check_exists=True,
            output=download_dir,
            timeout=timeout_seconds,
            checksum_url=item["checksum_url"] or None,
            expected_filename=item["name"],
        )
        if not path.endswith(".tar") or not tarfile.is_tarfile(path):
            raise ManifestDownloadError(
                "Evaluation download is not a readable tar archive: "
                f"{os.path.basename(path)!r}"
            )
        # Avoid unpacking multi-GB calibration tarballs: only LinmosBeamImages FITS.
        untar_file(
            path,
            extract_root,
            member_filter=_eval_calibration_tar_wanted_member,
            expected_member_count=1,
        )
        if not _evaluation_already_extracted(extract_root):
            raise ManifestDownloadError(
                f"Evaluation archive {os.path.basename(path)!r} did not produce a "
                "single non-empty LinmosBeamImages/*.cube.fits file"
            )
    print("Evaluation files downloaded (from manifest URLs)")


def process_CSV_mosaic(filename: str) -> list:
    """
    Reads a CSV file and processes its contents, returning a list of dictionaries.

    Parameters
    ----------
    filename:
        The name of the CSV file to be read.

    Returns
    -------
    list
        A list of dictionaries containing the dynamic parset generated for mosacicking
        for the HIPASS source.
    """
    # List to store the source dictionary
    data = []

    # Lists to store image and weight filenames
    linmos_images_string = []
    weights_images_string = []

    # Open the .csv file
    with open(filename, "r") as file:
        # Create a CSV reader
        reader = csv.reader(file)

        # Skip the first row (header)
        next(reader)

        # Process each row
        for idx, row in enumerate(reader):
            # Extract individual parameters
            name = str(row[0]).strip()
            source_identifier = str(row[6]).strip() if len(row) >= 7 else ""
            sbid = str(row[7]).strip() if len(row) >= 8 else ""
            if bool(source_identifier) != bool(sbid):
                raise ManifestValidationError(
                    "CSV source_identifier and sbid must be provided together"
                )

            if name:  # Only process if 'name' is not empty
                # Extract the base name from 'name'
                name_no_tar = name[: -len(".tar")] if name.endswith(".tar") else name
                field_id = _logical_field_id(name_no_tar)
                extracted_name = name_no_tar.split("_")[0]
                beam_dir = _beam_dir_from_ms_tar_name(name_no_tar)

                # Generate the file names
                prefix = ""
                if source_identifier and sbid:
                    source_identifier = _safe_path_segment(
                        source_identifier, "CSV source_identifier"
                    )
                    sbid = _safe_path_segment(sbid, "CSV sbid")
                    prefix = f"{source_identifier}/{sbid}/"
                linmos_image = f"{prefix}{beam_dir}/{_contsub_holo_outname(field_id)}"
                weight_image = f"{prefix}{beam_dir}/weights.{field_id}.contsub_holo"

                # Append to the lists
                linmos_images_string.append(linmos_image)
                weights_images_string.append(weight_image)

                # Add the 'outname' and 'outweight' only for the first row
                if idx == 0:
                    out_prefix = f"{source_identifier}/" if source_identifier else ""
                    output_dict = {
                        "linmos.outname": f"{out_prefix}image.{extracted_name}.10arc.final_mosaic",
                        "linmos.outweight": f"{out_prefix}weights.{extracted_name}.10arc.final_mosaic",
                    }
                    data.append(output_dict)

    if linmos_images_string and weights_images_string:
        data.append(
            {
                "linmos.names": "[" + ",".join(linmos_images_string) + "]",
                "linmos.weights": "[" + ",".join(weights_images_string) + "]",
            }
        )

    # Check if data is not empty and print a message
    if data:
        print(f"CSV file '{filename}' successfully read and processed into a list of \
                dictionaries.")
    else:
        print(f"Warning: CSV file '{filename}' is empty.")

    return data


def process_CSV(filename: str) -> list:
    """
    Reads a CSV file and processes its contents, returning a list of dictionaries.

    Parameters
    ----------
    filename:
        The name of the CSV file to be read.

    Returns
    -------
    list

        A list of dictionaries representing each processed row of the CSV file.
    """

    # List to store the source dictionary
    data = []

    # Opening the .csv file
    with open(filename, "r") as file:
        # Create a CSV reader
        reader = csv.reader(file)

        # Skip the header row
        next(reader)

        # Read and process each row, including the header
        for row in reader:

            # Extract individual parameters
            name = str(row[0]).strip()
            RA_string = str(row[1]).strip()
            Dec_string = str(row[2]).strip()
            Vsys = float(row[3])
            evaluation_file = row[5].strip()

            # Create the desired output dictionary
            fid = _logical_field_id(name)
            output_dict = {
                "Cimager.dataset": f"$DLG_ROOT/testdata/{_ms_dataset_basename(name)}",
                "Cimager.Images.Names": f"[image.{fid}]",
                "Cimager.Images.direction": f"[{RA_string},{Dec_string}, J2000]",
                "Cimager.write.weightsimage": "true",
                "Vsys": Vsys,
                "imcontsub.inputfitscube": _restored_cube_base(name),
                "imcontsub.outputfitscube": _contsub_cube_base(name),
                "linmos.names": f"[{_contsub_cube_base(name)}]",
                "linmos.weights": f"[weights.{fid}]",
                "linmos.outname": _contsub_holo_outname(name),
                "linmos.outweight": f"weights.{fid}.contsub_holo",
                "linmos.feeds.centre": f"[{RA_string},{Dec_string}]",
                f"linmos.feeds.image.restored.{fid}.contsub": "[0.0,0.0]",
                "linmos.primarybeam.ASKAP_PB.image": evaluation_file,
            }

            data.append(output_dict)

        # Check if the file is empty
        if not data:
            print(f"Warning: CSV file '{filename}' is empty.")
        else:
            print(f"CSV file '{filename}' successfully read and processed into a list of \
                    dictionaries.")

    return data


def process_CSV_str(csv_string: str) -> list:
    """
    Processes a CSV string into a list of parset dicts, then returns **pickle
    bytes** of that list.

    PyFunc ``_match_parser`` uses the **output port** encoding; if it does not
    match the drop, the engine defaults to **dill** and scatter reads **pickle**.
    Returning explicit pickle bytes with graph ports ``source_list`` /
    InMemory / GenericScatterApp ``array`` all set to **raw** avoids that
    mismatch and matches ``GenericScatterApp`` (``pickle.loads`` on raw bytes).

    Parameters
    ----------
    csv_string:
        The CSV data as a string.

    Returns
    -------
    list
        List of per-row parset dictionaries.
    """
    csv_text = _normalize_dlg_csv_string(csv_string)

    # List to store the source dictionary
    data = []

    # Convert the CSV string to a file-like object
    csv_file = io.StringIO(csv_text)

    # Create a CSV reader
    reader = csv.reader(csv_file)

    # Skip the header row
    next(reader)

    # Read and process each row
    for row in reader:

        # Extract individual parameters
        try:
            name = str(row[0]).strip()
        except Exception as e:
            raise ValueError(f"Missing or invalid 'name' (row[0]): {e}")
        try:
            RA_string = str(row[1]).strip()
        except Exception as e:
            raise ValueError(f"Missing or invalid 'RA_string' (row[1]): {e}")
        try:
            Dec_string = str(row[2]).strip()
        except Exception as e:
            raise ValueError(f"Missing or invalid 'Dec_string' (row[2]): {e}")
        try:
            Vsys = float(row[3])
        except Exception as e:
            raise ValueError(f"Missing or invalid 'Vsys' (row[3]): {e}")
        try:
            evaluation_file = row[5].strip()
        except Exception as e:
            raise ValueError(f"Missing or invalid 'evaluation_file' (row[5]): {e}")

        # name,ra_string,dec_string,vsys,,evaluation_file (index 5)

        source_identifier = ""
        sbid = ""
        if len(row) >= 8:
            source_identifier = str(row[6]).strip()
            sbid = str(row[7]).strip()
        if bool(source_identifier) != bool(sbid):
            raise ManifestValidationError(
                "CSV source_identifier and sbid must be provided together"
            )
        ms_name = name
        if ms_name.endswith(".tar"):
            ms_name = ms_name[: -len(".tar")]
        ms_dir = _ms_dataset_basename(ms_name)
        field_id = _logical_field_id(ms_name)
        beam_dir = _beam_dir_from_ms_tar_name(ms_name)
        beam_root = ""
        if source_identifier and sbid:
            source_identifier = _safe_path_segment(
                source_identifier, "CSV source_identifier"
            )
            sbid = _safe_path_segment(sbid, "CSV sbid")
            workspace_root = os.path.abspath(
                _resolve_staging_root_arg(None) or os.getcwd()
            )
            beam_root = _safe_join(workspace_root, source_identifier, sbid, beam_dir)
            dataset_path = os.path.join(beam_root, ms_dir)
            eval_rel = str(evaluation_file).lstrip("/").strip()
            marker = "LinmosBeamImages/"
            if marker in eval_rel:
                eval_rel = marker + eval_rel.split(marker, 1)[1]
            else:
                parts = eval_rel.split("/", 1)
                if len(parts) == 2 and parts[0].startswith(
                    "calibration-metadata-processing-logs-"
                ):
                    eval_rel = parts[1]
            if not _tar_member_name_safe(eval_rel):
                raise ManifestValidationError(
                    "CSV evaluation_file contains an unsafe path"
                )
            evaluation_file = _safe_join(
                workspace_root,
                source_identifier,
                sbid,
                "eval",
                *PurePosixPath(eval_rel.replace("\\", "/")).parts,
            )
            evaluation_file = _resolve_primary_beam_cube_path(evaluation_file)
        else:
            if os.path.isabs(ms_name):
                dataset_path = ms_name
            else:
                rel_dir = os.path.dirname(ms_name)
                dataset_path = os.path.join(rel_dir, ms_dir) if rel_dir else ms_dir

        # Create the desired output dictionary
        output_dict = {
            "Cimager.dataset": f'"{dataset_path}"',
            "Cimager.beam_root": beam_root if (source_identifier and sbid) else "",
            "Cimager.Images.Names": f"[image.{field_id}]",
            "Cimager.Images.direction": f"[{RA_string},{Dec_string}, J2000]",
            "Cimager.write.weightsimage": "true",
            "Vsys": Vsys,
            "imcontsub.beam_root": beam_root if (source_identifier and sbid) else "",
            "imcontsub.inputfitscube": _restored_cube_base(ms_name),
            "imcontsub.outputfitscube": _contsub_cube_base(ms_name),
            "linmos.names": f"[{_contsub_cube_base(ms_name)}]",
            "linmos.weights": f"[weights.{field_id}]",
            "linmos.outname": _contsub_holo_outname(ms_name),
            "linmos.outweight": f"weights.{field_id}.contsub_holo",
            "linmos.feeds.centre": f"[{RA_string},{Dec_string}]",
            f"linmos.feeds.image.restored.{field_id}.contsub": "[0.0,0.0]",
            "linmos.beam_root": beam_root if (source_identifier and sbid) else "",
            "linmos.primarybeam.ASKAP_PB.image": evaluation_file,
        }

        data.append(output_dict)

    # Check if the file is empty
    if not data:
        print("Warning: CSV data is empty.")
    else:
        print("Dynamic parsets for imager, imcontsub and linmos created")

    return data


def process_CSV_mosaic_str(csv_string: str) -> bytes:
    """
    Processes a CSV string and returns strict UTF-8 JSON bytes

    Parameters
    ----------
    csv_string:
        The CSV processed data as a string.

    Returns
    -------
    bytes
        UTF-8 encoded JSON array of mosaic parset objects.
    """
    csv_text = _normalize_dlg_csv_string(csv_string)

    # List to store the source dictionary
    data = []

    # Lists to store image and weight filenames
    linmos_images_string = []
    weights_images_string = []

    # Convert the CSV string to a file-like object
    csv_file = io.StringIO(csv_text)

    # Create a CSV reader
    reader = csv.reader(csv_file)

    # Skip the first row (header)
    next(reader)
    # Process each row
    for idx, row in enumerate(reader):
        # Extract individual parameters
        name = str(row[0]).strip()
        source_identifier = str(row[6]).strip() if len(row) >= 7 else ""
        sbid = str(row[7]).strip() if len(row) >= 8 else ""
        if bool(source_identifier) != bool(sbid):
            raise ManifestValidationError(
                "CSV source_identifier and sbid must be provided together"
            )

        if name:  # Only process if 'name' is not empty
            # Extract the base name from 'name'
            name_no_tar = name[: -len(".tar")] if name.endswith(".tar") else name
            field_id = _logical_field_id(name_no_tar)
            extracted_name = name_no_tar.split("_")[0]
            beam_dir = _beam_dir_from_ms_tar_name(name_no_tar)

            # Generate the file names
            prefix = ""
            if source_identifier and sbid:
                source_identifier = _safe_path_segment(
                    source_identifier, "CSV source_identifier"
                )
                sbid = _safe_path_segment(sbid, "CSV sbid")
                prefix = f"{source_identifier}/{sbid}/"
            # linmos.imagetype=fits makes linmos append ".fits" automatically.
            linmos_image = f"{prefix}{beam_dir}/{_contsub_holo_outname(field_id)}"
            weight_image = f"{prefix}{beam_dir}/weights.{field_id}.contsub_holo"

            # Append to the lists
            linmos_images_string.append(linmos_image)
            weights_images_string.append(weight_image)

            # Add the 'outname' and 'outweight' only for the first row
            if idx == 0:
                out_prefix = f"{source_identifier}/" if source_identifier else ""
                output_dict = {
                    "linmos.outname": f"{out_prefix}image.{extracted_name}.10arc.final_mosaic",
                    "linmos.outweight": f"{out_prefix}weights.{extracted_name}.10arc.final_mosaic",
                }
                data.append(output_dict)

    # Append the final lists to the data.
    if linmos_images_string and weights_images_string:
        data.append(
            {
                "linmos.names": "[" + ",".join(linmos_images_string) + "]",
                "linmos.weights": "[" + ",".join(weights_images_string) + "]",
            }
        )

    # Check if data is not empty and print a message
    if data:
        print("Dynamic parset for mosaic created")
    else:
        print("Warning: CSV data is empty.")

    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
