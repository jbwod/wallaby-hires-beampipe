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
import hashlib
import io
import json
import pickle
from typing import Optional

# Importing required modules
import os
import re
import tarfile
import urllib
import urllib.request
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlparse

PRESTAGE_INPUTS_DIR = "inputs"
CHECKSUMS_DIR = os.path.join(PRESTAGE_INPUTS_DIR, "checksums")


class ManifestDownloadError(RuntimeError):
    """
    Raised when a manifest-URL download fails (HTTP error, timeout, incomplete file).
    Propagates to DALiuGE so the run is marked failed for tracking.
    """

# Suffix to append to evaluation_file for linmos primary beam path (inside extracted tar)
EVALUATION_FILE_PATH_SUFFIX = "LinmosBeamImages/akpb.iquv.square_6x6.54.1295MHz.SB32736.cube.fits"


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
        raise TypeError(
            f"expected str, tuple, or buffer, got {type(value).__name__}"
        )
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
        if (
            len(obj) == 4
            and isinstance(obj[0], str)
            and isinstance(obj[1], str)
        ):
            return obj[1]
        raise TypeError(
            "csv_string out prestage_manifest_inputs, "
            f"got length {len(obj)}"
        )


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


def _download_url_to_path(url: str, path: str, timeout: int = 300) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with urllib.request.urlopen(url, timeout=timeout) as r:
        with open(path, "wb") as f:
            f.write(r.read())
    print(f"Downloaded {url} -> {path}")


def _write_text_atomic(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def _fetch_checksum_to_workspace(checksum_url: str, timeout: int = 300) -> tuple[str, str]:
    """
    Download checksum text into the current workspace and return
    (expected_hex, checksum_file_path). If checksum_url is empty, returns ("","").
    """
    if not checksum_url or not str(checksum_url).strip():
        return "", ""
    parsed = urlparse(checksum_url)
    name = os.path.basename(parsed.path) or "download.checksum"
    local_path = os.path.join(os.getcwd(), CHECKSUMS_DIR, name)
    with urllib.request.urlopen(checksum_url, timeout=timeout) as r:
        content = r.read().decode("utf-8", errors="replace").strip()
    _write_text_atomic(local_path, content + "\n")
    parts = content.split()
    expected = (parts[0] if parts else "").strip().lower()
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


def _flatten_sources_to_dataset_rows(manifest: dict) -> list:
    """
    Flatten manifest sources[].sbids[].datasets[] into a list of dataset rows.
    Each row has name, ra_string, dec_string, vsys, evaluation_file, staged_url,
    checksum_url, evaluation_file_url, evaluation_file_checksum_url.
    """
    rows = []
    sources = manifest.get("sources") or []
    for src in sources:
        if not isinstance(src, dict):
            continue
        source_identifier = src.get("source_identifier") or src.get("name") or src.get("source") or ""
        ra = src.get("ra_string") or ""
        dec = src.get("dec_string") or ""
        vsys = src.get("vsys")
        for sbid_group in src.get("sbids") or []:
            if not isinstance(sbid_group, dict):
                continue
            sbid = sbid_group.get("sbid") or ""
            evaluation_file = sbid_group.get("evaluation_file") or ""
            evaluation_file_url = sbid_group.get("evaluation_file_url") or ""
            evaluation_file_checksum_url = sbid_group.get("evaluation_file_checksum_url") or ""
            for ds in sbid_group.get("datasets") or []:
                if not isinstance(ds, dict):
                    continue
                name = ds.get("name") or ds.get("dataset_id") or ""
                rows.append({
                    "source_identifier": source_identifier,
                    "sbid": str(sbid) if sbid is not None else "",
                    "name": name,
                    "ra_string": ra or ds.get("ra_string") or "",
                    "dec_string": dec or ds.get("dec_string") or "",
                    "vsys": vsys if vsys is not None else ds.get("vsys"),
                    "evaluation_file": evaluation_file,
                    "staged_url": ds.get("staged_url") or "",
                    "checksum_url": ds.get("checksum_url") or "",
                    "evaluation_file_url": evaluation_file_url,
                    "evaluation_file_checksum_url": evaluation_file_checksum_url,
                })
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
    writer.writerow(["Name", "RA_string", "Dec_string", "Vsys", "", "evaluation_file", "source_identifier", "sbid"])
    for r in rows:
        vsys = r.get("vsys")
        vsys_str = "" if vsys is None else str(vsys)
        eval_file = r.get("evaluation_file", "")
        # Match original: evaluation_file_path = evaluation_file.replace(".tar", f"/{suffix}")
        eval_file_path = (
            eval_file.replace(".tar", f"/{EVALUATION_FILE_PATH_SUFFIX}")
            if eval_file
            else ""
        )
        writer.writerow([
            r.get("name", ""),
            r.get("ra_string", ""),
            r.get("dec_string", ""),
            vsys_str,
            "",
            eval_file_path,
            r.get("source_identifier", ""),
            r.get("sbid", ""),
        ])
    return buf.getvalue()


def prestage_manifest_inputs(manifest_bytes: bytes) -> tuple:
    """
    Parse manifest JSON; download only credentials (casda.ini); build csv_string and
    URL lists from sources/datasets. Returns 4-tuple for manifest-driven pipeline.

    Manifest format (preferred): inputs.credentials_ini_url; sources[] (nested) or
    datasets[] (flat). See wallaby-hires-beampipe/manifest_schema.md.

    Legacy: if sources/datasets absent, uses inputs.input_csv_url (content as csv_string)
    and staged.ms_urls / staged.eval_urls.

    Returns
    -------
    tuple[str, str, str, str]
        (credentials_path, csv_string, ms_urls_json, eval_urls_json)
    """
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    inputs = manifest.get("inputs") or {}
    staged = manifest.get("staged") or {}

    cred_url = inputs.get("credentials_ini_url") or inputs.get("casda_ini_url") or ""
    base = os.path.join(os.getcwd(), PRESTAGE_INPUTS_DIR)
    credentials_path = os.path.join(base, "casda.ini")
    if cred_url:
        _download_url_to_path(cred_url, credentials_path)

    rows = _flatten_sources_to_dataset_rows(manifest)
    if rows:
        csv_string = _build_csv_string_from_dataset_rows(rows)
        ms_urls = [
            {
                "url": r["staged_url"],
                "checksum_url": r.get("checksum_url") or "",
                "source_identifier": r.get("source_identifier") or "",
                "sbid": r.get("sbid") or "",
            }
            for r in rows
            if r.get("staged_url")
        ]
        seen_eval: set[str] = set()
        eval_urls = []
        for r in rows:
            url = r.get("evaluation_file_url") or ""
            if url and url not in seen_eval:
                seen_eval.add(url)
                eval_urls.append({
                    "url": url,
                    "checksum_url": r.get("evaluation_file_checksum_url") or "",
                    "source_identifier": r.get("source_identifier") or "",
                    "sbid": r.get("sbid") or "",
                })
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
            {"url": u, "checksum_url": ""} if isinstance(u, str) else u
            for u in raw_eval
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
            output_dict = {
                "Cimager.dataset": f"$DLG_ROOT/testdata/{name}.ms",
                "Cimager.Images.Names": f"[image.{name}]",
                "Cimager.Images.direction": f"[{RA_string},{Dec_string}, J2000]",
                "Cimager.write.weightsimage": "true",
                "Vsys": Vsys,
                "imcontsub.inputfitscube": f"image.restored.{name}",
                "imcontsub.outputfitscube": f"image.restored.{name}.contsub",
                "linmos.names": f"[image.restored.{name}.contsub]",
                "linmos.weights": f"[weights.{name}]",
                "linmos.outname": f"image.restored.{name}.contsub_holo",
                "linmos.outweight": f"weights.{name}.contsub_holo",
                "linmos.feeds.centre": f"[{RA_beam_string},{Dec_beam_string}]",
                f"linmos.feeds.image.restored.{name}.contsub": "[0.0,0.0]",
                "linmos.primarybeam.ASKAP_PB.image": evaluation_file,
            }

            data.append(output_dict)

        # Check if the file is empty
        if not data:
            print(f"Warning: CSV file '{filename}' is empty.")
        else:
            print(
                f"CSV file '{filename}' successfully read and processed into a list of \
                    dictionaries."
            )

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
        raise ValueError(
            "dynamic_parset is not JSON and is not valid base64"
        ) from e
    return _dynamic_buffer_to_obj(raw)


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

    # Make absolute inside the DALiuGE session workspace.
    if not os.path.isabs(beam_root):
        beam_root = os.path.abspath(os.path.join(os.getcwd(), beam_root))

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
) -> str:
    """
    Downloads a file from the specified URL to the given output directory.
    If a file with the same name already exists, it increments a counter in
    the filename to avoid overwriting.
    If checksum_url is provided, verifies the downloaded file via CASDA SHA-1 checksum.

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
        Optional URL to CASDA .checksum file for SHA-1 verification after download.

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

    # Large timeout is necessary as the file may need to be staged from tape

    try:
        os.makedirs(output, exist_ok=True)
    except Exception:
        output = "."

    if url is None:
        raise ManifestDownloadError("URL is empty")

    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            filename = r.info().get_filename()
            if not filename:
                parsed = urlparse(url)
                for val in parse_qs(parsed.query).get(
                    "response-content-disposition", []
                ):
                    m = re.search(r'filename="?([^";]+)"?', val)
                    if m:
                        filename = unquote(m.group(1))
                        break
                if not filename:
                    filename = os.path.basename(parsed.path.rstrip("/")) or "download"
            filepath = f"{output}/{filename}"

            http_size = int(r.info()["Content-Length"])

            should_overwrite = False
            if check_exists:
                try:
                    if os.path.exists(filepath):
                        if checksum_url and str(checksum_url).strip():
                            expected_hex, _local_checksum_path = _fetch_checksum_to_workspace(
                                checksum_url, timeout=timeout
                            )
                            if expected_hex:
                                algo = _infer_hash_algo_from_hex(expected_hex)
                                actual_hex = _hash_file(filepath, algo)
                                if actual_hex == expected_hex:
                                    print(
                                        f"File exists with matching checksum, ignoring: "
                                        f"{os.path.basename(filepath)}"
                                    )
                                    return filepath
                                print(
                                    f"Checksum mismatch re-downloading: {os.path.basename(filepath)} "
                                    f"(expected {expected_hex}, got {actual_hex})"
                                )
                                should_overwrite = True
                        else:
                            file_size = os.path.getsize(filepath)
                            if file_size == http_size:
                                print(f"File exists ignoring: {os.path.basename(filepath)}")
                                return filepath
                            should_overwrite = True
                except FileNotFoundError:
                    pass

            target_path = filepath
            tmp_path = None
            if should_overwrite and os.path.exists(target_path):
                tmp_path = f"{target_path}.tmp"
                filepath = tmp_path

            print(f"Downloading: {filepath} size: {http_size}")
            count = 0
            with open(filepath, "wb") as o:
                while http_size > count:
                    buff = r.read(buffer)
                    if not buff:
                        break
                    o.write(buff)
                    count += len(buff)

            download_size = os.path.getsize(filepath)
            if http_size != download_size:
                raise ManifestDownloadError(
                    f"Incomplete download for {url!r}: got {download_size} bytes, "
                    f"expected {http_size}"
                )

            if tmp_path:
                os.replace(tmp_path, target_path)
                filepath = target_path

            print(f"Download complete: {os.path.basename(filepath)}")
            if checksum_url and str(checksum_url).strip():
                expected_hex, _local_checksum_path = _fetch_checksum_to_workspace(
                    checksum_url, timeout=timeout
                )
                if expected_hex:
                    algo = _infer_hash_algo_from_hex(expected_hex)
                    actual_hex = _hash_file(filepath, algo)
                    if actual_hex != expected_hex:
                        raise ManifestDownloadError(
                            f"Checksum mismatch for {os.path.basename(filepath)}: "
                            f"expected {expected_hex}, got {actual_hex}"
                        )

            return filepath
    except HTTPError as e:
        raise ManifestDownloadError(
            f"HTTP {e.code} {e.reason!r} for URL {url!r}"
        ) from e
    except URLError as e:
        raise ManifestDownloadError(
            f"Network error for URL {url!r}: {e.reason!r}"
        ) from e
    except TimeoutError as e:
        raise ManifestDownloadError(
            f"Timed out after {timeout}s for URL {url!r}"
        ) from e


# Function to un-tar files
def untar_file(tar_file: str, output_dir: str = "."):
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
    try:
        os.makedirs(output_dir, exist_ok=True)
        with tarfile.open(tar_file) as tar:
            tar.extractall(path=output_dir)
            print(f"{tar_file} un-tarred to {output_dir}")

    except Exception as e:
        print(f"Failed to untar {tar_file}: {e}")


def _beam_dir_from_ms_tar_name(name: str) -> str:
    # Match both \"_beam25_\" and \"_beam25\" forms.
    m = re.search(r"_beam(\d+)(?:_|\b)", name or "")
    if not m:
        return "beam"
    return f"beam{int(m.group(1))}"


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
    items = []
    for u in raw:
        if isinstance(u, str):
            if not u.endswith("checksum"):
                items.append({"url": u, "checksum_url": "", "source_identifier": "", "sbid": ""})
        elif isinstance(u, dict) and u.get("url"):
            items.append({
                "url": u["url"],
                "checksum_url": u.get("checksum_url") or "",
                "source_identifier": u.get("source_identifier") or "",
                "sbid": str(u.get("sbid") or ""),
            })
    file_list = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    failed = False
    try:
        futures = []
        for item in items:
            stage_dir = "."
            if staging_root and item.get("source_identifier") and item.get("sbid"):
                stage_dir = os.path.join(staging_root, item["source_identifier"], str(item["sbid"]))
            futures.append(executor.submit(
                download_file,
                url=item["url"],
                check_exists=True,
                output=stage_dir,
                timeout=timeout_seconds,
                checksum_url=item["checksum_url"] or None,
            ))
        for future in concurrent.futures.as_completed(futures):
            file_list.append(future.result())
    except Exception:
        failed = True
        raise
    finally:
        executor.shutdown(wait=not failed, cancel_futures=failed)
    for file in file_list:
        if file.endswith(".tar") and tarfile.is_tarfile(file):
            base = os.path.basename(file)
            ms_name = base[: -len(".tar")] if base.endswith(".tar") else base
            extract_root = os.getcwd()
            # try to find item metadata by filename match
            src = ""
            sbid = ""
            for it in items:
                if isinstance(it, dict) and ms_name in str(it.get("url") or ""):
                    src = it.get("source_identifier") or ""
                    sbid = it.get("sbid") or ""
                    break
            if src and sbid:
                beam_dir = _beam_dir_from_ms_tar_name(ms_name)
                extract_root = os.path.join(os.getcwd(), src, str(sbid), beam_dir)
            untar_file(file, extract_root)
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
    items = []
    for u in raw:
        if isinstance(u, str):
            items.append({"url": u, "checksum_url": "", "source_identifier": "", "sbid": ""})
        elif isinstance(u, dict) and u.get("url"):
            items.append({
                "url": u["url"],
                "checksum_url": u.get("checksum_url") or "",
                "source_identifier": u.get("source_identifier") or "",
                "sbid": str(u.get("sbid") or ""),
            })
    for item in items:
        download_dir = os.getcwd()
        if staging_root and item.get("source_identifier") and item.get("sbid"):
            download_dir = os.path.join(staging_root, item["source_identifier"], str(item["sbid"]))
        path = download_file(
            url=item["url"],
            check_exists=True,
            output=download_dir,
            timeout=timeout_seconds,
            checksum_url=item["checksum_url"] or None,
        )
        if path.endswith(".tar") and tarfile.is_tarfile(path):
            src = item.get("source_identifier") or ""
            sbid = item.get("sbid") or ""
            extract_root = os.getcwd()
            if src and sbid:
                extract_root = os.path.join(os.getcwd(), src, str(sbid), "eval")
            untar_file(path, extract_root)
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
            sbid = str(row[7]).strip() if len(row) >= 8 else ""

            if name:  # Only process if 'name' is not empty
                # Extract the base name from 'name'
                extracted_name = name.split("_")[0]
                beam_dir = _beam_dir_from_ms_tar_name(name)

                # Generate the file names
                prefix = f"{sbid}/" if sbid else ""
                linmos_image = f"{prefix}{beam_dir}/image.restored.{name}.contsub_holo.fits"
                weight_image = f"{prefix}{beam_dir}/weights.{name}.contsub_holo.fits"

                # Append to the lists
                linmos_images_string.append(linmos_image)
                weights_images_string.append(weight_image)

                # Add the 'outname' and 'outweight' only for the first row
                if idx == 0:
                    output_dict = {
                        "linmos.outname": [f"image.{extracted_name}.10arc.final_mosaic"],
                        "linmos.outweight": [
                            f"weights.{extracted_name}.10arc.final_mosaic"
                        ],
                    }
                    data.append(output_dict)

    # Append the final lists to the data
    if linmos_images_string and weights_images_string:
        data.append(
            {
                "linmos.names": linmos_images_string,
                "linmos.weights": weights_images_string,
            }
        )

    # Check if data is not empty and print a message
    if data:
        print(
            f"CSV file '{filename}' successfully read and processed into a list of \
                dictionaries."
        )
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
            output_dict = {
                "Cimager.dataset": f"$DLG_ROOT/testdata/{name}.ms",
                "Cimager.Images.Names": f"[image.{name}]",
                "Cimager.Images.direction": f"[{RA_string},{Dec_string}, J2000]",
                "Cimager.write.weightsimage": "true",
                "Vsys": Vsys,
                "imcontsub.inputfitscube": f"image.restored.{name}",
                "imcontsub.outputfitscube": f"image.restored.{name}.contsub",
                "linmos.names": f"[image.restored.{name}.contsub]",
                "linmos.weights": f"[weights.{name}]",
                "linmos.outname": f"image.restored.{name}.contsub_holo",
                "linmos.outweight": f"weights.{name}.contsub_holo",
                "linmos.feeds.centre": f"[{RA_string},{Dec_string}]",
                f"linmos.feeds.image.restored.{name}.contsub": "[0.0,0.0]",
                "linmos.primarybeam.ASKAP_PB.image": evaluation_file,
            }

            data.append(output_dict)

        # Check if the file is empty
        if not data:
            print(f"Warning: CSV file '{filename}' is empty.")
        else:
            print(
                f"CSV file '{filename}' successfully read and processed into a list of \
                    dictionaries."
            )

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
        ms_name = name
        if ms_name.endswith(".tar"):
            ms_name = ms_name[: -len(".tar")]
        image_stem = ms_name
        beam_dir = _beam_dir_from_ms_tar_name(ms_name)
        if source_identifier and sbid:
            beam_root = os.path.abspath(
                os.path.join(os.getcwd(), source_identifier, str(sbid), beam_dir)
            )
            dataset_path = os.path.join(beam_root, ms_name)
            eval_rel = str(evaluation_file).lstrip("/").strip()
            marker = "LinmosBeamImages/"
            if marker in eval_rel:
                eval_rel = marker + eval_rel.split(marker, 1)[1]
            else:
                parts = eval_rel.split("/", 1)
                if len(parts) == 2 and parts[0].startswith("calibration-metadata-processing-logs-"):
                    eval_rel = parts[1]
            evaluation_file = os.path.abspath(
                os.path.join(os.getcwd(), source_identifier, str(sbid), "eval", eval_rel)
            )
        else:
            dataset_path = ms_name

        # Create the desired output dictionary
        output_dict = {
            "Cimager.dataset": f"\"{dataset_path}\"",
            "Cimager.beam_root": beam_root if (source_identifier and sbid) else "",
            "Cimager.Images.Names": f"[image.{image_stem}]",
            "Cimager.Images.direction": f"[{RA_string},{Dec_string}, J2000]",
            "Cimager.write.weightsimage": "true",
            "Vsys": Vsys,
            "imcontsub.beam_root": beam_root if (source_identifier and sbid) else "",
            "imcontsub.inputfitscube": f"image.restored.{image_stem}",
            "imcontsub.outputfitscube": f"image.restored.{image_stem}.contsub",
            "linmos.names": f"[image.restored.{image_stem}.contsub]",
            "linmos.weights": f"[weights.{image_stem}]",
            "linmos.outname": f"image.restored.{image_stem}.contsub_holo",
            "linmos.outweight": f"weights.{image_stem}.contsub_holo",
            "linmos.feeds.centre": f"[{RA_string},{Dec_string}]",
            f"linmos.feeds.image.restored.{image_stem}.contsub": "[0.0,0.0]",
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
        sbid = str(row[7]).strip() if len(row) >= 8 else ""

        if name:  # Only process if 'name' is not empty
            # Extract the base name from 'name'
            extracted_name = name.split("_")[0]
            beam_dir = _beam_dir_from_ms_tar_name(name)

            # Generate the file names
            prefix = f"{sbid}/" if sbid else ""
            linmos_image = f"{prefix}{beam_dir}/image.restored.{name}.contsub_holo.fits"
            weight_image = f"{prefix}{beam_dir}/weights.{name}.contsub_holo.fits"

            # Append to the lists
            linmos_images_string.append(linmos_image)
            weights_images_string.append(weight_image)

            # Add the 'outname' and 'outweight' only for the first row
            if idx == 0:
                output_dict = {
                    "linmos.outname": [f"image.{extracted_name}.10arc.final_mosaic"],
                    "linmos.outweight": [f"weights.{extracted_name}.10arc.final_mosaic"],
                }
                data.append(output_dict)

    # Append the final lists to the data
    if linmos_images_string and weights_images_string:
        data.append(
            {
                "linmos.names": linmos_images_string,
                "linmos.weights": weights_images_string,
            }
        )

    # Check if data is not empty and print a message
    if data:
        print("Dynamic parset for mosaic created")
    else:
        print("Warning: CSV data is empty.")

    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
