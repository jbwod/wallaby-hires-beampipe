#!/usr/bin/env python3

import json
import os
import sys

from wallaby_hires.funcs import (
    _build_csv_string_from_dataset_rows,
    _flatten_sources_to_dataset_rows,
    prestage_manifest_inputs,
    process_CSV_mosaic_str,
    process_CSV_str,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_MANIFEST = os.path.join(SCRIPT_DIR, "test_staging_e2e_manifest.json")


def main():
    manifest_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MANIFEST
    if not os.path.exists(manifest_path):
        sys.exit(1)

    with open(manifest_path, "rb") as f:
        manifest_bytes = f.read()

    manifest = json.loads(manifest_bytes.decode("utf-8"))

    # Show full output for critical functions so we can check format.

    # 1. _flatten_sources_to_dataset_rows
    print("Rows from _flatten_sources_to_dataset_rows:")
    rows = _flatten_sources_to_dataset_rows(manifest)
    print(
        json.dumps(rows, indent=2)[:2000]
    )  # Print only first 2000 chars to avoid huge output

    # 2. _build_csv_string_from_dataset_rows
    print("\nCSV string from _build_csv_string_from_dataset_rows:")
    csv_string = _build_csv_string_from_dataset_rows(rows)
    csv_lines = csv_string.strip().split("\n")
    for line in csv_lines[:10]:
        print(line)
    if len(csv_lines) > 10:
        print(f"... ({len(csv_lines) - 10} more lines)")

    # 3. prestage_manifest_inputs
    print("\nResult of prestage_manifest_inputs:")
    cred_path, csv_str, ms_urls_json, eval_urls_json = prestage_manifest_inputs(
        manifest_bytes
    )
    print("credentials_path:", cred_path)
    print("csv_str preview:", repr(csv_str[:200]) + ("..." if len(csv_str) > 200 else ""))
    print(
        "ms_urls_json preview:",
        repr(ms_urls_json[:200]) + ("..." if len(ms_urls_json) > 200 else ""),
    )
    print(
        "eval_urls_json preview:",
        repr(eval_urls_json[:200]) + ("..." if len(eval_urls_json) > 200 else ""),
    )

    # 4. process_CSV_str
    print("\nprocess_CSV_str first result(s):")
    parsets = process_CSV_str(csv_str)
    for p in parsets[:2]:
        print(json.dumps(p, indent=2))
    if len(parsets) > 2:
        print(f"... ({len(parsets) - 2} more parsets)")

    # 5. process_CSV_mosaic_str
    print("\nprocess_CSV_mosaic_str first result(s):")
    mosaic_parsets = process_CSV_mosaic_str(csv_str)
    for p in mosaic_parsets[:2]:
        print(json.dumps(p, indent=2))
    if len(mosaic_parsets) > 2:
        print(f"... ({len(mosaic_parsets) - 2} more mosaic parsets)")


if __name__ == "__main__":
    main()
