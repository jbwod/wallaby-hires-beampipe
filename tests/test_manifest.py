import json

import pytest

from wallaby_hires.funcs import (
    ManifestValidationError,
    _flatten_sources_to_dataset_rows,
    prestage_manifest_inputs,
    process_CSV_str,
    validate_manifest,
)


def manifest() -> dict:
    return {
        "inputs": {},
        "sources": [
            {
                "source_identifier": "HIPASSJ1318-21",
                "ra_string": "13h18m58.0s",
                "dec_string": "-21.2.11.0",
                "vsys": 668,
                "sbids": [
                    {
                        "sbid": "34166",
                        "evaluation_file": "evaluation.tar",
                        "evaluation_file_url": "https://example.test/evaluation.tar?token=secret",
                        "evaluation_file_checksum_url": "https://example.test/evaluation.tar.checksum",
                        "datasets": [
                            {
                                "name": "HIPASSJ1318-21_A_beam25_10arc_split.ms.tar",
                                "staged_url": "https://example.test/beam25.ms.tar?token=secret",
                                "checksum_url": "https://example.test/beam25.ms.tar.checksum",
                            }
                        ],
                    }
                ],
            }
        ],
    }


def test_nested_manifest_flattens_and_preserves_download_names():
    document = manifest()

    assert validate_manifest(document) is document
    rows = _flatten_sources_to_dataset_rows(document)
    credentials, csv_text, ms_json, evaluation_json = prestage_manifest_inputs(
        json.dumps(document).encode()
    )

    assert len(rows) == 1
    assert rows[0]["source_identifier"] == "HIPASSJ1318-21"
    assert credentials.endswith("inputs/casda.ini")
    assert "HIPASSJ1318-21_A_beam25_10arc_split.ms.tar" in csv_text
    assert json.loads(ms_json)[0]["name"].endswith(".ms.tar")
    assert json.loads(evaluation_json)[0]["name"] == "evaluation.tar"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda doc: doc["sources"][0].update(source_identifier="../outside"),
            "path segment",
        ),
        (lambda doc: doc["sources"][0].pop("sbids"), "sbids"),
        (
            lambda doc: doc["sources"][0]["sbids"][0]["datasets"][0].update(
                name="../../payload.tar"
            ),
            "safe basename",
        ),
        (
            lambda doc: doc["sources"][0]["sbids"][0]["datasets"][0].update(
                staged_url="file:///etc/passwd"
            ),
            r"HTTP\(S\)",
        ),
    ],
)
def test_manifest_rejects_incomplete_or_unsafe_rows(mutation, message):
    document = manifest()
    mutation(document)

    with pytest.raises(ManifestValidationError, match=message):
        validate_manifest(document)


def test_documented_direct_dataset_shape_is_rejected_instead_of_silently_empty():
    document = manifest()
    source = document["sources"][0]
    source["datasets"] = source.pop("sbids")[0]["datasets"]

    with pytest.raises(ManifestValidationError, match="sbids"):
        _flatten_sources_to_dataset_rows(document)


def test_csv_path_identifiers_cannot_escape_workspace():
    csv_text = (
        "Name,RA_string,Dec_string,Vsys,,evaluation_file,source_identifier,sbid\n"
        "beam.ms.tar,1h,2.0,3,,LinmosBeamImages/pb.fits,../outside,34166\n"
    )

    with pytest.raises(ManifestValidationError, match="path segment"):
        process_CSV_str(csv_text)
