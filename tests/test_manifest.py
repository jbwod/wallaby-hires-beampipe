import copy
import json

import pytest

from wallaby_hires.funcs import (
    ManifestValidationError,
    ManifestValidationMode,
    _flatten_sources_to_dataset_rows,
    prestage_manifest_inputs,
    prestage_manifest_inputs_no_download,
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


def test_core_dataset_shape_and_numeric_string_normalize_for_graph():
    document = manifest()
    source = document["sources"][0]
    source["vsys"] = "668.0"
    sbid = source["sbids"][0]
    evaluation_file = sbid.pop("evaluation_file")
    evaluation_url = sbid.pop("evaluation_file_url")
    evaluation_checksum_url = sbid.pop("evaluation_file_checksum_url")
    dataset = sbid["datasets"][0]
    dataset["dataset_id"] = dataset.pop("name")
    dataset["evaluation_file"] = evaluation_file
    dataset["evaluation_file_url"] = evaluation_url
    dataset["evaluation_file_checksum_url"] = evaluation_checksum_url

    rows = _flatten_sources_to_dataset_rows(document)
    _, csv_text, ms_json, evaluation_json = prestage_manifest_inputs(
        json.dumps(document).encode()
    )

    assert rows[0]["name"].endswith(".ms.tar")
    assert rows[0]["evaluation_file"] == "evaluation.tar"
    assert json.loads(ms_json)[0]["name"].endswith(".ms.tar")
    assert json.loads(evaluation_json)[0]["name"] == "evaluation.tar"
    assert "668.0" in csv_text


def test_nodownload_manifest_does_not_require_staged_url():
    document = manifest()
    document["sources"][0]["sbids"][0]["datasets"][0].pop("staged_url")

    with pytest.raises(ManifestValidationError, match="staged_url.*required"):
        prestage_manifest_inputs(json.dumps(document).encode())

    assert (
        validate_manifest(document, ManifestValidationMode.STRUCTURAL_NO_DOWNLOAD)
        is document
    )
    _, csv_text, ms_json, _ = prestage_manifest_inputs_no_download(
        json.dumps(document).encode()
    )

    assert "beam25" in csv_text
    assert json.loads(ms_json) == []


def test_setonix_production_rejects_legacy_while_nodownload_is_explicit():
    document = {}

    with pytest.raises(ManifestValidationError, match="sources is required"):
        validate_manifest(document)

    _, csv_text, ms_json, evaluation_json = prestage_manifest_inputs_no_download(
        json.dumps(document).encode()
    )

    assert csv_text.startswith("Name,RA_string")
    assert json.loads(ms_json) == []
    assert json.loads(evaluation_json) == []


def test_setonix_production_requires_exactly_one_source():
    document = manifest()
    document["sources"].append(copy.deepcopy(document["sources"][0]))

    with pytest.raises(ManifestValidationError, match="exactly one source"):
        validate_manifest(document)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda doc: doc["sources"][0]["sbids"][0]["datasets"][0].pop("staged_url"),
            "staged_url.*required",
        ),
        (
            lambda doc: doc["sources"][0]["sbids"][0]["datasets"][0].pop("checksum_url"),
            "checksum_url.*required",
        ),
        (
            lambda doc: doc["sources"][0]["sbids"][0]["datasets"][0].update(
                staged_url="http://example.test/beam25.ms.tar"
            ),
            "staged_url must be an HTTPS URL",
        ),
        (
            lambda doc: doc["sources"][0]["sbids"][0].pop("evaluation_file"),
            "evaluation_file.*required",
        ),
        (
            lambda doc: doc["sources"][0]["sbids"][0].pop("evaluation_file_url"),
            "evaluation_file_url.*required",
        ),
        (
            lambda doc: doc["sources"][0]["sbids"][0].pop("evaluation_file_checksum_url"),
            "evaluation_file_checksum_url.*required",
        ),
        (
            lambda doc: doc["sources"][0]["sbids"][0].update(
                evaluation_file_url="http://example.test/evaluation.tar"
            ),
            "evaluation_file_url must be an HTTPS URL",
        ),
    ],
)
def test_setonix_production_requires_https_staged_inputs_and_checksums(mutation, message):
    document = manifest()
    mutation(document)

    with pytest.raises(ManifestValidationError, match=message):
        validate_manifest(document)


def test_setonix_production_rejects_conflicting_evaluation_archive_metadata():
    document = manifest()
    dataset = document["sources"][0]["sbids"][0]["datasets"][0]
    dataset["evaluation_file_url"] = "https://example.test/different.tar"

    with pytest.raises(ManifestValidationError, match="exactly one evaluation archive"):
        validate_manifest(document)


def test_setonix_prestage_emits_one_evaluation_download_per_sbid():
    document = manifest()
    second = copy.deepcopy(document["sources"][0]["sbids"][0])
    second["sbid"] = "34275"
    document["sources"][0]["sbids"].append(second)

    _, _, _, evaluation_json = prestage_manifest_inputs(json.dumps(document).encode())

    assert [item["sbid"] for item in json.loads(evaluation_json)] == ["34166", "34275"]


def test_unknown_manifest_validation_mode_is_rejected():
    with pytest.raises(ManifestValidationError, match="validation_mode must be one of"):
        validate_manifest(manifest(), "permissive")


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
