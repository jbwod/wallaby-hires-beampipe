import json

import pytest

from wallaby_hires.__main__ import main
from wallaby_hires.funcs import mosaic
from wallaby_hires.outputs import (
    OutputValidationError,
    build_output_inventory,
    build_staging_output_inventory,
    publish_output_inventory,
    verify_output_inventory,
    verify_output_products,
)


def _products(root):
    image = root / "HIPASSJ1318-21/image.HIPASSJ1318-21.10arc.final_mosaic.fits"
    weights = root / "HIPASSJ1318-21/weights.HIPASSJ1318-21.10arc.final_mosaic.fits"
    image.parent.mkdir(parents=True)
    image.write_bytes(b"science-image")
    weights.write_bytes(b"science-weights")
    return image, weights


def test_no_download_mosaic_creates_nonempty_synthetic_products(tmp_path, monkeypatch):
    monkeypatch.setenv("BEAMPIPE_OUTPUT_ROOT", str(tmp_path))

    mosaic()

    image = tmp_path / "image.10arc.final_mosaic.fits"
    weights = tmp_path / "weights.10arc.final_mosaic.fits"
    assert image.read_bytes() == b"WALLABY no-download synthetic mosaic\n"
    assert weights.read_bytes() == b"WALLABY no-download synthetic mosaic weights\n"


def test_inventory_records_and_reverifies_nonempty_final_products(tmp_path):
    image, weights = _products(tmp_path)
    inventory_path = tmp_path / "wallaby-output-inventory.json"

    document = build_output_inventory(tmp_path, inventory_path=inventory_path)

    assert document["schema"] == "wallaby-hires-output-inventory/v1"
    assert {product["bytes"] for product in document["products"]} == {
        len(b"science-image"),
        len(b"science-weights"),
    }
    assert all(len(product["sha256"]) == 64 for product in document["products"])
    assert verify_output_inventory(tmp_path, inventory_path) == document

    image.write_bytes(b"changed")
    with pytest.raises(OutputValidationError, match="changed after inventory"):
        verify_output_inventory(tmp_path, inventory_path)


def test_inventory_rejects_missing_empty_and_symlink_products(tmp_path):
    image, weights = _products(tmp_path)
    weights.write_bytes(b"")
    with pytest.raises(OutputValidationError, match="empty"):
        build_output_inventory(tmp_path)

    weights.write_bytes(b"weights")
    image.unlink()
    image.symlink_to(weights)
    with pytest.raises(OutputValidationError, match="no outputs matched"):
        build_output_inventory(tmp_path)


def test_publication_copies_and_verifies_inventory(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "published"
    _products(source)
    inventory = build_output_inventory(source)

    published = publish_output_inventory(source, destination, inventory)

    assert published == inventory
    assert (
        verify_output_inventory(
            destination, destination / "wallaby-output-inventory.json"
        )
        == inventory
    )


def test_daliuge_wrapper_writes_machine_readable_inventory(tmp_path):
    _products(tmp_path)

    result = json.loads(verify_output_products(str(tmp_path)))

    assert result["inventory_sha256"]
    assert (tmp_path / "wallaby-output-inventory.json").is_file()


def test_staging_inventory_is_closed_over_environment_root(tmp_path, monkeypatch):
    _products(tmp_path)
    monkeypatch.setenv("WALLABY_HIRES_STAGING_ROOT", str(tmp_path))

    document = build_staging_output_inventory()

    assert document["pattern_counts"]
    assert (tmp_path / "wallaby-output-inventory.json").is_file()


def test_cli_validates_manifest_and_outputs(tmp_path, capsys, monkeypatch):
    manifest = {
        "sources": [
            {
                "source_identifier": "HIPASSJ1318-21",
                "ra_string": "1h",
                "dec_string": "-2.0",
                "vsys": 3,
                "sbids": [
                    {
                        "sbid": "34166",
                        "evaluation_file": "calibration-metadata-processing-logs-SB34166_2021-12-31-011733.tar",
                        "evaluation_file_url": "https://example.test/calibration-metadata-processing-logs-SB34166_2021-12-31-011733.tar",
                        "evaluation_file_checksum_url": "https://example.test/calibration-metadata-processing-logs-SB34166_2021-12-31-011733.tar.checksum",
                        "datasets": [
                            {
                                "name": "beam.ms.tar",
                                "staged_url": "https://example.test/beam.ms.tar",
                                "checksum_url": "https://example.test/beam.ms.tar.checksum",
                            }
                        ],
                    }
                ],
            }
        ]
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    assert main(["validate-manifest", str(manifest_path)]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "mode": "setonix-production",
        "sources": 1,
        "valid": True,
    }

    _products(tmp_path / "outputs")
    assert main(["inventory-outputs", str(tmp_path / "outputs")]) == 0
    assert json.loads(capsys.readouterr().out)["products"]

    monkeypatch.setenv("WALLABY_HIRES_STAGING_ROOT", str(tmp_path / "outputs"))
    assert main(["inventory-staging-outputs"]) == 0
    assert json.loads(capsys.readouterr().out)["products"]
