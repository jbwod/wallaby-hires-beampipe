import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCKER_GRAPH = ROOT / "dlg-graphs/wallaby-hires_deploy-pipeline-beampipe.graph"
SETONIX_GRAPH = ROOT / "dlg-graphs/wallaby-hires_deploy-setonix-beampipe.graph"
TEST_GRAPH = ROOT / "dlg-graphs/wallaby-hires_test-pipeline-beampipe.graph"
NO_DOWNLOAD_GRAPH = (
    ROOT / "dlg-graphs/wallaby-hires_test-pipeline-nodownloads-beampipe.graph"
)
BEAMPIPE_GRAPHS = (DOCKER_GRAPH, SETONIX_GRAPH, TEST_GRAPH, NO_DOWNLOAD_GRAPH)
MANIFEST_FIXTURE = ROOT / "wallaby_hires/test_staging_e2e_manifest.json"
ASKAPSOFT_DIGEST = (
    "csirocass/askapsoft@"
    "sha256:2b0cf3bac871664095cdfc9b13a6f438163d00dc344c3c1db8fcde4eef1aed65"
)
PUBLISH_PATTERNS_JSON = (
    '["**/image.*.10arc.final_mosaic.fits",' '"**/weights.*.10arc.final_mosaic.fits"]'
)
NO_DOWNLOAD_PUBLISH_PATTERNS_JSON = (
    '["**/image*.10arc.final_mosaic.fits",' '"**/weights*.10arc.final_mosaic.fits"]'
)
BEAMPIPE_PALETTE_URL = (
    "https://raw.githubusercontent.com/jbwod/beampipe-pallette/"
    "v0.3.0/daliuge/palettes/beampipe.palette"
)
INGEST_PALETTE_HASH = "086dd223d6f630bb4edb5cf08b89a02aef13d39234f6f7d0f62d43bf0fb3dbd1"
PUBLISH_PALETTE_HASH = "05cd5ef5ccd6c3a8de48707e07c931dfc967bdd287b6890efb25ff735ad75648"
DATA_FIXTURE_SUFFIXES = {
    ".csv",
    ".graph",
    ".ini",
    ".json",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
FIXTURE_DIRECTORIES = {"fixture", "fixtures", "test-data", "testdata", "dlg-testdata"}
BACKUP_SUFFIXES = (".bak", ".backup", ".old", ".orig", ".save", "~")
SECRET_PATTERNS = {
    "AWS signed URL credential": re.compile(
        rb"(?i)\bX-Amz-(?:Algorithm|Credential|Date|Expires|Security-Token|"
        rb"Signature|SignedHeaders)\b"
    ),
    "Google signed URL credential": re.compile(
        rb"(?i)\bX-Goog-(?:Algorithm|Credential|Date|Expires|Signature|SignedHeaders)\b"
    ),
    "legacy AWS query credential": re.compile(rb"(?i)\bAWSAccessKeyId\b"),
    "credential-bearing URL query": re.compile(
        rb"(?i)https?://[^\s\"'<>]*[?&](?:access_token|sig|signature|token)="
    ),
    "private key": re.compile(
        rb"-----BEGIN (?:RSA |DSA |EC |OPENSSH |PGP )?PRIVATE KEY-----"
    ),
    "known token prefix": re.compile(
        rb"(?:\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|"
        rb"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})\b|"
        rb"\bxox[baprs]-[A-Za-z0-9-]{20,}\b|"
        rb"\bAIza[A-Za-z0-9_-]{30,}\b|"
        rb"\b(?:sk|rk)_live_[A-Za-z0-9]{16,}\b)"
    ),
}


def _field_values(path):
    graph = json.loads(path.read_text(encoding="utf-8"))
    return [
        field.get("value")
        for node in graph["nodeDataArray"]
        for field in node.get("fields", [])
    ]


def _graph_nodes(path):
    return json.loads(path.read_text(encoding="utf-8"))["nodeDataArray"]


def _graph(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_docker_askapsoft_images_are_immutable():
    values = _field_values(DOCKER_GRAPH)

    assert values.count(ASKAPSOFT_DIGEST) == 4
    assert not any(
        isinstance(value, str) and "askapsoft:develop" in value for value in values
    )


def test_setonix_graph_uses_deployment_managed_account_and_image():
    values = _field_values(SETONIX_GRAPH)
    commands = "\n".join(value for value in values if isinstance(value, str))

    assert "exec wallaby_hires run-setonix-imager" in commands
    assert "sbatch --wait" not in commands
    assert commands.count("BEAMPIPE_ASKAPSOFT_SIF") >= 3
    assert "pawsey0411" not in commands
    assert "jblackwood" not in commands


def test_one_source_fixture_has_an_explicit_nested_imager_budget():
    manifest = json.loads(MANIFEST_FIXTURE.read_text(encoding="utf-8"))
    dataset_count = sum(
        len(sbid["datasets"])
        for source in manifest["sources"]
        for sbid in source["sbids"]
    )
    values = _field_values(SETONIX_GRAPH)
    command = next(
        value
        for value in values
        if isinstance(value, str) and "run-setonix-imager" in value
    )
    cimager = next(
        node for node in _graph_nodes(SETONIX_GRAPH) if node["name"] == "CimagerStaticNew"
    )
    cimager_fields = {field["name"]: field["value"] for field in cimager["fields"]}

    assert len(manifest["sources"]) == 1
    assert dataset_count == 6
    assert "--partition work" in command
    assert "--nodes 1" in command
    assert "--ntasks 6" in command
    assert "--ntasks-per-node 6" in command
    assert "--cpus-per-task 1" in command
    assert "--memory 4G" in command
    assert "--time-limit 00:50:00" in command

    assert cimager_fields["Cimager.Channels"] == "[250,0]"
    assert cimager_fields["Cimager.nchanpercore"] == 50


def test_setonix_graph_closes_runtime_over_one_root():
    values = _field_values(SETONIX_GRAPH)
    nodes = _graph_nodes(SETONIX_GRAPH)
    mosaic = next(node for node in nodes if node["name"] == "singularity linmos/mosiac")
    mosaic_command = next(
        field["value"] for field in mosaic["fields"] if field["name"] == "command"
    )

    assert values.count("$WALLABY_HIRES_CACHE_ROOT") == 2
    assert "$DLG_ROOT/wallaby_staging_data/" not in values
    assert "wallaby_hires.process_CSV_str_setonix" in values
    assert "wallaby_hires.process_CSV_mosaic_str_setonix" in values
    assert "wallaby_hires.extract_beam_root_setonix" in values
    assert 'cd "$ROOT"' in mosaic_command
    assert "--pwd /askapbuffer" in mosaic_command
    assert "wallaby_hires inventory-staging-outputs" not in mosaic_command


def test_setonix_publisher_is_native_terminal_and_translator_compatible():
    graph = _graph(SETONIX_GRAPH)
    nodes = graph["nodeDataArray"]
    links = graph["linkDataArray"]
    mosaic = next(node for node in nodes if node["name"] == "singularity linmos/mosiac")
    image_drops = [node for node in nodes if node["name"] == "mosiac_image"]
    ready = next(node for node in nodes if node["name"] == "publication ready")
    publisher = next(node for node in nodes if node["name"] == "beampipe-publish")
    inventory = next(node for node in nodes if node["name"] == "output inventory")
    mosaic_fields = {field["name"]: field for field in mosaic["fields"]}
    fields = {field["name"]: field for field in publisher["fields"]}
    ready_fields = {field["name"]: field for field in ready["fields"]}
    inventory_fields = {field["name"]: field for field in inventory["fields"]}

    assert graph["modelData"]["numLGNodes"] == len(nodes)
    assert publisher["category"] == "DALiuGEApp"
    assert publisher["categoryType"] == "Application"
    assert fields["dropclass"]["value"] == "beampipe_pallette.apps.BeampipePublishApp"
    assert publisher["commitHash"] == "0.3.0"
    assert publisher["paletteDownloadUrl"] == BEAMPIPE_PALETTE_URL
    assert publisher["dataHash"] == PUBLISH_PALETTE_HASH
    assert set(fields) == {
        "completion",
        "inventory",
        "expected_patterns_json",
        "log_level",
        "dropclass",
        "base_name",
        "execution_time",
        "num_cpus",
        "group_start",
        "input_error_threshold",
        "n_tries",
    }
    assert fields["expected_patterns_json"]["type"] == "String"
    assert fields["expected_patterns_json"]["value"] == PUBLISH_PATTERNS_JSON
    assert fields["completion"]["id"] == "93045d76-bad3-53fa-aeae-a9f2566b0f53"
    assert fields["inventory"]["id"] == "4563bd54-8666-5d62-b01d-ab1672490702"
    assert fields["expected_patterns_json"]["id"] == (
        "2db1aa2c-b185-57ee-8f75-1680a7d2ecd8"
    )
    assert fields["completion"]["usage"] == "InputPort"
    assert fields["inventory"]["usage"] == "OutputPort"
    assert fields["expected_patterns_json"]["usage"] == "NoPort"
    assert fields["completion"]["encoding"] == "pickle"
    assert fields["inventory"]["encoding"] == "pickle"
    assert ready_fields["filepath"]["value"] == "beampipe-publication-ready"
    assert inventory_fields["filepath"]["value"] == "beampipe-output-inventory.json"
    assert ready_fields["publication_ready"]["usage"] == "InputPort"
    assert ready_fields["completion"]["usage"] == "OutputPort"
    assert inventory_fields["inventory"]["usage"] == "InputPort"
    assert mosaic_fields["publication_ready"]["encoding"] == "path"
    assert ': > "{publication_ready}"' in mosaic_fields["command"]["value"]
    assert mosaic_fields["command"]["value"].endswith(': > "{publication_ready}"')

    mosaic_targets = {link["to"] for link in links if link["from"] == mosaic["id"]}
    assert {node["id"] for node in image_drops} <= mosaic_targets
    assert ready["id"] in mosaic_targets
    mosaic_to_ready = [
        link
        for link in links
        if link["from"] == mosaic["id"] and link["to"] == ready["id"]
    ]
    ready_to_publisher = [
        link
        for link in links
        if link["from"] == ready["id"] and link["to"] == publisher["id"]
    ]
    publisher_to_inventory = [
        link
        for link in links
        if link["from"] == publisher["id"] and link["to"] == inventory["id"]
    ]
    assert len(mosaic_to_ready) == 1
    assert mosaic_to_ready[0]["fromPort"] == mosaic_fields["publication_ready"]["id"]
    assert mosaic_to_ready[0]["toPort"] == ready_fields["publication_ready"]["id"]
    assert len(ready_to_publisher) == 1
    assert ready_to_publisher[0]["fromPort"] == ready_fields["completion"]["id"]
    assert ready_to_publisher[0]["toPort"] == fields["completion"]["id"]
    assert len(publisher_to_inventory) == 1
    assert publisher_to_inventory[0]["fromPort"] == fields["inventory"]["id"]
    assert publisher_to_inventory[0]["toPort"] == inventory_fields["inventory"]["id"]
    assert not any(link["from"] == inventory["id"] for link in links)

    # EAGLE and the translator see a compact alternating app/data chain with
    # every link using an output-capable source and input-capable target port.
    node_by_id = {node["id"]: node for node in nodes}
    field_by_id = {
        field["id"]: field for node in nodes for field in node.get("fields", [])
    }
    for link in mosaic_to_ready + ready_to_publisher + publisher_to_inventory:
        assert (
            node_by_id[link["from"]]["categoryType"]
            != node_by_id[link["to"]]["categoryType"]
        )
        assert field_by_id[link["fromPort"]]["usage"] in {
            "OutputPort",
            "InputOutput",
        }
        assert field_by_id[link["toPort"]]["usage"] in {
            "InputPort",
            "InputOutput",
        }
    assert mosaic["x"] < ready["x"] < publisher["x"] < inventory["x"]
    assert (
        max(
            ready["x"] - mosaic["x"],
            publisher["x"] - ready["x"],
            inventory["x"] - publisher["x"],
        )
        < 300
    )
    assert (
        max(
            abs(ready["y"] - mosaic["y"]),
            abs(publisher["y"] - mosaic["y"]),
            abs(inventory["y"] - mosaic["y"]),
        )
        < 20
    )


def test_no_download_graph_publishes_synthetic_outputs_terminally():
    graph = _graph(NO_DOWNLOAD_GRAPH)
    nodes = graph["nodeDataArray"]
    links = graph["linkDataArray"]
    mosaic = next(node for node in nodes if node["name"] == "mosaic")
    ready = next(node for node in nodes if node["name"] == "publication ready")
    publisher = next(node for node in nodes if node["name"] == "beampipe-publish")
    inventory = next(node for node in nodes if node["name"] == "output inventory")
    mosaic_fields = {field["name"]: field for field in mosaic["fields"]}
    ready_fields = {field["name"]: field for field in ready["fields"]}
    publisher_fields = {field["name"]: field for field in publisher["fields"]}
    inventory_fields = {field["name"]: field for field in inventory["fields"]}

    assert graph["modelData"]["numLGNodes"] == len(nodes)
    assert publisher["category"] == "DALiuGEApp"
    assert publisher["commitHash"] == "0.3.0"
    assert publisher["paletteDownloadUrl"] == BEAMPIPE_PALETTE_URL
    assert publisher["dataHash"] == PUBLISH_PALETTE_HASH
    assert publisher_fields["dropclass"]["value"] == (
        "beampipe_pallette.apps.BeampipePublishApp"
    )
    assert publisher_fields["expected_patterns_json"]["value"] == (
        NO_DOWNLOAD_PUBLISH_PATTERNS_JSON
    )
    assert ready_fields["filepath"]["value"] == "beampipe-publication-ready"
    assert inventory_fields["filepath"]["value"] == ("beampipe-output-inventory.json")

    mosaic_to_ready = [
        link
        for link in links
        if link["from"] == mosaic["id"] and link["to"] == ready["id"]
    ]
    ready_to_publisher = [
        link
        for link in links
        if link["from"] == ready["id"] and link["to"] == publisher["id"]
    ]
    publisher_to_inventory = [
        link
        for link in links
        if link["from"] == publisher["id"] and link["to"] == inventory["id"]
    ]
    assert len(mosaic_to_ready) == 1
    assert mosaic_to_ready[0]["fromPort"] == mosaic_fields["c"]["id"]
    assert mosaic_to_ready[0]["toPort"] == ready_fields["publication_ready"]["id"]
    assert len(ready_to_publisher) == 1
    assert ready_to_publisher[0]["fromPort"] == ready_fields["completion"]["id"]
    assert ready_to_publisher[0]["toPort"] == publisher_fields["completion"]["id"]
    assert len(publisher_to_inventory) == 1
    assert publisher_to_inventory[0]["fromPort"] == publisher_fields["inventory"]["id"]
    assert publisher_to_inventory[0]["toPort"] == inventory_fields["inventory"]["id"]
    assert not any(link["from"] == inventory["id"] for link in links)
    assert mosaic["x"] < ready["x"] < publisher["x"] < inventory["x"]
    assert (
        max(
            abs(ready["y"] - mosaic["y"]),
            abs(publisher["y"] - mosaic["y"]),
            abs(inventory["y"] - mosaic["y"]),
        )
        < 20
    )


def test_beampipe_graphs_use_native_hardened_manifest_transport():
    for path in BEAMPIPE_GRAPHS:
        graph = _graph(path)
        nodes = graph["nodeDataArray"]
        links = graph["linkDataArray"]
        ingest = next(node for node in nodes if node["name"] == "beampipe-ingest")
        manifest_drop = next(node for node in nodes if node["name"] == "manifest_bytes")
        prestage = next(
            node
            for node in nodes
            if any(
                field["name"] == "func_name"
                and str(field["value"]).startswith("wallaby_hires.prestage_manifest")
                for field in node["fields"]
            )
        )
        ingest_fields = {field["name"]: field for field in ingest["fields"]}
        manifest_fields = {field["name"]: field for field in manifest_drop["fields"]}
        prestage_fields = {field["name"]: field for field in prestage["fields"]}

        assert graph["modelData"]["numLGNodes"] == len(nodes)
        assert ingest["category"] == "DALiuGEApp"
        assert ingest["categoryType"] == "Application"
        assert ingest["commitHash"] == "0.3.0"
        assert ingest["paletteDownloadUrl"] == BEAMPIPE_PALETTE_URL
        assert ingest["dataHash"] == INGEST_PALETTE_HASH
        assert ingest_fields["dropclass"]["value"] == (
            "beampipe_pallette.apps.BeampipeIngestApp"
        )
        assert ingest_fields["manifest_path"]["usage"] == "NoPort"
        assert ingest_fields["manifest_bytes"]["usage"] == "OutputPort"
        assert ingest_fields["manifest_bytes"]["encoding"] == "pickle"
        assert not {"func_name", "func_code", "input_parser", "output_parser"} & set(
            ingest_fields
        )
        assert manifest_drop["category"] == "File"
        assert manifest_fields["dropclass"]["value"] == ("dlg.data.drops.file.FileDROP")
        assert manifest_fields["filepath"]["value"] == "beampipe-manifest.pickle"
        assert manifest_fields["manifest_bytes"]["usage"] == "InputOutput"
        assert manifest_fields["manifest_bytes"]["encoding"] == "pickle"
        assert prestage_fields["manifest_bytes"]["encoding"] == "pickle"

        ingest_to_manifest = [
            link
            for link in links
            if link["from"] == ingest["id"] and link["to"] == manifest_drop["id"]
        ]
        manifest_to_prestage = [
            link
            for link in links
            if link["from"] == manifest_drop["id"] and link["to"] == prestage["id"]
        ]
        assert len(ingest_to_manifest) == 1
        assert ingest_to_manifest[0]["fromPort"] == (
            ingest_fields["manifest_bytes"]["id"]
        )
        assert ingest_to_manifest[0]["toPort"] == (
            manifest_fields["manifest_bytes"]["id"]
        )
        assert len(manifest_to_prestage) == 1
        assert manifest_to_prestage[0]["fromPort"] == (
            manifest_fields["manifest_bytes"]["id"]
        )
        assert manifest_to_prestage[0]["toPort"] == (
            prestage_fields["manifest_bytes"]["id"]
        )


def test_no_download_graph_selects_structural_manifest_admission_explicitly():
    values = _field_values(NO_DOWNLOAD_GRAPH)

    assert "wallaby_hires.prestage_manifest_inputs_no_download" in values
    assert "wallaby_hires.prestage_manifest_inputs" not in values


def _is_secret_sensitive_artifact(path):
    name = path.name.lower()
    parts = {part.lower() for part in path.parts}
    is_backup = name.endswith(BACKUP_SUFFIXES) or name.endswith("bak")
    is_named_fixture = (
        "fixture" in name or "manifest" in name
    ) and path.suffix.lower() in DATA_FIXTURE_SUFFIXES
    return (
        path.suffix.lower() == ".graph"
        or bool(parts & FIXTURE_DIRECTORIES)
        or is_named_fixture
        or is_backup
    )


def _tracked_secret_sensitive_artifacts():
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return sorted(
        Path(value.decode("utf-8"))
        for value in result.stdout.split(b"\0")
        if value and _is_secret_sensitive_artifact(Path(value.decode("utf-8")))
    )


def test_tracked_graphs_fixtures_and_backups_do_not_contain_credentials():
    paths = _tracked_secret_sensitive_artifacts()

    assert DOCKER_GRAPH.relative_to(ROOT) in paths
    assert TEST_GRAPH.relative_to(ROOT) in paths
    assert MANIFEST_FIXTURE.relative_to(ROOT) in paths
    assert _is_secret_sensitive_artifact(Path("README.mdbak"))

    for path in paths:
        contents = (ROOT / path).read_bytes()
        for description, pattern in SECRET_PATTERNS.items():
            assert pattern.search(contents) is None, f"{description} found in {path}"
