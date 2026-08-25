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
MANIFEST_FIXTURE = ROOT / "wallaby_hires/test_staging_e2e_manifest.json"
ASKAPSOFT_DIGEST = (
    "csirocass/askapsoft@"
    "sha256:2b0cf3bac871664095cdfc9b13a6f438163d00dc344c3c1db8fcde4eef1aed65"
)
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
    assert "--time-limit 00:40:00" in command

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


def test_setonix_publisher_is_terminal_project_scoped_and_path_encoded():
    graph = _graph(SETONIX_GRAPH)
    nodes = graph["nodeDataArray"]
    links = graph["linkDataArray"]
    mosaic = next(node for node in nodes if node["name"] == "singularity linmos/mosiac")
    image_drops = [node for node in nodes if node["name"] == "mosiac_image"]
    ready = next(node for node in nodes if node["name"] == "wallaby publication ready")
    publisher = next(
        node for node in nodes if node["name"] == "beampipe-publish wallaby outputs"
    )
    inventory = next(node for node in nodes if node["name"] == "beampipe output inventory")
    mosaic_fields = {field["name"]: field for field in mosaic["fields"]}
    fields = {field["name"]: field for field in publisher["fields"]}
    ready_fields = {field["name"]: field for field in ready["fields"]}
    inventory_fields = {field["name"]: field for field in inventory["fields"]}

    assert fields["func_name"]["value"] == "beampipe_pallette.beampipe_publish"
    assert fields["func_code"]["value"] == ""
    assert fields["expected_patterns_json"]["type"] == "String"
    assert fields["expected_patterns_json"]["value"] == (
        '["**/image.*.10arc.final_mosaic.fits",'
        '"**/weights.*.10arc.final_mosaic.fits"]'
    )
    assert fields["publisher"]["value"] == "wallaby-hires/beampipe-publish"
    assert fields["completion"]["encoding"] == "path"
    assert fields["inventory"]["encoding"] == "path"
    assert fields["allow_inline_publisher_token"]["value"] is False
    assert ready_fields["filepath"]["value"] == "beampipe-publication-ready"
    assert inventory_fields["filepath"]["value"] == "beampipe-output-inventory.json"
    assert mosaic_fields["publication_ready"]["encoding"] == "path"
    assert ': > "{publication_ready}"' in mosaic_fields["command"]["value"]
    assert mosaic_fields["command"]["value"].endswith(
        ': > "{publication_ready}"'
    )

    mosaic_targets = {
        link["to"] for link in links if link["from"] == mosaic["id"]
    }
    assert {node["id"] for node in image_drops} <= mosaic_targets
    assert ready["id"] in mosaic_targets
    completion_links = [
        link
        for link in links
        if link["to"] == publisher["id"]
        and link["toPort"] == fields["completion"]["id"]
    ]
    assert len(completion_links) == 1
    assert completion_links[0]["from"] == ready["id"]
    ready_producers = [link for link in links if link["to"] == ready["id"]]
    assert len(ready_producers) == 1
    assert ready_producers[0]["from"] == mosaic["id"]
    assert ready_producers[0]["fromPort"] == mosaic_fields["publication_ready"]["id"]
    assert not any(
        link["to"] == publisher["id"]
        and link["toPort"] == fields["source_inventory"]["id"]
        for link in links
    )
    inventory_links = [
        link
        for link in links
        if link["from"] == publisher["id"]
        and link["fromPort"] == fields["inventory"]["id"]
    ]
    assert len(inventory_links) == 1
    assert inventory_links[0]["to"] == inventory["id"]


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
