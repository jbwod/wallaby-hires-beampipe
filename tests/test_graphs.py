import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCKER_GRAPH = ROOT / "dlg-graphs/wallaby-hires_deploy-pipeline-beampipe.graph"
SETONIX_GRAPH = ROOT / "dlg-graphs/wallaby-hires_deploy-setonix-beampipe.graph"
TEST_GRAPH = ROOT / "dlg-graphs/wallaby-hires_test-pipeline-beampipe.graph"
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


def test_docker_askapsoft_images_are_immutable():
    values = _field_values(DOCKER_GRAPH)

    assert values.count(ASKAPSOFT_DIGEST) == 4
    assert not any(
        isinstance(value, str) and "askapsoft:develop" in value for value in values
    )


def test_setonix_graph_uses_deployment_managed_account_and_image():
    values = _field_values(SETONIX_GRAPH)
    commands = "\n".join(value for value in values if isinstance(value, str))

    assert "BEAMPIPE_SLURM_ACCOUNT" in commands
    assert commands.count("BEAMPIPE_ASKAPSOFT_SIF") >= 4
    assert "pawsey0411" not in commands
    assert "jblackwood" not in commands


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
