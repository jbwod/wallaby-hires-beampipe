import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCKER_GRAPH = ROOT / "dlg-graphs/wallaby-hires_deploy-pipeline-beampipe.graph"
SETONIX_GRAPH = ROOT / "dlg-graphs/wallaby-hires_deploy-setonix-beampipe.graph"
ASKAPSOFT_DIGEST = (
    "csirocass/askapsoft@"
    "sha256:2b0cf3bac871664095cdfc9b13a6f438163d00dc344c3c1db8fcde4eef1aed65"
)


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
