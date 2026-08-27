#!/usr/bin/env python3
"""Run the no-download graph through local DALiuGE REST services.

This starts disposable NM, DIM, TM, and HTTPS fake-Core processes on unique
loopback ports.  It exercises the real TM ``/unroll`` route, DIM session REST
client, native ingest/publisher apps, durable file publication, and Core receipt
shape without contacting CASDA, Setonix, or external storage.
"""

from __future__ import annotations

import argparse
import hashlib
import http.server
import json
import os
import socket
import ssl
import stat
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from pathlib import Path

from dlg.clients import DataIslandManagerClient
from dlg.ddap_protocol import DROPStates

ROOT = Path(__file__).resolve().parents[1]
GRAPH = ROOT / "dlg-graphs/wallaby-hires_test-pipeline-nodownloads-beampipe.graph"
MANIFEST = ROOT / "wallaby_hires/test_staging_e2e_manifest.json"
EXPECTED_PATTERNS = [
    "**/image*.10arc.final_mosaic.fits",
    "**/weights*.10arc.final_mosaic.fits",
]


def _port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_http(url: str, process: subprocess.Popen, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"service exited before becoming ready: {url}")
        try:
            with urllib.request.urlopen(url, timeout=1):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"service did not become ready: {url}")


def _field(node: dict, name: str) -> dict:
    return next(field for field in node["fields"] if field["name"] == name)


def _graph_with_manifest() -> dict:
    graph = json.loads(GRAPH.read_text(encoding="utf-8"))
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    ingest = next(
        node for node in graph["nodeDataArray"] if node["name"] == "beampipe-ingest"
    )
    _field(ingest, "manifest_path")["value"] = json.dumps(
        manifest, sort_keys=True, separators=(",", ":")
    )
    return graph


def _certificate(work: Path) -> tuple[Path, Path]:
    cert = work / "fake-core.crt"
    key = work / "fake-core.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    key.chmod(0o600)
    return cert, key


def _fake_core(
    work: Path, execution_id: str, token: str, cert: Path, key: Path
) -> tuple[http.server.ThreadingHTTPServer, list[bytes]]:
    bodies: list[bytes] = []
    expected_path = f"/api/v2/executions/{execution_id}/outputs/verify"

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            if self.path != expected_path:
                self.send_error(404)
                return
            if self.headers.get("Authorization") != f"Bearer {token}":
                self.send_error(401)
                return
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            document = json.loads(raw)
            if document.get("execution_attempt") != 0:
                self.send_error(400)
                return
            bodies.append(raw)
            (work / "core-receipt-request.json").write_bytes(raw)
            response = {
                "execution": {
                    "uuid": execution_id,
                    "retry_count": 0,
                    "output_state": "verified",
                    "status": "running",
                },
                "artifact": {
                    "uuid": str(uuid.uuid4()),
                    "kind": "output_inventory",
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "uri": document["durable_destination_uri"],
                    "execution_attempt": 0,
                },
            }
            payload = json.dumps(response, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    return server, bodies


def _unroll(tm_port: int, graph: dict) -> list[dict]:
    content = urllib.parse.urlencode(
        {"lg_content": json.dumps(graph, separators=(",", ":"))}
    ).encode()
    request = urllib.request.Request(
        f"http://127.0.0.1:{tm_port}/unroll",
        data=content,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _root_uids(graph: list[dict]) -> list[str]:
    roots = []
    for drop in graph:
        if "oid" not in drop:
            continue
        if not drop.get("producers") and not drop.get("inputs"):
            roots.append(drop["oid"])
    return roots


def _status_value(value: object) -> int:
    if isinstance(value, dict):
        return int(value["status"])
    return int(value)


def _assert_outputs(
    work: Path,
    output_root: Path,
    destination: Path,
    execution_id: str,
    bodies: list[bytes],
) -> dict:
    if len(bodies) != 1:
        raise AssertionError(f"expected one Core receipt, got {len(bodies)}")
    report = json.loads(bodies[0])
    if report["patterns"] != EXPECTED_PATTERNS:
        raise AssertionError(report["patterns"])
    if report["pattern_counts"] != {pattern: 1 for pattern in EXPECTED_PATTERNS}:
        raise AssertionError(report["pattern_counts"])
    products = report["products"]
    if len(products) != 2 or any(product["bytes"] <= 0 for product in products):
        raise AssertionError(products)

    durable_root = destination / "executions" / execution_id / "attempt-0"
    durable_inventory = durable_root / "beampipe-output-inventory.json"
    if durable_inventory.read_bytes() != bodies[0]:
        raise AssertionError("durable inventory differs from the Core receipt")
    for product in products:
        source = output_root / product["path"]
        published = durable_root / product["path"]
        if source.read_bytes() != published.read_bytes():
            raise AssertionError(f"published bytes differ: {product['path']}")
        if hashlib.sha256(published.read_bytes()).hexdigest() != product["sha256"]:
            raise AssertionError(f"published checksum differs: {product['path']}")

    inventories = sorted(work.rglob("beampipe-output-inventory.json"))
    if not any(
        path != durable_inventory and path.read_bytes() == bodies[0]
        for path in inventories
    ):
        raise AssertionError("DALiuGE inventory FileDROP was not emitted")
    return {
        "core_receipts": len(bodies),
        "inventory_sha256": hashlib.sha256(bodies[0]).hexdigest(),
        "patterns": report["pattern_counts"],
        "products": [product["path"] for product in products],
        "durable_uri": report["durable_destination_uri"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--keep",
        action="store_true",
        help="retain the disposable workspace and print its path",
    )
    args = parser.parse_args()
    temporary = None
    if args.keep:
        work = Path(tempfile.mkdtemp(prefix="beampipe-nodownload-rest-"))
    else:
        temporary = tempfile.TemporaryDirectory(prefix="beampipe-nodownload-rest-")
        work = Path(temporary.name)

    logs = work / "logs"
    logs.mkdir()
    output_root = work / "outputs"
    output_root.mkdir()
    destination = work / "durable"
    destination.mkdir()
    dlg_root = work / "dlg"
    (work / "logical").mkdir()
    (work / "physical").mkdir()

    token = f"local-smoke-{uuid.uuid4().hex}"
    token_path = work / "publisher.token"
    token_path.write_text(token, encoding="utf-8")
    token_path.chmod(0o600)
    execution_id = str(uuid.uuid4())
    cert, key = _certificate(work)
    core, receipt_bodies = _fake_core(work, execution_id, token, cert, key)
    core_thread = threading.Thread(target=core.serve_forever, daemon=True)
    core_thread.start()

    nm_port, event_port, rpc_port, dim_port, tm_port = [_port() for _ in range(5)]
    env = os.environ.copy()
    env.update(
        {
            "DLG_ROOT": str(dlg_root),
            "PYTHONDONTWRITEBYTECODE": "1",
            "BEAMPIPE_OUTPUT_ROOT": str(output_root),
            "BEAMPIPE_OUTPUT_DESTINATION_URI": destination.as_uri(),
            "BEAMPIPE_CORE_URL": f"https://localhost:{core.server_port}/api/v2",
            "BEAMPIPE_EXECUTION_ID": execution_id,
            "BEAMPIPE_EXECUTION_ATTEMPT": "0",
            "BEAMPIPE_PUBLISHER_TOKEN_FILE": str(token_path),
            "SSL_CERT_FILE": str(cert),
            "NO_PROXY": "localhost,127.0.0.1",
            "no_proxy": "localhost,127.0.0.1",
        }
    )
    dlg = str(Path(os.environ.get("VIRTUAL_ENV", "")) / "bin" / "dlg")
    if not Path(dlg).is_file():
        dlg = str(Path(os.sys.executable).with_name("dlg"))

    processes: list[tuple[subprocess.Popen, object]] = []
    client = None
    session_id = f"beampipe-nodownload-{uuid.uuid4()}"

    def start(name: str, command: list[str]) -> subprocess.Popen:
        stream = (logs / f"{name}.log").open("wb")
        process = subprocess.Popen(
            command, env=env, stdout=stream, stderr=subprocess.STDOUT
        )
        processes.append((process, stream))
        return process

    try:
        nm = start(
            "nm",
            [
                dlg,
                "nm",
                "-H",
                "127.0.0.1",
                "-P",
                str(nm_port),
                "--event_port",
                str(event_port),
                "--rpc_port",
                str(rpc_port),
                "-t",
                "8",
                "-l",
                str(logs),
            ],
        )
        _wait_http(f"http://127.0.0.1:{nm_port}/api", nm)
        dim = start(
            "dim",
            [
                dlg,
                "dim",
                "-H",
                "127.0.0.1",
                "-P",
                str(dim_port),
                "-N",
                f"127.0.0.1:{nm_port}:{event_port}:{rpc_port}",
                "--dmCheckTimeout",
                "2",
                "-l",
                str(logs),
            ],
        )
        _wait_http(f"http://127.0.0.1:{dim_port}/api", dim)
        tm = start(
            "tm",
            [
                dlg,
                "tm",
                "-H",
                "127.0.0.1",
                "-p",
                str(tm_port),
                "-d",
                str(work / "logical"),
                "-t",
                str(work / "physical"),
                "-l",
                str(logs),
            ],
        )
        # DALiuGE 6.6's HTML index is incompatible with newer Starlette
        # TemplateResponse signatures; the API schema is a stable readiness
        # endpoint and the /unroll REST route is unaffected.
        _wait_http(f"http://127.0.0.1:{tm_port}/openapi.json", tm)

        pgt = _unroll(tm_port, _graph_with_manifest())
        roots = _root_uids(pgt)
        node = f"127.0.0.1:{nm_port}:{event_port}:{rpc_port}"
        island = f"127.0.0.1:{dim_port}"
        for drop in pgt:
            if "oid" in drop:
                drop["node"] = node
                drop["island"] = island

        client = DataIslandManagerClient("127.0.0.1", dim_port, timeout=30)
        client.create_session(session_id)
        client.append_graph(session_id, pgt)
        client.deploy_session(session_id, completed_uids=roots)

        deadline = time.monotonic() + 120
        while True:
            status = client.graph_status(session_id)
            values = [_status_value(value) for value in status.values()]
            if any(
                value in {DROPStates.ERROR, DROPStates.CANCELLED, DROPStates.SKIPPED}
                for value in values
            ):
                raise RuntimeError(f"DALiuGE graph failed: {Counter(values)}")
            if values and all(value == DROPStates.COMPLETED for value in values):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"DALiuGE graph did not finish: {Counter(values)}")
            time.sleep(0.25)

        summary = _assert_outputs(
            work, output_root, destination, execution_id, receipt_bodies
        )
        if stat.S_IMODE(token_path.stat().st_mode) != 0o600:
            raise AssertionError("publisher token file mode changed")
        combined_logs = b"".join(path.read_bytes() for path in logs.glob("*.log"))
        if token.encode() in combined_logs:
            raise AssertionError("publisher token leaked into DALiuGE logs")
        summary.update(
            {
                "drops": len(status),
                "roots_triggered": len(roots),
                "session_id": session_id,
                "workspace": str(work),
            }
        )
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    finally:
        if client is not None:
            try:
                client.destroy_session(session_id)
            except Exception:
                pass
        core.shutdown()
        core.server_close()
        for process, stream in reversed(processes):
            if process.poll() is None:
                process.terminate()
        for process, stream in reversed(processes):
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            stream.close()
        if args.keep:
            print(f"retained workspace: {work}")
        elif temporary is not None:
            temporary.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
