#!/usr/bin/env python3
"""Run exact-version source purge and controlled faults against an isolated RustFS container.

Requires cached rustfs/rustfs image, docker, nsenter, boto3 and the Rust dependencies.
Uses the container network namespace so host firewall/NAT rules need no changes.
"""

import http.client
import http.server
import copy
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid
import xml.etree.ElementTree as ET

import boto3
from botocore.config import Config


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def isolated_run():
    name = f"dm-nightly-source-purge-{uuid.uuid4().hex[:12]}"
    image = os.environ.get("SOURCE_PURGE_S3_IMAGE", "rustfs/rustfs:latest")
    container = docker(
        "run", "-d", "--name", name, "--network", "none", "--memory=768m", "--cpus=1",
        "--tmpfs", "/data:rw,size=128m,mode=1777", "--label", "codex.task=source-purge-test",
        "-e", "RUSTFS_ADDRESS=0.0.0.0:9000", "-e", "RUSTFS_ACCESS_KEY=purge-test",
        "-e", "RUSTFS_SECRET_KEY=purge-test-isolated-only", image, "/data",
    )
    try:
        print(f"isolated source-purge image={docker('inspect', container, '--format', '{{.Image}}')}", flush=True)
        pid = docker("inspect", container, "--format", "{{.State.Pid}}")
        arguments = [flag for flag in ("--namespace-read", "--source-observation", "--namespace-persistence", "--current-source", "--version-batch", "--version-batch-perf") if flag in sys.argv]
        subprocess.run(["nsenter", "-t", pid, "-n", sys.executable, __file__, "--inside", *arguments], check=True)
    finally:
        docker("rm", "-f", "-v", container)


def inside_run():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for attempt in range(60):
        try:
            with opener.open("http://127.0.0.1:9000/health/ready", timeout=1) as response:
                if response.status == 200:
                    break
        except OSError:
            pass
        time.sleep(0.25)
    else:
        raise RuntimeError("isolated RustFS readiness timed out")
    client = boto3.client(
        "s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="purge-test",
        aws_secret_access_key="purge-test-isolated-only", region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=3),
    )
    bucket = f"source-purge-{uuid.uuid4().hex[:12]}"
    client.create_bucket(Bucket=bucket)
    if "--version-batch" in sys.argv or "--version-batch-perf" in sys.argv:
        from source_version_batch import run
        return run(client, bucket, performance="--version-batch-perf" in sys.argv)
    current_source = "--current-source" in sys.argv
    if not current_source:
        client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    stopped = threading.Event()
    observing = "--source-observation" in sys.argv
    persisting = "--namespace-persistence" in sys.argv
    reading = "--namespace-read" in sys.argv or observing or persisting
    if current_source and reading:
        raise ValueError("--current-source is only for source purge")
    alternating = [0]
    requests = {"data_get": 0, "writes": 0}
    if not reading and not current_source:
        requests["version_deletes"] = 0
    if current_source:
        requests["payload_heads"] = 0
    with tempfile.TemporaryDirectory(prefix="source-purge-") as directory:
        control = Path(directory) / "fault"
        control.write_text("normal")

        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def handle_request(self):
                mode = control.read_text().strip()
                deleting = self.command == "DELETE" and "versionId=" in self.path
                if deleting and "version_deletes" in requests:
                    requests["version_deletes"] += 1
                payload = self.path.partition("?")[0].endswith("/payload")
                tagging = self.command == "GET" and "tagging" in self.path
                probing = reading and payload and (tagging if observing else self.command == "HEAD")
                affected = deleting or probing
                if observing or persisting or current_source:
                    if self.command == "GET" and payload and not tagging:
                        requests["data_get"] += 1
                    if self.command in ("PUT", "POST", "DELETE"):
                        requests["writes"] += 1
                    if current_source and self.command == "HEAD" and payload:
                        requests["payload_heads"] += 1
                if observing and payload and mode == "alternate":
                    if self.command == "HEAD":
                        alternating[0] += 1
                    if tagging or (self.command == "HEAD" and alternating[0] % 2):
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                if affected and mode == "missing_metadata":
                    body = b"<Error><Code>NoSuchKey</Code><Message>injected attribute read missing</Message></Error>"
                    self.send_response(404)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if affected and mode == "readonly":
                    body = b"<Error><Code>AccessDenied</Code><Message>injected read-only source</Message></Error>"
                    self.send_response(403)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if affected and mode in ("disconnect", "stall", "cancel", "flash", "recover_stall"):
                    if mode in ("flash", "recover_stall"):
                        control.write_text("normal")
                    if mode != "disconnect":
                        if mode != "flash":
                            stopped.wait(30)
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                upstream = http.client.HTTPConnection("127.0.0.1", 9000, timeout=5)
                try:
                    body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                    upstream.request(self.command, self.path, body=body, headers=dict(self.headers))
                    response = upstream.getresponse()
                    payload = response.read()
                    rewritten = mode.startswith("contradictory_") and self.command == "GET" and "versions" in self.path and response.status == 200
                    if rewritten:
                        document = ET.fromstring(payload)
                        if document.tag.startswith("{"):
                            ET.register_namespace("", document.tag[1:].partition("}")[0])
                        kind = "DeleteMarker" if mode.startswith("contradictory_marker_") else "Version"
                        latest = next(node for node in document if node.tag.rsplit("}", 1)[-1] == kind and
                                      node.find("{*}IsLatest").text == "true")
                        duplicate = copy.deepcopy(latest)
                        duplicate.find("{*}IsLatest").text = "false"
                        position = list(document).index(latest) + int(mode.endswith("_current_first"))
                        document.insert(position, duplicate)
                        payload = ET.tostring(document, encoding="utf-8")
                    if deleting and mode == "lost_ack":
                        control.write_text("normal")
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in ("connection", "transfer-encoding", "content-length") and not (rewritten and key.lower().startswith("x-amz-checksum")):
                            self.send_header(key, value)
                    self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                finally:
                    upstream.close()

            do_GET = do_HEAD = do_DELETE = do_PUT = do_POST = handle_request

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            modes = ("normal", "readonly", "missing_metadata", "disconnect", "stall", "cancel", "flash", "recover_stall", "alternate") if observing else (("normal", "readonly", "disconnect", "stall", "cancel", "flash", "recover_stall") if reading else ("normal", "readonly", "disconnect", "stall", "cancel", "flash", "lost_ack"))
            if not reading and not current_source:
                modes += ("contradictory_data_current_first", "contradictory_data_history_first",
                          "contradictory_marker_current_first", "contradictory_marker_history_first")
            for mode in modes:
                prefix = f"nightly-local-{mode}"
                for _ in range(1 if current_source else 3):
                    client.put_object(Bucket=bucket, Key=f"{prefix}/payload", Body=b"verified payload")
                if mode.startswith("contradictory_marker_"):
                    client.delete_object(Bucket=bucket, Key=f"{prefix}/payload")
                client.put_object(Bucket=bucket, Key=f"{prefix}/sentinel", Body=b"retain")
                environment = dict(os.environ, S3_CONNECT_TIMEOUT="1", S3_OPERATION_TIMEOUT="2", S3_READ_TIMEOUT="1")
                environment.update(
                    LAB_S3_PURGE_URL=f"s3://purge-test:purge-test-isolated-only@{bucket}.127.0.0.1:{server.server_port}/{prefix}",
                    LAB_S3_PURGE_KEY="payload", LAB_S3_PURGE_FAULT=mode, LAB_S3_PURGE_CONTROL=str(control),
                )
                if persisting:
                    test_name = "namespace_persistence::published_object_persistence_with_real_requests"
                elif observing:
                    test_name = "source_observation::bounded_source_observation_with_real_requests"
                elif reading:
                    test_name = "namespace_read::bounded_namespace_read_with_real_requests"
                else:
                    test_name = "source_purge::permanent_source_purge_with_real_requests"
                command = [
                    "cargo", "test", "--offline", "--locked", "--test", "s3_architecture_contract",
                    test_name, "--", "--ignored", "--exact", "--nocapture",
                ]
                if binary := os.environ.get("LAB_S3_PURGE_TEST_BINARY"):
                    command = [binary, os.environ["LAB_S3_PURGE_TEST_NAME"], "--ignored", "--exact", "--nocapture"]
                before_heads = requests.get("payload_heads", 0)
                before_deletes = requests.get("version_deletes", 0)
                subprocess.run(command, env=environment, check=True)
                if mode.startswith("contradictory_"):
                    assert requests["version_deletes"] == before_deletes, (mode, requests)
                if current_source and mode == "normal":
                    assert requests["payload_heads"] - before_heads == 1, requests
                remaining = client.list_object_versions(Bucket=bucket, Prefix=f"{prefix}/")
                records = remaining.get("Versions", []) + remaining.get("DeleteMarkers", [])
                payload_records = [record for record in records if record["Key"] == f"{prefix}/payload"]
                expected = 0 if not reading and mode in ("normal", "flash", "lost_ack") else 1 if current_source else 4 if mode.startswith("contradictory_marker_") else 3
                assert len(payload_records) == expected, (mode, payload_records)
                assert any(record["Key"] == f"{prefix}/sentinel" for record in records)
                print(f"independent S3 inspection fault={mode} source_records={len(payload_records)} sentinel=retained", flush=True)
            if observing or persisting:
                assert requests == {"data_get": 0, "writes": 0}, requests
                print("target-persistence data_GET=0 writes=0" if persisting else "source-observation data_GET=0 writes=0 missing_metadata_is_error=true shared_budget=true", flush=True)
            if current_source:
                assert requests["data_get"] == 0, requests
                print(f"current-source purge data_GET=0 normal_payload_HEAD=1 requests={requests}", flush=True)
        finally:
            control.write_text("normal")
            stopped.set()
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    if "--inside" in sys.argv:
        inside_run()
    else:
        isolated_run()
