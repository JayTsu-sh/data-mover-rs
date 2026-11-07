#!/usr/bin/env python3
"""Exact historical tag writes and bounded failures in an owned real RustFS namespace."""
import http.client
import http.server
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
MODES = ("normal", "deny", "disconnect", "stall", "cancel", "flash", "lost_ack")


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def isolated():
    container = docker(
        "run", "-d", "--name", f"dm-nightly-version-metadata-{uuid.uuid4().hex[:12]}",
        "--network", "none", "--memory=768m", "--cpus=1", "--tmpfs", "/data:rw,size=128m,mode=1777",
        "--label", "codex.task=version-metadata", "-e", "RUSTFS_ADDRESS=0.0.0.0:9000",
        "-e", "RUSTFS_ACCESS_KEY=version-test", "-e", "RUSTFS_SECRET_KEY=version-isolated-only",
        os.environ.get("VERSION_METADATA_S3_IMAGE", "rustfs/rustfs:latest"), "/data",
    )
    try:
        print(f"version metadata image={docker('inspect', container, '--format', '{{.Image}}')}", flush=True)
        pid = docker("inspect", container, "--format", "{{.State.Pid}}")
        subprocess.run(["nsenter", "-t", pid, "-n", sys.executable, __file__, "--inside"], cwd=ROOT, check=True)
    finally:
        docker("rm", "-f", "-v", container)


def inside():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for _ in range(60):
        try:
            with opener.open("http://127.0.0.1:9000/health/ready", timeout=1) as response:
                if response.status == 200:
                    break
        except OSError:
            pass
        time.sleep(0.25)
    else:
        raise RuntimeError("RustFS readiness timed out")
    client = boto3.client(
        "s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="version-test",
        aws_secret_access_key="version-isolated-only", region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=3),
    )
    bucket = f"version-metadata-{uuid.uuid4().hex[:12]}"
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    seeded = {}
    for mode in MODES:
        key = f"{mode}/payload"
        old = client.put_object(Bucket=bucket, Key=key, Body=b"old", Tagging="raw=old")["VersionId"]
        current = client.put_object(Bucket=bucket, Key=key, Body=b"current", Tagging="raw=current")["VersionId"]
        seeded[mode] = (old, current)
    requests, faults = [], {}
    stopped = threading.Event()
    with tempfile.TemporaryDirectory(prefix="version-metadata-") as directory:
        control = Path(directory) / "fault"
        control.write_text("normal")

        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def handle_request(self):
                mode = control.read_text().strip()
                arguments = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query, keep_blank_values=True)
                tagging = self.command == "PUT" and "tagging" in arguments
                requests.append((self.command, self.path, arguments))
                payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                if tagging and mode in ("deny", "disconnect", "stall", "cancel", "flash"):
                    faults[mode] = faults.get(mode, 0) + 1
                    if mode == "deny":
                        body = b"<Error><Code>AccessDenied</Code><Message>injected version metadata refusal</Message></Error>"
                        self.send_response(403)
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                        return
                    if mode == "flash":
                        control.write_text("normal")
                    if mode in ("stall", "cancel"):
                        stopped.wait(10)
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                upstream = http.client.HTTPConnection("127.0.0.1", 9000, timeout=5)
                try:
                    upstream.request(self.command, self.path, payload, dict(self.headers))
                    response = upstream.getresponse()
                    body = response.read()
                    if tagging and mode == "lost_ack":
                        faults[mode] = faults.get(mode, 0) + 1
                        control.write_text("normal")
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in ("connection", "content-length", "transfer-encoding"):
                            self.send_header(key, value)
                    self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(body)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(body)
                finally:
                    upstream.close()

            do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request

        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        proxy.daemon_threads = True
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        try:
            for mode in MODES:
                old, current = seeded[mode]
                environment = dict(os.environ, S3_CONNECT_TIMEOUT="1", S3_READ_TIMEOUT="1", S3_OPERATION_TIMEOUT="2")
                environment.update({
                    "LAB_VERSION_METADATA_URL": f"s3://version-test:version-isolated-only@{bucket}.127.0.0.1:{proxy.server_port}/{mode}",
                    "LAB_VERSION_METADATA_ID": old, "LAB_VERSION_METADATA_MODE": mode, "LAB_VERSION_METADATA_CONTROL": str(control),
                })
                subprocess.run([
                    "cargo", "test", "--offline", "--locked", "--test", "s3_architecture_contract",
                    "version_metadata::exact_version_metadata_with_real_requests", "--", "--ignored", "--exact", "--nocapture",
                ], cwd=ROOT, env=environment, check=True)
                key = f"{mode}/payload"
                versions = client.list_object_versions(Bucket=bucket, Prefix=key)
                assert len(versions.get("Versions", [])) == 2 and not versions.get("DeleteMarkers"), versions
                for version, data, raw in ((old, b"old", "updated"), (current, b"current", "current")):
                    assert client.get_object(Bucket=bucket, Key=key, VersionId=version)["Body"].read() == data
                    assert client.get_object_tagging(Bucket=bucket, Key=key, VersionId=version)["TagSet"] == [{"Key": "raw", "Value": raw}]
                assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"current"
                selected = [item for item in requests if f"/{mode}/" in item[1]]
                for method, path, arguments in selected:
                    if method == "PUT":
                        assert "tagging" in arguments and arguments.get("versionId") == [old], (method, path)
                    assert method not in ("POST", "DELETE")
                    assert not (method == "GET" and "payload" in path and "tagging" not in arguments), (method, path)
                print(f"independent version metadata fault={mode} versions=2 old_tags=updated current=unchanged data_GET=0 data_PUT=0", flush=True)
            assert all(faults.get(mode, 0) >= 1 for mode in MODES if mode != "normal"), faults
            assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
        finally:
            control.write_text("normal")
            stopped.set()
            proxy.shutdown()
            proxy.server_close()


if __name__ == "__main__":
    inside() if "--inside" in sys.argv else isolated()
