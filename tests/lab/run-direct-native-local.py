#!/usr/bin/env python3
"""Direct CopyObject against an owned real RustFS service and controlled HTTP faults."""
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
import urllib.request
import uuid

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
MODES = ("quick", "full", "tags_quick", "tags_full", "deny", "disconnect", "stall", "cancel", "flash", "lost_ack")


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def isolated():
    container = docker(
        "run", "-d", "--name", f"dm-nightly-direct-native-{uuid.uuid4().hex[:12]}",
        "--network", "none", "--memory=768m", "--cpus=1", "--tmpfs", "/data:rw,size=128m,mode=1777",
        "--label", "codex.task=direct-native", "-e", "RUSTFS_ADDRESS=0.0.0.0:9000",
        "-e", "RUSTFS_ACCESS_KEY=native-test", "-e", "RUSTFS_SECRET_KEY=native-isolated-only",
        os.environ.get("DIRECT_NATIVE_S3_IMAGE", "rustfs/rustfs:latest"), "/data",
    )
    try:
        print(f"Direct native image={docker('inspect', container, '--format', '{{.Image}}')}", flush=True)
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
        "s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="native-test",
        aws_secret_access_key="native-isolated-only", region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=3),
    )
    bucket = f"direct-native-{uuid.uuid4().hex[:12]}"
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    body = b"Z" * 1024
    for mode in MODES:
        client.put_object(Bucket=bucket, Key=f"{mode}/src/payload", Body=body, Tagging="generation=source")
    stopped = threading.Event()
    requests, faults = [], {}
    with tempfile.TemporaryDirectory(prefix="direct-native-") as directory:
        control = Path(directory) / "fault"
        control.write_text("normal")

        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def handle_request(self):
                mode = control.read_text().strip()
                copying = self.command == "PUT" and self.headers.get("x-amz-copy-source") is not None
                tagging = self.command == "PUT" and "tagging" in self.path
                requests.append((self.command, self.path, copying, tagging))
                affected = copying and mode in ("deny", "disconnect", "stall", "cancel", "flash") or tagging and mode.startswith("tags_")
                if affected:
                    faults[mode] = faults.get(mode, 0) + 1
                    if mode == "deny" or tagging:
                        payload = b"<Error><Code>AccessDenied</Code><Message>injected native refusal</Message></Error>"
                        self.send_response(403)
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                        return
                    if mode == "flash":
                        control.write_text("normal")
                    if mode in ("stall", "cancel"):
                        stopped.wait(10)
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                upstream = http.client.HTTPConnection("127.0.0.1", 9000, timeout=5)
                try:
                    upstream.request(self.command, self.path, payload, dict(self.headers))
                    response = upstream.getresponse()
                    payload = response.read()
                    if copying and mode == "lost_ack":
                        faults[mode] = faults.get(mode, 0) + 1
                        control.write_text("normal")
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in ("connection", "content-length", "transfer-encoding"):
                            self.send_header(key, value)
                    self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(payload)
                finally:
                    upstream.close()

            do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request

        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        proxy.daemon_threads = True
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        try:
            for mode in MODES:
                environment = dict(os.environ, S3_CONNECT_TIMEOUT="1", S3_READ_TIMEOUT="1", S3_OPERATION_TIMEOUT="2")
                environment.update({
                    "LAB_DIRECT_NATIVE_URL": f"s3://native-test:native-isolated-only@{bucket}.127.0.0.1:{proxy.server_port}",
                    "LAB_DIRECT_NATIVE_MODE": mode, "LAB_DIRECT_NATIVE_CONTROL": str(control),
                })
                subprocess.run([
                    "cargo", "test", "--offline", "--locked", "--test", "s3_architecture_contract",
                    "direct_native::direct_native_with_real_requests", "--", "--ignored", "--exact", "--nocapture",
                ], cwd=ROOT, env=environment, check=True)
                records = client.list_object_versions(Bucket=bucket, Prefix=f"{mode}/")
                assert not records.get("DeleteMarkers"), records
                versions = records.get("Versions", [])
                targets = [item for item in versions if item["Key"] == f"{mode}/target/payload"]
                assert len(targets) == (0 if mode == "cancel" else 1), (mode, targets)
                assert len(versions) == 1 + len(targets), (mode, versions)
                for item in versions:
                    assert client.get_object(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])["Body"].read() == body
                    tags = client.get_object_tagging(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])["TagSet"]
                    assert tags == ([] if mode.startswith("tags_") and "/target/" in item["Key"] else [{"Key": "generation", "Value": "source"}]), (mode, item, tags)
                selected = [item for item in requests if f"/{mode}/" in item[1]]
                assert not any(method in ("POST", "DELETE") or (method == "PUT" and not copying and not tagging) for method, _, copying, tagging in selected), (mode, selected)
                payload_reads = sum(method == "GET" and "payload" in path and "tagging" not in path for method, path, _, _ in selected)
                assert payload_reads == (2 if mode.endswith("full") else 0), (mode, payload_reads)
                print(f"independent Direct native fault={mode} source=retained target_versions={len(targets)} payload_GET={payload_reads}", flush=True)
            assert all(faults.get(mode, 0) >= 1 for mode in MODES if mode not in ("quick", "full")), faults
            assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
        finally:
            control.write_text("normal")
            stopped.set()
            proxy.shutdown()
            proxy.server_close()


if __name__ == "__main__":
    inside() if "--inside" in sys.argv else isolated()
