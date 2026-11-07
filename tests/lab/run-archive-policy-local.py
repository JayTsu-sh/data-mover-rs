#!/usr/bin/env python3
"""Verify Direct tar publication and controlled write faults in an owned RustFS namespace."""
import http.client
import http.server
import io
import os
from pathlib import Path
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
import uuid

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def isolated():
    container = docker(
        "run", "-d", "--name", f"dm-nightly-archive-{uuid.uuid4().hex[:12]}",
        "--network", "none", "--memory=768m", "--cpus=1",
        "--tmpfs", "/data:rw,size=128m,mode=1777", "--label", "codex.task=archive-policy",
        "-e", "RUSTFS_ADDRESS=0.0.0.0:9000", "-e", "RUSTFS_ACCESS_KEY=archive-test",
        "-e", "RUSTFS_SECRET_KEY=archive-isolated-only",
        os.environ.get("ARCHIVE_S3_IMAGE", "rustfs/rustfs:latest"), "/data",
    )
    try:
        print(f"archive image={docker('inspect', container, '--format', '{{.Image}}')}", flush=True)
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
        "s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="archive-test",
        aws_secret_access_key="archive-isolated-only", region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=3),
    )
    bucket = f"archive-policy-{uuid.uuid4().hex[:12]}"
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    stopped = threading.Event()
    faults = {}
    writes = []
    with tempfile.TemporaryDirectory(prefix="archive-policy-") as directory:
        control = Path(directory) / "fault"
        control.write_text("normal")

        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def handle_request(self):
                mode = control.read_text().strip()
                writing = self.command in ("PUT", "POST")
                if writing:
                    writes.append(self.path)
                if writing and mode != "normal":
                    faults[mode] = faults.get(mode, 0) + 1
                    if mode == "deny":
                        payload = b"<Error><Code>AccessDenied</Code><Message>injected archive write refusal</Message></Error>"
                        self.send_response(403)
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                        return
                    if mode == "disconnect":
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                    while not stopped.wait(0.025) and control.read_text().strip() == mode:
                        pass
                    return
                payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                upstream = http.client.HTTPConnection("127.0.0.1", 9000, timeout=5)
                try:
                    upstream.request(self.command, self.path, payload, dict(self.headers))
                    response = upstream.getresponse()
                    payload = response.read()
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in ("connection", "content-length", "transfer-encoding"):
                            self.send_header(key, value)
                    self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                finally:
                    upstream.close()

            do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request

        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        proxy.daemon_threads = True
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        try:
            environment = dict(os.environ)
            environment.update({
                "LAB_ARCHIVE_URL": f"s3://archive-test:archive-isolated-only@{bucket}.127.0.0.1:{proxy.server_port}",
                "LAB_ARCHIVE_CONTROL": str(control),
                "S3_CONNECT_TIMEOUT": "1", "S3_READ_TIMEOUT": "1", "S3_OPERATION_TIMEOUT": "2",
            })
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "--test", "role_tar",
                "policy::direct_s3_archive_uses_final_key_and_keeps_source_on_write_refusal",
                "--", "--ignored", "--exact", "--nocapture",
            ], cwd=ROOT, env=environment, check=True)
            records = client.list_object_versions(Bucket=bucket)
            versions = records.get("Versions", [])
            assert {item["Key"] for item in versions} == {
                "single/result.tar", "multipart/result.tar",
                "recovered-disconnect/result.tar", "recovered-stall/result.tar",
            }, records
            assert len(versions) == 4 and not records.get("DeleteMarkers"), records
            for item in versions:
                payload = client.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
                with tarfile.open(fileobj=io.BytesIO(payload)) as archive:
                    assert archive.getnames() == ["file"]
                    body = archive.extractfile("file").read()
                expected = b"Z" * (6 * 1024 * 1024 if item["Key"].startswith("multipart/") else 1024)
                if item["Key"].startswith("recovered-"):
                    expected = b"retained source"
                assert body == expected, item
            assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
            assert set(faults) == {"deny", "disconnect", "stall"}, faults
            assert all(".data-mover-" not in path for path in writes), writes
            print(f"independent S3 archive inspection versions=4 ordered_contents=verified staging_keys=0 uploads=0 source_overlap=refused faults={faults}", flush=True)
        finally:
            stopped.set()
            proxy.shutdown()
            proxy.server_close()


if __name__ == "__main__":
    inside() if "--inside" in sys.argv else isolated()
