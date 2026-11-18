#!/usr/bin/env python3
"""Real S3 checkpoint version provenance; only owned containers, buckets and prefixes."""
import http.client
import http.server
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
MODES = ("ack_saved", "lost_complete_ack", "pointer_refused", "lost_complete_ack_replaced", "ack_saved_replaced")
BODY = b"\x5a" * (9 * 1024 * 1024)
TEST = "checkpoint_version::created_version_evidence_survives_only_when_saved_in_the_checkpoint"


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def isolated():
    for mode in MODES:
        container = docker("run", "-d", "--pull", "never", "--name", "dm-checkpoint-version-" + uuid.uuid4().hex[:12],
                           "--network", "none", "--memory=1536m", "--cpus=2", "--tmpfs", "/data:rw,size=768m,mode=1777",
                           "--label", "codex.task=checkpoint-version", "-e", "RUSTFS_ADDRESS=0.0.0.0:9000",
                           "-e", "RUSTFS_ACCESS_KEY=checkpoint-test", "-e", "RUSTFS_SECRET_KEY=checkpoint-isolated-only",
                           os.environ.get("SOURCE_PURGE_S3_IMAGE", "rustfs/rustfs:latest"), "/data")
        try:
            print(f"owned checkpoint mode={mode} image=" + docker("inspect", container, "--format", "{{.Image}}"), flush=True)
            pid = docker("inspect", container, "--format", "{{.State.Pid}}")
            subprocess.run(["nsenter", "-t", pid, "-n", sys.executable, __file__, "--inside"], cwd=ROOT,
                           env=dict(os.environ, LAB_CHECKPOINT_VERSION_MODE=mode), check=True, timeout=180)
        finally:
            docker("rm", "-f", "-v", container)
    print("independent checkpoint scenarios=5 all_owned_containers_cleaned=true", flush=True)


def inside():
    mode = os.environ["LAB_CHECKPOINT_VERSION_MODE"]
    client = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="checkpoint-test",
                          aws_secret_access_key="checkpoint-isolated-only", region_name="us-east-1",
                          config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=5))
    for _ in range(60):
        try:
            client.list_buckets()
            break
        except Exception:
            time.sleep(.25)
    else:
        raise RuntimeError("owned checkpoint service readiness timeout")
    bucket = "checkpoint-version-" + uuid.uuid4().hex[:12]
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    client.put_object(Bucket=bucket, Key="sentinel", Body=b"retain sentinel")
    key = mode + "/payload"
    guard = threading.Lock()
    state = dict(armed=False, completed=0, complete_requests=0, uploads=0, parts=0, payload_puts=0, pointer_denials=0, replaced=False)

    class Relay(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            if self.path in ("/arm", "/recover", "/replace"):
                if self.path == "/replace":
                    client.put_object(Bucket=bucket, Key=key, Body=BODY)
                    state["replaced"] = True
                else:
                    with guard:
                        state["armed"] = self.path == "/arm"
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.relay()

        def read_body(self):
            if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
                return self.rfile.read(int(self.headers.get("Content-Length", "0")))
            chunks = []
            while True:
                count = int(self.rfile.readline().split(b";", 1)[0], 16)
                if not count:
                    while self.rfile.readline() not in (b"\r\n", b""):
                        pass
                    return b"".join(chunks)
                chunks.append(self.rfile.read(count))
                assert self.rfile.read(2) == b"\r\n"

        def relay(self):
            parsed = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            data = urllib.parse.unquote(parsed.path) == f"/{bucket}/{key}"
            completing = data and self.command == "POST" and "uploadId" in query
            body = self.read_body()
            with guard:
                if data and self.command == "POST" and "uploads" in query:
                    state["uploads"] += 1
                if data and self.command == "PUT":
                    state["parts" if "uploadId" in query else "payload_puts"] += 1
                if completing:
                    state["complete_requests"] += 1
                deny_pointer = mode == "pointer_refused" and state["armed"] and state["completed"] and self.command == "PUT" and not data
                if deny_pointer:
                    state["pointer_denials"] += 1
            if deny_pointer:
                payload = b"<Error><Code>AccessDenied</Code><Message>owned published checkpoint write refused</Message></Error>"
                self.send_response(403)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            upstream = http.client.HTTPConnection("127.0.0.1", 9000, timeout=10)
            try:
                headers = {name: value for name, value in self.headers.items()
                           if name.lower() not in ("connection", "content-length", "transfer-encoding")}
                headers["Content-Length"] = str(len(body))
                upstream.request(self.command, self.path, body, headers)
                response = upstream.getresponse()
                payload = response.read()
                if completing and response.status == 200:
                    with guard:
                        state["completed"] += 1
                    if mode.startswith("lost_complete_ack") and state["armed"]:
                        state["armed"] = False
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                self.send_response(response.status)
                for name, value in response.getheaders():
                    if name.lower() not in ("connection", "content-length", "transfer-encoding"):
                        self.send_header(name, value)
                self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                upstream.close()

        do_HEAD = do_PUT = do_POST = do_DELETE = relay

    relay = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Relay)
    relay.daemon_threads = True
    threading.Thread(target=relay.serve_forever, daemon=True).start()
    environment = dict(os.environ, S3_CONNECT_TIMEOUT="1", S3_READ_TIMEOUT="1", S3_OPERATION_TIMEOUT="2",
                       LAB_CHECKPOINT_VERSION_CONTROL=f"127.0.0.1:{relay.server_port}",
                       LAB_CHECKPOINT_VERSION_URL=f"s3://checkpoint-test:checkpoint-isolated-only@{bucket}.127.0.0.1:{relay.server_port}/{mode}")
    try:
        tested = subprocess.run(["cargo", "test", "--offline", "--locked", "--test", "s3_architecture_contract", TEST,
                                 "--", "--ignored", "--exact", "--nocapture"], cwd=ROOT, env=environment,
                                capture_output=True, text=True, timeout=90)
        print(tested.stdout, end="", flush=True)
        print(tested.stderr, end="", file=sys.stderr, flush=True)
        assert tested.returncode == 0, (mode, tested.returncode)
        assert state["uploads"] == 1 and state["parts"] == 2 and state["complete_requests"] == 1 and state["completed"] == 1 and state["payload_puts"] == 0, state
        if mode.startswith("lost_complete_ack"):
            assert "reconciled without a created-version receipt" in tested.stdout and "Connectivity" in tested.stdout, tested.stdout
        assert state["pointer_denials"] == (1 if mode == "pointer_refused" else 0), state
        versions = client.list_object_versions(Bucket=bucket)
        assert not versions.get("DeleteMarkers"), versions
        records = versions.get("Versions", [])
        data = [record for record in records if record["Key"] == key]
        assert len(data) == (2 if state["replaced"] else 1), data
        for record in data:
            assert client.get_object(Bucket=bucket, Key=key, VersionId=record["VersionId"])["Body"].read() == BODY
        roots = [record for record in records if record["Key"] not in (key, "sentinel")]
        assert bool(roots) == state["replaced"], (mode, roots)
        assert client.get_object(Bucket=bucket, Key="sentinel")["Body"].read() == b"retain sentinel"
        assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
        print(f"independent checkpoint mode={mode} target_versions={len(data)} retained_roots={len(roots)} no_duplicate_writes=true payloads_verified=true sentinel_preserved=true", flush=True)
    finally:
        relay.shutdown()
        relay.server_close()


if __name__ == "__main__":
    inside() if "--inside" in sys.argv else isolated()
