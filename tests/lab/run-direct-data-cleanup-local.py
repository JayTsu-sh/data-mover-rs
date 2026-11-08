#!/usr/bin/env python3
"""Verify owned Direct output cleanup in an isolated RustFS namespace; never touches shared storage."""
import http.client
import http.server
import json
import os
from pathlib import Path
import re
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
TEST = "direct_cleanup::confirmed_direct_data_cleanup_with_real_requests"
OLD = b"valid previous object"


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def isolated():
    for group in ("0-small", "0-large", "0-slash", "1-small", "1-large", "1-slash"):
        container = docker("run", "-d", "--pull", "never", "--name", "dm-direct-cleanup-" + uuid.uuid4().hex[:12],
                           "--network", "none", "--memory=1536m", "--cpus=2", "--tmpfs", "/data:rw,size=1024m,mode=1777",
                           "--label", "codex.task=direct-data-cleanup", "-e", "RUSTFS_ADDRESS=0.0.0.0:9000",
                           "-e", "RUSTFS_ACCESS_KEY=direct-test", "-e", "RUSTFS_SECRET_KEY=direct-test-isolated-only",
                           os.environ.get("SOURCE_PURGE_S3_IMAGE", "rustfs/rustfs:latest"), "/data")
        try:
            print(f"isolated Direct cleanup group={group} image=" + docker("inspect", container, "--format", "{{.Image}}"), flush=True)
            pid = docker("inspect", container, "--format", "{{.State.Pid}}")
            subprocess.run(["nsenter", "-t", pid, "-n", sys.executable, __file__, "--inside"], cwd=ROOT,
                           env=dict(os.environ, LAB_DIRECT_CLEANUP_GROUP=group), check=True, timeout=180)
        except BaseException:
            print(docker("logs", "--tail=30", container), file=sys.stderr)
            raise
        finally:
            docker("rm", "-f", "-v", container)
    print("independent Direct cleanup total_scenarios=60 groups=6 all_owned_containers_cleaned=true", flush=True)


def records(client, bucket, prefix):
    result = client.list_object_versions(Bucket=bucket, Prefix=prefix)
    return result.get("Versions", []) + result.get("DeleteMarkers", [])


def inside():
    client = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="direct-test",
                          aws_secret_access_key="direct-test-isolated-only", region_name="us-east-1",
                          config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=3))
    for _ in range(60):
        try:
            client.list_buckets()
            break
        except Exception:
            time.sleep(0.25)
    else:
        raise RuntimeError("owned RustFS readiness timeout")
    binary = os.environ.get("LAB_S3_DIRECT_TEST_BINARY")
    if binary is None:
        built = subprocess.run(["cargo", "test", "--offline", "--locked", "--test", "s3_architecture_contract", "--no-run", "--message-format=json"],
                               cwd=ROOT, check=True, capture_output=True, text=True)
        print(built.stderr, flush=True)
        artifacts = [json.loads(line) for line in built.stdout.splitlines() if line.startswith("{")]
        binary = next(item["executable"] for item in artifacts if item.get("reason") == "compiler-artifact"
                      and item.get("target", {}).get("name") == "s3_architecture_contract" and item.get("executable"))
    listed = subprocess.check_output([binary, "--list"], text=True)
    assert TEST + ": test" in listed, "exact ignored contract must exist"
    active = [None]
    guard = threading.Lock()
    all_states = []

    class Relay(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            if self.path == "/read-seen":
                self.send_response(204 if active[0]["read_seen"].is_set() else 202)
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

        def refused(self, message):
            body = f"<Error><Code>AccessDenied</Code><Message>{message}</Message></Error>".encode()
            self.send_response(403)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def disconnected(self):
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        def relay(self):
            state = active[0]
            mode = state["mode"]
            parsed = urllib.parse.urlsplit(self.path)
            path = urllib.parse.unquote(parsed.path)
            query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            target = path == "/" + state["bucket"] + "/" + state["target"]
            reading = target and self.command == "GET" and "tagging" not in query
            deleting = target and self.command == "DELETE" and "uploadId" not in query
            body = self.read_body()
            if target:
                with guard:
                    state["requests"].append((self.command, sorted(query)))
            if mode == "data403" and target and ((self.command == "PUT" and "tagging" not in query and "uploadId" not in query)
                                                   or (self.command == "POST" and "uploadId" in query)):
                self.refused("owned data write refused")
                return
            if mode == "metadata" and target and self.command == "PUT" and "tagging" in query:
                self.refused("owned tags write refused")
                return
            if reading:
                state["read_seen"].set()
                if mode == "cancel":
                    state["release"].wait(20)
                    self.disconnected()
                    return
                if mode == "read403":
                    self.refused("owned data read-back refused")
                    return
            if deleting:
                with guard:
                    state["delete_requests"] += 1
                    state["delete_started"] = state["delete_started"] or time.monotonic()
                assert query.get("versionId"), "cleanup must delete the exact version/null slot, never create a marker"
                if mode == "delete403":
                    self.refused("owned cleanup deletion refused")
                    return
                if mode in ("disconnect", "stall"):
                    if mode == "stall":
                        state["release"].wait(20)
                    self.disconnected()
                    return
            connection = http.client.HTTPConnection("127.0.0.1", 9000, timeout=5)
            try:
                headers = {key: value for key, value in self.headers.items()
                           if key.lower() not in ("content-length", "transfer-encoding", "connection")}
                headers["Content-Length"] = str(len(body))
                connection.request(self.command, self.path, body=body, headers=headers)
                response = connection.getresponse()
                payload = response.read()
                if deleting:
                    assert response.status == 204, response.status
                    state["confirmed_deletes"] += 1
                    if mode == "lostack":
                        self.disconnected()
                        return
                if reading and mode in ("verify", "delete403", "disconnect", "stall", "lostack"):
                    assert payload, "read-back fault must affect actual data"
                    payload = bytes([payload[0] ^ 1]) + payload[1:]
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ("content-length", "transfer-encoding", "connection") and not key.lower().startswith("x-amz-checksum"):
                        self.send_header(key, value)
                self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                connection.close()

        do_HEAD = do_PUT = do_POST = do_DELETE = relay

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Relay)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        group = os.environ["LAB_DIRECT_CLEANUP_GROUP"]
        for versioned in (group.startswith("1-"),):
            bucket = "direct-cleanup-" + uuid.uuid4().hex[:16]
            client.create_bucket(Bucket=bucket)
            if versioned:
                client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            client.put_object(Bucket=bucket, Key="sentinel", Body=b"unrelated sentinel")
            shapes = {"small": [(1024, "payload")], "large": [(12 * 1024 * 1024, "payload")], "slash": [(12 * 1024 * 1024, "payload/")]}[group.split("-", 1)[1]]
            for size, key in shapes:
                body = (b"owned-data-" * (size // 11 + 1))[:size]
                for mode in ("normal", "verify", "read403", "data403", "metadata", "delete403", "disconnect", "stall", "lostack", "cancel"):
                    prefix = "nightly-direct-" + uuid.uuid4().hex[:16]
                    source, target = prefix + "/source/" + key, prefix + "/target/" + key
                    for _ in range(3):
                        client.put_object(Bucket=bucket, Key=source, Body=body, Tagging="proof=raw")
                    client.put_object(Bucket=bucket, Key=target, Body=OLD, Tagging="proof=old")
                    before_source = records(client, bucket, source)
                    before_target = records(client, bucket, target)
                    assert len(before_target) == 1
                    state = dict(mode=mode, bucket=bucket, target=target, delete_requests=0, confirmed_deletes=0,
                                 delete_started=None, read_seen=threading.Event(), release=threading.Event(), requests=[])
                    active[0] = state
                    all_states.append(state)
                    address = f"127.0.0.1:{server.server_port}"
                    base = f"s3://direct-test:direct-test-isolated-only@{bucket}.127.0.0.1:{server.server_port}/{prefix}"
                    environment = dict(os.environ, S3_CONNECT_TIMEOUT="1", S3_READ_TIMEOUT="1", S3_OPERATION_TIMEOUT="2",
                                       LAB_DIRECT_CLEANUP_SOURCE=base + "/source", LAB_DIRECT_CLEANUP_TARGET=base + "/target",
                                       LAB_DIRECT_CLEANUP_MODE=mode, LAB_DIRECT_CLEANUP_KEY=key, LAB_DIRECT_CLEANUP_CONTROL=address)
                    started = time.monotonic()
                    try:
                        subprocess.run([binary, TEST, "--ignored", "--exact", "--nocapture"], env=environment, check=True, timeout=50)
                    finally:
                        state["release"].set()
                    after_source = records(client, bucket, source)
                    assert {item["VersionId"] for item in after_source} == {item["VersionId"] for item in before_source}
                    for item in after_source:
                        assert client.get_object(Bucket=bucket, Key=source, VersionId=item["VersionId"])["Body"].read() == body
                    remaining = records(client, bucket, target)
                    assert all("ETag" in item for item in remaining), "cleanup must not create delete markers"
                    removed = mode in ("verify", "read403", "lostack", "cancel")
                    kept_old = before_target[0]["VersionId"] not in (None, "null")
                    expected = OLD if mode == "data403" or (removed and kept_old) else (None if removed else body)
                    assert len(remaining) == (1 if expected is not None else 0) + (1 if kept_old and not removed and mode != "data403" else 0), (mode, versioned, key, remaining)
                    if expected is not None:
                        assert client.get_object(Bucket=bucket, Key=target)["Body"].read() == expected
                    if kept_old:
                        assert client.get_object(Bucket=bucket, Key=target, VersionId=before_target[0]["VersionId"])["Body"].read() == OLD
                    assert state["delete_requests"] == (0 if mode in ("normal", "metadata", "data403") else 1), state
                    assert state["confirmed_deletes"] == int(removed), state
                    assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
                    assert client.get_object(Bucket=bucket, Key="sentinel")["Body"].read() == b"unrelated sentinel"
                    print(f"independent Direct cleanup mode={mode} versioned={versioned} size={size} key={key} source_versions={len(before_source)} target_records={len(remaining)} delete_requests={state['delete_requests']} elapsed_ms={round((time.monotonic()-started)*1000)}", flush=True)
                    for item in records(client, bucket, prefix + "/"):
                        client.delete_object(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])
        assert len(all_states) == 10
        print(f"independent Direct cleanup group={group} scenarios=10 source_and_old_versions_preserved=true exact_owned_deletes=true metadata_and_unknown_results_retained=true", flush=True)
    finally:
        for state in all_states:
            state["release"].set()
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    inside() if "--inside" in sys.argv else isolated()
