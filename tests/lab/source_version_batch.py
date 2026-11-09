"""Exact-version DeleteObjects SDK contract in the source-purge owned namespace."""
import copy
import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import urllib.parse
import xml.etree.ElementTree as ET


def run(client, bucket, *, performance=False):
    modes = ("normal", "partial_denial", "missing_id", "missing_ack", "unknown_id",
             "duplicate_ack", "readonly", "disconnect", "stall", "cancel", "flash", "lost_ack")
    if not performance:
        modes += tuple("current_" + mode for mode in ("normal", "readonly", "disconnect", "stall", "cancel", "flash", "lost_ack"))
    if performance:
        modes = ("serial0", "bulk0", "bulk1", "serial1", "serial2", "bulk2")
    stopped = threading.Event()
    requests = []
    with tempfile.TemporaryDirectory(prefix="dm-exact-version-batch-") as directory:
        control = Path(directory) / "fault"
        control.write_text("normal")

        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def handle_request(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                bulk = self.command == "POST" and "delete" in self.path.partition("?")[2]
                mode = control.read_text().strip()
                current = mode.startswith("current_")
                fault = mode.removeprefix("current_")
                if performance and self.command == "DELETE" and "versionId=" in self.path:
                    query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                    requests.append({"mode": mode, "objects": [{"Key": urllib.parse.urlsplit(self.path).path,
                                                                "VersionId": query["versionId"][0]}]})
                if bulk:
                    document = ET.fromstring(body)
                    current = document.find("{*}Object/{*}VersionId") is None
                    objects = [{"Key": node.find("{*}Key").text,
                                "VersionId": node.find("{*}VersionId").text if node.find("{*}VersionId") is not None else None}
                               for node in document if node.tag.rsplit("}", 1)[-1] == "Object"]
                    assert len(objects) in ((100, 27) if performance else (1,) if current else (3,)), objects
                    assert all((obj["VersionId"] is None) == current for obj in objects), objects
                    assert document.find("{*}Quiet").text == ("true" if current else "false")
                    requests.append({"mode": mode, "objects": objects})
                    if fault in ("disconnect", "stall", "cancel", "flash"):
                        if fault == "flash":
                            control.write_text("normal")
                        if fault in ("stall", "cancel"):
                            stopped.wait(30)
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    if fault == "readonly":
                        payload = b"<Error><Code>AccessDenied</Code></Error>"
                        self.send_response(403)
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                        return
                    if mode == "partial_denial":
                        response = client.delete_objects(Bucket=bucket, Delete={"Objects": objects[1:], "Quiet": False})
                        assert len(response["Deleted"]) == 2, response
                        reply = ET.Element("DeleteResult")
                        for obj in response["Deleted"]:
                            node = ET.SubElement(reply, "Deleted")
                            ET.SubElement(node, "Key").text = obj["Key"]
                            ET.SubElement(node, "VersionId").text = obj["VersionId"]
                        error = ET.SubElement(reply, "Error")
                        ET.SubElement(error, "Key").text = objects[0]["Key"]
                        ET.SubElement(error, "VersionId").text = objects[0]["VersionId"]
                        ET.SubElement(error, "Code").text = "AccessDenied"
                        payload = ET.tostring(reply)
                        self.send_response(200)
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                        return
                connection = http.client.HTTPConnection("127.0.0.1", 9000, timeout=5)
                try:
                    connection.request(self.command, self.path, body=body, headers=dict(self.headers))
                    response = connection.getresponse()
                    payload = response.read()
                    rewritten = bulk and mode in ("missing_id", "missing_ack", "unknown_id", "duplicate_ack")
                    if rewritten:
                        document = ET.fromstring(payload)
                        first = next(node for node in document if node.tag.rsplit("}", 1)[-1] == "Deleted")
                        if mode == "missing_id":
                            first.remove(first.find("{*}VersionId"))
                        elif mode == "missing_ack":
                            document.remove(first)
                        elif mode == "unknown_id":
                            first.find("{*}VersionId").text = "not-requested"
                        else:
                            document.append(copy.deepcopy(first))
                        payload = ET.tostring(document)
                    if bulk and fault == "lost_ack":
                        control.write_text("normal")
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    self.send_response(response.status)
                    for name, value in response.getheaders():
                        if name.lower() not in ("connection", "content-length", "transfer-encoding") and not (
                                rewritten and name.lower().startswith("x-amz-checksum")):
                            self.send_header(name, value)
                    self.send_header("Content-Length", response.getheader("Content-Length", "0")
                                     if self.command == "HEAD" else str(len(payload)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(payload)
                finally:
                    connection.close()

            do_GET = do_HEAD = do_POST = do_PUT = do_DELETE = handle_request

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for mode in modes:
                prefix = f"nightly-exact-batch-{mode}"
                key = prefix + "/payload"
                client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
                null = client.put_object(Bucket=bucket, Key=key, Body=b"null payload")
                assert null.get("VersionId", "null") == "null", null
                client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
                for _ in range(125 if performance else 1):
                    client.put_object(Bucket=bucket, Key=key, Body=b"old payload")
                client.delete_object(Bucket=bucket, Key=key)
                current = client.put_object(Bucket=bucket, Key=key, Body=b"retain current")
                client.put_object(Bucket=bucket, Key=prefix + "/sentinel", Body=b"retain sentinel")
                before = client.list_object_versions(Bucket=bucket, Prefix=key)
                ids = {record["VersionId"] for record in before.get("Versions", []) + before.get("DeleteMarkers", [])}
                assert len(ids) == (128 if performance else 4) and "null" in ids, before
                environment = dict(os.environ, S3_CONNECT_TIMEOUT="1", S3_OPERATION_TIMEOUT="2", S3_READ_TIMEOUT="1",
                                   LAB_S3_PURGE_URL=f"s3://purge-test:purge-test-isolated-only@{bucket}.127.0.0.1:{server.server_port}/{prefix}",
                                   LAB_S3_PURGE_FAULT=mode, LAB_S3_PURGE_CONTROL=str(control))
                start = len(requests)
                command = ["cargo", "test", "--offline", "--locked", "-p", "data-mover-backend-s3",
                           "backend::client::role_protocol::delete_batch::real_tests::real_exact_version_batch_and_faults",
                           "--", "--ignored", "--exact", "--nocapture"]
                if binary := os.environ.get("LAB_S3_PURGE_TEST_BINARY"):
                    command = [binary, os.environ.get("LAB_S3_PURGE_TEST_NAME", "backend::client::role_protocol::delete_batch::real_tests::real_exact_version_batch_and_faults"),
                               "--ignored", "--exact", "--nocapture"]
                if performance:
                    if not binary:
                        raise ValueError("performance runs require a fixed LAB_S3_PURGE_TEST_BINARY")
                    output = Path(directory) / (mode + ".time")
                    measured = subprocess.run(["/usr/bin/time", "-f", "%e %M", "-o", str(output), *command],
                                              env=environment, check=True, timeout=90, capture_output=True, text=True)
                    print(measured.stdout, flush=True)
                    seconds, rss = output.read_text().split()
                    print(json.dumps({"mode": mode, "process_seconds": float(seconds), "peak_rss_kib": int(rss),
                                      "older_records": 127}), flush=True)
                else:
                    subprocess.run(command, env=environment, check=True, timeout=60)
                after = client.list_object_versions(Bucket=bucket, Prefix=key)
                remaining = after.get("Versions", []) + after.get("DeleteMarkers", [])
                current_case = mode.startswith("current_")
                fault = mode.removeprefix("current_")
                executed = fault not in ("readonly", "disconnect", "stall", "cancel")
                expected = (5 if executed else 4) if current_case else 4 if not executed else 2 if mode == "partial_denial" else 1
                assert len(remaining) == expected, (mode, remaining)
                assert any(record["VersionId"] == current["VersionId"] for record in remaining)
                if current_case:
                    assert ids.issubset({record["VersionId"] for record in remaining}), (mode, remaining)
                    assert sum(record["IsLatest"] for record in after.get("DeleteMarkers", [])) == int(executed)
                assert client.get_object(Bucket=bucket, Key=key, VersionId=current["VersionId"])["Body"].read() == b"retain current"
                assert client.get_object(Bucket=bucket, Key=prefix + "/sentinel")["Body"].read() == b"retain sentinel"
                assert all(obj["VersionId"] != current["VersionId"] for request in requests[start:] for obj in request["objects"])
                expected_requests = (127 if mode.startswith("serial") else 2) if performance else (2 if mode in ("flash", "lost_ack", "current_flash") else 1)
                assert len(requests) - start == expected_requests, requests[start:]
                print(f"independent version batch mode={mode} records={expected} current_data_version_retained=true selection={'Current' if current_case else 'Id'} requests={len(requests)-start}", flush=True)
            assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
            print(f"exact-version batch {len(modes)} cases passed; source current data version and all sentinels retained", flush=True)
        finally:
            control.write_text("normal")
            stopped.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
