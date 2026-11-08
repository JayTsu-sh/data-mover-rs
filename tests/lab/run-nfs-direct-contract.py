#!/usr/bin/env python3
"""Run a prebuilt NFS contract inside an exclusively prepared lab prefix.

Only this process's TCP relay injects faults. Preparation, independent server-side
inspection and exact-prefix cleanup belong to the existing lab lifecycle.
"""
import argparse
import http.server
import os
from pathlib import Path
import re
import selectors
import socket
import socketserver
import subprocess
import threading
import urllib.parse

from nfs_fault_rpc import RpcRequests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--url', required=True)
    parser.add_argument('--functional', action='store_true')
    parser.add_argument('--contract', choices=('direct', 'persistence'), default='direct')
    args = parser.parse_args()
    parsed = urllib.parse.urlsplit(args.url)
    if not re.fullmatch(r'/srv/nfs/v4:/ci/nightly-nfs-direct-[A-Za-z0-9_-]+', parsed.path):
        raise ValueError('owned prepared v4 NFS prefix required')
    if not args.binary.is_file():
        raise ValueError('prebuilt contract binary required')
    state = {'mode': 'normal', 'armed': None, 'sockets': set(), 'faults': {}, 'commit_hits': {}, 'lost_commit_replies': 0}
    triggered = threading.Event()
    guard = threading.Lock()
    stopped = threading.Event()

    def disconnect():
        with guard:
            peers = list(state['sockets'])
        for peer in peers:
            try:
                peer.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    class Relay(socketserver.BaseRequestHandler):
        def handle(self):
            upstream = None
            try:
                upstream = socket.create_connection((parsed.hostname, parsed.port or 2049), timeout=5)
                upstream.settimeout(None)
                with guard:
                    state['sockets'].update((self.request, upstream))
                with selectors.DefaultSelector() as ready:
                    ready.register(self.request, selectors.EVENT_READ, upstream)
                    ready.register(upstream, selectors.EVENT_READ, self.request)
                    requests = RpcRequests()
                    responses = RpcRequests()
                    lost_xid = None
                    while not stopped.is_set():
                        for selected, _ in ready.select(.1):
                            data = selected.fileobj.recv(65536)
                            if not data:
                                return
                            records = (requests if selected.fileobj is self.request else responses).feed(data)
                            for wire, tag, xid, message in records:
                                with guard:
                                    if tag == 'commit' and state['armed']:
                                        if state['armed'] == 'lost_ack':
                                            lost_xid = xid
                                        state['mode'] = state['armed']
                                        state['commit_hits'][state['armed']] = state['commit_hits'].get(state['armed'], 0) + 1
                                        state['armed'] = None
                                        triggered.set()
                                    mode = state['mode']
                                    lose_reply = mode == 'lost_ack' and selected.fileobj is upstream and message == 1 and (lost_xid is None or xid == lost_xid)
                                    if lose_reply and lost_xid is not None:
                                        state['lost_commit_replies'] += 1
                                    if mode in ('flash', 'disconnect', 'stall', 'cancel') or lose_reply:
                                        state['faults'][mode] = state['faults'].get(mode, 0) + 1
                                    if mode == 'flash' or lose_reply:
                                        state['mode'] = 'normal'
                                if mode in ('flash', 'disconnect') or lose_reply:
                                    return
                                if mode in ('stall', 'cancel'):
                                    continue
                                selected.data.sendall(wire)
            except OSError:
                pass
            finally:
                with guard:
                    state['sockets'].discard(self.request)
                    state['sockets'].discard(upstream)
                if upstream:
                    upstream.close()

    class Control(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            mode = self.path.lstrip('/')
            if mode == 'wait-fault':
                if not triggered.wait(10):
                    self.send_error(503)
                    return
                self.send_response(204)
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            armed = mode.removeprefix('commit-') if mode.startswith('commit-') else None
            if armed:
                mode = armed
            if mode not in ('normal', 'flash', 'disconnect', 'stall', 'cancel', 'lost_ack'):
                self.send_error(400)
                return
            with guard:
                previous = state['mode']
                state['mode'] = 'normal' if armed else mode
                state['armed'] = armed
                triggered.clear()
            if not armed and (mode == 'disconnect' or mode == 'normal' and previous in ('disconnect', 'stall', 'cancel')):
                disconnect()
            self.send_response(204)
            self.send_header('Content-Length', '0')
            self.end_headers()

    class Server(socketserver.ThreadingTCPServer):
        daemon_threads = True
        allow_reuse_address = True

    relay = Server(('127.0.0.1', 0), Relay)
    control = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Control)
    control.daemon_threads = True
    for server in (relay, control):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    proxy_url = urllib.parse.urlunsplit((parsed.scheme, f'127.0.0.1:{relay.server_address[1]}', parsed.path, parsed.query, ''))
    env = dict(os.environ, LAB_NFS_DIRECT_URL=args.url, LAB_NFS_DIRECT_PROXY_URL=proxy_url,
               LAB_NFS_DIRECT_CONTROL=f'127.0.0.1:{control.server_port}')
    try:
        if args.functional:
            subprocess.run([str(args.binary), '--ignored', '--exact', 'direct::real_nfs_direct_data_and_metadata_contract',
                            '--nocapture', '--test-threads=1'], check=True, env=env, timeout=100)
        subprocess.run([str(args.binary), '--ignored', '--exact', ('direct::fault::real_nfs_direct_fault_contract' if args.contract == 'direct' else 'direct::persistence::real_nfs_persistence_fault_contract'),
                        '--nocapture', '--test-threads=1'], check=True, env=env, timeout=180)
        expected = [os.environ['LAB_NFS_DIRECT_ONLY']] if os.environ.get('LAB_NFS_DIRECT_ONLY') else ('flash', 'disconnect', 'stall', 'cancel', 'lost_ack')
        assert all(state['faults'].get(mode, 0) for mode in expected), state['faults']
        if args.contract == 'persistence':
            assert all(state['commit_hits'].get(mode, 0) for mode in expected), state['commit_hits']
            if 'lost_ack' in expected:
                assert state['lost_commit_replies'] > 0
            print(f'independent NFS relay lost_commit_replies={state["lost_commit_replies"]}', flush=True)
            print(f'independent NFS relay commit_operation_hits={state["commit_hits"]}', flush=True)
        print(f'independent NFS relay actual_fault_rpc_records={state["faults"]} scope=owned_connections_only', flush=True)
    finally:
        stopped.set()
        disconnect()
        for server in (control, relay):
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    main()
