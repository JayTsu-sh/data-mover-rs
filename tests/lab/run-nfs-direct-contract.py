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

from nfs_fault_rpc import RpcRequests, sequence_rejection, _attribute_masks, _deny_acl_reply


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--url', required=True)
    parser.add_argument('--functional', action='store_true')
    parser.add_argument('--contract', choices=('direct', 'persistence', 'nonfile', 'purge', 'metadata-plan', 'metadata-cancel'), default='direct')
    parser.add_argument('--test-name')
    parser.add_argument('--operation-tag', choices=('commit', 'lookup_path', 'remove', 'setattr', 'setacl', 'getacl'))
    args = parser.parse_args()
    args.operation_tag = args.operation_tag or {'direct': 'commit', 'persistence': 'commit', 'nonfile': 'lookup_path', 'purge': 'remove', 'metadata-plan': 'lookup_path', 'metadata-cancel': 'setattr'}[args.contract]
    if args.contract == 'metadata-plan' and not args.test_name:
        raise ValueError('metadata-plan requires an explicit prebuilt test name')
    parsed = urllib.parse.urlsplit(args.url)
    allowed = r'/srv/nfs/v[34]:/ci/nightly-nfs-direct-[A-Za-z0-9_-]+' if args.contract in ('purge', 'metadata-cancel') else r'/srv/nfs/v4:/ci/nightly-nfs-direct-[A-Za-z0-9_-]+'
    if not re.fullmatch(allowed, parsed.path):
        raise ValueError('owned prepared NFS prefix required')
    if not args.binary.is_file():
        raise ValueError('prebuilt contract binary required')
    query = urllib.parse.parse_qs(parsed.query)
    v3 = query.get('version') == ['3']
    if v3 and not query.get('mountport'):
        raise ValueError('v3 relay requires the server mountport explicitly')
    state = {'mode': 'normal', 'armed': None, 'sockets': set(), 'faults': {}, 'operation_hits': {}, 'lost_operation_replies': 0, 'seen_requests': {}, 'session_rejections': 0, 'recovery_exchanges': 0, 'attribute_requests': 0, 'common_acl_requests': 0, 'explicit_acl_requests': 0, 'acl_denials': 0, 'plan_omit_verified': False}
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
        upstream_port = parsed.port or 2049

        def handle(self):
            upstream = None
            try:
                upstream = socket.create_connection((parsed.hostname, self.upstream_port), timeout=5)
                upstream.settimeout(None)
                with guard:
                    state['sockets'].update((self.request, upstream))
                with selectors.DefaultSelector() as ready:
                    ready.register(self.request, selectors.EVENT_READ, upstream)
                    ready.register(upstream, selectors.EVENT_READ, self.request)
                    requests = RpcRequests()
                    responses = RpcRequests()
                    lost_xid = None
                    acl_xids = set()
                    while not stopped.is_set():
                        for selected, _ in ready.select(.1):
                            data = selected.fileobj.recv(65536)
                            if not data:
                                return
                            records = (requests if selected.fileobj is self.request else responses).feed(data, include_payload=True)
                            for wire, tag, xid, message, payload in records:
                                masks = _attribute_masks(payload) if args.contract == 'metadata-plan' and selected.fileobj is self.request else None
                                with guard:
                                    if masks is not None:
                                        state['attribute_requests'] += len(masks)
                                        for request_tag, _, mask in masks:
                                            if mask and mask[0] & (1 << 12):
                                                key = 'explicit_acl_requests' if request_tag == 'getacl' else 'common_acl_requests'
                                                state[key] += 1
                                    if args.contract == 'metadata-plan' and selected.fileobj is self.request and tag == 'getacl' and state['mode'] == 'acl_denied':
                                        acl_xids.add(xid)
                                    deny_acl = args.contract == 'metadata-plan' and selected.fileobj is upstream and xid in acl_xids
                                    if deny_acl:
                                        acl_xids.remove(xid)
                                        state['acl_denials'] += 1
                                    if selected.fileobj is self.request and tag:
                                        state['seen_requests'][tag] = state['seen_requests'].get(tag, 0) + 1
                                        if tag == 'exchange_id' and state['session_rejections']:
                                            state['recovery_exchanges'] += 1
                                    if tag == args.operation_tag and state['armed']:
                                        if state['armed'] == 'lost_ack':
                                            lost_xid = xid
                                        state['mode'] = state['armed']
                                        state['operation_hits'][state['armed']] = state['operation_hits'].get(state['armed'], 0) + 1
                                        state['armed'] = None
                                        triggered.set()
                                    mode = state['mode']
                                    reject_session = mode == 'bad_session' and selected.fileobj is self.request and tag == args.operation_tag
                                    if reject_session:
                                        state['session_rejections'] += 1
                                        state['faults']['bad_session'] = state['faults'].get('bad_session', 0) + 1
                                        state['mode'] = 'normal'
                                    lose_reply = mode == 'lost_ack' and selected.fileobj is upstream and message == 1 and (lost_xid is None or xid == lost_xid)
                                    if lose_reply and lost_xid is not None:
                                        state['lost_operation_replies'] += 1
                                    if mode in ('flash', 'disconnect', 'stall', 'cancel') or lose_reply:
                                        state['faults'][mode] = state['faults'].get(mode, 0) + 1
                                    if mode == 'flash' or lose_reply:
                                        state['mode'] = 'normal'
                                if deny_acl:
                                    selected.data.sendall(_deny_acl_reply(payload))
                                    continue
                                if reject_session:
                                    self.request.sendall(sequence_rejection(xid, tag, 10052))
                                    continue
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
            if mode not in ('normal', 'flash', 'disconnect', 'stall', 'cancel', 'lost_ack', 'bad_session', 'acl_denied'):
                self.send_error(400)
                return
            with guard:
                if args.contract == 'metadata-plan' and mode == 'acl_denied':
                    if state['common_acl_requests'] or state['explicit_acl_requests']:
                        self.send_error(409, 'ACL requested during omitted phase')
                        return
                    state['plan_omit_verified'] = True
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

    servers = []
    if v3:
        class MountRelay(Relay):
            upstream_port = int(query['mountport'][0])
        mount_relay = Server(('127.0.0.1', 0), MountRelay)
        servers.append(mount_relay)
        query['mountport'] = [str(mount_relay.server_address[1])]
    query.pop('nfsport', None)
    relay = Server(('127.0.0.1', 0), Relay)
    control = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Control)
    control.daemon_threads = True
    servers.extend((relay, control))
    for server in servers:
        threading.Thread(target=server.serve_forever, daemon=True).start()
    proxy_url = urllib.parse.urlunsplit((parsed.scheme, f'127.0.0.1:{relay.server_address[1]}', parsed.path, urllib.parse.urlencode(query, doseq=True), ''))
    env = dict(os.environ, LAB_NFS_DIRECT_URL=args.url, LAB_NFS_DIRECT_PROXY_URL=proxy_url,
               LAB_NFS_DIRECT_CONTROL=f'127.0.0.1:{control.server_port}')
    try:
        if args.functional:
            subprocess.run([str(args.binary), '--ignored', '--exact', 'direct::real_nfs_direct_data_and_metadata_contract',
                            '--nocapture', '--test-threads=1'], check=True, env=env, timeout=100)
        subprocess.run([str(args.binary), '--ignored', '--exact', args.test_name or {'direct': 'direct::fault::real_nfs_direct_fault_contract', 'persistence': 'direct::persistence::real_nfs_persistence_fault_contract', 'nonfile': 'direct::nonfile::real_nfs_nonfile_persistence_fault_contract', 'purge': 'direct::purge::real_nfs_source_purge_fault_contract', 'metadata-cancel': 'direct::metadata_cancel::real_nfs_metadata_fault_contract'}[args.contract],
                        '--nocapture', '--test-threads=1'], check=True, env=env, timeout=180)
        if args.contract == 'metadata-plan':
            assert state['plan_omit_verified'] and state['attribute_requests'] > 0, state
            assert state['common_acl_requests'] == 0, state
            assert state['explicit_acl_requests'] == 3 and state['acl_denials'] == 2, state
            print(f'independent NFS metadata audit audited_attribute_requests={state["attribute_requests"]} implicit_acl_requests={state["common_acl_requests"]} explicit_acl_requests={state["explicit_acl_requests"]} denied_acl_replies={state["acl_denials"]} omit_phase_no_acl={state["plan_omit_verified"]}', flush=True)
        else:
            expected = [os.environ['LAB_NFS_DIRECT_ONLY']] if os.environ.get('LAB_NFS_DIRECT_ONLY') else ('flash', 'disconnect', 'stall', 'cancel', 'lost_ack')
            if args.contract == 'purge' and urllib.parse.parse_qs(parsed.query).get('version') != ['3'] and not os.environ.get('LAB_NFS_DIRECT_ONLY'):
                expected = (*expected, 'bad_session')
            assert all(state['faults'].get(mode, 0) for mode in expected), state['faults']
            if args.contract != 'direct':
                assert all(state['operation_hits'].get(mode, 0) for mode in expected), state['operation_hits']
                if 'lost_ack' in expected:
                    assert state['lost_operation_replies'] > 0
                if 'bad_session' in expected:
                    assert state['session_rejections'] == 1 and state['recovery_exchanges'] == 0, state
                    print(f'independent NFS relay session_rejections={state["session_rejections"]} fresh_recovery_exchanges={state["recovery_exchanges"]}', flush=True)
                if args.operation_tag == 'lookup_path':
                    assert state['seen_requests'].get('commit', 0) == 0, state['seen_requests']
                print(f'independent NFS relay confirmed_tag={args.operation_tag} commit_requests={state["seen_requests"].get("commit", 0)}', flush=True)
                print(f'independent NFS relay lost_operation_replies={state["lost_operation_replies"]}', flush=True)
                print(f'independent NFS relay operation_hits={state["operation_hits"]}', flush=True)
            print(f'independent NFS relay actual_fault_rpc_records={state["faults"]} scope=owned_connections_only', flush=True)
    finally:
        stopped.set()
        disconnect()
        for server in reversed(servers):
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    main()
