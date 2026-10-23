#!/usr/bin/env python3
"""Offline NFSv4 READ audit using local tshark, Python's standard library only.

Capture: tcpdump -i any -s 0 -B 16384 -w RUN.pcap 'tcp port 2049'
Begin before connection setup, keep both directions and tcpdump's drop counts.
Example: python3 tests/lab/nfs_read_audit.py RUN.pcap --source-fh HEX_OPAQUE_FH
  --mds 192.168.9.101 --saved-prefix 67108864 --size 402653517 --logical-offsets --capture-drops 0
  --assert-no-overlap

The PDML pass retains operation nesting for exact CURRENT_FH attribution. A
second, compact reply pass partitions each RPC's operation count and statuses;
it never zips unrelated filehandle/offset/count lists. This avoids materializing
gigabytes of reply payload hex. RPC retransmits deduplicate on TCP stream/XID.
Sparse files-layout DS offsets are logical; dense layouts are refused for range
assertions. --logical-offsets asserts the caller has checked any other mapping.
No packet capture proves source immutability or application delivery order.
"""

import argparse
import json
import subprocess
import sys
import tempfile
from collections import Counter
from itertools import pairwise
from pathlib import Path

from nfs_read_audit_core import Decoder, intersection, pdml_packets, replies, union

BASE = [
    "tshark",
    "-n",
    "-o",
    "tcp.desegment_tcp_streams:TRUE",
    "-o",
    "rpc.desegment_rpc_over_tcp:TRUE",
    "-o",
    "rpc.defragment_rpc_over_tcp:TRUE",
    "-o",
    "tcp.reassemble_out_of_order:TRUE",
]
NFS = "rpc.program == 100003 && rpc.programversion == 4 && nfs"
REQUESTS = (
    NFS
    + " && ((rpc.msgtyp == 0 && (!(nfs.opcode == 38) || nfs.opcode == 25)) || (rpc.msgtyp == 1 && (nfs.opcode == 10 || nfs.opcode == 50)))"
)
REPLIES = (
    "(" + NFS + " && rpc.msgtyp == 1 && nfs.opcode == 25) || frame.cap_len < frame.len"
)
REPLY_FIELDS = [
    "frame.number",
    "tcp.stream",
    "frame.cap_len",
    "frame.len",
    "rpc.xid",
    "rpc.msgtyp",
    "nfs.ops.count",
    "nfs.opcode",
    "nfs.status",
    "nfs.read.data_length",
]


def run_tshark(path, options, consume):
    with tempfile.TemporaryFile() as diagnostics:
        command = BASE + ["-r", str(path)] + options
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=diagnostics)
        try:
            consume(process.stdout)
            status = process.wait()
            if status:
                diagnostics.seek(0)
                raise ValueError(diagnostics.read(8192).decode(errors="replace"))
        finally:
            process.stdout.close()
            if process.poll() is None:
                process.terminate()
                process.wait()


def decode(path, maximum):
    decoder = Decoder(maximum)

    def requests(stream):
        for packet in pdml_packets(stream):
            decoder.packet(packet)

    run_tshark(path, ["-Y", REQUESTS, "-T", "pdml"], requests)
    options = ["-Y", REPLIES, "-T", "fields"]
    for name in REPLY_FIELDS:
        options += ["-e", name]

    def responses(stream):
        for line in stream:
            replies(line.decode(), decoder)

    run_tshark(path, options, responses)
    return decoder


def source_handles(decoder, args):
    selected = {bytes.fromhex(value.replace(":", "")).hex() for value in args.source_fh}
    suffix = tuple(args.source_name.strip("/").split("/")) if args.source_name else ()
    if suffix and len(suffix) < 2:
        raise ValueError(
            "basename-only selection is ambiguous; use an exact FH or a qualified observed path suffix"
        )
    if suffix:
        selected.update(
            item[1]
            for item, handle in decoder.objects.items()
            if handle.path[-len(suffix) :] == suffix
        )
    changed = True
    while changed:
        changed = False
        for layout in decoder.layouts:
            if layout["mds_fh"] in selected and layout["ds_fh"] not in selected:
                selected.add(layout["ds_fh"])
                changed = True
    return selected


def saved_ranges(args):
    if args.saved_d:
        if args.saved_d.stat().st_size > 1024 * 1024:
            raise ValueError("saved D JSON exceeds one MiB")
        value = json.loads(args.saved_d.read_text())
        value = value["ranges"] if isinstance(value, dict) else value
        return union(value)
    return union([[0, args.saved_prefix or 0]])


def completion_examples(completion):
    return [
        {
            "call_frame": item.frame,
            "reply_frame": item.reply[0],
            "offset": item.offset,
            "count": item.count,
            "server": item.server,
        }
        for item in completion[:24]
    ]


def summarize(reads):
    requested = union((item.offset, item.offset + item.count) for item in reads)
    completed = [item for item in reads if item.reply and item.reply[1] == 0]
    returned = union((item.offset, item.offset + item.reply[2]) for item in completed)
    order = {
        id(item): rank
        for rank, item in enumerate(sorted(reads, key=lambda r: (r.frame, r.operation)))
    }
    completion = sorted(completed, key=lambda item: item.reply[0])
    descents = sum(
        a.reply[0] != b.reply[0] and order[id(a)] > order[id(b)]
        for a, b in pairwise(completion)
    )
    inversions = [
        {
            "earlier_reply": {
                "call_frame": a.frame,
                "reply_frame": a.reply[0],
                "offset": a.offset,
            },
            "later_reply": {
                "call_frame": b.frame,
                "reply_frame": b.reply[0],
                "offset": b.offset,
            },
        }
        for a, b in pairwise(completion)
        if a.reply[0] != b.reply[0] and order[id(a)] > order[id(b)]
    ]
    return {
        "read_operations": len(reads),
        "requested_bytes": sum(item.count for item in reads),
        "requested_union": requested,
        "requested_union_bytes": sum(b - a for a, b in requested),
        "returned_union": returned,
        "returned_bytes": sum(item.reply[2] for item in completed),
        "unanswered": sum(item.reply is None for item in reads),
        "failed": sum(item.reply is not None and item.reply[1] != 0 for item in reads),
        "wire_completion_call_order_descents": descents,
        "wire_completion_inversion_examples": inversions[:16],
        "completion_examples": completion_examples(completion),
        "read_endpoints": dict(Counter(item.server for item in reads)),
    }


def audit_errors(
    decoder, args, result, durable, overlap, relevant, unmapped, unresolved
):
    errors = []
    if not result["read_operations"]:
        errors.append("no selected source READs; cannot claim no rereads")
    if overlap:
        errors.append("source READ requests intersect saved D")
    if decoder.truncated:
        errors.append("truncated selected NFS frames")
    if not args.logical_offsets or any(layout["dense"] for layout in relevant):
        errors.append("logical offset mapping is unverified or contains a dense layout")
    if not args.mds:
        errors.append(
            "MDS endpoint is required to distinguish logical from DS wire offsets"
        )
    if unmapped:
        errors.append(
            "non-MDS READ lacks a captured sparse files-layout identity mapping"
        )
    if args.capture_drops != 0:
        errors.append("capture loss is nonzero or not independently supplied")
    if result["unanswered"] or result["failed"]:
        errors.append("selected source READs have missing or failed replies")
    if result["returned_union"] != result["requested_union"]:
        errors.append("successful READ replies do not cover every requested byte")
    if unresolved:
        errors.append(
            "unresolved current FH on READ calls prevents complete source attribution"
        )
    if args.size is not None and union(durable + result["requested_union"]) != [
        [0, args.size]
    ]:
        errors.append(
            "saved D plus source READs do not exactly cover the fixed file size"
        )
    return errors


def report_metadata(decoder, args, selected):
    return {
        "schema": 1,
        "pcap": str(args.pcap),
        "source_selection": {
            "explicit_filehandles": args.source_fh,
            "observed_path_suffix": args.source_name,
            "all_reads_explicitly_isolated": args.all_reads_source,
        },
        "source_filehandles": sorted(selected),
        "duplicate_calls": decoder.duplicate_calls,
        "duplicate_read_replies": decoder.duplicate_replies,
        "capture_drops_supplied": args.capture_drops,
        "truncated_frames": decoder.truncated,
        "mds_endpoint": args.mds,
        "bindings": decoder.bindings,
        "layouts": decoder.layouts,
        "all_read_operations": len(decoder.reads),
        "all_read_filehandles": dict(
            Counter(item.handle.raw or "UNRESOLVED" for item in decoder.reads)
        ),
        "all_read_endpoint_fh_groups": endpoint_groups(decoder.reads),
    }


def unmapped_reads(reads, relevant, args):
    return [
        item.frame
        for item in reads
        if item.server != args.mds
        and not any(
            layout["mds"] == args.mds
            and layout["ds_fh"] == item.handle.raw
            and not layout["dense"]
            for layout in relevant
        )
    ]


def report(decoder, args):
    selected = source_handles(decoder, args)
    reads = [
        item
        for item in decoder.reads
        if args.from_frame <= item.frame <= args.through_frame
        and (args.all_reads_source or item.handle.raw in selected)
        and (not args.server or item.server in args.server)
    ]
    result = summarize(reads)
    durable = saved_ranges(args)
    overlap = intersection(result["requested_union"], durable)
    relevant = [
        layout
        for layout in decoder.layouts
        if layout["mds_fh"] in selected or layout["ds_fh"] in selected
    ]
    unmapped = unmapped_reads(reads, relevant, args)
    unresolved = [
        item.frame
        for item in decoder.reads
        if item.handle.raw is None
        and args.from_frame <= item.frame <= args.through_frame
        and (not args.server or item.server in args.server)
    ]
    errors = audit_errors(
        decoder, args, result, durable, overlap, relevant, unmapped, unresolved
    )
    result.update(report_metadata(decoder, args, selected))
    result.update(
        saved_d=durable,
        overlap_saved_d=overlap,
        audit_errors=errors,
        unresolved_read_frames=unresolved[:32],
        unmapped_ds_read_frames=unmapped[:32],
        no_overlap_and_complete=not errors,
    )
    return result


def endpoint_groups(reads):
    groups = {}
    for item in reads:
        groups.setdefault((item.server, item.handle.raw), []).append(item)
    return [
        {
            "server": server,
            "fh": raw,
            "read_operations": len(items),
            "requested_bytes": sum(item.count for item in items),
            "requested_union": union(
                (item.offset, item.offset + item.count) for item in items
            ),
        }
        for (server, raw), items in groups.items()
    ]


def arguments():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("pcap", type=Path)
    parser.add_argument("--source-fh", action="append", default=[])
    parser.add_argument(
        "--source-name",
        help="qualified observed path suffix (at least two components); never basename-only",
    )
    parser.add_argument(
        "--all-reads-source",
        action="store_true",
        help="only for isolated NFS-to-local captures",
    )
    parser.add_argument(
        "--server",
        action="append",
        default=[],
        help="allowed source MDS/DS IP, repeatable",
    )
    parser.add_argument(
        "--mds",
        help="metadata server IP; required for a positive completeness assertion",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--saved-d", type=Path, help="JSON [[start,end],...] or {ranges:...}"
    )
    group.add_argument("--saved-prefix", type=int)
    parser.add_argument("--size", type=int)
    parser.add_argument("--logical-offsets", action="store_true")
    parser.add_argument("--capture-drops", type=int)
    parser.add_argument("--from-frame", type=int, default=0)
    parser.add_argument("--through-frame", type=int, default=2**63 - 1)
    parser.add_argument("--max-calls", type=int, default=100_000)
    parser.add_argument("--assert-no-overlap", action="store_true")
    args = parser.parse_args()
    if not (args.source_fh or args.source_name or args.all_reads_source):
        parser.error(
            "select source filehandles/name, or explicitly isolate all source reads"
        )
    return args


def main():
    args = arguments()
    try:
        result = report(decode(args.pcap, args.max_calls), args)
        print(json.dumps(result, indent=2))
        return 1 if args.assert_no_overlap and result["audit_errors"] else 0
    except (ValueError, OSError) as error:
        print(f"NFS audit refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
