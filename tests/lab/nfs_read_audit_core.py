"""Operation-scoped NFSv4 READ attribution; no flattened filehandle/opcode zipping.

Primary references: RFC 8881 sections 2.3, 13.4, 18.22, 18.25 and 18.28;
https://www.rfc-editor.org/rfc/rfc8881.html
Wireshark 4.2.2 epan/dissectors/packet-nfs.c (READ and COMPOUND dissectors):
https://github.com/wireshark/wireshark/blob/wireshark-4.2.2/epan/dissectors/packet-nfs.c
"""

import hashlib
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field


def fields(node, name):
    return list(node.iterfind(f".//field[@name='{name}']"))


def number(node, name, default=None):
    found = fields(node, name)
    if not found:
        if default is not None:
            return default
        raise ValueError(f"missing {name}")
    if len(found) != 1:
        raise ValueError(f"ambiguous operation-local {name}")
    value = found[0].get("show")
    return int(value, 16 if value.startswith("0x") else 10)


def handles(node):
    return [
        bytes.fromhex(item.get("value")).hex() for item in fields(node, "nfs.fhandle")
    ]


def union(ranges):
    result = []
    for start, end in sorted(ranges):
        if start < 0 or end < start or end > 2**64 - 1:
            raise ValueError("invalid byte range")
        if start == end:
            continue
        if result and start <= result[-1][1]:
            result[-1][1] = max(result[-1][1], end)
        else:
            result.append([start, end])
    return result


def intersection(left, right):
    result, i, j = [], 0, 0
    while i < len(left) and j < len(right):
        start, end = max(left[i][0], right[j][0]), min(left[i][1], right[j][1])
        if start < end:
            result.append([start, end])
        if left[i][1] <= right[j][1]:
            i += 1
        else:
            j += 1
    return result


@dataclass
class Handle:
    raw: str | None = None
    path: tuple = ()
    origin: tuple | None = None  # DS handle inherits the MDS object's identity.


@dataclass
class Read:
    key: tuple
    operation: int
    frame: int
    server: str
    offset: int
    count: int
    handle: Handle
    reply: tuple | None = None  # reply frame, operation status, returned byte count


@dataclass
class Call:
    operations: list
    current: list
    reads: list = field(default_factory=list)


class Decoder:
    def __init__(self, maximum=500_000):
        self.maximum, self.objects, self.calls = maximum, {}, {}
        self.reads, self.bindings, self.layouts, self.errors = [], [], [], []
        self.duplicate_calls, self.duplicate_replies, self.truncated = 0, 0, 0

    def object(self, server, raw):
        return self.objects.setdefault((server, raw), Handle(raw=raw))

    def bind(self, server, raw, handle, frame):
        known = self.object(server, raw)
        if handle.path:
            known.path = handle.path
        handle.raw = raw
        self.bindings.append(
            {
                "frame": frame,
                "server": server,
                "fh": raw,
                "path_suffix": list(known.path),
            }
        )
        return known

    def request(self, key, server, frame, nfs):
        ops = fields(nfs, "nfs.opcode")
        codes = [int(op.get("show")) for op in ops]
        if len(codes) != number(nfs, "nfs.ops.count"):
            raise ValueError("incomplete COMPOUND request")
        semantic = [
            [
                (node.get("name"), node.get("value", node.get("show")))
                for node in op.iter("field")
            ]
            for op in ops
        ]
        signature = hashlib.sha256(repr(semantic).encode()).digest()
        if key in self.calls:
            if signature != self.calls[key][0]:
                raise ValueError(
                    "XID reused with different operations; cannot deduplicate safely"
                )
            self.duplicate_calls += 1
            return
        if len(self.calls) >= self.maximum:
            raise ValueError("RPC call limit reached; split the capture")
        current, saved, at = Handle(), Handle(), []
        call = Call(codes, at)
        for index, (code, op) in enumerate(zip(codes, ops)):
            current, saved = self.advance(server, current, saved, code, op)
            at.append(current)
            if code == 25:
                offset, count = number(op, "nfs.offset4"), number(op, "nfs.count4")
                if offset + count > 2**64 - 1:
                    raise ValueError("READ coordinate overflow")
                item = Read(key, index, frame, server, offset, count, current)
                call.reads.append(item)
                self.reads.append(item)
        self.calls[key] = (signature, call)

    def advance(self, server, current, saved, code, op):
        if code == 22:  # PUTFH: exactly this operation's opaque bytes.
            raw = handles(op)
            if len(raw) != 1:
                raise ValueError("PUTFH must have exactly one opaque filehandle")
            current = self.object(server, raw[0])
        elif code in (23, 24):
            current = Handle()
        elif code == 32:
            saved = current
        elif code == 31:
            current = saved
        elif code in (6, 15, 18):  # CREATE/LOOKUP/OPEN replace current FH.
            names = fields(op, "nfs.pathname.component")
            if names:
                current = Handle(path=current.path + (names[-1].get("show"),))
            elif code != 18 or number(op, "nfs.open.claim_type", -1) not in (4, 5, 6):
                current = Handle()
        elif code in (16, 19, 33, 52):  # Unknown/released current FH, never inherit it.
            current = Handle()
        return current, saved

    def metadata_reply(self, key, server, frame, nfs):
        if key not in self.calls:
            return
        call = self.calls[key][1]
        for index, op in enumerate(fields(nfs, "nfs.opcode")):
            code = int(op.get("show"))
            if index >= len(call.operations) or code != call.operations[index]:
                raise ValueError("COMPOUND reply operation order differs from request")
            if number(op, "nfs.status") != 0:
                break
            handle = call.current[index]
            if code == 10:  # GETFH, never arbitrary GETATTR or unrelated FH fields.
                raw = handles(op)
                if len(raw) != 1:
                    raise ValueError("GETFH reply lacks an exact opaque handle")
                self.bind(server, raw[0], handle, frame)
            elif code == 50:
                self.layout(server, handle, frame, op)

    def layout(self, server, handle, frame, op):
        dense = [int(item.get("show")) for item in fields(op, "nfs.nfl_util.dense")]
        if not dense:
            return  # Non-files layouts do not establish an identity mapping.
        for raw in handles(op):
            alias = self.object(server, raw)
            alias.origin = (server, handle.raw)
            alias.path = handle.path
            self.layouts.append(
                {
                    "frame": frame,
                    "mds": server,
                    "mds_fh": handle.raw,
                    "ds_fh": raw,
                    "dense": any(dense),
                }
            )

    def packet(self, packet):
        frame = number(packet, "frame.number")
        if number(packet, "frame.cap_len") < number(packet, "frame.len"):
            self.truncated += 1
        src = fields(packet, "ip.src") or fields(packet, "ipv6.src")
        dst = fields(packet, "ip.dst") or fields(packet, "ipv6.dst")
        if not src or not dst:
            raise ValueError("NFS/TCP packet has no unambiguous IP endpoints")
        stream, rpc = number(packet, "tcp.stream"), None
        for proto in packet:
            if proto.get("name") == "rpc":
                rpc = proto
            elif proto.get("name") == "nfs" and rpc is not None:
                key = (stream, number(rpc, "rpc.xid"))
                kind = number(rpc, "rpc.msgtyp")
                server = (dst if kind == 0 else src)[0].get("show")
                if kind == 0:
                    self.request(key, server, frame, proto)
                else:
                    self.metadata_reply(key, server, frame, proto)
                rpc = None

    def read_reply(self, frame, stream, xid, codes, statuses, lengths):
        key = (stream, xid)
        if key not in self.calls:
            raise ValueError("READ reply lacks captured call")
        call = self.calls[key][1]
        if codes != call.operations[: len(codes)]:
            raise ValueError("READ reply operation order differs from request")
        for item in call.reads:
            if item.operation >= len(codes):
                continue
            status = statuses[item.operation]
            length = lengths.pop(0) if status == 0 else 0
            if length > item.count:
                raise ValueError("READ response exceeds requested count")
            if item.reply is not None:
                if item.reply[1:] != (status, length):
                    raise ValueError("conflicting retransmitted READ reply")
                self.duplicate_replies += 1
            else:
                item.reply = (frame, status, length)


def pdml_packets(stream):
    parser = ET.iterparse(stream, events=("start", "end"))
    _, root = next(parser)
    for event, node in parser:
        if event == "end" and node.tag == "packet":
            yield node
            root.clear()


def numeric_list(value):
    return [
        int(item, 16 if item.startswith("0x") else 10)
        for item in value.split(",")
        if item
    ]


def replies(line, decoder):
    cells = line.rstrip("\n").split("\t")
    if len(cells) != 10:
        raise ValueError("unexpected tshark reply field layout")
    frame, stream = int(cells[0]), int(cells[1])
    if int(cells[2]) < int(cells[3]):
        decoder.truncated += 1
    if not cells[4]:
        return  # A truncated non-RPC frame was deliberately included by the filter.
    xids, kinds, counts, codes, statuses, lengths = map(numeric_list, cells[4:])
    if len(xids) != len(counts) or kinds != [1] * len(xids):
        raise ValueError("cannot partition coalesced RPC replies")
    for xid, count in zip(xids, counts):
        block, codes = codes[:count], codes[count:]
        status, statuses = statuses[1 : count + 1], statuses[count + 1 :]
        if len(block) != count or len(status) != count:
            raise ValueError("incomplete COMPOUND reply fields")
        needed = sum(code == 25 and value == 0 for code, value in zip(block, status))
        data, lengths = lengths[:needed], lengths[needed:]
        if len(data) != needed:
            raise ValueError("incomplete READ response lengths")
        if 25 in block:
            decoder.read_reply(frame, stream, xid, block, status, data)
    if codes or statuses or lengths:
        raise ValueError("unassigned reply fields")
