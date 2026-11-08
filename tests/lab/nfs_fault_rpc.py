"""Bounded RPC record assembly for operation-specific private NFS lab faults."""
import struct

MAX_RECORD = 8 * 1024 * 1024


def compound_tag(payload):
    if len(payload) < 24:
        return None
    _, message, rpc_version, program, version, procedure = struct.unpack_from('>6I', payload)
    if (message, rpc_version, program, version) == (0, 2, 100003, 3):
        return 'remove' if procedure in (12, 13) else None
    if (message, rpc_version, program, version, procedure) != (0, 2, 100003, 4, 1):
        return None
    offset = 24
    for _ in range(2):
        if offset + 8 > len(payload):
            return None
        _, size = struct.unpack_from('>2I', payload, offset)
        offset += 8 + (size + 3) // 4 * 4
    if offset + 4 > len(payload):
        return None
    size, = struct.unpack_from('>I', payload, offset)
    offset += 4
    if size > 1024 or offset + size > len(payload):
        return None
    return bytes(payload[offset:offset + size]).decode('ascii', errors='replace')


class RpcRequests:
    def __init__(self):
        self.pending = bytearray()
        self.wire = bytearray()
        self.payload = bytearray()

    def feed(self, data):
        self.pending.extend(data)
        records = []
        while len(self.pending) >= 4:
            header, = struct.unpack_from('>I', self.pending)
            length = header & 0x7fffffff
            if length + len(self.wire) > MAX_RECORD:
                raise ValueError('lab RPC request exceeds bounded record budget')
            if len(self.pending) < 4 + length:
                break
            self.wire.extend(self.pending[:4 + length])
            self.payload.extend(self.pending[4:4 + length])
            del self.pending[:4 + length]
            if header & 0x80000000:
                xid, message = struct.unpack_from('>2I', self.payload) if len(self.payload) >= 8 else (None, None)
                records.append((bytes(self.wire), compound_tag(self.payload), xid, message))
                self.wire.clear()
                self.payload.clear()
        return records


def sequence_rejection(xid, tag, status):
    """One accepted RPC reply with a failed SEQUENCE, without forwarding a mutation."""
    encoded = tag.encode('ascii')
    if len(encoded) > 1024 or not 0 <= xid <= 0xffffffff:
        raise ValueError('invalid lab reply identity')
    payload = struct.pack('>6I', xid, 1, 0, 0, 0, 0)
    payload += struct.pack('>2I', status, len(encoded)) + encoded + b'\0' * (-len(encoded) % 4)
    payload += struct.pack('>3I', 1, 53, status)
    return struct.pack('>I', 0x80000000 | len(payload)) + payload
