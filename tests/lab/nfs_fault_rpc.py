"""Bounded RPC record assembly for operation-specific private NFS lab faults."""
import struct

MAX_RECORD = 8 * 1024 * 1024


def _compound_header(payload):
    if len(payload) < 24:
        return None
    _, message, rpc_version, program, version, procedure = struct.unpack_from('>6I', payload)
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
    tag = bytes(payload[offset:offset + size]).decode('ascii', errors='replace')
    return tag, offset + (size + 3) // 4 * 4


def compound_tag(payload):
    if len(payload) < 24:
        return None
    _, message, rpc_version, program, version, procedure = struct.unpack_from('>6I', payload)
    if (message, rpc_version, program, version) == (0, 2, 100003, 3):
        return {2: 'setattr', 12: 'remove', 13: 'remove'}.get(procedure)
    header = _compound_header(payload)
    return header[0] if header is not None else None


class RpcRequests:
    def __init__(self):
        self.pending = bytearray()
        self.wire = bytearray()
        self.payload = bytearray()

    def feed(self, data, include_payload=False):
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
                record = (bytes(self.wire), compound_tag(self.payload), xid, message)
                records.append((*record, bytes(self.payload)) if include_payload else record)
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


def _attribute_masks(payload):
    """Audit only the existing ordinary read compounds; unknown layouts fail closed."""
    header = _compound_header(payload)
    if header is None or header[0] not in ('getattr', 'lookup', 'lookup_path', 'readdir', 'readdirplus', 'getacl'):
        return None
    tag, offset = header
    if offset + 8 > len(payload):
        raise ValueError('truncated lab COMPOUND arguments')
    _, count = struct.unpack_from('>2I', payload, offset)
    offset += 8
    if count > 1024:
        raise ValueError('lab COMPOUND operation count exceeds audit budget')
    masks = []
    for _ in range(count):
        opcode, = struct.unpack_from('>I', payload, offset)
        offset += 4
        if opcode == 53:
            offset += 32
        elif opcode in (22, 15):
            size, = struct.unpack_from('>I', payload, offset)
            offset += 4 + (size + 3) // 4 * 4
        elif opcode in (10, 24, 31, 32):
            pass
        elif opcode in (9, 26):
            if opcode == 26:
                offset += 24
            words, = struct.unpack_from('>I', payload, offset)
            offset += 4
            if words > 16 or offset + words * 4 > len(payload):
                raise ValueError('lab attribute mask exceeds audit budget')
            masks.append((tag, opcode, tuple(struct.unpack_from('>I', payload, offset + i * 4)[0] for i in range(words))))
            offset += words * 4
        else:
            raise ValueError(f'unsupported opcode {opcode} in audited {tag}')
        if offset > len(payload):
            raise ValueError('truncated lab operation arguments')
    if offset != len(payload):
        raise ValueError('unexpected trailing lab COMPOUND arguments')
    return masks


def _deny_acl_reply(payload):
    """Replace a successful real GETACL reply with ACCESS, retaining its SEQUENCE."""
    if len(payload) < 24:
        raise ValueError('truncated lab ACL reply')
    _, message, accepted = struct.unpack_from('>3I', payload)
    verifier_size, = struct.unpack_from('>I', payload, 16)
    offset = 20 + (verifier_size + 3) // 4 * 4
    if message != 1 or accepted != 0 or offset + 12 > len(payload):
        raise ValueError('accepted lab RPC reply required')
    accept_status, compound_status, tag_size = struct.unpack_from('>3I', payload, offset)
    body = offset + 4
    if accept_status != 0 or compound_status != 0 or tag_size > 1024:
        raise ValueError('successful bounded lab GETACL reply required')
    tag_offset = offset + 12
    tag = payload[tag_offset:tag_offset + tag_size]
    offset = tag_offset + (tag_size + 3) // 4 * 4
    if tag != b'getacl' or offset + 64 > len(payload):
        raise ValueError('complete lab GETACL compound required')
    count, sequence, sequence_status = struct.unpack_from('>3I', payload, offset)
    if (count, sequence, sequence_status) != (3, 53, 0):
        raise ValueError('original successful lab SEQUENCE required')
    offset += 4 + 8 + 36
    putfh, putfh_status, getattr, getattr_status = struct.unpack_from('>4I', payload, offset)
    if (putfh, putfh_status, getattr, getattr_status) != (22, 0, 9, 0):
        raise ValueError('lab PUTFH/GETATTR success required')
    result = bytearray(payload[:offset + 16])
    struct.pack_into('>I', result, body, 13)
    struct.pack_into('>I', result, offset + 12, 13)
    return struct.pack('>I', 0x80000000 | len(result)) + result
