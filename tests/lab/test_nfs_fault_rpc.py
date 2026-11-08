"""The fault trigger must recognize real RPC tags even across TCP/RPC fragments."""
import struct
import unittest

from nfs_fault_rpc import MAX_RECORD, RpcRequests, sequence_rejection, _attribute_masks, _deny_acl_reply


def request(tag=b'commit', program=100003):
    payload = struct.pack('>10I', 1, 0, 2, program, 4, 1, 0, 0, 0, 0)
    return payload + struct.pack('>I', len(tag)) + tag + bytes((-len(tag)) % 4)


def frame(payload, final=True):
    return struct.pack('>I', len(payload) | (0x80000000 if final else 0)) + payload


class RpcFaultTagTests(unittest.TestCase):
    def test_tcp_byte_splits_preserve_exact_request_and_tag(self):
        stream = RpcRequests()
        wire = frame(request())
        records = []
        for value in wire:
            records.extend(stream.feed(bytes([value])))
        self.assertEqual(records, [(wire, 'commit', 1, 0)])

    def test_multiple_rpc_fragments_and_records(self):
        payload = request()
        wire = frame(payload[:29], final=False) + frame(payload[29:])
        other = frame(request(b'lookup_path'))
        self.assertEqual(RpcRequests().feed(wire + other), [(wire, 'commit', 1, 0), (other, 'lookup_path', 1, 0)])

    def test_other_rpc_program_is_never_a_commit_trigger(self):
        wire = frame(request(program=100005))
        self.assertEqual(RpcRequests().feed(wire), [(wire, None, 1, 0)])

    def test_v3_only_remove_and_rmdir_are_purge_triggers(self):
        for procedure in (3, 12, 13, 21):
            payload = struct.pack('>10I', 7, 0, 2, 100003, 3, procedure, 0, 0, 0, 0)
            wire = frame(payload)
            tag = 'remove' if procedure in (12, 13) else None
            self.assertEqual(RpcRequests().feed(wire), [(wire, tag, 7, 0)])

    def test_session_rejection_preserves_rpc_and_compound_identity(self):
        wire = sequence_rejection(7, 'remove', 10052)
        self.assertEqual(RpcRequests().feed(wire), [(wire, None, 7, 1)])
        self.assertEqual(struct.unpack_from('>6I', wire, 4), (7, 1, 0, 0, 0, 0))
        self.assertEqual(struct.unpack_from('>2I', wire, 28), (10052, 6))
        self.assertEqual(wire[36:42], b'remove')
        self.assertEqual(struct.unpack_from('>3I', wire, 44), (1, 53, 10052))

    def test_optional_payload_is_the_original_reassembled_rpc_body(self):
        payload = request()
        wire = frame(payload[:29], final=False) + frame(payload[29:])
        self.assertEqual(RpcRequests().feed(wire, include_payload=True), [(wire, 'commit', 1, 0, payload)])

    def test_common_attribute_masks_are_audited_in_numeric_operation_order(self):
        sequence = struct.pack('>I', 53) + bytes(32)
        putfh = struct.pack('>2I', 22, 1) + b'h' + bytes(3)
        lookup = struct.pack('>2I', 15, 1) + b'x' + bytes(3)
        getattr = struct.pack('>4I', 9, 2, (1 << 1) | (1 << 4), 1 << 1)
        payload = request(b'lookup_path') + struct.pack('>2I', 1, 5)
        payload += sequence + putfh + lookup + struct.pack('>I', 10) + getattr
        self.assertEqual(_attribute_masks(payload), [('lookup_path', 9, (18, 2))])
        readdir = struct.pack('>I', 26) + bytes(16) + struct.pack('>4I', 1024, 4096, 1, 1 << 12)
        payload = request(b'readdirplus') + struct.pack('>2I', 1, 3) + sequence + putfh + readdir
        self.assertEqual(_attribute_masks(payload), [('readdirplus', 26, (1 << 12,))])
        with self.assertRaises((ValueError, struct.error)):
            _attribute_masks(payload[:-1])

    def test_acl_denial_retains_the_real_session_and_reply_prefix(self):
        sequence = bytes(range(16)) + struct.pack('>5I', 42, 3, 7, 7, 0)
        payload = struct.pack('>6I', 7, 1, 0, 0, 0, 0)
        payload += struct.pack('>2I', 0, 6) + b'getacl' + bytes(2)
        payload += struct.pack('>3I', 3, 53, 0) + sequence
        payload += struct.pack('>4I', 22, 0, 9, 0) + bytes(16)
        denied = _deny_acl_reply(payload)
        record = RpcRequests().feed(denied, include_payload=True)[0]
        self.assertEqual(record[2:4], (7, 1))
        body = record[4]
        self.assertEqual(body[44:88], payload[44:88])
        self.assertEqual(struct.unpack_from('>I', body, 24), (13,))
        self.assertEqual(struct.unpack_from('>4I', body, 88), (22, 0, 9, 13))
        self.assertEqual(len(body), 104)
        invalid = bytearray(payload)
        struct.pack_into('>I', invalid, 40, 4)
        with self.assertRaises(ValueError):
            _deny_acl_reply(invalid)

    def test_oversized_request_is_rejected_before_buffering(self):
        with self.assertRaises(ValueError):
            RpcRequests().feed(struct.pack('>I', 0x80000000 | MAX_RECORD + 1))


if __name__ == '__main__':
    unittest.main()
