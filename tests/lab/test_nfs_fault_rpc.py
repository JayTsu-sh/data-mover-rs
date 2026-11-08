"""The fault trigger must recognize real RPC tags even across TCP/RPC fragments."""
import struct
import unittest

from nfs_fault_rpc import MAX_RECORD, RpcRequests


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

    def test_oversized_request_is_rejected_before_buffering(self):
        with self.assertRaises(ValueError):
            RpcRequests().feed(struct.pack('>I', 0x80000000 | MAX_RECORD + 1))


if __name__ == '__main__':
    unittest.main()
