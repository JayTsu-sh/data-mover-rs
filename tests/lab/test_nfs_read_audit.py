"""Small operation-tree fixtures; run python3 -m unittest discover -s tests/lab -p test_nfs_read_audit.py."""

import unittest
import xml.etree.ElementTree as ET
from types import SimpleNamespace

from nfs_read_audit import report
from nfs_read_audit_core import Decoder, handles, intersection, replies, union


def field(name, value):
    return ET.Element("field", name=name, show=str(value), value=str(value))


def op(code, *children, status=None):
    node = field("nfs.opcode", code)
    if status is not None:
        node.append(field("nfs.status", status))
    node.extend(children)
    return node


def fh(raw):
    return field("nfs.fhandle", raw)


def put(raw):
    return op(22, fh(raw))


def read(offset, count):
    return op(25, field("nfs.offset4", offset), field("nfs.count4", count))


def compound(*operations):
    node = ET.Element("proto", name="nfs")
    count = field("nfs.ops.count", len(operations))
    count.extend(operations)
    node.append(count)
    return node


def args(**changes):
    values = {
        "source_fh": ["aabb"],
        "source_name": None,
        "all_reads_source": False,
        "from_frame": 0,
        "through_frame": 10**9,
        "server": [],
        "saved_d": None,
        "saved_prefix": 4,
        "logical_offsets": True,
        "capture_drops": 0,
        "size": 16,
        "pcap": "fixture",
        "mds": "mds",
    }
    values.update(changes)
    return SimpleNamespace(**values)


def response(decoder, frame, stream, xid, codes, statuses, lengths):
    cells = [
        frame,
        stream,
        100,
        100,
        hex(xid),
        1,
        len(codes),
        ",".join(map(str, codes)),
        ",".join(map(str, [0] + statuses)),
        ",".join(map(str, lengths)),
    ]
    replies("\t".join(map(str, cells)), decoder)


class ReadAuditTests(unittest.TestCase):
    def test_current_fh_changes_save_restore_and_metadata_do_not_zip(self):
        decoder = Decoder()
        request = compound(
            put("aabb"),
            op(32),
            put("ccdd"),
            op(9, fh("eeee")),
            read(4, 2),
            op(31),
            read(6, 2),
        )
        decoder.request((1, 8), "mds", 1, request)
        self.assertEqual([item.handle.raw for item in decoder.reads], ["ccdd", "aabb"])
        decoder.request((1, 9), "mds", 2, compound(read(0, 1)))
        self.assertIsNone(
            decoder.reads[-1].handle.raw
        )  # No cross-COMPOUND inheritance.

    def test_getfh_binding_is_operation_scoped(self):
        decoder = Decoder()
        request = compound(
            put("1122"),
            op(18, field("nfs.pathname.component", "payload.bin")),
            op(10),
            op(9),
        )
        decoder.request((1, 1), "mds", 1, request)
        reply = compound(
            op(22, status=0),
            op(18, status=0),
            op(10, fh("aabb"), status=0),
            op(9, fh("ccdd"), status=0),
        )
        decoder.metadata_reply((1, 1), "mds", 2, reply)
        self.assertEqual(decoder.objects["mds", "aabb"].path, ("payload.bin",))
        self.assertNotIn(("mds", "ccdd"), decoder.objects)

    def test_rpc_and_read_operation_reply_association(self):
        decoder = Decoder()
        decoder.request((3, 9), "mds", 1, compound(put("aabb"), read(4, 4), read(8, 8)))
        decoder.request((3, 10), "mds", 2, compound(put("ccdd"), read(0, 2)))
        line = "8\t3\t100\t100\t0xa,0x9\t1,1\t2,3\t22,25,22,25,25\t0,0,0,0,0,0,0\t2,4,8"
        replies(line, decoder)
        self.assertEqual(
            [item.reply for item in decoder.reads], [(8, 0, 4), (8, 0, 8), (8, 0, 2)]
        )

    def test_retransmits_deduplicate_and_conflicting_xid_reuse_fails(self):
        decoder = Decoder()
        request = compound(put("aabb"), read(4, 12))
        decoder.request((1, 7), "mds", 1, request)
        request.set("pos", "123")
        decoder.request((1, 7), "mds", 2, request)
        response(decoder, 3, 1, 7, [22, 25], [0, 0], [12])
        response(decoder, 4, 1, 7, [22, 25], [0, 0], [12])
        self.assertEqual(
            (len(decoder.reads), decoder.duplicate_calls, decoder.duplicate_replies),
            (1, 1, 1),
        )
        with self.assertRaises(ValueError):
            decoder.request((1, 7), "mds", 5, compound(put("aabb"), read(5, 11)))

    def test_source_excludes_destination_readback_and_checks_saved_d(self):
        decoder = Decoder()
        decoder.request((1, 1), "mds", 1, compound(put("aabb"), read(4, 12)))
        decoder.request((2, 1), "mds", 2, compound(put("ccdd"), read(0, 16)))
        response(decoder, 3, 1, 1, [22, 25], [0, 0], [12])
        response(decoder, 4, 2, 1, [22, 25], [0, 0], [16])
        result = report(decoder, args())
        self.assertTrue(result["no_overlap_and_complete"])
        self.assertEqual(
            (result["read_operations"], result["requested_bytes"]), (1, 12)
        )
        self.assertEqual(
            report(decoder, args(all_reads_source=True))["overlap_saved_d"], [[0, 4]]
        )

    def test_sparse_ds_alias_maps_logical_offsets_but_dense_is_refused(self):
        decoder = Decoder()
        decoder.request((1, 1), "mds", 1, compound(put("aabb"), op(50)))
        layout = op(50, fh("eeff"), field("nfs.nfl_util.dense", 0), status=0)
        decoder.metadata_reply((1, 1), "mds", 2, compound(op(22, status=0), layout))
        decoder.request((2, 2), "ds", 3, compound(put("eeff"), read(4, 12)))
        response(decoder, 4, 2, 2, [22, 25], [0, 0], [12])
        self.assertTrue(report(decoder, args())["no_overlap_and_complete"])
        decoder.layouts[0]["dense"] = True
        self.assertFalse(report(decoder, args())["no_overlap_and_complete"])
        self.assertFalse(
            report(decoder, args(source_fh=["eeff"]))["no_overlap_and_complete"]
        )

    def test_unmapped_ds_and_unresolved_fh_fail_closed(self):
        decoder = Decoder()
        decoder.request((1, 1), "ds", 1, compound(put("aabb"), read(4, 12)))
        response(decoder, 2, 1, 1, [22, 25], [0, 0], [12])
        self.assertFalse(report(decoder, args())["no_overlap_and_complete"])
        decoder.request((2, 2), "mds", 3, compound(read(0, 4)))
        self.assertEqual(report(decoder, args())["unresolved_read_frames"], [3])

    def test_wire_completion_inversions_are_observable(self):
        decoder = Decoder()
        decoder.request((1, 1), "mds", 1, compound(put("aabb"), read(4, 4)))
        decoder.request((1, 2), "mds", 2, compound(put("aabb"), read(8, 8)))
        response(decoder, 3, 1, 2, [22, 25], [0, 0], [8])
        response(decoder, 4, 1, 1, [22, 25], [0, 0], [4])
        result = report(decoder, args())
        self.assertEqual(result["wire_completion_call_order_descents"], 1)
        self.assertEqual(
            result["wire_completion_inversion_examples"][0]["earlier_reply"]["offset"],
            8,
        )

    def test_short_failed_or_missing_replies_never_claim_completeness(self):
        decoder = Decoder()
        decoder.request((1, 1), "mds", 1, compound(put("aabb"), read(4, 12)))
        self.assertFalse(report(decoder, args())["no_overlap_and_complete"])
        response(decoder, 2, 1, 1, [22, 25], [0, 0], [11])
        self.assertFalse(report(decoder, args())["no_overlap_and_complete"])
        with self.assertRaises(ValueError):
            response(decoder, 3, 1, 1, [22, 25], [0, 0], [13])

    def test_malformed_trees_partitioning_and_limits_are_rejected(self):
        decoder = Decoder(maximum=1)
        with self.assertRaises(ValueError):
            decoder.request((1, 1), "mds", 1, compound(op(22), read(0, 1)))
        with self.assertRaises(ValueError):
            decoder.request((1, 1), "mds", 1, compound(put("aa"), read(2**64 - 1, 1)))
        decoder.request((1, 1), "mds", 1, compound(put("aa"), read(0, 1)))
        with self.assertRaises(ValueError):
            decoder.request((1, 2), "mds", 2, compound(put("aa"), read(1, 1)))
        with self.assertRaises(ValueError):
            replies("1\t1\t10\t10\t1,2\t1,1\t2\t22,25\t0,0,0\t1", decoder)
        self.assertEqual(handles(op(9, fh("aa"), fh("bb"))), ["aa", "bb"])

    def test_unions_intersections_and_capture_loss(self):
        self.assertEqual(union([[0, 2], [2, 4], [1, 3]]), [[0, 4]])
        self.assertEqual(intersection([[0, 4], [8, 10]], [[3, 9]]), [[3, 4], [8, 9]])
        decoder = Decoder()
        decoder.request((1, 1), "mds", 1, compound(put("aabb"), read(4, 12)))
        response(decoder, 2, 1, 1, [22, 25], [0, 0], [12])
        self.assertFalse(
            report(decoder, args(capture_drops=None))["no_overlap_and_complete"]
        )

    def test_basename_only_selection_is_refused(self):
        with self.assertRaisesRegex(ValueError, "basename-only"):
            report(Decoder(), args(source_fh=[], source_name="payload.bin"))


if __name__ == "__main__":
    unittest.main()
