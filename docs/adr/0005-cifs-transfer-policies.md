# ADR-0005: CIFS streaming and checkpoint policies

Status: implemented, 2026-09-16.

CIFS uses the domain facade from JayTsu-sh/smb-rs master, pinned to
`d8291b3a3157b026074ab6b0f305dfd99af17f1e`. The legacy StorageEnum adapter
continues to use its separate historical dependency until it is removed.

## Policy behavior

| Behavior | Checkpointed | AtomicReplace |
|---|---|---|
| Sibling stage and atomic replacement | Yes | Yes |
| Final data FLUSH | Yes | No explicit FLUSH |
| Recovery records | Lazily after an eligible checkpoint | None |
| Default checkpoint interval | 64 MiB | Not applicable |
| Direct final-path writes | Unsupported | Unsupported |

Automatic recovery requires multiple source chunks and a source larger than the
checkpoint interval. A file exactly equal to that interval needs no intermediate
record. Publication still closes the stage in both policies. SMB FLUSH requests
persistent backing-store writeback; it is not a separate POSIX directory fsync.
See [MS-SMB2 FLUSH](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/e494678b-b1fc-44a0-b86e-8195acf74ad7).

## Pipeline and checkpoint ordering

Source reads use a single open file, up to eight outstanding reads, constrained by
the request depth and runtime chunk, byte and operation budgets before payload
allocation. Read results are emitted in offset order. Each read uses only the
negotiated read limit. Destination writes use only the negotiated write limit,
slice Bytes without copying payload, and issue up to eight positioned writes.

At a checkpoint boundary, finish all previously issued writes, FLUSH the stage,
write and FLUSH the checkpoint record, close it, atomically replace the previous
record, then register recovery. No following writes cross this barrier. Failed
writes do not advance recovery; already issued writes are drained before closing.
Cancellation and stream drop also close the source handle.

## Artifacts and recovery

For `dir/report.bin`, all artifacts are siblings:

- `dir/.data-mover-<target-hash16>-<uuid32>.stage`
- `dir/.data-mover-<target-hash16>-<uuid32>.stage.checkpoint`
- `dir/.data-mover-<target-hash16>-<uuid32>.stage.claim-<claim32>` after recovery claim
- A unique temporary checkpoint file while replacing the record.

The engine recovery store retains the opaque recovery identity. A checkpoint
contains magic, recovery binding, checkpoint-path hash, durable prefix and
checksum. The binding includes source version observation through the engine.
Recovery claims the stage by rename, validates the record and file size, and
resumes at the recorded prefix. A larger EOF can contain holes from out-of-order
writes and is never evidence of durable progress. The suffix is overwritten.
The checkpoint path remains stable across claims. Completion or discard removes
only this stage's checkpoint; files below the threshold create no records.

## Metadata and limits

Existing domain metadata observation and ACL application remain in use. The
pinned domain facade does not provide the timestamp/Unix numeric ownership
setters needed for local-style metadata copying; this change does not advertise
those capabilities. CIFS does not have NFS FILE_SYNC/UNSTABLE write receipts;
FLUSH is its explicit durability barrier.

Tests cover bounded ordered prefetch, cancellation and closure, independent
write chunk splitting, a sparse post-checkpoint tail, recovery ownership, and
record-free AtomicReplace/below-threshold operation. Optional real-share tests
cover domain operations, recovery across two LIFs, and local/CIFS/CIFS/local
roundtrips with read-back verification.
