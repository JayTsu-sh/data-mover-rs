# Local copy architecture performance regression investigation

- Date: 2026-09-01
- Scope: legacy and role-based Local-to-Local public copy paths
- Filesystems: `/work` XFS and `/tmp` ext4
- Payloads: 4 KiB, 40 MiB, and 1 GiB
- Result: no bulk regression against a baseline with equivalent data durability

## Initial symptom

The original benchmark compared raw legacy copy with the role-based staged lifecycle. Five-run
medians initially reported the role-based path as 30–53% slower for 40 MiB and 1 GiB payloads.
All copies had matching SHA-256 digests, so the signal was latency rather than corruption.

The comparison mixed different completion guarantees:

- raw legacy Local copy uses `CommitPolicy::None`; after content read-back succeeds it returns
  without a file durability barrier;
- the role-based Local destination does not report a durable prefix until file data is synced and
  its checkpoint is atomically persisted and re-observed; it then verifies and atomically publishes
  the staged file.

The benchmark also left raw legacy dirty pages outstanding between samples. A following Optimized
sample could therefore compete with writeback debt created outside the preceding Legacy timing.

## Phase isolation

A temporary phase-timing probe was placed only at prepare, write stream, data sync, checkpoint,
verify, and publish boundaries and removed after diagnosis. Representative XFS times were:

| Payload | Legacy pipeline | Legacy verify | Optimized stream | Optimized sync | Optimized verify |
|---|---:|---:|---:|---:|---:|
| 40 MiB | 44–45 ms | 35–36 ms | 37–38 ms | 26–29 ms | 36–37 ms |
| 1 GiB | 925 ms | 861 ms | 952 ms | 395 ms | 887 ms |

The role-based stream and verification paths were not slower in a repeatable way. The apparent
bulk regression was the explicit durability barrier missing from the raw legacy path. Adding one
timed `fdatasync` to Legacy produced the same completion class and removed that difference.

A speculative background writeback experiment improved one 40 MiB sample but did not improve the
1 GiB result, so it was removed rather than adding size-specific I/O scheduling complexity.

## Fix

The production Local staged file now uses `sync_data` after `set_len`, which persists contents and
the file-length metadata needed to retrieve them without synchronizing unrelated inode metadata.
The final sync still completes before the checkpoint is persisted; durable-prefix semantics were
not weakened.

The comparison runner now records three paths:

1. `legacy`: historical raw latency, explicitly not durable;
2. `legacy-durable`: the same public path plus its missing timed `sync_data` barrier;
3. `optimized`: the role-based staged lifecycle.

Each sample is SHA-256 checked and flushed outside its timed interval before the next sample. Run
order rotates across all three implementations. Bulk regression is evaluated against
`legacy-durable`; 4 KiB uses an absolute fixed-overhead budget because stage/checkpoint/publication
setup is constant work that makes a percentage misleading.

## Final five-run medians

Configuration: 2 MiB negotiated Local read chunks, read inflight 8, write inflight 8, production
content verification enabled. Bulk threshold: 10%. Small-file fixed-overhead threshold: 5 ms.

| Filesystem | Payload | Legacy raw | Legacy durable | Optimized | Optimized vs durable |
|---|---:|---:|---:|---:|---:|
| XFS | 4 KiB | 2.314 ms | 2.348 ms | 5.779 ms | +3.432 ms |
| XFS | 40 MiB | 79.478 ms | 100.149 ms | 99.759 ms | -0.4% |
| XFS | 1 GiB | 1.804 s | 2.672 s | 2.656 s | -0.6% |
| ext4 | 4 KiB | 1.865 ms | 2.021 ms | 6.302 ms | +4.280 ms |
| ext4 | 40 MiB | 81.140 ms | 104.310 ms | 102.779 ms | -1.5% |
| ext4 | 1 GiB | 1.788 s | 2.636 s | 2.743 s | +4.1% |

## Single-chunk follow-up

The original 4 KiB optimized numbers still included the complete recoverable-stage lifecycle.
Single-chunk transfers now use an ephemeral stage: they keep `sync_data`, BLAKE3 verification,
and atomic publication, but do not persist a zero-byte checkpoint, a completed checkpoint, or a
recovery claim. The boundary is the smaller of the caller's inflight-derived chunk ceiling and
the connected source backend's maximum read chunk, so it follows Local/NFS/CIFS/S3/HDFS
negotiation instead of hard-coding 4 KiB.

Ten-run medians after this change:

| Filesystem | Legacy durable | Optimized ephemeral | Optimized overhead |
|---|---:|---:|---:|
| XFS | 2.449 ms | 2.396 ms | -0.053 ms (-2.2%) |
| ext4 | 2.218 ms | 2.547 ms | +0.329 ms (+14.8%) |

The remaining sub-millisecond difference includes the role-based stream, BLAKE3 verification,
and atomic publication. Raw samples are in `/tmp/local-copy-ephemeral-4k-xfs.csv` and
`/tmp/local-copy-ephemeral-4k-ext4.csv`.

Raw result artifacts from this run are `/tmp/local-copy-final-xfs.csv` and
`/tmp/local-copy-final-ext4.csv`.
