# Local checkpointed range-hash: real-process validation

## Result

**PASS, 2026-10-23, Linux Local → Local.** An actual client was killed with
`SIGKILL` after a production-sized checkpoint. A fresh process recovered saved
source hash H and durable destination coverage D, read exactly D's complement,
passed pinned destination read-back and a separate full-file BLAKE3 comparison,
and removed its recovery artifacts. Corrupt-pointer and changed-source attempts
both failed before payload reads while preserving the checkpoint/stage data.
The changed-source case also preserved an existing final file.

No production files were edited and no Cargo commands were started for this
validation. No additional production audit instrumentation or performance gate
was needed.

## Version and environment pin

- Git HEAD: `6aa1633e749c0f5255c82d628f966102bcc39e81`, **dirty integration tree**;
  HEAD alone does not identify the implementation tested.
- Source/tool fingerprint, independently reproduced after the run:
  `f58c9e08bc69bdf47a4007728f2c0eee6478deb019763a114234fe1b3decd697`.
- Existing coordinated GNU executable:
  `/work/cargo-target/debug/examples/transfer_resume`, copied before testing to
  the private evidence root as `transfer_resume.gnu`. Both still had SHA-256
  `bb7e314842bd894bfd154cc54010e551cac1a1cd0a7d4a4e20377d3cb3bf475a`
  after testing. ELF x86-64, GNU dynamic interpreter
  `/lib64/ld-linux-x86-64.so.2`, build ID
  `1e04399ad5a8ecfce8e07e41af63361f7fc6cfc0`; this was not the separate musl
  executable. Binary mtime was `2026-10-23 09:57:02.860593593 +0800`.
- Host: `ubuntu-24c`, Linux `6.8.0-142-generic`, x86-64; `/work` is **XFS**.
  The root filesystem was nearly full, so the fixture was placed on `/work`.
- Tools: `strace 6.8`, `rustc 1.98.1 (48a229cea 2026-09-01)`;
  `Cargo.lock` pins `blake3 1.8.5`.
- Small pointer copies, logs, hashes and audit helpers are retained under the
  unique mode-0700 root (large fixtures/client copy were later removed below):
  `/work/data-mover-local-hash-validation-20261023.QtLaCv`.

The fingerprint command supplied by the coordinator was:

```sh
git ls-files -c -o --exclude-standard -- src Cargo.toml Cargo.lock \
  examples/support/endpoint.rs tests/lab/tools/transfer_resume.rs |
  LC_ALL=C sort | xargs sha256sum | sha256sum
```

Its intermediate per-file manifest is retained as `source-files.sha256`.
`worktree.patch`, `worktree-status.txt`, and `untracked-sha256.txt` capture the
dirty-tree context; their capture is not a claim of a reproducible-build
attestation. The exact copied executable's hash is the runtime version pin.

## Method and primary sources

The production Local checkpoint interval is 256 MiB and deferred recovery
requires a strictly larger file; this run used **805,306,368 bytes (768 MiB)**.
See [Local checkpoint policy](../../src/storage/backends/local/staged.rs#L57)
and the [design contract](checkpointed-range-hash-recovery.md#6-hash-与完成范围分别记录).
The existing [transfer harness](../../tests/lab/tools/transfer_resume.rs) seeded
deterministic nonzero bytes once and reused the same source pathname/inode.

The historical sequence, each audit action bounded by `timeout`, was:

```sh
R=/work/data-mover-local-hash-validation-20261023.QtLaCv
timeout 135 python3 "$R/validate.py" kill
timeout 165 python3 "$R/validate.py" corrupt
timeout 165 python3 "$R/validate.py" resume
timeout 165 python3 "$R/validate.py" mutate
```

These are one-shot fixture actions, not an idempotent suite: **do not rerun
`kill` in the retained root**. `kill` launched the pinned executable with:

```sh
"$R/transfer_resume.gnu" \
  --source "$R/source" --source-path payload-768m.bin \
  --destination "$R/destination" --destination-path payload-768m.bin \
  --policy checkpointed --read-back on --seed-bytes 805306368 \
  --bandwidth 67108864
```

The launcher set umask `000`, polled the pointer until its saved prefix reached
256 MiB, sent `SIGKILL` to client PID **1401744**, and reaped exit `-9`.
No cooperative cancellation or retained live adapter was involved. Exact
post-kill artifact copies were saved in `saved/` before any negative test.
Transfer identity remained
`c748425ecdfbac7ed45fd3db71495ceb04ff7b0c3061b8c1cfefd503268dffb6`.

Resume used a separate client process, PID **1412374**, the same endpoints and
paths, no seed request, and `--bandwidth 536870912` to retain source accounting.
The audit launched it under:

```text
strace -ff -qq -yy -s 32
  -e trace=pread64,read,readv,preadv,preadv2,mmap,copy_file_range,sendfile,splice
  -o <root>/resume.trace <pinned-client> <same endpoints and paths> ...
```

The parser selects the **exact source-file path**, extracts successful actual
`pread64` offset/length intervals across every thread, rejects unexpected
source-file read syscall forms, and checks both interval union and total bytes.
It therefore detects overlap/duplicate reads rather than relying only on a
reported reused-byte count. Local's source implementation uses positional
[`read_at`](../../src/storage/backends/local/source.rs#L601).
The separate `--compare` acceptance read is not included in these transfer
read counts.

A small standalone, non-production Rust helper used the already-built BLAKE3
library to validate all three saved checksums: outer pointer, checkpoint
envelope, and domain-separated RangeHash checksum. A Python decoder inspected
H/D using the actual [pointer](../../src/storage/pointer.rs),
[checkpoint](../../src/storage/checkpoint.rs), and
[RangeHash](../../src/storage/range_hash/codec.rs) layouts. This helper required
only a bounded `rustc` invocation, not Cargo or a production instrumentation
patch. The client separately decoded and validated the checkpoint on resume.

## Positive evidence

Artifact basename: `.data-mover-a6c63bf43310701940538d8ddc00907d`.

| Observation | Actual value |
|---|---|
| Saved pointer / checkpoint magic | `DMDPTR02` / `DMHCP002` |
| Pointer / checkpoint bytes | 350 / 222 |
| Saved phase / generation | Receiving / 416 |
| Saved source H | `[0, 268435456)`, one compact CV node, zero raw fragments |
| Saved durable D / prefix | `[0, 268435456)` / 268,435,456 |
| All saved checksums | Valid |
| Stage file length at kill | 269,106,545 |
| Pointer permissions with umask `000` | `0600` |
| Fresh-process prepare | `Resumed { bytes: 268435456 }` |
| Transfer source READ calls / bytes | 256 / 536,870,912 |
| Actual source READ union | `[268435456, 805306368)` |
| Actual source READ intersection with saved D | Empty |
| Reused bytes | 268,435,456 |
| Route / recovery | Streaming / Checkpointed |
| Read-back / method | Enabled / Pinned |
| Transfer result / elapsed | Success / 3,140 ms (not a performance gate) |
| Immediate successful-publication directory | Only `payload-768m.bin`; no `.data-mover-*` artifacts |

The extra **671,089 bytes beyond D** were not adopted merely because the file
was longer: they fall inside the observed resumed source READ complement.
This directly checks the uncommitted-tail case. The retained stage inode
`1611719891` became the final file's inode, consistent with publication by
rename. See [Local reopen](../../src/storage/backends/local/staged/at_destination.rs#L582),
[D/H restoration](../../src/transfer/engine/checkpointed/state.rs#L19), and
[checkpointed read scheduling](../../src/transfer/engine/checkpointed/producer.rs#L109).

The separate full-file comparison then reported:

```json
{
  "compared_bytes": 805306368,
  "content_equal": true,
  "source_blake3": "44a51ef46f9d785140a29c2365d332a7ddd0819748a1ca4c6314aee13ef46f5e",
  "destination_blake3": "44a51ef46f9d785140a29c2365d332a7ddd0819748a1ca4c6314aee13ef46f5e"
}
```

Final-file SHA-256 was
`16c3d975cd6866f71c7816d2af7018dc3a552ec3d8fbf5e416eae342af5ef568`.
Saved original pointer SHA-256 was
`1c8c004167d79b3059774335e33c8620621df20de771b32f304e1bd33263f81b`;
saved stage SHA-256 was
`2382be3af8b80b66ff9cb3dea9811b4d68c762bc1d58d5af09370ffe4dcf2254`.

## Fail-closed evidence

**Corrupt pointer, before positive resume.** Flipped one bit in the last byte
of the saved pointer's outer checksum, leaving magic, length, H/D, source and
stage unchanged. Prepare failed as `Unsupported, Permanent`, with diagnostic
`checkpoint hash state is corrupt, unsupported, or incompatible with this recovery interface`.
Both QoS and syscall audit recorded zero source reads. Pointer and stage
content hashes, sizes, modes and inodes were unchanged by the failed attempt;
no final appeared. Restoring the exact saved pointer bytes allowed the positive
resume above. Corrupt pointer SHA-256:
`276a1de25e32f78abeb1409b48815024fbd350b4bef5c564cf9c7a6083de4947`.

**Changed source, after positive publication.** Restored exact saved
pointer/stage/claim byte fixtures to their original destination names, keeping
the already-published final in place. Revalidated the restored pointer and all
checksums. Changed source byte 7 from `bf` to `be` in place and synced it;
source inode `1347446300` and size 805,306,368 stayed the same, while mtime changed
from `1792720782492944246` to `1792720976023320892` ns. The same transfer identity
and paths were used. Prepare again failed as `Unsupported, Permanent`, before
any source payload read, with `final_destination_changed=false`.

The original pointer and stage bytes and the existing final's SHA-256,
inode, mode and size were all preserved. This tests version/binding rejection,
not an attempt to defeat filesystem timestamp/version detection. Their
preservation was recorded before the later authorized fixture cleanup below.

On these failures Local removes its own ephemeral claim; it does **not** delete
the pointer/stage recovery data. The first audit assertion incorrectly included
claim preservation, and was corrected to distinguish the documented
[prepare-abandon claim release](../../src/storage/backends/local/staged/at_destination.rs#L563)
from deletion of recovery roots. The repeated corrupt-pointer attempt passed
that corrected assertion. `recoverable_stage=false` in the failure JSON means
no prepared-stage handle was returned, not that the disk artifacts vanished.

## Retained evidence and scope limits

Under the evidence root, `*-result.json`, `*-reads.json`,
`*-preservation.json`, `killed-checkpoint.json`, `killed-manifest.json`,
`published-manifest.json`, `*.stdout`, `*.stderr`, and per-thread `*.trace.*`
hold the observations. `validate.py` and `audit_blake3.rs` retain the exact
audit logic. `evidence-files.sha256` pins the individual logs/helpers/binary.
Compact log/source-context archive:

```text
evidence-logs.tar.gz
SHA-256 3c4bbcf391e2ed3fb59f0a0ec67305ac154064350838f5185d525c16b22285d4
```

### Authorized fixture cleanup, after evidence capture

All positive/negative manifests, hashes and syscall logs above describe the
fixtures **before deletion**. Successful publication's no-artifacts assertion
was recorded before deliberately reinstating the saved fixtures for the
source-mutation negative case; that case's preservation assertions were then
recorded before this cleanup. Cleanup does not change either observation.

At the user's request, cleanup completed at `2026-10-23T10:09:27+08:00`.
Before unlinking, each target was checked to be a regular nonsymlink file whose
canonical absolute path matched exactly, and its SHA-256 was verified against
`cleanup-targets.sha256`. Only these five explicit absolute files were removed:

```text
/work/data-mover-local-hash-validation-20261023.QtLaCv/source/payload-768m.bin
/work/data-mover-local-hash-validation-20261023.QtLaCv/destination/payload-768m.bin
/work/data-mover-local-hash-validation-20261023.QtLaCv/destination/.data-mover-a6c63bf43310701940538d8ddc00907d.stage
/work/data-mover-local-hash-validation-20261023.QtLaCv/saved/.data-mover-a6c63bf43310701940538d8ddc00907d.stage
/work/data-mover-local-hash-validation-20261023.QtLaCv/transfer_resume.gnu
```

Removed logical sizes: 805,306,368 bytes each for source/final payloads;
269,106,545 bytes each for stage/saved-stage; 149,633,960 bytes for the copied
GNU client, totaling **2,298,459,786 bytes (about 2.14 GiB)**. The mutated source's
pre-deletion SHA-256 was
`1e4805738b1dbb432c55eebcc16060d4a87d2a432236d372fb08b89aef13bf52`;
the other removed files' hashes were unchanged from those reported above.

The root remains, now **5.9 MiB**, with both original 350-byte pointer copies,
all small logs/manifests/hashes, unchanged `evidence-logs.tar.gz`, and helpers
`audit_blake3`, `audit_blake3.rs`, and `validate.py`. Both pointer copies still
hash to `1c8c004167d79b3059774335e33c8620621df20de771b32f304e1bd33263f81b`.
`cleanup-before.json`, `cleanup-targets.sha256`, and `cleanup-after.json` record
the cleanup boundary. The historical archive/`evidence-files.sha256` were not
rewritten; the latter's copied-client entry is now intentionally absent from
disk. Remaining historical entries can be checked with
`sha256sum --ignore-missing --quiet -c evidence-files.sha256`.

No directories, shared Cargo executable, repository production files or other
agent's artifacts were removed; no recursive deletion was used. The deleted
payloads/stages/client copy are **not recoverable from the retained archive**,
which retains their evidence and hashes rather than their large contents.

This is a real process-kill test, not host power-loss or filesystem-crash
certification. It covers a Receiving-phase contiguous D checkpoint, not sparse
islands, a full-H/empty-D intent, rename-gap/Published recovery, raw-fragment
payload privacy, Windows inherited ACLs, or a long-running bounded-memory
gate. The `0600` observation applies to this v2 pointer even though its saved H
contained no raw fragments. Atomic/Direct/ineligible small-file isolation and
other backends were not retested here. No claim beyond these measured Local
hash/recovery semantics is implied.
