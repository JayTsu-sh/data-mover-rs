# Checkpointed range-hash: real-machine completeness checklist

Status: independent source review, 2026-10-23. This note defines acceptance evidence; it does
not certify the concurrent CIFS/HDFS/S3/pNFS runs. The final section records this sidecar's
own NFSv3/v4.0 runs. No production instrumentation is proposed.
Authority: [final design §§1.1, 6–11, 14](checkpointed-range-hash-recovery.md).

## Evidence already available, and its limits

- **Recorded real remote run:** design §14.2 records production NFS v4.1/pNFS cancel and
  SIGKILL recovery of `384 MiB + 333`, with a 128 MiB committed prefix, complement-sized
  source reads, final verification and cleanup. This review did not independently inspect
  those remote logs. It is not evidence for sparse D, changed chunk sizes, a commit gap,
  another backend, or power-loss durability. [Recorded run and boundaries](checkpointed-range-hash-recovery.md#142-17-上的生产路径验证).
- **Real Local filesystem / fresh process:** `a_fresh_process_resumes_complete_durable_coverage_without_source_reads`
  starts a subprocess, exits deliberately without Drop at complete D/Receiving, decodes the v2
  root, rejects source requests overlapping D, then verifies publication and cleanup. This
  covers complete D, not partial/sparse D or interruption after rename/Complete. The source
  wrapper observes role requests, not remote protocol operations. [Test](../../src/transfer/checkpoint_policy_tests/fresh_process.rs).
- **In-memory / mathematical evidence:** engine mock tests exercise H-only versus D and missing
  ranges; RangeHash tests cover nonaligned cuts and changed geometry. NFS/CIFS fake protocols,
  MemoryHdfs and MemoryS3 cover backend failure branches. These are useful regression guards,
  not certifications of server barriers, leases, wire ordering or permissions.
  [Engine](../../src/transfer/engine/checkpointed/tests/producer.rs),
  [hash](../../src/storage/range_hash/tests.rs),
  [NFS](../../src/storage/backends/nfs/hash_checkpoint_tests.rs),
  [CIFS](../../src/storage/backends/cifs/hash_checkpoint_tests.rs),
  [HDFS](../../src/storage/backends/hdfs/hash_checkpoint_tests.rs),
  [S3](../../src/storage/backends/s3/staged/hash_checkpoint_tests.rs).

## Required real tests

For each applicable row, retain a first-attempt root and a fresh-process resume report.
Let `L` be source length, `H` validated persisted source evidence, and `D*` the destination's
validated/re-observed reusable coverage, including any safely adopted tail/parts. Audit against
`R = D* ∩ H`, not file length, write acknowledgements, or just `write_offset`. An adopted
range lacking H must fail closed, not become R. [Design §§5–7](checkpointed-range-hash-recovery.md).

| Invariant | Real acceptance case / required evidence | Existing regression source; not a remote pass |
| --- | --- | --- |
| H and D are independent | On HDFS/S3 cut after intent persistence but before corresponding payload; resume must still supply missing payload. Separately cut after recoverable data but before the next root update: lease tail/ListParts may advance D only within anchored H. Uncovered tails/parts fail without fabricating a source digest. | [HDFS intent/tail](../../src/storage/backends/hdfs/hash_checkpoint_tests.rs), [S3 dispatch/receipt](../../src/storage/backends/s3/staged/hash_checkpoint_tests.rs), [engine H-only](../../src/transfer/engine/checkpointed/tests/producer.rs). |
| Sparse OOO is real, and survives | On positioned destinations obtain at least two persisted D islands separated by a hole; prove actual completion disorder and snapshot the committed ranges. Restart and require every source READ to avoid both islands, while the hole is filled and old islands survive. For HDFS require only a continuous append prefix and bounded reordering, not sparse destination recovery. Do not infer D from which WRITE callback ran first. | [NFS sparse](../../src/storage/backends/nfs/hash_checkpoint_tests.rs), [CIFS sparse/FLUSH](../../src/storage/backends/cifs/hash_checkpoint_tests.rs), [S3 frozen sparse parts](../../src/storage/backends/s3/staged/hash_checkpoint_tests.rs). |
| Coordinates survive chunk changes | First process and resume use different source/destination ceilings; cover 32,771 / 49,157 / 65,536 bytes plus available 1,044,480 / 2 MiB / 8 MiB limits, odd EOF and a non-1024-aligned cut where the backend permits it. Record actual negotiated request sizes, not flags alone. S3 resumes the old upload geometry, including native↔streamed recovery and changed fresh single-copy thresholds. | [5 MiB + 17 math test](../../src/storage/range_hash/tests.rs), [restored eligibility](../../src/transfer/engine/checkpointed/tests/entry.rs), [S3 threshold/geometry](../../src/storage/backends/s3/staged/hash_checkpoint_tests.rs). |
| Barriers, not apparent length | NFS: WRITE/COMMIT and verifier evidence; CIFS: FLUSH; HDFS: hsync/close plus lease recovery; S3: complete part receipts/ListParts. Exercise failed barriers and before/after pointer commit cuts. Recovery may reuse only the certified ranges. A client SIGKILL tests process failure, not server power failure. | [Backend contract and limits](checkpointed-range-hash-recovery.md#4-backend-适配决策), [settling tests](../../src/transfer/engine/checkpointed/tests/settling.rs). |
| Commit gap preserves the source root | Cut after Ready but before publication, after rename/Complete before the Published record, and during final readback. Fresh process must verify the final using retained source H, even with readback disabled; no source payload reads, second rename/copy, or final metadata replay. Wrong/replaced finals must be refused or fail verification while retaining cleanup authority. Record backend identity-pin limitations and the single-writer assumption. | [Lifecycle](../../src/transfer/engine/publication.rs), [Local publication gap](../../src/storage/backends/local/staged/published_recovery_tests.rs), [HDFS gap](../../src/storage/backends/hdfs/hash_checkpoint_tests.rs), [S3 lost reply](../../src/storage/backends/s3/staged/hash_checkpoint_tests.rs). |
| Source and metadata bindings fail closed | Same-size content edit, source version/identity change, corrupt/unknown root: preserve incompatible v2 evidence and refuse, unless an explicit controlled restart is requested. Change S3 tags without changing source content identity; change ACL/xattr selectors and actual planned values. Recovered finals reject mismatched/missing metadata seals before acceptance; a surviving stage may return to Receiving and apply a new plan before Ready. | [Source-edit transfer test](../../src/transfer/tests.rs), [selector/actual-tag seal tests](../../src/transfer/checkpoint_policy_tests/metadata_seal.rs), [typed fingerprint](../../src/metadata/checkpoint_binding.rs). |
| Modes remain isolated | Fresh AtomicReplace, Direct, Checkpointed below/equal interval, one source chunk, native single-copy, nonresumable/NAS, and expert ordered transport retain prior routing and no added hash-state metadata I/O. Compare operation traces against the ordered baseline; absence of leftover files alone is insufficient. Existing eligible v2 roots remain discoverable when fresh eligibility settings change. | [Mode tests](../../src/transfer/checkpoint_policy_tests.rs), [S3 stage eligibility](../../src/storage/backends/s3/staged/hash_checkpoint_tests.rs), [design §1.1](checkpointed-range-hash-recovery.md#11-模式隔离实施硬约束). |
| Cleanup is owned, private, retryable | After verified success: no owned pointer/stage, open MPU, or old recoverable hash versions/markers. On failed final verification or release: retained root and typed committed cleanup, never deletion of final data. Object Lock/permission refusal is pending cleanup, not success. Stale owner cleanup cannot erase a newer root. Inspect private permissions/DACL on records containing partial source bytes. | [S3 cleanup](../../src/storage/backends/s3/staged/checkpoint_cleanup_tests.rs), [Local fencing](../../src/storage/backends/local/staged/cleanup_fencing_tests.rs), [CIFS privacy](../../src/storage/backends/cifs/privacy_checkpoint_tests.rs), [design §8](checkpointed-range-hash-recovery.md#8-最终比较发布与清理). |

Backend applicability matters: NFS versions and actual pNFS DS paths are separate records;
HDFS replicated/HA/EC limitations and S3 object/NAS, compatibility profiles, versioned versus
unversioned buckets cannot be generalized from a single endpoint. Preserve existing RSS/growth,
bandwidth/IOPS and request-cost gates; a short QoS-limited resume is not their replacement.
[Validation matrix and bounds](checkpointed-range-hash-recovery.md#112-实施前后的验证矩阵),
[approved environments](../../.claude/docs/test-environments.md).

## What the current lab tool can prove

`transfer_resume` reports `prepare`, `reused_bytes`, `source_streamed_bytes`,
`source_read_operations`, native counters and verification method. Source counters require
`--bandwidth`; without a budget, zero is **not evidence of zero reads**. Use a sufficiently
high nonlimiting budget for the measured resume, while noting the configured maximum charged
read size is 8 MiB. Counts describe budgeted client reads, not raw wire packets or server disk I/O.
[Request/report](../../tests/lab/tools/transfer_resume.rs#L194),
[counter implementation](../../src/runtime/qos.rs#L225).

For a successful streamed resume without retries, `source_streamed_bytes = L - |R|` is a useful
aggregate check against an extra full-source hash scan. It is not a proof that each request
avoids R: aggregate totals contain no offsets and can hide compensating overlap/omission.
Complete R plus zero source read operations is stronger, provided the exercised adapter charges
its reads and the transfer verifies the final. Native source hashing instead fills missing H;
its client-read total is not generally `L - reused_bytes`, and server-side copy traffic is
accounted separately. [H-only tests](../../src/transfer/engine/checkpointed/tests/producer.rs),
[native digest/copy lifecycle](../../src/transfer/engine/native.rs).

`--compare` invokes a separate content comparison that reads the source and destination.
Run it outside the resume audit/capture interval and label those reads as independent validation.
The existing resume matrix prints many outcomes but is not an assertion-based hash-completeness
gate: it explicitly makes fresh-HOME cleanliness its enforced criterion.
[Compare](../../tests/lab/tools/transfer_resume.rs#L275),
[matrix](../../.claude/skills/_shared/resume_matrix.sh).

The test tool now accepts `--source-block-size <u64>` and
`--destination-block-size <u64>` for transfer and independent comparison. Omitting them retains
the old connector defaults and `ensure_dir=true`; parent-directory setup is unchanged.
The connector passes ceilings into NFS/S3, maps Local block size to read chunks, and currently
does not pass them through to CIFS/HDFS. Therefore flags are not universal write-size controls,
nor proof of a renegotiated maximum. Use supported connection options, not unrecognized NFS
`rsize`/`wsize` URL parameters. Rebuild the tool and identify the actual binary used.
[CLI](../../tests/lab/tools/transfer_resume.rs#L59),
[connector](../../examples/support/endpoint.rs#L110).

## Minimal test-only range audit, without production tracing

Preferred for the planned `.17` NFS run: an **offline capture checker**, not a library hook.
Input: bounded decoded v2 checkpoint/validated recovery ranges, capture, resume start/end,
and an explicit source-file identity map. Output: request and returned-byte interval unions,
overlap count/bytes with R, missing complement, retransmission/retry counts, capture-drop and
decode-completeness status, and observed READ size/completion order. Any unknown identity,
truncated capture, incomplete reassembly or undecodable relevant operation makes the result
inconclusive, not a zero-overlap pass.

Filter READs by the **source filehandle**, not TCP/2049 or host alone: destination readback
and checkpoint reads may share the server and port. Correlate RPC requests/replies and preserve
all compound READ operations; do not confuse TCP retransmits with new logical READs. For pNFS,
map DS handles/offsets back to source logical coordinates using the observed layout; no claim
of logical nonoverlap is valid from unconverted dense-layout DS offsets. Capture before the
fresh connection so the filehandle/layout mapping is available. Require both no READ request
intersection with R and successful final verification; keep post-run comparison separate.
[Logical-coordinate requirement and pNFS limits](checkpointed-range-hash-recovery.md#5-统一坐标不统一-chunk-size),
[existing role-request guard](../../src/transfer/checkpoint_policy_tests/fresh_process.rs#L39).

If encryption/another protocol prevents passive decoding, the fallback is an **in-crate
`#[cfg(test)]` real-backend test** with a delegating `ReadSource` overlap guard, modeled on the
fresh-process guard. It must preserve source identity, version handling, QoS/admission,
metadata roles and native pairing; otherwise it tests another route. `Storage::connected` is
crate-private, so the external lab binary cannot simply install that wrapper through today's
public interface. Do not widen production interfaces merely for this audit. This fallback
proves role-level request exclusion, not independent wire-level behavior.
[Storage construction](../../src/storage/handle.rs#L67),
[guard delegation](../../src/transfer/checkpoint_policy_tests/fresh_process.rs#L77).

## Evidence handoff / pass rule

- Record revision plus dirty-tree/binary SHA-256, tool flags, endpoint/profile, negotiated
  limits, source descriptor/version, checkpoint generation/H/D/phase, interruption point,
  first/resume exit status, JSON counters, trace audit and full final digest.
- A kill before the first committed root is a valid negative case, not a successful resume.
  A timing-only kill that never reaches the intended phase does not cover that phase.
- Use unique approved roots, lab locks, timeouts and cleanup verification. Restrict raw roots
  and packet artifacts: they can contain source fragments, tags/principals and authentication
  material. Publish redacted summaries and evidence hashes, not secrets.
- Mark each row **real-pass**, **mock/local-only**, **blocked**, or **not-applicable with reason**.
  A backend is complete only for its demonstrated rows/profiles; CI or another backend's pass
  cannot fill gaps. Existing real-run results remain attached to their tested binary, not to
  subsequent unrelated edits. [Design evidence discipline](checkpointed-range-hash-recovery.md#14-本轮实施与验证).

## This sidecar's real ONTAP NFSv3 / v4.0 results

Executed 2026-10-23 on `ubuntu-24c`, root, `10.131.9.10`, against the approved
`10.128.61.200:/ontap_lisaauto_nfs` scratch area. NFSv3 used
`version=3&noresvport=false`; v4.0 used `version=4.0&noresvport=true`, both uid/gid 0.
Tests were serial, bounded by timeouts, with both shared lab flocks. DXN2 was not contacted.
No build, service/network configuration change, or mount change was performed here.

Binary: copied from the coordinated GNU `transfer_resume` build, SHA-256
`bb7e314842bd894bfd154cc54010e551cac1a1cd0a7d4a4e20377d3cb3bf475a`.
Base HEAD was `6aa1633e749c0f5255c82d628f966102bcc39e81` with uncommitted implementation;
the binary hash, not that base commit alone, identifies the tested implementation.
Evidence directory: `/tmp/data-mover-range-hash-nfs34-20261023-1bW2aH/` (0700), containing
binary, scripts, JSON, stderr and private checkpoint copies (0600). These machine-local
artifacts are retained, not committed as raw source-fragment evidence.

Each source was deterministic, nonzero, `201,326,925 = 192 MiB + 333` bytes. The first
process used source/destination ceilings `32,771 / 65,536`, source bandwidth 16 MiB/s and
cancellation at 7,500 ms; the second used `49,157 / 98,304`, a nonlimiting 1 GiB/s budget
and readback **enabled**. These are configured ceilings; actual negotiated NFS WRITE sizes
were not captured. Local source counters agree with the changed source read granularity.

| Case | First attempt source bytes / operations | Saved v2 D | Visible stage bytes | Fresh resume source bytes / operations | Final result |
| --- | ---: | --- | ---: | ---: | --- |
| NFSv3 | 71,309,696 / 2,176 | `[0,67,115,008)` | 71,309,696 | 134,211,917 / 2,731 | Resumed; pinned readback; independent BLAKE3 equal; no artifacts |
| NFSv4.0 | 71,965,116 / 2,196 | `[0,67,115,008)` | 71,965,116 | 134,211,917 / 2,731 | Resumed; pinned readback; independent BLAKE3 equal; no artifacts |

Both first processes exited 1 with a typed Transfer/Source cancellation, a recoverable stage,
and unchanged final destination. Both new processes exited 0, retained their original
transfer identities, and reported `reused_bytes=67,115,008`; source bytes exactly equal
`201,326,925 - 67,115,008`. The v3/v4.0 saved roots had generation 2,052/2,048,
length 515/450 bytes, Receiving phase and a metadata seal. A bounded structural inspector
recorded those fields; the production resume, not that inspector, validated checksums/H.

Independent comparisons, outside the resume operation, read both files and returned:

- v3: `c6b9ce3b42498ae12eb22ddd6df86ebf71b269c7672110daf14b17a613dd57a7`.
- v4.0: `8fab92a7477b64edbc3bd8c7ebb1b210435651a6683c1d2415ac3b28edcd2143`.

Cleanup: per-case artifact listings were empty. The lab tool's final `--remove-run` refused
the `data-mover-*` root because its guard only accepts `resume-*`; the script therefore
exited 1 after all transfer assertions passed. Follow-up under both locks removed exactly
our two final files via the pre-existing kernel mount, then removed the empty directory.
Independent userland NFSv3 lookup returned `NFS3ERR_NOENT`, and the kernel path was absent.
Local seeded payloads were also removed. Details remain in `cleanup-resolution.txt`.

Classification: **real-pass** for ordinary eligible cancel/fresh-process resume with changed
source chunk ceilings, aggregate missing-byte accounting, final verification and cleanup on
these two dialects. **Still unproven by these runs:** per-request source offset exclusion
(source was Local, no offset trace), sparse D, non-1024-aligned D cut, SIGKILL/power failure,
barrier fault injection, commit gaps, binding/metadata mutation, and mode-isolation request
counts. The existing pNFS wire audit and other backend runs remain separately owned.
