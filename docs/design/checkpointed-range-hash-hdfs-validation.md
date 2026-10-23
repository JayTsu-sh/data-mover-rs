# Real HDFS checkpointed hash validation

## Result

2026-10-23: **PASS** for ordinary eligible Local → Kerberos HDFS Checkpointed
cancel/fresh-process resume, SIGKILL/fresh-process resume, D=0 hash-intent
recovery, and adoption of a lease-recovered tail beyond the saved durable prefix.
All four resumed transfers passed pinned read-back verification and a separate
complete source/destination BLAKE3 comparison. Source syscall traces prove that
the resumed processes did not reread any recovered/completed source byte.

This is process-interruption evidence, not a power-loss, HA/failover, performance,
or complete policy-isolation certification. No production files were changed.

## Candidate and authorized environment

- Repository HEAD: `6aa1633e749c0f5255c82d628f966102bcc39e81`, with the existing
  uncommitted integration changes, not the clean commit alone.
- Build: `timeout 1200s env CARGO_TARGET_DIR=/work/cargo-target cargo build
  --locked --offline --example transfer_resume --example storage_inspect`.
  One build, completed in 7.55 s; no further Cargo invocation. The actual runner
  used the GNU dev-profile `transfer_resume`, not the separately supplied musl
  release binary. Timings below are diagnostic, not benchmarks.
- Runner probe SHA-256, after stripping its copied binary:
  `ca634c450958f269d95ffead76f37f2e88f2e52dc1f8db76efee31a377f76533`.
- Read-only HDFS inspector SHA-256:
  `3d06a86213f7b4bc728b706b39ff239be3b6fafbac457e24582fcc0346214fa5`.
  This temporary standalone Rust helper linked the current library and pinned
  dependencies; it listed native HDFS facts and decoded pointer/H envelopes.
  Its only mutation was exact-run-root cleanup.
- SHA-256 of the sorted `sha256sum` manifest of `rg --files src examples
  tests/lab/tools`, observed after the build:
  `8b47a721dd25b1d065b1b98c25a32f4427261da978494ea504c8a30521a352b0`.
  SHA-256 of `git diff --binary -- src examples tests/lab/tools Cargo.toml
  Cargo.lock`: `5aa488ecab10a4f13f62d4564c728e4af6ff53bb47a29469c747ea87434c0227`.
  These identify the observed dirty tree; the binary digest identifies the
  executable actually tested. No concurrent changes were reverted.
- Runner: VM102, x86_64, Ubuntu glibc 2.39. Access used the existing authorized
  PVE SSH key and `qm guest exec 102`, then the `github-runner` account. No
  passwords, key contents, keytab contents, or XML configuration were printed.
- HDFS: direct endpoint `10.131.9.30:9000`, existing Kerberos principal
  `hdfs/terrasync-runner@HDFS.LOCAL`, existing client configuration/keytab.
  The keytab was observed mode 0600 and readable by `github-runner`.
- Unique run: `nightly-hash-hdfs-20261023T0159-byY8OO`.
  All generated runner files were under
  `/tmp/data-mover-nightly/nightly-hash-hdfs-20261023T0159-byY8OO/`;
  all HDFS fixtures were under its `hdfs/` child. Existing shared lock files
  were the only shared files opened for synchronization.
- Both `/tmp/terrasync-lab-tests.lock` and
  `/tmp/terrasync-lab-performance.lock` were held exclusively, nonblocking, by
  each complete test suite including its HDFS cleanup. Both were subsequently
  confirmed free as `github-runner`; no lock permissions were changed.
- Binaries were transferred in bounded guest-agent stdin chunks. No S3 bucket,
  HTTP listener, service, network setting, Hadoop configuration, failover state,
  or global test service was changed. Only exact owned child PIDs received
  SIGKILL. Commands had outer timeouts.

Authorization and routing follow [test environments](../../.claude/docs/test-environments.md)
and the [lab README](../../tests/lab/README.md). The underlying ordered-append,
hsync, and forced-lease-recovery contract is described in
[ADR-0004](../adr/0004-hdfs-transfer-policies.md).

## Method

Each fixture was a fully written deterministic `268435593`-byte file
(`256 MiB + 137`), generated once by the existing
[transfer_resume example](../../tests/lab/tools/transfer_resume.rs). Relative
paths were `cancel.bin`, `kill.bin`, `intent.bin`, and `tail.bin`; source and
destination roots and transfer identities remained identical across attempts.
Every resume was a new executable process, with new source/destination handles.

With `RUN=/tmp/data-mover-nightly/nightly-hash-hdfs-20261023T0159-byY8OO` and
`DEST=hdfs://hdfs%2Fterrasync-runner%40HDFS.LOCAL@10.131.9.30:9000$RUN/hdfs`,
the logical command shape was:

```sh
transfer_resume --source "$RUN/source" --source-path "$CASE.bin" \
  --seed-bytes 268435593 --destination "$DEST" \
  --destination-path "$CASE.bin" --bandwidth 8388608
```

The successful cancellation case added `--cancel-after-ms 16000`. Kill cases
polled the read-only inspector for their required pointer state and sent
SIGKILL to the exact live probe PID, checking its `-SIGKILL` exit status.
The ordinary 64 MiB checkpoint interval and connector defaults were unchanged;
no block-size override was used. The source budget's existing 8 MiB burst and
debug-build hashing make time alone unsuitable for estimating durable bytes.

Immediately before resume, the inspector recorded the pointer's outer v2
checksum, checkpoint checksum, RangeHash domain-separated checksum, D ranges,
H ranges, phase, generation, native visible stage length, and permissions.
All observed pointers were valid v2 in Receiving phase. It did not fence live
writers; normal backend prepare performed lease recovery after writer death.

Each resume used bandwidth `67108864` and was traced with:

```sh
strace -f -qq -yy -s 0 \
  -e trace=pread64,read,readv,preadv,preadv2,lseek,openat,mmap \
  -o "$RUN/$CASE.resume.strace" transfer_resume ...
```

The audit selected only calls whose decoded FD path was the exact local source
fixture. It joined unfinished/resumed calls, parsed positive `pread64` results,
rejected unexpected source read/mapping forms, checked no returned range
intersected `[0,R)`, and required sorted ranges to form the exact contiguous,
nonoverlapping suffix `[R,size)`. Their byte sum also had to equal the probe's
source QoS count and `size - reused_bytes`. A separate `--compare` invocation
then read both entire files and compared their independent BLAKE3 digests;
those intentional full source reads are outside the resumed-process trace.

Relevant primary implementation: [HDFS preparation/fencing](../../src/storage/backends/hdfs/at_destination.rs),
[ordered writer](../../src/storage/backends/hdfs/writer.rs),
[hash-intent producer](../../src/transfer/engine/checkpointed/producer.rs),
[pointer envelope](../../src/storage/pointer.rs),
[checkpoint envelope](../../src/storage/checkpoint.rs), and
[RangeHash codec](../../src/storage/range_hash/codec.rs).

## Exact results

All values below are bytes; H is contiguous `[0,H)` in these fixtures. The
native stage length is the pre-resume NameNode observation, **not** the length
obtained after lease recovery. R is the actual fresh prepare/reused prefix.

| Case | Saved D | Saved H | Visible stage | Recovered R | Resumed source bytes | Source preads | Resume ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Cancellation | 67,108,864 | 109,051,904 | 109,051,904 | 109,051,904 | 159,383,689 | 248 | 10,422 |
| SIGKILL at checkpoint | 67,108,864 | 67,108,864 | 67,108,864 | 67,108,864 | 201,326,729 | 313 | 12,005 |
| SIGKILL with H-only pointer | 0 | 8,388,608 | 0 | 8,386,560 | 260,049,033 | 404 | 15,313 |
| SIGKILL after hash-ahead intent | 67,108,864 | 75,497,472 | 67,108,864 | 75,495,424 | 192,940,169 | 300 | 12,238 |

Every row reported `Resumed { bytes: R }`, `Checkpointed`, `Streaming`,
`read_back=Enabled`, `read_back_method=Pinned`, exact final size `268435593`,
zero native-copy bytes, zero recovered-prefix source-read overlaps, and zero
unexpected source read forms. Each successful publication left no
`.data-mover-*` artifact in the run root. `upload_digest=Unchecked(NotApplicable)`
is the HDFS upload-receipt field, not a skipped read-back/content comparison.

The strongest append result adopted `8,386,560` bytes **beyond** saved D, while
remaining within previously persisted H. Both killed hash-ahead writers
recovered 2,048 fewer bytes than H covered. The missing bytes were transferred
again as payload, while the existing source hash evidence remained usable.
This distinguishes H from durable data instead of promoting intent to D.
The H-only case also disproves reliance on the stale visible stage length of
zero when recovering an abandoned append lease.

Full source and independently reopened destination BLAKE3 were identical:

| Case | BLAKE3 |
| --- | --- |
| Cancellation | `16ecd215e089b3b62b40ebe7d34e206b6cea32f51425b8ea8020ed49523a41bc` |
| SIGKILL at checkpoint | `460fc2dee27e34a3caca6469ae3b4ebc8649ccddd54233cc05560ef48ec7f963` |
| H-only | `98c65927ae8dfba59c50bd33c4b427624d3c19b60b0849be0629c13f3a1bd7fd` |
| Lease tail | `1739778a4bc5dcb9b36b0d9192f49138418fbbc9d778c838c15539d7c5f25d08` |

## Privacy, cleanup, and evidence

All saved v2 pointers were mode 0600 and only 350–450 bytes. The H-only and
lease-tail sources were explicitly created under umask 0077 and observed 0600;
their published finals were also 0600. No owned live pointer remained after
successful verification/publication, including for those restricted sources.
Initial payload stages were observed 0644, the existing payload create behavior,
before metadata application. This report therefore certifies the observed
**hash-pointer** privacy, not confidential payload-stage permissions throughout
the transfer. No broader ACL or global permission setting was changed.

There were two harness-only preliminary stops: a 10-second cancellation had
valid H and a 56 MiB closed stage but D=0, contrary to the harness's positive-D
precondition; and an extra-case assertion incorrectly expected payload stages
to begin at 0600. Both were cleaned before rerunning. Neither was a backend
failure, and neither is counted as a completed resume test.

Native exact-root cleanup returned `true` for both successful suites. A separate
fresh native inspection after cleanup returned
`FileNotFound("/tmp/data-mover-nightly/nightly-hash-hdfs-20261023T0159-byY8OO/hdfs")`.
Runner payloads, binaries, helper files, and chunks were subsequently removed
only from this run's exact runner directory. Regenerable local binary copies
were removed; credential-free logs, temporary harness sources, and the evidence
archive remain in this run's local directory for audit, not on the runner.

Raw evidence retained locally:
`/tmp/data-mover-nightly/nightly-hash-hdfs-20261023T0159-byY8OO/evidence/`.
The accompanying `evidence.tgz` contains 85 files, is 45,750 bytes, and has
SHA-256 `0623245a8ed37c5077ce9aa93ea332a7413de7788c58db573a9bc0fa2d1f4a50`.
Each `CASE.evidence.json` contains the exact initial facts, resume JSON,
individual source ranges, full comparison JSON, and final artifact listing.
`source.facts.json`, cleanup logs, both suite logs, and the helper/harness
sources are retained alongside the four syscall traces. Trace SHA-256:

| Trace | SHA-256 |
| --- | --- |
| `cancel.resume.strace` | `97fe8cad56ff1546b570781de502b765235103baa981310577bb71e9ffb24248` |
| `kill.resume.strace` | `97447a0721d0b8ecf8f642784bbf1a73d8ab2585be0c539137c92c31c0a40d67` |
| `intent.resume.strace` | `901af8544be4dfd4f804001051e9ef0b4492dd4e01a36b9122c2ab446b4f8515` |
| `tail.resume.strace` | `78ce7ddbaef3548318d9e045bd54f2a14e1eec992d707b378bed5fdde4120221` |

The trace strings were suppressed with `-s 0`; no keytab or source payload
contents were captured. Local evidence is temporary, not a committed CI artifact;
retain/archive it separately if long-term reproducibility requires the raw logs.

## Remaining gaps

- No power interruption, NameNode/DataNode failure, HA transition, network
  disruption, or erasure-coded HDFS validation was attempted.
- No real-cluster corrupt/binding-mismatch negative or ReadyToPublish/Published
  final-without-stage gap was injected. Those paths are not certified here.
- Read-back-disabled recovery, multiple concurrent writers, and conflict fencing
  were not runtime matrix cases. All positives used read-back enabled and one
  authorized writer per final path.
- AtomicReplace, Direct, ineligible small files, and expert/low-level legacy I/O
  counts were not remeasured by this real-cluster run. Cluster-free isolation
  contracts and main's workspace test/clippy run remain separate evidence.
- Source non-reread proof is for the Local source syscall path used here, not
  every possible remote source adapter or its server-side request telemetry.
- Pointer privacy was sampled at interruption and absence checked at success;
  transient pointer creation modes were not independently wire-captured.
