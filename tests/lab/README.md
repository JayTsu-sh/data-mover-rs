# Terrasync integration lab

The lab is shared by `nfs-rs`, `data-mover-rs`, and `terrasync-rs`.

| Role | Management | Data | Services |
|---|---|---|---|
| Controller | 10.131.9.11 | 10.10.1.11 | GitHub Actions Runner |
| Source | 10.131.9.12 | 10.10.1.12 | NFSv3, NFSv4.1, RustFS |
| Destination | 10.131.9.13 | 10.10.1.13 | NFSv3, NFSv4.1, RustFS |
| Worker | 10.131.9.14 | 10.10.1.14 | RustFS, fault injection |

Every run must call `prepare-run.sh` with a unique `nightly-*` or `release-*`
identifier and call `cleanup-run.sh` from an `always()` step.

Management traffic uses `10.131.9.0/20`. Test data uses `10.10.1.0/24`.
Credentials are provisioned on the self-hosted runner and must not be committed.
The runner must resolve the canonical Kerberos HDFS hostnames to their lab
management/data addresses. `health-check.sh` verifies the NameNode and both
DataNode mappings (plus the second NameNode when HA is configured) before any
long-running test starts.

`run-hdfs-smoke.sh <run-id>` exercises the HDFS dependency and backend contract.
For a Kerberized cluster, `LAB_HDFS_LOCATION` contains the percent-encoded
principal in its username, while `LAB_HDFS_CONFIG_DIR` and `LAB_HDFS_KEYTAB`
provide client-scoped configuration for that backend. The keytab must be mounted
into the process running the test and is never embedded in the URL. Stateful HDFS
cases run with one test thread so concurrent creates, appends, block deletion,
and lease recovery do not compete for the two shared lab DataNodes.
Root lifecycle cases use `LAB_HDFS_ADMIN_USER` (default
`hdfs/hdfs-namenode@HDFS.LOCAL`) only to create
and remove their isolated `/tmp/data-mover-nightly` namespace; read-only smoke
coverage continues to use the user embedded in `LAB_HDFS_LOCATION`.
The `Windows native runtime` workflow runs filesystem tests on GitHub-hosted Windows,
using the native Rust MSVC target. Its `local_transfer_runtime` target covers Local
AtomicReplace/Checkpointed copy and overwrite, read-back enabled/disabled, cancellation,
and handle cleanup, alongside the existing positional-I/O and cluster-free contracts.
It runs for pull requests, manual dispatch, and `test/windows-native-*` branch pushes.
A successful cross-compile alone is not a native runtime result. This workflow does not
exercise real NFS/CIFS/HDFS servers or assert POSIX metadata support on Windows.

Regular CI also runs a native `windows-latest` compile gate for the library,
the cluster-free HDFS contract target, and all examples. This establishes
Windows build support for explicit Simple mode without silently skipping a
missing cross target. It does not claim real Windows-to-HDFS runtime support;
that requires a separately provisioned Windows lab before becoming a gate.
`tests/hdfs_upstream_contract.py` is the acceptance entry point for the hsync-enabled
`hdfs-native` Git dependency. It verifies the pinned fork revision,
prints the full `Cargo.lock`-resolved commit (and the previous commit when it
changed), then runs the cluster-free API/config/error contracts. Nightly invokes
the same entry point with `--nightly --run-id "$RUN_ID"`, which additionally
runs the complete real HDFS lab. A new resolved commit is accepted only after those contracts, bounded
streaming tests, the Windows build job, and the real lab all pass.
Both HDFS runners require a validated `nightly-*` or `release-*` identifier and
derive exactly one remote root at
`/tmp/data-mover-{nightly|release}/<run-id>/hdfs`. Every HDFS fixture is a
validated child of that root, and an EXIT trap performs idempotent exact-root
cleanup. Manual runs must therefore supply their own unique safe ID, for
example `nightly-local-$(date +%s)-$$`; no shared fallback namespace exists.
`run-hdfs-s3-metadata.sh <run-id>` adds the real S3-to-HDFS metadata contract using the
standard `LAB_S3_ACCESS_KEY`, `LAB_S3_SECRET_KEY`, and `LAB_S3_BUCKET` secrets.
It creates a unique S3 prefix, exercises S3-to-HDFS and HDFS-to-S3, verifies
single-part and multipart source-mtime metadata, and removes the prefix on exit.
The runner image must preinstall `rustup`, Rust `1.95.0`, `clippy`, and
`rustfmt` for the `github-runner` account, with
`/home/github-runner/.cargo/bin` on `PATH`. Keeping these tools in the image
avoids a runtime dependency on `sh.rustup.rs`; the workflow still selects the
exact toolchain to keep builds reproducible.

`run-e2e.sh` exercises the complete directed copy matrix across Local, NFSv3,
NFSv4.1, S3, and HDFS: 5 same-protocol paths plus 20 cross-protocol paths. Every
case uses an isolated payload and independently reopens the destination to
verify its exact size and full content hash. HDFS destinations use the bounded
`storage_inspect` reader in a separate process because the runner does not
require an HDFS CLI. The script also runs the complete 5-by-5 matrix with a
`48 MiB + 137 byte` offset-derived
fixture, large enough to exceed a queue depth of eight even for 5 MiB S3
chunks. Every destination is reopened and checked for both exact size and a
full digest; `storage_copy` additionally checks capability-aware metadata,
including HDFS's millisecond timestamp boundary. A
separate payload larger than 12 MiB is copied from NFSv4.1 to NFSv4.1 so normal
copy exercises multiple negotiated read and write requests, including the
session-limited effective `wsize`.

`run-nfs3-contract.sh <run-id>` is the dedicated `DM-NFS3-CONTRACT` gate. It bypasses
the legacy storage enum and validates the public NFS role handle, protocol-neutral traversal,
typed unsupported NFSv3 ACL/xattr observations, streaming overwrite publication, and recovery
of a remotely re-observed durable prefix after a fresh mount.

`run-nfs40-contract.sh <run-id>` is the independent `DM-NFS40-CONTRACT` gate against the
DXN `10.131.7.201:/jay_nfs` fixture using the exact `version=4.0` selector. It seeds an isolated
run root through NFSv4.0, then validates the production role seam for traversal, stateful file
handles, streaming copy, source-only QoS accounting, reconnect recovery, and atomic recovery
claims. ACL operations are exercised rather than inferred from a capability bit: DXN GETACL is
observed and SETACL must succeed with identical ACL read-back, both through raw `nfs-rs` and
through a fresh production role connection. The current fixture supports this round-trip;
`--expect-setacl-unsupported` is reserved for explicitly unsupported fixtures, not selected by
the nightly runner. Typed permanent `Unsupported` mapping is covered by a cluster-free
metadata-adapter test. Named attributes are also typed `Unsupported`.

`run-nfs41-contract.sh <run-id>` is the independent `DM-NFS41-CONTRACT` gate across the source
and destination Linux NFS servers using exact `version=4.1` URLs. It replaces a source directory
after the role has cached its old file handle, then covers ACL operation results, traversal,
streaming integrity, in-flight cancellation, restart upload, reconnect recovery, and competing
recovery claims. Named attributes must return typed `Unsupported` until the adapter implements
that operation.

For NFSv4 cancellation, restart is a new transfer attempt on freshly connected
source and destination handles. The native client can fence session slots whose
outcome became unknown when an RPC future was dropped; the gate must not assume
that the canceled session remains reusable. A post-cancel source probe accepts
only success or a session-scoped `Connectivity` failure, never a generic entry
error. Source LOOKUP preserves native failure classification through the role's
typed lookup path rather than the legacy string wrapper.
The gate still requires exactly an unpublished, non-recoverable stage and its
explicit discard, unchanged source identity/size/mtime across reconnect, a Fresh
restart, the complete 512 KiB upload, matching BLAKE3 with read-back enabled,
and an independent read/hash of the published destination. Subsequent recovery
checks use the reconnected handles as well; reconnect does not swallow failed
cleanup, permanent errors, or content mismatches.

The nightly workflow runs this matrix three times: with protocol defaults,
strictly serial read/write pipelines, and higher read/write concurrency. Release
validation runs the defaults and higher-concurrency profiles. The integrity and
resumable-copy matrices also run with the higher-concurrency profile, so a
release cannot pass by exercising configuration parsing without exercising the
configured pipelines and verifying their final size and SHA-256 digest.

`run-inflight-benchmark.sh` measures the complete 4-by-4 directed protocol
matrix with a fixed 128 MiB payload at inflight depths 1, 2, 4, 8, and 16. It
runs each case twice in opposite depth order and records elapsed time,
throughput, user/system CPU time, process CPU percentage, and peak RSS. The
timed command includes the production copy integrity check, so the CSV reports
end-to-end copy-and-verify cost rather than an unchecked microbenchmark.

`run-local-copy-comparison.sh <run-id> [output.csv]` compares three Local-to-Local
paths on the runner's real filesystem: raw legacy `StorageEnum`, the same legacy
path plus its missing `fdatasync` durability barrier, and the role-based Local
backend with the unified staged lifecycle. It measures exact 4 KiB, 40 MiB, and
1 GiB files three times by default, rotating implementation order. All paths run
their content verification inside the timed copy; the runner additionally checks
SHA-256 and flushes each sample outside the timed interval so the next sample does
not inherit dirty-page debt. Raw legacy remains a historical reference, while the
role-based path uses an ephemeral, non-resumable stage when the payload fits the
backend-negotiated source chunk; it retains verification and atomic publication but
does not persist a checkpoint. The larger payloads retain resumable staging. The
optional `LOCAL_COPY_PERF_MAX_REGRESSION_PERCENT` guard compares Optimized against
`legacy-durable`, the baseline with matching durability. Bulk payloads use that
percentage guard; 4 KiB uses the absolute
`LOCAL_COPY_PERF_MAX_FIXED_OVERHEAD_MS` budget (5 ms by default) because staged
lifecycle setup is a fixed cost. Use
`LOCAL_COPY_PERF_PAYLOADS` for a tight subset, `LOCAL_COPY_PERF_ROOT` to select the
filesystem, and `LOCAL_COPY_PERF_REPEATS` to increase the sample count.

`run-local-write-benchmark.sh` isolates the role-based Local destination write shape from source
reads, hashing, channels, verification, and publication. Each sample opens one new scratch file,
issues full-size positional `write_at` pieces through bounded `spawn_blocking` inflight work,
records write submission and `sync_all` separately, validates syscall and short-write counts, and
then deletes that file. Defaults cover 64 KiB through 8 MiB chunks, inflight 1/2/4/8, three rounds,
and 256 MiB per sample on `/tmp` (ext4), `/work` (XFS), and `/dev/shm` (tmpfs). Override
`LOCAL_WRITE_BENCHMARK_ROOTS`, `LOCAL_WRITE_BENCHMARK_CHUNKS`,
`LOCAL_WRITE_BENCHMARK_INFLIGHT`, `LOCAL_WRITE_BENCHMARK_TOTAL_BYTES`, or
`LOCAL_WRITE_BENCHMARK_ROUNDS` for a focused confirmation. This is a destination capability
benchmark; it deliberately reuses one immutable payload so source allocation is outside the
measurement, and production inflight memory must still be budgeted as distinct upstream pieces.

`run-performance-baseline.sh` freezes the pre-refactor performance evidence.
It requires a stable `PERF_HARDWARE_ID`, records the exact Git commit, dataset,
single-file concurrency, requested 2 MiB chunk and inflight depth 8, and emits
both raw CSV samples and a SHA-256-bound JSON summary. The baseline contains
the depth-8 4-by-4 large-copy matrix plus five public-seam Local scan and copy
runs over 100 fixed small files. Its report includes throughput, entries/s,
p95 entry scheduling latency and peak RSS; mixed hardware or incomplete measurements
are rejected instead of being summarized. Nightly uploads the CSV and JSON as
one artifact and signs their SLSA provenance with GitHub artifact attestations,
so later architecture gates can compare like-for-like evidence. Verify a
downloaded baseline with `gh attestation verify performance-baseline.csv
--repo JayTsu-sh/data-mover-rs` (and repeat for the JSON file).

At this public batch seam, scan scheduling latency is the interval between
successive entry delivery events. Copy scheduling latency is the time from
admitting the fixed batch to the bounded, concurrency-one queue until each
entry starts `transfer`; copy service time remains represented in
entries/s and end-to-end elapsed time rather than being mislabeled as queueing.
Scheduling latency is deliberately null for the single-entry large-copy rows,
where throughput and elapsed time are the applicable metrics.
The self-hosted lab uploads raw evidence before the remaining HDFS gates. A
dependent GitHub-hosted job downloads those exact files and signs them, keeping
Fulcio traffic away from the lab proxy. Signing failure still fails the overall
workflow, while uploaded raw evidence remains available for diagnosis. Unsigned
evidence does not satisfy the release gate.

`run-qos-e2e.sh <run-id> [output.csv]` is the real-backend source-QoS gate for
Local, NFSv3, NFSv4.1, S3, and HDFS. It first measures each unthrottled source
and requires that baseline to exceed the configured limit by 25%; a naturally
slow backend therefore fails as inconclusive instead of producing a false QoS
pass. It then copies a `16 MiB + 137 byte` fixture at 8 MiB/s with a 64 KiB
burst, checks the hard wall-clock lower bound, independently verifies the copy,
and requires the reported source payload bytes to equal the file size exactly.
Set `QOS_LAB_PROFILE=iops` for protocol-operation shaping and
`QOS_LAB_IOPS_MODE=soft-hard` for dual-rate IOPS. Nightly and release validation
run strict bandwidth, soft/hard bandwidth, strict IOPS, and soft/hard IOPS as
four separate gates and upload their measurement CSV files.
For Local/NFS/HDFS, source IOPS must equal the number of 64 KiB real reads (257).
S3 uses an explicit 8 MiB GET/request cap (three requests for this fixture),
including at the 2 IOPS setting. Bandwidth paces each entire S3 request before
dispatch rather than shrinking GETs to a 10 ms byte allowance. Thus the request
size bounds the permitted burst; this is source admission pacing, not a claim
about instantaneous socket throughput. S3 baseline and shaped runs use the
same configured read size. Failed S3 GET attempts consume IOPS again, with at
most three attempts per range; HEAD/metadata requests use separate accounting.
The CSV preserves baseline and
limited measurements for diagnosing lab drift. The default strict case has
soft and hard rates both set to 8 MiB/s. Set `QOS_LAB_SOFT_RATE_MIB`,
`QOS_LAB_HARD_RATE_MIB`, and `QOS_LAB_PEAK_DURATION_MS` to exercise a dual-rate
soft-credit scenario; for example `8`, `12`, and `500` derive a 2 MiB credit
capacity.

`python3 tests/lab/run-local-qos.py --work-dir /work/qos-scratch --output
.scratch/qos-local-<run-id>` runs the four performance acceptance scenarios on
Local alone, without network services. Allow at least 1 GiB of scratch space.
It builds the release `local_qos_acceptance` example with `transfer`,
`backend-local` and the opt-in `qos-diagnostics` trace feature enabled. The fixed runtime has four workers. Fixtures are fully
written files on the requested filesystem; every source is hashed before timing,
so these measurements use a warm page cache. Every run checks the complete BLAKE3
digest; scratch fixtures and destination files are removed when the run ends.

Bandwidth and IOPS are exercised independently and together. Unlimited and very
high QoS runs use identical 256 MiB files and request sizes, alternate order over
three repetitions, and compare median throughput with a 98% minimum ratio.
Reasonable limits are 16/32 MiB/s and 128/256 IOPS, plus strict limits with equal
soft/hard values. The baseline must have at least 25% headroom over the hard rate.
The dual-rate cases first idle for one second to accumulate bounded credit.
They must show an initial burst, then settle below the soft ceiling. Both strict
and dual-rate cases must attain 98% of the soft rate; the source-read trace also
checks the last five seconds so initial credit cannot hide sustained underfill.

Two paths are measured: serial `ReadSource` reads record every real Local read's
completion, while public `transfer` uses eight reads/writes in flight and retains
the default verification and durability policy. Both paths also record exact QoS
admission timestamps under the shared scheduler lock. They check **every
interval between admissions**, including event left limits, against hard and
soft traffic envelopes. The hard envelope allows exactly one actual read packet,
and the soft envelope adds `(hard - soft) * peak_duration` credit. There is no
percentage tolerance on either ceiling. These are discrete application-read
guarantees, not instantaneous disk-device bandwidth or physical device IOPS.
Full-copy timings include default publication and verification, so the 98% floor
also reveals overhead outside source admission. Unlimited copy IOPS is inferred
from fixed Local read size; shaped copy IOPS is the actual QoS operation counter.
Completion traces are retained separately: IO latency variation can compress
completion spacing even when admission spacing obeys the hard limit, so completion
timestamps cannot be substituted for admission timestamps in these ceiling checks.
Diagnostic tracing is absent from normal builds and must be explicitly enabled;
its unbounded event buffer is intended only for finite acceptance workloads.

The runner saves environment details, commands, per-read traces, a measurement
CSV and individual pass/fail checks. It runs all scenarios after a failed gate,
then exits nonzero if any check failed. An IO or verification error aborts the
run and preserves the completed raw measurements. `--output` must be a fresh
directory. `python3 -m unittest discover -s tests -p test_local_qos_acceptance.py`
checks the envelope validator, including overshoots hidden by idle time and
startup bursts masking a steady rate below 98%.

The [2026-10-29 Local/XFS measurements](../../docs/reports/local-qos-20261029/measurements.csv)
and [individual acceptance results](../../docs/reports/local-qos-20261029/checks.json)
record the original 95% acceptance run on commit `2c86e68`: soft/hard envelopes and
content verification passed, while high-IOPS QoS overhead and the former 95% throughput
floor failed. Raw per-read events, exact commands, environment and the run log
are retained beside those files. No production QoS code was changed for this run.

The [historical optimized Local/XFS results](../../docs/reports/local-qos-optimized-20261029/checks.json)
passed the former 95% threshold and use exact admission traces for serial reads and eight-inflight public
transfers. The scheduler now skips already-satisfied waits, counts elapsed IO
towards the next strict-rate interval, and replenishes soft credit while waiting
for either resource. Long waits use Tokio; the final at-most-2-ms tail uses
OS sleep on a blocking worker, waking up to 200 microseconds early. The last
margin yields cooperatively so timer-return latency does not accumulate.
Initial waits of at most 8 microseconds use a bounded CPU wait to avoid a runtime
handoff costing more than the remaining interval. A separate wall-clock bound
keeps paused clocks safe; cancellation is rechecked after at most that margin.
Longer waits do not block runtime workers. Cancellation is uncharged, and a
detached blocking tail can occupy its worker until that bounded sleep finishes. Hard
intervals round up to nanoseconds and restart from the actual release time, so
late wakeups never accumulate permission for catch-up bursts. Admission tracing
is an explicit diagnostic feature and adds no trace buffer to normal builds.

`run-backend-qos.py --config <private.json> --root <isolated-local-run-root>
--binary <release-backend_qos_acceptance> --phase survey|final|stress` extends this gate
to real Local, NFSv3/v4.0/v4.1, CIFS, S3 and Kerberos HDFS sources. Build with
`cargo build --release --locked --features qos-diagnostics --example backend_qos_acceptance`.
A private JSON file contains `run_id` and `profiles`; every profile has `name`,
`env` (including `QOS_SOURCE`), optional `size_mib` and `chunk_bytes`. Endpoint
shortcuts and credentials follow `examples/support/endpoint.rs`; environment
credentials can also be inherited. Never commit the private file. Use unique
source directories/prefixes and the lab's preparation/cleanup lifecycle.

The survey phase seeds fully written 64 MiB fixtures (128 MiB for S3); the final
phase uses the same fixtures and copies each source to Local with default
verification/durability. Seeding uses verified AtomicReplace: it does not require
the source server to support recoverable checkpoint ACLs. Five baseline/high-QoS
pairs alternate order. The high case enables both bandwidth and IOPS at once,
retaining the same negotiated read cap and operation count as baseline.
Strict, dual-rate and combined cases check the 98% end-to-end floor as well as
steady source admission rates and all-interval hard/soft envelopes. Steady-rate
windows anchor at actual admissions, excluding the left event, so large GETs and
low IOPS do not create fixed-window quantization errors. Rates, request caps,
peak credit duration, baseline headroom, exact binary hash and all raw traces
are saved. High-QoS timings disable the diagnostic admission buffer and retain
production accounting; every finite limit case enables the exact trace. Each final source fixture is removed, and cleanup failures fail the gate.
Full run-root and versioned-object cleanup still belongs to the lab lifecycle.

For longer performance samples, pass `--performance-size-mib 512`; only the
baseline/high cases use that size, and finite limit cases keep their 64/128 MiB
fixtures. A profile's `performance_size_mib` overrides the common size. The
`stress` phase reads 128 MiB serially in 64 KiB requests and tests strict 128 IOPS
and soft/hard 128/256 IOPS. Use `--phase stress --measure-only` to measure its
baseline/high pairs without starting finite limit cases or removing their source.
`--baseline <survey-output>` imports those exact measurements for `final` or
`stress`; a binary SHA-256 mismatch is rejected. Performance surveys must run
serially. Independent finite limit cases may be batched after surveys, provided
the aggregate offered rate remains below measured backend capacity. Profiles
sharing a source filename must be reseeded between cleanup and the next batch.

On a capacity-constrained isolated export, `remove_old_seed: true` deletes only
the selected old fixture before seeding its replacement. Build
`cargo build --release --locked --example backend_qos_cleanup` for abandoned
transfer-artifact cleanup inside a source endpoint containing `--run-id`.
`QOS_SOURCE=... backend_qos_cleanup --run-id <run-id> --file 128.bin` also removes
that known fixture; missing fixtures are accepted. This helper does not replace
`cleanup-run.sh` or its versioned S3 and confined HDFS cleanup.

The [2026-10-30 multi-backend 98% report](../../docs/reports/qos98-backends-20261030/REPORT.md)
contains 406 passing final checks across seven real protocol profiles, earlier
failures, root causes, raw evidence and the application-admission hard-limit scope.

`run-cross-backend-qos.py --config <endpoints.private.json> --root <run-root>
--binary <backend_qos_acceptance>` adds 23 directed routes: the seven source-to-Local
regressions and all 20 off-diagonal pairs among Local, NFSv4.1, CIFS, S3 and HDFS.
The endpoint file contains `run_id`, `sources` (seven keys, each containing `env`
with `QOS_SOURCE`) and `destinations` (five keys, each containing `endpoint` with
an optional `{pair}` placeholder and credential `env`). Endpoint credentials stay
in the private file/environment and never appear in command arguments.

The `default` transfer profile retains default copy-time read-back and recovery.
Network destinations use the `cross` profile: AtomicReplace, timed payload transfer
and atomic publication, then an independent complete destination read/hash outside
that timer. AtomicReplace omits final persistence barriers on Local/NFS and does
not promise crash durability; other backends retain their protocol close/commit
requirements. These results must not be described as default durable-copy results. Its duration is saved as `verification_seconds`; integrity
failure fails the sample and retains the destination for diagnosis. This avoids
requiring protected checkpoint ACLs on the CIFS fixture and keeps independent
acceptance read-back separate from source QoS performance. QoS still shapes source
payload reads only. Both baseline and high use the same profile and request cap.

Stages `warmup`, `seed`, `survey`, and `limits` may run separately. Warmup copies and
hashes 16 MiB on every route; finite limits use 128 MiB. The survey runs five
alternating baseline/high pairs serially. Finite limits may run in four independent
processes after surveys. Shared source fixtures are explicitly retained until the
matrix lifecycle cleans the isolated run. Imported baselines bind both the binary
SHA-256, the complete endpoint/profile configuration SHA-256, repetition count,
and unmeasured idle. An explicit
`baseline-overrides.private.json` may name a route's larger-sample survey and
matching private configuration after failed short-sample measurement; preserve
both attempts and document this provenance in the report. Overrides may change only
the performance fixture size, retaining the route, copy policy and finite limits.
`--performance-idle-ms` applies the same unmeasured idle to both performance arms.
A longer sample that passes does not erase a failed short-file end-to-end result.
Native server-internal
S3 copy is outside the client source-read shaping contract.

For S3, bandwidth shaping must preserve the configured bounded GET size (up to
8 MiB) and wait between requests, rather than inflate the request count with
small GETs. Finite bandwidth and combined cases explicitly check the expected
payload request count. Report any tolerated bandwidth deviation separately from
IOPS and hard-envelope checks; record the tolerance and observation window before
relaxing a verdict. Diagnostic wire counts must distinguish source payload, target
uploads, metadata and independent acceptance read-back.

Use the lab's `prepare-run.sh` / `cleanup-run.sh` lifecycle around the matrix.
`backend_qos_cleanup --run-id <run-id> --tree` removes children recursively only
inside an endpoint whose path contains that isolated run ID, retaining the root.

The [2026-10-30 cross-backend report](../../docs/reports/qos98-cross-backends-20261030/REPORT.md)
retains every failed gate and separates source steady rates, complete-copy rates,
and longer-sample requalification. Short-file strict soft=hard checks are retained
even when dual-rate QoS or a longer file meets the 98% floor.
This supplements standard cleanup for the CIFS and NFSv4.0 fixtures.

The [high-QoS root-cause and regression report](../../docs/reports/qos-high-root-cause-20261030/REPORT.md)
documents the S3 HTTP transport reuse fix, original-size old/new controls,
independent confirmation, and remaining failed gates. It preserves the initial
matrix failures separately from subsequent controls.

For noisy remote targets, add `--performance-control --isolated-destinations
--cleanup-binary /path/to/backend_qos_cleanup` to `run-backend-qos.py` or the
cross-backend runner. Use at least three repeats; multiples of three balance the
baseline/control/high positions. The control repeats the same no-QoS workload.
Every copy uses a fresh destination prefix and all destination cleanup runs after
the profile loop, including when an exception or KeyboardInterrupt unwinds it. `config.run_id` and every
selected destination must identify the same isolated `nightly-` or `release-` run.
Baseline imports must retain both options.

The original median high/baseline 98% gate remains unchanged. Additional gates
check the A/A ratio's paired-block bootstrap interval against `[0.98, 1/0.98]`
and the high/baseline interval's lower bound against 0.98. The deterministic
4,000-draw interval is a diagnostic precision guard, not a guarantee for every
future transfer. An unrepeatable baseline remains unsuccessful and is labeled
`environment_not_repeatable`; it cannot qualify a favorable QoS point estimate.

`run-hdfs-memory-e2e.sh` is the bounded-memory acceptance gate. It generates
fully written deterministic `256 MiB + 137 byte` and `2 GiB + 137 byte`
fixtures with a 1 MiB producer buffer, then measures only the release
`storage_copy` process for Local→HDFS, HDFS→HDFS and HDFS→Local under
serial/default/high inflight profiles. A separate `storage_inspect` process
reopens and fully hashes every destination. The CSV records sizes, profile,
windows, chunk/channel/file concurrency, duration, throughput and peak RSS.
Every measured child uses exactly four Tokio runtime workers, recorded as `runtime_workers`
in the CSV. The runner overrides an inherited `TOKIO_WORKER_THREADS` value, and the validator
rejects missing or different worker counts. Runtime workers are independent of the configured
asynchronous read/write windows: the high profile still runs with read=8 and write=16.
Without this fixed runtime profile, Tokio defaults to the runner's CPU count (24 on the lab
runner), and per-thread allocator arenas make the fixed process allowance non-reproducible.
Nightly `39711328550` reached 217.3 MiB despite decreasing transfer RSS and flat verification RSS.
The gate now fixes that previously unspecified variable rather than increasing its budgets.
HDFS read-back verification reuses one opened reader across bounded chunks of at most
2 MiB. It still reads and hashes the complete file; increasing the application chunk is
not required to avoid reopening the file and refetching block locations for every range.
The budget is `(3×read + write + channel[4] + active[2]) × 2 MiB × concurrent
files + 112 MiB`; it accounts for DataNode packet storage, `hdfs-native` range
aggregation, published chunks, the copy channel and destination write window.
The fixed process allowance includes runtime/allocator headroom and is not multiplied
by file concurrency or payload size. It was increased by 16 MiB after Nightly #185
peaked at 191.86 MiB with decreasing transfer RSS and only 0.10% verification RSS
growth. Serial/default/high budgets for one file are now 132/150/204 MiB; the
within-process 10% growth limit and all sampling checks remain unchanged.
The HDFS file block size remains independently configured at 128 MiB. Each baseline
process must meet its own absolute budget. The difference between the 256 MiB and
2 GiB processes' peaks is NOT a sustained-growth measurement: independent runs can
reach different allocator high-water marks while both remain bounded. The former
72 MiB cross-process delta check caused a false positive in Nightly #191 and has
been removed, without changing any absolute budget or the same-process growth gate.
Nightly and release validation fail on either an absolute-budget violation or
sustained growth in the long-run trace below.
The full gate also fails closed unless all nine baseline profile/direction groups
contain both exact fixture sizes with their declared concurrency settings.
The same run also preloads sparse deterministic zero fixtures into HDFS, then
measures one `high` HDFS→HDFS transfer at `1 GiB + 137 bytes` and one at
`100 GiB + 137 bytes`, with identical inflight, chunk, channel and file-concurrency
settings. The short run is a size/absolute-budget control, NOT the reference for
long-run RSS: peaks from unrelated processes cannot demonstrate sustained growth.

For the 100 GiB run, `hdfs_memory_sampler.py` samples only the copy child's Linux
`VmRSS` every 100 ms into `hdfs-memory.csv.samples.csv`. The example's optional
`--report-phases` markers distinguish transfer (including its policy verification)
from the subsequent independent content comparison. In EACH phase, the p90 RSS
in the 70–90% elapsed-time window may be at most 10% above the p90 in the 10–30%
window. This excludes initial warmup and final buffer release, and avoids treating
one transient allocation as sustained growth. It is a within-process steady-state
test, not a proof of absence of leaks, nor a per-byte allocation slope measurement.

Both phases must last at least five seconds, contain at least 50 samples and at
least ten samples per comparison window, with no internal sampling gap above two
seconds. Unknown/reordered phases, nonpositive RSS, missing evidence and mismatched
run/commit bindings fail closed. The whole-process peak (the larger of the kernel
child high-water mark and sampled RSS) STILL must satisfy the calculated absolute
budget; short spikes are not exempt. The sampler excludes its own RSS and the
later `storage_inspect` processes, propagates copy failure, kills a timed-out child
after 90 minutes, and preserves partial traces for diagnostics.

Nightly and release validation upload BOTH CSVs, even after failure. Validate with
`python3 tests/hdfs_memory_budget.py --require-100-gib --trace hdfs-memory.csv.samples.csv hdfs-memory.csv`.
Old peak-only artifacts intentionally cannot pass the rebuilt growth gate: a new
real run is required. No measured result for the old peak-comparison gate is
retroactively reclassified as passing.

`run-resume-e2e.sh` exercises the complete 5-by-5 Local/NFSv3/NFSv4.1/S3/HDFS
directed matrix in independent processes. The first process writes a durable
prefix, publishes a machine-readable receiver state and is then killed with
`SIGKILL` without committing. The second process rejects a changed source,
rediscovers the remaining range from destination truth, transfers only that
range, commits, and verifies the final hash. Representative HDFS-source and
HDFS-destination cases delete the source only after verification. This covers
every source interval reader and destination resume writer. NAS destinations resume
from a `.terrasync-part` file; S3 destinations resume the server-side multipart
upload through `ListMultipartUploads` and `ListParts`; HDFS destinations accept
only a durable contiguous prefix and append its missing tail sequentially.
The interrupt probe retries below-checkpoint cancellations with the SAME bounded
time window, never an increasing one: for the 96 MiB fixture at 8 MiB/s every
attempt cancels after 10 seconds (an 80 MiB pacing window plus at most one 2 MiB
admission burst). A 14-second retry can already retain every S3 part; a fresh
process then correctly reuses the whole file, which does not prove partial reuse.
The gate still requires `0 < reused_bytes < size`, independently reopens and hashes
the result, and reports the actual reuse and prepare fact if that assertion fails.
Source streamed bytes remain diagnostic evidence, not an estimate of durable bytes.
Each interruption attempt reconnects its endpoints instead of reusing a cancelled
transport session. Before starting it, the probe rechecks source size, observation
and derived transfer identity; a changed source or unexpected read/connection error
still fails the gate rather than being silently retried.

`run-hdfs-fault-e2e.sh` is the destructive HDFS service-recovery gate. Before
every mutation it verifies that PVE VM 301/302/303 still map to the expected
NameNode and two DataNodes by VM name, configured IP, guest hostname and exact
systemd unit. It stops only `hadoop-namenode.service` or
`hadoop-datanode.service`, never a VM. An EXIT trap restores all three units and
waits for SafeMode to be off with two live DataNodes before exact run-root
cleanup. The gate proves replica failover with one DataNode down, bounded and
root-relative failure with both replicas down, bounded NameNode failure, and
successful independent reopen/hash verification after each recovery. Its
artifact contains structured redacted diagnostics only.
The raw append checkpoint opens a writer but sends no append payload before SIGKILL.
After services are healthy, the harness explicitly recovers ONLY that killed
writer's file lease and waits for closure (120 seconds including RPC waits, with
an outer 125-second process bound). It then independently reopens the file and
requires the exact original size AND BLAKE3 digest, not just a size range. A healthy
new-write probe does not establish closure of an unrelated abandoned append lease;
ordinary readers must not fence potentially live writers to make such a file readable.
Reopen retries have a 90-second overall deadline and bounded individual probes;
their last redacted failure is preserved in the artifact instead of being discarded.

Cancellation can kill the fault harness before its EXIT restoration finishes.
Before any service mutation the harness writes a run-scoped `restore-required`
marker. Independent cleanup restores the validated services and cluster health
before deleting any fixtures when that marker is present. Restoration failure
stops cleanup and retains the marker for another attempt; ordinary unmarked
runs do not restart or fail over services.
The run-root delete additionally retries only the NameNode restart
`SafeModeException` (including Hadoop's `RetriableException` wrapper), with one
120-second deadline covering both RPC waits and retry sleeps. Other errors fail
immediately. This does not force SafeMode off or broaden deletion authority:
the same validated run root is deleted, and repeated deletion remains idempotent.

Fault cleanup also restores the direct NameNode to HA active. A systemd service can be active
before its RPC/HA ports accept requests, so restoration retries state queries and failed
failovers under one 120-second administration deadline, including blocked CLI calls. Each
attempt queries the current state; only a successful `active` observation completes restoration.
Node validation and credential failures still fail restoration, and the temporary Kerberos
cache is destroyed on exit.

Runner transport failures are separate from storage test failures. For example,
nightly #188's runner exhausted ten broker long-poll retries and canceled its
worker; its 150-minute job limit had not elapsed. On the dedicated
`vm102-data-mover-rs` runner, both `NO_PROXY` and `no_proxy` now include
`broker.actions.githubusercontent.com`, preserving the existing lab exclusions
and all other proxy settings. This is a runner service setting, not a workflow
environment override (the listener starts before workflow steps). Verify long
runs before treating this routing mitigation as a proven connectivity fix.
The dedicated service drop-in is `zz-broker-direct.conf` under
`/etc/systemd/system/actions.runner.JayTsu-sh-data-mover-rs.vm102-data-mover-rs.service.d/`.
The original `lab.conf` is unchanged. Rollback consists of removing only this
drop-in, reloading systemd, and restarting this runner while it is idle.

`run-hdfs-ha-e2e.sh` is a separate, opt-in HA acceptance gate. The current
10.131.9.30/31/32 lab has one NameNode and two DataNodes, so it is not an HA
topology and this gate visibly reports `NOT RUN`; that result is not HA
acceptance. A real run requires an all-or-none set of
`LAB_HDFS_HA_LOCATION`, `LAB_HDFS_HA_CONFIG_DIR`, and
`LAB_HDFS_HA_NAMENODE{1,2}_{ID,HOST,VMID,NAME,SERVICE}`. The location must be a
password-free logical NameService URL whose path is the exact run-scoped HDFS
root, and the configuration directory must contain the matching Hadoop client
XML files. When configured, the runner validates both PVE guests and exact
NameNode services, discovers active/standby state, streams a fixture through
the logical NameService, stops only the validated active service, verifies that
the former standby becomes active and independently reopens and hashes the
fixture, then restores both services and deletes the run root from an EXIT
trap. Hadoop XML contents and credentials must be provisioned outside the
repository and must not be printed or uploaded.

`run-integrity-e2e.sh` independently re-reads both the small and large complete
4-by-4 directed matrices through Local, NFSv3, NFSv4.1, and S3: four
same-backend paths plus all twelve cross-protocol paths in each matrix. It
compares traversal observations for every small-matrix pair and for the
cross-protocol negative ring, checking that observed inputs match before
corruption and report a difference afterward. Local path and observed inputs
also verify that permission comparison is opt-in. The lab bucket is not
assumed to be versioned; pinned S3 version comparisons have separate
versioned-backend unit tests. The gate checks the dedicated large NFSv4.1 fixture
using the negotiated effective read
size and proves that Quick mode does not read content. A cross-protocol negative
ring mutates each configured destination
backend in turn. Each negative case uses a `12 MiB + 123 byte` fixture and
places an equal-sized corruption at the unaligned offset `6 MiB + 17`, beyond
the default NFS, Local, and S3 read boundaries. Full must report that exact
global offset, while Quick must reject a one-byte destination size increase
without reading content. All four backends also cover empty files and missing
destinations. Local, NFSv3, and NFSv4.1 additionally cover matching
directories, file/directory type mismatches, and POSIX mtime mismatches. S3 is
excluded from directory and POSIX metadata cases because it exposes prefixes
rather than real directories and cannot faithfully represent those fields.
Timestamp coverage also proves that the example uses exact timestamp precision
with the library's default zero tolerance, that an explicit inclusive
tolerance accepts a bounded difference, and that the opt-in automatic precision
mode compares at the coarser apparent resolution.
The lab has no CIFS endpoint, so CIFS remains covered by the shared stream
contract tests until a real SMB service is added.

The self-hosted runner may provide `LAB_S3_ACCESS_KEY` and
`LAB_S3_SECRET_KEY`. When both are absent, the lab reads the active values from
`/etc/default/rustfs` over the existing root SSH channel, verifies that source
and destination use the same credentials, and exports them only to the current
test process. A partially supplied pair is rejected. Credentials must not be
committed, printed, or written to artifacts.

`run-s3-architecture-contract.sh` is the `DM-S3-CONTRACT` gate for the
ArchitectureReady roles. It exercises multipart staging, interruption and
reconnect resume, stale-upload restart, verification, publication, tags,
cancellation, range readback, connected-pair native CopyObject into an unpublished stage,
copy-time BLAKE3 verification, and unshaped native QoS reporting. Automated request contracts fix
the 5 GiB strategy boundary, and the real gate forces multi-part `UploadPartCopy` over the contract fixture with a
5 MiB test part size while retaining the production 1 GiB part size. The gate removes
both published objects and unfinished multipart uploads on exit. A successful
legacy S3 copy run does not certify this gate: the configured service must
accept CreateMultipartUpload and the complete multipart lifecycle.

`run-dxn-s3-contract.sh` is the independent `DM-DXN-CONTRACT` gate. It uses
the `?compat=dxn` profile and requires `LAB_DXN_S3_ACCESS_KEY` plus
`LAB_DXN_S3_SECRET_KEY`; the endpoint and bucket default to the shared DXN lab
but can be overridden with `LAB_DXN_S3_ENDPOINT` and `LAB_DXN_S3_BUCKET`.
Credentials remain process-local. The gate covers multipart interruption,
service-enumerated recovery after outstanding requests settle, restart,
readback, cancellation, connected-pair route selection, the explicit tag-unsupported result, the
DXN delete checksum contract, and a
standard-S3 isolation control.
Every run writes `dxn-s3-evidence.json`, including the exact candidate SHA,
run ID, environment fingerprint, timestamps, gate ID, and outcome. Nightly and
release workflows upload this report even when the gate fails.

The lab does not currently contain a real StorageGRID system. Request-level
tests use a capturing Smithy connector to verify that standard S3 requests
retain the SDK-generated `x-id` and StorageGRID requests remove it from the
transmitted URI. They also verify that StorageGRID multi-object delete carries
a body-matching, SigV4-signed `Content-MD5`. Dedicated `?compat=sg` smoke cases run against the
configured S3-compatible endpoints while the main S3 matrix continues using
standard `s3://`. These checks validate profile selection, copy, rename, and
non-regression behavior, but they are not a substitute for a smoke test on the
target StorageGRID version.

### Optional FAS2750 CIFS policy contracts

`tests/cifs_policy_contract.rs` uses an explicitly configured writable share through
`CIFS_REAL_SERVER`, `CIFS_REAL_SECOND_SERVER`, `CIFS_REAL_SHARE`, `CIFS_REAL_USER`,
and `CIFS_REAL_PASS`. Run with
`cargo test --release --test cifs_policy_contract -- --ignored --nocapture`; recovery state lives
beside the final file on the share, nothing is recorded where the test runs (ADR-0006).
`CIFS_POLICY_TEST_BYTES` optionally supplies comma-separated file sizes. The test
uses unique names, exercises both policies with read-back enabled, and cleans up
its own final/stage/checkpoint files. See the [FAS2750 validation report](../../docs/reports/2026-09-16-cifs-policy-validation.md)
for tested sizes, recovery coverage and debug-runtime limitations.

The [continued high-QoS investigation](../../docs/reports/qos-followup-20261030/REPORT.md)
records original-target A/A controls, all candidate regressions, rejected optimizations,
source-only attribution, fixed-quota checks, and exact-run cleanup evidence.

### Sustained QoS qualification

The end-to-end 98% gate includes the last read, upload and publication. At strict
quota a run with ideal duration `T` only has `T / 0.98 - T` extra seconds: a
16-second run allows 0.327 seconds, less than the measured S3 publication tail.
Keep short-run results as diagnostics; they must not be relabeled as passing.
For sustained qualification, select a larger fixture and keep the quota fixed:

```bash
python3 tests/lab/run-cross-backend-qos.py \
  --config "$PRIVATE_ENDPOINT_CONFIG" --root "$ISOLATED_RUN_ROOT" \
  --binary "$QOS_BINARY" --cleanup-binary "$QOS_CLEANUP_BINARY" \
  --limit-size-mib 1024 --limit-duration-seconds 128 \
  --performance-control --isolated-destinations
```

For the single-backend runner set `profile.size_mib=1024` and pass
`--limit-duration-seconds 128`. Increasing the fixture alone would also increase
its automatically selected IOPS rate and leave the ideal test duration near
16 seconds. The explicit duration prevents that; strict cases also check their
ideal duration, and imported baselines bind this setting. A fixture with fewer
than 128 source requests cannot run for 128 seconds at the minimum 1 IOPS: choose
a larger fixture rather than hiding the failed duration check. S3 retains its
8 MiB GETs. Strict summaries include admission timing and the remaining tail;
these diagnostics never replace the end-to-end verdict. High-QoS raw ratios and
A/A precision remain separate gates.
