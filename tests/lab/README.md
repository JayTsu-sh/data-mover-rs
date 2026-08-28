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
Regular CI also runs a native `windows-latest` compile gate for the library,
the cluster-free HDFS contract target, and all examples. This establishes
Windows build support for explicit Simple mode without silently skipping a
missing cross target. It does not claim real Windows-to-HDFS runtime support;
that requires a separately provisioned Windows lab before becoming a gate.
`tests/hdfs_upstream_contract.py` is the acceptance entry point for the rolling
`hdfs-native` Git dependency. It verifies that `Cargo.toml` remains unpinned,
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
entry starts `StorageEnum::copy_file`; copy service time remains represented in
entries/s and end-to-end elapsed time rather than being mislabeled as queueing.
Scheduling latency is deliberately null for the single-entry large-copy rows,
where throughput and elapsed time are the applicable metrics.
The workflow retries signing up to three times because Fulcio certificate
issuance is an external network operation, and it uploads the raw evidence even
when all signing attempts fail so the failure remains diagnosable. Unsigned
evidence still does not satisfy the release gate.

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
For Local/NFS/HDFS, source IOPS must equal the number of burst-sized real reads.
For S3 it must equal the number of 5 MiB Range GETs, proving that pacing slices
inside one response do not inflate request IOPS. The CSV preserves baseline and
limited measurements for diagnosing lab drift. The default strict case has
soft and hard rates both set to 8 MiB/s. Set `QOS_LAB_SOFT_RATE_MIB`,
`QOS_LAB_HARD_RATE_MIB`, and `QOS_LAB_PEAK_DURATION_MS` to exercise a dual-rate
soft-credit scenario; for example `8`, `12`, and `500` derive a 2 MiB credit
capacity.

`run-hdfs-memory-e2e.sh` is the bounded-memory acceptance gate. It generates
fully written deterministic `256 MiB + 137 byte` and `2 GiB + 137 byte`
fixtures with a 1 MiB producer buffer, then measures only the release
`storage_copy` process for Local→HDFS, HDFS→HDFS and HDFS→Local under
serial/default/high inflight profiles. A separate `storage_inspect` process
reopens and fully hashes every destination. The CSV records sizes, profile,
windows, chunk/channel/file concurrency, duration, throughput and peak RSS.
The budget is `(3×read + write + channel[4] + active[2]) × 2 MiB × concurrent
files + 96 MiB`; it accounts for DataNode packet storage, `hdfs-native` range
aggregation, published chunks, the copy channel and destination write window.
The HDFS file block size remains independently configured at 8 MiB. The 2 GiB
sample may grow by at most 72 MiB
over its 256 MiB peer. Nightly and release validation upload the CSV artifact and fail on either
an absolute-budget or size-dependent-growth violation.

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
same-backend paths plus all twelve cross-protocol paths in each matrix. It also
checks the dedicated large NFSv4.1 fixture using the negotiated effective read
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
Timestamp coverage also proves that comparison remains exact by default, that
an explicit inclusive tolerance accepts a bounded difference, and that the
opt-in automatic precision mode compares at the coarser apparent resolution.
The lab has no CIFS endpoint, so CIFS remains covered by the shared stream
contract tests until a real SMB service is added.

The self-hosted runner may provide `LAB_S3_ACCESS_KEY` and
`LAB_S3_SECRET_KEY`. When both are absent, the lab reads the active values from
`/etc/default/rustfs` over the existing root SSH channel, verifies that source
and destination use the same credentials, and exports them only to the current
test process. A partially supplied pair is rejected. Credentials must not be
committed, printed, or written to artifacts.

The lab does not currently contain a real StorageGRID system. Request-level
tests use a capturing Smithy connector to verify that standard S3 requests
retain the SDK-generated `x-id` and StorageGRID requests remove it from the
transmitted URI. They also verify that StorageGRID multi-object delete carries
a body-matching, SigV4-signed `Content-MD5`. Dedicated `s3+sg://` smoke cases run against the
configured S3-compatible endpoints while the main S3 matrix continues using
standard `s3://`. These checks validate scheme selection, copy, rename, and
non-regression behavior, but they are not a substitute for a smoke test on the
target StorageGRID version.
