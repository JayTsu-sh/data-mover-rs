# HDFS transfer policies and bounded streaming

The ordinary `transfer` entry accepts `TransferPolicy::Checkpointed`, `AtomicReplace`, and
`Direct` for HDFS destinations. Existing expert recovery and metadata APIs remain available.

| Policy | Payload target | Checkpoint boundary | Publication |
| --- | --- | --- | --- |
| Checkpointed | exclusive sibling `.part` file | keep one writer open; `hsync` each 64 MiB prefix, confirm its visible length, register after the first useful prefix | overwrite rename |
| AtomicReplace | exclusive sibling `.part` file | none | close then overwrite rename |
| Direct | final path, recreated with overwrite | none | close; no stage or rename |

Superseded in part by [ADR-0006](0006-destination-resident-recovery.md): since C12c the staged
policies write a deterministic `.data-mover-<digest>.stage` beside the final file and record their
checkpoints in a `.pointer` there, nothing is registered in a local recovery store, and a resume
forces lease recovery on the stage found at the destination; the `.part` stage, the recovery
identity and the claim rename described below were removed in C12d. The hsync and lease-recovery
mechanics below still apply.

Checkpoint eligibility matches Local/NFS: the file must be larger than 64 MiB and span more
than one effective source chunk. Exactly 64 MiB creates no recovery registration. The final
writer closes normally without registering a new checkpoint. The shared private recovery
store persists the opaque binding and stage location only after a synchronized prefix exists.
HDFS uses the synchronized file's server-side length as its prefix record; it does not need a
second remote `.checkpoint` file. Its existing exclusive claim rename is retained. After acquiring
the claim, recovery calls the NameNode `recoverLease` RPC for that stage and polls it every 250 ms
for up to 120 seconds. Once the RPC reports that the file is closed, the adapter observes its
stable length and starts the subsequent append there. Each stage has an independent lease and can
recover concurrently under the caller's existing transfer-concurrency bound. The forced recovery
does not wait for the old client's 60-second lease soft limit, although DataNode block recovery can
still take time. This prevents bytes accepted after the last recorded checkpoint from being
appended twice after a process kill. The last `hsync` prefix remains the minimum durable recovery
point; a longer tail retained by HDFS lease recovery may also be reused. Source path, size and
modification time remain bound into recovery identity.

HDFS is append-oriented. Application writes remain ordered; source reads and packet delivery
can overlap, but the adapter does not issue arbitrary positional writes to the file. One writer
receives the complete ordered byte stream. At each 64 MiB boundary, `hsync` marks the current
packet with `sync_block`, waits for its DataNode pipeline ACK, updates the NameNode length through
`fsync`, and then the adapter confirms that exact length before registering recovery. It does not
collect 64 MiB into a separate buffer or introduce a second channel/reorder queue. Ordinary input
errors preserve their error after closing the writer; a process or host failure leaves the last
successful `hsync` prefix as recovery truth.

## Chunk planning and inflight

Read and write role limits are independent, both currently 2 MiB. The effective read size is
bounded by the source limit and the transfer's chunk/byte/operation budgets. Destination writes
split larger input chunks into `Bytes` slices, without copying the complete source buffer.
Smaller source chunks can be passed directly to the native writer; HDFS packetization handles
its checksum and packet boundaries. HDFS does not negotiate NFS-style `rsize` and `wsize`:
the native client obtains server defaults and handles packet and filesystem block boundaries.
The HDFS filesystem block size (typically 128 MiB) is not an application I/O chunk limit.

The source opens one native reader per stream and shares it across independent positional
reads. Read depth is bounded by backend read concurrency and the transfer limits. Reservations
are acquired before reads allocate data, survive ordered submission, and are released according
to the shared runtime's payload and operation lifecycle. Retries use the same opened reader.

## Durability and Direct semantics

The pinned `hdfs-native` revision `373739f` provides public asynchronous and synchronous
`FileWriter::hsync` plus the explicit `Client::recover_lease` API. Its replicated-block
implementation waits for the exact sync packet ACK and then calls the NameNode `fsync` RPC while
retaining the writer and pipeline. Erasure-coded writers reject `hsync`, matching Hadoop's lack of
an equivalent striped-output implementation. Close still uses a synchronized last packet, so
AtomicReplace and Direct retain their final close durability. Closing an append session that writes
no new bytes supplies the existing last block to the NameNode `complete` RPC; that remains a
defensive append behavior, while checkpoint recovery normally stabilizes abandoned writers through
the explicit lease RPC. NameNode completion and rename provide the namespace operations; the
adapter does not invent a Local directory `fsync` equivalent. The dependency contract checks the
public lease-recovery and hsync assumptions.

The pin later moved (2026-09-23) to upstream `kimahriman/hdfs-native` master `695fc47`, which
contains the fork's hsync and lease-recovery commits; `client.rs`, `file.rs` and
`hdfs/block_writer.rs` are byte-identical to `373739f`, so the evidence below still applies.

Evidence: [native client](https://github.com/JayTsu-sh/hdfs-native/blob/373739fc0fb69f3f3cd2a32db58170d8cf83a514/rust/src/client.rs),
[native file writer](https://github.com/JayTsu-sh/hdfs-native/blob/373739fc0fb69f3f3cd2a32db58170d8cf83a514/rust/src/file.rs),
[native block writer](https://github.com/JayTsu-sh/hdfs-native/blob/373739fc0fb69f3f3cd2a32db58170d8cf83a514/rust/src/hdfs/block_writer.rs),
and [Apache DataNode handling](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java)
(`syncBlock` triggers synchronization before the success acknowledgment).

HDFS Direct uses one create/write/close lifecycle for the final file, without an empty-file
close and reopen. It recreates the file rather than preserving the old inode as Local Direct does.
Preparation itself is non-destructive. Source opening and the first input result precede
creation. A same-path request within the same backend identity is rejected. Callers must not
represent the same physical namespace with unrelated identities for a self-copy. Once writing
begins, readers can observe partial contents. Failures report possible final changes and do
not provide stage-discard authority. Cancellation before publication prevents rename; Direct
has already exposed its writes. Optional read-back and explicit metadata application operate
on the final path in Direct and on the stage for the other policies.

Ordinary Local/NFS-to-HDFS transfers copy mode and modified time before publication. Numeric
source uid/gid values are not treated as HDFS principal names: the metadata plan reports that
owner/group mapping loss and retains the owner/group assigned by HDFS. HDFS timestamp application
uses millisecond precision and reports any source precision reduction.

## Validation

Contract tests cover asymmetric read/write limits, actual overlapping source reads, all three
admission limits, overwrite, same-path rejection, the exact 64 MiB eligibility boundary, and
resumption after a failure following the first synchronized prefix. The real-cluster test is
`policy_runtime::nightly_lab_transfer_policies_and_chunk_boundaries` in `hdfs_native_contract`;
it checks all three policies at 4 KiB, 2 MiB + 1, and 64 MiB + 1, with read-back enabled.
`nightly_lab_recovers_closed_hdfs_prefix` additionally injects an input cancellation, resumes the
real HDFS stage from a fresh connection (since ADR-0006 C12d through `prepare_at_destination`,
which must report `Resumed`), appends only the suffix, checks the published content, and checks
that no `.data-mover-*` artifact is left.

On 2026-09-15 the working tree based on `fe0f35f` passed the real HA/Kerberos tests against
the configured `hdfs-ha` namespace: all nine policy/size combinations and interrupted-prefix
recovery passed. The final HDFS unit/contract filter passed 96 tests; all-target Clippy,
architecture guards, the native hsync contract, and Windows GNU library cross-compilation
also passed. `make ci` passed the full suite before the final Direct lifecycle simplification;
that simplification was then rechecked by those HDFS tests, real-cluster tests, Clippy and Windows
compilation. The full suite's 751 library tests passed, with four intentionally ignored tests.

The process-death recovery path was revalidated on the same date with a 512 MiB + 137 byte Local
source. The first process was sent `SIGKILL` after its first checkpoint and a new process started
immediately. HDFS lease recovery stabilized an 80,140,288-byte prefix; the replacement process read
only the remaining 456,730,761 bytes, published the exact 536,871,049-byte file, removed its recovery
record, and produced the same SHA-256 as the source. The updated HDFS filter passed 97 tests, the
complete library suite passed 760 tests with four ignored, and all-target/all-feature Clippy passed
with warnings denied.

On 2026-09-16 the explicit lease-recovery path was exercised with two writers in one process against
the same HA/Kerberos namespace. Both 268,435,593-byte Local sources had a registered 67,108,864-byte
stage when the process was sent `SIGKILL`. One replacement process recovered both paths concurrently,
reused 81,753,088 and 85,236,736 bytes respectively, and finished both copies in 3,434 ms wall time.
The replacement read 186,682,505 and 183,198,857 source bytes, left no per-transfer recovery records,
and produced exact source SHA-256 values and lengths for both final files. Thus independent writers
use independent lease recovery and do not incur the old client's 60-second soft lease period per
file. The complete library suite passed 761 tests with four ignored, the HDFS filter passed 98 tests,
the upstream dependency contract passed, and all-target/all-feature Clippy passed with warnings
denied.
