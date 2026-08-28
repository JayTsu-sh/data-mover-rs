# Architecture Acceptance Gates

Status: normative evidence specification

Authorities: `storage-transfer-architecture.md`, `backend-capability-matrix.yaml`,
`directed-transfer-matrix.yaml`

This document defines how a claim becomes evidence. It does not redefine architecture or
capabilities.

`current-real-environment-evidence.yaml` is a machine-validated historical ledger for the
eight profiles. It records current passed, failed, and missing evidence only; it is
non-normative and cannot create a compatibility or support requirement.

## 1. Acceptance levels

### ArchitectureReady

ArchitectureReady means implementation may begin. It requires:

- the four architecture-package files exist and parse/render;
- five backend kinds and eight validation profiles agree across all files;
- the directed matrix contains exactly 64 unique ordered pairs;
- every cell has byte-transfer, recovery, native, metadata, and namespace declarations;
- interfaces, state transitions, cancellation, failure, ownership, and migration are total;
- every Required claim maps to a gate below;
- no unresolved placeholder, blank state, ambiguous support claim, or duplicate authority exists.

ArchitectureReady does not claim the target implementation passes ReleaseReady.

### ReleaseReady

ReleaseReady means the exact candidate commit may be formally released. It requires all PR,
nightly, scheduled, consumer, device, and documentation gates applicable to every formally
supported profile. There are no manual waivers, `continue-on-error` exceptions, or flaky
retry-until-green acceptance. Invalid environment evidence is rerun for the same commit or
the profile/feature is formally reclassified experimental in both matrices and documentation.

## 2. Gate result record

Every real or scheduled run emits a durable report containing:

```text
gate_id, repository, exact_commit, dependency_commits, run_id,
source_profile, destination_profile, mode, fixture_set,
started_at, completed_at, outcome, artifact_links, environment_fingerprint
```

The two repositories share profile definitions, fixture identities, and run IDs. Each owns
its orchestration and report; a data-mover pass cannot satisfy a terrasync gate.

Credentials are CI secrets only. Every run uses isolated roots, shares/prefixes/buckets,
database names, and a unique run ID, followed by cleanup in an unconditional finalizer.

## 3. PR gates

| Gate | Required evidence |
|---|---|
| `DM-COMPILE` | data-mover all targets/features used by supported profiles compile |
| `DM-FMT-CLIPPY` | rustfmt and clippy with repository deny policy |
| `DM-UNIT` | model, codecs, capabilities, state transitions, metadata conversion, QoS |
| `DM-LOCAL-CONTRACT` | Local traversal, staging, recovery, integrity, metadata, failures |
| `DM-MATRIX-SCHEMA` | both YAML files parse; profiles and all 64 unique cells complete |
| `DM-ARCH-DEPS` | target module direction and no terrasync/product terminology violation |
| `DM-SECRET-SAFETY` | configs, errors, debug output, and snapshots do not expose secrets |
| `TS-COMPILE` | terrasync workspace compiles against the exact candidate data-mover commit |
| `TS-FMT-CLIPPY` | terrasync formatting and lint policy |
| `TS-UNIT` | snapshot codec use, DB query index, local/remote projections, loss policy |
| `TS-LOCAL-SMOKE` | pure scan, local copy, and in-process control flow on Local fixtures |

PR fault tests use deterministic adapters at the same public/expert seams as real backends.
They inject every lifecycle failure without testing private protocol representations.

## 4. Parameterized directed matrix gates

For each `gate_key` in `directed-transfer-matrix.yaml`, the following three independent
gates are mandatory:

| Gate family | Execution |
|---|---|
| `DM-DIRECTED/<gate_key>` | data-mover high-level transfer plus expert-seam equivalence |
| `TS-SINGLE/<gate_key>` | terrasync single-process real source and destination |
| `TS-REMOTE/<gate_key>` | terrasync sender and receiver as two OS processes over real QUIC |

Every family executes the functional fixture set and asserts:

- streaming remains bounded and `FinalDestination` is unchanged before publication;
- create and overwrite (old target smaller, equal, and larger) are correct;
- copy-time BLAKE3 covers complete logical content;
- independent byte-stream validation stops at the first mismatch/read failure;
- cancellation and injected source/destination/verification/publication failures leave no
  partial final destination and return truthful staged/recovery disposition;
- valid checkpoint resumes; missing/invalid/unavailable checkpoint restarts;
- entry failures and backend-session failures have different propagation;
- capability rejection happens before remote mutation;
- metadata and namespace result equals the cell declaration;
- source-only QoS is not bypassed or double-charged on client-streamed paths;
- native bytes, when used, are reported separately and explicitly unshaped.

Nightly runs all 192 parameterized gates (64 data-mover, 64 terrasync single-process, 64
terrasync two-process). Fixtures may be shared; result records may not.

## 5. Functional fixtures

With configured `chunk_size = C`, every functional cell includes:

```text
0, 1, C-1, C, C+1, 2*C+17 bytes
```

Additional fixtures:

- interruption inside an inflight/checkpoint interval and immediately after its durability
  barrier, with at least three intervals available;
- S3 object sizes on both sides of the single CopyObject/multipart-copy boundary;
- paths with spaces, Unicode, maximum supported length, and legal special characters;
- empty, deep, and wide directory trees plus an entry-level failure during traversal;
- overwrite targets smaller than, equal to, and larger than the source;
- metadata values that are preserved, mapped, dropped, unsupported, and fail to apply.

Heavy 1 GiB and 100 GiB fixtures belong to scheduled performance gates, not every functional
matrix cell.

## 6. Backend/profile gates

### Local

- `DM-LOCAL-CONTRACT`
- `TS-LOCAL-SINGLE`
- `TS-LOCAL-REMOTE`

The gate covers positional inflight writes, flush durability, rename/replace, ACL/xattr,
symlink, cancellation, resume, restart, and overwrite truncation.

### NFSv3

- `DM-NFS3-CONTRACT`
- `TS-NFS3-SINGLE`
- `TS-NFS3-REMOTE`

The gate covers mount/reconnect, stable-write durability, file handles, ACL/xattr, symlink,
rename/replace, safe RPC retry, and checkpoint recovery.

### NFSv4.0

- `DM-NFS40-CONTRACT`
- `TS-NFS40-SINGLE`
- `TS-NFS40-REMOTE`

The gate independently covers v4.0 state/reconnect, file handles, stable writes, ACL/xattr,
rename/replace, replay safety, and recovery. NFSv3 or v4.1 evidence cannot substitute.

### NFSv4.1

- `DM-NFS41-CONTRACT`
- `TS-NFS41-SINGLE`
- `TS-NFS41-REMOTE`

The gate independently covers v4.1 sessions/state, file handles, reconnect, stable writes,
ACL/xattr, rename/replace, replay safety, and recovery.

### CIFS / ONTAP FAS2750

Required gate order:

1. `SMB-FAS2750-NEGOTIATE`: smb-rs parses and completes SMB 3.1.1 negotiation;
2. `DM-CIFS-CONTRACT`: data-mover validates both FAS2750 LIFs, authentication, traversal,
   sequential read, positional/out-of-order staged write, flush and re-observation,
   interruption/resume, restart, rename/replace, ACL, cancellation, and failure isolation;
3. `TS-CIFS-SINGLE` and `TS-CIFS-REMOTE`: terrasync application workflows.

Environment identity:

```text
LIFs: 10.128.61.200, 10.128.61.201
share: ontap_lisaauto_cifs
credentials: CI secret only
```

System libsmbclient is not acceptable evidence for smb-rs/data-mover capability. CIFS and
CIFS resume remain Uncertified until all named gates pass.

### Standard-compatible S3

- `DM-S3-CONTRACT`
- `TS-S3-SINGLE`
- `TS-S3-REMOTE`

Covers range read, multipart staging/re-observation/restart, overwrite publication, tags,
delete, readback verification, and same-environment NativeTransfer planning/fallback.

### DXN S3

- `DM-DXN-CONTRACT`
- `TS-DXN-SINGLE`
- `TS-DXN-REMOTE`

Covers DXN delete checksum, verified rename/tag limitations, multipart resume/restart,
readback, native planning, and a standard-S3 isolation control proving the DXN adapter does
not alter ordinary S3 requests.

### HDFS

- `DM-HDFS-CONTRACT`
- `DM-HDFS-HA`
- `DM-HDFS-KERBEROS`
- `TS-HDFS-SINGLE`
- `TS-HDFS-REMOTE`

Covers continuous partial recovery, append/flush/re-observation, rename/replace, ACL/xattr,
HA failover, Kerberos, cancellation, memory budget, and safe retry.

### StorageGRID overlay

`DM-STORAGEGRID-REQUEST-CONTRACT` verifies exact `x-id` removal and required delete
`Content-MD5` without changing standard S3 requests. StorageGRID is not a ninth matrix
profile until a durable shared endpoint exists; the request contract remains a ReleaseReady
device overlay.

## 7. Recovery and failure gates

`DM-RECOVERY-FAULTS` and the matching terrasync modes inject:

- connection, authentication, and configured-root failure;
- traversal session loss and isolated entry permission/object failure;
- source open/read/range failure;
- destination prepare/write/persist failure, including partial out-of-order writes;
- corrupt, expired, tampered, source-mismatched, destination-mismatched, missing, and overlong
  checkpoint state;
- verification mismatch and publish/rename/complete-multipart failure;
- cancellation, process termination, reconnect, and restart;
- safe and unsafe protocol replay cases;
- QUIC disconnect and receiver termination/restart.

Every case asserts terminal state, failure stage and attribution, entry/session scope,
`FinalDestination`, staged-state disposition, and `RecoveryIdentity` validity. Merely
returning an error does not satisfy the gate.

## 8. Metadata and traversal gates

`DM-METADATA-MATRIX` enumerates all cell metadata declarations and verifies each
`preserved`/`mapped` result positively. `dropped_with_loss`, `unsupported`, and
`not_applicable` receive negative/preflight and loss-report tests. Application failures must
produce `failed`, never semantic loss.

`DM-TRAVERSAL-CONTRACT` verifies bounded backpressure, cancellation, optional
`ObservationPlan` modes, no default extra ACL/xattr/tag calls, ordered result delivery as
defined by the traversal request, entry failures as items, and backend-session termination.

Terrasync gates additionally verify:

- lossless opaque snapshot round-trip and query-index consistency;
- no backend refetch while reconstructing a stored observation;
- scan completeness blocks deletion and generation commit after any incomplete traversal;
- local work distribution does not introduce wire/NDX identity;
- remote paging assigns session-local NDX without treating it as persistent identity.

## 9. Integrity gates

`DM-INTEGRITY-CONTRACT` covers:

- copy-time BLAKE3 over complete logical content;
- resume/restart equivalence to fresh transfer;
- native guarantee classification and mandatory stream fallback when insufficient;
- independent dual-byte-stream comparison;
- first mismatching offset and read failure cause immediate cancellation of both streams;
- size, ETag, successful write, or native call alone never proves content equivalence.

## 10. Source-only QoS gates

`DM-SOURCE-QOS/<source_profile>` runs for all eight source profiles and verifies:

- soft average bandwidth, hard ceiling, bounded peak credit/duration;
- source-read IOPS only;
- sub-chunk grants and multi-range/inflight payload;
- one shared transfer-group budget across concurrent files and absence of starvation;
- immediate cancellation while awaiting a grant;
- actual retried source reads consume source IOPS again;
- resume charges only reread/new client payload; restart charges all rereads;
- destination writes/persist/publication are not charged;
- traversal, metadata, standalone validation, chunks, and channels are not silently charged.

Every directed cell verifies client-streamed payload is shaped and counted once. S3 native
copy remains eligible: its logical/native bytes and native requests are separate, its
server-internal payload is reported unshaped, and no opaque source/destination IOPS are
invented. A strict client-shaped request explicitly disables native transfer.

Terrasync single-process and two-process gates verify the shared limiter is applied on the
source/Sender side only and remains consistent with QUIC backpressure.

## 11. Performance gates

The pre-refactor reference is produced by
`tests/lab/run-performance-baseline.sh`. Its raw samples and JSON summary form
one evidence unit bound by `samples_sha256`; the reporter rejects mixed commit,
hardware, dataset, concurrency, requested chunk, or inflight conditions. The
nightly workflow signs both files with a Sigstore-backed GitHub artifact
attestation; a bare SHA-256 digest is not accepted as signed baseline evidence.

Before refactoring, capture a signed baseline on the same hardware, datasets, profile,
concurrency, chunk size, and inflight budget. `PERF-RELEASE` compares the exact candidate:

- large client-streamed throughput is at least 90% of baseline;
- small-file scan/copy entries per second are at least 90% of baseline;
- p95 entry scheduling latency degrades no more than 20%;
- peak buffered payload never exceeds configured inflight bytes;
- stable RSS does not grow linearly with file size;
- at identical concurrency/inflight settings, stable RSS differs by at most 10% between
  1 GiB and 100 GiB transfers;
- every internal channel has explicit capacity, backpressure, and cancellation.

Increasing default buffers, concurrency, or inflight capacity cannot be used to mask a
failure. HDFS additionally passes its profile memory budget and HA long-run gates.

## 12. Terrasync consumer gates

Terrasync must use the exact candidate data-mover revision and pass:

- full workspace compile, tests, formatting, and lint;
- observation snapshot/database/index round-trip;
- pure scan, complete/incomplete generation, and deletion safety;
- single-process local-copy projection for all 64 cells;
- real two-process QUIC projection for all 64 cells;
- paging, bounded backpressure, session-local index, terminal protocol, and error propagation;
- metadata policy/loss behavior, resume/restart, integrity, FinalDestination, and source QoS;
- absence of backend-fact/recovery parsing and backend-name transfer orchestration;
- enforced one-way dependency.

In-process transport tests supplement but never replace real two-process QUIC evidence.

## 13. Documentation and traceability gates

`ARCH-PACKAGE-CHECK` shall verify:

- Markdown links resolve and YAML parses;
- profile sets match and ordered cell count is exactly 64;
- every `gate_key` produces all three matrix gate families;
- every Required capability and cell maps to a gate;
- Unsupported and NotApplicable facts have a reason/semantic negative contract;
- Uncertified facts name a gate and cannot enter the formal support list;
- the legacy-disposition table covers every inventory cluster;
- no public interface exposes terrasync products or private backend facts;
- no file contains an unresolved placeholder, blank status, or ambiguous support claim;
- superseded QoS proposals do not survive: QoS is source-only and does not disable S3 native
  copy.

The exact release candidate must have a successful nightly and scheduled report. Historical
passes, passes from another commit, and library-only evidence cannot satisfy ReleaseReady.
