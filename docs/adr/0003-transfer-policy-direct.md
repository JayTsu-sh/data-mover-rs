# TransferPolicy and Local Direct writes

`TransferPolicy` replaces `RecoveryPolicy`: the choice controls write visibility, publication durability, and recovery. Callers use `with_transfer_policy(...)`. There are no old-name aliases.

| Policy | Target | Completion | Recovery | Final Local synchronization |
| --- | --- | --- | --- | --- |
| Checkpointed (default) | private stage | atomic rename | destination-selected threshold | data and parent directory |
| AtomicReplace | private stage | atomic rename | disabled | none |
| Direct | final file inode | validate completion and close | disabled | none |

Checkpointed and AtomicReplace preserve their previous behavior. Local Checkpointed still uses the independent 64 MiB interval and skips new recovery records for single-source-chunk files and files at or below that interval. Read-back remains optional for every policy. Metadata apply remains enabled independently of read-back and recovery.

Direct supports ordinary transfers to Unix Local regular files and HDFS files (see [ADR-0004](0004-hdfs-transfer-policies.md)). Other backends/platforms and the expert destination API reject it before opening a target. Existing final symlinks and non-regular objects are rejected. The final file is opened without following symlinks or truncating; its inode is compared with the described Local source identity (even across different backend names). A same-file or hard-link alias is rejected before truncation. The writer truncates before payload writes. Existing destination hard links observe the in-place modifications.

Direct reuses Local positional writes, the single-source-chunk fast path, multi-chunk inflight reads/writes, optional digest verification and metadata batching. Its prepared target has an explicit direct marker, a cached final-file descriptor and no stage token or recovery authority; no stage/checkpoint/claim files are created. Completion validates the length and final pathname's inode, then releases the handle without rename or persistence barriers. Read-back and metadata act on the opened descriptor.

Once a direct target has been opened, failures conservatively report `final_destination_changed() == true` (the target may have changed), including verification, metadata and cancellation failures. They never expose an unpublished stage or cleanup authority. Direct discard cannot remove the final file. Explicit cancellation drains submitted writes before returning; readers may observe partial contents throughout the attempt. Successful completion does not guarantee survival of a crash. Direct does not consult or delete previous staged recovery records.

`EffectiveRecovery` remains an inexpensive outcome field. `Disabled` replaces its former AtomicReplace variant and is shared by AtomicReplace and Direct. Checkpointed/skipped/native outcomes retain their meaning. It does not control I/O or introduce per-chunk work.

The Local comparison CLI uses `--transfer-policy checkpointed|atomic-replace|direct` and retains `--atomic-replace` as an exclusive shorthand. The NFS comparison CLI uses `--transfer-policy checkpointed|atomic-replace`. Historical benchmark snapshots keep their original APIs and labels.

## NFS Direct（2026-11-08）

NFS ordinary transfers now use the existing `prepare_direct`/`write`/`publish`/
`remove_failed_direct` contract. The final regular file is visible while copying;
there is no staging rename or checkpoint. The same-key transfer artifacts are checked
before opening the final file. Missing artifacts require no DELETE, so an existing
writable file in a read-only parent remains writable. Actual CREATE/OPEN returns the
file handle with the file object; a later pathname lookup cannot claim a replacement.

The opened handle is retained until writing and final close settle. Long existing
files are truncated only after complete input; empty input truncates to zero. Final
publication, verification, metadata application and failed-data cleanup recheck the
opened FH. A data failure may remove only the still-owned affected output; an
untouched existing target is retained. Issued writes with unchanged size/version are reported as uncertain instead of claiming
cleanup success. Required durable Direct writes use existing bounded receipt batches
rather than retaining all unstable payloads until end of file. Starting metadata work prevents data cleanup,
and successful publication releases cleanup authority. Discard closes the handle
and retains the final file. Generated streams still reject NFS Direct because their
multiple-source alias protection is not implemented.

After failed Direct writing or abandoned writes, CLOSE has a three-second cleanup
budget, rather than starting a second full protocol recovery wait. Timeout is typed
as Timeout/Unknown, keeps the preceding diagnostic, and cannot be reported as
confirmed target cleanup. This bound is not the whole transfer's detection budget:
real silent packet loss still takes about 45 seconds in the current SDK before
failed-write close, about 48 seconds total. Task-wide network control remains work
for the transfer scheduler and calling application. Normal Direct completion alone
does not authorize application source deletion; required post-metadata persistence
must be established separately through the namespace contract.

The earlier Local-only description above records its original implementation;
current backend capabilities and source code are authoritative for other backends.

## S3 Direct failed-data cleanup（2026-11-09）

Explicit `FailedDataCleanup::RemoveOwned` now applies to data verification failures
following Direct publication. Ordinary discard, the default Keep policy, and metadata
failures retain final output. The existing S3 staged role records the original write
receipt privately; a matching object inferred after an unacknowledged write is not
ownership proof. No-write refusals require no final-object cleanup.

Certified object buckets remove only a confirmed immutable version by its reported
ID, or a mutable/null slot after fresh size/ETag checks. Replaced output, missing
version echoes and uncertain write outcomes are retained with explicit typed errors.
NAS output is not certified for this cleanup. Concurrent writers remain excluded;
HEAD followed by mutable DELETE is not atomic. Failed deletion preserves the original
data failure and its cleanup cause; an unacknowledged DELETE remains Unknown.

The existing VerifyRequest cancellation token now covers in-flight read-only HEAD
and GET, avoiding a wait for SDK read timeout. Cleanup still has the protocol's
operation timeout and does not add retries. The isolated real sixty-case matrix is
documented in [the lab guide](../../tests/lab/README.md#s3-direct-failed-data-cleanup).

## S3 Direct created-version evidence

Direct publication now exports a destination version only from this attempt's original
successful write receipt. It reuses the existing private write claim introduced for
owned failed-data cleanup and the existing PublicationEvidence/TransferOutcome fields;
no public function or type is added. A failed CopyObject can find preexisting identical
data, and an acknowledged copy with no version header can infer a stale version from
LIST. Those content checks do not prove that the inferred version was newly created.
The data may be usable while destination_version remains None. Historical callers must
require a confirmed mapping and block later versions when publication is unconfirmed.

This change applies to Direct single/native, multipart and streamed paths. Existing
AtomicReplace/Checkpointed inference and checkpoint recovery are a separate remaining
step before enabling application-wide historical source purge. It adds no normal
storage request or retained per-entry state; publication reads the existing bounded claim.
