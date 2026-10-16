# Checkpointed and AtomicReplace transfer policies

The public type is now `TransferPolicy`; [ADR-0003](0003-transfer-policy-direct.md) adds Local `Direct` and defines the current API. The two staged policies below retain their behavior.

Accepted 2026-09-10. Supersedes the policy names and unconditional final durability in ADR-0001.

The public TransferPolicy has two variants: Checkpointed (default, formerly Auto) and AtomicReplace (formerly Quick). This rename preserves the existing behavior and thresholds. EffectiveRecovery::Disabled uses the new name too. EffectiveRecovery::Checkpointed indicates that the transfer actually retained durable intermediate progress; selecting TransferPolicy::Checkpointed does not force checkpoints for ineligible files.

Ordinary Local Checkpointed uses a deferred checkpoint policy: a multi-source-chunk file must be strictly larger than the destination interval, currently 64 MiB. Its first checkpoint synchronizes the completed data prefix before creating recovery authority and records. Below/equal to the interval and single-source-chunk copies do not create new recovery records. Checkpointed always keeps final data synchronization and publication-directory synchronization, even without checkpoints. EOF skips redundant periodic checkpoint creation; existing recovery records receive final completed progress.

AtomicReplace never creates new checkpoints and clears prior registered work through the existing controlled restart lifecycle. Local AtomicReplace creates a stage in the final parent, transfers using the single-chunk fast path or multi-chunk inflight pipeline, checks completion/cancellation, and atomically publishes by rename (or the existing policy-specific publication operation). It omits the final file-data synchronization and publication-directory synchronization, including directory checks used only for that barrier. Fresh successful multi-chunk writes also skip the now-unneeded final sync task. Length normalization is retained for repeat/failure paths when required.

The engine carries publication durability on the prepared stage independently of recovery registration. An Checkpointed stage below the checkpoint threshold is still durable; an ephemeral stage must not imply AtomicReplace. Low-level/direct-role stages default to durable publication. Expert AtomicReplace also carries the relaxed Local publication setting. Other destinations retain their protocol-required operations; AtomicReplace is not a promise that every protocol can omit server persistence.

Read-back verification remains an independent option. AtomicReplace may verify content but verification does not guarantee persistence after a crash. WriteEvidence::persisted_bytes denotes backend-completed bytes under the requested publication policy; it is not by itself evidence of a recoverable checkpoint in AtomicReplace. Atomic rename keeps live-reader publication atomic, while crash durability is deliberately not promised.

Local write-ceiling follow-up (2026-09-10): the internal positional-write task ceiling is 8 MiB.
Local accepts larger upstream chunks and splits them into zero-copy `Bytes` views of at most 8 MiB;
it does not aggregate smaller source chunks. The Checkpointed checkpoint interval remains an independent,
fixed 64 MiB policy value, so tuning write scheduling cannot change recovery eligibility or cadence.

Local metadata follow-up (2026-09-10): staged metadata mutations are applied as one ordered batch.
Local reuses the stage file descriptor and performs ownership, mode, and mtime changes in one
blocking task. Checkpointed issues one metadata `fsync` after the complete batch; AtomicReplace issues none. The
generic staged-destination default retains per-mutation behavior for other backends. Batch failures
retain both the failing mutation index and the number of mutations already completed.

Local does not advertise or claim ctime preservation. Linux ctime is the kernel-maintained inode
status-change time and has no supported setter: applying ownership, mode, or mtime sets the target
ctime to the operation time. Consequently Checkpointed and AtomicReplace produce a fresh target ctime as part of
the metadata update but cannot copy the source ctime value. A private xattr is not treated as ctime.

Local entry-kind follow-up (2026-09-10): callers use one entry-copy interface; entry-kind-specific
behavior remains inside the Local implementation. Regular files retain the staged byte-transfer
and recovery lifecycle described above. Directories have no per-entry data stage or checkpoint.
The backend exposes directory creation and directory metadata application independently; it does not
wait for children or impose a deep-to-shallow order. Callers choose when to apply uid/gid/mode/mtime.
Symbolic links have no byte
stream, data stage, or checkpoint. Their link target and metadata are always read and written without
following the link. Linux Local copies symlink uid/gid with `lchown` and mtime with no-follow
timestamp operations; symlink mode is not advertised because Linux does not support setting it.
When replacing an existing symlink atomically, Local may create a hidden sibling symlink and rename
it into place. That temporary namespace entry is publication state only, not a recoverable stage.
The current entry-copy behavior is fixed to Overwrite rather than exposing destination-policy
branches. Local first attempts to create a missing final symlink
directly; only EEXIST uses a temporary sibling plus rename. atime remains omitted and ctime remains
kernel-maintained for every entry kind.

Rust callers migrate TransferPolicy::Auto to TransferPolicy::Checkpointed and TransferPolicy::Quick to TransferPolicy::AtomicReplace. The comparison examples accept checkpointed|atomic-replace (--transfer-policy for both Local and NFS); the Local example also accepts --atomic-replace, conflicting with --transfer-policy. Old policy names and CLI selectors are removed. Historical benchmark source snapshots/results retain their original policy labels and are not rewritten.

Validation covers single- and multi-source-chunk copies, exact final sync counts (Checkpointed 1/1, AtomicReplace 0/0 on ordinary non-checkpointed Local copies), AtomicReplace checkpoint omission above an injected threshold, content verification, artifact cleanup, existing restart recovery cleanup, and cancellation/publication tests.

NFS policy follow-up (2026-09-11): NFS uses the same fixed 64 MiB Checkpointed eligibility
interval as Local, independently of negotiated `rsize`, `wsize`, and the configured transfer
chunk. Files at or below the interval, and files fitting one effective source chunk, create no
recovery record. Larger multi-source-chunk copies begin without recovery authority, drain the
current write window when crossing the first interval, COMMIT the contiguous prefix, then register
the opaque NFS stage identity. After each interval, NFS atomically replaces a per-stage checkpoint
file only after the contiguous prefix has passed the required COMMIT barrier. Once registered, the
existing bounded UNSTABLE WRITE batch path may COMMIT more frequently than 64 MiB to release
verifier-dependent retry payloads; those extra COMMITs do not advance recovery until the next
checkpoint file replacement.

NFS Checkpointed sends UNSTABLE WRITEs and completes the final durability batch before closing and atomically
renaming the stage, including copies below the recovery threshold. The batch issues COMMIT only when
the server replies below `FILE_SYNC`. NFS AtomicReplace sends UNSTABLE WRITEs without
retaining verifier retry batches, closes without COMMIT, and atomically renames the stage. AtomicReplace
still supports optional read-back verification, but neither successful verification nor rename
promises survival of an NFS server crash. AtomicReplace first clears an owned prior recovery record through
the common restart lifecycle. Both modes retain bounded concurrent reads and writes and split writes
at the negotiated NFS maximum.

NFS server commitment follow-up (2026-09-11): every WRITE records the server-reported commitment.
`FILE_SYNC` replies contribute no periodic-COMMIT pressure and retain only their protocol outcome,
not the payload bytes needed for verifier recovery. `UNSTABLE` and `DATA_SYNC` replies retain their
payload and drive the bounded batch COMMIT threshold. A checkpoint still passes all retained
outcomes through `commit_write_batch`, allowing ordinary all-`FILE_SYNC` batches to complete without
a COMMIT RPC while preserving pNFS layout synchronization. If a write verifier changes, only ranges
whose replies still required a COMMIT are rewritten. Checkpointed keeps the fixed recovery boundary even
when all replies are already `FILE_SYNC`; the boundary then registers already-durable progress.
Recovery trusts the integrity-checked checkpoint prefix instead of the observed stage length and
truncates any later inflight tail before resuming. NFS places the stage and checkpoint beside the
final file. Every stage name carries an immutable random stage ID; claim rename changes only the
claim suffix and preserves that ID. The checkpoint path is derived from the ID, so recovery identity
needs to carry only the current stage token. Unique IDs prevent competing attempts for one binding
from overwriting or deleting each other's checkpoint files. NFS namespace traversal reserves and
omits the `.data-mover-` siblings. Publication and owned-stage discard remove the checkpoint; Checkpointed
below the eligibility threshold and AtomicReplace issue no checkpoint cleanup.


Local/NFS artifact naming follow-up: new stages use the shared
`.data-mover-<destination-path-hash-16>-<uuid-32>.stage` base name in the final parent.
Checkpoints append `.checkpoint`; checkpoint update temporaries append `.tmp-<uuid-32>`.
Local retains its `.claim` file lock. NFS claim rename appends `.claim-<claim-id-32>`
to the stage base name, while its checkpoint name remains based on the unchanged base.
RecoveryIdentity still carries one current stage token. Only the unified naming format is
accepted for recovery; legacy stage names and the old centralized staging layout are rejected.


Automatic metadata follow-up (2026-09-14): ordinary `transfer()` copies numeric uid/gid,
mode and mtime for supported Local/NFS source and destination pairs. The NFS metadata
observation carries the file handle returned with the attributes and compares it to the
source descriptor before accepting the metadata. Required ownership with unknown uid/gid
fails instead of substituting root. The common engine applies metadata to the stage before
publication for Checkpointed and AtomicReplace; callers must not repeat metadata application
after transfer. Metadata failure prevents publication. Atime, ctime, ACLs and xattrs are outside
this default baseline copy.

Local and NFS both reference `DEFAULT_CHECKPOINT_INTERVAL_BYTES` (64 MiB) from the backend
facade. Files at or below 64 MiB create no recovery records. Larger multi-source-chunk files
become eligible, registering recovery when the first checkpoint is persisted. AtomicReplace
never enables checkpoint recovery. This shared constant replaces two equal private constants;
it does not change either backend's interval or the strict greater-than eligibility rule.

NFS scheduling follow-up (2026-09-14): the write consumer polls available writes
alongside source input, so the configured inflight depth is an upper bound rather
than a batch-start threshold. Input admission stops at the write limit or a
checkpoint boundary; shared Bytes slices retain zero-copy splitting. On a periodic
checkpoint the current window drains and the existing data durability barrier
completes before creating its recovery record. Record persistence and first-time
registration may then overlap the next write window. At most one record operation
is pending; it must settle before the next periodic data barrier and before final
close, error truncation, or publication. Failures stop input admission and drain
already issued writes. FILE_SYNC/UNSTABLE handling, write-verifier validation and
retry payload retention remain owned by the existing NFS file implementation;
this change does not overlap the periodic COMMIT barrier with the next window or
advance a recovery prefix before its data is durable.

NFS commit cadence follow-up (2026-09-14): when a stage has a periodic checkpoint
plan, both pre-registration and registered/recovered writes retain receipts until
that checkpoint's data barrier. Registration must not switch these writes to the
non-periodic inflight-count COMMIT threshold. The periodic barrier still drains
issued writes, confirms the entire contiguous prefix durable using the existing
COMMIT/verifier/retry implementation, and only then persists its recovery record.
Final tails retain the existing final data barrier. Non-periodic recoverable writes
retain their bounded unstable batching behavior. FILE_SYNC receipt handling and
AtomicReplace behavior are unchanged; periodic UNSTABLE writes may retain retry
payloads for one checkpoint window instead of one inflight batch.
