# TransferPolicy and Local Direct writes

`TransferPolicy` replaces `RecoveryPolicy`: the choice controls write visibility, publication durability, and recovery. Callers use `with_transfer_policy(...)`. There are no old-name aliases.

| Policy | Target | Completion | Recovery | Final Local synchronization |
| --- | --- | --- | --- | --- |
| Checkpointed (default) | private stage | atomic rename | destination-selected threshold | data and parent directory |
| AtomicReplace | private stage | atomic rename | disabled | none |
| Direct | final file inode | validate completion and close | disabled | none |

Checkpointed and AtomicReplace preserve their previous behavior. Local Checkpointed still uses the independent 64 MiB interval and skips new recovery records for single-source-chunk files and files at or below that interval. Read-back remains optional for every policy. Metadata apply remains enabled independently of read-back and recovery.

Direct currently supports ordinary transfers to Unix Local regular files. Other backends/platforms and the expert destination API reject it before opening a target. Existing final symlinks and non-regular objects are rejected. The final file is opened without following symlinks or truncating; its inode is compared with the described Local source identity (even across different backend names). A same-file or hard-link alias is rejected before truncation. The writer truncates before payload writes. Existing destination hard links observe the in-place modifications.

Direct reuses Local positional writes, the single-source-chunk fast path, multi-chunk inflight reads/writes, optional digest verification and metadata batching. Its prepared target has an explicit direct marker, a cached final-file descriptor and no stage token or recovery authority; no stage/checkpoint/claim files are created. Completion validates the length and final pathname's inode, then releases the handle without rename or persistence barriers. Read-back and metadata act on the opened descriptor.

Once a direct target has been opened, failures conservatively report `final_destination_changed() == true` (the target may have changed), including verification, metadata and cancellation failures. They never expose an unpublished stage or cleanup authority. Direct discard cannot remove the final file. Explicit cancellation drains submitted writes before returning; readers may observe partial contents throughout the attempt. Successful completion does not guarantee survival of a crash. Direct does not consult or delete previous staged recovery records.

`EffectiveRecovery` remains an inexpensive outcome field. `Disabled` replaces its former AtomicReplace variant and is shared by AtomicReplace and Direct. Checkpointed/skipped/native outcomes retain their meaning. It does not control I/O or introduce per-chunk work.

The Local comparison CLI uses `--transfer-policy checkpointed|atomic-replace|direct` and retains `--atomic-replace` as an exclusive shorthand. The NFS comparison CLI uses `--transfer-policy checkpointed|atomic-replace`. Historical benchmark snapshots keep their original APIs and labels.
