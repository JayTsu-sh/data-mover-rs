# CIFS negotiated signing integration

The new storage factory accepts `CifsBackendConfig.signing_policy` with the
backend-owned `CifsSigningPolicy::{WhenRequired, Required}` enum. Its default is
`WhenRequired`; the factory maps it through the public smb-rs facade. Server
requirements, SMB 3.1.1 TREE_CONNECT, authentication/binding, and encryption
integrity remain enforced by smb-rs. There is no independent verification-off
switch. Signed responses are still verified under the optional policy.

The dependency is pinned to `72c8d31438502221bdffaca358c221da664df6f3`, published
on `JayTsu-sh/smb-rs` branch `feat/negotiated-signing`:
[upstream draft PR #69](https://github.com/JayTsu-sh/smb-rs/pull/69).
The underlying smb-rs default remains Required; data-mover's ordinary CIFS
factory explicitly chooses the caller's policy. Existing legacy StorageEnum
behavior remains unchanged.

Validation:

- `cargo check --all-targets`: passed.
- `cargo test --lib`: 769 passed, 4 ignored.
- `cargo test --test backend_factory`: 2 passed.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed.
- Architecture dependency guard: passed; bounded architecture review found no blockers.
- Release real CIFS contract: passed. Both transfer policies tested Local→CIFS,
  CIFS→CIFS between the two FAS LIFs, and CIFS→Local with read-back verification
  enabled. Sizes were 4 KiB, 2 MiB + 1, and 64 MiB + 1. Checkpointed activated
  recovery above the 64 MiB interval; AtomicReplace did not.

The release benchmark exposes `--signing required|when-required`. The previous
signed mount comparison runner now explicitly selects Required so its recorded
security assumptions remain valid when rerun. Historical frozen binaries and
result snapshots were not modified.

[Same-binary signing performance comparison](../../benchmarks/cifs-signing-comparison/report.md)
contains the measured results and their interpretation boundaries.

The benchmark completed all 64 samples (48 measured); every sample passed external
SHA256 verification and cleanup checks. Temporary mountpoints were removed.
Upstream PR CI passed after commit `dcd99e73320449f7816b8beb80d2db0b14d6adda`
classified the shared SigningPolicy value in the architecture guard. That follow-up
changes only architecture documentation/rules; its runtime code is identical to
the dependency implementation commit pinned here.
