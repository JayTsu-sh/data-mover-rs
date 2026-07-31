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
The runner image must preinstall `rustup`, Rust `1.95.0`, `clippy`, and
`rustfmt` for the `github-runner` account, with
`/home/github-runner/.cargo/bin` on `PATH`. Keeping these tools in the image
avoids a runtime dependency on `sh.rustup.rs`; the workflow still selects the
exact toolchain to keep builds reproducible.

`run-e2e.sh` exercises the complete directed copy matrix across Local, NFSv3,
NFSv4.1, and S3: 4 same-protocol paths plus 12 cross-protocol paths. Every case
uses an isolated payload and verifies the destination SHA-256 checksum. A
separate payload larger than 12 MiB is copied from NFSv4.1 to NFSv4.1 so normal
copy exercises multiple negotiated read and write requests, including the
session-limited effective `wsize`.

`run-resume-e2e.sh` exercises eight representative paths in two independent
processes: the four same-backend paths plus Local → S3, S3 → Local, NFSv3 → S3,
and S3 → NFSv4.1. This covers every source interval reader, every destination
resume writer, and the NAS ↔ S3 boundaries without duplicating the full copy
matrix. The first process writes a durable prefix without committing it. The
second process discovers the remaining range from the destination, transfers
only that range, commits, and verifies the final hash. NAS destinations resume
from a `.terrasync-part` file; S3 destinations resume the server-side multipart
upload through `ListMultipartUploads` and `ListParts`.

`run-integrity-e2e.sh` independently re-reads copied data through Local,
NFSv3, NFSv4.1, and S3. It covers all four same-backend paths plus four
representative NAS/S3 crossings, checks the large NFSv4.1 fixture using the
negotiated effective read size, proves that Quick mode does not read content,
and requires Full mode to report the exact first corrupted byte offset. The
lab has no CIFS endpoint, so CIFS remains covered by the shared stream contract
tests until a real SMB service is added.

The self-hosted runner must provide `LAB_S3_ACCESS_KEY` and
`LAB_S3_SECRET_KEY`. Credentials must not be committed or printed.

Nightly and release validation set `AWS_S3_STORAGEGRID_COMPAT=true`. The lab
does not currently contain a real StorageGRID system: request-level tests use a
capturing Smithy connector to verify that the SDK normally generates `x-id`
and that compatibility mode removes it from the transmitted URI. The S3 lab
matrix then runs against the configured S3-compatible endpoints to detect
copy, rename, multipart, and resumable-transfer regressions while the mode is
enabled. These checks validate the workaround's protocol behavior and
non-regression properties, but they are not a substitute for a smoke test on
the target StorageGRID version.
