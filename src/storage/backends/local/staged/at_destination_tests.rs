use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::{StreamExt as _, stream};

use super::*;
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity, SourceVersion,
};
use crate::storage::{
    ByteStream, FinalDestination, PrepareFact, PrepareRequest, PublishRequest, RestartReason,
    ResumeMode, SourceDescriptor, StagedDestination,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const IDENTITY: [u8; 32] = [4; 32];
/// The Local test checkpoint interval: four 64 KiB pieces.
const CHECKPOINT: usize = 4 * 64 * 1024;

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct Root(PathBuf);

impl Root {
    fn new() -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "data-mover-local-at-destination-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(path.join("dir"))?;
        Ok(Self(path))
    }

    fn artifact(&self, kind: ArtifactKind) -> PathBuf {
        self.0.join("dir").join(artifact_name("final.bin", kind))
    }

    fn artifacts_left(&self) -> io::Result<Vec<String>> {
        let mut left = Vec::new();
        for entry in std::fs::read_dir(self.0.join("dir"))? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if name.starts_with(".data-mover-") {
                left.push(name);
            }
        }
        Ok(left)
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn backend() -> Result<BackendIdentity, Box<dyn std::error::Error>> {
    Ok(BackendIdentity::new(
        BackendKind::Local,
        "local-at-destination",
    )?)
}

fn adapter(root: &Root) -> Result<LocalStagedDestination, Box<dyn std::error::Error>> {
    Ok(LocalStagedDestination::new(&root.0, backend()?, 2)?)
}

fn request(
    size: usize,
    binding: [u8; 32],
    resume: ResumeMode,
    recoverable: bool,
) -> Result<DestinationPrepareRequest, Box<dyn std::error::Error>> {
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new("dir/final.bin")?),
        source: SourceDescriptor {
            path: StoragePath::new("source.bin")?,
            kind: EntryKind::File,
            size: Some(size as u64),
            source_identity: SourceIdentity::new(
                backend()?,
                IdentityStrength::StableWithinBackend,
                b"source-file",
            )?,
            backend_fact: None,
            content_version: None,
            inline_timestamps: None,
            inline_mode: None,
            version: SourceVersion::Current,
        },
        recovery_binding: binding,
    };
    Ok(DestinationPrepareRequest::new(prepare, IDENTITY)
        .with_resume(resume)
        .with_recoverable(recoverable))
}

fn discover_request(size: usize) -> Result<DestinationPrepareRequest, Box<dyn std::error::Error>> {
    request(size, [7; 32], ResumeMode::Discover, true)
}

fn content(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| u8::try_from(index % 251).unwrap_or_default())
        .collect()
}

fn chunks(bytes: &[u8]) -> ByteStream {
    let pieces: Vec<_> = bytes
        .chunks(64 * 1024)
        .map(|piece| Ok(Bytes::copy_from_slice(piece)))
        .collect();
    Box::pin(stream::iter(pieces))
}

fn class(error: &StorageRoleFailure) -> Option<(FailureClass, Transience)> {
    match error {
        StorageRoleFailure::Entry(entry) => Some((entry.class(), entry.transience())),
        StorageRoleFailure::Session(_) => None,
    }
}

async fn publish(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
    bytes: &[u8],
) -> Result<(), StorageRoleFailure> {
    try_publish(adapter, stage, bytes)
        .await
        .map_err(|failure| failure.error)
}

async fn try_publish(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
    bytes: &[u8],
) -> Result<(), PublicationFailure> {
    adapter
        .publish(
            stage,
            PublishRequest {
                expected_size: bytes.len() as u64,
                expected_blake3: Some(*blake3::hash(bytes).as_bytes()),
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map(|_| ())
}

/// Writes the first checkpoint's worth of `bytes` and stops with the input still open, as a
/// process killed mid-transfer would: the stage and a pointer at the checkpoint remain.
async fn interrupt_after_one_checkpoint(root: &Root, bytes: &[u8]) -> TestResult {
    let adapter = Arc::new(adapter(root)?);
    let stage = Arc::new(
        adapter
            .prepare_at_destination(discover_request(bytes.len())?)
            .await?,
    );
    let input: ByteStream = Box::pin(
        chunks(&bytes[..CHECKPOINT + 64 * 1024])
            .chain(stream::pending::<Result<Bytes, StorageRoleFailure>>()),
    );
    let writing = {
        let (adapter, stage) = (Arc::clone(&adapter), Arc::clone(&stage));
        tokio::spawn(async move { adapter.write(&stage, input).await })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while adapter
            .observe_checkpoint(&stage)
            .await
            .map_or(0, |seen| seen.durable_prefix)
            < CHECKPOINT as u64
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    writing.abort();
    let _ = writing.await;
    drop(Arc::into_inner(stage).ok_or("stage still shared")?);
    Ok(())
}

#[tokio::test]
async fn a_fresh_stage_publishes_and_leaves_no_artifact() -> TestResult {
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let bytes = content(1000);
    let stage = adapter
        .prepare_at_destination(discover_request(bytes.len())?)
        .await?;
    assert_eq!(stage.prepare_fact, PrepareFact::Fresh);
    assert!(stage.at_destination);
    for kind in [
        ArtifactKind::Stage,
        ArtifactKind::Pointer,
        ArtifactKind::Claim,
    ] {
        assert!(root.artifact(kind).is_file(), "{kind:?}");
    }
    adapter.write(&stage, chunks(&bytes)).await?;
    publish(&adapter, &stage, &bytes).await?;
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, bytes);
    assert_eq!(root.artifacts_left()?, Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn a_new_process_resumes_from_the_pointers_durable_prefix() -> TestResult {
    let root = Root::new()?;
    let bytes = content(CHECKPOINT * 2 + 17);
    interrupt_after_one_checkpoint(&root, &bytes).await?;

    let adapter = adapter(&root)?;
    let stage = adapter
        .prepare_at_destination(discover_request(bytes.len())?)
        .await?;
    assert_eq!(
        stage.prepare_fact,
        PrepareFact::Resumed {
            bytes: CHECKPOINT as u64
        }
    );
    assert_eq!(stage.write_offset, CHECKPOINT as u64);
    // Whatever was written past the durable prefix is dropped before writing on.
    assert_eq!(
        std::fs::metadata(root.artifact(ArtifactKind::Stage))?.len(),
        CHECKPOINT as u64
    );
    adapter.write(&stage, chunks(&bytes[CHECKPOINT..])).await?;
    publish(&adapter, &stage, &bytes).await?;
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, bytes);
    assert_eq!(root.artifacts_left()?, Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn a_live_claim_refuses_a_second_prepare_until_the_stage_is_gone() -> TestResult {
    let root = Root::new()?;
    let (first, second) = (adapter(&root)?, adapter(&root)?);
    let held = first.prepare_at_destination(discover_request(10)?).await?;
    let refused = second
        .prepare_at_destination(discover_request(10)?)
        .await
        .err()
        .ok_or("a held claim must refuse a second prepare")?;
    assert_eq!(
        class(&refused),
        Some((FailureClass::Conflict, Transience::Transient))
    );
    first.discard(held).await?;
    let after = second.prepare_at_destination(discover_request(10)?).await?;
    assert_eq!(after.prepare_fact, PrepareFact::Fresh);
    second.discard(after).await?;
    assert_eq!(root.artifacts_left()?, Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn what_is_found_restarts_for_each_reason() -> TestResult {
    let bytes = content(CHECKPOINT * 2);
    let mut other_transfer = discover_request(bytes.len())?;
    other_transfer.transfer_identity = [5; 32];
    let cases: [(&str, DestinationPrepareRequest, RestartReason); 3] = [
        (
            "other transfer",
            other_transfer,
            RestartReason::OtherTransfer,
        ),
        (
            "restart",
            request(bytes.len(), [7; 32], ResumeMode::Restart, false)?,
            RestartReason::Requested,
        ),
        (
            "binding",
            request(bytes.len(), [8; 32], ResumeMode::Discover, true)?,
            RestartReason::BindingChanged,
        ),
    ];
    for (case, request, reason) in cases {
        let root = Root::new()?;
        interrupt_after_one_checkpoint(&root, &bytes).await?;
        let adapter = adapter(&root)?;
        let stage = adapter.prepare_at_destination(request).await?;
        assert_eq!(
            stage.prepare_fact,
            PrepareFact::Restarted { reason },
            "{case}"
        );
        assert_eq!(stage.write_offset, 0, "{case}");
        assert_eq!(
            std::fs::metadata(root.artifact(ArtifactKind::Stage))?.len(),
            0,
            "{case}"
        );
        adapter.discard(stage).await?;
    }
    Ok(())
}

/// Damage done to an interrupted transfer's artifacts.
type Damage = fn(&Root) -> io::Result<()>;

#[tokio::test]
async fn a_damaged_stage_or_pointer_restarts() -> TestResult {
    let bytes = content(CHECKPOINT * 2);
    let damage: [(&str, Damage, RestartReason); 4] = [
        (
            "truncated stage",
            |root| {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(root.artifact(ArtifactKind::Stage))?
                    .set_len(10)
            },
            RestartReason::StageBehindPointer,
        ),
        (
            "flipped pointer bit",
            |root| {
                let path = root.artifact(ArtifactKind::Pointer);
                let mut pointer = std::fs::read(&path)?;
                pointer[40] ^= 1;
                std::fs::write(path, pointer)
            },
            RestartReason::PointerCorrupt,
        ),
        (
            "untagged pointer",
            |root| {
                let path = root.artifact(ArtifactKind::Pointer);
                let mut pointer = DestinationPointer::decode(&std::fs::read(&path)?)
                    .map_err(|_| io::Error::other("decode"))?;
                pointer.extension = Bytes::new();
                std::fs::write(
                    path,
                    pointer.encode().map_err(|_| io::Error::other("encode"))?,
                )
            },
            RestartReason::PointerCorrupt,
        ),
        (
            "lost pointer",
            |root| std::fs::remove_file(root.artifact(ArtifactKind::Pointer)),
            RestartReason::StageWithoutPointer,
        ),
    ];
    for (case, damage, reason) in damage {
        let root = Root::new()?;
        interrupt_after_one_checkpoint(&root, &bytes).await?;
        damage(&root)?;
        let adapter = adapter(&root)?;
        let stage = adapter
            .prepare_at_destination(discover_request(bytes.len())?)
            .await?;
        assert_eq!(
            stage.prepare_fact,
            PrepareFact::Restarted { reason },
            "{case}"
        );
        adapter.discard(stage).await?;
        assert_eq!(root.artifacts_left()?, Vec::<String>::new(), "{case}");
    }
    Ok(())
}

/// A publication that fails after its commit keeps the claim, so the clean-up that follows still
/// owns the names and removes what is left; once the claim is gone, a clean-up removes nothing.
#[tokio::test]
async fn a_committed_publication_failure_is_cleaned_up_under_the_claim() -> TestResult {
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let bytes = content(100);
    let stage = adapter
        .prepare_at_destination(discover_request(bytes.len())?)
        .await?;
    adapter.write(&stage, chunks(&bytes)).await?;
    adapter
        .write_probe
        .fail_after_publication_commit
        .store(true, Ordering::SeqCst);
    let failed = try_publish(&adapter, &stage, &bytes)
        .await
        .err()
        .ok_or("the injected failure")?;
    assert!(failed.final_destination_changed);
    adapter.discard(stage).await?;
    assert_eq!(root.artifacts_left()?, Vec::<String>::new());
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, bytes);

    // A stage whose claim is gone leaves another prepare's artifacts alone.
    let released = adapter
        .prepare_at_destination(discover_request(10)?)
        .await?;
    released.release_claim();
    let second = self::adapter(&root)?;
    let other = second.prepare_at_destination(discover_request(10)?).await?;
    adapter.discard(released).await?;
    assert!(root.artifact(ArtifactKind::Stage).is_file());
    assert!(root.artifact(ArtifactKind::Claim).is_file());
    second.discard(other).await?;
    Ok(())
}

/// A crash after the stage became the final file but before the pointer went: the next prepare
/// finds a pointer without a stage and starts over (one re-copy, accepted by ADR-0006).
#[tokio::test]
async fn a_pointer_left_by_a_crash_after_publication_restarts() -> TestResult {
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let bytes = content(100);
    let stage = adapter
        .prepare_at_destination(discover_request(bytes.len())?)
        .await?;
    adapter.write(&stage, chunks(&bytes)).await?;
    adapter
        .write_probe
        .fail_after_publication_commit
        .store(true, Ordering::SeqCst);
    let failed = try_publish(&adapter, &stage, &bytes)
        .await
        .err()
        .ok_or("the injected failure")?;
    assert!(failed.final_destination_changed);
    assert!(root.artifact(ArtifactKind::Pointer).is_file());
    assert!(!root.artifact(ArtifactKind::Stage).exists());
    drop(stage);

    let stage = adapter
        .prepare_at_destination(discover_request(bytes.len())?)
        .await?;
    assert_eq!(
        stage.prepare_fact,
        PrepareFact::Restarted {
            reason: RestartReason::PointerWithoutStage
        }
    );
    adapter.discard(stage).await?;
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, bytes);
    Ok(())
}

#[tokio::test]
async fn a_leftover_pointer_temporary_is_overwritten() -> TestResult {
    let root = Root::new()?;
    std::fs::write(
        root.0
            .join("dir")
            .join(artifact_temporary_name("final.bin", ArtifactKind::Pointer)),
        b"half a pointer from a killed process, longer than any real one would be ....",
    )?;
    let adapter = adapter(&root)?;
    let bytes = content(10);
    let stage = adapter
        .prepare_at_destination(discover_request(bytes.len())?)
        .await?;
    assert_eq!(adapter.observe_checkpoint(&stage).await?.durable_prefix, 0);
    adapter.write(&stage, chunks(&bytes)).await?;
    publish(&adapter, &stage, &bytes).await?;
    assert_eq!(root.artifacts_left()?, Vec::<String>::new());
    Ok(())
}

/// A prepare removes every leftover named after its final file — even when it writes no pointer
/// of its own — and nothing named after another file.
#[tokio::test]
async fn a_prepare_sweeps_its_own_final_names_leftovers_only() -> TestResult {
    let root = Root::new()?;
    let dir = root.0.join("dir");
    let leftovers = [
        artifact_temporary_name("final.bin", ArtifactKind::Pointer),
        artifact_temporary_name("final.bin", ArtifactKind::Stage),
        artifact_name("final.bin", ArtifactKind::Checkpoint),
    ];
    for name in &leftovers {
        std::fs::write(dir.join(name), b"left by a crash")?;
    }
    let others = artifact_name("other.bin", ArtifactKind::Stage);
    std::fs::write(dir.join(&others), b"another file's stage")?;
    let adapter = adapter(&root)?;
    let stage = adapter
        .prepare_at_destination(request(10, [7; 32], ResumeMode::Discover, false)?)
        .await?;
    assert_eq!(stage.prepare_fact, PrepareFact::Fresh);
    for name in &leftovers {
        assert!(!dir.join(name).exists(), "{name}");
    }
    assert_eq!(std::fs::read(dir.join(&others))?, b"another file's stage");
    adapter.discard(stage).await?;
    assert_eq!(root.artifacts_left()?, vec![others]);
    Ok(())
}

/// A direct write cleans up what an interrupted checkpointed run left for its final file, is
/// refused while another stage of the file is live, and takes no claim when nothing is there.
#[cfg(unix)]
#[tokio::test]
async fn a_direct_write_clears_the_files_leftovers_first() -> TestResult {
    let bytes = content(CHECKPOINT * 2);
    let direct = || -> Result<PrepareRequest, Box<dyn std::error::Error>> {
        Ok(discover_request(bytes.len())?.prepare)
    };
    let cancel = tokio_util::sync::CancellationToken::new;

    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let fresh = adapter.prepare_direct(direct()?, cancel()).await?;
    assert_eq!(fresh.prepare_fact, PrepareFact::Fresh);
    assert!(!root.artifact(ArtifactKind::Claim).exists());
    adapter.discard(fresh).await?;

    interrupt_after_one_checkpoint(&root, &bytes).await?;
    let held = adapter
        .prepare_at_destination(discover_request(bytes.len())?)
        .await?;
    let refused = adapter
        .prepare_direct(direct()?, cancel())
        .await
        .err()
        .ok_or("a live stage must refuse a direct write")?;
    assert_eq!(
        class(&refused),
        Some((FailureClass::Conflict, Transience::Transient))
    );
    drop(held);

    let stage = adapter.prepare_direct(direct()?, cancel()).await?;
    assert_eq!(
        stage.prepare_fact,
        PrepareFact::Restarted {
            reason: RestartReason::Requested
        }
    );
    assert_eq!(root.artifacts_left()?, Vec::<String>::new());
    adapter.write(&stage, chunks(&bytes)).await?;
    publish(&adapter, &stage, &bytes).await?;
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, bytes);
    assert_eq!(root.artifacts_left()?, Vec::<String>::new());
    Ok(())
}

/// With no stage or pointer of the file there, a direct write takes no claim — a claim someone
/// else holds does not refuse it — and only a lone pointer temporary sends it through a clean-up.
/// A clean-up that fails for another reason leaves the leftovers and still writes in place.
#[cfg(unix)]
#[tokio::test]
async fn a_direct_write_takes_no_claim_unless_something_is_there() -> TestResult {
    let cancel = tokio_util::sync::CancellationToken::new;
    let root = Root::new()?;
    let adapter = adapter(&root)?;
    let claim = std::fs::File::create(root.artifact(ArtifactKind::Claim))?;
    claim.try_lock()?;
    let stage = adapter
        .prepare_direct(discover_request(10)?.prepare, cancel())
        .await?;
    assert_eq!(stage.prepare_fact, PrepareFact::Fresh);
    adapter.discard(stage).await?;
    drop(claim);
    std::fs::remove_file(root.artifact(ArtifactKind::Claim))?;

    let temporary = root
        .0
        .join("dir")
        .join(artifact_temporary_name("final.bin", ArtifactKind::Pointer));
    std::fs::write(&temporary, b"half a pointer")?;
    let stage = adapter
        .prepare_direct(discover_request(10)?.prepare, cancel())
        .await?;
    assert_eq!(stage.prepare_fact, PrepareFact::Fresh);
    assert!(!temporary.exists());
    adapter.discard(stage).await?;
    assert_eq!(root.artifacts_left()?, Vec::<String>::new());

    std::fs::create_dir(root.artifact(ArtifactKind::Pointer))?;
    let bytes = content(10);
    let stage = adapter
        .prepare_direct(discover_request(bytes.len())?.prepare, cancel())
        .await?;
    adapter.write(&stage, chunks(&bytes)).await?;
    publish(&adapter, &stage, &bytes).await?;
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, bytes);
    assert!(root.artifact(ArtifactKind::Pointer).is_dir());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlink_at_an_artifact_name_is_refused_and_its_target_untouched() -> TestResult {
    for kind in [
        ArtifactKind::Stage,
        ArtifactKind::Pointer,
        ArtifactKind::Claim,
    ] {
        let root = Root::new()?;
        let target = root.0.join("outside.bin");
        std::fs::write(&target, b"keep me")?;
        std::os::unix::fs::symlink(&target, root.artifact(kind))?;
        let refused = adapter(&root)?
            .prepare_at_destination(discover_request(10)?)
            .await
            .err()
            .ok_or("a symlinked artifact must be refused")?;
        assert_eq!(
            class(&refused).map(|(class, _)| class),
            Some(FailureClass::Conflict),
            "{kind:?}"
        );
        assert_eq!(std::fs::read(&target)?, b"keep me", "{kind:?}");
        if kind != ArtifactKind::Claim {
            assert!(!root.artifact(ArtifactKind::Claim).exists(), "{kind:?}");
        }
    }
    Ok(())
}

/// Nothing looks at the pointer's temporary before it is written, so a symlink there must not be
/// written through: it is removed, and the pointer is created in its place.
#[cfg(unix)]
#[tokio::test]
async fn a_symlink_at_the_pointer_temporary_is_replaced_not_followed() -> TestResult {
    let root = Root::new()?;
    let target = root.0.join("dir/sibling.bin");
    std::fs::write(&target, b"keep me")?;
    let temporary = root
        .0
        .join("dir")
        .join(artifact_temporary_name("final.bin", ArtifactKind::Pointer));
    std::os::unix::fs::symlink(&target, &temporary)?;
    let adapter = adapter(&root)?;
    let stage = adapter
        .prepare_at_destination(discover_request(10)?)
        .await?;
    assert_eq!(std::fs::read(&target)?, b"keep me");
    assert!(
        std::fs::symlink_metadata(root.artifact(ArtifactKind::Pointer))?
            .file_type()
            .is_file()
    );
    adapter.discard(stage).await?;
    assert_eq!(root.artifacts_left()?, Vec::<String>::new());
    Ok(())
}

/// Metadata applied before a crash may leave the stage read-only; resuming restores the owner's
/// write permission (publication applies the metadata again).
#[cfg(unix)]
#[tokio::test]
async fn a_read_only_stage_is_resumed() -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;
    let root = Root::new()?;
    let bytes = content(CHECKPOINT * 2);
    interrupt_after_one_checkpoint(&root, &bytes).await?;
    std::fs::set_permissions(
        root.artifact(ArtifactKind::Stage),
        std::fs::Permissions::from_mode(0o444),
    )?;
    let adapter = adapter(&root)?;
    let stage = adapter
        .prepare_at_destination(discover_request(bytes.len())?)
        .await?;
    assert_eq!(
        stage.prepare_fact,
        PrepareFact::Resumed {
            bytes: CHECKPOINT as u64
        }
    );
    adapter.write(&stage, chunks(&bytes[CHECKPOINT..])).await?;
    publish(&adapter, &stage, &bytes).await?;
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, bytes);
    Ok(())
}
