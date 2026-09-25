use std::sync::atomic::Ordering;

use futures::stream;

use super::*;
use crate::model::{BackendIdentity, EntryKind, IdentityStrength, SourceIdentity, SourceVersion};
use crate::storage::backends::nfs::staged::tests::{FakeProtocol, adapter};
use crate::storage::{
    FinalDestination, PrepareFact, PrepareRequest, PublishRequest, RestartReason, ResumeMode,
    SourceDescriptor, StagedDestination,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const IDENTITY: [u8; 32] = [4; 32];
const BINDING: [u8; 32] = [7; 32];

fn request(
    backend: &BackendIdentity,
    size: usize,
    binding: [u8; 32],
    resume: ResumeMode,
) -> Result<DestinationPrepareRequest, Box<dyn std::error::Error>> {
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new("dir/final.bin")?),
        source: SourceDescriptor {
            path: StoragePath::new("source.bin")?,
            kind: EntryKind::File,
            size: Some(size as u64),
            source_identity: SourceIdentity::new(
                backend.clone(),
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
        .with_recoverable(true))
}

fn name(kind: ArtifactKind, temporary: bool) -> String {
    let artifact = if temporary {
        artifact_temporary_name("final.bin", kind)
    } else {
        artifact_name("final.bin", kind)
    };
    format!("dir/{artifact}")
}

fn artifacts_left(protocol: &FakeProtocol) -> Vec<String> {
    let mut left: Vec<_> = protocol
        .files
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .keys()
        .filter(|key| key.contains(".data-mover-"))
        .cloned()
        .collect();
    left.sort();
    left
}

fn file(protocol: &FakeProtocol, path: &str) -> Option<Vec<u8>> {
    protocol
        .files
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(path)
        .cloned()
}

fn put(protocol: &FakeProtocol, path: &str, bytes: Vec<u8>) {
    protocol
        .files
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(path.to_owned(), bytes);
}

async fn write(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
    bytes: &[u8],
) -> Result<(), StorageRoleFailure> {
    let input = Box::pin(stream::iter([Ok(Bytes::copy_from_slice(bytes))]));
    adapter.write(stage, input).await.map(|_| ())
}

async fn publish(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
    bytes: &[u8],
) -> Result<(), crate::storage::PublicationFailure> {
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

fn class(error: &StorageRoleFailure) -> Option<FailureClass> {
    match error {
        StorageRoleFailure::Entry(entry) => Some(entry.class()),
        StorageRoleFailure::Session(_) => None,
    }
}

#[tokio::test]
async fn a_fresh_stage_publishes_and_leaves_no_artifact() -> TestResult {
    let (adapter, protocol, backend) = adapter();
    let bytes = b"payload".to_vec();
    let stage = adapter
        .prepare_at_destination(request(
            &backend,
            bytes.len(),
            BINDING,
            ResumeMode::Discover,
        )?)
        .await?;
    assert_eq!(stage.prepare_fact, PrepareFact::Fresh);
    assert_eq!(
        artifacts_left(&protocol),
        [
            name(ArtifactKind::Pointer, false),
            name(ArtifactKind::Stage, false)
        ]
    );
    write(&adapter, &stage, &bytes).await?;
    publish(&adapter, &stage, &bytes)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(file(&protocol, "dir/final.bin"), Some(bytes));
    assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    Ok(())
}

/// A new process — a new adapter over the same server — resumes from the pointer's prefix and
/// drops whatever lies past it.
#[tokio::test]
async fn a_new_adapter_resumes_from_the_pointers_prefix() -> TestResult {
    let (first, protocol, backend) = adapter();
    let bytes = b"0123456789abcdef".to_vec();
    let stage = first
        .prepare_at_destination(request(
            &backend,
            bytes.len(),
            BINDING,
            ResumeMode::Discover,
        )?)
        .await?;
    write(&first, &stage, &bytes[..10]).await?;
    drop(stage);
    drop(first);
    // Bytes past the durable prefix, as an interrupted unstable write could leave.
    let stage_name = name(ArtifactKind::Stage, false);
    let mut torn = file(&protocol, &stage_name).ok_or("stage")?;
    torn.extend_from_slice(b"torn");
    put(&protocol, &stage_name, torn);

    let second = NfsStagedDestinationAdapter::new(protocol.clone(), backend.clone());
    let stage = second
        .prepare_at_destination(request(
            &backend,
            bytes.len(),
            BINDING,
            ResumeMode::Discover,
        )?)
        .await?;
    assert_eq!(stage.prepare_fact, PrepareFact::Resumed { bytes: 10 });
    assert_eq!(stage.write_offset, 10);
    assert_eq!(
        file(&protocol, &stage_name).map(|stage| stage.len()),
        Some(10)
    );
    write(&second, &stage, &bytes[10..]).await?;
    publish(&second, &stage, &bytes)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(file(&protocol, "dir/final.bin"), Some(bytes));
    assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    Ok(())
}

/// A second host that resumes the stage takes it over: the first can no longer checkpoint,
/// publish or clean it up, and the second publishes.
#[tokio::test]
async fn a_stage_taken_over_by_another_writer_is_left_to_it() -> TestResult {
    let (first, protocol, backend) = adapter();
    let bytes = b"0123456789".to_vec();
    let held = first
        .prepare_at_destination(request(
            &backend,
            bytes.len(),
            BINDING,
            ResumeMode::Discover,
        )?)
        .await?;
    write(&first, &held, &bytes).await?;

    let second = NfsStagedDestinationAdapter::new(protocol.clone(), backend.clone());
    let taken = second
        .prepare_at_destination(request(
            &backend,
            bytes.len(),
            BINDING,
            ResumeMode::Discover,
        )?)
        .await?;
    assert_eq!(
        taken.prepare_fact,
        PrepareFact::Resumed {
            bytes: bytes.len() as u64
        }
    );

    let refused = publish(&first, &held, &bytes)
        .await
        .err()
        .ok_or("a taken-over stage must not publish")?;
    assert_eq!(class(&refused.error), Some(FailureClass::Conflict));
    assert!(!refused.final_destination_changed);
    assert_eq!(file(&protocol, "dir/final.bin"), None);
    assert!(first.observe_checkpoint(&held).await.is_err());
    // Its next checkpoint is refused too, before it touches the pointer.
    let refused = write(&first, &held, &bytes)
        .await
        .err()
        .ok_or("a taken-over stage must not checkpoint")?;
    assert_eq!(class(&refused), Some(FailureClass::Conflict));
    first.discard(held).await?;
    assert!(file(&protocol, &name(ArtifactKind::Stage, false)).is_some());

    publish(&second, &taken, &bytes)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(file(&protocol, "dir/final.bin"), Some(bytes));
    assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn what_is_found_restarts_for_each_reason() -> TestResult {
    let (_, _, backend) = adapter();
    let mut other_transfer = request(&backend, 10, BINDING, ResumeMode::Discover)?;
    other_transfer.transfer_identity = [5; 32];
    let cases = [
        (
            request(&backend, 10, BINDING, ResumeMode::Restart)?,
            RestartReason::Requested,
        ),
        (
            request(&backend, 10, [8; 32], ResumeMode::Discover)?,
            RestartReason::BindingChanged,
        ),
        (other_transfer, RestartReason::OtherTransfer),
    ];
    for (request_case, reason) in cases {
        let (adapter, protocol, backend) = self::adapter();
        let first = adapter
            .prepare_at_destination(request(&backend, 10, BINDING, ResumeMode::Discover)?)
            .await?;
        write(&adapter, &first, b"0123456789").await?;
        drop(first);
        let fresh = NfsStagedDestinationAdapter::new(protocol.clone(), backend);
        let stage = fresh.prepare_at_destination(request_case).await?;
        assert_eq!(stage.prepare_fact, PrepareFact::Restarted { reason });
        assert_eq!(stage.write_offset, 0);
        assert_eq!(
            file(&protocol, &name(ArtifactKind::Stage, false)).map(|stage| stage.len()),
            Some(0)
        );
        fresh.discard(stage).await?;
        assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    }
    Ok(())
}

#[tokio::test]
async fn a_damaged_or_lone_pointer_restarts() -> TestResult {
    for (damage, reason) in [
        ("untagged", RestartReason::PointerCorrupt),
        ("lone", RestartReason::PointerWithoutStage),
    ] {
        let (adapter, protocol, backend) = adapter();
        let first = adapter
            .prepare_at_destination(request(&backend, 10, BINDING, ResumeMode::Discover)?)
            .await?;
        write(&adapter, &first, b"0123456789").await?;
        drop(first);
        let pointer = name(ArtifactKind::Pointer, false);
        if damage == "untagged" {
            let mut found =
                DestinationPointer::decode(&file(&protocol, &pointer).ok_or("pointer")?)
                    .map_err(|_| "decode")?;
            found.extension = Bytes::new();
            put(&protocol, &pointer, found.encode().map_err(|_| "encode")?);
        } else {
            protocol
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&name(ArtifactKind::Stage, false));
        }
        let fresh = NfsStagedDestinationAdapter::new(protocol.clone(), backend.clone());
        let stage = fresh
            .prepare_at_destination(request(&backend, 10, BINDING, ResumeMode::Discover)?)
            .await?;
        assert_eq!(
            stage.prepare_fact,
            PrepareFact::Restarted { reason },
            "{damage}"
        );
        fresh.discard(stage).await?;
    }
    Ok(())
}

/// A RENAME that fails after the server committed it, and a temporary a killed process left, do
/// not stop the pointer from being written.
#[tokio::test]
async fn a_lost_rename_reply_and_a_leftover_temporary_are_settled() -> TestResult {
    let (adapter, protocol, backend) = adapter();
    put(
        &protocol,
        &name(ArtifactKind::Pointer, true),
        b"half a pointer".to_vec(),
    );
    // Mode 4: the rename happens, but the reply says it failed.
    protocol.rename_mode.store(4, Ordering::SeqCst);
    let stage = adapter
        .prepare_at_destination(request(&backend, 10, BINDING, ResumeMode::Discover)?)
        .await?;
    protocol.rename_mode.store(0, Ordering::SeqCst);
    assert_eq!(adapter.observe_checkpoint(&stage).await?.durable_prefix, 0);
    assert_eq!(file(&protocol, &name(ArtifactKind::Pointer, true)), None);
    adapter.discard(stage).await?;
    assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    Ok(())
}

/// A fresh stage whose first pointer cannot be written is removed again; nothing is left.
#[tokio::test]
async fn a_failed_first_pointer_removes_the_fresh_stage() -> TestResult {
    let (adapter, protocol, backend) = adapter();
    // Mode 2: the rename fails and did not happen.
    protocol.rename_mode.store(2, Ordering::SeqCst);
    let failed = adapter
        .prepare_at_destination(request(&backend, 10, BINDING, ResumeMode::Discover)?)
        .await;
    protocol.rename_mode.store(0, Ordering::SeqCst);
    assert!(failed.is_err());
    assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    Ok(())
}

/// A publication whose RENAME reply is lost after the server committed it is settled by the
/// final file's content, and the pointer still goes.
#[tokio::test]
async fn a_lost_publication_reply_still_removes_the_pointer() -> TestResult {
    let (adapter, protocol, backend) = adapter();
    let bytes = b"payload".to_vec();
    let stage = adapter
        .prepare_at_destination(request(
            &backend,
            bytes.len(),
            BINDING,
            ResumeMode::Discover,
        )?)
        .await?;
    write(&adapter, &stage, &bytes).await?;
    protocol.rename_mode.store(4, Ordering::SeqCst);
    let published = publish(&adapter, &stage, &bytes).await;
    protocol.rename_mode.store(0, Ordering::SeqCst);
    published.map_err(|failure| failure.error)?;
    assert_eq!(file(&protocol, "dir/final.bin"), Some(bytes));
    assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    Ok(())
}
