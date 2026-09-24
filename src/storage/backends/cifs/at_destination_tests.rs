//! Destination-kept recovery for the CIFS stage (ADR-0006 C11), against the in-memory share.

use super::*;
use crate::storage::artifacts::{ArtifactKind, artifact_name, artifact_temporary_name};
use crate::storage::pointer::DestinationPointer;
use crate::storage::{
    DestinationPrepareRequest, PrepareFact, PreparedStage, PublicationFailure, RestartReason,
    ResumeMode,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const IDENTITY: [u8; 32] = [4; 32];
const BINDING: [u8; 32] = [7; 32];

fn request(
    backend: &BackendIdentity,
    size: u64,
    binding: [u8; 32],
    resume: ResumeMode,
) -> Result<DestinationPrepareRequest, Box<dyn std::error::Error>> {
    let mut prepare = prepare_request(backend)?;
    prepare.final_destination = FinalDestination::new(StoragePath::new("dir/final.bin")?);
    prepare.source.size = Some(size);
    prepare.recovery_binding = binding;
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

fn artifacts_left(protocol: &MemoryProtocol) -> Vec<String> {
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

fn file(protocol: &MemoryProtocol, path: &str) -> Option<Vec<u8>> {
    protocol
        .files
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(path)
        .cloned()
}

fn put(protocol: &MemoryProtocol, path: &str, bytes: Vec<u8>) {
    protocol
        .files
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(path.to_owned(), bytes);
}

async fn write(
    destination: &CifsStagedDestination,
    stage: &PreparedStage,
    bytes: &[u8],
) -> Result<(), StorageRoleFailure> {
    let input: ByteStream = Box::pin(stream::iter([Ok(Bytes::copy_from_slice(bytes))]));
    destination.write(stage, input).await.map(|_| ())
}

async fn publish(
    destination: &CifsStagedDestination,
    stage: &PreparedStage,
    bytes: &[u8],
) -> Result<(), PublicationFailure> {
    destination
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

fn share() -> Result<(Arc<MemoryProtocol>, BackendIdentity), Box<dyn std::error::Error>> {
    Ok((Arc::new(MemoryProtocol::default()), identity()?))
}

fn adapter(protocol: &Arc<MemoryProtocol>, identity: &BackendIdentity) -> CifsStagedDestination {
    CifsStagedDestination::new(Arc::clone(protocol), identity.clone())
}

#[tokio::test]
async fn a_fresh_stage_publishes_and_leaves_no_artifact() -> TestResult {
    let (protocol, backend) = share()?;
    let destination = adapter(&protocol, &backend);
    let stage = destination
        .prepare_at_destination(request(&backend, 6, BINDING, ResumeMode::Discover)?)
        .await?;
    assert_eq!(stage.prepare_fact(), PrepareFact::Fresh);
    assert_eq!(
        artifacts_left(&protocol),
        [
            name(ArtifactKind::Pointer, false),
            name(ArtifactKind::Stage, false)
        ]
    );
    write(&destination, &stage, b"abcdef").await?;
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        6
    );
    publish(&destination, &stage, b"abcdef")
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(file(&protocol, "dir/final.bin"), Some(b"abcdef".to_vec()));
    assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    Ok(())
}

/// A new connection resumes from the pointer's prefix. SMB cannot shorten the stage, so the
/// bytes past the prefix stay until the writer overwrites them from the prefix on.
#[tokio::test]
async fn a_new_connection_resumes_from_the_pointers_prefix() -> TestResult {
    let (protocol, backend) = share()?;
    let first = adapter(&protocol, &backend);
    let stage = first
        .prepare_at_destination(request(&backend, 10, BINDING, ResumeMode::Discover)?)
        .await?;
    write(&first, &stage, b"01234").await?;
    drop(stage);
    drop(first);
    // Bytes past the durable prefix, as an interrupted write could leave.
    let stage_name = name(ArtifactKind::Stage, false);
    put(&protocol, &stage_name, b"01234torn!".to_vec());

    let second = adapter(&protocol, &backend);
    let stage = second
        .prepare_at_destination(request(&backend, 10, BINDING, ResumeMode::Discover)?)
        .await?;
    assert_eq!(stage.prepare_fact(), PrepareFact::Resumed { bytes: 5 });
    assert_eq!(stage.write_offset, 5);
    write(&second, &stage, b"56789").await?;
    publish(&second, &stage, b"0123456789")
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        file(&protocol, "dir/final.bin"),
        Some(b"0123456789".to_vec())
    );
    assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    Ok(())
}

/// A second connection that resumes the stage takes it over: the first can no longer
/// checkpoint, observe, publish or clean it up, and the second publishes.
#[tokio::test]
async fn a_stage_taken_over_by_another_writer_is_left_to_it() -> TestResult {
    let (protocol, backend) = share()?;
    let first = adapter(&protocol, &backend);
    let held = first
        .prepare_at_destination(request(&backend, 6, BINDING, ResumeMode::Discover)?)
        .await?;
    write(&first, &held, b"abcdef").await?;

    let second = adapter(&protocol, &backend);
    let taken = second
        .prepare_at_destination(request(&backend, 6, BINDING, ResumeMode::Discover)?)
        .await?;
    assert_eq!(taken.prepare_fact(), PrepareFact::Resumed { bytes: 6 });

    assert!(first.observe_checkpoint(&held).await.is_err());
    let refused = write(&first, &held, b"abcdef")
        .await
        .err()
        .ok_or("a taken-over stage must not checkpoint")?;
    assert_eq!(class(&refused), Some(FailureClass::Conflict));
    let refused = publish(&first, &held, b"abcdef")
        .await
        .err()
        .ok_or("a taken-over stage must not publish")?;
    assert_eq!(class(&refused.error), Some(FailureClass::Conflict));
    assert!(!refused.final_destination_changed);
    assert_eq!(file(&protocol, "dir/final.bin"), None);
    first.discard(held).await?;
    assert!(file(&protocol, &name(ArtifactKind::Stage, false)).is_some());

    publish(&second, &taken, b"abcdef")
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(file(&protocol, "dir/final.bin"), Some(b"abcdef".to_vec()));
    assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn what_is_found_restarts_for_each_reason() -> TestResult {
    let (_, backend) = share()?;
    let mut other_transfer = request(&backend, 6, BINDING, ResumeMode::Discover)?;
    other_transfer.transfer_identity = [5; 32];
    let cases = [
        (
            request(&backend, 6, BINDING, ResumeMode::Restart)?,
            RestartReason::Requested,
        ),
        (
            request(&backend, 6, [8; 32], ResumeMode::Discover)?,
            RestartReason::BindingChanged,
        ),
        (other_transfer, RestartReason::OtherTransfer),
        (
            request(&backend, 6, BINDING, ResumeMode::Discover)?,
            RestartReason::PointerWithoutStage,
        ),
    ];
    for (case, reason) in cases {
        let (protocol, backend) = share()?;
        let first = adapter(&protocol, &backend);
        let stage = first
            .prepare_at_destination(request(&backend, 6, BINDING, ResumeMode::Discover)?)
            .await?;
        write(&first, &stage, b"abcdef").await?;
        drop(stage);
        if reason == RestartReason::PointerWithoutStage {
            protocol
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&name(ArtifactKind::Stage, false));
        }
        let fresh = adapter(&protocol, &backend);
        let stage = fresh.prepare_at_destination(case).await?;
        assert_eq!(stage.prepare_fact(), PrepareFact::Restarted { reason });
        assert_eq!(stage.write_offset, 0);
        fresh.discard(stage).await?;
        assert_eq!(
            artifacts_left(&protocol),
            Vec::<String>::new(),
            "{reason:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_untagged_pointer_is_cleaned_as_corrupt() -> TestResult {
    let (protocol, backend) = share()?;
    let first = adapter(&protocol, &backend);
    let stage = first
        .prepare_at_destination(request(&backend, 6, BINDING, ResumeMode::Discover)?)
        .await?;
    write(&first, &stage, b"abcdef").await?;
    drop(stage);
    let pointer = name(ArtifactKind::Pointer, false);
    let mut found = DestinationPointer::decode(&file(&protocol, &pointer).ok_or("pointer")?)
        .map_err(|_| "decode")?;
    found.extension = Bytes::new();
    put(&protocol, &pointer, found.encode().map_err(|_| "encode")?);
    let fresh = adapter(&protocol, &backend);
    let stage = fresh
        .prepare_at_destination(request(&backend, 6, BINDING, ResumeMode::Discover)?)
        .await?;
    assert_eq!(
        stage.prepare_fact(),
        PrepareFact::Restarted {
            reason: RestartReason::PointerCorrupt
        }
    );
    fresh.discard(stage).await?;
    Ok(())
}

/// A RENAME reply lost after the server committed it — for the pointer and for publication — and
/// a temporary a killed process left do not get in the way.
#[tokio::test]
async fn lost_rename_replies_and_a_leftover_temporary_are_settled() -> TestResult {
    let (protocol, backend) = share()?;
    put(
        &protocol,
        &name(ArtifactKind::Pointer, true),
        b"half".to_vec(),
    );
    let destination = adapter(&protocol, &backend);
    protocol
        .fail_rename_after_commit
        .store(true, Ordering::SeqCst);
    let stage = destination
        .prepare_at_destination(request(&backend, 6, BINDING, ResumeMode::Discover)?)
        .await?;
    write(&destination, &stage, b"abcdef").await?;
    publish(&destination, &stage, b"abcdef")
        .await
        .map_err(|failure| failure.error)?;
    protocol
        .fail_rename_after_commit
        .store(false, Ordering::SeqCst);
    assert_eq!(file(&protocol, "dir/final.bin"), Some(b"abcdef".to_vec()));
    assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    Ok(())
}

/// A pointer only ever records flushed bytes — also when publication does not ask for
/// durability — on both the streaming and the positioned writer.
#[tokio::test]
async fn the_pointer_follows_a_flush_on_both_writers() -> TestResult {
    for positioned in [false, true] {
        let (protocol, backend) = share()?;
        let destination = adapter(&protocol, &backend);
        let mut stage = destination
            .prepare_at_destination(request(&backend, 6, BINDING, ResumeMode::Discover)?)
            .await?;
        stage.durable_publication = false;

        if positioned {
            let chunks = [(4, &b"ef"[..]), (0, &b"abcd"[..])].map(|(offset, data)| {
                Ok(crate::storage::PositionedChunk {
                    offset,
                    data: Bytes::from_static(data),
                })
            });
            destination
                .write_positioned(&stage, Box::pin(stream::iter(chunks)))
                .await?;
        } else {
            write(&destination, &stage, b"abcdef").await?;
        }
        let stage_name = name(ArtifactKind::Stage, false);
        // The last pointer written (under its temporary name) comes after the stage's last flush.
        let flushed = protocol
            .flushed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let last_stage = flushed.iter().rposition(|path| *path == stage_name);
        let last_pointer = flushed
            .iter()
            .rposition(|path| path.ends_with(".pointer.tmp"));
        assert!(
            matches!((last_stage, last_pointer), (Some(stage), Some(pointer)) if stage < pointer),
            "positioned={positioned}: {flushed:?}"
        );
        super::assert_pointer_handles_closed(&protocol);
        assert_eq!(
            destination.observe_checkpoint(&stage).await?.durable_prefix,
            6,
            "positioned={positioned}"
        );
        publish(&destination, &stage, b"abcdef")
            .await
            .map_err(|failure| failure.error)?;
        assert_eq!(artifacts_left(&protocol), Vec::<String>::new());
    }
    Ok(())
}

/// CIFS keeps its recovery state at the destination: the engine routes it through
/// `prepare_at_destination` and never through the local recovery store (ADR-0006 C11c).
#[test]
fn cifs_keeps_recovery_at_the_destination() -> TestResult {
    let (protocol, backend) = share()?;
    assert!(adapter(&protocol, &backend).recovery_at_destination());
    Ok(())
}
