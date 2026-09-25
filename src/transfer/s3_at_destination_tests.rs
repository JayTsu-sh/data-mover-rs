//! Checkpointed transfers to S3 through the engine's at-destination route (ADR-0006 C15b; on for
//! every S3 connection since C15c), over the in-memory S3 with a 16 MiB automatic interval.

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use futures::channel::mpsc;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, ObservedEntry, StoragePath, Transience};
use crate::storage::backends::s3::tests::{MemoryS3, identity};
use crate::storage::backends::s3::{S3Protocol as _, S3ProtocolFailure, connect_at_destination};
use crate::storage::{PreflightPolicy, PrepareFact, Storage};
use crate::transfer::{
    EffectiveRecovery, ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceRequest,
    ExpertSourceSession, InflightLimits, TransferOutcome, TransferPolicy, TransferRequest,
    transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const MIB: usize = 1024 * 1024;
/// The automatic checkpoint interval these destinations declare (64 MiB for real connections).
const INTERVAL: usize = 16 * MIB;

/// Each test writes its own final key: the engine's per-file guard is process-wide.
fn checkpointed(protocol: &Arc<MemoryS3>, final_key: &str) -> TestResult<TransferRequest> {
    checkpointed_every(protocol, final_key, INTERVAL)
}

/// [`checkpointed`] to a destination whose automatic interval is `interval`.
fn checkpointed_every(
    protocol: &Arc<MemoryS3>,
    final_key: &str,
    interval: usize,
) -> TestResult<TransferRequest> {
    let storage = || connect_at_destination(protocol.clone(), identity(), interval as u64);
    Ok(TransferRequest::new(
        storage()?,
        StoragePath::new("source")?,
        storage()?,
        StoragePath::new(final_key)?,
        InflightLimits::new(4, 4 * MIB, 4)?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Checkpointed))
}

async fn seeded(size: usize) -> (Arc<MemoryS3>, Bytes) {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from((0..=252_u8).cycle().take(size).collect::<Vec<_>>());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), payload.clone());
    (protocol, payload)
}

/// Nothing but the source and the final object: no pointer, no upload.
async fn leaves_nothing_behind(protocol: &MemoryS3, final_key: &str) -> TestResult<bool> {
    let open = protocol
        .list_uploads(final_key)
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    let artifacts = protocol
        .objects
        .lock()
        .await
        .keys()
        .any(|key| key.contains(".data-mover-"));
    Ok(open.is_empty() && !artifacts)
}

/// D3: a checkpointed object no larger than the interval (64 MiB for real connections) is a
/// multipart upload on the final key that never writes a pointer.
#[tokio::test]
async fn a_checkpointed_object_up_to_the_interval_writes_no_pointer() -> TestResult {
    const FINAL: &str = "dir/small-checkpointed";
    let (protocol, payload) = seeded(INTERVAL).await;
    let outcome = transfer(checkpointed(&protocol, FINAL)?).await?;
    assert_eq!(outcome.prepare, PrepareFact::Fresh);
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::SkippedBelowCheckpointThreshold
    );
    assert!(outcome.blake3.is_some());
    assert_eq!(*protocol.puts.lock().await, 0);
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&payload));
    assert!(leaves_nothing_behind(&protocol, FINAL).await?);
    Ok(())
}

/// Above the interval the engine's first deferred checkpoint makes the destination write its
/// pointer (one `PutObject`); publication completes the upload on the final key and removes it.
#[tokio::test]
async fn a_checkpointed_object_over_the_interval_writes_one_pointer() -> TestResult {
    const FINAL: &str = "dir/large-checkpointed";
    let (protocol, payload) = seeded(INTERVAL + 8 * MIB).await;
    let outcome = transfer(checkpointed(&protocol, FINAL)?).await?;
    assert_eq!(outcome.recovery, EffectiveRecovery::Checkpointed);
    assert!(outcome.blake3.is_some());
    assert_eq!(*protocol.puts.lock().await, 1);
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert_eq!(*protocol.completes.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&payload));
    assert!(leaves_nothing_behind(&protocol, FINAL).await?);
    Ok(())
}

/// A failed part before any checkpoint keeps an unrecoverable stage; discarding it aborts the
/// upload and never touches the final key.
#[tokio::test]
async fn a_failed_upload_is_discarded_without_touching_the_final_key() -> TestResult {
    const FINAL: &str = "dir/failed-checkpointed";
    let (protocol, _) = seeded(20 * MIB).await;
    protocol
        .objects
        .lock()
        .await
        .insert(FINAL.into(), Bytes::from_static(b"previous"));
    *protocol.part_failure.lock().await = Some((
        2,
        S3ProtocolFailure::entry(
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "denied",
        ),
    ));
    let failure = transfer(checkpointed(&protocol, FINAL)?)
        .await
        .err()
        .ok_or("a failed part must fail the transfer")?;
    assert!(!failure.final_destination_changed());
    assert!(failure.has_unpublished_stage() && !failure.has_recoverable_stage());
    failure.discard_stage().await?;
    assert!(leaves_nothing_behind(&protocol, FINAL).await?);
    assert_eq!(
        protocol.objects.lock().await.get(FINAL),
        Some(&Bytes::from_static(b"previous"))
    );
    Ok(())
}

/// A connectivity failure of one part.
fn reset() -> S3ProtocolFailure {
    S3ProtocolFailure::session(FailureClass::Connectivity, Transience::Transient, "reset")
}

/// A transfer cut after its first checkpoint — at an 8 MiB interval, part 3 of 3 fails once
/// parts 1 and 2 are stored — is dropped; a second `transfer` through fresh connections, with
/// nothing kept where data-mover runs, resumes from the two parts the service lists, uploads only
/// the last, and publishes the source's content. The same holds when the pointer's `PutObject`
/// stored it but lost the reply (it reads back byte for byte, so it counts as written).
#[tokio::test]
async fn an_interrupted_transfer_resumes_in_a_fresh_connection() -> TestResult {
    const PART: usize = 8 * MIB;
    for (final_key, lost_pointer_reply) in [("dir/resumed", false), ("dir/resumed-lost", true)] {
        let (protocol, payload) = seeded(3 * PART).await;
        *protocol.part_failure.lock().await = Some((3, reset()));
        *protocol.part_failure_waits.lock().await = true;
        *protocol.put_commits_then_fails.lock().await = lost_pointer_reply;
        let failure = transfer(checkpointed_every(&protocol, final_key, PART)?)
            .await
            .err()
            .ok_or("the failed part must cut the transfer")?;
        assert!(!failure.final_destination_changed());
        assert!(failure.has_recoverable_stage(), "{final_key}");
        drop(failure);
        assert_eq!(*protocol.puts.lock().await, 1, "one pointer");
        assert_eq!(open_uploads(&protocol, final_key).await?, 1);
        assert!(!protocol.objects.lock().await.contains_key(final_key));

        *protocol.part_failure.lock().await = None;
        let sent = *protocol.part_uploads.lock().await;
        let outcome = transfer(checkpointed_every(&protocol, final_key, PART)?).await?;
        let reused = 2 * PART as u64;
        assert_eq!(outcome.prepare, PrepareFact::Resumed { bytes: reused });
        assert_eq!(outcome.reused_bytes, reused);
        assert_eq!(*protocol.part_uploads.lock().await, sent + 1, "part 3 only");
        assert_eq!(outcome.transferred_bytes, payload.len() as u64);
        assert_eq!(outcome.blake3, Some(*blake3::hash(&payload).as_bytes()));
        assert_eq!(protocol.objects.lock().await.get(final_key), Some(&payload));
        assert!(leaves_nothing_behind(&protocol, final_key).await?);
    }
    Ok(())
}

async fn open_uploads(protocol: &MemoryS3, final_key: &str) -> TestResult<usize> {
    Ok(protocol
        .list_uploads(final_key)
        .await
        .map_err(|failure| format!("{failure:?}"))?
        .len())
}

/// What the expert source half is offered: the source object as `source` describes it.
async fn observed(source: &Storage) -> TestResult<ObservedEntry> {
    let described = source
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new("source")?)
        .await?;
    Ok(ObservedEntry::new(
        described.path,
        described.kind,
        described.size,
        None,
        described.source_identity,
    )?)
}

/// Runs the two expert halves from and to the in-memory S3, whose destination declares
/// `interval` as its automatic interval.
async fn expert_run(
    protocol: &Arc<MemoryS3>,
    interval: u64,
    final_key: &str,
) -> TestResult<TransferOutcome> {
    let storage = || connect_at_destination(protocol.clone(), identity(), interval);
    let source = storage()?;
    let observation = observed(&source).await?;
    let limits = InflightLimits::new(4, 4 * MIB, 4)?;
    let source_half = ExpertSourceSession::open(ExpertSourceRequest::new(
        source,
        observation.clone(),
        limits,
        CancellationToken::new(),
    ))
    .await?;
    let session = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observation,
            source_half.offer().maximum_chunk_bytes,
            storage()?,
            StoragePath::new(final_key)?,
            limits,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Checkpointed),
    )
    .await?;
    let mut payload = source_half.stream_from(session.write_offset())?;
    let (sender, receiver) = mpsc::unbounded();
    let pump = async move {
        while let Some(chunk) = payload.next_chunk().await? {
            if sender.unbounded_send(Ok(chunk)).is_err() {
                break;
            }
        }
        drop(sender);
        payload.finish().await
    };
    let (written, evidence) = futures::join!(session.write(Box::pin(receiver)), pump);
    Ok(written?.complete(evidence?).await?)
}

/// D3 in the expert destination half, which asks for a recoverable prepare of every checkpointed
/// object over one chunk: a 24 MiB object writes its pointer only when the destination's
/// interval is below it (16 MiB here), never at the 64 MiB interval of real connections.
#[tokio::test]
async fn the_expert_half_writes_a_pointer_only_over_the_interval() -> TestResult {
    let cases = [
        (
            64 * MIB,
            0,
            EffectiveRecovery::SkippedBelowCheckpointThreshold,
        ),
        (INTERVAL, 1, EffectiveRecovery::Checkpointed),
    ];
    for (interval, pointers, recovery) in cases {
        let final_key = format!("dir/expert-{interval}");
        let (protocol, payload) = seeded(24 * MIB).await;
        let outcome = expert_run(&protocol, interval as u64, &final_key).await?;
        assert_eq!(outcome.recovery, recovery, "{interval}");
        assert_eq!(*protocol.puts.lock().await, pointers, "{interval}");
        assert_eq!(
            protocol.objects.lock().await.get(&final_key),
            Some(&payload)
        );
        assert!(leaves_nothing_behind(&protocol, &final_key).await?);
    }
    Ok(())
}
