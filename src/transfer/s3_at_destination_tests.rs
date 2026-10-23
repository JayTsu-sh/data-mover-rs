//! Checkpointed transfers to S3 through the engine's at-destination route (ADR-0006 C15b; on for
//! every S3 connection since C15c), over the in-memory S3 with a 16 MiB automatic interval.

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use futures::channel::mpsc;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, ObservedEntry, StoragePath, Transience};
use crate::storage::backends::s3::tests::{
    MemoryS3, content_md5, endpoint_of, identity, source_storage,
};
use crate::storage::backends::s3::{
    S3PartFacts, S3Protocol as _, S3ProtocolFailure, connect_at_destination,
};
use crate::storage::{
    DestinationPrepareRequest, FinalDestination, PrepareFact, PrepareRequest, Storage,
};
use crate::transfer::{
    EffectiveRecovery, ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceRequest,
    ExpertSourceSession, TransferOutcome, TransferPolicy, TransferRequest, transfer,
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
        source_storage(protocol, "src")?,
        StoragePath::new(final_key)?,
        storage()?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Checkpointed))
}

async fn seeded(size: usize, path: &str) -> (Arc<MemoryS3>, Bytes) {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from((0..=252_u8).cycle().take(size).collect::<Vec<_>>());
    protocol
        .objects
        .lock()
        .await
        .insert(format!("src/{path}"), payload.clone());
    (protocol, payload)
}

/// One empty H anchor and one intent per bounded source window; lifecycle adds prepare,
/// final D, `ReadyToPublish` and `Published`. These fixtures never retry an uncommitted PUT.
pub(super) fn hash_batches(request: &TransferRequest, size: usize) -> TestResult<u32> {
    let source = request.source.read_source()?;
    let window = source.maximum_read_chunk_bytes() * source.read_concurrency();
    Ok(u32::try_from(size.div_ceil(window))?)
}

pub(super) struct CompletedParts(pub(super) Vec<S3PartFacts>);

impl CompletedParts {
    pub(super) fn assert_third_failed(&self) {
        let numbers: Vec<_> = self.0.iter().map(|part| part.number).collect();
        assert!(
            numbers.starts_with(&[1, 2]),
            "lower parts must have settled: {numbers:?}"
        );
        assert!(
            !numbers.contains(&3),
            "the injected failing part cannot be stored"
        );
    }

    pub(super) fn bytes(&self) -> u64 {
        self.0.iter().map(|part| part.size).sum()
    }

    pub(super) fn missing(&self, size: usize, part_size: usize) -> Vec<i32> {
        for part in &self.0 {
            assert!(part.number > 0, "part numbers must be positive");
            let start = usize::try_from(part.number - 1)
                .unwrap_or_else(|_| panic!("part number must fit usize"))
                * part_size;
            assert!(start < size, "part outside fixture geometry");
            assert_eq!(part.size, (size - start).min(part_size) as u64);
        }
        let count = i32::try_from(size.div_ceil(part_size))
            .unwrap_or_else(|_| panic!("fixture part count must fit i32"));
        (1..=count)
            .filter(|number| !self.0.iter().any(|part| part.number == *number))
            .collect()
    }
}

pub(super) async fn completed_parts(protocol: &MemoryS3, key: &str) -> TestResult<CompletedParts> {
    assert!(
        stored_pointer(protocol, key).await?.checkpoint.is_some(),
        "ordinary hash-mode fixture"
    );
    let uploads = protocol
        .list_uploads(key)
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(uploads.len(), 1, "one interrupted upload on {key}");
    let mut parts = protocol
        .list_parts(key, &uploads[0])
        .await
        .map_err(|error| format!("{error:?}"))?;
    parts.sort_by_key(|part| part.number);
    assert!(parts.windows(2).all(|pair| pair[0].number < pair[1].number));
    assert!(
        parts
            .iter()
            .all(|part| part.number > 0 && !part.etag.is_empty())
    );
    Ok(CompletedParts(parts))
}

pub(super) async fn stored_pointer(
    protocol: &MemoryS3,
    key: &str,
) -> TestResult<crate::storage::pointer::DestinationPointer> {
    let path = StoragePath::new(key)?;
    let pointer = crate::storage::artifacts::sibling_artifact(
        &path,
        crate::storage::artifacts::ArtifactKind::Upload,
    )
    .ok_or("pointer path")?;
    let bytes = protocol
        .objects
        .lock()
        .await
        .get(pointer.as_str())
        .cloned()
        .ok_or("stored pointer")?;
    crate::storage::pointer::DestinationPointer::decode(&bytes).map_err(|_| "pointer decode".into())
}

pub(super) async fn assert_uploaded_parts(protocol: &MemoryS3, expected: &[i32]) -> TestResult {
    let recorded = protocol.body_fragments.lock().await;
    let mut numbers: Vec<_> = recorded
        .as_ref()
        .ok_or("part recording was not enabled")?
        .iter()
        .map(|(number, _)| *number)
        .collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers, expected,
        "only the missing part numbers may be uploaded"
    );
    Ok(())
}

/// Explicit low-level v1 fixture: ordinary eligible transfers now create v2 roots instead.
pub(super) async fn legacy_upload(
    protocol: &Arc<MemoryS3>,
    source_prefix: &str,
    key: &str,
    interval: usize,
) -> TestResult {
    let path = StoragePath::new(key)?;
    let source = source_storage(protocol, source_prefix)?
        .read_source()?
        .describe(&path)
        .await?;
    let destination =
        connect_at_destination(protocol.clone(), endpoint_of(protocol), interval as u64)?;
    let stage = destination
        .staged_destination()?
        .prepare_at_destination(DestinationPrepareRequest::new(
            PrepareRequest {
                final_destination: FinalDestination::new(path.clone()),
                source,
                recovery_binding: [7; 32],
            },
            [4; 32],
        ))
        .await?;
    assert!(!stage.checkpoint_enabled());
    assert!(stored_pointer(protocol, key).await?.checkpoint.is_none());
    seed_legacy_parts(protocol, source_prefix, key).await?;
    Ok(())
}

async fn seed_legacy_parts(protocol: &MemoryS3, source_prefix: &str, key: &str) -> TestResult {
    const PART: usize = 8 * MIB;
    let uploads = protocol
        .list_uploads(key)
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(uploads.len(), 1);
    let data = protocol
        .objects
        .lock()
        .await
        .get(&format!("{source_prefix}/{key}"))
        .cloned()
        .ok_or("legacy source payload")?;
    assert!(data.len() > 2 * PART);
    for number in 1..=2 {
        let body = data.slice((number - 1) * PART..number * PART);
        let md5 = content_md5(&body);
        protocol
            .upload_part(key, &uploads[0], i32::try_from(number)?, body.into(), &md5)
            .await
            .map_err(|error| format!("{error:?}"))?;
    }
    Ok(())
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
    let (protocol, payload) = seeded(INTERVAL, FINAL).await;
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

/// Eligible Checkpointed transfers anchor H before payload and persist their final D and phases.
#[tokio::test]
async fn a_checkpointed_object_over_the_interval_bounds_hash_pointer_writes() -> TestResult {
    const FINAL: &str = "dir/large-checkpointed";
    let (protocol, payload) = seeded(INTERVAL + 8 * MIB, FINAL).await;
    let request = checkpointed(&protocol, FINAL)?;
    let batches = hash_batches(&request, payload.len())?;
    let outcome = transfer(request).await?;
    assert_eq!(outcome.recovery, EffectiveRecovery::Checkpointed);
    assert!(outcome.blake3.is_some());
    assert_eq!(
        *protocol.puts.lock().await,
        batches + 5,
        "batched H plus lifecycle"
    );
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert_eq!(*protocol.completes.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&payload));
    assert!(leaves_nothing_behind(&protocol, FINAL).await?);
    Ok(())
}

/// A failed part after persisted H keeps a recoverable stage; discarding it deletes the
/// pointer, aborts the upload and never touches the final key.
#[tokio::test]
async fn a_failed_upload_is_discarded_without_touching_the_final_key() -> TestResult {
    const FINAL: &str = "dir/failed-checkpointed";
    let (protocol, _) = seeded(20 * MIB, FINAL).await;
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
    assert!(failure.has_unpublished_stage() && failure.has_recoverable_stage());
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
        let (protocol, payload) = seeded(3 * PART, final_key).await;
        *protocol.part_failure.lock().await = Some((3, reset()));
        *protocol.part_failure_waits.lock().await = true;
        *protocol.put_commits_then_fails.lock().await = lost_pointer_reply;
        let request = checkpointed_every(&protocol, final_key, PART)?;
        let batches = hash_batches(&request, payload.len())?;
        let failure = transfer(request)
            .await
            .err()
            .ok_or("the failed part must cut the transfer")?;
        assert!(!failure.final_destination_changed());
        assert!(failure.has_recoverable_stage(), "{final_key}");
        drop(failure);
        assert_eq!(
            *protocol.puts.lock().await,
            batches + 2,
            "prepare and write-ahead H"
        );
        assert_eq!(open_uploads(&protocol, final_key).await?, 1);
        assert!(!protocol.objects.lock().await.contains_key(final_key));

        *protocol.part_failure.lock().await = None;
        let completed = completed_parts(&protocol, final_key).await?;
        completed.assert_third_failed();
        let missing = completed.missing(payload.len(), PART);
        *protocol.body_fragments.lock().await = Some(Vec::new());
        let sent = *protocol.part_uploads.lock().await;
        let outcome = transfer(checkpointed_every(&protocol, final_key, PART)?).await?;
        let reused = completed.bytes();
        assert_eq!(outcome.prepare, PrepareFact::Resumed { bytes: reused });
        assert_eq!(outcome.reused_bytes, reused);
        assert_eq!(
            *protocol.part_uploads.lock().await,
            sent + u32::try_from(missing.len())?,
            "only missing parts {missing:?}"
        );
        assert_uploaded_parts(&protocol, &missing).await?;
        assert_eq!(outcome.transferred_bytes, payload.len() as u64);
        assert_eq!(outcome.blake3, Some(*blake3::hash(&payload).as_bytes()));
        assert_eq!(protocol.objects.lock().await.get(final_key), Some(&payload));
        assert!(leaves_nothing_behind(&protocol, final_key).await?);
    }
    Ok(())
}

/// ADR-0006 C16: a transfer cut before its first checkpoint — at a 25 MiB interval, part 3 of 4
/// fails once parts 1 and 2 are stored — already has recoverable H intent. A later attempt
/// preserves every listed part, including an island beyond the failed part.
#[tokio::test]
async fn a_transfer_killed_before_its_first_checkpoint_resumes() -> TestResult {
    const PART: usize = 8 * MIB;
    let final_key = "dir/early";
    let (protocol, payload) = seeded(4 * PART, final_key).await;
    *protocol.part_failure.lock().await = Some((3, reset()));
    *protocol.part_failure_waits.lock().await = true;
    let request = checkpointed_every(&protocol, final_key, 3 * PART + MIB)?;
    let batches = hash_batches(&request, payload.len())?;
    let failure = transfer(request)
        .await
        .err()
        .ok_or("the failed part must cut the transfer")?;
    assert!(failure.has_unpublished_stage() && failure.has_recoverable_stage());
    drop(failure);
    assert_eq!(
        *protocol.puts.lock().await,
        batches + 2,
        "prepare and write-ahead H"
    );

    *protocol.part_failure.lock().await = None;
    let completed = completed_parts(&protocol, final_key).await?;
    completed.assert_third_failed();
    let missing = completed.missing(payload.len(), PART);
    *protocol.body_fragments.lock().await = Some(Vec::new());
    let sent = *protocol.part_uploads.lock().await;
    let outcome = transfer(checkpointed_every(&protocol, final_key, 3 * PART + MIB)?).await?;
    let reused = completed.bytes();
    assert_eq!(outcome.prepare, PrepareFact::Resumed { bytes: reused });
    assert_eq!(outcome.reused_bytes, reused);
    assert_eq!(
        *protocol.part_uploads.lock().await,
        sent + u32::try_from(missing.len())?,
        "completed parts must not be reuploaded"
    );
    assert_uploaded_parts(&protocol, &missing).await?;
    assert_eq!(outcome.blake3, Some(*blake3::hash(&payload).as_bytes()));
    assert_eq!(protocol.objects.lock().await.get(final_key), Some(&payload));
    assert!(leaves_nothing_behind(&protocol, final_key).await?);
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
        .read_source()?
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
pub(super) async fn expert_run(
    protocol: &Arc<MemoryS3>,
    interval: u64,
    final_key: &str,
) -> TestResult<TransferOutcome> {
    let storage = || connect_at_destination(protocol.clone(), identity(), interval);
    let source = source_storage(protocol, "src")?;
    let observation = observed(&source).await?;
    let source_half = ExpertSourceSession::open(ExpertSourceRequest::new(
        source,
        observation.clone(),
        CancellationToken::new(),
    ))
    .await?;
    let session = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observation,
            source_half.offer().maximum_chunk_bytes,
            storage()?,
            StoragePath::new(final_key)?,
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
        let (protocol, payload) = seeded(24 * MIB, "source").await;
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
