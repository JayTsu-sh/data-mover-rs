//! The streamed single `PutObject` (P7 §14.2 N3) over the in-memory bucket: the last byte waits
//! for publication, so nothing before it changes the final key; the watchdog, the MD5 check and
//! the settling of a lost reply.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::super::super::tests::{MemoryS3, content_md5, etag_of, identity};
use super::super::S3StagedDestination;
use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity};
use crate::storage::{
    DestinationPrepareRequest, FinalDestination, ResumeMode, SourceDescriptor, StagedDestination,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const MIB: usize = 1024 * 1024;
/// Over the 8 MiB single-PUT threshold.
const SIZE: usize = 9 * MIB + 3;

fn payload() -> Bytes {
    Bytes::from((0..=250_u8).cycle().take(SIZE).collect::<Vec<_>>())
}

fn prepare_request(size: usize) -> TestResult<PrepareRequest> {
    Ok(PrepareRequest {
        source: SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            Some(size as u64),
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
        ),
        final_destination: FinalDestination::new(StoragePath::new("dir/final")?),
        recovery_binding: [9; 32],
    })
}

/// A NAS bucket (optionally holding `old` at the final key) and its destination role, with a
/// short stall timeout and hold limit.
async fn nas(
    old: Option<&'static [u8]>,
) -> TestResult<(Arc<MemoryS3>, S3StagedDestination<MemoryS3>)> {
    let protocol = Arc::new(MemoryS3::nas_bucket());
    protocol.streaming().stall_timeout = Some(Duration::from_millis(300));
    if let Some(old) = old {
        let body = Bytes::from_static(old);
        protocol
            .put_object("dir/final", body.clone(), &content_md5(&body))
            .await
            .map_err(|failure| format!("{failure:?}"))?;
    }
    let destination = S3StagedDestination::new(protocol.clone(), identity())
        .with_hold_limit(Duration::from_millis(400), Duration::from_millis(20));
    Ok((protocol, destination))
}

async fn prepare(destination: &S3StagedDestination<MemoryS3>) -> TestResult<PreparedStage> {
    let request = DestinationPrepareRequest::new(prepare_request(SIZE)?, [4; 32])
        .with_resume(ResumeMode::Restart);
    Ok(destination.prepare_at_destination(request).await?)
}

/// `bytes` in 1 MiB chunks.
fn chunks(bytes: &Bytes) -> ByteStream {
    let pieces: Vec<_> = bytes
        .chunks(MIB)
        .map(|piece| Ok(Bytes::copy_from_slice(piece)))
        .collect();
    Box::pin(stream::iter(pieces))
}

fn publish_request() -> PublishRequest {
    PublishRequest {
        expected_size: SIZE as u64,
        expected_blake3: None,
        cancel: CancellationToken::new(),
    }
}

async fn stored(protocol: &MemoryS3) -> Option<Bytes> {
    protocol.objects.lock().await.get("dir/final").cloned()
}

fn class_of(error: &StorageRoleFailure) -> (FailureClass, Transience) {
    match error {
        StorageRoleFailure::Entry(failure) => (failure.class(), failure.transience()),
        StorageRoleFailure::Session(failure) => (failure.class(), failure.transience()),
    }
}

/// `write` sends all but the last byte and commits nothing; `publish` sends it, and the object —
/// one request, no multipart upload — commits with our MD5 as the reply's `ETag`.
#[tokio::test]
async fn the_last_byte_waits_for_publication() -> TestResult {
    let (protocol, destination) = nas(Some(b"old")).await?;
    let stage = prepare(&destination).await?;
    assert!(!stage.recovery_enabled());
    let bytes = payload();
    let written = destination.write(&stage, chunks(&bytes)).await?;
    assert_eq!(written.persisted_bytes, SIZE as u64);
    // The last chunk `write` sent may still wait in the channel: give the server a moment.
    for _ in 0..100 {
        if protocol.streaming().received == SIZE as u64 - 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(protocol.streaming().received, SIZE as u64 - 1);
    assert_eq!(stored(&protocol).await.as_deref(), Some(&b"old"[..]));
    let published = destination
        .publish(&stage, publish_request())
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(published.version, None);
    assert_eq!(stored(&protocol).await, Some(bytes.clone()));
    assert_eq!(protocol.streaming().puts, 1);
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    assert_eq!(
        of(&stage)
            .and_then(StreamingStage::published)
            .map(|facts| facts.etag),
        Some(etag_of(&bytes))
    );
    Ok(())
}

/// A stage discarded — or simply dropped — after `write` leaves the old object: the held byte
/// never went, the request is aborted, nothing committed.
#[tokio::test]
async fn a_stage_given_up_before_publication_changes_nothing() -> TestResult {
    for discard in [true, false] {
        let (protocol, destination) = nas(Some(b"old")).await?;
        let stage = prepare(&destination).await?;
        destination.write(&stage, chunks(&payload())).await?;
        if discard {
            destination.discard(stage).await?;
        } else {
            drop(stage);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            stored(&protocol).await.as_deref(),
            Some(&b"old"[..]),
            "discard {discard}"
        );
        assert!(protocol.streaming().received < SIZE as u64);
    }
    Ok(())
}

/// A source that fails mid-way fails the write and leaves the final key as it was.
#[tokio::test]
async fn a_source_failure_mid_way_changes_nothing() -> TestResult {
    let (protocol, destination) = nas(Some(b"old")).await?;
    let stage = prepare(&destination).await?;
    let bytes = payload();
    let failing: ByteStream = Box::pin(stream::iter([
        Ok(bytes.slice(..MIB)),
        Err(entry(
            stage.final_destination.path(),
            Operation::Read,
            "source broke",
        )),
    ]));
    assert!(destination.write(&stage, failing).await.is_err());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(stored(&protocol).await.as_deref(), Some(&b"old"[..]));
    Ok(())
}

/// A server that stops taking the body fails the write within the stall timeout (transient
/// `Connectivity`), with nothing committed; a byte held past the hold limit is given up, and
/// publication then reports the final key unchanged.
#[tokio::test]
async fn stalls_and_a_byte_held_too_long_are_given_up_unchanged() -> TestResult {
    let (protocol, destination) = nas(Some(b"old")).await?;
    protocol.streaming().stall = true;
    let stage = prepare(&destination).await?;
    let started = std::time::Instant::now();
    let Err(error) = destination.write(&stage, chunks(&payload())).await else {
        return Err("a stalled upload was written".into());
    };
    assert_eq!(
        class_of(&error),
        (FailureClass::Connectivity, Transience::Transient)
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(stored(&protocol).await.as_deref(), Some(&b"old"[..]));

    let (protocol, destination) = nas(Some(b"old")).await?;
    let stage = prepare(&destination).await?;
    destination.write(&stage, chunks(&payload())).await?;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let Err(failure) = destination.publish(&stage, publish_request()).await else {
        return Err("a byte held past the limit was published".into());
    };
    assert!(!failure.final_destination_changed);
    assert_eq!(
        class_of(&failure.error),
        (FailureClass::Connectivity, Transience::Transient)
    );
    assert_eq!(stored(&protocol).await.as_deref(), Some(&b"old"[..]));
    Ok(())
}

/// A reply lost after the commit, or never sent: on a NAS bucket the `HEAD` no longer answers what
/// prepare saw, so the final key changed; an `ETag` in the reply that is not our MD5 is a
/// transient `Corruption` with the final key changed.
#[tokio::test]
async fn an_uncertain_or_corrupt_publication_reports_the_final_key_changed() -> TestResult {
    for knob in ["lost", "hangs", "etag"] {
        let (protocol, destination) = nas(Some(b"old")).await?;
        let stage = prepare(&destination).await?;
        destination.write(&stage, chunks(&payload())).await?;
        match knob {
            "lost" => protocol.streaming().commits_then_fails = true,
            "hangs" => protocol.streaming().reply_hangs = true,
            _ => protocol.streaming().etag = Some(etag_of(b"something else")),
        }
        let Err(failure) = destination.publish(&stage, publish_request()).await else {
            return Err(format!("{knob}: published").into());
        };
        assert!(failure.final_destination_changed, "{knob}");
        if knob == "etag" {
            assert_eq!(
                class_of(&failure.error),
                (FailureClass::Corruption, Transience::Transient)
            );
        }
    }
    Ok(())
}

/// A reply lost after a request that did **not** commit: the key still answers what prepare saw,
/// so publication reports it unchanged. A request that fails before taking the body fails the
/// write itself.
#[tokio::test]
async fn an_uncommitted_request_is_reported_unchanged() -> TestResult {
    let (protocol, destination) = nas(Some(b"old")).await?;
    let stage = prepare(&destination).await?;
    destination.write(&stage, chunks(&payload())).await?;
    protocol.streaming().fails_after_body = true;
    let Err(failure) = destination.publish(&stage, publish_request()).await else {
        return Err("an uncommitted upload was published".into());
    };
    assert!(!failure.final_destination_changed);
    assert_eq!(stored(&protocol).await.as_deref(), Some(&b"old"[..]));

    let (protocol, destination) = nas(Some(b"old")).await?;
    let stage = prepare(&destination).await?;
    protocol.streaming().refuse = Some(S3ProtocolFailure::entry(
        FailureClass::Conflict,
        Transience::Permanent,
        "NameCollision",
    ));
    let Err(error) = destination.write(&stage, chunks(&payload())).await else {
        return Err("a refused upload was written".into());
    };
    assert_eq!(
        class_of(&error),
        (FailureClass::Conflict, Transience::Permanent)
    );
    assert_eq!(stored(&protocol).await.as_deref(), Some(&b"old"[..]));
    Ok(())
}

/// `Direct` streams every byte and commits inside `write`.
#[tokio::test]
async fn direct_commits_inside_write() -> TestResult {
    let (protocol, destination) = nas(None).await?;
    let stage = destination
        .prepare_direct(prepare_request(SIZE)?, CancellationToken::new())
        .await?;
    let bytes = payload();
    destination.write(&stage, chunks(&bytes)).await?;
    assert_eq!(stored(&protocol).await, Some(bytes));
    let published = destination
        .publish(&stage, publish_request())
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(published.version, None);
    Ok(())
}
