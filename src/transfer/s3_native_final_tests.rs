//! Native S3→S3 copies to the final key through the engine (ADR-0006 C18), over the in-memory S3
//! with a 6 MiB single-copy limit, 5 MiB copy parts and a 10 MiB automatic interval (64 MiB each
//! for real connections): no temp key, resumable through the same `.upload` pointer as a streamed
//! upload, in either direction.

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::backends::s3::tests::{MemoryS3, endpoint_of};
use crate::storage::backends::s3::{
    S3Protocol as _, S3ProtocolFailure, connect_native_at_destination,
};
use crate::storage::{PrepareFact, RestartReason, Storage};
use crate::transfer::{
    InflightLimits, PayloadShapingPolicy, TransferPolicy, TransferRequest, TransferRoute, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const MIB: usize = 1024 * 1024;
/// The native copy part size of these destinations.
const PART: usize = 5 * MIB;
/// The streamed part size (the S3 default).
const STREAMED_PART: usize = 8 * MIB;
const INTERVAL: usize = 2 * PART;

async fn seeded(sources: &[(&str, usize)]) -> (Arc<MemoryS3>, Vec<Bytes>) {
    let protocol = Arc::new(MemoryS3::default());
    let mut payloads = Vec::new();
    for (index, (key, size)) in sources.iter().enumerate() {
        let payload: Vec<u8> = (0..=250_u8).cycle().skip(index).take(*size).collect();
        let payload = Bytes::from(payload);
        protocol
            .objects
            .lock()
            .await
            .insert((*key).to_string(), payload.clone());
        payloads.push(payload);
    }
    (protocol, payloads)
}

/// A connection to the in-memory bucket with this file's native sizes.
fn storage(protocol: &Arc<MemoryS3>) -> TestResult<Storage> {
    connect_native_at_destination(
        protocol.clone(),
        endpoint_of(protocol),
        INTERVAL as u64,
        ((PART + MIB) as u64, PART as u64),
    )
}

fn request_with(
    protocol: &Arc<MemoryS3>,
    (source, final_key): (&str, &str),
    policy: TransferPolicy,
    cancel: CancellationToken,
) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        storage(protocol)?,
        StoragePath::new(source)?,
        storage(protocol)?,
        StoragePath::new(final_key)?,
        InflightLimits::new(4, 4 * MIB, 4)?,
        cancel,
    )
    .with_transfer_policy(policy))
}

fn native(protocol: &Arc<MemoryS3>, final_key: &str) -> TestResult<TransferRequest> {
    request_with(
        protocol,
        ("source", final_key),
        TransferPolicy::Checkpointed,
        CancellationToken::new(),
    )
}

fn streamed(protocol: &Arc<MemoryS3>, final_key: &str) -> TestResult<TransferRequest> {
    Ok(
        native(protocol, final_key)?
            .with_payload_shaping(PayloadShapingPolicy::RequireClientShaped),
    )
}

/// Nothing but sources and final objects: no pointer, no temp key, no upload on `final_key`.
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

fn reset() -> S3ProtocolFailure {
    S3ProtocolFailure::session(FailureClass::Connectivity, Transience::Transient, "reset")
}

/// Runs `request` with part 3 failing once parts 1 and 2 are stored, then drops the failure (a
/// writer that dies): the upload and its pointer stay on the key.
async fn cut_at_part_three(protocol: &MemoryS3, request: TransferRequest) -> TestResult {
    *protocol.part_failure.lock().await = Some((3, reset()));
    *protocol.part_failure_waits.lock().await = true;
    let failure = transfer(request)
        .await
        .err()
        .ok_or("the failed part must cut the transfer")?;
    assert!(!failure.final_destination_changed());
    assert!(failure.has_recoverable_stage());
    drop(failure);
    *protocol.part_failure.lock().await = None;
    Ok(())
}

/// Up to the single-copy limit a native copy is one `CopyObject` to the final key at publication,
/// for either policy: no upload, no pointer, no temp key.
#[tokio::test]
async fn a_small_native_copy_is_one_copy_object_to_the_final_key() -> TestResult {
    let (protocol, payloads) = seeded(&[("source", 4 * MIB)]).await;
    for policy in [TransferPolicy::Checkpointed, TransferPolicy::AtomicReplace] {
        let request = request_with(
            &protocol,
            ("source", "small"),
            policy,
            CancellationToken::new(),
        )?;
        let outcome = transfer(request).await?;
        assert_eq!(outcome.route, TransferRoute::Native, "{policy:?}");
        assert_eq!(outcome.prepare, PrepareFact::Fresh);
        assert_eq!(outcome.source_qos.native_requests, 1);
        assert_eq!(outcome.blake3, Some(*blake3::hash(&payloads[0]).as_bytes()));
    }
    assert_eq!(*protocol.native_copies.lock().await, 2);
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    assert_eq!(*protocol.puts.lock().await, 0);
    assert_eq!(
        protocol.objects.lock().await.get("small"),
        Some(&payloads[0])
    );
    assert!(leaves_nothing_behind(&protocol, "small").await?);
    Ok(())
}

/// Above it, an upload on the final key filled with `UploadPartCopy` and completed at
/// publication; a checkpointed copy writes its pointer once, an atomic one none.
#[tokio::test]
async fn a_large_native_copy_fills_an_upload_on_the_final_key() -> TestResult {
    let size = 4 * PART + 3 * MIB;
    let (protocol, payloads) = seeded(&[("source", size)]).await;
    for (policy, pointers) in [
        (TransferPolicy::Checkpointed, 1),
        (TransferPolicy::AtomicReplace, 0),
    ] {
        let puts = *protocol.puts.lock().await;
        let request = request_with(
            &protocol,
            ("source", "large"),
            policy,
            CancellationToken::new(),
        )?;
        let outcome = transfer(request).await?;
        assert_eq!(outcome.route, TransferRoute::Native, "{policy:?}");
        assert_eq!(outcome.prepare, PrepareFact::Fresh);
        assert_eq!(outcome.source_qos.native_bytes, size as u64);
        assert_eq!(outcome.source_qos.native_requests, 5);
        assert_eq!(*protocol.puts.lock().await, puts + pointers, "{policy:?}");
        assert_eq!(
            protocol.objects.lock().await.get("large"),
            Some(&payloads[0])
        );
        assert!(leaves_nothing_behind(&protocol, "large").await?);
    }
    assert_eq!(*protocol.native_copies.lock().await, 0);
    assert_eq!(*protocol.multipart_begins.lock().await, 2);
    assert_eq!(*protocol.part_copies.lock().await, 10);
    assert_eq!(*protocol.completes.lock().await, 2);
    Ok(())
}

/// A copy cut at part 3 of 5 is resumed by a fresh `transfer`, with nothing kept where
/// data-mover runs: the two listed parts are reused and only parts 3–5 are copied again.
#[tokio::test]
async fn an_interrupted_native_copy_resumes_from_the_parts_it_copied() -> TestResult {
    let (protocol, payloads) = seeded(&[("source", 4 * PART + 3 * MIB)]).await;
    cut_at_part_three(&protocol, native(&protocol, "cut")?).await?;
    assert_eq!(*protocol.puts.lock().await, 1, "one pointer");
    assert!(!protocol.objects.lock().await.contains_key("cut"));
    let copied = *protocol.part_copies.lock().await;

    let outcome = transfer(native(&protocol, "cut")?).await?;
    let reused = 2 * PART as u64;
    assert_eq!(outcome.prepare, PrepareFact::Resumed { bytes: reused });
    assert_eq!(outcome.reused_bytes, reused);
    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(*protocol.part_copies.lock().await, copied + 3);
    assert_eq!(protocol.objects.lock().await.get("cut"), Some(&payloads[0]));
    assert!(leaves_nothing_behind(&protocol, "cut").await?);
    Ok(())
}

/// A cancellation starts no more parts but lets those in flight finish: every part they copied
/// is reused by the next attempt.
#[tokio::test]
async fn a_cancelled_native_copy_keeps_the_parts_in_flight() -> TestResult {
    let size = 10 * PART + 2 * MIB;
    let (protocol, payloads) = seeded(&[("source", size)]).await;
    let cancel = CancellationToken::new();
    protocol.cancel_after_part_copy(2, cancel.clone()).await;
    let request = request_with(
        &protocol,
        ("source", "cancelled"),
        TransferPolicy::Checkpointed,
        cancel,
    )?;
    let failure = transfer(request)
        .await
        .err()
        .ok_or("the cancellation must stop the copy")?;
    assert!(!failure.final_destination_changed());
    drop(failure);
    let copied = u64::from(*protocol.part_copies.lock().await);
    assert!((6..11).contains(&copied), "{copied} parts copied");

    let outcome = transfer(native(&protocol, "cancelled")?).await?;
    let PrepareFact::Resumed { bytes } = outcome.prepare else {
        return Err(format!("not resumed: {:?}", outcome.prepare).into());
    };
    // The six parts started before the cancellation (a seventh may follow part 1) all finished.
    assert!(bytes >= 6 * PART as u64 && bytes < size as u64, "{bytes}");
    assert_eq!(bytes % PART as u64, 0);
    assert_eq!(
        protocol.objects.lock().await.get("cancelled"),
        Some(&payloads[0])
    );
    assert!(leaves_nothing_behind(&protocol, "cancelled").await?);
    Ok(())
}

/// A streamed attempt's upload (8 MiB parts) is resumed by a native copy at that part size.
#[tokio::test]
async fn a_native_copy_resumes_a_streamed_upload_at_its_part_size() -> TestResult {
    let size = 3 * STREAMED_PART + 2 * MIB;
    let (protocol, payloads) = seeded(&[("source", size)]).await;
    cut_at_part_three(&protocol, streamed(&protocol, "mixed")?).await?;

    let outcome = transfer(native(&protocol, "mixed")?).await?;
    let reused = 2 * STREAMED_PART as u64;
    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(outcome.prepare, PrepareFact::Resumed { bytes: reused });
    assert_eq!(outcome.source_qos.native_bytes, size as u64 - reused);
    assert_eq!(*protocol.part_copies.lock().await, 2, "8 MiB and 2 MiB");
    assert_eq!(
        protocol.objects.lock().await.get("mixed"),
        Some(&payloads[0])
    );
    assert!(leaves_nothing_behind(&protocol, "mixed").await?);
    Ok(())
}

/// And a native copy's upload (5 MiB parts here) is resumed by a streamed attempt at that size.
#[tokio::test]
async fn a_streamed_attempt_resumes_a_native_upload_at_its_part_size() -> TestResult {
    let (protocol, payloads) = seeded(&[("source", 4 * PART + 3 * MIB)]).await;
    cut_at_part_three(&protocol, native(&protocol, "back")?).await?;

    let outcome = transfer(streamed(&protocol, "back")?).await?;
    let reused = 2 * PART as u64;
    assert_eq!(outcome.route, TransferRoute::Streaming);
    assert_eq!(outcome.prepare, PrepareFact::Resumed { bytes: reused });
    assert_eq!(
        *protocol.part_uploads.lock().await,
        3,
        "parts 3 to 5 of 5 MiB"
    );
    assert_eq!(
        protocol.objects.lock().await.get("back"),
        Some(&payloads[0])
    );
    assert!(leaves_nothing_behind(&protocol, "back").await?);
    Ok(())
}

/// What a native copy may not resume — another transfer's upload and pointer, or anything when
/// the policy is atomic — is cleaned up in place before it copies, for both copy shapes.
#[tokio::test]
async fn a_native_copy_cleans_up_what_it_cannot_resume() -> TestResult {
    let big = 4 * PART + 3 * MIB;
    for (source, policy, reason) in [
        (
            "small",
            TransferPolicy::Checkpointed,
            RestartReason::OtherTransfer,
        ),
        (
            "other",
            TransferPolicy::Checkpointed,
            RestartReason::OtherTransfer,
        ),
        (
            "big",
            TransferPolicy::AtomicReplace,
            RestartReason::Requested,
        ),
    ] {
        let (protocol, payloads) =
            seeded(&[("big", big), ("small", 4 * MIB), ("other", big + MIB)]).await;
        let leftover = request_with(
            &protocol,
            ("big", "key"),
            TransferPolicy::Checkpointed,
            CancellationToken::new(),
        )?;
        cut_at_part_three(
            &protocol,
            leftover.with_payload_shaping(PayloadShapingPolicy::RequireClientShaped),
        )
        .await?;
        let request = request_with(&protocol, (source, "key"), policy, CancellationToken::new())?;
        let outcome = transfer(request).await?;
        assert_eq!(outcome.route, TransferRoute::Native, "{source}");
        assert_eq!(
            outcome.prepare,
            PrepareFact::Restarted { reason },
            "{source}"
        );
        assert!(*protocol.aborts.lock().await >= 1, "{source}");
        let expected = ["big", "small", "other"]
            .iter()
            .position(|name| *name == source);
        let stored = protocol.objects.lock().await.get("key").cloned();
        assert_eq!(stored.as_ref(), expected.map(|index| &payloads[index]));
        assert!(leaves_nothing_behind(&protocol, "key").await?, "{source}");
    }
    Ok(())
}

/// A copy whose parts are all copied but that is cancelled before publication leaves the final
/// key as it was; discarding the failure removes the pointer and aborts the upload.
#[tokio::test]
async fn a_native_copy_cancelled_before_publication_leaves_the_final_key() -> TestResult {
    let (protocol, _) = seeded(&[("source", 4 * PART + 3 * MIB)]).await;
    let previous = Bytes::from_static(b"previous");
    protocol
        .objects
        .lock()
        .await
        .insert("kept".into(), previous.clone());
    let cancel = CancellationToken::new();
    protocol.cancel_after_part_copy(5, cancel.clone()).await;
    let request = request_with(
        &protocol,
        ("source", "kept"),
        TransferPolicy::Checkpointed,
        cancel,
    )?;
    let failure = transfer(request)
        .await
        .err()
        .ok_or("the cancellation must stop the transfer")?;
    assert!(!failure.final_destination_changed());
    assert!(failure.has_unpublished_stage());
    assert_eq!(*protocol.part_copies.lock().await, 5);
    assert_eq!(*protocol.completes.lock().await, 0);
    failure.discard_stage().await?;
    assert_eq!(protocol.objects.lock().await.get("kept"), Some(&previous));
    assert!(leaves_nothing_behind(&protocol, "kept").await?);
    Ok(())
}

/// The transfer's operation bound caps the `UploadPartCopy` requests in flight: a bound of 2
/// never has more than 2, and a bound of 8 still no more than the six a copy allows.
#[tokio::test]
async fn native_part_copies_stay_within_the_operation_bound() -> TestResult {
    for (operations, peak) in [(2, 2), (8, 6)] {
        let (protocol, payloads) = seeded(&[("source", 10 * PART)]).await;
        let request = TransferRequest::new(
            storage(&protocol)?,
            StoragePath::new("source")?,
            storage(&protocol)?,
            StoragePath::new("bounded")?,
            InflightLimits::new(4, 4 * MIB, operations)?,
            CancellationToken::new(),
        );
        let outcome = transfer(request).await?;
        assert_eq!(outcome.route, TransferRoute::Native);
        assert_eq!(*protocol.part_copies.lock().await, 10);
        assert_eq!(
            *protocol.part_copies_peak.lock().await,
            peak,
            "{operations}"
        );
        assert_eq!(
            protocol.objects.lock().await.get("bounded"),
            Some(&payloads[0])
        );
    }
    Ok(())
}

/// A streamed upload cut and left with its pointer, then a source replaced by other bytes of the
/// same size: a native attempt must not resume the old parts (the binding changed), and the final
/// object is the new source.
#[tokio::test]
async fn a_native_copy_restarts_over_a_streamed_upload_of_a_changed_source() -> TestResult {
    let size = 3 * STREAMED_PART + 2 * MIB;
    let (protocol, _) = seeded(&[("source", size)]).await;
    cut_at_part_three(&protocol, streamed(&protocol, "changed")?).await?;
    let replaced = Bytes::from(vec![0x42; size]);
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), replaced.clone());

    let outcome = transfer(native(&protocol, "changed")?).await?;
    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(
        outcome.prepare,
        PrepareFact::Restarted {
            reason: RestartReason::BindingChanged
        }
    );
    assert_eq!(outcome.reused_bytes, 0);
    assert_eq!(
        protocol.objects.lock().await.get("changed"),
        Some(&replaced)
    );
    assert!(leaves_nothing_behind(&protocol, "changed").await?);
    Ok(())
}
