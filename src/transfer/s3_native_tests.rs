use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::PrepareFact;
use crate::storage::backends::s3::tests::{MemoryS3, endpoint_of, native_context};
use crate::storage::backends::s3::{S3NativeContext, S3Protocol as _, S3ProtocolFailure, connect};
use crate::transfer::{
    EffectiveRecovery, InflightLimits, PayloadShapingPolicy, ReadBackVerification, SourceQosGroup,
    SourceQosPolicy, TransferIdentity, TransferPolicy, TransferRequest, TransferRoute, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn request(
    source: crate::storage::Storage,
    destination: crate::storage::Storage,
) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        source,
        StoragePath::new("source")?,
        destination,
        StoragePath::new("final")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        CancellationToken::new(),
    )
    .with_identity_override(TransferIdentity::from_label("native-s3")?))
}

#[tokio::test]
async fn same_connected_pair_uses_native_stage_and_reports_unshaped_payload() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from_static(b"native payload");
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), payload.clone());
    let source = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let destination = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let qos = SourceQosGroup::new(SourceQosPolicy::new(None, 4, None)?);

    let outcome = transfer(request(source, destination)?.with_source_qos(qos)).await?;

    assert_eq!(outcome.source_qos.logical_bytes, payload.len() as u64);
    assert_eq!(outcome.source_qos.native_bytes, payload.len() as u64);
    assert_eq!(outcome.source_qos.native_requests, 1);
    assert_eq!(outcome.source_qos.source_read_operations, 4);
    assert_eq!(
        outcome.source_qos.client_streamed_shaped_bytes,
        payload.len() as u64
    );
    assert!(!outcome.source_qos.native_payload_shaped);
    assert_eq!(*protocol.native_copies.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    Ok(())
}

#[tokio::test]
async fn disabled_read_back_native_copy_skips_client_source_hashing() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from(vec![0x5a; 256 * 1024]);
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), payload.clone());
    let source = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let destination = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let qos = SourceQosGroup::new(SourceQosPolicy::new(None, 4, None)?);

    let outcome = transfer(
        request(source, destination)?
            .with_source_qos(qos)
            .with_read_back_verification(ReadBackVerification::Disabled),
    )
    .await?;

    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(outcome.blake3, None);
    assert_eq!(outcome.source_qos.source_read_operations, 0);
    assert_eq!(outcome.source_qos.client_streamed_shaped_bytes, 0);
    assert_eq!(outcome.source_qos.native_bytes, payload.len() as u64);
    assert_eq!(outcome.source_qos.native_requests, 1);
    assert_eq!(*protocol.native_copies.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    Ok(())
}

#[tokio::test]
async fn disabled_read_back_reconciles_a_committed_native_publication() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from(vec![0x6b; 256 * 1024]);
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), payload.clone());
    *protocol.copy_commits_then_fails.lock().await = true;
    let source = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let destination = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let qos = SourceQosGroup::new(SourceQosPolicy::new(None, 4, None)?);

    let outcome = transfer(
        request(source, destination)?
            .with_source_qos(qos)
            .with_read_back_verification(ReadBackVerification::Disabled),
    )
    .await?;

    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(outcome.blake3, None);
    assert_eq!(outcome.source_qos.source_read_operations, 0);
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    Ok(())
}

#[tokio::test]
async fn native_copy_never_enables_streaming_recovery() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from(vec![0x31; 128 * 1024]);
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), payload.clone());
    let source = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let destination = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;

    let outcome = transfer(request(source, destination)?).await?;

    assert_eq!(outcome.transferred_bytes, payload.len() as u64);
    assert_eq!(outcome.source_qos.native_bytes, payload.len() as u64);
    assert_eq!(outcome.source_qos.native_requests, 1);
    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(outcome.recovery, EffectiveRecovery::NotApplicableNative);
    assert_eq!(*protocol.native_copies.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    Ok(())
}

#[tokio::test]
async fn strict_shaping_falls_back_before_native_mutation() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"stream me"));
    let source = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let destination = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let outcome = transfer(
        request(source, destination)?
            .with_payload_shaping(PayloadShapingPolicy::RequireClientShaped),
    )
    .await?;

    assert_eq!(outcome.source_qos.native_bytes, 0);
    assert_eq!(*protocol.native_copies.lock().await, 0);
    assert_eq!(
        protocol.objects.lock().await.get("final"),
        Some(&Bytes::from_static(b"stream me"))
    );
    Ok(())
}

#[tokio::test]
async fn different_endpoint_affinity_falls_back_to_streaming() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"fallback"));
    let source = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let other = Some(S3NativeContext::new(
        "memory://other",
        "standard",
        "memory".into(),
        None,
    ));
    let destination = connect(protocol.clone(), endpoint_of(&protocol), other)?;

    let outcome = transfer(request(source, destination)?).await?;

    assert_eq!(outcome.source_qos.native_requests, 0);
    assert_eq!(*protocol.native_copies.lock().await, 0);
    Ok(())
}

/// A small native copy whose one `CopyObject` (sent at publication, ADR-0006 C18) fails with
/// `failure` before it copies anything; the failure, and the fake.
async fn failed_small_native_copy(
    failure: S3ProtocolFailure,
) -> TestResult<(crate::transfer::TransferFailure, Arc<MemoryS3>)> {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"failure"));
    *protocol.native_failure.lock().await = Some(failure);
    let storage = || {
        connect(
            protocol.clone(),
            endpoint_of(&protocol),
            Some(native_context()),
        )
    };
    let qos = SourceQosGroup::new(SourceQosPolicy::new(None, 2, None)?);
    let Err(error) = transfer(request(storage()?, storage()?)?.with_source_qos(qos)).await else {
        return Err("native failure unexpectedly succeeded".into());
    };
    // The copy was counted when the stage was filled; publication sent it.
    assert_eq!(error.source_qos().native_requests, 1);
    assert_eq!(error.source_qos().client_streamed_shaped_bytes, 7);
    assert_eq!(error.source_qos().source_read_operations, 4);
    assert!(!protocol.objects.lock().await.contains_key("final"));
    assert_eq!(*protocol.native_copies.lock().await, 0);
    Ok((error, protocol))
}

/// A refusal the service answers before copying leaves the final key unchanged and the stage with
/// the failure; its discard aborts nothing and leaves no artifact.
#[tokio::test]
async fn native_failure_retains_cleanup_authority_without_changing_final() -> TestResult {
    let denied = S3ProtocolFailure::entry(
        FailureClass::PermissionDenied,
        Transience::Permanent,
        "injected native refusal",
    );
    let (error, protocol) = failed_small_native_copy(denied).await?;
    assert!(!error.has_recoverable_stage());
    assert!(error.has_unpublished_stage());
    assert!(!error.final_destination_changed());
    error.discard_stage().await?;
    assert_eq!(*protocol.aborts.lock().await, 0);
    let objects = protocol.objects.lock().await;
    assert!(!objects.keys().any(|key| key.contains(".data-mover-")));
    Ok(())
}

/// A transport failure (no answer) may have copied: with nothing of the source's size and `ETag`
/// at the final key it cannot be settled, so the final key is reported changed.
#[tokio::test]
async fn an_unanswered_native_copy_reports_the_final_key_changed() -> TestResult {
    let reset = S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "injected native failure",
    );
    let (error, _) = failed_small_native_copy(reset).await?;
    assert!(error.final_destination_changed());
    assert!(!error.has_unpublished_stage());
    Ok(())
}

#[tokio::test]
async fn cancellation_before_planning_performs_no_native_or_final_mutation() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"cancel"));
    let source = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let destination = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut request = request(source, destination)?;
    request = TransferRequest::new(
        request.source,
        StoragePath::new("source")?,
        request.destination,
        StoragePath::new("final")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        cancel,
    )
    .with_identity_override(TransferIdentity::from_label("cancel-native")?);

    assert!(transfer(request).await.is_err());
    assert_eq!(*protocol.native_copies.lock().await, 0);
    assert!(!protocol.objects.lock().await.contains_key("final"));
    Ok(())
}

/// An unversioned bucket, or an object written before versioning, reports `versionId` `"null"` (or
/// nothing). Describe identifies such an object by its `ETag`, so the native binding must too, or the
/// same untouched object reads as "changed" and every native copy of it fails.
#[tokio::test]
async fn native_binding_accepts_a_null_or_empty_version_id() -> TestResult {
    // A real version is the control: it is identified by itself and must still bind.
    for version in [None, Some("null"), Some(""), Some("version-1")] {
        let protocol = Arc::new(MemoryS3::default());
        *protocol.version.lock().await = version.map(str::to_string);
        let payload = Bytes::from_static(b"unversioned payload");
        protocol
            .objects
            .lock()
            .await
            .insert("source".into(), payload.clone());
        let source = connect(
            protocol.clone(),
            endpoint_of(&protocol),
            Some(native_context()),
        )?;
        let destination = connect(
            protocol.clone(),
            endpoint_of(&protocol),
            Some(native_context()),
        )?;

        let outcome = transfer(request(source, destination)?).await?;

        assert_eq!(outcome.route, TransferRoute::Native, "{version:?}");
        assert_eq!(*protocol.native_copies.lock().await, 1, "{version:?}");
        assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    }
    Ok(())
}

/// With recovery kept at the destination (ADR-0006 C18), a native copy above the single-PUT
/// threshold but within the 64 MiB single-copy limit is one `CopyObject` to the final key at
/// publication — no temp key, no upload, no pointer — whatever the policy.
#[tokio::test]
async fn a_native_copy_up_to_64_mib_is_one_copy_to_the_final_key() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from((0..=250_u8).cycle().take(9 << 20).collect::<Vec<_>>());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), payload.clone());
    let source = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    let destination = connect(
        protocol.clone(),
        endpoint_of(&protocol),
        Some(native_context()),
    )?;
    for policy in [TransferPolicy::Checkpointed, TransferPolicy::AtomicReplace] {
        let request = TransferRequest::new(
            source.clone(),
            StoragePath::new("source")?,
            destination.clone(),
            StoragePath::new("final")?,
            InflightLimits::new(2, 4 << 20, 2)?,
            CancellationToken::new(),
        )
        .with_transfer_policy(policy);
        let outcome = transfer(request).await?;
        assert_eq!(outcome.route, TransferRoute::Native, "{policy:?}");
        assert_eq!(outcome.prepare, PrepareFact::Fresh);
        assert_eq!(outcome.recovery, EffectiveRecovery::NotApplicableNative);
        assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    }
    assert_eq!(*protocol.native_copies.lock().await, 2);
    assert_eq!(*protocol.puts.lock().await, 0);
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    let open = protocol.list_uploads("final").await;
    assert!(open.is_ok_and(|uploads| uploads.is_empty()));
    let objects = protocol.objects.lock().await;
    assert!(!objects.keys().any(|key| key.contains(".data-mover-")));
    Ok(())
}
