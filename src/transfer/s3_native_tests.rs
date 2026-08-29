use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::RecoveryIdentity;
use crate::storage::backends::s3::tests::{MemoryS3, identity, native_context};
use crate::storage::backends::s3::{S3NativeContext, S3ProtocolFailure, connect};
use crate::transfer::{
    InflightLimits, PayloadShapingPolicy, RecoveryPolicy, SourceQosGroup, SourceQosPolicy,
    TransferIdentity, TransferRequest, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn request(
    source: crate::storage::Storage,
    destination: crate::storage::Storage,
) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        TransferIdentity::new("native-s3")?,
        source,
        StoragePath::new("source")?,
        destination,
        StoragePath::new("final")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        CancellationToken::new(),
    ))
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
    let source = connect(protocol.clone(), identity(), Some(native_context()))?;
    let destination = connect(protocol.clone(), identity(), Some(native_context()))?;
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
async fn strict_shaping_falls_back_before_native_mutation() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"stream me"));
    let source = connect(protocol.clone(), identity(), Some(native_context()))?;
    let destination = connect(protocol.clone(), identity(), Some(native_context()))?;
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
    let source = connect(protocol.clone(), identity(), Some(native_context()))?;
    let other = Some(S3NativeContext::new(
        "memory://other",
        "standard",
        "memory".into(),
        None,
    ));
    let destination = connect(protocol.clone(), identity(), other)?;

    let outcome = transfer(request(source, destination)?).await?;

    assert_eq!(outcome.source_qos.native_requests, 0);
    assert_eq!(*protocol.native_copies.lock().await, 0);
    Ok(())
}

#[tokio::test]
async fn native_failure_retains_cleanup_authority_without_changing_final() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"failure"));
    *protocol.native_failure.lock().await = Some(S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "injected native failure",
    ));
    let source = connect(protocol.clone(), identity(), Some(native_context()))?;
    let destination = connect(protocol.clone(), identity(), Some(native_context()))?;
    let qos = SourceQosGroup::new(SourceQosPolicy::new(None, 2, None)?);

    let Err(error) = transfer(request(source, destination)?.with_source_qos(qos)).await else {
        return Err("native failure unexpectedly succeeded".into());
    };

    assert!(error.has_recoverable_stage());
    assert!(!error.final_destination_changed());
    assert_eq!(error.source_qos().native_bytes, 0);
    assert_eq!(error.source_qos().native_requests, 1);
    assert_eq!(error.source_qos().client_streamed_shaped_bytes, 7);
    assert_eq!(error.source_qos().source_read_operations, 4);
    assert!(!protocol.objects.lock().await.contains_key("final"));
    error.discard_stage().await?;
    Ok(())
}

#[tokio::test]
async fn supplied_recovery_identity_uses_streaming_recovery_path() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"restart"));
    let source = connect(protocol.clone(), identity(), Some(native_context()))?;
    let destination = connect(protocol.clone(), identity(), Some(native_context()))?;
    let stale = RecoveryIdentity::from_bytes(Bytes::from_static(b"invalid"))?;

    let outcome = transfer(
        request(source, destination)?.with_recovery(RecoveryPolicy::ResumeOrRestart, Some(stale)),
    )
    .await?;

    assert_eq!(outcome.source_qos.native_requests, 0);
    assert_eq!(*protocol.native_copies.lock().await, 0);
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
    let source = connect(protocol.clone(), identity(), Some(native_context()))?;
    let destination = connect(protocol.clone(), identity(), Some(native_context()))?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut request = request(source, destination)?;
    request = TransferRequest::new(
        TransferIdentity::new("cancel-native")?,
        request.source,
        StoragePath::new("source")?,
        request.destination,
        StoragePath::new("final")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        cancel,
    );

    assert!(transfer(request).await.is_err());
    assert_eq!(*protocol.native_copies.lock().await, 0);
    assert!(!protocol.objects.lock().await.contains_key("final"));
    Ok(())
}
