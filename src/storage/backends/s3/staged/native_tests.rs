use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity, StoragePath};
use crate::storage::PrepareRequest;
use crate::storage::backends::s3::{S3_NATIVE_COPY_SINGLE_MAX, S3NativeCopySource};
use crate::storage::{FinalDestination, SourceDescriptor};

use super::super::tests::{MemoryS3, identity};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn native_prepare() -> TestResult<PrepareRequest> {
    Ok(PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new("large-final")?),
        source: SourceDescriptor {
            path: StoragePath::new("large-source")?,
            kind: EntryKind::File,
            size: Some(S3_NATIVE_COPY_SINGLE_MAX + 1),
            source_identity: SourceIdentity::new(
                identity(),
                IdentityStrength::PathScoped,
                b"large-etag",
            )?,
            backend_fact: None,
        },
        recovery_binding: [5; 32],
    })
}

fn native_source() -> S3NativeCopySource {
    S3NativeCopySource {
        bucket: "source".into(),
        key: "large-source".into(),
        etag: "large-etag".into(),
        version_id: None,
        size: S3_NATIVE_COPY_SINGLE_MAX + 1,
    }
}

#[tokio::test]
async fn multipart_native_failure_retains_upload_for_discard_retry() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    *protocol.native_failure.lock().await = Some(S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "injected part failure",
    ));
    let adapter = S3StagedDestination::new(protocol.clone(), identity());
    let Err(failure) = adapter
        .prepare_native(native_prepare()?, native_source(), CancellationToken::new())
        .await
    else {
        return Err("multipart native failure unexpectedly succeeded".into());
    };
    assert_eq!(failure.native_bytes, 0);
    assert_eq!(failure.native_requests, 2);
    let stage = failure.stage.ok_or("native failure lost stage authority")?;
    assert_eq!(protocol.uploads.lock().await.len(), 1);
    adapter.discard(stage).await?;
    assert!(protocol.uploads.lock().await.is_empty());
    assert_eq!(*protocol.aborts.lock().await, 1);
    Ok(())
}

#[tokio::test]
async fn multipart_native_cancellation_keeps_abort_authority() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = S3StagedDestination::new(protocol.clone(), identity());
    let cancel = CancellationToken::new();
    cancel.cancel();
    let Err(failure) = adapter
        .prepare_native(native_prepare()?, native_source(), cancel)
        .await
    else {
        return Err("cancelled multipart native copy succeeded".into());
    };
    assert_eq!(failure.native_bytes, 0);
    assert_eq!(failure.native_requests, 1);
    adapter
        .discard(failure.stage.ok_or("cancel lost stage authority")?)
        .await?;
    assert!(protocol.uploads.lock().await.is_empty());
    assert_eq!(*protocol.aborts.lock().await, 1);
    Ok(())
}
