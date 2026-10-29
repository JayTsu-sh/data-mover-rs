//! `TransferRequest::with_source_version` through the engine (ADR-0006 C6c), over the in-memory S3.

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, StoragePath};
use crate::storage::StorageRoleFailure;
use crate::storage::backends::s3::connect;
use crate::storage::backends::s3::tests::{MemoryS3, endpoint_of, native_context, source_storage};
use crate::transfer::{
    SourceVersion, TransferIdentity, TransferPhase, TransferRequest, TransferRoute, TransferSide,
    transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

async fn versioned() -> Arc<MemoryS3> {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version("src/final", "v1", Bytes::from_static(b"first version"))
        .await;
    protocol
        .put_version(
            "src/final",
            "v2",
            Bytes::from_static(b"second, the current one"),
        )
        .await;
    protocol
}

fn request(protocol: &Arc<MemoryS3>, native: bool) -> TestResult<TransferRequest> {
    let source = source_storage(protocol, "src")?;
    let destination = connect(
        protocol.clone(),
        endpoint_of(protocol),
        native.then(native_context),
    )?;
    Ok(TransferRequest::new(
        source,
        StoragePath::new("final")?,
        destination,
        CancellationToken::new(),
    ))
}

/// A named version is copied as it is — streamed or by native copy — although it is not current.
#[tokio::test]
async fn a_named_version_is_copied_streaming_and_native() -> TestResult {
    for (native, expected_route) in [
        (false, TransferRoute::Streaming),
        (true, TransferRoute::Native),
    ] {
        let protocol = versioned().await;
        let outcome = transfer(
            request(&protocol, native)?.with_source_version(SourceVersion::Id("v1".into())),
        )
        .await?;
        assert_eq!(outcome.route, expected_route);
        assert_eq!(
            protocol.objects.lock().await.get("final"),
            Some(&Bytes::from_static(b"first version")),
            "{expected_route:?}"
        );
    }
    Ok(())
}

/// A deleted version fails the entry when described — before anything is written.
#[tokio::test]
async fn a_deleted_version_fails_the_entry_at_describe() -> TestResult {
    let protocol = versioned().await;
    protocol.put_delete_marker("src/final", "marker").await;
    let Err(failure) =
        transfer(request(&protocol, true)?.with_source_version(SourceVersion::Id("marker".into())))
            .await
    else {
        return Err("a delete marker was copied".into());
    };
    assert_eq!(failure.phase(), TransferPhase::Describe);
    let role = Error::source(&failure).and_then(|cause| cause.downcast_ref::<StorageRoleFailure>());
    assert!(
        matches!(role, Some(StorageRoleFailure::Entry(entry)) if entry.class() == FailureClass::NotFound),
        "{failure:?}"
    );
    assert!(!protocol.objects.lock().await.contains_key("final"));
    Ok(())
}

#[tokio::test]
async fn a_malformed_version_id_fails_at_preflight() -> TestResult {
    let protocol = versioned().await;
    for bad in [String::new(), "x".repeat(1025), "a\0b".to_string()] {
        let Err(failure) =
            transfer(request(&protocol, true)?.with_source_version(SourceVersion::Id(bad))).await
        else {
            return Err("a malformed version id was accepted".into());
        };
        assert_eq!(failure.phase(), TransferPhase::Preflight);
        assert_eq!(failure.side(), TransferSide::Source);
        let role =
            Error::source(&failure).and_then(|cause| cause.downcast_ref::<StorageRoleFailure>());
        assert!(
            matches!(role, Some(StorageRoleFailure::Entry(entry)) if entry.class() == FailureClass::InvalidInput),
            "{failure:?}"
        );
    }
    Ok(())
}

/// The selector names the transfer, and an identity override wins in either order.
#[tokio::test]
async fn the_version_selects_the_identity_unless_overridden() -> TestResult {
    let protocol = versioned().await;
    let current = request(&protocol, true)?;
    assert_eq!(*current.source_version(), SourceVersion::Current);
    let v1 = request(&protocol, true)?.with_source_version(SourceVersion::Id("v1".into()));
    assert_ne!(v1.identity(), current.identity());
    assert_eq!(
        request(&protocol, true)?
            .with_source_version(SourceVersion::Current)
            .identity(),
        current.identity()
    );
    let label = TransferIdentity::from_label("job")?;
    for request in [
        request(&protocol, true)?
            .with_identity_override(label)
            .with_source_version(SourceVersion::Id("v1".into())),
        request(&protocol, true)?
            .with_source_version(SourceVersion::Id("v1".into()))
            .with_identity_override(label),
    ] {
        assert_eq!(request.identity(), label);
        assert_eq!(*request.source_version(), SourceVersion::Id("v1".into()));
    }
    Ok(())
}
