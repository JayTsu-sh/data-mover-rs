//! Required ACL selectors are enforced before writes and bound into recovery policy.
use super::*;
use crate::transfer::CopiedMetadataRequest;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[cfg(unix)]
#[tokio::test]
async fn required_native_acl_is_applied_and_passes_public_integrity() -> Result {
    let src = TestRoot::new("strict-acl-source")?;
    let dst = TestRoot::new("strict-acl-target")?;
    std::fs::write(src.path().join("final.bin"), b"strict ACL bytes")?;
    let source = local_source(src.path())?;
    let destination = local_destination(dst.path())?;
    let outcome = transfer(
        transfer_request(
            source.clone(),
            destination.clone(),
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_copied_metadata(CopiedMetadataRequest::default().with_required_acl()),
    )
    .await?;
    let report = outcome.metadata.ok_or("required ACL report missing")?;
    assert!(report.skipped().is_empty());
    assert!(
        report
            .outcomes()
            .iter()
            .any(|v| v.family == crate::metadata::MetadataFamily::Acl
                && v.outcome == ApplicationOutcome::Applied)
    );
    let report = crate::integrity::compare(crate::integrity::IntegrityRequest {
        source,
        destination: local_source(dst.path())?,
        source_path: StoragePath::new("final.bin")?,
        destination_path: StoragePath::new("final.bin")?,
        plan: crate::integrity::IntegrityPlan::default().with_required_acl(),
        options: crate::integrity::IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    })
    .await?;
    assert!(report.matches(), "{:?}", report.differences());
    assert_eq!(
        report.acl_scope(),
        Some(crate::integrity::AclComparisonScope::NativeConfiguration {
            encoding: crate::model::AclEncoding::Posix
        })
    );
    Ok(())
}

#[tokio::test]
async fn required_acl_on_an_unsupported_destination_refuses_before_any_object_write() -> Result {
    use crate::storage::backends::s3::{
        connect,
        tests::{MemoryS3, endpoint_of},
    };
    let src = TestRoot::new("strict-acl-refused-source")?;
    std::fs::write(src.path().join("final.bin"), b"strict ACL bytes")?;
    let protocol = Arc::new(MemoryS3::default());
    let destination = connect(protocol.clone(), endpoint_of(&protocol), None)?;
    for selectors in [
        CopiedMetadataRequest::default().with_required_acl(),
        CopiedMetadataRequest::default().with_acl_mapping(),
    ] {
        let request = transfer_request(
            local_source(src.path())?,
            destination.clone(),
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_copied_metadata(selectors);
        let error = transfer(request)
            .await
            .err()
            .ok_or("required ACL must refuse")?;
        assert_eq!(error.phase(), TransferPhase::Metadata);
        assert_eq!(error.side(), TransferSide::Destination);
        assert_eq!(
            error
                .metadata_refusal()
                .map(crate::metadata::MetadataPlanError::cause),
            Some(crate::metadata::RefusalCause::DestinationCannotStore)
        );
        assert!(!error.final_destination_changed());
    }
    assert!(protocol.objects.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn required_acl_on_an_unsupported_source_refuses_before_preparing_the_destination() -> Result
{
    use crate::storage::backends::s3::tests::{MemoryS3, source_storage};
    let protocol = Arc::new(MemoryS3::default());
    protocol.objects.lock().await.insert(
        "src/final.bin".into(),
        bytes::Bytes::from_static(b"strict ACL bytes"),
    );
    let dst = TestRoot::new("strict-acl-missing-source")?;
    let request = transfer_request(
        source_storage(&protocol, "src")?,
        local_destination(dst.path())?,
        tokio_util::sync::CancellationToken::new(),
    )?
    .with_copied_metadata(CopiedMetadataRequest::default().with_required_acl());
    let error = transfer(request)
        .await
        .err()
        .ok_or("required source ACL must refuse")?;
    assert_eq!(error.phase(), TransferPhase::Metadata);
    assert_eq!(error.side(), TransferSide::Source);
    assert_eq!(
        error
            .metadata_refusal()
            .map(crate::metadata::MetadataPlanError::cause),
        Some(crate::metadata::RefusalCause::SourceCannotObserve)
    );
    assert!(!error.final_destination_changed());
    assert_eq!(std::fs::read_dir(dst.path())?.count(), 0);
    Ok(())
}
