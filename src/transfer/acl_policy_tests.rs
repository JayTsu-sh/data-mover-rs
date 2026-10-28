//! One ACL selector preserves native state or reports automatic projection losses.
use super::*;
use crate::transfer::CopiedMetadataRequest;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[cfg(unix)]
#[tokio::test]
async fn selected_native_acl_is_applied_and_passes_public_integrity() -> Result {
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
        .with_copied_metadata(CopiedMetadataRequest::default().with_acl()),
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

#[cfg(unix)]
#[tokio::test]
async fn selected_acl_on_an_unavailable_source_installs_a_reported_fallback() -> Result {
    use crate::model::acl::AclCopyIssueKind;
    use crate::storage::backends::s3::tests::{MemoryS3, source_storage};
    use std::os::unix::fs::PermissionsExt as _;

    let protocol = Arc::new(MemoryS3::default());
    protocol.objects.lock().await.insert(
        "src/final.bin".into(),
        bytes::Bytes::from_static(b"ACL bytes"),
    );
    let dst = TestRoot::new("acl-unavailable-source")?;
    let outcome = transfer(
        transfer_request(
            source_storage(&protocol, "src")?,
            local_destination(dst.path())?,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_copied_metadata(CopiedMetadataRequest::default().with_acl()),
    )
    .await?;
    assert_eq!(outcome.transferred_bytes, 9);
    assert_eq!(outcome.blake3, Some(*blake3::hash(b"ACL bytes").as_bytes()));
    let metadata = outcome.metadata.ok_or("ACL report missing")?;
    let report = metadata
        .acl_copy_report()
        .ok_or("fallback report missing")?;
    assert!(report.conservative_fallback);
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == AclCopyIssueKind::SourceUnavailable)
    );
    assert_eq!(
        std::fs::metadata(dst.path().join("final.bin"))?
            .permissions()
            .mode()
            & 0o777,
        0
    );
    Ok(())
}

#[test]
fn acl_selection_is_one_independent_optional_feature() {
    let defaults = CopiedMetadataRequest::default();
    assert!(!defaults.acl());
    assert!(defaults.is_empty());
    let selected = defaults.with_acl();
    assert!(selected.acl());
    assert!(!selected.xattrs());
    assert!(!selected.is_empty());
    assert_eq!(selected.with_acl(), selected);
    assert!(selected.with_xattrs().acl());
    assert!(selected.with_xattrs().xattrs());
}

#[tokio::test]
async fn selected_acl_to_an_unsupported_destination_copies_and_reports_skip() -> Result {
    use crate::metadata::{MetadataFamily, RefusalCause};
    use crate::storage::backends::s3::{
        connect,
        tests::{MemoryS3, endpoint_of},
    };

    let src = TestRoot::new("partial-acl-unsupported-target")?;
    std::fs::write(src.path().join("final.bin"), b"partial ACL bytes")?;
    let protocol = Arc::new(MemoryS3::default());
    let destination = connect(protocol.clone(), endpoint_of(&protocol), None)?;
    let outcome = transfer(
        transfer_request(
            local_source(src.path())?,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_copied_metadata(CopiedMetadataRequest::default().with_acl()),
    )
    .await?;
    let report = outcome.metadata.ok_or("partial mapping report missing")?;
    assert!(report.skipped().iter().any(|skipped| {
        skipped.family == MetadataFamily::Acl
            && skipped.reason == RefusalCause::DestinationCannotStore
    }));
    assert_eq!(
        protocol
            .objects
            .lock()
            .await
            .get("final.bin")
            .map(bytes::Bytes::as_ref),
        Some(b"partial ACL bytes".as_slice()),
    );
    Ok(())
}
