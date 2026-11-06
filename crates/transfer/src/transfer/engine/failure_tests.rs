use std::error::Error;
use std::path::Path;

use bytes::Bytes;
use std::ptr::from_ref;
use tokio_util::sync::CancellationToken;

use super::TransferFailure;
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, IdentityStrength, ObservedEntry, Operation,
    SourceIdentity, StoragePath, Transience,
};
use crate::storage::StorageRoleFailure;
use crate::storage::artifacts::{ArtifactKind, artifact_name};
use crate::storage::backends::local::test_destination_storage;
use crate::transfer::TransferSide;
use crate::transfer::{
    ExpertDestinationRequest, ExpertDestinationSession, TransferPhase, TransferPolicy,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

async fn failed_input(root: &Path, policy: TransferPolicy) -> TestResult<TransferFailure> {
    std::fs::write(root.join("file"), b"old")?;
    let target = test_destination_storage(root, "cleanup-target")?;
    let observed = ObservedEntry::new(
        StoragePath::new("source")?,
        EntryKind::File,
        Some(12),
        None,
        SourceIdentity::new(
            target.identity().clone(),
            IdentityStrength::PathScoped,
            b"source",
        )?,
    )?;
    let prepared = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observed,
            3,
            target,
            StoragePath::new("file")?,
            CancellationToken::new(),
        )
        .with_transfer_policy(policy),
    )
    .await?;
    let cause = StorageRoleFailure::Entry(EntryOperationFailure::new(
        StoragePath::new("file")?,
        Operation::Write,
        FailureClass::PermissionDenied,
        Transience::Permanent,
        "original stream failure",
    )?);
    let input = futures::stream::iter([Ok(Bytes::from_static(b"abc")), Err(cause)]);
    match prepared.write(Box::pin(input)).await {
        Err(failure) => Ok(failure),
        Ok(_) => Err("a failed input was accepted".into()),
    }
}

#[tokio::test]
async fn cleanup_preserves_original_typed_cause_and_recovery_policy() -> TestResult {
    for policy in [TransferPolicy::AtomicReplace, TransferPolicy::Checkpointed] {
        let root = tempfile::tempdir()?;
        let failure = failed_input(root.path(), policy).await?;
        assert!(failure.has_unpublished_stage());
        let original = from_ref(Error::source(&failure).ok_or("missing original cause")?);
        let failure = failure.cleanup_for_policy(policy).await;
        let retained = from_ref(Error::source(&failure).ok_or("cause lost during cleanup")?);
        assert!(std::ptr::eq(original, retained));
        assert_eq!(failure.phase(), TransferPhase::Transfer);
        assert!(failure.cleanup_failure().is_none());
        assert_eq!(std::fs::read(root.path().join("file"))?, b"old");
        assert_eq!(
            failure.has_unpublished_stage(),
            policy == TransferPolicy::Checkpointed
        );
        if failure.has_unpublished_stage() {
            failure.discard_stage().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn cleanup_failure_retains_the_original_cause() -> TestResult {
    let root = tempfile::tempdir()?;
    let failure = failed_input(root.path(), TransferPolicy::AtomicReplace).await?;
    let stage = root.path().join(artifact_name("file", ArtifactKind::Stage));
    assert!(stage.is_file());
    std::fs::remove_file(&stage)?;
    std::fs::create_dir(&stage)?;
    let original = from_ref(Error::source(&failure).ok_or("missing original cause")?);
    let failure = failure
        .cleanup_for_policy(TransferPolicy::AtomicReplace)
        .await;
    let retained = from_ref(Error::source(&failure).ok_or("original cause lost")?);
    assert!(std::ptr::eq(original, retained));
    assert!(matches!(
        failure.cleanup_failure(),
        Some(StorageRoleFailure::Entry(_))
    ));
    assert!(stage.is_dir());
    assert_eq!(std::fs::read(root.path().join("file"))?, b"old");
    Ok(())
}

#[test]
fn non_metadata_display_keeps_the_underlying_failure_class() -> TestResult {
    let cause = StorageRoleFailure::Entry(EntryOperationFailure::new(
        StoragePath::new("file")?,
        Operation::Publish,
        FailureClass::PermissionDenied,
        Transience::Permanent,
        "scoped publish refusal",
    )?);
    let failure = TransferFailure::role(TransferPhase::Publish, TransferSide::Destination, cause);
    let message = failure.to_string();
    assert!(message.contains("PermissionDenied"), "{message}");
    assert!(message.contains("Permanent"), "{message}");
    assert!(failure.source().is_some());
    Ok(())
}

#[test]
fn a_source_input_error_cannot_replace_an_independent_destination_failure() -> TestResult {
    let destination = StorageRoleFailure::Entry(EntryOperationFailure::new(
        StoragePath::new("result.tar")?,
        Operation::Write,
        FailureClass::PermissionDenied,
        Transience::Permanent,
        "destination refused the write",
    )?);
    let input = StorageRoleFailure::Entry(EntryOperationFailure::new(
        StoragePath::new("file")?,
        Operation::Read,
        FailureClass::Conflict,
        Transience::Permanent,
        "source changed",
    )?);
    let mut failure = TransferFailure::role(
        TransferPhase::Publish,
        TransferSide::Destination,
        destination,
    );
    failure.final_destination_changed = true;
    let failure = failure.attribute_source_input(&input);
    assert_eq!(failure.side(), TransferSide::Destination);
    assert_eq!(failure.phase(), TransferPhase::Publish);
    assert!(failure.final_destination_changed());
    assert!(matches!(
        failure.source().and_then(|cause| cause.downcast_ref::<StorageRoleFailure>()),
        Some(StorageRoleFailure::Entry(cause)) if cause.operation() == Operation::Write
            && cause.class() == FailureClass::PermissionDenied
    ));
    Ok(())
}
