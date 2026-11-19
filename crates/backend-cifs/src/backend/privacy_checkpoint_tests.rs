use super::*;
use crate::model::{AclEncoding, AclMetadata};
use crate::storage::MetadataMutation;
use crate::storage::backends::cifs::metadata::{CifsMetadataWriter, MetadataWriteFailure};

#[derive(Default)]
struct RecordingMetadata {
    paths: Mutex<Vec<String>>,
}

#[async_trait]
impl CifsMetadataWriter for RecordingMetadata {
    async fn apply_checked(
        &self,
        path: &StoragePath,
        _: MetadataMutation,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<(), MetadataWriteFailure> {
        self.paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(path.as_str().to_owned());
        Ok(())
    }
}

#[tokio::test]
async fn refusing_dacl_protection_never_writes_source_fragments() -> TestResult {
    let (protocol, backend) = share()?;
    let destination = adapter(&protocol, &backend);
    let stage = destination
        .prepare_at_destination(hash_request(&backend, 6)?.with_recoverable(false))
        .await?;
    arm(&stage, b"abcdef")?;
    protocol
        .fail_checkpoint_protection
        .store(true, Ordering::SeqCst);
    assert!(destination.persist_hash_checkpoint(&stage).await.is_err());
    assert_eq!(
        artifacts_left(&protocol),
        [name(ArtifactKind::Stage, false)]
    );
    assert_eq!(
        file(&protocol, &name(ArtifactKind::Stage, false)),
        Some(Vec::new())
    );
    assert_pointer_handles_closed(&protocol);
    Ok(())
}

#[tokio::test]
async fn acl_tightening_reaches_pointer_first_and_legacy_sends_no_security_calls() -> TestResult {
    for armed in [false, true] {
        let (protocol, backend) = share()?;
        let metadata = Arc::new(RecordingMetadata::default());
        let destination = adapter(&protocol, &backend).with_metadata(metadata.clone());
        let mut stage = destination
            .prepare_at_destination(hash_request(&backend, 6)?)
            .await?;
        stage.set_durable_publication(false);
        if armed {
            arm(&stage, b"abcdef")?;
            destination.persist_hash_checkpoint(&stage).await?;
        }
        // Metadata itself is mocked; this valid envelope tests which paths receive it.
        let acl = AclMetadata::new(AclEncoding::WindowsSecurityDescriptor, vec![1])?;
        destination
            .apply_metadata(
                &stage,
                MetadataMutation::Acl(acl),
                tokio_util::sync::CancellationToken::new(),
            )
            .await?;
        let mut expected = Vec::new();
        if armed {
            expected.push(name(ArtifactKind::Pointer, false));
        }
        expected.push(name(ArtifactKind::Stage, false));
        assert_eq!(
            *metadata
                .paths
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            expected
        );
        let calls = protocol
            .checkpoint_protections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(calls.len(), if armed { 2 } else { 0 });
        if armed {
            assert_eq!(
                calls[1],
                (
                    name(ArtifactKind::Pointer, false),
                    name(ArtifactKind::Stage, false)
                )
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn checkpoint_acl_refresh_failure_stops_the_batch_after_payload_acl() -> TestResult {
    let (protocol, backend) = share()?;
    let metadata = Arc::new(RecordingMetadata::default());
    let destination = adapter(&protocol, &backend).with_metadata(metadata.clone());
    let stage = destination
        .prepare_at_destination(hash_request(&backend, 6)?)
        .await?;
    arm(&stage, b"abcdef")?;
    destination.persist_hash_checkpoint(&stage).await?;
    protocol
        .fail_checkpoint_protection
        .store(true, Ordering::SeqCst);
    let acl = AclMetadata::new(AclEncoding::WindowsSecurityDescriptor, vec![1])?;
    let failure = destination
        .apply_metadata_batch(
            &stage,
            vec![
                MetadataMutation::Acl(acl.clone()),
                MetadataMutation::Acl(acl),
            ],
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("checkpoint ACL refresh failure was ignored")?;
    assert_eq!(failure.failed_index, 2);
    assert_eq!(failure.completed, 1);
    assert!(
        matches!(failure.error, Some(StorageRoleFailure::Entry(error))
        if error.class() == FailureClass::Unsupported)
    );
    assert_eq!(
        *metadata
            .paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        [
            name(ArtifactKind::Pointer, false),
            name(ArtifactKind::Stage, false)
        ]
    );
    assert_pointer_handles_closed(&protocol);
    Ok(())
}
