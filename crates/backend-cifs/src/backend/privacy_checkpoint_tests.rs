use super::*;
use crate::model::{AclEncoding, AclMetadata, MetadataObservations, ObservationPlan};
use crate::storage::{Metadata, MetadataMutation};

#[derive(Default)]
struct RecordingMetadata {
    paths: Mutex<Vec<String>>,
}

#[async_trait]
impl Metadata for RecordingMetadata {
    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        unreachable!("metadata mutation fixture")
    }
    async fn apply(
        &self,
        path: &StoragePath,
        _: MetadataMutation,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
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
