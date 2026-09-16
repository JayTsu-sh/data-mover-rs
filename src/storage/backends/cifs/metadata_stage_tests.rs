use super::*;
use crate::model::{MetadataObservations, ObservationPlan, TimestampMetadata};
use crate::storage::{Metadata, MetadataMutation};

struct RecordingMetadata {
    protocol: Arc<MemoryProtocol>,
    applied: AtomicUsize,
}

#[async_trait]
impl Metadata for RecordingMetadata {
    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        unreachable!("destination-only metadata probe")
    }
    async fn apply(
        &self,
        path: &StoragePath,
        _: MetadataMutation,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        assert_eq!(
            std::path::Path::new(path.as_str()).extension(),
            Some(std::ffi::OsStr::new("stage"))
        );
        assert!(
            self.protocol
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(path.as_str())
        );
        assert_eq!(self.protocol.flushes.load(Ordering::SeqCst), 0);
        self.applied.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn metadata_flush_obeys_publication_policy_and_closes_on_failure()
-> Result<(), Box<dyn std::error::Error>> {
    for (durable, fail_flush) in [(false, false), (true, false), (true, true)] {
        let protocol = Arc::new(MemoryProtocol::default());
        let metadata = Arc::new(RecordingMetadata {
            protocol: protocol.clone(),
            applied: AtomicUsize::new(0),
        });
        let identity = identity()?;
        let destination = CifsStagedDestination::new(protocol.clone(), identity.clone())
            .with_metadata(metadata.clone());
        let mut stage = destination.prepare(prepare_request(&identity)?).await?;
        stage.durable_publication = durable;
        protocol.fail_flush.store(fail_flush, Ordering::SeqCst);
        let closes = protocol.closes.load(Ordering::SeqCst);
        let result = destination
            .apply_metadata(
                &stage,
                MetadataMutation::Timestamps(TimestampMetadata {
                    accessed: None,
                    modified: None,
                    created: None,
                }),
                tokio_util::sync::CancellationToken::new(),
            )
            .await;
        assert_eq!(result.is_err(), fail_flush);
        assert_eq!(metadata.applied.load(Ordering::SeqCst), 1);
        assert_eq!(
            protocol.flushes.load(Ordering::SeqCst),
            usize::from(durable)
        );
        assert_eq!(
            protocol.closes.load(Ordering::SeqCst) - closes,
            usize::from(durable)
        );
        assert!(
            !protocol
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key("final.bin")
        );
    }
    Ok(())
}
