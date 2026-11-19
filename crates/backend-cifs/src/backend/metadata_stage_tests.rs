use super::super::metadata::{CifsMetadataWriter, MetadataWriteFailure};
use super::*;
use crate::backend::source::entry_failure;
use crate::model::{AclEncoding, AclMetadata, TimestampMetadata};
use crate::storage::MetadataMutation;

struct RecordingMetadata {
    protocol: Arc<MemoryProtocol>,
    applied: AtomicUsize,
    unconfirmed: bool,
}

#[async_trait]
impl CifsMetadataWriter for RecordingMetadata {
    async fn apply_checked(
        &self,
        path: &StoragePath,
        _: MetadataMutation,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<(), MetadataWriteFailure> {
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
        let index = self.applied.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            assert_eq!(self.protocol.flushes.load(Ordering::SeqCst), 0);
        }
        if self.unconfirmed && index == 1 {
            return Err(MetadataWriteFailure::Unconfirmed(entry_failure(
                path,
                Operation::Metadata,
                FailureClass::Protocol,
            )));
        }
        Ok(())
    }
}

#[tokio::test]
async fn metadata_flush_obeys_publication_policy_and_closes_on_failure()
-> Result<(), Box<dyn std::error::Error>> {
    for (durable, fail_flush, unconfirmed) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (true, false, true),
    ] {
        let protocol = Arc::new(MemoryProtocol::default());
        let metadata = Arc::new(RecordingMetadata {
            protocol: protocol.clone(),
            applied: AtomicUsize::new(0),
            unconfirmed,
        });
        let identity = identity()?;
        let destination = CifsStagedDestination::new(protocol.clone(), identity.clone())
            .with_metadata(metadata.clone());
        let mut stage = prepare_stage(&destination, prepare_request(&identity)?).await?;
        stage.set_durable_publication(durable);
        protocol.fail_flush.store(fail_flush, Ordering::SeqCst);
        let closes = protocol.closes.load(Ordering::SeqCst);
        let timestamp = MetadataMutation::Timestamps(TimestampMetadata {
            accessed: None,
            modified: None,
            created: None,
        });
        let mutations = if unconfirmed {
            vec![
                MetadataMutation::Acl(AclMetadata::new(
                    AclEncoding::WindowsSecurityDescriptor,
                    vec![1],
                )?),
                timestamp.clone(),
                timestamp,
            ]
        } else {
            vec![timestamp]
        };
        let result = destination
            .apply_metadata_batch(
                &stage,
                mutations,
                tokio_util::sync::CancellationToken::new(),
            )
            .await;
        if unconfirmed {
            let failure = result.err().ok_or("unconfirmed mutation was ignored")?;
            assert_eq!(failure.failed_index, 1);
            assert_eq!(failure.completed, 1);
            assert!(failure.write_unconfirmed);
            assert!(
                matches!(failure.error, Some(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Protocol)
            );
        } else if fail_flush {
            let failure = result.err().ok_or("FLUSH failure was ignored")?;
            assert_eq!(failure.failed_index, 1, "FLUSH failed the whole batch");
            assert_eq!(failure.completed, 1, "the mutation ran before FLUSH");
            assert!(!failure.write_unconfirmed);
            assert!(matches!(failure.error, Some(StorageRoleFailure::Entry(_))));
        } else {
            assert_eq!(result, Ok(()));
        }
        assert_eq!(
            metadata.applied.load(Ordering::SeqCst),
            1 + usize::from(unconfirmed)
        );
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
