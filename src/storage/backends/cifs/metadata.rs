use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use binrw::{BinRead as _, BinWrite as _};

use super::source::{classify, entry_failure};
use crate::model::{
    AclEncoding, AclMetadata, FailureClass, MetadataObservation, MetadataObservations,
    MetadataProvenance, ObservationMode, ObservationPlan, Operation, StoragePath, StorageTimestamp,
    TimePrecision, TimestampMetadata,
};
use crate::storage::{Metadata, MetadataMutation, StorageRoleFailure};

#[derive(Clone, Copy)]
pub(super) struct CifsInlineMetadata {
    pub(super) accessed: std::time::SystemTime,
    pub(super) modified: std::time::SystemTime,
    pub(super) created: std::time::SystemTime,
}

#[async_trait]
pub(super) trait CifsMetadataProtocol: Send + Sync {
    async fn metadata(&self, path: &StoragePath) -> smb_domain::Result<CifsInlineMetadata>;
    async fn get_acl(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<smb_domain::SecurityDescriptor>;
    async fn set_acl(
        &self,
        path: &StoragePath,
        descriptor: smb_domain::SecurityDescriptor,
    ) -> smb_domain::Result<()>;
}

pub(super) struct CifsMetadata {
    protocol: Arc<dyn CifsMetadataProtocol>,
}

impl CifsMetadata {
    pub(super) fn new<P>(protocol: Arc<P>) -> Self
    where
        P: CifsMetadataProtocol + 'static,
    {
        Self { protocol }
    }
}

#[async_trait]
impl Metadata for CifsMetadata {
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let inline = self
            .protocol
            .metadata(path)
            .await
            .map_err(|error| classify(path, Operation::Metadata, &error))?;
        let acl = observe_acl(self.protocol.as_ref(), path, plan.acl()).await?;
        let timestamps = match plan.timestamps() {
            ObservationMode::Omit => MetadataObservation::NotRequested,
            _ => MetadataObservation::Value {
                value: TimestampMetadata {
                    accessed: timestamp(inline.accessed),
                    modified: timestamp(inline.modified),
                    created: timestamp(inline.created),
                },
                provenance: MetadataProvenance::Inline,
            },
        };
        MetadataObservations::new(
            acl,
            not_applicable(plan.xattrs()),
            not_applicable(plan.tags()),
            not_applicable(plan.ownership_mode()),
            timestamps,
        )
        .map_err(|_| entry_failure(path, Operation::Metadata, FailureClass::Protocol))
    }

    async fn apply(
        &self,
        path: &StoragePath,
        mutation: MetadataMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if cancel.is_cancelled() {
            return Err(entry_failure(
                path,
                Operation::Metadata,
                FailureClass::Cancelled,
            ));
        }
        let MetadataMutation::Acl(value) = mutation else {
            return Err(entry_failure(
                path,
                Operation::Metadata,
                FailureClass::Unsupported,
            ));
        };
        if value.encoding() != AclEncoding::WindowsSecurityDescriptor {
            return Err(entry_failure(
                path,
                Operation::Metadata,
                FailureClass::InvalidInput,
            ));
        }
        let bytes = value
            .access()
            .ok_or_else(|| entry_failure(path, Operation::Metadata, FailureClass::InvalidInput))?;
        let descriptor = smb_domain::SecurityDescriptor::read_le(&mut Cursor::new(bytes))
            .map_err(|_| entry_failure(path, Operation::Metadata, FailureClass::InvalidInput))?;
        self.protocol
            .set_acl(path, descriptor)
            .await
            .map_err(|error| classify(path, Operation::Metadata, &error))
    }
}

async fn observe_acl(
    protocol: &dyn CifsMetadataProtocol,
    path: &StoragePath,
    mode: ObservationMode,
) -> Result<MetadataObservation<AclMetadata>, StorageRoleFailure> {
    if matches!(mode, ObservationMode::Omit | ObservationMode::InlineOnly) {
        return Ok(MetadataObservation::NotRequested);
    }
    match protocol.get_acl(path).await {
        Ok(descriptor) => {
            let mut output = Cursor::new(Vec::new());
            descriptor
                .write_le(&mut output)
                .map_err(|_| entry_failure(path, Operation::Metadata, FailureClass::Protocol))?;
            let value =
                AclMetadata::new(AclEncoding::WindowsSecurityDescriptor, output.into_inner())
                    .map_err(|_| {
                        entry_failure(path, Operation::Metadata, FailureClass::Protocol)
                    })?;
            Ok(MetadataObservation::Value {
                value,
                provenance: MetadataProvenance::AdditionalCall,
            })
        }
        Err(error) => match classify(path, Operation::Metadata, &error) {
            StorageRoleFailure::Entry(error) if mode == ObservationMode::BestEffort => {
                Ok(MetadataObservation::Failed {
                    class: error.class(),
                    transience: error.transience(),
                })
            }
            failure => Err(failure),
        },
    }
}

fn not_applicable<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::NotApplicable
    }
}

fn timestamp(value: std::time::SystemTime) -> Option<StorageTimestamp> {
    let nanos = match value.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i128::try_from(duration.as_nanos()).ok()?,
        Err(error) => -i128::try_from(error.duration().as_nanos()).ok()?,
    };
    StorageTimestamp::new(nanos, TimePrecision::Nanoseconds).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct ProbeProtocol {
        acl_calls: AtomicUsize,
    }

    #[async_trait]
    impl CifsMetadataProtocol for ProbeProtocol {
        async fn metadata(&self, _path: &StoragePath) -> smb_domain::Result<CifsInlineMetadata> {
            Ok(CifsInlineMetadata {
                accessed: std::time::UNIX_EPOCH,
                modified: std::time::UNIX_EPOCH,
                created: std::time::UNIX_EPOCH,
            })
        }

        async fn get_acl(
            &self,
            _path: &StoragePath,
        ) -> smb_domain::Result<smb_domain::SecurityDescriptor> {
            self.acl_calls.fetch_add(1, Ordering::SeqCst);
            Err(smb_domain::Error::InvalidMessage(
                "scripted ACL failure".into(),
            ))
        }

        async fn set_acl(
            &self,
            _path: &StoragePath,
            _descriptor: smb_domain::SecurityDescriptor,
        ) -> smb_domain::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn acl_requires_explicit_additional_call_mode() -> Result<(), Box<dyn std::error::Error>>
    {
        let protocol = Arc::new(ProbeProtocol {
            acl_calls: AtomicUsize::new(0),
        });
        let metadata = CifsMetadata::new(Arc::clone(&protocol));
        let path = StoragePath::new("file")?;

        for plan in [
            ObservationPlan::default(),
            ObservationPlan::default().with_acl(ObservationMode::InlineOnly),
        ] {
            let observed = metadata.observe(&path, plan).await?;
            assert_eq!(observed.acl(), &MetadataObservation::NotRequested);
        }
        assert_eq!(protocol.acl_calls.load(Ordering::SeqCst), 0);

        let observed = metadata
            .observe(
                &path,
                ObservationPlan::default().with_acl(ObservationMode::BestEffort),
            )
            .await?;
        assert!(matches!(
            observed.acl(),
            MetadataObservation::Failed {
                class: FailureClass::Protocol,
                ..
            }
        ));
        assert_eq!(protocol.acl_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
