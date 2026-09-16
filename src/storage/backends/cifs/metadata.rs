use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use binrw::{BinRead as _, BinWrite as _};

use super::source::{CifsSourceFacts, classify, descriptor_from_facts, entry_failure};
use crate::model::{
    AclEncoding, AclMetadata, BackendIdentity, FailureClass, MetadataObservation,
    MetadataObservations, MetadataProvenance, ObservationMode, ObservationPlan, Operation,
    StoragePath, StorageTimestamp, TimePrecision, TimestampMetadata,
};
use crate::storage::{Metadata, MetadataMutation, StorageRoleFailure};

#[derive(Clone)]
pub(super) struct CifsInlineMetadata {
    pub(super) facts: CifsSourceFacts,
    pub(super) accessed: std::time::SystemTime,
    pub(super) modified: std::time::SystemTime,
    pub(super) created: std::time::SystemTime,
}

#[async_trait]
pub(super) trait CifsMetadataProtocol: Send + Sync {
    async fn metadata(&self, path: &StoragePath) -> smb_domain::Result<CifsInlineMetadata>;
    async fn set_timestamps(
        &self,
        path: &StoragePath,
        value: TimestampMetadata,
    ) -> smb_domain::Result<()>;
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
    identity: BackendIdentity,
}

impl CifsMetadata {
    pub(super) fn new<P>(protocol: Arc<P>, identity: BackendIdentity) -> Self
    where
        P: CifsMetadataProtocol + 'static,
    {
        Self { protocol, identity }
    }
}

impl CifsMetadata {
    async fn observe_inline(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
        inline: CifsInlineMetadata,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
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
}

#[async_trait]
impl Metadata for CifsMetadata {
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        Some(
            ObservationPlan::default()
                .with_ownership_mode(ObservationMode::InlineOnly)
                .with_timestamps(ObservationMode::Required),
        )
    }

    async fn observe_bound(
        &self,
        path: &StoragePath,
        expected: &crate::model::SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let inline = self
            .protocol
            .metadata(path)
            .await
            .map_err(|error| classify(path, Operation::Metadata, &error))?;
        let observed =
            descriptor_from_facts(&self.identity, path, &inline.facts, Operation::Metadata)?;
        if observed.source_identity != *expected {
            return Err(entry_failure(
                path,
                Operation::Metadata,
                FailureClass::Conflict,
            ));
        }
        self.observe_inline(path, plan, inline).await
    }

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
        self.observe_inline(path, plan, inline).await
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
        if let MetadataMutation::Timestamps(value) = mutation {
            return self
                .protocol
                .set_timestamps(path, value)
                .await
                .map_err(|error| classify(path, Operation::Metadata, &error));
        }
        let MetadataMutation::Acl(value) = mutation else {
            return Err(entry_failure(
                path,
                Operation::Metadata,
                FailureClass::Unsupported,
            ));
        };
        let descriptor = decode_acl(path, &value)?;
        self.protocol
            .set_acl(path, descriptor)
            .await
            .map_err(|error| classify(path, Operation::Metadata, &error))
    }
}

fn decode_acl(
    path: &StoragePath,
    value: &AclMetadata,
) -> Result<smb_domain::SecurityDescriptor, StorageRoleFailure> {
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
    smb_domain::SecurityDescriptor::read_le(&mut Cursor::new(bytes))
        .map_err(|_| entry_failure(path, Operation::Metadata, FailureClass::InvalidInput))
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
    StorageTimestamp::new(nanos, TimePrecision::HundredNanoseconds).ok()
}

pub(super) fn timestamp_update(
    value: TimestampMetadata,
) -> smb_domain::Result<smb_domain::MetadataUpdate> {
    Ok(smb_domain::MetadataUpdate {
        accessed: system_time(value.accessed)?,
        written: system_time(value.modified)?,
        created: system_time(value.created)?,
    })
}

fn system_time(
    value: Option<StorageTimestamp>,
) -> smb_domain::Result<Option<std::time::SystemTime>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let nanos = value.unix_nanos();
    let magnitude = nanos.unsigned_abs();
    let seconds = u64::try_from(magnitude / 1_000_000_000)
        .map_err(|_| smb_domain::Error::InvalidArgument("timestamp is out of range".into()))?;
    let fraction = u32::try_from(magnitude % 1_000_000_000)
        .map_err(|_| smb_domain::Error::InvalidArgument("invalid timestamp fraction".into()))?;
    let duration = std::time::Duration::new(seconds, fraction);
    let time = if nanos < 0 {
        std::time::UNIX_EPOCH.checked_sub(duration)
    } else {
        std::time::UNIX_EPOCH.checked_add(duration)
    };
    time.map(Some)
        .ok_or_else(|| smb_domain::Error::InvalidArgument("timestamp is out of range".into()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct ProbeProtocol {
        acl_calls: AtomicUsize,
        sets: std::sync::Mutex<Vec<TimestampMetadata>>,
    }

    #[async_trait]
    impl CifsMetadataProtocol for ProbeProtocol {
        async fn metadata(&self, _path: &StoragePath) -> smb_domain::Result<CifsInlineMetadata> {
            Ok(CifsInlineMetadata {
                facts: CifsSourceFacts {
                    kind: crate::model::EntryKind::File,
                    size: 4,
                    identity: bytes::Bytes::from_static(b"source-version"),
                    maximum_read_chunk: 1024,
                },
                accessed: std::time::UNIX_EPOCH,
                modified: std::time::UNIX_EPOCH,
                created: std::time::UNIX_EPOCH,
            })
        }

        async fn set_timestamps(
            &self,
            _path: &StoragePath,
            value: TimestampMetadata,
        ) -> smb_domain::Result<()> {
            self.sets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(value);
            Ok(())
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
    async fn copied_timestamps_are_bound_to_the_observed_source_version()
    -> Result<(), Box<dyn std::error::Error>> {
        let protocol = Arc::new(ProbeProtocol::default());
        let identity = BackendIdentity::new(crate::model::BackendKind::Cifs, "test")?;
        let adapter = CifsMetadata::new(protocol.clone(), identity.clone());
        let path = StoragePath::new("source")?;
        let mut inline = protocol.metadata(&path).await?;
        let plan = adapter
            .copied_metadata_observation_plan()
            .ok_or("missing plan")?;
        let descriptor =
            descriptor_from_facts(&identity, &path, &inline.facts, Operation::Metadata)?;
        let observed = adapter
            .observe_bound(&path, &descriptor.source_identity, plan)
            .await?;
        assert!(matches!(
            observed.timestamps(),
            MetadataObservation::Value { .. }
        ));
        assert_eq!(
            observed.ownership_mode(),
            &MetadataObservation::NotApplicable
        );
        assert_eq!(protocol.acl_calls.load(Ordering::SeqCst), 0);
        inline.facts.identity = bytes::Bytes::from_static(b"different-version");
        let changed = descriptor_from_facts(&identity, &path, &inline.facts, Operation::Metadata)?;
        assert!(
            matches!(adapter.observe_bound(&path, &changed.source_identity, plan).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
        );
        Ok(())
    }

    #[tokio::test]
    async fn mtime_only_application_and_cancellation_preserve_unrequested_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let protocol = Arc::new(ProbeProtocol::default());
        let adapter = CifsMetadata::new(
            protocol.clone(),
            BackendIdentity::new(crate::model::BackendKind::Cifs, "test")?,
        );
        let path = StoragePath::new("target")?;
        let value = TimestampMetadata {
            accessed: None,
            modified: Some(StorageTimestamp::new(
                1_700_000_001_123_456_700,
                TimePrecision::HundredNanoseconds,
            )?),
            created: None,
        };
        let cancel = tokio_util::sync::CancellationToken::new();
        adapter
            .apply(&path, MetadataMutation::Timestamps(value), cancel.clone())
            .await?;
        let update = timestamp_update(value)?;
        assert_eq!(update.accessed, None);
        assert_eq!(update.created, None);
        assert_eq!(update.written.and_then(timestamp), value.modified);
        cancel.cancel();
        assert!(
            matches!(adapter.apply(&path, MetadataMutation::Timestamps(value), cancel).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled && error.transience() == crate::model::Transience::Transient)
        );
        assert_eq!(
            *protocol
                .sets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![value]
        );
        Ok(())
    }

    #[test]
    fn timestamp_conversion_handles_pre_epoch_and_rejects_overflow()
    -> Result<(), Box<dyn std::error::Error>> {
        let before = StorageTimestamp::new(-100, TimePrecision::HundredNanoseconds)?;
        assert_eq!(system_time(Some(before))?.and_then(timestamp), Some(before));
        assert!(
            system_time(Some(StorageTimestamp::new(
                i128::MAX,
                TimePrecision::Nanoseconds
            )?))
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn acl_requires_explicit_additional_call_mode() -> Result<(), Box<dyn std::error::Error>>
    {
        let protocol = Arc::new(ProbeProtocol::default());
        let metadata = CifsMetadata::new(
            Arc::clone(&protocol),
            BackendIdentity::new(crate::model::BackendKind::Cifs, "test")?,
        );
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
