use std::sync::Arc;

use async_trait::async_trait;

use super::acl;
use super::source::NfsProtocolFailure;
use crate::model::{
    ExtendedAttribute, MetadataObservation, MetadataObservations, MetadataProvenance,
    ObservationMode, ObservationPlan, OwnershipMode, StoragePath, StorageTimestamp, TimePrecision,
    TimestampMetadata,
};
use crate::storage::{Metadata, MetadataMutation, StorageRoleFailure};

pub(crate) struct NfsMetadataAdapter {
    protocol: Arc<dyn NfsMetadataProtocol>,
}

pub(crate) struct NfsMetadataInline {
    pub(crate) symlink: bool,
    pub(crate) uid: Option<u32>,
    pub(crate) gid: Option<u32>,
    pub(crate) mode: u32,
    pub(crate) atime: i64,
    pub(crate) mtime: i64,
    pub(crate) ctime: i64,
}

#[async_trait]
pub(crate) trait NfsMetadataProtocol: Send + Sync {
    async fn stat(&self, path: &StoragePath) -> Result<NfsMetadataInline, NfsProtocolFailure>;
    fn supports_acl(&self) -> bool;
    fn supports_xattrs(&self) -> bool;
    async fn get_acl(&self, path: &StoragePath) -> Result<nfs_rs::Acl, NfsProtocolFailure>;
    async fn get_xattrs(
        &self,
        path: &StoragePath,
    ) -> Result<Vec<ExtendedAttribute>, NfsProtocolFailure>;
    async fn set_acl(
        &self,
        path: &StoragePath,
        acl: &nfs_rs::Acl,
    ) -> Result<(), NfsProtocolFailure>;
    async fn set_xattr(
        &self,
        path: &StoragePath,
        value: &ExtendedAttribute,
    ) -> Result<(), NfsProtocolFailure>;
    async fn set_numeric_ownership(
        &self,
        path: &StoragePath,
        value: OwnershipMode,
    ) -> Result<(), NfsProtocolFailure>;
    async fn set_timestamps(
        &self,
        path: &StoragePath,
        value: TimestampMetadata,
    ) -> Result<(), NfsProtocolFailure>;
}

impl NfsMetadataAdapter {
    pub(crate) fn new(protocol: Arc<dyn NfsMetadataProtocol>) -> Self {
        Self { protocol }
    }
}

#[async_trait]
impl Metadata for NfsMetadataAdapter {
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let entry = self.protocol.stat(path).await.map_err(|error| {
            super::source::role_failure(path, crate::model::Operation::Metadata, error)
        })?;
        let symlink = entry.symlink;
        let acl = if symlink {
            optional_not_applicable(plan.acl())
        } else {
            observe_optional(path, plan.acl(), self.protocol.supports_acl(), || async {
                let value = self.protocol.get_acl(path).await?;
                acl::encode(&value).map_err(|_| super::source::NfsProtocolFailure::protocol())
            })
            .await?
        };
        let xattrs = if symlink {
            optional_not_applicable(plan.xattrs())
        } else {
            observe_optional(
                path,
                plan.xattrs(),
                self.protocol.supports_xattrs(),
                || async { self.protocol.get_xattrs(path).await },
            )
            .await?
        };
        let ownership = inline(
            plan.ownership_mode(),
            OwnershipMode {
                uid: entry.uid.unwrap_or_default(),
                gid: entry.gid.unwrap_or_default(),
                mode: entry.mode,
            },
        );
        let timestamps = inline(
            plan.timestamps(),
            TimestampMetadata {
                accessed: timestamp(entry.atime),
                modified: timestamp(entry.mtime),
                created: timestamp(entry.ctime),
            },
        );
        MetadataObservations::new(
            acl,
            xattrs,
            optional_not_applicable(plan.tags()),
            ownership,
            timestamps,
        )
        .map_err(|_| {
            super::source::entry_failure(
                path,
                crate::model::Operation::Metadata,
                crate::model::FailureClass::Protocol,
                crate::model::Transience::Permanent,
            )
        })
    }

    async fn apply(
        &self,
        path: &StoragePath,
        mutation: MetadataMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if cancel.is_cancelled() {
            return Err(super::source::cancelled(path));
        }
        let result = match mutation {
            MetadataMutation::Acl(value) => {
                let value = acl::decode(&value).map_err(|_| {
                    super::source::entry_failure(
                        path,
                        crate::model::Operation::Metadata,
                        crate::model::FailureClass::InvalidInput,
                        crate::model::Transience::Permanent,
                    )
                })?;
                self.protocol.set_acl(path, &value).await
            }
            MetadataMutation::Xattrs(values) => {
                for value in values {
                    if cancel.is_cancelled() {
                        return Err(super::source::cancelled(path));
                    }
                    self.protocol
                        .set_xattr(path, &value)
                        .await
                        .map_err(|error| {
                            super::source::role_failure(
                                path,
                                crate::model::Operation::Metadata,
                                error,
                            )
                        })?;
                }
                return Ok(());
            }
            MetadataMutation::NumericOwnership(value) => {
                self.protocol.set_numeric_ownership(path, value).await
            }
            MetadataMutation::Timestamps(value) => self.protocol.set_timestamps(path, value).await,
            MetadataMutation::Tags(_) | MetadataMutation::MappedOwnership(_) => {
                return Err(super::source::entry_failure(
                    path,
                    crate::model::Operation::Metadata,
                    crate::model::FailureClass::Unsupported,
                    crate::model::Transience::Permanent,
                ));
            }
        };
        result.map_err(|error| {
            super::source::role_failure(path, crate::model::Operation::Metadata, error)
        })
    }
}

fn timestamp(value: i64) -> Option<StorageTimestamp> {
    StorageTimestamp::new(i128::from(value), TimePrecision::Nanoseconds).ok()
}

fn inline<T>(mode: ObservationMode, value: T) -> MetadataObservation<T> {
    match mode {
        ObservationMode::Omit => MetadataObservation::NotRequested,
        ObservationMode::InlineOnly | ObservationMode::BestEffort | ObservationMode::Required => {
            MetadataObservation::Value {
                value,
                provenance: MetadataProvenance::Inline,
            }
        }
    }
}

fn optional_not_applicable<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::NotApplicable
    }
}

async fn observe_optional<T, F, Fut>(
    path: &StoragePath,
    mode: ObservationMode,
    supported: bool,
    operation: F,
) -> Result<MetadataObservation<T>, StorageRoleFailure>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, super::source::NfsProtocolFailure>>,
{
    if mode == ObservationMode::Omit {
        return Ok(MetadataObservation::NotRequested);
    }
    if mode == ObservationMode::InlineOnly {
        return Ok(MetadataObservation::NotRequested);
    }
    if !supported {
        return Ok(MetadataObservation::Unsupported);
    }
    match operation().await {
        Ok(value) => Ok(MetadataObservation::Value {
            value,
            provenance: MetadataProvenance::AdditionalCall,
        }),
        Err(error) if mode == ObservationMode::BestEffort => Ok(MetadataObservation::Failed {
            class: error.class,
            transience: error.transience,
        }),
        Err(error) => Err(super::source::entry_failure(
            path,
            crate::model::Operation::Metadata,
            error.class,
            error.transience,
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::model::{FailureClass, Transience};

    struct CancellingProtocol {
        cancel: tokio_util::sync::CancellationToken,
        sets: AtomicUsize,
    }

    #[async_trait]
    impl NfsMetadataProtocol for CancellingProtocol {
        async fn stat(&self, _path: &StoragePath) -> Result<NfsMetadataInline, NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        fn supports_acl(&self) -> bool {
            true
        }
        fn supports_xattrs(&self) -> bool {
            true
        }
        async fn get_acl(&self, _path: &StoragePath) -> Result<nfs_rs::Acl, NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn get_xattrs(
            &self,
            _path: &StoragePath,
        ) -> Result<Vec<ExtendedAttribute>, NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn set_acl(
            &self,
            _path: &StoragePath,
            _acl: &nfs_rs::Acl,
        ) -> Result<(), NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn set_xattr(
            &self,
            _path: &StoragePath,
            _value: &ExtendedAttribute,
        ) -> Result<(), NfsProtocolFailure> {
            self.sets.fetch_add(1, Ordering::SeqCst);
            self.cancel.cancel();
            Ok(())
        }
        async fn set_numeric_ownership(
            &self,
            _path: &StoragePath,
            _value: OwnershipMode,
        ) -> Result<(), NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn set_timestamps(
            &self,
            _path: &StoragePath,
            _value: TimestampMetadata,
        ) -> Result<(), NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
    }

    #[tokio::test]
    async fn modes_avoid_unrequested_or_unsupported_storage_calls() {
        for (mode, supported, expected) in [
            (
                ObservationMode::Omit,
                true,
                MetadataObservation::NotRequested,
            ),
            (
                ObservationMode::InlineOnly,
                true,
                MetadataObservation::NotRequested,
            ),
            (
                ObservationMode::Required,
                false,
                MetadataObservation::Unsupported,
            ),
        ] {
            let calls = AtomicUsize::new(0);
            let result = observe_optional(&StoragePath::root(), mode, supported, || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, super::super::source::NfsProtocolFailure>(7_u8)
            })
            .await
            .unwrap_or_else(|error| panic!("{error}"));
            assert_eq!(result, expected);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn optional_failure_policy_preserves_entry_scope() {
        let path = StoragePath::new("file").unwrap_or_else(|error| panic!("{error}"));
        let failure = super::super::source::NfsProtocolFailure {
            class: FailureClass::PermissionDenied,
            transience: Transience::Permanent,
        };
        let observed = observe_optional(&path, ObservationMode::BestEffort, true, || async {
            Err::<u8, _>(failure)
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
        assert!(matches!(
            observed,
            MetadataObservation::Failed {
                class: FailureClass::PermissionDenied,
                ..
            }
        ));

        let required = observe_optional(&path, ObservationMode::Required, true, || async {
            Err::<u8, _>(failure)
        })
        .await;
        assert!(matches!(required, Err(StorageRoleFailure::Entry(error)) if error.path() == &path));
    }

    #[tokio::test]
    async fn xattr_apply_stops_before_the_next_remote_mutation_after_cancel() {
        let cancel = tokio_util::sync::CancellationToken::new();
        let protocol = Arc::new(CancellingProtocol {
            cancel: cancel.clone(),
            sets: AtomicUsize::new(0),
        });
        let adapter = NfsMetadataAdapter::new(protocol.clone());
        let values = vec![
            ExtendedAttribute::new(b"one".to_vec(), b"1".to_vec())
                .unwrap_or_else(|error| panic!("{error}")),
            ExtendedAttribute::new(b"two".to_vec(), b"2".to_vec())
                .unwrap_or_else(|error| panic!("{error}")),
        ];
        let result = adapter
            .apply(
                &StoragePath::new("file").unwrap_or_else(|error| panic!("{error}")),
                MetadataMutation::Xattrs(values),
                cancel,
            )
            .await;
        assert!(result.is_err());
        assert_eq!(protocol.sets.load(Ordering::SeqCst), 1);
    }
}
