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
    identity: crate::model::BackendIdentity,
}

pub(crate) struct NfsMetadataInline {
    pub(crate) file_handle: bytes::Bytes,
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
    /// Change permission bits without synthesizing or rewriting numeric ownership.
    async fn set_mode(&self, path: &StoragePath, mode: u32) -> Result<(), NfsProtocolFailure> {
        let _ = (path, mode);
        Err(NfsProtocolFailure {
            class: crate::model::FailureClass::Unsupported,
            transience: crate::model::Transience::Permanent,
        })
    }
    async fn set_timestamps(
        &self,
        path: &StoragePath,
        value: TimestampMetadata,
    ) -> Result<(), NfsProtocolFailure>;
}

impl NfsMetadataAdapter {
    pub(crate) fn new(
        protocol: Arc<dyn NfsMetadataProtocol>,
        identity: crate::model::BackendIdentity,
    ) -> Self {
        Self { protocol, identity }
    }

    async fn observe_entry(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
        entry: NfsMetadataInline,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if plan.ownership_mode() == ObservationMode::Required
            && (entry.uid.is_none() || entry.gid.is_none())
        {
            return Err(unsupported(path));
        }
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
}

#[async_trait]
impl Metadata for NfsMetadataAdapter {
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        Some(
            ObservationPlan::default()
                .with_ownership_mode(ObservationMode::Required)
                .with_timestamps(ObservationMode::Required),
        )
    }

    async fn observe_bound(
        &self,
        path: &StoragePath,
        expected: &crate::model::SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let entry = self.protocol.stat(path).await.map_err(|error| {
            super::source::role_failure(path, crate::model::Operation::Metadata, error)
        })?;
        let observed = crate::model::SourceIdentity::new(
            self.identity.clone(),
            crate::model::IdentityStrength::StableWithinBackend,
            &entry.file_handle,
        )
        .map_err(|_| {
            super::source::entry_failure(
                path,
                crate::model::Operation::Metadata,
                crate::model::FailureClass::Protocol,
                crate::model::Transience::Permanent,
            )
        })?;
        if observed != *expected {
            return Err(super::source::entry_failure(
                path,
                crate::model::Operation::Metadata,
                crate::model::FailureClass::Conflict,
                crate::model::Transience::Permanent,
            ));
        }
        self.observe_entry(path, plan, entry).await
    }

    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let entry = self.protocol.stat(path).await.map_err(|error| {
            super::source::role_failure(path, crate::model::Operation::Metadata, error)
        })?;
        self.observe_entry(path, plan, entry).await
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
                // The xattr branch below has always checked this. Without the same check here a
                // mount that never negotiated ACL support still sends a SETACL, and the caller
                // gets whatever protocol error comes back instead of the typed refusal that says
                // the destination cannot do this at all.
                if !self.protocol.supports_acl() {
                    return Err(unsupported(path));
                }
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
                if !self.protocol.supports_xattrs() {
                    return Err(unsupported(path));
                }
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
            MetadataMutation::Mode(mode) => self.protocol.set_mode(path, mode).await,
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

fn unsupported(path: &StoragePath) -> StorageRoleFailure {
    super::source::entry_failure(
        path,
        crate::model::Operation::Metadata,
        crate::model::FailureClass::Unsupported,
        crate::model::Transience::Permanent,
    )
}

pub(crate) fn timestamp(value: i64) -> Option<StorageTimestamp> {
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

    use super::super::source::role_failure;
    use super::*;
    use crate::model::{FailureClass, Transience};

    struct CancellingProtocol {
        cancel: tokio_util::sync::CancellationToken,
        sets: AtomicUsize,
        xattrs_supported: bool,
    }

    #[async_trait]
    impl NfsMetadataProtocol for CancellingProtocol {
        async fn stat(&self, _path: &StoragePath) -> Result<NfsMetadataInline, NfsProtocolFailure> {
            self.sets.fetch_add(1, Ordering::SeqCst);
            Ok(NfsMetadataInline {
                file_handle: bytes::Bytes::from_static(b"source-handle"),
                symlink: false,
                uid: Some(12345),
                gid: Some(12346),
                mode: 0o640,
                atime: 1_700_000_000_123_456_789,
                mtime: 1_700_000_001_123_456_789,
                ctime: 1_700_000_002_123_456_789,
            })
        }
        fn supports_acl(&self) -> bool {
            true
        }
        fn supports_xattrs(&self) -> bool {
            self.xattrs_supported
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
        async fn set_mode(&self, _path: &StoragePath, mode: u32) -> Result<(), NfsProtocolFailure> {
            // Any unexpected stat first increments `sets`, making this assertion fail.
            self.sets
                .compare_exchange(0, mode as usize, Ordering::SeqCst, Ordering::SeqCst)
                .map_err(|_| NfsProtocolFailure::protocol())?;
            Ok(())
        }
        async fn set_timestamps(
            &self,
            _path: &StoragePath,
            _value: TimestampMetadata,
        ) -> Result<(), NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
    }

    /// `BestEffort` tolerates a refusal only while it is entry-scoped: a session-scoped failure
    /// ends every write after it, so it is never tolerated. A server refusing SETACL therefore has
    /// to stay entry-scoped, or every best-effort ACL copy to such a server becomes a failed copy.
    /// This pins the class-to-scope step (only a lost connection is session-scoped); the status to
    /// class step is `classify_error`'s.
    #[tokio::test]
    async fn a_refused_setacl_stays_entry_scoped_and_only_a_lost_connection_does_not() {
        let protocol = Arc::new(CancellingProtocol {
            cancel: tokio_util::sync::CancellationToken::new(),
            sets: AtomicUsize::new(0),
            xattrs_supported: false,
        });
        let identity = crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "dest")
            .unwrap_or_else(|error| panic!("{error}"));
        let adapter = NfsMetadataAdapter::new(protocol, identity);
        let path = StoragePath::new("file").unwrap_or_else(|error| panic!("{error}"));
        let acl = acl::encode(&nfs_rs::Acl::default()).unwrap_or_else(|error| panic!("{error:?}"));
        let refused = adapter
            .apply(
                &path,
                MetadataMutation::Acl(acl),
                tokio_util::sync::CancellationToken::new(),
            )
            .await;
        assert!(
            matches!(refused, Err(StorageRoleFailure::Entry(_))),
            "{refused:?}"
        );
        for class in [
            FailureClass::Protocol,
            FailureClass::PermissionDenied,
            FailureClass::Unsupported,
            FailureClass::InvalidInput,
        ] {
            let failure = NfsProtocolFailure {
                class,
                transience: Transience::Permanent,
            };
            assert!(matches!(
                role_failure(&path, crate::model::Operation::Metadata, failure),
                StorageRoleFailure::Entry(_)
            ));
        }
        let lost = NfsProtocolFailure {
            class: FailureClass::Connectivity,
            transience: Transience::Transient,
        };
        assert!(matches!(
            role_failure(&path, crate::model::Operation::Metadata, lost),
            StorageRoleFailure::Session(_)
        ));
    }

    #[tokio::test]
    async fn automatic_metadata_is_bound_to_the_described_handle() {
        let protocol = Arc::new(CancellingProtocol {
            cancel: tokio_util::sync::CancellationToken::new(),
            sets: AtomicUsize::new(0),
            xattrs_supported: true,
        });
        let identity = crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "source")
            .unwrap_or_else(|error| panic!("{error}"));
        let adapter = NfsMetadataAdapter::new(protocol.clone(), identity.clone());
        let plan = adapter
            .copied_metadata_observation_plan()
            .unwrap_or_else(|| panic!("metadata observation plan must be present"));
        let path = StoragePath::new("file").unwrap_or_else(|error| panic!("{error}"));
        for (handle, matches) in [
            (b"source-handle".as_slice(), true),
            (b"replaced-handle".as_slice(), false),
        ] {
            let expected = crate::model::SourceIdentity::new(
                identity.clone(),
                crate::model::IdentityStrength::StableWithinBackend,
                handle,
            )
            .unwrap_or_else(|error| panic!("{error}"));
            let result = adapter.observe_bound(&path, &expected, plan).await;
            if matches {
                let observations = result.unwrap_or_else(|error| panic!("{error}"));
                assert!(
                    matches!(observations.ownership_mode(), MetadataObservation::Value { value, .. } if *value == OwnershipMode { uid: 12345, gid: 12346, mode: 0o640 })
                );
                assert!(
                    matches!(observations.timestamps(), MetadataObservation::Value { value, .. } if value.modified.is_some_and(|modified| modified.unix_nanos() == 1_700_000_001_123_456_789))
                );
                assert!(matches!(
                    observations.acl(),
                    MetadataObservation::NotRequested
                ));
                assert!(matches!(
                    observations.xattrs(),
                    MetadataObservation::NotRequested
                ));
            } else {
                assert!(
                    matches!(result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
                );
            }
        }
        // One stat supplies both the handle and baseline attributes; no extra identity RPC.
        assert_eq!(protocol.sets.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn automatic_metadata_does_not_substitute_root_for_unknown_ownership() {
        let protocol = Arc::new(CancellingProtocol {
            cancel: tokio_util::sync::CancellationToken::new(),
            sets: AtomicUsize::new(0),
            xattrs_supported: true,
        });
        let identity = crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "source")
            .unwrap_or_else(|error| panic!("{error}"));
        let adapter = NfsMetadataAdapter::new(protocol.clone(), identity);
        let path = StoragePath::new("file").unwrap_or_else(|error| panic!("{error}"));
        for missing_uid in [true, false] {
            let mut entry = protocol
                .stat(&path)
                .await
                .unwrap_or_else(|_| panic!("stat failed"));
            if missing_uid {
                entry.uid = None;
            } else {
                entry.gid = None;
            }
            let result = adapter
                .observe_entry(
                    &path,
                    adapter
                        .copied_metadata_observation_plan()
                        .unwrap_or_else(|| panic!("metadata observation plan must be present")),
                    entry,
                )
                .await;
            assert!(
                matches!(result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Unsupported)
            );
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
            xattrs_supported: true,
        });
        let adapter = NfsMetadataAdapter::new(
            protocol.clone(),
            crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "metadata-test")
                .unwrap_or_else(|error| panic!("{error}")),
        );
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
    #[tokio::test]
    async fn mode_only_apply_never_observes_or_rewrites_numeric_ownership()
    -> Result<(), Box<dyn std::error::Error>> {
        let protocol = Arc::new(CancellingProtocol {
            cancel: tokio_util::sync::CancellationToken::new(),
            sets: AtomicUsize::new(0),
            xattrs_supported: false,
        });
        let adapter = NfsMetadataAdapter::new(
            protocol.clone(),
            crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "mode-only")?,
        );
        let path = StoragePath::new("file")?;
        adapter
            .apply(
                &path,
                MetadataMutation::Mode(0o640),
                tokio_util::sync::CancellationToken::new(),
            )
            .await?;
        assert_eq!(protocol.sets.load(Ordering::SeqCst), 0o640);
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let result = adapter
            .apply(&path, MetadataMutation::Mode(0o600), cancel)
            .await;
        assert!(
            matches!(result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
        );
        assert_eq!(protocol.sets.load(Ordering::SeqCst), 0o640);
        Ok(())
    }
}
