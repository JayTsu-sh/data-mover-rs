//! Core current-path helper + production strong chain + actual NFS metadata adapter.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{Result, Script};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, ExtendedAttribute, FailureClass, IdentityStrength,
    ObservationMode, ObservationPlan, OwnershipMode, SourceIdentity, StoragePath,
    TimestampMetadata,
};
use crate::storage::backends::nfs::metadata::{
    NfsMetadataAdapter, NfsMetadataInline, NfsMetadataProtocol,
};
use crate::storage::backends::nfs::source::{NfsProtocolFailure, role_failure};
use crate::storage::{
    BackendCapabilities, CapabilityAvailability, ListRequest, ListingStream, Metadata, Namespace,
    NamespaceReadCause, NamespaceReadFailure, NamespaceReadOutcome, NamespaceReadPolicy,
    SourceDescriptor, Storage, StorageBuilder, StorageRoleFailure, UnsupportedReason,
    read_namespace_entry,
};

struct Names {
    calls: Script,
    identity: BackendIdentity,
    metadata_calls: AtomicUsize,
}

#[async_trait]
impl Namespace for Names {
    async fn stat(
        &self,
        path: &StoragePath,
        fresh: bool,
    ) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        assert!(
            fresh,
            "the current-path helper must request current-name semantics"
        );
        let (handle, _) = self
            .calls
            .strong(path)
            .await
            .map_err(|failure| role_failure(path, crate::model::Operation::Observe, failure))?;
        Ok(SourceDescriptor::new(
            path.clone(),
            EntryKind::File,
            Some(1),
            SourceIdentity::new(
                self.identity.clone(),
                IdentityStrength::StableWithinBackend,
                &handle,
            )
            .unwrap(),
        ))
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        ListingStream::from_pages(request.path, [])
    }
}

fn unused() -> NfsProtocolFailure {
    NfsProtocolFailure::protocol()
}

#[async_trait]
impl NfsMetadataProtocol for Names {
    async fn stat(
        &self,
        _: &StoragePath,
    ) -> std::result::Result<NfsMetadataInline, NfsProtocolFailure> {
        self.metadata_calls.fetch_add(1, Ordering::AcqRel);
        let file = self
            .calls
            .cached_file()
            .await
            .map_err(crate::storage::backends::nfs::protocol::classify_error)?;
        let generation = i64::from(file.fh == b"file:1"[..]);
        Ok(NfsMetadataInline {
            file_handle: file.fh,
            symlink: false,
            ownership_available: true,
            uid: Some(1),
            gid: Some(1),
            mode: Some(0o640),
            atime: Some(generation),
            mtime: Some(generation),
            ctime: Some(generation),
        })
    }
    fn supports_acl(&self) -> bool {
        false
    }
    fn supports_xattrs(&self) -> bool {
        false
    }
    async fn get_acl(
        &self,
        _: &StoragePath,
    ) -> std::result::Result<nfs_rs::Acl, NfsProtocolFailure> {
        Err(unused())
    }
    async fn get_xattrs(
        &self,
        _: &StoragePath,
    ) -> std::result::Result<Vec<ExtendedAttribute>, NfsProtocolFailure> {
        Err(unused())
    }
    async fn set_acl(
        &self,
        _: &StoragePath,
        _: &nfs_rs::Acl,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        Err(unused())
    }
    async fn set_xattr(
        &self,
        _: &StoragePath,
        _: &ExtendedAttribute,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        Err(unused())
    }
    async fn set_numeric_ownership(
        &self,
        _: &StoragePath,
        _: OwnershipMode,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        Err(unused())
    }
    async fn set_timestamps(
        &self,
        _: &StoragePath,
        _: TimestampMetadata,
    ) -> std::result::Result<(), NfsProtocolFailure> {
        Err(unused())
    }
}

fn fixture() -> Result<(Storage, Arc<Names>, Arc<NfsMetadataAdapter>)> {
    let identity = BackendIdentity::new(BackendKind::Nfs, "strong-chain-helper")?;
    let names = Arc::new(Names {
        calls: Script::new(),
        identity: identity.clone(),
        metadata_calls: AtomicUsize::new(0),
    });
    names.calls.warm();
    let metadata = Arc::new(NfsMetadataAdapter::new(names.clone(), identity.clone()));
    let no = CapabilityAvailability::Unsupported(UnsupportedReason::new("unused")?);
    let storage = StorageBuilder::new(
        identity,
        BackendCapabilities::new(
            no.clone(),
            no,
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
        ),
    )
    .namespace(names.clone())
    .metadata(metadata.clone())
    .build()?;
    Ok((storage, names, metadata))
}

fn plan() -> ObservationPlan {
    ObservationPlan::default().with_timestamps(ObservationMode::Required)
}
fn policy() -> NamespaceReadPolicy {
    NamespaceReadPolicy {
        operation_timeout: Duration::from_secs(1),
        recovery_window: Duration::from_secs(3),
        retry_interval: Duration::from_millis(100),
    }
}

#[tokio::test]
async fn strong_helper_deep_file_none_and_some_use_rebuilt_binding_for_metadata() -> Result {
    for requested in [Some(plan()), None] {
        let (storage, names, metadata) = fixture()?;
        names.calls.generation.store(1, Ordering::Release);
        let path = StoragePath::new("a/b/file")?;
        let outcome = read_namespace_entry(
            &storage,
            &path,
            requested,
            policy(),
            &CancellationToken::new(),
        )
        .await?;
        let expected = match outcome {
            NamespaceReadOutcome::Descriptor {
                descriptor: Some(descriptor),
                attempts: 1,
            } => descriptor.source_identity,
            NamespaceReadOutcome::Observation {
                entry: Some(entry),
                attempts: 1,
            } => {
                let time = entry
                    .metadata()
                    .timestamps()
                    .value()
                    .unwrap()
                    .modified
                    .unwrap();
                assert_eq!(
                    time,
                    crate::model::StorageTimestamp::new(
                        1,
                        crate::model::TimePrecision::Nanoseconds
                    )?
                );
                entry.source_identity().clone()
            }
            other => panic!("unexpected helper outcome: {other:?}"),
        };
        assert_eq!(
            expected,
            SourceIdentity::new(
                names.identity.clone(),
                IdentityStrength::StableWithinBackend,
                b"file:1"
            )?
        );
        // Explicit existing expected-source guard must succeed after the same strong read.
        metadata.observe_bound(&path, &expected, plan()).await?;
    }
    Ok(())
}

#[tokio::test]
async fn strong_helper_changed_or_absent_root_refuses_both_branches_without_metadata() -> Result {
    for requested in [Some(plan()), None] {
        for root_state in [1, 2] {
            let (storage, names, _) = fixture()?;
            names.calls.root_state.store(root_state, Ordering::Release);
            let result = read_namespace_entry(
                &storage,
                &StoragePath::new("a/b/file")?,
                requested,
                policy(),
                &CancellationToken::new(),
            )
            .await;
            assert!(
                matches!(result, Err(NamespaceReadFailure::Operation { last, attempts: 1, .. }) if matches!(*last, NamespaceReadCause::Role(StorageRoleFailure::Entry(ref error)) if error.class() == FailureClass::Conflict))
            );
            assert_eq!(names.metadata_calls.load(Ordering::Acquire), 0);
            assert!(
                names
                    .calls
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|call| !call.starts_with("child:"))
            );
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn strong_helper_pending_root_and_each_level_cancel_without_reopening_budget() -> Result {
    for requested in [Some(plan()), None] {
        for stage in 0..=3 {
            let (storage, names, _) = fixture()?;
            names.calls.stall_stage.store(stage, Ordering::Release);
            let cancel = CancellationToken::new();
            let child_cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(250)).await;
                child_cancel.cancel();
            });
            let start = tokio::time::Instant::now();
            let result = read_namespace_entry(
                &storage,
                &StoragePath::new("a/b/file")?,
                requested,
                policy(),
                &cancel,
            )
            .await;
            assert!(matches!(
                result,
                Err(NamespaceReadFailure::Cancelled { .. })
            ));
            assert_eq!(start.elapsed(), Duration::from_millis(250));
            assert_eq!(names.metadata_calls.load(Ordering::Acquire), 0);
        }
    }
    Ok(())
}

fn class(cause: &NamespaceReadCause) -> Option<FailureClass> {
    match cause {
        NamespaceReadCause::Role(StorageRoleFailure::Entry(error)) => Some(error.class()),
        NamespaceReadCause::Role(StorageRoleFailure::Session(error)) => Some(error.class()),
        _ => None,
    }
}

#[tokio::test(start_paused = true)]
async fn strong_helper_faults_preserve_typed_first_last_causes_and_recovery_window() -> Result {
    for requested in [Some(plan()), None] {
        for (kind, expected) in [(1, FailureClass::Connectivity), (2, FailureClass::Timeout)] {
            for stage in 0..=3 {
                let (storage, names, _) = fixture()?;
                names.calls.denied_stage.store(stage, Ordering::Release);
                names.calls.error_kind.store(kind, Ordering::Release);
                let start = tokio::time::Instant::now();
                let result = read_namespace_entry(
                    &storage,
                    &StoragePath::new("a/b/file")?,
                    requested,
                    policy(),
                    &CancellationToken::new(),
                )
                .await;
                match result {
                    Err(NamespaceReadFailure::Operation {
                        first,
                        last,
                        attempts,
                        ..
                    }) => {
                        assert_eq!(class(&first), Some(expected));
                        assert_eq!(class(&last), Some(expected));
                        assert!(attempts > 1);
                    }
                    other => panic!("unexpected result: {other:?}"),
                }
                assert_eq!(start.elapsed(), Duration::from_secs(3));
                assert_eq!(names.metadata_calls.load(Ordering::Acquire), 0);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn strong_helper_child_missing_is_absence_under_the_verified_root_in_both_branches() -> Result
{
    for requested in [Some(plan()), None] {
        let (storage, names, _) = fixture()?;
        names.calls.missing_stage.store(2, Ordering::Release);
        let result = read_namespace_entry(
            &storage,
            &StoragePath::new("a/b/file")?,
            requested,
            policy(),
            &CancellationToken::new(),
        )
        .await?;
        assert!(matches!(
            result,
            NamespaceReadOutcome::Descriptor {
                descriptor: None,
                attempts: 1
            } | NamespaceReadOutcome::Observation {
                entry: None,
                attempts: 1
            }
        ));
        assert_eq!(names.metadata_calls.load(Ordering::Acquire), 0);
    }
    Ok(())
}
