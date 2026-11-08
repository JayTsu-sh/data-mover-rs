//! 源删除资格来自原始扫描证据；协议错误和取消不能升级为成功或重试。
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::DirectoryCookie;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::purge::{validate, verify_current};
use super::{
    NfsListingPages, NfsNamespaceAdapter, NfsNamespaceObservation, NfsNamespaceProtocol,
    NfsReadEnd, NfsReadFailure,
};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, FailureClass, IdentityStrength, MetadataObservation,
    MetadataObservations, MetadataProvenance, ObservedEntry, OwnershipMode, SourceIdentity,
    StoragePath, StorageTimestamp, SymlinkTarget, SymlinkTargetEncoding, TimePrecision,
    TimestampMetadata, Transience,
};
use crate::storage::backends::nfs::source::NfsProtocolFailure;
use crate::storage::{InlineOwnership, Namespace, PurgeOutcome, StorageRoleFailure};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type ProtocolResult<T> = std::result::Result<T, NfsProtocolFailure>;

fn sample(kind: EntryKind) -> Result<(ObservedEntry, NfsNamespaceObservation)> {
    let path = StoragePath::new("dir/entry")?;
    let identity = SourceIdentity::new(
        BackendIdentity::new(BackendKind::Nfs, "purge")?,
        IdentityStrength::StableWithinBackend,
        b"original",
    )?;
    let time = Some(StorageTimestamp::new(123, TimePrecision::Nanoseconds)?);
    let times = TimestampMetadata {
        accessed: time,
        modified: time,
        created: time,
    };
    let owner = OwnershipMode {
        uid: 1,
        gid: 2,
        mode: 0o644,
    };
    let mut metadata = MetadataObservations::default();
    metadata.set_timestamps(MetadataObservation::Value {
        value: times,
        provenance: MetadataProvenance::Inline,
    });
    metadata.set_ownership_mode(MetadataObservation::Value {
        value: owner,
        provenance: MetadataProvenance::Inline,
    });
    let size = (kind == EntryKind::File).then_some(3);
    let entry = if kind == EntryKind::Symlink {
        ObservedEntry::new_symlink(
            path.clone(),
            time,
            identity,
            SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"missing".to_vec())?,
        )?
    } else {
        ObservedEntry::new(path.clone(), kind, size, time, identity)?
    }
    .with_metadata(metadata);
    let current = NfsNamespaceObservation {
        path,
        kind,
        size,
        file_handle: Bytes::from_static(b"original"),
        timestamps: Some(times),
        mode: Some(owner.mode),
        uid: Some(owner.uid),
        gid: Some(owner.gid),
        ownership: Some(InlineOwnership::Known(owner)),
    };
    Ok((entry, current))
}

#[test]
fn files_and_links_require_original_change_evidence() -> Result {
    for kind in [EntryKind::File, EntryKind::Symlink] {
        let (entry, mut current) = sample(kind)?;
        assert!(verify_current(&entry, &current).is_ok());
        current.timestamps.as_mut().ok_or("missing times")?.created =
            Some(StorageTimestamp::new(124, TimePrecision::Nanoseconds)?);
        assert!(
            matches!(verify_current(&entry, &current), Err(error) if error.class == FailureClass::Conflict)
        );
        // 即使恢复 mtime、长度未变，变化后的 ctime 仍禁止源删除。
        let absent = entry.clone().with_metadata(MetadataObservations::default());
        assert!(
            matches!(validate(&absent), Err(error) if error.class == FailureClass::Unsupported)
        );
    }
    Ok(())
}

#[test]
fn directory_child_cleanup_changes_are_allowed_but_identity_and_owner_changes_are_not() -> Result {
    let (entry, mut current) = sample(EntryKind::Directory)?;
    current.timestamps = None;
    assert!(verify_current(&entry, &current).is_ok());
    current.file_handle = Bytes::from_static(b"replacement");
    assert!(
        matches!(verify_current(&entry, &current), Err(error) if error.class == FailureClass::Conflict)
    );
    current.file_handle = Bytes::from_static(b"original");
    current.ownership = Some(InlineOwnership::Unmapped);
    assert!(
        matches!(verify_current(&entry, &current), Err(error) if error.class == FailureClass::Conflict)
    );
    Ok(())
}

#[test]
fn replacement_size_mode_and_mtime_do_not_get_delete_authority() -> Result {
    for variant in 0..5 {
        let (entry, mut current) = sample(EntryKind::File)?;
        match variant {
            0 => current.file_handle = Bytes::from_static(b"replacement"),
            1 => current.size = Some(4),
            2 => {
                current.ownership = Some(InlineOwnership::Known(OwnershipMode {
                    uid: 1,
                    gid: 2,
                    mode: 0o600,
                }));
            }
            3 => current.timestamps.as_mut().ok_or("missing times")?.modified = None,
            _ => current.timestamps.as_mut().ok_or("missing times")?.created = None,
        }
        assert!(
            matches!(verify_current(&entry, &current), Err(error) if error.class == FailureClass::Conflict)
        );
    }
    Ok(())
}

struct Protocol {
    calls: AtomicUsize,
    outcome: usize,
    entered: Notify,
}
fn unsupported() -> NfsProtocolFailure {
    NfsProtocolFailure::new(FailureClass::Unsupported, Transience::Permanent)
}

#[async_trait]
impl NfsNamespaceProtocol for Protocol {
    async fn stat(&self, _: &StoragePath, _: bool) -> ProtocolResult<NfsNamespaceObservation> {
        Err(unsupported())
    }
    async fn directory_handle(&self, _: &StoragePath) -> ProtocolResult<Bytes> {
        Err(unsupported())
    }
    async fn read_directory(
        &self,
        _: &StoragePath,
        _: Bytes,
        _: DirectoryCookie,
        _: &mut dyn NfsListingPages,
    ) -> std::result::Result<NfsReadEnd, NfsReadFailure> {
        Ok(NfsReadEnd::Complete)
    }
    async fn forget_directory(&self, _: &StoragePath) -> ProtocolResult<()> {
        Err(unsupported())
    }
    async fn read_link(&self, _: &StoragePath) -> ProtocolResult<Bytes> {
        Err(unsupported())
    }
    async fn create_directory(&self, _: &StoragePath) -> ProtocolResult<()> {
        Err(unsupported())
    }
    async fn delete(
        &self,
        path: &StoragePath,
        kind: EntryKind,
        expected: Option<&ObservedEntry>,
    ) -> ProtocolResult<()> {
        assert!(expected.is_some_and(|entry| entry.path() == path && entry.kind() == kind));
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.entered.notify_one();
        match self.outcome {
            0 => Ok(()),
            1 => Err(NfsProtocolFailure::new(
                FailureClass::NotFound,
                Transience::Permanent,
            )),
            2 => Err(NfsProtocolFailure::new(
                FailureClass::Connectivity,
                Transience::Unknown,
            )),
            3 => Err(NfsProtocolFailure::new(
                FailureClass::Conflict,
                Transience::Permanent,
            )),
            _ => futures::future::pending().await,
        }
    }
    async fn rename(&self, _: &StoragePath, _: &StoragePath) -> ProtocolResult<()> {
        Err(unsupported())
    }
}

fn namespace(outcome: usize) -> Result<(NfsNamespaceAdapter, Arc<Protocol>)> {
    let protocol = Arc::new(Protocol {
        calls: AtomicUsize::new(0),
        outcome,
        entered: Notify::new(),
    });
    let namespace = NfsNamespaceAdapter::new(
        protocol.clone(),
        BackendIdentity::new(BackendKind::Nfs, "purge")?,
        None,
    );
    Ok((namespace, protocol))
}

#[tokio::test]
async fn confirmed_absent_and_unknown_outcomes_remain_distinct_without_reissue() -> Result {
    let (entry, _) = sample(EntryKind::File)?;
    for outcome in 0..4 {
        let (namespace, protocol) = namespace(outcome)?;
        let result = namespace
            .purge_observed(&entry, CancellationToken::new())
            .await;
        match outcome {
            0 => assert!(matches!(result, Ok(PurgeOutcome::Deleted { records: 1 }))),
            1 => assert!(matches!(result, Ok(PurgeOutcome::AlreadyAbsent))),
            2 => assert!(
                matches!(result, Err(StorageRoleFailure::Session(error)) if error.class() == FailureClass::Connectivity && error.transience() == Transience::Unknown)
            ),
            _ => assert!(
                matches!(result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
            ),
        }
        assert_eq!(protocol.calls.load(Ordering::Acquire), 1);
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_before_start_has_no_io_and_inflight_cancellation_remains_unknown() -> Result {
    let (entry, _) = sample(EntryKind::File)?;
    let (namespace, protocol) = namespace(4)?;
    let pre = CancellationToken::new();
    pre.cancel();
    assert!(namespace.purge_observed(&entry, pre).await.is_err());
    assert_eq!(protocol.calls.load(Ordering::Acquire), 0);
    let cancel = CancellationToken::new();
    let trigger = async {
        protocol.entered.notified().await;
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(namespace.purge_observed(&entry, cancel.clone()), trigger);
    assert!(
        matches!(result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled && error.transience() == Transience::Unknown)
    );
    assert_eq!(protocol.calls.load(Ordering::Acquire), 1);
    Ok(())
}

#[test]
fn partial_numeric_permissions_are_not_lost_when_full_ownership_is_unmapped() -> Result {
    use crate::model::{PermissionFacts, PrincipalIdentity};
    let (entry, mut current) = sample(EntryKind::Directory)?;
    let mut metadata = MetadataObservations::default();
    metadata.set_timestamps(entry.metadata().timestamps().clone());
    let entry = entry
        .with_metadata(metadata)
        .with_permissions(Some(Box::new(PermissionFacts {
            owner: Some(PrincipalIdentity::Numeric(1)),
            group: None,
            mode: Some(0o644),
        })))?;
    current.ownership = Some(InlineOwnership::Unmapped);
    assert!(verify_current(&entry, &current).is_ok());
    current.uid = Some(2);
    assert!(
        matches!(verify_current(&entry, &current), Err(error) if error.class == FailureClass::Conflict)
    );
    current.uid = Some(1);
    current.mode = Some(0o600);
    assert!(
        matches!(verify_current(&entry, &current), Err(error) if error.class == FailureClass::Conflict)
    );
    Ok(())
}
