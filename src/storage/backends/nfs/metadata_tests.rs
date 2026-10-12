use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::source::role_failure;
use super::*;
use crate::model::{FailureClass, Transience};

struct CancellingProtocol {
    cancel: tokio_util::sync::CancellationToken,
    sets: AtomicUsize,
    xattrs_supported: bool,
    /// The server named an owner and group nfs-rs could not map (`nfs::owner_id` → `None`).
    owner_unmapped: bool,
}

fn xattr_removal_adapter(supported: bool) -> (NfsMetadataAdapter, Arc<CancellingProtocol>) {
    let protocol = Arc::new(CancellingProtocol {
        cancel: tokio_util::sync::CancellationToken::new(),
        sets: AtomicUsize::new(0),
        xattrs_supported: supported,
        owner_unmapped: false,
    });
    let identity = crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "xattr")
        .unwrap_or_else(|error| panic!("{error}"));
    (
        NfsMetadataAdapter::new(protocol.clone(), identity),
        protocol,
    )
}

#[tokio::test]
async fn remove_xattr_preserves_protocol_success_and_refusal_classes() {
    let (adapter, protocol) = xattr_removal_adapter(true);
    let path = StoragePath::new("file").unwrap_or_else(|error| panic!("{error}"));
    adapter
        .remove_xattr(&path, b"user.existing")
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    for (name, class) in [
        (&b"user.denied"[..], FailureClass::PermissionDenied),
        (&b"user.missing"[..], FailureClass::NotFound),
    ] {
        assert!(matches!(adapter.remove_xattr(&path, name).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == class));
    }
    assert_eq!(protocol.sets.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn remove_xattr_rejects_unsupported_or_invalid_names_before_io() {
    let path = StoragePath::new("file").unwrap_or_else(|error| panic!("{error}"));
    let (adapter, protocol) = xattr_removal_adapter(false);
    assert!(
        matches!(adapter.remove_xattr(&path, b"user.existing").await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Unsupported)
    );
    assert_eq!(protocol.sets.load(Ordering::SeqCst), 0);
    let (adapter, protocol) = xattr_removal_adapter(true);
    for name in [&b""[..], &b"user.\0bad"[..], &[0xff][..]] {
        assert!(matches!(adapter.remove_xattr(&path, name).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::InvalidInput));
    }
    assert_eq!(protocol.sets.load(Ordering::SeqCst), 0);
}

#[async_trait]
impl NfsMetadataProtocol for CancellingProtocol {
    async fn stat(&self, _path: &StoragePath) -> Result<NfsMetadataInline, NfsProtocolFailure> {
        self.sets.fetch_add(1, Ordering::SeqCst);
        Ok(NfsMetadataInline {
            file_handle: bytes::Bytes::from_static(b"source-handle"),
            symlink: false,
            uid: (!self.owner_unmapped).then_some(12345),
            gid: (!self.owner_unmapped).then_some(12346),
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
    async fn remove_xattr(
        &self,
        _path: &StoragePath,
        name: &str,
    ) -> Result<(), NfsProtocolFailure> {
        self.sets.fetch_add(1, Ordering::SeqCst);
        match name {
            "user.existing" => Ok(()),
            "user.denied" => Err(NfsProtocolFailure::new(
                FailureClass::PermissionDenied,
                Transience::Permanent,
            )),
            _ => Err(NfsProtocolFailure::new(
                FailureClass::NotFound,
                Transience::Permanent,
            )),
        }
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

/// Application goes on past a refusal only while it is entry-scoped: a session-scoped failure
/// ends every write after it, so application stops there. A server refusing SETACL therefore
/// has to stay entry-scoped, or the families after it would never be applied or reported.
/// This pins the class-to-scope step (only a lost connection is session-scoped); the status to
/// class step is `classify_error`'s.
#[tokio::test]
async fn a_refused_setacl_stays_entry_scoped_and_only_a_lost_connection_does_not() {
    let protocol = Arc::new(CancellingProtocol {
        cancel: tokio_util::sync::CancellationToken::new(),
        sets: AtomicUsize::new(0),
        xattrs_supported: false,
        owner_unmapped: false,
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
        let failure = NfsProtocolFailure::new(class, Transience::Permanent);
        assert!(matches!(
            role_failure(&path, crate::model::Operation::Metadata, failure),
            StorageRoleFailure::Entry(_)
        ));
    }
    let lost = NfsProtocolFailure::new(FailureClass::Connectivity, Transience::Transient);
    assert!(matches!(
        role_failure(&path, crate::model::Operation::Metadata, lost),
        StorageRoleFailure::Session(_)
    ));
}

fn unmapped_owner_adapter() -> (NfsMetadataAdapter, crate::model::SourceIdentity) {
    let protocol = Arc::new(CancellingProtocol {
        cancel: tokio_util::sync::CancellationToken::new(),
        sets: AtomicUsize::new(0),
        xattrs_supported: false,
        owner_unmapped: true,
    });
    let identity = crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "source")
        .unwrap_or_else(|error| panic!("{error}"));
    let expected = crate::model::SourceIdentity::new(
        identity.clone(),
        crate::model::IdentityStrength::StableWithinBackend,
        b"source-handle",
    )
    .unwrap_or_else(|error| panic!("{error}"));
    (NfsMetadataAdapter::new(protocol, identity), expected)
}

/// An owner nfs-rs could not map used to be reported as uid 0 whenever ownership was not
/// `Required` (`unwrap_or_default`): root, made up.
#[tokio::test]
async fn an_unmapped_owner_is_never_reported_as_an_id() {
    let (adapter, _) = unmapped_owner_adapter();
    let path = StoragePath::new("file").unwrap_or_else(|error| panic!("{error}"));
    for mode in [ObservationMode::InlineOnly, ObservationMode::BestEffort] {
        let observations = adapter
            .observe(&path, ObservationPlan::default().with_ownership_mode(mode))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(
            matches!(
                observations.ownership_mode(),
                MetadataObservation::Unsupported
            ),
            "{mode:?}: {:?}",
            observations.ownership_mode()
        );
    }
}

/// A copy whose owner cannot be mapped carries the mode and records owner and group as
/// dropped — the same path as a mode-only source — instead of failing or writing nobody.
#[tokio::test]
async fn an_unmapped_owner_is_copied_as_mode_only() {
    let (adapter, expected) = unmapped_owner_adapter();
    let path = StoragePath::new("file").unwrap_or_else(|error| panic!("{error}"));
    let plan = adapter
        .copied_metadata_observation_plan()
        .unwrap_or_else(|| panic!("metadata observation plan must be present"));
    let copied = adapter
        .observe_copy_bound(&path, &expected, plan)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(copied.mode_without_ownership, Some(0o640));
    assert!(copied.owner_names_unmapped);
    assert!(matches!(
        copied.observations.ownership_mode(),
        MetadataObservation::NotRequested
    ));
}

#[tokio::test]
async fn automatic_metadata_is_bound_to_the_described_handle() {
    let protocol = Arc::new(CancellingProtocol {
        cancel: tokio_util::sync::CancellationToken::new(),
        sets: AtomicUsize::new(0),
        xattrs_supported: true,
        owner_unmapped: false,
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
        owner_unmapped: false,
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
    let failure = super::super::source::NfsProtocolFailure::new(
        FailureClass::PermissionDenied,
        Transience::Permanent,
    );
    let observed = observe_optional(&path, ObservationMode::BestEffort, true, || async {
        Err::<u8, _>(failure.clone())
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
        owner_unmapped: false,
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
        owner_unmapped: false,
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

/// Every plan, answered from a listed child's READDIRPLUS attributes, is exactly what a GETATTR
/// of the same attributes gives — values, provenance and the `Required`-ownership refusal — and
/// it declines only when an ACL or xattrs the mount supports are really wanted.
#[tokio::test]
async fn a_listed_child_is_observed_exactly_as_a_getattr_would() {
    let path = StoragePath::new("file").unwrap_or_else(|error| panic!("{error}"));
    let mut answered = 0;
    for (owner_unmapped, xattrs_supported) in [(false, false), (false, true), (true, true)] {
        let protocol = Arc::new(CancellingProtocol {
            cancel: tokio_util::sync::CancellationToken::new(),
            sets: AtomicUsize::new(0),
            xattrs_supported,
            owner_unmapped,
        });
        let identity = crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "src")
            .unwrap_or_else(|error| panic!("{error}"));
        let adapter = NfsMetadataAdapter::new(Arc::clone(&protocol) as _, identity.clone());
        let listed = listed_like_stat(&protocol, &path, identity).await;
        for plan in ObservationPlan::every() {
            let wants = |mode, supported| {
                supported
                    && matches!(
                        mode,
                        ObservationMode::BestEffort | ObservationMode::Required
                    )
            };
            let needs_call = wants(plan.acl(), true) || wants(plan.xattrs(), xattrs_supported);
            match adapter.observe_listed(&listed, plan) {
                Some(from_listing) => {
                    assert!(!needs_call, "{plan:?} answered without its call");
                    let from_getattr = adapter.observe(&path, plan).await;
                    assert_eq!(
                        format!("{from_listing:?}"),
                        format!("{from_getattr:?}"),
                        "{plan:?} unmapped={owner_unmapped}"
                    );
                    answered += 1;
                }
                None => assert!(needs_call, "{plan:?} declined without a reason"),
            }
        }
    }
    assert!(answered > 300, "{answered}");
}

/// The descriptor an NFS listing builds from the attributes the fake's GETATTR returns.
async fn listed_like_stat(
    protocol: &CancellingProtocol,
    path: &StoragePath,
    identity: crate::model::BackendIdentity,
) -> SourceDescriptor {
    let attrs = protocol
        .stat(path)
        .await
        .unwrap_or_else(|_| panic!("the fake's GETATTR always answers"));
    let source_identity = crate::model::SourceIdentity::new(
        identity,
        crate::model::IdentityStrength::StableWithinBackend,
        &attrs.file_handle,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let mut listed = SourceDescriptor::new(
        path.clone(),
        crate::model::EntryKind::File,
        Some(1),
        source_identity,
    )
    .with_inline_timestamps(TimestampMetadata {
        accessed: timestamp(attrs.atime),
        modified: timestamp(attrs.mtime),
        created: timestamp(attrs.ctime),
    });
    listed.inline_ownership = Some(inline_ownership(attrs.uid, attrs.gid, attrs.mode));
    listed
}
