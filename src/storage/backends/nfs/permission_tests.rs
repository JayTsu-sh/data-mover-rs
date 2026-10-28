use super::*;
use crate::model::{BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity};
use crate::storage::PrincipalIdentity;

#[tokio::test]
async fn permissions_keep_mode_when_nfs_owner_names_cannot_be_mapped()
-> Result<(), Box<dyn std::error::Error>> {
    for owner_unmapped in [false, true] {
        let protocol = Arc::new(CancellingProtocol {
            cancel: tokio_util::sync::CancellationToken::new(),
            sets: AtomicUsize::new(0),
            xattrs_supported: false,
            owner_unmapped,
        });
        let identity = BackendIdentity::new(BackendKind::Nfs, "permissions")?;
        let adapter = NfsMetadataAdapter::new(protocol.clone(), identity.clone());
        let descriptor = SourceDescriptor::new(
            StoragePath::new("file")?,
            EntryKind::File,
            Some(4),
            SourceIdentity::new(
                identity,
                IdentityStrength::StableWithinBackend,
                b"source-handle",
            )?,
        )
        .with_inline_ownership(if owner_unmapped {
            InlineOwnership::Unmapped
        } else {
            InlineOwnership::Known(OwnershipMode {
                uid: 12345,
                gid: 12346,
                mode: 0o640,
            })
        });
        let facts = adapter.permissions(&descriptor).await?;
        assert_eq!(facts.mode, Some(0o640));
        assert_eq!(
            facts.owner,
            (!owner_unmapped).then_some(PrincipalIdentity::Numeric(12345))
        );
        assert_eq!(
            facts.group,
            (!owner_unmapped).then_some(PrincipalIdentity::Numeric(12346))
        );
        // Mapped facts are inline; unmapped names require only a bound stat, never ACL/xattr I/O.
        assert_eq!(
            protocol.sets.load(Ordering::SeqCst),
            usize::from(owner_unmapped)
        );
    }
    Ok(())
}

#[tokio::test]
async fn permissions_reject_named_versions_before_using_current_inline_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(CancellingProtocol {
        cancel: tokio_util::sync::CancellationToken::new(),
        sets: AtomicUsize::new(0),
        xattrs_supported: false,
        owner_unmapped: false,
    });
    let identity = BackendIdentity::new(BackendKind::Nfs, "permission-version")?;
    let adapter = NfsMetadataAdapter::new(protocol.clone(), identity.clone());
    let mut descriptor = SourceDescriptor::new(
        StoragePath::new("file")?,
        EntryKind::File,
        Some(4),
        SourceIdentity::new(
            identity,
            IdentityStrength::StableWithinBackend,
            b"source-handle",
        )?,
    )
    .with_inline_ownership(InlineOwnership::Known(OwnershipMode {
        uid: 12345,
        gid: 12346,
        mode: 0o640,
    }));
    descriptor.version = crate::model::SourceVersion::Id("old-version".into());
    let result = adapter.permissions(&descriptor).await;
    assert!(matches!(result, Err(StorageRoleFailure::Entry(ref entry))
        if entry.class() == FailureClass::Unsupported && entry.operation() == crate::model::Operation::Metadata));
    assert_eq!(protocol.sets.load(Ordering::SeqCst), 0);
    Ok(())
}
