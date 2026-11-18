use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{BackendIdentity, BackendKind, Operation};

#[derive(Default)]
struct PendingProtocol {
    entered: Notify,
    dropped: AtomicUsize,
    lookups: AtomicUsize,
    bound_writes: Mutex<Vec<(Bytes, MetadataMutation)>>,
}

impl PendingProtocol {
    async fn write(&self) -> Result<(), NfsProtocolFailure> {
        struct Active<'a>(&'a AtomicUsize);
        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let _active = Active(&self.dropped);
        self.entered.notify_one();
        pending().await
    }
}

#[async_trait]
impl NfsMetadataProtocol for PendingProtocol {
    async fn stat(&self, _: &StoragePath) -> Result<NfsMetadataInline, NfsProtocolFailure> {
        panic!("取消写入不能增加 stat")
    }
    fn supports_bound_directory_metadata(&self) -> bool {
        true
    }
    async fn resolve_bound_directory(&self, _: &StoragePath) -> Result<Bytes, NfsProtocolFailure> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        Ok(Bytes::from_static(b"current-directory"))
    }
    async fn apply_to_bound_directory(
        &self,
        handle: &Bytes,
        mutation: MetadataMutation,
        _: &CancellationToken,
    ) -> Result<(), NfsProtocolFailure> {
        let stalling = matches!(mutation, MetadataMutation::Mode(0));
        self.bound_writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((handle.clone(), mutation));
        if stalling {
            return self.write().await;
        }
        Ok(())
    }
    fn supports_acl(&self) -> bool {
        true
    }
    fn supports_xattrs(&self) -> bool {
        true
    }
    async fn get_acl(&self, _: &StoragePath) -> Result<nfs_rs::Acl, NfsProtocolFailure> {
        panic!("写入未确认不能读回 ACL")
    }
    async fn get_xattrs(
        &self,
        _: &StoragePath,
    ) -> Result<Vec<ExtendedAttribute>, NfsProtocolFailure> {
        panic!("取消写入不能增加 xattr 读取")
    }
    async fn set_acl(&self, _: &StoragePath, _: &nfs_rs::Acl) -> Result<(), NfsProtocolFailure> {
        self.write().await
    }
    async fn set_xattr(
        &self,
        _: &StoragePath,
        _: &ExtendedAttribute,
    ) -> Result<(), NfsProtocolFailure> {
        self.write().await
    }
    async fn set_numeric_ownership(
        &self,
        _: &StoragePath,
        _: OwnershipMode,
    ) -> Result<(), NfsProtocolFailure> {
        self.write().await
    }
    async fn set_mode(&self, _: &StoragePath, _: u32) -> Result<(), NfsProtocolFailure> {
        self.write().await
    }
    async fn set_timestamps(
        &self,
        _: &StoragePath,
        _: TimestampMetadata,
    ) -> Result<(), NfsProtocolFailure> {
        self.write().await
    }
}

#[tokio::test]
async fn in_flight_metadata_cancel_preserves_unknown_write_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    let mutations = [
        MetadataMutation::Mode(0o640),
        MetadataMutation::NumericOwnership(OwnershipMode {
            uid: 1,
            gid: 2,
            mode: 0o640,
        }),
        MetadataMutation::Timestamps(TimestampMetadata {
            accessed: None,
            modified: Some(timestamp(1).ok_or("time")?),
            created: None,
        }),
        MetadataMutation::Xattrs(vec![
            ExtendedAttribute::new(b"user.one".to_vec(), b"1".to_vec())?,
            ExtendedAttribute::new(b"user.two".to_vec(), b"2".to_vec())?,
        ]),
        MetadataMutation::Acl(acl::encode(&nfs_rs::Acl::default()).map_err(|e| format!("{e:?}"))?),
    ];
    for mutation in mutations {
        let protocol = Arc::new(PendingProtocol::default());
        let adapter = NfsMetadataAdapter::new(
            protocol.clone(),
            BackendIdentity::new(BackendKind::Nfs, "pending")?,
        );
        let path = StoragePath::new("nested/directory")?;
        let cancel = CancellationToken::new();
        let request = adapter.apply(&path, mutation, cancel.clone());
        tokio::pin!(request);
        tokio::select! {
            result = &mut request => panic!("请求未等待协议: {result:?}"),
            () = protocol.entered.notified() => {}
        }
        cancel.cancel();
        let result = timeout(Duration::from_millis(200), &mut request).await?;
        assert!(
            matches!(&result, Err(StorageRoleFailure::Entry(error))
            if error.path() == &path && error.operation() == Operation::Metadata
                && error.class() == FailureClass::Cancelled && error.transience() == Transience::Unknown && error.diagnostic().contains("unconfirmed")),
            "{result:?}"
        );
        assert_eq!(protocol.dropped.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_metadata_before_start_names_metadata_without_unknown_write()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(PendingProtocol::default());
    let adapter = NfsMetadataAdapter::new(
        protocol.clone(),
        BackendIdentity::new(BackendKind::Nfs, "not-started")?,
    );
    let path = StoragePath::new("directory")?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = adapter
        .apply(&path, MetadataMutation::Mode(0o600), cancel)
        .await;
    assert!(
        matches!(&result, Err(StorageRoleFailure::Entry(error))
        if error.operation() == Operation::Metadata && error.class() == FailureClass::Cancelled
            && error.transience() == Transience::Transient),
        "{result:?}"
    );
    assert_eq!(protocol.dropped.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn bound_directory_uses_one_fresh_resolution_and_all_writes_use_its_handle()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(PendingProtocol::default());
    let backend = BackendIdentity::new(BackendKind::Nfs, "bound-directory")?;
    let adapter = NfsMetadataAdapter::new(protocol.clone(), backend.clone());
    let path = StoragePath::new("parent/directory")?;
    let expected = SourceIdentity::new(
        backend,
        IdentityStrength::StableWithinBackend,
        b"current-directory",
    )?;
    let mutations = vec![
        MetadataMutation::Mode(0o751),
        MetadataMutation::Timestamps(TimestampMetadata {
            accessed: None,
            modified: Some(timestamp(1).ok_or("time")?),
            created: None,
        }),
    ];
    let result = adapter
        .apply_bound_directory(
            &path,
            expected.identity_key(),
            mutations.clone(),
            CancellationToken::new(),
        )
        .await;
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(protocol.lookups.load(Ordering::SeqCst), 1);
    let writes = protocol
        .bound_writes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(writes.len(), mutations.len());
    for ((handle, actual), expected) in writes.iter().zip(mutations.iter()) {
        assert_eq!(handle.as_ref(), b"current-directory");
        assert_eq!(actual, expected);
    }
    Ok(())
}

#[tokio::test]
async fn bound_directory_rejects_replacement_before_writing_including_empty_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(PendingProtocol::default());
    let backend = BackendIdentity::new(BackendKind::Nfs, "bound-replacement")?;
    let adapter = NfsMetadataAdapter::new(protocol.clone(), backend.clone());
    let path = StoragePath::new("dir")?;
    let expected = SourceIdentity::new(
        backend,
        IdentityStrength::StableWithinBackend,
        b"previous-directory",
    )?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let failure = adapter
        .apply_bound_directory(&path, expected.identity_key(), Vec::new(), cancel)
        .await
        .err()
        .ok_or("expected cancelled binding")?;
    assert!(
        matches!(failure.error, Some(StorageRoleFailure::Entry(error))
        if error.class() == FailureClass::Cancelled && error.transience() == Transience::Transient)
    );
    assert_eq!(protocol.lookups.load(Ordering::SeqCst), 0);

    for mutations in [vec![], vec![MetadataMutation::Mode(0o755)]] {
        let count = mutations.len();
        let result = adapter
            .apply_bound_directory(
                &path,
                expected.identity_key(),
                mutations,
                CancellationToken::new(),
            )
            .await;
        let failure = result.err().ok_or("expected identity conflict")?;
        assert_eq!(failure.failed_index, count);
        assert_eq!(failure.completed, 0);
        assert!(
            matches!(failure.error, Some(StorageRoleFailure::Entry(error))
            if error.path() == &path && error.class() == FailureClass::Conflict)
        );
    }
    assert_eq!(protocol.lookups.load(Ordering::SeqCst), 2);
    assert!(
        protocol
            .bound_writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn bound_directory_cancellation_reports_unknown_current_write_and_confirmed_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(PendingProtocol::default());
    let backend = BackendIdentity::new(BackendKind::Nfs, "bound-cancel")?;
    let adapter = NfsMetadataAdapter::new(protocol.clone(), backend.clone());
    let path = StoragePath::new("dir")?;
    let expected = SourceIdentity::new(
        backend,
        IdentityStrength::StableWithinBackend,
        b"current-directory",
    )?;
    let cancel = CancellationToken::new();
    let request = adapter.apply_bound_directory(
        &path,
        expected.identity_key(),
        vec![
            MetadataMutation::Mode(0o755),
            MetadataMutation::Mode(0),
            MetadataMutation::Mode(0o700),
        ],
        cancel.clone(),
    );
    tokio::pin!(request);
    tokio::select! {
        result = &mut request => panic!("请求未等待协议: {result:?}"),
        () = protocol.entered.notified() => {}
    }
    cancel.cancel();
    let failure = timeout(Duration::from_millis(200), &mut request)
        .await?
        .err()
        .ok_or("expected cancelled write")?;
    assert_eq!(failure.completed, 1);
    assert_eq!(failure.failed_index, 1);
    assert!(
        matches!(&failure.error, Some(StorageRoleFailure::Entry(error))
        if error.class() == FailureClass::Cancelled && error.transience() == Transience::Unknown
            && error.diagnostic().contains("unconfirmed")),
        "{failure:?}"
    );
    assert_eq!(protocol.lookups.load(Ordering::SeqCst), 1);
    assert_eq!(protocol.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        protocol
            .bound_writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        2
    );
    Ok(())
}
