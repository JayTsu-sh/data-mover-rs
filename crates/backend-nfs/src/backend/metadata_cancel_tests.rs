use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{BackendIdentity, BackendKind, Operation};

struct PendingProtocol {
    entered: Notify,
    dropped: AtomicUsize,
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
        let protocol = Arc::new(PendingProtocol {
            entered: Notify::new(),
            dropped: AtomicUsize::new(0),
        });
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
    let protocol = Arc::new(PendingProtocol {
        entered: Notify::new(),
        dropped: AtomicUsize::new(0),
    });
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
