#![allow(clippy::unwrap_used)]

use super::*;
use crate::model::{EntryOperationFailure, FailureClass, Operation, Transience};
use crate::storage::{ListingPage, ListingStream, SourceDescriptor};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

struct NfsProbeNamespace {
    pages: Vec<std::result::Result<ListingPage, StorageRoleFailure>>,
    cancel: Mutex<Option<CancellationToken>>,
}

fn denied(class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            StoragePath::root(),
            Operation::Namespace,
            class,
            Transience::Permanent,
            "NFS role failed",
        )
        .unwrap(),
    )
}

#[async_trait]
impl Namespace for NfsProbeNamespace {
    // 复现当前 NFS 角色：根目录可以枚举，但 stat 不接受根路径。
    async fn stat(
        &self,
        _: &StoragePath,
    ) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        Err(denied(FailureClass::InvalidInput))
    }

    fn list(&self, request: ListRequest) -> ListingStream {
        assert_eq!(request.path, StoragePath::root());
        assert!(request.page_hint.get() <= 16);
        *self.cancel.lock().unwrap() = Some(request.cancel);
        ListingStream::from_pages(request.path, self.pages.clone())
    }
}
#[tokio::test]
async fn nfs_connectivity_accepts_readable_root_and_stops_after_first_page() -> TestResult {
    let namespace = Arc::new(NfsProbeNamespace {
        pages: vec![
            Ok(ListingPage::default()),
            Err(denied(FailureClass::Protocol)),
        ],
        cancel: Mutex::new(None),
    });
    check_root_listing(namespace.as_ref()).await?;
    assert!(
        namespace
            .cancel
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_cancelled()
    );
    Ok(())
}

#[tokio::test]
async fn nfs_connectivity_accepts_an_empty_directory() -> TestResult {
    let namespace = Arc::new(NfsProbeNamespace {
        pages: vec![],
        cancel: Mutex::new(None),
    });
    check_root_listing(namespace.as_ref()).await?;
    Ok(())
}

#[tokio::test]
async fn nfs_connectivity_preserves_real_listing_failure() -> TestResult {
    for class in [
        FailureClass::PermissionDenied,
        FailureClass::Connectivity,
        FailureClass::NotFound,
    ] {
        let failure = denied(class);
        let namespace = Arc::new(NfsProbeNamespace {
            pages: vec![Err(failure.clone())],
            cancel: Mutex::new(None),
        });
        let result = check_root_listing(namespace.as_ref()).await;
        assert!(matches!(result, Err(actual) if actual == failure));
        assert!(
            namespace
                .cancel
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_cancelled()
        );
    }
    Ok(())
}
