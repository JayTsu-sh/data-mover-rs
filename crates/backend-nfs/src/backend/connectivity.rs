//! NFS 根路径不接受 stat；连接探测只读取第一小页并取消剩余枚举。
use std::num::NonZeroUsize;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::model::StoragePath;
use crate::storage::{ListRequest, Namespace, StorageRoleFailure};

pub(super) async fn check_root_listing(
    namespace: &dyn Namespace,
) -> Result<(), StorageRoleFailure> {
    let cancel = CancellationToken::new();
    let request = ListRequest::new(StoragePath::root(), cancel.clone())
        .with_page_hint(NonZeroUsize::MIN.saturating_add(15));
    let mut listing = namespace.list(request);
    let result = listing.next().await.transpose().map(|_| ());
    cancel.cancel();
    result
}

#[cfg(test)]
#[path = "connectivity_tests.rs"]
mod tests;
