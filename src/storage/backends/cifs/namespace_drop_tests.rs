//! A dropped listing never leaks the CIFS directory handle (P7 §5.1): fault injection against the
//! producer's close path.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::metadata::CifsInlineMetadata;
use super::namespace::{CifsNamespace, CifsNamespaceProtocol};
use super::source::CifsSourceFacts;
use crate::model::{BackendIdentity, BackendKind, StoragePath};
use crate::storage::{ListRequest, Namespace};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn path(value: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(value)?)
}

/// A listing protocol that opens a (fake) directory handle, waits at a gate while it is open, and
/// closes it before answering — the shape of `SmbDomainProtocol::list`.
#[derive(Default)]
struct GatedProtocol {
    open_handles: AtomicUsize,
    entered: Notify,
    gate: Notify,
}

#[async_trait]
impl CifsNamespaceProtocol for GatedProtocol {
    async fn stat(&self, _path: &StoragePath) -> smb_domain::Result<CifsSourceFacts> {
        Err(smb_domain::Error::InvalidArgument("unused".into()))
    }

    async fn list(
        &self,
        _path: &StoragePath,
    ) -> smb_domain::Result<Vec<(StoragePath, CifsInlineMetadata)>> {
        self.open_handles.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.gate.notified().await;
        // The close: skipped only if the listing future is dropped half way.
        self.open_handles.fetch_sub(1, Ordering::SeqCst);
        Ok(Vec::new())
    }

    async fn create_directory(&self, _path: &StoragePath) -> smb_domain::Result<()> {
        Ok(())
    }

    async fn remove(&self, _path: &StoragePath) -> smb_domain::Result<()> {
        Ok(())
    }

    async fn rename_entry(&self, _from: &StoragePath, _to: &StoragePath) -> smb_domain::Result<()> {
        Ok(())
    }
}

/// Fault injection for the no-leak contract (P7 §5.1): the consumer drops the listing while the
/// directory handle is open. The producer must still run the protocol call to its close and
/// then end; a stream that aborted its producer on drop would leave the handle open (and the
/// protocol referenced) and fail here.
#[tokio::test]
async fn dropping_a_listing_mid_request_still_closes_the_directory_handle() -> Result {
    let protocol = Arc::new(GatedProtocol::default());
    let namespace = CifsNamespace::new(
        Arc::clone(&protocol),
        BackendIdentity::new(BackendKind::Cifs, "cifs-drop-test")?,
    );
    let request = ListRequest::new(path("dir")?, CancellationToken::new());
    let mut stream = namespace.list(request);
    let entered = protocol.entered.notified();
    assert!(futures::poll!(stream.next()).is_pending());
    tokio::time::timeout(Duration::from_secs(5), entered).await?;
    assert_eq!(protocol.open_handles.load(Ordering::SeqCst), 1);

    drop(stream);
    drop(namespace);
    protocol.gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while protocol.open_handles.load(Ordering::SeqCst) != 0 || Arc::strong_count(&protocol) != 1
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .map_err(|_| {
        format!(
            "handle left open ({}) or producer still alive ({} references)",
            protocol.open_handles.load(Ordering::SeqCst),
            Arc::strong_count(&protocol)
        )
    })?;
    Ok(())
}
