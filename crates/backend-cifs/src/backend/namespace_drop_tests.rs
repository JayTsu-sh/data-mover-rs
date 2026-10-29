//! A dropped listing never leaks the CIFS directory handle (P7 §5.1): fault injection against the
//! producer's close path.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::namespace::{CifsDirectory, CifsListedChild, CifsNamespace, CifsNamespaceProtocol};
use super::source::CifsSourceFacts;
use crate::model::{BackendIdentity, BackendKind, StoragePath};
use crate::storage::{ListRequest, Namespace};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn path(value: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(value)?)
}

/// A listing protocol whose (fake) directory handle waits at a gate for its first record: the
/// consumer drops the listing while a `QUERY_DIRECTORY` is in flight on an open handle.
#[derive(Default)]
struct GatedProtocol {
    state: Arc<Gated>,
}

#[derive(Default)]
struct Gated {
    open_handles: AtomicUsize,
    entered: Notify,
    gate: Notify,
}

struct GatedDirectory {
    state: Arc<Gated>,
}

#[async_trait]
impl CifsDirectory for GatedDirectory {
    fn entries(&self) -> BoxStream<'_, smb_domain::Result<CifsListedChild>> {
        stream::once(async move {
            self.state.entered.notify_one();
            self.state.gate.notified().await;
        })
        .filter_map(|()| async { None })
        .boxed()
    }

    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        // The close: skipped only if the listing's producer is dropped half way.
        self.state.open_handles.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl CifsNamespaceProtocol for GatedProtocol {
    async fn stat(&self, _path: &StoragePath) -> smb_domain::Result<CifsSourceFacts> {
        Err(smb_domain::Error::InvalidArgument("unused".into()))
    }

    async fn open_directory(
        &self,
        _path: &StoragePath,
    ) -> smb_domain::Result<Box<dyn CifsDirectory>> {
        self.state.open_handles.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(GatedDirectory {
            state: Arc::clone(&self.state),
        }))
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
/// directory handle is open and a record is in flight. The producer must let that record arrive,
/// close the handle and then end; a stream that aborted its producer on drop would leave the handle open (and the
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
    let state = Arc::clone(&protocol.state);
    let entered = state.entered.notified();
    assert!(futures::poll!(stream.next()).is_pending());
    tokio::time::timeout(Duration::from_secs(5), entered).await?;
    assert_eq!(state.open_handles.load(Ordering::SeqCst), 1);

    drop(stream);
    drop(namespace);
    state.gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.open_handles.load(Ordering::SeqCst) != 0 || Arc::strong_count(&protocol) != 1 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .map_err(|_| {
        format!(
            "handle left open ({}) or producer still alive ({} references)",
            state.open_handles.load(Ordering::SeqCst),
            Arc::strong_count(&protocol)
        )
    })?;
    Ok(())
}
