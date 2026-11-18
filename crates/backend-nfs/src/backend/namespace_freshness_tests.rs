//! 相同路径被换名替换时，旧目录句柄仍存活，强观察必须按当前名字重解析。
#![allow(clippy::unwrap_used)]
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::DirectoryCookie;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{
    NfsListingPages, NfsNamespaceAdapter, NfsNamespaceObservation, NfsNamespaceProtocol,
    NfsReadEnd, NfsReadFailure,
};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, FailureClass, ObservedEntry, StoragePath,
    StorageTimestamp, TimePrecision, TimestampMetadata, Transience,
};
use crate::storage::backends::nfs::source::NfsProtocolFailure;
use crate::storage::{Namespace, SourceDescriptor, StorageRoleFailure};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ProtocolResult<T> = std::result::Result<T, NfsProtocolFailure>;

#[derive(Default)]
struct State {
    named_generation: AtomicU64,
    timestamp: AtomicU64,
    cached: Mutex<HashMap<StoragePath, u64>>,
    requests: Mutex<Vec<(StoragePath, bool)>>,
    failure: Mutex<Option<NfsProtocolFailure>>,
    stall: AtomicBool,
    entered: Notify,
}

struct Protocol(Arc<State>);

fn unavailable() -> NfsProtocolFailure {
    NfsProtocolFailure::new(FailureClass::Unsupported, Transience::Permanent)
}

#[async_trait]
impl NfsNamespaceProtocol for Protocol {
    async fn stat(
        &self,
        path: &StoragePath,
        fresh: bool,
    ) -> ProtocolResult<NfsNamespaceObservation> {
        self.0.requests.lock().unwrap().push((path.clone(), fresh));
        if fresh {
            self.0.entered.notify_one();
            if self.0.stall.load(Ordering::Acquire) {
                futures::future::pending::<()>().await;
            }
            if let Some(error) = self.0.failure.lock().unwrap().clone() {
                return Err(error);
            }
        }
        let generation = self.0.named_generation.load(Ordering::Acquire);
        let generation = if fresh {
            generation
        } else {
            *self
                .0
                .cached
                .lock()
                .unwrap()
                .entry(path.clone())
                .or_insert(generation)
        };
        let time = StorageTimestamp::new(
            i128::from(self.0.timestamp.load(Ordering::Acquire)),
            TimePrecision::Nanoseconds,
        )
        .map_err(|_| unavailable())?;
        Ok(NfsNamespaceObservation {
            path: path.clone(),
            kind: EntryKind::Directory,
            size: None,
            file_handle: Bytes::from(format!("{path:?}:generation:{generation}")),
            timestamps: Some(TimestampMetadata {
                accessed: None,
                modified: Some(time),
                created: Some(time),
            }),
            mode: Some(0o755),
            uid: None,
            gid: None,
            ownership: None,
        })
    }
    async fn directory_handle(&self, _: &StoragePath) -> ProtocolResult<Bytes> {
        Err(unavailable())
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
        Err(unavailable())
    }
    async fn read_link(&self, _: &StoragePath) -> ProtocolResult<Bytes> {
        Err(unavailable())
    }
    async fn create_directory(&self, _: &StoragePath) -> ProtocolResult<()> {
        Err(unavailable())
    }
    async fn delete(
        &self,
        _: &StoragePath,
        _: EntryKind,
        _: Option<&ObservedEntry>,
    ) -> ProtocolResult<()> {
        Err(unavailable())
    }
    async fn rename(&self, _: &StoragePath, _: &StoragePath) -> ProtocolResult<()> {
        Err(unavailable())
    }
}

fn fixture() -> Result<(NfsNamespaceAdapter, Arc<State>)> {
    let state = Arc::new(State::default());
    let identity = BackendIdentity::new(BackendKind::Nfs, "nfs-fresh-stat-test")?;
    Ok((
        NfsNamespaceAdapter::new(Arc::new(Protocol(state.clone())), identity, None),
        state,
    ))
}

// 普通调用可保留缓存；需要当前名字绑定的调用显式选择 fresh=true。
async fn current(
    namespace: &NfsNamespaceAdapter,
    path: &StoragePath,
) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
    namespace.stat(path, true).await
}

async fn replaced_directory(name: &str) -> Result {
    let (namespace, state) = fixture()?;
    let path = StoragePath::new(name)?;
    let prepared = namespace.stat(&path, false).await?;
    state.named_generation.store(1, Ordering::Release);
    let actual_name = current(&namespace, &path).await?;
    assert_ne!(
        prepared.source_identity.identity_key(),
        actual_name.source_identity.identity_key(),
        "strong read must not mistake a live renamed-away directory for the new name binding"
    );
    Ok(())
}

#[tokio::test]
async fn fresh_namespace_stat_replaces_cached_direct_directory_binding() -> Result {
    replaced_directory("dir").await
}

#[tokio::test]
async fn fresh_namespace_stat_replaces_cached_ancestor_directory_binding() -> Result {
    replaced_directory("ancestor/dir").await
}

#[tokio::test]
async fn fresh_namespace_stat_preserves_identity_when_only_change_time_changes() -> Result {
    let (namespace, state) = fixture()?;
    let path = StoragePath::new("dir")?;
    let before = current(&namespace, &path).await?;
    state.timestamp.store(42, Ordering::Release);
    let after = current(&namespace, &path).await?;
    assert_eq!(before.source_identity, after.source_identity);
    assert_ne!(before.inline_timestamps, after.inline_timestamps);
    Ok(())
}

#[tokio::test]
async fn ordinary_namespace_stat_retains_the_existing_cached_mode_without_fresh_calls() -> Result {
    let (namespace, state) = fixture()?;
    let path = StoragePath::new("dir")?;
    let before = namespace.stat(&path, false).await?;
    state.named_generation.store(1, Ordering::Release);
    let cached = namespace.stat(&path, false).await?;
    assert_eq!(before.source_identity, cached.source_identity);
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|(_, fresh)| !fresh));
    Ok(())
}

#[tokio::test]
async fn fresh_namespace_stat_never_falls_back_to_cached_success_after_lookup_failure() -> Result {
    let (namespace, state) = fixture()?;
    let path = StoragePath::new("dir")?;
    namespace.stat(&path, false).await?;
    *state.failure.lock().unwrap() = Some(NfsProtocolFailure::new(
        FailureClass::PermissionDenied,
        Transience::Permanent,
    ));
    assert!(
        matches!(current(&namespace, &path).await, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::PermissionDenied)
    );
    Ok(())
}

#[tokio::test]
async fn fresh_namespace_stat_respects_caller_cancellation_without_cached_success() -> Result {
    let (namespace, state) = fixture()?;
    let path = StoragePath::new("dir")?;
    namespace.stat(&path, false).await?;
    state.stall.store(true, Ordering::Release);
    let cancel = CancellationToken::new();
    let operation = async {
        tokio::select! {
            result = current(&namespace, &path) => Some(result),
            () = cancel.cancelled() => None,
        }
    };
    let stop = async {
        state.entered.notified().await;
        cancel.cancel();
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        tokio::join!(operation, stop)
    })
    .await?;
    assert!(
        result.is_none(),
        "cancellation must not return the old cached directory"
    );
    Ok(())
}
