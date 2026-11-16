//! Production strong lookup against a controllable three-operation SDK boundary.
#![allow(clippy::unwrap_used)]
use std::path::PathBuf;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::{Attr, Nfs3ErrorCode, NfsError, ObjRes};
use tokio_util::sync::CancellationToken;

use super::namespace_lookup::{Calls, lookup};
use super::{GLOBAL_CACHE, NFSStorage};
use crate::model::{FailureClass, StoragePath};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Script {
    generation: AtomicU64,
    root_state: AtomicUsize,
    missing_stage: AtomicUsize,
    denied_stage: AtomicUsize,
    stall_stage: AtomicUsize,
    delay_ms: AtomicU64,
    without_attrs: AtomicBool,
    error_kind: AtomicUsize,
    calls: Mutex<Vec<String>>,
    scope: Bytes,
}

impl Script {
    fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            root_state: AtomicUsize::new(0),
            missing_stage: AtomicUsize::new(usize::MAX),
            denied_stage: AtomicUsize::new(usize::MAX),
            stall_stage: AtomicUsize::new(usize::MAX),
            delay_ms: AtomicU64::new(0),
            without_attrs: AtomicBool::new(false),
            error_kind: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
            scope: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        }
    }
    fn key(&self, path: &str) -> (PathBuf, Bytes) {
        (PathBuf::from(path), self.scope.clone())
    }
    fn warm(&self) {
        NFSStorage::cache_directory(self.key("a"), &Bytes::from_static(b"a:0"));
        NFSStorage::cache_directory(self.key("a/b"), &Bytes::from_static(b"b:0"));
        NFSStorage::cache_directory(self.key("a/b/old-child"), &Bytes::from_static(b"old-child"));
        NFSStorage::cache_directory(self.key("neighbor"), &Bytes::from_static(b"neighbor"));
    }
    async fn stage(&self, stage: usize) -> nfs_rs::Result<()> {
        if self.stall_stage.load(Ordering::Acquire) == stage {
            futures::future::pending::<()>().await;
        }
        let delay = self.delay_ms.load(Ordering::Acquire);
        if delay != 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        if self.denied_stage.load(Ordering::Acquire) == stage {
            return Err(match self.error_kind.load(Ordering::Acquire) {
                1 => NfsError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
                2 => NfsError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut)),
                _ => NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_ACCES),
            });
        }
        if self.missing_stage.load(Ordering::Acquire) == stage {
            return Err(NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_NOENT));
        }
        Ok(())
    }
    fn object(&self, handle: Bytes, file: bool) -> ObjRes {
        ObjRes {
            fh: handle,
            attr: (!self.without_attrs.load(Ordering::Acquire)).then_some(Attr {
                type_: if file { 1 } else { 2 },
                file_mode: 0o640,
                ..Attr::default()
            }),
        }
    }
    async fn strong(
        &self,
        path: &StoragePath,
    ) -> std::result::Result<(Bytes, Attr), super::NfsProtocolFailure> {
        lookup(
            self,
            "configured",
            &Bytes::from_static(b"bound"),
            &self.scope,
            path,
        )
        .await
    }
    async fn cached_file(&self) -> nfs_rs::Result<ObjRes> {
        let parent = GLOBAL_CACHE.get(&self.key("a/b")).unwrap();
        self.child(parent, "file").await
    }
}

#[async_trait]
impl Calls for Script {
    async fn root(&self, path: &str) -> nfs_rs::Result<ObjRes> {
        self.calls.lock().unwrap().push(format!("root:{path}"));
        self.stage(0).await?;
        match self.root_state.load(Ordering::Acquire) {
            1 => return Ok(self.object(Bytes::from_static(b"replacement-root"), false)),
            2 => return Err(NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_NOENT)),
            _ => {}
        }
        // The old full-path algorithm remains executable for the pre-fix red run.
        if path.trim_matches('/') == "configured" {
            return Ok(self.object(Bytes::from_static(b"bound"), false));
        }
        let generation = self.generation.load(Ordering::Acquire);
        let name = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
        Ok(self.object(Bytes::from(format!("{name}:{generation}")), name == "file"))
    }
    async fn child(&self, parent: Bytes, name: &str) -> nfs_rs::Result<ObjRes> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("child:{}:{name}", String::from_utf8_lossy(&parent)));
        let stage = match name {
            "a" => 1,
            "b" => 2,
            _ => 3,
        };
        self.stage(stage).await?;
        let generation = if parent == b"bound"[..] {
            self.generation.load(Ordering::Acquire)
        } else {
            String::from_utf8_lossy(&parent)
                .rsplit(':')
                .next()
                .unwrap()
                .parse::<u64>()
                .unwrap()
        };
        Ok(self.object(Bytes::from(format!("{name}:{generation}")), name == "file"))
    }
    async fn attrs(&self, handle: Bytes) -> nfs_rs::Result<Attr> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("attrs:{}", String::from_utf8_lossy(&handle)));
        self.stage(4).await?;
        Ok(Attr {
            type_: if handle.starts_with(b"file:") { 1 } else { 2 },
            ..Attr::default()
        })
    }
}

#[tokio::test]
async fn strong_chain_rebuilds_warm_ancestors_before_cached_file_metadata() -> Result {
    let calls = Script::new();
    calls.warm();
    calls.generation.store(1, Ordering::Release);
    let (file, _) = calls
        .strong(&StoragePath::new("a/b/file")?)
        .await
        .unwrap_or_else(|error| panic!("{:?}", error.class));
    assert_eq!(file, Bytes::from_static(b"file:1"));
    assert_eq!(
        calls.cached_file().await?.fh,
        file,
        "bound metadata must no longer see the renamed-away parent"
    );
    assert!(GLOBAL_CACHE.get(&calls.key("a/b/old-child")).is_none());
    assert_eq!(
        GLOBAL_CACHE.get(&calls.key("neighbor")),
        Some(Bytes::from_static(b"neighbor"))
    );
    Ok(())
}

#[tokio::test]
async fn strong_chain_refuses_replaced_or_missing_root_even_for_nonroot_paths() -> Result {
    for root_state in [1, 2] {
        let calls = Script::new();
        calls.warm();
        calls.root_state.store(root_state, Ordering::Release);
        let error = calls
            .strong(&StoragePath::new("a/b/file")?)
            .await
            .unwrap_err();
        assert_eq!(error.class, FailureClass::Conflict);
        assert!(GLOBAL_CACHE.get(&calls.key("a")).is_none());
        assert!(
            calls
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|call| !call.starts_with("child:"))
        );
    }
    Ok(())
}

#[tokio::test]
async fn strong_chain_child_absence_invalidates_only_that_old_branch() -> Result {
    let calls = Script::new();
    calls.warm();
    calls.missing_stage.store(2, Ordering::Release);
    let error = calls
        .strong(&StoragePath::new("a/b/file")?)
        .await
        .unwrap_err();
    assert_eq!(error.class, FailureClass::NotFound);
    assert!(GLOBAL_CACHE.get(&calls.key("a/b")).is_none());
    assert!(GLOBAL_CACHE.get(&calls.key("a/b/old-child")).is_none());
    assert_eq!(
        GLOBAL_CACHE.get(&calls.key("a")),
        Some(Bytes::from_static(b"a:0"))
    );
    assert_eq!(
        GLOBAL_CACHE.get(&calls.key("neighbor")),
        Some(Bytes::from_static(b"neighbor"))
    );
    Ok(())
}

#[tokio::test]
async fn strong_chain_preserves_root_and_each_level_permission_failure() -> Result {
    for stage in 0..=3 {
        let calls = Script::new();
        calls.warm();
        calls.denied_stage.store(stage, Ordering::Release);
        let error = calls
            .strong(&StoragePath::new("a/b/file")?)
            .await
            .unwrap_err();
        assert_eq!(error.class, FailureClass::PermissionDenied);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn strong_chain_root_and_each_level_cancel_share_the_callers_budget() -> Result {
    for stage in 0..=3 {
        let calls = Script::new();
        calls.warm();
        calls.stall_stage.store(stage, Ordering::Release);
        let cancel = CancellationToken::new();
        let cancel_later = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            cancel_later.cancel();
        });
        let path = StoragePath::new("a/b/file")?;
        let result = tokio::select! { biased; () = cancel.cancelled() => None, result = calls.strong(&path) => Some(result) };
        assert!(
            result.is_none(),
            "pending stage must not fall back to the warm cache"
        );
    }
    let calls = Script::new();
    calls.delay_ms.store(100, Ordering::Release);
    let path = StoragePath::new("a/b/file")?;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), calls.strong(&path))
            .await
            .is_err(),
        "root and child lookups must share one outer deadline"
    );
    Ok(())
}

#[tokio::test]
async fn strong_chain_uses_canonical_root_once_then_bound_handle_and_keeps_unchanged_children()
-> Result {
    let calls = Script::new();
    calls.warm();
    lookup(
        &calls,
        "/configured/./",
        &Bytes::from_static(b"bound"),
        &calls.scope,
        &StoragePath::new("a/b")?,
    )
    .await
    .unwrap_or_else(|error| panic!("{:?}", error.class));
    assert_eq!(
        *calls.calls.lock().unwrap(),
        ["root:configured", "child:bound:a", "child:a:0:b"]
    );
    assert_eq!(
        GLOBAL_CACHE.get(&calls.key("a/b/old-child")),
        Some(Bytes::from_static(b"old-child"))
    );
    calls.calls.lock().unwrap().clear();
    calls
        .strong(&StoragePath::root())
        .await
        .unwrap_or_else(|error| panic!("{:?}", error.class));
    assert_eq!(*calls.calls.lock().unwrap(), ["root:configured"]);
    Ok(())
}

#[path = "namespace_lookup_helper_tests.rs"]
mod helper_tests;

#[tokio::test]
async fn strong_chain_preserves_network_causes_at_root_and_each_relative_lookup() -> Result {
    for (kind, class) in [(1, FailureClass::Connectivity), (2, FailureClass::Timeout)] {
        for stage in 0..=3 {
            let calls = Script::new();
            calls.warm();
            calls.denied_stage.store(stage, Ordering::Release);
            calls.error_kind.store(kind, Ordering::Release);
            let error = calls
                .strong(&StoragePath::new("a/b/file")?)
                .await
                .unwrap_err();
            assert_eq!(error.class, class);
        }
    }
    Ok(())
}

#[tokio::test]
async fn strong_chain_getattr_fallback_keeps_root_absence_distinct_from_child_absence() -> Result {
    for root in [true, false] {
        let calls = Script::new();
        calls.warm();
        calls.without_attrs.store(true, Ordering::Release);
        calls.missing_stage.store(4, Ordering::Release);
        let path = if root {
            StoragePath::root()
        } else {
            StoragePath::new("a/b/file")?
        };
        let error = calls.strong(&path).await.unwrap_err();
        assert_eq!(
            error.class,
            if root {
                FailureClass::Conflict
            } else {
                FailureClass::NotFound
            }
        );
        assert!(GLOBAL_CACHE.get(&calls.key("a")).is_none());
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn strong_chain_getattr_fallback_preserves_success_failure_and_cancel() -> Result {
    let calls = Script::new();
    calls.warm();
    calls.without_attrs.store(true, Ordering::Release);
    calls.generation.store(1, Ordering::Release);
    let path = StoragePath::new("a/b/file")?;
    let (handle, attrs) = calls
        .strong(&path)
        .await
        .unwrap_or_else(|error| panic!("{:?}", error.class));
    assert_eq!(handle, calls.cached_file().await?.fh);
    assert_eq!(attrs.type_, 1);
    assert_eq!(
        calls
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.starts_with("attrs:"))
            .count(),
        3
    );
    calls.denied_stage.store(4, Ordering::Release);
    let error = calls.strong(&path).await.unwrap_err();
    assert_eq!(error.class, FailureClass::PermissionDenied);
    calls.denied_stage.store(usize::MAX, Ordering::Release);
    calls.stall_stage.store(4, Ordering::Release);
    assert!(
        tokio::time::timeout(Duration::from_millis(250), calls.strong(&path))
            .await
            .is_err()
    );
    Ok(())
}
