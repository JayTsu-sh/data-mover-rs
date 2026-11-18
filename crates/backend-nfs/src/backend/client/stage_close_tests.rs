//! Exercises the actual stage ownership at the SDK seam with a gated CLOSE.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::{
    Attr, ExportEntry, FSInfo, FSStat, Mount, NFSVersion, Nfs4ErrorCode, NfsError, ObjRes,
    Pathconf, ReaddirStream, ReaddirplusStream, Result as NfsResult, Time, WriteOutcome,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::{
    DEFAULT_TRANSFER_CONCURRENCY, NFSFileHandle, NFSStorage, NfsRoleStageFile, NfsStageFile,
    StorageConfig,
};
use crate::model::FailureClass;

#[derive(Debug, Default)]
struct CloseObserver {
    calls: AtomicUsize,
    started: CancellationToken,
    release: CancellationToken,
    finished: CancellationToken,
    refuse: bool,
}

#[derive(Debug)]
struct CloseMount(Arc<CloseObserver>);

// Expand the entire impl before async_trait processes the otherwise unused SDK operations.
macro_rules! close_mount {
    ($(async fn $operation:ident(&self $(, $arg:ident: $kind:ty)*) -> $output:ty;)*) => {
        #[async_trait]
        impl Mount for CloseMount {
            fn get_max_read_size(&self) -> u32 {
                4096
            }

            fn get_max_write_size(&self) -> u32 {
                4096
            }

            fn version(&self) -> NFSVersion {
                NFSVersion::NFSv4p1
            }

            async fn getfh(&self) -> Bytes {
                Bytes::from_static(b"root")
            }

            async fn umount(&self) -> NfsResult<()> {
                Ok(())
            }

            async fn close(&self, fh: Bytes) -> NfsResult<()> {
                assert_eq!(fh.as_ref(), b"file");
                self.0.calls.fetch_add(1, Ordering::Relaxed);
                self.0.started.cancel();
                self.0.release.cancelled().await;
                self.0.finished.cancel();
                if self.0.refuse {
                    Err(NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_ACCESS))
                } else {
                    Ok(())
                }
            }

            $(async fn $operation(&self $(, $arg: $kind)*) -> $output {
                panic!("unexpected SDK operation {}", stringify!($operation))
            })*
        }
    };
}

close_mount! {
    async fn null(&self) -> NfsResult<()>;
    async fn access(&self, _fh: Bytes, _mode: u32) -> NfsResult<u32>;
    async fn commit(&self, _fh: Bytes, _offset: u64, _count: u32) -> NfsResult<()>;
    async fn create(&self, _dir: Bytes, _name: &str, _mode: Option<u32>) -> NfsResult<ObjRes>;
    async fn create_path(&self, _path: &str, _mode: Option<u32>) -> NfsResult<ObjRes>;
    async fn fsinfo(&self) -> NfsResult<FSInfo>;
    async fn fsstat(&self) -> NfsResult<FSStat>;
    async fn getattr(&self, _fh: Bytes) -> NfsResult<Attr>;
    async fn setattr(&self, _fh: Bytes, _guard: Option<Time>, _mode: Option<u32>, _uid: Option<u32>, _gid: Option<u32>, _size: Option<u64>, _atime: Option<Time>, _mtime: Option<Time>) -> NfsResult<()>;
    async fn setattr_path(&self, _path: &str, _guard: bool, _mode: Option<u32>, _uid: Option<u32>, _gid: Option<u32>, _size: Option<u64>, _atime: Option<Time>, _mtime: Option<Time>) -> NfsResult<()>;
    async fn link(&self, _source: Bytes, _dir: Bytes, _name: &str) -> NfsResult<Attr>;
    async fn link_path(&self, _source: &str, _target: &str) -> NfsResult<Attr>;
    async fn symlink(&self, _source: &str, _dir: Bytes, _name: &str) -> NfsResult<ObjRes>;
    async fn symlink_path(&self, _source: &str, _target: &str) -> NfsResult<ObjRes>;
    async fn readlink(&self, _fh: Bytes) -> NfsResult<String>;
    async fn lookup(&self, _dir: Bytes, _name: &str) -> NfsResult<ObjRes>;
    async fn lookup_path(&self, _path: &str) -> NfsResult<ObjRes>;
    async fn pathconf(&self, _fh: Bytes) -> NfsResult<Pathconf>;
    async fn read(&self, _fh: Bytes, _offset: u64, _count: u32) -> NfsResult<Bytes>;
    async fn write(&self, _fh: Bytes, _offset: u64, _data: Bytes) -> NfsResult<WriteOutcome>;
    async fn readdir(&self, _dir: Bytes) -> ReaddirStream<'_>;
    async fn readdirplus(&self, _dir: Bytes) -> ReaddirplusStream<'_>;
    async fn mkdir(&self, _dir: Bytes, _name: &str, _mode: u32) -> NfsResult<ObjRes>;
    async fn mkdir_path(&self, _path: &str, _mode: u32) -> NfsResult<ObjRes>;
    async fn remove(&self, _dir: Bytes, _name: &str) -> NfsResult<()>;
    async fn remove_path(&self, _path: &str) -> NfsResult<()>;
    async fn rmdir(&self, _dir: Bytes, _name: &str) -> NfsResult<()>;
    async fn rmdir_path(&self, _path: &str) -> NfsResult<()>;
    async fn rename(&self, _from_dir: Bytes, _from: &str, _to_dir: Bytes, _to: &str) -> NfsResult<()>;
    async fn rename_path(&self, _from: &str, _to: &str) -> NfsResult<()>;
    async fn exports(&self) -> NfsResult<Vec<ExportEntry>>;
}

fn staged_file(refuse: bool) -> (NfsRoleStageFile, Arc<CloseObserver>) {
    let observer = Arc::new(CloseObserver {
        refuse,
        ..CloseObserver::default()
    });
    let storage = NFSStorage {
        mount: Arc::new(Box::new(CloseMount(Arc::clone(&observer)))),
        root_fh: Bytes::from_static(b"root"),
        root: Arc::new(String::new()),
        config: StorageConfig {
            read_chunk_bytes: 4096,
            write_chunk_bytes: 4096,
            transfer_concurrency: DEFAULT_TRANSFER_CONCURRENCY,
        },
        refresh_generation: Arc::default(),
        refresh_lock: Arc::default(),
        server_id: 0,
        cache_root_fh: Bytes::new(),
    };
    let handle = NFSFileHandle::new(Bytes::from_static(b"file"), PathBuf::from("file"));
    (NfsRoleStageFile::new(storage, handle), observer)
}

#[tokio::test(start_paused = true)]
async fn stage_close_finishes_once_after_caller_drops_its_wait() {
    for commit in [false, true] {
        let (file, observer) = staged_file(false);
        let mut closing = if commit {
            file.close()
        } else {
            file.close_uncommitted()
        };
        tokio::select! {
            () = observer.started.cancelled() => {}
            _ = &mut closing => panic!("the gated SDK CLOSE completed early"),
        }
        drop(closing);
        let gate_stayed_held = file.deferred_gate.try_write().is_err();
        drop(file);
        observer.release.cancel();
        assert!(
            timeout(Duration::from_secs(1), observer.finished.cancelled())
                .await
                .is_ok(),
            "SDK CLOSE was lost after cancelling the caller wait"
        );
        assert!(gate_stayed_held);
        assert_eq!(observer.calls.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn stage_close_dropped_before_its_gate_leaves_handle_for_drop() {
    let (file, observer) = staged_file(false);
    let held_gate = file.deferred_gate.read().await;
    let mut closing = file.close_uncommitted();
    assert!(futures::poll!(&mut closing).is_pending());
    drop(closing);
    assert_eq!(observer.calls.load(Ordering::Relaxed), 0);
    drop(held_gate);
    drop(file);
    observer.release.cancel();
    assert!(
        timeout(Duration::from_secs(1), observer.finished.cancelled())
            .await
            .is_ok(),
        "the stage destructor did not close the still-owned handle"
    );
    assert_eq!(observer.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn concurrent_stage_close_waits_for_the_original_sdk_outcome() {
    let (file, observer) = staged_file(true);
    let mut first = file.close_uncommitted();
    tokio::select! {
        () = observer.started.cancelled() => {}
        _ = &mut first => panic!("the gated SDK CLOSE completed early"),
    }
    let mut second = file.close();
    assert!(
        futures::poll!(&mut second).is_pending(),
        "second close returned before the SDK CLOSE completed"
    );
    drop(first);

    observer.release.cancel();
    for result in [second.await, file.close_uncommitted().await] {
        let failure = result
            .err()
            .unwrap_or_else(|| panic!("SDK CLOSE was refused"));
        assert_eq!(failure.class, FailureClass::PermissionDenied);
        assert!(matches!(
            failure.cause.as_deref(),
            Some(NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_ACCESS))
        ));
    }
    drop(file);
    assert_eq!(observer.calls.load(Ordering::Relaxed), 1);
}
