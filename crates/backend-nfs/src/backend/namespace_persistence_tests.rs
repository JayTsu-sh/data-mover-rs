//! Namespace 持久化通过实际 role 组合验证，无数据读取或写入的替代实现。
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

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
    BackendIdentity, BackendKind, EntryKind, FailureClass, IdentityStrength, OwnershipMode,
    SourceIdentity, StoragePath, StorageTimestamp, TimePrecision, TimestampMetadata, Transience,
};
use crate::storage::backends::nfs::source::NfsProtocolFailure;
use crate::storage::backends::nfs::staged::{NfsStageFile, NfsStagedProtocol};
use crate::storage::{InlineOwnership, Namespace, StorageRoleFailure};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type ProtocolResult<T> = std::result::Result<T, NfsProtocolFailure>;

#[derive(Default)]
struct State {
    generation: AtomicU64,
    fresh_stats: AtomicU64,
    opened: AtomicU64,
    committed: AtomicU64,
    closed: AtomicU64,
    replace_on_open: AtomicBool,
    replace_on_commit: AtomicBool,
    fail_commit: AtomicBool,
    stall_commit: AtomicBool,
    stall_close: AtomicBool,
    fail_close: AtomicBool,
    entered: Notify,
}

struct Protocol(Arc<State>);
struct File(Arc<State>);

fn unavailable() -> NfsProtocolFailure {
    NfsProtocolFailure::new(FailureClass::Unsupported, Transience::Permanent)
}

fn handle(state: &State) -> Bytes {
    Bytes::from(format!("fh:{}", state.generation.load(Ordering::Acquire)))
}

#[async_trait]
impl NfsStageFile for File {
    async fn read_at(&self, _: u64, _: usize) -> ProtocolResult<Bytes> {
        Err(unavailable())
    }
    async fn write_at(&self, _: u64, _: Bytes) -> ProtocolResult<u64> {
        Err(unavailable())
    }
    async fn set_len(&self, _: u64) -> ProtocolResult<()> {
        Err(unavailable())
    }
    async fn checkpoint(&self, whole_file: bool) -> ProtocolResult<()> {
        assert!(
            whole_file,
            "published file must issue an entire-file COMMIT"
        );
        self.0.committed.fetch_add(1, Ordering::AcqRel);
        self.0.entered.notify_one();
        if self.0.stall_commit.load(Ordering::Acquire) {
            futures::future::pending::<()>().await;
        }
        if self.0.replace_on_commit.load(Ordering::Acquire) {
            self.0.generation.fetch_add(1, Ordering::AcqRel);
        }
        if self.0.fail_commit.load(Ordering::Acquire) {
            return Err(
                NfsProtocolFailure::new(FailureClass::Timeout, Transience::Unknown)
                    .with_status("injected COMMIT failure"),
            );
        }
        Ok(())
    }
    async fn close(&self) -> ProtocolResult<()> {
        self.0.closed.fetch_add(1, Ordering::AcqRel);
        if self.0.stall_close.load(Ordering::Acquire) {
            futures::future::pending::<()>().await;
        }
        if self.0.fail_close.load(Ordering::Acquire) {
            return Err(
                NfsProtocolFailure::new(FailureClass::Connectivity, Transience::Unknown)
                    .with_status("injected CLOSE failure"),
            );
        }
        Ok(())
    }
}

#[async_trait]
impl NfsStagedProtocol for Protocol {
    fn read_inflight(&self) -> usize {
        1
    }
    fn write_inflight(&self) -> usize {
        1
    }
    async fn create_empty_open(
        &self,
        _: &StoragePath,
        _: u32,
    ) -> ProtocolResult<(Box<dyn NfsStageFile>, Bytes)> {
        Err(unavailable())
    }
    async fn open_read(&self, _: &StoragePath) -> ProtocolResult<(Box<dyn NfsStageFile>, Bytes)> {
        self.0.opened.fetch_add(1, Ordering::AcqRel);
        if self.0.replace_on_open.load(Ordering::Acquire) {
            self.0.generation.fetch_add(1, Ordering::AcqRel);
        }
        Ok((Box::new(File(self.0.clone())), handle(&self.0)))
    }
    async fn open_write(&self, _: &StoragePath) -> ProtocolResult<(Box<dyn NfsStageFile>, Bytes)> {
        Err(unavailable())
    }
    async fn size(&self, _: &StoragePath) -> ProtocolResult<u64> {
        Ok(3)
    }
    async fn rename(&self, _: &StoragePath, _: &StoragePath) -> ProtocolResult<()> {
        Err(unavailable())
    }
    async fn delete(&self, _: &StoragePath) -> ProtocolResult<()> {
        Err(unavailable())
    }
}

#[async_trait]
impl NfsNamespaceProtocol for Protocol {
    async fn stat(
        &self,
        path: &StoragePath,
        fresh: bool,
    ) -> ProtocolResult<NfsNamespaceObservation> {
        if fresh {
            self.0.fresh_stats.fetch_add(1, Ordering::AcqRel);
        }
        let time = Some(
            StorageTimestamp::new(123, TimePrecision::Nanoseconds).map_err(|_| unavailable())?,
        );
        Ok(NfsNamespaceObservation {
            path: path.clone(),
            kind: EntryKind::File,
            size: Some(3),
            file_handle: handle(&self.0),
            timestamps: Some(TimestampMetadata {
                accessed: time,
                modified: time,
                created: time,
            }),
            mode: Some(0o644),
            uid: Some(0),
            gid: Some(0),
            ownership: Some(InlineOwnership::Known(OwnershipMode {
                uid: 0,
                gid: 0,
                mode: 0o644,
            })),
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
    async fn delete(&self, _: &StoragePath, _: EntryKind) -> ProtocolResult<()> {
        Err(unavailable())
    }
    async fn rename(&self, _: &StoragePath, _: &StoragePath) -> ProtocolResult<()> {
        Err(unavailable())
    }
}

fn fixture() -> Result<(NfsNamespaceAdapter, Arc<State>, StoragePath)> {
    let state = Arc::new(State::default());
    let protocol = Arc::new(Protocol(state.clone()));
    let namespace = NfsNamespaceAdapter::new(
        protocol.clone(),
        BackendIdentity::new(BackendKind::Nfs, "persist")?,
        Some(protocol),
    );
    Ok((namespace, state, StoragePath::new("dir/file")?))
}

#[tokio::test]
async fn published_file_commits_with_fresh_binding_checks_without_reading_data() -> Result {
    let (namespace, state, path) = fixture()?;
    let expected = namespace.stat(&path).await?;
    namespace
        .persist_observed(&expected, CancellationToken::new())
        .await?;
    assert_eq!(state.fresh_stats.load(Ordering::Acquire), 2);
    assert_eq!(state.opened.load(Ordering::Acquire), 1);
    assert_eq!(state.committed.load(Ordering::Acquire), 1);
    assert_eq!(state.closed.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn stale_facts_and_missing_required_metadata_are_refused_before_open() -> Result {
    let (namespace, state, path) = fixture()?;
    let expected = namespace.stat(&path).await?;
    for mode in 0..5 {
        let mut changed = expected.clone();
        match mode {
            0 => changed.size = Some(4),
            1 => changed.kind = EntryKind::Directory,
            2 => changed.inline_timestamps = None,
            3 => {
                changed.source_identity = SourceIdentity::new(
                    BackendIdentity::new(BackendKind::Nfs, "other")?,
                    IdentityStrength::StableWithinBackend,
                    b"fh:0",
                )?;
            }
            _ => {
                changed.inline_ownership = Some(InlineOwnership::Known(OwnershipMode {
                    uid: 1,
                    gid: 0,
                    mode: 0o644,
                }));
            }
        }
        assert!(
            namespace
                .persist_observed(&changed, CancellationToken::new())
                .await
                .is_err()
        );
    }
    assert_eq!(state.opened.load(Ordering::Acquire), 0);
    assert_eq!(state.committed.load(Ordering::Acquire), 0);
    Ok(())
}

#[tokio::test]
async fn replacements_during_open_and_commit_cannot_confirm_persistence() -> Result {
    for at_open in [true, false] {
        let (namespace, state, path) = fixture()?;
        let expected = namespace.stat(&path).await?;
        if at_open {
            state.replace_on_open.store(true, Ordering::Release);
        } else {
            state.replace_on_commit.store(true, Ordering::Release);
        }
        let error = namespace
            .persist_observed(&expected, CancellationToken::new())
            .await
            .err()
            .ok_or("replacement accepted")?;
        assert!(
            matches!(error, StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict)
        );
        assert_eq!(state.committed.load(Ordering::Acquire), u64::from(!at_open));
        assert_eq!(state.closed.load(Ordering::Acquire), 1);
    }
    Ok(())
}

#[tokio::test]
async fn commit_and_close_failures_keep_both_diagnostics() -> Result {
    let (namespace, state, path) = fixture()?;
    let expected = namespace.stat(&path).await?;
    state.fail_commit.store(true, Ordering::Release);
    state.fail_close.store(true, Ordering::Release);
    let error = namespace
        .persist_observed(&expected, CancellationToken::new())
        .await
        .err()
        .ok_or("failure accepted")?;
    assert!(
        matches!(error, StorageRoleFailure::Session(error) if error.class() == FailureClass::Timeout && error.diagnostic().contains("injected COMMIT failure") && error.diagnostic().contains("injected CLOSE failure"))
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn failed_close_has_a_three_second_budget() -> Result {
    let (namespace, state, path) = fixture()?;
    let expected = namespace.stat(&path).await?;
    state.stall_close.store(true, Ordering::Release);
    let error = tokio::time::timeout(
        Duration::from_secs(4),
        namespace.persist_observed(&expected, CancellationToken::new()),
    )
    .await?
    .err()
    .ok_or("unknown close accepted")?;
    assert!(
        matches!(error, StorageRoleFailure::Session(error) if error.class() == FailureClass::Timeout && error.diagnostic().contains("close result is unknown"))
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_interrupts_inflight_commit_and_pre_cancel_opens_nothing() -> Result {
    let (namespace, state, path) = fixture()?;
    let expected = namespace.stat(&path).await?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(namespace.persist_observed(&expected, cancel).await.is_err());
    assert_eq!(state.opened.load(Ordering::Acquire), 0);
    state.stall_commit.store(true, Ordering::Release);
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let cancelling = async {
        state.entered.notified().await;
        signal.cancel();
    };
    let operation = namespace.persist_observed(&expected, cancel);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(operation, cancelling)
    })
    .await?;
    assert!(
        matches!(result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
    );
    assert_eq!(state.committed.load(Ordering::Acquire), 1);
    Ok(())
}
