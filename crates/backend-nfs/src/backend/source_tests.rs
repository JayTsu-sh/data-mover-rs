use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::namespace::{NfsUndescribedChild, undescribed};
use super::super::protocol::classify_error;
use super::*;
use crate::model::{BackendKind, FailureClass, SourceVersion};

#[test]
fn sdk_readlink_decode_reason_reaches_entry_failure() -> Result<(), Box<dyn std::error::Error>> {
    let path = StoragePath::new("link")?;
    let failed = classify_error(nfs_rs::NfsError::Xdr(
        "READLINK target is not UTF-8: invalid utf-8 sequence of 1 bytes from index 0".into(),
    ));
    assert!(
        failed.status.is_none(),
        "a decode error is not a server status"
    );
    let StorageRoleFailure::Entry(error) = role_failure(&path, Operation::Observe, failed) else {
        panic!("READLINK decoding remains entry scoped");
    };
    assert_eq!(error.class(), FailureClass::Protocol);
    assert_eq!(error.transience(), Transience::Unknown);
    assert!(
        error.diagnostic().contains("READLINK target is not UTF-8"),
        "{}",
        error.diagnostic()
    );
    Ok(())
}

#[test]
fn sdk_failure_keeps_shared_typed_cause() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<NfsProtocolFailure>();
    let failure = classify_error(nfs_rs::NfsError::Xdr("READLINK target is not UTF-8".into()));
    let cloned = failure.clone();
    let (Some(cause), Some(shared)) = (&failure.cause, &cloned.cause) else {
        panic!("classified SDK failures keep their cause");
    };
    assert!(Arc::ptr_eq(cause, shared));
    assert!(matches!(cause.as_ref(), nfs_rs::NfsError::Xdr(reason) if reason.contains("READLINK")));
    assert!(NfsProtocolFailure::protocol().cause.is_none());
}

#[test]
fn sdk_listing_decode_keeps_specific_cause_and_entry_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let path = StoragePath::new("dir/link")?;
    let detail = "READLINK target is not UTF-8";
    let failure = undescribed(NfsUndescribedChild {
        path: path.clone(),
        failure: classify_error(nfs_rs::NfsError::Xdr(detail.into())),
        reason: "the child attributes could not be decoded",
        raw_name: None,
    });
    assert_eq!(failure.path(), &path);
    assert_eq!(failure.operation(), Operation::Traverse);
    assert_eq!(failure.class(), FailureClass::Protocol);
    assert_eq!(failure.transience(), Transience::Unknown);
    assert!(
        failure
            .diagnostic()
            .contains("child attributes could not be decoded")
    );
    assert!(
        failure.diagnostic().contains(detail),
        "{}",
        failure.diagnostic()
    );
    Ok(())
}

#[test]
fn sdk_listing_keeps_transport_and_server_causes_entry_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    for (source, class, transience, detail) in [
        (
            nfs_rs::NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "child GETATTR timed out",
            )),
            FailureClass::Timeout,
            Transience::Transient,
            "child GETATTR timed out",
        ),
        (
            nfs_rs::NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_ACCES),
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "NFS3ERR_ACCES",
        ),
    ] {
        let failure = undescribed(NfsUndescribedChild {
            path: StoragePath::new("dir/child")?,
            failure: classify_error(source),
            reason: "the child attributes could not be read",
            raw_name: None,
        });
        assert_eq!(failure.operation(), Operation::Traverse);
        assert_eq!(failure.class(), class);
        assert_eq!(failure.transience(), transience);
        assert!(failure.diagnostic().contains(detail));
    }
    Ok(())
}

#[test]
fn sdk_transport_reason_reaches_session_failure() -> Result<(), Box<dyn std::error::Error>> {
    let path = StoragePath::new("file")?;
    let failure = classify_error(nfs_rs::NfsError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "peer reset while reading response header",
    )));
    let StorageRoleFailure::Session(error) = role_failure(&path, Operation::Read, failure) else {
        panic!("connection reset is session scoped");
    };
    assert_eq!(error.class(), FailureClass::Connectivity);
    assert_eq!(error.transience(), Transience::Transient);
    assert!(
        error
            .diagnostic()
            .contains("peer reset while reading response header")
    );
    Ok(())
}

#[test]
fn sdk_failure_diagnostic_is_bounded_redacted_and_nul_free()
-> Result<(), Box<dyn std::error::Error>> {
    let path = StoragePath::new("file")?;
    for detail in [
        "READLINK nfs://alice:p/a#ss@server/export and smb://bob:other-secret@host/share"
            .to_owned(),
        format!(
            "READLINK nfs://alice:{}@server/export",
            "secret".repeat(2000)
        ),
        format!("READLINK invalid UTF-8\0{}", "错误🦀".repeat(2000)),
    ] {
        let failure = classify_error(nfs_rs::NfsError::Xdr(detail));
        let StorageRoleFailure::Entry(error) = role_failure(&path, Operation::Observe, failure)
        else {
            panic!("decode failures remain entry scoped");
        };
        let text = error.diagnostic();
        assert!(text.starts_with("NFS role failed"));
        assert!(text.contains("READLINK"));
        assert!(text.len() <= MAX_DIAGNOSTIC_BYTES);
        assert!(!text.contains('\0'));
        for secret in ["alice", "p/a#ss", "bob", "other-secret", "secret"] {
            assert!(!text.contains(secret), "leaked {secret}: {text}");
        }
    }
    Ok(())
}

#[test]
fn invalid_status_and_long_parent_context_keep_a_valid_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let path = StoragePath::new(format!("dir/{}", "长".repeat(4000)))?;
    let parent = StoragePath::new("dir")?;
    let mut failure =
        NfsProtocolFailure::new(FailureClass::PermissionDenied, Transience::Permanent);
    failure.status = Some(Arc::from(format!("NFS4ERR_PERM\0{}", "拒绝".repeat(4000))));
    for entry in [failure.clone(), failure.at_level(parent.clone())] {
        let StorageRoleFailure::Entry(error) = role_failure(&path, Operation::Prepare, entry)
        else {
            panic!("permission errors remain entry scoped");
        };
        assert_eq!(error.class(), FailureClass::PermissionDenied);
        assert_eq!(error.transience(), Transience::Permanent);
        assert!(!error.diagnostic().contains('\0'));
        assert!(error.diagnostic().len() <= MAX_DIAGNOSTIC_BYTES);
        assert!(error.diagnostic().contains("NFS4ERR_PERM"));
    }
    let StorageRoleFailure::Entry(error) = entry_scoped(
        &StoragePath::new("dir/leaf")?,
        Operation::Prepare,
        classify_error(nfs_rs::NfsError::Xdr("READLINK target is not UTF-8".into()))
            .at_level(parent.clone()),
    ) else {
        panic!("parent failures remain entry scoped");
    };
    assert_eq!(error.path(), &parent);
    assert!(error.diagnostic().contains("READLINK target is not UTF-8"));
    Ok(())
}

struct FakeProtocol {
    payload: Bytes,
    handle: Bytes,
    maximum_read_chunk_bytes: usize,
    content_version: Option<Bytes>,
    described: AtomicUsize,
    opened: Mutex<usize>,
    active_reads: Arc<AtomicUsize>,
    maximum_active_reads: Arc<AtomicUsize>,
    read_started: Arc<Mutex<Vec<tokio::time::Instant>>>,
}

struct FakeCursor {
    payload: Bytes,
    active_reads: Arc<AtomicUsize>,
    maximum_active_reads: Arc<AtomicUsize>,
    read_started: Arc<Mutex<Vec<tokio::time::Instant>>>,
}

#[async_trait]
impl NfsReadCursor for FakeCursor {
    async fn read_at(&self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure> {
        self.read_started
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(tokio::time::Instant::now());
        let active = self.active_reads.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum_active_reads
            .fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let start = usize::try_from(offset).map_err(|_| NfsProtocolFailure::protocol())?;
        let result = self.payload.slice(start..start + count);
        self.active_reads.fetch_sub(1, Ordering::SeqCst);
        Ok(result)
    }
}

#[async_trait]
impl NfsSourceProtocol for FakeProtocol {
    fn maximum_read_chunk_bytes(&self) -> usize {
        self.maximum_read_chunk_bytes
    }

    async fn describe(
        &self,
        _path: &StoragePath,
    ) -> Result<NfsSourceObservation, NfsProtocolFailure> {
        self.described.fetch_add(1, Ordering::SeqCst);
        Ok(NfsSourceObservation {
            uid: None,
            gid: None,
            timestamps: None,
            ownership: None,
            mode: None,
            kind: EntryKind::File,
            size: Some(self.payload.len() as u64),
            file_handle: self.handle.clone(),
            content_version: self.content_version.clone(),
        })
    }

    async fn open(
        &self,
        _path: &StoragePath,
    ) -> Result<(Box<dyn NfsReadCursor>, Bytes), NfsProtocolFailure> {
        *self
            .opened
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        Ok((
            Box::new(FakeCursor {
                payload: self.payload.clone(),
                active_reads: Arc::clone(&self.active_reads),
                maximum_active_reads: Arc::clone(&self.maximum_active_reads),
                read_started: Arc::clone(&self.read_started),
            }),
            self.handle.clone(),
        ))
    }
}

fn adapter(protocol: Arc<FakeProtocol>) -> NfsReadSourceAdapter {
    NfsReadSourceAdapter::with_protocol(
        protocol,
        BackendIdentity::new(BackendKind::Nfs, "test-export")
            .unwrap_or_else(|error| panic!("{error}")),
    )
}

#[tokio::test]
async fn one_stream_uses_one_open_cursor_and_exact_ranges() -> Result<(), Box<dyn std::error::Error>>
{
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from(vec![7; 2 * 1024 * 1024 + 3]),
        handle: Bytes::from_static(b"stable-file-handle"),
        maximum_read_chunk_bytes: 1024 * 1024,
        content_version: Some(Bytes::from_static(b"version-1")),
        described: AtomicUsize::new(0),
        opened: Mutex::new(0),
        active_reads: Arc::new(AtomicUsize::new(0)),
        maximum_active_reads: Arc::new(AtomicUsize::new(0)),
        read_started: Arc::new(Mutex::new(Vec::new())),
    });
    let source = adapter(Arc::clone(&protocol));
    let path = StoragePath::new("large.bin")?;
    let descriptor = source.describe(&path).await?;
    let mut stream = source
        .read(ReadRequest {
            path,
            range: Some(1..1_500_001),
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?;
    let mut total = 0;
    while let Some(chunk) = stream.next().await.transpose()? {
        assert!(chunk.len() <= MAX_ROLE_READ);
        total += chunk.len();
    }
    assert_eq!(total, 1_500_000);
    assert_eq!(
        protocol.described.load(Ordering::SeqCst),
        1,
        "an explicit range already carries the size boundary and must not re-describe"
    );
    assert_eq!(
        *protocol
            .opened
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        1
    );
    Ok(())
}

#[tokio::test]
async fn shared_budget_bounds_nfs_prefetch_without_serializing_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from(vec![7; 4 * 1024 * 1024]),
        handle: Bytes::from_static(b"stable-file-handle"),
        maximum_read_chunk_bytes: 1024 * 1024,
        content_version: Some(Bytes::from_static(b"version-1")),
        described: AtomicUsize::new(0),
        opened: Mutex::new(0),
        active_reads: Arc::new(AtomicUsize::new(0)),
        maximum_active_reads: Arc::new(AtomicUsize::new(0)),
        read_started: Arc::new(Mutex::new(Vec::new())),
    });
    let source = adapter(Arc::clone(&protocol));
    let path = StoragePath::new("large.bin")?;
    let descriptor = source.describe(&path).await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    let (runtime, mut ordered) = crate::storage::InflightRuntime::channel(
        crate::storage::InflightConfig::new(128, 2 * 1024 * 1024, 128)?,
        0,
        protocol.payload.len() as u64,
        cancel.clone(),
    )?;
    let budget = crate::storage::ReadBudget::new(runtime.clone());
    let mut stream = source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 128,
            read_budget: Some(budget.clone()),
            cancel,
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?;
    let mut offset = 0;
    while let Some(bytes) = stream.next().await.transpose()? {
        let length = bytes.len() as u64;
        runtime
            .complete_read(
                budget.take(offset).ok_or("missing reservation")?,
                offset,
                bytes,
            )
            .await?;
        assert!(ordered.next().await.transpose()?.is_some());
        offset += length;
    }
    assert_eq!(offset, protocol.payload.len() as u64);
    assert_eq!(protocol.maximum_active_reads.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn opened_identity_detects_change_and_precancel_avoids_another_open()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from_static(b"payload"),
        handle: Bytes::from_static(b"current-handle"),
        maximum_read_chunk_bytes: 1024 * 1024,
        content_version: Some(Bytes::from_static(b"version-1")),
        described: AtomicUsize::new(0),
        opened: Mutex::new(0),
        active_reads: Arc::new(AtomicUsize::new(0)),
        maximum_active_reads: Arc::new(AtomicUsize::new(0)),
        read_started: Arc::new(Mutex::new(Vec::new())),
    });
    let source = adapter(Arc::clone(&protocol));
    let path = StoragePath::new("file")?;
    let other = SourceIdentity::new(
        BackendIdentity::new(BackendKind::Nfs, "test-export")?,
        IdentityStrength::StableWithinBackend,
        b"old-handle",
    )?;
    let changed = source
        .read(ReadRequest {
            path: path.clone(),
            range: None,
            expected_source: Some(other),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await;
    assert!(
        matches!(changed, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
    );

    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let cancelled = source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel,
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await;
    assert!(
        matches!(cancelled, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
    );
    assert_eq!(
        *protocol
            .opened
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        1
    );
    Ok(())
}

#[tokio::test]
async fn negotiated_protocol_limit_prevents_internal_read_resplitting()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from_static(b"abcdefgh"),
        handle: Bytes::from_static(b"stable-file-handle"),
        maximum_read_chunk_bytes: 3,
        content_version: Some(Bytes::from_static(b"version-1")),
        described: AtomicUsize::new(0),
        opened: Mutex::new(0),
        active_reads: Arc::new(AtomicUsize::new(0)),
        maximum_active_reads: Arc::new(AtomicUsize::new(0)),
        read_started: Arc::new(Mutex::new(Vec::new())),
    });
    let source = adapter(protocol);
    assert_eq!(source.maximum_read_chunk_bytes(), 3);
    let path = StoragePath::new("file")?;
    let descriptor = source.describe(&path).await?;
    let mut stream = source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 8,
            read_inflight: 4,
            read_budget: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?;
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await.transpose()? {
        chunks.push(chunk.len());
    }
    assert_eq!(chunks, vec![3, 3, 2]);
    Ok(())
}

#[test]
fn connectivity_is_session_scoped_not_entry_scoped() -> Result<(), Box<dyn std::error::Error>> {
    let failure = role_failure(
        &StoragePath::new("file")?,
        crate::model::Operation::Read,
        NfsProtocolFailure::new(
            FailureClass::Connectivity,
            crate::model::Transience::Unknown,
        ),
    );
    assert!(matches!(failure, StorageRoleFailure::Session(_)));
    Ok(())
}

/// A failure at a missing parent level the operation had to create is reported at that level;
/// the diagnostic names the operation's own entry and the server's status. A level that is the
/// entry itself, or a lost session, is reported as before.
#[test]
fn a_failed_missing_parent_is_reported_at_its_level() -> Result<(), Box<dyn std::error::Error>> {
    let entry = StoragePath::new("ro/missing/sub/f")?;
    let refused = NfsProtocolFailure::new(FailureClass::PermissionDenied, Transience::Permanent)
        .with_status("NFS3ERR_ACCES");
    let at_level = refused.clone().at_level(StoragePath::new("ro/missing")?);
    let StorageRoleFailure::Entry(failure) =
        role_failure(&entry, crate::model::Operation::Prepare, at_level)
    else {
        panic!("a refused level is entry-scoped");
    };
    assert_eq!(failure.path().as_str(), "ro/missing");
    assert_eq!(failure.operation(), crate::model::Operation::Prepare);
    assert_eq!(failure.class(), FailureClass::PermissionDenied);
    assert_eq!(failure.transience(), Transience::Permanent);
    assert_eq!(
        failure.diagnostic(),
        "NFS role failed creating a missing parent directory of `ro/missing/sub/f` (Prepare): \
         \"NFS3ERR_ACCES\" (NFS3ERR_ACCES)"
    );

    let itself = refused.at_level(entry.clone());
    let StorageRoleFailure::Entry(failure) =
        role_failure(&entry, crate::model::Operation::Prepare, itself)
    else {
        panic!("a refused entry is entry-scoped");
    };
    assert_eq!(failure.path(), &entry);
    assert!(failure.diagnostic().starts_with("NFS role failed: "));

    let lost = NfsProtocolFailure::new(FailureClass::Connectivity, Transience::Transient)
        .at_level(StoragePath::new("ro")?);
    assert!(matches!(
        role_failure(&entry, crate::model::Operation::Prepare, lost),
        StorageRoleFailure::Session(_)
    ));
    Ok(())
}

struct DelayedPrefixCursor;
#[async_trait]
impl NfsReadCursor for DelayedPrefixCursor {
    async fn read_at(&self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure> {
        tokio::time::sleep(std::time::Duration::from_millis(if offset == 0 {
            25
        } else {
            1
        }))
        .await;
        let offset = usize::try_from(offset).map_err(|_| {
            NfsProtocolFailure::new(FailureClass::InvalidInput, Transience::Permanent)
        })?;
        Ok(Bytes::from_static(b"abcdefghijkl").slice(offset..offset + count))
    }
}

#[tokio::test]
async fn positioned_nfs_read_does_not_wait_for_delayed_first_range()
-> Result<(), Box<dyn std::error::Error>> {
    let state = ReadState {
        cursor: Arc::new(DelayedPrefixCursor),
        path: StoragePath::new("source")?,
        next_issue: 0,
        next_emit: 0,
        end: 12,
        maximum_chunk_bytes: 4,
        read_concurrency: 3,
        inflight: Either::Right(FuturesUnordered::new()),
        cancel: tokio_util::sync::CancellationToken::new(),
        qos: None,
        budget: None,
    };
    let mut input = Box::pin(futures::stream::try_unfold(state, read_next));
    let first = input.next().await.ok_or("missing first read")??;
    assert_ne!(first.offset, 0);
    let mut chunks = vec![first];
    while let Some(chunk) = input.next().await.transpose()? {
        chunks.push(chunk);
    }
    chunks.sort_by_key(|chunk| chunk.offset);
    assert_eq!(
        chunks
            .into_iter()
            .flat_map(|chunk| chunk.data.to_vec())
            .collect::<Vec<_>>(),
        b"abcdefghijkl"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn qos_paces_actual_cursor_calls_instead_of_queued_futures()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from_static(b"abcdefghijklmnop"),
        handle: Bytes::from_static(b"handle"),
        maximum_read_chunk_bytes: 4,
        content_version: Some(Bytes::from_static(b"version-1")),
        described: AtomicUsize::new(0),
        opened: Mutex::new(0),
        active_reads: Arc::new(AtomicUsize::new(0)),
        maximum_active_reads: Arc::new(AtomicUsize::new(0)),
        read_started: Arc::new(Mutex::new(Vec::new())),
    });
    let source = adapter(protocol.clone());
    let budget = crate::storage::SourceQosGroup::new(crate::storage::SourceQosPolicy::new(
        None,
        4,
        Some((2, 2, std::time::Duration::ZERO)),
    )?)
    .transfer_budget();
    let mut stream = source
        .read(ReadRequest {
            path: StoragePath::new("file")?,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 4,
            read_inflight: 4,
            read_budget: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: Some(budget.clone()),
            version: SourceVersion::Current,
        })
        .await?;
    while stream.next().await.transpose()?.is_some() {}
    let calls = protocol
        .read_started
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(calls.len(), 4);
    for pair in calls.windows(2) {
        assert!(pair[1].duration_since(pair[0]) >= std::time::Duration::from_millis(500));
    }
    assert_eq!(budget.stats().source_read_operations, 4);
    assert_eq!(budget.stats().client_streamed_shaped_bytes, 16);
    Ok(())
}

#[tokio::test]
async fn absent_metadata_and_content_evidence_do_not_block_payload_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from_static(b"payload without optional metadata"),
        handle: Bytes::from_static(b"stable-file-handle"),
        maximum_read_chunk_bytes: 1024,
        content_version: None,
        described: AtomicUsize::new(0),
        opened: Mutex::new(0),
        active_reads: Arc::new(AtomicUsize::new(0)),
        maximum_active_reads: Arc::new(AtomicUsize::new(0)),
        read_started: Arc::new(Mutex::new(Vec::new())),
    });
    let source = adapter(protocol.clone());
    let path = StoragePath::new("file")?;
    let descriptor = source.describe(&path).await?;
    assert!(descriptor.content_version.is_none());
    assert!(descriptor.inline_timestamps.is_none());
    assert!(descriptor.inline_ownership.is_none());
    let mut stream = source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024,
            read_inflight: 1,
            read_budget: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?;
    let mut actual = Vec::new();
    while let Some(chunk) = stream.next().await.transpose()? {
        actual.extend_from_slice(&chunk);
    }
    assert_eq!(actual, protocol.payload);
    assert_eq!(protocol.active_reads.load(Ordering::SeqCst), 0);
    Ok(())
}
