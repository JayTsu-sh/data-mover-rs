use std::sync::Mutex;

use futures::StreamExt as _;

use super::*;
use crate::model::{BackendKind, FailureClass};

struct FakeProtocol {
    payload: Bytes,
    handle: Bytes,
    opened: Mutex<usize>,
}

struct FakeCursor(Bytes);

#[async_trait]
impl NfsReadCursor for FakeCursor {
    async fn read_at(&mut self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure> {
        let start = usize::try_from(offset).map_err(|_| NfsProtocolFailure::protocol())?;
        Ok(self.0.slice(start..start + count))
    }
}

#[async_trait]
impl NfsSourceProtocol for FakeProtocol {
    async fn describe(
        &self,
        _path: &StoragePath,
    ) -> Result<NfsSourceObservation, NfsProtocolFailure> {
        Ok(NfsSourceObservation {
            kind: EntryKind::File,
            size: Some(self.payload.len() as u64),
            file_handle: self.handle.clone(),
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
            Box::new(FakeCursor(self.payload.clone())),
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
        opened: Mutex::new(0),
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
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let mut total = 0;
    while let Some(chunk) = stream.next().await.transpose()? {
        assert!(chunk.len() <= usize::try_from(MAX_ROLE_READ)?);
        total += chunk.len();
    }
    assert_eq!(total, 1_500_000);
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
async fn opened_identity_detects_change_and_precancel_avoids_another_open()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from_static(b"payload"),
        handle: Bytes::from_static(b"current-handle"),
        opened: Mutex::new(0),
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
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
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
            cancel,
            source_qos: None,
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

#[test]
fn connectivity_is_session_scoped_not_entry_scoped() -> Result<(), Box<dyn std::error::Error>> {
    let failure = role_failure(
        &StoragePath::new("file")?,
        crate::model::Operation::Read,
        NfsProtocolFailure {
            class: FailureClass::Connectivity,
            transience: crate::model::Transience::Unknown,
        },
    );
    assert!(matches!(failure, StorageRoleFailure::Session(_)));
    Ok(())
}
