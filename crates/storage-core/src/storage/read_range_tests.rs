use std::error::Error;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;

use super::*;
use crate::model::{BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity};
use crate::storage::SourceDescriptor;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct Source {
    descriptor: SourceDescriptor,
    input: Mutex<Option<ByteStream>>,
    requests: Mutex<Vec<ReadRequest>>,
}

#[async_trait]
impl ReadSource for Source {
    fn supports_source_versions(&self) -> bool {
        true
    }
    fn read_concurrency(&self) -> usize {
        3
    }
    fn maximum_read_chunk_bytes(&self) -> usize {
        4
    }
    async fn describe(&self, _: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        Ok(self.descriptor.clone())
    }
    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request);
        Ok(self
            .input
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| Box::pin(stream::empty())))
    }
}

fn source(size: Option<u64>, input: Vec<Result<Bytes, StorageRoleFailure>>) -> TestResult<Source> {
    let identity = SourceIdentity::new(
        BackendIdentity::new(BackendKind::S3, "basis-range")?,
        IdentityStrength::PathScoped,
        b"file",
    )?;
    let mut descriptor =
        SourceDescriptor::new(StoragePath::new("file")?, EntryKind::File, size, identity);
    descriptor.version = SourceVersion::Id("pinned-version".into());
    Ok(Source {
        descriptor,
        input: Mutex::new(Some(Box::pin(stream::iter(input)))),
        requests: Mutex::default(),
    })
}

async fn read(
    source: &Source,
    range: Option<Range<u64>>,
) -> Result<PositionedByteStream, StorageRoleFailure> {
    read_current_range(
        source,
        source.descriptor.path.clone(),
        range,
        NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
        CancellationToken::new(),
    )
    .await
}

#[tokio::test]
async fn basis_range_moves_buffers_and_pins_the_described_version() -> TestResult {
    let payload = Bytes::from(vec![1, 2, 3, 4]);
    let address = payload.as_ptr();
    let source = source(Some(10), vec![Ok(payload)])?;
    let mut input = read(&source, Some(3..7)).await?;
    let chunk = input.next().await.transpose()?.ok_or("missing chunk")?;
    assert_eq!(chunk.offset, 3);
    assert_eq!(chunk.data.as_ptr(), address);
    assert!(input.next().await.is_none());
    let requests = source
        .requests
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].range, Some(3..7));
    assert_eq!(requests[0].version, source.descriptor.version);
    assert_eq!(
        requests[0].expected_source.as_ref(),
        Some(&source.descriptor.source_identity)
    );
    assert_eq!(requests[0].maximum_chunk_bytes, 4);
    assert_eq!(requests[0].read_inflight, 3);
    Ok(())
}

#[tokio::test]
async fn basis_short_read_and_excess_payload_are_terminal_typed_errors() -> TestResult {
    for payload in [b"abc".as_slice(), b"abcde".as_slice()] {
        let source = source(Some(4), vec![Ok(Bytes::copy_from_slice(payload))])?;
        let results = read(&source, None).await?.collect::<Vec<_>>().await;
        let error = results
            .last()
            .and_then(|result| result.as_ref().err())
            .ok_or("missing error")?;
        let StorageRoleFailure::Entry(error) = error else {
            return Err("wrong error scope".into());
        };
        assert_eq!(error.operation(), Operation::Read);
        assert_eq!(error.class(), FailureClass::Protocol);
        assert_eq!(error.transience(), Transience::Permanent);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn empty_ranges_do_not_open_reads_and_invalid_ranges_fail_before_io() -> TestResult {
    let source = source(Some(4), Vec::new())?;
    assert!(read(&source, Some(2..2)).await?.next().await.is_none());
    for (start, end) in [(3, 2), (0, 5)] {
        assert!(read(&source, Some(start..end)).await.is_err());
    }
    assert!(
        source
            .requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn unknown_size_is_rejected_before_opening_a_read() -> TestResult {
    let source = source(None, Vec::new())?;
    assert!(read(&source, None).await.is_err());
    assert!(
        source
            .requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn input_errors_keep_the_original_backend_cause() -> TestResult {
    let cause = invalid(&StoragePath::new("file")?, "backend read refused");
    let source = source(Some(4), vec![Err(cause.clone())])?;
    let mut input = read(&source, None).await?;
    assert_eq!(input.next().await.transpose().err(), Some(cause));
    assert!(input.next().await.is_none());
    Ok(())
}
