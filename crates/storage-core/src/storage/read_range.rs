//! Bounded, identity-pinned reads for consumers that reconstruct an existing file.

use std::num::NonZeroUsize;
use std::ops::Range;

use futures::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

use super::{
    ByteStream, PositionedByteStream, PositionedChunk, ReadRequest, ReadSource, StorageRoleFailure,
};
use crate::model::{
    EntryOperationFailure, FailureClass, Operation, SourceVersion, StoragePath, Transience,
};

/// Describes the current object once and reads an exact ordered range from that pinned version.
///
/// `None` reads the complete object. Buffers move from the backend without copying; polling and
/// prefetch remain bounded by the backend's read window and `maximum_chunk_bytes`. No task or
/// channel is created. The stream retains the backend's drop and cancellation semantics;
/// cancellation does not replace its request timeout. The initial describe has no cancellation
/// parameter and is bounded by that backend timeout.
///
/// # Errors
/// Returns the original backend error, or a typed Read/Protocol/Permanent failure for an unknown
/// size, invalid range, excess payload, or short EOF. The observation is intentionally fresh:
/// this is for reading the current basis, not for validating an earlier source observation.
pub async fn read_current_range(
    source: &dyn ReadSource,
    path: StoragePath,
    range: Option<Range<u64>>,
    maximum_chunk_bytes: NonZeroUsize,
    cancel: CancellationToken,
) -> Result<PositionedByteStream, StorageRoleFailure> {
    let descriptor = source
        .describe_version(&path, &SourceVersion::Current)
        .await?;
    let size = descriptor
        .size
        .ok_or_else(|| invalid(&path, "unknown source size"))?;
    let range = range.unwrap_or(0..size);
    if range.start > range.end || range.end > size {
        return Err(invalid(&path, "source range outside object"));
    }
    if range.is_empty() {
        return Ok(Box::pin(stream::empty()));
    }
    let input = source
        .read(ReadRequest {
            path: path.clone(),
            range: Some(range.clone()),
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: source
                .maximum_read_chunk_bytes()
                .min(maximum_chunk_bytes.get())
                .max(1),
            read_inflight: source.read_concurrency(),
            read_budget: None,
            cancel,
            source_qos: None,
            version: descriptor.version,
        })
        .await?;
    Ok(exact_input(input, path, range))
}

fn exact_input(input: ByteStream, path: StoragePath, range: Range<u64>) -> PositionedByteStream {
    Box::pin(stream::unfold(
        Some((input, path, range.start, range.end)),
        |state| async {
            let (mut input, path, offset, end) = state?;
            match input.next().await {
                Some(Ok(data)) => {
                    let Some(next) = offset
                        .checked_add(data.len() as u64)
                        .filter(|next| *next <= end)
                    else {
                        return Some((
                            Err(invalid(&path, "source read exceeded requested range")),
                            None,
                        ));
                    };
                    Some((
                        Ok(PositionedChunk { offset, data }),
                        Some((input, path, next, end)),
                    ))
                }
                Some(Err(error)) => Some((Err(error), None)),
                None if offset != end => {
                    Some((Err(invalid(&path, "source returned a short read")), None))
                }
                None => None,
            }
        },
    ))
}

fn invalid(path: &StoragePath, message: &'static str) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Read,
            FailureClass::Protocol,
            Transience::Permanent,
            message,
        )
        .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
    )
}

#[cfg(test)]
#[path = "read_range_tests.rs"]
mod tests;
