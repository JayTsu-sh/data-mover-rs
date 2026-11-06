//! Sequential positioned input for an expert destination, including replayed recovery prefixes.
use std::sync::{Arc, Mutex, PoisonError};

use blake3::Hasher;
use bytes::Buf;
use futures::{StreamExt, stream};

use super::{ExpertDestinationSession, ExpertDestinationTransferred, TransferFailure};
use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use crate::storage::{ByteStream, PositionedByteStream, StorageRoleFailure};

/// Facts about the complete logical input, including bytes already present at the destination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpertInputProgress {
    pub bytes: u64,
    pub blake3: [u8; 32],
    pub ended: bool,
}

/// A positioned write always reports input facts, even when the backend rejects the write.
#[must_use = "the write outcome still owns verification, publication, or cleanup"]
pub struct ExpertDestinationWrite {
    pub progress: ExpertInputProgress,
    pub outcome: Result<ExpertDestinationTransferred, TransferFailure>,
}

#[derive(Default)]
struct InputFacts {
    bytes: u64,
    hash: Hasher,
    ended: bool,
}

impl ExpertDestinationSession {
    /// Accepts contiguous input from offset zero, hashes it all, and writes only after the
    /// destination's recovered prefix. Input buffers move into the backend stream; replayed
    /// prefixes are skipped by advancing the owned Bytes view. The caller still owns input backpressure.
    ///
    /// This does not publish. The returned outcome owns verification/publication or cleanup;
    /// progress distinguishes a short completed input from a write that stopped early.
    pub async fn write_positioned(self, input: PositionedByteStream) -> ExpertDestinationWrite {
        let facts = Arc::new(Mutex::new(InputFacts::default()));
        let incoming = positioned_bytes(input, Arc::clone(&facts), self.write_offset());
        let outcome = self.write(incoming).await;
        let facts = facts.lock().unwrap_or_else(PoisonError::into_inner);
        ExpertDestinationWrite {
            progress: ExpertInputProgress {
                bytes: facts.bytes,
                blake3: *facts.hash.finalize().as_bytes(),
                ended: facts.ended,
            },
            outcome,
        }
    }
}

fn positioned_bytes(
    input: PositionedByteStream,
    facts: Arc<Mutex<InputFacts>>,
    resume: u64,
) -> ByteStream {
    Box::pin(stream::unfold(
        (input, false),
        move |(mut input, failed)| {
            let facts = Arc::clone(&facts);
            async move {
                if failed {
                    return None;
                }
                let result = loop {
                    let chunk = match input.next().await {
                        Some(Ok(chunk)) => chunk,
                        Some(Err(error)) => break Err(error),
                        None => {
                            facts.lock().unwrap_or_else(PoisonError::into_inner).ended = true;
                            return None;
                        }
                    };
                    let mut facts = facts.lock().unwrap_or_else(PoisonError::into_inner);
                    if chunk.offset != facts.bytes {
                        break Err(input_failure("non-contiguous payload"));
                    }
                    let Some(end) = facts.bytes.checked_add(chunk.data.len() as u64) else {
                        break Err(input_failure("positioned payload length overflow"));
                    };
                    facts.bytes = end;
                    facts.hash.update(&chunk.data);
                    let skip = usize::try_from(resume.saturating_sub(chunk.offset))
                        .unwrap_or(usize::MAX)
                        .min(chunk.data.len());
                    if skip < chunk.data.len() {
                        let mut data = chunk.data;
                        data.advance(skip);
                        break Ok(data);
                    }
                };
                let failed = result.is_err();
                Some((result, (input, failed)))
            }
        },
    ))
}

fn input_failure(message: &'static str) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            StoragePath::root(),
            Operation::Write,
            FailureClass::Protocol,
            Transience::Permanent,
            message,
        )
        .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
    )
}

#[cfg(test)]
#[path = "expert_input_tests.rs"]
mod tests;
