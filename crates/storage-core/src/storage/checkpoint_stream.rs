//! Bounded conversion for checkpointed destinations that still require ordered appends.

use std::collections::BTreeMap;

use bytes::Bytes;
use futures::StreamExt as _;

use super::{ByteStream, PositionedByteStream, PreparedStage, StorageRoleFailure};
use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};

#[must_use]
pub fn failure(
    path: &StoragePath,
    class: FailureClass,
    message: &'static str,
) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Write,
            class,
            Transience::Permanent,
            message,
        )
        .unwrap_or_else(|_| unreachable!("static checkpoint diagnostic")),
    )
}

struct Ordered {
    input: PositionedByteStream,
    ready: BTreeMap<u64, Bytes>,
    next: u64,
    end: u64,
    buffered: usize,
    limit: usize,
    path: StoragePath,
}

pub fn ordered(prepared: &PreparedStage, input: PositionedByteStream) -> ByteStream {
    let state = Ordered {
        input,
        ready: BTreeMap::new(),
        next: prepared.write_offset,
        end: prepared.checkpoint_size(),
        buffered: 0,
        limit: prepared.checkpoint_window_bytes(),
        path: prepared.final_destination.path().clone(),
    };
    Box::pin(futures::stream::try_unfold(state, |mut state| async move {
        loop {
            if let Some(data) = state.ready.remove(&state.next) {
                state.next += data.len() as u64;
                state.buffered -= data.len();
                return Ok(Some((data, state)));
            }
            let Some(chunk) = state.input.next().await else {
                return if state.next == state.end && state.ready.is_empty() {
                    Ok(None)
                } else {
                    Err(failure(
                        &state.path,
                        FailureClass::Corruption,
                        "checkpoint stream has a missing range",
                    ))
                };
            };
            let chunk = chunk?;
            let end = chunk
                .offset
                .checked_add(chunk.data.len() as u64)
                .ok_or_else(|| {
                    failure(
                        &state.path,
                        FailureClass::Corruption,
                        "checkpoint offset overflow",
                    )
                })?;
            if chunk.data.is_empty()
                || chunk.offset < state.next
                || end > state.end
                || state
                    .ready
                    .range(..end)
                    .next_back()
                    .is_some_and(|(&offset, data)| offset + data.len() as u64 > chunk.offset)
            {
                return Err(failure(
                    &state.path,
                    FailureClass::Corruption,
                    "checkpoint stream has overlapping or invalid ranges",
                ));
            }
            state.buffered = state
                .buffered
                .checked_add(chunk.data.len())
                .filter(|value| *value <= state.limit)
                .ok_or_else(|| {
                    failure(
                        &state.path,
                        FailureClass::Corruption,
                        "checkpoint reorder window exceeded",
                    )
                })?;
            state.ready.insert(chunk.offset, chunk.data);
        }
    }))
}
