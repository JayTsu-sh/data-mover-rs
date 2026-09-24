//! Ordered HDFS writes with durable in-place checkpoints on one open writer.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use futures::StreamExt as _;

use super::protocol::entry_failure;
use super::staged::{HdfsStagedDestination, expected_size};
use crate::model::{FailureClass, Operation, Transience};
use crate::storage::{
    ByteStream, PreparedStage, StagedDestination, StorageRoleFailure, WriteEvidence,
};

struct Input {
    stream: ByteStream,
    pending: Bytes,
    exhausted: bool,
}

pub(super) async fn write(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
    mut stream: ByteStream,
) -> Result<WriteEvidence, StorageRoleFailure> {
    let path = adapter.part(stage)?;
    let expected = expected_size(stage)?;
    // Opening/validating the source must precede destructive Direct creation.
    let first = stream.next().await.transpose()?;
    let mut input = Input {
        stream,
        pending: first.clone().unwrap_or_default(),
        exhausted: first.is_none(),
    };
    let mut writer = adapter
        .protocol
        .open_stage_writer(&path, stage.write_offset, stage.direct)
        .await?;
    let result = consume(adapter, stage, &mut *writer, &mut input, expected).await;
    // A normal input failure still closes and durably exposes the received prefix.
    // After process loss, the last hsync is the minimum durable prefix; HDFS lease
    // recovery may also retain a longer acknowledged tail.
    let closed = writer.close().await;
    let offset = result?;
    closed?;
    let observed = adapter.protocol.stat(&path).await?;
    if observed.size != Some(offset) {
        return Err(invalid(stage));
    }
    Ok(WriteEvidence {
        persisted_bytes: offset,
    })
}

async fn consume(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
    writer: &mut dyn super::protocol::HdfsWriteSession,
    input: &mut Input,
    expected: u64,
) -> Result<u64, StorageRoleFailure> {
    let maximum = adapter.protocol.maximum_write_chunk_bytes().max(1);
    let interval = stage
        .deferred_checkpoint
        .as_ref()
        .map(|value| value.interval_bytes);
    let mut offset = stage.write_offset;
    let mut next_checkpoint = interval.map(|step| offset.saturating_add(step).min(expected));

    while offset < expected {
        while input.pending.is_empty() {
            if input.exhausted {
                return Err(invalid(stage));
            }
            if let Some(next) = input.stream.next().await {
                input.pending = next?;
            } else {
                input.exhausted = true;
                return Err(invalid(stage));
            }
        }
        let checkpoint_remaining =
            next_checkpoint.map_or(u64::MAX, |checkpoint| checkpoint.saturating_sub(offset));
        let remaining = expected.saturating_sub(offset).min(checkpoint_remaining);
        let count = input
            .pending
            .len()
            .min(maximum)
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        if count == 0 {
            return Err(invalid(stage));
        }
        let data = input.pending.split_to(count);
        let written = writer.write(data).await?;
        if written != count {
            return Err(invalid(stage));
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or_else(|| invalid(stage))?;

        if next_checkpoint == Some(offset) && offset < expected {
            writer.hsync().await?;
            register_durable_prefix(adapter, stage, offset).await?;
            next_checkpoint = interval.map(|step| offset.saturating_add(step).min(expected));
        }
    }

    if !input.pending.is_empty() {
        return Err(invalid(stage));
    }
    if !input.exhausted {
        while let Some(extra) = input.stream.next().await {
            if !extra?.is_empty() {
                return Err(invalid(stage));
            }
        }
        input.exhausted = true;
    }
    Ok(offset)
}

async fn register_durable_prefix(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
    expected_prefix: u64,
) -> Result<(), StorageRoleFailure> {
    let observed = adapter.protocol.stat(&adapter.part(stage)?).await?;
    if observed.size != Some(expected_prefix) {
        return Err(invalid(stage));
    }
    if stage.at_destination {
        // The pointer is the whole recovery record; nothing registers where data-mover runs.
        super::at_destination::write_pointer(adapter, stage, expected_prefix, false).await?;
        stage.recovery_enabled.store(true, Ordering::Release);
        return Ok(());
    }
    if stage.recovery_enabled() {
        return Ok(());
    }
    let checkpoint = stage
        .deferred_checkpoint
        .as_ref()
        .ok_or_else(|| invalid(stage))?;
    checkpoint
        .registration
        .register(stage, adapter.recovery_identity(stage).await?)
        .await?;
    stage.recovery_enabled.store(true, Ordering::Release);
    Ok(())
}

fn invalid(stage: &PreparedStage) -> StorageRoleFailure {
    entry_failure(
        stage.final_destination.path(),
        Operation::Write,
        FailureClass::Corruption,
        Transience::Permanent,
    )
}
