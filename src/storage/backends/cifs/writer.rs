//! Concurrent positioned writes with a contiguous FLUSH barrier at each checkpoint.
use super::{
    checkpoint,
    source::{classify, entry_failure},
    staged::{CifsStageFile, CifsStagedDestination},
};
use crate::model::{FailureClass, Operation};
use crate::storage::{ByteStream, PreparedStage, StagedDestination, StorageRoleFailure};
use bytes::Bytes;
use futures::{StreamExt as _, stream::FuturesUnordered};

pub(super) async fn write(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
    file: &dyn CifsStageFile,
    mut input: ByteStream,
) -> Result<u64, StorageRoleFailure> {
    let path = stage.final_destination.path();
    let maximum = file.maximum_write_chunk() as usize;
    if maximum == 0 {
        return Err(entry_failure(
            path,
            Operation::Write,
            FailureClass::Protocol,
        ));
    }
    let depth = adapter.write_inflight;
    let mut writes = FuturesUnordered::new();
    let mut offset = stage.write_offset;
    let mut pending = Bytes::new();
    let mut done = false;
    let mut failure = None;
    let interval = stage.deferred_checkpoint.as_ref().map(|v| v.interval_bytes);
    let mut next = interval.map(|step| offset.saturating_add(step));
    loop {
        if failure.is_some() || (done && pending.is_empty()) {
            break;
        }
        if next == Some(offset)
            && stage
                .deferred_checkpoint
                .as_ref()
                .is_some_and(|v| offset < v.source_size)
        {
            while let Some(result) = writes.next().await {
                if let Err(error) = result {
                    failure.get_or_insert_with(|| classify(path, Operation::Write, &error));
                }
            }
            if failure.is_some() {
                break;
            }
            if let Err(error) = save(adapter, stage, file, offset).await {
                failure = Some(error);
                break;
            }
            next = interval.map(|step| offset.saturating_add(step));
        }
        if !pending.is_empty() && writes.len() < depth {
            let count = pending.len().min(maximum).min(
                usize::try_from(
                    next.filter(|v| *v > offset)
                        .map_or(u64::MAX, |v| v - offset),
                )
                .unwrap_or(usize::MAX),
            );
            let chunk = pending.split_to(count);
            let start = offset;
            let Some(end) = offset.checked_add(count as u64) else {
                failure = Some(entry_failure(
                    path,
                    Operation::Write,
                    FailureClass::InvalidInput,
                ));
                break;
            };
            offset = end;
            writes.push(file.write_all_at(start, chunk));
            continue;
        }
        tokio::select! {
            result = writes.next(), if !writes.is_empty() => {
                if let Some(Err(error)) = result { failure = Some(classify(path, Operation::Write, &error)); }
            }
            item = input.next(), if pending.is_empty() && !done && writes.len() < depth => {
                match item { Some(Ok(bytes)) => pending = bytes, Some(Err(error)) => failure = Some(error), None => done = true }
            }
        }
    }
    // Never close while already-issued writes may still be completing.
    while let Some(result) = writes.next().await {
        if let Err(error) = result {
            failure.get_or_insert_with(|| classify(path, Operation::Write, &error));
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    if stage.durable_publication {
        file.flush()
            .await
            .map_err(|e| classify(path, Operation::Write, &e))?;
    }
    if stage.recovery_enabled() {
        checkpoint::persist(adapter, stage, offset).await?;
    }
    Ok(offset)
}

pub(super) async fn save(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
    file: &dyn CifsStageFile,
    offset: u64,
) -> Result<(), StorageRoleFailure> {
    file.flush()
        .await
        .map_err(|e| classify(stage.final_destination.path(), Operation::Write, &e))?;
    checkpoint::persist(adapter, stage, offset).await?;
    if !stage.recovery_enabled() {
        let deferred = stage.deferred_checkpoint.as_ref().ok_or_else(|| {
            entry_failure(
                stage.final_destination.path(),
                Operation::Write,
                FailureClass::Internal,
            )
        })?;
        deferred
            .registration
            .register(stage, adapter.recovery_identity(stage).await?)
            .await?;
        stage
            .recovery_enabled
            .store(true, std::sync::atomic::Ordering::Release);
    }
    Ok(())
}
