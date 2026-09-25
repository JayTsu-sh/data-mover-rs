//! The streamed source producer, exact-range source reads, and inflight failure mapping.

use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;

use super::failure::source_failure;
use super::{TransferFailure, TransferPhase, TransferSide};
use crate::model::{FailureClass, SourceIdentity, SourceVersion, StoragePath};
use crate::runtime::inflight::{InflightFailure, InflightRuntime, OrderedChunks, ReadRange};
use crate::storage::{ReadRequest, ReadSource, SourceQosBudget, StorageRoleFailure};

pub(super) struct ProducerRequest {
    pub(super) source: Arc<dyn ReadSource>,
    pub(super) path: StoragePath,
    pub(super) source_identity: SourceIdentity,
    /// The version the describe pinned; every read of this producer reads it.
    pub(super) version: SourceVersion,
    pub(super) cancel: tokio_util::sync::CancellationToken,
    pub(super) runtime: InflightRuntime,
    pub(super) failure: Arc<Mutex<Option<StorageRoleFailure>>>,
    pub(super) size: u64,
    pub(super) write_start: u64,
    pub(super) source_qos: Option<SourceQosBudget>,
    pub(super) hash_content: bool,
}

async fn open_producer_stream(
    request: &ProducerRequest,
    read_start: u64,
) -> Result<
    (
        crate::storage::ByteStream,
        crate::storage::ReadBudget,
        Option<crate::storage::ReadAdmission>,
    ),
    TransferFailure,
> {
    let budgeted = request.source.supports_read_budget();
    let budget = crate::storage::ReadBudget::new(request.runtime.clone());
    let read_inflight = if budgeted {
        request.runtime.read_depth()
    } else {
        1
    };
    let maximum_chunk_bytes = request.runtime.negotiated_chunk_ceiling();
    let serial_admission = if budgeted {
        None
    } else {
        request
            .runtime
            .reserve_read(maximum_chunk_bytes, true)
            .await
            .map_err(|error| inflight_transfer_failure(&error, &request.path))?
    };
    let stream = request
        .source
        .read(ReadRequest {
            path: request.path.clone(),
            range: Some(read_start..request.size),
            expected_source: Some(request.source_identity.clone()),
            maximum_chunk_bytes,
            read_inflight,
            read_budget: budgeted.then(|| budget.clone()),
            cancel: request.cancel.clone(),
            source_qos: request.source_qos.clone(),
            version: request.version.clone(),
        })
        .await;
    let stream = match stream {
        Ok(stream) => stream,
        Err(error) => return fail_source_producer(request, error).await,
    };
    Ok((stream, budget, serial_admission))
}

pub(super) async fn produce(request: ProducerRequest) -> Result<Option<[u8; 32]>, TransferFailure> {
    let mut hasher = request.hash_content.then(blake3::Hasher::new);
    let read_start = if request.hash_content {
        0
    } else {
        request.write_start
    };
    let budgeted = request.source.supports_read_budget();
    let maximum_chunk_bytes = request.runtime.negotiated_chunk_ceiling();
    let (mut stream, budget, mut serial_admission) =
        open_producer_stream(&request, read_start).await?;
    let mut offset = read_start;
    loop {
        let admission = if budgeted {
            None
        } else if let Some(admission) = serial_admission.take() {
            Some(admission)
        } else {
            request
                .runtime
                .reserve_read(maximum_chunk_bytes, true)
                .await
                .map_err(|error| inflight_transfer_failure(&error, &request.path))?
        };
        let Some(item) = stream.next().await else {
            break;
        };
        let bytes = match item {
            Ok(bytes) if !bytes.is_empty() => bytes,
            Ok(_) => {
                return Err(TransferFailure::orchestration(
                    TransferPhase::Transfer,
                    "source emitted an empty chunk",
                ));
            }
            Err(error) => return fail_source_producer(&request, error).await,
        };
        let admission = if budgeted {
            budget.take(offset)
        } else {
            admission
        }
        .ok_or_else(|| {
            TransferFailure::orchestration(TransferPhase::Transfer, "source omitted read admission")
        })?;
        let next = offset.checked_add(bytes.len() as u64).ok_or_else(|| {
            TransferFailure::orchestration(TransferPhase::Transfer, "source offset overflowed")
        })?;
        if next > request.size {
            return Err(TransferFailure::orchestration(
                TransferPhase::Transfer,
                "source emitted more bytes than described",
            ));
        }
        if let Some(hasher) = &mut hasher {
            hasher.update(&bytes);
        }
        if next > request.write_start {
            let skip =
                usize::try_from(request.write_start.saturating_sub(offset)).map_err(|_| {
                    TransferFailure::orchestration(
                        TransferPhase::Transfer,
                        "recovery prefix exceeds addressable range",
                    )
                })?;
            let output = bytes.slice(skip..);
            let output_offset = offset + skip as u64;
            request
                .runtime
                .complete_read(admission, output_offset, output)
                .await
                .map_err(|error| inflight_transfer_failure(&error, &request.path))?;
        }
        offset = next;
    }
    if offset != request.size {
        return Err(TransferFailure::orchestration(
            TransferPhase::Transfer,
            "source emitted fewer bytes than described",
        ));
    }
    Ok(hasher.map(|hasher| *hasher.finalize().as_bytes()))
}

async fn fail_source_producer<T>(
    request: &ProducerRequest,
    error: StorageRoleFailure,
) -> Result<T, TransferFailure> {
    *request
        .failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
    let _ = request.runtime.fail(InflightFailure::Upstream).await;
    Err(TransferFailure::role(
        TransferPhase::Transfer,
        TransferSide::Source,
        error,
    ))
}

pub(super) async fn read_exact_range(
    source: &dyn ReadSource,
    path: &StoragePath,
    source_identity: &SourceIdentity,
    version: &SourceVersion,
    cancel: &tokio_util::sync::CancellationToken,
    source_qos: Option<SourceQosBudget>,
    range: ReadRange,
) -> Result<Bytes, StorageRoleFailure> {
    let mut stream = source
        .read(ReadRequest {
            path: path.clone(),
            range: Some(range.offset..range.offset + range.length as u64),
            expected_source: Some(source_identity.clone()),
            maximum_chunk_bytes: range.length,
            read_inflight: 1,
            read_budget: None,
            cancel: cancel.clone(),
            source_qos,
            version: version.clone(),
        })
        .await?;
    let mut output = BytesMut::with_capacity(range.length);
    while let Some(item) = stream.next().await {
        let item = item?;
        let remaining = range.length - output.len();
        if item.len() > remaining {
            return Err(source_failure(path, FailureClass::Corruption));
        }
        output.extend_from_slice(&item);
    }
    if output.len() != range.length {
        return Err(source_failure(path, FailureClass::Corruption));
    }
    Ok(output.freeze())
}

pub(super) fn ordered_stream(
    ordered: OrderedChunks,
    failure: Arc<Mutex<Option<StorageRoleFailure>>>,
    path: StoragePath,
) -> crate::storage::ByteStream {
    Box::pin(futures::stream::unfold(ordered, move |mut ordered| {
        let failure = Arc::clone(&failure);
        let path = path.clone();
        async move {
            let item = ordered.next().await?;
            let item = item.map_err(|error| {
                failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                    .unwrap_or_else(|| inflight_role_failure(&error, &path))
            });
            Some((item, ordered))
        }
    }))
}

fn inflight_transfer_failure(error: &InflightFailure, path: &StoragePath) -> TransferFailure {
    let role = inflight_role_failure(error, path);
    TransferFailure::role(TransferPhase::Transfer, TransferSide::Source, role)
}

pub(super) fn inflight_role_failure(
    error: &InflightFailure,
    path: &StoragePath,
) -> StorageRoleFailure {
    let class = if *error == InflightFailure::Cancelled {
        FailureClass::Cancelled
    } else {
        FailureClass::Internal
    };
    source_failure(path, class)
}
