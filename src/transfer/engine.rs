use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;

use super::TransferRequest;
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, Operation, SourceIdentity, StoragePath,
    Transience,
};
use crate::runtime::inflight::{
    InflightConfig, InflightFailure, InflightRuntime, OrderedChunks, ReadRange, SequentialRanges,
};
use crate::storage::{
    CheckpointObservation, FinalDestination, PreflightPolicy, PrepareRequest, PreparedStage,
    ReadRequest, ReadSource, SourceDescriptor, StagedDestination, StorageRoleFailure,
    WriteEvidence,
};

/// Lifecycle stage at which one transfer attempt stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferPhase {
    Preflight,
    Describe,
    Prepare,
    Transfer,
    Checkpoint,
}

/// Side responsible for a transfer failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferSide {
    Source,
    Destination,
    Orchestration,
}

/// Backend-neutral data path selected during planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransferDataPath {
    Streaming,
}

#[derive(Clone, Copy)]
struct TransferPlan {
    data_path: TransferDataPath,
    source_size: u64,
    chunk_bytes: usize,
}

/// Structured failure from the unified transfer lifecycle.
#[derive(Debug)]
pub struct TransferFailure {
    phase: TransferPhase,
    side: TransferSide,
    message: &'static str,
    role: Option<Box<StorageRoleFailure>>,
    failed_stage: Option<Box<FailedStage>>,
}

struct FailedStage {
    destination: Arc<dyn StagedDestination>,
    stage: PreparedStage,
}

impl fmt::Debug for FailedStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FailedStage(<opaque>)")
    }
}

impl TransferFailure {
    fn role(phase: TransferPhase, side: TransferSide, role: StorageRoleFailure) -> Self {
        Self {
            phase,
            side,
            message: "storage role failed",
            role: Some(Box::new(role)),
            failed_stage: None,
        }
    }

    fn orchestration(phase: TransferPhase, message: &'static str) -> Self {
        Self {
            phase,
            side: TransferSide::Orchestration,
            message,
            role: None,
            failed_stage: None,
        }
    }

    fn capability(side: TransferSide, message: &'static str) -> Self {
        Self {
            phase: TransferPhase::Preflight,
            side,
            message,
            role: None,
            failed_stage: None,
        }
    }

    fn with_stage(mut self, destination: Arc<dyn StagedDestination>, stage: PreparedStage) -> Self {
        self.failed_stage = Some(Box::new(FailedStage { destination, stage }));
        self
    }

    #[must_use]
    pub const fn phase(&self) -> TransferPhase {
        self.phase
    }

    #[must_use]
    pub const fn side(&self) -> TransferSide {
        self.side
    }

    pub(crate) const fn has_recoverable_stage(&self) -> bool {
        self.failed_stage.is_some()
    }

    pub(crate) async fn discard_stage(mut self) -> Result<(), StorageRoleFailure> {
        let failed = self
            .failed_stage
            .take()
            .ok_or_else(|| source_failure(&StoragePath::root(), FailureClass::InvalidInput))?;
        failed.destination.discard(failed.stage).await
    }
}

impl fmt::Display for TransferFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "transfer {:?} failed on {:?}: {}",
            self.phase, self.side, self.message
        )
    }
}

impl std::error::Error for TransferFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.role
            .as_ref()
            .map(|error| error.as_ref() as &(dyn std::error::Error + 'static))
    }
}

pub(crate) struct Transferred {
    identity: super::TransferIdentity,
    destination: Arc<dyn StagedDestination>,
    stage: PreparedStage,
    write: WriteEvidence,
    checkpoint: CheckpointObservation,
    #[allow(dead_code)]
    source: SourceDescriptor,
    data_path: TransferDataPath,
}

impl Transferred {
    pub(crate) const fn durable_prefix(&self) -> u64 {
        self.checkpoint.durable_prefix
    }

    pub(crate) const fn data_path(&self) -> TransferDataPath {
        self.data_path
    }

    pub(crate) async fn discard(self) -> Result<(), StorageRoleFailure> {
        self.destination.discard(self.stage).await
    }
}

pub(crate) async fn run_until_transferred(
    request: TransferRequest,
) -> Result<Transferred, TransferFailure> {
    let TransferRoles {
        source,
        destination,
    } = lend_transfer_roles(&request)?;
    if request.cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Preflight,
            "transfer was cancelled",
        ));
    }
    let descriptor = source
        .describe(&request.source_path)
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Describe, TransferSide::Source, error)
        })?;
    if request.cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "transfer was cancelled before prepare",
        ));
    }
    let plan = plan_transfer(&descriptor, request.inflight)?;
    let stage = destination
        .prepare(PrepareRequest {
            final_destination: FinalDestination::new(request.final_path.clone()),
            source: descriptor.clone(),
        })
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Prepare, TransferSide::Destination, error)
        })?;
    let identity = request.identity.clone();
    let result = transfer_stage(&request, source, &destination, &descriptor, &stage, plan).await;
    match result {
        Ok(evidence) => Ok(Transferred {
            identity,
            destination,
            stage,
            write: evidence.write,
            checkpoint: evidence.checkpoint,
            source: descriptor,
            data_path: plan.data_path,
        }),
        Err(error) => Err(error.with_stage(destination, stage)),
    }
}

struct TransferRoles {
    source: Arc<dyn ReadSource>,
    destination: Arc<dyn StagedDestination>,
}

fn lend_transfer_roles(request: &TransferRequest) -> Result<TransferRoles, TransferFailure> {
    let policy = PreflightPolicy::production();
    let source = request.source.read_source(&policy).map_err(|_| {
        TransferFailure::capability(TransferSide::Source, "source capability unavailable")
    })?;
    let destination = request
        .destination
        .staged_destination(&policy)
        .map_err(|_| {
            TransferFailure::capability(
                TransferSide::Destination,
                "destination capability unavailable",
            )
        })?;
    Ok(TransferRoles {
        source,
        destination,
    })
}

fn plan_transfer(
    descriptor: &SourceDescriptor,
    limits: super::InflightLimits,
) -> Result<TransferPlan, TransferFailure> {
    if descriptor.kind != EntryKind::File {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "ordinary transfer source must be a file",
        ));
    }
    let source_size = descriptor.size.ok_or_else(|| {
        TransferFailure::orchestration(TransferPhase::Describe, "source has no byte size")
    })?;
    Ok(TransferPlan {
        data_path: TransferDataPath::Streaming,
        source_size,
        chunk_bytes: limits.bytes,
    })
}

async fn transfer_stage(
    request: &TransferRequest,
    source: Arc<dyn ReadSource>,
    destination: &Arc<dyn StagedDestination>,
    descriptor: &SourceDescriptor,
    stage: &PreparedStage,
    plan: TransferPlan,
) -> Result<TransferEvidence, TransferFailure> {
    let (runtime, ordered) =
        inflight_channel(request.inflight, plan.source_size, request.cancel.clone())?;
    let source_failure = Arc::new(Mutex::new(None));
    let producer = tokio::spawn(produce(ProducerRequest {
        source: Arc::clone(&source),
        path: descriptor.path.clone(),
        source_identity: descriptor.source_identity.clone(),
        cancel: request.cancel.clone(),
        chunk_bytes: plan.chunk_bytes,
        runtime,
        failure: Arc::clone(&source_failure),
        size: plan.source_size,
    }));
    let stream = ordered_stream(ordered, source_failure, descriptor.path.clone());
    let write = settle_transfer(destination.write(stage, stream).await, producer).await?;
    let checkpoint = destination
        .observe_checkpoint(stage)
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Checkpoint, TransferSide::Destination, error)
        })?;
    if checkpoint.durable_prefix != plan.source_size || write.persisted_bytes != plan.source_size {
        return Err(TransferFailure::orchestration(
            TransferPhase::Checkpoint,
            "durable bytes differ from source size",
        ));
    }
    Ok(TransferEvidence { write, checkpoint })
}

struct TransferEvidence {
    write: WriteEvidence,
    checkpoint: CheckpointObservation,
}

async fn settle_transfer(
    write: Result<WriteEvidence, StorageRoleFailure>,
    producer: tokio::task::JoinHandle<Result<(), TransferFailure>>,
) -> Result<WriteEvidence, TransferFailure> {
    let write = match write {
        Ok(write) => write,
        Err(error) => {
            producer.abort();
            let _ = producer.await;
            return Err(TransferFailure::role(
                TransferPhase::Transfer,
                failure_side(&error),
                error,
            ));
        }
    };
    let producer_result = producer.await.map_err(|_| {
        TransferFailure::orchestration(TransferPhase::Transfer, "source producer stopped")
    })?;
    producer_result?;
    Ok(write)
}

fn inflight_channel(
    limits: super::InflightLimits,
    size: u64,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(InflightRuntime, OrderedChunks), TransferFailure> {
    let config =
        InflightConfig::new(limits.chunks, limits.bytes, limits.operations).map_err(|_| {
            TransferFailure::orchestration(TransferPhase::Preflight, "invalid inflight limits")
        })?;
    InflightRuntime::channel(config, 0, size, cancel)
        .map_err(|_| TransferFailure::orchestration(TransferPhase::Preflight, "invalid byte range"))
}

struct ProducerRequest {
    source: Arc<dyn ReadSource>,
    path: StoragePath,
    source_identity: SourceIdentity,
    cancel: tokio_util::sync::CancellationToken,
    chunk_bytes: usize,
    runtime: InflightRuntime,
    failure: Arc<Mutex<Option<StorageRoleFailure>>>,
    size: u64,
}

async fn produce(request: ProducerRequest) -> Result<(), TransferFailure> {
    let ranges = SequentialRanges::new(0, request.size, request.chunk_bytes).map_err(|_| {
        TransferFailure::orchestration(TransferPhase::Preflight, "invalid source range")
    })?;
    for range in ranges {
        let admission = request
            .runtime
            .admit(range.offset, range.length)
            .await
            .map_err(|error| inflight_transfer_failure(&error, &request.path))?;
        let bytes = read_exact_range(
            &*request.source,
            &request.path,
            &request.source_identity,
            &request.cancel,
            range,
        )
        .await;
        match bytes {
            Ok(bytes) => admission
                .complete(bytes)
                .await
                .map_err(|error| inflight_transfer_failure(&error, &request.path))?,
            Err(error) => {
                *request
                    .failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
                let _ = request.runtime.fail(InflightFailure::Upstream).await;
                return Err(TransferFailure::role(
                    TransferPhase::Transfer,
                    TransferSide::Source,
                    error,
                ));
            }
        }
    }
    Ok(())
}

async fn read_exact_range(
    source: &dyn ReadSource,
    path: &StoragePath,
    source_identity: &SourceIdentity,
    cancel: &tokio_util::sync::CancellationToken,
    range: ReadRange,
) -> Result<Bytes, StorageRoleFailure> {
    let mut stream = source
        .read(ReadRequest {
            path: path.clone(),
            range: Some(range.offset..range.offset + range.length as u64),
            expected_source: Some(source_identity.clone()),
            cancel: cancel.clone(),
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

fn ordered_stream(
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

fn inflight_role_failure(error: &InflightFailure, path: &StoragePath) -> StorageRoleFailure {
    let class = if *error == InflightFailure::Cancelled {
        FailureClass::Cancelled
    } else {
        FailureClass::Internal
    };
    source_failure(path, class)
}

fn source_failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    let transience = if class == FailureClass::Cancelled {
        Transience::Transient
    } else {
        Transience::Permanent
    };
    let error = EntryOperationFailure::new(
        path.clone(),
        Operation::Read,
        class,
        transience,
        "transfer source stream failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"));
    StorageRoleFailure::Entry(error)
}

fn failure_side(error: &StorageRoleFailure) -> TransferSide {
    match error {
        StorageRoleFailure::Entry(error) if error.operation() == Operation::Read => {
            TransferSide::Source
        }
        StorageRoleFailure::Session(error) if error.operation() == Operation::Read => {
            TransferSide::Source
        }
        StorageRoleFailure::Entry(_) | StorageRoleFailure::Session(_) => TransferSide::Destination,
    }
}
