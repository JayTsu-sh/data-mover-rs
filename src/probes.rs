//! Explicit connectivity and server-clock probes using the connected roles.

use std::{error::Error, fmt};

use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::model::{
    BackendKind, EntryKind, FailureClass, IdentityStrength, ModelValueError, ObservationMode,
    ObservationPlan, ObservedEntry, SourceIdentity, StoragePath, StorageTimestamp,
};
use crate::transfer::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceEvidence, InflightLimits,
    TransferFailure, TransferPhase, TransferPolicy, TransferSide, TransferValueError,
};

use crate::storage::{
    CapabilityUnavailable, PreflightPolicy, SourceQosStats, Storage, StorageRoleFailure,
};

#[cfg(test)]
mod tests;

/// The failed part of an explicit storage probe.
#[derive(Debug)]
pub enum ProbeError {
    Capability(CapabilityUnavailable),
    Role(StorageRoleFailure),
    Model(ModelValueError),
    Limits(TransferValueError),
    Transfer {
        phase: TransferPhase,
        side: TransferSide,
        diagnostic: String,
    },
    MissingTimestamp,
    PathAlreadyExists,
    DeleteUnsupported(&'static str),
}

/// Probe failure. Cleanup failures are retained alongside the original cause.
#[derive(Debug)]
pub struct ProbeFailure {
    pub error: Box<ProbeError>,
    pub path: Option<StoragePath>,
    pub cleanup: Vec<StorageRoleFailure>,
}

impl From<ProbeError> for ProbeFailure {
    fn from(error: ProbeError) -> Self {
        Self {
            error: Box::new(error),
            path: None,
            cleanup: Vec::new(),
        }
    }
}

impl fmt::Display for ProbeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "storage probe failed: {:?}; cleanup failures: {}",
            self.error,
            self.cleanup.len()
        )
    }
}

impl Error for ProbeFailure {}

/// Makes a fresh namespace connectivity request (S3: `HeadBucket`).
///
/// # Errors
/// Returns an unavailable namespace capability or the actual connectivity failure.
pub async fn check_connectivity(storage: &Storage) -> Result<(), ProbeFailure> {
    let namespace = storage
        .namespace(&PreflightPolicy::production())
        .map_err(ProbeError::Capability)?;
    namespace
        .check_connectivity()
        .await
        .map_err(ProbeError::Role)?;
    Ok(())
}

/// Writes one temporary byte without applying source timestamps, observes its server mtime,
/// and deletes the temporary file. Local returns `None`, since it shares the caller's clock.
/// Versioned S3 retains the probe's object version and delete marker, as ordinary deletion does.
/// This future must be awaited to completion so cleanup runs; failures report cleanup separately.
///
/// # Errors
/// Returns unavailable roles, write/verification/observation failures, a missing timestamp, or
/// failed cleanup. A random UUID path is checked absent before publication.
pub async fn probe_server_time(
    storage: &Storage,
) -> Result<Option<StorageTimestamp>, ProbeFailure> {
    if storage.kind() == BackendKind::Local {
        return Ok(None);
    }
    let policy = PreflightPolicy::production();
    let namespace = storage.namespace(&policy).map_err(ProbeError::Capability)?;
    namespace
        .capabilities()
        .delete
        .map_err(ProbeError::DeleteUnsupported)?;
    let metadata = storage.metadata(&policy).map_err(ProbeError::Capability)?;
    // `.data-mover-*` is reserved for destination-owned transfer artifacts and cannot be
    // reached through the public namespace role.
    let path =
        StoragePath::new(format!(".~clock-{}", Uuid::new_v4())).map_err(ProbeError::Model)?;
    match namespace.stat(&path).await {
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {}
        Err(error) => return Err(ProbeError::Role(error).into()),
        Ok(_) => return Err(ProbeError::PathAlreadyExists.into()),
    }
    let result = async {
        write_probe(storage, &path).await?;
        let plan = ObservationPlan::default().with_timestamps(ObservationMode::Required);
        let facts = metadata
            .observe(&path, plan)
            .await
            .map_err(ProbeError::Role)?;
        facts
            .timestamps()
            .value()
            .and_then(|times| times.modified)
            .map(Some)
            .ok_or_else(|| ProbeError::MissingTimestamp.into())
    }
    .await;
    let cleanup = namespace.delete(&path).await;
    finish_probe(result, cleanup, path)
}

fn finish_probe(
    result: Result<Option<StorageTimestamp>, ProbeFailure>,
    cleanup: Result<(), StorageRoleFailure>,
    path: StoragePath,
) -> Result<Option<StorageTimestamp>, ProbeFailure> {
    let cleanup = cleanup.err().filter(|error| {
        !matches!(error, StorageRoleFailure::Entry(entry) if entry.class() == FailureClass::NotFound)
    });
    match (result, cleanup) {
        (Ok(time), None) => Ok(time),
        (Ok(_), Some(error)) => Err(ProbeFailure {
            error: Box::new(ProbeError::Role(error)),
            path: Some(path),
            cleanup: Vec::new(),
        }),
        (Err(mut failure), cleanup) => {
            failure.path = Some(path);
            failure.cleanup.extend(cleanup);
            Err(failure)
        }
    }
}

async fn write_probe(storage: &Storage, path: &StoragePath) -> Result<(), ProbeFailure> {
    let identity = SourceIdentity::new(
        storage.identity().clone(),
        IdentityStrength::PathScoped,
        path.as_str().as_bytes(),
    )
    .map_err(ProbeError::Model)?;
    let observed = ObservedEntry::new(path.clone(), EntryKind::File, Some(1), None, identity)
        .map_err(ProbeError::Model)?;
    let evidence = ExpertSourceEvidence {
        source_size: 1,
        blake3: *blake3::hash(b"\0").as_bytes(),
        identity_key: observed.identity_key(),
        source_qos: SourceQosStats::default(),
    };
    let request = ExpertDestinationRequest::new(
        observed,
        1,
        storage.clone(),
        path.clone(),
        InflightLimits::new(1, 1, 1).map_err(ProbeError::Limits)?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::AtomicReplace);
    let result = async {
        let session = ExpertDestinationSession::prepare(request).await?;
        let payload = Box::pin(futures::stream::once(async {
            Ok(Bytes::from_static(b"\0"))
        }));
        session.write(payload).await?.complete(evidence).await
    }
    .await;
    match result {
        Ok(_) => Ok(()),
        Err(failure) => Err(clean_transfer(failure).await),
    }
}

async fn clean_transfer(failure: TransferFailure) -> ProbeFailure {
    let error = ProbeError::Transfer {
        phase: failure.phase(),
        side: failure.side(),
        diagnostic: failure.to_string(),
    };
    let cleanup = if failure.has_unpublished_stage() {
        failure.discard_stage().await.err()
    } else if failure.has_pending_cleanup() {
        failure.cleanup_published_stage().await.err()
    } else {
        None
    };
    ProbeFailure {
        error: Box::new(error),
        path: None,
        cleanup: cleanup.into_iter().collect(),
    }
}
