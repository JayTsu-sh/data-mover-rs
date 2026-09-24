//! Transfer evidence and independent byte-stream validation.
//!
//! Cross-cutting checks compose deep roles instead of matching on a backend enum: the source
//! and the destination are two connected [`Storage`] handles, and the comparison only uses
//! [`ReadSource`] (describe plus streamed bytes) and [`Metadata`] (timestamps). Any pair of
//! backends that lends those roles can therefore be compared, including pairs that never
//! existed in the legacy dispatch table.
//!
//! The digest is BLAKE3 over the whole logical object, the same function the transfer engine
//! uses for read-back verification, so a report produced here is comparable with the evidence
//! a transfer emits.

use std::num::NonZeroUsize;
use std::time::Duration;

use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use crate::model::{
    EntryKind, FailureClass, MetadataObservation, ObservationMode, ObservationPlan, StoragePath,
    StorageTimestamp,
};
use crate::storage::{
    CapabilityUnavailable, CopiedTimestampTarget, Metadata, PreflightPolicy, ReadRequest,
    ReadSource, SourceDescriptor, Storage, StorageRoleFailure,
};

/// Which side of a comparison a fact or failure came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegritySide {
    Source,
    Destination,
}

/// How much evidence the comparison gathers.
///
/// Symlink targets are never compared: reading a link target needs the `Namespace` role, which
/// this module deliberately does not take (S3 lends none). Two links with equal size and
/// timestamp therefore compare as matching whatever they point at.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IntegrityMode {
    /// Kind, size, and modification time only. No object bytes are read.
    #[default]
    Metadata,
    /// Everything `Metadata` checks plus a full BLAKE3 digest of both objects.
    Content,
}

/// Comparison tuning. The defaults check metadata only and demand exact timestamps.
#[derive(Clone, Copy, Debug)]
pub struct IntegrityOptions {
    pub mode: IntegrityMode,
    /// Absolute timestamp difference still treated as equal, on top of the automatic
    /// comparison at the coarser of the two observed precisions.
    pub mtime_tolerance: Duration,
    /// Caller-side upper bound per read; each backend may negotiate something smaller.
    pub maximum_chunk_bytes: NonZeroUsize,
    pub read_inflight: NonZeroUsize,
}

impl Default for IntegrityOptions {
    fn default() -> Self {
        Self {
            mode: IntegrityMode::default(),
            mtime_tolerance: Duration::ZERO,
            maximum_chunk_bytes: NonZeroUsize::new(1 << 20)
                .unwrap_or_else(|| unreachable!("constant is nonzero")),
            read_inflight: NonZeroUsize::new(4)
                .unwrap_or_else(|| unreachable!("constant is nonzero")),
        }
    }
}

/// One comparison of a single object present on two connected backends.
#[derive(Clone)]
pub struct IntegrityRequest {
    pub source: Storage,
    pub source_path: StoragePath,
    pub destination: Storage,
    pub destination_path: StoragePath,
    pub options: IntegrityOptions,
    pub cancel: CancellationToken,
}

/// One way in which the two objects disagree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntegrityDifference {
    /// The object does not exist on the named side.
    Missing { side: IntegritySide },
    Kind {
        source: EntryKind,
        destination: EntryKind,
    },
    Size {
        source: Option<u64>,
        destination: Option<u64>,
    },
    Modified {
        source: Option<StorageTimestamp>,
        destination: Option<StorageTimestamp>,
    },
    /// Byte content differs; only produced in [`IntegrityMode::Content`].
    Content {
        source_bytes: u64,
        destination_bytes: u64,
    },
}

/// The result of one comparison. An empty difference list is a positive match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntegrityReport {
    differences: Vec<IntegrityDifference>,
    compared_bytes: u64,
    digests: Option<([u8; 32], [u8; 32])>,
}

impl IntegrityReport {
    /// Whether the two objects agree on every checked property.
    #[must_use]
    pub fn matches(&self) -> bool {
        self.differences.is_empty()
    }

    #[must_use]
    pub fn differences(&self) -> &[IntegrityDifference] {
        &self.differences
    }

    /// Bytes read from the source side; zero in [`IntegrityMode::Metadata`].
    #[must_use]
    pub const fn compared_bytes(&self) -> u64 {
        self.compared_bytes
    }

    /// Source and destination BLAKE3 digests when content was compared.
    #[must_use]
    pub const fn digests(&self) -> Option<([u8; 32], [u8; 32])> {
        self.digests
    }
}

/// A comparison that could not be completed, attributed to the side that failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntegrityFailure {
    pub side: IntegritySide,
    pub failure: StorageRoleFailure,
}

impl std::fmt::Display for IntegrityFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?} side failed: {:?}", self.side, self.failure)
    }
}

impl std::error::Error for IntegrityFailure {}

/// Roles lent by one side for the duration of a comparison.
struct Endpoint {
    side: IntegritySide,
    read_source: std::sync::Arc<dyn ReadSource>,
    metadata: std::sync::Arc<dyn Metadata>,
    path: StoragePath,
    /// Whether this side's modification time is the copied one. A destination that declares it
    /// does not store the time (an object store stamps its own) never received the source's, so
    /// its time is no evidence of a difference.
    mtime_copied: bool,
}

impl Endpoint {
    fn new(
        side: IntegritySide,
        storage: &Storage,
        path: StoragePath,
    ) -> Result<Self, CapabilityUnavailable> {
        let policy = PreflightPolicy::production();
        let mtime_copied = side == IntegritySide::Source
            || storage
                .staged_destination(&policy)
                .ok()
                .and_then(|destination| destination.copied_metadata_target())
                .is_none_or(|target| {
                    !matches!(target.timestamps, CopiedTimestampTarget::NotStored)
                });
        Ok(Self {
            side,
            read_source: storage.read_source(&policy)?,
            metadata: storage.metadata(&policy)?,
            path,
            mtime_copied,
        })
    }

    fn attribute(&self, failure: StorageRoleFailure) -> IntegrityFailure {
        IntegrityFailure {
            side: self.side,
            failure,
        }
    }

    /// `Ok(None)` when the object does not exist on this side.
    async fn describe(&self) -> Result<Option<SourceDescriptor>, IntegrityFailure> {
        match self.read_source.describe(&self.path).await {
            Ok(descriptor) => Ok(Some(descriptor)),
            Err(failure) if is_not_found(&failure) => Ok(None),
            Err(failure) => Err(self.attribute(failure)),
        }
    }

    async fn modified(&self) -> Result<ObservedMtime, IntegrityFailure> {
        if !self.mtime_copied {
            return Ok(ObservedMtime::Unobservable);
        }
        let plan = ObservationPlan::default().with_timestamps(ObservationMode::BestEffort);
        let observed = self
            .metadata
            .observe(&self.path, plan)
            .await
            .map_err(|failure| self.attribute(failure))?;
        Ok(match observed.timestamps() {
            MetadataObservation::Value { value, .. } => ObservedMtime::Observed(value.modified),
            // The backend cannot report modification time at all. That is not evidence of a
            // difference.
            MetadataObservation::NotRequested
            | MetadataObservation::NotApplicable
            | MetadataObservation::Unsupported
            | MetadataObservation::Failed { .. } => ObservedMtime::Unobservable,
        })
    }

    /// Streams the whole object and returns its digest and byte count.
    async fn digest(
        &self,
        descriptor: &SourceDescriptor,
        options: IntegrityOptions,
        cancel: &CancellationToken,
    ) -> Result<([u8; 32], u64), IntegrityFailure> {
        let mut stream = self
            .read_source
            .read(ReadRequest {
                path: self.path.clone(),
                range: None,
                expected_source: Some(descriptor.source_identity.clone()),
                maximum_chunk_bytes: options.maximum_chunk_bytes.get(),
                read_inflight: options.read_inflight.get(),
                read_budget: None,
                cancel: cancel.clone(),
                source_qos: None,
            })
            .await
            .map_err(|failure| self.attribute(failure))?;
        let mut hasher = blake3::Hasher::new();
        let mut bytes = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|failure| self.attribute(failure))?;
            bytes = bytes.saturating_add(chunk.len() as u64);
            hasher.update(&chunk);
        }
        Ok((*hasher.finalize().as_bytes(), bytes))
    }
}

/// Whether a side could report a modification time at all, kept separate from the value so an
/// unobservable side never reads as a mismatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObservedMtime {
    Unobservable,
    Observed(Option<StorageTimestamp>),
}

fn is_not_found(failure: &StorageRoleFailure) -> bool {
    match failure {
        StorageRoleFailure::Entry(error) => error.class() == FailureClass::NotFound,
        StorageRoleFailure::Session(_) => false,
    }
}

/// Compares one object on two connected backends.
///
/// Absence on either side is a reported difference, not a failure; only a role failure that
/// prevents the comparison is an error. Cancellation surfaces as the backend's own
/// `Cancelled` role failure on the side that observed it.
///
/// # Errors
/// Returns the first role failure, attributed to the side that produced it, or a capability
/// failure when either side lends no read-source or metadata role.
pub async fn compare(request: IntegrityRequest) -> Result<IntegrityReport, IntegrityFailure> {
    let source = endpoint(IntegritySide::Source, &request.source, &request.source_path)?;
    let destination = endpoint(
        IntegritySide::Destination,
        &request.destination,
        &request.destination_path,
    )?;
    let (source_entry, destination_entry) =
        reconcile(futures::join!(source.describe(), destination.describe()))?;
    let mut report = IntegrityReport {
        differences: Vec::new(),
        compared_bytes: 0,
        digests: None,
    };
    let (Some(source_entry), Some(destination_entry)) = (&source_entry, &destination_entry) else {
        report
            .differences
            .extend(missing(source_entry.is_none(), destination_entry.is_none()));
        return Ok(report);
    };
    compare_facts(source_entry, destination_entry, &mut report.differences);
    compare_modified(
        &source,
        &destination,
        request.options.mtime_tolerance,
        &mut report.differences,
    )
    .await?;
    if request.options.mode == IntegrityMode::Content
        && source_entry.kind == EntryKind::File
        && destination_entry.kind == EntryKind::File
    {
        compare_content(
            &source,
            &destination,
            (source_entry, destination_entry),
            request.options,
            &request.cancel,
            &mut report,
        )
        .await?;
    }
    Ok(report)
}

fn endpoint(
    side: IntegritySide,
    storage: &Storage,
    path: &StoragePath,
) -> Result<Endpoint, IntegrityFailure> {
    Endpoint::new(side, storage, path.clone()).map_err(|error| IntegrityFailure {
        side,
        failure: StorageRoleFailure::Entry(
            crate::model::EntryOperationFailure::new(
                path.clone(),
                crate::model::Operation::Observe,
                FailureClass::Unsupported,
                crate::model::Transience::Permanent,
                error.to_string(),
            )
            .unwrap_or_else(|_| unreachable!("capability diagnostics are valid")),
        ),
    })
}

fn missing(source: bool, destination: bool) -> Vec<IntegrityDifference> {
    let mut differences = Vec::new();
    if source {
        differences.push(IntegrityDifference::Missing {
            side: IntegritySide::Source,
        });
    }
    if destination {
        differences.push(IntegrityDifference::Missing {
            side: IntegritySide::Destination,
        });
    }
    differences
}

fn compare_facts(
    source: &SourceDescriptor,
    destination: &SourceDescriptor,
    differences: &mut Vec<IntegrityDifference>,
) {
    if source.kind != destination.kind {
        differences.push(IntegrityDifference::Kind {
            source: source.kind,
            destination: destination.kind,
        });
    }
    if source.size != destination.size {
        differences.push(IntegrityDifference::Size {
            source: source.size,
            destination: destination.size,
        });
    }
}

async fn compare_modified(
    source: &Endpoint,
    destination: &Endpoint,
    tolerance: Duration,
    differences: &mut Vec<IntegrityDifference>,
) -> Result<(), IntegrityFailure> {
    let (source_time, destination_time) =
        reconcile(futures::join!(source.modified(), destination.modified()))?;
    let (ObservedMtime::Observed(source_time), ObservedMtime::Observed(destination_time)) =
        (source_time, destination_time)
    else {
        return Ok(());
    };
    if !same_instant(source_time, destination_time, tolerance) {
        differences.push(IntegrityDifference::Modified {
            source: source_time,
            destination: destination_time,
        });
    }
    Ok(())
}

/// Combines two settled role results, keeping the source-side failure when both failed.
///
/// Both futures are always driven to completion: a role call that opened a backend handle
/// closes it inside its own future, and dropping that future on the first error would abandon
/// the handle (the long-session handle-exhaustion failure mode).
fn reconcile<T, U>(
    results: (Result<T, IntegrityFailure>, Result<U, IntegrityFailure>),
) -> Result<(T, U), IntegrityFailure> {
    match results {
        (Ok(first), Ok(second)) => Ok((first, second)),
        (Err(failure), _) | (Ok(_), Err(failure)) => Err(failure),
    }
}

/// Two timestamps agree when they are equal at the coarser of the observed precisions, or
/// within the configured tolerance. Cross-protocol copies routinely lose resolution, so a
/// nanosecond comparison would report every NFS-to-SMB copy as different.
fn same_instant(
    source: Option<StorageTimestamp>,
    destination: Option<StorageTimestamp>,
    tolerance: Duration,
) -> bool {
    let (Some(source), Some(destination)) = (source, destination) else {
        return source.is_none() && destination.is_none();
    };
    let quantum = source
        .precision()
        .quantum_nanos()
        .max(destination.precision().quantum_nanos());
    if source.unix_nanos().div_euclid(quantum) == destination.unix_nanos().div_euclid(quantum) {
        return true;
    }
    source.unix_nanos().abs_diff(destination.unix_nanos()) <= tolerance.as_nanos()
}

async fn compare_content(
    source: &Endpoint,
    destination: &Endpoint,
    entries: (&SourceDescriptor, &SourceDescriptor),
    options: IntegrityOptions,
    cancel: &CancellationToken,
    report: &mut IntegrityReport,
) -> Result<(), IntegrityFailure> {
    let (source_entry, destination_entry) = entries;
    let ((source_digest, source_bytes), (destination_digest, destination_bytes)) =
        reconcile(futures::join!(
            source.digest(source_entry, options, cancel),
            destination.digest(destination_entry, options, cancel)
        ))?;
    report.compared_bytes = source_bytes;
    report.digests = Some((source_digest, destination_digest));
    if source_digest != destination_digest {
        report.differences.push(IntegrityDifference::Content {
            source_bytes,
            destination_bytes,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
