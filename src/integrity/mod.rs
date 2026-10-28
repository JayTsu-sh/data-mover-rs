//! Transfer evidence and independent byte-stream validation.
//!
//! Cross-cutting checks compose deep roles instead of matching on a backend enum: the source
//! and the destination are two connected [`Storage`] handles, and the comparison only uses
//! [`ReadSource`] (describe plus streamed bytes) and [`Metadata`] (permission and metadata facts). Any pair of
//! backends that lends those roles can therefore be compared, including pairs that never
//! existed in the legacy dispatch table.
//!
//! The digest is BLAKE3 over the whole logical object, the same function the transfer engine
//! uses for read-back verification, so a report produced here is comparable with the evidence
//! a transfer emits.

use std::time::Duration;

use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use crate::model::{
    AclEncoding, EntryKind, FailureClass, MetadataObservation, ObservationMode, ObservationPlan,
    ObservedEntry, SourceVersion, StoragePath, StorageTimestamp,
};
use permissions::compare_permissions;

use crate::storage::{
    CapabilityUnavailable, CopiedTimestampTarget, Metadata, ReadRequest, ReadSource,
    SourceDescriptor, Storage, StorageRoleFailure,
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
/// this module deliberately does not take (S3's lists only). Two links whose compared metadata
/// agree therefore compare as matching whatever they point at.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IntegrityMode {
    /// Kind, size, modification time, comparable permissions, and metadata selected by the plan.
    /// No object bytes are read.
    #[default]
    Metadata,
    /// Everything `Metadata` checks plus a full BLAKE3 digest of both objects.
    Content,
}

/// Resolution used when comparing observed modification times.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MtimePrecision {
    /// Compare the exact observed nanoseconds, with the configured tolerance.
    Exact,
    /// Compare at the coarser resolution reported by the two backends.
    #[default]
    Coarsest,
}

/// Comparison tuning. Defaults check metadata at the coarser reported time resolution.
#[derive(Clone, Copy, Debug)]
pub struct IntegrityOptions {
    pub mode: IntegrityMode,
    pub mtime_precision: MtimePrecision,
    /// Absolute timestamp difference still treated as equal, on top of the automatic
    /// comparison at the coarser of the two observed precisions. Defaults to zero.
    pub mtime_tolerance: Duration,
}

impl Default for IntegrityOptions {
    fn default() -> Self {
        Self {
            mode: IntegrityMode::default(),
            mtime_precision: MtimePrecision::default(),
            mtime_tolerance: Duration::ZERO,
        }
    }
}

/// Optional metadata families. Owner, group, and mode comparisons are determined automatically
/// from the facts each backend supplies; `NFSv4` does not guarantee ACL or xattr support.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)] // Independent optional families and explicit strict ACL selectors.
pub struct IntegrityPlan {
    pub acl: bool,
    pub xattrs: bool,
    pub acl_required: bool,
    pub acl_equivalent: bool,
}

impl IntegrityPlan {
    /// Requires complete native ACL observations; unavailable ACLs cannot match.
    #[must_use]
    pub const fn with_required_acl(mut self) -> Self {
        self.acl = true;
        self.acl_required = true;
        self
    }

    /// Compares current regular-file access under a shared numeric identity/group namespace.
    #[must_use]
    pub const fn with_acl_equivalence(mut self) -> Self {
        self.acl = true;
        self.acl_required = true;
        self.acl_equivalent = true;
        self
    }
}

/// One side of a comparison. An observation constrains the entry found at its path.
#[derive(Clone)]
pub enum IntegrityInput {
    /// Describe and compare the current entry at this path.
    Path(StoragePath),
    /// Validate this observation against its backend before comparing the selected entry.
    Observed(Box<ObservedEntry>),
}

impl IntegrityInput {
    fn path(&self) -> &StoragePath {
        match self {
            Self::Path(path) => path,
            Self::Observed(entry) => entry.path(),
        }
    }

    fn observation(&self) -> Option<&ObservedEntry> {
        match self {
            Self::Path(_) => None,
            Self::Observed(entry) => Some(entry),
        }
    }
}

impl From<StoragePath> for IntegrityInput {
    fn from(path: StoragePath) -> Self {
        Self::Path(path)
    }
}

impl From<ObservedEntry> for IntegrityInput {
    fn from(entry: ObservedEntry) -> Self {
        Self::Observed(Box::new(entry))
    }
}

impl From<Box<ObservedEntry>> for IntegrityInput {
    fn from(entry: Box<ObservedEntry>) -> Self {
        Self::Observed(entry)
    }
}

/// One comparison of a single entry on two connected backends.
///
/// Each side can select a path or an [`ObservedEntry`]. The default type arguments preserve
/// path-based requests, so existing callers need no conversion.
#[derive(Clone)]
pub struct IntegrityRequest<S = StoragePath, D = StoragePath> {
    pub source: Storage,
    pub source_path: S,
    pub destination: Storage,
    pub destination_path: D,
    /// Selects ACL and xattr comparisons independently. Both default to omitted.
    pub plan: IntegrityPlan,
    pub options: IntegrityOptions,
    pub cancel: CancellationToken,
}

/// One way in which the two objects disagree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntegrityDifference {
    /// Two native ACLs with the same encoding differ.
    Acl { encoding: AclEncoding },
    /// A required ACL could not be completely observed or proven comparable.
    AclUnverified { reason: &'static str },
    /// The two supported xattr sets differ. Values are not included in the report.
    Xattrs,
    Permissions {
        source: crate::storage::PermissionFacts,
        destination: crate::storage::PermissionFacts,
    },
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

/// A requested fact that the two backends cannot compare without inventing a mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntegrityIgnored {
    Owner,
    Group,
    Mode,
    /// `None` means the side did not provide a comparable ACL value.
    Acl {
        source: Option<AclEncoding>,
        destination: Option<AclEncoding>,
    },
    Xattrs,
    Modified,
}

/// Scope of a successful strict ACL comparison; identity and other facts are checked separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AclComparisonScope {
    NativeConfiguration {
        encoding: AclEncoding,
    },
    /// Current regular-file rwx access with shared numeric identities and group membership.
    CurrentRegularFileAccess,
}

/// The result of one comparison. An empty difference list is a positive match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntegrityReport {
    acl_scope: Option<AclComparisonScope>,
    differences: Vec<IntegrityDifference>,
    ignored: Vec<IntegrityIgnored>,
    compared_bytes: u64,
    digests: Option<([u8; 32], [u8; 32])>,
}

impl IntegrityReport {
    /// The ACL contract proven from complete observations, when strict ACL checking was selected.
    #[must_use]
    pub const fn acl_scope(&self) -> Option<AclComparisonScope> {
        self.acl_scope
    }

    /// Whether the two objects agree on every checked property.
    #[must_use]
    pub fn matches(&self) -> bool {
        self.differences.is_empty()
    }

    #[must_use]
    pub fn differences(&self) -> &[IntegrityDifference] {
        &self.differences
    }

    /// Requested facts skipped because the two sides cannot compare them faithfully.
    #[must_use]
    pub fn ignored(&self) -> &[IntegrityIgnored] {
        &self.ignored
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
    input: IntegrityInput,
    /// Whether this side's modification time is the copied one. A destination that declares it
    /// does not store the time (an object store stamps its own) never received the source's, so
    /// its time is no evidence of a difference.
    mtime_copied: bool,
}

impl Endpoint {
    fn new(
        side: IntegritySide,
        storage: &Storage,
        input: IntegrityInput,
    ) -> Result<Self, CapabilityUnavailable> {
        let mtime_copied = side == IntegritySide::Source
            || storage
                .staged_destination()
                .ok()
                .and_then(|destination| destination.copied_metadata_target())
                .is_none_or(|target| {
                    !matches!(target.timestamps, CopiedTimestampTarget::NotStored)
                });
        Ok(Self {
            side,
            read_source: storage.read_source()?,
            metadata: storage.metadata()?,
            path: input.path().clone(),
            input,
            mtime_copied,
        })
    }

    fn attribute(&self, failure: StorageRoleFailure) -> IntegrityFailure {
        IntegrityFailure {
            side: self.side,
            failure,
        }
    }

    fn observation_failure(&self, class: FailureClass, reason: &'static str) -> IntegrityFailure {
        observation_failure(self.side, &self.path, class, reason)
    }

    /// `Ok(None)` when a path-selected entry does not exist on this side.
    async fn describe(&self) -> Result<Option<SourceDescriptor>, IntegrityFailure> {
        let version = match self.input.observation() {
            Some(observed) => observed.source_version().ok_or_else(|| {
                self.observation_failure(
                    FailureClass::InvalidInput,
                    "observation does not identify a current entry",
                )
            })?,
            None => SourceVersion::Current,
        };
        let described = match &version {
            SourceVersion::Current => self.read_source.describe(&self.path).await,
            SourceVersion::Id(_) => {
                self.read_source
                    .describe_version(&self.path, &version)
                    .await
            }
        };
        match described {
            Ok(descriptor) => {
                if let Some(observed) = self.input.observation() {
                    let modified_changed = observed
                        .modified()
                        .zip(
                            descriptor
                                .inline_timestamps()
                                .and_then(|times| times.modified),
                        )
                        .is_some_and(|(before, now)| {
                            !same_instant(Some(before), Some(now), Duration::ZERO)
                        });
                    if observed.path() != &descriptor.path
                        || observed.kind() != descriptor.kind
                        || observed.size() != descriptor.size
                        || modified_changed
                        || !self
                            .read_source
                            .observation_matches(observed.source_identity(), &descriptor)
                    {
                        return Err(self.observation_failure(
                            FailureClass::Conflict,
                            "entry differs from advertised observation",
                        ));
                    }
                }
                Ok(Some(descriptor))
            }
            Err(failure) if is_not_found(&failure) && self.input.observation().is_none() => {
                Ok(None)
            }
            Err(failure) if is_not_found(&failure) => {
                Err(self
                    .observation_failure(FailureClass::Conflict, "observed entry no longer exists"))
            }
            Err(failure) => Err(self.attribute(failure)),
        }
    }

    async fn modified(
        &self,
        described: &SourceDescriptor,
    ) -> Result<ObservedMtime, IntegrityFailure> {
        if !self.mtime_copied {
            return Ok(ObservedMtime::Unobservable);
        }
        if let Some(timestamps) = described.inline_timestamps() {
            return Ok(ObservedMtime::Observed(timestamps.modified));
        }
        let plan = ObservationPlan::default().with_timestamps(ObservationMode::BestEffort);
        let metadata = if self.input.observation().is_some() {
            self.metadata
                .observe_copy_bound_version(
                    &self.path,
                    &described.source_identity,
                    described.version(),
                    plan,
                )
                .await
                .map_err(|failure| self.attribute(failure))?
                .observations
        } else {
            self.metadata
                .observe(&self.path, plan)
                .await
                .map_err(|failure| self.attribute(failure))?
        };
        let modified = match metadata.timestamps() {
            MetadataObservation::Value { value, .. } => ObservedMtime::Observed(value.modified),
            // The backend cannot report modification time at all. That is not evidence of a
            // difference.
            MetadataObservation::NotRequested
            | MetadataObservation::NotApplicable
            | MetadataObservation::Unsupported
            | MetadataObservation::Failed { .. } => ObservedMtime::Unobservable,
        };
        if let (Some(before), ObservedMtime::Observed(Some(now))) = (
            self.input.observation().and_then(ObservedEntry::modified),
            modified,
        ) && !same_instant(Some(before), Some(now), Duration::ZERO)
        {
            return Err(self.observation_failure(
                FailureClass::Conflict,
                "modification time differs from advertised observation",
            ));
        }
        Ok(modified)
    }

    /// Streams the whole object and returns its digest and byte count.
    async fn digest(
        &self,
        descriptor: &SourceDescriptor,
        cancel: &CancellationToken,
    ) -> Result<([u8; 32], u64), IntegrityFailure> {
        let mut stream = self
            .read_source
            .read(ReadRequest {
                path: self.path.clone(),
                range: None,
                expected_source: Some(descriptor.source_identity.clone()),
                maximum_chunk_bytes: self.read_source.maximum_read_chunk_bytes().max(1),
                read_inflight: self.read_source.read_concurrency().max(1),
                read_budget: None,
                cancel: cancel.clone(),
                source_qos: None,
                // Path inputs compare what is current now; an observed input reads the version
                // its validated description selected. The expected identity guards both reads.
                version: if self.input.observation().is_some() {
                    descriptor.version().clone()
                } else {
                    SourceVersion::Current
                },
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

/// Compares one entry on two connected backends, selected by path or observation on each side.
///
/// Absence on either side is a reported difference, not a failure; only a role failure that
/// prevents the comparison is an error. An observed entry is revalidated against the connected
/// backend; a stale or foreign observation is a failure on that side. Observed versioned entries
/// select their recorded current version. Cancellation surfaces as the backend's own `Cancelled`
/// role failure on the side that observed it.
///
/// # Errors
/// Returns the first role failure, attributed to the side that produced it, or a capability
/// failure when either side lends no read-source or metadata role.
pub async fn compare<S, D>(
    request: IntegrityRequest<S, D>,
) -> Result<IntegrityReport, IntegrityFailure>
where
    S: Into<IntegrityInput>,
    D: Into<IntegrityInput>,
{
    let source = endpoint(
        IntegritySide::Source,
        &request.source,
        request.source_path.into(),
    )?;
    let destination = endpoint(
        IntegritySide::Destination,
        &request.destination,
        request.destination_path.into(),
    )?;
    let (source_entry, destination_entry) =
        reconcile(futures::join!(source.describe(), destination.describe()))?;
    let mut report = IntegrityReport {
        acl_scope: None,
        differences: Vec::new(),
        ignored: Vec::new(),
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
    report.acl_scope = if request.plan.acl
        || request.plan.acl_required
        || request.plan.acl_equivalent
        || request.plan.xattrs
    {
        permissions::compare_optional_metadata(
            &source,
            &destination,
            source_entry,
            destination_entry,
            request.plan,
            &mut report.differences,
            &mut report.ignored,
        )
        .await?
    } else {
        None
    };
    compare_permissions(
        &source,
        &destination,
        source_entry,
        destination_entry,
        matches!(
            report.acl_scope,
            Some(AclComparisonScope::CurrentRegularFileAccess)
        ),
        &mut report.differences,
        &mut report.ignored,
    )
    .await?;
    compare_modified(
        &source,
        &destination,
        (source_entry, destination_entry),
        request.options,
        &mut report.differences,
        &mut report.ignored,
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
    input: IntegrityInput,
) -> Result<Endpoint, IntegrityFailure> {
    let path = input.path().clone();
    if input
        .observation()
        .is_some_and(|entry| entry.source_identity().backend() != storage.identity())
    {
        return Err(observation_failure(
            side,
            &path,
            FailureClass::InvalidInput,
            "observation belongs to another storage",
        ));
    }
    Endpoint::new(side, storage, input).map_err(|error| IntegrityFailure {
        side,
        failure: StorageRoleFailure::Entry(
            crate::model::EntryOperationFailure::new(
                path,
                crate::model::Operation::Observe,
                FailureClass::Unsupported,
                crate::model::Transience::Permanent,
                error.to_string(),
            )
            .unwrap_or_else(|_| unreachable!("capability diagnostics are valid")),
        ),
    })
}

fn observation_failure(
    side: IntegritySide,
    path: &StoragePath,
    class: FailureClass,
    reason: &'static str,
) -> IntegrityFailure {
    IntegrityFailure {
        side,
        failure: StorageRoleFailure::Entry(
            crate::model::EntryOperationFailure::new(
                path.clone(),
                crate::model::Operation::Observe,
                class,
                crate::model::Transience::Permanent,
                reason,
            )
            .unwrap_or_else(|_| unreachable!("static observation diagnostics are valid")),
        ),
    }
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
    entries: (&SourceDescriptor, &SourceDescriptor),
    options: IntegrityOptions,
    differences: &mut Vec<IntegrityDifference>,
    ignored: &mut Vec<IntegrityIgnored>,
) -> Result<(), IntegrityFailure> {
    let (source_time, destination_time) = reconcile(futures::join!(
        source.modified(entries.0),
        destination.modified(entries.1)
    ))?;
    let (ObservedMtime::Observed(source_time), ObservedMtime::Observed(destination_time)) =
        (source_time, destination_time)
    else {
        ignored.push(IntegrityIgnored::Modified);
        return Ok(());
    };
    let equal = match options.mtime_precision {
        MtimePrecision::Coarsest => {
            same_instant(source_time, destination_time, options.mtime_tolerance)
        }
        MtimePrecision::Exact => match (source_time, destination_time) {
            (Some(source), Some(destination)) => {
                source.unix_nanos().abs_diff(destination.unix_nanos())
                    <= options.mtime_tolerance.as_nanos()
            }
            (None, None) => true,
            _ => false,
        },
    };
    if !equal {
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
    cancel: &CancellationToken,
    report: &mut IntegrityReport,
) -> Result<(), IntegrityFailure> {
    let (source_entry, destination_entry) = entries;
    let ((source_digest, source_bytes), (destination_digest, destination_bytes)) =
        reconcile(futures::join!(
            source.digest(source_entry, cancel),
            destination.digest(destination_entry, cancel)
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

mod acl;
mod permissions;

#[cfg(test)]
mod tests;
