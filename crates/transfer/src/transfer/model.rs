use std::{fmt, sync::Arc};

use tokio_util::sync::CancellationToken;

use crate::model::{
    EntryKind, ObservedEntry, SourceIdentity, SourceVersion, StoragePath, StorageTimestamp,
};
use crate::storage::{SourceQosGroup, Storage};

use super::TransferIdentity;

/// Failure to construct a transfer-domain value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferValueError(&'static str);

impl TransferValueError {
    pub(crate) const fn new(message: &'static str) -> Self {
        Self(message)
    }
}

impl fmt::Display for TransferValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for TransferValueError {}

/// Selects target write visibility, recovery behavior, and publication durability.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TransferPolicy {
    /// Use destination-selected checkpoints and retain final durability barriers.
    /// Single-source-chunk and below-threshold transfers may omit checkpoints.
    #[default]
    Checkpointed,
    /// Restart interrupted transfers from zero without creating checkpoints.
    /// Local and NFS retain staging and atomic publication, but omit final persistence
    /// barriers: successful completion does not guarantee crash durability.
    /// Other destinations may retain persistence required by their protocol.
    AtomicReplace,
    /// Write the final path directly, without staging, rename, or checkpoints.
    /// Failure may leave partial content. Supported by Unix Local and HDFS destinations.
    /// HDFS recreates the target and completes its writer; Local writes the existing inode.
    Direct,
}

/// Whether copied content is independently read back before publication.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadBackVerification {
    #[default]
    Enabled,
    Disabled,
}

/// Which layer applies copied metadata. Caller-managed transfers publish data only; the caller
/// must apply and verify its captured metadata before treating the entry as ready or deleting it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetadataTransferMode {
    #[default]
    Automatic,
    CallerManaged,
}

/// Immutable inputs for one transfer attempt.
#[derive(Clone)]
pub struct TransferRequest {
    identity_override: Option<TransferIdentity>,
    input: SourceInput,
    pub(crate) source: Storage,
    pub(crate) destination: Storage,
    pub(crate) cancel: CancellationToken,
    pub(crate) transfer_policy: TransferPolicy,
    pub(crate) source_qos: Option<SourceQosGroup>,
    pub(crate) read_back: ReadBackVerification,
    pub(crate) copied_metadata: CopiedMetadataRequest,
    pub(crate) metadata_mode: MetadataTransferMode,
}

/// One source selection, optionally constrained by an observation. An observed selection
/// takes its path directly from the entry, so the two cannot disagree.
#[derive(Clone)]
enum SourceInput {
    Path {
        path: StoragePath,
        version: SourceVersion,
    },
    Observed {
        source: ObservedSource,
        version_override: Option<SourceVersion>,
    },
}

#[derive(Clone)]
pub(super) struct ObservedSource {
    entry: Arc<ObservedEntry>,
    version_index: Option<usize>,
    directory_marker: bool,
    path_override: Option<StoragePath>,
    pub(super) version: SourceVersion,
}
impl ObservedSource {
    pub(super) fn new(
        source: &Storage,
        observation: Arc<ObservedEntry>,
        version_index: Option<usize>,
        allow_marker: bool,
    ) -> Result<Self, TransferValueError> {
        let directory_marker = version_index.is_none()
            && allow_marker
            && observation.directory_marker_object().is_some();
        let entry = observed_entry(&observation, directory_marker);
        let (identity, version) = if let Some(index) = version_index {
            let record = entry
                .history()
                .and_then(|history| history.records().get(index))
                .ok_or(TransferValueError("invalid observed history index"))?;
            (
                record.identity(),
                record
                    .source()
                    .ok_or(TransferValueError("delete markers are not readable"))?,
            )
        } else {
            if entry.kind() != EntryKind::File {
                return Err(TransferValueError("an observed transfer requires a file"));
            }
            (
                entry.source_identity(),
                entry.source_version().ok_or(TransferValueError(
                    "observation does not identify a current object",
                ))?,
            )
        };
        if identity.backend() != source.identity() {
            return Err(TransferValueError(
                "observation belongs to another source storage",
            ));
        }
        let path_override = if version_index.is_some() && entry.kind() == EntryKind::Directory {
            Some(
                entry
                    .history_source_path()
                    .map_err(|_| TransferValueError("invalid observed history path"))?
                    .ok_or(TransferValueError("observed directory has no history"))?,
            )
        } else {
            None
        };
        Ok(Self {
            entry: observation,
            version_index,
            directory_marker,
            path_override,
            version,
        })
    }
    pub(super) fn path(&self) -> &StoragePath {
        self.path_override
            .as_ref()
            .unwrap_or_else(|| observed_entry(&self.entry, self.directory_marker).path())
    }
    pub(super) fn constraint(&self) -> Option<ObservationConstraint<'_>> {
        let entry = observed_entry(&self.entry, self.directory_marker);
        let mut constraint = ObservationConstraint::from_entry(entry);
        constraint.path = self.path();
        if let Some(index) = self.version_index {
            let record = entry.history()?.records().get(index)?;
            constraint.identity = record.identity();
            constraint.kind = EntryKind::File;
            constraint.size = record.size();
            constraint.modified = record.modified();
        }
        Some(constraint)
    }
}

/// The borrowed facts checked before any destination preparation.
#[derive(Clone, Copy)]
pub(super) struct ObservationConstraint<'a> {
    pub(super) path: &'a StoragePath,
    pub(super) identity: &'a SourceIdentity,
    pub(super) kind: EntryKind,
    pub(super) size: Option<u64>,
    pub(super) modified: Option<StorageTimestamp>,
}
impl<'a> ObservationConstraint<'a> {
    pub(super) fn from_entry(entry: &'a ObservedEntry) -> Self {
        Self {
            path: entry.path(),
            identity: entry.source_identity(),
            kind: entry.kind(),
            size: entry.size(),
            modified: entry.modified(),
        }
    }
}

fn observed_entry(entry: &ObservedEntry, directory_marker: bool) -> &ObservedEntry {
    if directory_marker {
        entry.directory_marker_object().unwrap_or(entry)
    } else {
        entry
    }
}

/// The optional metadata features a copy should carry, beyond the baseline — the "should" of a
/// copy, which the calling application decides (typically from its command line).
///
/// Only the optional features appear here. Ownership, mode and mtime are carried by every copy
/// and cannot be turned off — **what is absent from this type is what is mandatory**. xattrs are
/// one feature, never selected attribute by attribute. Object tags are always copied for S3 to
/// S3 transfers and are not applicable to other backend pairs, so they are not caller-configured.
///
/// Asking for a feature is not a guarantee it is carried: whether it *can* be is decided inside
/// data-mover, by the source's ability to read it and the destination's ability to store it,
/// and comes out as one of
/// - **exact** — carried as is;
/// - **lossy** — carried with a named loss (a coarser precision), recorded in the report;
/// - **cannot** — not read, not written, and the reason recorded in the report. This is never a
///   failure: what the user asked for that the storage cannot do is the caller's to judge.
///
/// ACL requests on regular files automatically project across supported ACL models and report
/// omitted permissions or an explicit deny-all fallback. Other entry kinds retain compatible
/// native ACLs and report incompatible ones as skipped.
///
/// A feature both ends can handle is then carried for real: a read that fails, or a write the
/// destination refuses, fails the transfer — after every other family has been applied, with
/// every reason listed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CopiedMetadataRequest {
    acl: bool,
    xattrs: bool,
}

impl CopiedMetadataRequest {
    /// Copies the access control list, automatically projecting regular-file permissions.
    /// Uncopied ACL meanings and conservative fallback are recorded in the metadata report.
    #[must_use]
    pub const fn with_acl(mut self) -> Self {
        self.acl = true;
        self
    }

    /// Asks for extended attributes.
    #[must_use]
    pub const fn with_xattrs(mut self) -> Self {
        self.xattrs = true;
        self
    }

    #[must_use]
    pub const fn acl(self) -> bool {
        self.acl
    }

    #[must_use]
    pub const fn xattrs(self) -> bool {
        self.xattrs
    }

    /// Whether no optional feature is asked for, which is the default.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        !self.acl && !self.xattrs
    }
}

impl TransferRequest {
    /// Creates one transfer attempt preserving the source path relative to each storage's root.
    /// Each backend applies its own configured root/prefix.
    ///
    /// Its [`TransferIdentity`] is derived from the two endpoints and paths, so any process
    /// building the same request — with no state of its own — names the same transfer.
    #[must_use]
    pub fn new(
        source: Storage,
        source_path: StoragePath,
        destination: Storage,
        cancel: CancellationToken,
    ) -> Self {
        Self::from_input(
            source,
            SourceInput::Path {
                path: source_path,
                version: SourceVersion::Current,
            },
            destination,
            cancel,
        )
    }

    fn from_input(
        source: Storage,
        input: SourceInput,
        destination: Storage,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            identity_override: None,
            input,
            source,
            destination,
            cancel,
            transfer_policy: TransferPolicy::default(),
            source_qos: None,
            read_back: ReadBackVerification::default(),
            copied_metadata: CopiedMetadataRequest::default(),
            metadata_mode: MetadataTransferMode::Automatic,
        }
    }

    /// Copies the file advertised by an observation to the same relative path at the destination,
    /// revalidating it before destination writes. Each backend applies its own root/prefix.
    /// A history selects the observed current object's version, even if a newer version is
    /// written later. Without a history, the current object must still match the observation.
    /// This copies one object, not every version of a history. Validation checks the backend's
    /// identity, size, and modification time when both observations provide it; it does not
    /// turn an observation into a content snapshot.
    /// A real unversioned directory marker is unwrapped to its original File observation when
    /// the destination holds directory objects. Virtual directories are never synthesized into
    /// objects; versioned marker directories use history replay at `history_source_path()`.
    ///
    /// # Errors
    /// Rejects an observation from another storage, a non-file entry, a deleted key, or a
    /// history segment that does not identify the current object.
    pub fn from_observation(
        source: Storage,
        observation: ObservedEntry,
        destination: Storage,
        cancel: CancellationToken,
    ) -> Result<Self, TransferValueError> {
        Self::from_shared_observation(source, Arc::new(observation), None, destination, cancel)
    }

    /// Borrows one immutable observation through an Arc, optionally selecting one history record.
    /// Request cloning shares the original observation, including large histories. A directory's
    /// real current marker is selected without cloning it when the destination holds objects.
    ///
    /// # Errors
    /// Rejects a foreign backend, invalid index, delete marker or non-readable entry before I/O.
    pub fn from_shared_observation(
        source: Storage,
        observation: Arc<ObservedEntry>,
        version_index: Option<usize>,
        destination: Storage,
        cancel: CancellationToken,
    ) -> Result<Self, TransferValueError> {
        let observed = ObservedSource::new(
            &source,
            observation,
            version_index,
            destination.holds_directory_objects(),
        )?;
        Ok(Self::from_input(
            source,
            SourceInput::Observed {
                source: observed,
                version_override: None,
            },
            destination,
            cancel,
        ))
    }

    /// Names the transfer with a caller-chosen identity instead of the derived one. The binding
    /// still covers both paths and the destination, but an interrupted transfer is only resumed
    /// by a request carrying the same override.
    #[must_use]
    pub const fn with_identity_override(mut self, identity: TransferIdentity) -> Self {
        self.identity_override = Some(identity);
        self
    }

    /// Copies one stored version of the source instead of the current one (`SourceVersion::Id`,
    /// S3 only: any other source fails at preflight, before the destination is touched). The
    /// selector is part of the derived identity, so each version is its own transfer; an identity
    /// override stays as given, whichever of the two is called first. For an observed request,
    /// selecting a different version is refused at preflight: its observation cannot describe
    /// another version. Use a path request to select an arbitrary historical version.
    #[must_use]
    pub fn with_source_version(mut self, version: SourceVersion) -> Self {
        match &mut self.input {
            SourceInput::Path {
                version: selected, ..
            } => *selected = version,
            SourceInput::Observed {
                source,
                version_override,
            } => {
                *version_override = (version != source.version).then_some(version);
            }
        }
        self
    }

    /// The source path, taken from the observation when one was supplied.
    #[must_use]
    pub fn source_path(&self) -> &StoragePath {
        match &self.input {
            SourceInput::Path { path, .. } => path,
            SourceInput::Observed { source, .. } => source.path(),
        }
    }

    /// The destination-relative path, always equal to the source-relative path. The destination
    /// backend applies its own configured root/prefix when accessing this path.
    #[must_use]
    pub fn final_path(&self) -> &StoragePath {
        self.source_path()
    }

    /// The source version this request copies.
    #[must_use]
    pub const fn source_version(&self) -> &SourceVersion {
        match &self.input {
            SourceInput::Path { version, .. } => version,
            SourceInput::Observed {
                source,
                version_override,
            } => match version_override {
                Some(selected) => selected,
                None => &source.version,
            },
        }
    }

    pub(super) fn source_observation(&self) -> Option<ObservationConstraint<'_>> {
        match &self.input {
            SourceInput::Observed { source, .. } => source.constraint(),
            SourceInput::Path { .. } => None,
        }
    }

    pub(super) const fn observation_selector_matches(&self) -> bool {
        !matches!(
            self.input,
            SourceInput::Observed {
                version_override: Some(_),
                ..
            }
        )
    }

    /// The identity this request transfers under: derived from its current selection, or overridden.
    #[must_use]
    pub fn identity(&self) -> TransferIdentity {
        self.identity_override.unwrap_or_else(|| {
            TransferIdentity::derive(
                self.source.identity(),
                self.source_path(),
                self.source_version(),
                self.destination.identity(),
                self.final_path(),
            )
        })
    }

    /// Asks the copy to carry optional metadata families. Left alone, a transfer carries the
    /// baseline only.
    #[must_use]
    pub const fn with_copied_metadata(mut self, request: CopiedMetadataRequest) -> Self {
        self.copied_metadata = request;
        self
    }

    /// Selects independent data publication and caller-owned metadata application. In
    /// `CallerManaged` mode no source metadata is fetched and `outcome.metadata` is `None`.
    #[must_use]
    pub const fn with_metadata_mode(mut self, mode: MetadataTransferMode) -> Self {
        self.metadata_mode = mode;
        self
    }

    /// Selects the job-level transfer policy. Route-specific details remain inside data-mover.
    #[must_use]
    pub const fn with_transfer_policy(mut self, policy: TransferPolicy) -> Self {
        self.transfer_policy = policy;
        self
    }

    /// Selects destination read-back verification independently of the transfer policy.
    #[must_use]
    pub const fn with_read_back_verification(mut self, policy: ReadBackVerification) -> Self {
        self.read_back = policy;
        self
    }

    pub(crate) fn needs_source_digest(&self) -> bool {
        self.read_back == ReadBackVerification::Enabled
    }

    pub(crate) fn copies_tags(&self) -> bool {
        self.source.copies_tags_to(&self.destination)
    }

    /// Joins this attempt to one immutable shared source-read `QoS` group.
    #[must_use]
    pub fn with_source_qos(mut self, group: SourceQosGroup) -> Self {
        self.source_qos = Some(group);
        self
    }
}
