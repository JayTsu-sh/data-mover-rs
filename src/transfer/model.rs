use std::fmt;

use tokio_util::sync::CancellationToken;

use crate::model::{EntryKind, ObservedEntry, SourceVersion, StoragePath};
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
        entry: Box<ObservedEntry>,
        version: SourceVersion,
    },
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
/// A feature both ends can handle is then carried for real: a read that fails, or a write the
/// destination refuses, fails the transfer — after every other family has been applied, with
/// every reason listed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CopiedMetadataRequest {
    acl: bool,
    xattrs: bool,
}

impl CopiedMetadataRequest {
    /// Asks for the access control list.
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
        Self {
            identity_override: None,
            input: SourceInput::Path {
                path: source_path,
                version: SourceVersion::Current,
            },
            source,
            destination,
            cancel,
            transfer_policy: TransferPolicy::default(),
            source_qos: None,
            read_back: ReadBackVerification::default(),
            copied_metadata: CopiedMetadataRequest::default(),
        }
    }

    /// Copies the file advertised by an observation to the same relative path at the destination,
    /// revalidating it before destination writes. Each backend applies its own root/prefix.
    /// A history selects the observed current object's version, even if a newer version is
    /// written later. Without a history, the current object must still match the observation.
    /// This copies one object, not every version of a history. Validation checks the backend's
    /// identity, size, and modification time when both observations provide it; it does not
    /// turn an observation into a content snapshot.
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
        if observation.source_identity().backend() != source.identity() {
            return Err(TransferValueError(
                "observation belongs to another source storage",
            ));
        }
        if observation.kind() != EntryKind::File {
            return Err(TransferValueError("an observed transfer requires a file"));
        }
        let version = observation.source_version().ok_or(TransferValueError(
            "observation does not identify a current object",
        ))?;
        let mut request = Self::new(source, observation.path().clone(), destination, cancel);
        request.input = SourceInput::Observed {
            entry: Box::new(observation),
            version,
        };
        Ok(request)
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
            }
            | SourceInput::Observed {
                version: selected, ..
            } => *selected = version,
        }
        self
    }

    /// The source path, taken from the observation when one was supplied.
    #[must_use]
    pub fn source_path(&self) -> &StoragePath {
        match &self.input {
            SourceInput::Path { path, .. } => path,
            SourceInput::Observed { entry, .. } => entry.path(),
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
            SourceInput::Path { version, .. } | SourceInput::Observed { version, .. } => version,
        }
    }

    pub(crate) fn source_observation(&self) -> Option<&ObservedEntry> {
        match &self.input {
            SourceInput::Path { .. } => None,
            SourceInput::Observed { entry, .. } => Some(entry),
        }
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
