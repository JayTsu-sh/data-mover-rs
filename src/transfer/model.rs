use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::model::{SourceVersion, StoragePath};
use crate::storage::{RecoveryIdentity, SourceQosGroup, Storage};

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

/// Failure to durably register a recovery identity inside data-mover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryRegistrationFailure {
    /// The persistence or IPC path is temporarily unavailable.
    Unavailable,
    /// A persisted record failed validation.
    Rejected,
}

impl RecoveryRegistrationFailure {
    #[must_use]
    pub(crate) const fn unavailable() -> Self {
        Self::Unavailable
    }

    #[must_use]
    pub(crate) const fn rejected() -> Self {
        Self::Rejected
    }
}

impl fmt::Display for RecoveryRegistrationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("recovery registration is unavailable"),
            Self::Rejected => formatter.write_str("recovery state validation failed"),
        }
    }
}

impl std::error::Error for RecoveryRegistrationFailure {}

/// Data-mover-owned persistence seam for one opaque recovery identity.
#[async_trait]
pub(crate) trait RecoveryRegistrar: Send + Sync {
    async fn register(&self, identity: RecoveryIdentity)
    -> Result<(), RecoveryRegistrationFailure>;
}

/// Recovery inputs opened internally only after planning proves a checkpoint is useful.
pub(crate) struct RecoveryContext {
    pub(crate) identity: Option<RecoveryIdentity>,
    pub(crate) claim: [u8; 32],
    pub(crate) publication_pending: bool,
    pub(crate) registrar: Arc<dyn RecoveryRegistrar>,
    pub(crate) lease: Arc<std::fs::File>,
}

impl RecoveryContext {
    #[must_use]
    pub(crate) fn new(
        identity: Option<RecoveryIdentity>,
        claim: [u8; 32],
        publication_pending: bool,
        registrar: Arc<dyn RecoveryRegistrar>,
        lease: Arc<std::fs::File>,
    ) -> Self {
        Self {
            identity,
            claim,
            publication_pending,
            registrar,
            lease,
        }
    }
}

/// Explicit chunk, payload-byte, and source-operation admission bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InflightLimits {
    pub(crate) chunks: usize,
    pub(crate) bytes: usize,
    pub(crate) operations: usize,
}

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

/// Whether a planner may select a server-internal, unshaped native payload path.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PayloadShapingPolicy {
    #[default]
    AllowUnshapedNative,
    RequireClientShaped,
}

impl InflightLimits {
    /// Creates non-zero bounded inflight limits.
    ///
    /// # Errors
    /// Returns an error when any limit is zero or exceeds semaphore capacity.
    pub fn new(chunks: usize, bytes: usize, operations: usize) -> Result<Self, TransferValueError> {
        let maximum = u32::MAX as usize;
        if chunks == 0 || bytes == 0 || operations == 0 {
            return Err(TransferValueError(
                "inflight limits must be greater than zero",
            ));
        }
        if chunks > maximum || bytes > maximum || operations > maximum {
            return Err(TransferValueError(
                "inflight limits exceed semaphore capacity",
            ));
        }
        Ok(Self {
            chunks,
            bytes,
            operations,
        })
    }

    pub(crate) fn negotiated_chunk_ceiling(self) -> usize {
        let streams = self.chunks.min(self.operations).max(1);
        (self.bytes / streams).max(1)
    }
}

/// Immutable inputs for one transfer attempt.
#[derive(Clone)]
pub struct TransferRequest {
    pub(crate) identity: TransferIdentity,
    /// Whether `identity` is a caller override, which a source version must not re-derive.
    identity_overridden: bool,
    pub(crate) source_version: SourceVersion,
    pub(crate) source: Storage,
    pub(crate) source_path: StoragePath,
    pub(crate) destination: Storage,
    pub(crate) final_path: StoragePath,
    pub(crate) inflight: InflightLimits,
    pub(crate) cancel: CancellationToken,
    pub(crate) transfer_policy: TransferPolicy,
    pub(crate) source_qos: Option<SourceQosGroup>,
    pub(crate) payload_shaping: PayloadShapingPolicy,
    pub(crate) read_back: ReadBackVerification,
    pub(crate) copied_metadata: CopiedMetadataRequest,
}

/// The optional metadata features a copy should carry, beyond the baseline — the "should" of a
/// copy, which the calling application decides (typically from its command line).
///
/// Only the optional features appear here. Ownership, mode and mtime are carried by every copy
/// and cannot be turned off — **what is absent from this type is what is mandatory**. xattrs are
/// one feature, never selected attribute by attribute.
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
    /// Creates one transfer attempt from connected storage backends.
    ///
    /// Its [`TransferIdentity`] is derived from the two endpoints and paths, so any process
    /// building the same request — with no state of its own — names the same transfer.
    #[must_use]
    pub fn new(
        source: Storage,
        source_path: StoragePath,
        destination: Storage,
        final_path: StoragePath,
        inflight: InflightLimits,
        cancel: CancellationToken,
    ) -> Self {
        let identity = TransferIdentity::derive(
            source.identity(),
            &source_path,
            &SourceVersion::Current,
            destination.identity(),
            &final_path,
        );
        Self {
            identity,
            identity_overridden: false,
            source_version: SourceVersion::Current,
            source,
            source_path,
            destination,
            final_path,
            inflight,
            cancel,
            transfer_policy: TransferPolicy::default(),
            source_qos: None,
            payload_shaping: PayloadShapingPolicy::default(),
            read_back: ReadBackVerification::default(),
            copied_metadata: CopiedMetadataRequest::default(),
        }
    }

    /// Names the transfer with a caller-chosen identity instead of the derived one. The binding
    /// still covers both paths and the destination, but an interrupted transfer is only resumed
    /// by a request carrying the same override.
    #[must_use]
    pub const fn with_identity_override(mut self, identity: TransferIdentity) -> Self {
        self.identity = identity;
        self.identity_overridden = true;
        self
    }

    /// Copies one stored version of the source instead of the current one (`SourceVersion::Id`,
    /// S3 only: any other source fails at preflight, before the destination is touched). The
    /// selector is part of the derived identity, so each version is its own transfer; an identity
    /// override stays as given, whichever of the two is called first.
    #[must_use]
    pub fn with_source_version(mut self, version: SourceVersion) -> Self {
        if !self.identity_overridden {
            self.identity = TransferIdentity::derive(
                self.source.identity(),
                &self.source_path,
                &version,
                self.destination.identity(),
                &self.final_path,
            );
        }
        self.source_version = version;
        self
    }

    /// The source version this request copies.
    #[must_use]
    pub const fn source_version(&self) -> &SourceVersion {
        &self.source_version
    }

    /// The identity this request transfers under: derived, or the override.
    #[must_use]
    pub const fn identity(&self) -> TransferIdentity {
        self.identity
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

    /// Joins this attempt to one immutable shared source-read `QoS` group.
    #[must_use]
    pub fn with_source_qos(mut self, group: SourceQosGroup) -> Self {
        self.source_qos = Some(group);
        self
    }

    /// Selects whether server-internal native payload is eligible.
    #[must_use]
    pub fn with_payload_shaping(mut self, policy: PayloadShapingPolicy) -> Self {
        self.payload_shaping = policy;
        self
    }
}
