use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::metadata::MetadataPolicy;
use crate::model::StoragePath;
use crate::storage::{RecoveryIdentity, SourceQosGroup, Storage};

const MAX_IDENTITY_BYTES: usize = 1024;

/// Failure to construct a transfer-domain value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferValueError(&'static str);

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

/// Caller-provided stable identity for one logical transfer.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct TransferIdentity(String);

impl TransferIdentity {
    /// Creates an opaque identity that remains stable across attempts.
    ///
    /// # Errors
    /// Returns an error for blank, NUL-containing, or unbounded values.
    pub fn new(value: impl Into<String>) -> Result<Self, TransferValueError> {
        let value = value.into();
        if value.trim().is_empty() || value.contains('\0') || value.len() > MAX_IDENTITY_BYTES {
            Err(TransferValueError(
                "transfer identity must be non-blank and bounded",
            ))
        } else {
            Ok(Self(value))
        }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for TransferIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TransferIdentity(<opaque>)")
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

/// Metadata families a caller can ask a copy to carry, beyond the baseline.
///
/// Only the optional families appear here. Ownership, mode and timestamps are copied by every
/// transfer and cannot be turned off — **what is absent from this type is what is mandatory**.
/// A family named here is carried only when asked for, and only when both ends can: the
/// destination reports what it accepts through `CopiedMetadataTarget`, the source reports what it
/// can read through its observations, and either one can refuse.
///
/// The policy picks what happens when one of them refuses:
/// - [`MetadataPolicy::Omit`] (the default) does not even observe the family.
/// - [`MetadataPolicy::BestEffort`] carries it when both ends can and records why when they
///   cannot. A write the destination refuses after advertising the capability — the usual `NFSv4`
///   `SETACL` outcome on many servers — still fails the transfer, once the other families have
///   been applied.
/// - [`MetadataPolicy::AllowKnownLoss`] requires the family to be carried, accepting a documented
///   downgrade, and **fails** when either end lacks it outright.
/// - [`MetadataPolicy::RequireExact`] additionally rejects any downgrade.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CopiedMetadataRequest {
    acl: MetadataPolicy,
    xattrs: MetadataPolicy,
}

impl CopiedMetadataRequest {
    /// Asks for the access control list under `policy`.
    #[must_use]
    pub const fn with_acl(mut self, policy: MetadataPolicy) -> Self {
        self.acl = policy;
        self
    }

    /// Asks for extended attributes under `policy`.
    #[must_use]
    pub const fn with_xattrs(mut self, policy: MetadataPolicy) -> Self {
        self.xattrs = policy;
        self
    }

    #[must_use]
    pub const fn acl(self) -> MetadataPolicy {
        self.acl
    }

    #[must_use]
    pub const fn xattrs(self) -> MetadataPolicy {
        self.xattrs
    }

    /// Whether every optional family is switched off, which is the default.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.acl == MetadataPolicy::Omit && self.xattrs == MetadataPolicy::Omit
    }
}

impl TransferRequest {
    /// Creates one transfer attempt from connected storage backends.
    #[must_use]
    pub fn new(
        identity: TransferIdentity,
        source: Storage,
        source_path: StoragePath,
        destination: Storage,
        final_path: StoragePath,
        inflight: InflightLimits,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            identity,
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
