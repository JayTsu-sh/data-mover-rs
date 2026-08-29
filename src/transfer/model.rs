use std::fmt;

use tokio_util::sync::CancellationToken;

use crate::model::StoragePath;
use crate::storage::{ExistingDestinationPolicy, RecoveryIdentity, SourceQosGroup, Storage};

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

/// Recovery behavior for one transfer attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecoveryPolicy {
    #[default]
    ResumeOrRestart,
    RequireResume,
    Restart,
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
    pub(crate) existing_destination: ExistingDestinationPolicy,
    pub(crate) recovery_policy: RecoveryPolicy,
    pub(crate) recovery_identity: Option<RecoveryIdentity>,
    pub(crate) recovery_claim: [u8; 32],
    pub(crate) source_qos: Option<SourceQosGroup>,
    pub(crate) payload_shaping: PayloadShapingPolicy,
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
            existing_destination: ExistingDestinationPolicy::default(),
            recovery_policy: RecoveryPolicy::default(),
            recovery_identity: None,
            recovery_claim: *blake3::hash(uuid::Uuid::new_v4().as_bytes()).as_bytes(),
            source_qos: None,
            payload_shaping: PayloadShapingPolicy::default(),
        }
    }

    /// Selects how publication handles an existing destination.
    #[must_use]
    pub fn with_existing_destination_policy(mut self, policy: ExistingDestinationPolicy) -> Self {
        self.existing_destination = policy;
        self
    }

    /// Selects recovery behavior and supplies an optional persisted opaque identity.
    #[must_use]
    pub fn with_recovery(
        mut self,
        policy: RecoveryPolicy,
        identity: Option<RecoveryIdentity>,
    ) -> Self {
        self.recovery_policy = policy;
        self.recovery_identity = identity;
        self
    }

    /// Supplies the caller-persisted identity for an idempotent recovery attempt.
    #[must_use]
    pub const fn with_recovery_claim(mut self, claim: [u8; 32]) -> Self {
        self.recovery_claim = claim;
        self
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
