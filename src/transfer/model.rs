use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
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

/// Caller-side failure to durably register a recovery identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryRegistrationFailure {
    /// The persistence or IPC path is temporarily unavailable.
    Unavailable,
    /// The caller permanently rejected the registration.
    Rejected,
}

impl RecoveryRegistrationFailure {
    #[must_use]
    pub const fn unavailable() -> Self {
        Self::Unavailable
    }

    #[must_use]
    pub const fn rejected() -> Self {
        Self::Rejected
    }
}

impl fmt::Display for RecoveryRegistrationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("recovery registration is unavailable"),
            Self::Rejected => formatter.write_str("recovery registration was rejected"),
        }
    }
}

impl std::error::Error for RecoveryRegistrationFailure {}

/// Caller-owned persistence seam for one opaque recovery identity.
///
/// Returning `Ok(())` acknowledges that the identity can be supplied after a caller or worker
/// restart. Data-mover does not write recoverable payload before that acknowledgement.
#[async_trait]
pub trait RecoveryRegistrar: Send + Sync {
    async fn register(&self, identity: RecoveryIdentity)
    -> Result<(), RecoveryRegistrationFailure>;
}

/// Caller-owned recovery inputs opened only after the planner proves a reusable checkpoint is
/// possible for this transfer.
pub struct RecoveryContext {
    pub(crate) identity: Option<RecoveryIdentity>,
    pub(crate) claim: [u8; 32],
    pub(crate) registrar: Arc<dyn RecoveryRegistrar>,
}

impl RecoveryContext {
    #[must_use]
    pub fn new(
        identity: Option<RecoveryIdentity>,
        claim: [u8; 32],
        registrar: Arc<dyn RecoveryRegistrar>,
    ) -> Self {
        Self {
            identity,
            claim,
            registrar,
        }
    }
}

/// Lazily opens caller persistence for a transfer that can actually retain reusable work.
#[async_trait]
pub trait RecoveryProvider: Send + Sync {
    async fn open(&self) -> Result<RecoveryContext, RecoveryRegistrationFailure>;
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

/// Whether a transfer may retain backend-owned state for a later attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Resumability {
    /// Use an ephemeral stage and restart from zero after interruption.
    Disabled,
    /// Allow the backend to retain and re-observe reusable staged work.
    #[default]
    Enabled,
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
    pub(crate) existing_destination: ExistingDestinationPolicy,
    pub(crate) resumability: Resumability,
    pub(crate) recovery_provider: Option<Arc<dyn RecoveryProvider>>,
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
            resumability: Resumability::default(),
            recovery_provider: None,
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

    /// Selects recovery behavior and supplies lazy caller persistence.
    #[must_use]
    pub fn with_recovery(
        mut self,
        resumability: Resumability,
        provider: Option<Arc<dyn RecoveryProvider>>,
    ) -> Self {
        self.resumability = resumability;
        self.recovery_provider = provider;
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
