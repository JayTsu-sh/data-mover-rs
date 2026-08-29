use std::fmt;

use tokio_util::sync::CancellationToken;

use crate::model::StoragePath;
use crate::storage::Storage;

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
        }
    }
}
