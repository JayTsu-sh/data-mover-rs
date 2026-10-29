//! Verified copy followed by guarded source removal.

use std::{error::Error, fmt};

use crate::model::{EntryKind, SourceVersion};
use crate::storage::{CapabilityUnavailable, SourceDescriptor, StorageRoleFailure};

use super::{ReadBackVerification, TransferFailure, TransferOutcome, TransferRequest, transfer};

#[cfg(test)]
mod tests;

/// Why a move stopped. A transfer failure retains its normal recovery authority.
#[derive(Debug)]
pub enum MoveError {
    InvalidRequest(&'static str),
    Capability(CapabilityUnavailable),
    Source(StorageRoleFailure),
    Transfer(Box<TransferFailure>),
    SourceChanged,
    Cancelled,
}

/// Failed move, including the completed copy when source removal did not complete.
#[derive(Debug)]
pub struct MoveFailure {
    pub error: MoveError,
    /// `Some` means verification, metadata application and publication succeeded. The source
    /// may remain, or a failed delete may have taken effect; inspect it before retrying.
    pub published: Option<Box<TransferOutcome>>,
}

impl fmt::Display for MoveFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "move failed: {:?}; copy published: {}",
            self.error,
            self.published.is_some()
        )
    }
}

impl Error for MoveFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.error {
            MoveError::Capability(error) => Some(error),
            MoveError::Source(error) => Some(error),
            MoveError::Transfer(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<MoveError> for MoveFailure {
    fn from(error: MoveError) -> Self {
        Self {
            error,
            published: None,
        }
    }
}

/// Copies one current regular file, then removes its source only after successful read-back,
/// baseline/selected metadata application and publication. All request policies and `QoS` remain
/// effective, except read-back is always enabled. S3 removal retains version history.
///
/// The caller must exclusively control the source path through this operation and must ensure
/// destination aliases do not name that same source. Identity/content checks detect observed
/// changes, but namespace protocols do not provide a universal atomic compare-and-unlink: a
/// concurrent replacement between the final check and delete cannot be protected here.
///
/// # Errors
/// Refuses historical versions, identical endpoint/path pairs, non-files and unavailable delete
/// support before copying. Copy failure leaves the source intact. Later failure returns the
/// published copy in [`MoveFailure::published`], so callers need not copy it again.
pub async fn move_file(mut request: TransferRequest) -> Result<TransferOutcome, MoveFailure> {
    validate(&request)?;

    let namespace = request.source.namespace().map_err(MoveError::Capability)?;
    namespace
        .capabilities()
        .delete
        .map_err(MoveError::InvalidRequest)?;
    let source = request
        .source
        .read_source()
        .map_err(MoveError::Capability)?;
    let before = source
        .describe(request.source_path())
        .await
        .map_err(MoveError::Source)?;
    if before.kind != EntryKind::File {
        return Err(MoveError::InvalidRequest("move requires a regular file").into());
    }
    request.read_back = ReadBackVerification::Enabled;
    let outcome = Box::pin(transfer(request.clone()))
        .await
        .map_err(|failure| MoveError::Transfer(Box::new(failure)))?;
    let cleanup = async {
        if request.cancel.is_cancelled() {
            return Err(MoveError::Cancelled);
        }
        let after = source
            .describe(request.source_path())
            .await
            .map_err(MoveError::Source)?;
        if !same_source(&before, &after) {
            return Err(MoveError::SourceChanged);
        }
        namespace
            .delete(request.source_path())
            .await
            .map_err(MoveError::Source)
    }
    .await;
    match cleanup {
        Ok(()) => Ok(outcome),
        Err(error) => Err(MoveFailure {
            error,
            published: Some(Box::new(outcome)),
        }),
    }
}

fn validate(request: &TransferRequest) -> Result<(), MoveError> {
    if request.source_version() != &SourceVersion::Current {
        return Err(MoveError::InvalidRequest(
            "move cannot delete a selected historical version",
        ));
    }
    if request.source.identity() == request.destination.identity()
        && request.source_path() == request.final_path()
    {
        return Err(MoveError::InvalidRequest(
            "move source and destination are identical",
        ));
    }
    if request.cancel.is_cancelled() {
        return Err(MoveError::Cancelled);
    }
    Ok(())
}

fn same_source(before: &SourceDescriptor, after: &SourceDescriptor) -> bool {
    before.path == after.path
        && before.kind == after.kind
        && before.size == after.size
        && before.source_identity == after.source_identity
        && before.content_version == after.content_version
        && before.version == after.version
        && before.inline_timestamps.map(|times| times.modified)
            == after.inline_timestamps.map(|times| times.modified)
}
