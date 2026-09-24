//! The source version a transfer asks for, checked before any destination write (ADR-0006 C6).

use super::{TransferFailure, TransferPhase, TransferRequest, TransferSide};
use crate::model::{EntryOperationFailure, FailureClass, Operation, SourceVersion, Transience};
use crate::storage::{ReadSource, StorageRoleFailure};

/// A named source version must be well formed and the source must keep versions; both are
/// checked before any destination write.
pub(super) fn preflight_source_version(
    request: &TransferRequest,
    source: &dyn ReadSource,
) -> Result<(), TransferFailure> {
    let refuse = |class, message| {
        let failure = EntryOperationFailure::new(
            request.source_path.clone(),
            Operation::Observe,
            class,
            Transience::Permanent,
            message,
        )
        .map_err(|_| {
            TransferFailure::orchestration(TransferPhase::Preflight, "invalid source version")
        })?;
        Err(TransferFailure::role(
            TransferPhase::Preflight,
            TransferSide::Source,
            StorageRoleFailure::Entry(failure),
        ))
    };
    if request.source_version.validate().is_err() {
        return refuse(
            FailureClass::InvalidInput,
            "a source version id must be non-empty, bounded and free of NUL",
        );
    }
    if request.source_version != SourceVersion::Current && !source.supports_source_versions() {
        return refuse(
            FailureClass::Unsupported,
            "this source has no versions to select",
        );
    }
    Ok(())
}
