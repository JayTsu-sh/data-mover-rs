//! Refusing a named source version where a source keeps none (ADR-0006 C6).

use super::{ReadRequest, StorageRoleFailure};
use crate::model::{
    EntryOperationFailure, FailureClass, Operation, SourceVersion, StoragePath, Transience,
};

impl ReadRequest {
    /// Refuses a named version on a source without versions, so it is never silently read as
    /// the current one.
    ///
    /// # Errors
    /// Returns an `Unsupported` entry failure for `SourceVersion::Id`.
    pub fn require_current(&self) -> Result<(), StorageRoleFailure> {
        match self.version {
            SourceVersion::Current => Ok(()),
            SourceVersion::Id(_) => Err(version_unsupported(&self.path, Operation::Read)),
        }
    }
}

pub(super) fn version_unsupported(path: &StoragePath, operation: Operation) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            operation,
            FailureClass::Unsupported,
            Transience::Permanent,
            "this source has no versions to select",
        )
        .unwrap_or_else(|_| unreachable!("the static version diagnostic is valid")),
    )
}
