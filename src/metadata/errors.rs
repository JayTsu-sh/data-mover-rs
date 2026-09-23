//! Why metadata could not be planned or applied, in terms a caller can act on.

use std::fmt;

use super::{MetadataApplicationReport, MetadataFamily};
use crate::storage::StorageRoleFailure;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataPlanErrorKind {
    KnownLossRejected,
    ExternalMappingRequired,
    Unsupported,
    ObservationFailed,
    ObservationRequired,
    PrincipalMappingFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataPlanError {
    pub(super) family: MetadataFamily,
    pub(super) kind: MetadataPlanErrorKind,
}

impl MetadataPlanError {
    /// Builds one refusal. Callers outside this module only ever read refusals; this exists so
    /// their tests can state the case they are checking instead of provoking it.
    #[cfg(test)]
    pub(crate) const fn new(family: MetadataFamily, kind: MetadataPlanErrorKind) -> Self {
        Self { family, kind }
    }

    #[must_use]
    pub const fn family(self) -> MetadataFamily {
        self.family
    }
    #[must_use]
    pub const fn kind(self) -> MetadataPlanErrorKind {
        self.kind
    }
}

impl fmt::Display for MetadataPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "metadata {:?} planning failed: {:?}",
            self.family, self.kind
        )
    }
}

impl std::error::Error for MetadataPlanError {}

#[derive(Debug)]
pub struct MetadataApplicationFailure {
    pub(super) family: MetadataFamily,
    /// Boxed so the whole `Result` stays small: the role failure carries paths and diagnostic
    /// strings, and every successful application would otherwise pay for its size.
    pub(super) error: Option<Box<StorageRoleFailure>>,
    pub(super) report: MetadataApplicationReport,
}

impl MetadataApplicationFailure {
    #[must_use]
    pub const fn family(&self) -> MetadataFamily {
        self.family
    }
    #[must_use]
    pub fn storage_error(&self) -> Option<&StorageRoleFailure> {
        self.error.as_deref()
    }
    #[must_use]
    pub const fn report(&self) -> &MetadataApplicationReport {
        &self.report
    }
}

impl fmt::Display for MetadataApplicationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "metadata {:?} application failed", self.family)
    }
}

impl std::error::Error for MetadataApplicationFailure {}
