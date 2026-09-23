//! Why metadata could not be planned or applied, in terms a caller can act on.

use std::fmt;

use super::{MetadataApplicationReport, MetadataFamily, MetadataPolicy, SemanticLoss};
use crate::model::{AclEncoding, FailureClass, Transience};
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

/// Why one family could not be planned, precise enough to act on: which end lacks it, what the
/// source reported, or which loss the policy refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RefusalCause {
    /// The source cannot read this family at all.
    SourceCannotObserve,
    /// The source tried to read it and failed.
    SourceObservationFailed {
        class: FailureClass,
        transience: Transience,
    },
    /// The source was not asked to read it, so there is nothing to copy.
    SourceDidNotObserve,
    /// The destination cannot store this family.
    DestinationCannotStore,
    /// Both ends store ACLs, in different encodings; carrying one needs a mapping that is never
    /// attempted.
    EncodingsDiffer {
        source: AclEncoding,
        destination: AclEncoding,
    },
    /// The destination needs owner/group mapped to its own principals and no mapper was given.
    PrincipalMapperMissing,
    /// The principal mapper could not map owner/group.
    PrincipalMappingFailed,
    /// Copying would lose this, and the policy does not allow it.
    LossRejected(SemanticLoss),
}

/// Which end a refusal is about — where a caller has to look.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RefusalSide {
    /// The source lacks the family or failed to read it.
    Source,
    /// The destination cannot store the family, or not without a loss the policy refuses.
    Destination,
    /// Neither end alone: how the two ends pair (encodings, principals) or what the plan asked
    /// the source to read.
    Mapping,
}

impl RefusalCause {
    /// Which end this refusal is about.
    #[must_use]
    pub const fn side(self) -> RefusalSide {
        match self {
            Self::SourceCannotObserve | Self::SourceObservationFailed { .. } => RefusalSide::Source,
            Self::DestinationCannotStore | Self::LossRejected(_) => RefusalSide::Destination,
            Self::SourceDidNotObserve
            | Self::EncodingsDiffer { .. }
            | Self::PrincipalMapperMissing
            | Self::PrincipalMappingFailed => RefusalSide::Mapping,
        }
    }

    const fn kind(self) -> MetadataPlanErrorKind {
        match self {
            Self::SourceCannotObserve | Self::DestinationCannotStore => {
                MetadataPlanErrorKind::Unsupported
            }
            Self::SourceObservationFailed { .. } => MetadataPlanErrorKind::ObservationFailed,
            Self::SourceDidNotObserve => MetadataPlanErrorKind::ObservationRequired,
            Self::EncodingsDiffer { .. } | Self::PrincipalMapperMissing => {
                MetadataPlanErrorKind::ExternalMappingRequired
            }
            Self::PrincipalMappingFailed => MetadataPlanErrorKind::PrincipalMappingFailed,
            Self::LossRejected(_) => MetadataPlanErrorKind::KnownLossRejected,
        }
    }
}

impl fmt::Display for RefusalCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceCannotObserve => formatter.write_str("the source cannot read it"),
            Self::SourceObservationFailed { class, transience } => write!(
                formatter,
                "reading it from the source failed ({class:?}, {transience:?})"
            ),
            Self::SourceDidNotObserve => formatter.write_str("the source did not read it"),
            Self::DestinationCannotStore => formatter.write_str("the destination cannot store it"),
            Self::EncodingsDiffer {
                source,
                destination,
            } => write!(
                formatter,
                "the source holds a {source:?} ACL and the destination stores {destination:?}; \
                 converting between them is never attempted"
            ),
            Self::PrincipalMapperMissing => formatter.write_str(
                "the destination needs owner/group mapped to its own principals and no mapper \
                 was given",
            ),
            Self::PrincipalMappingFailed => {
                formatter.write_str("mapping owner/group to destination principals failed")
            }
            Self::LossRejected(loss) => write!(
                formatter,
                "copying it would lose {}, and the policy does not allow that",
                loss_text(*loss)
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataPlanError {
    pub(super) family: MetadataFamily,
    pub(super) policy: MetadataPolicy,
    pub(super) cause: RefusalCause,
}

impl MetadataPlanError {
    pub(crate) const fn new(
        family: MetadataFamily,
        policy: MetadataPolicy,
        cause: RefusalCause,
    ) -> Self {
        Self {
            family,
            policy,
            cause,
        }
    }

    #[must_use]
    pub const fn family(self) -> MetadataFamily {
        self.family
    }
    #[must_use]
    pub const fn kind(self) -> MetadataPlanErrorKind {
        self.cause.kind()
    }
    /// Why the family was refused.
    #[must_use]
    pub const fn cause(self) -> RefusalCause {
        self.cause
    }
    /// The policy the family was planned under when it was refused.
    #[must_use]
    pub const fn policy(self) -> MetadataPolicy {
        self.policy
    }
}

impl fmt::Display for MetadataPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} ({:?}): {}",
            family_name(self.family),
            self.policy,
            self.cause
        )
    }
}

/// The family as a person would name it.
const fn family_name(family: MetadataFamily) -> &'static str {
    match family {
        MetadataFamily::Acl => "ACL",
        MetadataFamily::Xattrs => "extended attributes",
        MetadataFamily::Tags => "object tags",
        MetadataFamily::OwnershipMode => "owner/group/mode",
        MetadataFamily::Timestamps => "timestamps",
    }
}

const fn loss_text(loss: SemanticLoss) -> &'static str {
    match loss {
        SemanticLoss::AclDropped => "the ACL",
        SemanticLoss::XattrsDropped => "the extended attributes",
        SemanticLoss::TagsDropped => "the object tags",
        SemanticLoss::OwnershipModeDropped => "owner, group and mode",
        SemanticLoss::OwnerAndGroupDropped => "owner and group (mode is kept)",
        SemanticLoss::TimestampPrecisionReduced => "timestamp precision",
        SemanticLoss::AccessedTimestampDropped => "the access time",
        SemanticLoss::ModifiedTimestampDropped => "the modification time",
        SemanticLoss::CreatedTimestampDropped => "the creation time",
    }
}

impl std::error::Error for MetadataPlanError {}

/// What stopped metadata application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ApplicationFailureKind {
    /// The destination declined this one write.
    Refused,
    /// The batch failed as a whole — its stage could not be opened, or the barrier that makes it
    /// durable failed — so nothing in it is durable.
    BatchFailed,
    /// The destination session was lost.
    SessionLost,
    /// The copy was cancelled.
    Cancelled,
}

impl ApplicationFailureKind {
    /// What stopped application, in the order a caller has to act on it: a cancellation first,
    /// then a batch that is not durable, then a lost session, and only then one declined write.
    /// A batch that failed because its session was lost reads as `BatchFailed`; the session
    /// failure itself is still in `storage_error()`.
    pub(super) fn of(
        error: Option<&StorageRoleFailure>,
        cancelled: bool,
        whole_batch: bool,
    ) -> Self {
        if cancelled {
            Self::Cancelled
        } else if whole_batch {
            Self::BatchFailed
        } else if matches!(error, Some(StorageRoleFailure::Session(_))) {
            Self::SessionLost
        } else {
            Self::Refused
        }
    }
}

impl fmt::Display for ApplicationFailureKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Refused => "the destination refused the write",
            Self::BatchFailed => {
                "the destination failed the batch as a whole (stage open or durability barrier); \
                 nothing in it is durable"
            }
            Self::SessionLost => "the destination session was lost",
            Self::Cancelled => "the copy was cancelled",
        })
    }
}

/// One family that could not be applied.
#[derive(Debug)]
pub struct FamilyFailure {
    pub(super) family: MetadataFamily,
    pub(super) kind: ApplicationFailureKind,
    /// Boxed so the whole `Result` stays small: the role failure carries paths and diagnostic
    /// strings, and every successful application would otherwise pay for its size.
    pub(super) error: Option<Box<StorageRoleFailure>>,
}

impl FamilyFailure {
    /// The family that could not be applied.
    #[must_use]
    pub const fn family(&self) -> MetadataFamily {
        self.family
    }
    /// How it failed.
    #[must_use]
    pub const fn kind(&self) -> ApplicationFailureKind {
        self.kind
    }
    /// What the storage reported; `None` when the copy was cancelled.
    #[must_use]
    pub fn storage_error(&self) -> Option<&StorageRoleFailure> {
        self.error.as_deref()
    }
}

impl fmt::Display for FamilyFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "applying {} failed: {}",
            family_name(self.family),
            self.kind
        )?;
        if let Some(error) = &self.error {
            write!(formatter, " — {}", role_failure_text(error))?;
        }
        Ok(())
    }
}

/// Every family of one item that could not be applied. Application goes on past a family the
/// destination refuses, so the item fails once with every reason rather than with the first; only
/// a cancellation, a lost session, or a batch that failed as a whole stops it early, and that one
/// is always last.
#[derive(Debug)]
pub struct MetadataApplicationFailure {
    /// Never empty.
    pub(super) failures: Vec<FamilyFailure>,
    /// In a failure report `Applied` means written to the stage, not durable: the item fails and
    /// is not published, so nothing it wrote is claimed durable.
    pub(super) report: MetadataApplicationReport,
}

impl MetadataApplicationFailure {
    /// Every family that failed, in the order they were applied.
    #[must_use]
    pub fn failures(&self) -> &[FamilyFailure] {
        &self.failures
    }
    /// The family of the most severe failure. A failure that stops application (cancellation, a
    /// lost session, a whole batch) is always the last one; when every failure was a refusal, it
    /// is the last refusal.
    #[must_use]
    pub fn family(&self) -> MetadataFamily {
        self.last().family
    }
    /// The most severe failure's kind: `Cancelled` exactly when the copy was cancelled, whatever
    /// came before it.
    #[must_use]
    pub fn kind(&self) -> ApplicationFailureKind {
        self.last().kind
    }
    /// The most severe failure's storage error. Earlier refusals keep theirs in `failures()` — a
    /// retry decision has to look at all of them.
    #[must_use]
    pub fn storage_error(&self) -> Option<&StorageRoleFailure> {
        self.last().storage_error()
    }
    #[must_use]
    pub const fn report(&self) -> &MetadataApplicationReport {
        &self.report
    }

    fn last(&self) -> &FamilyFailure {
        self.failures
            .last()
            .unwrap_or_else(|| unreachable!("a metadata application failure lists at least one"))
    }
}

impl fmt::Display for MetadataApplicationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, failure) in self.failures.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{failure}")?;
        }
        Ok(())
    }
}

/// No `source()`: the Display above already carries the storage failure and its diagnostic, and
/// a chain printer would show them twice. `storage_error()` reaches it as a value.
impl std::error::Error for MetadataApplicationFailure {}

/// A role failure with the adapter's diagnostic, which the failure's own Display leaves out. The
/// diagnostic is redacted by the adapter's contract, so it is safe to show.
pub(crate) fn role_failure_text(error: &StorageRoleFailure) -> String {
    let diagnostic = match error {
        StorageRoleFailure::Entry(entry) => entry.diagnostic(),
        StorageRoleFailure::Session(session) => session.diagnostic(),
    };
    format!("{error}: {diagnostic}")
}
