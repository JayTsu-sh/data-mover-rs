//! What a Local listing already answers: the listing took the same kind of `lstat` the
//! observation role takes, so ownership and timestamps of a listed child need no second one.
//! Only an ACL or xattrs the plan really wants take a call.

#[cfg(not(unix))]
use super::additional_only;
use super::failure;
use super::{inline_value, optional_not_applicable};
use crate::model::{
    EntryKind, FailureClass, MetadataObservation, MetadataObservations, ObservationMode,
    ObservationPlan, OwnershipMode,
};
use crate::storage::{InlineOwnership, SourceDescriptor, StorageRoleFailure};

/// Exactly what the role's `observe` reports for the same `lstat`, or `None` when the plan asks
/// for something only a call can give.
pub(super) fn observe_listed(
    descriptor: &SourceDescriptor,
    plan: ObservationPlan,
) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
    let symlink = descriptor.kind == EntryKind::Symlink;
    let acl = listed_optional(plan.acl(), symlink)?;
    let xattrs = listed_optional(plan.xattrs(), symlink)?;
    let ownership = listed_ownership(plan.ownership_mode(), descriptor.inline_ownership)?;
    let timestamps = match plan.timestamps() {
        ObservationMode::Omit => MetadataObservation::NotRequested,
        mode => inline_value(mode, descriptor.inline_timestamps?),
    };
    let tags = optional_not_applicable(plan.tags());
    Some(
        MetadataObservations::new(acl, xattrs, tags, ownership, timestamps)
            .map_err(|_| failure(&descriptor.path, FailureClass::Internal)),
    )
}

/// ACL or xattrs when answering takes no call: what `optional_observation` and the symlink rule
/// in `observe_acl` / `observe_xattrs` report without touching the file.
fn listed_optional<T>(mode: ObservationMode, symlink: bool) -> Option<MetadataObservation<T>> {
    if symlink {
        return Some(optional_not_applicable(mode));
    }
    match mode {
        ObservationMode::Omit => Some(MetadataObservation::NotRequested),
        ObservationMode::InlineOnly => Some(MetadataObservation::Unsupported),
        ObservationMode::BestEffort | ObservationMode::Required => None,
    }
}

/// Ownership as `inline_ownership` reads it from the listing's `lstat`.
#[cfg(unix)]
fn listed_ownership(
    mode: ObservationMode,
    listed: Option<InlineOwnership>,
) -> Option<MetadataObservation<OwnershipMode>> {
    match (mode, listed) {
        (ObservationMode::Omit, _) => Some(MetadataObservation::NotRequested),
        (_, Some(InlineOwnership::Known(value))) => Some(inline_value(mode, value)),
        (_, Some(InlineOwnership::Unmapped) | None) => None,
    }
}

/// Off Unix there is no numeric ownership to read, listed or not.
#[cfg(not(unix))]
#[allow(clippy::unnecessary_wraps)]
fn listed_ownership(
    mode: ObservationMode,
    _listed: Option<InlineOwnership>,
) -> Option<MetadataObservation<OwnershipMode>> {
    Some(additional_only(mode))
}
