//! Compare described entries while keeping optional metadata and content checks independent.

use futures::join;
use tokio_util::sync::CancellationToken;

use super::permissions::compare_permissions;
use super::{
    AclComparisonScope, Endpoint, IntegrityFailure, IntegrityInput, IntegrityMode,
    IntegrityOptions, IntegrityPlan, IntegrityReport, IntegrityRequest, IntegritySide,
    compare_content, compare_facts, compare_modified, endpoint, missing, permissions, reconcile,
};
use crate::model::EntryKind;
use crate::storage::SourceDescriptor;

/// Compares one entry on two connected backends, selected by path or observation on each side.
///
/// Absence on either side is a reported difference, not a failure; only a role failure that
/// prevents the comparison is an error. An observed entry is revalidated against the connected
/// backend; a stale or foreign observation is a failure on that side. Observed versioned entries
/// select their recorded current version. Cancellation surfaces as the backend's own `Cancelled`
/// role failure on the side that observed it.
///
/// # Errors
/// Returns the first role failure, attributed to the side that produced it, or a capability
/// failure when either side lends no read-source or metadata role.
pub async fn compare<S, D>(
    request: IntegrityRequest<S, D>,
) -> Result<IntegrityReport, IntegrityFailure>
where
    S: Into<IntegrityInput>,
    D: Into<IntegrityInput>,
{
    let source = endpoint(
        IntegritySide::Source,
        &request.source,
        request.source_path.into(),
    )?;
    let destination = endpoint(
        IntegritySide::Destination,
        &request.destination,
        request.destination_path.into(),
    )?;
    let (source_entry, destination_entry) =
        reconcile(join!(source.describe(), destination.describe()))?;
    let mut report = IntegrityReport {
        acl_scope: None,
        differences: Vec::new(),
        ignored: Vec::new(),
        compared_bytes: 0,
        digests: None,
    };
    let (Some(source_entry), Some(destination_entry)) = (&source_entry, &destination_entry) else {
        report
            .differences
            .extend(missing(source_entry.is_none(), destination_entry.is_none()));
        return Ok(report);
    };
    compare_entries(
        &source,
        &destination,
        (source_entry, destination_entry),
        request.plan,
        request.options,
        &request.cancel,
        &mut report,
    )
    .await?;
    Ok(report)
}

async fn compare_entries(
    source: &Endpoint,
    destination: &Endpoint,
    entries: (&SourceDescriptor, &SourceDescriptor),
    plan: IntegrityPlan,
    options: IntegrityOptions,
    cancel: &CancellationToken,
    report: &mut IntegrityReport,
) -> Result<(), IntegrityFailure> {
    compare_facts(entries.0, entries.1, &mut report.differences);
    compare_metadata(source, destination, entries, plan, report).await?;
    compare_modified(
        source,
        destination,
        entries,
        options,
        &mut report.differences,
        &mut report.ignored,
    )
    .await?;
    if options.mode == IntegrityMode::Content
        && entries.0.kind == EntryKind::File
        && entries.1.kind == EntryKind::File
    {
        compare_content(source, destination, entries, cancel, report).await?;
    }
    Ok(())
}

async fn compare_metadata(
    source: &Endpoint,
    destination: &Endpoint,
    entries: (&SourceDescriptor, &SourceDescriptor),
    plan: IntegrityPlan,
    report: &mut IntegrityReport,
) -> Result<(), IntegrityFailure> {
    report.acl_scope = if plan.acl || plan.acl_required || plan.acl_equivalent || plan.xattrs {
        permissions::compare_optional_metadata(
            source,
            destination,
            entries.0,
            entries.1,
            plan,
            &mut report.differences,
            &mut report.ignored,
        )
        .await?
    } else {
        None
    };
    compare_permissions(
        source,
        destination,
        entries.0,
        entries.1,
        matches!(
            report.acl_scope,
            Some(AclComparisonScope::CurrentRegularFileAccess)
        ),
        &mut report.differences,
        &mut report.ignored,
    )
    .await?;
    Ok(())
}
