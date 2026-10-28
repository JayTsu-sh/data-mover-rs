//! Observe a real directory marker at its raw object key, independently of its logical directory.

use super::{Listed, assemble, confirm_vanished, internal, metadata_without_io, observe_io};
use crate::model::{ObservationPlan, ObservedEntry};
use crate::storage::{Metadata, Namespace, SourceDescriptor, StorageRoleFailure};

fn attach(
    metadata: &dyn Metadata,
    directory: SourceDescriptor,
    marker: ObservedEntry,
    plan: ObservationPlan,
) -> Result<ObservedEntry, StorageRoleFailure> {
    let facts = metadata_without_io(metadata, plan, &directory).ok_or_else(internal)??;
    assemble(directory, facts, None)?
        .with_directory_marker_object(Some(Box::new(marker)))
        .map_err(|_| internal())
}

pub(super) fn listed(
    metadata: &dyn Metadata,
    mut directory: SourceDescriptor,
    marker: SourceDescriptor,
    plan: ObservationPlan,
) -> Listed {
    match metadata_without_io(metadata, plan, &marker) {
        Some(Ok(facts)) => Listed::Observed(
            assemble(marker, facts, None)
                .and_then(|marker| attach(metadata, directory, marker, plan))
                .map(Box::new),
        ),
        Some(Err(failure)) => Listed::Observed(Err(failure)),
        None => {
            directory.listing.directory_marker = Some(Box::new(marker));
            Listed::Unobserved(Box::new(directory))
        }
    }
}

pub(super) async fn observe(
    namespace: &dyn Namespace,
    metadata: &dyn Metadata,
    directory: SourceDescriptor,
    marker: SourceDescriptor,
    plan: ObservationPlan,
) -> Result<ObservedEntry, StorageRoleFailure> {
    match observe_io(namespace, metadata, &marker, plan).await {
        Ok((facts, target)) => {
            let object = assemble(marker, facts, target)?;
            attach(metadata, directory, object, plan)
        }
        Err(failure) => Err(confirm_vanished(namespace, &marker.path, failure).await),
    }
}
