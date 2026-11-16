//! Actor-visible fixed storage and payload accounting, independent of observation slots.
//! This is a logical byte reservation, not allocator/RSS or opaque producer-buffer accounting.

use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use super::super::DirectoryWork;
use super::super::observe::ObservationError;
use crate::model::observation::PrivateBackendEntryFacts;
use crate::model::{
    EntryOperationFailure, MetadataObservationParts, MetadataObservations, ObjectHistory,
    ObservedEntry, PermissionFacts, PrincipalIdentity,
};
use crate::storage::{ListingPage, SourceDescriptor, StorageRoleFailure};
use crate::traversal::{ObservationFailure, TraversalItem, TraversalTerminalFailure};

pub(in crate::traversal::storage) struct Resources {
    limit: usize,
    held: Mutex<usize>,
}

pub(super) struct Charge {
    resources: Arc<Resources>,
    bytes: usize,
}

pub(super) struct Held<T> {
    pub(super) value: T,
    pub(super) charge: Charge,
}

/// A page's charge follows its detached observer until results actually leave that task.
#[derive(Clone)]
pub(in crate::traversal::storage) struct PageCharge(Arc<Mutex<Charge>>);

impl Resources {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            held: Mutex::new(0),
        })
    }

    pub(super) fn reserve(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<Charge, TraversalTerminalFailure> {
        let mut charge = Charge {
            resources: Arc::clone(self),
            bytes: 0,
        };
        charge.resize(bytes)?;
        Ok(charge)
    }

    pub(super) fn hold<T>(
        self: &Arc<Self>,
        value: T,
        bytes: usize,
    ) -> Result<Held<T>, TraversalTerminalFailure> {
        Ok(Held {
            value,
            charge: self.reserve(bytes)?,
        })
    }

    #[cfg(test)]
    pub(super) fn held(&self) -> usize {
        *self.held.lock()
    }
}

impl Charge {
    pub(super) fn resize(&mut self, bytes: usize) -> Result<(), TraversalTerminalFailure> {
        let mut held = self.resources.held.lock();
        let required = held.saturating_sub(self.bytes).saturating_add(bytes);
        if required > self.resources.limit {
            return Err(TraversalTerminalFailure::ConsumerCompletionBudgetExceeded {
                limit: self.resources.limit,
                required,
            });
        }
        *held = required;
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        let mut held = self.resources.held.lock();
        debug_assert!(*held >= self.bytes);
        *held = held.saturating_sub(self.bytes);
    }
}

impl PageCharge {
    pub(super) fn new(charge: Charge) -> Self {
        Self(Arc::new(Mutex::new(charge)))
    }

    pub(super) fn resize(&self, bytes: usize) -> Result<(), TraversalTerminalFailure> {
        self.0.lock().resize(bytes)
    }

    pub(super) fn take<T>(
        &self,
        value: T,
        bytes: usize,
    ) -> Result<Held<T>, TraversalTerminalFailure> {
        let mut charge = self.0.lock();
        charge.bytes = charge
            .bytes
            .checked_sub(bytes)
            .ok_or(TraversalTerminalFailure::Internal)?;
        Ok(Held {
            value,
            charge: Charge {
                resources: Arc::clone(&charge.resources),
                bytes,
            },
        })
    }

    pub(in crate::traversal::storage) fn observation(
        &self,
        result: Result<&ObservedEntry, &ObservationError>,
    ) -> Result<(), TraversalTerminalFailure> {
        let bytes = match result {
            Ok(entry) => entry_bytes(entry),
            Err(error) => observation_error_bytes(error),
        };
        let mut charge = self.0.lock();
        let required = charge.bytes.saturating_add(bytes);
        charge.resize(required)
    }
}

fn sum(values: impl Iterator<Item = usize>) -> usize {
    values.fold(0, usize::saturating_add)
}

fn permissions(value: Option<&PermissionFacts>) -> usize {
    value.map_or(0, |facts| {
        size_of::<PermissionFacts>().saturating_add(sum([&facts.owner, &facts.group]
            .into_iter()
            .map(|principal| {
                if let Some(PrincipalIdentity::Named(name)) = principal {
                    name.len()
                } else {
                    0
                }
            })))
    })
}

fn metadata(value: &MetadataObservations) -> usize {
    // Fixed cold-state reserve avoids depending on model's private packed representation.
    let acl = value.acl().value().map_or(0, |acl| {
        acl.access()
            .map_or(0, <[u8]>::len)
            .saturating_add(acl.default_acl().map_or(0, <[u8]>::len))
    });
    let attributes = value.xattrs().value().map_or(0, |values| {
        values
            .capacity()
            .saturating_mul(size_of::<crate::model::ExtendedAttribute>())
            .saturating_add(sum(values
                .iter()
                .map(|item| item.name().len().saturating_add(item.value().len()))))
    });
    let tags = value.tags().value().map_or(0, |values| {
        values
            .capacity()
            .saturating_mul(size_of::<crate::model::ObjectTag>())
            .saturating_add(sum(values
                .iter()
                .map(|item| item.key().len().saturating_add(item.value().len()))))
    });
    size_of::<MetadataObservationParts>()
        .saturating_add(acl)
        .saturating_add(attributes)
        .saturating_add(tags)
}

fn history(value: Option<&ObjectHistory>) -> usize {
    value.map_or(0, |history| {
        size_of::<ObjectHistory>().saturating_add(sum(history.records().iter().map(|record| {
            size_of::<crate::model::VersionRecord>()
                .saturating_add(record.version_id().map_or(0, str::len))
                .saturating_add(record.etag().map_or(0, str::len))
                .saturating_add(record.identity().stable_bytes().map_or(0, <[u8]>::len))
                .saturating_add(record.metadata().map_or(0, metadata))
        })))
    })
}

pub(super) fn entry_bytes(entry: &ObservedEntry) -> usize {
    let backend = match entry.backend_facts() {
        PrivateBackendEntryFacts::None => 0,
        PrivateBackendEntryFacts::Local(bytes)
        | PrivateBackendEntryFacts::Nfs(bytes)
        | PrivateBackendEntryFacts::Cifs(bytes)
        | PrivateBackendEntryFacts::S3(bytes)
        | PrivateBackendEntryFacts::Hdfs(bytes) => bytes.len(),
    };
    size_of::<ObservedEntry>()
        .saturating_add(entry.path().as_str().len())
        .saturating_add(
            entry
                .source_identity()
                .stable_bytes()
                .map_or(0, <[u8]>::len),
        )
        .saturating_add(
            entry
                .symlink_target()
                .map_or(0, |target| target.as_bytes().len()),
        )
        .saturating_add(backend)
        .saturating_add(metadata(entry.metadata()))
        .saturating_add(permissions(entry.permissions()))
        .saturating_add(history(entry.history()))
        .saturating_add(entry.directory_marker_object().map_or(0, entry_bytes))
}

pub(super) fn descriptor_bytes(entry: &SourceDescriptor) -> usize {
    size_of::<SourceDescriptor>()
        .saturating_add(entry.path.as_str().len())
        .saturating_add(entry.source_identity.stable_bytes().map_or(0, <[u8]>::len))
        .saturating_add(entry.backend_fact.as_ref().map_or(0, Bytes::len))
        .saturating_add(entry.content_version.as_ref().map_or(0, Bytes::len))
        .saturating_add(entry.version.version_id().map_or(0, str::len))
        .saturating_add(permissions(entry.permissions.as_deref()))
        .saturating_add(history(entry.listing.history.as_deref()))
        .saturating_add(
            entry
                .listing
                .directory_marker
                .as_deref()
                .map_or(0, descriptor_bytes),
        )
}

pub(super) fn work_bytes(work: &DirectoryWork) -> usize {
    work.path
        .as_str()
        .len()
        .saturating_add(work.hint.as_ref().map_or(0, |hint| hint.as_bytes().len()))
}

fn failure_bytes(failure: &EntryOperationFailure) -> usize {
    size_of::<EntryOperationFailure>()
        .saturating_add(failure.path().as_str().len())
        .saturating_add(failure.diagnostic().len())
}

pub(super) fn observation_failure_bytes(failure: &ObservationFailure) -> usize {
    size_of::<ObservationFailure>()
        .saturating_add(failure.path.as_str().len())
        .saturating_add(failure.version.version_id().map_or(0, str::len))
}

pub(super) fn observation_error_bytes(error: &ObservationError) -> usize {
    let failure = match &error.failure {
        StorageRoleFailure::Entry(failure) => failure_bytes(failure),
        StorageRoleFailure::Session(failure) => {
            size_of_val(failure).saturating_add(failure.diagnostic().len())
        }
    };
    failure.saturating_add(sum(error
        .observations
        .iter()
        .map(observation_failure_bytes)))
}

pub(super) fn terminal_bytes(terminal: &TraversalTerminalFailure) -> usize {
    match terminal {
        TraversalTerminalFailure::ConsumerSource(failure) => failure_bytes(failure),
        TraversalTerminalFailure::Session(failure) => {
            size_of_val(failure).saturating_add(failure.diagnostic().len())
        }
        _ => 0,
    }
}

pub(super) fn item_bytes(item: &TraversalItem) -> usize {
    match item {
        TraversalItem::Entry(entry) => entry_bytes(entry),
        TraversalItem::Failure(failure) => size_of_val(&**failure)
            .saturating_add(failure.path.as_str().len())
            .saturating_add(failure_bytes(&failure.failure)),
        TraversalItem::ObservationFailure(failure) => observation_failure_bytes(failure),
        TraversalItem::DirectoryStatistics(counts) => {
            size_of_val(&**counts).saturating_add(counts.path.as_str().len())
        }
        _ => 0, // Controlled production never constructs checkpoints, blocks or consumer envelopes here.
    }
}

pub(super) fn page_bytes(page: &ListingPage) -> usize {
    let children = sum(page.children.iter().map(|child| {
        // Move-free inspection: no snapshot encoding or cloning on the normal entry path.
        child
            .name()
            .len()
            .saturating_add(child.source_identity.stable_bytes().map_or(0, <[u8]>::len))
            .saturating_add(child.backend_fact.as_ref().map_or(0, Bytes::len))
            .saturating_add(child.content_version.as_ref().map_or(0, Bytes::len))
            .saturating_add(child.version.version_id().map_or(0, str::len))
            .saturating_add(permissions(child.permissions.as_deref()))
            .saturating_add(history(child.listing.history.as_deref()))
            .saturating_add(
                child
                    .listing
                    .directory_marker
                    .as_deref()
                    .map_or(0, descriptor_bytes),
            )
            .saturating_add(child.descent_hint().map_or(0, |hint| hint.as_bytes().len()))
    }));
    let failures = sum(page.failures.iter().map(|child| {
        child
            .name
            .as_ref()
            .map_or(0, String::len)
            .saturating_add(child.raw_name.as_ref().map_or(0, Bytes::len))
            .saturating_add(failure_bytes(&child.failure))
    }));
    size_of::<ListingPage>()
        .saturating_add(
            page.children
                .capacity()
                .saturating_mul(size_of::<crate::storage::ListedChild>()),
        )
        .saturating_add(
            page.failures
                .capacity()
                .saturating_mul(size_of::<crate::storage::ChildFailure>()),
        )
        .saturating_add(children)
        .saturating_add(failures)
}
