use std::sync::Arc;

use async_trait::async_trait;

use super::metadata::{CifsInlineMetadata, timestamp};
use super::source::{
    CifsSourceFacts, classify, descriptor_from_facts, entry_failure, entry_failure_with_transience,
    is_directory_not_empty, is_not_found, smb_attributes_to_mode,
};
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, FailureClass, Operation, StoragePath,
    SymlinkTarget, TimestampMetadata,
};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::{
    ListRequest, ListingPage, ListingStream, Namespace, NamespaceCapabilities, SourceDescriptor,
    StorageRoleFailure,
};

/// Protocol verbs behind the CIFS namespace role.
///
/// There is no link verb: the smb-rs domain facade does not expose reparse points and the
/// capability matrix marks CIFS symlinks unsupported, so `ReadLink` fails typed preflight.
#[async_trait]
pub(super) trait CifsNamespaceProtocol: Send + Sync {
    async fn stat(&self, path: &StoragePath) -> smb_domain::Result<CifsSourceFacts>;
    /// Lists direct children with the timestamps the `QUERY_DIRECTORY` records already carry.
    async fn list(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<Vec<(StoragePath, CifsInlineMetadata)>>;
    /// Creates one directory; an existing entry surfaces as `STATUS_OBJECT_NAME_COLLISION`.
    async fn create_directory(&self, path: &StoragePath) -> smb_domain::Result<()>;
    /// Deletes one file or one empty directory.
    async fn remove(&self, path: &StoragePath) -> smb_domain::Result<()>;
    /// Renames one file or directory, replacing an existing destination like the NFS and
    /// HDFS roles (servers only replace what NTFS semantics allow).
    async fn rename_entry(&self, from: &StoragePath, to: &StoragePath) -> smb_domain::Result<()>;
}

#[derive(Clone)]
pub(super) struct CifsNamespace {
    protocol: Arc<dyn CifsNamespaceProtocol>,
    identity: BackendIdentity,
}

impl CifsNamespace {
    pub(super) fn new<P>(protocol: Arc<P>, identity: BackendIdentity) -> Self
    where
        P: CifsNamespaceProtocol + 'static,
    {
        Self { protocol, identity }
    }

    async fn stat_entry(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let facts = self
            .protocol
            .stat(path)
            .await
            .map_err(|error| classify(path, Operation::Observe, &error))?;
        descriptor_from_facts(&self.identity, path, &facts, Operation::Observe)
    }

    /// Reads one whole directory; the protocol opens, reads and closes the directory handle
    /// inside this one call, so nothing stays open between pages (true paging comes with C9).
    async fn list_whole(&self, path: &StoragePath) -> Result<ListingPage, StorageRoleFailure> {
        let facts = self
            .protocol
            .list(path)
            .await
            .map_err(|error| classify(path, Operation::Traverse, &error))?;
        // Transfer artifacts never appear in a listing (ADR-0006 C3b). A child that cannot be
        // described is reported on its own, beside the siblings that could be: the directory
        // itself was listed, so failing it whole would drop them.
        let mut entries = Vec::with_capacity(facts.len());
        let mut failures = Vec::new();
        let mut hidden = 0_u32;
        for (child, inline) in facts {
            if is_artifact_path(child.as_str()) {
                tracing::trace!("[CIFS] listing skips transfer artifact {}", child.as_str());
                hidden = hidden.saturating_add(1);
                continue;
            }
            match self.describe_listed(&child, &inline) {
                Ok(descriptor) => entries.push(descriptor),
                Err(StorageRoleFailure::Entry(failure)) => {
                    failures.push(undescribed(&child, &failure));
                }
                Err(session @ StorageRoleFailure::Session(_)) => return Err(session),
            }
        }
        Ok(ListingPage::from_descriptors(
            path, entries, failures, hidden,
        ))
    }

    /// One listed child with the timestamps and mode its `QUERY_DIRECTORY` record carries.
    fn describe_listed(
        &self,
        child: &StoragePath,
        inline: &CifsInlineMetadata,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        let descriptor =
            descriptor_from_facts(&self.identity, child, &inline.facts, Operation::Traverse)?
                .with_inline_timestamps(TimestampMetadata {
                    accessed: timestamp(inline.accessed),
                    modified: timestamp(inline.modified),
                    created: timestamp(inline.created),
                });
        Ok(match inline.readonly {
            Some(readonly) => descriptor.with_inline_mode(smb_attributes_to_mode(
                inline.facts.kind == EntryKind::Directory,
                readonly,
            )),
            None => descriptor,
        })
    }

    /// Deletes one file or empty directory. A directory that only transfer artifacts keep
    /// non-empty — empty as seen through `List` — is emptied of them and deleted: the caller asked
    /// for the directory to go, and hidden artifacts must not block that. Anything visible left in
    /// it, or a reparse point (link, junction) anywhere among the artifacts, keeps the original
    /// conflict and nothing is removed. An artifact that vanishes meanwhile (a writer published it)
    /// is fine: the final remove decides. Every failure names `path`, the entry the caller asked
    /// for, never a hidden one.
    async fn delete_entry(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        let error = match self.protocol.remove(path).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        let refused = classify(path, Operation::Namespace, &error);
        if !is_directory_not_empty(&error) {
            return Err(refused);
        }
        let blame = |error: &smb_domain::Error| classify(path, Operation::Namespace, error);
        let children = self
            .protocol
            .list(path)
            .await
            .map_err(|error| blame(&error))?;
        if !children
            .iter()
            .all(|(child, inline)| is_artifact_path(child.as_str()) && !inline.reparse_point)
        {
            return Err(refused);
        }
        let Some(removals) = self
            .artifact_removals(children)
            .await
            .map_err(|error| blame(&error))?
        else {
            return Err(refused);
        };
        for artifact in removals {
            match self.protocol.remove(&artifact).await {
                Err(error) if !is_not_found(&error) => return Err(blame(&error)),
                _ => {}
            }
        }
        self.protocol
            .remove(path)
            .await
            .map_err(|error| blame(&error))
    }

    /// Every artifact to remove, children before their directory; `None` when a reparse point
    /// lies anywhere below, which is never descended into. Nothing is removed while planning.
    async fn artifact_removals(
        &self,
        children: Vec<(StoragePath, CifsInlineMetadata)>,
    ) -> smb_domain::Result<Option<Vec<StoragePath>>> {
        let mut removals = Vec::new();
        let mut pending = children
            .into_iter()
            .map(|(child, inline)| (child, inline.facts.kind, false))
            .collect::<Vec<_>>();
        while let Some((path, kind, expanded)) = pending.pop() {
            if expanded || kind != EntryKind::Directory {
                removals.push(path);
                continue;
            }
            let listed = match self.protocol.list(&path).await {
                Ok(listed) => listed,
                Err(error) if is_not_found(&error) => continue,
                Err(error) => return Err(error),
            };
            if listed.iter().any(|(_, inline)| inline.reparse_point) {
                return Ok(None);
            }
            pending.push((path, kind, true));
            pending.extend(
                listed
                    .into_iter()
                    .map(|(child, inline)| (child, inline.facts.kind, false)),
            );
        }
        Ok(Some(removals))
    }
}

/// A listed child's failure, saying that the listing could not describe it.
fn undescribed(child: &StoragePath, failure: &EntryOperationFailure) -> EntryOperationFailure {
    match entry_failure_with_transience(
        child,
        Operation::Traverse,
        failure.class(),
        failure.transience(),
        "CIFS listing could not describe the entry",
    ) {
        StorageRoleFailure::Entry(failure) => failure,
        StorageRoleFailure::Session(_) => unreachable!("an entry failure is built"),
    }
}

/// Transfer artifacts are not addressable through the namespace (ADR-0006 C3b), as on Local and
/// NFS: listings hide them, so no verb may reach them either.
fn refuse_artifacts(paths: &[&StoragePath]) -> Result<(), StorageRoleFailure> {
    match paths.iter().find(|path| is_artifact_path(path.as_str())) {
        Some(path) => Err(entry_failure(
            path,
            Operation::Namespace,
            FailureClass::InvalidInput,
        )),
        None => Ok(()),
    }
}

#[async_trait]
impl Namespace for CifsNamespace {
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        refuse_artifacts(&[path])?;
        self.stat_entry(path).await
    }

    /// The producer task owns the listing to its end, so a dropped stream never abandons an open
    /// directory handle: the protocol call inside it always runs to its close.
    fn list(&self, request: ListRequest) -> ListingStream {
        if let Err(failure) = refuse_artifacts(&[&request.path]) {
            return ListingStream::failed(request.path, failure);
        }
        let namespace = self.clone();
        ListingStream::from_producer(request.path.clone(), move |sink| async move {
            let listing = namespace.list_whole(&request.path).await;
            sink.deliver(listing, &request).await;
        })
    }

    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        refuse_artifacts(&[path])?;
        Err(entry_failure(
            path,
            Operation::Observe,
            FailureClass::Unsupported,
        ))
    }

    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        refuse_artifacts(&[path])?;
        self.protocol
            .create_directory(path)
            .await
            .map_err(|error| classify(path, Operation::Namespace, &error))
    }

    async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        refuse_artifacts(&[path])?;
        self.delete_entry(path).await
    }

    async fn rename(&self, from: &StoragePath, to: &StoragePath) -> Result<(), StorageRoleFailure> {
        refuse_artifacts(&[from, to])?;
        self.protocol
            .rename_entry(from, to)
            .await
            .map_err(|error| classify(from, Operation::Namespace, &error))
    }

    /// Name semantics stay unknown until the volume's `FILE_CASE_SENSITIVE_SEARCH` attribute is
    /// queried at connect; unknown is treated as insensitive.
    fn capabilities(&self) -> NamespaceCapabilities {
        NamespaceCapabilities {
            symlinks: false,
            ..NamespaceCapabilities::BASIC
        }
    }
}
