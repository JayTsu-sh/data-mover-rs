use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;

use super::listing::{produce, read_whole};
use super::metadata::{CifsInlineMetadata, timestamp};
use super::source::{
    CifsSourceFacts, classify, descriptor_from_facts, entry_failure, is_directory_not_empty,
    is_not_found, smb_attributes_to_mode,
};
use crate::model::{
    BackendIdentity, EntryKind, FailureClass, Operation, StoragePath, SymlinkTarget,
    TimestampMetadata,
};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::listing::child_path;
use crate::storage::{
    ListRequest, ListingStream, Namespace, NamespaceCapabilities, SourceDescriptor,
    StorageRoleFailure,
};

/// Protocol verbs behind the CIFS namespace role.
///
/// There is no link verb: the smb-rs domain facade does not expose reparse points and the
/// capability matrix marks CIFS symlinks unsupported, so `ReadLink` fails typed preflight.
#[async_trait]
pub(super) trait CifsNamespaceProtocol: Send + Sync {
    async fn stat(&self, path: &StoragePath) -> smb_domain::Result<CifsSourceFacts>;
    /// Opens `path` as a directory to read its children. The caller holds the handle for as long
    /// as it reads and must [`CifsDirectory::close`] it on every exit (P7 C9).
    async fn open_directory(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<Box<dyn CifsDirectory>>;
    /// Creates one directory; an existing entry surfaces as `STATUS_OBJECT_NAME_COLLISION`.
    async fn create_directory(&self, path: &StoragePath) -> smb_domain::Result<()>;
    /// Deletes one file or one empty directory.
    async fn remove(&self, path: &StoragePath) -> smb_domain::Result<()>;
    /// Renames one file or directory, replacing an existing destination like the NFS and
    /// HDFS roles (servers only replace what NTFS semantics allow).
    async fn rename_entry(&self, from: &StoragePath, to: &StoragePath) -> smb_domain::Result<()>;
}

/// One open directory handle, read child by child.
#[async_trait]
pub(super) trait CifsDirectory: Send + Sync {
    /// The directory's direct children (never `.` or `..`) with the timestamps the
    /// `QUERY_DIRECTORY` records already carry, in the order the server returns them. A child whose
    /// name no storage path can spell is listed as [`CifsUnspelled`], beside its siblings: one such
    /// name must not fail the whole directory (P7 §8).
    ///
    /// Called once per open directory: a second call starts the scan over (and, while the first
    /// stream lives, two scans interleave on one handle); read again by opening again. The first
    /// `Err` ends a read — the stream would send another request after it. It sends a
    /// `QUERY_DIRECTORY` whenever it has handed out the last record of the one before, so dropping
    /// it part way may abandon a request already sent; smb-rs cancels that request and tolerates
    /// its late answer (smb-rs #75, #76).
    fn entries(&self) -> BoxStream<'_, smb_domain::Result<CifsListedChild>>;
    /// Closes the handle; an unconfirmed close is an error.
    async fn close(self: Box<Self>) -> smb_domain::Result<()>;
}

/// One listed child: its path and what its `QUERY_DIRECTORY` record carries, or the name the
/// server gave a child that no storage path can spell.
pub(super) type CifsListedChild = Result<(StoragePath, CifsInlineMetadata), CifsUnspelled>;

/// A listed child no storage path can spell: the joined path is too long, or smb-rs decoded the
/// UTF-16 name lossily (it holds U+FFFD), so the name does not reach the entry it came from.
#[derive(Debug)]
pub(super) struct CifsUnspelled {
    /// The name as smb-rs decoded it.
    pub(super) name: String,
}

impl CifsUnspelled {
    /// The path of child `name` of `directory`, or the name when no path spells it.
    pub(super) fn spell(directory: &StoragePath, name: &str) -> Result<StoragePath, Self> {
        // smb-rs decodes names lossily: one holding U+FFFD may not name its entry.
        Some(name)
            .filter(|name| !name.contains(char::REPLACEMENT_CHARACTER))
            .and_then(|name| child_path(directory, name).ok())
            .ok_or_else(|| Self {
                name: name.to_owned(),
            })
    }
}

/// Most listings a consumer may keep started and not read to their end at once (P7 §6.2, Q10):
/// each holds an open directory handle on the server while it waits.
const PARKED_LISTING_LIMIT: NonZeroUsize = NonZeroUsize::MIN.saturating_add(7);

#[derive(Clone)]
pub(super) struct CifsNamespace {
    pub(super) protocol: Arc<dyn CifsNamespaceProtocol>,
    pub(super) identity: BackendIdentity,
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

    /// One listed child with the timestamps and mode its `QUERY_DIRECTORY` record carries.
    pub(super) fn describe_listed(
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
        let children = read_whole(self.protocol.as_ref(), path)
            .await
            .map_err(|error| blame(&error))?;
        if !children.iter().all(|listed| {
            listed.as_ref().is_ok_and(|(child, inline)| {
                is_artifact_path(child.as_str()) && !inline.reparse_point
            })
        }) {
            return Err(refused);
        }
        let Some(removals) = self
            .artifact_removals(children.into_iter().flatten().collect())
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
            let listed = match read_whole(self.protocol.as_ref(), &path).await {
                Ok(listed) => listed,
                Err(error) if is_not_found(&error) => continue,
                Err(error) => return Err(error),
            };
            // A reparse point is never descended into, and a child no path can spell cannot be
            // removed: either keeps the directory.
            if listed.iter().any(|listed| {
                listed
                    .as_ref()
                    .ok()
                    .is_none_or(|(_, inline)| inline.reparse_point)
            }) {
                return Ok(None);
            }
            pending.push((path, kind, true));
            pending.extend(
                listed
                    .into_iter()
                    .flatten()
                    .map(|(child, inline)| (child, inline.facts.kind, false)),
            );
        }
        Ok(Some(removals))
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

    /// Streams the directory page by page (P7 C9): the producer task holds the directory handle
    /// while it reads and closes it on every exit, so a dropped stream never abandons one.
    fn list(&self, request: ListRequest) -> ListingStream {
        if let Err(failure) = refuse_artifacts(&[&request.path]) {
            return ListingStream::failed(request.path, failure);
        }
        let namespace = self.clone();
        ListingStream::from_producer(request.path.clone(), move |sink| {
            produce(namespace, request, sink)
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

    /// A listing waiting for its consumer holds its directory handle open on the server.
    fn parked_listing_limit(&self) -> Option<NonZeroUsize> {
        Some(PARKED_LISTING_LIMIT)
    }
}
