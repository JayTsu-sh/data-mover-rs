use std::sync::Arc;

use async_trait::async_trait;

use super::metadata::{CifsInlineMetadata, timestamp};
use super::source::{
    CifsSourceFacts, classify, descriptor_from_facts, entry_failure, is_directory_not_empty,
    is_not_found, smb_attributes_to_mode,
};
use crate::model::TimestampMetadata;
use crate::model::{BackendIdentity, EntryKind, FailureClass, Operation, StoragePath};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::{Namespace, NamespaceRequest, NamespaceResult, StorageRoleFailure};

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

    async fn stat(&self, path: &StoragePath) -> Result<NamespaceResult, StorageRoleFailure> {
        let facts = self
            .protocol
            .stat(path)
            .await
            .map_err(|error| classify(path, Operation::Observe, &error))?;
        let entry = descriptor_from_facts(&self.identity, path, &facts, Operation::Observe)?;
        Ok(NamespaceResult::Entries(vec![entry]))
    }

    async fn list(&self, path: &StoragePath) -> Result<NamespaceResult, StorageRoleFailure> {
        let facts = self
            .protocol
            .list(path)
            .await
            .map_err(|error| classify(path, Operation::Traverse, &error))?;
        // Transfer artifacts never appear in a listing (ADR-0006 C3b).
        let entries = facts
            .into_iter()
            .filter(|(child, _)| {
                let artifact = is_artifact_path(child.as_str());
                if artifact {
                    tracing::trace!("[CIFS] listing skips transfer artifact {}", child.as_str());
                }
                !artifact
            })
            .map(|(child, inline)| {
                descriptor_from_facts(&self.identity, &child, &inline.facts, Operation::Traverse)
                    .map(|descriptor| {
                        let descriptor = descriptor.with_inline_timestamps(TimestampMetadata {
                            accessed: timestamp(inline.accessed),
                            modified: timestamp(inline.modified),
                            created: timestamp(inline.created),
                        });
                        match inline.readonly {
                            Some(readonly) => descriptor.with_inline_mode(smb_attributes_to_mode(
                                inline.facts.kind == EntryKind::Directory,
                                readonly,
                            )),
                            None => descriptor,
                        }
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(NamespaceResult::Entries(entries))
    }

    /// Deletes one file or empty directory. A directory that only transfer artifacts keep
    /// non-empty — empty as seen through `List` — is emptied of them and deleted: the caller asked
    /// for the directory to go, and hidden artifacts must not block that. Anything visible left in
    /// it, or a reparse point (link, junction) anywhere among the artifacts, keeps the original
    /// conflict and nothing is removed. An artifact that vanishes meanwhile (a writer published it)
    /// is fine: the final remove decides. Every failure names `path`, the entry the caller asked
    /// for, never a hidden one.
    async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
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

/// Transfer artifacts are not addressable through the namespace (ADR-0006 C3b), as on Local and
/// NFS: listings hide them, so no verb may reach them either.
fn refuse_artifacts(request: &NamespaceRequest) -> Result<(), StorageRoleFailure> {
    let paths: &[&StoragePath] = match request {
        NamespaceRequest::Rename { from, to } => &[from, to],
        NamespaceRequest::Stat(path)
        | NamespaceRequest::List(path)
        | NamespaceRequest::ReadLink(path)
        | NamespaceRequest::CreateDirectory(path)
        | NamespaceRequest::Delete(path) => &[path],
    };
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
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        refuse_artifacts(&request)?;
        match request {
            NamespaceRequest::Stat(path) => self.stat(&path).await,
            NamespaceRequest::List(path) => self.list(&path).await,
            NamespaceRequest::ReadLink(path) => Err(entry_failure(
                &path,
                Operation::Observe,
                FailureClass::Unsupported,
            )),
            NamespaceRequest::CreateDirectory(path) => {
                self.protocol
                    .create_directory(&path)
                    .await
                    .map_err(|error| classify(&path, Operation::Namespace, &error))?;
                Ok(NamespaceResult::Completed)
            }
            NamespaceRequest::Delete(path) => {
                self.delete(&path).await?;
                Ok(NamespaceResult::Completed)
            }
            NamespaceRequest::Rename { from, to } => {
                self.protocol
                    .rename_entry(&from, &to)
                    .await
                    .map_err(|error| classify(&from, Operation::Namespace, &error))?;
                Ok(NamespaceResult::Completed)
            }
        }
    }
}
