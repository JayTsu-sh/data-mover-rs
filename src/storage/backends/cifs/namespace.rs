use std::sync::Arc;

use async_trait::async_trait;

use super::metadata::{CifsInlineMetadata, timestamp};
use super::source::{CifsSourceFacts, classify, descriptor_from_facts, entry_failure};
use crate::model::TimestampMetadata;
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath};
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
        let entries = facts
            .into_iter()
            .map(|(child, inline)| {
                descriptor_from_facts(&self.identity, &child, &inline.facts, Operation::Traverse)
                    .map(|descriptor| {
                        descriptor.with_inline_timestamps(TimestampMetadata {
                            accessed: timestamp(inline.accessed),
                            modified: timestamp(inline.modified),
                            created: timestamp(inline.created),
                        })
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(NamespaceResult::Entries(entries))
    }
}

#[async_trait]
impl Namespace for CifsNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
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
                self.protocol
                    .remove(&path)
                    .await
                    .map_err(|error| classify(&path, Operation::Namespace, &error))?;
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
