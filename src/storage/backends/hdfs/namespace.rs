use std::sync::Arc;

use async_trait::async_trait;

use super::protocol::{HdfsProtocol, entry_failure};
use super::source::descriptor;
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath, Transience};
use crate::storage::{Namespace, NamespaceRequest, NamespaceResult, StorageRoleFailure};

pub(super) struct HdfsNamespace {
    protocol: Arc<dyn HdfsProtocol>,
    identity: BackendIdentity,
}

impl HdfsNamespace {
    pub(super) fn new<P: HdfsProtocol + 'static>(
        protocol: Arc<P>,
        identity: BackendIdentity,
    ) -> Self {
        Self { protocol, identity }
    }

    async fn stat(
        &self,
        path: &StoragePath,
    ) -> Result<crate::storage::SourceDescriptor, StorageRoleFailure> {
        let facts = self.protocol.stat(path).await?;
        descriptor(&self.identity, path.clone(), &facts)
    }
}

#[async_trait]
impl Namespace for HdfsNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        match request {
            NamespaceRequest::Stat(path) => {
                Ok(NamespaceResult::Entries(vec![self.stat(&path).await?]))
            }
            NamespaceRequest::List(path) => {
                let entries = self.protocol.list(&path).await?;
                let values = entries
                    .iter()
                    .map(|facts| descriptor(&self.identity, facts.path.clone(), facts))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(NamespaceResult::Entries(values))
            }
            NamespaceRequest::ReadLink(path) => Err(failure(&path, FailureClass::Unsupported)),
            NamespaceRequest::CreateDirectory(path) => {
                refuse_root(&path)?;
                self.protocol.create_directory(&path).await?;
                Ok(NamespaceResult::Completed)
            }
            NamespaceRequest::Delete(path) => {
                refuse_root(&path)?;
                let kind = self.stat(&path).await?.kind;
                self.protocol.delete(&path, kind).await?;
                Ok(NamespaceResult::Completed)
            }
            NamespaceRequest::Rename { from, to } => {
                refuse_root(&from)?;
                refuse_root(&to)?;
                self.protocol.rename(&from, &to, true).await?;
                Ok(NamespaceResult::Completed)
            }
        }
    }
}

fn refuse_root(path: &StoragePath) -> Result<(), StorageRoleFailure> {
    if path.as_str().is_empty() {
        Err(failure(path, FailureClass::InvalidInput))
    } else {
        Ok(())
    }
}

fn failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, Operation::Namespace, class, Transience::Permanent)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_mutation_is_refused_before_hdfs_io() {
        assert!(refuse_root(&StoragePath::root()).is_err());
        assert!(refuse_root(&StoragePath::new("safe").unwrap_or_else(|e| panic!("{e}"))).is_ok());
    }
}
