use std::sync::Arc;

use async_trait::async_trait;

use super::protocol::{HdfsProtocol, entry_failure};
use super::source::descriptor;
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath, Transience};
use crate::storage::artifacts::is_artifact_path;
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
        refuse_artifacts(&request)?;
        match request {
            NamespaceRequest::Stat(path) => {
                Ok(NamespaceResult::Entries(vec![self.stat(&path).await?]))
            }
            NamespaceRequest::List(path) => {
                let entries = self.protocol.list(&path).await?;
                // Transfer artifacts never appear in a listing (ADR-0006 C3b); the protocol
                // listing stays raw for callers that must find them.
                let values = entries
                    .iter()
                    .filter(|facts| !is_artifact_path(facts.path.as_str()))
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
        Some(path) => Err(failure(path, FailureClass::InvalidInput)),
        None => Ok(()),
    }
}

fn failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, Operation::Namespace, class, Transience::Permanent)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_paths_are_refused_for_every_verb() {
        let path = |value: &str| StoragePath::new(value).unwrap_or_else(|e| panic!("{e}"));
        for request in [
            NamespaceRequest::Stat(path("dir/.data-mover-0123.part")),
            NamespaceRequest::List(path(".data-mover-staging")),
            NamespaceRequest::Delete(path("dir/.data-mover-x.claimed")),
            NamespaceRequest::CreateDirectory(path(".data-mover-staging/x")),
            NamespaceRequest::Rename {
                from: path("a"),
                to: path("dir/.data-mover-a.part"),
            },
        ] {
            assert!(matches!(
                refuse_artifacts(&request),
                Err(StorageRoleFailure::Entry(ref failure))
                    if failure.class() == FailureClass::InvalidInput
            ));
        }
        assert!(refuse_artifacts(&NamespaceRequest::Stat(path("dir/x.data-mover-y"))).is_ok());
    }

    /// The role `List` hides artifacts; the protocol listing underneath stays raw.
    #[tokio::test]
    async fn listing_hides_artifacts_but_the_protocol_does_not() {
        let protocol = Arc::new(super::super::contract_tests::MemoryHdfs::default());
        protocol
            .insert("a.txt", bytes::Bytes::from_static(b"a"))
            .await;
        protocol
            .insert(".data-mover-0123.part", bytes::Bytes::from_static(b"p"))
            .await;
        let namespace = HdfsNamespace::new(
            Arc::clone(&protocol),
            BackendIdentity::new(crate::model::BackendKind::Hdfs, "hdfs://nn:8020")
                .unwrap_or_else(|e| panic!("{e}")),
        );
        let listed = namespace
            .execute(NamespaceRequest::List(StoragePath::root()))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let NamespaceResult::Entries(entries) = listed else {
            panic!("list must return entries");
        };
        let names = entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a.txt"]);
        let raw = protocol
            .list(&StoragePath::root())
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(raw.len(), 2);
    }

    #[test]
    fn root_mutation_is_refused_before_hdfs_io() {
        assert!(refuse_root(&StoragePath::root()).is_err());
        assert!(refuse_root(&StoragePath::new("safe").unwrap_or_else(|e| panic!("{e}"))).is_ok());
    }
}
