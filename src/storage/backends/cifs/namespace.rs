use std::sync::Arc;

use async_trait::async_trait;

use super::source::{CifsSourceFacts, classify, descriptor_from_facts, entry_failure};
use crate::model::{BackendIdentity, FailureClass, Operation, StoragePath};
use crate::storage::{Namespace, NamespaceRequest, NamespaceResult, StorageRoleFailure};

#[async_trait]
pub(super) trait CifsNamespaceProtocol: Send + Sync {
    async fn list(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<Vec<(StoragePath, CifsSourceFacts)>>;
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
}

#[async_trait]
impl Namespace for CifsNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        let NamespaceRequest::List(path) = request else {
            let path = request_path(&request);
            return Err(entry_failure(
                path,
                Operation::Observe,
                FailureClass::Unsupported,
            ));
        };
        let facts = self
            .protocol
            .list(&path)
            .await
            .map_err(|error| classify(&path, Operation::Traverse, &error))?;
        let entries = facts
            .into_iter()
            .map(|(child, facts)| {
                descriptor_from_facts(&self.identity, &child, &facts, Operation::Traverse)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(NamespaceResult::Entries(entries))
    }
}

fn request_path(request: &NamespaceRequest) -> &StoragePath {
    match request {
        NamespaceRequest::Stat(path)
        | NamespaceRequest::List(path)
        | NamespaceRequest::ReadLink(path)
        | NamespaceRequest::CreateDirectory(path)
        | NamespaceRequest::Delete(path) => path,
        NamespaceRequest::Rename { from, .. } => from,
    }
}
