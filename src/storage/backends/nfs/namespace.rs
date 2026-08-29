use std::path::{Component, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use super::source::{NfsProtocolFailure, entry_failure, role_failure};
use crate::model::{
    BackendIdentity, EntryKind, FailureClass, IdentityStrength, Operation, SourceIdentity,
    StoragePath, SymlinkTarget, SymlinkTargetEncoding, Transience,
};
use crate::storage::{
    Namespace, NamespaceRequest, NamespaceResult, SourceDescriptor, StorageRoleFailure,
};

pub(crate) struct NfsNamespaceAdapter {
    protocol: Arc<dyn NfsNamespaceProtocol>,
    identity: BackendIdentity,
}

pub(crate) struct NfsNamespaceObservation {
    pub(crate) path: StoragePath,
    pub(crate) kind: EntryKind,
    pub(crate) size: Option<u64>,
    pub(crate) file_handle: bytes::Bytes,
}

#[async_trait]
pub(crate) trait NfsNamespaceProtocol: Send + Sync {
    async fn stat(&self, path: &StoragePath)
    -> Result<NfsNamespaceObservation, NfsProtocolFailure>;
    async fn list(
        &self,
        path: &StoragePath,
    ) -> Result<Vec<NfsNamespaceObservation>, NfsProtocolFailure>;
    async fn read_link(&self, path: &StoragePath) -> Result<bytes::Bytes, NfsProtocolFailure>;
    async fn create_directory(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure>;
    async fn delete(&self, path: &StoragePath, kind: EntryKind) -> Result<(), NfsProtocolFailure>;
    async fn rename(&self, from: &StoragePath, to: &StoragePath) -> Result<(), NfsProtocolFailure>;
}

impl NfsNamespaceAdapter {
    pub(crate) fn new(protocol: Arc<dyn NfsNamespaceProtocol>, identity: BackendIdentity) -> Self {
        Self { protocol, identity }
    }

    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        checked(path)?;
        let entry = self
            .protocol
            .stat(path)
            .await
            .map_err(|error| role_failure(path, Operation::Namespace, error))?;
        descriptor(&self.identity, path.clone(), &entry)
    }
}

#[async_trait]
impl Namespace for NfsNamespaceAdapter {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        match request {
            NamespaceRequest::Stat(path) => {
                Ok(NamespaceResult::Entries(vec![self.describe(&path).await?]))
            }
            NamespaceRequest::List(path) => {
                checked_allow_root(&path)?;
                let entries = self
                    .protocol
                    .list(&path)
                    .await
                    .map_err(|error| role_failure(&path, Operation::Namespace, error))?;
                let values = entries
                    .iter()
                    .map(|entry| descriptor(&self.identity, entry.path.clone(), entry))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(NamespaceResult::Entries(values))
            }
            NamespaceRequest::ReadLink(path) => {
                checked(&path)?;
                let target = self
                    .protocol
                    .read_link(&path)
                    .await
                    .map_err(|error| role_failure(&path, Operation::Namespace, error))?;
                let target = SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, target.to_vec())
                    .map_err(|_| invalid(&path))?;
                Ok(NamespaceResult::LinkTarget(target))
            }
            NamespaceRequest::CreateDirectory(path) => {
                checked(&path)?;
                self.protocol
                    .create_directory(&path)
                    .await
                    .map_err(|error| role_failure(&path, Operation::Namespace, error))?;
                Ok(NamespaceResult::Completed)
            }
            NamespaceRequest::Delete(path) => {
                checked(&path)?;
                let observed = self.describe(&path).await?;
                self.protocol
                    .delete(&path, observed.kind)
                    .await
                    .map_err(|error| role_failure(&path, Operation::Namespace, error))?;
                Ok(NamespaceResult::Completed)
            }
            NamespaceRequest::Rename { from, to } => {
                checked(&from)?;
                checked(&to)?;
                self.protocol
                    .rename(&from, &to)
                    .await
                    .map_err(|error| role_failure(&from, Operation::Namespace, error))?;
                Ok(NamespaceResult::Completed)
            }
        }
    }
}

fn descriptor(
    identity: &BackendIdentity,
    path: StoragePath,
    entry: &NfsNamespaceObservation,
) -> Result<SourceDescriptor, StorageRoleFailure> {
    let source_identity = SourceIdentity::new(
        identity.clone(),
        IdentityStrength::StableWithinBackend,
        &entry.file_handle,
    )
    .map_err(|_| invalid(&path))?;
    Ok(SourceDescriptor {
        path,
        kind: entry.kind,
        size: entry.size,
        source_identity,
    })
}

fn checked(path: &StoragePath) -> Result<PathBuf, StorageRoleFailure> {
    if path.as_str().is_empty() {
        return Err(invalid(path));
    }
    checked_allow_root(path)
}

fn checked_allow_root(path: &StoragePath) -> Result<PathBuf, StorageRoleFailure> {
    let native = PathBuf::from(path.as_str());
    if native.is_absolute()
        || native.components().any(|part| {
            matches!(
                part,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        Err(invalid(path))
    } else {
        Ok(native)
    }
}

fn invalid(path: &StoragePath) -> StorageRoleFailure {
    entry_failure(
        path,
        Operation::Namespace,
        FailureClass::InvalidInput,
        Transience::Permanent,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_paths_are_confined_and_root_mutation_is_refused() {
        assert!(checked(&StoragePath::root()).is_err());
        assert!(
            checked(&StoragePath::new("../escape").unwrap_or_else(|error| panic!("{error}")))
                .is_err()
        );
        assert!(
            checked(&StoragePath::new("safe/child").unwrap_or_else(|error| panic!("{error}")))
                .is_ok()
        );
        assert!(checked_allow_root(&StoragePath::root()).is_ok());
    }
}
