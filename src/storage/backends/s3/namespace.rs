//! The S3 namespace role (ADR-0006 C22): `Stat` and `List` (delimiter `/`, one prefix at a time),
//! and the listing of every stored version that a traversal of all versions drives. S3 has no
//! directories to create, delete or rename, so every mutating verb is refused, and
//! [`Namespace::mutations_unsupported`] lets recursive delete and create refuse before any I/O.

use std::sync::Arc;

use async_trait::async_trait;

use super::listing::{
    directory_descriptor, directory_prefix, file_descriptor, list_all, list_current,
};
use super::paging::{ListingLimits, Paging};
use super::source::role_failure;
use super::{S3Protocol, S3ProtocolFailure};
use crate::model::{
    BackendIdentity, EntryOperationFailure, FailureClass, Operation, StoragePath, Transience,
};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::{
    Namespace, NamespaceRequest, NamespaceResult, SourceDescriptor, StorageRoleFailure,
};

const NO_MUTATIONS: &str = "S3 has no directories; namespace mutations are unsupported";

pub(super) struct S3Namespace<P> {
    protocol: Arc<P>,
    identity: BackendIdentity,
    limits: ListingLimits,
}

impl<P: S3Protocol + 'static> S3Namespace<P> {
    pub(super) const fn new(protocol: Arc<P>, identity: BackendIdentity) -> Self {
        Self {
            protocol,
            identity,
            limits: ListingLimits::PRODUCTION,
        }
    }

    /// The same role with other paging bounds, so tests reach them with a few objects.
    #[cfg(test)]
    pub(super) const fn with_limits(mut self, limits: ListingLimits) -> Self {
        self.limits = limits;
        self
    }

    /// An object when one has the path as its key; otherwise a directory when anything lies
    /// under `<path>/`; otherwise `NotFound`.
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        if path.as_str().is_empty() {
            return directory_descriptor(&self.identity, path.clone())
                .map_err(StorageRoleFailure::Entry);
        }
        match self.protocol.head(path.as_str()).await {
            Ok(facts) => file_descriptor(&self.identity, path.clone(), &facts)
                .map_err(StorageRoleFailure::Entry),
            Err(S3ProtocolFailure::Entry {
                class: FailureClass::NotFound,
                ..
            }) => self.stat_prefix(path).await,
            Err(failure) => Err(role_failure(path, Operation::Observe, failure)),
        }
    }

    /// A directory when anything at all lies under `<path>/`. Empty pages that name a
    /// continuation prove nothing, so the probe pages on — within the listing bounds — until it
    /// sees an entry or the end.
    async fn stat_prefix(
        &self,
        path: &StoragePath,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        let prefix = directory_prefix(path)?;
        let mut paging = Paging::new(self.limits);
        let mut token: Option<String> = None;
        loop {
            let page = self
                .protocol
                .list_objects_page(&prefix, token.as_deref())
                .await
                .map_err(|e| role_failure(path, Operation::Observe, e))?;
            if !page.objects.is_empty() || !page.prefixes.is_empty() {
                return directory_descriptor(&self.identity, path.clone())
                    .map_err(StorageRoleFailure::Entry);
            }
            token = paging.advance(path, 0, page.next)?;
            if token.is_none() {
                return Err(failure(
                    path,
                    FailureClass::NotFound,
                    "no S3 object or prefix",
                ));
            }
        }
    }
}

#[async_trait]
impl<P: S3Protocol + 'static> Namespace for S3Namespace<P> {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        match request {
            NamespaceRequest::Stat(path) => {
                refuse_artifact(&path)?;
                Ok(NamespaceResult::Entries(vec![self.stat(&path).await?]))
            }
            NamespaceRequest::List(path) => {
                refuse_artifact(&path)?;
                list_current(self.protocol.as_ref(), &self.identity, &path, self.limits).await
            }
            NamespaceRequest::ReadLink(path) => Err(failure(
                &path,
                FailureClass::Unsupported,
                "S3 has no symbolic links",
            )),
            NamespaceRequest::CreateDirectory(path)
            | NamespaceRequest::Delete(path)
            | NamespaceRequest::Rename { from: path, .. } => {
                Err(failure(&path, FailureClass::Unsupported, NO_MUTATIONS))
            }
        }
    }

    fn supports_versions(&self) -> bool {
        true
    }

    async fn list_versions(
        &self,
        path: &StoragePath,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        refuse_artifact(path)?;
        list_all(self.protocol.as_ref(), &self.identity, path, self.limits).await
    }

    fn mutations_unsupported(&self) -> Option<&'static str> {
        Some(NO_MUTATIONS)
    }

    /// Pages are stateless requests: nothing is left open between them.
    fn listings_are_abortable(&self) -> bool {
        true
    }
}

/// Transfer artifacts are not addressable through the namespace (ADR-0006 C3b): listings hide
/// them, so no verb may reach them either.
fn refuse_artifact(path: &StoragePath) -> Result<(), StorageRoleFailure> {
    if is_artifact_path(path.as_str()) {
        Err(failure(
            path,
            FailureClass::InvalidInput,
            "transfer artifacts are not part of the namespace",
        ))
    } else {
        Ok(())
    }
}

fn failure(path: &StoragePath, class: FailureClass, diagnostic: &str) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Namespace,
            class,
            Transience::Permanent,
            diagnostic,
        )
        .unwrap_or_else(|_| unreachable!("static S3 namespace diagnostics are valid")),
    )
}
