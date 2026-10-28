//! The S3 namespace role (ADR-0006 C22): `stat` and `list` (delimiter `/`, one prefix at a time):
//! current objects on a bucket that was never versioned, and one child per key with its whole
//! history on one that was (P7 §10, [`S3Protocol::lists_histories`]). Deletion removes the current
//! object (retaining version history); rename copies server-side before deleting the source.
//! Directories are virtual prefixes, so directory creation is unsupported.

use std::sync::Arc;

use async_trait::async_trait;

use super::current::produce_current;
use super::histories::produce_histories;
use super::listing::{directory_descriptor, directory_prefix, file_descriptor};
use super::paging::{ListingLimits, Paging};
use super::source::role_failure;
use super::{S3Protocol, S3ProtocolFailure};
use crate::model::{
    BackendIdentity, EntryOperationFailure, FailureClass, Operation, StoragePath, SymlinkTarget,
    Transience,
};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::{
    ListRequest, ListingStream, NameSemantics, Namespace, NamespaceCapabilities, SourceDescriptor,
    StorageRoleFailure,
};

const NO_DIRECTORIES: &str = "S3 prefixes are virtual; creating a directory is unsupported";

pub(super) struct S3Namespace<P> {
    protocol: Arc<P>,
    identity: BackendIdentity,
    limits: ListingLimits,
    /// Whether listings give each key its history: taken once, when the namespace is connected
    /// ([`S3Protocol::lists_histories`], P7 Q-A), so it holds for the connection.
    histories: bool,
}

impl<P: S3Protocol + 'static> S3Namespace<P> {
    pub(super) fn new(protocol: Arc<P>, identity: BackendIdentity) -> Self {
        let histories = protocol.lists_histories();
        Self {
            protocol,
            identity,
            limits: ListingLimits::PRODUCTION,
            histories,
        }
    }

    /// The same role with other paging bounds, so tests reach them with a few objects.
    #[cfg(test)]
    pub(super) const fn with_limits(mut self, limits: ListingLimits) -> Self {
        self.limits = limits;
        self
    }

    /// An object when one has the path as its key; otherwise a directory when anything lies
    /// under `<path>/`; otherwise `NotFound`. A path ending in `/` names a directory object
    /// (P7 F5) and only that: gone, it is `NotFound` (a vanished entry), never the directory it
    /// spells nor an invalid directory path.
    async fn stat_entry(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
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
            }) if path.names_directory_object() => Err(failure(
                path,
                FailureClass::NotFound,
                "no S3 object at this key ending in '/'",
            )),
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
    async fn check_connectivity(&self) -> Result<(), StorageRoleFailure> {
        self.protocol
            .head_bucket()
            .await
            .map_err(|e| role_failure(&StoragePath::root(), Operation::Observe, e))
    }

    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        refuse_artifact(path)?;
        self.stat_entry(path).await
    }

    /// The subdirectories of the prefix, and its objects: on a bucket that keeps histories
    /// ([`S3Protocol::lists_histories`]) one child per key with its whole history, the directory
    /// gathered whole first ([`produce_histories`], P7 §5.3, Q9); otherwise the current objects,
    /// each server page handed over as it arrives ([`produce_current`], P7 C6). Pages are
    /// stateless requests, so cancellation or a dropped stream abandons the listing at once.
    fn list(&self, request: ListRequest) -> ListingStream {
        if let Err(failure) = refuse_artifact(&request.path) {
            return ListingStream::failed(request.path, failure);
        }
        let (protocol, identity, limits) = (
            Arc::clone(&self.protocol),
            self.identity.clone(),
            self.limits,
        );
        let histories = self.histories;
        ListingStream::from_producer(request.path.clone(), move |sink| async move {
            let protocol = protocol.as_ref();
            if histories {
                produce_histories(protocol, &identity, limits, &request, &sink).await;
            } else {
                produce_current(protocol, &identity, limits, &request, &sink).await;
            }
        })
    }

    fn list_for_traversal(&self, request: ListRequest, is_root: bool) -> ListingStream {
        if !self.protocol.bucket_capabilities().directory_objects {
            return self.list(request);
        }
        let namespace = Self {
            protocol: Arc::clone(&self.protocol),
            identity: self.identity.clone(),
            limits: self.limits,
            histories: self.histories,
        };
        let identity = self.identity.clone();
        ListingStream::from_producer(request.path.clone(), move |sink| async move {
            super::traversal_listing::produce(&namespace, &identity, &request, is_root, &sink)
                .await;
        })
    }

    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        Err(failure(
            path,
            FailureClass::Unsupported,
            "S3 has no symbolic links",
        ))
    }

    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        Err(failure(path, FailureClass::Unsupported, NO_DIRECTORIES))
    }

    async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        refuse_artifact(path)?;
        if path == &StoragePath::root() {
            return Err(failure(
                path,
                FailureClass::InvalidInput,
                "cannot delete the storage root as one entry",
            ));
        }
        match self.protocol.head(path.as_str()).await {
            Ok(_) => self
                .protocol
                .delete_object(path.as_str())
                .await
                .map_err(|e| role_failure(path, Operation::Namespace, e)),
            Err(S3ProtocolFailure::Entry {
                class: FailureClass::NotFound,
                ..
            }) => {
                super::namespace_cleanup::cleanup(self.protocol.as_ref(), path, self.limits, true)
                    .await
            }
            Err(e) => Err(role_failure(path, Operation::Namespace, e)),
        }
    }

    async fn cleanup_transfer_artifacts(
        &self,
        path: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        refuse_artifact(path)?;
        super::namespace_cleanup::cleanup(self.protocol.as_ref(), path, self.limits, false).await
    }

    fn delete_batch_size(&self) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(100).unwrap_or(std::num::NonZeroUsize::MIN)
    }

    async fn delete_batch(&self, paths: &[StoragePath]) -> Vec<Result<(), StorageRoleFailure>> {
        // The caller obtained these non-directory keys from list_for_delete, so no HEAD is needed.
        if paths
            .iter()
            .any(|path| path == &StoragePath::root() || is_artifact_path(path.as_str()))
        {
            let mut results = Vec::with_capacity(paths.len());
            for path in paths {
                results.push(self.delete(path).await);
            }
            return results;
        }
        let keys: Vec<_> = paths.iter().map(|path| path.as_str().to_owned()).collect();
        self.protocol
            .delete_objects(&keys)
            .await
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                result.map_err(|e| role_failure(&paths[index], Operation::Namespace, e))
            })
            .collect()
    }

    fn list_for_delete(&self, request: ListRequest) -> ListingStream {
        if let Err(failure) = refuse_artifact(&request.path) {
            return ListingStream::failed(request.path, failure);
        }
        let (protocol, identity, limits) = (
            Arc::clone(&self.protocol),
            self.identity.clone(),
            self.limits,
        );
        ListingStream::from_producer(request.path.clone(), move |sink| async move {
            produce_current(protocol.as_ref(), &identity, limits, &request, &sink).await;
        })
    }

    fn allows_paged_delete(&self) -> bool {
        true
    }

    async fn rename(&self, from: &StoragePath, to: &StoragePath) -> Result<(), StorageRoleFailure> {
        refuse_artifact(from)?;
        refuse_artifact(to)?;
        if from.as_str().is_empty() || to.as_str().is_empty() {
            return Err(failure(
                from,
                FailureClass::InvalidInput,
                "the S3 storage root cannot be renamed or replaced",
            ));
        }
        if let Err(reason) = self.protocol.namespace_rename_support() {
            return Err(failure(from, FailureClass::Unsupported, reason));
        }
        self.protocol
            .rename_object(from.as_str(), to.as_str())
            .await
            .map_err(|e| role_failure(from, Operation::Namespace, e))
    }

    fn listing_resume_key(&self) -> &[u8] {
        // Revision 2 classifies full marker histories and lifts content objects into their
        // parent's block. Old checkpoints must not mix block membership/counting semantics.
        // Suspended buckets and refused versioning probes also list histories.
        if self.histories { &[1, 2] } else { &[0, 2] }
    }

    /// Object buckets keep content under keys ending in `/` (`directory_objects`,
    /// P7 F5). An ONTAP NAS bucket keeps neither (P7 §14.2): no versions, and a key ending in `/`
    /// holding content is stored as a file.
    fn capabilities(&self) -> NamespaceCapabilities {
        let bucket = self.protocol.bucket_capabilities();
        NamespaceCapabilities {
            create_directory: Err(NO_DIRECTORIES),
            delete: Ok(()),
            rename: self.protocol.namespace_rename_support(),
            symlinks: false,
            names: NameSemantics::EXACT,
            directory_objects: bucket.directory_objects,
        }
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
