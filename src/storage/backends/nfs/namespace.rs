use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use super::source::{NfsProtocolFailure, entry_failure, role_failure};
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength, Operation,
    SourceIdentity, SourceVersion, StoragePath, SymlinkTarget, SymlinkTargetEncoding, Transience,
};
use crate::storage::ListingFacts;
use crate::storage::artifacts::is_artifact_native;
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
    /// Timestamps the enumerating operation already returned. `readdirplus` and `LOOKUP` both
    /// carry the full attribute set, so neither path costs an extra round trip; `created` holds
    /// the POSIX change time, as the NFS metadata role also reports it (`NFSv3` has no birth time).
    pub(crate) timestamps: Option<crate::model::TimestampMetadata>,
    /// POSIX permission bits from the same attributes.
    pub(crate) mode: Option<u32>,
}

/// A listed child the listing could not describe.
pub(crate) struct NfsUndescribedChild {
    /// The child's path, or the listed directory's when the child's own cannot be spelled.
    pub(crate) path: StoragePath,
    pub(crate) failure: NfsProtocolFailure,
    /// What was wrong with it, for the diagnostic.
    pub(crate) reason: &'static str,
}

/// One child of a listing: its observation, or why it could not be described.
pub(crate) type NfsListedChild = Result<NfsNamespaceObservation, NfsUndescribedChild>;

/// A child of `directory` the role listing could not describe, blamed on the child's own path
/// when it can be spelled and on the directory otherwise.
pub(crate) fn undescribed_child(
    directory: &StoragePath,
    child: &Path,
    reason: &'static str,
) -> NfsUndescribedChild {
    NfsUndescribedChild {
        path: StoragePath::new(child.to_string_lossy()).unwrap_or_else(|_| directory.clone()),
        // The server answered for the directory but not fully for this entry: a later look may
        // well succeed, so whether retrying helps is not known.
        failure: NfsProtocolFailure::new(FailureClass::Protocol, Transience::Unknown),
        reason,
    }
}

#[async_trait]
pub(crate) trait NfsNamespaceProtocol: Send + Sync {
    async fn stat(&self, path: &StoragePath)
    -> Result<NfsNamespaceObservation, NfsProtocolFailure>;
    /// The children of `path`, each observed or the reason it could not be; a failure of the
    /// directory itself (lookup, the READDIRPLUS stream) fails the whole call.
    async fn list(&self, path: &StoragePath) -> Result<Vec<NfsListedChild>, NfsProtocolFailure>;
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

    /// Lists one directory. A child that cannot be described is reported on its own, beside the
    /// siblings that could be: the directory itself was listed, so failing it whole would drop
    /// them.
    async fn list(&self, path: &StoragePath) -> Result<NamespaceResult, StorageRoleFailure> {
        checked_allow_root(path)?;
        let children = self
            .protocol
            .list(path)
            .await
            .map_err(|error| role_failure(path, Operation::Namespace, error))?;
        let mut entries = Vec::with_capacity(children.len());
        let mut failures = Vec::new();
        for child in children {
            match child {
                Ok(entry) if is_internal(&entry.path) => {}
                Ok(entry) => match descriptor(&self.identity, entry.path.clone(), &entry) {
                    Ok(descriptor) => entries.push(descriptor),
                    Err(StorageRoleFailure::Entry(failure)) => failures.push(failure),
                    Err(session @ StorageRoleFailure::Session(_)) => return Err(session),
                },
                Err(child) if is_internal(&child.path) => {}
                Err(child) => failures.push(undescribed(child)),
            }
        }
        Ok(NamespaceResult::from_children(entries, failures))
    }
}

/// The entry failure of a child the listing could not describe. Always entry-scoped: whatever
/// was wrong concerns that child, never the session.
fn undescribed(child: NfsUndescribedChild) -> EntryOperationFailure {
    let NfsUndescribedChild {
        path,
        failure,
        reason,
    } = child;
    EntryOperationFailure::new(
        path,
        Operation::Traverse,
        failure.class,
        failure.transience,
        format!("NFS listing could not describe the entry: {reason}"),
    )
    .unwrap_or_else(|_| unreachable!("static NFS listing diagnostic is valid"))
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
            NamespaceRequest::List(path) => self.list(&path).await,
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
        backend_fact: None,
        content_version: None,
        inline_timestamps: entry.timestamps,
        inline_mode: entry.mode,
        version: SourceVersion::Current,
        listing: ListingFacts::default(),
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
        || is_internal(path)
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

fn is_internal(path: &StoragePath) -> bool {
    is_artifact_native(Path::new(path.as_str()))
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

    fn path(value: &str) -> StoragePath {
        StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
    }

    fn observed(value: &str) -> NfsNamespaceObservation {
        NfsNamespaceObservation {
            path: path(value),
            kind: EntryKind::File,
            size: Some(1),
            file_handle: bytes::Bytes::from(value.to_owned()),
            timestamps: None,
            mode: None,
        }
    }

    fn omitted(value: &str) -> NfsListedChild {
        Err(NfsUndescribedChild {
            path: path(value),
            failure: NfsProtocolFailure::new(FailureClass::Protocol, Transience::Unknown),
            reason: "the server omitted its attributes",
        })
    }

    /// A listing fake: `List` answers the children it was built with, or fails the directory.
    struct Listing(Option<fn() -> Vec<NfsListedChild>>);

    #[async_trait]
    impl NfsNamespaceProtocol for Listing {
        async fn stat(
            &self,
            _path: &StoragePath,
        ) -> Result<NfsNamespaceObservation, NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn list(
            &self,
            _path: &StoragePath,
        ) -> Result<Vec<NfsListedChild>, NfsProtocolFailure> {
            self.0
                .map(|children| children())
                .ok_or(NfsProtocolFailure::new(
                    FailureClass::PermissionDenied,
                    Transience::Permanent,
                ))
        }
        async fn read_link(&self, _path: &StoragePath) -> Result<bytes::Bytes, NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn create_directory(&self, _path: &StoragePath) -> Result<(), NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn delete(
            &self,
            _path: &StoragePath,
            _kind: EntryKind,
        ) -> Result<(), NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn rename(
            &self,
            _from: &StoragePath,
            _to: &StoragePath,
        ) -> Result<(), NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
    }

    fn adapter(listing: Listing) -> NfsNamespaceAdapter {
        let identity = BackendIdentity::new(crate::model::BackendKind::Nfs, "nfs-listing-test")
            .unwrap_or_else(|error| panic!("{error}"));
        NfsNamespaceAdapter::new(Arc::new(listing), identity)
    }

    /// A child the listing could not describe is an entry failure at its own path, beside the
    /// siblings that were described; hidden artifacts stay hidden whether described or not.
    #[tokio::test]
    async fn an_undescribable_child_fails_alone() {
        let namespace = adapter(Listing(Some(|| {
            vec![
                Ok(observed("d/a")),
                omitted("d/b"),
                Ok(observed("d/c")),
                Ok(observed("d/.data-mover-x.part")),
                omitted("d/.data-mover-y.part"),
            ]
        })));
        let result = namespace
            .execute(NamespaceRequest::List(path("d")))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let NamespaceResult::Listing { entries, failures } = result else {
            panic!("expected a partial listing, got {result:?}");
        };
        let listed: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(listed, ["d/a", "d/c"]);
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].path(), &path("d/b"));
        assert_eq!(failures[0].class(), FailureClass::Protocol);
        assert_eq!(failures[0].transience(), Transience::Unknown);
        assert!(failures[0].diagnostic().contains("omitted its attributes"));
    }

    /// A listing whose every child is described is plain `Entries`; a directory that cannot be
    /// listed still fails whole.
    #[tokio::test]
    async fn a_whole_listing_is_entries_and_a_failed_directory_fails_whole() {
        let complete = adapter(Listing(Some(|| vec![Ok(observed("d/a"))])))
            .execute(NamespaceRequest::List(path("d")))
            .await;
        assert!(matches!(complete, Ok(NamespaceResult::Entries(ref e)) if e.len() == 1));
        let refused = adapter(Listing(None))
            .execute(NamespaceRequest::List(path("d")))
            .await;
        assert!(
            matches!(refused, Err(StorageRoleFailure::Entry(ref f))
                if f.path() == &path("d") && f.class() == FailureClass::PermissionDenied),
            "{refused:?}"
        );
    }

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
        assert!(
            checked(
                &StoragePath::new("safe/.data-mover-private.stage")
                    .unwrap_or_else(|error| panic!("{error}"))
            )
            .is_err()
        );
        assert!(checked_allow_root(&StoragePath::root()).is_ok());
    }
}
