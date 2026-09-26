use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use super::source::{NfsProtocolFailure, entry_failure, role_failure};
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength, Operation,
    OwnershipMode, SourceIdentity, SourceVersion, StoragePath, SymlinkTarget,
    SymlinkTargetEncoding, Transience,
};
use crate::storage::artifacts::is_artifact_native;
use crate::storage::listing::respelled;
use crate::storage::{
    ChildFailure, ListRequest, ListingPage, ListingStream, Namespace, NamespaceCapabilities,
    SourceDescriptor, StorageRoleFailure,
};
use crate::storage::{InlineOwnership, ListingFacts};

#[derive(Clone)]
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
    /// Numeric ownership from the same attributes, mapped the way the NFS metadata role maps a
    /// GETATTR (`nfs::owner_id`), so a plan asking for ownership costs no extra round trip.
    pub(crate) ownership: Option<InlineOwnership>,
}

/// The inline ownership of one attribute set: known only when both owner and group map to a
/// number. `mode` is the full attribute mode, which is what the metadata role reports.
pub(crate) fn inline_ownership(uid: Option<u32>, gid: Option<u32>, mode: u32) -> InlineOwnership {
    match (uid, gid) {
        (Some(uid), Some(gid)) => InlineOwnership::Known(OwnershipMode { uid, gid, mode }),
        _ => InlineOwnership::Unmapped,
    }
}

/// A listed child the listing could not describe.
pub(crate) struct NfsUndescribedChild {
    /// The child's path, or the listed directory's when the child's own cannot be spelled.
    pub(crate) path: StoragePath,
    pub(crate) failure: NfsProtocolFailure,
    /// What was wrong with it, for the diagnostic.
    pub(crate) reason: &'static str,
    /// The name as nfs-rs decoded it, when no path can spell the child (see [`unnamed_child`]).
    pub(crate) raw_name: Option<bytes::Bytes>,
}

/// One child of a listing: its observation, or why it could not be described.
pub(crate) type NfsListedChild = Result<NfsNamespaceObservation, NfsUndescribedChild>;

/// A child of `directory` the role listing could not describe, blamed on the child's own path
/// when it can be spelled and on the directory otherwise. A name nfs-rs decoded lossily (see
/// [`lossily_decoded`]) cannot be trusted to spell the child, so it is blamed on the directory.
pub(crate) fn undescribed_child(
    directory: &StoragePath,
    child: &Path,
    reason: &'static str,
) -> NfsUndescribedChild {
    let spelled = child.to_string_lossy();
    NfsUndescribedChild {
        path: if lossily_decoded(&spelled) {
            directory.clone()
        } else {
            StoragePath::new(spelled).unwrap_or_else(|_| directory.clone())
        },
        // The server answered for the directory but not fully for this entry: a later look may
        // well succeed, so whether retrying helps is not known.
        failure: NfsProtocolFailure::new(FailureClass::Protocol, Transience::Unknown),
        reason,
        raw_name: None,
    }
}

/// A child of `directory` whose name the server sent in bytes that are not UTF-8. nfs-rs hands
/// every name over as a `String`, replacing invalid bytes with U+FFFD, so the raw name is lost and
/// the replaced one names a different entry (or none): a path built from it must not be listed,
/// or the traversal would descend into it, find nothing, and count its whole subtree as gone.
/// A name that really contains U+FFFD is reported the same way; it cannot be told apart.
pub(crate) fn unnamed_child(directory: &StoragePath, name: &str) -> NfsUndescribedChild {
    NfsUndescribedChild {
        path: directory.clone(),
        failure: NfsProtocolFailure::new(FailureClass::Unsupported, Transience::Permanent),
        reason: "an NFS name is not UTF-8 (the NFS client replaced the invalid bytes), so no path \
                 can spell it",
        raw_name: Some(bytes::Bytes::copy_from_slice(name.as_bytes())),
    }
}

/// Whether a name nfs-rs decoded may have been replaced: it holds U+FFFD.
pub(crate) fn lossily_decoded(name: &str) -> bool {
    name.contains(char::REPLACEMENT_CHARACTER)
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

    /// Lists one whole directory. A child that cannot be described is reported on its own,
    /// beside the siblings that could be: the directory itself was listed, so failing it whole
    /// would drop them. Children come back joined with `Path::join`, which folds a trailing `/`
    /// of the request (`d/` gives `d/x`); each is spelled again as the request and its name.
    async fn list_whole(&self, path: &StoragePath) -> Result<ListingPage, StorageRoleFailure> {
        checked_allow_root(path)?;
        let children = self
            .protocol
            .list(path)
            .await
            .map_err(|error| role_failure(path, Operation::Namespace, error))?;
        let mut entries = Vec::with_capacity(children.len());
        let mut failures = Vec::new();
        let mut unnamed = Vec::new();
        let mut hidden = 0_u32;
        for child in children {
            match child {
                Ok(entry) if is_internal(&entry.path) => hidden = hidden.saturating_add(1),
                Ok(entry) => {
                    match descriptor(&self.identity, respelled(path, entry.path.clone()), &entry) {
                        Ok(descriptor) => entries.push(descriptor),
                        Err(StorageRoleFailure::Entry(failure)) => failures.push(failure),
                        Err(session @ StorageRoleFailure::Session(_)) => return Err(session),
                    }
                }
                Err(NfsUndescribedChild {
                    raw_name: Some(raw),
                    reason,
                    failure,
                    ..
                }) => unnamed.push(ChildFailure::unnamed(
                    &self.identity,
                    path,
                    raw,
                    failure.class,
                    reason,
                )),
                Err(child) if is_internal(&child.path) => hidden = hidden.saturating_add(1),
                Err(child) => failures.push(undescribed(child)),
            }
        }
        let mut page = ListingPage::from_descriptors(path, entries, failures, hidden);
        for failure in unnamed {
            page.push_failure(failure);
        }
        Ok(page)
    }
}

/// The entry failure of a child the listing could not describe. Always entry-scoped: whatever
/// was wrong concerns that child, never the session.
fn undescribed(child: NfsUndescribedChild) -> EntryOperationFailure {
    let NfsUndescribedChild {
        path,
        failure,
        reason,
        ..
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
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.describe(path).await
    }

    /// Reads the whole directory, then hands it over in pages (true paging comes with C7). The
    /// listing runs to its end even when the stream is dropped: nothing is abandoned mid-RPC.
    fn list(&self, request: ListRequest) -> ListingStream {
        let namespace = self.clone();
        ListingStream::from_producer(request.path.clone(), move |sink| async move {
            let listing = namespace.list_whole(&request.path).await;
            sink.deliver(listing, &request).await;
        })
    }

    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        checked(path)?;
        let target = self
            .protocol
            .read_link(path)
            .await
            .map_err(|error| role_failure(path, Operation::Namespace, error))?;
        SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, target.to_vec())
            .map_err(|_| invalid(path))
    }

    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        checked(path)?;
        self.protocol
            .create_directory(path)
            .await
            .map_err(|error| role_failure(path, Operation::Namespace, error))
    }

    async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        checked(path)?;
        let observed = self.describe(path).await?;
        self.protocol
            .delete(path, observed.kind)
            .await
            .map_err(|error| role_failure(path, Operation::Namespace, error))
    }

    async fn rename(&self, from: &StoragePath, to: &StoragePath) -> Result<(), StorageRoleFailure> {
        checked(from)?;
        checked(to)?;
        self.protocol
            .rename(from, to)
            .await
            .map_err(|error| role_failure(from, Operation::Namespace, error))
    }

    /// Name semantics stay unknown until `PATHCONF` (v3) or the `case_insensitive` attribute (v4)
    /// is queried; unknown is treated as insensitive.
    fn capabilities(&self) -> NamespaceCapabilities {
        NamespaceCapabilities {
            symlinks: true,
            ..NamespaceCapabilities::BASIC
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
        inline_ownership: entry.ownership,
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
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::storage::{CollectLimits, ListedChild, collect_listing};

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
            ownership: None,
        }
    }

    fn omitted(value: &str) -> NfsListedChild {
        Err(NfsUndescribedChild {
            path: path(value),
            failure: NfsProtocolFailure::new(FailureClass::Protocol, Transience::Unknown),
            reason: "the server omitted its attributes",
            raw_name: None,
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

    async fn list(
        namespace: &NfsNamespaceAdapter,
        directory: &str,
    ) -> Result<ListingPage, StorageRoleFailure> {
        let request = ListRequest::new(path(directory), CancellationToken::new());
        collect_listing(namespace.list(request), CollectLimits::UNBOUNDED).await
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
        let page = list(&namespace, "d")
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let listed: Vec<&str> = page.children.iter().map(ListedChild::name).collect();
        assert_eq!(listed, ["a", "c"]);
        assert_eq!(page.internal_hidden, 2);
        let failures: Vec<_> = page.failures.iter().map(|f| &f.failure).collect();
        assert_eq!(failures.len(), 1);
        assert_eq!(page.failures[0].name.as_deref(), Some("b"));
        assert_eq!(failures[0].path(), &path("d/b"));
        assert_eq!(failures[0].class(), FailureClass::Protocol);
        assert_eq!(failures[0].transience(), Transience::Unknown);
        assert!(failures[0].diagnostic().contains("omitted its attributes"));
    }

    /// A name nfs-rs decoded lossily is a child no path can spell: a failure at the directory,
    /// with the name as decoded, never a child listed under a path that names nothing (P7 C3).
    #[tokio::test]
    async fn a_lossily_decoded_name_is_an_unnamed_failure() {
        let namespace = adapter(Listing(Some(|| {
            vec![
                Ok(observed("d/a")),
                Err(super::unnamed_child(&path("d"), "bad\u{FFFD}name")),
            ]
        })));
        let page = list(&namespace, "d")
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(page.children.len(), 1);
        assert_eq!(page.failures.len(), 1);
        let unnamed = &page.failures[0];
        assert_eq!(unnamed.name, None);
        assert_eq!(
            unnamed.raw_name.as_deref(),
            Some("bad\u{FFFD}name".as_bytes())
        );
        assert_eq!(unnamed.failure.path(), &path("d"));
        assert_eq!(unnamed.failure.class(), FailureClass::Unsupported);
        assert!(unnamed.failure.identity().is_some());
    }

    /// A listing whose every child is described has no failures; a directory that cannot be
    /// listed still fails whole.
    #[tokio::test]
    async fn a_whole_listing_has_no_failures_and_a_failed_directory_fails_whole() {
        let complete = list(&adapter(Listing(Some(|| vec![Ok(observed("d/a"))]))), "d").await;
        assert!(
            matches!(complete, Ok(ref page) if page.children.len() == 1 && page.failures.is_empty())
        );
        let refused = list(&adapter(Listing(None)), "d").await;
        assert!(
            matches!(refused, Err(StorageRoleFailure::Entry(ref f))
                if f.path() == &path("d") && f.class() == FailureClass::PermissionDenied),
            "{refused:?}"
        );
    }

    /// `Path::join` folds a trailing `/` of the request (`d/` lists `d/a`); the child is spelled
    /// again as the request and its name, so it is listed, not refused as misspelled.
    #[tokio::test]
    async fn a_trailing_slash_request_keeps_its_spelling_in_the_children() {
        let namespace = adapter(Listing(Some(|| vec![Ok(observed("d/a"))])));
        let page = list(&namespace, "d/")
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(page.failures.is_empty(), "{:?}", page.failures);
        let (descriptors, _) = page.into_descriptors(&path("d/"));
        let paths: Vec<&str> = descriptors
            .iter()
            .map(|entry| entry.path.as_str())
            .collect();
        assert_eq!(paths, ["d//a"]);
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
