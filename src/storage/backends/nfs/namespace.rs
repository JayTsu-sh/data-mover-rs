use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::DirectoryCookie;

use super::source::{NfsProtocolFailure, role_failure};
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength, Operation,
    OwnershipMode, SourceIdentity, SourceVersion, StoragePath, SymlinkTarget,
    SymlinkTargetEncoding, Transience,
};
use crate::storage::artifacts::{is_artifact_native, is_artifact_path};
use crate::storage::{InlineOwnership, ListingFacts};
use crate::storage::{
    ListRequest, ListingStream, Namespace, NamespaceCapabilities, SourceDescriptor,
    StorageRoleFailure,
};

#[derive(Clone)]
pub(crate) struct NfsNamespaceAdapter {
    pub(super) protocol: Arc<dyn NfsNamespaceProtocol>,
    pub(super) identity: BackendIdentity,
    /// Leads every descent hint this namespace makes, so it reads only its own
    /// (`listing::hinted_handle`): 8 bytes of the endpoint identity's digest. It keeps a hint from
    /// another endpoint from being read by mistake; it is no proof — hints are plain bytes a
    /// caller can make, and two endpoints given the same identity share a tag.
    pub(super) hint_tag: [u8; HINT_TAG_LEN],
}

/// The length of [`NfsNamespaceAdapter::hint_tag`].
pub(super) const HINT_TAG_LEN: usize = 8;

pub(crate) struct NfsNamespaceObservation {
    pub(crate) path: StoragePath,
    pub(crate) kind: EntryKind,
    pub(crate) size: Option<u64>,
    pub(crate) file_handle: Bytes,
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
    pub(crate) raw_name: Option<Bytes>,
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
        raw_name: Some(Bytes::copy_from_slice(name.as_bytes())),
    }
}

/// Whether a name nfs-rs decoded may have been replaced: it holds U+FFFD.
pub(crate) fn lossily_decoded(name: &str) -> bool {
    name.contains(char::REPLACEMENT_CHARACTER)
}

/// How a READDIRPLUS read of a directory ended without a failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NfsReadEnd {
    /// The server said end of directory.
    Complete,
    /// [`NfsListingPages`] asked to stop: the listing was cancelled, or its consumer is gone.
    Stopped,
    /// The server handed back a directory cookie this read had already reached
    /// ([`NfsListingPages::reached`]): reading on would loop (P7 §8).
    Repeated,
}

/// What reading the directory again could do about a failed READDIRPLUS read: from the last
/// cookie it reached, before and after the first page, while its entries are remembered for
/// deduplication (P7 §5.2, Q18, C7c).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NfsRestart {
    /// Nothing: the failure stands.
    Never,
    /// The handle may no longer name the directory (`NFS3ERR_STALE` / `BADHANDLE`,
    /// `NFS4ERR_STALE` / `BADHANDLE` / `FHEXPIRED`, or a `NOENT` from a concurrent rename):
    /// resolve the path again, then read anew.
    Relookup,
    /// The server asked to be asked again later (`NFS4ERR_DELAY`, `NFS3ERR_JUKEBOX`,
    /// `NFS4ERR_GRACE`): back off, then read anew.
    Backoff,
    /// The request got no answer and nfs-rs certifies sending it again safe on the session it has
    /// (`RecoveryAction::Retry`; or a bare connection reset or timeout) — unlike `Remount`, no
    /// new session is needed. Pause briefly, then read anew; when the pauses run out after a page
    /// is out, only this listing fails.
    Resend,
    /// The server rejected the directory cookie or its verifier (`NFS3ERR_BAD_COOKIE`,
    /// `NFS4ERR_BAD_COOKIE`, `NFS4ERR_NOT_SAME`: nfs-rs `is_bad_directory_cookie`): only a read
    /// from the directory's start can go on; the listing keeps its buffer and skips entries it
    /// already took while it remembers them.
    FromStart,
}

/// A failed READDIRPLUS read of one directory.
#[derive(Clone)]
pub(crate) struct NfsReadFailure {
    pub(crate) failure: NfsProtocolFailure,
    pub(crate) restart: NfsRestart,
}

/// Where a READDIRPLUS read hands the children of the directory it reads.
#[async_trait]
pub(crate) trait NfsListingPages: Send {
    /// Takes the next child, in the order the server sent it. `true` once the page is full: the
    /// reader then calls [`Self::hand_over`] before it reads on.
    fn push(&mut self, child: NfsListedChild) -> bool;
    /// Counts an entry the listing hid instead of taking it as a child (ONTAP's `NFSv3`
    /// snapshot directory, P7 C7b), with the transfer artifacts in `internal_hidden`.
    fn hide(&mut self);
    /// Hands the full page over, waiting while the consumer still holds the previous one.
    /// `false` once the listing is over (cancelled, or its consumer gone): the reader must stop.
    async fn hand_over(&mut self) -> bool;
    /// Whether the listing is over, so the reader must not ask the server for more.
    fn stopping(&self) -> bool;
    /// Whether to take the entry the server sent as `fileid` / `name`, asked before the reader
    /// looks at it (the `NFSv3` `.snapshot` LOOKUP included): `false` for an entry this listing
    /// already took — after it continued from a cookie the server may send part of a reply again,
    /// or the whole directory — which the reader then skips. The entry counts as taken once it is
    /// pushed or hidden.
    fn first_sight(&mut self, fileid: u64, name: &str) -> bool;
    /// Every entry of the server's reply up to `next` has been taken and the directory goes on:
    /// a later read continues from `next`. `false` when this read already reached `next`, so the
    /// server's cookies loop: the reader ends with [`NfsReadEnd::Repeated`].
    fn reached(&mut self, next: DirectoryCookie) -> bool;
}

#[async_trait]
pub(crate) trait NfsNamespaceProtocol: Send + Sync {
    async fn create_symlink(
        &self,
        _request: &crate::storage::CreateSymlinkRequest,
    ) -> Result<(), NfsProtocolFailure> {
        Err(NfsProtocolFailure::new(
            FailureClass::Unsupported,
            Transience::Permanent,
        ))
    }
    async fn stat(&self, path: &StoragePath)
    -> Result<NfsNamespaceObservation, NfsProtocolFailure>;
    /// The handle of the directory `path` names, resolved from the root through the directory
    /// handle cache (a LOOKUP for each component it does not hold). A listing asks for it only
    /// when it has no descent hint, or its hint no longer names the directory.
    async fn directory_handle(&self, path: &StoragePath) -> Result<Bytes, NfsProtocolFailure>;
    /// Reads the directory whose handle is `handle` and whose path is `path` with READDIRPLUS,
    /// from the position `from` ([`DirectoryCookie::default`] is its start), handing every child
    /// to `pages` as the server sends it and every position it passes to
    /// [`NfsListingPages::reached`], and returns once the server says end of directory or
    /// `pages` says to stop. It checks
    /// [`NfsListingPages::stopping`] before every child, so a stopped listing sends no further
    /// READDIRPLUS request (a child already taken may still cost the LOOKUP / GETATTR of an
    /// incomplete `NFSv3` child, including `.snapshot`, P7 C7b), but it never abandons a request
    /// in flight (an NFSv4.1 session fences the slot of a cancelled request).
    /// It holds no more than the reply it is working through, and
    /// caches nothing.
    async fn read_directory(
        &self,
        path: &StoragePath,
        handle: Bytes,
        from: DirectoryCookie,
        pages: &mut dyn NfsListingPages,
    ) -> Result<NfsReadEnd, NfsReadFailure>;
    /// Forgets what is cached about reaching `path` (and refreshes the root handle when it has
    /// gone stale), after the handle [`Self::directory_handle`] gave for it proved stale.
    async fn forget_directory(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure>;
    async fn read_link(&self, path: &StoragePath) -> Result<Bytes, NfsProtocolFailure>;
    async fn create_directory(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure>;
    async fn cleanup_transfer_artifacts(
        &self,
        _path: &StoragePath,
    ) -> Result<(), NfsProtocolFailure> {
        Ok(())
    }
    async fn delete(&self, path: &StoragePath, kind: EntryKind) -> Result<(), NfsProtocolFailure>;
    async fn rename(&self, from: &StoragePath, to: &StoragePath) -> Result<(), NfsProtocolFailure>;
}

impl NfsNamespaceAdapter {
    pub(crate) fn new(protocol: Arc<dyn NfsNamespaceProtocol>, identity: BackendIdentity) -> Self {
        let digest = blake3::hash(identity.stable_id().as_bytes());
        let mut hint_tag = [0_u8; HINT_TAG_LEN];
        hint_tag.copy_from_slice(&digest.as_bytes()[..HINT_TAG_LEN]);
        Self {
            protocol,
            identity,
            hint_tag,
        }
    }

    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        checked(path)?;
        let entry = self
            .protocol
            .stat(path)
            .await
            .map_err(|error| role_failure(path, Operation::Namespace, error))?;
        descriptor(&self.identity, path.clone(), &entry).map_err(StorageRoleFailure::Entry)
    }
}

/// The entry failure of a child the listing could not describe. Always entry-scoped: whatever
/// was wrong concerns that child, never the session.
pub(super) fn undescribed(child: NfsUndescribedChild) -> EntryOperationFailure {
    let NfsUndescribedChild {
        path,
        failure,
        reason,
        ..
    } = child;
    let diagnostic = match failure.status.as_deref() {
        Some(status) => format!("NFS listing could not describe the entry: {reason}: {status}"),
        None => format!("NFS listing could not describe the entry: {reason}"),
    };
    EntryOperationFailure::new(
        path,
        Operation::Traverse,
        failure.class,
        failure.transience,
        diagnostic,
    )
    .unwrap_or_else(|_| unreachable!("the NFS listing diagnostic is non-blank"))
}

#[async_trait]
impl Namespace for NfsNamespaceAdapter {
    async fn create_symlink(
        &self,
        request: crate::storage::CreateSymlinkRequest,
    ) -> Result<(), StorageRoleFailure> {
        checked(&request.path)?;
        self.protocol
            .create_symlink(&request)
            .await
            .map_err(|error| role_failure(&request.path, Operation::Namespace, error))
    }
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.describe(path).await
    }

    /// Streams the directory page by page with READDIRPLUS, starting from the request's descent
    /// hint when it has one (P7 C7, see [`super::listing`]).
    fn list(&self, request: ListRequest) -> ListingStream {
        let namespace = self.clone();
        ListingStream::from_producer(request.path.clone(), move |sink| {
            super::listing::produce(namespace, request, sink)
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

    async fn cleanup_transfer_artifacts(
        &self,
        path: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        checked_allow_root(path)?;
        self.protocol
            .cleanup_transfer_artifacts(path)
            .await
            .map_err(|e| role_failure(path, Operation::Namespace, e))
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
        self.protocol.rename(from, to).await.map_err(|error| {
            // A missing parent of the destination failed (reported at that level): the
            // destination is the entry that needed it.
            let entry = if error.level.is_some() { to } else { from };
            role_failure(entry, Operation::Namespace, error)
        })
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

/// The descriptor of `entry` at `path`, or a failure at `path` when the handle is too large to
/// be an identity.
pub(super) fn descriptor(
    identity: &BackendIdentity,
    path: StoragePath,
    entry: &NfsNamespaceObservation,
) -> Result<SourceDescriptor, EntryOperationFailure> {
    let Ok(source_identity) = SourceIdentity::new(
        identity.clone(),
        IdentityStrength::StableWithinBackend,
        &entry.file_handle,
    ) else {
        return Err(invalid_entry(&path));
    };
    Ok(SourceDescriptor {
        rename_evidence: None,
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

pub(super) fn checked_allow_root(path: &StoragePath) -> Result<PathBuf, StorageRoleFailure> {
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

/// Whether `path` names a transfer artifact or lies inside one. On Unix a native path splits on
/// `/` alone, so the `/`-separated check is the same one without parsing a native path — every
/// listed child goes through it.
pub(super) fn is_internal(path: &StoragePath) -> bool {
    if cfg!(unix) {
        is_artifact_path(path.as_str())
    } else {
        is_artifact_native(Path::new(path.as_str()))
    }
}

fn invalid(path: &StoragePath) -> StorageRoleFailure {
    StorageRoleFailure::Entry(invalid_entry(path))
}

fn invalid_entry(path: &StoragePath) -> EntryOperationFailure {
    EntryOperationFailure::new(
        path.clone(),
        Operation::Namespace,
        FailureClass::InvalidInput,
        Transience::Permanent,
        "NFS role failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::storage::{
        CollectLimits, CreateSymlinkRequest, ListedChild, ListingPage, collect_listing,
    };

    fn path(value: &str) -> StoragePath {
        StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
    }

    fn observed(value: &str) -> NfsNamespaceObservation {
        NfsNamespaceObservation {
            path: path(value),
            kind: EntryKind::File,
            size: Some(1),
            file_handle: Bytes::from(value.to_owned()),
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

    /// A listing fake: a read of the directory hands over the children it was built with; with
    /// none, the directory cannot be resolved.
    struct Listing(Option<fn() -> Vec<NfsListedChild>>);

    #[async_trait]
    impl NfsNamespaceProtocol for Listing {
        async fn stat(
            &self,
            _path: &StoragePath,
        ) -> Result<NfsNamespaceObservation, NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn directory_handle(&self, _path: &StoragePath) -> Result<Bytes, NfsProtocolFailure> {
            self.0
                .map(|_| Bytes::from_static(b"fh"))
                .ok_or(NfsProtocolFailure::new(
                    FailureClass::PermissionDenied,
                    Transience::Permanent,
                ))
        }
        async fn read_directory(
            &self,
            _path: &StoragePath,
            _handle: Bytes,
            _from: DirectoryCookie,
            pages: &mut dyn NfsListingPages,
        ) -> Result<NfsReadEnd, NfsReadFailure> {
            for child in self.0.map(|children| children()).unwrap_or_default() {
                if pages.push(child) && !pages.hand_over().await {
                    return Ok(NfsReadEnd::Stopped);
                }
            }
            Ok(NfsReadEnd::Complete)
        }
        async fn forget_directory(&self, _path: &StoragePath) -> Result<(), NfsProtocolFailure> {
            Ok(())
        }
        async fn read_link(&self, _path: &StoragePath) -> Result<Bytes, NfsProtocolFailure> {
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
        /// A destination whose missing parent `d/missing` is refused.
        async fn rename(
            &self,
            _from: &StoragePath,
            _to: &StoragePath,
        ) -> Result<(), NfsProtocolFailure> {
            Err(
                NfsProtocolFailure::new(FailureClass::PermissionDenied, Transience::Permanent)
                    .at_level(path("d/missing")),
            )
        }
    }

    /// Captures a symlink request at the role boundary, where its path, opaque target and
    /// optional attributes must reach the NFS protocol together.
    struct SymlinkProtocol {
        request: Mutex<Option<CreateSymlinkRequest>>,
        failure: Option<NfsProtocolFailure>,
    }

    #[async_trait]
    impl NfsNamespaceProtocol for SymlinkProtocol {
        async fn create_symlink(
            &self,
            request: &CreateSymlinkRequest,
        ) -> Result<(), NfsProtocolFailure> {
            *self
                .request
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(request.clone());
            self.failure.clone().map_or(Ok(()), Err)
        }

        async fn stat(
            &self,
            _path: &StoragePath,
        ) -> Result<NfsNamespaceObservation, NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn directory_handle(&self, _path: &StoragePath) -> Result<Bytes, NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn read_directory(
            &self,
            _path: &StoragePath,
            _handle: Bytes,
            _from: DirectoryCookie,
            _pages: &mut dyn NfsListingPages,
        ) -> Result<NfsReadEnd, NfsReadFailure> {
            unreachable!("symlink tests do not list directories")
        }
        async fn forget_directory(&self, _path: &StoragePath) -> Result<(), NfsProtocolFailure> {
            Err(NfsProtocolFailure::protocol())
        }
        async fn read_link(&self, _path: &StoragePath) -> Result<Bytes, NfsProtocolFailure> {
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

    fn symlink_adapter(
        failure: Option<NfsProtocolFailure>,
    ) -> (NfsNamespaceAdapter, Arc<SymlinkProtocol>) {
        let protocol = Arc::new(SymlinkProtocol {
            request: Mutex::new(None),
            failure,
        });
        let identity = BackendIdentity::new(crate::model::BackendKind::Nfs, "nfs-symlink-test")
            .unwrap_or_else(|error| panic!("{error}"));
        (
            NfsNamespaceAdapter::new(protocol.clone(), identity),
            protocol,
        )
    }

    /// The NFS namespace forwards link target bytes and requested link metadata unchanged in a
    /// single protocol request; a relative parent segment in the target is opaque payload.
    #[tokio::test]
    async fn create_symlink_forwards_target_and_attributes() {
        use crate::model::{StorageTimestamp, SymlinkTarget, SymlinkTargetEncoding, TimePrecision};

        let (namespace, protocol) = symlink_adapter(None);
        let request = CreateSymlinkRequest {
            path: path("d/link"),
            target: SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"../target".to_vec())
                .unwrap_or_else(|error| panic!("{error}")),
            uid: Some(101),
            gid: Some(202),
            accessed: Some(
                StorageTimestamp::new(-1, TimePrecision::Nanoseconds)
                    .unwrap_or_else(|error| panic!("{error}")),
            ),
            modified: Some(
                StorageTimestamp::new(1_700_000_000_123_456_789, TimePrecision::Nanoseconds)
                    .unwrap_or_else(|error| panic!("{error}")),
            ),
        };

        namespace
            .create_symlink(request.clone())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let captured = protocol
            .request
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| panic!("the protocol did not receive the link request"));
        assert_eq!(captured.path, request.path);
        assert_eq!(captured.target, request.target);
        assert_eq!(captured.uid, request.uid);
        assert_eq!(captured.gid, request.gid);
        assert_eq!(captured.accessed, request.accessed);
        assert_eq!(captured.modified, request.modified);
    }

    /// An NFS protocol failure remains an entry-scoped namespace failure at the link path.
    #[tokio::test]
    async fn create_symlink_maps_typed_protocol_failure_to_entry() {
        let failure = NfsProtocolFailure::new(FailureClass::Protocol, Transience::Transient)
            .with_status("NFS3ERR_STALE");
        let (namespace, _) = symlink_adapter(Some(failure));
        let request = CreateSymlinkRequest {
            path: path("d/link"),
            target: crate::model::SymlinkTarget::new(
                crate::model::SymlinkTargetEncoding::UnixBytes,
                b"target".to_vec(),
            )
            .unwrap_or_else(|error| panic!("{error}")),
            uid: None,
            gid: None,
            accessed: None,
            modified: None,
        };

        assert!(matches!(
            namespace.create_symlink(request).await,
            Err(StorageRoleFailure::Entry(ref failure))
                if failure.path() == &path("d/link")
                    && failure.operation() == Operation::Namespace
                    && failure.class() == FailureClass::Protocol
                    && failure.transience() == Transience::Transient
                    && failure.diagnostic().contains("NFS3ERR_STALE")
        ));
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

    /// A rename whose destination needed a missing parent that was refused fails at that parent,
    /// and the diagnostic names the destination that needed it.
    #[tokio::test]
    async fn a_refused_destination_parent_is_reported_at_its_level() {
        let namespace = adapter(Listing(None));
        let refused = namespace
            .rename(&path("w/x"), &path("d/missing/x"))
            .await
            .err();
        let Some(StorageRoleFailure::Entry(failure)) = refused else {
            panic!("expected an entry failure: {refused:?}");
        };
        assert_eq!(failure.path(), &path("d/missing"));
        assert_eq!(failure.class(), FailureClass::PermissionDenied);
        assert!(
            failure.diagnostic().contains("`d/missing/x`"),
            "{}",
            failure.diagnostic()
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
