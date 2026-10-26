//! The namespace role: typed verbs over one storage's tree (P7 §5.1, §5.4.1, §5.4.5).

use std::num::NonZeroUsize;

use async_trait::async_trait;

use super::StorageRoleFailure;
use crate::model::{
    EntryOperationFailure, FailureClass, Operation, RenameContext, StoragePath, SymlinkTarget,
    Transience,
};
use crate::storage::SourceDescriptor;
use crate::storage::listing::{ListRequest, ListingStream};

/// Creates a link itself; the target is stored verbatim, never traversed during creation.
/// Parents must exist. Ownership and times may be sent with creation in one protocol operation.
#[derive(Clone, Debug)]
pub struct CreateSymlinkRequest {
    pub path: StoragePath,
    pub target: SymlinkTarget,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub accessed: Option<crate::model::StorageTimestamp>,
    pub modified: Option<crate::model::StorageTimestamp>,
}

/// Coherent namespace role: one method per verb.
///
/// Every failure is a [`StorageRoleFailure`]: `Entry` concerns only the path at hand and the
/// caller may go on; `Session` means the connection is unusable and the caller should stop.
///
/// The verbs other than [`Namespace::list`] default to an `Unsupported` entry failure, never to a
/// silent no-op; every backend implements each one it supports.
#[async_trait]
pub trait Namespace: Send + Sync {
    /// Performs a fresh connectivity operation. Object stores override this with a bucket
    /// request because their root descriptor is synthetic.
    async fn check_connectivity(&self) -> Result<(), StorageRoleFailure> {
        self.stat(&StoragePath::root()).await.map(|_| ())
    }

    /// Describes one entry itself, without following a symlink in its last segment (a link is
    /// described as the link). A missing entry is an `Entry` failure of class `NotFound`.
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        Err(unsupported(
            path,
            Operation::Observe,
            "stat is unsupported by this namespace",
        ))
    }

    /// Lists the direct children of one directory, page by page: the only listing entry. See
    /// [`crate::storage::listing`] for the stream's contract.
    fn list(&self, request: ListRequest) -> ListingStream;

    /// Opaque identifier for the listing semantics used to validate traversal checkpoints.
    /// Consumers must compare/hash the bytes without interpreting them. Implementations must
    /// keep the key stable across connections with equivalent listing semantics, and change it
    /// when backend configuration makes an old listing checkpoint incompatible. This is not a
    /// cursor or a snapshot identifier; ordinary changes to directory contents do not change it.
    /// The default is for backends with no configurable listing semantics. This method does no I/O.
    fn listing_resume_key(&self) -> &[u8] {
        &[0]
    }

    /// Optional scan-wide context for interpreting rename evidence returned by this namespace.
    /// Consumers persist this once with a scan, not once per entry. Its policy never grants
    /// permission to skip content verification or to delete a source or destination entry.
    fn rename_context(&self) -> Option<&RenameContext> {
        None
    }

    /// Reads the text a symlink itself stores, without resolving or following it.
    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        Err(unsupported(
            path,
            Operation::Observe,
            "symbolic links are unsupported",
        ))
    }

    /// Creates a new symbolic link, refusing an existing destination. Metadata is applied to
    /// the link itself. A failure applying metadata may leave the created link in place.
    async fn create_symlink(
        &self,
        request: CreateSymlinkRequest,
    ) -> Result<(), StorageRoleFailure> {
        Err(unsupported(
            &request.path,
            Operation::Namespace,
            "creating symbolic links is unsupported",
        ))
    }

    /// Creates one directory level. An existing entry is an `Entry` failure of class `Conflict`
    /// (a backend whose protocol creates idempotently may answer success instead), which the
    /// caller decides about. The parent is expected to exist: creating missing parents is
    /// [`crate::storage::create_directory_all`]'s job.
    ///
    /// **NFS exception: a vanished cached parent is recreated.** The NFS adapter answers a level
    /// it holds in its process-wide directory-handle cache without asking the server. When
    /// another client deleted that directory afterwards, the next level's `create_directory`
    /// finds its parent's handle stale and the parent gone; it then recreates every missing
    /// level of the parent within the same call and creates its own level, as the legacy
    /// recursive helper rebuilt the path — so there one call may create more than one level.
    /// A parent the server never had (not reached through a stale cached handle) stays
    /// `NotFound` and is not created; a failure while recreating is reported at the level that
    /// failed. The NFS adapter also replaces a non-directory it finds at the level itself (see
    /// the NFS exception in [`crate::storage::create_directory_all`]'s module docs).
    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        Err(unsupported(path, Operation::Namespace, NO_MUTATIONS))
    }

    /// Deletes one file, one link itself, or one empty directory. A directory that only transfer
    /// artifacts keep non-empty counts as empty; anything else left in it is a `Conflict` and
    /// nothing is deleted.
    async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        Err(unsupported(path, Operation::Namespace, NO_MUTATIONS))
    }

    /// Deletes listed non-directory entries. Results correspond one-for-one, in input order;
    /// failure of one entry never hides successful siblings. Backends may issue one bulk RPC.
    async fn delete_batch(&self, paths: &[StoragePath]) -> Vec<Result<(), StorageRoleFailure>> {
        let mut results = Vec::with_capacity(paths.len());
        for path in paths {
            results.push(self.delete(path).await);
        }
        results
    }

    /// Maximum useful number of entries in one [`Self::delete_batch`] call.
    fn delete_batch_size(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }

    /// Lists the current children to remove, without loading historical object versions.
    /// Deletion of a versioned key leaves its history intact.
    fn list_for_delete(&self, request: ListRequest) -> ListingStream {
        self.list(request)
    }

    /// Whether recursive deletion may delete children before their directory listing finishes.
    /// The default keeps all children when a directory read fails midway. Object stores may
    /// opt into bounded page-by-page deletion; a later listing failure then keeps the directory
    /// and reports the failure alongside earlier successful deletions.
    fn allows_paged_delete(&self) -> bool {
        false
    }

    /// Removes transfer artifacts directly inside a directory, including their private subtrees.
    /// Ordinary children and the directory itself are retained. Recursive deletion invokes this
    /// also for a retained storage root, whose hidden artifacts are absent from public listings.
    async fn cleanup_transfer_artifacts(
        &self,
        _path: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        Ok(())
    }

    /// Deletes an entire subtree using the backend's native recursive operation, without
    /// enumerating progress. Also removes transfer artifacts inside it. This can delete the
    /// configured storage root only when `allow_storage_root` is explicitly true.
    /// Backends without a native recursive primitive return `Unsupported`; use
    /// [`crate::storage::delete_tree`] for portable progress-reporting deletion.
    async fn delete_subtree(
        &self,
        path: &StoragePath,
        _allow_storage_root: bool,
    ) -> Result<(), StorageRoleFailure> {
        Err(unsupported(
            path,
            Operation::Namespace,
            "native subtree deletion is unsupported",
        ))
    }

    /// Renames one file or directory, replacing an existing destination as far as the protocol
    /// allows. S3 copies the current object server-side, preserves metadata and tags, then
    /// deletes its current source key; this is not atomic and keeps stored versions. Callers
    /// must exclude concurrent writers to the source. A failed source deletion leaves both keys.
    async fn rename(
        &self,
        from: &StoragePath,
        _to: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        Err(unsupported(from, Operation::Namespace, NO_MUTATIONS))
    }

    /// What this namespace can do and how it compares names, answered without I/O so a caller
    /// refuses what it cannot do before any.
    fn capabilities(&self) -> NamespaceCapabilities {
        NamespaceCapabilities::BASIC
    }

    /// Most listings a consumer should keep *parked* at once — started, and not read to their
    /// end — when a listing waiting for its consumer holds a resource on the server (P7 §6.2,
    /// Q10). `None`, the default, when a parked listing holds nothing there (NFS, S3, HDFS: their
    /// position is a cookie or token, or nothing) or only a local one (Local: one file
    /// descriptor).
    ///
    /// A consumer that reads listings ahead of need (the Blocks traversal's prefetch) starts no
    /// more of them than this while that many are unfinished; the listing it needs next always
    /// starts. A consumer that keeps each listing open while it waits for its own consumer (the
    /// Unordered traversal's workers) keeps no more than this many open at once. CIFS answers 8:
    /// each such listing holds an open directory handle.
    fn parked_listing_limit(&self) -> Option<NonZeroUsize> {
        None
    }
}

const NO_MUTATIONS: &str = "namespace mutations are unsupported by this namespace";

/// What a namespace can do, and how it compares names (P7 §5.4.5).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceCapabilities {
    /// Whether creating a directory is supported.
    pub create_directory: Result<(), &'static str>,
    /// Whether deleting an entry is supported. Versioned object stores retain object history.
    pub delete: Result<(), &'static str>,
    /// Whether renaming a file is supported. Object stores use server-side copy then delete.
    pub rename: Result<(), &'static str>,
    /// Whether [`Namespace::read_link`] can read link targets.
    pub symlinks: bool,
    /// When two names are the same name.
    pub names: NameSemantics,
    /// Whether the namespace holds directory objects (P7 F5): files stored under a key ending in
    /// `/` (`p/d/`), beside the directory `p/d`. An object store does; a filesystem cannot store
    /// content on a directory, so a consumer does not copy one there.
    pub directory_objects: bool,
}

impl NamespaceCapabilities {
    /// Mutations supported, no symlinks, name semantics unknown, no directory
    /// objects.
    pub const BASIC: Self = Self {
        create_directory: Ok(()),
        delete: Ok(()),
        rename: Ok(()),
        symlinks: false,
        names: NameSemantics::UNKNOWN,
        directory_objects: false,
    };
}

impl Default for NamespaceCapabilities {
    fn default() -> Self {
        Self::BASIC
    }
}

/// How a namespace decides that two names are the same name. `Unknown` is treated as the unsafe
/// answer (insensitive) by consumers that delete by name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NameSemantics {
    /// Whether case distinguishes names.
    pub case: CaseSensitivity,
    /// Which Unicode normalisation names are compared under.
    pub normalization: NameNormalization,
}

impl NameSemantics {
    /// Nothing known about either.
    pub const UNKNOWN: Self = Self {
        case: CaseSensitivity::Unknown,
        normalization: NameNormalization::Unknown,
    };

    /// Byte-exact names: case matters and nothing is normalised.
    pub const EXACT: Self = Self {
        case: CaseSensitivity::Sensitive,
        normalization: NameNormalization::None,
    };
}

/// Whether names differing only in case are different names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaseSensitivity {
    /// `a` and `A` are two names.
    Sensitive,
    /// `a` and `A` are one name.
    Insensitive,
    /// Not known; treated as insensitive by consumers that delete by name.
    Unknown,
}

/// The Unicode normalisation a namespace applies when it compares names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameNormalization {
    /// Names are compared as stored, byte for byte.
    None,
    /// Names are compared after NFC normalisation.
    Nfc,
    /// Names are compared after NFD normalisation.
    Nfd,
    /// Not known.
    Unknown,
}

fn unsupported(path: &StoragePath, operation: Operation, diagnostic: &str) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            operation,
            FailureClass::Unsupported,
            Transience::Permanent,
            diagnostic,
        )
        .unwrap_or_else(|_| unreachable!("the static namespace diagnostic is valid")),
    )
}
