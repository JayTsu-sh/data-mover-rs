//! The namespace role: typed verbs over one storage's tree (P7 §5.1, §5.4.1, §5.4.5).

use std::num::NonZeroUsize;

use async_trait::async_trait;

use super::StorageRoleFailure;
use crate::model::{
    EntryOperationFailure, FailureClass, Operation, StoragePath, SymlinkTarget, Transience,
};
use crate::storage::SourceDescriptor;
use crate::storage::listing::{ListRequest, ListingStream};

/// Coherent namespace role: one method per verb.
///
/// Every failure is a [`StorageRoleFailure`]: `Entry` concerns only the path at hand and the
/// caller may go on; `Session` means the connection is unusable and the caller should stop.
///
/// The verbs other than [`Namespace::list`] default to an `Unsupported` entry failure, never to a
/// silent no-op; every backend implements each one it supports.
#[async_trait]
pub trait Namespace: Send + Sync {
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

    /// Reads the text a symlink itself stores, without resolving or following it.
    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        Err(unsupported(
            path,
            Operation::Observe,
            "symbolic links are unsupported",
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

    /// Renames one file or directory, replacing an existing destination as far as the protocol
    /// allows.
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
    /// Whether [`Namespace::list`] gives each key its history (P7 §10): decided when the storage
    /// was connected (an S3 bucket that was versioned then, or would not say).
    pub histories: bool,
    /// Whether `create_directory`, `delete` and `rename` are supported; the reason when they are
    /// not. Recursive delete and recursive create refuse a namespace without them before any I/O.
    pub mutations: Result<(), &'static str>,
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
    /// No histories, mutations supported, no symlinks, name semantics unknown, no directory
    /// objects.
    pub const BASIC: Self = Self {
        histories: false,
        mutations: Ok(()),
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
