//! The namespace role: typed verbs over one storage's tree (P7 §5.1, §5.4.1, §5.4.5).

use async_trait::async_trait;

use super::StorageRoleFailure;
use super::version::version_unsupported;
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
    /// caller decides about.
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

    /// Lists one directory with every stored version and delete marker of each child object,
    /// each object's entries oldest first with the latest last, and its subdirectories once each
    /// (ADR-0006 C22). Every child carries its listed version. The stream has the same contract
    /// as [`Namespace::list`].
    ///
    /// The default serves namespaces without versions: an `Unsupported` failure, never the
    /// current entries instead.
    fn list_versions(&self, request: ListRequest) -> ListingStream {
        let failure = version_unsupported(&request.path, Operation::Traverse);
        ListingStream::failed(request.path, failure)
    }
}

const NO_MUTATIONS: &str = "namespace mutations are unsupported by this namespace";

/// What a namespace can do, and how it compares names (P7 §5.4.5).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceCapabilities {
    /// Whether [`Namespace::list_versions`] lists stored versions (a versioned object store).
    pub histories: bool,
    /// Whether `create_directory`, `delete` and `rename` are supported; the reason when they are
    /// not. Recursive delete and recursive create refuse a namespace without them before any I/O.
    pub mutations: Result<(), &'static str>,
    /// Whether [`Namespace::read_link`] can read link targets.
    pub symlinks: bool,
    /// When two names are the same name.
    pub names: NameSemantics,
}

impl NamespaceCapabilities {
    /// No histories, mutations supported, no symlinks, name semantics unknown.
    pub const BASIC: Self = Self {
        histories: false,
        mutations: Ok(()),
        symlinks: false,
        names: NameSemantics::UNKNOWN,
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
