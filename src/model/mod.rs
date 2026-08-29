//! Protocol-neutral values shared by the storage and transfer modules.

use std::fmt;
use std::str::FromStr;

pub(crate) const MAX_MODEL_FIELD_BYTES: usize = 16 * 1024 * 1024;

pub(crate) mod observation;
pub use observation::{
    EntryIdentityKey, EntrySnapshot, IdentityStrength, MetadataObservations, ObservedEntry,
    SnapshotDecodeError, SourceIdentity, SymlinkTarget, SymlinkTargetEncoding,
};

/// A failure to construct a neutral model value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelValueError {
    field: &'static str,
    reason: &'static str,
}

impl ModelValueError {
    const fn new(field: &'static str, reason: &'static str) -> Self {
        Self { field, reason }
    }

    /// Returns the invalid field without returning its possibly sensitive value.
    #[must_use]
    pub const fn field(self) -> &'static str {
        self.field
    }
}

impl fmt::Display for ModelValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid {}: {}", self.field, self.reason)
    }
}

impl std::error::Error for ModelValueError {}

#[derive(Clone, Eq, Hash, PartialEq)]
struct RedactedString(String);

impl fmt::Debug for RedactedString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

fn required(
    value: impl Into<String>,
    field: &'static str,
) -> Result<RedactedString, ModelValueError> {
    let value = value.into();
    if value.trim().is_empty() {
        Err(ModelValueError::new(field, "must not be blank"))
    } else if value.contains('\0') {
        Err(ModelValueError::new(field, "must not contain NUL"))
    } else if value.len() > MAX_MODEL_FIELD_BYTES {
        Err(ModelValueError::new(field, "exceeds model field limit"))
    } else {
        Ok(RedactedString(value))
    }
}

/// The closed set of storage implementations.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BackendKind {
    /// A filesystem attached to the worker.
    Local,
    /// Any supported NFS protocol version.
    Nfs,
    /// An SMB/CIFS share.
    Cifs,
    /// An S3-compatible object store.
    S3,
    /// A Hadoop filesystem.
    Hdfs,
}

impl BackendKind {
    /// Returns the stable lowercase representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Nfs => "nfs",
            Self::Cifs => "cifs",
            Self::S3 => "s3",
            Self::Hdfs => "hdfs",
        }
    }
}

impl fmt::Display for BackendKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for BackendKind {
    type Err = ModelValueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "local" => Ok(Self::Local),
            "nfs" => Ok(Self::Nfs),
            "cifs" => Ok(Self::Cifs),
            "s3" => Ok(Self::S3),
            "hdfs" => Ok(Self::Hdfs),
            _ => Err(ModelValueError::new("backend_kind", "unknown backend kind")),
        }
    }
}

/// The NFS dialect selected by configuration and negotiation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NfsVersion {
    /// NFS version 3.
    V3,
    /// NFS version 4.0.
    V4_0,
    /// NFS version 4.1.
    V4_1,
}

macro_rules! string_accessors {
    ($($name:ident),+ $(,)?) => {
        $(
            #[doc = concat!("Returns `", stringify!($name), "`.")]
            #[must_use]
            pub fn $name(&self) -> &str {
                &self.$name.0
            }
        )+
    };
}

/// Configuration for an attached local filesystem root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalConfig {
    root: RedactedString,
}

impl LocalConfig {
    /// Creates a local configuration.
    ///
    /// # Errors
    /// Returns an error when the root is blank, contains NUL, or exceeds the model field limit.
    pub fn new(root: impl Into<String>) -> Result<Self, ModelValueError> {
        Ok(Self {
            root: required(root, "root")?,
        })
    }

    string_accessors!(root);
}

/// Configuration for an NFS export.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NfsConfig {
    server: RedactedString,
    export: RedactedString,
    version: NfsVersion,
}

impl NfsConfig {
    /// Creates an NFS configuration with an explicit dialect.
    ///
    /// # Errors
    /// Returns an error when a required field is blank, contains NUL, or exceeds the model field limit.
    pub fn new(
        server: impl Into<String>,
        export: impl Into<String>,
        version: NfsVersion,
    ) -> Result<Self, ModelValueError> {
        Ok(Self {
            server: required(server, "server")?,
            export: required(export, "export")?,
            version,
        })
    }

    string_accessors!(server, export);

    /// Returns the selected NFS dialect.
    #[must_use]
    pub const fn version(&self) -> NfsVersion {
        self.version
    }
}

/// Configuration for a CIFS share.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CifsConfig {
    server: RedactedString,
    share: RedactedString,
}

impl CifsConfig {
    /// Creates a CIFS configuration.
    ///
    /// # Errors
    /// Returns an error when a required field is blank, contains NUL, or exceeds the model field limit.
    pub fn new(
        server: impl Into<String>,
        share: impl Into<String>,
    ) -> Result<Self, ModelValueError> {
        Ok(Self {
            server: required(server, "server")?,
            share: required(share, "share")?,
        })
    }

    string_accessors!(server, share);
}

/// Configuration for an S3-compatible bucket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3Config {
    endpoint: RedactedString,
    bucket: RedactedString,
    region: RedactedString,
}

impl S3Config {
    /// Creates an S3 configuration.
    ///
    /// # Errors
    /// Returns an error when a required field is blank, contains NUL, or exceeds the model field limit.
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
    ) -> Result<Self, ModelValueError> {
        Ok(Self {
            endpoint: required(endpoint, "endpoint")?,
            bucket: required(bucket, "bucket")?,
            region: required(region, "region")?,
        })
    }

    string_accessors!(endpoint, bucket, region);
}

/// Configuration for an HDFS namespace root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HdfsConfig {
    namenode: RedactedString,
    root: RedactedString,
}

impl HdfsConfig {
    /// Creates an HDFS configuration.
    ///
    /// # Errors
    /// Returns an error when a required field is blank, contains NUL, or exceeds the model field limit.
    pub fn new(
        namenode: impl Into<String>,
        root: impl Into<String>,
    ) -> Result<Self, ModelValueError> {
        Ok(Self {
            namenode: required(namenode, "namenode")?,
            root: required(root, "root")?,
        })
    }

    string_accessors!(namenode, root);
}

/// A typed backend construction request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendConfig {
    /// Local filesystem configuration.
    Local(LocalConfig),
    /// NFS configuration.
    Nfs(NfsConfig),
    /// CIFS configuration.
    Cifs(CifsConfig),
    /// S3 configuration.
    S3(S3Config),
    /// HDFS configuration.
    Hdfs(HdfsConfig),
}

impl BackendConfig {
    /// Returns the configured backend without inspecting a path.
    #[must_use]
    pub const fn kind(&self) -> BackendKind {
        match self {
            Self::Local(_) => BackendKind::Local,
            Self::Nfs(_) => BackendKind::Nfs,
            Self::Cifs(_) => BackendKind::Cifs,
            Self::S3(_) => BackendKind::S3,
            Self::Hdfs(_) => BackendKind::Hdfs,
        }
    }
}

/// A lossless protocol-neutral path relative to a configured backend root.
///
/// Backend adapters enforce their own root-containment and hierarchy rules. This common value
/// therefore preserves legal object keys such as `a//b`, `..`, and backslashes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StoragePath(String);

impl StoragePath {
    /// Returns the configured root itself.
    #[must_use]
    pub const fn root() -> Self {
        Self(String::new())
    }

    /// Creates an opaque root-relative path.
    ///
    /// # Errors
    /// Returns an error when the path contains NUL or exceeds the model field limit.
    pub fn new(value: impl Into<String>) -> Result<Self, ModelValueError> {
        let value = value.into();
        if value.contains('\0') {
            return Err(ModelValueError::new("storage_path", "must not contain NUL"));
        }
        if value.len() > MAX_MODEL_FIELD_BYTES {
            return Err(ModelValueError::new(
                "storage_path",
                "exceeds model field limit",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the slash-separated relative representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StoragePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable identity for one configured backend namespace.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BackendIdentity {
    kind: BackendKind,
    stable_id: RedactedString,
}

impl BackendIdentity {
    /// Creates an identity from a backend-produced stable identifier.
    ///
    /// # Errors
    /// Returns an error when the stable identifier is blank, contains NUL, or exceeds the model field limit.
    pub fn new(kind: BackendKind, stable_id: impl Into<String>) -> Result<Self, ModelValueError> {
        Ok(Self {
            kind,
            stable_id: required(stable_id, "stable_id")?,
        })
    }

    /// Returns the backend kind.
    #[must_use]
    pub const fn kind(&self) -> BackendKind {
        self.kind
    }

    /// Returns the opaque stable identifier.
    #[must_use]
    pub fn stable_id(&self) -> &str {
        &self.stable_id.0
    }
}

/// Resolution known for a storage timestamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimePrecision {
    /// Whole seconds.
    Seconds,
    /// Milliseconds.
    Milliseconds,
    /// Microseconds.
    Microseconds,
    /// Nanoseconds.
    Nanoseconds,
}

/// A signed Unix timestamp plus the precision actually observed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageTimestamp {
    unix_nanos: i128,
    precision: TimePrecision,
}

impl StorageTimestamp {
    /// Creates a timestamp aligned to the precision actually observed.
    ///
    /// # Errors
    /// Returns an error when the value is not aligned to the declared precision.
    pub const fn new(unix_nanos: i128, precision: TimePrecision) -> Result<Self, ModelValueError> {
        let quantum = match precision {
            TimePrecision::Seconds => 1_000_000_000,
            TimePrecision::Milliseconds => 1_000_000,
            TimePrecision::Microseconds => 1_000,
            TimePrecision::Nanoseconds => 1,
        };
        if unix_nanos % quantum == 0 {
            Ok(Self {
                unix_nanos,
                precision,
            })
        } else {
            Err(ModelValueError::new(
                "storage_timestamp",
                "value is not aligned to declared precision",
            ))
        }
    }

    /// Returns nanoseconds relative to the Unix epoch.
    #[must_use]
    pub const fn unix_nanos(self) -> i128 {
        self.unix_nanos
    }

    /// Returns the observed precision.
    #[must_use]
    pub const fn precision(self) -> TimePrecision {
        self.precision
    }
}

/// A protocol-neutral special filesystem entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpecialFileKind {
    /// A block device.
    BlockDevice,
    /// A character device.
    CharacterDevice,
    /// A named pipe.
    Fifo,
    /// A local-domain socket.
    Socket,
}

/// The neutral namespace kind observed for an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryKind {
    /// A regular file or object.
    File,
    /// A directory or concrete directory marker.
    Directory,
    /// A symbolic link.
    Symlink,
    /// An explicitly classified special file.
    Special(SpecialFileKind),
}

/// A neutral storage operation used for failure attribution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    /// Establish or validate a backend session.
    Connect,
    /// Traverse a namespace.
    Traverse,
    /// Observe one entry.
    Observe,
    /// Read payload bytes.
    Read,
    /// Prepare unpublished destination state.
    Prepare,
    /// Write payload bytes.
    Write,
    /// Verify content or metadata.
    Verify,
    /// Publish staged state.
    Publish,
    /// Mutate a namespace.
    Namespace,
    /// Observe or apply metadata.
    Metadata,
}

/// A backend-neutral failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureClass {
    /// Connectivity or transport loss.
    Connectivity,
    /// Authentication failed.
    Authentication,
    /// Permission was denied.
    PermissionDenied,
    /// The requested entry does not exist.
    NotFound,
    /// The caller supplied an invalid value.
    InvalidInput,
    /// The operation is unsupported.
    Unsupported,
    /// Existing state conflicts with the operation.
    Conflict,
    /// Storage or quota capacity was exhausted.
    Capacity,
    /// Data or protocol evidence is corrupt.
    Corruption,
    /// A backend protocol failed.
    Protocol,
    /// The caller cancelled the operation.
    Cancelled,
    /// An invariant failed inside the implementation.
    Internal,
}

/// Whether repeating work may succeed without an external state change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Transience {
    /// The same attempt may succeed after backoff; this says nothing about replay safety.
    Transient,
    /// Repeating unchanged work cannot succeed.
    Permanent,
    /// The adapter cannot certify either conclusion.
    Unknown,
}

macro_rules! failure_accessors {
    () => {
        /// Returns the failed operation.
        #[must_use]
        pub const fn operation(&self) -> Operation {
            self.operation
        }

        /// Returns the neutral failure class.
        #[must_use]
        pub const fn class(&self) -> FailureClass {
            self.class
        }

        /// Returns the transience classification.
        #[must_use]
        pub const fn transience(&self) -> Transience {
            self.transience
        }

        /// Returns the adapter-provided, already-redacted diagnostic.
        #[must_use]
        pub fn diagnostic(&self) -> &str {
            &self.diagnostic.0
        }
    };
}

/// A failure scoped to one entry while the backend session remains usable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntryOperationFailure {
    path: StoragePath,
    identity: Option<EntryFailureIdentity>,
    operation: Operation,
    class: FailureClass,
    transience: Transience,
    diagnostic: RedactedString,
}

impl EntryOperationFailure {
    /// Creates an entry-scoped failure from an already-redacted diagnostic.
    ///
    /// # Errors
    /// Returns an error when the diagnostic is blank, contains NUL, or exceeds the model field limit.
    pub fn new(
        path: StoragePath,
        operation: Operation,
        class: FailureClass,
        transience: Transience,
        diagnostic: impl Into<String>,
    ) -> Result<Self, ModelValueError> {
        Ok(Self {
            path,
            identity: None,
            operation,
            class,
            transience,
            diagnostic: required(diagnostic, "diagnostic")?,
        })
    }

    /// Returns the affected path.
    #[must_use]
    pub const fn path(&self) -> &StoragePath {
        &self.path
    }

    /// Attaches an opaque identity when a path alone cannot identify the failed entry.
    #[must_use]
    pub fn with_identity(mut self, identity: EntryFailureIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Returns the opaque failed-entry identity when the adapter supplied one.
    #[must_use]
    pub const fn identity(&self) -> Option<EntryFailureIdentity> {
        self.identity
    }

    failure_accessors!();
}

impl fmt::Display for EntryOperationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:?} failed at {} ({:?}, {:?})",
            self.operation, self.path, self.class, self.transience
        )
    }
}

impl std::error::Error for EntryOperationFailure {}

/// Opaque comparison identity for an entry that could not be fully observed.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct EntryFailureIdentity([u8; 32]);

impl EntryFailureIdentity {
    /// Derives a backend-bound identity from lossless adapter bytes.
    #[must_use]
    pub fn derive(backend: &BackendIdentity, bytes: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"data-mover/entry-failure-identity/v1\0");
        let kind = backend.kind().as_str().as_bytes();
        let stable_id = backend.stable_id().as_bytes();
        hasher.update(&(kind.len() as u64).to_le_bytes());
        hasher.update(kind);
        hasher.update(&(stable_id.len() as u64).to_le_bytes());
        hasher.update(stable_id);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
        Self(*hasher.finalize().as_bytes())
    }

    /// Returns the opaque identity bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for EntryFailureIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EntryFailureIdentity(<opaque-32-bytes>)")
    }
}

/// A session-wide failure that terminates the affected operation stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendSessionFailure {
    operation: Operation,
    class: FailureClass,
    transience: Transience,
    diagnostic: RedactedString,
}

impl BackendSessionFailure {
    /// Creates a session-scoped failure from an already-redacted diagnostic.
    ///
    /// # Errors
    /// Returns an error when the diagnostic is blank, contains NUL, or exceeds the model field limit.
    pub fn new(
        operation: Operation,
        class: FailureClass,
        transience: Transience,
        diagnostic: impl Into<String>,
    ) -> Result<Self, ModelValueError> {
        Ok(Self {
            operation,
            class,
            transience,
            diagnostic: required(diagnostic, "diagnostic")?,
        })
    }

    failure_accessors!();
}

impl fmt::Display for BackendSessionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "backend session {:?} failed ({:?}, {:?})",
            self.operation, self.class, self.transience
        )
    }
}

impl std::error::Error for BackendSessionFailure {}
