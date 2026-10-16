use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use crate::model::{BackendIdentity, BackendKind, BackendSessionFailure};
use crate::storage::{HdfsConfig, Storage, backends, endpoint};
use crate::url_redact::redact_storage_url;

#[derive(Clone, Debug)]
pub struct LocalBackendConfig {
    pub root: PathBuf,
    pub read_concurrency: NonZeroUsize,
    pub write_concurrency: NonZeroUsize,
}

#[derive(Clone, Debug)]
pub struct NfsBackendConfig {
    pub url: String,
    pub block_size: Option<u64>,
    pub ensure_dir: bool,
}

/// Controls SMB integrity negotiation for CIFS connections.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CifsSigningPolicy {
    /// Require signed or encrypted authenticated traffic.
    Required,
    /// Omit ordinary signing when the server permits it; mandatory protocol
    /// protection and encryption integrity remain enabled.
    /// Without encryption, unsigned traffic has no SMB message integrity protection.
    #[default]
    WhenRequired,
}

/// Controls whether a session the server downgraded to guest or anonymous may proceed.
///
/// Servers that map unknown or password-less users to a guest account (ONTAP
/// `guest-unix-user`, Samba `map to guest`) answer with an unsigned guest session that has
/// no session key; without `AllowUnsigned` the connection is refused. An empty `username`
/// together with `AllowUnsigned` sends a placeholder identity so the server can apply its
/// guest mapping.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CifsGuestPolicy {
    /// Refuse guest and anonymous sessions (message integrity is preserved).
    #[default]
    Deny,
    /// Accept unsigned guest or anonymous sessions when the server does not require signing.
    AllowUnsigned,
}

#[derive(Clone)]
pub struct CifsBackendConfig {
    pub server: String,
    pub share: String,
    pub root: Option<String>,
    /// Create missing components of `root` at connect time (parity with the NFS / HDFS
    /// `ensure_dir`). `false` never probes the root; a missing root then fails lazily on first
    /// use.
    pub ensure_dir: bool,
    pub username: String,
    pub password: String,
    /// Signing policy negotiated independently with each server.
    pub signing_policy: CifsSigningPolicy,
    /// Guest / anonymous session policy; see [`CifsGuestPolicy`].
    pub guest_policy: CifsGuestPolicy,
}

impl fmt::Debug for CifsBackendConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CifsBackendConfig")
            .field("server", &self.server)
            .field("share", &self.share)
            .field("root", &self.root)
            .field("ensure_dir", &self.ensure_dir)
            .field("username", &"<redacted>")
            .field("password", &"<redacted>")
            .field("signing_policy", &self.signing_policy)
            .field("guest_policy", &self.guest_policy)
            .finish()
    }
}

#[derive(Clone)]
pub struct S3BackendConfig {
    /// `s3://AK:SK@bucket.host[:port]/prefix`; the key pair is never printed by `Debug`.
    pub url: String,
    pub block_size: Option<u64>,
    /// Objects of at most this many bytes are written as one `PutObject` to the final key,
    /// verified after publication; larger ones go through a multipart upload. `None` = 8 MiB;
    /// a value outside `[5 MiB, 5 GiB]` is refused at connect. Such an object is held in memory
    /// whole until it is published, outside the inflight read budget: the threshold times the
    /// number of concurrent transfers bounds that memory.
    pub single_put_threshold: Option<u64>,
}

impl fmt::Debug for S3BackendConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3BackendConfig")
            .field("url", &redact_storage_url(&self.url))
            .field("block_size", &self.block_size)
            .field("single_put_threshold", &self.single_put_threshold)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct HdfsBackendConfig {
    pub location: String,
    pub client: HdfsConfig,
    pub block_size: Option<u64>,
    pub ensure_dir: bool,
}

#[derive(Clone, Debug)]
pub enum BackendConfig {
    Local(LocalBackendConfig),
    Nfs(NfsBackendConfig),
    Cifs(CifsBackendConfig),
    S3(S3BackendConfig),
    Hdfs(HdfsBackendConfig),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendConnectError {
    kind: BackendKind,
    message: String,
    session: Option<BackendSessionFailure>,
}

impl BackendConnectError {
    pub(super) fn new(kind: BackendKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
            session: None,
        }
    }

    /// A connection refused by the roles: typed when they say why (a `BackendSessionFailure`,
    /// e.g. S3's unanswered `GetBucketVersioning`, P7 Q-A), with its diagnostic in the message.
    pub(super) fn from_roles(kind: BackendKind, error: &(dyn std::error::Error + 'static)) -> Self {
        let session = error.downcast_ref::<BackendSessionFailure>().cloned();
        let message = session.as_ref().map_or_else(
            || error.to_string(),
            |session| format!("{session}: {}", session.diagnostic()),
        );
        Self {
            kind,
            message,
            session,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> BackendKind {
        self.kind
    }

    /// The typed failure behind this error, when the connection's roles reported one: its class
    /// and transience say whether connecting again may help (S3: an unanswered
    /// `GetBucketVersioning` is `Connectivity` / `Transient`).
    #[must_use]
    pub const fn session_failure(&self) -> Option<&BackendSessionFailure> {
        self.session.as_ref()
    }
}

impl fmt::Display for BackendConnectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "failed to connect {:?} backend: {}",
            self.kind, self.message
        )
    }
}

impl std::error::Error for BackendConnectError {}

/// The identity `config` connects as: its canonical endpoint (ADR-0006), derived without any
/// network I/O — protocol, address, share or bucket, and prefix, never credentials. Every
/// [`Storage`] returned by [`connect_backend`] for this config carries exactly this identity, so a
/// caller can rebuild it offline, e.g. to decode snapshots taken from this endpoint.
///
/// A Local root is canonicalized (symlinks resolved) and must exist. An S3 or HDFS location is
/// parsed as its backend parses it; see `src/storage/endpoint.rs` for the rules.
///
/// # Errors
/// Returns a backend-attributed error when the configuration cannot name an endpoint.
pub fn endpoint_identity(config: &BackendConfig) -> Result<BackendIdentity, BackendConnectError> {
    match config {
        BackendConfig::Local(config) => std::fs::canonicalize(&config.root)
            .map_err(|error| BackendConnectError::new(BackendKind::Local, error))
            .and_then(|root| {
                endpoint::local(&root)
                    .map_err(|error| BackendConnectError::new(BackendKind::Local, error))
            }),
        BackendConfig::Nfs(config) => endpoint::nfs(&config.url)
            .map_err(|error| BackendConnectError::new(BackendKind::Nfs, error)),
        BackendConfig::Cifs(config) => {
            endpoint::smb(&config.server, &config.share, config.root.as_deref())
                .map_err(|error| BackendConnectError::new(BackendKind::Cifs, error))
        }
        BackendConfig::S3(config) => backends::s3::endpoint_identity(&config.url)
            .map_err(|error| BackendConnectError::new(BackendKind::S3, error)),
        BackendConfig::Hdfs(config) => {
            backends::hdfs::endpoint_identity(&config.location, &config.client)
                .map_err(|error| BackendConnectError::new(BackendKind::Hdfs, error))
        }
    }
}

/// Connects exactly one explicitly selected backend without path-based type inference.
///
/// The returned storage's [`Storage::identity`] is [`endpoint_identity`] of `config`.
///
/// # Errors
/// Returns a backend-attributed connection error when configuration, authentication, root
/// validation, or role construction fails.
pub async fn connect_backend(config: BackendConfig) -> Result<Storage, BackendConnectError> {
    match config {
        BackendConfig::Local(config) => backends::local::connect_backend(config),
        BackendConfig::Nfs(config) => backends::nfs::connect_backend(config).await,
        BackendConfig::Cifs(config) => backends::cifs::connect_backend(config).await,
        BackendConfig::S3(config) => backends::s3::connect_backend(config).await,
        BackendConfig::Hdfs(config) => backends::hdfs::connect_backend(config).await,
    }
}
