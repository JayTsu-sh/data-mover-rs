use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use crate::hdfs::HdfsConfig;
use crate::model::{BackendIdentity, BackendKind};
use crate::storage::Storage;
use crate::url_redact::redact_storage_url;

#[derive(Clone, Debug)]
pub struct LocalBackendConfig {
    pub root: PathBuf,
    pub identity: BackendIdentity,
    pub read_concurrency: NonZeroUsize,
    pub write_concurrency: NonZeroUsize,
}

#[derive(Clone, Debug)]
pub struct NfsBackendConfig {
    pub url: String,
    pub identity: BackendIdentity,
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

/// Identity sent for an empty username under `CifsGuestPolicy::AllowUnsigned`; the NTLM
/// layer rejects a truly empty identity, so guest mapping is reached through a name the
/// server does not know.
const ANONYMOUS_PLACEHOLDER_USER: &str = "anonymous";

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
    pub identity: BackendIdentity,
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
            .field("identity", &self.identity)
            .finish()
    }
}

#[derive(Clone)]
pub struct S3BackendConfig {
    /// `s3://AK:SK@bucket.host[:port]/prefix`; the key pair is never printed by `Debug`.
    pub url: String,
    pub identity: BackendIdentity,
    pub block_size: Option<u64>,
}

impl fmt::Debug for S3BackendConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3BackendConfig")
            .field("url", &redact_storage_url(&self.url))
            .field("identity", &self.identity)
            .field("block_size", &self.block_size)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct HdfsBackendConfig {
    pub location: String,
    pub identity: BackendIdentity,
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
}

impl BackendConnectError {
    fn new(kind: BackendKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> BackendKind {
        self.kind
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

/// Connects exactly one explicitly selected backend without path-based type inference.
///
/// # Errors
/// Returns a backend-attributed connection error when configuration, authentication, root
/// validation, or role construction fails.
pub async fn connect_backend(config: BackendConfig) -> Result<Storage, BackendConnectError> {
    match config {
        BackendConfig::Local(config) => crate::storage::backends::local::connect_transfer(
            config.root,
            config.identity,
            config.read_concurrency,
            config.write_concurrency,
        )
        .map_err(|error| BackendConnectError::new(BackendKind::Local, error)),
        BackendConfig::Nfs(config) => crate::nfs::create_nfs_role_storage(
            &config.url,
            config.block_size,
            config.ensure_dir,
            config.identity,
        )
        .await
        .map_err(|error| BackendConnectError::new(BackendKind::Nfs, error)),
        BackendConfig::Cifs(config) => {
            let signing = match config.signing_policy {
                CifsSigningPolicy::Required => smb_domain::SigningPolicy::Required,
                CifsSigningPolicy::WhenRequired => smb_domain::SigningPolicy::WhenRequired,
            };
            let guest = match config.guest_policy {
                CifsGuestPolicy::Deny => smb_domain::GuestPolicy::Deny,
                CifsGuestPolicy::AllowUnsigned => smb_domain::GuestPolicy::AllowUnsigned,
            };
            let username = if config.username.is_empty()
                && config.guest_policy == CifsGuestPolicy::AllowUnsigned
            {
                ANONYMOUS_PLACEHOLDER_USER.to_owned()
            } else {
                config.username
            };
            let client = smb_domain::Client::with_policies(signing, guest);
            let target = smb_domain::ShareTarget::new(&config.server, &config.share)
                .map_err(|error| BackendConnectError::new(BackendKind::Cifs, error))?;
            let share = client
                .connect_share(
                    &target,
                    smb_domain::Credentials::ntlm(username, config.password),
                )
                .await
                .map_err(|error| BackendConnectError::new(BackendKind::Cifs, error))?;
            crate::cifs::create_cifs_role_storage(
                share,
                config.root,
                config.ensure_dir,
                config.identity,
            )
            .await
            .map_err(|error| BackendConnectError::new(BackendKind::Cifs, error))
        }
        BackendConfig::S3(config) => {
            let storage = crate::s3::S3Storage::new(&config.url, config.block_size)
                .await
                .map_err(|error| BackendConnectError::new(BackendKind::S3, error))?;
            storage
                .architecture_storage(config.identity)
                .map_err(|error| BackendConnectError::new(BackendKind::S3, error))
        }
        BackendConfig::Hdfs(config) => {
            let storage = crate::hdfs::create_hdfs_storage(
                &config.location,
                &config.client,
                config.block_size,
                config.ensure_dir,
            )
            .await
            .map_err(|error| BackendConnectError::new(BackendKind::Hdfs, error))?;
            storage
                .architecture_storage(config.identity)
                .map_err(|error| BackendConnectError::new(BackendKind::Hdfs, error))
        }
    }
}
