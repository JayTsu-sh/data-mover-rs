//! Private CIFS connection, policy negotiation, and role composition.

use std::num::NonZeroUsize;

use crate::model::{BackendIdentity, BackendKind};
use crate::storage::{
    BackendConnectError, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, Storage, endpoint,
};
use crate::transfer_concurrency::TransferConcurrency;

/// NTLM requires a nonempty name even when the caller opts into server guest mapping.
const ANONYMOUS_PLACEHOLDER_USER: &str = "anonymous";

pub(crate) async fn connect_backend(
    config: CifsBackendConfig,
) -> Result<Storage, BackendConnectError> {
    // Derive identity before network I/O so malformed endpoints fail fast.
    let identity = endpoint::smb(&config.server, &config.share, config.root.as_deref())
        .map_err(|error| BackendConnectError::new(BackendKind::Cifs, error))?;
    let signing = match config.signing_policy {
        CifsSigningPolicy::Required => smb_domain::SigningPolicy::Required,
        CifsSigningPolicy::WhenRequired => smb_domain::SigningPolicy::WhenRequired,
    };
    let guest = match config.guest_policy {
        CifsGuestPolicy::Deny => smb_domain::GuestPolicy::Deny,
        CifsGuestPolicy::AllowUnsigned => smb_domain::GuestPolicy::AllowUnsigned,
    };
    let username =
        if config.username.is_empty() && config.guest_policy == CifsGuestPolicy::AllowUnsigned {
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
    create_cifs_role_storage(share, config.root, config.ensure_dir, identity)
        .await
        .map_err(|error| BackendConnectError::new(BackendKind::Cifs, error))
}

/// Builds the architecture-ready CIFS role handle from a connected smb-rs share.
///
/// With `ensure_dir`, missing components of `root` are created first (legacy
/// `ensure_root_exists`); without it the root is never created here. Either way the connect
/// then runs one identity probe (the root listed once, the first of its entries that will open
/// opened once) to decide whether this session may use file ids as identity; the probe never
/// fails the connect, it only falls back to path-scoped identities with a warning.
///
/// # Errors
/// Returns an error when the identity is not CIFS, when a root component exists but is not a
/// directory, when root creation fails, or when connected roles contradict capabilities.
async fn create_cifs_role_storage(
    share: smb_domain::Share,
    root: Option<String>,
    ensure_dir: bool,
    identity: BackendIdentity,
) -> std::result::Result<Storage, Box<dyn std::error::Error>> {
    if ensure_dir && let Some(root) = root.as_deref() {
        super::ensure_root(&share, root).await?;
    }
    // The root's first entry, listed and opened once, decides whether this session may use file
    // ids as identity; see `protocol::probe_identity_mode`. Never fails the connect.
    let use_file_ids = super::probe_identity_mode(&share, root.as_deref()).await;
    let concurrency =
        TransferConcurrency::from_env(BackendKind::Cifs, TransferConcurrency::defaults(8, 8))?;
    super::connect(
        share,
        root,
        identity,
        NonZeroUsize::new(concurrency.read()).ok_or("invalid CIFS read depth")?,
        NonZeroUsize::new(concurrency.write()).ok_or("invalid CIFS write depth")?,
        use_file_ids,
    )
}
