//! CIFS backend composition entry.
//!
//! The legacy `CifsStorage` path built on the smb-rs historical API (commit `c3ecf00`)
//! was removed together with its `StorageEnum::CIFS` variant (#150). CIFS is reachable
//! only through the architecture-ready role-based backend in
//! `crate::storage::backends::cifs`, which uses the smb-rs domain facade
//! (`smb-domain`: `Client → Session → Share → File / Directory`).
//!
//! This module keeps only the factory bridge that resolves transfer concurrency from
//! the environment and hands a connected share to the role-based backend.

use std::num::NonZeroUsize;

use crate::model::{BackendIdentity, BackendKind};
use crate::storage::Storage;
use crate::storage::backends::cifs;
use crate::transfer_concurrency::TransferConcurrency;

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
pub(crate) async fn create_cifs_role_storage(
    share: smb_domain::Share,
    root: Option<String>,
    ensure_dir: bool,
    identity: BackendIdentity,
) -> std::result::Result<Storage, Box<dyn std::error::Error>> {
    if ensure_dir && let Some(root) = root.as_deref() {
        cifs::ensure_root(&share, root).await?;
    }
    // The root's first entry, listed and opened once, decides whether this session may use file
    // ids as identity; see `protocol::probe_identity_mode`. Never fails the connect.
    let use_file_ids = cifs::probe_identity_mode(&share, root.as_deref()).await;
    let concurrency =
        TransferConcurrency::from_env(BackendKind::Cifs, TransferConcurrency::defaults(8, 8))?;
    cifs::connect(
        share,
        root,
        identity,
        NonZeroUsize::new(concurrency.read()).ok_or("invalid CIFS read depth")?,
        NonZeroUsize::new(concurrency.write()).ok_or("invalid CIFS write depth")?,
        use_file_ids,
    )
}
