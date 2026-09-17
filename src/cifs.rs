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
use crate::transfer_concurrency::TransferConcurrency;

/// Builds the architecture-ready CIFS role handle from a connected smb-rs share.
///
/// With `ensure_dir`, missing components of `root` are created first (legacy
/// `ensure_root_exists`); without it the root is used lazily and never probed.
///
/// # Errors
/// Returns an error when the identity is not CIFS, when a root component exists but is not a
/// directory, when root creation fails, or when connected roles contradict capabilities.
pub async fn create_cifs_role_storage(
    share: smb_domain::Share,
    root: Option<String>,
    ensure_dir: bool,
    identity: BackendIdentity,
) -> std::result::Result<Storage, Box<dyn std::error::Error>> {
    if ensure_dir && let Some(root) = root.as_deref() {
        crate::storage::backends::cifs::ensure_root(&share, root).await?;
    }
    let concurrency =
        TransferConcurrency::from_env(BackendKind::Cifs, TransferConcurrency::defaults(8, 8))?;
    crate::storage::backends::cifs::connect(
        share,
        root,
        identity,
        NonZeroUsize::new(concurrency.read()).ok_or("invalid CIFS read depth")?,
        NonZeroUsize::new(concurrency.write()).ok_or("invalid CIFS write depth")?,
    )
}
