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
/// # Errors
/// Returns an error when the identity is not CIFS or connected roles contradict capabilities.
pub fn create_cifs_role_storage(
    share: smb_domain::Share,
    root: Option<String>,
    identity: BackendIdentity,
) -> std::result::Result<Storage, Box<dyn std::error::Error>> {
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
