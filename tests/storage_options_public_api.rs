//! Compile-time coverage for the role-based public configuration and transfer API.

use std::num::NonZeroUsize;

use data_mover::storage::{BackendConfig, LocalBackendConfig};
use data_mover::transfer::TransferPolicy;

#[test]
fn public_role_and_transfer_modules_expose_the_supported_local_path()
-> Result<(), Box<dyn std::error::Error>> {
    let config = BackendConfig::Local(LocalBackendConfig {
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        root: std::env::temp_dir(),
        read_concurrency: NonZeroUsize::new(2).ok_or("non-zero read limit")?,
        write_concurrency: NonZeroUsize::new(7).ok_or("non-zero write limit")?,
    });
    assert!(matches!(config, BackendConfig::Local(_)));

    assert_eq!(TransferPolicy::default(), TransferPolicy::Checkpointed);
    Ok(())
}
