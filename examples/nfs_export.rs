//! Lists the exports of an NFS server (`e2e-nfs` step 3).
//!
//! ```text
//! cargo run --example nfs_export -- 'nfs://host/export?noresvport=true'   # or just: host
//! ```
//!
//! A full URL is passed through, so its options apply to the MOUNT query — `noresvport=true`
//! avoids binding a privileged local port, which an unprivileged user cannot do.

use std::env;

use data_mover::error::StorageError;
use data_mover::{NFSStorage, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let target = env::args().nth(1).ok_or_else(|| {
        StorageError::ConfigError("usage: nfs_export <host | nfs://host/...?options>".to_string())
    })?;
    let entries = NFSStorage::list_exports(&target).await?;
    for entry in entries {
        println!("{entry:?}");
    }
    Ok(())
}
