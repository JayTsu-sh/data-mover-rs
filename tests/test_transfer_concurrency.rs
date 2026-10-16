use std::num::NonZeroUsize;

use data_mover::storage::{BackendConfig, LocalBackendConfig, connect_backend};
use data_mover::transfer::InflightLimits;

#[tokio::test]
async fn local_backend_config_keeps_read_and_write_limits_independent()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let config = LocalBackendConfig {
        root: root.path().to_path_buf(),
        read_concurrency: NonZeroUsize::new(2).ok_or("non-zero read limit")?,
        write_concurrency: NonZeroUsize::new(8).ok_or("non-zero write limit")?,
    };
    assert_eq!(config.read_concurrency.get(), 2);
    assert_eq!(config.write_concurrency.get(), 8);

    let storage = connect_backend(BackendConfig::Local(config)).await?;
    assert_eq!(storage.kind(), data_mover::model::BackendKind::Local);
    Ok(())
}

#[test]
fn transfer_inflight_limits_reject_zero_and_unsafe_values() {
    assert!(InflightLimits::new(0, 1, 1).is_err());
    assert!(InflightLimits::new(1, 0, 1).is_err());
    assert!(InflightLimits::new(1, 1, 0).is_err());
    assert!(InflightLimits::new(1, 1024, 1).is_ok());
    if usize::BITS > u32::BITS {
        assert!(InflightLimits::new(u32::MAX as usize + 1, 1, 1).is_err());
    }
}
