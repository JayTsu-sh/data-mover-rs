//! Explicit backend construction and endpoint identity through storage roles.

use std::num::NonZeroUsize;

use data_mover::model::BackendKind;
use data_mover::storage::{
    BackendConfig, HdfsBackendConfig, LocalBackendConfig, connect_backend, endpoint_identity,
};

#[tokio::test]
async fn an_explicit_local_backend_connects_with_its_canonical_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let config = BackendConfig::Local(LocalBackendConfig {
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        root: root.path().to_path_buf(),
        read_concurrency: NonZeroUsize::new(4).ok_or("invalid read concurrency")?,
        write_concurrency: NonZeroUsize::new(8).ok_or("invalid write concurrency")?,
    });
    let expected = endpoint_identity(&config)?;
    let storage = connect_backend(config).await?;

    assert_eq!(storage.kind(), BackendKind::Local);
    assert_eq!(storage.identity(), &expected);
    Ok(())
}

#[test]
fn malformed_hdfs_endpoint_is_rejected_before_connection() {
    let Err(error) = endpoint_identity(&BackendConfig::Hdfs(HdfsBackendConfig {
        location: "hdfs://".to_owned(),
        client: data_mover::storage::HdfsConfig::default(),
        block_size: None,
        ensure_dir: false,
    })) else {
        panic!("malformed HDFS endpoint unexpectedly connected");
    };
    assert_eq!(error.kind(), BackendKind::Hdfs);
}
