use std::num::NonZeroUsize;
use std::path::Path;

use bytes::Bytes;
use data_mover::model::{BackendIdentity, BackendKind, ObservationPlan, StoragePath};
use data_mover::storage::Storage;
use data_mover::transfer::{InflightLimits, TransferIdentity, TransferRequest, transfer};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOrder, TraversalRequest, TraversalSource as _,
};
use data_mover::{
    DataChunk, HDFSStorage, HdfsConfig, HdfsKerberosCredentials, HdfsLocation, TransferConcurrency,
    create_hdfs_storage,
};
use hdfs_native::WriteOptions;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn lab_config() -> HdfsConfig {
    HdfsConfig {
        config_dir: std::env::var_os("LAB_HDFS_CONFIG_DIR").map(Into::into),
        kerberos_credentials: std::env::var_os("LAB_HDFS_KEYTAB").map(|keytab| {
            HdfsKerberosCredentials {
                keytab: Some(keytab.into()),
                ..Default::default()
            }
        }),
        ..Default::default()
    }
}

fn lab_location(case: &str) -> TestResult<String> {
    let root = HdfsLocation::parse(&std::env::var("LAB_HDFS_RUN_ROOT")?)?;
    let endpoint = root
        .endpoint()
        .strip_prefix("hdfs://")
        .ok_or("invalid endpoint")?;
    let user = utf8_percent_encode(root.user(), NON_ALPHANUMERIC);
    Ok(format!("hdfs://{user}@{endpoint}{}/{case}", root.root()))
}

async fn create_file(storage: &HDFSStorage, path: &str, data: Bytes) -> TestResult {
    let path = storage.resolve_path(Path::new(path))?;
    let mut writer = storage
        .client()
        .create(&path, WriteOptions::default().overwrite(true))
        .await?;
    Box::pin(writer.write_bytes(data)).await?;
    Box::pin(writer.close()).await?;
    Ok(())
}

async fn observes_nested_source(storage: &Storage) -> TestResult<bool> {
    let traversal = StorageTraversalSource::new(storage)?;
    let mut session = traversal.traverse(TraversalRequest {
        root: StoragePath::root(),
        order: TraversalOrder::Admission,
        max_inflight_operations: NonZeroUsize::new(2).ok_or("invalid inflight")?,
        max_buffered_items: NonZeroUsize::new(2).ok_or("invalid buffer")?,
        observation_plan: ObservationPlan::default(),
        cancel: CancellationToken::new(),
    });
    let mut observed = false;
    while let Some(item) = session.next_item().await {
        if let TraversalItem::Entry(entry) = item {
            observed |= entry.path().as_str() == "nested/source.bin";
        }
    }
    session.finish().await?;
    Ok(observed)
}

async fn transfer_and_assert(
    source: Storage,
    destination: Storage,
    destination_backend: &HDFSStorage,
    payload: &Bytes,
) -> TestResult {
    let outcome = transfer(TransferRequest::new(
        TransferIdentity::new("hdfs-architecture-contract")?,
        source,
        StoragePath::new("nested/source.bin")?,
        destination,
        StoragePath::new("published/final.bin")?,
        InflightLimits::new(2, 128 * 1024, 2)?,
        CancellationToken::new(),
    ))
    .await?;
    assert_eq!(outcome.blake3, *blake3::hash(payload).as_bytes());
    let published = destination_backend
        .open_file(Path::new("published/final.bin"))
        .await?;
    assert_eq!(
        destination_backend
            .read_at(&published, 0, payload.len() as u64)
            .await?,
        *payload
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn architecture_roles_traverse_stream_verify_and_overwrite() -> TestResult {
    let source = create_hdfs_storage(
        &lab_location("architecture-source")?,
        &lab_config(),
        None,
        true,
    )
    .await?;
    let destination = create_hdfs_storage(
        &lab_location("architecture-destination")?,
        &lab_config(),
        None,
        true,
    )
    .await?;
    let payload = Bytes::from(vec![0x5a; 1024 * 1024 + 31]);
    create_file(&source, "nested/source.bin", payload.clone()).await?;
    let source_roles = source.architecture_storage(BackendIdentity::new(
        BackendKind::Hdfs,
        "architecture-source",
    )?)?;
    let destination_roles = destination.architecture_storage(BackendIdentity::new(
        BackendKind::Hdfs,
        "architecture-destination",
    )?)?;
    assert!(observes_nested_source(&source_roles).await?);
    destination
        .create_dir_all(Path::new("published"), 0o755)
        .await?;
    create_file(
        &destination,
        "published/final.bin",
        Bytes::from_static(b"old"),
    )
    .await?;
    transfer_and_assert(source_roles, destination_roles, &destination, &payload).await?;
    source.delete_storage_root().await?;
    destination.delete_storage_root().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn native_writer_reorders_bounded_out_of_order_chunks() -> TestResult {
    let storage = create_hdfs_storage(
        &lab_location("architecture-reorder")?,
        &lab_config(),
        None,
        true,
    )
    .await?
    .with_transfer_concurrency(TransferConcurrency::new(2, 3)?);
    let payload = Bytes::from(vec![0x3c; 3 * 1024 * 1024]);
    let (sender, receiver) = mpsc::channel(3);
    for (offset, range) in [
        (1024 * 1024, 1024 * 1024..2 * 1024 * 1024),
        (0, 0..1024 * 1024),
        (2 * 1024 * 1024, 2 * 1024 * 1024..3 * 1024 * 1024),
    ] {
        sender
            .send(DataChunk {
                offset,
                data: payload.slice(range),
            })
            .await?;
    }
    drop(sender);
    storage
        .write_stream(
            receiver,
            Path::new("reordered.bin"),
            payload.len() as u64,
            0o640,
            None,
        )
        .await?;
    let file = storage.open_file(Path::new("reordered.bin")).await?;
    assert_eq!(
        storage.read_at(&file, 0, payload.len() as u64).await?,
        payload
    );
    storage.delete_storage_root().await?;
    Ok(())
}
