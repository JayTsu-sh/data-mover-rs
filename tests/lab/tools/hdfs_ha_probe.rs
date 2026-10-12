use std::path::{Path, PathBuf};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use data_mover::model::StoragePath;
use data_mover::storage::HdfsConfig;
use data_mover::storage::build_hdfs_client;
use data_mover::storage::{BackendConfig, HdfsBackendConfig, PreflightPolicy, connect_backend};
#[allow(dead_code)]
#[path = "../../../examples/support/copy.rs"]
mod copy_support;
use copy_support::Result;

#[path = "support/hdfs.rs"]
mod hdfs_support;

const FIXTURE_SIZE: u64 = 128 * 1024 * 1024 + 137;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Action {
    Seed,
    Inspect,
    MetadataLoop,
    Cleanup,
}

#[derive(Debug, Parser)]
#[command(about = "Conditional real-lab HDFS HA acceptance probe")]
struct Args {
    #[arg(long)]
    storage: String,
    #[arg(long)]
    config_dir: PathBuf,
    #[arg(long, value_enum)]
    action: Action,
    #[arg(long)]
    ready_file: Option<PathBuf>,
}

fn config(args: &Args) -> HdfsConfig {
    let mut config = hdfs_support::config();
    config.config_dir = Some(args.config_dir.clone());
    config
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = config(&args);
    let (client, location) = build_hdfs_client(&args.storage, &config)?;
    let storage = connect_backend(BackendConfig::Hdfs(HdfsBackendConfig {
        location: args.storage.clone(),
        client: config,
        block_size: None,
        ensure_dir: matches!(args.action, Action::Seed),
    }))
    .await?;
    let path = StoragePath::new("fixture.bin")?;

    match args.action {
        Action::Seed => {
            let resolved = hdfs_support::resolve(&location, Path::new("fixture.bin"))?;
            let mut writer = client
                .create(
                    &resolved,
                    hdfs_native::WriteOptions::default()
                        .overwrite(true)
                        .create_parent(true),
                )
                .await
                .map_err(|_| {
                    data_mover::error::StorageError::OperationError(
                        "HA fixture create failed".to_string(),
                    )
                })?;
            let pattern = Bytes::from((0_u8..=250).cycle().take(1024 * 1024).collect::<Vec<_>>());
            let pattern_len = u64::try_from(pattern.len()).map_err(|_| {
                data_mover::error::StorageError::OperationError(
                    "HA fixture pattern length does not fit u64".to_string(),
                )
            })?;
            let mut remaining = FIXTURE_SIZE;
            while remaining > 0 {
                let length = usize::try_from(remaining.min(pattern_len)).map_err(|_| {
                    data_mover::error::StorageError::OperationError(
                        "HA fixture chunk length does not fit usize".to_string(),
                    )
                })?;
                Box::pin(writer.write_bytes(pattern.slice(..length)))
                    .await
                    .map_err(|_| {
                        data_mover::error::StorageError::OperationError(
                            "HA fixture write failed".to_string(),
                        )
                    })?;
                remaining -= u64::try_from(length).map_err(|_| {
                    data_mover::error::StorageError::OperationError(
                        "HA fixture chunk length does not fit u64".to_string(),
                    )
                })?;
            }
            Box::pin(writer.close()).await.map_err(|_| {
                data_mover::error::StorageError::OperationError(
                    "HA fixture close failed".to_string(),
                )
            })?;
        }
        Action::Inspect => {
            let (size, hash) = copy_support::hash(&storage, &path).await?;
            println!("{size}\t{hash}");
        }
        Action::MetadataLoop => {
            let ready = args.ready_file.as_ref().ok_or_else(|| {
                data_mover::error::StorageError::ConfigError(
                    "metadata-loop requires ready-file".to_string(),
                )
            })?;
            storage
                .namespace(&PreflightPolicy::production())?
                .stat(&path)
                .await?;
            tokio::fs::write(ready, b"ready\n").await?;
            for _ in 0..300 {
                let entry = storage
                    .namespace(&PreflightPolicy::production())?
                    .stat(&path)
                    .await?;
                if entry.size != Some(FIXTURE_SIZE) {
                    return Err(data_mover::error::StorageError::OperationError(
                        "HA fixture size changed during failover".to_string(),
                    )
                    .into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
        Action::Cleanup => {
            if location.root() == "/" {
                return Err("refusing to remove HDFS filesystem root".into());
            }
            client.delete(location.root(), true).await?;
        }
    }
    Ok(())
}
