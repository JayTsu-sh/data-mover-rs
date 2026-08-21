use std::path::{Path, PathBuf};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use data_mover::{HdfsConfig, Result, StorageEnum};

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
    let storage = data_mover::create_hdfs_storage(
        &args.storage,
        &config(&args),
        None,
        matches!(args.action, Action::Seed),
    )
    .await?;
    let relative = Path::new("fixture.bin");

    match args.action {
        Action::Seed => {
            let resolved = storage.resolve_path(relative)?;
            let mut writer = storage
                .client()
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
            let storage = StorageEnum::HDFS(storage);
            let entry = storage.get_metadata(relative).await?;
            let size = entry.get_size();
            let hash = storage.compute_hash(relative, size).await?;
            println!("{size}\t{hash}");
        }
        Action::MetadataLoop => {
            let ready = args.ready_file.as_ref().ok_or_else(|| {
                data_mover::error::StorageError::ConfigError(
                    "metadata-loop requires ready-file".to_string(),
                )
            })?;
            storage.get_metadata(relative).await?;
            tokio::fs::write(ready, b"ready\n").await?;
            for _ in 0..300 {
                let entry = storage.get_metadata(relative).await?;
                if entry.size != FIXTURE_SIZE {
                    return Err(data_mover::error::StorageError::OperationError(
                        "HA fixture size changed during failover".to_string(),
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
        Action::Cleanup => storage.delete_storage_root().await?,
    }
    Ok(())
}
