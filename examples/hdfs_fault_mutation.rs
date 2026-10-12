use std::path::{Path, PathBuf};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use data_mover::hdfs::build_hdfs_client;

mod hdfs_support;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Phase {
    Seed,
    Create,
    Close,
    Append,
    Rename,
    Metadata,
}

#[derive(Debug, Parser)]
#[command(about = "Nightly-only HDFS mutation checkpoint probe")]
struct Args {
    #[arg(long)]
    storage: String,
    #[arg(long)]
    path: String,
    #[arg(long)]
    destination_path: Option<String>,
    #[arg(long, value_enum)]
    phase: Phase,
    #[arg(long)]
    ready_file: Option<PathBuf>,
    #[arg(long)]
    go_file: Option<PathBuf>,
}

async fn checkpoint(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let (Some(ready), Some(go)) = (&args.ready_file, &args.go_file) else {
        return Err(data_mover::error::StorageError::ConfigError(
            "fault phase requires ready-file and go-file".to_string(),
        )
        .into());
    };
    tokio::fs::write(ready, b"ready\n").await?;
    while !go.exists() {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let (client, location) = build_hdfs_client(&args.storage, &hdfs_support::config())?;
    let resolved = hdfs_support::resolve(&location, Path::new(&args.path))?;
    let payload = Bytes::from(vec![0x5a; 4 * 1024 * 1024 + 17]);

    match args.phase {
        Phase::Seed => {
            let mut writer = client
                .create(
                    &resolved,
                    hdfs_native::WriteOptions::default()
                        .overwrite(true)
                        .create_parent(true),
                )
                .await?;
            Box::pin(writer.write_bytes(payload)).await?;
            Box::pin(writer.close()).await?;
        }
        Phase::Create => {
            checkpoint(&args).await?;
            let mut writer = client
                .create(
                    &resolved,
                    hdfs_native::WriteOptions::default()
                        .overwrite(true)
                        .create_parent(true),
                )
                .await?;
            Box::pin(writer.close()).await?;
        }
        Phase::Close => {
            let mut writer = client
                .create(
                    &resolved,
                    hdfs_native::WriteOptions::default()
                        .overwrite(true)
                        .create_parent(true),
                )
                .await?;
            Box::pin(writer.write_bytes(payload)).await?;
            checkpoint(&args).await?;
            Box::pin(writer.close()).await?;
        }
        Phase::Append => {
            let mut writer = client.append(&resolved).await?;
            checkpoint(&args).await?;
            Box::pin(writer.write_bytes(payload)).await?;
            Box::pin(writer.close()).await?;
        }
        Phase::Rename => {
            let destination = args.destination_path.as_deref().ok_or_else(|| {
                data_mover::error::StorageError::ConfigError(
                    "rename phase requires destination-path".to_string(),
                )
            })?;
            checkpoint(&args).await?;
            let destination = hdfs_support::resolve(&location, Path::new(destination))?;
            if let Some(parent) = Path::new(&destination).parent().and_then(Path::to_str) {
                client.mkdirs(parent, 0o755, true).await?;
            }
            client.rename(&resolved, &destination, true).await?;
        }
        Phase::Metadata => {
            checkpoint(&args).await?;
            client.set_permission(&resolved, 0o601).await?;
        }
    }
    Ok(())
}
