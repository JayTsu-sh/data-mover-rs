use std::path::{Path, PathBuf};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use data_mover::storage::build_hdfs_client;

#[path = "support/hdfs.rs"]
mod hdfs_support;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Phase {
    Seed,
    Create,
    Close,
    Append,
    /// Fence the harness-owned writer after SIGKILL and wait for a closed file.
    RecoverLease,
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

async fn recover_lease_until_closed<F, Fut>(
    mut recover: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool, hdfs_native::HdfsError>>,
{
    tokio::time::timeout(std::time::Duration::from_mins(2), async {
        loop {
            match recover().await {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(
                    hdfs_native::HdfsError::RPCError(class, _)
                    | hdfs_native::HdfsError::FatalRPCError(class, _),
                ) if class.rsplit('.').next() == Some("RecoveryInProgressException") => {}
                Err(error) => return Err(Box::<dyn std::error::Error>::from(error)),
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    })
    .await
    .map_err(|_| "HDFS lease recovery did not close the file within 120 seconds")?
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
        Phase::RecoverLease => {
            recover_lease_until_closed(|| client.recover_lease(&resolved))
                .await
                .map_err(|error| {
                    format!(
                        "HDFS fault lease recovery failed: {}",
                        error.to_string().replace(&resolved, &args.path)
                    )
                })?;
            println!("HDFS fault-owned file lease recovered and closed");
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

#[cfg(test)]
mod tests {
    use super::recover_lease_until_closed;
    use hdfs_native::HdfsError;
    use std::{cell::Cell, future::ready, time::Duration};

    #[tokio::test(start_paused = true)]
    async fn recovery_waits_for_closed_including_recovery_in_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        let calls = Cell::new(0);
        let started = tokio::time::Instant::now();
        recover_lease_until_closed(|| {
            calls.set(calls.get() + 1);
            ready(match calls.get() {
                1 => Err(HdfsError::RPCError(
                    "org.apache.hadoop.hdfs.protocol.RecoveryInProgressException".into(),
                    "recovering".into(),
                )),
                2 => Ok(false),
                _ => Ok(true),
            })
        })
        .await?;
        assert_eq!(calls.get(), 3);
        assert_eq!(started.elapsed(), Duration::from_millis(500));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_deadline_includes_a_stuck_rpc_and_false_results() {
        let started = tokio::time::Instant::now();
        let stuck = recover_lease_until_closed(std::future::pending).await;
        assert!(stuck.is_err());
        assert_eq!(started.elapsed(), Duration::from_mins(2));
        let started = tokio::time::Instant::now();
        assert!(
            recover_lease_until_closed(|| ready(Ok(false)))
                .await
                .is_err()
        );
        assert_eq!(started.elapsed(), Duration::from_mins(2));
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_does_not_retry_unrelated_errors() {
        let calls = Cell::new(0);
        let result = recover_lease_until_closed(|| {
            calls.set(calls.get() + 1);
            ready(Err(HdfsError::FileNotFound("missing.bin".into())))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.get(), 1);
    }
}
