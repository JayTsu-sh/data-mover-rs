use std::path::{Path, PathBuf};

use clap::Parser;
use data_mover::model::StoragePath;
use data_mover::storage::build_hdfs_client;
use data_mover::storage::{BackendConfig, HdfsBackendConfig, PreflightPolicy, connect_backend};
use data_mover::storage::{HdfsConfig, HdfsKerberosCredentials};
use data_mover::transfer::{TransferRequest, transfer};
use tokio_util::sync::CancellationToken;
#[allow(dead_code)]
#[path = "../../../examples/support/copy.rs"]
mod copy_support;
#[allow(dead_code)]
#[path = "support/hdfs.rs"]
mod hdfs_support;
use copy_support::Result;

#[derive(Debug, Parser)]
#[command(about = "Exercise two client-scoped Kerberos HDFS backends in one process")]
struct Args {
    #[arg(long)]
    source: String,
    #[arg(long)]
    source_config_dir: PathBuf,
    #[arg(long)]
    source_keytab: PathBuf,
    #[arg(long)]
    destination: String,
    #[arg(long)]
    destination_config_dir: PathBuf,
    #[arg(long)]
    destination_keytab: PathBuf,
    #[arg(long, default_value = "fixture.bin")]
    path: PathBuf,
}

fn config(config_dir: PathBuf, keytab: PathBuf) -> HdfsConfig {
    HdfsConfig {
        config_dir: Some(config_dir),
        kerberos_credentials: Some(HdfsKerberosCredentials {
            keytab: Some(keytab),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source_config = config(args.source_config_dir, args.source_keytab);
    let (client, location) = build_hdfs_client(&args.source, &source_config)?;
    let source = connect_backend(BackendConfig::Hdfs(HdfsBackendConfig {
        location: args.source,
        client: source_config,
        block_size: None,
        ensure_dir: true,
    }))
    .await?;
    let destination = connect_backend(BackendConfig::Hdfs(HdfsBackendConfig {
        location: args.destination,
        client: config(args.destination_config_dir, args.destination_keytab),
        block_size: None,
        ensure_dir: true,
    }))
    .await?;
    let source_path = hdfs_support::resolve(&location, Path::new(&args.path))?;
    let payload = bytes::Bytes::from_static(b"two client-scoped Kerberos HDFS backends");
    let mut writer = client
        .create(
            &source_path,
            hdfs_native::WriteOptions::default()
                .overwrite(true)
                .create_parent(true),
        )
        .await?;
    Box::pin(writer.write_bytes(payload)).await?;
    Box::pin(writer.close()).await?;
    let path = StoragePath::new(args.path.to_str().ok_or("path must be UTF-8")?)?;
    copy_support::create_parent(&destination, &path).await?;
    transfer(TransferRequest::new(
        source.clone(),
        path.clone(),
        destination.clone(),
        path.clone(),
        copy_support::limits(2 << 20)?,
        CancellationToken::new(),
    ))
    .await?;
    let bytes = copy_support::verify(&source, &destination, &path).await?;
    let (_, hash) = copy_support::hash(&source, &path).await?;
    destination
        .namespace(&PreflightPolicy::production())?
        .delete(&path)
        .await?;
    source
        .namespace(&PreflightPolicy::production())?
        .delete(&path)
        .await?;
    println!("two-client Kerberos HDFS copy passed: {bytes} bytes, hash {hash}");
    Ok(())
}
