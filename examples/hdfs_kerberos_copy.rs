use std::path::{Path, PathBuf};

use clap::Parser;
use data_mover::{
    BackendConfig, CopyOptions, CreateStorageOptions, HdfsConfig, HdfsKerberosCredentials, Result,
    StorageEnum, create_storage,
};

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

fn options(config_dir: PathBuf, keytab: PathBuf) -> CreateStorageOptions {
    CreateStorageOptions {
        ensure_dir: true,
        backend: BackendConfig::Hdfs(HdfsConfig {
            config_dir: Some(config_dir),
            kerberos_credentials: Some(HdfsKerberosCredentials::Keytab { keytab }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source = create_storage(
        &args.source,
        options(args.source_config_dir, args.source_keytab),
    )
    .await?;
    let destination = create_storage(
        &args.destination,
        options(args.destination_config_dir, args.destination_keytab),
    )
    .await?;
    let StorageEnum::HDFS(source_hdfs) = &source else {
        return Err(data_mover::error::StorageError::MismatchedType);
    };
    let source_path = source_hdfs.resolve_path(&args.path)?;
    let payload = bytes::Bytes::from_static(b"two client-scoped Kerberos HDFS backends");
    let mut writer = source_hdfs
        .client()
        .create(
            &source_path,
            hdfs_native::WriteOptions::default()
                .overwrite(true)
                .create_parent(true),
        )
        .await
        .map_err(|error| {
            data_mover::error::StorageError::OperationError(format!(
                "failed to seed Kerberos HDFS source: {error}"
            ))
        })?;
    Box::pin(writer.write_bytes(payload.clone()))
        .await
        .map_err(|error| {
            data_mover::error::StorageError::OperationError(format!(
                "failed to write Kerberos HDFS source: {error}"
            ))
        })?;
    Box::pin(writer.close()).await.map_err(|error| {
        data_mover::error::StorageError::OperationError(format!(
            "failed to close Kerberos HDFS source: {error}"
        ))
    })?;

    let entry = source.get_metadata(&args.path).await?;
    StorageEnum::copy_file(
        &source,
        &destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: true,
            ..Default::default()
        },
    )
    .await?;
    let copied = destination.get_metadata(&args.path).await?;
    let source_hash = source
        .compute_hash(Path::new(&args.path), entry.get_size())
        .await?;
    let destination_hash = destination
        .compute_hash(Path::new(&args.path), copied.get_size())
        .await?;
    if entry.get_size() != copied.get_size() || source_hash != destination_hash {
        return Err(data_mover::error::StorageError::OperationError(
            "Kerberos HDFS copy verification failed".to_string(),
        ));
    }
    destination.delete_file(&copied).await?;
    source.delete_file(&entry).await?;
    println!(
        "two-client Kerberos HDFS copy passed: {} bytes, hash {source_hash}",
        entry.get_size()
    );
    Ok(())
}
