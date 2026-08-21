use std::path::Path;
use std::sync::atomic::Ordering;

use clap::Parser;
use data_mover::{EntryEnum, Result, StorageEnum, create_storage};

mod hdfs_support;

fn creation_options(location: &str, ensure_dir: bool) -> data_mover::CreateStorageOptions {
    data_mover::CreateStorageOptions {
        ensure_dir,
        backend: if location.starts_with("hdfs://") {
            data_mover::BackendConfig::Hdfs(hdfs_support::config())
        } else {
            data_mover::BackendConfig::Default
        },
        ..Default::default()
    }
}

fn mtimes_match(source: &EntryEnum, destination: &EntryEnum) -> bool {
    if matches!(destination, EntryEnum::S3(_)) {
        true
    } else if matches!(source, EntryEnum::HDFS(_)) || matches!(destination, EntryEnum::HDFS(_)) {
        source.get_mtime().div_euclid(1_000_000) == destination.get_mtime().div_euclid(1_000_000)
    } else {
        source.get_mtime() == destination.get_mtime()
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Args, creation_options, mtimes_match};

    #[test]
    fn qos_cli_accepts_explicit_bandwidth_burst_and_iops() {
        let args = Args::try_parse_from([
            "storage_copy",
            "--source",
            "/source",
            "--destination",
            "/destination",
            "--path",
            "payload.bin",
            "--qos-bandwidth",
            "8MiB/s",
            "--qos-burst-bytes",
            "65536",
            "--qos-iops",
            "32",
        ])
        .expect("QoS arguments should parse");

        assert_eq!(args.qos_bandwidth.as_deref(), Some("8MiB/s"));
        assert_eq!(args.qos_burst_bytes, Some(65_536));
        assert_eq!(args.qos_iops, Some(32));
    }

    #[test]
    fn qos_burst_requires_bandwidth() {
        let error = Args::try_parse_from([
            "storage_copy",
            "--source",
            "/source",
            "--destination",
            "/destination",
            "--path",
            "payload.bin",
            "--qos-burst-bytes",
            "65536",
        ])
        .expect_err("burst without bandwidth must be rejected");

        assert!(error.to_string().contains("--qos-bandwidth"));
    }

    fn hdfs(mtime: i64) -> data_mover::EntryEnum {
        data_mover::EntryEnum::HDFS(data_mover::HDFSEntry {
            name: "file".to_string(),
            relative_path: "file".into(),
            extension: None,
            size: 1,
            mtime,
            atime: 0,
            mode: 0o640,
            owner: "owner".to_string(),
            group: "group".to_string(),
            replication: Some(1),
            block_size: Some(1),
            is_dir: false,
        })
    }

    #[test]
    fn hdfs_mtime_comparison_uses_millisecond_precision() {
        assert!(mtimes_match(
            &hdfs(1_700_000_000_123_999_999),
            &hdfs(1_700_000_000_123_000_000)
        ));
        assert!(!mtimes_match(
            &hdfs(1_700_000_000_124_000_000),
            &hdfs(1_700_000_000_123_000_000)
        ));
    }

    #[test]
    fn s3_destination_mtime_comparison_is_skipped() {
        let s3 = data_mover::EntryEnum::S3(data_mover::S3Entry {
            name: "file".to_string(),
            relative_path: "file".to_string(),
            extension: None,
            size: 1,
            mtime: 1_000_000,
            tags: None,
            version_id: None,
            is_latest: true,
            is_delete_marker: false,
            version_count: None,
            is_dir: false,
        });

        assert!(mtimes_match(&hdfs(2_000_000), &s3));
        assert!(!mtimes_match(&s3, &hdfs(2_000_000)));
    }

    #[test]
    fn hdfs_urls_select_explicit_backend_configuration() {
        assert!(matches!(
            creation_options("hdfs://user@host:9000/root", true).backend,
            data_mover::BackendConfig::Hdfs(_)
        ));
        assert!(matches!(
            creation_options("/tmp/local", false).backend,
            data_mover::BackendConfig::Default
        ));
    }
}

#[derive(Debug, Parser)]
#[command(about = "Copy one file between two data-mover storage URLs")]
struct Args {
    /// Source storage URL.
    #[arg(long)]
    source: String,

    /// Destination storage URL.
    #[arg(long)]
    destination: String,

    /// Path relative to both storage roots.
    #[arg(long)]
    path: String,

    /// Delete the source only after destination publish, metadata and integrity succeed.
    #[arg(long)]
    delete_source: bool,

    /// Source-side sustained bandwidth limit, for example `8MiB/s`.
    #[arg(long)]
    qos_bandwidth: Option<String>,

    /// Hard source-read burst/request cap in bytes; requires `--qos-bandwidth`.
    #[arg(long, requires = "qos_bandwidth")]
    qos_burst_bytes: Option<u64>,

    /// Hard source-side protocol operation limit in operations per second.
    #[arg(long)]
    qos_iops: Option<u32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let qos = match (&args.qos_bandwidth, args.qos_burst_bytes, args.qos_iops) {
        (Some(bandwidth), Some(burst_bytes), iops) => Some(
            data_mover::QosManager::try_new_with_burst(bandwidth, burst_bytes, iops)?,
        ),
        (bandwidth, None, iops) if bandwidth.is_some() || iops.is_some() => Some(
            data_mover::QosManager::try_new(bandwidth.as_deref(), 1.0, iops)?,
        ),
        _ => None,
    };
    let source = create_storage(&args.source, creation_options(&args.source, false)).await?;
    let destination =
        create_storage(&args.destination, creation_options(&args.destination, true)).await?;
    let entry = source.get_metadata(Path::new(&args.path)).await?;

    StorageEnum::copy_file(
        &source,
        &destination,
        &entry,
        data_mover::CopyOptions {
            qos: qos.clone(),
            enable_integrity_check: true,
            is_source_reserved: !args.delete_source,
            ..Default::default()
        },
    )
    .await?;

    let copied = destination.get_metadata(Path::new(&args.path)).await?;
    if copied.get_size() != entry.get_size() {
        return Err(data_mover::error::StorageError::OperationError(format!(
            "destination size mismatch: expected {}, got {}",
            entry.get_size(),
            copied.get_size()
        )));
    }
    if matches!(
        (&entry, &copied),
        (
            EntryEnum::NAS(_) | EntryEnum::S3(_) | EntryEnum::HDFS(_),
            EntryEnum::NAS(_) | EntryEnum::HDFS(_)
        )
    ) && !mtimes_match(&entry, &copied)
    {
        return Err(data_mover::error::StorageError::OperationError(format!(
            "destination mtime mismatch: expected {}, got {}",
            entry.get_mtime(),
            copied.get_mtime()
        )));
    }
    if matches!(
        (&entry, &copied),
        (
            EntryEnum::NAS(_) | EntryEnum::HDFS(_),
            EntryEnum::NAS(_) | EntryEnum::HDFS(_)
        )
    ) {
        if copied.get_mode().map(|mode| mode & 0o7777) != entry.get_mode().map(|mode| mode & 0o7777)
        {
            return Err(data_mover::error::StorageError::OperationError(format!(
                "destination mode mismatch: expected {:?}, got {:?}",
                entry.get_mode().map(|mode| mode & 0o7777),
                copied.get_mode().map(|mode| mode & 0o7777)
            )));
        }
        if matches!((&entry, &copied), (EntryEnum::NAS(_), EntryEnum::NAS(_)))
            && (copied.get_uid() != entry.get_uid() || copied.get_gid() != entry.get_gid())
        {
            return Err(data_mover::error::StorageError::OperationError(format!(
                "destination ownership mismatch: expected {:?}:{:?}, got {:?}:{:?}",
                entry.get_uid(),
                entry.get_gid(),
                copied.get_uid(),
                copied.get_gid()
            )));
        }
    }
    if let (EntryEnum::HDFS(source), EntryEnum::HDFS(destination)) = (&entry, &copied)
        && (source.owner != destination.owner || source.group != destination.group)
    {
        return Err(data_mover::error::StorageError::OperationError(format!(
            "destination HDFS ownership mismatch: expected {}:{}, got {}:{}",
            source.owner, source.group, destination.owner, destination.group
        )));
    }

    println!(
        "copied and verified {} bytes and applicable metadata: {}",
        entry.get_size(),
        args.path
    );
    if let Some(qos) = qos {
        let stats = qos.stats();
        println!(
            "qos_stats\ttotal_bytes={}\ttotal_iops={}\telapsed_seconds={:.6}\tactual_mibps={:.6}\tactual_iops={:.6}",
            stats.total_bytes.load(Ordering::Relaxed),
            stats.total_iops.load(Ordering::Relaxed),
            stats.start_time.elapsed().as_secs_f64(),
            stats.actual_bandwidth_mibps(),
            stats.actual_iops(),
        );
    }
    Ok(())
}
