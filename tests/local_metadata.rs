use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use data_mover::model::{
    FailureClass, MetadataObservation, MetadataObservations, ObservationMode, ObservationPlan,
    Operation, StoragePath, StorageTimestamp, TimePrecision, TimestampMetadata,
};
use data_mover::storage::Storage;
use data_mover::storage::{MetadataMutation, Namespace};

mod common;

static TEMP_DIR_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(test_name: &str) -> std::io::Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let sequence = TEMP_DIR_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "data-mover-{test_name}-{}-{timestamp}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn local(root: &Path) -> Result<Storage, Box<dyn std::error::Error>> {
    Ok(common::connect_local(root, 4).await?)
}

fn namespace(
    storage: &Storage,
) -> Result<std::sync::Arc<dyn Namespace>, Box<dyn std::error::Error>> {
    Ok(storage.namespace()?)
}

async fn observe_timestamps(
    storage: &Storage,
    path: &StoragePath,
) -> Result<MetadataObservations, Box<dyn std::error::Error>> {
    Ok(storage
        .metadata()?
        .observe(
            path,
            ObservationPlan::default().with_timestamps(ObservationMode::Required),
        )
        .await?)
}

#[tokio::test]
async fn local_namespace_classifies_missing_path() -> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new("missing-metadata")?;
    let path = StoragePath::new("missing-file")?;
    let result = namespace(&local(root.path()).await?)?.stat(&path).await;

    assert!(matches!(
        result,
        Err(data_mover::storage::StorageRoleFailure::Entry(error))
            if error.class() == FailureClass::NotFound
                && error.operation() == Operation::Observe
                && error.to_string().contains("missing-file")
    ));
    Ok(())
}

#[tokio::test]
async fn local_namespace_describes_existing_file() -> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new("existing-metadata")?;
    std::fs::write(root.path().join("file.txt"), b"content")?;
    let path = StoragePath::new("file.txt")?;
    let descriptor = namespace(&local(root.path()).await?)?.stat(&path).await?;

    assert_eq!(descriptor.path, path);
    assert_eq!(descriptor.kind, data_mover::model::EntryKind::File);
    assert_eq!(descriptor.size, Some(7));
    Ok(())
}

#[tokio::test]
async fn local_timestamp_mutations_preserve_the_omitted_side()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new("selective-timestamps")?;
    let path = StoragePath::new("file.txt")?;
    let absolute = root.path().join(path.as_str());
    std::fs::write(&absolute, b"content")?;
    filetime::set_file_times(
        &absolute,
        filetime::FileTime::from_unix_time(1_610_000_000, 123_000_000),
        filetime::FileTime::from_unix_time(1_620_000_000, 456_000_000),
    )?;
    let storage = local(root.path()).await?;
    let metadata = storage.metadata()?;
    let initial = observe_timestamps(&storage, &path).await?;
    let initial_timestamps = match initial.timestamps() {
        MetadataObservation::Value { value, .. } => value,
        other => return Err(format!("timestamps not observed: {other:?}").into()),
    };

    let replacement_modified =
        StorageTimestamp::new(1_730_000_000_789_000_000, TimePrecision::Nanoseconds)?;
    metadata
        .apply(
            &path,
            MetadataMutation::Timestamps(TimestampMetadata {
                accessed: None,
                modified: Some(replacement_modified),
                created: None,
            }),
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;
    let modified = observe_timestamps(&storage, &path).await?;
    let modified = match modified.timestamps() {
        MetadataObservation::Value { value, .. } => value,
        other => return Err(format!("timestamps not observed after mtime: {other:?}").into()),
    };
    assert_eq!(modified.accessed, initial_timestamps.accessed);
    assert_eq!(modified.modified, Some(replacement_modified));

    let replacement_accessed =
        StorageTimestamp::new(1_740_000_000_321_000_000, TimePrecision::Nanoseconds)?;
    metadata
        .apply(
            &path,
            MetadataMutation::Timestamps(TimestampMetadata {
                accessed: Some(replacement_accessed),
                modified: None,
                created: None,
            }),
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;
    let accessed = observe_timestamps(&storage, &path).await?;
    let accessed = match accessed.timestamps() {
        MetadataObservation::Value { value, .. } => value,
        other => return Err(format!("timestamps not observed after atime: {other:?}").into()),
    };
    assert_eq!(accessed.accessed, Some(replacement_accessed));
    assert_eq!(accessed.modified, Some(replacement_modified));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn local_namespace_describes_and_reads_dangling_symlink()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;

    let root = TempDir::new("dangling-symlink-metadata")?;
    symlink("missing-target", root.path().join("link"))?;
    let path = StoragePath::new("link")?;
    let namespace = namespace(&local(root.path()).await?)?;

    let descriptor = namespace.stat(&path).await?;
    assert_eq!(descriptor.kind, data_mover::model::EntryKind::Symlink);
    assert_eq!(descriptor.size, None);
    assert_eq!(
        namespace.read_link(&path).await?.as_bytes(),
        b"missing-target"
    );
    Ok(())
}
