use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use data_mover::error::StorageError;
use data_mover::{LocalStorage, ResumeContext, StorageEnum};

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

fn assert_read_error(result: data_mover::Result<()>) {
    assert!(
        matches!(
            result,
            Err(StorageError::ReadError(ref message))
                if message.starts_with("Source read failed: ")
                    && message.len() > "Source read failed: ".len()
        ),
        "expected ReadError, got {result:?}"
    );
}

fn assert_write_error(result: data_mover::Result<()>) {
    assert!(
        matches!(
            result,
            Err(StorageError::WriteError(ref message))
                if message.starts_with("Destination write failed: ")
                    && message.len() > "Destination write failed: ".len()
        ),
        "expected WriteError, got {result:?}"
    );
}

#[test]
fn attributed_errors_preserve_their_variant_when_cloned() {
    assert!(matches!(
        StorageError::ReadError("source".to_string()).clone(),
        StorageError::ReadError(message) if message == "source"
    ));
    assert!(matches!(
        StorageError::WriteError("destination".to_string()).clone(),
        StorageError::WriteError(message) if message == "destination"
    ));
}

#[tokio::test]
async fn single_chunk_copy_attributes_source_read_error() -> Result<(), Box<dyn std::error::Error>>
{
    let source_root = TempDir::new("single-read-source")?;
    let destination_root = TempDir::new("single-read-destination")?;
    std::fs::write(source_root.path().join("file"), b"data")?;
    let source = StorageEnum::Local(LocalStorage::new(source_root.path(), Some(8)));
    let destination = StorageEnum::Local(LocalStorage::new(destination_root.path(), Some(8)));
    let entry = source.get_metadata(Path::new("file")).await?;
    std::fs::remove_file(source_root.path().join("file"))?;

    let result =
        StorageEnum::copy_file(&source, &destination, &entry, None, false, true, None).await;

    assert_read_error(result);
    Ok(())
}

#[tokio::test]
async fn single_chunk_copy_attributes_destination_write_error()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TempDir::new("single-write-source")?;
    let invalid_destination_root = TempDir::new("single-write-destination")?;
    std::fs::write(source_root.path().join("file"), b"data")?;
    let destination_file = invalid_destination_root.path().join("not-a-directory");
    std::fs::write(&destination_file, b"blocking file")?;
    let source = StorageEnum::Local(LocalStorage::new(source_root.path(), Some(8)));
    let destination = StorageEnum::Local(LocalStorage::new(destination_file, Some(8)));
    let entry = source.get_metadata(Path::new("file")).await?;

    let result =
        StorageEnum::copy_file(&source, &destination, &entry, None, false, true, None).await;

    assert_write_error(result);
    Ok(())
}

#[tokio::test]
async fn multi_chunk_copy_attributes_source_read_error() -> Result<(), Box<dyn std::error::Error>> {
    let source_root = TempDir::new("multi-read-source")?;
    let destination_root = TempDir::new("multi-read-destination")?;
    std::fs::write(source_root.path().join("file"), b"eight123")?;
    let source = StorageEnum::Local(LocalStorage::new(source_root.path(), Some(4)));
    let destination = StorageEnum::Local(LocalStorage::new(destination_root.path(), Some(4)));
    let entry = source.get_metadata(Path::new("file")).await?;
    std::fs::remove_file(source_root.path().join("file"))?;

    let result =
        StorageEnum::copy_file(&source, &destination, &entry, None, false, true, None).await;

    assert_read_error(result);
    Ok(())
}

#[tokio::test]
async fn multi_chunk_copy_prefers_destination_error_when_both_sides_fail()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TempDir::new("multi-write-source")?;
    let invalid_destination_root = TempDir::new("multi-write-destination")?;
    std::fs::write(source_root.path().join("file"), b"eight123")?;
    let destination_file = invalid_destination_root.path().join("not-a-directory");
    std::fs::write(&destination_file, b"blocking file")?;
    let source = StorageEnum::Local(LocalStorage::new(source_root.path(), Some(4)));
    let destination = StorageEnum::Local(LocalStorage::new(destination_file, Some(4)));
    let entry = source.get_metadata(Path::new("file")).await?;

    let result =
        StorageEnum::copy_file(&source, &destination, &entry, None, false, true, None).await;

    assert_write_error(result);
    Ok(())
}

#[tokio::test]
async fn resumable_copy_attributes_source_read_error() -> Result<(), Box<dyn std::error::Error>> {
    let source_root = TempDir::new("resume-read-source")?;
    let destination_root = TempDir::new("resume-read-destination")?;
    std::fs::write(source_root.path().join("file"), b"eight123")?;
    let source = StorageEnum::Local(LocalStorage::new(source_root.path(), Some(4)));
    let destination = StorageEnum::Local(LocalStorage::new(destination_root.path(), Some(4)));
    let entry = source.get_metadata(Path::new("file")).await?;
    std::fs::remove_file(source_root.path().join("file"))?;
    let resume = ResumeContext {
        part_relative_path: PathBuf::from("file.terrasync-part"),
        missing_intervals: vec![(0, 8)],
        on_committed: Arc::new(|_, _| {}),
    };

    let result = StorageEnum::copy_file_resumable(
        &source,
        &destination,
        &entry,
        None,
        false,
        true,
        None,
        resume,
    )
    .await;

    assert_read_error(result);
    Ok(())
}

#[tokio::test]
async fn resumable_copy_prefers_destination_error_when_both_sides_fail()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TempDir::new("resume-write-source")?;
    let invalid_destination_root = TempDir::new("resume-write-destination")?;
    std::fs::write(source_root.path().join("file"), b"eight123")?;
    let destination_file = invalid_destination_root.path().join("not-a-directory");
    std::fs::write(&destination_file, b"blocking file")?;
    let source = StorageEnum::Local(LocalStorage::new(source_root.path(), Some(4)));
    let destination = StorageEnum::Local(LocalStorage::new(destination_file, Some(4)));
    let entry = source.get_metadata(Path::new("file")).await?;
    let resume = ResumeContext {
        part_relative_path: PathBuf::from("file.terrasync-part"),
        missing_intervals: vec![(0, 8)],
        on_committed: Arc::new(|_, _| {}),
    };

    let result = StorageEnum::copy_file_resumable(
        &source,
        &destination,
        &entry,
        None,
        false,
        true,
        None,
        resume,
    )
    .await;

    assert_write_error(result);
    Ok(())
}
