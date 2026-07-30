use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use data_mover::error::StorageError;
use data_mover::{EntryEnum, LocalStorage, StorageEnum};

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

#[tokio::test]
async fn local_get_metadata_classifies_missing_path() -> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new("missing-metadata")?;
    let storage = LocalStorage::new(root.path(), None);

    let result = storage.get_metadata(Path::new("missing-file")).await;

    assert!(matches!(
        result,
        Err(StorageError::FileNotFound(path)) if path == "missing-file"
    ));
    Ok(())
}

#[tokio::test]
async fn storage_enum_preserves_missing_path_classification()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new("missing-enum-metadata")?;
    let storage = StorageEnum::Local(LocalStorage::new(root.path(), None));

    let result = storage.get_metadata(Path::new("nested/missing-file")).await;

    assert!(matches!(
        result,
        Err(StorageError::FileNotFound(path)) if path == "nested/missing-file"
    ));
    Ok(())
}

#[tokio::test]
async fn local_get_metadata_returns_existing_file() -> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new("existing-metadata")?;
    std::fs::write(root.path().join("file.txt"), b"content")?;
    let storage = LocalStorage::new(root.path(), None);

    let entry = storage.get_metadata(Path::new("file.txt")).await?;

    assert!(matches!(
        entry,
        EntryEnum::NAS(entry)
            if entry.relative_path == Path::new("file.txt")
                && entry.name == "file.txt"
                && !entry.is_symlink
    ));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn local_get_metadata_returns_dangling_symlink() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;

    let root = TempDir::new("dangling-symlink-metadata")?;
    symlink("missing-target", root.path().join("link"))?;
    let storage = LocalStorage::new(root.path(), None);

    let entry = storage.get_metadata(Path::new("link")).await?;

    assert!(matches!(
        entry,
        EntryEnum::NAS(entry)
            if entry.relative_path == Path::new("link")
                && entry.name == "link"
                && entry.is_symlink
    ));
    Ok(())
}
