use data_mover::storage_enum::{StorageType, create_storage, detect_storage_type};

#[test]
fn test_local_unix_path() {
    assert_eq!(detect_storage_type("/data/dir"), StorageType::Local);
}

#[test]
fn test_local_windows_path() {
    assert_eq!(detect_storage_type("C:\\data\\dir"), StorageType::Local);
}

#[test]
fn test_nfs_url() {
    assert_eq!(
        detect_storage_type("nfs://server:2049/export"),
        StorageType::Nfs
    );
}

#[test]
fn test_s3_basic() {
    assert_eq!(
        detect_storage_type("s3://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI@bucket.host:9000/prefix"),
        StorageType::S3
    );
}

#[test]
fn test_s3_https() {
    assert_eq!(
        detect_storage_type("s3+https://bucket.host/data"),
        StorageType::S3
    );
}

#[test]
fn test_s3_http() {
    assert_eq!(
        detect_storage_type("s3+http://bucket.host/data"),
        StorageType::S3
    );
}

#[test]
fn test_s3_hcp() {
    assert_eq!(
        detect_storage_type("s3+hcp://bucket.host/data"),
        StorageType::S3
    );
}

#[test]
fn test_relative_path() {
    assert_eq!(detect_storage_type("./relative/path"), StorageType::Local);
}

#[test]
fn test_empty_string() {
    assert_eq!(detect_storage_type(""), StorageType::Local);
}

#[tokio::test]
async fn test_create_storage_ensure_dir_creates_missing_local_dir() {
    let dir = std::env::temp_dir().join("data-mover-ensure-dir-true");
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_string_lossy().into_owned();

    let storage = create_storage(&path, None, true).await;
    assert!(storage.is_ok(), "ensure_dir=true 应自动创建缺失目录");
    assert!(dir.is_dir());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_create_storage_no_ensure_dir_errors_on_missing_local_dir() {
    let dir = std::env::temp_dir().join("data-mover-ensure-dir-false");
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_string_lossy().into_owned();

    let storage = create_storage(&path, None, false).await;
    assert!(storage.is_err(), "ensure_dir=false 时缺失目录应报错");
}
