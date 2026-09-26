use data_mover::HdfsConfig;
use data_mover::error::StorageError;
use data_mover::storage_enum::{
    BackendConfig, CreateStorageOptions, StorageType, create_storage, detect_storage_type,
};

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
fn test_hdfs_url() {
    assert_eq!(
        detect_storage_type("hdfs://user@namenode:9000/root"),
        StorageType::Hdfs
    );
}

#[tokio::test]
async fn hdfs_factory_requires_matching_typed_configuration() {
    let missing = create_storage(
        "hdfs://user@127.0.0.1:9000/root",
        CreateStorageOptions::default(),
    )
    .await;
    assert!(missing.is_err());

    let mismatched = create_storage(
        "/tmp",
        CreateStorageOptions {
            backend: BackendConfig::Hdfs(HdfsConfig::default()),
            ..Default::default()
        },
    )
    .await;
    assert!(mismatched.is_err());
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

/// `s3+hcp://` (HCP support was removed) is still S3, so it reaches the parser's error.
#[test]
fn test_s3_hcp() {
    assert_eq!(
        detect_storage_type("s3+hcp://bucket.host/data"),
        StorageType::S3
    );
}

#[test]
fn test_s3_storagegrid() {
    assert_eq!(
        detect_storage_type("s3+sg://bucket.host/data"),
        StorageType::S3
    );
}

#[test]
fn test_s3_storagegrid_https() {
    assert_eq!(
        detect_storage_type("s3+sg+https://bucket.host/data"),
        StorageType::S3
    );
}

#[test]
fn test_s3_dxn() {
    assert_eq!(
        detect_storage_type("s3+dxn://bucket.host/data"),
        StorageType::S3
    );
}

#[test]
fn test_s3_dxn_https() {
    assert_eq!(
        detect_storage_type("s3+dxn+https://bucket.host/data"),
        StorageType::S3
    );
}

/// The URL options live in the query; any `s3+…://` scheme (e.g. a removed one) is S3, so it
/// reaches the S3 parser's error naming the new spelling.
#[test]
fn test_s3_options_and_unknown_s3_schemes() {
    for url in [
        "s3://bucket.host/data?compat=dxn",
        "s3+https://bucket.host/data?compat=sg&tls_verify=false",
        "s3://bucket.host/data?compat=ontap&bucket_type=nas",
        "S3://bucket.host/data",
        "S3+HTTPS://bucket.host/data",
        "s3+whatever://bucket.host/data",
    ] {
        assert_eq!(detect_storage_type(url), StorageType::S3, "{url}");
    }
    for path in ["s3+backup/dir", "./s3+x/y", "s3", "dir/s3://x"] {
        assert_eq!(detect_storage_type(path), StorageType::Local, "{path}");
    }
}

#[test]
fn test_relative_path() {
    assert_eq!(detect_storage_type("./relative/path"), StorageType::Local);
}

#[test]
fn test_empty_string() {
    assert_eq!(detect_storage_type(""), StorageType::Local);
}

/// A removed scheme fails before any network I/O, naming its replacement and never the key pair.
#[tokio::test]
async fn test_removed_s3_scheme_names_the_new_spelling() {
    let error = create_storage(
        "s3+dxn+https://AKIDREMOVED:SeCrEt/Removed@bucket.127.0.0.1:1/p",
        CreateStorageOptions::default(),
    )
    .await
    .err();
    let Some(StorageError::UrlParseError(message)) = error else {
        panic!("expected UrlParseError, got {error:?}");
    };
    assert!(
        message.contains("s3+https://…?compat=dxn&tls_verify=false"),
        "{message}"
    );
    assert!(!message.contains("AKIDREMOVED") && !message.contains("SeCrEt"));
}

#[tokio::test]
async fn test_create_storage_ensure_dir_creates_missing_local_dir() {
    let dir = std::env::temp_dir().join("data-mover-ensure-dir-true");
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_string_lossy().into_owned();

    let storage = create_storage(&path, CreateStorageOptions::new(None, true)).await;
    assert!(storage.is_ok(), "ensure_dir=true 应自动创建缺失目录");
    assert!(dir.is_dir());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_create_storage_no_ensure_dir_errors_on_missing_local_dir() {
    let dir = std::env::temp_dir().join("data-mover-ensure-dir-false");
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_string_lossy().into_owned();

    let storage = create_storage(&path, CreateStorageOptions::new(None, false)).await;
    assert!(storage.is_err(), "ensure_dir=false 时缺失目录应报错");
}

#[test]
fn create_storage_options_default_to_backend_defaults_without_side_effects() {
    assert_eq!(
        CreateStorageOptions::default(),
        CreateStorageOptions {
            block_size: None,
            ensure_dir: false,
            backend: BackendConfig::Default,
        }
    );
}
