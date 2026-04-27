use data_mover::storage_enum::{StorageType, detect_storage_type};

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
    assert_eq!(detect_storage_type("nfs://server:2049/export"), StorageType::Nfs);
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
    assert_eq!(detect_storage_type("s3+https://bucket.host/data"), StorageType::S3);
}

#[test]
fn test_s3_http() {
    assert_eq!(detect_storage_type("s3+http://bucket.host/data"), StorageType::S3);
}

#[test]
fn test_s3_hcp() {
    assert_eq!(detect_storage_type("s3+hcp://bucket.host/data"), StorageType::S3);
}

#[test]
fn test_relative_path() {
    assert_eq!(detect_storage_type("./relative/path"), StorageType::Local);
}

#[test]
fn test_empty_string() {
    assert_eq!(detect_storage_type(""), StorageType::Local);
}
