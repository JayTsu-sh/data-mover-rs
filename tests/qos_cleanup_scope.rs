//! Cleanup must reject roots that merely look scoped before normalization.
use std::error::Error;
use std::fs::create_dir_all;
#[cfg(unix)]
use std::fs::{read, write};
#[cfg(unix)]
use std::os::unix::fs::symlink;

use tempfile::tempdir;

#[path = "lab/tools/qos_run_scope.rs"]
mod qos_run_scope;

#[test]
fn parent_and_encoded_parent_paths_cannot_escape_a_run() {
    for endpoint in [
        "/tmp/nightly-test/../shared",
        "/tmp/nightly-test/../../",
        "s3://user:pass@bucket.host/ci/nightly-test/%2e%2e/shared",
        "nfs://host/export:/ci/nightly-test/%2E%2E%2fshared",
        "smb:nightly-test/../shared",
        "s3@bucket:nightly-test/../shared",
        "s3://user:pass/nightly-test/x@bucket.host/shared",
        "s3://user:pass@bucket.host?prefix=/nightly-test/",
        "file://host/nightly-test/link",
        "NFS://host/nightly-test/link",
        "/tmp/x://host/nightly-test",
    ] {
        assert!(
            qos_run_scope::validated_endpoint(endpoint, "nightly-test").is_err(),
            "{endpoint}"
        );
    }
}

#[test]
fn canonical_local_root_and_supported_remote_paths_are_accepted() -> Result<(), Box<dyn Error>> {
    let root = tempdir()?;
    let local = root.path().join("nightly-test/destination");
    create_dir_all(&local)?;
    assert_eq!(
        qos_run_scope::validated_endpoint(local.to_str().ok_or("UTF-8 path")?, "nightly-test")?,
        local.canonicalize()?.to_str().ok_or("UTF-8 path")?
    );
    for endpoint in [
        "smb:nightly-test/destination",
        "s3@bucket:ci/nightly-test/destination",
        "nfs://host/export:/ci/nightly-test/destination?version=4.1",
        "s3://user:pass/word@bucket.host/ci/nightly-test/destination",
    ] {
        assert_eq!(
            qos_run_scope::validated_endpoint(endpoint, "nightly-test")?,
            endpoint
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn symlinks_at_or_below_a_run_root_cannot_retarget_cleanup() -> Result<(), Box<dyn Error>> {
    let root = tempdir()?;
    let shared = root.path().join("shared");
    create_dir_all(&shared)?;
    let sentinel = shared.join("sentinel");
    write(&sentinel, b"keep")?;
    let run = root.path().join("nightly-test");
    symlink(&shared, &run)?;
    assert!(
        qos_run_scope::validated_endpoint(run.to_str().ok_or("UTF-8 path")?, "nightly-test")
            .is_err()
    );
    let nested = root.path().join("release-test");
    create_dir_all(&nested)?;
    symlink(&shared, nested.join("destination"))?;
    assert!(
        qos_run_scope::validated_endpoint(
            nested.join("destination").to_str().ok_or("UTF-8 path")?,
            "release-test"
        )
        .is_err()
    );
    assert_eq!(read(sentinel)?, b"keep");
    Ok(())
}
