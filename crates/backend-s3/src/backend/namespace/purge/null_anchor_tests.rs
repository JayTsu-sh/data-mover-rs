//! null 记录在模型中没有版本 ID，不能用字面量 "null" 分支绕过修改时间保护。
#![allow(clippy::unwrap_used)]
use super::{record, same_anchor};
use crate::model::{
    BackendIdentity, BackendKind, StoragePath, StorageTimestamp, TimePrecision, VersionRecord,
};
use crate::storage::backends::s3::{S3ListedVersion, S3VersionFacts};

fn anchor(id: &str, marker: bool, nanos: i128) -> VersionRecord {
    record(
        &BackendIdentity::new(BackendKind::S3, "null-source-guard").unwrap(),
        &StoragePath::new("key").unwrap(),
        S3ListedVersion {
            key: "key".into(),
            facts: S3VersionFacts {
                version_id: id.into(),
                is_latest: true,
                delete_marker: marker,
                size: if marker { 0 } else { 7 },
                etag: if marker { "" } else { "same-content" }.into(),
            },
            last_modified: Some(StorageTimestamp::new(nanos, TimePrecision::Nanoseconds).unwrap()),
            position: None,
        },
    )
    .unwrap()
}

#[test]
fn normalized_null_objects_and_markers_must_retain_the_original_modification_evidence() {
    for marker in [false, true] {
        let before = anchor("null", marker, 1);
        let unchanged = anchor("null", marker, 1);
        let replaced = anchor("null", marker, 2);
        assert_eq!(before.version_id(), None);
        assert_eq!(before.identity(), replaced.identity());
        assert!(same_anchor(&unchanged, &before));
        assert!(
            !same_anchor(&replaced, &before),
            "mutable null marker={marker} changed but purge was authorized"
        );
        assert!(same_anchor(
            &anchor("immutable", marker, 2),
            &anchor("immutable", marker, 1)
        ));
    }
}
