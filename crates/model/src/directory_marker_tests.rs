//! A logical directory retains an independently copyable, nonrecursive marker observation.

use super::*;
use crate::model::{
    HistoryOrder, MetadataObservation, MetadataProvenance, ObjectTag, RecordKind, TimePrecision,
};

fn backend(endpoint: &str) -> BackendIdentity {
    BackendIdentity::new(BackendKind::S3, endpoint).unwrap_or_else(|error| panic!("{error}"))
}

fn entry(path: &str, kind: EntryKind, size: Option<u64>) -> ObservedEntry {
    let identity = SourceIdentity::new(
        backend("s3://bucket"),
        IdentityStrength::PathScoped,
        path.as_bytes(),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    ObservedEntry::new(
        StoragePath::new(path).unwrap_or_else(|error| panic!("{error}")),
        kind,
        size,
        None,
        identity,
    )
    .unwrap_or_else(|error| panic!("{error}"))
}

fn directory() -> ObservedEntry {
    entry("data", EntryKind::Directory, None)
}

fn marker() -> ObservedEntry {
    let modified = StorageTimestamp::new(123, TimePrecision::Nanoseconds)
        .unwrap_or_else(|error| panic!("{error}"));
    let metadata = MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::Value {
            value: vec![
                ObjectTag::new("marker", "retained").unwrap_or_else(|error| panic!("{error}")),
            ],
            provenance: MetadataProvenance::AdditionalCall,
        },
        MetadataObservation::NotApplicable,
        MetadataObservation::NotRequested,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let mut marker = entry("data/", EntryKind::File, Some(0))
        .with_metadata(metadata)
        .with_backend_fact_bytes(vec![1, 2, 3])
        .unwrap_or_else(|error| panic!("{error}"));
    marker.modified = Some(modified);
    marker
}

fn history() -> ObjectHistory {
    let record = VersionRecord::from_listing(
        "v1",
        true,
        RecordKind::Object {
            size: 0,
            etag: "empty".into(),
            modified: None,
        },
        marker().source_identity,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    ObjectHistory::whole(vec![record], HistoryOrder::Server)
        .unwrap_or_else(|error| panic!("{error}"))
}

pub(super) fn directory_marker_fixture() -> ObservedEntry {
    directory()
        .with_directory_marker_object(Some(Box::new(marker())))
        .unwrap_or_else(|error| panic!("{error}"))
}

/// Assemble wire bytes independently of the validating model builder. The leaf uses ordinary
/// snapshot framing, but must have an absent marker tag when decoded inside a directory.
fn wire_marker(outer: &ObservedEntry, inner: &ObservedEntry) -> Vec<u8> {
    let mut bytes = outer.encode_snapshot().as_bytes().to_vec();
    let tag = bytes.len() - 33;
    assert_eq!(bytes[tag], 0);
    bytes[tag] = 1;
    let at = tag + 1;
    bytes.splice(at..at, inner.encode_snapshot().as_bytes().iter().copied());
    bytes
}

#[test]
fn marker_snapshot_preserves_the_raw_object_and_structural_identity() {
    let outer = directory();
    let inner = marker();
    assert!(outer.directory_marker_object().is_none());
    let combined = outer
        .clone()
        .with_directory_marker_object(Some(Box::new(inner.clone())))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(combined.identity_key(), outer.identity_key());
    assert_eq!(combined.source_identity(), outer.source_identity());
    assert_eq!(combined.path(), outer.path());
    assert_eq!(combined.size(), None);
    assert_eq!(combined.directory_marker_object(), Some(&inner));
    assert_eq!(
        combined.encode_snapshot().as_bytes(),
        wire_marker(&outer, &inner)
    );
    let decoded = ObservedEntry::decode_snapshot(
        combined.encode_snapshot().as_bytes(),
        &backend("s3://bucket"),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(decoded, combined);
    let raw = decoded
        .directory_marker_object()
        .unwrap_or_else(|| panic!("missing marker"));
    assert_eq!(raw.path().as_str(), "data/");
    assert_eq!(raw.source_version(), Some(SourceVersion::Current));
    assert_ne!(raw.identity_key(), outer.identity_key());
    assert_eq!(raw.metadata(), inner.metadata());
    assert_eq!(raw.modified(), inner.modified());
    assert_eq!(
        combined
            .with_directory_marker_object(None)
            .unwrap_or_else(|error| panic!("{error}")),
        outer
    );
}

fn invalid_markers() -> Vec<ObservedEntry> {
    let mut cases = vec![
        entry("data/", EntryKind::Directory, Some(0)),
        entry("data/", EntryKind::File, None),
        entry("data/", EntryKind::File, Some(1)),
        marker().with_history(Some(Box::new(history()))),
    ];
    for path in ["data", "data//", "other/", "data/x/", "data2/"] {
        cases.push(entry(path, EntryKind::File, Some(0)));
    }
    cases
}

#[test]
fn marker_constraints_are_enforced_by_builder_and_decoder() {
    for inner in invalid_markers() {
        assert!(
            directory()
                .with_directory_marker_object(Some(Box::new(inner.clone())))
                .is_err()
        );
        assert_eq!(
            ObservedEntry::decode_snapshot(
                &wire_marker(&directory(), &inner),
                &backend("s3://bucket")
            ),
            Err(SnapshotDecodeError::Malformed),
        );
    }
    for outer in [
        entry("data", EntryKind::File, None),
        directory().with_history(Some(Box::new(history()))),
    ] {
        assert!(
            outer
                .clone()
                .with_directory_marker_object(Some(Box::new(marker())))
                .is_err()
        );
        assert_eq!(
            ObservedEntry::decode_snapshot(
                &wire_marker(&outer, &marker()),
                &backend("s3://bucket")
            ),
            Err(SnapshotDecodeError::Malformed),
        );
    }
}

#[test]
fn attaching_history_after_a_marker_still_cannot_decode_as_a_valid_snapshot() {
    // with_history is an internal, infallible builder used by history segmentation. Even if
    // callers compose the builders in the wrong order, the decoder enforces exclusivity.
    let invalid = directory()
        .with_directory_marker_object(Some(Box::new(marker())))
        .unwrap_or_else(|error| panic!("{error}"))
        .with_history(Some(Box::new(history())));
    assert_eq!(
        ObservedEntry::decode_snapshot(
            invalid.encode_snapshot().as_bytes(),
            &backend("s3://bucket")
        ),
        Err(SnapshotDecodeError::Malformed),
    );
}

#[test]
fn a_marker_from_another_endpoint_or_backend_is_rejected() {
    for elsewhere in [
        backend("s3://other-bucket"),
        BackendIdentity::new(BackendKind::Nfs, "s3://bucket")
            .unwrap_or_else(|error| panic!("{error}")),
    ] {
        let identity = SourceIdentity::new(elsewhere, IdentityStrength::PathScoped, b"data/")
            .unwrap_or_else(|error| panic!("{error}"));
        let inner = ObservedEntry::new(
            StoragePath::new("data/").unwrap_or_else(|error| panic!("{error}")),
            EntryKind::File,
            Some(0),
            None,
            identity,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert!(
            directory()
                .with_directory_marker_object(Some(Box::new(inner.clone())))
                .is_err()
        );
        assert_eq!(
            ObservedEntry::decode_snapshot(
                &wire_marker(&directory(), &inner),
                &backend("s3://bucket")
            ),
            Err(SnapshotDecodeError::BackendMismatch),
        );
    }
}

#[test]
fn nested_markers_are_rejected_without_decoding_another_entry() {
    let mut inner = marker();
    inner.marker = Some(Box::new(entry("data//", EntryKind::File, Some(0))));
    assert!(
        directory()
            .with_directory_marker_object(Some(Box::new(inner)))
            .is_err()
    );
    let mut bytes = wire_marker(&directory(), &marker());
    // The leaf's absent marker tag precedes its identity and the outer identity.
    let leaf_tag = bytes.len() - 65;
    assert_eq!(bytes[leaf_tag], 0);
    for tag in [1, 2, u8::MAX] {
        bytes[leaf_tag] = tag;
        // No nested header or identities: rejection must precede reading any further bytes.
        assert_eq!(
            ObservedEntry::decode_snapshot(&bytes[..=leaf_tag], &backend("s3://bucket")),
            Err(SnapshotDecodeError::Malformed),
        );
    }
}

#[test]
fn marker_framing_checks_tags_truncation_both_identities_and_trailing_bytes() {
    let outer = directory();
    let bytes = wire_marker(&outer, &marker());
    let outer_tag = outer.encode_snapshot().as_bytes().len() - 33;
    let mut unknown = bytes.clone();
    unknown[outer_tag] = 2;
    assert_eq!(
        ObservedEntry::decode_snapshot(&unknown, &backend("s3://bucket")),
        Err(SnapshotDecodeError::Malformed)
    );
    for len in 0..bytes.len() {
        assert!(ObservedEntry::decode_snapshot(&bytes[..len], &backend("s3://bucket")).is_err());
    }
    for distance in [1, 33] {
        let mut damaged = bytes.clone();
        damaged[bytes.len() - distance] ^= 1;
        assert_eq!(
            ObservedEntry::decode_snapshot(&damaged, &backend("s3://bucket")),
            Err(SnapshotDecodeError::IdentityMismatch)
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        ObservedEntry::decode_snapshot(&trailing, &backend("s3://bucket")),
        Err(SnapshotDecodeError::TrailingData)
    );
}
