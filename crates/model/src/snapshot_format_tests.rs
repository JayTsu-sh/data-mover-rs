//! The current entry snapshot format, byte for byte (P7 §10.4): four v10 golden fixtures under
//! `tests/data/snapshot/` — a plain NFS file, an S3 key's whole history (versions, delete markers,
//! the `"null"` version, an inferred order) and a later, incomplete segment whose records carry
//! metadata, and a directory carrying a current marker object with metadata. Any change to the
//! layout must bump the version byte and these fixtures together; bytes
//! of any other version are refused, never misread.

use std::fmt::Write as _;
use std::slice;

use super::codec::VERSION;
use super::*;
use crate::model::{
    HistoryOrder, MAX_HISTORY_RECORDS, MetadataObservation, MetadataProvenance, ObjectHistory,
    ObjectTag, RecordKind, TimePrecision, TimestampMetadata, VersionRecord,
};

const NFS_FILE: &str = include_str!("../../../tests/data/snapshot/v10-nfs-file.hex");
const S3_HISTORY: &str = include_str!("../../../tests/data/snapshot/v10-s3-history.hex");
const S3_SEGMENT: &str = include_str!("../../../tests/data/snapshot/v10-s3-history-segment.hex");
const S3_MARKER: &str = include_str!("../../../tests/data/snapshot/v10-s3-directory-marker.hex");

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

fn unhex(text: &str) -> Vec<u8> {
    let digits: Vec<u8> = text.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    digits
        .chunks(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap_or_else(|error| panic!("{error}"));
            u8::from_str_radix(pair, 16).unwrap_or_else(|error| panic!("{error}"))
        })
        .collect()
}

fn at(millis: i128) -> StorageTimestamp {
    StorageTimestamp::new(millis * 1_000_000, TimePrecision::Milliseconds)
        .unwrap_or_else(|error| panic!("{error}"))
}

fn nfs_file() -> ObservedEntry {
    let backend = BackendIdentity::new(BackendKind::Nfs, "nfs://10.0.0.1/export")
        .unwrap_or_else(|error| panic!("{error}"));
    let source = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"fh-1")
        .unwrap_or_else(|error| panic!("{error}"));
    ObservedEntry::new(
        StoragePath::new("dir/file").unwrap_or_else(|error| panic!("{error}")),
        EntryKind::File,
        Some(7),
        Some(at(1_700_000_000_000)),
        source,
    )
    .unwrap_or_else(|error| panic!("{error}"))
}

fn s3() -> BackendIdentity {
    BackendIdentity::new(BackendKind::S3, "s3://10.0.0.2:9000/bucket")
        .unwrap_or_else(|error| panic!("{error}"))
}

fn identity(strength: IdentityStrength, bytes: &[u8]) -> SourceIdentity {
    SourceIdentity::new(s3(), strength, bytes).unwrap_or_else(|error| panic!("{error}"))
}

fn record(id: Option<&str>, latest: bool, kind: RecordKind) -> VersionRecord {
    let identity = match id {
        Some(id) => identity(IdentityStrength::VersionScoped, id.as_bytes()),
        None => identity(IdentityStrength::PathScoped, b"\x03\0\0\0\0\0\0\0p/k\"n\""),
    };
    VersionRecord::new(id.map(str::to_string), latest, kind, identity, None)
        .unwrap_or_else(|error| panic!("{error}"))
}

fn object(size: u64, etag: &str, millis: i128) -> RecordKind {
    RecordKind::Object {
        size,
        etag: etag.to_string(),
        modified: Some(at(millis)),
    }
}

fn marker(millis: i128) -> RecordKind {
    RecordKind::DeleteMarker {
        modified: Some(at(millis)),
    }
}

/// The key `p/k`: the `"null"` version, a version, a delete marker, the latest version; ordered
/// by time (inferred).
fn s3_history() -> ObservedEntry {
    let records = vec![
        record(None, false, object(1, "\"n\"", 1_000)),
        record(Some("v1"), false, object(2, "\"a\"", 2_000)),
        record(Some("m1"), false, marker(3_000)),
        record(Some("v2"), true, object(3, "\"b\"", 4_000)),
    ];
    let history = ObjectHistory::whole(records, HistoryOrder::Inferred)
        .unwrap_or_else(|error| panic!("{error}"));
    let source = identity(IdentityStrength::PathScoped, b"\x03\0\0\0\0\0\0\0p/k\"b\"");
    ObservedEntry::new(
        StoragePath::new("p/k").unwrap_or_else(|error| panic!("{error}")),
        EntryKind::File,
        Some(3),
        Some(at(4_000)),
        source,
    )
    .unwrap_or_else(|error| panic!("{error}"))
    .with_history(Some(Box::new(history)))
    .unwrap_or_else(|error| panic!("{error}"))
}

/// Segment 1 of a longer history (whose largest version is 9 bytes), not complete, server-ordered;
/// its version carries what a plan asking for tags and times observed of it, the marker nothing.
fn s3_segment() -> ObservedEntry {
    let observed = |tag: &str| {
        MetadataObservations::new(
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::Value {
                value: vec![ObjectTag::new("t", tag).unwrap_or_else(|error| panic!("{error}"))],
                provenance: MetadataProvenance::AdditionalCall,
            },
            MetadataObservation::NotApplicable,
            MetadataObservation::Value {
                value: TimestampMetadata {
                    accessed: None,
                    modified: Some(at(5_000)),
                    created: None,
                },
                provenance: MetadataProvenance::AdditionalCall,
            },
        )
        .unwrap_or_else(|error| panic!("{error}"))
    };
    let mut records = vec![
        record(Some("v3"), false, object(4, "\"c\"", 5_000)),
        record(Some("m2"), false, marker(6_000)),
    ];
    records[0].set_metadata(observed("three"));
    let history = ObjectHistory::segment_of(records, HistoryOrder::Server, 1, false, Some(9))
        .unwrap_or_else(|error| panic!("{error}"));
    s3_history()
        .with_history(Some(Box::new(history)))
        .unwrap_or_else(|error| panic!("{error}"))
}

fn fixtures() -> [(&'static str, ObservedEntry, &'static str); 4] {
    [
        ("v10-nfs-file", nfs_file(), NFS_FILE),
        ("v10-s3-history", s3_history(), S3_HISTORY),
        ("v10-s3-history-segment", s3_segment(), S3_SEGMENT),
        (
            "v10-s3-directory-marker",
            super::directory_marker_tests::directory_marker_fixture(),
            S3_MARKER,
        ),
    ]
}

fn backend_of(entry: &ObservedEntry) -> BackendIdentity {
    entry.source_identity().backend().clone()
}

/// The current format, byte for byte: a change to the layout fails here until the version byte
/// is bumped and the fixtures are rewritten from the printed bytes.
#[test]
fn the_current_format_is_pinned_byte_for_byte() {
    let changed: Vec<String> = fixtures()
        .into_iter()
        .filter(|(_, entry, golden)| hex(entry.encode_snapshot().as_bytes()) != hex(&unhex(golden)))
        .map(|(name, entry, _)| format!("{name}.hex: {}", hex(entry.encode_snapshot().as_bytes())))
        .collect();
    assert!(
        changed.is_empty(),
        "the format changed — bump VERSION and rewrite the golden fixtures:\n{}",
        changed.join("\n")
    );
    for (name, entry, golden) in fixtures() {
        let encoded = entry.encode_snapshot();
        assert_eq!(encoded.as_bytes()[4], VERSION, "{name}");
        let decoded = ObservedEntry::decode_snapshot(&unhex(golden), &backend_of(&entry))
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(decoded, entry, "{name}");
    }
}

#[test]
fn another_version_byte_is_unsupported_never_misread() {
    assert_eq!(VERSION, 10);
    for (name, entry, golden) in fixtures() {
        for version in [4, 5, 6, 7, 8, 9, 11, 0, u8::MAX] {
            let mut bytes = unhex(golden);
            bytes[4] = version;
            assert_eq!(
                ObservedEntry::decode_snapshot(&bytes, &backend_of(&entry)),
                Err(SnapshotDecodeError::UnsupportedVersion),
                "{name}: version {version}"
            );
        }
    }
}

/// Where the history section of `entry`'s snapshot starts: right after what the same entry
/// without a history encodes before its rename evidence, marker tag and identity key.
fn history_at(entry: &ObservedEntry) -> usize {
    let bare = entry
        .clone()
        .with_history(None)
        .unwrap_or_else(|error| panic!("{error}"))
        .encode_snapshot();
    bare.as_bytes().len() - 32 - 1 - 1 - 1
}

#[test]
fn an_impossible_history_section_is_malformed() {
    let entry = s3_history();
    let backend = backend_of(&entry);
    let bytes = entry.encode_snapshot().as_bytes().to_vec();
    let section = history_at(&entry);
    assert_eq!(bytes[section], 1);
    let count_at = section + 1 + 1 + 4;
    let count = |value: u32| {
        let mut bytes = bytes.clone();
        bytes[count_at..count_at + 4].copy_from_slice(&value.to_le_bytes());
        bytes
    };
    assert_eq!(bytes[count_at..count_at + 4], 4_u32.to_le_bytes());
    let over = u32::try_from(MAX_HISTORY_RECORDS + 1).unwrap_or(u32::MAX);
    let patched = |patch: &dyn Fn(&mut Vec<u8>)| {
        let mut bytes = bytes.clone();
        patch(&mut bytes);
        bytes
    };
    for (what, damaged) in [
        ("no records", count(0)),
        ("more records than a segment holds", count(over)),
        (
            "an unknown history flag",
            patched(&|bytes| bytes[section + 1] |= 0x80),
        ),
        (
            "an unknown record flag",
            // After the count, the key's largest version (8 bytes), then the first record's flags.
            patched(&|bytes| bytes[count_at + 4 + 8] |= 0x80),
        ),
        (
            "an unknown section tag",
            patched(&|bytes| bytes[section] = 2),
        ),
    ] {
        assert_eq!(
            ObservedEntry::decode_snapshot(&damaged, &backend),
            Err(SnapshotDecodeError::Malformed),
            "{what}"
        );
    }
}

/// Segments of one key are the same entry — path, identity key, every entry-level field — each
/// with its part of the history; only the last is complete, and only it names the current object.
#[test]
fn segments_keep_the_entry_and_split_its_history() {
    let entry = s3_history();
    let segments = entry.clone().into_segments(3);
    assert_eq!(segments.len(), 2);
    for (index, segment) in segments.iter().enumerate() {
        assert_eq!(segment.identity_key(), entry.identity_key());
        assert_eq!(segment.path(), entry.path());
        assert_eq!(segment.size(), entry.size());
        let history = segment.history().unwrap_or_else(|| panic!("{index}"));
        assert_eq!(history.segment(), u32::try_from(index).unwrap_or(u32::MAX));
        assert_eq!(history.is_complete(), index == 1);
    }
    assert_eq!(segments[0].source_version(), None);
    assert_eq!(segments[1].source_version(), entry.source_version());
    assert_eq!(entry.source_version(), Some(SourceVersion::Id("v2".into())));
    assert_eq!(entry.clone().into_segments(4), slice::from_ref(&entry));
    assert_eq!(nfs_file().into_segments(1), [nfs_file()]);
    // Each segment is a snapshot of its own.
    for segment in segments {
        let decoded = ObservedEntry::decode_snapshot(
            segment.encode_snapshot().as_bytes(),
            &backend_of(&segment),
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(decoded, segment);
    }
}
