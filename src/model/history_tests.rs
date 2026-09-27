use super::{
    HistoryOrder, MAX_HISTORY_RECORDS, ObjectHistory, RecordKind, SourceVersion, VersionRecord,
};
use crate::model::{BackendIdentity, BackendKind, IdentityStrength, SourceIdentity};

fn identity(id: &str) -> SourceIdentity {
    let backend = BackendIdentity::new(BackendKind::S3, "s3://host/bucket")
        .unwrap_or_else(|error| panic!("{error}"));
    SourceIdentity::new(backend, IdentityStrength::VersionScoped, id)
        .unwrap_or_else(|error| panic!("{error}"))
}

fn object(id: &str, latest: bool) -> VersionRecord {
    let kind = RecordKind::Object {
        size: 1,
        etag: "\"e\"".into(),
        modified: None,
    };
    VersionRecord::from_listing(id, latest, kind, identity(id))
        .unwrap_or_else(|error| panic!("{error}"))
}

fn marker(id: &str, latest: bool) -> VersionRecord {
    let kind = RecordKind::DeleteMarker { modified: None };
    VersionRecord::from_listing(id, latest, kind, identity(id))
        .unwrap_or_else(|error| panic!("{error}"))
}

fn whole(records: Vec<VersionRecord>) -> ObjectHistory {
    ObjectHistory::whole(records, HistoryOrder::Server).unwrap_or_else(|error| panic!("{error}"))
}

#[test]
fn a_real_version_is_selected_by_its_id() {
    let version = object("v1", false);
    assert_eq!(version.version_id(), Some("v1"));
    assert_eq!(version.source(), Some(SourceVersion::Id("v1".into())));
    assert_eq!(
        object("v3", true).source(),
        Some(SourceVersion::Id("v3".into()))
    );
}

#[test]
fn the_null_version_names_no_id_and_is_current_only_while_latest() {
    let latest = object("null", true);
    assert_eq!(latest.version_id(), None);
    assert_eq!(latest.source(), Some(SourceVersion::Current));
    let older = object("null", false);
    assert_eq!(older.version_id(), None);
    assert_eq!(older.source(), Some(SourceVersion::Id("null".into())));
}

#[test]
fn a_delete_marker_cannot_be_copied() {
    let marker = marker("m1", true);
    assert!(marker.is_delete_marker() && marker.is_latest());
    assert_eq!(marker.source(), None);
    assert_eq!(marker.size(), None);
    assert_eq!(marker.etag(), None);
}

#[test]
fn malformed_ids_are_refused() {
    let kind = || RecordKind::DeleteMarker { modified: None };
    assert!(VersionRecord::from_listing("x".repeat(1025), true, kind(), identity("x")).is_err());
    assert!(VersionRecord::from_listing("a\0b", true, kind(), identity("x")).is_err());
    assert!(VersionRecord::new(Some("null".into()), true, kind(), identity("x"), None).is_err());
}

#[test]
fn a_history_holds_at_least_one_record_and_a_segment_at_most_the_limit() {
    assert!(ObjectHistory::whole(Vec::new(), HistoryOrder::Server).is_err());
    assert!(ObjectHistory::segment_of(Vec::new(), HistoryOrder::Server, 0, true, None).is_err());
    let full = vec![object("v", false); MAX_HISTORY_RECORDS];
    assert!(
        ObjectHistory::segment_of(full.clone(), HistoryOrder::Server, 0, true, Some(1)).is_ok()
    );
    let mut over = full;
    over.push(object("w", true));
    assert!(
        ObjectHistory::segment_of(over.clone(), HistoryOrder::Server, 0, true, Some(1)).is_err()
    );
    // A version larger than the key's largest is impossible.
    let one = vec![object("v", true)];
    assert!(ObjectHistory::segment_of(one.clone(), HistoryOrder::Server, 0, true, None).is_err());
    assert!(ObjectHistory::segment_of(one, HistoryOrder::Server, 0, true, Some(0)).is_err());
    // A whole history as listed may be longer: it is split before it is handed over.
    assert!(ObjectHistory::whole(over, HistoryOrder::Server).is_ok());
}

#[test]
fn the_current_object_is_the_latest_record_when_it_is_a_version() {
    let live = whole(vec![
        object("v1", false),
        marker("m1", false),
        object("v2", true),
    ]);
    assert_eq!(
        live.latest().and_then(VersionRecord::version_id),
        Some("v2")
    );
    assert_eq!(
        live.current().and_then(VersionRecord::version_id),
        Some("v2")
    );
    let deleted = whole(vec![object("v1", false), marker("m1", true)]);
    assert_eq!(
        deleted.latest().and_then(VersionRecord::version_id),
        Some("m1")
    );
    assert!(deleted.current().is_none());
    let null = whole(vec![object("null", true)]);
    assert_eq!(
        null.current().and_then(VersionRecord::source),
        Some(SourceVersion::Current)
    );
}

#[test]
fn a_split_history_numbers_its_segments_and_completes_only_the_last() {
    let records: Vec<VersionRecord> = (1..=7).map(|n| object(&format!("v{n}"), n == 7)).collect();
    let segments = whole(records.clone()).split(3);
    let shape: Vec<(u32, bool, usize)> = segments
        .iter()
        .map(|segment| {
            (
                segment.segment(),
                segment.is_complete(),
                segment.records().len(),
            )
        })
        .collect();
    assert_eq!(shape, [(0, false, 3), (1, false, 3), (2, true, 1)]);
    let joined: Vec<VersionRecord> = segments
        .iter()
        .flat_map(|segment| segment.records().iter().cloned())
        .collect();
    assert_eq!(joined, records, "every record once, in order");
    assert!(
        segments[..2]
            .iter()
            .all(|segment| segment.current().is_none())
    );
    assert_eq!(
        segments[2].current().and_then(VersionRecord::version_id),
        Some("v7")
    );
    assert!(segments.iter().all(|s| s.order() == HistoryOrder::Server));
    // Every segment knows the whole key's largest version.
    assert!(segments.iter().all(|s| s.largest_version() == Some(1)));
    let markers = whole(vec![marker("m1", false), marker("m2", true)]);
    assert_eq!(markers.largest_version(), None);
    // Short enough: one segment, unchanged.
    assert_eq!(whole(records.clone()).split(7), [whole(records)]);
}
