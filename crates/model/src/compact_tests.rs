use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem::size_of;

use super::*;

const NEGATIVE: Result<StorageTimestamp, ModelValueError> =
    StorageTimestamp::new(-1, TimePrecision::Nanoseconds);
const NEGATIVE_NANOS: i128 = match NEGATIVE {
    Ok(value) => value.unix_nanos(),
    Err(_) => 0,
};

#[test]
fn timestamps_preserve_full_range_precision_const_access_and_debug() {
    assert_eq!(NEGATIVE_NANOS, -1);
    for precision in [
        TimePrecision::Seconds,
        TimePrecision::Milliseconds,
        TimePrecision::Microseconds,
        TimePrecision::HundredNanoseconds,
        TimePrecision::Nanoseconds,
    ] {
        let quantum = precision.quantum_nanos();
        for nanos in [i128::MIN, i128::MAX, -1, 0, 1] {
            let aligned = nanos / quantum * quantum;
            let timestamp =
                StorageTimestamp::new(aligned, precision).unwrap_or_else(|error| panic!("{error}"));
            assert_eq!(timestamp.unix_nanos(), aligned);
            assert_eq!(timestamp.precision(), precision);
            assert_eq!(
                format!("{timestamp:?}"),
                format!("StorageTimestamp {{ unix_nanos: {aligned}, precision: {precision:?} }}")
            );
        }
        if quantum > 1 {
            assert!(StorageTimestamp::new(-1, precision).is_err());
            assert!(StorageTimestamp::new(1, precision).is_err());
        }
    }
}

#[test]
fn backend_clones_share_redacted_namespace_without_changing_value_identity() {
    let first = BackendIdentity::new(BackendKind::Nfs, "private-namespace")
        .unwrap_or_else(|error| panic!("{error}"));
    let cloned = first.clone();
    let separate = BackendIdentity::new(BackendKind::Nfs, "private-namespace")
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(first, separate);
    assert_eq!(first.stable_id().as_ptr(), cloned.stable_id().as_ptr());
    let digest = |backend: &BackendIdentity| {
        let mut hasher = DefaultHasher::new();
        backend.hash(&mut hasher);
        hasher.finish()
    };
    assert_eq!(digest(&first), digest(&separate));
    assert!(!format!("{cloned:?}").contains("private-namespace"));
    let source = SourceIdentity::new(first.clone(), IdentityStrength::StableWithinBackend, b"fh")
        .unwrap_or_else(|error| panic!("{error}"));
    let entry = ObservedEntry::new(StoragePath::root(), EntryKind::File, None, None, source)
        .unwrap_or_else(|error| panic!("{error}"));
    let decoded = ObservedEntry::decode_snapshot(entry.encode_snapshot().as_bytes(), &first)
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        decoded.source_identity().backend().stable_id().as_ptr(),
        first.stable_id().as_ptr()
    );
    assert_eq!(decoded, entry);
}

#[test]
#[cfg(target_pointer_width = "64")]
fn queued_observations_stay_within_the_compact_record_budget() {
    // 防止百万条有界队列再次因内联冷字段和时间对齐膨胀。
    assert!(size_of::<ObservedEntry>() <= 384);
    assert!(size_of::<MetadataObservations>() <= 80);
    assert!(size_of::<StorageTimestamp>() <= 17);
}
