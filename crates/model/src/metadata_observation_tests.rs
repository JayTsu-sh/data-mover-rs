use super::*;

#[test]
fn observation_parts_validate_and_preserve_retained_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let failure = MetadataObservationFailure {
        family: MetadataFamily::Timestamps,
        class: FailureClass::PermissionDenied,
        transience: Transience::Permanent,
    };
    let mut parts = MetadataObservationParts::default();
    parts.retain_failure_summary(failure);
    parts.retain_failure_summary(failure);
    let observed = parts.build()?;
    assert_eq!(observed.failures().collect::<Vec<_>>(), [failure]);
    assert_eq!(
        MetadataObservationParts::from(observed.clone()).build()?,
        observed
    );
    let duplicate = MetadataObservationParts {
        retained_failures: Some(vec![failure; 2].into_boxed_slice()),
        ..MetadataObservationParts::default()
    };
    assert!(duplicate.build().is_err());
    let excessive = MetadataObservationParts {
        retained_failures: Some(vec![failure; MetadataFamily::ALL.len() + 1].into_boxed_slice()),
        ..MetadataObservationParts::default()
    };
    assert!(excessive.build().is_err());
    let empty = MetadataObservationParts {
        retained_failures: Some(Vec::new().into_boxed_slice()),
        ..MetadataObservationParts::default()
    }
    .build()?;
    assert!(
        MetadataObservationParts::from(empty)
            .retained_failures
            .is_none()
    );
    Ok(())
}

fn observed_xattrs(values: Vec<ExtendedAttribute>) -> MetadataObservation<Vec<ExtendedAttribute>> {
    MetadataObservation::Value {
        value: values,
        provenance: MetadataProvenance::AdditionalCall,
    }
}

#[test]
fn model_rejects_excessive_xattr_count() {
    let attribute = ExtendedAttribute::new(vec![b'n'], Vec::new())
        .unwrap_or_else(|error| panic!("valid test attribute: {error}"));
    let attributes = vec![attribute; MAX_XATTR_COUNT + 1];

    assert!(
        MetadataObservations::new(
            MetadataObservation::NotRequested,
            observed_xattrs(attributes),
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
        )
        .is_err()
    );
}

#[test]
fn model_rejects_excessive_aggregate_payload() {
    let attribute = ExtendedAttribute::new(vec![b'n'], vec![0; MAX_MODEL_FIELD_BYTES])
        .unwrap_or_else(|error| panic!("valid bounded test attribute: {error}"));

    assert!(
        MetadataObservations::new(
            MetadataObservation::NotRequested,
            observed_xattrs(vec![attribute]),
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
        )
        .is_err()
    );
}

#[test]
fn maximum_aggregate_payload_round_trips() {
    let attribute = ExtendedAttribute::new(vec![b'n'], vec![0; MAX_MODEL_FIELD_BYTES - 1])
        .unwrap_or_else(|error| panic!("valid bounded test attribute: {error}"));
    let metadata = MetadataObservations::new(
        MetadataObservation::NotRequested,
        observed_xattrs(vec![attribute]),
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )
    .unwrap_or_else(|error| panic!("valid maximum metadata payload: {error}"));
    let mut bytes = Vec::new();
    encode(&metadata, &mut bytes);

    let mut cursor = Cursor::new(&bytes);
    assert_eq!(decode(&mut cursor), Ok(metadata));
}

#[test]
fn decoder_rejects_excessive_xattr_count_before_allocation() {
    let mut bytes = vec![0, 4, 0];
    bytes.extend_from_slice(&4_097_u32.to_le_bytes());
    let mut cursor = Cursor::new(&bytes);
    assert_eq!(decode(&mut cursor), Err(SnapshotDecodeError::FieldTooLarge));
}

#[test]
fn decoder_enforces_one_total_payload_budget() {
    let mut bytes = vec![4, 0, 0, 1];
    let size = u32::try_from(MAX_MODEL_FIELD_BYTES)
        .unwrap_or_else(|_| unreachable!("test model limit fits u32"));
    bytes.extend_from_slice(&size.to_le_bytes());
    bytes.resize(bytes.len() + MAX_MODEL_FIELD_BYTES, 0);
    bytes.extend_from_slice(&[1, 1, 0, 0, 0, 0]);
    let mut cursor = Cursor::new(&bytes);
    assert_eq!(decode(&mut cursor), Err(SnapshotDecodeError::FieldTooLarge));
}

#[test]
fn tags_round_trip_without_debug_value_disclosure() {
    let tag = ObjectTag::new("classification", "secret")
        .unwrap_or_else(|error| panic!("valid test tag: {error}"));
    assert!(!format!("{tag:?}").contains("secret"));
    let metadata = MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::Value {
            value: vec![tag],
            provenance: MetadataProvenance::AdditionalCall,
        },
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )
    .unwrap_or_else(|error| panic!("valid tag observation: {error}"));
    let mut bytes = Vec::new();
    encode(&metadata, &mut bytes);

    assert_eq!(decode(&mut Cursor::new(&bytes)), Ok(metadata));
}

/// Every failure class has its own snapshot tag and decodes back to itself; `Throttled` (P7 C3)
/// took the next free tag, so snapshots written before it still decode unchanged.
#[test]
fn every_failure_class_round_trips_through_its_snapshot_tag() {
    use crate::model::FailureClass;
    let classes = [
        FailureClass::Connectivity,
        FailureClass::Authentication,
        FailureClass::PermissionDenied,
        FailureClass::NotFound,
        FailureClass::InvalidInput,
        FailureClass::Unsupported,
        FailureClass::Conflict,
        FailureClass::Capacity,
        FailureClass::Corruption,
        FailureClass::Protocol,
        FailureClass::Cancelled,
        FailureClass::Internal,
        FailureClass::Throttled,
    ];
    for (tag, class) in classes.into_iter().enumerate() {
        let tag = u8::try_from(tag).unwrap_or(u8::MAX);
        assert_eq!(super::failure_tag(class), tag, "{class:?}");
        assert_eq!(super::failure_from_tag(tag), Some(class));
    }
    assert_eq!(super::failure_from_tag(13), None);
}

#[test]
fn failed_families_are_available_without_discarding_other_observations() {
    let observations = MetadataObservations {
        acl: MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            transience: Transience::Permanent,
        },
        tags: MetadataObservation::Failed {
            class: FailureClass::Throttled,
            transience: Transience::Transient,
        },
        ..MetadataObservations::default()
    };
    let failures: Vec<_> = observations.failures().collect();
    assert_eq!(failures.len(), 2);
    assert_eq!(failures[0].family, MetadataFamily::Acl);
    assert_eq!(failures[0].class, FailureClass::PermissionDenied);
    assert_eq!(failures[0].transience, Transience::Permanent);
    assert_eq!(failures[1].family, MetadataFamily::Tags);
    assert_eq!(failures[1].class, FailureClass::Throttled);
}

#[test]
fn a_successful_timestamp_fallback_retains_its_failed_attempt_after_snapshot_roundtrip()
-> Result<(), Box<dyn std::error::Error>> {
    let mut metadata = MetadataObservations {
        timestamps: MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            transience: Transience::Permanent,
        },
        ..MetadataObservations::default()
    };
    metadata.retain_failure(MetadataFamily::Timestamps);
    metadata.retain_failure(MetadataFamily::Timestamps);
    assert_eq!(metadata.failures().count(), 1);
    metadata.timestamps = MetadataObservation::Value {
        value: TimestampMetadata {
            accessed: None,
            modified: Some(StorageTimestamp::new(42, TimePrecision::Nanoseconds)?),
            created: None,
        },
        provenance: MetadataProvenance::Inline,
    };
    let mut bytes = Vec::new();
    encode(&metadata, &mut bytes);
    let decoded = decode(&mut Cursor::new(&bytes))?;
    assert_eq!(decoded, metadata);
    assert!(decoded.timestamps().value().is_some());
    let failures = decoded.failures().collect::<Vec<_>>();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].family, MetadataFamily::Timestamps);
    assert_eq!(
        (failures[0].class, failures[0].transience),
        (FailureClass::PermissionDenied, Transience::Permanent)
    );
    Ok(())
}

#[test]
fn retained_failure_codec_refuses_unbounded_duplicate_or_unknown_families() {
    let class = failure_tag(FailureClass::PermissionDenied);
    let transience = transience_tag(Transience::Permanent);
    for (tail, expected) in [
        (vec![6], SnapshotDecodeError::FieldTooLarge),
        (
            vec![1, 5, class, transience],
            SnapshotDecodeError::Malformed,
        ),
        (
            vec![2, 4, class, transience, 4, class, transience],
            SnapshotDecodeError::Malformed,
        ),
    ] {
        let mut bytes = vec![0; 5];
        bytes.extend(tail);
        assert_eq!(decode(&mut Cursor::new(&bytes)), Err(expected));
    }
}
