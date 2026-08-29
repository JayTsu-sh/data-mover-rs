use super::*;

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
