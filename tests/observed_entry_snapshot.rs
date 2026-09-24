use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, MetadataObservation,
    MetadataProvenance, ObservationMode, ObservationPlan, ObservedEntry, SnapshotDecodeError,
    SourceIdentity, StoragePath, StorageTimestamp, TimePrecision,
};

fn backend(kind: BackendKind) -> Result<BackendIdentity, Box<dyn std::error::Error>> {
    Ok(BackendIdentity::new(kind, format!("test-{kind}"))?)
}

fn source(kind: BackendKind, value: &[u8]) -> Result<SourceIdentity, Box<dyn std::error::Error>> {
    Ok(SourceIdentity::new(
        backend(kind)?,
        IdentityStrength::StableWithinBackend,
        value,
    )?)
}

#[test]
fn optional_metadata_plan_is_omitted_by_default_and_selective() {
    let default = ObservationPlan::default();
    assert_eq!(default.acl(), ObservationMode::Omit);
    assert_eq!(default.xattrs(), ObservationMode::Omit);
    assert_eq!(default.ownership_mode(), ObservationMode::Omit);
    assert_eq!(default.timestamps(), ObservationMode::Omit);

    let selective = default
        .with_acl(ObservationMode::Required)
        .with_timestamps(ObservationMode::InlineOnly);
    assert_eq!(selective.acl(), ObservationMode::Required);
    assert_eq!(selective.xattrs(), ObservationMode::Omit);
    assert_eq!(selective.timestamps(), ObservationMode::InlineOnly);
    let value = MetadataObservation::Value {
        value: 42_u32,
        provenance: MetadataProvenance::Inline,
    };
    assert_eq!(value.value(), Some(&42));
}

#[test]
fn identity_key_is_fixed_stable_and_not_a_content_hash() -> Result<(), Box<dyn std::error::Error>> {
    let first = source(BackendKind::Nfs, b"file-id:42")?.identity_key();
    let same = source(BackendKind::Nfs, b"file-id:42")?.identity_key();
    let different = source(BackendKind::Nfs, b"file-id:43")?.identity_key();
    let other_backend = source(BackendKind::Cifs, b"file-id:42")?.identity_key();
    assert_eq!(first.as_bytes().len(), 32);
    assert_eq!(first, same);
    assert_ne!(first, different);
    assert_ne!(first, other_backend);
    Ok(())
}

#[test]
fn snapshot_roundtrip_reconstructs_without_backend_access() -> Result<(), Box<dyn std::error::Error>>
{
    let observed = ObservedEntry::new(
        StoragePath::new("dir/file")?,
        EntryKind::File,
        Some(123),
        Some(StorageTimestamp::new(
            1_725_000_000_000_000_000,
            TimePrecision::Seconds,
        )?),
        source(BackendKind::Nfs, b"nfs-file-handle")?,
    )?;
    let encoded = observed.encode_snapshot();
    let rebuilt = ObservedEntry::decode_snapshot(encoded.as_bytes(), &backend(BackendKind::Nfs)?)?;
    assert_eq!(rebuilt.identity_key(), observed.identity_key());
    assert_eq!(rebuilt.backend_kind(), BackendKind::Nfs);
    assert_eq!(rebuilt.path().as_str(), "dir/file");
    assert_eq!(rebuilt.kind(), EntryKind::File);
    assert_eq!(rebuilt.size(), Some(123));
    assert_eq!(rebuilt.modified(), observed.modified());
    Ok(())
}

/// Decoding needs the endpoint the scan ran on; another endpoint or backend kind is reported as
/// such, distinct from a corrupted entry.
#[test]
fn snapshot_decodes_only_against_the_endpoint_it_was_taken_on()
-> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        BackendIdentity::new(BackendKind::Nfs, "nfs://10.128.61.200/ontap_lisaauto_nfs")?;
    let observed = ObservedEntry::new(
        StoragePath::new("dir/file")?,
        EntryKind::File,
        Some(9),
        None,
        SourceIdentity::new(endpoint.clone(), IdentityStrength::PathScoped, b"dir/file")?,
    )?;
    let bytes = observed.encode_snapshot().as_bytes().to_vec();
    assert_eq!(ObservedEntry::decode_snapshot(&bytes, &endpoint)?, observed);
    for other in [
        BackendIdentity::new(BackendKind::Nfs, "nfs://10.128.61.201/ontap_lisaauto_nfs")?,
        BackendIdentity::new(BackendKind::Cifs, "nfs://10.128.61.200/ontap_lisaauto_nfs")?,
    ] {
        assert_eq!(
            ObservedEntry::decode_snapshot(&bytes, &other),
            Err(SnapshotDecodeError::BackendMismatch)
        );
    }
    Ok(())
}

#[test]
fn decoder_rejects_unknown_truncated_tampered_and_trailing_data()
-> Result<(), Box<dyn std::error::Error>> {
    let observed = ObservedEntry::new(
        StoragePath::new("file")?,
        EntryKind::File,
        Some(1),
        None,
        source(BackendKind::Local, b"inode:1")?,
    )?;
    let valid = observed.encode_snapshot().as_bytes().to_vec();
    let local = backend(BackendKind::Local)?;

    let mut unknown_version = valid.clone();
    unknown_version[4] = 99;
    assert_eq!(
        ObservedEntry::decode_snapshot(&unknown_version, &local),
        Err(SnapshotDecodeError::UnsupportedVersion)
    );
    assert_eq!(
        ObservedEntry::decode_snapshot(&valid[..valid.len() - 1], &local),
        Err(SnapshotDecodeError::Truncated)
    );

    let mut tampered = valid.clone();
    let last = tampered.len() - 1;
    tampered[last] ^= 1;
    assert_eq!(
        ObservedEntry::decode_snapshot(&tampered, &local),
        Err(SnapshotDecodeError::IdentityMismatch)
    );

    let mut trailing = valid;
    trailing.push(0);
    assert_eq!(
        ObservedEntry::decode_snapshot(&trailing, &local),
        Err(SnapshotDecodeError::TrailingData)
    );
    Ok(())
}

#[test]
fn snapshot_fields_reject_values_above_the_codec_limit() -> Result<(), Box<dyn std::error::Error>> {
    let oversized = vec![b'x'; 16 * 1024 * 1024 + 1];
    let oversized_string = String::from_utf8(oversized.clone())?;
    assert!(StoragePath::new(oversized_string.clone()).is_err());
    assert!(BackendIdentity::new(BackendKind::Local, oversized_string).is_err());
    let backend = BackendIdentity::new(BackendKind::Local, "bounded")?;
    assert!(
        SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, &oversized).is_err()
    );
    Ok(())
}
