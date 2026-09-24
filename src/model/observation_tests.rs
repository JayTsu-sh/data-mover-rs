use super::*;

#[test]
fn private_backend_facts_roundtrip_without_public_inspection() {
    let backend =
        BackendIdentity::new(BackendKind::Nfs, "cluster").unwrap_or_else(|error| panic!("{error}"));
    let source = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"fh")
        .unwrap_or_else(|error| panic!("{error}"));
    let entry = ObservedEntry::new(StoragePath::root(), EntryKind::File, None, None, source)
        .unwrap_or_else(|error| panic!("{error}"));
    let entry = entry
        .with_backend_fact_bytes(vec![0, 1, 255])
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(!format!("{entry:?}").contains("255"));
    let rebuilt = ObservedEntry::decode_snapshot(
        entry.encode_snapshot().as_bytes(),
        entry.source_identity().backend(),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(rebuilt.backend_fact, entry.backend_fact);
}

fn nfs_entry(stable_id: &str) -> ObservedEntry {
    let backend =
        BackendIdentity::new(BackendKind::Nfs, stable_id).unwrap_or_else(|error| panic!("{error}"));
    let source = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"fh")
        .unwrap_or_else(|error| panic!("{error}"));
    ObservedEntry::new(
        StoragePath::new("dir/file").unwrap_or_else(|error| panic!("{error}")),
        EntryKind::File,
        Some(7),
        None,
        source,
    )
    .unwrap_or_else(|error| panic!("{error}"))
}

#[test]
fn snapshots_no_longer_carry_the_endpoint_and_decode_against_the_callers() {
    let endpoint = "nfs://10.128.61.200/ontap_lisaauto_nfs";
    let entry = nfs_entry(endpoint);
    let snapshot = entry.encode_snapshot();
    let bytes = snapshot.as_bytes();
    assert!(
        !bytes
            .windows(endpoint.len())
            .any(|window| window == endpoint.as_bytes())
    );
    assert!(bytes.len() < entry.encode_snapshot_v4().as_bytes().len());
    let backend =
        BackendIdentity::new(BackendKind::Nfs, endpoint).unwrap_or_else(|error| panic!("{error}"));
    let rebuilt =
        ObservedEntry::decode_snapshot(bytes, &backend).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(rebuilt, entry);
    let elsewhere = BackendIdentity::new(BackendKind::Nfs, "nfs://10.128.61.201/other")
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        ObservedEntry::decode_snapshot(bytes, &elsewhere),
        Err(SnapshotDecodeError::BackendMismatch)
    );
    let other_kind =
        BackendIdentity::new(BackendKind::Cifs, endpoint).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        ObservedEntry::decode_snapshot(bytes, &other_kind),
        Err(SnapshotDecodeError::BackendMismatch)
    );
    // A corrupted key under the right endpoint is corruption, not a wrong endpoint.
    let mut corrupted = bytes.to_vec();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 1;
    assert_eq!(
        ObservedEntry::decode_snapshot(&corrupted, &backend),
        Err(SnapshotDecodeError::IdentityMismatch)
    );
}

/// `nfs_entry("source")` as the pre-C4c encoder wrote it (HEAD `cc952ef`), frozen so the v4 decoder
/// is checked against real bytes, not against the encoder it shares code with.
const V4_FIXTURE: &str = "444d45530401080000006469722f66696c65000001070000000000000000010600\
0000736f757263650200000066680200000000000000000000ca80e48bbc766395ccd1dfb50572ec8ccd1d09e9dab9db73e0\
183cb385635de4";

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&text[index..index + 2], 16)
                .unwrap_or_else(|error| panic!("{error}"))
        })
        .collect()
}

#[test]
fn frozen_v4_bytes_decode_with_their_own_identity_and_a_kind_check() {
    let bytes = unhex(V4_FIXTURE);
    assert_eq!(bytes, nfs_entry("source").encode_snapshot_v4().as_bytes());
    let endpoint = BackendIdentity::new(BackendKind::Nfs, "nfs://h/v")
        .unwrap_or_else(|error| panic!("{error}"));
    let rebuilt =
        ObservedEntry::decode_snapshot(&bytes, &endpoint).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(rebuilt, nfs_entry("source"));
    let cifs = BackendIdentity::new(BackendKind::Cifs, "smb://h/s")
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        ObservedEntry::decode_snapshot(&bytes, &cifs),
        Err(SnapshotDecodeError::BackendMismatch)
    );
}

#[test]
fn v5_bytes_relabelled_as_v4_do_not_decode() {
    let backend = BackendIdentity::new(BackendKind::Nfs, "nfs://h/v")
        .unwrap_or_else(|error| panic!("{error}"));
    let mut bytes = nfs_entry("nfs://h/v").encode_snapshot().as_bytes().to_vec();
    bytes[4] = VERSION_WITH_BACKEND_ID;
    // The fingerprint is then read as the length of a stored identity.
    let result = ObservedEntry::decode_snapshot(&bytes, &backend);
    assert!(
        matches!(
            result,
            Err(SnapshotDecodeError::FieldTooLarge | SnapshotDecodeError::Truncated)
        ),
        "{result:?}"
    );
}

/// The documented hazard: a v4 entry keeps its old identity, so re-encoding it gives a v5 snapshot
/// that no derived endpoint decodes. Callers copy v4 bytes forward instead.
#[test]
fn re_encoding_a_v4_entry_keeps_its_old_identity() {
    let endpoint = BackendIdentity::new(BackendKind::Nfs, "nfs://h/v")
        .unwrap_or_else(|error| panic!("{error}"));
    let legacy = ObservedEntry::decode_snapshot(&unhex(V4_FIXTURE), &endpoint)
        .unwrap_or_else(|error| panic!("{error}"));
    let re_encoded = legacy.encode_snapshot();
    assert_eq!(
        ObservedEntry::decode_snapshot(re_encoded.as_bytes(), &endpoint),
        Err(SnapshotDecodeError::BackendMismatch)
    );
}

#[test]
fn symlink_without_target_is_rejected_at_construction_and_decode() {
    let backend =
        BackendIdentity::new(BackendKind::Local, "local").unwrap_or_else(|error| panic!("{error}"));
    let source = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"inode")
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(
        ObservedEntry::new(
            StoragePath::root(),
            EntryKind::Symlink,
            None,
            None,
            source.clone(),
        )
        .is_err()
    );
    let file = ObservedEntry::new(StoragePath::root(), EntryKind::File, None, None, source)
        .unwrap_or_else(|error| panic!("{error}"));
    let mut malformed = file.encode_snapshot().as_bytes().to_vec();
    malformed[10] = 2;
    assert_eq!(
        ObservedEntry::decode_snapshot(&malformed, file.source_identity().backend()),
        Err(SnapshotDecodeError::Malformed)
    );
}
