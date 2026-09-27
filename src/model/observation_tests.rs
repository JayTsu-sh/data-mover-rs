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

/// `nfs_entry("source")` as the pre-C4c encoder wrote it (HEAD `cc952ef`), frozen: an older
/// format is refused as a version this build does not read (P7 C11b), never misread.
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
fn frozen_v4_bytes_are_an_unsupported_version() {
    let endpoint = BackendIdentity::new(BackendKind::Nfs, "nfs://h/v")
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        ObservedEntry::decode_snapshot(&unhex(V4_FIXTURE), &endpoint),
        Err(SnapshotDecodeError::UnsupportedVersion)
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

/// Identity bytes held inline and on the heap are the same identity: the bytes read back, the
/// key derived from them (the one formula, whatever the storage), equality, and a snapshot's
/// round trip all agree on either side of the inline limit.
#[test]
fn identity_bytes_mean_the_same_whether_held_inline_or_not() {
    let backend =
        BackendIdentity::new(BackendKind::Nfs, "cluster").unwrap_or_else(|error| panic!("{error}"));
    for len in [
        1,
        16,
        32,
        INLINE_STABLE_BYTES - 1,
        INLINE_STABLE_BYTES,
        INLINE_STABLE_BYTES + 1,
        64,
        300,
    ] {
        let bytes: Vec<u8> = (0..len)
            .map(|index| u8::try_from(index * 7 % 251).unwrap_or(0))
            .collect();
        let source = SourceIdentity::new(
            backend.clone(),
            IdentityStrength::StableWithinBackend,
            &bytes,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            matches!(source.stable_bytes, StableBytes::Inline { .. }),
            len <= INLINE_STABLE_BYTES,
            "{len}"
        );
        assert_eq!(source.stable_bytes(), Some(bytes.as_slice()), "{len}");
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"data-mover/source-identity/v1\0");
        hasher.update(&[backend_tag(BackendKind::Nfs), 1]);
        hasher.update(&("cluster".len() as u64).to_le_bytes());
        hasher.update(b"cluster");
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
        assert_eq!(
            source.identity_key().as_bytes(),
            hasher.finalize().as_bytes(),
            "{len}"
        );
        assert_eq!(source.clone(), source, "{len}");
        let mut other = bytes.clone();
        other[len - 1] ^= 1;
        let differs = SourceIdentity::new(
            backend.clone(),
            IdentityStrength::StableWithinBackend,
            &other,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_ne!(differs, source, "{len}");
        let entry = ObservedEntry::new(StoragePath::root(), EntryKind::File, None, None, source)
            .unwrap_or_else(|error| panic!("{error}"));
        let rebuilt = ObservedEntry::decode_snapshot(entry.encode_snapshot().as_bytes(), &backend)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(rebuilt, entry, "{len}");
    }
}
