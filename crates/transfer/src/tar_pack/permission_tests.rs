use super::*;
use crate::AssertTestValue;
use crate::model::{
    BackendIdentity, BackendKind, MetadataObservation, MetadataObservations, MetadataProvenance,
    OwnershipMode, PermissionFacts, PrincipalIdentity,
};

fn observed(kind: EntryKind, facts: Option<PermissionFacts>) -> ObservedEntry {
    let backend = BackendIdentity::new(BackendKind::Nfs, "archive-tests").assert_value("backend");
    let identity = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"file")
        .assert_value("identity");
    ObservedEntry::new(
        StoragePath::new("entry").assert_value("path"),
        kind,
        Some(0),
        None,
        identity,
    )
    .assert_value("entry")
    .with_permissions(facts.map(Box::new))
    .assert_value("permissions")
}

fn fields(entry: &ObservedEntry) -> (u32, u32, u32) {
    let header = build_header_for_entry(entry).assert_value("header");
    let octal = |start| {
        let value = std::str::from_utf8(&header[start..start + 7]).assert_value("octal field");
        u32::from_str_radix(value, 8).assert_value("octal value")
    };
    (octal(100), octal(108), octal(116))
}

#[test]
fn permission_facts_supply_mode_without_inventing_numeric_principals() {
    let entry = observed(
        EntryKind::File,
        Some(PermissionFacts {
            owner: Some(PrincipalIdentity::Named("1000".into())),
            group: Some(PrincipalIdentity::Named("hadoop".into())),
            mode: Some(0o640),
        }),
    );
    assert_eq!(fields(&entry), (0o640, TAR_UNKNOWN_ID, TAR_UNKNOWN_ID));
}

#[test]
fn permission_facts_keep_only_set_id_bits_with_known_numeric_subjects() {
    for (owner, group, expected) in [
        (
            PrincipalIdentity::Numeric(1000),
            PrincipalIdentity::Named("hadoop".into()),
            (0o4755, 1000, TAR_UNKNOWN_ID),
        ),
        (
            PrincipalIdentity::Named("hdfs".into()),
            PrincipalIdentity::Numeric(100),
            (0o2755, TAR_UNKNOWN_ID, 100),
        ),
    ] {
        let entry = observed(
            EntryKind::File,
            Some(PermissionFacts {
                owner: Some(owner),
                group: Some(group),
                mode: Some(0o6755),
            }),
        );
        assert_eq!(fields(&entry), expected);
    }
}

#[test]
fn directory_setgid_survives_unknown_numeric_subjects() {
    let entry = observed(
        EntryKind::Directory,
        Some(PermissionFacts {
            mode: Some(0o2775),
            ..Default::default()
        }),
    );
    assert_eq!(fields(&entry), (0o2775, TAR_UNKNOWN_ID, TAR_UNKNOWN_ID));
}

#[test]
fn explicit_zero_mode_and_root_ids_are_known_facts() {
    let entry = observed(
        EntryKind::File,
        Some(PermissionFacts {
            owner: Some(PrincipalIdentity::Numeric(0)),
            group: Some(PrincipalIdentity::Numeric(0)),
            mode: Some(0),
        }),
    );
    assert_eq!(fields(&entry), (0, 0, 0));
}

fn numeric_metadata() -> MetadataObservations {
    let mut metadata = MetadataObservations::default();
    metadata.set_ownership_mode(MetadataObservation::Value {
        value: OwnershipMode {
            uid: 1000,
            gid: 100,
            mode: 0o6755,
        },
        provenance: MetadataProvenance::Inline,
    });
    metadata
}

#[test]
fn absent_permission_facts_use_numeric_ownership_metadata() {
    let entry = observed(EntryKind::File, None).with_metadata(numeric_metadata());
    assert_eq!(fields(&entry), (0o6755, 1000, 100));
}

#[test]
fn unknown_permission_facets_do_not_fall_back_to_numeric_metadata() {
    let entry = observed(EntryKind::File, Some(PermissionFacts::default()))
        .with_metadata(numeric_metadata());
    assert_eq!(fields(&entry), (0, TAR_UNKNOWN_ID, TAR_UNKNOWN_ID));
}
