use std::error::Error;
use std::fmt::Write as _;

use crate::model::{
    BackendIdentity, BackendKind, EntryIdentityKey, IdentityStrength, SourceIdentity, StoragePath,
};

use super::{BindingSource, TransferIdentity, binding_hash};

type TestResult = Result<(), Box<dyn Error>>;

/// Kinds are named by their string form: `transfer` never names a backend variant (architecture
/// guard), and the kind's string is exactly what the identity hashes.
fn endpoint(kind: &str, stable_id: &str) -> Result<BackendIdentity, Box<dyn Error>> {
    Ok(BackendIdentity::new(
        kind.parse::<BackendKind>()?,
        stable_id,
    )?)
}

fn nfs(stable_id: &str) -> Result<BackendIdentity, Box<dyn Error>> {
    endpoint("nfs", stable_id)
}

fn s3(stable_id: &str) -> Result<BackendIdentity, Box<dyn Error>> {
    endpoint("s3", stable_id)
}

fn path(value: &str) -> Result<StoragePath, Box<dyn Error>> {
    Ok(StoragePath::new(value)?)
}

/// `nfs://src/export` `a/b.txt` → `s3://bucket.dst/prefix` `a/b.txt`.
fn reference() -> Result<TransferIdentity, Box<dyn Error>> {
    Ok(TransferIdentity::derive(
        &nfs("nfs://src/export")?,
        &path("a/b.txt")?,
        &s3("s3://dst/bucket/prefix")?,
        &path("a/b.txt")?,
    ))
}

/// Pins the derivation: a change to the encoding would silently orphan every recorded transfer,
/// so it must be a deliberate new domain version, never an accident. Both values were computed
/// independently from the documented encoding (Python `blake3`), not copied from this code.
#[test]
fn derived_identity_matches_the_frozen_vector() -> TestResult {
    assert_eq!(
        reference()?.to_string(),
        "23082950a6e642af339e0dbf47cac5c1c808e66ab13ec12e42f01c1a5e46ad28"
    );
    assert_eq!(
        TransferIdentity::new("logical-copy-7")?.to_string(),
        "e8cdb2c64b3e406705a02aa3779bf2e5cf5509391eb823b24db79045280288ea"
    );
    Ok(())
}

#[test]
fn equal_inputs_derive_equal_identities_from_independent_values() -> TestResult {
    assert_eq!(reference()?, reference()?);
    Ok(())
}

#[test]
fn every_field_changes_the_identity() -> TestResult {
    let reference = reference()?;
    let source = nfs("nfs://src/export")?;
    let destination = s3("s3://dst/bucket/prefix")?;
    let same_path = path("a/b.txt")?;
    let variants = [
        TransferIdentity::derive(
            &endpoint("local", "nfs://src/export")?,
            &same_path,
            &destination,
            &same_path,
        ),
        TransferIdentity::derive(
            &nfs("nfs://src/other")?,
            &same_path,
            &destination,
            &same_path,
        ),
        TransferIdentity::derive(&source, &path("a/c.txt")?, &destination, &same_path),
        TransferIdentity::derive(
            &source,
            &same_path,
            &endpoint("nfs", "s3://dst/bucket/prefix")?,
            &same_path,
        ),
        TransferIdentity::derive(
            &source,
            &same_path,
            &s3("s3://dst/bucket/other")?,
            &same_path,
        ),
        TransferIdentity::derive(&source, &same_path, &destination, &path("a/c.txt")?),
    ];
    for (index, variant) in variants.iter().enumerate() {
        assert_ne!(*variant, reference, "variant {index}");
    }
    Ok(())
}

/// Source and destination are not interchangeable: copying back is another transfer.
#[test]
fn swapping_the_two_sides_changes_the_identity() -> TestResult {
    let left = nfs("nfs://host/a")?;
    let right = nfs("nfs://host/b")?;
    let file = path("f")?;
    assert_ne!(
        TransferIdentity::derive(&left, &file, &right, &file),
        TransferIdentity::derive(&right, &file, &left, &file)
    );
    Ok(())
}

/// Moving bytes between an endpoint and a path must not give the same input stream.
#[test]
fn field_boundaries_are_unambiguous() -> TestResult {
    let destination = nfs("nfs://dst/export")?;
    let file = path("f")?;
    assert_ne!(
        TransferIdentity::derive(&nfs("nfs://src/a")?, &path("b/c")?, &destination, &file),
        TransferIdentity::derive(&nfs("nfs://src/a/b")?, &path("c")?, &destination, &file)
    );
    Ok(())
}

#[test]
fn a_label_is_validated_and_names_one_identity() -> TestResult {
    assert!(TransferIdentity::new("").is_err());
    assert!(TransferIdentity::new("  ").is_err());
    assert!(TransferIdentity::new("a\0b").is_err());
    assert!(TransferIdentity::new("x".repeat(1025)).is_err());
    assert!(TransferIdentity::new("x".repeat(1024)).is_ok());
    assert_eq!(
        TransferIdentity::new("job-1")?,
        TransferIdentity::new("job-1")?
    );
    assert_ne!(
        TransferIdentity::new("job-1")?,
        TransferIdentity::new("job-2")?
    );
    Ok(())
}

#[test]
fn identity_displays_as_lowercase_hex_and_debug_shows_it() -> TestResult {
    let identity = reference()?;
    let shown = identity.to_string();
    assert_eq!(shown.len(), 64);
    assert!(
        shown
            .chars()
            .all(|digit| digit.is_ascii_digit() || ('a'..='f').contains(&digit))
    );
    assert_eq!(
        format!("{identity:?}"),
        format!("TransferIdentity({shown})")
    );
    Ok(())
}

fn source_key(stable_bytes: &[u8]) -> Result<EntryIdentityKey, Box<dyn Error>> {
    Ok(SourceIdentity::new(
        nfs("nfs://src/export")?,
        IdentityStrength::StableWithinBackend,
        stable_bytes,
    )?
    .identity_key())
}

/// One binding input; each test varies one field of [`BASE`].
#[derive(Clone, Copy)]
struct Case<'a> {
    source_path: &'a str,
    stable_bytes: &'a [u8],
    size: Option<u64>,
    content_version: Option<&'a [u8]>,
    destination: &'a str,
    final_path: &'a str,
}

const BASE: Case<'static> = Case {
    source_path: "a/b.txt",
    stable_bytes: b"fileid-7",
    size: Some(10),
    content_version: Some(b"v1"),
    destination: "s3://dst/b",
    final_path: "f",
};

fn binding(identity: &TransferIdentity, case: Case<'_>) -> Result<[u8; 32], Box<dyn Error>> {
    Ok(binding_hash(
        identity,
        &BindingSource {
            path: &path(case.source_path)?,
            identity_key: source_key(case.stable_bytes)?,
            size: case.size,
            content_version: case.content_version,
        },
        &s3(case.destination)?,
        &path(case.final_path)?,
    ))
}

/// Pins the binding encoding, which is persisted in every stage and recovery record. Computed
/// independently from the documented encoding (Python `blake3`), not copied from this code.
#[test]
fn binding_matches_the_frozen_vector() -> TestResult {
    let hex = binding(&reference()?, BASE)?
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        });
    assert_eq!(
        hex,
        "5072d721a8cadca8b846308e7d6a77d7c04d28f7e9e90cd3095d70cdeccb7ecf"
    );
    Ok(())
}

/// A source edit keeps the identity — it is still the same file going to the same place — but
/// changes the binding, so the old stage is restarted rather than resumed.
#[test]
fn a_source_change_keeps_the_identity_and_changes_the_binding() -> TestResult {
    let identity = reference()?;
    let original = binding(&identity, BASE)?;
    assert_eq!(original, binding(&identity, BASE)?);
    for (name, changed) in [
        (
            "size",
            Case {
                size: Some(11),
                ..BASE
            },
        ),
        ("no size", Case { size: None, ..BASE }),
        // v2 encoded an unknown size as u64::MAX, so these two collided.
        (
            "max size",
            Case {
                size: Some(u64::MAX),
                ..BASE
            },
        ),
        (
            "version",
            Case {
                content_version: Some(b"v2"),
                ..BASE
            },
        ),
        (
            "no version",
            Case {
                content_version: None,
                ..BASE
            },
        ),
        (
            "empty version",
            Case {
                content_version: Some(b""),
                ..BASE
            },
        ),
        (
            "entry",
            Case {
                stable_bytes: b"fileid-8",
                ..BASE
            },
        ),
    ] {
        assert_ne!(binding(&identity, changed)?, original, "{name}");
    }
    assert_ne!(
        binding(&identity, Case { size: None, ..BASE })?,
        binding(
            &identity,
            Case {
                size: Some(u64::MAX),
                ..BASE
            }
        )?
    );
    Ok(())
}

/// One label reused for two sources or two destinations must not share a binding, or a stage
/// could be resumed into the wrong file. The source path matters even with an equal identity key:
/// an S3 object's identity key is its `versionId` or `ETag`, so two keys with equal content share it.
#[test]
fn a_reused_label_binds_each_source_and_destination_separately() -> TestResult {
    let label = TransferIdentity::new("nightly")?;
    let original = binding(&label, BASE)?;
    for (name, changed) in [
        (
            "source path",
            Case {
                source_path: "a/c.txt",
                ..BASE
            },
        ),
        (
            "destination",
            Case {
                destination: "s3://dst/other",
                ..BASE
            },
        ),
        (
            "final path",
            Case {
                final_path: "g",
                ..BASE
            },
        ),
    ] {
        assert_ne!(binding(&label, changed)?, original, "{name}");
    }
    assert_ne!(binding(&TransferIdentity::new("weekly")?, BASE)?, original);
    Ok(())
}
