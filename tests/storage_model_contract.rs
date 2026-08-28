use std::str::FromStr;

use data_mover::model::{
    BackendConfig, BackendIdentity, BackendKind, BackendSessionFailure, CifsConfig, EntryKind,
    EntryOperationFailure, FailureClass, HdfsConfig, LocalConfig, NfsConfig, NfsVersion, Operation,
    S3Config, SpecialFileKind, StoragePath, StorageTimestamp, TimePrecision, Transience,
};

#[test]
fn backend_kind_is_closed_and_has_stable_wire_names() {
    let cases = [
        (BackendKind::Local, "local"),
        (BackendKind::Nfs, "nfs"),
        (BackendKind::Cifs, "cifs"),
        (BackendKind::S3, "s3"),
        (BackendKind::Hdfs, "hdfs"),
    ];
    for (kind, name) in cases {
        assert_eq!(kind.as_str(), name);
        assert_eq!(BackendKind::from_str(name), Ok(kind));
    }
    assert!(BackendKind::from_str("nfs41").is_err());
}

#[test]
fn typed_configs_carry_their_kind_without_path_inference() -> Result<(), Box<dyn std::error::Error>>
{
    let configs = [
        BackendConfig::Local(LocalConfig::new("/srv/source")?),
        BackendConfig::Nfs(NfsConfig::new("10.0.0.1", "/export", NfsVersion::V4_1)?),
        BackendConfig::Cifs(CifsConfig::new("fas2750", "share")?),
        BackendConfig::S3(S3Config::new("https://s3.example", "bucket", "region-1")?),
        BackendConfig::Hdfs(HdfsConfig::new("hdfs-ha", "/data")?),
    ];
    assert_eq!(
        configs.map(|config| config.kind()),
        [
            BackendKind::Local,
            BackendKind::Nfs,
            BackendKind::Cifs,
            BackendKind::S3,
            BackendKind::Hdfs,
        ]
    );
    Ok(())
}

#[test]
fn typed_configs_reject_blank_required_fields() {
    assert!(LocalConfig::new("").is_err());
    assert!(NfsConfig::new("host", "", NfsVersion::V3).is_err());
    assert!(CifsConfig::new("host", " ").is_err());
    assert!(S3Config::new("endpoint", "", "region").is_err());
    assert!(HdfsConfig::new("", "/root").is_err());
}

#[test]
fn configuration_debug_output_redacts_connection_values() -> Result<(), Box<dyn std::error::Error>>
{
    let config = S3Config::new(
        "https://user:secret@example.test",
        "private-bucket",
        "region",
    )?;
    let debug = format!("{config:?}");
    assert!(!debug.contains("secret"));
    assert!(!debug.contains("private-bucket"));
    Ok(())
}

#[test]
fn storage_paths_losslessly_preserve_backend_valid_names() -> Result<(), Box<dyn std::error::Error>>
{
    assert_eq!(StoragePath::root().as_str(), "");
    for value in [
        "dir/file",
        "/absolute-key",
        "../key",
        "dir//key",
        "dir\\key",
        "trailing/",
    ] {
        assert_eq!(StoragePath::new(value)?.as_str(), value);
    }
    assert!(StoragePath::new("nul\0byte").is_err());
    Ok(())
}

#[test]
fn identity_time_and_entry_kind_are_protocol_neutral() -> Result<(), Box<dyn std::error::Error>> {
    let identity = BackendIdentity::new(BackendKind::Nfs, "cluster-a:/export")?;
    assert_eq!(identity.kind(), BackendKind::Nfs);
    assert_eq!(identity.stable_id(), "cluster-a:/export");

    let timestamp = StorageTimestamp::new(1_725_000_000_123_456_789, TimePrecision::Nanoseconds)?;
    assert_eq!(timestamp.unix_nanos(), 1_725_000_000_123_456_789);
    assert_eq!(timestamp.precision(), TimePrecision::Nanoseconds);

    let kinds = [
        EntryKind::File,
        EntryKind::Directory,
        EntryKind::Symlink,
        EntryKind::Special(SpecialFileKind::Fifo),
    ];
    assert_eq!(kinds.len(), 4);
    Ok(())
}

#[test]
fn timestamps_reject_precision_claims_the_value_does_not_support()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        (2_000_000_000, TimePrecision::Seconds),
        (-2_000_000, TimePrecision::Milliseconds),
        (2_000, TimePrecision::Microseconds),
        (-1, TimePrecision::Nanoseconds),
    ];
    for (value, precision) in cases {
        assert_eq!(StorageTimestamp::new(value, precision)?.unix_nanos(), value);
    }
    assert!(StorageTimestamp::new(1, TimePrecision::Seconds).is_err());
    assert!(StorageTimestamp::new(-1_000_001, TimePrecision::Milliseconds).is_err());
    Ok(())
}

#[test]
fn entry_and_session_failures_are_distinct_public_axes() -> Result<(), Box<dyn std::error::Error>> {
    let entry = EntryOperationFailure::new(
        StoragePath::new("dir/file")?,
        Operation::Observe,
        FailureClass::PermissionDenied,
        Transience::Permanent,
        "ACL denied",
    )?;
    assert_eq!(entry.path().as_str(), "dir/file");
    assert_eq!(entry.operation(), Operation::Observe);
    assert_eq!(entry.class(), FailureClass::PermissionDenied);
    assert_eq!(entry.transience(), Transience::Permanent);
    assert!(entry.to_string().contains("dir/file"));
    let secret = EntryOperationFailure::new(
        StoragePath::root(),
        Operation::Read,
        FailureClass::Authentication,
        Transience::Permanent,
        "password=hunter2",
    )?;
    assert!(!secret.to_string().contains("hunter2"));
    assert!(!format!("{secret:?}").contains("hunter2"));

    let session = BackendSessionFailure::new(
        Operation::Connect,
        FailureClass::Authentication,
        Transience::Permanent,
        "ticket expired",
    )?;
    assert_eq!(session.operation(), Operation::Connect);
    assert_eq!(session.class(), FailureClass::Authentication);
    assert_eq!(session.transience(), Transience::Permanent);
    assert!(session.to_string().contains("backend session"));
    Ok(())
}
