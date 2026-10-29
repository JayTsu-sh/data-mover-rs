use std::num::NonZeroUsize;
use std::path::Path;

use super::{
    IntegrityDifference, IntegrityIgnored, IntegrityMode, IntegrityOptions, IntegrityPlan,
    IntegrityRequest, IntegritySide, compare, same_instant,
};
use crate::model::{
    BackendIdentity, BackendKind, FailureClass, ObservedEntry, StoragePath, StorageTimestamp,
    TimePrecision,
};
use crate::storage::{LocalBackendConfig, Storage};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Fixed instant both fixtures are pinned to, so comparisons test content rather than clocks.
const FIXED_MTIME: filetime::FileTime = filetime::FileTime::from_unix_time(1_700_000_000, 0);

fn storage(root: &Path) -> Result<Storage> {
    let depth = NonZeroUsize::new(2).ok_or("constant is nonzero")?;
    Ok(crate::storage::backends::local::connect_backend(
        LocalBackendConfig {
            rename_context: None,
            read_chunk_bytes: None,
            write_chunk_bytes: None,
            root: root.to_path_buf(),
            read_concurrency: depth,
            write_concurrency: depth,
        },
    )?)
}

fn write(root: &Path, name: &str, payload: &[u8]) -> Result {
    let path = root.join(name);
    std::fs::write(&path, payload)?;
    filetime::set_file_mtime(&path, FIXED_MTIME)?;
    Ok(())
}

async fn observed_file(storage: &Storage, path: &StoragePath) -> Result<ObservedEntry> {
    let described = storage.read_source()?.describe(path).await?;
    let modified = described
        .inline_timestamps()
        .and_then(|times| times.modified);
    Ok(ObservedEntry::new(
        described.path,
        described.kind,
        described.size,
        modified,
        described.source_identity,
    )?)
}

struct Fixture {
    source_root: tempfile::TempDir,
    destination_root: tempfile::TempDir,
}

impl Fixture {
    fn new(source: Option<&[u8]>, destination: Option<&[u8]>) -> Result<Self> {
        let fixture = Self {
            source_root: tempfile::tempdir()?,
            destination_root: tempfile::tempdir()?,
        };
        if let Some(payload) = source {
            write(fixture.source_root.path(), "object.bin", payload)?;
        }
        if let Some(payload) = destination {
            write(fixture.destination_root.path(), "object.bin", payload)?;
        }
        Ok(fixture)
    }

    fn request(&self, mode: IntegrityMode) -> Result<IntegrityRequest> {
        Ok(IntegrityRequest {
            source: storage(self.source_root.path())?,
            source_path: StoragePath::new("object.bin")?,
            destination: storage(self.destination_root.path())?,
            destination_path: StoragePath::new("object.bin")?,
            plan: IntegrityPlan::default(),
            options: IntegrityOptions {
                mode,
                ..IntegrityOptions::default()
            },
            cancel: tokio_util::sync::CancellationToken::new(),
        })
    }
}

#[tokio::test]
async fn identical_objects_match_and_report_equal_digests() -> Result {
    let fixture = Fixture::new(Some(b"payload-bytes"), Some(b"payload-bytes"))?;
    let report = compare(fixture.request(IntegrityMode::Content)?).await?;
    assert!(report.matches(), "unexpected {:?}", report.differences());
    assert_eq!(report.compared_bytes(), 13);
    let (source, destination) = report.digests().ok_or("content mode must report digests")?;
    assert_eq!(source, destination);
    assert_eq!(source, *blake3::hash(b"payload-bytes").as_bytes());
    Ok(())
}

#[tokio::test]
async fn compare_accepts_observations_on_either_side_and_paths_still_work() -> Result {
    let fixture = Fixture::new(Some(b"payload"), Some(b"payload"))?;
    let source = storage(fixture.source_root.path())?;
    let destination = storage(fixture.destination_root.path())?;
    let path = StoragePath::new("object.bin")?;
    let source_observed = observed_file(&source, &path).await?;
    let destination_observed = observed_file(&destination, &path).await?;

    let both = IntegrityRequest {
        source: source.clone(),
        source_path: Box::new(source_observed.clone()),
        destination: destination.clone(),
        destination_path: destination_observed.clone(),
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    assert!(compare(both).await?.matches());

    let source_observed_only = IntegrityRequest {
        source: source.clone(),
        source_path: source_observed,
        destination: destination.clone(),
        destination_path: path.clone(),
        plan: IntegrityPlan::default(),
        options: IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    assert!(compare(source_observed_only).await?.matches());

    let destination_observed_only = IntegrityRequest {
        source,
        source_path: path,
        destination,
        destination_path: destination_observed,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    assert!(compare(destination_observed_only).await?.matches());
    Ok(())
}

#[tokio::test]
async fn stale_or_foreign_observation_is_attributed_to_its_side() -> Result {
    let fixture = Fixture::new(Some(b"payload"), Some(b"payload"))?;
    let source = storage(fixture.source_root.path())?;
    let destination = storage(fixture.destination_root.path())?;
    let path = StoragePath::new("object.bin")?;
    let observed = observed_file(&source, &path).await?;

    let Err(foreign) = compare(IntegrityRequest {
        source: destination.clone(),
        source_path: observed.clone(),
        destination: destination.clone(),
        destination_path: path.clone(),
        plan: IntegrityPlan::default(),
        options: IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    })
    .await
    else {
        panic!("an observation from another storage must be rejected");
    };
    assert_eq!(foreign.side, IntegritySide::Source);
    assert!(matches!(
        foreign.failure,
        crate::storage::StorageRoleFailure::Entry(ref error)
            if error.class() == FailureClass::InvalidInput
    ));

    filetime::set_file_mtime(
        fixture.source_root.path().join("object.bin"),
        filetime::FileTime::from_unix_time(1_700_000_050, 0),
    )?;
    let Err(stale) = compare(IntegrityRequest {
        source,
        source_path: observed,
        destination,
        destination_path: path,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    })
    .await
    else {
        panic!("a changed entry must not pass an observation constraint");
    };
    assert_eq!(stale.side, IntegritySide::Source);
    assert!(matches!(
        stale.failure,
        crate::storage::StorageRoleFailure::Entry(ref error)
            if error.class() == FailureClass::Conflict
    ));
    Ok(())
}

#[tokio::test]
async fn observed_versions_are_compared_even_after_newer_versions_become_current() -> Result {
    use crate::model::{HistoryOrder, ObjectHistory, RecordKind, VersionRecord};
    use crate::storage::backends::s3::tests::{MemoryS3, Versioning};

    let source_protocol = std::sync::Arc::new(MemoryS3::default());
    let destination_protocol = std::sync::Arc::new(MemoryS3::default());
    source_protocol.set_versioning(Versioning::Enabled);
    destination_protocol.set_versioning(Versioning::Enabled);
    for protocol in [&source_protocol, &destination_protocol] {
        protocol
            .put_version("object", "v1", bytes::Bytes::from_static(b"same"))
            .await;
    }
    let source = crate::storage::backends::s3::connect(
        source_protocol.clone(),
        BackendIdentity::new(BackendKind::S3, "integrity-versioned-source")?,
        None,
    )?;
    let destination = crate::storage::backends::s3::connect(
        destination_protocol.clone(),
        BackendIdentity::new(BackendKind::S3, "integrity-versioned-destination")?,
        None,
    )?;
    let path = StoragePath::new("object")?;
    let versioned = |described: crate::storage::SourceDescriptor| -> Result<ObservedEntry> {
        let record = VersionRecord::new(
            Some("v1".into()),
            true,
            RecordKind::Object {
                size: described.size.ok_or("v1 must have a size")?,
                etag: String::new(),
                modified: None,
            },
            described.source_identity.clone(),
            None,
        )?;
        let history = ObjectHistory::whole(vec![record], HistoryOrder::Server)?;
        Ok(ObservedEntry::new(
            described.path,
            described.kind,
            described.size,
            None,
            described.source_identity,
        )?
        .with_history(Some(Box::new(history))))
    };
    let source_observed = versioned(source.read_source()?.describe(&path).await?)?;
    let destination_observed = versioned(destination.read_source()?.describe(&path).await?)?;

    source_protocol
        .put_version("object", "v2", bytes::Bytes::from_static(b"AAAA"))
        .await;
    destination_protocol
        .put_version("object", "v2", bytes::Bytes::from_static(b"BBBB"))
        .await;

    let observed_report = compare(IntegrityRequest {
        source: source.clone(),
        source_path: source_observed,
        destination: destination.clone(),
        destination_path: destination_observed,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    })
    .await?;
    assert!(observed_report.matches());

    let current_report = compare(IntegrityRequest {
        source,
        source_path: path.clone(),
        destination,
        destination_path: path,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    })
    .await?;
    assert!(matches!(
        current_report.differences(),
        [IntegrityDifference::Content { .. }]
    ));
    Ok(())
}

#[tokio::test]
async fn equal_size_but_different_bytes_is_only_caught_in_content_mode() -> Result {
    let fixture = Fixture::new(Some(b"aaaa"), Some(b"bbbb"))?;
    let metadata_report = compare(fixture.request(IntegrityMode::Metadata)?).await?;
    assert!(
        metadata_report.matches(),
        "metadata mode cannot see byte differences"
    );
    assert_eq!(metadata_report.compared_bytes(), 0);
    assert!(metadata_report.digests().is_none());

    let content_report = compare(fixture.request(IntegrityMode::Content)?).await?;
    assert_eq!(
        content_report.differences(),
        [IntegrityDifference::Content {
            source_bytes: 4,
            destination_bytes: 4
        }]
    );
    Ok(())
}

#[tokio::test]
async fn a_size_difference_is_reported_without_reading_bytes() -> Result {
    let fixture = Fixture::new(Some(b"four"), Some(b"seventeen-ish"))?;
    let report = compare(fixture.request(IntegrityMode::Metadata)?).await?;
    assert_eq!(
        report.differences(),
        [IntegrityDifference::Size {
            source: Some(4),
            destination: Some(13)
        }]
    );
    Ok(())
}

#[tokio::test]
async fn absence_on_either_side_is_a_difference_not_a_failure() -> Result {
    let missing_destination = Fixture::new(Some(b"only-source"), None)?;
    let report = compare(missing_destination.request(IntegrityMode::Content)?).await?;
    assert_eq!(
        report.differences(),
        [IntegrityDifference::Missing {
            side: IntegritySide::Destination
        }]
    );

    let missing_both = Fixture::new(None, None)?;
    let report = compare(missing_both.request(IntegrityMode::Metadata)?).await?;
    assert_eq!(
        report.differences(),
        [
            IntegrityDifference::Missing {
                side: IntegritySide::Source
            },
            IntegrityDifference::Missing {
                side: IntegritySide::Destination
            }
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_differing_modification_time_is_reported() -> Result {
    let fixture = Fixture::new(Some(b"same"), Some(b"same"))?;
    filetime::set_file_mtime(
        fixture.destination_root.path().join("object.bin"),
        filetime::FileTime::from_unix_time(1_700_000_600, 0),
    )?;
    let report = compare(fixture.request(IntegrityMode::Metadata)?).await?;
    assert!(matches!(
        report.differences(),
        [IntegrityDifference::Modified { .. }]
    ));
    Ok(())
}

#[tokio::test]
async fn mtime_tolerance_absorbs_a_small_skew() -> Result {
    let fixture = Fixture::new(Some(b"same"), Some(b"same"))?;
    filetime::set_file_mtime(
        fixture.destination_root.path().join("object.bin"),
        filetime::FileTime::from_unix_time(1_700_000_001, 0),
    )?;
    let mut request = fixture.request(IntegrityMode::Metadata)?;
    request.options.mtime_tolerance = std::time::Duration::from_secs(2);
    let report = compare(request).await?;
    assert!(report.matches(), "unexpected {:?}", report.differences());
    Ok(())
}

#[test]
fn timestamps_compare_at_the_coarser_observed_precision() -> Result {
    let nanosecond = StorageTimestamp::new(1_700_000_000_123_456_789, TimePrecision::Nanoseconds)?;
    let second = StorageTimestamp::new(1_700_000_000_000_000_000, TimePrecision::Seconds)?;
    assert!(
        same_instant(Some(nanosecond), Some(second), std::time::Duration::ZERO),
        "a whole-second destination matches a nanosecond source in the same second"
    );

    let next_second = StorageTimestamp::new(1_700_000_001_000_000_000, TimePrecision::Seconds)?;
    assert!(!same_instant(
        Some(nanosecond),
        Some(next_second),
        std::time::Duration::ZERO
    ));
    assert!(same_instant(
        Some(nanosecond),
        Some(next_second),
        std::time::Duration::from_secs(1)
    ));

    assert!(same_instant(None, None, std::time::Duration::ZERO));
    assert!(!same_instant(
        Some(second),
        None,
        std::time::Duration::from_secs(30)
    ));
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// A backend that cannot report modification time at all (the S3 shape)
// ---------------------------------------------------------------------------------------------

/// Read source over one in-memory object, paired with a metadata role that reports timestamps
/// as `NotApplicable` — a backend with no modification time at all. (S3 does report its
/// `Last-Modified`; see [`an_object_stores_upload_time_so_its_mtime_is_not_compared`].)
struct TimelessBackend {
    identity: crate::model::BackendIdentity,
    payload: bytes::Bytes,
    timestamp: Option<StorageTimestamp>,
    read_limits: Option<(usize, usize)>,
    acl: crate::model::MetadataObservation<crate::model::AclMetadata>,
    xattrs: crate::model::MetadataObservation<Vec<crate::model::ExtendedAttribute>>,
}

#[async_trait::async_trait]
impl crate::storage::ReadSource for TimelessBackend {
    fn read_concurrency(&self) -> usize {
        self.read_limits.map_or(1, |(_, depth)| depth)
    }

    fn maximum_read_chunk_bytes(&self) -> usize {
        self.read_limits.map_or(1024 * 1024, |(chunk, _)| chunk)
    }

    async fn describe(
        &self,
        path: &StoragePath,
    ) -> std::result::Result<crate::storage::SourceDescriptor, crate::storage::StorageRoleFailure>
    {
        let descriptor = crate::storage::SourceDescriptor::new(
            path.clone(),
            crate::model::EntryKind::File,
            Some(self.payload.len() as u64),
            crate::model::SourceIdentity::new(
                self.identity.clone(),
                crate::model::IdentityStrength::PathScoped,
                path.as_str().as_bytes(),
            )
            .unwrap_or_else(|error| panic!("{error}")),
        );
        Ok(match self.timestamp {
            Some(modified) => descriptor.with_inline_timestamps(crate::model::TimestampMetadata {
                accessed: None,
                modified: Some(modified),
                created: None,
            }),
            None => descriptor,
        })
    }

    async fn read(
        &self,
        request: crate::storage::ReadRequest,
    ) -> std::result::Result<crate::storage::ByteStream, crate::storage::StorageRoleFailure> {
        if let Some((chunk, depth)) = self.read_limits {
            assert_eq!(request.maximum_chunk_bytes, chunk);
            assert_eq!(request.read_inflight, depth);
        }
        let payload = self.payload.clone();
        Ok(Box::pin(futures::stream::once(async move { Ok(payload) })))
    }
}

#[async_trait::async_trait]
impl crate::storage::Metadata for TimelessBackend {
    async fn observe(
        &self,
        _path: &StoragePath,
        plan: crate::model::ObservationPlan,
    ) -> std::result::Result<crate::model::MetadataObservations, crate::storage::StorageRoleFailure>
    {
        assert!(
            self.timestamp.is_none() || plan.timestamps() == crate::model::ObservationMode::Omit,
            "inline timestamps must avoid metadata I/O"
        );
        let timestamps = if plan.timestamps() == crate::model::ObservationMode::Omit {
            crate::model::MetadataObservation::NotRequested
        } else {
            crate::model::MetadataObservation::NotApplicable
        };
        crate::model::MetadataObservations::new(
            if plan.acl() == crate::model::ObservationMode::Omit {
                crate::model::MetadataObservation::NotRequested
            } else {
                self.acl.clone()
            },
            if plan.xattrs() == crate::model::ObservationMode::Omit {
                crate::model::MetadataObservation::NotRequested
            } else {
                self.xattrs.clone()
            },
            crate::model::MetadataObservation::NotRequested,
            crate::model::MetadataObservation::NotRequested,
            timestamps,
        )
        .map_err(|error| panic!("{error}"))
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: crate::storage::MetadataMutation,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<(), crate::storage::StorageRoleFailure> {
        unreachable!("integrity never applies metadata")
    }
}

fn timeless_storage(name: &str, payload: &'static [u8]) -> Result<Storage> {
    described_storage(name, payload, None)
}

fn described_storage(
    name: &str,
    payload: &'static [u8],
    timestamp: Option<StorageTimestamp>,
) -> Result<Storage> {
    described_storage_with_limits(name, payload, timestamp, None)
}

fn described_storage_with_limits(
    name: &str,
    payload: &'static [u8],
    timestamp: Option<StorageTimestamp>,
    read_limits: Option<(usize, usize)>,
) -> Result<Storage> {
    described_storage_with_acl(
        name,
        payload,
        timestamp,
        read_limits,
        crate::model::MetadataObservation::Unsupported,
    )
}

fn described_storage_with_acl(
    name: &str,
    payload: &'static [u8],
    timestamp: Option<StorageTimestamp>,
    read_limits: Option<(usize, usize)>,
    acl: crate::model::MetadataObservation<crate::model::AclMetadata>,
) -> Result<Storage> {
    described_storage_with_optional(
        name,
        payload,
        timestamp,
        read_limits,
        acl,
        crate::model::MetadataObservation::Unsupported,
        BackendKind::S3,
    )
}

fn described_storage_with_optional(
    name: &str,
    payload: &'static [u8],
    timestamp: Option<StorageTimestamp>,
    read_limits: Option<(usize, usize)>,
    acl: crate::model::MetadataObservation<crate::model::AclMetadata>,
    xattrs: crate::model::MetadataObservation<Vec<crate::model::ExtendedAttribute>>,
    kind: BackendKind,
) -> Result<Storage> {
    let identity = BackendIdentity::new(kind, name)?;
    let backend = std::sync::Arc::new(TimelessBackend {
        identity: identity.clone(),
        payload: bytes::Bytes::from_static(payload),
        timestamp,
        read_limits,
        acl,
        xattrs,
    });
    let supported = crate::storage::CapabilityAvailability::Supported;
    let unsupported = crate::storage::CapabilityAvailability::Unsupported(
        crate::storage::UnsupportedReason::new("not part of this fixture")?,
    );
    Ok(Storage::connected(
        identity,
        crate::storage::BackendCapabilities::new(
            supported.clone(),
            unsupported.clone(),
            unsupported,
            supported,
        ),
        Some(backend.clone()),
        None,
        None,
        Some(backend),
        None,
    )?)
}

#[tokio::test]
async fn native_acl_difference_is_detected_and_unmappable_acls_are_reported_ignored() -> Result {
    use crate::model::{AclEncoding, AclMetadata, MetadataObservation, MetadataProvenance};

    let value = |encoding, bytes| {
        Ok::<_, Box<dyn std::error::Error>>(MetadataObservation::Value {
            value: AclMetadata::new(encoding, bytes)?,
            provenance: MetadataProvenance::AdditionalCall,
        })
    };
    let request = |source_acl, destination_acl| -> Result<IntegrityRequest> {
        Ok(IntegrityRequest {
            source: described_storage_with_acl("acl-source", b"same", None, None, source_acl)?,
            source_path: StoragePath::new("object.bin")?,
            destination: described_storage_with_acl(
                "acl-destination",
                b"same",
                None,
                None,
                destination_acl,
            )?,
            destination_path: StoragePath::new("object.bin")?,
            plan: IntegrityPlan {
                acl: true,
                ..IntegrityPlan::default()
            },
            options: IntegrityOptions {
                mode: IntegrityMode::Content,
                ..IntegrityOptions::default()
            },
            cancel: tokio_util::sync::CancellationToken::new(),
        })
    };

    let equal = compare(request(
        value(AclEncoding::NfsV4, vec![1, 2])?,
        value(AclEncoding::NfsV4, vec![1, 2])?,
    )?)
    .await?;
    assert!(equal.matches());
    assert!(
        !equal
            .ignored()
            .iter()
            .any(|item| matches!(item, IntegrityIgnored::Acl { .. }))
    );

    let different = compare(request(
        value(AclEncoding::NfsV4, vec![1, 2])?,
        value(AclEncoding::NfsV4, vec![1, 3])?,
    )?)
    .await?;
    assert!(!different.matches());
    assert!(different.differences().contains(&IntegrityDifference::Acl {
        encoding: AclEncoding::NfsV4,
    }));

    let incompatible = compare(request(
        value(AclEncoding::NfsV4, vec![1, 2])?,
        value(AclEncoding::Posix, vec![9])?,
    )?)
    .await?;
    assert!(incompatible.matches());
    assert!(incompatible.ignored().contains(&IntegrityIgnored::Acl {
        source: Some(AclEncoding::NfsV4),
        destination: Some(AclEncoding::Posix),
    }));

    let unavailable = compare(request(
        value(AclEncoding::NfsV4, vec![1, 2])?,
        MetadataObservation::Unsupported,
    )?)
    .await?;
    assert!(unavailable.matches());
    assert!(unavailable.ignored().contains(&IntegrityIgnored::Acl {
        source: Some(AclEncoding::NfsV4),
        destination: None,
    }));

    let Err(failed) = compare(request(
        MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            transience: crate::model::Transience::Permanent,
        },
        value(AclEncoding::NfsV4, vec![1, 2])?,
    )?)
    .await
    else {
        panic!("a failed ACL read must not be silently ignored");
    };
    assert_eq!(failed.side, IntegritySide::Source);
    assert!(matches!(
        failed.failure,
        crate::storage::StorageRoleFailure::Entry(ref error)
            if error.class() == FailureClass::PermissionDenied
    ));
    Ok(())
}

#[tokio::test]
async fn content_check_uses_each_backend_read_limits() -> Result {
    let request = IntegrityRequest {
        source: described_storage_with_limits("small-read", b"payload", None, Some((3, 2)))?,
        source_path: StoragePath::new("object.bin")?,
        destination: described_storage_with_limits("wide-read", b"payload", None, Some((7, 5)))?,
        destination_path: StoragePath::new("object.bin")?,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    assert!(compare(request).await?.matches());
    Ok(())
}

#[tokio::test]
async fn a_backend_that_cannot_report_mtime_is_not_a_difference() -> Result {
    // Both sides timeless: nothing to compare, and no fabricated Modified difference.
    let request = IntegrityRequest {
        source: timeless_storage("integrity-timeless-source", b"payload")?,
        source_path: StoragePath::new("object.bin")?,
        destination: timeless_storage("integrity-timeless-destination", b"payload")?,
        destination_path: StoragePath::new("object.bin")?,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    let report = compare(request).await?;
    assert!(report.matches(), "unexpected {:?}", report.differences());

    // Mixed pair: a local source with a real mtime against a timeless destination must still
    // match, because one side simply cannot report the fact.
    let fixture = Fixture::new(Some(b"payload"), None)?;
    let mixed = IntegrityRequest {
        source: storage(fixture.source_root.path())?,
        source_path: StoragePath::new("object.bin")?,
        destination: timeless_storage("integrity-timeless-only", b"payload")?,
        destination_path: StoragePath::new("object.bin")?,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    let report = compare(mixed).await?;
    assert!(report.matches(), "unexpected {:?}", report.differences());
    Ok(())
}

/// An S3 destination reports its `Last-Modified`, but that is when the object was written: a copy
/// never carries the source's time there (the destination declares it `NotStored`). Comparing it
/// would flag every object ever copied to S3.
#[tokio::test]
async fn an_object_stores_upload_time_so_its_mtime_is_not_compared() -> Result {
    use crate::storage::backends::s3::tests::MemoryS3;

    let fixture = Fixture::new(Some(b"payload"), None)?;
    let protocol = std::sync::Arc::new(MemoryS3::default());
    protocol.objects.lock().await.insert(
        "object.bin".to_owned(),
        bytes::Bytes::from_static(b"payload"),
    );
    *protocol.last_modified.lock().await = Some(StorageTimestamp::new(
        1_800_000_000_000_000_000,
        TimePrecision::Seconds,
    )?);
    let request = IntegrityRequest {
        source: storage(fixture.source_root.path())?,
        source_path: StoragePath::new("object.bin")?,
        destination: crate::storage::backends::s3::connect(
            protocol.clone(),
            BackendIdentity::new(BackendKind::S3, "integrity-s3-destination")?,
            None,
        )?,
        destination_path: StoragePath::new("object.bin")?,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Metadata,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    let report = compare(request).await?;
    assert!(report.matches(), "unexpected {:?}", report.differences());
    for fact in [
        IntegrityIgnored::Owner,
        IntegrityIgnored::Group,
        IntegrityIgnored::Mode,
    ] {
        assert!(report.ignored().contains(&fact));
    }
    assert_eq!(
        protocol.head_versions.lock().await.len(),
        1,
        "unsupported permission facts need no extra HEAD"
    );
    Ok(())
}

#[tokio::test]
async fn timestamp_precision_and_default_tolerance_use_inline_facts_without_metadata_io() -> Result
{
    let request = IntegrityRequest {
        source: described_storage(
            "precise",
            b"x",
            Some(StorageTimestamp::new(
                1_000_000_100,
                TimePrecision::Nanoseconds,
            )?),
        )?,
        source_path: StoragePath::new("object")?,
        destination: described_storage(
            "coarse",
            b"x",
            Some(StorageTimestamp::new(
                1_000_000_000,
                TimePrecision::Seconds,
            )?),
        )?,
        destination_path: StoragePath::new("object")?,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    assert!(compare(request.clone()).await?.matches());
    let mut exact = request;
    exact.options.mtime_precision = super::MtimePrecision::Exact;
    assert_eq!(exact.options.mtime_tolerance, std::time::Duration::ZERO);
    assert!(matches!(
        compare(exact.clone()).await?.differences(),
        [IntegrityDifference::Modified { .. }]
    ));
    exact.options.mtime_tolerance = std::time::Duration::from_nanos(99);
    assert!(matches!(
        compare(exact.clone()).await?.differences(),
        [IntegrityDifference::Modified { .. }]
    ));
    exact.options.mtime_tolerance = std::time::Duration::from_nanos(100);
    assert!(compare(exact).await?.matches());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn permission_comparison_detects_a_mode_difference_with_equal_bytes_and_times() -> Result {
    use std::os::unix::fs::PermissionsExt as _;
    let fixture = Fixture::new(Some(b"payload"), Some(b"payload"))?;
    std::fs::set_permissions(
        fixture.source_root.path().join("object.bin"),
        std::fs::Permissions::from_mode(0o640),
    )?;
    std::fs::set_permissions(
        fixture.destination_root.path().join("object.bin"),
        std::fs::Permissions::from_mode(0o600),
    )?;
    let request = fixture.request(IntegrityMode::Content)?;
    let report = compare(request).await?;
    assert!(
        matches!(report.differences(), [IntegrityDifference::Permissions { source, destination }] if source.mode == Some(0o640) && destination.mode == Some(0o600))
    );
    Ok(())
}

#[path = "plan_tests.rs"]
mod plan_tests;

#[path = "strict_acl_tests.rs"]
mod strict_acl_tests;

#[path = "observed_time_tests.rs"]
mod observed_time_tests;
