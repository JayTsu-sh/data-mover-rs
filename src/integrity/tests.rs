use std::num::NonZeroUsize;
use std::path::Path;

use super::{
    IntegrityDifference, IntegrityMode, IntegrityOptions, IntegrityRequest, IntegritySide, compare,
    same_instant,
};
use crate::model::{BackendIdentity, BackendKind, StoragePath, StorageTimestamp, TimePrecision};
use crate::storage::Storage;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Fixed instant both fixtures are pinned to, so comparisons test content rather than clocks.
const FIXED_MTIME: filetime::FileTime = filetime::FileTime::from_unix_time(1_700_000_000, 0);

fn storage(root: &Path) -> Result<Storage> {
    let depth = NonZeroUsize::new(2).ok_or("constant is nonzero")?;
    crate::storage::backends::local::connect_transfer(root, depth, depth)
}

fn write(root: &Path, name: &str, payload: &[u8]) -> Result {
    let path = root.join(name);
    std::fs::write(&path, payload)?;
    filetime::set_file_mtime(&path, FIXED_MTIME)?;
    Ok(())
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
}

#[async_trait::async_trait]
impl crate::storage::ReadSource for TimelessBackend {
    async fn describe(
        &self,
        path: &StoragePath,
    ) -> std::result::Result<crate::storage::SourceDescriptor, crate::storage::StorageRoleFailure>
    {
        Ok(crate::storage::SourceDescriptor::new(
            path.clone(),
            crate::model::EntryKind::File,
            Some(self.payload.len() as u64),
            crate::model::SourceIdentity::new(
                self.identity.clone(),
                crate::model::IdentityStrength::PathScoped,
                path.as_str().as_bytes(),
            )
            .unwrap_or_else(|error| panic!("{error}")),
        ))
    }

    async fn read(
        &self,
        _request: crate::storage::ReadRequest,
    ) -> std::result::Result<crate::storage::ByteStream, crate::storage::StorageRoleFailure> {
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
        let timestamps = if plan.timestamps() == crate::model::ObservationMode::Omit {
            crate::model::MetadataObservation::NotRequested
        } else {
            crate::model::MetadataObservation::NotApplicable
        };
        crate::model::MetadataObservations::new(
            crate::model::MetadataObservation::NotRequested,
            crate::model::MetadataObservation::NotRequested,
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
    let identity = BackendIdentity::new(BackendKind::S3, name)?;
    let backend = std::sync::Arc::new(TimelessBackend {
        identity: identity.clone(),
        payload: bytes::Bytes::from_static(payload),
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
async fn a_backend_that_cannot_report_mtime_is_not_a_difference() -> Result {
    // Both sides timeless: nothing to compare, and no fabricated Modified difference.
    let request = IntegrityRequest {
        source: timeless_storage("integrity-timeless-source", b"payload")?,
        source_path: StoragePath::new("object.bin")?,
        destination: timeless_storage("integrity-timeless-destination", b"payload")?,
        destination_path: StoragePath::new("object.bin")?,
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
            protocol,
            BackendIdentity::new(BackendKind::S3, "integrity-s3-destination")?,
            None,
        )?,
        destination_path: StoragePath::new("object.bin")?,
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    let report = compare(request).await?;
    assert!(report.matches(), "unexpected {:?}", report.differences());
    Ok(())
}
