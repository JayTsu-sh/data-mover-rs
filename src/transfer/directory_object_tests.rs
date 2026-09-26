//! Directory objects in transfers (P7 F5, D2): between buckets they are copied to the same key; a
//! filesystem side refuses them at `Preflight` before any I/O; and `directory_object_action`
//! tells a consumer what to do with each listed entry.

use std::error::Error;
use std::fs;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{EntryVersion, FailureClass, ObservedEntry, StoragePath};
use crate::storage::backends::local::{test_destination_storage, test_source_storage};
use crate::storage::backends::s3::tests::{
    MemoryS3, Versioning, content_md5, endpoint_of, native_context,
};
use crate::storage::backends::s3::{S3Protocol as _, connect};
use crate::storage::listing::testing::{listed, listed_versions};
use crate::storage::{
    NamespaceCapabilities, PreflightPolicy, SourceDescriptor, Storage, StorageRoleFailure,
};
use crate::transfer::{
    DIRECTORY_OBJECT_SKIP_REASON, DirectoryObjectAction, ExpertDestinationRequest,
    ExpertDestinationSession, ExpertSourceRequest, ExpertSourceSession, InflightLimits,
    PayloadShapingPolicy, TransferFailure, TransferPhase, TransferRequest, TransferSide,
    directory_object_action, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn bucket(protocol: &Arc<MemoryS3>) -> TestResult<Storage> {
    connect(
        protocol.clone(),
        endpoint_of(protocol),
        Some(native_context()),
    )
}

async fn put(protocol: &MemoryS3, key: &str, body: &'static [u8]) -> TestResult {
    let bytes = Bytes::from_static(body);
    protocol
        .put_object(key, bytes.clone(), &content_md5(&bytes))
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

fn request(
    source: Storage,
    source_path: &str,
    destination: Storage,
    final_path: &str,
) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        source,
        StoragePath::new(source_path)?,
        destination,
        StoragePath::new(final_path)?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        CancellationToken::new(),
    ))
}

/// (phase, side, class) of a failed transfer.
fn refusal(failure: &TransferFailure) -> (TransferPhase, TransferSide, Option<FailureClass>) {
    let role = Error::source(failure).and_then(|cause| cause.downcast_ref::<StorageRoleFailure>());
    let class = match role {
        Some(StorageRoleFailure::Entry(entry)) => Some(entry.class()),
        _ => None,
    };
    (failure.phase(), failure.side(), class)
}

/// What a side that cannot hold directory objects answers: `Unsupported` at `Preflight`.
const fn refused_at_preflight(
    side: TransferSide,
) -> (TransferPhase, TransferSide, Option<FailureClass>) {
    (
        TransferPhase::Preflight,
        side,
        Some(FailureClass::Unsupported),
    )
}

/// Between buckets the key `d/` is recreated with its bytes, content or none, streamed or native.
#[tokio::test]
async fn a_directory_object_between_buckets_is_copied() -> TestResult {
    for (body, shaping) in [
        (&b"hello"[..], PayloadShapingPolicy::RequireClientShaped),
        (&b"hello"[..], PayloadShapingPolicy::AllowUnshapedNative),
        (&b""[..], PayloadShapingPolicy::RequireClientShaped),
    ] {
        let protocol = Arc::new(MemoryS3::default());
        let bytes = Bytes::copy_from_slice(body);
        protocol
            .objects
            .lock()
            .await
            .insert("src/d/".into(), bytes.clone());
        let copy = request(bucket(&protocol)?, "src/d/", bucket(&protocol)?, "dst/d/")?
            .with_payload_shaping(shaping);
        transfer(copy).await?;
        assert_eq!(
            protocol.objects.lock().await.get("dst/d/"),
            Some(&bytes),
            "{shaping:?} {} B",
            body.len()
        );
    }
    Ok(())
}

/// To a filesystem the key `d/` is refused at `Preflight`, `Unsupported` at that path, before any
/// I/O: the destination directory stays empty. The mirror: a filesystem source path ending in `/`.
#[tokio::test]
async fn a_directory_object_to_a_filesystem_is_unsupported_before_any_io() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    put(&protocol, "d/", b"hello").await?;
    let root = tempfile::tempdir()?;
    let local = test_destination_storage(root.path(), "directory-object-destination")?;
    let Err(failure) = transfer(request(bucket(&protocol)?, "d/", local, "d/")?).await else {
        return Err("a directory object was written to a filesystem".into());
    };
    assert_eq!(
        refusal(&failure),
        refused_at_preflight(TransferSide::Destination)
    );
    assert_eq!(fs::read_dir(root.path())?.count(), 0);
    assert!(protocol.range_observations.lock().await.is_empty());

    let source_root = tempfile::tempdir()?;
    let (local_source, _) = test_source_storage(source_root.path(), "directory-object-source")?;
    let Err(failure) = transfer(request(local_source, "x/", bucket(&protocol)?, "x/")?).await
    else {
        return Err("a filesystem path ending in '/' was read".into());
    };
    assert_eq!(
        refusal(&failure),
        refused_at_preflight(TransferSide::Source)
    );
    assert!(!protocol.objects.lock().await.contains_key("x/"));
    Ok(())
}

/// The expert source half refuses a filesystem path ending in `/` before it describes anything,
/// as the ordinary entry does.
#[tokio::test]
async fn expert_source_refuses_a_directory_object_on_a_filesystem() -> TestResult {
    let root = tempfile::tempdir()?;
    let (local, _) = test_source_storage(root.path(), "expert-directory-object-source")?;
    let protocol = Arc::new(MemoryS3::default());
    put(&protocol, "d/", b"hello").await?;
    let observation = entries(&bucket(&protocol)?, "d", false)
        .await?
        .into_iter()
        .find(ObservedEntry::is_directory_object)
        .ok_or("d/ is listed")?;
    let opened = ExpertSourceSession::open(ExpertSourceRequest::new(
        local,
        observation,
        InflightLimits::new(2, 1024, 2)?,
        CancellationToken::new(),
    ))
    .await;
    let Err(failure) = opened else {
        return Err("the expert source opened a directory object on a filesystem".into());
    };
    assert_eq!(
        refusal(&failure),
        refused_at_preflight(TransferSide::Source)
    );
    Ok(())
}

/// The expert destination half refuses it too, before it prepares anything.
#[tokio::test]
async fn expert_destination_refuses_a_directory_object_on_a_filesystem() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    put(&protocol, "d/", b"hello").await?;
    let storage = bucket(&protocol)?;
    let described = entries(&storage, "d", false).await?;
    let observation = described
        .into_iter()
        .find(ObservedEntry::is_directory_object)
        .ok_or("d/ is listed")?;
    let root = tempfile::tempdir()?;
    let local = test_destination_storage(root.path(), "expert-directory-object")?;
    let prepared = ExpertDestinationSession::prepare(ExpertDestinationRequest::new(
        observation,
        1024,
        local,
        StoragePath::new("d/")?,
        InflightLimits::new(2, 1024, 2)?,
        CancellationToken::new(),
    ))
    .await;
    let Err(failure) = prepared else {
        return Err("the expert destination prepared a directory object".into());
    };
    assert_eq!(
        refusal(&failure),
        refused_at_preflight(TransferSide::Destination)
    );
    assert_eq!(fs::read_dir(root.path())?.count(), 0);
    Ok(())
}

fn observation(descriptor: &SourceDescriptor) -> TestResult<ObservedEntry> {
    Ok(ObservedEntry::new(
        descriptor.path.clone(),
        descriptor.kind,
        descriptor.size,
        None,
        descriptor.source_identity.clone(),
    )?
    .with_version(descriptor.listing.version.clone()))
}

/// Every listed child of `directory` as the traversal observes it.
async fn entries(storage: &Storage, directory: &str, all: bool) -> TestResult<Vec<ObservedEntry>> {
    let namespace = storage.namespace(&PreflightPolicy::production())?;
    let directory = StoragePath::new(directory)?;
    let (descriptors, _) = if all {
        listed_versions(&*namespace, &directory).await?
    } else {
        listed(&*namespace, &directory).await?
    };
    descriptors.iter().map(observation).collect()
}

/// D2: to a bucket every entry is `Copy`; to a filesystem a folder marker or a delete marker is
/// nothing to do, content is skipped with the reason, and any other entry is `Copy`.
#[tokio::test]
async fn directory_object_action_decides_per_destination() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "d/", b"hello").await?;
    put(&protocol, "d/x", b"1").await?;
    put(&protocol, "e/", b"").await?;
    put(&protocol, "m/", b"gone").await?;
    protocol
        .delete_object("m/")
        .await
        .map_err(|e| format!("{e:?}"))?;
    let storage = bucket(&protocol)?;
    let filesystem = NamespaceCapabilities::BASIC;
    let object_store = storage
        .namespace(&PreflightPolicy::production())?
        .capabilities();
    assert!(object_store.directory_objects && storage.holds_directory_objects());
    let mut decided = Vec::new();
    for (directory, all) in [("d", false), ("e", false), ("m", true)] {
        for entry in entries(&storage, directory, all).await? {
            decided.push((
                entry.path().to_string(),
                entry.version().is_some_and(EntryVersion::is_latest),
                directory_object_action(&entry, &filesystem),
                directory_object_action(&entry, &object_store),
            ));
        }
    }
    let copy = DirectoryObjectAction::Copy;
    let nothing = DirectoryObjectAction::NothingToDo;
    let skip = DirectoryObjectAction::Skip(DIRECTORY_OBJECT_SKIP_REASON);
    assert_eq!(
        decided,
        [
            ("d/".to_string(), false, skip, copy),
            ("d/x".to_string(), false, copy, copy),
            ("e/".to_string(), false, nothing, copy),
            // The key `m/`: its content version (not the latest), then its latest delete marker.
            ("m/".to_string(), false, skip, copy),
            ("m/".to_string(), true, nothing, copy),
        ]
    );
    Ok(())
}
