//! Directory objects in transfers (P7 F5, D2): between buckets they are copied to the same key,
//! by one request whatever their size (D13) — and refused at `Preflight`, before any destination
//! I/O, when one request cannot carry them; a filesystem side refuses them at `Preflight` before
//! any I/O; and `directory_object_action` tells a consumer what to do with each listed entry.

use std::error::Error;
use std::fs;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{
    EntryKind, FailureClass, HistoryOrder, IdentityStrength, ObjectHistory, ObservedEntry,
    RecordKind, SourceIdentity, StoragePath, VersionRecord,
};
use crate::storage::artifacts::{ArtifactKind, artifact_name};
use crate::storage::backends::local::{test_destination_storage, test_source_storage};
use crate::storage::backends::s3::tests::{
    MemoryS3, Versioning, content_md5, endpoint_of, identity, native_context, source_storage,
};
use crate::storage::backends::s3::{S3Protocol as _, connect};
use crate::storage::listing::testing::listed;
use crate::storage::{NamespaceCapabilities, SourceDescriptor, Storage, StorageRoleFailure};
use crate::transfer::{
    DIRECTORY_OBJECT_SKIP_REASON, DIRECTORY_OBJECT_TOO_LARGE_REASON, DirectoryObjectAction,
    DirectoryObjectDestination, ExpertDestinationRequest, ExpertDestinationSession,
    ExpertSourceRequest, ExpertSourceSession, TransferFailure, TransferPhase, TransferPolicy,
    TransferRequest, TransferRoute, TransferSide, directory_object_action, transfer,
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
        .put_object(key, bytes.clone().into(), &content_md5(&bytes))
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

fn request(
    source: Storage,
    source_path: &str,
    destination: Storage,
) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        source,
        StoragePath::new(source_path)?,
        destination,
        CancellationToken::new(),
    ))
}

/// The diagnostic of a failed transfer's entry failure.
fn diagnostic(failure: &TransferFailure) -> Option<String> {
    let role = Error::source(failure).and_then(|cause| cause.downcast_ref::<StorageRoleFailure>());
    match role {
        Some(StorageRoleFailure::Entry(entry)) => Some(entry.diagnostic().to_string()),
        _ => None,
    }
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
    for (body, native) in [
        (&b"hello"[..], false),
        (&b"hello"[..], true),
        (&b""[..], false),
    ] {
        let protocol = Arc::new(MemoryS3::default());
        let bytes = Bytes::copy_from_slice(body);
        protocol
            .objects
            .lock()
            .await
            .insert("src/d/".into(), bytes.clone());
        let destination = connect(
            protocol.clone(),
            endpoint_of(&protocol),
            native.then(native_context),
        )?;
        let copy = request(source_storage(&protocol, "src")?, "d/", destination)?;
        transfer(copy).await?;
        assert_eq!(
            protocol.objects.lock().await.get("d/"),
            Some(&bytes),
            "native={native} {} B",
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
    let Err(failure) = transfer(request(bucket(&protocol)?, "d/", local)?).await else {
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
    let Err(failure) = transfer(request(local_source, "x/", bucket(&protocol)?)?).await else {
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
    let observation = entries(&bucket(&protocol)?, "d")
        .await?
        .into_iter()
        .find(ObservedEntry::is_directory_object)
        .ok_or("d/ is listed")?;
    let opened = ExpertSourceSession::open(ExpertSourceRequest::new(
        local,
        observation,
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
    let described = entries(&storage, "d").await?;
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
    .with_history(descriptor.listing.history.clone())
    .unwrap_or_else(|error| panic!("{error}")))
}

/// Every listed child of `directory` as the traversal observes it.
async fn entries(storage: &Storage, directory: &str) -> TestResult<Vec<ObservedEntry>> {
    let namespace = storage.namespace()?;
    let directory = StoragePath::new(directory)?;
    let (descriptors, _) = listed(&*namespace, &directory).await?;
    descriptors.iter().map(observation).collect()
}

/// D2: raw namespace folder markers remain nothing to do on a filesystem. Any content in a
/// key's history skips it, even when deleted now; other entries and bucket destinations copy.
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
    let object_store = storage.namespace()?.capabilities();
    assert!(object_store.directory_objects && storage.holds_directory_objects());
    let mut decided = Vec::new();
    for directory in ["d", "e", "m"] {
        for entry in entries(&storage, directory).await? {
            // The bucket's `Storage` knows its 5 GiB limit too: none of these is near it.
            assert_eq!(
                directory_object_action(&entry, &storage),
                directory_object_action(&entry, &object_store)
            );
            decided.push((
                entry.path().to_string(),
                entry
                    .history()
                    .is_some_and(|history| history.current().is_none()),
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
            // The key `m/`: a content version, then its latest record, a delete marker.
            ("m/".to_string(), true, skip, copy),
        ]
    );
    Ok(())
}

/// 12 MiB: over the 8 MiB single-PUT threshold.
const OVER_THRESHOLD: usize = 12 * 1024 * 1024;
/// What one `PutObject` or `CopyObject` carries: 5 GiB.
const ONE_REQUEST: u64 = 5 * 1024 * 1024 * 1024;
/// One more byte than one `PutObject` or `CopyObject` carries.
const OVER_ONE_REQUEST: u64 = ONE_REQUEST + 1;

fn observed(path: &str, size: Option<u64>) -> TestResult<ObservedEntry> {
    let identity = SourceIdentity::new(identity(), IdentityStrength::PathScoped, path.as_bytes())?;
    Ok(ObservedEntry::new(
        StoragePath::new(path)?,
        EntryKind::File,
        size,
        None,
        identity,
    )?)
}

/// D13 up front: to a bucket (given its `Storage`), a directory object one request cannot carry —
/// over 5 GiB, or of unknown size — is skipped with the reason the engine's `Preflight` refusal
/// gives; 5 GiB exactly and any other key are copied. The namespace's capabilities alone know no
/// limit; to a filesystem the answers are unchanged.
#[tokio::test]
async fn directory_object_action_answers_too_large_up_front() -> TestResult {
    let storage = bucket(&Arc::new(MemoryS3::default()))?;
    let capabilities = storage.namespace()?.capabilities();
    let root = tempfile::tempdir()?;
    let local = test_destination_storage(root.path(), "directory-object-action")?;
    assert_eq!(
        DirectoryObjectDestination::from(&storage),
        DirectoryObjectDestination::new(true, Some(ONE_REQUEST))
    );
    assert_eq!(
        DirectoryObjectDestination::from(&local),
        DirectoryObjectDestination::new(false, None)
    );
    let copy = DirectoryObjectAction::Copy;
    let nothing = DirectoryObjectAction::NothingToDo;
    let too_large = DirectoryObjectAction::Skip(DIRECTORY_OBJECT_TOO_LARGE_REASON);
    let skip = DirectoryObjectAction::Skip(DIRECTORY_OBJECT_SKIP_REASON);
    // A store that holds no directory objects but reports a limit (an ONTAP NAS bucket's S3
    // destination): it cannot hold the object at all, whatever its size.
    let nas = DirectoryObjectDestination::new(false, Some(ONE_REQUEST));
    for (path, size, to_bucket, to_filesystem) in [
        ("d/", Some(0), copy, nothing),
        ("d/", Some(ONE_REQUEST), copy, skip),
        ("d/", Some(OVER_ONE_REQUEST), too_large, skip),
        ("d/", None, too_large, nothing),
        ("d", Some(OVER_ONE_REQUEST), copy, copy),
    ] {
        let entry = observed(path, size)?;
        let decided = (
            directory_object_action(&entry, &storage),
            directory_object_action(&entry, &local),
            directory_object_action(&entry, &capabilities),
        );
        assert_eq!(decided, (to_bucket, to_filesystem, copy), "{path} {size:?}");
        assert_eq!(directory_object_action(&entry, nas), to_filesystem);
    }
    Ok(())
}

/// A version too large skips the whole history to a bucket; any content skips it to a filesystem.
#[tokio::test]
async fn directory_object_action_checks_history_size_on_every_segment() -> TestResult {
    let storage = bucket(&Arc::new(MemoryS3::default()))?;
    let root = tempfile::tempdir()?;
    let local = test_destination_storage(root.path(), "directory-object-history-size")?;
    let copy = DirectoryObjectAction::Copy;
    let too_large = DirectoryObjectAction::Skip(DIRECTORY_OBJECT_TOO_LARGE_REASON);
    let skip = DirectoryObjectAction::Skip(DIRECTORY_OBJECT_SKIP_REASON);
    for (sizes, to_bucket, to_filesystem) in [
        (vec![Some(OVER_ONE_REQUEST), Some(1)], too_large, skip),
        (vec![Some(1), Some(OVER_ONE_REQUEST)], too_large, skip),
        (vec![Some(OVER_ONE_REQUEST), None], too_large, skip),
        (vec![Some(5), Some(0)], copy, skip),
        (vec![Some(5), None], copy, skip),
        (vec![None, Some(3)], copy, skip),
    ] {
        let entry = with_history("d/", &sizes)?;
        assert_eq!(
            (
                directory_object_action(&entry, &storage),
                directory_object_action(&entry, &local)
            ),
            (to_bucket, to_filesystem),
            "{sizes:?}"
        );
        // Segmented (P7 Q8), every segment of the key is decided alike to a bucket: a replay never
        // writes the part before a version too large to write.
        for segment in entry.clone().into_segments(1) {
            assert_eq!(
                (
                    directory_object_action(&segment, &storage),
                    directory_object_action(&segment, &local)
                ),
                (to_bucket, to_filesystem),
                "{sizes:?}"
            );
        }
    }
    Ok(())
}

/// Empty/deleted latest versions and segments without content still belong to a content key.
#[test]
fn directory_object_action_skips_whole_content_history_to_unsupported_destinations() -> TestResult {
    let storage = bucket(&Arc::new(MemoryS3::default()))?;
    let nas = bucket(&Arc::new(MemoryS3::nas_bucket()))?;
    let root = tempfile::tempdir()?;
    let local = test_destination_storage(root.path(), "directory-object-history")?;
    assert!(!nas.holds_directory_objects());
    let skip = DirectoryObjectAction::Skip(DIRECTORY_OBJECT_SKIP_REASON);
    for sizes in [
        vec![Some(7), Some(0)],
        vec![Some(0), Some(7)],
        vec![Some(7), None],
        vec![Some(7), Some(0), None],
    ] {
        let entry = with_history("d/", &sizes)?;
        for segment in std::iter::once(entry.clone()).chain(entry.into_segments(1)) {
            for (destination, capabilities) in [
                (&local, NamespaceCapabilities::BASIC),
                (&nas, nas.namespace()?.capabilities()),
            ] {
                assert_eq!(
                    directory_object_action(&segment, destination),
                    skip,
                    "{sizes:?}"
                );
                assert_eq!(
                    directory_object_action(&segment, &capabilities),
                    skip,
                    "{sizes:?}"
                );
            }
            for destination in [
                DirectoryObjectDestination::from(&storage),
                DirectoryObjectDestination::from(&storage.namespace()?.capabilities()),
            ] {
                assert_eq!(
                    directory_object_action(&segment, destination),
                    DirectoryObjectAction::Copy,
                    "{sizes:?}"
                );
            }
        }
    }
    Ok(())
}

/// Compatibility for raw namespace callers: zero-only and delete-only histories stay no-ops.
#[test]
fn directory_object_action_keeps_raw_marker_fallback() -> TestResult {
    let unsupported = DirectoryObjectDestination::new(false, Some(ONE_REQUEST));
    let supported = DirectoryObjectDestination::new(true, Some(ONE_REQUEST));
    for sizes in [
        vec![Some(0), Some(0)],
        vec![Some(0), None],
        vec![None, Some(0)],
        vec![None, None],
    ] {
        let entry = with_history("d/", &sizes)?;
        for segment in std::iter::once(entry.clone()).chain(entry.into_segments(1)) {
            assert_eq!(
                directory_object_action(&segment, unsupported),
                DirectoryObjectAction::NothingToDo,
                "{sizes:?}"
            );
            assert_eq!(
                directory_object_action(&segment, supported),
                DirectoryObjectAction::Copy
            );
        }
    }
    let directory = ObservedEntry::new(
        StoragePath::new("d")?,
        EntryKind::Directory,
        None,
        None,
        SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"d")?,
    )?;
    assert_eq!(
        directory_object_action(&directory, unsupported),
        DirectoryObjectAction::Copy
    );
    Ok(())
}

/// Marker directories create a destination directory only from the live final segment.
#[test]
fn marker_directory_history_decides_creation_per_destination() -> TestResult {
    let root = tempfile::tempdir()?;
    let local = test_destination_storage(root.path(), "marker-directory-history")?;
    let nas = bucket(&Arc::new(MemoryS3::nas_bucket()))?;
    let storage = bucket(&Arc::new(MemoryS3::default()))?;
    let copy = DirectoryObjectAction::Copy;
    let nothing = DirectoryObjectAction::NothingToDo;
    for (sizes, current_action) in [
        (vec![Some(0), Some(0)], copy),
        (vec![None, Some(0)], copy),
        (vec![Some(0), None], nothing),
        (vec![None, None], nothing),
        (vec![Some(0), Some(0), None], nothing),
    ] {
        let directory = directory_with_history(&sizes)?;
        let last = sizes.len() - 1;
        let entries = std::iter::once((last, directory.clone()))
            .chain(directory.into_segments(1).into_iter().enumerate());
        for (index, entry) in entries {
            let expected = if index == last {
                current_action
            } else {
                nothing
            };
            for destination in [&local, &nas] {
                assert_eq!(
                    directory_object_action(&entry, destination),
                    expected,
                    "{sizes:?}, segment {index}"
                );
            }
            assert_eq!(directory_object_action(&entry, &storage), copy);
        }
    }
    // Skipping the deleted parent marker does not suppress an independently traversed child.
    let child = observed("d/child", Some(7))?;
    for destination in [&local, &nas, &storage] {
        assert_eq!(directory_object_action(&child, destination), copy);
    }
    Ok(())
}

fn directory_with_history(sizes: &[Option<u64>]) -> TestResult<ObservedEntry> {
    let raw = with_history("d/", sizes)?;
    Ok(ObservedEntry::new(
        StoragePath::new("d")?,
        EntryKind::Directory,
        None,
        None,
        SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"d")?,
    )?
    .with_history(raw.history().cloned().map(Box::new))
    .unwrap_or_else(|error| panic!("{error}")))
}

/// `path` with a history of one record per size, oldest first: a version of that size, or a
/// delete marker (`None`); the entry-level size is the current version's.
fn with_history(path: &str, sizes: &[Option<u64>]) -> TestResult<ObservedEntry> {
    let mut records = Vec::new();
    for (index, size) in sizes.iter().enumerate() {
        let id = format!("v{index}");
        let kind = size.map_or(RecordKind::DeleteMarker { modified: None }, |size| {
            RecordKind::Object {
                size,
                etag: "\"e\"".into(),
                modified: None,
            }
        });
        let identity = SourceIdentity::new(identity(), IdentityStrength::VersionScoped, &id)?;
        records.push(VersionRecord::new(
            Some(id),
            index + 1 == sizes.len(),
            kind,
            identity,
            None,
        )?);
    }
    let history = ObjectHistory::whole(records, HistoryOrder::Server)?;
    let size = history.current().and_then(VersionRecord::size);
    Ok(observed(path, size)?
        .with_history(Some(Box::new(history)))
        .unwrap_or_else(|error| panic!("{error}")))
}

/// A store that completes a multipart upload to `x/` as `x` (`RustFS` 1.0.0), holding the real
/// file `dst/d` beside the directory object `dst/d/` a copy writes.
async fn rustfs_like() -> Arc<MemoryS3> {
    let protocol = Arc::new(MemoryS3::default());
    *protocol.rustfs_multipart_directory_objects.lock().await = true;
    protocol
        .objects
        .lock()
        .await
        .insert("d".into(), Bytes::from_static(b"a real file"));
    protocol
}

/// D13: a 12 MiB `d/` is copied by one request under every policy — no multipart request is
/// made, and the real file `d` beside it is untouched. (The native row is one `CopyObject` at this
/// size anyway; the staged tests cover a native `d/` over the single-copy limit.)
#[tokio::test]
async fn a_directory_object_over_the_single_put_threshold_is_one_request() -> TestResult {
    let bytes = Bytes::from(
        (0..=250_u8)
            .cycle()
            .take(OVER_THRESHOLD)
            .collect::<Vec<_>>(),
    );
    for (policy, native, route) in [
        (
            TransferPolicy::Checkpointed,
            false,
            TransferRoute::Streaming,
        ),
        (
            TransferPolicy::AtomicReplace,
            false,
            TransferRoute::Streaming,
        ),
        (TransferPolicy::Direct, true, TransferRoute::Streaming),
        (TransferPolicy::Checkpointed, true, TransferRoute::Native),
    ] {
        let label = format!("{policy:?} {route:?}");
        let protocol = rustfs_like().await;
        protocol
            .objects
            .lock()
            .await
            .insert("src/d/".into(), bytes.clone());
        let destination = connect(
            protocol.clone(),
            endpoint_of(&protocol),
            native.then(native_context),
        )?;
        let copy = request(source_storage(&protocol, "src")?, "d/", destination)?
            .with_transfer_policy(policy);
        let outcome = transfer(copy)
            .await
            .map_err(|e| format!("{label}: {e:?}"))?;
        assert_eq!(outcome.route, route, "{label}");
        let objects = protocol.objects.lock().await.clone();
        assert_eq!(objects.get("d/"), Some(&bytes), "{label}");
        assert_eq!(
            objects.get("d").map(Bytes::as_ref),
            Some(&b"a real file"[..]),
            "{label}"
        );
        let multipart = (
            *protocol.multipart_begins.lock().await,
            *protocol.part_uploads.lock().await,
            *protocol.part_copies.lock().await,
        );
        assert_eq!(multipart, (0, 0, 0), "{label}");
    }
    Ok(())
}

/// D13: more than one request carries is the destination's `Unsupported` at `Preflight`, before
/// any destination I/O — a leftover pointer of `d/` is still there, nothing is written, no
/// multipart request is made. The source is a sparse local file (only described); the expert
/// destination half does the same from the observed size.
#[tokio::test]
async fn a_directory_object_one_request_cannot_carry_is_refused_at_preflight() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let pointer = artifact_name("d/", ArtifactKind::Upload);
    put(&protocol, &pointer, b"stale pointer").await?;
    put(&protocol, "src/d/", b"x").await?;
    *protocol.declared_size.lock().await = Some(OVER_ONE_REQUEST);
    let Err(failure) = transfer(request(
        source_storage(&protocol, "src")?,
        "d/",
        bucket(&protocol)?,
    )?)
    .await
    else {
        return Err("a directory object over 5 GiB was written".into());
    };
    assert_eq!(
        refusal(&failure),
        refused_at_preflight(TransferSide::Destination)
    );
    // The reason a consumer learns up front from `directory_object_action`.
    assert_eq!(
        diagnostic(&failure).as_deref(),
        Some(DIRECTORY_OBJECT_TOO_LARGE_REASON)
    );
    let mut keys: Vec<String> = protocol.objects.lock().await.keys().cloned().collect();
    keys.sort();
    assert_eq!(keys, [pointer.as_str(), "src/d/"]);
    assert_eq!(
        *protocol.puts.lock().await,
        2,
        "the pointer and source fixture"
    );
    assert_eq!(*protocol.multipart_begins.lock().await, 0);

    let observation = ObservedEntry::new(
        StoragePath::new("src/big/")?,
        EntryKind::File,
        Some(OVER_ONE_REQUEST),
        None,
        SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"big")?,
    )?;
    assert_eq!(
        directory_object_action(&observation, &bucket(&protocol)?),
        DirectoryObjectAction::Skip(DIRECTORY_OBJECT_TOO_LARGE_REASON)
    );
    let prepared = ExpertDestinationSession::prepare(ExpertDestinationRequest::new(
        observation,
        1024,
        bucket(&protocol)?,
        StoragePath::new("d/")?,
        CancellationToken::new(),
    ))
    .await;
    let Err(failure) = prepared else {
        return Err("the expert destination prepared a directory object over 5 GiB".into());
    };
    assert_eq!(
        refusal(&failure),
        refused_at_preflight(TransferSide::Destination)
    );
    assert_eq!(
        diagnostic(&failure).as_deref(),
        Some(DIRECTORY_OBJECT_TOO_LARGE_REASON)
    );
    assert!(protocol.objects.lock().await.contains_key(&pointer));
    assert_eq!(*protocol.puts.lock().await, 2);
    Ok(())
}
