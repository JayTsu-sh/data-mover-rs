//! The expert halves fed an observation the way the S3 traversal makes it (ADR-0006 C22): from a
//! delimiter listing, not a describe. A listing of current objects has no version id, so on a
//! versioned bucket the observation (`PathScoped(ETag)`) and the describe (`VersionScoped(id)`)
//! differ in identity while naming the same object.

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use futures::channel::mpsc;
use tokio_util::sync::CancellationToken;

use crate::model::{EntryVersion, IdentityStrength, ObservedEntry, StoragePath};
use crate::storage::backends::s3::tests::{MemoryS3, Versioning, content_md5, identity};
use crate::storage::backends::s3::{S3Protocol as _, connect};
use crate::storage::{NamespaceRequest, PreflightPolicy, SourceDescriptor, Storage};
use crate::transfer::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceRequest, ExpertSourceSession,
    InflightLimits, TransferOutcome,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

async fn put(protocol: &MemoryS3, key: &str, body: &'static [u8]) -> TestResult {
    let bytes = Bytes::from_static(body);
    protocol
        .put_object(key, bytes.clone(), &content_md5(&bytes))
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

/// A versioned bucket whose `source` holds v1 then v2.
async fn versioned() -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "source", b"first").await?;
    put(&protocol, "source", b"second!").await?;
    Ok(protocol)
}

/// The observation the traversal emits for a listed child: its path, kind, size, time and the
/// listing's identity, with the listed version when there is one.
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

/// Every listed `source` entry, current objects only or every version.
async fn listed(storage: &Storage, all: bool) -> TestResult<Vec<ObservedEntry>> {
    let namespace = storage.namespace(&PreflightPolicy::production())?;
    let result = if all {
        namespace.list_versions(&StoragePath::root()).await?
    } else {
        namespace
            .execute(NamespaceRequest::List(StoragePath::root()))
            .await?
    };
    let (entries, _) = result.into_listing().ok_or("not a listing")?;
    entries
        .iter()
        .filter(|entry| entry.path.as_str() == "source")
        .map(observation)
        .collect()
}

/// Runs both expert halves from `observation` to `final_key` of the same bucket.
async fn copy(
    storage: &Storage,
    observation: ObservedEntry,
    final_key: &str,
) -> TestResult<TransferOutcome> {
    let limits = InflightLimits::new(2, 1024, 2)?;
    let source = ExpertSourceSession::open(ExpertSourceRequest::new(
        storage.clone(),
        observation.clone(),
        limits,
        CancellationToken::new(),
    ))
    .await?;
    let session = ExpertDestinationSession::prepare(ExpertDestinationRequest::new(
        observation,
        source.offer().maximum_chunk_bytes,
        storage.clone(),
        StoragePath::new(final_key)?,
        limits,
        CancellationToken::new(),
    ))
    .await?;
    let mut payload = source.stream_from(session.write_offset())?;
    let (sender, receiver) = mpsc::unbounded();
    let pump = async move {
        while let Some(chunk) = payload.next_chunk().await? {
            if sender.unbounded_send(Ok(chunk)).is_err() {
                break;
            }
        }
        drop(sender);
        payload.finish().await
    };
    let (written, evidence) = futures::join!(session.write(Box::pin(receiver)), pump);
    Ok(written?.complete(evidence?).await?)
}

async fn open_error(storage: &Storage, observation: ObservedEntry) -> String {
    let limits = InflightLimits::new(2, 1024, 2).unwrap_or_else(|e| panic!("{e}"));
    match ExpertSourceSession::open(ExpertSourceRequest::new(
        storage.clone(),
        observation,
        limits,
        CancellationToken::new(),
    ))
    .await
    {
        Ok(_) => panic!("the expert source half accepted the observation"),
        Err(error) => error.to_string(),
    }
}

/// A current object listed from a versioned bucket is identified by its `ETag`, the describe by
/// its version; the unchanged object is still copied, and its current bytes arrive.
#[tokio::test]
async fn a_listed_current_object_of_a_versioned_bucket_is_copied() -> TestResult {
    let protocol = versioned().await?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let observed = listed(&storage, false).await?;
    assert_eq!(observed.len(), 1);
    assert_eq!(
        observed[0].source_identity().strength(),
        IdentityStrength::PathScoped
    );
    copy(&storage, observed[0].clone(), "copied").await?;
    let copied = protocol.objects.lock().await.get("copied").cloned();
    assert_eq!(copied.as_deref(), Some(&b"second!"[..]));
    Ok(())
}

/// An object replaced after it was listed is not what the observation advertised: refused, not
/// copied under the old observation. A new version with the same bytes is the same content and
/// may be copied.
#[tokio::test]
async fn an_object_changed_after_listing_is_refused() -> TestResult {
    let protocol = versioned().await?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let observed = listed(&storage, false).await?.remove(0);
    put(&protocol, "source", b"changed").await?;
    let error = open_error(&storage, observed.clone()).await;
    assert!(error.contains("source differs"), "{error}");
    put(&protocol, "source", b"second!").await?;
    copy(&storage, observed, "same-bytes").await?;
    Ok(())
}

/// From a listing of every version, the latest version is the current object and is copied; an
/// older one and a delete marker are refused with the reason, not as a changed source.
#[tokio::test]
async fn only_the_latest_listed_version_is_an_expert_source() -> TestResult {
    let protocol = versioned().await?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let versions = listed(&storage, true).await?;
    assert_eq!(versions.len(), 2);
    let error = open_error(&storage, versions[0].clone()).await;
    assert!(error.contains("older version"), "{error}");
    copy(&storage, versions[1].clone(), "latest").await?;
    protocol
        .delete_object("source")
        .await
        .map_err(|e| format!("{e:?}"))?;
    let marker = listed(&storage, true).await?.remove(2);
    assert!(marker.version().is_some_and(EntryVersion::is_delete_marker));
    let error = open_error(&storage, marker).await;
    assert!(error.contains("delete marker"), "{error}");
    Ok(())
}
