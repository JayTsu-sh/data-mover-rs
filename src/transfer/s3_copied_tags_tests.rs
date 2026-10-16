//! Ordinary migration selects object tags independently of payload route and write shape.
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::metadata::{ApplicationOutcome, MetadataFamily, RefusalCause};
use crate::model::{FailureClass, ObjectTag, StoragePath, Transience};
use crate::storage::backends::s3::tests::{MemoryS3, endpoint_of, identity, source_storage};
use crate::storage::backends::s3::{
    S3Protocol, S3ProtocolFailure, S3TagSupport, connect_native_at_destination,
    connect_with_tag_support,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const MIB: usize = 1024 * 1024;

fn request(
    protocol: &Arc<MemoryS3>,
    policy: TransferPolicy,
    shaping: PayloadShapingPolicy,
) -> Result<TransferRequest> {
    let destination = connect_native_at_destination(
        protocol.clone(),
        endpoint_of(protocol),
        64 * MIB as u64,
        (5 * MIB as u64, 5 * MIB as u64),
    )?;
    let request = TransferRequest::new(
        source_storage(protocol, "src")?,
        StoragePath::new("file")?,
        destination,
        CancellationToken::new(),
    )
    .with_transfer_policy(policy)
    .with_payload_shaping(shaping);
    Ok(request)
}

fn tag_outcome(report: &crate::metadata::MetadataApplicationReport) -> Option<ApplicationOutcome> {
    report
        .outcomes()
        .iter()
        .find(|outcome| outcome.family == MetadataFamily::Tags)
        .map(|outcome| outcome.outcome)
}

const PAYLOAD_AND_WRITE_PATHS: [(usize, TransferPolicy, PayloadShapingPolicy, TransferRoute); 6] = [
    (
        1024,
        TransferPolicy::Checkpointed,
        PayloadShapingPolicy::RequireClientShaped,
        TransferRoute::Streaming,
    ),
    (
        9 * MIB,
        TransferPolicy::Checkpointed,
        PayloadShapingPolicy::RequireClientShaped,
        TransferRoute::Streaming,
    ),
    (
        1024,
        TransferPolicy::Direct,
        PayloadShapingPolicy::RequireClientShaped,
        TransferRoute::Streaming,
    ),
    (
        9 * MIB,
        TransferPolicy::Direct,
        PayloadShapingPolicy::RequireClientShaped,
        TransferRoute::Streaming,
    ),
    (
        1024,
        TransferPolicy::Checkpointed,
        PayloadShapingPolicy::AllowUnshapedNative,
        TransferRoute::Native,
    ),
    (
        9 * MIB,
        TransferPolicy::Checkpointed,
        PayloadShapingPolicy::AllowUnshapedNative,
        TransferRoute::Native,
    ),
];

#[tokio::test]
async fn tags_follow_the_selected_version_on_every_payload_and_write_path() -> Result {
    for (size, policy, shaping, route) in PAYLOAD_AND_WRITE_PATHS {
        let protocol = Arc::new(MemoryS3::default());
        let old = Bytes::from(vec![7; size]);
        protocol.put_version("src/file", "v1", old.clone()).await;
        protocol
            .put_version("src/file", "v2", Bytes::from_static(b"new content"))
            .await;
        let old_tags = vec![ObjectTag::new("generation", "old")?];
        protocol
            .tagged_versions
            .lock()
            .await
            .insert(("src/file".into(), "v1".into()), old_tags.clone());
        protocol
            .put_tags("src/file", &[ObjectTag::new("generation", "new")?])
            .await
            .map_err(|e| format!("{e:?}"))?;
        let outcome = transfer(
            request(&protocol, policy, shaping)?
                .with_source_version(SourceVersion::Id("v1".into()))
                .with_copied_metadata(CopiedMetadataRequest::default().with_tags()),
        )
        .await?;
        assert_eq!(outcome.route, route);
        assert_eq!(outcome.final_destination, StoragePath::new("file")?);
        assert_eq!(protocol.objects.lock().await.get("file"), Some(&old));
        assert_eq!(*protocol.tag_versions.lock().await, [Some("v1".into())]);
        assert_eq!(
            tag_outcome(&outcome.metadata.ok_or("missing metadata report")?),
            Some(ApplicationOutcome::Applied)
        );
        assert_eq!(
            protocol
                .get_tags("file", outcome.destination_version.as_deref())
                .await
                .map_err(|e| format!("{e:?}"))?,
            old_tags
        );
    }
    Ok(())
}

#[tokio::test]
async fn tags_are_opt_in_and_an_unsupported_destination_does_not_read_them() -> Result {
    assert!(CopiedMetadataRequest::default().is_empty());
    assert!(!CopiedMetadataRequest::default().with_tags().is_empty());
    assert!(CopiedMetadataRequest::default().with_tags().tags());
    for requested in [false, true] {
        let protocol = Arc::new(MemoryS3::default());
        protocol
            .objects
            .lock()
            .await
            .insert("src/file".into(), Bytes::from_static(b"payload"));
        let report = transfer(tag_selection_request(&protocol, requested)?)
            .await?
            .metadata
            .ok_or("missing metadata report")?;
        assert_eq!(*protocol.tag_reads.lock().await, 0);
        assert_eq!(
            tag_outcome(&report),
            Some(if requested {
                ApplicationOutcome::Unsupported
            } else {
                ApplicationOutcome::OmittedByPolicy
            })
        );
        if requested {
            assert!(
                report
                    .skipped()
                    .iter()
                    .any(|skip| skip.family == MetadataFamily::Tags
                        && skip.reason == RefusalCause::DestinationCannotStore)
            );
        }
    }
    Ok(())
}

fn tag_selection_request(protocol: &Arc<MemoryS3>, requested: bool) -> Result<TransferRequest> {
    let destination = connect_with_tag_support(
        protocol.clone(),
        endpoint_of(protocol),
        None,
        if requested {
            S3TagSupport::Unsupported
        } else {
            S3TagSupport::Supported
        },
    )?;
    let mut request = TransferRequest::new(
        source_storage(protocol, "src")?,
        StoragePath::new("file")?,
        destination,
        CancellationToken::new(),
    );
    if requested {
        request = request.with_copied_metadata(CopiedMetadataRequest::default().with_tags());
    }
    Ok(request)
}

#[tokio::test]
async fn an_unsupported_source_is_reported_without_failing_the_copy() -> Result {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("src/file".into(), Bytes::from_static(b"payload"));
    let source = connect_with_tag_support(
        protocol.source_root("src"),
        identity(),
        None,
        S3TagSupport::Unsupported,
    )?;
    let destination = connect_with_tag_support(
        protocol.clone(),
        endpoint_of(&protocol),
        None,
        S3TagSupport::Supported,
    )?;
    let outcome = transfer(
        TransferRequest::new(
            source,
            StoragePath::new("file")?,
            destination,
            CancellationToken::new(),
        )
        .with_copied_metadata(CopiedMetadataRequest::default().with_tags()),
    )
    .await?;
    let report = outcome.metadata.ok_or("missing metadata report")?;
    assert_eq!(tag_outcome(&report), Some(ApplicationOutcome::Unsupported));
    assert_eq!(*protocol.tag_reads.lock().await, 0);
    assert!(
        report
            .skipped()
            .iter()
            .any(|skip| skip.family == MetadataFamily::Tags
                && skip.reason == RefusalCause::SourceCannotObserve)
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn a_filesystem_destination_skips_tags_without_reading_them() -> Result {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version("src/file", "v1", Bytes::from_static(b"payload"))
        .await;
    // If tags are fetched despite the destination's capability, the copy would fail.
    *protocol.tag_failure.lock().await = Some(("v1".into(), denied()));
    let root = tempfile::tempdir()?;
    let destination = crate::storage::backends::local::test_destination_storage(
        root.path(),
        "tagless-filesystem",
    )?;
    let outcome = transfer(
        TransferRequest::new(
            source_storage(&protocol, "src")?,
            StoragePath::new("file")?,
            destination,
            CancellationToken::new(),
        )
        .with_copied_metadata(CopiedMetadataRequest::default().with_tags()),
    )
    .await?;
    assert_eq!(std::fs::read(root.path().join("file"))?, b"payload");
    assert_eq!(*protocol.tag_reads.lock().await, 0);
    let report = outcome.metadata.ok_or("missing metadata report")?;
    assert_eq!(tag_outcome(&report), Some(ApplicationOutcome::Unsupported));
    assert!(
        report
            .skipped()
            .iter()
            .any(|skip| skip.family == MetadataFamily::Tags
                && skip.reason == RefusalCause::DestinationCannotStore)
    );
    Ok(())
}

fn denied() -> S3ProtocolFailure {
    S3ProtocolFailure::entry(
        FailureClass::PermissionDenied,
        Transience::Permanent,
        "tag access denied",
    )
}

#[tokio::test]
async fn a_tag_read_failure_fails_before_destination_writes() -> Result {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version("src/file", "v1", Bytes::from_static(b"payload"))
        .await;
    *protocol.tag_failure.lock().await = Some(("v1".into(), denied()));
    let failure = transfer(
        request(
            &protocol,
            TransferPolicy::Checkpointed,
            PayloadShapingPolicy::RequireClientShaped,
        )?
        .with_copied_metadata(CopiedMetadataRequest::default().with_tags()),
    )
    .await
    .err()
    .ok_or("tag read unexpectedly succeeded")?;
    assert_eq!(
        (failure.phase(), failure.side()),
        (TransferPhase::Metadata, TransferSide::Source)
    );
    assert_eq!(*protocol.puts.lock().await, 0);
    assert!(!protocol.objects.lock().await.contains_key("file"));
    Ok(())
}

#[tokio::test]
async fn tag_write_failures_are_not_silently_skipped() -> Result {
    for policy in [TransferPolicy::Checkpointed, TransferPolicy::Direct] {
        let protocol = Arc::new(MemoryS3::default());
        protocol
            .objects
            .lock()
            .await
            .insert("src/file".into(), Bytes::from_static(b"payload"));
        *protocol.tag_write_failure.lock().await = Some(denied());
        let failure = transfer(
            request(&protocol, policy, PayloadShapingPolicy::RequireClientShaped)?
                .with_copied_metadata(CopiedMetadataRequest::default().with_tags()),
        )
        .await
        .err()
        .ok_or("tag write unexpectedly succeeded")?;
        assert_eq!(failure.side(), TransferSide::Destination);
        assert_eq!(
            failure.phase(),
            if policy == TransferPolicy::Direct {
                TransferPhase::Metadata
            } else {
                TransferPhase::Publish
            }
        );
        assert!(failure.final_destination_changed());
    }
    Ok(())
}

#[tokio::test]
async fn an_empty_source_tag_set_clears_old_destination_tags() -> Result {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("src/file".into(), Bytes::from_static(b"payload"));
    protocol
        .put_tags("file", &[ObjectTag::new("stale", "value")?])
        .await
        .map_err(|e| format!("{e:?}"))?;
    transfer(
        request(
            &protocol,
            TransferPolicy::Checkpointed,
            PayloadShapingPolicy::RequireClientShaped,
        )?
        .with_copied_metadata(CopiedMetadataRequest::default().with_tags()),
    )
    .await?;
    assert!(
        protocol
            .get_tags("file", None)
            .await
            .map_err(|e| format!("{e:?}"))?
            .is_empty()
    );
    Ok(())
}
