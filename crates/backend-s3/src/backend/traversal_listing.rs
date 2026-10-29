//! S3's logical traversal children, leaving raw namespace/delete listings untouched.
//! A common prefix is inspected before it is emitted: an own content object alone is a File,
//! all-zero object history is a directory marker, and real descendants establish a Directory.
//! Inspection retains only the own descriptor and an existence flag, never the child listing.
//! It may read the child listing twice; a hint is not required for correctness after resume.

use futures::StreamExt as _;

use super::listing::{child_failure, directory_descriptor};
use crate::model::{BackendIdentity, EntryKind, FailureClass, StoragePath};
use crate::storage::listing::cancelled;
use crate::storage::{
    ChildFailure, ListRequest, ListedChild, ListingPage, Namespace, PageSink, SourceDescriptor,
    StorageRoleFailure,
};

/// Root own objects retain their diagnostics. Below the root they were already represented by
/// the parent's projected listing and must not be emitted twice.
pub(super) async fn produce(
    namespace: &dyn Namespace,
    identity: &BackendIdentity,
    request: &ListRequest,
    is_root: bool,
    sink: &PageSink,
) {
    let mut raw = namespace.list(request.clone());
    let mut root_object_reported = false;
    loop {
        let next = tokio::select! {
            biased;
            () = request.cancel.cancelled() => return sink.fail(cancelled(&request.path)).await,
            () = sink.closed() => return,
            next = raw.next() => next,
        };
        let Some(next) = next else { return };
        let page = match next {
            Ok(page) => page,
            Err(failure) => return sink.fail(failure).await,
        };
        let projected = tokio::select! {
            biased;
            () = request.cancel.cancelled() => return sink.fail(cancelled(&request.path)).await,
            () = sink.closed() => return,
            result = project(namespace, identity, request, is_root, &mut root_object_reported, page) => result,
        };
        match projected {
            Ok(page) => {
                if sink.send_split(page, request).await.is_err() {
                    return;
                }
            }
            Err(failure) => return sink.fail(failure).await,
        }
    }
}

async fn project(
    namespace: &dyn Namespace,
    identity: &BackendIdentity,
    request: &ListRequest,
    is_root: bool,
    root_object_reported: &mut bool,
    page: ListingPage,
) -> Result<ListingPage, StorageRoleFailure> {
    let mut result = ListingPage::new(Vec::new(), page.failures, page.internal_hidden);
    for child in page.children {
        if child.is_directory_object() {
            if is_root {
                root_object(&request.path, child, root_object_reported, &mut result)?;
            }
            continue;
        }
        if child.kind != EntryKind::Directory {
            result.children.push(child);
            continue;
        }
        let directory = child
            .into_descriptor(&request.path)
            .map_err(|_| {
                child_failure(
                    &request.path,
                    FailureClass::Unsupported,
                    "an S3 prefix exceeds the path limit",
                )
            })
            .map_err(StorageRoleFailure::Entry)?;
        match inspect(namespace, request, &directory.path).await {
            Ok(evidence) => emit(identity, &request.path, directory, evidence, &mut result)?,
            Err(StorageRoleFailure::Entry(failure)) => {
                result.push_failure(ChildFailure::from_failure(&request.path, failure));
            }
            Err(session) => return Err(session),
        }
    }
    Ok(result)
}

/// The scan root itself is omitted: only a current unversioned zero-byte marker is harmless.
/// Every history is diagnostic here, even when its object versions are all zero bytes.
fn root_object(
    directory: &StoragePath,
    child: ListedChild,
    reported: &mut bool,
    page: &mut ListingPage,
) -> Result<(), StorageRoleFailure> {
    if *reported || (child.size == Some(0) && child.listing.history.is_none()) {
        return Ok(());
    }
    let descriptor = child.into_descriptor(directory).map_err(|_| {
        StorageRoleFailure::Entry(child_failure(
            directory,
            FailureClass::Unsupported,
            "an S3 own key exceeds the path limit",
        ))
    })?;
    *reported = true;
    page.push_failure(ChildFailure::from_failure(
        directory,
        child_failure(
            &descriptor.path,
            FailureClass::Unsupported,
            "the traversal root's own directory object holds content or history but is not part of the \
             traversal; traverse its parent to include it",
        ),
    ));
    Ok(())
}

#[derive(Default)]
struct Evidence {
    own: Option<SourceDescriptor>,
    descendants: bool,
}

/// Absence requires successful exhaustion. A failure never masquerades as a leaf or a marker.
async fn inspect(
    namespace: &dyn Namespace,
    parent: &ListRequest,
    directory: &StoragePath,
) -> Result<Evidence, StorageRoleFailure> {
    let request =
        ListRequest::new(directory.clone(), parent.cancel.clone()).with_page_hint(parent.page_hint);
    let mut listing = namespace.list(request);
    let mut evidence = Evidence::default();
    while let Some(page) = listing.next().await {
        let page = page?;
        if let Some(failure) = page.failures.into_iter().next() {
            return Err(StorageRoleFailure::Entry(failure.failure));
        }
        // Artifacts are hidden children, but still prove this is more than an own object.
        evidence.descendants |= page.internal_hidden > 0;
        for child in page.children {
            if child.is_directory_object() {
                evidence.own = Some(
                    child
                        .into_descriptor(directory)
                        .map_err(|_| {
                            child_failure(
                                directory,
                                FailureClass::Unsupported,
                                "an S3 own key exceeds the path limit",
                            )
                        })
                        .map_err(StorageRoleFailure::Entry)?,
                );
            } else {
                evidence.descendants = true;
            }
        }
        if evidence.own.is_some() && evidence.descendants {
            break;
        }
    }
    Ok(evidence)
}

/// A history consisting only of delete markers has no zero-byte object version: keep it as a
/// File history rather than inventing a directory marker from the absence of sizes.
fn is_marker(descriptor: &SourceDescriptor) -> bool {
    match descriptor.listing.history.as_deref() {
        Some(history) => history.largest_version() == Some(0),
        None => descriptor.size == Some(0),
    }
}

fn emit(
    identity: &BackendIdentity,
    parent: &StoragePath,
    directory: SourceDescriptor,
    evidence: Evidence,
    page: &mut ListingPage,
) -> Result<(), StorageRoleFailure> {
    match evidence.own {
        Some(mut own) if is_marker(&own) => {
            let mut marker = directory_descriptor(identity, directory.path)
                .map_err(StorageRoleFailure::Entry)?;
            marker.listing.history = own.listing.history.take();
            marker.inline_timestamps = own.inline_timestamps;
            if marker.listing.history.is_none() {
                marker.listing.directory_marker = Some(Box::new(own));
            }
            page.push_descriptor(parent, marker, None);
        }
        Some(own) => {
            if evidence.descendants {
                page.push_descriptor(parent, directory, None);
            }
            page.push_child_object(parent, own);
        }
        // A ghost prefix alone is not a real directory. MinIO can return one for a deleted
        // zero-byte marker whose raw own-object descriptor was deliberately suppressed.
        None if evidence.descendants => page.push_descriptor(parent, directory, None),
        None => {}
    }
    Ok(())
}
