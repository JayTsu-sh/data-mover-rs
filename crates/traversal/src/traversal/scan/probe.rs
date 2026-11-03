//! Verify the original failed read before rebuilding a potentially large checkpoint prefix.
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::{GuardedMetadata, GuardedNamespace, RecoveryTarget, ScanFailureSignal, ScanRead};
use crate::model::{FailureClass, ObservationPlan, SourceVersion};
use crate::storage::{
    ListRequest, ListingStream, Metadata, Namespace, Storage, StorageRoleFailure,
};
use crate::traversal::RemoteScanFailureKind;

struct CancelListing(CancellationToken);
impl Drop for CancelListing {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl RecoveryTarget {
    pub(in crate::traversal) async fn probe(
        &self,
        storage: &Storage,
        guard: &ScanFailureSignal,
        cancel: CancellationToken,
    ) -> bool {
        let (Ok(namespace), Ok(metadata)) = (storage.namespace(), storage.metadata()) else {
            return false;
        };
        let namespace = GuardedNamespace {
            inner: namespace,
            guard: guard.clone(),
        };
        let metadata = GuardedMetadata {
            inner: metadata,
            guard: guard.clone(),
        };
        let result = self.read_probe(&namespace, &metadata, guard, cancel).await;
        let verified = match result {
            Ok(()) => true,
            Err(StorageRoleFailure::Entry(error)) => match error.class() {
                FailureClass::NotFound => true,
                FailureClass::Conflict => self.target_changed(&namespace).await,
                _ => false,
            },
            Err(StorageRoleFailure::Session(_)) => false,
        };
        if verified {
            guard.record_success(&self.read, &self.path, false);
        }
        verified && guard.recovered_at().is_some()
    }

    async fn read_probe(
        &self,
        namespace: &GuardedNamespace,
        metadata: &GuardedMetadata,
        guard: &ScanFailureSignal,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        let path = &self.path;
        match &self.read {
            ScanRead::Stat => namespace.stat(path).await.map(|_| ()),
            ScanRead::ReadLink => namespace.read_link(path).await.map(|_| ()),
            ScanRead::Listing {
                page,
                root,
                page_hint,
            } => {
                let request = ListRequest::new(path.clone(), cancel).with_page_hint(*page_hint);
                self.listing_probe(namespace, guard, request, *root, *page)
                    .await
            }
            ScanRead::InlineMetadata(plan) => self.inline_probe(namespace, metadata, *plan).await,
            ScanRead::Metadata(plan) => metadata.observe(path, *plan).await.map(|_| ()),
            ScanRead::BoundMetadata(plan, identity) => metadata
                .observe_bound(path, identity, *plan)
                .await
                .map(|_| ()),
            ScanRead::CopyMetadata(plan, identity) => metadata
                .observe_copy_bound(path, identity, *plan)
                .await
                .map(|_| ()),
            ScanRead::VersionMetadata(plan, version, identity) => metadata
                .observe_copy_bound_version(path, identity, version, *plan)
                .await
                .map(|_| ()),
        }
    }

    async fn inline_probe(
        &self,
        namespace: &GuardedNamespace,
        metadata: &GuardedMetadata,
        plan: ObservationPlan,
    ) -> Result<(), StorageRoleFailure> {
        let descriptor = namespace.stat(&self.path).await?;
        let matches = self.inline_descriptor.as_ref().is_some_and(|original| {
            original.source_identity == descriptor.source_identity
                && original.version == descriptor.version
                && original.kind == descriptor.kind
                && original.listing == descriptor.listing
        });
        if !matches && self.inline_descriptor.is_some() {
            // The original read no longer applies. Replay observes this fresh
            // descriptor normally; no cached facts or entries are emitted here.
            return Ok(());
        }
        match metadata.observe_listed(&descriptor, plan) {
            Some(result) if matches => result.map(|_| ()),
            _ => Err(super::stopped_failure(
                self.read.operation(),
                RemoteScanFailureKind::ConnectionLost,
            )),
        }
    }

    async fn target_changed(&self, namespace: &GuardedNamespace) -> bool {
        let Ok(current) = namespace.stat(&self.path).await else {
            return false;
        };
        match &self.read {
            ScanRead::BoundMetadata(_, identity)
            | ScanRead::CopyMetadata(_, identity)
            | ScanRead::VersionMetadata(_, SourceVersion::Current, identity) => {
                current.source_identity != *identity
            }
            _ => false,
        }
    }

    async fn listing_probe(
        &self,
        namespace: &GuardedNamespace,
        guard: &ScanFailureSignal,
        request: ListRequest,
        root: Option<bool>,
        target_page: usize,
    ) -> Result<(), StorageRoleFailure> {
        let _cancel = CancelListing(request.cancel.clone());
        let mut read = ScanRead::listing(&request, root);
        // Use the raw stream here: a second guarded producer could read N+1 before
        // the probe of page N is dropped and contaminate the retained fresh guard.
        let pages: ListingStream = match root {
            Some(root) => namespace.inner.list_for_traversal(request, root),
            None => namespace.inner.list(request),
        };
        let pages = Arc::new(Mutex::new(pages));
        for page in 0..=target_page {
            let next_page = pages.clone();
            let next = guard
                .operation(&read, &self.path, async move {
                    next_page.lock().await.next().await.transpose()
                })
                .await?;
            let next = guard.checked_page(&read, &self.path, next)?;
            if next.is_none() || page == target_page {
                return Ok(());
            }
            read.advance_page();
        }
        Ok(())
    }
}
