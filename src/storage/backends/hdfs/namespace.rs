use std::sync::Arc;

use async_trait::async_trait;

use super::protocol::{HdfsProtocol, entry_failure};
use super::source::descriptor;
use crate::model::{
    BackendIdentity, FailureClass, Operation, StoragePath, SymlinkTarget, Transience,
};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::listing::respelled;
use crate::storage::{
    ListRequest, ListingPage, ListingStream, NameSemantics, Namespace, NamespaceCapabilities,
    SourceDescriptor, StorageRoleFailure,
};

#[derive(Clone)]
pub(super) struct HdfsNamespace {
    protocol: Arc<dyn HdfsProtocol>,
    identity: BackendIdentity,
}

impl HdfsNamespace {
    pub(super) fn new<P: HdfsProtocol + 'static>(
        protocol: Arc<P>,
        identity: BackendIdentity,
    ) -> Self {
        Self { protocol, identity }
    }

    async fn stat_entry(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let facts = self.protocol.stat(path).await?;
        descriptor(&self.identity, path.clone(), &facts)
    }

    /// Reads one whole directory (true paging comes with C8). Transfer artifacts never appear in
    /// a listing (ADR-0006 C3b); the protocol listing stays raw for callers that must find them.
    /// A child that cannot be described is reported on its own, beside the siblings that could
    /// be.
    ///
    /// The server spells children normalised (`a/b/x` for a listing of `a//b`); each is spelled
    /// again as the requested path and its name, the listing contract.
    async fn list_whole(&self, path: &StoragePath) -> Result<ListingPage, StorageRoleFailure> {
        let children = self.protocol.list(path).await?;
        let mut entries = Vec::with_capacity(children.len());
        let mut failures = Vec::new();
        let mut hidden = 0_u32;
        for child in children {
            let described = match child {
                Ok(facts) if is_artifact_path(facts.path.as_str()) => {
                    hidden = hidden.saturating_add(1);
                    continue;
                }
                Ok(facts) => {
                    let spelled = respelled(path, facts.path.clone());
                    descriptor(&self.identity, spelled, &facts)
                }
                Err(failure) if is_artifact_path(failure.path().as_str()) => {
                    hidden = hidden.saturating_add(1);
                    continue;
                }
                Err(failure) => Err(StorageRoleFailure::Entry(failure)),
            };
            match described {
                Ok(descriptor) => entries.push(descriptor),
                Err(StorageRoleFailure::Entry(failure)) => failures.push(failure),
                Err(session @ StorageRoleFailure::Session(_)) => return Err(session),
            }
        }
        Ok(ListingPage::from_descriptors(
            path, entries, failures, hidden,
        ))
    }
}

#[async_trait]
impl Namespace for HdfsNamespace {
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        refuse_artifacts(&[path])?;
        self.stat_entry(path).await
    }

    /// The listing runs to its end in its producer even when the stream is dropped.
    fn list(&self, request: ListRequest) -> ListingStream {
        if let Err(failure) = refuse_artifacts(&[&request.path]) {
            return ListingStream::failed(request.path, failure);
        }
        let namespace = self.clone();
        ListingStream::from_producer(request.path.clone(), move |sink| async move {
            let listing = namespace.list_whole(&request.path).await;
            sink.deliver(listing, &request).await;
        })
    }

    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        refuse_artifacts(&[path])?;
        Err(failure(path, FailureClass::Unsupported))
    }

    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        refuse_artifacts(&[path])?;
        refuse_root(path)?;
        self.protocol.create_directory(path).await
    }

    async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        refuse_artifacts(&[path])?;
        refuse_root(path)?;
        let kind = self.stat_entry(path).await?.kind;
        self.protocol.delete(path, kind).await
    }

    async fn rename(&self, from: &StoragePath, to: &StoragePath) -> Result<(), StorageRoleFailure> {
        refuse_artifacts(&[from, to])?;
        refuse_root(from)?;
        refuse_root(to)?;
        self.protocol.rename(from, to, true).await
    }

    fn capabilities(&self) -> NamespaceCapabilities {
        NamespaceCapabilities {
            symlinks: false,
            names: NameSemantics::EXACT,
            ..NamespaceCapabilities::BASIC
        }
    }
}

fn refuse_root(path: &StoragePath) -> Result<(), StorageRoleFailure> {
    if path.as_str().is_empty() {
        Err(failure(path, FailureClass::InvalidInput))
    } else {
        Ok(())
    }
}

/// Transfer artifacts are not addressable through the namespace (ADR-0006 C3b), as on Local and
/// NFS: listings hide them, so no verb may reach them either.
fn refuse_artifacts(paths: &[&StoragePath]) -> Result<(), StorageRoleFailure> {
    match paths.iter().find(|path| is_artifact_path(path.as_str())) {
        Some(path) => Err(failure(path, FailureClass::InvalidInput)),
        None => Ok(()),
    }
}

fn failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, Operation::Namespace, class, Transience::Permanent)
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::storage::{CollectLimits, ListedChild, collect_listing};

    fn path(value: &str) -> StoragePath {
        StoragePath::new(value).unwrap_or_else(|e| panic!("{e}"))
    }

    fn namespace(protocol: &Arc<super::super::contract_tests::MemoryHdfs>) -> HdfsNamespace {
        HdfsNamespace::new(
            Arc::clone(protocol),
            BackendIdentity::new(crate::model::BackendKind::Hdfs, "hdfs://nn:8020")
                .unwrap_or_else(|e| panic!("{e}")),
        )
    }

    async fn list(namespace: &HdfsNamespace, directory: &str) -> ListingPage {
        let request = ListRequest::new(path(directory), CancellationToken::new());
        collect_listing(namespace.list(request), CollectLimits::UNBOUNDED)
            .await
            .unwrap_or_else(|e| panic!("{e}"))
    }

    fn refused<T: std::fmt::Debug>(result: &Result<T, StorageRoleFailure>) -> bool {
        matches!(result, Err(StorageRoleFailure::Entry(failure))
            if failure.class() == FailureClass::InvalidInput)
    }

    #[tokio::test]
    async fn artifact_paths_are_refused_for_every_verb() {
        let protocol = Arc::new(super::super::contract_tests::MemoryHdfs::default());
        let namespace = namespace(&protocol);
        assert!(refused(
            &namespace.stat(&path("dir/.data-mover-0123.part")).await
        ));
        let request = ListRequest::new(path(".data-mover-staging"), CancellationToken::new());
        assert!(refused(
            &collect_listing(namespace.list(request), CollectLimits::UNBOUNDED).await
        ));
        assert!(refused(
            &namespace.delete(&path("dir/.data-mover-x.claimed")).await
        ));
        assert!(refused(
            &namespace
                .create_directory(&path(".data-mover-staging/x"))
                .await
        ));
        assert!(refused(
            &namespace
                .rename(&path("a"), &path("dir/.data-mover-a.part"))
                .await
        ));
        assert!(refuse_artifacts(&[&path("dir/x.data-mover-y")]).is_ok());
    }

    /// The role listing hides artifacts and counts them; the protocol listing underneath stays
    /// raw.
    #[tokio::test]
    async fn listing_hides_artifacts_but_the_protocol_does_not() {
        let protocol = Arc::new(super::super::contract_tests::MemoryHdfs::default());
        protocol
            .insert("a.txt", bytes::Bytes::from_static(b"a"))
            .await;
        protocol
            .insert(".data-mover-0123.part", bytes::Bytes::from_static(b"p"))
            .await;
        let page = list(&namespace(&protocol), "").await;
        let names = page
            .children
            .iter()
            .map(ListedChild::name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a.txt"]);
        assert_eq!(page.internal_hidden, 1);
        let raw = protocol
            .list(&StoragePath::root())
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(raw.len(), 2);
    }

    /// A child the listing could not describe is an entry failure at its own path; its
    /// siblings are still listed, and an undescribable artifact stays hidden.
    #[tokio::test]
    async fn an_undescribable_child_fails_alone() {
        let protocol = Arc::new(super::super::contract_tests::MemoryHdfs::default());
        for name in ["a.txt", "bad.txt", "c.txt", ".data-mover-0123.part"] {
            protocol.insert(name, bytes::Bytes::from_static(b"a")).await;
        }
        protocol.make_undescribable("bad.txt").await;
        protocol.make_undescribable(".data-mover-0123.part").await;
        let page = list(&namespace(&protocol), "").await;
        let mut names = page
            .children
            .iter()
            .map(ListedChild::name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, vec!["a.txt", "c.txt"]);
        let failed = page
            .failures
            .iter()
            .map(|failure| failure.failure.path().as_str())
            .collect::<Vec<_>>();
        assert_eq!(failed, vec!["bad.txt"]);
        assert_eq!(page.failures[0].name.as_deref(), Some("bad.txt"));
    }

    /// A child spelled normalised by the server is named as the requested directory and its
    /// last segment, so `a//b`'s children join back to `a//b/<name>`; a server path outside the
    /// requested directory is not renamed into it.
    #[test]
    fn a_normalised_child_is_respelled_under_the_requested_path() {
        assert_eq!(respelled(&path("a//b"), path("a/b/x")), path("a//b/x"));
        assert_eq!(respelled(&path("./"), path("x")), path(".//x"));
        assert_eq!(respelled(&StoragePath::root(), path("x")), path("x"));
        assert_eq!(respelled(&path("a//b"), path("c/x")), path("c/x"));
        assert_eq!(respelled(&path("a"), path("a/b/x")), path("a/b/x"));
    }

    #[test]
    fn root_mutation_is_refused_before_hdfs_io() {
        assert!(refuse_root(&StoragePath::root()).is_err());
        assert!(refuse_root(&StoragePath::new("safe").unwrap_or_else(|e| panic!("{e}"))).is_ok());
    }
}
