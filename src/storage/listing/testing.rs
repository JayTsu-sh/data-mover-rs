//! Helpers for test fakes of the namespace role and for tests that read a whole listing.

use tokio_util::sync::CancellationToken;

use super::{CollectLimits, ListRequest, ListingPage, ListingStream, collect_listing};
use crate::model::{EntryOperationFailure, StoragePath};
use crate::storage::{Namespace, SourceDescriptor, StorageRoleFailure};

/// A listing's children with full paths, and the children it could not describe.
pub(crate) type Described = (Vec<SourceDescriptor>, Vec<EntryOperationFailure>);

/// A fake's answer to a listing of `directory` — children spelled with full paths, as fakes
/// build them — as the one-page stream a namespace returns. A descriptor at `directory` + `/` is
/// handed over as the directory's own object (P7 F5).
pub(crate) fn answer(
    directory: &StoragePath,
    result: Result<Described, StorageRoleFailure>,
) -> ListingStream {
    let item = result.map(|(descriptors, failures)| {
        let own_key = format!("{}/", directory.as_str());
        let (own, children): (Vec<_>, Vec<_>) = descriptors
            .into_iter()
            .partition(|descriptor| descriptor.path.as_str() == own_key);
        let mut page = ListingPage::from_descriptors(directory, children, failures, 0);
        for descriptor in own {
            page.push_directory_object(directory, descriptor);
        }
        page
    });
    ListingStream::from_pages(directory.clone(), [item])
}

/// [`answer`] for a listing in which every child was described.
pub(crate) fn entries(
    directory: &StoragePath,
    descriptors: Vec<SourceDescriptor>,
) -> ListingStream {
    answer(directory, Ok((descriptors, Vec::new())))
}

/// Lists `directory` through `namespace` and collects it whole.
pub(crate) async fn listed(
    namespace: &(impl Namespace + ?Sized),
    directory: &StoragePath,
) -> Result<Described, StorageRoleFailure> {
    let request = ListRequest::new(directory.clone(), CancellationToken::new());
    collect_listing(namespace.list(request), CollectLimits::UNBOUNDED)
        .await
        .map(|page| page.into_descriptors(directory))
}

/// One namespace verb with its arguments, so a test can check a table of verbs the same way.
#[derive(Clone, Debug)]
pub(crate) enum Verb {
    Stat(StoragePath),
    List(StoragePath),
    ReadLink(StoragePath),
    CreateDirectory(StoragePath),
    Delete(StoragePath),
    Rename { from: StoragePath, to: StoragePath },
}

/// Performs a [`Verb`] on any namespace.
pub(crate) trait Perform {
    /// Performs `verb`, keeping only whether it succeeded; a listing is read to its end.
    async fn perform(&self, verb: Verb) -> Result<(), StorageRoleFailure>;
}

impl<N: Namespace + ?Sized> Perform for N {
    async fn perform(&self, verb: Verb) -> Result<(), StorageRoleFailure> {
        match verb {
            Verb::Stat(path) => self.stat(&path).await.map(drop),
            Verb::List(path) => listed(self, &path).await.map(drop),
            Verb::ReadLink(path) => self.read_link(&path).await.map(drop),
            Verb::CreateDirectory(path) => self.create_directory(&path).await,
            Verb::Delete(path) => self.delete(&path).await,
            Verb::Rename { from, to } => self.rename(&from, &to).await,
        }
    }
}
