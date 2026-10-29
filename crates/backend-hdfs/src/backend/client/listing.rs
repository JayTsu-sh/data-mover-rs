use std::cmp::Ordering as NameOrder;
use std::num::NonZeroUsize as ListingPageSize;

use futures::stream::BoxStream as ListingStatusStream;
use hdfs_native::client::FileStatus as ListedStatus;
use tokio::time::timeout as attempt_timeout;

use crate::storage::backends::hdfs::protocol::{FailedHdfsListing, HdfsListing as RoleHdfsListing};

/// The statuses of one directory, in listing order, as the name node returns them.
type HdfsStatuses = ListingStatusStream<'static, Result<ListedStatus, HdfsError>>;

/// One role listing of an HDFS directory, page by page (P7 C8, §5.2).
///
/// It asks the name node for nothing but the listing itself: no status of the directory first. A
/// missing directory fails the first page with `NotFound`; a file answers its own listing with its
/// own status, which fails the listing as not a directory (`InvalidInput`), as the status check it
/// replaces did.
///
/// The listing is stateless on the name node (each batch is a `getListing` after the last name
/// returned), so dropping it abandons the request in flight and leaves nothing open.
///
/// A retryable failure (a lost connection, a batch slower than the adapter's attempt deadline)
/// continues after the last name read, within the adapter's retry budget, which progress renews:
/// the listing is opened again and skips what it already read (P7 §8, `startAfter`). `hdfs-native`
/// keeps its `getListing` with `startAfter` crate-private and ends a listing at its first error,
/// so the skip re-reads the batches before that name — only on a failure. Children are never
/// handed out twice. Any other failure, or a retryable one past the budget, ends the listing after
/// the children read before it (`Interrupted` once a page is out).
struct HdfsStatusListing<Open> {
    storage: HDFSStorage,
    directory: RoleStoragePath,
    /// The listed directory relative to the root, as a child's parent is spelled.
    relative: PathBuf,
    /// The listed directory on the name node: a status spelled so is the directory itself.
    resolved: String,
    open: Open,
    statuses: Option<HdfsStatuses>,
    /// The path of the last status read; a listing opened again skips up to and including it.
    last_read: Option<String>,
    /// Whether a listing opened again is still skipping what was read before.
    skipping: bool,
    retries: usize,
    order_warned: bool,
    /// A failure met after some children of the page had been read, reported on the next call.
    pending: Option<RoleFailure>,
    ended: bool,
}

impl HDFSStorage {
    /// The role listing of `path`'s immediate children ([`HdfsStatusListing`]).
    fn role_listing(&self, path: &RoleStoragePath) -> Box<dyn RoleHdfsListing> {
        let relative = Path::new(path.as_str());
        let resolved = match self.resolve_path(relative) {
            Ok(resolved) => resolved,
            Err(error) => {
                let failure = hdfs_role_error(path, RoleOperation::Traverse, error);
                return Box::new(FailedHdfsListing::new(failure));
            }
        };
        let client = self.client.clone();
        let target = resolved.clone();
        Box::new(HdfsStatusListing::new(
            self.clone(),
            path.clone(),
            resolved,
            move || client.list_status_iter(&target, false).into_stream(),
        ))
    }
}

impl<Open> HdfsStatusListing<Open>
where
    Open: FnMut() -> HdfsStatuses + Send,
{
    fn new(storage: HDFSStorage, directory: RoleStoragePath, resolved: String, open: Open) -> Self {
        let relative = Path::new(directory.as_str())
            .components()
            .filter(|component| matches!(component, Component::Normal(_)))
            .collect();
        Self {
            storage,
            directory,
            relative,
            resolved,
            open,
            statuses: None,
            last_read: None,
            skipping: false,
            retries: 0,
            order_warned: false,
            pending: None,
            ended: false,
        }
    }

    /// The next status not read before, opening the listing first when it is not open; `Ok(None)`
    /// at its end. One status that takes longer than the adapter's attempt deadline (one
    /// `getListing` round trip) is a retryable I/O failure.
    async fn next_status(&mut self) -> Result<Option<ListedStatus>, HdfsError> {
        loop {
            let statuses = match &mut self.statuses {
                Some(statuses) => statuses,
                empty => {
                    self.skipping = self.last_read.is_some();
                    empty.insert((self.open)())
                }
            };
            let status = match attempt_timeout(HDFS_ADAPTER_ATTEMPT_TIMEOUT, statuses.next()).await
            {
                Ok(Some(next)) => next?,
                Ok(None) => return Ok(None),
                Err(_) => return Err(adapter_deadline_error()),
            };
            // The listed path itself (it became a file) is never skipped as already read.
            if self.skipping && !self.is_listed_path(&status) {
                let last = self.last_read.as_deref().unwrap_or_default();
                if hdfs_name_order(&status.path, last) != NameOrder::Greater {
                    continue;
                }
                self.skipping = false;
            }
            return Ok(Some(status));
        }
    }

    /// How long to wait before continuing after `error`, when it may be retried: a retryable
    /// failure with retries left.
    fn retry_delay(&mut self, error: &StorageError) -> Option<Duration> {
        if !matches!(error, StorageError::HdfsOperation(details) if details.retryable) {
            return None;
        }
        let delay = HDFS_ADAPTER_RETRY_DELAYS.get(self.retries).copied()?;
        self.retries += 1;
        Some(delay)
    }

    /// Whether `status` is the listed path itself: what the name node returns for a file.
    fn is_listed_path(&self, status: &ListedStatus) -> bool {
        status.path.trim_end_matches('/') == self.resolved.trim_end_matches('/')
    }

    /// Reads at most `max` statuses: `Ok` with what was read (at the listing's end, or once a
    /// failure follows some statuses, possibly fewer; the failure is then pending), or the
    /// listing's failure.
    async fn read_statuses(
        &mut self,
        max: ListingPageSize,
    ) -> Result<Vec<ListedStatus>, RoleFailure> {
        let mut statuses: Vec<ListedStatus> = Vec::new();
        let failure = loop {
            if statuses.len() >= max.get() {
                break None;
            }
            let error = match self.next_status().await {
                Ok(Some(status)) if self.is_listed_path(&status) => {
                    break Some(self.not_a_directory());
                }
                Ok(Some(status)) => {
                    // Progress renews the retry budget.
                    self.retries = 0;
                    self.check_order(statuses.last(), &status);
                    statuses.push(status);
                    continue;
                }
                Ok(None) => {
                    self.ended = true;
                    break None;
                }
                Err(error) => error,
            };
            if let Some(last) = statuses.last() {
                self.last_read = Some(last.path.clone());
            }
            let mapped = hdfs_operation_error("list directory", Some(&self.relative), &error);
            if let Some(delay) = self.retry_delay(&mapped) {
                self.statuses = None;
                sleep(delay).await;
                continue;
            }
            break Some(hdfs_role_error(
                &self.directory,
                RoleOperation::Traverse,
                mapped,
            ));
        };
        self.settle(statuses, failure)
    }

    /// What one page's read hands back: the statuses read, with a failure after them left
    /// pending, or the failure alone when nothing was read. Either way a failure ends the listing.
    fn settle(
        &mut self,
        statuses: Vec<ListedStatus>,
        failure: Option<RoleFailure>,
    ) -> Result<Vec<ListedStatus>, RoleFailure> {
        if let Some(last) = statuses.last() {
            self.last_read = Some(last.path.clone());
        }
        let Some(failure) = failure else {
            return Ok(statuses);
        };
        self.ended = true;
        if statuses.is_empty() {
            return Err(failure);
        }
        self.pending = Some(failure);
        Ok(statuses)
    }

    /// Warns once per listing when the name node does not return names in the order continuing
    /// after the last name relies on ([`hdfs_name_order`]); continuing would then skip children.
    fn check_order(&mut self, previous: Option<&ListedStatus>, next: &ListedStatus) {
        let previous = previous
            .map(|status| status.path.as_str())
            .or(self.last_read.as_deref());
        if self.order_warned
            || previous
                .is_none_or(|previous| hdfs_name_order(&next.path, previous) == NameOrder::Greater)
        {
            return;
        }
        self.order_warned = true;
        tracing::warn!(
            directory = %self.directory,
            "HDFS listed names out of signed-byte order; \
             a listing continued after a failure may skip children"
        );
    }

    fn not_a_directory(&self) -> RoleFailure {
        hdfs_role_entry(
            &self.directory,
            RoleOperation::Traverse,
            RoleFailureClass::InvalidInput,
            RoleTransience::Permanent,
        )
    }
}

/// How the name node orders a directory's children: by name, comparing bytes as signed
/// (`DFSUtilClient.compareBytes`), so a name with a byte of 0x80 or above sorts before an ASCII
/// one. Children of one directory share its prefix, so their full paths compare as their names.
fn hdfs_name_order(left: &str, right: &str) -> NameOrder {
    let signed = |byte: u8| i8::from_ne_bytes([byte]);
    left.bytes().map(signed).cmp(right.bytes().map(signed))
}

#[async_trait::async_trait]
impl<Open> RoleHdfsListing for HdfsStatusListing<Open>
where
    Open: FnMut() -> HdfsStatuses + Send,
{
    async fn next_page(
        &mut self,
        max: ListingPageSize,
    ) -> Result<Option<Vec<RoleHdfsListedChild>>, RoleFailure> {
        if let Some(failure) = self.pending.take() {
            return Err(failure);
        }
        if self.ended {
            return Ok(None);
        }
        let statuses = self.read_statuses(max).await?;
        if statuses.is_empty() {
            return Ok(None);
        }
        let children = self.storage.children_from_listing(statuses, &self.relative);
        Ok(Some(
            children
                .into_iter()
                .map(|child| hdfs_role_child(&self.directory, child))
                .collect(),
        ))
    }
}
