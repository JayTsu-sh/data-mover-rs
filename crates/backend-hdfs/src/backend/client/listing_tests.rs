#[cfg(any(test, feature = "test-support"))]
mod listing_tests {
    //! The role listing of an HDFS directory over scripted name-node answers (P7 C8): pages
    //! follow the hint, a retryable failure continues after the last name read (never handing a
    //! child out twice), and any other failure ends the listing after what was read.

    use std::collections::VecDeque;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};

    use futures::StreamExt as _;
    use hdfs_native::HdfsError;
    use hdfs_native::client::FileStatus;

    use super::{
        HDFSStorage, HdfsConfig, HdfsStatusListing, HdfsStatuses, RoleFailure, RoleFailureClass,
        RoleHdfsListing as _, RoleStoragePath, build_hdfs_client,
    };

    type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    /// One scripted answer: the statuses and failures of one opened listing, and whether it
    /// then stops answering instead of ending.
    type Script = (Vec<std::result::Result<FileStatus, HdfsError>>, bool);

    const ENDS: bool = false;
    const STALLS: bool = true;

    fn storage() -> Result<HDFSStorage> {
        let (client, location) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())?;
        Ok(HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::transfer_concurrency::TransferConcurrency::defaults(4, 1),
        })
    }

    fn status(path: &str) -> FileStatus {
        FileStatus {
            path: path.to_owned(),
            length: 1,
            isdir: false,
            permission: 0o644,
            owner: "user".to_owned(),
            group: "group".to_owned(),
            modification_time: 0,
            access_time: 0,
            replication: None,
            blocksize: None,
        }
    }

    fn children(range: std::ops::Range<usize>) -> Vec<std::result::Result<FileStatus, HdfsError>> {
        range
            .map(|index| Ok(status(&format!("/root/d/f{index:02}"))))
            .collect()
    }

    fn reset() -> HdfsError {
        HdfsError::IOError(std::io::Error::from(std::io::ErrorKind::ConnectionReset))
    }

    /// A listing of `directory` (on the name node `resolved`) answering `scripts` in turn, one
    /// per opening, and how often it opened.
    fn listing_of(
        directory: &str,
        resolved: &str,
        scripts: Vec<Script>,
    ) -> Result<(
        HdfsStatusListing<impl FnMut() -> HdfsStatuses + Send>,
        Arc<AtomicUsize>,
    )> {
        let opens = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&opens);
        let scripts = Arc::new(Mutex::new(VecDeque::from(scripts)));
        let open = move || -> HdfsStatuses {
            counter.fetch_add(1, Ordering::SeqCst);
            let script = scripts
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pop_front();
            match script {
                Some((items, ENDS)) => futures::stream::iter(items).boxed(),
                Some((items, STALLS)) => futures::stream::iter(items)
                    .chain(futures::stream::pending())
                    .boxed(),
                None => futures::stream::pending().boxed(),
            }
        };
        let directory = RoleStoragePath::new(directory)?;
        Ok((
            HdfsStatusListing::new(storage()?, directory, resolved.to_owned(), open),
            opens,
        ))
    }

    /// [`listing_of`] the directory `d`.
    fn listing(
        scripts: Vec<Script>,
    ) -> Result<(
        HdfsStatusListing<impl FnMut() -> HdfsStatuses + Send>,
        Arc<AtomicUsize>,
    )> {
        listing_of("d", "/root/d", scripts)
    }

    /// `statuses` followed by `error`.
    fn then(
        mut statuses: Vec<std::result::Result<FileStatus, HdfsError>>,
        error: HdfsError,
    ) -> Vec<std::result::Result<FileStatus, HdfsError>> {
        statuses.push(Err(error));
        statuses
    }

    fn hint(value: usize) -> Result<NonZeroUsize> {
        Ok(NonZeroUsize::new(value).ok_or("the hint is not zero")?)
    }

    /// The paths of the next page's children, failing on anything but a page.
    async fn page(
        listing: &mut HdfsStatusListing<impl FnMut() -> HdfsStatuses + Send>,
        max: usize,
    ) -> Result<Vec<String>> {
        match listing.next_page(hint(max)?).await {
            Ok(Some(children)) => Ok(children
                .into_iter()
                .map(|child| child.map(|facts| facts.path.as_str().to_owned()))
                .collect::<std::result::Result<_, _>>()
                .map_err(|failure| format!("{failure:?}"))?),
            other => Err(format!("expected a page, got {other:?}").into()),
        }
    }

    fn entry_class(
        result: std::result::Result<impl std::fmt::Debug, RoleFailure>,
    ) -> Option<RoleFailureClass> {
        match result {
            Err(RoleFailure::Entry(failure)) => Some(failure.class()),
            _ => None,
        }
    }

    #[tokio::test]
    async fn pages_follow_the_hint_and_the_listing_ends() -> Result {
        let (mut listing, opens) = listing(vec![(children(0..25), ENDS)])?;
        assert_eq!(page(&mut listing, 10).await?.len(), 10);
        assert_eq!(page(&mut listing, 10).await?.len(), 10);
        assert_eq!(
            page(&mut listing, 10).await?,
            ["d/f20", "d/f21", "d/f22", "d/f23", "d/f24"]
        );
        assert!(matches!(listing.next_page(hint(10)?).await, Ok(None)));
        assert!(matches!(listing.next_page(hint(10)?).await, Ok(None)));
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_directory_ends_at_once() -> Result {
        let (mut listing, _) = listing(vec![(Vec::new(), ENDS)])?;
        assert!(matches!(listing.next_page(hint(10)?).await, Ok(None)));
        Ok(())
    }

    /// The listing's own spelling of its directory, normalised, is what a child's parent must be:
    /// the root, `a//b` and `./x` list their children as entries, not as foreign paths.
    #[tokio::test]
    async fn children_of_any_spelling_of_the_directory_are_listed() -> Result {
        for (directory, resolved, child, expected) in [
            ("", "/root", "/root/x", "x"),
            ("a//b", "/root/a/b", "/root/a/b/x", "a/b/x"),
            ("./x", "/root/x", "/root/x/y", "x/y"),
        ] {
            let (mut listing, _) =
                listing_of(directory, resolved, vec![(vec![Ok(status(child))], ENDS)])?;
            assert_eq!(page(&mut listing, 10).await?, [expected], "{directory:?}");
        }
        Ok(())
    }

    /// A retryable failure continues after the last name read, before the first page as after
    /// it, without handing out twice what was read.
    #[tokio::test(start_paused = true)]
    async fn a_retryable_failure_before_the_first_page_continues_after_the_last_name() -> Result {
        let failed = then(children(0..2), reset());
        let (mut listing, opens) = listing(vec![(failed, ENDS), (children(0..5), ENDS)])?;
        assert_eq!(
            page(&mut listing, 10).await?,
            ["d/f00", "d/f01", "d/f02", "d/f03", "d/f04"]
        );
        assert!(matches!(listing.next_page(hint(10)?).await, Ok(None)));
        assert_eq!(opens.load(Ordering::SeqCst), 2);
        Ok(())
    }

    /// After the first page too: the listing goes on after `f11`, the last name it read, and the
    /// traversal never sees the brief failure (P7 §8, `startAfter`). Fault injection: continue
    /// from the beginning, or end the listing, and this goes red.
    #[tokio::test(start_paused = true)]
    async fn a_retryable_failure_after_the_first_page_continues_after_the_last_name() -> Result {
        let failed = then(children(0..12), reset());
        let (mut listing, opens) = listing(vec![(failed, ENDS), (children(0..20), ENDS)])?;
        assert_eq!(page(&mut listing, 10).await?.len(), 10);
        let second = page(&mut listing, 10).await?;
        assert_eq!(second.first().map(String::as_str), Some("d/f10"));
        assert_eq!(second.last().map(String::as_str), Some("d/f19"));
        assert_eq!(second.len(), 10);
        assert!(matches!(listing.next_page(hint(10)?).await, Ok(None)));
        assert_eq!(opens.load(Ordering::SeqCst), 2);
        Ok(())
    }

    /// The last name read was deleted before the listing was opened again: it continues with the
    /// first name after it in the name node's order.
    #[tokio::test(start_paused = true)]
    async fn a_listing_continues_after_a_last_name_deleted_meanwhile() -> Result {
        let failed = then(children(0..12), reset());
        let mut again = children(0..11);
        again.extend(children(12..15));
        let (mut listing, _) = listing(vec![(failed, ENDS), (again, ENDS)])?;
        assert_eq!(page(&mut listing, 10).await?.len(), 10);
        assert_eq!(
            page(&mut listing, 10).await?,
            ["d/f10", "d/f11", "d/f12", "d/f13", "d/f14"]
        );
        Ok(())
    }

    /// The name node orders names by signed bytes: `é` (0xC3 0xA9) comes before `a`. Continuing
    /// after `é` must not skip `a` and `b`, as an unsigned comparison would.
    #[tokio::test(start_paused = true)]
    async fn continuing_follows_the_name_nodes_signed_byte_order() -> Result {
        let names = ["/root/d/é", "/root/d/a", "/root/d/b"];
        let again = names.iter().map(|name| Ok(status(name))).collect();
        let failed = then(vec![Ok(status(names[0]))], reset());
        let (mut listing, _) = listing(vec![(failed, ENDS), (again, ENDS)])?;
        assert_eq!(page(&mut listing, 10).await?, ["d/é", "d/a", "d/b"]);
        Ok(())
    }

    /// A batch that never answers counts as a retryable failure once the adapter's attempt
    /// deadline passes, at the start as after a page.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_listing_continues_after_the_deadline() -> Result {
        let (mut listing, opens) = listing(vec![(Vec::new(), STALLS), (children(0..3), ENDS)])?;
        assert_eq!(page(&mut listing, 10).await?.len(), 3);
        assert_eq!(opens.load(Ordering::SeqCst), 2);

        let scripts = vec![(children(0..12), STALLS), (children(0..15), ENDS)];
        let (mut listing, opens) = listing_of("d", "/root/d", scripts)?;
        assert_eq!(page(&mut listing, 10).await?.len(), 10);
        assert_eq!(page(&mut listing, 10).await?.len(), 5);
        assert_eq!(opens.load(Ordering::SeqCst), 2);
        Ok(())
    }

    /// A failure that is not retried ends the listing after the children read before it: a
    /// page first, then the failure (the traversal reports `Interrupted`, or `Unreadable` when
    /// nothing was read).
    #[tokio::test]
    async fn a_failure_not_retried_ends_the_listing_after_what_was_read() -> Result {
        let gone = || HdfsError::FileNotFound("/root/d".to_owned());
        let (mut listing, opens) = listing(vec![(then(children(0..12), gone()), ENDS)])?;
        assert_eq!(page(&mut listing, 10).await?.len(), 10);
        assert_eq!(page(&mut listing, 10).await?, ["d/f10", "d/f11"]);
        assert_eq!(
            entry_class(listing.next_page(hint(10)?).await),
            Some(RoleFailureClass::NotFound)
        );
        assert!(matches!(listing.next_page(hint(10)?).await, Ok(None)));
        assert_eq!(opens.load(Ordering::SeqCst), 1);

        let scripts = vec![(then(children(0..3), gone()), ENDS)];
        let (mut listing, _) = listing_of("d", "/root/d", scripts)?;
        assert_eq!(page(&mut listing, 10).await?.len(), 3);
        assert!(listing.next_page(hint(10)?).await.is_err());
        Ok(())
    }

    /// The directory became a file while the listing was being continued: the name node answers
    /// with the file's own status, which ends the listing as not a directory after what was read,
    /// never as a complete listing with the unread children missing.
    #[tokio::test(start_paused = true)]
    async fn a_directory_replaced_by_a_file_while_continuing_is_not_a_directory() -> Result {
        let failed = then(children(0..2), reset());
        let replaced = vec![Ok(status("/root/d"))];
        let (mut listing, _) = listing(vec![(failed, ENDS), (replaced, ENDS)])?;
        assert_eq!(page(&mut listing, 10).await?, ["d/f00", "d/f01"]);
        assert_eq!(
            entry_class(listing.next_page(hint(10)?).await),
            Some(RoleFailureClass::InvalidInput)
        );
        assert!(matches!(listing.next_page(hint(10)?).await, Ok(None)));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn retries_without_progress_are_bounded_by_the_adapter_budget() -> Result {
        let scripts = (0..10).map(|_| (vec![Err(reset())], ENDS)).collect();
        let (mut listing, opens) = listing(scripts)?;
        assert!(listing.next_page(hint(10)?).await.is_err());
        assert_eq!(
            opens.load(Ordering::SeqCst),
            super::HDFS_ADAPTER_MAX_ATTEMPTS
        );
        Ok(())
    }

    /// Progress renews the budget: a listing that fails after every new name still finishes.
    #[tokio::test(start_paused = true)]
    async fn progress_renews_the_retry_budget() -> Result {
        let mut scripts: Vec<Script> = (1..=6)
            .map(|read| (then(children(0..read), reset()), ENDS))
            .collect();
        scripts.push((children(0..8), ENDS));
        let (mut listing, opens) = listing(scripts)?;
        assert_eq!(page(&mut listing, 100).await?.len(), 8);
        assert_eq!(opens.load(Ordering::SeqCst), 7);
        Ok(())
    }

    /// A missing directory fails at once, without a retry and without a status request first.
    #[tokio::test]
    async fn a_missing_directory_is_not_found() -> Result {
        let missing = HdfsError::FileNotFound("/root/d".to_owned());
        let (mut listing, opens) = listing(vec![(vec![Err(missing)], ENDS)])?;
        assert_eq!(
            entry_class(listing.next_page(hint(10)?).await),
            Some(RoleFailureClass::NotFound)
        );
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        Ok(())
    }

    /// The name node answers the listing of a file with the file's own status: the listing is
    /// refused as not a directory, as the status check it replaces refused it.
    #[tokio::test]
    async fn listing_a_file_is_invalid_input() -> Result {
        let (mut listing, _) = listing(vec![(vec![Ok(status("/root/d"))], ENDS)])?;
        assert_eq!(
            entry_class(listing.next_page(hint(10)?).await),
            Some(RoleFailureClass::InvalidInput)
        );
        assert!(matches!(listing.next_page(hint(10)?).await, Ok(None)));
        Ok(())
    }

    /// A status the listing cannot describe fails that child alone, beside its siblings.
    #[tokio::test]
    async fn an_undescribable_status_fails_alone() -> Result {
        let mut statuses = children(0..2);
        statuses.push(Ok(status("/root/d/sub/nested")));
        let (mut listing, _) = listing(vec![(statuses, ENDS)])?;
        let Ok(Some(children)) = listing.next_page(hint(10)?).await else {
            return Err("expected a page".into());
        };
        assert_eq!(children.iter().filter(|child| child.is_err()).count(), 1);
        assert_eq!(children.len(), 3);
        Ok(())
    }
}
