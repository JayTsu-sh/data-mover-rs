//! What the traversal holds of a paged listing (P7 C4, §6.2 / §6.3): a directory of any width is
//! held a few pages at a time, and a prefetched listing stops once it has handed over its first
//! page. These bounds are asserted exactly, for tokio's current-thread test runtime (see each
//! test), so a change that holds one page more than documented turns them red.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use tokio::task::yield_now;

use super::streaming::paged_request;
use super::*;
use crate::storage::PageSink;

/// One directory of `total` files, listed in pages of `page` built on demand, counting every
/// child it has produced.
struct WideListing {
    total: usize,
    page: usize,
    produced: Arc<AtomicUsize>,
}

#[async_trait]
impl Namespace for WideListing {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        let (total, size, produced) = (self.total, self.page, Arc::clone(&self.produced));
        ListingStream::from_producer(directory.clone(), move |sink: PageSink| async move {
            let mut next = 0;
            while next < total {
                let end = (next + size).min(total);
                let children = (next..end)
                    .map(|index| descriptor(&format!("f{index:06}"), EntryKind::File))
                    .collect();
                produced.fetch_add(end - next, Ordering::SeqCst);
                let page = ListingPage::from_descriptors(&directory, children, Vec::new(), 0);
                if sink.send(page).await.is_err() {
                    return;
                }
                next = end;
            }
        })
    }
}

/// A directory of a hundred thousand files in pages of a hundred: at no point has the listing
/// produced more than a few pages beyond what the consumer has received, and the cursor never
/// holds more than a few pages of it. Collecting the listing whole before admitting it would
/// produce all of it before the first entry arrives.
///
/// The bound is exact for this test's runtime. The page being admitted, the one handed over behind
/// it, and the producer's own two (one in its channel, one built and being sent): four pages, plus
/// what the window (4) and the session (1) hold. On this current-thread runtime the producer cannot
/// refill its channel while the task prepares a page, so the task's peek finds nothing and it
/// holds no page while it waits for room (on a multi-thread runtime a peek can hold one more).
/// A task that took and prepared its next page before waiting for room is one page over (504).
#[tokio::test]
async fn a_wide_directory_is_held_a_few_pages_at_a_time() {
    const TOTAL: usize = 100_000;
    const PAGE: usize = 100;
    let produced = Arc::new(AtomicUsize::new(0));
    let (source, peak) = StorageTraversalSource::with_roles_probed(
        Arc::new(WideListing {
            total: TOTAL,
            page: PAGE,
            produced: Arc::clone(&produced),
        }),
        Arc::new(FakeMetadata),
    );
    let mut session = source
        .output_capacity(NonZeroUsize::MIN)
        .traverse(paged_request(4));
    let (mut received, mut ahead) = (0_usize, 0_usize);
    while let Some(item) = session.next_item().await {
        received += usize::from(matches!(item, TraversalItem::Entry(_)));
        ahead = ahead.max(produced.load(Ordering::SeqCst).saturating_sub(received));
        if received % 1_000 == 0 {
            // Let the pipeline fill up behind a slow consumer.
            for _ in 0..64 {
                yield_now().await;
            }
        }
    }
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
    assert_eq!(received, TOTAL);
    assert!(
        ahead <= 4 * PAGE + 4 + 1,
        "the listing ran {ahead} children ahead of the consumer"
    );
    let peak = peak.peak();
    assert!(
        peak.block_children + peak.prefetched_children <= 3 * PAGE,
        "{peak:?}"
    );
    assert!(peak.pages_buffered <= 2, "{peak:?}");
}

/// Names every child of a directory `d<n>` it lists, in pages of `page`, counting what it has
/// produced per directory; the root holds `directories` such directories.
struct ManyWide {
    directories: usize,
    pages: usize,
    page: usize,
    produced: Arc<Mutex<HashMap<String, usize>>>,
}

#[async_trait]
impl Namespace for ManyWide {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        if directory.as_str().is_empty() {
            let children = (0..self.directories)
                .map(|index| descriptor(&format!("d{index:02}"), EntryKind::Directory))
                .collect();
            return entries(&directory, children);
        }
        let (pages, size, produced) = (self.pages, self.page, Arc::clone(&self.produced));
        ListingStream::from_producer(directory.clone(), move |sink: PageSink| async move {
            for page in 0..pages {
                let children = (0..size)
                    .map(|index| {
                        let name = format!("{}/f{:05}", directory.as_str(), page * size + index);
                        descriptor(&name, EntryKind::File)
                    })
                    .collect();
                *produced
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .entry(directory.as_str().to_owned())
                    .or_default() += size;
                let page = ListingPage::from_descriptors(&directory, children, Vec::new(), 0);
                if sink.send(page).await.is_err() {
                    return;
                }
            }
        })
    }
}

/// Many wide sibling directories and a slow consumer: prefetch starts well ahead, and every
/// listing the cursor has not reached stops after its first page — never more than `2P` of them
/// are parked, and none has produced more than the few pages a stopped listing may hold, however
/// many it has.
#[tokio::test]
async fn prefetched_listings_stop_after_their_first_page() {
    const PAGE: usize = 50;
    const WINDOW: usize = 4;
    let produced = Arc::new(Mutex::new(HashMap::new()));
    let (source, peak) = StorageTraversalSource::with_roles_probed(
        Arc::new(ManyWide {
            directories: 12,
            pages: 10,
            page: PAGE,
            produced: Arc::clone(&produced),
        }),
        Arc::new(FakeMetadata),
    );
    let mut session = source
        .output_capacity(NonZeroUsize::MIN)
        .traverse(paged_request(WINDOW));
    let mut received: HashMap<String, usize> = HashMap::new();
    let mut ahead = 0;
    while let Some(item) = session.next_item().await {
        if let TraversalItem::Entry(entry) = &item
            && let Some((directory, _)) = entry.path().as_str().split_once('/')
        {
            *received.entry(directory.to_owned()).or_default() += 1;
        }
        for _ in 0..8 {
            yield_now().await;
        }
        let produced = produced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for (directory, count) in produced {
            let got = received.get(&directory).copied().unwrap_or_default();
            ahead = ahead.max(count.saturating_sub(got));
        }
    }
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
    assert_eq!(received.values().sum::<usize>(), 12 * 10 * PAGE);
    // The listing being admitted is at most four pages ahead on this runtime (see the
    // wide-directory test), a parked one three: its handed-over page and the producer's two.
    // Preparing the next page before waiting for room makes it 255.
    assert!(ahead <= 4 * PAGE + WINDOW + 1, "{ahead} ahead");
    let peak = peak.peak();
    assert!(peak.parked_listings > 1, "prefetch never parked: {peak:?}");
    assert!(peak.parked_listings <= 2 * WINDOW, "{peak:?}");
}
