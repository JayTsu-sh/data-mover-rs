//! A key's history longer than one entry carries is delivered in segments (P7 Q8): the same entry
//! several times, each with a part of the history, numbered from 0, only the last complete; the
//! segments of one key come together and in order in both orders; and with checkpoints (P7 C11)
//! a resumed traversal delivers every record, a key's segments again from the first.

use std::collections::{BTreeSet, HashSet};
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::s3_tests::{put, request};
use super::{
    Checkpoint, Resumable, ResumeRefusal, StorageTraversalSource, TraversalItem, TraversalOrder,
    TraversalOutcome, TraversalRequest, TraversalSource, TraversalTerminalFailure,
};
use crate::model::{ObjectHistory, ObservedEntry};
use crate::storage::backends::s3::tests::{MemoryS3, Versioning, identity};
use crate::storage::backends::s3::{S3VersioningAtConnect, connect};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Outcome = Result<TraversalOutcome, TraversalTerminalFailure>;

const ORDERS: [TraversalOrder; 2] = [TraversalOrder::Blocks, TraversalOrder::Unordered];

/// The traversal source of `protocol`, segmenting histories longer than `cap` records.
fn source(protocol: &Arc<MemoryS3>, cap: usize) -> TestResult<StorageTraversalSource> {
    let storage = connect(protocol.clone(), identity(), None)?;
    Ok(StorageTraversalSource::new(&storage)?.history_segment(cap))
}

/// Runs to the end; with `cancel_after`, cancels once that many entries arrived and drains.
async fn run(
    source: &StorageTraversalSource,
    request: TraversalRequest,
    cancel_after: Option<usize>,
) -> TestResult<(Vec<TraversalItem>, Outcome)> {
    let cancel = request.cancel.clone();
    let mut session = source.traverse(request);
    let ran = tokio::time::timeout(Duration::from_secs(60), async {
        let mut items = Vec::new();
        let mut entries = 0;
        while let Some(item) = session.next_item().await {
            entries += usize::from(matches!(item, TraversalItem::Entry(_)));
            items.push(item);
            if cancel_after == Some(entries) {
                cancel.cancel();
            }
        }
        (items, session.finish().await)
    })
    .await?;
    Ok(ran)
}

fn entries(items: &[TraversalItem]) -> Vec<&ObservedEntry> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry.as_ref()),
            _ => None,
        })
        .collect()
}

/// Every (path, version id) the entries' histories hold, or (path, "-") for an entry without.
fn records(items: &[TraversalItem]) -> BTreeSet<(String, String)> {
    let mut all = BTreeSet::new();
    for entry in entries(items) {
        let path = entry.path().to_string();
        match entry.history() {
            Some(history) => all.extend(history.records().iter().map(|record| {
                (
                    path.clone(),
                    record.version_id().unwrap_or("null").to_owned(),
                )
            })),
            None => {
                all.insert((path, "-".to_owned()));
            }
        }
    }
    all
}

fn checkpoints(items: &[TraversalItem]) -> Vec<(usize, Checkpoint)> {
    items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| match item {
            TraversalItem::Checkpoint(checkpoint) => Some((index, (**checkpoint).clone())),
            _ => None,
        })
        .collect()
}

/// `d/a`, `d/big` with seven versions, `d/z`, and `writes` other directories of 30 keys each.
async fn bucket(others: usize) -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "d/a", b"1").await?;
    for _ in 0..7 {
        put(&protocol, "d/big", b"22").await?;
    }
    put(&protocol, "d/z", b"1").await?;
    for directory in 0..others {
        for key in 0..30 {
            put(&protocol, &format!("o{directory}/k{key:02}"), b"1").await?;
        }
    }
    Ok(protocol)
}

/// Seven records at three a segment: three entries of `d/big` — segments 0, 1, 2 of 3, 3 and 1
/// records, only the last complete and naming the current version — one after another in the
/// stream, however the session's buffer and the other directories' workers interleave; every
/// record once, in order; one identity key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_history_comes_in_consecutive_segments() -> TestResult {
    let protocol = bucket(20).await?;
    for order in ORDERS {
        for buffered in [1, 8] {
            let request = request()
                .order(order)
                .max_buffered_items(NonZeroUsize::new(buffered).ok_or("nonzero")?);
            let (items, outcome) = run(&source(&protocol, 3)?, request, None).await?;
            assert!(
                matches!(outcome, Ok(TraversalOutcome::Completed(_))),
                "{outcome:?}"
            );
            let found = entries(&items);
            let at: Vec<usize> = found
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.path().as_str() == "d/big")
                .map(|(index, _)| index)
                .collect();
            assert_eq!(at.len(), 3, "{order:?} {buffered}");
            assert!(
                at.windows(2).all(|pair| pair[1] == pair[0] + 1),
                "{order:?}: {at:?}"
            );
            let shape: Vec<(u32, bool, usize)> = at
                .iter()
                .filter_map(|index| found[*index].history())
                .map(|h| (h.segment(), h.is_complete(), h.records().len()))
                .collect();
            assert_eq!(shape, [(0, false, 3), (1, false, 3), (2, true, 1)]);
            let ids: Vec<String> = at
                .iter()
                .filter_map(|index| found[*index].history())
                .flat_map(|h| {
                    h.records()
                        .iter()
                        .filter_map(|r| r.version_id().map(str::to_owned))
                })
                .collect();
            let mut sorted = ids.clone();
            sorted.sort();
            assert_eq!(ids, sorted, "oldest first");
            assert_eq!(ids.len(), 7);
            let keys: HashSet<_> = at.iter().map(|i| found[*i].identity_key()).collect();
            assert_eq!(keys.len(), 1);
            assert!(found[at[0]].source_version().is_none());
            assert!(found[at[2]].source_version().is_some());
        }
    }
    Ok(())
}

/// Cancelled at any point, Unordered delivers a key's segments together and in order: a batch goes
/// to the session whole, so nothing lands between them. A consumer may still stop part way through
/// the batch (cancelling discards what it has not taken), so what arrives is the first segments —
/// the key's history is whole only once its complete segment arrived.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_unordered_traversal_delivers_all_segments_or_none() -> TestResult {
    let protocol = bucket(6).await?;
    for after in 1..=24 {
        let request = request()
            .order(TraversalOrder::Unordered)
            .max_buffered_items(NonZeroUsize::MIN)
            .cancel(CancellationToken::new());
        let (items, _) = run(&source(&protocol, 3)?, request, Some(after)).await?;
        let found = entries(&items);
        let at: Vec<usize> = found
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.path().as_str() == "d/big")
            .map(|(index, _)| index)
            .collect();
        let numbers: Vec<u32> = at
            .iter()
            .filter_map(|index| found[*index].history())
            .map(ObjectHistory::segment)
            .collect();
        let first: Vec<u32> = (0..3).take(numbers.len()).collect();
        assert_eq!(numbers, first, "after {after}");
        assert!(
            at.windows(2).all(|pair| pair[1] == pair[0] + 1),
            "after {after}: {at:?}"
        );
    }
    Ok(())
}

/// Under the limit nothing is segmented: one entry, segment 0, complete.
#[tokio::test]
async fn a_history_at_the_limit_is_one_entry() -> TestResult {
    let protocol = bucket(0).await?;
    let (items, _) = run(&source(&protocol, 7)?, request(), None).await?;
    let big: Vec<&ObservedEntry> = entries(&items)
        .into_iter()
        .filter(|entry| entry.path().as_str() == "d/big")
        .collect();
    assert_eq!(big.len(), 1);
    let history = big[0].history().ok_or("history")?;
    assert_eq!(
        (
            history.segment(),
            history.is_complete(),
            history.records().len()
        ),
        (0, true, 7)
    );
    Ok(())
}

/// Blocks, one item of buffer: cancelled while `d/big`'s segments are handed over piece by piece,
/// the final checkpoint stays before its block, so the resumed traversal lists the block again and
/// delivers the key from its first segment: together the two runs deliver every record.
#[tokio::test]
async fn a_cancel_among_segments_resumes_from_the_first_segment() -> TestResult {
    let protocol = bucket(0).await?;
    let full = records(&run(&source(&protocol, 3)?, request(), None).await?.0);
    let resumable = || {
        request()
            .max_buffered_items(NonZeroUsize::MIN)
            .resumable(Resumable::On {
                every: Duration::ZERO,
            })
            .cancel(CancellationToken::new())
    };
    // `d`, `d/a`, then `d/big`'s first segment: cancelled with its other segments to hand over.
    let (items, outcome) = run(&source(&protocol, 3)?, resumable(), Some(3)).await?;
    assert!(
        matches!(outcome, Ok(TraversalOutcome::Cancelled)),
        "{outcome:?}"
    );
    let (_, last) = checkpoints(&items).pop().ok_or("a final checkpoint")?;
    let delivered = records(&items);
    assert!(delivered.len() < full.len());
    let (resumed, outcome) =
        run(&source(&protocol, 3)?, resumable().resume_from(last), None).await?;
    assert!(
        matches!(outcome, Ok(TraversalOutcome::Completed(_))),
        "{outcome:?}"
    );
    let first = entries(&resumed)
        .into_iter()
        .find(|entry| entry.path().as_str() == "d/big")
        .and_then(ObservedEntry::history)
        .map(ObjectHistory::segment);
    assert_eq!(first, Some(0), "resumed from the key's first segment");
    let union: BTreeSet<_> = delivered.union(&records(&resumed)).cloned().collect();
    assert_eq!(union, full);
    Ok(())
}

/// Unordered, a checkpoint after any page of a directory gathered whole (2 100 keys: three pages)
/// keeps that directory in the frontier: resumed from any checkpoint, what was delivered before it
/// and the resumed traversal together hold every record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unordered_resumes_a_gathered_directory_whole() -> TestResult {
    let protocol = bucket(4).await?;
    for key in 0..2100 {
        put(&protocol, &format!("w/k{key:04}"), b"1").await?;
    }
    let unordered = || {
        request()
            .order(TraversalOrder::Unordered)
            .resumable(Resumable::On {
                every: Duration::ZERO,
            })
            .cancel(CancellationToken::new())
    };
    let (items, _) = run(&source(&protocol, 3)?, unordered(), None).await?;
    let full = records(&items);
    let taken = checkpoints(&items);
    assert!(taken.len() >= 3, "{}", taken.len());
    let step = (taken.len() / 6).max(1);
    for (index, checkpoint) in taken.into_iter().step_by(step) {
        let (resumed, outcome) = run(
            &source(&protocol, 3)?,
            unordered().resume_from(checkpoint),
            None,
        )
        .await?;
        assert!(
            matches!(outcome, Ok(TraversalOutcome::Completed(_))),
            "{outcome:?}"
        );
        let mut union = records(&items[..index]);
        union.extend(records(&resumed));
        assert_eq!(union, full, "from the checkpoint at item {index}");
    }
    Ok(())
}

/// A checkpoint of a traversal that listed histories is another traversal's for a connection
/// that lists none (the bucket's versioning changed between the runs): refused before any I/O.
#[tokio::test]
async fn a_checkpoint_does_not_resume_across_a_versioning_change() -> TestResult {
    let protocol = bucket(0).await?;
    let resumable = || {
        request()
            .resumable(Resumable::On {
                every: Duration::ZERO,
            })
            .cancel(CancellationToken::new())
    };
    let (items, _) = run(&source(&protocol, 3)?, resumable(), Some(1)).await?;
    let (_, last) = checkpoints(&items).pop().ok_or("a checkpoint")?;
    protocol.answer_at_connect(S3VersioningAtConnect::Never);
    let (resumed, outcome) =
        run(&source(&protocol, 3)?, resumable().resume_from(last), None).await?;
    assert!(resumed.is_empty());
    assert!(
        matches!(
            outcome,
            Err(TraversalTerminalFailure::Resume(ResumeRefusal::Mismatch))
        ),
        "{outcome:?}"
    );
    Ok(())
}
