//! What a checkpoint is made of, and when it is due: the frontier without descendants, the
//! Blocks position records, and the one-per-period claim on a manual clock.

use std::time::Duration;

use super::positions::{Level, Positions};
use super::*;
use crate::traversal::{Resumable, TraversalRequest};

fn work(path: &str, child_depth: usize) -> DirectoryWork {
    DirectoryWork {
        path: StoragePath::new(path).unwrap_or_else(|error| panic!("{error}")),
        child_depth,
        filter_children: false,
        hint: None,
    }
}

fn paths(entries: &[FrontierEntry]) -> Vec<&str> {
    entries.iter().map(|entry| entry.path.as_str()).collect()
}

fn identity() -> BackendIdentity {
    crate::storage::backends::local::test_identity("resume-test")
}

/// A directory whose ancestor is also waiting is left out (listing the ancestor finds it again),
/// duplicates collapse, and ancestry is by component: `a-b` is no child of `a`.
#[test]
fn the_frontier_keeps_only_directories_without_a_waiting_ancestor() {
    let works = [
        work("a/x/y", 4),
        work("a", 2),
        work("a-b", 2),
        work("a/x", 3),
        work("b/c", 3),
        work("b/c", 3),
        work("ab/c", 3),
    ];
    assert_eq!(
        paths(&frontier_entries(&works)),
        ["a", "a-b", "ab/c", "b/c"]
    );
    // The storage root waiting covers everything.
    let works = [work("", 1), work("a", 2), work("b/c", 3)];
    assert_eq!(paths(&frontier_entries(&works)), [""]);
    // A root spelled with a trailing `/` lists its children as `x//a`.
    let works = [work("x/", 1), work("x//a", 2)];
    assert_eq!(paths(&frontier_entries(&works)), ["x/"]);
}

/// Records are kept from the latest one at or before what was delivered; the checkpoint is that
/// one, never a newer one whose earlier items are still in flight.
#[test]
fn a_position_is_used_only_once_everything_before_it_is_delivered() {
    let root = Level::enter(None, &work("", 1));
    let a = Level::enter(Some(&root), &work("a", 2));
    let b = Level::enter(Some(&a), &work("a/b", 3));
    let mut positions = Positions::default();
    positions.record(0, Some(Arc::clone(&root)));
    positions.record(5, Some(Arc::clone(&a)));
    positions.record(9, Some(b));
    let chain = |positions: &Positions, delivered| {
        positions.latest(delivered).map(|chain| {
            paths(&chain)
                .iter()
                .map(|path| (*path).to_owned())
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(chain(&positions, 4), Some(vec![String::new()]));
    assert_eq!(chain(&positions, 5), Some(vec![String::new(), "a".into()]));
    assert_eq!(
        chain(&positions, 100),
        Some(vec![String::new(), "a".into(), "a/b".into()])
    );
    positions.prune(6);
    assert_eq!(positions.len(), 2, "the record at 0 is no longer needed");
    positions.prune(9);
    assert_eq!(positions.len(), 1);
    // The end of the traversal: nothing left.
    positions.record(12, None);
    assert_eq!(positions.latest(12), Some(Vec::new()));
    assert_eq!(
        chain(&positions, 11),
        Some(vec![String::new(), "a".into(), "a/b".into()])
    );
}

fn resume(request: &TraversalRequest, clock: &Arc<ManualClock>) -> Resume {
    Resume::prepare(request, &identity(), &[0], || {
        Clock::Manual(Arc::clone(clock))
    })
    .unwrap_or_else(|refusal| panic!("{refusal}"))
    .unwrap_or_else(|| panic!("checkpoints are on"))
}

/// One checkpoint per period however many callers see it due; none before the first period.
#[test]
fn one_caller_claims_each_period() {
    let clock = Arc::new(ManualClock::default());
    let every = Duration::from_mins(1);
    let request = TraversalRequest::new(StoragePath::root()).resumable(Resumable::On { every });
    let resume = resume(&request, &clock);
    assert!(!resume.claim_due());
    clock.advance(Duration::from_secs(59));
    assert!(!resume.claim_due());
    clock.advance(Duration::from_secs(1));
    assert!(resume.claim_due());
    assert!(!resume.claim_due(), "claimed once per period");
    clock.advance(Duration::from_secs(150));
    assert!(resume.claim_due());
    assert!(!resume.claim_due());
    // The next period counts from the claim, not from a fixed grid.
    clock.advance(Duration::from_secs(59));
    assert!(!resume.claim_due());
    clock.advance(Duration::from_secs(1));
    assert!(resume.claim_due());
}

/// Off, and not resuming: nothing at all, not even a clock.
#[test]
fn off_prepares_nothing_and_reads_no_clock() {
    let request = TraversalRequest::new(StoragePath::root());
    let prepared = Resume::prepare(&request, &identity(), &[0], || {
        panic!("Off asked for a clock")
    });
    assert!(matches!(prepared, Ok(None)));
    // Resuming with checkpoints off: a start, no clock, nothing due.
    let clock = Arc::new(ManualClock::default());
    let first = resume(
        &TraversalRequest::new(StoragePath::root()).resumable(Resumable::on()),
        &clock,
    );
    let saved = first.checkpoint(&[entry_of(&work("", 1))]);
    let Some(TraversalItem::Checkpoint(saved)) = saved else {
        panic!("a checkpoint within the limits")
    };
    let request = TraversalRequest::new(StoragePath::root()).resume_from(*saved);
    let prepared = Resume::prepare(&request, &identity(), &[0], || {
        panic!("Off asked for a clock")
    })
    .unwrap_or_else(|refusal| panic!("{refusal}"))
    .unwrap_or_else(|| panic!("resuming"));
    assert!(!prepared.emits());
    assert!(!prepared.claim_due());
    assert_eq!(prepared.start.as_deref().map(<[_]>::len), Some(1));
    assert_eq!(clock.reads(), 0);
}
