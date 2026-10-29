//! The pool on its own: the frontier invariant at every move (checked by the pool itself under
//! test, at every gap between critical sections), shallowest-first stealing, and termination
//! under many concurrent workers.

use std::sync::atomic::AtomicU64;
use std::time::Duration;

use super::*;
use crate::model::StoragePath;

fn work(path: &str) -> DirectoryWork {
    let child_depth = if path.is_empty() {
        1
    } else {
        path.split('/').count() + 1
    };
    DirectoryWork {
        path: StoragePath::new(path).unwrap_or_else(|error| panic!("{error}")),
        child_depth,
        filter_children: false,
        hint: None,
    }
}

fn paths(works: &[Arc<DirectoryWork>]) -> Vec<String> {
    let mut paths: Vec<_> = works
        .iter()
        .map(|work| work.path.as_str().to_owned())
        .collect();
    paths.sort();
    paths
}

/// A directory is in the frontier from its push until its finish, on a stack or active, through
/// pops and steals; a finished one leaves it.
#[tokio::test]
async fn the_frontier_holds_every_unfinished_directory_through_pops_and_steals() {
    let pool = Pool::new(2, vec![work("")]);
    let halt = CancellationToken::new();
    let root = pool.take(0, &halt).await.unwrap_or_else(|| panic!("root"));
    assert_eq!(root.path.as_str(), "");
    assert_eq!(paths(&pool.frontier()), [""]);
    pool.push(0, vec![work("a"), work("b"), work("c")]);
    assert_eq!(paths(&pool.frontier()), ["", "a", "b", "c"]);
    assert!(!pool.finish(0));
    assert_eq!(paths(&pool.frontier()), ["a", "b", "c"]);
    // Worker 1 has nothing of its own: it steals worker 0's oldest.
    let stolen = pool.take(1, &halt).await.unwrap_or_else(|| panic!("steal"));
    assert_eq!(stolen.path.as_str(), "a");
    // Worker 0 pops its own newest.
    let own = pool.take(0, &halt).await.unwrap_or_else(|| panic!("pop"));
    assert_eq!(own.path.as_str(), "c");
    assert_eq!(paths(&pool.frontier()), ["a", "b", "c"]);
    for me in [0, 1] {
        assert!(!pool.finish(me));
    }
    assert_eq!(paths(&pool.frontier()), ["b"]);
    let last = pool.take(1, &halt).await.unwrap_or_else(|| panic!("steal"));
    assert_eq!(last.path.as_str(), "b");
    assert!(pool.finish(1), "the last directory finished");
    assert!(pool.drained());
    assert!(pool.frontier().is_empty());
}

/// A thief takes the shallowest directory any other worker holds, not the first it finds.
#[tokio::test]
async fn a_thief_takes_the_shallowest_directory_held() {
    let pool = Pool::new(3, vec![work("")]);
    let halt = CancellationToken::new();
    let _root = pool.take(0, &halt).await;
    pool.push(0, vec![work("a/b/c")]);
    let _deep = pool.take(2, &halt).await;
    pool.push(2, vec![work("x")]);
    pool.push(0, vec![work("a/b/d/e")]);
    // Worker 0 holds a depth-5 directory, worker 2 a depth-2 one.
    let stolen = pool.take(1, &halt).await.unwrap_or_else(|| panic!("steal"));
    assert_eq!(stolen.path.as_str(), "x");
}

/// An idle worker wakes for a push, and every worker returns `None` once the pool halts.
#[tokio::test]
async fn an_idle_worker_wakes_for_a_push_and_for_the_halt() {
    let pool = Arc::new(Pool::new(3, vec![work("")]));
    let halt = CancellationToken::new();
    let _root = pool.take(0, &halt).await;
    let waiting = {
        let (pool, halt) = (Arc::clone(&pool), halt.clone());
        tokio::spawn(async move { pool.take(1, &halt).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished());
    pool.push(0, vec![work("a")]);
    let woken = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap_or_else(|_| panic!("the idle worker was not woken"))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        woken.map(|work| work.path.as_str().to_owned()),
        Some("a".into())
    );
    let idle = {
        let (pool, halt) = (Arc::clone(&pool), halt.clone());
        tokio::spawn(async move { pool.take(2, &halt).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    halt.cancel();
    let ended = tokio::time::timeout(Duration::from_secs(5), idle)
        .await
        .unwrap_or_else(|_| panic!("the halt did not wake the idle worker"))
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(ended.is_none());
}

/// Many workers over a synthetic tree: every directory is taken exactly once and the pool drains
/// exactly when the last one finishes — never early, which would lose a subtree, and never not
/// at all, which would hang. Children are pushed before the parent finishes, as a worker does.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn many_workers_take_every_directory_once_and_drain() {
    // Fan-out 4, six levels: 1365 directories.
    const FANOUT: usize = 4;
    const DEPTH: usize = 6;
    let expected: u64 = (0..DEPTH)
        .scan(1_u64, |width, _| {
            let level = *width;
            *width *= FANOUT as u64;
            Some(level)
        })
        .sum();
    for round in 0..20 {
        let workers = [1, 3, 8, 16][round % 4];
        let mut pool = Pool::new(workers, vec![work("")]);
        // Every other round without the frontier check, which locks every slot between two
        // moves: those rounds keep the interleavings of `take` / `push` a real traversal has.
        pool.checked = round % 2 == 0;
        let pool = Arc::new(pool);
        let halt = CancellationToken::new();
        let taken = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::new();
        for me in 0..workers {
            let (pool, halt, taken) = (Arc::clone(&pool), halt.clone(), Arc::clone(&taken));
            tasks.push(tokio::spawn(async move {
                while let Some(directory) = pool.take(me, &halt).await {
                    taken.fetch_add(1, Ordering::Relaxed);
                    if directory.child_depth < DEPTH {
                        let children = (0..FANOUT)
                            .map(|index| {
                                let prefix = directory.path.as_str();
                                work(&if prefix.is_empty() {
                                    format!("d{index}")
                                } else {
                                    format!("{prefix}/d{index}")
                                })
                            })
                            .collect();
                        pool.push(me, children);
                    }
                    tokio::task::yield_now().await;
                    if pool.finish(me) {
                        halt.cancel();
                    }
                }
            }));
        }
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(20), task)
                .await
                .unwrap_or_else(|_| panic!("round {round}: a worker never ended"))
                .unwrap_or_else(|error| panic!("{error}"));
        }
        assert_eq!(taken.load(Ordering::Relaxed), expected, "round {round}");
        assert!(pool.drained());
        assert!(pool.frontier().is_empty());
    }
}

/// The check is not vacuous: a pop that registers its directory in a second critical section
/// (P7 §14 "先出栈后登记") leaves it out of the frontier at the gap, and that fails at once, with
/// no concurrent snapshot needed to land in the gap.
#[test]
#[should_panic(expected = "left the frontier while unfinished: [\"\"]")]
fn a_pop_registered_in_a_second_critical_section_is_caught() {
    let pool = Pool::new(1, vec![work("")]);
    let _ = pool.pop_split(0);
}
