//! Coalesce concurrent durability barriers for the same directory inode.
//!
//! A caller arriving after a sync started joins the *next* round: that earlier sync cannot
//! acknowledge its rename. There is no timer and no success before the caller's round completes.
#[cfg(unix)]
use std::collections::HashMap;
use std::io;
#[cfg(unix)]
use std::sync::Weak;
#[cfg(any(unix, test))]
use std::sync::{Arc, Condvar, Mutex};

#[derive(Default)]
pub(super) struct DirectorySync {
    #[cfg(unix)]
    groups: Mutex<HashMap<(u64, u64), Weak<Group>>>,
}

#[cfg(any(unix, test))]
#[derive(Default)]
struct Group {
    state: Mutex<State>,
}

#[cfg(any(unix, test))]
#[derive(Default)]
struct State {
    running: bool,
    pending: Option<Arc<Round>>,
}

#[cfg(any(unix, test))]
#[derive(Default)]
struct Round {
    state: Mutex<RoundState>,
    done: Condvar,
}

#[cfg(any(unix, test))]
#[derive(Default)]
enum RoundState {
    #[default]
    Waiting,
    Ready,
    Finished(Result<(), Arc<io::Error>>),
}

impl DirectorySync {
    /// Reuse the identity already checked by publication, avoiding another stat.
    #[cfg(unix)]
    pub(super) fn sync(
        &self,
        file: cap_std::fs::File,
        metadata: &cap_std::fs::Metadata,
    ) -> io::Result<()> {
        use cap_std::fs::MetadataExt as _;
        let file = file.into_std();
        let key = (metadata.dev(), metadata.ino());
        let group = {
            let mut groups = self
                .groups
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if groups.len() >= 128 {
                groups.retain(|_, group| group.strong_count() > 0);
            }
            if let Some(group) = groups.get(&key).and_then(Weak::upgrade) {
                group
            } else {
                let group = Arc::new(Group::default());
                groups.insert(key, Arc::downgrade(&group));
                group
            }
        };
        group.sync(|| file.sync_all())
    }

    #[cfg(not(unix))]
    #[allow(clippy::unused_self)]
    pub(super) fn sync(&self, file: cap_std::fs::File) -> io::Result<()> {
        file.into_std().sync_all()
    }
}

#[cfg(any(unix, test))]
impl Group {
    fn sync(&self, mut sync: impl FnMut() -> io::Result<()>) -> io::Result<()> {
        let (round, leader) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.running {
                if let Some(round) = &state.pending {
                    (Arc::clone(round), false)
                } else {
                    let round = Arc::new(Round::default());
                    state.pending = Some(Arc::clone(&round));
                    (round, true)
                }
            } else {
                state.running = true;
                (
                    Arc::new(Round {
                        state: Mutex::new(RoundState::Ready),
                        ..Round::default()
                    }),
                    true,
                )
            }
        };
        if leader {
            let mut status = round
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while matches!(*status, RoundState::Waiting) {
                status = round
                    .done
                    .wait(status)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            drop(status);
            let result = sync().map_err(Arc::new);
            *round
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = RoundState::Finished(result);
            round.done.notify_all();

            // Hand off to a caller in the pending round. This caller never executes or
            // waits for later rounds after its own durability barrier has completed.
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(next) = state.pending.take() {
                *next
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = RoundState::Ready;
                next.done.notify_all();
            } else {
                state.running = false;
            }
        }
        let mut status = round
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            match &*status {
                RoundState::Finished(Ok(())) => return Ok(()),
                RoundState::Finished(Err(error)) => {
                    return Err(io::Error::new(error.kind(), error.to_string()));
                }
                RoundState::Waiting | RoundState::Ready => {
                    status = round
                        .done
                        .wait(status)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    #[test]
    fn arrivals_during_sync_share_the_next_round_and_keep_their_own_result() {
        let group = Arc::new(Group::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let leader_group = Arc::clone(&group);
        let leader_calls = Arc::clone(&calls);
        let leader = std::thread::spawn(move || {
            leader_group.sync(|| {
                let call = leader_calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    started_tx.send(()).map_err(io::Error::other)?;
                    release_rx.recv().map_err(io::Error::other)?;
                    Err(io::Error::from(io::ErrorKind::PermissionDenied))
                } else {
                    Ok(())
                }
            })
        });
        assert!(started_rx.recv_timeout(Duration::from_secs(3)).is_ok());
        let followers: Vec<_> = (0..2)
            .map(|_| {
                let group = Arc::clone(&group);
                let calls = Arc::clone(&calls);
                std::thread::spawn(move || {
                    group.sync(|| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                })
            })
            .collect();
        let deadline = Instant::now() + Duration::from_secs(3);
        let queued = loop {
            if group
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pending
                .as_ref()
                .is_some_and(|round| Arc::strong_count(round) == 3)
            {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            std::thread::yield_now();
        };
        assert!(release_tx.send(()).is_ok());
        let result = leader.join().unwrap_or_else(|_| panic!("leader panicked"));
        assert_eq!(
            result.err().map(|error| error.kind()),
            Some(io::ErrorKind::PermissionDenied)
        );
        for follower in followers {
            assert!(
                follower
                    .join()
                    .unwrap_or_else(|_| panic!("follower panicked"))
                    .is_ok()
            );
        }
        assert!(queued, "followers failed to join the pending round");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

#[cfg(test)]
mod handoff_tests {
    use super::*;
    use std::sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::time::{Duration, Instant};

    #[test]
    fn leader_should_return_after_its_own_round() {
        let group = Arc::new(Group::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let gates = std::array::from_fn::<_, 3, _>(|_| Arc::new(Barrier::new(2)));
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let spawn = |id| {
            let group = Arc::clone(&group);
            let calls = Arc::clone(&calls);
            let gates = gates.clone();
            let started_tx = started_tx.clone();
            let done_tx = done_tx.clone();
            std::thread::spawn(move || {
                let result = group.sync(|| {
                    let round = calls.fetch_add(1, Ordering::SeqCst);
                    started_tx.send(round).map_err(io::Error::other)?;
                    gates[round].wait();
                    Ok(())
                });
                assert!(result.is_ok());
                assert!(done_tx.send(id).is_ok());
            })
        };
        let leader = spawn(0);
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(3)).ok(),
            Some(0)
        );
        let follower = spawn(1);
        let deadline = Instant::now() + Duration::from_secs(3);
        while group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .is_none()
        {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        gates[0].wait();
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(3)).ok(),
            Some(1)
        );
        let leader_returned = done_rx.recv_timeout(Duration::from_secs(3)).ok() == Some(0);
        let third = spawn(2);
        let deadline = Instant::now() + Duration::from_secs(3);
        while group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .is_none()
        {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        // A late arrival cannot be acknowledged by the still-running second barrier.
        assert!(started_rx.try_recv().is_err());
        assert!(done_rx.try_recv().is_err());
        gates[1].wait();
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(3)).ok(),
            Some(2)
        );
        let follower_returned = done_rx.recv_timeout(Duration::from_secs(3)).ok() == Some(1);
        gates[2].wait();
        assert!(third.join().is_ok());
        assert!(leader.join().is_ok());
        assert!(follower.join().is_ok());
        assert!(
            leader_returned,
            "own sync completed, but leader waited for another caller's round"
        );
        assert!(follower_returned, "second round waited for the third round");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(
            group.sync(|| Ok(())).is_ok(),
            "group must become idle again"
        );
    }
}
