//! The in-process per-destination guard (ADR-0006): one transfer at a time writes a final file of a
//! destination endpoint in this process. Artifact names are deterministic, so two transfers of one
//! final file would otherwise share — and clean up — each other's stage.

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex, PoisonError};

use crate::model::{BackendIdentity, StoragePath};

/// A final file of one destination endpoint.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DestinationKey {
    endpoint: BackendIdentity,
    final_path: StoragePath,
}

static HELD: LazyLock<Mutex<HashSet<DestinationKey>>> = LazyLock::new(Mutex::default);

/// The right to write one final file, released on drop. The stage holds it for as long as it
/// lives — through verification and publication, or inside a failure that keeps the stage.
#[derive(Debug)]
pub(crate) struct DestinationLease {
    key: DestinationKey,
}

impl Drop for DestinationLease {
    fn drop(&mut self) {
        HELD.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.key);
    }
}

/// Takes the lease on `final_path` of `endpoint`, or `None` while another transfer holds it. Never
/// waits: the caller reports the conflict and may retry later.
pub(crate) fn try_acquire(
    endpoint: &BackendIdentity,
    final_path: &StoragePath,
) -> Option<DestinationLease> {
    let key = DestinationKey {
        endpoint: endpoint.clone(),
        final_path: final_path.clone(),
    };
    // The lock is released before a lease exists: a lease built and dropped under it (as an
    // eager `then_some` would, when the key is taken) deadlocks in `Drop` and would free the
    // holder's key.
    let inserted = HELD
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(key.clone());
    inserted.then(|| DestinationLease { key })
}

#[cfg(test)]
mod tests {
    use super::try_acquire;
    use crate::model::{BackendIdentity, BackendKind, StoragePath};

    // `transfer` never names a backend variant (architecture guard): kinds are parsed.

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn endpoint(id: &str) -> Result<BackendIdentity, Box<dyn std::error::Error>> {
        Ok(BackendIdentity::new("local".parse::<BackendKind>()?, id)?)
    }

    #[test]
    fn one_lease_per_final_file_of_an_endpoint() -> TestResult {
        let a = endpoint("guard-test-a")?;
        let file = StoragePath::new("guard/file.bin")?;
        let held = try_acquire(&a, &file).ok_or("first lease")?;
        assert!(try_acquire(&a, &file).is_none());
        // Another file, or the same path on another endpoint, is another key.
        assert!(try_acquire(&a, &StoragePath::new("guard/other.bin")?).is_some());
        assert!(try_acquire(&endpoint("guard-test-b")?, &file).is_some());
        drop(held);
        assert!(try_acquire(&a, &file).is_some());
        Ok(())
    }
}
