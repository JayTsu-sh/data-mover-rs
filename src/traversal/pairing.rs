//! Pair the direct children of two closed [`super::TraversalOrder::Blocks`] blocks.
//!
//! This is a comparison seam, not a synchronization or deletion engine. Buffer one directory
//! block from each traversal, map their storage paths to the same logical directory, then call
//! [`pair_directory_blocks`]. Children need not arrive in the same order. The result borrows the
//! observations, including every history segment; it performs no I/O and clones no histories.
//!
//! `OnlyLeft` and `OnlyRight` describe these two observations only. Before using absence for
//! deletion, the consumer must finish **both** sessions with [`super::TraversalOutcome::Completed`],
//! exclude every subtree of an ancestor listing failure, and apply the same filter and depth to
//! previously known entries. A pruned or depth-truncated child says nothing about its descendants.
//! A block is not a snapshot: a vanished listing or child can have been recreated meanwhile.
//!
//! Unknown or non-exact namespace name semantics suppress absence conclusions. This helper does
//! not invent a server's Unicode folding rules; a caller needing insensitive-name conflict
//! detection must use the destination's actual equivalence rules before acting on the result.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;

use super::{DirectoryListed, TraversalItem};
use crate::model::{ObservedEntry, StoragePath};
use crate::storage::roles::NameSemantics;

mod input;
#[cfg(test)]
mod tests;

use input::Block;

/// Explicitly maps two storage directories to one logical directory.
#[derive(Clone, Debug)]
pub struct BlockPairingOptions {
    pub logical_directory: StoragePath,
    pub left_directory: StoragePath,
    pub right_directory: StoragePath,
    pub left_names: NameSemantics,
    pub right_names: NameSemantics,
}

impl BlockPairingOptions {
    /// Name semantics default to unknown, so absence remains undetermined.
    #[must_use]
    pub const fn new(logical: StoragePath, left: StoragePath, right: StoragePath) -> Self {
        Self {
            logical_directory: logical,
            left_directory: left,
            right_directory: right,
            left_names: NameSemantics::UNKNOWN,
            right_names: NameSemantics::UNKNOWN,
        }
    }

    /// Supplies name semantics from the two namespace capabilities.
    #[must_use]
    pub const fn with_names(mut self, left: NameSemantics, right: NameSemantics) -> Self {
        self.left_names = left;
        self.right_names = right;
        self
    }
}

/// A byte-exact child name, or the object stored on the directory's own trailing-slash key.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ChildKey {
    /// Kept verbatim: never case-folded, normalized, or stripped of path components.
    Name(String),
    /// `p/a/` in directory `p/a`; distinct from directory `p/a` in parent `p`.
    DirectoryObject,
}

/// One logical entry: either one ordinary observation or all of a key's history segments.
#[derive(Debug)]
pub struct ObservedChild<'a> {
    /// In segment order. A tombstoned key remains present here, with its delete marker.
    pub segments: Vec<&'a ObservedEntry>,
}

/// What was observed at one name. `Matched` does not mean equal content or metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PairingStatus {
    /// Both sides have one entry per kind, with the same kinds.
    Matched,
    /// Present only on the left in these complete, exact-name blocks; not deletion permission.
    OnlyLeft,
    /// Present only on the right in these complete, exact-name blocks; not deletion permission.
    OnlyRight,
    /// Kinds differ, or one side repeated the same logical name and kind.
    Conflict,
    /// A failure, filtering, or name semantics prevents concluding absence.
    Undetermined(AbsenceBarrier),
}

/// Why a local absence cannot be concluded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbsenceBarrier {
    IncompleteListing,
    UnnamedChildFailure,
    ChildFailure,
    FilteredChildren,
    NameSemantics,
}

/// A name's observations, retaining all kinds and duplicates instead of overwriting them.
#[derive(Debug)]
pub struct PairedChild<'a> {
    pub key: ChildKey,
    /// A file `a` and a prefix directory `a` are separate children within this name group.
    pub left: Vec<ObservedChild<'a>>,
    pub right: Vec<ObservedChild<'a>>,
    pub status: PairingStatus,
}

/// The two directory boundaries and their direct-child comparison. Result order is unspecified.
#[derive(Debug)]
pub struct DirectoryPairing<'a> {
    pub logical_directory: StoragePath,
    /// Counts and outcomes remain available, including pruning, truncation and vanished state.
    pub left: &'a DirectoryListed,
    pub right: &'a DirectoryListed,
    pub children: Vec<PairedChild<'a>>,
}

/// Which input block is malformed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PairingSide {
    Left,
    Right,
}

/// Invalid block shape, path mapping, history sequence, or accounting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PairingInputError {
    pub side: PairingSide,
    /// Static diagnostic; no storage credentials or paths are included.
    pub reason: &'static str,
}

impl fmt::Display for PairingInputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} directory block: {}", self.side, self.reason)
    }
}

impl Error for PairingInputError {}

/// Pairs exactly one closed directory block from each traversal.
///
/// Checkpoints may appear anywhere and are ignored. Each slice must contain exactly one final
/// `DirectoryListed` boundary, and every entry must belong directly to that directory. Histories
/// must contain consecutive segments from zero through the complete segment. Repeated complete
/// entries are retained as conflicts, so resumed input must be deduplicated by the consumer.
///
/// # Errors
/// Returns [`PairingInputError`] for malformed or mismapped blocks; never guesses their boundary.
pub fn pair_directory_blocks<'a>(
    left: &'a [TraversalItem],
    right: &'a [TraversalItem],
    options: &BlockPairingOptions,
) -> Result<DirectoryPairing<'a>, PairingInputError> {
    let mut left = Block::read(left, &options.left_directory, PairingSide::Left)?;
    let mut right = Block::read(right, &options.right_directory, PairingSide::Right)?;
    let keys: HashSet<_> = left.keys().chain(right.keys()).cloned().collect();
    let mut children = Vec::with_capacity(keys.len());
    for key in keys {
        let l = left.children.remove(&key).unwrap_or_default();
        let r = right.children.remove(&key).unwrap_or_default();
        let status = status(&key, &l, &r, &left, &right, options);
        children.push(PairedChild {
            key,
            left: l,
            right: r,
            status,
        });
    }
    Ok(DirectoryPairing {
        logical_directory: options.logical_directory.clone(),
        left: left.listed,
        right: right.listed,
        children,
    })
}

fn status(
    key: &ChildKey,
    left: &[ObservedChild<'_>],
    right: &[ObservedChild<'_>],
    lblock: &Block<'_>,
    rblock: &Block<'_>,
    options: &BlockPairingOptions,
) -> PairingStatus {
    if lblock.failures.contains(key) || rblock.failures.contains(key) {
        return PairingStatus::Undetermined(AbsenceBarrier::ChildFailure);
    }
    if duplicates(left) || duplicates(right) {
        return PairingStatus::Conflict;
    }
    if !left.is_empty() && !right.is_empty() {
        return if same_kinds(left, right) {
            PairingStatus::Matched
        } else {
            PairingStatus::Conflict
        };
    }
    let barrier = lblock.barrier().or_else(|| rblock.barrier()).or_else(|| {
        (options.left_names != NameSemantics::EXACT || options.right_names != NameSemantics::EXACT)
            .then_some(AbsenceBarrier::NameSemantics)
    });
    match barrier {
        Some(reason) => PairingStatus::Undetermined(reason),
        None if right.is_empty() => PairingStatus::OnlyLeft,
        None => PairingStatus::OnlyRight,
    }
}

fn duplicates(children: &[ObservedChild<'_>]) -> bool {
    children.iter().enumerate().any(|(index, child)| {
        children[..index]
            .iter()
            .any(|other| other.segments[0].kind() == child.segments[0].kind())
    })
}

fn same_kinds(left: &[ObservedChild<'_>], right: &[ObservedChild<'_>]) -> bool {
    left.len() == right.len()
        && left.iter().all(|l| {
            right
                .iter()
                .any(|r| l.segments[0].kind() == r.segments[0].kind())
        })
}
