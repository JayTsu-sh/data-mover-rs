//! Source-derived standard BLAKE3 evidence (H), independent of destination durability (D).
//!
//! The caller binds this state to one immutable source version, persists it before
//! adoptable writes, and bounds the active window. Sparse observations consume
//! memory proportional to their frontier and incomplete leaf spans, not file size.
//! No state here proves that destination payload exists or is durable.

use std::collections::BTreeMap;
use std::ops::Range;

use blake3::hazmat::{Mode, merge_subtrees_non_root, merge_subtrees_root};

#[path = "range_hash/codec.rs"]
mod codec;
#[path = "range_hash/tree.rs"]
mod tree;

#[cfg(test)]
#[path = "range_hash/tests.rs"]
mod tests;

const CHUNK: u64 = 1024;

/// A mathematical BLAKE3 frontier, never a dump of a private `Hasher` layout.
#[derive(Clone, Debug)]
pub(crate) struct RangeHash {
    size: u64,
    nodes: BTreeMap<u64, Node>,
    // Absolute byte offsets; each owned span stays inside one 1 KiB leaf.
    fragments: BTreeMap<u64, Vec<u8>>,
}

#[derive(Clone, Debug)]
enum Node {
    Full { height: u8, cv: [u8; 32] },
    Tail { len: u16, cv: [u8; 32] },
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RangeHashError {
    #[error("hash observation is outside the fixed source length or overflows")]
    OutOfBounds,
    #[error("hash observation overlaps existing evidence; filter through missing_ranges")]
    Overlap,
    #[error("source hash coverage is incomplete")]
    Incomplete,
    #[error("invalid range hash state: {0}")]
    InvalidState(&'static str),
    #[error("unsupported range hash schema, algorithm, or mode")]
    UnsupportedFormat,
    #[error("range hash metadata checksum mismatch")]
    Checksum,
    #[error("range hash metadata exceeds codec limits")]
    LimitExceeded,
}

impl Node {
    fn len(&self) -> u64 {
        match self {
            Self::Full { height, .. } => CHUNK << height,
            Self::Tail { len, .. } => u64::from(*len),
        }
    }

    fn cv(&self) -> &[u8; 32] {
        match self {
            Self::Full { cv, .. } | Self::Tail { cv, .. } => cv,
        }
    }
}

impl RangeHash {
    pub(crate) fn new(size: u64) -> Self {
        Self {
            size,
            nodes: BTreeMap::new(),
            fragments: BTreeMap::new(),
        }
    }

    /// Admit disjoint source bytes. Any overlap, including an exact duplicate,
    /// fails before mutation: compacted CVs cannot be compared with old payload.
    pub(crate) fn observe(&mut self, offset: u64, bytes: &[u8]) -> Result<(), RangeHashError> {
        let len = u64::try_from(bytes.len()).map_err(|_| RangeHashError::OutOfBounds)?;
        let end = offset.checked_add(len).ok_or(RangeHashError::OutOfBounds)?;
        if end > self.size {
            return Err(RangeHashError::OutOfBounds);
        }
        if len == 0 {
            return Ok(());
        }
        if self.overlaps(offset..end) {
            return Err(RangeHashError::Overlap);
        }
        self.insert_bytes(offset, bytes);
        Ok(())
    }

    pub(crate) fn covers(&self, range: Range<u64>) -> bool {
        range.start <= range.end && range.end <= self.size && self.missing_ranges(range).is_empty()
    }

    /// Exact gaps in H. Out-of-file bytes are missing; reversed ranges are empty.
    /// Observation still requires all offsets to lie within the fixed file size.
    pub(crate) fn missing_ranges(&self, range: Range<u64>) -> Vec<Range<u64>> {
        let mut missing = Vec::new();
        let mut cursor = range.start;
        for span in self.spans_from(range.start) {
            if span.start >= range.end {
                break;
            }
            if span.start > cursor {
                missing.push(cursor..span.start);
            }
            cursor = cursor.max(span.end);
        }
        if cursor < range.end {
            missing.push(cursor..range.end);
        }
        missing
    }

    /// Normalized source hash coverage, independent of backend recovery coverage.
    #[cfg(test)]
    pub(crate) fn coverage(&self) -> Vec<Range<u64>> {
        self.coverage_iter().collect()
    }

    /// Borrowed, allocation-free alternative for repeated checkpoint queries.
    #[cfg(test)]
    pub(crate) fn coverage_iter(&self) -> impl Iterator<Item = Range<u64>> + '_ {
        let mut spans = self.spans_from(0).peekable();
        std::iter::from_fn(move || {
            let mut span = spans.next()?;
            while let Some(next) = spans.peek() {
                if next.start != span.end {
                    break;
                }
                span.end = next.end;
                spans.next();
            }
            Some(span)
        })
    }

    pub(crate) fn finalize(&self) -> Result<[u8; 32], RangeHashError> {
        if !self.covers(0..self.size) {
            return Err(RangeHashError::Incomplete);
        }
        if self.size <= CHUNK {
            let raw = self.fragments.get(&0).map_or(&[][..], Vec::as_slice);
            return Ok(*blake3::hash(raw).as_bytes());
        }
        let mut nodes = self.nodes.values().rev();
        let mut suffix = *nodes.next().ok_or(RangeHashError::Incomplete)?.cv();
        while let Some(node) = nodes.next() {
            if nodes.len() == 0 {
                return Ok(*merge_subtrees_root(node.cv(), &suffix, Mode::Hash).as_bytes());
            }
            suffix = merge_subtrees_non_root(node.cv(), &suffix, Mode::Hash);
        }
        Err(RangeHashError::InvalidState(
            "whole-file CV lost root children",
        ))
    }

    fn overlaps(&self, range: Range<u64>) -> bool {
        self.nodes
            .range(..range.end)
            .next_back()
            .is_some_and(|(&start, node)| start + node.len() > range.start)
            || self
                .fragments
                .range(..range.end)
                .next_back()
                .is_some_and(|(&start, bytes)| start + bytes.len() as u64 > range.start)
    }

    fn spans_from(&self, start: u64) -> impl Iterator<Item = Range<u64>> + '_ {
        let node_start = self
            .nodes
            .range(..=start)
            .next_back()
            .map_or(start, |(&s, _)| s);
        let raw_start = self
            .fragments
            .range(..=start)
            .next_back()
            .map_or(start, |(&s, _)| s);
        let mut nodes = self
            .nodes
            .range(node_start..)
            .map(|(&s, n)| s..s + n.len())
            .peekable();
        let mut raw = self
            .fragments
            .range(raw_start..)
            .map(|(&s, b)| s..s + b.len() as u64)
            .peekable();
        std::iter::from_fn(move || match (nodes.peek(), raw.peek()) {
            (Some(n), Some(r)) if n.start <= r.start => nodes.next(),
            (_, Some(_)) => raw.next(),
            (Some(_), None) => nodes.next(),
            (None, None) => None,
        })
    }
}
