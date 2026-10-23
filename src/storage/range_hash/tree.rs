use blake3::hazmat::{HasherExt, Mode, merge_subtrees_non_root};

use super::{CHUNK, Node, RangeHash, RangeHashError};

impl RangeHash {
    pub(super) fn insert_bytes(&mut self, mut offset: u64, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let len = if self.size > CHUNK && offset.is_multiple_of(CHUNK) && bytes.len() >= 1024 {
                let height = self.bulk_height(offset, bytes.len() as u64);
                let len = 1024_usize << height;
                let cv = hash_subtree(offset, &bytes[..len]);
                self.insert_full(offset, height, cv);
                len
            } else {
                // The remainder is at most 1024, including on 32-bit targets.
                #[allow(clippy::cast_possible_truncation)]
                let leaf_left = (CHUNK - offset % CHUNK) as usize;
                let len = bytes.len().min(leaf_left);
                self.insert_fragment(offset, &bytes[..len]);
                len
            };
            offset += len as u64;
            bytes = &bytes[len..];
        }
    }

    // The byte-length quotient bounds height to at most 53, even for u64::MAX.
    #[allow(clippy::cast_possible_truncation)]
    fn bulk_height(&self, offset: u64, len: u64) -> u8 {
        let mut height = (len / CHUNK).ilog2();
        if offset != 0 {
            height = height.min((offset / CHUNK).trailing_zeros());
        }
        // A power-of-two whole-file CV cannot produce the ROOT compression.
        if offset == 0 && CHUNK << height == self.size {
            height -= 1;
        }
        height as u8
    }

    fn insert_full(&mut self, mut start: u64, mut height: u8, mut cv: [u8; 32]) {
        loop {
            let len = CHUNK << height;
            if len.checked_mul(2) == Some(self.size) && start < len * 2 {
                break;
            }
            let buddy = start ^ len;
            let Some(Node::Full {
                height: other,
                cv: other_cv,
            }) = self.nodes.get(&buddy)
            else {
                break;
            };
            if *other != height {
                break;
            }
            cv = if buddy < start {
                merge_subtrees_non_root(other_cv, &cv, Mode::Hash)
            } else {
                merge_subtrees_non_root(&cv, other_cv, Mode::Hash)
            };
            self.nodes.remove(&buddy);
            start = start.min(buddy);
            height += 1;
        }
        self.nodes.insert(start, Node::Full { height, cv });
    }

    fn insert_fragment(&mut self, mut start: u64, bytes: &[u8]) {
        let leaf_start = start / CHUNK * CHUNK;
        let mut raw = bytes.to_vec();
        if let Some((&prev, data)) = self.fragments.range(leaf_start..start).next_back()
            && prev + data.len() as u64 == start
            && let Some(mut joined) = self.fragments.remove(&prev)
        {
            joined.extend_from_slice(&raw);
            raw = joined;
            start = prev;
        }
        let end = start + raw.len() as u64;
        if !end.is_multiple_of(CHUNK)
            && let Some(next) = self.fragments.remove(&end)
        {
            raw.extend_from_slice(&next);
        }
        let leaf_len = (self.size - leaf_start).min(CHUNK);
        if self.size > CHUNK && start == leaf_start && raw.len() as u64 == leaf_len {
            let cv = hash_subtree(start, &raw);
            if leaf_len == CHUNK {
                self.insert_full(start, 0, cv);
            } else {
                self.nodes.insert(
                    start,
                    Node::Tail {
                        len: leaf_len as u16,
                        cv,
                    },
                );
            }
        } else {
            self.fragments.insert(start, raw);
        }
    }

    /// Validate coordinates and canonical compaction before trusting decoded CVs.
    pub(super) fn validate(&self) -> Result<(), RangeHashError> {
        for (&start, node) in &self.nodes {
            self.validate_node(start, node)?;
        }
        for (&start, raw) in &self.fragments {
            let len = raw.len() as u64;
            let leaf_len = (self.size.saturating_sub(start / CHUNK * CHUNK)).min(CHUNK);
            if len == 0
                || len > CHUNK - start % CHUNK
                || start.checked_add(len).is_none_or(|end| end > self.size)
                || (self.size > CHUNK && start % CHUNK == 0 && len == leaf_len)
            {
                return Err(RangeHashError::InvalidState("invalid raw fragment"));
            }
        }
        self.validate_spans()
    }

    pub(super) fn validate_node(&self, start: u64, node: &Node) -> Result<(), RangeHashError> {
        let valid = match node {
            Node::Full { height, .. } if *height <= 53 => {
                let len = CHUNK << height;
                start.is_multiple_of(len)
                    && len < self.size
                    && start.checked_add(len).is_some_and(|end| end <= self.size)
                    && !self.mergeable_buddy(start, *height)
            }
            Node::Tail { len, .. } => {
                *len > 0
                    && u64::from(*len) == self.size % CHUNK
                    && start == self.size / CHUNK * CHUNK
            }
            Node::Full { .. } => false,
        };
        if self.size <= CHUNK || !start.is_multiple_of(CHUNK) || !valid {
            return Err(RangeHashError::InvalidState(
                "invalid or noncanonical subtree",
            ));
        }
        Ok(())
    }

    fn mergeable_buddy(&self, start: u64, height: u8) -> bool {
        let len = CHUNK << height;
        if len.checked_mul(2) == Some(self.size) {
            return false;
        }
        matches!(self.nodes.get(&(start ^ len)), Some(Node::Full { height: h, .. }) if *h == height)
    }

    fn validate_spans(&self) -> Result<(), RangeHashError> {
        let mut previous_end = 0;
        let mut previous_raw_leaf = None;
        for span in self.spans_from(0) {
            if span.start < previous_end {
                return Err(RangeHashError::InvalidState("overlapping records"));
            }
            let raw_leaf = self
                .fragments
                .contains_key(&span.start)
                .then_some(span.start / CHUNK);
            if span.start == previous_end && raw_leaf.is_some() && raw_leaf == previous_raw_leaf {
                return Err(RangeHashError::InvalidState("uncoalesced fragments"));
            }
            previous_end = span.end;
            previous_raw_leaf = raw_leaf;
        }
        Ok(())
    }
}

fn hash_subtree(offset: u64, bytes: &[u8]) -> [u8; 32] {
    blake3::Hasher::new()
        .set_input_offset(offset)
        .update(bytes)
        .finalize_non_root()
}
