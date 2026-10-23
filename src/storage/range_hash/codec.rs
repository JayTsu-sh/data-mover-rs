//! Version 1, unsigned little-endian integers, no native layout or Hasher state.
//!
//! ```text
//! Header: magic[8], version:u16, algorithm:u8, mode:u8, reserved:u32,
//! source_size:u64, node_count:u32, fragment_count:u32.
//! Full node: tag=1:u8, start_chunk:u64, height:u8, non-root CV[32].
//! Short EOF leaf: tag=2:u8, start_chunk:u64, actual_length:u16, CV[32].
//! Fragment: tag=3:u8, absolute_offset:u64, length:u16, source_bytes[length].
//! Nodes and then fragments are strictly ordered, maximally compact/coalesced.
//! Trailer: domain-separated BLAKE3 checksum[32] of all preceding bytes.
//! ```
//! The checksum detects corruption; source/stage binding and authentication
//! belong to the enclosing checkpoint, as do persistence ordering and D.

use super::{CHUNK, Node, RangeHash, RangeHashError};

const MAGIC: &[u8; 8] = b"DMRHASH1";
const VERSION: u16 = 1;
const MAX_BLOB: usize = 64 * 1024 * 1024;
const MAX_RECORDS: usize = 1_000_000;
const HEADER: usize = 32;
const CHECKSUM: usize = 32;
const CHECKSUM_DOMAIN: &str = "data-mover RangeHash metadata checksum v1";

impl RangeHash {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, RangeHashError> {
        self.validate()?;
        let len = self.encoded_len()?;
        let mut out = Vec::new();
        out.try_reserve_exact(len)
            .map_err(|_| RangeHashError::LimitExceeded)?;
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&[1, 0]); // Standard BLAKE3, unkeyed file hashing.
        out.extend_from_slice(&0_u32.to_le_bytes());
        out.extend_from_slice(&self.size.to_le_bytes());
        let nodes = u32::try_from(self.nodes.len()).map_err(|_| RangeHashError::LimitExceeded)?;
        let fragments =
            u32::try_from(self.fragments.len()).map_err(|_| RangeHashError::LimitExceeded)?;
        out.extend_from_slice(&nodes.to_le_bytes());
        out.extend_from_slice(&fragments.to_le_bytes());
        for (&start, node) in &self.nodes {
            out.push(match node {
                Node::Full { .. } => 1,
                Node::Tail { .. } => 2,
            });
            out.extend_from_slice(&(start / CHUNK).to_le_bytes());
            match node {
                Node::Full { height, .. } => out.push(*height),
                Node::Tail { len, .. } => out.extend_from_slice(&len.to_le_bytes()),
            }
            out.extend_from_slice(node.cv());
        }
        for (&start, raw) in &self.fragments {
            out.push(3);
            out.extend_from_slice(&start.to_le_bytes());
            let len = u16::try_from(raw.len()).map_err(|_| RangeHashError::LimitExceeded)?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(raw);
        }
        let checksum = metadata_checksum(&out);
        out.extend_from_slice(&checksum);
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8], expected_size: u64) -> Result<Self, RangeHashError> {
        if bytes.len() > MAX_BLOB {
            return Err(RangeHashError::LimitExceeded);
        }
        if bytes.len() < HEADER + CHECKSUM {
            return Err(RangeHashError::InvalidState("truncated header"));
        }
        let (payload, checksum) = bytes.split_at(bytes.len() - CHECKSUM);
        if metadata_checksum(payload) != checksum {
            return Err(RangeHashError::Checksum);
        }
        let mut reader = Reader { bytes: payload };
        let (mut state, node_count, fragment_count) = read_header(&mut reader, expected_size)?;
        for _ in 0..node_count {
            read_node(&mut reader, &mut state)?;
        }
        for _ in 0..fragment_count {
            read_fragment(&mut reader, &mut state)?;
        }
        if !reader.bytes.is_empty() {
            return Err(RangeHashError::InvalidState("trailing bytes"));
        }
        state.validate()?;
        Ok(state)
    }

    fn encoded_len(&self) -> Result<usize, RangeHashError> {
        let records = self
            .nodes
            .len()
            .checked_add(self.fragments.len())
            .ok_or(RangeHashError::LimitExceeded)?;
        if records > MAX_RECORDS {
            return Err(RangeHashError::LimitExceeded);
        }
        let mut len = HEADER + CHECKSUM;
        for node in self.nodes.values() {
            len = len
                .checked_add(match node {
                    Node::Full { .. } => 42,
                    Node::Tail { .. } => 43,
                })
                .ok_or(RangeHashError::LimitExceeded)?;
        }
        for raw in self.fragments.values() {
            len = len
                .checked_add(11 + raw.len())
                .ok_or(RangeHashError::LimitExceeded)?;
        }
        if len > MAX_BLOB {
            return Err(RangeHashError::LimitExceeded);
        }
        Ok(len)
    }
}

fn read_header(
    reader: &mut Reader<'_>,
    expected_size: u64,
) -> Result<(RangeHash, usize, usize), RangeHashError> {
    if &reader.array::<8>()? != MAGIC
        || reader.u16()? != VERSION
        || reader.u8()? != 1
        || reader.u8()? != 0
        || reader.u32()? != 0
    {
        return Err(RangeHashError::UnsupportedFormat);
    }
    let size = reader.u64()?;
    if size != expected_size {
        return Err(RangeHashError::InvalidState("source length mismatch"));
    }
    let nodes = reader.u32()? as usize;
    let fragments = reader.u32()? as usize;
    let records = nodes
        .checked_add(fragments)
        .ok_or(RangeHashError::LimitExceeded)?;
    // Reject allocation/loop bombs before creating any records. Twelve bytes is
    // the minimum fragment record; a CV node always needs at least 42 bytes.
    if records > MAX_RECORDS
        || records as u64 > size
        || nodes > reader.bytes.len() / 42
        || fragments > reader.bytes.len() / 12
    {
        return Err(RangeHashError::LimitExceeded);
    }
    Ok((RangeHash::new(size), nodes, fragments))
}

fn read_node(reader: &mut Reader<'_>, state: &mut RangeHash) -> Result<(), RangeHashError> {
    let tag = reader.u8()?;
    let start = reader
        .u64()?
        .checked_mul(CHUNK)
        .ok_or(RangeHashError::InvalidState("subtree coordinate overflow"))?;
    let node = match tag {
        1 => {
            let height = reader.u8()?;
            if height > 53 {
                return Err(RangeHashError::InvalidState("subtree height overflow"));
            }
            Node::Full {
                height,
                cv: reader.array()?,
            }
        }
        2 => Node::Tail {
            len: reader.u16()?,
            cv: reader.array()?,
        },
        _ => return Err(RangeHashError::InvalidState("unknown node tag")),
    };
    state.validate_node(start, &node)?;
    if state
        .nodes
        .last_key_value()
        .is_some_and(|(&prev, _)| prev >= start)
    {
        return Err(RangeHashError::InvalidState("unsorted or duplicate nodes"));
    }
    state.nodes.insert(start, node);
    Ok(())
}

fn read_fragment(reader: &mut Reader<'_>, state: &mut RangeHash) -> Result<(), RangeHashError> {
    if reader.u8()? != 3 {
        return Err(RangeHashError::InvalidState("unknown fragment tag"));
    }
    let start = reader.u64()?;
    let len = usize::from(reader.u16()?);
    if len == 0
        || len as u64 > CHUNK - start % CHUNK
        || start
            .checked_add(len as u64)
            .is_none_or(|end| end > state.size)
    {
        return Err(RangeHashError::InvalidState("invalid fragment coordinates"));
    }
    if state
        .fragments
        .last_key_value()
        .is_some_and(|(&prev, _)| prev >= start)
    {
        return Err(RangeHashError::InvalidState(
            "unsorted or duplicate fragments",
        ));
    }
    let raw = reader.take(len)?.to_vec();
    state.fragments.insert(start, raw);
    Ok(())
}

fn metadata_checksum(bytes: &[u8]) -> [u8; 32] {
    blake3::derive_key(CHECKSUM_DOMAIN, bytes)
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], RangeHashError> {
        if len > self.bytes.len() {
            return Err(RangeHashError::InvalidState("truncated record"));
        }
        let (value, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], RangeHashError> {
        self.take(N)?
            .try_into()
            .map_err(|_| RangeHashError::InvalidState("truncated integer"))
    }

    fn u8(&mut self) -> Result<u8, RangeHashError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, RangeHashError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, RangeHashError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, RangeHashError> {
        Ok(u64::from_le_bytes(self.array()?))
    }
}
