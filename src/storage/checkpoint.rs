//! Source hash evidence and destination coverage, owned by one recoverable stage.

use std::ops::Range;

use bytes::Bytes;

const MAGIC: &[u8; 8] = b"DMHCP002";
pub(crate) const MAX_CHECKPOINT_BYTES: usize = 1024 * 1024;
const HEADER_BYTES: usize = 68;
const CHECKSUM_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum CheckpointPhase {
    #[default]
    Receiving,
    ReadyToPublish,
    Published,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid, unsupported, or oversized hash checkpoint")]
pub(crate) struct CheckpointError;

#[derive(Clone, Debug, Default)]
pub(crate) struct CheckpointData {
    pub(crate) size: u64,
    pub(crate) generation: u64,
    pub(crate) phase: CheckpointPhase,
    /// Actual planned metadata writes plus the ordinary request selectors. Recovered finals
    /// must match; surviving stages re-enter Receiving before using a new plan.
    pub(crate) metadata_binding: Option<[u8; 32]>,
    pub(crate) hash: Bytes,
    pub(crate) ranges: Vec<Range<u64>>,
}

impl CheckpointData {
    pub(crate) fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(
            HEADER_BYTES + self.hash.len() + self.ranges.len() * 16 + CHECKSUM_BYTES,
        );
        out.extend_from_slice(MAGIC);
        out.push(match self.phase {
            CheckpointPhase::Receiving => 0,
            CheckpointPhase::ReadyToPublish => 1,
            CheckpointPhase::Published => 2,
        });
        out.extend_from_slice(&[u8::from(self.metadata_binding.is_some()), 0, 0]);
        out.extend_from_slice(&self.size.to_le_bytes());
        out.extend_from_slice(&self.generation.to_le_bytes());
        // Producers preflight the much smaller envelope cap before exposing payload. A caller
        // violating it still cannot wrap a length into a valid smaller checkpoint.
        out.extend_from_slice(
            &u32::try_from(self.hash.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        out.extend_from_slice(
            &u32::try_from(self.ranges.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        out.extend_from_slice(&self.metadata_binding.unwrap_or([0; 32]));
        for range in &self.ranges {
            out.extend_from_slice(&range.start.to_le_bytes());
            out.extend_from_slice(&range.end.to_le_bytes());
        }
        out.extend_from_slice(&self.hash);
        out.extend_from_slice(blake3::hash(&out).as_bytes());
        Bytes::from(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, CheckpointError> {
        if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES || bytes.len() > MAX_CHECKPOINT_BYTES {
            return Err(CheckpointError);
        }
        let (body, checksum) = bytes.split_at(bytes.len() - CHECKSUM_BYTES);
        if &body[..8] != MAGIC
            || body[9] > 1
            || body[10..12] != [0; 2]
            || blake3::hash(body).as_bytes() != checksum
        {
            return Err(CheckpointError);
        }
        let phase = match body[8] {
            0 => CheckpointPhase::Receiving,
            1 => CheckpointPhase::ReadyToPublish,
            2 => CheckpointPhase::Published,
            _ => return Err(CheckpointError),
        };
        let size = u64_field(body, 12)?;
        let generation = u64_field(body, 20)?;
        let hash_len = u32_field(body, 28)?;
        let count = u32_field(body, 32)?;
        let metadata_binding = decode_metadata_binding(body)?;
        let hash_at = HEADER_BYTES
            .checked_add(count.checked_mul(16).ok_or(CheckpointError)?)
            .ok_or(CheckpointError)?;
        if hash_at.checked_add(hash_len) != Some(body.len()) || hash_len == 0 {
            return Err(CheckpointError);
        }
        let ranges = decode_ranges(body, count, size)?;
        Ok(Self {
            size,
            generation,
            phase,
            metadata_binding,
            ranges,
            hash: Bytes::copy_from_slice(&body[hash_at..]),
        })
    }

    pub(crate) fn prefix(&self) -> u64 {
        self.ranges
            .first()
            .filter(|range| range.start == 0)
            .map_or(0, |range| range.end)
    }
}

fn decode_metadata_binding(body: &[u8]) -> Result<Option<[u8; 32]>, CheckpointError> {
    let binding: [u8; 32] = body[36..68].try_into().map_err(|_| CheckpointError)?;
    match body[9] {
        1 => Ok(Some(binding)),
        0 if binding == [0; 32] => Ok(None),
        _ => Err(CheckpointError),
    }
}

fn u64_field(bytes: &[u8], at: usize) -> Result<u64, CheckpointError> {
    let field = bytes.get(at..at + 8).ok_or(CheckpointError)?;
    Ok(u64::from_le_bytes(
        field.try_into().map_err(|_| CheckpointError)?,
    ))
}

fn u32_field(bytes: &[u8], at: usize) -> Result<usize, CheckpointError> {
    let field = bytes.get(at..at + 4).ok_or(CheckpointError)?;
    usize::try_from(u32::from_le_bytes(
        field.try_into().map_err(|_| CheckpointError)?,
    ))
    .map_err(|_| CheckpointError)
}

fn decode_ranges(
    bytes: &[u8],
    count: usize,
    size: u64,
) -> Result<Vec<Range<u64>>, CheckpointError> {
    let mut ranges: Vec<Range<u64>> = Vec::with_capacity(count);
    for index in 0..count {
        let at = HEADER_BYTES + index * 16;
        let range = u64_field(bytes, at)?..u64_field(bytes, at + 8)?;
        if range.start >= range.end
            || range.end > size
            || ranges
                .last()
                .is_some_and(|previous| previous.end >= range.start)
        {
            return Err(CheckpointError);
        }
        ranges.push(range);
    }
    Ok(ranges)
}

pub(crate) fn insert_range(ranges: &mut Vec<Range<u64>>, mut range: Range<u64>) {
    if range.start >= range.end {
        return;
    }
    let first = ranges.partition_point(|other| other.end < range.start);
    let mut end = first;
    while end < ranges.len() && ranges[end].start <= range.end {
        range.start = range.start.min(ranges[end].start);
        range.end = range.end.max(ranges[end].end);
        end += 1;
    }
    ranges.splice(first..end, [range]);
}

pub(crate) fn missing_ranges(ranges: &[Range<u64>], range: Range<u64>) -> Vec<Range<u64>> {
    let mut cursor = range.start;
    let mut missing = Vec::new();
    for covered in ranges {
        if covered.end <= cursor {
            continue;
        }
        if covered.start >= range.end {
            break;
        }
        if covered.start > cursor {
            missing.push(cursor..covered.start.min(range.end));
        }
        cursor = cursor.max(covered.end).min(range.end);
    }
    if cursor < range.end {
        missing.push(cursor..range.end);
    }
    missing
}

#[derive(Default)]
pub(crate) struct StageCheckpoint {
    pub(crate) enabled: bool,
    pub(crate) restored: bool,
    pub(crate) data: CheckpointData,
    pub(crate) confirmed: Vec<Range<u64>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_intent_and_durable_ranges_are_independent() -> Result<(), CheckpointError> {
        let value = CheckpointData {
            size: 100,
            hash: Bytes::from_static(b"source-intent"),
            ranges: vec![0..17, 31..72],
            ..CheckpointData::default()
        };
        let decoded = CheckpointData::decode(&value.encode())?;
        assert_eq!(decoded.prefix(), 17);
        assert_eq!(decoded.ranges, value.ranges);
        assert_eq!(decoded.hash, value.hash);
        assert_eq!(
            missing_ranges(&decoded.ranges, 0..100),
            vec![17..31, 72..100]
        );
        Ok(())
    }

    #[test]
    fn rejects_corruption_and_noncanonical_ranges() {
        let value = CheckpointData {
            size: 100,
            hash: Bytes::from_static(b"hash"),
            ranges: vec![0..10, 10..20],
            ..CheckpointData::default()
        };
        assert!(CheckpointData::decode(&value.encode()).is_err());
        let mut bytes = value.encode().to_vec();
        bytes[12] ^= 1;
        assert!(CheckpointData::decode(&bytes).is_err());
    }

    #[test]
    fn metadata_seal_presence_and_values_survive_the_envelope() -> Result<(), CheckpointError> {
        for metadata_binding in [None, Some([0; 32]), Some([0xa5; 32])] {
            let value = CheckpointData {
                metadata_binding,
                phase: CheckpointPhase::ReadyToPublish,
                hash: Bytes::from_static(b"hash"),
                ..CheckpointData::default()
            };
            let decoded = CheckpointData::decode(&value.encode())?;
            assert_eq!(decoded.metadata_binding, metadata_binding);
            assert_eq!(decoded.phase, CheckpointPhase::ReadyToPublish);
        }
        Ok(())
    }
}
