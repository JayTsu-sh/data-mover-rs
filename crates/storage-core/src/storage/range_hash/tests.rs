#![allow(clippy::unwrap_used, clippy::expect_used)]
// Test inputs and their indices are bounded well below 32-bit limits. The range
// vectors and deliberately reversed queries exercise the public range contract.
#![allow(
    clippy::cast_possible_truncation,
    clippy::single_range_in_vec_init,
    clippy::reversed_empty_ranges
)]

use std::ops::Range;

use super::{CHUNK, Node, RangeHash, RangeHashError};

#[path = "tests/malformed.rs"]
mod malformed;
#[path = "tests/reference.rs"]
mod reference;
#[path = "tests/vectors.rs"]
mod vectors;

fn input(len: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 13 + i / 251) % 251) as u8).collect()
}

fn random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn shuffled_ranges(len: usize, max_chunk: usize, seed: &mut u64) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < len {
        let end = len.min(start + 1 + random(seed) as usize % max_chunk);
        ranges.push(start..end);
        start = end;
    }
    for i in (1..ranges.len()).rev() {
        let j = random(seed) as usize % (i + 1);
        ranges.swap(i, j);
    }
    ranges
}

fn assert_digest(state: &RangeHash, source: &[u8]) {
    let digest = state.finalize().unwrap();
    assert_eq!(
        digest,
        *blake3::hash(source).as_bytes(),
        "length {}",
        source.len()
    );
    let mut oracle = reference::Hasher::new();
    oracle.update(source);
    let mut expected = [0; 32];
    oracle.finalize(&mut expected);
    assert_eq!(digest, expected, "reference, length {}", source.len());
}

#[test]
fn every_length_zero_through_4097_with_sparse_checkpoint_and_shuffled_resume() {
    for len in 0..=4097 {
        let source = input(len);
        let mut state = RangeHash::new(len as u64);
        let mut seed = len as u64 + 17;
        let ranges = shuffled_ranges(len, 1709, &mut seed);
        for range in ranges.iter().step_by(2) {
            state
                .observe(range.start as u64, &source[range.clone()])
                .unwrap();
        }
        let encoded = state.encode().unwrap();
        state = RangeHash::decode(&encoded, len as u64).unwrap();
        assert_eq!(encoded, state.encode().unwrap());
        for range in state.missing_ranges(0..len as u64).into_iter().rev() {
            assert!(!state.covers(range.clone()));
            state
                .observe(
                    range.start,
                    &source[range.start as usize..range.end as usize],
                )
                .unwrap();
        }
        assert_digest(&state, &source);
        assert_eq!(
            state.coverage(),
            if len == 0 {
                vec![]
            } else {
                vec![0..len as u64]
            }
        );
    }
}

#[test]
fn official_vectors_and_canonical_encoding_across_partitions() {
    for &(len, expected) in vectors::VECTORS {
        let source: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let mut bulk = RangeHash::new(len as u64);
        bulk.observe(0, &source).unwrap();
        let mut shuffled = RangeHash::new(len as u64);
        let mut seed = len as u64 + 1;
        for range in shuffled_ranges(len, 2001, &mut seed) {
            shuffled
                .observe(range.start as u64, &source[range])
                .unwrap();
        }
        assert_eq!(bulk.encode().unwrap(), shuffled.encode().unwrap());
        assert_eq!(
            blake3::Hash::from(bulk.finalize().unwrap())
                .to_hex()
                .as_str(),
            expected
        );
        assert_digest(&shuffled, &source);
    }
}

#[test]
fn exact_five_mib_plus_17_roundtrip_resume_with_changed_read_geometry() {
    let source = input(7 * 1024 * 1024 + 333);
    let cut = 5 * 1024 * 1024 + 17;
    for read_size in [32_771, 65_536, 98_304] {
        let mut state = RangeHash::new(source.len() as u64);
        let mut ranges: Vec<_> = (0..cut)
            .step_by(read_size)
            .map(|start| start..cut.min(start + read_size))
            .collect();
        ranges.reverse();
        for range in ranges {
            state.observe(range.start as u64, &source[range]).unwrap();
        }
        assert_eq!(state.nodes.len(), 2);
        assert_eq!(state.fragments.values().map(Vec::len).sum::<usize>(), 17);
        let encoded = state.encode().unwrap();
        assert!(encoded.len() < 256);
        state = RangeHash::decode(&encoded, source.len() as u64).unwrap();
        assert_eq!(state.coverage(), vec![0..cut as u64]);
        for range in state.missing_ranges(0..source.len() as u64) {
            assert!(range.start >= cut as u64);
            for start in (range.start as usize..range.end as usize).step_by(49_157) {
                let end = (start + 49_157).min(range.end as usize);
                state.observe(start as u64, &source[start..end]).unwrap();
            }
        }
        assert_digest(&state, &source);
    }
}

#[test]
fn power_of_two_roots_keep_both_children_in_bulk_and_reverse_leaf_order() {
    for height in 1..=10 {
        let len = 1024 << height;
        let source = input(len);
        let mut bulk = RangeHash::new(len as u64);
        bulk.observe(0, &source).unwrap();
        let mut leaves = RangeHash::new(len as u64);
        for offset in (0..len).step_by(1024).rev() {
            leaves
                .observe(offset as u64, &source[offset..offset + 1024])
                .unwrap();
        }
        assert_eq!(leaves.nodes.len(), 2);
        assert!(leaves.fragments.is_empty());
        assert_eq!(bulk.encode().unwrap(), leaves.encode().unwrap());
        assert_digest(
            &RangeHash::decode(&leaves.encode().unwrap(), len as u64).unwrap(),
            &source,
        );
    }
}

#[test]
fn randomized_islands_preserve_exact_gaps_after_every_observation() {
    let source = input(32_769);
    for mut seed in [1, 7, 42, 1234] {
        let mut state = RangeHash::new(source.len() as u64);
        let mut known = vec![false; source.len()];
        for range in shuffled_ranges(source.len(), 311, &mut seed) {
            state
                .observe(range.start as u64, &source[range.clone()])
                .unwrap();
            known[range].fill(true);
            let mut actual = vec![false; source.len()];
            for covered in state.coverage_iter() {
                actual[covered.start as usize..covered.end as usize].fill(true);
                assert!(state.covers(covered));
            }
            assert_eq!(actual, known);
            for missing in state.missing_ranges(17..32_751) {
                assert!(
                    known[missing.start as usize..missing.end as usize]
                        .iter()
                        .all(|x| !x)
                );
            }
            state = RangeHash::decode(&state.encode().unwrap(), source.len() as u64).unwrap();
        }
        assert_digest(&state, &source);
    }
}

#[test]
fn fragmented_leaf_bridging_drops_payload_only_when_leaf_is_complete() {
    let source = input(3073);
    let mut state = RangeHash::new(source.len() as u64);
    for range in [100..200, 300..400, 200..300, 0..100, 400..1024, 3072..3073] {
        state.observe(range.start as u64, &source[range]).unwrap();
    }
    assert!(state.fragments.is_empty());
    assert_eq!(state.coverage(), vec![0..1024, 3072..3073]);
    assert_eq!(state.missing_ranges(0..3073), vec![1024..3072]);
    state.observe(1024, &source[1024..3072]).unwrap();
    assert_digest(&state, &source);
}

#[test]
fn all_overlap_forms_are_rejected_atomically_including_known_duplicates() {
    let mut state = RangeHash::new(8192);
    state.observe(2048, &[2; 2048]).unwrap();
    state.observe(5000, &[3; 200]).unwrap();
    let original = state.encode().unwrap();
    for (offset, len) in [
        (2048, 2048),
        (2047, 2),
        (4095, 100),
        (0, 8192),
        (5001, 1),
        (4900, 101),
    ] {
        assert_eq!(
            state.observe(offset, &vec![9; len]),
            Err(RangeHashError::Overlap)
        );
        assert_eq!(state.encode().unwrap(), original);
    }
    assert_eq!(
        state.missing_ranges(2000..5300),
        vec![2000..2048, 4096..5000, 5200..5300]
    );
    assert!(state.covers(2048..4096));
    assert!(!state.covers(0..4096));
    assert_eq!(state.finalize(), Err(RangeHashError::Incomplete));
}

#[test]
fn hash_intent_crossing_destination_cut_stays_whole() {
    let source = input(8 * 1024 * 1024 + 1);
    let mut state = RangeHash::new(source.len() as u64);
    state.observe(0, &source[..8 * 1024 * 1024]).unwrap();
    let mut state = RangeHash::decode(&state.encode().unwrap(), source.len() as u64).unwrap();
    let destination_cut = 5 * 1024 * 1024 + 17;
    assert_eq!(state.nodes.len(), 1);
    // Payload must be read from D's cut, but only the final byte is missing in H.
    let missing = state.missing_ranges(destination_cut..source.len() as u64);
    assert_eq!(missing, vec![8 * 1024 * 1024..source.len() as u64]);
    for range in missing {
        state
            .observe(
                range.start,
                &source[range.start as usize..range.end as usize],
            )
            .unwrap();
    }
    assert_digest(&state, &source);
}

#[test]
fn high_offsets_and_size_arithmetic_never_wrap() {
    let mut state = RangeHash::new(u64::MAX);
    assert_eq!(
        state.observe(u64::MAX, &[1]),
        Err(RangeHashError::OutOfBounds)
    );
    assert_eq!(
        state.observe(u64::MAX - 1, &[1, 2]),
        Err(RangeHashError::OutOfBounds)
    );
    let start = u64::MAX / CHUNK * CHUNK;
    state.observe(start, &[7; 1023]).unwrap();
    assert!(state.covers(start..u64::MAX));
    assert_eq!(
        state.missing_ranges(start - 1..u64::MAX),
        vec![start - 1..start]
    );
    assert_eq!(
        RangeHash::decode(&state.encode().unwrap(), u64::MAX)
            .unwrap()
            .coverage(),
        vec![start..u64::MAX]
    );
    let mut small = RangeHash::new(3);
    assert_eq!(small.observe(4, &[]), Err(RangeHashError::OutOfBounds));
    small.observe(3, &[]).unwrap();
    assert!(!small.covers(0..4));
    assert!(!small.covers(4..3));
    assert!(small.missing_ranges(4..3).is_empty());
}

#[test]
fn long_stream_retains_only_logarithmic_frontier_and_leaf_boundary() {
    let block = input(65_537);
    let size = block.len() as u64 * 257;
    let mut state = RangeHash::new(size);
    let mut oracle = blake3::Hasher::new();
    for offset in (0..size).step_by(block.len()) {
        state.observe(offset, &block).unwrap();
        oracle.update(&block);
        assert!(state.nodes.len() <= 16);
        assert!(state.fragments.values().map(Vec::len).sum::<usize>() <= 1023);
        assert_eq!(state.coverage(), vec![0..offset + block.len() as u64]);
    }
    assert_eq!(state.finalize().unwrap(), *oracle.finalize().as_bytes());
}

#[test]
fn leaf_counters_and_root_domains_cannot_be_substituted() {
    use blake3::hazmat::HasherExt;
    let source = input(2048);
    let mut state = RangeHash::new(2048);
    state.observe(0, &source).unwrap();
    let expected = state.finalize().unwrap();
    state.nodes.insert(
        1024,
        Node::Full {
            height: 0,
            cv: blake3::Hasher::new()
                .update(&source[1024..])
                .finalize_non_root(),
        },
    );
    assert_ne!(state.finalize().unwrap(), expected);
    state.nodes.insert(
        1024,
        Node::Full {
            height: 0,
            cv: *blake3::hash(&source[1024..]).as_bytes(),
        },
    );
    assert_ne!(state.finalize().unwrap(), expected);
}
