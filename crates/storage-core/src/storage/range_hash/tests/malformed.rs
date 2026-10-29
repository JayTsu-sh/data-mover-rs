use super::*;

fn blob(size: u64, nodes: &[Vec<u8>], fragments: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = b"DMRHASH1".to_vec();
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&[1, 0]);
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&size.to_le_bytes());
    bytes.extend_from_slice(&(nodes.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(fragments.len() as u32).to_le_bytes());
    for record in nodes.iter().chain(fragments) {
        bytes.extend_from_slice(record);
    }
    append_checksum(&mut bytes);
    bytes
}

fn append_checksum(bytes: &mut Vec<u8>) {
    let checksum = blake3::derive_key("data-mover RangeHash metadata checksum v1", bytes);
    bytes.extend_from_slice(&checksum);
}

fn resign(bytes: &mut Vec<u8>) {
    bytes.truncate(bytes.len() - 32);
    append_checksum(bytes);
}

fn full(start_chunk: u64, height: u8) -> Vec<u8> {
    let mut record = vec![1];
    record.extend_from_slice(&start_chunk.to_le_bytes());
    record.push(height);
    record.extend_from_slice(&[42; 32]);
    record
}

fn tail(start_chunk: u64, len: u16) -> Vec<u8> {
    let mut record = vec![2];
    record.extend_from_slice(&start_chunk.to_le_bytes());
    record.extend_from_slice(&len.to_le_bytes());
    record.extend_from_slice(&[43; 32]);
    record
}

fn raw(start: u64, bytes: &[u8]) -> Vec<u8> {
    let mut record = vec![3];
    record.extend_from_slice(&start.to_le_bytes());
    record.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    record.extend_from_slice(bytes);
    record
}

#[test]
fn checksum_truncation_and_trailing_bytes_are_rejected() {
    let encoded = blob(4097, &[full(0, 1), tail(4, 1)], &[raw(3000, b"hello")]);
    assert!(RangeHash::decode(&encoded, 4097).is_ok());
    for end in 0..encoded.len() {
        assert!(RangeHash::decode(&encoded[..end], 4097).is_err());
    }
    for i in 0..encoded.len() {
        let mut corrupted = encoded.clone();
        corrupted[i] ^= 1;
        assert!(RangeHash::decode(&corrupted, 4097).is_err());
    }
    let mut trailing = encoded[..encoded.len() - 32].to_vec();
    trailing.push(0);
    append_checksum(&mut trailing);
    assert!(matches!(
        RangeHash::decode(&trailing, 4097),
        Err(RangeHashError::InvalidState("trailing bytes"))
    ));
    assert!(RangeHash::decode(&encoded, 4096).is_err());
}

#[test]
fn unknown_schemas_algorithms_modes_and_reserved_bits_are_rejected() {
    let valid = blob(0, &[], &[]);
    for offset in [0, 8, 9, 10, 11, 12, 13, 14, 15] {
        let mut bytes = valid.clone();
        bytes[offset] ^= 1;
        resign(&mut bytes);
        assert!(matches!(
            RangeHash::decode(&bytes, 0),
            Err(RangeHashError::UnsupportedFormat)
        ));
    }
    for mode in [1, 2, 255] {
        let mut bytes = valid.clone();
        bytes[11] = mode;
        resign(&mut bytes);
        assert!(RangeHash::decode(&bytes, 0).is_err());
    }
}

#[test]
fn oversized_counts_and_declared_fragment_lengths_fail_before_allocation() {
    for offset in [24, 28] {
        let mut bytes = blob(u64::MAX, &[], &[]);
        bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        resign(&mut bytes);
        assert!(matches!(
            RangeHash::decode(&bytes, u64::MAX),
            Err(RangeHashError::LimitExceeded)
        ));
    }
    let mut record = raw(0, b"a");
    record[9..11].copy_from_slice(&u16::MAX.to_le_bytes());
    assert!(RangeHash::decode(&blob(8192, &[], &[record]), 8192).is_err());
    let mut record = raw(0, b"a");
    record[9..11].copy_from_slice(&1000_u16.to_le_bytes());
    assert!(RangeHash::decode(&blob(8192, &[], &[record]), 8192).is_err());
}

#[test]
fn illegal_subtree_coordinates_heights_and_whole_file_cvs_are_rejected() {
    for record in [
        full(u64::MAX, 0),
        full(1, 1),
        full(8, 0),
        full(0, 54),
        full(0, 255),
    ] {
        assert!(RangeHash::decode(&blob(8192, &[record], &[]), 8192).is_err());
    }
    for height in 0..=10 {
        let size = CHUNK << height;
        assert!(RangeHash::decode(&blob(size, &[full(0, height)], &[]), size).is_err());
    }
    for record in [tail(1, 1), tail(8, 2), tail(8, 0), tail(8, 1024)] {
        assert!(RangeHash::decode(&blob(8193, &[record], &[]), 8193).is_err());
    }
    assert!(RangeHash::decode(&blob(1024, &[tail(0, 1024)], &[]), 1024).is_err());
    let mut unknown = full(0, 0);
    unknown[0] = 4; // No final-digest record can masquerade as a subtree.
    assert!(RangeHash::decode(&blob(8192, &[unknown], &[]), 8192).is_err());
}

#[test]
fn unsorted_duplicate_overlapping_and_uncompacted_nodes_are_rejected() {
    for nodes in [
        vec![full(2, 0), full(0, 0)],
        vec![full(0, 0), full(0, 0)],
        vec![full(0, 1), full(1, 0)],
        vec![full(0, 0), full(1, 0)],
    ] {
        assert!(RangeHash::decode(&blob(8192, &nodes, &[]), 8192).is_err());
    }
    // Adjacent leaves 1 and 2 are not buddies and must stay separate.
    assert!(RangeHash::decode(&blob(8192, &[full(1, 0), full(2, 0)], &[]), 8192).is_ok());
    assert!(RangeHash::decode(&blob(2048, &[full(0, 0), full(1, 0)], &[]), 2048).is_ok());
}

#[test]
fn invalid_fragment_spans_and_node_fragment_aliases_are_rejected() {
    for fragments in [
        vec![raw(0, b"")],
        vec![raw(1023, b"ab")],
        vec![raw(8191, b"ab")],
        vec![raw(u64::MAX, b"ab")],
        vec![raw(0, &[0; 1024])],
        vec![raw(1, b"ab"), raw(0, b"c")],
        vec![raw(0, b"ab"), raw(0, b"ab")],
        vec![raw(0, b"ab"), raw(1, b"c")],
        vec![raw(0, b"ab"), raw(2, b"c")],
    ] {
        assert!(RangeHash::decode(&blob(8192, &[], &fragments), 8192).is_err());
    }
    assert!(RangeHash::decode(&blob(8192, &[full(0, 1)], &[raw(1024, b"a")]), 8192).is_err());
    let valid = blob(8192, &[], &[raw(1023, b"a"), raw(1024, b"b")]);
    assert_eq!(
        RangeHash::decode(&valid, 8192).unwrap().coverage(),
        vec![1023..1025]
    );
}

#[test]
fn invalid_private_states_cannot_be_encoded() {
    let mut state = RangeHash::new(8192);
    state.nodes.insert(
        0,
        Node::Full {
            height: 255,
            cv: [0; 32],
        },
    );
    assert!(state.encode().is_err());
    state.nodes.clear();
    state.fragments.insert(u64::MAX, vec![0; 2]);
    assert!(state.encode().is_err());
}
