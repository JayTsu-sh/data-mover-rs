//! The checkpoint codec and fingerprint: the exact bytes of the current format, every refusal,
//! what the fingerprint binds, and how the size grows with the tree's shape.

use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::*;
use crate::model::{ObservationMode, ObservationPlan};
use crate::storage::backends::{hdfs, local};
use crate::traversal::{TraversalCandidate, TraversalDecision, TraversalFilter};

const FINGERPRINT: [u8; 32] = [0xab; 32];

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

fn entry(value: &str, child_depth: usize, filter_children: bool) -> FrontierEntry {
    FrontierEntry {
        path: path(value),
        child_depth,
        filter_children,
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

fn encoded(order: TraversalOrder, entries: &[FrontierEntry]) -> Checkpoint {
    encode(order, &FINGERPRINT, entries).unwrap_or_else(|| panic!("within the limits"))
}

/// A body sealed with a valid checksum, so a test reaches the checks behind it.
fn sealed(mut body: Vec<u8>) -> Checkpoint {
    let checksum = blake3::hash(&body);
    body.extend_from_slice(&checksum.as_bytes()[..CHECKSUM]);
    Checkpoint::from_bytes(Bytes::from(body))
}

/// The header of a well-formed checkpoint with `count` entries.
fn header(order: TraversalOrder, count: u32) -> Vec<u8> {
    let mut body = MAGIC.to_vec();
    body.push(VERSION);
    body.push(order_tag(order));
    body.extend_from_slice(&FINGERPRINT);
    body.extend_from_slice(&count.to_le_bytes());
    body
}

fn raw_entry(body: &mut Vec<u8>, shared: u32, suffix: &[u8], child_depth: u32, flags: u8) {
    body.extend_from_slice(&shared.to_le_bytes());
    body.extend_from_slice(
        &u32::try_from(suffix.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    body.extend_from_slice(suffix);
    body.extend_from_slice(&child_depth.to_le_bytes());
    body.push(flags);
}

fn opened(
    checkpoint: &Checkpoint,
    order: TraversalOrder,
    root: &str,
) -> Result<Vec<FrontierEntry>, ResumeRefusal> {
    open(checkpoint, &FINGERPRINT, order, &path(root))
}

/// The current format, byte for byte: any change to it must bump [`VERSION`] (and this test).
#[test]
fn the_current_format_is_pinned_byte_for_byte() {
    let unordered = encoded(
        TraversalOrder::Unordered,
        &[entry("data/a", 2, false), entry("data/ab", 2, true)],
    );
    assert_eq!(
        hex(unordered.as_bytes()),
        concat!(
            "444d5443",
            "02",
            "00",
            "abababababababababababababababababababababababababababababababab",
            "02000000",
            "00000000",
            "06000000",
            "646174612f61",
            "02000000",
            "00",
            "06000000",
            "01000000",
            "62",
            "02000000",
            "01",
            "a2d2f95514c95534c473ac21ba9269a8"
        )
    );
    let blocks = encoded(
        TraversalOrder::Blocks,
        &[
            entry("", 1, true),
            entry("x", 2, true),
            entry("x/y", 3, false),
        ],
    );
    assert_eq!(
        hex(blocks.as_bytes()),
        concat!(
            "444d5443",
            "02",
            "01",
            "abababababababababababababababababababababababababababababababab",
            "03000000",
            "00000000",
            "00000000",
            "",
            "01000000",
            "01",
            "00000000",
            "01000000",
            "78",
            "02000000",
            "01",
            "01000000",
            "02000000",
            "2f79",
            "03000000",
            "00",
            "54d1ed2a928f029e664f63fec0c09e09"
        )
    );
}

#[test]
fn entries_round_trip_in_both_orders() {
    let frontier = vec![
        entry("r", 1, true),
        entry("r/a", 2, false),
        entry("r/a/deep/er", 4, true),
        entry("r/é", 2, false),
        entry("r/éa", 2, false),
    ];
    let checkpoint = encoded(TraversalOrder::Unordered, &frontier);
    assert_eq!(
        opened(&checkpoint, TraversalOrder::Unordered, "r"),
        Ok(frontier)
    );
    let chain = vec![
        entry("r", 1, true),
        entry("r/a", 2, true),
        entry("r/a/b", 3, false),
    ];
    let checkpoint = encoded(TraversalOrder::Blocks, &chain);
    assert_eq!(opened(&checkpoint, TraversalOrder::Blocks, "r"), Ok(chain));
    // The end of a traversal: nothing left.
    let done = encoded(TraversalOrder::Blocks, &[]);
    assert_eq!(opened(&done, TraversalOrder::Blocks, "r"), Ok(Vec::new()));
}

#[test]
fn another_version_is_refused_never_misread() {
    let checkpoint = encoded(TraversalOrder::Unordered, &[entry("a", 2, false)]);
    for version in [0, VERSION + 1, u8::MAX] {
        let mut bytes = checkpoint.as_bytes().to_vec();
        bytes[MAGIC.len()] = version;
        assert_eq!(
            opened(
                &Checkpoint::from_bytes(Bytes::from(bytes)),
                TraversalOrder::Unordered,
                ""
            ),
            Err(ResumeRefusal::UnsupportedVersion(version))
        );
    }
}

#[test]
fn damaged_bytes_are_malformed() {
    let checkpoint = encoded(TraversalOrder::Unordered, &[entry("a/b", 3, false)]);
    let bytes = checkpoint.as_bytes();
    let refused = |bytes: Vec<u8>| {
        opened(
            &Checkpoint::from_bytes(Bytes::from(bytes)),
            TraversalOrder::Unordered,
            "",
        )
    };
    let mut magic = bytes.to_vec();
    magic[0] = b'X';
    assert_eq!(refused(magic), Err(ResumeRefusal::Malformed));
    for cut in [0, 3, 5, HEADER, bytes.len() - 1] {
        assert_eq!(
            refused(bytes[..cut].to_vec()),
            Err(ResumeRefusal::Malformed),
            "cut {cut}"
        );
    }
    let mut flipped = bytes.to_vec();
    flipped[HEADER + 9] ^= 1;
    assert_eq!(refused(flipped), Err(ResumeRefusal::Malformed), "checksum");
    let mut longer = bytes.to_vec();
    longer.push(0);
    assert_eq!(
        refused(longer),
        Err(ResumeRefusal::Malformed),
        "trailing byte"
    );
}

/// Appends entries to a body.
type Build = Box<dyn Fn(&mut Vec<u8>)>;

/// Bodies with a valid checksum that no encoder writes.
#[test]
fn impossible_bodies_are_malformed() {
    let unordered = |build: &dyn Fn(&mut Vec<u8>), count: u32| {
        let mut body = header(TraversalOrder::Unordered, count);
        build(&mut body);
        opened(&sealed(body), TraversalOrder::Unordered, "")
    };
    let cases: Vec<(&str, Build, u32)> = vec![
        (
            "shares more than the previous path",
            Box::new(|b| raw_entry(b, 1, b"a", 1, 0)),
            1,
        ),
        ("not UTF-8", Box::new(|b| raw_entry(b, 0, &[0xff], 1, 0)), 1),
        (
            "NUL in a path",
            Box::new(|b| raw_entry(b, 0, b"a\0", 1, 0)),
            1,
        ),
        ("depth 0", Box::new(|b| raw_entry(b, 0, b"a", 0, 0)), 1),
        ("unknown flag", Box::new(|b| raw_entry(b, 0, b"a", 1, 2)), 1),
        (
            "fewer entries than counted",
            Box::new(|b| raw_entry(b, 0, b"a", 1, 0)),
            2,
        ),
        (
            "more than the cap",
            Box::new(|_| {}),
            u32::try_from(MAX_ENTRIES + 1).unwrap_or(0),
        ),
        (
            "suffix past the end",
            Box::new(|b| {
                b.extend_from_slice(&0_u32.to_le_bytes());
                b.extend_from_slice(&9_u32.to_le_bytes());
                b.push(b'a');
            }),
            1,
        ),
    ];
    for (case, build, count) in &cases {
        assert_eq!(
            unordered(build.as_ref(), *count),
            Err(ResumeRefusal::Malformed),
            "{case}"
        );
    }
}

/// A few bytes cannot decode into a flood: entries that each repeat a long shared prefix are
/// refused once what they spell out passes the size limit.
#[test]
fn a_checkpoint_that_spells_out_too_much_is_malformed() {
    const LONG: usize = 1 << 20;
    let count = MAX_BYTES / LONG + 2;
    let mut body = header(
        TraversalOrder::Unordered,
        u32::try_from(count).unwrap_or(u32::MAX),
    );
    raw_entry(&mut body, 0, &vec![b'a'; LONG], 1, 0);
    for _ in 1..count {
        raw_entry(
            &mut body,
            u32::try_from(LONG).unwrap_or(u32::MAX),
            b"",
            1,
            0,
        );
    }
    assert!(body.len() < 2 * LONG, "the input itself is small");
    assert_eq!(
        opened(&sealed(body), TraversalOrder::Unordered, ""),
        Err(ResumeRefusal::Malformed)
    );
}

/// The fingerprint is no signature: a checkpoint cannot widen a scan beyond its root, and a
/// Blocks position must be one chain down from it.
#[test]
fn entries_outside_the_root_or_off_the_chain_are_malformed() {
    let outside = encoded(TraversalOrder::Unordered, &[entry("keeper/a", 3, false)]);
    assert_eq!(
        opened(&outside, TraversalOrder::Unordered, "keep"),
        Err(ResumeRefusal::Malformed)
    );
    let inside = encoded(TraversalOrder::Unordered, &[entry("keep/a", 3, false)]);
    assert!(opened(&inside, TraversalOrder::Unordered, "keep").is_ok());
    for chain in [
        vec![entry("other", 1, false)],
        vec![entry("keep", 2, false)],
        vec![entry("keep", 1, false), entry("keep/a/b", 2, false)],
        vec![entry("keep", 1, false), entry("keep/a", 3, false)],
        vec![entry("keep", 1, false), entry("keeper", 2, false)],
    ] {
        let checkpoint = encoded(TraversalOrder::Blocks, &chain);
        assert_eq!(
            opened(&checkpoint, TraversalOrder::Blocks, "keep"),
            Err(ResumeRefusal::Malformed),
            "{chain:?}"
        );
    }
}

#[test]
fn another_fingerprint_or_order_is_a_mismatch() {
    let checkpoint = encoded(TraversalOrder::Unordered, &[entry("a", 2, false)]);
    assert_eq!(
        open(
            &checkpoint,
            &[0; 32],
            TraversalOrder::Unordered,
            &StoragePath::root()
        ),
        Err(ResumeRefusal::Mismatch)
    );
    assert_eq!(
        opened(&checkpoint, TraversalOrder::Blocks, ""),
        Err(ResumeRefusal::Mismatch)
    );
}

/// A filter with no resume key of its own.
#[derive(Debug)]
struct Keyless;

impl TraversalFilter for Keyless {
    fn needs_modified(&self) -> bool {
        false
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision::unfiltered(candidate.kind)
    }
}

/// A filter whose key is its name: two filters with the same name decide alike.
#[derive(Debug)]
struct Named(&'static str);

impl TraversalFilter for Named {
    fn needs_modified(&self) -> bool {
        false
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision::unfiltered(candidate.kind)
    }

    fn resume_key(&self) -> Option<Vec<u8>> {
        Some(self.0.as_bytes().to_vec())
    }
}

fn named(name: &'static str) -> Arc<dyn TraversalFilter> {
    Arc::new(Named(name))
}

/// Every field that decides what a traversal lists changes the fingerprint; what only tunes it
/// does not.
#[test]
fn the_fingerprint_binds_what_decides_the_listing_and_nothing_else() {
    let local = local::test_identity("store");
    let base = TraversalRequest::new(path("r")).filter(named("logs"));
    let print = |identity: &BackendIdentity, request: &TraversalRequest| {
        fingerprint(identity, request, &[0]).unwrap_or_else(|error| panic!("{error}"))
    };
    let expected = print(&local, &base);
    let depth = NonZeroUsize::new(3);
    let changed = [
        (
            "kind",
            print(
                &hdfs::test_identity("store").unwrap_or_else(|error| panic!("{error}")),
                &base,
            ),
        ),
        ("storage", print(&local::test_identity("other"), &base)),
        (
            "root",
            print(
                &local,
                &TraversalRequest::new(path("s")).filter(named("logs")),
            ),
        ),
        (
            "order",
            print(&local, &base.clone().order(TraversalOrder::Blocks)),
        ),
        ("depth", print(&local, &base.clone().max_depth(depth))),
        (
            "listing semantics",
            fingerprint(&local, &base, b"another-listing-mode")
                .unwrap_or_else(|error| panic!("{error}")),
        ),
        (
            "filter",
            print(&local, &base.clone().filter(named("texts"))),
        ),
        (
            "no filter",
            print(&local, &TraversalRequest::new(path("r"))),
        ),
    ];
    for (field, printed) in changed {
        assert_ne!(printed, expected, "{field} is not bound");
    }
    let tuned = base
        .clone()
        .max_inflight_operations(NonZeroUsize::MIN)
        .max_buffered_items(NonZeroUsize::MIN)
        .observation_plan(ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly))
        .resumable(Resumable::on());
    assert_eq!(print(&local, &tuned), expected);
    // Built twice, the same filter.
    assert_eq!(print(&local, &base.clone().filter(named("logs"))), expected);
}

#[test]
fn a_filter_without_a_resume_key_cannot_be_resumable() {
    let request = TraversalRequest::new(StoragePath::root()).filter(Arc::new(Keyless));
    assert_eq!(
        fingerprint(&local::test_identity("store"), &request, &[0]),
        Err(ResumeRefusal::FilterNotResumable)
    );
}

/// What a checkpoint weighs as the tree grows: 13 bytes per directory plus the path bytes it does
/// not share with the one before, on top of 58 bytes. A Blocks position is one entry per level, so
/// a deep chain grows with its depth and a wide directory not at all; an Unordered frontier holds
/// every directory still waiting, so a wide one grows with the number of its subdirectories.
#[test]
fn the_size_follows_the_shape_of_the_tree() {
    const FIXED: usize = HEADER + CHECKSUM;
    // A chain 500 levels deep of 8-byte names: each level adds `/` and the name.
    let mut chain = vec![entry("", 1, false)];
    for level in 1..=500 {
        let parent = chain[level - 1].path.as_str().to_owned();
        let child = if parent.is_empty() {
            "d0000001".to_owned()
        } else {
            format!("{parent}/d{level:07}")
        };
        chain.push(entry(&child, level + 1, false));
    }
    let deep = encoded(TraversalOrder::Blocks, &chain);
    assert_eq!(deep.as_bytes().len(), FIXED + 13 + 13 + 8 + 499 * (13 + 9));
    assert!(deep.as_bytes().len() < 12 * 1024);
    // 10,000 sibling directories waiting: each costs 13 bytes and its differing suffix.
    let siblings: Vec<_> = (0..10_000)
        .map(|index| entry(&format!("wide/s{index:05}"), 2, false))
        .collect();
    let wide = encoded(TraversalOrder::Unordered, &siblings);
    assert!(
        wide.as_bytes().len() <= FIXED + 10_000 * (13 + 11),
        "{}",
        wide.as_bytes().len()
    );
    assert!(wide.as_bytes().len() >= FIXED + 10_000 * 13);
    // Past the cap no checkpoint is taken at all.
    let too_many = vec![entry("a", 2, false); MAX_ENTRIES + 1];
    assert!(encode(TraversalOrder::Unordered, &FINGERPRINT, &too_many).is_none());
}
