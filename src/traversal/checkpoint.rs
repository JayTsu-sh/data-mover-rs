//! Resumable traversal (P7 §9): the request's switch, the opaque checkpoint a consumer persists,
//! and the fingerprint that binds a checkpoint to the request that produced it.
//!
//! A checkpoint records **which directories are still to be listed**, never which entries were
//! seen: under [`TraversalOrder::Unordered`] the frontier (every directory pushed and not finished,
//! each directory whose ancestor is also there left out, since listing the ancestor finds it
//! again); under [`TraversalOrder::Blocks`] the depth-first position (the chain from the root to
//! the directory whose block comes next; each ancestor's remaining subdirectories are the ones
//! named after the next link, in the byte order Blocks descends in). Protocol positions (an NFS
//! cookie, an S3 continuation token) never enter it (§16): a directory is resumed by listing it
//! again, whole.
//!
//! **Not exact** (§9.2). Resuming may deliver again what was delivered after the checkpoint was
//! taken — the directories it names are listed whole — but never skips anything that was not
//! delivered before the checkpoint item. A consumer persists the checkpoint only once everything
//! it received before that item is committed, and resumes from the last one it persisted.
//!
//! **Format** (version 1; bumped on any change, and a checkpoint of another version is refused,
//! never misread):
//!
//! ```text
//! "DMTC" | version u8 | order u8 | fingerprint [32] | count u32 LE |
//! count × { shared u32 LE | suffix length u32 LE | suffix | child depth u32 LE | flags u8 } |
//! blake3 of everything before [..16]
//! ```
//!
//! Each path is stored as the bytes it shares with the previous entry's path and the rest
//! (entries are sorted, or, in Blocks, each is a child of the previous). `flags` bit 0: the filter
//! still applies below the directory.
//!
//! **Size.** An entry costs 13 bytes plus the part of its path it does not share with the one
//! before. Unordered: at most one entry per directory held by the workers — about depth × fan-out
//! × workers for a balanced tree (a few thousand on a 53-million-entry tree), but every waiting
//! subdirectory of a directory with millions of them: past [`MAX_ENTRIES`] no checkpoint is taken
//! (an earlier one stays valid). Blocks: one entry per level of the current path, whatever the
//! tree's width.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;

use super::{Checkpoint, TraversalOrder, TraversalRequest, TraversalVersions, within};
use crate::model::{BackendIdentity, StoragePath};

/// Whether a traversal emits checkpoints (P7 §9.1). Off by default.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum Resumable {
    /// No checkpoints and no cost: nothing is tracked and the clock is never read.
    #[default]
    Off,
    /// A [`TraversalItem::Checkpoint`](super::TraversalItem::Checkpoint) about every `every`
    /// (none in a traversal that ends sooner), plus one as the last item of a traversal that ends
    /// early — cancelled, or failed while it could still send one — unless its position is too
    /// large to record (more than a million directories waiting at once), when the last one sent
    /// stays the one to resume from. Under [`TraversalOrder::Blocks`] a periodic checkpoint only
    /// comes between two blocks; the final one may follow part of a block, whose rest a
    /// resumed traversal delivers with the whole block again.
    ///
    /// `every` is a lower bound checked as the traversal goes (after each page, or each flush):
    /// `Duration::ZERO` checkpoints at every chance, which suits tests, not production.
    ///
    /// A resumed traversal's [`TraversalCompletion::totals`](super::TraversalCompletion) count
    /// what that run delivered (the repeated part included), not the whole tree (P7 Q15).
    ///
    /// **Cancellation drains.** With checkpoints on, cancelling does not drop what the traversal
    /// already produced: [`TraversalSession::next_item`](super::TraversalSession::next_item)
    /// keeps returning it, then the final checkpoint, then `None`, and
    /// [`TraversalSession::finish`](super::TraversalSession::finish) returns `Cancelled`. The
    /// consumer drains to `None` (or drops the session); cancelling is pausing.
    On {
        /// How often a checkpoint is emitted. The checkpoint records the position at the moment
        /// it is emitted; it is not a delay before saving.
        every: Duration,
    },
}

impl Resumable {
    /// The interval P7 decided on (Q14).
    pub const DEFAULT_EVERY: Duration = Duration::from_secs(60);

    /// Checkpoints every [`Self::DEFAULT_EVERY`].
    #[must_use]
    pub const fn on() -> Self {
        Self::On {
            every: Self::DEFAULT_EVERY,
        }
    }

    /// The interval, when on.
    #[must_use]
    pub const fn every(self) -> Option<Duration> {
        match self {
            Self::Off => None,
            Self::On { every } => Some(every),
        }
    }
}

/// Why a traversal refused to resume from a checkpoint, or to be resumable at all. Always
/// returned before any I/O, as the session's
/// [`TraversalTerminalFailure::Resume`](super::TraversalTerminalFailure::Resume).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResumeRefusal {
    /// Not a checkpoint this crate wrote, or truncated, or corrupted.
    Malformed,
    /// A checkpoint of a format version this build does not read (a development-phase format:
    /// no older version is read; scan again from the start).
    UnsupportedVersion(u8),
    /// A checkpoint of a different traversal: another storage, root, order, filter, depth limit
    /// or version mode.
    Mismatch,
    /// The request's filter gives no [`resume_key`](super::TraversalFilter::resume_key), so a
    /// checkpoint could not tell whether it was taken under the same filter.
    FilterNotResumable,
}

impl fmt::Display for ResumeRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => formatter.write_str("the checkpoint is malformed"),
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "checkpoint format version {version} is not supported"
                )
            }
            Self::Mismatch => formatter.write_str("the checkpoint belongs to another traversal"),
            Self::FilterNotResumable => {
                formatter.write_str("the traversal's filter gives no resume key")
            }
        }
    }
}

impl std::error::Error for ResumeRefusal {}

/// One directory a resumed traversal lists: where, at which depth its children are, and whether
/// the filter still applies to them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FrontierEntry {
    pub(crate) path: StoragePath,
    pub(crate) child_depth: usize,
    pub(crate) filter_children: bool,
}

const MAGIC: [u8; 4] = *b"DMTC";
/// The current format; see the module docs. Bump on any change.
pub(crate) const VERSION: u8 = 1;
/// Bytes of the trailing checksum.
const CHECKSUM: usize = 16;
/// Magic, version, order, fingerprint, count.
const HEADER: usize = 4 + 1 + 1 + 32 + 4;
/// Most entries one checkpoint holds.
pub(crate) const MAX_ENTRIES: usize = 1 << 20;
/// Most bytes one checkpoint holds.
const MAX_BYTES: usize = 64 << 20;
const FILTER_CHILDREN: u8 = 1;

/// The fingerprint a request's checkpoints carry: what decides which directories a traversal
/// lists and what it delivers, and nothing that only tunes it (concurrency, buffering, the
/// observation plan, the checkpoint interval). The storage identity is hashed, never stored.
///
/// # Errors
/// `FilterNotResumable` when the request's filter has no resume key.
pub(crate) fn fingerprint(
    identity: &BackendIdentity,
    request: &TraversalRequest,
) -> Result<[u8; 32], ResumeRefusal> {
    let filter = match &request.filter {
        None => None,
        Some(filter) => Some(
            filter
                .resume_key()
                .ok_or(ResumeRefusal::FilterNotResumable)?,
        ),
    };
    let mut hasher = blake3::Hasher::new_derive_key("data-mover traversal checkpoint fingerprint");
    let mut field = |bytes: &[u8]| {
        hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(bytes);
    };
    field(&[VERSION]);
    field(identity.kind().as_str().as_bytes());
    field(identity.stable_id().as_bytes());
    field(request.root.as_str().as_bytes());
    field(&[order_tag(request.order)]);
    let depth = request
        .max_depth
        .map_or(0, |depth| u64::try_from(depth.get()).unwrap_or(u64::MAX));
    field(&depth.to_le_bytes());
    field(&[versions_tag(request.versions)]);
    match &filter {
        None => field(&[0]),
        Some(key) => {
            field(&[1]);
            field(key);
        }
    }
    Ok(*hasher.finalize().as_bytes())
}

const fn order_tag(order: TraversalOrder) -> u8 {
    match order {
        TraversalOrder::Unordered => 0,
        TraversalOrder::Blocks => 1,
    }
}

const fn versions_tag(versions: TraversalVersions) -> u8 {
    match versions {
        TraversalVersions::Current => 0,
        TraversalVersions::All => 1,
    }
}

/// Encodes `entries` (sorted, or a Blocks chain) as a checkpoint. `None` past the size limits.
pub(crate) fn encode(
    order: TraversalOrder,
    fingerprint: &[u8; 32],
    entries: &[FrontierEntry],
) -> Option<Checkpoint> {
    if entries.len() > MAX_ENTRIES {
        return None;
    }
    let mut out = Vec::with_capacity(HEADER + entries.len() * 24 + CHECKSUM);
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(order_tag(order));
    out.extend_from_slice(fingerprint);
    put_u32(&mut out, entries.len())?;
    let mut previous: &[u8] = &[];
    for entry in entries {
        let path = entry.path.as_str().as_bytes();
        let shared = previous
            .iter()
            .zip(path)
            .take_while(|(left, right)| left == right)
            .count();
        let suffix = path.get(shared..)?;
        put_u32(&mut out, shared)?;
        put_u32(&mut out, suffix.len())?;
        out.extend_from_slice(suffix);
        put_u32(&mut out, entry.child_depth)?;
        out.push(if entry.filter_children {
            FILTER_CHILDREN
        } else {
            0
        });
        if out.len() > MAX_BYTES {
            return None;
        }
        previous = path;
    }
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes().get(..CHECKSUM)?);
    Some(Checkpoint::from_bytes(Bytes::from(out)))
}

fn put_u32(out: &mut Vec<u8>, value: usize) -> Option<()> {
    out.extend_from_slice(&u32::try_from(value).ok()?.to_le_bytes());
    Some(())
}

/// Opens `checkpoint` for a traversal of `root` in `order` whose fingerprint is `fingerprint`:
/// its entries, checked to stay inside `root` (and, in Blocks, to form one chain from it).
///
/// # Errors
/// See [`ResumeRefusal`]: the magic, the version and the checksum first, then the fingerprint,
/// then the entries.
pub(crate) fn open(
    checkpoint: &Checkpoint,
    fingerprint: &[u8; 32],
    order: TraversalOrder,
    root: &StoragePath,
) -> Result<Vec<FrontierEntry>, ResumeRefusal> {
    let bytes = checkpoint.as_bytes();
    if bytes.get(..MAGIC.len()) != Some(&MAGIC[..]) {
        return Err(ResumeRefusal::Malformed);
    }
    match bytes.get(MAGIC.len()) {
        Some(&VERSION) => {}
        Some(&other) => return Err(ResumeRefusal::UnsupportedVersion(other)),
        None => return Err(ResumeRefusal::Malformed),
    }
    let body_len = bytes
        .len()
        .checked_sub(CHECKSUM)
        .filter(|len| *len >= HEADER)
        .ok_or(ResumeRefusal::Malformed)?;
    let (body, checksum) = bytes.split_at(body_len);
    if blake3::hash(body).as_bytes().get(..CHECKSUM) != Some(checksum) {
        return Err(ResumeRefusal::Malformed);
    }
    let mut reader = Reader {
        bytes: body,
        at: MAGIC.len() + 1,
    };
    let tag = reader.u8()?;
    if reader.take(32)? != fingerprint || tag != order_tag(order) {
        return Err(ResumeRefusal::Mismatch);
    }
    let entries = reader.entries()?;
    if !valid(order, root, &entries) {
        return Err(ResumeRefusal::Malformed);
    }
    Ok(entries)
}

/// Reads a checkpoint's body.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take(&mut self, count: usize) -> Result<&[u8], ResumeRefusal> {
        let end = self.at.checked_add(count).ok_or(ResumeRefusal::Malformed)?;
        let taken = self
            .bytes
            .get(self.at..end)
            .ok_or(ResumeRefusal::Malformed)?;
        self.at = end;
        Ok(taken)
    }

    fn u8(&mut self) -> Result<u8, ResumeRefusal> {
        Ok(self.take(1)?.first().copied().unwrap_or_default())
    }

    fn u32(&mut self) -> Result<usize, ResumeRefusal> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| ResumeRefusal::Malformed)?;
        usize::try_from(u32::from_le_bytes(bytes)).map_err(|_| ResumeRefusal::Malformed)
    }

    /// Every entry, and nothing after them.
    fn entries(&mut self) -> Result<Vec<FrontierEntry>, ResumeRefusal> {
        let count = self.u32()?;
        if count > MAX_ENTRIES {
            return Err(ResumeRefusal::Malformed);
        }
        let mut entries = Vec::with_capacity(count.min(4096));
        let mut previous: Vec<u8> = Vec::new();
        // What the entries spell out, shared prefixes included: bounded like an encoded
        // checkpoint, so a crafted one cannot make a few bytes decode into a flood.
        let mut spelled: usize = 0;
        for _ in 0..count {
            let shared = self.u32()?;
            let suffix_len = self.u32()?;
            spelled = spelled.saturating_add(shared).saturating_add(suffix_len);
            if spelled > MAX_BYTES {
                return Err(ResumeRefusal::Malformed);
            }
            let mut path = previous
                .get(..shared)
                .ok_or(ResumeRefusal::Malformed)?
                .to_vec();
            path.extend_from_slice(self.take(suffix_len)?);
            let child_depth = self.u32()?;
            let flags = self.u8()?;
            if child_depth == 0 || flags & !FILTER_CHILDREN != 0 {
                return Err(ResumeRefusal::Malformed);
            }
            let text = String::from_utf8(path.clone()).map_err(|_| ResumeRefusal::Malformed)?;
            entries.push(FrontierEntry {
                path: StoragePath::new(text).map_err(|_| ResumeRefusal::Malformed)?,
                child_depth,
                filter_children: flags & FILTER_CHILDREN != 0,
            });
            previous = path;
        }
        if self.at != self.bytes.len() {
            return Err(ResumeRefusal::Malformed);
        }
        Ok(entries)
    }
}

/// Whether `entries` could have come from a traversal of `root` in `order`. The fingerprint is
/// no signature, so this is what keeps a checkpoint from widening a scan beyond its root.
fn valid(order: TraversalOrder, root: &StoragePath, entries: &[FrontierEntry]) -> bool {
    match order {
        TraversalOrder::Unordered => entries.iter().all(|entry| within(root, &entry.path)),
        TraversalOrder::Blocks => {
            let root_first = entries
                .first()
                .is_none_or(|first| first.path == *root && first.child_depth == 1);
            root_first
                && entries.windows(2).all(|pair| {
                    let [parent, child] = pair else { return false };
                    child.child_depth == parent.child_depth.saturating_add(1)
                        && is_child(parent.path.as_str(), child.path.as_str())
                })
        }
    }
}

/// Whether `child` is spelled as a direct child of `parent`: the parent's path verbatim, a `/`
/// (none after the storage root), and one name.
pub(crate) fn is_child(parent: &str, child: &str) -> bool {
    let Some(rest) = child.strip_prefix(parent) else {
        return false;
    };
    let name = if parent.is_empty() {
        Some(rest)
    } else {
        rest.strip_prefix('/')
    };
    name.is_some_and(|name| !name.is_empty() && !name.contains('/'))
}

#[cfg(test)]
mod tests;
