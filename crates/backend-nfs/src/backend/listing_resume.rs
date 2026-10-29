//! What an NFS listing keeps so it can continue a directory from a cookie (P7 C7c, Q18).
//!
//! [`Position`]: where the next READDIRPLUS read starts (the cookie after the last reply taken
//! whole, with its verifier and the handle it belongs to) and the cookies the current read has
//! reached, so a server whose cookies loop is caught instead of followed.
//!
//! [`Taken`]: a keyed 64-bit digest of `(fileid, name)` of every entry the listing has taken, so
//! an entry the server sends again after the listing continued — part of a reply taken before
//! the failure, or the whole directory from a server that restarted it without saying so, as a
//! server that does not check cookies may — is skipped instead of handed over twice. The name
//! alone would not do: nfs-rs decodes names lossily, so two different names can arrive as one.
//! The digests are bounded ([`MAX_REMEMBERED`]); past that the listing can no longer continue
//! from a cookie without risking duplicates. A failure then ends a listing that already handed
//! a page over; before that it can discard the buffer and retry from the start with a fixed
//! budget. Remembering never starts again for that listing.

use std::collections::HashSet;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::mem;

use bytes::Bytes;
use nfs_rs::DirectoryCookie;

/// The most entries one listing remembers: 2^18 digests, about 4.5 MiB at the most per listing,
/// so a directory of tens of millions of entries does not make its listing's memory grow with it.
pub(super) const MAX_REMEMBERED: usize = 1 << 18;

/// Where the next read of a listing starts, and what the current read has reached.
#[derive(Default)]
pub(super) struct Position {
    /// The cookie after the last reply taken whole ([`DirectoryCookie::default`]: the start).
    at: DirectoryCookie,
    /// The directory handle `at` was reached with: a cookie means nothing on another handle.
    of: Option<Bytes>,
    /// Where the current read started.
    started: DirectoryCookie,
    /// The cookies the current read has reached, and the directory's start. Not the cookie it
    /// started from: a server that ignores it and sends the directory from its start passes it
    /// again, and the listing then skips what it already took instead of giving up. A loop is
    /// still caught within one turn of it. One `u64` per reply of the current read: a few KiB for
    /// a directory of millions (replies hold tens to hundreds of entries), cleared at every read.
    reached: HashSet<u64>,
    /// Entries the server sent in the current read, skipped ones included.
    sent: usize,
}

impl Position {
    /// Starts a read with `handle` and returns where it starts: where the last read got to when
    /// that was with the same handle, the directory's start otherwise.
    pub(super) fn begin(&mut self, handle: &Bytes) -> DirectoryCookie {
        if self.of.as_ref() != Some(handle) {
            self.at = DirectoryCookie::default();
            self.of = Some(handle.clone());
        }
        self.started = self.at;
        self.reached.clear();
        self.reached.insert(DirectoryCookie::default().cookie);
        self.sent = 0;
        self.at
    }

    /// The next read starts at the directory's start.
    pub(super) fn rewind(&mut self) {
        self.at = DirectoryCookie::default();
    }

    /// Counts an entry the server sent in the current read.
    pub(super) fn sent(&mut self) {
        self.sent = self.sent.saturating_add(1);
    }

    /// Records that the current read got to `next`; `false` when it had already been there.
    pub(super) fn reach(&mut self, next: DirectoryCookie) -> bool {
        if !self.reached.insert(next.cookie) {
            return false;
        }
        self.at = next;
        true
    }

    /// Whether the current read continued from a cookie and the server answered it with the end
    /// of the directory and no entry — which is also how servers answer a cookie they do not
    /// know (ONTAP and DXN-2 alike), so the rest of the directory may have been skipped.
    pub(super) fn ended_at_once(&self) -> bool {
        self.started != DirectoryCookie::default() && self.sent == 0
    }
}

/// The entries a listing has taken. See the [module docs](self).
pub(super) struct Taken {
    digests: Option<HashSet<u64, BuildHasherDefault<Digested>>>,
    keys: RandomState,
    /// The entry the reader asked about last and was told to take: it is taken once it is pushed
    /// or hidden, not before — a read that fails while looking at it must not skip it later.
    pending: Option<u64>,
    /// Entries taken since [`Self::progressed`] was last asked.
    fresh: usize,
}

impl Taken {
    pub(super) fn new() -> Self {
        Self {
            digests: Some(HashSet::default()),
            keys: RandomState::new(),
            pending: None,
            fresh: 0,
        }
    }

    /// Whether the entry `(fileid, name)` has not been taken yet (always, once the listing no
    /// longer remembers).
    pub(super) fn first_sight(&mut self, fileid: u64, name: &str) -> bool {
        self.pending = None;
        let Some(digests) = &self.digests else {
            return true;
        };
        let mut hasher = self.keys.build_hasher();
        fileid.hash(&mut hasher);
        name.hash(&mut hasher);
        let digest = hasher.finish();
        if digests.contains(&digest) {
            return false;
        }
        self.pending = Some(digest);
        true
    }

    /// The entry last seen for the first time was pushed or hidden.
    pub(super) fn take(&mut self) {
        let Some(digest) = self.pending.take() else {
            return;
        };
        self.fresh = self.fresh.saturating_add(1);
        match &mut self.digests {
            Some(digests) if digests.len() < MAX_REMEMBERED => {
                digests.insert(digest);
            }
            Some(_) => self.digests = None,
            None => {}
        }
    }

    /// Whether every entry taken so far is remembered, so continuing from a cookie repeats none.
    pub(super) const fn remembers(&self) -> bool {
        self.digests.is_some()
    }

    /// Whether an entry was taken since the last time this was asked.
    pub(super) fn progressed(&mut self) -> bool {
        mem::take(&mut self.fresh) > 0
    }
}

/// Hashes a digest by being it: the digests are already keyed hashes.
#[derive(Default)]
struct Digested(u64);

impl Hasher for Digested {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(*byte);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

/// What made a read the listing could not trust, for the failure's diagnostic.
#[derive(Clone, Copy)]
pub(super) enum Anomaly {
    /// [`Position::reach`] said no.
    RepeatedCookie,
    /// [`Position::ended_at_once`].
    EmptyAfterCookie,
}

impl fmt::Debug for Anomaly {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RepeatedCookie => "DIRECTORY_COOKIE_REPEATED",
            Self::EmptyAfterCookie => "EMPTY_AFTER_COOKIE",
        })
    }
}

impl fmt::Display for Anomaly {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RepeatedCookie => {
                "the server handed back a directory cookie this read had already reached"
            }
            Self::EmptyAfterCookie => {
                "the server answered a continued directory cookie with its end and no entry"
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cookie(cookie: u64) -> DirectoryCookie {
        DirectoryCookie {
            cookie,
            verifier: [7; 8],
        }
    }

    /// A read continues where the last one got to with the same handle, and from the start with
    /// another; cookies loop only within one read.
    #[test]
    fn a_position_is_kept_per_handle_and_loops_are_per_read() {
        let (first, other) = (Bytes::from_static(b"fh"), Bytes::from_static(b"fh2"));
        let mut position = Position::default();
        assert_eq!(position.begin(&first), DirectoryCookie::default());
        assert!(position.reach(cookie(10)) && position.reach(cookie(20)));
        assert!(!position.reach(cookie(10)), "10 again in the same read");
        assert!(!position.reach(cookie(0)), "back to the start");
        assert_eq!(position.begin(&first), cookie(20));
        assert!(position.ended_at_once());
        position.sent();
        assert!(!position.ended_at_once());
        assert!(position.reach(cookie(10)), "a new read may pass 10 again");
        assert!(position.reach(cookie(20)), "and the cookie it started from");
        assert!(!position.reach(cookie(10)));
        assert_eq!(position.begin(&other), DirectoryCookie::default());
        assert!(!position.ended_at_once());
        position.rewind();
        assert_eq!(position.begin(&other), DirectoryCookie::default());
    }

    /// An entry is taken only once pushed; the same `(fileid, name)` is then skipped, another
    /// fileid under the same (lossily decoded) name is not.
    #[test]
    fn an_entry_is_remembered_once_taken() {
        let mut taken = Taken::new();
        assert!(taken.first_sight(1, "a\u{fffd}"));
        assert!(taken.first_sight(1, "a\u{fffd}"), "looked at, not taken");
        taken.take();
        assert!(!taken.first_sight(1, "a\u{fffd}"));
        assert!(taken.first_sight(2, "a\u{fffd}"));
        taken.take();
        assert!(taken.progressed());
        assert!(!taken.progressed());
        taken.take();
        assert!(!taken.progressed(), "nothing pending, nothing taken");
    }

    /// Past [`MAX_REMEMBERED`] the listing forgets everything and says so.
    #[test]
    fn remembering_is_bounded() {
        let mut taken = Taken::new();
        for fileid in 0..=u64::try_from(MAX_REMEMBERED).unwrap_or(u64::MAX) {
            assert!(taken.first_sight(fileid, "f"));
            taken.take();
        }
        assert!(!taken.remembers());
        assert!(taken.first_sight(0, "f"), "no longer remembered");
    }
}
