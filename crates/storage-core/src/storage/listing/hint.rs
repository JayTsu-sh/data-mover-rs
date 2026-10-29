//! The descent hint: what a listing may hand back with a child directory so the listing of that
//! child can skip resolving its path again (P7 §12.7).

use std::fmt;

use bytes::Bytes;

/// A backend-private, opaque hint for listing one child directory, carried from the listing that
/// returned the child to the [`ListRequest`](super::ListRequest) that lists it.
///
/// A backend sets it on a [`ListedChild`](super::ListedChild) with
/// [`ListedChild::with_descent_hint`](super::ListedChild::with_descent_hint); a traversal that
/// descends into that child passes it back on the child's own request. Only the backend that made
/// it may read it, and only as a hint: a request without one, or with one that no longer holds (an
/// NFS handle gone stale), must still list the path. Nothing outside the backend interprets it,
/// and it is never part of an entry's snapshot or identity.
///
/// It always holds its own copy of the bytes. A hint that pointed into a page's reply buffer would
/// keep that whole reply alive for as long as the directory waits to be listed, and a traversal
/// can hold one hint per pending directory (§12.7: the NFS handle cache measured about 625 B per
/// directory that way).
///
/// `Debug` shows only the length: a hint may be an access capability (an NFS file handle reaches
/// the file without its path).
#[derive(Clone, Eq, PartialEq)]
pub struct DescentHint(Bytes);

impl DescentHint {
    /// A hint holding a copy of `bytes`.
    #[must_use]
    pub fn copied_from(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }

    /// The hint's bytes, for the backend that made it.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for DescentHint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescentHint")
            .field("len", &self.0.len())
            .finish_non_exhaustive()
    }
}
