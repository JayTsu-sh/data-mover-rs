//! Test seams inside the Local namespace's blocking calls.

use std::ffi::OsStr;
use std::io;
use std::path::Path;
#[cfg(any(test, feature = "test-support"))]
use std::sync::Arc;

/// A test's action on one native path (relative to the root).
#[cfg(any(test, feature = "test-support"))]
pub type Hook = Arc<dyn Fn(&Path) + Send + Sync>;

/// A test's stand-in for a failing directory read: given a listed directory's native path and how
/// many of its entries were read so far, the error the next read returns instead, if any.
#[cfg(any(test, feature = "test-support"))]
pub type ReadFailure = Arc<dyn Fn(&Path, usize) -> Option<io::Error> + Send + Sync>;

/// Test seams inside a blocking namespace call: before a directory is opened for listing, between
/// reading a directory entry and describing it, and before reading a link, so a test can remove
/// an entry in exactly that window; and before each directory read, so a test can fail one.
/// Outside tests this is a zero-sized type whose calls do nothing.
#[derive(Clone, Default)]
pub(super) struct Hooks {
    #[cfg(any(test, feature = "test-support"))]
    pub(super) listing: Option<Hook>,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) describing: Option<Hook>,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) reading_link: Option<Hook>,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) reading_directory: Option<ReadFailure>,
}

impl Hooks {
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn before_list(&self, native: &Path) {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(hook) = &self.listing {
            hook(native);
        }
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = native;
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn before_describe(&self, directory: &Path, name: &OsStr) {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(hook) = &self.describing {
            hook(&directory.join(name));
        }
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = (directory, name);
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn before_read_link(&self, native: &Path) {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(hook) = &self.reading_link {
            hook(native);
        }
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = native;
    }

    /// The error the read of entry number `read` (0-based) of the directory at `native` returns
    /// instead of an entry, when a test injects one.
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn read_failure(&self, native: &Path, read: usize) -> Option<io::Error> {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(hook) = &self.reading_directory {
            return hook(native, read);
        }
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = (native, read);
        None
    }
}
