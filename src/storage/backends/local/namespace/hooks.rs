//! Test seams inside the Local namespace's blocking calls.

use std::ffi::OsStr;
use std::path::Path;
#[cfg(test)]
use std::sync::Arc;

/// A test's action on one native path (relative to the root).
#[cfg(test)]
pub(crate) type Hook = Arc<dyn Fn(&Path) + Send + Sync>;

/// Test seams inside a blocking namespace call: before a directory is opened for listing, between
/// reading a directory entry and describing it, and before reading a link, so a test can remove
/// an entry in exactly that window. Outside tests this is a zero-sized type whose calls do
/// nothing.
#[derive(Clone, Default)]
pub(super) struct Hooks {
    #[cfg(test)]
    pub(super) listing: Option<Hook>,
    #[cfg(test)]
    pub(super) describing: Option<Hook>,
    #[cfg(test)]
    pub(super) reading_link: Option<Hook>,
}

impl Hooks {
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn before_list(&self, native: &Path) {
        #[cfg(test)]
        if let Some(hook) = &self.listing {
            hook(native);
        }
        #[cfg(not(test))]
        let _ = native;
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn before_describe(&self, directory: &Path, name: &OsStr) {
        #[cfg(test)]
        if let Some(hook) = &self.describing {
            hook(&directory.join(name));
        }
        #[cfg(not(test))]
        let _ = (directory, name);
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn before_read_link(&self, native: &Path) {
        #[cfg(test)]
        if let Some(hook) = &self.reading_link {
            hook(native);
        }
        #[cfg(not(test))]
        let _ = native;
    }
}
