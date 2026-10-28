//! The listing contract's path arithmetic: a child's path is its directory's path as requested,
//! a `/`, and its name.

use crate::model::{EntryKind, MAX_MODEL_FIELD_BYTES, ModelValueError, StoragePath};

/// The full path of `name` in `directory`: the name itself at the root, otherwise the directory,
/// `/` and the name. This is the listing contract; no normalisation is applied to either part.
///
/// # Errors
/// Returns an error when the joined path exceeds the storage path limit or `name` holds NUL.
pub fn child_path(directory: &StoragePath, name: &str) -> Result<StoragePath, ModelValueError> {
    let parent = directory.as_str();
    if parent.is_empty() {
        return StoragePath::new(name);
    }
    let mut joined = String::with_capacity(parent.len() + 1 + name.len());
    joined.push_str(parent);
    joined.push('/');
    joined.push_str(name);
    StoragePath::new(joined)
}

/// [`child_path`] built in `name`'s own allocation when it has the room (a backend that cut the
/// name out of a full path keeps it), so a listed child costs no second string. Returns the name
/// when the joined path cannot be a [`StoragePath`]: it would hold NUL or exceed the limit, which
/// is everything [`StoragePath::new`] refuses (the directory, being one, holds no NUL).
pub(super) fn child_path_owned(
    directory: &StoragePath,
    mut name: String,
) -> Result<StoragePath, String> {
    let parent = directory.as_str();
    let joined = if parent.is_empty() {
        name.len()
    } else {
        parent.len() + 1 + name.len()
    };
    if joined > MAX_MODEL_FIELD_BYTES || name.contains('\0') {
        return Err(name);
    }
    if !parent.is_empty() {
        name.reserve_exact(joined - name.len());
        name.insert(0, '/');
        name.insert_str(0, parent);
    }
    Ok(StoragePath::checked(name))
}

/// `child` spelled again as `directory`, `/` and its last segment, for a backend whose server
/// hands back normalised paths (`a/b/x` for a listing of `a//b`, `x` for one of `./`). Only a
/// child whose parent is `directory` up to empty and `.` segments is respelled; any other path is
/// returned as it is, so [`super::ListingPage::from_descriptors`] refuses it rather than a stray child
/// being renamed into this directory.
#[must_use]
pub(crate) fn respelled(directory: &StoragePath, child: StoragePath) -> StoragePath {
    if child_name(directory, &child).is_some() {
        return child;
    }
    let (parent, name) = child
        .as_str()
        .rsplit_once('/')
        .unwrap_or(("", child.as_str()));
    if !is_segment(name) || !same_directory(parent, directory.as_str()) {
        return child;
    }
    child_path(directory, name).unwrap_or(child)
}

/// Whether two directory spellings name the same directory up to empty and `.` segments.
fn same_directory(left: &str, right: &str) -> bool {
    segments(left).eq(segments(right))
}

/// The segments of a directory spelling that name something: not empty, not `.`.
fn segments(value: &str) -> impl Iterator<Item = &str> {
    value
        .split('/')
        .filter(|segment| !matches!(*segment, "" | "."))
}

/// The name `path` has as a child of `directory`, when it is exactly one segment below it.
pub(super) fn child_name<'a>(directory: &StoragePath, path: &'a StoragePath) -> Option<&'a str> {
    let parent = directory.as_str();
    let rest = if parent.is_empty() {
        path.as_str()
    } else {
        path.as_str().strip_prefix(parent)?.strip_prefix('/')?
    };
    is_segment(rest).then_some(rest)
}

/// Whether `name` is one path segment a child may be named. It does not apply to the directory's
/// own object, whose name is empty by design and which only
/// [`super::ListedChild::directory_object`] and [`super::ListingPage::push_directory_object`] make.
pub(super) fn is_segment(name: &str) -> bool {
    !matches!(name, "" | "." | "..") && !name.contains(['/', '\0'])
}

/// Whether an optional trailing slash names one file key segment. Ordinary child names and
/// the empty own-object name retain their separate constructor checks.
pub(super) fn valid_object_suffix(name: &str, kind: EntryKind) -> bool {
    !name.ends_with('/')
        || (kind == EntryKind::File && name.strip_suffix('/').is_some_and(is_segment))
}
