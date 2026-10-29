//! A child a listing returned but could not describe.

use bytes::Bytes;

use super::listing_failure;
use super::names::{child_name, child_path, respelled};
use crate::model::{
    BackendIdentity, EntryFailureIdentity, EntryOperationFailure, FailureClass, ModelValueError,
    Operation, StoragePath, Transience,
};

/// A child the listing returned but could not describe (P7 §5.4.4, §8).
///
/// Two kinds are distinguished by the failure's class:
///
/// - **Vanished** ([`FailureClass::NotFound`], [`Self::is_vanished`]): the child was gone by the
///   time the listing went to describe it (a Local `lstat` after `readdir`). It is no failure: a
///   traversal counts it as `vanished` and emits nothing for it (Q12).
/// - Anything else is that child's failure, beside its siblings. A child whose name cannot be
///   spelled as a path at all (not UTF-8, a name a protocol library decoded lossily, a key outside
///   the listed prefix) has no `name`: its failure is at the **directory's** path and carries the
///   raw bytes in `raw_name` and in the diagnostic ([`Self::unnamed`]).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildFailure {
    /// The child's name, when it could be spelled.
    pub name: Option<String>,
    /// The name's raw bytes, when it could not be spelled: the native bytes (Local on Unix), the
    /// UTF-16LE code units (Local on Windows), or the text as the server sent it (an S3 key).
    pub raw_name: Option<Bytes>,
    /// What went wrong, at the child's path — or at the directory's, when the child has none.
    pub failure: EntryOperationFailure,
}

impl ChildFailure {
    /// The failure of a child of `directory`, named when its path is one name below `directory`.
    #[must_use]
    pub fn from_failure(directory: &StoragePath, failure: EntryOperationFailure) -> Self {
        let named = respelled(directory, failure.path().clone());
        Self {
            name: child_name(directory, &named).map(str::to_owned),
            raw_name: None,
            failure,
        }
    }

    /// A child of `directory` whose name no path can spell. The failure is at `directory`, its
    /// diagnostic is `reason` followed by the raw name (escaped), and its
    /// [`EntryFailureIdentity`] is derived from the raw name, so two such children stay distinct.
    #[must_use]
    pub fn unnamed(
        backend: &BackendIdentity,
        directory: &StoragePath,
        raw_name: Bytes,
        class: FailureClass,
        reason: &str,
    ) -> Self {
        let diagnostic = format!("{reason}; raw name \"{}\"", raw_name.escape_ascii());
        let failure = EntryOperationFailure::new(
            directory.clone(),
            Operation::Traverse,
            class,
            Transience::Permanent,
            diagnostic,
        )
        .unwrap_or_else(|_| listing_failure(directory, class, Transience::Permanent, UNNAMED));
        Self {
            name: None,
            failure: failure.with_identity(EntryFailureIdentity::derive(
                backend,
                &identity_bytes(directory, &raw_name),
            )),
            raw_name: Some(raw_name),
        }
    }

    /// The child `name` of `directory` was listed and was gone when the listing described it.
    ///
    /// # Errors
    /// Returns an error when `name` does not join `directory` into a storage path.
    pub fn vanished(directory: &StoragePath, name: &str) -> Result<Self, ModelValueError> {
        let path = child_path(directory, name)?;
        let failure = EntryOperationFailure::new(
            path,
            Operation::Traverse,
            FailureClass::NotFound,
            Transience::Permanent,
            "the child was gone by the time the listing described it",
        )?;
        Ok(Self {
            name: Some(name.to_owned()),
            raw_name: None,
            failure,
        })
    }

    /// Whether the child vanished between the listing and its description: not a failure.
    /// Only a named child can have vanished; see [`is_vanished`].
    #[must_use]
    pub fn is_vanished(&self) -> bool {
        self.name.is_some() && self.failure.class() == FailureClass::NotFound
    }
}

/// Whether `failure`, one of the child failures a listing of `directory` returned, says that a
/// child vanished before the listing could describe it — not a failure (P7 Q12). A `NotFound`
/// at the directory's own path cannot name the child it was about (a read of the directory
/// failed part way), so it is a failure, which keeps every child of the directory out of
/// deletion. Every caller that sorts a listing's failures uses this one test.
#[must_use]
pub fn is_vanished(directory: &StoragePath, failure: &EntryOperationFailure) -> bool {
    failure.class() == FailureClass::NotFound && failure.path() != directory
}

const UNNAMED: &str = "the listing returned a child whose name no storage path can spell";

/// What an unnamed child's identity is derived from: its directory and its raw name, so the same
/// raw name in two directories stays two children.
fn identity_bytes(directory: &StoragePath, raw_name: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(directory.as_str().len() + 1 + raw_name.len());
    bytes.extend_from_slice(directory.as_str().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(raw_name);
    bytes
}
