//! A key's history in a versioned object store (P7 §10, Q7 / Q8): every stored version and
//! delete marker of one key, oldest first, the latest last — what a traversal of a versioned
//! bucket hands over with each key's one entry
//! ([`ObservedEntry::history`](super::ObservedEntry::history)).
//!
//! A history longer than [`MAX_HISTORY_RECORDS`] is delivered in segments: several entries of the
//! same key, each with its segment number (from 0) and only the last one
//! [`complete`](ObjectHistory::is_complete). A consumer treats a key's history as whole only once
//! it has the segment that is complete; a resumed traversal delivers a key's segments again from 0.

use super::{
    MetadataObservations, ModelValueError, SourceIdentity, SourceVersion, StorageTimestamp,
};

/// The most records one entry's history carries (P7 Q8); a longer history is split into segments.
pub const MAX_HISTORY_RECORDS: usize = 100_000;

/// The version id an unversioned bucket, or an object written before versioning, reports.
const NULL_VERSION: &str = "null";

/// Where the order of a key's history — its versions and delete markers, oldest first — came from.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum HistoryOrder {
    /// The server's own listing order: `ListObjectVersions` lists each key's records newest first,
    /// versions and delete markers interleaved in one sequence, and the listing passed the checks
    /// (every record of the key listed together, exactly one latest record and it first). Exact
    /// even for records written in the same millisecond.
    Server,
    /// Inferred from `LastModified` (millisecond resolution; the latest record last) because the
    /// server's order could not be used: the listing broke one of the checks (a store whose
    /// listing order is wrong), or the response's document order could not be read. Records
    /// written in the same millisecond may be out of order.
    Inferred,
}

/// What one record is.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecordKind {
    /// A stored version: its size, `ETag` and `LastModified` as the listing gave them.
    Object {
        size: u64,
        etag: String,
        modified: Option<StorageTimestamp>,
    },
    /// A delete marker: no content, nothing to copy.
    DeleteMarker { modified: Option<StorageTimestamp> },
}

/// One stored version, or delete marker, of a key as the listing reported it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionRecord {
    /// `None` for the `"null"` version: the one an unversioned (or suspended) bucket keeps.
    version_id: Option<String>,
    /// The listing's `IsLatest`, as reported.
    latest: bool,
    kind: RecordKind,
    /// What identifies exactly this record: `VersionScoped` by its id, or, for a `"null"` one,
    /// path-scoped (the path and `ETag` of a version, the path and a marker tag of a marker).
    identity: SourceIdentity,
    /// What the observation plan asked beyond the listing, observed bound to this record; `None`
    /// when nothing was (the listing's facts are the record's own fields). Boxed: most records
    /// carry none.
    metadata: Option<Box<MetadataObservations>>,
}

impl VersionRecord {
    /// A record as the listing spelled it: `"null"` (or an empty id) names no real version.
    ///
    /// # Errors
    /// Returns an error for a real id that is longer than 1024 bytes or contains NUL.
    pub(crate) fn from_listing(
        reported_id: impl Into<String>,
        latest: bool,
        kind: RecordKind,
        identity: SourceIdentity,
    ) -> Result<Self, ModelValueError> {
        let reported_id = reported_id.into();
        let id = (!reported_id.is_empty() && reported_id != NULL_VERSION).then_some(reported_id);
        Self::new(id, latest, kind, identity, None)
    }

    /// # Errors
    /// Returns an error for a malformed id, or the spelled-out `"null"` id.
    pub(crate) fn new(
        version_id: Option<String>,
        latest: bool,
        kind: RecordKind,
        identity: SourceIdentity,
        metadata: Option<Box<MetadataObservations>>,
    ) -> Result<Self, ModelValueError> {
        if let Some(id) = &version_id {
            SourceVersion::Id(id.clone()).validate()?;
            if id == NULL_VERSION {
                return Err(ModelValueError::new(
                    "version",
                    "the null version carries no version id",
                ));
            }
        }
        Ok(Self {
            version_id,
            latest,
            kind,
            identity,
            metadata,
        })
    }

    /// The real version id; `None` for the `"null"` version.
    #[must_use]
    pub fn version_id(&self) -> Option<&str> {
        self.version_id.as_deref()
    }

    /// Whether the listing reported this record as the key's latest (`IsLatest`).
    #[must_use]
    pub const fn is_latest(&self) -> bool {
        self.latest
    }

    /// What the record is.
    #[must_use]
    pub const fn kind(&self) -> &RecordKind {
        &self.kind
    }

    /// Whether this record is a delete marker: it has no content and cannot be copied.
    #[must_use]
    pub const fn is_delete_marker(&self) -> bool {
        matches!(self.kind, RecordKind::DeleteMarker { .. })
    }

    /// The version's size; `None` for a delete marker.
    #[must_use]
    pub const fn size(&self) -> Option<u64> {
        match &self.kind {
            RecordKind::Object { size, .. } => Some(*size),
            RecordKind::DeleteMarker { .. } => None,
        }
    }

    /// The version's `ETag`; `None` for a delete marker.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        match &self.kind {
            RecordKind::Object { etag, .. } => Some(etag),
            RecordKind::DeleteMarker { .. } => None,
        }
    }

    /// When the record was written, as the listing said.
    #[must_use]
    pub const fn modified(&self) -> Option<StorageTimestamp> {
        match &self.kind {
            RecordKind::Object { modified, .. } | RecordKind::DeleteMarker { modified } => {
                *modified
            }
        }
    }

    /// What identifies exactly this record, for a copy that must read what was listed.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    /// What was observed of this record beyond the listing, when the plan asked for any.
    #[must_use]
    pub fn metadata(&self) -> Option<&MetadataObservations> {
        self.metadata.as_deref()
    }

    pub(crate) fn set_metadata(&mut self, metadata: MetadataObservations) {
        self.metadata = Some(Box::new(metadata));
    }

    /// The selector that copies exactly this record with `TransferRequest::with_source_version`:
    ///
    /// - a real version: `Id(id)`;
    /// - the `"null"` version while it is the latest: `Current` (it is what `Current` copies, and
    ///   an unversioned bucket names no other);
    /// - the `"null"` version after newer records were written: `Id("null")`, because `Current`
    ///   would copy a newer version;
    /// - a delete marker: `None`.
    #[must_use]
    pub fn source(&self) -> Option<SourceVersion> {
        if self.is_delete_marker() {
            return None;
        }
        Some(match (&self.version_id, self.latest) {
            (Some(id), _) => SourceVersion::Id(id.clone()),
            (None, true) => SourceVersion::Current,
            (None, false) => SourceVersion::Id(NULL_VERSION.to_string()),
        })
    }
}

/// A key's history, or one segment of it (P7 §10.1, Q8).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectHistory {
    /// Oldest first within the segment; the key's last record is its latest.
    records: Vec<VersionRecord>,
    order: HistoryOrder,
    /// From 0.
    segment: u32,
    /// Only the key's last segment is complete.
    complete: bool,
    /// The largest stored version of the whole key, whatever the segment; `None` when it has none.
    largest: Option<u64>,
}

impl ObjectHistory {
    /// A key's whole history as a listing gathered it: segment 0, complete, any length (a traversal
    /// splits it into segments of at most [`MAX_HISTORY_RECORDS`] before handing it over).
    ///
    /// # Errors
    /// Returns an error for a history without records.
    pub(crate) fn whole(
        records: Vec<VersionRecord>,
        order: HistoryOrder,
    ) -> Result<Self, ModelValueError> {
        if records.is_empty() {
            return Err(empty());
        }
        let largest = records.iter().filter_map(VersionRecord::size).max();
        Ok(Self {
            records,
            order,
            segment: 0,
            complete: true,
            largest,
        })
    }

    /// One segment, as a snapshot stored it; `largest` is the whole key's largest version.
    ///
    /// # Errors
    /// Returns an error for a segment without records, with more than [`MAX_HISTORY_RECORDS`], or
    /// with a version larger than `largest`.
    pub(crate) fn segment_of(
        records: Vec<VersionRecord>,
        order: HistoryOrder,
        segment: u32,
        complete: bool,
        largest: Option<u64>,
    ) -> Result<Self, ModelValueError> {
        if records.is_empty() {
            return Err(empty());
        }
        if records.len() > MAX_HISTORY_RECORDS {
            return Err(ModelValueError::new(
                "history",
                "a history segment holds at most MAX_HISTORY_RECORDS records",
            ));
        }
        let own = records.iter().filter_map(VersionRecord::size).max();
        if own.is_some_and(|own| largest.is_none_or(|largest| own > largest)) {
            return Err(ModelValueError::new(
                "history",
                "a version is larger than the key's largest",
            ));
        }
        Ok(Self {
            records,
            order,
            segment,
            complete,
            largest,
        })
    }

    /// The records of this segment, oldest first.
    #[must_use]
    pub fn records(&self) -> &[VersionRecord] {
        &self.records
    }

    pub(crate) fn records_mut(&mut self) -> &mut [VersionRecord] {
        &mut self.records
    }

    /// Where the history's order came from (see [`HistoryOrder`]).
    #[must_use]
    pub const fn order(&self) -> HistoryOrder {
        self.order
    }

    /// This segment's number, from 0.
    #[must_use]
    pub const fn segment(&self) -> u32 {
        self.segment
    }

    /// The largest stored version of the whole key — of every segment, not only this one — so a
    /// decision about the key (P7 R3: can every version be written?) is the same on each segment.
    /// `None` when the key holds only delete markers.
    #[must_use]
    pub const fn largest_version(&self) -> Option<u64> {
        self.largest
    }

    /// Whether this is the key's last segment: only then is its last record the key's latest.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.complete
    }

    /// The key's latest record — a version, or a delete marker — on its last segment; `None` on
    /// any other segment.
    #[must_use]
    pub fn latest(&self) -> Option<&VersionRecord> {
        self.records.last().filter(|_| self.complete)
    }

    /// The key's current object: its latest record when that is a version. `None` when the key is
    /// deleted now (its latest record is a delete marker), and on any segment but the last.
    #[must_use]
    pub fn current(&self) -> Option<&VersionRecord> {
        self.latest().filter(|record| !record.is_delete_marker())
    }

    /// This history in segments of at most `cap` records (at least one), numbered from this one's,
    /// only the last complete when this one is. `cap` 0 counts as 1.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn split(self, cap: usize) -> Vec<Self> {
        let cap = cap.max(1);
        let Self {
            records,
            order,
            segment,
            complete,
            largest,
        } = self;
        let count = records.len().div_ceil(cap).max(1);
        let mut segments = Vec::with_capacity(count);
        let mut records = records.into_iter();
        for index in 0..count {
            let last = index + 1 == count;
            segments.push(Self {
                records: records.by_ref().take(cap).collect(),
                order,
                segment: segment.saturating_add(u32::try_from(index).unwrap_or(u32::MAX)),
                complete: complete && last,
                largest,
            });
        }
        segments
    }
}

fn empty() -> ModelValueError {
    ModelValueError::new("history", "a history holds at least one record")
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;
