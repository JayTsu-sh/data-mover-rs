//! Bridges the filter DSL (`crate::filter`) to the role-based traversal seam
//! (`crate::traversal::TraversalFilter`).
//!
//! The traversal module must stay independent of the expression language, so the adapter
//! lives here at the crate root and is handed to `TraversalRequest::filter` as a trait
//! object.
//!
//! Mapping from the `should_skip` triple `(should_skip, continue_scan, check_children)`:
//!
//! | triple component | `TraversalDecision` field |
//! |---|---|
//! | `should_skip`  | `emit = !should_skip` |
//! | `continue_scan` | `descend` (only meaningful for directories) |
//! | `check_children` | `filter_children` (transitive: once `false`, the subtree is admitted unfiltered) |
//!
//! Expressions referencing `modified` declare a timestamp dependency. The caller explicitly
//! enables timestamps in its observation plan: `InlineOnly` uses listed times, while modes
//! allowing additional calls can fill missing times. A decision waits for the observation to
//! settle, not for a timestamp to exist. A final missing time retains the DSL's `LazyMatch`
//! semantics; an observation still pending must never be mistaken for a final missing value.

use crate::filter::{FilterExpression, FilterInput, parse_filter_expression, should_skip};
use crate::model::{EntryKind, MetadataFamily, MetadataRequirements};
use crate::traversal::{TraversalCandidate, TraversalDecision, TraversalFilter};

#[cfg(test)]
mod local_tests;

/// `match` / `exclude` filter expressions applied during a role-based traversal.
#[derive(Debug)]
pub struct DslTraversalFilter {
    match_expressions: Option<FilterExpression>,
    exclude_expressions: Option<FilterExpression>,
    requirements: MetadataRequirements,
    /// What a resumable traversal binds its checkpoints to: see [`resume_key`].
    resume_key: [u8; 32],
}

/// A key of the DSL semantics version and both parsed expressions: equal keys decide alike.
/// Their derived `Debug` form spells out every condition, operator, value and glob, and keeps
/// a relative time such as `modified < 7d` relative. A new DSL version may spell a tree differently;
/// a checkpoint then refuses to resume, which is the safe direction.
///
/// A relative time is evaluated against the moment of each decision, so a traversal resumed a day
/// later keeps the same key under a window a day later.
///
/// Version 2 gates unfiltered subtree admission. Old checkpoints may contain unsafe
/// `filter_children = false` frontiers and must be refused even when their expressions are identical.
fn resume_key(
    match_expressions: Option<&FilterExpression>,
    exclude_expressions: Option<&FilterExpression>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("data-mover DSL traversal filter v2");
    hasher.update(format!("{match_expressions:?}").as_bytes());
    hasher.update(&[0]);
    hasher.update(format!("{exclude_expressions:?}").as_bytes());
    *hasher.finalize().as_bytes()
}

impl DslTraversalFilter {
    /// Wraps already parsed expressions. Both `None` admits everything.
    #[must_use]
    pub fn new(
        match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>,
    ) -> Self {
        let needs_modified = [&match_expressions, &exclude_expressions]
            .into_iter()
            .flatten()
            .any(|expression| expression.referenced_fields().modified);
        let requirements = if needs_modified {
            MetadataRequirements::NONE.with(MetadataFamily::Timestamps)
        } else {
            MetadataRequirements::NONE
        };
        Self {
            resume_key: resume_key(match_expressions.as_ref(), exclude_expressions.as_ref()),
            match_expressions,
            exclude_expressions,
            requirements,
        }
    }

    /// Parses optional `match` / `exclude` expression strings.
    ///
    /// # Errors
    /// Returns the DSL parse error (`InvalidToken` / `MismatchedParentheses` /
    /// `InvalidFilterExpression` / `UnexpectedEndOfToken`) of the first expression that fails.
    pub fn parse(
        match_expression: Option<&str>,
        exclude_expression: Option<&str>,
    ) -> crate::Result<Self> {
        Ok(Self::new(
            match_expression.map(parse_filter_expression).transpose()?,
            exclude_expression
                .map(parse_filter_expression)
                .transpose()?,
        ))
    }

    /// Whether neither expression is present.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.match_expressions.is_none() && self.exclude_expressions.is_none()
    }
}

/// `file_type` value the DSL sees for a neutral entry kind.
const fn file_type(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::File => "file",
        EntryKind::Directory => "dir",
        EntryKind::Symlink => "symlink",
        EntryKind::Special(_) => "special",
    }
}

/// Extension exactly as `Path::extension` reports it, which is what the NFS, Local, and S3
/// walkers feed the DSL: text after the last `.`, but only when something precedes that dot, so
/// a dotfile has no extension. `""` when there is none; directories are included.
///
/// Never `None`: an absent `extension` makes an `extension` condition return `LazyMatch`, which
/// is a permissive keep rather than "unknown".
fn extension(name: &str) -> &str {
    match name.rsplit_once('.') {
        Some((stem, extension)) if !stem.is_empty() => extension,
        _ => "",
    }
}

impl TraversalFilter for DslTraversalFilter {
    fn required_metadata(&self) -> MetadataRequirements {
        self.requirements
    }

    fn resume_key(&self) -> Option<Vec<u8>> {
        Some(self.resume_key.to_vec())
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        if self.is_empty() {
            return TraversalDecision::unfiltered(candidate.kind);
        }
        let is_directory = candidate.kind == EntryKind::Directory;
        let (skip, continue_scan, check_children) = should_skip(
            self.match_expressions.as_ref(),
            self.exclude_expressions.as_ref(),
            FilterInput {
                file_name: Some(candidate.name),
                file_path: Some(candidate.path),
                file_type: Some(file_type(candidate.kind)),
                modified_epoch: candidate
                    .modified
                    .map(|value| crate::time_util::unix_nanos_to_secs(value.unix_nanos())),
                // Directories carried `end_of_file = 0` on every legacy backend; keep that so a
                // `size` condition never turns into a permissive `LazyMatch` for them.
                size: if is_directory {
                    Some(candidate.size.unwrap_or(0))
                } else {
                    candidate.size
                },
                extension: Some(extension(candidate.name)),
            },
        );
        TraversalDecision {
            emit: !skip,
            descend: continue_scan,
            filter_children: check_children,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        MetadataObservations, SpecialFileKind, StoragePath, StorageTimestamp, TimePrecision,
    };

    fn decide(
        filter: &DslTraversalFilter,
        path: &str,
        kind: EntryKind,
        size: Option<u64>,
        modified_secs: Option<i64>,
    ) -> (bool, bool, bool) {
        let path = StoragePath::new(path).unwrap_or_else(|error| panic!("{error}"));
        let name = path.as_str().rsplit('/').next().unwrap_or_default();
        let modified = modified_secs.map(|secs| {
            StorageTimestamp::new(i128::from(secs) * 1_000_000_000, TimePrecision::Seconds)
                .unwrap_or_else(|error| panic!("{error}"))
        });
        let decision = filter.decide(&TraversalCandidate {
            path: path.as_str(),
            name,
            kind,
            size,
            modified,
            metadata: &MetadataObservations::default(),
        });
        (decision.emit, decision.descend, decision.filter_children)
    }

    fn filter(
        match_expression: Option<&str>,
        exclude_expression: Option<&str>,
    ) -> DslTraversalFilter {
        DslTraversalFilter::parse(match_expression, exclude_expression)
            .unwrap_or_else(|error| panic!("{error}"))
    }

    #[test]
    fn no_expressions_admit_everything_without_evaluation() {
        let filter = filter(None, None);
        assert!(filter.is_empty());
        assert!(
            !filter
                .required_metadata()
                .contains(MetadataFamily::Timestamps)
        );
        assert_eq!(
            decide(&filter, "a", EntryKind::File, Some(1), None),
            (true, false, false)
        );
        assert_eq!(
            decide(&filter, "d", EntryKind::Directory, None, None),
            (true, true, false)
        );
    }

    #[test]
    fn exclude_path_prunes_subtree() {
        let filter = filter(None, Some("path == \"tmp/**\""));
        assert_eq!(
            decide(&filter, "tmp/x", EntryKind::Directory, None, None),
            (false, false, false)
        );
    }

    #[test]
    fn exclude_name_hides_directory_but_descends() {
        let filter = filter(None, Some("name == \"*.tmp\""));
        assert_eq!(
            decide(&filter, "a/x.tmp", EntryKind::Directory, None, None),
            (false, true, true)
        );
        assert_eq!(
            decide(&filter, "a/x.tmp", EntryKind::File, Some(1), None),
            (false, false, true)
        );
    }

    #[test]
    fn include_path_match_disables_child_filtering() {
        let filter = filter(Some("path == \"src/**\""), None);
        assert_eq!(
            decide(&filter, "src", EntryKind::Directory, None, None),
            (false, true, true),
            "partial match: hidden but descended"
        );
        assert_eq!(
            decide(&filter, "src/lib", EntryKind::Directory, None, None),
            (true, true, false),
            "full path match: children admitted unfiltered"
        );
        assert_eq!(
            decide(&filter, "x/y", EntryKind::File, Some(1), None),
            (false, false, false)
        );
    }

    #[test]
    fn extension_matches_path_extension_semantics() {
        for name in [
            "a.tar.gz",
            ".bashrc",
            "README",
            "v1.2",
            "trailing.",
            "a.b.c",
        ] {
            let expected = std::path::Path::new(name)
                .extension()
                .and_then(std::ffi::OsStr::to_str)
                .unwrap_or_default();
            assert_eq!(extension(name), expected, "extension of {name}");
        }
        // Pinned explicitly: a dotfile has no extension, matching the NFS/Local/S3 walkers.
        assert_eq!(extension(".bashrc"), "");
        assert_eq!(extension("a.tar.gz"), "gz");
    }

    #[test]
    fn file_type_mapping_covers_every_kind() {
        assert_eq!(file_type(EntryKind::File), "file");
        assert_eq!(file_type(EntryKind::Directory), "dir");
        assert_eq!(file_type(EntryKind::Symlink), "symlink");
        assert_eq!(
            file_type(EntryKind::Special(SpecialFileKind::Fifo)),
            "special"
        );
        let filter = filter(Some("type == \"file\""), None);
        assert_eq!(
            decide(
                &filter,
                "fifo",
                EntryKind::Special(SpecialFileKind::Fifo),
                Some(0),
                None
            ),
            (false, false, true)
        );
    }

    #[test]
    fn needs_modified_only_when_referenced() {
        assert!(
            !filter(Some("name == \"*.log\""), None)
                .required_metadata()
                .contains(MetadataFamily::Timestamps)
        );
        assert!(
            filter(Some("modified < 7d"), None)
                .required_metadata()
                .contains(MetadataFamily::Timestamps)
        );
        assert!(
            filter(None, Some("size > 1 and modified > \"2024-01-01\""))
                .required_metadata()
                .contains(MetadataFamily::Timestamps)
        );
    }

    #[test]
    fn modified_decisions_use_the_supplied_timestamp() {
        let filter = filter(Some("modified < 7d"), None);
        let now = crate::time_util::now_secs();
        assert_eq!(
            decide(
                &filter,
                "old.log",
                EntryKind::File,
                Some(1),
                Some(now - 30 * 86_400)
            ),
            (false, false, true)
        );
        assert_eq!(
            decide(&filter, "new.log", EntryKind::File, Some(1), Some(now)),
            (true, false, true)
        );
    }

    #[test]
    fn directory_size_defaults_to_zero() {
        let filter = filter(Some("size > 100"), None);
        assert_eq!(
            decide(&filter, "d", EntryKind::Directory, None, None),
            (false, true, true)
        );
    }

    /// The resume key: the same expressions parsed twice give the same key, any other expression
    /// (or the same one as the other kind) another; a relative time stays relative, so parsing it
    /// later gives the same key.
    #[test]
    fn the_resume_key_is_the_parsed_expressions() {
        let key = |filter: &DslTraversalFilter| filter.resume_key().unwrap_or_default();
        let logs = key(&filter(Some("name == \"*.log\""), None));
        assert_eq!(logs, key(&filter(Some("name == \"*.log\""), None)));
        assert_ne!(logs, key(&filter(Some("name == \"*.txt\""), None)));
        assert_ne!(logs, key(&filter(None, Some("name == \"*.log\""))));
        assert_ne!(logs, key(&filter(None, None)));
        let recent = key(&filter(Some("modified < 7d"), None));
        assert_eq!(recent, key(&filter(Some("modified < 7d"), None)));
        assert_ne!(recent, key(&filter(Some("modified < 8d"), None)));
    }
}
