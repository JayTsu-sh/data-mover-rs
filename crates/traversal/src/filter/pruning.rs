use super::{CompareOp, FilterASTNode, FilterCondition};

/// Only positive path matches are inherited by every descendant of a matched directory.
/// A negative path match can become false below it when `**` precedes another component.
pub(super) fn allows_unfiltered_descendants(node: &FilterASTNode) -> bool {
    match node {
        FilterASTNode::Condition(FilterCondition::Path {
            operator: CompareOp::Eq,
            ..
        }) => true,
        FilterASTNode::And(left, right) | FilterASTNode::Or(left, right) => {
            allows_unfiltered_descendants(left) && allows_unfiltered_descendants(right)
        }
        FilterASTNode::Condition(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::AssertTestValue;
    use crate::filter::{FilterInput, parse_filter_expression, should_skip};
    use crate::time_util::now_secs;

    fn decision(
        include: &str,
        exclude: Option<&str>,
        path: &str,
        kind: &str,
        modified_epoch: Option<i64>,
    ) -> (bool, bool, bool) {
        let include = parse_filter_expression(include).assert_value("include");
        let exclude = exclude.map(|text| parse_filter_expression(text).assert_value("exclude"));
        should_skip(
            Some(&include),
            exclude.as_ref(),
            FilterInput {
                file_name: path.rsplit('/').next(),
                file_path: Some(path),
                file_type: Some(kind),
                modified_epoch,
                size: Some(0),
                extension: Some(""),
            },
        )
    }

    #[test]
    fn lisa3849_exclude_remains_active_below_a_matched_path() {
        let include = "path == k";
        let exclude = Some("name == 123.txt");
        assert_eq!(
            decision(include, exclude, "k", "dir", None),
            (false, true, true)
        );
        assert!(decision(include, exclude, "k/123.txt", "file", None).0);
        assert!(!decision(include, exclude, "k/top.txt", "file", None).0);
    }

    #[test]
    fn lisa3849_missing_directory_time_does_not_disable_child_filtering() {
        let include = "path == k and modified > 7d";
        assert_eq!(
            decision(include, None, "k", "dir", None),
            (false, true, true)
        );
        let now = now_secs();
        assert!(!decision(include, None, "k/old.txt", "file", Some(now - 30 * 86_400)).0);
        assert!(decision(include, None, "k/new.txt", "file", Some(now - 86_400)).0);
    }

    #[test]
    fn mixed_or_keeps_child_filtering_and_path_ancestor_matching() {
        let include = "path == k or name == '*.pdf'";
        assert_eq!(
            decision(include, None, "k", "dir", None),
            (false, true, true)
        );
        assert!(!decision(include, None, "k/plain.txt", "file", None).0);
        assert!(!decision(include, None, "outside.pdf", "file", None).0);
        assert!(decision(include, None, "outside.txt", "file", None).0);
    }

    #[test]
    fn positive_path_combinations_preserve_subtree_admission() {
        for include in [
            "path == k",
            "path == k and path == '**'",
            "path == k or path == elsewhere",
            "(path == k and path == '**') or path == elsewhere",
        ] {
            assert_eq!(
                decision(include, None, "k", "dir", None),
                (false, true, false),
                "{include}"
            );
        }
    }

    #[test]
    fn positive_paths_inherit_ancestors_when_globstars_match_zero_segments() {
        for include in ["path == 'k/**/a'", "path == 'k/**/**/a'", "path == '**/a'"] {
            let exclude = Some("name == blocked");
            assert_eq!(
                decision(include, exclude, "k/a", "dir", None),
                (false, true, true),
                "{include}"
            );
            assert!(
                !decision(include, exclude, "k/a/allowed", "file", None).0,
                "{include}"
            );
            assert!(
                decision(include, exclude, "k/a/blocked", "file", None).0,
                "{include}"
            );
        }
    }

    #[test]
    fn negative_path_with_a_wildcard_must_recheck_descendants() {
        let include = "path != 'k/**/blocked'";
        assert_eq!(
            decision(include, None, "k/a", "dir", None),
            (false, true, true)
        );
        assert!(decision(include, None, "k/a/blocked", "file", None).0);
        assert!(!decision(include, None, "k/a/allowed", "file", None).0);
    }

    #[test]
    fn positive_and_negative_path_combinations_keep_child_filtering() {
        for include in [
            "path == k and path != 'k/**/blocked'",
            "path == k or path != 'k/**/blocked'",
            "path != 'k/**/blocked' or path == elsewhere",
        ] {
            assert_eq!(
                decision(include, None, "k/a", "dir", None),
                (false, true, true),
                "{include}"
            );
        }
    }

    #[test]
    fn subtree_admission_gate_preserves_file_and_pruned_directory_decisions() {
        assert_eq!(
            decision("path == k", Some("name == blocked"), "k/file", "file", None),
            (false, false, false)
        );
        assert_eq!(
            decision("path == k", Some("name == blocked"), "other", "dir", None),
            (true, false, false)
        );
        assert_eq!(
            decision("path == k", Some("path == k"), "k", "dir", None),
            (true, false, false)
        );
    }
}
