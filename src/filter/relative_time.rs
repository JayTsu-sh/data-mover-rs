//! Detect filters whose relative modified-time window can move between scans.

use super::{FilterASTNode, FilterCondition, FilterExpression, ModifiedValue};

impl FilterExpression {
    /// Whether this expression contains a relative-time condition.
    ///
    /// A relative `modified` condition uses the time of each evaluation. Absolute
    /// `modified` dates and dates extracted from directory names do not. This
    /// conservatively checks every branch, including conditions that may be
    /// short-circuited for a particular entry. A `true` result does not guarantee
    /// that a particular entry's match result will change.
    #[must_use]
    pub fn has_relative_time_condition(&self) -> bool {
        let mut pending = vec![&self.root];
        while let Some(node) = pending.pop() {
            match node {
                FilterASTNode::Condition(FilterCondition::Modified {
                    value: ModifiedValue::RelativeDays(_),
                    ..
                }) => return true,
                FilterASTNode::Condition(_) => {}
                FilterASTNode::And(left, right) | FilterASTNode::Or(left, right) => {
                    pending.push(left.as_ref());
                    pending.push(right.as_ref());
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::AssertTestValue;
    use crate::filter::{
        FilterInput, MatchAddon, MatchResult, MisMatchAddon, evaluate_filter,
        parse_filter_expression,
    };

    #[test]
    fn has_relative_time_condition_distinguishes_relative_and_absolute_conditions() {
        let parse = |value: &str| parse_filter_expression(value).assert_value("parse");

        for expression in [
            "modified < 7d",
            "modified > 30",
            "modified <= 0.5",
            "name == '*.log' and modified > 7d",
            "name == '*.log' or modified > 7d",
            "(modified < 7d or size > 100) and type == file",
        ] {
            assert!(
                parse(expression).has_relative_time_condition(),
                "{expression}"
            );
        }

        for expression in [
            "name == '*.log'",
            "size > 100 or path == 'archive/**'",
            "dir_date <= 20240301",
            "modified > '2024-01-01'",
            "modified < 20240101 or dir_date > 240301",
        ] {
            assert!(
                !parse(expression).has_relative_time_condition(),
                "{expression}"
            );
        }

        let absolute = parse("modified > '2024-01-01'");
        assert!(absolute.referenced_fields().modified);
        assert!(!absolute.has_relative_time_condition());
    }

    #[test]
    fn has_relative_time_condition_tracks_result_for_unchanged_input() {
        let expression = parse_filter_expression("modified < 7d").assert_value("parse");
        let file_time = 10 * 86_400;
        let input = FilterInput {
            modified_epoch: Some(file_time),
            ..FilterInput::default()
        };

        assert!(expression.has_relative_time_condition());
        assert_eq!(
            evaluate_filter(&expression, &input, file_time + 6 * 86_400),
            MatchResult::Match(MatchAddon::NonPath)
        );
        assert_eq!(
            evaluate_filter(&expression, &input, file_time + 8 * 86_400),
            MatchResult::MisMatch(MisMatchAddon::Other)
        );
    }
}
