//! 固定时钟与有序策略指纹分别用于本轮评估和跨轮选择比较。
use super::DslTraversalFilter;
use crate::AssertTestValue;
use crate::filter::{FilterInput, parse_filter_expression, should_skip_at};
use crate::model::{EntryKind, MetadataObservations, StorageTimestamp, TimePrecision};
use crate::traversal::{TraversalCandidate, TraversalFilter};

fn emits(filter: &DslTraversalFilter, seconds: i64) -> bool {
    filter
        .decide(&TraversalCandidate {
            name: "file",
            path: "file",
            kind: EntryKind::File,
            size: Some(1),
            modified: Some(
                StorageTimestamp::new(i128::from(seconds) * 1_000_000_000, TimePrecision::Seconds)
                    .assert_value("timestamp"),
            ),
            metadata: &MetadataObservations::default(),
        })
        .emit
}

#[test]
fn an_anchor_controls_both_include_and_exclude_without_changing_the_selection_key() {
    for (include, exclude, early, late) in [
        (Some("modified < 7d"), None, true, false),
        (None, Some("modified < 7d"), false, true),
    ] {
        let first = DslTraversalFilter::parse(include, exclude)
            .assert_value("filter")
            .with_time_anchor(16 * 86_400);
        let next = DslTraversalFilter::parse(include, exclude)
            .assert_value("filter")
            .with_time_anchor(18 * 86_400);
        assert_eq!(emits(&first, 10 * 86_400), early);
        assert_eq!(emits(&first, 10 * 86_400), early);
        assert_eq!(emits(&next, 10 * 86_400), late);
        assert_eq!(first.selection_key(), next.selection_key());
        assert_ne!(first.resume_key(), next.resume_key());
    }
}

#[test]
fn selection_keys_normalize_spacing_but_preserve_traversal_expression_order() {
    let parse = |value| DslTraversalFilter::parse(Some(value), None).assert_value("filter");
    let first = parse("path == k and name == '*.log'");
    let same = parse("path   ==   k    and    name == '*.log'");
    let reordered = parse("name == '*.log' and path == k");
    assert_eq!(first.selection_key(), same.selection_key());
    assert_ne!(first.selection_key(), reordered.selection_key());
}

#[test]
fn extreme_explicit_clocks_do_not_overflow_relative_time_comparison() {
    let expression = parse_filter_expression("modified > 1d").assert_value("filter");
    let input = |modified_epoch| FilterInput {
        modified_epoch: Some(modified_epoch),
        file_type: Some("file"),
        ..FilterInput::default()
    };
    assert!(!should_skip_at(Some(&expression), None, input(i64::MIN), i64::MAX).0);
    assert!(should_skip_at(Some(&expression), None, input(i64::MAX), i64::MIN).0);
}
