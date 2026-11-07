//! 给定同一时刻的包含、排除与下钻决策；不在子表达式中重复读取时钟。
use tracing::trace;

use super::{
    FilterExpression, FilterInput, evaluate_filter, excluded_entry_decision,
    included_entry_decision, pruning,
};

pub(super) fn trace_input(
    match_expressions: Option<&FilterExpression>,
    exclude_expressions: Option<&FilterExpression>,
    input: FilterInput<'_>,
) {
    trace!(
        "[filter::should_skip:FILTER-ENTRY] 开始检查: 匹配表达式= {:?}, 排除表达式={:?}, name={:?}, path={:?}, type={:?}, modified_epoch={:?}, size={:?}, extension={:?}",
        match_expressions,
        exclude_expressions,
        input.file_name,
        input.file_path,
        input.file_type,
        input.modified_epoch,
        input.size,
        input.extension
    );
}

pub(super) fn decide(
    match_expressions: Option<&FilterExpression>,
    exclude_expressions: Option<&FilterExpression>,
    input: FilterInput<'_>,
    now_epoch: i64,
) -> (bool, bool, bool) {
    let is_dir = input.file_type == Some("dir");
    if let Some(decision) = exclude(exclude_expressions, &input, now_epoch, is_dir) {
        return decision;
    }
    if let Some(decision) = include(
        match_expressions,
        exclude_expressions,
        &input,
        now_epoch,
        is_dir,
    ) {
        return decision;
    }
    trace!(
        "[filter::should_skip:FILTER-DEFAULT] 无匹配表达式，默认处理: name={:?}, path={:?}, is_dir={}, 保留条目，继续递归",
        input.file_name, input.file_path, is_dir
    );
    (false, is_dir, true)
}

fn exclude(
    expression: Option<&FilterExpression>,
    input: &FilterInput<'_>,
    now_epoch: i64,
    is_dir: bool,
) -> Option<(bool, bool, bool)> {
    let expr = expression?;
    let match_result = evaluate_filter(expr, input, now_epoch);
    trace!(
        "[filter::should_skip:FILTER-EXCLUDE] 黑名单匹配结果: expr={:?}, result={:?}",
        expr, match_result
    );
    excluded_entry_decision(&match_result, is_dir)
}

fn include(
    expression: Option<&FilterExpression>,
    exclude: Option<&FilterExpression>,
    input: &FilterInput<'_>,
    now_epoch: i64,
    is_dir: bool,
) -> Option<(bool, bool, bool)> {
    let expr = expression?;
    let match_result = evaluate_filter(expr, input, now_epoch);
    trace!(
        "[filter::should_skip:FILTER-MATCH] 白名单匹配结果: expr={:?}, result={:?}, is_dir={}",
        expr, match_result, is_dir
    );
    let (skip_entry, continue_scan, check_children) =
        included_entry_decision(&match_result, is_dir)?;
    let check_children = check_children
        || (is_dir
            && !skip_entry
            && (exclude.is_some() || !pruning::allows_unfiltered_descendants(&expr.root)));
    Some((skip_entry, continue_scan, check_children))
}
