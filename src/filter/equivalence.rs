//! Boolean equivalence only. The AST remains the executable traversal policy.

use std::collections::HashMap;

use super::{
    CompareOp, FilterASTNode, FilterCondition, FilterExpression, ModifiedValue,
    parse_filter_expression,
};
use crate::{Result, error::StorageError};

type NodeId = usize;
const FALSE: NodeId = 0;
const TRUE: NodeId = 1;

/// Borrow only semantic condition data, not compiled glob/path caches or Debug output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ConditionKey<'a> {
    Name(CompareOp, &'a str),
    Path(CompareOp, &'a str),
    Type(CompareOp, &'a str),
    ModifiedRelative(CompareOp, u64),
    ModifiedAbsolute(CompareOp, i64),
    Size(CompareOp, u64),
    Extension(CompareOp, &'a str),
    DirDate(CompareOp, i64),
}

impl<'a> From<&'a FilterCondition> for ConditionKey<'a> {
    fn from(condition: &'a FilterCondition) -> Self {
        match condition {
            FilterCondition::Name { operator, pattern } => Self::Name(*operator, pattern.as_str()),
            FilterCondition::Path {
                operator,
                raw_value,
                ..
            } => Self::Path(*operator, raw_value),
            FilterCondition::Type { operator, value } => Self::Type(*operator, value),
            FilterCondition::Modified { operator, value } => match value {
                ModifiedValue::RelativeDays(days) => {
                    // Numeric equality treats positive and negative zero alike.
                    let bits = if *days == 0.0 { 0 } else { days.to_bits() };
                    Self::ModifiedRelative(*operator, bits)
                }
                ModifiedValue::AbsoluteEpoch(epoch) => Self::ModifiedAbsolute(*operator, *epoch),
            },
            FilterCondition::Size { operator, value } => Self::Size(*operator, *value),
            FilterCondition::Extension { operator, pattern } => {
                Self::Extension(*operator, pattern.as_str())
            }
            FilterCondition::DirDate { operator, epoch } => Self::DirDate(*operator, *epoch),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Op {
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Node {
    variable: usize,
    low: NodeId,
    high: NodeId,
}

impl Node {
    // Terminals sort after every variable. Their edges are never followed.
    const TERMINAL: Self = Self {
        variable: usize::MAX,
        low: FALSE,
        high: FALSE,
    };
}

#[derive(Clone, Copy)]
struct Limits {
    variables: usize,
    nodes: usize,
    steps: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            // Also bounds the recursion depth of apply(). AST compilation is iterative.
            variables: 256,
            nodes: 65_536,
            steps: 1_000_000,
        }
    }
}

enum CompileTask<'a> {
    Visit(&'a FilterASTNode),
    AfterLeft(Op, &'a FilterASTNode),
    AfterRight(Op, NodeId),
}

struct Bdd<'a> {
    conditions: HashMap<ConditionKey<'a>, NodeId>,
    nodes: Vec<Node>,
    unique: HashMap<Node, NodeId>,
    applications: HashMap<(Op, NodeId, NodeId), NodeId>,
    limits: Limits,
    steps: usize,
}

impl<'a> Bdd<'a> {
    fn new(limits: Limits) -> Self {
        Self {
            conditions: HashMap::new(),
            nodes: vec![Node::TERMINAL; 2],
            unique: HashMap::new(),
            applications: HashMap::new(),
            limits,
            steps: 0,
        }
    }

    fn limit_error(resource: &str, limit: usize) -> StorageError {
        StorageError::OperationError(format!(
            "Filter logical equivalence exceeded {resource} limit ({limit})"
        ))
    }

    fn step(&mut self) -> Result<()> {
        if self.steps >= self.limits.steps {
            return Err(Self::limit_error("step", self.limits.steps));
        }
        self.steps += 1;
        Ok(())
    }

    fn make_node(&mut self, variable: usize, low: NodeId, high: NodeId) -> Result<NodeId> {
        if low == high {
            return Ok(low);
        }
        let node = Node {
            variable,
            low,
            high,
        };
        if let Some(&id) = self.unique.get(&node) {
            return Ok(id);
        }
        if self.nodes.len() - 2 >= self.limits.nodes {
            return Err(Self::limit_error("decision node", self.limits.nodes));
        }
        let id = self.nodes.len();
        self.nodes.push(node);
        self.unique.insert(node, id);
        Ok(id)
    }

    fn condition(&mut self, condition: &'a FilterCondition) -> Result<NodeId> {
        let key = ConditionKey::from(condition);
        if let Some(&id) = self.conditions.get(&key) {
            return Ok(id);
        }
        let variable = self.conditions.len();
        if variable >= self.limits.variables {
            return Err(Self::limit_error(
                "distinct condition",
                self.limits.variables,
            ));
        }
        let id = self.make_node(variable, FALSE, TRUE)?;
        self.conditions.insert(key, id);
        Ok(id)
    }

    fn compile(&mut self, root: &'a FilterASTNode) -> Result<NodeId> {
        let mut pending = vec![CompileTask::Visit(root)];
        let mut result = FALSE;
        while let Some(task) = pending.pop() {
            self.step()?;
            match task {
                CompileTask::Visit(FilterASTNode::Condition(condition)) => {
                    result = self.condition(condition)?;
                }
                CompileTask::Visit(FilterASTNode::And(left, right)) => {
                    pending.push(CompileTask::AfterLeft(Op::And, right));
                    pending.push(CompileTask::Visit(left));
                }
                CompileTask::Visit(FilterASTNode::Or(left, right)) => {
                    pending.push(CompileTask::AfterLeft(Op::Or, right));
                    pending.push(CompileTask::Visit(left));
                }
                CompileTask::AfterLeft(op, right) => {
                    pending.push(CompileTask::AfterRight(op, result));
                    pending.push(CompileTask::Visit(right));
                }
                CompileTask::AfterRight(op, left) => result = self.apply(op, left, result)?,
            }
        }
        Ok(result)
    }

    fn apply(&mut self, op: Op, left: NodeId, right: NodeId) -> Result<NodeId> {
        self.step()?;
        // Both operations commute; normalize cache keys even when operand IDs are reversed.
        let (left, right) = if left <= right {
            (left, right)
        } else {
            (right, left)
        };
        if left == right {
            return Ok(left);
        }
        match op {
            Op::And if left == FALSE => return Ok(FALSE),
            Op::And if left == TRUE => return Ok(right),
            Op::Or if left == TRUE || right == TRUE => return Ok(TRUE),
            Op::Or if left == FALSE => return Ok(right),
            _ => {}
        }
        let key = (op, left, right);
        if let Some(&id) = self.applications.get(&key) {
            return Ok(id);
        }

        let left_node = self.nodes[left];
        let right_node = self.nodes[right];
        let variable = left_node.variable.min(right_node.variable);
        let (left_low, left_high) = if left_node.variable == variable {
            (left_node.low, left_node.high)
        } else {
            (left, left)
        };
        let (right_low, right_high) = if right_node.variable == variable {
            (right_node.low, right_node.high)
        } else {
            (right, right)
        };
        let low = self.apply(op, left_low, right_low)?;
        let high = self.apply(op, left_high, right_high)?;
        let result = self.make_node(variable, low, high)?;
        self.applications.insert(key, result);
        Ok(result)
    }
}

impl FilterExpression {
    /// Compares the Boolean structure of two expressions using a temporary ROBDD.
    ///
    /// Supports commutativity, associativity, distributivity, idempotence and absorption.
    /// Each parsed condition (field, operator and value) is an independent Boolean variable;
    /// this does not infer implications or complements between different conditions.
    ///
    /// This is not equivalence of traversal decisions: evaluation still uses the AST, whose
    /// short-circuiting, partial matches and missing metadata can affect those decisions.
    /// Do not use this comparison to establish checkpoint compatibility.
    ///
    /// # Errors
    /// Returns [`StorageError::OperationError`] if comparison exceeds 256 distinct conditions,
    /// 65,536 decision nodes or 1,000,000 construction/application steps.
    pub fn logically_equivalent(&self, other: &Self) -> Result<bool> {
        // First encounter assigns variable order. One manager makes that order identical for
        // both expressions; roots from separate managers would not be comparable by ID.
        let mut bdd = Bdd::new(Limits::default());
        let left = bdd.compile(&self.root)?;
        let right = bdd.compile(&other.root)?;
        Ok(left == right)
    }

    /// Parses another expression and compares its Boolean structure with this expression.
    /// See [`Self::logically_equivalent`] for comparison semantics and resource limits.
    ///
    /// # Errors
    /// Returns the expression's parse error, or an error if ROBDD construction exceeds a limit.
    pub fn logically_equivalent_str(&self, other: &str) -> Result<bool> {
        let other = parse_filter_expression(other)?;
        self.logically_equivalent(&other)
    }
}

#[cfg(test)]
mod tests {
    use std::mem::discriminant;

    use super::*;
    use crate::filter::{FilterInput, parse_filter_expression, should_skip};
    use crate::{AssertTestError, AssertTestValue};

    fn assert_equivalence(left: &str, right: &str, expected: bool) {
        let left_expr = parse_filter_expression(left).assert_value("parse left");
        let right_expr = parse_filter_expression(right).assert_value("parse right");
        for result in [
            left_expr.logically_equivalent(&right_expr),
            right_expr.logically_equivalent(&left_expr),
            left_expr.logically_equivalent_str(right),
            right_expr.logically_equivalent_str(left),
        ] {
            assert_eq!(
                result.assert_value("compare expressions"),
                expected,
                "{left} versus {right}"
            );
        }
    }

    #[test]
    fn commutativity_associativity_and_idempotence_hold() {
        let a = "name == \"*.rs\"";
        let b = "size > 100";
        let c = "type == file";
        let cases = [
            (format!("{a} and {b}"), format!("{b} and {a}")),
            (format!("{a} or {b}"), format!("{b} or {a}")),
            (
                format!("({a} and {b}) and {c}"),
                format!("{a} and ({b} and {c})"),
            ),
            (
                format!("({a} or {b}) or {c}"),
                format!("{a} or ({b} or {c})"),
            ),
            (format!("{a} and {a}"), a.to_string()),
            (format!("{a} or {a}"), a.to_string()),
        ];
        for (left, right) in cases {
            assert_equivalence(&left, &right, true);
        }
    }

    #[test]
    fn both_distributive_laws_hold() {
        let a = "name == \"*.rs\"";
        let b = "size > 100";
        let c = "type == file";
        let d = "extension != tmp";
        for (left, right) in [
            (
                format!("{a} and ({b} or {c})"),
                format!("({a} and {b}) or ({a} and {c})"),
            ),
            (
                format!("{a} or ({b} and {c})"),
                format!("({a} or {b}) and ({a} or {c})"),
            ),
            (
                format!("({a} or {b}) and ({c} or {d})"),
                format!("({d} and {b}) or ({a} and {c}) or ({b} and {c}) or ({d} and {a})"),
            ),
        ] {
            assert_equivalence(&left, &right, true);
        }
    }

    #[test]
    fn both_absorption_laws_hold_with_swapped_and_nested_operands() {
        let a = "name == \"*.rs\"";
        let b = "size > 100";
        let c = "type == file";
        for (left, right) in [
            (format!("{a} or ({a} and {b})"), a.to_string()),
            (format!("{a} and ({a} or {b})"), a.to_string()),
            (format!("({b} and {a}) or {a}"), a.to_string()),
            (format!("({b} or {a}) and {a}"), a.to_string()),
            (
                format!("({a} and {b}) or (({b} and {a}) and {c})"),
                format!("{b} and {a}"),
            ),
            (
                format!("({a} or {b}) and (({b} or {a}) or {c})"),
                format!("{b} or {a}"),
            ),
        ] {
            assert_equivalence(&left, &right, true);
        }
    }

    #[test]
    fn distinct_boolean_functions_do_not_compare_equal() {
        let a = "name == \"*.rs\"";
        let b = "size > 100";
        let c = "type == file";
        for (left, right) in [
            (format!("{a} and {b}"), format!("{a} or {b}")),
            (
                format!("({a} or {b}) and {c}"),
                format!("{a} or ({b} and {c})"),
            ),
            (format!("{a} or {b}"), a.to_string()),
            (format!("{a} and {b}"), a.to_string()),
            (
                format!("({a} or {b}) and ({a} or {c})"),
                format!("{a} and ({b} or {c})"),
            ),
        ] {
            assert_equivalence(&left, &right, false);
        }
    }

    #[test]
    fn all_condition_variants_use_parsed_values_and_preserve_field_and_operator() {
        for (left, equal, different) in [
            ("name == \"*.rs\"", "(name == *.rs)", "name != *.rs"),
            ("path == \"src/\"", "path == src", "path == src/**"),
            ("type == \"file\"", "type == file", "type == dir"),
            ("modified < 3d", "modified < 3.0", "modified <= 3d"),
            (
                "modified > \"2025-01-15\"",
                "modified > 20250115",
                "modified > \"2025-01-15T00:00:01\"",
            ),
            ("size > 0100", "size > 100", "size > 101"),
            ("extension == \"rs\"", "extension == rs", "extension == txt"),
            (
                "dir_date == \"2024-03-01\"",
                "dir_date == 240301",
                "dir_date == 240302",
            ),
        ] {
            assert_equivalence(left, equal, true);
            assert_equivalence(left, different, false);
        }
        assert_equivalence("modified < -0.0", "modified < 0d", true);
        assert_equivalence("modified < 0.5", "modified < 0.5000000000000001", false);
        assert_equivalence("name == rs", "extension == rs", false);
        assert_equivalence("path == rs", "name == rs", false);
        assert_equivalence("modified > 3d", "modified > 20250115", false);
        assert_equivalence("size > 100", "size < 100", false);
    }

    #[test]
    fn comparison_does_not_infer_relations_between_different_predicates() {
        assert_equivalence("size > 100 or size > 200", "size > 100", false);
        assert_equivalence(
            "(name == foo or name != foo) and type == file",
            "type == file",
            false,
        );
    }

    #[test]
    fn string_interface_preserves_parse_errors() {
        let expression = parse_filter_expression("type == file").assert_value("parse");
        for invalid in ["name == \"[\"", "(type == file", "size > invalid", ""] {
            let expected = parse_filter_expression(invalid).assert_error("invalid expression");
            let actual = expression
                .logically_equivalent_str(invalid)
                .assert_error("string interface must return the parse error");
            assert_eq!(actual.to_string(), expected.to_string());
            assert_eq!(discriminant(&actual), discriminant(&expected));
        }
    }

    #[test]
    fn boolean_equivalence_preserves_existing_order_sensitive_traversal() {
        let path_first =
            parse_filter_expression("path == src or name == src").assert_value("parse");
        let name_first =
            parse_filter_expression("name == src or path == src").assert_value("parse");
        let input = FilterInput {
            file_name: Some("src"),
            file_path: Some("src"),
            file_type: Some("dir"),
            ..FilterInput::default()
        };
        assert!(
            path_first
                .logically_equivalent(&name_first)
                .assert_value("compare")
        );
        assert_eq!(
            should_skip(Some(&path_first), None, input),
            (false, true, false)
        );
        assert_eq!(
            should_skip(Some(&name_first), None, input),
            (false, true, true)
        );
    }

    #[test]
    fn long_associative_chains_compile_without_recursive_ast_walks() {
        let expression = vec!["type == file"; 1_024].join(" and ");
        let expression = parse_filter_expression(&expression).assert_value("parse chain");
        assert!(
            expression
                .logically_equivalent_str("type == file")
                .assert_value("compare chain")
        );
    }

    #[test]
    fn resource_limits_return_errors_instead_of_false() {
        let expression =
            parse_filter_expression("name == foo and size > 100").assert_value("parse");
        for (limits, resource) in [
            (
                Limits {
                    variables: 1,
                    ..Limits::default()
                },
                "distinct condition",
            ),
            (
                Limits {
                    nodes: 2,
                    ..Limits::default()
                },
                "decision node",
            ),
            (
                Limits {
                    steps: 5,
                    ..Limits::default()
                },
                "step",
            ),
        ] {
            let error = Bdd::new(limits)
                .compile(&expression.root)
                .assert_error("must exceed limit");
            assert!(matches!(&error, StorageError::OperationError(_)));
            assert!(error.to_string().contains(resource), "{error}");
        }
    }

    #[test]
    fn public_interfaces_report_combined_condition_limit() {
        let many = (0..257)
            .map(|value| format!("size > {value}"))
            .collect::<Vec<_>>()
            .join(" or ");
        let many_expr = parse_filter_expression(&many).assert_value("parse many conditions");
        let one_expr = parse_filter_expression("name == foo").assert_value("parse one condition");
        for result in [
            one_expr.logically_equivalent(&many_expr),
            one_expr.logically_equivalent_str(&many),
        ] {
            let error = result.assert_error("combined variables must exceed limit");
            assert!(matches!(&error, StorageError::OperationError(_)));
            assert!(error.to_string().contains("distinct condition"), "{error}");
        }
    }

    fn evaluate_boolean(node: &FilterASTNode, assignment: u16) -> bool {
        match node {
            FilterASTNode::Condition(FilterCondition::Size { value, .. }) => {
                assignment & (1 << value) != 0
            }
            FilterASTNode::Condition(_) => panic!("only size atoms in this truth-table test"),
            FilterASTNode::And(left, right) => {
                evaluate_boolean(left, assignment) && evaluate_boolean(right, assignment)
            }
            FilterASTNode::Or(left, right) => {
                evaluate_boolean(left, assignment) || evaluate_boolean(right, assignment)
            }
        }
    }

    fn boolean_combinations(left: &FilterASTNode, right: &FilterASTNode) -> [FilterASTNode; 2] {
        [
            FilterASTNode::And(Box::new(left.clone()), Box::new(right.clone())),
            FilterASTNode::Or(Box::new(left.clone()), Box::new(right.clone())),
        ]
    }

    fn truth_table_expressions() -> Vec<FilterASTNode> {
        let atoms: Vec<_> = (0..4)
            .map(|value| {
                FilterASTNode::Condition(FilterCondition::Size {
                    operator: CompareOp::Eq,
                    value,
                })
            })
            .collect();
        let mut expressions = atoms.clone();
        for left in &atoms {
            for right in &atoms {
                expressions.extend(boolean_combinations(left, right));
            }
        }
        for left in expressions.clone() {
            for right in &atoms {
                expressions.extend(boolean_combinations(&left, right));
            }
        }
        expressions
    }

    #[test]
    fn canonical_roots_agree_with_an_independent_exhaustive_truth_table() {
        let expressions = truth_table_expressions();
        let mut bdd = Bdd::new(Limits::default());
        let mut by_truth_table = HashMap::new();
        let mut by_root = HashMap::new();
        for expression in &expressions {
            let truth_table = (0..16).fold(0u16, |table, assignment| {
                if evaluate_boolean(expression, assignment) {
                    table | (1 << assignment)
                } else {
                    table
                }
            });
            let root = bdd
                .compile(expression)
                .assert_value("compile truth-table expression");
            if let Some(previous) = by_truth_table.insert(truth_table, root) {
                assert_eq!(root, previous, "equal functions must share a root");
            }
            if let Some(previous) = by_root.insert(root, truth_table) {
                assert_eq!(
                    truth_table, previous,
                    "different functions must not share a root"
                );
            }
        }
    }
}
