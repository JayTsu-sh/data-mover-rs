// 标准库
use std::ops::{BitAnd, BitOr};
use std::time::SystemTime;

// 外部crate
use glob::{MatchOptions, Pattern};
use tracing::trace;

// 内部模块
use crate::{Result, error::StorageError};

/// glob 匹配选项：`require_literal_separator = true` 确保 `*` 不匹配路径分隔符 `/`，
/// 只有 `**` 才能跨越目录边界。
const GLOB_MATCH_OPTIONS: MatchOptions = MatchOptions {
    case_sensitive: true,
    require_literal_separator: true,
    require_literal_leading_dot: false,
};

/// 比较操作符枚举，替代原来的字符串 operator
#[derive(Debug, Clone, Copy, PartialEq)]
enum CompareOp {
    Eq, // "=="
    Ne, // "!="
    Lt, // "<"
    Gt, // ">"
    Le, // "<="
    Ge, // ">="
}

/// Modified 条件的值类型
#[derive(Debug, Clone)]
enum ModifiedValue {
    /// 相对天数（如 3d, 30, 0.5）
    RelativeDays(f64),
    /// 绝对时间点（Unix epoch seconds）
    AbsoluteEpoch(i64),
}

/// 检查文件/目录是否应该被跳过的核心过滤函数
///
/// # 参数说明
/// - `match_expressions`: 匹配表达式，用于白名单过滤
/// - `exclude_expressions`: 排除表达式，用于黑名单过滤
/// - `file_name`: 文件名（可选）
/// - `file_path`: 文件路径（可选）
/// - `file_type`: 文件类型（"file"或"dir"，可选）
/// - `modified_epoch`: 文件修改时间的 epoch seconds（可选）
/// - `size`: 文件大小（可选）
/// - `extension`: 文件扩展名（可选）
///
/// # 返回值
/// 返回一个三元组 (`should_skip`, `continue_scan`, `check_children`)，其中：
/// 1. `should_skip`: 当前条目是否应该被跳过，true表示跳过，不构建为StorageEntry
/// 2. `continue_scan`: 是否需要继续扫描子目录（仅对目录有效），true表示继续扫描
/// 3. `check_children`: 子目录或文件是否需要执行`should_skip`匹配，true表示需要进一步匹配
///
/// # 执行逻辑
/// 1. 优先检查排除表达式（黑名单），匹配则跳过
/// 2. 再检查匹配表达式（白名单），根据匹配结果决定是否跳过
/// 3. 最后处理无匹配表达式的情况
#[allow(clippy::too_many_arguments)]
pub fn should_skip(
    match_expressions: Option<&FilterExpression>, exclude_expressions: Option<&FilterExpression>,
    file_name: Option<&str>, file_path: Option<&str>, file_type: Option<&str>, modified_epoch: Option<i64>,
    size: Option<u64>, extension: Option<&str>,
) -> (bool, bool, bool) {
    trace!(
        "[filter::should_skip:FILTER-ENTRY] 开始检查: 匹配表达式= {:?}, 排除表达式={:?}, name={:?}, path={:?}, type={:?}, modified_epoch={:?}, size={:?}, extension={:?}",
        match_expressions, exclude_expressions, file_name, file_path, file_type, modified_epoch, size, extension
    );

    // 预计算 now_epoch，避免在 evaluate 中重复调用 SystemTime::now()
    let now_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let is_dir = file_type == Some("dir");

    // 排除条件检查：黑名单过滤
    if let Some(expr) = exclude_expressions {
        let match_result = evaluate_filter(
            expr,
            file_name,
            file_path,
            file_type,
            modified_epoch,
            size,
            extension,
            now_epoch,
        );

        trace!(
            "[filter::should_skip:FILTER-EXCLUDE] 黑名单匹配结果: expr={:?}, result={:?}",
            expr, match_result
        );

        match match_result {
            MatchResult::Match(MatchAddon::PathMatch | MatchAddon::MixMatch) => {
                // 带路径条件完整匹配黑名单，跳过当前条目，不继续扫描子目录
                trace!(
                    "[filter::should_skip:FILTER-EXCLUDE-MATCH] 带路径条件完整匹配黑名单，跳过当前条目，停止递归: name={:?}, path={:?}",
                    file_name, file_path
                );
                return (true, false, false);
            }
            MatchResult::Match(_) => {
                // 不带路径条件完整匹配黑名单，跳过当前条目，继续扫描子目录
                trace!(
                    "[filter::should_skip:FILTER-EXCLUDE-MATCH] 不带路径条件完整匹配黑名单，跳过当前条目，继续扫描子目录: name={:?}, path={:?}, is_dir={}",
                    file_name, file_path, is_dir
                );
                return (true, is_dir, true);
            }
            MatchResult::PartialMatch => {
                // 目录部分匹配黑名单(这意味着当前条目是目录)，跳过当前条目，但继续扫描子目录
                trace!(
                    "[filter::should_skip:FILTER-EXCLUDE-PARTIAL] 目录部分匹配黑名单，跳过当前条目，继续扫描子目录: name={:?}, path={:?}, is_dir={}",
                    file_name, file_path, is_dir
                );
                return (true, true, true);
            }
            _ => { /*流转到白名单匹配*/ }
        }
    }

    // 匹配条件检查：白名单过滤
    if let Some(expr) = match_expressions {
        let match_result = evaluate_filter(
            expr,
            file_name,
            file_path,
            file_type,
            modified_epoch,
            size,
            extension,
            now_epoch,
        );

        trace!(
            "[filter::should_skip:FILTER-MATCH] 白名单匹配结果: expr={:?}, result={:?}, is_dir={}",
            expr, match_result, is_dir
        );

        match match_result {
            MatchResult::Match(MatchAddon::PathMatch) => {
                // 完整匹配白名单，不跳过当前条目， 继续扫描子目录，又只有path匹配条件，子目录无需匹配
                trace!(
                    "[filter::should_skip:FILTER-MATCH-FULL] 完整匹配白名单，保留当前条目，若是目录则继续递归子项(无需检查): name={:?}, path={:?}, is_dir={}",
                    file_name, file_path, is_dir
                );
                return (false, is_dir, false);
            }
            MatchResult::Match(_) => {
                // 完整匹配白名单，不跳过当前条目， 继续扫描子目录，子目录需匹配检查
                trace!(
                    "[filter::should_skip:FILTER-MATCH-FULL] 完整匹配白名单，保留当前条目，若是目录则继续递归子项(需要检查): name={:?}, path={:?}",
                    file_name, file_path
                );
                return (false, is_dir, true);
            }
            MatchResult::PartialMatch => {
                // 目录部分匹配白名单，跳过当前条目，但继续扫描子目录
                trace!(
                    "[filter::should_skip:FILTER-MATCH-PARTIAL] 白名单目录部分匹配，跳过当前条目，若是目录则继续递归检查子项: name={:?}, path={:?}, match_result={:?}, is_dir={}",
                    file_name, file_path, match_result, is_dir
                );

                // Always continue scanning for partial matches if it's a directory
                return (true, is_dir, true);
            }
            MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch) => {
                // 完整路径不匹配白名单，跳过当前条目，也无需扫描子目录
                trace!(
                    "[filter::should_skip:FILTER-MATCH-PARTIAL] 白名单完整路径不匹配，跳过当前条目，也无需扫描子目录: name={:?}, path={:?}, match_result={:?}",
                    file_name, file_path, match_result
                );

                // Always continue scanning for partial matches if it's a directory
                return (true, false, false);
            }
            MatchResult::MisMatch(_) => {
                // 其它原因不匹配白名单，跳过当前条目，继续扫描子目录
                trace!(
                    "[filter::should_skip:FILTER-MATCH-PARTIAL] 白名单其它原因不匹配，跳过当前条目，继续扫描子目录: name={:?}, path={:?}, match_result={:?}, is_dir={}",
                    file_name, file_path, match_result, is_dir
                );

                // Always continue scanning for partial matches if it's a directory
                return (true, is_dir, true);
            }
            MatchResult::LazyMatch => { /* 过滤表达式没有白名单适用条件，继续后续流程 */ }
        }
    }

    // 无匹配表达式，默认不跳过，目录继续扫描
    trace!(
        "[filter::should_skip:FILTER-DEFAULT] 无匹配表达式，默认处理: name={:?}, path={:?}, is_dir={}, 保留条目，继续递归",
        file_name, file_path, is_dir
    );
    (false, is_dir, true)
}

/// 检查目录名是否匹配 `match_expressions` 中的 `DirDate` 条件
///
/// 仅在 packaged 模式下由 walkdir 调用。如果 `match_expressions` 为 None 或不包含
/// `DirDate` 条件，返回 false。
pub fn dir_matches_date_filter(match_expressions: Option<&FilterExpression>, dir_name: &str) -> bool {
    match match_expressions {
        Some(expr) => expr.has_matching_dir_date(dir_name),
        None => false,
    }
}

/// 解析过滤表达式的辅助函数
pub fn parse_filter_expression(expr: &str) -> Result<FilterExpression> {
    FilterExpression::parse(expr)
}

// ========== 过滤条件元数据（供 Web API 暴露给前端） ==========

/// 操作符定义
#[derive(Debug, Clone)]
pub struct FilterOperatorDef {
    /// 操作符符号，如 "==", ">", "<"
    pub value: &'static str,
    /// 操作符中文标签
    pub label: &'static str,
}

/// 过滤字段定义
#[derive(Debug, Clone)]
pub struct FilterFieldDef {
    /// 字段名（与表达式语法中的字段名一致）
    pub name: &'static str,
    /// 中文标签
    pub label: &'static str,
    /// 值类型：glob, bytes, `duration_or_date`, enum, date
    pub value_type: &'static str,
    /// 该字段支持的操作符列表
    pub operators: Vec<FilterOperatorDef>,
    /// 枚举类型字段的可选值
    pub enum_values: Option<Vec<&'static str>>,
}

const GLOB_OPS: &[(&str, &str)] = &[("==", "匹配"), ("!=", "不匹配")];
const CMP_OPS: &[(&str, &str)] = &[(">", "大于"), ("<", "小于"), (">=", "大于等于"), ("<=", "小于等于")];
const ALL_OPS: &[(&str, &str)] = &[
    ("==", "等于"),
    ("!=", "不等于"),
    (">", "大于"),
    ("<", "小于"),
    (">=", "大于等于"),
    ("<=", "小于等于"),
];
const EQ_NE_OPS: &[(&str, &str)] = &[("==", "等于"), ("!=", "不等于")];

fn ops_from(defs: &[(&'static str, &'static str)]) -> Vec<FilterOperatorDef> {
    defs.iter()
        .map(|&(value, label)| FilterOperatorDef { value, label })
        .collect()
}

/// 返回所有支持的过滤字段定义，与 `FilterCondition` 枚举保持同步。
pub fn get_filter_field_definitions() -> Vec<FilterFieldDef> {
    vec![
        FilterFieldDef {
            name: "name",
            label: "文件名称",
            value_type: "glob",
            operators: ops_from(GLOB_OPS),
            enum_values: None,
        },
        FilterFieldDef {
            name: "size",
            label: "文件大小",
            value_type: "bytes",
            operators: ops_from(CMP_OPS),
            enum_values: None,
        },
        FilterFieldDef {
            name: "modified",
            label: "修改时间",
            value_type: "duration_or_date",
            operators: ops_from(CMP_OPS),
            enum_values: None,
        },
        FilterFieldDef {
            name: "extension",
            label: "扩展名",
            value_type: "glob",
            operators: ops_from(GLOB_OPS),
            enum_values: None,
        },
        FilterFieldDef {
            name: "path",
            label: "路径",
            value_type: "glob",
            operators: ops_from(GLOB_OPS),
            enum_values: None,
        },
        FilterFieldDef {
            name: "type",
            label: "类型",
            value_type: "enum",
            operators: ops_from(EQ_NE_OPS),
            enum_values: Some(vec!["file", "dir", "symlink"]),
        },
        FilterFieldDef {
            name: "dir_date",
            label: "目录日期",
            value_type: "date",
            operators: ops_from(ALL_OPS),
            enum_values: None,
        },
    ]
}

/// 评估过滤表达式的辅助函数
#[allow(clippy::too_many_arguments)]
fn evaluate_filter(
    expr: &FilterExpression, file_name: Option<&str>, file_path: Option<&str>, file_type: Option<&str>,
    modified_epoch: Option<i64>, size: Option<u64>, extension: Option<&str>, now_epoch: i64,
) -> MatchResult {
    expr.evaluate(
        file_name,
        file_path,
        file_type,
        modified_epoch,
        size,
        extension,
        now_epoch,
    )
}

/// Individual filter condition
#[derive(Debug, Clone)]
enum FilterCondition {
    /// Name matching with precompiled glob pattern
    Name { operator: CompareOp, pattern: Pattern },

    /// Path matching with precompiled pattern and metadata
    Path {
        operator: CompareOp,
        raw_value: String,
        pattern: Pattern,
        pattern_parts: Vec<String>,
        pattern_depth: usize,
        has_double_wildcard: bool,
        pattern_after_wildcard: Vec<String>,
    },

    /// File type matching
    Type {
        operator: CompareOp,
        value: String, // "file", "dir", "symlink"
    },

    /// Modification time
    Modified { operator: CompareOp, value: ModifiedValue },

    /// File size (bytes)
    Size { operator: CompareOp, value: u64 },

    /// Extension matching with precompiled glob pattern
    Extension { operator: CompareOp, pattern: Pattern },

    /// Directory date matching: extracts date from directory name and compares
    DirDate { operator: CompareOp, epoch: i64 },
}

// Token定义
#[derive(Debug, Clone)]
enum Token {
    Condition(FilterCondition),
    And,
    Or,
    LParen,
    RParen,
}

pub struct Lexer<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Lexer { input, position: 0 }
    }

    fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();

        while self.position < self.input.len() {
            // 跳过空白字符
            if self.peek().is_whitespace() {
                self.consume();
                continue;
            }

            // 处理括号
            if self.peek() == '(' {
                tokens.push(Token::LParen);
                self.consume();
                continue;
            }

            if self.peek() == ')' {
                tokens.push(Token::RParen);
                self.consume();
                continue;
            }

            // 处理逻辑运算符
            if (self.starts_with("and ") || self.peek_rest().starts_with("and)") || self.peek_rest() == "and")
                && self.position + 3 <= self.input.len()
            {
                // 检查前面是否有条件或右括号
                if tokens
                    .last()
                    .is_some_and(|t| matches!(t, Token::Condition(_) | Token::RParen))
                {
                    tokens.push(Token::And);
                    self.consume_n(3);
                    continue;
                }
            }

            if (self.starts_with("or ") || self.peek_rest().starts_with("or)") || self.peek_rest() == "or")
                && self.position + 2 <= self.input.len()
            {
                // 检查前面是否有条件或右括号
                if tokens
                    .last()
                    .is_some_and(|t| matches!(t, Token::Condition(_) | Token::RParen))
                {
                    tokens.push(Token::Or);
                    self.consume_n(2);
                    continue;
                }
            }

            // 处理条件表达式
            let start_pos = self.position;
            let condition = self.read_condition();
            if let Some(cond) = Self::parse_condition(&condition)? {
                tokens.push(Token::Condition(cond));
            } else if start_pos == self.position {
                // 如果位置没有移动，说明无法解析，跳过当前字符
                self.consume();
            }
        }

        Ok(tokens)
    }

    fn read_condition(&mut self) -> String {
        let start = self.position;
        let mut paren_count = 0;
        let mut in_quotes = false;

        while self.position < self.input.len() {
            let ch = self.peek();

            // 处理引号
            if ch == '"' {
                in_quotes = !in_quotes;
                self.consume();
                continue;
            }

            // 在引号内，继续读取
            if in_quotes {
                self.consume();
                continue;
            }

            // 处理括号
            if ch == '(' {
                paren_count += 1;
                self.consume();
                continue;
            }

            if ch == ')' {
                if paren_count > 0 {
                    paren_count -= 1;
                    self.consume();
                    continue;
                }
                // 遇到右括号，条件结束
                break;
            }

            // 检查是否遇到逻辑运算符（不在引号内且没有未匹配的括号）
            if paren_count == 0 {
                if self.starts_with(" and ") || self.starts_with(" or ") {
                    break;
                }

                // 检查是否到达字符串末尾
                if self.position + 3 <= self.input.len() && &self.input[self.position..self.position + 3] == "and" {
                    let next_char = self.input.as_bytes().get(self.position + 3).map(|&b| b as char);
                    if next_char.is_none_or(|c| c.is_whitespace() || c == ')') {
                        break;
                    }
                }

                if self.position + 2 <= self.input.len() && &self.input[self.position..self.position + 2] == "or" {
                    let next_char = self.input.as_bytes().get(self.position + 2).map(|&b| b as char);
                    if next_char.is_none_or(|c| c.is_whitespace() || c == ')') {
                        break;
                    }
                }
            }

            self.consume();
        }

        // 提取条件并去除首尾空格
        let condition = &self.input[start..self.position].trim();
        condition.to_string()
    }

    fn peek(&self) -> char {
        self.input.as_bytes().get(self.position).map_or('\0', |&b| b as char)
    }

    fn peek_rest(&self) -> &str {
        &self.input[self.position..]
    }

    fn starts_with(&self, pattern: &str) -> bool {
        self.input[self.position..].starts_with(pattern)
    }

    fn consume(&mut self) {
        self.position += 1;
    }

    fn consume_n(&mut self, n: usize) {
        self.position += n;
    }

    /// 解析操作符字符串为 `CompareOp` 枚举
    fn parse_operator(op: &str) -> Option<CompareOp> {
        match op {
            "==" => Some(CompareOp::Eq),
            "!=" => Some(CompareOp::Ne),
            "<" => Some(CompareOp::Lt),
            ">" => Some(CompareOp::Gt),
            "<=" => Some(CompareOp::Le),
            ">=" => Some(CompareOp::Ge),
            _ => None,
        }
    }

    /// Parse a single filter condition
    fn parse_condition(expr: &str) -> Result<Option<FilterCondition>> {
        let expr = expr.trim();

        // Handle comparison operators
        let operators = ["==", "!=", "<=", ">=", "<", ">"];
        for op in &operators {
            if let Some(pos) = expr.find(op) {
                let field = expr[..pos].trim();
                let value = expr[pos + op.len()..].trim();

                let compare_op = Self::parse_operator(op)
                    .ok_or_else(|| StorageError::InvalidFilterExpression(format!("Unknown operator: {op}")))?;

                match field {
                    "name" => {
                        let value = Self::extract_quoted_value(value, "");
                        let pattern = Pattern::new(&value).map_err(|e| {
                            StorageError::InvalidFilterExpression(format!("Invalid glob pattern '{value}': {e}"))
                        })?;
                        return Ok(Some(FilterCondition::Name {
                            operator: compare_op,
                            pattern,
                        }));
                    }
                    "path" => {
                        let value = Self::extract_quoted_value(value, "");
                        let raw_value = value.trim_end_matches('/').to_string();
                        let pattern = Pattern::new(&raw_value).map_err(|e| {
                            StorageError::InvalidFilterExpression(format!("Invalid glob pattern '{raw_value}': {e}",))
                        })?;
                        let pattern_parts: Vec<String> = raw_value
                            .split('/')
                            .filter(|s| !s.is_empty())
                            .map(std::string::ToString::to_string)
                            .collect();
                        let pattern_depth = pattern_parts.len();
                        let has_double_wildcard = pattern_parts.iter().any(|p| p == "**");
                        let pattern_after_wildcard: Vec<String> = pattern_parts
                            .iter()
                            .skip_while(|p| p.as_str() != "**")
                            .skip(1)
                            .cloned()
                            .collect();
                        return Ok(Some(FilterCondition::Path {
                            operator: compare_op,
                            raw_value,
                            pattern,
                            pattern_parts,
                            pattern_depth,
                            has_double_wildcard,
                            pattern_after_wildcard,
                        }));
                    }
                    "type" => {
                        let value = Self::extract_quoted_value(value, "");
                        // 验证类型值是否有效
                        if value != "file" && value != "dir" && value != "symlink" {
                            return Err(StorageError::InvalidFilterExpression(format!(
                                "Invalid file type: {value}"
                            )));
                        }
                        return Ok(Some(FilterCondition::Type {
                            operator: compare_op,
                            value,
                        }));
                    }
                    "modified" => {
                        let modified_value = Self::parse_modified_value(value, compare_op)?;
                        return Ok(Some(FilterCondition::Modified {
                            operator: compare_op,
                            value: modified_value,
                        }));
                    }
                    "size" => {
                        let value = value
                            .parse::<u64>()
                            .map_err(|e| StorageError::InvalidPath(format!("Failed to parse size value: {e}")))?;
                        return Ok(Some(FilterCondition::Size {
                            operator: compare_op,
                            value,
                        }));
                    }
                    "extension" => {
                        let value = Self::extract_quoted_value(value, "");
                        let pattern = Pattern::new(&value).map_err(|e| {
                            StorageError::InvalidFilterExpression(format!("Invalid glob pattern '{value}': {e}"))
                        })?;
                        return Ok(Some(FilterCondition::Extension {
                            operator: compare_op,
                            pattern,
                        }));
                    }
                    "dir_date" => {
                        let epoch = parse_dir_date_value(value)?;
                        return Ok(Some(FilterCondition::DirDate {
                            operator: compare_op,
                            epoch,
                        }));
                    }
                    _ => {}
                }
            }
        }

        Ok(None)
    }

    /// 解析 modified 条件的值
    fn parse_modified_value(value: &str, op: CompareOp) -> Result<ModifiedValue> {
        let trimmed = value.trim();

        // 检查是否有引号包裹（绝对时间格式）
        let unquoted = if (trimmed.starts_with('"') && trimmed.ends_with('"'))
            || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        {
            &trimmed[1..trimmed.len() - 1]
        } else {
            trimmed
        };

        // 带引号且含 '-' → ISO 8601 日期 "YYYY-MM-DD" 或 "YYYY-MM-DDTHH:MM:SS"
        if unquoted != trimmed && unquoted.contains('-') {
            let epoch = parse_date_to_epoch(unquoted)?;
            return Ok(ModifiedValue::AbsoluteEpoch(epoch));
        }

        // 带引号且含 'T' → ISO 8601 日期时间
        if unquoted != trimmed && unquoted.contains('T') {
            let epoch = parse_date_to_epoch(unquoted)?;
            return Ok(ModifiedValue::AbsoluteEpoch(epoch));
        }

        // 8位及以上纯数字（≥ 10000000）→ 紧凑日期 YYYYMMDD
        if let Ok(num) = unquoted.parse::<i64>()
            && num >= 10_000_000
        {
            let epoch = parse_compact_date_to_epoch(unquoted)?;
            return Ok(ModifiedValue::AbsoluteEpoch(epoch));
        }

        // 其它：相对天数（带 d 后缀或纯数字）
        let days = if let Some(last_char) = unquoted.chars().last() {
            if last_char.is_alphabetic() {
                // 提取数字部分（如 "3d" → 3.0）
                let num_str: String = unquoted
                    .chars()
                    .take_while(|c| c.is_ascii_digit() || *c == '.')
                    .collect();
                num_str.parse::<f64>().map_err(|e| {
                    StorageError::InvalidFilterExpression(format!("Failed to parse modified value: {e}"))
                })?
            } else {
                unquoted.parse::<f64>().map_err(|e| {
                    StorageError::InvalidFilterExpression(format!("Failed to parse modified value: {e}"))
                })?
            }
        } else {
            unquoted
                .parse::<f64>()
                .map_err(|e| StorageError::InvalidFilterExpression(format!("Failed to parse modified value: {e}")))?
        };

        // 相对天数不支持 == 操作符
        if op == CompareOp::Eq {
            return Err(StorageError::InvalidFilterExpression(
                "Relative days modified condition does not support '==' operator".to_string(),
            ));
        }

        Ok(ModifiedValue::RelativeDays(days))
    }

    /// Extract quoted string value from expression
    fn extract_quoted_value(expr: &str, prefix: &str) -> String {
        if !prefix.is_empty()
            && let Some(start) = expr.find(prefix)
        {
            let rest = &expr[start + prefix.len()..];
            return Self::extract_quoted_value(rest, "");
        }

        let rest = expr.trim_start();

        // Handle both single and double quotes
        for quote_char in &['\"', '\''] {
            if let Some(quote_start) = rest.find(*quote_char) {
                let after_quote = &rest[quote_start + 1..];
                if let Some(quote_end) = after_quote.find(*quote_char) {
                    return after_quote[..quote_end].to_string();
                }
            }
        }

        // 如果没有引号，尝试提取下一个token
        rest.split_whitespace().next().unwrap_or("").to_string()
    }
}

/// 解析 "YYYY-MM-DD" 或 "YYYY-MM-DDTHH:MM:SS" 格式日期为 epoch seconds
fn parse_date_to_epoch(s: &str) -> Result<i64> {
    // 分离日期和时间部分
    let (date_part, time_part) = if let Some(t_pos) = s.find('T') {
        (&s[..t_pos], Some(&s[t_pos + 1..]))
    } else {
        (s, None)
    };

    let date_parts: Vec<&str> = date_part.split('-').collect();
    if date_parts.len() != 3 {
        return Err(StorageError::InvalidFilterExpression(format!(
            "Invalid date format '{s}', expected YYYY-MM-DD",
        )));
    }

    let year: i64 = date_parts[0]
        .parse()
        .map_err(|_| StorageError::InvalidFilterExpression(format!("Invalid year in date '{s}'")))?;
    let month: i64 = date_parts[1]
        .parse()
        .map_err(|_| StorageError::InvalidFilterExpression(format!("Invalid month in date '{s}'")))?;
    let day: i64 = date_parts[2]
        .parse()
        .map_err(|_| StorageError::InvalidFilterExpression(format!("Invalid day in date '{s}'")))?;

    let (hour, minute, second) = if let Some(tp) = time_part {
        let time_parts: Vec<&str> = tp.split(':').collect();
        if time_parts.len() != 3 {
            return Err(StorageError::InvalidFilterExpression(format!(
                "Invalid time format in '{s}', expected HH:MM:SS"
            )));
        }
        let h: i64 = time_parts[0]
            .parse()
            .map_err(|_| StorageError::InvalidFilterExpression(format!("Invalid hour in date '{s}'")))?;
        let m: i64 = time_parts[1]
            .parse()
            .map_err(|_| StorageError::InvalidFilterExpression(format!("Invalid minute in date '{s}'")))?;
        let sec: i64 = time_parts[2]
            .parse()
            .map_err(|_| StorageError::InvalidFilterExpression(format!("Invalid second in date '{s}'")))?;
        (h, m, sec)
    } else {
        (0, 0, 0)
    };

    Ok(date_to_epoch(year, month, day, hour, minute, second))
}

/// 解析 "YYYYMMDD" 格式紧凑日期为 epoch seconds
fn parse_compact_date_to_epoch(s: &str) -> Result<i64> {
    if s.len() < 8 {
        return Err(StorageError::InvalidFilterExpression(format!(
            "Invalid compact date format '{s}', expected YYYYMMDD"
        )));
    }

    let year: i64 = s[..4]
        .parse()
        .map_err(|_| StorageError::InvalidFilterExpression(format!("Invalid year in compact date '{s}'")))?;
    let month: i64 = s[4..6]
        .parse()
        .map_err(|_| StorageError::InvalidFilterExpression(format!("Invalid month in compact date '{s}'")))?;
    let day: i64 = s[6..8]
        .parse()
        .map_err(|_| StorageError::InvalidFilterExpression(format!("Invalid day in compact date '{s}'")))?;

    Ok(date_to_epoch(year, month, day, 0, 0, 0))
}

/// 将年月日时分秒转换为 Unix epoch seconds（UTC）
/// 使用简化的日期算法（基于 Civil 日期到 Unix 天数的转换）
fn date_to_epoch(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64) -> i64 {
    // 调整月份：将 1-2 月视为前一年的 13-14 月，简化闰年计算
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };

    // 从公元0年到给定日期的天数（Rata Die 算法变体）
    let days = 365 * y + y / 4 - y / 100 + y / 400 + (m * 306 + 5) / 10 + (day - 1) - 719_468;

    days * 86400 + hour * 3600 + minute * 60 + second
}

/// 验证月份和日期是否有效
fn is_valid_date(month: i64, day: i64) -> bool {
    (1..=12).contains(&month) && (1..=31).contains(&day)
}

/// 从目录名中提取日期，按 YYYY-MM-DD → YYYYMMDD → YYMMDD 优先级扫描
///
/// 支持日期出现在目录名的开头、中间或末尾，例如：
/// - "20240301" / "240301" / "2024-03-01"
/// - `"backup_240301"` / `"20240301_logs"` / "project_2024-03-01_final"
///
/// 返回提取到的日期对应的 UTC 午夜 epoch seconds，未找到有效日期则返回 None
fn extract_date_from_dir_name(name: &str) -> Option<i64> {
    let bytes = name.as_bytes();
    let len = bytes.len();

    // 1. 搜索 YYYY-MM-DD 模式（10 字符：4位数字-2位数字-2位数字）
    if len >= 10 {
        for i in 0..=len - 10 {
            if bytes[i + 4] == b'-' && bytes[i + 7] == b'-' {
                let all_digits = bytes[i..i + 4].iter().all(u8::is_ascii_digit)
                    && bytes[i + 5..i + 7].iter().all(u8::is_ascii_digit)
                    && bytes[i + 8..i + 10].iter().all(u8::is_ascii_digit);
                if all_digits {
                    let year = name[i..i + 4].parse::<i64>().ok()?;
                    let month = name[i + 5..i + 7].parse::<i64>().ok()?;
                    let day = name[i + 8..i + 10].parse::<i64>().ok()?;
                    if is_valid_date(month, day) {
                        return Some(date_to_epoch(year, month, day, 0, 0, 0));
                    }
                }
            }
        }
    }

    // 2/3. 搜索连续数字串，按长度判定 YYYYMMDD 或 YYMMDD
    let mut i = 0;
    while i < len {
        if bytes[i].is_ascii_digit() {
            let start = i;
            while i < len && bytes[i].is_ascii_digit() {
                i += 1;
            }
            let digit_len = i - start;
            let digit_str = &name[start..i];

            // ≥8 位：取前 8 位尝试 YYYYMMDD
            if digit_len >= 8 {
                let year = digit_str[..4].parse::<i64>().ok();
                let month = digit_str[4..6].parse::<i64>().ok();
                let day = digit_str[6..8].parse::<i64>().ok();
                if let (Some(y), Some(m), Some(d)) = (year, month, day)
                    && y >= 1900
                    && is_valid_date(m, d)
                {
                    return Some(date_to_epoch(y, m, d, 0, 0, 0));
                }
            }

            // 恰好 6 位：尝试 YYMMDD
            if digit_len == 6 {
                let yy = digit_str[..2].parse::<i64>().ok();
                let month = digit_str[2..4].parse::<i64>().ok();
                let day = digit_str[4..6].parse::<i64>().ok();
                if let (Some(y), Some(m), Some(d)) = (yy, month, day)
                    && is_valid_date(m, d)
                {
                    return Some(date_to_epoch(2000 + y, m, d, 0, 0, 0));
                }
            }
        } else {
            i += 1;
        }
    }

    None
}

/// 解析 `dir_date` 条件值为 epoch seconds
///
/// 支持格式：
/// - "2024-03-01" 或 '2024-03-01'（带引号 ISO 日期）
/// - 20240301（8 位 YYYYMMDD）
/// - 240301（6 位 YYMMDD，year = 2000+YY）
fn parse_dir_date_value(value: &str) -> Result<i64> {
    let trimmed = value.trim();

    // 去引号
    let unquoted = if (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
    {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    // 含 '-' → YYYY-MM-DD
    if unquoted.contains('-') {
        return parse_date_to_epoch(unquoted);
    }

    // 纯数字
    let digit_len = unquoted.len();
    if digit_len >= 8 {
        return parse_compact_date_to_epoch(unquoted);
    }

    if digit_len == 6 {
        let yy: i64 = unquoted[..2].parse().map_err(|_| {
            StorageError::InvalidFilterExpression(format!("Invalid dir_date value '{unquoted}': bad year"))
        })?;
        let mm: i64 = unquoted[2..4].parse().map_err(|_| {
            StorageError::InvalidFilterExpression(format!("Invalid dir_date value '{unquoted}': bad month"))
        })?;
        let dd: i64 = unquoted[4..6].parse().map_err(|_| {
            StorageError::InvalidFilterExpression(format!("Invalid dir_date value '{unquoted}': bad day"))
        })?;
        if !is_valid_date(mm, dd) {
            return Err(StorageError::InvalidFilterExpression(format!(
                "Invalid dir_date value '{unquoted}': month or day out of range",
            )));
        }
        return Ok(date_to_epoch(2000 + yy, mm, dd, 0, 0, 0));
    }

    Err(StorageError::InvalidFilterExpression(format!(
        "Invalid dir_date value '{value}', expected YYMMDD, YYYYMMDD, or YYYY-MM-DD",
    )))
}

// 表达式节点
#[derive(Debug, Clone)]
enum FilterASTNode {
    Condition(FilterCondition),
    And(Box<FilterASTNode>, Box<FilterASTNode>),
    Or(Box<FilterASTNode>, Box<FilterASTNode>),
}

// ========== Parser（语法分析器） ==========
struct FilterParser {
    tokens: Vec<Token>,
    position: usize,
}

impl FilterParser {
    fn new(tokens: Vec<Token>) -> Self {
        FilterParser { tokens, position: 0 }
    }

    fn current_token(&self) -> Option<&Token> {
        if self.position < self.tokens.len() {
            Some(&self.tokens[self.position])
        } else {
            None
        }
    }

    fn consume(&mut self) -> Option<Token> {
        if self.position < self.tokens.len() {
            let token = self.tokens[self.position].clone();
            self.position += 1;
            Some(token)
        } else {
            None
        }
    }

    fn parse_expression(&mut self) -> Result<FilterASTNode> {
        let expr = self.parse_or_expr()?;

        // 检查是否还有剩余的token
        if self.current_token().is_some() {
            return Err(StorageError::InvalidFilterExpression(
                "Unexpected tokens after expression".to_string(),
            ));
        }

        Ok(expr)
    }

    // 解析 OR 表达式 (优先级最低)
    fn parse_or_expr(&mut self) -> Result<FilterASTNode> {
        let mut left = self.parse_and_expr()?;

        while let Some(Token::Or) = self.current_token() {
            self.consume(); // 消费 'or'
            let right = self.parse_and_expr()?;
            left = FilterASTNode::Or(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    // 解析 AND 表达式 (优先级较高)
    fn parse_and_expr(&mut self) -> Result<FilterASTNode> {
        let mut left = self.parse_primary()?;

        while let Some(Token::And) = self.current_token() {
            self.consume(); // 消费 'and'
            let right = self.parse_primary()?;
            left = FilterASTNode::And(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    // 解析基础表达式：变量和括号表达式
    fn parse_primary(&mut self) -> Result<FilterASTNode> {
        match self.current_token() {
            Some(Token::Condition(condition)) => {
                let condition_clone = condition.clone();
                self.consume();
                Ok(FilterASTNode::Condition(condition_clone))
            }
            Some(Token::LParen) => {
                self.consume(); // 消费 '('
                let expr = self.parse_or_expr()?;
                if let Some(Token::RParen) = self.current_token() {
                    self.consume(); // 消费 ')'
                    Ok(expr)
                } else {
                    Err(StorageError::MismatchedParentheses(
                        "Mismatched parentheses".to_string(),
                    ))
                }
            }
            Some(token) => Err(StorageError::InvalidToken(
                format!("{token:?}").chars().next().unwrap_or(' '),
            )),
            None => Err(StorageError::UnexpectedEndOfToken(
                "Unexpected end of filter token".to_string(),
            )),
        }
    }
}

// 主结构
#[derive(Debug, Clone)]
pub struct FilterExpression {
    root: FilterASTNode,
}

// ========== FilterExpression 实现 ==========
impl FilterExpression {
    /// 解析过滤表达式字符串，构建逻辑表达式树
    fn parse(expr: &str) -> Result<Self> {
        trace!("[FilterExpression::parse] parsing filter expression: {}", expr);

        let mut lexer = Lexer::new(expr);
        let tokens = lexer.tokenize()?;

        let mut parser = FilterParser::new(tokens);
        let root = parser.parse_expression()?;

        trace!(
            "[FilterExpression::parse] parsed filter expression: {} => {:?}",
            expr, root
        );
        Ok(FilterExpression { root })
    }

    /// 评估过滤表达式对文件的匹配情况
    #[allow(clippy::too_many_arguments)]
    fn evaluate(
        &self, file_name: Option<&str>, file_path: Option<&str>, file_type: Option<&str>, modified_epoch: Option<i64>,
        size: Option<u64>, extension: Option<&str>, now_epoch: i64,
    ) -> MatchResult {
        trace!(
            "[FilterExpression::evaluate] evaluating filter expression: {:?}, file_name: {:?}, file_path: {:?}, file_type: {:?}, modified_epoch: {:?}, size: {:?}, extension: {:?}",
            self.root, file_name, file_path, file_type, modified_epoch, size, extension
        );

        let result = Self::evaluate_recursive(
            &self.root,
            file_name,
            file_path,
            file_type,
            modified_epoch,
            size,
            extension,
            now_epoch,
        );

        trace!(
            "[FilterExpression::evaluate] evaluated filter expression result: {:?}",
            result
        );
        result
    }

    /// 递归评估过滤表达式树（含短路求值）
    #[allow(clippy::too_many_arguments)]
    fn evaluate_recursive(
        ast_node: &FilterASTNode, file_name: Option<&str>, file_path: Option<&str>, file_type: Option<&str>,
        modified_epoch: Option<i64>, size: Option<u64>, extension: Option<&str>, now_epoch: i64,
    ) -> MatchResult {
        match ast_node {
            FilterASTNode::Condition(condition) => Self::evaluate_condition(
                condition,
                file_name,
                file_path,
                file_type,
                modified_epoch,
                size,
                extension,
                now_epoch,
            ),
            FilterASTNode::And(left, right) => {
                let left_result = Self::evaluate_recursive(
                    left,
                    file_name,
                    file_path,
                    file_type,
                    modified_epoch,
                    size,
                    extension,
                    now_epoch,
                );

                // 短路求值：左侧 MisMatch 时直接返回
                if let MatchResult::MisMatch(_) = &left_result {
                    trace!(
                        "\n[FilterExpression::evaluate_recursive] and short-circuit: left={:?} => {:?}",
                        left, left_result
                    );
                    return left_result;
                }

                let right_result = Self::evaluate_recursive(
                    right,
                    file_name,
                    file_path,
                    file_type,
                    modified_epoch,
                    size,
                    extension,
                    now_epoch,
                );

                trace!(
                    "\n[FilterExpression::evaluate_recursive] and(left: {:?} => {:?}, right: {:?} => {:?}) => {:?}",
                    left,
                    left_result.clone(),
                    right,
                    right_result.clone(),
                    left_result.clone() & right_result.clone()
                );

                left_result & right_result
            }
            FilterASTNode::Or(left, right) => {
                let left_result = Self::evaluate_recursive(
                    left,
                    file_name,
                    file_path,
                    file_type,
                    modified_epoch,
                    size,
                    extension,
                    now_epoch,
                );

                // 短路求值：左侧 Match 时直接返回
                if let MatchResult::Match(_) = &left_result {
                    trace!(
                        "\n[FilterExpression::evaluate_recursive] or short-circuit: left={:?} => {:?}",
                        left, left_result
                    );
                    return left_result;
                }

                let right_result = Self::evaluate_recursive(
                    right,
                    file_name,
                    file_path,
                    file_type,
                    modified_epoch,
                    size,
                    extension,
                    now_epoch,
                );

                trace!(
                    "\n[FilterExpression::evaluate_recursive] or(left: {:?} => {:?}, right: {:?} => {:?}) => {:?}",
                    left,
                    left_result.clone(),
                    right,
                    right_result.clone(),
                    left_result.clone() | right_result.clone()
                );

                left_result | right_result
            }
        }
    }

    /// 评估单个过滤条件对文件的匹配情况
    #[allow(clippy::too_many_arguments)]
    fn evaluate_condition(
        condition: &FilterCondition, file_name: Option<&str>, file_path: Option<&str>, file_type: Option<&str>,
        modified_epoch: Option<i64>, size: Option<u64>, extension: Option<&str>, now_epoch: i64,
    ) -> MatchResult {
        match condition {
            FilterCondition::Name { operator, pattern } => file_name.map_or(MatchResult::LazyMatch, |file_name| {
                let is_match = pattern.matches_with(file_name, GLOB_MATCH_OPTIONS);
                MatchResult::from_bool(match operator {
                    CompareOp::Eq => is_match,
                    CompareOp::Ne => !is_match,
                    _ => false,
                })
            }),
            FilterCondition::Path {
                operator,
                raw_value,
                pattern,
                pattern_parts,
                pattern_depth,
                has_double_wildcard,
                pattern_after_wildcard,
            } => file_path.map_or(MatchResult::LazyMatch, |file_path| {
                let file_path = file_path.trim_end_matches('/');
                let match_result = match_path_with_pattern(
                    file_path,
                    file_type,
                    pattern,
                    raw_value,
                    pattern_parts,
                    *pattern_depth,
                    *has_double_wildcard,
                    pattern_after_wildcard,
                );
                match operator {
                    CompareOp::Eq => match_result,
                    CompareOp::Ne => match match_result {
                        MatchResult::Match(MatchAddon::PathMatch | MatchAddon::MixMatch) => {
                            MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch)
                        }
                        MatchResult::Match(_) | MatchResult::PartialMatch => {
                            MatchResult::MisMatch(MisMatchAddon::Other)
                        }
                        MatchResult::MisMatch(_) => MatchResult::Match(MatchAddon::PathMatch),
                        MatchResult::LazyMatch => MatchResult::LazyMatch,
                    },
                    _ => MatchResult::MisMatch(MisMatchAddon::Other),
                }
            }),
            FilterCondition::Type { operator, value } => file_type.map_or(MatchResult::LazyMatch, |file_type| {
                MatchResult::from_bool(match operator {
                    CompareOp::Eq => file_type == value,
                    CompareOp::Ne => file_type != value,
                    _ => false,
                })
            }),
            FilterCondition::Modified { operator, value } => {
                modified_epoch.map_or(MatchResult::LazyMatch, |file_epoch| match value {
                    ModifiedValue::RelativeDays(days) => {
                        let file_days = (now_epoch - file_epoch) as f64 / 86400.0;
                        MatchResult::from_bool(match operator {
                            CompareOp::Lt => file_days < *days,
                            CompareOp::Gt => file_days > *days,
                            CompareOp::Le => file_days <= *days,
                            CompareOp::Ge => file_days >= *days,
                            _ => false,
                        })
                    }
                    ModifiedValue::AbsoluteEpoch(ts) => {
                        MatchResult::from_bool(match operator {
                            CompareOp::Eq => file_epoch / 86400 == ts / 86400, // 按天粒度比较
                            CompareOp::Ne => file_epoch / 86400 != ts / 86400,
                            CompareOp::Lt => file_epoch < *ts,
                            CompareOp::Gt => file_epoch > *ts,
                            CompareOp::Le => file_epoch <= *ts,
                            CompareOp::Ge => file_epoch >= *ts,
                        })
                    }
                })
            }
            FilterCondition::Size { operator, value } => size.map_or(MatchResult::LazyMatch, |size| {
                MatchResult::from_bool(match operator {
                    CompareOp::Eq => size == *value,
                    CompareOp::Lt => size < *value,
                    CompareOp::Gt => size > *value,
                    CompareOp::Le => size <= *value,
                    CompareOp::Ge => size >= *value,
                    CompareOp::Ne => false,
                })
            }),
            FilterCondition::Extension { operator, pattern } => extension.map_or(MatchResult::LazyMatch, |extension| {
                let is_match = pattern.matches_with(extension, GLOB_MATCH_OPTIONS);
                MatchResult::from_bool(match operator {
                    CompareOp::Eq => is_match,
                    CompareOp::Ne => !is_match,
                    _ => false,
                })
            }),
            FilterCondition::DirDate { operator, epoch } => {
                // 非目录条目：透明通过
                if file_type != Some("dir") {
                    return MatchResult::Match(MatchAddon::NonPathMatch);
                }

                // 从目录名中提取日期
                let Some(dir_name) = file_name else {
                    return MatchResult::Match(MatchAddon::NonPathMatch);
                };

                match extract_date_from_dir_name(dir_name) {
                    Some(dir_epoch) => {
                        // 目录包含日期，进行比较
                        let matches = match operator {
                            CompareOp::Eq => dir_epoch / 86400 == epoch / 86400, // 按天粒度
                            CompareOp::Ne => dir_epoch / 86400 != epoch / 86400,
                            CompareOp::Lt => dir_epoch < *epoch,
                            CompareOp::Gt => dir_epoch > *epoch,
                            CompareOp::Le => dir_epoch <= *epoch,
                            CompareOp::Ge => dir_epoch >= *epoch,
                        };
                        if matches {
                            // 日期匹配：PathMatch → 子项免检
                            MatchResult::Match(MatchAddon::PathMatch)
                        } else {
                            // 日期不匹配：FullPathNotMatch → 跳过 + 停止扫描子项
                            MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch)
                        }
                    }
                    None => {
                        // 目录名不含日期：透明通过
                        MatchResult::Match(MatchAddon::NonPathMatch)
                    }
                }
            }
        }
    }

    /// 检查目录名是否匹配 `match_expressions` 中的 `DirDate` 条件
    ///
    /// 仅在 packaged 模式下由 walkdir 调用，用于判断一级目录是否应发射 Packaged 消息。
    /// 递归遍历 AST，找到所有 `DirDate` 节点，提取目录名中的日期并比较。
    ///
    /// 返回 true 表示目录名中的日期满足至少一个 `DirDate` 条件。
    pub fn has_matching_dir_date(&self, dir_name: &str) -> bool {
        let Some(dir_epoch) = extract_date_from_dir_name(dir_name) else {
            return false;
        };
        Self::check_dir_date_in_node(&self.root, dir_epoch)
    }

    /// 递归检查 AST 节点中的 `DirDate` 条件是否匹配给定的 `dir_epoch`
    fn check_dir_date_in_node(node: &FilterASTNode, dir_epoch: i64) -> bool {
        match node {
            FilterASTNode::Condition(FilterCondition::DirDate { operator, epoch }) => match operator {
                CompareOp::Eq => dir_epoch / 86400 == epoch / 86400,
                CompareOp::Ne => dir_epoch / 86400 != epoch / 86400,
                CompareOp::Lt => dir_epoch < *epoch,
                CompareOp::Gt => dir_epoch > *epoch,
                CompareOp::Le => dir_epoch <= *epoch,
                CompareOp::Ge => dir_epoch >= *epoch,
            },
            FilterASTNode::Condition(_) => false, // 非 DirDate 条件，忽略
            FilterASTNode::And(left, right) => {
                Self::check_dir_date_in_node(left, dir_epoch) && Self::check_dir_date_in_node(right, dir_epoch)
            }
            FilterASTNode::Or(left, right) => {
                Self::check_dir_date_in_node(left, dir_epoch) || Self::check_dir_date_in_node(right, dir_epoch)
            }
        }
    }

    /// 计算逻辑表达式树中的节点数量
    #[allow(dead_code)]
    fn count_nodes(&self) -> usize {
        Self::count_nodes_in_expr(&self.root)
    }

    /// 递归计算表达式树中的节点数量
    #[allow(dead_code)]
    fn count_nodes_in_expr(expr: &FilterASTNode) -> usize {
        match expr {
            FilterASTNode::Condition(_) => 1,
            FilterASTNode::And(left, right) | FilterASTNode::Or(left, right) => {
                1 + Self::count_nodes_in_expr(left) + Self::count_nodes_in_expr(right)
            }
        }
    }
}

/// 匹配附加信息枚举，用于标识匹配的类型
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
enum MatchAddon {
    /// 只有路径条件的匹配
    PathMatch,
    /// 没有路径条件的匹配
    NonPathMatch,
    /// 既有路径条件又有非路径条件的匹配
    MixMatch,
}

/// 不匹配附加信息枚举，用于标识不匹配的原因
#[derive(Debug, Clone, PartialEq)]
enum MisMatchAddon {
    /// 文件路径深度超过条件中指定的深度
    FullPathNotMatch,
    /// 其他导致不匹配的原因
    Other,
}

/// 匹配结果枚举
#[derive(Debug, Clone, PartialEq)]
enum MatchResult {
    /// 完全匹配
    Match(MatchAddon),
    /// 部分匹配，仅在目录匹配Path时才可能有该类型
    PartialMatch,
    /// 不匹配
    MisMatch(MisMatchAddon),
    /// 匹配条件不充分，LazyMatch
    LazyMatch,
}

impl MatchResult {
    /// Convert a boolean to `MatchResult`, only applied to Path condition
    /// - When b is true: return `Match(MatchAddon::NonPathMatch)`
    /// - When b is false: return `MisMatch(MisMatchAddon::Other)`
    fn from_bool(b: bool) -> Self {
        if b {
            MatchResult::Match(MatchAddon::NonPathMatch)
        } else {
            MatchResult::MisMatch(MisMatchAddon::Other)
        }
    }
}

impl BitAnd for MatchResult {
    type Output = Self;

    /// 与操作符实现
    ///
    /// 优先级:  `MisMatch` > `PartialMatch` > `Match` > `LazyMatch`
    /// 使用 `&` 运算符调用
    fn bitand(self, other: Self) -> Self {
        match (self, other) {
            // 两个操作数均为 MisMatch 的情况
            (MatchResult::MisMatch(addon1), MatchResult::MisMatch(addon2)) => {
                // 优先保留 FullPathNotMatch 标记
                let result_addon = match (addon1, addon2) {
                    (MisMatchAddon::FullPathNotMatch, _) | (_, MisMatchAddon::FullPathNotMatch) => {
                        MisMatchAddon::FullPathNotMatch
                    }
                    _ => MisMatchAddon::Other,
                };
                MatchResult::MisMatch(result_addon)
            }
            // 任一操作数为 MisMatch 的情况
            (MatchResult::MisMatch(addon), _) | (_, MatchResult::MisMatch(addon)) => MatchResult::MisMatch(addon),
            // 任一操作数为 PartialMatch 的情况
            (MatchResult::PartialMatch, _) | (_, MatchResult::PartialMatch) => MatchResult::PartialMatch,
            // 两个操作数均为 Match 的情况
            (MatchResult::Match(addon1), MatchResult::Match(addon2)) => {
                // 合并 MatchAddon 标记，优先级：MixMatch > PathMatch > NonPathMatch
                let result_addon = match (addon1, addon2) {
                    (MatchAddon::PathMatch, MatchAddon::PathMatch) => MatchAddon::PathMatch,
                    (MatchAddon::NonPathMatch, MatchAddon::NonPathMatch) => MatchAddon::NonPathMatch,
                    _ => MatchAddon::MixMatch,
                };
                MatchResult::Match(result_addon)
            }
            // 任一操作数为 Match 的情况
            (MatchResult::Match(addon), _) | (_, MatchResult::Match(addon)) => MatchResult::Match(addon),
            // 任一操作数为 LazyMatch 的情况
            (MatchResult::LazyMatch, MatchResult::LazyMatch) => MatchResult::LazyMatch,
        }
    }
}

impl BitOr for MatchResult {
    type Output = Self;

    /// 或操作符实现
    ///
    /// 优先级: `Match` > `PartialMatch` > `MisMatch` > `LazyMatch`
    /// 使用 `|` 运算符调用
    fn bitor(self, other: Self) -> Self {
        match (self, other) {
            // 两个操作数均为 Match 的情况
            (MatchResult::Match(addon1), MatchResult::Match(addon2)) => {
                // 合并 MatchAddon 标记，优先级：MixMatch > PathMatch > NonPathMatch
                let result_addon = match (addon1, addon2) {
                    (MatchAddon::PathMatch, MatchAddon::PathMatch) => MatchAddon::PathMatch,
                    (MatchAddon::NonPathMatch, MatchAddon::NonPathMatch) => MatchAddon::NonPathMatch,
                    _ => MatchAddon::MixMatch,
                };
                MatchResult::Match(result_addon)
            }
            // 任一操作数为 Match 的情况
            (MatchResult::Match(addon), _) | (_, MatchResult::Match(addon)) => MatchResult::Match(addon),

            // 任一操作数为 PartialMatch 的情况
            (MatchResult::PartialMatch, _) | (_, MatchResult::PartialMatch) => MatchResult::PartialMatch,
            // 两个操作数均为 MisMatch 的情况
            (MatchResult::MisMatch(addon1), MatchResult::MisMatch(addon2)) => {
                // 优先保留 FullPathNotMatch 标记
                let result_addon = match (addon1, addon2) {
                    (MisMatchAddon::FullPathNotMatch, _) | (_, MisMatchAddon::FullPathNotMatch) => {
                        MisMatchAddon::FullPathNotMatch
                    }
                    _ => MisMatchAddon::Other,
                };
                MatchResult::MisMatch(result_addon)
            }
            // 任一操作数为 MisMatch 的情况
            (MatchResult::MisMatch(addon), _) | (_, MatchResult::MisMatch(addon)) => MatchResult::MisMatch(addon),
            // 两个操作数为 LazyMatch 的情况
            (MatchResult::LazyMatch, MatchResult::LazyMatch) => MatchResult::LazyMatch,
        }
    }
}

/// 匹配文件路径与模式，返回匹配结果
///
/// 该函数用于判断文件路径是否与给定的模式匹配，支持三种匹配结果：
/// - `MatchResult::Match`: 完全匹配，文件路径与模式完全匹配
/// - `MatchResult::PartialMatch`: 目录部分匹配，仅对目录有效，表示目录路径匹配模式的前缀
/// - `MatchResult::MisMatch`: 不匹配，文件路径与模式不匹配
///
/// # 参数
/// - `file_path`: 要匹配的文件路径
/// - `file_type`: 文件类型，可选值为"file"、"dir"或"symlink"
/// - `pattern`: 预编译的 glob Pattern
/// - `raw_value`: 原始 pattern 字符串（用于日志）
/// - `pattern_parts`: 按 '/' 分割后的各段
/// - `pattern_depth`: pattern 段数
/// - `has_double_wildcard`: 是否含 **
/// - `pattern_after_wildcard`: ** 之后的部分
#[allow(clippy::too_many_arguments)]
fn match_path_with_pattern(
    file_path: &str, file_type: Option<&str>, pattern: &Pattern, raw_value: &str, pattern_parts: &[String],
    pattern_depth: usize, has_double_wildcard: bool, pattern_after_wildcard: &[String],
) -> MatchResult {
    trace!(
        "[Filter:match_path_with_pattern] 开始匹配: pattern_value={}, file_path={}, file_type={:?}",
        raw_value, file_path, file_type
    );

    // 检查全路径匹配
    if pattern.matches_with(file_path, GLOB_MATCH_OPTIONS) {
        trace!(
            "[Filter:match_path_with_pattern] 全路径匹配成功: pattern_value={}, file_path={}",
            raw_value, file_path
        );
        return MatchResult::Match(MatchAddon::PathMatch);
    }

    // 祖先路径匹配：当 file_depth > pattern_depth 时，截取祖先路径用原始 pattern 匹配
    // 因为 require_literal_separator=true，* 只匹配单段，** 匹配多段，
    // pattern.matches_with() 本身已正确处理两种情况，无需截断 pattern
    let file_parts: Vec<&str> = file_path.split('/').filter(|s| !s.is_empty()).collect();
    let file_depth = file_parts.len();

    if file_depth > pattern_depth {
        if has_double_wildcard {
            // 有 **：pattern 结构为 [prefix]/**/[suffix]
            // ** 必须是独立路径分量，prefix_count + suffix_count 是祖先最小深度
            // 只需遍历 min_depth..file_depth，大幅收窄范围
            let double_star_pos = pattern_parts.iter().position(|p| p == "**").unwrap_or(0);
            let prefix_count = double_star_pos;
            let suffix_count = pattern_after_wildcard.len();
            let min_depth = prefix_count + suffix_count;
            for depth in min_depth..file_depth {
                let ancestor_path = file_parts[..depth].join("/");
                if pattern.matches_with(&ancestor_path, GLOB_MATCH_OPTIONS) {
                    trace!(
                        "[Filter:match_path_with_pattern] 祖先路径匹配成功(含**): pattern_value={}, file_path={}, ancestor={}",
                        raw_value, file_path, ancestor_path
                    );
                    return MatchResult::Match(MatchAddon::PathMatch);
                }
            }
        } else {
            // 无 **：* 不跨 /，只有 depth=pattern_depth 的祖先可能匹配 → O(1)
            let ancestor_path = file_parts[..pattern_depth].join("/");
            if pattern.matches_with(&ancestor_path, GLOB_MATCH_OPTIONS) {
                trace!(
                    "[Filter:match_path_with_pattern] 祖先路径匹配成功: pattern_value={}, file_path={}, ancestor={}",
                    raw_value, file_path, ancestor_path
                );
                return MatchResult::Match(MatchAddon::PathMatch);
            }
        }
    }

    // 如果不是目录，且祖先路径也没命中，直接返回 MisMatch
    if file_type != Some("dir") {
        trace!(
            "[Filter:match_path_with_pattern] 不是目录且祖先不匹配，返回NoMatch: file_path={}, file_type={:?}",
            file_path, file_type
        );
        return MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch);
    }

    // 如果文件深度大于等于模式深度，无需目录部分匹配
    if !has_double_wildcard && file_depth >= pattern_depth {
        trace!(
            "[Filter:match_path_with_pattern] 文件深度大于等于模式深度，无需目录部分匹配: file_path={}, file_depth={}, pattern_depth={}, has_double_wildcard={}, 返回NoMatch",
            file_path, file_depth, pattern_depth, has_double_wildcard
        );

        // Return FullPathNotMatch regardless of wildcards
        return MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch);
    }

    trace!(
        "[Filter:match_path_with_pattern] 开始目录部分匹配检查: file_depth={}, pattern_depth={}, has_double_wildcard={}",
        file_depth, pattern_depth, has_double_wildcard
    );

    // 统一截取策略：构建截断 pattern 并匹配
    let mut new_parts = Vec::new();
    let mut current_depth = 0;
    let mut hit_double_wildcard = false;

    for part in pattern_parts {
        if current_depth >= file_depth {
            break;
        }

        new_parts.push(part.as_str());

        if part == "**" {
            current_depth = file_depth;
            hit_double_wildcard = true;
        } else {
            current_depth += 1;
        }
    }

    let new_pattern = new_parts.join("/");
    if let Ok(p) = Pattern::new(&new_pattern)
        && p.matches_with(file_path, GLOB_MATCH_OPTIONS)
    {
        // ** 被命中且后面还有具体 pattern 时，需验证目录名是否匹配后缀
        // 避免黑名单中 ** 开头的 pattern 过度跳过无关目录
        if hit_double_wildcard && !pattern_after_wildcard.is_empty() {
            if let Some(suffix) = pattern_after_wildcard.first()
                && let Ok(sp) = Pattern::new(suffix)
            {
                let file_last = file_path.rsplit('/').next().unwrap_or(file_path);
                if sp.matches_with(file_last, GLOB_MATCH_OPTIONS) {
                    trace!(
                        "[Filter:match_path_with_pattern] 含有double wildcard的目录部分匹配成功(后缀匹配): pattern_value={}, file_path={}",
                        raw_value, file_path
                    );
                    return MatchResult::PartialMatch;
                }
            }
            // 不匹配后缀 → 返回 MisMatch(Other) 让黑名单不跳过，但扫描继续
            trace!(
                "[Filter:match_path_with_pattern] double wildcard 后缀不匹配，返回MisMatch(Other): pattern_value={}, file_path={}",
                raw_value, file_path
            );
            return MatchResult::MisMatch(MisMatchAddon::Other);
        }

        trace!(
            "[Filter:match_path_with_pattern] 目录部分匹配成功: pattern_value={}, file_path={}",
            raw_value, file_path
        );
        return MatchResult::PartialMatch;
    }

    // 匹配失败，返回NoMatch
    trace!(
        "[Filter:match_path_with_pattern] 匹配失败，返回NoMatch: pattern_value={}, file_path={}",
        raw_value, file_path
    );
    MatchResult::MisMatch(MisMatchAddon::Other)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Helper Functions ====================

    /// Helper to parse a filter expression and evaluate it, returning MatchResult.
    fn eval(
        expr_str: &str, file_name: Option<&str>, file_path: Option<&str>, file_type: Option<&str>,
        modified_epoch: Option<i64>, size: Option<u64>, extension: Option<&str>,
    ) -> MatchResult {
        let expr = FilterExpression::parse(expr_str).expect("Failed to parse expression");
        let now_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        evaluate_filter(
            &expr,
            file_name,
            file_path,
            file_type,
            modified_epoch,
            size,
            extension,
            now_epoch,
        )
    }

    /// Helper to evaluate with explicit now_epoch (for deterministic modified tests).
    fn eval_with_now(
        expr_str: &str, file_name: Option<&str>, file_path: Option<&str>, file_type: Option<&str>,
        modified_epoch: Option<i64>, size: Option<u64>, extension: Option<&str>, now_epoch: i64,
    ) -> MatchResult {
        let expr = FilterExpression::parse(expr_str).expect("Failed to parse expression");
        evaluate_filter(
            &expr,
            file_name,
            file_path,
            file_type,
            modified_epoch,
            size,
            extension,
            now_epoch,
        )
    }

    /// Helper to call should_skip with match/exclude expressions.
    fn skip(
        match_expr: Option<&str>, exclude_expr: Option<&str>, file_name: Option<&str>, file_path: Option<&str>,
        file_type: Option<&str>, modified_epoch: Option<i64>, size: Option<u64>, extension: Option<&str>,
    ) -> (bool, bool, bool) {
        let match_parsed = match_expr.map(|e| FilterExpression::parse(e).expect("Failed to parse match expr"));
        let exclude_parsed = exclude_expr.map(|e| FilterExpression::parse(e).expect("Failed to parse exclude expr"));
        should_skip(
            match_parsed.as_ref(),
            exclude_parsed.as_ref(),
            file_name,
            file_path,
            file_type,
            modified_epoch,
            size,
            extension,
        )
    }

    // ==================== 7.1 Basic parsing / existing test updates ====================

    #[test]
    fn test_parse_basic_name_condition() {
        let expr = FilterExpression::parse("name == \"*.txt\"").unwrap();
        assert_eq!(expr.count_nodes(), 1);
    }

    #[test]
    fn test_parse_basic_path_condition() {
        let expr = FilterExpression::parse("path == \"src/**\"").unwrap();
        assert_eq!(expr.count_nodes(), 1);
    }

    #[test]
    fn test_parse_and_expression() {
        let expr = FilterExpression::parse("name == \"*.txt\" and size > 100").unwrap();
        assert_eq!(expr.count_nodes(), 3); // And + 2 conditions
    }

    #[test]
    fn test_parse_or_expression() {
        let expr = FilterExpression::parse("name == \"*.txt\" or name == \"*.rs\"").unwrap();
        assert_eq!(expr.count_nodes(), 3);
    }

    #[test]
    fn test_parse_parenthesized_expression() {
        let expr = FilterExpression::parse("(name == \"*.txt\" or name == \"*.rs\") and type == file").unwrap();
        assert_eq!(expr.count_nodes(), 5);
    }

    #[test]
    fn test_basic_name_match() {
        let r = eval("name == \"*.txt\"", Some("hello.txt"), None, None, None, None, None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_basic_name_mismatch() {
        let r = eval("name == \"*.txt\"", Some("hello.rs"), None, None, None, None, None);
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_basic_type_match() {
        let r = eval("type == file", None, None, Some("file"), None, None, None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_basic_size_match() {
        let r = eval("size > 100", None, None, None, None, Some(200), None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_basic_extension_match() {
        let r = eval("extension == \"rs\"", None, None, None, None, None, Some("rs"));
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    // ==================== 7.2 glob * vs ** basic behavior ====================

    #[test]
    fn test_glob_star_does_not_match_slash() {
        // path=="a/*/c" should NOT match "a/x/y/c" (single star can't cross /)
        let r = eval(
            "path == \"a/*/c\"",
            None,
            Some("a/x/y/c"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch));
    }

    #[test]
    fn test_glob_star_matches_single_component() {
        // path=="a/*/c" should match "a/x/c"
        let r = eval("path == \"a/*/c\"", None, Some("a/x/c"), Some("file"), None, None, None);
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_glob_doublestar_matches_multi_components() {
        // path=="a/**/c" should match "a/x/y/c"
        let r = eval(
            "path == \"a/**/c\"",
            None,
            Some("a/x/y/c"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_glob_doublestar_matches_zero_components() {
        // path=="a/**/c" should match "a/c"
        let r = eval("path == \"a/**/c\"", None, Some("a/c"), Some("file"), None, None, None);
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_glob_name_star_matches() {
        // name == "test_*" matches "test_hello"
        let r = eval("name == \"test_*\"", Some("test_hello"), None, None, None, None, None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_glob_name_star_no_match() {
        let r = eval("name == \"test_*\"", Some("other_hello"), None, None, None, None, None);
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_glob_name_question_mark() {
        // ? matches single character
        let r = eval("name == \"test_?\"", Some("test_a"), None, None, None, None, None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        let r2 = eval("name == \"test_?\"", Some("test_ab"), None, None, None, None, None);
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_glob_extension_star() {
        let r = eval("extension == \"t*\"", None, None, None, None, None, Some("txt"));
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        let r2 = eval("extension == \"t*\"", None, None, None, None, None, Some("rs"));
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_glob_extension_question_mark() {
        let r = eval("extension == \"r?\"", None, None, None, None, None, Some("rs"));
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        let r2 = eval("extension == \"r?\"", None, None, None, None, None, Some("rust"));
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    // ==================== 7.3 ** whitelist partial matching ====================

    // ** at beginning: path == "**/target"
    #[test]
    fn test_whitelist_doublestar_at_start_depth1_no_match() {
        // "src" (depth 1, dir) -> does not match "**/target", but could have descendants
        let (skip, cont, check) = skip(
            Some("path == \"**/target\""),
            None,
            Some("src"),
            Some("src"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_whitelist_doublestar_at_start_full_match() {
        // "src/app/target" (depth 3, dir) -> full path matches "**/target"
        let (skip, cont, check) = skip(
            Some("path == \"**/target\""),
            None,
            Some("target"),
            Some("src/app/target"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, true, false));
    }

    // ** in middle: path == "src/**/test"
    #[test]
    fn test_whitelist_doublestar_in_middle_prefix_match() {
        // "src" (depth 1, dir) -> partial match prefix
        let (skip, cont, check) = skip(
            Some("path == \"src/**/test\""),
            None,
            Some("src"),
            Some("src"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_whitelist_doublestar_in_middle_consume() {
        // "src/app" (depth 2, dir) -> ** consumes
        let (skip, cont, check) = skip(
            Some("path == \"src/**/test\""),
            None,
            Some("app"),
            Some("src/app"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_whitelist_doublestar_in_middle_deep_match() {
        // "src/app/lib/test" (depth 4, dir) -> full path matches
        let (skip, cont, check) = skip(
            Some("path == \"src/**/test\""),
            None,
            Some("test"),
            Some("src/app/lib/test"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, true, false));
    }

    #[test]
    fn test_whitelist_doublestar_in_middle_prefix_no_match() {
        // "other" (depth 1, dir) -> prefix doesn't match "src"
        // MisMatch(Other) in whitelist -> skip entry, but continue scan for dirs (is_dir=true)
        // because MisMatch(Other) doesn't definitively exclude all descendants
        let (skip, cont, check) = skip(
            Some("path == \"src/**/test\""),
            None,
            Some("other"),
            Some("other"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_whitelist_doublestar_in_middle_zero_match() {
        // "src/test" (depth 2, dir) -> ** matches zero layers
        let (skip, cont, check) = skip(
            Some("path == \"src/**/test\""),
            None,
            Some("test"),
            Some("src/test"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, true, false));
    }

    // ** at end: path == "src/**"
    #[test]
    fn test_whitelist_doublestar_at_end_prefix() {
        // "src" (depth 1, dir) -> partial match
        let (skip, cont, check) = skip(
            Some("path == \"src/**\""),
            None,
            Some("src"),
            Some("src"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_whitelist_doublestar_at_end_match() {
        // "src/anything" (depth 2, file) -> full path matches
        let (skip, cont, check) = skip(
            Some("path == \"src/**\""),
            None,
            Some("anything"),
            Some("src/anything"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, false, false));
    }

    #[test]
    fn test_whitelist_doublestar_at_end_deep_match() {
        // "src/a/b/c" (depth 4, dir) -> full path matches
        let (skip, cont, check) = skip(
            Some("path == \"src/**\""),
            None,
            Some("c"),
            Some("src/a/b/c"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, true, false));
    }

    #[test]
    fn test_whitelist_doublestar_at_end_no_match() {
        // "other" (depth 1, dir) -> doesn't match "src/**"
        // MisMatch(Other) in whitelist -> skip entry, but continue scan for dirs
        let (skip, cont, check) = skip(
            Some("path == \"src/**\""),
            None,
            Some("other"),
            Some("other"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    // ==================== 7.4 ** blacklist partial matching ====================

    // ** at beginning (exclude): exclude == "**/temp*"
    #[test]
    fn test_blacklist_doublestar_at_start_no_match() {
        // "data" (depth 1, dir) -> doesn't match temp*, should NOT be skipped by blacklist
        let (skip, cont, check) = skip(
            None,
            Some("path == \"**/temp*\""),
            Some("data"),
            Some("data"),
            Some("dir"),
            None,
            None,
            None,
        );
        // Blacklist doesn't match, no whitelist -> default behavior
        assert_eq!((skip, cont, check), (false, true, true));
    }

    #[test]
    fn test_blacklist_doublestar_at_start_nested_no_match() {
        // "data/logs" (depth 2, dir) -> doesn't match temp*
        let (skip, cont, check) = skip(
            None,
            Some("path == \"**/temp*\""),
            Some("logs"),
            Some("data/logs"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, true, true));
    }

    #[test]
    fn test_blacklist_doublestar_at_start_full_match() {
        // "data/temp_cache" (depth 2, dir) -> full path matches "**/temp*"
        let (skip, cont, check) = skip(
            None,
            Some("path == \"**/temp*\""),
            Some("temp_cache"),
            Some("data/temp_cache"),
            Some("dir"),
            None,
            None,
            None,
        );
        // PathMatch blacklist => skip, no continue, no check
        assert_eq!((skip, cont, check), (true, false, false));
    }

    #[test]
    fn test_blacklist_doublestar_at_start_root_match() {
        // "temp_dir" (depth 1, dir) -> matches "**/temp*"
        let (skip, cont, check) = skip(
            None,
            Some("path == \"**/temp*\""),
            Some("temp_dir"),
            Some("temp_dir"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, false, false));
    }

    #[test]
    fn test_blacklist_doublestar_at_start_deep_match() {
        // "a/b/c/temp_x" (depth 4, dir) -> matches "**/temp*"
        let (skip, cont, check) = skip(
            None,
            Some("path == \"**/temp*\""),
            Some("temp_x"),
            Some("a/b/c/temp_x"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, false, false));
    }

    // ** in middle (exclude): exclude == "logs/**/debug"
    #[test]
    fn test_blacklist_doublestar_in_middle_prefix() {
        // "logs" (depth 1, dir) -> prefix matches blacklist
        let (skip, cont, check) = skip(
            None,
            Some("path == \"logs/**/debug\""),
            Some("logs"),
            Some("logs"),
            Some("dir"),
            None,
            None,
            None,
        );
        // PartialMatch on blacklist -> skip entry, continue scan, check children
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_blacklist_doublestar_in_middle_consume() {
        // "logs/app" (depth 2, dir) -> truncated pattern "logs/**" matches "logs/app",
        // but pattern_after_wildcard=["debug"], file_last="app" doesn't match "debug"
        // -> MisMatch(Other) -> blacklist doesn't match -> flows to default (no whitelist)
        let (skip, cont, check) = skip(
            None,
            Some("path == \"logs/**/debug\""),
            Some("app"),
            Some("logs/app"),
            Some("dir"),
            None,
            None,
            None,
        );
        // Not matched by blacklist -> default: no skip, continue scan
        assert_eq!((skip, cont, check), (false, true, true));
    }

    #[test]
    fn test_blacklist_doublestar_in_middle_full_match() {
        // "logs/app/debug" (depth 3, dir) -> full path matches
        let (skip, cont, check) = skip(
            None,
            Some("path == \"logs/**/debug\""),
            Some("debug"),
            Some("logs/app/debug"),
            Some("dir"),
            None,
            None,
            None,
        );
        // Full match blacklist with PathMatch -> skip, no continue
        assert_eq!((skip, cont, check), (true, false, false));
    }

    #[test]
    fn test_blacklist_doublestar_in_middle_prefix_no_match() {
        // "src" (depth 1, dir) -> prefix doesn't match "logs"
        let (skip, cont, check) = skip(
            None,
            Some("path == \"logs/**/debug\""),
            Some("src"),
            Some("src"),
            Some("dir"),
            None,
            None,
            None,
        );
        // Not matched by blacklist -> default
        assert_eq!((skip, cont, check), (false, true, true));
    }

    #[test]
    fn test_blacklist_doublestar_in_middle_not_match_suffix() {
        // "logs/app/info" (depth 3, dir) -> doesn't match "debug"
        let (skip, cont, check) = skip(
            None,
            Some("path == \"logs/**/debug\""),
            Some("info"),
            Some("logs/app/info"),
            Some("dir"),
            None,
            None,
            None,
        );
        // Doesn't fully match -> not skipped by blacklist
        assert_eq!((skip, cont, check), (false, true, true));
    }

    // ** at end (exclude): exclude == "tmp/**"
    #[test]
    fn test_blacklist_doublestar_at_end_prefix() {
        // "tmp" (depth 1, dir) -> partial match
        let (skip, cont, check) = skip(
            None,
            Some("path == \"tmp/**\""),
            Some("tmp"),
            Some("tmp"),
            Some("dir"),
            None,
            None,
            None,
        );
        // PartialMatch blacklist -> skip entry, continue scan
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_blacklist_doublestar_at_end_full_match() {
        // "tmp/anything" (depth 2, dir) -> full path matches
        let (skip, cont, check) = skip(
            None,
            Some("path == \"tmp/**\""),
            Some("anything"),
            Some("tmp/anything"),
            Some("dir"),
            None,
            None,
            None,
        );
        // PathMatch blacklist -> skip, no continue
        assert_eq!((skip, cont, check), (true, false, false));
    }

    #[test]
    fn test_blacklist_doublestar_at_end_deep_match() {
        // "tmp/a/b/c" (depth 4, dir) -> full path matches
        let (skip, cont, check) = skip(
            None,
            Some("path == \"tmp/**\""),
            Some("c"),
            Some("tmp/a/b/c"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, false, false));
    }

    #[test]
    fn test_blacklist_doublestar_at_end_no_match() {
        // "src" (depth 1, dir) -> doesn't match "tmp/**"
        let (skip, cont, check) = skip(
            None,
            Some("path == \"tmp/**\""),
            Some("src"),
            Some("src"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, true, true));
    }

    // ==================== 7.5 Whitelist + Blacklist combination ====================

    #[test]
    fn test_blacklist_priority_over_whitelist() {
        // match: path == "src/**", exclude: name == "*.tmp"
        // File "src/data.tmp" matches whitelist but also blacklist -> blacklist wins
        let (skip, _cont, _check) = skip(
            Some("path == \"src/**\""),
            Some("name == \"*.tmp\""),
            Some("data.tmp"),
            Some("src/data.tmp"),
            Some("file"),
            None,
            None,
            None,
        );
        assert!(skip, "Blacklist should take priority, entry should be skipped");
    }

    #[test]
    fn test_whitelist_match_blacklist_no_match() {
        // match: path == "src/**", exclude: name == "*.tmp"
        // File "src/data.rs" matches whitelist, not blacklist -> keep
        let (skip, cont, check) = skip(
            Some("path == \"src/**\""),
            Some("name == \"*.tmp\""),
            Some("data.rs"),
            Some("src/data.rs"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, false, false));
    }

    // ==================== 7.6 Modified absolute time tests ====================

    // Parsing tests
    #[test]
    fn test_parse_modified_iso_date() {
        let expr = FilterExpression::parse("modified > \"2025-01-15\"");
        assert!(expr.is_ok(), "Should parse ISO 8601 date");
    }

    #[test]
    fn test_parse_modified_compact_date() {
        let expr = FilterExpression::parse("modified > 20250115");
        assert!(expr.is_ok(), "Should parse compact date YYYYMMDD");
    }

    #[test]
    fn test_parse_modified_relative_days_with_suffix() {
        let expr = FilterExpression::parse("modified < 3d");
        assert!(expr.is_ok(), "Should parse relative days with d suffix");
    }

    #[test]
    fn test_parse_modified_relative_days_numeric() {
        let expr = FilterExpression::parse("modified < 30");
        assert!(expr.is_ok(), "Should parse relative days as plain number (<30 days)");
    }

    #[test]
    fn test_parse_modified_relative_days_fractional() {
        let expr = FilterExpression::parse("modified < 0.5");
        assert!(expr.is_ok(), "Should parse fractional relative days");
    }

    #[test]
    fn test_parse_modified_eq_relative_days_error() {
        let expr = FilterExpression::parse("modified == 3d");
        assert!(expr.is_err(), "Relative days should not support == operator");
    }

    #[test]
    fn test_parse_modified_eq_plain_number_relative_error() {
        // modified == 30 -> 30 < 10000000, so it's relative days, and == is invalid
        let expr = FilterExpression::parse("modified == 30");
        assert!(
            expr.is_err(),
            "Relative days (plain number <10000000) should not support =="
        );
    }

    // Absolute time == day granularity tests
    #[test]
    fn test_modified_eq_absolute_same_day_start() {
        // modified == "2025-01-15", file mtime = 2025-01-15 00:00:00 -> match
        let target_epoch = date_to_epoch(2025, 1, 15, 0, 0, 0);
        let file_epoch = date_to_epoch(2025, 1, 15, 0, 0, 0);
        let r = eval_with_now(
            "modified == \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            target_epoch + 86400, // now doesn't matter for absolute ==
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_eq_absolute_same_day_end() {
        // modified == "2025-01-15", file mtime = 2025-01-15 23:59:59 -> match (same day)
        let file_epoch = date_to_epoch(2025, 1, 15, 23, 59, 59);
        let r = eval_with_now(
            "modified == \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_eq_absolute_next_day() {
        // modified == "2025-01-15", file mtime = 2025-01-16 00:00:00 -> no match
        let file_epoch = date_to_epoch(2025, 1, 16, 0, 0, 0);
        let r = eval_with_now(
            "modified == \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_modified_eq_absolute_prev_day() {
        // modified == "2025-01-15", file mtime = 2025-01-14 23:59:59 -> no match
        let file_epoch = date_to_epoch(2025, 1, 14, 23, 59, 59);
        let r = eval_with_now(
            "modified == \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400 * 2,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_modified_eq_compact_format() {
        // modified == 20250115, same behavior as quoted
        let file_epoch = date_to_epoch(2025, 1, 15, 12, 0, 0);
        let r = eval_with_now(
            "modified == 20250115",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    // Absolute time comparison operators
    #[test]
    fn test_modified_gt_absolute() {
        // modified > "2025-01-15", file mtime = 2025-01-16 -> match
        let file_epoch = date_to_epoch(2025, 1, 16, 0, 0, 0);
        let r = eval_with_now(
            "modified > \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_gt_absolute_same_day() {
        // modified > "2025-01-15", file mtime = 2025-01-15 00:00:00 -> no match (not strictly greater)
        let target_epoch = date_to_epoch(2025, 1, 15, 0, 0, 0);
        let file_epoch = date_to_epoch(2025, 1, 15, 0, 0, 0);
        let r = eval_with_now(
            "modified > \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            target_epoch + 86400 * 10,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_modified_lt_absolute() {
        // modified < "2025-01-15", file mtime = 2025-01-14 -> match
        let file_epoch = date_to_epoch(2025, 1, 14, 0, 0, 0);
        let r = eval_with_now(
            "modified < \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400 * 10,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_ge_absolute() {
        // modified >= "2025-01-15", file mtime = 2025-01-15 00:00:00 -> match
        let file_epoch = date_to_epoch(2025, 1, 15, 0, 0, 0);
        let r = eval_with_now(
            "modified >= \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400 * 10,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_le_absolute() {
        // modified <= "2025-01-15", file mtime = 2025-01-15 23:59:59 -> match
        let file_epoch = date_to_epoch(2025, 1, 15, 23, 59, 59);
        let target_epoch = date_to_epoch(2025, 1, 15, 0, 0, 0);
        let r = eval_with_now(
            "modified <= \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            target_epoch + 86400 * 10,
        );
        // file_epoch (Jan 15 23:59:59) <= target_epoch (Jan 15 00:00:00)? No, file_epoch > target_epoch
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_modified_le_absolute_before() {
        // modified <= "2025-01-15", file mtime = 2025-01-14 -> match
        let file_epoch = date_to_epoch(2025, 1, 14, 23, 59, 59);
        let r = eval_with_now(
            "modified <= \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400 * 10,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    // Modified combination tests
    #[test]
    fn test_modified_absolute_and_size() {
        let file_epoch = date_to_epoch(2025, 1, 16, 0, 0, 0);
        let r = eval_with_now(
            "modified > \"2025-01-15\" and size > 1000",
            None,
            None,
            None,
            Some(file_epoch),
            Some(2000),
            None,
            file_epoch + 86400,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_absolute_time_range() {
        // modified < "2025-03-01" and modified > "2025-01-01"
        let file_epoch = date_to_epoch(2025, 2, 15, 0, 0, 0);
        let r = eval_with_now(
            "modified < \"2025-03-01\" and modified > \"2025-01-01\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400 * 30,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_absolute_time_range_outside() {
        // modified < "2025-03-01" and modified > "2025-01-01" but file is from 2024
        let file_epoch = date_to_epoch(2024, 6, 15, 0, 0, 0);
        let r = eval_with_now(
            "modified < \"2025-03-01\" and modified > \"2025-01-01\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400 * 365,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    // Relative days tests (ensure backward compatibility)
    #[test]
    fn test_modified_relative_days_less_than() {
        // modified < 3d, file modified 1 day ago -> match
        let now = 1000000;
        let file_epoch = now - 86400; // 1 day ago
        let r = eval_with_now("modified < 3d", None, None, None, Some(file_epoch), None, None, now);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_relative_days_greater_than() {
        // modified > 3d, file modified 5 days ago -> match (file_days=5 > 3)
        let now = 1000000;
        let file_epoch = now - 86400 * 5;
        let r = eval_with_now("modified > 3d", None, None, None, Some(file_epoch), None, None, now);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_relative_days_no_match() {
        // modified < 3d, file modified 5 days ago -> no match (file_days=5 >= 3)
        let now = 1000000;
        let file_epoch = now - 86400 * 5;
        let r = eval_with_now("modified < 3d", None, None, None, Some(file_epoch), None, None, now);
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    // ==================== 7.7 Short-circuit evaluation ====================

    #[test]
    fn test_and_short_circuit_left_mismatch() {
        // name == "*.txt" and size > 100
        // name doesn't match -> And short-circuits, result is MisMatch
        let r = eval(
            "name == \"*.txt\" and size > 100",
            Some("hello.rs"),
            None,
            None,
            None,
            Some(200),
            None,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_or_short_circuit_left_match() {
        // name == "*.txt" or size > 100
        // name matches -> Or short-circuits, result is Match(NonPathMatch)
        let r = eval(
            "name == \"*.txt\" or size > 100",
            Some("hello.txt"),
            None,
            None,
            None,
            Some(50),
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_and_short_circuit_left_not_support() {
        // name == "*.txt" and modified < 3d  (no modified_epoch provided -> LazyMatch)
        let now = 1000000i64;
        let r = eval_with_now(
            "name == \"*.txt\" and modified < 3d",
            Some("hello.txt"),
            None,
            None,
            None, // no modified_epoch -> LazyMatch
            None,
            None,
            now,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    // ==================== 7.8 CompareOp / precompiled Pattern ====================

    #[test]
    fn test_compare_op_parsing() {
        assert_eq!(Lexer::parse_operator("=="), Some(CompareOp::Eq));
        assert_eq!(Lexer::parse_operator("!="), Some(CompareOp::Ne));
        assert_eq!(Lexer::parse_operator("<"), Some(CompareOp::Lt));
        assert_eq!(Lexer::parse_operator(">"), Some(CompareOp::Gt));
        assert_eq!(Lexer::parse_operator("<="), Some(CompareOp::Le));
        assert_eq!(Lexer::parse_operator(">="), Some(CompareOp::Ge));
        assert_eq!(Lexer::parse_operator("~"), None);
    }

    #[test]
    fn test_compare_op_size_all_operators() {
        // Eq
        let r = eval("size == 100", None, None, None, None, Some(100), None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        // Lt
        let r = eval("size < 100", None, None, None, None, Some(50), None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        // Gt
        let r = eval("size > 100", None, None, None, None, Some(200), None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        // Le
        let r = eval("size <= 100", None, None, None, None, Some(100), None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        // Ge
        let r = eval("size >= 100", None, None, None, None, Some(100), None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        // Ne (size doesn't support != in current implementation, returns MisMatch)
        let r = eval("size != 100", None, None, None, None, Some(200), None);
        // Size only supports ==, <, >, <=, >= — != falls through to false
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_precompiled_pattern_name() {
        // Verify precompiled pattern works correctly
        let r1 = eval("name == \"hello_*\"", Some("hello_world"), None, None, None, None, None);
        assert_eq!(r1, MatchResult::Match(MatchAddon::NonPathMatch));

        let r2 = eval("name == \"hello_*\"", Some("goodbye"), None, None, None, None, None);
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_precompiled_pattern_extension() {
        let r1 = eval("extension == \"t?t\"", None, None, None, None, None, Some("txt"));
        assert_eq!(r1, MatchResult::Match(MatchAddon::NonPathMatch));

        let r2 = eval("extension == \"t?t\"", None, None, None, None, None, Some("ts"));
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_precompiled_pattern_path() {
        let r = eval(
            "path == \"src/main.rs\"",
            None,
            Some("src/main.rs"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_name_ne_operator() {
        let r = eval("name != \"*.tmp\"", Some("hello.rs"), None, None, None, None, None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        let r2 = eval("name != \"*.tmp\"", Some("data.tmp"), None, None, None, None, None);
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    // ==================== 7.9 Compound filter conditions ====================

    // Path + file attribute combinations
    #[test]
    fn test_compound_path_and_type_file() {
        // path == "src/**" and type == file
        // directory "src" -> path partial match, type = dir != file -> And(PartialMatch, MisMatch) -> MisMatch
        // But short-circuit: PartialMatch is not MisMatch, so right side is evaluated
        let r = eval(
            "path == \"src/**\" and type == file",
            Some("src"),
            Some("src"),
            Some("dir"),
            None,
            None,
            None,
        );
        // PartialMatch & MisMatch(Other) => MisMatch(Other)
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_compound_path_and_name_dir() {
        // path == "src/**" and name == "*.rs"
        // directory "src/app" -> path partial match, name "app" != "*.rs" -> MisMatch
        let r = eval(
            "path == \"src/**\" and name == \"*.rs\"",
            Some("app"),
            Some("src/app"),
            Some("dir"),
            None,
            None,
            None,
        );
        // PartialMatch & MisMatch(Other) => MisMatch(Other)
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_compound_path_and_name_file_match() {
        // path == "src/**" and name == "*.rs"
        // file "src/main.rs" -> path matches, name matches
        let r = eval(
            "path == \"src/**\" and name == \"*.rs\"",
            Some("main.rs"),
            Some("src/main.rs"),
            Some("file"),
            None,
            None,
            None,
        );
        // Match(PathMatch) & Match(NonPathMatch) => Match(MixMatch)
        assert_eq!(r, MatchResult::Match(MatchAddon::MixMatch));
    }

    #[test]
    fn test_compound_path_and_size_dir() {
        // path == "src/**" and size > 1000
        // directory "src" -> path partial match, size not provided -> PartialMatch
        let r = eval(
            "path == \"src/**\" and size > 1000",
            Some("src"),
            Some("src"),
            Some("dir"),
            None,
            None,
            None,
        );
        // PartialMatch & LazyMatch => PartialMatch
        assert_eq!(r, MatchResult::PartialMatch);
    }

    // Or combination paths
    #[test]
    fn test_compound_or_paths_first_match() {
        // path == "src/**" or path == "lib/**"
        // dir "src" -> partial match on first
        let r = eval(
            "path == \"src/**\" or path == \"lib/**\"",
            Some("src"),
            Some("src"),
            Some("dir"),
            None,
            None,
            None,
        );
        // PartialMatch | ... => at least PartialMatch
        assert_eq!(r, MatchResult::PartialMatch);
    }

    #[test]
    fn test_compound_or_paths_second_match() {
        // path == "src/**" or path == "lib/**"
        // dir "lib" -> no match first, partial match second
        let r = eval(
            "path == \"src/**\" or path == \"lib/**\"",
            Some("lib"),
            Some("lib"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::PartialMatch);
    }

    #[test]
    fn test_compound_or_paths_neither() {
        // path == "src/**" or path == "lib/**"
        // dir "other" -> no match either
        let r = eval(
            "path == \"src/**\" or path == \"lib/**\"",
            Some("other"),
            Some("other"),
            Some("dir"),
            None,
            None,
            None,
        );
        // MisMatch | MisMatch => MisMatch
        assert!(matches!(r, MatchResult::MisMatch(_)));
    }

    #[test]
    fn test_compound_or_path_and_name() {
        // path == "**/test" or name == "*.log"
        // dir "src" -> path PartialMatch (** at start), name MisMatch
        let r = eval(
            "path == \"**/test\" or name == \"*.log\"",
            Some("src"),
            Some("src"),
            Some("dir"),
            None,
            None,
            None,
        );
        // MisMatch(Other) | MisMatch(Other) => MisMatch; but ** at start -> needs partial check
        // The path "src" doesn't match "**/test" fully; it's a dir, partial match check:
        // truncated pattern for depth 1 = "**", matches "src" -> but pattern_after_wildcard = ["test"],
        // file_last = "src", "src" doesn't match "test" -> MisMatch(Other)
        // So: MisMatch(Other) | MisMatch(Other) => MisMatch(Other)
        assert!(matches!(r, MatchResult::MisMatch(_)));
    }

    // And + Or + parentheses
    #[test]
    fn test_compound_parenthesized_or_and_type() {
        // (path == "a/**" or path == "b/**") and type == file
        // dir "a" -> partial match path, type dir != file -> MisMatch
        let r = eval(
            "(path == \"a/**\" or path == \"b/**\") and type == file",
            Some("a"),
            Some("a"),
            Some("dir"),
            None,
            None,
            None,
        );
        // PartialMatch & MisMatch(Other) => MisMatch(Other)
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_compound_parenthesized_or_and_type_file_match() {
        // (path == "a/**" or path == "b/**") and type == file
        // file "a/x" -> path matches, type file -> Match
        let r = eval(
            "(path == \"a/**\" or path == \"b/**\") and type == file",
            Some("x"),
            Some("a/x"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::MixMatch));
    }

    #[test]
    fn test_compound_path_and_or_names() {
        // path == "src/**" and (name == "*.rs" or name == "*.toml")
        // file "src/Cargo.toml" -> path match, name match (second or)
        let r = eval(
            "path == \"src/**\" and (name == \"*.rs\" or name == \"*.toml\")",
            Some("Cargo.toml"),
            Some("src/Cargo.toml"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::MixMatch));
    }

    #[test]
    fn test_compound_path_and_or_names_no_name_match() {
        // path == "src/**" and (name == "*.rs" or name == "*.toml")
        // file "src/data.txt" -> path match, neither name matches
        let r = eval(
            "path == \"src/**\" and (name == \"*.rs\" or name == \"*.toml\")",
            Some("data.txt"),
            Some("src/data.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_compound_type_or_and_size_no_match() {
        // type == dir or type == file and size >= 15
        // type == file and size >= 15 -> nonpath match
        let r = eval(
            "type == \"dir\" or type == \"file\" and size >= 15",
            Some("data.txt"),
            Some("src/data.txt"),
            Some("file"),
            None,
            Some(15),
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    // Blacklist + whitelist + compound conditions
    #[test]
    fn test_compound_whitelist_blacklist_dir() {
        // match: path == "data/**" and type == file
        // exclude: name == "*.tmp" or name == "*.log"
        // dir "data" -> whitelist partial match (path) & MisMatch (type!=file) -> MisMatch
        // blacklist: name "data" != "*.tmp" nor "*.log" -> MisMatch -> flows to whitelist
        let (skip, cont, check) = skip(
            Some("path == \"data/**\" and type == file"),
            Some("name == \"*.tmp\" or name == \"*.log\""),
            Some("data"),
            Some("data"),
            Some("dir"),
            None,
            None,
            None,
        );
        // Blacklist doesn't match, whitelist evaluates:
        // PartialMatch & MisMatch -> MisMatch(Other) -> should_skip handles MisMatch(Other) for whitelist
        assert_eq!(skip, true);
        assert_eq!(cont, true); // is_dir
        assert_eq!(check, true);
    }

    #[test]
    fn test_compound_whitelist_blacklist_file_blacklisted() {
        // match: path == "data/**" and type == file
        // exclude: name == "*.tmp" or name == "*.log"
        // file "data/report.tmp" -> blacklist matches "*.tmp" -> skip
        let (skip, cont, _check) = skip(
            Some("path == \"data/**\" and type == file"),
            Some("name == \"*.tmp\" or name == \"*.log\""),
            Some("report.tmp"),
            Some("data/report.tmp"),
            Some("file"),
            None,
            None,
            None,
        );
        assert!(skip, "File matching blacklist should be skipped");
        assert!(!cont, "File should not continue scan (not a dir)");
    }

    #[test]
    fn test_compound_whitelist_blacklist_file_kept() {
        // match: path == "data/**" and type == file
        // exclude: name == "*.tmp" or name == "*.log"
        // file "data/report.csv" -> blacklist doesn't match, whitelist matches -> keep
        let (skip, cont, check) = skip(
            Some("path == \"data/**\" and type == file"),
            Some("name == \"*.tmp\" or name == \"*.log\""),
            Some("report.csv"),
            Some("data/report.csv"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, false, true));
    }

    // MatchAddon merge verification
    #[test]
    fn test_match_addon_and_path_nonpath_mix() {
        // path == "a/b" and name == "*.rs"
        // file at path "a/b" with name "test.rs" -> Match(PathMatch) & Match(NonPathMatch) -> Match(MixMatch)
        let r = eval(
            "path == \"a/b\" and name == \"*.rs\"",
            Some("test.rs"),
            Some("a/b"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::MixMatch));
    }

    #[test]
    fn test_match_addon_or_path_nonpath_mix() {
        // path == "a/b" or name == "*.rs"
        // file at path "a/b" with name "test.rs" -> both match
        // Or short-circuits: left is Match(PathMatch) -> returns Match(PathMatch)
        let r = eval(
            "path == \"a/b\" or name == \"*.rs\"",
            Some("test.rs"),
            Some("a/b"),
            Some("file"),
            None,
            None,
            None,
        );
        // Due to Or short-circuit, left Match(PathMatch) is returned directly
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_match_addon_or_right_only() {
        // path == "x/y" or name == "*.rs"
        // file at path "a/b" with name "test.rs"
        // path doesn't match (MisMatch), name matches (Match(NonPathMatch))
        let r = eval(
            "path == \"x/y\" or name == \"*.rs\"",
            Some("test.rs"),
            Some("a/b"),
            Some("file"),
            None,
            None,
            None,
        );
        // MisMatch | Match(NonPathMatch) => Match(NonPathMatch)
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    // ==================== MatchResult BitAnd / BitOr tests ====================

    #[test]
    fn test_match_result_and_combinations() {
        // Match & Match
        assert_eq!(
            MatchResult::Match(MatchAddon::PathMatch) & MatchResult::Match(MatchAddon::PathMatch),
            MatchResult::Match(MatchAddon::PathMatch)
        );
        assert_eq!(
            MatchResult::Match(MatchAddon::PathMatch) & MatchResult::Match(MatchAddon::NonPathMatch),
            MatchResult::Match(MatchAddon::MixMatch)
        );
        assert_eq!(
            MatchResult::Match(MatchAddon::NonPathMatch) & MatchResult::Match(MatchAddon::NonPathMatch),
            MatchResult::Match(MatchAddon::NonPathMatch)
        );

        // Match & PartialMatch
        assert_eq!(
            MatchResult::Match(MatchAddon::PathMatch) & MatchResult::PartialMatch,
            MatchResult::PartialMatch
        );

        // Match & MisMatch
        assert_eq!(
            MatchResult::Match(MatchAddon::PathMatch) & MatchResult::MisMatch(MisMatchAddon::Other),
            MatchResult::MisMatch(MisMatchAddon::Other)
        );

        // Match & LazyMatch
        assert_eq!(
            MatchResult::Match(MatchAddon::PathMatch) & MatchResult::LazyMatch,
            MatchResult::Match(MatchAddon::PathMatch)
        );

        // PartialMatch & PartialMatch
        assert_eq!(
            MatchResult::PartialMatch & MatchResult::PartialMatch,
            MatchResult::PartialMatch
        );

        // MisMatch & MisMatch (FullPathNotMatch priority)
        assert_eq!(
            MatchResult::MisMatch(MisMatchAddon::Other) & MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch),
            MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch)
        );
    }

    #[test]
    fn test_match_result_or_combinations() {
        // Match | Match
        assert_eq!(
            MatchResult::Match(MatchAddon::PathMatch) | MatchResult::Match(MatchAddon::NonPathMatch),
            MatchResult::Match(MatchAddon::MixMatch)
        );

        // Match | MisMatch
        assert_eq!(
            MatchResult::Match(MatchAddon::PathMatch) | MatchResult::MisMatch(MisMatchAddon::Other),
            MatchResult::Match(MatchAddon::PathMatch)
        );

        // PartialMatch | MisMatch
        assert_eq!(
            MatchResult::PartialMatch | MatchResult::MisMatch(MisMatchAddon::Other),
            MatchResult::PartialMatch
        );

        // MisMatch | MisMatch
        assert_eq!(
            MatchResult::MisMatch(MisMatchAddon::Other) | MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch),
            MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch)
        );

        // LazyMatch | LazyMatch
        assert_eq!(MatchResult::LazyMatch | MatchResult::LazyMatch, MatchResult::LazyMatch);

        // MisMatch | LazyMatch
        assert_eq!(
            MatchResult::MisMatch(MisMatchAddon::Other) | MatchResult::LazyMatch,
            MatchResult::MisMatch(MisMatchAddon::Other)
        );
    }

    // ==================== date_to_epoch utility tests ====================

    #[test]
    fn test_date_to_epoch_unix_epoch() {
        // 1970-01-01 00:00:00 UTC = 0
        assert_eq!(date_to_epoch(1970, 1, 1, 0, 0, 0), 0);
    }

    #[test]
    fn test_date_to_epoch_known_date() {
        // 2025-01-15 00:00:00 UTC
        let epoch = date_to_epoch(2025, 1, 15, 0, 0, 0);
        // Verify it's a reasonable value (~55 years * 365.25 * 86400 ≈ 1736899200)
        assert!(epoch > 1700000000, "Epoch should be after ~2023");
        assert!(epoch < 1800000000, "Epoch should be before ~2027");
    }

    #[test]
    fn test_date_to_epoch_with_time() {
        let base = date_to_epoch(2025, 1, 15, 0, 0, 0);
        let with_time = date_to_epoch(2025, 1, 15, 12, 30, 45);
        assert_eq!(with_time - base, 12 * 3600 + 30 * 60 + 45);
    }

    #[test]
    fn test_parse_date_to_epoch_iso() {
        let epoch = parse_date_to_epoch("2025-01-15").unwrap();
        assert_eq!(epoch, date_to_epoch(2025, 1, 15, 0, 0, 0));
    }

    #[test]
    fn test_parse_date_to_epoch_iso_with_time() {
        let epoch = parse_date_to_epoch("2025-01-15T08:30:00").unwrap();
        assert_eq!(epoch, date_to_epoch(2025, 1, 15, 8, 30, 0));
    }

    #[test]
    fn test_parse_compact_date_to_epoch() {
        let epoch = parse_compact_date_to_epoch("20250115").unwrap();
        assert_eq!(epoch, date_to_epoch(2025, 1, 15, 0, 0, 0));
    }

    // ==================== Edge cases ====================

    #[test]
    fn test_no_expressions_default() {
        let (skip, cont, check) = skip(
            None,
            None,
            Some("file.txt"),
            Some("a/file.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, false, true));
    }

    #[test]
    fn test_no_expressions_dir_default() {
        let (skip, cont, check) = skip(None, None, Some("dir"), Some("a/dir"), Some("dir"), None, None, None);
        assert_eq!((skip, cont, check), (false, true, true));
    }

    #[test]
    fn test_path_trailing_slash_trimmed() {
        // Paths with trailing slashes should be handled
        let r = eval(
            "path == \"src/main.rs\"",
            None,
            Some("src/main.rs/"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_not_support_when_field_missing() {
        // size condition with no size provided -> LazyMatch
        let r = eval("size > 100", None, None, None, None, None, None);
        assert_eq!(r, MatchResult::LazyMatch);

        // modified condition with no modified_epoch -> LazyMatch
        let now = 1000000i64;
        let r = eval_with_now("modified < 3d", None, None, None, None, None, None, now);
        assert_eq!(r, MatchResult::LazyMatch);
    }

    #[test]
    fn test_invalid_filter_expression() {
        let r = FilterExpression::parse("invalid_field == \"test\"");
        // Should fail because "invalid_field" is not a recognized field
        // Actually the parser skips unknown fields and may produce UnexpectedEndOfToken
        assert!(r.is_err());
    }

    #[test]
    fn test_type_ne_operator() {
        let r = eval("type != file", None, None, Some("dir"), None, None, None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        let r2 = eval("type != file", None, None, Some("file"), None, None, None);
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_path_ne_operator() {
        let r = eval(
            "path != \"src/**\"",
            None,
            Some("lib/test.rs"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        let r2 = eval(
            "path != \"src/**\"",
            None,
            Some("src/test.rs"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch));
    }

    // ==================== should_skip integration with whitelist PartialMatch ====================

    #[test]
    fn test_should_skip_whitelist_partial_match_for_dir() {
        // Whitelist: path == "a/b/c", dir "a" -> partial match -> skip entry, continue scan
        let (skip, cont, check) = skip(
            Some("path == \"a/b/c\""),
            None,
            Some("a"),
            Some("a"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_should_skip_whitelist_full_path_not_match() {
        // Whitelist: path == "a/b", file "x/y" -> FullPathNotMatch -> skip, no continue
        let (skip, cont, check) = skip(
            Some("path == \"a/b\""),
            None,
            Some("y"),
            Some("x/y"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, false, false));
    }

    #[test]
    fn test_should_skip_whitelist_match_path_only() {
        // Whitelist: path == "src/**", file "src/test.rs" -> Match(PathMatch) -> no skip, check_children=false
        let (skip, cont, check) = skip(
            Some("path == \"src/**\""),
            None,
            Some("test.rs"),
            Some("src/test.rs"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, false, false));
    }

    #[test]
    fn test_should_skip_whitelist_match_non_path() {
        // Whitelist: name == "*.rs", file "test.rs" -> Match(NonPathMatch) -> no skip, check_children=true
        let (skip, cont, check) = skip(
            Some("name == \"*.rs\""),
            None,
            Some("test.rs"),
            None,
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, false, true));
    }

    // ==================== 7.3 Additional ** whitelist partial matching ====================

    #[test]
    fn test_whitelist_doublestar_at_start_depth2_no_match() {
        // "src/app" (depth 2, dir) -> doesn't match "**/target", but could have descendants
        let (skip, cont, check) = skip(
            Some("path == \"**/target\""),
            None,
            Some("app"),
            Some("src/app"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_whitelist_doublestar_at_start_depth3_other() {
        // "src/app/other" (depth 3, dir) -> doesn't match "**/target", but ** means descendants could still match
        let (skip, cont, check) = skip(
            Some("path == \"**/target\""),
            None,
            Some("other"),
            Some("src/app/other"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_whitelist_doublestar_in_middle_depth3_consume() {
        // "src/app/lib" (depth 3, dir) for pattern "src/**/test" -> ** continues consuming
        let (skip, cont, check) = skip(
            Some("path == \"src/**/test\""),
            None,
            Some("lib"),
            Some("src/app/lib"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    // ==================== 7.6 Additional Modified tests ====================

    #[test]
    fn test_parse_modified_iso_datetime() {
        // ISO 8601 date with time component
        let expr = FilterExpression::parse("modified > \"2025-01-15T08:30:00\"");
        assert!(expr.is_ok(), "Should parse ISO 8601 datetime");
    }

    #[test]
    fn test_modified_absolute_datetime_evaluation() {
        // modified > "2025-01-15T08:30:00", file mtime = 2025-01-15 09:00:00 -> match
        let target_epoch = date_to_epoch(2025, 1, 15, 8, 30, 0);
        let file_epoch = date_to_epoch(2025, 1, 15, 9, 0, 0);
        let r = eval_with_now(
            "modified > \"2025-01-15T08:30:00\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            target_epoch + 86400,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_absolute_datetime_no_match() {
        // modified > "2025-01-15T08:30:00", file mtime = 2025-01-15 08:00:00 -> no match
        let target_epoch = date_to_epoch(2025, 1, 15, 8, 30, 0);
        let file_epoch = date_to_epoch(2025, 1, 15, 8, 0, 0);
        let r = eval_with_now(
            "modified > \"2025-01-15T08:30:00\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            target_epoch + 86400,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_modified_ne_absolute() {
        // modified != "2025-01-15", file mtime on a different day -> match
        let file_epoch = date_to_epoch(2025, 1, 16, 12, 0, 0);
        let r = eval_with_now(
            "modified != \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_ne_absolute_same_day() {
        // modified != "2025-01-15", file mtime on the same day -> no match
        let file_epoch = date_to_epoch(2025, 1, 15, 12, 0, 0);
        let r = eval_with_now(
            "modified != \"2025-01-15\"",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_modified_relative_le() {
        // modified <= 3d, file modified 3 days ago -> file_days=3.0, 3.0 <= 3.0 -> match
        let now = 1000000i64;
        let file_epoch = now - 86400 * 3;
        let r = eval_with_now("modified <= 3d", None, None, None, Some(file_epoch), None, None, now);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_modified_relative_ge() {
        // modified >= 3d, file modified 5 days ago -> file_days=5.0, 5.0 >= 3.0 -> match
        let now = 1000000i64;
        let file_epoch = now - 86400 * 5;
        let r = eval_with_now("modified >= 3d", None, None, None, Some(file_epoch), None, None, now);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    // ==================== 7.7 Additional short-circuit tests ====================

    #[test]
    fn test_and_short_circuit_both_match() {
        // When both sides match, And returns combined Match
        let r = eval(
            "name == \"*.txt\" and size > 100",
            Some("hello.txt"),
            None,
            None,
            None,
            Some(200),
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_or_short_circuit_left_mismatch_right_match() {
        // Or: left doesn't match, right matches -> evaluates both
        let r = eval(
            "name == \"*.txt\" or size > 100",
            Some("hello.rs"),
            None,
            None,
            None,
            Some(200),
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_or_short_circuit_both_mismatch() {
        // Or: neither matches -> MisMatch
        let r = eval(
            "name == \"*.txt\" or size > 100",
            Some("hello.rs"),
            None,
            None,
            None,
            Some(50),
            None,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    // ==================== 7.8 Additional CompareOp tests ====================

    #[test]
    fn test_compare_op_type_eq_ne() {
        let r1 = eval("type == dir", None, None, Some("dir"), None, None, None);
        assert_eq!(r1, MatchResult::Match(MatchAddon::NonPathMatch));

        let r2 = eval("type == dir", None, None, Some("file"), None, None, None);
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));

        let r3 = eval("type != dir", None, None, Some("file"), None, None, None);
        assert_eq!(r3, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_extension_ne_operator() {
        let r = eval("extension != \"tmp\"", None, None, None, None, None, Some("rs"));
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        let r2 = eval("extension != \"tmp\"", None, None, None, None, None, Some("tmp"));
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_size_boundary_values() {
        // Exact boundary: size == 0
        let r = eval("size == 0", None, None, None, None, Some(0), None);
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));

        // size < 0 (no file can have size less than 0)
        let r2 = eval("size > 0", None, None, None, None, Some(0), None);
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));

        // Large size
        let r3 = eval("size >= 1000000000", None, None, None, None, Some(1_000_000_000), None);
        assert_eq!(r3, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    // ==================== 7.9 Additional compound filter conditions ====================

    #[test]
    fn test_compound_path_and_modified() {
        // path == "**/test" and modified < 3d
        // Both conditions need file-level verification
        let now = 1000000i64;
        let file_epoch = now - 86400; // 1 day ago
        let r = eval_with_now(
            "path == \"**/test\" and modified < 3d",
            Some("test"),
            Some("src/test"),
            Some("file"),
            Some(file_epoch),
            None,
            None,
            now,
        );
        // path matches (PathMatch), modified matches (NonPathMatch) -> MixMatch
        assert_eq!(r, MatchResult::Match(MatchAddon::MixMatch));
    }

    #[test]
    fn test_compound_path_and_modified_dir_partial() {
        // path == "**/test" and modified < 3d
        // dir "src" -> path partial, modified not relevant for dir yet
        let now = 1000000i64;
        let r = eval_with_now(
            "path == \"**/test\" and modified < 3d",
            Some("src"),
            Some("src"),
            Some("dir"),
            None, // no modified for directory
            None,
            None,
            now,
        );
        // path: MisMatch(Other) for dir "src" with pattern "**/test" (** + suffix check fails)
        // short-circuit on MisMatch -> return MisMatch
        assert!(matches!(r, MatchResult::MisMatch(_) | MatchResult::LazyMatch));
    }

    #[test]
    fn test_compound_path_and_or_names_dir_partial() {
        // path == "src/**" and (name == "*.rs" or name == "*.toml")
        // dir "src" -> path partial, name MisMatch -> overall MisMatch (continue scanning)
        let r = eval(
            "path == \"src/**\" and (name == \"*.rs\" or name == \"*.toml\")",
            Some("src"),
            Some("src"),
            Some("dir"),
            None,
            None,
            None,
        );
        // PartialMatch & MisMatch -> MisMatch
        assert!(matches!(r, MatchResult::MisMatch(_)));
    }

    #[test]
    fn test_compound_blacklist_whitelist_dir_partial_match() {
        // White: path == "data/**", black: path == "**/temp*"
        // dir "data" -> blacklist: "data" doesn't match "temp*" -> flows to whitelist
        // whitelist: "data" partial matches "data/**" -> PartialMatch
        let (skip, cont, check) = skip(
            Some("path == \"data/**\""),
            Some("path == \"**/temp*\""),
            Some("data"),
            Some("data"),
            Some("dir"),
            None,
            None,
            None,
        );
        // Blacklist doesn't match -> whitelist PartialMatch -> skip entry, continue scan
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_compound_blacklist_whitelist_both_match() {
        // White: path == "data/**", black: path == "**/temp*"
        // dir "data/temp_cache" -> blacklist full match -> skip, stop
        let (skip, cont, check) = skip(
            Some("path == \"data/**\""),
            Some("path == \"**/temp*\""),
            Some("temp_cache"),
            Some("data/temp_cache"),
            Some("dir"),
            None,
            None,
            None,
        );
        // Blacklist PathMatch -> (true, false, false)
        assert_eq!((skip, cont, check), (true, false, false));
    }

    #[test]
    fn test_compound_multiple_or_paths_with_match() {
        // (path == "src/**" or path == "lib/**" or path == "tests/**")
        // Actually, parser handles chained or: a or b or c
        let r = eval(
            "path == \"src/**\" or path == \"lib/**\" or path == \"tests/**\"",
            Some("main.rs"),
            Some("tests/main.rs"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_compound_nested_parentheses() {
        // ((name == "*.rs") and (type == file))
        let r = eval(
            "((name == \"*.rs\") and (type == file))",
            Some("main.rs"),
            None,
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    // ==================== MatchAddon additional merge verification ====================

    #[test]
    fn test_match_addon_and_propagation_to_should_skip() {
        // path == "a/b" and name == "*.rs" -> MixMatch
        // In should_skip, Match(MixMatch) -> (false, is_dir, true)
        let (skip, cont, check) = skip(
            Some("path == \"a/b\" and name == \"*.rs\""),
            None,
            Some("test.rs"),
            Some("a/b"),
            Some("file"),
            None,
            None,
            None,
        );
        // Match with non-path component -> check_children=true
        assert_eq!((skip, cont, check), (false, false, true));
    }

    #[test]
    fn test_match_addon_path_only_propagation() {
        // path == "src/**" only -> Match(PathMatch)
        // In should_skip, Match(PathMatch) -> (false, is_dir, false)
        let (skip, cont, check) = skip(
            Some("path == \"src/**\""),
            None,
            Some("test.rs"),
            Some("src/test.rs"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, true, false));
    }

    // ==================== Additional edge cases ====================

    #[test]
    fn test_glob_star_single_component_deep_path() {
        // path == "a/*/c/*/e" should match "a/x/c/y/e" but not "a/x/y/c/z/e"
        let r1 = eval(
            "path == \"a/*/c/*/e\"",
            None,
            Some("a/x/c/y/e"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r1, MatchResult::Match(MatchAddon::PathMatch));

        let r2 = eval(
            "path == \"a/*/c/*/e\"",
            None,
            Some("a/x/y/c/z/e"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch));
    }

    #[test]
    fn test_glob_doublestar_multiple() {
        // path == "a/**/b/**/c" should match "a/x/b/y/z/c"
        let r = eval(
            "path == \"a/**/b/**/c\"",
            None,
            Some("a/x/b/y/z/c"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_path_exact_match_no_wildcards() {
        // Exact path match without any wildcards
        let r = eval(
            "path == \"src/main.rs\"",
            None,
            Some("src/main.rs"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        let r2 = eval(
            "path == \"src/main.rs\"",
            None,
            Some("src/lib.rs"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch));
    }

    #[test]
    fn test_name_with_bracket_glob() {
        // name == "[abc].txt" should match "a.txt", "b.txt", "c.txt" but not "d.txt"
        let r1 = eval("name == \"[abc].txt\"", Some("a.txt"), None, None, None, None, None);
        assert_eq!(r1, MatchResult::Match(MatchAddon::NonPathMatch));

        let r2 = eval("name == \"[abc].txt\"", Some("d.txt"), None, None, None, None, None);
        assert_eq!(r2, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_invalid_type_value() {
        // type == "invalid_type" should error during parsing
        let r = FilterExpression::parse("type == invalid_type");
        assert!(r.is_err());
    }

    #[test]
    fn test_invalid_modified_date_format() {
        // Invalid date format
        let r = FilterExpression::parse("modified > \"not-a-date\"");
        assert!(r.is_err());
    }

    #[test]
    fn test_modified_eq_compact_different_day() {
        // modified == 20250116, file mtime = 2025-01-15 -> no match
        let file_epoch = date_to_epoch(2025, 1, 15, 12, 0, 0);
        let r = eval_with_now(
            "modified == 20250116",
            None,
            None,
            None,
            Some(file_epoch),
            None,
            None,
            file_epoch + 86400 * 10,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::Other));
    }

    #[test]
    fn test_whitelist_doublestar_at_start_file_match() {
        // "**/target" with file (not dir) at "src/target"
        let (skip, cont, check) = skip(
            Some("path == \"**/target\""),
            None,
            Some("target"),
            Some("src/target"),
            Some("file"),
            None,
            None,
            None,
        );
        // File full match -> (false, false, false)
        assert_eq!((skip, cont, check), (false, false, false));
    }

    #[test]
    fn test_blacklist_name_only_continues_scan() {
        // Blacklist with name-only condition (no path)
        // Match(NonPathMatch) in blacklist -> skip entry, continue scan for dirs
        let (skip, cont, check) = skip(
            None,
            Some("name == \"*.tmp\""),
            Some("test.tmp"),
            Some("a/test.tmp"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, true, true));
    }

    #[test]
    fn test_blacklist_name_only_file() {
        // Blacklist with name-only condition for a file
        let (skip, cont, check) = skip(
            None,
            Some("name == \"*.tmp\""),
            Some("test.tmp"),
            Some("a/test.tmp"),
            Some("file"),
            None,
            None,
            None,
        );
        // Match(NonPathMatch) for file -> skip, is_dir=false so cont=false, check=true
        assert_eq!((skip, cont, check), (true, false, true));
    }

    #[test]
    fn test_date_to_epoch_leap_year() {
        // 2024 is a leap year, Feb 29 should be valid
        let feb28 = date_to_epoch(2024, 2, 28, 0, 0, 0);
        let feb29 = date_to_epoch(2024, 2, 29, 0, 0, 0);
        assert_eq!(feb29 - feb28, 86400); // exactly one day apart
    }

    #[test]
    fn test_date_to_epoch_year_boundary() {
        // Dec 31 to Jan 1
        let dec31 = date_to_epoch(2024, 12, 31, 23, 59, 59);
        let jan1 = date_to_epoch(2025, 1, 1, 0, 0, 0);
        assert_eq!(jan1 - dec31, 1); // 1 second apart
    }

    // ==================== 祖先路径匹配测试 ====================
    // 对应 app/tests/test_scan.rs 中 7 个失败的集成测试

    #[test]
    fn test_path_ancestor_match_dir1_files() {
        // pattern "*dir1*" (depth=1) 应匹配 dir1 下所有文件和子目录
        // 对应 test_scan_rmatch_path_dir1

        // dir1 本身（目录）→ 全路径匹配
        let r = eval(
            "path == '*dir1*'",
            Some("dir1"),
            Some("dir1"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // dir1/file1.txt（文件，depth=2 > pattern_depth=1）→ 祖先 "dir1" 匹配
        let r = eval(
            "path == '*dir1*'",
            Some("file1.txt"),
            Some("dir1/file1.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // dir1/subdir1（目录，depth=2 > pattern_depth=1）→ 祖先 "dir1" 匹配
        let r = eval(
            "path == '*dir1*'",
            Some("subdir1"),
            Some("dir1/subdir1"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert!(matches!(
            r,
            MatchResult::Match(MatchAddon::PathMatch) | MatchResult::PartialMatch
        ));

        // dir1/subdir1/file3.txt（文件，depth=3 > pattern_depth=1）→ 祖先 "dir1" 匹配
        let r = eval(
            "path == '*dir1*'",
            Some("file3.txt"),
            Some("dir1/subdir1/file3.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // dir2/file5.txt 不应匹配
        let r = eval(
            "path == '*dir1*'",
            Some("file5.txt"),
            Some("dir2/file5.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert!(matches!(r, MatchResult::MisMatch(_)));

        // file8.txt（根目录文件，depth=1 == pattern_depth=1）全路径不匹配
        let r = eval(
            "path == '*dir1*'",
            Some("file8.txt"),
            Some("file8.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert!(matches!(r, MatchResult::MisMatch(_)));
    }

    #[test]
    fn test_path_ancestor_match_dir_wildcard() {
        // pattern "*dir*" (depth=1) 应匹配 dir1 和 dir2 下所有文件
        // 对应 test_scan_rmatch_path_dir

        let r = eval(
            "path == '*dir*'",
            Some("file1.txt"),
            Some("dir1/file1.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        let r = eval(
            "path == '*dir*'",
            Some("file5.txt"),
            Some("dir2/file5.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        let r = eval(
            "path == '*dir*'",
            Some("file6.txt"),
            Some("dir2/subdir2/file6.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // file8.txt 在根目录，不匹配
        let r = eval(
            "path == '*dir*'",
            Some("file8.txt"),
            Some("file8.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert!(matches!(r, MatchResult::MisMatch(_)));
    }

    #[test]
    fn test_path_ancestor_match_subdir1() {
        // pattern "*dir*/subdir1*" (depth=2) 应匹配 dir1/subdir1 下的文件
        // 对应 test_scan_rmatch_path_subdir1

        // dir1/subdir1 目录本身 → 全路径匹配
        let r = eval(
            "path == '*dir*/subdir1*'",
            Some("subdir1"),
            Some("dir1/subdir1"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // dir1/subdir1/file3.txt（depth=3 > pattern_depth=2）→ 祖先匹配
        let r = eval(
            "path == '*dir*/subdir1*'",
            Some("file3.txt"),
            Some("dir1/subdir1/file3.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // dir1/file1.txt（depth=2 == pattern_depth=2）但全路径不匹配
        let r = eval(
            "path == '*dir*/subdir1*'",
            Some("file1.txt"),
            Some("dir1/file1.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert!(matches!(r, MatchResult::MisMatch(_)));
    }

    #[test]
    fn test_path_ancestor_match_subdir_wildcard() {
        // pattern "*dir*/subdir*" (depth=2) 应匹配 subdir1 和 subdir2 下的文件
        // 对应 test_scan_rmatch_path_subdir

        let r = eval(
            "path == '*dir*/subdir*'",
            Some("file3.txt"),
            Some("dir1/subdir1/file3.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        let r = eval(
            "path == '*dir*/subdir*'",
            Some("file6.txt"),
            Some("dir2/subdir2/file6.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // dir1/file1.txt 不在 subdir 下，不应匹配
        let r = eval(
            "path == '*dir*/subdir*'",
            Some("file1.txt"),
            Some("dir1/file1.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert!(matches!(r, MatchResult::MisMatch(_)));
    }

    #[test]
    fn test_path_ancestor_match_globstar_subdir() {
        // pattern "**/subdir*" (depth=2, 含 **) 应匹配任意深度下的 subdir*
        // 对应 test_scan_rmatch_path_subdir_globstar

        // dir1/subdir1 → 全路径匹配
        let r = eval(
            "path == '**/subdir*'",
            Some("subdir1"),
            Some("dir1/subdir1"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // dir1/subdir1/file3.txt（depth=3）→ 祖先 "dir1/subdir1" 匹配 "**/subdir*"
        let r = eval(
            "path == '**/subdir*'",
            Some("file3.txt"),
            Some("dir1/subdir1/file3.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // dir2/subdir2/file6.txt → 祖先 "dir2/subdir2" 匹配
        let r = eval(
            "path == '**/subdir*'",
            Some("file6.txt"),
            Some("dir2/subdir2/file6.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // dir1/file1.txt → 无祖先匹配 "**/subdir*"
        let r = eval(
            "path == '**/subdir*'",
            Some("file1.txt"),
            Some("dir1/file1.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert!(matches!(r, MatchResult::MisMatch(_)));
    }

    #[test]
    fn test_path_ancestor_match_deep_date_pattern() {
        // pattern "*/*/*/*/20250[123456]*" (depth=5) 应匹配深层路径
        // 对应 test_scan_rmatch_date

        let deep_path = "JA12831_AB/HZ/A3/SMT-AVI-0026/20250119/NG/C47533669B31ND4AD/file.jpg";
        let r = eval(
            "path == '*/*/*/*/20250[123456]*'",
            Some("file.jpg"),
            Some(deep_path),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // 不匹配的日期 20251119（月份不在 [123456] 范围内）
        let deep_path2 = "JA12833_AB/HZ/A3/SMT-AVI-0026/20251119/NG/C47533669B31ND4AD/file.jpg";
        let r = eval(
            "path == '*/*/*/*/20250[123456]*'",
            Some("file.jpg"),
            Some(deep_path2),
            Some("file"),
            None,
            None,
            None,
        );
        assert!(matches!(r, MatchResult::MisMatch(_)));
    }

    #[test]
    fn test_path_ancestor_match_shallow_pattern_deep_file() {
        // pattern "JA12835_*" (depth=1) 应匹配 JA12835_AB 下所有深层文件
        // 对应 test_scan_rmatch_ja12835_

        let deep_path = "JA12835_AB/GX/A3/SMT-AVI-0026/20250519/NG/C47533669B31ND4AD/file.jpg";
        let r = eval(
            "path == 'JA12835_*'",
            Some("file.jpg"),
            Some(deep_path),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));

        // JA12831_AB 开头的不应匹配
        let deep_path2 = "JA12831_AB/HZ/A3/SMT-AVI-0026/20250119/NG/C47533669B31ND4AD/file.jpg";
        let r = eval(
            "path == 'JA12835_*'",
            Some("file.jpg"),
            Some(deep_path2),
            Some("file"),
            None,
            None,
            None,
        );
        assert!(matches!(r, MatchResult::MisMatch(_)));
    }

    // ==================== dir_date tests ====================

    // --- extract_date_from_dir_name tests ---

    #[test]
    fn test_extract_date_yyyymmdd() {
        let epoch = extract_date_from_dir_name("20240301");
        assert_eq!(epoch, Some(date_to_epoch(2024, 3, 1, 0, 0, 0)));
    }

    #[test]
    fn test_extract_date_yymmdd() {
        let epoch = extract_date_from_dir_name("240301");
        assert_eq!(epoch, Some(date_to_epoch(2024, 3, 1, 0, 0, 0)));
    }

    #[test]
    fn test_extract_date_iso() {
        let epoch = extract_date_from_dir_name("2024-03-01");
        assert_eq!(epoch, Some(date_to_epoch(2024, 3, 1, 0, 0, 0)));
    }

    #[test]
    fn test_extract_date_at_end() {
        let epoch = extract_date_from_dir_name("backup_240301");
        assert_eq!(epoch, Some(date_to_epoch(2024, 3, 1, 0, 0, 0)));
    }

    #[test]
    fn test_extract_date_at_start() {
        let epoch = extract_date_from_dir_name("20240301_logs");
        assert_eq!(epoch, Some(date_to_epoch(2024, 3, 1, 0, 0, 0)));
    }

    #[test]
    fn test_extract_date_in_middle() {
        let epoch = extract_date_from_dir_name("project_2024-03-01_final");
        assert_eq!(epoch, Some(date_to_epoch(2024, 3, 1, 0, 0, 0)));
    }

    #[test]
    fn test_extract_date_none_no_date() {
        assert_eq!(extract_date_from_dir_name("nodate_folder"), None);
    }

    #[test]
    fn test_extract_date_none_invalid_month() {
        assert_eq!(extract_date_from_dir_name("20241301"), None);
    }

    #[test]
    fn test_extract_date_none_invalid_day() {
        assert_eq!(extract_date_from_dir_name("240132"), None);
    }

    #[test]
    fn test_extract_date_none_short_digits() {
        assert_eq!(extract_date_from_dir_name("abc12"), None);
    }

    #[test]
    fn test_extract_date_iso_priority_over_digits() {
        // "2024-03-01" 优先于 YYYYMMDD/YYMMDD
        let epoch = extract_date_from_dir_name("x2024-03-01x");
        assert_eq!(epoch, Some(date_to_epoch(2024, 3, 1, 0, 0, 0)));
    }

    // --- parse dir_date condition tests ---

    #[test]
    fn test_parse_dir_date_yymmdd() {
        let expr = FilterExpression::parse("dir_date <= 240301");
        assert!(expr.is_ok(), "Should parse YYMMDD dir_date");
    }

    #[test]
    fn test_parse_dir_date_yyyymmdd() {
        let expr = FilterExpression::parse("dir_date >= 20240101");
        assert!(expr.is_ok(), "Should parse YYYYMMDD dir_date");
    }

    #[test]
    fn test_parse_dir_date_iso_quoted() {
        let expr = FilterExpression::parse("dir_date == \"2024-03-01\"");
        assert!(expr.is_ok(), "Should parse quoted ISO dir_date");
    }

    #[test]
    fn test_parse_dir_date_invalid() {
        let expr = FilterExpression::parse("dir_date < abc");
        assert!(expr.is_err(), "Should fail for invalid dir_date value");
    }

    // --- evaluate dir_date condition tests ---

    #[test]
    fn test_dir_date_match_le() {
        // dir_date <= 240301, dir named "20240101" → Match(PathMatch)
        let r = eval(
            "dir_date <= 240301",
            Some("20240101"),
            Some("20240101"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_dir_date_no_match_le() {
        // dir_date <= 240301, dir named "20240501" → MisMatch(FullPathNotMatch)
        let r = eval(
            "dir_date <= 240301",
            Some("20240501"),
            Some("20240501"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch));
    }

    #[test]
    fn test_dir_date_no_date_dir() {
        // dir_date <= 240301, dir named "nodate" → Match(NonPathMatch)
        let r = eval(
            "dir_date <= 240301",
            Some("nodate"),
            Some("nodate"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_dir_date_file_transparent() {
        // dir_date <= 240301 on a file → Match(NonPathMatch)
        let r = eval(
            "dir_date <= 240301",
            Some("test.txt"),
            Some("test.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::NonPathMatch));
    }

    #[test]
    fn test_dir_date_eq_day_granularity() {
        // dir_date == "2024-03-01", dir named "20240301" → Match(PathMatch)
        let r = eval(
            "dir_date == \"2024-03-01\"",
            Some("20240301"),
            Some("20240301"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_dir_date_gt() {
        // dir_date > 20240101, dir named "20240301" → Match(PathMatch)
        let r = eval(
            "dir_date > 20240101",
            Some("20240301"),
            Some("20240301"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_dir_date_ne() {
        // dir_date != "2024-03-01", dir named "20240301" → MisMatch(FullPathNotMatch)
        let r = eval(
            "dir_date != \"2024-03-01\"",
            Some("20240301"),
            Some("20240301"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::MisMatch(MisMatchAddon::FullPathNotMatch));
    }

    #[test]
    fn test_dir_date_ne_different_date() {
        // dir_date != "2024-03-01", dir named "20240501" → Match(PathMatch)
        let r = eval(
            "dir_date != \"2024-03-01\"",
            Some("20240501"),
            Some("20240501"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_dir_date_with_prefix() {
        // dir named "backup_240101" → should extract 240101
        let r = eval(
            "dir_date <= 240301",
            Some("backup_240101"),
            Some("backup_240101"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    #[test]
    fn test_dir_date_iso_in_name() {
        // dir named "project_2024-01-15_v2" → should extract 2024-01-15
        let r = eval(
            "dir_date <= \"2024-03-01\"",
            Some("project_2024-01-15_v2"),
            Some("project_2024-01-15_v2"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!(r, MatchResult::Match(MatchAddon::PathMatch));
    }

    // --- should_skip integration tests for dir_date ---

    #[test]
    fn test_skip_dir_date_match() {
        // 日期目录匹配 → 保留，扫描子目录，子项免检
        let (skip, cont, check) = skip(
            Some("dir_date <= 240301"),
            None,
            Some("20240101"),
            Some("20240101"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, true, false));
    }

    #[test]
    fn test_skip_dir_date_no_match() {
        // 日期目录不匹配 → 跳过，停止扫描
        let (skip, cont, check) = skip(
            Some("dir_date <= 240301"),
            None,
            Some("20240501"),
            Some("20240501"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (true, false, false));
    }

    #[test]
    fn test_skip_dir_date_non_date_dir() {
        // 非日期目录 → 保留，扫描子目录，子项需检查
        let (skip, cont, check) = skip(
            Some("dir_date <= 240301"),
            None,
            Some("project"),
            Some("project"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, true, true));
    }

    #[test]
    fn test_skip_dir_date_file() {
        // 文件 → 保留
        let (skip, cont, check) = skip(
            Some("dir_date <= 240301"),
            None,
            Some("readme.txt"),
            Some("readme.txt"),
            Some("file"),
            None,
            None,
            None,
        );
        assert_eq!((skip, cont, check), (false, false, true));
    }

    #[test]
    fn test_skip_dir_date_and_path_combined_nondate_dir() {
        // 非日期目录 "project" → dir_date=Match(NonPath), path=PartialMatch → PartialMatch
        let (s, c, ch) = skip(
            Some("dir_date <= \"2024-03-01\" and path == \"project/*\""),
            None,
            Some("project"),
            Some("project"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((s, c, ch), (true, true, true));
    }

    #[test]
    fn test_skip_dir_date_and_path_combined_match() {
        // 日期子目录 "project/20240101" → dir_date=Match(Path), path=Match(Path) → Match(Path)
        let (s, c, ch) = skip(
            Some("dir_date <= \"2024-03-01\" and path == \"project/*\""),
            None,
            Some("20240101"),
            Some("project/20240101"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((s, c, ch), (false, true, false));
    }

    #[test]
    fn test_skip_dir_date_and_path_combined_no_match() {
        // 日期子目录不匹配 "project/20240501" → dir_date=MisMatch(Full), path=Match(Path) → MisMatch(Full)
        let (s, c, ch) = skip(
            Some("dir_date <= \"2024-03-01\" and path == \"project/*\""),
            None,
            Some("20240501"),
            Some("project/20240501"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((s, c, ch), (true, false, false));
    }

    #[test]
    fn test_skip_dir_date_range_match() {
        // dir_date >= 20240101 and dir_date <= 20240331
        let (s, c, ch) = skip(
            Some("dir_date >= 20240101 and dir_date <= 20240331"),
            None,
            Some("20240215"),
            Some("20240215"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((s, c, ch), (false, true, false));
    }

    #[test]
    fn test_skip_dir_date_range_no_match() {
        // 超出范围
        let (s, c, ch) = skip(
            Some("dir_date >= 20240101 and dir_date <= 20240331"),
            None,
            Some("20240501"),
            Some("20240501"),
            Some("dir"),
            None,
            None,
            None,
        );
        assert_eq!((s, c, ch), (true, false, false));
    }

    // ==================== get_filter_field_definitions Tests ====================

    #[test]
    fn test_field_definitions_covers_all_condition_variants() {
        // 确保 get_filter_field_definitions 覆盖了 FilterCondition 的所有变体
        let defs = get_filter_field_definitions();
        let field_names: Vec<&str> = defs.iter().map(|d| d.name).collect();

        // FilterCondition 枚举的所有字段名
        let expected = vec!["name", "size", "modified", "extension", "path", "type", "dir_date"];
        for name in &expected {
            assert!(field_names.contains(name), "缺少字段定义: {}", name);
        }
        assert_eq!(defs.len(), expected.len(), "字段数量应与 FilterCondition 变体数一致");
    }

    #[test]
    fn test_field_definitions_operators_not_empty() {
        let defs = get_filter_field_definitions();
        for def in &defs {
            assert!(!def.operators.is_empty(), "字段 {} 的操作符列表不应为空", def.name);
        }
    }

    #[test]
    fn test_field_definitions_operators_are_valid() {
        // 所有操作符 value 必须能被 Lexer::parse_operator 解析
        let valid_ops = ["==", "!=", "<", ">", "<=", ">="];
        let defs = get_filter_field_definitions();
        for def in &defs {
            for op in &def.operators {
                assert!(
                    valid_ops.contains(&op.value),
                    "字段 {} 包含无效操作符: {}",
                    def.name,
                    op.value
                );
            }
        }
    }

    #[test]
    fn test_field_definitions_enum_field_has_values() {
        let defs = get_filter_field_definitions();
        let type_def = defs.iter().find(|d| d.name == "type").expect("应有 type 字段");
        let enum_values = type_def.enum_values.as_ref().expect("type 字段应有 enum_values");
        assert!(enum_values.contains(&"file"));
        assert!(enum_values.contains(&"dir"));
        assert!(enum_values.contains(&"symlink"));
    }

    #[test]
    fn test_field_definitions_non_enum_fields_have_no_values() {
        let defs = get_filter_field_definitions();
        for def in &defs {
            if def.name != "type" {
                assert!(def.enum_values.is_none(), "非枚举字段 {} 不应有 enum_values", def.name);
            }
        }
    }

    #[test]
    fn test_field_definitions_expressions_parseable() {
        // 验证每个字段+操作符组合能被 parse_filter_expression 成功解析
        let defs = get_filter_field_definitions();
        for def in &defs {
            for op in &def.operators {
                let test_value = match def.value_type {
                    "glob" => "\"*.txt\"",
                    "bytes" => "1024",
                    "duration_or_date" => "3d",
                    "enum" => def.enum_values.as_ref().and_then(|v| v.first()).unwrap_or(&"file"),
                    "date" => "\"20240101\"",
                    _ => "test",
                };
                let expr = format!("{} {} {}", def.name, op.value, test_value);
                let result = parse_filter_expression(&expr);
                assert!(result.is_ok(), "表达式解析失败: '{}', 错误: {:?}", expr, result.err());
            }
        }
    }
}
