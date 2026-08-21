use std::path::PathBuf;
use thiserror::Error;

use crate::integrity_check::{MismatchDataField, MismatchMetaField};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HdfsErrorKind {
    AlreadyExists,
    BlocksMissing,
    DataTransfer,
    Directory,
    ErasureCoding,
    Io,
    Rpc,
    Authentication,
    Unsupported,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HdfsOperationError {
    pub operation: &'static str,
    pub relative_path: Option<PathBuf>,
    pub kind: HdfsErrorKind,
    pub hadoop_class: Option<String>,
    pub diagnostic: &'static str,
    pub retryable: bool,
}

impl std::fmt::Display for HdfsOperationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "HDFS {} failed ({:?})",
            self.operation, self.kind
        )?;
        if let Some(path) = &self.relative_path {
            write!(formatter, " at {}", path.display())?;
        }
        if let Some(class) = &self.hadoop_class {
            write!(formatter, " [{class}]")?;
        }
        write!(formatter, ": {}", self.diagnostic)
    }
}

mod clone;

/// 存储操作的错误类型定义
/// 使用thiserror库实现错误处理和转换
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("{0}")]
    HdfsOperation(HdfsOperationError),
    /// IO操作错误
    /// 文件读写、目录操作等IO失败时触发
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// 配置错误
    /// 当存储配置无效或不完整时触发
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// 不支持的存储类型
    /// 当尝试使用未实现的存储类型时触发
    #[error("Unsupported storage type: {0}")]
    UnsupportedType(String),

    /// 存储操作错误
    /// 一般性的存储操作失败时触发
    #[error("Storage operation error: {0}")]
    OperationError(String),

    /// 路径格式错误
    /// 当存储路径格式无效时触发
    #[error("Invalid path format: {0}")]
    InvalidPath(String),

    /// 过滤表达式错误
    /// 当过滤表达式格式无效或无法解析时触发
    #[error("Invalid filter expression: {0}")]
    InvalidFilterExpression(String),

    /// 括号不匹配错误
    /// 当过滤条件中的括号不匹配时触发
    #[error("Mismatched parentheses: {0}")]
    MismatchedParentheses(String),

    /// 无效令牌错误
    /// 当过滤条件中的令牌无效时触发
    #[error("Invalid filter token: {0}")]
    InvalidToken(char),

    /// 令牌结束错误
    /// 当过滤条件中的令牌非预期结束时触发
    #[error("Unexpected end of filter token: {0}")]
    UnexpectedEndOfToken(String),

    /// 校验和错误
    /// 数据完整性检查失败时触发
    #[error("Checksum error: {0}")]
    ChecksumError(String),

    /// 传输被调用方取消
    /// 当调用方通过 `CancellationToken` 主动取消传输时触发。
    /// 此错误应被视为可重试的结束状态，而非失败：上层可选择重新入队或直接放弃。
    #[error("transfer cancelled by caller")]
    Cancelled,

    /// S3存储相关错误
    /// 与S3服务交互时发生错误时触发
    #[error("S3 error: {0}")]
    S3Error(String),

    /// NFS存储相关错误
    /// 与NFS服务交互时发生错误时触发
    #[error("NFS error: {0}")]
    NfsError(String),

    /// 文件未找到错误
    /// 当尝试访问不存在的文件时触发
    #[error("File not found: {0}")]
    FileNotFound(String),

    /// 目录未找到错误
    /// 当尝试访问不存在的目录时触发
    #[error("Directory not found: {0}")]
    DirectoryNotFound(String),

    /// 权限错误
    /// 当没有足够权限执行操作时触发
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    /// 不匹配的存储类型和文件句柄类型错误
    /// 当存储类型与文件句柄类型不匹配时触发
    #[error("Mismatched storage type and file handle type")]
    MismatchedType,

    /// 异步任务执行错误
    /// 当异步任务执行失败时触发
    #[error("Task join error: {0}")]
    TaskJoinError(#[from] tokio::task::JoinError),

    /// URL解析错误
    /// 当解析存储URL失败时触发
    #[error("URL parse error: {0}")]
    UrlParseError(String),

    /// 序列化/反序列化错误
    /// 数据转换过程中发生错误时触发
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// 存储空间不足错误
    /// 当存储空间不足时触发
    #[error("Insufficient storage space: {0}")]
    InsufficientSpace(String),

    /// 文件锁定错误
    /// 当文件锁定失败时触发
    #[error("File lock error: {0}")]
    FileLockError(String),

    /// Windows ACE错误
    /// 当Windows ACE（访问控制项）操作失败时触发
    #[error("Windows ACE error: {0}")]
    WinAceError(String),

    /// CIFS/SMB 存储相关错误
    /// 与 SMB 服务交互时发生错误时触发
    #[error("CIFS error: {0}")]
    CifsError(String),

    /// 从源存储读取文件数据失败
    #[error("Read error: {0}")]
    ReadError(String),

    /// 向目标存储写入文件数据失败
    #[error("Write error: {0}")]
    WriteError(String),

    /// 文件内容或条目类型不匹配
    #[error("Content mismatch: {0:?}")]
    MismatchData(Vec<MismatchDataField>),

    /// POSIX 元数据不匹配
    #[error("Metadata mismatch: {0:?}")]
    MismatchMeta(Vec<MismatchMetaField>),
}

/// 存储操作的结果类型别名
/// 封装了 `StorageError` 作为错误类型
pub type Result<T> = std::result::Result<T, StorageError>;
