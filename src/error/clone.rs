use super::StorageError;

impl Clone for StorageError {
    fn clone(&self) -> Self {
        match self {
            StorageError::HdfsOperation(value) => StorageError::HdfsOperation(value.clone()),
            StorageError::IoError(error) => StorageError::OperationError(error.to_string()),
            StorageError::ConfigError(value) => StorageError::ConfigError(value.clone()),
            StorageError::UnsupportedType(value) => StorageError::UnsupportedType(value.clone()),
            StorageError::OperationError(value) => StorageError::OperationError(value.clone()),
            StorageError::InvalidPath(value) => StorageError::InvalidPath(value.clone()),
            StorageError::InvalidFilterExpression(value) => {
                StorageError::InvalidFilterExpression(value.clone())
            }
            StorageError::MismatchedParentheses(value) => {
                StorageError::MismatchedParentheses(value.clone())
            }
            StorageError::InvalidToken(value) => StorageError::InvalidToken(*value),
            StorageError::UnexpectedEndOfToken(value) => {
                StorageError::UnexpectedEndOfToken(value.clone())
            }
            StorageError::ChecksumError(value) => StorageError::ChecksumError(value.clone()),
            StorageError::Cancelled => StorageError::Cancelled,
            StorageError::S3Error(value) => StorageError::S3Error(value.clone()),
            StorageError::NfsError(value) => StorageError::NfsError(value.clone()),
            StorageError::FileNotFound(value) => StorageError::FileNotFound(value.clone()),
            StorageError::DirectoryNotFound(value) => {
                StorageError::DirectoryNotFound(value.clone())
            }
            StorageError::PermissionDenied(value) => StorageError::PermissionDenied(value.clone()),
            StorageError::MismatchedType => StorageError::MismatchedType,
            StorageError::TaskJoinError(error) => StorageError::OperationError(error.to_string()),
            StorageError::UrlParseError(value) => StorageError::UrlParseError(value.clone()),
            StorageError::SerializationError(value) => {
                StorageError::SerializationError(value.clone())
            }
            StorageError::InsufficientSpace(value) => {
                StorageError::InsufficientSpace(value.clone())
            }
            StorageError::FileLockError(value) => StorageError::FileLockError(value.clone()),
            StorageError::WinAceError(value) => StorageError::WinAceError(value.clone()),
            StorageError::CifsError(value) => StorageError::CifsError(value.clone()),
            StorageError::ReadError(value) => StorageError::ReadError(value.clone()),
            StorageError::WriteError(value) => StorageError::WriteError(value.clone()),
            StorageError::MismatchData(value) => StorageError::MismatchData(value.clone()),
            StorageError::MismatchMeta(value) => StorageError::MismatchMeta(value.clone()),
        }
    }
}
