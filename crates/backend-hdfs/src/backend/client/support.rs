fn plan_read_range(
    file_length: u64,
    offset: u64,
    count: u64,
) -> Result<Option<(usize, usize)>, StorageError> {
    if count == 0 || offset >= file_length {
        return Ok(None);
    }
    let length = count.min(file_length - offset);
    let offset = usize::try_from(offset)
        .map_err(|_| config_error("HDFS read offset exceeds platform capacity"))?;
    let length = usize::try_from(length)
        .map_err(|_| config_error("HDFS read length exceeds platform capacity"))?;
    Ok(Some((offset, length)))
}

fn strip_root<'a>(path: &'a str, root: &str) -> Result<&'a str, StorageError> {
    if root == "/" {
        return path
            .strip_prefix('/')
            .ok_or_else(|| config_error("HDFS status path must be absolute"));
    }
    if path == root {
        return Ok("");
    }
    path.strip_prefix(root)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .ok_or_else(|| config_error("HDFS status path is outside the configured root"))
}

fn millis_to_nanos(value: u64) -> Result<i64, StorageError> {
    let nanos = value
        .checked_mul(1_000_000)
        .ok_or_else(|| config_error("HDFS timestamp overflows nanoseconds"))?;
    i64::try_from(nanos).map_err(|_| config_error("HDFS timestamp does not fit i64"))
}

fn nanos_to_millis(value: i64) -> Result<u64, StorageError> {
    let value = u64::try_from(value)
        .map_err(|_| config_error("HDFS timestamp cannot precede the Unix epoch"))?;
    Ok(value / 1_000_000)
}

/// Construct and validate an HDFS storage root.
///
/// # Errors
///
/// Returns an error when client construction, root lookup, or root creation
/// fails, or when the configured root is an existing file.
pub async fn create_hdfs_storage(
    location: &str,
    config: &HdfsConfig,
    block_size: Option<u64>,
    ensure_dir: bool,
) -> Result<HDFSStorage, StorageError> {
    let transfer_concurrency = crate::transfer_concurrency::resolve_transfer_concurrency(
        crate::transfer_concurrency::TransferBackend::Hdfs,
        crate::transfer_concurrency::TransferConcurrency::defaults(4, 1),
        None,
    )?;
    let (client, location) = build_hdfs_client(location, config)?;
    match retry_hdfs_read("get storage root", None, None, || {
        client.get_file_info(location.root())
    })
    .await
    {
        Ok(status) if !status.isdir => {
            return Err(StorageError::InvalidPath(
                "configured HDFS root is not a directory".to_string(),
            ));
        }
        Ok(_) => {}
        Err(StorageError::FileNotFound(_)) if ensure_dir => client
            .mkdirs(location.root(), 0o755, true)
            .await
            .map_err(|error| hdfs_operation_error("create storage root", None, &error))?,
        Err(StorageError::FileNotFound(_)) => {
            return Err(StorageError::DirectoryNotFound(
                "<configured-root>".to_string(),
            ));
        }
        Err(error) => return Err(error),
    }
    Ok(HDFSStorage {
        client,
        location,
        block_size: block_size.unwrap_or(DEFAULT_BLOCK_SIZE),
        transfer_concurrency,
    })
}

fn safe_error_path(relative_path: Option<&Path>) -> String {
    relative_path.map_or_else(|| "<root>".to_string(), |path| path.display().to_string())
}

fn safe_hadoop_class(class: &str) -> Option<String> {
    (!class.is_empty()
        && class.len() <= 160
        && class
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'$')))
    .then(|| class.to_string())
}

fn class_has_suffix(class: &str, suffixes: &[&str]) -> bool {
    suffixes
        .iter()
        .any(|suffix| class == *suffix || class.ends_with(&format!(".{suffix}")))
}

fn structured_hdfs_error(
    operation: &'static str,
    relative_path: Option<&Path>,
    kind: crate::error::HdfsErrorKind,
    class: Option<&str>,
    diagnostic: &'static str,
    retryable: bool,
) -> StorageError {
    StorageError::HdfsOperation(crate::error::HdfsOperationError {
        operation,
        relative_path: relative_path.map(Path::to_path_buf),
        kind,
        hadoop_class: class.and_then(safe_hadoop_class),
        diagnostic,
        retryable,
    })
}

fn hdfs_rpc_error(
    operation: &'static str,
    relative_path: Option<&Path>,
    class: &str,
) -> StorageError {
    let safe_path = safe_error_path(relative_path);
    if class_has_suffix(class, &["FileNotFoundException", "PathNotFoundException"]) {
        StorageError::FileNotFound(safe_path)
    } else if class_has_suffix(class, &["AccessControlException"]) {
        StorageError::PermissionDenied(safe_path)
    } else if class_has_suffix(class, &["ChecksumException"]) {
        StorageError::ChecksumError(safe_path)
    } else if class_has_suffix(
        class,
        &[
            "DiskOutOfSpaceException",
            "DiskChecker$DiskOutOfSpaceException",
            "NSQuotaExceededException",
        ],
    ) {
        StorageError::InsufficientSpace(safe_path)
    } else {
        let kind = if class_has_suffix(
            class,
            &["FileAlreadyExistsException", "AlreadyBeingCreatedException"],
        ) {
            crate::error::HdfsErrorKind::AlreadyExists
        } else if class_has_suffix(class, &["UnsupportedOperationException"]) {
            crate::error::HdfsErrorKind::Unsupported
        } else {
            crate::error::HdfsErrorKind::Rpc
        };
        structured_hdfs_error(
            operation,
            relative_path,
            kind,
            Some(class),
            "upstream RPC failure",
            false,
        )
    }
}

fn hdfs_operation_error(
    operation: &'static str,
    relative_path: Option<&Path>,
    error: &hdfs_native::HdfsError,
) -> StorageError {
    use hdfs_native::HdfsError;
    let safe_path = safe_error_path(relative_path);
    match error {
        HdfsError::FileNotFound(_) => StorageError::FileNotFound(safe_path),
        HdfsError::InvalidPath(_) | HdfsError::InvalidArgument(_) => {
            StorageError::InvalidPath(safe_path)
        }
        HdfsError::ChecksumError => StorageError::ChecksumError(safe_path),
        HdfsError::IOError(error) if error.kind() == std::io::ErrorKind::NotFound => {
            StorageError::FileNotFound(safe_path)
        }
        HdfsError::IOError(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            StorageError::PermissionDenied(safe_path)
        }
        HdfsError::IOError(error) if error.kind() == std::io::ErrorKind::StorageFull => {
            StorageError::InsufficientSpace(safe_path)
        }
        HdfsError::RPCError(class, _) | HdfsError::FatalRPCError(class, _) => {
            hdfs_rpc_error(operation, relative_path, class)
        }
        HdfsError::AlreadyExists(_)
        | HdfsError::BlocksNotFound(_)
        | HdfsError::DataTransferError(_)
        | HdfsError::IsADirectoryError(_)
        | HdfsError::UnsupportedFeature(_)
        | HdfsError::UnsupportedErasureCodingPolicy(_)
        | HdfsError::TrashNotEnabled
        | HdfsError::ErasureCodingError(_)
        | HdfsError::SASLError(_)
        | HdfsError::GSSAPIError(_, _, _)
        | HdfsError::NoSASLMechanism
        | HdfsError::IOError(_)
        | HdfsError::OperationFailed(_)
        | HdfsError::InternalError(_)
        | HdfsError::InvalidRPCResponse(_)
        | HdfsError::UrlParseError(_)
        | HdfsError::XmlParseError(_) => {
            let (kind, diagnostic, retryable) = hdfs_structured_attributes(error);
            structured_hdfs_error(operation, relative_path, kind, None, diagnostic, retryable)
        }
    }
}

fn hdfs_structured_attributes(
    error: &hdfs_native::HdfsError,
) -> (crate::error::HdfsErrorKind, &'static str, bool) {
    use hdfs_native::HdfsError;
    match error {
        HdfsError::AlreadyExists(_) => (
            crate::error::HdfsErrorKind::AlreadyExists,
            "destination already exists",
            false,
        ),
        HdfsError::BlocksNotFound(_) => (
            crate::error::HdfsErrorKind::BlocksMissing,
            "file blocks are unavailable",
            true,
        ),
        HdfsError::DataTransferError(_) => (
            crate::error::HdfsErrorKind::DataTransfer,
            "data transfer failed",
            true,
        ),
        HdfsError::IsADirectoryError(_) => (
            crate::error::HdfsErrorKind::Directory,
            "target is a directory",
            false,
        ),
        HdfsError::UnsupportedFeature(_)
        | HdfsError::UnsupportedErasureCodingPolicy(_)
        | HdfsError::TrashNotEnabled => (
            crate::error::HdfsErrorKind::Unsupported,
            "operation is unsupported",
            false,
        ),
        HdfsError::ErasureCodingError(_) => (
            crate::error::HdfsErrorKind::ErasureCoding,
            "erasure coding operation failed",
            false,
        ),
        HdfsError::SASLError(_) | HdfsError::GSSAPIError(_, _, _) | HdfsError::NoSASLMechanism => (
            crate::error::HdfsErrorKind::Authentication,
            "authentication failed",
            false,
        ),
        HdfsError::IOError(error) => (
            crate::error::HdfsErrorKind::Io,
            "I/O operation failed",
            is_retryable_io(error.kind()),
        ),
        HdfsError::OperationFailed(_)
        | HdfsError::InternalError(_)
        | HdfsError::InvalidRPCResponse(_)
        | HdfsError::UrlParseError(_)
        | HdfsError::XmlParseError(_)
        | HdfsError::InvalidPath(_)
        | HdfsError::InvalidArgument(_)
        | HdfsError::ChecksumError
        | HdfsError::FileNotFound(_)
        | HdfsError::RPCError(_, _)
        | HdfsError::FatalRPCError(_, _) => (
            crate::error::HdfsErrorKind::Internal,
            "internal upstream operation failed",
            false,
        ),
    }
}

fn is_retryable_io(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::UnexpectedEof
    )
}
