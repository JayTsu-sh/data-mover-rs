fn hdfs_entry_is_filtered(
    entry: &HDFSEntry,
    match_expression: Option<&crate::FilterExpression>,
    exclude_expression: Option<&crate::FilterExpression>,
) -> bool {
    hdfs_filter_decision(entry, match_expression, exclude_expression).0
}
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

fn validate_write_chunk(
    pending: &BTreeMap<u64, bytes::Bytes>,
    next_offset: u64,
    expected_size: u64,
    chunk: &crate::DataChunk,
) -> Result<(), StorageError> {
    let length = u64::try_from(chunk.data.len())
        .map_err(|_| config_error("HDFS write chunk length does not fit u64"))?;
    let end = chunk
        .offset
        .checked_add(length)
        .ok_or_else(|| config_error("HDFS write chunk range overflow"))?;
    if chunk.offset < next_offset || end > expected_size {
        return Err(StorageError::OperationError(format!(
            "invalid HDFS write range [{}, {end}) at committed offset {next_offset}",
            chunk.offset
        )));
    }
    if let Some((&previous_offset, previous)) = pending.range(..=chunk.offset).next_back() {
        let previous_end =
            previous_offset.saturating_add(u64::try_from(previous.len()).unwrap_or(u64::MAX));
        if previous_end > chunk.offset {
            return Err(StorageError::OperationError(
                "overlapping or duplicate HDFS write chunk".to_string(),
            ));
        }
    }
    if let Some((&following_offset, _)) = pending.range(chunk.offset..).next()
        && end > following_offset
    {
        return Err(StorageError::OperationError(
            "overlapping HDFS write chunk".to_string(),
        ));
    }
    Ok(())
}

fn validate_sequential_end(
    next_offset: u64,
    expected_size: u64,
    has_pending: bool,
    require_final_size: bool,
) -> Result<(), StorageError> {
    if has_pending || (require_final_size && next_offset != expected_size) {
        return Err(StorageError::OperationError(format!(
            "HDFS write ended at {next_offset} bytes, expected {expected_size}"
        )));
    }
    Ok(())
}

fn hdfs_filter_decision(
    entry: &HDFSEntry,
    match_expression: Option<&crate::FilterExpression>,
    exclude_expression: Option<&crate::FilterExpression>,
) -> (bool, bool, bool) {
    let path = entry.relative_path.to_string_lossy();
    let file_type = if entry.is_dir { "dir" } else { "file" };
    should_skip(
        match_expression,
        exclude_expression,
        FilterInput {
            file_name: Some(&entry.name),
            file_path: Some(&path),
            file_type: Some(file_type),
            modified_epoch: Some(entry.mtime / 1_000_000_000),
            size: Some(entry.size),
            extension: entry.extension.as_deref().or(Some("")),
        },
    )
}

fn hdfs_read_result(
    dir_path: &str,
    entries: Vec<HDFSEntry>,
    ctx: &crate::dir_tree::ReadContext,
) -> crate::dir_tree::ReadResult {
    let mut files = Vec::new();
    let mut subdirs = Vec::new();
    for entry in entries {
        let (skip, continue_scan, need_filter) = if ctx.apply_filter {
            hdfs_filter_decision(
                &entry,
                ctx.match_expr.as_ref().as_ref(),
                ctx.exclude_expr.as_ref().as_ref(),
            )
        } else {
            (false, true, false)
        };
        let can_descend = entry.is_dir
            && (ctx.max_depth == 0 || ctx.current_depth.saturating_add(1) < ctx.max_depth);
        let entry = Arc::new(crate::EntryEnum::HDFS(entry));
        if skip {
            if can_descend && continue_scan {
                subdirs.push(crate::dir_tree::SubdirEntry {
                    entry,
                    visible: false,
                    need_filter,
                });
            }
        } else if can_descend {
            subdirs.push(crate::dir_tree::SubdirEntry {
                entry,
                visible: true,
                need_filter,
            });
        } else {
            files.push(entry);
        }
    }
    files.sort_by(|left, right| left.get_name().cmp(right.get_name()));
    subdirs.sort_by(|left, right| left.entry.get_name().cmp(right.entry.get_name()));
    crate::dir_tree::ReadResult {
        dir_path: dir_path.to_string(),
        files,
        subdirs,
        errors: Vec::new(),
    }
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
        crate::TransferConcurrency::defaults(4, 1),
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
    kind: crate::HdfsErrorKind,
    class: Option<&str>,
    diagnostic: &'static str,
    retryable: bool,
) -> StorageError {
    StorageError::HdfsOperation(crate::HdfsOperationError {
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
            crate::HdfsErrorKind::AlreadyExists
        } else if class_has_suffix(class, &["UnsupportedOperationException"]) {
            crate::HdfsErrorKind::Unsupported
        } else {
            crate::HdfsErrorKind::Rpc
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
) -> (crate::HdfsErrorKind, &'static str, bool) {
    use hdfs_native::HdfsError;
    match error {
        HdfsError::AlreadyExists(_) => (
            crate::HdfsErrorKind::AlreadyExists,
            "destination already exists",
            false,
        ),
        HdfsError::BlocksNotFound(_) => (
            crate::HdfsErrorKind::BlocksMissing,
            "file blocks are unavailable",
            true,
        ),
        HdfsError::DataTransferError(_) => (
            crate::HdfsErrorKind::DataTransfer,
            "data transfer failed",
            true,
        ),
        HdfsError::IsADirectoryError(_) => (
            crate::HdfsErrorKind::Directory,
            "target is a directory",
            false,
        ),
        HdfsError::UnsupportedFeature(_)
        | HdfsError::UnsupportedErasureCodingPolicy(_)
        | HdfsError::TrashNotEnabled => (
            crate::HdfsErrorKind::Unsupported,
            "operation is unsupported",
            false,
        ),
        HdfsError::ErasureCodingError(_) => (
            crate::HdfsErrorKind::ErasureCoding,
            "erasure coding operation failed",
            false,
        ),
        HdfsError::SASLError(_) | HdfsError::GSSAPIError(_, _, _) | HdfsError::NoSASLMechanism => (
            crate::HdfsErrorKind::Authentication,
            "authentication failed",
            false,
        ),
        HdfsError::IOError(error) => (
            crate::HdfsErrorKind::Io,
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
            crate::HdfsErrorKind::Internal,
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

#[derive(Deserialize)]
struct HadoopConfiguration {
    #[serde(rename = "property", default)]
    properties: Vec<HadoopProperty>,
}

#[derive(Deserialize)]
struct HadoopProperty {
    name: String,
    value: String,
}

fn validate_authentication(
    settings: &HashMap<String, String>,
    credentials: Option<&HdfsKerberosCredentials>,
) -> Result<(), StorageError> {
    let mode = settings.get("hadoop.security.authentication").map_or_else(
        || {
            if credentials.is_some() {
                "kerberos"
            } else {
                "simple"
            }
        },
        |mode| mode.trim(),
    );
    if mode.eq_ignore_ascii_case("simple") {
        if credentials.is_some() {
            return Err(config_error(
                "HDFS Kerberos credentials require kerberos authentication",
            ));
        }
    } else if mode.eq_ignore_ascii_case("kerberos") {
        if credentials.is_none() {
            return Err(config_error(
                "Kerberos HDFS authentication requires client-scoped credentials",
            ));
        }
    } else {
        return Err(config_error("unsupported HDFS authentication mode"));
    }
    Ok(())
}

fn format_nameservice_endpoint(
    host: &Host<&str>,
    settings: &HashMap<String, String>,
) -> Result<String, StorageError> {
    let Host::Domain(service) = host else {
        return Err(config_error("direct HDFS IP locations require an RPC port"));
    };
    let membership_key = format!("dfs.ha.namenodes.{service}");
    let members = settings
        .get(&membership_key)
        .ok_or_else(|| config_error("HDFS NameService requires explicit HA membership"))?;
    let members = members
        .split(',')
        .map(str::trim)
        .filter(|member| !member.is_empty())
        .collect::<Vec<_>>();
    if members.is_empty() {
        return Err(config_error(
            "HDFS NameService membership must not be empty",
        ));
    }
    for member in members {
        let address_key = format!("dfs.namenode.rpc-address.{service}.{member}");
        if settings
            .get(&address_key)
            .is_none_or(|address| address.trim().is_empty())
        {
            return Err(config_error(
                "every HDFS NameService member requires an RPC address",
            ));
        }
    }
    Ok(format!("hdfs://{service}"))
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    ["password", "secret", "token", "credential", "keytab"]
        .iter()
        .any(|marker| key.contains(marker))
}

fn reject_raw_traversal(location: &str) -> Result<(), StorageError> {
    let Some(authority_start) = location.find("://").map(|index| index + 3) else {
        return Ok(());
    };
    let Some(path_offset) = location[authority_start..].find('/') else {
        return Ok(());
    };
    let raw_path = &location[authority_start + path_offset..];
    let raw_path = raw_path
        .split(['?', '#'])
        .next()
        .ok_or_else(|| config_error("HDFS location contains an invalid path"))?;
    let decoded = decode_component(raw_path, "root path")?;
    if decoded.split('/').any(|component| component == "..") {
        return Err(config_error("HDFS root must not contain traversal"));
    }
    Ok(())
}

fn config_error(message: &str) -> StorageError {
    StorageError::ConfigError(message.to_string())
}

fn validate_percent_encoding(value: &str) -> Result<(), StorageError> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
        {
            return Err(config_error(
                "HDFS location contains malformed percent encoding",
            ));
        }
        index += if bytes[index] == b'%' { 3 } else { 1 };
    }
    Ok(())
}

fn decode_component(value: &str, label: &str) -> Result<String, StorageError> {
    percent_decode_str(value)
        .decode_utf8()
        .map(String::from)
        .map_err(|_| config_error(&format!("HDFS {label} is not valid UTF-8")))
}

fn format_endpoint(host: &Host<&str>, port: u16) -> String {
    match host {
        Host::Domain(domain) => format!("hdfs://{domain}:{port}"),
        Host::Ipv4(address) => format!("hdfs://{address}:{port}"),
        Host::Ipv6(address) => format!("hdfs://[{address}]:{port}"),
    }
}

fn normalize_root(encoded_path: &str) -> Result<String, StorageError> {
    let decoded = decode_component(encoded_path, "root path")?;
    if !decoded.starts_with('/') {
        return Err(config_error("HDFS root must be absolute"));
    }

    let mut components = Vec::new();
    for component in decoded.split('/') {
        match component {
            "" | "." => {}
            ".." => return Err(config_error("HDFS root must not contain traversal")),
            value => components.push(value),
        }
    }

    if components.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}
