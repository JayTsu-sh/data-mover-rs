#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_util::sync::CancellationToken;

    use super::{
        HdfsConfig, HdfsEndpointKind, HdfsKerberosCredentials, HdfsLocation, build_hdfs_client,
    };
    use crate::error::StorageError;

    #[test]
    fn transfer_chunks_are_positive_and_do_not_change_hdfs_block_size() {
        assert_eq!(super::transfer_chunk_size(0), 1);
        assert_eq!(super::transfer_chunk_size(crate::MB), crate::MB);
        assert_eq!(
            super::transfer_chunk_size(super::DEFAULT_BLOCK_SIZE),
            super::MAX_TRANSFER_CHUNK_SIZE
        );
        assert_eq!(super::DEFAULT_BLOCK_SIZE, 8 * crate::MB);
    }

    #[tokio::test]
    async fn adapter_retry_is_bounded_and_eventually_succeeds() {
        assert_eq!(super::HDFS_ADAPTER_MAX_ATTEMPTS, 5);
        assert_eq!(
            super::HDFS_ADAPTER_ATTEMPT_TIMEOUT,
            std::time::Duration::from_secs(5)
        );
        assert_eq!(
            super::HDFS_ADAPTER_RETRY_DELAYS
                .iter()
                .sum::<std::time::Duration>(),
            std::time::Duration::from_millis(3_750)
        );
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let started = std::time::Instant::now();
        let result =
            super::retry_hdfs_read("test read", Some(Path::new("item")), None, move || {
                let attempt = observed.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(hdfs_native::HdfsError::IOError(std::io::Error::from(
                            std::io::ErrorKind::TimedOut,
                        )))
                    } else {
                        Ok(7_u8)
                    }
                }
            })
            .await;
        assert_eq!(result.ok(), Some(7));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(started.elapsed() >= std::time::Duration::from_millis(750));
    }

    #[tokio::test]
    async fn adapter_retry_short_circuits_permanent_errors() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let result = super::retry_hdfs_read::<(), _, _>(
            "test read",
            Some(Path::new("missing")),
            None,
            move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async {
                    Err(hdfs_native::HdfsError::FileNotFound(
                        "/absolute/missing".to_string(),
                    ))
                }
            },
        )
        .await;
        assert!(matches!(result, Err(StorageError::FileNotFound(path)) if path == "missing"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn adapter_retry_exhaustion_preserves_structured_error() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let result = super::retry_hdfs_read::<(), _, _>(
            "open file",
            Some(Path::new("item")),
            None,
            move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async {
                    Err(hdfs_native::HdfsError::DataTransferError(
                        "ignored".to_string(),
                    ))
                }
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(StorageError::HdfsOperation(crate::HdfsOperationError {
                operation: "open file",
                relative_path: Some(path),
                retryable: true,
                ..
            })) if path == Path::new("item")
        ));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            super::HDFS_ADAPTER_MAX_ATTEMPTS
        );
    }

    #[tokio::test]
    async fn adapter_retry_cancellation_stops_wait_and_inflight_attempt() {
        let token = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            cancel.cancel();
        });
        let result =
            super::retry_hdfs_read::<(), _, _>("test read", None, Some(&token), move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async {
                    Err(hdfs_native::HdfsError::DataTransferError(
                        "ignored".to_string(),
                    ))
                }
            })
            .await;
        assert!(matches!(result, Err(StorageError::Cancelled)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancel.cancel();
        });
        let result =
            super::retry_hdfs_read::<(), _, _>("test read", None, Some(&token), || async {
                std::future::pending::<Result<(), hdfs_native::HdfsError>>().await
            })
            .await;
        assert!(matches!(result, Err(StorageError::Cancelled)));
    }

    #[test]
    fn parses_and_normalizes_direct_locations() {
        let cases = [
            (
                "hdfs://alice@namenode.example:9000/data//./incoming/",
                "hdfs://namenode.example:9000",
                "alice",
                "/data/incoming",
            ),
            (
                "hdfs://data%20mover@10.131.9.30:9000/%E6%95%B0%E6%8D%AE",
                "hdfs://10.131.9.30:9000",
                "data mover",
                "/数据",
            ),
            (
                "hdfs://alice@[2001:db8::1]:8020/",
                "hdfs://[2001:db8::1]:8020",
                "alice",
                "/",
            ),
        ];

        for (input, endpoint, user, root) in cases {
            let Ok(parsed) = HdfsLocation::parse(input) else {
                panic!("valid direct HDFS URL was rejected: {input}");
            };
            assert_eq!(parsed.endpoint(), endpoint);
            assert_eq!(parsed.user(), user);
            assert_eq!(parsed.root(), root);
        }
    }

    #[test]
    fn rejects_unsupported_or_ambiguous_locations() {
        let invalid = [
            "viewfs://alice@namenode:9000/",
            "hdfs://namenode:9000/",
            "hdfs://@namenode:9000/",
            "hdfs://alice:secret@namenode:9000/",
            "hdfs://alice@namenode/",
            "hdfs://alice@:9000/",
            "hdfs://alice@namenode:9000/root?x=1",
            "hdfs://alice@namenode:9000/root#fragment",
            "hdfs://alice@namenode:9000/root/%2e%2e/outside",
            "hdfs://alice@namenode:9000/root/%ZZ",
            "hdfs://%FF@namenode:9000/",
        ];

        for input in invalid {
            assert!(HdfsLocation::parse(input).is_err(), "accepted {input}");
        }
    }

    #[test]
    fn errors_do_not_echo_rejected_userinfo() {
        let input = "hdfs://alice:do-not-log@namenode:9000/root";
        let Err(error) = HdfsLocation::parse(input) else {
            panic!("password was accepted");
        };
        assert!(!error.to_string().contains("do-not-log"));
    }

    #[test]
    fn storage_root_resolution_cannot_escape() {
        let Ok((client, location)) = build_hdfs_client(
            "hdfs://user@127.0.0.1:9000/isolated/root",
            &HdfsConfig::default(),
        ) else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        let Ok(resolved) = storage.resolve_path(Path::new("a/./b")) else {
            panic!("valid relative path was rejected");
        };
        assert_eq!(resolved, "/isolated/root/a/b");
        assert!(storage.resolve_path(Path::new("../outside")).is_err());
        assert!(storage.resolve_path(Path::new("/outside")).is_err());
    }

    #[test]
    fn status_conversion_preserves_hdfs_metadata_and_common_accessors() {
        let Ok((client, location)) = build_hdfs_client(
            "hdfs://user@127.0.0.1:9000/isolated/root",
            &HdfsConfig::default(),
        ) else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        let status = hdfs_native::client::FileStatus {
            path: "/isolated/root/目录/report.txt".to_string(),
            length: 42,
            isdir: false,
            permission: 0o640,
            owner: "alice".to_string(),
            group: "analytics".to_string(),
            modification_time: 1_700_000_000_123,
            access_time: 1_700_000_001_456,
            replication: Some(3),
            blocksize: Some(128 * crate::MB),
        };
        let Ok(entry) = storage.entry_from_status(status) else {
            panic!("valid status conversion failed");
        };
        assert_eq!(entry.relative_path, Path::new("目录/report.txt"));
        assert_eq!(entry.name, "report.txt");
        assert_eq!(entry.extension.as_deref(), Some("txt"));
        assert_eq!(entry.owner, "alice");
        assert_eq!(entry.group, "analytics");
        assert_eq!(entry.replication, Some(3));
        assert_eq!(entry.block_size, Some(128 * crate::MB));
        assert_eq!(entry.mtime, 1_700_000_000_123_000_000);
        assert_eq!(entry.atime, 1_700_000_001_456_000_000);

        let common = crate::EntryEnum::HDFS(entry);
        assert_eq!(common.get_name(), "report.txt");
        assert_eq!(common.get_relative_path(), Path::new("目录/report.txt"));
        assert_eq!(common.get_size(), 42);
        assert!(!common.get_is_dir());
        assert!(common.get_is_regular_file());
        assert!(!common.get_is_symlink());
        assert_eq!(common.get_mode(), Some(0o640));
        assert_eq!(common.get_uid(), None);
        assert_eq!(common.get_gid(), None);
    }

    #[test]
    fn status_conversion_enforces_root_component_boundary() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        let status = hdfs_native::client::FileStatus {
            path: "/rooted/file".to_string(),
            length: 0,
            isdir: true,
            permission: 0o755,
            owner: String::new(),
            group: String::new(),
            modification_time: 0,
            access_time: 0,
            replication: None,
            blocksize: None,
        };
        assert!(storage.entry_from_status(status).is_err());
    }

    #[test]
    fn missing_upstream_path_keeps_not_found_classification() {
        let error = super::hdfs_operation_error(
            "get metadata",
            Some(Path::new("missing")),
            &hdfs_native::HdfsError::FileNotFound("/isolated/root/missing".to_string()),
        );
        assert!(matches!(
            error,
            crate::error::StorageError::FileNotFound(path)
                if path == "missing"
        ));
    }

    #[test]
    fn rpc_classes_map_without_inspecting_messages() {
        for (class, expected) in [
            ("org.apache.hadoop.fs.FileNotFoundException", "not-found"),
            (
                "org.apache.hadoop.security.AccessControlException",
                "permission",
            ),
            ("org.apache.hadoop.fs.ChecksumException", "checksum"),
            (
                "org.apache.hadoop.util.DiskChecker$DiskOutOfSpaceException",
                "space",
            ),
        ] {
            let error = super::hdfs_operation_error(
                "open file",
                Some(Path::new("safe.bin")),
                &hdfs_native::HdfsError::RPCError(
                    class.to_string(),
                    "secret message with hdfs://user:password@host/absolute".to_string(),
                ),
            );
            assert!(
                match expected {
                    "not-found" => matches!(error, StorageError::FileNotFound(_)),
                    "permission" => matches!(error, StorageError::PermissionDenied(_)),
                    "checksum" => matches!(error, StorageError::ChecksumError(_)),
                    "space" => matches!(error, StorageError::InsufficientSpace(_)),
                    _ => false,
                },
                "class {class} mapped to {error:?}"
            );
            let displayed = error.to_string();
            assert!(!displayed.contains("password"));
            assert!(!displayed.contains("/absolute"));
        }
    }

    #[test]
    fn unknown_rpc_error_is_structured_conservative_and_redacted() {
        let error = super::hdfs_operation_error(
            "rename",
            Some(Path::new("relative/source")),
            &hdfs_native::HdfsError::RPCError(
                "vendor.CustomFailure".to_string(),
                "token=secret /outside/root".to_string(),
            ),
        );
        let StorageError::HdfsOperation(details) = &error else {
            panic!("unknown RPC error lost structured HDFS context");
        };
        assert_eq!(details.operation, "rename");
        assert_eq!(
            details.relative_path.as_deref(),
            Some(Path::new("relative/source"))
        );
        assert_eq!(details.kind, crate::HdfsErrorKind::Rpc);
        assert_eq!(
            details.hadoop_class.as_deref(),
            Some("vendor.CustomFailure")
        );
        assert!(!details.retryable);
        let displayed = error.to_string();
        assert!(displayed.contains("relative/source"));
        assert!(!displayed.contains("secret"));
        assert!(!displayed.contains("/outside/root"));
    }

    #[test]
    fn malformed_rpc_class_is_not_reflected_in_diagnostics() {
        let error = super::hdfs_operation_error(
            "list directory",
            None,
            &hdfs_native::HdfsError::FatalRPCError(
                "bad\nclass hdfs://user:secret@host".to_string(),
                "ignored".to_string(),
            ),
        );
        let StorageError::HdfsOperation(details) = error else {
            panic!("fatal RPC error lost structured HDFS context");
        };
        assert!(details.hadoop_class.is_none());
        assert!(!details.retryable);
    }

    #[test]
    fn transient_io_and_data_transfer_errors_are_the_only_retryable_samples() {
        let io = super::hdfs_operation_error(
            "read range",
            Some(Path::new("file")),
            &hdfs_native::HdfsError::IOError(std::io::Error::from(std::io::ErrorKind::TimedOut)),
        );
        let transfer = super::hdfs_operation_error(
            "write data",
            Some(Path::new("file")),
            &hdfs_native::HdfsError::DataTransferError("secret".to_string()),
        );
        for error in [io, transfer] {
            assert!(matches!(
                &error,
                StorageError::HdfsOperation(crate::HdfsOperationError {
                    retryable: true,
                    ..
                })
            ));
            assert!(!error.to_string().contains("secret"));
        }
    }

    #[test]
    fn single_directory_listing_conversion_is_bounded_and_root_relative() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        assert!(
            storage
                .entries_from_listing(Vec::new(), Path::new("目录"))
                .is_ok_and(|entries| entries.is_empty())
        );

        let status = |path: &str, isdir| hdfs_native::client::FileStatus {
            path: path.to_string(),
            length: 0,
            isdir,
            permission: 0o755,
            owner: "user".to_string(),
            group: "group".to_string(),
            modification_time: 0,
            access_time: 0,
            replication: None,
            blocksize: None,
        };
        let Ok(entries) = storage.entries_from_listing(
            vec![
                status("/root/目录/file.txt", false),
                status("/root/目录/subdir", true),
            ],
            Path::new("目录"),
        ) else {
            panic!("valid immediate children were rejected");
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].relative_path, Path::new("目录/file.txt"));
        assert_eq!(entries[1].relative_path, Path::new("目录/subdir"));
        assert!(entries[1].is_dir);

        assert!(
            storage
                .entries_from_listing(
                    vec![status("/root/目录/subdir/nested", false)],
                    Path::new("目录"),
                )
                .is_err()
        );
        assert!(
            storage
                .entries_from_listing(vec![status("/rooted/outside", false)], Path::new("目录"),)
                .is_err()
        );
    }

    #[test]
    fn recursive_scanner_validates_concurrency_and_root() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        assert!(storage.scan_recursive(None, None, 0).is_err());
        assert!(
            storage
                .scan_recursive(None, None, crate::TransferConcurrency::MAX + 1)
                .is_err()
        );
        assert!(
            storage
                .scan_recursive(Some(Path::new("../outside")), None, 1)
                .is_err()
        );
        assert!(
            storage
                .walkdir(
                    None,
                    crate::storage_enum::WalkOptions {
                        packaged: true,
                        ..Default::default()
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn hdfs_filter_adapter_uses_truthful_entry_fields() {
        let entry = crate::HDFSEntry {
            name: "报告.txt".to_string(),
            relative_path: std::path::PathBuf::from("目录/报告.txt"),
            is_dir: false,
            size: 42,
            mtime: 1_700_000_000_000_000_000,
            atime: 0,
            mode: 0o640,
            owner: "alice".to_string(),
            group: "users".to_string(),
            replication: Some(3),
            block_size: Some(128 * crate::MB),
            extension: Some("txt".to_string()),
        };
        let Ok(include) = crate::filter::parse_filter_expression("extension == \"txt\"") else {
            panic!("valid include filter was rejected");
        };
        let Ok(exclude) = crate::filter::parse_filter_expression("path == \"目录/**\"") else {
            panic!("valid exclude filter was rejected");
        };
        assert!(!super::hdfs_entry_is_filtered(&entry, Some(&include), None));
        assert!(super::hdfs_entry_is_filtered(
            &entry,
            Some(&include),
            Some(&exclude)
        ));
    }

    #[test]
    fn positional_read_planning_truncates_at_eof_without_overflow() {
        assert_eq!(super::plan_read_range(10, 0, 0).ok(), Some(None));
        assert_eq!(super::plan_read_range(10, 10, 4).ok(), Some(None));
        assert_eq!(super::plan_read_range(10, 12, u64::MAX).ok(), Some(None));
        assert_eq!(super::plan_read_range(10, 2, 4).ok(), Some(Some((2, 4))));
        assert_eq!(
            super::plan_read_range(10, 8, u64::MAX).ok(),
            Some(Some((8, 2)))
        );
    }

    #[test]
    fn sequential_writer_rejects_overlap_duplicate_and_oversize_chunks() {
        let mut pending = std::collections::BTreeMap::new();
        pending.insert(4, bytes::Bytes::from_static(b"efgh"));
        let chunk = |offset, data| crate::DataChunk {
            offset,
            data: bytes::Bytes::from_static(data),
        };
        assert!(super::validate_write_chunk(&pending, 0, 12, &chunk(0, b"abcd")).is_ok());
        assert!(super::validate_write_chunk(&pending, 0, 12, &chunk(4, b"efgh")).is_err());
        assert!(super::validate_write_chunk(&pending, 0, 12, &chunk(2, b"cdef")).is_err());
        assert!(super::validate_write_chunk(&pending, 0, 12, &chunk(7, b"hijk")).is_err());
        assert!(super::validate_write_chunk(&pending, 4, 12, &chunk(0, b"abcd")).is_err());
        assert!(super::validate_write_chunk(&pending, 0, 12, &chunk(10, b"wxyz")).is_err());
    }

    #[test]
    fn resumable_append_may_stop_at_a_durable_contiguous_prefix() {
        assert!(super::validate_sequential_end(8, 16, false, false).is_ok());
        assert!(super::validate_sequential_end(16, 16, false, true).is_ok());
        assert!(super::validate_sequential_end(8, 16, false, true).is_err());
        assert!(super::validate_sequential_end(8, 16, true, false).is_err());
    }

    #[tokio::test]
    async fn delete_primitives_reject_escape_and_protect_root_before_io() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        assert!(storage.delete_dir_all(Path::new("")).await.is_err());
        assert!(storage.delete_file(Path::new("../outside")).await.is_err());
        assert!(storage.delete_dir_all(Path::new("/outside")).await.is_err());
    }

    #[tokio::test]
    async fn metadata_primitives_reject_root_escape_and_invalid_values_before_io() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };

        assert!(storage.set_permission(Path::new(""), 0o640).await.is_err());
        assert!(
            storage
                .set_permission(Path::new("../outside"), 0o640)
                .await
                .is_err()
        );
        assert!(
            storage
                .set_permission(Path::new("file"), 0o10_000)
                .await
                .is_err()
        );
        assert!(storage.set_mtime(Path::new("file"), -1).await.is_err());
        assert!(
            storage
                .set_owner_group(Path::new("/outside"), Some("alice"), Some("users"))
                .await
                .is_err()
        );
        assert!(
            storage
                .set_owner_group(Path::new(""), None, None)
                .await
                .is_err()
        );
        assert!(
            storage
                .set_owner_group(Path::new("missing"), Some(""), None)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn rename_rejects_roots_and_escape_before_io() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        assert!(
            storage
                .rename(Path::new(""), Path::new("to"))
                .await
                .is_err()
        );
        assert!(
            storage
                .rename(Path::new("from"), Path::new(""))
                .await
                .is_err()
        );
        assert!(
            storage
                .rename(Path::new("../from"), Path::new("to"))
                .await
                .is_err()
        );
        assert!(
            storage
                .rename(Path::new("from"), Path::new("/to"))
                .await
                .is_err()
        );
    }

    #[test]
    fn walkdir2_reader_result_is_sorted_and_preserves_hidden_descent() {
        let entry = |path: &str, is_dir: bool| crate::HDFSEntry {
            name: Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string(),
            relative_path: std::path::PathBuf::from(path),
            is_dir,
            size: 0,
            mtime: 0,
            atime: 0,
            mode: 0o755,
            owner: String::new(),
            group: String::new(),
            replication: None,
            block_size: None,
            extension: Path::new(path)
                .extension()
                .and_then(|value| value.to_str())
                .map(str::to_string),
        };
        let Ok(include) = crate::filter::parse_filter_expression("name == \"*.txt\"") else {
            panic!("valid filter rejected");
        };
        let ctx = crate::dir_tree::ReadContext {
            match_expr: std::sync::Arc::new(Some(include)),
            exclude_expr: std::sync::Arc::new(None),
            current_depth: 0,
            max_depth: 3,
            apply_filter: true,
            include_tags: false,
            is_versioned: false,
        };
        let result = super::hdfs_read_result(
            "",
            vec![
                entry("z.txt", false),
                entry("keep/乙", true),
                entry("keep/甲", true),
                entry("a.txt", false),
            ],
            &ctx,
        );
        assert_eq!(result.files.len(), 2);
        assert_eq!(result.files[0].get_name(), "a.txt");
        assert_eq!(result.files[1].get_name(), "z.txt");
        assert_eq!(result.subdirs.len(), 2);
        assert_eq!(result.subdirs[0].entry.get_name(), "乙");
        assert_eq!(result.subdirs[1].entry.get_name(), "甲");
        assert!(result.subdirs.iter().all(|subdir| !subdir.visible));
        assert!(result.subdirs.iter().all(|subdir| subdir.need_filter));

        let boundary = crate::dir_tree::ReadContext {
            current_depth: 2,
            ..ctx
        };
        let result = super::hdfs_read_result("keep", vec![entry("keep/子目录", true)], &boundary);
        assert!(result.subdirs.is_empty());
    }

    #[tokio::test]
    async fn walkdir2_reader_rejects_non_hdfs_handle_before_io() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        let ctx = crate::dir_tree::ReadContext {
            match_expr: std::sync::Arc::new(None),
            exclude_expr: std::sync::Arc::new(None),
            current_depth: 0,
            max_depth: 0,
            apply_filter: true,
            include_tags: false,
            is_versioned: false,
        };
        let result = storage
            .read_dir_sorted(
                "",
                &crate::dir_tree::DirHandle::Local(std::path::PathBuf::from("/")),
                &ctx,
            )
            .await;
        assert!(matches!(
            result,
            Err(crate::error::StorageError::MismatchedType)
        ));
        assert!(storage.walkdir_2(None, None, None, None, 0).is_err());
        assert!(
            storage
                .walkdir_2(Some(Path::new("../outside")), None, None, None, 1,)
                .is_err()
        );
        assert!(
            storage
                .create_dir_all(Path::new("../outside"), 0o755)
                .await
                .is_err()
        );
    }

    #[test]
    fn parses_nameservice_from_explicit_complete_overrides() {
        let config = HdfsConfig {
            config_dir: None,
            overrides: HashMap::from([
                (
                    "dfs.ha.namenodes.analytics".to_string(),
                    " nn1, nn2 ".to_string(),
                ),
                (
                    "dfs.namenode.rpc-address.analytics.nn1".to_string(),
                    "10.0.0.1:8020".to_string(),
                ),
                (
                    "dfs.namenode.rpc-address.analytics.nn2".to_string(),
                    "10.0.0.2:8020".to_string(),
                ),
                (
                    "hadoop.security.authentication".to_string(),
                    "simple".to_string(),
                ),
            ]),
            ..Default::default()
        };
        let Ok(location) =
            HdfsLocation::parse_configured("hdfs://migration@analytics/warehouse", &config)
        else {
            panic!("complete explicit NameService configuration was rejected");
        };
        assert_eq!(location.endpoint(), "hdfs://analytics");
        assert_eq!(location.kind(), HdfsEndpointKind::NameService);
        assert_eq!(location.user(), "migration");
        assert_eq!(location.root(), "/warehouse");
    }

    #[test]
    fn rejects_incomplete_nameservice_and_non_simple_authentication() {
        let incomplete = HdfsConfig {
            config_dir: None,
            overrides: HashMap::from([(
                "dfs.ha.namenodes.analytics".to_string(),
                "nn1,nn2".to_string(),
            )]),
            ..Default::default()
        };
        assert!(HdfsLocation::parse_configured("hdfs://user@analytics/", &incomplete).is_err());

        let kerberos = HdfsConfig {
            config_dir: None,
            overrides: HashMap::from([(
                "hadoop.security.authentication".to_string(),
                "kerberos".to_string(),
            )]),
            ..Default::default()
        };
        assert!(HdfsLocation::parse_configured("hdfs://user@namenode:9000/", &kerberos).is_err());
    }

    #[test]
    fn overrides_take_precedence_over_explicit_xml() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        let directory = std::env::temp_dir().join(format!("data-mover-hdfs-config-{suffix}"));
        let Ok(()) = fs::create_dir(&directory) else {
            panic!("failed to create test configuration directory");
        };
        let xml = r"<configuration>
          <property><name>dfs.ha.namenodes.analytics</name><value>old</value></property>
          <property><name>dfs.namenode.rpc-address.analytics.old</name><value>old:8020</value></property>
        </configuration>";
        let Ok(()) = fs::write(directory.join("hdfs-site.xml"), xml) else {
            panic!("failed to write test configuration");
        };
        let config = HdfsConfig {
            config_dir: Some(directory.clone()),
            overrides: HashMap::from([
                ("dfs.ha.namenodes.analytics".to_string(), "new".to_string()),
                (
                    "dfs.namenode.rpc-address.analytics.new".to_string(),
                    "new:8020".to_string(),
                ),
            ]),
            ..Default::default()
        };
        assert!(HdfsLocation::parse_configured("hdfs://user@analytics/", &config).is_ok());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn debug_redacts_sensitive_override_values() {
        let config = HdfsConfig {
            config_dir: None,
            overrides: HashMap::from([
                ("service.token".to_string(), "do-not-log".to_string()),
                ("safe.setting".to_string(), "visible".to_string()),
            ]),
            ..Default::default()
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("do-not-log"));
        assert!(debug.contains("service.token"));
        assert!(debug.contains("visible"));
    }

    #[test]
    fn kerberos_keytab_uses_url_principal_and_redacts_secret_path() {
        let config = HdfsConfig {
            kerberos_credentials: Some(HdfsKerberosCredentials::Keytab {
                keytab: "/run/secrets/source.keytab".into(),
            }),
            ..Default::default()
        };
        let location = "hdfs://source%2Fclient%40SOURCE.EXAMPLE@namenode:9000/root";
        let Ok((client, parsed)) = build_hdfs_client(location, &config) else {
            panic!("client-scoped keytab configuration was rejected");
        };
        drop(client);
        assert_eq!(parsed.user(), "source/client@SOURCE.EXAMPLE");
        let debug = format!("{config:?}");
        assert!(debug.contains("Keytab"));
        assert!(!debug.contains("/run/secrets/source.keytab"));
    }

    #[test]
    fn kerberos_cache_and_simple_authentication_cannot_be_mixed() {
        let config = HdfsConfig {
            overrides: HashMap::from([(
                "hadoop.security.authentication".to_string(),
                "simple".to_string(),
            )]),
            kerberos_credentials: Some(HdfsKerberosCredentials::CredentialCache {
                cache: "FILE:/run/krb5/source.ccache".to_string(),
            }),
            ..Default::default()
        };
        assert!(
            HdfsLocation::parse_configured("hdfs://source@namenode:9000/root", &config).is_err()
        );
        assert!(!format!("{config:?}").contains("/run/krb5/source.ccache"));
    }

    #[test]
    fn explicit_configuration_ignores_poisoned_hadoop_environment() {
        const CHILD_MARKER: &str = "DATA_MOVER_HDFS_ENV_TEST_CHILD";
        if std::env::var_os(CHILD_MARKER).is_some() {
            let result = build_hdfs_client(
                "hdfs://explicit@127.0.0.1:9000/isolated",
                &HdfsConfig::default(),
            );
            assert!(result.is_ok());
            return;
        }

        let Ok(executable) = std::env::current_exe() else {
            panic!("failed to locate the unit-test executable");
        };
        let Ok(status) = Command::new(executable)
            .args([
                "--exact",
                "hdfs::tests::explicit_configuration_ignores_poisoned_hadoop_environment",
            ])
            .env(CHILD_MARKER, "1")
            .env("HADOOP_CONF_DIR", "/poisoned/hadoop/conf")
            .env("HADOOP_HOME", "/poisoned/hadoop/home")
            .env("HADOOP_USER_NAME", "wrong-user")
            .env("HADOOP_PROXY_USER", "wrong-proxy")
            .env("KRB5CCNAME", "/poisoned/krb5cc")
            .env("HADOOP_TOKEN_FILE_LOCATION", "/poisoned/token")
            .status()
        else {
            panic!("failed to start the isolated environment test");
        };
        assert!(status.success());
    }
}
