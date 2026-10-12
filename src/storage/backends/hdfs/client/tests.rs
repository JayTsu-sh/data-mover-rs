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
    fn append_open_waits_only_for_a_bounded_lease_recovery_window() {
        let lease = hdfs_native::HdfsError::AlreadyExists("lease is still owned".to_string());
        assert_eq!(
            super::append_open_retry_delay(&lease, 0, std::time::Duration::ZERO),
            Some(std::time::Duration::from_secs(1))
        );
        assert_eq!(
            super::append_open_retry_delay(&lease, 5, std::time::Duration::from_secs(31)),
            Some(std::time::Duration::from_secs(8))
        );
        let recovering = hdfs_native::HdfsError::RPCError(
            "org.apache.hadoop.hdfs.protocol.RecoveryInProgressException".to_string(),
            "redacted".to_string(),
        );
        assert_eq!(
            super::append_open_retry_delay(&recovering, 6, std::time::Duration::from_secs(63)),
            Some(std::time::Duration::from_secs(1))
        );
        assert_eq!(
            super::append_open_retry_delay(&recovering, 7, super::HDFS_LEASE_RECOVERY_TIMEOUT),
            None
        );
        let missing = hdfs_native::HdfsError::FileNotFound("missing".to_string());
        assert_eq!(
            super::append_open_retry_delay(&missing, 0, std::time::Duration::ZERO),
            None
        );
    }

    #[test]
    fn explicit_lease_recovery_polling_is_bounded() {
        assert_eq!(
            super::lease_recovery_poll_delay(std::time::Duration::ZERO),
            Some(std::time::Duration::from_millis(250))
        );
        assert_eq!(
            super::lease_recovery_poll_delay(std::time::Duration::from_millis(119_750)),
            None
        );
        assert_eq!(
            super::lease_recovery_poll_delay(super::HDFS_LEASE_RECOVERY_TIMEOUT),
            None
        );
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
            Err(StorageError::HdfsOperation(crate::error::HdfsOperationError {
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
        assert_eq!(details.kind, crate::error::HdfsErrorKind::Rpc);
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
                StorageError::HdfsOperation(crate::error::HdfsOperationError {
                    retryable: true,
                    ..
                })
            ));
            assert!(!error.to_string().contains("secret"));
        }
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
            kerberos_credentials: Some(HdfsKerberosCredentials {
                keytab: Some("/run/secrets/source.keytab".into()),
                ..Default::default()
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
        assert!(debug.contains("HdfsKerberosCredentials"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("/run/secrets/source.keytab"));
    }

    #[test]
    fn kerberos_cache_and_simple_authentication_cannot_be_mixed() {
        let config = HdfsConfig {
            overrides: HashMap::from([(
                "hadoop.security.authentication".to_string(),
                "simple".to_string(),
            )]),
            kerberos_credentials: Some(HdfsKerberosCredentials {
                cache: Some("FILE:/run/krb5/source.ccache".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            HdfsLocation::parse_configured("hdfs://source@namenode:9000/root", &config).is_err()
        );
        assert!(!format!("{config:?}").contains("/run/krb5/source.ccache"));
    }

    #[test]
    fn kerberos_credentials_redact_keytab_and_cache_together() {
        let credentials = HdfsKerberosCredentials {
            principal: Some("source/client@SOURCE.EXAMPLE".to_string()),
            keytab: Some("/run/secrets/source.keytab".into()),
            cache: Some("FILE:/run/krb5/source.ccache".to_string()),
        };

        let debug = format!("{credentials:?}");
        assert!(debug.contains("source/client@SOURCE.EXAMPLE"));
        assert!(!debug.contains("/run/secrets/source.keytab"));
        assert!(!debug.contains("/run/krb5/source.ccache"));
    }

    #[test]
    fn kerberos_credentials_require_keytab_or_cache() {
        let config = HdfsConfig {
            kerberos_credentials: Some(HdfsKerberosCredentials {
                principal: Some("source/client@SOURCE.EXAMPLE".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = build_hdfs_client("hdfs://source@namenode:9000/root", &config);
        assert!(result.is_err());
    }

    #[test]
    fn combined_keytab_and_cache_are_accepted() {
        let config = HdfsConfig {
            kerberos_credentials: Some(HdfsKerberosCredentials {
                keytab: Some("/run/secrets/source.keytab".into()),
                cache: Some("FILE:/run/krb5/source.ccache".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = build_hdfs_client("hdfs://source@namenode:9000/root", &config);
        assert!(result.is_ok());
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
        let Ok(output) = Command::new(executable)
            .args([
                "--exact",
                concat!(
                    module_path!(),
                    "::explicit_configuration_ignores_poisoned_hadoop_environment"
                )
                .trim_start_matches(concat!(env!("CARGO_CRATE_NAME"), "::")),
            ])
            .env(CHILD_MARKER, "1")
            .env("HADOOP_CONF_DIR", "/poisoned/hadoop/conf")
            .env("HADOOP_HOME", "/poisoned/hadoop/home")
            .env("HADOOP_USER_NAME", "wrong-user")
            .env("HADOOP_PROXY_USER", "wrong-proxy")
            .env("KRB5CCNAME", "/poisoned/krb5cc")
            .env("HADOOP_TOKEN_FILE_LOCATION", "/poisoned/token")
            .output()
        else {
            panic!("failed to start the isolated environment test");
        };
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("test result: ok. 1 passed;"),
            "isolated environment test did not execute: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}
