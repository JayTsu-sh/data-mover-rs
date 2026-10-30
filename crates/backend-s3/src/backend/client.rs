use std::sync::OnceLock;
use std::time::Duration;

use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client;
use aws_sdk_s3::types::CompletedPart;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use aws_types::region::Region;
use data_mover_storage_core::storage::config::s3::{
    ParsedS3Url, parse_s3_endpoint_url, parse_s3_url,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::Result;
use crate::error::StorageError;
use crate::storage::backends::s3::S3VersioningAtConnect;
use tls::{build_skip_verify_http_client, untrusted_certificate, warn_unverified};
use url_options::{S3BucketType, S3Compatibility, S3UrlOptions};

mod architecture;
mod copy_request;
#[cfg(test)]
mod credential_log_tests;
mod delete_objects_md5;
#[cfg(test)]
mod directory_object_write_tests;
mod dxn;
mod qos_client;
mod storagegrid;
mod tls;
mod url_options;
#[cfg(test)]
mod url_options_tests;
mod version_order;
mod versioning_answer;

/// S3 桶信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BucketInfo {
    /// 桶名称
    pub name: String,
    /// 创建时间（ISO 8601 格式）
    pub creation_date: Option<String>,
}

fn versioning_is_unsupported(error_code: Option<&str>, status: Option<u16>) -> bool {
    matches!(error_code, Some("NotImplemented" | "UnsupportedOperation")) || status == Some(501)
}

/// Build the value for the `x-amz-copy-source` request header.
///
/// The bucket and encoded key are appended into one allocation. Path separators
/// and RFC 3986 unreserved bytes in the key remain readable. Every other byte,
/// including each byte of a UTF-8 sequence, is percent-encoded. Encoding a
/// literal `%` is important because object keys are not pre-encoded URLs.
fn build_copy_source(bucket: &str, key: &str) -> String {
    let mut copy_source = String::with_capacity(bucket.len() + 1 + key.len());
    copy_source.push_str(bucket);
    copy_source.push('/');

    for &byte in key.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                copy_source.push(byte as char);
            }
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                copy_source.push('%');
                copy_source.push(HEX[(byte >> 4) as usize] as char);
                copy_source.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    copy_source
}

fn build_s3_config(sdk_config: &SdkConfig, compatibility: S3Compatibility) -> aws_sdk_s3::Config {
    let builder = aws_sdk_s3::config::Builder::from(sdk_config)
        .force_path_style(true)
        .request_checksum_calculation(aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired);
    configure_compatibility(builder, compatibility).build()
}

/// Per-profile request adjustments. Every profile signs a body-matching Content-MD5 into
/// `DeleteObjects`; `StorageGRID` also strips `x-id`.
fn configure_compatibility(
    builder: aws_sdk_s3::config::Builder,
    compatibility: S3Compatibility,
) -> aws_sdk_s3::config::Builder {
    match compatibility {
        S3Compatibility::Standard | S3Compatibility::Ontap => {
            delete_objects_md5::configure(builder)
        }
        S3Compatibility::StorageGrid => storagegrid::configure(builder),
        S3Compatibility::Dxn => dxn::configure(builder),
    }
}

/// Connected SDK client and bucket configuration used only by the storage roles.
#[derive(Clone, Debug)]
pub struct S3Storage {
    transfer_concurrency: crate::transfer_concurrency::TransferConcurrency,
    read_chunk_bytes: usize,
    /// What the URL stated besides its credentials and location.
    options: S3UrlOptions,
    pub endpoint: String,
    bucket_name: String,
    prefix: Option<String>,
    client: Client,
    single_attempt_client: OnceLock<Client>,
    http_client_capture: qos_client::HttpClientCapture,
    pub is_bucket_versioned: bool,
    /// `GetBucketVersioning` answered `Suspended` when the bucket was connected: a write there
    /// creates the `"null"` version, whether or not its reply says so (`RustFS` names none). A
    /// snapshot, like `is_bucket_versioned`: changing the bucket's versioning while connected is
    /// outside the contract.
    versioning_suspended: bool,
    /// What `GetBucketVersioning` answered at connection, whether the role-based listing gives
    /// each key its history (P7 Q-A). The role connection
    /// refuses a bucket it got no answer about.
    versioning_at_connect: S3VersioningAtConnect,
}

const S3_CONNECT_TIMEOUT_ENV: &str = "S3_CONNECT_TIMEOUT";
const S3_OPERATION_TIMEOUT_ENV: &str = "S3_OPERATION_TIMEOUT";
const S3_READ_TIMEOUT_ENV: &str = "S3_READ_TIMEOUT";
const DEFAULT_S3_CONNECT_TIMEOUT_SECS: u64 = 10;
const DEFAULT_S3_OPERATION_TIMEOUT_SECS: u64 = 30;
const DEFAULT_S3_READ_TIMEOUT_SECS: u64 = 20;

fn parse_s3_timeout_secs(name: &str, value: Option<&str>, default: u64) -> u64 {
    let Some(value) = value else {
        return default;
    };

    match value.parse::<u64>() {
        Ok(seconds) if seconds > 0 => seconds,
        _ => {
            warn!(
                variable = name,
                value, default, "无效的 S3 超时配置，将使用默认值（秒）"
            );
            default
        }
    }
}

fn s3_timeout_config_from_values(
    connect: Option<&str>,
    operation: Option<&str>,
    read: Option<&str>,
) -> TimeoutConfig {
    TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(parse_s3_timeout_secs(
            S3_CONNECT_TIMEOUT_ENV,
            connect,
            DEFAULT_S3_CONNECT_TIMEOUT_SECS,
        )))
        .operation_timeout(Duration::from_secs(parse_s3_timeout_secs(
            S3_OPERATION_TIMEOUT_ENV,
            operation,
            DEFAULT_S3_OPERATION_TIMEOUT_SECS,
        )))
        .read_timeout(Duration::from_secs(parse_s3_timeout_secs(
            S3_READ_TIMEOUT_ENV,
            read,
            DEFAULT_S3_READ_TIMEOUT_SECS,
        )))
        .build()
}

fn s3_timeout_config() -> TimeoutConfig {
    let connect = std::env::var(S3_CONNECT_TIMEOUT_ENV).ok();
    let operation = std::env::var(S3_OPERATION_TIMEOUT_ENV).ok();
    let read = std::env::var(S3_READ_TIMEOUT_ENV).ok();

    s3_timeout_config_from_values(connect.as_deref(), operation.as_deref(), read.as_deref())
}

impl S3Storage {
    /// Builds connected roles with a configured single-PUT threshold. Sources of at most
    /// `single_put_threshold` bytes (default 8 MiB, within `[5 MiB, 5 GiB]`) use one `PutObject`.
    pub fn architecture_storage_with_single_put_threshold(
        &self,
        single_put_threshold: u64,
    ) -> std::result::Result<crate::storage::Storage, Box<dyn std::error::Error>> {
        architecture::connect(self, single_put_threshold)
    }

    /// What the bucket's versioning status was (P7 Q-A: never `Err` for a refusal or a lost
    /// answer, which only the role connection refuses). The one request every connection makes:
    /// a server certificate that is not trusted fails the connection here.
    async fn detect_bucket_versioning(
        client: &Client,
        bucket_name: &str,
        endpoint: &str,
        options: S3UrlOptions,
    ) -> Result<S3VersioningAtConnect> {
        match client
            .get_bucket_versioning()
            .bucket(bucket_name)
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                debug!("bucket {bucket_name} versioning status: {status:?}");
                Ok(versioning_answer::answered(status))
            }
            Err(error) => {
                if options.https
                    && let Some(untrusted) = untrusted_certificate(endpoint, &error)
                {
                    return Err(untrusted);
                }
                let code = error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code);
                let status = error
                    .raw_response()
                    .map(|response| response.status().as_u16());
                let lost = matches!(
                    error,
                    aws_sdk_s3::error::SdkError::TimeoutError(_)
                        | aws_sdk_s3::error::SdkError::DispatchFailure(_)
                        | aws_sdk_s3::error::SdkError::ResponseError(_)
                );
                let answer = versioning_answer::failed(code, status, lost);
                match answer {
                    S3VersioningAtConnect::NoVersioning => {
                        info!("S3 bucket {bucket_name} does not support versioning");
                    }
                    S3VersioningAtConnect::Refused => {
                        warn!("S3 bucket {bucket_name} refused GetBucketVersioning: {error:?}");
                    }
                    S3VersioningAtConnect::Unreachable { .. } => {
                        error!("failed to get versioning status for {bucket_name}: {error:?}");
                    }
                    S3VersioningAtConnect::Enabled
                    | S3VersioningAtConnect::Suspended
                    | S3VersioningAtConnect::Never => {}
                }
                Ok(answer)
            }
        }
    }

    /// 查询指定 S3 端点的桶列表
    ///
    /// 该方法不需要已有的 `S3Storage` 实例，类似于 NFS 的 `list_exports`。
    ///
    /// # 参数
    /// - `url`: S3 端点 URL，格式为 `s3://access_key:secret_key@host:port`（不含 bucket）
    ///
    /// # 返回值
    /// - `Ok(Vec<S3BucketInfo>)`：查询成功，返回桶列表
    /// - `Err(StorageError)`：查询失败，返回错误信息
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn list_buckets(url: &str) -> Result<Vec<S3BucketInfo>> {
        let (access_key, secret_key, endpoint, options) = parse_s3_endpoint_url(url)?;

        let region = "us-east-1";
        let credentials = Credentials::new(&access_key, &secret_key, None, None, "s3-list-buckets");
        let credentials_provider = SharedCredentialsProvider::new(credentials);

        let mut sdk_builder = SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .endpoint_url(endpoint.clone())
            .credentials_provider(credentials_provider)
            .timeout_config(s3_timeout_config());
        if options.skips_tls_verification() {
            warn_unverified("list_buckets", &endpoint);
            sdk_builder = sdk_builder.http_client(build_skip_verify_http_client()?);
        }
        let sdk_config = sdk_builder.build();

        let client = Client::from_conf(build_s3_config(&sdk_config, options.compat));

        let response = client.list_buckets().send().await.map_err(|e| {
            options
                .https
                .then(|| untrusted_certificate(&endpoint, &e))
                .flatten()
                .unwrap_or_else(|| {
                    StorageError::S3Error(format!("Failed to list S3 buckets: {e:?}"))
                })
        })?;

        let buckets = response
            .buckets()
            .iter()
            .filter_map(|b| {
                let name = b.name()?.to_string();
                let creation_date = b.creation_date().map(std::string::ToString::to_string);
                Some(S3BucketInfo {
                    name,
                    creation_date,
                })
            })
            .collect();

        Ok(buckets)
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn new(url: &str, block_size: Option<u64>) -> Result<Self> {
        if block_size == Some(0) {
            return Err(StorageError::ConfigError(
                "S3 read chunk must be non-zero".into(),
            ));
        }
        let read_chunk_bytes = usize::try_from(
            block_size.unwrap_or(8 * 1024 * 1024).min(8 * 1024 * 1024),
        )
        .map_err(|_| StorageError::ConfigError("S3 read chunk exceeds address space".into()))?;
        let transfer_concurrency = crate::transfer_concurrency::TransferConcurrency::from_env(
            crate::model::BackendKind::S3,
            crate::transfer_concurrency::TransferConcurrency::defaults(
                super::DEFAULT_READ_CONCURRENCY,
                super::DEFAULT_WRITE_CONCURRENCY,
            ),
        )?;
        // 解析URL
        let ParsedS3Url {
            access_key,
            secret_key,
            bucket_name,
            endpoint,
            prefix,
            options,
        } = parse_s3_url(url)?;
        let region = "us-east-1"; // MinIO 默认区域

        // 凭据一个字符都不记（R8）：AK 与 SK 都不写，连 SK 的长度也不写。
        debug!("从URL解析结果:");
        debug!("- 凭据: ***:***");
        debug!("- 存储桶名称: {}", bucket_name);
        debug!("- 端点: {}", endpoint);
        debug!("- Prefix: {}", prefix);

        // 创建凭据
        let credentials =
            Credentials::new(&access_key, &secret_key, None, None, "minio-credentials");

        // 将凭据包装成SharedCredentialsProvider
        let credentials_provider = SharedCredentialsProvider::new(credentials);

        // 构建 SDK 配置，增加超时配置以提高HTTPS连接稳定性
        let mut sdk_builder = SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .endpoint_url(endpoint.clone())
            .credentials_provider(credentials_provider)
            // 配置超时参数，避免连接挂起或不完整消息错误
            .timeout_config(s3_timeout_config());
        if options.skips_tls_verification() {
            warn_unverified("connection", &endpoint);
            sdk_builder = sdk_builder.http_client(build_skip_verify_http_client()?);
        }
        let sdk_config = sdk_builder.build();

        // 创建 S3 客户端，强制使用路径样式以支持 FQDN 和自定义域名
        // 禁用自动 checksum 计算（WhenRequired），避免 SDK 对 UploadPart 等操作
        // 自动附加 trailing CRC32 + aws-chunked 编码，某些 S3 兼容存储不支持该编码
        let (client, http_client_capture) =
            qos_client::capturing_client(build_s3_config(&sdk_config, options.compat));

        // 只有当prefix不为空时才设置
        let prefix_option = if prefix.is_empty() {
            None
        } else {
            Some(prefix)
        };

        // 检查桶是否启用了版本控制
        let versioning =
            Self::detect_bucket_versioning(&client, &bucket_name, &endpoint, options).await?;
        let reports_state = matches!(
            versioning,
            S3VersioningAtConnect::Enabled | S3VersioningAtConnect::Suspended
        );
        if options.bucket_type == S3BucketType::Nas && reports_state {
            // An ONTAP NAS bucket answers GetBucketVersioning with 501; one that reports a state
            // (Enabled / Suspended) is an object bucket — one never versioned reports none and goes
            // unnoticed. The URL's choice still holds (P7 §14.2, decision 11): the NAS profile only
            // weakens the guarantees.
            warn!(
                "S3 bucket {bucket_name} is marked bucket_type=nas but reports a versioning state: \
                 it is probably an object bucket; the NAS profile (weaker guarantees: unpinned \
                 reads, no source versions) applies anyway"
            );
        }
        let is_bucket_versioned = versioning == S3VersioningAtConnect::Enabled;
        let versioning_suspended = versioning == S3VersioningAtConnect::Suspended;

        Ok(Self {
            transfer_concurrency,
            read_chunk_bytes,
            options,
            endpoint,
            bucket_name,
            prefix: prefix_option,
            single_attempt_client: OnceLock::from(qos_client::single_attempt_client(
                &client,
                &http_client_capture,
            )),
            http_client_capture,
            client,
            is_bucket_versioned,
            versioning_suspended,
            versioning_at_connect: versioning,
        })
    }

    /// 构建完整的S3 key，将prefix和相对路径结合起来
    #[must_use]
    pub fn build_full_key(&self, relative_path: &str) -> String {
        if let Some(prefix) = &self.prefix {
            // 直接字符串连接，S3 key使用正斜杠
            format!("{prefix}{relative_path}")
        } else {
            // 如果没有prefix，直接返回相对路径
            relative_path.to_string()
        }
    }
}

mod role_protocol;

/// Lists buckets visible to the endpoint credentials, without connecting a bucket storage.
/// # Errors
/// Returns a connection, authentication or listing error.
pub async fn list_buckets(url: &str) -> Result<Vec<S3BucketInfo>> {
    S3Storage::list_buckets(url).await
}

#[cfg(test)]
use data_mover_storage_core::storage::config::s3::endpoint_identity;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssertTestValue;
    use std::time::Duration;

    /// Creates the connected adapter around a replayable SDK client. S3 version-history tests
    /// replace its protocol with an in-memory response source before any request is sent.
    pub(super) fn make_test_storage(prefix: Option<&str>) -> S3Storage {
        let credentials = Credentials::new("test-ak", "test-sk", None, None, "test");
        let credentials_provider = SharedCredentialsProvider::new(credentials);
        let sdk_config = SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://localhost:9000")
            .credentials_provider(credentials_provider)
            .build();
        let client = Client::from_conf(
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .force_path_style(true)
                .request_checksum_calculation(
                    aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
                )
                .build(),
        );
        S3Storage {
            read_chunk_bytes: 8 * 1024 * 1024,
            transfer_concurrency: crate::transfer_concurrency::TransferConcurrency::defaults(4, 4),
            options: S3UrlOptions::default(),
            endpoint: "http://localhost:9000".to_string(),
            bucket_name: "test-bucket".to_string(),
            prefix: prefix.map(std::string::ToString::to_string),
            client,
            single_attempt_client: OnceLock::new(),
            http_client_capture: qos_client::HttpClientCapture::default(),
            is_bucket_versioned: false,
            versioning_suspended: false,
            versioning_at_connect: S3VersioningAtConnect::Never,
        }
    }

    #[test]
    fn versioning_unsupported_detection_is_narrow() {
        assert!(versioning_is_unsupported(Some("NotImplemented"), None));
        assert!(versioning_is_unsupported(
            Some("UnsupportedOperation"),
            None
        ));
        assert!(versioning_is_unsupported(None, Some(501)));
        assert!(!versioning_is_unsupported(Some("AccessDenied"), Some(403)));
        assert!(!versioning_is_unsupported(None, Some(500)));
    }

    #[test]
    fn copy_source_builder_preserves_path_and_unreserved_bytes() {
        assert_eq!(
            build_copy_source("source-bucket", "prefix/a-b.c_d~9/file"),
            "source-bucket/prefix/a-b.c_d~9/file"
        );
    }

    #[test]
    fn copy_source_builder_escapes_reserved_and_utf8_bytes() {
        assert_eq!(build_copy_source("bucket", "a b"), "bucket/a%20b");
        assert_eq!(
            build_copy_source("bucket", "literal%20"),
            "bucket/literal%2520"
        );
        assert_eq!(
            build_copy_source("bucket", "query?v=1"),
            "bucket/query%3Fv%3D1"
        );
        assert_eq!(
            build_copy_source("bucket", "fragment#1"),
            "bucket/fragment%231"
        );
        assert_eq!(
            build_copy_source("bucket", "中文"),
            "bucket/%E4%B8%AD%E6%96%87"
        );
    }

    #[test]
    fn s3_timeout_config_uses_defaults() {
        let config = s3_timeout_config_from_values(None, None, None);

        assert_eq!(config.connect_timeout(), Some(Duration::from_secs(10)));
        assert_eq!(config.operation_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(config.read_timeout(), Some(Duration::from_secs(20)));
    }

    #[test]
    fn s3_timeout_config_accepts_positive_second_overrides() {
        let config = s3_timeout_config_from_values(Some("11"), Some("31"), Some("21"));

        assert_eq!(config.connect_timeout(), Some(Duration::from_secs(11)));
        assert_eq!(config.operation_timeout(), Some(Duration::from_secs(31)));
        assert_eq!(config.read_timeout(), Some(Duration::from_secs(21)));
    }

    #[test]
    fn s3_timeout_config_falls_back_for_invalid_values() {
        let config = s3_timeout_config_from_values(Some("invalid"), Some("-1"), Some("0"));

        assert_eq!(config.connect_timeout(), Some(Duration::from_secs(10)));
        assert_eq!(config.operation_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(config.read_timeout(), Some(Duration::from_secs(20)));
    }

    #[test]
    fn s3_timeout_config_supports_partial_override() {
        let config = s3_timeout_config_from_values(None, Some("45"), None);

        assert_eq!(config.connect_timeout(), Some(Duration::from_secs(10)));
        assert_eq!(config.operation_timeout(), Some(Duration::from_secs(45)));
        assert_eq!(config.read_timeout(), Some(Duration::from_secs(20)));
    }

    #[test]
    fn test_parse_s3_url_special_chars_in_secret_key() {
        // SK 包含 + 和 /（Base64 编码常见字符）
        let url = "s3://X9HENFMKAC41MT11J14H:AsxLb0dEhjxXIlKfVnCSVhM+hjO80rbhRmPLp/UK@bucket.192.168.3.210:10444";
        let parsed = parse_s3_url(url).assert_value("test value should be present");
        let ParsedS3Url {
            access_key: ak,
            secret_key: sk,
            bucket_name: bucket,
            endpoint,
            prefix,
            options,
            ..
        } = parsed;
        assert_eq!(ak, "X9HENFMKAC41MT11J14H");
        assert_eq!(sk, "AsxLb0dEhjxXIlKfVnCSVhM+hjO80rbhRmPLp/UK");
        assert_eq!(bucket, "bucket");
        assert_eq!(endpoint, "http://192.168.3.210:10444");
        assert_eq!(prefix, "");
        assert_eq!(options.compat, S3Compatibility::Standard);
    }

    #[test]
    fn test_parse_s3_url_normal() {
        let url = "s3://myak:mysk@mybucket.minio.example.com:9000/data/prefix";
        let ParsedS3Url {
            access_key: ak,
            secret_key: sk,
            bucket_name: bucket,
            endpoint,
            prefix,
            options,
            ..
        } = parse_s3_url(url).assert_value("test value should be present");
        assert_eq!(ak, "myak");
        assert_eq!(sk, "mysk");
        assert_eq!(bucket, "mybucket");
        assert_eq!(endpoint, "http://minio.example.com:9000");
        assert_eq!(prefix, "data/prefix/");
        assert_eq!(options.compat, S3Compatibility::Standard);
    }

    #[test]
    fn test_parse_s3_url_https() {
        let url = "s3+https://ak:sk@bucket.host.com:443/prefix";
        let ParsedS3Url {
            access_key: ak,
            secret_key: sk,
            bucket_name: bucket,
            endpoint,
            options,
            ..
        } = parse_s3_url(url).assert_value("test value should be present");
        assert_eq!(ak, "ak");
        assert_eq!(sk, "sk");
        assert_eq!(bucket, "bucket");
        assert_eq!(endpoint, "https://host.com:443");
        assert_eq!(options.compat, S3Compatibility::Standard);
    }

    #[test]
    fn test_parse_s3_url_no_prefix() {
        let url = "s3://ak:sk@bucket.host.com:9000";
        let ParsedS3Url {
            access_key: ak,
            secret_key: sk,
            bucket_name: bucket,
            endpoint,
            prefix,
            ..
        } = parse_s3_url(url).assert_value("test value should be present");
        assert_eq!(ak, "ak");
        assert_eq!(sk, "sk");
        assert_eq!(bucket, "bucket");
        assert_eq!(endpoint, "http://host.com:9000");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_s3_url_missing_at() {
        let url = "s3://aksk_no_at_sign";
        assert!(parse_s3_url(url).is_err());
    }

    #[test]
    fn test_parse_s3_url_missing_colon_in_credentials() {
        let url = "s3://aksk_no_colon@bucket.host.com:9000";
        assert!(parse_s3_url(url).is_err());
    }

    #[test]
    fn test_parse_s3_url_deep_prefix() {
        let url = "s3://ak:sk@bucket.host.com:9000/a/b/c";
        let ParsedS3Url { prefix, .. } =
            parse_s3_url(url).assert_value("test value should be present");
        assert_eq!(prefix, "a/b/c/");
    }

    #[test]
    fn test_parse_s3_url_prefix_trailing_slash() {
        let url = "s3://ak:sk@bucket.host.com:9000/prefix/";
        let ParsedS3Url { prefix, .. } =
            parse_s3_url(url).assert_value("test value should be present");
        assert_eq!(prefix, "prefix/");
    }

    #[test]
    fn test_parse_s3_url_percent_encoded_prefix() {
        let base = "s3://ak:sk@bucket.host.com:9000/";
        for (raw, encoded, expected) in [
            ("测试", "%E6%B5%8B%E8%AF%95", "测试/"),
            ("a b", "a%20b", "a b/"),
            ("100%", "100%25", "100%/"),
            ("a//b", "a//b", "a//b/"),
            ("a/./b", "a/./b", "a/./b/"),
        ] {
            let raw_url = format!("{base}{raw}");
            let encoded_url = format!("{base}{encoded}");
            assert_eq!(parse_s3_url(&raw_url).assert_value("raw").prefix, expected);
            assert_eq!(
                parse_s3_url(&encoded_url).assert_value("encoded").prefix,
                expected
            );
            assert_eq!(
                endpoint_identity(&raw_url).assert_value("raw identity"),
                endpoint_identity(&encoded_url).assert_value("encoded identity")
            );
        }
        assert_eq!(
            parse_s3_url(&format!("{base}100%2525"))
                .assert_value("literal percent encoding")
                .prefix,
            "100%25/"
        );
        assert!(parse_s3_url(&format!("{base}%FF")).is_err());
        assert!(parse_s3_url(&format!("{base}name#fragment")).is_err());
    }

    // --- TLS 校验解析测试（s3+https 默认校验，tls_verify=false 才跳过）---

    #[test]
    fn test_parse_s3_url_https_verifies_unless_told_not_to() {
        // s3+https 默认校验证书，只有 tls_verify=false 才跳过
        let url = "s3+https://ak:sk@bucket.host.com:443/prefix";
        let ParsedS3Url { options, .. } =
            parse_s3_url(url).assert_value("test value should be present");
        assert!(options.tls_verify && !options.skips_tls_verification());
        let ParsedS3Url { options, .. } = parse_s3_url(&format!("{url}?tls_verify=false"))
            .assert_value("test value should be present");
        assert!(options.skips_tls_verification());
    }

    #[test]
    fn test_parse_s3_url_http_no_tls_skip() {
        // s3:// (http) 不跳过验证
        let url = "s3://ak:sk@bucket.host.com:9000/prefix";
        let ParsedS3Url { options, .. } =
            parse_s3_url(url).assert_value("test value should be present");
        assert!(!options.skips_tls_verification());
    }

    #[test]
    fn test_parse_s3_endpoint_url_https_verifies_unless_told_not_to() {
        let url = "s3+https://ak:sk@host.com:9000";
        let (_, _, _, options) =
            parse_s3_endpoint_url(url).assert_value("test value should be present");
        assert!(!options.skips_tls_verification());
        let (_, _, endpoint, options) = parse_s3_endpoint_url(&format!("{url}?tls_verify=false"))
            .assert_value("test value should be present");
        assert!(options.skips_tls_verification());
        assert!(endpoint.starts_with("https://"));
        assert_eq!(options.compat, S3Compatibility::Standard);
    }

    #[test]
    fn test_parse_s3_endpoint_url_http_no_tls_skip() {
        // s3:// (http) 不跳过验证
        let url = "s3://ak:sk@host.com:9000";
        let (_, _, _, options) =
            parse_s3_endpoint_url(url).assert_value("test value should be present");
        assert!(!options.skips_tls_verification());
        assert_eq!(options.compat, S3Compatibility::Standard);
    }

    // --- 构造测试用 S3Storage（不连接实际 S3 服务） ---

    // --- build_full_key 测试 ---

    // --- calculate_relative_path 测试 ---

    // --- get_file_info 测试 ---

    // `S3Storage::get_file_info` is a static method, no instance needed.

    // -- MultipartUpload wrapper tests ---------------------------------------
    // These tests don't talk to a real S3 (`localhost:9000` is a stub
    // endpoint). They only exercise the wrapper's local state machine —
    // accessors, sort-on-complete, Drop-guard tagging.
}
