// 标准库
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};

// 外部crate
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedPart, Delete, DeleteMarkerEntry, ObjectIdentifier,
    ObjectVersion,
};
use aws_smithy_runtime_api::client::http::{
    HttpConnector as SdkHttpConnector, HttpConnectorFuture, SharedHttpClient, SharedHttpConnector,
    http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{
    HttpRequest as SdkHttpRequest, HttpResponse as SdkHttpResponse,
};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_types::body::SdkBody;
use aws_types::region::Region;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client as HyperLegacyClient;
use hyper_util::client::legacy::connect::HttpConnector as HyperHttpConnector;
use hyper_util::rt::TokioExecutor;
use rustls::ClientConfig as RustlsClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, error, trace, warn};
use url::Url;

use crate::checksum::{ConsistencyCheck, HashCalculator, create_hash_calculator};
use crate::error::StorageError;
use crate::filter::{FilterExpression, dir_matches_date_filter, should_skip};
use crate::qos::QosManager;
use crate::storage_enum::StorageEnum;
use crate::third_party::hcp::client::HCPRestClient;
use crate::walk_scheduler::{create_worker_contexts, run_worker_loop};
use crate::{
    DataChunk, DeleteDirIterator, DeleteEvent, EntryEnum, ErrorEvent, Result, S3Entry,
    StorageEntryMessage, Tag, WalkDirAsyncIterator, datetime_to_string,
};

/// S3 桶信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BucketInfo {
    /// 桶名称
    pub name: String,
    /// 创建时间（ISO 8601 格式）
    pub creation_date: Option<String>,
}

/// 从 S3 URL 中提取公共部分：scheme、凭据、host+path 尾部
/// 返回 (`scheme_str`, `access_key`, `secret_key`, `host_and_rest`)
/// 其中 `host_and_rest` 是 `@` 之后的全部内容（含 host:port 和可选的 /path）
fn extract_s3_credentials(url: &str) -> Result<(&str, String, String, &str)> {
    let scheme_end = url
        .find("://")
        .ok_or_else(|| StorageError::UrlParseError("URL中缺少 :// 分隔符".to_string()))?;
    let scheme_str = &url[..scheme_end];
    let after_scheme = &url[scheme_end + 3..];

    // 用 rfind('@') 找到 userinfo 与 host 的分界（SK 不含 @，所以最后一个 @ 就是分界）
    let at_pos = after_scheme.rfind('@').ok_or_else(|| {
        StorageError::UrlParseError("URL中缺少 @ 分隔符，无法提取凭据".to_string())
    })?;
    let userinfo = &after_scheme[..at_pos];
    let host_and_rest = &after_scheme[at_pos + 1..];

    // 用第一个 ':' 分割 userinfo 为 AK 和 SK（AK 不含 ':'）
    let colon_pos = userinfo.find(':').ok_or_else(|| {
        StorageError::UrlParseError("URL凭据中缺少 : 分隔符，无法分割AK和SK".to_string())
    })?;
    let access_key = userinfo[..colon_pos].to_string();
    let secret_key = userinfo[colon_pos + 1..].to_string();

    Ok((scheme_str, access_key, secret_key, host_and_rest))
}

/// 将 scheme 字符串映射为 HTTP 协议前缀（不含 HCP）
fn s3_http_scheme(scheme_str: &str) -> Result<&'static str> {
    match scheme_str {
        "s3" | "s3+http" => Ok("http"),
        "s3+https" => Ok("https"),
        _ => Err(StorageError::InvalidPath(
            "无效的S3 URL格式,协议必须是s3://、s3+http://或s3+https://".to_string(),
        )),
    }
}

/// 解析不含 bucket 的 S3 端点 URL：`s3://ak:sk@host:port` 或 `s3+https://ak:sk@host:port`
/// 返回 (`access_key`, `secret_key`, endpoint, `tls_skip_verify`)
fn parse_s3_endpoint_url(url: &str) -> Result<(String, String, String, bool)> {
    let (scheme_str, access_key, secret_key, host_and_port) = extract_s3_credentials(url)?;
    let http_scheme = s3_http_scheme(scheme_str)?;

    // s3+https scheme 即表示跳过 TLS 证书验证（用于自签名/私有 CA 部署）
    let tls_skip_verify = http_scheme == "https";
    let endpoint = format!("{}://{}", http_scheme, host_and_port.trim_end_matches('/'));
    Ok((access_key, secret_key, endpoint, tls_skip_verify))
}

// 使用url库解析S3 URL格式: s3://access_key:secret_key@bucket.host:port/prefix, s3+https://access_key:secret_key@bucket.host:port/prefix，或s3+hcp://access_key:secret_key@bucket.host:port/prefix
// 注意：secret_key 可能包含 `+`、`/` 等特殊字符（Base64 编码），直接传给 Url::parse 会导致解析错误，
// 因此先手动提取凭据，再用占位凭据构建安全 URL 交给 url crate 解析 host/port/path。
#[allow(clippy::type_complexity)]
fn parse_s3_url(
    url: &str,
) -> Result<(
    String,
    String,
    String,
    String,
    String,
    String,
    StorageType,
    bool,
)> {
    let (scheme_str, access_key, secret_key, host_and_path) = extract_s3_credentials(url)?;

    // 用占位凭据构建安全 URL，让 url crate 解析 host/port/path
    let safe_url = format!("{scheme_str}://dummy:dummy@{host_and_path}");
    let parsed_url =
        Url::parse(&safe_url).map_err(|e| StorageError::UrlParseError(e.to_string()))?;

    // 检查URL协议并确定使用的HTTP协议（含 HCP）
    let (http_scheme, storage_type) = match parsed_url.scheme() {
        "s3" | "s3+http" => ("http", StorageType::S3),
        "s3+https" => ("https", StorageType::S3),
        "s3+hcp" => ("http", StorageType::Hcp),
        _ => {
            return Err(StorageError::InvalidPath(
                "无效的S3 URL格式,协议必须是s3://、s3+http://、s3+https://或s3+hcp://".to_string(),
            ));
        }
    };

    // s3+https scheme 即表示跳过 TLS 证书验证（用于自签名/私有 CA 部署）
    let tls_skip_verify = http_scheme == "https";

    // 获取主机名
    let host = parsed_url
        .host_str()
        .ok_or_else(|| StorageError::InvalidPath("URL中缺少主机名".to_string()))?;

    // 解析bucket名称和主机信息
    let (bucket_name, host_and_port) = if let Some(first_dot_index) = host.find('.') {
        (
            host[..first_dot_index].to_string(),
            format!(
                "{}{}",
                &host[first_dot_index + 1..],
                parsed_url.port().map_or(String::new(), |p| format!(":{p}"))
            ),
        )
    } else {
        // 如果没有点，那么整个主机部分可能就是bucket名称
        (
            host.to_string(),
            "localhost:9000".to_string(), // 默认值
        )
    };

    // 提取主机部分（不包含端口）
    let host_only = host_and_port
        .split(':')
        .next()
        .unwrap_or(&host_and_port)
        .to_string();

    // 构建HTTP端点，根据URL的scheme决定使用http还是https
    let endpoint = format!("{http_scheme}://{host_and_port}");

    // 获取URL路径部分作为prefix，并确保以/结尾
    let mut prefix = parsed_url
        .path()
        .strip_prefix('/')
        .unwrap_or(parsed_url.path())
        .to_string();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }

    Ok((
        access_key,
        secret_key,
        bucket_name,
        endpoint,
        prefix,
        host_only,
        storage_type,
        tls_skip_verify,
    ))
}

/// TLS 证书验证跳过器（用于自签名/私有 CA 证书场景，通过 s3+https:// scheme 隐式启用）
#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // 仅列出安全的签名方案，排除已废弃的 SHA-1 系列
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

/// 跳过 TLS 验证的 SDK HTTP 连接器，包装 hyper-rustls 客户端
#[derive(Clone)]
struct SkipVerifyConnector {
    inner: Arc<HyperLegacyClient<HttpsConnector<HyperHttpConnector>, SdkBody>>,
}

impl fmt::Debug for SkipVerifyConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkipVerifyConnector")
            .finish_non_exhaustive()
    }
}

impl SdkHttpConnector for SkipVerifyConnector {
    fn call(&self, request: SdkHttpRequest) -> HttpConnectorFuture {
        let client = self.inner.clone();
        HttpConnectorFuture::new(async move {
            let req_1x = request
                .try_into_http1x()
                .map_err(|e| ConnectorError::other(Box::new(e) as _, None))?;
            let response = client
                .request(req_1x)
                .await
                .map_err(|e| ConnectorError::io(Box::new(e) as _))?;
            SdkHttpResponse::try_from(response.map(SdkBody::from_body_1_x))
                .map_err(|e| ConnectorError::other(Box::new(e) as _, None))
        })
    }
}

/// 构建跳过 TLS 证书验证的 AWS SDK HTTP 客户端（s3+https:// scheme 时使用）
fn build_skip_verify_http_client() -> SharedHttpClient {
    // SECURITY NOTE: dangerous() 仅在用户使用 s3+https:// scheme 时调用，
    // 跳过证书验证仅适用于受信任的私有环境（如 MinIO 自签名证书部署）
    let tls_config = RustlsClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();

    let connector = HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();

    let hyper_client: Arc<HyperLegacyClient<HttpsConnector<HyperHttpConnector>, SdkBody>> =
        Arc::new(HyperLegacyClient::builder(TokioExecutor::new()).build(connector));
    let skip_connector = SkipVerifyConnector {
        inner: hyper_client,
    };

    http_client_fn(move |_settings, _components| SharedHttpConnector::new(skip_connector.clone()))
}

const DEFAULT_BLOCK_SIZE: u64 = 5 * 1024 * 1024; // 5MiB
const MULTIPART_THRESHOLD: u64 = 5 * 1024 * 1024; // 5MiB
const MAX_CONCURRENCY: usize = 5; // 最大并发上传数

/// 单 object 读取的同时在飞 Range GET 请求数（inflight read pipeline 深度）。
///
/// aws-sdk-rust 底层 HTTP/2 connection pool 天然支持多请求并发；S3 兼容存储
/// （AWS S3 / MinIO / Ceph 等）服务端对同 object 不同 byte range 高并发友好，
/// 与单 inflight 相比高 RTT 链路收益线性。默认 4 与 CIFS 对称。
const DEFAULT_READ_INFLIGHT: usize = 4;

/// 转换时间戳为纳秒时间戳
fn datatime_to_i64(timestamp: Option<&DateTime>) -> i64 {
    timestamp.map_or_else(crate::time_util::now_nanos, |t| {
        crate::time_util::combine_secs_nanos(t.secs(), t.subsec_nanos())
    })
}

/// Zero-copy streaming body for multi-chunk singlepart S3 uploads.
/// Holds a deque of `Bytes` objects and yields them one at a time to the S3
/// SDK without ever merging them into a single contiguous buffer.
struct ChunkedBody {
    chunks: VecDeque<Bytes>,
    total_size: u64,
}

impl http_body::Body for ChunkedBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    // http-body 1.x unified poll_frame replaces the old poll_data + poll_trailers pair.
    // Returning Frame::data() for each chunk means the SDK reads each Bytes directly
    // without us ever building a contiguous buffer.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(
            self.chunks
                .pop_front()
                .map(|b| Ok(http_body::Frame::data(b))),
        )
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.total_size)
    }
}

/// 本地文件句柄包装
#[derive(Debug, Clone)]
pub(crate) struct S3FileHandle {
    pub key: String,
    pub version_id: Option<String>,
    pub last_modified: String,
    pub tags: Option<Vec<Tag>>,
}

#[derive(Clone, Debug)]
enum StorageType {
    S3,
    Hcp,
}

/// 本地存储实现
#[derive(Clone, Debug)]
pub struct S3Storage {
    storage_type: StorageType,
    pub(crate) endpoint: String,
    bucket_name: String,
    prefix: Option<String>,
    client: Client,
    hcp_client: Option<HCPRestClient>,
    pub block_size: u64,
    pub is_bucket_versioned: bool,
}

impl S3Storage {
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
    pub async fn list_buckets(url: &str) -> Result<Vec<S3BucketInfo>> {
        let (access_key, secret_key, endpoint, tls_skip_verify) = parse_s3_endpoint_url(url)?;

        let region = "us-east-1";
        let credentials = Credentials::new(&access_key, &secret_key, None, None, "s3-list-buckets");
        let credentials_provider = SharedCredentialsProvider::new(credentials);

        let mut sdk_builder = SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .endpoint_url(endpoint)
            .credentials_provider(credentials_provider)
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(std::time::Duration::from_secs(10))
                    .operation_timeout(std::time::Duration::from_secs(30))
                    .read_timeout(std::time::Duration::from_secs(20))
                    .build(),
            );
        if tls_skip_verify {
            warn!(
                "S3 list_buckets: 使用 s3+https scheme，TLS 证书验证已跳过，仅用于受信任的私有环境"
            );
            sdk_builder = sdk_builder.http_client(build_skip_verify_http_client());
        }
        let sdk_config = sdk_builder.build();

        let client = Client::from_conf(
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .force_path_style(true)
                .request_checksum_calculation(
                    aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
                )
                .build(),
        );

        let response = client
            .list_buckets()
            .send()
            .await
            .map_err(|e| StorageError::S3Error(format!("Failed to list S3 buckets: {e:?}")))?;

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

    pub async fn new(url: &str, block_size: Option<u64>) -> Result<Self> {
        // 解析URL
        let (
            access_key,
            secret_key,
            bucket_name,
            endpoint,
            prefix,
            host,
            storage_type,
            tls_skip_verify,
        ) = parse_s3_url(url)?;

        let region = "us-east-1"; // MinIO 默认区域

        debug!("从URL解析结果:");
        debug!("- 访问密钥: {}", access_key);
        debug!("- 密钥: {}", "*".repeat(secret_key.len())); // 不显示实际密钥
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
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(std::time::Duration::from_secs(10))
                    .operation_timeout(std::time::Duration::from_secs(30))
                    .read_timeout(std::time::Duration::from_secs(20))
                    .build(),
            );
        if tls_skip_verify {
            warn!(
                "S3 Storage::new: 使用 s3+https scheme，TLS 证书验证已跳过，仅用于受信任的私有环境"
            );
            sdk_builder = sdk_builder.http_client(build_skip_verify_http_client());
        }
        let sdk_config = sdk_builder.build();

        // 创建 S3 客户端，强制使用路径样式以支持 FQDN 和自定义域名
        // 禁用自动 checksum 计算（WhenRequired），避免 SDK 对 UploadPart 等操作
        // 自动附加 trailing CRC32 + aws-chunked 编码，某些 S3 兼容存储不支持该编码
        let client = Client::from_conf(
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .force_path_style(true)
                .request_checksum_calculation(
                    aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
                )
                .build(),
        );

        // 只有当prefix不为空时才设置
        let prefix_option = if prefix.is_empty() {
            None
        } else {
            Some(prefix)
        };

        // 如果是HCP存储类型，创建HCP客户端
        let hcp_client = if matches!(storage_type, StorageType::Hcp) {
            Some(crate::third_party::hcp::client::HCPRestClient::try_new(
                bucket_name.clone(),
                host,
                &access_key,
                &secret_key,
            )?)
        } else {
            None
        };

        // 检查桶是否启用了版本控制
        let is_bucket_versioned = match storage_type {
            StorageType::S3 => {
                debug!("检查桶是否启用了版本控制, bucket: {}", bucket_name);
                match client
                    .get_bucket_versioning()
                    .bucket(&bucket_name)
                    .send()
                    .await
                {
                    Ok(response) => {
                        let status = response.status();
                        let is_versioned = status == Some(&BucketVersioningStatus::Enabled);
                        debug!(
                            "桶 {} 的版本控制状态: {:?}, is_versioned: {}",
                            bucket_name, status, is_versioned
                        );
                        is_versioned
                    }
                    Err(e) => {
                        error!(
                            "检查桶版本控制状态失败, bucket: {}, 错误: {:?}",
                            bucket_name, e
                        );
                        false
                    }
                }
            }
            StorageType::Hcp => {
                debug!("HCP支持版本控制检查");
                true
            }
        };

        Ok(Self {
            storage_type,
            endpoint,
            bucket_name,
            prefix: prefix_option,
            client,
            hcp_client,
            block_size: block_size.map_or(DEFAULT_BLOCK_SIZE, |size| {
                std::cmp::max(size, DEFAULT_BLOCK_SIZE)
            }),
            is_bucket_versioned,
        })
    }

    /// 验证 S3 连通性：尝试 `HeadBucket` 检查 bucket 是否可访问
    pub async fn check_connectivity(&self) -> Result<()> {
        self.client
            .head_bucket()
            .bucket(&self.bucket_name)
            .send()
            .await
            .map_err(|e| StorageError::S3Error(format!("S3 connectivity check failed: {e}")))?;
        Ok(())
    }

    /// 构建完整的S3 key，将prefix和相对路径结合起来
    pub fn build_full_key(&self, relative_path: &str) -> String {
        if let Some(prefix) = &self.prefix {
            // 直接字符串连接，S3 key使用正斜杠
            format!("{prefix}{relative_path}")
        } else {
            // 如果没有prefix，直接返回相对路径
            relative_path.to_string()
        }
    }

    /// Get the bucket name
    pub fn bucket(&self) -> &str {
        &self.bucket_name
    }

    /// Get the endpoint URL
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// 删除单个 S3 对象
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(|e| StorageError::S3Error(format!("Failed to delete object '{key}': {e}")))?;
        Ok(())
    }

    /// 批量删除 S3 对象，内部按 `CHUNK_SIZE` 分批并发发送（避免请求体过大被 S3 兼容存储拒绝），返回成功删除的 key 列表
    async fn delete_objects_batch(&self, keys: &[String]) -> Result<Vec<String>> {
        const CHUNK_SIZE: usize = 100;

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let chunks: Vec<&[String]> = keys.chunks(CHUNK_SIZE).collect();
        let concurrency = std::cmp::min(chunks.len(), MAX_CONCURRENCY);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let all_succeeded = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            let chunk_keys: Vec<String> = chunk.to_vec();
            let self_clone = self.clone();
            let semaphore_clone = semaphore.clone();
            let succeeded_clone = all_succeeded.clone();

            let permit = semaphore_clone
                .acquire_owned()
                .await
                .map_err(|e| StorageError::S3Error(format!("Failed to acquire semaphore: {e}")))?;

            let handle = tokio::spawn(async move {
                let _permit = permit;

                let objects: Vec<ObjectIdentifier> = chunk_keys
                    .iter()
                    .filter_map(|k| ObjectIdentifier::builder().key(k).build().ok())
                    .collect();

                let delete = Delete::builder()
                    .set_objects(Some(objects))
                    .quiet(true)
                    .build()
                    .map_err(|e| {
                        StorageError::S3Error(format!("Failed to build Delete request: {e}"))
                    })?;

                let resp = self_clone
                    .client
                    .delete_objects()
                    .bucket(&self_clone.bucket_name)
                    .delete(delete)
                    .send()
                    .await
                    .map_err(|e| {
                        StorageError::S3Error(format!("Failed to delete objects batch: {e}"))
                    })?;

                // 记录失败的 key
                let mut failed_keys = std::collections::HashSet::new();
                let errors = resp.errors();
                if !errors.is_empty() {
                    for err in errors {
                        let key = err.key().unwrap_or("<unknown>");
                        let msg = err.message().unwrap_or("<no message>");
                        error!("Failed to delete S3 object '{}': {}", key, msg);
                        failed_keys.insert(key.to_string());
                    }
                }

                // 收集成功删除的 key
                let succeeded: Vec<String> = chunk_keys
                    .iter()
                    .filter(|k| !failed_keys.contains(k.as_str()))
                    .cloned()
                    .collect();
                succeeded_clone.lock().await.extend(succeeded);

                Ok::<(), StorageError>(())
            });

            handles.push(handle);
        }

        // 等待所有任务完成，收集第一个错误
        let mut first_error: Option<StorageError> = None;
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!("Delete batch task failed: {:?}", e);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(e) => {
                    error!("Delete batch task panicked: {:?}", e);
                    if first_error.is_none() {
                        first_error = Some(StorageError::S3Error(format!(
                            "Delete task panicked: {e:?}"
                        )));
                    }
                }
            }
        }

        if let Some(e) = first_error {
            return Err(e);
        }

        let result = std::mem::take(&mut *all_succeeded.lock().await);
        Ok(result)
    }

    /// 分页列举 + 批量删除，返回进度迭代器
    pub fn delete_dir_all_with_progress(
        &self,
        relative_path: Option<&str>,
        _concurrency: usize,
    ) -> Result<DeleteDirIterator> {
        let (tx, rx) = async_channel::bounded::<DeleteEvent>(1000);
        let storage = self.clone();
        let prefix = match relative_path {
            Some(p) => storage.build_full_key(p),
            None => storage.prefix.clone().unwrap_or_default(),
        };

        tokio::spawn(async move {
            let mut continuation_token: Option<String> = None;

            loop {
                let mut req = storage
                    .client
                    .list_objects_v2()
                    .bucket(&storage.bucket_name)
                    .prefix(&prefix)
                    .max_keys(1000);

                if let Some(ref token) = continuation_token {
                    req = req.continuation_token(token);
                }

                let resp = match req.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Failed to list objects for delete: {}", e);
                        break;
                    }
                };

                let keys: Vec<String> = resp
                    .contents()
                    .iter()
                    .filter_map(|obj| obj.key().map(std::string::ToString::to_string))
                    .collect();

                if !keys.is_empty() {
                    match storage.delete_objects_batch(&keys).await {
                        Ok(deleted) => {
                            for key in deleted {
                                // 将 full key 转为相对路径
                                let rel = key.strip_prefix(&prefix).unwrap_or(&key);
                                let _ = tx
                                    .send(DeleteEvent {
                                        relative_path: std::path::PathBuf::from(rel),
                                        is_dir: false,
                                    })
                                    .await;
                            }
                        }
                        Err(e) => {
                            error!("Failed to delete objects batch: {:?}", e);
                        }
                    }
                }

                // 检查是否还有更多对象
                match resp.next_continuation_token() {
                    Some(token) => continuation_token = Some(token.to_string()),
                    None => break,
                }
            }
            // tx drop → channel 关闭
        });

        Ok(DeleteDirIterator::new(rx))
    }

    /// Single-chunk read: opens the object and returns all bytes at once.
    pub(crate) async fn read_file(&self, relative_path: &str, size: u64) -> Result<Bytes> {
        let key = self.build_full_key(relative_path);
        let mut handle = S3FileHandle {
            key,
            version_id: None,
            last_modified: String::new(),
            tags: None,
        };
        self.read(&mut handle, 0, size as usize).await
    }

    /// Single-chunk write: uploads all bytes as one `PutObject` call.
    pub(crate) async fn write_file(
        &self,
        relative_path: &str,
        data: Bytes,
        mtime: i64,
        tags: Option<Vec<Tag>>,
    ) -> Result<()> {
        let mut handle = self.create(relative_path, mtime, tags);
        self.write(&mut handle, data).await.map(|_| ())
    }

    /// Chunked read: sends `DataChunks` into `tx`, used by the multi-chunk pipeline.
    ///
    /// 实现 inflight pipeline：维持最多 [`DEFAULT_READ_INFLIGHT`] 个 Range GET 同时
    /// 在飞，aws-sdk-rust 底层 HTTP/2 connection pool 自动复用连接 + 并发请求。
    /// 用 `FuturesOrdered` 保证按 offset 顺序送 channel，下游 hasher.update 需顺序。
    pub(crate) async fn read_data(
        &self,
        tx: mpsc::Sender<DataChunk>,
        relative_path: &str,
        size: u64,
        enable_integrity_check: bool,
        qos: Option<QosManager>,
    ) -> Result<Option<HashCalculator>> {
        if size == 0 {
            return Ok(None);
        }
        let key = Arc::new(self.build_full_key(relative_path));
        let chunk_size = self.block_size as usize;
        let mut hasher = create_hash_calculator(enable_integrity_check);

        type ReadFut<'a> = Pin<Box<dyn Future<Output = Result<Bytes>> + Send + 'a>>;
        let mut inflight: FuturesOrdered<ReadFut<'_>> = FuturesOrdered::new();
        let mut issue_offset: u64 = 0;
        let mut send_offset: u64 = 0;

        loop {
            // 填满 inflight，直到达到深度上限或所有字节已发出。
            while inflight.len() < DEFAULT_READ_INFLIGHT && issue_offset < size {
                if let Some(ref qos) = qos {
                    qos.acquire(chunk_size as u64).await;
                }
                let count = ((size - issue_offset) as usize).min(chunk_size);
                let key_clone = key.clone();
                let range_offset = issue_offset;
                let range_count = count as u64;
                // 调 `read_range_uncached` helper（与 `self.read` 共享 Range GET 路径），
                // 避免 GetObject + body.collect 模板在两处漂移。version_id = None：
                // read_data 调用域内本来就不带版本。
                let fut = Box::pin(async move {
                    self.read_range_uncached(key_clone.as_str(), None, range_offset, range_count)
                        .await
                });
                inflight.push_back(fut);
                issue_offset += count as u64;
            }

            // inflight 已空且全部读完 → 退出循环。
            let Some(result) = inflight.next().await else {
                break;
            };
            let data = match result {
                Ok(d) => d,
                Err(e) => {
                    // 上抛真实读错误；旧实现用 `?`，本次重写曾误降级为 break+Ok（regression）。
                    error!("S3 read chunk at offset {} failed: {:?}", send_offset, e);
                    drop(inflight);
                    return Err(e);
                }
            };

            if data.is_empty() {
                break;
            }
            let len = data.len() as u64;
            if let Some(ref mut h) = hasher {
                h.update(&data);
            }
            if tx
                .send(DataChunk {
                    offset: send_offset,
                    data,
                })
                .await
                .is_err()
            {
                // 下游 receiver 关闭：视为协作取消信号（与旧实现一致），不当读错误。
                break;
            }
            send_offset += len;
        }

        // FuturesOrdered drop 取消未完成的 Range GET，aws-sdk 内部丢弃响应。
        drop(inflight);
        Ok(hasher)
    }

    /// Chunked write: receives `DataChunks` from `rx` and dispatches to singlepart or multipart upload.
    pub(crate) async fn write_data(
        &self,
        rx: mpsc::Receiver<DataChunk>,
        relative_path: &str,
        size: u64,
        mtime: i64,
        tags: Option<Vec<Tag>>,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<()> {
        let written = if size <= MULTIPART_THRESHOLD {
            self.write_singlepart_data(rx, relative_path, mtime, tags)
                .await?
        } else {
            self.write_multipart_data(rx, relative_path, tags).await?
        };
        if let Some(ref c) = bytes_counter {
            c.fetch_add(written as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Server-side copy within the same S3-compatible endpoint.
    pub(crate) async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<()> {
        let copy_source = format!("{src_bucket}/{src_key}");
        self.client
            .copy_object()
            .copy_source(copy_source)
            .bucket(dst_bucket)
            .key(dst_key)
            .send()
            .await
            .map_err(|e| StorageError::S3Error(format!("CopyObject failed: {e:?}")))?;
        Ok(())
    }

    /// Cross-endpoint streaming copy: pipes `GetObject` `ByteStream` directly into `PutObject` /
    /// `UploadPart` without buffering into Bytes. Small files use a single `PutObject`;
    /// large files (> `MULTIPART_THRESHOLD`) use ranged `GetObject` + multipart upload on `dst`.
    pub(crate) async fn stream_copy_to(
        &self,
        dst: &S3Storage,
        src_key: &str,
        dst_key: &str,
        size: u64,
        tags: Option<Vec<Tag>>,
    ) -> Result<()> {
        if size <= MULTIPART_THRESHOLD {
            // ── single PutObject ──────────────────────────────────────────────────
            let resp = self
                .client
                .get_object()
                .bucket(&self.bucket_name)
                .key(src_key)
                .send()
                .await
                .map_err(|e| StorageError::S3Error(format!("GetObject failed: {e:?}")))?;

            let content_length = resp.content_length().unwrap_or(size as i64);

            let mut put_builder = dst
                .client
                .put_object()
                .bucket(&dst.bucket_name)
                .key(dst_key)
                .body(resp.body)
                .content_length(content_length);

            if let Some(ref tags) = tags
                && !tags.is_empty()
            {
                put_builder = put_builder.tagging(build_tagging_str(tags));
            }

            put_builder
                .send()
                .await
                .map_err(|e| StorageError::S3Error(format!("PutObject failed: {e:?}")))?;

            Ok(())
        } else {
            // ── multipart: ranged GetObject → UploadPart per chunk ────────────────
            let upload_id = dst.create_multipart_upload(dst_key, tags.as_ref()).await?;

            // 用于存储已上传分块的信息
            let parts = Arc::new(Mutex::new(Vec::new()));
            let mut offset = 0u64;
            let mut part_number = 1i32;

            // 计算实际的分片数量
            let total_parts = size.div_ceil(MULTIPART_THRESHOLD);
            // 限制并发上传的数量，不超过实际分片数量和最大并发数
            let concurrency = std::cmp::min(total_parts as usize, MAX_CONCURRENCY);
            let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

            // 存储上传任务的句柄
            let mut upload_handles = Vec::new();

            // 计算所有分块的范围和信息
            while offset < size {
                let count = (size - offset).min(MULTIPART_THRESHOLD);
                let range = format!("bytes={}-{}", offset, offset + count - 1);
                let part_number_clone = part_number;
                let count_clone = count;
                let range_clone = range;
                let src_key_clone = src_key.to_string();
                let dst_key_clone = dst_key.to_string();
                let upload_id_clone = upload_id.clone();
                let self_clone = self.clone();
                let dst_clone = dst.clone();
                let parts_clone = parts.clone();
                let semaphore_clone = semaphore.clone();

                // 获取信号量许可
                let permit = semaphore_clone.acquire_owned().await.map_err(|_| {
                    StorageError::S3Error("Semaphore closed unexpectedly".to_string())
                })?;

                // 创建异步上传任务
                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    let resp = match self_clone
                        .client
                        .get_object()
                        .bucket(&self_clone.bucket_name)
                        .key(&src_key_clone)
                        .range(range_clone)
                        .send()
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            error!("GetObject range failed: {:?}", e);
                            return Err(StorageError::S3Error(format!(
                                "GetObject range failed: {e:?}"
                            )));
                        }
                    };

                    let part_resp = match dst_clone
                        .client
                        .upload_part()
                        .bucket(&dst_clone.bucket_name)
                        .key(&dst_key_clone)
                        .upload_id(&upload_id_clone)
                        .part_number(part_number_clone)
                        .content_length(count_clone as i64)
                        .body(resp.body)
                        .send()
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            error!("UploadPart failed: {:?}", e);
                            return Err(StorageError::S3Error(format!("UploadPart failed: {e:?}")));
                        }
                    };

                    let Some(etag_ref) = part_resp.e_tag() else {
                        error!("ETag missing in UploadPart response");
                        return Err(StorageError::S3Error(
                            "ETag missing in UploadPart response".into(),
                        ));
                    };
                    let etag = etag_ref.to_string();

                    // 保存分块信息
                    let mut parts = parts_clone.lock().await;
                    parts.push(
                        CompletedPart::builder()
                            .part_number(part_number_clone)
                            .e_tag(etag)
                            .build(),
                    );
                    Ok(())
                });

                // 存储上传任务的句柄
                upload_handles.push(handle);

                offset += count;
                part_number += 1;
            }

            dst.finish_multipart_upload(dst_key, &upload_id, upload_handles, parts)
                .await?;

            Ok(())
        }
    }

    fn create(
        &self,
        relative_path: &str,
        last_modified: i64,
        tags: Option<Vec<Tag>>,
    ) -> S3FileHandle {
        // 构建完整的S3 key，包含prefix
        let full_key = self.build_full_key(relative_path);
        debug!(
            "when creating, relative_path is {:?}, full_key is {:?}",
            relative_path, full_key
        );

        S3FileHandle {
            key: full_key,
            version_id: None,
            last_modified: datetime_to_string(last_modified),
            tags,
        }
    }

    /// 中止未完成的multipart upload
    pub async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<()> {
        debug!(
            "尝试中止multipart upload, key: {}, upload_id: {}",
            key, upload_id
        );
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket_name)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| {
                error!(
                    "中止multipart upload失败: {:?}, key: {}, upload_id: {}",
                    e, key, upload_id
                );
                StorageError::S3Error(format!("Failed to abort multipart upload: {e}"))
            })?;
        debug!(
            "成功中止multipart upload，key: {}, upload_id: {}",
            key, upload_id
        );
        Ok(())
    }

    /// 等待所有分块上传任务完成，排序 parts，完成或中止 multipart upload。
    /// 统一处理 `JoinHandle` 内层 Result 错误和 JoinError（task panic）。
    async fn finish_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        handles: Vec<tokio::task::JoinHandle<Result<()>>>,
        parts: Arc<Mutex<Vec<CompletedPart>>>,
    ) -> Result<()> {
        // 等待所有上传任务完成，收集内层和外层错误
        let mut first_error: Option<StorageError> = None;
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!("Upload part failed: {:?}", e);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(e) => {
                    error!("Upload task panicked: {:?}", e);
                    if first_error.is_none() {
                        first_error = Some(StorageError::S3Error(format!(
                            "Upload task panicked: {e:?}"
                        )));
                    }
                }
            }
        }

        // 检查上传是否有错误，有则中止
        if let Some(e) = first_error {
            if let Err(abort_err) = self.abort_multipart_upload(key, upload_id).await {
                error!("Failed to abort multipart upload: {:?}", abort_err);
            }
            return Err(e);
        }

        // 获取所有已上传的分块信息并按 part_number 排序
        let mut parts_vec = parts.lock().await;
        parts_vec.sort_by_key(|p| p.part_number().unwrap_or(0));
        let parts_slice = parts_vec.as_slice();

        // 完成 multipart upload
        if let Err(e) = self
            .complete_multipart_upload(key, upload_id, parts_slice)
            .await
        {
            let _ = self.abort_multipart_upload(key, upload_id).await;
            return Err(e);
        }

        Ok(())
    }

    /// 获取对象的标签
    /// 根据 `storage_type` 选择不同的获取方式：
    /// - S3: 使用 AWS S3 SDK 的 `get_object_tagging`
    /// - HCP: 使用 HCP REST API 的 `get_tags`
    async fn get_object_tags(
        &self,
        bucket: &str,
        object_key: &str,
        version_id: Option<&str>,
    ) -> Result<Option<Vec<Tag>>> {
        debug!(
            "获取对象标签, bucket: {}, object_key: {}, version_id: {:?}",
            bucket, object_key, version_id
        );
        match self.storage_type {
            StorageType::S3 => {
                let mut request = self
                    .client
                    .get_object_tagging()
                    .bucket(bucket)
                    .key(object_key);

                if let Some(version_id) = version_id {
                    request = request.version_id(version_id);
                }

                match request.send().await {
                    Ok(response) => {
                        let s3_tags = response.tag_set();
                        trace!("tags are: {:?}", s3_tags);
                        let converted_tags: Vec<Tag> = s3_tags
                            .iter()
                            .map(|s3_tag| Tag {
                                key: s3_tag.key().to_string(),
                                value: s3_tag.value().to_string(),
                            })
                            .collect();
                        Ok(Some(converted_tags))
                    }
                    Err(e) => {
                        error!("获取{}的标签失败，原因是：{:?}", object_key, e);
                        Ok(None)
                    }
                }
            }
            StorageType::Hcp => {
                debug!("test point 1");
                if let Some(hcp_client) = &self.hcp_client {
                    let path = format!("/{object_key}");
                    match hcp_client.get_tags(&path).await {
                        Ok(tags) => {
                            trace!("tags are: {:?}", tags);
                            Ok(Some(tags))
                        }
                        Err(e) => {
                            error!("获取{}的标签失败，原因是：{:?}", object_key, e);
                            Ok(None)
                        }
                    }
                } else {
                    error!("HCP client 未初始化，无法获取标签");
                    Ok(None)
                }
            }
        }
    }

    /// 计算相对路径：移除存储的基本前缀和末尾斜杠
    #[inline]
    fn calculate_relative_path<'a>(&self, full_path: &'a str) -> &'a str {
        if let Some(base_prefix) = &self.prefix {
            full_path.strip_prefix(base_prefix).unwrap_or(full_path)
        } else {
            full_path
        }
    }

    /// 从路径计算文件名和扩展名
    fn get_file_info(path_str: &str) -> (String, Option<String>) {
        let path_buf = PathBuf::from(path_str);
        let file_name = path_buf.file_name().map_or_else(
            || path_str.to_string(),
            |f| f.to_string_lossy().into_owned(),
        );
        let extension = path_buf
            .extension()
            .map(|ext| ext.to_string_lossy().into_owned());
        (file_name, extension)
    }

    /// 构建 `EntryEnum`
    #[allow(clippy::too_many_arguments)]
    fn build_entry(
        file_name: String,
        relative_path: &str,
        extension: Option<String>,
        size: u64,
        last_modified: i64,
        tags: Option<Vec<Tag>>,
        version_id: Option<&str>,
        is_latest: bool,
        is_delete_marker: bool,
        version_count: Option<u32>,
        is_dir: bool,
    ) -> EntryEnum {
        EntryEnum::S3(S3Entry {
            name: file_name,
            relative_path: relative_path.to_string(),
            extension,
            size,
            mtime: last_modified,
            tags,
            version_id: version_id.map(std::string::ToString::to_string),
            is_latest,
            is_delete_marker,
            version_count,
            is_dir,
        })
    }

    /// 处理目录条目
    #[allow(clippy::too_many_arguments)]
    async fn process_directory(
        &self,
        ctx: &crate::walk_scheduler::WorkerContext<(String, usize, bool, Option<usize>)>,
        thread_id: usize,
        prefix_name: &str,
        current_depth: usize,
        depth_limit: Option<usize>,
        skip_filter: bool,
        match_expressions: Option<&FilterExpression>,
        exclude_expressions: Option<&FilterExpression>,
        packaged: bool,
        package_depth: usize,
        package_remaining: Option<usize>,
        tx: &async_channel::Sender<StorageEntryMessage>,
        total_file_count: &Arc<AtomicUsize>,
    ) -> Result<()> {
        // 计算相对路径：移除存储的基本前缀和末尾斜杠
        let relative_path = self.calculate_relative_path(prefix_name);
        let clean_relative_path = relative_path.trim_end_matches('/');

        // 获取目录名
        let dir_name = PathBuf::from(clean_relative_path).file_name().map_or_else(
            || {
                let mut name = prefix_name.to_string();
                if name.ends_with('/') {
                    name.pop();
                }
                name
            },
            |f| f.to_string_lossy().into_owned(),
        );

        // 检查是否应该跳过目录
        let (skip_entry, continue_scan, need_submatch) = if skip_filter {
            should_skip(
                match_expressions,
                exclude_expressions,
                Some(&dir_name),
                Some(clean_relative_path),
                Some("dir"),
                None, // S3对象没有modified_epoch属性
                Some(0),
                None,
            )
        } else {
            (false, true, false)
        };
        debug!(
            "[S3] 线程 {} 处理目录 {}，skip_entry: {}, continue_scan: {}, need_submatch: {}",
            thread_id, clean_relative_path, skip_entry, continue_scan, need_submatch
        );

        // 计算子目录的深度
        let subdir_depth = current_depth + 1;
        let mut send_packaged = false;

        // package 深度追踪模式：只处理目录递归
        if let Some(remaining) = package_remaining {
            if remaining > 1 {
                ctx.push_task((
                    prefix_name.to_string(),
                    subdir_depth,
                    false,
                    Some(remaining - 1),
                ))
                .await;
                return Ok(());
            }
            send_packaged = true;
        }

        // packaged 模式：目录匹配 DirDate 条件时决定打包策略
        if !send_packaged && packaged && dir_matches_date_filter(match_expressions, &dir_name) {
            if depth_limit.is_some_and(|max_depth| subdir_depth + package_depth > max_depth) {
                return Ok(());
            }
            let within_depth = match depth_limit {
                Some(max_depth) => current_depth < max_depth,
                None => true,
            };
            if within_depth {
                if package_depth > 0 {
                    ctx.push_task((
                        prefix_name.to_string(),
                        subdir_depth,
                        false,
                        Some(package_depth),
                    ))
                    .await;
                } else {
                    send_packaged = true;
                }
            }
            if !send_packaged {
                return Ok(());
            }
        }

        // 统一的 Packaged 发送
        if send_packaged {
            let entry = Self::build_entry(
                dir_name,
                clean_relative_path,
                None,
                0,
                0,
                None,
                None,
                false,
                false,
                None,
                true,
            );
            debug!(
                "[S3] 线程 {} Packaged dir {} (depth: {})",
                thread_id, clean_relative_path, current_depth
            );
            if tx
                .send(StorageEntryMessage::Packaged(Arc::new(entry)))
                .await
                .is_err()
            {
                return Err(StorageError::OperationError("接收端已关闭".to_string()));
            }
            total_file_count.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        let should_scan_subdir = match depth_limit {
            Some(max_depth) => current_depth < max_depth,
            None => true,
        };
        debug!(
            "[S3] 线程 {} 处理目录 {}，subdir_depth: {}, should_scan_subdir: {}",
            thread_id, relative_path, subdir_depth, should_scan_subdir
        );

        // 发送目录条目到 channel
        if !skip_entry {
            let entry = Self::build_entry(
                dir_name,
                clean_relative_path,
                None,
                0,
                0,
                None,
                None,
                false,
                false,
                None,
                true,
            );
            if tx
                .send(StorageEntryMessage::Scanned(Arc::new(entry)))
                .await
                .is_err()
            {
                return Err(StorageError::OperationError("接收端已关闭".to_string()));
            }
            total_file_count.fetch_add(1, Ordering::Relaxed);
        }

        // 处理子目录扫描逻辑：只要should_scan_subdir和continue_scan为true，就将子目录push到栈里
        if should_scan_subdir && continue_scan {
            let new_skip_filter = need_submatch;
            ctx.push_task((prefix_name.to_string(), subdir_depth, new_skip_filter, None))
                .await;
        }

        Ok(())
    }

    /// 处理文件条目
    #[allow(clippy::too_many_arguments)]
    async fn process_object(
        &self,
        tx: &async_channel::Sender<StorageEntryMessage>,
        thread_id: usize,
        key: &str,
        size: u64,
        last_modified: i64,
        extension: Option<String>,
        include_tags: bool,
        skip_filter: bool,
        match_expressions: Option<&FilterExpression>,
        exclude_expressions: Option<&FilterExpression>,
        total_file_count: &Arc<AtomicUsize>,
    ) -> Result<()> {
        // 构建路径信息
        let relative_path = self.calculate_relative_path(key);
        let path_buf = PathBuf::from(relative_path);
        let file_name = path_buf
            .file_name()
            .map_or_else(|| key.to_string(), |f| f.to_string_lossy().into_owned());

        // 检查是否应该跳过文件
        let (skip_entry, _, _) = if skip_filter {
            should_skip(
                match_expressions,
                exclude_expressions,
                Some(&file_name),
                Some(relative_path),
                Some("file"),
                None, // S3对象没有modified_epoch属性
                Some(size),
                extension.clone().or(Some(String::new())).as_deref(),
            )
        } else {
            (false, false, false)
        };
        debug!(
            "[S3] 线程 {} 处理文件 {}，skip_entry: {}",
            thread_id, relative_path, skip_entry,
        );

        // 如果不应该跳过，处理文件
        if !skip_entry {
            // 获取对象标签
            let tags = if include_tags {
                self.get_object_tags(&self.bucket_name, key, None)
                    .await
                    .unwrap_or_default()
            } else {
                None
            };

            // 构建并发送EntryEnum
            let entry = Self::build_entry(
                file_name,
                relative_path,
                extension,
                size,
                last_modified,
                tags,
                None,
                false,
                false,
                None,
                false,
            );

            total_file_count.fetch_add(1, Ordering::Relaxed);
            // 发送条目
            if tx
                .send(StorageEntryMessage::Scanned(Arc::new(entry)))
                .await
                .is_err()
            {
                return Err(StorageError::OperationError("接收端已关闭".to_string()));
            }
        }

        Ok(())
    }

    /// 处理版本化对象条目，按对象分组并按时间排序
    #[allow(clippy::too_many_arguments)]
    async fn process_versioned_entries(
        &self,
        tx: &async_channel::Sender<StorageEntryMessage>,
        version_entries: &[ObjectVersion],
        delete_marker_entries: &[DeleteMarkerEntry],
        include_tags: bool,
        match_expressions: Option<&FilterExpression>,
        exclude_expressions: Option<&FilterExpression>,
        total_file_count: Arc<AtomicUsize>,
    ) -> Result<()> {
        // 定义一个枚举来表示版本或删除标记
        enum VersionOrDeleteMarker {
            Version(ObjectVersion),
            DeleteMarker(DeleteMarkerEntry),
        }

        // 创建一个HashMap来按对象key分组所有版本条目
        let mut object_versions: HashMap<String, Vec<(i64, VersionOrDeleteMarker)>> =
            HashMap::new();

        // 处理版本对象
        for version in version_entries {
            if let Some(key) = version.key() {
                let key = key.to_string();
                let last_modified = datatime_to_i64(version.last_modified());

                object_versions.entry(key).or_default().push((
                    last_modified,
                    VersionOrDeleteMarker::Version(version.clone()),
                ));
            }
        }

        // 处理删除标记
        for delete_marker in delete_marker_entries {
            if let Some(key) = delete_marker.key() {
                let key = key.to_string();
                let last_modified = datatime_to_i64(delete_marker.last_modified());

                object_versions.entry(key).or_default().push((
                    last_modified,
                    VersionOrDeleteMarker::DeleteMarker(delete_marker.clone()),
                ));
            }
        }

        // 处理每个对象的所有版本
        for (_key, versions) in object_versions {
            // 按时间从旧到新排序
            let mut sorted_versions = versions;
            sorted_versions.sort_by(|a, b| a.0.cmp(&b.0));

            // 计算该对象的版本总数
            let version_count = sorted_versions.len() as u32;

            // 检查最后一个版本是否是删除标记，如果是则跳过整个对象
            let should_skip_object = match sorted_versions.last() {
                Some((_, VersionOrDeleteMarker::DeleteMarker(_))) => {
                    debug!("[S3] 跳过对象，因为最后一个版本是删除标记");
                    true
                }
                _ => false,
            };

            if should_skip_object {
                continue;
            }

            // 依次处理每个版本
            for (_, version_or_delete_marker) in sorted_versions {
                match version_or_delete_marker {
                    VersionOrDeleteMarker::Version(version) => {
                        // 处理版本对象
                        if let Some(key_str) = version.key() {
                            // 计算相对路径：移除存储的基本前缀
                            let relative_path = self.calculate_relative_path(key_str);
                            let (file_name, extension) = Self::get_file_info(relative_path);
                            let size = version.size().unwrap_or(0) as u64;

                            // 检查是否应该跳过文件
                            let (skip_entry, _, _) = should_skip(
                                match_expressions,
                                exclude_expressions,
                                Some(&file_name),
                                Some(relative_path),
                                Some("file"),
                                None, // S3对象没有modified_epoch属性
                                Some(size),
                                extension.clone().or(Some(String::new())).as_deref(),
                            );

                            if !skip_entry {
                                // 转换时间戳
                                let last_modified = datatime_to_i64(version.last_modified());

                                // 根据include_tags参数决定是否获取标签
                                let tags = if include_tags {
                                    self.get_object_tags(
                                        &self.bucket_name,
                                        key_str,
                                        version.version_id(),
                                    )
                                    .await
                                    .unwrap_or_default()
                                } else {
                                    None
                                };

                                // 构建并发送EntryEnum
                                let entry = Self::build_entry(
                                    file_name,
                                    relative_path,
                                    extension,
                                    size,
                                    last_modified,
                                    tags,
                                    version.version_id(),
                                    version.is_latest().unwrap_or(false),
                                    false,
                                    Some(version_count),
                                    false,
                                );

                                total_file_count.fetch_add(1, Ordering::Relaxed);

                                // 发送条目
                                // 记录发送的版本化对象EntryEnum
                                trace!("[S3] 发送版本化对象 EntryEnum : {:?}", entry);

                                if tx
                                    .send(StorageEntryMessage::Scanned(Arc::new(entry)))
                                    .await
                                    .is_err()
                                {
                                    return Err(StorageError::OperationError(
                                        "接收端已关闭".to_string(),
                                    ));
                                }
                            }
                        }
                    }
                    VersionOrDeleteMarker::DeleteMarker(delete_marker) => {
                        // 处理删除标记
                        if let Some(key_str) = delete_marker.key() {
                            // 计算相对路径
                            let relative_path = self.calculate_relative_path(key_str);
                            let (file_name, _) = Self::get_file_info(relative_path);

                            // 转换时间戳
                            let last_modified = datatime_to_i64(delete_marker.last_modified());

                            // 构建并发送EntryEnum（删除标记）
                            let entry = Self::build_entry(
                                file_name,
                                relative_path,
                                None,
                                0,
                                last_modified,
                                None,
                                delete_marker.version_id(),
                                delete_marker.is_latest().unwrap_or(false),
                                true,
                                Some(version_count),
                                false,
                            );

                            trace!("[S3] 发送删除标记 EntryEnum : {:?}", entry,);

                            // 发送条目
                            if tx
                                .send(StorageEntryMessage::Scanned(Arc::new(entry)))
                                .await
                                .is_err()
                            {
                                return Err(StorageError::OperationError(
                                    "接收端已关闭".to_string(),
                                ));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 通过rx接收到`DataChunk`, 并将其写入到目标S3文件中（`MULTIPART_THRESHOLD`以下的文件一次性写入整个文件）
    pub async fn write_singlepart_data(
        &self,
        rx: mpsc::Receiver<DataChunk>,
        relative_path: &str,
        last_modified: i64,
        tags: Option<Vec<Tag>>,
    ) -> Result<usize> {
        debug!("Starting write_s3_data_task for file {:?}", relative_path);

        let mut reader = rx;

        // Collect incoming Bytes handles without copying — each push() only bumps
        // the Arc-style refcount inside Bytes, not the underlying data.
        let mut chunks: Vec<Bytes> = Vec::new();
        let mut expected_offset: u64 = 0;
        let mut data_valid = true;

        while let Some(chunk) = reader.recv().await {
            let length = chunk.data.len() as u64;

            trace!(
                "Received chunk of {} bytes at offset {} for file {:?}",
                length, chunk.offset, relative_path
            );

            // 确保数据块按顺序排列且没有间隙
            if chunk.offset != expected_offset {
                error!(
                    "Data chunks are not contiguous: expected offset {}, got {}",
                    expected_offset, chunk.offset
                );
                data_valid = false;
                break;
            }

            expected_offset += length;
            chunks.push(chunk.data); // zero-copy: refcount bump only
        }

        if !data_valid {
            error!(
                "File data is invalid due to non-contiguous chunks, skipping write operation for {:?}",
                relative_path
            );
            return Err(StorageError::OperationError(
                "Data chunks are not contiguous or in order".to_string(),
            ));
        }

        debug!(
            "Collected all {} chunks ({} bytes) for file {:?}",
            chunks.len(),
            expected_offset,
            relative_path
        );

        // 一次性写入整个文件
        let mut dest_file = self.create(relative_path, last_modified, tags);

        // Upload strategy (zero-copy throughout):
        //   • 0 chunks → empty body via trait write()
        //   • 1 chunk  → hand the Bytes handle straight through (zero-copy)
        //   • N chunks → stream each Bytes directly to S3 via ChunkedBody;
        //                no contiguous merge buffer is ever allocated
        let written = match chunks.len() {
            0 => self.write(&mut dest_file, Bytes::new()).await?,
            1 => {
                let chunk = chunks
                    .into_iter()
                    .next()
                    .ok_or_else(|| StorageError::S3Error("No chunks found".to_string()))?;
                self.write(&mut dest_file, chunk).await?
            }
            _ => {
                debug!(
                    "Streaming {} chunks to S3 for file {:?} without copy",
                    chunks.len(),
                    relative_path
                );
                self.put_singlepart_streaming(&dest_file, chunks).await?
            }
        };

        debug!(
            "Wrote {} bytes to destination file {:?} in a single operation",
            written, relative_path
        );

        Ok(written)
    }

    /// Streams a pre-collected list of `Bytes` chunks to S3 as a single-part upload
    /// without copying them into a contiguous buffer. Each `Bytes` in `chunks` is
    /// yielded directly to the AWS SDK via `ChunkedBody`.
    async fn put_singlepart_streaming(
        &self,
        file: &S3FileHandle,
        chunks: Vec<Bytes>,
    ) -> Result<usize> {
        let total_size: u64 = chunks.iter().map(|b| b.len() as u64).sum();
        let body = ChunkedBody {
            chunks: VecDeque::from(chunks),
            total_size,
        };
        let stream = aws_sdk_s3::primitives::ByteStream::from_body_1_x(body);

        let mut put_object_builder = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&file.key)
            .body(stream)
            .content_length(total_size as i64)
            .metadata("last-modified", file.last_modified.clone());

        if let Some(tags) = &file.tags
            && !tags.is_empty()
        {
            put_object_builder = put_object_builder.tagging(build_tagging_str(tags));
        }

        put_object_builder.send().await.map_err(|e| {
            error!(
                "s3 streaming write error, file key is {}, error is {e:?}",
                file.key
            );
            StorageError::S3Error(format!("写入对象 {} 失败: {:?}", file.key, e))
        })?;

        Ok(total_size as usize)
    }

    /// 写入数据到目标S3文件（`MULTIPART_THRESHOLD`以上的文件使用multipart上传）
    pub async fn write_multipart_data(
        &self,
        rx: mpsc::Receiver<DataChunk>,
        relative_path: &str,
        tags: Option<Vec<Tag>>,
    ) -> Result<usize> {
        debug!(
            "Starting write_s3_multipart_data_task for file {:?}",
            relative_path
        );

        let mut reader = rx;

        let key = self.build_full_key(relative_path);

        // 创建multipart上传
        let upload_id = match self.create_multipart_upload(&key, tags.as_ref()).await {
            Ok(id) => id,
            Err(e) => {
                error!(
                    "Failed to create multipart upload for file {:?}: {:?}",
                    relative_path, e
                );
                return Err(e);
            }
        };

        // 用于存储已上传分块的信息
        let parts = Arc::new(Mutex::new(Vec::new()));
        let mut part_number = 1;
        let mut expected_offset = 0;
        let mut data_valid = true; // 标记数据块是否有效

        // 用于累积数据块的缓冲区
        let mut buffer_chunks = Vec::new();
        let mut buffer_size = 0;

        // 限制并发上传的数量
        let concurrency = MAX_CONCURRENCY;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

        // 存储上传任务的句柄
        let mut upload_handles = Vec::new();

        // 处理从通道接收的所有数据块
        while let Some(chunk) = reader.recv().await {
            let length = chunk.data.len() as u64;

            trace!(
                "Received chunk of {} bytes at offset {} for file {:?}",
                length, chunk.offset, relative_path
            );

            // 确保数据块按顺序排列且没有间隙
            if chunk.offset != expected_offset {
                error!(
                    "Data chunks are not contiguous: expected offset {}, got {}",
                    expected_offset, chunk.offset
                );
                data_valid = false;
                // 即使数据无效，也要尝试接收完所有数据块
                expected_offset = chunk.offset + length;
                continue;
            }

            expected_offset += length;

            // 将数据块添加到缓冲区
            buffer_chunks.push(chunk.data);
            buffer_size += length;

            // 当缓冲区大小达到或超过CHUNK_SIZE时，上传该分块
            if buffer_size >= self.block_size {
                debug!(
                    "Uploading buffered part {} with size {} bytes (reached chunk size)",
                    part_number, buffer_size
                );

                // 克隆必要的变量用于异步任务
                let self_clone = self.clone();
                let key_clone = key.clone();
                let upload_id_clone = upload_id.clone();
                let part_number_clone = part_number;
                let buffer_chunks_clone = buffer_chunks;
                let buffer_size_clone = buffer_size;
                let parts_clone = parts.clone();
                let semaphore_clone = semaphore.clone();

                // 获取信号量许可
                let permit = semaphore_clone.acquire_owned().await.map_err(|_| {
                    StorageError::S3Error("Semaphore closed unexpectedly".to_string())
                })?;

                // 创建异步上传任务
                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    let etag = match self_clone
                        .upload_part_with_stream(
                            &key_clone,
                            &upload_id_clone,
                            part_number_clone,
                            buffer_chunks_clone,
                            buffer_size_clone,
                        )
                        .await
                    {
                        Ok(tag) => tag,
                        Err(e) => {
                            error!(
                                "Failed to upload part {} for file: {:?}",
                                part_number_clone, e
                            );
                            return Err(e);
                        }
                    };

                    // 保存分块信息
                    let mut parts = parts_clone.lock().await;
                    parts.push(
                        CompletedPart::builder()
                            .part_number(part_number_clone)
                            .e_tag(etag)
                            .build(),
                    );
                    Ok(())
                });

                // 存储上传任务的句柄
                upload_handles.push(handle);
                part_number += 1;

                // 清空缓冲区，准备下一个分块
                buffer_chunks = Vec::new();
                buffer_size = 0;
            }
        }

        // 上传剩余的数据（如果有）
        if !buffer_chunks.is_empty() {
            debug!(
                "Uploading final part {} with size {} bytes",
                part_number, buffer_size
            );

            // 克隆必要的变量用于异步任务
            let self_clone = self.clone();
            let key_clone = key.clone();
            let upload_id_clone = upload_id.clone();
            let part_number_clone = part_number;
            let buffer_chunks_clone = buffer_chunks;
            let buffer_size_clone = buffer_size;
            let parts_clone = parts.clone();
            let semaphore_clone = semaphore.clone();

            // 获取信号量许可
            let permit = semaphore_clone
                .acquire_owned()
                .await
                .map_err(|_| StorageError::S3Error("Semaphore closed unexpectedly".to_string()))?;

            // 创建异步上传任务
            let handle = tokio::spawn(async move {
                let _permit = permit;
                let etag = match self_clone
                    .upload_part_with_stream(
                        &key_clone,
                        &upload_id_clone,
                        part_number_clone,
                        buffer_chunks_clone,
                        buffer_size_clone,
                    )
                    .await
                {
                    Ok(tag) => tag,
                    Err(e) => {
                        error!(
                            "Failed to upload final part {} for file: {:?}",
                            part_number_clone, e
                        );
                        return Err(e);
                    }
                };

                // 保存分块信息
                let mut parts = parts_clone.lock().await;
                parts.push(
                    CompletedPart::builder()
                        .part_number(part_number_clone)
                        .e_tag(etag)
                        .build(),
                );
                Ok(())
            });

            // 存储上传任务的句柄
            upload_handles.push(handle);
        }

        // 检查数据是否有效（在完成上传前，但需先等待所有任务结束以避免泄漏）
        if !data_valid {
            error!(
                "File data is invalid due to non-contiguous chunks, aborting multipart upload for {:?}",
                relative_path
            );
            // 等待所有进行中的上传任务结束，避免泄漏
            for handle in upload_handles {
                let _ = handle.await;
            }
            if let Err(abort_err) = self.abort_multipart_upload(&key, &upload_id).await {
                error!("Failed to abort multipart upload: {:?}", abort_err);
            }
            return Err(StorageError::OperationError(
                "Data chunks are not contiguous or in order".to_string(),
            ));
        }

        self.finish_multipart_upload(&key, &upload_id, upload_handles, parts)
            .await?;

        debug!(
            "Successfully completed multipart upload for file {:?}",
            relative_path
        );
        Ok(expected_offset as usize)
    }

    /// 创建分块上传请求
    pub async fn create_multipart_upload(
        &self,
        key: &str,
        tags: Option<&Vec<Tag>>,
    ) -> Result<String> {
        debug!("Creating multipart upload for key: {}", key);

        let client = self.client.clone();

        let mut create_multipart_upload_builder = client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(key);

        // 如果tags存在且不为空，添加tagging到请求中
        if let Some(tags) = tags
            && !tags.is_empty()
        {
            create_multipart_upload_builder =
                create_multipart_upload_builder.tagging(build_tagging_str(tags));
        }

        let response = create_multipart_upload_builder.send().await.map_err(|e| {
            error!("Failed to create multipart upload: {}", e);
            StorageError::S3Error(format!("Failed to create multipart upload: {e}"))
        })?;

        let upload_id = response
            .upload_id
            .ok_or_else(|| StorageError::S3Error("Upload ID not found in response".to_string()))?;

        debug!("Created multipart upload with ID: {}", upload_id);
        Ok(upload_id)
    }

    async fn upload_part_with_stream(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        chunks: Vec<Bytes>,
        size: u64,
    ) -> Result<String> {
        debug!(
            "Uploading part {} for key {}, size: {} bytes",
            part_number, key, size
        );

        let client = self.client.clone();

        // 创建ChunkedBody并上传
        let body = ChunkedBody {
            chunks: VecDeque::from(chunks),
            total_size: size,
        };
        let stream = aws_sdk_s3::primitives::ByteStream::from_body_1_x(body);

        let response = client
            .upload_part()
            .bucket(&self.bucket_name)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .content_length(size as i64)
            .body(stream)
            .send()
            .await
            .map_err(|e| {
                error!(
                    "Failed to upload part {} for key {}: {}",
                    part_number, key, e
                );
                StorageError::S3Error(format!("Failed to upload part {part_number}: {e}"))
            })?;

        let etag = response
            .e_tag
            .ok_or_else(|| StorageError::S3Error("ETag not found in response".to_string()))?;

        debug!(
            "Uploaded part {} for key {} with ETag: {}",
            part_number, key, etag
        );
        Ok(etag)
    }

    /// 完成分块上传
    pub async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[CompletedPart], // (part_number, etag) 的元组列表
    ) -> Result<usize> {
        debug!(
            "Completing multipart upload for key: {}, upload_id: {}",
            key, upload_id
        );

        let client = self.client.clone();
        let bucket = &self.bucket_name;

        // 构建完成上传请求
        let mut complete_request = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id);

        // 设置已完成的分块列表
        let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(parts.to_vec()))
            .build();

        complete_request = complete_request.multipart_upload(completed_multipart_upload);

        // 发送完成上传请求
        complete_request.send().await.map_err(|e| {
            error!("Failed to complete multipart upload: {}", e);
            StorageError::S3Error(format!("Failed to complete multipart upload: {e}"))
        })?;

        debug!("Successfully completed multipart upload for key: {}", key);

        // 返回0作为成功状态，实际大小可以根据需求从响应中获取
        Ok(0)
    }

    /// 获取S3对象的元数据
    ///
    /// 该方法根据提供的相对路径构建S3对象的键，然后使用HEAD请求获取对象的元数据。
    /// 元数据包括文件名、扩展名、大小、最后修改时间和标签。
    ///
    /// # 参数
    /// - `relative_path`: 指向S3对象的相对路径，来自`StorageEntry`的`relative_path`
    ///
    /// # 返回值
    /// - `Result<EntryEnum>`: 包含S3对象元数据的`EntryEnum`结构体
    pub(crate) async fn get_metadata(&self, relative_path: &str) -> Result<EntryEnum> {
        debug!("Getting metadata for S3 object: {:?}", relative_path);

        let key = self.build_full_key(relative_path);

        debug!("Constructed S3 key: {}", key);

        let head_object_builder = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(&key);

        let response = head_object_builder.send().await.map_err(|e| {
            // HeadObject 404 → 以 FileNotFound 上报，让 integrity-check 区分"确实不存在"
            // 与"瞬时错误"（连接断开/服务繁忙）。
            // 双判定：SDK 把 404 解析为 ServiceError::NotFound 时走 is_not_found()；
            // 罕见情况下未解析（如 Unhandled）则回退到原始 HTTP 状态。
            let is_404 = matches!(
                &e,
                aws_sdk_s3::error::SdkError::ServiceError(svc) if svc.err().is_not_found()
            ) || e.raw_response().is_some_and(|r| r.status().as_u16() == 404);
            if is_404 {
                debug!("S3 object not found: {}", key);
                return StorageError::FileNotFound(key.clone());
            }
            error!("Failed to get metadata for S3 object {}: {:?}", key, e);
            StorageError::S3Error(format!("Failed to get metadata for object {key}: {e:?}"))
        })?;

        // 从相对路径计算文件名和扩展名（不使用含 prefix 的 full key）
        let (file_name, extension) = Self::get_file_info(relative_path);

        let size = response.content_length().unwrap_or(0) as u64;

        let last_modified = datatime_to_i64(response.last_modified());

        let tags = self
            .get_object_tags(&self.bucket_name, &key, None)
            .await
            .unwrap_or_default();

        let entry = Self::build_entry(
            file_name,
            relative_path,
            extension,
            size,
            last_modified,
            tags,
            None,
            false,
            false,
            None,
            false,
        );

        debug!("Successfully retrieved metadata for S3 object: {:?}", key);
        Ok(entry)
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::unused_async)]
    pub async fn walkdir(
        &self,
        sub_path: Option<&str>,
        depth: Option<usize>,
        match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>,
        concurrency: usize,
        include_tags: bool,
        packaged: bool,
        package_depth: usize,
    ) -> Result<WalkDirAsyncIterator> {
        debug!(
            "[S3] 开始执行walkdir，并发度: {}, bucket: {}, 深度: {:?}, sub_path: {:?}",
            concurrency, self.bucket_name, depth, sub_path
        );

        let (tx, rx) = async_channel::bounded(1000);

        // 全局文件总数计数器
        let total_file_count = Arc::new(AtomicUsize::new(0));

        // 如果指定了子路径，调整 prefix 以从子目录开始遍历
        let mut self_clone = self.clone();
        if let Some(p) = sub_path {
            let full_key = self.build_full_key(p);
            self_clone.prefix = Some(full_key);
        }
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = self_clone
                .iterative_walkdir(
                    tx_clone.clone(),
                    depth,
                    match_expressions,
                    exclude_expressions,
                    concurrency,
                    include_tags,
                    self_clone.is_bucket_versioned,
                    total_file_count,
                    packaged,
                    package_depth,
                )
                .await
            {
                error!("[S3] 迭代式遍历失败: {:?}", e);
                let _ = tx_clone
                    .send(StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: std::path::PathBuf::new(),
                        reason: format!("{e:?}"),
                    })
                    .await;
            }
        });

        Ok(WalkDirAsyncIterator::new(rx))
    }

    /// 迭代式目录遍历函数，使用工作窃取队列实现高效并发
    #[allow(clippy::too_many_arguments)]
    async fn iterative_walkdir(
        &self,
        tx: async_channel::Sender<StorageEntryMessage>,
        depth: Option<usize>,
        match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>,
        concurrency: usize,
        include_tags: bool,
        is_versioned: bool,
        total_file_count: Arc<AtomicUsize>,
        packaged: bool,
        package_depth: usize,
    ) -> Result<()> {
        let start_prefix = self.prefix.clone().unwrap_or_default();
        debug!("[S3] 使用起始前缀: {:?}", start_prefix);

        let contexts =
            create_worker_contexts(concurrency, (start_prefix, 0usize, true, None::<usize>)).await;
        debug!("[S3] 调整后的并发度: {} (限制在1-64之间)", contexts.len());

        let mut handles = Vec::with_capacity(contexts.len());
        for ctx in contexts {
            let self_clone = self.clone();
            let tx_clone = tx.clone();
            let match_expr_clone = match_expressions.clone();
            let exclude_expr_clone = exclude_expressions.clone();
            let total_file_count_clone = Arc::clone(&total_file_count);

            handles.push(tokio::spawn(async move {
                run_worker_loop(
                    &ctx,
                    |(prefix, current_depth, skip_filter, package_remaining)| {
                        self_clone.process_dir(
                            ctx.worker_id,
                            prefix,
                            current_depth,
                            &tx_clone,
                            &ctx,
                            match_expr_clone.as_ref(),
                            exclude_expr_clone.as_ref(),
                            depth,
                            include_tags,
                            is_versioned,
                            &total_file_count_clone,
                            skip_filter,
                            packaged,
                            package_depth,
                            package_remaining,
                        )
                    },
                    |task| task.0.clone(),
                )
                .await;
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        Ok(())
    }

    /// 处理单个目录（S3 中的前缀），读取条目并过滤，发送符合条件的EntryEnum
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::fn_params_excessive_bools)]
    async fn process_dir(
        &self,
        producer_id: usize,
        prefix: String,
        current_depth: usize,
        tx: &async_channel::Sender<StorageEntryMessage>,
        ctx: &crate::walk_scheduler::WorkerContext<(String, usize, bool, Option<usize>)>,
        match_expressions: Option<&FilterExpression>,
        exclude_expressions: Option<&FilterExpression>,
        depth_limit: Option<usize>,
        include_tags: bool,
        is_versioned: bool,
        total_file_count: &Arc<AtomicUsize>,
        skip_filter: bool,
        packaged: bool,
        package_depth: usize,
        package_remaining: Option<usize>,
    ) -> Result<()> {
        debug!(
            "[S3 Producer {}] 开始处理前缀: {}, 当前深度: {}, skip_filter: {}",
            producer_id, prefix, current_depth, skip_filter
        );

        // 如果当前目录的深度大于等于限制深度，直接返回，不处理该目录下的内容
        if depth_limit.is_some_and(|max_depth| {
            if current_depth >= max_depth {
                debug!(
                    "[S3 Producer {}] 当前目录 {} 深度 {} >= 限制深度 {}, 直接返回",
                    producer_id, prefix, current_depth, max_depth
                );
                true
            } else {
                false
            }
        }) {
            return Ok(());
        }

        if is_versioned {
            // 处理版本化桶，使用list_object_versions API
            let mut key_marker: Option<String> = None;
            let mut version_id_marker: Option<String> = None;
            let mut page_count = 0;

            loop {
                page_count += 1;
                debug!(
                    "[S3 Producer {}] 处理版本化桶 {} 前缀 {} 的第 {} 页，key_marker: {:?}, version_id_marker: {:?}",
                    producer_id,
                    self.bucket_name,
                    prefix,
                    page_count,
                    key_marker,
                    version_id_marker
                );

                // 构建请求
                let mut request = self
                    .client
                    .list_object_versions()
                    .bucket(&self.bucket_name)
                    .delimiter("/")
                    .prefix(&prefix);

                if let Some(marker) = &key_marker {
                    request = request.key_marker(marker);
                    if let Some(version_marker) = &version_id_marker {
                        request = request.version_id_marker(version_marker);
                    }
                }

                // 发送请求并获取响应
                debug!(
                    "[S3 Producer {}] 发送S3 list_object_versions请求, 前缀: {}",
                    producer_id, prefix
                );
                let response = match request.send().await {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!("[S3 Producer {}] 请求失败，错误: {:?}", producer_id, e);
                        // 非致命错误，记录并继续
                        return Ok(());
                    }
                };

                // 处理公共前缀（目录）
                let mut subdir_count = 0;
                for prefix_data in response.common_prefixes() {
                    if let Some(prefix_name) = prefix_data.prefix() {
                        self.process_directory(
                            ctx,
                            producer_id,
                            prefix_name,
                            current_depth,
                            depth_limit,
                            skip_filter,
                            match_expressions,
                            exclude_expressions,
                            packaged,
                            package_depth,
                            package_remaining,
                            tx,
                            total_file_count,
                        )
                        .await?;
                        subdir_count += 1;
                    }
                }
                debug!(
                    "[S3 Producer {}] 共处理 {} 个子目录",
                    producer_id, subdir_count
                );

                // 收集同一对象的所有版本和删除标记
                let mut version_entries = Vec::new();
                let mut delete_marker_entries = Vec::new();

                // 收集版本对象
                for version in response.versions() {
                    if version.key().is_some_and(|key| key != prefix) {
                        version_entries.push(version.clone());
                    }
                }

                // 收集删除标记
                for delete_marker in response.delete_markers() {
                    if delete_marker.key().is_some_and(|key| key != prefix) {
                        delete_marker_entries.push(delete_marker.clone());
                    }
                }

                // 处理版本对象和删除标记，按对象分组并按时间排序
                self.process_versioned_entries(
                    tx,
                    &version_entries,
                    &delete_marker_entries,
                    include_tags,
                    match_expressions,
                    exclude_expressions,
                    total_file_count.clone(),
                )
                .await?;

                // 更新分页标记
                key_marker = response
                    .next_key_marker()
                    .map(std::string::ToString::to_string);
                version_id_marker = response
                    .next_version_id_marker()
                    .map(std::string::ToString::to_string);

                // 检查是否还有更多页面
                if key_marker.is_none() {
                    debug!(
                        "[S3 Producer {}] 处理前缀 {} 完成，没有更多页面",
                        producer_id, prefix
                    );
                    break; // 没有更多数据了，退出循环
                }
            }

            debug!(
                "[S3 Producer {}] 完成前缀 {} 的处理，全局累计处理 {} 个文件",
                producer_id,
                prefix,
                total_file_count.load(Ordering::Relaxed),
            );
            Ok(())
        } else {
            // 处理非版本化桶，使用传统的list_objects_v2 API
            let mut continuation_token: Option<String> = None;
            let mut page_count = 0;

            loop {
                page_count += 1;
                debug!(
                    "[S3 Producer {}] 处理桶 {} 前缀 {} 的第 {} 页，continuation_token: {:?}",
                    producer_id, self.bucket_name, prefix, page_count, continuation_token
                );

                // 构建请求，增加批处理大小以减少请求次数
                let mut request = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket_name)
                    .delimiter("/")
                    .prefix(&prefix); // 增加每次请求的最大键数量

                if let Some(token) = &continuation_token {
                    request = request.continuation_token(token);
                }

                // 发送请求并获取响应
                debug!(
                    "[S3 Producer {}] 发送S3 list_objects_v2请求, 前缀: {}",
                    producer_id, prefix
                );
                let response = match request.send().await {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!("[S3 Producer {}] 请求失败，错误: {:?}", producer_id, e);
                        // 非致命错误，记录并继续
                        return Ok(());
                    }
                };

                // 处理当前响应页中的公共前缀（CommonPrefixes）
                let mut subdir_count = 0;
                for prefix_data in response.common_prefixes() {
                    if let Some(prefix_name) = prefix_data.prefix() {
                        self.process_directory(
                            ctx,
                            producer_id,
                            prefix_name,
                            current_depth,
                            depth_limit,
                            skip_filter,
                            match_expressions,
                            exclude_expressions,
                            packaged,
                            package_depth,
                            package_remaining,
                            tx,
                            total_file_count,
                        )
                        .await?;
                        subdir_count += 1;
                    }
                }
                debug!(
                    "[S3 Producer {}] 共处理 {} 个子目录",
                    producer_id, subdir_count
                );

                // 处理当前响应页中的对象（Contents）并立即发送
                let mut processed_files = 0;
                for obj in response.contents() {
                    if let Some(key) = obj.key() {
                        if prefix == key {
                            continue;
                        }

                        let size = obj.size().unwrap_or(0) as u64;

                        // 转换时间戳
                        let last_modified = datatime_to_i64(obj.last_modified());

                        let extension = PathBuf::from(key)
                            .extension()
                            .map(|ext| ext.to_string_lossy().to_string());

                        self.process_object(
                            tx,
                            producer_id,
                            key,
                            size,
                            last_modified,
                            extension,
                            include_tags,
                            skip_filter,
                            match_expressions,
                            exclude_expressions,
                            total_file_count,
                        )
                        .await?;

                        processed_files += 1;
                    }
                }
                debug!(
                    "[S3 Producer {}] 本页处理 {} 个文件，全局累计文件数: {}",
                    producer_id,
                    processed_files,
                    total_file_count.load(Ordering::Relaxed)
                );

                // 检查是否还有更多页面
                continuation_token = response
                    .next_continuation_token()
                    .map(std::string::ToString::to_string);
                if continuation_token.is_none() {
                    debug!(
                        "[S3 Producer {}] 处理前缀 {} 完成，没有更多页面",
                        producer_id, prefix
                    );
                    break; // 没有更多数据了，退出循环
                }
                debug!(
                    "[S3 Producer {}] 将继续处理前缀 {} 的下一页",
                    producer_id, prefix
                );
            }

            debug!(
                "[S3 Producer {}] 完成前缀 {} 的处理，全局累计处理 {} 个文件",
                producer_id,
                prefix,
                total_file_count.load(Ordering::Relaxed),
            );
            Ok(())
        }
    }

    /// 单次 Range GET 并把响应 body 收成 `Bytes`。
    ///
    /// 共享 helper：被 [`Self::read`] 与 [`Self::read_data`] 内联 future 复用，避免
    /// 两处 `GetObject + collect` 模板漂移（未来加 metric / retry / observability 只改这）。
    /// `version_id = None` 时不附带版本约束（最常见 case）。
    pub(crate) async fn read_range_uncached(
        &self,
        key: &str,
        version_id: Option<&str>,
        offset: u64,
        count: u64,
    ) -> Result<Bytes> {
        // Defensive: count = 0 would produce `bytes=N-(N-1)` which wraps to
        // `bytes=N-18446744073709551615` (u64 underflow) and the server either
        // returns 416 Requested Range Not Satisfiable or, worse, serves the
        // entire object. Internal callers today gate count > 0 themselves,
        // but with read_range_uncached now pub(crate) we make the contract
        // explicit at the helper.
        if count == 0 {
            return Ok(Bytes::new());
        }
        let mut builder = self.client.get_object().bucket(&self.bucket_name).key(key);
        if let Some(v) = version_id {
            builder = builder.version_id(v);
        }
        let range_header = format!("bytes={}-{}", offset, offset + count - 1);
        builder = builder.range(range_header);

        let resp = builder.send().await.map_err(|e| {
            StorageError::S3Error(format!("读取对象 {key} 的偏移量 {offset} 失败: {e:?}"))
        })?;
        let body = resp
            .body
            .collect()
            .await
            .map_err(|e| StorageError::S3Error(format!("收集对象内容失败: {e:?}")))?;
        Ok(body.into_bytes())
    }

    async fn read(&self, file: &mut S3FileHandle, offset: u64, count: usize) -> Result<Bytes> {
        if count == 0 {
            return Ok(Bytes::new());
        }
        self.read_range_uncached(&file.key, file.version_id.as_deref(), offset, count as u64)
            .await
    }

    // 将数据data(类型是Bytes)一次性写入对象
    async fn write(&self, file: &mut S3FileHandle, data: Bytes) -> Result<usize> {
        let length = data.len();
        let mut put_object_builder = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&file.key)
            // 将Bytes转换为ByteStream以匹配AWS SDK要求
            .body(aws_sdk_s3::primitives::ByteStream::from(data))
            .metadata("last-modified", file.last_modified.clone());

        // 如果tags存在且不为空，添加tagging到请求中
        if let Some(tags) = &file.tags
            && !tags.is_empty()
        {
            put_object_builder = put_object_builder.tagging(build_tagging_str(tags));
        }

        let response = put_object_builder.send().await.map_err(|e| {
            error!("s3 write error, file key is {}, error is {e:?}", file.key);
            StorageError::S3Error(format!("写入对象 {} 失败: {:?}", file.key, e))
        })?;

        // 保存新生成的version_id到S3FileHandle中
        if let Some(version_id) = response.version_id() {
            file.version_id = Some(version_id.to_string());
        }

        Ok(length)
    }

    // ============================================================
    // walkdir_2: 目录分页 + NDX 编号 + 并行预读
    // ============================================================

    /// 读取单个 S3 "目录"（prefix），返回排序后的 files + subdirs。
    pub(crate) async fn read_dir_sorted(
        &self,
        dir_path: &str,
        handle: &crate::dir_tree::DirHandle,
        ctx: &crate::dir_tree::ReadContext,
    ) -> Result<crate::dir_tree::ReadResult> {
        use crate::dir_tree::{DirHandle, ReadResult, SubdirEntry};

        let prefix_key = match handle {
            DirHandle::S3Prefix(p) => self.build_full_key(p),
            _ => {
                return Err(StorageError::OperationError(
                    "DirHandle type mismatch: expected S3Prefix".into(),
                ));
            }
        };

        let mut files: Vec<Arc<EntryEnum>> = Vec::new();
        let mut subdirs: Vec<SubdirEntry> = Vec::new();
        let mut errors: Vec<String> = Vec::new();

        if ctx.is_versioned {
            // === Versioned 模式 ===
            let mut key_marker: Option<String> = None;
            let mut version_id_marker: Option<String> = None;

            loop {
                let mut request = self
                    .client
                    .list_object_versions()
                    .bucket(&self.bucket_name)
                    .prefix(&prefix_key)
                    .delimiter("/");

                if let Some(km) = &key_marker {
                    request = request.key_marker(km);
                }
                if let Some(vim) = &version_id_marker {
                    request = request.version_id_marker(vim);
                }

                let response = match request.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        errors.push(format!("list_object_versions failed: {e:?}"));
                        break;
                    }
                };

                // common_prefixes → subdirs（带 filter 三元组处理）
                for cp in response.common_prefixes() {
                    if let Some(pfx) = cp.prefix() {
                        let rel = self.calculate_relative_path(pfx);
                        let clean_rel = rel.trim_end_matches('/');
                        let (dir_name, _) = Self::get_file_info(clean_rel);

                        let (skip_entry, continue_scan, need_submatch) = if ctx.apply_filter {
                            should_skip(
                                ctx.match_expr.as_ref().as_ref(),
                                ctx.exclude_expr.as_ref().as_ref(),
                                Some(&dir_name),
                                Some(clean_rel),
                                Some("dir"),
                                None,
                                None,
                                None,
                            )
                        } else {
                            (false, true, false)
                        };

                        if skip_entry && !continue_scan {
                            continue;
                        }

                        let entry = Self::build_entry(
                            dir_name, clean_rel, None, 0, 0, None, None, true, false, None, true,
                        );
                        subdirs.push(SubdirEntry {
                            entry: Arc::new(entry),
                            visible: !skip_entry,
                            need_filter: need_submatch,
                        });
                    }
                }

                // versions → 分组处理
                let mut version_groups: HashMap<String, Vec<(i64, ObjectVersion)>> = HashMap::new();
                for ver in response.versions() {
                    if let Some(key) = ver.key() {
                        let ts = datatime_to_i64(ver.last_modified());
                        version_groups
                            .entry(key.to_string())
                            .or_default()
                            .push((ts, ver.clone()));
                    }
                }

                // delete markers
                let mut delete_markers: HashMap<String, Vec<DeleteMarkerEntry>> = HashMap::new();
                for dm in response.delete_markers() {
                    if let Some(key) = dm.key() {
                        delete_markers
                            .entry(key.to_string())
                            .or_default()
                            .push(dm.clone());
                    }
                }

                // 处理每个 object 的版本
                for (key, mut versions) in version_groups {
                    if key == prefix_key {
                        continue;
                    }
                    // 检查最新版本是否是 delete marker
                    if let Some(dms) = delete_markers.get(&key)
                        && !dms.is_empty()
                    {
                        // 如果有 delete marker，检查是否是最新的
                        let latest_dm_ts: i64 = dms
                            .iter()
                            .filter_map(|d| d.last_modified())
                            .map(|t| datatime_to_i64(Some(t)))
                            .max()
                            .unwrap_or(0);
                        let latest_ver_ts = versions.iter().map(|(ts, _)| *ts).max().unwrap_or(0);
                        if latest_dm_ts > latest_ver_ts {
                            continue; // 已删除，跳过
                        }
                    }

                    versions.sort_by(|a, b| a.0.cmp(&b.0)); // 按 mtime 升序：旧版本在前（NDX 小）
                    let version_count = versions.len() as u32;
                    let last_idx = versions.len().saturating_sub(1);

                    for (i, (ts, ver)) in versions.into_iter().enumerate() {
                        let rel = self.calculate_relative_path(ver.key().unwrap_or_default());
                        let (file_name, extension) = Self::get_file_info(rel);
                        let is_latest = i == last_idx; // 最后一个（mtime 最大）是 latest
                        let vid = ver.version_id().map(std::string::ToString::to_string);
                        let size = ver.size().unwrap_or(0) as u64;

                        // filter（仅 apply_filter 时）
                        let (skip, _, _) = if ctx.apply_filter {
                            should_skip(
                                ctx.match_expr.as_ref().as_ref(),
                                ctx.exclude_expr.as_ref().as_ref(),
                                Some(&file_name),
                                Some(rel),
                                Some("file"),
                                Some(crate::time_util::nanos_to_secs(ts)),
                                Some(size),
                                extension.as_deref().or(Some("")),
                            )
                        } else {
                            (false, true, false)
                        };
                        if skip {
                            continue;
                        }

                        let tags = if ctx.include_tags {
                            self.get_object_tags(
                                &self.bucket_name,
                                ver.key().unwrap_or_default(),
                                vid.as_deref(),
                            )
                            .await
                            .ok()
                            .flatten()
                        } else {
                            None
                        };

                        let entry = Self::build_entry(
                            file_name,
                            rel,
                            extension,
                            size,
                            ts,
                            tags,
                            vid.as_deref(),
                            is_latest,
                            false,
                            Some(version_count),
                            false,
                        );
                        files.push(Arc::new(entry));
                    }
                }

                key_marker = response
                    .next_key_marker()
                    .map(std::string::ToString::to_string);
                version_id_marker = response
                    .next_version_id_marker()
                    .map(std::string::ToString::to_string);
                if key_marker.is_none() {
                    break;
                }
            }
        } else {
            // === 非 Versioned 模式 ===
            let mut continuation_token: Option<String> = None;

            loop {
                let mut request = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket_name)
                    .prefix(&prefix_key)
                    .delimiter("/");

                if let Some(token) = &continuation_token {
                    request = request.continuation_token(token);
                }

                let response = match request.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        errors.push(format!("list_objects_v2 failed: {e:?}"));
                        break;
                    }
                };

                // common_prefixes → subdirs（带 filter 三元组处理）
                for cp in response.common_prefixes() {
                    if let Some(pfx) = cp.prefix() {
                        let rel = self.calculate_relative_path(pfx);
                        let clean_rel = rel.trim_end_matches('/');
                        if clean_rel.is_empty() {
                            continue;
                        }
                        let (dir_name, _) = Self::get_file_info(clean_rel);

                        let (skip_entry, continue_scan, need_submatch) = if ctx.apply_filter {
                            should_skip(
                                ctx.match_expr.as_ref().as_ref(),
                                ctx.exclude_expr.as_ref().as_ref(),
                                Some(&dir_name),
                                Some(clean_rel),
                                Some("dir"),
                                None,
                                None,
                                None,
                            )
                        } else {
                            (false, true, false)
                        };

                        if skip_entry && !continue_scan {
                            continue;
                        }

                        let entry = Self::build_entry(
                            dir_name, clean_rel, None, 0, 0, None, None, true, false, None, true,
                        );
                        subdirs.push(SubdirEntry {
                            entry: Arc::new(entry),
                            visible: !skip_entry,
                            need_filter: need_submatch,
                        });
                    }
                }

                // contents → files
                for obj in response.contents() {
                    if let Some(key) = obj.key() {
                        if key == prefix_key || key.ends_with('/') {
                            continue;
                        }

                        let rel = self.calculate_relative_path(key);
                        let (file_name, extension) = Self::get_file_info(rel);
                        let size = obj.size().unwrap_or(0) as u64;
                        let mtime = datatime_to_i64(obj.last_modified());

                        // filter
                        let (skip, _, _) = if ctx.apply_filter {
                            should_skip(
                                ctx.match_expr.as_ref().as_ref(),
                                ctx.exclude_expr.as_ref().as_ref(),
                                Some(&file_name),
                                Some(rel),
                                Some("file"),
                                Some(crate::time_util::nanos_to_secs(mtime)),
                                Some(size),
                                extension.as_deref().or(Some("")),
                            )
                        } else {
                            (false, true, false)
                        };
                        if skip {
                            continue;
                        }

                        let tags = if ctx.include_tags {
                            self.get_object_tags(&self.bucket_name, key, None)
                                .await
                                .ok()
                                .flatten()
                        } else {
                            None
                        };

                        let entry = Self::build_entry(
                            file_name, rel, extension, size, mtime, tags, None, true, false, None,
                            false,
                        );
                        files.push(Arc::new(entry));
                    }
                }

                continuation_token = response
                    .next_continuation_token()
                    .map(std::string::ToString::to_string);
                if continuation_token.is_none() {
                    break;
                }
            }
        }

        // 排序：files 按 name（多版本已按 mtime 排好，同名不同版本保持 mtime 序）
        // 注意：versioned 模式下同一 key 的多个版本已按 mtime 升序排列
        files.sort_by(|a, b| a.get_name().cmp(b.get_name()));
        subdirs.sort_by(|a, b| a.entry.get_name().cmp(b.entry.get_name()));

        Ok(ReadResult {
            dir_path: dir_path.to_string(),
            files,
            subdirs,
            errors,
        })
    }

    /// `walkdir_2`: 目录分页遍历，DFS 顺序分配 NDX，页级输出
    #[allow(clippy::unused_async)]
    pub async fn walkdir_2(
        &self,
        sub_path: Option<&str>,
        depth: Option<usize>,
        match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>,
        concurrency: usize,
        include_tags: bool,
    ) -> Result<crate::WalkDirAsyncIterator2> {
        use crate::dir_tree::{DirHandle, ReadContext, ReadRequest, run_dfs_driver};

        let start_prefix = match sub_path {
            Some(p) if !p.is_empty() => p.to_string(),
            _ => String::new(),
        };

        let concurrency = concurrency.clamp(1, 64);
        let (req_tx, req_rx) = async_channel::bounded::<ReadRequest>(concurrency * 2);
        let (out_tx, out_rx) = async_channel::bounded(64);

        // 启动 Reader Worker
        for _ in 0..concurrency {
            let storage = self.clone();
            let rx = req_rx.clone();
            tokio::spawn(async move {
                while let Ok(req) = rx.recv().await {
                    let result = storage
                        .read_dir_sorted(&req.dir_path, &req.handle, &req.ctx)
                        .await;
                    let _ = req.reply.send(result);
                }
            });
        }

        let root_handle = DirHandle::S3Prefix(start_prefix);
        let root_path = std::path::PathBuf::new(); // S3 不需要 root_path
        let base_ctx = ReadContext {
            match_expr: Arc::new(match_expressions),
            exclude_expr: Arc::new(exclude_expressions),
            current_depth: 0,
            max_depth: depth.unwrap_or(0),
            apply_filter: true,
            include_tags,
            is_versioned: self.is_bucket_versioned,
        };

        tokio::spawn(run_dfs_driver(
            req_tx,
            out_tx,
            root_path,
            root_handle,
            base_ctx,
        ));

        Ok(crate::AsyncReceiver::new(out_rx))
    }
}

/// 将 Tag 列表构建为 URL 编码的 tagging 字符串，格式为 key1=value1&key2=value2
fn build_tagging_str(tags: &[Tag]) -> String {
    tags.iter()
        .enumerate()
        .map(|(i, t)| {
            if i == 0 {
                format!("{}={}", t.key, t.value)
            } else {
                format!("&{}={}", t.key, t.value)
            }
        })
        .collect()
}

/// 创建S3存储实例
pub async fn create_s3_storage(url: &str, block_size: Option<u64>) -> Result<StorageEnum> {
    let s3_storage = S3Storage::new(url, block_size).await?;
    Ok(StorageEnum::S3(s3_storage))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url_special_chars_in_secret_key() {
        // SK 包含 + 和 /（Base64 编码常见字符）
        let url = "s3://X9HENFMKAC41MT11J14H:AsxLb0dEhjxXIlKfVnCSVhM+hjO80rbhRmPLp/UK@bucket.192.168.3.210:10444";
        let (ak, sk, bucket, endpoint, prefix, host, storage_type, _tls_skip) =
            parse_s3_url(url).unwrap();
        assert_eq!(ak, "X9HENFMKAC41MT11J14H");
        assert_eq!(sk, "AsxLb0dEhjxXIlKfVnCSVhM+hjO80rbhRmPLp/UK");
        assert_eq!(bucket, "bucket");
        assert_eq!(endpoint, "http://192.168.3.210:10444");
        assert_eq!(prefix, "");
        assert_eq!(host, "192.168.3.210");
        assert!(matches!(storage_type, StorageType::S3));
    }

    #[test]
    fn test_parse_s3_url_normal() {
        let url = "s3://myak:mysk@mybucket.minio.example.com:9000/data/prefix";
        let (ak, sk, bucket, endpoint, prefix, host, storage_type, _tls_skip) =
            parse_s3_url(url).unwrap();
        assert_eq!(ak, "myak");
        assert_eq!(sk, "mysk");
        assert_eq!(bucket, "mybucket");
        assert_eq!(endpoint, "http://minio.example.com:9000");
        assert_eq!(prefix, "data/prefix/");
        assert_eq!(host, "minio.example.com");
        assert!(matches!(storage_type, StorageType::S3));
    }

    #[test]
    fn test_parse_s3_url_https() {
        let url = "s3+https://ak:sk@bucket.host.com:443/prefix";
        let (ak, sk, bucket, endpoint, _prefix, host, storage_type, _) = parse_s3_url(url).unwrap();
        assert_eq!(ak, "ak");
        assert_eq!(sk, "sk");
        assert_eq!(bucket, "bucket");
        assert_eq!(endpoint, "https://host.com:443");
        assert_eq!(host, "host.com");
        assert!(matches!(storage_type, StorageType::S3));
    }

    #[test]
    fn test_parse_s3_url_no_prefix() {
        let url = "s3://ak:sk@bucket.host.com:9000";
        let (ak, sk, bucket, endpoint, prefix, host, _, _) = parse_s3_url(url).unwrap();
        assert_eq!(ak, "ak");
        assert_eq!(sk, "sk");
        assert_eq!(bucket, "bucket");
        assert_eq!(endpoint, "http://host.com:9000");
        assert_eq!(prefix, "");
        assert_eq!(host, "host.com");
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
        let (.., prefix, _, _, _) = parse_s3_url(url).unwrap();
        assert_eq!(prefix, "a/b/c/");
    }

    #[test]
    fn test_parse_s3_url_prefix_trailing_slash() {
        let url = "s3://ak:sk@bucket.host.com:9000/prefix/";
        let (.., prefix, _, _, _) = parse_s3_url(url).unwrap();
        assert_eq!(prefix, "prefix/");
    }

    // --- tls_skip_verify 解析测试（s3+https scheme 自动跳过 TLS 验证）---

    #[test]
    fn test_parse_s3_url_https_skips_tls() {
        // s3+https scheme 自动启用跳过验证
        let url = "s3+https://ak:sk@bucket.host.com:443/prefix";
        let (.., tls_skip) = parse_s3_url(url).unwrap();
        assert!(tls_skip);
    }

    #[test]
    fn test_parse_s3_url_http_no_tls_skip() {
        // s3:// (http) 不跳过验证
        let url = "s3://ak:sk@bucket.host.com:9000/prefix";
        let (.., tls_skip) = parse_s3_url(url).unwrap();
        assert!(!tls_skip);
    }

    #[test]
    fn test_parse_s3_endpoint_url_https_skips_tls() {
        // s3+https scheme 自动启用跳过验证
        let url = "s3+https://ak:sk@host.com:9000";
        let (_, _, endpoint, tls_skip) = parse_s3_endpoint_url(url).unwrap();
        assert!(tls_skip);
        assert!(endpoint.starts_with("https://"));
    }

    #[test]
    fn test_parse_s3_endpoint_url_http_no_tls_skip() {
        // s3:// (http) 不跳过验证
        let url = "s3://ak:sk@host.com:9000";
        let (_, _, _, tls_skip) = parse_s3_endpoint_url(url).unwrap();
        assert!(!tls_skip);
    }

    // --- 构造测试用 S3Storage（不连接实际 S3 服务） ---

    fn make_test_storage(prefix: Option<&str>) -> S3Storage {
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
            storage_type: StorageType::S3,
            endpoint: "http://localhost:9000".to_string(),
            bucket_name: "test-bucket".to_string(),
            prefix: prefix.map(|s| s.to_string()),
            client,
            hcp_client: None,
            block_size: DEFAULT_BLOCK_SIZE,
            is_bucket_versioned: false,
        }
    }

    // --- build_full_key 测试 ---

    #[test]
    fn test_build_full_key_with_prefix() {
        let storage = make_test_storage(Some("data/prefix/"));
        assert_eq!(
            storage.build_full_key("dir/file.txt"),
            "data/prefix/dir/file.txt"
        );
    }

    #[test]
    fn test_build_full_key_without_prefix() {
        let storage = make_test_storage(None);
        assert_eq!(storage.build_full_key("dir/file.txt"), "dir/file.txt");
    }

    #[test]
    fn test_build_full_key_with_empty_relative_path() {
        let storage = make_test_storage(Some("data/"));
        assert_eq!(storage.build_full_key(""), "data/");
    }

    #[test]
    fn test_build_full_key_nested_prefix() {
        let storage = make_test_storage(Some("a/b/c/"));
        assert_eq!(storage.build_full_key("d/e.txt"), "a/b/c/d/e.txt");
    }

    #[test]
    fn test_build_full_key_root_file_with_prefix() {
        let storage = make_test_storage(Some("backup/"));
        assert_eq!(storage.build_full_key("readme.md"), "backup/readme.md");
    }

    // --- calculate_relative_path 测试 ---

    #[test]
    fn test_calculate_relative_path_with_prefix() {
        let storage = make_test_storage(Some("data/prefix/"));
        assert_eq!(
            storage.calculate_relative_path("data/prefix/dir/file.txt"),
            "dir/file.txt"
        );
    }

    #[test]
    fn test_calculate_relative_path_without_prefix() {
        let storage = make_test_storage(None);
        assert_eq!(
            storage.calculate_relative_path("dir/file.txt"),
            "dir/file.txt"
        );
    }

    #[test]
    fn test_calculate_relative_path_prefix_mismatch() {
        let storage = make_test_storage(Some("other/"));
        // prefix 不匹配时 fallback 返回原始路径
        assert_eq!(
            storage.calculate_relative_path("data/file.txt"),
            "data/file.txt"
        );
    }

    #[test]
    fn test_calculate_relative_path_exact_prefix() {
        let storage = make_test_storage(Some("data/prefix/"));
        // full_path 刚好等于 prefix 时，返回空字符串
        assert_eq!(storage.calculate_relative_path("data/prefix/"), "");
    }

    #[test]
    fn test_build_full_key_and_calculate_relative_path_roundtrip() {
        let storage = make_test_storage(Some("prefix/sub/"));
        let relative = "path/to/file.txt";
        let full_key = storage.build_full_key(relative);
        assert_eq!(full_key, "prefix/sub/path/to/file.txt");
        assert_eq!(storage.calculate_relative_path(&full_key), relative);
    }

    // --- get_file_info 测试 ---

    // `S3Storage::get_file_info` is a static method, no instance needed.

    #[test]
    fn test_get_file_info_with_extension() {
        let (name, ext) = S3Storage::get_file_info("dir/file.txt");
        assert_eq!(name, "file.txt");
        assert_eq!(ext, Some("txt".to_string()));
    }

    #[test]
    fn test_get_file_info_without_extension() {
        let (name, ext) = S3Storage::get_file_info("dir/file");
        assert_eq!(name, "file");
        assert_eq!(ext, None);
    }

    #[test]
    fn test_get_file_info_compound_extension() {
        let (name, ext) = S3Storage::get_file_info("file.tar.gz");
        assert_eq!(name, "file.tar.gz");
        assert_eq!(ext, Some("gz".to_string()));
    }

    #[test]
    fn test_get_file_info_root_level() {
        let (name, ext) = S3Storage::get_file_info("readme.md");
        assert_eq!(name, "readme.md");
        assert_eq!(ext, Some("md".to_string()));
    }

    // -- MultipartUpload wrapper tests ---------------------------------------
    // These tests don't talk to a real S3 (`localhost:9000` is a stub
    // endpoint). They only exercise the wrapper's local state machine —
    // accessors, sort-on-complete, Drop-guard tagging.

    #[test]
    fn test_multipart_upload_accessors_and_part_tracking() {
        let storage = make_test_storage(None);
        // Construct the wrapper directly (we're in the same module).
        let mut up = MultipartUpload {
            storage: &storage,
            key: "tenants/42/blob.bin".to_string(),
            upload_id: "fake-upload-id".to_string(),
            parts: Vec::new(),
            finished: true, // suppress Drop's spawned abort
        };

        assert_eq!(up.key(), "tenants/42/blob.bin");
        assert_eq!(up.upload_id(), "fake-upload-id");
        assert_eq!(up.part_count(), 0);

        // Simulate two parts having been recorded.
        up.parts.push(
            CompletedPart::builder()
                .part_number(2)
                .e_tag("\"etag-2\"")
                .build(),
        );
        up.parts.push(
            CompletedPart::builder()
                .part_number(1)
                .e_tag("\"etag-1\"")
                .build(),
        );
        assert_eq!(up.part_count(), 2);
    }

    #[test]
    fn test_multipart_upload_drop_when_finished_is_silent() {
        // With `finished=true`, Drop must early-return — no panic, no spawn.
        let storage = make_test_storage(None);
        {
            let _up = MultipartUpload {
                storage: &storage,
                key: "k".into(),
                upload_id: "u".into(),
                parts: Vec::new(),
                finished: true,
            };
        }
        // Reaching here means Drop didn't panic.
    }

    #[tokio::test]
    async fn test_multipart_upload_drop_without_finish_does_not_panic() {
        // With `finished=false` and a tokio runtime, Drop should log + spawn
        // a best-effort abort. The abort itself will fail (no real S3) but
        // must not panic the dropping thread.
        let storage = make_test_storage(None);
        {
            let _up = MultipartUpload {
                storage: &storage,
                key: "k".into(),
                upload_id: "u".into(),
                parts: Vec::new(),
                finished: false,
            };
        }
        // Give the spawned best-effort abort a chance to run + fail quietly.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

// ============================================================================
// 公共细粒度 multipart 上传 API
// ============================================================================
//
// 上层应用（HSM copytool / 自定义同步流水线）需要 part-level 控制：
//   - 自己决定何时切 part（边界对齐文件 chunk、cancel 触发等）
//   - 每 part 完成后单独上报进度
//   - 失败时只重传该 part 而不是整个对象
//   - cancel 时 abort 而不是 complete
//
// 现存的 `S3Storage::write_multipart_data` 一次性吞整流，进度回调粗，
// 也无法在 part 失败时单独重试。这套 wrapper 把已有的内部 helper
// （`create_multipart_upload` / `upload_part_with_stream` /
// `complete_multipart_upload` / `abort_multipart_upload`）暴露成一个
// 类型安全 + RAII 的 high-level 接口。

/// 重新导出 aws-sdk 的 `CompletedPart`，便于直接调用 low-level
/// `complete_multipart_upload(key, upload_id, parts)` 的用户构造 parts。
pub use aws_sdk_s3::types::CompletedPart as S3CompletedPart;

/// 一次进行中的 S3 multipart upload。
///
/// 通过 [`S3Storage::multipart_begin`] 创建，必须以 [`MultipartUpload::complete`]
/// 或 [`MultipartUpload::abort`] 之一收尾。
///
/// # Drop 行为
///
/// 如果未显式收尾即被 drop（例如 panic、提前 `?` early-return）：
/// - 记录 `error!` 日志（带 key + upload_id），便于事后排查
/// - 若当前线程持有 tokio runtime handle，会 `spawn` 一次 best-effort
///   abort 请求（不 await，仅减少 S3 上的"僵尸"未完成上传——这在 AWS
///   上会按 storage 计费）
/// - **无 runtime 时不会自动清理**，调用方应当用 [`abort`](Self::abort)
///   显式收尾
///
/// # 示例
///
/// ```ignore
/// use bytes::Bytes;
/// use storage_v2::S3Storage;
///
/// let mut up = s3.multipart_begin("path/to/large.bin", None).await?;
/// for (i, chunk) in chunks.into_iter().enumerate() {
///     up.upload_part(i as i32 + 1, chunk).await?;        // 1-based
///     // 上层在这里上报进度 / 检查 cancel token / ...
/// }
/// up.complete().await?;
/// ```
pub struct MultipartUpload<'s> {
    storage: &'s S3Storage,
    key: String,
    upload_id: String,
    parts: Vec<CompletedPart>,
    /// `true` after `complete` / `abort` finished (success or fail).
    /// Drop guard reads this to decide whether to fire the failsafe abort.
    finished: bool,
}

impl<'s> MultipartUpload<'s> {
    /// 已分配的 upload_id（S3 端的句柄）。
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }

    /// 对象的 key。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 已成功上传的 part 数量。
    pub fn part_count(&self) -> usize {
        self.parts.len()
    }

    /// 上传一个 part。
    ///
    /// - `part_number`: 1-based，必须严格递增（S3 要求）。本方法不强制校验，
    ///   由调用方保证；乱序在 [`complete`](Self::complete) 时会按 part_number
    ///   重新排序。
    /// - `data`: part 内容。S3 规则：除最后一段外，每 part ≥ 5 MiB；上限 5 GiB。
    ///   本方法不做 ≥ 5 MiB 检查（在某些场景如最后一段更小是合法的）。
    pub async fn upload_part(&mut self, part_number: i32, data: Bytes) -> Result<()> {
        let size = data.len() as u64;
        let etag = self
            .storage
            .upload_part_with_stream(&self.key, &self.upload_id, part_number, vec![data], size)
            .await?;
        self.parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(etag)
                .build(),
        );
        Ok(())
    }

    /// 完成 multipart upload，对象在 S3 上变为可见。
    ///
    /// 在调用前会按 part_number 升序排序，避免乱序上传导致 S3 拒绝。
    /// 调用成功或失败后 RAII 守卫不再触发。
    pub async fn complete(mut self) -> Result<()> {
        self.parts.sort_by_key(|p| p.part_number().unwrap_or(0));
        let res = self
            .storage
            .complete_multipart_upload(&self.key, &self.upload_id, &self.parts)
            .await
            .map(|_| ());
        self.finished = true;
        res
    }

    /// 主动放弃这次上传（cancel / 错误恢复时调用）。
    ///
    /// 即便本调用失败，也会标记 `finished` 防止 Drop 重复 abort——失败路径
    /// 留给调用方记录。
    pub async fn abort(mut self) -> Result<()> {
        let res = self
            .storage
            .abort_multipart_upload(&self.key, &self.upload_id)
            .await;
        self.finished = true;
        res
    }
}

impl Drop for MultipartUpload<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        error!(
            "MultipartUpload dropped without complete/abort: key={}, upload_id={}, \
             {} parts uploaded — leaking S3 multipart resources (you will be billed)",
            self.key,
            self.upload_id,
            self.parts.len(),
        );

        // Best-effort failsafe: if there's a tokio runtime handle, spawn an abort.
        // Cannot await in Drop, so we clone the necessary state and fire-and-forget.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let storage = self.storage.clone();
            let key = std::mem::take(&mut self.key);
            let upload_id = std::mem::take(&mut self.upload_id);
            handle.spawn(async move {
                if let Err(e) = storage.abort_multipart_upload(&key, &upload_id).await {
                    error!("best-effort multipart abort failed: key={key}, upload_id={upload_id}, err={e}");
                }
            });
        }
    }
}

impl S3Storage {
    /// 开始一次 multipart upload，返回 RAII 句柄。
    ///
    /// 详见 [`MultipartUpload`] 的文档。
    pub async fn multipart_begin(
        &self,
        key: &str,
        tags: Option<&Vec<Tag>>,
    ) -> Result<MultipartUpload<'_>> {
        let upload_id = self.create_multipart_upload(key, tags).await?;
        Ok(MultipartUpload {
            storage: self,
            key: key.to_string(),
            upload_id,
            parts: Vec::new(),
            finished: false,
        })
    }
}
