use super::s3_options::{S3UrlOptions, S3UrlTarget, split_s3_url};
use crate::{Result, error::StorageError};
use percent_encoding::percent_decode_str;
use url::Url;
///
/// # Errors
/// Returns an error for a malformed S3 URL, credentials, endpoint, or URL options.
pub fn parse_s3_endpoint_url(url: &str) -> Result<(String, String, String, S3UrlOptions)> {
    let parts = split_s3_url(url, S3UrlTarget::Endpoint)?;
    let endpoint = format!(
        "{}://{}",
        parts.options.http_scheme(),
        parts.location.trim_end_matches('/')
    );
    Ok((parts.access_key, parts.secret_key, endpoint, parts.options))
}

// 使用 url 库解析 S3 URL（标准 S3、StorageGRID、DXN、ONTAP）。
// 注意：secret_key 可能包含 `+`、`/` 等特殊字符（Base64 编码），直接传给 Url::parse 会导致解析错误，
// 因此先手动提取凭据，再用占位凭据构建安全 URL 交给 url crate 解析 host/port/path。
pub struct ParsedS3Url {
    pub access_key: String,
    pub secret_key: String,
    pub bucket_name: String,
    pub endpoint: String,
    pub prefix: String,
    pub options: S3UrlOptions,
}

/// The canonical endpoint identity of an S3 URL without connecting (ADR-0006): parsed exactly as
/// the S3 connector parses it, so it equals the connected storage's identity.
///
/// # Errors
/// Returns an error if the S3 URL or its canonical identity is invalid.
pub fn endpoint_identity(url: &str) -> Result<crate::model::BackendIdentity> {
    let parsed = parse_s3_url(url)?;
    crate::storage::endpoint::s3(
        &parsed.endpoint,
        &parsed.bucket_name,
        (!parsed.prefix.is_empty()).then_some(parsed.prefix.as_str()),
    )
    .map_err(|error| StorageError::ConfigError(error.to_string()))
}

///
/// # Errors
/// Returns an error for malformed credentials, bucket, endpoint, prefix, or options.
pub fn parse_s3_url(url: &str) -> Result<ParsedS3Url> {
    let parts = split_s3_url(url, S3UrlTarget::Bucket)?;
    let options = parts.options;
    let http_scheme = options.http_scheme();

    // 用占位凭据构建安全 URL，让 url crate 解析 host/port/path
    let safe_url = format!("s3://dummy:dummy@{}", parts.location);
    let parsed_url =
        Url::parse(&safe_url).map_err(|e| StorageError::UrlParseError(e.to_string()))?;
    if parsed_url.fragment().is_some() {
        return Err(StorageError::UrlParseError(
            "S3 URL must not contain a fragment; encode a literal # as %23".to_string(),
        ));
    }

    // 获取主机名
    let host = parsed_url
        .host_str()
        .ok_or_else(|| StorageError::UrlParseError("URL中缺少主机名".to_string()))?;

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

    // 构建HTTP端点，根据URL的scheme决定使用http还是https
    let endpoint = format!("{http_scheme}://{host_and_port}");

    // Read the original path so URL dot-segment normalization cannot change an S3 key.
    // Both raw UTF-8 and percent-encoded URL spellings name the same prefix.
    let encoded_prefix = parts.location.split_once('/').map_or("", |(_, path)| path);
    let mut prefix = percent_decode_str(encoded_prefix)
        .decode_utf8()
        .map_err(|_| StorageError::UrlParseError("S3 prefix is not valid UTF-8".to_string()))?
        .into_owned();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }

    Ok(ParsedS3Url {
        access_key: parts.access_key,
        secret_key: parts.secret_key,
        bucket_name,
        endpoint,
        prefix,
        options,
    })
}
