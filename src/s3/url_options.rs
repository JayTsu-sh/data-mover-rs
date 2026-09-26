//! The one reader of an S3 URL's credentials, scheme and options, shared by every path that takes
//! an S3 URL: the legacy `StorageEnum` (`S3Storage::new`), the role-based
//! `storage::connect_backend`, `storage::endpoint_identity` (before connecting) and the bucket-less
//! endpoint URL of `S3Storage::list_buckets`.

use crate::Result;
use crate::error::StorageError;

/// The request adjustments and service quirks of one S3 service family; every profile is the same
/// S3 backend.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum S3Compatibility {
    #[default]
    Standard,
    StorageGrid,
    Dxn,
    /// Hitachi Content Platform: object tags are read through its REST annotation.
    Hcp,
}

/// Everything an S3 URL states besides its credentials and location. The default is `s3://`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct S3UrlOptions {
    /// `s3+https://` (HTTPS) rather than `s3://` (HTTP).
    pub(crate) https: bool,
    pub(crate) compat: S3Compatibility,
    /// Verify the server's certificate (HTTPS only).
    pub(crate) tls_verify: bool,
}

impl Default for S3UrlOptions {
    fn default() -> Self {
        Self {
            https: false,
            compat: S3Compatibility::Standard,
            tls_verify: true,
        }
    }
}

impl S3UrlOptions {
    /// `http` or `https`.
    pub(crate) const fn http_scheme(self) -> &'static str {
        if self.https { "https" } else { "http" }
    }

    /// An HTTPS connection that accepts any server certificate.
    pub(crate) const fn skips_tls_verification(self) -> bool {
        self.https && !self.tls_verify
    }
}

/// An S3 URL split into its credentials, its location and its options.
pub(crate) struct S3UrlParts<'a> {
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    /// `bucket.host[:port][/prefix]`, or `host[:port]` for an endpoint URL.
    pub(crate) location: &'a str,
    pub(crate) options: S3UrlOptions,
}

/// Splits `scheme://AK:SK@location` into its parts.
///
/// The credentials end at the **last** `@` (a secret key never contains `@`, but often `/`, `+` or
/// `=`); the access key ends at the first `:` of the credentials.
///
/// # Errors
/// `UrlParseError` when the URL has no `://`, no `@` or no `:` in its credentials; `InvalidPath`
/// when the scheme is not an S3 scheme.
pub(crate) fn split_s3_url(url: &str) -> Result<S3UrlParts<'_>> {
    let scheme_end = url
        .find("://")
        .ok_or_else(|| StorageError::UrlParseError("URL中缺少 :// 分隔符".to_string()))?;
    let scheme = &url[..scheme_end];
    let after_scheme = &url[scheme_end + 3..];

    let at_pos = after_scheme.rfind('@').ok_or_else(|| {
        StorageError::UrlParseError("URL中缺少 @ 分隔符，无法提取凭据".to_string())
    })?;
    let userinfo = &after_scheme[..at_pos];
    let location = &after_scheme[at_pos + 1..];

    let colon_pos = userinfo.find(':').ok_or_else(|| {
        StorageError::UrlParseError("URL凭据中缺少 : 分隔符，无法分割AK和SK".to_string())
    })?;
    Ok(S3UrlParts {
        access_key: userinfo[..colon_pos].to_string(),
        secret_key: userinfo[colon_pos + 1..].to_string(),
        location,
        options: scheme_options(scheme)?,
    })
}

/// The options a scheme names.
fn scheme_options(scheme: &str) -> Result<S3UrlOptions> {
    let (https, compat) = match scheme.to_ascii_lowercase().as_str() {
        "s3" | "s3+http" => (false, S3Compatibility::Standard),
        "s3+https" => (true, S3Compatibility::Standard),
        "s3+sg" => (false, S3Compatibility::StorageGrid),
        "s3+sg+https" => (true, S3Compatibility::StorageGrid),
        "s3+dxn" => (false, S3Compatibility::Dxn),
        "s3+dxn+https" => (true, S3Compatibility::Dxn),
        "s3+hcp" => (false, S3Compatibility::Hcp),
        _ => {
            return Err(StorageError::InvalidPath(
                "无效的S3 URL格式,协议必须是s3://、s3+http://、s3+https://、s3+sg://、s3+sg+https://、s3+dxn://、s3+dxn+https://或s3+hcp://"
                    .to_string(),
            ));
        }
    };
    Ok(S3UrlOptions {
        https,
        compat,
        // Every HTTPS scheme accepts any server certificate (self-signed / private-CA deployments).
        tls_verify: !https,
    })
}
