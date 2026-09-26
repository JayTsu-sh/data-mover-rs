//! The one reader of an S3 URL's credentials, scheme and options, shared by every path that takes
//! an S3 URL: the legacy `StorageEnum` (`S3Storage::new`), the role-based
//! `storage::connect_backend`, `storage::endpoint_identity` (before connecting) and the bucket-less
//! endpoint URL of `S3Storage::list_buckets`.
//!
//! ```text
//! s3://AK:SK@bucket.host[:port][/prefix][?options]         HTTP
//! s3+https://AK:SK@bucket.host[:port][/prefix][?options]   HTTPS
//! options: compat=sg|dxn|hcp|ontap  tls_verify=false  bucket_type=nas
//! ```
//!
//! Leaving an option out gives its default (standard S3, certificates verified, object bucket);
//! naming a default, an unknown or repeated option, an empty value, `tls_verify` on `s3://` and
//! `bucket_type=nas` without `compat=ontap` are `UrlParseError`s — everything the URL text alone
//! decides. No option is part of the endpoint identity.
//!
//! Errors never contain the URL: an unknown option or value is echoed only when it is short and
//! plainly not a secret, otherwise by its length.

use url::form_urlencoded;

use crate::Result;
use crate::error::StorageError;

/// The request adjustments of one S3 service family (`compat=`). Only the adjustments differ:
/// every profile is the same S3 backend.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum S3Compatibility {
    #[default]
    Standard,
    StorageGrid,
    Dxn,
    /// Hitachi Content Platform: object tags are read through its REST annotation.
    Hcp,
    /// `NetApp` ONTAP S3. No request differs from standard S3: every ONTAP quirk of an object bucket
    /// is handled generically. It owns `bucket_type=nas` and its own native-copy affinity.
    Ontap,
}

impl S3Compatibility {
    const VALUES: [(&'static str, Self); 4] = [
        ("sg", Self::StorageGrid),
        ("dxn", Self::Dxn),
        ("hcp", Self::Hcp),
        ("ontap", Self::Ontap),
    ];
}

/// What kind of bucket the URL names (`bucket_type=`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum S3BucketType {
    #[default]
    Object,
    /// An ONTAP NAS (multiprotocol) bucket.
    Nas,
}

impl S3BucketType {
    const VALUES: [(&'static str, Self); 1] = [("nas", Self::Nas)];

    /// The one `compat` this bucket type exists under, if it is bound to one.
    const fn required_compat(self) -> Option<(S3Compatibility, &'static str)> {
        match self {
            Self::Object => None,
            Self::Nas => Some((S3Compatibility::Ontap, "ontap")),
        }
    }
}

/// Everything an S3 URL states besides its credentials and location. The default is `s3://`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct S3UrlOptions {
    /// `s3+https://` (HTTPS) rather than `s3://` (HTTP).
    pub(crate) https: bool,
    pub(crate) compat: S3Compatibility,
    /// Verify the server's certificate (HTTPS only).
    pub(crate) tls_verify: bool,
    pub(crate) bucket_type: S3BucketType,
}

impl Default for S3UrlOptions {
    fn default() -> Self {
        Self {
            https: false,
            compat: S3Compatibility::Standard,
            tls_verify: true,
            bucket_type: S3BucketType::Object,
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

/// Whether the URL names a bucket or only an endpoint (`list_buckets`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum S3UrlTarget {
    Bucket,
    Endpoint,
}

/// An S3 URL split into its credentials, its location and its options.
pub(crate) struct S3UrlParts<'a> {
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    /// `bucket.host[:port][/prefix]`, or `host[:port]` for an endpoint URL; without the query.
    pub(crate) location: &'a str,
    pub(crate) options: S3UrlOptions,
}

/// The schemes that were replaced by options, with their new spelling. The old `+https` schemes
/// accepted any certificate, hence `tls_verify=false` in their replacement.
const REMOVED_SCHEMES: [(&str, &str); 6] = [
    ("s3+http", "s3://…"),
    ("s3+sg", "s3://…?compat=sg"),
    ("s3+sg+https", "s3+https://…?compat=sg&tls_verify=false"),
    ("s3+dxn", "s3://…?compat=dxn"),
    ("s3+dxn+https", "s3+https://…?compat=dxn&tls_verify=false"),
    ("s3+hcp", "s3://…?compat=hcp"),
];

/// Splits `scheme://AK:SK@location[?options]` into its parts.
///
/// The credentials end at the **last** `@` (a secret key never contains `@`, but often `/`, `+`,
/// `=` or `?`); the access key ends at the first `:` of the credentials; the options start at the
/// first `?` after the credentials.
///
/// # Errors
/// `UrlParseError` for anything the URL text alone decides: a missing `://`, `@` or `:` in the
/// credentials, a scheme other than `s3` / `s3+https` (a removed scheme names its replacement), and
/// every option error listed in the module documentation.
pub(crate) fn split_s3_url(url: &str, target: S3UrlTarget) -> Result<S3UrlParts<'_>> {
    let scheme_end = url
        .find("://")
        .ok_or_else(|| StorageError::UrlParseError("URL中缺少 :// 分隔符".to_string()))?;
    let https = scheme_https(&url[..scheme_end])?;
    let after_scheme = &url[scheme_end + 3..];

    let at_pos = after_scheme.rfind('@').ok_or_else(|| {
        StorageError::UrlParseError("URL中缺少 @ 分隔符，无法提取凭据".to_string())
    })?;
    let userinfo = &after_scheme[..at_pos];
    let (location, query) = after_scheme[at_pos + 1..]
        .split_once('?')
        .unwrap_or((&after_scheme[at_pos + 1..], ""));

    let colon_pos = userinfo.find(':').ok_or_else(|| {
        StorageError::UrlParseError("URL凭据中缺少 : 分隔符，无法分割AK和SK".to_string())
    })?;
    Ok(S3UrlParts {
        access_key: userinfo[..colon_pos].to_string(),
        secret_key: userinfo[colon_pos + 1..].to_string(),
        location,
        options: parse_options(query, https, target)?,
    })
}

/// Whether the scheme is `s3+https` rather than `s3`.
fn scheme_https(scheme: &str) -> Result<bool> {
    let scheme = scheme.to_ascii_lowercase();
    match scheme.as_str() {
        "s3" => Ok(false),
        "s3+https" => Ok(true),
        _ => Err(invalid(
            REMOVED_SCHEMES
                .iter()
                .find(|(removed, _)| *removed == scheme)
                .map_or_else(
                    || "an S3 URL starts with s3:// (HTTP) or s3+https:// (HTTPS)".to_string(),
                    |(removed, replacement)| {
                        format!("the scheme {removed}:// was removed; write {replacement}")
                    },
                ),
        )),
    }
}

/// The options a query names, each at most once.
#[derive(Default)]
struct Given {
    compat: Option<S3Compatibility>,
    tls_verify: Option<bool>,
    bucket_type: Option<S3BucketType>,
}

/// The options of the query, in any order, checked against each other and the URL.
fn parse_options(query: &str, https: bool, target: S3UrlTarget) -> Result<S3UrlOptions> {
    let given = read_query(query)?;
    if given.tls_verify.is_some() && !https {
        return Err(invalid(
            "tls_verify applies to s3+https:// only".to_string(),
        ));
    }
    let compat = given.compat.unwrap_or_default();
    let bucket_type = given.bucket_type.unwrap_or_default();
    if bucket_type != S3BucketType::Object && target == S3UrlTarget::Endpoint {
        return Err(invalid(
            "bucket_type describes a bucket; an endpoint URL without a bucket cannot carry it"
                .to_string(),
        ));
    }
    if let Some((required, name)) = bucket_type.required_compat()
        && compat != required
    {
        return Err(invalid(format!("bucket_type=nas requires compat={name}")));
    }
    Ok(S3UrlOptions {
        https,
        compat,
        // Every HTTPS scheme accepts any server certificate (self-signed / private-CA deployments).
        tls_verify: given.tls_verify.unwrap_or(!https),
        bucket_type,
    })
}

/// Every option of the query, each known, valued and given once.
fn read_query(query: &str) -> Result<Given> {
    let mut given = Given::default();
    for (name, value) in form_urlencoded::parse(query.as_bytes()) {
        let name = name.as_ref();
        if !matches!(name, "compat" | "tls_verify" | "bucket_type") {
            return Err(invalid(format!(
                "unknown S3 URL option {}; the options are compat, tls_verify and bucket_type",
                echo_name(name)
            )));
        }
        if value.is_empty() {
            return Err(invalid(format!("the S3 URL option {name} has no value")));
        }
        match name {
            "compat" => set(
                &mut given.compat,
                name,
                value_of(name, &value, S3Compatibility::VALUES, "standard")?,
            )?,
            "tls_verify" => set(
                &mut given.tls_verify,
                name,
                value_of(name, &value, [("false", false)], "true")?,
            )?,
            _ => set(
                &mut given.bucket_type,
                name,
                value_of(name, &value, S3BucketType::VALUES, "object")?,
            )?,
        }
    }
    Ok(given)
}

/// The value `text` names among `values`; the default's own spelling is refused, since leaving
/// the option out already means it.
fn value_of<T: Copy, const N: usize>(
    name: &str,
    text: &str,
    values: [(&'static str, T); N],
    default: &str,
) -> Result<T> {
    if text == default {
        return Err(invalid(format!(
            "{name}={default} is the default: omit the option"
        )));
    }
    values
        .iter()
        .find(|(spelling, _)| *spelling == text)
        .map(|(_, value)| *value)
        .ok_or_else(|| {
            let allowed: Vec<&str> = values.iter().map(|(spelling, _)| *spelling).collect();
            invalid(format!(
                "the S3 URL option {name} does not accept {}; allowed: {}",
                echo_value(text),
                allowed.join(", ")
            ))
        })
}

/// Stores the first occurrence of an option; a second one is refused, even with the same value.
fn set<T>(slot: &mut Option<T>, name: &str, value: T) -> Result<()> {
    if slot.replace(value).is_some() {
        return Err(invalid(format!("the S3 URL option {name} is given twice")));
    }
    Ok(())
}

/// An option name as it may appear in an error: short lower-case identifiers only (an access key
/// id is upper case).
fn echo_name(name: &str) -> String {
    echo(name, 32, |byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
    })
}

/// An option value as it may appear in an error: short lower-case words only.
fn echo_value(value: &str) -> String {
    echo(value, 16, |byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
    })
}

/// `text` quoted when it is at most `max` bytes that all pass `plain`, otherwise only its length:
/// a mistyped URL can put anything there, including a secret.
fn echo(text: &str, max: usize, plain: fn(u8) -> bool) -> String {
    if !text.is_empty() && text.len() <= max && text.bytes().all(plain) {
        format!("`{text}`")
    } else {
        format!("<{} bytes>", text.len())
    }
}

fn invalid(message: String) -> StorageError {
    StorageError::UrlParseError(message)
}
