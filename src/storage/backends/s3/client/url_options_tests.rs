//! Every spelling of an S3 URL's scheme and options, accepted and refused (P7 §14.2).

use super::url_options::S3BucketType::{Nas, Object};
use super::url_options::S3Compatibility::{Dxn, Ontap, Standard, StorageGrid};
use super::url_options::{S3BucketType, S3Compatibility, S3UrlOptions, S3UrlTarget, split_s3_url};
use super::{ParsedS3Url, S3Storage, endpoint_identity, parse_s3_endpoint_url, parse_s3_url};
use crate::AssertTestValue;
use crate::error::StorageError;
use crate::storage::backends::s3::{S3BucketCapabilities, S3Protocol as _};

const AK: &str = "AKIDOPTIONS7Q";
const SK: &str = "sEcReT/opt+ions=Zq?x";
/// Secret-looking text a mistyped URL could put where an option belongs.
const SECRET_WORD: &str = "wjalrxutnfemik7mdengbpxrficyexamplekey";

fn url(scheme: &str, query: &str) -> String {
    format!("{scheme}://{AK}:{SK}@bucket.host.example:9000/prefix{query}")
}

fn options(scheme: &str, query: &str) -> S3UrlOptions {
    split_s3_url(&url(scheme, query), S3UrlTarget::Bucket)
        .assert_value(&format!("{scheme} {query} parses"))
        .options
}

/// The message of the `UrlParseError` the URL is refused with; it names neither credential.
fn refusal(url: &str, target: S3UrlTarget) -> String {
    let error = split_s3_url(url, target)
        .err()
        .assert_value(&format!("{url} is refused"));
    let debug = format!("{error:?}");
    let StorageError::UrlParseError(message) = error else {
        panic!("{url}: expected UrlParseError, got {debug}");
    };
    for secret in [AK, SK] {
        assert!(
            !message.contains(secret) && !debug.contains(secret),
            "{url}: {debug}"
        );
    }
    message
}

const fn with(https: bool, compat: S3Compatibility, bucket_type: S3BucketType) -> S3UrlOptions {
    S3UrlOptions {
        https,
        compat,
        tls_verify: true,
        bucket_type,
    }
}

#[test]
fn accepted_spellings() {
    let cases = [
        ("s3", "", with(false, Standard, Object)),
        ("S3", "", with(false, Standard, Object)),
        ("s3+https", "", with(true, Standard, Object)),
        ("s3", "?compat=sg", with(false, StorageGrid, Object)),
        ("s3", "?compat=dxn", with(false, Dxn, Object)),
        ("s3", "?compat=ontap", with(false, Ontap, Object)),
        ("s3", "?compat=%73g", with(false, StorageGrid, Object)),
        (
            "s3",
            "?compat=ontap&bucket_type=nas",
            with(false, Ontap, Nas),
        ),
        (
            "s3",
            "?bucket_type=nas&compat=ontap",
            with(false, Ontap, Nas),
        ),
        ("s3", "?", with(false, Standard, Object)),
        ("s3", "?compat=dxn&", with(false, Dxn, Object)),
        ("s3+https", "?compat=sg", with(true, StorageGrid, Object)),
        (
            "s3+https",
            "?tls_verify=false&compat=ontap&bucket_type=nas",
            S3UrlOptions {
                tls_verify: false,
                ..with(true, Ontap, Nas)
            },
        ),
    ];
    for (scheme, query, expected) in cases {
        assert_eq!(options(scheme, query), expected, "{scheme} {query}");
    }
    assert!(!options("s3+https", "").skips_tls_verification());
    let explicit = options("s3+https", "?compat=dxn&tls_verify=false");
    assert!(explicit.skips_tls_verification() && !explicit.tls_verify);
}

#[test]
fn removed_schemes_name_their_replacement() {
    for (scheme, replacement) in [
        ("s3+http", "write s3://…"),
        ("s3+sg", "write s3://…?compat=sg"),
        ("S3+SG", "write s3://…?compat=sg"),
        (
            "s3+sg+https",
            "write s3+https://…?compat=sg&tls_verify=false",
        ),
        ("s3+dxn", "write s3://…?compat=dxn"),
        (
            "s3+dxn+https",
            "write s3+https://…?compat=dxn&tls_verify=false",
        ),
        ("s3+hcp", "write s3://… (HCP support was removed"),
    ] {
        let message = refusal(&url(scheme, ""), S3UrlTarget::Bucket);
        assert!(
            message.contains(&format!(
                "the scheme {}:// was removed",
                scheme.to_ascii_lowercase()
            )) && message.contains(replacement),
            "{scheme}: {message}"
        );
    }
    for scheme in ["http", "https", "s3+foo", "s3+https+dxn"] {
        let message = refusal(&url(scheme, ""), S3UrlTarget::Bucket);
        assert!(
            message.contains("starts with s3:// (HTTP) or s3+https:// (HTTPS)"),
            "{scheme}: {message}"
        );
    }
}

/// (scheme, query, what the refusal says).
const REFUSED: &[(&str, &str, &str)] = &[
    (
        "s3",
        "?compat=standard",
        "compat=standard is the default: omit the option",
    ),
    (
        "s3",
        "?bucket_type=object",
        "bucket_type=object is the default: omit the option",
    ),
    (
        "s3+https",
        "?tls_verify=true",
        "tls_verify=true is the default: omit the option",
    ),
    (
        "s3",
        "?bucket-type=nas",
        "unknown S3 URL option `bucket-type`",
    ),
    ("s3", "?Compat=sg", "unknown S3 URL option <6 bytes>"),
    ("s3", "?=sg", "unknown S3 URL option <0 bytes>"),
    (
        "s3",
        "?compat=sg&compat=sg",
        "the S3 URL option compat is given twice",
    ),
    (
        "s3",
        "?compat=sg&compat=dxn",
        "the S3 URL option compat is given twice",
    ),
    ("s3", "?compat=", "the S3 URL option compat has no value"),
    (
        "s3",
        "?tls_verify",
        "the S3 URL option tls_verify has no value",
    ),
    (
        "s3",
        "?compat=SG",
        "compat does not accept <2 bytes>; allowed: sg, dxn, ontap",
    ),
    (
        "s3",
        "?compat=hcp",
        "compat=hcp was removed; write s3://… (HCP support was removed",
    ),
    (
        "s3",
        "?compat=minio",
        "compat does not accept `minio`; allowed: sg, dxn, ontap",
    ),
    (
        "s3+https",
        "?tls_verify=no",
        "tls_verify does not accept `no`; allowed: false",
    ),
    (
        "s3",
        "?bucket_type=directory",
        "bucket_type does not accept `directory`; allowed: nas",
    ),
    (
        "s3",
        "?tls_verify=false",
        "tls_verify applies to s3+https:// only",
    ),
    (
        "s3",
        "?bucket_type=nas",
        "bucket_type=nas requires compat=ontap",
    ),
    (
        "s3",
        "?compat=sg&bucket_type=nas",
        "bucket_type=nas requires compat=ontap",
    ),
];

#[test]
fn refused_options() {
    for &(scheme, query, expected) in REFUSED {
        let message = refusal(&url(scheme, query), S3UrlTarget::Bucket);
        assert!(message.contains(expected), "{scheme} {query}: {message}");
    }
}

/// A mistyped URL can put a secret where an option name or value belongs; only its length is
/// echoed.
#[test]
fn refusals_echo_only_short_plain_words() {
    let long_name = "a".repeat(33);
    let long_value = "b".repeat(17);
    for (query, expected) in [
        (
            format!("?{SECRET_WORD}=1"),
            format!("<{} bytes>", SECRET_WORD.len()),
        ),
        (
            format!("?compat={SECRET_WORD}"),
            format!("<{} bytes>", SECRET_WORD.len()),
        ),
        (format!("?{long_name}=1"), "<33 bytes>".to_string()),
        (format!("?compat={long_value}"), "<17 bytes>".to_string()),
    ] {
        let message = refusal(&url("s3", &query), S3UrlTarget::Bucket);
        assert!(
            message.contains(&expected) && !message.contains(SECRET_WORD),
            "{query}: {message}"
        );
    }
    let message = refusal(
        &url("s3", &format!("?{}=1", "a".repeat(32))),
        S3UrlTarget::Bucket,
    );
    assert!(
        message.contains(&format!("`{}`", "a".repeat(32))),
        "{message}"
    );
}

#[test]
fn an_endpoint_url_takes_every_option_but_the_bucket_type() {
    let endpoint = |query: &str| format!("s3+https://{AK}:{SK}@host.example:9000{query}");
    let ontap = endpoint("?compat=ontap&tls_verify=false");
    let parts = split_s3_url(&ontap, S3UrlTarget::Endpoint).assert_value("endpoint URL parses");
    assert_eq!(parts.location, "host.example:9000");
    assert_eq!(parts.options.compat, Ontap);
    let message = refusal(
        &endpoint("?compat=ontap&bucket_type=nas"),
        S3UrlTarget::Endpoint,
    );
    assert!(
        message.contains("an endpoint URL without a bucket"),
        "{message}"
    );

    let (access_key, secret_key, http_endpoint, options) =
        parse_s3_endpoint_url(&endpoint("/?compat=dxn")).assert_value("list_buckets URL parses");
    assert_eq!((access_key.as_str(), secret_key.as_str()), (AK, SK));
    assert_eq!(http_endpoint, "https://host.example:9000");
    assert_eq!(options.compat, Dxn);
}

/// The query is neither part of the prefix nor of the endpoint, and credentials are split as
/// before: a secret key may contain `?`.
#[test]
fn the_query_leaves_location_and_credentials_alone() {
    let ParsedS3Url {
        access_key,
        secret_key,
        bucket_name,
        endpoint,
        prefix,
        options,
        ..
    } = parse_s3_url(&url("s3+https", "?compat=ontap&bucket_type=nas")).assert_value("URL parses");
    assert_eq!((access_key.as_str(), secret_key.as_str()), (AK, SK));
    assert_eq!(bucket_name, "bucket");
    assert_eq!(endpoint, "https://host.example:9000");
    assert_eq!(prefix, "prefix/");
    assert_eq!(options.bucket_type, Nas);

    let ParsedS3Url { prefix, .. } = parse_s3_url(&format!(
        "s3://{AK}:{SK}@bucket.host.example:9000?compat=dxn"
    ))
    .assert_value("URL without a prefix parses");
    assert_eq!(prefix, "");
}

/// Options are not part of the endpoint identity: a transfer's identity and recovery binding do
/// not change when an option does.
#[test]
fn options_are_not_part_of_the_endpoint_identity() {
    let plain = endpoint_identity(&url("s3", "")).assert_value("identity");
    for other in [
        url("s3", "?compat=dxn"),
        url("s3", "?compat=ontap&bucket_type=nas"),
        url("s3+https", ""),
        url("s3+https", "?compat=sg&tls_verify=false"),
    ] {
        assert_eq!(
            endpoint_identity(&other)
                .assert_value("identity")
                .stable_id(),
            plain.stable_id(),
            "{other}"
        );
    }
    assert!(matches!(
        endpoint_identity(&url("s3+sg", "")),
        Err(StorageError::UrlParseError(_))
    ));
}

/// `bucket_type=nas` connects (P7 N1) and gives the roles an ONTAP NAS bucket's switches; the same
/// endpoint under `compat=ontap` alone is an object bucket. Nothing listens on port 1: the
/// connection's versioning probe fails and is only logged.
#[tokio::test]
async fn a_nas_url_connects_with_the_nas_bucket_capabilities() {
    let nas = format!("s3://{AK}:{SK}@bucket.127.0.0.1:1/p?compat=ontap&bucket_type=nas");
    let storage = S3Storage::new(&nas, None)
        .await
        .assert_value("a NAS URL connects");
    assert_eq!(storage.bucket_capabilities(), S3BucketCapabilities::NAS);
    let object = format!("s3://{AK}:{SK}@bucket.127.0.0.1:1/p?compat=ontap");
    let storage = S3Storage::new(&object, None)
        .await
        .assert_value("an object URL connects");
    assert_eq!(storage.bucket_capabilities(), S3BucketCapabilities::OBJECT);
}

/// The last `@` ends the credentials; a URL without a host is a URL error; `tls_verify` needs
/// `s3+https://` on an endpoint URL too; `+` decodes to a space, which no value contains.
#[test]
fn edge_spellings() {
    let parts = split_s3_url("s3://AK:S@K@bucket.host/p", S3UrlTarget::Bucket)
        .assert_value("secret key with @");
    assert_eq!(
        (parts.access_key.as_str(), parts.secret_key.as_str()),
        ("AK", "S@K")
    );
    assert!(matches!(
        parse_s3_url("s3://AK:SK@/p"),
        Err(StorageError::UrlParseError(_))
    ));
    let message = refusal(
        &format!("s3://{AK}:{SK}@host.example:9000?tls_verify=false"),
        S3UrlTarget::Endpoint,
    );
    assert!(
        message.contains("tls_verify applies to s3+https:// only"),
        "{message}"
    );
    let message = refusal(&url("s3", "?compat=s+g"), S3UrlTarget::Bucket);
    assert!(message.contains("does not accept <3 bytes>"), "{message}");
    let message = refusal(&url("s3", "?compat=sg#x"), S3UrlTarget::Bucket);
    assert!(message.contains("does not accept <4 bytes>"), "{message}");
}
