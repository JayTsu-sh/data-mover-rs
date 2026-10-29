use aws_sdk_s3::config::Builder;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use md5::{Digest, Md5};

/// Register only the `DeleteObjects` Content-MD5, for the standard profile.
///
/// The SDK's own checksum for `DeleteObjects` is `x-amz-checksum-crc32`, which older S3
/// implementations ignore and then reject the request (`MissingContentMD5` on `MinIO`
/// `RELEASE.2023-03-20` and Ceph RGW Octopus); every S3 implementation this crate targets accepts
/// Content-MD5, and AWS S3 verifies both when both are sent.
pub(super) fn configure(builder: Builder) -> Builder {
    builder.interceptor(DeleteObjectsMd5Interceptor)
}

#[derive(Debug)]
struct DeleteObjectsMd5Interceptor;

impl Intercept for DeleteObjectsMd5Interceptor {
    fn name(&self) -> &'static str {
        "DeleteObjectsMd5Interceptor"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        add_content_md5(context.request_mut())
    }
}

/// Add the legacy RFC 1864 checksum to a `DeleteObjects` request; every profile does.
///
/// The SDK-generated XML body is buffered, so hashing borrows it without a
/// body copy. Interceptors call this before `SigV4` signing so the header is
/// covered by the request signature.
pub(super) fn add_content_md5(request: &mut HttpRequest) -> Result<(), BoxError> {
    let is_delete_objects = request.method() == "POST"
        && request
            .uri()
            .split_once('?')
            .is_some_and(|(_, query)| query.split('&').any(|pair| pair == "delete"));
    if !is_delete_objects || request.headers().contains_key("content-md5") {
        return Ok(());
    }

    let body = request.body().bytes().ok_or_else(|| {
        std::io::Error::other("DeleteObjects request body must be buffered for Content-MD5")
    })?;
    let content_md5 = BASE64_STANDARD.encode(Md5::digest(body));
    request
        .headers_mut()
        .try_insert("content-md5", content_md5)?;
    Ok(())
}
