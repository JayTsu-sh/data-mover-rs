use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use md5::{Digest, Md5};

/// Add the legacy RFC 1864 checksum required by selected compatible endpoints.
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
