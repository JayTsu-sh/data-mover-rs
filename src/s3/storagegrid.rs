use aws_sdk_s3::config::Builder;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;

/// Register the `StorageGRID` compatibility interceptor.
pub(super) fn configure(builder: Builder) -> Builder {
    builder.interceptor(StripXIdInterceptor)
}

pub(super) fn compatibility_enabled() -> bool {
    enabled_from_value(std::env::var("AWS_S3_STORAGEGRID_COMPAT").ok().as_deref())
}

/// Remove the exact `x-id` query parameter from an encoded request URI.
///
/// The common no-match path performs no allocation. A matching URI is scanned
/// once to detect the edit and rebuilt into one preallocated `String`; encoded
/// path and query bytes are copied verbatim.
fn strip_x_id_from_uri(uri: &str) -> Option<String> {
    let query_start = uri.find('?')?;
    let query = &uri[query_start + 1..];
    let is_x_id = |pair: &str| pair == "x-id" || pair.starts_with("x-id=");

    if !query.split('&').any(is_x_id) {
        return None;
    }

    let mut stripped = String::with_capacity(uri.len());
    stripped.push_str(&uri[..query_start]);

    let mut separator = '?';
    for pair in query.split('&') {
        if is_x_id(pair) {
            continue;
        }
        stripped.push(separator);
        stripped.push_str(pair);
        separator = '&';
    }

    Some(stripped)
}

#[derive(Debug)]
struct StripXIdInterceptor;

impl Intercept for StripXIdInterceptor {
    fn name(&self) -> &'static str {
        "StripXIdInterceptor"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request_mut();
        if let Some(uri) = strip_x_id_from_uri(request.uri()) {
            request.set_uri(uri)?;
        }
        Ok(())
    }
}

fn enabled_from_value(value: Option<&str>) -> bool {
    value.is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_path_returns_none() {
        assert_eq!(
            strip_x_id_from_uri("https://s3.example/bucket/key?versionId=x-id=value"),
            None
        );
        assert_eq!(
            strip_x_id_from_uri("https://s3.example/bucket/key?x-id-extra=GetObject"),
            None
        );
        assert_eq!(strip_x_id_from_uri("https://s3.example/bucket/key"), None);
    }

    #[test]
    fn preserves_encoded_uri_and_other_parameters() {
        assert_eq!(
            strip_x_id_from_uri(
                "https://s3.example/a%20b/file%2Bname?partNumber=1&x-id=GetObject&versionId=abc"
            ),
            Some("https://s3.example/a%20b/file%2Bname?partNumber=1&versionId=abc".to_string())
        );
        assert_eq!(
            strip_x_id_from_uri("https://s3.example/bucket/key?x-id=GetObject"),
            Some("https://s3.example/bucket/key".to_string())
        );
    }

    #[test]
    fn compatibility_is_opt_in() {
        assert!(!enabled_from_value(None));
        assert!(enabled_from_value(Some("1")));
        assert!(enabled_from_value(Some("TRUE")));
        assert!(!enabled_from_value(Some("0")));
        assert!(!enabled_from_value(Some("false")));
    }
}
