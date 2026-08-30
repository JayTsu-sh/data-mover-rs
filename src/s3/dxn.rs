use aws_sdk_s3::config::Builder;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;

/// Register only the compatibility required by DXN endpoints.
pub(super) fn configure(builder: Builder) -> Builder {
    builder.interceptor(DxnCompatibilityInterceptor)
}

#[derive(Debug)]
struct DxnCompatibilityInterceptor;

impl Intercept for DxnCompatibilityInterceptor {
    fn name(&self) -> &'static str {
        "DxnCompatibilityInterceptor"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        super::delete_objects_md5::add_content_md5(context.request_mut())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::AssertTestValue;

    use super::super::S3Storage;

    const SIZE: usize = 20 * 1024 * 1024;
    const SOURCE: &str = "rename limit source %?# 中文.bin";
    const DESTINATION: &str = "rename limit destination %?# 中文.bin";

    async fn seed(storage: &S3Storage) {
        storage
            .client
            .put_object()
            .bucket(&storage.bucket_name)
            .key(storage.build_full_key(SOURCE))
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0x2d; SIZE]))
            .send()
            .await
            .assert_value("seed DXN multipart rename source");
    }

    async fn assert_source_only(storage: &S3Storage) {
        let source = storage.build_full_key(SOURCE);
        storage
            .client
            .head_object()
            .bucket(&storage.bucket_name)
            .key(&source)
            .send()
            .await
            .assert_value("DXN rename failure preserves source");
        assert!(
            storage
                .client
                .head_object()
                .bucket(&storage.bucket_name)
                .key(storage.build_full_key(DESTINATION))
                .send()
                .await
                .is_err()
        );
        storage
            .client
            .delete_object()
            .bucket(&storage.bucket_name)
            .key(source)
            .send()
            .await
            .assert_value("clean DXN rename source");
    }

    #[tokio::test]
    async fn multipart_rename_limit_preserves_source_when_lab_is_configured() {
        let Ok(url) = std::env::var("LAB_DXN_S3_ARCHITECTURE_URL") else {
            eprintln!("skip: LAB_DXN_S3_ARCHITECTURE_URL is not configured");
            return;
        };
        let storage = S3Storage::new(&url, None)
            .await
            .assert_value("connect to DXN rename test storage");
        seed(&storage).await;
        assert!(
            storage
                .rename_with_limits(
                    Path::new(SOURCE),
                    Path::new(DESTINATION),
                    Some(SIZE as u64),
                    8 * 1024 * 1024,
                    5 * 1024 * 1024,
                )
                .await
                .is_err()
        );
        assert_source_only(&storage).await;
    }
}
