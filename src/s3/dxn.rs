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
