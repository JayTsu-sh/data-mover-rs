//! Network failures surface once; only operations with safe replay retain service retries.
use aws_sdk_s3::config::{Builder, retry::RetryConfig};
use aws_sdk_s3::operation::{
    abort_multipart_upload::{self, AbortMultipartUploadInput},
    delete_object::{self, DeleteObjectInput},
    get_bucket_location::{self, GetBucketLocationInput},
    get_bucket_versioning::{self, GetBucketVersioningInput},
    get_object::{self, GetObjectInput},
    get_object_tagging::{self, GetObjectTaggingInput},
    head_bucket::{self, HeadBucketInput},
    head_object::{self, HeadObjectInput},
    list_buckets::{self, ListBucketsInput},
    list_multipart_uploads::{self, ListMultipartUploadsInput},
    list_object_versions::{self, ListObjectVersionsInput},
    list_objects_v2::{self, ListObjectsV2Input},
    list_parts::{self, ListPartsInput},
    upload_part::{self, UploadPartInput},
    upload_part_copy::{self, UploadPartCopyInput},
};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::{
    Intercept,
    context::{BeforeSerializationInterceptorContextRef, Error, Input, InterceptorContext},
};
use aws_smithy_runtime_api::client::retries::classifiers::{ClassifyRetry, RetryAction};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::{config_bag::ConfigBag, error::metadata::ProvideErrorMetadata};

pub(super) fn configure(builder: Builder) -> Builder {
    builder
        .retry_classifier(NetworkFailure)
        .interceptor(OperationReplay)
}

#[derive(Debug)]
struct NetworkFailure;
impl ClassifyRetry for NetworkFailure {
    fn classify_retry(&self, context: &InterceptorContext) -> RetryAction {
        let Some(Err(error)) = context.output_or_error() else {
            return RetryAction::NoActionIndicated;
        };
        if error.is_timeout_error()
            || error.is_connector_error()
            || error.is_response_error()
            || error.as_operation_error().is_some_and(service_timeout)
        {
            RetryAction::RetryForbidden
        } else {
            RetryAction::NoActionIndicated
        }
    }
    fn name(&self) -> &'static str {
        "S3NetworkFailure"
    }
}

#[derive(Debug)]
struct OperationReplay;
impl Intercept for OperationReplay {
    fn name(&self) -> &'static str {
        "S3OperationReplay"
    }
    fn read_before_serialization(
        &self,
        context: &BeforeSerializationInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        config: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // The SDK consumes the typed input during serialization. Store policy in
        // this operation's config, never in mutable shared interceptor state.
        if !safe_replay(context.input()) {
            config
                .interceptor_state()
                .store_put(RetryConfig::disabled());
        }
        Ok(())
    }
}

fn safe_replay(input: &Input) -> bool {
    input.downcast_ref::<GetBucketLocationInput>().is_some()
        || input.downcast_ref::<GetBucketVersioningInput>().is_some()
        || input.downcast_ref::<GetObjectInput>().is_some()
        || input.downcast_ref::<GetObjectTaggingInput>().is_some()
        || input.downcast_ref::<HeadBucketInput>().is_some()
        || input.downcast_ref::<HeadObjectInput>().is_some()
        || input.downcast_ref::<ListBucketsInput>().is_some()
        || input.downcast_ref::<ListMultipartUploadsInput>().is_some()
        || input.downcast_ref::<ListObjectsV2Input>().is_some()
        || input.downcast_ref::<ListObjectVersionsInput>().is_some()
        || input.downcast_ref::<ListPartsInput>().is_some()
        || input.downcast_ref::<UploadPartInput>().is_some()
        || input.downcast_ref::<AbortMultipartUploadInput>().is_some()
        || input
            .downcast_ref::<UploadPartCopyInput>()
            .is_some_and(|copy| copy.copy_source_if_match().is_some())
        || input
            .downcast_ref::<DeleteObjectInput>()
            .and_then(DeleteObjectInput::version_id)
            .is_some_and(|version| !version.is_empty() && version != "null")
}

// Operation errors are type-erased by the SDK. Check metadata for every operation
// permitted to replay; unlisted writes already have a single-attempt policy.
fn service_timeout(error: &Error) -> bool {
    macro_rules! timeout {
        ($($operation:ident::$kind:ident),+ $(,)?) => {
            $(error.downcast_ref::<$operation::$kind>()
                .is_some_and(|error| matches!(error.code(),
                    Some("RequestTimeout" | "RequestTimeoutException"))))||+
        };
    }
    timeout!(
        get_bucket_location::GetBucketLocationError,
        get_bucket_versioning::GetBucketVersioningError,
        get_object::GetObjectError,
        get_object_tagging::GetObjectTaggingError,
        head_bucket::HeadBucketError,
        head_object::HeadObjectError,
        list_buckets::ListBucketsError,
        list_multipart_uploads::ListMultipartUploadsError,
        list_objects_v2::ListObjectsV2Error,
        list_object_versions::ListObjectVersionsError,
        list_parts::ListPartsError,
        upload_part::UploadPartError,
        abort_multipart_upload::AbortMultipartUploadError,
        upload_part_copy::UploadPartCopyError,
        delete_object::DeleteObjectError,
    )
}
