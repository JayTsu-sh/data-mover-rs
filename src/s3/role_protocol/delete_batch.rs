//! Bounded, current-key bulk deletion. Content-MD5 is supplied by the connection interceptor.
use super::{
    ProvideErrorMetadata, S3ProtocolFailure, S3Result, S3Storage, s3_role_remote_failure,
    s3_role_transport_failure,
};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};

pub(super) async fn delete(storage: &S3Storage, keys: &[String]) -> Vec<S3Result<()>> {
    let mut results = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(100) {
        match send(storage, chunk).await {
            Ok(batch) => results.extend(batch),
            Err(failure) => results.extend(chunk.iter().map(|_| Err(failure.clone()))),
        }
    }
    results
}

async fn send(storage: &S3Storage, keys: &[String]) -> S3Result<Vec<S3Result<()>>> {
    let full_keys: Vec<_> = keys.iter().map(|key| storage.build_full_key(key)).collect();
    let objects = full_keys
        .iter()
        .map(|key| ObjectIdentifier::builder().key(key).build())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| S3ProtocolFailure::protocol("invalid S3 bulk-delete key"))?;
    let delete = Delete::builder()
        .set_objects(Some(objects))
        .quiet(true)
        .build()
        .map_err(|_| S3ProtocolFailure::protocol("invalid S3 bulk-delete request"))?;
    let response = storage
        .client
        .delete_objects()
        .bucket(&storage.bucket_name)
        .delete(delete)
        .send()
        .await
        .map_err(|e| classify_sdk!(e, "S3 DeleteObjects failed"))?;
    if response.errors().iter().any(|e| {
        e.key()
            .is_none_or(|key| !full_keys.iter().any(|known| known == key))
    }) {
        return Err(S3ProtocolFailure::protocol(
            "S3 bulk-delete response named an unknown key",
        ));
    }
    Ok(full_keys
        .iter()
        .map(|key| {
            response
                .errors()
                .iter()
                .find(|e| e.key() == Some(key.as_str()))
                .map_or(Ok(()), |e| {
                    Err(s3_role_remote_failure(
                        None,
                        e.code(),
                        "S3 bulk-delete entry failed",
                    ))
                })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssertTestValue;
    use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, Default)]
    struct Recorder {
        bodies: Arc<Mutex<Vec<String>>>,
    }
    impl HttpConnector for Recorder {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let body =
                String::from_utf8_lossy(request.body().bytes().unwrap_or_default()).into_owned();
            self.bodies
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(body.clone());
            let reply = if body.contains("<Key>key-7</Key>") {
                "<DeleteResult><Error><Key>key-7</Key><Code>AccessDenied</Code></Error></DeleteResult>"
            } else {
                "<DeleteResult/>"
            };
            HttpConnectorFuture::ready(Ok(HttpResponse::new(
                StatusCode::try_from(200).assert_value("valid status"),
                SdkBody::from(reply),
            )))
        }
    }

    #[tokio::test]
    async fn batches_are_bounded_and_partial_failure_keeps_successful_siblings() {
        let recorder = Recorder::default();
        let storage = super::super::native::tests::storage(recorder.clone());
        let keys: Vec<_> = (0..101).map(|index| format!("key-{index}")).collect();
        let results = delete(&storage, &keys).await;
        assert_eq!(results.len(), 101);
        assert!(
            results
                .iter()
                .enumerate()
                .all(|(index, result)| result.is_ok() == (index != 7))
        );
        assert!(matches!(
            &results[7],
            Err(S3ProtocolFailure::Entry {
                class: crate::model::FailureClass::PermissionDenied,
                ..
            })
        ));
        let bodies = recorder
            .bodies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0].matches("<Object>").count(), 100);
        assert_eq!(bodies[1].matches("<Object>").count(), 1);
        assert!(
            !bodies.iter().any(|body| body.contains("VersionId")),
            "current keys only"
        );
    }
}
