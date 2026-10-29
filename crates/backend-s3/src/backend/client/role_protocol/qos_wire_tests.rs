//! Count actual SDK connector attempts, including retryable service failures.
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::native::tests::storage;
use crate::model::SourceVersion;
use crate::storage::backends::s3::source::S3ReadSource;
use crate::storage::backends::s3::tests::identity;
use crate::storage::{ReadRequest, ReadSource, SourceQosGroup, SourceQosPolicy};

#[derive(Clone, Debug)]
struct RetryServer {
    gets: Arc<Mutex<Vec<tokio::time::Instant>>>,
    failures: usize,
    failure_status: u16,
    cancel_after_first_get: Option<CancellationToken>,
}

impl HttpConnector for RetryServer {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let mut status = 200;
        let mut body = "";
        if request.method() == "GET" {
            let mut gets = self.gets.lock().unwrap_or_else(PoisonError::into_inner);
            gets.push(tokio::time::Instant::now());
            if gets.len() == 1
                && let Some(cancel) = &self.cancel_after_first_get
            {
                cancel.cancel();
            }
            if gets.len() <= self.failures {
                status = self.failure_status;
                body = if status == 403 {
                    "<Error><Code>AccessDenied</Code></Error>"
                } else if status == 400 {
                    "<Error><Code>RequestTimeout</Code></Error>"
                } else {
                    "<Error><Code>SlowDown</Code></Error>"
                };
            } else {
                status = 206;
                body = "x";
            }
        }
        let result = StatusCode::try_from(status)
            .map(|status| {
                let mut response = HttpResponse::new(status, SdkBody::from(body));
                response.headers_mut().insert("etag", "\"source\"");
                let length = if body.is_empty() { 1 } else { body.len() };
                response
                    .headers_mut()
                    .insert("content-length", length.to_string());
                response
            })
            .map_err(|error| ConnectorError::other(Box::new(error), None));
        HttpConnectorFuture::ready(result)
    }
}

#[tokio::test(start_paused = true)]
async fn source_qos_counts_and_paces_every_sdk_get_attempt()
-> Result<(), Box<dyn std::error::Error>> {
    for (failures, failure_status, expected_gets, success, cancelled) in [
        (2, 503, 3, true, false),
        (2, 400, 3, true, false),
        (10, 503, 3, false, false),
        (1, 403, 1, false, false),
        (10, 503, 1, false, true),
    ] {
        let cancel = CancellationToken::new();
        let server = RetryServer {
            gets: Arc::default(),
            failures,
            failure_status,
            cancel_after_first_get: cancelled.then(|| cancel.clone()),
        };
        let source = S3ReadSource::new(Arc::new(storage(server.clone())), identity());
        let path = crate::model::StoragePath::new("source")?;
        let described = source.describe(&path).await?;
        let budget =
            SourceQosGroup::new(SourceQosPolicy::new(None, 8, Some((2, 2, Duration::ZERO)))?)
                .transfer_budget();
        let started = tokio::time::Instant::now();
        let mut stream = source
            .read(ReadRequest {
                path,
                range: Some(0..1),
                expected_source: Some(described.source_identity),
                maximum_chunk_bytes: 8,
                read_inflight: 1,
                read_budget: None,
                cancel,
                source_qos: Some(budget.clone()),
                version: SourceVersion::Current,
            })
            .await?;
        let item = stream.next().await.ok_or("missing GET result")?;
        assert_eq!(item.is_ok(), success, "{item:?}");
        if cancelled {
            assert!(
                matches!(&item, Err(crate::storage::StorageRoleFailure::Entry(error))
                if error.class() == crate::model::FailureClass::Cancelled)
            );
        }
        assert_eq!(budget.stats().source_read_operations, expected_gets);
        assert_eq!(
            budget.stats().client_streamed_shaped_bytes,
            u64::from(success)
        );
        let gets = server.gets.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(u64::try_from(gets.len())?, expected_gets);
        assert!(gets[0].duration_since(started) >= Duration::from_millis(500));
        for pair in gets.windows(2) {
            assert!(pair[1].duration_since(pair[0]) >= Duration::from_millis(500));
        }
    }
    Ok(())
}
