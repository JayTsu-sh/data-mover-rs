use super::*;
use crate::AssertTestValue;
use crate::backend::tests::{MemoryS3, Versioning};
use crate::model::{FailureClass, Transience};
use aws_sdk_s3::types::{DeletedObject, Error as DeleteError};
use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default)]
struct Recorder {
    bodies: Arc<Mutex<Vec<String>>>,
    exact_reply: bool,
    denied_ids: Vec<String>,
}
impl HttpConnector for Recorder {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let body = String::from_utf8_lossy(request.body().bytes().unwrap_or_default()).into_owned();
        self.bodies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(body.clone());
        let reply = if self.exact_reply {
            let mut reply = String::from("<DeleteResult>");
            for node in body.split("<Object>").skip(1) {
                let fields = node
                    .split("</Object>")
                    .next()
                    .assert_value("encoded object");
                let denied = self
                    .denied_ids
                    .iter()
                    .any(|id| fields.contains(&format!("<VersionId>{id}</VersionId>")));
                let (start, end) = if denied {
                    ("<Error>", "<Code>AccessDenied</Code></Error>")
                } else {
                    ("<Deleted>", "</Deleted>")
                };
                reply.push_str(start);
                reply.push_str(fields);
                reply.push_str(end);
            }
            reply.push_str("</DeleteResult>");
            reply
        } else if body.contains("<Key>key-7</Key>") {
            "<DeleteResult><Error><Key>key-7</Key><Code>AccessDenied</Code></Error></DeleteResult>"
                .into()
        } else {
            "<DeleteResult/>".into()
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
    let keys: Vec<_> = (0..101)
        .map(|index| (format!("key-{index}"), SourceVersion::Current))
        .collect();
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

fn selected(ids: &[&str]) -> Vec<(String, SourceVersion)> {
    ids.iter()
        .map(|id| ("key".into(), SourceVersion::Id((*id).into())))
        .collect()
}

fn acknowledged(id: &str) -> DeletedObject {
    DeletedObject::builder().key("key").version_id(id).build()
}

fn denied(id: Option<&str>) -> DeleteError {
    DeleteError::builder()
        .key("key")
        .set_version_id(id.map(str::to_owned))
        .code("AccessDenied")
        .build()
}

fn protocol_unknown(result: &S3Result<()>) -> bool {
    matches!(
        result,
        Err(S3ProtocolFailure::Session {
            class: FailureClass::Protocol,
            transience: Transience::Unknown,
            ..
        })
    )
}

#[test]
fn same_key_versions_match_shuffled_receipts_and_keep_each_error() {
    let request = selected(&["old", "null", "current"]);
    let response = DeleteObjectsOutput::builder()
        .deleted(acknowledged("current"))
        .errors(denied(Some("null")))
        .deleted(acknowledged("old"))
        .build();
    let results = decode(&request, &response, true);
    assert!(results[0].is_ok() && results[2].is_ok());
    assert!(matches!(&results[1], Err(S3ProtocolFailure::Entry {
        class: FailureClass::PermissionDenied, transience: Transience::Permanent, diagnostic
    }) if diagnostic.contains("AccessDenied")));
}

#[test]
fn missing_version_acknowledgement_cannot_be_inferred_from_its_siblings() {
    let request = selected(&["old", "current"]);
    let response = DeleteObjectsOutput::builder()
        .deleted(acknowledged("old"))
        .build();
    let results = decode(&request, &response, true);
    assert!(results[0].is_ok());
    assert!(protocol_unknown(&results[1]));
}

#[test]
fn unknown_duplicate_and_versionless_receipts_refuse_success_but_keep_denial() {
    let request = selected(&["old", "current"]);
    for bad in [
        DeleteObjectsOutput::builder().deleted(acknowledged("not-requested")),
        DeleteObjectsOutput::builder().deleted(acknowledged("current")),
        DeleteObjectsOutput::builder().errors(denied(None)),
    ] {
        let response = bad
            .deleted(acknowledged("current"))
            .errors(denied(Some("old")))
            .build();
        let results = decode(&request, &response, true);
        assert!(matches!(&results[0], Err(S3ProtocolFailure::Entry {
            class: FailureClass::PermissionDenied, diagnostic, ..
        }) if diagnostic.contains("AccessDenied") && diagnostic.contains("invalid S3 bulk-delete receipts")));
        assert!(protocol_unknown(&results[1]));
    }
}

#[test]
fn absent_key_or_wrong_version_never_matches_a_pinned_request() {
    let request = selected(&["old"]);
    for response in [
        DeleteObjectsOutput::builder()
            .deleted(DeletedObject::builder().version_id("old").build())
            .build(),
        DeleteObjectsOutput::builder().errors(denied(None)).build(),
        DeleteObjectsOutput::builder()
            .deleted(acknowledged("null"))
            .build(),
    ] {
        let results = decode(&request, &response, true);
        assert!(results.iter().all(protocol_unknown));
        if !response.errors().is_empty() {
            assert!(
                matches!(&results[0], Err(S3ProtocolFailure::Session {diagnostic, ..})
                if diagnostic.contains("AccessDenied") && diagnostic.contains("unattributed remote failure"))
            );
        }
    }
}

#[tokio::test]
async fn invalid_selectors_are_rejected_before_any_request() {
    let recorder = Recorder::default();
    let storage = super::super::native::tests::storage(recorder.clone());
    for objects in [
        selected(&["old", ""]),
        selected(&["old", "old"]),
        vec![
            ("key".into(), SourceVersion::Current),
            ("key".into(), SourceVersion::Id("old".into())),
        ],
    ] {
        assert!(
            delete(&storage, &objects)
                .await
                .iter()
                .all(|result| matches!(
                    result,
                    Err(S3ProtocolFailure::Entry {
                        class: FailureClass::InvalidInput,
                        ..
                    })
                ))
        );
    }
    assert!(
        recorder
            .bodies
            .lock()
            .assert_value("recorder lock")
            .is_empty()
    );
}

#[test]
fn nas_profile_rejects_all_version_selectors_including_null() {
    for version in ["old", "null"] {
        assert!(matches!(
            validate_delete_objects(
                crate::storage::backends::s3::S3BucketCapabilities::NAS,
                &selected(&[version])
            ),
            Err(S3ProtocolFailure::Entry {
                class: FailureClass::Unsupported,
                ..
            })
        ));
    }
}

#[tokio::test]
async fn incomplete_version_receipt_stops_later_batches_and_reports_not_submitted() {
    let recorder = Recorder::default();
    let storage = super::super::native::tests::storage(recorder.clone());
    let request: Vec<_> = (0..101)
        .map(|i| ("key".into(), SourceVersion::Id(format!("v{i}"))))
        .collect();
    let results = delete(&storage, &request).await;
    assert_eq!(results.len(), 101);
    assert!(results.iter().all(protocol_unknown));
    assert!(
        matches!(&results[100], Err(S3ProtocolFailure::Session {diagnostic, ..})
        if diagnostic.contains("not submitted"))
    );
    let bodies = recorder.bodies.lock().assert_value("recorder lock");
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0].matches("<Object>").count(), 100);
    assert_eq!(bodies[0].matches("<VersionId>").count(), 100);
    assert!(bodies[0].contains("<Quiet>false</Quiet>"));
}

#[tokio::test]
async fn complete_version_batches_keep_order_and_do_not_submit_after_partial_denials() {
    for denied_ids in [Vec::new(), vec!["v7".into(), "v8".into()]] {
        let recorder = Recorder {
            exact_reply: true,
            denied_ids: denied_ids.clone(),
            ..Recorder::default()
        };
        let storage = super::super::native::tests::storage(recorder.clone());
        let request: Vec<_> = (0..101)
            .map(|i| ("key".into(), SourceVersion::Id(format!("v{i}"))))
            .collect();
        let results = delete(&storage, &request).await;
        assert_eq!(results.len(), 101);
        for (index, result) in results.iter().enumerate() {
            if denied_ids.is_empty() || (index < 100 && index != 7 && index != 8) {
                assert!(result.is_ok(), "{index}: {result:?}");
            } else {
                assert!(matches!(result, Err(S3ProtocolFailure::Entry {
                    class: FailureClass::PermissionDenied, diagnostic, ..
                }) if diagnostic.contains("AccessDenied") && (index < 100 || diagnostic.contains("not submitted"))));
            }
        }
        let bodies = recorder.bodies.lock().assert_value("recorder lock");
        assert_eq!(bodies.len(), if denied_ids.is_empty() { 2 } else { 1 });
        assert_eq!(bodies[0].matches("<Object>").count(), 100);
        if bodies.len() == 2 {
            assert_eq!(bodies[1].matches("<Object>").count(), 1);
        }
    }
}

#[tokio::test]
async fn default_protocol_uses_exact_selectors_and_stops_after_first_failure() {
    let protocol = MemoryS3::default();
    protocol.set_versioning(Versioning::Enabled);
    for id in ["null", "held", "retained", "current"] {
        protocol
            .put_version("key", id, Bytes::from_static(b"retain"))
            .await;
    }
    protocol.lock_version("key", "held");
    let results = protocol
        .delete_objects(&selected(&["null", "held", "retained"]))
        .await;
    assert!(results[0].is_ok());
    assert!(matches!(
        &results[1],
        Err(S3ProtocolFailure::Entry {
            class: FailureClass::PermissionDenied,
            ..
        })
    ));
    assert!(
        matches!(&results[2], Err(S3ProtocolFailure::Entry {diagnostic, ..}) if diagnostic.contains("not submitted"))
    );
    assert_eq!(
        protocol.version_deletes(),
        vec![("key".into(), "null".into())]
    );
    assert!(protocol.plain_deletes_of_final_keys().is_empty());
}

#[tokio::test]
async fn default_protocol_validates_whole_nas_request_before_any_mutation() {
    let protocol = MemoryS3::nas_bucket();
    let results = protocol
        .delete_objects(&[
            ("other".into(), SourceVersion::Current),
            ("key".into(), SourceVersion::Id("null".into())),
        ])
        .await;
    assert!(results.iter().all(|result| matches!(
        result,
        Err(S3ProtocolFailure::Entry {
            class: FailureClass::Unsupported,
            ..
        })
    )));
    assert!(protocol.plain_deletes_of_final_keys().is_empty());
    assert!(protocol.version_deletes().is_empty());
}

#[test]
fn remote_error_codes_keep_existing_bounded_sanitization() {
    let response = DeleteObjectsOutput::builder()
        .errors(
            DeleteError::builder()
                .key("key")
                .version_id("old")
                .code("s3://access:secret@example.test")
                .build(),
        )
        .build();
    let results = decode(&selected(&["old"]), &response, true);
    assert!(
        matches!(&results[0], Err(S3ProtocolFailure::Session {diagnostic, ..})
        if diagnostic.contains("code=unknown") && !diagnostic.contains("secret"))
    );
}

#[tokio::test]
async fn default_mixed_key_batch_stops_before_versions_after_current_failure() {
    let protocol = MemoryS3::default();
    *protocol.delete_failure.lock().await = Some((
        "other".into(),
        S3ProtocolFailure::entry(
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "refused current selector",
        ),
    ));
    let results = protocol
        .delete_objects(&[
            ("other".into(), SourceVersion::Current),
            ("key".into(), SourceVersion::Id("old".into())),
        ])
        .await;
    assert!(
        matches!(&results[1], Err(S3ProtocolFailure::Entry {diagnostic, ..}) if diagnostic.contains("not submitted"))
    );
    assert!(protocol.version_deletes().is_empty());
}

#[test]
fn oversized_receipts_do_not_amplify_diagnostics_or_hide_known_failure() {
    let response = DeleteObjectsOutput::builder()
        .set_deleted(Some(vec![acknowledged("old"); 10_000]))
        .errors(denied(Some("old")))
        .build();
    let results = decode(&selected(&["old"]), &response, true);
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Err(S3ProtocolFailure::Entry {
        class: FailureClass::PermissionDenied, diagnostic, ..
    }) if diagnostic.contains("10001 receipts for 1 selectors")
        && diagnostic.contains("AccessDenied") && diagnostic.len() < 2048));
}
