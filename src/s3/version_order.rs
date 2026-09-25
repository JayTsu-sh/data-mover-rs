//! The server's document order of a `ListObjectVersions` response (P7 review, existing issue 8).
//!
//! S3 lists each key's versions and delete markers newest first as **one** sequence of
//! `<Version>` and `<DeleteMarker>` elements, but the SDK parses them into two separate lists
//! (`versions()`, `delete_markers()`) and loses how they interleave. `LastModified` cannot rebuild
//! it: it has a millisecond resolution, and concurrent writes to one key share milliseconds (`MinIO`
//! RELEASE.2023-03-20, 12 PUT + 4 DELETE × 3 rounds on one key: 20 of 44 entries share a
//! millisecond with another, and a time merge is wrong from the fourth entry on).
//!
//! [`send_in_document_order`] sends the request with an interceptor that keeps the raw response
//! body, reads the element sequence from it and gives every SDK-parsed entry its place in that
//! sequence. Every other field still comes from the SDK. The positions are only kept when the raw
//! elements name exactly the SDK's entries (same kind, key and version id, in the same order);
//! otherwise, or when the body is unavailable, the order is unknown and the caller infers it.

use std::sync::{Arc, Mutex, PoisonError};

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::list_object_versions::builders::ListObjectVersionsFluentBuilder;
use aws_sdk_s3::operation::list_object_versions::{
    ListObjectVersionsError, ListObjectVersionsOutput,
};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::AfterDeserializationInterceptorContextRef;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use bytes::Bytes;
use quick_xml::Reader;
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::Event;

/// One `ListObjectVersions` response and, when it could be read, the document position of each
/// of its entries.
pub(crate) struct VersionsInOrder {
    pub output: ListObjectVersionsOutput,
    pub order: Option<DocumentOrder>,
}

/// Document positions (0-based, `<Version>` and `<DeleteMarker>` counted together), index-aligned
/// with the SDK's `versions()` and `delete_markers()`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DocumentOrder {
    versions: Vec<u64>,
    markers: Vec<u64>,
}

impl DocumentOrder {
    /// The position of `versions()[index]`.
    pub(crate) fn version(&self, index: usize) -> Option<u64> {
        self.versions.get(index).copied()
    }

    /// The position of `delete_markers()[index]`.
    pub(crate) fn marker(&self, index: usize) -> Option<u64> {
        self.markers.get(index).copied()
    }

    /// Reads the element sequence of `body` and aligns it with the SDK's parse of the same
    /// response; `None` when the body is not a readable listing or names other entries.
    pub(crate) fn read(body: &[u8], output: &ListObjectVersionsOutput) -> Option<Self> {
        let records = document_records(body)?;
        let mut order = Self {
            versions: Vec::with_capacity(output.versions().len()),
            markers: Vec::with_capacity(output.delete_markers().len()),
        };
        let (mut versions, mut markers) =
            (output.versions().iter(), output.delete_markers().iter());
        for (position, record) in (0_u64..).zip(records) {
            let named = match record.kind {
                Kind::Version => versions.next().map(|v| (v.key(), v.version_id())),
                Kind::DeleteMarker => markers.next().map(|m| (m.key(), m.version_id())),
            };
            if named != Some((record.key.as_deref(), record.version_id.as_deref())) {
                return None;
            }
            match record.kind {
                Kind::Version => order.versions.push(position),
                Kind::DeleteMarker => order.markers.push(position),
            }
        }
        (versions.next().is_none() && markers.next().is_none()).then_some(order)
    }
}

/// Sends `request` and reads the document order of its response.
///
/// # Errors
/// The SDK's error, unchanged (boxed: it is large).
pub(crate) async fn send_in_document_order(
    request: ListObjectVersionsFluentBuilder,
) -> Result<VersionsInOrder, Box<SdkError<ListObjectVersionsError, HttpResponse>>> {
    let capture = CaptureBody::default();
    let output = request
        .customize()
        .interceptor(capture.clone())
        .send()
        .await
        .map_err(Box::new)?;
    let order = capture
        .take()
        .and_then(|body| DocumentOrder::read(&body, &output));
    if order.is_none() {
        tracing::debug!(
            "S3 ListObjectVersions: the response's document order could not be read; \
             version histories on this page are ordered by LastModified"
        );
    }
    Ok(VersionsInOrder { output, order })
}

/// Keeps the raw body of the last successful attempt of one request.
#[derive(Clone, Debug, Default)]
struct CaptureBody(Arc<Mutex<Option<Bytes>>>);

impl CaptureBody {
    fn take(&self) -> Option<Bytes> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

impl Intercept for CaptureBody {
    fn name(&self) -> &'static str {
        "ListObjectVersionsDocumentOrder"
    }

    /// After deserialization a non-streaming response's body is in memory; a retried request
    /// runs this per attempt, so the successful (last) attempt's body is the one kept.
    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        let body = response
            .status()
            .is_success()
            .then(|| response.body().bytes().map(Bytes::copy_from_slice))
            .flatten();
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = body;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Version,
    DeleteMarker,
}

/// One `<Version>` or `<DeleteMarker>` element: its key and version id as the text says them.
#[derive(Debug)]
struct Record {
    kind: Kind,
    key: Option<String>,
    version_id: Option<String>,
}

#[derive(Clone, Copy)]
enum Field {
    Key,
    VersionId,
}

impl Record {
    const fn empty(kind: Kind) -> Self {
        Self {
            kind,
            key: None,
            version_id: None,
        }
    }

    fn field(&mut self, field: Field) -> &mut String {
        match field {
            Field::Key => self.key.get_or_insert_with(String::new),
            Field::VersionId => self.version_id.get_or_insert_with(String::new),
        }
    }
}

/// The `<Version>` / `<DeleteMarker>` children of the root, in document order; `None` for a body
/// that is not well formed.
fn document_records(body: &[u8]) -> Option<Vec<Record>> {
    let mut reader = Reader::from_reader(body);
    let (mut depth, mut records) = (0_usize, Vec::new());
    let (mut current, mut field): (Option<Record>, Option<Field>) = (None, None);
    loop {
        match reader.read_event().ok()? {
            Event::Start(element) => {
                depth += 1;
                let name = element.local_name();
                match depth {
                    2 => current = kind_of(name.as_ref()).map(Record::empty),
                    3 if current.is_some() => field = field_of(name.as_ref()),
                    _ => {}
                }
            }
            Event::Empty(element) => {
                if depth == 1
                    && let Some(kind) = kind_of(element.local_name().as_ref())
                {
                    records.push(Record::empty(kind));
                } else if depth == 2
                    && let (Some(record), Some(field)) =
                        (current.as_mut(), field_of(element.local_name().as_ref()))
                {
                    record.field(field);
                }
            }
            Event::End(_) => {
                match depth {
                    2 => records.extend(current.take()),
                    3 => field = None,
                    _ => {}
                }
                depth = depth.checked_sub(1)?;
            }
            Event::Text(text) => text_into(&mut current, field, &text.decode().ok()?),
            Event::CData(text) => text_into(&mut current, field, &text.decode().ok()?),
            Event::GeneralRef(reference) => {
                let resolved = match reference.resolve_char_ref().ok()? {
                    Some(character) => character.to_string(),
                    None => resolve_predefined_entity(&reference.decode().ok()?)?.to_string(),
                };
                text_into(&mut current, field, &resolved);
            }
            Event::Eof => return (depth == 0).then_some(records),
            _ => {}
        }
    }
}

fn text_into(current: &mut Option<Record>, field: Option<Field>, text: &str) {
    if let (Some(record), Some(field)) = (current.as_mut(), field) {
        record.field(field).push_str(text);
    }
}

fn kind_of(name: &[u8]) -> Option<Kind> {
    match name {
        b"Version" => Some(Kind::Version),
        b"DeleteMarker" => Some(Kind::DeleteMarker),
        _ => None,
    }
}

fn field_of(name: &[u8]) -> Option<Field> {
    match name {
        b"Key" => Some(Field::Key),
        b"VersionId" => Some(Field::VersionId),
        _ => None,
    }
}

#[cfg(test)]
#[path = "version_order_tests.rs"]
mod tests;
