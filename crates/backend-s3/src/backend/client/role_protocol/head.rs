//! 指定删除标记的 HEAD 可返回 405；只有明确的标记类型及匹配版本头才算观察成功。
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_types::{DateTime, date_time::Format};

use super::{
    FailureClass, S3ProtocolFailure, S3Result, Transience, http_last_modified, s3_role_entry,
};
use crate::model::StorageTimestamp;
use crate::storage::backends::s3::S3ObjectFacts;

pub(super) fn facts(reply: &HeadObjectOutput, requested: Option<&str>) -> S3Result<S3ObjectFacts> {
    let modified = reply
        .last_modified()
        .and_then(|time| http_last_modified(time.secs()));
    if reply.delete_marker().unwrap_or(false) {
        return marker(requested, reply.version_id(), modified);
    }
    let size = reply
        .content_length()
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| {
            s3_role_entry(
                FailureClass::Corruption,
                "S3 HeadObject returned invalid content length",
            )
        })?;
    Ok(S3ObjectFacts {
        delete_marker: false,
        size,
        etag: reply.e_tag().unwrap_or_default().to_owned(),
        version_id: reply.version_id().map(str::to_owned),
        last_modified: modified,
    })
}

pub(super) fn marker_error(
    reply: Option<&HttpResponse>,
    requested: Option<&str>,
) -> Option<S3Result<S3ObjectFacts>> {
    let requested = requested?;
    let reply = reply?;
    if !matches!(reply.status().as_u16(), 404 | 405) {
        return None;
    }
    match reply.headers().get("x-amz-delete-marker") {
        Some("true") => {}
        None | Some("false") => return None,
        Some(_) => {
            return Some(Err(S3ProtocolFailure::protocol(
                "invalid delete-marker HEAD type header",
            )));
        }
    }
    let modified = reply
        .headers()
        .get("last-modified")
        .and_then(|value| DateTime::from_str(value, Format::HttpDate).ok())
        .and_then(|time| http_last_modified(time.secs()));
    Some(marker(
        Some(requested),
        reply.headers().get("x-amz-version-id"),
        modified,
    ))
}

fn marker(
    requested: Option<&str>,
    actual: Option<&str>,
    modified: Option<StorageTimestamp>,
) -> S3Result<S3ObjectFacts> {
    let Some(requested) = requested else {
        // 当前路径仍表现为缺失，保留 namespace 前缀及普通内容读取语义。
        return Err(S3ProtocolFailure::entry(
            FailureClass::NotFound,
            Transience::Permanent,
            "current object is hidden by a delete marker",
        ));
    };
    if requested.is_empty() || actual != Some(requested) {
        return Err(S3ProtocolFailure::protocol(
            "delete-marker HEAD did not confirm the exact requested version",
        ));
    }
    Ok(S3ObjectFacts {
        delete_marker: true,
        size: 0,
        etag: String::new(),
        version_id: Some(requested.to_owned()),
        last_modified: modified,
    })
}

#[cfg(test)]
mod tests;
