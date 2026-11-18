//! 有界批量删除；指定版本要求完整回执，Current 维持原有 quiet 路径。
use super::{ProvideErrorMetadata, S3ProtocolFailure, S3Result, S3Storage, s3_role_remote_failure};
use crate::backend::protocol::{unsubmitted_delete, validate_delete_objects};
use crate::model::SourceVersion;
use crate::storage::backends::s3::S3Protocol;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};

pub(super) async fn delete(
    storage: &S3Storage,
    objects: &[(String, SourceVersion)],
) -> Vec<S3Result<()>> {
    if let Err(failure) = validate_delete_objects(storage.bucket_capabilities(), objects) {
        return objects.iter().map(|_| Err(failure.clone())).collect();
    }
    let exact = objects
        .iter()
        .any(|(_, version)| matches!(version, SourceVersion::Id(_)));
    let mut results = Vec::with_capacity(objects.len());
    let mut stopped: Option<S3ProtocolFailure> = None;
    for chunk in objects.chunks(100) {
        let batch: Vec<S3Result<()>> = if let Some(failure) = &stopped {
            chunk
                .iter()
                .map(|_| Err(unsubmitted_delete(failure)))
                .collect()
        } else {
            match send(storage, chunk, exact).await {
                Ok(batch) => batch,
                Err(failure) => chunk.iter().map(|_| Err(failure.clone())).collect(),
            }
        };
        if exact && stopped.is_none() {
            stopped = batch
                .iter()
                .find_map(|result| result.as_ref().err().cloned());
        }
        results.extend(batch);
    }
    results
}

async fn send(
    storage: &S3Storage,
    selected: &[(String, SourceVersion)],
    exact: bool,
) -> S3Result<Vec<S3Result<()>>> {
    let full: Vec<_> = selected
        .iter()
        .map(|(key, version)| (storage.build_full_key(key), version.clone()))
        .collect();
    let objects = full
        .iter()
        .map(|(key, version)| {
            ObjectIdentifier::builder()
                .key(key)
                .set_version_id(match version {
                    SourceVersion::Current => None,
                    SourceVersion::Id(id) => Some(id.clone()),
                })
                .build()
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| S3ProtocolFailure::protocol("invalid S3 bulk-delete key"))?;
    let delete = Delete::builder()
        .set_objects(Some(objects))
        .quiet(!exact)
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
    Ok(decode(&full, &response, exact))
}

// 版本回执必须按 key＋ID 归属，不能把同 key 的一次成功或失败套给全部历史。
fn decode(
    selected: &[(String, SourceVersion)],
    response: &DeleteObjectsOutput,
    exact: bool,
) -> Vec<S3Result<()>> {
    let mut results = vec![None; selected.len()];
    let mut invalid = Vec::new();
    let receipts = response
        .errors()
        .len()
        .saturating_add(response.deleted().len());
    if receipts > selected.len() {
        invalid.push(S3ProtocolFailure::protocol(format!(
            "S3 bulk-delete returned {receipts} receipts for {} selectors",
            selected.len(),
        )));
    }
    // SDK 已解析响应；额外分类、错误集合与诊断仍按本批输入限制，异常响应不放大内存。
    for error in response.errors().iter().take(selected.len()) {
        // 复用统一的有界 code 脱敏，不能再把服务端原始错误码附回日志。
        let failure = s3_role_remote_failure(None, error.code(), "S3 bulk-delete entry failed");
        let receipt = match response_index(selected, error.key(), error.version_id()) {
            Ok(index) => remember(&mut results, index, Err(failure)),
            Err(mut invalid) => {
                // 无法归属的错误不冒充某一版本失败，但仍保留已脱敏的服务端原因。
                append_diagnostic(
                    &mut invalid,
                    &format!("unattributed remote failure: {failure:?}"),
                );
                Err(invalid)
            }
        };
        if let Err(failure) = receipt {
            invalid.push(failure);
        }
    }
    for deleted in response.deleted().iter().take(selected.len()) {
        if let Err(failure) = response_index(selected, deleted.key(), deleted.version_id())
            .and_then(|index| remember(&mut results, index, Ok(())))
        {
            invalid.push(failure);
        }
    }
    let invalid =
        (!invalid.is_empty()).then(|| format!("invalid S3 bulk-delete receipts: {invalid:?}"));
    results
        .into_iter()
        .map(|result| match (result, &invalid) {
            (Some(Err(mut failure)), Some(invalid)) => {
                // 协议回执异常不覆盖已确认的逐版本权限等错误分类及原始 code。
                append_diagnostic(&mut failure, invalid);
                Err(failure)
            }
            (_, Some(invalid)) => Err(S3ProtocolFailure::protocol(invalid)),
            (Some(result), None) => result,
            (None, None) if exact => Err(S3ProtocolFailure::protocol(
                "S3 bulk-delete response omitted a requested version",
            )),
            (None, None) => Ok(()),
        })
        .collect()
}

fn append_diagnostic(failure: &mut S3ProtocolFailure, detail: &str) {
    match failure {
        S3ProtocolFailure::Entry { diagnostic, .. }
        | S3ProtocolFailure::Session { diagnostic, .. } => {
            diagnostic.push_str("; ");
            diagnostic.push_str(detail);
        }
    }
}

fn response_index(
    selected: &[(String, SourceVersion)],
    key: Option<&str>,
    id: Option<&str>,
) -> S3Result<usize> {
    selected
        .iter()
        .position(|(known, version)| {
            key == Some(known.as_str())
                && match version {
                    SourceVersion::Current => true,
                    SourceVersion::Id(known) => id == Some(known.as_str()),
                }
        })
        .ok_or_else(|| {
            S3ProtocolFailure::protocol("S3 bulk-delete response named an unknown key or version")
        })
}

fn remember(
    results: &mut [Option<S3Result<()>>],
    index: usize,
    result: S3Result<()>,
) -> S3Result<()> {
    if results[index].is_some() {
        return Err(S3ProtocolFailure::protocol(format!(
            "S3 bulk-delete response repeated a key or version: {result:?}",
        )));
    }
    results[index] = Some(result);
    Ok(())
}

#[cfg(test)]
#[path = "delete_batch_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "delete_batch_real_tests.rs"]
mod real_tests;
